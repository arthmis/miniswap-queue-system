mod errors;
mod message;
mod retry;

use std::sync::Arc;

use bb8::{Pool, PooledConnection};
use bb8_postgres::PostgresConnectionManager;
use rand::Rng;
use serde_json::json;
use tokio_postgres::{Config, NoTls};

use crate::{
    errors::{MessageProcessingError, WorkerError},
    message::{JobStatus, MessagePayload},
    retry::{RetryStrategy, retry},
};

#[tokio::main]
async fn main() -> Result<(), tokio_postgres::Error> {
    let mut config = Config::new();
    config
        .user("miniswap")
        .dbname("miniswap")
        .password("miniswap")
        .hostaddr(std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)))
        .port(7777)
        .connect_timeout(core::time::Duration::from_secs(10));

    let manager = PostgresConnectionManager::new(config, NoTls);
    let conn_pool = Arc::new(Pool::builder().build(manager).await.unwrap());

    create_messages_table(&conn_pool).await?;
    insert_test_messages(&conn_pool, 5).await?;

    let pool = conn_pool.clone();

    let worker_count = 16;

    let handle = tokio::spawn(async move {
        loop {
            for _ in 0..worker_count {
                let worker_pool_handle = pool.clone();
                tokio::spawn(async move {
                    let client = worker_pool_handle.get().await.unwrap();
                    let _ = worker_run(client).await;
                });
            }
            tokio::time::sleep(tokio::time::Duration::from_mins(1)).await
        }
    });

    handle.await.unwrap();

    Ok(())
}

type PoolConnection<'a> = PooledConnection<'a, PostgresConnectionManager<NoTls>>;

async fn worker_run(mut connection: PoolConnection<'_>) -> Result<(), WorkerError> {
    let statement = "WITH task AS (SELECT * FROM messages WHERE status = 'pending' ORDER BY created_at DESC FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE messages SET status = 'in_progress' FROM task
    WHERE messages.id = task.id RETURNING task.*";
    let transaction = connection.transaction().await?;
    let result = transaction.query_opt(statement, &[]).await;

    let Ok(Some(row)) = result else {
        return Ok(());
    };

    let message = MessagePayload::try_from(row)?;

    retry(
        RetryStrategy::ExponentialBackoff {
            max_attempts: 5,
            duration: tokio::time::Duration::from_millis(1000),
        },
        || process_message(message.id(), message.payload()),
    )
    .await?;

    transaction
        .execute(
            "UPDATE messages SET status = 'completed' WHERE id = $1",
            &[&message.id()],
        )
        .await?;

    transaction.commit().await?;
    Ok(())
}

async fn process_message(
    id: i32,
    payload: &serde_json::Value,
) -> Result<(), MessageProcessingError> {
    println!("message: {id}\n payload: {:?}", payload);

    // this is just to have a simulated processing time
    // rng needs to be dropped before await, can't be held across an await point
    let (delay, should_error) = {
        let mut rng = rand::rng();
        let delay = rng.random_range(1..=1000);
        let should_error = rng.random_ratio(4, 5);
        (delay, should_error)
    };

    // is processing the task async as well?
    // might need to put a timeout in case we want to avoid really long running tasks
    tokio::time::sleep(tokio::time::Duration::from_millis(delay)).await;

    // should error is used to simulate task failing
    if should_error {
        return Err(MessageProcessingError::Fail);
    }

    Ok(())
}

async fn create_messages_table(
    pool: &Pool<PostgresConnectionManager<NoTls>>,
) -> Result<(), tokio_postgres::Error> {
    let client = pool.get().await.unwrap();

    // Create the messages table if it doesn't exist
    client
        .batch_execute(
            "
            DO $$ BEGIN
                CREATE TYPE job_status AS ENUM ('pending', 'in_progress', 'completed');
            EXCEPTION
                WHEN duplicate_object THEN null;
            END $$;

            CREATE INDEX IF NOT EXISTS message_status_idx
            ON messages(status, id);

            DROP TABLE IF EXISTS messages;
            CREATE TABLE IF NOT EXISTS messages (
                id SERIAL PRIMARY KEY,
                queue_name TEXT NOT NULL,
                payload JSONB,
                status job_status NOT NULL DEFAULT 'pending',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );",
        )
        .await?;

    Ok(())
}

async fn insert_test_messages(
    pool: &Pool<PostgresConnectionManager<NoTls>>,
    count: usize,
) -> Result<(), tokio_postgres::Error> {
    let client = pool.get().await.unwrap();
    let mut rng = rand::rng();

    let queue_names = ["orders", "notifications", "payments", "analytics", "emails"];
    let actions = ["create", "update", "delete", "process", "sync"];
    let entities = ["user", "product", "order", "invoice", "subscription"];

    for _ in 0..count {
        let queue_name = queue_names[rng.random_range(0..queue_names.len())];
        let action = actions[rng.random_range(0..actions.len())];
        let entity = entities[rng.random_range(0..entities.len())];

        let payload = json!({
            "action": action,
            "entity": entity,
            "entity_id": rng.random_range(1000..9999),
            "amount": rng.random_range(10.0..1000.0_f64),
            "priority": rng.random_range(1..5),
            "metadata": {
                "source": format!("system_{}", rng.random_range(1..10)),
                "timestamp": chrono::Utc::now().to_rfc3339(),
                "retry_count": 0
            }
        });

        client
            .execute(
                "INSERT INTO messages (queue_name, payload, status) VALUES ($1, $2, $3)",
                &[&queue_name, &payload, &JobStatus::Pending],
            )
            .await?;
    }

    Ok(())
}
