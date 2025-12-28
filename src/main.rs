mod clean_up_stuck_tasks;
mod errors;
mod message;
mod real_time_tasks;
mod retry;
mod scheduled_tasks;
mod simulated_data_generation;

use std::sync::Arc;
use std::time::Duration;

use bb8::{Pool, PooledConnection};
use bb8_postgres::PostgresConnectionManager;
use futures::{StreamExt, TryStreamExt, stream};
use rand::Rng;
use tokio::time;
use tokio_postgres::{Config, NoTls};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::Level;
use tracing::debug;
use tracing::error;
use tracing::info;

use crate::simulated_data_generation::create_messages_table;
use crate::simulated_data_generation::insert_test_messages;
use crate::{
    errors::{MessageProcessingError, WorkerError},
    message::MessagePayload,
    retry::{RetryStrategy, retry},
};

#[tokio::main]
async fn main() -> Result<(), tokio_postgres::Error> {
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_file(true)
        .with_line_number(true)
        .init();

    let mut config = Config::new();
    config
        .user("miniswap")
        .dbname("miniswap")
        .password("miniswap")
        .hostaddr(std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)))
        .port(7777)
        .connect_timeout(core::time::Duration::from_secs(10));

    let listen_config = config.clone();
    let manager = PostgresConnectionManager::new(config, NoTls);
    let conn_pool = match Pool::builder().build(manager).await {
        Ok(pool) => pool,
        Err(err) => {
            error!("{:?}", err);
            panic!("expected to make a connection to the database");
        }
    };
    let conn_pool = Arc::new(conn_pool);

    let (sender, receiver) = futures::channel::mpsc::unbounded();
    let (client, mut connection) = listen_config.connect(NoTls).await.unwrap();

    let stream = stream::poll_fn(move |context| connection.poll_message(context))
        .map_err(|e| panic!("{}", e));
    let listen_connection = stream.forward(sender);
    tokio::spawn(listen_connection);

    client.execute("LISTEN new_task", &[]).await.unwrap();

    let pool_clone = conn_pool.clone();
    create_messages_table(pool_clone).await.unwrap();

    let pool_clone = conn_pool.clone();
    tokio::spawn(async {
        insert_test_messages(pool_clone, 5).await.unwrap();
    });

    let tracker = TaskTracker::new();

    let token = CancellationToken::new();

    let task_count: u32 = 6;

    let cloned_token = token.clone();
    let main_tracker = tracker.clone();
    let pool = conn_pool.clone();
    tokio::join!(
        real_time_tasks::handle_messages(
            pool.clone(),
            main_tracker.clone(),
            cloned_token.clone(),
            receiver,
        ),
        clean_up_stuck_tasks::handle_failed_and_stuck_messages(
            task_count,
            pool.clone(),
            main_tracker.clone(),
            cloned_token.clone(),
        ),
        scheduled_tasks::handle_scheduled_tasks(
            task_count,
            pool.clone(),
            main_tracker.clone(),
            cloned_token.clone()
        )
    );

    tracker.close();
    tracker.wait().await;

    Ok(())
}

type PoolConnection<'a> = PooledConnection<'a, PostgresConnectionManager<NoTls>>;

async fn worker_run(mut connection: PoolConnection<'_>) -> Result<(), WorkerError> {
    let statement = "WITH task AS (
        SELECT * FROM messages
        WHERE status = 'pending' AND scheduled_at IS NULL
        ORDER BY priority ASC, created_at DESC
        FOR UPDATE SKIP LOCKED LIMIT 1
        )
        UPDATE messages SET status = 'in_progress',
        last_started_at = NOW()
        FROM task
        WHERE messages.id = task.id RETURNING task.*";
    let transaction = connection.transaction().await?;
    let result = transaction.query_opt(statement, &[]).await;

    let Some(row) = result? else {
        return Ok(());
    };
    let message = MessagePayload::try_from(row)?;

    retry(
        RetryStrategy::ExponentialBackoff {
            max_attempts: 5,
            duration: tokio::time::Duration::from_millis(1000),
        },
        || process_task(message.id(), message.payload()),
    )
    .await?;

    transaction
        .execute(
            "UPDATE messages SET status = 'completed' WHERE id = $1",
            &[&message.id()],
        )
        .await?;

    transaction.commit().await?;
    debug!("Task completed - id: {}", message.id());
    Ok(())
}

async fn process_task(id: i32, payload: &serde_json::Value) -> Result<(), MessageProcessingError> {
    info!("task: {id}\npayload: {:?}\n", payload);

    // this is just to have a simulated processing time
    // rng needs to be dropped before await, can't be held across an await point
    let (delay, should_error) = {
        let mut rng = rand::rng();
        let delay = rng.random_range(1000..=5000);
        let should_error = rng.random_ratio(5, 100);
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
