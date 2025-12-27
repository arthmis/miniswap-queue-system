mod errors;
mod message;
mod retry;

use std::{sync::Arc, time::Duration};

use bb8::{Pool, PooledConnection};
use bb8_postgres::PostgresConnectionManager;
#[cfg(windows)]
use futures::channel::mpsc::UnboundedReceiver;
use futures::{FutureExt, StreamExt, TryStreamExt, stream};
use rand::Rng;
use serde_json::json;
use tokio::time::{self, sleep};
use tokio_postgres::{Config, NoTls};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::Level;
use tracing::info;

use crate::{
    errors::{MessageProcessingError, WorkerError},
    message::{JobStatus, MessagePayload},
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
    let conn_pool = Arc::new(Pool::builder().build(manager).await.unwrap());

    let (sender, receiver) = futures::channel::mpsc::unbounded();
    let (client, mut connection) = listen_config.connect(NoTls).await.unwrap();

    let stream = stream::poll_fn(move |context| connection.poll_message(context))
        .map_err(|e| panic!("{}", e));
    let listen_connection = stream.forward(sender).map(|r| r.unwrap());
    tokio::spawn(listen_connection);

    client.execute("LISTEN new_task", &[]).await.unwrap();

    let pool_clone = conn_pool.clone();
    tokio::spawn(async {
        time::sleep(Duration::from_secs(5)).await;
        create_messages_table(pool_clone).await.unwrap();
    });

    let pool_clone = conn_pool.clone();
    tokio::spawn(async {
        time::sleep(Duration::from_secs(5)).await;
        insert_test_messages(pool_clone, 20).await.unwrap();
    });

    let tracker = TaskTracker::new();

    let token = CancellationToken::new();

    let cloned_token = token.clone();
    let main_tracker = tracker.clone();
    let pool = conn_pool.clone();
    tokio::join!(
        handle_messages(
            pool.clone(),
            main_tracker.clone(),
            cloned_token.clone(),
            receiver,
        ),
        handle_failed_and_stuck_messages(pool.clone(), main_tracker, cloned_token)
    );

    tracker.close();
    tracker.wait().await;

    Ok(())
}

#[cfg(unix)]
async fn handle_messages(
    worker_count: u32,
    pool: Arc<Pool<PostgresConnectionManager<NoTls>>>,
    main_tracker: TaskTracker,
    cancellation_token: CancellationToken,
) {
    use tokio::signal::unix;

    let mut signal_terminate = signal(SignalKind::terminate()).unwrap();
    let mut signal_interrupt = signal(SignalKind::interrupt()).unwrap();

    loop {
        let tracker_handle = main_tracker.clone();
        let signal_tracker_handle = main_tracker.clone();
        tokio::select! {
            _ = signal_terminate.recv() => {
                cancellation_token.cancel();
                signal_tracker_handle.close();
                break;
            },
            _ = signal_interrupt.recv() => {
                cancellation_token.cancel();
                signal_tracker_handle.close();
                break;
            },
            _ = launch_workers(worker_count, pool.clone(), tracker_handle) => {},
        };
    }
}

#[cfg(windows)]
async fn handle_messages(
    pool: Arc<Pool<PostgresConnectionManager<NoTls>>>,
    main_tracker: TaskTracker,
    cancellation_token: CancellationToken,
    mut receiver: UnboundedReceiver<tokio_postgres::AsyncMessage>,
) {
    use tokio::signal::windows;

    let mut signal_c = windows::ctrl_c().unwrap();
    let mut signal_break = windows::ctrl_break().unwrap();
    let mut signal_close = windows::ctrl_close().unwrap();
    let mut signal_shutdown = windows::ctrl_shutdown().unwrap();

    loop {
        let signal_tracker_handle = main_tracker.clone();
        tokio::select! {
            message = receiver.next() => {
                match message {
                    Some(tokio_postgres::AsyncMessage::Notification(_notification_message)) => {
                        let worker_pool_handle = pool.clone();
                        main_tracker.spawn(async move {
                            let client = worker_pool_handle.get().await.unwrap();
                            let _ = worker_run(client).await;
                        });
                    },
                    Some(n) => info!("{:?}", n),
                    _ => info!("No notification sent."),
                };
            },
            _ = signal_c.recv() => {
                cancellation_token.cancel();
                signal_tracker_handle.close();
                break;
            },
            _ = signal_break.recv() => {
                cancellation_token.cancel();
                signal_tracker_handle.close();
                break;
            },
            _ = signal_close.recv() => {
                cancellation_token.cancel();
                signal_tracker_handle.close();
                break;
            },
            _ = signal_shutdown.recv() => {
                cancellation_token.cancel();
                signal_tracker_handle.close();
                break;
            },
        }
    }
}

#[cfg(windows)]
async fn handle_failed_and_stuck_messages(
    pool: Arc<Pool<PostgresConnectionManager<NoTls>>>,
    main_tracker: TaskTracker,
    cancellation_token: CancellationToken,
) {
    use tokio::signal::windows;

    let mut signal_c = windows::ctrl_c().unwrap();
    let mut signal_break = windows::ctrl_break().unwrap();
    let mut signal_close = windows::ctrl_close().unwrap();
    let mut signal_shutdown = windows::ctrl_shutdown().unwrap();

    loop {
        let signal_tracker_handle = main_tracker.clone();
        tokio::select! {
            _ = handle_stuck_jobs(pool.get().await.unwrap()) => {
            },
            _ = signal_c.recv() => {
                cancellation_token.cancel();
                signal_tracker_handle.close();
                break;
            },
            _ = signal_break.recv() => {
                cancellation_token.cancel();
                signal_tracker_handle.close();
                break;
            },
            _ = signal_close.recv() => {
                cancellation_token.cancel();
                signal_tracker_handle.close();
                break;
            },
            _ = signal_shutdown.recv() => {
                cancellation_token.cancel();
                signal_tracker_handle.close();
                break;
            },
        }
    }
}

async fn handle_stuck_jobs(mut connection: PoolConnection<'_>) -> Result<(), WorkerError> {
    let statement = "WITH task AS (
        SELECT * FROM messages
        WHERE status = 'in_progress'
        AND started_at < NOW() - INTERVAL '5 minutes'
        FOR UPDATE SKIP LOCKED LIMIT 1
        )
        UPDATE messages SET status = 'in_progress',
        last_started_at = NOW()
        FROM task
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
    println!("task completed {:?}\n", message);
    Ok(())
}

type PoolConnection<'a> = PooledConnection<'a, PostgresConnectionManager<NoTls>>;

async fn worker_run(mut connection: PoolConnection<'_>) -> Result<(), WorkerError> {
    let statement = "WITH task AS (
        SELECT * FROM messages
        WHERE status = 'pending'
        ORDER BY created_at DESC
        FOR UPDATE SKIP LOCKED LIMIT 1
        )
        UPDATE messages SET status = 'in_progress',
        last_started_at = NOW()
        FROM task
        WHERE messages.id = task.id RETURNING task.*";
    let transaction = connection.transaction().await?;
    let result = transaction.query_opt(statement, &[]).await;

    let row = result.unwrap().unwrap();
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
    println!("task completed {:?}\n", message);
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

async fn create_messages_table(
    pool: Arc<Pool<PostgresConnectionManager<NoTls>>>,
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
                last_started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE OR REPLACE FUNCTION new_job_trigger_fn() RETURNS trigger AS $$
            BEGIN
              PERFORM pg_notify('new_task', NEW.id::text);
              RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;

            CREATE TRIGGER new_task_trigger
            AFTER INSERT ON messages
            FOR EACH ROW
            EXECUTE FUNCTION new_job_trigger_fn();
            ",
        )
        .await?;

    Ok(())
}

async fn insert_test_messages(
    pool: Arc<Pool<PostgresConnectionManager<NoTls>>>,
    count: usize,
) -> Result<(), tokio_postgres::Error> {
    let client = pool.get().await.unwrap();
    loop {
        let payloads = {
            let mut rng = rand::rng();

            let queue_names = ["orders", "notifications", "payments", "analytics", "emails"];
            let actions = ["create", "update", "delete", "process", "sync"];
            let entities = ["user", "product", "order", "invoice", "subscription"];

            let mut payloads = Vec::new();
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
                payloads.push((queue_name, payload));
            }
            payloads
        };

        for (queue_name, payload) in payloads {
            client
                .execute(
                    "INSERT INTO messages (queue_name, payload, status) VALUES ($1, $2, $3)",
                    &[&queue_name, &payload, &JobStatus::Pending],
                )
                .await?;
        }
        sleep(Duration::from_secs(30)).await;
    }
}
