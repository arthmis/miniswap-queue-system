mod errors;
mod message;
mod retry;

use std::{sync::Arc, time::Duration};

use bb8::{Pool, PooledConnection};
use bb8_postgres::PostgresConnectionManager;
#[cfg(windows)]
use futures::channel::mpsc::UnboundedReceiver;
use futures::{StreamExt, TryStreamExt, stream};
use rand::Rng;
use serde_json::json;
use tokio::time::{self, sleep};
use tokio_postgres::{Config, NoTls};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::Level;
use tracing::debug;
use tracing::error;
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

    let task_count: u32 = 6;

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
        handle_failed_and_stuck_messages(task_count, pool.clone(), main_tracker, cloned_token)
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

    let mut signal_terminate =
        signal(SignalKind::terminate()).expect("signal terminate to be available");
    let mut signal_interrupt =
        signal(SignalKind::interrupt()).expect("signal interrupt to be available");

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

    let mut signal_c = windows::ctrl_c().expect("ctrl c to be available");
    let mut signal_break = windows::ctrl_break().expect("ctrl break to be available");
    let mut signal_close = windows::ctrl_close().expect("ctrl_close to be available");
    let mut signal_shutdown = windows::ctrl_shutdown().expect("ctrl_shutdown to be available");

    loop {
        let signal_tracker_handle = main_tracker.clone();
        tokio::select! {
            message = receiver.next() => {
                match message {
                    Some(tokio_postgres::AsyncMessage::Notification(_notification_message)) => {
                        let worker_pool_handle = pool.clone();
                        main_tracker.spawn(async move {
                            let client = retry(
                                RetryStrategy::ExponentialBackoff {
                                    max_attempts: 3,
                                    duration: Duration::from_millis(50)
                                },
                                || worker_pool_handle.get()
                            ).await;
                            match client {
                                Ok(client) => {
                                    let _ = worker_run(client).await;
                                },
                                Err(err) => error!("Couldn't get a connection to pool within 3 retries:\n{:?}", err),
                            };

                        });
                    },
                    Some(n) => debug!("{:?}", n),
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
    task_count: u32,
    pool: Arc<Pool<PostgresConnectionManager<NoTls>>>,
    main_tracker: TaskTracker,
    cancellation_token: CancellationToken,
) {
    use tokio::signal::windows;

    let mut signal_c = windows::ctrl_c().expect("ctrl c to be available");
    let mut signal_break = windows::ctrl_break().expect("ctrl break to be available");
    let mut signal_close = windows::ctrl_close().expect("ctrl close to be available");
    let mut signal_shutdown = windows::ctrl_shutdown().expect("ctrl shutdown to be available");

    loop {
        let signal_tracker_handle = main_tracker.clone();
        tokio::select! {
            _ = schedule_stuck_jobs(task_count, pool.clone(), main_tracker.clone()) => {
                time::sleep(Duration::from_mins(1)).await;
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

async fn schedule_stuck_jobs(
    task_count: u32,
    pool: Arc<Pool<PostgresConnectionManager<NoTls>>>,
    task_tracker: TaskTracker,
) -> Result<(), WorkerError> {
    async fn handle_stuck_job(
        cloned_pool: Arc<Pool<PostgresConnectionManager<NoTls>>>,
    ) -> Result<(), WorkerError> {
        let mut connection = cloned_pool.get().await.unwrap();
        let statement = "WITH task AS (
        SELECT * FROM messages
        WHERE status = 'in_progress'
        AND started_at < NOW() - INTERVAL '5 minutes'
        ORDER BY priority ASC
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
        let message_result = retry(
            RetryStrategy::ExponentialBackoff {
                max_attempts: 5,
                duration: tokio::time::Duration::from_millis(1000),
            },
            || process_task(message.id(), message.payload()),
        )
        .await;
        if let Err(err) = message_result {
            error!(
                "Error processing message for message id: {}\nerror: {:?}",
                message.id(),
                err
            );
            return Err(err.into());
        }

        let status_update_result = transaction
            .execute(
                "UPDATE messages SET status = 'completed' WHERE id = $1",
                &[&message.id()],
            )
            .await;
        let updated_row_count = match status_update_result {
            Ok(updated_row_count) => updated_row_count,
            Err(err) => {
                error!(
                    "Error updating status of task with id: {}\nerror: {:?}",
                    message.id(),
                    err
                );
                return Err(err.into());
            }
        };
        assert_eq!(updated_row_count, 1);

        if let Err(err) = transaction.commit().await {
            error!(
                "Error completing transaction for task with id: {}\nerror: {:?}",
                message.id(),
                err
            );
            return Err(err.into());
        }

        debug!("Task completed - id: {}", message.id());
        Ok(())
    }

    for _ in 0..task_count {
        let cloned_pool = pool.clone();
        task_tracker.spawn(handle_stuck_job(cloned_pool));
    }
    Ok(())
}

type PoolConnection<'a> = PooledConnection<'a, PostgresConnectionManager<NoTls>>;

async fn worker_run(mut connection: PoolConnection<'_>) -> Result<(), WorkerError> {
    let statement = "WITH task AS (
        SELECT * FROM messages
        WHERE status = 'pending'
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
    println!("task: {id}\npayload: {:?}\n", payload);

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
                priority INTEGER NOT NULL,
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
                let priority = rng.random_range(0..=4); // 0 = highest priority, 4 = lowest

                let payload = json!({
                    "action": action,
                    "entity": entity,
                    "entity_id": rng.random_range(1000..9999),
                    "amount": rng.random_range(10.0..1000.0_f64),
                    "priority": priority,
                    "metadata": {
                        "source": format!("system_{}", rng.random_range(1..10)),
                        "timestamp": chrono::Utc::now().to_rfc3339(),
                        "retry_count": 0
                    }
                });
                payloads.push((queue_name, payload, priority));
            }
            payloads
        };

        for (queue_name, payload, priority) in payloads {
            client
                .execute(
                    "INSERT INTO messages (queue_name, payload, priority, status) VALUES ($1, $2, $3, $4)",
                    &[&queue_name, &payload, &priority, &JobStatus::Pending],
                )
                .await?;
        }
        sleep(Duration::from_secs(30)).await;
    }
}
