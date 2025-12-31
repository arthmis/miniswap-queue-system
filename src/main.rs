mod clean_up_stuck_tasks;
mod db;
mod delete_completed_tasks;
mod errors;
mod message;
mod real_time_tasks;
mod retry;
mod scheduled_tasks;

use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use futures::{StreamExt, TryStreamExt, stream};
use rand::Rng;
use tokio_postgres::{Config, NoTls};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::Level;
use tracing::error;
use tracing::info;

use crate::db::PostgresQueue;
use crate::{
    errors::{MessageProcessingError, WorkerError},
    message::TaskPayload,
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

    let (sender, receiver) = futures::channel::mpsc::unbounded();
    let listen_config = config.clone();
    let (client, mut connection) = listen_config.connect(NoTls).await.unwrap();

    let stream = stream::poll_fn(move |context| connection.poll_message(context))
        .map_err(|e| panic!("{}", e));
    let listen_connection = stream.forward(sender);
    tokio::spawn(listen_connection);

    client.execute("LISTEN new_task", &[]).await.unwrap();

    let tracker = TaskTracker::new();

    let token = CancellationToken::new();

    let task_count: u32 = 10;

    let queue = {
        let manager = PostgresConnectionManager::new(config.clone(), NoTls);
        let conn_pool = match Pool::builder().build(manager).await {
            Ok(pool) => pool,
            Err(err) => {
                error!("{:?}", err);
                panic!("expected to make a connection to the database");
            }
        };
        PostgresQueue::new(conn_pool)
    };
    let cloned_token = token.clone();
    let main_tracker = tracker.clone();
    tokio::join!(
        real_time_tasks::handle_tasks_in_real_time(
            queue.clone(),
            main_tracker.clone(),
            cloned_token.clone(),
            receiver,
        ),
        clean_up_stuck_tasks::handle_failed_and_stuck_messages(
            task_count,
            queue.clone(),
            main_tracker.clone(),
            cloned_token.clone(),
        ),
        scheduled_tasks::handle_scheduled_tasks(
            task_count,
            queue.clone(),
            main_tracker.clone(),
            cloned_token.clone()
        ),
        delete_completed_tasks::periodically_delete_completed_tasks(
            queue.clone(),
            main_tracker.clone(),
            cloned_token.clone()
        )
    );

    tracker.close();
    tracker.wait().await;

    Ok(())
}

pub async fn worker_run(task: TaskPayload) -> Result<(), WorkerError> {
    retry(
        RetryStrategy::ExponentialBackoff {
            max_attempts: 5,
            duration: tokio::time::Duration::from_millis(1000),
        },
        || process_task(task.id(), task.payload()),
    )
    .await?;

    Ok(())
}

async fn process_task(id: i32, payload: &serde_json::Value) -> Result<(), MessageProcessingError> {
    // this is just to have a simulated processing time
    // rng needs to be dropped before await, can't be held across an await point
    let (delay, should_error) = {
        let mut rng = rand::rng();
        let delay = rng.random_range(1000..=5000);
        let should_error = rng.random_ratio(1, 100);
        (delay, should_error)
    };

    // is processing the task async as well?
    // might need to put a timeout in case we want to avoid really long running tasks
    tokio::time::sleep(tokio::time::Duration::from_millis(delay)).await;

    info!("task: {id}\npayload: {:?}\n", payload);

    // should error is used to simulate task failing
    if should_error {
        return Err(MessageProcessingError::Fail);
    }

    Ok(())
}
