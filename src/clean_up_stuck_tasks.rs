use std::sync::Arc;

use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use tokio_postgres::NoTls;
#[cfg(windows)]
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info};

use crate::{
    errors::WorkerError,
    message::MessagePayload,
    process_task,
    retry::{RetryStrategy, retry},
};

#[cfg(windows)]
pub async fn handle_failed_and_stuck_messages(
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
        use std::time::Duration;

        use tokio::time;

        let signal_tracker_handle = main_tracker.clone();
        tokio::select! {
            _ = crate::clean_up_stuck_tasks::schedule_stuck_jobs(task_count, pool.clone(), main_tracker.clone()) => {
                time::sleep(Duration::from_secs(20)).await;
            },
            _ = signal_c.recv() => {
                info!("received ctrl c signal");
                cancellation_token.cancel();
                signal_tracker_handle.close();
                break;
            },
            _ = signal_break.recv() => {
                info!("received ctrl break signal");
                cancellation_token.cancel();
                signal_tracker_handle.close();
                break;
            },
            _ = signal_close.recv() => {
                info!("received ctrl close signal");
                cancellation_token.cancel();
                signal_tracker_handle.close();
                break;
            },
            _ = signal_shutdown.recv() => {
                info!("received ctrl shutdown signal");
                cancellation_token.cancel();
                signal_tracker_handle.close();
                break;
            },
        }
    }
}

pub async fn schedule_stuck_jobs(
    task_count: u32,
    pool: Arc<Pool<PostgresConnectionManager<NoTls>>>,
    task_tracker: TaskTracker,
) -> Result<(), WorkerError> {
    info!("scheduling {} tasks for stuck jobs", task_count);

    async fn handle_stuck_job(
        cloned_pool: Arc<Pool<PostgresConnectionManager<NoTls>>>,
    ) -> Result<(), WorkerError> {
        let mut connection = cloned_pool.get().await.unwrap();
        let statement = "WITH task AS (
        SELECT * FROM messages
        WHERE (status = 'in_progress' AND last_started_at < NOW() - INTERVAL '5 minutes')
        OR (status = 'pending' AND last_started_at < NOW() - INTERVAL '5 minutes')
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

        info!("Task completed - id: {}", message.id());
        Ok(())
    }

    for _ in 0..task_count {
        let cloned_pool = pool.clone();
        task_tracker.spawn(handle_stuck_job(cloned_pool));
    }
    Ok(())
}
