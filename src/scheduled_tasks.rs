use std::sync::Arc;

use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use std::time::Duration;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};

use crate::{errors::WorkerError, worker_run};
use tokio::time;

#[cfg(windows)]
pub async fn handle_scheduled_tasks(
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

    let batch_tracker = TaskTracker::new();
    loop {
        let signal_tracker_handle = main_tracker.clone();
        tokio::select! {
            biased;
            _ = cancellation_token.cancelled() => {
                warn!("scheduled tasks received task cancelled");
                batch_tracker.wait().await;
                break;
            },
            _ = signal_c.recv() => {
                info!("received ctrl c signal");
                cancellation_token.cancel();
                signal_tracker_handle.close();
            },
            _ = signal_break.recv() => {
                info!("received ctrl break signal");
                cancellation_token.cancel();
                signal_tracker_handle.close();
            },
            _ = signal_close.recv() => {
                info!("received ctrl close signal");
                cancellation_token.cancel();
                signal_tracker_handle.close();
            },
            _ = signal_shutdown.recv() => {
                info!("received ctrl shutdown signal");
                cancellation_token.cancel();
                signal_tracker_handle.close();
            },
            result = schedule_tasks(task_count, pool.clone(), batch_tracker.clone(), cancellation_token.clone()) => {
                let ready_scheduled_tasks_count = result.unwrap();
                batch_tracker.wait().await;
                if let None = ready_scheduled_tasks_count {
                    let sleep_length = Duration::from_secs(10);
                    warn!("All scheduled tasks complete, sleeping for {:?}", sleep_length);
                    time::sleep(sleep_length).await;
                }
            },
        }
    }
}

async fn schedule_tasks(
    task_count: u32,
    pool: Arc<Pool<PostgresConnectionManager<NoTls>>>,
    batch_tracker: TaskTracker,
    cancellation_token: CancellationToken,
) -> Result<Option<u32>, WorkerError> {
    let statement = "WITH task AS (
    SELECT * FROM messages
    WHERE scheduled_at IS NOT NULL AND status = 'pending' AND scheduled_at <= NOW()
    ORDER BY priority ASC
    FOR UPDATE SKIP LOCKED LIMIT 1
    )
    UPDATE messages SET status = 'in_progress',
    last_started_at = NOW()
    FROM task
    WHERE messages.id = task.id RETURNING task.*";

    let count_statement = "SELECT COUNT(*) FROM messages
        WHERE scheduled_at IS NOT NULL AND status = 'pending' AND scheduled_at <= NOW()";

    let pending_scheduled_tasks_count = {
        let connection = match pool.get().await {
            Ok(conn) => conn,
            Err(err) => {
                error!("Failed to get connection from pool: {:?}", err);
                return Err(WorkerError::PoolError(err));
            }
        };

        let pending_count: i64 = match connection.query_one(count_statement, &[]).await {
            Ok(row) => row.get(0),
            Err(err) => {
                error!("Failed to query pending scheduled task count: {:?}", err);
                return Err(err.into());
            }
        };
        pending_count
    };

    if pending_scheduled_tasks_count == 0 {
        warn!("No pending scheduled tasks found");
        return Ok(None);
    }

    warn!(
        "Found {} pending scheduled tasks, scheduling batch of {} workers",
        pending_scheduled_tasks_count, task_count
    );

    // if pending tasks are less than given task count only spin up what's necessary
    // to avoid querying the database more than necessary
    let task_count = task_count.min(pending_scheduled_tasks_count as u32);

    for _ in 0..task_count {
        let cloned_pool = pool.clone();
        batch_tracker.spawn(worker_run(cloned_pool, statement));
    }

    batch_tracker.close();

    tokio::select! {
        biased;
        _ = cancellation_token.cancelled() => {
            warn!("Cancellation requested during batch execution, waiting for current batch to complete");
            batch_tracker.wait().await;
            warn!("Batch completed after cancellation, exiting schedule_tasks");
            return Ok( None);
        }
        _ = batch_tracker.wait() => {
            warn!("Batch complete, checking for remaining scheduled tasks");
        }
    }

    Ok(Some(pending_scheduled_tasks_count as u32))
}
