use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};

#[cfg(windows)]
use crate::db::Queue;
use crate::{db::PostgresQueueError, worker_run};
use tokio::time;

#[cfg(windows)]
pub async fn handle_scheduled_tasks<T>(
    task_count: u32,
    pool: T,
    main_tracker: TaskTracker,
    cancellation_token: CancellationToken,
) where
    T: Queue + Clone + Send + 'static,
    PostgresQueueError: From<<T as Queue>::QueueError>,
{
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

async fn schedule_tasks<T>(
    task_count: u32,
    queue: T,
    batch_tracker: TaskTracker,
    cancellation_token: CancellationToken,
) -> Result<Option<u32>, PostgresQueueError>
where
    T: Queue + Clone + Send + 'static,
    PostgresQueueError: From<<T as Queue>::QueueError>,
{
    let pending_scheduled_tasks_count = queue.pending_scheduled_tasks_count().await?;

    if pending_scheduled_tasks_count == 0 {
        info!("No pending scheduled tasks found");
        return Ok(None);
    }

    info!(
        "Found {} pending scheduled tasks, scheduling batch of {} workers",
        pending_scheduled_tasks_count, task_count
    );

    // if pending tasks are less than given task count only spin up what's necessary
    // to avoid querying the database more than necessary
    let task_count = task_count.min(pending_scheduled_tasks_count as u32);

    for _ in 0..task_count {
        let worker_queue = queue.clone();
        batch_tracker.spawn(async move {
            if let Ok(Some(task)) = worker_queue.get_scheduled_task().await {
                let task_id = task.id();
                if let Err(err) = worker_run(task).await {
                    error!(
                        "Error processing scheduled task with id: {}\nerror: {:?}",
                        task_id, err
                    );
                };
                if let Err(err) = worker_queue.update_task_status_to_complete(task_id).await {
                    error!(
                        "Error updating scheduled task status for task with id: {}\nerror: {:?}",
                        task_id, err
                    );
                };
            }
        });
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
