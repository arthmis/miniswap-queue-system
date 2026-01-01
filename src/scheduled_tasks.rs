use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};

use crate::db::Queue;
use crate::{db::PostgresQueueError, message::TaskStatus, worker_run};
use tokio::time;


#[cfg(unix)]
pub async fn handle_scheduled_tasks<T>(
    task_count: u32,
    queue: T,
    main_tracker: TaskTracker,
    cancellation_token: CancellationToken,
) where
    T: Queue + Clone + Send + 'static,
    PostgresQueueError: From<<T as Queue>::QueueError>,
{
    use tokio::signal::unix::{signal, SignalKind};

    let mut signal_terminate =
        signal(SignalKind::terminate()).expect("signal terminate to be available");
    let mut signal_interrupt =
        signal(SignalKind::interrupt()).expect("signal interrupt to be available");


    let batch_tracker = TaskTracker::new();
    let signal_tracker_handle = main_tracker.clone();
    loop {
        tokio::select! {
            biased;
            _ = cancellation_token.cancelled() => {
                warn!("scheduled tasks received task cancelled");
                batch_tracker.close();
                batch_tracker.wait().await;
                break;
            },
            _ = signal_terminate.recv() => {
                info!("received terminate signal");
                cancellation_token.cancel();
                signal_tracker_handle.close();
            },
            _ = signal_interrupt.recv() => {
                info!("received interrupt signal");
                cancellation_token.cancel();
                signal_tracker_handle.close();
            },
            result = schedule_tasks(task_count, queue.clone(), batch_tracker.clone()) => {
                if let Err(err) = result {
                    error!("Error handling scheduled tasks\nerror: {:?}", err);
                }

                let pending_scheduled_tasks_count = match queue.pending_scheduled_tasks_count().await {
                    Ok(count) => count,
                    Err(err) => {
                        error!("Error handling scheduled tasks\nerror: {:?}", err);
                        continue;
                    }
                };

                if pending_scheduled_tasks_count == 0 {
                    let sleep_length = Duration::from_secs(20);
                    info!("All scheduled tasks complete, sleeping for {:?}", sleep_length);
                    time::sleep(sleep_length).await;
                }
            },
        }
    }
}

#[cfg(windows)]
pub async fn handle_scheduled_tasks<T>(
    task_count: u32,
    queue: T,
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
    let signal_tracker_handle = main_tracker.clone();
    loop {
        tokio::select! {
            biased;
            _ = cancellation_token.cancelled() => {
                warn!("scheduled tasks received task cancelled");
                batch_tracker.close();
                batch_tracker.wait().await;
                break;
            },
            _ = signal_c.recv() => {
                warn!("received ctrl c signal");
                cancellation_token.cancel();
                signal_tracker_handle.close();
            },
            _ = signal_break.recv() => {
                warn!("received ctrl break signal");
                cancellation_token.cancel();
                signal_tracker_handle.close();
            },
            _ = signal_close.recv() => {
                warn!("received ctrl close signal");
                cancellation_token.cancel();
                signal_tracker_handle.close();
            },
            _ = signal_shutdown.recv() => {
                warn!("received ctrl shutdown signal");
                cancellation_token.cancel();
                signal_tracker_handle.close();
            },
            result = schedule_tasks(task_count, queue.clone(), batch_tracker.clone()) => {
                if let Err(err) = result {
                    error!("Error handling scheduled tasks\nerror: {:?}", err);
                }

                let pending_scheduled_tasks_count = match queue.pending_scheduled_tasks_count().await {
                    Ok(count) => count,
                    Err(err) => {
                        error!("Error handling scheduled tasks\nerror: {:?}", err);
                        continue;
                    }
                };

                if pending_scheduled_tasks_count == 0 {
                    let sleep_length = Duration::from_secs(20);
                    info!("All scheduled tasks complete, sleeping for {:?}", sleep_length);
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
) -> Result<(), PostgresQueueError>
where
    T: Queue + Clone + Send + 'static,
    PostgresQueueError: From<<T as Queue>::QueueError>,
{
    let pending_scheduled_tasks_count = queue.pending_scheduled_tasks_count().await?;
    if pending_scheduled_tasks_count == 0 {
        return Ok(());
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
                if let Err(err) = worker_queue
                    .update_task_status(task_id, TaskStatus::Completed)
                    .await
                {
                    error!(
                        "Error updating scheduled task status for task with id: {}\nerror: {:?}",
                        task_id, err
                    );
                };
            }
        });
    }

    batch_tracker.close();
    batch_tracker.wait().await;

    Ok(())
}
