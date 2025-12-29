use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info};

use crate::db::Queue;
use crate::worker_run;
use std::time::Duration;

use tokio::time;

#[cfg(windows)]
pub async fn handle_failed_and_stuck_messages<T: Queue + Clone + Send + 'static>(
    task_count: u32,
    queue: T,
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
            biased;
            _ = cancellation_token.cancelled() => {
                info!("clean up stuck tasks received task cancelled");
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
            _ = schedule_stuck_tasks(task_count, queue.clone(), main_tracker.clone()) => {
                time::sleep(Duration::from_secs(20)).await;
            },
        }
    }
}

async fn schedule_stuck_tasks<T: Queue + Clone + Send + 'static>(
    task_count: u32,
    queue: T,
    task_tracker: TaskTracker,
) {
    info!("scheduling {} tasks for stuck jobs", task_count);

    for _ in 0..task_count {
        let worker_queue = queue.clone();
        task_tracker.spawn(async move {
            match worker_queue.get_failed_or_stuck_task().await {
                Ok(task) => {
                    let Some(task) = task else {
                        return;
                    };
                    let task_id = task.id();
                    if let Err(err) = worker_run(task).await {
                        error!(
                            "Error processing task with id: {}\nerror: {:?}",
                            task_id, err
                        );
                    }
                    if let Err(err) = worker_queue.update_task_status_to_complete(task_id).await {
                        error!(
                            "Error updating task status for task with id: {}\nerror: {:?}",
                            task_id, err
                        );
                    }
                }
                Err(err) => error!(
                    "Couldn't get failed or stuck task from queue\nerror: {:?}",
                    err
                ),
            }
        });
    }
}
