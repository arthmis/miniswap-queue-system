use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info};

use crate::db::Queue;
use std::time::Duration;

use tokio::time;

#[cfg(windows)]
pub async fn periodically_delete_completed_tasks<T: Queue + Clone + Send + 'static>(
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
                info!("delete completed tasks received cancelled signal");
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
            _ = delete_completed_tasks(queue.clone(), main_tracker.clone()) => {
                time::sleep(Duration::from_secs(60)).await;
            },
        }
    }
}

async fn delete_completed_tasks<T: Queue + Clone + Send + 'static>(
    queue: T,
    task_tracker: TaskTracker,
) {
    info!("Deleting completed tasks.");

    task_tracker.spawn(async move {
        match queue.delete_completed_tasks().await {
            Ok(count) => info!("Deleted {count} completed tasks"),
            Err(err) => error!("Could not delete completed tasks\nerror: {:?}", err),
        }
    });
}
