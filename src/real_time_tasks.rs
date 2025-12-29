use futures::channel::mpsc::UnboundedReceiver;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use tokio_stream::StreamExt;
use tracing::error;
use tracing::{debug, info};

use crate::db::Queue;
use crate::worker_run;

#[cfg(windows)]
pub async fn handle_tasks_in_real_time<T: Queue + Clone + Send + 'static>(
    queue: T,
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
            biased;
            _ = cancellation_token.cancelled() => {
                info!("real time tasks received task cancelled");
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
            message = receiver.next() => {
                match message {
                    Some(tokio_postgres::AsyncMessage::Notification(_notification_message)) => {
                        let worker_queue = queue.clone();

                        main_tracker.spawn(async move {
                            if let Ok(Some(task)) = worker_queue.get_newest_pending_task().await {
                                let task_id = task.id();
                                if let Err(err) = worker_run(task).await {
                                    error!("Error processing task with id: {}\nerror: {:?}", task_id, err);
                                };
                                if let Err(err) = worker_queue.update_task_status_to_complete(task_id).await {
                                    error!("Error updating task status for task with id: {}\nerror: {:?}", task_id, err);
                                };
                            }
                        });
                    },
                    Some(n) => debug!("{:?}", n),
                    _ => info!("No notification sent."),
                };
            },
        }
    }
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
