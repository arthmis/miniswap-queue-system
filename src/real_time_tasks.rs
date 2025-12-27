#[cfg(windows)]
use std::sync::Arc;

#[cfg(windows)]
use bb8::Pool;
#[cfg(windows)]
use bb8_postgres::PostgresConnectionManager;
#[cfg(windows)]
use futures::channel::mpsc::UnboundedReceiver;
#[cfg(windows)]
use tokio_postgres::NoTls;
#[cfg(windows)]
use tokio_util::{sync::CancellationToken, task::TaskTracker};

#[cfg(windows)]
pub async fn handle_messages(
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
        use std::time::Duration;

        use tokio_stream::StreamExt;
        use tracing::{debug, error, info};

        use crate::{
            retry::{RetryStrategy, retry},
            worker_run,
        };

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
