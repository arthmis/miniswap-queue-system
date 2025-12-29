use std::sync::Arc;

use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::info;

use crate::{errors::WorkerError, worker_run};

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
            _ = schedule_stuck_jobs(task_count, pool.clone(), main_tracker.clone()) => {
                time::sleep(Duration::from_secs(20)).await;
            },
        }
    }
}

async fn schedule_stuck_jobs(
    task_count: u32,
    pool: Arc<Pool<PostgresConnectionManager<NoTls>>>,
    task_tracker: TaskTracker,
) -> Result<(), WorkerError> {
    info!("scheduling {} tasks for stuck jobs", task_count);

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

    for _ in 0..task_count {
        let cloned_pool = pool.clone();
        task_tracker.spawn(worker_run(cloned_pool, statement));
    }
    Ok(())
}
