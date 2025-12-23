use std::error::Error;

use tokio::time::Duration;
use tokio_stream::StreamExt;

/// Retries the given future using configuration provided by `[RetryStrategy]`
///
/// To further improve this, randomization needs to be done if dealing with issues like network
/// congestion or contentious resources
pub async fn retry<T, U, E>(
    strategy: RetryStrategy,
    future_producer: impl Fn() -> T,
) -> Result<U, E>
where
    T: Future<Output = Result<U, E>>,
    E: Error,
{
    let future = future_producer();
    let mut output = future.await;

    if output.is_ok() {
        return output;
    }

    let sequence = match strategy {
        RetryStrategy::ExponentialBackoff {
            max_attempts,
            duration,
        } => {
            let mut sequence = Vec::with_capacity(max_attempts as usize);
            let mut current_duration = duration;
            for _ in 0..max_attempts as usize {
                sequence.push(current_duration);
                current_duration *= 2;
            }
            sequence
        }
    };

    let mut retry_delays = tokio_stream::iter(sequence);
    while let Some(delay) = retry_delays.next().await {
        tokio::time::sleep(delay).await;
        let future = future_producer();
        output = future.await;
        if output.is_ok() {
            return output;
        }
    }

    return output;
}

pub enum RetryStrategy {
    ExponentialBackoff {
        max_attempts: u32,
        duration: Duration,
    },
}
