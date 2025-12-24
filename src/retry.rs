use std::{
    error::Error,
    pin::Pin,
    task::{Context, Poll},
};

use pin_project_lite::pin_project;
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
use tokio::time::Sleep;
pin_project! {
    pub struct Retry<F, Fut, T, E>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        producer: F,
        #[pin]
        current_future: Fut,
        #[pin]
        delay: Option<Sleep>,
        strategy: RetryStrategy,
        attempts: u32,
        max_attempts: u32,
        current_delay: Duration,
        last_error: Option<E>,
    }
}

impl<F, Fut, T, E> Retry<F, Fut, T, E>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: Error,
{
    pub fn new(producer: F, strategy: RetryStrategy) -> Self {
        let (max_attempts, initial_delay) = match &strategy {
            RetryStrategy::ExponentialBackoff {
                max_attempts,
                duration,
            } => (*max_attempts, *duration),
        };

        let current_future = producer();

        Self {
            producer,
            current_future,
            delay: None,
            strategy,
            attempts: 0,
            max_attempts,
            current_delay: initial_delay,
            last_error: None,
        }
    }
}

impl<F, Fut, T, E> Future for Retry<F, Fut, T, E>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: Error,
{
    type Output = Result<T, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        loop {
            // If we're in a delay, wait for it to complete
            if let Some(delay) = this.delay.as_mut().as_pin_mut() {
                match delay.poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(()) => {
                        this.delay.set(None);
                        // Create a new future for the retry
                        let new_future = (this.producer)();
                        this.current_future.set(new_future);
                    }
                }
            }

            // Poll the current future
            match this.current_future.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(value)) => return Poll::Ready(Ok(value)),
                Poll::Ready(Err(e)) => {
                    *this.attempts += 1;

                    // Check if we've exhausted retries
                    if *this.attempts >= *this.max_attempts {
                        return Poll::Ready(Err(e));
                    }

                    // Set up delay for next retry
                    *this.last_error = Some(e);
                    let sleep = tokio::time::sleep(*this.current_delay);
                    this.delay.set(Some(sleep));

                    // Update delay for next time (exponential backoff)
                    *this.current_delay *= 2;
                }
            }
        }
    }
}

pub trait Retryable: Sized {
    type Output;

    fn retry_if_error(self, strategy: RetryStrategy) -> Self::Output;
}

impl<F, Fut, T, E> Retryable for F
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: Error,
{
    type Output = Retry<F, Fut, T, E>;

    fn retry_if_error(self, strategy: RetryStrategy) -> Self::Output {
        Retry::new(self, strategy)
    }
}
