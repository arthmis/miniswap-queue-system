use std::{sync::Arc, time::Duration};

use bb8::{Pool, PooledConnection};
use bb8_postgres::PostgresConnectionManager;
use tokio_postgres::NoTls;
use tracing::error;

use crate::{
    message::TaskPayload,
    retry::{RetryStrategy, retry},
};

pub trait Queue {
    type QueueError: std::error::Error + Send;

    /// Get the newest pending task that doesn't have a scheduled time
    fn get_newest_pending_task(
        &self,
    ) -> impl Future<Output = Result<Option<TaskPayload>, Self::QueueError>> + Send;
    /// Get a task that failed before, the job system has taken too long to complete or never completed
    fn get_failed_or_stuck_task(
        &self,
    ) -> impl Future<Output = Result<Option<TaskPayload>, Self::QueueError>> + Send;
    /// Get a scheduled task that is now due
    fn get_scheduled_task(
        &self,
    ) -> impl Future<Output = Result<Option<TaskPayload>, Self::QueueError>> + Send;
    /// Update the task status
    fn update_task_status_to_complete(
        &self,
        task_id: i32,
    ) -> impl Future<Output = Result<(), Self::QueueError>> + Send;
    /// Get count of scheduled tasks that are due
    fn pending_scheduled_tasks_count(
        &self,
    ) -> impl Future<Output = Result<i64, Self::QueueError>> + Send;
}

#[derive(Clone)]
pub struct PostgresQueue {
    pool: Arc<Pool<PostgresConnectionManager<NoTls>>>,
}

impl PostgresQueue {
    pub fn new(pool: Pool<PostgresConnectionManager<NoTls>>) -> Self {
        Self {
            pool: Arc::new(pool),
        }
    }

    async fn get_connection(
        &self,
    ) -> Result<PooledConnection<'_, PostgresConnectionManager<NoTls>>, PostgresQueueError> {
        let client = retry(
            RetryStrategy::ExponentialBackoff {
                max_attempts: 3,
                duration: Duration::from_millis(50),
            },
            || self.pool.get(),
        )
        .await;
        let connection = client.map_err(|err| {
            error!("error getting client connection to database: {:?}", err);
            PostgresQueueError::Pool(err)
        })?;
        Ok(connection)
    }
}

impl Queue for PostgresQueue {
    type QueueError = PostgresQueueError;

    async fn get_newest_pending_task(&self) -> Result<Option<TaskPayload>, Self::QueueError> {
        let statement = "WITH task AS (
            SELECT * FROM messages
            WHERE status = 'pending' AND scheduled_at IS NULL
            ORDER BY priority ASC, created_at DESC
            FOR UPDATE SKIP LOCKED LIMIT 1
            )
            UPDATE messages SET status = 'in_progress',
            last_started_at = NOW()
            FROM task
            WHERE messages.id = task.id RETURNING task.*";

        let client = retry(
            RetryStrategy::ExponentialBackoff {
                max_attempts: 3,
                duration: Duration::from_millis(50),
            },
            || self.pool.get(),
        )
        .await;
        let mut connection = client.map_err(|err| {
            error!("error getting client connection to database: {:?}", err);
            PostgresQueueError::Pool(err)
        })?;

        let transaction = connection.transaction().await?;
        let result = transaction.query_opt(statement, &[]).await;

        let Some(row) = result? else {
            return Ok(None);
        };

        let payload = TaskPayload::try_from(row)
            .map_err(|e| PostgresQueueError::PayloadConversion(format!("{:?}", e)))?;

        transaction.commit().await?;

        Ok(Some(payload))
    }

    async fn get_failed_or_stuck_task(&self) -> Result<Option<TaskPayload>, Self::QueueError> {
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

        let mut connection = self.get_connection().await?;
        let transaction = connection.transaction().await?;
        let result = transaction.query_opt(statement, &[]).await;

        let Some(row) = result? else {
            return Ok(None);
        };

        let payload = TaskPayload::try_from(row)
            .map_err(|e| PostgresQueueError::PayloadConversion(format!("{:?}", e)))?;

        transaction.commit().await?;

        Ok(Some(payload))
    }

    async fn get_scheduled_task(&self) -> Result<Option<TaskPayload>, Self::QueueError> {
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

        let mut connection = self.get_connection().await?;
        let transaction = connection.transaction().await?;
        let result = transaction.query_opt(statement, &[]).await;

        let Some(row) = result? else {
            return Ok(None);
        };

        let payload = TaskPayload::try_from(row)
            .map_err(|e| PostgresQueueError::PayloadConversion(format!("{:?}", e)))?;

        transaction.commit().await?;

        Ok(Some(payload))
    }

    async fn update_task_status_to_complete(&self, task_id: i32) -> Result<(), Self::QueueError> {
        let statement = "UPDATE messages SET status = 'completed' WHERE id = $1";

        let mut connection = self.get_connection().await?;
        let transaction = connection.transaction().await?;
        let _status_update_result = transaction.execute(statement, &[&task_id]).await?;

        transaction.commit().await?;
        Ok(())
    }

    async fn pending_scheduled_tasks_count(&self) -> Result<i64, Self::QueueError> {
        let statement = "SELECT COUNT(*) FROM messages
            WHERE scheduled_at IS NOT NULL AND status = 'pending' AND scheduled_at <= NOW()";

        let connection = self.get_connection().await?;
        let row = connection.query_one(statement, &[]).await.map_err(|err| {
            error!("Failed to query pending scheduled task count: {:?}", err);
            return err;
        })?;

        let pending_count = row.get(0);
        Ok(pending_count)
    }
}

#[derive(Debug)]
pub enum PostgresQueueError {
    Database(tokio_postgres::Error),
    Pool(bb8::RunError<tokio_postgres::Error>),
    PayloadConversion(String),
}

impl std::fmt::Display for PostgresQueueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PostgresQueueError::Database(err) => write!(f, "Database error: {}", err),
            PostgresQueueError::Pool(err) => write!(f, "Connection pool error: {}", err),
            PostgresQueueError::PayloadConversion(err) => {
                write!(f, "Payload conversion error: {}", err)
            }
        }
    }
}

impl std::error::Error for PostgresQueueError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PostgresQueueError::Database(err) => Some(err),
            PostgresQueueError::Pool(err) => Some(err),
            PostgresQueueError::PayloadConversion(_) => None,
        }
    }
}

impl From<tokio_postgres::Error> for PostgresQueueError {
    fn from(err: tokio_postgres::Error) -> Self {
        PostgresQueueError::Database(err)
    }
}

impl From<bb8::RunError<tokio_postgres::Error>> for PostgresQueueError {
    fn from(err: bb8::RunError<tokio_postgres::Error>) -> Self {
        PostgresQueueError::Pool(err)
    }
}
