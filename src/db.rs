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
         OR (status = 'pending' AND created_at < NOW() - INTERVAL '5 minutes')
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

#[cfg(test)]
mod tests {
    use crate::message::TaskStatus;

    use super::*;
    use chrono::Duration;
    use chrono::Utc;
    use rand::Rng;
    use serde_json::json;
    use testcontainers_modules::{
        postgres::{self, Postgres},
        testcontainers::{ContainerAsync, runners::AsyncRunner},
    };
    use tokio_postgres::Config;

    async fn start_postgres() -> (
        ContainerAsync<Postgres>,
        Pool<PostgresConnectionManager<NoTls>>,
    ) {
        let postgres_instance_handle = postgres::Postgres::default().start().await.unwrap();

        let config = test_container_postgres_config(&postgres_instance_handle).await;
        let manager = PostgresConnectionManager::new(config.clone(), NoTls);
        let conn_pool = Pool::builder().build(manager).await.unwrap();

        let conn = conn_pool.get().await.unwrap();
        run_migrations(conn).await;

        (postgres_instance_handle, conn_pool)
    }

    async fn test_container_postgres_config(container: &ContainerAsync<Postgres>) -> Config {
        let mut config = Config::new();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        // make the assumption that the addres is localhost since this is only used in tests
        // at least for local test running, might need to change with CI
        let addr = std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1));
        config
            .user("postgres")
            .dbname("postgres")
            .password("postgres")
            .hostaddr(addr)
            .port(port)
            .connect_timeout(core::time::Duration::from_secs(10));
        config
    }

    async fn run_migrations(conn: PooledConnection<'_, PostgresConnectionManager<NoTls>>) {
        create_messages_table(conn).await.unwrap();
    }

    async fn seed_pending_tasks(conn: PooledConnection<'_, PostgresConnectionManager<NoTls>>) {
        let mut rng = rand::rng();

        let queue_names = ["orders", "notifications", "payments", "analytics", "emails"];
        let actions = ["create", "update", "delete", "process", "sync"];
        let entities = ["user", "product", "order", "invoice", "subscription"];

        let count = 2;
        let mut payloads = Vec::new();
        let scheduled_at_values = vec![None, Some(Utc::now() + Duration::seconds(40))];
        for (_, scheduled_at) in (0..count).zip(scheduled_at_values.iter()) {
            let queue_name = queue_names[rng.random_range(0..queue_names.len())];
            let action = actions[rng.random_range(0..actions.len())];
            let entity = entities[rng.random_range(0..entities.len())];
            let priority = rng.random_range(0..=4); // 0 = highest priority, 4 = lowest

            let payload = json!({
                "action": action,
                "entity": entity,
                "entity_id": rng.random_range(1000..9999),
                "amount": rng.random_range(10.0..1000.0_f64),
                "priority": priority,
                "metadata": {
                    "source": format!("system_{}", rng.random_range(1..10)),
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                }
            });
            // Randomly decide if this task should be scheduled (50% chance)
            // let scheduled_at = if rng.random_bool(0.5) {
            //     // Generate scheduled_at time: 0 to 2 minutes ahead
            //     let scheduled_seconds_ahead = rng.random_range(0..=120);
            //     Some(Utc::now() + Duration::seconds(scheduled_seconds_ahead))
            // } else {
            //     None
            // };
            // let scheduled_at = Some(Utc::now() + Duration::seconds(40));

            payloads.push((queue_name, payload, priority, scheduled_at));
        }
        for (queue_name, payload, priority, scheduled_at) in payloads {
            conn
                .execute(
                    "INSERT INTO messages (queue_name, payload, priority, status, scheduled_at) VALUES ($1, $2, $3, $4, $5)",
                    &[&queue_name, &payload, &priority, &TaskStatus::Pending, &scheduled_at],
                )
                .await.unwrap();
        }
    }

    async fn seed_pending_tasks_with_due_scheduled_tasks(
        conn: PooledConnection<'_, PostgresConnectionManager<NoTls>>,
    ) {
        let mut rng = rand::rng();

        let queue_names = ["orders", "notifications", "payments", "analytics", "emails"];
        let actions = ["create", "update", "delete", "process", "sync"];
        let entities = ["user", "product", "order", "invoice", "subscription"];

        let count = 2;
        let mut payloads = Vec::new();
        let scheduled_at_values = vec![None, Some(Utc::now() - Duration::seconds(40))];
        for (_, scheduled_at) in (0..count).zip(scheduled_at_values.iter()) {
            let queue_name = queue_names[rng.random_range(0..queue_names.len())];
            let action = actions[rng.random_range(0..actions.len())];
            let entity = entities[rng.random_range(0..entities.len())];
            let priority = rng.random_range(0..=4); // 0 = highest priority, 4 = lowest

            let payload = json!({
                "action": action,
                "entity": entity,
                "entity_id": rng.random_range(1000..9999),
                "amount": rng.random_range(10.0..1000.0_f64),
                "priority": priority,
                "metadata": {
                    "source": format!("system_{}", rng.random_range(1..10)),
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                }
            });

            payloads.push((queue_name, payload, priority, scheduled_at));
        }
        for (queue_name, payload, priority, scheduled_at) in payloads {
            conn
                .execute(
                    "INSERT INTO messages (queue_name, payload, priority, status, scheduled_at) VALUES ($1, $2, $3, $4, $5)",
                    &[&queue_name, &payload, &priority, &TaskStatus::Pending, &scheduled_at],
                )
                .await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_get_pending_task_not_scheduled() {
        let (_postgres_instance_handle, pool) = start_postgres().await;
        let conn = pool.get().await.unwrap();
        seed_pending_tasks(conn).await;

        let queue = PostgresQueue::new(pool.clone());

        let task = queue.get_newest_pending_task().await.unwrap().unwrap();
        assert_eq!(task.id(), 1);
    }

    #[tokio::test]
    async fn test_get_pending_scheduled_task_that_is_due() {
        let (_postgres_instance_handle, pool) = start_postgres().await;
        let conn = pool.get().await.unwrap();
        seed_pending_tasks_with_due_scheduled_tasks(conn).await;

        let queue = PostgresQueue::new(pool.clone());

        let task = queue.get_scheduled_task().await.unwrap().unwrap();
        assert_eq!(task.id(), 2);
    }

    #[tokio::test]
    async fn test_get_pending_task_with_future_schedule() {
        let (_postgres_instance_handle, pool) = start_postgres().await;
        let conn = pool.get().await.unwrap();
        seed_pending_tasks(conn).await;

        let queue = PostgresQueue::new(pool.clone());

        let task = queue.get_scheduled_task().await.unwrap();
        assert!(task.is_none());
    }

    pub async fn create_messages_table(
        client: PooledConnection<'_, PostgresConnectionManager<NoTls>>,
    ) -> Result<(), tokio_postgres::Error> {
        client
            .batch_execute(
                "
                DO $$ BEGIN
                    CREATE TYPE job_status AS ENUM ('pending', 'in_progress', 'completed');
                EXCEPTION
                    WHEN duplicate_object THEN null;
                END $$;

                DROP TABLE IF EXISTS messages;
                CREATE TABLE IF NOT EXISTS messages (
                    id SERIAL PRIMARY KEY,
                    queue_name TEXT NOT NULL,
                    payload JSONB,
                    priority INTEGER NOT NULL,
                    status job_status NOT NULL DEFAULT 'pending',
                    scheduled_at TIMESTAMPTZ,
                    last_started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS message_status_idx
                ON messages(status, id);

                CREATE OR REPLACE FUNCTION new_job_trigger_fn() RETURNS trigger AS $$
                BEGIN
                  PERFORM pg_notify('new_task', NEW.id::text);
                  RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;

                CREATE TRIGGER new_task_trigger
                AFTER INSERT ON messages
                FOR EACH ROW
                EXECUTE FUNCTION new_job_trigger_fn();
                ",
            )
            .await?;

        Ok(())
    }
}
