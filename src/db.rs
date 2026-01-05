use std::{sync::Arc, time::Duration};

use bb8::{Pool, PooledConnection};
use bb8_postgres::PostgresConnectionManager;
use tokio_postgres::NoTls;
use tracing::error;

use crate::{
    message::{TaskPayload, TaskStatus},
    retry::{RetryStrategy, retry},
};

pub trait Queue {
    type QueueError: std::error::Error + Send;


    /// Gets an unscheduled task by its id and marks it as in progress/being processed
    fn get_task_with_id_for_processing(
        &self,
        id: i32,
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
    fn update_task_status(
        &self,
        task_id: i32,
        new_status: TaskStatus,
    ) -> impl Future<Output = Result<(), Self::QueueError>> + Send;
    /// Get count of scheduled tasks that are due
    fn pending_scheduled_tasks_count(
        &self,
    ) -> impl Future<Output = Result<i64, Self::QueueError>> + Send;
    /// Get count of stuck tasks that were never handled or never completed
    fn get_failed_or_stuck_task_count(
        &self,
    ) -> impl Future<Output = Result<i64, Self::QueueError>> + Send;
    // Questions:
    // Do you need to retain these tasks for audits or anything like that
    // When would you want to delete this data or archive it to keep the disk usage compact?
    /// Delete all completed tasks
    fn delete_completed_tasks(&self) -> impl Future<Output = Result<u64, Self::QueueError>> + Send;
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

    async fn get_task_with_id_for_processing(
        &self,
        id: i32,
    ) -> Result<Option<TaskPayload>, Self::QueueError>  {
        let statement = "WITH task AS (
            SELECT * FROM messages
            WHERE id = $1 AND scheduled_at IS NULL
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
        let result = transaction.query_opt(statement, &[&id]).await;

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
            WHERE
                (status = 'in_progress' AND last_started_at IS NOT NULL AND last_started_at < NOW() - INTERVAL '5 minutes')
                OR (status = 'pending' AND scheduled_at IS NOT NULL AND scheduled_at < NOW() - INTERVAL '5 minutes')
                OR (status = 'pending' AND scheduled_at IS NULL AND created_at < NOW() - INTERVAL '5 minutes')
            ORDER BY priority, scheduled_at, created_at, id
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

    async fn update_task_status(
        &self,
        task_id: i32,
        new_status: TaskStatus,
    ) -> Result<(), Self::QueueError> {
        let statement = "UPDATE messages SET status = $2 WHERE id = $1";

        let mut connection = self.get_connection().await?;
        let transaction = connection.transaction().await?;
        let _status_update_result = transaction
            .execute(statement, &[&task_id, &new_status])
            .await?;

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

    async fn delete_completed_tasks(&self) -> Result<u64, Self::QueueError> {
        let statement = "DELETE FROM messages WHERE status = 'completed'";

        let mut connection = self.get_connection().await?;
        let transaction = connection.transaction().await?;
        let count_affected_rows = transaction.execute(statement, &[]).await.map_err(|err| {
            error!("Failed to delete completed tasks\nerror: {:?}", err);
            return err;
        })?;

        transaction.commit().await?;

        Ok(count_affected_rows)
    }

    async fn get_failed_or_stuck_task_count(
        &self,
    ) -> Result<i64, Self::QueueError> {

        let statement = "
            SELECT COUNT(*) FROM messages
            WHERE
                (status = 'in_progress' AND last_started_at IS NOT NULL AND last_started_at < NOW() - INTERVAL '5 minutes')
                OR (status = 'pending' AND scheduled_at IS NOT NULL AND scheduled_at < NOW() - INTERVAL '5 minutes')
                OR (status = 'pending' AND scheduled_at IS NULL AND created_at < NOW() - INTERVAL '5 minutes')
        ";

        let connection = self.get_connection().await?;
        let row = connection.query_one(statement, &[]).await.map_err(|err| {
            error!("Failed to query pending scheduled task count: {:?}", err);
            return err;
        })?;

        let count = row.get(0);
        Ok(count)
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

    struct TaskGenConfig {
        status: Option<TaskStatus>,
        priority: Option<i32>,
        scheduled_at: Option<chrono::DateTime<Utc>>,
        last_started_at: Option<chrono::DateTime<Utc>>,
        created_at: Option<chrono::DateTime<Utc>>,
    }

    async fn create_task(
        config: TaskGenConfig,
        conn: &PooledConnection<'_, PostgresConnectionManager<NoTls>>,
    ) {
        let mut rng = rand::rng();

        let queue_names = ["orders", "notifications", "payments", "analytics", "emails"];
        let actions = ["create", "update", "delete", "process", "sync"];
        let entities = ["user", "product", "order", "invoice", "subscription"];

        let queue_name = queue_names[rng.random_range(0..queue_names.len())];
        let action = actions[rng.random_range(0..actions.len())];
        let entity = entities[rng.random_range(0..entities.len())];

        let payload = json!({
            "action": action,
            "entity": entity,
            "entity_id": rng.random_range(1000..9999),
            "amount": rng.random_range(10.0..1000.0_f64),
            "metadata": {
                "source": format!("system_{}", rng.random_range(1..10)),
                "timestamp": chrono::Utc::now().to_rfc3339(),
            }
        });

        let scheduled_at = config.scheduled_at;
        let last_started_at = config.last_started_at;
        let status = config.status.unwrap_or(TaskStatus::Pending);
        let priority = config.priority.unwrap_or(0);
        let created_at = config.created_at.unwrap_or_else(Utc::now);

        conn
            .execute(
                "INSERT INTO messages (queue_name, payload, priority, status, scheduled_at, last_started_at, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                &[&queue_name, &payload, &priority, &status, &scheduled_at, &last_started_at, &created_at],
            )
            .await.unwrap();
    }

    #[tokio::test]
    async fn test_get_task_by_id_and_not_scheduled() {
        let (_postgres_instance_handle, pool) = start_postgres().await;
        let conn = pool.get().await.unwrap();
        let task_parameters: Vec<TaskGenConfig> = vec![
            TaskGenConfig {
                status: Some(TaskStatus::Pending),
                priority: None,
                scheduled_at: None,
                last_started_at: None,
                created_at: None,
            },
            TaskGenConfig {
                status: Some(TaskStatus::Pending),
                priority: None,
                scheduled_at: Some(Utc::now() + Duration::seconds(40)),
                last_started_at: None,
                created_at: None,
            },
        ];

        for config in task_parameters {
            create_task(config, &conn).await;
        }

        let queue = PostgresQueue::new(pool.clone());

        let task = queue.get_task_with_id_for_processing(1).await.unwrap().unwrap();
        assert_eq!(task.id(), 1);

        let task = queue.get_task_with_id_for_processing(2).await.unwrap();
        assert!(task.is_none());
    }

    #[tokio::test]
    async fn test_get_pending_scheduled_task_that_is_due() {
        let (_postgres_instance_handle, pool) = start_postgres().await;
        let conn = pool.get().await.unwrap();
        let task_parameters: Vec<TaskGenConfig> = vec![
            TaskGenConfig {
                status: Some(TaskStatus::Pending),
                priority: None,
                scheduled_at: None,
                last_started_at: None,
                created_at: None,
            },
            TaskGenConfig {
                status: Some(TaskStatus::Pending),
                priority: None,
                scheduled_at: Some(Utc::now() - Duration::seconds(40)),
                last_started_at: None,
                created_at: None,
            },
        ];

        for config in task_parameters {
            create_task(config, &conn).await;
        }

        let queue = PostgresQueue::new(pool.clone());

        let task = queue.get_scheduled_task().await.unwrap().unwrap();
        assert_eq!(task.id(), 2);
    }

    #[tokio::test]
    async fn test_get_pending_task_with_future_schedule() {
        let (_postgres_instance_handle, pool) = start_postgres().await;
        let conn = pool.get().await.unwrap();
        let task_parameters: Vec<TaskGenConfig> = vec![
            TaskGenConfig {
                status: Some(TaskStatus::Pending),
                priority: None,
                scheduled_at: None,
                last_started_at: None,
                created_at: None,
            },
            TaskGenConfig {
                status: Some(TaskStatus::Pending),
                priority: None,
                scheduled_at: Some(Utc::now() + Duration::seconds(40)),
                last_started_at: None,
                created_at: None,
            },
        ];

        for config in task_parameters {
            create_task(config, &conn).await;
        }

        let queue = PostgresQueue::new(pool.clone());

        let task = queue.get_scheduled_task().await.unwrap();
        assert!(task.is_none());
    }

    #[tokio::test]
    async fn test_get_stuck_task_while_there_are_none() {
        let (_postgres_instance_handle, pool) = start_postgres().await;
        let conn = pool.get().await.unwrap();
        let task_parameters: Vec<TaskGenConfig> = vec![
            TaskGenConfig {
                status: Some(TaskStatus::Completed),
                priority: None,
                scheduled_at: None,
                last_started_at: None,
                created_at: None,
            },
            TaskGenConfig {
                status: Some(TaskStatus::Pending),
                priority: None,
                scheduled_at: None,
                last_started_at: None,
                created_at: None,
            },
            TaskGenConfig {
                status: Some(TaskStatus::Pending),
                priority: None,
                scheduled_at: Some(Utc::now() + Duration::seconds(150)),
                last_started_at: None,
                created_at: None,
            },
            TaskGenConfig {
                status: Some(TaskStatus::InProgress),
                priority: None,
                scheduled_at: Some(Utc::now() - Duration::seconds(150)),
                last_started_at: Some(Utc::now() - Duration::seconds(140)),
                created_at: Some(Utc::now() - Duration::seconds(200)),
            },
        ];

        for config in task_parameters {
            create_task(config, &conn).await;
        }

        let queue = PostgresQueue::new(pool.clone());

        let task = queue.get_failed_or_stuck_task().await.unwrap();
        assert!(task.is_none());
    }

    #[tokio::test]
    async fn test_get_stuck_task_while_there_are_some() {
        let (_postgres_instance_handle, pool) = start_postgres().await;
        let conn = pool.get().await.unwrap();
        let task_parameters: Vec<TaskGenConfig> = vec![
            TaskGenConfig {
                status: Some(TaskStatus::Completed),
                priority: None,
                scheduled_at: None,
                last_started_at: None,
                created_at: None,
            },
            TaskGenConfig {
                status: Some(TaskStatus::InProgress),
                priority: None,
                scheduled_at: None,
                last_started_at: None,
                created_at: None,
            },
            TaskGenConfig {
                status: Some(TaskStatus::Pending),
                priority: None,
                scheduled_at: Some(Utc::now() - Duration::seconds(400)),
                last_started_at: None,
                created_at: Some(Utc::now() - Duration::seconds(600)),
            },
        ];

        for config in task_parameters {
            create_task(config, &conn).await;
        }

        let queue = PostgresQueue::new(pool.clone());

        let task = queue.get_failed_or_stuck_task().await.unwrap();
        assert!(task.is_some());
        let task = task.unwrap();
        assert_eq!(task.id(), 3);
    }

    #[tokio::test]
    async fn test_get_stuck_tasks_with_scheduled_task_in_order() {
        let (_postgres_instance_handle, pool) = start_postgres().await;
        let conn = pool.get().await.unwrap();
        let now = Utc::now();
        let task_parameters: Vec<TaskGenConfig> = vec![
            TaskGenConfig {
                status: Some(TaskStatus::Completed),
                priority: None,
                scheduled_at: None,
                last_started_at: None,
                created_at: None,
            },
            TaskGenConfig {
                status: Some(TaskStatus::InProgress),
                priority: None,
                scheduled_at: None,
                last_started_at: Some(now - Duration::seconds(348)),
                created_at: Some(now - Duration::seconds(400)),
            },
            TaskGenConfig {
                status: Some(TaskStatus::Pending),
                priority: None,
                scheduled_at: Some(now - Duration::seconds(400)),
                last_started_at: None,
                created_at: Some(now - Duration::seconds(600)),
            },
        ];

        for config in task_parameters {
            create_task(config, &conn).await;
        }

        let queue = PostgresQueue::new(pool.clone());

        let task = queue.get_failed_or_stuck_task().await.unwrap().unwrap();
        assert_eq!(task.id(), 3);
        queue
            .update_task_status(task.id(), TaskStatus::Completed)
            .await
            .unwrap();

        let task = queue.get_failed_or_stuck_task().await.unwrap().unwrap();
        assert_eq!(task.id(), 2);
    }

    #[tokio::test]
    async fn test_get_stuck_in_progress_scheduled_task() {
        let (_postgres_instance_handle, pool) = start_postgres().await;
        let conn = pool.get().await.unwrap();
        let now = Utc::now();
        let task_parameters: Vec<TaskGenConfig> = vec![TaskGenConfig {
            status: Some(TaskStatus::InProgress),
            priority: None,
            scheduled_at: Some(now - Duration::seconds(400)),
            last_started_at: Some(now - Duration::seconds(401)),
            created_at: Some(now - Duration::seconds(600)),
        }];

        for config in task_parameters {
            create_task(config, &conn).await;
        }

        let queue = PostgresQueue::new(pool.clone());

        let task = queue.get_failed_or_stuck_task().await.unwrap().unwrap();
        assert_eq!(task.id(), 1);
    }

    #[tokio::test]
    async fn test_update_task_status() {
        let (_postgres_instance_handle, pool) = start_postgres().await;
        let conn = pool.get().await.unwrap();
        let now = Utc::now();
        let task_parameters: Vec<TaskGenConfig> = vec![
            TaskGenConfig {
                status: Some(TaskStatus::InProgress),
                priority: None,
                scheduled_at: Some(now - Duration::seconds(400)),
                last_started_at: Some(now - Duration::seconds(401)),
                created_at: Some(now - Duration::seconds(600)),
            },
            TaskGenConfig {
                status: Some(TaskStatus::Pending),
                priority: None,
                scheduled_at: None,
                last_started_at: None,
                created_at: Some(now - Duration::seconds(600)),
            },
            TaskGenConfig {
                status: Some(TaskStatus::InProgress),
                priority: None,
                scheduled_at: None,
                last_started_at: Some(now - Duration::seconds(500)),
                created_at: Some(now - Duration::seconds(600)),
            },
            TaskGenConfig {
                status: Some(TaskStatus::Completed),
                priority: None,
                scheduled_at: None,
                last_started_at: Some(now - Duration::seconds(500)),
                created_at: Some(now - Duration::seconds(600)),
            },
        ];

        for config in task_parameters {
            create_task(config, &conn).await;
        }

        let queue = PostgresQueue::new(pool.clone());

        queue
            .update_task_status(1, TaskStatus::Completed)
            .await
            .unwrap();
        queue
            .update_task_status(2, TaskStatus::InProgress)
            .await
            .unwrap();
        queue
            .update_task_status(3, TaskStatus::Pending)
            .await
            .unwrap();
        queue
            .update_task_status(4, TaskStatus::Completed)
            .await
            .unwrap();

        let row = conn
            .query_one("SELECT status FROM messages WHERE id = 1", &[])
            .await
            .unwrap();
        let stored_status: TaskStatus = row.get("status");
        assert_eq!(stored_status, TaskStatus::Completed);

        let row = conn
            .query_one("SELECT status FROM messages WHERE id = 2", &[])
            .await
            .unwrap();
        let stored_status: TaskStatus = row.get("status");
        assert_eq!(stored_status, TaskStatus::InProgress);

        let row = conn
            .query_one("SELECT status FROM messages WHERE id = 3", &[])
            .await
            .unwrap();
        let stored_status: TaskStatus = row.get("status");
        assert_eq!(stored_status, TaskStatus::Pending);

        let row = conn
            .query_one("SELECT status FROM messages WHERE id = 4", &[])
            .await
            .unwrap();
        let stored_status: TaskStatus = row.get("status");
        assert_eq!(stored_status, TaskStatus::Completed);
    }

    #[tokio::test]
    async fn test_delete_completed_tasks() {
        let (_postgres_instance_handle, pool) = start_postgres().await;
        let conn = pool.get().await.unwrap();
        let now = Utc::now();
        let task_parameters: Vec<TaskGenConfig> = vec![
            TaskGenConfig {
                status: Some(TaskStatus::InProgress),
                priority: None,
                scheduled_at: Some(now - Duration::seconds(400)),
                last_started_at: Some(now - Duration::seconds(401)),
                created_at: Some(now - Duration::seconds(600)),
            },
            TaskGenConfig {
                status: Some(TaskStatus::Pending),
                priority: None,
                scheduled_at: None,
                last_started_at: None,
                created_at: Some(now - Duration::seconds(600)),
            },
            TaskGenConfig {
                status: Some(TaskStatus::InProgress),
                priority: None,
                scheduled_at: None,
                last_started_at: Some(now - Duration::seconds(500)),
                created_at: Some(now - Duration::seconds(600)),
            },
            TaskGenConfig {
                status: Some(TaskStatus::Completed),
                priority: None,
                scheduled_at: None,
                last_started_at: Some(now - Duration::seconds(500)),
                created_at: Some(now - Duration::seconds(600)),
            },
        ];

        for config in task_parameters {
            create_task(config, &conn).await;
        }

        let queue = PostgresQueue::new(pool.clone());

        let count_deleted = queue.delete_completed_tasks().await.unwrap();

        assert_eq!(count_deleted, 1);
    }

    async fn create_messages_table(
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
                    last_started_at TIMESTAMPTZ,
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
