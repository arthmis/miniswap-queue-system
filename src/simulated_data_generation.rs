use std::{sync::Arc, time::Duration as StdDuration};

use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use chrono::{Duration, Utc};
use rand::Rng;
use serde_json::json;
use tokio::time::sleep;
use tokio_postgres::NoTls;

use crate::message::JobStatus;

pub async fn create_messages_table(
    pool: Arc<Pool<PostgresConnectionManager<NoTls>>>,
) -> Result<(), tokio_postgres::Error> {
    let client = pool.get().await.unwrap();

    // Create the messages table if it doesn't exist
    client
        .batch_execute(
            "
            DO $$ BEGIN
                CREATE TYPE job_status AS ENUM ('pending', 'in_progress', 'completed');
            EXCEPTION
                WHEN duplicate_object THEN null;
            END $$;

            CREATE INDEX IF NOT EXISTS message_status_idx
            ON messages(status, id);

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

pub async fn insert_test_messages(
    pool: Arc<Pool<PostgresConnectionManager<NoTls>>>,
    count: usize,
) -> Result<(), tokio_postgres::Error> {
    let client = pool.get().await.unwrap();
    loop {
        let payloads = {
            let mut rng = rand::rng();

            let queue_names = ["orders", "notifications", "payments", "analytics", "emails"];
            let actions = ["create", "update", "delete", "process", "sync"];
            let entities = ["user", "product", "order", "invoice", "subscription"];

            let mut payloads = Vec::new();
            for _ in 0..count {
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
                        "retry_count": 0
                    }
                });
                // Randomly decide if this task should be scheduled (50% chance)
                let scheduled_at = if rng.random_bool(0.5) {
                    // Generate scheduled_at time: 0 to 2 minutes ahead
                    let scheduled_seconds_ahead = rng.random_range(0..=120);
                    Some(Utc::now() + Duration::seconds(scheduled_seconds_ahead))
                } else {
                    None
                };

                payloads.push((queue_name, payload, priority, scheduled_at));
            }
            payloads
        };

        for (queue_name, payload, priority, scheduled_at) in payloads {
            client
                .execute(
                    "INSERT INTO messages (queue_name, payload, priority, status, scheduled_at) VALUES ($1, $2, $3, $4, $5)",
                    &[&queue_name, &payload, &priority, &JobStatus::Pending, &scheduled_at],
                )
                .await?;
        }
        sleep(StdDuration::from_secs(5)).await;
    }
}
