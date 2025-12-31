DO $$ BEGIN
    CREATE TYPE job_status AS ENUM ('pending', 'in_progress', 'completed');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

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

CREATE INDEX IF NOT EXISTS message_idx
ON messages(id, scheduled_at, status);

CREATE OR REPLACE FUNCTION new_job_trigger_fn() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('new_task', NEW.id::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER new_task_trigger
AFTER INSERT ON messages
FOR EACH ROW EXECUTE FUNCTION new_job_trigger_fn();
