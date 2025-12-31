use postgres_types::ToSql;
use tokio_postgres::Row;
use tokio_postgres::types::FromSql;

#[derive(Debug)]
pub struct TaskPayload {
    id: i32,
    payload: serde_json::Value,
}

impl TaskPayload {
    pub fn id(&self) -> i32 {
        self.id
    }

    pub fn payload(&self) -> &serde_json::Value {
        &self.payload
    }
}

impl TryFrom<Row> for TaskPayload {
    type Error = tokio_postgres::Error;

    fn try_from(value: Row) -> Result<Self, Self::Error> {
        let id = value.try_get("id")?;
        let payload = value.try_get("payload")?;

        Ok(Self { id, payload })
    }
}

#[derive(Copy, Clone, Debug, ToSql, FromSql, Eq, PartialEq)]
#[postgres(name = "job_status")]
pub enum TaskStatus {
    #[postgres(name = "pending")]
    Pending,
    #[postgres(name = "in_progress")]
    InProgress,
    #[postgres(name = "completed")]
    Completed,
}
