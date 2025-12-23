use postgres_types::ToSql;
use tokio_postgres::Row;
use tokio_postgres::types::FromSql;

pub struct MessagePayload {
    id: i32,
    payload: serde_json::Value,
}

impl MessagePayload {
    pub fn id(&self) -> i32 {
        self.id
    }

    pub fn payload(&self) -> &serde_json::Value {
        &self.payload
    }
}

impl TryFrom<Row> for MessagePayload {
    type Error = tokio_postgres::Error;

    fn try_from(value: Row) -> Result<Self, Self::Error> {
        let id = value.try_get("id")?;
        let payload = value.try_get("payload")?;

        Ok(Self { id, payload })
    }
}

#[derive(Copy, Clone, Debug, ToSql, FromSql)]
#[postgres(name = "job_status")]
pub enum JobStatus {
    #[postgres(name = "pending")]
    Pending,
    #[postgres(name = "in_progress")]
    InProgress,
    #[postgres(name = "completed")]
    Completed,
}
