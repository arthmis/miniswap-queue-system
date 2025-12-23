use std::fmt::Display;

#[derive(Debug)]
pub enum MessageProcessingError {
    Fail,
}

impl Display for MessageProcessingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let val: &str = match self {
            MessageProcessingError::Fail => "Couldn't process the message.",
        };

        f.write_str(val)
    }
}

impl std::error::Error for MessageProcessingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

#[derive(Debug)]
pub enum WorkerError {
    Database(tokio_postgres::Error),
    MessageProcessing(MessageProcessingError),
}

impl Display for WorkerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let val = match self {
            WorkerError::Database(error) => error.to_string(),
            WorkerError::MessageProcessing(error) => error.to_string(),
        };

        f.write_str(&val)
    }
}

impl std::error::Error for WorkerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WorkerError::Database(error) => error.source(),
            WorkerError::MessageProcessing(error) => error.source(),
        }
    }
}

impl From<tokio_postgres::Error> for WorkerError {
    fn from(value: tokio_postgres::Error) -> Self {
        WorkerError::Database(value)
    }
}

impl From<MessageProcessingError> for WorkerError {
    fn from(value: MessageProcessingError) -> Self {
        WorkerError::MessageProcessing(value)
    }
}
