use std::error::Error;

#[derive(Debug)]
pub enum PgDriverError {
    /// Error from the underlying tokio_postgres driver.
    Postgres(tokio_postgres::Error),
    /// Internal driver error (e.g. connection already shut down).
    Internal(String),
}

impl PgDriverError {
    pub(crate) fn connection_closed() -> Self {
        Self::Internal("connection is closed".into())
    }

    /// Returns the SQL state code if this is a Postgres error.
    pub fn code(&self) -> Option<&tokio_postgres::error::SqlState> {
        match self {
            Self::Postgres(e) => e.code(),
            Self::Internal(_) => None,
        }
    }
}

impl std::fmt::Display for PgDriverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Postgres(e) => {
                write!(f, "{}", e)?;
                if let Some(cause) = e.source() {
                    write!(f, ": {}", cause)?;
                }
                Ok(())
            }
            Self::Internal(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for PgDriverError {}

impl From<tokio_postgres::Error> for PgDriverError {
    fn from(value: tokio_postgres::Error) -> Self {
        Self::Postgres(value)
    }
}
