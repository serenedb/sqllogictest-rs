
#[derive(Debug)]
pub struct PgDriverError(String);

impl PgDriverError {
    pub(crate) fn connection_closed() -> Self {
        Self("connection is closed".into())
    }
}

impl std::fmt::Display for PgDriverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for PgDriverError {}

impl From<tokio_postgres::Error> for PgDriverError {
    fn from(e: tokio_postgres::Error) -> Self {
        use std::error::Error;
        let msg = if let Some(cause) = e.source() {
            format!("{}: {}", e, cause)
        } else {
            e.to_string()
        };
        Self(msg)
    }
}
