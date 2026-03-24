
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

impl PgDriverError {
    pub fn code(&self) -> Option<&tokio_postgres::error::SqlState> {
        self.0.code()
    }
}
