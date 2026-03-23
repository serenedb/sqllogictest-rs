use std::path::Path;

#[derive(Debug)]
pub struct PgDriverError(String);

impl PgDriverError {
    pub(crate) fn connection_closed() -> Self {
        Self("connection is closed".into())
    }

    pub(crate) fn ca_cert_read(path: &Path, source: &std::io::Error) -> Self {
        Self(format!("failed to read CA certificate from {}: {}", path.display(), source))
    }

    pub(crate) fn ca_cert_parse(path: &Path, source: &std::io::Error) -> Self {
        Self(format!("failed to parse CA certificate from {}: {}", path.display(), source))
    }

    pub(crate) fn ca_cert_empty(path: &Path) -> Self {
        Self(format!("CA certificate file at {} contains no valid PEM certificates", path.display()))
    }

    pub(crate) fn ca_cert_invalid(path: &Path, source: &rustls::Error) -> Self {
        Self(format!("CA certificate from {} rejected by rustls: {}", path.display(), source))
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
