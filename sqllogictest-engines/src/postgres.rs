mod error;
mod extended;
mod simple;

use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use tokio::task::JoinHandle;
use tokio_postgres::config::SslMode;

type Result<T> = std::result::Result<T, error::PgDriverError>;

/// Marker type for the Postgres simple query protocol.
pub struct Simple;
/// Marker type for the Postgres extended query protocol.
pub struct Extended;

mod sealed {
    pub trait Protocol {}
}
impl sealed::Protocol for Simple {}
impl sealed::Protocol for Extended {}

pub fn to_pg_ssl_mode(mode: sqllogictest::SslMode) -> tokio_postgres::config::SslMode {
    match mode {
        sqllogictest::SslMode::Disable => tokio_postgres::config::SslMode::Disable,
        sqllogictest::SslMode::Prefer  => tokio_postgres::config::SslMode::Prefer,
        sqllogictest::SslMode::Require => tokio_postgres::config::SslMode::Require,
    }
}

// ── ConnectOptions ─────────────────────────────────────────────────────────

/// Path to the self-signed CA certificate used for TLS in tests.
/// Resolved at runtime from the `RESOURCES` environment variable.
fn test_ca_cert_path() -> PathBuf {
    let resources = std::env::var("RESOURCES")
        .expect("RESOURCES environment variable must be set");
    PathBuf::from(resources).join("ca.pem")
}

/// All options needed to establish a Postgres connection.
///
/// # Examples
///
/// Plain connection (no TLS):
/// ```rust
/// let opts = ConnectOptions::new(config); // sslmode=disable
/// ```
///
/// TLS using the built-in test CA certificate:
/// ```rust
/// let opts = ConnectOptions::new(config.ssl_mode(SslMode::Require));
/// // ca.pem is loaded automatically from resources/ca.pem
/// ```
///
/// TLS with a custom CA certificate path (overrides the default):
/// ```rust
/// let opts = ConnectOptions::new(config.ssl_mode(SslMode::Require))
///     .with_ca_cert("/custom/path/ca.pem");
/// ```
pub struct ConnectOptions {
    /// Core tokio_postgres connection config.
    pub pg_config: PostgresConfig,

    /// Path to the PEM-encoded CA certificate the server must be signed by.
    /// When `None` and TLS is required, falls back to [`TEST_CA_CERT_PATH`].
    /// No other CA is trusted — the server certificate must chain to exactly
    /// this certificate.
    pub ca_cert: Option<PathBuf>,
}

impl ConnectOptions {
    pub fn new(mut pg_config: PostgresConfig) -> Self {
        // Default to Disable so callers that don't configure TLS explicitly
        // are never accidentally routed into the TLS path.
        pg_config.ssl_mode(tokio_postgres::config::SslMode::Disable);
        Self { pg_config, ca_cert: None }
    }

    /// Overrides the CA certificate path. Useful when the test CA is not in
    /// the default location. Has no effect when `sslmode=disable`.
    pub fn with_ca_cert(mut self, path: impl AsRef<Path>) -> Self {
        self.ca_cert = Some(path.as_ref().to_path_buf());
        self
    }
}

// ── Postgres<P> ────────────────────────────────────────────────────────────

/// Generic Postgres engine based on the client from [`tokio_postgres`].
/// The protocol `P` can be either [`Simple`] or [`Extended`].
pub struct Postgres<P: sealed::Protocol> {
    conn: Option<ActiveConn>,
    _protocol: PhantomData<P>,
}

struct ActiveConn {
    client: tokio_postgres::Client,
    handle: JoinHandle<()>,
}

/// Postgres engine using the simple query protocol.
pub type PostgresSimple = Postgres<Simple>;
/// Postgres engine using the extended query protocol.
pub type PostgresExtended = Postgres<Extended>;
/// Connection configuration. Re-export of [`tokio_postgres::Config`].
pub type PostgresConfig = tokio_postgres::Config;

impl<P: sealed::Protocol> Postgres<P> {
    /// Connects using the given [`ConnectOptions`].
    ///
    /// | sslmode   | `ca_cert` required? | behaviour                        |
    /// |-----------|---------------------|----------------------------------|
    /// | `disable` | no (ignored)        | plain TCP, no TLS                |
    /// | `prefer`  | yes                 | TLS with strict CA verification  |
    /// | `require` | yes                 | TLS mandatory, strict CA         |
    ///
    /// Returns [`PgDriverError::CaCertRequired`] if `sslmode` is not
    /// `disable` and no `ca_cert` path was provided.
    pub async fn connect(opts: ConnectOptions) -> Result<Self> {
        let (client, handle) = match opts.pg_config.get_ssl_mode() {
            SslMode::Disable => Self::connect_plain(&opts.pg_config).await?,
            _ => {
                // Use the explicitly provided CA path, or fall back to the
                // path derived from the RESOURCES environment variable.
                let default_ca = test_ca_cert_path();
                let ca_path = opts.ca_cert.as_deref().unwrap_or(&default_ca);
                Self::connect_tls(&opts.pg_config, ca_path).await?
            }
        };
        Ok(Self {
            conn: Some(ActiveConn { client, handle }),
            _protocol: PhantomData,
        })
    }

    // ── internal helpers ───────────────────────────────────────────────────

    async fn connect_plain(
        config: &PostgresConfig,
    ) -> Result<(tokio_postgres::Client, JoinHandle<()>)> {
        let (client, connection) = config.connect(tokio_postgres::NoTls).await?;
        Ok((client, Self::spawn_connection(connection)))
    }

    async fn connect_tls(
        config: &PostgresConfig,
        ca_path: &Path,
    ) -> Result<(tokio_postgres::Client, JoinHandle<()>)> {
        let roots = Self::load_ca_cert(ca_path)?;
        let tls_config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config);
        let (client, connection) = config.connect(tls).await?;
        Ok((client, Self::spawn_connection(connection)))
    }

    /// Builds a root store containing *only* the provided PEM CA certificate.
    /// No platform roots are loaded — the server must present a certificate
    /// signed by exactly this CA.
    fn load_ca_cert(ca_path: &Path) -> Result<rustls::RootCertStore> {
        let pem = std::fs::read(ca_path)
            .map_err(|e| error::PgDriverError::ca_cert_read(ca_path, &e))?;

        let mut cursor = std::io::Cursor::new(pem);
        let certs = rustls_pemfile::certs(&mut cursor)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| error::PgDriverError::ca_cert_parse(ca_path, &e))?;

        if certs.is_empty() {
            return Err(error::PgDriverError::ca_cert_empty(ca_path));
        }

        let mut roots = rustls::RootCertStore::empty();
        for cert in certs {
            roots.add(cert)
                .map_err(|e| error::PgDriverError::ca_cert_invalid(ca_path, &e))?;
        }
        Ok(roots)
    }

    fn spawn_connection<C>(connection: C) -> JoinHandle<()>
    where
        C: std::future::Future<Output = std::result::Result<(), tokio_postgres::Error>>
            + Send
            + 'static,
    {
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                if e.is_closed() {
                    log::info!("Postgres connection closed");
                } else {
                    log::error!("Postgres connection error: {:?}", e);
                }
            }
        })
    }

    // ── public API ─────────────────────────────────────────────────────────

    /// Returns a reference to the inner Postgres client, or an error if
    /// the connection has been shut down.
    pub fn client(&self) -> Result<&tokio_postgres::Client> {
        self.conn
            .as_ref()
            .map(|c| &c.client)
            .ok_or_else(error::PgDriverError::connection_closed)
    }

    /// Gracefully shuts down the Postgres connection.
    pub async fn shutdown(&mut self) {
        if let Some(ActiveConn { client, handle }) = self.conn.take() {
            if let Err(e) = client
                .cancel_token()
                .cancel_query(tokio_postgres::NoTls)
                .await
            {
                log::warn!("Failed to cancel query during shutdown: {:?}", e);
            }
            drop(client);
            handle.await.ok();
        }
    }
}

impl<P: sealed::Protocol> Drop for Postgres<P> {
    fn drop(&mut self) {
        if let Some(ActiveConn { handle, .. }) = &self.conn {
            handle.abort();
        }
    }
}
