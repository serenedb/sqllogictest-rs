mod error;
mod extended;
mod simple;
use std::sync::Arc;
use std::marker::PhantomData;
use tokio::task::JoinHandle;
use tokio_postgres::config::SslMode;
use rustls::pki_types::*;
use rustls::DigitallySignedStruct;
use rustls::SignatureScheme;
use rustls::client::danger::{ServerCertVerifier, ServerCertVerified, HandshakeSignatureValid};

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

#[derive(Debug)]
struct NoVerification;


impl ServerCertVerifier for NoVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

       fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::ED25519,
        ]
    }
}

/// Postgres engine using the simple query protocol.
pub type PostgresSimple = Postgres<Simple>;
/// Postgres engine using the extended query protocol.
pub type PostgresExtended = Postgres<Extended>;
/// Connection configuration. Re-export of [`tokio_postgres::Config`].
pub type PostgresConfig = tokio_postgres::Config;

impl<P: sealed::Protocol> Postgres<P> {

    pub async fn connect(opts: PostgresConfig) -> Result<Self> {
        let (client, handle) = match opts.get_ssl_mode() {
            SslMode::Disable => Self::connect_plain(&opts).await?,
            _ => {
                Self::connect_tls(&opts).await?
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
    ) -> Result<(tokio_postgres::Client, JoinHandle<()>)> {
        let tls_config = rustls::ClientConfig::builder().dangerous().with_custom_certificate_verifier(Arc::new(NoVerification))
        .with_no_client_auth();
        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config);
        let (client, connection) = config.connect(tls).await?;
        Ok((client, Self::spawn_connection(connection)))
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
