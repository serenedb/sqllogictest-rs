mod error;
mod extended;
mod simple;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::*;
use rustls::DigitallySignedStruct;
use rustls::SignatureScheme;
use std::marker::PhantomData;
use std::sync::Arc;
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
        sqllogictest::SslMode::Prefer => tokio_postgres::config::SslMode::Prefer,
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
            _ => Self::connect_tls(&opts).await?,
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
        let tls_config = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerification))
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
            if handle.is_finished() {
                drop(client);
                handle.await.ok();
                return;
            }
            let token = client.cancel_token();
            let cancel = token.cancel_query(tokio_postgres::NoTls);
            match tokio::time::timeout(std::time::Duration::from_secs(5), cancel).await {
                Ok(Err(e)) => {
                    log::warn!("Failed to cancel query during shutdown: {:?}", e);
                }
                Err(_) => {
                    log::warn!("Timed out cancelling query during shutdown");
                }
                Ok(Ok(())) => {}
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

#[cfg(test)]
mod shutdown_tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio_postgres::config::SslMode;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_skips_cancel_after_connection_drop() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepts = Arc::new(AtomicUsize::new(0));
        let counter = accepts.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let mut s = stream.unwrap();
                if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                    let mut len = [0u8; 4];
                    s.read_exact(&mut len).unwrap();
                    let mut rest = vec![0u8; u32::from_be_bytes(len) as usize - 4];
                    s.read_exact(&mut rest).unwrap();
                    s.write_all(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0]).unwrap();
                    s.write_all(&[b'Z', 0, 0, 0, 5, b'I']).unwrap();
                    s.flush().unwrap();
                }
            }
        });

        let mut cfg = PostgresConfig::new();
        cfg.host("127.0.0.1")
            .port(port)
            .user("postgres")
            .dbname("postgres")
            .ssl_mode(SslMode::Disable);
        let mut pg = PostgresSimple::connect(cfg).await.expect("connect");

        let mut finished = false;
        for _ in 0..200 {
            if pg.conn.as_ref().is_none_or(|c| c.handle.is_finished()) {
                finished = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(finished, "connection task never observed the drop");

        tokio::time::timeout(Duration::from_secs(5), pg.shutdown())
            .await
            .expect("shutdown stalled");
        assert_eq!(
            accepts.load(Ordering::SeqCst),
            1,
            "shutdown reconnected to cancel a dead connection"
        );
    }
}
