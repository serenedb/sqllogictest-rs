use std::collections::HashMap;
use std::future::IntoFuture;

use futures::future::join_all;
use futures::Future;

use crate::{AsyncDB, Connection as ConnectionName, DBOutput, DBPort, SslMode};

/// Trait for making connections to an [`AsyncDB`].
///
/// This is introduced to allow querying the database with different connections
/// (then generally different sessions) in a single test file with `connection` records.
pub trait MakeConnection {
    /// The database type.
    type Conn: AsyncDB;
    /// The future returned by [`MakeConnection::make`].
    type MakeFuture: Future<Output = Result<Self::Conn, <Self::Conn as AsyncDB>::Error>>;

    /// Creates a new connection to the database using the given [`SslMode`]
    /// and optional port override.
    fn make(&mut self, ssl_mode: SslMode, port: DBPort) -> Self::MakeFuture;
}

/// Make connections directly from a closure returning a future.
///
/// The closure receives the [`SslMode`] and optional port override so callers
/// can configure TLS and routing per connection.
impl<D: AsyncDB, F, Fut> MakeConnection for F
where
    F: FnMut(SslMode, DBPort) -> Fut,
    Fut: IntoFuture<Output = Result<D, D::Error>>,
{
    type Conn = D;
    type MakeFuture = Fut::IntoFuture;

    fn make(&mut self, ssl_mode: SslMode, port: DBPort) -> Self::MakeFuture {
        self(ssl_mode, port).into_future()
    }
}

/// Connections established in a [`Runner`](crate::Runner).
pub(crate) struct Connections<D, M> {
    make_conn: M,
    conns: HashMap<ConnectionName, D>,
}

impl<D: AsyncDB, M: MakeConnection<Conn = D>> Connections<D, M> {
    pub fn new(make_conn: M) -> Self {
        Connections {
            make_conn,
            conns: HashMap::new(),
        }
    }

    /// Get a connection by name. Make a new connection if it doesn't exist.
    ///
    /// The [`SslMode`] from the [`ConnectionName`] is forwarded to
    /// [`MakeConnection::make`] only when creating a new connection — it is
    /// ignored on cache hits since the connection is already established.
    pub async fn get(&mut self, name: ConnectionName) -> Result<&mut D, D::Error> {
        use std::collections::hash_map::Entry;

        // Extract ssl_mode and port before moving `name` into the entry API.
        let (ssl_mode, port) = match &name {
            ConnectionName::Named { ssl_mode, port, .. } => (ssl_mode.clone(), port.clone()),
            ConnectionName::Default => (SslMode::Disable, DBPort::Plain),
        };

        let conn = match self.conns.entry(name) {
            Entry::Occupied(o) => o.into_mut(),
            Entry::Vacant(v) => {
                let conn = self.make_conn.make(ssl_mode, port).await?;
                v.insert(conn)
            }
        };

        Ok(conn)
    }

    /// Create a new connection without caching it.
    ///
    /// Used by the `nowait` feature to create a dedicated connection for a background query.
    pub async fn make_new(&mut self, ssl_mode: SslMode, port: DBPort) -> Result<D, D::Error> {
        self.make_conn.make(ssl_mode, port).await
    }

    /// Run a SQL statement on the default connection.
    ///
    /// This is a shortcut for calling `get(Default)` then `run`.
    pub async fn run_default(&mut self, sql: &str) -> Result<DBOutput<D::ColumnType>, D::Error> {
        self.get(ConnectionName::Default).await?.run(sql).await
    }

    /// Shutdown all connections.
    pub async fn shutdown_all(&mut self) {
        join_all(self.conns.values_mut().map(|conn| conn.shutdown())).await;
    }
}
