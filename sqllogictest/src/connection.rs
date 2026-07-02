use std::collections::HashMap;
use std::future::IntoFuture;

use futures::future::join_all;
use futures::Future;

use crate::{AsyncDB, Connection as ConnectionName, DBPort, SslMode};

/// Trait for making connections to an [`AsyncDB`].
///
/// This is introduced to allow querying the database with different connections
/// (then generally different sessions) in a single test file with `connection` records.
pub trait MakeConnection {
    /// The database type.
    type Conn: AsyncDB;
    /// The future returned by [`MakeConnection::make`].
    type MakeFuture: Future<Output = Result<Self::Conn, <Self::Conn as AsyncDB>::Error>>;

    /// Creates a new connection to the database using the given [`SslMode`],
    /// optional port override, and optional login-user / login-password
    /// override.
    fn make(
        &mut self,
        ssl_mode: SslMode,
        port: DBPort,
        user: Option<String>,
        password: Option<String>,
    ) -> Self::MakeFuture;
}

/// Make connections directly from a closure returning a future.
///
/// The closure receives the [`SslMode`], optional port override, and optional
/// login-user / login-password override so callers can configure TLS, routing,
/// and the authenticating credentials per connection.
impl<D: AsyncDB, F, Fut> MakeConnection for F
where
    F: FnMut(SslMode, DBPort, Option<String>, Option<String>) -> Fut,
    Fut: IntoFuture<Output = Result<D, D::Error>>,
{
    type Conn = D;
    type MakeFuture = Fut::IntoFuture;

    fn make(
        &mut self,
        ssl_mode: SslMode,
        port: DBPort,
        user: Option<String>,
        password: Option<String>,
    ) -> Self::MakeFuture {
        self(ssl_mode, port, user, password).into_future()
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
    pub async fn get(&mut self, name: ConnectionName) -> Result<D, D::Error> {
        use std::collections::hash_map::Entry;

        // Extract ssl_mode, port, user, and password before moving `name` into
        // the entry API.
        let (ssl_mode, port, user, password) = match &name {
            ConnectionName::Named {
                ssl_mode,
                port,
                user,
                password,
                ..
            } => (
                ssl_mode.clone(),
                port.clone(),
                user.clone(),
                password.clone(),
            ),
            ConnectionName::Default => (SslMode::Disable, DBPort::Plain, None, None),
        };

        let conn = match self.conns.entry(name) {
            Entry::Occupied(o) => o.remove(),
            Entry::Vacant(_) => self.make_conn.make(ssl_mode, port, user, password).await?,
        };

        Ok(conn)
    }

    pub fn add(&mut self, name: ConnectionName, conn: D) {
        self.conns.insert(name, conn);
    }

    pub async fn make_new(&mut self) -> Result<D, D::Error> {
        self.make_conn
            .make(SslMode::Disable, DBPort::Plain, None, None)
            .await
    }

    /// Shutdown all connections.
    pub async fn shutdown_all(&mut self) {
        join_all(self.conns.values_mut().map(|conn| conn.shutdown())).await;
    }
}
