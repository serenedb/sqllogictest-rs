use std::fmt::Display;
use std::time::Duration;

use async_trait::async_trait;
use clap::ValueEnum;
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType};
use sqllogictest_engines::external::ExternalDriver;
use sqllogictest_engines::mysql::{MySql, MySqlConfig};
use sqllogictest_engines::postgres::{PostgresConfig, PostgresExtended, PostgresSimple};
use tokio::process::Command;
use sqllogictest_engines::postgres::ConnectOptions;
use sqllogictest_engines::postgres::to_pg_ssl_mode;
use sqllogictest::parser::{SslMode};

use super::{DBConfig, Result};

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum EngineType {
    Mysql,
    Postgres,
    PostgresExtended,
    External,
}

#[derive(Clone, Debug)]
pub enum EngineConfig {
    MySql,
    Postgres,
    PostgresExtended,
    External(String),
}

pub(crate) enum Engines {
    MySql(MySql),
    Postgres(PostgresSimple),
    PostgresExtended(PostgresExtended),
    External(ExternalDriver),
}

impl From<&DBConfig> for MySqlConfig {
    fn from(config: &DBConfig) -> Self {
        let (host, port) = config.random_addr();
        let database_url = format!(
            "mysql://{}:{}@{}:{}/{}",
            config.user, config.pass, host, port, config.db
        );
        MySqlConfig::from_url(&database_url).unwrap()
    }
}


// ── helpers ────────────────────────────────────────────────────────────────

/// Builds a [`PostgresConfig`] from a [`DBConfig`], with an optional port override.
/// When `port_override` is `Some`, all addresses use that port instead of the
/// one stored in `DBConfig`.
fn pg_config_from(config: &DBConfig, port_override: Option<u16>) -> PostgresConfig {
    let (host, port) = config.random_addr();
    let port = port_override.unwrap_or(port);

    let mut pg_config = PostgresConfig::new();
    pg_config
        .host(host)
        .port(port)
        .dbname(&config.db)
        .user(&config.user)
        .password(&config.pass);
    if let Some(options) = &config.options {
        pg_config.options(options);
    }
    pg_config
}

// ── From impls ─────────────────────────────────────────────────────────────

impl From<&DBConfig> for PostgresConfig {
    fn from(config: &DBConfig) -> Self {
        pg_config_from(config, None)
    }
}


fn make_connect_opts(config: &DBConfig, ssl_mode: SslMode, port: Option<u16>) -> ConnectOptions {
    let mut pg_config = PostgresConfig::from(config);
    if let Some(p) = port {
        // tokio_postgres::Config doesn't allow mutating ports after construction,
        // so rebuild with the overridden port.
        let (host, _) = config.random_addr();
        pg_config = PostgresConfig::new();
        pg_config
            .host(host)
            .port(p)
            .dbname(&config.db)
            .user(&config.user)
            .password(&config.pass);
        if let Some(options) = &config.options {
            pg_config.options(options);
        }
    }
    log::error!("make_connect_opts {}", ssl_mode.as_str());
    pg_config.ssl_mode(to_pg_ssl_mode(ssl_mode));
    ConnectOptions::new(pg_config)
}

/// Converts a [`DBConfig`] into [`ConnectOptions`] with no TLS.
///
/// TLS (including custom CA certs) is not configured here — this path is used
/// for standard test runs where the database does not require SSL. For TLS
/// connections, construct [`ConnectOptions`] directly with [`ConnectOptions::with_ca_cert`].
impl From<&DBConfig> for ConnectOptions {
    fn from(config: &DBConfig) -> Self {
        make_connect_opts(config, SslMode::Disable, None)
    }
}

pub(crate) async fn connect(
    engine: &EngineConfig,
    config: &DBConfig,
    ssl_mode: SslMode,
    port: Option<u16>,
) -> Result<Engines, EnginesError> {
    Ok(match engine {
        EngineConfig::MySql => Engines::MySql(
            MySql::connect(config.into())
                .await
                .map_err(|e| EnginesError(e.into()))?,
        ),
        EngineConfig::Postgres => Engines::Postgres(
            PostgresSimple::connect(make_connect_opts(config, ssl_mode, port))
                .await
                .map_err(|e| EnginesError(e.into()))?,
        ),
        EngineConfig::PostgresExtended => Engines::PostgresExtended(
            PostgresExtended::connect(make_connect_opts(config, ssl_mode, port))
                .await
                .map_err(|e| EnginesError(e.into()))?,
        ),
        EngineConfig::External(cmd_tmpl) => {
            let (host, port) = config.random_addr();
            let cmd_str = cmd_tmpl
                .replace("{db}", &config.db)
                .replace("{host}", host)
                .replace("{port}", &port.to_string())
                .replace("{user}", &config.user)
                .replace("{pass}", &config.pass);
            let mut cmd = Command::new("bash");
            cmd.args(["-c", &cmd_str]);
            Engines::External(
                ExternalDriver::connect(cmd)
                    .await
                    .map_err(|e| EnginesError(e.into()))?,
            )
        }
    })
}

#[derive(Debug)]
pub(crate) struct EnginesError(anyhow::Error);

impl Display for EnginesError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for EnginesError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

macro_rules! dispatch_engines {
    ($impl:expr, $inner:ident, $body:tt) => {{
        match $impl {
            Engines::MySql($inner) => $body,
            Engines::Postgres($inner) => $body,
            Engines::PostgresExtended($inner) => $body,
            Engines::External($inner) => $body,
        }
    }};
}

#[async_trait]
impl AsyncDB for Engines {
    type Error = EnginesError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        dispatch_engines!(self, e, {
            e.run(sql)
                .await
                .map_err(|e| EnginesError(anyhow::Error::from(e)))
        })
    }

    fn engine_name(&self) -> &str {
        dispatch_engines!(self, e, { e.engine_name() })
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await
    }

    async fn run_command(command: std::process::Command) -> std::io::Result<std::process::Output> {
        Command::from(command).output().await
    }

    async fn shutdown(&mut self) {
        dispatch_engines!(self, e, { e.shutdown().await })
    }
}
