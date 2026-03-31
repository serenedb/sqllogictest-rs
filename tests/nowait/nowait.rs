use std::sync::{Arc, Mutex};

use sqllogictest::{DBOutput, DefaultColumnType};

// ---------------------------------------------------------------------------
// Shared fake database
// ---------------------------------------------------------------------------

/// A simple in-memory database that records every SQL string it receives.
/// Shared state is passed via `Arc<Mutex<>>` so that all connections
/// created by `make_new` contribute to the same log.
#[derive(Clone)]
struct FakeDB {
    log: Arc<Mutex<Vec<String>>>,
}

#[derive(Debug)]
struct FakeDBError(String);

impl std::fmt::Display for FakeDBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for FakeDBError {}

impl sqllogictest::DB for FakeDB {
    type Error = FakeDBError;
    type ColumnType = DefaultColumnType;

    fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, FakeDBError> {
        if let Some(msg) = sql.strip_prefix("fail ") {
            return Err(FakeDBError(msg.to_string()));
        }
        self.log.lock().unwrap().push(sql.to_string());
        Ok(DBOutput::StatementComplete(0))
    }
}

macro_rules! runner {
    ($log:expr) => {{
        let log = $log.clone();
        sqllogictest::Runner::new(move |_, _| {
            let db = FakeDB { log: log.clone() };
            async move { Ok(db) }
        })
    }};
}

// ---------------------------------------------------------------------------
// nowait statement tests
// ---------------------------------------------------------------------------

#[test]
fn test_nowait_statement_executes() {
    let log = Arc::new(Mutex::new(vec![]));
    runner!(log)
        .run_script(
            "\
statement nowait ok
insert a

statement nowait ok
insert b

sync
",
        )
        .expect("script should succeed");

    let got = log.lock().unwrap().clone();
    assert_eq!(got.len(), 2, "both nowait statements should have executed");
    assert!(got.contains(&"insert a".to_string()));
    assert!(got.contains(&"insert b".to_string()));
}

#[test]
fn test_nowait_statement_auto_sync_at_eof() {
    // No explicit `sync` — statements should still execute by end of script.
    let log = Arc::new(Mutex::new(vec![]));
    runner!(log)
        .run_script(
            "\
statement nowait ok
insert x

statement nowait ok
insert y
",
        )
        .expect("script should succeed");

    let got = log.lock().unwrap().clone();
    assert_eq!(got.len(), 2);
    assert!(got.contains(&"insert x".to_string()));
    assert!(got.contains(&"insert y".to_string()));
}

#[test]
fn test_nowait_statement_error_reported_at_sync() {
    let log = Arc::new(Mutex::new(vec![]));

    let err = runner!(log)
        .run_script(
            "\
statement nowait ok
fail something went wrong

sync
",
        )
        .expect_err("error from nowait statement should surface at sync");

    assert!(
        err.to_string().contains("something went wrong"),
        "unexpected error message: {err}"
    );
}

#[test]
fn test_nowait_statement_error_reported_at_eof() {
    let log = Arc::new(Mutex::new(vec![]));

    let err = runner!(log)
        .run_script(
            "\
statement nowait ok
fail another error
",
        )
        .expect_err("error from nowait statement should surface at end of script");

    assert!(
        err.to_string().contains("another error"),
        "unexpected error message: {err}"
    );
}

// ---------------------------------------------------------------------------
// nowait query tests
// ---------------------------------------------------------------------------

#[test]
fn test_nowait_query_executes() {
    // The query result is not validated (nowait), but the SQL must be executed.
    let log = Arc::new(Mutex::new(vec![]));
    runner!(log)
        .run_script(
            "\
query nowait I
select count

sync
",
        )
        .expect("nowait query should succeed");

    let got = log.lock().unwrap().clone();
    assert!(
        got.contains(&"select count".to_string()),
        "query was not executed: {got:?}"
    );
}

#[test]
fn test_nowait_query_auto_sync() {
    let log = Arc::new(Mutex::new(vec![]));
    runner!(log)
        .run_script(
            "\
query nowait I
select value
",
        )
        .expect("nowait query should succeed");

    let got = log.lock().unwrap().clone();
    assert!(got.contains(&"select value".to_string()));
}

// ---------------------------------------------------------------------------
// Mixing nowait with regular records
// ---------------------------------------------------------------------------

#[test]
fn test_nowait_interleaved_with_regular() {
    let log = Arc::new(Mutex::new(vec![]));
    runner!(log)
        .run_script(
            "\
statement nowait ok
insert background

statement ok
insert foreground

sync
",
        )
        .expect("mixed nowait/regular should succeed");

    let got = log.lock().unwrap().clone();
    assert_eq!(got.len(), 2);
    assert!(got.contains(&"insert background".to_string()));
    assert!(got.contains(&"insert foreground".to_string()));
}

#[test]
fn test_multiple_sync_points() {
    let log = Arc::new(Mutex::new(vec![]));
    runner!(log)
        .run_script(
            "\
statement nowait ok
insert batch1_a

statement nowait ok
insert batch1_b

sync

statement nowait ok
insert batch2_a

sync
",
        )
        .expect("multiple sync points should work");

    let got = log.lock().unwrap().clone();
    assert_eq!(got.len(), 3);
    assert!(got.contains(&"insert batch1_a".to_string()));
    assert!(got.contains(&"insert batch1_b".to_string()));
    assert!(got.contains(&"insert batch2_a".to_string()));
}

// ---------------------------------------------------------------------------
// sync with no pending is a no-op
// ---------------------------------------------------------------------------

#[test]
fn test_sync_with_no_pending_is_noop() {
    let log = Arc::new(Mutex::new(vec![]));
    runner!(log)
        .run_script(
            "\
statement ok
insert regular

sync

statement ok
insert another
",
        )
        .expect("sync with nothing pending should not error");

    let got = log.lock().unwrap().clone();
    assert_eq!(got.len(), 2);
}
