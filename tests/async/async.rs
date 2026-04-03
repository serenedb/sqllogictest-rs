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
// async statement tests
// ---------------------------------------------------------------------------

#[test]
fn test_async_statement_executes() {
    let log = Arc::new(Mutex::new(vec![]));
    runner!(log)
        .run_script(
            "\
statement async ok
insert a

statement async ok
insert b

wait
",
        )
        .expect("script should succeed");

    let got = log.lock().unwrap().clone();
    assert_eq!(got.len(), 2, "both async statements should have executed");
    assert!(got.contains(&"insert a".to_string()));
    assert!(got.contains(&"insert b".to_string()));
}

#[test]
fn test_async_statement_auto_sync_at_eof() {
    // No explicit `wait` — statements should still execute by end of script.
    let log = Arc::new(Mutex::new(vec![]));
    runner!(log)
        .run_script(
            "\
statement async ok
insert x

statement async ok
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
fn test_async_statement_error_reported_at_sync() {
    let log = Arc::new(Mutex::new(vec![]));

    let err = runner!(log)
        .run_script(
            "\
statement async ok
fail something went wrong

wait
",
        )
        .expect_err("error from async statement should surface at wait");

    assert!(
        err.to_string().contains("something went wrong"),
        "unexpected error message: {err}"
    );
}

#[test]
fn test_async_statement_error_reported_at_eof() {
    let log = Arc::new(Mutex::new(vec![]));

    let err = runner!(log)
        .run_script(
            "\
statement async ok
fail another error
",
        )
        .expect_err("error from async statement should surface at end of script");

    assert!(
        err.to_string().contains("another error"),
        "unexpected error message: {err}"
    );
}

// ---------------------------------------------------------------------------
// async query tests
// ---------------------------------------------------------------------------

#[test]
fn test_async_query_executes() {
    // The query result is not validated (async), but the SQL must be executed.
    let log = Arc::new(Mutex::new(vec![]));
    runner!(log)
        .run_script(
            "\
query async I
select count

wait
",
        )
        .expect("async query should succeed");

    let got = log.lock().unwrap().clone();
    assert!(
        got.contains(&"select count".to_string()),
        "query was not executed: {got:?}"
    );
}

#[test]
fn test_async_query_auto_sync() {
    let log = Arc::new(Mutex::new(vec![]));
    runner!(log)
        .run_script(
            "\
query async I
select value
",
        )
        .expect("async query should succeed");

    let got = log.lock().unwrap().clone();
    assert!(got.contains(&"select value".to_string()));
}

// ---------------------------------------------------------------------------
// Mixing async with regular records
// ---------------------------------------------------------------------------

#[test]
fn test_async_interleaved_with_regular() {
    let log = Arc::new(Mutex::new(vec![]));
    runner!(log)
        .run_script(
            "\
statement async ok
insert background

statement ok
insert foreground

wait
",
        )
        .expect("mixed async/regular should succeed");

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
statement async ok
insert batch1_a

statement async ok
insert batch1_b

wait

statement async ok
insert batch2_a

wait
",
        )
        .expect("multiple wait points should work");

    let got = log.lock().unwrap().clone();
    assert_eq!(got.len(), 3);
    assert!(got.contains(&"insert batch1_a".to_string()));
    assert!(got.contains(&"insert batch1_b".to_string()));
    assert!(got.contains(&"insert batch2_a".to_string()));
}

// ---------------------------------------------------------------------------
// wait with no pending is a no-op
// ---------------------------------------------------------------------------

#[test]
fn test_sync_with_no_pending_is_noop() {
    let log = Arc::new(Mutex::new(vec![]));
    runner!(log)
        .run_script(
            "\
statement ok
insert regular

wait

statement ok
insert another
",
        )
        .expect("wait with nothing pending should not error");

    let got = log.lock().unwrap().clone();
    assert_eq!(got.len(), 2);
}
