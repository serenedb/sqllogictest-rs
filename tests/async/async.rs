use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use async_trait::async_trait;
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
        .run_script_test(
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
        .run_script_test(
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
        .run_script_test(
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
        .run_script_test(
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
// async statement error-expectation tests
// ---------------------------------------------------------------------------

#[test]
fn test_async_statement_expected_error_matches() {
    // `statement async error <pat>` should succeed when the error matches.
    let log = Arc::new(Mutex::new(vec![]));
    runner!(log)
        .run_script_test(
            "\
statement async error expected failure
fail expected failure

wait
",
        )
        .expect("expected error should be treated as success");
}

#[test]
fn test_async_statement_expected_error_mismatch() {
    // The actual error message does not match the expected pattern.
    let log = Arc::new(Mutex::new(vec![]));
    let err = runner!(log)
        .run_script_test(
            "\
statement async error wrong pattern
fail actual message

wait
",
        )
        .expect_err("error-pattern mismatch should surface at wait");

    // The runner should report what the error actually was.
    assert!(
        err.to_string().contains("actual message"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_async_statement_expected_to_fail_but_succeeded() {
    // `statement async error` when the statement succeeds → error at wait.
    let log = Arc::new(Mutex::new(vec![]));
    let err = runner!(log)
        .run_script_test(
            "\
statement async error some error
insert succeeds

wait
",
        )
        .expect_err("statement that succeeded when failure was expected should error at wait");

    assert!(
        err.to_string().contains("succeed") || err.to_string().contains("ok"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_async_statement_expected_error_at_eof() {
    // Error-pattern match should also work when surfaced at EOF instead of wait.
    let log = Arc::new(Mutex::new(vec![]));
    runner!(log)
        .run_script_test(
            "\
statement async error eof error
fail eof error
",
        )
        .expect("expected error at EOF should be treated as success");
}

#[test]
fn test_async_multiple_tasks_one_errors() {
    // Several async statements queued; one fails. The error is reported at wait
    // and the statement index is identifiable from the message.
    let log = Arc::new(Mutex::new(vec![]));
    let err = runner!(log)
        .run_script_test(
            "\
statement async ok
insert before_error

statement async ok
fail mid_error

statement async ok
insert after_error

wait
",
        )
        .expect_err("mid-task error should surface at wait");

    assert!(
        err.to_string().contains("mid_error"),
        "unexpected error: {err}"
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
        .run_script_test(
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
        .run_script_test(
            "\
query async I
select value
",
        )
        .expect("async query should succeed");

    let got = log.lock().unwrap().clone();
    assert!(got.contains(&"select value".to_string()));
}

#[test]
fn test_async_query_error_at_wait() {
    // A query that raises a DB error (not a result mismatch) should surface at wait.
    let log = Arc::new(Mutex::new(vec![]));
    let err = runner!(log)
        .run_script_test(
            "\
query async I
fail query broke

wait
",
        )
        .expect_err("query DB error should surface at wait");

    assert!(
        err.to_string().contains("query broke"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_async_query_error_at_eof() {
    let log = Arc::new(Mutex::new(vec![]));
    let err = runner!(log)
        .run_script_test(
            "\
query async I
fail eof query error
",
        )
        .expect_err("query DB error should surface at EOF");

    assert!(
        err.to_string().contains("eof query error"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_async_query_result_mismatch_at_wait() {
    // FakeDB returns StatementComplete(0) for everything; expecting rows causes mismatch.
    let log = Arc::new(Mutex::new(vec![]));
    let err = runner!(log)
        .run_script_test(
            "\
show-column-names false

query async I
select data
----
42

wait
",
        )
        .expect_err("result mismatch should surface at wait");

    let msg = err.to_string();
    assert!(
        msg.contains("mismatch") || msg.contains("result") || msg.contains("42"),
        "unexpected error: {err}"
    );
}

// ---------------------------------------------------------------------------
// Mixing async with regular records
// ---------------------------------------------------------------------------

#[test]
fn test_async_interleaved_with_regular() {
    let log = Arc::new(Mutex::new(vec![]));
    runner!(log)
        .run_script_test(
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
        .run_script_test(
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
        .run_script_test(
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

// ---------------------------------------------------------------------------
// async + retry tests
// ---------------------------------------------------------------------------

/// A FakeDB that fails the first N calls then succeeds.
#[derive(Clone)]
struct FailNTimes {
    remaining: Arc<AtomicUsize>,
    log: Arc<Mutex<Vec<String>>>,
}

impl sqllogictest::DB for FailNTimes {
    type Error = FakeDBError;
    type ColumnType = DefaultColumnType;

    fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, FakeDBError> {
        if self.remaining.load(Ordering::SeqCst) > 0 {
            self.remaining.fetch_sub(1, Ordering::SeqCst);
            return Err(FakeDBError("transient failure".to_string()));
        }
        self.log.lock().unwrap().push(sql.to_string());
        Ok(DBOutput::StatementComplete(0))
    }
}

#[test]
fn test_async_statement_with_retry_succeeds() {
    let remaining = Arc::new(AtomicUsize::new(2));
    let log = Arc::new(Mutex::new(vec![]));
    let r = remaining.clone();
    let l = log.clone();
    sqllogictest::Runner::new(move |_, _| {
        let db = FailNTimes {
            remaining: r.clone(),
            log: l.clone(),
        };
        async move { Ok(db) }
    })
    .run_script_test(
        "\
statement async ok retry 3 backoff 0s
insert retried

wait
",
    )
    .expect("should succeed after retries");

    let got = log.lock().unwrap().clone();
    assert!(
        got.contains(&"insert retried".to_string()),
        "statement was not executed: {got:?}"
    );
}

#[test]
fn test_async_statement_with_retry_exhausted() {
    let remaining = Arc::new(AtomicUsize::new(5));
    let log = Arc::new(Mutex::new(vec![]));
    let r = remaining.clone();
    let l = log.clone();
    let err = sqllogictest::Runner::new(move |_, _| {
        let db = FailNTimes {
            remaining: r.clone(),
            log: l.clone(),
        };
        async move { Ok(db) }
    })
    .run_script_test(
        "\
statement async ok retry 2 backoff 0s
insert always_fails

wait
",
    )
    .expect_err("should fail when retries exhausted");

    assert!(
        err.to_string().contains("transient failure"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_async_query_with_retry_succeeds() {
    let remaining = Arc::new(AtomicUsize::new(1));
    let log = Arc::new(Mutex::new(vec![]));
    let r = remaining.clone();
    let l = log.clone();
    sqllogictest::Runner::new(move |_, _| {
        let db = FailNTimes {
            remaining: r.clone(),
            log: l.clone(),
        };
        async move { Ok(db) }
    })
    .run_script_test(
        "\
show-column-names false

query async I retry 2 backoff 0s
select value

wait
",
    )
    .expect("query should succeed after retry");

    let got = log.lock().unwrap().clone();
    assert!(
        got.contains(&"select value".to_string()),
        "query was not executed: {got:?}"
    );
}

// ---------------------------------------------------------------------------
// async system tests
// ---------------------------------------------------------------------------

#[test]
fn test_async_system_executes() {
    let log = Arc::new(Mutex::new(vec![]));
    runner!(log)
        .run_script_test(
            "\
system async ok
echo hello

system async ok
echo world

wait
",
        )
        .expect("async system commands should complete without error");
}

#[test]
fn test_async_system_auto_sync_at_eof() {
    let log = Arc::new(Mutex::new(vec![]));
    runner!(log)
        .run_script_test(
            "\
system async ok
echo auto_sync
",
        )
        .expect("async system should auto-sync at EOF");
}

#[test]
fn test_async_system_error_at_wait() {
    let log = Arc::new(Mutex::new(vec![]));
    let err = runner!(log)
        .run_script_test(
            "\
system async ok
exit 1

wait
",
        )
        .expect_err("async system failure should surface at wait");

    assert!(
        err.to_string().contains("process exited unsuccessfully"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_async_system_error_at_eof() {
    let log = Arc::new(Mutex::new(vec![]));
    let err = runner!(log)
        .run_script_test(
            "\
system async ok
exit 1
",
        )
        .expect_err("async system failure should surface at EOF");

    assert!(
        err.to_string().contains("process exited unsuccessfully"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_async_system_with_stdout_check() {
    let log = Arc::new(Mutex::new(vec![]));
    runner!(log)
        .run_script_test(
            "\
system async ok
echo hello world
----
hello world


wait
",
        )
        .expect("async system stdout check should pass");
}

#[test]
fn test_async_system_with_retry() {
    let log = Arc::new(Mutex::new(vec![]));
    runner!(log)
        .run_script_test(
            "\
system async ok retry 3 backoff 0s
echo retry_ok

wait
",
        )
        .expect("async system with retry should succeed");
}

// ---------------------------------------------------------------------------
// control always-async tests
// ---------------------------------------------------------------------------

#[test]
fn test_always_async_makes_statements_async() {
    let log = Arc::new(Mutex::new(vec![]));
    runner!(log)
        .run_script_test(
            "\
control always-async on

statement ok
insert always_a

statement ok
insert always_b

wait
",
        )
        .expect("always-async should make plain statements run asynchronously");

    let got = log.lock().unwrap().clone();
    assert_eq!(got.len(), 2);
    assert!(got.contains(&"insert always_a".to_string()));
    assert!(got.contains(&"insert always_b".to_string()));
}

#[test]
fn test_always_async_off_restores_sync() {
    let log = Arc::new(Mutex::new(vec![]));
    runner!(log)
        .run_script_test(
            "\
control always-async on

statement ok
insert async_stmt

control always-async off

statement ok
insert sync_stmt

wait
",
        )
        .expect("turning always-async off should restore synchronous behaviour");

    let got = log.lock().unwrap().clone();
    assert_eq!(got.len(), 2);
    assert!(got.contains(&"insert async_stmt".to_string()));
    assert!(got.contains(&"insert sync_stmt".to_string()));
}

#[test]
fn test_always_async_with_explicit_async_is_still_async() {
    // An explicit `async` keyword must still work when always-async is also on.
    let log = Arc::new(Mutex::new(vec![]));
    runner!(log)
        .run_script_test(
            "\
control always-async on

statement async ok
insert explicit_async

wait
",
        )
        .expect("explicit async with always-async on should succeed");

    let got = log.lock().unwrap().clone();
    assert!(got.contains(&"insert explicit_async".to_string()));
}

#[test]
fn test_always_async_error_surfaces_at_wait() {
    let log = Arc::new(Mutex::new(vec![]));
    let err = runner!(log)
        .run_script_test(
            "\
control always-async on

statement ok
fail always_async_error

wait
",
        )
        .expect_err("error from always-async statement should surface at wait");

    assert!(
        err.to_string().contains("always_async_error"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_always_async_query() {
    let log = Arc::new(Mutex::new(vec![]));
    runner!(log)
        .run_script_test(
            "\
control always-async on

query I
select implicit_async

wait
",
        )
        .expect("always-async should make queries run asynchronously");

    let got = log.lock().unwrap().clone();
    assert!(got.contains(&"select implicit_async".to_string()));
}

// ---------------------------------------------------------------------------
// control max-async-connections tests
// ---------------------------------------------------------------------------

#[test]
fn test_max_async_connections_all_tasks_complete() {
    // Basic correctness: all tasks complete when a limit is set.
    let log = Arc::new(Mutex::new(vec![]));
    runner!(log)
        .run_script_test(
            "\
control max-async-connections 2

statement async ok
insert mac_a

statement async ok
insert mac_b

statement async ok
insert mac_c

statement async ok
insert mac_d

wait
",
        )
        .expect("all tasks should complete with max-async-connections 2");

    let got = log.lock().unwrap().clone();
    assert_eq!(got.len(), 4);
    assert!(got.contains(&"insert mac_a".to_string()));
    assert!(got.contains(&"insert mac_b".to_string()));
    assert!(got.contains(&"insert mac_c".to_string()));
    assert!(got.contains(&"insert mac_d".to_string()));
}

#[test]
fn test_max_async_connections_one_serializes() {
    // limit=1 forces one-at-a-time but all tasks still complete.
    let log = Arc::new(Mutex::new(vec![]));
    runner!(log)
        .run_script_test(
            "\
control max-async-connections 1

statement async ok
insert serial_a

statement async ok
insert serial_b

statement async ok
insert serial_c

wait
",
        )
        .expect("limit of 1 should still complete all tasks");

    let got = log.lock().unwrap().clone();
    assert_eq!(got.len(), 3);
    assert!(got.contains(&"insert serial_a".to_string()));
    assert!(got.contains(&"insert serial_b".to_string()));
    assert!(got.contains(&"insert serial_c".to_string()));
}

#[test]
fn test_max_async_connections_zero_is_unlimited() {
    // 0 clears any previously set limit.
    let log = Arc::new(Mutex::new(vec![]));
    runner!(log)
        .run_script_test(
            "\
control max-async-connections 1
control max-async-connections 0

statement async ok
insert unl_a

statement async ok
insert unl_b

wait
",
        )
        .expect("max-async-connections 0 should restore unlimited");

    let got = log.lock().unwrap().clone();
    assert_eq!(got.len(), 2);
    assert!(got.contains(&"insert unl_a".to_string()));
    assert!(got.contains(&"insert unl_b".to_string()));
}

#[test]
fn test_max_async_connections_error_propagated() {
    let log = Arc::new(Mutex::new(vec![]));
    let err = runner!(log)
        .run_script_test(
            "\
control max-async-connections 2

statement async ok
fail mac_error

statement async ok
insert after_error

wait
",
        )
        .expect_err("error should surface with max-async-connections set");

    assert!(
        err.to_string().contains("mac_error"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_max_async_connections_system_tasks() {
    // System tasks also count against the semaphore.
    let log = Arc::new(Mutex::new(vec![]));
    runner!(log)
        .run_script_test(
            "\
control max-async-connections 2

system async ok
echo sys_mac_a

system async ok
echo sys_mac_b

system async ok
echo sys_mac_c

wait
",
        )
        .expect("system tasks should complete within max-async-connections limit");
}

// ConcurrentDB implements AsyncDB directly so that it can use `yield_now` to
// actually interleave tasks and measure peak concurrency on the tokio runtime.
#[derive(Clone)]
struct ConcurrentDB {
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}

#[derive(Debug)]
struct ConcurrentDBError;

impl std::fmt::Display for ConcurrentDBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "concurrent db error")
    }
}

impl std::error::Error for ConcurrentDBError {}

#[async_trait]
impl sqllogictest::AsyncDB for ConcurrentDB {
    type Error = ConcurrentDBError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, _sql: &str) -> Result<DBOutput<Self::ColumnType>, ConcurrentDBError> {
        // Increment active count and record peak before yielding.
        let current = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        loop {
            let peak = self.peak.load(Ordering::SeqCst);
            if current <= peak {
                break;
            }
            if self
                .peak
                .compare_exchange(peak, current, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }
        }
        // Yield so the executor can start other queued tasks.
        tokio::task::yield_now().await;
        self.active.fetch_sub(1, Ordering::SeqCst);
        Ok(DBOutput::StatementComplete(0))
    }

    async fn shutdown(&mut self) {}
}

#[test]
fn test_max_async_connections_limits_peak_concurrency() {
    const LIMIT: usize = 2;

    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let a = active.clone();
    let p = peak.clone();

    sqllogictest::Runner::new(move |_, _| {
        let db = ConcurrentDB {
            active: a.clone(),
            peak: p.clone(),
        };
        async move { Ok(db) }
    })
    .run_script_test(&format!(
        "\
control max-async-connections {LIMIT}

statement async ok
task_1

statement async ok
task_2

statement async ok
task_3

statement async ok
task_4

wait
"
    ))
    .expect("all tasks should complete");

    let peak_val = peak.load(Ordering::SeqCst);
    assert!(
        peak_val <= LIMIT,
        "peak concurrency {peak_val} exceeded limit {LIMIT}"
    );
}
