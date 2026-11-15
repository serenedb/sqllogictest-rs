use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use sqllogictest::{DBOutput, DefaultColumnType, RecordOutcome, RecordOutput};

/// Runs `select <value>` as a query of one row, `count <n>` as a statement affecting `n` rows,
/// `fail <message>` as an error and `attempt` as a query of the number of attempts so far.
#[derive(Clone)]
struct FakeDB {
    log: Arc<Mutex<Vec<String>>>,
    attempts: Arc<AtomicUsize>,
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
        self.log.lock().unwrap().push(sql.to_string());
        let rows = |value: String| DBOutput::Rows {
            types: vec![DefaultColumnType::Any],
            rows: vec![vec![value]],
        };
        if let Some(message) = sql.strip_prefix("fail ") {
            return Err(FakeDBError(message.to_string()));
        }
        if let Some(value) = sql.strip_prefix("select ") {
            return Ok(rows(value.to_string()));
        }
        if let Some(count) = sql.strip_prefix("count ") {
            return Ok(DBOutput::StatementComplete(count.parse().unwrap()));
        }
        assert_eq!(sql, "attempt");
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(rows(attempt.to_string()))
    }
}

struct Run {
    log: Vec<String>,
    outcomes: Vec<RecordOutcome<DefaultColumnType>>,
    result: Result<(), sqllogictest::TestError>,
}

async fn run(script: &str) -> Run {
    let db = FakeDB {
        log: Arc::default(),
        attempts: Arc::default(),
    };
    let log = db.log.clone();
    let mut runner = sqllogictest::Runner::new(move |_, _, _, _, _| {
        let db = db.clone();
        async move { Ok(db) }
    });
    let mut outcomes = vec![];
    let result = runner
        .run_multi_records_reporting(sqllogictest::parse(script).unwrap(), |outcome| {
            outcomes.push(outcome);
            Ok(())
        })
        .await;
    let log = log.lock().unwrap().clone();
    Run {
        log,
        outcomes,
        result,
    }
}

fn rows(outcome: &RecordOutcome<DefaultColumnType>) -> Vec<Vec<String>> {
    match &outcome.output {
        RecordOutput::Query { rows, .. } => rows.clone(),
        output => panic!("not a query output: {output:?}"),
    }
}

#[tokio::test]
async fn test_reporting_goes_on_after_a_failed_record() {
    let run = run("\
statement ok
fail one

query I
select 2
----
3

statement count 1
count 1
")
    .await;
    run.result.expect("no record stops the run");
    assert_eq!(run.log, ["fail one", "select 2", "count 1"]);
    let reported = run
        .outcomes
        .iter()
        .map(|outcome| (outcome.index, outcome.error.is_some()))
        .collect::<Vec<_>>();
    assert_eq!(reported, [(0, true), (1, true), (2, false)]);
    assert_eq!(rows(&run.outcomes[1]), [["2"]]);
}

#[tokio::test]
async fn test_reporting_takes_an_async_outcome_before_its_connection_runs_again() {
    let run = run("\
connection a
statement async ok
fail late

connection a
statement ok
count 1

statement async ok
fail anonymous

wait

query I
select 4
----
4
")
    .await;
    run.result.expect("no record stops the run");
    let reported = run
        .outcomes
        .iter()
        .map(|outcome| (outcome.index, outcome.error.is_some()))
        .collect::<Vec<_>>();
    assert_eq!(reported, [(1, true), (3, false), (4, true), (7, false)]);
    assert!(run.outcomes[0]
        .error
        .as_ref()
        .unwrap()
        .to_string()
        .contains("late"));
}

#[tokio::test]
async fn test_reporting_gives_the_last_attempt_of_a_retry() {
    let run = run("\
query I retry 3 backoff 0s
attempt
----
9

query I retry 3 backoff 0s
attempt
----
5
")
    .await;
    run.result.expect("no record stops the run");
    assert_eq!(run.log, ["attempt"; 5]);
    assert!(run.outcomes[0].error.is_some());
    assert_eq!(rows(&run.outcomes[0]), [["3"]]);
    assert!(run.outcomes[1].error.is_none());
    assert_eq!(rows(&run.outcomes[1]), [["5"]]);
}

#[tokio::test]
async fn test_per_run_values_are_the_test_directory_and_run_id_once_used() {
    let db = FakeDB {
        log: Arc::default(),
        attempts: Arc::default(),
    };
    let mut runner = sqllogictest::Runner::new(move |_, _, _, _, _| {
        let db = db.clone();
        async move { Ok(db) }
    });
    assert!(runner.per_run_values().is_empty());
    let script = "control substitution on\n\nquery T\nselect ${__TEST_DIR__}\n----\n/tmp\n";
    let mut outcomes = vec![];
    runner
        .run_multi_records_reporting(sqllogictest::parse(script).unwrap(), |outcome| {
            outcomes.push(outcome);
            Ok(())
        })
        .await
        .expect("no record stops the run");
    let values = runner.per_run_values();
    let [test_dir, run_id] = values.as_slice() else {
        panic!("not a test directory and a run id: {values:?}");
    };
    assert!(test_dir.contains(run_id.as_str()));
    assert_eq!(rows(&outcomes[0]), [[test_dir.as_str()]]);
}

#[tokio::test]
async fn test_reporting_stops_at_a_failed_let() {
    let run = run("\
let x
fail nothing to bind

statement ok
count 1
")
    .await;
    assert!(run
        .result
        .expect_err("a failed let stops the run")
        .to_string()
        .contains("nothing to bind"));
    assert_eq!(run.log, ["fail nothing to bind"]);
    assert!(run.outcomes.is_empty());
}
