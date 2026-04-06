#[cfg(any(test, feature = "testing"))]
use std::path::Path;

pub use glob::glob;
pub use libtest_mimic::{run, Arguments, Failed, Trial};

#[cfg(any(test, feature = "testing"))]
use crate::{MakeConnection, Runner};

/// * `db_fn`: `fn() -> sqllogictest::AsyncDB`
/// * `pattern`: The glob used to match against and select each file to be tested. It is relative to
///   the root of the crate.
#[cfg(any(test, feature = "testing"))]
#[macro_export]
macro_rules! harness {
    ($db_fn:path, $pattern:expr) => {
        fn main() {
            let paths = $crate::harness::glob($pattern).expect("failed to find test files");
            let mut tests = vec![];

            for entry in paths {
                let path = entry.expect("failed to read glob entry");
                tests.push($crate::harness::Trial::test(
                    path.to_str().unwrap().to_string(),
                    move || $crate::harness::test(&path, |_, _| async { Ok($db_fn()) }),
                ));
            }

            if tests.is_empty() {
                panic!("no test found for sqllogictest under: {}", $pattern);
            }

            $crate::harness::run(&$crate::harness::Arguments::from_args(), tests).exit();
        }
    };
}

#[cfg(any(test, feature = "testing"))]
pub fn test<M: MakeConnection>(filename: impl AsRef<Path>, make_conn: M) -> Result<(), Failed>
where
    M::Conn: Send + 'static,
{
    let mut tester = Runner::new(make_conn);
    tester.run_file_test(filename)?;
    tester.shutdown();
    Ok(())
}
