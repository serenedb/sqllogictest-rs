use subst::Env;

use crate::RunnerLocals;

pub mod well_known {
    pub const TEST_DIR: &str = "__TEST_DIR__";
    pub const RUN_ID: &str = "__RUN_ID__";
    pub const NOW: &str = "__NOW__";
    pub const DATABASE: &str = "__DATABASE__";
}

/// Substitute environment variables and special variables like `__TEST_DIR__` in SQL.
pub(crate) struct Substitution<'a> {
    runner_locals: &'a RunnerLocals,
    subst_env_vars: bool,
}

impl Substitution<'_> {
    pub fn new(runner_locals: &RunnerLocals, subst_env_vars: bool) -> Substitution<'_> {
        Substitution {
            runner_locals,
            subst_env_vars,
        }
    }
}

#[derive(thiserror::Error, Debug)]
#[error("substitution failed: {0}")]
pub(crate) struct SubstError(subst::Error);

fn now_string() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("failed to get current time")
        .as_nanos()
        .to_string()
}

/// Prefix every `$` that does NOT begin a `subst` variable reference (`${` or
/// `$letter`/`$_`) with a backslash, so `subst` keeps it literal instead of
/// rejecting it. Runs after backslashes have already been doubled, so the
/// backslash inserted here is the single one `subst` consumes as `\$` -> `$`.
fn escape_bare_dollars(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '$'
            && !matches!(chars.peek(), Some(&('{' | 'a'..='z' | 'A'..='Z' | '_')))
        {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

impl Substitution<'_> {
    pub fn substitute(&self, input: &str) -> Result<String, SubstError> {
        if self.subst_env_vars {
            // The input is SQL, where `\` is just a backslash (`e'a\tb'`, `'\\'`)
            // and a `$` is only a variable when it starts a reference. `subst`
            // disagrees on both: it treats `\` as an escape (rejecting `\t` as an
            // invalid sequence) and a lone `$` as a malformed variable ("Missing
            // variable name", e.g. a regex end-anchor `[a-z]+$`). Pre-process so
            // `subst` still expands `$VAR` / `${VAR}` / `${VAR:-default}` but
            // passes everything else through verbatim:
            //   * double every backslash, so each survives subst's un-escaping;
            //   * escape (with subst's `\$`) every `$` that does NOT begin a
            //     reference (`${` or `$letter`/`$_`), so it stays literal.
            let escaped = input.replace('\\', "\\\\");
            let escaped = escape_bare_dollars(&escaped);
            subst::substitute(&escaped, self).map_err(SubstError)
        } else {
            Ok(self.simple_replace(input))
        }
    }

    fn simple_replace(&self, input: &str) -> String {
        let mut res = input
            .replace(
                &format!("${}", well_known::TEST_DIR),
                &self.runner_locals.test_dir(),
            )
            .replace(
                &format!("${}", well_known::RUN_ID),
                &self.runner_locals.run_id(),
            )
            .replace(&format!("${}", well_known::NOW), &now_string());
        for (key, value) in self.runner_locals.vars() {
            res = res.replace(&format!("${}", key), value);
        }
        res
    }
}

impl<'a> subst::VariableMap<'a> for Substitution<'a> {
    type Value = String;

    fn get(&'a self, key: &str) -> Option<Self::Value> {
        match key {
            well_known::TEST_DIR => self.runner_locals.test_dir().into(),
            well_known::RUN_ID => self.runner_locals.run_id().into(),
            well_known::NOW => now_string().into(),
            key => self
                .runner_locals
                .get_var(key)
                .cloned()
                .or_else(|| Env.get(key)),
        }
    }
}
