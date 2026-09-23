//! Compile-check a config without running anything.

use serde_json::Value;

use crate::codegen::generate;
use crate::config::{Config, Source, default_max_ops};
use crate::diagnostic::Diagnostic;
use crate::runner::sandboxed_engine;

/// What [`check_config`] found.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CheckResult {
    /// Every problem, in config order. Empty when the config is valid.
    pub diagnostics: Vec<Diagnostic>,
    /// The program `ops` compile to; `None` in Code mode or when `ops` is invalid.
    pub generated_script: Option<String>,
}

/// Parse and compile a `document_script` config. **Never executes it.**
///
/// Runs every check `Config::parse` does, collecting all problems instead of stopping at
/// the first, then compiles the script or the program generated from `ops`.
pub fn check_config(config: &Value) -> CheckResult {
    if !config.is_object() {
        return CheckResult {
            diagnostics: vec![Diagnostic::at("config", "config must be an object")],
            generated_script: None,
        };
    }
    let engine = sandboxed_engine(default_max_ops());
    let cfg = match Config::check(config.clone(), &engine) {
        Ok(cfg) => cfg,
        Err(diagnostics) => return CheckResult { diagnostics, generated_script: None },
    };
    match &cfg.source {
        Source::Script(script) => match engine.compile(script) {
            Ok(_) => CheckResult::default(),
            Err(e) => CheckResult {
                diagnostics: vec![Diagnostic::from_parse_error("script", &e)],
                generated_script: None,
            },
        },
        Source::Ops(ops) => {
            let program = generate(ops);
            match engine.compile(&program) {
                Ok(_) => CheckResult { diagnostics: vec![], generated_script: Some(program) },
                Err(e) => CheckResult {
                    diagnostics: vec![Diagnostic::at("ops", format!("generated program does not compile: {e}"))],
                    generated_script: None,
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_valid_script_has_no_diagnostics() {
        assert_eq!(check_config(&json!({"script": "doc.fields.x = 1;"})), CheckResult::default());
    }

    #[test]
    fn a_syntax_error_reports_its_line_and_column() {
        let result = check_config(&json!({"script": "let a = 1;\ndoc.fields.x = ;"}));
        assert_eq!(result.diagnostics.len(), 1);
        let d = &result.diagnostics[0];
        assert_eq!(d.path, "script");
        assert_eq!(d.line, Some(2));
        assert!(d.column.is_some());
    }

    #[test]
    fn valid_ops_return_their_generated_program() {
        let result = check_config(&json!({"ops": [{"rename": {"from": "a", "to": "b"}}]}));
        assert!(result.diagnostics.is_empty());
        assert_eq!(result.generated_script.as_deref(), Some("doc.rename_field(\"a\", \"b\");\n"));
    }

    #[test]
    fn every_problem_is_collected() {
        let result = check_config(&json!({
            "ops": [{"set": {"field": "t", "expr": "1 +"}}, {"drop": []}],
            "max_ops": 0
        }));
        let paths: Vec<&str> = result.diagnostics.iter().map(|d| d.path.as_str()).collect();
        assert_eq!(paths, ["ops[0].expr", "ops[1]", "max_ops"]);
        assert_eq!(result.generated_script, None);
    }

    #[test]
    fn a_non_object_config_is_reported() {
        let result = check_config(&json!("nope"));
        assert_eq!(result.diagnostics[0].path, "config");
    }

    /// The check compiles only. An endless loop compiles fine and returns at once.
    #[test]
    fn the_check_never_executes_the_script() {
        let started = std::time::Instant::now();
        let result = check_config(&json!({"script": "loop { }"}));
        assert!(result.diagnostics.is_empty());
        assert!(started.elapsed() < std::time::Duration::from_secs(2));
    }
}
