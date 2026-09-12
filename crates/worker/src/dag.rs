//! Pure, deterministic helpers used by the workflow to schedule steps. Kept free of
//! Temporal types so they can be unit-tested directly.

use std::collections::HashMap;

use meili_ingest_plugin_sdk::{
    Backoff, PipelineDefinition, PluginInput, PluginOutput, RetryConfig, StepDefinition,
};

/// Steps whose dependencies are all in `done` and that are not yet done themselves,
/// in `order` (topological) order.
pub fn ready_steps<'a>(
    pipeline: &'a PipelineDefinition,
    order: &[String],
    done: &HashMap<String, PluginOutput>,
) -> Vec<&'a StepDefinition> {
    order
        .iter()
        .filter(|id| !done.contains_key(id.as_str()))
        .filter_map(|id| pipeline.step(id))
        .filter(|s| s.depends_on.iter().all(|d| done.contains_key(d)))
        .collect()
}

/// Build the input of a step from the initial workflow input and the outputs of its
/// dependencies: no deps → initial input; one dep → that output; several → `Many`.
pub fn resolve_input(
    step: &StepDefinition,
    initial: &PluginInput,
    done: &HashMap<String, PluginOutput>,
) -> PluginInput {
    match step.depends_on.as_slice() {
        [] => initial.clone(),
        [single] => done
            .get(single)
            .cloned()
            .map(PluginInput::from)
            .unwrap_or(PluginInput::Empty),
        many => PluginInput::Many(many.iter().filter_map(|d| done.get(d).cloned()).collect()),
    }
}

/// Result of expanding a fan-out step.
#[derive(Debug, Clone, PartialEq)]
pub enum FanOut {
    /// The upstream output was inline; here are the branch inputs.
    Branches(Vec<PluginInput>),
    /// The upstream output is a spilled reference; an activity must expand it.
    NeedsActivity(PluginOutput),
}

/// Split the upstream output into fan-out branches according to `path`
/// (`$.documents`, `$.many` or `$`). Documents are grouped so that at most
/// `max_branches` activities are scheduled.
pub fn fan_out_branches(path: &str, upstream: PluginOutput, max_branches: usize) -> FanOut {
    let max_branches = max_branches.max(1);
    match (path, upstream) {
        ("$", out) => FanOut::Branches(vec![PluginInput::from(out)]),
        (_, PluginOutput::Ref(r)) => FanOut::NeedsActivity(PluginOutput::Ref(r)),
        ("$.many", PluginOutput::Many(items)) => {
            // Refs inside Many are resolved by the activity that receives them.
            FanOut::Branches(items.into_iter().map(PluginInput::from).collect())
        }
        ("$.many", other) => FanOut::Branches(vec![PluginInput::from(other)]),
        (_, out) => match out.into_documents() {
            Ok(docs) => FanOut::Branches(group_documents(docs, max_branches)),
            Err(_) => FanOut::Branches(vec![]),
        },
    }
}

/// Split documents into at most `max_branches` groups of contiguous documents.
pub fn group_documents(
    docs: Vec<meili_ingest_plugin_sdk::Document>,
    max_branches: usize,
) -> Vec<PluginInput> {
    if docs.is_empty() {
        return vec![];
    }
    let per_branch = docs.len().div_ceil(max_branches.max(1)).max(1);
    let mut out = Vec::with_capacity(docs.len().div_ceil(per_branch));
    let mut iter = docs.into_iter().peekable();
    while iter.peek().is_some() {
        let chunk: Vec<_> = iter.by_ref().take(per_branch).collect();
        out.push(PluginInput::Documents(chunk));
    }
    out
}

/// Temporal retry parameters derived from a step's [`RetryConfig`].
#[derive(Debug, Clone, PartialEq)]
pub struct RetryParams {
    /// Maximum attempts (1 = no retry).
    pub maximum_attempts: u32,
    /// Backoff coefficient.
    pub backoff_coefficient: f64,
    /// Initial interval in seconds.
    pub initial_interval_secs: u64,
}

/// Map a [`RetryConfig`] onto Temporal retry parameters.
pub fn retry_params(cfg: &RetryConfig) -> RetryParams {
    match cfg.backoff {
        Backoff::Exponential => RetryParams {
            maximum_attempts: cfg.max_attempts.max(1),
            backoff_coefficient: 2.0,
            initial_interval_secs: cfg.initial_interval_secs.max(1),
        },
        Backoff::Linear => RetryParams {
            maximum_attempts: cfg.max_attempts.max(1),
            backoff_coefficient: 1.0,
            initial_interval_secs: cfg.initial_interval_secs.max(1),
        },
        Backoff::None => RetryParams {
            maximum_attempts: cfg.max_attempts.max(1),
            backoff_coefficient: 1.0,
            initial_interval_secs: 0,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meili_ingest_plugin_sdk::Document;

    fn pipeline() -> PipelineDefinition {
        let mut p = PipelineDefinition {
            uid: "t".into(),
            name: "t".into(),
            description: None,
            version: 1,
            trigger: None,
            steps: vec![
                StepDefinition::new("a", "x"),
                StepDefinition::new("b", "x").depends_on(["a"]),
                StepDefinition::new("c", "x").depends_on(["a"]),
                StepDefinition::new("d", "x").depends_on(["b", "c"]),
            ],
            builtin: false,
            project_id: None,
        };
        p.normalize();
        p
    }

    #[test]
    fn ready_steps_follow_dependencies() {
        let p = pipeline();
        let order = p.validate().unwrap();
        let mut done = HashMap::new();
        let ids = |v: Vec<&StepDefinition>| v.iter().map(|s| s.id.clone()).collect::<Vec<_>>();
        assert_eq!(ids(ready_steps(&p, &order, &done)), vec!["a"]);
        done.insert("a".into(), PluginOutput::Empty);
        assert_eq!(ids(ready_steps(&p, &order, &done)), vec!["b", "c"]);
        done.insert("b".into(), PluginOutput::Empty);
        assert_eq!(ids(ready_steps(&p, &order, &done)), vec!["c"]);
        done.insert("c".into(), PluginOutput::Empty);
        assert_eq!(ids(ready_steps(&p, &order, &done)), vec!["d"]);
        done.insert("d".into(), PluginOutput::Empty);
        assert!(ready_steps(&p, &order, &done).is_empty());
    }

    #[test]
    fn resolve_input_variants() {
        let p = pipeline();
        let initial = PluginInput::Documents(vec![Document::with_id("i", "init")]);
        let mut done = HashMap::new();
        done.insert(
            "b".into(),
            PluginOutput::Documents(vec![Document::with_id("b", "b")]),
        );
        done.insert(
            "c".into(),
            PluginOutput::Documents(vec![Document::with_id("c", "c")]),
        );
        assert_eq!(
            resolve_input(p.step("a").unwrap(), &initial, &done),
            initial
        );
        assert_eq!(
            resolve_input(p.step("d").unwrap(), &initial, &done),
            PluginInput::Many(vec![
                PluginOutput::Documents(vec![Document::with_id("b", "b")]),
                PluginOutput::Documents(vec![Document::with_id("c", "c")]),
            ])
        );
        done.insert(
            "a".into(),
            PluginOutput::Documents(vec![Document::with_id("a", "a")]),
        );
        assert_eq!(
            resolve_input(p.step("b").unwrap(), &initial, &done),
            PluginInput::Documents(vec![Document::with_id("a", "a")])
        );
    }

    #[test]
    fn fan_out_documents_one_per_doc_under_cap() {
        let docs: Vec<Document> = (0..5)
            .map(|i| Document::with_id(format!("d{i}"), "x"))
            .collect();
        match fan_out_branches("$.documents", PluginOutput::Documents(docs), 1000) {
            FanOut::Branches(b) => {
                assert_eq!(b.len(), 5);
                assert!(matches!(&b[0], PluginInput::Documents(d) if d.len() == 1));
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn fan_out_documents_grouped_when_over_cap() {
        let docs: Vec<Document> = (0..10)
            .map(|i| Document::with_id(format!("d{i}"), "x"))
            .collect();
        match fan_out_branches("$.documents", PluginOutput::Documents(docs), 4) {
            FanOut::Branches(b) => {
                assert_eq!(b.len(), 4);
                let total: usize = b
                    .iter()
                    .map(|i| match i {
                        PluginInput::Documents(d) => d.len(),
                        _ => 0,
                    })
                    .sum();
                assert_eq!(total, 10);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn fan_out_ref_needs_activity() {
        let r = PluginOutput::Ref(meili_ingest_plugin_sdk::ContentRef::Staged {
            uri: "jobs/x/y.json".into(),
            mime: None,
            filename: None,
        });
        assert!(matches!(
            fan_out_branches("$.documents", r, 10),
            FanOut::NeedsActivity(_)
        ));
    }

    #[test]
    fn fan_out_many_and_identity() {
        let many = PluginOutput::Many(vec![PluginOutput::Empty, PluginOutput::Empty]);
        assert!(
            matches!(fan_out_branches("$.many", many.clone(), 10), FanOut::Branches(b) if b.len() == 2)
        );
        assert!(matches!(fan_out_branches("$", many, 10), FanOut::Branches(b) if b.len() == 1));
    }

    #[test]
    fn retry_mapping() {
        let exp = retry_params(&RetryConfig::default());
        assert_eq!(
            exp,
            RetryParams {
                maximum_attempts: 3,
                backoff_coefficient: 2.0,
                initial_interval_secs: 1
            }
        );
        let none = retry_params(&RetryConfig {
            max_attempts: 5,
            backoff: Backoff::None,
            initial_interval_secs: 9,
        });
        assert_eq!(none.initial_interval_secs, 0);
        assert_eq!(none.maximum_attempts, 5);
        let lin = retry_params(&RetryConfig {
            max_attempts: 0,
            backoff: Backoff::Linear,
            initial_interval_secs: 2,
        });
        assert_eq!(lin.maximum_attempts, 1);
        assert_eq!(lin.backoff_coefficient, 1.0);
    }
}
