//! Pure, deterministic helpers used by the workflow to schedule steps. Kept free of
//! Temporal types so they can be unit-tested directly.

use std::collections::HashMap;

use meili_ingest_plugin_sdk::{
    Backoff, INDEXER_PLUGIN, IndexReport, JobStatus, PipelineDefinition, PluginInput, PluginOutput,
    RetryConfig, StepActivityOutput, StepDefinition, StepResult, UsageUnits,
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
    /// The upstream output is, or contains, a spilled reference; an activity must
    /// expand it.
    NeedsActivity(PluginOutput),
    /// The upstream output cannot be split along the path (a `$.documents` fan-out
    /// over bytes, say): the step fails with this reason.
    Invalid(String),
}

/// Whether `out` is a reference or holds one at any depth.
fn contains_ref(out: &PluginOutput) -> bool {
    match out {
        PluginOutput::Ref(_) => true,
        PluginOutput::Many(items) => items.iter().any(contains_ref),
        _ => false,
    }
}

/// Split the upstream output into fan-out branches according to `path`
/// (`$.documents`, `$.many` or `$`). Documents are grouped so that at most
/// `max_branches` activities are scheduled.
///
/// An upstream with no documents at all (`Empty`, `Indexed`, an empty list) is zero
/// branches. One that is not documents is [`FanOut::Invalid`], never an empty split:
/// that would report a job that dropped everything as succeeded.
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
        // `$.documents` over the output of a fanned-out step: a branch that was
        // spilled sits as a `Ref` inside the `Many`, and only an activity can read it.
        (_, out) if contains_ref(&out) => FanOut::NeedsActivity(out),
        (_, out) => match out.into_documents() {
            Ok(docs) => FanOut::Branches(group_documents(docs, max_branches)),
            Err(e) => FanOut::Invalid(e.to_string()),
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

/// What a step consumed, for the job's usage event.
#[derive(Debug, Clone, PartialEq)]
pub struct StepMetering {
    /// Parallel branches the step ran as (1 unless fanned out).
    pub branches: usize,
    /// Wall time, summed across the branches that completed.
    pub duration_ms: u64,
    /// Bytes consumed, summed across the branches that completed.
    pub input_bytes: u64,
    /// Billable units the plugin reported, summed across the branches that completed.
    pub usage: UsageUnits,
}

impl StepMetering {
    /// A step of `branches` branches that metered nothing.
    pub fn none(branches: usize) -> Self {
        Self {
            branches,
            duration_ms: 0,
            input_bytes: 0,
            usage: UsageUnits::none(),
        }
    }
}

/// What a completed step produced, plus the metering the usage event needs.
#[derive(Debug, Clone, PartialEq)]
pub struct StepOutcome {
    /// The step's output (`Many` of the branch outputs when fanned out).
    pub output: PluginOutput,
    /// What it consumed.
    pub metering: StepMetering,
}

/// Why a step did not complete. Both carry what the step consumed before it stopped:
/// branches that finished spent tokens and CPU whatever happened to their siblings.
#[derive(Debug, Clone, PartialEq)]
pub enum StepFailure {
    /// Cancelled. `Some` when branches had already completed.
    Cancelled(Option<StepMetering>),
    /// Failed, with the reason.
    Failed {
        /// Why.
        message: String,
        /// What the branches that completed consumed.
        metering: StepMetering,
    },
}

/// Why one branch activity did not complete.
#[derive(Debug, Clone, PartialEq)]
pub enum BranchError {
    /// The activity was cancelled.
    Cancelled,
    /// The activity failed, with the reason.
    Failed(String),
}

/// Fold the results of a fanned-out step's branches into the step's outcome.
///
/// Metering is summed across branches: one fanned-out step is one billable line,
/// however many activities carried it out. A failed branch fails the step and takes
/// precedence over a cancelled one; either way the sums of the branches that did
/// complete are kept.
pub fn merge_branches(
    results: Vec<Result<StepActivityOutput, BranchError>>,
) -> Result<StepOutcome, StepFailure> {
    let total = results.len();
    let mut outputs = Vec::with_capacity(total);
    let mut metering = StepMetering::none(total);
    let mut cancelled = false;
    let mut first_error: Option<String> = None;
    for result in results {
        match result {
            Ok(o) => {
                metering.duration_ms += o.duration_ms;
                metering.input_bytes += o.input_bytes;
                metering.usage.merge(o.usage);
                outputs.push(o.output);
            }
            Err(BranchError::Cancelled) => cancelled = true,
            Err(BranchError::Failed(msg)) => {
                first_error.get_or_insert(msg);
            }
        }
    }
    if let Some(msg) = first_error {
        return Err(StepFailure::Failed {
            message: format!(
                "{} of {total} branches failed; first error: {msg}",
                total - outputs.len()
            ),
            metering,
        });
    }
    if cancelled {
        return Err(StepFailure::Cancelled(
            (!outputs.is_empty()).then_some(metering),
        ));
    }
    Ok(StepOutcome {
        output: PluginOutput::Many(outputs),
        metering,
    })
}

/// How a round of parallel steps ends the job, when it does.
#[derive(Debug, Clone, PartialEq)]
pub enum RoundEnd {
    /// A step was cancelled.
    Cancelled,
    /// A step failed; the job's error message.
    Failed(String),
}

/// What one round of parallel steps did to the job.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Round {
    /// One row per step to record, the steps that completed first: a job that stops
    /// here must end on the row of a step that did not complete (the usage event
    /// reads the job's document count off the last row).
    pub rows: Vec<StepResult>,
    /// Outputs of the steps that completed, by step id.
    pub outputs: Vec<(String, PluginOutput)>,
    /// The indexer's report, when the indexer completed in this round.
    pub index_report: Option<IndexReport>,
    /// Set when the job stops here: the first step, in `ready` order, that did not
    /// complete decides how.
    pub end: Option<RoundEnd>,
}

fn step_row(
    step: &StepDefinition,
    status: JobStatus,
    document_count: usize,
    error: Option<String>,
    metering: StepMetering,
) -> StepResult {
    StepResult {
        step_id: step.id.clone(),
        plugin: step.plugin.clone(),
        status,
        document_count,
        branches: metering.branches,
        error,
        duration_ms: metering.duration_ms,
        input_bytes: metering.input_bytes,
        usage: metering.usage,
    }
}

/// Settle one round: `results[i]` is what running `ready[i]` gave.
///
/// Every step of the round has finished by now, so every one that consumed something
/// is recorded, whatever happened to its siblings: a step that completed, a step that
/// failed, and a cancelled step whose branches had partly completed.
pub fn settle_round(
    ready: &[StepDefinition],
    results: Vec<Result<StepOutcome, StepFailure>>,
) -> Round {
    let mut round = Round::default();
    let mut stopped = Vec::new();
    for (step, result) in ready.iter().zip(results) {
        match result {
            Ok(outcome) => {
                if let PluginOutput::Indexed(r) = &outcome.output
                    && step.plugin == INDEXER_PLUGIN
                {
                    round.index_report = Some(r.clone());
                }
                round.rows.push(step_row(
                    step,
                    JobStatus::Succeeded,
                    outcome.output.document_count(),
                    None,
                    outcome.metering,
                ));
                round.outputs.push((step.id.clone(), outcome.output));
            }
            Err(StepFailure::Cancelled(partial)) => {
                round.end.get_or_insert(RoundEnd::Cancelled);
                if let Some(metering) = partial {
                    stopped.push(step_row(step, JobStatus::Cancelled, 0, None, metering));
                }
            }
            Err(StepFailure::Failed { message, metering }) => {
                round.end.get_or_insert_with(|| {
                    RoundEnd::Failed(format!(
                        "step {} ({}) failed: {message}",
                        step.id, step.plugin
                    ))
                });
                stopped.push(step_row(
                    step,
                    JobStatus::Failed,
                    0,
                    Some(message),
                    metering,
                ));
            }
        }
    }
    round.rows.extend(stopped);
    round
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
            tenant_id: None,
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
            origin: None,
        });
        assert!(matches!(
            fan_out_branches("$.documents", r, 10),
            FanOut::NeedsActivity(_)
        ));
    }

    fn spilled_ref() -> PluginOutput {
        PluginOutput::Ref(meili_ingest_plugin_sdk::ContentRef::Staged {
            uri: "jobs/x/b-1.json".into(),
            mime: Some("application/vnd.meili-ingest.output+json".into()),
            filename: None,
            origin: None,
        })
    }

    #[test]
    fn fan_out_documents_with_a_nested_ref_needs_activity() {
        // The output of a fanned-out step whose second branch was spilled.
        let upstream = PluginOutput::Many(vec![
            PluginOutput::Documents(vec![Document::with_id("a", "x")]),
            spilled_ref(),
        ]);
        assert_eq!(
            fan_out_branches("$.documents", upstream.clone(), 10),
            FanOut::NeedsActivity(upstream)
        );
        // Deeper nesting is found too.
        let nested = PluginOutput::Many(vec![PluginOutput::Many(vec![spilled_ref()])]);
        assert!(matches!(
            fan_out_branches("$.documents", nested, 10),
            FanOut::NeedsActivity(_)
        ));
    }

    #[test]
    fn fan_out_documents_over_bytes_is_invalid() {
        let bytes = PluginOutput::Bytes(meili_ingest_plugin_sdk::Blob::new(
            vec![1, 2, 3],
            "application/pdf",
            None,
        ));
        match fan_out_branches("$.documents", bytes, 10) {
            FanOut::Invalid(why) => assert!(why.contains("expected documents"), "{why}"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn fan_out_documents_over_nothing_is_zero_branches() {
        // A legitimate empty result, not a mis-wired pipeline.
        for upstream in [
            PluginOutput::Empty,
            PluginOutput::Documents(vec![]),
            PluginOutput::Indexed(Default::default()),
            PluginOutput::Many(vec![]),
        ] {
            assert_eq!(
                fan_out_branches("$.documents", upstream, 10),
                FanOut::Branches(vec![])
            );
        }
    }

    #[test]
    fn fan_out_many_passes_nested_refs_through() {
        let many = PluginOutput::Many(vec![spilled_ref(), PluginOutput::Empty]);
        match fan_out_branches("$.many", many, 10) {
            FanOut::Branches(b) => {
                assert_eq!(b.len(), 2);
                assert!(matches!(b[0], PluginInput::Ref(_)));
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn fan_out_many_and_identity() {
        let many = PluginOutput::Many(vec![PluginOutput::Empty, PluginOutput::Empty]);
        assert!(
            matches!(fan_out_branches("$.many", many.clone(), 10), FanOut::Branches(b) if b.len() == 2)
        );
        assert!(matches!(fan_out_branches("$", many, 10), FanOut::Branches(b) if b.len() == 1));
    }

    fn branch_ok(tokens: u64, duration_ms: u64, input_bytes: u64) -> StepActivityOutput {
        StepActivityOutput {
            step_id: "enrich".into(),
            output: PluginOutput::Documents(vec![Document::with_id("d", "x")]),
            duration_ms,
            usage: UsageUnits::llm(tokens, 1),
            input_bytes,
            documents_out: 1,
        }
    }

    #[test]
    fn merged_branches_sum_their_metering() {
        let out = merge_branches(vec![Ok(branch_ok(10, 5, 100)), Ok(branch_ok(20, 7, 200))])
            .expect("both branches completed");
        assert_eq!(out.output.document_count(), 2);
        assert!(matches!(&out.output, PluginOutput::Many(o) if o.len() == 2));
        assert_eq!(out.metering.branches, 2);
        assert_eq!(out.metering.duration_ms, 12);
        assert_eq!(out.metering.input_bytes, 300);
        assert_eq!(out.metering.usage.llm_input_tokens, 30);
        assert_eq!(out.metering.usage.llm_requests, 2);
    }

    #[test]
    fn a_failed_branch_keeps_what_its_siblings_consumed() {
        let err = merge_branches(vec![
            Ok(branch_ok(10, 5, 100)),
            Err(BranchError::Failed("provider said no".into())),
            Ok(branch_ok(20, 7, 200)),
            Err(BranchError::Failed("second error".into())),
        ])
        .expect_err("a branch failed");
        let StepFailure::Failed { message, metering } = err else {
            panic!("unexpected {err:?}");
        };
        assert_eq!(
            message,
            "2 of 4 branches failed; first error: provider said no"
        );
        assert_eq!(metering.branches, 4);
        assert_eq!(metering.duration_ms, 12);
        assert_eq!(metering.input_bytes, 300);
        assert_eq!(metering.usage.llm_input_tokens, 30);
    }

    #[test]
    fn a_failed_branch_wins_over_a_cancelled_one() {
        let err = merge_branches(vec![
            Err(BranchError::Cancelled),
            Err(BranchError::Failed("boom".into())),
        ])
        .expect_err("nothing completed");
        assert!(
            matches!(&err, StepFailure::Failed { message, .. } if message.starts_with("2 of 2")),
            "{err:?}"
        );
    }

    #[test]
    fn a_cancelled_fan_out_keeps_what_completed() {
        let err = merge_branches(vec![Ok(branch_ok(10, 5, 100)), Err(BranchError::Cancelled)])
            .expect_err("a branch was cancelled");
        let StepFailure::Cancelled(Some(metering)) = err else {
            panic!("unexpected {err:?}");
        };
        assert_eq!(metering.branches, 2);
        assert_eq!(metering.usage.llm_input_tokens, 10);
        // Nothing completed: nothing to meter.
        assert_eq!(
            merge_branches(vec![Err(BranchError::Cancelled)]),
            Err(StepFailure::Cancelled(None))
        );
    }

    #[test]
    fn a_fan_out_over_nothing_is_an_empty_step() {
        let out = merge_branches(vec![]).expect("nothing to run");
        assert_eq!(out.output, PluginOutput::Many(vec![]));
        assert_eq!(out.metering, StepMetering::none(0));
    }

    fn outcome(tokens: u64) -> StepOutcome {
        StepOutcome {
            output: PluginOutput::Documents(vec![
                Document::with_id("a", "x"),
                Document::with_id("b", "y"),
            ]),
            metering: StepMetering {
                branches: 1,
                duration_ms: 40,
                input_bytes: 9,
                usage: UsageUnits::llm(tokens, 1),
            },
        }
    }

    fn failed(message: &str, metering: StepMetering) -> StepFailure {
        StepFailure::Failed {
            message: message.into(),
            metering,
        }
    }

    fn steps(ids: &[&str]) -> Vec<StepDefinition> {
        ids.iter().map(|id| StepDefinition::new(*id, "x")).collect()
    }

    #[test]
    fn a_round_of_completed_steps_is_recorded_in_order() {
        let round = settle_round(&steps(&["b", "c"]), vec![Ok(outcome(1)), Ok(outcome(2))]);
        assert_eq!(round.end, None);
        assert_eq!(round.index_report, None);
        let ids: Vec<_> = round.outputs.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(ids, ["b", "c"]);
        assert_eq!(round.rows.len(), 2);
        let row = &round.rows[0];
        assert_eq!(row.step_id, "b");
        assert_eq!(row.plugin, "x");
        assert_eq!(row.status, JobStatus::Succeeded);
        assert_eq!(row.document_count, 2);
        assert_eq!(row.branches, 1);
        assert_eq!(row.error, None);
        assert_eq!(row.duration_ms, 40);
        assert_eq!(row.input_bytes, 9);
        assert_eq!(row.usage.llm_input_tokens, 1);
    }

    #[test]
    fn a_sibling_that_completed_after_a_failed_step_is_still_recorded() {
        let round = settle_round(
            &steps(&["b", "c"]),
            vec![Err(failed("boom", StepMetering::none(1))), Ok(outcome(400))],
        );
        assert_eq!(
            round.end,
            Some(RoundEnd::Failed("step b (x) failed: boom".into()))
        );
        assert_eq!(round.outputs.len(), 1);
        // The sibling's tokens are billed; the failed step comes last, so the job
        // reports no documents.
        let rows: Vec<_> = round
            .rows
            .iter()
            .map(|r| (r.step_id.as_str(), r.status, r.usage.llm_input_tokens))
            .collect();
        assert_eq!(
            rows,
            [
                ("c", JobStatus::Succeeded, 400),
                ("b", JobStatus::Failed, 0)
            ]
        );
        let last = round.rows.last().expect("rows");
        assert_eq!(last.document_count, 0);
        assert_eq!(last.error.as_deref(), Some("boom"));
        assert_eq!(last.branches, 1);
    }

    #[test]
    fn a_partly_failed_fan_out_is_recorded_with_what_it_consumed() {
        let metering = StepMetering {
            branches: 500,
            duration_ms: 1_000,
            input_bytes: 2_000,
            usage: UsageUnits::llm(900, 90),
        };
        let round = settle_round(
            &steps(&["enrich"]),
            vec![Err(failed("1 of 500 branches failed", metering))],
        );
        assert_eq!(round.rows.len(), 1);
        let row = &round.rows[0];
        assert_eq!(row.status, JobStatus::Failed);
        assert_eq!(row.branches, 500);
        assert_eq!(row.document_count, 0);
        assert_eq!(row.duration_ms, 1_000);
        assert_eq!(row.input_bytes, 2_000);
        assert_eq!(row.usage.llm_input_tokens, 900);
        assert!(round.outputs.is_empty());
    }

    #[test]
    fn every_failed_step_gets_a_row_and_the_first_names_the_job_error() {
        let round = settle_round(
            &steps(&["b", "c"]),
            vec![
                Err(failed("first", StepMetering::none(1))),
                Err(failed("second", StepMetering::none(1))),
            ],
        );
        assert_eq!(
            round.end,
            Some(RoundEnd::Failed("step b (x) failed: first".into()))
        );
        let ids: Vec<_> = round.rows.iter().map(|r| r.step_id.as_str()).collect();
        assert_eq!(ids, ["b", "c"]);
    }

    #[test]
    fn a_cancelled_round_still_records_what_completed() {
        let partial = StepMetering {
            branches: 4,
            duration_ms: 30,
            input_bytes: 3,
            usage: UsageUnits::llm(7, 1),
        };
        let round = settle_round(
            &steps(&["b", "c", "d"]),
            vec![
                Err(StepFailure::Cancelled(None)),
                Ok(outcome(400)),
                Err(StepFailure::Cancelled(Some(partial))),
            ],
        );
        assert_eq!(round.end, Some(RoundEnd::Cancelled));
        // `b` consumed nothing and gets no row; `d` had branches that completed.
        let rows: Vec<_> = round
            .rows
            .iter()
            .map(|r| (r.step_id.as_str(), r.status, r.usage.llm_input_tokens))
            .collect();
        assert_eq!(
            rows,
            [
                ("c", JobStatus::Succeeded, 400),
                ("d", JobStatus::Cancelled, 7)
            ]
        );
        assert_eq!(round.rows[1].branches, 4);
    }

    #[test]
    fn a_failed_step_ends_the_job_on_its_own_row() {
        let round = settle_round(
            &steps(&["a", "b"]),
            vec![Ok(outcome(1)), Err(failed("boom", StepMetering::none(1)))],
        );
        assert_eq!(
            round.end,
            Some(RoundEnd::Failed("step b (x) failed: boom".into()))
        );
        let last = round.rows.last().expect("rows");
        assert_eq!(last.step_id, "b");
        assert_eq!(last.status, JobStatus::Failed);
        assert_eq!(last.document_count, 0);
        assert_eq!(last.error.as_deref(), Some("boom"));
        assert_eq!(round.outputs.len(), 1);
    }

    #[test]
    fn the_first_step_that_did_not_complete_decides_how_the_job_ends() {
        let round = settle_round(
            &steps(&["b", "c"]),
            vec![
                Err(StepFailure::Cancelled(None)),
                Err(failed("boom", StepMetering::none(1))),
            ],
        );
        assert_eq!(round.end, Some(RoundEnd::Cancelled));
    }

    #[test]
    fn the_indexer_report_is_kept_only_for_the_indexer() {
        let report = IndexReport {
            index: "movies".into(),
            ..Default::default()
        };
        let indexed = |report: &IndexReport| StepOutcome {
            output: PluginOutput::Indexed(report.clone()),
            metering: StepMetering::none(1),
        };
        let ready = vec![
            StepDefinition::new("other", "x"),
            StepDefinition::new("index", INDEXER_PLUGIN),
        ];
        let round = settle_round(&ready, vec![Ok(indexed(&report)), Ok(indexed(&report))]);
        assert_eq!(round.index_report, Some(report.clone()));
        let round = settle_round(&ready[..1], vec![Ok(indexed(&report))]);
        assert_eq!(round.index_report, None);
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
