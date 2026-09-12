use std::time::Duration;
use serde::{Deserialize, Serialize};
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, UntypedQuery, UntypedSignal,
    UntypedWorkflow, WorkflowQueryOptions, WorkflowSignalOptions, WorkflowStartOptions, Url,
};
use temporalio_common::data_converters::{PayloadConverter, RawValue};
use temporalio_common::RetryPolicy;
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    activities::{ActivityContext, ActivityError},
    workflows::join_all,
    ActivityOptions, ApplicationFailure, SyncWorkflowContext, WorkflowContext,
    WorkflowContextView, WorkflowResult, Runtime, Worker, WorkerOptions,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepInput { pub plugin: String, pub payload: serde_json::Value }
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepOutput { pub payload: serde_json::Value }
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WfInput { pub job_id: uuid::Uuid, pub steps: Vec<StepInput> }
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WfOutput { pub job_id: uuid::Uuid, pub outputs: Vec<StepOutput> }
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Progress { pub current_step: String, pub completed: usize, pub total: usize, pub cancelled: bool }

pub struct StepActivities;

#[activities]
impl StepActivities {
    #[activity]
    pub async fn execute_step(ctx: ActivityContext, input: StepInput) -> Result<StepOutput, ActivityError> {
        if ctx.is_cancelled() { return Err(ActivityError::cancelled()); }
        ctx.record_heartbeat(()).await?;
        if input.plugin == "boom" {
            return Err(ActivityError::application(ApplicationFailure::non_retryable(anyhow::anyhow!("bad plugin"))));
        }
        Ok(StepOutput { payload: input.payload })
    }
}

fn queue_for(plugin: &str) -> &'static str {
    match plugin { "llm_enricher" => "workers-llm", _ => "workers-general" }
}

#[workflow]
#[derive(Default)]
pub struct PipelineWorkflow { progress: Progress }

#[workflow_methods]
impl PipelineWorkflow {
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>, input: WfInput) -> WorkflowResult<WfOutput> {
        let total = input.steps.len();
        ctx.state_mut(|s| s.progress.total = total);
        let mut outputs = Vec::new();
        // fan-out
        let handles: Vec<_> = input.steps.iter().map(|step| {
            ctx.execute_activity(
                StepActivities::execute_step,
                step.clone(),
                ActivityOptions::with_start_to_close_timeout(Duration::from_secs(120))
                    .task_queue(queue_for(&step.plugin).to_string())
                    .heartbeat_timeout(Duration::from_secs(30))
                    .retry_policy(RetryPolicy::builder().maximum_attempts(3).build())
                    .build(),
            )
        }).collect();
        let results = join_all(handles).await;
        for r in results {
            match r {
                Ok(o) => outputs.push(o),
                Err(e) => return Err(ApplicationFailure::non_retryable(anyhow::anyhow!("step failed: {e}")).into()),
            }
        }
        if ctx.state(|s| s.progress.cancelled) {
            return Err(ApplicationFailure::non_retryable(anyhow::anyhow!("cancelled")).into());
        }
        Ok(WfOutput { job_id: input.job_id, outputs })
    }

    #[signal]
    pub fn cancel(&mut self, _ctx: &mut SyncWorkflowContext<Self>, _input: ()) { self.progress.cancelled = true; }

    #[query]
    pub fn current_step(&self, _ctx: &WorkflowContextView) -> String { self.progress.current_step.clone() }

    #[query]
    pub fn progress(&self, _ctx: &WorkflowContextView) -> Progress { self.progress.clone() }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url: Url = "http://localhost:7233".parse()?;
    let connection = Connection::connect(ConnectionOptions::new(url).build()).await?;
    let client = Client::new(connection, ClientOptions::new("default").build())?;

    // untyped start from gateway side
    let conv = PayloadConverter::default();
    let input = WfInput { job_id: uuid::Uuid::new_v4(), steps: vec![] };
    let raw = RawValue::from_value(&input, &conv);
    let handle = client.start_workflow(
        UntypedWorkflow::new("PipelineWorkflow"),
        raw,
        WorkflowStartOptions::new("workers-general", format!("ingest-{}", input.job_id)).build(),
    ).await?;
    let h2 = client.get_workflow_handle::<UntypedWorkflow>(handle.info().workflow_id.clone());
    h2.signal(UntypedSignal::new("cancel"), RawValue::from_value(&(), &conv), WorkflowSignalOptions::default()).await?;
    let out: RawValue = h2.query(UntypedQuery::new("progress"), RawValue::from_value(&(), &conv), WorkflowQueryOptions::default()).await?;
    let p: Progress = out.to_value(&conv);
    println!("{p:?}");
    let desc = h2.describe(Default::default()).await?;
    println!("{:?}", desc.status());

    // worker side
    let runtime = Runtime::from_current_tokio(Default::default())?;
    let opts = WorkerOptions::new("workers-general")
        .register_workflow::<PipelineWorkflow>()?
        .register_activities(StepActivities)
        .build();
    let mut worker = Worker::new(&runtime, client, opts)?;
    worker.run().await?;
    Ok(())
}
