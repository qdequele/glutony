//! gRPC plugins: remote containers implementing `proto/plugin.proto`.
//!
//! [`GrpcPlugin::connect`] dials the endpoint, fetches the manifest with `GetManifest`
//! and then forwards every `execute` call as an `Execute` RPC. `GetManifest` has a
//! deadline, and [`GrpcConnectOptions::retry_for`] keeps trying a container that is not
//! up yet (see [`crate::load`]).
//!
//! Messages up to 64 MiB are accepted in both directions.

use std::time::{Duration, Instant};

use meili_ingest_plugin_sdk::{
    ActivityContext, Plugin, PluginError, PluginInput, PluginKind, PluginManifest, PluginOutput,
    async_trait,
};
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Status};

use crate::error::RuntimeError;

/// Generated protobuf/tonic code for `meili_ingest.plugin.v1`.
///
/// Exposed so tests and Rust-based plugin containers can implement
/// [`proto::plugin_service_server::PluginService`] with the same types.
pub mod proto {
    tonic::include_proto!("meili_ingest.plugin.v1");
}

use proto::execute_response::Result as RpcResult;
use proto::plugin_service_client::PluginServiceClient;
use proto::{ExecuteRequest, GetManifestRequest};

/// Maximum gRPC message size accepted and produced by the client (64 MiB).
pub const MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

/// How long [`crate::load`] keeps retrying a gRPC plugin that is not ready, so a
/// container starting next to the worker is not lost to a boot race.
pub const LOAD_RETRY_FOR: Duration = Duration::from_secs(30);

/// First pause between two attempts; it doubles up to [`RETRY_PAUSE_MAX`].
const RETRY_PAUSE_START: Duration = Duration::from_millis(250);
const RETRY_PAUSE_MAX: Duration = Duration::from_secs(4);

/// How [`GrpcPlugin::connect_with_options`] reaches a plugin container.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GrpcConnectOptions {
    /// Longest one connection attempt may take.
    pub connect_timeout: Duration,
    /// Longest the plugin may take to answer `GetManifest`. `Execute` calls are not
    /// bounded by it: the step's own timeout governs those.
    pub manifest_timeout: Duration,
    /// Keep retrying a plugin that cannot be reached or does not answer yet until
    /// this much time has passed. Zero (the default) tries once.
    pub retry_for: Duration,
}

impl Default for GrpcConnectOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            manifest_timeout: Duration::from_secs(10),
            retry_for: Duration::ZERO,
        }
    }
}

/// Why one attempt to reach a plugin failed.
enum AttemptError {
    /// The plugin may simply not be up yet.
    NotReady(RuntimeError),
    /// Trying again cannot help.
    Fatal(RuntimeError),
}

/// A plugin backed by a remote gRPC container.
#[derive(Debug, Clone)]
pub struct GrpcPlugin {
    client: PluginServiceClient<Channel>,
    manifest: PluginManifest,
    endpoint: String,
}

impl GrpcPlugin {
    /// Connect to `endpoint` (e.g. `http://whisper:50051`) and fetch the manifest.
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self, RuntimeError> {
        Self::connect_with_manifest(endpoint, None).await
    }

    /// Connect to `endpoint`; when `manifest_override` is given the `GetManifest` RPC is
    /// skipped and the override is used instead (for containers that only implement
    /// `Execute`, or to rename a plugin).
    pub async fn connect_with_manifest(
        endpoint: impl Into<String>,
        manifest_override: Option<PluginManifest>,
    ) -> Result<Self, RuntimeError> {
        Self::connect_with_options(endpoint, manifest_override, GrpcConnectOptions::default()).await
    }

    /// [`GrpcPlugin::connect_with_manifest`] with explicit timeouts and, when
    /// [`GrpcConnectOptions::retry_for`] is set, retries for a plugin that is not up yet.
    pub async fn connect_with_options(
        endpoint: impl Into<String>,
        manifest_override: Option<PluginManifest>,
        options: GrpcConnectOptions,
    ) -> Result<Self, RuntimeError> {
        let endpoint = endpoint.into();
        let target = Endpoint::from_shared(endpoint.clone())
            .map_err(|e| RuntimeError::Transport(format!("{endpoint}: {e}")))?
            .connect_timeout(options.connect_timeout);

        let started = Instant::now();
        let mut pause = RETRY_PAUSE_START;
        loop {
            let error = match Self::attempt(&target, &endpoint, manifest_override.as_ref(), options)
                .await
            {
                Ok(plugin) => return Ok(plugin),
                Err(AttemptError::Fatal(e)) => return Err(e),
                Err(AttemptError::NotReady(e)) => e,
            };
            if started.elapsed() + pause > options.retry_for {
                return Err(error);
            }
            tracing::warn!(endpoint = %endpoint, error = %error, retry_in = ?pause, "grpc plugin is not ready yet");
            tokio::time::sleep(pause).await;
            pause = (pause * 2).min(RETRY_PAUSE_MAX);
        }
    }

    /// Dial once and learn the manifest.
    async fn attempt(
        target: &Endpoint,
        endpoint: &str,
        manifest_override: Option<&PluginManifest>,
        options: GrpcConnectOptions,
    ) -> Result<Self, AttemptError> {
        let channel = target.connect().await.map_err(|e| {
            AttemptError::NotReady(RuntimeError::Transport(format!("{endpoint}: {e}")))
        })?;

        let mut client = PluginServiceClient::new(channel)
            .max_decoding_message_size(MAX_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_BYTES);

        let mut manifest = match manifest_override {
            Some(m) => m.clone(),
            None => {
                // Bound this call only: a deadline on the channel would also cut off
                // long `Execute` calls.
                let answer = tokio::time::timeout(
                    options.manifest_timeout,
                    client.get_manifest(GetManifestRequest {}),
                )
                .await
                .map_err(|_| {
                    AttemptError::NotReady(RuntimeError::Rpc(format!(
                        "{endpoint} GetManifest: no answer within {:?}",
                        options.manifest_timeout
                    )))
                })?;
                let response = answer.map_err(|status| {
                    let transient = is_transient(status.code());
                    let error = RuntimeError::Rpc(format!("{endpoint} GetManifest: {status}"));
                    if transient {
                        AttemptError::NotReady(error)
                    } else {
                        AttemptError::Fatal(error)
                    }
                })?;
                serde_json::from_slice(&response.into_inner().manifest_json)
                    .map_err(|e| AttemptError::Fatal(e.into()))?
            }
        };
        manifest.kind = PluginKind::Grpc;

        tracing::info!(plugin = %manifest.name, version = %manifest.version, endpoint = %endpoint, "connected grpc plugin");
        Ok(Self {
            client,
            manifest,
            endpoint: endpoint.to_string(),
        })
    }

    /// The endpoint this plugin talks to.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

/// Whether a gRPC status describes a failure worth trying again.
fn is_transient(code: Code) -> bool {
    matches!(
        code,
        Code::Unavailable
            | Code::DeadlineExceeded
            | Code::ResourceExhausted
            | Code::Aborted
            | Code::Unknown
            | Code::Internal
            | Code::Cancelled
    )
}

/// Map a transport-level gRPC status onto plugin retry semantics.
fn status_to_error(plugin: &str, status: Status) -> PluginError {
    let msg = format!(
        "grpc plugin {plugin}: {} ({:?})",
        status.message(),
        status.code()
    );
    if is_transient(status.code()) {
        PluginError::Retryable(msg)
    } else {
        PluginError::NonRetryable(msg)
    }
}

#[async_trait]
impl Plugin for GrpcPlugin {
    fn manifest(&self) -> PluginManifest {
        self.manifest.clone()
    }

    async fn execute(
        &self,
        ctx: &ActivityContext,
        input: PluginInput,
        config: serde_json::Value,
    ) -> Result<PluginOutput, PluginError> {
        ctx.check_cancelled()?;
        let name = self.manifest.name.as_str();
        let request = ExecuteRequest {
            job_id: ctx.job_id().to_string(),
            step_id: ctx.step_id().to_string(),
            input_json: serde_json::to_vec(&input)?,
            config_json: serde_json::to_vec(&config)?,
            attempt: ctx.attempt(),
        };
        ctx.heartbeat(format!("grpc {name}: calling Execute on {}", self.endpoint));

        let mut client = self.client.clone();
        let response = client
            .execute(request)
            .await
            .map_err(|s| status_to_error(name, s))?
            .into_inner();

        match response.result {
            Some(RpcResult::OutputJson(bytes)) => serde_json::from_slice(&bytes).map_err(|e| {
                PluginError::non_retryable(format!("grpc plugin {name}: invalid output_json: {e}"))
            }),
            Some(RpcResult::Error(e)) if e.retryable => Err(PluginError::Retryable(e.message)),
            Some(RpcResult::Error(e)) => Err(PluginError::NonRetryable(e.message)),
            None => Err(PluginError::non_retryable(format!(
                "grpc plugin {name}: empty ExecuteResponse"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_statuses_that_may_pass_are_transient() {
        assert!(is_transient(Code::Unavailable));
        assert!(is_transient(Code::DeadlineExceeded));
        assert!(!is_transient(Code::Unimplemented));
        assert!(!is_transient(Code::InvalidArgument));
    }
}
