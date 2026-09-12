//! gRPC plugins: remote containers implementing `proto/plugin.proto`.
//!
//! [`GrpcPlugin::connect`] dials the endpoint, fetches the manifest with `GetManifest`
//! and then forwards every `execute` call as an `Execute` RPC. Messages up to 64 MiB
//! are accepted in both directions.

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
        let endpoint = endpoint.into();
        let channel = Endpoint::from_shared(endpoint.clone())
            .map_err(|e| RuntimeError::Transport(format!("{endpoint}: {e}")))?
            .connect_timeout(std::time::Duration::from_secs(10))
            .connect()
            .await
            .map_err(|e| RuntimeError::Transport(format!("{endpoint}: {e}")))?;

        let mut client = PluginServiceClient::new(channel)
            .max_decoding_message_size(MAX_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_BYTES);

        let mut manifest = match manifest_override {
            Some(m) => m,
            None => {
                let resp = client
                    .get_manifest(GetManifestRequest {})
                    .await
                    .map_err(|s| RuntimeError::Rpc(format!("{endpoint} GetManifest: {s}")))?
                    .into_inner();
                serde_json::from_slice(&resp.manifest_json)?
            }
        };
        manifest.kind = PluginKind::Grpc;

        tracing::info!(plugin = %manifest.name, version = %manifest.version, endpoint = %endpoint, "connected grpc plugin");
        Ok(Self {
            client,
            manifest,
            endpoint,
        })
    }

    /// The endpoint this plugin talks to.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

/// Map a transport-level gRPC status onto plugin retry semantics.
fn status_to_error(plugin: &str, status: Status) -> PluginError {
    let msg = format!(
        "grpc plugin {plugin}: {} ({:?})",
        status.message(),
        status.code()
    );
    match status.code() {
        Code::Unavailable
        | Code::DeadlineExceeded
        | Code::ResourceExhausted
        | Code::Aborted
        | Code::Unknown
        | Code::Internal
        | Code::Cancelled => PluginError::Retryable(msg),
        _ => PluginError::NonRetryable(msg),
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
