//! End-to-end test: an in-process tonic server implementing `PluginService` (an "echo"
//! plugin) is driven through `GrpcPlugin` over a real TCP socket.
#![cfg(feature = "grpc")]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use meili_ingest_plugin_runtime::grpc::proto::execute_response::Result as RpcResult;
use meili_ingest_plugin_runtime::grpc::proto::plugin_service_server::{
    PluginService, PluginServiceServer,
};
use meili_ingest_plugin_runtime::grpc::proto::{
    ExecuteError, ExecuteRequest, ExecuteResponse, GetManifestRequest, GetManifestResponse,
};
use meili_ingest_plugin_runtime::{ExternalPluginSpec, GrpcPlugin, RuntimeError, load};
use meili_ingest_plugin_sdk::prelude::*;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

/// Uppercases document content; fails on demand via `config.fail`.
#[derive(Default)]
struct EchoService {
    calls: Arc<AtomicBool>,
}

#[tonic::async_trait]
impl PluginService for EchoService {
    async fn get_manifest(
        &self,
        _req: Request<GetManifestRequest>,
    ) -> Result<Response<GetManifestResponse>, Status> {
        let manifest = PluginManifest::new("echo", "1.2.3")
            .description("uppercases content")
            .accepts([InputKind::Documents])
            .produces(OutputKind::Documents);
        Ok(Response::new(GetManifestResponse {
            manifest_json: serde_json::to_vec(&manifest)
                .map_err(|e| Status::internal(e.to_string()))?,
        }))
    }

    async fn execute(
        &self,
        req: Request<ExecuteRequest>,
    ) -> Result<Response<ExecuteResponse>, Status> {
        self.calls.store(true, Ordering::SeqCst);
        let req = req.into_inner();
        assert!(!req.job_id.is_empty());
        assert_eq!(req.attempt, 1);
        let config: serde_json::Value = serde_json::from_slice(&req.config_json)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        match config["fail"].as_str() {
            Some("retryable") => {
                return Ok(Response::new(ExecuteResponse {
                    result: Some(RpcResult::Error(ExecuteError {
                        message: "try later".into(),
                        retryable: true,
                    })),
                }));
            }
            Some("fatal") => {
                return Ok(Response::new(ExecuteResponse {
                    result: Some(RpcResult::Error(ExecuteError {
                        message: "nope".into(),
                        retryable: false,
                    })),
                }));
            }
            Some("status") => return Err(Status::unavailable("down for maintenance")),
            _ => {}
        }
        let input: PluginInput = serde_json::from_slice(&req.input_json)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let docs = input
            .into_documents()
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let out = PluginOutput::Documents(
            docs.into_iter()
                .map(|mut d| {
                    d.content = d.content.to_uppercase();
                    d.fields.insert(
                        "step".into(),
                        serde_json::Value::String(req.step_id.clone()),
                    );
                    d
                })
                .collect(),
        );
        Ok(Response::new(ExecuteResponse {
            result: Some(RpcResult::OutputJson(
                serde_json::to_vec(&out).map_err(|e| Status::internal(e.to_string()))?,
            )),
        }))
    }
}

async fn spawn_server() -> (String, Arc<AtomicBool>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let svc = EchoService::default();
    let calls = svc.calls.clone();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(PluginServiceServer::new(svc))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    (format!("http://{addr}"), calls)
}

#[tokio::test]
async fn grpc_plugin_round_trips_manifest_and_execute() {
    let (endpoint, calls) = spawn_server().await;
    let plugin = GrpcPlugin::connect(endpoint.clone()).await.unwrap();

    let manifest = plugin.manifest();
    assert_eq!(manifest.name, "echo");
    assert_eq!(manifest.version, "1.2.3");
    assert_eq!(manifest.kind, PluginKind::Grpc);
    assert_eq!(plugin.endpoint(), endpoint);

    let input = PluginInput::Documents(vec![Document::with_id("a", "hello")]);
    let out = plugin
        .execute(&ActivityContext::noop(), input, serde_json::json!({}))
        .await
        .unwrap();
    let docs = out.into_documents().unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].content, "HELLO");
    assert_eq!(docs[0].fields["step"], "test");
    assert!(calls.load(Ordering::SeqCst));
}

#[tokio::test]
async fn grpc_plugin_maps_errors() {
    let (endpoint, _) = spawn_server().await;
    let plugin = GrpcPlugin::connect(endpoint).await.unwrap();
    let ctx = ActivityContext::noop();

    let err = plugin
        .execute(
            &ctx,
            PluginInput::Empty,
            serde_json::json!({"fail": "retryable"}),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, PluginError::Retryable(m) if m == "try later"));

    let err = plugin
        .execute(
            &ctx,
            PluginInput::Empty,
            serde_json::json!({"fail": "fatal"}),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, PluginError::NonRetryable(m) if m == "nope"));

    let err = plugin
        .execute(
            &ctx,
            PluginInput::Empty,
            serde_json::json!({"fail": "status"}),
        )
        .await
        .unwrap_err();
    assert!(
        err.is_retryable(),
        "UNAVAILABLE must be retryable, got {err:?}"
    );
}

#[tokio::test]
async fn manifest_override_skips_get_manifest_and_load_returns_dyn_plugin() {
    let (endpoint, _) = spawn_server().await;
    let mut spec = ExternalPluginSpec::new(PluginKind::Grpc, endpoint);
    spec.manifest = Some(PluginManifest::new("renamed", "0.0.1").produces(OutputKind::Documents));
    let plugin = load(&spec).await.unwrap();
    assert_eq!(plugin.manifest().name, "renamed");
    assert_eq!(plugin.manifest().kind, PluginKind::Grpc);
}

#[tokio::test]
async fn unreachable_endpoint_is_a_transport_error() {
    // Bind then drop a listener so the port is very likely closed.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let err = GrpcPlugin::connect(format!("http://{addr}"))
        .await
        .err()
        .unwrap();
    assert!(matches!(err, RuntimeError::Transport(_)), "got {err:?}");
    let err = GrpcPlugin::connect("not a uri").await.err().unwrap();
    assert!(matches!(err, RuntimeError::Transport(_)), "got {err:?}");
}
