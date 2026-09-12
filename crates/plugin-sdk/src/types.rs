//! Shared data types: tenant context, documents, plugin I/O, pipeline definitions
//! and workflow payloads. Every public type derives `Debug`, `Clone`, `Serialize`,
//! `Deserialize` so it can travel through Temporal payloads unchanged.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::PluginError;

// ---------------------------------------------------------------------------
// Tenant context
// ---------------------------------------------------------------------------

/// Tenant context. Resolved once at the gateway from the Envoy-injected `X-Meili-*`
/// headers (or from env vars in standalone mode) and carried immutably through the
/// whole pipeline as part of the workflow input. Workers never read Meilisearch
/// credentials from the environment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeiliContext {
    /// Tenant / project identifier (from `X-Meili-Project-Id`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    /// Full Meilisearch host URL (e.g. `https://xxx.us-west.meilisearch.io`).
    pub host: String,
    /// Meilisearch API key with write access.
    pub api_key: String,
    /// Target index. Fully resolved before the workflow starts (see index chain in SPEC §3.4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<String>,
    /// Region tag for observability.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
}

impl MeiliContext {
    /// Redacted view for logs: never prints the API key.
    pub fn redacted(&self) -> String {
        format!(
            "MeiliContext{{project_id={:?}, host={}, index={:?}, region={:?}}}",
            self.project_id, self.host, self.index, self.region
        )
    }
}

// ---------------------------------------------------------------------------
// Documents
// ---------------------------------------------------------------------------

/// Provenance and structural metadata attached to every document.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DocumentMeta {
    /// Where the content came from (filename, URL, S3 URI, ...).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// Original filename if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    /// MIME type of the original content.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime: Option<String>,
    /// 1-based page number (PDF, DOCX pages, slides, spreadsheet sheets...).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page: Option<u32>,
    /// Sheet / section / heading name when relevant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub section: Option<String>,
    /// 0-based chunk index, set by the chunker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_index: Option<usize>,
    /// Total chunks produced from the parent document, set by the chunker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_total: Option<usize>,
    /// Id of the document this one was derived from (chunk → parent).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    /// Detected language (BCP-47) if any step produced it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    /// Free-form extra metadata.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra: BTreeMap<String, serde_json::Value>,
}

/// The unit of data flowing between steps and into Meilisearch.
///
/// `fields` carries arbitrary structured attributes (CSV columns, JSON keys, LLM
/// extractions). When indexed, the document is flattened as
/// `{ id, title?, content, ...fields, _meta: { ... } }`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Document {
    /// Stable identifier. Must be Meilisearch-safe (`[a-zA-Z0-9_-]`, ≤ 511 bytes).
    pub id: String,
    /// Optional title.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Main textual content.
    #[serde(default)]
    pub content: String,
    /// Structured attributes.
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub fields: serde_json::Map<String, serde_json::Value>,
    /// Provenance metadata.
    #[serde(default)]
    pub meta: DocumentMeta,
}

impl Document {
    /// Create a document with content and a fresh UUID id.
    pub fn new(content: impl Into<String>) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            content: content.into(),
            ..Default::default()
        }
    }

    /// Create a document with an explicit id.
    pub fn with_id(id: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            id: sanitize_id(&id.into()),
            content: content.into(),
            ..Default::default()
        }
    }

    /// Flatten into the JSON object sent to Meilisearch.
    pub fn to_index_json(&self) -> serde_json::Value {
        let mut obj = serde_json::Map::new();
        obj.insert("id".into(), serde_json::Value::String(self.id.clone()));
        if let Some(t) = &self.title {
            obj.insert("title".into(), serde_json::Value::String(t.clone()));
        }
        obj.insert(
            "content".into(),
            serde_json::Value::String(self.content.clone()),
        );
        for (k, v) in &self.fields {
            if k != "id" && k != "content" && k != "_meta" {
                obj.insert(k.clone(), v.clone());
            }
        }
        obj.insert(
            "_meta".into(),
            serde_json::to_value(&self.meta).unwrap_or(serde_json::Value::Null),
        );
        serde_json::Value::Object(obj)
    }
}

/// Make an arbitrary string a valid Meilisearch document id
/// (only `a-zA-Z0-9`, `-` and `_`; max 511 bytes).
pub fn sanitize_id(raw: &str) -> String {
    let mut out: String = raw
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if out.is_empty() {
        out = Uuid::new_v4().to_string();
    }
    if out.len() > 511 {
        out.truncate(511);
    }
    out
}

// ---------------------------------------------------------------------------
// Plugin I/O
// ---------------------------------------------------------------------------

/// Raw binary content plus what we know about it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Blob {
    /// The bytes (base64 in JSON).
    #[serde(with = "base64_bytes")]
    pub data: Vec<u8>,
    /// Detected MIME type.
    pub mime: String,
    /// Original filename if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
}

impl Blob {
    /// Build a blob.
    pub fn new(data: Vec<u8>, mime: impl Into<String>, filename: Option<String>) -> Self {
        Self {
            data,
            mime: mime.into(),
            filename,
        }
    }

    /// Interpret the bytes as UTF-8 text (lossy).
    pub fn text_lossy(&self) -> String {
        String::from_utf8_lossy(&self.data).into_owned()
    }
}

/// A reference to content the worker must fetch before running the plugin.
/// Plugins never see these: the activity runner resolves them into [`Blob`]s.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ContentRef {
    /// Public/authenticated HTTP(S) URL.
    Url {
        /// The URL.
        url: String,
        /// MIME hint.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        mime: Option<String>,
        /// Filename hint.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filename: Option<String>,
    },
    /// `s3://bucket/key` (or any URI understood by the configured object store).
    S3 {
        /// The URI.
        uri: String,
        /// MIME hint.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        mime: Option<String>,
        /// Filename hint.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filename: Option<String>,
    },
    /// An object in meili-ingest's own blob store (gateway staged an upload, or an
    /// activity spilled a large output). `uri` is relative to the configured store.
    Staged {
        /// Object key inside the blob store.
        uri: String,
        /// MIME hint.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        mime: Option<String>,
        /// Filename hint.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filename: Option<String>,
    },
}

impl ContentRef {
    /// MIME hint, whichever variant.
    pub fn mime(&self) -> Option<&str> {
        match self {
            ContentRef::Url { mime, .. }
            | ContentRef::S3 { mime, .. }
            | ContentRef::Staged { mime, .. } => mime.as_deref(),
        }
    }

    /// Filename hint, whichever variant.
    pub fn filename(&self) -> Option<&str> {
        match self {
            ContentRef::Url { filename, .. }
            | ContentRef::S3 { filename, .. }
            | ContentRef::Staged { filename, .. } => filename.as_deref(),
        }
    }

    /// The location string.
    pub fn location(&self) -> &str {
        match self {
            ContentRef::Url { url, .. } => url,
            ContentRef::S3 { uri, .. } | ContentRef::Staged { uri, .. } => uri,
        }
    }
}

/// What a plugin receives.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum PluginInput {
    /// Raw bytes (file upload, fetched URL, previous step's binary output).
    Bytes(Blob),
    /// Content that still has to be fetched. Resolved by the worker before dispatch.
    Ref(ContentRef),
    /// Structured documents (from a previous step or inline JSON in the request).
    Documents(Vec<Document>),
    /// Merged outputs of a fan-out step (one entry per parallel branch).
    Many(Vec<PluginOutput>),
    /// Nothing.
    Empty,
}

impl PluginInput {
    /// Which [`InputKind`] this is.
    pub fn kind(&self) -> InputKind {
        match self {
            PluginInput::Bytes(_) => InputKind::Bytes,
            PluginInput::Ref(_) => InputKind::Ref,
            PluginInput::Documents(_) => InputKind::Documents,
            PluginInput::Many(_) => InputKind::Many,
            PluginInput::Empty => InputKind::Empty,
        }
    }

    /// Return the bytes or an `InvalidInput` error.
    pub fn into_bytes(self) -> Result<Blob, PluginError> {
        match self {
            PluginInput::Bytes(b) => Ok(b),
            other => Err(PluginError::InvalidInput(format!(
                "expected bytes, got {:?}",
                other.kind()
            ))),
        }
    }

    /// Return the documents, flattening `Many` (recursively) into one list.
    /// Errors on `Bytes`/`Ref`.
    pub fn into_documents(self) -> Result<Vec<Document>, PluginError> {
        match self {
            PluginInput::Documents(d) => Ok(d),
            PluginInput::Many(outs) => {
                let mut all = Vec::new();
                for o in outs {
                    all.extend(o.into_documents()?);
                }
                Ok(all)
            }
            PluginInput::Empty => Ok(Vec::new()),
            other => Err(PluginError::InvalidInput(format!(
                "expected documents, got {:?}",
                other.kind()
            ))),
        }
    }

    /// Approximate serialized size in bytes (used to decide spilling to blob storage).
    pub fn approx_size(&self) -> usize {
        match self {
            PluginInput::Bytes(b) => b.data.len() * 4 / 3,
            PluginInput::Ref(_) => 256,
            PluginInput::Documents(d) => d.iter().map(doc_size).sum(),
            PluginInput::Many(m) => m.iter().map(PluginOutput::approx_size).sum(),
            PluginInput::Empty => 8,
        }
    }
}

impl From<PluginOutput> for PluginInput {
    fn from(o: PluginOutput) -> Self {
        match o {
            PluginOutput::Bytes(b) => PluginInput::Bytes(b),
            PluginOutput::Ref(r) => PluginInput::Ref(r),
            PluginOutput::Documents(d) => PluginInput::Documents(d),
            PluginOutput::Many(m) => PluginInput::Many(m),
            PluginOutput::Indexed(_) | PluginOutput::Empty => PluginInput::Empty,
        }
    }
}

/// Result of pushing documents to Meilisearch.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexReport {
    /// Index the documents went to.
    pub index: String,
    /// Number of documents sent.
    pub document_count: usize,
    /// Meilisearch task uids created.
    pub task_uids: Vec<u32>,
}

/// What a plugin returns.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum PluginOutput {
    /// Binary output (e.g. audio extracted from video).
    Bytes(Blob),
    /// Large output spilled to the blob store by the worker. Plugins never return this
    /// themselves; the worker rewrites oversized outputs into it.
    Ref(ContentRef),
    /// Documents.
    Documents(Vec<Document>),
    /// Fan-out results, one per branch.
    Many(Vec<PluginOutput>),
    /// Terminal result of the indexer.
    Indexed(IndexReport),
    /// Nothing.
    Empty,
}

impl PluginOutput {
    /// Which [`OutputKind`] this is.
    pub fn kind(&self) -> OutputKind {
        match self {
            PluginOutput::Bytes(_) => OutputKind::Bytes,
            PluginOutput::Ref(_) => OutputKind::Ref,
            PluginOutput::Documents(_) => OutputKind::Documents,
            PluginOutput::Many(_) => OutputKind::Many,
            PluginOutput::Indexed(_) => OutputKind::Indexed,
            PluginOutput::Empty => OutputKind::Empty,
        }
    }

    /// Flatten into a document list (recursing through `Many`). `Indexed`/`Empty` → empty.
    pub fn into_documents(self) -> Result<Vec<Document>, PluginError> {
        match self {
            PluginOutput::Documents(d) => Ok(d),
            PluginOutput::Many(m) => {
                let mut all = Vec::new();
                for o in m {
                    all.extend(o.into_documents()?);
                }
                Ok(all)
            }
            PluginOutput::Indexed(_) | PluginOutput::Empty => Ok(Vec::new()),
            other => Err(PluginError::InvalidInput(format!(
                "expected documents, got {:?}",
                other.kind()
            ))),
        }
    }

    /// Number of documents, recursively.
    pub fn document_count(&self) -> usize {
        match self {
            PluginOutput::Documents(d) => d.len(),
            PluginOutput::Many(m) => m.iter().map(PluginOutput::document_count).sum(),
            PluginOutput::Indexed(r) => r.document_count,
            _ => 0,
        }
    }

    /// Approximate serialized size in bytes.
    pub fn approx_size(&self) -> usize {
        match self {
            PluginOutput::Bytes(b) => b.data.len() * 4 / 3,
            PluginOutput::Ref(_) => 256,
            PluginOutput::Documents(d) => d.iter().map(doc_size).sum(),
            PluginOutput::Many(m) => m.iter().map(PluginOutput::approx_size).sum(),
            PluginOutput::Indexed(_) => 128,
            PluginOutput::Empty => 8,
        }
    }
}

fn doc_size(d: &Document) -> usize {
    d.id.len()
        + d.content.len()
        + d.title.as_ref().map(String::len).unwrap_or(0)
        + serde_json::to_vec(&d.fields).map(|v| v.len()).unwrap_or(0)
        + 256
}

/// Input variants a plugin declares it accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputKind {
    /// [`PluginInput::Bytes`]
    Bytes,
    /// [`PluginInput::Ref`] (only plugins that stream their own input, e.g. an S3 downloader)
    Ref,
    /// [`PluginInput::Documents`]
    Documents,
    /// [`PluginInput::Many`]
    Many,
    /// [`PluginInput::Empty`]
    Empty,
}

/// Output variants a plugin declares it produces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputKind {
    /// [`PluginOutput::Bytes`]
    Bytes,
    /// [`PluginOutput::Ref`]
    Ref,
    /// [`PluginOutput::Documents`]
    Documents,
    /// [`PluginOutput::Many`]
    Many,
    /// [`PluginOutput::Indexed`]
    Indexed,
    /// [`PluginOutput::Empty`]
    Empty,
}

/// How a plugin is executed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum PluginKind {
    /// Compiled into the worker binary.
    #[default]
    Builtin,
    /// Loaded at runtime via extism/wasmtime.
    Wasm,
    /// Remote container called over gRPC.
    Grpc,
}

/// Static description of a plugin.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginManifest {
    /// Unique plugin name referenced by `steps[].plugin` (snake_case).
    pub name: String,
    /// Semver version string.
    pub version: String,
    /// Human description.
    #[serde(default)]
    pub description: String,
    /// Input variants accepted.
    #[serde(default)]
    pub accepts: Vec<InputKind>,
    /// Output variant produced.
    pub produces: OutputKind,
    /// JSON Schema for the step `config:` block.
    #[serde(default = "default_schema")]
    pub config_schema: serde_json::Value,
    /// Execution kind.
    #[serde(default)]
    pub kind: PluginKind,
    /// MIME types this plugin is designed for (informational; used by `GET /plugins`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub content_types: Vec<String>,
}

fn default_schema() -> serde_json::Value {
    serde_json::json!({"type": "object"})
}

impl PluginManifest {
    /// Start a manifest with name and version.
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
            description: String::new(),
            accepts: vec![],
            produces: OutputKind::Documents,
            config_schema: default_schema(),
            kind: PluginKind::Builtin,
            content_types: vec![],
        }
    }
    /// Set the description.
    pub fn description(mut self, d: impl Into<String>) -> Self {
        self.description = d.into();
        self
    }
    /// Set accepted input kinds.
    pub fn accepts(mut self, kinds: impl IntoIterator<Item = InputKind>) -> Self {
        self.accepts = kinds.into_iter().collect();
        self
    }
    /// Set produced output kind.
    pub fn produces(mut self, kind: OutputKind) -> Self {
        self.produces = kind;
        self
    }
    /// Set the JSON Schema of the config block.
    pub fn config_schema(mut self, schema: serde_json::Value) -> Self {
        self.config_schema = schema;
        self
    }
    /// Set the execution kind.
    pub fn kind(mut self, kind: PluginKind) -> Self {
        self.kind = kind;
        self
    }
    /// Set the MIME types this plugin handles.
    pub fn content_types<S: Into<String>>(mut self, types: impl IntoIterator<Item = S>) -> Self {
        self.content_types = types.into_iter().map(Into::into).collect();
        self
    }
    /// Whether the plugin accepts this input kind.
    pub fn accepts_kind(&self, kind: InputKind) -> bool {
        self.accepts.contains(&kind)
    }
}

// ---------------------------------------------------------------------------
// Pipeline definition
// ---------------------------------------------------------------------------

/// Backoff strategy for step retries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Backoff {
    /// Doubles each attempt.
    #[default]
    Exponential,
    /// Constant interval.
    Linear,
    /// Retry immediately.
    None,
}

/// Retry policy for a step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Maximum attempts including the first one.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    /// Backoff strategy.
    #[serde(default)]
    pub backoff: Backoff,
    /// Initial interval in seconds.
    #[serde(default = "default_initial_interval")]
    pub initial_interval_secs: u64,
}

fn default_max_attempts() -> u32 {
    3
}
fn default_initial_interval() -> u64 {
    1
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: default_max_attempts(),
            backoff: Backoff::Exponential,
            initial_interval_secs: default_initial_interval(),
        }
    }
}

/// One node of the pipeline DAG.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StepDefinition {
    /// Unique step id within the pipeline.
    pub id: String,
    /// Plugin name (matches [`PluginManifest::name`]).
    pub plugin: String,
    /// Steps this one waits for. Empty = runs on the initial input (or after the previous
    /// step when using the implicit sequential form, see [`PipelineDefinition::normalize`]).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,
    /// JSONPath into the (single) upstream output; each match becomes one parallel activity.
    /// The canonical value is `$.documents`, meaning "one activity per document".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fan_out: Option<String>,
    /// Plugin config block.
    #[serde(default = "empty_object")]
    pub config: serde_json::Value,
    /// Start-to-close timeout in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
    /// Retry policy (defaults to 3 attempts, exponential).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry: Option<RetryConfig>,
}

fn empty_object() -> serde_json::Value {
    serde_json::Value::Object(Default::default())
}

impl StepDefinition {
    /// Minimal step.
    pub fn new(id: impl Into<String>, plugin: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            plugin: plugin.into(),
            depends_on: vec![],
            fan_out: None,
            config: empty_object(),
            timeout_secs: None,
            retry: None,
        }
    }
    /// Set dependencies.
    pub fn depends_on<S: Into<String>>(mut self, deps: impl IntoIterator<Item = S>) -> Self {
        self.depends_on = deps.into_iter().map(Into::into).collect();
        self
    }
    /// Set config.
    pub fn config(mut self, config: serde_json::Value) -> Self {
        self.config = config;
        self
    }
    /// Set fan-out path.
    pub fn fan_out(mut self, path: impl Into<String>) -> Self {
        self.fan_out = Some(path.into());
        self
    }
    /// Set timeout.
    pub fn timeout_secs(mut self, secs: u64) -> Self {
        self.timeout_secs = Some(secs);
        self
    }
    /// Effective timeout (default 300s).
    pub fn effective_timeout_secs(&self) -> u64 {
        self.timeout_secs.unwrap_or(300)
    }
    /// Effective retry config.
    pub fn effective_retry(&self) -> RetryConfig {
        self.retry.clone().unwrap_or_default()
    }
}

/// When a pipeline is auto-selected by `POST /ingest`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PipelineTrigger {
    /// MIME types (exact match, or `type/*` wildcard).
    #[serde(default)]
    pub content_types: Vec<String>,
    /// Glob on the filename (e.g. `contract_*.pdf`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filename_pattern: Option<String>,
    /// Index to write to when this pipeline is selected (overrides header/query).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_pattern: Option<String>,
}

/// A full pipeline. Stored as JSONB in Postgres, authored as YAML or JSON.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PipelineDefinition {
    /// Unique id, e.g. `builtin.pdf` or `my-pdf-with-enrichment`.
    pub uid: String,
    /// Display name.
    #[serde(default)]
    pub name: String,
    /// Description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Version, bumped on update.
    #[serde(default = "one")]
    pub version: u32,
    /// Auto-routing trigger.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trigger: Option<PipelineTrigger>,
    /// Steps (a DAG).
    pub steps: Vec<StepDefinition>,
    /// True for pipelines compiled into the control plane (cannot be deleted).
    #[serde(default)]
    pub builtin: bool,
    /// Tenant scope. `None` = global.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
}

fn one() -> u32 {
    1
}

/// Errors from pipeline validation.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
pub enum PipelineError {
    /// No steps.
    #[error("pipeline has no steps")]
    Empty,
    /// Empty or invalid uid.
    #[error("invalid pipeline uid {0:?}: must be non-empty and match [a-zA-Z0-9._-]+")]
    InvalidUid(String),
    /// Two steps share an id.
    #[error("duplicate step id {0:?}")]
    DuplicateStep(String),
    /// `depends_on` names a step that does not exist.
    #[error("step {step:?} depends on unknown step {dep:?}")]
    UnknownDependency {
        /// The step.
        step: String,
        /// The missing dependency.
        dep: String,
    },
    /// Cycle detected.
    #[error("pipeline has a cycle involving steps {0:?}")]
    Cycle(Vec<String>),
    /// A fan-out step must have exactly one dependency.
    #[error("fan_out step {0:?} must depend on exactly one step")]
    FanOutArity(String),
    /// Unsupported fan-out path.
    #[error(
        "fan_out path {path:?} on step {step:?} is not supported (use \"$.documents\" or \"$.many\")"
    )]
    FanOutPath {
        /// The step.
        step: String,
        /// The path.
        path: String,
    },
}

impl PipelineDefinition {
    /// Parse from YAML or JSON (tries JSON first, then YAML) and normalize.
    pub fn parse(text: &str) -> Result<Self, String> {
        let mut def: PipelineDefinition = match serde_json::from_str(text) {
            Ok(v) => v,
            Err(_) => serde_yaml_from_str(text)?,
        };
        def.normalize();
        Ok(def)
    }

    /// Apply the implicit sequential rule: a step with no `depends_on` that is **not** the
    /// first step depends on the step right before it. Steps that explicitly want to run
    /// on the initial input in parallel with others should set `depends_on: []`
    /// explicitly and be listed first — in practice, list root steps first.
    ///
    /// This is deliberately simple: authors writing linear pipelines never need
    /// `depends_on`; authors writing DAGs write `depends_on` on every non-root step.
    pub fn normalize(&mut self) {
        let has_any_explicit = self.steps.iter().any(|s| !s.depends_on.is_empty());
        if has_any_explicit {
            // Explicit DAG mode: only fill in the very common "first step(s) are roots,
            // remaining steps without depends_on follow the previous step" pattern
            // when the previous step exists and the step isn't a root.
            for i in 1..self.steps.len() {
                if self.steps[i].depends_on.is_empty() {
                    let prev = self.steps[i - 1].id.clone();
                    self.steps[i].depends_on = vec![prev];
                }
            }
        } else {
            for i in 1..self.steps.len() {
                let prev = self.steps[i - 1].id.clone();
                self.steps[i].depends_on = vec![prev];
            }
        }
        if self.name.is_empty() {
            self.name = self.uid.clone();
        }
    }

    /// Validate structure and return a topological order (Kahn's algorithm). Steps
    /// with no mutual dependency are ordered by their position in `steps` for
    /// determinism.
    pub fn validate(&self) -> Result<Vec<String>, PipelineError> {
        if self.uid.is_empty()
            || !self
                .uid
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
        {
            return Err(PipelineError::InvalidUid(self.uid.clone()));
        }
        if self.steps.is_empty() {
            return Err(PipelineError::Empty);
        }
        let mut ids = HashSet::new();
        for s in &self.steps {
            if !ids.insert(s.id.as_str()) {
                return Err(PipelineError::DuplicateStep(s.id.clone()));
            }
        }
        for s in &self.steps {
            for d in &s.depends_on {
                if !ids.contains(d.as_str()) {
                    return Err(PipelineError::UnknownDependency {
                        step: s.id.clone(),
                        dep: d.clone(),
                    });
                }
            }
            if let Some(path) = &s.fan_out {
                if s.depends_on.len() != 1 {
                    return Err(PipelineError::FanOutArity(s.id.clone()));
                }
                if !matches!(path.as_str(), "$.documents" | "$.many" | "$") {
                    return Err(PipelineError::FanOutPath {
                        step: s.id.clone(),
                        path: path.clone(),
                    });
                }
            }
        }
        // Kahn
        let index: HashMap<&str, usize> = self
            .steps
            .iter()
            .enumerate()
            .map(|(i, s)| (s.id.as_str(), i))
            .collect();
        let mut indegree = vec![0usize; self.steps.len()];
        let mut children: Vec<Vec<usize>> = vec![vec![]; self.steps.len()];
        for (i, s) in self.steps.iter().enumerate() {
            for d in &s.depends_on {
                let j = index[d.as_str()];
                indegree[i] += 1;
                children[j].push(i);
            }
        }
        let mut queue: VecDeque<usize> = (0..self.steps.len())
            .filter(|&i| indegree[i] == 0)
            .collect();
        let mut order = Vec::with_capacity(self.steps.len());
        while let Some(i) = queue.pop_front() {
            order.push(self.steps[i].id.clone());
            let mut ready: Vec<usize> = Vec::new();
            for &c in &children[i] {
                indegree[c] -= 1;
                if indegree[c] == 0 {
                    ready.push(c);
                }
            }
            ready.sort_unstable();
            queue.extend(ready);
        }
        if order.len() != self.steps.len() {
            let stuck: Vec<String> = self
                .steps
                .iter()
                .enumerate()
                .filter(|(i, _)| indegree[*i] > 0)
                .map(|(_, s)| s.id.clone())
                .collect();
            return Err(PipelineError::Cycle(stuck));
        }
        Ok(order)
    }

    /// Look up a step by id.
    pub fn step(&self, id: &str) -> Option<&StepDefinition> {
        self.steps.iter().find(|s| s.id == id)
    }

    /// Whether this pipeline's trigger matches a MIME type / filename.
    pub fn trigger_matches(&self, mime: &str, filename: Option<&str>) -> bool {
        let Some(t) = &self.trigger else { return false };
        let mime_ok = t.content_types.iter().any(|ct| mime_matches(ct, mime));
        if !mime_ok && !t.content_types.is_empty() {
            return false;
        }
        match (&t.filename_pattern, filename) {
            (Some(pat), Some(name)) => glob_match(pat, name),
            (Some(_), None) => false,
            (None, _) => mime_ok,
        }
    }
}

fn serde_yaml_from_str(text: &str) -> Result<PipelineDefinition, String> {
    // Keep serde_yaml out of the public dependency surface of the SDK: a tiny
    // YAML→JSON bridge is provided by the caller (gateway/control-plane) in practice,
    // but we support it here too when the `yaml` cfg is on. Without it, report a
    // helpful error.
    #[cfg(any(test, feature = "yaml"))]
    {
        serde_yaml::from_str(text).map_err(|e| format!("invalid pipeline YAML/JSON: {e}"))
    }
    #[cfg(not(any(test, feature = "yaml")))]
    {
        let _ = text;
        Err("pipeline text is not valid JSON (enable the `yaml` feature to parse YAML)".into())
    }
}

/// `type/*` aware MIME match. `type/subtype;params` on the right side is tolerated.
pub fn mime_matches(pattern: &str, mime: &str) -> bool {
    let mime = mime
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    let pattern = pattern.trim().to_ascii_lowercase();
    if pattern == "*/*" || pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix("/*") {
        return mime.split('/').next() == Some(prefix);
    }
    pattern == mime
}

/// Minimal glob: `*` (any run) and `?` (one char), case-insensitive on the basename.
pub fn glob_match(pattern: &str, name: &str) -> bool {
    let name = name.rsplit(['/', '\\']).next().unwrap_or(name);
    let p: Vec<char> = pattern.to_ascii_lowercase().chars().collect();
    let n: Vec<char> = name.to_ascii_lowercase().chars().collect();
    fn rec(p: &[char], n: &[char]) -> bool {
        match (p.first(), n.first()) {
            (None, None) => true,
            (Some('*'), _) => rec(&p[1..], n) || (!n.is_empty() && rec(p, &n[1..])),
            (Some('?'), Some(_)) => rec(&p[1..], &n[1..]),
            (Some(a), Some(b)) if a == b => rec(&p[1..], &n[1..]),
            _ => false,
        }
    }
    rec(&p, &n)
}

// ---------------------------------------------------------------------------
// Workflow payloads
// ---------------------------------------------------------------------------

/// Input of the `PipelineWorkflow` Temporal workflow. One workflow = one ingest job.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PipelineWorkflowInput {
    /// Job id (also used to build the workflow id `ingest-<job_id>`).
    pub job_id: Uuid,
    /// The full pipeline definition (snapshot at submission time).
    pub pipeline: PipelineDefinition,
    /// Initial input (bytes, ref or documents).
    pub input: PluginInput,
    /// Tenant context, carried end-to-end.
    pub context: MeiliContext,
}

impl PipelineWorkflowInput {
    /// Temporal workflow id for a job.
    pub fn workflow_id(job_id: Uuid) -> String {
        format!("ingest-{job_id}")
    }
}

/// Terminal status of a job / step.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    /// Accepted, workflow not yet picked up.
    #[default]
    Queued,
    /// At least one step running.
    Running,
    /// All steps finished.
    Succeeded,
    /// A step failed permanently.
    Failed,
    /// Cancelled by the user.
    Cancelled,
}

impl JobStatus {
    /// Lower-case string form (matches the JSON representation).
    pub fn as_str(&self) -> &'static str {
        match self {
            JobStatus::Queued => "queued",
            JobStatus::Running => "running",
            JobStatus::Succeeded => "succeeded",
            JobStatus::Failed => "failed",
            JobStatus::Cancelled => "cancelled",
        }
    }
    /// Whether no further transitions are possible.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            JobStatus::Succeeded | JobStatus::Failed | JobStatus::Cancelled
        )
    }
}

/// Per-step outcome recorded in the workflow output / progress query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StepResult {
    /// Step id.
    pub step_id: String,
    /// Plugin name.
    pub plugin: String,
    /// Status.
    pub status: JobStatus,
    /// Number of documents produced (0 for binary outputs).
    #[serde(default)]
    pub document_count: usize,
    /// Number of parallel branches (1 unless fan-out).
    #[serde(default = "one_usize")]
    pub branches: usize,
    /// Error message when failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

fn one_usize() -> usize {
    1
}

/// Progress snapshot exposed by the workflow `progress` query.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WorkflowProgress {
    /// Overall status.
    pub status: JobStatus,
    /// Step currently executing (or last executed).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_step: Option<String>,
    /// Steps completed so far.
    pub completed_steps: usize,
    /// Total steps.
    pub total_steps: usize,
    /// Per-step results in execution order.
    #[serde(default)]
    pub steps: Vec<StepResult>,
    /// Failure message when status is `failed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// True once a cancel signal was received.
    #[serde(default)]
    pub cancel_requested: bool,
}

/// Output of the `PipelineWorkflow`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PipelineWorkflowOutput {
    /// Job id.
    pub job_id: Uuid,
    /// Terminal status.
    pub status: JobStatus,
    /// Per-step results.
    pub steps: Vec<StepResult>,
    /// Index report from the indexer step if the pipeline had one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_report: Option<IndexReport>,
    /// Error message when failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Input of the `execute_step` activity: everything the worker needs to run one plugin
/// invocation. The workflow injects [`MeiliContext`] into `config` for the
/// `meili_indexer` plugin (flattened, see SPEC §7.4).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StepActivityInput {
    /// Job id.
    pub job_id: Uuid,
    /// Step id.
    pub step_id: String,
    /// Plugin name.
    pub plugin: String,
    /// Config block (with `MeiliContext` merged in for the indexer).
    pub config: serde_json::Value,
    /// Input for this invocation (a single branch when fanned out).
    pub input: PluginInput,
    /// Branch index when fanned out (0-based), `None` otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<usize>,
    /// Total branches when fanned out.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch_total: Option<usize>,
    /// Tenant project id for logging / scoping.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
}

/// Output of the `execute_step` activity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StepActivityOutput {
    /// Step id.
    pub step_id: String,
    /// The plugin output (possibly spilled to a [`ContentRef::Staged`] by the worker).
    pub output: PluginOutput,
    /// Wall time in milliseconds.
    #[serde(default)]
    pub duration_ms: u64,
}

/// The `meili_indexer` plugin's config, once the workflow merged the tenant context in.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexerConfig {
    /// Tenant context (flattened into the config object).
    #[serde(flatten)]
    pub meili: MeiliContext,
    /// Primary key to declare when creating the index.
    #[serde(default = "default_primary_key")]
    pub primary_key: String,
    /// Create the index if missing.
    #[serde(default = "default_true")]
    pub auto_create_index: bool,
    /// Documents per `addDocuments` request.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Wait for Meilisearch tasks to complete before returning.
    #[serde(default = "default_true")]
    pub wait_for_completion: bool,
}

fn default_primary_key() -> String {
    "id".into()
}
fn default_true() -> bool {
    true
}
fn default_batch_size() -> usize {
    1000
}

/// Merge a [`MeiliContext`] into a step config object (used by the workflow for the
/// indexer step). Existing keys in `config` win, except `host`/`api_key` which always
/// come from the context.
pub fn inject_meili_context(config: &mut serde_json::Value, ctx: &MeiliContext) {
    if !config.is_object() {
        *config = serde_json::Value::Object(Default::default());
    }
    let obj = config.as_object_mut().expect("just ensured object");
    let ctx_val = serde_json::to_value(ctx).unwrap_or_default();
    if let serde_json::Value::Object(m) = ctx_val {
        for (k, v) in m {
            if k == "host" || k == "api_key" || !obj.contains_key(&k) {
                obj.insert(k, v);
            }
        }
    }
}

/// Name of the plugin that is the only one allowed to talk to Meilisearch.
pub const INDEXER_PLUGIN: &str = "meili_indexer";

// ---------------------------------------------------------------------------
// base64 serde helper
// ---------------------------------------------------------------------------

mod base64_bytes {
    use base64::Engine;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&base64::engine::general_purpose::STANDARD.encode(bytes))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let s = String::deserialize(d)?;
        base64::engine::general_purpose::STANDARD
            .decode(s.as_bytes())
            .map_err(serde::de::Error::custom)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn step(id: &str, deps: &[&str]) -> StepDefinition {
        StepDefinition::new(id, "noop").depends_on(deps.iter().copied())
    }

    fn pipeline(steps: Vec<StepDefinition>) -> PipelineDefinition {
        PipelineDefinition {
            uid: "test".into(),
            name: "test".into(),
            description: None,
            version: 1,
            trigger: None,
            steps,
            builtin: false,
            project_id: None,
        }
    }

    #[test]
    fn topological_order_linear() {
        let p = pipeline(vec![step("a", &[]), step("b", &["a"]), step("c", &["b"])]);
        assert_eq!(p.validate().unwrap(), vec!["a", "b", "c"]);
    }

    #[test]
    fn topological_order_diamond_is_deterministic() {
        let p = pipeline(vec![
            step("a", &[]),
            step("b", &["a"]),
            step("c", &["a"]),
            step("d", &["b", "c"]),
        ]);
        assert_eq!(p.validate().unwrap(), vec!["a", "b", "c", "d"]);
    }

    #[test]
    fn cycle_is_rejected() {
        let p = pipeline(vec![
            step("a", &["c"]),
            step("b", &["a"]),
            step("c", &["b"]),
        ]);
        assert!(matches!(p.validate(), Err(PipelineError::Cycle(_))));
    }

    #[test]
    fn unknown_dependency_is_rejected() {
        let p = pipeline(vec![step("a", &[]), step("b", &["zzz"])]);
        assert_eq!(
            p.validate(),
            Err(PipelineError::UnknownDependency {
                step: "b".into(),
                dep: "zzz".into()
            })
        );
    }

    #[test]
    fn duplicate_step_is_rejected() {
        let p = pipeline(vec![step("a", &[]), step("a", &[])]);
        assert_eq!(p.validate(), Err(PipelineError::DuplicateStep("a".into())));
    }

    #[test]
    fn empty_pipeline_is_rejected() {
        assert_eq!(pipeline(vec![]).validate(), Err(PipelineError::Empty));
    }

    #[test]
    fn fan_out_needs_one_dependency() {
        let p = pipeline(vec![
            step("a", &[]),
            step("b", &[]),
            StepDefinition::new("c", "x")
                .depends_on(["a", "b"])
                .fan_out("$.documents"),
        ]);
        assert_eq!(p.validate(), Err(PipelineError::FanOutArity("c".into())));
    }

    #[test]
    fn normalize_makes_linear_pipeline_sequential() {
        let mut p = pipeline(vec![step("a", &[]), step("b", &[]), step("c", &[])]);
        p.normalize();
        assert_eq!(p.steps[1].depends_on, vec!["a"]);
        assert_eq!(p.steps[2].depends_on, vec!["b"]);
        assert_eq!(p.validate().unwrap(), vec!["a", "b", "c"]);
    }

    #[test]
    fn parse_yaml_pipeline_from_spec() {
        let yaml = r#"
uid: my-pdf-with-enrichment
name: "PDF with LLM enrichment"
version: 1
trigger:
  content_types: [application/pdf]
  filename_pattern: "contract_*.pdf"
steps:
  - id: extract
    plugin: pdf_extractor
    config:
      per_page: true
    timeout_secs: 120
    retry:
      max_attempts: 3
      backoff: exponential
  - id: chunk
    plugin: chunker
    depends_on: [extract]
    config:
      strategy: sentence
      chunk_size: 512
      overlap: 64
  - id: enrich
    plugin: llm_enricher
    depends_on: [chunk]
    fan_out: "$.documents"
    config:
      model: gpt-4o-mini
  - id: index
    plugin: meili_indexer
    depends_on: [enrich]
"#;
        let p = PipelineDefinition::parse(yaml).unwrap();
        assert_eq!(p.uid, "my-pdf-with-enrichment");
        assert_eq!(p.steps.len(), 4);
        assert_eq!(p.steps[0].timeout_secs, Some(120));
        assert_eq!(p.steps[0].retry.as_ref().unwrap().max_attempts, 3);
        assert_eq!(p.steps[2].fan_out.as_deref(), Some("$.documents"));
        assert_eq!(
            p.validate().unwrap(),
            vec!["extract", "chunk", "enrich", "index"]
        );
        assert!(p.trigger_matches("application/pdf", Some("contract_2024.pdf")));
        assert!(!p.trigger_matches("application/pdf", Some("invoice.pdf")));
        assert!(!p.trigger_matches("application/pdf", None));
    }

    #[test]
    fn trigger_without_filename_pattern_matches_mime_only() {
        let mut p = pipeline(vec![step("a", &[])]);
        p.trigger = Some(PipelineTrigger {
            content_types: vec!["image/*".into()],
            ..Default::default()
        });
        assert!(p.trigger_matches("image/png", None));
        assert!(p.trigger_matches("image/jpeg; charset=binary", Some("x.jpg")));
        assert!(!p.trigger_matches("video/mp4", None));
    }

    #[test]
    fn glob_matching() {
        assert!(glob_match("contract_*.pdf", "contract_2024.pdf"));
        assert!(glob_match("contract_*.pdf", "/tmp/uploads/CONTRACT_a.PDF"));
        assert!(!glob_match("contract_*.pdf", "invoice.pdf"));
        assert!(glob_match("*.csv", "data.csv"));
        assert!(glob_match("report_?.md", "report_1.md"));
        assert!(!glob_match("report_?.md", "report_10.md"));
    }

    #[test]
    fn mime_wildcards() {
        assert!(mime_matches("video/*", "video/mp4"));
        assert!(mime_matches(
            "application/pdf",
            "application/pdf; charset=binary"
        ));
        assert!(!mime_matches("video/*", "audio/mpeg"));
        assert!(mime_matches("*/*", "anything/at-all"));
    }

    #[test]
    fn blob_roundtrips_through_json_as_base64() {
        let input = PluginInput::Bytes(Blob::new(
            vec![0, 1, 2, 255],
            "application/pdf",
            Some("a.pdf".into()),
        ));
        let json = serde_json::to_string(&input).unwrap();
        assert!(json.contains("\"data\":\"AAEC/w==\""));
        let back: PluginInput = serde_json::from_str(&json).unwrap();
        assert_eq!(back, input);
    }

    #[test]
    fn many_flattens_into_documents() {
        let out = PluginOutput::Many(vec![
            PluginOutput::Documents(vec![Document::with_id("a", "x")]),
            PluginOutput::Many(vec![PluginOutput::Documents(vec![Document::with_id(
                "b", "y",
            )])]),
            PluginOutput::Empty,
        ]);
        let docs = PluginInput::from(out).into_documents().unwrap();
        assert_eq!(
            docs.iter().map(|d| d.id.as_str()).collect::<Vec<_>>(),
            vec!["a", "b"]
        );
    }

    #[test]
    fn indexer_config_deserializes_from_flattened_context() {
        let ctx = MeiliContext {
            project_id: Some("xxx".into()),
            host: "http://localhost:7700".into(),
            api_key: "masterKey".into(),
            index: Some("documents".into()),
            region: None,
        };
        let mut config = serde_json::json!({"batch_size": 50, "host": "should-be-overridden"});
        inject_meili_context(&mut config, &ctx);
        let parsed: IndexerConfig = serde_json::from_value(config).unwrap();
        assert_eq!(parsed.meili, ctx);
        assert_eq!(parsed.batch_size, 50);
        assert_eq!(parsed.primary_key, "id");
        assert!(parsed.auto_create_index);
    }

    #[test]
    fn document_index_json_shape() {
        let mut d = Document::with_id("doc 1", "hello");
        d.title = Some("T".into());
        d.fields.insert("price".into(), serde_json::json!(3));
        d.meta.page = Some(2);
        let v = d.to_index_json();
        assert_eq!(v["id"], "doc_1");
        assert_eq!(v["title"], "T");
        assert_eq!(v["content"], "hello");
        assert_eq!(v["price"], 3);
        assert_eq!(v["_meta"]["page"], 2);
    }

    #[test]
    fn redacted_context_hides_key() {
        let ctx = MeiliContext {
            project_id: None,
            host: "h".into(),
            api_key: "SECRET".into(),
            index: None,
            region: None,
        };
        assert!(!ctx.redacted().contains("SECRET"));
    }
}
