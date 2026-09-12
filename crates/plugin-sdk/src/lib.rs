//! # meili-ingest plugin SDK
//!
//! This crate is the contract between the `meili-ingest` pipeline engine and the
//! plugins that do the actual work (extraction, chunking, enrichment, indexing).
//!
//! A plugin is any type implementing [`Plugin`]. Built-in plugins are compiled into
//! the worker binary; community plugins can be shipped as WASM modules (see the
//! `wasm` feature) or as gRPC containers.
//!
//! ```no_run
//! use meili_ingest_plugin_sdk::prelude::*;
//!
//! pub struct Upper;
//!
//! #[async_trait]
//! impl Plugin for Upper {
//!     fn manifest(&self) -> PluginManifest {
//!         PluginManifest::new("upper", env!("CARGO_PKG_VERSION"))
//!             .description("Uppercases document content")
//!             .accepts([InputKind::Documents])
//!             .produces(OutputKind::Documents)
//!     }
//!
//!     async fn execute(
//!         &self,
//!         ctx: &ActivityContext,
//!         input: PluginInput,
//!         _config: serde_json::Value,
//!     ) -> Result<PluginOutput, PluginError> {
//!         let docs = input.into_documents()?;
//!         let out = docs
//!             .into_iter()
//!             .enumerate()
//!             .map(|(i, mut d)| {
//!                 if i % 10 == 0 { ctx.heartbeat(format!("doc {i}")); }
//!                 d.content = d.content.to_uppercase();
//!                 d
//!             })
//!             .collect();
//!         Ok(PluginOutput::Documents(out))
//!     }
//! }
//! ```

pub mod context;
pub mod error;
pub mod types;

pub use async_trait::async_trait;
pub use context::ActivityContext;
pub use error::PluginError;
pub use types::*;

/// The plugin contract. Implement this for every processing step.
#[async_trait]
pub trait Plugin: Send + Sync + 'static {
    /// Static description of the plugin: name, what it accepts/produces and its config schema.
    fn manifest(&self) -> PluginManifest;

    /// Run the plugin on one input with the `config:` block from the pipeline step.
    ///
    /// Implementations should call [`ActivityContext::heartbeat`] periodically in long
    /// loops (every ~10 items) and check [`ActivityContext::is_cancelled`], returning
    /// [`PluginError::Cancelled`] when set.
    async fn execute(
        &self,
        ctx: &ActivityContext,
        input: PluginInput,
        config: serde_json::Value,
    ) -> Result<PluginOutput, PluginError>;
}

/// Convenience re-exports for plugin authors.
pub mod prelude {
    pub use crate::context::ActivityContext;
    pub use crate::error::PluginError;
    pub use crate::types::*;
    pub use crate::Plugin;
    pub use async_trait::async_trait;
    pub use serde_json;
}
