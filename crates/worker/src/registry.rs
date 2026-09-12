//! [`PluginRegistry`]: name → plugin instance, built from the compiled-in plugins and
//! optional external (WASM / gRPC) plugins.

use std::collections::BTreeMap;
use std::sync::Arc;

use meili_ingest_plugin_sdk::{Plugin, PluginError, PluginManifest};

/// Registry of plugins available on this worker.
#[derive(Default)]
pub struct PluginRegistry {
    plugins: BTreeMap<String, Arc<dyn Plugin>>,
    /// Plugins that exist but could not be constructed (e.g. `LLM_API_KEY` missing),
    /// with the reason. Executing them yields a clear non-retryable error.
    unavailable: BTreeMap<String, String>,
}

impl std::fmt::Debug for PluginRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginRegistry")
            .field("plugins", &self.plugins.keys().collect::<Vec<_>>())
            .field("unavailable", &self.unavailable)
            .finish()
    }
}

impl PluginRegistry {
    /// Empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registry with every plugin compiled into this binary. Env-backed plugins that
    /// fail to construct are recorded as unavailable instead of aborting startup, so a
    /// `workers-general` pool without `LLM_API_KEY` still boots.
    pub fn builtin() -> Self {
        let mut reg = Self::new();
        reg.register(Arc::new(meili_ingest_plugin_pdf::PdfExtractorPlugin::new()));
        reg.register(Arc::new(
            meili_ingest_plugin_docx::DocxExtractorPlugin::new(),
        ));
        reg.register(Arc::new(
            meili_ingest_plugin_xlsx::XlsxExtractorPlugin::new(),
        ));
        reg.register(Arc::new(
            meili_ingest_plugin_html::HtmlExtractorPlugin::new(),
        ));
        reg.register(Arc::new(
            meili_ingest_plugin_markdown::MarkdownExtractorPlugin::new(),
        ));
        reg.register(Arc::new(meili_ingest_plugin_csv::CsvParserPlugin::new()));
        reg.register(Arc::new(
            meili_ingest_plugin_json::JsonFlattenerPlugin::new(),
        ));
        reg.register(Arc::new(meili_ingest_plugin_chunker::ChunkerPlugin::new()));
        reg.register(Arc::new(
            meili_ingest_plugin_meili_indexer::MeiliIndexerPlugin::new(),
        ));
        match meili_ingest_plugin_llm_enricher::LlmEnricherPlugin::from_env() {
            Ok(p) => reg.register(Arc::new(p)),
            Err(e) => reg.mark_unavailable(meili_ingest_plugin_llm_enricher::NAME, e.to_string()),
        }
        match meili_ingest_plugin_image_captioner::ImageCaptionerPlugin::from_env() {
            Ok(p) => reg.register(Arc::new(p)),
            Err(e) => {
                reg.mark_unavailable(meili_ingest_plugin_image_captioner::NAME, e.to_string())
            }
        }
        reg
    }

    /// Load external plugins from an `EXTERNAL_PLUGINS` spec string and add them.
    /// Failures are logged and recorded as unavailable.
    pub async fn load_external(&mut self, spec: &str) {
        for s in meili_ingest_plugin_runtime::parse_env(spec) {
            let location = s.location.clone();
            match meili_ingest_plugin_runtime::load(&s).await {
                Ok(p) => {
                    let name = p.manifest().name;
                    tracing::info!(plugin = %name, location = %location, "loaded external plugin");
                    self.register(p);
                }
                Err(e) => {
                    tracing::error!(location = %location, error = %e, "failed to load external plugin");
                    self.mark_unavailable(location, e.to_string());
                }
            }
        }
    }

    /// Register (or replace) a plugin under its manifest name.
    pub fn register(&mut self, plugin: Arc<dyn Plugin>) {
        let name = plugin.manifest().name;
        self.unavailable.remove(&name);
        self.plugins.insert(name, plugin);
    }

    /// Record a plugin that is known but cannot run here.
    pub fn mark_unavailable(&mut self, name: impl Into<String>, reason: impl Into<String>) {
        self.unavailable.insert(name.into(), reason.into());
    }

    /// Look up a plugin.
    pub fn get(&self, name: &str) -> Option<Arc<dyn Plugin>> {
        self.plugins.get(name).cloned()
    }

    /// Look up a plugin, producing the error the activity should return when missing.
    pub fn resolve(&self, name: &str) -> Result<Arc<dyn Plugin>, PluginError> {
        if let Some(p) = self.get(name) {
            return Ok(p);
        }
        if let Some(reason) = self.unavailable.get(name) {
            return Err(PluginError::NonRetryable(format!(
                "plugin {name:?} is not available on this worker: {reason}"
            )));
        }
        Err(PluginError::NonRetryable(format!(
            "plugin {name:?} is not registered on this worker (task queue routing sends it here); \
             deploy a worker that provides it (built-in, WASM or gRPC via EXTERNAL_PLUGINS)"
        )))
    }

    /// Manifests of all available plugins.
    pub fn manifests(&self) -> Vec<PluginManifest> {
        self.plugins.values().map(|p| p.manifest()).collect()
    }

    /// Names of available plugins.
    pub fn names(&self) -> Vec<String> {
        self.plugins.keys().cloned().collect()
    }

    /// Names and reasons of unavailable plugins.
    pub fn unavailable(&self) -> &BTreeMap<String, String> {
        &self.unavailable
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meili_ingest_plugin_sdk::prelude::*;

    struct Noop;
    #[async_trait]
    impl Plugin for Noop {
        fn manifest(&self) -> PluginManifest {
            PluginManifest::new("noop", "0.0.1")
        }
        async fn execute(
            &self,
            _ctx: &ActivityContext,
            _input: PluginInput,
            _config: serde_json::Value,
        ) -> Result<PluginOutput, PluginError> {
            Ok(PluginOutput::Empty)
        }
    }

    #[test]
    fn register_and_resolve() {
        let mut r = PluginRegistry::new();
        r.register(Arc::new(Noop));
        assert!(r.resolve("noop").is_ok());
        assert_eq!(r.names(), vec!["noop"]);
        assert!(
            matches!(r.resolve("missing"), Err(PluginError::NonRetryable(m)) if m.contains("not registered"))
        );
    }

    #[test]
    fn unavailable_has_reason() {
        let mut r = PluginRegistry::new();
        r.mark_unavailable("llm_enricher", "LLM_API_KEY not set");
        assert!(
            matches!(r.resolve("llm_enricher"), Err(PluginError::NonRetryable(m)) if m.contains("LLM_API_KEY"))
        );
        r.register(Arc::new(Noop));
        assert_eq!(r.unavailable().len(), 1);
    }

    #[test]
    fn builtin_registry_has_core_plugins() {
        let r = PluginRegistry::builtin();
        for name in [
            "pdf_extractor",
            "docx_extractor",
            "xlsx_extractor",
            "html_extractor",
            "markdown_extractor",
            "csv_parser",
            "json_flattener",
            "chunker",
            "meili_indexer",
        ] {
            assert!(r.get(name).is_some(), "missing {name}");
        }
        // llm plugins are either available or explicitly unavailable
        for name in ["llm_enricher", "image_captioner"] {
            assert!(r.get(name).is_some() || r.unavailable().contains_key(name));
        }
    }
}

#[cfg(test)]
mod builtin_pipeline_compat {
    //! Every built-in pipeline must be runnable by a worker that has the built-in
    //! plugins: auto-routing always hands the first step raw bytes (an upload, or a
    //! URL/S3 reference the activity resolves into bytes), and each step's output kind
    //! must be acceptable to the next one. These are contract checks between
    //! `meili-ingest-router`'s pipeline table and the plugins' manifests.

    use super::PluginRegistry;
    use meili_ingest_plugin_sdk::{InputKind, OutputKind};
    use meili_ingest_router::builtin_pipelines;
    use std::sync::Arc;

    /// Registry holding every in-repo plugin, including the LLM-backed ones that
    /// `PluginRegistry::builtin()` marks unavailable when no API key is configured.
    /// Their manifests are still the contract these tests check.
    fn registry_with_all_manifests() -> PluginRegistry {
        let mut reg = PluginRegistry::builtin();
        reg.register(Arc::new(
            meili_ingest_plugin_llm_enricher::LlmEnricherPlugin::new(),
        ));
        reg.register(Arc::new(
            meili_ingest_plugin_image_captioner::ImageCaptionerPlugin::new(),
        ));
        reg
    }

    /// Plugins provided by external gRPC containers, not by this binary.
    const EXTERNAL: &[&str] = &[
        "pptx_extractor",
        "whisper_transcriber",
        "video_audio_extractor",
        "ocr",
        "s3_downloader",
    ];

    fn output_satisfies(produced: OutputKind, accepts: &[InputKind]) -> bool {
        if accepts.is_empty() {
            return true;
        }
        let equivalent = match produced {
            OutputKind::Bytes => InputKind::Bytes,
            OutputKind::Ref => InputKind::Bytes, // resolved by the activity before dispatch
            OutputKind::Documents => InputKind::Documents,
            OutputKind::Many => InputKind::Many,
            OutputKind::Indexed | OutputKind::Empty => InputKind::Empty,
        };
        if accepts.contains(&equivalent) {
            return true;
        }
        // A `Many` of documents is flattened for plugins that take documents.
        equivalent == InputKind::Many && accepts.contains(&InputKind::Documents)
    }

    #[test]
    fn first_step_of_every_builtin_pipeline_accepts_raw_bytes() {
        let reg = registry_with_all_manifests();
        for pipeline in builtin_pipelines() {
            let first = &pipeline.steps[0];
            if EXTERNAL.contains(&first.plugin.as_str()) {
                continue;
            }
            let plugin = reg.get(&first.plugin).unwrap_or_else(|| {
                panic!("{}: plugin {} not registered", pipeline.uid, first.plugin)
            });
            let accepts = plugin.manifest().accepts;
            assert!(
                accepts.is_empty() || accepts.contains(&InputKind::Bytes),
                "{}: first step {:?} ({}) does not accept Bytes but auto-routing always \
                 delivers an uploaded/fetched file; accepts={:?}",
                pipeline.uid,
                first.id,
                first.plugin,
                accepts
            );
        }
    }

    #[test]
    fn every_builtin_pipeline_step_accepts_its_predecessor_output() {
        let reg = registry_with_all_manifests();
        for pipeline in builtin_pipelines() {
            for window in pipeline.steps.windows(2) {
                let (prev, next) = (&window[0], &window[1]);
                if EXTERNAL.contains(&prev.plugin.as_str())
                    || EXTERNAL.contains(&next.plugin.as_str())
                {
                    continue;
                }
                let (Some(p), Some(n)) = (reg.get(&prev.plugin), reg.get(&next.plugin)) else {
                    continue;
                };
                let produced = p.manifest().produces;
                let accepts = n.manifest().accepts;
                assert!(
                    output_satisfies(produced, &accepts),
                    "{}: step {:?} ({}) produces {:?} which step {:?} ({}) does not accept ({:?})",
                    pipeline.uid,
                    prev.id,
                    prev.plugin,
                    produced,
                    next.id,
                    next.plugin,
                    accepts
                );
            }
        }
    }

    #[test]
    fn every_builtin_pipeline_ends_in_the_indexer() {
        for pipeline in builtin_pipelines() {
            let last = pipeline.steps.last().expect("pipeline has steps");
            assert_eq!(
                last.plugin,
                meili_ingest_plugin_sdk::INDEXER_PLUGIN,
                "{} does not end in the indexer",
                pipeline.uid
            );
        }
    }
}
