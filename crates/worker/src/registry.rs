//! [`PluginRegistry`]: name → plugin instance, built from the compiled-in plugins and
//! optional external (WASM / gRPC) plugins.

use std::collections::BTreeMap;
use std::sync::Arc;

use meili_ingest_plugin_sdk::{Plugin, PluginError, PluginManifest};

use crate::ai_target::LAB_AI_PLUGINS;

/// What the env-backed AI plugins' `from_env` returned.
struct AiPlugins {
    llm_enricher: Result<meili_ingest_plugin_llm_enricher::LlmEnricherPlugin, PluginError>,
    jev_enricher: Result<meili_ingest_plugin_jev_enricher::JevEnricherPlugin, PluginError>,
    image_captioner: Result<meili_ingest_plugin_image_captioner::ImageCaptionerPlugin, PluginError>,
    whisper_transcriber:
        Result<meili_ingest_plugin_audio_transcriber::WhisperTranscriberPlugin, PluginError>,
}

impl AiPlugins {
    fn from_env() -> Self {
        Self {
            llm_enricher: meili_ingest_plugin_llm_enricher::LlmEnricherPlugin::from_env(),
            jev_enricher: meili_ingest_plugin_jev_enricher::JevEnricherPlugin::from_env(),
            image_captioner: meili_ingest_plugin_image_captioner::ImageCaptionerPlugin::from_env(),
            whisper_transcriber:
                meili_ingest_plugin_audio_transcriber::WhisperTranscriberPlugin::from_env(),
        }
    }
}

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
        Self::builtin_with(false)
    }

    /// [`Self::builtin`], for a worker whose Lab tenants' AI steps may run on the
    /// team's Gateway (`LAB_AI_GATEWAY`). With `lab_ai_gateway`, an AI plugin of
    /// [`LAB_AI_PLUGINS`] without its env key is registered disabled rather than
    /// unavailable: a step with a Lab target runs, any other fails with
    /// `InvalidConfig` naming the missing key. `whisper_transcriber` takes no target
    /// and is unchanged.
    pub fn builtin_with(lab_ai_gateway: bool) -> Self {
        Self::builtin_from(lab_ai_gateway, AiPlugins::from_env())
    }

    /// [`Self::builtin_with`] from already-built AI plugins (tests pass them without
    /// touching the process environment).
    fn builtin_from(lab_ai_gateway: bool, ai: AiPlugins) -> Self {
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
        reg.register(Arc::new(
            meili_ingest_plugin_msgpack::MsgpackParserPlugin::new(),
        ));
        reg.register(Arc::new(meili_ingest_plugin_avro::AvroParserPlugin::new()));
        reg.register(Arc::new(
            meili_ingest_plugin_parquet::ParquetParserPlugin::new(),
        ));
        reg.register(Arc::new(meili_ingest_plugin_chunker::ChunkerPlugin::new()));
        reg.register(Arc::new(
            meili_ingest_plugin_script::DocumentScriptPlugin::new(),
        ));
        reg.register(Arc::new(
            meili_ingest_plugin_meili_indexer::MeiliIndexerPlugin::new(),
        ));
        reg.register(Arc::new(
            meili_ingest_plugin_pptx::PptxExtractorPlugin::new(),
        ));
        reg.register(Arc::new(
            meili_ingest_plugin_video_audio::VideoAudioExtractorPlugin::new(),
        ));
        reg.register_ai(
            meili_ingest_plugin_llm_enricher::NAME,
            ai.llm_enricher,
            meili_ingest_plugin_llm_enricher::LlmEnricherPlugin::disabled,
            lab_ai_gateway,
        );
        reg.register_ai(
            meili_ingest_plugin_jev_enricher::NAME,
            ai.jev_enricher,
            meili_ingest_plugin_jev_enricher::JevEnricherPlugin::disabled,
            lab_ai_gateway,
        );
        reg.register_ai(
            meili_ingest_plugin_image_captioner::NAME,
            ai.image_captioner,
            meili_ingest_plugin_image_captioner::ImageCaptionerPlugin::disabled,
            lab_ai_gateway,
        );
        match ai.whisper_transcriber {
            Ok(p) => reg.register(Arc::new(p)),
            Err(e) => {
                reg.mark_unavailable(meili_ingest_plugin_audio_transcriber::NAME, e.to_string())
            }
        }
        reg
    }

    /// Register an env-backed AI plugin: built, disabled (it can still run on a Lab
    /// target) or unavailable.
    fn register_ai<P: Plugin + 'static>(
        &mut self,
        name: &str,
        built: Result<P, PluginError>,
        disabled: fn() -> P,
        lab_ai_gateway: bool,
    ) {
        match built {
            Ok(p) => self.register(Arc::new(p)),
            Err(_) if lab_ai_gateway && LAB_AI_PLUGINS.contains(&name) => {
                self.register(Arc::new(disabled()))
            }
            Err(e) => self.mark_unavailable(name, e.to_string()),
        }
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
            "msgpack_parser",
            "avro_parser",
            "parquet_parser",
            "chunker",
            "document_script",
            "meili_indexer",
            "pptx_extractor",
            "video_audio_extractor",
        ] {
            assert!(r.get(name).is_some(), "missing {name}");
        }
        // llm plugins are either available or explicitly unavailable
        for name in [
            "llm_enricher",
            "jev_enricher",
            "image_captioner",
            "whisper_transcriber",
        ] {
            assert!(r.get(name).is_some() || r.unavailable().contains_key(name));
        }
    }
    /// What the env-backed AI plugins' `from_env` returns on a worker with none of
    /// their keys, without touching this process's environment.
    fn no_ai_keys() -> AiPlugins {
        fn missing<P>(key: &str) -> Result<P, PluginError> {
            Err(PluginError::invalid_config(format!("{key} not set")))
        }
        AiPlugins {
            llm_enricher: missing("LLM_API_KEY"),
            jev_enricher: missing("TYPESAFE_API_KEY"),
            image_captioner: missing("LLM_API_KEY"),
            whisper_transcriber: missing("TRANSCRIBE_API_KEY"),
        }
    }

    #[test]
    fn builtin_with_lab_gateway_registers_ai_plugins_without_keys() {
        let r = PluginRegistry::builtin_from(true, no_ai_keys());
        for name in ["llm_enricher", "image_captioner", "jev_enricher"] {
            assert!(r.get(name).is_some(), "{name} runs on the Lab's Gateway");
            assert!(!r.unavailable().contains_key(name), "{name}");
        }
        assert!(r.get("whisper_transcriber").is_none());
        assert!(r.unavailable().contains_key("whisper_transcriber"));
        for name in crate::ai_target::LAB_AI_PLUGINS {
            assert!(r.get(name).is_some(), "{name}");
        }
    }

    #[test]
    fn builtin_without_lab_gateway_marks_keyless_ai_plugins_unavailable() {
        let r = PluginRegistry::builtin_from(false, no_ai_keys());
        for name in [
            "llm_enricher",
            "image_captioner",
            "jev_enricher",
            "whisper_transcriber",
        ] {
            assert!(r.get(name).is_none(), "{name}");
            assert!(r.unavailable().contains_key(name), "{name}");
        }
        assert!(
            matches!(r.resolve("llm_enricher"), Err(PluginError::NonRetryable(m)) if m.contains("LLM_API_KEY"))
        );
    }

    #[test]
    fn lab_ai_plugins_are_the_plugins_that_take_a_target() {
        assert_eq!(
            crate::ai_target::LAB_AI_PLUGINS,
            [
                meili_ingest_plugin_llm_enricher::NAME,
                meili_ingest_plugin_image_captioner::NAME,
                meili_ingest_plugin_jev_enricher::NAME,
            ]
        );
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
            meili_ingest_plugin_jev_enricher::JevEnricherPlugin::new(),
        ));
        reg.register(Arc::new(
            meili_ingest_plugin_image_captioner::ImageCaptionerPlugin::new(),
        ));
        reg.register(Arc::new(
            meili_ingest_plugin_audio_transcriber::WhisperTranscriberPlugin::new(),
        ));
        reg
    }

    /// Plugins provided by external gRPC containers, not by this binary.
    const EXTERNAL: &[&str] = &["ocr", "s3_downloader"];

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

    /// Every pipeline glutony ships: the built-ins, the catalog templates with an
    /// inline definition and the examples under `config/pipelines/`.
    fn shipped_pipelines() -> Vec<meili_ingest_plugin_sdk::PipelineDefinition> {
        let mut out = builtin_pipelines();
        out.extend(
            meili_ingest_router::catalog::catalog()
                .workflows
                .into_iter()
                .filter_map(|w| w.definition),
        );
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../../config/pipelines");
        for entry in std::fs::read_dir(dir).expect("config/pipelines") {
            let path = entry.expect("dir entry").path();
            if path.extension().is_some_and(|e| e == "yaml") {
                let text = std::fs::read_to_string(&path).expect("read example");
                out.push(
                    serde_yaml::from_str(&text)
                        .unwrap_or_else(|e| panic!("{}: {e}", path.display())),
                );
            }
        }
        out
    }

    /// The control plane checks every step config against its plugin's schema on save
    /// (`POST /pipelines`). A shipped pipeline that failed that check could not be
    /// duplicated and saved, which is how the Lab edits a built-in.
    #[test]
    fn every_shipped_pipeline_step_config_satisfies_its_plugin_schema() {
        let reg = registry_with_all_manifests();
        let mut failures = Vec::new();
        for pipeline in shipped_pipelines() {
            for step in &pipeline.steps {
                let Some(plugin) = reg.get(&step.plugin) else {
                    assert!(
                        EXTERNAL.contains(&step.plugin.as_str()),
                        "{}: plugin {} not registered",
                        pipeline.uid,
                        step.plugin
                    );
                    continue;
                };
                let schema = plugin.manifest().config_schema;
                let validator = jsonschema::validator_for(&schema)
                    .unwrap_or_else(|e| panic!("{}: schema does not compile: {e}", step.plugin));
                for e in validator.iter_errors(&step.config) {
                    failures.push(format!(
                        "{}: step {:?} ({}): {e}",
                        pipeline.uid, step.id, step.plugin
                    ));
                }
            }
        }
        assert!(failures.is_empty(), "{failures:#?}");
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

#[cfg(test)]
mod priced_plugins {
    #[test]
    fn priced_plugins_are_the_plugins_that_call_providers() {
        // Every plugin that calls `ProviderCosts::apply`, by its own NAME: a rename
        // must not silently drop it from the boot check for missing prices.
        let mut callers = vec![
            meili_ingest_plugin_llm_enricher::NAME,
            meili_ingest_plugin_jev_enricher::NAME,
            meili_ingest_plugin_image_captioner::NAME,
            meili_ingest_plugin_audio_transcriber::NAME,
        ];
        callers.sort_unstable();
        let mut priced = meili_ingest_plugin_sdk::cost::PRICED_PLUGINS.to_vec();
        priced.sort_unstable();
        assert_eq!(priced, callers);
    }
}
