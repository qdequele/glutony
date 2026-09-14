//! Built-in pipelines (SPEC §9) and the static list of known plugin names.
//!
//! The pipeline table itself lives in `meili-ingest-router` so the gateway, the
//! control plane and tests share one definition; this module re-exports it.

pub use meili_ingest_router::builtin_pipelines;

/// Plugins compiled into the worker binary (in-repo crates under `crates/plugins`).
pub const IN_REPO_PLUGINS: &[&str] = &[
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
    "meili_indexer",
    "llm_enricher",
    "image_captioner",
    "pptx_extractor",
    "whisper_transcriber",
    "video_audio_extractor",
];

/// Plugins referenced by task-queue routing that are expected to be provided by
/// external gRPC containers (not implemented in this repository).
pub const EXTERNAL_PLUGINS: &[&str] = &["s3_downloader", "ocr"];

const ALL_KNOWN: &[&str] = &[
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
    "meili_indexer",
    "llm_enricher",
    "image_captioner",
    "pptx_extractor",
    "whisper_transcriber",
    "video_audio_extractor",
    "s3_downloader",
    "ocr",
];

/// Every plugin name the control plane accepts in a user pipeline without a worker
/// having registered it first: the 17 in-repo plugins plus the known gRPC plugins.
pub fn builtin_plugin_names() -> &'static [&'static str] {
    ALL_KNOWN
}

/// Whether `name` is one of [`builtin_plugin_names`].
pub fn is_known_plugin(name: &str) -> bool {
    ALL_KNOWN.contains(&name)
}

/// Whether `name` is compiled into the worker binary (as opposed to an external
/// gRPC plugin).
pub fn is_in_repo_plugin(name: &str) -> bool {
    IN_REPO_PLUGINS.contains(&name)
}

/// Prefix reserved for built-in pipeline uids.
pub const BUILTIN_PREFIX: &str = "builtin.";

/// Whether a pipeline uid belongs to the reserved built-in namespace.
pub fn is_builtin_uid(uid: &str) -> bool {
    uid.starts_with(BUILTIN_PREFIX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_names_are_the_union_of_in_repo_and_external() {
        let mut expected: Vec<&str> = IN_REPO_PLUGINS.to_vec();
        expected.extend_from_slice(EXTERNAL_PLUGINS);
        assert_eq!(builtin_plugin_names(), expected.as_slice());
        assert_eq!(IN_REPO_PLUGINS.len(), 17);
        assert_eq!(builtin_plugin_names().len(), 19);
    }

    #[test]
    fn every_builtin_pipeline_only_uses_known_plugins() {
        let pipelines = builtin_pipelines();
        assert_eq!(pipelines.len(), 15, "SPEC §9 lists 15 built-in pipelines");
        for p in &pipelines {
            assert!(is_builtin_uid(&p.uid), "{} must start with builtin.", p.uid);
            assert!(p.builtin, "{} must be flagged builtin", p.uid);
            p.validate()
                .unwrap_or_else(|e| panic!("{} invalid: {e}", p.uid));
            for s in &p.steps {
                assert!(
                    is_known_plugin(&s.plugin),
                    "{}: unknown plugin {}",
                    p.uid,
                    s.plugin
                );
            }
        }
    }

    #[test]
    fn builtin_uid_detection() {
        assert!(is_builtin_uid("builtin.pdf"));
        assert!(!is_builtin_uid("builtin"));
        assert!(!is_builtin_uid("my-builtin.pdf"));
        assert!(!is_builtin_uid("pdf"));
    }

    #[test]
    fn catalog_describes_every_known_plugin() {
        // The catalog keeps its own list because router cannot depend on this
        // crate. This test is the joint that stops the two drifting.
        let mut catalog_names = meili_ingest_router::catalog::ALL_KNOWN_PLUGINS.to_vec();
        let mut known = builtin_plugin_names().to_vec();
        catalog_names.sort_unstable();
        known.sort_unstable();
        assert_eq!(catalog_names, known);
    }
}
