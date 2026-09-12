//! # `chunker` — split document content into overlapping chunks
//!
//! Built-in `meili-ingest` plugin that turns each incoming [`Document`] into
//! one or more chunk documents. Sizes are measured in Unicode scalar values
//! (`char`s), never bytes, so multi-byte text is never split mid-character.
//!
//! Strategies:
//! * `sentence` (default) — split on `.`, `!`, `?` followed by whitespace and
//!   on newlines, then greedily pack sentences into chunks of at most
//!   `chunk_size` chars. Each new chunk starts with the trailing `overlap`
//!   chars of the previous one. A single sentence longer than `chunk_size` is
//!   hard-split.
//! * `fixed` — sliding window of `chunk_size` chars stepping back `overlap`
//!   chars, cutting on the nearest whitespace before the boundary when possible.
//! * `paragraph` — split on blank lines, then pack like sentences.
//!
//! A trailing fragment shorter than `min_chunk_size` is merged into the
//! previous chunk (which may then exceed `chunk_size` by less than
//! `min_chunk_size` plus the separator). A document that fits in `chunk_size` yields exactly one
//! chunk; empty content yields none.
//!
//! Every chunk gets `id = <parent_id>_<index>`, `meta.chunk_index`,
//! `meta.chunk_total` and `meta.parent_id`. With `keep_parent_fields` (default
//! `true`) the parent's `title`, `fields` and `meta` are copied first.

use meili_ingest_plugin_sdk::prelude::*;
use serde::Deserialize;
use serde_json::Value;

/// Plugin name referenced by `steps[].plugin`.
pub const NAME: &str = "chunker";

/// Documents between two heartbeats.
const HEARTBEAT_EVERY: usize = 10;

/// The chunker plugin. Stateless; construct with [`ChunkerPlugin::new`].
#[derive(Debug, Clone, Default)]
pub struct ChunkerPlugin;

impl ChunkerPlugin {
    /// Create the plugin.
    pub fn new() -> Self {
        Self
    }
}

/// Chunking strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
enum Strategy {
    #[default]
    Sentence,
    Fixed,
    Paragraph,
}

/// Step configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Config {
    strategy: Strategy,
    chunk_size: usize,
    overlap: usize,
    min_chunk_size: usize,
    keep_parent_fields: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            strategy: Strategy::Sentence,
            chunk_size: 512,
            overlap: 64,
            min_chunk_size: 32,
            keep_parent_fields: true,
        }
    }
}

impl Config {
    fn parse(value: Value) -> Result<Self, PluginError> {
        if value.is_null() {
            return Ok(Self::default());
        }
        let cfg: Config = serde_json::from_value(value)
            .map_err(|e| PluginError::InvalidConfig(format!("{NAME}: {e}")))?;
        if cfg.chunk_size == 0 {
            return Err(PluginError::InvalidConfig(format!(
                "{NAME}: `chunk_size` must be at least 1"
            )));
        }
        if cfg.overlap >= cfg.chunk_size {
            return Err(PluginError::InvalidConfig(format!(
                "{NAME}: `overlap` ({}) must be smaller than `chunk_size` ({})",
                cfg.overlap, cfg.chunk_size
            )));
        }
        Ok(cfg)
    }
}

// ---------------------------------------------------------------------------
// Splitting primitives (all indices are char indices)
// ---------------------------------------------------------------------------

/// Sliding-window ranges over `chars`. Every index in `0..chars.len()` is
/// covered by at least one range; consecutive ranges overlap by up to
/// `overlap` chars; each range has at most `chunk_size` chars.
fn fixed_ranges(chars: &[char], chunk_size: usize, overlap: usize) -> Vec<(usize, usize)> {
    let n = chars.len();
    let mut ranges = Vec::new();
    let mut start = 0;
    while start < n {
        if n - start <= chunk_size {
            ranges.push((start, n));
            break;
        }
        let end = start + chunk_size;
        // Cut on whitespace, but never so early that the next window would not advance.
        let lower = (start + (chunk_size / 2).max(overlap + 1)).min(end);
        let cut = (lower..=end)
            .rev()
            .find(|&i| i > start && chars[i - 1].is_whitespace())
            .unwrap_or(end);
        ranges.push((start, cut));
        start = cut - overlap;
    }
    ranges
}

/// Split text into sentences: terminators `.`, `!`, `?` followed by whitespace
/// (or end of text) end a sentence, as does any newline. Sentences are trimmed
/// and empty ones dropped.
fn split_sentences(text: &str) -> Vec<String> {
    let chars: Vec<char> = text.chars().collect();
    let mut out = Vec::new();
    let mut current = String::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '\n' {
            push_unit(&mut out, &mut current);
        } else {
            current.push(c);
            if matches!(c, '.' | '!' | '?') {
                let next_is_terminator = chars
                    .get(i + 1)
                    .is_some_and(|n| matches!(n, '.' | '!' | '?'));
                let ends = chars.get(i + 1).is_none_or(|n| n.is_whitespace());
                if !next_is_terminator && ends {
                    push_unit(&mut out, &mut current);
                }
            }
        }
        i += 1;
    }
    push_unit(&mut out, &mut current);
    out
}

/// Split text into paragraphs on blank (whitespace-only) lines.
fn split_paragraphs(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            push_unit(&mut out, &mut current);
        } else {
            if !current.is_empty() {
                current.push('\n');
            }
            current.push_str(line);
        }
    }
    push_unit(&mut out, &mut current);
    out
}

fn push_unit(out: &mut Vec<String>, current: &mut String) {
    let trimmed = current.trim();
    if !trimmed.is_empty() {
        out.push(trimmed.to_owned());
    }
    current.clear();
}

/// Hard-split any unit longer than `chunk_size` (whitespace-aware, no overlap).
fn cap_units(units: Vec<String>, chunk_size: usize) -> Vec<String> {
    let mut out = Vec::with_capacity(units.len());
    for unit in units {
        let chars: Vec<char> = unit.chars().collect();
        if chars.len() <= chunk_size {
            out.push(unit);
            continue;
        }
        for (s, e) in fixed_ranges(&chars, chunk_size, 0) {
            let piece: String = chars[s..e].iter().collect();
            let piece = piece.trim();
            if !piece.is_empty() {
                out.push(piece.to_owned());
            }
        }
    }
    out
}

/// A packed chunk: its text plus how many leading chars came from the overlap
/// with the previous chunk (so merging never duplicates text).
#[derive(Debug, Clone)]
struct Packed {
    text: String,
    overlap_len: usize,
}

/// Trailing `overlap` chars of `text`, advanced to the next word boundary so the
/// prefix never starts mid-word.
fn tail_overlap(text: &str, overlap: usize) -> String {
    if overlap == 0 {
        return String::new();
    }
    let chars: Vec<char> = text.chars().collect();
    if chars.len() <= overlap {
        return text.to_owned();
    }
    let start = chars.len() - overlap;
    let tail = &chars[start..];
    // If we landed inside a word, skip to the next whitespace.
    let adjusted = if chars[start - 1].is_whitespace() {
        tail
    } else {
        match tail.iter().position(|c| c.is_whitespace()) {
            Some(ws) => &tail[ws..],
            None => &[],
        }
    };
    adjusted.iter().collect::<String>().trim_start().to_owned()
}

/// Greedily pack units (each ≤ `chunk_size` chars) into chunks of at most
/// `chunk_size` chars, joined by `sep`, seeding each new chunk with the
/// trailing overlap of the previous one.
fn pack_units(units: &[String], sep: &str, chunk_size: usize, overlap: usize) -> Vec<Packed> {
    let sep_len = sep.chars().count();
    let mut chunks: Vec<Packed> = Vec::new();
    let mut cur = String::new();
    let mut cur_len = 0usize;
    let mut cur_overlap = 0usize;
    let mut cur_units = 0usize;

    for unit in units {
        let unit_len = unit.chars().count();
        let joined_len = if cur.is_empty() {
            unit_len
        } else {
            cur_len + sep_len + unit_len
        };
        if cur_units > 0 && joined_len > chunk_size {
            chunks.push(Packed {
                text: std::mem::take(&mut cur),
                overlap_len: cur_overlap,
            });
            // Seed the next chunk with the previous tail, shrunk so the unit still fits.
            let last = &chunks[chunks.len() - 1].text;
            let mut prefix = tail_overlap(last, overlap);
            let mut prefix_len = prefix.chars().count();
            let budget = chunk_size.saturating_sub(unit_len + sep_len);
            if prefix_len > budget {
                prefix = tail_overlap(&prefix, budget);
                prefix_len = prefix.chars().count();
            }
            cur_overlap = prefix_len;
            cur_len = prefix_len;
            cur = prefix;
            cur_units = 0;
        }
        if !cur.is_empty() {
            cur.push_str(sep);
            cur_len += sep_len;
        }
        cur.push_str(unit);
        cur_len += unit_len;
        cur_units += 1;
    }
    if cur_units > 0 {
        chunks.push(Packed {
            text: cur,
            overlap_len: cur_overlap,
        });
    }
    chunks
}

/// Merge a trailing chunk shorter than `min_chunk_size` into the previous one.
fn merge_short_tail(mut chunks: Vec<Packed>, sep: &str, min_chunk_size: usize) -> Vec<String> {
    if chunks.len() >= 2 {
        let last_len = chunks[chunks.len() - 1].text.chars().count();
        if last_len < min_chunk_size
            && let Some(last) = chunks.pop()
            && let Some(prev) = chunks.last_mut()
        {
            let fresh: String = last.text.chars().skip(last.overlap_len).collect();
            let fresh = fresh.trim_start();
            if !fresh.is_empty() {
                prev.text.push_str(sep);
                prev.text.push_str(fresh);
            }
        }
    }
    chunks.into_iter().map(|p| p.text).collect()
}

/// Chunk `content` according to `cfg`. Pure function used by the plugin and tests.
fn chunk_text(content: &str, cfg: &Config) -> Vec<String> {
    let trimmed = content.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    if trimmed.chars().count() <= cfg.chunk_size {
        return vec![trimmed.to_owned()];
    }
    match cfg.strategy {
        Strategy::Fixed => {
            let chars: Vec<char> = trimmed.chars().collect();
            let mut ranges = fixed_ranges(&chars, cfg.chunk_size, cfg.overlap);
            if ranges.len() >= 2 {
                let (ls, le) = ranges[ranges.len() - 1];
                if le - ls < cfg.min_chunk_size {
                    ranges.pop();
                    if let Some(prev) = ranges.last_mut() {
                        prev.1 = le;
                    }
                }
            }
            ranges
                .into_iter()
                .map(|(s, e)| chars[s..e].iter().collect::<String>().trim().to_owned())
                .filter(|s| !s.is_empty())
                .collect()
        }
        Strategy::Sentence => {
            let units = cap_units(split_sentences(trimmed), cfg.chunk_size);
            merge_short_tail(
                pack_units(&units, " ", cfg.chunk_size, cfg.overlap),
                " ",
                cfg.min_chunk_size,
            )
        }
        Strategy::Paragraph => {
            let units = cap_units(split_paragraphs(trimmed), cfg.chunk_size);
            merge_short_tail(
                pack_units(&units, "\n\n", cfg.chunk_size, cfg.overlap),
                "\n\n",
                cfg.min_chunk_size,
            )
        }
    }
}

fn chunk_document(parent: &Document, cfg: &Config) -> Vec<Document> {
    let chunks = chunk_text(&parent.content, cfg);
    let total = chunks.len();
    chunks
        .into_iter()
        .enumerate()
        .map(|(i, text)| {
            let (title, fields, mut meta) = if cfg.keep_parent_fields {
                (
                    parent.title.clone(),
                    parent.fields.clone(),
                    parent.meta.clone(),
                )
            } else {
                (None, Default::default(), DocumentMeta::default())
            };
            meta.chunk_index = Some(i);
            meta.chunk_total = Some(total);
            meta.parent_id = Some(parent.id.clone());
            Document {
                id: format!("{}_{}", parent.id, i),
                title,
                content: text,
                fields,
                meta,
            }
        })
        .collect()
}

#[async_trait]
impl Plugin for ChunkerPlugin {
    fn manifest(&self) -> PluginManifest {
        PluginManifest::new(NAME, env!("CARGO_PKG_VERSION"))
            .description(
                "Splits document content into overlapping chunks (sentence, fixed or paragraph \
                 strategy) sized in characters; sets chunk_index/chunk_total/parent_id metadata.",
            )
            .accepts([InputKind::Documents, InputKind::Many])
            .produces(OutputKind::Documents)
            .config_schema(serde_json::json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "strategy": {
                        "type": "string",
                        "enum": ["sentence", "fixed", "paragraph"],
                        "default": "sentence",
                        "description": "How to find chunk boundaries."
                    },
                    "chunk_size": {
                        "type": "integer",
                        "minimum": 1,
                        "default": 512,
                        "description": "Maximum chunk length in characters."
                    },
                    "overlap": {
                        "type": "integer",
                        "minimum": 0,
                        "default": 64,
                        "description": "Characters repeated from the end of the previous chunk. Must be smaller than chunk_size."
                    },
                    "min_chunk_size": {
                        "type": "integer",
                        "minimum": 0,
                        "default": 32,
                        "description": "A trailing fragment shorter than this is merged into the previous chunk."
                    },
                    "keep_parent_fields": {
                        "type": "boolean",
                        "default": true,
                        "description": "Copy the parent's title, fields and meta onto every chunk."
                    }
                }
            }))
    }

    async fn execute(
        &self,
        ctx: &ActivityContext,
        input: PluginInput,
        config: Value,
    ) -> Result<PluginOutput, PluginError> {
        let cfg = Config::parse(config)?;
        let docs = input.into_documents()?;
        let mut out = Vec::with_capacity(docs.len());
        for (i, doc) in docs.iter().enumerate() {
            ctx.check_cancelled()?;
            if i > 0 && i.is_multiple_of(HEARTBEAT_EVERY) {
                ctx.heartbeat(format!(
                    "{NAME}: {i}/{} documents, {} chunks",
                    docs.len(),
                    out.len()
                ));
            }
            out.extend(chunk_document(doc, &cfg));
        }
        Ok(PluginOutput::Documents(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    fn cfg(v: Value) -> Config {
        Config::parse(v).unwrap()
    }

    fn doc(id: &str, content: &str) -> Document {
        Document::with_id(id, content)
    }

    async fn run(docs: Vec<Document>, config: Value) -> Vec<Document> {
        match ChunkerPlugin::new()
            .execute(
                &ActivityContext::noop(),
                PluginInput::Documents(docs),
                config,
            )
            .await
            .unwrap()
        {
            PluginOutput::Documents(d) => d,
            other => panic!("expected documents, got {other:?}"),
        }
    }

    /// Deterministic pseudo-random text mixing ASCII words, accents, emoji and punctuation.
    fn synth_text(seed: u64, words: usize) -> String {
        let vocab = [
            "lorem",
            "ipsum",
            "café",
            "naïve",
            "😀",
            "🚀🌍",
            "Zürich",
            "dolor",
            "sit",
            "amet",
            "über",
            "日本語",
            "consectetur",
            "élève",
        ];
        let mut x = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let mut out = String::new();
        for i in 0..words {
            x = x
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let w = vocab[(x >> 33) as usize % vocab.len()];
            out.push_str(w);
            match (x >> 20) % 11 {
                0 => out.push_str(". "),
                1 => out.push_str("! "),
                2 => out.push_str("?\n"),
                3 if i % 7 == 0 => out.push_str("\n\n"),
                _ => out.push(' '),
            }
        }
        out
    }

    fn non_ws_counts(s: &str) -> HashMap<char, usize> {
        let mut m = HashMap::new();
        for c in s.chars().filter(|c| !c.is_whitespace()) {
            *m.entry(c).or_insert(0) += 1;
        }
        m
    }

    fn assert_contiguous(chunks: &[Document], parent: &str) {
        for (i, c) in chunks.iter().enumerate() {
            assert_eq!(c.meta.chunk_index, Some(i));
            assert_eq!(c.meta.chunk_total, Some(chunks.len()));
            assert_eq!(c.meta.parent_id.as_deref(), Some(parent));
            assert_eq!(c.id, format!("{parent}_{i}"));
        }
    }

    #[test]
    fn manifest_is_correct() {
        let m = ChunkerPlugin.manifest();
        assert_eq!(m.name, NAME);
        assert_eq!(m.accepts, vec![InputKind::Documents, InputKind::Many]);
        assert_eq!(m.produces, OutputKind::Documents);
        let props = m.config_schema["properties"].as_object().unwrap();
        for key in [
            "strategy",
            "chunk_size",
            "overlap",
            "min_chunk_size",
            "keep_parent_fields",
        ] {
            assert!(props.contains_key(key), "schema missing {key}");
        }
    }

    #[test]
    fn invalid_config_is_rejected() {
        assert!(matches!(
            Config::parse(json!({"chunk_size": 10, "overlap": 10})),
            Err(PluginError::InvalidConfig(_))
        ));
        assert!(matches!(
            Config::parse(json!({"chunk_size": 0})),
            Err(PluginError::InvalidConfig(_))
        ));
        assert!(matches!(
            Config::parse(json!({"strategy": "words"})),
            Err(PluginError::InvalidConfig(_))
        ));
        assert!(matches!(
            Config::parse(json!({"nope": 1})),
            Err(PluginError::InvalidConfig(_))
        ));
        let d = Config::parse(Value::Null).unwrap();
        assert_eq!((d.chunk_size, d.overlap, d.min_chunk_size), (512, 64, 32));
        assert_eq!(d.strategy, Strategy::Sentence);
        assert!(d.keep_parent_fields);
    }

    #[tokio::test]
    async fn short_document_is_one_chunk_and_empty_is_none() {
        let mut parent = doc("p", "Short text.");
        parent.title = Some("T".into());
        parent.fields.insert("k".into(), json!(1));
        parent.meta.page = Some(4);
        let out = run(vec![parent, doc("e", "   \n ")], json!({})).await;
        assert_eq!(out.len(), 1);
        let c = &out[0];
        assert_eq!(c.id, "p_0");
        assert_eq!(c.content, "Short text.");
        assert_eq!(c.meta.chunk_index, Some(0));
        assert_eq!(c.meta.chunk_total, Some(1));
        assert_eq!(c.meta.parent_id.as_deref(), Some("p"));
        assert_eq!(c.meta.page, Some(4), "parent meta is kept");
        assert_eq!(c.title.as_deref(), Some("T"));
        assert_eq!(c.fields["k"], json!(1));
    }

    #[tokio::test]
    async fn keep_parent_fields_false_drops_parent_data() {
        let mut parent = doc("p", "Short text.");
        parent.title = Some("T".into());
        parent.fields.insert("k".into(), json!(1));
        parent.meta.page = Some(4);
        let out = run(vec![parent], json!({"keep_parent_fields": false})).await;
        assert!(out[0].title.is_none());
        assert!(out[0].fields.is_empty());
        assert_eq!(out[0].meta.page, None);
        assert_eq!(out[0].meta.parent_id.as_deref(), Some("p"));
    }

    #[test]
    fn fixed_strategy_covers_every_char_and_respects_size() {
        for seed in 0..40u64 {
            for (size, overlap) in [(50usize, 10usize), (17, 0), (100, 60), (7, 3), (1, 0)] {
                let text = synth_text(seed, 40 + (seed as usize % 50));
                let c = cfg(
                    json!({"strategy": "fixed", "chunk_size": size, "overlap": overlap, "min_chunk_size": 5}),
                );
                let chunks = chunk_text(&text, &c);
                assert!(!chunks.is_empty());
                let tolerance = c.min_chunk_size;
                for ch in &chunks {
                    let len = ch.chars().count();
                    assert!(
                        len <= size + tolerance,
                        "chunk of {len} chars > {size}+{tolerance}: {ch:?}"
                    );
                    assert!(!ch.is_empty());
                }
                let mut got: HashMap<char, usize> = HashMap::new();
                for ch in &chunks {
                    for (k, v) in non_ws_counts(ch) {
                        *got.entry(k).or_insert(0) += v;
                    }
                }
                for (k, v) in non_ws_counts(&text) {
                    assert!(
                        got.get(&k).copied().unwrap_or(0) >= v,
                        "char {k:?} lost (seed {seed}, size {size})"
                    );
                }
                // Chunks appear in order: each chunk is a substring of the original.
                for ch in &chunks {
                    assert!(
                        text.contains(ch.as_str()),
                        "chunk is not a substring: {ch:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn fixed_overlap_repeats_tail_of_previous_chunk() {
        let text: String = (0..30).map(|i| format!("w{i} ")).collect();
        let c =
            cfg(json!({"strategy": "fixed", "chunk_size": 40, "overlap": 10, "min_chunk_size": 4}));
        let chunks = chunk_text(&text, &c);
        assert!(chunks.len() >= 3);
        for pair in chunks.windows(2) {
            // Some suffix (up to `overlap` chars) of the previous chunk must start the next chunk.
            let overlap_found = (1..=10).any(|n| {
                let suffix: String = pair[0]
                    .chars()
                    .rev()
                    .take(n)
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .collect();
                !suffix.trim().is_empty() && pair[1].starts_with(suffix.trim())
            });
            assert!(
                overlap_found,
                "no overlap between {:?} and {:?}",
                pair[0], pair[1]
            );
        }
    }

    #[test]
    fn sentence_strategy_packs_whole_sentences_with_overlap() {
        let text =
            "First sentence here. Second one is a bit longer! Third? Fourth sentence ends it.";
        let c = cfg(json!({"chunk_size": 45, "overlap": 10, "min_chunk_size": 5}));
        let chunks = chunk_text(text, &c);
        assert!(chunks.len() >= 2, "{chunks:?}");
        for ch in &chunks {
            assert!(ch.chars().count() <= 45 + 5, "{ch:?}");
        }
        assert!(chunks[0].starts_with("First sentence here."));
        // Sentences are never cut in the middle unless longer than chunk_size.
        for ch in &chunks {
            assert!(
                ch.ends_with(['.', '!', '?']),
                "chunk should end on a sentence boundary: {ch:?}"
            );
        }
        // Every sentence appears in at least one chunk.
        for s in [
            "First sentence here.",
            "Second one is a bit longer!",
            "Third?",
            "Fourth sentence ends it.",
        ] {
            assert!(
                chunks.iter().any(|c| c.contains(s)),
                "missing {s:?} in {chunks:?}"
            );
        }
    }

    #[test]
    fn sentence_strategy_hard_splits_oversized_sentences_and_never_panics_on_utf8() {
        let long = "😀".repeat(50) + " é".repeat(40).as_str() + " end.";
        let c = cfg(json!({"chunk_size": 30, "overlap": 5, "min_chunk_size": 3}));
        let chunks = chunk_text(&long, &c);
        assert!(chunks.len() > 2);
        for ch in &chunks {
            assert!(ch.chars().count() <= 33, "{ch:?}");
        }
        let joined: String = chunks.concat();
        assert!(joined.matches('😀').count() >= 50, "emoji lost");

        for seed in 0..40u64 {
            let text = synth_text(seed, 120);
            for strategy in ["sentence", "paragraph", "fixed"] {
                let c = cfg(
                    json!({"strategy": strategy, "chunk_size": 40, "overlap": 8, "min_chunk_size": 6}),
                );
                let chunks = chunk_text(&text, &c);
                // Bound: chunk_size + merged tail (< min_chunk_size) + separator (<= 2 chars).
                for ch in &chunks {
                    assert!(ch.chars().count() <= 40 + 6 + 2, "{strategy}: {ch:?}");
                }
                // All non-whitespace chars survive (some duplicated by overlap).
                let mut got: HashMap<char, usize> = HashMap::new();
                for ch in &chunks {
                    for (k, v) in non_ws_counts(ch) {
                        *got.entry(k).or_insert(0) += v;
                    }
                }
                for (k, v) in non_ws_counts(&text) {
                    assert!(
                        got.get(&k).copied().unwrap_or(0) >= v,
                        "{strategy}: char {k:?} lost (seed {seed})"
                    );
                }
            }
        }
    }

    #[test]
    fn paragraph_strategy_splits_on_blank_lines() {
        let text = "Para one line a\nline b.\n\n   \nPara two is here.\n\nPara three.";
        let c = cfg(
            json!({"strategy": "paragraph", "chunk_size": 25, "overlap": 0, "min_chunk_size": 2}),
        );
        let chunks = chunk_text(text, &c);
        assert_eq!(
            chunks,
            vec![
                "Para one line a\nline b.",
                "Para two is here.",
                "Para three."
            ]
        );
        assert_eq!(split_paragraphs(text).len(), 3);
    }

    #[test]
    fn short_tail_is_merged_into_previous_chunk() {
        let text = "Alpha beta gamma delta. Epsilon zeta eta theta. Ok.";
        let c = cfg(json!({"chunk_size": 24, "overlap": 0, "min_chunk_size": 10}));
        let chunks = chunk_text(text, &c);
        assert_eq!(chunks.last().unwrap(), "Epsilon zeta eta theta. Ok.");
        assert_eq!(chunks.len(), 2);
    }

    #[tokio::test]
    async fn many_input_and_metadata_indices_are_contiguous() {
        let text = synth_text(7, 200);
        let a = doc("a", &text);
        let b = doc("b", "tiny");
        let input = PluginInput::Many(vec![
            PluginOutput::Documents(vec![a]),
            PluginOutput::Many(vec![PluginOutput::Documents(vec![b])]),
        ]);
        let out = match ChunkerPlugin::new()
            .execute(
                &ActivityContext::noop(),
                input,
                json!({"chunk_size": 60, "overlap": 10}),
            )
            .await
            .unwrap()
        {
            PluginOutput::Documents(d) => d,
            other => panic!("{other:?}"),
        };
        let a_chunks: Vec<Document> = out
            .iter()
            .filter(|d| d.meta.parent_id.as_deref() == Some("a"))
            .cloned()
            .collect();
        let b_chunks: Vec<Document> = out
            .iter()
            .filter(|d| d.meta.parent_id.as_deref() == Some("b"))
            .cloned()
            .collect();
        assert!(a_chunks.len() > 3);
        assert_eq!(b_chunks.len(), 1);
        assert_contiguous(&a_chunks, "a");
        assert_contiguous(&b_chunks, "b");
    }

    #[tokio::test]
    async fn rejects_bytes_input_and_honours_cancellation() {
        let plugin = ChunkerPlugin::new();
        let err = plugin
            .execute(
                &ActivityContext::noop(),
                PluginInput::Bytes(Blob::new(vec![1], "text/plain", None)),
                json!({}),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidInput(_)));

        let ctx = ActivityContext::noop();
        ctx.cancellation_flag()
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let err = plugin
            .execute(&ctx, PluginInput::Documents(vec![doc("x", "y")]), json!({}))
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::Cancelled));
    }
}
