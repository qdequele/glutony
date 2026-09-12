//! # `xlsx_extractor`
//!
//! Built-in meili-ingest plugin that reads spreadsheets (`.xlsx`, `.xlsm`, `.xlsb`,
//! `.xls`, `.ods`) with [`calamine`](https://crates.io/crates/calamine).
//!
//! * `mode: "row"` (default) → one [`Document`] per data row. The first row of each
//!   sheet is used as headers (`has_headers: true`), each cell becomes
//!   `fields[header]` with its natural JSON type, `content` is the non-empty cell
//!   texts joined by a space, `fields._row` is the 1-based row number,
//!   `_meta.section` is the sheet name and `_meta.page` the 1-based sheet index.
//!   Id: `<filename_stem>_<sheet>_r<row>`.
//! * `mode: "sheet"` → one document per sheet with tab-separated content.
//!
//! `sheets` restricts extraction to the named sheets; `max_rows` caps the rows read
//! per sheet.

use std::io::Cursor;

use calamine::{Data, Range, Reader};
use meili_ingest_plugin_sdk::prelude::*;
use serde::Deserialize;

/// Plugin name, as referenced by `steps[].plugin`.
pub const NAME: &str = "xlsx_extractor";

/// Number of rows between two heartbeats.
const HEARTBEAT_EVERY: usize = 100;

/// The spreadsheet extractor plugin. Stateless; construct with [`XlsxExtractorPlugin::new`].
#[derive(Debug, Clone, Default)]
pub struct XlsxExtractorPlugin;

impl XlsxExtractorPlugin {
    /// Create the plugin.
    pub fn new() -> Self {
        Self
    }
}

/// How the workbook is turned into documents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Mode {
    /// One document per data row.
    #[default]
    Row,
    /// One document per sheet.
    Sheet,
}

/// Step configuration for [`XlsxExtractorPlugin`].
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Config {
    /// `row` (default) or `sheet`.
    mode: Mode,
    /// Treat the first row of every sheet as column headers.
    has_headers: bool,
    /// Only extract these sheets (by name). All sheets when unset.
    sheets: Option<Vec<String>>,
    /// Maximum number of data rows to read per sheet.
    max_rows: Option<usize>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            mode: Mode::Row,
            has_headers: true,
            sheets: None,
            max_rows: None,
        }
    }
}

fn parse_config(value: serde_json::Value) -> Result<Config, PluginError> {
    if value.is_null() {
        return Ok(Config::default());
    }
    serde_json::from_value(value).map_err(|e| PluginError::InvalidConfig(e.to_string()))
}

/// Meilisearch-safe id derived from the blob's filename stem (a UUID when unknown).
fn base_id(blob: &Blob) -> String {
    let stem = blob
        .filename
        .as_deref()
        .and_then(|f| std::path::Path::new(f).file_stem())
        .and_then(|s| s.to_str())
        .unwrap_or("");
    sanitize_id(stem)
}

/// Provenance metadata shared by every document produced from `blob`.
fn base_meta(blob: &Blob) -> DocumentMeta {
    DocumentMeta {
        source: blob.filename.clone(),
        filename: blob.filename.clone(),
        mime: Some(blob.mime.clone()),
        ..DocumentMeta::default()
    }
}

/// Cell → JSON. `None` for empty cells, empty strings and error cells.
fn cell_value(cell: &Data) -> Option<serde_json::Value> {
    use serde_json::Value;
    match cell {
        Data::Empty | Data::Error(_) => None,
        Data::Int(i) => Some(Value::from(*i)),
        Data::Float(f) => {
            // Excel stores every number as a float; keep integers as integers.
            if f.fract() == 0.0 && f.abs() < 9_007_199_254_740_992.0 {
                Some(Value::from(*f as i64))
            } else {
                serde_json::Number::from_f64(*f).map(Value::Number)
            }
        }
        Data::String(s) => {
            let s = s.trim();
            (!s.is_empty()).then(|| Value::String(s.to_owned()))
        }
        Data::Bool(b) => Some(Value::Bool(*b)),
        Data::DateTime(dt) => Some(Value::String(match dt.as_datetime() {
            Some(naive) => naive.format("%Y-%m-%dT%H:%M:%S").to_string(),
            None => dt.to_string(),
        })),
        Data::DateTimeIso(s) | Data::DurationIso(s) => Some(Value::String(s.clone())),
    }
}

/// Cell → display text (empty for empty/error cells).
fn cell_text(cell: &Data) -> String {
    match cell_value(cell) {
        Some(serde_json::Value::String(s)) => s,
        Some(other) => other.to_string(),
        None => String::new(),
    }
}

/// Header name for a column: the trimmed header cell text, or `col_<n>` (1-based).
fn header_name(headers: &[String], col: usize) -> String {
    headers
        .get(col)
        .filter(|h| !h.is_empty())
        .cloned()
        .unwrap_or_else(|| format!("col_{}", col + 1))
}

fn sheet_error(name: &str, e: calamine::Error) -> PluginError {
    PluginError::NonRetryable(format!("failed to read sheet {name:?}: {e}"))
}

/// Build the documents for one sheet in row mode.
fn extract_rows(
    ctx: &ActivityContext,
    cfg: &Config,
    base: &str,
    meta: &DocumentMeta,
    sheet_index: usize,
    sheet: &str,
    range: &Range<Data>,
) -> Result<Vec<Document>, PluginError> {
    let first_row = range.start().map(|(r, _)| r as usize).unwrap_or(0);
    let mut rows = range.rows();
    let headers: Vec<String> = if cfg.has_headers {
        rows.next()
            .map(|r| r.iter().map(cell_text).collect())
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    let header_offset = usize::from(cfg.has_headers);
    let sheet_id = sanitize_id(sheet);
    let page = u32::try_from(sheet_index + 1).ok();

    let mut docs = Vec::new();
    for (i, row) in rows.enumerate() {
        ctx.check_cancelled()?;
        if cfg.max_rows.is_some_and(|max| i >= max) {
            break;
        }
        let row_number = first_row + header_offset + i + 1;
        if row_number.is_multiple_of(HEARTBEAT_EVERY) {
            ctx.heartbeat(format!("{sheet} row {row_number}"));
        }
        let mut fields = serde_json::Map::new();
        let mut parts = Vec::new();
        for (col, cell) in row.iter().enumerate() {
            let Some(value) = cell_value(cell) else {
                continue;
            };
            let text = match &value {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            parts.push(text);
            fields.insert(header_name(&headers, col), value);
        }
        if parts.is_empty() {
            continue;
        }
        fields.insert("_row".into(), serde_json::Value::from(row_number));
        let mut doc =
            Document::with_id(format!("{base}_{sheet_id}_r{row_number}"), parts.join(" "));
        doc.fields = fields;
        doc.meta = meta.clone();
        doc.meta.section = Some(sheet.to_owned());
        doc.meta.page = page;
        docs.push(doc);
    }
    Ok(docs)
}

/// Build the single document for one sheet in sheet mode.
fn extract_sheet(
    ctx: &ActivityContext,
    cfg: &Config,
    base: &str,
    meta: &DocumentMeta,
    sheet_index: usize,
    sheet: &str,
    range: &Range<Data>,
) -> Result<Option<Document>, PluginError> {
    let mut lines = Vec::new();
    for (i, row) in range.rows().enumerate() {
        ctx.check_cancelled()?;
        if cfg.max_rows.is_some_and(|max| i >= max) {
            break;
        }
        if i > 0 && i.is_multiple_of(HEARTBEAT_EVERY) {
            ctx.heartbeat(format!("{sheet} row {i}"));
        }
        let mut cells: Vec<String> = row.iter().map(cell_text).collect();
        // Drop trailing empty cells so rows shorter than the sheet width have no dangling tabs.
        while cells.last().is_some_and(String::is_empty) {
            cells.pop();
        }
        if !cells.is_empty() {
            lines.push(cells.join("\t"));
        }
    }
    if lines.is_empty() {
        return Ok(None);
    }
    let mut doc = Document::with_id(format!("{base}_{}", sanitize_id(sheet)), lines.join("\n"));
    doc.title = Some(sheet.to_owned());
    doc.fields
        .insert("_rows".into(), serde_json::Value::from(lines.len()));
    doc.meta = meta.clone();
    doc.meta.section = Some(sheet.to_owned());
    doc.meta.page = u32::try_from(sheet_index + 1).ok();
    Ok(Some(doc))
}

#[async_trait]
impl Plugin for XlsxExtractorPlugin {
    fn manifest(&self) -> PluginManifest {
        PluginManifest::new(NAME, env!("CARGO_PKG_VERSION"))
            .description(
                "Extracts spreadsheets (xlsx, xlsm, xlsb, xls, ods): one document per row \
                 with header-named fields, or one document per sheet.",
            )
            .accepts([InputKind::Bytes])
            .produces(OutputKind::Documents)
            .content_types([
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "application/vnd.ms-excel",
                "application/vnd.ms-excel.sheet.macroEnabled.12",
                "application/vnd.ms-excel.sheet.binary.macroEnabled.12",
                "application/vnd.oasis.opendocument.spreadsheet",
            ])
            .config_schema(serde_json::json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "mode": {
                        "type": "string",
                        "enum": ["row", "sheet"],
                        "default": "row",
                        "description": "`row`: one document per data row; `sheet`: one document per sheet."
                    },
                    "has_headers": {
                        "type": "boolean",
                        "default": true,
                        "description": "Use the first row of each sheet as field names (row mode)."
                    },
                    "sheets": {
                        "type": ["array", "null"],
                        "items": {"type": "string"},
                        "default": null,
                        "description": "Only extract these sheet names."
                    },
                    "max_rows": {
                        "type": ["integer", "null"],
                        "minimum": 0,
                        "default": null,
                        "description": "Maximum number of rows to read per sheet."
                    }
                }
            }))
    }

    async fn execute(
        &self,
        ctx: &ActivityContext,
        input: PluginInput,
        config: serde_json::Value,
    ) -> Result<PluginOutput, PluginError> {
        let cfg = parse_config(config)?;
        let blob = input.into_bytes()?;
        let base = base_id(&blob);
        let meta = base_meta(&blob);

        let data = blob.data.clone();
        let mut workbook =
            run_blocking(move || calamine::open_workbook_auto_from_rs(Cursor::new(data)))
                .await?
                .map_err(|e| {
                    PluginError::NonRetryable(format!("failed to open spreadsheet: {e}"))
                })?;

        let mut docs = Vec::new();
        for (sheet_index, sheet) in workbook.sheet_names().iter().enumerate() {
            ctx.check_cancelled()?;
            if cfg
                .sheets
                .as_ref()
                .is_some_and(|wanted| !wanted.iter().any(|w| w == sheet))
            {
                continue;
            }
            let range = workbook
                .worksheet_range(sheet)
                .map_err(|e| sheet_error(sheet, e))?;
            match cfg.mode {
                Mode::Row => docs.extend(extract_rows(
                    ctx,
                    &cfg,
                    &base,
                    &meta,
                    sheet_index,
                    sheet,
                    &range,
                )?),
                Mode::Sheet => docs.extend(extract_sheet(
                    ctx,
                    &cfg,
                    &base,
                    &meta,
                    sheet_index,
                    sheet,
                    &range,
                )?),
            }
            ctx.heartbeat(format!("sheet {sheet} done"));
        }
        tracing::debug!(
            plugin = NAME,
            documents = docs.len(),
            "extracted spreadsheet"
        );
        Ok(PluginOutput::Documents(docs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_xlsxwriter::Workbook;

    const XLSX_MIME: &str = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";

    /// Two sheets: `People` (name/age/active + a blank row) and `Notes` (single column).
    fn make_xlsx() -> Vec<u8> {
        let mut wb = Workbook::new();
        let ws = wb.add_worksheet();
        ws.set_name("People").unwrap();
        ws.write_string(0, 0, "name").unwrap();
        ws.write_string(0, 1, "age").unwrap();
        ws.write_string(0, 2, "active").unwrap();
        ws.write_string(0, 3, "score").unwrap();
        ws.write_string(1, 0, "Alice").unwrap();
        ws.write_number(1, 1, 30).unwrap();
        ws.write_boolean(1, 2, true).unwrap();
        ws.write_number(1, 3, 4.5).unwrap();
        // row 3 (index 2) intentionally blank
        ws.write_string(3, 0, "Bob").unwrap();
        ws.write_number(3, 1, 41).unwrap();
        ws.write_boolean(3, 2, false).unwrap();
        let ws2 = wb.add_worksheet();
        ws2.set_name("Notes").unwrap();
        ws2.write_string(0, 0, "note").unwrap();
        ws2.write_string(1, 0, "hello world").unwrap();
        wb.save_to_buffer().unwrap()
    }

    fn input(bytes: Vec<u8>) -> PluginInput {
        PluginInput::Bytes(Blob::new(
            bytes,
            XLSX_MIME,
            Some("data/Team Roster.xlsx".into()),
        ))
    }

    #[test]
    fn manifest_is_well_formed() {
        let m = XlsxExtractorPlugin::new().manifest();
        assert_eq!(m.name, NAME);
        assert!(m.accepts_kind(InputKind::Bytes));
        assert_eq!(m.produces, OutputKind::Documents);
        assert!(m.content_types.iter().any(|c| c == XLSX_MIME));
        for key in ["mode", "has_headers", "sheets", "max_rows"] {
            assert!(
                m.config_schema["properties"][key].is_object(),
                "missing {key}"
            );
        }
    }

    #[tokio::test]
    async fn row_mode_produces_one_document_per_row() {
        let out = XlsxExtractorPlugin::new()
            .execute(
                &ActivityContext::noop(),
                input(make_xlsx()),
                serde_json::json!({}),
            )
            .await
            .unwrap();
        let docs = out.into_documents().unwrap();
        assert_eq!(docs.len(), 3, "{docs:#?}");

        let alice = &docs[0];
        assert_eq!(alice.id, "Team_Roster_People_r2");
        assert_eq!(alice.fields["name"], "Alice");
        assert_eq!(alice.fields["age"], 30);
        assert_eq!(alice.fields["active"], true);
        assert_eq!(alice.fields["score"], 4.5);
        assert_eq!(alice.fields["_row"], 2);
        assert_eq!(alice.content, "Alice 30 true 4.5");
        assert_eq!(alice.meta.section.as_deref(), Some("People"));
        assert_eq!(alice.meta.page, Some(1));
        assert_eq!(
            alice.meta.filename.as_deref(),
            Some("data/Team Roster.xlsx")
        );
        assert_eq!(alice.meta.mime.as_deref(), Some(XLSX_MIME));

        let bob = &docs[1];
        assert_eq!(bob.id, "Team_Roster_People_r4");
        assert_eq!(bob.fields["_row"], 4);
        assert!(!bob.fields.contains_key("score"), "empty cells are skipped");

        let note = &docs[2];
        assert_eq!(note.id, "Team_Roster_Notes_r2");
        assert_eq!(note.fields["note"], "hello world");
        assert_eq!(note.meta.page, Some(2));
    }

    #[tokio::test]
    async fn sheet_mode_filters_and_limits() {
        let plugin = XlsxExtractorPlugin::new();
        let ctx = ActivityContext::noop();

        let out = plugin
            .execute(
                &ctx,
                input(make_xlsx()),
                serde_json::json!({"mode": "sheet"}),
            )
            .await
            .unwrap();
        let docs = out.into_documents().unwrap();
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[0].id, "Team_Roster_People");
        assert_eq!(docs[0].title.as_deref(), Some("People"));
        assert_eq!(
            docs[0].content,
            "name\tage\tactive\tscore\nAlice\t30\ttrue\t4.5\nBob\t41\tfalse"
        );
        assert_eq!(docs[0].fields["_rows"], 3);
        assert_eq!(docs[1].content, "note\nhello world");

        let out = plugin
            .execute(
                &ctx,
                input(make_xlsx()),
                serde_json::json!({"sheets": ["Notes"]}),
            )
            .await
            .unwrap();
        let docs = out.into_documents().unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].meta.section.as_deref(), Some("Notes"));

        let out = plugin
            .execute(&ctx, input(make_xlsx()), serde_json::json!({"max_rows": 1}))
            .await
            .unwrap();
        let docs = out.into_documents().unwrap();
        assert_eq!(
            docs.iter().map(|d| d.id.as_str()).collect::<Vec<_>>(),
            vec!["Team_Roster_People_r2", "Team_Roster_Notes_r2"]
        );

        let out = plugin
            .execute(
                &ctx,
                input(make_xlsx()),
                serde_json::json!({"has_headers": false, "sheets": ["Notes"]}),
            )
            .await
            .unwrap();
        let docs = out.into_documents().unwrap();
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[0].fields["col_1"], "note");
        assert_eq!(docs[0].fields["_row"], 1);
    }

    #[tokio::test]
    async fn invalid_config_is_rejected() {
        let err = XlsxExtractorPlugin::new()
            .execute(
                &ActivityContext::noop(),
                input(make_xlsx()),
                serde_json::json!({"mode": "column"}),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidConfig(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn wrong_input_kind_is_invalid_input() {
        let err = XlsxExtractorPlugin::new()
            .execute(
                &ActivityContext::noop(),
                PluginInput::Empty,
                serde_json::json!({}),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidInput(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn corrupt_workbook_is_non_retryable() {
        let err = XlsxExtractorPlugin::new()
            .execute(
                &ActivityContext::noop(),
                input(b"definitely not a spreadsheet".to_vec()),
                serde_json::json!({}),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::NonRetryable(_)), "got {err:?}");
    }
}
