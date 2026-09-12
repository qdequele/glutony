//! Request payload extraction: multipart uploads, JSON bodies (`url` / `s3` /
//! `documents` / `items`) and raw bodies (SPEC §4 "Request formats").
//!
//! MIME detection for every file goes through [`meili_ingest_router::detect_mime`]
//! (magic bytes win over headers, SPEC §10).

use axum::extract::Multipart;
use bytes::Bytes;
use meili_ingest_plugin_sdk::{Blob, Document};
use meili_ingest_router::detect_mime;
use serde_json::Value;
use uuid::Uuid;

use crate::error::GatewayError;

/// What the caller wants ingested.
#[derive(Debug, Clone, PartialEq)]
pub enum IngestPayload {
    /// Uploaded bytes (multipart `file` field or raw body), MIME already detected.
    File(Blob),
    /// Remote HTTP(S) URL; the worker fetches it.
    Url {
        /// The URL.
        url: String,
        /// Filename hint.
        filename: Option<String>,
    },
    /// `s3://bucket/key` (or any object-store URI); the worker streams it.
    S3 {
        /// The URI.
        uri: String,
        /// Filename hint.
        filename: Option<String>,
    },
    /// Inline documents, indexed without extraction.
    Documents(Vec<Document>),
    /// Several independent items (one job each).
    Batch(Vec<IngestPayload>),
}

impl IngestPayload {
    /// Filename hint of this payload (basename of the URL/URI when no explicit hint).
    pub fn filename(&self) -> Option<String> {
        match self {
            IngestPayload::File(b) => b.filename.clone(),
            IngestPayload::Url { url, filename } => filename.clone().or_else(|| basename_of(url)),
            IngestPayload::S3 { uri, filename } => filename.clone().or_else(|| basename_of(uri)),
            IngestPayload::Documents(_) | IngestPayload::Batch(_) => None,
        }
    }

    /// Best-effort MIME type: detected for files, guessed from the name for refs,
    /// `application/json` for documents, `None` for batches.
    pub fn mime(&self) -> Option<String> {
        match self {
            IngestPayload::File(b) => Some(b.mime.clone()),
            IngestPayload::Url { .. } | IngestPayload::S3 { .. } => Some(
                self.filename()
                    .and_then(|n| mime_guess::from_path(&n).first_raw())
                    .unwrap_or("application/octet-stream")
                    .to_string(),
            ),
            IngestPayload::Documents(_) => Some("application/json".into()),
            IngestPayload::Batch(_) => None,
        }
    }
}

/// Result of [`extract_payload`]: the payload plus the optional `index` and `pipeline`
/// values carried in the body (multipart fields or JSON keys).
#[derive(Debug, Clone, PartialEq)]
pub struct Extracted {
    /// The payload.
    pub payload: IngestPayload,
    /// `index` form field / JSON key.
    pub index: Option<String>,
    /// `pipeline` form field / JSON key.
    pub pipeline: Option<String>,
}

/// Last path segment of a URL/URI, without query string. `None` when empty.
pub fn basename_of(location: &str) -> Option<String> {
    let no_query = location.split(['?', '#']).next().unwrap_or(location);
    // Drop the scheme + authority so a bare host (`https://x.com`) yields nothing.
    let rest = no_query
        .split_once("://")
        .map(|(_, r)| r)
        .unwrap_or(no_query);
    let trimmed = rest.trim_end_matches('/');
    let (_, name) = trimmed.rsplit_once('/')?;
    let name = name.trim();
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

/// Whether the content type is multipart/form-data.
pub fn is_multipart(content_type: Option<&str>) -> bool {
    content_type
        .map(|ct| {
            ct.trim()
                .to_ascii_lowercase()
                .starts_with("multipart/form-data")
        })
        .unwrap_or(false)
}

/// Whether the content type is a JSON media type (`application/json`, `*/*+json`).
pub fn is_json(content_type: Option<&str>) -> bool {
    let Some(ct) = content_type else { return false };
    let essence = ct
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    essence == "application/json" || essence.ends_with("+json")
}

/// Extract the ingest payload from a request body.
///
/// * `multipart` set → the multipart form is parsed (`file`, `url`, `s3`, `documents`,
///   `index`, `pipeline`, `filename` fields);
/// * else JSON content type → [`payload_from_json`];
/// * else → raw body becomes a [`IngestPayload::File`] with `filename_hint` (from a
///   `Content-Disposition` header or `?filename=`) used for MIME detection.
pub async fn extract_payload(
    content_type: Option<&str>,
    body: Bytes,
    multipart: Option<Multipart>,
    filename_hint: Option<&str>,
) -> Result<Extracted, GatewayError> {
    if let Some(mp) = multipart {
        return extract_multipart(mp).await;
    }
    if is_json(content_type) {
        if body.is_empty() {
            return Err(GatewayError::BadRequest("empty JSON body".into()));
        }
        let value: Value = serde_json::from_slice(&body)?;
        return payload_from_json(value);
    }
    if body.is_empty() {
        return Err(GatewayError::BadRequest(
            "empty body: upload a file (multipart `file` field or raw body) or send a JSON body with `url`, `s3` or `documents`"
                .into(),
        ));
    }
    Ok(Extracted {
        payload: file_payload(body.to_vec(), filename_hint, content_type),
        index: None,
        pipeline: None,
    })
}

/// Build a [`IngestPayload::File`] running MIME detection.
pub fn file_payload(
    data: Vec<u8>,
    filename: Option<&str>,
    content_type_hint: Option<&str>,
) -> IngestPayload {
    let filename = filename
        .map(str::trim)
        .filter(|f| !f.is_empty())
        .map(str::to_string);
    let hint = content_type_hint
        .map(|ct| ct.split(';').next().unwrap_or("").trim())
        .filter(|ct| !ct.is_empty());
    let mime = detect_mime(&data, filename.as_deref(), hint);
    IngestPayload::File(Blob::new(data, mime, filename))
}

async fn extract_multipart(mut mp: Multipart) -> Result<Extracted, GatewayError> {
    let mut file: Option<(Vec<u8>, Option<String>, Option<String>)> = None;
    let mut url = None;
    let mut s3 = None;
    let mut documents: Option<String> = None;
    let mut index = None;
    let mut pipeline = None;
    let mut filename_override = None;

    while let Some(field) = mp.next_field().await? {
        let name = field.name().unwrap_or("").to_string();
        match name.as_str() {
            "file" => {
                let filename = field.file_name().map(str::to_string);
                let ct = field.content_type().map(str::to_string);
                let data = field.bytes().await?;
                file = Some((data.to_vec(), filename, ct));
            }
            "url" => url = non_empty(field.text().await?),
            "s3" => s3 = non_empty(field.text().await?),
            "documents" => documents = non_empty(field.text().await?),
            "index" => index = non_empty(field.text().await?),
            "pipeline" => pipeline = non_empty(field.text().await?),
            "filename" => filename_override = non_empty(field.text().await?),
            other => {
                tracing::debug!(field = other, "ignoring unknown multipart field");
                // Drain the field so the stream stays consistent.
                let _ = field.bytes().await?;
            }
        }
    }

    let payload = if let Some((data, filename, ct)) = file {
        let filename = filename_override.or(filename);
        file_payload(data, filename.as_deref(), ct.as_deref())
    } else if let Some(url) = url {
        IngestPayload::Url {
            url,
            filename: filename_override,
        }
    } else if let Some(uri) = s3 {
        IngestPayload::S3 {
            uri,
            filename: filename_override,
        }
    } else if let Some(text) = documents {
        let value: Value = serde_json::from_str(&text).map_err(|e| {
            GatewayError::BadRequest(format!("`documents` field is not valid JSON: {e}"))
        })?;
        IngestPayload::Documents(documents_from_json(value)?)
    } else {
        return Err(GatewayError::BadRequest(
            "multipart form must contain a `file`, `url`, `s3` or `documents` field".into(),
        ));
    };
    Ok(Extracted {
        payload,
        index,
        pipeline,
    })
}

fn non_empty(s: String) -> Option<String> {
    let t = s.trim();
    if t.is_empty() {
        None
    } else {
        Some(t.to_string())
    }
}

fn str_field(obj: &serde_json::Map<String, Value>, key: &str) -> Option<String> {
    obj.get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// Parse a JSON body: `{url}`, `{s3}`, `{documents:[...]}`, `{items:[...]}` (batch), a bare
/// array of documents; `index` and `pipeline` keys are returned alongside.
pub fn payload_from_json(value: Value) -> Result<Extracted, GatewayError> {
    match value {
        Value::Array(_) => Ok(Extracted {
            payload: IngestPayload::Documents(documents_from_json(value)?),
            index: None,
            pipeline: None,
        }),
        Value::Object(obj) => {
            let index = str_field(&obj, "index");
            let pipeline = str_field(&obj, "pipeline");
            let payload = if let Some(items) = obj.get("items") {
                let Value::Array(items) = items else {
                    return Err(GatewayError::BadRequest("`items` must be an array".into()));
                };
                let mut out = Vec::with_capacity(items.len());
                for (i, item) in items.iter().enumerate() {
                    let Value::Object(_) = item else {
                        return Err(GatewayError::BadRequest(format!(
                            "items[{i}] must be an object"
                        )));
                    };
                    let inner = single_from_object(item.clone())
                        .map_err(|e| GatewayError::BadRequest(format!("items[{i}]: {e}")))?;
                    out.push(inner);
                }
                IngestPayload::Batch(out)
            } else {
                single_from_object(Value::Object(obj))?
            };
            Ok(Extracted {
                payload,
                index,
                pipeline,
            })
        }
        _ => Err(GatewayError::BadRequest(
            "JSON body must be an object or an array of documents".into(),
        )),
    }
}

/// `{url}` / `{s3}` / `{documents}` object → a single (non-batch) payload.
fn single_from_object(value: Value) -> Result<IngestPayload, GatewayError> {
    let Value::Object(obj) = value else {
        return Err(GatewayError::BadRequest("expected a JSON object".into()));
    };
    let filename = str_field(&obj, "filename");
    if let Some(url) = str_field(&obj, "url") {
        return Ok(IngestPayload::Url { url, filename });
    }
    if let Some(uri) = str_field(&obj, "s3") {
        return Ok(IngestPayload::S3 { uri, filename });
    }
    if let Some(docs) = obj.get("documents") {
        return Ok(IngestPayload::Documents(documents_from_json(docs.clone())?));
    }
    Err(GatewayError::BadRequest(
        "JSON body must contain `url`, `s3`, `documents` or `items`".into(),
    ))
}

/// Convert a JSON array (or a single object) into [`Document`]s.
pub fn documents_from_json(value: Value) -> Result<Vec<Document>, GatewayError> {
    let items = match value {
        Value::Array(a) => a,
        v @ Value::Object(_) => vec![v],
        _ => {
            return Err(GatewayError::BadRequest(
                "`documents` must be an array of objects".into(),
            ));
        }
    };
    if items.is_empty() {
        return Err(GatewayError::BadRequest(
            "`documents` must not be empty".into(),
        ));
    }
    items
        .into_iter()
        .enumerate()
        .map(|(i, v)| {
            document_from_json(v)
                .map_err(|e| GatewayError::BadRequest(format!("documents[{i}]: {e}")))
        })
        .collect()
}

/// One JSON object → [`Document`]: `id` from `id` (string or number) or generated;
/// `title` from `title`; `content` from `content`/`text`/`body` (string) or all string
/// values joined by a space; every other key goes into `fields`.
pub fn document_from_json(value: Value) -> Result<Document, GatewayError> {
    let Value::Object(mut obj) = value else {
        return Err(GatewayError::BadRequest(
            "each document must be a JSON object".into(),
        ));
    };
    let id = match obj.remove("id") {
        Some(Value::String(s)) if !s.trim().is_empty() => {
            meili_ingest_plugin_sdk::sanitize_id(s.trim())
        }
        Some(Value::Number(n)) => n.to_string(),
        Some(Value::Null) | None => Uuid::new_v4().to_string(),
        Some(other) => {
            return Err(GatewayError::BadRequest(format!(
                "`id` must be a string or number, got {other}"
            )));
        }
    };
    let title = match obj.remove("title") {
        Some(Value::String(s)) => Some(s),
        Some(Value::Null) | None => None,
        Some(other) => Some(other.to_string()),
    };
    let mut content = None;
    for key in ["content", "text", "body"] {
        if let Some(Value::String(s)) = obj.get(key) {
            content = Some(s.clone());
            obj.remove(key);
            break;
        }
    }
    let content = content.unwrap_or_else(|| {
        obj.values()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join(" ")
    });
    Ok(Document {
        id,
        title,
        content,
        fields: obj,
        meta: Default::default(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::extract::FromRequest;
    use axum::http::{Request, header};
    use serde_json::json;

    const PDF_MAGIC: &[u8] =
        b"%PDF-1.4\n%\xE2\xE3\xCF\xD3\n1 0 obj\n<< /Type /Catalog >>\nendobj\n";

    async fn multipart_from(boundary: &str, body: String) -> Multipart {
        let req = Request::builder()
            .method("POST")
            .uri("/ingest")
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(Body::from(body))
            .unwrap();
        Multipart::from_request(req, &()).await.unwrap()
    }

    fn part(
        boundary: &str,
        name: &str,
        filename: Option<&str>,
        ct: Option<&str>,
        data: &[u8],
    ) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        match filename {
            Some(f) => out.extend_from_slice(
                format!("Content-Disposition: form-data; name=\"{name}\"; filename=\"{f}\"\r\n")
                    .as_bytes(),
            ),
            None => out.extend_from_slice(
                format!("Content-Disposition: form-data; name=\"{name}\"\r\n").as_bytes(),
            ),
        }
        if let Some(ct) = ct {
            out.extend_from_slice(format!("Content-Type: {ct}\r\n").as_bytes());
        }
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(data);
        out.extend_from_slice(b"\r\n");
        out
    }

    fn form(boundary: &str, parts: Vec<Vec<u8>>) -> String {
        let mut out = Vec::new();
        for p in parts {
            out.extend(p);
        }
        out.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
        // Test bodies are ASCII apart from the PDF magic, which is valid latin-1 → use lossy.
        String::from_utf8(out)
            .unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
    }

    // --- helpers ------------------------------------------------------------------------

    #[test]
    fn basename_extraction() {
        assert_eq!(
            basename_of("https://x.com/a/b/report.pdf?x=1"),
            Some("report.pdf".into())
        );
        assert_eq!(
            basename_of("s3://bucket/dir/video.mp4"),
            Some("video.mp4".into())
        );
        assert_eq!(basename_of("https://x.com/"), None);
        assert_eq!(basename_of("https://x.com"), None);
        assert_eq!(
            basename_of("http://host:8080/file.csv#frag"),
            Some("file.csv".into())
        );
    }

    #[test]
    fn content_type_predicates() {
        assert!(is_multipart(Some("multipart/form-data; boundary=abc")));
        assert!(is_multipart(Some("Multipart/Form-Data")));
        assert!(!is_multipart(Some("application/json")));
        assert!(!is_multipart(None));
        assert!(is_json(Some("application/json")));
        assert!(is_json(Some("application/json; charset=utf-8")));
        assert!(is_json(Some("application/vnd.api+json")));
        assert!(!is_json(Some("text/json-ish")));
        assert!(!is_json(None));
    }

    #[test]
    fn document_from_json_uses_id_title_content() {
        let d = document_from_json(
            json!({"id": "doc 1", "title": "T", "content": "hello", "price": 3}),
        )
        .unwrap();
        assert_eq!(d.id, "doc_1");
        assert_eq!(d.title.as_deref(), Some("T"));
        assert_eq!(d.content, "hello");
        assert_eq!(d.fields.get("price"), Some(&json!(3)));
        assert!(!d.fields.contains_key("content"));
        assert!(!d.fields.contains_key("id"));
    }

    #[test]
    fn document_from_json_numeric_id_and_text_body_fallbacks() {
        let d = document_from_json(json!({"id": 42, "text": "from text"})).unwrap();
        assert_eq!(d.id, "42");
        assert_eq!(d.content, "from text");
        let d = document_from_json(json!({"body": "from body", "n": 1})).unwrap();
        assert_eq!(d.content, "from body");
        assert_eq!(d.fields.len(), 1);
    }

    #[test]
    fn document_from_json_generates_id_and_joins_strings() {
        let d = document_from_json(json!({"a": "x", "b": 2, "c": "y"})).unwrap();
        assert!(Uuid::parse_str(&d.id).is_ok());
        assert_eq!(d.content, "x y");
        assert_eq!(d.fields.len(), 3);
    }

    #[test]
    fn document_from_json_rejects_bad_shapes() {
        assert!(document_from_json(json!("str")).is_err());
        assert!(document_from_json(json!({"id": ["x"]})).is_err());
        assert!(documents_from_json(json!([])).is_err());
        assert!(documents_from_json(json!("nope")).is_err());
        let err = documents_from_json(json!([{"id": "ok"}, 3])).unwrap_err();
        assert!(matches!(err, GatewayError::BadRequest(m) if m.starts_with("documents[1]")));
    }

    // --- JSON bodies --------------------------------------------------------------------

    #[test]
    fn json_url_with_index_and_pipeline() {
        let e = payload_from_json(
            json!({"url": "https://e.com/doc.pdf", "index": "contracts", "pipeline": "p"}),
        )
        .unwrap();
        assert_eq!(
            e.payload,
            IngestPayload::Url {
                url: "https://e.com/doc.pdf".into(),
                filename: None
            }
        );
        assert_eq!(e.index.as_deref(), Some("contracts"));
        assert_eq!(e.pipeline.as_deref(), Some("p"));
        assert_eq!(e.payload.filename().as_deref(), Some("doc.pdf"));
        assert_eq!(e.payload.mime().as_deref(), Some("application/pdf"));
    }

    #[test]
    fn json_s3_with_filename_hint() {
        let e = payload_from_json(json!({"s3": "s3://b/k", "filename": "movie.mp4"})).unwrap();
        assert_eq!(
            e.payload,
            IngestPayload::S3 {
                uri: "s3://b/k".into(),
                filename: Some("movie.mp4".into())
            }
        );
        assert_eq!(e.payload.mime().as_deref(), Some("video/mp4"));
        let e = payload_from_json(json!({"s3": "s3://b/k"})).unwrap();
        assert_eq!(e.payload.filename().as_deref(), Some("k"));
        assert_eq!(
            e.payload.mime().as_deref(),
            Some("application/octet-stream")
        );
    }

    #[test]
    fn json_documents_and_bare_array() {
        let e = payload_from_json(json!({"documents": [{"id": "1", "title": "Hello"}]})).unwrap();
        match &e.payload {
            IngestPayload::Documents(d) => {
                assert_eq!(d.len(), 1);
                assert_eq!(d[0].id, "1");
                assert_eq!(d[0].title.as_deref(), Some("Hello"));
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(e.payload.mime().as_deref(), Some("application/json"));
        let e = payload_from_json(json!([{"id": "a"}, {"id": "b"}])).unwrap();
        assert!(matches!(e.payload, IngestPayload::Documents(ref d) if d.len() == 2));
    }

    #[test]
    fn json_items_batch() {
        let e = payload_from_json(json!({"items": [{"url": "https://e.com/a.pdf"}, {"s3": "s3://b/k"}, {"documents": [{"id": "1"}]}], "index": "i"}))
            .unwrap();
        let IngestPayload::Batch(items) = e.payload else {
            panic!()
        };
        assert_eq!(items.len(), 3);
        assert!(matches!(items[0], IngestPayload::Url { .. }));
        assert!(matches!(items[1], IngestPayload::S3 { .. }));
        assert!(matches!(items[2], IngestPayload::Documents(_)));
        assert_eq!(e.index.as_deref(), Some("i"));
    }

    #[test]
    fn json_items_errors() {
        assert!(payload_from_json(json!({"items": "x"})).is_err());
        let err = payload_from_json(json!({"items": [{"url": "u"}, {"nope": 1}]})).unwrap_err();
        assert!(matches!(err, GatewayError::BadRequest(m) if m.starts_with("items[1]")));
        let err = payload_from_json(json!({"items": [3]})).unwrap_err();
        assert!(matches!(err, GatewayError::BadRequest(m) if m.contains("items[0]")));
    }

    #[test]
    fn json_without_known_keys_is_400() {
        assert!(matches!(
            payload_from_json(json!({"index": "only"})),
            Err(GatewayError::BadRequest(_))
        ));
        assert!(matches!(
            payload_from_json(json!(3)),
            Err(GatewayError::BadRequest(_))
        ));
    }

    #[tokio::test]
    async fn extract_json_body_via_content_type() {
        let body = Bytes::from(r#"{"url":"https://e.com/x.pdf"}"#);
        let e = extract_payload(Some("application/json; charset=utf-8"), body, None, None)
            .await
            .unwrap();
        assert!(matches!(e.payload, IngestPayload::Url { .. }));
        let err = extract_payload(Some("application/json"), Bytes::from("{"), None, None)
            .await
            .unwrap_err();
        assert!(matches!(err, GatewayError::BadRequest(_)));
        let err = extract_payload(Some("application/json"), Bytes::new(), None, None)
            .await
            .unwrap_err();
        assert!(matches!(err, GatewayError::BadRequest(_)));
    }

    // --- raw bodies -----------------------------------------------------------------------

    #[tokio::test]
    async fn raw_pdf_body_detects_by_magic_even_with_wrong_content_type() {
        let e = extract_payload(
            Some("text/plain"),
            Bytes::from_static(PDF_MAGIC),
            None,
            Some("x.bin"),
        )
        .await
        .unwrap();
        let IngestPayload::File(blob) = e.payload else {
            panic!()
        };
        assert_eq!(blob.mime, "application/pdf");
        assert_eq!(blob.filename.as_deref(), Some("x.bin"));
        assert_eq!(blob.data, PDF_MAGIC);
    }

    #[tokio::test]
    async fn raw_utf8_body_without_hints_is_text_plain() {
        let e = extract_payload(None, Bytes::from("hello world"), None, None)
            .await
            .unwrap();
        let IngestPayload::File(blob) = e.payload else {
            panic!()
        };
        assert_eq!(blob.mime, "text/plain");
        assert_eq!(blob.filename, None);
    }

    #[tokio::test]
    async fn raw_body_uses_extension_for_text_formats() {
        let e = extract_payload(None, Bytes::from("a,b\n1,2\n"), None, Some("data.csv"))
            .await
            .unwrap();
        let IngestPayload::File(blob) = e.payload else {
            panic!()
        };
        assert_eq!(blob.mime, "text/csv");
    }

    #[tokio::test]
    async fn raw_empty_body_is_400() {
        let err = extract_payload(Some("application/pdf"), Bytes::new(), None, None)
            .await
            .unwrap_err();
        assert!(matches!(err, GatewayError::BadRequest(_)));
    }

    // --- multipart ---------------------------------------------------------------------------

    #[tokio::test]
    async fn multipart_file_with_index_and_pipeline_fields() {
        let b = "XyZ";
        let body = form(
            b,
            vec![
                part(
                    b,
                    "file",
                    Some("report.pdf"),
                    Some("application/octet-stream"),
                    PDF_MAGIC,
                ),
                part(b, "index", None, None, b"contracts"),
                part(b, "pipeline", None, None, b"my-pipe"),
                part(b, "extra", None, None, b"ignored"),
            ],
        );
        let mp = multipart_from(b, body).await;
        let e = extract_payload(
            Some(&format!("multipart/form-data; boundary={b}")),
            Bytes::new(),
            Some(mp),
            None,
        )
        .await
        .unwrap();
        let IngestPayload::File(blob) = e.payload else {
            panic!("{:?}", e.payload)
        };
        assert_eq!(blob.mime, "application/pdf");
        assert_eq!(blob.filename.as_deref(), Some("report.pdf"));
        assert_eq!(e.index.as_deref(), Some("contracts"));
        assert_eq!(e.pipeline.as_deref(), Some("my-pipe"));
    }

    #[tokio::test]
    async fn multipart_filename_field_overrides_part_filename() {
        let b = "XyZ";
        let body = form(
            b,
            vec![
                part(b, "file", Some("blob.bin"), None, b"a,b\n1,2\n"),
                part(b, "filename", None, None, b"data.csv"),
            ],
        );
        let mp = multipart_from(b, body).await;
        let e = extract_payload(None, Bytes::new(), Some(mp), None)
            .await
            .unwrap();
        let IngestPayload::File(blob) = e.payload else {
            panic!()
        };
        assert_eq!(blob.filename.as_deref(), Some("data.csv"));
        assert_eq!(blob.mime, "text/csv");
    }

    #[tokio::test]
    async fn multipart_url_s3_documents_fields() {
        let b = "XyZ";
        let mp = multipart_from(
            b,
            form(b, vec![part(b, "url", None, None, b"https://e.com/a.pdf")]),
        )
        .await;
        let e = extract_payload(None, Bytes::new(), Some(mp), None)
            .await
            .unwrap();
        assert_eq!(
            e.payload,
            IngestPayload::Url {
                url: "https://e.com/a.pdf".into(),
                filename: None
            }
        );

        let mp = multipart_from(
            b,
            form(
                b,
                vec![
                    part(b, "s3", None, None, b"s3://b/k"),
                    part(b, "filename", None, None, b"k.mp4"),
                ],
            ),
        )
        .await;
        let e = extract_payload(None, Bytes::new(), Some(mp), None)
            .await
            .unwrap();
        assert_eq!(
            e.payload,
            IngestPayload::S3 {
                uri: "s3://b/k".into(),
                filename: Some("k.mp4".into())
            }
        );

        let mp = multipart_from(
            b,
            form(
                b,
                vec![part(
                    b,
                    "documents",
                    None,
                    None,
                    br#"[{"id":"1","content":"c"}]"#,
                )],
            ),
        )
        .await;
        let e = extract_payload(None, Bytes::new(), Some(mp), None)
            .await
            .unwrap();
        assert!(
            matches!(e.payload, IngestPayload::Documents(ref d) if d.len() == 1 && d[0].content == "c")
        );

        let mp = multipart_from(
            b,
            form(b, vec![part(b, "documents", None, None, b"not json")]),
        )
        .await;
        assert!(matches!(
            extract_payload(None, Bytes::new(), Some(mp), None).await,
            Err(GatewayError::BadRequest(_))
        ));
    }

    #[tokio::test]
    async fn multipart_without_payload_field_is_400() {
        let b = "XyZ";
        let mp = multipart_from(b, form(b, vec![part(b, "index", None, None, b"only")])).await;
        let err = extract_payload(None, Bytes::new(), Some(mp), None)
            .await
            .unwrap_err();
        assert!(matches!(err, GatewayError::BadRequest(m) if m.contains("`file`")));
    }
}
