//! Self-hosted ClickHouse as the usage store, instead of Tinybird.
//!
//! Tinybird is managed ClickHouse, so the rows, the engine and the deduplication are
//! the same: the workers append the [`UsageEvent`](crate::UsageEvent) NDJSON lines
//! with `INSERT … FORMAT JSONEachRow`, into a `ReplacingMergeTree` with the same
//! columns, sorting key and TTL as `tinybird/datasources/meili_ingest_usage.datasource`.
//! The table is created on first use ([`ClickHouseConfig::create_table_sql`]), so a
//! deployment only provides a database and a user.
//!
//! The read side ([`ClickHouseConfig::daily_usage`], the gateway's `GET /usage`) does
//! what Tinybird's hourly copy and `tenant_usage` endpoint do together, in one query
//! over the raw table with `FINAL`: rows are deduplicated before they are counted, and
//! there is no hourly lag. At one deployment's volume that scan is cheap; it only reads
//! the tenant's own granules (the sorting key leads with `project_id`).
//!
//! | var | default | notes |
//! |---|---|---|
//! | `USAGE_CLICKHOUSE_URL` | — | HTTP interface, e.g. `http://127.0.0.1:8123`. Absent ⇒ not this backend. |
//! | `USAGE_CLICKHOUSE_DATABASE` | `default` | Must exist. |
//! | `USAGE_CLICKHOUSE_TABLE` | `meili_ingest_usage` | Created if missing. |
//! | `USAGE_CLICKHOUSE_USER` | `default` | Workers: `INSERT`, `SELECT`, `CREATE TABLE`. Gateway: `SELECT`. |
//! | `USAGE_CLICKHOUSE_PASSWORD` | empty | |
//! | `USAGE_CLICKHOUSE_TIMEOUT_SECS` | `20` | Worker HTTP timeout. |

use std::fmt;

use crate::{MAX_ERROR_CHARS, UsageError, truncate_chars};

/// Env var holding the ClickHouse HTTP URL. Present ⇒ ClickHouse is the usage store.
pub const ENV_URL: &str = "USAGE_CLICKHOUSE_URL";
/// Env var naming the database (must already exist).
pub const ENV_DATABASE: &str = "USAGE_CLICKHOUSE_DATABASE";
/// Env var naming the table (created if missing).
pub const ENV_TABLE: &str = "USAGE_CLICKHOUSE_TABLE";
/// Env var naming the ClickHouse user.
pub const ENV_USER: &str = "USAGE_CLICKHOUSE_USER";
/// Env var holding the ClickHouse user's password.
pub const ENV_PASSWORD: &str = "USAGE_CLICKHOUSE_PASSWORD";
/// Env var overriding the worker's HTTP timeout, in seconds.
pub const ENV_TIMEOUT_SECS: &str = "USAGE_CLICKHOUSE_TIMEOUT_SECS";

/// Default database.
pub const DEFAULT_DATABASE: &str = "default";
/// Default table, named like the Tinybird data source.
pub const DEFAULT_TABLE: &str = crate::DEFAULT_DATASOURCE;
/// Default user.
pub const DEFAULT_USER: &str = "default";

/// Columns of the usage table, in [`UsageEvent`](crate::UsageEvent) field order. Same
/// names and types as the Tinybird data source; a test keeps the three in step.
pub const COLUMNS: &[(&str, &str)] = &[
    ("event_id", "String"),
    ("kind", "LowCardinality(String)"),
    ("ts", "DateTime64(3)"),
    ("job_id", "String"),
    ("workflow_id", "String"),
    ("project_id", "LowCardinality(String)"),
    ("region", "LowCardinality(String)"),
    ("pipeline_uid", "LowCardinality(String)"),
    ("pipeline_builtin", "UInt8"),
    ("index_name", "String"),
    ("step_id", "String"),
    ("plugin", "LowCardinality(String)"),
    ("task_queue", "LowCardinality(String)"),
    ("status", "LowCardinality(String)"),
    ("attempt", "UInt32"),
    ("branches", "UInt32"),
    ("duration_ms", "UInt64"),
    ("input_bytes", "UInt64"),
    ("input_mime", "LowCardinality(String)"),
    ("documents_out", "UInt64"),
    ("llm_input_tokens", "UInt64"),
    ("llm_output_tokens", "UInt64"),
    ("llm_requests", "UInt64"),
    ("audio_seconds", "Float64"),
    ("pages", "UInt64"),
    ("images", "UInt64"),
    ("external_requests", "UInt64"),
    ("error_kind", "LowCardinality(String)"),
    ("error", "String"),
];

/// ClickHouse exception codes that mean "these credentials cannot do this": unknown
/// user, wrong or missing password, failed authentication, access denied.
const AUTH_CODES: &[u32] = &[192, 193, 194, 497, 516];

/// Codes for a request a retry cannot fix: bad data or schema drift (parse errors,
/// unknown column, type mismatch), an unknown database, a syntax error.
const PERMANENT_CODES: &[u32] = &[6, 16, 26, 27, 38, 41, 47, 53, 62, 69, 72, 81, 117];

/// `UNKNOWN_TABLE`: no row was ever written (the table is created on first insert).
pub const UNKNOWN_TABLE: u32 = 60;

/// Where usage lives in a self-hosted ClickHouse, and how to log in.
#[derive(Clone, PartialEq, Eq)]
pub struct ClickHouseConfig {
    url: String,
    database: String,
    table: String,
    user: String,
    password: Option<String>,
}

/// Redacts the password: the config is printed in startup logs.
impl fmt::Debug for ClickHouseConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClickHouseConfig")
            .field("url", &self.url)
            .field("database", &self.database)
            .field("table", &self.table)
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

/// A failed ClickHouse call: the HTTP status and the `X-ClickHouse-Exception-Code`.
#[derive(Debug, Clone)]
pub struct ClickHouseFailure {
    /// HTTP status (0 for a transport failure).
    pub status: u16,
    /// ClickHouse exception code, when the server sent one.
    pub code: Option<u32>,
    /// Truncated response body, or the transport error.
    pub body: String,
}

impl ClickHouseFailure {
    /// The table does not exist yet.
    pub fn is_unknown_table(&self) -> bool {
        self.code == Some(UNKNOWN_TABLE)
    }
}

impl fmt::Display for ClickHouseFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.status, self.code) {
            (0, _) => write!(f, "ClickHouse unreachable: {}", self.body),
            (s, Some(c)) => write!(f, "ClickHouse returned HTTP {s} (code {c}): {}", self.body),
            (s, None) => write!(f, "ClickHouse returned HTTP {s}: {}", self.body),
        }
    }
}

impl From<ClickHouseFailure> for UsageError {
    /// Same retryability split as the Tinybird client: credentials and bad requests are
    /// permanent, everything else (overload, a restarting server, network) is retried.
    fn from(f: ClickHouseFailure) -> Self {
        let code = f.code.unwrap_or(0);
        if f.status == 0 {
            UsageError::Transport(f.body)
        } else if f.status == 401 || f.status == 403 || AUTH_CODES.contains(&code) {
            UsageError::Auth { status: f.status }
        } else if PERMANENT_CODES.contains(&code)
            || f.is_unknown_table()
            || ((400..500).contains(&f.status) && f.status != 429)
        {
            UsageError::Rejected {
                status: f.status,
                body: f.to_string(),
            }
        } else {
            UsageError::Upstream {
                status: f.status,
                body: f.to_string(),
            }
        }
    }
}

impl ClickHouseConfig {
    /// Read the `USAGE_CLICKHOUSE_*` variables. `Ok(None)` when `USAGE_CLICKHOUSE_URL`
    /// is unset; [`UsageError::Config`] when a database or table name is not a plain
    /// identifier (they are spliced into SQL, so anything else is refused).
    pub fn from_env() -> Result<Option<Self>, UsageError> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    pub(crate) fn from_lookup(
        get: impl Fn(&str) -> Option<String>,
    ) -> Result<Option<Self>, UsageError> {
        let non_empty = |key: &str| {
            get(key)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };
        let Some(url) = non_empty(ENV_URL) else {
            return Ok(None);
        };
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return Err(UsageError::Config(format!(
                "{ENV_URL} must be an http(s) URL to ClickHouse's HTTP interface"
            )));
        }
        let database = non_empty(ENV_DATABASE).unwrap_or_else(|| DEFAULT_DATABASE.into());
        let table = non_empty(ENV_TABLE).unwrap_or_else(|| DEFAULT_TABLE.into());
        for (var, value) in [(ENV_DATABASE, &database), (ENV_TABLE, &table)] {
            if !is_identifier(value) {
                return Err(UsageError::Config(format!(
                    "{var} must be letters, digits and underscores, got {value:?}"
                )));
            }
        }
        Ok(Some(Self::new(
            url,
            database,
            table,
            non_empty(ENV_USER).unwrap_or_else(|| DEFAULT_USER.into()),
            // Not trimmed-to-nothing: an all-space password is still "no password".
            non_empty(ENV_PASSWORD),
        )))
    }

    /// Build a config explicitly. `database` and `table` must be plain identifiers.
    ///
    /// # Panics
    ///
    /// When `database` or `table` is not `[A-Za-z_][A-Za-z0-9_]*`.
    pub fn new(
        url: impl Into<String>,
        database: impl Into<String>,
        table: impl Into<String>,
        user: impl Into<String>,
        password: Option<String>,
    ) -> Self {
        let (database, table) = (database.into(), table.into());
        assert!(
            is_identifier(&database),
            "invalid database name {database:?}"
        );
        assert!(is_identifier(&table), "invalid table name {table:?}");
        Self {
            url: url.into().trim_end_matches('/').to_string(),
            database,
            table,
            user: user.into(),
            password,
        }
    }

    /// HTTP interface URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Fully qualified table, `` `db`.`table` ``.
    pub fn qualified_table(&self) -> String {
        format!("`{}`.`{}`", self.database, self.table)
    }

    /// Table name.
    pub fn table(&self) -> &str {
        &self.table
    }

    /// `CREATE TABLE IF NOT EXISTS` for the usage table: the Tinybird data source's
    /// engine, sorting key, partitioning and 180-day TTL (see its header for why).
    pub fn create_table_sql(&self) -> String {
        let columns = COLUMNS
            .iter()
            .map(|(name, ty)| format!("    `{name}` {ty}"))
            .collect::<Vec<_>>()
            .join(",\n");
        format!(
            "CREATE TABLE IF NOT EXISTS {}\n(\n{columns}\n)\n\
             ENGINE = ReplacingMergeTree\n\
             PARTITION BY toYYYYMM(ts)\n\
             ORDER BY (project_id, ts, event_id)\n\
             TTL toDateTime(ts) + INTERVAL 180 DAY",
            self.qualified_table()
        )
    }

    /// The `GET /usage` query: one row per (day, pipeline_uid, plugin) for one tenant,
    /// with the columns of Tinybird's `tenant_usage` endpoint. `FINAL` collapses
    /// redelivered rows before they are counted. Tenant and dates are bound as typed
    /// query parameters (`param_*`), never spliced into the SQL.
    pub fn daily_usage_sql(&self) -> String {
        // Raw columns are qualified with `u.`: the output aliases reuse their names,
        // and an unqualified `documents_out` would resolve to the `sumIf(...)` alias.
        format!(
            "SELECT
    toDate(u.ts) AS day,
    u.pipeline_uid AS pipeline_uid,
    u.plugin AS plugin,
    countIf(u.kind = 'step') AS steps,
    countIf(u.kind = 'job') AS jobs,
    countIf(u.kind = 'job' AND u.status = 'succeeded') AS jobs_succeeded,
    countIf(u.kind = 'job' AND u.status = 'failed') AS jobs_failed,
    sumIf(u.documents_out, u.kind = 'step') AS documents_out,
    sumIf(u.documents_out, u.kind = 'job') AS documents_indexed,
    sumIf(u.input_bytes, u.kind = 'step') AS input_bytes,
    sumIf(u.duration_ms, u.kind = 'step') AS duration_ms,
    sumIf(u.llm_input_tokens, u.kind = 'step') AS llm_input_tokens,
    sumIf(u.llm_output_tokens, u.kind = 'step') AS llm_output_tokens,
    sumIf(u.llm_requests, u.kind = 'step') AS llm_requests,
    sumIf(u.audio_seconds, u.kind = 'step') AS audio_seconds,
    sumIf(u.pages, u.kind = 'step') AS pages,
    sumIf(u.images, u.kind = 'step') AS images,
    sumIf(u.external_requests, u.kind = 'step') AS external_requests
FROM {} AS u FINAL
WHERE u.project_id = {{project_id:String}}
  AND toDate(u.ts) >= {{date_from:Date}}
  AND toDate(u.ts) <= {{date_to:Date}}
GROUP BY day, pipeline_uid, plugin
ORDER BY day ASC, pipeline_uid ASC, plugin ASC
FORMAT JSON",
            self.qualified_table()
        )
    }

    /// POST `sql` (or, with `body`, `query=sql` and the body as data) to ClickHouse.
    pub(crate) async fn post(
        &self,
        http: &reqwest::Client,
        sql: &str,
        body: Option<String>,
        params: &[(&str, &str)],
    ) -> Result<String, ClickHouseFailure> {
        let mut req = http
            .post(format!("{}/", self.url))
            .header("X-ClickHouse-User", &self.user);
        if let Some(password) = &self.password {
            req = req.header("X-ClickHouse-Key", password);
        }
        let mut query: Vec<(&str, &str)> = vec![("database", self.database.as_str())];
        query.extend_from_slice(params);
        req = match body {
            Some(data) => {
                query.push(("query", sql));
                req.body(data)
            }
            None => req.body(sql.to_string()),
        };
        let resp = req
            .query(&query)
            .send()
            .await
            .map_err(|e| ClickHouseFailure {
                status: 0,
                code: None,
                // reqwest errors carry the URL, never the headers: no password here.
                body: e.to_string(),
            })?;
        let status = resp.status().as_u16();
        let code = resp
            .headers()
            .get("X-ClickHouse-Exception-Code")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse().ok());
        let text = resp.text().await.map_err(|e| ClickHouseFailure {
            status: 0,
            code: None,
            body: format!("response body unreadable: {e}"),
        })?;
        if (200..300).contains(&status) && code.is_none() {
            Ok(text)
        } else {
            Err(ClickHouseFailure {
                status,
                code,
                body: truncate_chars(text.trim(), MAX_ERROR_CHARS),
            })
        }
    }

    /// Create the usage table if it is missing.
    pub async fn ensure_table(&self, http: &reqwest::Client) -> Result<(), ClickHouseFailure> {
        self.post(http, &self.create_table_sql(), None, &[])
            .await
            .map(|_| ())
    }

    /// Insert NDJSON usage rows. Strict on purpose: an unknown field fails the insert
    /// (the struct and the table have drifted) instead of being dropped silently.
    pub(crate) async fn insert(
        &self,
        http: &reqwest::Client,
        ndjson: String,
    ) -> Result<(), ClickHouseFailure> {
        let sql = format!("INSERT INTO {} FORMAT JSONEachRow", self.qualified_table());
        self.post(
            http,
            &sql,
            Some(ndjson),
            &[
                // `ts` is RFC 3339 with a `Z`, which the default parser refuses.
                ("date_time_input_format", "best_effort"),
                ("input_format_skip_unknown_fields", "0"),
            ],
        )
        .await
        .map(|_| ())
    }

    /// Daily usage rows of one tenant between two `YYYY-MM-DD` dates, inclusive, as JSON
    /// objects shaped like Tinybird's `tenant_usage` rows. An empty list when no usage
    /// was ever recorded (the table does not exist yet).
    ///
    /// The caller validates the dates; they are bound as `Date` parameters anyway, so a
    /// malformed one is a ClickHouse error, not an injection.
    pub async fn daily_usage(
        &self,
        http: &reqwest::Client,
        tenant_id: &str,
        date_from: &str,
        date_to: &str,
    ) -> Result<Vec<serde_json::Value>, ClickHouseFailure> {
        let text = match self
            .post(
                http,
                &self.daily_usage_sql(),
                None,
                &[
                    ("param_project_id", tenant_id),
                    ("param_date_from", date_from),
                    ("param_date_to", date_to),
                    // Counters are UInt64; keep them JSON numbers, as Tinybird does.
                    ("output_format_json_quote_64bit_integers", "0"),
                ],
            )
            .await
        {
            Ok(text) => text,
            Err(f) if f.is_unknown_table() => return Ok(Vec::new()),
            Err(f) => return Err(f),
        };
        #[derive(serde::Deserialize)]
        struct JsonFormat {
            #[serde(default)]
            data: Vec<serde_json::Value>,
        }
        serde_json::from_str::<JsonFormat>(&text)
            .map(|r| r.data)
            .map_err(|e| ClickHouseFailure {
                status: 200,
                code: None,
                body: format!("unexpected ClickHouse response: {e}"),
            })
    }
}

/// `[A-Za-z_][A-Za-z0-9_]*`: safe to splice into SQL between backticks.
fn is_identifier(s: &str) -> bool {
    let mut chars = s.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_string_contains, header, method, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn config(server: &MockServer) -> ClickHouseConfig {
        ClickHouseConfig::new(
            server.uri(),
            "glutony",
            "meili_ingest_usage",
            "glutony",
            Some("ch-secret".into()),
        )
    }

    #[test]
    fn columns_match_the_event_struct_and_the_tinybird_datasource() {
        // The event struct: the keys it serializes, in order.
        let event = crate::events_for_job(&crate::JobUsageInput::default())
            .into_iter()
            .next()
            .expect("a job row");
        let json = serde_json::to_value(&event).expect("serializes");
        let keys: Vec<&str> = json
            .as_object()
            .expect("object")
            .keys()
            .map(String::as_str)
            .collect();
        let mut names: Vec<&str> = COLUMNS.iter().map(|(n, _)| *n).collect();
        let mut sorted_keys = keys.clone();
        sorted_keys.sort_unstable();
        names.sort_unstable();
        assert_eq!(names, sorted_keys, "COLUMNS and UsageEvent drifted");

        // The Tinybird data source: same names, same types, same order.
        let datasource =
            include_str!("../../../tinybird/datasources/meili_ingest_usage.datasource");
        let tinybird: Vec<(String, String)> = datasource
            .lines()
            .map(str::trim)
            .filter(|l| l.starts_with('`'))
            .map(|l| {
                let name = l.trim_start_matches('`').split('`').next().unwrap_or("");
                let rest = l.splitn(3, '`').nth(2).unwrap_or("").trim();
                let ty = rest.split(" `json:").next().unwrap_or("").trim();
                (name.to_string(), ty.to_string())
            })
            .collect();
        let ours: Vec<(String, String)> = COLUMNS
            .iter()
            .map(|(n, t)| (n.to_string(), t.to_string()))
            .collect();
        assert_eq!(
            ours, tinybird,
            "COLUMNS and the Tinybird data source drifted"
        );
    }

    #[test]
    fn create_table_mirrors_the_tinybird_engine() {
        let sql = ClickHouseConfig::new("http://ch:8123", "glutony", "usage", "u", None)
            .create_table_sql();
        assert!(sql.starts_with("CREATE TABLE IF NOT EXISTS `glutony`.`usage`"));
        assert!(sql.contains("ENGINE = ReplacingMergeTree"));
        assert!(sql.contains("ORDER BY (project_id, ts, event_id)"));
        assert!(sql.contains("PARTITION BY toYYYYMM(ts)"));
        assert!(sql.contains("INTERVAL 180 DAY"));
        assert!(sql.contains("`ts` DateTime64(3)"));
    }

    #[test]
    fn from_env_is_off_without_a_url_and_refuses_unsafe_names() {
        assert!(
            ClickHouseConfig::from_lookup(|_| None)
                .expect("no error")
                .is_none()
        );
        let c = ClickHouseConfig::from_lookup(|k| (k == ENV_URL).then(|| "http://ch:8123/".into()))
            .expect("no error")
            .expect("configured");
        assert_eq!(c.url(), "http://ch:8123");
        assert_eq!(c.qualified_table(), "`default`.`meili_ingest_usage`");

        for (var, bad) in [(ENV_TABLE, "usage; DROP TABLE x"), (ENV_DATABASE, "a`b")] {
            let err = ClickHouseConfig::from_lookup(|k| match k {
                ENV_URL => Some("http://ch:8123".into()),
                k if k == var => Some(bad.into()),
                _ => None,
            })
            .expect_err("unsafe identifier");
            assert!(matches!(err, UsageError::Config(_)), "{err:?}");
        }
        let err = ClickHouseConfig::from_lookup(|k| (k == ENV_URL).then(|| "ch:8123".into()))
            .expect_err("no scheme");
        assert!(matches!(err, UsageError::Config(_)));
    }

    #[test]
    fn debug_redacts_the_password() {
        let c = ClickHouseConfig::new("http://ch:8123", "d", "t", "u", Some("hunter2".into()));
        let printed = format!("{c:?}");
        assert!(!printed.contains("hunter2"), "{printed}");
        assert!(printed.contains("<redacted>"));
    }

    #[test]
    fn failures_map_to_the_right_retryability() {
        let f = |status, code: Option<u32>| {
            UsageError::from(ClickHouseFailure {
                status,
                code,
                body: "x".into(),
            })
        };
        assert!(matches!(f(403, Some(516)), UsageError::Auth { .. }));
        assert!(matches!(f(500, Some(497)), UsageError::Auth { .. }));
        assert!(!f(400, Some(27)).is_retryable(), "a parse error is drift");
        assert!(!f(500, Some(16)).is_retryable(), "unknown column is drift");
        assert!(!f(404, Some(UNKNOWN_TABLE)).is_retryable());
        assert!(f(503, None).is_retryable());
        assert!(f(500, Some(241)).is_retryable(), "memory limit: try again");
        assert!(f(429, None).is_retryable());
        assert!(f(0, None).is_retryable(), "transport");
    }

    #[test]
    fn the_client_picks_one_store_from_the_environment() {
        use crate::{ENV_TOKEN, UsageClient};
        let ch = UsageClient::from_lookup(|k| (k == ENV_URL).then(|| "http://ch:8123".into()))
            .expect("no error")
            .expect("configured");
        assert_eq!(ch.backend(), "clickhouse");
        assert_eq!(ch.datasource(), DEFAULT_TABLE);
        let tb = UsageClient::from_lookup(|k| (k == ENV_TOKEN).then(|| "tok".into()))
            .expect("no error")
            .expect("configured");
        assert_eq!(tb.backend(), "tinybird");
        let err = UsageClient::from_lookup(|k| match k {
            ENV_TOKEN => Some("tok".into()),
            ENV_URL => Some("http://ch:8123".into()),
            _ => None,
        })
        .expect_err("two stores would split the data");
        assert!(matches!(err, UsageError::Config(_)), "{err:?}");
    }

    #[tokio::test]
    async fn send_creates_the_table_once_then_inserts_strict_ndjson() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_string_contains(
                "CREATE TABLE IF NOT EXISTS `glutony`.`meili_ingest_usage`",
            ))
            .and(header("X-ClickHouse-Key", "ch-secret"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(query_param(
                "query",
                "INSERT INTO `glutony`.`meili_ingest_usage` FORMAT JSONEachRow",
            ))
            .and(query_param("date_time_input_format", "best_effort"))
            .and(query_param("input_format_skip_unknown_fields", "0"))
            .and(body_string_contains("\"event_id\""))
            .respond_with(ResponseTemplate::new(200))
            .expect(2)
            .mount(&server)
            .await;
        let client = crate::UsageClient::clickhouse(config(&server), reqwest::Client::new());
        let events = crate::events_for_job(&crate::JobUsageInput::default());
        client.send(&events).await.expect("first send");
        client
            .clone()
            .send(&events)
            .await
            .expect("second send, no CREATE");
    }

    #[tokio::test]
    async fn a_store_that_refuses_the_table_fails_the_send_and_retries_creation() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(500)
                    .insert_header("X-ClickHouse-Exception-Code", "497")
                    .set_body_string("Code: 497. Not enough privileges"),
            )
            .expect(2)
            .mount(&server)
            .await;
        let client = crate::UsageClient::clickhouse(config(&server), reqwest::Client::new());
        let events = crate::events_for_job(&crate::JobUsageInput::default());
        let err = client.send(&events).await.expect_err("no grant");
        assert!(matches!(err, UsageError::Auth { status: 500 }), "{err:?}");
        assert!(!err.to_string().contains("ch-secret"), "{err}");
        // Not cached as ready: the next send tries the CREATE again.
        client.send(&events).await.expect_err("still no grant");
    }

    #[tokio::test]
    async fn daily_usage_binds_parameters_and_reads_json_numbers() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(query_param("database", "glutony"))
            .and(query_param("param_project_id", "acme' OR 1=1"))
            .and(query_param("param_date_from", "2026-11-01"))
            .and(query_param("param_date_to", "2026-11-06"))
            .and(query_param("output_format_json_quote_64bit_integers", "0"))
            .and(header("X-ClickHouse-User", "glutony"))
            .and(header("X-ClickHouse-Key", "ch-secret"))
            .and(body_string_contains(
                "FROM `glutony`.`meili_ingest_usage` AS u FINAL",
            ))
            .and(body_string_contains("u.project_id = {project_id:String}"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "meta": [], "rows": 1,
                "data": [{"day": "2026-11-02", "pipeline_uid": "builtin.pdf", "plugin": "",
                          "jobs": 3, "documents_indexed": 42}]
            })))
            .expect(1)
            .mount(&server)
            .await;
        let rows = config(&server)
            .daily_usage(
                &reqwest::Client::new(),
                "acme' OR 1=1",
                "2026-11-01",
                "2026-11-06",
            )
            .await
            .expect("rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["jobs"], 3);
    }

    #[tokio::test]
    async fn daily_usage_is_empty_before_the_table_exists() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(404)
                    .insert_header("X-ClickHouse-Exception-Code", "60")
                    .set_body_string("Code: 60. DB::Exception: Unknown table"),
            )
            .mount(&server)
            .await;
        let rows = config(&server)
            .daily_usage(&reqwest::Client::new(), "acme", "2026-11-01", "2026-11-06")
            .await
            .expect("no table is no usage");
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn an_exception_after_a_200_is_still_a_failure() {
        // ClickHouse can start a 200 response and then report an exception: the
        // header is what tells.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("X-ClickHouse-Exception-Code", "241")
                    .set_body_string("Code: 241. Memory limit exceeded"),
            )
            .mount(&server)
            .await;
        let err = config(&server)
            .daily_usage(&reqwest::Client::new(), "acme", "2026-11-01", "2026-11-06")
            .await
            .expect_err("exception code");
        assert_eq!(err.code, Some(241));
    }
}

/// Round trip against a real ClickHouse. Skipped unless `USAGE_CLICKHOUSE_TEST_URL`
/// points at one (`docker run -p 8123:8123 clickhouse/clickhouse-server`), the way the
/// control plane's DB tests skip without `DATABASE_URL`. Uses a throwaway table.
#[cfg(test)]
mod live {
    use super::*;
    use chrono::TimeZone;
    use meili_ingest_plugin_sdk::{JobStatus, StepResult, UsageUnits};

    #[tokio::test]
    async fn rows_round_trip_and_redeliveries_are_counted_once() {
        let Ok(url) = std::env::var("USAGE_CLICKHOUSE_TEST_URL") else {
            eprintln!("USAGE_CLICKHOUSE_TEST_URL unset: skipping the live ClickHouse test");
            return;
        };
        let table = format!("usage_test_{}", uuid::Uuid::new_v4().simple());
        let user = std::env::var("USAGE_CLICKHOUSE_TEST_USER").unwrap_or_else(|_| "default".into());
        let password = std::env::var("USAGE_CLICKHOUSE_TEST_PASSWORD").ok();
        let database =
            std::env::var("USAGE_CLICKHOUSE_TEST_DATABASE").unwrap_or_else(|_| "default".into());
        let config = ClickHouseConfig::new(url, database, table.clone(), user, password);
        let http = reqwest::Client::new();
        let client = crate::UsageClient::clickhouse(config.clone(), http.clone());

        let ts = |s| {
            chrono::Utc
                .with_ymd_and_hms(2026, 11, 2, 10, 0, s)
                .single()
                .unwrap()
        };
        let step = |id: &str, plugin: &str, usage| StepResult {
            step_id: id.into(),
            plugin: plugin.into(),
            status: JobStatus::Succeeded,
            document_count: 5,
            branches: 1,
            error: None,
            duration_ms: 100,
            input_bytes: 2_000,
            usage,
        };
        let input = crate::JobUsageInput {
            job_id: uuid::Uuid::new_v4(),
            workflow_id: "wf".into(),
            pipeline_uid: "builtin.pdf".into(),
            pipeline_builtin: true,
            tenant_id: "acme".into(),
            region: "fr-par".into(),
            index_name: "docs".into(),
            task_queue: "workers-general".into(),
            status: JobStatus::Succeeded,
            started_at: ts(0),
            finished_at: ts(9),
            input_bytes: 2_000,
            input_mime: "application/pdf".into(),
            steps: vec![
                step("extract", "pdf_extractor", UsageUnits::pages(7)),
                step("enrich", "llm_enricher", UsageUnits::llm(400, 90)),
            ],
            error: None,
        };
        let events = crate::events_for_job(&input);
        client.send(&events).await.expect("insert");
        // At-least-once delivery: the same rows again must not double the counts.
        client.send(&events).await.expect("redelivery");

        let rows = config
            .daily_usage(&http, "acme", "2026-11-01", "2026-11-30")
            .await
            .expect("query");
        let other = config
            .daily_usage(&http, "someone-else", "2026-11-01", "2026-11-30")
            .await
            .expect("query");
        let _ = config
            .post(
                &http,
                &format!("DROP TABLE {}", config.qualified_table()),
                None,
                &[],
            )
            .await;

        assert!(other.is_empty(), "another tenant sees nothing: {other:?}");
        let job = rows
            .iter()
            .find(|r| r["plugin"] == "")
            .expect("job-level row");
        assert_eq!(job["day"], "2026-11-02");
        assert_eq!(job["jobs"], 1, "deduplicated: {rows:?}");
        assert_eq!(job["jobs_succeeded"], 1);
        assert_eq!(job["documents_indexed"], 5);
        let pdf = rows
            .iter()
            .find(|r| r["plugin"] == "pdf_extractor")
            .expect("step row");
        assert_eq!(pdf["steps"], 1, "deduplicated: {rows:?}");
        assert_eq!(pdf["pages"], 7);
        assert_eq!(pdf["input_bytes"], 2_000);
        let llm = rows
            .iter()
            .find(|r| r["plugin"] == "llm_enricher")
            .expect("llm row");
        assert_eq!(llm["llm_input_tokens"], 400);
    }
}
