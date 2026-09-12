//! `GET /usage` — per-tenant consumption for the dashboard.
//!
//! The browser never talks to Tinybird directly. A read token in front-end code would
//! be extractable by anyone with dev tools, and Tinybird endpoint parameters are
//! caller-supplied, so a tenant could simply ask for another tenant's `project_id`.
//! This handler holds the token server-side and forces `project_id` to the value
//! resolved from the request's own context, which the tenant cannot influence when
//! Envoy is in front.

use std::collections::HashMap;

use axum::Json;
use axum::extract::{Query, State};
use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};

use crate::context::resolve_project_id;
use crate::error::GatewayError;
use crate::state::AppState;

/// Query parameters accepted by [`get_usage`].
#[derive(Debug, Clone, Deserialize)]
pub struct UsageQuery {
    /// First day of the period, inclusive (`YYYY-MM-DD`).
    pub date_from: String,
    /// Last day of the period, inclusive (`YYYY-MM-DD`).
    pub date_to: String,
}

/// One day of a tenant's consumption, as returned by the Tinybird endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageRow {
    /// Day (`YYYY-MM-DD`).
    #[serde(default)]
    pub day: String,
    /// Pipeline the work ran through.
    #[serde(default)]
    pub pipeline_uid: String,
    /// Plugin, empty for job-level rows.
    #[serde(default)]
    pub plugin: String,
    /// Remaining numeric columns, passed through as-is so adding a metric to the
    /// Tinybird pipe does not require a gateway release.
    #[serde(flatten)]
    pub metrics: HashMap<String, serde_json::Value>,
}

/// Response of `GET /usage`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageResponse {
    /// Tenant the rows belong to (empty when self-hosted).
    pub project_id: String,
    /// Daily rows, oldest first.
    pub data: Vec<UsageRow>,
}

/// `GET /usage?date_from=&date_to=`.
///
/// Returns 501 when usage analytics is not configured, so the dashboard can show an
/// honest "not enabled" state instead of an error.
pub async fn get_usage(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<UsageQuery>,
) -> Result<Json<UsageResponse>, GatewayError> {
    let Some(usage) = &state.config.usage_api else {
        return Err(GatewayError::NotImplemented(
            "usage analytics is not configured on this deployment".into(),
        ));
    };
    validate_date(&q.date_from, "date_from")?;
    validate_date(&q.date_to, "date_to")?;
    let project_id = resolve_project_id(&headers, &state.config).unwrap_or_default();

    let url = format!("{}/v0/pipes/{}.json", usage.base_url, usage.pipe);
    let resp = state
        .http
        .get(&url)
        .bearer_auth(&usage.token)
        .query(&[
            ("project_id", project_id.as_str()),
            ("date_from", q.date_from.as_str()),
            ("date_to", q.date_to.as_str()),
        ])
        .send()
        .await
        .map_err(|e| GatewayError::Upstream(format!("usage analytics unreachable: {e}")))?;

    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        // Never echo the token; the body can contain the query but not the header.
        return Err(GatewayError::Upstream(format!(
            "usage analytics returned {status}: {}",
            truncate(&body, 300)
        )));
    }
    let parsed: TinybirdResponse = serde_json::from_str(&body)
        .map_err(|e| GatewayError::Upstream(format!("unexpected usage response: {e}")))?;
    Ok(Json(UsageResponse {
        project_id,
        data: parsed.data,
    }))
}

#[derive(Deserialize)]
struct TinybirdResponse {
    #[serde(default)]
    data: Vec<UsageRow>,
}

/// Reject anything that is not a plain `YYYY-MM-DD` date before it reaches the
/// upstream query string.
fn validate_date(value: &str, field: &str) -> Result<(), GatewayError> {
    let ok = value.len() == 10
        && value.as_bytes().iter().enumerate().all(|(i, b)| match i {
            4 | 7 => *b == b'-',
            _ => b.is_ascii_digit(),
        });
    if ok {
        Ok(())
    } else {
        Err(GatewayError::BadRequest(format!(
            "{field} must be a YYYY-MM-DD date, got {value:?}"
        )))
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_plain_dates_are_accepted() {
        assert!(validate_date("2026-09-01", "date_from").is_ok());
        for bad in [
            "2026-9-1",
            "2026-09-01'",
            "' OR 1=1",
            "2026-09-011",
            "",
            "yyyy-mm-dd",
        ] {
            assert!(
                validate_date(bad, "date_from").is_err(),
                "{bad:?} should be rejected"
            );
        }
    }
}
