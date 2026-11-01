//! `GET /internal/lab/ai/{account_id}`: the tenant's Gateway target for AI steps.
//!
//! Workers hold no Lab credentials (`LAB_INSTANCE_SECRET` signs billing events), so just
//! before an AI step they ask the control plane, which asks the Lab
//! ([`meili_ingest_lab::AiTargetCache`], with the same short-lived caching and
//! stale-on-error rules as the credit check). The answer carries the team's Gateway key,
//! which the worker needs: the route sits behind `CONTROL_PLANE_TOKEN` like every
//! `/internal/*` route, and the key never reaches a log line, an error message or a
//! `Debug` output.

use axum::Json;
use axum::extract::{Path, Query, State};
use meili_ingest_lab::{AiLookup, LabAiTarget};
use serde::{Deserialize, Serialize};

use crate::{AppState, CpError};

/// Query of `GET /internal/lab/ai/{account_id}`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct RejectedKey {
    /// The key id the Gateway just refused: the Lab is asked for a replacement.
    pub rejected_key_id: Option<String>,
}

/// `200` answer of an AI target lookup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AiTargetAnswer {
    /// The account's Gateway target. `null`: nothing to use, because this control plane
    /// has no Lab credentials or the id is not a Lab account id.
    pub target: Option<LabAiTarget>,
}

/// `200 {"target": {...}}`, `200 {"target": null}` when there is no Lab target to
/// serve, `404 no_gateway` when the Lab has no Gateway for the account, `503
/// lab_unavailable` when the Lab cannot answer and no cached target is recent enough.
pub async fn get_ai_target(
    State(state): State<AppState>,
    Path(account_id): Path<String>,
    Query(query): Query<RejectedKey>,
) -> Result<Json<AiTargetAnswer>, CpError> {
    let Some(cache) = state.lab_ai.as_deref() else {
        return Ok(Json(AiTargetAnswer { target: None }));
    };
    match cache
        .get(&account_id, query.rejected_key_id.as_deref())
        .await
    {
        AiLookup::Target(target) => Ok(Json(AiTargetAnswer {
            target: Some(target),
        })),
        AiLookup::NotALabAccount => Ok(Json(AiTargetAnswer { target: None })),
        AiLookup::NoGateway => Err(CpError::NoGateway(format!(
            "account {account_id} has no Gateway"
        ))),
        AiLookup::Unavailable(m) => Err(CpError::LabUnavailable(m)),
    }
}
