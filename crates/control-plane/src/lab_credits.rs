//! `GET /internal/lab/credits/{account_id}`: the credit pre-check (spec v2 §8.1) for
//! the workers.
//!
//! Scheduled source runs start from Temporal, not through the gateway, so the worker
//! asks before fetching anything. Workers hold no Lab credentials (`LAB_INSTANCE_SECRET`
//! signs billing events), so the control plane asks the Lab for them, with the same
//! cache rules as the gateway ([`meili_ingest_lab::AccountCreditCache`]).

use axum::Json;
use axum::extract::{Path, State};
use meili_ingest_lab::CreditDecision;
use serde::{Deserialize, Serialize};

use crate::{AppState, CpError};

/// `200` answer of a credit check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreditCheck {
    /// `true`: the Lab account can spend. `false`: nothing was checked, because this
    /// control plane has no Lab credentials or the id is not a Lab account id.
    pub checked: bool,
}

/// `200 {"checked": true|false}` when the work may start, `402 insufficient_credits`
/// when the account cannot spend, `503 lab_unavailable` when the Lab cannot answer and
/// no cached lookup is recent enough.
pub async fn check_credits(
    State(state): State<AppState>,
    Path(account_id): Path<String>,
) -> Result<Json<CreditCheck>, CpError> {
    let Some(cache) = state.lab_credits.as_deref() else {
        return Ok(Json(CreditCheck { checked: false }));
    };
    match cache.check(&account_id).await {
        CreditDecision::Allowed => Ok(Json(CreditCheck { checked: true })),
        CreditDecision::NotALabAccount => Ok(Json(CreditCheck { checked: false })),
        CreditDecision::NoCredits(m) => Err(CpError::InsufficientCredits(m)),
        CreditDecision::Unavailable(m) => Err(CpError::LabUnavailable(m)),
    }
}
