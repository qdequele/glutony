//! The connector abstraction.
//!
//! A connector turns a [`Location`] into zero or more **source items**. An item is a
//! file, not a document: one TMDB export is a single item that later yields ~600k
//! documents, so the workflow's fan-out cap never applies to a source's document count
//! (Decision 7).
//!
//! v1 ships only [`crate::url::UrlConnector`]. [`Resolution::Items`] is a `Vec` from the
//! start so a future bucket connector returning 400 objects needs no change here or in
//! the scheduler.
//!
//! [`ResolvedItem`] carries bytes rather than a staged reference on purpose: staging
//! into the blob store is the worker activity's job, and keeping it out of this crate is
//! what lets every connector test run without an object store.

use chrono::{DateTime, Utc};

use crate::SourceError;
use crate::guard::UrlGuard;
use crate::model::{FetchAuth, IncrementalState, Location};

/// One fetched item, before it is staged into the blob store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedItem {
    /// Decompressed content.
    pub bytes: Vec<u8>,
    /// MIME of the content as it will be routed.
    pub mime: String,
    /// Filename hint, used for MIME detection and provenance.
    pub filename: Option<String>,
}

/// Outcome of resolving a location.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolution {
    /// Upstream is byte-identical to the previous run. No job, no usage.
    Unchanged,
    /// Items to ingest, plus the state to persist for the next run.
    Items {
        /// One entry per source item.
        items: Vec<ResolvedItem>,
        /// Etag / last-modified / hash learned from this fetch.
        state: IncrementalState,
    },
}

/// Ambient services a connector needs.
#[derive(Debug, Clone)]
pub struct ResolveRuntime {
    /// Shared HTTP client.
    pub http: reqwest::Client,
    /// Address policy and caps.
    ///
    /// `None` disables the address check and the size cap — used only by tests against a
    /// loopback mock server, which the guard would otherwise reject. Production always
    /// sets it.
    pub guard: Option<UrlGuard>,
    /// The run's scheduled time, used for URL templating (Decision 9).
    pub scheduled_at: DateTime<Utc>,
    /// IANA timezone the templates render in.
    pub timezone: String,
}

impl ResolveRuntime {
    /// Runtime with the default guard enforced.
    pub fn new(http: reqwest::Client, scheduled_at: DateTime<Utc>, timezone: String) -> Self {
        Self {
            http,
            guard: Some(UrlGuard::default()),
            scheduled_at,
            timezone,
        }
    }
}

/// Turns a location into items.
#[async_trait::async_trait]
pub trait SourceConnector: Send + Sync {
    /// The [`Location`] variant this connector handles, e.g. `"url"`.
    fn kind(&self) -> &'static str;

    /// Resolve `loc`, honouring the previous run's `state`.
    async fn resolve(
        &self,
        loc: &Location,
        auth: Option<&FetchAuth>,
        state: &IncrementalState,
        rt: &ResolveRuntime,
    ) -> Result<Resolution, SourceError>;
}
