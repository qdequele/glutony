//! The `SOURCE_FETCH_HOSTS` policy applied to URL refs in step inputs.
//!
//! An ingest body like `{"url": "http://127.0.0.1:8123/?query=…"}` is fetched by the
//! worker, from inside the deployment, where loopback and private services are
//! reachable. Scheduled sources were already held to `SOURCE_FETCH_HOSTS`; this holds
//! request-driven URL refs to the very same policy, re-checked on every redirect hop
//! by [`FetchGuard`].

use std::sync::Arc;

use meili_ingest_blob::{FetchGuard, UrlCheck};
use meili_ingest_source::HostPolicy;
use url::Url;

/// [`UrlCheck`] over a [`HostPolicy`].
#[derive(Debug, Clone)]
pub struct PolicyCheck(pub HostPolicy);

#[async_trait::async_trait]
impl UrlCheck for PolicyCheck {
    /// The refusal names the variable, not its value beyond what
    /// [`HostPolicy::check_disclosing`] says: the allowed hosts for a standalone job, and
    /// nothing for a tenant's (`list_allowed == false`). The worker logs the policy at
    /// boot for the operator.
    async fn check(&self, url: &Url, list_allowed: bool) -> Result<(), String> {
        self.0
            .check_disclosing(url, list_allowed)
            .await
            .map_err(|e| format!("{e} (URL refs obey SOURCE_FETCH_HOSTS)"))
    }
}

/// The guard the worker installs on its blob store.
pub fn fetch_guard(policy: HostPolicy) -> FetchGuard {
    FetchGuard::new(Arc::new(PolicyCheck(policy)))
}
