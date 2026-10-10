//! The control plane's configuration, read and validated as a whole before anything
//! touches Postgres. A configuration the binary would refuse must be refused before
//! migrations run: a migration applied by a binary that then exits leaves the database
//! ahead of the image still running (sqlx refuses to start on a migration it does not
//! know).

use std::net::SocketAddr;

use anyhow::Context;

use crate::lab_sender::LabConfig;

/// Default listen address (SPEC §13).
pub const DEFAULT_BIND: &str = "0.0.0.0:9000";

/// Everything the control plane reads from the environment. Its `Debug` redacts the
/// database password and the control-plane token (the Lab secret is redacted by
/// [`LabConfig`]'s own).
pub struct BootConfig {
    /// `DATABASE_URL`.
    pub database_url: String,
    /// `BIND`.
    pub addr: SocketAddr,
    /// `CONTROL_PLANE_TOKEN`, or `None` when explicitly disabled.
    pub internal_token: Option<String>,
    /// `LAB_URL` + `LAB_INSTANCE_*`, when the deployment reports to a Lab.
    pub lab: Option<LabConfig>,
}

impl std::fmt::Debug for BootConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let database_url = match url::Url::parse(&self.database_url) {
            Ok(mut u) => {
                if u.password().is_some() {
                    // Only fails for URLs that cannot carry credentials at all.
                    let _ = u.set_password(Some("redacted"));
                }
                u.to_string()
            }
            Err(_) => "<unparseable, redacted>".to_string(),
        };
        f.debug_struct("BootConfig")
            .field("database_url", &database_url)
            .field("addr", &self.addr)
            .field(
                "internal_token",
                &self.internal_token.as_ref().map(|_| "<redacted>"),
            )
            .field("lab", &self.lab)
            .finish()
    }
}

impl BootConfig {
    /// Validate every variable from `get` (`None` = unset).
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> anyhow::Result<Self> {
        let database_url =
            get("DATABASE_URL").context("DATABASE_URL environment variable is required")?;
        let bind = get("BIND").unwrap_or_else(|| DEFAULT_BIND.to_string());
        let addr: SocketAddr = bind
            .parse()
            .with_context(|| format!("BIND {bind:?} is not a valid socket address"))?;
        let internal_token = crate::control_plane_token_policy(
            get("CONTROL_PLANE_TOKEN"),
            get("CONTROL_PLANE_TOKEN_DISABLED")
                .map(|v| v.trim().eq_ignore_ascii_case("true") || v.trim() == "1")
                .unwrap_or(false),
        )?;
        let nonblank = |n: &str| get(n).filter(|v| !v.trim().is_empty());
        let lab = LabConfig::from_values(
            nonblank("LAB_URL"),
            nonblank("LAB_INSTANCE_ID"),
            nonblank("LAB_INSTANCE_SECRET"),
            nonblank("LAB_EVENTS_SECRET"),
        )
        .context("invalid Lab events configuration")?;
        Ok(Self {
            database_url,
            addr,
            internal_token,
            lab,
        })
    }

    /// [`BootConfig::from_lookup`] over the process environment.
    pub fn from_env() -> anyhow::Result<Self> {
        Self::from_lookup(|n| std::env::var(n).ok())
    }
}

/// Ask the Lab who this deployment is (spec §3.6), before anything touches Postgres.
/// A `401` (wrong or revoked credentials) and credentials of another product's engine
/// abort boot; any other failure is logged and boot goes on: events are sent anyway
/// and retried.
pub async fn confirm_lab_identity(config: &LabConfig) -> anyhow::Result<()> {
    // Bounded like the sender's own client: a hung Lab must not block boot.
    let http = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(2))
        .timeout(std::time::Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("cannot build the Lab HTTP client")?;
    match meili_ingest_lab::fetch_instance_info(&http, config.credentials()).await {
        Ok(info) => {
            // Credentials of another product's engine: its events would be
            // attributed to that product.
            meili_ingest_lab::check_product(&info)?;
            tracing::info!(
                kind = ?info.kind,
                product = %info.product,
                region = ?info.region,
                "Lab instance identity confirmed"
            );
        }
        Err(meili_ingest_lab::LabError::Unauthorized) => anyhow::bail!(
            "the Lab rejected LAB_INSTANCE_ID / LAB_INSTANCE_SECRET (401); fix the credentials"
        ),
        Err(e) => tracing::warn!(
            error = %e,
            "could not confirm this deployment's Lab identity; events are sent anyway and retried"
        ),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn boot(vars: &[(&str, &str)]) -> anyhow::Result<BootConfig> {
        let env: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        BootConfig::from_lookup(|n| env.get(n).cloned())
    }

    #[test]
    fn a_full_configuration_is_read() {
        let c = boot(&[
            ("DATABASE_URL", "postgres://x"),
            ("CONTROL_PLANE_TOKEN", "t"),
            ("LAB_URL", "https://lab.example"),
            ("LAB_INSTANCE_ID", "id"),
            ("LAB_INSTANCE_SECRET", "s"),
            ("LAB_EVENTS_SECRET", " "),
        ])
        .unwrap();
        assert_eq!(c.addr.to_string(), DEFAULT_BIND);
        assert_eq!(c.internal_token.as_deref(), Some("t"));
        assert_eq!(c.lab.unwrap().credentials().instance_id(), "id");
    }

    async fn identity(resp: wiremock::ResponseTemplate) -> anyhow::Result<()> {
        let lab = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::path("/internal/instances/me"))
            .respond_with(resp)
            .mount(&lab)
            .await;
        let config =
            LabConfig::from_values(Some(lab.uri()), Some("id".into()), Some("s".into()), None)
                .unwrap()
                .unwrap();
        confirm_lab_identity(&config).await
    }

    #[tokio::test]
    async fn the_lab_identity_aborts_boot_only_on_401_or_another_product() {
        let me = |product: &str| {
            wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "instance_id": "id", "kind": "hosted", "product": product
            }))
        };
        identity(me("glutony")).await.unwrap();
        let err = identity(me("lumen")).await.unwrap_err().to_string();
        assert!(err.contains("lumen"), "{err}");
        let err = identity(wiremock::ResponseTemplate::new(401))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("LAB_INSTANCE_SECRET"), "{err}");
        // Anything else only warns.
        identity(wiremock::ResponseTemplate::new(503))
            .await
            .unwrap();
        identity(wiremock::ResponseTemplate::new(200).set_body_string("nope"))
            .await
            .unwrap();
    }

    #[test]
    fn debug_redacts_the_secrets() {
        let c = boot(&[
            (
                "DATABASE_URL",
                "postgres://glutony:db-pa55word@db.internal:5432/glutony",
            ),
            ("CONTROL_PLANE_TOKEN", "cp-t0ken-value"),
            ("LAB_URL", "https://lab.example"),
            ("LAB_INSTANCE_ID", "inst-id"),
            ("LAB_INSTANCE_SECRET", "lab-s3cret-value"),
        ])
        .unwrap();
        let dbg = format!("{c:?}");
        for secret in ["db-pa55word", "cp-t0ken-value", "lab-s3cret-value"] {
            assert!(!dbg.contains(secret), "{secret} leaked: {dbg}");
        }
        // Still useful: where it connects and which Lab instance it is.
        assert!(dbg.contains("db.internal"), "{dbg}");
        assert!(dbg.contains("inst-id"), "{dbg}");
    }

    #[test]
    fn every_refusal_happens_here() {
        let base = [
            ("DATABASE_URL", "postgres://x"),
            ("CONTROL_PLANE_TOKEN", "t"),
        ];
        let with = |extra: &[(&'static str, &'static str)]| {
            let mut v = base.to_vec();
            v.extend_from_slice(extra);
            boot(&v).unwrap_err().to_string()
        };
        assert!(boot(&[("CONTROL_PLANE_TOKEN", "t")]).is_err());
        assert!(boot(&[("DATABASE_URL", "postgres://x")]).is_err());
        assert!(with(&[("BIND", "nope")]).contains("BIND"));
        assert!(with(&[("LAB_EVENTS_SECRET", "old")]).contains("Lab"));
        assert!(with(&[("LAB_URL", "https://lab.example")]).contains("Lab"));
    }
}
