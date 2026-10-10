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

/// Everything the control plane reads from the environment.
#[derive(Debug)]
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
