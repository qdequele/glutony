//! Which hosts a Meilisearch connection may point at (spec Decision 14).
//!
//! A connection's host is a tenant-supplied URL that meili-ingest writes documents to
//! from inside the cluster — the fetch-side SSRF exposure, on the write side. The safe
//! default is public addresses only, but self-hosted users routinely run Meilisearch on
//! a private address (the dev stack itself writes to `http://meilisearch:7700`), so the
//! policy is one line of deployment config rather than hard-coded.
//!
//! | `MEILI_CONNECTION_HOSTS` | Accepts |
//! |---|---|
//! | unset / `public` | `https`, every resolved address public ([`UrlGuard`]) |
//! | `any` | any `http`/`https` host |
//! | `meilisearch:7700,meili.internal` | exactly those hosts, `http` or `https` |
//!
//! An allowlist entry without a port matches any port on that host: listing a host is
//! already the statement that it is trusted.

use url::Url;

use crate::SourceError;
use crate::guard::UrlGuard;

/// Name of the environment variable holding the policy.
pub const HOSTS_ENV: &str = "MEILI_CONNECTION_HOSTS";

/// One allowlist entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostPort {
    /// Lowercased host as `url` renders it (IPv6 in brackets).
    pub host: String,
    /// Port; `None` matches any port.
    pub port: Option<u16>,
}

/// Connection host policy.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum HostPolicy {
    /// `https` and public addresses only. The default, safe for multi-tenant.
    #[default]
    Public,
    /// Any `http` or `https` host.
    Any,
    /// Exactly these hosts.
    Allow(Vec<HostPort>),
}

impl HostPolicy {
    /// Parse a policy string. An empty string means the default, [`HostPolicy::Public`].
    pub fn parse(raw: &str) -> Result<Self, SourceError> {
        let raw = raw.trim();
        match raw.to_ascii_lowercase().as_str() {
            "" | "public" => return Ok(HostPolicy::Public),
            "any" => return Ok(HostPolicy::Any),
            _ => {}
        }
        let mut entries = Vec::new();
        for entry in raw.split(',') {
            entries.push(parse_entry(entry.trim())?);
        }
        Ok(HostPolicy::Allow(entries))
    }

    /// Read [`HOSTS_ENV`]; unset means [`HostPolicy::Public`].
    pub fn from_env() -> Result<Self, SourceError> {
        match std::env::var(HOSTS_ENV) {
            Ok(raw) => Self::parse(&raw),
            Err(_) => Ok(HostPolicy::Public),
        }
    }

    /// Accept or reject a connection URL.
    ///
    /// Called when a connection is saved **and** again before each use, because DNS can
    /// change between the two.
    pub async fn check(&self, url: &Url) -> Result<(), SourceError> {
        match self {
            HostPolicy::Public => UrlGuard::default().check_url(url).await,
            HostPolicy::Any => check_http_scheme(url),
            HostPolicy::Allow(entries) => {
                check_http_scheme(url)?;
                let host = url
                    .host_str()
                    .ok_or_else(|| SourceError::Blocked("url has no host".into()))?
                    .to_ascii_lowercase();
                let port = url.port_or_known_default();
                let allowed = entries
                    .iter()
                    .any(|e| e.host == host && e.port.is_none_or(|p| Some(p) == port));
                if allowed {
                    Ok(())
                } else {
                    Err(SourceError::Blocked(format!(
                        "{url} is not in {HOSTS_ENV}; allowed: {}",
                        entries
                            .iter()
                            .map(|e| match e.port {
                                Some(p) => format!("{}:{p}", e.host),
                                None => e.host.clone(),
                            })
                            .collect::<Vec<_>>()
                            .join(", ")
                    )))
                }
            }
        }
    }
}

fn check_http_scheme(url: &Url) -> Result<(), SourceError> {
    match url.scheme() {
        "http" | "https" => Ok(()),
        other => Err(SourceError::Blocked(format!(
            "scheme {other:?} is not allowed for a Meilisearch connection"
        ))),
    }
}

/// Parse one `host[:port]` entry by letting `url` do the hard parts (IPv6 brackets,
/// port range, invalid characters) rather than splitting on `:` by hand.
fn parse_entry(entry: &str) -> Result<HostPort, SourceError> {
    let bad = |why: &str| {
        SourceError::Blocked(format!(
            "invalid {HOSTS_ENV} entry {entry:?}: {why}; expected host[:port], \
             comma-separated, or `public` / `any`"
        ))
    };
    if entry.is_empty() {
        return Err(bad("empty entry"));
    }
    if entry.contains("://") || entry.contains('/') || entry.contains('@') {
        return Err(bad("give a bare host, not a url"));
    }
    let url = Url::parse(&format!("http://{entry}")).map_err(|e| bad(&e.to_string()))?;
    let host = url
        .host_str()
        .filter(|h| !h.is_empty())
        .ok_or_else(|| bad("no host"))?
        .to_ascii_lowercase();
    Ok(HostPort {
        host,
        port: url.port(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(s: &str) -> Url {
        Url::parse(s).expect("valid url")
    }

    #[test]
    fn empty_and_public_mean_the_default() {
        assert_eq!(HostPolicy::parse("").expect("parses"), HostPolicy::Public);
        assert_eq!(HostPolicy::parse("  ").expect("parses"), HostPolicy::Public);
        assert_eq!(
            HostPolicy::parse("PUBLIC").expect("parses"),
            HostPolicy::Public
        );
        assert_eq!(HostPolicy::default(), HostPolicy::Public);
    }

    #[test]
    fn any_parses_case_insensitively() {
        assert_eq!(HostPolicy::parse("Any").expect("parses"), HostPolicy::Any);
    }

    #[test]
    fn an_allowlist_parses_hosts_and_optional_ports() {
        let p = HostPolicy::parse("meilisearch:7700, Meili.Internal ,[::1]:7700").expect("parses");
        assert_eq!(
            p,
            HostPolicy::Allow(vec![
                HostPort {
                    host: "meilisearch".into(),
                    port: Some(7700)
                },
                HostPort {
                    host: "meili.internal".into(),
                    port: None
                },
                HostPort {
                    host: "[::1]".into(),
                    port: Some(7700)
                },
            ])
        );
    }

    #[test]
    fn malformed_allowlists_are_rejected() {
        for bad in [
            "a,,b",
            "http://meilisearch:7700",
            "meilisearch:7700/path",
            "user@meilisearch",
            "meilisearch:99999",
            "meilisearch:notaport",
        ] {
            assert!(HostPolicy::parse(bad).is_err(), "{bad:?} must be rejected");
        }
    }

    #[tokio::test]
    async fn public_rejects_the_dev_stack_and_private_addresses() {
        let p = HostPolicy::Public;
        // The dev compose's own Meilisearch: http, and a private address.
        assert!(p.check(&url("http://meilisearch:7700")).await.is_err());
        assert!(p.check(&url("https://127.0.0.1:7700")).await.is_err());
        assert!(p.check(&url("https://169.254.169.254")).await.is_err());
    }

    #[tokio::test]
    async fn any_accepts_both_schemes_but_nothing_else() {
        let p = HostPolicy::Any;
        assert!(p.check(&url("http://meilisearch:7700")).await.is_ok());
        assert!(p.check(&url("https://127.0.0.1:7700")).await.is_ok());
        assert!(p.check(&url("ftp://meilisearch:7700")).await.is_err());
    }

    #[tokio::test]
    async fn an_allowlist_accepts_exactly_its_entries() {
        let p = HostPolicy::parse("meilisearch:7700,meili.internal").expect("parses");
        assert!(p.check(&url("http://meilisearch:7700")).await.is_ok());
        assert!(
            p.check(&url("https://MEILISEARCH:7700")).await.is_ok(),
            "case-insensitive"
        );
        assert!(
            p.check(&url("http://meilisearch:7701")).await.is_err(),
            "a pinned port rejects any other port"
        );
        assert!(
            p.check(&url("https://meili.internal:9999")).await.is_ok(),
            "a portless entry matches any port"
        );
        assert!(p.check(&url("http://127.0.0.1:7700")).await.is_err());
        assert!(
            p.check(&url("http://meilisearch.evil.test:7700"))
                .await
                .is_err()
        );
        assert!(p.check(&url("ftp://meilisearch:7700")).await.is_err());
    }

    #[tokio::test]
    async fn the_rejection_message_lists_what_is_allowed() {
        let p = HostPolicy::parse("meilisearch:7700").expect("parses");
        let err = p
            .check(&url("http://other:7700"))
            .await
            .expect_err("rejected")
            .to_string();
        assert!(err.contains("meilisearch:7700"), "{err}");
        assert!(err.contains(HOSTS_ENV), "{err}");
    }
}
