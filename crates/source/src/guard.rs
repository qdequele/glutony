//! SSRF guard for tenant-supplied URLs.
//!
//! A source's URL is fetched from inside the cluster, where the cloud metadata endpoint
//! (`169.254.169.254`) and every internal service (`meili-control-plane:9000`,
//! `temporal-frontend:7233`) are reachable. Everything here runs *before* a socket is
//! opened, and again after each redirect — an allowed host redirecting to `127.0.0.1`
//! is the obvious bypass.
//!
//! Known residual risk, not solved in v1: DNS rebinding between this check and the
//! actual connect. Closing it means pinning the resolved address into the connector.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, ToSocketAddrs};

use url::Url;

use crate::SourceError;

/// What kind of address a host resolved to. Only [`AddressClass::Public`] may be fetched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddressClass {
    /// Routable on the public internet.
    Public,
    /// `127.0.0.0/8`, `::1`.
    Loopback,
    /// RFC1918 `10/8`, `172.16/12`, `192.168/16`.
    Private,
    /// `169.254/16`, `fe80::/10` — includes the cloud metadata endpoint.
    LinkLocal,
    /// Carrier-grade NAT `100.64/10`.
    Cgnat,
    /// IPv6 unique local `fc00::/7`.
    UniqueLocal,
    /// `0.0.0.0`, `::`.
    Unspecified,
    /// Multicast and broadcast ranges.
    Multicast,
}

impl AddressClass {
    /// Whether a source may fetch from an address of this class.
    pub fn is_allowed(self) -> bool {
        matches!(self, AddressClass::Public)
    }
}

/// Classify one address.
///
/// IPv4-mapped IPv6 addresses are unwrapped first, so `::ffff:127.0.0.1` classifies as
/// loopback rather than public.
pub fn classify(ip: IpAddr) -> AddressClass {
    match ip {
        IpAddr::V4(v4) => classify_v4(v4),
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => classify_v4(v4),
            None => classify_v6(v6),
        },
    }
}

fn classify_v4(ip: Ipv4Addr) -> AddressClass {
    let [a, b, ..] = ip.octets();
    if ip.is_unspecified() {
        AddressClass::Unspecified
    } else if ip.is_loopback() {
        AddressClass::Loopback
    } else if ip.is_link_local() {
        AddressClass::LinkLocal
    } else if a == 100 && (64..128).contains(&b) {
        AddressClass::Cgnat
    } else if ip.is_private() {
        AddressClass::Private
    } else if ip.is_multicast() || ip.is_broadcast() {
        AddressClass::Multicast
    } else {
        AddressClass::Public
    }
}

fn classify_v6(ip: Ipv6Addr) -> AddressClass {
    let first = ip.segments()[0];
    if ip.is_unspecified() {
        AddressClass::Unspecified
    } else if ip.is_loopback() {
        AddressClass::Loopback
    } else if (first & 0xffc0) == 0xfe80 {
        AddressClass::LinkLocal
    } else if (first & 0xfe00) == 0xfc00 {
        AddressClass::UniqueLocal
    } else if ip.is_multicast() {
        AddressClass::Multicast
    } else {
        AddressClass::Public
    }
}

/// Reject any scheme but `https`.
pub fn check_scheme(url: &Url) -> Result<(), SourceError> {
    match url.scheme() {
        "https" => Ok(()),
        other => Err(SourceError::Blocked(format!(
            "scheme {other:?} is not allowed; sources must use https"
        ))),
    }
}

/// Policy applied to every source fetch.
#[derive(Debug, Clone, Copy)]
pub struct UrlGuard {
    /// Maximum redirects followed before giving up.
    pub max_redirects: usize,
    /// Maximum bytes read from a response body.
    pub max_bytes: u64,
}

impl Default for UrlGuard {
    fn default() -> Self {
        Self {
            max_redirects: 5,
            max_bytes: 512 * 1024 * 1024,
        }
    }
}

impl UrlGuard {
    /// Check scheme, then resolve the host and reject unless **every** resolved address
    /// is public.
    ///
    /// Checking only the first address would let a host publishing one public and one
    /// loopback record through, which is a standard SSRF trick.
    pub async fn check_url(&self, url: &Url) -> Result<(), SourceError> {
        check_scheme(url)?;
        let host = url
            .host_str()
            .ok_or_else(|| SourceError::Blocked("url has no host".into()))?
            .to_owned();
        let port = url.port_or_known_default().unwrap_or(443);
        let shown = url.clone();

        // `ToSocketAddrs` blocks; keep it off the async worker threads.
        let resolved = tokio::task::spawn_blocking(move || {
            (host.as_str(), port)
                .to_socket_addrs()
                .map(|it| it.map(|s| s.ip()).collect::<Vec<_>>())
        })
        .await
        .map_err(|e| SourceError::Dns(format!("resolver task failed: {e}")))?
        .map_err(|e| SourceError::Dns(format!("could not resolve {shown}: {e}")))?;

        if resolved.is_empty() {
            return Err(SourceError::Dns(format!(
                "{shown} resolved to no addresses"
            )));
        }
        for ip in resolved {
            let class = classify(ip);
            if !class.is_allowed() {
                return Err(SourceError::Blocked(format!(
                    "{shown} resolves to {ip}, which is {class:?}; \
                     only public addresses may be fetched"
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::IpAddr;
    use url::Url;

    fn ip(s: &str) -> IpAddr {
        s.parse().expect("parses")
    }

    #[test]
    fn classifies_addresses() {
        // Public.
        assert_eq!(classify(ip("1.1.1.1")), AddressClass::Public);
        assert_eq!(classify(ip("2606:4700::1111")), AddressClass::Public);
        // Loopback.
        assert_eq!(classify(ip("127.0.0.1")), AddressClass::Loopback);
        assert_eq!(classify(ip("::1")), AddressClass::Loopback);
        // RFC1918 private.
        assert_eq!(classify(ip("10.0.0.5")), AddressClass::Private);
        assert_eq!(classify(ip("172.16.0.1")), AddressClass::Private);
        assert_eq!(classify(ip("192.168.1.1")), AddressClass::Private);
        // Link-local — this is the cloud metadata endpoint.
        assert_eq!(classify(ip("169.254.169.254")), AddressClass::LinkLocal);
        assert_eq!(classify(ip("fe80::1")), AddressClass::LinkLocal);
        // Carrier-grade NAT.
        assert_eq!(classify(ip("100.64.0.1")), AddressClass::Cgnat);
        // IPv6 unique local.
        assert_eq!(classify(ip("fc00::1")), AddressClass::UniqueLocal);
        assert_eq!(classify(ip("fd12:3456::1")), AddressClass::UniqueLocal);
        // Unspecified and multicast.
        assert_eq!(classify(ip("0.0.0.0")), AddressClass::Unspecified);
        assert_eq!(classify(ip("224.0.0.1")), AddressClass::Multicast);
    }

    #[test]
    fn ipv4_mapped_loopback_is_not_mistaken_for_public() {
        assert_eq!(classify(ip("::ffff:127.0.0.1")), AddressClass::Loopback);
        assert_eq!(
            classify(ip("::ffff:169.254.169.254")),
            AddressClass::LinkLocal
        );
    }

    #[test]
    fn only_public_addresses_are_allowed() {
        assert!(AddressClass::Public.is_allowed());
        for c in [
            AddressClass::Loopback,
            AddressClass::Private,
            AddressClass::LinkLocal,
            AddressClass::Cgnat,
            AddressClass::UniqueLocal,
            AddressClass::Unspecified,
            AddressClass::Multicast,
        ] {
            assert!(!c.is_allowed(), "{c:?} must be rejected");
        }
    }

    #[test]
    fn rejects_non_https_schemes() {
        assert!(check_scheme(&Url::parse("https://example.test/a").expect("url")).is_ok());
        for bad in [
            "http://example.test/a",
            "file:///etc/passwd",
            "ftp://example.test/a",
            "gopher://example.test/a",
        ] {
            let url = Url::parse(bad).expect("url");
            assert!(check_scheme(&url).is_err(), "{bad} must be rejected");
        }
    }

    #[tokio::test]
    async fn rejects_a_literal_private_host() {
        let guard = UrlGuard::default();
        for bad in [
            "https://127.0.0.1/x",
            "https://169.254.169.254/latest/meta-data/",
            "https://10.0.0.1/x",
            "https://[::1]/x",
        ] {
            let url = Url::parse(bad).expect("url");
            assert!(
                guard.check_url(&url).await.is_err(),
                "{bad} must be rejected"
            );
        }
    }

    #[tokio::test]
    async fn rejects_a_plain_http_url_before_resolving() {
        let guard = UrlGuard::default();
        let url = Url::parse("http://example.test/x").expect("url");
        let err = guard.check_url(&url).await.expect_err("must reject");
        assert!(matches!(err, SourceError::Blocked(_)));
    }

    #[test]
    fn default_guard_caps_redirects_and_size() {
        let g = UrlGuard::default();
        assert_eq!(g.max_redirects, 5);
        assert_eq!(g.max_bytes, 512 * 1024 * 1024);
    }
}
