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
use std::time::Duration;

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
    /// Special-purpose blocks that are not routable on the public internet:
    /// `0.0.0.0/8`, `192.0.0.0/24`, the documentation and benchmarking ranges,
    /// `240.0.0.0/4`, and their IPv6 counterparts.
    Reserved,
}

impl AddressClass {
    /// Whether a source may fetch from an address of this class.
    pub fn is_allowed(self) -> bool {
        matches!(self, AddressClass::Public)
    }
}

/// Classify one address.
///
/// An IPv6 address that carries an IPv4 one is classified as that IPv4 address, so
/// `::ffff:127.0.0.1` is loopback and the NAT64 form of `10.0.0.5` (`64:ff9b::a00:5`)
/// is private rather than public. Anything not known to be routable on the public
/// internet is refused: a range missing from here is reachable, not blocked.
pub fn classify(ip: IpAddr) -> AddressClass {
    match ip {
        IpAddr::V4(v4) => classify_v4(v4),
        IpAddr::V6(v6) => classify_v6(v6),
    }
}

fn classify_v4(ip: Ipv4Addr) -> AddressClass {
    let [a, b, c, _] = ip.octets();
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
    } else if is_reserved_v4(a, b, c) {
        AddressClass::Reserved
    } else {
        AddressClass::Public
    }
}

/// IPv4 special-purpose blocks (RFC 6890) that no public host lives in.
fn is_reserved_v4(a: u8, b: u8, c: u8) -> bool {
    match (a, b, c) {
        // 0.0.0.0/8, "this network".
        (0, ..) => true,
        // 192.0.0.0/24 protocol assignments, and TEST-NET-1.
        (192, 0, 0 | 2) => true,
        // 198.18.0.0/15, benchmarking.
        (198, 18 | 19, _) => true,
        // TEST-NET-2 and TEST-NET-3.
        (198, 51, 100) | (203, 0, 113) => true,
        // 240.0.0.0/4, reserved.
        (240.., ..) => true,
        _ => false,
    }
}

/// IPv6 special-purpose blocks that no public host lives in.
fn is_reserved_v6(s: [u16; 8]) -> bool {
    // 64:ff9b:1::/48, local-use NAT64.
    s[..3] == [0x64, 0xff9b, 1]
        // 2001::/32 Teredo, and 2001:db8::/32 documentation.
        || s[..2] == [0x2001, 0]
        || s[..2] == [0x2001, 0xdb8]
        // 100::/64, discard-only.
        || s[..4] == [0x100, 0, 0, 0]
}

fn classify_v6(ip: Ipv6Addr) -> AddressClass {
    let s = ip.segments();
    let first = s[0];
    let v4 = |hi: u16, lo: u16| Ipv4Addr::from((u32::from(hi) << 16) | u32::from(lo));
    if ip.is_unspecified() {
        AddressClass::Unspecified
    } else if ip.is_loopback() {
        AddressClass::Loopback
    } else if let Some(mapped) = ip.to_ipv4_mapped() {
        classify_v4(mapped)
    } else if s[..6] == [0x64, 0xff9b, 0, 0, 0, 0] {
        // NAT64 (RFC 6052). Unwrapped rather than refused: in a DNS64 cluster every
        // IPv4-only public site resolves into this prefix.
        classify_v4(v4(s[6], s[7]))
    } else if first == 0x2002 {
        // 6to4: the IPv4 address of the relay it is tunnelled to.
        classify_v4(v4(s[1], s[2]))
    } else if s[..6] == [0; 6] {
        // IPv4-compatible `::a.b.c.d`, deprecated and never routed as IPv6.
        match classify_v4(v4(s[6], s[7])) {
            AddressClass::Public => AddressClass::Reserved,
            other => other,
        }
    } else if (first & 0xffc0) == 0xfe80 {
        AddressClass::LinkLocal
    } else if (first & 0xfe00) == 0xfc00 {
        AddressClass::UniqueLocal
    } else if ip.is_multicast() {
        AddressClass::Multicast
    } else if is_reserved_v6(s) {
        AddressClass::Reserved
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
    /// Deadline for one whole fetch: every redirect hop, the response headers and the
    /// body. It has to stay below the start-to-close timeout of the activity that
    /// fetches, which must still stage what was downloaded.
    pub total_timeout: Duration,
    /// Longest wait for the next piece of a response body. A download that keeps
    /// moving is bounded by `total_timeout` only; one that stalls is given up early.
    pub idle_timeout: Duration,
}

impl Default for UrlGuard {
    fn default() -> Self {
        Self {
            max_redirects: 5,
            max_bytes: 512 * 1024 * 1024,
            total_timeout: Duration::from_secs(10 * 60),
            idle_timeout: Duration::from_secs(60),
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
        let shown = url.clone();
        let resolved = match url.host() {
            None => return Err(SourceError::Blocked("url has no host".into())),
            // A literal address is classified as it is. Handing it to the resolver
            // would fail for IPv6, which `url` renders in brackets.
            Some(url::Host::Ipv4(ip)) => vec![IpAddr::V4(ip)],
            Some(url::Host::Ipv6(ip)) => vec![IpAddr::V6(ip)],
            Some(url::Host::Domain(domain)) => {
                let host = domain.to_owned();
                let port = url.port_or_known_default().unwrap_or(443);
                // `ToSocketAddrs` blocks; keep it off the async worker threads.
                tokio::task::spawn_blocking(move || {
                    (host.as_str(), port)
                        .to_socket_addrs()
                        .map(|it| it.map(|s| s.ip()).collect::<Vec<_>>())
                })
                .await
                .map_err(|e| SourceError::Dns(format!("resolver task failed: {e}")))?
                .map_err(|e| SourceError::Dns(format!("could not resolve {shown}: {e}")))?
            }
        };

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
    fn special_purpose_ipv4_blocks_are_not_public() {
        for reserved in [
            "0.1.2.3",         // 0.0.0.0/8 "this network"
            "192.0.0.1",       // IETF protocol assignments
            "192.0.2.1",       // TEST-NET-1
            "198.18.0.10",     // benchmarking 198.18.0.0/15
            "198.19.255.255",  //
            "198.51.100.7",    // TEST-NET-2
            "203.0.113.7",     // TEST-NET-3
            "240.0.0.1",       // reserved 240.0.0.0/4
            "254.255.255.254", //
        ] {
            assert_eq!(classify(ip(reserved)), AddressClass::Reserved, "{reserved}");
        }
        // The neighbours of those blocks stay public.
        for public in [
            "1.0.0.1",
            "192.0.1.1",
            "192.0.3.1",
            "198.17.255.255",
            "198.20.0.1",
            "198.51.101.1",
            "203.0.112.1",
            "203.0.114.1",
            "223.255.255.255",
        ] {
            assert_eq!(classify(ip(public)), AddressClass::Public, "{public}");
        }
    }

    #[test]
    fn an_ipv4_address_embedded_in_ipv6_is_classified_as_that_ipv4() {
        // NAT64, 6to4 and IPv4-compatible forms of 10.0.0.5 and of the metadata endpoint.
        assert_eq!(classify(ip("64:ff9b::a00:5")), AddressClass::Private);
        assert_eq!(classify(ip("64:ff9b::a9fe:a9fe")), AddressClass::LinkLocal);
        assert_eq!(classify(ip("2002:0a00:0005::1")), AddressClass::Private);
        assert_eq!(classify(ip("2002:7f00:0001::")), AddressClass::Loopback);
        assert_eq!(classify(ip("::10.0.0.5")), AddressClass::Private);
        assert_eq!(classify(ip("::127.0.0.1")), AddressClass::Loopback);
        assert_eq!(classify(ip("64:ff9b::c612:a")), AddressClass::Reserved);
        // In a DNS64 cluster every IPv4-only site resolves into 64:ff9b::/96: a public
        // IPv4 behind it must stay fetchable.
        assert_eq!(classify(ip("64:ff9b::101:101")), AddressClass::Public);
        assert_eq!(classify(ip("2002:0101:0101::1")), AddressClass::Public);
    }

    #[test]
    fn special_purpose_ipv6_blocks_are_not_public() {
        for reserved in [
            "64:ff9b:1::a00:5", // local-use NAT64
            "2001:db8::1",      // documentation
            "2001::1",          // Teredo
            "100::1",           // discard-only
        ] {
            assert_eq!(classify(ip(reserved)), AddressClass::Reserved, "{reserved}");
        }
        assert_eq!(classify(ip("2001:4860:4860::8888")), AddressClass::Public);
        assert_eq!(classify(ip("2a00:1450::1")), AddressClass::Public);
    }

    #[tokio::test]
    async fn an_ipv6_literal_is_classified_not_sent_to_the_resolver() {
        let guard = UrlGuard::default();
        for bad in [
            "https://[::1]/x",
            "https://[fd00::1]/x",
            "https://[64:ff9b::a00:5]/x",
            "https://[::ffff:169.254.169.254]/x",
        ] {
            let url = Url::parse(bad).expect("url");
            let err = guard.check_url(&url).await.expect_err(bad);
            assert!(matches!(err, SourceError::Blocked(_)), "{bad}: {err:?}");
        }
        let public = Url::parse("https://[2606:4700::1111]/x").expect("url");
        guard
            .check_url(&public)
            .await
            .expect("a public IPv6 literal is allowed");
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
            AddressClass::Reserved,
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
    fn default_guard_caps_redirects_size_and_time() {
        let g = UrlGuard::default();
        assert_eq!(g.max_redirects, 5);
        assert_eq!(g.max_bytes, 512 * 1024 * 1024);
        assert_eq!(g.total_timeout, Duration::from_secs(600));
        assert_eq!(g.idle_timeout, Duration::from_secs(60));
    }
}
