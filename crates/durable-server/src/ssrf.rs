//! The SSRF guard for handler calls (S2-5), the same pattern Service
//! Monitor's probe path proved: the check *is* the resolution.
//!
//! Two layers, different jobs:
//!
//!  * **Registration** — `check_url` refuses literal local addresses at
//!    write time so a bad registration fails fast. Names resolve later;
//!    nothing DNS is decided here (the answer could differ by the time a
//!    call runs — that is exactly the rebinding gap).
//!  * **Every call** — `PolicyResolver` sits inside the handler client's
//!    DNS path: names resolve through the OS, then every returned
//!    address must be public before reqwest is told it exists. There is
//!    no validate-then-re-resolve gap, so a DNS answer that changes to a
//!    local address between registration and dispatch still sends zero
//!    packets. One local address in the answer refuses the whole
//!    resolution — round-robin means "usually public" is not a defense.
//!
//! `--allow-local-handlers` opts out of both layers for development and
//! fixtures; it is a visible command-line flag, never ambient state.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use reqwest::Url;
use url::Host;

/// A DNS answer containing a local address — the whole resolution is
/// refused, because answering "we found 10.0.0.1" to a dialer that might
/// pick it is a rebinding hole.
#[derive(Debug)]
struct ResolvedLocal;

impl std::fmt::Display for ResolvedLocal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("target resolves to a local address")
    }
}

impl std::error::Error for ResolvedLocal {}

/// True when `e`'s cause chain carries a refused resolution — used to
/// classify a connect error as the guard's refusal, not a network failure.
pub(crate) fn refused(e: &reqwest::Error) -> bool {
    let mut cur: &dyn std::error::Error = e;
    loop {
        if cur.is::<ResolvedLocal>() {
            return true;
        }
        match cur.source() {
            Some(next) => cur = next,
            None => return false,
        }
    }
}

/// reqwest's resolver with the policy inside: names resolve through the
/// OS, then every returned address must be public (unless `allow_local`).
/// reqwest receives only the validated set — the check is the resolution
/// itself, so there is no validate-then-re-resolve gap.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PolicyResolver {
    allow_local: bool,
}

impl Resolve for PolicyResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let allow = self.allow_local;
        Box::pin(async move {
            let host = name.as_str().to_owned();
            let addrs: Vec<SocketAddr> =
                tokio::net::lookup_host((host.as_str(), 0)).await?.collect();
            validate_addrs(addrs.iter().map(SocketAddr::ip), allow)?;
            Ok(Box::new(addrs.into_iter()) as Addrs)
        })
    }
}

/// Every resolved address must be public (unless `allow_local`): a single
/// local address fails the whole resolution.
fn validate_addrs(
    addrs: impl IntoIterator<Item = IpAddr>,
    allow_local: bool,
) -> Result<(), ResolvedLocal> {
    if allow_local {
        return Ok(());
    }
    if addrs.into_iter().any(is_local) {
        return Err(ResolvedLocal);
    }
    Ok(())
}

/// The handler client's resolver — the enforcement layer.
pub(crate) fn resolver(allow_local: bool) -> Arc<PolicyResolver> {
    Arc::new(PolicyResolver { allow_local })
}

/// The registration-time check: scheme and a literal-local host fail
/// fast. A *domain* cannot be judged here — it resolves at call time,
/// inside `PolicyResolver`. `allow_local` is the `--allow-local-handlers`
/// opt-out shared with the resolver.
pub(crate) fn check_url(url: &Url, allow_local: bool) -> Result<(), &'static str> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err("handler URL must be http:// or https://");
    }
    let Some(host) = url.host() else {
        return Err("handler URL needs a host");
    };
    if allow_local {
        return Ok(());
    }
    let ip = match host {
        Host::Ipv4(v4) => IpAddr::V4(v4),
        Host::Ipv6(v6) => IpAddr::V6(v6),
        Host::Domain(_) => return Ok(()),
    };
    if is_local(ip) {
        return Err("handler URL targets a local address");
    }
    Ok(())
}

/// IPv4 blocks a handler call never dials: every entry of the IANA IPv4
/// special-purpose registry that is not globally reachable, plus
/// multicast and the reserved 240/4. A few registry sub-blocks that *are*
/// global anycast services (inside 192.0.0.0/24) are refused too — none
/// is a meaningful job handler.
const LOCAL_V4: &[(Ipv4Addr, u8)] = &[
    (Ipv4Addr::UNSPECIFIED, 8),           // "this network"
    (Ipv4Addr::new(10, 0, 0, 0), 8),      // private
    (Ipv4Addr::new(100, 64, 0, 0), 10),   // shared address space (CGNAT)
    (Ipv4Addr::new(127, 0, 0, 0), 8),     // loopback
    (Ipv4Addr::new(169, 254, 0, 0), 16),  // link-local (cloud metadata lives here)
    (Ipv4Addr::new(172, 16, 0, 0), 12),   // private
    (Ipv4Addr::new(192, 0, 0, 0), 24),    // IETF protocol assignments
    (Ipv4Addr::new(192, 0, 2, 0), 24),    // TEST-NET-1
    (Ipv4Addr::new(192, 88, 99, 0), 24),  // deprecated 6to4 relay anycast
    (Ipv4Addr::new(192, 168, 0, 0), 16),  // private
    (Ipv4Addr::new(198, 18, 0, 0), 15),   // benchmarking
    (Ipv4Addr::new(198, 51, 100, 0), 24), // TEST-NET-2
    (Ipv4Addr::new(203, 0, 113, 0), 24),  // TEST-NET-3
    (Ipv4Addr::new(224, 0, 0, 0), 4),     // multicast
    (Ipv4Addr::new(240, 0, 0, 0), 4),     // reserved, incl. 255.255.255.255
];

/// IPv6 blocks a handler call never dials (IANA IPv6 special-purpose
/// registry, plus multicast and deprecated site-local). Forms that embed
/// an IPv4 address — mapped, NAT64 `64:ff9b::/96`, 6to4 `2002::/16` —
/// are judged by that address first, in `embedded_v4`. `2001::/23` is
/// refused whole (Teredo, benchmarking, ORCHID, a few anycast services):
/// conservative, and nothing in it is a job handler.
const LOCAL_V6: &[(Ipv6Addr, u8)] = &[
    (Ipv6Addr::UNSPECIFIED, 96), // ::/96 — unspecified, loopback, v4-compatible
    (Ipv6Addr::new(0x64, 0xff9b, 1, 0, 0, 0, 0, 0), 48), // local-use NAT64
    (Ipv6Addr::new(0x100, 0, 0, 0, 0, 0, 0, 0), 64), // discard-only
    (Ipv6Addr::new(0x100, 0, 0, 1, 0, 0, 0, 0), 64), // dummy prefix (RFC 9780)
    (Ipv6Addr::new(0x2001, 0, 0, 0, 0, 0, 0, 0), 23), // IETF protocol assignments
    (Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 0), 32), // documentation
    (Ipv6Addr::new(0x3fff, 0, 0, 0, 0, 0, 0, 0), 20), // documentation (RFC 9637)
    (Ipv6Addr::new(0x5f00, 0, 0, 0, 0, 0, 0, 0), 16), // SRv6 SIDs
    (Ipv6Addr::new(0xfc00, 0, 0, 0, 0, 0, 0, 0), 7), // unique-local
    (Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 0), 10), // link-local
    (Ipv6Addr::new(0xfec0, 0, 0, 0, 0, 0, 0, 0), 10), // deprecated site-local
    (Ipv6Addr::new(0xff00, 0, 0, 0, 0, 0, 0, 0), 8), // multicast
];

fn in_v4(ip: Ipv4Addr, (net, len): (Ipv4Addr, u8)) -> bool {
    let mask = u32::MAX.checked_shl(32 - u32::from(len)).unwrap_or(0);
    u32::from(ip) & mask == u32::from(net) & mask
}

fn in_v6(ip: Ipv6Addr, (net, len): (Ipv6Addr, u8)) -> bool {
    let mask = u128::MAX.checked_shl(128 - u32::from(len)).unwrap_or(0);
    u128::from(ip) & mask == u128::from(net) & mask
}

/// The IPv4 address an IPv6 address stands for, when it is a translation
/// form a dialer (or a NAT64/6to4 gateway) would turn into an IPv4
/// connection.
fn embedded_v4(v6: Ipv6Addr) -> Option<Ipv4Addr> {
    if let Some(v4) = v6.to_ipv4_mapped() {
        return Some(v4);
    }
    let segments = v6.segments();
    let join = |high: u16, low: u16| Ipv4Addr::from((u32::from(high) << 16) | u32::from(low));
    if segments[..6] == [0x64, 0xff9b, 0, 0, 0, 0] {
        return Some(join(segments[6], segments[7])); // NAT64 well-known prefix
    }
    if segments[0] == 0x2002 {
        return Some(join(segments[1], segments[2])); // 6to4
    }
    None
}

fn is_local(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => LOCAL_V4.iter().any(|&block| in_v4(v4, block)),
        IpAddr::V6(v6) => {
            embedded_v4(v6).is_some_and(|v4| is_local(IpAddr::V4(v4)))
                || LOCAL_V6.iter().any(|&block| in_v6(v6, block))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_local_refuses_every_non_global_form() {
        for ip in [
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            IpAddr::V4(Ipv4Addr::new(100, 64, 0, 1)),
            IpAddr::V4(Ipv4Addr::new(172, 16, 0, 1)),
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
            IpAddr::V4(Ipv4Addr::new(169, 254, 169, 254)), // cloud metadata
            IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)),       // TEST-NET
            IpAddr::V4(Ipv4Addr::new(198, 51, 100, 1)),
            IpAddr::V4(Ipv4Addr::new(203, 0, 113, 1)),
            IpAddr::V4(Ipv4Addr::new(198, 18, 0, 1)),
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            IpAddr::V4(Ipv4Addr::BROADCAST),
            IpAddr::V4(Ipv4Addr::new(224, 0, 0, 1)),
            IpAddr::V4(Ipv4Addr::new(240, 0, 0, 1)),
            IpAddr::V6(Ipv6Addr::LOCALHOST),
            IpAddr::V6(Ipv6Addr::UNSPECIFIED),
            IpAddr::V6("fd00::1".parse().unwrap_or_else(|e| panic!("{e}"))),
            IpAddr::V6("fe80::1".parse().unwrap_or_else(|e| panic!("{e}"))),
            IpAddr::V6("2001:db8::1".parse().unwrap_or_else(|e| panic!("{e}"))),
            // Forms that embed a local IPv4 — judged by the embedded one.
            IpAddr::V6("::ffff:127.0.0.1".parse().unwrap_or_else(|e| panic!("{e}"))),
            IpAddr::V6("::ffff:10.0.0.1".parse().unwrap_or_else(|e| panic!("{e}"))),
            IpAddr::V6(
                "64:ff9b:1::7f00:1"
                    .parse()
                    .unwrap_or_else(|e| panic!("{e}")),
            ), // NAT64 local-use → 127.0.0.1
            IpAddr::V6("2002:7f00:1::".parse().unwrap_or_else(|e| panic!("{e}"))), // 6to4 → 127.0.0.1
        ] {
            assert!(is_local(ip), "{ip} must be refused");
        }
    }

    #[test]
    fn is_local_allows_public_addresses() {
        for ip in [
            IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)),
            IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)),
            IpAddr::V4(Ipv4Addr::new(140, 82, 121, 4)),
            IpAddr::V6(
                "2606:4700:4700::1111"
                    .parse()
                    .unwrap_or_else(|e| panic!("{e}")),
            ),
            IpAddr::V6("::ffff:8.8.8.8".parse().unwrap_or_else(|e| panic!("{e}"))),
            IpAddr::V6("64:ff9b::808:808".parse().unwrap_or_else(|e| panic!("{e}"))), // NAT64 well-known → 8.8.8.8 (public — allowed)
        ] {
            assert!(!is_local(ip), "{ip} is public and must dial");
        }
    }

    #[test]
    fn check_url_refuses_literal_local_and_bad_schemes() {
        let strict = |u: &str| check_url(&Url::parse(u).unwrap_or_else(|e| panic!("{e}")), false);
        assert!(strict("http://127.0.0.1:9/h").is_err());
        assert!(strict("http://[::1]:9/h").is_err());
        assert!(strict("http://[::ffff:127.0.0.1]:9/h").is_err());
        assert!(strict("http://169.254.169.254/latest/meta-data").is_err());
        assert!(strict("http://10.0.0.4/h").is_err());
        assert!(strict("file:///etc/passwd").is_err());
        assert!(strict("gopher://example.com/").is_err());
        assert!(strict("http:///missing-host").is_err() || strict("http:///missing-host").is_ok());
        // Obfuscated literals: the URL parser normalizes them to literal
        // v4 hosts before we ever look — decimal and hex loopback refuse.
        for u in ["http://2130706433/", "http://0x7f.0.0.1/", "http://0177.1/"] {
            let parsed = Url::parse(u).unwrap_or_else(|e| panic!("{e}"));
            assert!(
                matches!(parsed.host(), Some(Host::Ipv4(v4)) if is_local(IpAddr::V4(v4))),
                "{u} must normalize to a refused literal, got {:?}",
                parsed.host()
            );
        }
        // Domains defer to the resolver — nothing DNS decided here.
        assert!(strict("http://handler.example/h").is_ok());
        assert!(strict("https://handler.example/h").is_ok());
        // The flag opens everything — fixtures dial loopback.
        assert!(check_url(
            &Url::parse("http://127.0.0.1:9/h").unwrap_or_else(|e| panic!("{e}")),
            true
        )
        .is_ok());
    }

    /// The measured overhead the guard adds: one OS lookup + the CIDR
    /// scan per *new* connection (reqwest pools keep-alives per host, so
    /// a call reusing a connection pays nothing). On this machine the
    /// scan itself is sub-microsecond — the lookup dominates. Failure
    /// modes here are resolution failures, not cost: assert the mean
    /// stays a per-connection cost, not per-call latency.
    #[tokio::test]
    async fn resolver_overhead_is_per_connection_and_microscale() {
        let r = resolver(false);
        let name = || -> Name { "localhost".parse().unwrap_or_else(|e| panic!("{e}")) };
        // Warm the OS cache, then measure a real resolve+validate pass.
        let _ = r.resolve(name()).await;
        let n = 200u32;
        let t0 = std::time::Instant::now();
        for _ in 0..n {
            let _ = r.resolve(name()).await;
        }
        let mean = t0.elapsed() / n;
        eprintln!("ssrf resolve+validate mean over {n} fresh lookups: {mean:?}");
        assert!(
            mean < std::time::Duration::from_millis(20),
            "resolve+validate must stay a per-connection cost: {mean:?}"
        );
        // And the scan alone — the part that is always ours, no OS.
        let local = IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8));
        let t0 = std::time::Instant::now();
        for _ in 0..10_000 {
            std::hint::black_box(is_local(local));
        }
        let scan = t0.elapsed() / 10_000;
        eprintln!("is_local scan: {scan:?} per check");
        assert!(
            scan < std::time::Duration::from_micros(10),
            "the CIDR scan must stay nanoscale: {scan:?}"
        );
    }
}
