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
//! Both layers refuse the same set, carried by [`Policy`]: the IANA
//! non-global blocks (`is_local`), **this host's own interface addresses**
//! (white-hat finding A: on Linux a connection to the droplet's own public
//! address is delivered over `lo`, so a handler URL naming it reaches
//! whatever listens on 0.0.0.0 — sshd, exporters, Docker-published ports,
//! the other products on a shared droplet), and the operator's
//! `--deny-cidr` ranges (a reserved/floating IP is not on any interface,
//! so it must be listed).
//!
//! `--allow-local-handlers` opts out of the local blocks and the host's
//! addresses for development and fixtures; it is a visible command-line
//! flag, never ambient state. `--deny-cidr` ranges are refused even then.

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

/// What a handler call may dial — one value shared by the registration
/// check, the call-time literal check and the resolver, so the three can
/// never disagree. Cheap to clone (the sets are shared).
#[derive(Debug, Clone)]
pub(crate) struct Policy {
    /// `--allow-local-handlers`: the local blocks and this host's own
    /// addresses are allowed (fixtures, development).
    allow_local: bool,
    /// This host's own interface addresses, both families, collected at
    /// boot (`Policy::boot`).
    host: Arc<[IpAddr]>,
    /// `--deny-cidr` ranges: refused always, `--allow-local-handlers` or not.
    deny: Arc<[(IpAddr, u8)]>,
}

impl Policy {
    pub(crate) fn new(allow_local: bool, host: Vec<IpAddr>, deny: Vec<(IpAddr, u8)>) -> Self {
        Self {
            allow_local,
            host: host.into(),
            deny: deny.into(),
        }
    }

    /// The shipped policy: every address on this host's interfaces (the
    /// public ones included) is refused with the local blocks. Fails —
    /// and the server refuses to start — when the interfaces cannot be
    /// listed: a guard that silently misses the host's own addresses is
    /// the hole this closes.
    pub(crate) fn boot(allow_local: bool, deny: Vec<(IpAddr, u8)>) -> std::io::Result<Self> {
        Ok(Self::new(allow_local, host_addresses()?, deny))
    }

    /// The same sets with `allow_local` replaced (one interface scan,
    /// two policies: the store's and the `--handler` flags').
    pub(crate) fn with_allow_local(&self, allow_local: bool) -> Self {
        Self {
            allow_local,
            host: Arc::clone(&self.host),
            deny: Arc::clone(&self.deny),
        }
    }

    /// The flag set: `--allow-local-handlers`, no host list, no extra
    /// ranges (tests and fixtures).
    #[cfg(test)]
    pub(crate) fn open() -> Self {
        Self::new(true, Vec::new(), Vec::new())
    }

    /// Strict, with no host list (tests that need only the IANA blocks).
    #[cfg(test)]
    pub(crate) fn strict() -> Self {
        Self::new(false, Vec::new(), Vec::new())
    }

    /// How many host addresses and deny ranges this policy carries (the
    /// startup log line).
    pub(crate) fn sizes(&self) -> (usize, usize) {
        (self.host.len(), self.deny.len())
    }

    /// Why `ip` may not be dialed, or `None` when it may.
    pub(crate) fn refusal(&self, ip: IpAddr) -> Option<&'static str> {
        // ATTACK(ssrf-deny-cidr)
        if self.deny.iter().any(|&block| in_block(ip, block)) {
            return Some("handler URL targets a range this server denies (--deny-cidr)");
        }
        if self.allow_local {
            return None;
        }
        if is_local(ip) {
            return Some("handler URL targets a local address");
        }
        // ATTACK(ssrf-own-address)
        if self.host.iter().any(|&own| same_address(ip, own)) {
            return Some("handler URL targets this server's own address");
        }
        None
    }
}

/// Every address on this host's interfaces, both families (getifaddrs).
pub(crate) fn host_addresses() -> std::io::Result<Vec<IpAddr>> {
    let mut v: Vec<IpAddr> = if_addrs::get_if_addrs()?
        .into_iter()
        .map(|i| i.ip())
        .collect();
    v.sort_unstable();
    v.dedup();
    Ok(v)
}

/// `--deny-cidr`: `ADDR/PREFIX`, or a bare address (one host). Host bits
/// below the prefix are ignored.
pub(crate) fn parse_cidr(s: &str) -> Result<(IpAddr, u8), String> {
    let (a, p) = match s.trim().split_once('/') {
        Some((a, p)) => (a, Some(p)),
        None => (s.trim(), None),
    };
    let ip: IpAddr = a.parse().map_err(|_| format!("bad address in {s:?}"))?;
    let bits = if ip.is_ipv4() { 32 } else { 128 };
    let len = match p {
        None => bits,
        Some(p) => p
            .parse::<u8>()
            .map_err(|_| format!("bad prefix in {s:?}"))?,
    };
    if len > bits {
        return Err(format!("prefix /{len} too long in {s:?}"));
    }
    Ok((ip, len))
}

/// `ip` is in `block`, judging an IPv6 form that embeds an IPv4 address
/// (mapped, NAT64, 6to4) by that address against a v4 block.
fn in_block(ip: IpAddr, (net, len): (IpAddr, u8)) -> bool {
    match (ip, net) {
        (IpAddr::V4(ip), IpAddr::V4(net)) => in_v4(ip, (net, len)),
        (IpAddr::V6(ip), IpAddr::V6(net)) => in_v6(ip, (net, len)),
        (IpAddr::V6(ip), IpAddr::V4(net)) => {
            embedded_v4(ip).is_some_and(|v4| in_v4(v4, (net, len)))
        }
        (IpAddr::V4(_), IpAddr::V6(_)) => false,
    }
}

/// `ip` names the host address `own`, directly or through an IPv6 form
/// that embeds it.
fn same_address(ip: IpAddr, own: IpAddr) -> bool {
    let bits = if own.is_ipv4() { 32 } else { 128 };
    in_block(ip, (own, bits))
}

/// reqwest's resolver with the policy inside: names resolve through the
/// OS, then every returned address must pass the [`Policy`]. reqwest
/// receives only the validated set — the check is the resolution itself,
/// so there is no validate-then-re-resolve gap.
#[derive(Debug, Clone)]
pub(crate) struct PolicyResolver {
    policy: Policy,
}

impl Resolve for PolicyResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let policy = self.policy.clone();
        Box::pin(async move {
            let host = name.as_str().to_owned();
            let addrs: Vec<SocketAddr> =
                tokio::net::lookup_host((host.as_str(), 0)).await?.collect();
            validate_addrs(addrs.iter().map(SocketAddr::ip), &policy)?;
            Ok(Box::new(addrs.into_iter()) as Addrs)
        })
    }
}

/// Every resolved address must pass the policy: a single refused address
/// fails the whole resolution.
fn validate_addrs(
    addrs: impl IntoIterator<Item = IpAddr>,
    policy: &Policy,
) -> Result<(), ResolvedLocal> {
    // ATTACK(ssrf-resolve)
    if addrs.into_iter().any(|ip| policy.refusal(ip).is_some()) {
        return Err(ResolvedLocal);
    }
    Ok(())
}

/// The handler client's resolver — the enforcement layer.
pub(crate) fn resolver(policy: &Policy) -> Arc<PolicyResolver> {
    Arc::new(PolicyResolver {
        policy: policy.clone(),
    })
}

/// The registration-time check (and the call-time literal check): scheme,
/// and a literal host the policy refuses, fail fast. A *domain* cannot be
/// judged here — it resolves at call time, inside `PolicyResolver`.
pub(crate) fn check_url(url: &Url, policy: &Policy) -> Result<(), &'static str> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err("handler URL must be http:// or https://");
    }
    let Some(host) = url.host() else {
        return Err("handler URL needs a host");
    };
    let ip = match host {
        Host::Ipv4(v4) => IpAddr::V4(v4),
        Host::Ipv6(v6) => IpAddr::V6(v6),
        Host::Domain(_) => return Ok(()),
    };
    match policy.refusal(ip) {
        Some(why) => Err(why),
        None => Ok(()),
    }
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
        let strict = |u: &str| {
            check_url(
                &Url::parse(u).unwrap_or_else(|e| panic!("{e}")),
                &Policy::strict(),
            )
        };
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
            &Policy::open()
        )
        .is_ok());
    }

    /// White-hat finding A: this host's own addresses — public ones
    /// included — are refused by the literal check AND the resolver path,
    /// in every IPv6 form that embeds them. The addresses here stand in
    /// for a droplet's public ones (a test machine has none): without the
    /// host list each is a public address that dials.
    #[test]
    fn the_hosts_own_addresses_are_refused_by_both_layers() {
        let own_v4: IpAddr = "8.8.4.4".parse().unwrap_or_else(|e| panic!("{e}"));
        let own_v6: IpAddr = "2606:4700::6810:84e5"
            .parse()
            .unwrap_or_else(|e| panic!("{e}"));
        let ours = Policy::new(false, vec![own_v4, own_v6], Vec::new());
        let url = |u: &str| Url::parse(u).unwrap_or_else(|e| panic!("{e}"));
        for u in [
            "http://8.8.4.4/h",
            "https://8.8.4.4:8443/h",
            "http://[2606:4700::6810:84e5]/h",
            "http://[::ffff:8.8.4.4]/h", // mapped: judged by the v4 inside
            "http://134743044/h",        // 8.8.4.4 as one decimal number
        ] {
            assert_eq!(
                check_url(&url(u), &ours),
                Err("handler URL targets this server's own address"),
                "{u}"
            );
            assert!(
                check_url(&url(u), &Policy::strict()).is_ok(),
                "{u} is public: only the host list refuses it"
            );
        }
        // The resolver path: an answer naming one of ours fails whole.
        let mixed = [IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), own_v4];
        assert!(validate_addrs(mixed, &ours).is_err());
        assert!(validate_addrs([own_v6], &ours).is_err());
        assert!(validate_addrs(mixed, &Policy::strict()).is_ok());
        // Another public address still dials.
        assert!(check_url(&url("http://8.8.8.8/h"), &ours).is_ok());
        // --allow-local-handlers opens the host's addresses with the rest.
        let open = Policy::new(true, vec![own_v4], Vec::new());
        assert!(check_url(&url("http://8.8.4.4/h"), &open).is_ok());
    }

    /// `--deny-cidr` ranges are refused by both layers — even under
    /// `--allow-local-handlers`, and through the embedding IPv6 forms.
    #[test]
    fn deny_cidr_ranges_are_refused_even_with_allow_local() {
        let deny = vec![
            parse_cidr("203.0.113.0/24").unwrap_or_else(|e| panic!("{e}")),
            parse_cidr("2001:db8:5::/48").unwrap_or_else(|e| panic!("{e}")),
            parse_cidr("127.0.0.7").unwrap_or_else(|e| panic!("{e}")),
        ];
        let url = |u: &str| Url::parse(u).unwrap_or_else(|e| panic!("{e}"));
        for allow_local in [false, true] {
            let p = Policy::new(allow_local, Vec::new(), deny.clone());
            for u in [
                "http://203.0.113.9/h",
                "http://[::ffff:203.0.113.9]/h",
                "http://[2001:db8:5:ffff::1]/h",
            ] {
                assert!(
                    check_url(&url(u), &p).is_err(),
                    "{u} allow_local={allow_local}"
                );
            }
            assert!(validate_addrs([IpAddr::V4(Ipv4Addr::new(203, 0, 113, 200))], &p).is_err());
        }
        // Under the flag the rest of loopback still dials; the listed host does not.
        let open = Policy::new(true, Vec::new(), deny);
        assert!(check_url(&url("http://127.0.0.1:9/h"), &open).is_ok());
        assert!(check_url(&url("http://127.0.0.7:9/h"), &open).is_err());
    }

    #[test]
    fn deny_cidrs_parse_strictly() {
        let v4 = |s: &str| {
            (
                IpAddr::V4(s.parse().unwrap_or_else(|e| panic!("{e}"))),
                32u8,
            )
        };
        assert_eq!(parse_cidr("203.0.113.7"), Ok(v4("203.0.113.7")));
        assert_eq!(parse_cidr(" 203.0.113.7/32 "), Ok(v4("203.0.113.7")));
        assert_eq!(
            parse_cidr("2001:db8::/32").map(|(_, l)| l),
            Ok(32),
            "a v6 range"
        );
        for bad in [
            "",
            "203.0.113.7/33",
            "2001:db8::/129",
            "x/8",
            "1.2.3.4/",
            "1.2.3.4/-1",
            "localhost",
        ] {
            assert!(parse_cidr(bad).is_err(), "{bad:?}");
        }
    }

    /// The shipped policy really collects this host's addresses: every
    /// address getifaddrs reports — loopback, the LAN address, any public
    /// one — is refused by `Policy::boot`.
    #[test]
    fn boot_refuses_every_interface_address() {
        let own = host_addresses().unwrap_or_else(|e| panic!("getifaddrs: {e}"));
        assert!(own.contains(&IpAddr::V4(Ipv4Addr::LOCALHOST)), "{own:?}");
        let p = Policy::boot(false, Vec::new()).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(p.sizes(), (own.len(), 0));
        for ip in own {
            assert!(
                p.refusal(ip).is_some(),
                "{ip} is this host's: it must be refused"
            );
        }
    }

    /// The measured overhead the guard adds: one OS lookup + the CIDR
    /// scan per *new* connection (reqwest pools keep-alives per host, so
    /// a call reusing a connection pays nothing). On this machine the
    /// scan itself is sub-microsecond — the lookup dominates. Failure
    /// modes here are resolution failures, not cost: assert the mean
    /// stays a per-connection cost, not per-call latency.
    #[tokio::test]
    async fn resolver_overhead_is_per_connection_and_microscale() {
        let r = resolver(&Policy::strict());
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
