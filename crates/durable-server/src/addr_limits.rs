//! Per-address bounds (the D-045 tail): a request rate and a connection
//! count per address, so one flooding address cannot spend the server's
//! capacity while its neighbours wait.
//!
//! The client address is the TCP peer, or the `CF-Connecting-IP` header
//! when the peer is inside a configured trusted-proxy range. The header
//! from any other peer is ignored — anyone can set it, so believing it
//! would let a flooder claim a fresh address per request.
//!
//! Memory is bounded: the rate table holds at most `max_addrs` buckets
//! (~64 bytes each); idle entries are reclaimed when the table fills, and
//! any address that still does not fit shares one overflow bucket, so a
//! spray of millions of distinct addresses limits itself.
//!
//! The connection cap is per TCP peer, not per resolved client address:
//! at accept time no request has been read yet. Behind a trusted proxy
//! the peer is the proxy's edge — raise `--max-conns-per-addr` there (the
//! per-address request rate still bounds each real client).

use std::collections::HashMap;
use std::net::IpAddr;
use std::time::{Duration, Instant};

/// Every bounded table caps at this many entries (~4 MiB of buckets) —
/// plenty for real clients, nowhere near enough to matter to memory.
pub const MAX_LIMITED_ADDRS: usize = 65_536;

/// Idle this long with a full bucket and the entry is reclaimable.
const IDLE_EVICT: Duration = Duration::from_secs(60);

/// An `ip/prefix` range (IPv4 or IPv6) a proxy address may come from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IpNet {
    addr: IpAddr,
    prefix: u8,
}

impl IpNet {
    /// `10.0.0.0/8`, `2606:4700::/32`, or a bare address (`/32`, `/128`).
    pub fn parse(s: &str) -> Result<Self, String> {
        let (ip_s, prefix) = match s.split_once('/') {
            Some((a, p)) => (
                a,
                p.parse::<u8>()
                    .map_err(|_| format!("bad prefix in {s:?}"))?,
            ),
            None => (s, 255),
        };
        let addr: IpAddr = ip_s.parse().map_err(|_| format!("bad address in {s:?}"))?;
        let bits = match addr {
            IpAddr::V4(_) => 32,
            IpAddr::V6(_) => 128,
        };
        if prefix != 255 && prefix > bits {
            return Err(format!("prefix /{prefix} too long in {s:?}"));
        }
        Ok(Self {
            addr,
            prefix: prefix.min(bits),
        })
    }

    pub fn contains(&self, ip: IpAddr) -> bool {
        // Families never mix: a v4 range does not cover a v6 peer (a
        // v4-mapped v6 peer is its own thing — list it separately).
        match (self.addr, ip) {
            (IpAddr::V4(net), IpAddr::V4(ip)) => {
                let keep = 32 - self.prefix;
                net.to_bits() >> keep == ip.to_bits() >> keep
            }
            (IpAddr::V6(net), IpAddr::V6(ip)) => {
                let keep = 128 - self.prefix;
                net.to_bits() >> keep == ip.to_bits() >> keep
            }
            _ => false,
        }
    }
}

/// The proxy ranges that may speak for a client (D-052). Usually
/// Cloudflare's published ranges; empty means the TCP peer is always the
/// client address.
#[derive(Debug, Clone, Default)]
pub struct TrustedProxies(pub Vec<IpNet>);

impl TrustedProxies {
    fn trusts(&self, peer: IpAddr) -> bool {
        self.0.iter().any(|r| r.contains(peer))
    }
}

/// The address the per-address limits count. `cf_connecting_ip` is the
/// raw header value — believed only for a trusted peer, and only when it
/// parses as an address (a malformed one is not a free pass either).
pub fn client_addr(
    peer: IpAddr,
    trusted: &TrustedProxies,
    cf_connecting_ip: Option<&str>,
) -> IpAddr {
    if trusted.trusts(peer) {
        if let Some(ip) = cf_connecting_ip.and_then(|v| v.trim().parse::<IpAddr>().ok()) {
            return ip;
        }
    }
    peer
}

/// One token bucket: `burst` tokens refill at `rate`/second.
#[derive(Debug, Clone, Copy)]
struct Bucket {
    tokens: f64,
    updated: Instant,
}

impl Bucket {
    fn full(burst: f64, now: Instant) -> Self {
        Self {
            tokens: burst,
            updated: now,
        }
    }

    /// Take a token, refilling for elapsed time. Err holds how long until
    /// one token exists — the `Retry-After` the answer should carry.
    fn take(&mut self, rate: f64, burst: f64, now: Instant) -> Result<(), Duration> {
        let refill = now.saturating_duration_since(self.updated).as_secs_f64() * rate;
        self.tokens = self.tokens.min(burst) + refill;
        if self.tokens > burst {
            self.tokens = burst;
        }
        self.updated = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            Ok(())
        } else {
            Err(Duration::from_secs_f64((1.0 - self.tokens) / rate))
        }
    }
}

/// A bounded token-bucket table: one bucket per client address.
#[derive(Debug)]
pub struct RateLimit {
    rate: f64,
    burst: f64,
    max_addrs: usize,
    addrs: HashMap<IpAddr, Bucket>,
    /// Every address that cannot get its own entry shares this one: a
    /// spray of distinct addresses limits itself, never evicts hot entries.
    overflow: Bucket,
}

impl RateLimit {
    /// `rate` requests/second per address, bursting up to one second.
    pub fn new(rate: f64, max_addrs: usize) -> Self {
        Self {
            rate,
            burst: rate.max(1.0),
            max_addrs,
            addrs: HashMap::new(),
            overflow: Bucket::full(rate.max(1.0), Instant::now()),
        }
    }

    /// Ok: this request may proceed. Err: refuse with that Retry-After.
    pub fn check(&mut self, addr: IpAddr, now: Instant) -> Result<(), Duration> {
        let Self {
            rate,
            burst,
            max_addrs,
            addrs,
            overflow,
        } = self;
        let bucket = match addrs.get_mut(&addr) {
            Some(b) => b,
            None => {
                if addrs.len() < *max_addrs {
                    addrs.entry(addr).or_insert(Bucket::full(*burst, now))
                } else {
                    // Table full: reclaim entries untouched for a minute
                    // (their stale token count would refill to full anyway
                    // on return — evicting costs nothing), then overflow
                    // whatever is left.
                    let idle = now - IDLE_EVICT;
                    addrs.retain(|_, b| b.updated > idle);
                    if addrs.len() < *max_addrs {
                        addrs.entry(addr).or_insert(Bucket::full(*burst, now))
                    } else {
                        overflow
                    }
                }
            }
        };
        bucket.take(*rate, *burst, now)
    }

    #[cfg(test)]
    fn tracked(&self) -> usize {
        self.addrs.len()
    }
}

/// Per-peer open connections. Over the cap the socket is closed at
/// accept; the guard releases the slot when the connection ends.
#[derive(Debug, Clone)]
pub struct ConnLimit {
    cap: usize,
    counts: std::sync::Arc<std::sync::Mutex<HashMap<IpAddr, usize>>>,
}

impl ConnLimit {
    pub fn new(cap: usize) -> Self {
        Self {
            cap: cap.max(1),
            counts: std::sync::Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    pub fn acquire(&self, addr: IpAddr) -> Option<ConnGuard> {
        let mut counts = self.counts.lock().unwrap_or_else(|p| p.into_inner());
        let n = counts.entry(addr).or_insert(0);
        if *n >= self.cap {
            return None;
        }
        *n += 1;
        Some(ConnGuard {
            addr,
            counts: self.counts.clone(),
        })
    }

    #[cfg(test)]
    fn held(&self, addr: IpAddr) -> usize {
        *self
            .counts
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&addr)
            .unwrap_or(&0)
    }
}

#[derive(Debug)]
pub struct ConnGuard {
    addr: IpAddr,
    counts: std::sync::Arc<std::sync::Mutex<HashMap<IpAddr, usize>>>,
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        let mut counts = self.counts.lock().unwrap_or_else(|p| p.into_inner());
        match counts.get_mut(&self.addr) {
            Some(n) if *n > 1 => *n -= 1,
            _ => {
                counts.remove(&self.addr);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap_or_else(|e| panic!("{e}"))
    }

    #[test]
    fn a_cidr_matches_inside_not_outside() {
        let net = IpNet::parse("203.0.113.0/24").unwrap_or_else(|e| panic!("{e}"));
        assert!(net.contains(ip("203.0.113.44")));
        assert!(!net.contains(ip("203.0.114.1")));
        assert!(!net.contains(ip("::ffff:203.0.113.44"))); // families never mix
        let v6 = IpNet::parse("2001:db8::/32").unwrap_or_else(|e| panic!("{e}"));
        assert!(v6.contains(ip("2001:db8:5::1")));
        assert!(!v6.contains(ip("2001:db9::1")));
        let host = IpNet::parse("203.0.113.7").unwrap_or_else(|e| panic!("{e}"));
        assert!(host.contains(ip("203.0.113.7")));
        assert!(!host.contains(ip("203.0.113.8")));
        assert!(IpNet::parse("203.0.113.0/33").is_err());
        assert!(IpNet::parse("not-an-ip/24").is_err());
    }

    #[test]
    fn the_header_is_believed_only_from_a_trusted_peer() {
        let trusted = TrustedProxies(vec![
            IpNet::parse("203.0.113.0/24").unwrap_or_else(|e| panic!("{e}"))
        ]);
        // A trusted peer names its client; anyone else's header is ignored.
        assert_eq!(
            client_addr(ip("203.0.113.9"), &trusted, Some("198.51.100.7")),
            ip("198.51.100.7")
        );
        assert_eq!(
            client_addr(ip("198.51.100.9"), &trusted, Some("203.0.113.7")),
            ip("198.51.100.9")
        );
        // A malformed header is not a free pass even from a trusted peer.
        assert_eq!(
            client_addr(ip("203.0.113.9"), &trusted, Some("junk")),
            ip("203.0.113.9")
        );
    }

    #[test]
    fn the_bucket_refuses_then_refills() {
        let mut lim = RateLimit::new(5.0, 1024);
        let now = Instant::now();
        let a = ip("192.0.2.1");
        for _ in 0..5 {
            assert!(lim.check(a, now).is_ok());
        }
        let after = match lim.check(a, now) {
            Err(retry) => retry,
            Ok(()) => panic!("the sixth request in one second was allowed"),
        };
        assert!(after <= Duration::from_secs(1) && after > Duration::ZERO);
        // One second later one token has refilled.
        assert!(lim.check(a, now + Duration::from_secs(1)).is_ok());
    }

    /// The bound: a spray of distinct addresses cannot grow the table past
    /// `max_addrs`, and the overflow still limits.
    #[test]
    fn a_million_addresses_stay_bounded() {
        let mut lim = RateLimit::new(5.0, 1024);
        let now = Instant::now();
        for i in 0..1_000_000u32 {
            let addr = IpAddr::from([1, (i >> 16) as u8, (i >> 8) as u8, i as u8]); // distinct
            let _ = lim.check(addr, now);
        }
        assert_eq!(lim.tracked(), 1024, "the table is bounded");
        // The million-and-first and every address past the cap share the
        // overflow bucket: a few proceed, then it refuses. (The spray
        // itself drained it — probe after a refill second.)
        let later = now + Duration::from_millis(1500);
        let mut ok = 0;
        let mut refused = 0;
        for i in 0..20u32 {
            let addr = IpAddr::from([15, (i >> 16) as u8, (i >> 8) as u8, i as u8]);
            match lim.check(addr, later) {
                Ok(()) => ok += 1,
                Err(_) => refused += 1,
            }
        }
        assert_eq!(ok, 5, "the overflow holds a one-second burst");
        assert_eq!(refused, 15);
    }

    /// Idle entries are reclaimed when the table fills; a hot entry is not.
    #[test]
    fn idle_entries_make_room_but_hot_entries_stay() {
        let mut lim = RateLimit::new(5.0, 2);
        let t0 = Instant::now();
        let hot = ip("192.0.2.10");
        let gone = ip("192.0.2.20");
        assert!(lim.check(hot, t0).is_ok());
        assert!(lim.check(gone, t0).is_ok());
        // 61s later: `hot` is still active (keeps touching its bucket),
        // `gone` is idle — the table is full but `new` gets `gone`'s slot.
        let t1 = t0 + Duration::from_secs(61);
        assert!(lim.check(hot, t1).is_ok());
        let new = ip("192.0.2.30");
        assert!(lim.check(new, t1).is_ok());
        assert_eq!(lim.tracked(), 2);
        // With both entries hot, a third address falls to the overflow
        // bucket — still limited, not free.
        let another = ip("192.0.2.40");
        for _ in 0..5 {
            assert!(lim.check(another, t1).is_ok());
        }
        assert!(lim.check(another, t1).is_err());
    }

    #[test]
    fn the_connection_cap_releases_on_drop() {
        let limit = ConnLimit::new(2);
        let a = ip("192.0.2.1");
        let b = ip("192.0.2.2");
        let g1 = limit.acquire(a).unwrap_or_else(|| panic!("first conn"));
        let _g2 = limit.acquire(a).unwrap_or_else(|| panic!("second conn"));
        assert!(
            limit.acquire(a).is_none(),
            "third conn from one peer refused"
        );
        assert!(limit.acquire(b).is_some(), "another peer proceeds");
        drop(g1);
        assert!(limit.acquire(a).is_some(), "a slot frees on close");
        // The guard above was a temporary — it already released, so `a`
        // holds only `_g2`'s slot again.
        assert_eq!(limit.held(a), 1);
    }
}
