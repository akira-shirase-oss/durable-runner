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
//! at accept time no request has been read yet. A peer inside the
//! trusted ranges gets the larger `--max-conns-per-proxy` (a proxy edge
//! legitimately holds many real clients); everyone else gets
//! `--max-conns-per-addr`. The per-address request rate still bounds
//! each real client behind the proxy.
//!
//! Residual: an address space far wider than the table — a spray of
//! distinct /48s — can still push new real clients into the shared
//! overflow bucket. That is what the /48 aggregate is for: one /48
//! cannot also mint the whole table of /64s.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv6Addr};
use std::time::{Duration, Instant};

/// Every bounded table caps at this many entries (~4 MiB of buckets) —
/// plenty for real clients, nowhere near enough to matter to memory.
pub const MAX_LIMITED_ADDRS: usize = 65_536;

/// One IPv6 `/64` counts as one client address by default: a v6 address
/// has 2^64 host bits, so keying the full address would let one /64 mint
/// unlimited buckets — and fill the table, pushing real new clients into
/// the shared overflow. `--rate-limit-v6-prefix` widens or narrows it.
pub const DEFAULT_V6_PREFIX: u8 = 64;

/// Inside one `/48` the per-address buckets are also charged against an
/// aggregate bucket at this multiple of the address rate, so a /48
/// cannot mint `MAX_LIMITED_ADDRS` fresh /64s to spray through.
const V6_AGG_PREFIX: u8 = 48;
const V6_AGG_MULT: f64 = 10.0;

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
                self.prefix == 0
                    || net.to_bits() >> (32 - self.prefix) == ip.to_bits() >> (32 - self.prefix)
            }
            (IpAddr::V6(net), IpAddr::V6(ip)) => {
                self.prefix == 0
                    || net.to_bits() >> (128 - self.prefix) == ip.to_bits() >> (128 - self.prefix)
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
    pub fn trusts(&self, peer: IpAddr) -> bool {
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
    // ATTACK(cf-header)
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

    /// Add the tokens `now - updated` earns at `rate`, capped at `burst`.
    fn refill(&mut self, now: Instant, rate: f64, burst: f64) {
        let refill = now.saturating_duration_since(self.updated).as_secs_f64() * rate;
        self.tokens = (self.tokens + refill).min(burst);
        self.updated = now;
    }
}

/// What an address is counted as in the bounded table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum LimitKey {
    /// A normalized address: v4 as itself (a v4-mapped v6 unwrapped to
    /// the same key), v6 masked to the configured prefix.
    Addr(IpAddr),
    /// An IPv6 /48 aggregate, so one /48 cannot spray through a fresh
    /// /64 bucket per request.
    V6Aggregate(u128),
}

/// A bounded token-bucket table: one bucket per normalized client
/// address, plus one per IPv6 /48 aggregate.
#[derive(Debug)]
pub struct RateLimit {
    rate: f64,
    burst: f64,
    agg_rate: f64,
    agg_burst: f64,
    v6_prefix: u8,
    max_addrs: usize,
    addrs: HashMap<LimitKey, Bucket>,
    /// Every key that cannot get its own entry shares this one: a spray
    /// of distinct addresses limits itself, never evicts hot entries.
    overflow: Bucket,
}

impl RateLimit {
    /// `rate` requests/second per address, bursting up to one second;
    /// IPv6 addresses key at `v6_prefix` bits and share a `/48` aggregate
    /// at ten times the rate.
    pub fn new(rate: f64, max_addrs: usize, v6_prefix: u8) -> Self {
        let agg_rate = rate * V6_AGG_MULT;
        Self {
            rate,
            burst: rate.max(1.0),
            agg_rate,
            agg_burst: agg_rate.max(1.0),
            v6_prefix: v6_prefix.min(128),
            max_addrs,
            addrs: HashMap::new(),
            overflow: Bucket::full(rate.max(1.0), Instant::now()),
        }
    }

    /// What a checked address is counted as: v4 as itself (a mapped v6
    /// unwrapped to the same key), v6 masked to `v6_prefix` bits.
    fn addr_key(&self, addr: IpAddr) -> LimitKey {
        LimitKey::Addr(bucket_addr(addr, self.v6_prefix))
    }

    /// The `/48` aggregate for a real v6 address — only when it is
    /// strictly coarser than the address key (a `v6_prefix` of /48 or
    /// wider would make the aggregate the same bucket: charge once).
    fn agg_key(&self, addr: IpAddr) -> Option<LimitKey> {
        if self.v6_prefix <= V6_AGG_PREFIX {
            return None;
        }
        match addr {
            IpAddr::V6(a) if a.to_ipv4_mapped().is_none() => Some(LimitKey::V6Aggregate(
                a.to_bits() & (!0u128 << (128 - V6_AGG_PREFIX)),
            )),
            _ => None,
        }
    }

    fn params(&self, key: LimitKey) -> (f64, f64) {
        match key {
            LimitKey::Addr(_) => (self.rate, self.burst),
            LimitKey::V6Aggregate(_) => (self.agg_rate, self.agg_burst),
        }
    }

    /// The bucket for `key`, creating it while there is room (reclaiming
    /// minute-idle entries on a full table first) — otherwise the shared
    /// overflow bucket.
    fn bucket(&mut self, key: LimitKey, now: Instant) -> &mut Bucket {
        let (_, burst) = self.params(key);
        if !self.addrs.contains_key(&key) {
            if self.addrs.len() >= self.max_addrs {
                let idle = now - IDLE_EVICT;
                self.addrs.retain(|_, b| b.updated > idle);
            }
            if self.addrs.len() < self.max_addrs {
                self.addrs.insert(key, Bucket::full(burst, now));
            }
        }
        match self.addrs.get_mut(&key) {
            Some(b) => b,
            None => &mut self.overflow,
        }
    }

    /// How long until `key`'s bucket holds a token (zero if it does now).
    fn wait(&mut self, key: LimitKey, now: Instant) -> Duration {
        let (rate, burst) = self.params(key);
        let b = self.bucket(key, now);
        b.refill(now, rate, burst);
        if b.tokens >= 1.0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64((1.0 - b.tokens) / rate)
        }
    }

    fn spend(&mut self, key: LimitKey, now: Instant) {
        let (rate, burst) = self.params(key);
        let b = self.bucket(key, now);
        b.refill(now, rate, burst);
        b.tokens -= 1.0;
    }

    /// Ok: this request may proceed. Err: refuse with that Retry-After —
    /// the longer wait of the address and aggregate buckets.
    pub fn check(&mut self, addr: IpAddr, now: Instant) -> Result<(), Duration> {
        let addr_key = self.addr_key(addr);
        let agg_key = self.agg_key(addr);
        let wait = self.wait(addr_key, now);
        let agg_wait = match agg_key {
            Some(k) => self.wait(k, now),
            None => Duration::ZERO,
        };
        let needed = wait.max(agg_wait);
        if needed > Duration::ZERO {
            return Err(needed);
        }
        self.spend(addr_key, now);
        if let Some(k) = agg_key {
            self.spend(k, now);
        }
        Ok(())
    }

    #[cfg(test)]
    fn tracked(&self) -> usize {
        self.addrs.len()
    }
}

/// The address a connection or request is counted under: v4 as itself
/// (a v4-mapped v6 peer unwrapped to the same bucket), a real v6 masked
/// to `v6_prefix` bits — one /64 is one bucket, so a host's 2^64 host
/// bits cannot mint a fresh bucket per connection (S2 re-review F3: the
/// conn cap used to key the exact address while the rate limiter keyed
/// the prefix, giving one /64 an unbounded connection budget).
fn bucket_addr(addr: IpAddr, v6_prefix: u8) -> IpAddr {
    match addr {
        IpAddr::V4(_) => addr,
        IpAddr::V6(a) => match a.to_ipv4_mapped() {
            Some(v4) => IpAddr::V4(v4),
            None => {
                let masked = if v6_prefix == 0 {
                    0
                } else {
                    a.to_bits() & (!0u128 << (128 - v6_prefix))
                };
                IpAddr::V6(Ipv6Addr::from_bits(masked))
            }
        },
    }
}

/// Per-peer open connections, counted by address bucket (v6 peers share
/// the prefix bucket above). Over the cap the socket is refused at
/// accept; the guard releases the slot when the connection ends. A peer
/// inside the trusted-proxy ranges gets the larger `cap_trusted`: the
/// peer is the proxy's edge, and one edge legitimately holds many real
/// clients' connections (D-052). The global connection cap still bounds
/// everything. The counts are kept even with no cap configured — the
/// fair-share pressure yield reads them to evict only conns whose
/// bucket holds more than its share of the pool.
#[derive(Debug, Clone)]
pub struct ConnLimit {
    cap_untrusted: usize,
    cap_trusted: usize,
    v6_prefix: u8,
    counts: std::sync::Arc<std::sync::Mutex<HashMap<IpAddr, usize>>>,
}

impl ConnLimit {
    pub fn new(cap_untrusted: usize, cap_trusted: usize, v6_prefix: u8) -> Self {
        Self {
            cap_untrusted,
            cap_trusted,
            v6_prefix: v6_prefix.min(128),
            counts: std::sync::Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    pub fn acquire(&self, addr: IpAddr, trusted: bool) -> Option<ConnGuard> {
        let cap = if trusted {
            self.cap_trusted
        } else {
            self.cap_untrusted
        };
        let bucket = bucket_addr(addr, self.v6_prefix);
        let mut counts = self.counts.lock().unwrap_or_else(|p| p.into_inner());
        let n = counts.entry(bucket).or_insert(0);
        if cap > 0 && *n >= cap {
            return None;
        }
        *n += 1;
        Some(ConnGuard {
            bucket,
            counts: self.counts.clone(),
        })
    }

    /// Conns held under `addr`'s bucket — the fair-share numerator.
    pub fn held(&self, addr: IpAddr) -> usize {
        *self
            .counts
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&bucket_addr(addr, self.v6_prefix))
            .unwrap_or(&0)
    }

    /// Distinct buckets currently holding conns — the fair-share
    /// denominator.
    pub fn buckets(&self) -> usize {
        self.counts.lock().unwrap_or_else(|p| p.into_inner()).len()
    }
}

#[derive(Debug)]
pub struct ConnGuard {
    bucket: IpAddr,
    counts: std::sync::Arc<std::sync::Mutex<HashMap<IpAddr, usize>>>,
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        let mut counts = self.counts.lock().unwrap_or_else(|p| p.into_inner());
        match counts.get_mut(&self.bucket) {
            Some(n) if *n > 1 => *n -= 1,
            _ => {
                counts.remove(&self.bucket);
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
        let mut lim = RateLimit::new(5.0, 1024, 64);
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

    /// S2: one /64 is one bucket — a v6 address's 2^64 host bits cannot
    /// mint a fresh bucket per request.
    #[test]
    fn a_thousand_addresses_in_one_v6_slash64_share_a_bucket() {
        let mut lim = RateLimit::new(5.0, 4096, 64);
        let now = Instant::now();
        let mut ok = 0;
        for i in 0..1000u16 {
            // 2001:db8::/64, host bits vary
            let a = IpAddr::from([
                0x20,
                0x01,
                0x0d,
                0xb8,
                0,
                0,
                0,
                0,
                (i >> 8) as u8,
                i as u8,
                0,
                0,
                0,
                0,
                0,
                1,
            ]);
            if lim.check(a, now).is_ok() {
                ok += 1;
            }
        }
        assert_eq!(ok, 5, "one /64 is one bucket, however many addresses");
        assert_eq!(lim.tracked(), 2, "one address bucket + one /48 aggregate");
    }

    /// S2: distinct /64s inside one /48 get their own buckets but share
    /// the /48 aggregate (10× the address rate), so a /48 cannot mint
    /// unlimited buckets either.
    #[test]
    fn a_thousand_slash64s_in_one_slash48_hit_the_aggregate() {
        let mut lim = RateLimit::new(5.0, 4096, 64); // aggregate: 50 per /48
        let now = Instant::now();
        let mut ok = 0;
        for i in 0..1000u16 {
            // 2001:db8:0:i::1 — distinct /64s inside 2001:db8::/48
            let a = IpAddr::from([
                0x20,
                0x01,
                0x0d,
                0xb8,
                0,
                0,
                (i >> 8) as u8,
                i as u8,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                1,
            ]);
            if lim.check(a, now).is_ok() {
                ok += 1;
            }
        }
        assert_eq!(ok, 50, "the /48 aggregate caps at 10x the address rate");
    }

    /// S2: a v4-mapped v6 peer and the same client on v4 share a bucket.
    #[test]
    fn an_ipv4_mapped_v6_shares_the_ipv4_bucket() {
        let mut lim = RateLimit::new(5.0, 4096, 64);
        let now = Instant::now();
        for _ in 0..5 {
            assert!(lim.check(ip("1.2.3.4"), now).is_ok());
        }
        assert!(
            lim.check(ip("::ffff:1.2.3.4"), now).is_err(),
            "the mapped form got a second bucket"
        );
    }

    /// Plain v4 addresses still get independent buckets.
    #[test]
    fn distinct_ipv4_addresses_stay_independent() {
        let mut lim = RateLimit::new(5.0, 4096, 64);
        let now = Instant::now();
        for _ in 0..5 {
            assert!(lim.check(ip("192.0.2.10"), now).is_ok());
        }
        assert!(lim.check(ip("192.0.2.10"), now).is_err(), "drained");
        assert!(lim.check(ip("192.0.2.11"), now).is_ok(), "independent");
    }

    /// The bound: a spray of distinct addresses cannot grow the table past
    /// `max_addrs`, and the overflow still limits.
    #[test]
    fn a_million_addresses_stay_bounded() {
        let mut lim = RateLimit::new(5.0, 1024, 64);
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
        let mut lim = RateLimit::new(5.0, 2, 64);
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
        let limit = ConnLimit::new(2, 4, DEFAULT_V6_PREFIX);
        let a = ip("192.0.2.1");
        let b = ip("192.0.2.2");
        let g1 = limit
            .acquire(a, false)
            .unwrap_or_else(|| panic!("first conn"));
        let _g2 = limit
            .acquire(a, false)
            .unwrap_or_else(|| panic!("second conn"));
        assert!(
            limit.acquire(a, false).is_none(),
            "third conn from one untrusted peer refused"
        );
        assert!(limit.acquire(b, false).is_some(), "another peer proceeds");
        drop(g1);
        assert!(limit.acquire(a, false).is_some(), "a slot frees on close");
        // The guard above was a temporary — it already released, so `a`
        // holds only `_g2`'s slot again.
        assert_eq!(limit.held(a), 1);
    }

    /// A trusted proxy's edge holds many clients at once: its cap is the
    /// larger `cap_trusted`, the untrusted-peer cap does not apply.
    #[test]
    fn a_trusted_peer_gets_the_higher_cap() {
        let limit = ConnLimit::new(2, 4, DEFAULT_V6_PREFIX);
        let a = ip("192.0.2.1");
        let _held: Vec<ConnGuard> = (0..4)
            .map(|_| {
                limit
                    .acquire(a, true)
                    .unwrap_or_else(|| panic!("trusted conn"))
            })
            .collect();
        assert!(
            limit.acquire(a, true).is_none(),
            "the fifth trusted conn is refused"
        );
        assert!(
            limit.acquire(ip("192.0.2.2"), false).is_some(),
            "an untrusted peer still gets its own count"
        );
    }

    /// S2 re-review F3: the conn cap keys the same bucket as the rate
    /// limiter — a /64's 2^64 host addresses share one count, so a host
    /// cannot mint a fresh connection budget per address. A v4-mapped v6
    /// peer lands in its v4 bucket.
    #[test]
    fn a_slash64_shares_one_connection_budget() {
        let limit = ConnLimit::new(2, 4, 64);
        let a1 = ip("2001:db8::1");
        let a2 = ip("2001:db8::2");
        let _g1 = limit.acquire(a1, false).unwrap_or_else(|| panic!("conn 1"));
        let _g2 = limit.acquire(a2, false).unwrap_or_else(|| panic!("conn 2"));
        assert_eq!(limit.held(a1), 2, "one /64 is one bucket");
        assert!(
            limit.acquire(ip("2001:db8::3"), false).is_none(),
            "a third address in the /64 is over the cap"
        );
        assert!(
            limit.acquire(ip("2001:db8:1::1"), false).is_some(),
            "a different /64 is a different bucket"
        );
        // The mapped v4 peer is its v4 self, not a fresh v6 bucket.
        let mapped = limit.acquire(ip("::ffff:192.0.2.1"), false);
        assert!(mapped.is_some(), "v4-mapped: its own bucket");
        let _gv4 = limit
            .acquire(ip("192.0.2.1"), false)
            .unwrap_or_else(|| panic!("v4"));
        assert!(
            limit.acquire(ip("::ffff:192.0.2.1"), false).is_none(),
            "the mapped form shares the v4 bucket"
        );
        drop(mapped);
    }
}
