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
//! IPv6 sprays (re-review S2-2): the /48 aggregate is judged before any
//! /64 bucket is created, so a refusing /48 fills nothing; one /48 holds
//! at most `MAX_V6_PER_AGG` /64 entries (its further /64s share one
//! bucket); idleness counts only admitted requests (a refused flood ages
//! out); and the full table is scanned for idle entries at most once per
//! second. Residual: a spray of distinct /48s — about 256 of them, a /40,
//! to fill the default table — can still push new real clients into the
//! shared overflow bucket; past that the edge (Cloudflare) carries it.

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

/// At most this many `/64` entries per `/48` (re-review S2-2): past it,
/// the /48's further /64s share ONE bucket at the address rate, so one
/// /48 holds at most this many + 2 entries of the table.
pub const MAX_V6_PER_AGG: usize = 256;

/// A full table is scanned for idle entries at most this often: a miss
/// flood must not run the O(table) scan under the limiter's lock per
/// request (re-review S2-2: 259 µs per miss).
const RECLAIM_EVERY: Duration = Duration::from_secs(1);

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
            // A mapped `::ffff:a.b.c.d` names an IPv4 client: key it as one.
            return ip.to_canonical();
        }
    }
    peer
}

/// One token bucket: `burst` tokens refill at `rate`/second.
#[derive(Debug, Clone, Copy)]
struct Bucket {
    tokens: f64,
    /// When the tokens were last refilled (the refill clock).
    updated: Instant,
    /// When a request last proceeded on this bucket — the idleness clock.
    /// A refusal never refreshes it (re-review S2-2: a refused flood kept
    /// its entries forever young and the table full).
    active: Instant,
}

impl Bucket {
    fn full(burst: f64, now: Instant) -> Self {
        Self {
            tokens: burst,
            updated: now,
            active: now,
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
    /// The one address bucket a /48's /64s share once it holds
    /// `MAX_V6_PER_AGG` of its own.
    V6Shared(u128),
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
    /// How many `Addr` (/64) entries each /48 holds now.
    per_agg: HashMap<u128, usize>,
    /// When the full table was last scanned for idle entries.
    last_reclaim: Option<Instant>,
    /// Scans run so far (tests: the scan is rate-limited).
    #[cfg(test)]
    reclaims: u64,
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
            per_agg: HashMap::new(),
            last_reclaim: None,
            #[cfg(test)]
            reclaims: 0,
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
    /// The /48 prefix of a real v6 address, when the aggregate applies —
    /// only when it is strictly coarser than the address key (a
    /// `v6_prefix` of /48 or wider would make it the same bucket).
    fn agg_prefix(&self, addr: IpAddr) -> Option<u128> {
        if self.v6_prefix <= V6_AGG_PREFIX {
            return None;
        }
        match addr {
            IpAddr::V6(a) if a.to_ipv4_mapped().is_none() => {
                Some(a.to_bits() & (!0u128 << (128 - V6_AGG_PREFIX)))
            }
            _ => None,
        }
    }

    fn params(&self, key: LimitKey) -> (f64, f64) {
        match key {
            LimitKey::Addr(_) | LimitKey::V6Shared(_) => (self.rate, self.burst),
            LimitKey::V6Aggregate(_) => (self.agg_rate, self.agg_burst),
        }
    }

    /// The /48 an `Addr` key's /64 belongs to (for the per-/48 count).
    fn agg_of_key(key: &LimitKey, v6_prefix: u8) -> Option<u128> {
        match key {
            LimitKey::Addr(IpAddr::V6(a)) if v6_prefix > V6_AGG_PREFIX => {
                Some(a.to_bits() & (!0u128 << (128 - V6_AGG_PREFIX)))
            }
            _ => None,
        }
    }

    /// On a full table, drop minute-idle entries — at most once per
    /// `RECLAIM_EVERY`, never per request.
    fn reclaim(&mut self, now: Instant) {
        if self.addrs.len() < self.max_addrs
            || self
                .last_reclaim
                .is_some_and(|t| now.saturating_duration_since(t) < RECLAIM_EVERY)
        {
            return;
        }
        self.last_reclaim = Some(now);
        #[cfg(test)]
        {
            self.reclaims += 1;
        }
        let idle = now.checked_sub(IDLE_EVICT);
        let v6_prefix = self.v6_prefix;
        let per_agg = &mut self.per_agg;
        self.addrs.retain(|k, b| {
            // ATTACK(limiter-idle-clock)
            let keep = idle.is_none_or(|i| b.active > i);
            if !keep {
                if let Some(agg) = Self::agg_of_key(k, v6_prefix) {
                    if let Some(n) = per_agg.get_mut(&agg) {
                        *n = n.saturating_sub(1);
                        if *n == 0 {
                            per_agg.remove(&agg);
                        }
                    }
                }
            }
            keep
        });
    }

    /// The bucket for `key`, creating it while there is room — otherwise
    /// the shared overflow bucket.
    fn bucket(&mut self, key: LimitKey, now: Instant) -> &mut Bucket {
        let (_, burst) = self.params(key);
        if !self.addrs.contains_key(&key) {
            self.reclaim(now);
            if self.addrs.len() < self.max_addrs {
                self.addrs.insert(key, Bucket::full(burst, now));
                if let Some(agg) = Self::agg_of_key(&key, self.v6_prefix) {
                    *self.per_agg.entry(agg).or_default() += 1;
                }
            }
        }
        match self.addrs.get_mut(&key) {
            Some(b) => b,
            None => &mut self.overflow,
        }
    }

    /// How long until `key`'s bucket holds a token (zero if it does now).
    /// A refusal refills the tokens but never refreshes `active`.
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
        b.active = now;
    }

    /// Ok: this request may proceed. Err: refuse with that Retry-After —
    /// the longer wait of the address and aggregate buckets.
    ///
    /// The /48 aggregate is judged FIRST (re-review S2-2): a /48 that is
    /// refusing never creates a /64 entry, so a spray of fresh /64s fills
    /// nothing; and a /48 already holding `MAX_V6_PER_AGG` /64 entries
    /// keys its further /64s at one shared bucket instead of new entries.
    pub fn check(&mut self, addr: IpAddr, now: Instant) -> Result<(), Duration> {
        let agg = self.agg_prefix(addr);
        let mut agg_wait = Duration::ZERO;
        if let Some(p) = agg {
            // ATTACK(v6-agg-first)
            agg_wait = self.wait(LimitKey::V6Aggregate(p), now);
            if agg_wait > Duration::ZERO {
                return Err(agg_wait);
            }
        }
        let mut addr_key = self.addr_key(addr);
        if let Some(p) = agg {
            if !self.addrs.contains_key(&addr_key)
                // ATTACK(v6-per-agg-cap)
                && self.per_agg.get(&p).copied().unwrap_or(0) >= MAX_V6_PER_AGG
            {
                addr_key = LimitKey::V6Shared(p);
            }
        }
        let wait = self.wait(addr_key, now).max(agg_wait);
        if wait > Duration::ZERO {
            return Err(wait);
        }
        self.spend(addr_key, now);
        if let Some(p) = agg {
            self.spend(LimitKey::V6Aggregate(p), now);
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

/// The bucket an address's fair share is counted under: a v4 address
/// stands alone; a real v6 address's share is its /48 aggregate, so a
/// host that owns a /48 cannot mint 65,536 share buckets (or escape the
/// aggregate cap below) by spraying its /64s (S2 re-review N3, same
/// convention as the rate limiter's /48 aggregate).
fn share_bucket_addr(addr: IpAddr) -> IpAddr {
    match addr {
        IpAddr::V4(_) => addr,
        IpAddr::V6(a) => match a.to_ipv4_mapped() {
            Some(v4) => IpAddr::V4(v4),
            None => IpAddr::V6(Ipv6Addr::from_bits(a.to_bits() & (!0u128 << (128 - 48)))),
        },
    }
}

/// The aggregate cap multiplier over a /48 — the rate limiter's
/// convention, applied to connection counts (S2 re-review N3).
const AGGREGATE_CAP_MULT: usize = 10;

/// Per-peer open connections, counted under two keys: the configured
/// prefix bucket for the per-address cap, and the /48 share bucket for
/// the fair-share count and — for real v6 peers — an aggregate cap at
/// `AGGREGATE_CAP_MULT` times the per-bucket cap. Over a cap the socket
/// is refused at accept; the guard releases both slots when the
/// connection ends. A peer inside the trusted-proxy ranges gets the
/// larger `cap_trusted`: the peer is the proxy's edge, and one edge
/// legitimately holds many real clients' connections (D-052). The
/// global connection cap still bounds everything. The counts are kept
/// even with no cap configured — fair-share accounting and the starved
/// -accept eviction read them (S2 re-review F3 + N3).
#[derive(Debug, Clone)]
pub struct ConnLimit {
    cap_untrusted: usize,
    cap_trusted: usize,
    v6_prefix: u8,
    counts: Counts,
}

// The dual counters: configured-prefix caps first, /48 share buckets second.
type Counts = std::sync::Arc<std::sync::Mutex<(HashMap<IpAddr, usize>, HashMap<IpAddr, usize>)>>;

impl ConnLimit {
    pub fn new(cap_untrusted: usize, cap_trusted: usize, v6_prefix: u8) -> Self {
        Self {
            cap_untrusted,
            cap_trusted,
            v6_prefix: v6_prefix.min(128),
            counts: std::sync::Arc::new(std::sync::Mutex::new((HashMap::new(), HashMap::new()))),
        }
    }

    pub fn acquire(&self, addr: IpAddr, trusted: bool) -> Option<ConnGuard> {
        let cap = if trusted {
            self.cap_trusted
        } else {
            self.cap_untrusted
        };
        let cap_bucket = bucket_addr(addr, self.v6_prefix);
        let share_bucket = share_bucket_addr(addr);
        let mut counts = self.counts.lock().unwrap_or_else(|p| p.into_inner());
        let (cap_counts, share_counts) = &mut *counts;
        if cap > 0 {
            if cap_counts.get(&cap_bucket).copied().unwrap_or(0) >= cap {
                return None;
            }
            // The /48 aggregate bounds a real-v6 tenant to 10x one
            // bucket's conns — subnet-hopping inside its /48 cannot mint
            // an unlimited connection budget (S2 re-review N3).
            if share_bucket.is_ipv6()
                && share_counts.get(&share_bucket).copied().unwrap_or(0)
                    >= cap.saturating_mul(AGGREGATE_CAP_MULT)
            {
                return None;
            }
        }
        *cap_counts.entry(cap_bucket).or_insert(0) += 1;
        *share_counts.entry(share_bucket).or_insert(0) += 1;
        Some(ConnGuard {
            cap_bucket,
            share_bucket,
            counts: self.counts.clone(),
        })
    }

    /// Conns held under `addr`'s share bucket — the fair-share
    /// numerator (v6 answers for its whole /48).
    pub fn held(&self, addr: IpAddr) -> usize {
        let share = share_bucket_addr(addr);
        self.counts
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .1
            .get(&share)
            .copied()
            .unwrap_or(0)
    }

    /// Distinct share buckets currently holding conns — the fair-share
    /// denominator.
    pub fn buckets(&self) -> usize {
        self.counts
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .1
            .len()
    }
}

#[derive(Debug)]
pub struct ConnGuard {
    cap_bucket: IpAddr,
    share_bucket: IpAddr,
    counts: Counts,
}

impl ConnGuard {
    /// The conn's fair-share bucket — the victim ordering keys on it.
    pub fn share_bucket(&self) -> IpAddr {
        self.share_bucket
    }
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        let mut counts = self.counts.lock().unwrap_or_else(|p| p.into_inner());
        let (cap_counts, share_counts) = &mut *counts;
        for (map, bucket) in [
            (&mut *cap_counts, self.cap_bucket),
            (&mut *share_counts, self.share_bucket),
        ] {
            match map.get_mut(&bucket) {
                Some(n) if *n > 1 => *n -= 1,
                _ => {
                    map.remove(&bucket);
                }
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

    /// `2001:db8:0:<n>::1` — the n-th /64 of one /48.
    fn slash64(n: u32) -> IpAddr {
        let n = (n % 65_536) as u16;
        IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, n, 0, 0, 0, 1))
    }

    /// Re-review S2-2: 70,000 requests from distinct /64s of ONE /48 used
    /// to insert a /64 entry before the aggregate refused — the table
    /// filled (65,536/65,536) and new real clients shared the overflow.
    /// Judged aggregate-first with a per-/48 cap, the flood leaves the
    /// table almost empty and 200 new clients each get their own bucket.
    #[test]
    fn a_slash48_spray_leaves_room_for_new_clients() {
        let mut lim = RateLimit::new(5.0, MAX_LIMITED_ADDRS, 64);
        let t0 = Instant::now();
        let mut admitted = 0;
        for i in 0..70_000u32 {
            if lim
                .check(slash64(i), t0 + Duration::from_micros(u64::from(i)))
                .is_ok()
            {
                admitted += 1;
            }
        }
        assert!(admitted <= 60, "the /48 aggregate held: {admitted}");
        assert!(
            lim.tracked() <= MAX_V6_PER_AGG + 2,
            "one /48 filled the table: {} entries",
            lim.tracked()
        );
        let t1 = t0 + Duration::from_millis(100);
        let passed = (0..200u32)
            .filter(|i| {
                let a = IpAddr::from([198, 51, (100 + i / 200) as u8, (i % 200) as u8]);
                lim.check(a, t1).is_ok()
            })
            .count();
        assert_eq!(passed, 200, "new clients must not share an overflow bucket");
    }

    /// Past `MAX_V6_PER_AGG` /64 entries a /48's further /64s share one
    /// bucket — admitted at the address rate, never new entries.
    #[test]
    fn a_slash48_holds_at_most_its_cap_of_slash64_entries() {
        // A generous aggregate so the per-/48 cap is what binds.
        let mut lim = RateLimit::new(1_000.0, MAX_LIMITED_ADDRS, 64);
        let t0 = Instant::now();
        for i in 0..(MAX_V6_PER_AGG as u32 + 500) {
            let _ = lim.check(slash64(i), t0 + Duration::from_millis(u64::from(i)));
        }
        assert_eq!(lim.per_agg.values().copied().max(), Some(MAX_V6_PER_AGG));
        assert!(lim.tracked() <= MAX_V6_PER_AGG + 2, "{}", lim.tracked());
    }

    /// A refused flood never keeps its entry young: past a minute of only
    /// refusals the entry is reclaimable, and a new client takes its slot.
    /// The scan itself runs at most once per `RECLAIM_EVERY`.
    #[test]
    fn refusals_do_not_refresh_idleness_and_the_scan_is_rate_limited() {
        // One token per 1000 s: after its first request the flooder is
        // refused for the whole minute.
        let mut lim = RateLimit::new(0.001, 2, 64);
        let t0 = Instant::now();
        let flooder = ip("192.0.2.1");
        let other = ip("192.0.2.2");
        assert!(lim.check(flooder, t0).is_ok());
        assert!(lim.check(other, t0).is_ok());
        for ms in (1..61_000u64).step_by(5) {
            assert!(lim.check(flooder, t0 + Duration::from_millis(ms)).is_err());
        }
        // Both entries are a minute idle: the refused one included.
        let t1 = t0 + Duration::from_secs(62);
        let new = ip("192.0.2.3");
        assert!(lim.check(new, t1).is_ok());
        assert!(
            lim.addrs.contains_key(&lim.addr_key(new)),
            "new took a slot"
        );
        assert!(
            !lim.addrs.contains_key(&lim.addr_key(flooder)),
            "the refused flooder kept its slot by being refused"
        );
        // A thousand more misses inside the same second: one scan, not 1,000.
        let before = lim.reclaims;
        for i in 0..1000u32 {
            let a = IpAddr::from([203, 0, (i >> 8) as u8, i as u8]);
            let _ = lim.check(a, t1 + Duration::from_micros(u64::from(i)));
        }
        assert!(
            lim.reclaims - before <= 1,
            "{} scans",
            lim.reclaims - before
        );
    }

    /// Probe (release, ignored): the per-check cost of the re-review S2-2
    /// flood — 70k distinct /64s of one /48 into a full-size table, then
    /// 100k misses from fresh IPv4 addresses into a table full of active
    /// entries (before the fix each miss scanned the whole table).
    ///   cargo test --release -p durable-server --bin durable-server -- --ignored --nocapture limiter_cost
    #[test]
    #[ignore = "probe: run in release with --nocapture"]
    fn limiter_cost_under_a_slash48_flood_and_a_miss_flood() {
        let mut lim = RateLimit::new(5.0, MAX_LIMITED_ADDRS, 64);
        let t0 = Instant::now();
        let start = std::time::Instant::now();
        for i in 0..70_000u32 {
            let _ = lim.check(slash64(i), t0 + Duration::from_micros(u64::from(i)));
        }
        let flood = start.elapsed().as_nanos() as f64 / 70_000.0;
        // Fill the table with active entries, then miss.
        let mut full = RateLimit::new(1_000.0, MAX_LIMITED_ADDRS, 64);
        for i in 0..MAX_LIMITED_ADDRS as u32 {
            let _ = full.check(
                IpAddr::from([10, (i >> 16) as u8, (i >> 8) as u8, i as u8]),
                t0,
            );
        }
        let start = std::time::Instant::now();
        for i in 0..100_000u32 {
            let a = IpAddr::from([11, (i >> 16) as u8, (i >> 8) as u8, i as u8]);
            let _ = full.check(a, t0 + Duration::from_micros(u64::from(i)));
        }
        let miss = start.elapsed().as_nanos() as f64 / 100_000.0;
        println!(
            "limiter: {flood:.0} ns/check in the /48 flood ({} entries after), \
             {miss:.0} ns/check per miss on a full table",
            lim.tracked()
        );
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
