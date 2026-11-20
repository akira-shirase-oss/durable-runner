//! Opaque public run ids (S2 review #7).
//!
//! The internal run sequence is global, so a raw `run_id` tells a tenant
//! how much work every other workspace has submitted since their last
//! call. Public ids are a *keyed 64-bit permutation* of the internal id:
//! a four-round Feistel network over HMAC-SHA256. Because it is a
//! permutation it needs no mapping table and touches no journal record;
//! because it is keyed, two submits from one workspace produce
//! uncorrelated strings no matter what happened in between elsewhere.
//!
//! The key is a 32-byte secret at `<journal>/public_id.key`, generated
//! mode-0600 on first boot and reused after. Losing it orphans every
//! public id (the runs themselves are untouched); rotating it renames
//! every run, so it is generated once and never rewritten.

use base64::Engine as _;
use durable_domain::RunId;
use ring::hmac;
use std::io;
use std::path::Path;

const KEY_FILE: &str = "public_id.key";

/// The keyed permutation. Cheap to clone (the key is fixed at open).
#[derive(Clone)]
pub struct PubId {
    key: hmac::Key,
}

impl PubId {
    /// Load the key file or create it (0600) when absent — same lifecycle
    /// as the journal it sits beside, and deliberately outside it so no
    /// journal record changes shape. The journal dir may not exist yet
    /// (first boot): create it owner-only, the mode the journal uses.
    pub fn load_or_generate(journal_dir: &Path) -> io::Result<Self> {
        let mut d = std::fs::DirBuilder::new();
        d.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            d.mode(0o700);
        }
        d.create(journal_dir)?;
        let path = journal_dir.join(KEY_FILE);
        let bytes = match std::fs::read(&path) {
            Ok(b) if b.len() == 32 => b,
            // A wrong-length key file is not silently replaced (that
            // would rename every run): fail loudly like a corrupt secret.
            Ok(b) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{}: {} bytes, expected 32", path.display(), b.len()),
                ));
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                let mut b = [0u8; 32];
                ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut b)
                    .map_err(|_| io::Error::other("system rng failed"))?;
                write_secret(&path, &b)?;
                b.to_vec()
            }
            Err(e) => return Err(e),
        };
        Ok(Self {
            key: hmac::Key::new(hmac::HMAC_SHA256, &bytes),
        })
    }

    /// Test/ephemeral construction — the same permutation, a key that
    /// dies with the caller.
    #[cfg(test)]
    pub fn ephemeral() -> Self {
        let mut b = [0u8; 32];
        ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut b)
            .unwrap_or_else(|_| panic!("rng"));
        Self {
            key: hmac::Key::new(hmac::HMAC_SHA256, &b),
        }
    }

    /// The public form of an internal id: base64url(no pad) of the
    /// permuted u64 — 11 chars, url-safe, fixed width.
    pub fn encode(&self, id: RunId) -> String {
        let x = permute(&self.key, id.0);
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(x.to_be_bytes())
    }

    /// Back to the internal id. Any malformed, truncated, or forged
    /// string maps to `None`; callers answer it exactly like an id that
    /// was never issued (no oracle — the permutation is total).
    pub fn decode(&self, s: &str) -> Option<u64> {
        let b: [u8; 8] = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(s)
            .ok()?
            .try_into()
            .ok()?;
        Some(unpermute(&self.key, u64::from_be_bytes(b)))
    }
}

/// One Feistel round function: 32 bits of HMAC(key, domain‖round‖half).
fn f(key: &hmac::Key, round: u8, half: u32) -> u32 {
    let mut ctx = hmac::Context::with_key(key);
    ctx.update(b"pubid");
    ctx.update(&[round]);
    ctx.update(&half.to_be_bytes());
    let t = ctx.sign();
    u32::from_be_bytes([t.as_ref()[0], t.as_ref()[1], t.as_ref()[2], t.as_ref()[3]])
}

fn permute(key: &hmac::Key, x: u64) -> u64 {
    let (mut l, mut r) = ((x >> 32) as u32, x as u32);
    for i in 0..4u8 {
        (l, r) = (r, l ^ f(key, i, r));
    }
    ((l as u64) << 32) | r as u64
}

fn unpermute(key: &hmac::Key, x: u64) -> u64 {
    let (mut l, mut r) = ((x >> 32) as u32, x as u32);
    for i in (0..4u8).rev() {
        (l, r) = (r ^ f(key, i, l), l);
    }
    ((l as u64) << 32) | r as u64
}

/// Write the key file private-at-creation (0600 before the bytes land)
/// and fsynced — a half-written key would rename every run.
fn write_secret(path: &Path, bytes: &[u8]) -> io::Result<()> {
    use std::io::Write;
    let mut o = std::fs::OpenOptions::new();
    o.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        o.mode(0o600);
    }
    let mut f = o.open(path)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_round_trips_every_id() {
        let p = PubId::ephemeral();
        for id in [1, 2, 3, 255, 256, 4096, 1_000_000, u64::MAX, u64::MAX - 1] {
            let s = p.encode(RunId(id));
            assert_eq!(p.decode(&s), Some(id), "round trip {id}");
            assert_eq!(s.len(), 11, "fixed width {s}");
            assert!(
                !s.chars().any(|c| c.is_ascii_digit()) || s.parse::<u64>().is_err(),
                "no public id parses as a bare integer: {s}"
            );
        }
    }

    #[test]
    fn distinct_ids_are_distinct_publics() {
        let p = PubId::ephemeral();
        let set: std::collections::HashSet<String> =
            (0..10_000).map(|i| p.encode(RunId(i))).collect();
        assert_eq!(set.len(), 10_000, "a permutation has no collisions");
    }

    #[test]
    fn decode_refuses_malformed_strings() {
        let p = PubId::ephemeral();
        for s in ["", "!!!", "a", "123456789012", "42", &"A".repeat(24)] {
            assert_eq!(p.decode(s), None, "{s:?}");
        }
    }

    #[test]
    fn a_second_key_renames_everything() {
        let a = PubId::ephemeral();
        let b = PubId::ephemeral();
        let s = a.encode(RunId(42));
        assert_ne!(b.decode(&s), Some(42));
    }
}
