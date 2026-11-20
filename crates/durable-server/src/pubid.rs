//! Opaque public run ids (S2 review #7).
//!
//! The internal run sequence is global, so a raw `run_id` tells a tenant
//! how much work every other workspace has submitted since their last
//! call. Public ids are a *keyed 64-bit permutation* of the internal id:
//! an eight-round Feistel network over HMAC-SHA256. Because it is a
//! permutation it needs no mapping table and touches no journal record;
//! because it is keyed, two submits from one workspace produce
//! uncorrelated strings no matter what happened in between elsewhere.
//!
//! The key is a 32-byte secret at `<journal>/public_id.key`, generated
//! mode-0600 on first boot and reused after. Losing it orphans every
//! public id (the runs themselves are untouched); rotating it renames
//! every run, so it is generated once and never rewritten — a journal
//! that already holds runs but no key fails loudly rather than mint a
//! second identity for them.

use base64::Engine as _;
use durable_domain::RunId;
use ring::hmac;
use std::io;
use std::path::Path;

const KEY_FILE: &str = "public_id.key";
const KEY_TMP: &str = "public_id.key.tmp";

/// Feistel rounds: four suffice for a strong PRP in theory; eight keeps
/// the margin against the 32-bit-half birthday bound a reviewer flagged
/// (~2^16 observations) without a mapping table or a larger primitive.
const ROUNDS: u8 = 8;

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
    ///
    /// A journal that already holds committed content but no key refuses:
    /// silently generating a fresh key would rename every run id a client
    /// ever saw — the missing key is data loss to surface, not to hide.
    pub fn load_or_generate(journal_dir: &Path) -> io::Result<Self> {
        let mut d = std::fs::DirBuilder::new();
        d.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            d.mode(0o700);
        }
        d.create(journal_dir)?;
        // A crash mid-create leaves the tmp file; it is never a key.
        let tmp = journal_dir.join(KEY_TMP);
        if tmp.exists() {
            std::fs::remove_file(&tmp)?;
        }
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
                if journal_has_content(journal_dir)? {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "{}: journal has committed runs but no public-id key — \
                             restore the key from backup; generating one would rename every run",
                            path.display()
                        ),
                    ));
                }
                let mut b = [0u8; 32];
                ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut b)
                    .map_err(|_| io::Error::other("system rng failed"))?;
                if write_secret(&tmp, &path, &b)? {
                    b.to_vec()
                } else {
                    // Another creator's link won — use their key.
                    std::fs::read(&path)?
                }
            }
            Err(e) => return Err(e),
        };
        // An existing key file re-tightens to owner-only: an older or
        // hand-restored key is never left world-readable (S3).
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(&path)?.permissions().mode() & 0o777;
            if mode != 0o600 {
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
            }
        }
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

/// True when the journal already holds committed content — any non-empty
/// file inside a `gen-*` directory (a records tail or a snapshot). A
/// journal this fresh has no manifest to consult; presence of bytes is
/// the conservative signal.
fn journal_has_content(dir: &Path) -> io::Result<bool> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        if !name.to_string_lossy().starts_with("gen-") || !entry.file_type()?.is_dir() {
            continue;
        }
        for inner in std::fs::read_dir(entry.path())? {
            let inner = inner?;
            if inner.file_type()?.is_file() && inner.metadata()?.len() > 0 {
                return Ok(true);
            }
        }
    }
    Ok(false)
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
    for i in 0..ROUNDS {
        (l, r) = (r, l ^ f(key, i, r));
    }
    ((l as u64) << 32) | r as u64
}

fn unpermute(key: &hmac::Key, x: u64) -> u64 {
    let (mut l, mut r) = ((x >> 32) as u32, x as u32);
    for i in (0..ROUNDS).rev() {
        (l, r) = (r ^ f(key, i, l), l);
    }
    ((l as u64) << 32) | r as u64
}

/// Write the key atomically and without clobbering: tmp file → fsync →
/// hard-link onto the real path (the link fails if a key exists, so two
/// first-boots can never end with different keys) → dir fsync (S3: a
/// crash mid-create must leave either nothing or a whole key, and an
/// existing key is never replaced). Returns `false` when another
/// creator's key won the link — the caller then reads that file.
fn write_secret(tmp: &Path, path: &Path, bytes: &[u8]) -> io::Result<bool> {
    use std::io::Write;
    let mut o = std::fs::OpenOptions::new();
    o.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        o.mode(0o600);
    }
    let mut f = o.open(tmp)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    drop(f);
    match std::fs::hard_link(tmp, path) {
        Ok(()) => {
            let _ = std::fs::remove_file(tmp);
        }
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
            let _ = std::fs::remove_file(tmp);
            return Ok(false);
        }
        Err(e) => return Err(e),
    }
    if let Some(dir) = path.parent() {
        std::fs::File::open(dir)?.sync_all()?;
    }
    Ok(true)
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

    /// S3: the key write is atomic — a leftover `.tmp` is swept, never
    /// mistaken for a key, and two creations can't clobber each other.
    #[test]
    fn a_leftover_tmp_is_swept_and_never_read() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        std::fs::write(d.path().join(KEY_TMP), b"torn").unwrap_or_else(|e| panic!("{e}"));
        let p = PubId::load_or_generate(d.path()).unwrap_or_else(|e| panic!("{e}"));
        assert!(!d.path().join(KEY_TMP).exists(), "tmp swept");
        let key = std::fs::read(d.path().join(KEY_FILE)).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(key.len(), 32);
        assert_eq!(p.decode(&p.encode(RunId(7))), Some(7));
    }

    /// S3: an existing loose-permission key is tightened on load.
    #[cfg(unix)]
    #[test]
    fn a_loose_key_is_tightened_on_load() {
        use std::os::unix::fs::PermissionsExt as _;
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let path = d.path().join(KEY_FILE);
        std::fs::write(&path, [9u8; 32]).unwrap_or_else(|e| panic!("{e}"));
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))
            .unwrap_or_else(|e| panic!("{e}"));
        PubId::load_or_generate(d.path()).unwrap_or_else(|e| panic!("{e}"));
        let mode = std::fs::metadata(&path)
            .unwrap_or_else(|e| panic!("{e}"))
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "the loaded key is owner-only");
    }

    /// S3: when another creator's link wins, `write_secret` reports the
    /// loss — the caller reads the winner's bytes, not its own, so two
    /// first-boots can never end on different keys.
    #[test]
    fn a_losing_creator_reads_the_winners_key() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let path = d.path().join(KEY_FILE);
        let winner = [7u8; 32];
        std::fs::write(&path, winner).unwrap_or_else(|e| panic!("{e}"));
        let mine = [9u8; 32];
        let tmp = d.path().join(KEY_TMP);
        assert!(
            !write_secret(&tmp, &path, &mine).unwrap_or_else(|e| panic!("{e}")),
            "an existing key beats the link attempt"
        );
        assert_eq!(
            std::fs::read(&path).unwrap_or_else(|e| panic!("{e}")),
            winner,
            "the winner's key is what stands"
        );
    }

    /// S3/N6: a journal holding runs but no key refuses to mint one —
    /// generation would rename every run id already issued.
    #[test]
    fn a_journal_with_runs_refuses_to_mint_a_second_key() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let gen = d.path().join("gen-00000001");
        std::fs::create_dir(&gen).unwrap_or_else(|e| panic!("{e}"));
        std::fs::write(gen.join("records"), b"one committed record")
            .unwrap_or_else(|e| panic!("{e}"));
        let e = match PubId::load_or_generate(d.path()) {
            Err(e) => e,
            Ok(_) => panic!("a non-empty journal must refuse a fresh key"),
        };
        assert_eq!(e.kind(), io::ErrorKind::InvalidData);
        assert!(
            e.to_string().contains("no public-id key"),
            "the refusal says what to do: {e}"
        );
        assert!(!d.path().join(KEY_FILE).exists(), "no key was generated");
    }
}
