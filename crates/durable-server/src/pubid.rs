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
//! carrying evidence a public-id binary already ran there but no key
//! fails loudly rather than mint a second identity. A v1.0.x journal
//! has runs but never had the feature: upgrading mints the key once,
//! then the new binary keeps it.

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
    /// A journal that is already public-id era but has no key refuses:
    /// silently generating a fresh key would rename every run id a client
    /// ever saw — the missing key is data loss to surface, not to hide.
    /// A v1.0.x journal holds runs yet is pre-pubid: its ids were never
    /// public, so minting here is the upgrade path, not a rename.
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
                if journal_is_pubid_era(journal_dir)? {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "{}: journal is public-id era but has no public-id key — \
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

    /// Derive a domain-separated HMAC key for the session-token tag
    /// (review #8): one secret file on disk, distinct keys per use.
    pub fn derive_key(&self, domain: &[u8]) -> hmac::Key {
        let tag = hmac::sign(&self.key, domain);
        hmac::Key::new(hmac::HMAC_SHA256, tag.as_ref())
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

/// True when the journal carries evidence a public-id binary has already
/// run here — a `journal.shape` claiming record format ≥ 3 (the sidecar's
/// monotonic max, which covers v3 records compaction pruned into a
/// snapshot), any generation's `journal.log` holding a `"v":3` envelope,
/// or a `snapshot.bin` holding a ws ≠ 0 run (S2-created; v1.0.2 only ever
/// parks them — S3 re-review N4).
///
/// Any other journal — empty, v1/v2 records, no sidecar — is pre-pubid:
/// the upgrade path mints the key there (S1: refusing to mint made every
/// v1.0.x install crash-loop on the S2 binary). A malformed sidecar
/// decides nothing — the record evidence does.
fn journal_is_pubid_era(dir: &Path) -> io::Result<bool> {
    match std::fs::read_to_string(dir.join("journal.shape")) {
        Ok(text) => {
            for line in text.lines() {
                if let Some(v) = line.strip_prefix("records=") {
                    if v.trim().parse::<u64>().unwrap_or(0) >= 3 {
                        return Ok(true);
                    }
                }
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        if !name.to_string_lossy().starts_with("gen-") || !entry.file_type()?.is_dir() {
            continue;
        }
        match std::fs::read(entry.path().join("journal.log")) {
            Ok(bytes) => {
                if bytes.windows(5).any(|w| w == *br#""v":3"#) {
                    return Ok(true);
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        // A compacted journal drops its v3 history into snapshot.bin and
        // is documented as still opening with journal.shape deleted — so
        // the snapshot is the last era evidence left (S3 re-review N4).
        match std::fs::read(entry.path().join("snapshot.bin")) {
            Ok(bytes) => {
                if snapshot_has_tenant_runs(&bytes) {
                    return Ok(true);
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }
    Ok(false)
}

/// Scan a `snapshot.bin` for a run a v1.0.x binary could never have
/// written: ws != 0. v1.0.2 parks tenant runs but only S2 creates them,
/// so one in the snapshot proves the pubid era — a lost key must refuse
/// re-minting rather than rename every public id (S3 re-review N4).
///
/// Layout mirrors `snapshot.rs` (the DRIX payload inside the framed
/// container). A payload that ends mid-record decides nothing — open
/// refuses a corrupt snapshot anyway, so no wrong mint ships.
fn snapshot_has_tenant_runs(bytes: &[u8]) -> bool {
    /// snapshot.bin: magic8 covered_seq u64 len u64 crc u32, then DRIX.
    const FRAME: usize = 8 + 8 + 8 + 4;
    let p = match bytes.get(FRAME..) {
        Some(p) => p,
        None => return false,
    };
    // DRIX header: magic4 | version u16 | next_run u64 | count u64.
    if p.len() < 4 + 2 + 8 + 8 || &p[..4] != b"DRIX" {
        return false;
    }
    if u16::from_be_bytes([p[4], p[5]]) < 2 {
        return false; // version 1 predates ws — nothing to find
    }
    let count = u64::from_be_bytes(p[14..22].try_into().unwrap_or([0; 8]));
    let mut at = 22usize;
    // Field walkers: nothing allocates, a short read ends the scan.
    let u32at = |p: &[u8], at: usize| -> Option<u32> {
        p.get(at..at + 4)
            .map(|b| u32::from_be_bytes(b.try_into().unwrap_or([0; 4])))
    };
    let u64at = |p: &[u8], at: usize| -> Option<u64> {
        p.get(at..at + 8)
            .map(|b| u64::from_be_bytes(b.try_into().unwrap_or([0; 8])))
    };
    for _ in 0..count {
        // run = id u64 | ws u64 | key str | spec u32 | steps u32 ×
        //       (name str, handler str) | input bytes | state | fin u8[+u64]
        let Some(_id) = u64at(p, at) else {
            return false;
        };
        let Some(ws) = u64at(p, at + 8) else {
            return false;
        };
        if ws != 0 {
            return true;
        }
        at += 16;
        let Some(key_len) = u32at(p, at) else {
            return false;
        };
        at += 4 + key_len as usize + 4; // key, then spec.version
        let Some(steps) = u32at(p, at) else {
            return false;
        };
        at += 4;
        for _ in 0..steps {
            for _ in 0..2 {
                let Some(l) = u32at(p, at) else {
                    return false;
                };
                at += 4 + l as usize;
            }
        }
        let Some(input_len) = u32at(p, at) else {
            return false;
        };
        at += 4 + input_len as usize;
        let Some(tag) = p.get(at).copied() else {
            return false;
        };
        at += 1;
        // step/attempt are u32, timestamps u64 (mirrors snapshot.rs).
        at += match tag {
            0 => 8,             // Queued{at}
            1 | 2 => 4 + 4 + 8, // Running / RetryWaiting{step, attempt, t}
            3 => 4 + 4,         // Reconciling{step, attempt}
            5 => 4,             // Failed{step}
            4 | 6 => 0,         // Done / Cancelled
            _ => return false,
        };
        match p.get(at).copied() {
            Some(0) => at += 1,
            Some(1) => at += 9,
            _ => return false,
        }
    }
    false
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

    /// Drop a `journal.log` holding `line` under `gen-000001` — the
    /// bytes the era check reads, no framing needed for it.
    fn journal_log_with(dir: &Path, line: &str) {
        let gen = dir.join("gen-000001");
        std::fs::create_dir(&gen).unwrap_or_else(|e| panic!("{e}"));
        std::fs::write(gen.join("journal.log"), line).unwrap_or_else(|e| panic!("{e}"));
    }

    /// S3/N6: a journal a public-id binary already wrote to (a `"v":3`
    /// envelope is unmistakable — v1.0.x writers top out at `v:2`) but
    /// that lost its key refuses to mint one — generation would rename
    /// every run id already issued.
    #[test]
    fn a_pubid_era_journal_refuses_to_mint_a_second_key() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        journal_log_with(
            d.path(),
            r#"{"v":3,"ws":0,"run":1,"at":1,"event":{"Done":{}}}"#,
        );
        let e = match PubId::load_or_generate(d.path()) {
            Err(e) => e,
            Ok(_) => panic!("a public-id-era journal must refuse a fresh key"),
        };
        assert_eq!(e.kind(), io::ErrorKind::InvalidData);
        assert!(
            e.to_string().contains("no public-id key"),
            "the refusal says what to do: {e}"
        );
        assert!(!d.path().join(KEY_FILE).exists(), "no key was generated");
    }

    /// S1 (finding 2): a v1.0.x journal holds runs but never had public
    /// ids — `load_or_generate` ran before the engine on upgrade and
    /// refused every non-empty journal, so S2 could never start on an
    /// existing install. With no era marker, the key is minted here.
    #[test]
    fn a_v1_journal_mints_the_key_on_upgrade() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        journal_log_with(
            d.path(),
            r#"{"v":2,"run":1,"at":1,"event":{"Accepted":{"key":"k"}}}"#,
        );
        let p = PubId::load_or_generate(d.path()).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            std::fs::read(d.path().join(KEY_FILE))
                .unwrap_or_else(|e| panic!("{e}"))
                .len(),
            32
        );
        assert_eq!(p.decode(&p.encode(RunId(1))), Some(1));
    }

    /// The sidecar's `records` is the max record version — `3` means the
    /// pubid-era writer ran even when compaction pruned the v3 records
    /// into a snapshot, so a missing key still refuses. `records=2` (a
    /// v1.0.2 journal, sidecar era but not pubid era) mints.
    #[test]
    fn the_sidecar_records_field_decides_the_era() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        journal_log_with(d.path(), r#"{"v":2,"run":1,"at":1,"event":{"Done":{}}}"#);
        std::fs::write(
            d.path().join("journal.shape"),
            "records=3\nsnapshots=1\nws=0\n",
        )
        .unwrap_or_else(|e| panic!("{e}"));
        let e = match PubId::load_or_generate(d.path()) {
            Err(e) => e,
            Ok(_) => panic!("records=3 means the pubid writer ran — refuse"),
        };
        assert!(
            e.to_string().contains("no public-id key"),
            "the refusal says what to do: {e}"
        );

        let d2 = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        journal_log_with(d2.path(), r#"{"v":2,"run":1,"at":1,"event":{"Done":{}}}"#);
        std::fs::write(
            d2.path().join("journal.shape"),
            "records=2\nsnapshots=1\nws=3\n",
        )
        .unwrap_or_else(|e| panic!("{e}"));
        PubId::load_or_generate(d2.path()).unwrap_or_else(|e| panic!("{e}"));
        assert!(d2.path().join(KEY_FILE).exists(), "records=2 is pre-pubid");
    }

    /// S3 re-review N4: a compacted pubid-era journal holds its tenant
    /// runs only inside snapshot.bin — a v1.0.x writer can park but
    /// never create a ws != 0 run, so finding one in the snapshot is
    /// unambiguous era evidence even with journal.shape deleted and no
    /// v3 records left in the log. Minting there would rename every id.
    #[test]
    fn a_tenant_run_inside_a_snapshot_still_refuses_a_second_key() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        journal_log_with(d.path(), r#"{"v":2,"run":1,"at":1,"event":{"Done":{}}}"#);
        // DRIX v2 payload: one run, ws = 7, terminal Done, not finished.
        let mut p = b"DRIX".to_vec();
        p.extend(2u16.to_be_bytes()); // version
        p.extend(2u64.to_be_bytes()); // next_run
        p.extend(1u64.to_be_bytes()); // count
        p.extend(1u64.to_be_bytes()); // run id
        p.extend(7u64.to_be_bytes()); // ws — S2 only
        p.extend(1u32.to_be_bytes());
        p.extend(b"k"); // key
        p.extend(1u32.to_be_bytes()); // spec.version
        p.extend(1u32.to_be_bytes()); // steps
        p.extend(1u32.to_be_bytes());
        p.extend(b"s"); // step name
        p.extend(1u32.to_be_bytes());
        p.extend(b"h"); // handler
        p.extend(0u32.to_be_bytes()); // input
        p.push(4); // Done
        p.push(0); // no finished_at
        let mut snap = vec![0u8; 28]; // framed container header
        snap.extend(p);
        std::fs::write(d.path().join("gen-000001").join("snapshot.bin"), snap)
            .unwrap_or_else(|e| panic!("{e}"));
        let e = match PubId::load_or_generate(d.path()) {
            Err(e) => e,
            Ok(_) => panic!("a snapshot-held tenant run proves the era — refuse"),
        };
        assert!(
            e.to_string().contains("no public-id key"),
            "the refusal says what to do: {e}"
        );
        assert!(!d.path().join(KEY_FILE).exists(), "no key was generated");
    }

    /// The same snapshot holding only ws-0 runs is a pre-pubid v1.0.2
    /// journal — it parks nothing its writer did not create, so the
    /// upgrade path still mints.
    #[test]
    fn a_ws0_only_snapshot_still_mints_on_upgrade() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        journal_log_with(d.path(), r#"{"v":2,"run":1,"at":1,"event":{"Done":{}}}"#);
        let mut p = b"DRIX".to_vec();
        p.extend(2u16.to_be_bytes());
        p.extend(2u64.to_be_bytes());
        p.extend(1u64.to_be_bytes());
        p.extend(1u64.to_be_bytes());
        p.extend(0u64.to_be_bytes()); // ws = 0
        p.extend(1u32.to_be_bytes());
        p.extend(b"k");
        p.extend(1u32.to_be_bytes());
        p.extend(1u32.to_be_bytes());
        p.extend(1u32.to_be_bytes());
        p.extend(b"s");
        p.extend(1u32.to_be_bytes());
        p.extend(b"h");
        p.extend(0u32.to_be_bytes());
        p.push(4);
        p.push(0);
        let mut snap = vec![0u8; 28];
        snap.extend(p);
        std::fs::write(d.path().join("gen-000001").join("snapshot.bin"), snap)
            .unwrap_or_else(|e| panic!("{e}"));
        PubId::load_or_generate(d.path()).unwrap_or_else(|e| panic!("{e}"));
        assert!(d.path().join(KEY_FILE).exists(), "ws-0-only is pre-pubid");
    }

    /// A malformed sidecar decides nothing — the record evidence does —
    /// and a v1/v2 log under a damaged shape is still pre-pubid.
    #[test]
    fn a_malformed_sidecar_falls_back_to_record_evidence() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        journal_log_with(d.path(), r#"{"v":2,"run":1,"at":1,"event":{"Done":{}}}"#);
        std::fs::write(d.path().join("journal.shape"), "junk\n").unwrap_or_else(|e| panic!("{e}"));
        PubId::load_or_generate(d.path()).unwrap_or_else(|e| panic!("{e}"));
        assert!(d.path().join(KEY_FILE).exists());
    }
}
