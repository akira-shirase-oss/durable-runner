//! Password hashing: argon2id PHC strings at the OWASP floor
//! (m = 19 MiB, t = 2, p = 1). Both operations are pure CPU and hold
//! tens of MiB — callers run them on `spawn_blocking` behind a
//! semaphore, never on an async worker (the wrapper lives in
//! durable-server).
//!
//! The constant-work property for unknown accounts comes from the
//! caller: login verifies `DUMMY_PHC` when the address has no account,
//! so "this email exists" is not a timing oracle.

use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::{Algorithm, Argon2, Params, Version};

/// Memory cost in KiB (19 MiB), iterations, lanes — the fixed set.
const M_KIB: u32 = 19_456;
const T: u32 = 2;
const P: u32 = 1;

/// A stored PHC's parameters are not trusted either: a row that somehow
/// carried inflated costs would turn one verify into a hash bomb. Costs
/// beyond the floor are refused outright — our own writes always use
/// the fixed set.
const MAX_M_KIB: u32 = 65_536;
const MAX_T: u32 = 8;
const MAX_P: u32 = 4;

fn argon2() -> Argon2<'static> {
    // The fixed parameter set is always valid; a panic here means the
    // argon2 crate itself rejected OWASP's floor — an impossible
    // condition, reported as one.
    let params = Params::new(M_KIB, T, P, Some(Params::DEFAULT_OUTPUT_LEN))
        .unwrap_or_else(|e| panic!("invariant: the argon2id parameter set is valid: {e}"));
    Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
}

/// The PHC string for `password`. `Err` is a broken RNG — no hash is
/// issued rather than a weak one.
pub fn hash_password(password: &str) -> Result<String, HashError> {
    // A 16-byte CSPRNG salt from the workspace's existing getrandom —
    // argon2's `rand` feature would pull rand_core/getrandom for the
    // same bytes.
    let mut salt_bytes = [0u8; 16];
    getrandom::fill(&mut salt_bytes).map_err(|e| HashError(format!("cannot draw a salt: {e}")))?;
    let salt = SaltString::encode_b64(&salt_bytes)
        .map_err(|e| HashError(format!("cannot encode a salt: {e}")))?;
    argon2()
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| HashError(e.to_string()))
}

/// Verify `password` against a PHC string. Always runs argon2id on the
/// PHC's own (bounded) parameters; a malformed or inflated PHC is a
/// `false`, never a panic and never a cheap skip.
pub fn verify_password(phc: &str, password: &str) -> bool {
    let Ok(hash) = PasswordHash::new(phc) else {
        return false;
    };
    if hash.algorithm != argon2::ARGON2ID_IDENT {
        return false;
    }
    let cost = |name: &str| hash.params.get_decimal(name).unwrap_or(u32::MAX);
    if cost("m") > MAX_M_KIB || cost("t") > MAX_T || cost("p") > MAX_P {
        return false;
    }
    argon2().verify_password(password.as_bytes(), &hash).is_ok()
}

/// A valid argon2id PHC for a fixed dummy input — used when a login
/// names an address with no account so the attempt costs the same work
/// as a real verify. Generated once at implementation time; the secret
/// it hashes is not a secret.
pub const DUMMY_PHC: &str = DUMMY_PHC_VALUE;
const DUMMY_PHC_VALUE: &str = "$argon2id$v=19$m=19456,t=2,p=1$HM6oqziEcKi55qzjvmgn+A$6Ei1bGRVxIbGJwt3aNM7+bgDGJuhEY9QaSuFJ7tkXQw";

/// A typed error for hashing — callers map it to "internal", it never
/// reaches a response body (it carries no useful detail anyway).
#[derive(Debug, thiserror::Error)]
#[error("password hash failed: {0}")]
pub struct HashError(String);

/// Password policy (NIST-aligned): 12–128 characters, UTF-8 by Rust's
/// own string type, and a small common-password denylist — no
/// composition rules. Returns the refusal message, or `None` to admit.
pub fn policy_refusal(password: &str) -> Option<&'static str> {
    let chars = password.chars().count();
    if chars < 12 {
        return Some("password must be at least 12 characters");
    }
    if chars > 128 {
        return Some("password must be at most 128 characters");
    }
    let lower = password.to_lowercase();
    if COMMON.iter().any(|c| *c == lower) {
        return Some("password is on the common-password denylist");
    }
    None
}

/// A small denylist — the point is catching the trivially guessable,
/// not enumerating the internet. Compared lower-cased.
const COMMON: &[&str] = &[
    "password",
    "password1",
    "password123",
    "123456789012",
    "qwerty123456",
    "letmein12345",
    "iloveyou1234",
    "admin123456789",
    "welcome12345",
    "changeme1234",
    "trustno1trust",
    "durable-runner",
    "durable runner",
    "durablerunner",
    "abc123abc123",
    "1q2w3e4r5t6y",
    "zaq12wsxcde3",
    "p@ssw0rdp@ssw0rd",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_hash_verifies_and_a_wrong_password_does_not() {
        let phc =
            hash_password("correct horse battery staple").unwrap_or_else(|e| panic!("hash: {e}"));
        assert!(phc.starts_with("$argon2id$v=19$m=19456,t=2,p=1$"));
        assert!(verify_password(&phc, "correct horse battery staple"));
        assert!(!verify_password(&phc, "correct horse battery staple!"));
    }

    #[test]
    fn the_dummy_phc_is_a_real_argon2id_hash() {
        // DUMMY_PHC must verify (running argon2, not skipping it) or the
        // unknown-account path is cheaper than the real one — a timing
        // oracle by construction.
        assert!(verify_password(DUMMY_PHC, "durable-runner-dummy-password"));
        assert!(!verify_password(DUMMY_PHC, "anything else"));
    }

    #[test]
    fn malformed_and_inflated_phcs_are_false_not_panics() {
        for bad in [
            "",
            "not-a-phc",
            "$argon2id$v=19$m=99999999,t=2,p=1$c2FsdA$AAAA",
            "$bcrypt$10$whatever",
            "$argon2id$v=19$m=19456,t=2,p=1$%%%$%%%",
        ] {
            assert!(!verify_password(bad, "x"), "{bad:?} must not verify");
        }
    }

    #[test]
    fn the_policy_refuses_short_long_and_common() {
        assert!(policy_refusal("short").is_some());
        assert!(policy_refusal(&"x".repeat(129)).is_some());
        assert!(policy_refusal("Password123").is_some());
        assert!(policy_refusal("a perfectly decent passphrase").is_none());
    }
}
