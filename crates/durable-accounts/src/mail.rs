//! Transactional mail (D-064). The outbox row is the source of truth —
//! a mailer only ever *sends* what a row already holds, so a crash or a
//! provider outage is a pending row, never a lost mail.
//!
//! `MemoryMailer` captures sends for dialect-free tests; `LogMailer`
//! logs one metadata line per mail and is the boot default until the
//! Resend mailer lands (S3-1). Mail bodies carry one-time tokens, so a
//! mailer must never log them.

/// One mail as the outbox stores it: the rendered content plus the
/// idempotency key the provider dedupes on. Bodies are plain text and a
/// simple HTML twin — no tracking pixels, no remote assets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mail {
    /// The mail family: verify|reset|invite|notice — the outbox column
    /// is free text, the well-known set is the callers' contract.
    pub kind: &'static str,
    pub send_to: String,
    pub subject: String,
    pub text: String,
    pub html: String,
    pub idempotency_key: String,
}

/// Bounds on what a route may enqueue — a bug in a renderer must not be
/// able to grow the table unboundedly.
pub const MAX_MAIL_SUBJECT: usize = 200;
pub const MAX_MAIL_BODY: usize = 32 * 1024;
/// Pending rows one address may hold; beyond it a new mail is coalesced
/// (the row it would duplicate is still pending — nothing is lost).
pub const MAX_PENDING_PER_ADDR: i64 = 3;

#[derive(Debug, thiserror::Error)]
pub enum MailError {
    /// The provider refused or could not be reached — retryable: the
    /// row stays pending for the next drain.
    #[error("mail send failed: {0}")]
    Send(String),
}

#[async_trait::async_trait]
pub trait Mailer: Send + Sync {
    /// Deliver one outbox mail. `Err` means "not delivered, may retry";
    /// a mailer never decides permanence — that is the caller's
    /// `MailOutcome` bookkeeping.
    async fn send(&self, mail: &Mail) -> Result<(), MailError>;

    /// Downcast seam so tests can read a captured mailbox behind the
    /// trait object.
    fn as_any(&self) -> &dyn std::any::Any;
}

/// What the caller records on the row after attempting a send.
#[derive(Debug, Clone)]
pub enum MailOutcome {
    Sent,
    /// A retryable failure: stamp the error and when to try again
    /// (unix millis — the store keeps the clock out of the trait).
    Retry {
        next_attempt_at_ms: i64,
        error: String,
    },
}

/// Captures every send; never fails. Tests read `captured()` to prove a
/// flow produced exactly the mail it should.
#[derive(Default)]
pub struct MemoryMailer {
    sent: std::sync::Mutex<Vec<Mail>>,
}

impl MemoryMailer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn captured(&self) -> Vec<Mail> {
        self.sent.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

#[async_trait::async_trait]
impl Mailer for MemoryMailer {
    async fn send(&self, mail: &Mail) -> Result<(), MailError> {
        self.sent
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(mail.clone());
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// The boot default: one stderr line per mail, never the body (the
/// verify/reset links carry one-time tokens — a log line must not be
/// able to sign someone in).
pub struct LogMailer;

#[async_trait::async_trait]
impl Mailer for LogMailer {
    async fn send(&self, mail: &Mail) -> Result<(), MailError> {
        eprintln!(
            "durable-server: mail {} to {} (subject {:?})",
            mail.kind, mail.send_to, mail.subject
        );
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
