//! Transactional mail (D-068). The outbox row is the source of truth —
//! a mailer only ever *sends* what a row already holds, so a crash or a
//! provider outage is a pending row, never a lost mail.
//!
//! `MemoryMailer` captures sends for dialect-free tests; `LogMailer`
//! logs one metadata line per mail and is the boot default; the Resend
//! mailer is wired at deploy in the binary (it owns the HTTP client).
//! Mail bodies carry one-time tokens, so a mailer must never log them.

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
    /// When the fact the mail carries dies — the token's expiry for a
    /// link mail, `None` for mails that stay meaningful until sent. A
    /// pending row past it is dropped with its bodies cleared, so a
    /// dead token never sits in the table waiting to leak.
    pub expires_at_ms: Option<i64>,
}

/// Bounds on what a route may enqueue — a bug in a renderer must not be
/// able to grow the table unboundedly.
pub const MAX_MAIL_SUBJECT: usize = 200;
pub const MAX_MAIL_BODY: usize = 32 * 1024;
/// Mails one (address, kind) pair may accrue per hour; beyond it a new
/// mail is coalesced — newest *replaces* the pending row, and rows sent
/// or queued inside the window count toward the cap (review #14: the
/// outbox cannot amplify a spam loop into a mail flood).
pub const MAX_HOURLY_PER_ADDR: i64 = 3;
/// Total delivery attempts before a row is `failed` — each marked
/// retry and each crash-reclaimed `sending` row counts one.
pub const OUTBOX_MAX_ATTEMPTS: i64 = 8;

/// Strict single addr-spec (review #14): the header-injection surface
/// is killed at the grammar — atoms and addr-spec specials only, one
/// `@`, no display names, comments, commas or CRLF. Local part is a
/// dot-atom; domain labels are alnum-hyphen atoms.
pub fn valid_addr_spec(s: &str) -> bool {
    let Some((local, domain)) = s.split_once('@') else {
        return false;
    };
    if s.len() > 254 || local.is_empty() || local.len() > 64 || domain.is_empty() {
        return false;
    }
    let atom = |b: u8| {
        b.is_ascii_alphanumeric()
            || matches!(
                b,
                b'!' | b'#'
                    | b'$'
                    | b'%'
                    | b'&'
                    | b'\''
                    | b'*'
                    | b'+'
                    | b'-'
                    | b'/'
                    | b'='
                    | b'?'
                    | b'^'
                    | b'_'
                    | b'`'
                    | b'{'
                    | b'|'
                    | b'}'
                    | b'~'
            )
    };
    // Dot-atom: atoms joined by single dots, never leading/trailing/`.`-doubled.
    // `extra` must permit the '.' separator itself plus the part's own
    // specials ('-' for domain labels).
    let dot_atom = |part: &str, extra: fn(u8) -> bool| {
        !part.starts_with('.')
            && !part.ends_with('.')
            && !part.contains("..")
            && part.bytes().all(|b| atom(b) || b == b'.' || extra(b))
    };
    dot_atom(local, |_| false)
        && dot_atom(domain, |b| b == b'-')
        && domain
            .split('.')
            .all(|l| !l.starts_with('-') && !l.ends_with('-'))
}

/// HTML-escape a value interpolated into a mail body (review #14) —
/// the origin and any address text the template carries.
pub fn html_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

#[derive(Debug, thiserror::Error)]
pub enum MailError {
    /// The provider refused or could not be reached — retryable: the
    /// row stays pending for the next drain.
    #[error("mail send failed: {0}")]
    Send(String),
    /// The provider permanently refused the mail (a 4xx that cannot
    /// succeed on retry) — the caller marks the row `failed`.
    #[error("mail refused: {0}")]
    Refused(String),
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
    /// A permanent refusal (provider 4xx): the row is `failed` for
    /// inspection — retrying a refused payload can never succeed.
    Failed {
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

/// Stderr mailer. `new()` — the boot default — logs one metadata line
/// per mail, never the body (the verify/reset links carry one-time
/// tokens — a log line must not be able to sign someone in).
/// `with_bodies()` is the dev mailer (`DURABLE_MAILER=log`): it prints
/// the full body because the verify link *is* how a Resend-less dev
/// box tests the flow; it is never the deploy default.
pub struct LogMailer {
    bodies: bool,
}

impl Default for LogMailer {
    fn default() -> Self {
        Self::new()
    }
}

impl LogMailer {
    pub fn new() -> Self {
        Self { bodies: false }
    }

    /// Dev-only (`DURABLE_MAILER=log`): bodies — including mailed
    /// links — go to stderr.
    pub fn with_bodies() -> Self {
        Self { bodies: true }
    }
}

#[async_trait::async_trait]
impl Mailer for LogMailer {
    async fn send(&self, mail: &Mail) -> Result<(), MailError> {
        if self.bodies {
            eprintln!(
                "durable-server: mail {} to {}\nsubject: {}\n\n{}\n",
                mail.kind, mail.send_to, mail.subject, mail.text
            );
        } else {
            eprintln!(
                "durable-server: mail {} to {} (subject {:?})",
                mail.kind, mail.send_to, mail.subject
            );
        }
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
