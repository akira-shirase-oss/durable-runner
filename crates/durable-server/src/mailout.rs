//! Mailer selection at boot (D-068): `RESEND_API_KEY` wires the Resend
//! provider, `DURABLE_MAILER=log` the dev body-printing LogMailer, and
//! the default is the metadata-only LogMailer. The Resend mailer is
//! deploy-only — tests drive `MemoryMailer` and `drain_outbox_once`,
//! so no test path ever dials the provider.
//!
//! Dedupe: the outbox row's claim (`pending → sending`) is the only
//! dedupe we can guarantee — the provider API docs are not readable
//! offline, so no `Idempotency-Key` contract is asserted. A crash
//! between send and mark can therefore re-send; at-least-once already
//! admits that (the attempt cap bounds it).

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use durable_accounts::{Mail, MailError, Mailer};

/// `POST /emails` — the only call the provider side ever makes.
const RESEND_ENDPOINT: &str = "https://api.resend.com/emails";

/// HTTPS + Bearer key. The API key comes from `RESEND_API_KEY` (env —
/// never argv or logs); the From address from `DURABLE_MAIL_FROM`,
/// required together: a boot with one and not the other is a
/// misconfiguration, not a runtime surprise.
pub(crate) struct ResendMailer {
    client: reqwest::Client,
    key: String,
    from: String,
}

impl ResendMailer {
    fn new(key: String, from: String) -> Self {
        Self {
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(5))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
            key,
            from,
        }
    }
}

#[async_trait::async_trait]
impl Mailer for ResendMailer {
    async fn send(&self, mail: &Mail) -> Result<(), MailError> {
        // Only ids/statuses are ever handled — the body's tokens are
        // for the recipient's mailbox, not our logs or error strings.
        let res = self
            .client
            .post(RESEND_ENDPOINT)
            .bearer_auth(&self.key)
            .json(&serde_json::json!({
                "from": self.from,
                "to": [mail.send_to],
                "subject": mail.subject,
                "text": mail.text,
                "html": mail.html,
            }))
            .send()
            .await
            .map_err(|e| MailError::Send(format!("resend request: {e}")))?;
        let status = res.status();
        if status.is_success() {
            return Ok(());
        }
        // A 4xx cannot succeed on retry — the row is `failed`. The body
        // is provider error text (bounded, no secrets), never the mail.
        let detail = res.text().await.unwrap_or_default();
        let detail = detail.chars().take(300).collect::<String>();
        if status.is_client_error() {
            Err(MailError::Refused(format!("resend {status}: {detail}")))
        } else {
            Err(MailError::Send(format!("resend {status}: {detail}")))
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Pick the mailer from the environment (plan "Transactional email"):
/// `RESEND_API_KEY` (+ `DURABLE_MAIL_FROM`) → Resend;
/// `DURABLE_MAILER=log` → the dev body-printer; otherwise the
/// metadata-only LogMailer so a default deploy can never leak tokens
/// into logs.
pub(crate) fn mailer_from_env() -> Result<Arc<dyn Mailer>> {
    if let Ok(key) = std::env::var("RESEND_API_KEY") {
        let from = std::env::var("DURABLE_MAIL_FROM")
            .context("RESEND_API_KEY is set but DURABLE_MAIL_FROM is not")?;
        if key.trim().is_empty() || from.trim().is_empty() {
            anyhow::bail!("RESEND_API_KEY / DURABLE_MAIL_FROM must be non-empty");
        }
        return Ok(Arc::new(ResendMailer::new(key, from)));
    }
    if std::env::var("DURABLE_MAILER").is_ok_and(|v| v == "log") {
        return Ok(Arc::new(durable_accounts::LogMailer::with_bodies()));
    }
    Ok(Arc::new(durable_accounts::LogMailer::new()))
}

/// `DURABLE_MAIL_DAILY_BUDGET` — the global outbound tripwire's cap;
/// absent or unparseable falls back to the built-in default.
pub(crate) fn mail_budget_from_env(default: u32) -> u32 {
    std::env::var("DURABLE_MAIL_DAILY_BUDGET")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
