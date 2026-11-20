# Writing a handler

A handler is an HTTP endpoint the runner calls when a step executes.
This page is the whole contract: what arrives, what you answer, and how
to prove the call came from your runner.

## The call

`POST <registered URL>` with body

```json
{"input": "<the step's input, JSON-encoded>", "idem_key": "<op_key>/<step index>"}
```

and headers:

| header | meaning |
|---|---|
| `x-idempotency-key` | The same key, repeated as a header. |
| `x-durable-run-id` | The run this call belongs to. |
| `x-durable-step` | The step's index in the spec. |
| `x-durable-attempt` | The attempt number (1 = first try). |
| `x-durable-timestamp` | Unix seconds when the call was signed. |
| `x-durable-signature` | Hex HMAC-SHA256 — see below. |

Answer `2xx` with the step's output as the body and the step completes.
Any other status fails the attempt (it is retried under the *same*
idempotency key, up to the run's retry bound). A transport error or
timeout is `unknown` — the runner reconciles instead of assuming.

## The reconcile probe

After a lost reply the runner asks `GET <registered
URL>?idem_key=<key>`; `200` means "the effect landed, do not repeat
it". The probe carries the same run, step and attempt headers (the
attempt being reconciled) and is signed exactly like a call.

## Signature verification (S2-6)

Every call from a store-registered handler is signed with the
workspace's 32-byte secret — the value shown once when the workspace is
created (`POST /admin/workspaces` → `handler_secret`) or rotated (`POST
/handler-secret`). The signature covers:

```
ts.run.step.attempt.idem_key.body_sha256
```

joined with `.`, where `ts` is the `x-durable-timestamp` value,
`run`/`step`/`attempt` are the `x-durable-run-id`/`x-durable-step`/
`x-durable-attempt` header values *verbatim* (and `-` when a header is
absent), and `body_sha256` is the lowercase hex SHA-256 of the exact
request body — for a probe (no body) the digest field is empty, so the
canonical string ends in `.`.

```python
canonical = f"{ts}.{run}.{step}.{attempt}.{idem_key}.{body_sha256_hex}"
expected  = hmac_sha256(secret, canonical).hexdigest()
```

A verifying handler checks, in order:

1. **Freshness.** Reject `abs(now − ts) > 300` seconds.
2. **Signature.** Recompute over the wire fields as received; compare
   in constant time. Reject on mismatch.
3. **Replay.** `(ts, idem_key)` must not have been seen before — a
   bounded set: entries older than 600 s can be evicted, since the
   freshness check already refuses them.

Only then believe the body. `crates/durable-server/src/main.rs` carries
the reference check (`verify_sig`, in the test module) plus a live
fixture (`spawn_verifier`) exercised by the signed-call tests —
rejects proven: tampered body, stale timestamp, wrong secret, replayed
`(ts, idem_key)`.

## Rotation

`POST /handler-secret` (workspace admin key) draws a fresh secret and
returns it once, in the response. The old secret dies immediately:
every registration is re-armed in the engine and the on-disk snapshot
in the same request. A call already in flight under the old secret
fails the handler's check honestly (`401` → a failed attempt → retried,
now signed with the new secret). Copy the new secret to your handler at
rotation time; the runner never returns it a second time and no `GET`
or diagnostics endpoint carries it.

## What is not signed

Compat mode (no `DATABASE_URL`) and `--handler` flag registrations send
no signature headers — flag registrations are the operator's own
wiring, trusted by deployment. A verifying handler should therefore be
deployed only behind store registrations, or treat an absent signature
as policy (e.g. accept only inside a lab network).
