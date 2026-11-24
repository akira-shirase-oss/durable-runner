<script lang="ts">
	// The workspace's handler registry (signed in): every registered handler with its URL and health (last test
	// call, p50/p95), register / edit / remove (member and above), a test call per handler (one signed call through
	// the same SSRF-guarded path as a real step, never journaled — D-066), the signing secret (shown only at
	// workspace creation or rotation; rotating is admin and above — D-069) and how a handler verifies a call.
	import Icon from '../Icon.svelte';
	import CodeBlock from '../ui/CodeBlock.svelte';
	import Confirm from '../ui/Confirm.svelte';
	import ErrorBanner from '../ui/ErrorBanner.svelte';
	import Notice from '../ui/Notice.svelte';
	import RoleNote from '../ui/RoleNote.svelte';
	import Skeleton from '../ui/Skeleton.svelte';
	import { account } from '../account.svelte.ts';
	import { ApiError, deleteHandler, listHandlers, putHandler, rotateHandlerSecret, testHandler } from '../api.ts';
	import { fmtAgo, fmtApproxMs } from '../format.ts';
	import { live, poll, Resource } from '../live.svelte.ts';
	import { isSample, listOf, normHandler, normTest, SAMPLE_HANDLER, type HandlerRow, type TestResult } from '../saas.ts';
	import { ui } from '../ui-state.svelte.ts';
	import { b64ToText, handlerUrlError, inputToB64, nameError } from '../validate.ts';

	const res = new Resource((signal) => listHandlers(account.cred, signal), { interval: 15_000, live: false });
	poll(res);
	const handlers = $derived(res.data ? listOf(res.data, 'handlers').map(normHandler).filter((h): h is HandlerRow => h !== null) : []);
	const hasSample = $derived(handlers.some((h) => h.name === SAMPLE_HANDLER));
	const write = $derived(account.can('write'));
	const admin = $derived(account.can('admin'));

	let busy = $state<string | null>(null);
	let error = $state<ApiError | null>(null);
	let notice = $state<string | null>(null);

	async function guard(id: string, fn: () => Promise<void>) {
		busy = id;
		error = null;
		notice = null;
		try {
			await fn();
			await res.refresh(true);
		} catch (e) {
			error = e instanceof ApiError ? e : new ApiError(String(e), 0);
		} finally {
			busy = null;
		}
	}

	// register (or, with `editing`, change a URL)
	let name = $state('');
	let url = $state('');
	let tried = $state(false);
	const nameErr = $derived(nameError(name, 'handler name'));
	const urlErr = $derived(handlerUrlError(url));
	function register(e: SubmitEvent) {
		e.preventDefault();
		tried = true;
		if (nameErr || urlErr || busy) return;
		const n = name.trim();
		const u = url.trim();
		void guard('register', async () => {
			await putHandler(account.cred, n, u);
			notice = `Handler ${n} is registered. Send it a test call to check the URL and your signature check.`;
			name = '';
			url = '';
			tried = false;
		});
	}
	const useSample = () =>
		guard('sample', async () => {
			await putHandler(account.cred, SAMPLE_HANDLER, SAMPLE_HANDLER);
			notice = 'The sample handler is registered: it echoes its input back, in-process, with no network call.';
		});

	let editing = $state<string | null>(null);
	let editUrl = $state('');
	const editErr = $derived(handlerUrlError(editUrl));
	function startEdit(h: HandlerRow) {
		editing = h.name;
		editUrl = h.url ?? '';
	}
	function saveEdit(e: SubmitEvent, h: HandlerRow) {
		e.preventDefault();
		if (editErr || busy) return;
		void guard(h.name, async () => {
			await putHandler(account.cred, h.name, editUrl.trim());
			notice = `${h.name} now calls ${editUrl.trim()}.`;
			editing = null;
		});
	}
	const remove = (h: HandlerRow) =>
		guard(h.name, async () => {
			await deleteHandler(account.cred, h.name);
			notice = `${h.name} is removed. Steps that name it are refused from now on; runs already past it are unaffected.`;
		});

	// test calls, one panel at a time
	let testing = $state<string | null>(null);
	let testInput = $state('{"hello": "world"}');
	let testResult = $state<{ name: string; r: TestResult } | null>(null);
	const testInputErr = $derived(inputToB64(testInput).error);
	function openTest(h: HandlerRow) {
		testing = testing === h.name ? null : h.name;
		testResult = null;
	}
	function sendTest(e: SubmitEvent, h: HandlerRow) {
		e.preventDefault();
		const enc = inputToB64(testInput);
		if (enc.b64 === null || busy) return;
		const b64 = enc.b64;
		void guard(h.name, async () => {
			const r = normTest(await testHandler(account.cred, h.name, b64));
			if (r.status === 'ok') account.testOkSeen = true;
			testResult = { name: h.name, r };
		});
	}
	const TEST_TONE: Record<string, string> = { ok: 'ok', failed: 'bad', refused: 'bad', lost: 'warn' };
	const TEST_WORD: Record<string, string> = {
		ok: 'answered 2xx',
		failed: 'answered with an error status',
		refused: 'was refused before any packet left: the target is not allowed (private, local or the runner itself)',
		lost: 'did not answer in time, or the connection dropped',
	};

	const rotate = () =>
		guard('secret', async () => {
			const r = await rotateHandlerSecret(account.cred);
			ui.show({
				title: 'New signing secret',
				label: 'Handler signing secret',
				value: r.secret,
				note: 'The old secret stopped working at once: calls signed with it now fail your check and are retried with this one. Put it in every handler of this workspace now; it is never shown again.',
				usage: "export DURABLE_HANDLER_SECRET='<paste the secret>'",
			});
		});

	let lang = $state<'node' | 'python'>('node');
	const NODE = `import { createHash, createHmac, timingSafeEqual } from 'node:crypto';

const SECRET = Buffer.from(process.env.DURABLE_HANDLER_SECRET, 'hex');
// host[:port] of the URL you registered: never read it from the Host header
const AUTHORITY = 'handlers.example.com';

// req: the incoming POST; rawBody: its exact bytes (a Buffer)
export function verified(req, rawBody) {
  const h = (n) => req.headers[n] ?? '-';
  const ts = h('x-durable-timestamp');
  if (!/^\\d+$/.test(ts) || Math.abs(Date.now() / 1000 - Number(ts)) > 300) return false;
  const { idem_key } = JSON.parse(rawBody);
  if (req.headers['x-idempotency-key'] !== idem_key) return false;
  const path = new URL(req.url, 'http://local').pathname; // no query string
  const canonical = [ts, req.method, AUTHORITY, path, h('x-durable-run-id'),
    h('x-durable-step'), h('x-durable-attempt'), idem_key,
    createHash('sha256').update(rawBody).digest('hex')].join('\\n');
  const want = createHmac('sha256', SECRET).update(canonical).digest();
  const got = Buffer.from(h('x-durable-signature'), 'hex');
  // then refuse a signature already seen in the last 600 s (replay)
  return got.length === want.length && timingSafeEqual(got, want);
}`;
	const PY = `import hashlib, hmac, json, os, time

SECRET = bytes.fromhex(os.environ["DURABLE_HANDLER_SECRET"])
# host[:port] of the URL you registered: never read it from the Host header
AUTHORITY = "handlers.example.com"

def verified(method, path, headers, raw_body):
    h = lambda n: headers.get(n, "-")
    ts = h("x-durable-timestamp")
    if not ts.isdigit() or abs(time.time() - int(ts)) > 300:
        return False
    idem_key = json.loads(raw_body)["idem_key"]
    if headers.get("x-idempotency-key") != idem_key:
        return False
    canonical = "\\n".join([ts, method, AUTHORITY, path,  # path without the query
        h("x-durable-run-id"), h("x-durable-step"), h("x-durable-attempt"),
        idem_key, hashlib.sha256(raw_body).hexdigest()])
    want = hmac.new(SECRET, canonical.encode(), hashlib.sha256).hexdigest()
    # then refuse a signature already seen in the last 600 s (replay)
    return hmac.compare_digest(want, h("x-durable-signature"))`;
</script>

<RoleNote need="write" what="registering, changing, removing or testing handlers" />
{#if notice}<p class="banner notice" role="status"><Icon name="check" size={16} /><span class="msg">{notice}</span><button class="btn ghost sm act" onclick={() => (notice = null)}>Dismiss</button></p>{/if}
{#if error}<ErrorBanner {error} dismiss={() => (error = null)} />{/if}

<section class="card" aria-labelledby="reg-title">
	<div class="card-head ruled">
		<div>
			<h2 id="reg-title"><Icon name="webhook" size={17} />Registered handlers</h2>
			<p class="sub">The HTTPS endpoints your steps call, by name. Private and local addresses are refused, here and at every call.</p>
		</div>
		{#if write && !hasSample && res.data}
			<button class="btn sm" disabled={busy !== null} onclick={useSample}><Icon name="zap" />{busy === 'sample' ? 'Registering…' : 'Add the sample handler'}</button>
		{/if}
	</div>
	{#if res.status === 'unsupported' || res.status === 'needs-key' || (res.status === 'error' && !res.data)}
		<div class="card-body"><Notice status={res.status} error={res.error} what="The handler registry" route="GET /handlers" retry={() => res.refresh(true)} /></div>
	{:else if res.loading}
		<Skeleton label="Loading handlers" />
	{:else if handlers.length === 0}
		<p class="empty">No handlers yet.{write ? ' Register your endpoint below, or add the sample handler to try a run with no setup.' : ''}</p>
	{:else}
		<ul class="rows">
			{#each handlers as h (h.name)}
				{@const sample = isSample(h)}
				{@const flag = h.source === 'flag'}
				<li>
					<div class="row-main">
						<p class="row-title">
							<span class="mono">{h.name}</span>
							{#if sample}<span class="badge">Sample · in-process</span>{/if}
							{#if flag}<span class="badge">Operator flag</span>{/if}
							{#if h.health?.lastTestStatus}<span class="badge {TEST_TONE[h.health.lastTestStatus] ?? ''}">Last test: {h.health.lastTestStatus}</span>{/if}
						</p>
						<p class="row-meta">
							<span class="mono url">{sample ? 'Echoes its input back. No network call.' : (h.url ?? 'URL not shown')}</span>
						</p>
						{#if h.health}
							<p class="row-meta">
								{#if h.health.lastTestAt}<span>Tested {fmtAgo(live.now - h.health.lastTestAt)}{h.health.lastTestLatencyMs !== null ? ` · ${h.health.lastTestLatencyMs} ms` : ''}</span>{/if}
								<span>p50 {fmtApproxMs(h.health.p50)}</span><span>p95 {fmtApproxMs(h.health.p95)}</span>
							</p>
						{/if}
					</div>
					{#if write && !flag}
						<div class="row-actions">
							<button class="btn sm" aria-expanded={testing === h.name} onclick={() => openTest(h)}><Icon name="send" />Test call</button>
							{#if !sample}<button class="btn sm" aria-expanded={editing === h.name} onclick={() => (editing === h.name ? (editing = null) : startEdit(h))}><Icon name="pencil" />Edit</button>{/if}
							<Confirm label="Remove" question="Remove {h.name}? Steps that name it are refused until it is registered again." confirmLabel="Remove" busy={busy === h.name} onconfirm={() => remove(h)} />
						</div>
					{/if}
					{#if editing === h.name}
						<form class="form inline-panel" onsubmit={(e) => saveEdit(e, h)} novalidate>
							<div class="field">
								<label class="field-label" for="edit-{h.name}">New URL for {h.name}</label>
								<input id="edit-{h.name}" class="mono" bind:value={editUrl} inputmode="url" autocomplete="off" spellcheck="false" aria-invalid={editErr ? 'true' : undefined} aria-describedby="edit-{h.name}-hint" />
								<p class="field-hint" class:bad={!!editErr} id="edit-{h.name}-hint">{editErr ?? 'Steps already sent keep their attempt; the next call goes to the new URL.'}</p>
							</div>
							<div class="form-actions">
								<button class="btn sm" type="submit" disabled={busy !== null}>{busy === h.name ? 'Saving…' : 'Save URL'}</button>
								<button class="btn ghost sm" type="button" onclick={() => (editing = null)}>Cancel</button>
							</div>
						</form>
					{/if}
					{#if testing === h.name}
						<form class="form inline-panel" onsubmit={(e) => sendTest(e, h)} novalidate>
							<p class="field-hint">One signed call, exactly like a step's, outside any run: nothing is journaled and it is not retried. Free plan: 30 test calls an hour.</p>
							<div class="field">
								<label class="field-label" for="test-{h.name}">Input (JSON)</label>
								<textarea id="test-{h.name}" class="mono" rows="3" bind:value={testInput} spellcheck="false" aria-invalid={testInputErr ? 'true' : undefined} aria-describedby={testInputErr ? `test-${h.name}-err` : undefined}></textarea>
								{#if testInputErr}<p class="field-hint bad" id="test-{h.name}-err">{testInputErr}</p>{/if}
							</div>
							<div class="form-actions">
								<button class="btn brand sm" type="submit" disabled={busy !== null || !!testInputErr}><Icon name="send" />{busy === h.name ? 'Calling…' : `Call ${h.name}`}</button>
							</div>
							{#if testResult && testResult.name === h.name}
								{@const r = testResult.r}
								{@const text = b64ToText(r.replyB64)}
								<div class="test-result" role="status">
									<p class="row-title"><span class="badge {TEST_TONE[r.status] ?? ''}">{r.status}</span>{h.name} {TEST_WORD[r.status] ?? `answered ${r.status}`}.</p>
									<p class="row-meta">
										{#if r.httpStatus !== null}<span>HTTP {r.httpStatus}</span>{/if}
										{#if r.latencyMs !== null}<span>{r.latencyMs} ms</span>{/if}
									</p>
									{#if r.replyB64}
										<p class="field-label">Reply (first 1 KiB{text === null ? ', base64' : ''})</p>
										<pre class="reply mono">{text ?? r.replyB64}</pre>
									{/if}
								</div>
							{/if}
						</form>
					{/if}
				</li>
			{/each}
		</ul>
	{/if}
	{#if write}
		<form class="form register" onsubmit={register} novalidate aria-labelledby="register-title">
			<h3 id="register-title">Register a handler</h3>
			<div class="form-row">
				<div class="field">
					<label class="field-label" for="h-name">Name</label>
					<input id="h-name" class="mono" bind:value={name} maxlength="128" autocomplete="off" spellcheck="false" placeholder="charge-card" aria-invalid={tried && nameErr ? 'true' : undefined} aria-describedby="h-name-hint" />
					<p class="field-hint" class:bad={tried && !!nameErr} id="h-name-hint">{tried && nameErr ? nameErr : 'What steps call it by. Registering a name again changes its URL.'}</p>
				</div>
				<div class="field">
					<label class="field-label" for="h-url">URL</label>
					<input id="h-url" class="mono" bind:value={url} inputmode="url" autocomplete="off" spellcheck="false" placeholder="https://api.example.com/steps/charge" aria-invalid={tried && urlErr ? 'true' : undefined} aria-describedby="h-url-hint" />
					<p class="field-hint" class:bad={tried && !!urlErr} id="h-url-hint">{tried && urlErr ? urlErr : 'Public HTTPS. Each call is signed with the workspace secret below.'}</p>
				</div>
			</div>
			<div class="form-actions">
				<button class="btn brand sm" type="submit" disabled={busy !== null}><Icon name="plus" />{busy === 'register' ? 'Registering…' : 'Register'}</button>
			</div>
		</form>
	{/if}
</section>

<div class="grid side-by">
	<section class="card" aria-labelledby="secret-title">
		<div class="card-head">
			<div>
				<h2 id="secret-title"><Icon name="shield" size={17} />Signing secret</h2>
				<p class="sub">Every call to your handlers carries an HMAC-SHA256 signature made with it.</p>
			</div>
		</div>
		<div class="card-body secret">
			<p class="hint">Shown once, when the workspace was created. It is never shown again and no page or API can read it back: rotate to see a new one.</p>
			{#if admin}
				<Confirm label="Rotate the secret" icon="rotate" tone="plain" question="Rotate the signing secret? Calls signed with the old one fail at once, until every handler has the new one." confirmLabel="Rotate now" busy={busy === 'secret'} onconfirm={rotate} />
			{:else}
				<p class="muted">Rotating it needs the admin or owner role.</p>
			{/if}
		</div>
	</section>
	<section class="card" aria-labelledby="verify-title">
		<div class="card-head">
			<div>
				<h2 id="verify-title"><Icon name="lock" size={17} />Verify a call</h2>
				<p class="sub">Check freshness, the idempotency key and the signature before you act. <a href="https://docs.durable-runner.404human.dev/" target="_blank" rel="noopener noreferrer">The full contract<span class="sr"> (opens in a new tab)</span></a></p>
			</div>
			<div class="seg" role="group" aria-label="Language">
				<button aria-pressed={lang === 'node'} onclick={() => (lang = 'node')}>Node.js</button>
				<button aria-pressed={lang === 'python'} onclick={() => (lang = 'python')}>Python</button>
			</div>
		</div>
		<div class="card-body">
			<CodeBlock code={lang === 'node' ? NODE : PY} label={lang === 'node' ? 'verify.mjs' : 'verify.py'} />
		</div>
	</section>
</div>

<style>
	.url {
		overflow-wrap: anywhere;
		word-break: break-all;
	}
	.inline-panel {
		flex-basis: 100%;
		padding: 14px 16px;
		border-radius: 14px;
		background: var(--c-panel);
		box-shadow: inset 0 0 0 1px var(--c-line);
	}
	.register {
		padding: 18px 24px 24px;
		border-top: 1px solid var(--c-line);
	}
	.register h3 {
		margin: 0;
	}
	.test-result {
		display: flex;
		flex-direction: column;
		gap: 6px;
		padding-top: 12px;
		border-top: 1px solid var(--c-line);
	}
	.reply {
		margin: 0;
		padding: 10px 12px;
		border-radius: 10px;
		background: var(--c-card);
		box-shadow: inset 0 0 0 1px var(--c-line);
		font-size: 12.5px;
		white-space: pre-wrap;
		overflow-wrap: anywhere;
	}
	.side-by {
		grid-template-columns: minmax(0, 1fr) minmax(0, 1.6fr);
		align-items: start;
	}
	@media (max-width: 1100px) {
		.side-by {
			grid-template-columns: minmax(0, 1fr);
		}
	}
	.secret {
		display: flex;
		flex-direction: column;
		align-items: flex-start;
		gap: 12px;
	}
	.secret .hint {
		margin: 0;
		line-height: 1.5;
	}
	.sub a {
		color: var(--c-brand-deep);
		text-underline-offset: 3px;
	}
	@media (max-width: 600px) {
		.register {
			padding: 16px;
		}
	}
</style>
