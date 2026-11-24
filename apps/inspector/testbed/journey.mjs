// The S3 exit journey against a REAL durable-server (not the mock): sign up → verify from the mailed link → sign
// in → first workspace (secret shown once) → API key (shown once) → the builtin:echo sample → a test call → the
// first job with curl exactly as the onboarding card shows it → Overview, Runs, run detail → cancel a scheduled
// run → invite a second address → accept it as a new account → role limits → isolation from a third account →
// forgot/reset → sign out. Then the core again at 390 wide.
//
// The server must run with an account store and the body-printing log mailer, its stderr in a file:
//   DURABLE_ACCOUNTS_BACKEND=memory DURABLE_MAILER=log DURABLE_API_KEY=<64 chars> \
//     durable-server --listen 127.0.0.1:PORT --public-origin http://127.0.0.1:PORT --ui-dir <build> ... 2> server.log
//   node testbed/journey.mjs http://127.0.0.1:PORT server.log <out-dir>
//
// Mailed links are read from server.log. Every step is screenshotted; the run ends with a table of results.
import fs from 'node:fs';
import { execFileSync } from 'node:child_process';

const { chromium } = await import('playwright');
const [BASE, LOG, OUT] = process.argv.slice(2);
if (!BASE || !LOG || !OUT) {
	console.error('usage: node testbed/journey.mjs <http://127.0.0.1:PORT> <server stderr log> <out-dir>');
	process.exit(2);
}
fs.mkdirSync(OUT, { recursive: true });
const APP = `${BASE}/app/`;
const stamp = Date.now().toString(36);
const who = (n) => `journey-${n}-${stamp}@example.com`;
const PASS = { a: 'a long enough passphrase for ada', b: 'a long enough passphrase for bea', c: 'a long enough passphrase for cyd', d: 'a long enough passphrase for dee' };
const rows = [];
const failures = [];
const browser = await chromium.launch();

async function ctxFor(width = 1440) {
	const ctx = await browser.newContext({ viewport: { width, height: 900 }, deviceScaleFactor: width === 390 ? 2 : 1, colorScheme: 'light' });
	await ctx.grantPermissions(['clipboard-read', 'clipboard-write'], { origin: BASE });
	await ctx.addInitScript(() => {
		window.__csp = [];
		document.addEventListener('securitypolicyviolation', (e) => window.__csp.push(`${e.violatedDirective} ${e.blockedURI}`));
	});
	const page = await ctx.newPage();
	page.__errors = [];
	page.on('pageerror', (e) => page.__errors.push(String(e)));
	page.on('console', (m) => { if (m.type() === 'error' && !/status of (4\d\d|503)/.test(m.text())) page.__errors.push(m.text()); });
	return { ctx, page };
}

let n = 0;
const broken = new Set();
/** One journey step: run, screenshot, check CSP/console/overflow, record. `needs`: chains that must be intact (a
 *  failure breaks the step's own `chain` and every step needing it is recorded as skipped, not timed out). */
async function step(page, label, fn, { shot = true, width = 1440, full = false, chain = 'a', needs = [chain] } = {}) {
	n++;
	const name = `${String(n).padStart(2, '0')}-${width}-${label.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '').slice(0, 48)}`;
	const missing = needs.filter((c) => broken.has(c));
	if (missing.length) {
		rows.push({ step: n, label, width, result: 'skipped', shot: '', note: `needs ${missing.join(', ')}` });
		console.log(`skip ${n} ${label}`);
		broken.add(chain);
		return false;
	}
	let result = 'pass';
	let note = '';
	try {
		note = (await fn()) ?? '';
	} catch (e) {
		result = 'FAIL';
		note = String(e).split('\n')[0];
		failures.push(`${label}: ${note}`);
	}
	if (page && shot) {
		await page.waitForTimeout(250);
		await page.screenshot({ path: `${OUT}/${name}.png`, fullPage: full }).catch(() => {});
		const csp = await page.evaluate(() => window.__csp ?? []).catch(() => []);
		if (csp.length) { result = 'FAIL'; failures.push(`${label}: CSP ${csp.join('; ')}`); }
		if (page.__errors.length) { result = 'FAIL'; failures.push(`${label}: console ${page.__errors.join(' | ')}`); page.__errors = []; }
		const sw = await page.evaluate(() => document.scrollingElement.scrollWidth).catch(() => 0);
		if (sw > width) { result = 'FAIL'; failures.push(`${label}: horizontal overflow ${sw}`); }
	}
	if (result !== 'pass') broken.add(chain);
	rows.push({ step: n, label, width, result, shot: shot && page ? `${name}.png` : '', note });
	console.log(`${result === 'pass' ? 'ok  ' : 'FAIL'} ${n} ${label}${note ? ` — ${note}` : ''}`);
	return result === 'pass';
}

/** The newest mailed link of `kind` to `to`, from the log mailer's output (waits for the 5 s outbox tick). */
async function mailedLink(to, kind, after = 0) {
	for (let i = 0; i < 40; i++) {
		// `after` is a byte offset (fs.statSync size): slice the bytes, then decode (the log holds multi-byte "—")
		const log = fs.readFileSync(LOG).subarray(after).toString('utf8');
		const blocks = log.split('durable-server: mail ').filter((b) => b.startsWith(`${kind} to ${to}\n`));
		const last = blocks.at(-1);
		const link = last?.match(/https?:\/\/\S+#\/[a-z-]+\?token=[A-Za-z0-9_-]+/)?.[0];
		if (link) return link;
		await new Promise((r) => setTimeout(r, 500));
	}
	throw new Error(`no ${kind} mail to ${to} within 20 s`);
}
const logSize = () => fs.statSync(LOG).size;

async function signUp(page, email, password) {
	await page.goto(`${APP}#/sign-up`, { waitUntil: 'networkidle' });
	await page.fill('#email', email);
	await page.fill('#password', password);
	await page.click('button[type=submit]');
	await page.waitForSelector('h1:has-text("Check your inbox")', { timeout: 10_000 });
}
async function verifyFromMail(page, email, password, since) {
	const link = await mailedLink(email, 'verify', since);
	if (!link.startsWith(`${BASE}/app#/verify?token=drt_`)) throw new Error(`unexpected verify link ${link}`);
	await page.goto(link, { waitUntil: 'networkidle' });
	await page.waitForSelector('h1:has-text("Confirm your email")', { timeout: 10_000 });
	await page.fill('#password', password);
	await page.click('button[type=submit]');
	await page.waitForSelector('.banner.notice:has-text("Email verified")', { timeout: 10_000 });
	const h = await page.evaluate(() => location.hash);
	if (h.includes('token')) throw new Error(`token left in the URL: ${h}`);
}
async function signIn(page, email, password) {
	if (!(await page.evaluate(() => location.hash)).startsWith('#/sign-in')) await page.goto(`${APP}#/sign-in`, { waitUntil: 'networkidle' });
	await page.fill('#email', email);
	await page.fill('#password', password);
	await page.click('button[type=submit]');
	await page.waitForFunction(() => !location.hash.startsWith('#/sign-in'), null, { timeout: 10_000 });
}
async function createWorkspace(page, name) {
	await page.waitForSelector('#first-name', { timeout: 10_000 });
	await page.fill('#first-name', name);
	await page.click('button:has-text("Create workspace")');
	await page.waitForSelector('dialog[open] .copy-value', { timeout: 10_000 });
	const secret = (await page.locator('dialog[open] .copy-value').textContent())?.trim() ?? '';
	if (!/^[0-9a-f]{64}$/.test(secret)) throw new Error(`signing secret looks wrong: ${secret.length} chars`);
	return secret;
}
async function dismissReveal(page) {
	await page.getByRole('button', { name: 'I have copied it' }).click();
	await page.waitForSelector('dialog[open]', { state: 'detached', timeout: 5_000 }).catch(() => {});
}
async function apiFromPage(page, method, path, body, ws) {
	return page.evaluate(async ([method, path, body, ws]) => {
		const s = await (await fetch('/auth/session')).json();
		const headers = { 'content-type': 'application/json', ...(method !== 'GET' ? { 'x-durable-csrf': s.csrf } : {}), ...(ws ? { 'x-durable-workspace': ws } : {}) };
		const r = await fetch(path, { method, headers, body: body ? JSON.stringify(body) : undefined });
		let j = null;
		try { j = await r.json(); } catch { /* none */ }
		return { status: r.status, body: j };
	}, [method, path, body, ws]);
}

const ctxA = await ctxFor();
const A = ctxA.page;
let secretA = '', keyA = '', runId = '', laterRun = '', inviteLink = '', wsA = '';

await step(A, 'Open the dashboard signed out: sign-in', async () => {
	await A.goto(APP, { waitUntil: 'networkidle' });
	await A.waitForSelector('h1:has-text("Sign in")', { timeout: 10_000 });
	return `landed on ${await A.evaluate(() => location.hash)}`;
});
let since = logSize();
await step(A, 'Sign up account A', async () => { await signUp(A, who('a'), PASS.a); return who('a'); });
await step(A, 'Verify A from the mailed link (password form)', async () => {
	const link = await mailedLink(who('a'), 'verify', since);
	await A.goto(link, { waitUntil: 'networkidle' });
	await A.waitForSelector('h1:has-text("Confirm your email")', { timeout: 10_000 });
	const posted = await A.evaluate(() => performance.getEntriesByType('resource').some((e) => e.name.includes('/auth/verify')));
	if (posted) throw new Error('verify was posted on load');
	return 'form shown, nothing posted on load';
});
await step(A, 'Confirm with the sign-up password → sign-in with a note', async () => {
	await A.fill('#password', PASS.a);
	await A.click('button[type=submit]');
	await A.waitForSelector('.banner.notice:has-text("Email verified")', { timeout: 10_000 });
	const h = await A.evaluate(() => location.hash);
	if (h.includes('token')) throw new Error(`token left in the URL: ${h}`);
	return h;
});
await step(A, 'Sign in A: no workspace yet', async () => {
	await signIn(A, who('a'), PASS.a);
	await A.waitForSelector('#first-ws-title', { timeout: 10_000 });
});
await step(A, 'Create the first workspace: secret shown once', async () => {
	secretA = await createWorkspace(A, 'Journey A');
	return `secret ${secretA.length} hex chars`;
});
await step(A, 'Overview with the setup checklist', async () => {
	await dismissReveal(A);
	await A.waitForSelector('#ob-title', { timeout: 10_000 });
	wsA = await A.evaluate(() => sessionStorage.getItem('durable-workspace') ?? '');
	const steps = await A.locator('.steps li').count();
	return `${steps} steps; workspace ${wsA}`;
}, { full: true });
await step(A, 'Create an API key: shown once', async () => {
	await A.goto(`${APP}#/keys`, { waitUntil: 'networkidle' });
	await A.fill('#key-name', 'journey-key');
	await A.click('form button[type=submit]');
	await A.waitForSelector('dialog[open] .copy-value', { timeout: 10_000 });
	keyA = (await A.locator('dialog[open] .copy-value').textContent())?.trim() ?? '';
	if (!/^drk_/.test(keyA)) throw new Error(`key looks wrong: ${keyA.slice(0, 6)}…`);
	return `${keyA.slice(0, 8)}… (${keyA.length} chars)`;
});
await step(A, 'Keys list: prefix only, created, last used not yet', async () => {
	await dismissReveal(A);
	await A.waitForSelector('.rows li', { timeout: 10_000 });
	const text = await A.locator('.rows').textContent();
	if (text?.includes(keyA)) throw new Error('the full key is shown in the list');
	return text?.replace(/\s+/g, ' ').trim().slice(0, 120);
});
await step(A, 'Use the sample handler builtin:echo', async () => {
	await A.goto(`${APP}#/handlers`, { waitUntil: 'networkidle' });
	await A.getByRole('button', { name: /Add the sample handler/ }).click();
	await A.waitForSelector('.rows li:has-text("builtin:echo")', { timeout: 10_000 });
});
await step(A, 'Send a test call to builtin:echo', async () => {
	const row = A.locator('.rows li', { hasText: 'builtin:echo' });
	await row.getByRole('button', { name: 'Test call' }).click();
	await row.locator('form button[type=submit]').click();
	await A.waitForSelector('.test-result', { timeout: 15_000 });
	const r = (await A.locator('.test-result').textContent())?.replace(/\s+/g, ' ').trim();
	if (!/^ok builtin:echo answered 2xx/.test(r ?? '')) throw new Error(`test call: ${r}`);
	return r;
});
await step(A, 'Submit the first job with curl as the onboarding card shows it', async () => {
	await A.goto(`${APP}#/overview`, { waitUntil: 'networkidle' });
	await A.waitForSelector('.code-block pre', { timeout: 10_000 });
	const card = (await A.locator('.code-block pre').first().textContent()) ?? '';
	if (!card.includes('<key>')) throw new Error('the card does not hold the <key> placeholder');
	const out = execFileSync('/bin/sh', ['-c', `${card.replace('<key>', keyA)} -sS -w '\\n%{http_code}'`], { encoding: 'utf8', timeout: 20_000 });
	const [body, code] = [out.slice(0, out.lastIndexOf('\n')), out.slice(out.lastIndexOf('\n') + 1)];
	if (code !== '200' && code !== '201' && code !== '202') throw new Error(`curl answered ${code}: ${body}`);
	runId = JSON.parse(body).run_id;
	return `HTTP ${code}, run ${runId}`;
}, { full: true });
await step(A, 'The run on Overview (checklist done, activity)', async () => {
	await A.waitForTimeout(6_000);
	await A.reload({ waitUntil: 'networkidle' });
	await A.waitForSelector('.feed li, .chart svg', { timeout: 15_000 });
	const checklist = await A.locator('#ob-title').count();
	return checklist ? 'checklist still shown' : 'checklist gone (all four steps proved)';
}, { full: true });
await step(A, 'The run on Runs', async () => {
	await A.goto(`${APP}#/runs`, { waitUntil: 'networkidle' });
	await A.waitForSelector(`table.runlist tbody tr:has-text("${runId}")`, { timeout: 15_000 });
	return (await A.locator('table.runlist tbody tr').first().textContent())?.replace(/\s+/g, ' ').trim();
});
await step(A, 'Run detail with its timeline', async () => {
	await A.goto(`${APP}#/runs/${runId}`, { waitUntil: 'networkidle' });
	await A.waitForSelector('.tl li', { timeout: 15_000 });
	return `${await A.locator('.tl li').count()} timeline rows; ${(await A.locator('.head-card .chip').textContent())?.trim()}`;
}, { full: true });
await step(A, 'Cancel a second run scheduled an hour out', async () => {
	const body = { op_key: `later-${stamp}`, spec: { version: 1, steps: [{ name: 'echo', handler: 'builtin:echo' }], input_b64: '' }, not_before: Date.now() + 3_600_000 };
	const out = execFileSync('curl', ['-sS', '-X', 'POST', `${BASE}/jobs`, '-H', `Authorization: Bearer ${keyA}`, '-H', 'Content-Type: application/json', '-d', JSON.stringify(body)], { encoding: 'utf8', timeout: 20_000 });
	laterRun = JSON.parse(out).run_id;
	await A.goto(`${APP}#/runs/${laterRun}`, { waitUntil: 'networkidle' });
	await A.getByRole('button', { name: /Cancel run/ }).click();
	await A.waitForSelector('.banner.notice:has-text("cancelled")', { timeout: 10_000 });
	await A.waitForSelector('.head-card .chip:has-text("Cancelled")', { timeout: 15_000 });
	return `run ${laterRun} cancelled`;
});
since = logSize();
await step(A, 'Invite a second address (member)', async () => {
	await A.goto(`${APP}#/team`, { waitUntil: 'networkidle' });
	await A.fill('#inv-email', who('b'));
	await A.click('form button[type=submit]');
	await A.waitForSelector('dialog[open] .copy-value', { timeout: 10_000 });
	inviteLink = (await A.locator('dialog[open] .copy-value').textContent())?.trim() ?? '';
	if (!inviteLink.includes('#/accept-invite?token=dri_')) throw new Error(`invite link: ${inviteLink}`);
	const mailed = await mailedLink(who('b'), 'workspace-invite', since).catch(() => mailedLink(who('b'), 'invite', since));
	if (!mailed.includes('#/accept-invite?token=dri_')) throw new Error(`mailed invite link: ${mailed}`);
	return `link shown once and mailed`;
});
await step(A, 'Team after the invite: pending invite listed', async () => {
	await dismissReveal(A);
	await A.waitForSelector(`.rows li:has-text("${who('b')}")`, { timeout: 10_000 });
}, { full: true });

// B: a new account through the invite
const ctxB = await ctxFor();
const B = ctxB.page;
await step(B, 'Open the invite signed out', async () => {
	await B.goto(inviteLink.replace(/^https?:\/\/[^/]+/, BASE), { waitUntil: 'networkidle' });
	await B.waitForSelector('h1:has-text("invited")', { timeout: 10_000 });
}, { chain: 'b', needs: ['a', 'b'] });
since = logSize();
await step(B, 'Create account B from the invite', async () => {
	await B.getByRole('button', { name: 'Create an account' }).click();
	await B.waitForSelector('#email');
	await B.fill('#email', who('b'));
	await B.fill('#password', PASS.b);
	await B.click('button[type=submit]');
	await B.waitForSelector('h1:has-text("Check your inbox")', { timeout: 10_000 });
}, { chain: 'b', needs: ['a', 'b'] });
await step(B, 'Verify B and sign in: back on the invite', async () => {
	await verifyFromMail(B, who('b'), PASS.b, since);
	await signIn(B, who('b'), PASS.b);
	await B.waitForSelector('h1:has-text("Join")', { timeout: 10_000 });
	return await B.evaluate(() => location.hash.replace(/token=.*/, 'token=…'));
}, { chain: 'b', needs: ['a', 'b'] });
await step(B, 'Accept the invite: B is in Journey A', async () => {
	await B.getByRole('button', { name: /Accept and join/ }).click();
	await B.waitForSelector('.ws-btn:has-text("Journey A")', { timeout: 10_000 });
	return (await B.locator('.ws-btn').textContent())?.replace(/\s+/g, ' ').trim();
}, { chain: 'b', needs: ['a', 'b'], full: true });
await step(B, 'B (member) sees A\'s run but cannot manage keys or the team', async () => {
	await B.goto(`${APP}#/runs/${runId}`, { waitUntil: 'networkidle' });
	await B.waitForSelector('.tl li', { timeout: 15_000 });
	await B.goto(`${APP}#/keys`, { waitUntil: 'networkidle' });
	await B.waitForSelector('.banner.hint:has-text("member")', { timeout: 10_000 });
	if (await B.locator('#key-name').count()) throw new Error('a member sees the create-key form');
	return 'keys read-only for a member';
}, { chain: 'b', needs: ['a', 'b'] });
await step(B, 'B (member) on Team: no invite form, no role controls', async () => {
	await B.goto(`${APP}#/team`, { waitUntil: 'networkidle' });
	await B.waitForSelector('.rows li', { timeout: 10_000 });
	if (await B.locator('#inv-email').count()) throw new Error('a member sees the invite form');
	if (await B.locator('select.role-select').count()) throw new Error('a member sees role controls');
}, { chain: 'b', needs: ['a', 'b'] });
await step(A, 'A makes B a viewer', async () => {
	await A.goto(`${APP}#/team`, { waitUntil: 'networkidle' });
	const row = A.locator('.rows li', { hasText: who('b') });
	await row.locator('select').selectOption('viewer');
	await A.waitForSelector('.banner.notice:has-text("viewer")', { timeout: 10_000 });
}, { chain: 'b', needs: ['a', 'b'] });
await step(B, 'B (viewer): no cancel, no spec, no handler changes', async () => {
	await B.goto(`${APP}#/handlers`, { waitUntil: 'networkidle' });
	await B.reload({ waitUntil: 'networkidle' });
	await B.waitForSelector('.rows li', { timeout: 10_000 });
	if (await B.locator('#h-name').count()) throw new Error('a viewer sees the register form');
	const r = await apiFromPage(B, 'PUT', '/handlers/sneaky', { url: 'https://example.com/x' }, wsA);
	if (r.status !== 403) throw new Error(`viewer PUT /handlers answered ${r.status} ${JSON.stringify(r.body)}`);
	await B.goto(`${APP}#/runs/${runId}`, { waitUntil: 'networkidle' });
	await B.waitForSelector('.tl li', { timeout: 15_000 });
	const reveal = B.getByRole('button', { name: /Reveal spec/ }).first();
	if (!(await reveal.isDisabled())) throw new Error('a viewer can reveal the spec');
	return `viewer PUT /handlers → 403 ${r.body?.code}`;
}, { chain: 'b', needs: ['a', 'b'] });

// C: isolation
const ctxC = await ctxFor();
const C = ctxC.page;
since = logSize();
await step(C, 'Account C with its own workspace', async () => {
	await signUp(C, who('c'), PASS.c);
	await verifyFromMail(C, who('c'), PASS.c, since);
	await signIn(C, who('c'), PASS.c);
	await createWorkspace(C, 'Journey C');
	await dismissReveal(C);
}, { chain: 'c', needs: ['c'] });
await step(C, 'Isolation: C cannot open A\'s run by URL, nor list A\'s workspace', async () => {
	await C.goto(`${APP}#/runs/${runId}`, { waitUntil: 'networkidle' });
	await C.waitForSelector('.banner.error', { timeout: 15_000 });
	const text = (await C.locator('.banner.error').first().textContent())?.trim();
	await C.locator('.ws-btn').click();
	const items = await C.locator('.pop .item').allTextContents();
	if (items.some((t) => t.includes('Journey A'))) throw new Error('A\'s workspace is in C\'s switcher');
	await C.keyboard.press('Escape');
	const direct = await apiFromPage(C, 'GET', `/jobs/${runId}`, null, wsA);
	const list = await apiFromPage(C, 'GET', '/jobs?order=desc&limit=50', null, wsA);
	const ws = await apiFromPage(C, 'GET', `/workspaces/${wsA}`, null, null);
	if (direct.status === 200 || list.status === 200 || ws.status === 200) throw new Error(`leak: run ${direct.status}, list ${list.status}, workspace ${ws.status}`);
	const own = await apiFromPage(C, 'GET', `/jobs/${runId}`, null, await C.evaluate(() => sessionStorage.getItem('durable-workspace')));
	if (own.status === 200) throw new Error('A\'s run reads through C\'s own workspace');
	return `page: “${text}”; API with A's workspace: run ${direct.status} ${direct.body?.code}, list ${list.status} ${list.body?.code}, workspace ${ws.status}; via C's workspace ${own.status}`;
}, { chain: 'c', needs: ['a', 'c'] });

// forgot / reset for A, in a fresh browser
const ctxR = await ctxFor();
const R = ctxR.page;
since = logSize();
await step(R, 'Forgot password for A', async () => {
	await R.goto(`${APP}#/forgot`, { waitUntil: 'networkidle' });
	await R.fill('#email', who('a'));
	await R.click('button[type=submit]');
	await R.waitForSelector('h1:has-text("Check your inbox")', { timeout: 10_000 });
}, { chain: 'r', needs: ['r'] });
const NEW_A = 'a different long passphrase for ada';
await step(R, 'Reset from the mailed link → sign-in with a note', async () => {
	const link = await mailedLink(who('a'), 'reset', since);
	await R.goto(link, { waitUntil: 'networkidle' });
	await R.fill('#password', NEW_A);
	await R.click('button[type=submit]');
	// the reset contract is moving to "no cookie" (then: sign-in with a note); a server that still signs the
	// browser in lands on the overview instead — record which
	await R.waitForSelector('.banner.notice:has-text("Password changed"), #ob-title, .ws-btn', { timeout: 10_000 });
	const h = await R.evaluate(() => location.hash);
	if (h.startsWith('#/overview')) {
		await R.locator('.acct-btn').click();
		await R.locator('#acct-menu button', { hasText: 'Sign out' }).click();
		await R.waitForSelector('h1:has-text("Sign in")', { timeout: 10_000 });
		return 'the server set a session cookie on reset (old contract): landed on the overview, signed out to continue';
	}
	return h;
}, { chain: 'r', needs: ['r'] });
await step(R, 'Sign in with the new password; the old one is refused', async () => {
	await R.fill('#email', who('a'));
	await R.fill('#password', PASS.a);
	await R.click('button[type=submit]');
	await R.waitForSelector('.banner.error:has-text("invalid_credentials")', { timeout: 10_000 });
	await signIn(R, who('a'), NEW_A);
	await R.waitForSelector('.ws-btn:has-text("Journey A")', { timeout: 10_000 });
}, { chain: 'r', needs: ['r'] });
await step(A, 'A\'s old tab after the reset: signed out', async () => {
	await A.goto(`${APP}#/runs`, { waitUntil: 'networkidle' });
	await A.waitForSelector('h1:has-text("Sign in")', { timeout: 15_000 });
	return await A.locator('.banner.notice').first().textContent().catch(() => '');
}, { chain: 'r2', needs: ['a', 'r'] });
await step(R, 'Sign out', async () => {
	await R.locator('.acct-btn').click();
	await R.locator('#acct-menu button', { hasText: 'Sign out' }).click();
	await R.waitForSelector('h1:has-text("Sign in")', { timeout: 10_000 });
	const s = await R.evaluate(async () => (await fetch('/auth/session')).status);
	if (s !== 401) throw new Error(`after sign-out /auth/session answered ${s}`);
	return '/auth/session → 401';
}, { chain: 'r', needs: ['r'] });
for (const c of [ctxA, ctxB, ctxC, ctxR]) await c.ctx.close();

// the core at 390
const ctxD = await ctxFor(390);
const D = ctxD.page;
since = logSize();
await step(D, 'Phone: sign up', async () => signUp(D, who('d'), PASS.d), { chain: 'd', width: 390 });
await step(D, 'Phone: verify from the mail', async () => verifyFromMail(D, who('d'), PASS.d, since), { chain: 'd', width: 390 });
await step(D, 'Phone: sign in, first workspace, secret once', async () => {
	await signIn(D, who('d'), PASS.d);
	await createWorkspace(D, 'Journey D');
}, { chain: 'd', width: 390 });
await step(D, 'Phone: overview checklist', async () => { await dismissReveal(D); await D.waitForSelector('#ob-title', { timeout: 10_000 }); }, { chain: 'd', width: 390, full: true });
await step(D, 'Phone: API key shown once', async () => {
	await D.goto(`${APP}#/keys`, { waitUntil: 'networkidle' });
	await D.fill('#key-name', 'phone-key');
	await D.click('form button[type=submit]');
	await D.waitForSelector('dialog[open] .copy-value', { timeout: 10_000 });
}, { chain: 'd', width: 390 });
await step(D, 'Phone: sample handler and a test call', async () => {
	await dismissReveal(D);
	await D.goto(`${APP}#/handlers`, { waitUntil: 'networkidle' });
	await D.getByRole('button', { name: /Add the sample handler/ }).click();
	const row = D.locator('.rows li', { hasText: 'builtin:echo' });
	await row.getByRole('button', { name: 'Test call' }).click();
	await row.locator('form button[type=submit]').click();
	await D.waitForSelector('.test-result', { timeout: 15_000 });
}, { chain: 'd', width: 390, full: true });
await step(D, 'Phone: send a test job from the overview', async () => {
	await D.goto(`${APP}#/overview`, { waitUntil: 'networkidle' });
	await D.getByRole('button', { name: /Send a test job/ }).click();
	await D.waitForSelector('.tl li', { timeout: 15_000 });
	return await D.evaluate(() => location.hash);
}, { chain: 'd', width: 390, full: true });
await step(D, 'Phone: menu and sign out', async () => {
	await D.getByRole('button', { name: 'Menu', exact: true }).click();
	await D.locator('.acct-btn').click();
	await D.locator('#acct-menu button', { hasText: 'Sign out' }).click();
	await D.waitForSelector('h1:has-text("Sign in")', { timeout: 10_000 });
}, { chain: 'd', width: 390 });
await ctxD.ctx.close();
await browser.close();

const table = ['| # | step | width | result | screenshot | note |', '|---|---|---|---|---|---|', ...rows.map((r) => `| ${r.step} | ${r.label} | ${r.width} | ${r.result} | ${r.shot} | ${String(r.note).replace(/\|/g, '/').slice(0, 160)} |`)].join('\n');
fs.writeFileSync(`${OUT}/journey.md`, `${table}\n`);
console.log(`\n${rows.filter((r) => r.result === 'pass').length}/${rows.length} steps pass; table in ${OUT}/journey.md`);
if (failures.length) { console.error(`FAILURES:\n  ${failures.join('\n  ')}`); process.exit(4); }
