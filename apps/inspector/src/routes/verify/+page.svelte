<script lang="ts">
	// #/verify?token=… — the link from the verification mail. The token travels in the fragment (prefetchers fetch
	// the page, never the token) and is POSTed once, on load. Verifying signs every session of the account out
	// and sets no cookie (a mailed link never signs anybody in), so success always ends at "sign in".
	import Icon from '../../lib/Icon.svelte';
	import ErrorBanner from '../../lib/ui/ErrorBanner.svelte';
	import { goto } from '$app/navigation';
	import { account } from '../../lib/account.svelte.ts';
	import { ApiError, resendVerification, verifyEmail } from '../../lib/api.ts';
	import { hashParam, tokenShapeOk } from '../../lib/mode.ts';
	import { emailError } from '../../lib/validate.ts';

	const token = hashParam(location.hash, 'token');
	let phase = $state<'checking' | 'ok' | 'gone' | 'error'>(tokenShapeOk(token) ? 'checking' : 'gone');
	let error = $state<ApiError | null>(null);

	async function run() {
		if (!tokenShapeOk(token)) return;
		phase = 'checking';
		error = null;
		try {
			await verifyEmail(token);
			phase = 'ok';
			account.notice = 'Email verified. Sign in to continue.';
			// every session of the account ended with the verify: this tab's too
			if (account.mode === 'signed-in') await account.refresh().catch(() => {});
			void goto('#/verify', { replace: true, shallow: true });
		} catch (e) {
			const err = e instanceof ApiError ? e : new ApiError(String(e), 0);
			if (err.status === 410 || err.code === 'token_gone' || err.code === 'invalid_or_expired_token') phase = 'gone';
			else {
				phase = 'error';
				error = err;
			}
		}
	}
	void run();

	let email = $state('');
	let tried = $state(false);
	let busy = $state(false);
	let sent = $state(false);
	let sendError = $state<ApiError | null>(null);
	const emailErr = $derived(emailError(email));
	async function resend(e: SubmitEvent) {
		e.preventDefault();
		tried = true;
		if (emailErr || busy) return;
		busy = true;
		sendError = null;
		try {
			await resendVerification(email.trim());
			sent = true;
		} catch (err) {
			sendError = err instanceof ApiError ? err : new ApiError(String(err), 0);
		} finally {
			busy = false;
		}
	}
</script>

<section class="card auth-card" aria-labelledby="t" aria-busy={phase === 'checking'}>
	{#if phase === 'checking'}
		<span class="auth-icon" aria-hidden="true"><Icon name="mail" size={22} /></span>
		<h1 id="t">Verifying your email…</h1>
		<p class="lede" role="status">One moment.</p>
	{:else if phase === 'ok'}
		<span class="auth-icon" aria-hidden="true"><Icon name="check" size={22} /></span>
		<h1 id="t">Email verified</h1>
		<p class="lede">Your account can now create workspaces and make changes. For safety, verifying signed out every browser on this account, this one included.</p>
		<a class="btn brand" href="#/sign-in">Sign in</a>
	{:else if phase === 'error' && error}
		<h1 id="t">Verify your email</h1>
		<ErrorBanner {error} retry={() => void run()} />
	{:else}
		<span class="auth-icon" aria-hidden="true"><Icon name="alert" size={22} /></span>
		<h1 id="t">This link is no longer valid</h1>
		<p class="lede">Verification links work once, for 24 hours, and a newer link replaces older ones. Enter your address for a fresh one.</p>
		{#if sent}<p class="banner notice" role="status"><Icon name="check" size={16} /><span class="msg">If that address still needs verifying, a fresh link is on its way.</span></p>{/if}
		{#if sendError}<ErrorBanner error={sendError} />{/if}
		<form class="form" onsubmit={resend} novalidate>
			<div class="field">
				<label class="field-label" for="email">Email</label>
				<input id="email" type="email" bind:value={email} autocomplete="email" aria-invalid={tried && emailErr ? 'true' : undefined} aria-describedby={tried && emailErr ? 'email-hint' : undefined} />
				{#if tried && emailErr}<p class="field-hint bad" id="email-hint">{emailErr}</p>{/if}
			</div>
			<button class="btn brand" type="submit" disabled={busy || sent}>{busy ? 'Sending…' : 'Send a fresh link'}</button>
		</form>
		<p class="auth-alt">Already verified? <a href="#/sign-in">Sign in</a></p>
	{/if}
</section>
