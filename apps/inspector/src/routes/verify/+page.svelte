<script lang="ts">
	// #/verify?token=… — the link from the verification mail. The token stays in the fragment (prefetchers fetch the
	// page, never the token) and leaves the browser only in the POST body, together with the password chosen at
	// sign-up: the server refuses a token whose pending password does not match, so a link that reached someone
	// else cannot finish a sign-up they did not make. Nothing is sent on load. Verifying signs every session of the
	// account out and sets no cookie (a mailed link never signs anybody in), so success always ends at sign-in.
	import Icon from '../../lib/Icon.svelte';
	import ErrorBanner from '../../lib/ui/ErrorBanner.svelte';
	import PasswordField from '../../lib/ui/PasswordField.svelte';
	import { goto } from '$app/navigation';
	import { account } from '../../lib/account.svelte.ts';
	import { ApiError, resendVerification, verifyEmail } from '../../lib/api.ts';
	import { hashParam, tokenShapeOk } from '../../lib/mode.ts';
	import { emailError } from '../../lib/validate.ts';
	import { copyFor } from '../../lib/http.ts';

	const token = hashParam(location.hash, 'token');
	let phase = $state<'confirm' | 'gone'>(tokenShapeOk(token) ? 'confirm' : 'gone');

	// confirm: the sign-up password
	let password = $state('');
	let tried = $state(false);
	let busy = $state(false);
	let fieldError = $state<string | null>(null);
	let error = $state<ApiError | null>(null);
	const passMissing = $derived(password === '' ? 'Enter the password you chose at sign-up' : null);

	/** The refusals the server answers for the token itself: every one reads as "no longer valid". */
	function deadToken(e: ApiError): boolean {
		return e.status === 410 || e.code === 'token_gone' || e.code === 'invalid_or_expired_token';
	}

	async function confirm(e: SubmitEvent) {
		e.preventDefault();
		tried = true;
		if (!tokenShapeOk(token) || passMissing || busy) return;
		busy = true;
		error = null;
		fieldError = null;
		try {
			await verifyEmail(token, password);
			password = '';
			account.notice = 'Email verified. Verifying signed out every browser on this account: sign in to continue.';
			// every session of the account ended with the verify: this tab's too, if it had one
			if (account.mode === 'signed-in') await account.refresh().catch(() => {});
			// replace: the token leaves the address bar and the history
			void goto('#/sign-in', { replace: true });
		} catch (err) {
			const ae = err instanceof ApiError ? err : new ApiError(String(err), 0);
			if (deadToken(ae)) phase = 'gone';
			else if (ae.status === 0 || ae.status === 429 || ae.status >= 500) error = ae;
			else {
				// a refused password (or a refused pairing): say what the server's code says, no more
				fieldError =
					ae.code === 'bad_input' && ae.message && ae.message !== String(ae.status)
						? ae.message.charAt(0).toUpperCase() + ae.message.slice(1)
						: ae.code === 'invalid_credentials' || ae.code === 'forbidden' || ae.code === null
							? 'This link and password do not match. Use the password you chose when you signed up.'
							: copyFor(ae.code, ae.message);
				password = '';
				tried = false;
			}
		} finally {
			busy = false;
		}
	}

	// gone: ask for a fresh link
	let email = $state('');
	let mailTried = $state(false);
	let sending = $state(false);
	let sent = $state(false);
	let sendError = $state<ApiError | null>(null);
	const emailErr = $derived(emailError(email));
	async function resend(e: SubmitEvent) {
		e.preventDefault();
		mailTried = true;
		if (emailErr || sending) return;
		sending = true;
		sendError = null;
		try {
			await resendVerification(email.trim());
			sent = true;
		} catch (err) {
			sendError = err instanceof ApiError ? err : new ApiError(String(err), 0);
		} finally {
			sending = false;
		}
	}
</script>

<section class="card auth-card" aria-labelledby="t">
	{#if phase === 'confirm'}
		<span class="auth-icon" aria-hidden="true"><Icon name="mail" size={22} /></span>
		<h1 id="t">Confirm your email</h1>
		<p class="lede">Enter the password you chose at sign-up. It shows this link reached the person who signed up.</p>
		{#if error}<ErrorBanner {error} />{/if}
		<form class="form" onsubmit={confirm} novalidate>
			<PasswordField
				id="password"
				label="Your password"
				bind:value={password}
				autocomplete="current-password"
				error={fieldError ?? (tried ? passMissing : null)}
			/>
			<button class="btn brand" type="submit" disabled={busy}>{busy ? 'Confirming…' : 'Confirm'}</button>
		</form>
		<p class="auth-alt">Forgot the password? <a href="#/sign-up">Sign up again</a> with the same address: the newest link replaces this one.</p>
	{:else}
		<span class="auth-icon" aria-hidden="true"><Icon name="alert" size={22} /></span>
		<h1 id="t">This link is no longer valid</h1>
		<p class="lede">Verification links work once, for 24 hours, and a newer link replaces older ones. Enter your address for a fresh one.</p>
		{#if sent}<p class="banner notice" role="status"><Icon name="check" size={16} /><span class="msg">If that address still needs verifying, a fresh link is on its way.</span></p>{/if}
		{#if sendError}<ErrorBanner error={sendError} />{/if}
		<form class="form" onsubmit={resend} novalidate>
			<div class="field">
				<label class="field-label" for="email">Email</label>
				<input id="email" type="email" bind:value={email} autocomplete="email" aria-invalid={mailTried && emailErr ? 'true' : undefined} aria-describedby={mailTried && emailErr ? 'email-hint' : undefined} />
				{#if mailTried && emailErr}<p class="field-hint bad" id="email-hint">{emailErr}</p>{/if}
			</div>
			<button class="btn brand" type="submit" disabled={sending || sent}>{sending ? 'Sending…' : 'Send a fresh link'}</button>
		</form>
		<p class="auth-alt">Already verified? <a href="#/sign-in">Sign in</a></p>
	{/if}
</section>
