<script lang="ts">
	// After sign-up: mail is on its way either way (a verify link for a new address, a "you already have an
	// account" notice for a registered one), so the page says the same thing for both. Resend asks for a fresh
	// link (POST /auth/resend-verification, also always the same 202).
	import Icon from '../../lib/Icon.svelte';
	import ErrorBanner from '../../lib/ui/ErrorBanner.svelte';
	import { account } from '../../lib/account.svelte.ts';
	import { ApiError, resendVerification } from '../../lib/api.ts';
	import { emailError } from '../../lib/validate.ts';

	let email = $state(account.pendingEmail);
	const known = account.pendingEmail !== '';
	let busy = $state(false);
	let sent = $state(false);
	let tried = $state(false);
	let error = $state<ApiError | null>(null);
	const emailErr = $derived(emailError(email));

	async function resend(e: SubmitEvent) {
		e.preventDefault();
		tried = true;
		if (emailErr || busy) return;
		busy = true;
		error = null;
		try {
			await resendVerification(email.trim());
			sent = true;
		} catch (err) {
			error = err instanceof ApiError ? err : new ApiError(String(err), 0);
		} finally {
			busy = false;
		}
	}
</script>

<section class="card auth-card" aria-labelledby="t">
	<span class="auth-icon" aria-hidden="true"><Icon name="mail" size={22} /></span>
	<h1 id="t">Check your inbox</h1>
	<p class="lede">
		{#if known}If <strong>{account.pendingEmail}</strong> is new here, a verification link is on its way; if it already has an account, the mail says how to sign in instead.{:else}If that address is registered, mail is on its way.{/if}
		The link works for 24 hours.
	</p>
	{#if sent}<p class="banner notice" role="status"><Icon name="check" size={16} /><span class="msg">If that address still needs verifying, a fresh link is on its way. It replaces the older ones.</span></p>{/if}
	{#if error}<ErrorBanner {error} />{/if}
	<form class="form" onsubmit={resend} novalidate>
		{#if !known}
			<div class="field">
				<label class="field-label" for="email">Email</label>
				<input id="email" type="email" bind:value={email} autocomplete="email" aria-invalid={tried && emailErr ? 'true' : undefined} aria-describedby={tried && emailErr ? 'email-hint' : undefined} />
				{#if tried && emailErr}<p class="field-hint bad" id="email-hint">{emailErr}</p>{/if}
			</div>
		{/if}
		<div class="form-actions">
			<button class="btn" type="submit" disabled={busy || sent}><Icon name="send" />{busy ? 'Sending…' : 'Send the link again'}</button>
			<a class="btn ghost" href="#/sign-in">Back to sign in</a>
		</div>
	</form>
	<p class="auth-alt">Nothing after a few minutes? Check spam, or <a href="#/sign-up">sign up again</a> with the right address.</p>
</section>
