<script lang="ts">
	// Forgot password: POST /auth/forgot answers the same 202 for every address, so the page says the same thing
	// for every address too.
	import Icon from '../../lib/Icon.svelte';
	import ErrorBanner from '../../lib/ui/ErrorBanner.svelte';
	import { ApiError, forgotPassword } from '../../lib/api.ts';
	import { emailError } from '../../lib/validate.ts';

	let email = $state('');
	let tried = $state(false);
	let busy = $state(false);
	let sent = $state(false);
	let error = $state<ApiError | null>(null);
	const emailErr = $derived(emailError(email));

	async function submit(e: SubmitEvent) {
		e.preventDefault();
		tried = true;
		if (emailErr || busy) return;
		busy = true;
		error = null;
		try {
			await forgotPassword(email.trim());
			sent = true;
		} catch (err) {
			error = err instanceof ApiError ? err : new ApiError(String(err), 0);
		} finally {
			busy = false;
		}
	}
</script>

<section class="card auth-card" aria-labelledby="t">
	{#if sent}
		<span class="auth-icon" aria-hidden="true"><Icon name="mail" size={22} /></span>
		<h1 id="t">Check your inbox</h1>
		<p class="lede" role="status">If <strong>{email.trim()}</strong> is registered, mail with a reset link is on its way. The link works for one hour; your password stays the same until you use it.</p>
		<div class="form-actions">
			<a class="btn" href="#/sign-in">Back to sign in</a>
			<button class="btn ghost" onclick={() => (sent = false)}>Use another address</button>
		</div>
	{:else}
		<h1 id="t">Reset your password</h1>
		<p class="lede">Enter the address you signed up with. We will email a link to choose a new password.</p>
		{#if error}<ErrorBanner {error} />{/if}
		<form class="form" onsubmit={submit} novalidate>
			<div class="field">
				<label class="field-label" for="email">Email</label>
				<input id="email" type="email" bind:value={email} autocomplete="email" spellcheck="false" autocapitalize="off" aria-invalid={tried && emailErr ? 'true' : undefined} aria-describedby={tried && emailErr ? 'email-hint' : undefined} />
				{#if tried && emailErr}<p class="field-hint bad" id="email-hint">{emailErr}</p>{/if}
			</div>
			<button class="btn brand" type="submit" disabled={busy}>{busy ? 'Sending…' : 'Email me a reset link'}</button>
		</form>
		<p class="auth-alt">Remembered it? <a href="#/sign-in">Sign in</a></p>
	{/if}
</section>
