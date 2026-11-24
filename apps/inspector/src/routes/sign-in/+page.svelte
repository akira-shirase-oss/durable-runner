<script lang="ts">
	// Sign in: email + password → POST /auth/login, then the session bootstrap. One message for a wrong email or
	// password (the server does not say which). An unverified account signs in and meets the verify-wall.
	import Icon from '../../lib/Icon.svelte';
	import ErrorBanner from '../../lib/ui/ErrorBanner.svelte';
	import PasswordField from '../../lib/ui/PasswordField.svelte';
	import { account } from '../../lib/account.svelte.ts';
	import { ApiError } from '../../lib/api.ts';
	import { emailError } from '../../lib/validate.ts';

	let email = $state(account.pendingEmail);
	let password = $state('');
	let tried = $state(false);
	let busy = $state(false);
	let error = $state<ApiError | null>(null);
	const emailErr = $derived(emailError(email));
	const passErr = $derived(password === '' ? 'Enter your password' : null);

	async function submit(e: SubmitEvent) {
		e.preventDefault();
		tried = true;
		if (emailErr || passErr || busy) return;
		busy = true;
		error = null;
		try {
			await account.signIn(email.trim(), password);
			// the shell moves on to the dashboard (or where the visitor was going)
		} catch (err) {
			error = err instanceof ApiError ? err : new ApiError(String(err), 0);
			password = '';
			tried = false;
		} finally {
			busy = false;
		}
	}
</script>

<section class="card auth-card" aria-labelledby="t">
	<h1 id="t">Sign in</h1>
	<p class="lede">to your Durable Runner dashboard.</p>
	{#if account.notice}<p class="banner notice" role="status"><Icon name="check" size={16} /><span class="msg">{account.notice}</span></p>{/if}
	{#if error}<ErrorBanner {error} />{/if}
	<form class="form" onsubmit={submit} novalidate>
		<div class="field">
			<label class="field-label" for="email">Email</label>
			<input id="email" type="email" bind:value={email} autocomplete="email" spellcheck="false" autocapitalize="off" aria-invalid={tried && emailErr ? 'true' : undefined} aria-describedby={tried && emailErr ? 'email-hint' : undefined} />
			{#if tried && emailErr}<p class="field-hint bad" id="email-hint">{emailErr}</p>{/if}
		</div>
		<PasswordField id="password" label="Password" bind:value={password} autocomplete="current-password" error={tried ? passErr : null}>
			{#snippet aside()}<a href="#/forgot">Forgot your password?</a>{/snippet}
		</PasswordField>
		<button class="btn brand" type="submit" disabled={busy}>{busy ? 'Signing in…' : 'Sign in'}</button>
	</form>
	<p class="auth-alt">New to Durable Runner? <a href="#/sign-up">Create an account</a></p>
</section>
