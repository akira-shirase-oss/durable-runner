<script lang="ts">
	// Sign up: email + password → POST /auth/signup, which always answers the same 202 (it never says whether the
	// address already has an account), then the verify-sent page. An invite link's token rides along as
	// `invite_token`, which an invite-only deployment requires.
	import ErrorBanner from '../../lib/ui/ErrorBanner.svelte';
	import PasswordField from '../../lib/ui/PasswordField.svelte';
	import { goto } from '$app/navigation';
	import { account } from '../../lib/account.svelte.ts';
	import { ApiError, signup } from '../../lib/api.ts';
	import { hashParam, tokenShapeOk } from '../../lib/mode.ts';
	import { emailError, passwordError, PASSWORD_MIN } from '../../lib/validate.ts';

	const invite = (() => {
		const t = hashParam(location.hash, 'invite');
		return tokenShapeOk(t) ? t : null;
	})();

	let email = $state('');
	let password = $state('');
	let tried = $state(false);
	let busy = $state(false);
	let error = $state<ApiError | null>(null);
	const emailErr = $derived(emailError(email));
	const passErr = $derived(passwordError(password));
	const chars = $derived([...password].length);

	async function submit(e: SubmitEvent) {
		e.preventDefault();
		tried = true;
		if (emailErr || passErr || busy) return;
		busy = true;
		error = null;
		try {
			await signup(email.trim(), password, invite);
			account.pendingEmail = email.trim();
			password = '';
			void goto('#/verify-sent');
		} catch (err) {
			error = err instanceof ApiError ? err : new ApiError(String(err), 0);
		} finally {
			busy = false;
		}
	}
</script>

<section class="card auth-card" aria-labelledby="t">
	<h1 id="t">Create your account</h1>
	<p class="lede">Free while in beta. We email a link to verify the address before anything can be changed.</p>
	{#if invite}<p class="banner hint"><span class="msg">You are signing up from an invite. Use the address the invite was sent to.</span></p>{/if}
	{#if error}<ErrorBanner {error} />{/if}
	<form class="form" onsubmit={submit} novalidate>
		<div class="field">
			<label class="field-label" for="email">Email</label>
			<input id="email" type="email" bind:value={email} autocomplete="email" spellcheck="false" autocapitalize="off" aria-invalid={tried && emailErr ? 'true' : undefined} aria-describedby={tried && emailErr ? 'email-hint' : undefined} />
			{#if tried && emailErr}<p class="field-hint bad" id="email-hint">{emailErr}</p>{/if}
		</div>
		<PasswordField
			id="password"
			label="Password"
			bind:value={password}
			autocomplete="new-password"
			hint={chars > 0 && chars < PASSWORD_MIN ? `${chars} of at least ${PASSWORD_MIN} characters` : `At least ${PASSWORD_MIN} characters. A passphrase works well; no symbols required.`}
			error={tried ? passErr : null}
		/>
		<button class="btn brand" type="submit" disabled={busy}>{busy ? 'Creating…' : 'Create account'}</button>
	</form>
	<p class="auth-alt">Have an account? <a href="#/sign-in">Sign in</a></p>
</section>
