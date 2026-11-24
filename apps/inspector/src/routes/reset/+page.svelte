<script lang="ts">
	// #/reset?token=… — choose a new password. The token stays in the fragment and is POSTed on submit with the
	// new password. A reset signs out every session of the account; the server signs this browser in afresh unless
	// it is signed in as somebody else, so the page checks the session afterwards instead of assuming.
	import Icon from '../../lib/Icon.svelte';
	import ErrorBanner from '../../lib/ui/ErrorBanner.svelte';
	import PasswordField from '../../lib/ui/PasswordField.svelte';
	import { goto } from '$app/navigation';
	import { account } from '../../lib/account.svelte.ts';
	import { ApiError, resetPassword } from '../../lib/api.ts';
	import { hashParam, tokenShapeOk } from '../../lib/mode.ts';
	import { passwordError, PASSWORD_MIN } from '../../lib/validate.ts';

	const token = hashParam(location.hash, 'token');
	let gone = $state(!tokenShapeOk(token));
	let password = $state('');
	let tried = $state(false);
	let busy = $state(false);
	let done = $state(false);
	let error = $state<ApiError | null>(null);
	const passErr = $derived(passwordError(password));

	async function submit(e: SubmitEvent) {
		e.preventDefault();
		tried = true;
		if (passErr || busy || !tokenShapeOk(token)) return;
		busy = true;
		error = null;
		try {
			await resetPassword(token, password);
			password = '';
			done = true;
			await account.refresh().catch(() => {});
			if (account.mode === 'signed-in') {
				void goto('#/overview', { replace: true });
			} else {
				account.notice = 'Password changed. Sign in with the new one.';
				void goto('#/reset', { replace: true, shallow: true });
			}
		} catch (err) {
			const ae = err instanceof ApiError ? err : new ApiError(String(err), 0);
			if (ae.status === 410 || ae.code === 'token_gone' || ae.code === 'invalid_or_expired_token') gone = true;
			else error = ae;
		} finally {
			busy = false;
		}
	}
</script>

<section class="card auth-card" aria-labelledby="t">
	{#if done}
		<span class="auth-icon" aria-hidden="true"><Icon name="check" size={22} /></span>
		<h1 id="t">Password changed</h1>
		<p class="lede" role="status">Every session on this account was signed out. Sign in with the new password.</p>
		<a class="btn brand" href="#/sign-in">Sign in</a>
	{:else if gone}
		<span class="auth-icon" aria-hidden="true"><Icon name="alert" size={22} /></span>
		<h1 id="t">This link is no longer valid</h1>
		<p class="lede">Reset links work once, for one hour, and a newer link replaces older ones.</p>
		<a class="btn brand" href="#/forgot">Request a fresh link</a>
	{:else}
		<h1 id="t">Choose a new password</h1>
		<p class="lede">Saving it signs out every browser on this account.</p>
		{#if error}<ErrorBanner {error} />{/if}
		<form class="form" onsubmit={submit} novalidate>
			<input type="text" name="username" autocomplete="username" value={account.me?.email ?? ''} hidden />
			<PasswordField id="password" label="New password" bind:value={password} autocomplete="new-password" hint="At least {PASSWORD_MIN} characters." error={tried ? passErr : null} />
			<button class="btn brand" type="submit" disabled={busy}>{busy ? 'Saving…' : 'Save the new password'}</button>
		</form>
	{/if}
</section>
