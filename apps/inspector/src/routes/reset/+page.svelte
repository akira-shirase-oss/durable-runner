<script lang="ts">
	// #/reset?token=… — choose a new password. The token stays in the fragment and is POSTed on submit with the
	// new password. A reset signs out every session of the account and sets no cookie, so success ends at sign-in
	// with a note — unless this tab holds a different account's session, which the page reads rather than assumes.
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
			// who this tab was before: a reset signs out every session of ITS account, never another one's
			const before = account.mode === 'signed-in' ? (account.me?.email ?? '') : null;
			await resetPassword(token, password);
			password = '';
			// read what is true now, assume nothing (a server may or may not sign this browser in)
			await account.refresh().catch(() => {});
			if (account.mode === 'signed-in' && before === null) {
				// signed out before, signed in now: the server opened a fresh session for the reset account
				void goto('#/overview', { replace: true });
			} else if (account.mode === 'signed-in') {
				// this tab holds another account's session, which a reset does not touch
				done = true;
				void goto('#/reset', { replace: true, shallow: true });
			} else {
				account.notice = 'Password changed. Every session on that account was signed out: sign in with the new password.';
				// replace: the token leaves the address bar and the history
				void goto('#/sign-in', { replace: true });
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
		<p class="lede" role="status">Every session on that account was signed out. This browser is signed in as {account.me?.email ?? 'another account'}, which the reset did not touch: sign out first to use the new password here.</p>
		<a class="btn brand" href="#/overview">Go to the dashboard</a>
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
