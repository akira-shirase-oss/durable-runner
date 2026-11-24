<script lang="ts">
	// #/accept-invite?token=… — an invite to a workspace. Signed out: sign in (or create an account) with the
	// invited address first; the page remembers to come back. Signed in: Accept or Decline, POSTed with the
	// token on the button press. The server only accepts it for a verified session whose email is the invite's.
	import Icon from '../../lib/Icon.svelte';
	import ErrorBanner from '../../lib/ui/ErrorBanner.svelte';
	import { goto } from '$app/navigation';
	import { account } from '../../lib/account.svelte.ts';
	import { acceptInvite, ApiError, declineInvite } from '../../lib/api.ts';
	import { hashParam, tokenShapeOk } from '../../lib/mode.ts';
	import { normWorkspace } from '../../lib/saas.ts';

	const token = hashParam(location.hash, 'token');
	const valid = tokenShapeOk(token);
	const here = valid ? `#/accept-invite?token=${token}` : null;
	let busy = $state<'accept' | 'decline' | null>(null);
	let phase = $state<'ask' | 'declined' | 'gone'>(valid ? 'ask' : 'gone');
	let error = $state<ApiError | null>(null);
	const signedIn = $derived(account.mode === 'signed-in');

	function signInFirst(to: string) {
		account.rememberNext(here);
		void goto(to);
	}

	async function act(kind: 'accept' | 'decline') {
		if (!valid) return;
		busy = kind;
		error = null;
		try {
			if (kind === 'accept') {
				const r = await acceptInvite(account.cred, token);
				const ws = normWorkspace((r as { workspace?: unknown } | undefined)?.workspace);
				await account.refresh();
				if (ws) account.select(ws.public_id);
				void goto('#/overview', { replace: true });
			} else {
				await declineInvite(account.cred, token);
				phase = 'declined';
				void goto('#/accept-invite', { replace: true, shallow: true });
			}
		} catch (e) {
			const ae = e instanceof ApiError ? e : new ApiError(String(e), 0);
			if (ae.status === 410 || ae.code === 'token_gone' || ae.code === 'invalid_or_expired_token') phase = 'gone';
			else error = ae;
		} finally {
			busy = null;
		}
	}
</script>

<section class="card auth-card" aria-labelledby="t">
	<span class="auth-icon" aria-hidden="true"><Icon name="users" size={22} /></span>
	{#if phase === 'gone'}
		<h1 id="t">This invite is no longer valid</h1>
		<p class="lede">Invites work once, for 14 days, and an admin can revoke them. Ask the person who invited you for a new one.</p>
		<a class="btn" href={signedIn ? '#/overview' : '#/sign-in'}>{signedIn ? 'Go to the dashboard' : 'Sign in'}</a>
	{:else if phase === 'declined'}
		<h1 id="t">Invite declined</h1>
		<p class="lede" role="status">Nothing changed in your account. The person who invited you can send a new invite later.</p>
		<a class="btn" href="#/overview">Go to the dashboard</a>
	{:else if !signedIn}
		<h1 id="t">You have been invited to a workspace</h1>
		<p class="lede">Sign in with the address the invite was sent to, or create an account with it. New accounts verify their email first, then open this invite link again.</p>
		<div class="form-actions">
			<button class="btn brand sm" onclick={() => signInFirst('#/sign-in')}>Sign in to accept</button>
			<button class="btn sm" onclick={() => signInFirst(`#/sign-up?invite=${token}`)}>Create an account</button>
		</div>
	{:else}
		<h1 id="t">Join the workspace?</h1>
		<p class="lede">You are signed in as <strong>{account.me?.email}</strong>. Accepting adds you with the role the invite names; an admin can change it later.</p>
		{#if !account.verified}
			<p class="banner warn"><Icon name="mail" size={16} /><span class="msg">Verify your email first: invites can only be accepted by a verified account.</span></p>
		{/if}
		{#if error}
			<ErrorBanner {error} />
			{#if error.code === 'invite_email_mismatch'}
				<button class="btn sm" onclick={() => { account.rememberNext(here); void account.signOut(); }}><Icon name="log-out" />Sign out and use the invited address</button>
			{/if}
		{/if}
		<div class="form-actions">
			<button class="btn brand sm" disabled={busy !== null || !account.verified} onclick={() => act('accept')}><Icon name="check" />{busy === 'accept' ? 'Joining…' : 'Accept and join'}</button>
			<button class="btn sm" disabled={busy !== null} onclick={() => act('decline')}>{busy === 'decline' ? 'Declining…' : 'Decline'}</button>
		</div>
	{/if}
</section>
