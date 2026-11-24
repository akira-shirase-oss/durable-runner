<script lang="ts">
	// The verify-wall for a signed-in account whose email is not verified yet: what is blocked, where the link
	// went, and Resend. Verifying signs every session out (a mailed link never signs anybody in), so the copy
	// says to sign in again after opening it. `compact` is the banner form shown above a page.
	import Icon from '../Icon.svelte';
	import ErrorBanner from './ErrorBanner.svelte';
	import { account } from '../account.svelte.ts';
	import { ApiError, resendVerification } from '../api.ts';

	let { compact = false }: { compact?: boolean } = $props();
	let busy = $state(false);
	let sent = $state(false);
	let error = $state<ApiError | null>(null);

	async function resend() {
		if (!account.me) return;
		busy = true;
		error = null;
		try {
			await resendVerification(account.me.email);
			sent = true;
		} catch (e) {
			error = e instanceof ApiError ? e : new ApiError(String(e), 0);
		} finally {
			busy = false;
		}
	}
</script>

{#if compact}
	<p class="banner warn">
		<Icon name="mail" size={16} />
		<span class="msg"><strong>Verify your email to make changes.</strong> The link went to {account.me?.email}. {sent ? 'A fresh link is on its way.' : ''}</span>
		<button class="btn ghost sm act" disabled={busy || sent} onclick={resend}>{busy ? 'Sending…' : 'Resend link'}</button>
	</p>
	{#if error}<ErrorBanner {error} />{/if}
{:else}
	<section class="card wall" aria-labelledby="wall-title">
		<span class="wall-icon" aria-hidden="true"><Icon name="mail" size={22} /></span>
		<h2 id="wall-title">Verify your email to start</h2>
		<p class="hint">
			We sent a link to <strong>{account.me?.email}</strong>. Open it within 24 hours to create a workspace, register
			handlers and send jobs. Until then you can look around; nothing can be changed.
		</p>
		<p class="hint">Opening the link signs this browser out (a mailed link never signs anyone in), so sign in again afterwards.</p>
		{#if error}<ErrorBanner {error} />{/if}
		{#if sent}<p class="banner notice" role="status"><Icon name="check" size={16} /><span class="msg">If the address still needs verifying, a fresh link is on its way. The newest link replaces the older ones.</span></p>{/if}
		<div class="form-actions">
			<button class="btn brand sm" disabled={busy || sent} onclick={resend}><Icon name="send" />{busy ? 'Sending…' : 'Resend the link'}</button>
			<button class="btn sm" onclick={() => void account.signOut()}><Icon name="log-out" />Sign out</button>
		</div>
	</section>
{/if}

<style>
	.wall {
		display: flex;
		flex-direction: column;
		align-items: flex-start;
		gap: 10px;
		max-width: 720px;
		padding: 26px;
	}
	.wall h2 {
		font-size: 19px;
	}
	.wall .hint {
		max-width: 62ch;
		margin: 0;
		line-height: 1.55;
	}
	.wall-icon {
		display: grid;
		place-items: center;
		width: 46px;
		height: 46px;
		border-radius: 14px;
		background: var(--c-brand-tint);
		color: var(--c-brand-deep);
	}
</style>
