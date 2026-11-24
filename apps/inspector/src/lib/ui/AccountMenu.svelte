<script lang="ts">
	// The account menu at the foot of the sidebar: who is signed in, the theme switch, personal settings and
	// Sign out. A disclosure that opens upwards; Escape or a click outside closes it.
	import Icon from '../Icon.svelte';
	import { account } from '../account.svelte.ts';
	import { theme } from '../theme.svelte.ts';

	let open = $state(false);
	let busy = $state(false);
	let root: HTMLDivElement | undefined = $state();
	let button: HTMLButtonElement | undefined = $state();
	const dark = $derived(theme.scheme === 'dark');

	async function signOut() {
		busy = true;
		try {
			await account.signOut();
		} finally {
			busy = false;
		}
	}
	function onKey(e: KeyboardEvent) {
		if (e.key === 'Escape' && open) {
			open = false;
			button?.focus();
		}
	}
	function onDoc(e: MouseEvent) {
		if (open && root && !root.contains(e.target as Node)) open = false;
	}
</script>

<svelte:window onkeydown={onKey} onclick={onDoc} />

<div class="acct" bind:this={root}>
	{#if open}
		<div class="pop" id="acct-menu">
			<div class="who">
				<span class="label">Signed in as</span>
				<span class="email">{account.me?.email ?? ''}</span>
				{#if account.me && !account.me.verified}<span class="badge warn">Email not verified</span>{/if}
			</div>
			<a class="item" href="#/settings" onclick={() => (open = false)}><Icon name="user" size={16} />Account settings</a>
			<button class="item" aria-pressed={dark} onclick={() => theme.toggle()}><Icon name={dark ? 'sun' : 'moon'} size={16} />Dark theme<span class="state">{dark ? 'On' : 'Off'}{theme.choice === 'system' ? ' · system' : ''}</span></button>
			<button class="item" disabled={busy} onclick={signOut}><Icon name="log-out" size={16} />{busy ? 'Signing out…' : 'Sign out'}</button>
		</div>
	{/if}
	<button class="acct-btn" bind:this={button} aria-expanded={open} aria-controls="acct-menu" onclick={() => (open = !open)}>
		<span class="avatar" aria-hidden="true"><Icon name="user" size={15} /></span>
		<span class="email">{account.me?.email ?? 'Account'}</span>
		<Icon name="chevrons-up-down" size={15} />
		<span class="sr">: account menu</span>
	</button>
</div>

<style>
	.acct {
		position: relative;
	}
	.acct-btn {
		display: flex;
		align-items: center;
		gap: 9px;
		width: 100%;
		min-height: 44px;
		padding: 6px 10px 6px 6px;
		border: 0;
		border-radius: 12px;
		background: transparent;
		color: var(--c-ink);
		font: inherit;
		text-align: left;
		cursor: pointer;
		transition: background-color 150ms ease-out;
	}
	.acct-btn:hover {
		background: var(--c-hover);
	}
	.acct-btn > :global(svg) {
		flex: none;
		color: var(--c-body);
	}
	.avatar {
		display: grid;
		flex: none;
		place-items: center;
		width: 30px;
		height: 30px;
		border-radius: 50%;
		background: var(--c-surface);
		box-shadow: inset 0 0 0 1px var(--c-line);
		color: var(--c-ink-soft);
	}
	.email {
		flex: 1;
		min-width: 0;
		overflow: hidden;
		font-size: 13.5px;
		font-weight: 520;
		text-overflow: ellipsis;
		white-space: nowrap;
	}
	.pop {
		position: absolute;
		right: 0;
		bottom: calc(100% + 6px);
		left: 0;
		z-index: 20;
		display: flex;
		flex-direction: column;
		gap: 2px;
		padding: 8px;
		border-radius: 14px;
		background: var(--c-card);
		box-shadow:
			0 0 0 1px var(--c-bar-edge),
			0 12px 28px -10px var(--c-shadow);
		animation: pop-up 120ms ease-out;
	}
	.who {
		display: flex;
		flex-direction: column;
		align-items: flex-start;
		gap: 3px;
		padding: 6px 8px 10px;
		border-bottom: 1px solid var(--c-line);
		margin-bottom: 4px;
	}
	.who .label {
		color: var(--c-body);
		font-size: 12.5px;
	}
	.who .email {
		max-width: 100%;
		font-size: 14px;
		overflow-wrap: anywhere;
		white-space: normal;
	}
	.item {
		display: flex;
		align-items: center;
		gap: 10px;
		width: 100%;
		min-height: 38px;
		padding: 0 8px;
		border: 0;
		border-radius: 10px;
		background: transparent;
		color: var(--c-ink);
		font: inherit;
		font-size: 14px;
		text-align: left;
		text-decoration: none;
		cursor: pointer;
	}
	.item:hover:not(:disabled) {
		background: var(--c-hover);
	}
	.item:disabled {
		opacity: 0.6;
	}
	.item :global(svg) {
		color: var(--c-body);
	}
	.state {
		margin-left: auto;
		color: var(--c-body);
		font-size: 12.5px;
	}
	@keyframes pop-up {
		from {
			opacity: 0;
			transform: translateY(4px);
		}
	}
</style>
