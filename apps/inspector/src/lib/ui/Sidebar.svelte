<script lang="ts">
	// The dashboard's navigation. Wide: a left sidebar (brand, an optional top slot — the workspace switcher when
	// signed in —, sections, then the foot: keys, docs and the theme in compat mode; docs and the account menu
	// signed in). Narrow (< 900 px): a top bar with the brand and a Menu button that opens the same list below it.
	import type { Snippet } from 'svelte';
	import AppIcon from '../AppIcon.svelte';
	import Icon, { type IconName } from '../Icon.svelte';
	import ThemeToggle from './ThemeToggle.svelte';
	import { afterNavigate } from '$app/navigation';
	import { NAV, type NavItem, type Section } from '../routes.ts';
	import { session } from '../session.svelte.ts';

	let {
		section,
		nav = NAV,
		home = '#/',
		sub = 'Inspector',
		top,
		foot,
	}: { section: Section; nav?: readonly NavItem[]; home?: string; sub?: string; top?: Snippet; foot?: Snippet } = $props();

	const ICONS: Record<Section, IconName> = {
		overview: 'dashboard',
		runs: 'list',
		workflows: 'workflow',
		handlers: 'webhook',
		keys: 'key',
		usage: 'gauge',
		team: 'users',
		submit: 'file-plus',
		admin: 'wrench',
		settings: 'settings',
	};
	const DOCS = 'https://docs.durable-runner.404human.dev/inspector/';
	const docs = $derived(foot ? 'https://docs.durable-runner.404human.dev/' : DOCS);

	let open = $state(false);
	let menuButton: HTMLButtonElement | undefined = $state();

	afterNavigate(() => (open = false));

	function onKey(e: KeyboardEvent) {
		if (e.key === 'Escape' && open) {
			open = false;
			menuButton?.focus();
		}
	}
</script>

<svelte:window onkeydown={onKey} />

<aside class="side" class:open aria-label="Navigation">
	<div class="head">
		<a class="brand" href={home}>
			<AppIcon size={30} />
			<span class="names"><span class="name">Durable Runner</span> <span class="sub">{sub}</span></span>
		</a>
		<button
			class="btn ghost menu"
			bind:this={menuButton}
			aria-expanded={open}
			aria-controls="side-menu"
			onclick={() => (open = !open)}
		>
			<Icon name={open ? 'x' : 'menu'} size={18} /><span>Menu</span>
		</button>
	</div>
	<div class="menu-panel" id="side-menu">
		{#if top}<div class="top-slot">{@render top()}</div>{/if}
		<nav aria-label="Sections">
			<ul>
				{#each nav as n (n.section)}
					<li>
						<a href={n.href} aria-current={section === n.section ? 'page' : undefined}>
							<Icon name={ICONS[n.section]} size={17} />{n.label}
						</a>
					</li>
				{/each}
			</ul>
		</nav>
		<div class="foot">
			{#if foot}
				<a class="docs" href={docs} target="_blank" rel="noopener noreferrer"><Icon name="book" size={16} />Docs<Icon name="arrow-up-right" size={13} /><span class="sr">(opens in a new tab)</span></a>
				{@render foot()}
			{:else}
			<div class="keys">
				<p class="keys-title"><Icon name="key" size={14} />Keys for this tab</p>
				<p class="key-row"><span>Read key</span><em class="badge" class:set={!!session.readKey}>{session.readKey ? 'set' : 'not set'}</em></p>
				<p class="key-row"><span>API key</span><em class="badge" class:set={!!session.apiKey}>{session.apiKey ? 'set' : 'not set'}</em></p>
				<div class="key-actions">
					<a class="btn sm" href="#/settings">Manage</a>
					<button class="btn danger sm" disabled={!session.readKey && !session.apiKey} onclick={() => session.forget()}>Forget keys</button>
				</div>
			</div>
			<div class="foot-row">
				<a class="docs" href={DOCS} target="_blank" rel="noopener noreferrer"><Icon name="book" size={16} />Docs<Icon name="arrow-up-right" size={13} /><span class="sr">(opens in a new tab)</span></a>
				<ThemeToggle />
			</div>
			{/if}
		</div>
	</div>
</aside>

<style>
	.side {
		position: sticky;
		top: 0;
		display: flex;
		flex-direction: column;
		gap: 18px;
		height: 100vh;
		height: 100dvh;
		padding: 22px 16px 16px;
		overflow-y: auto;
		background: var(--c-side);
		border-right: 1px solid var(--c-side-edge);
	}
	.head {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 10px;
	}
	.brand {
		display: flex;
		align-items: center;
		gap: 10px;
		padding: 4px 6px;
		border-radius: 12px;
		color: var(--c-ink);
		text-decoration: none;
	}
	.names {
		display: flex;
		flex-direction: column;
		line-height: 1.15;
	}
	.name {
		font-size: 16px;
		font-weight: 600;
		letter-spacing: -0.015em;
		white-space: nowrap;
	}
	.sub {
		color: var(--c-body);
		font-size: 12.5px;
		font-weight: 460;
	}
	.menu {
		display: none;
	}
	.menu-panel {
		display: flex;
		flex: 1;
		flex-direction: column;
		gap: 18px;
		min-height: 0;
	}
	.menu-panel nav {
		flex: 1;
	}
	.top-slot {
		margin-top: -4px;
	}
	nav ul {
		display: flex;
		flex-direction: column;
		gap: 2px;
		margin: 0;
		padding: 0;
		list-style: none;
	}
	nav a {
		display: flex;
		align-items: center;
		gap: 11px;
		height: 38px;
		padding: 0 11px;
		border-radius: 11px;
		color: var(--c-ink-soft);
		font-size: 14.5px;
		font-weight: 520;
		text-decoration: none;
		transition:
			background-color 150ms ease-out,
			color 150ms ease-out;
	}
	nav a :global(svg) {
		color: var(--c-body);
	}
	nav a:hover {
		background: var(--c-hover);
		color: var(--c-ink);
	}
	nav a[aria-current='page'] {
		background: var(--c-card);
		color: var(--c-ink);
		box-shadow:
			0 0 0 1px var(--c-bar-edge),
			0 1px 3px var(--c-tile-shadow);
	}
	nav a[aria-current='page'] :global(svg) {
		color: var(--c-brand-deep);
	}
	.foot {
		display: flex;
		flex-direction: column;
		gap: 10px;
	}
	.keys {
		display: flex;
		flex-direction: column;
		gap: 6px;
		padding: 12px;
		border-radius: 14px;
		background: var(--c-card);
		box-shadow: 0 0 0 1px var(--c-bar-edge);
	}
	.keys p {
		margin: 0;
	}
	.keys-title {
		display: flex;
		align-items: center;
		gap: 7px;
		margin-bottom: 2px !important;
		color: var(--c-ink);
		font-size: 13px;
		font-weight: 560;
	}
	.keys-title :global(svg) {
		color: var(--c-body);
	}
	.key-row {
		display: flex;
		align-items: center;
		justify-content: space-between;
		color: var(--c-ink-soft);
		font-size: 13px;
	}
	.key-actions {
		display: flex;
		gap: 6px;
		margin-top: 4px;
	}
	.key-actions .btn {
		flex: 1;
		height: 30px;
		padding: 0 8px;
		font-size: 13px;
		text-decoration: none;
	}
	.foot-row {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 8px;
	}
	.docs {
		display: inline-flex;
		align-self: flex-start;
		align-items: center;
		gap: 8px;
		height: 36px;
		padding: 0 10px;
		border-radius: 10px;
		color: var(--c-ink-soft);
		font-size: 14px;
		font-weight: 520;
		text-decoration: none;
		transition: background-color 150ms ease-out;
	}
	.docs:hover {
		background: var(--c-hover);
		color: var(--c-ink);
	}
	.docs :global(svg) {
		color: var(--c-body);
	}

	/* narrow: a top bar; the menu opens the sections, keys and theme below it */
	@media (max-width: 899px) {
		.side {
			position: static;
			gap: 0;
			height: auto;
			padding: 10px 12px;
			overflow: visible;
			border-bottom: 1px solid var(--c-line);
			background: var(--c-card);
		}
		.brand .sub {
			display: none;
		}
		.name {
			font-size: 16.5px;
		}
		.menu {
			display: inline-flex;
			height: 38px;
			padding: 0 12px;
			border-color: var(--c-line-strong);
		}
		.menu-panel {
			display: none;
			padding: 12px 2px 4px;
		}
		.side.open .menu-panel {
			display: flex;
		}
		nav a {
			height: 44px;
		}
	}
</style>
