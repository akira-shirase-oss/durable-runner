<script lang="ts">
	// The shell: sidebar (a top bar with a menu on narrow screens), the page's heading row and the page. The
	// session (keys, window), the theme and the live controller start here, before any page asks for data.
	import { onDestroy } from 'svelte';
	import type { Snippet } from 'svelte';
	import { page } from '$app/state';
	import '../lib/styles/tokens.css';
	import '../lib/styles/base.css';
	import '../lib/styles/ui.css';
	import Sidebar from '../lib/ui/Sidebar.svelte';
	import TopBar from '../lib/ui/TopBar.svelte';
	import { health as getHealth, ready as getReady } from '../lib/api.ts';
	import { live, poll, Resource } from '../lib/live.svelte.ts';
	import { NAV, parseRunId, sectionOf } from '../lib/routes.ts';
	import { session } from '../lib/session.svelte.ts';
	import { theme } from '../lib/theme.svelte.ts';

	let { children }: { children: Snippet } = $props();

	// Synchronous, at the shell's creation: pages are created after this and read the keys at once.
	session.init();
	const stopTheme = theme.init();
	const stopLive = live.install();
	onDestroy(() => {
		stopTheme();
		stopLive();
	});

	const status = new Resource(
		async (signal) => {
			await getHealth(signal);
			return getReady(signal);
		},
	);
	poll(status);

	const section = $derived(sectionOf(page.route.id));
	const nav = $derived(NAV.find((n) => n.section === section)!);
	const runId = $derived(page.route.id === '/runs/[id]' ? parseRunId(page.params.id) : null);
	const title = $derived(page.route.id === '/runs/[id]' ? (runId === null ? 'Run' : `Run ${runId}`) : page.route.id === null ? 'Not found' : nav.label);
</script>

<svelte:head>
	<title>{title} · Inspector · Durable Runner</title>
</svelte:head>

<!-- a hash link would be read as a route, so the skip link moves focus itself -->
<button class="skip" onclick={() => document.getElementById('main')?.focus()}>Skip to content</button>
<div class="shell">
	<Sidebar {section} />
	<div class="content">
		<TopBar
			{title}
			monitoring={nav.monitoring && page.route.id !== null && session.metricsSupported !== false}
			health={status.data}
			healthStatus={status.status}
		/>
		<main id="main" tabindex="-1">
			{@render children()}
		</main>
	</div>
</div>

<style>
	.shell {
		display: grid;
		grid-template-columns: 256px minmax(0, 1fr);
		min-height: 100vh;
		min-height: 100dvh;
	}
	.content {
		min-width: 0;
		padding: 0 40px 64px;
	}
	.skip {
		position: fixed;
		top: 8px;
		left: 8px;
		z-index: 50;
		padding: 8px 12px;
		border: 0;
		border-radius: 10px;
		background: var(--c-card);
		box-shadow: 0 0 0 1px var(--c-bar-edge);
		color: var(--c-ink);
		font: inherit;
		font-size: 14px;
	}
	.skip:not(:focus-visible) {
		width: 1px;
		height: 1px;
		padding: 0;
		overflow: hidden;
		clip-path: inset(50%);
		white-space: nowrap;
	}
	main:focus {
		outline: none;
	}
	main {
		min-width: 0;
		padding-top: 8px;
	}
	@media (max-width: 899px) {
		.shell {
			grid-template-columns: minmax(0, 1fr);
			grid-template-rows: auto minmax(0, 1fr);
		}
		.content {
			padding: 0 16px 48px;
		}
	}
</style>
