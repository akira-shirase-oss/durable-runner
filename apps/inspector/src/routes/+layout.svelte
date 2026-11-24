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
		{ live: false },
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

<div class="shell">
	<Sidebar {section} />
	<div class="content">
		<TopBar {title} monitoring={nav.monitoring && page.route.id !== null} health={status.data} healthStatus={status.status} />
		<main id="main">
			{@render children()}
		</main>
	</div>
</div>

<style>
	.shell {
		display: grid;
		grid-template-columns: 244px minmax(0, 1fr);
		min-height: 100vh;
		min-height: 100dvh;
	}
	.content {
		min-width: 0;
		padding: 0 28px 48px 12px;
	}
	main {
		min-width: 0;
		max-width: 1360px;
		padding-top: 4px;
	}
	@media (max-width: 899px) {
		.shell {
			grid-template-columns: minmax(0, 1fr);
		}
		.content {
			padding: 0 16px 40px;
		}
	}
</style>
