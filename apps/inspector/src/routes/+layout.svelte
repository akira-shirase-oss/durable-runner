<script lang="ts">
	// The shell. It asks the server which dashboard this is (GET /auth/session, mode.ts) and then renders one of:
	//   compat     — the key console exactly as before: sidebar with the keys, the monitoring pages, Submit, Admin;
	//   signed-out — the auth pages in a bare frame (no sidebar);
	//   signed-in  — the SaaS dashboard: workspace switcher, the S3 navigation, the account menu, and the page
	//                keyed on the selected workspace (a switch starts every page fresh).
	// A route the mode does not have is replaced at once (mode.ts redirectFor). The session (keys, window), the
	// theme and the live controller start here, before any page asks for data.
	import { onDestroy } from 'svelte';
	import type { Snippet } from 'svelte';
	import { goto } from '$app/navigation';
	import { page } from '$app/state';
	import '../lib/styles/tokens.css';
	import '../lib/styles/base.css';
	import '../lib/styles/ui.css';
	import Icon from '../lib/Icon.svelte';
	import AccountMenu from '../lib/ui/AccountMenu.svelte';
	import AuthShell from '../lib/ui/AuthShell.svelte';
	import BootScreen from '../lib/ui/BootScreen.svelte';
	import CopyField from '../lib/ui/CopyField.svelte';
	import CreateWorkspace from '../lib/ui/CreateWorkspace.svelte';
	import Modal from '../lib/ui/Modal.svelte';
	import Sidebar from '../lib/ui/Sidebar.svelte';
	import TopBar from '../lib/ui/TopBar.svelte';
	import VerifyWall from '../lib/ui/VerifyWall.svelte';
	import WorkspaceSwitcher from '../lib/ui/WorkspaceSwitcher.svelte';
	import CodeBlock from '../lib/ui/CodeBlock.svelte';
	import { account } from '../lib/account.svelte.ts';
	import { health as getHealth, ready as getReady } from '../lib/api.ts';
	import { live, poll, Resource } from '../lib/live.svelte.ts';
	import { isAuthSection, redirectFor, safeNext, segmentOf } from '../lib/mode.ts';
	import { NAV, parseRunId, SAAS_NAV, sectionOf } from '../lib/routes.ts';
	import { session } from '../lib/session.svelte.ts';
	import { theme } from '../lib/theme.svelte.ts';
	import { ui } from '../lib/ui-state.svelte.ts';

	let { children }: { children: Snippet } = $props();

	// Synchronous, at the shell's creation: pages are created after this and read the keys at once.
	session.init();
	const stopTheme = theme.init();
	const stopLive = live.install();
	void account.boot();
	onDestroy(() => {
		stopTheme();
		stopLive();
	});

	const status = new Resource(async (signal) => {
		await getHealth(signal);
		return getReady(signal);
	});
	poll(status);

	const mode = $derived(account.mode);
	const seg = $derived(segmentOf(page.route.id));
	const target = $derived(redirectFor(mode, page.route.id));
	$effect(() => {
		if (target === null) return;
		// a signed-out visitor sent to sign-in comes back here afterwards (in-app routes only: mode.ts safeNext)
		if (mode === 'signed-out' && !isAuthSection(seg)) account.rememberNext(location.hash || null);
		// just signed in on an auth page: back to where the visitor was going, if anywhere
		const to = mode === 'signed-in' && isAuthSection(seg) ? (safeNext(account.takeNext()) ?? target) : target;
		void goto(to, { replace: true });
	});

	const nav = $derived(mode === 'signed-in' ? SAAS_NAV : NAV);
	const section = $derived(sectionOf(page.route.id, nav));
	const navItem = $derived(nav.find((n) => n.section === section) ?? nav[0]!);
	const runId = $derived(page.route.id === '/runs/[id]' ? parseRunId(page.params.id) : null);
	const AUTH_TITLES: Record<string, string> = {
		'sign-in': 'Sign in',
		'sign-up': 'Create your account',
		verify: 'Verify your email',
		'verify-sent': 'Check your inbox',
		forgot: 'Reset your password',
		reset: 'Choose a new password',
		'accept-invite': 'Join a workspace',
	};
	const title = $derived(
		page.route.id === '/runs/[id]'
			? runId === null
				? 'Run'
				: `Run ${runId}`
			: (page.route.id as string | null) === '/workflows/[id]'
				? 'Workflow'
				: page.route.id === null
					? 'Not found'
					: isAuthSection(seg)
						? AUTH_TITLES[seg]!
						: navItem.label,
	);
	const product = $derived(mode === 'compat' ? 'Inspector · Durable Runner' : 'Durable Runner');
	// workspace pages need a workspace; personal settings do not
	const needsWorkspace = $derived(mode === 'signed-in' && account.workspaces.length === 0 && section !== 'settings' && page.route.id !== null);
	const revealOpen = $derived(ui.reveal !== null);
</script>

<svelte:head>
	<title>{title} · {product}</title>
</svelte:head>

{#if mode === 'booting' || (target !== null && mode !== 'error')}
	<BootScreen />
{:else if mode === 'error'}
	<BootScreen error={account.bootError} retry={() => void account.boot()} />
{:else if mode === 'signed-out' || (mode === 'signed-in' && isAuthSection(seg))}
	<AuthShell>{@render children()}</AuthShell>
{:else}
	<!-- a hash link would be read as a route, so the skip link moves focus itself -->
	<button class="skip" onclick={() => document.getElementById('main')?.focus()}>Skip to content</button>
	<div class="shell">
		{#if mode === 'signed-in'}
			<Sidebar {section} {nav} home="#/overview" sub="Dashboard">
				{#snippet top()}<WorkspaceSwitcher />{/snippet}
				{#snippet foot()}<AccountMenu />{/snippet}
			</Sidebar>
		{:else}
			<Sidebar {section} />
		{/if}
		<div class="content">
			<TopBar
				{title}
				monitoring={navItem.monitoring && page.route.id !== null && session.metricsSupported !== false && !needsWorkspace}
				health={status.data}
				healthStatus={status.status}
			/>
			<main id="main" tabindex="-1">
				{#if mode === 'signed-in'}
					{#if needsWorkspace}
						<div class="page">
							{#if !account.verified}
								<VerifyWall />
							{:else}
								<section class="card first-ws" aria-labelledby="first-ws-title">
									<span class="first-icon" aria-hidden="true"><Icon name="box" size={22} /></span>
									<h2 id="first-ws-title">Create your first workspace</h2>
									<p class="hint">
										A workspace holds your runs, handlers, API keys and team. You can make up to three on the Free plan, and
										invite people to each. Invited to a team already? Open the invite link from your mail instead.
									</p>
									<CreateWorkspace idPrefix="first" />
								</section>
							{/if}
						</div>
					{:else}
						{#key account.generation}
							{#if !account.verified}<div class="wall-banner"><VerifyWall compact /></div>{/if}
							{@render children()}
						{/key}
					{/if}
				{:else}
					{@render children()}
				{/if}
			</main>
		</div>
	</div>
{/if}

{#if mode === 'signed-in'}
	<Modal bind:open={ui.createOpen} title="New workspace" id="new-ws">
		<CreateWorkspace idPrefix="modal" oncreated={() => (ui.createOpen = false)} />
	</Modal>
{/if}
<Modal open={revealOpen} title={ui.reveal?.title ?? ''} id="reveal" dismissible={false} onclose={() => ui.dismiss()}>
	{#if ui.reveal}
		<div class="reveal">
			<p class="banner warn"><Icon name="eye" size={16} /><span class="msg"><strong>Shown once.</strong> {ui.reveal.note}</span></p>
			<CopyField value={ui.reveal.value} label={ui.reveal.label} id="reveal-value" />
			{#if ui.reveal.usage}<CodeBlock code={ui.reveal.usage} label="Use it" />{/if}
			<div class="form-actions">
				<button class="btn brand sm" onclick={() => ui.dismiss()}><Icon name="check" />I have copied it</button>
			</div>
		</div>
	{/if}
</Modal>

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
	.wall-banner {
		margin-bottom: 16px;
	}
	.first-ws {
		display: flex;
		flex-direction: column;
		align-items: stretch;
		gap: 10px;
		max-width: 640px;
		padding: 26px;
	}
	.first-ws h2 {
		font-size: 19px;
	}
	.first-ws .hint {
		margin: 0 0 6px;
		line-height: 1.55;
	}
	.first-icon {
		display: grid;
		place-items: center;
		width: 46px;
		height: 46px;
		border-radius: 14px;
		background: var(--c-brand-tint);
		color: var(--c-brand-deep);
	}
	.reveal {
		display: flex;
		flex-direction: column;
		gap: 14px;
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
