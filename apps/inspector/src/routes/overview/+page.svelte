<script lang="ts">
	// #/overview (signed in): the setup checklist until it is done, then the workspace's health at a glance — the
	// monitoring overview's KPI tiles, charts, runs by state, activity and handlers, all workspace-scoped. A
	// workspace that has never had a run shows the checklist instead of empty charts.
	import Icon from '../../lib/Icon.svelte';
	import Onboarding from '../../lib/ui/Onboarding.svelte';
	import MonitoringOverview from '../../lib/pages/MonitoringOverview.svelte';
	import { account } from '../../lib/account.svelte.ts';
	import { workspaceDiagnostics } from '../../lib/api.ts';
	import { poll, Resource } from '../../lib/live.svelte.ts';

	let setup = $state(false);
	const diag = new Resource((signal) => workspaceDiagnostics(account.cred, signal), { interval: () => (setup ? 10_000 : 0), live: false });
	poll(diag, () => setup);
	// no run in the index at all (and the checklist still open): charts would be empty, so they wait
	const empty = $derived(setup && diag.status === 'ok' && diag.data !== null && diag.data.live_runs === 0);
</script>

<div class="page">
	<Onboarding bind:open={setup} />
	{#if empty}
		<p class="banner hint"><Icon name="chart" size={16} /><span class="msg">Charts, the activity feed and handler traffic appear here once this workspace has its first run.</span></p>
	{:else}
		<MonitoringOverview ops={false} />
	{/if}
</div>
