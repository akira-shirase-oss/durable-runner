<script lang="ts">
	// #/overview (signed in): the setup checklist until it is done, then the workspace's health at a glance — the
	// monitoring overview's KPI tiles, charts, runs by state, activity and handlers, all workspace-scoped.
	import Onboarding from '../../lib/ui/Onboarding.svelte';
	import MonitoringOverview from '../../lib/pages/MonitoringOverview.svelte';
</script>

<div class="page">
	<Onboarding />
	<MonitoringOverview ops={false} />
</div>
