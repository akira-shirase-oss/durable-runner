<script lang="ts">
	// The page's heading row: its title (the page's h1), the time window on monitoring pages, the live indicator
	// and the server's status. Sticky at the top of the content column.
	import HealthPill from './HealthPill.svelte';
	import LiveIndicator from './LiveIndicator.svelte';
	import WindowPicker from './WindowPicker.svelte';
	import type { ResourceStatus } from '../live.svelte.ts';

	let {
		title,
		monitoring,
		health,
		healthStatus,
	}: {
		title: string;
		monitoring: boolean;
		health: { ready: boolean; code: string | null } | null;
		healthStatus: ResourceStatus;
	} = $props();
</script>

<header class="top">
	<h1>{title}</h1>
	<div class="controls">
		{#if monitoring}<WindowPicker />{/if}
		<LiveIndicator />
		<HealthPill data={health} status={healthStatus} />
	</div>
</header>

<style>
	.top {
		position: sticky;
		top: 0;
		z-index: 5;
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		justify-content: space-between;
		gap: 10px 16px;
		min-height: 68px;
		padding: 14px 0 12px;
		background: var(--c-app);
	}
	h1 {
		margin: 0;
		font-size: 22px;
		font-weight: 580;
		letter-spacing: -0.02em;
		line-height: 1.2;
		overflow-wrap: anywhere;
	}
	.controls {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 8px 10px;
		min-width: 0;
	}
	@media (max-width: 899px) {
		.top {
			position: static;
			min-height: 0;
			padding: 16px 0 4px;
		}
		h1 {
			font-size: 20px;
		}
		.controls {
			width: 100%;
		}
	}
</style>
