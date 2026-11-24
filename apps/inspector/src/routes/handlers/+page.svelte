<script lang="ts">
	// #/handlers. Compat: handler traffic from GET /metrics, as before. Signed in: the workspace's registry
	// (register, edit, remove, test call, the signing secret and how to verify it) above the same traffic view.
	import MonitoringHandlers from '../../lib/pages/MonitoringHandlers.svelte';
	import HandlerRegistry from '../../lib/pages/HandlerRegistry.svelte';
	import { session } from '../../lib/session.svelte.ts';
</script>

{#if session.signedIn}
	<div class="page">
		<HandlerRegistry />
		<MonitoringHandlers />
	</div>
{:else}
	<MonitoringHandlers />
{/if}
