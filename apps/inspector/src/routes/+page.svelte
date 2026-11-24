<script lang="ts">
	// #/ — the compat console's overview (no account store). Signed in, the shell sends #/ to #/overview.
	import MonitoringOverview from '../lib/pages/MonitoringOverview.svelte';
	import { session } from '../lib/session.svelte.ts';
</script>

{#if !session.signedIn}<MonitoringOverview ops />{/if}
