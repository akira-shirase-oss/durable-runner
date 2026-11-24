<script lang="ts">
	// #/settings: compat keys and theme (the signed-in account settings land next).
	import CompatSettings from '../../lib/pages/CompatSettings.svelte';
	import { session } from '../../lib/session.svelte.ts';
</script>

{#if !session.signedIn}<CompatSettings />{/if}
