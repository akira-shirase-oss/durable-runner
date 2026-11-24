<script lang="ts">
	// #/settings. Compat: the key-paste card, the theme and the page notes, as before. Signed in: the selected
	// workspace (rename, delete) and the personal section (password, sessions, theme).
	import AccountSettings from '../../lib/pages/AccountSettings.svelte';
	import CompatSettings from '../../lib/pages/CompatSettings.svelte';
	import { session } from '../../lib/session.svelte.ts';
</script>

{#if session.signedIn}<AccountSettings />{:else}<CompatSettings />{/if}
