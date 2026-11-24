<script lang="ts">
	// The landing's theme toggle: shows the moon in light and the sun in dark; follows the device until pressed,
	// and pressing it onto the device's own scheme follows the device again.
	import Icon from '../Icon.svelte';
	import { theme } from '../theme.svelte.ts';

	let { label = false }: { label?: boolean } = $props();
	const dark = $derived(theme.scheme === 'dark');
	const title = $derived(`${dark ? 'Switch to light' : 'Switch to dark'}${theme.choice === 'system' ? ' (following your system)' : ''}`);
</script>

<button class="btn ghost toggle" class:icon={!label} aria-label={label ? undefined : 'Dark theme'} aria-pressed={dark} {title} onclick={() => theme.toggle()}>
	<Icon name={dark ? 'sun' : 'moon'} size={17} />{#if label}<span>Dark theme</span>{/if}
</button>

<style>
	.toggle {
		color: var(--c-ink-soft);
	}
</style>
