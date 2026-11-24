<script lang="ts">
	// System / Light / Dark as a radio group (arrow keys move the choice), stored in this browser.
	import { theme } from '../theme.svelte.ts';
	import type { Choice } from '../theme.ts';

	let { labelledby }: { labelledby: string } = $props();
	const THEMES: { id: Choice; label: string }[] = [
		{ id: 'system', label: 'System' },
		{ id: 'light', label: 'Light' },
		{ id: 'dark', label: 'Dark' },
	];
	let buttons: HTMLButtonElement[] = $state([]);
	function key(e: KeyboardEvent, i: number) {
		let j = i;
		if (e.key === 'ArrowRight' || e.key === 'ArrowDown') j = (i + 1) % THEMES.length;
		else if (e.key === 'ArrowLeft' || e.key === 'ArrowUp') j = (i - 1 + THEMES.length) % THEMES.length;
		else return;
		e.preventDefault();
		theme.set(THEMES[j]!.id);
		buttons[j]?.focus();
	}
</script>

<div class="seg" role="radiogroup" aria-labelledby={labelledby}>
	{#each THEMES as t, i (t.id)}
		<button
			bind:this={buttons[i]}
			role="radio"
			aria-checked={theme.choice === t.id}
			tabindex={theme.choice === t.id ? 0 : -1}
			onclick={() => theme.set(t.id)}
			onkeydown={(e) => key(e, i)}>{t.label}</button
		>
	{/each}
</div>
