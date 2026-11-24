<script lang="ts">
	// The time window of the monitoring pages: 15m, 1h, 6h, 24h (a radio group: arrow keys move the choice).
	import { session } from '../session.svelte.ts';
	import { WINDOWS, WINDOW_LABEL } from '../series.ts';
	import type { Window } from '../api.ts';

	let buttons: HTMLButtonElement[] = $state([]);

	function key(e: KeyboardEvent, i: number) {
		let j = i;
		if (e.key === 'ArrowRight' || e.key === 'ArrowDown') j = (i + 1) % WINDOWS.length;
		else if (e.key === 'ArrowLeft' || e.key === 'ArrowUp') j = (i - 1 + WINDOWS.length) % WINDOWS.length;
		else return;
		e.preventDefault();
		session.setWindow(WINDOWS[j] as Window);
		buttons[j]?.focus();
	}
</script>

<div class="seg" role="radiogroup" aria-label="Time window">
	{#each WINDOWS as w, i (w)}
		<button
			bind:this={buttons[i]}
			role="radio"
			aria-checked={session.window === w}
			tabindex={session.window === w ? 0 : -1}
			title={WINDOW_LABEL[w]}
			onclick={() => session.setWindow(w)}
			onkeydown={(e) => key(e, i)}>{w}</button
		>
	{/each}
</div>

<style>
	.seg button {
		min-width: 40px;
		padding: 0 9px;
		font-variant-numeric: tabular-nums;
	}
</style>
