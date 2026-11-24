<script lang="ts">
	// A loading placeholder for a list: still, grey bars (no shimmer: nothing animates while waiting).
	let { rows = 3, label = 'Loading' }: { rows?: number; label?: string } = $props();
</script>

<div class="skel-rows" role="status" aria-label={label}>
	{#each Array.from({ length: rows }, (_, i) => i) as i (i)}
		<span class="skeleton" style:width="{[62, 44, 54, 38][i % 4]}%"></span>
	{/each}
</div>
