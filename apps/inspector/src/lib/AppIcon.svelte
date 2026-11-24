<script lang="ts">
	// Durable Runner's app icon (the landing's AppIcon): three journal frames
	// stacked on a vermilion tile, the front one committed (solid). Decorative:
	// the product name always sits beside it.
	let { size = 32 }: { size?: number } = $props();
</script>

<span class="app-icon" style:--s="{size}px" style:--r="{Math.round(size * 0.27)}px" aria-hidden="true">
	<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-linejoin="round" focusable="false">
		<rect x="9.6" y="4.2" width="10.2" height="10.2" rx="2.6" stroke-width="1.6" opacity="0.6"></rect>
		<rect x="6.9" y="6.9" width="10.2" height="10.2" rx="2.6" stroke-width="1.6" opacity="0.85"></rect>
		<rect x="4.2" y="9.6" width="10.2" height="10.2" rx="2.6" fill="currentColor" stroke-width="1.6"></rect>
	</svg>
</span>

<style>
	.app-icon {
		display: inline-grid;
		place-items: center;
		flex: none;
		width: var(--s);
		height: var(--s);
		border-radius: var(--r);
		color: var(--c-on-brand);
		background:
			radial-gradient(120% 90% at 22% 8%, var(--c-brand-glow) 0%, transparent 48%),
			linear-gradient(150deg, var(--c-brand-1) 0%, var(--c-brand-2) 100%);
		box-shadow:
			inset 0 0 0 1px var(--c-brand-edge),
			inset 0 -6px 12px -6px var(--c-brand-shade);
	}
	.app-icon svg {
		width: 58%;
		height: 58%;
	}
</style>
