<script lang="ts">
	// A KPI tile's trend: a 2 px line with a 10% wash and an end dot, no axes. Decorative (aria-hidden): the tile
	// states the value and the trend in words. Null samples break the line.
	import { areaPath, linePath } from '../series.ts';

	let {
		values,
		color = 'var(--c-chart-1)',
		height = 34,
		min0 = true,
	}: { values: (number | null)[]; color?: string; height?: number; min0?: boolean } = $props();

	let width = $state(0);

	const geom = $derived.by(() => {
		const n = values.length;
		const finite = values.filter((v): v is number => v !== null && Number.isFinite(v));
		if (n < 2 || finite.length === 0 || width <= 0) return null;
		const hi = Math.max(...finite);
		const lo = min0 ? 0 : Math.min(...finite);
		const span = hi - lo || 1;
		const pad = 4;
		const xs = values.map((_, i) => pad + (i / (n - 1)) * (width - 2 * pad));
		const ys = values.map((v) => (v === null || !Number.isFinite(v) ? null : pad + (1 - (v - lo) / span) * (height - 2 * pad)));
		let last = -1;
		for (let i = n - 1; i >= 0; i--) if (ys[i] !== null) { last = i; break; }
		return {
			line: linePath(xs, ys),
			area: areaPath(xs, ys, height - pad + 2),
			end: last >= 0 ? { x: xs[last]!, y: ys[last]! } : null,
		};
	});
</script>

<div class="spark" bind:clientWidth={width} style:height="{height}px" aria-hidden="true">
	{#if geom}
		<svg {width} {height} focusable="false">
			<path d={geom.area} fill={color} fill-opacity="0.1" />
			<path d={geom.line} fill="none" stroke={color} stroke-width="2" stroke-linejoin="round" stroke-linecap="round" />
			{#if geom.end}<circle cx={geom.end.x} cy={geom.end.y} r="3.5" fill={color} stroke="var(--c-card)" stroke-width="2" />{/if}
		</svg>
	{/if}
</div>

<style>
	.spark {
		width: 100%;
		min-width: 0;
	}
	svg {
		display: block;
		overflow: visible;
	}
</style>
