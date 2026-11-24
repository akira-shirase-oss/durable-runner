<script lang="ts" module>
	export interface ChartSeries {
		id: string;
		label: string;
		/** a CSS colour, always a token: var(--c-chart-1) */
		color: string;
		values: (number | null)[];
		kind: 'area' | 'line' | 'bar';
	}
	export interface ChartBin {
		t: number;
		end: number;
		/** the bucket in progress: drawn lighter (bars) or dashed (lines), "in progress" in the tooltip */
		partial?: boolean;
	}
</script>

<script lang="ts">
	// A time-series chart drawn in SVG: areas, lines or (stacked) columns over the window's display bins, with a
	// y-axis on clean steps, local clock ticks, hairline gridlines and one tooltip that lists every series at the
	// bin under the pointer (pointerover on per-bin hit areas: no pointermove) or under keyboard focus (arrow keys
	// move between bins). A visually hidden summary and the card's table view carry the same numbers.
	import { fmtClock, fmtCompact, fmtInt } from '../format.ts';
	import { areaPath, columnPath, linePath, niceScale, stackedMax, stackSegments, timeTicks } from '../series.ts';

	interface Extra {
		label: string;
		value: string;
	}

	let {
		bins,
		series,
		label,
		summary,
		stacked = false,
		format = fmtInt,
		tick = fmtCompact,
		yFloor = 1,
		height = 196,
		loading = false,
		empty = 'Nothing in this window',
		extra,
		since = null,
	}: {
		bins: ChartBin[];
		series: ChartSeries[];
		/** the chart's accessible name */
		label: string;
		/** a sentence for screen readers: totals, peak, latest */
		summary: string;
		stacked?: boolean;
		format?: (v: number) => string;
		tick?: (v: number) => string;
		yFloor?: number;
		height?: number;
		loading?: boolean;
		empty?: string;
		/** extra tooltip rows for a bin (values that are not plotted) */
		extra?: (i: number) => Extra[];
		/** when the server started counting: buckets before it are empty by construction, not idle (D-058) */
		since?: number | null;
	} = $props();

	let width = $state(0);
	let active = $state<number | null>(null);
	let keyboard = $state(false);
	let tipWidth = $state(0);

	const ML = 46;
	const MR = 10;
	const MT = 10;
	const MB = 24;

	const n = $derived(bins.length);
	const pw = $derived(Math.max(0, width - ML - MR));
	const ph = $derived(Math.max(0, height - MT - MB));
	const bw = $derived(n > 0 ? pw / n : 0);
	const bars = $derived(series.filter((s) => s.kind === 'bar'));
	const others = $derived(series.filter((s) => s.kind !== 'bar'));

	const hasData = $derived(series.some((s) => s.values.some((v) => v !== null && v > 0)));
	const dataMax = $derived.by(() => {
		let m = stacked ? stackedMax(bars.map((s) => s.values)) : Math.max(0, ...bars.flatMap((s) => s.values.map((v) => v ?? 0)));
		for (const s of others) for (const v of s.values) if (v !== null && v > m) m = v;
		return m;
	});
	const scale = $derived(niceScale(dataMax, ph < 120 ? 3 : 4, yFloor));
	const y = (v: number) => MT + ph - (v / scale.max) * ph;
	const cx = (i: number) => ML + (i + 0.5) * bw;

	const xTicks = $derived.by(() => {
		if (n === 0 || pw <= 0) return [] as { x: number; text: string }[];
		const t0 = bins[0]!.t;
		const t1 = bins[n - 1]!.end;
		const ticks = timeTicks(t0, t1, Math.max(2, Math.floor(pw / 84)));
		const step = ticks.length > 1 ? ticks[1]! - ticks[0]! : 60_000;
		return ticks.map((t) => ({ x: ML + ((t - t0) / (t1 - t0)) * pw, text: fmtClock(t, step < 60_000) }));
	});

	const sinceX = $derived.by(() => {
		if (since === null || since === undefined || n === 0) return null;
		const t0 = bins[0]!.t;
		const t1 = bins[n - 1]!.end;
		if (!(since > t0) || since >= t1) return null;
		return ML + ((since - t0) / (t1 - t0)) * pw;
	});

	const barW = $derived(Math.max(1, Math.min(24, bw - 2)));

	// one path per series for the finished bins, one more per series for the bin in progress (lighter)
	const barPaths = $derived.by(() => {
		if (bars.length === 0 || n === 0) return [] as { id: string; color: string; d: string; partial: boolean }[];
		const done = bars.map((s) => ({ id: s.id, color: s.color, d: '', partial: false }));
		const now = bars.map((s) => ({ id: `${s.id}-now`, color: s.color, d: '', partial: true }));
		for (let i = 0; i < n; i++) {
			const out = bins[i]!.partial ? now : done;
			const x = ML + i * bw + (bw - barW) / 2;
			if (stacked) {
				const segs = stackSegments(bars.map((s) => s.values[i] ?? null), y);
				let topIdx = -1;
				segs.forEach((g, k) => {
					if (g) topIdx = k;
				});
				segs.forEach((g, k) => {
					if (g) out[k]!.d += columnPath(x, barW, g.top, g.bottom, k === topIdx ? 4 : 0);
				});
			} else {
				// side by side within the bin
				const w = Math.max(1, (barW - 2 * (bars.length - 1)) / bars.length);
				bars.forEach((s, k) => {
					const v = s.values[i];
					if (v !== null && v !== undefined && v > 0) {
						const top = Math.min(y(v), MT + ph - 1);
						out[k]!.d += columnPath(x + k * (w + 2), w, top, MT + ph);
					}
				});
			}
		}
		return [...done, ...now];
	});

	const xs = $derived(Array.from({ length: n }, (_, i) => cx(i)));
	// lines stop at the last finished bin; the stretch into the bin in progress is dashed
	const linePaths = $derived(
		others.map((s) => {
			const ys = s.values.map((v) => (v === null || v === undefined ? null : y(v)));
			const finished = ys.map((v, i) => (bins[i]?.partial ? null : v));
			let tail = '';
			for (let i = 1; i < n; i++) {
				const a = ys[i - 1];
				const b = ys[i];
				if (bins[i]!.partial && !bins[i - 1]!.partial && a !== null && a !== undefined && b !== null && b !== undefined)
					tail += `M${xs[i - 1]} ${a}L${xs[i]} ${b}`;
			}
			return {
				id: s.id,
				color: s.color,
				kind: s.kind,
				line: linePath(xs, finished),
				tail,
				area: s.kind === 'area' ? areaPath(xs, ys, MT + ph) : '',
			};
		}),
	);

	const readout = $derived.by(() => {
		if (active === null || active >= n) return null;
		const b = bins[active]!;
		const rows = series.map((s) => {
			const v = s.values[active!];
			return { id: s.id, label: s.label, color: s.color, kind: s.kind, value: v === null || v === undefined ? '—' : format(v) };
		});
		const before = since !== null && since !== undefined && b.end <= since;
		const note = before ? ' · before the server started' : b.partial ? ' · in progress' : '';
		return { when: `${fmtClock(b.t)} – ${fmtClock(b.end)}${note}`, rows, extra: extra ? extra(active) : [] };
	});
	const readoutText = $derived(
		readout ? `${readout.when}: ${[...readout.rows.map((r) => `${r.label} ${r.value}`), ...readout.extra.map((e) => `${e.label} ${e.value}`)].join(', ')}` : '',
	);
	const tipLeft = $derived.by(() => {
		if (active === null) return 0;
		const want = cx(active) - tipWidth / 2;
		return Math.max(0, Math.min(width - tipWidth, want));
	});

	function lastWithData(): number {
		for (let i = n - 1; i >= 0; i--) if (series.some((s) => (s.values[i] ?? 0) > 0)) return i;
		return n - 1;
	}

	function onOver(e: PointerEvent) {
		const i = Number((e.target as Element).getAttribute('data-i'));
		if (Number.isInteger(i)) {
			keyboard = false;
			active = i;
		}
	}

	function onKey(e: KeyboardEvent) {
		if (n === 0) return;
		const cur = active ?? lastWithData();
		let next = cur;
		if (e.key === 'ArrowLeft') next = Math.max(0, cur - 1);
		else if (e.key === 'ArrowRight') next = Math.min(n - 1, cur + 1);
		else if (e.key === 'Home') next = 0;
		else if (e.key === 'End') next = n - 1;
		else if (e.key === 'Escape') {
			active = null;
			return;
		} else return;
		e.preventDefault();
		keyboard = true;
		active = next;
	}
</script>

<div class="chart" bind:clientWidth={width} style:height="{height}px" class:dim={loading && !hasData}>
	<!-- a focusable chart: arrow keys move the readout (announced in the live region below) -->
	<!-- svelte-ignore a11y_no_noninteractive_tabindex, a11y_no_noninteractive_element_interactions -->
	<div
		class="plot"
		role="group"
		tabindex={n > 0 ? 0 : -1}
		aria-label="{label}. Use the left and right arrow keys to read each interval."
		onfocus={() => {
			if (active === null && n > 0) {
				keyboard = true;
				active = lastWithData();
			}
		}}
		onblur={() => {
			if (keyboard) active = null;
		}}
		onkeydown={onKey}
		onpointerleave={() => {
			if (!keyboard) active = null;
		}}
	>
		{#if width > 0}
			<svg {width} {height} aria-hidden="true" focusable="false">
				{#each scale.ticks as t (t)}
					<line class={t === 0 ? 'base' : 'grid'} x1={ML} x2={ML + pw} y1={y(t)} y2={y(t)} />
					<text class="ylab" x={ML - 8} y={y(t)} dy="0.32em" text-anchor="end">{tick(t)}</text>
				{/each}
				{#each xTicks as t (t.x)}
					<text class="xlab" x={t.x} y={height - 6} text-anchor="middle">{t.text}</text>
				{/each}

				{#if sinceX !== null}
					<rect class="before" x={ML} y={MT} width={Math.max(0, sinceX - ML)} height={ph} />
					<line class="start" x1={sinceX} x2={sinceX} y1={MT} y2={MT + ph} />
					<!-- the label sits at the top of the region when the chart's "nothing here" note takes the middle -->
					{#if sinceX - ML > 80}
						<text class="start-lab" x={(ML + sinceX) / 2} y={hasData ? MT + ph / 2 : MT + 16} dy="0.32em" text-anchor="middle">before start</text>
					{/if}
				{/if}

				{#if active !== null && active < n}
					{#if bars.length}
						<rect class="band" x={ML + active * bw} y={MT} width={bw} height={ph} />
					{:else}
						<line class="cross" x1={cx(active)} x2={cx(active)} y1={MT} y2={MT + ph} />
					{/if}
				{/if}

				{#each barPaths as p (p.id)}
					<path d={p.d} fill={p.color} fill-opacity={p.partial ? 0.4 : 1} />
				{/each}
				{#each linePaths as p (p.id)}
					{#if p.area}<path d={p.area} fill={p.color} fill-opacity="0.1" />{/if}
					<path d={p.line} fill="none" stroke={p.color} stroke-width="2" stroke-linejoin="round" stroke-linecap="round" />
					{#if p.tail}<path d={p.tail} fill="none" stroke={p.color} stroke-width="2" stroke-dasharray="2 4" stroke-linecap="round" />{/if}
				{/each}

				{#if active !== null && active < n}
					{#each others as s (s.id)}
						{@const v = s.values[active]}
						{#if v !== null && v !== undefined}
							<circle class="mark" cx={cx(active)} cy={y(v)} r="4" fill={s.color} />
						{/if}
					{/each}
				{/if}

				<!-- per-bin hit areas: pointerover only, never pointermove -->
				<!-- svelte-ignore a11y_no_static_element_interactions -->
					<g class="hits" onpointerover={onOver}>
					{#each xs as _, i (i)}
						<rect data-i={i} x={ML + i * bw} y={MT} width={Math.max(0, bw)} height={ph + MB - 4} />
					{/each}
				</g>
			</svg>
		{/if}

		{#if loading && !hasData}
			<p class="overlay">Loading…</p>
		{:else if !hasData && n > 0}
			<p class="overlay">{empty}</p>
		{/if}

		{#if readout}
			<div class="tip" bind:clientWidth={tipWidth} style:left="{tipLeft}px" aria-hidden="true">
				<p class="when">{readout.when}</p>
				{#each readout.rows as r (r.id)}
					<p class="row"><i class="key {r.kind}" style:--k={r.color}></i><strong>{r.value}</strong><span>{r.label}</span></p>
				{/each}
				{#each readout.extra as e (e.label)}
					<p class="row extra"><i class="key none"></i><strong>{e.value}</strong><span>{e.label}</span></p>
				{/each}
			</div>
		{/if}
	</div>
	<p class="sr">{summary}</p>
	<p class="sr" aria-live="polite">{keyboard ? readoutText : ''}</p>
</div>

<style>
	.chart {
		position: relative;
		width: 100%;
		min-width: 0;
		transition: opacity 150ms ease-out;
	}
	.chart.dim {
		opacity: 0.7;
	}
	.plot {
		position: relative;
		width: 100%;
		height: 100%;
		border-radius: 10px;
	}
	.plot:focus-visible {
		outline-offset: 4px;
	}
	svg {
		display: block;
		overflow: visible;
	}
	.grid {
		stroke: var(--c-grid);
		stroke-width: 1;
		shape-rendering: crispEdges;
	}
	.base {
		stroke: var(--c-axis);
		stroke-width: 1;
		shape-rendering: crispEdges;
	}
	.ylab,
	.xlab {
		fill: var(--c-body);
		font-size: 11.5px;
		font-variant-numeric: tabular-nums;
	}
	.band {
		fill: var(--c-hover-band);
	}
	.before {
		fill: var(--c-hover-band);
	}
	.start {
		stroke: var(--c-crosshair);
		stroke-width: 1;
		shape-rendering: crispEdges;
	}
	.start-lab {
		fill: var(--c-body);
		font-size: 11px;
	}
	.cross {
		stroke: var(--c-crosshair);
		stroke-width: 1;
		shape-rendering: crispEdges;
	}
	.mark {
		stroke: var(--c-card);
		stroke-width: 2;
	}
	.hits rect {
		fill: transparent;
	}
	.overlay {
		position: absolute;
		inset: 0 0 24px 46px;
		display: grid;
		place-items: center;
		margin: 0;
		color: var(--c-body);
		font-size: 13.5px;
		pointer-events: none;
	}
	.tip {
		position: absolute;
		top: 2px;
		z-index: 2;
		min-width: 150px;
		max-width: 240px;
		padding: 8px 10px 9px;
		border-radius: 12px;
		background: var(--c-card);
		box-shadow:
			0 0 0 1px var(--c-bar-edge),
			0 8px 24px -10px var(--c-shadow);
		pointer-events: none;
	}
	.tip p {
		margin: 0;
	}
	.when {
		margin-bottom: 5px !important;
		color: var(--c-body);
		font-size: 12px;
		font-variant-numeric: tabular-nums;
	}
	.row {
		display: grid;
		grid-template-columns: 14px auto 1fr;
		gap: 7px;
		align-items: center;
		font-size: 12.5px;
		line-height: 1.6;
		white-space: nowrap;
	}
	.row strong {
		font-weight: 600;
		font-variant-numeric: tabular-nums;
	}
	.row span {
		color: var(--c-body);
	}
	.key {
		display: block;
		width: 14px;
		height: 3px;
		border-radius: 2px;
		background: var(--k);
	}
	.key.bar {
		height: 8px;
		width: 8px;
		margin-left: 3px;
	}
	.key.none {
		background: transparent;
	}
	.extra {
		color: var(--c-body);
	}
</style>
