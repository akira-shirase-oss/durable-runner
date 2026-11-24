<script lang="ts">
	// The run's step attempts over time: one row per step, one bar per attempt from dispatch to answer, coloured by
	// outcome; after a lost reply a thin probe line runs to the reconcile answer, ending in a dot (effect found, or
	// not). Open attempts reach "now". Pointerover on a bar or arrow keys on the focused chart show its details; the
	// vertical timeline beside it is the full text equivalent.
	import { fmtClockMs, fmtMs, fmtMsTick } from '../format.ts';
	import { barLayout, ganttDomain, offsetTicks, type Attempt } from '../timeline.ts';

	let {
		attempts,
		now,
		steps,
		stepName = () => null,
		empty = 'No step has been dispatched yet',
	}: { attempts: Attempt[]; now: number; steps: number; stepName?: (i: number) => string | null; empty?: string } = $props();

	let width = $state(0);
	let active = $state<number | null>(null);
	let keyboard = $state(false);
	let tipWidth = $state(0);

	const LABEL = $derived(width < 520 ? 40 : 132);
	const MR = 14;
	const ROW = 30;
	const MT = 6;
	const MB = 26;

	const rows = $derived(Math.max(steps, ...attempts.map((a) => a.step + 1), 0));
	const height = $derived(MT + rows * ROW + MB);
	const pw = $derived(Math.max(0, width - LABEL - MR));
	const domain = $derived(ganttDomain(attempts, now));
	const span = $derived(domain ? domain[1] - domain[0] : 0);
	const ticks = $derived(domain ? offsetTicks(span, Math.max(2, Math.floor(pw / 80))) : []);
	const laid = $derived(domain ? attempts.map((a) => barLayout(a, domain, pw, now)) : []);

	const OUTCOME: Record<string, { color: string; label: string }> = {
		ok: { color: 'var(--c-chart-ok)', label: 'checkpointed' },
		failed: { color: 'var(--c-chart-fail)', label: 'failed' },
		lost: { color: 'var(--c-chart-lost)', label: 'reply lost' },
		open: { color: 'var(--c-chart-1)', label: 'in flight' },
		unknown: { color: 'var(--c-chart-neutral)', label: 'no answer recorded' },
	};

	function lines(a: Attempt): { head: string; rows: string[] } {
		const name = stepName(a.step);
		const rows: string[] = [];
		rows.push(a.end === null ? `In flight for ${fmtMs(now - a.start)}` : `${cap(OUTCOME[a.outcome]!.label)} after ${fmtMs(a.end - a.start)}`);
		if (a.reconciledAt !== null && a.end !== null) rows.push(`Reconciled in ${fmtMs(a.reconciledAt - a.end)}: ${a.effectFound ? 'effect found' : 'no effect found'}`);
		else if (a.outcome === 'lost') rows.push('Reconcile pending');
		rows.push(`Dispatched ${fmtClockMs(a.start)}`);
		return { head: `Step ${a.step}${name ? ` (${name})` : ''} · attempt ${a.attempt}`, rows };
	}

	function cap(s: string): string {
		return s.charAt(0).toUpperCase() + s.slice(1);
	}

	function describe(a: Attempt): string {
		const name = stepName(a.step);
		const head = `Step ${a.step}${name ? ` (${name})` : ''}, attempt ${a.attempt}`;
		const dur = a.end === null ? `in flight for ${fmtMs(now - a.start)}` : `${OUTCOME[a.outcome]!.label} after ${fmtMs(a.end - a.start)}`;
		const probe =
			a.reconciledAt !== null && a.end !== null
				? `; reconciled in ${fmtMs(a.reconciledAt - a.end)}: ${a.effectFound ? 'effect found' : 'no effect found'}`
				: a.outcome === 'lost'
					? '; reconcile pending'
					: '';
		return `${head}: ${dur}${probe}. Dispatched ${fmtClockMs(a.start)}.`;
	}

	const tipLeft = $derived.by(() => {
		if (active === null || !laid[active]) return 0;
		const b = laid[active]!;
		const want = LABEL + b.x + b.w / 2 - tipWidth / 2;
		return Math.max(0, Math.min(width - tipWidth, want));
	});
	const tipTop = $derived(active !== null && attempts[active] ? MT + (attempts[active]!.step + 1) * ROW + 2 : 0);

	function onOver(e: PointerEvent) {
		const i = Number((e.target as Element).getAttribute('data-i'));
		if (Number.isInteger(i)) {
			keyboard = false;
			active = i;
		}
	}

	function onKey(e: KeyboardEvent) {
		const n = attempts.length;
		if (n === 0) return;
		const cur = active ?? n - 1;
		let next = cur;
		if (e.key === 'ArrowLeft' || e.key === 'ArrowUp') next = Math.max(0, cur - 1);
		else if (e.key === 'ArrowRight' || e.key === 'ArrowDown') next = Math.min(n - 1, cur + 1);
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

<div class="gantt" bind:clientWidth={width}>
	{#if attempts.length === 0 || !domain}
		<p class="empty">{empty}</p>
	{:else}
		<!-- a focusable chart: arrow keys move the readout (announced in the live region below) -->
		<!-- svelte-ignore a11y_no_noninteractive_tabindex, a11y_no_noninteractive_element_interactions -->
		<div
			class="plot"
			role="group"
			tabindex="0"
			aria-label="Step attempts over time, {attempts.length} attempt{attempts.length === 1 ? '' : 's'}. Use the arrow keys to read each attempt."
			onfocus={() => {
				if (active === null) {
					keyboard = true;
					active = attempts.length - 1;
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
					{#each Array.from({ length: rows }, (_, i) => i) as r (r)}
						<rect class="lane" class:odd={r % 2 === 1} x={LABEL} y={MT + r * ROW} width={pw} height={ROW} />
						<text class="step" x={LABEL - 10} y={MT + r * ROW + ROW / 2} dy="0.32em" text-anchor="end">
							<tspan class="n">{r}</tspan>{#if stepName(r) && LABEL > 60}<tspan dx="6">{(stepName(r) ?? '').slice(0, 14)}</tspan>{/if}
						</text>
					{/each}
					{#each ticks as t (t)}
						{@const x = LABEL + (span > 0 ? (t / span) * pw : 0)}
						<line class="grid" x1={x} x2={x} y1={MT} y2={MT + rows * ROW} />
						<text class="xlab" {x} y={height - 7} text-anchor={t === 0 ? 'start' : 'middle'}>{t === 0 ? '0' : `+${fmtMsTick(t)}`}</text>
					{/each}
					<line class="base" x1={LABEL} x2={LABEL + pw} y1={MT + rows * ROW} y2={MT + rows * ROW} />

					{#each attempts as a, i (i)}
						{@const b = laid[i]!}
						{@const yy = MT + a.step * ROW + 7}
						<g class="bar" class:on={active === i}>
							<rect x={LABEL + b.x} y={yy} width={b.w} height={ROW - 14} rx="4" fill={OUTCOME[a.outcome]!.color} />
							{#if b.probeX !== null}
								<line class="probe" x1={LABEL + b.probeX} x2={LABEL + b.probeX + b.probeW} y1={yy + (ROW - 14) / 2} y2={yy + (ROW - 14) / 2} />
								<circle
									cx={LABEL + b.probeX + b.probeW}
									cy={yy + (ROW - 14) / 2}
									r="4.5"
									fill={a.effectFound ? 'var(--c-chart-ok)' : 'var(--c-chart-retry)'}
									stroke="var(--c-card)"
									stroke-width="2"
								/>
							{/if}
						</g>
					{/each}
					<!-- svelte-ignore a11y_no_static_element_interactions -->
					<g class="hits" onpointerover={onOver}>
						{#each attempts as a, i (i)}
							{@const b = laid[i]!}
							<rect data-i={i} x={LABEL + b.x - 3} y={MT + a.step * ROW} width={b.w + b.probeW + 10} height={ROW} />
						{/each}
					</g>
				</svg>
			{/if}
			{#if active !== null && attempts[active]}
				{@const tip = lines(attempts[active]!)}
				<div class="tip" bind:clientWidth={tipWidth} style:left="{tipLeft}px" style:top="{tipTop}px" aria-hidden="true">
					<p class="tip-head">{tip.head}</p>
					{#each tip.rows as r (r)}<p>{r}</p>{/each}
				</div>
			{/if}
		</div>
		<p class="sr" aria-live="polite">{keyboard && active !== null && attempts[active] ? describe(attempts[active]!) : ''}</p>
		<ul class="legend" aria-label="Legend">
			<li><i style:--k="var(--c-chart-ok)"></i>Checkpointed</li>
			<li><i style:--k="var(--c-chart-fail)"></i>Failed (retried)</li>
			<li><i style:--k="var(--c-chart-lost)"></i>Reply lost</li>
			<li><i class="dot" style:--k="var(--c-chart-ok)"></i>Reconciled: effect found</li>
			<li><i class="dot" style:--k="var(--c-chart-retry)"></i>Reconciled: none</li>
			<li><i style:--k="var(--c-chart-1)"></i>In flight</li>
			<li><i style:--k="var(--c-chart-neutral)"></i>No answer recorded</li>
		</ul>
	{/if}
</div>

<style>
	.gantt {
		position: relative;
		min-width: 0;
	}
	.plot {
		position: relative;
		border-radius: 10px;
	}
	.plot:focus-visible {
		outline-offset: 4px;
	}
	svg {
		display: block;
	}
	.lane {
		fill: transparent;
	}
	.lane.odd {
		fill: var(--c-panel);
	}
	.step {
		fill: var(--c-ink-soft);
		font-family: var(--font-mono);
		font-size: 12px;
	}
	.step .n {
		fill: var(--c-body);
	}
	.grid {
		stroke: var(--c-grid);
		shape-rendering: crispEdges;
	}
	.base {
		stroke: var(--c-axis);
		shape-rendering: crispEdges;
	}
	.xlab {
		fill: var(--c-body);
		font-size: 11.5px;
		font-variant-numeric: tabular-nums;
	}
	.bar rect {
		transition: opacity 150ms ease-out;
	}
	.bar.on rect {
		stroke: var(--c-ink);
		stroke-width: 1.5;
	}
	.probe {
		stroke: var(--c-chart-lost);
		stroke-width: 2;
		stroke-dasharray: 3 3;
	}
	.hits rect {
		fill: transparent;
	}
	.tip {
		position: absolute;
		z-index: 2;
		max-width: 300px;
		padding: 8px 10px;
		border-radius: 12px;
		background: var(--c-card);
		box-shadow:
			0 0 0 1px var(--c-bar-edge),
			0 8px 24px -10px var(--c-shadow);
		color: var(--c-ink);
		font-size: 12.5px;
		line-height: 1.5;
		pointer-events: none;
	}
	.tip p {
		margin: 0;
		color: var(--c-body);
	}
	.tip .tip-head {
		margin-bottom: 2px;
		color: var(--c-ink);
		font-weight: 600;
	}
	.legend {
		display: flex;
		flex-wrap: wrap;
		gap: 4px 14px;
		margin: 10px 0 0;
		padding: 0 0 0 4px;
		list-style: none;
		color: var(--c-ink-soft);
		font-size: 12.5px;
	}
	.legend li {
		display: inline-flex;
		align-items: center;
		gap: 6px;
	}
	.legend i {
		width: 12px;
		height: 8px;
		border-radius: 3px;
		background: var(--k);
	}
	.legend i.dot {
		width: 9px;
		height: 9px;
		border-radius: 50%;
	}
	.empty {
		margin: 0;
		padding: 28px 12px;
		color: var(--c-body);
		font-size: 14px;
		text-align: center;
	}
</style>
