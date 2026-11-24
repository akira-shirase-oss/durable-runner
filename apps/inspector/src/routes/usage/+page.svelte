<script lang="ts">
	// Usage (signed in): the workspace's plan and every meter against its limit, from GET …/usage — the same plan
	// row the server enforces, so what the page says and what admission does cannot drift (D-063). The Free numbers
	// are labelled proposals: measurement may change them, through the plan table, never a code change.
	import Icon from '../../lib/Icon.svelte';
	import Notice from '../../lib/ui/Notice.svelte';
	import Skeleton from '../../lib/ui/Skeleton.svelte';
	import { account } from '../../lib/account.svelte.ts';
	import { usage } from '../../lib/api.ts';
	import { fmtBytes, fmtInt } from '../../lib/format.ts';
	import { live, poll, Resource } from '../../lib/live.svelte.ts';
	import { allowances, meters, toMs } from '../../lib/saas.ts';

	const pid = account.pid ?? '';
	const res = new Resource((signal) => usage(account.cred, pid, signal), { interval: 10_000, live: false });
	poll(res);
	const list = $derived(res.data ? meters(res.data) : []);
	const allow = $derived(res.data ? allowances(res.data) : []);
	const plan = $derived.by(() => {
		const p = (res.data as { plan?: unknown } | null)?.plan;
		return typeof p === 'string' && p ? p : (account.current?.plan ?? 'free');
	});
	const windows = $derived((res.data as { windows?: Record<string, unknown> } | null)?.windows ?? {});
	const minuteEnds = $derived(toMs(windows.minute_ends_at_ms ?? windows.minute_ends_at));
	const hourEnds = $derived(toMs(windows.hour_ends_at_ms ?? windows.hour_ends_at));
	const resetIn = (t: number | null) => (t === null ? null : Math.max(0, Math.ceil((t - live.now) / 1000)));
	function reset(id: string): string {
		if (id === 'jobs_per_min') {
			const s = resetIn(minuteEnds);
			return s === null ? '' : `Resets in ${s} s.`;
		}
		if (id === 'test_calls_per_hour') {
			const s = resetIn(hourEnds);
			return s === null ? '' : `Resets in ${Math.ceil(s / 60)} min.`;
		}
		return '';
	}
	function value(v: number | null, unit: 'bytes' | 'days' | 'count'): string {
		if (v === null) return '—';
		if (unit === 'bytes') return fmtBytes(v);
		if (unit === 'days') return `${fmtInt(v)} day${v === 1 ? '' : 's'}`;
		return fmtInt(v);
	}
</script>

<div class="page narrow">
	<section class="card plan" aria-labelledby="plan-title">
		<div class="card-head">
			<div>
				<h2 id="plan-title"><Icon name="gauge" size={17} />{plan.charAt(0).toUpperCase() + plan.slice(1)} plan</h2>
				<p class="sub">Free while Durable Runner is in beta. These limits are proposals we are measuring; when one changes, it changes here first.</p>
			</div>
			<span class="badge ok">Current plan</span>
		</div>
		{#if allow.length}
			<div class="card-body">
				<dl class="allow">
					{#each allow as a (a.id)}
						<div><dt>{a.label}</dt><dd>{value(a.value, a.unit)}</dd></div>
					{/each}
				</dl>
			</div>
		{/if}
	</section>

	<section class="card" aria-labelledby="meters-title">
		<div class="card-head ruled">
			<div>
				<h2 id="meters-title">This workspace now</h2>
				<p class="sub">Live counts against each limit. At a limit the server refuses with <span class="mono">plan_limit</span> and says which; rate limits clear on their own.</p>
			</div>
		</div>
		{#if res.status === 'unsupported' || res.status === 'needs-key' || (res.status === 'error' && !res.data)}
			<div class="card-body"><Notice status={res.status} error={res.error} what="Usage" route="GET /workspaces/{'{pid}'}/usage" retry={() => res.refresh(true)} /></div>
		{:else if res.loading}
			<Skeleton label="Loading usage" rows={4} />
		{:else}
			<ul class="meters">
				{#each list as m (m.id)}
					<li>
						<div class="m-head">
							<span class="m-label">{m.label}</span>
							<span class="m-num" class:full={m.level === 'full'}>{m.used === null ? '—' : fmtInt(m.used)}<span class="muted">{` / ${m.cap === null ? '—' : fmtInt(m.cap)}`}</span></span>
						</div>
						<div class="meter" role="img" aria-label="{m.label}: {m.used === null ? 'unknown' : fmtInt(m.used)} of {m.cap === null ? 'unknown' : fmtInt(m.cap)}{m.level === 'full' ? ', at the limit' : ''}">
							<i style:width="{m.pct ?? 0}%" data-level={m.level}></i>
						</div>
						<p class="m-note">{m.level === 'full' ? 'At the limit. ' : ''}{m.note} {reset(m.id)}</p>
					</li>
				{/each}
			</ul>
		{/if}
	</section>
</div>

<style>
	.narrow {
		max-width: 960px;
	}
	.allow {
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
		gap: 14px;
		margin: 0;
	}
	.allow dt {
		color: var(--c-body);
		font-size: 13px;
	}
	.allow dd {
		margin: 3px 0 0;
		font-size: 18px;
		font-weight: 560;
		letter-spacing: -0.01em;
	}
	.meters {
		display: grid;
		grid-template-columns: repeat(2, minmax(0, 1fr));
		gap: 0;
		margin: 0;
		padding: 0;
		list-style: none;
	}
	.meters li {
		display: flex;
		flex-direction: column;
		gap: 8px;
		padding: 18px 24px;
		border-top: 1px solid var(--c-line);
	}
	.meters li:nth-child(odd) {
		border-right: 1px solid var(--c-line);
	}
	.meters li:nth-child(-n + 2) {
		border-top: 0;
	}
	.m-head {
		display: flex;
		align-items: baseline;
		justify-content: space-between;
		gap: 10px;
	}
	.m-label {
		font-size: 14.5px;
		font-weight: 560;
	}
	.m-num {
		font-size: 15px;
		font-variant-numeric: tabular-nums;
		font-weight: 560;
		white-space: nowrap;
	}
	.m-num.full {
		color: var(--c-danger);
	}
	.m-note {
		margin: 0;
		color: var(--c-body);
		font-size: 13px;
		line-height: 1.45;
	}
	@media (max-width: 760px) {
		.meters {
			grid-template-columns: minmax(0, 1fr);
		}
		.meters li:nth-child(odd) {
			border-right: 0;
		}
		.meters li:nth-child(2) {
			border-top: 1px solid var(--c-line);
		}
		.meters li {
			padding: 16px;
		}
	}
</style>
