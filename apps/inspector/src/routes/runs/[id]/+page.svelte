<script lang="ts">
	// One run: what it is (key, state, steps, times), its step progress, a Gantt of its attempts over time and the
	// vertical timeline of every event the server kept (GET /jobs/{id}/events), with an honest banner when the
	// timeline is incomplete. The spec is a deliberate reveal with the API key (D-051); Cancel as before.
	// Polls every 5 s until the run is terminal.
	import { page } from '$app/state';
	import Icon, { type IconName } from '../../../lib/Icon.svelte';
	import Gantt from '../../../lib/charts/Gantt.svelte';
	import Notice from '../../../lib/ui/Notice.svelte';
	import StateChip from '../../../lib/ui/StateChip.svelte';
	import { cancelRun, inspect, runEvents, spec as fetchSpec, type RunItem, type RunTimeline, type Spec } from '../../../lib/api.ts';
	import { fmtAgo, fmtClockMs, fmtDateTime, fmtMs, fmtOffset } from '../../../lib/format.ts';
	import { live, poll, Resource } from '../../../lib/live.svelte.ts';
	import { POLL_MS } from '../../../lib/poll.ts';
	import { parseRunId } from '../../../lib/routes.ts';
	import { session } from '../../../lib/session.svelte.ts';
	import { cancellable, terminal } from '../../../lib/state.ts';
	import { buildAttempts, gapIndex, stepStates, timelineRows, TRUNCATE_HEAD, type Tone } from '../../../lib/timeline.ts';

	const id = $derived(parseRunId(page.params.id));

	// polling stops once the run (and its timeline) are terminal: neither will change again
	let finished = false;
	const runRes: Resource<RunItem> = new Resource((signal) => inspect(session.readerKey, id ?? -1, signal), {
		interval: (): number => (finished ? 0 : POLL_MS),
	});
	poll(runRes, () => id);
	const tlRes: Resource<RunTimeline> = new Resource((signal) => runEvents(session.readerKey, id ?? -1, signal), {
		interval: (): number => (finished && tlRes.data !== null && terminal(tlRes.data.state) ? 0 : POLL_MS),
	});
	poll(tlRes, () => id);
	$effect(() => {
		finished = runRes.data !== null && runRes.data.id === id && terminal(runRes.data.state);
	});

	let specData = $state<Spec | null>(null);
	let specFor = $state<number | null>(null);
	let specError = $state<string | null>(null);
	let actionError = $state<string | null>(null);
	let notice = $state<string | null>(null);

	$effect(() => {
		// a different run: forget the revealed spec
		if (specFor !== id) {
			specData = null;
			specError = null;
			expanded = false;
		}
	});

	const run = $derived(runRes.data && runRes.data.id === id ? runRes.data : null);
	const tl = $derived(tlRes.data && tlRes.data.run === id ? tlRes.data : null);
	const events = $derived(tl?.events ?? []);
	const stepName = (i: number): string | null => specData?.steps[i]?.name ?? null;
	const gap = $derived(tl ? gapIndex(tl.complete, tl.reason, events.length) : -1);
	const attempts = $derived(buildAttempts(events, gap));
	const hole = $derived<[number, number] | null>(gap > 0 && gap < events.length ? [events[gap - 1]!.at, events[gap]!.at] : null);
	const rows = $derived(timelineRows(events, stepName, gap));
	// a long timeline shows its first and last rows; the middle opens on request
	const FOLD = 36;
	const KEEP = 12;
	let expanded = $state(false);
	const folded = $derived(!expanded && rows.length > FOLD);
	const hiddenFrom = $derived(folded ? KEEP : rows.length);
	const hiddenTo = $derived(folded ? rows.length - KEEP : rows.length);
	const progress = $derived(run ? stepStates(run.state, run.steps, events) : []);
	const accepted = $derived(events.find((e) => e.kind === 'accepted') ?? null);
	const firstDispatch = $derived(events.find((e) => e.kind === 'dispatched') ?? null);
	const queuedFor = $derived(accepted && firstDispatch ? firstDispatch.at - accepted.at : null);
	const notFound = $derived(runRes.error?.status === 404 && runRes.error.code === 'not_found');

	const KIND_ICON: Record<string, IconName> = {
		accepted: 'inbox',
		dispatched: 'send',
		checkpointed: 'bookmark-check',
		step_failed: 'x-circle',
		reply_lost: 'unplug',
		reconciled: 'scan',
		cancel_requested: 'ban',
		finished: 'flag',
	};
	const REASON: Record<string, string> = {
		compacted: 'Earlier events were folded into a snapshot when the journal was compacted; the timeline starts at the first event kept.',
		evicted: 'The bounded timeline store dropped this run to make room for newer ones; what is left is shown, possibly nothing.',
		truncated: `This run has more events than the per-run cap: the first ${TRUNCATE_HEAD} and the last 80 are kept, the ones between are not.`,
	};
	const STEP_LABEL: Record<string, string> = {
		done: 'checkpointed',
		now: 'running',
		retry: 'waiting to retry',
		probe: 'reconciling',
		failed: 'failed',
		pending: 'not started',
		cancelled: 'not run (cancelled)',
	};

	async function reveal() {
		if (id === null) return;
		try {
			specData = (await fetchSpec(session.apiKey, id)).spec;
			specFor = id;
			specError = null;
		} catch (e) {
			specError = e instanceof Error ? e.message : String(e);
		}
	}

	async function cancel() {
		if (id === null) return;
		try {
			await cancelRun(id, session.apiKey);
			notice = `Run ${id} cancelled. A step already sent to its handler cannot be undone.`;
			actionError = null;
			void runRes.refresh(true);
			void tlRes.refresh(true);
		} catch (e) {
			actionError = e instanceof Error ? e.message : String(e);
		}
	}

	function toneOf(t: Tone): string {
		return t;
	}
</script>

<div class="page">
	<a class="back" href="#/runs"><Icon name="arrow-left" size={15} />All runs</a>

	{#if id === null}
		<p class="banner error"><Icon name="alert" size={16} /><span class="msg">“{page.params.id}” is not a run id. A run id is a whole number, like 10418.</span></p>
	{:else if notFound}
		<p class="banner error"><Icon name="alert" size={16} /><span class="msg">There is no run {id} on this server. Finished runs leave after their retention when the journal is compacted.</span></p>
	{:else}
		{#if notice}
			<p class="banner notice"><Icon name="check" size={16} /><span class="msg">{notice}</span><button class="btn ghost sm act" onclick={() => (notice = null)}>Dismiss</button></p>
		{/if}
		{#if actionError}
			<p class="banner error"><Icon name="alert" size={16} /><span class="msg">{actionError}</span><button class="btn ghost sm act" onclick={() => (actionError = null)}>Dismiss</button></p>
		{/if}
		<Notice status={runRes.status} error={runRes.error} what="This run" route="GET /jobs/{id}" stale={runRes.updatedAt} />

		<section class="card head-card" aria-labelledby="run-key">
			<div class="head">
				<div class="who">
					<p class="eyebrow">Operation key</p>
					<h2 id="run-key" class="key">{run?.key ?? (runRes.loading ? 'Loading…' : '—')}</h2>
					{#if run}<p class="state"><StateChip state={run.state} /></p>{/if}
				</div>
				<div class="actions">
					{#if run && cancellable(run.state)}
						<button class="btn danger" disabled={!session.apiKey} title={session.apiKey ? '' : 'Cancelling needs the API key'} onclick={cancel}>
							<Icon name="x" />Cancel run
						</button>
					{/if}
					{#if !specData}
						<button
							class="btn"
							disabled={!session.apiKey || !run}
							title={session.apiKey ? 'Fetch the spec (steps and input) with the API key' : 'The spec may carry customer input (D-051): the API key reveals it'}
							onclick={reveal}
						>
							<Icon name="eye" />Reveal spec{session.apiKey ? '' : ': needs the API key'}
						</button>
					{/if}
				</div>
			</div>
			{#if run}
				<dl class="facts">
					<div><dt>Steps</dt><dd>{run.steps}</dd></div>
					<div>
						<dt>Accepted</dt>
						<dd>{accepted ? fmtDateTime(accepted.at, live.now) : '—'}{#if accepted}<span class="muted">{' · '}{fmtAgo(live.now - accepted.at)}</span>{/if}</dd>
					</div>
					<div>
						<dt>Finished</dt>
						<dd>{run.finished_at ? fmtDateTime(run.finished_at, live.now) : terminal(run.state) ? '—' : 'not yet'}</dd>
					</div>
					<div>
						<dt>{run.finished_at ? 'Took' : 'Running for'}</dt>
						<dd>{accepted ? fmtMs((run.finished_at ?? live.now) - accepted.at) : '—'}</dd>
					</div>
				</dl>
				{#if run.steps > 0}
					<ol class="progress" aria-label="Step progress">
						{#each progress as p, i (i)}
							<li class={p}>
								<span class="seg" aria-hidden="true"></span>
								<span class="lbl"><span class="n">{i}</span>{stepName(i) ?? `step ${i}`}</span>
								<span class="sr">: {STEP_LABEL[p]}</span>
							</li>
						{/each}
					</ol>
				{/if}
			{/if}
		</section>

		{#if tl && !tl.complete}
			<p class="banner warn">
				<Icon name="history" size={16} />
				<span class="msg"><strong>This timeline is incomplete ({tl.reason ?? 'unknown reason'}).</strong> {REASON[tl.reason ?? ''] ?? 'The server kept only part of it.'}</span>
			</p>
		{/if}
		<Notice status={tlRes.status} error={tlRes.error} what="The run's timeline" route="GET /jobs/{id}/events" stale={tlRes.updatedAt} />

		{#if tlRes.status !== 'unsupported' && tlRes.status !== 'needs-key'}
			<section class="card" aria-labelledby="gantt-title">
				<div class="card-head">
					<div>
						<h2 id="gantt-title"><Icon name="gantt" size={17} />Attempts over time</h2>
						<p class="sub">
							From the first dispatch{#if queuedFor !== null && queuedFor > 0}; queued {fmtMs(queuedFor)} before it{/if}. One bar per attempt, dispatch to answer.
						</p>
					</div>
				</div>
				<div class="card-body">
					{#if tl}
						<Gantt
							{attempts}
							now={live.now}
							steps={run?.steps ?? 0}
							{stepName}
							{hole}
							empty={tl.complete ? 'No step has been dispatched yet' : 'The server kept no attempts for this run'}
						/>
					{:else}
						<p class="empty">{tlRes.loading ? 'Loading…' : 'Unavailable'}</p>
					{/if}
				</div>
			</section>
		{/if}

		<div class="grid detail-grid">
			{#if tlRes.status !== 'unsupported' && tlRes.status !== 'needs-key'}
				<section class="card" aria-labelledby="tl-title">
					<div class="card-head">
						<div>
							<h2 id="tl-title"><Icon name="history" size={17} />Timeline</h2>
							<p class="sub">Every event the server kept, in commit order{events.length ? `, ${events.length} in all` : ''}. Inputs and outputs are never shown.</p>
						</div>
					</div>
					<div class="card-body">
						{#if !tl}
							<p class="empty">{tlRes.loading ? 'Loading…' : 'Unavailable'}</p>
						{:else if rows.length === 0}
							<p class="empty">No events kept for this run</p>
						{:else}
							<ol class="tl">
								{#each rows as r, i (i)}
									{#if folded && i === hiddenFrom}
										<li class="gap fold">
											<span class="rail" aria-hidden="true"><span class="node"><Icon name="list" size={13} /></span></span>
											<p class="gap-text"><button class="btn sm" onclick={() => (expanded = true)}>Show {hiddenTo - hiddenFrom} more events</button>{#if gap >= hiddenFrom && gap < hiddenTo}<span class="fold-note">including where the per-run cap dropped events</span>{/if}</p>
										</li>
									{/if}
									{#if folded && i >= hiddenFrom && i < hiddenTo}
										<!-- folded -->
									{:else}
									{#if i === gap}
										<li class="gap"><span class="rail" aria-hidden="true"><span class="node"><Icon name="history" size={13} /></span></span><p class="gap-text">{tl.reason === 'truncated' ? 'Events between were dropped by the per-run cap' : 'Earlier events are not kept'}</p></li>
									{/if}
									<li class="tone-{toneOf(r.tone)}">
										<span class="rail" aria-hidden="true"><span class="node"><Icon name={KIND_ICON[r.event.kind] ?? 'circle-dot'} size={14} /></span></span>
										<div class="what">
											<p class="title">{r.title}</p>
											{#if r.notes.length}<p class="notes">{r.notes.join(' · ')}</p>{/if}
										</div>
										<p class="when">
											<span class="off">{fmtOffset(r.offset)}</span>
											<time datetime={new Date(r.event.at).toISOString()}>{fmtClockMs(r.event.at)}</time>
										</p>
									</li>
									{/if}
								{/each}
								{#if gap === rows.length}
									<li class="gap"><span class="rail" aria-hidden="true"><span class="node"><Icon name="history" size={13} /></span></span><p class="gap-text">Later events were dropped by the per-run cap</p></li>
								{/if}
							</ol>
						{/if}
					</div>
				</section>
			{/if}

			<section class="card spec-card" aria-labelledby="spec-title">
				<div class="card-head">
					<div>
						<h2 id="spec-title"><Icon name="eye" size={17} />Spec</h2>
						<p class="sub">Steps, handlers and input, fetched only when you ask</p>
					</div>
					{#if specData}<span class="tag">v{specData.version}</span>{/if}
				</div>
				<div class="card-body">
					{#if specError}<p class="banner error"><Icon name="alert" size={16} /><span class="msg">{specError}</span></p>{/if}
					{#if specData}
						<ol class="step-list">
							{#each specData.steps as s, i (i)}
								<li><span class="n">{i}</span><span class="mono">{s.name}</span><span class="arrow" aria-hidden="true">→</span><span class="mono handler">{s.handler}</span></li>
							{/each}
						</ol>
						<p class="input-label">Input (base64)</p>
						<p class="input mono">{specData.input_b64 || '—'}</p>
					{:else}
						<p class="hint reveal-hint">The input may be customer content (D-051), so the page asks for the API key instead of fetching it on open.</p>
						<button class="btn" disabled={!session.apiKey || !run} onclick={reveal}><Icon name="eye" />Reveal spec{session.apiKey ? '' : ': needs the API key'}</button>
					{/if}
				</div>
			</section>
		</div>
	{/if}
</div>

<style>
	.back {
		display: inline-flex;
		align-items: center;
		gap: 6px;
		align-self: flex-start;
		margin: -4px 0 -6px;
		padding: 4px 6px 4px 2px;
		border-radius: 8px;
		color: var(--c-body);
		font-size: 13.5px;
		font-weight: 520;
		text-decoration: none;
	}
	.back:hover {
		color: var(--c-ink);
	}
	.head-card {
		padding: 18px 20px 20px;
	}
	.head {
		display: flex;
		flex-wrap: wrap;
		align-items: flex-start;
		justify-content: space-between;
		gap: 12px 20px;
	}
	.who {
		min-width: 0;
	}
	.eyebrow {
		margin: 0 0 4px;
		color: var(--c-body);
		font-size: 13px;
		font-weight: 520;
	}
	.key {
		margin: 0;
		font-family: var(--font-mono);
		font-size: 18px;
		font-weight: 600;
		letter-spacing: -0.01em;
		overflow-wrap: anywhere;
	}
	.state {
		margin: 10px 0 0;
	}
	.actions {
		display: flex;
		flex-wrap: wrap;
		gap: 8px;
	}
	.facts {
		display: grid;
		grid-template-columns: repeat(4, minmax(0, 1fr));
		gap: 12px;
		margin: 18px 0 0;
		padding-top: 16px;
		border-top: 1px solid var(--c-line);
	}
	.facts dt {
		color: var(--c-body);
		font-size: 12.5px;
	}
	.facts dd .muted {
		white-space: nowrap;
	}
	.facts dd {
		margin: 3px 0 0;
		font-size: 14.5px;
		font-variant-numeric: tabular-nums;
	}
	.progress {
		display: grid;
		grid-auto-columns: minmax(0, 1fr);
		grid-auto-flow: column;
		gap: 6px;
		margin: 18px 0 0;
		padding: 0;
		list-style: none;
	}
	.progress li {
		display: flex;
		flex-direction: column;
		gap: 7px;
		min-width: 0;
	}
	.progress .seg {
		height: 6px;
		border-radius: 3px;
		background: var(--c-line);
	}
	.progress .done .seg {
		background: var(--c-state-done);
	}
	.progress .now .seg {
		background: var(--c-state-run);
	}
	.progress .retry .seg {
		background: var(--c-state-retry);
	}
	.progress .probe .seg {
		background: var(--c-state-probe);
	}
	.progress .failed .seg {
		background: var(--c-danger);
	}
	.progress .cancelled .seg {
		background: repeating-linear-gradient(135deg, var(--c-line-strong) 0 3px, transparent 3px 6px);
	}
	.lbl {
		display: flex;
		align-items: center;
		gap: 6px;
		min-width: 0;
		overflow: hidden;
		color: var(--c-ink-soft);
		font-family: var(--font-mono);
		font-size: 12.5px;
		text-overflow: ellipsis;
		white-space: nowrap;
	}
	.pending .lbl,
	.cancelled .lbl {
		color: var(--c-body);
	}
	.n {
		display: inline-grid;
		flex: none;
		place-items: center;
		min-width: 18px;
		height: 18px;
		padding: 0 4px;
		border-radius: 5px;
		background: var(--c-app);
		box-shadow: inset 0 0 0 1px var(--c-line);
		color: var(--c-body);
		font-family: var(--font-sans);
		font-size: 11px;
		font-weight: 600;
	}

	.detail-grid {
		grid-template-columns: minmax(0, 1.6fr) minmax(0, 1fr);
		align-items: start;
	}
	@media (max-width: 1100px) {
		.detail-grid {
			grid-template-columns: minmax(0, 1fr);
		}
	}

	/* the vertical timeline */
	.tl {
		margin: 0;
		padding: 0;
		list-style: none;
	}
	.tl li {
		position: relative;
		display: grid;
		grid-template-columns: 30px minmax(0, 1fr) auto;
		gap: 12px;
		padding-bottom: 14px;
	}
	.rail {
		position: relative;
		display: flex;
		justify-content: center;
	}
	.tl li:not(:last-child) .rail::after {
		content: '';
		position: absolute;
		top: 30px;
		bottom: -14px;
		left: 50%;
		width: 2px;
		margin-left: -1px;
		background: var(--c-line);
	}
	.node {
		position: relative;
		z-index: 1;
		display: grid;
		place-items: center;
		width: 28px;
		height: 28px;
		border-radius: 9px;
		background: var(--c-state-queued-bg);
		color: var(--c-state-queued-ink);
	}
	.tone-run .node {
		background: var(--c-state-run-bg);
		color: var(--c-state-run-ink);
	}
	.tone-ok .node {
		background: var(--c-state-done-bg);
		color: var(--c-state-done-ink);
	}
	.tone-fail .node {
		background: var(--c-danger-bg);
		color: var(--c-danger);
	}
	.tone-probe .node {
		background: var(--c-state-probe-bg);
		color: var(--c-state-probe-ink);
	}
	.tone-warn .node {
		background: var(--c-state-retry-bg);
		color: var(--c-state-retry-ink);
	}
	.tone-cancel .node {
		background: var(--c-card);
		box-shadow: inset 0 0 0 1px var(--c-line-strong);
		color: var(--c-state-cancelled);
	}
	.what {
		min-width: 0;
		padding-top: 4px;
	}
	.title {
		margin: 0;
		color: var(--c-ink);
		font-size: 14px;
		font-weight: 540;
		line-height: 1.35;
	}
	.notes {
		margin: 3px 0 0;
		color: var(--c-body);
		font-size: 13px;
		line-height: 1.45;
	}
	.when {
		display: flex;
		flex-direction: column;
		align-items: flex-end;
		margin: 0;
		padding-top: 4px;
		font-variant-numeric: tabular-nums;
		white-space: nowrap;
	}
	.off {
		color: var(--c-ink-soft);
		font-size: 13px;
		font-weight: 540;
	}
	.when time {
		color: var(--c-body);
		font-family: var(--font-mono);
		font-size: 11.5px;
	}
	.tl li.gap {
		grid-template-columns: 30px minmax(0, 1fr);
	}
	.gap .node {
		background: transparent;
		box-shadow: inset 0 0 0 1.5px var(--c-line-strong);
		border-style: dashed;
		color: var(--c-body);
	}
	.fold .gap-text {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 6px 10px;
		padding-top: 0;
		font-style: normal;
	}
	.fold-note {
		font-style: italic;
	}
	.gap-text {
		margin: 0;
		padding-top: 5px;
		color: var(--c-body);
		font-size: 13px;
		font-style: italic;
	}

	/* the spec */
	.step-list {
		margin: 0;
		padding: 0;
		overflow: hidden;
		border-radius: 10px;
		box-shadow: inset 0 0 0 1px var(--c-line-strong);
		list-style: none;
	}
	.step-list li {
		display: flex;
		align-items: center;
		gap: 8px;
		min-height: 36px;
		padding: 6px 11px;
		overflow-wrap: anywhere;
	}
	.step-list li + li {
		border-top: 1px solid var(--c-line);
	}
	.arrow,
	.handler {
		color: var(--c-body);
	}
	.input-label {
		margin: 14px 0 6px;
		color: var(--c-body);
		font-size: 13px;
	}
	.input {
		margin: 0;
		padding: 10px 12px;
		border-radius: 10px;
		background: var(--c-panel);
		box-shadow: inset 0 0 0 1px var(--c-line);
		overflow-wrap: anywhere;
	}
	.reveal-hint {
		margin: 0 0 12px;
		font-size: 13.5px;
		line-height: 1.5;
	}

	@media (max-width: 600px) {
		.head-card {
			padding: 16px;
		}
		.facts {
			grid-template-columns: repeat(2, minmax(0, 1fr));
		}
		.progress {
			grid-auto-flow: row;
			grid-template-columns: repeat(2, minmax(0, 1fr));
		}
		.tl li {
			grid-template-columns: 30px minmax(0, 1fr);
		}
		.when {
			grid-column: 2;
			flex-direction: row;
			gap: 8px;
			align-items: baseline;
			padding-top: 0;
		}
	}
</style>
