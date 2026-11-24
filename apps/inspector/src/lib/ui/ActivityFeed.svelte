<script lang="ts">
	// The activity feed (GET /events): newest first, across runs. Each row names the event, links its run, and says
	// how long ago; the absolute time is in the row's title and its <time datetime>.
	import Icon, { type IconName } from '../Icon.svelte';
	import type { FeedEvent } from '../api.ts';
	import { fmtAgo, fmtClock } from '../format.ts';
	import { runHref } from '../routes.ts';

	let { events, now }: { events: FeedEvent[]; now: number } = $props();

	function view(e: FeedEvent): { icon: IconName; tone: string; text: string } {
		const step = typeof e.step === 'number' ? ` step ${e.step}` : '';
		switch (e.kind) {
			case 'accepted':
				return { icon: 'inbox', tone: 'neutral', text: 'Accepted' };
			case 'dispatched':
				return { icon: 'send', tone: 'run', text: `Dispatched${step}${e.attempt && e.attempt > 1 ? `, attempt ${e.attempt}` : ''}` };
			case 'checkpointed':
				return { icon: 'bookmark-check', tone: 'ok', text: `Checkpointed${step}` };
			case 'step_failed':
				return { icon: 'x-circle', tone: 'fail', text: `Attempt failed on${step}` };
			case 'reply_lost':
				return { icon: 'unplug', tone: 'probe', text: `Reply lost on${step}` };
			case 'reconciled':
				return { icon: 'scan', tone: e.effect_found ? 'ok' : 'warn', text: `Reconciled${step}: ${e.effect_found ? 'effect found' : 'no effect'}` };
			case 'cancel_requested':
				return { icon: 'ban', tone: 'cancel', text: 'Cancel requested' };
			case 'finished':
				return {
					icon: 'flag',
					tone: e.outcome === 'done' ? 'ok' : e.outcome === 'failed' ? 'fail' : 'cancel',
					text: e.outcome === 'done' ? 'Finished: done' : e.outcome === 'failed' ? 'Finished: failed' : `Finished: ${e.outcome ?? '?'}`,
				};
			default:
				return { icon: 'circle-dot', tone: 'neutral', text: String(e.kind) };
		}
	}
</script>

{#if events.length === 0}
	<p class="empty">No activity yet</p>
{:else}
	<ol class="feed">
		{#each events as e, i (`${e.at}-${e.run}-${e.kind}-${i}`)}
			{@const v = view(e)}
			<li>
				<span class="ic {v.tone}" aria-hidden="true"><Icon name={v.icon} size={14} /></span>
				<span class="what">
					<span class="text">{v.text}</span>
					<a class="run" href={runHref(e.run)}><span class="id">{e.run}</span>{#if e.key !== null}<span class="key">{e.key}</span>{:else}<span class="key expired">expired</span>{/if}</a>
				</span>
				<time datetime={new Date(e.at).toISOString()} title={fmtClock(e.at)}>{fmtAgo(now - e.at)}</time>
			</li>
		{/each}
	</ol>
{/if}

<style>
	.feed {
		display: flex;
		flex-direction: column;
		margin: 0;
		padding: 0;
		list-style: none;
	}
	li {
		display: grid;
		grid-template-columns: 28px minmax(0, 1fr) auto;
		gap: 10px;
		align-items: center;
		min-height: 46px;
		padding: 6px 0;
		border-top: 1px solid var(--c-line);
	}
	li:first-child {
		border-top: 0;
	}
	.ic {
		display: grid;
		place-items: center;
		width: 28px;
		height: 28px;
		border-radius: 9px;
		background: var(--c-state-queued-bg);
		color: var(--c-state-queued-ink);
	}
	.ic.run {
		background: var(--c-state-run-bg);
		color: var(--c-state-run-ink);
	}
	.ic.ok {
		background: var(--c-state-done-bg);
		color: var(--c-state-done-ink);
	}
	.ic.fail {
		background: var(--c-danger-bg);
		color: var(--c-danger);
	}
	.ic.probe {
		background: var(--c-state-probe-bg);
		color: var(--c-state-probe-ink);
	}
	.ic.warn {
		background: var(--c-state-retry-bg);
		color: var(--c-state-retry-ink);
	}
	.ic.cancel {
		background: transparent;
		box-shadow: inset 0 0 0 1px var(--c-line-strong);
		color: var(--c-state-cancelled);
	}
	.what {
		display: flex;
		flex-direction: column;
		min-width: 0;
		line-height: 1.35;
	}
	.text {
		color: var(--c-ink);
		font-size: 13.5px;
		font-weight: 520;
	}
	.run {
		display: flex;
		gap: 7px;
		min-width: 0;
		color: var(--c-body);
		font-family: var(--font-mono);
		font-size: 12.5px;
		text-decoration: none;
	}
	.run:hover .key,
	.run:focus-visible .key {
		color: var(--c-ink);
		text-decoration: underline;
		text-underline-offset: 3px;
	}
	.id {
		color: var(--c-brand-deep);
		font-weight: 600;
	}
	.key.expired {
		font-family: var(--font-sans);
		font-style: italic;
	}
	.key {
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
	}
	time {
		color: var(--c-body);
		font-size: 12.5px;
		font-variant-numeric: tabular-nums;
		white-space: nowrap;
	}
	.empty {
		margin: 0;
		padding: 28px 12px;
		color: var(--c-body);
		text-align: center;
	}
</style>
