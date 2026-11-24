<script lang="ts">
	// "Live · updated 3 s ago" with a pause button. The status word is a polite live region (it changes rarely:
	// live, paused, retrying, hidden); the seconds count beside it is not announced every second.
	import Icon from '../Icon.svelte';
	import { fmtAgo } from '../format.ts';
	import { live } from '../live.svelte.ts';

	const state = $derived(live.paused ? 'paused' : !live.visible ? 'hidden' : live.failing > 0 ? 'retrying' : 'live');
	const word = $derived(state === 'paused' ? 'Paused' : state === 'hidden' ? 'Paused while hidden' : state === 'retrying' ? 'Retrying' : 'Live');
	const ago = $derived(live.lastUpdate === null ? 'waiting for data' : `updated ${fmtAgo(live.now - live.lastUpdate)}`);
	const retry = $derived(state === 'retrying' && live.retryAt !== null ? Math.max(0, Math.ceil((live.retryAt - live.now) / 1000)) : null);
</script>

<div class="live {state}">
	<span class="status" role="status"><i class="dot" aria-hidden="true"></i>{word}</span>
	<span class="ago">· {ago}{#if retry !== null}, next try in {retry} s{/if}</span>
	<button
		class="btn ghost sm toggle"
		aria-pressed={live.paused}
		title={live.paused ? 'Resume live updates' : 'Pause live updates'}
		onclick={() => live.setPaused(!live.paused)}
	>
		<Icon name={live.paused ? 'play' : 'pause'} size={14} /><span class="sr">{live.paused ? 'Resume live updates' : 'Pause live updates'}</span>
	</button>
</div>

<style>
	.live {
		display: inline-flex;
		align-items: center;
		gap: 6px;
		min-width: 0;
		height: 34px;
		padding: 0 3px 0 11px;
		border-radius: 11px;
		background: var(--c-card);
		box-shadow: inset 0 0 0 1px var(--c-line);
		color: var(--c-body);
		font-size: 13px;
		white-space: nowrap;
	}
	.status {
		display: inline-flex;
		align-items: center;
		gap: 7px;
		color: var(--c-ink);
		font-weight: 560;
	}
	.dot {
		width: 7px;
		height: 7px;
		border-radius: 50%;
		background: var(--c-state-done);
		box-shadow: 0 0 0 3px var(--c-state-done-bg);
	}
	.paused .dot,
	.hidden .dot {
		background: var(--c-state-queued);
		box-shadow: 0 0 0 3px var(--c-state-queued-bg);
	}
	.retrying .dot {
		background: var(--c-state-retry);
		box-shadow: 0 0 0 3px var(--c-state-retry-bg);
	}
	.ago {
		overflow: hidden;
		text-overflow: ellipsis;
		font-variant-numeric: tabular-nums;
	}
	.toggle {
		width: 28px;
		height: 28px;
		padding: 0;
		border-radius: 8px;
		color: var(--c-ink-soft);
	}
</style>
