<script lang="ts">
	// The server's status from /health and /ready: Ready, Not ready (with the server's reason), or Unreachable.
	import type { ResourceStatus } from '../live.svelte.ts';

	let {
		data,
		status,
		compact = false,
	}: { data: { ready: boolean; code: string | null } | null; status: ResourceStatus; compact?: boolean } = $props();

	const view = $derived.by(() => {
		if (status === 'error') return { tone: 'bad', text: 'Unreachable', title: 'The server did not answer /health' };
		if (!data) return { tone: 'idle', text: 'Checking…', title: 'Asking /health and /ready' };
		if (data.ready) return { tone: 'ok', text: 'Ready', title: '/health and /ready both answer' };
		return { tone: 'bad', text: compact ? 'Not ready' : `Not ready: ${data.code ?? 'starting'}`, title: `/ready: ${data.code ?? 'starting'}` };
	});
</script>

<span class="pill {view.tone}" title={view.title}><i class="dot" aria-hidden="true"></i>{view.text}</span>

<style>
	.pill {
		display: inline-flex;
		align-items: center;
		gap: 7px;
		height: 28px;
		padding: 0 11px;
		border-radius: 999px;
		background: var(--c-state-queued-bg);
		color: var(--c-state-queued-ink);
		font-size: 13px;
		font-weight: 540;
		white-space: nowrap;
	}
	.dot {
		width: 7px;
		height: 7px;
		border-radius: 50%;
		background: var(--c-state-queued);
	}
	.ok {
		background: var(--c-state-done-bg);
		color: var(--c-state-done-ink);
	}
	.ok .dot {
		background: var(--c-state-done);
	}
	.bad {
		background: var(--c-danger-bg);
		color: var(--c-danger);
	}
	.bad .dot {
		background: var(--c-danger);
	}
</style>
