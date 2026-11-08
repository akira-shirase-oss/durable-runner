<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import { listRuns, cancelRun, type RunItem } from '../lib/api';

	let runs = $state<RunItem[]>([]);
	let error = $state<string | null>(null);
	let timer: ReturnType<typeof setInterval> | null = null;

	async function refresh() {
		try {
			runs = await listRuns();
			error = null;
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		}
	}

	async function cancel(id: number) {
		try {
			await cancelRun(id);
			await refresh();
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		}
	}

	function cancellable(state: string): boolean {
		return !state.startsWith('done') && !state.startsWith('failed') && state !== 'cancelled';
	}

	onMount(() => {
		refresh();
		timer = setInterval(refresh, 2000);
	});
	onDestroy(() => {
		if (timer) clearInterval(timer);
	});
</script>

<main>
	<h1>durable — runs</h1>
	{#if error}
		<p class="error">cannot reach the server: {error}</p>
	{/if}
	{#if runs.length === 0 && !error}
		<p class="empty">no runs yet</p>
	{/if}
	{#if runs.length > 0}
		<table>
			<thead>
				<tr><th>id</th><th>key</th><th>state</th><th>steps</th><th>finished</th><th></th></tr>
			</thead>
			<tbody>
				{#each runs as r (r.id)}
					<tr>
						<td>{r.id}</td>
						<td class="key">{r.key}</td>
						<td class="state" data-state={r.state.split(':')[0]}>{r.state}</td>
						<td>{r.steps}</td>
						<td>{r.finished_at ? new Date(r.finished_at).toLocaleTimeString() : '—'}</td>
						<td>
							{#if cancellable(r.state)}
								<button onclick={() => cancel(r.id)}>cancel</button>
							{/if}
						</td>
					</tr>
				{/each}
			</tbody>
		</table>
	{/if}
</main>

<style>
	:global(body) {
		margin: 0;
		background: #0d1117;
		color: #c9d1d9;
		font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
		font-size: 14px;
	}
	main {
		max-width: 960px;
		margin: 2rem auto;
		padding: 0 1rem;
	}
	h1 {
		font-size: 1.1rem;
		font-weight: 600;
		border-bottom: 1px solid #30363d;
		padding-bottom: 0.5rem;
	}
	table {
		width: 100%;
		border-collapse: collapse;
	}
	th,
	td {
		text-align: left;
		padding: 0.4rem 0.6rem;
		border-bottom: 1px solid #21262d;
	}
	th {
		color: #8b949e;
		font-weight: 500;
	}
	.key {
		color: #79c0ff;
	}
	.state[data-state='done'] {
		color: #3fb950;
	}
	.state[data-state='failed'] {
		color: #f85149;
	}
	.state[data-state='cancelled'] {
		color: #8b949e;
	}
	.state[data-state='running'],
	.state[data-state='reconciling'] {
		color: #d29922;
	}
	.state[data-state='queued'],
	.state[data-state='retry_waiting'] {
		color: #a371f7;
	}
	.error {
		color: #f85149;
	}
	.empty {
		color: #8b949e;
	}
	button {
		background: #21262d;
		color: #f85149;
		border: 1px solid #30363d;
		border-radius: 4px;
		padding: 0.15rem 0.6rem;
		font: inherit;
		cursor: pointer;
	}
	button:hover {
		border-color: #f85149;
	}
</style>
