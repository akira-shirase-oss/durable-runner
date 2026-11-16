<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import { listRuns, cancelRun, loadApiKey, saveApiKey, type RunItem } from '../lib/api';

	let runs = $state<RunItem[]>([]);
	// Newest first. `cursor` is the `before` of the page on screen (null =
	// the newest page); `trail` holds the cursors of the newer pages so
	// "newer" can step back; `older` is the next page's cursor.
	let cursor = $state<number | null>(null);
	let trail = $state<(number | null)[]>([]);
	let older = $state<number | null>(null);
	let apiKey = $state('');
	let error = $state<string | null>(null);
	let timer: ReturnType<typeof setInterval> | null = null;

	async function refresh() {
		try {
			const page = await listRuns(cursor);
			runs = page.runs;
			older = page.next_before;
			error = null;
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		}
	}

	async function cancel(id: number) {
		try {
			await cancelRun(id, apiKey);
			await refresh();
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		}
	}

	async function showOlder() {
		if (older === null) return;
		trail = [...trail, cursor];
		cursor = older;
		await refresh();
	}

	async function showNewer() {
		if (trail.length === 0) return;
		cursor = trail.at(-1) ?? null;
		trail = trail.slice(0, -1);
		await refresh();
	}

	function cancellable(state: string): boolean {
		return !state.startsWith('done') && !state.startsWith('failed') && state !== 'cancelled';
	}

	onMount(() => {
		apiKey = loadApiKey();
		refresh();
		timer = setInterval(refresh, 2000);
	});
	onDestroy(() => {
		if (timer) clearInterval(timer);
	});
</script>

<main>
	<h1>durable — runs</h1>
	<label class="key-field">
		API key (needed to cancel)
		<input
			type="password"
			autocomplete="off"
			bind:value={apiKey}
			onchange={() => saveApiKey(apiKey)}
		/>
	</label>
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
		<nav class="pager">
			<button class="nav" onclick={showNewer} disabled={trail.length === 0}>&larr; newer</button>
			<span class="empty">page {trail.length + 1}, newest first</span>
			<button class="nav" onclick={showOlder} disabled={older === null}>older &rarr;</button>
		</nav>
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
	.key-field {
		display: flex;
		gap: 0.6rem;
		align-items: center;
		color: #8b949e;
		margin-bottom: 1rem;
	}
	.key-field input {
		flex: 1;
		max-width: 28rem;
		background: #0d1117;
		color: #c9d1d9;
		border: 1px solid #30363d;
		border-radius: 4px;
		padding: 0.25rem 0.5rem;
		font: inherit;
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
	.pager {
		display: flex;
		justify-content: space-between;
		align-items: center;
		margin-top: 1rem;
	}
	button.nav {
		color: #c9d1d9;
	}
	button.nav:hover:enabled {
		border-color: #8b949e;
	}
	button:disabled {
		opacity: 0.4;
		cursor: default;
	}
</style>
