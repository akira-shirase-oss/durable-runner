<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import {
		listRuns, inspect, spec as fetchSpec, submitJob, cancelRun,
		diagnostics, compact, backup, health, ready,
		loadKey, saveKey, forgetKeys,
		type RunItem, type Spec, type Diag,
	} from '../lib/api';

	type Tab = 'runs' | 'submit' | 'admin';
	type Order = 'asc' | 'desc';

	let tab = $state<Tab>('runs');
	let theme = $state<'dark' | 'light'>('dark');
	let readKey = $state('');
	let apiKey = $state('');
	let error = $state<string | null>(null);
	let notice = $state<string | null>(null);
	let timer: ReturnType<typeof setInterval> | null = null;

	// Header status
	let alive = $state<boolean | null>(null);
	let readyNow = $state<{ ready: boolean; code: string | null } | null>(null);

	// Runs tab: paging works in both orders; `trail` walks back toward page 1.
	let order = $state<Order>('desc');
	let runs = $state<RunItem[]>([]);
	let cursor = $state<number | null>(null);
	let trail = $state<(number | null)[]>([]);
	let nextCursor = $state<number | null>(null);
	let detail = $state<RunItem | null>(null);
	let detailSpec = $state<Spec | null>(null);
	let detailErr = $state<string | null>(null);

	// Submit tab
	let opKey = $state('');
	let steps = $state<{ name: string; handler: string }[]>([{ name: 'only', handler: '' }]);
	let inputText = $state('');
	let notBefore = $state('');
	let lastRun = $state<number | null>(null);

	// Admin tab
	let diag = $state<Diag | null>(null);
	let backupName = $state(`backup-${new Date().toISOString().slice(0, 10)}`);
	let confirming = $state<null | 'compact' | 'backup'>(null);

	function applyTheme(t: 'dark' | 'light') {
		theme = t;
		document.documentElement.dataset.theme = t;
		try { localStorage.setItem('durable-theme', t); } catch { /* cosmetic only */ }
	}

	async function refreshStatus() {
		try {
			await health();
			alive = true;
		} catch {
			alive = false;
			return;
		}
		try {
			readyNow = await ready();
		} catch {
			readyNow = null;
		}
	}

	async function refresh() {
		try {
			const page = await listRuns(readKey || apiKey, order, cursor);
			runs = page.runs;
			nextCursor = (order === 'desc' ? page.next_before : page.next_after) ?? null;
			error = null;
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		}
	}

	function setOrder(o: Order) {
		order = o;
		cursor = null;
		trail = [];
		refresh();
	}

	async function pageForward() {
		if (nextCursor === null) return;
		trail = [...trail, cursor];
		cursor = nextCursor;
		await refresh();
	}

	async function pageBack() {
		if (trail.length === 0) return;
		cursor = trail.at(-1) ?? null;
		trail = trail.slice(0, -1);
		await refresh();
	}

	async function openDetail(id: number) {
		detail = null;
		detailSpec = null;
		detailErr = null;
		try {
			detail = await inspect(readKey || apiKey, id);
		} catch (e) {
			detailErr = e instanceof Error ? e.message : String(e);
		}
	}

	async function revealSpec() {
		if (!detail) return;
		try {
			detailSpec = (await fetchSpec(apiKey, detail.id)).spec;
			detailErr = null;
		} catch (e) {
			detailErr = e instanceof Error ? e.message : String(e);
		}
	}

	async function cancel(id: number) {
		try {
			await cancelRun(id, apiKey);
			notice = `run ${id} cancelled`;
			await refresh();
			if (detail?.id === id) await openDetail(id);
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		}
	}

	function cancellable(state: string): boolean {
		return !state.startsWith('done') && !state.startsWith('failed') && state !== 'cancelled';
	}

	function b64(s: string): string {
		return btoa(unescape(encodeURIComponent(s)));
	}

	async function submit() {
		try {
			const body: Parameters<typeof submitJob>[1] = {
				op_key: opKey,
				spec: { version: 1, steps: steps.filter((s) => s.name && s.handler), input_b64: b64(inputText) },
			};
			if (notBefore) body.not_before = Date.parse(notBefore);
			const r = await submitJob(apiKey, body);
			lastRun = r.run_id;
			notice = `run ${r.run_id} accepted under '${opKey}'`;
			error = null;
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		}
	}

	async function refreshDiag() {
		try {
			diag = await diagnostics(apiKey);
			error = null;
		} catch (e) {
			diag = null;
			error = e instanceof Error ? e.message : String(e);
		}
	}

	async function doCompact() {
		confirming = null;
		try {
			const r = await compact(apiKey);
			notice = `compacted — journal is now generation ${r.generation}`;
			error = null;
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		}
	}

	async function doBackup() {
		confirming = null;
		try {
			const r = await backup(apiKey, backupName);
			notice = `backup '${r.backup}' written under the server's backup root`;
			error = null;
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		}
	}

	function forget() {
		forgetKeys();
		readKey = '';
		apiKey = '';
		detail = null;
		detailSpec = null;
		diag = null;
		notice = 'keys forgotten — nothing held for this tab';
		refresh();
	}

	function keyBadge(kind: 'read' | 'api'): string {
		const set = kind === 'read' ? readKey : apiKey;
		return set ? 'set' : 'not set';
	}

	onMount(() => {
		try { theme = (localStorage.getItem('durable-theme') as 'dark' | 'light') || 'dark'; } catch { /* default */ }
		document.documentElement.dataset.theme = theme;
		readKey = loadKey('read');
		apiKey = loadKey('api');
		refreshStatus();
		refresh();
		timer = setInterval(() => {
			refreshStatus();
			if (tab === 'runs') refresh();
		}, 2000);
	});
	onDestroy(() => {
		if (timer) clearInterval(timer);
	});
</script>

<main>
	<header>
		<h1>durable</h1>
		<span class="pill" class:ok={alive === true && readyNow?.ready} class:bad={alive === false || readyNow?.ready === false}>
			{#if alive === null}…
			{:else if alive === false}server unreachable
			{:else if readyNow === null}up
			{:else if readyNow.ready}ready
			{:else}not ready: {readyNow.code ?? 'starting'}{/if}
		</span>
		<nav class="tabs">
			<button class:active={tab === 'runs'} onclick={() => (tab = 'runs')}>runs</button>
			<button class:active={tab === 'submit'} onclick={() => (tab = 'submit')}>submit</button>
			<button class:active={tab === 'admin'} onclick={() => { tab = 'admin'; refreshDiag(); }}>admin</button>
		</nav>
		<button class="tool" title="switch theme" onclick={() => applyTheme(theme === 'dark' ? 'light' : 'dark')}>
			{theme === 'dark' ? 'light' : 'dark'}
		</button>
		<button class="tool danger" onclick={forget}>Forget keys</button>
	</header>

	<section class="keys">
		<label>
			read key <em class="badge" class:set={!!readKey}>{keyBadge('read')}</em>
			<input type="password" autocomplete="off" placeholder="lists and run detail"
				bind:value={readKey}
				onchange={() => { saveKey('read', readKey); refresh(); }} />
		</label>
		<label>
			API key <em class="badge" class:set={!!apiKey}>{keyBadge('api')}</em>
			<input type="password" autocomplete="off" placeholder="submit, cancel, spec, admin"
				bind:value={apiKey}
				onchange={() => saveKey('api', apiKey)} />
		</label>
	</section>

	{#if error}<p class="error">{error}</p>{/if}
	{#if notice}<p class="notice">{notice} <button class="tool" onclick={() => (notice = null)}>dismiss</button></p>{/if}

	{#if tab === 'runs'}
		{#if !readKey && !apiKey}<p class="hint">reads need a key — enter the read key (or the API key) above</p>{/if}
		<div class="bar">
			<span class="hint">order</span>
			<button class="tool" class:active={order === 'desc'} onclick={() => setOrder('desc')}>newest first</button>
			<button class="tool" class:active={order === 'asc'} onclick={() => setOrder('asc')}>oldest first</button>
		</div>
		{#if runs.length === 0 && !error}
			<p class="empty">no runs yet</p>
		{/if}
		{#if runs.length > 0}
			<table class="runlist">
				<thead><tr><th>id</th><th>key</th><th>state</th><th>steps</th><th>finished</th><th></th></tr></thead>
				<tbody>
					{#each runs as r (r.id)}
						<tr class:selected={detail?.id === r.id}>
							<td><button class="link" onclick={() => openDetail(r.id)}>{r.id}</button></td>
							<td class="key">{r.key}</td>
							<td class="state" data-state={r.state.split(':')[0]}>{r.state}</td>
							<td>{r.steps}</td>
							<td>{r.finished_at ? new Date(r.finished_at).toLocaleTimeString() : '—'}</td>
							<td>{#if cancellable(r.state)}<button class="danger" onclick={() => cancel(r.id)}>cancel</button>{/if}</td>
						</tr>
					{/each}
				</tbody>
			</table>
			<nav class="pager">
				<button class="tool" onclick={pageBack} disabled={trail.length === 0}>&larr; back</button>
				<span class="empty">page {trail.length + 1}, {order === 'desc' ? 'newest' : 'oldest'} first</span>
				<button class="tool" onclick={pageForward} disabled={nextCursor === null}>forward &rarr;</button>
			</nav>
		{/if}
		{#if detailErr}<p class="error">{detailErr}</p>{/if}
		{#if detail}
			<section class="panel">
				<h2>run {detail.id} <button class="tool" onclick={() => { detail = null; detailSpec = null; }}>close</button></h2>
				<dl>
					<dt>key</dt><dd class="key">{detail.key}</dd>
					<dt>state</dt><dd class="state" data-state={detail.state.split(':')[0]}>{detail.state}</dd>
					<dt>steps</dt><dd>{detail.steps}</dd>
					<dt>finished</dt><dd>{detail.finished_at ? new Date(detail.finished_at).toLocaleString() : '—'}</dd>
				</dl>
				{#if cancellable(detail.state)}<button class="danger" onclick={() => cancel(detail!.id)}>cancel this run</button>{/if}
				{#if !detailSpec}
					<button class="tool" disabled={!apiKey} title={apiKey ? '' : 'the spec may carry customer input (D-051) — the API key reveals it'} onclick={revealSpec}>
						reveal spec{apiKey ? '' : ' — needs the API key'}
					</button>
				{:else}
					<dl>
						<dt>version</dt><dd>{detailSpec.version}</dd>
						<dt>steps</dt>
						<dd>{#each detailSpec.steps as s}<span class="chip">{s.name} → {s.handler}</span> {/each}</dd>
						<dt>input</dt><dd class="key">{detailSpec.input_b64 || '—'}</dd>
					</dl>
				{/if}
			</section>
		{/if}
	{:else if tab === 'submit'}
		{#if !apiKey}<p class="hint">submitting needs the API key — enter it above</p>{/if}
		<form class="panel" onsubmit={(e) => { e.preventDefault(); submit(); }}>
			<label>operation key (idempotency)
				<input required maxlength="256" bind:value={opKey} placeholder="deploy-2026-11-18" /></label>
			<fieldset>
				<legend>steps</legend>
				{#each steps as s, i}
					<div class="step-row">
						<input required placeholder="name" bind:value={s.name} />
						<input required placeholder="handler" bind:value={s.handler} />
						<button type="button" class="tool" disabled={steps.length === 1}
							onclick={() => (steps = steps.filter((_, j) => j !== i))}>remove</button>
					</div>
				{/each}
				<button type="button" class="tool" onclick={() => (steps = [...steps, { name: '', handler: '' }])}>+ step</button>
			</fieldset>
			<label>input (sent base64-encoded)
				<textarea rows="3" bind:value={inputText} placeholder="job input bytes"></textarea></label>
			<label>not before (optional)
				<input type="datetime-local" bind:value={notBefore} /></label>
			<div>
				<button type="submit" disabled={!apiKey || !opKey || steps.some((s) => !s.name || !s.handler)}>submit</button>
				{#if lastRun}<span class="notice-inline">last accepted: run {lastRun}</span>{/if}
			</div>
		</form>
	{:else}
		{#if !apiKey}<p class="hint">admin operations need the API key — enter it above</p>{/if}
		<section class="panel">
			<h2>diagnostics <button class="tool" onclick={refreshDiag} disabled={!apiKey}>refresh</button></h2>
			{#if diag}
				<dl>
					<dt>journal generation</dt><dd>{diag.generation}</dd>
					<dt>records written</dt><dd>{diag.records_written.toLocaleString()}</dd>
					<dt>live runs</dt><dd>{diag.live_runs.toLocaleString()}</dd>
					<dt>active runs</dt><dd>{diag.active_runs.toLocaleString()}</dd>
					<dt>index bytes</dt><dd>{diag.index_bytes.toLocaleString()}</dd>
					<dt>dedup keys</dt><dd>{diag.dedup_keys.toLocaleString()}</dd>
					<dt>disk reserve</dt><dd class={diag.space_ok ? 'state-ok' : 'state-bad'}>{diag.space_ok ? 'ok' : 'below reserve'}</dd>
				</dl>
				{#if Object.keys(diag.states).length}
					<h3>states</h3>
					<dl>{#each Object.entries(diag.states) as [s, n]}<dt>{s}</dt><dd>{n.toLocaleString()}</dd>{/each}</dl>
				{/if}
				{#if Object.keys(diag.errors).length}
					<h3>errors (counted, never dropped)</h3>
					<dl>{#each Object.entries(diag.errors) as [s, n]}<dt>{s}</dt><dd>{n.toLocaleString()}</dd>{/each}</dl>
				{/if}
			{:else}
				<p class="empty">no diagnostics yet — refresh above</p>
			{/if}
		</section>
		<section class="panel">
			<h2>compact the journal</h2>
			<p class="hint">expires finished runs past retention, snapshots the index, swaps the manifest — all or nothing</p>
			{#if confirming === 'compact'}
				<span class="confirm">run compaction now? <button class="danger" onclick={doCompact}>yes, compact</button> <button class="tool" onclick={() => (confirming = null)}>no</button></span>
			{:else}
				<button disabled={!apiKey} onclick={() => (confirming = 'compact')}>compact</button>
			{/if}
		</section>
		<section class="panel">
			<h2>back up the journal</h2>
			<p class="hint">copies the journal into a named directory under the server's --backup-root (needs the flag set)</p>
			<label>backup name <input maxlength="64" pattern="[A-Za-z0-9_.\-]+" bind:value={backupName} /></label>
			{#if confirming === 'backup'}
				<span class="confirm">write backup '{backupName}'? <button class="danger" onclick={doBackup}>yes, back up</button> <button class="tool" onclick={() => (confirming = null)}>no</button></span>
			{:else}
				<button disabled={!apiKey || !backupName} onclick={() => (confirming = 'backup')}>backup</button>
			{/if}
		</section>
	{/if}
</main>

<style>
	/* Semantic tokens only — a later shared skin re-themes these names. */
	:root {
		--bg: #0d1117;
		--bg-raised: #161b22;
		--fg: #c9d1d9;
		--fg-muted: #8b949e;
		--border: #30363d;
		--border-soft: #21262d;
		--accent: #79c0ff;
		--ok: #3fb950;
		--warn: #d29922;
		--danger: #f85149;
		--queued: #a371f7;
		--chip-bg: #21262d;
	}
	:root[data-theme='light'] {
		--bg: #ffffff;
		--bg-raised: #f6f8fa;
		--fg: #1f2328;
		--fg-muted: #59636e;
		--border: #d1d9e0;
		--border-soft: #e8ebee;
		--accent: #0969da;
		--ok: #1a7f37;
		--warn: #9a6700;
		--danger: #d1242f;
		--queued: #8250df;
		--chip-bg: #eaeef2;
	}
	:global(body) {
		margin: 0;
		background: var(--bg);
		color: var(--fg);
		font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
		font-size: 14px;
	}
	main {
		max-width: 960px;
		margin: 1.5rem auto;
		padding: 0 1rem;
	}
	header {
		display: flex;
		align-items: center;
		gap: 0.75rem;
		border-bottom: 1px solid var(--border);
		padding-bottom: 0.6rem;
		flex-wrap: wrap;
	}
	h1 { font-size: 1.1rem; font-weight: 600; margin: 0; }
	.pill {
		font-size: 0.8rem;
		border: 1px solid var(--border);
		border-radius: 999px;
		padding: 0.1rem 0.6rem;
		color: var(--fg-muted);
	}
	.pill.ok { color: var(--ok); border-color: var(--ok); }
	.pill.bad { color: var(--danger); border-color: var(--danger); }
	.tabs { display: flex; gap: 0.25rem; flex: 1; }
	button {
		background: var(--chip-bg);
		color: var(--fg);
		border: 1px solid var(--border);
		border-radius: 4px;
		padding: 0.15rem 0.6rem;
		font: inherit;
		cursor: pointer;
	}
	button:hover:not(:disabled) { border-color: var(--fg-muted); }
	button:disabled { opacity: 0.4; cursor: default; }
	button.active { border-color: var(--accent); color: var(--accent); }
	button.danger { color: var(--danger); }
	button.danger:hover:not(:disabled) { border-color: var(--danger); }
	button.link {
		background: none; border: none; padding: 0;
		color: var(--accent); cursor: pointer;
	}
	.keys {
		display: flex;
		gap: 1rem;
		flex-wrap: wrap;
		margin: 0.75rem 0;
	}
	.keys label {
		display: flex;
		gap: 0.5rem;
		align-items: center;
		color: var(--fg-muted);
		flex: 1;
		min-width: 16rem;
	}
	.badge {
		font-style: normal;
		font-size: 0.75rem;
		color: var(--fg-muted);
	}
	.badge.set { color: var(--ok); }
	input, textarea {
		background: var(--bg-raised);
		color: var(--fg);
		border: 1px solid var(--border);
		border-radius: 4px;
		padding: 0.25rem 0.5rem;
		font: inherit;
		flex: 1;
	}
	.bar { display: flex; gap: 0.4rem; align-items: center; margin-bottom: 0.5rem; }
	table { width: 100%; border-collapse: collapse; }
	th, td {
		text-align: left;
		padding: 0.4rem 0.6rem;
		border-bottom: 1px solid var(--border-soft);
		white-space: nowrap;
	}
	th { color: var(--fg-muted); font-weight: 500; }
	tr.selected td { background: var(--bg-raised); }
	.key { color: var(--accent); word-break: break-all; white-space: normal; }
	.state[data-state='done'], .state-ok { color: var(--ok); }
	.state[data-state='failed'], .state-bad { color: var(--danger); }
	.state[data-state='cancelled'] { color: var(--fg-muted); }
	.state[data-state='running'], .state[data-state='reconciling'] { color: var(--warn); }
	.state[data-state='queued'], .state[data-state='retry_waiting'] { color: var(--queued); }
	.error { color: var(--danger); }
	.notice { color: var(--ok); }
	.hint, .empty { color: var(--fg-muted); }
	.pager { display: flex; justify-content: space-between; align-items: center; margin-top: 1rem; }
	.panel {
		background: var(--bg-raised);
		border: 1px solid var(--border-soft);
		border-radius: 6px;
		padding: 0.9rem 1rem;
		margin-top: 1rem;
	}
	h2 { font-size: 1rem; font-weight: 600; margin: 0 0 0.6rem; display: flex; gap: 0.6rem; align-items: center; }
	h3 { font-size: 0.9rem; color: var(--fg-muted); margin: 0.8rem 0 0.3rem; }
	dl { display: grid; grid-template-columns: 11rem 1fr; gap: 0.2rem 1rem; margin: 0 0 0.6rem; }
	dt { color: var(--fg-muted); }
	dd { margin: 0; }
	.chip {
		display: inline-block;
		background: var(--chip-bg);
		border: 1px solid var(--border-soft);
		border-radius: 999px;
		padding: 0.05rem 0.55rem;
		margin-right: 0.3rem;
	}
	.step-row { display: flex; gap: 0.5rem; margin-bottom: 0.4rem; }
	fieldset { border: 1px solid var(--border-soft); border-radius: 6px; margin: 0 0 0.8rem; }
	legend { color: var(--fg-muted); }
	form label { display: block; margin-bottom: 0.8rem; color: var(--fg-muted); }
	form label input, form label textarea { display: block; margin-top: 0.3rem; width: 100%; box-sizing: border-box; }
	.confirm { display: block; margin-top: 0.5rem; color: var(--warn); }
	.notice-inline { color: var(--ok); margin-left: 0.6rem; }
	@media (max-width: 600px) {
		dl { grid-template-columns: 8rem 1fr; }
		/* Stacked rows: id + key (ellipsized) + action on one line,
		   state/steps/finished below — nothing escapes the viewport. */
		.runlist thead { display: none; }
		.runlist, .runlist tbody { display: block; }
		.runlist tr {
			display: grid;
			grid-template-columns: auto minmax(0, 1fr) auto;
			column-gap: 0.7rem;
			padding: 0.45rem 0;
			align-items: baseline;
		}
		.runlist td { display: block; padding: 0.15rem 0; }
		.runlist td:nth-child(1) { grid-area: 1 / 1; }
		.runlist td:nth-child(2) {
			grid-area: 1 / 2;
			min-width: 0;
			overflow: hidden;
			text-overflow: ellipsis;
			white-space: nowrap;
			word-break: normal;
		}
		.runlist td:nth-child(6) { grid-area: 1 / 3; }
		.runlist td:nth-child(3) { grid-area: 2 / 1; }
		.runlist td:nth-child(4) { grid-area: 2 / 2; }
		.runlist td:nth-child(5) { grid-area: 2 / 3; }
	}
</style>
