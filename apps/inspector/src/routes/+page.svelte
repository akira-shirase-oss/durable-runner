<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import {
		listRuns, inspect, spec as fetchSpec, submitJob, cancelRun,
		diagnostics, compact, backup, health, ready,
		loadKey, saveKey, forgetKeys,
		type RunItem, type Spec, type Diag,
	} from '../lib/api';
	import { idChWidth } from '../lib/idch';
	import { stateView, share } from '../lib/state';
	import Icon from '../lib/Icon.svelte';
	import AppIcon from '../lib/AppIcon.svelte';
	import '../lib/styles/tokens.css';
	import '../lib/styles/base.css';

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
	// The phone layout's id column: widest id on this page, so keys stay
	// aligned and ids never clip (capture.mjs asserts it).
	let idCh = $derived(idChWidth(runs.map((r) => r.id)));
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

<header class="top">
	<div class="bar">
		<h1 class="brand">
			<AppIcon size={32} />
			<span class="name">Durable Runner</span>
			<span class="sub">Inspector</span>
		</h1>
		<span class="pill" class:ok={alive === true && readyNow?.ready} class:bad={alive === false || readyNow?.ready === false}>
			<i class="dot" aria-hidden="true"></i>
			{#if alive === null}…
			{:else if alive === false}Server unreachable
			{:else if readyNow === null}Up
			{:else if readyNow.ready}Ready
			{:else}Not ready: {readyNow.code ?? 'starting'}{/if}
		</span>
		<span class="break" aria-hidden="true"></span>
		<nav class="tabs" aria-label="Inspector sections">
			<button class:active={tab === 'runs'} aria-current={tab === 'runs' ? 'page' : undefined} onclick={() => (tab = 'runs')}>
				<Icon name="activity" size={15} />Runs
			</button>
			<button class:active={tab === 'submit'} aria-current={tab === 'submit' ? 'page' : undefined} onclick={() => (tab = 'submit')}>
				<Icon name="file-plus" size={15} />Submit
			</button>
			<button class:active={tab === 'admin'} aria-current={tab === 'admin' ? 'page' : undefined} onclick={() => { tab = 'admin'; refreshDiag(); }}>
				<Icon name="gauge" size={15} />Admin
			</button>
		</nav>
		<div class="tools">
			<button class="btn ghost icon" title={theme === 'dark' ? 'Switch to light' : 'Switch to dark'} aria-label="Dark theme" aria-pressed={theme === 'dark'}
				onclick={() => applyTheme(theme === 'dark' ? 'light' : 'dark')}>
				<Icon name={theme === 'dark' ? 'sun' : 'moon'} size={17} />
			</button>
			<button class="btn danger forget" onclick={forget}><Icon name="log-out" size={15} />Forget keys</button>
		</div>
	</div>
</header>

<main>
	<section class="card keys" aria-label="Keys">
		<label class="field">
			<span class="field-label"><Icon name="key" size={14} />Read key <em class="badge" class:set={!!readKey}>{keyBadge('read')}</em></span>
			<input type="password" autocomplete="off" placeholder="Lists and run detail"
				bind:value={readKey}
				onchange={() => { saveKey('read', readKey); refresh(); }} />
		</label>
		<label class="field">
			<span class="field-label"><Icon name="key" size={14} />API key <em class="badge" class:set={!!apiKey}>{keyBadge('api')}</em></span>
			<input type="password" autocomplete="off" placeholder="Submit, cancel, spec, admin"
				bind:value={apiKey}
				onchange={() => saveKey('api', apiKey)} />
		</label>
	</section>

	{#if error}<p class="banner error"><Icon name="alert" size={16} /><span class="msg">{error}</span></p>{/if}
	{#if notice}<p class="banner notice"><Icon name="check" size={16} /><span class="msg">{notice}</span> <button class="btn ghost sm" onclick={() => (notice = null)}>Dismiss</button></p>{/if}

	{#if tab === 'runs'}
		{#if !readKey && !apiKey}<p class="banner hint"><Icon name="key" size={16} /><span class="msg">Reads need a key — enter the read key (or the API key) above</span></p>{/if}
		<div class="runs-layout" class:split={!!(detail || detailErr)}>
			<section class="card runs" aria-labelledby="runs-title">
				<div class="card-head">
					<h2 id="runs-title">Runs</h2>
					<div class="order">
						<span class="hint" id="order-label">Order</span>
						<div class="seg" role="group" aria-labelledby="order-label">
							<button class:active={order === 'desc'} aria-pressed={order === 'desc'} onclick={() => setOrder('desc')}>Newest first</button>
							<button class:active={order === 'asc'} aria-pressed={order === 'asc'} onclick={() => setOrder('asc')}>Oldest first</button>
						</div>
					</div>
				</div>
				{#if runs.length === 0 && !error}
					<p class="empty">No runs yet</p>
				{/if}
				{#if runs.length > 0}
					<table class="runlist" style:--id-ch="{idCh}ch">
						<thead><tr><th>ID</th><th>Key</th><th>State</th><th>Steps</th><th>Finished</th><th><span class="sr">Action</span></th></tr></thead>
						<tbody>
							{#each runs as r (r.id)}
								{@const sv = stateView(r.state)}
								<tr class:selected={detail?.id === r.id}>
									<td class="id"><button class="link" onclick={() => openDetail(r.id)}>{r.id}</button></td>
									<td class="key">{r.key}</td>
									<td class="state" data-state={sv.kind}>
										<span class="chip" data-state={sv.kind}><i class="dot" aria-hidden="true"></i>{sv.label}</span>
										{#if sv.detail}<span class="detail">{sv.detail}</span>{/if}
									</td>
									<td class="num">{r.steps}</td>
									<td class="time">{r.finished_at ? new Date(r.finished_at).toLocaleTimeString() : '—'}</td>
									<td class="act">{#if cancellable(r.state)}<button class="btn danger sm" onclick={() => cancel(r.id)}>Cancel</button>{/if}</td>
									<td class="meta">
										<span class="chip" data-state={sv.kind}><i class="dot" aria-hidden="true"></i>{sv.label}</span>
										{#if sv.detail}{`${sv.detail} · `}{/if}{r.steps} {r.steps === 1 ? 'step' : 'steps'} ·
										{r.finished_at ? new Date(r.finished_at).toLocaleTimeString() : '—'}
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
					<nav class="pager" aria-label="Pages">
						<button class="btn sm" onclick={pageBack} disabled={trail.length === 0}><Icon name="chevron-left" />Back</button>
						<span class="hint">Page {trail.length + 1}, {order === 'desc' ? 'newest' : 'oldest'} first</span>
						<button class="btn sm" onclick={pageForward} disabled={nextCursor === null}>Forward<Icon name="chevron-right" /></button>
					</nav>
				{/if}
			</section>
			{#if detailErr || detail}
				<div class="side">
					{#if detailErr}<p class="banner error"><Icon name="alert" size={16} /><span class="msg">{detailErr}</span></p>{/if}
					{#if detail}
						{@const dv = stateView(detail.state)}
						<section class="card panel run" aria-labelledby="detail-title">
							<div class="card-head">
								<h2 id="detail-title">Run <span class="mono">{detail.id}</span></h2>
								<button class="btn ghost sm" onclick={() => { detail = null; detailSpec = null; }}><Icon name="x" />Close</button>
							</div>
							<dl class="kv">
								<div><dt>Key</dt><dd class="key">{detail.key}</dd></div>
								<div>
									<dt>State</dt>
									<dd class="state" data-state={dv.kind}>
										<span class="chip" data-state={dv.kind}><i class="dot" aria-hidden="true"></i>{dv.label}</span>
										{#if dv.detail}<span class="detail">{dv.detail}</span>{/if}
									</dd>
								</div>
								<div><dt>Steps</dt><dd>{detail.steps}</dd></div>
								<div><dt>Finished</dt><dd>{detail.finished_at ? new Date(detail.finished_at).toLocaleString() : '—'}</dd></div>
							</dl>
							<div class="actions">
								{#if cancellable(detail.state)}<button class="btn danger" onclick={() => cancel(detail!.id)}><Icon name="x" />Cancel this run</button>{/if}
								{#if !detailSpec}
									<button class="btn" disabled={!apiKey} title={apiKey ? '' : 'the spec may carry customer input (D-051) — the API key reveals it'} onclick={revealSpec}>
										<Icon name="eye" />Reveal spec{apiKey ? '' : ' — needs the API key'}
									</button>
								{/if}
							</div>
							{#if detailSpec}
								<h3>Spec</h3>
								<dl class="kv">
									<div><dt>Version</dt><dd>{detailSpec.version}</dd></div>
									<div>
										<dt>Steps</dt>
										<dd>
											<ol class="step-list">
												{#each detailSpec.steps as s, i}
													<li><span class="n">{i + 1}</span><span class="mono">{s.name}</span><span class="arrow">→</span><span class="mono handler">{s.handler}</span></li>
												{/each}
											</ol>
										</dd>
									</div>
									<div><dt>Input</dt><dd class="key">{detailSpec.input_b64 || '—'}</dd></div>
								</dl>
							{/if}
						</section>
					{/if}
				</div>
			{/if}
		</div>
	{:else if tab === 'submit'}
		{#if !apiKey}<p class="banner hint"><Icon name="key" size={16} /><span class="msg">Submitting needs the API key — enter it above</span></p>{/if}
		<form class="card panel submit" onsubmit={(e) => { e.preventDefault(); submit(); }}>
			<div class="card-head">
				<h2>Submit a job</h2>
				<span class="tag">spec v1</span>
			</div>
			<label class="field">
				<span class="field-label">Operation key (idempotency)</span>
				<input class="mono" required maxlength="256" bind:value={opKey} placeholder="deploy-2026-11-18" />
			</label>
			<fieldset>
				<legend class="field-label">Steps</legend>
				<div class="step-rows">
					{#each steps as s, i}
						<div class="step-row">
							<span class="n" aria-hidden="true">{i + 1}</span>
							<input class="mono" required placeholder="name" aria-label="Step {i + 1} name" bind:value={s.name} />
							<span class="arrow" aria-hidden="true">→</span>
							<input class="mono" required placeholder="handler" aria-label="Step {i + 1} handler" bind:value={s.handler} />
							<button type="button" class="btn ghost icon remove" title="Remove step {i + 1}" disabled={steps.length === 1}
								onclick={() => (steps = steps.filter((_, j) => j !== i))}><Icon name="trash" /><span class="sr">Remove step {i + 1}</span></button>
						</div>
					{/each}
				</div>
				<button type="button" class="btn sm add" onclick={() => (steps = [...steps, { name: '', handler: '' }])}><Icon name="plus" />Add step</button>
			</fieldset>
			<label class="field">
				<span class="field-label">Input (sent base64-encoded)</span>
				<textarea class="mono" rows="3" bind:value={inputText} placeholder="job input bytes"></textarea>
			</label>
			<label class="field narrow">
				<span class="field-label">Not before (optional)</span>
				<input type="datetime-local" bind:value={notBefore} />
			</label>
			<div class="form-actions">
				<button type="submit" class="btn brand" disabled={!apiKey || !opKey || steps.some((s) => !s.name || !s.handler)}><Icon name="arrow-up-right" />Submit</button>
				{#if lastRun}<span class="notice-inline"><Icon name="check" size={15} />Last accepted: run {lastRun}</span>{/if}
			</div>
		</form>
	{:else}
		{#if !apiKey}<p class="banner hint"><Icon name="key" size={16} /><span class="msg">Admin operations need the API key — enter it above</span></p>{/if}
		<section class="panel diag" aria-labelledby="diag-title">
			<div class="section-head">
				<h2 id="diag-title">Diagnostics</h2>
				<button class="btn sm" onclick={refreshDiag} disabled={!apiKey}><Icon name="rotate" />Refresh</button>
			</div>
			{#if diag}
				<dl class="tiles">
					<div class="tile"><dt>Journal generation</dt><dd>{diag.generation}</dd></div>
					<div class="tile"><dt>Records written</dt><dd>{diag.records_written.toLocaleString()}</dd></div>
					<div class="tile"><dt>Live runs</dt><dd>{diag.live_runs.toLocaleString()}</dd></div>
					<div class="tile"><dt>Active runs</dt><dd>{diag.active_runs.toLocaleString()}</dd></div>
					<div class="tile"><dt>Index bytes</dt><dd>{diag.index_bytes.toLocaleString()}</dd></div>
					<div class="tile"><dt>Dedup keys</dt><dd>{diag.dedup_keys.toLocaleString()}</dd></div>
					<div class="tile"><dt>Disk reserve</dt><dd class={diag.space_ok ? 'state-ok' : 'state-bad'}><Icon name="hard-drive" size={16} />{diag.space_ok ? 'ok' : 'below reserve'}</dd></div>
				</dl>
				{#if Object.keys(diag.states).length || Object.keys(diag.errors).length}
					<div class="counts">
						{#if Object.keys(diag.states).length}
							{@const top = Math.max(...Object.values(diag.states))}
							<div class="card">
								<h3>States</h3>
								<dl class="bars">
									{#each Object.entries(diag.states) as [s, n]}
										<div><dt class="mono">{s}</dt><dd><span class="track"><i data-state={s} style:width="{share(n, top)}%"></i></span><span class="count">{n.toLocaleString()}</span></dd></div>
									{/each}
								</dl>
							</div>
						{/if}
						{#if Object.keys(diag.errors).length}
							{@const top = Math.max(...Object.values(diag.errors))}
							<div class="card">
								<h3>Errors (counted, never dropped)</h3>
								<dl class="bars errors">
									{#each Object.entries(diag.errors) as [s, n]}
										<div><dt class="mono">{s}</dt><dd><span class="track"><i style:width="{share(n, top)}%"></i></span><span class="count">{n.toLocaleString()}</span></dd></div>
									{/each}
								</dl>
							</div>
						{/if}
					</div>
				{/if}
			{:else}
				<p class="card empty">No diagnostics yet — refresh above</p>
			{/if}
		</section>
		<div class="admin-actions">
			<section class="card panel" aria-labelledby="compact-title">
				<h2 id="compact-title"><Icon name="layers" size={17} />Compact the journal</h2>
				<p class="hint">Expires finished runs past retention, snapshots the index, swaps the manifest — all or nothing</p>
				{#if confirming === 'compact'}
					<div class="confirm">
						<p>Run compaction now?</p>
						<div class="confirm-row">
							<button class="btn brand sm" onclick={doCompact}>Yes, compact</button>
							<button class="btn" onclick={() => (confirming = null)}>No</button>
						</div>
					</div>
				{:else}
					<button class="btn" disabled={!apiKey} onclick={() => (confirming = 'compact')}><Icon name="layers" />Compact</button>
				{/if}
			</section>
			<section class="card panel" aria-labelledby="backup-title">
				<h2 id="backup-title"><Icon name="backup" size={17} />Back up the journal</h2>
				<p class="hint">Copies the journal into a named directory under the server's --backup-root (needs the flag set)</p>
				<label class="field">
					<span class="field-label">Backup name</span>
					<input class="mono" maxlength="64" pattern="[A-Za-z0-9_.\-]+" bind:value={backupName} />
				</label>
				{#if confirming === 'backup'}
					<div class="confirm">
						<p>Write backup '<span class="mono">{backupName}</span>'?</p>
						<div class="confirm-row">
							<button class="btn brand sm" onclick={doBackup}>Yes, back up</button>
							<button class="btn" onclick={() => (confirming = null)}>No</button>
						</div>
					</div>
				{:else}
					<button class="btn" disabled={!apiKey || !backupName} onclick={() => (confirming = 'backup')}><Icon name="backup" />Backup</button>
				{/if}
			</section>
		</div>
	{/if}
</main>

<style>
	/* ---- layout ------------------------------------------------------------------------------------ */
	.top,
	main {
		width: 100%;
		max-width: 1232px;
		margin: 0 auto;
		padding: 0 16px;
	}
	.top {
		position: sticky;
		top: 12px;
		z-index: 10;
		margin-top: 12px;
	}
	main {
		display: flex;
		flex-direction: column;
		gap: 16px;
		padding-top: 20px;
		padding-bottom: 48px;
	}

	/* ---- the floating bar --------------------------------------------------------------------------- */
	.bar {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 10px 12px;
		min-height: 60px;
		padding: 10px 10px 10px 16px;
		border-radius: 20px;
		background: var(--c-card);
		box-shadow:
			0 0 0 1px var(--c-bar-edge),
			0 10px 30px -14px var(--c-shadow);
	}
	.brand {
		display: flex;
		align-items: center;
		gap: 9px;
		margin: 0;
		font-size: 19px;
		font-weight: 560;
		line-height: 1;
		letter-spacing: -0.018em;
		white-space: nowrap;
	}
	.brand .sub {
		padding-left: 2px;
		color: var(--c-body);
		font-weight: 440;
		letter-spacing: -0.01em;
	}
	.break {
		display: none;
	}
	.tabs {
		display: inline-flex;
		gap: 2px;
		margin-left: auto;
		padding: 4px;
		border-radius: 13px;
		background: var(--c-app);
		box-shadow: inset 0 0 0 1px var(--c-line);
	}
	.tabs button,
	.seg button {
		display: inline-flex;
		align-items: center;
		justify-content: center;
		gap: 7px;
		height: 34px;
		padding: 0 14px;
		border: 0;
		border-radius: 9px;
		background: transparent;
		color: var(--c-body);
		font: inherit;
		font-size: 14px;
		font-weight: 520;
		letter-spacing: -0.005em;
		white-space: nowrap;
		cursor: pointer;
		transition:
			background-color 150ms ease-out,
			color 150ms ease-out;
	}
	.tabs button:hover,
	.seg button:hover {
		color: var(--c-ink);
	}
	.tabs button.active,
	.seg button.active {
		background: var(--c-tile);
		color: var(--c-ink);
		box-shadow:
			0 0 0 1px var(--c-bar-edge),
			0 1px 3px var(--c-tile-shadow);
	}
	.tabs button.active :global(svg) {
		color: var(--c-brand-deep);
	}
	.tools {
		display: flex;
		align-items: center;
		gap: 6px;
	}
	.forget {
		height: 40px;
		border-radius: 12px;
	}

	/* status: ready / up / unreachable */
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
		font-weight: 520;
		white-space: nowrap;
	}
	.pill .dot {
		width: 7px;
		height: 7px;
		border-radius: 50%;
		background: var(--c-state-queued);
	}
	.pill.ok {
		background: var(--c-state-done-bg);
		color: var(--c-state-done-ink);
	}
	.pill.ok .dot {
		background: var(--c-state-done);
	}
	.pill.bad {
		background: var(--c-danger-bg);
		color: var(--c-danger);
	}
	.pill.bad .dot {
		background: var(--c-danger);
	}

	/* ---- cards, headings, banners ------------------------------------------------------------------ */
	.card {
		border-radius: 18px;
		background: var(--c-card);
		box-shadow:
			0 0 0 1px var(--c-bar-edge),
			0 1px 2px var(--c-tile-shadow);
	}
	.card-head,
	.section-head {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		justify-content: space-between;
		gap: 10px 16px;
	}
	h2 {
		display: flex;
		align-items: center;
		gap: 8px;
		margin: 0;
		font-size: 19px;
		font-weight: 540;
		letter-spacing: -0.015em;
	}
	h2 :global(svg) {
		color: var(--c-ink-soft);
	}
	h3 {
		margin: 0 0 10px;
		font-size: 14px;
		font-weight: 600;
		letter-spacing: -0.005em;
	}
	.hint {
		color: var(--c-body);
		font-size: 14px;
	}
	.empty {
		margin: 0;
		padding: 40px 20px;
		color: var(--c-body);
		text-align: center;
	}
	.tag {
		padding: 4px 10px;
		border-radius: 10px;
		background: var(--c-app);
		box-shadow: inset 0 0 0 1px var(--c-line);
		color: var(--c-ink-soft);
		font-size: 12.5px;
		font-weight: 520;
	}
	.sr {
		position: absolute;
		width: 1px;
		height: 1px;
		overflow: hidden;
		clip-path: inset(50%);
		white-space: nowrap;
	}

	.banner {
		display: flex;
		align-items: center;
		gap: 10px;
		margin: 0;
		padding: 11px 14px;
		border-radius: 14px;
		font-size: 14.5px;
		font-weight: 480;
	}
	.banner :global(svg) {
		flex: none;
	}
	.banner .msg {
		flex: 1;
		min-width: 0;
		overflow-wrap: anywhere;
	}
	.banner .msg::first-letter {
		text-transform: uppercase;
	}
	.banner.error {
		background: var(--c-danger-bg);
		color: var(--c-danger);
	}
	.banner.notice {
		padding-top: 6px;
		padding-bottom: 6px;
		background: var(--c-state-done-bg);
		color: var(--c-state-done-ink);
	}
	.banner.notice .btn {
		color: inherit;
	}
	.banner.hint {
		background: var(--c-card);
		box-shadow: inset 0 0 0 1px var(--c-line);
		color: var(--c-body);
	}
	.banner.hint :global(svg) {
		color: var(--c-brand-deep);
	}

	/* ---- keys -------------------------------------------------------------------------------------- */
	.keys {
		display: grid;
		grid-template-columns: 1fr 1fr;
		gap: 12px 16px;
		padding: 14px 16px 16px;
	}
	.field {
		display: flex;
		flex-direction: column;
		gap: 7px;
		min-width: 0;
	}
	.field-label {
		display: flex;
		align-items: center;
		gap: 7px;
		padding: 0;
		color: var(--c-ink-soft);
		font-size: 13.5px;
		font-weight: 520;
	}
	.field-label :global(svg) {
		color: var(--c-body);
	}
	.badge {
		margin-left: 2px;
		padding: 2px 8px;
		border-radius: 999px;
		background: var(--c-state-queued-bg);
		color: var(--c-state-queued-ink);
		font-size: 12px;
		font-style: normal;
		font-weight: 520;
	}
	.badge.set {
		background: var(--c-state-done-bg);
		color: var(--c-state-done-ink);
	}

	/* ---- runs -------------------------------------------------------------------------------------- */
	.runs-layout {
		display: flex;
		flex-direction: column;
		gap: 16px;
	}
	.runs {
		min-width: 0;
		overflow-x: auto;
	}
	.runs .card-head {
		padding: 16px 18px 14px;
	}
	.order {
		display: flex;
		align-items: center;
		gap: 10px;
	}
	.seg {
		display: inline-flex;
		gap: 2px;
		padding: 3px;
		border-radius: 12px;
		background: var(--c-app);
		box-shadow: inset 0 0 0 1px var(--c-line);
	}
	.seg button {
		height: 30px;
		padding: 0 12px;
		font-size: 13.5px;
	}

	table {
		width: 100%;
		border-collapse: collapse;
		font-size: 14px;
	}
	th,
	td {
		padding: 11px 12px;
		border-top: 1px solid var(--c-line);
		text-align: left;
		white-space: nowrap;
		vertical-align: middle;
	}
	th {
		padding-top: 9px;
		padding-bottom: 9px;
		background: var(--c-panel);
		color: var(--c-body);
		font-size: 12.5px;
		font-weight: 520;
	}
	th:first-child,
	td:first-child {
		padding-left: 18px;
	}
	th:nth-child(6),
	td.act {
		padding-right: 18px;
		text-align: right;
	}
	tbody tr {
		transition: background-color 150ms ease-out;
	}
	tbody tr:hover {
		background: var(--c-row-hover);
	}
	tbody tr.selected {
		background: var(--c-row-selected);
	}
	td.id {
		width: 1%;
		font-family: var(--font-mono);
		font-size: 14px;
		font-weight: 600;
	}
	td.id .link {
		font-variant-numeric: tabular-nums;
	}
	.key {
		color: var(--c-ink-soft);
		font-family: var(--font-mono);
		font-size: 13.5px;
		white-space: normal;
		overflow-wrap: anywhere;
	}
	td.state {
		white-space: normal;
		line-height: 1.9;
	}
	td.num,
	td.time {
		color: var(--c-ink-soft);
		font-variant-numeric: tabular-nums;
	}
	td.act {
		width: 1%;
		padding-top: 7px;
		padding-bottom: 7px;
	}
	td.meta {
		display: none;
	}

	/* state chips: the kind picks the colour (queued, running, retry_waiting, reconciling, done, failed,
	   cancelled); an unknown kind looks queued */
	.chip {
		display: inline-flex;
		align-items: center;
		gap: 6px;
		height: 24px;
		padding: 0 9px;
		border-radius: 8px;
		background: var(--c-state-queued-bg);
		color: var(--c-state-queued-ink);
		font-family: var(--font-sans);
		font-size: 12.5px;
		font-weight: 560;
		white-space: nowrap;
		vertical-align: middle;
	}
	.chip .dot {
		width: 6px;
		height: 6px;
		border-radius: 50%;
		background: var(--c-state-queued);
	}
	.chip[data-state='running'] {
		background: var(--c-state-run-bg);
		color: var(--c-state-run-ink);
	}
	.chip[data-state='running'] .dot {
		background: var(--c-state-run);
	}
	.chip[data-state='retry_waiting'] {
		background: var(--c-state-retry-bg);
		color: var(--c-state-retry-ink);
	}
	.chip[data-state='retry_waiting'] .dot {
		background: var(--c-state-retry);
	}
	.chip[data-state='reconciling'] {
		background: var(--c-state-probe-bg);
		color: var(--c-state-probe-ink);
	}
	.chip[data-state='reconciling'] .dot {
		background: var(--c-state-probe);
	}
	.chip[data-state='done'] {
		background: var(--c-state-done-bg);
		color: var(--c-state-done-ink);
	}
	.chip[data-state='done'] .dot {
		background: var(--c-state-done);
	}
	.chip[data-state='failed'] {
		background: var(--c-danger-bg);
		color: var(--c-danger);
	}
	.chip[data-state='failed'] .dot {
		background: var(--c-danger);
	}
	.chip[data-state='cancelled'] {
		background: transparent;
		box-shadow: inset 0 0 0 1px var(--c-line-strong);
		color: var(--c-state-cancelled);
	}
	.chip[data-state='cancelled'] .dot {
		background: transparent;
		box-shadow: inset 0 0 0 1.5px var(--c-state-cancelled);
	}
	.detail {
		display: inline-block;
		margin-left: 8px;
		color: var(--c-body);
		font-family: var(--font-mono);
		font-size: 12.5px;
		line-height: 1.4;
		white-space: nowrap;
	}

	.pager {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 12px;
		padding: 12px 18px;
		border-top: 1px solid var(--c-line);
	}
	.pager .hint {
		font-size: 13.5px;
	}

	/* the run in focus */
	.side {
		display: flex;
		flex-direction: column;
		gap: 12px;
		min-width: 0;
	}
	.panel.run {
		padding: 16px 18px 18px;
	}
	.kv {
		display: flex;
		flex-direction: column;
		margin: 14px 0 0;
	}
	.kv > div {
		display: grid;
		grid-template-columns: 7.5rem minmax(0, 1fr);
		gap: 12px;
		align-items: baseline;
		padding: 10px 0;
		border-top: 1px solid var(--c-line);
	}
	.kv dt {
		color: var(--c-body);
		font-size: 13.5px;
	}
	.kv dd {
		min-width: 0;
		margin: 0;
		font-size: 14px;
	}
	.kv dd.state {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 6px 8px;
	}
	.kv dd.state .detail {
		margin-left: 0;
	}
	.actions {
		display: flex;
		flex-wrap: wrap;
		gap: 8px;
		padding-top: 14px;
		border-top: 1px solid var(--c-line);
	}
	.panel.run h3 {
		margin: 18px 0 0;
	}
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
		min-height: 34px;
		padding: 6px 11px;
		overflow-wrap: anywhere;
	}
	.step-list li + li {
		border-top: 1px solid var(--c-line);
	}
	.n {
		display: inline-grid;
		flex: none;
		place-items: center;
		width: 20px;
		height: 20px;
		border-radius: 6px;
		background: var(--c-state-run-bg);
		color: var(--c-state-run-ink);
		font-size: 11.5px;
		font-weight: 600;
	}
	.arrow {
		flex: none;
		color: var(--c-body);
	}
	.handler {
		color: var(--c-body);
	}

	/* ---- submit ------------------------------------------------------------------------------------ */
	.submit {
		display: flex;
		flex-direction: column;
		gap: 18px;
		width: 100%;
		max-width: 760px;
		padding: 18px 20px 20px;
	}
	fieldset {
		display: flex;
		flex-direction: column;
		align-items: flex-start;
		gap: 10px;
		min-width: 0;
		margin: 0;
		padding: 0;
		border: 0;
	}
	legend {
		margin-bottom: 7px;
	}
	.step-rows {
		display: flex;
		flex-direction: column;
		gap: 8px;
		width: 100%;
	}
	.step-row {
		display: grid;
		grid-template-columns: 20px minmax(0, 1fr) auto minmax(0, 1fr) 40px;
		gap: 10px;
		align-items: center;
	}
	.remove {
		color: var(--c-body);
	}
	.field.narrow {
		max-width: 300px;
	}
	.form-actions {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 10px 16px;
		padding-top: 4px;
	}
	.notice-inline {
		display: inline-flex;
		align-items: center;
		gap: 7px;
		color: var(--c-state-done-ink);
		font-size: 14px;
		font-weight: 520;
	}

	/* ---- admin ------------------------------------------------------------------------------------- */
	.diag {
		display: flex;
		flex-direction: column;
		gap: 12px;
	}
	.section-head {
		padding: 2px 2px 0;
	}
	.tiles {
		display: grid;
		grid-template-columns: repeat(auto-fill, minmax(150px, 1fr));
		gap: 10px;
		margin: 0;
	}
	.tile {
		padding: 13px 15px 14px;
		border-radius: 16px;
		background: var(--c-card);
		box-shadow:
			0 0 0 1px var(--c-bar-edge),
			0 1px 2px var(--c-tile-shadow);
	}
	.tile dt {
		color: var(--c-body);
		font-size: 13px;
	}
	.tile dd {
		display: flex;
		align-items: center;
		gap: 6px;
		margin: 4px 0 0;
		font-size: 22px;
		font-weight: 520;
		letter-spacing: -0.015em;
		font-variant-numeric: tabular-nums;
	}
	.tile dd.state-ok {
		color: var(--c-state-done-ink);
	}
	.tile dd.state-bad {
		color: var(--c-danger);
	}
	.counts {
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
		align-items: start;
		gap: 12px;
	}
	.counts .card {
		padding: 16px 18px;
	}
	.bars {
		display: flex;
		flex-direction: column;
		gap: 4px;
		margin: 0;
	}
	.bars > div {
		display: grid;
		grid-template-columns: minmax(7rem, 11rem) minmax(0, 1fr);
		gap: 12px;
		align-items: center;
		min-height: 28px;
	}
	.bars dt {
		overflow: hidden;
		color: var(--c-ink-soft);
		font-size: 13px;
		text-overflow: ellipsis;
		white-space: nowrap;
	}
	.bars dd {
		display: grid;
		grid-template-columns: minmax(0, 1fr) auto;
		gap: 12px;
		align-items: center;
		margin: 0;
	}
	.track {
		height: 6px;
		overflow: hidden;
		border-radius: 3px;
		background: var(--c-line);
	}
	.track i {
		display: block;
		height: 100%;
		border-radius: 3px;
		background: var(--c-state-queued);
	}
	.track i[data-state='running'] {
		background: var(--c-state-run);
	}
	.track i[data-state='retry_waiting'] {
		background: var(--c-state-retry);
	}
	.track i[data-state='reconciling'] {
		background: var(--c-state-probe);
	}
	.track i[data-state='done'] {
		background: var(--c-state-done);
	}
	.track i[data-state='failed'],
	.errors .track i {
		background: var(--c-danger);
	}
	.track i[data-state='cancelled'] {
		background: var(--c-line-strong);
	}
	.count {
		min-width: 3.5rem;
		color: var(--c-ink-soft);
		font-size: 13px;
		font-variant-numeric: tabular-nums;
		text-align: right;
	}
	.diag > .empty {
		padding: 32px 20px;
	}

	.admin-actions {
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
		gap: 16px;
		align-items: start;
	}
	.admin-actions .panel {
		display: flex;
		flex-direction: column;
		align-items: flex-start;
		gap: 12px;
		padding: 18px 20px 20px;
	}
	.admin-actions h2 {
		font-size: 17px;
	}
	.admin-actions .hint {
		margin: 0;
		line-height: 1.5;
	}
	.admin-actions .field {
		width: 100%;
	}
	.confirm {
		display: flex;
		flex-direction: column;
		gap: 10px;
		width: 100%;
		padding: 12px 14px 14px;
		border-radius: 14px;
		background: var(--c-brand-tint);
	}
	.confirm p {
		margin: 0;
		color: var(--c-ink);
		font-size: 14px;
		font-weight: 520;
	}
	.confirm-row {
		display: flex;
		flex-wrap: wrap;
		gap: 8px;
	}

	/* ---- wide: the run in focus sits beside the list ---------------------------------------------- */
	@media (min-width: 1180px) {
		.runs-layout:not(.split) td.state {
			white-space: nowrap;
		}
		.runs-layout.split {
			display: grid;
			grid-template-columns: minmax(0, 1fr) 400px;
			align-items: start;
			gap: 16px;
		}
		.runs-layout.split .side {
			position: sticky;
			top: 92px;
		}
	}

	/* ---- narrow ------------------------------------------------------------------------------------ */
	/* the bar folds into two rows: name and status, then the tabs with the theme and keys buttons */
	@media (max-width: 900px) {
		.pill {
			margin-left: auto;
		}
		.break {
			display: block;
			flex-basis: 100%;
			height: 0;
		}
		.tabs {
			flex: 1;
			margin-left: 0;
		}
		.tabs button {
			flex: 1;
			padding: 0 8px;
		}
	}

	@media (max-width: 760px) {
		.top {
			position: static;
		}
		.bar {
			padding: 9px 9px 9px 12px;
			border-radius: 17px;
		}
		.brand {
			font-size: 17.5px;
		}
		.keys {
			grid-template-columns: 1fr;
		}
	}

	@media (max-width: 600px) {
		.top {
			top: 8px;
			margin-top: 8px;
		}
		main {
			padding-top: 14px;
			gap: 12px;
		}
		.tabs button :global(svg) {
			display: none;
		}
		.brand {
			gap: 8px;
			font-size: 17px;
		}
		.brand .sub {
			padding-left: 0;
		}
		.pill {
			height: 26px;
			padding: 0 9px;
			font-size: 12.5px;
		}
		/* "Forget keys" stays a visible, labelled button (D-053); on a phone it drops its icon to fit */
		.tools {
			gap: 4px;
		}
		.tools .forget {
			padding: 0 10px;
			font-size: 13.5px;
		}
		.forget :global(svg) {
			display: none;
		}
		.tabs button {
			padding: 0 6px;
			font-size: 13.5px;
		}
		.runs .card-head {
			padding: 14px 14px 12px;
		}
		.kv > div {
			grid-template-columns: 6rem minmax(0, 1fr);
		}
		.step-row {
			grid-template-columns: 20px minmax(0, 1fr) 40px;
			grid-template-areas: 'n name rm' '. handler rm';
			row-gap: 6px;
		}
		.step-row .n {
			grid-area: n;
		}
		.step-row input:first-of-type {
			grid-area: name;
		}
		.step-row input:last-of-type {
			grid-area: handler;
		}
		.step-row .arrow {
			display: none;
		}
		.step-row .remove {
			grid-area: rm;
		}
		.submit,
		.admin-actions .panel {
			padding: 16px;
		}
		.admin-actions {
			grid-template-columns: 1fr;
		}
		.counts {
			grid-template-columns: 1fr;
		}
		.bars > div {
			grid-template-columns: 7.5rem minmax(0, 1fr);
		}

		/* The run list as two explicit rows: id (sized to the widest id on the page, so keys align) + key
		   ellipsized only when truly too long + action; then "state · N steps · finished" under the key.
		   Nothing escapes the viewport (capture.mjs asserts it). Ids and keys share the monospace font, so
		   the id column's ch matches the id's own digits. */
		.runlist thead {
			display: none;
		}
		.runlist,
		.runlist tbody {
			display: block;
		}
		.runlist tr {
			display: grid;
			grid-template-columns: var(--id-ch, 4ch) minmax(0, 1fr) auto;
			column-gap: 10px;
			align-items: baseline;
			padding: 10px 14px;
			border-top: 1px solid var(--c-line);
			font-family: var(--font-mono);
			font-size: 14px;
		}
		.runlist td {
			display: block;
			width: auto;
			padding: 0;
			border: 0;
			line-height: 1.45;
		}
		.runlist td:nth-child(1) {
			grid-area: 1 / 1;
			overflow: hidden;
		}
		.runlist td:nth-child(2) {
			grid-area: 1 / 2;
			min-width: 0;
			overflow: hidden;
			text-overflow: ellipsis;
			white-space: nowrap;
			word-break: normal;
		}
		.runlist td:nth-child(6) {
			grid-column: 3;
			grid-row: 1 / 3;
			align-self: center;
		}
		.runlist td:nth-child(3),
		.runlist td:nth-child(4),
		.runlist td:nth-child(5) {
			display: none;
		}
		.runlist td.meta {
			grid-area: 2 / 2;
			display: block;
			min-width: 0;
			margin-top: 6px;
			overflow: hidden;
			color: var(--c-body);
			font-family: var(--font-sans);
			font-size: 13px;
			text-overflow: ellipsis;
			white-space: nowrap;
		}
		.runlist td.meta .chip {
			height: 20px;
			margin-right: 4px;
			padding: 0 7px;
			font-size: 12px;
		}
		.pager {
			padding: 10px 14px;
		}
	}
</style>
