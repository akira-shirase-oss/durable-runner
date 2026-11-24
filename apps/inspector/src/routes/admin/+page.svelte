<script lang="ts">
	// Admin (API key): the diagnostics counters, runs by state and the error counters, refreshed live, and the two
	// operations that change the journal, compact and backup, each behind an explicit confirm step. Pressing
	// Compact or Backup moves focus to the confirm row's "No" (the safe choice), and answering returns it to the
	// button that asked.
	import { tick } from 'svelte';
	import Icon from '../../lib/Icon.svelte';
	import KpiTile from '../../lib/ui/KpiTile.svelte';
	import Notice from '../../lib/ui/Notice.svelte';
	import StateBars from '../../lib/ui/StateBars.svelte';
	import { ApiError, backup, compact, diagnostics } from '../../lib/api.ts';
	import { fmtBytes, fmtCompact, fmtInt } from '../../lib/format.ts';
	import { poll, Resource } from '../../lib/live.svelte.ts';
	import { session } from '../../lib/session.svelte.ts';
	import { share } from '../../lib/state.ts';

	const diag = new Resource((signal) =>
		session.apiKey ? diagnostics(session.apiKey, signal) : Promise.reject(new ApiError('needs the API key', 401)),
	);
	poll(diag);

	let backupName = $state(`backup-${new Date().toISOString().slice(0, 10)}`);
	let confirming = $state<null | 'compact' | 'backup'>(null);
	let busy = $state(false);
	let notice = $state<string | null>(null);
	let error = $state<string | null>(null);
	let compactBtn: HTMLButtonElement | undefined = $state();
	let backupBtn: HTMLButtonElement | undefined = $state();
	let noCompact: HTMLButtonElement | undefined = $state();
	let noBackup: HTMLButtonElement | undefined = $state();

	const d = $derived(diag.data);
	const errors = $derived(d ? Object.entries(d.errors) : []);
	const errTop = $derived(Math.max(0, ...errors.map(([, n]) => n)));
	const nameOk = $derived(/^[A-Za-z0-9_.-]{1,64}$/.test(backupName));

	async function ask(op: 'compact' | 'backup') {
		confirming = op;
		await tick();
		(op === 'compact' ? noCompact : noBackup)?.focus();
	}

	async function answer(op: 'compact' | 'backup', yes: boolean) {
		confirming = null;
		await tick();
		(op === 'compact' ? compactBtn : backupBtn)?.focus();
		if (!yes) return;
		busy = true;
		try {
			if (op === 'compact') {
				const r = await compact(session.apiKey);
				notice = `Compacted: the journal is now generation ${r.generation}`;
			} else {
				const r = await backup(session.apiKey, backupName);
				notice = `Backup '${r.backup}' written under the server's backup root`;
			}
			error = null;
			void diag.refresh(true);
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
		}
	}
</script>

<div class="page">
	{#if notice}
		<p class="banner notice" role="status"><Icon name="check" size={16} /><span class="msg">{notice}</span><button class="btn ghost sm act" onclick={() => (notice = null)}>Dismiss</button></p>
	{/if}
	{#if error}
		<p class="banner error" role="alert"><Icon name="alert" size={16} /><span class="msg">{error}</span><button class="btn ghost sm act" onclick={() => (error = null)}>Dismiss</button></p>
	{/if}
	<Notice status={diag.status} error={diag.error} what="Diagnostics" route="GET /admin/diagnostics" needsApi stale={diag.updatedAt} />

	<div class="section-head">
		<h2 class="section-title">Diagnostics</h2>
		<button class="btn sm" onclick={() => diag.refresh(true)} disabled={!session.apiKey}><Icon name="rotate" />Refresh</button>
	</div>
	{#if d}
		<dl class="tiles">
			<KpiTile label="Journal generation" value={fmtInt(d.generation)} sub="+1 per compaction" />
			<KpiTile label="Records written" value={fmtCompact(d.records_written)} sub="{fmtInt(d.records_written)} since start" />
			<KpiTile label="Live runs" value={fmtCompact(d.live_runs)} sub="held in the index" />
			<KpiTile label="Active runs" value={fmtCompact(d.active_runs)} sub="not yet terminal" />
			<KpiTile label="Index" value={fmtBytes(d.index_bytes)} sub="{fmtInt(d.index_bytes)} bytes" />
			<KpiTile label="Dedup keys" value={fmtCompact(d.dedup_keys)} sub="op keys remembered" />
			<KpiTile label="Disk reserve" value={d.space_ok ? 'OK' : 'Below reserve'} tone={d.space_ok ? 'ok' : 'bad'} sub={d.space_ok ? 'admission open' : 'new runs refused'}>
				{#snippet icon()}<Icon name="hard-drive" size={18} />{/snippet}
			</KpiTile>
		</dl>
		<div class="grid two counts">
			<section class="card" aria-labelledby="states-title">
				<div class="card-head"><h2 id="states-title">Runs by state</h2></div>
				<div class="card-body"><StateBars states={d.states} /></div>
			</section>
			<section class="card" aria-labelledby="errors-title">
				<div class="card-head">
					<div>
						<h2 id="errors-title">Errors</h2>
						<p class="sub">Counted, never dropped</p>
					</div>
				</div>
				<div class="card-body">
					{#if errors.length === 0}
						<p class="empty">No error counters</p>
					{:else}
						<dl class="bars">
							{#each errors as [k, n] (k)}
								<div>
									<dt class="mono">{k}</dt>
									<dd class="track"><i data-state="error" style:width="{share(n, errTop)}%"></i></dd>
									<dd class="count">{fmtInt(n)}</dd>
								</div>
							{/each}
						</dl>
					{/if}
				</div>
			</section>
		</div>
	{:else if diag.loading && session.apiKey}
		<p class="card empty">Loading diagnostics…</p>
	{/if}

	<div class="section-head">
		<h2 class="section-title">Journal operations</h2>
	</div>
	<div class="grid two ops">
		<section class="card op" aria-labelledby="compact-title">
			<h3 id="compact-title"><Icon name="layers" size={17} />Compact the journal</h3>
			<p class="hint">Expires finished runs past retention, snapshots the index and swaps the manifest, all or nothing.</p>
			{#if confirming === 'compact'}
				<div class="confirm" role="group" aria-labelledby="compact-q">
					<p id="compact-q">Run compaction now?</p>
					<div class="confirm-row">
						<button class="btn brand sm" onclick={() => answer('compact', true)}>Yes, compact</button>
						<button class="btn sm" bind:this={noCompact} onclick={() => answer('compact', false)}>No</button>
					</div>
				</div>
			{:else}
				<button class="btn" bind:this={compactBtn} disabled={!session.apiKey || busy} onclick={() => ask('compact')}><Icon name="layers" />Compact</button>
			{/if}
		</section>
		<section class="card op" aria-labelledby="backup-title">
			<h3 id="backup-title"><Icon name="backup" size={17} />Back up the journal</h3>
			<p class="hint">Copies the journal into a named directory under the server's <span class="mono">--backup-root</span> (the flag must be set).</p>
			<label class="field">
				<span class="field-label">Backup name</span>
				<input class="mono" maxlength="64" bind:value={backupName} aria-invalid={nameOk ? undefined : 'true'} aria-describedby="backup-hint" />
				<span class="field-hint" id="backup-hint">Letters, digits, dot, dash and underscore.</span>
			</label>
			{#if confirming === 'backup'}
				<div class="confirm" role="group" aria-labelledby="backup-q">
					<p id="backup-q">Write backup '<span class="mono">{backupName}</span>'?</p>
					<div class="confirm-row">
						<button class="btn brand sm" onclick={() => answer('backup', true)}>Yes, back up</button>
						<button class="btn sm" bind:this={noBackup} onclick={() => answer('backup', false)}>No</button>
					</div>
				</div>
			{:else}
				<button class="btn" bind:this={backupBtn} disabled={!session.apiKey || !nameOk || busy} onclick={() => ask('backup')}><Icon name="backup" />Backup</button>
			{/if}
		</section>
	</div>
</div>

<style>
	.tiles {
		display: grid;
		grid-template-columns: repeat(auto-fill, minmax(146px, 1fr));
		gap: 12px;
		margin: 0;
	}
	.counts {
		align-items: start;
	}
	.ops {
		align-items: start;
	}
	.op {
		display: flex;
		flex-direction: column;
		align-items: flex-start;
		gap: 12px;
		padding: 18px 20px 20px;
	}
	.op h3 {
		display: flex;
		align-items: center;
		gap: 8px;
		margin: 0;
		font-size: 16px;
		font-weight: 560;
	}
	.op h3 :global(svg) {
		color: var(--c-ink-soft);
	}
	.op .hint {
		margin: 0;
		line-height: 1.5;
	}
	.op .field {
		width: 100%;
	}
	@media (max-width: 600px) {
		.tiles {
			grid-template-columns: repeat(2, minmax(0, 1fr));
		}
		.op {
			padding: 16px;
		}
	}
</style>
