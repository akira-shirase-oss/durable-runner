<script lang="ts">
	// One workflow (signed in): its current steps and default input, Run now (optionally with another input; one
	// Idempotency-Key per press, so a retried request is the same run, never a second one), edit (a new version —
	// old versions never change), rename, archive, the version history and its recent runs.
	import { page } from '$app/state';
	import { goto } from '$app/navigation';
	import Icon from '../../../lib/Icon.svelte';
	import Confirm from '../../../lib/ui/Confirm.svelte';
	import ErrorBanner from '../../../lib/ui/ErrorBanner.svelte';
	import Notice from '../../../lib/ui/Notice.svelte';
	import RoleNote from '../../../lib/ui/RoleNote.svelte';
	import Skeleton from '../../../lib/ui/Skeleton.svelte';
	import StateChip from '../../../lib/ui/StateChip.svelte';
	import StepsEditor from '../../../lib/ui/StepsEditor.svelte';
	import { account } from '../../../lib/account.svelte.ts';
	import { ApiError, archiveWorkflow, getWorkflow, listHandlers, runWorkflow, updateWorkflow, workflowRuns } from '../../../lib/api.ts';
	import { fmtAgo, fmtDateTime } from '../../../lib/format.ts';
	import { live, poll, Resource } from '../../../lib/live.svelte.ts';
	import { parseWorkflowId, runHref } from '../../../lib/routes.ts';
	import { listOf, normHandler, normWorkflowDetail, normWorkflowRun, type HandlerRow, type StepSpec, type WorkflowRun } from '../../../lib/saas.ts';
	import { b64ToText, inputToB64, nameError, stepsError } from '../../../lib/validate.ts';

	const pid = account.pid ?? '';
	const id = $derived(parseWorkflowId(page.params.id));
	const res = new Resource((signal) => (id ? getWorkflow(account.cred, pid, id, signal) : Promise.reject(new ApiError('not a workflow id', 404, 'not_found'))), { interval: 30_000, live: false });
	poll(res, () => id);
	const runsRes = new Resource((signal) => (id ? workflowRuns(account.cred, pid, id, signal) : Promise.resolve(null)), { interval: 10_000, live: false });
	poll(runsRes, () => id);
	const handlersRes = new Resource((signal) => listHandlers(account.cred, signal), { interval: 0, live: false });
	poll(handlersRes);

	const wf = $derived(res.data ? normWorkflowDetail(res.data) : null);
	const current = $derived(wf?.versions[0] ?? null);
	const runs = $derived(runsRes.data ? listOf(runsRes.data, 'runs').map(normWorkflowRun).filter((r): r is WorkflowRun => r !== null) : []);
	const handlerNames = $derived(handlersRes.status === 'ok' ? listOf(handlersRes.data, 'handlers').map(normHandler).filter((h): h is HandlerRow => h !== null).map((h) => h.name) : null);
	const write = $derived(account.can('write'));
	const notFound = $derived(res.error?.status === 404);

	let busy = $state<string | null>(null);
	let error = $state<ApiError | null>(null);
	let notice = $state<string | null>(null);
	async function guard(key: string, fn: () => Promise<void>) {
		busy = key;
		error = null;
		notice = null;
		try {
			await fn();
		} catch (e) {
			error = e instanceof ApiError ? e : new ApiError(String(e), 0);
		} finally {
			busy = null;
		}
	}

	// Run now
	let override = $state(false);
	let runInput = $state('');
	const runInputErr = $derived(override ? inputToB64(runInput).error : null);
	function runNow() {
		if (!id) return;
		const enc = override ? inputToB64(runInput) : { b64: null, error: null };
		if (override && enc.b64 === null) return;
		const key = crypto.randomUUID();
		void guard('run', async () => {
			const r = await runWorkflow(account.cred, pid, id, override ? enc.b64 : null, key);
			void goto(runHref(r.run_id));
		});
	}

	// edit → a new version
	let editing = $state(false);
	let steps = $state<StepSpec[]>([]);
	let input = $state('');
	let name = $state('');
	let tried = $state(false);
	const stepsErr = $derived(stepsError(steps));
	const inputErr = $derived(inputToB64(input).error);
	const nameErr = $derived(nameError(name, 'workflow name'));
	function startEdit() {
		if (!wf || !current) return;
		steps = current.spec.steps.map((s) => ({ ...s }));
		input = b64ToText(current.spec.input_b64) ?? '';
		name = wf.name;
		tried = false;
		editing = true;
	}
	function save(e: SubmitEvent) {
		e.preventDefault();
		tried = true;
		const enc = inputToB64(input);
		if (!id || !wf || stepsErr || nameErr || enc.b64 === null || busy) return;
		const spec = { version: 1, steps: steps.map((s) => ({ name: s.name.trim(), handler: s.handler.trim() })), input_b64: enc.b64 };
		const n = name.trim();
		const changedSpec = JSON.stringify(spec.steps) !== JSON.stringify(current?.spec.steps) || spec.input_b64 !== (current?.spec.input_b64 ?? '');
		void guard('save', async () => {
			await updateWorkflow(account.cred, pid, id, { ...(n !== wf.name ? { name: n } : {}), ...(changedSpec ? { spec } : {}) });
			editing = false;
			notice = changedSpec ? 'Saved as a new version. Runs already started keep the version they began with.' : 'Saved.';
			await res.refresh(true);
		});
	}
	const archive = () =>
		guard('archive', async () => {
			if (!id) return;
			await archiveWorkflow(account.cred, pid, id);
			void goto('#/workflows');
		});
</script>

<div class="page">
	<a class="back" href="#/workflows"><Icon name="arrow-left" size={15} />All workflows</a>
	{#if id === null || notFound}
		<p class="banner error"><Icon name="alert" size={16} /><span class="msg">There is no such workflow in this workspace. It may have been archived.</span></p>
	{:else}
		<RoleNote need="write" what="running or editing this workflow" />
		{#if notice}<p class="banner notice" role="status"><Icon name="check" size={16} /><span class="msg">{notice}</span><button class="btn ghost sm act" onclick={() => (notice = null)}>Dismiss</button></p>{/if}
		{#if error}<ErrorBanner {error} dismiss={() => (error = null)} />{/if}
		{#if res.status === 'unsupported' || res.status === 'needs-key' || (res.status === 'error' && !res.data)}
			<Notice status={res.status} error={res.error} what="This workflow" route="GET /workspaces/{'{pid}'}/workflows/{'{id}'}" retry={() => res.refresh(true)} />
		{:else if !wf}
			<section class="card"><Skeleton label="Loading the workflow" /></section>
		{:else}
			<section class="card head" aria-labelledby="wf-name">
				<div class="card-head">
					<div>
						<p class="eyebrow">Workflow</p>
						<h2 id="wf-name" class="wf-name">{wf.name}{#if wf.currentVersion !== null}<span class="tag">v{wf.currentVersion}</span>{/if}</h2>
						<p class="sub">Updated {wf.updatedAt ? fmtAgo(live.now - wf.updatedAt) : '—'}{wf.lastRun ? ` · last run ${wf.lastRun.at ? fmtAgo(live.now - wf.lastRun.at) : ''}` : ' · never run'}</p>
					</div>
					{#if write}
						<div class="actions">
							<button class="btn brand sm" disabled={busy !== null || !!runInputErr} onclick={runNow}><Icon name="play" />{busy === 'run' ? 'Starting…' : 'Run now'}</button>
							{#if !editing}<button class="btn sm" onclick={startEdit}><Icon name="pencil" />Edit</button>{/if}
							<Confirm label="Archive" icon="archive" question="Archive {wf.name}? It leaves the list; its runs stay on Runs." confirmLabel="Archive" busy={busy === 'archive'} onconfirm={archive} />
						</div>
					{/if}
				</div>
				<div class="card-body">
					{#if write}
						<label class="override"><input type="checkbox" bind:checked={override} />Run with a different input this time</label>
						{#if override}
							<div class="field">
								<label class="field-label" for="run-input">Input for this run (JSON)</label>
								<textarea id="run-input" class="mono" rows="3" bind:value={runInput} spellcheck="false" aria-invalid={runInputErr ? 'true' : undefined} aria-describedby={runInputErr ? 'run-input-err' : undefined}></textarea>
								{#if runInputErr}<p class="field-hint bad" id="run-input-err">{runInputErr}</p>{/if}
							</div>
						{/if}
					{/if}
					{#if editing}
						<form class="form edit" onsubmit={save} novalidate>
							<div class="field">
								<label class="field-label" for="wf-rename">Name</label>
								<input id="wf-rename" bind:value={name} maxlength="128" autocomplete="off" aria-invalid={tried && nameErr ? 'true' : undefined} />
								{#if tried && nameErr}<p class="field-hint bad">{nameErr}</p>{/if}
							</div>
							<StepsEditor bind:steps handlers={handlerNames} id="edit-steps" error={tried ? stepsErr : null} />
							<div class="field">
								<label class="field-label" for="wf-input">Default input (JSON)</label>
								<textarea id="wf-input" class="mono" rows="3" bind:value={input} spellcheck="false" aria-invalid={inputErr ? 'true' : undefined} aria-describedby="wf-input-hint"></textarea>
								<p class="field-hint" class:bad={!!inputErr} id="wf-input-hint">{inputErr ?? 'Saving a change to the steps or input adds a version.'}</p>
							</div>
							<div class="form-actions">
								<button class="btn brand sm" type="submit" disabled={busy !== null}>{busy === 'save' ? 'Saving…' : 'Save'}</button>
								<button class="btn ghost sm" type="button" onclick={() => (editing = false)}>Cancel</button>
							</div>
						</form>
					{:else if current}
						<ol class="step-list">
							{#each current.spec.steps as s, i (i)}
								<li><span class="n">{i + 1}</span><span class="mono">{s.name}</span><span class="arrow" aria-hidden="true">→</span><span class="mono handler">{s.handler}</span></li>
							{/each}
						</ol>
						<p class="field-label">Default input</p>
						<p class="input mono">{current.spec.input_b64 ? (b64ToText(current.spec.input_b64) ?? current.spec.input_b64) : 'none (empty)'}</p>
					{/if}
				</div>
			</section>

			<div class="grid two">
				<section class="card" aria-labelledby="runs-title">
					<div class="card-head ruled">
						<div>
							<h2 id="runs-title"><Icon name="list" size={17} />Recent runs</h2>
							<p class="sub">Started from this workflow, newest first.</p>
						</div>
					</div>
					{#if runsRes.status === 'unsupported' || runsRes.status === 'needs-key' || (runsRes.status === 'error' && !runsRes.data)}
						<div class="card-body"><Notice status={runsRes.status} error={runsRes.error} what="Its runs" route="GET …/workflows/{'{id}'}/runs" retry={() => runsRes.refresh(true)} /></div>
					{:else if runsRes.loading}
						<Skeleton label="Loading runs" rows={2} />
					{:else if runs.length === 0}
						<p class="empty">Not run yet.{write ? ' Press Run now.' : ''}</p>
					{:else}
						<ul class="rows">
							{#each runs as r (r.runId)}
								<li>
									<div class="row-main">
										<p class="row-title"><a class="mono run-link" href={runHref(r.runId)}>{r.runId}</a></p>
										<p class="row-meta"><span title={r.submittedAt ? fmtDateTime(r.submittedAt, live.now) : ''}>Started {r.submittedAt ? fmtAgo(live.now - r.submittedAt) : '—'}</span></p>
									</div>
									{#if r.state}<div class="row-actions"><StateChip state={r.state} /></div>{/if}
								</li>
							{/each}
						</ul>
					{/if}
				</section>
				<section class="card" aria-labelledby="versions-title">
					<div class="card-head ruled">
						<div>
							<h2 id="versions-title"><Icon name="history" size={17} />Versions</h2>
							<p class="sub">Every saved spec, kept as it was.</p>
						</div>
					</div>
					<ul class="rows">
						{#each wf.versions as v (v.version)}
							<li>
								<div class="row-main">
									<p class="row-title"><span class="tag">v{v.version}</span>{#if v.version === wf.currentVersion}<span class="badge ok">Current</span>{/if}</p>
									<p class="row-meta"><span class="mono steps-sum">{v.spec.steps.map((s) => s.handler).join(' → ') || 'no steps'}</span><span>{v.createdAt ? fmtAgo(live.now - v.createdAt) : ''}</span></p>
								</div>
							</li>
						{/each}
					</ul>
				</section>
			</div>
		{/if}
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
	.eyebrow {
		margin: 0 0 2px;
		color: var(--c-body);
		font-size: 12.5px;
	}
	.wf-name {
		font-size: 20px !important;
		overflow-wrap: anywhere;
	}
	.actions {
		display: flex;
		flex-wrap: wrap;
		align-items: flex-start;
		gap: 8px;
	}
	.override {
		display: inline-flex;
		align-items: center;
		gap: 8px;
		margin-bottom: 12px;
		color: var(--c-ink-soft);
		font-size: 14px;
		cursor: pointer;
	}
	.override input {
		width: 16px;
		min-height: 0;
		height: 16px;
		margin: 0;
		accent-color: var(--c-brand-deep);
	}
	.edit {
		padding-top: 4px;
	}
	.step-list {
		display: flex;
		flex-direction: column;
		gap: 6px;
		margin: 0 0 16px;
		padding: 0;
		list-style: none;
	}
	.step-list li {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 8px;
		font-size: 14px;
	}
	.step-list .n {
		display: grid;
		place-items: center;
		width: 24px;
		height: 24px;
		border-radius: 7px;
		background: var(--c-surface);
		color: var(--c-body);
		font-size: 12px;
	}
	.step-list .arrow {
		color: var(--c-body);
	}
	.step-list .handler {
		color: var(--c-brand-deep);
	}
	.input {
		margin: 6px 0 0;
		padding: 10px 12px;
		border-radius: 10px;
		background: var(--c-panel);
		box-shadow: inset 0 0 0 1px var(--c-line);
		font-size: 13px;
		overflow-wrap: anywhere;
		white-space: pre-wrap;
	}
	.run-link {
		color: var(--c-brand-deep);
		text-decoration: none;
	}
	.run-link:hover {
		text-decoration: underline;
		text-underline-offset: 3px;
	}
	.steps-sum {
		overflow-wrap: anywhere;
	}
</style>
