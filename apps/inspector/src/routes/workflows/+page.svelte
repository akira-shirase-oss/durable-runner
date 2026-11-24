<script lang="ts">
	// Workflows (signed in): saved, versioned job definitions (D-065). The list (name, version, last run), a create
	// form (name, steps from the registry, default input) and, while there are none, the sample: a one-step
	// workflow on the built-in echo handler that runs at once and lands on its timeline — no setup at all.
	import Icon from '../../lib/Icon.svelte';
	import ErrorBanner from '../../lib/ui/ErrorBanner.svelte';
	import Notice from '../../lib/ui/Notice.svelte';
	import RoleNote from '../../lib/ui/RoleNote.svelte';
	import Skeleton from '../../lib/ui/Skeleton.svelte';
	import StateChip from '../../lib/ui/StateChip.svelte';
	import StepsEditor from '../../lib/ui/StepsEditor.svelte';
	import { goto } from '$app/navigation';
	import { account } from '../../lib/account.svelte.ts';
	import { ApiError, createWorkflow, listHandlers, listWorkflows, putHandler, runWorkflow } from '../../lib/api.ts';
	import { fmtAgo, fmtDateTime } from '../../lib/format.ts';
	import { live, poll, Resource } from '../../lib/live.svelte.ts';
	import { runHref, workflowHref } from '../../lib/routes.ts';
	import { listOf, normHandler, normWorkflow, SAMPLE_HANDLER, type HandlerRow, type StepSpec, type WorkflowRow } from '../../lib/saas.ts';
	import { inputToB64, nameError, stepsError } from '../../lib/validate.ts';

	const pid = account.pid ?? '';
	const res = new Resource((signal) => listWorkflows(account.cred, pid, signal), { interval: 15_000, live: false });
	poll(res);
	const handlersRes = new Resource((signal) => listHandlers(account.cred, signal), { interval: 0, live: false });
	poll(handlersRes);
	const workflows = $derived(res.data ? listOf(res.data, 'workflows').map(normWorkflow).filter((w): w is WorkflowRow => w !== null) : []);
	const handlerNames = $derived(handlersRes.status === 'ok' ? listOf(handlersRes.data, 'handlers').map(normHandler).filter((h): h is HandlerRow => h !== null).map((h) => h.name) : null);
	const write = $derived(account.can('write'));

	let busy = $state<string | null>(null);
	let error = $state<ApiError | null>(null);
	async function guard(id: string, fn: () => Promise<void>) {
		busy = id;
		error = null;
		try {
			await fn();
		} catch (e) {
			error = e instanceof ApiError ? e : new ApiError(String(e), 0);
		} finally {
			busy = null;
		}
	}

	// the sample: register builtin:echo if needed, create "demo", run it, land on the run
	const trySample = () =>
		guard('sample', async () => {
			if (!(handlerNames ?? []).includes(SAMPLE_HANDLER)) await putHandler(account.cred, SAMPLE_HANDLER, SAMPLE_HANDLER);
			const created = normWorkflow(await createWorkflow(account.cred, pid, 'demo', { version: 1, steps: [{ name: 'echo', handler: SAMPLE_HANDLER }], input_b64: 'eyJoZWxsbyI6IndvcmxkIn0=' }));
			if (!created) throw new ApiError('the server did not answer with the new workflow', 0);
			const r = await runWorkflow(account.cred, pid, created.id, null, crypto.randomUUID());
			void goto(runHref(r.run_id));
		});

	// create
	let showCreate = $state(false);
	let name = $state('');
	let steps = $state<StepSpec[]>([{ name: 'step-1', handler: '' }]);
	let input = $state('');
	let tried = $state(false);
	const nameErr = $derived(nameError(name, 'workflow name'));
	const stepsErr = $derived(stepsError(steps));
	const inputErr = $derived(inputToB64(input).error);
	$effect(() => {
		// pick the first registered handler for the first step once the registry is known
		if (handlerNames && handlerNames.length && steps.length === 1 && steps[0]!.handler === '') steps[0]!.handler = handlerNames[0]!;
	});
	function create(e: SubmitEvent) {
		e.preventDefault();
		tried = true;
		const enc = inputToB64(input);
		if (nameErr || stepsErr || enc.b64 === null || busy) return;
		const spec = { version: 1, steps: steps.map((s) => ({ name: s.name.trim(), handler: s.handler.trim() })), input_b64: enc.b64 };
		const n = name.trim();
		void guard('create', async () => {
			const w = normWorkflow(await createWorkflow(account.cred, pid, n, spec));
			if (w) void goto(workflowHref(w.id));
			else await res.refresh(true);
		});
	}
</script>

<div class="page">
	<RoleNote need="write" what="creating, editing or running workflows" />
	{#if error}<ErrorBanner {error} dismiss={() => (error = null)} />{/if}

	{#if res.status === 'unsupported' || res.status === 'needs-key' || (res.status === 'error' && !res.data)}
		<Notice status={res.status} error={res.error} what="Workflows" route="GET /workspaces/{'{pid}'}/workflows" retry={() => res.refresh(true)} />
	{:else if res.loading}
		<section class="card"><Skeleton label="Loading workflows" /></section>
	{:else}
		{#if workflows.length === 0}
			<section class="card sample" aria-labelledby="sample-title">
				<span class="sample-icon" aria-hidden="true"><Icon name="workflow" size={22} /></span>
				<h2 id="sample-title">Try a workflow with no setup</h2>
				<p class="hint">A workflow is a saved job: its steps, each calling a handler, plus a default input. <strong>Run now</strong> submits it like any job: journaled, retried, visible on Runs.</p>
				<p class="hint">The sample has one step on the built-in echo handler, which runs in-process and answers with its input. You land on its timeline.</p>
				{#if write}
					<div class="form-actions">
						<button class="btn brand sm" disabled={busy !== null} onclick={trySample}><Icon name="play" />{busy === 'sample' ? 'Creating and running…' : 'Create the sample and run it'}</button>
						<a class="btn ghost sm" href="#/handlers">Point it at a real endpoint<Icon name="arrow-right" size={14} /></a>
					</div>
				{/if}
			</section>
		{:else}
			<section class="card" aria-labelledby="wf-title">
				<div class="card-head ruled">
					<div>
						<h2 id="wf-title"><Icon name="workflow" size={17} />Workflows</h2>
						<p class="sub">{workflows.length} saved. Editing a workflow adds a version; runs keep the version they started with.</p>
					</div>
					{#if write && !showCreate}<button class="btn sm" onclick={() => (showCreate = true)}><Icon name="plus" />New workflow</button>{/if}
				</div>
				<ul class="rows">
					{#each workflows as w (w.id)}
						<li>
							<div class="row-main">
								<p class="row-title"><a class="wf-link" href={workflowHref(w.id)}>{w.name}</a>{#if w.currentVersion !== null}<span class="tag">v{w.currentVersion}</span>{/if}</p>
								<p class="row-meta">
									<span title={w.updatedAt ? fmtDateTime(w.updatedAt, live.now) : ''}>Updated {w.updatedAt ? fmtAgo(live.now - w.updatedAt) : '—'}</span>
									{#if w.lastRun}<span>Last run {w.lastRun.at ? fmtAgo(live.now - w.lastRun.at) : ''}</span>{:else}<span>Never run</span>{/if}
								</p>
							</div>
							{#if w.lastRun}
								<div class="row-actions">
									{#if w.lastRun.state}<StateChip state={w.lastRun.state} />{/if}
									<a class="btn ghost sm" href={runHref(w.lastRun.runId)}>Last run<Icon name="arrow-right" size={14} /></a>
								</div>
							{/if}
						</li>
					{/each}
				</ul>
			</section>
		{/if}

		{#if write && (showCreate || workflows.length === 0)}
			<section class="card" aria-labelledby="new-wf">
				<div class="card-head">
					<div>
						<h2 id="new-wf"><Icon name="plus" size={17} />New workflow</h2>
						<p class="sub">Steps run in order; each calls a registered handler with the run's input.</p>
					</div>
				</div>
				<div class="card-body">
					<form class="form" onsubmit={create} novalidate>
						<div class="field">
							<label class="field-label" for="wf-name">Name</label>
							<input id="wf-name" bind:value={name} maxlength="128" autocomplete="off" placeholder="e.g. nightly-export" aria-invalid={tried && nameErr ? 'true' : undefined} aria-describedby={tried && nameErr ? 'wf-name-err' : undefined} />
							{#if tried && nameErr}<p class="field-hint bad" id="wf-name-err">{nameErr}</p>{/if}
						</div>
						<StepsEditor bind:steps handlers={handlerNames} id="wf-steps" error={tried ? stepsErr : null} />
						<div class="field">
							<label class="field-label" for="wf-input">Default input (JSON, optional)</label>
							<textarea id="wf-input" class="mono" rows="3" bind:value={input} spellcheck="false" placeholder={'{"tenant": "acme"}'} aria-invalid={inputErr ? 'true' : undefined} aria-describedby="wf-input-hint"></textarea>
							<p class="field-hint" class:bad={!!inputErr} id="wf-input-hint">{inputErr ?? 'Run now can override it. Up to 64 KiB.'}</p>
						</div>
						<div class="form-actions">
							<button class="btn brand sm" type="submit" disabled={busy !== null}><Icon name="plus" />{busy === 'create' ? 'Saving…' : 'Save workflow'}</button>
							{#if workflows.length > 0}<button class="btn ghost sm" type="button" onclick={() => (showCreate = false)}>Cancel</button>{/if}
						</div>
					</form>
				</div>
			</section>
		{/if}
	{/if}
</div>

<style>
	.sample {
		display: flex;
		flex-direction: column;
		align-items: flex-start;
		gap: 10px;
		max-width: 760px;
		padding: 26px;
	}
	.sample h2 {
		font-size: 19px;
	}
	.sample .hint {
		max-width: 66ch;
		margin: 0;
		line-height: 1.55;
	}
	.sample-icon {
		display: grid;
		place-items: center;
		width: 46px;
		height: 46px;
		border-radius: 14px;
		background: var(--c-brand-tint);
		color: var(--c-brand-deep);
	}
	.sample a {
		text-decoration: none;
	}
	.wf-link {
		color: var(--c-ink);
		text-decoration: none;
	}
	.wf-link:hover {
		text-decoration: underline;
		text-underline-offset: 3px;
	}
	.row-actions a {
		text-decoration: none;
	}
</style>
