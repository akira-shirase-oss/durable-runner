<script lang="ts">
	// The setup checklist on the Overview until every step is done: create an API key → register a handler (or use
	// the built-in sample) → a test call that answers ok → the first job. Each step is checked off from facts
	// (GET …/onboarding; on a server without that route yet, from the keys, handlers and diagnostics this page can
	// read — saas.ts deriveOnboarding), never from a flag the page set. Actions go straight to the API where they
	// are one click (sample handler, test call, a test job); the rest link to their page.
	import Icon from '../Icon.svelte';
	import CodeBlock from './CodeBlock.svelte';
	import ErrorBanner from './ErrorBanner.svelte';
	import { account } from '../account.svelte.ts';
	import { ApiError, listHandlers, listKeys, onboarding, putHandler, submitJob, testHandler, workspaceDiagnostics } from '../api.ts';
	import { poll, Resource } from '../live.svelte.ts';
	import { runHref } from '../routes.ts';
	import {
		deriveOnboarding,
		finishedFromStates,
		listOf,
		normHandler,
		normKey,
		normOnboarding,
		normTest,
		onboardingDone,
		SAMPLE_HANDLER,
		type HandlerRow,
		type KeyRow,
		type OnboardingStep,
		type StepId,
	} from '../saas.ts';
	import { goto } from '$app/navigation';

	const pid = account.pid ?? '';
	const SLOW = 15_000;

	const server = new Resource((signal) => onboarding(account.cred, pid, signal), { interval: SLOW, live: false });
	poll(server);
	const fallback = $derived(server.status === 'unsupported');

	// the facts, for a server without the onboarding route (and for the actions below either way)
	const handlersRes = new Resource((signal) => listHandlers(account.cred, signal), { interval: SLOW, live: false });
	poll(handlersRes);
	const keysRes = new Resource((signal) => (fallback ? listKeys(account.cred, pid, signal) : Promise.resolve(null)), { interval: () => (fallback ? SLOW : 0), live: false });
	poll(keysRes, () => fallback);
	const diagRes = new Resource((signal) => (fallback ? workspaceDiagnostics(account.cred, signal) : Promise.resolve(null)), { interval: () => (fallback ? SLOW : 0), live: false });
	poll(diagRes, () => fallback);

	const handlers = $derived(handlersRes.data ? listOf(handlersRes.data, 'handlers').map(normHandler).filter((h): h is HandlerRow => h !== null) : null);
	const keys = $derived(keysRes.status === 'ok' && keysRes.data ? listOf(keysRes.data, 'keys').map(normKey).filter((k): k is KeyRow => k !== null) : null);
	const steps: OnboardingStep[] | null = $derived(
		server.status === 'ok'
			? normOnboarding(server.data)
			: fallback
				? deriveOnboarding({
						keys,
						handlers,
						finishedRuns: diagRes.status === 'ok' ? finishedFromStates(diagRes.data?.states) : null,
						testOkSeen: account.testOkSeen,
					})
				: null,
	);
	const done = $derived(steps !== null && onboardingDone(steps));
	const count = $derived(steps?.filter((s) => s.done).length ?? 0);
	const hasSample = $derived(handlers?.some((h) => h.name === SAMPLE_HANDLER) ?? false);
	const firstHandler = $derived(handlers?.[0]?.name ?? null);
	const canWrite = $derived(account.can('write'));
	const canAdmin = $derived(account.can('admin'));

	let busy = $state<StepId | null>(null);
	let error = $state<ApiError | null>(null);
	let note = $state<string | null>(null);

	const origin = typeof location === 'undefined' ? '' : location.origin;
	const curl = $derived(
		`curl -X POST ${origin}/jobs \\\n  -H "Authorization: Bearer <key>" \\\n  -H "Content-Type: application/json" \\\n  -d '{"op_key":"hello-1","spec":{"version":1,"steps":[{"name":"echo","handler":"${firstHandler ?? SAMPLE_HANDLER}"}],"input_b64":"eyJoZWxsbyI6IndvcmxkIn0="}}'`,
	);

	async function act(id: StepId, fn: () => Promise<void>) {
		busy = id;
		error = null;
		note = null;
		try {
			await fn();
			void server.refresh(true);
			void handlersRes.refresh(true);
			if (fallback) {
				void keysRes.refresh(true);
				void diagRes.refresh(true);
			}
		} catch (e) {
			error = e instanceof ApiError ? e : new ApiError(String(e), 0);
		} finally {
			busy = null;
		}
	}

	const useSample = () =>
		act('handler', async () => {
			await putHandler(account.cred, SAMPLE_HANDLER, SAMPLE_HANDLER);
			note = 'The sample handler is registered: it echoes its input back, in-process, with no network call.';
		});
	const testCall = () =>
		act('test_call', async () => {
			const name = hasSample ? SAMPLE_HANDLER : firstHandler;
			if (!name) return;
			const r = normTest(await testHandler(account.cred, name, 'eyJoZWxsbyI6IndvcmxkIn0='));
			if (r.status === 'ok') account.testOkSeen = true;
			note = r.status === 'ok' ? `${name} answered ok${r.latencyMs !== null ? ` in ${r.latencyMs} ms` : ''}.` : `${name} answered ${r.status}${r.httpStatus ? ` (HTTP ${r.httpStatus})` : ''}. Open Handlers for the reply.`;
		});
	const testJob = () =>
		act('first_run', async () => {
			const name = hasSample ? SAMPLE_HANDLER : firstHandler;
			if (!name) return;
			const r = await submitJob(account.cred, {
				op_key: `dashboard-test-${crypto.randomUUID()}`,
				spec: { version: 1, steps: [{ name: 'echo', handler: name }], input_b64: 'eyJoZWxsbyI6IndvcmxkIn0=' },
			});
			void goto(runHref(r.run_id));
		});

	const TITLE: Record<StepId, string> = {
		api_key: 'Create an API key',
		handler: 'Register a handler',
		test_call: 'Send a test call',
		first_run: 'Send your first job',
	};
	const WHY: Record<StepId, string> = {
		api_key: 'Your code sends jobs with a write key. The key is shown once, when it is created.',
		handler: 'A handler is the HTTPS endpoint a step calls. No endpoint yet? The built-in sample echoes its input back.',
		test_call: 'One signed call to the handler, outside any run, to prove the URL and signature check work.',
		first_run: 'Submit a job with your key. It is done when the run finishes.',
	};
</script>

{#if steps && !done}
	<section class="card checklist" aria-labelledby="ob-title">
		<div class="card-head ruled">
			<div>
				<h2 id="ob-title"><Icon name="flag" size={17} />Get set up</h2>
				<p class="sub">{count} of {steps.length} done{fallback ? ' · checked from your keys, handlers and runs' : ''}. This card goes away when every step is done.</p>
			</div>
			<span class="progress" aria-hidden="true"><i style:width="{(count / steps.length) * 100}%"></i></span>
		</div>
		<div class="card-body">
			{#if error}<ErrorBanner {error} dismiss={() => (error = null)} />{/if}
			{#if note}<p class="banner notice" role="status"><Icon name="check" size={16} /><span class="msg">{note}</span><button class="btn ghost sm act" onclick={() => (note = null)}>Dismiss</button></p>{/if}
			<ol class="steps">
				{#each steps as s, i (s.id)}
					<li class:done={s.done}>
						<span class="tick" aria-hidden="true"><Icon name={s.done ? 'check' : 'circle'} size={20} /></span>
						<div class="step-body">
							<p class="step-title">
								<span class="sr">Step {i + 1}{s.done ? ', done' : ', to do'}: </span>{TITLE[s.id]}{#if s.done && s.detail}<span class="step-detail mono">{s.detail}</span>{/if}
							</p>
							{#if !s.done}
								<p class="step-why">{WHY[s.id]}</p>
								<div class="step-actions">
									{#if s.id === 'api_key'}
										{#if canAdmin}<a class="btn sm" href="#/keys"><Icon name="key" />Create a key</a>{:else}<span class="muted">Creating keys needs the admin role: ask an admin.</span>{/if}
									{:else if s.id === 'handler'}
										{#if canWrite}
											<button class="btn sm" disabled={busy !== null || hasSample} onclick={useSample}><Icon name="zap" />{busy === 'handler' ? 'Registering…' : 'Use the sample handler'}</button>
											<a class="btn ghost sm" href="#/handlers">Register your own<Icon name="arrow-right" size={14} /></a>
										{:else}<span class="muted">Registering handlers needs the member role.</span>{/if}
									{:else if s.id === 'test_call'}
										{#if canWrite && (hasSample || firstHandler)}
											<button class="btn sm" disabled={busy !== null} onclick={testCall}><Icon name="send" />{busy === 'test_call' ? 'Calling…' : `Test ${hasSample ? SAMPLE_HANDLER : firstHandler}`}</button>
										{:else if canWrite}<span class="muted">Register a handler first.</span>{:else}<span class="muted">Test calls need the member role.</span>{/if}
									{:else}
										{#if canWrite && (hasSample || firstHandler)}
											<button class="btn sm" disabled={busy !== null} onclick={testJob}><Icon name="play" />{busy === 'first_run' ? 'Sending…' : 'Send a test job from here'}</button>
										{/if}
									{/if}
								</div>
								{#if s.id === 'first_run'}
									<CodeBlock code={curl} label="Submit with curl (replace <key> with your write key)" />
								{/if}
							{/if}
						</div>
					</li>
				{/each}
			</ol>
		</div>
	</section>
{:else if server.status === 'error' && !steps}
	<ErrorBanner error={server.error!} what="The setup checklist" retry={() => server.refresh(true)} />
{/if}

<style>
	.checklist .card-head {
		align-items: flex-start;
	}
	.progress {
		flex: none;
		width: 140px;
		height: 8px;
		margin-top: 6px;
		overflow: hidden;
		border-radius: 4px;
		background: var(--c-line);
	}
	.progress i {
		display: block;
		height: 100%;
		border-radius: 4px;
		background: var(--c-state-done);
	}
	.steps {
		display: flex;
		flex-direction: column;
		gap: 0;
		margin: 0;
		padding: 0;
		list-style: none;
	}
	.steps li {
		display: flex;
		gap: 12px;
		padding: 14px 0;
		border-top: 1px solid var(--c-line);
	}
	.steps li:first-child {
		border-top: 0;
		padding-top: 4px;
	}
	.tick {
		flex: none;
		padding-top: 1px;
		color: var(--c-line-strong);
	}
	li.done .tick {
		color: var(--c-state-done);
	}
	.step-body {
		display: flex;
		flex: 1;
		flex-direction: column;
		gap: 8px;
		min-width: 0;
	}
	.step-title {
		display: flex;
		flex-wrap: wrap;
		align-items: baseline;
		gap: 4px 10px;
		margin: 0;
		font-size: 15px;
		font-weight: 560;
	}
	li.done .step-title {
		color: var(--c-body);
		font-weight: 500;
	}
	.step-detail {
		color: var(--c-body);
		font-size: 12.5px;
		font-weight: 450;
	}
	.step-why {
		max-width: 70ch;
		margin: 0;
		color: var(--c-body);
		font-size: 14px;
		line-height: 1.5;
	}
	.step-actions {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 8px;
	}
	.step-actions a {
		text-decoration: none;
	}
	.step-actions .muted {
		font-size: 13.5px;
	}
	@media (max-width: 600px) {
		.progress {
			display: none;
		}
	}
</style>
