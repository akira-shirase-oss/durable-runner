<script lang="ts">
	// Submit a job (POST /jobs, API key): an operation key (the idempotency key: the same key answers with the same
	// run), named steps with their handlers, the input (sent base64-encoded) and an optional not-before time.
	import Icon from '../../lib/Icon.svelte';
	import { submitJob } from '../../lib/api.ts';
	import { runHref } from '../../lib/routes.ts';
	import { session } from '../../lib/session.svelte.ts';

	let opKey = $state('');
	let steps = $state<{ name: string; handler: string }[]>([{ name: 'only', handler: '' }]);
	let inputText = $state('');
	let notBefore = $state('');
	let sending = $state(false);
	let error = $state<string | null>(null);
	let accepted = $state<{ id: string; key: string } | null>(null);
	let successHeading: HTMLElement | undefined = $state();

	const ready = $derived(!!session.apiKey && !!opKey && steps.every((s) => s.name && s.handler));

	function b64(s: string): string {
		return btoa(unescape(encodeURIComponent(s)));
	}

	async function submit(e: SubmitEvent) {
		e.preventDefault();
		if (!ready || sending) return;
		sending = true;
		try {
			const body: Parameters<typeof submitJob>[1] = {
				op_key: opKey,
				spec: { version: 1, steps: steps.filter((s) => s.name && s.handler), input_b64: b64(inputText) },
			};
			if (notBefore) body.not_before = Date.parse(notBefore);
			const r = await submitJob(session.apiKey, body);
			accepted = { id: r.run_id, key: opKey };
			error = null;
			queueMicrotask(() => successHeading?.focus());
		} catch (err) {
			error = err instanceof Error ? err.message : String(err);
		} finally {
			sending = false;
		}
	}

	function again() {
		accepted = null;
		opKey = '';
	}
</script>

<div class="page">
	{#if !session.apiKey}
		<p class="banner hint"><Icon name="key" size={16} /><span class="msg">Submitting needs the API key. <a href="#/settings">Add it in Settings</a></span></p>
	{/if}
	{#if error}
		<p class="banner error"><Icon name="alert" size={16} /><span class="msg">{error}</span><button class="btn ghost sm act" onclick={() => (error = null)}>Dismiss</button></p>
	{/if}

	<div class="layout">
		{#if accepted}
			<section class="card success" aria-labelledby="ok-title">
				<span class="ok-icon" aria-hidden="true"><Icon name="check" size={22} /></span>
				<h2 id="ok-title" tabindex="-1" bind:this={successHeading}>Run {accepted.id} accepted</h2>
				<p class="hint">
					Committed to the journal under <span class="mono">{accepted.key}</span>. Submitting the same operation key again
					answers with this run instead of starting a second one.
				</p>
				<div class="row">
					<a class="btn brand sm" href={runHref(accepted.id)}><Icon name="arrow-right" />Open run {accepted.id}</a>
					<button class="btn sm" onclick={again}><Icon name="plus" />Submit another</button>
				</div>
			</section>
		{:else}
			<form class="card form" onsubmit={submit} aria-labelledby="submit-title">
				<div class="form-head">
					<h2 id="submit-title">New job</h2>
					<span class="tag">spec v1</span>
				</div>
				<label class="field">
					<span class="field-label">Operation key</span>
					<input class="mono" required maxlength="256" bind:value={opKey} placeholder="deploy-2026-11-24" aria-describedby="opkey-hint" />
					<span class="field-hint" id="opkey-hint">The idempotency key: one business operation, one run, across retries.</span>
				</label>
				<fieldset>
					<legend class="field-label">Steps</legend>
					<ol class="step-rows">
						{#each steps as s, i (i)}
							<li class="step-row">
								<span class="n" aria-hidden="true">{i}</span>
								<input class="mono" required placeholder="name" aria-label="Step {i} name" bind:value={s.name} />
								<span class="arrow" aria-hidden="true">→</span>
								<input class="mono" required placeholder="handler" aria-label="Step {i} handler" bind:value={s.handler} />
								<button type="button" class="btn ghost icon remove" title="Remove step {i}" disabled={steps.length === 1}
									onclick={() => (steps = steps.filter((_, j) => j !== i))}><Icon name="trash" /><span class="sr">Remove step {i}</span></button>
							</li>
						{/each}
					</ol>
					<button type="button" class="btn sm add" onclick={() => (steps = [...steps, { name: '', handler: '' }])}><Icon name="plus" />Add step</button>
					<span class="field-hint">A handler is a name from the server's allowlist (<span class="mono">--handler name=url</span>); steps run in order, each checkpointed.</span>
				</fieldset>
				<label class="field">
					<span class="field-label">Input</span>
					<textarea class="mono" rows="4" bind:value={inputText} placeholder={'{"env":"prod"}'} aria-describedby="input-hint"></textarea>
					<span class="field-hint" id="input-hint">Sent base64-encoded (up to 64 KiB). Only the spec reveal shows it again, with the API key.</span>
				</label>
				<label class="field narrow">
					<span class="field-label">Not before <span class="opt">optional</span></span>
					<input type="datetime-local" bind:value={notBefore} />
				</label>
				<div class="form-actions">
					<button type="submit" class="btn brand" disabled={!ready || sending}><Icon name="arrow-up-right" />{sending ? 'Submitting…' : 'Submit'}</button>
				</div>
			</form>
		{/if}

		<aside class="card help" aria-labelledby="help-title">
			<h2 id="help-title"><Icon name="book" size={16} />What happens</h2>
			<ol>
				<li><strong>Committed first.</strong> The run is durable in the journal before the server answers.</li>
				<li><strong>Each step is dispatched</strong> to its handler with the run's idempotency key and a deadline.</li>
				<li><strong>Failures retry</strong> with backoff; a lost reply is reconciled before anything repeats.</li>
				<li><strong>Every attempt shows</strong> on the run's timeline and Gantt as it happens.</li>
			</ol>
		</aside>
	</div>
</div>

<style>
	.layout {
		display: grid;
		grid-template-columns: minmax(0, 760px) minmax(0, 320px);
		gap: 16px;
		align-items: start;
	}
	@media (max-width: 1100px) {
		.layout {
			grid-template-columns: minmax(0, 1fr);
		}
	}
	.form {
		display: flex;
		flex-direction: column;
		gap: 18px;
		padding: 18px 20px 20px;
	}
	.form-head {
		display: flex;
		align-items: center;
		justify-content: space-between;
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
		padding: 0;
	}
	.step-rows {
		display: flex;
		flex-direction: column;
		gap: 8px;
		width: 100%;
		margin: 0;
		padding: 0;
		list-style: none;
	}
	.step-row {
		display: grid;
		grid-template-columns: 22px minmax(0, 1fr) auto minmax(0, 1fr) 40px;
		gap: 10px;
		align-items: center;
	}
	.n {
		display: inline-grid;
		place-items: center;
		width: 22px;
		height: 22px;
		border-radius: 6px;
		background: var(--c-state-run-bg);
		color: var(--c-state-run-ink);
		font-size: 11.5px;
		font-weight: 600;
	}
	.arrow {
		color: var(--c-body);
	}
	.remove {
		color: var(--c-body);
	}
	.opt {
		color: var(--c-body);
		font-weight: 440;
	}
	.field.narrow {
		max-width: 300px;
	}
	.form-actions {
		display: flex;
		gap: 10px;
		padding-top: 4px;
	}
	.success {
		display: flex;
		flex-direction: column;
		align-items: flex-start;
		gap: 10px;
		padding: 22px 22px 24px;
	}
	.success h2 {
		font-size: 20px;
	}
	.success h2:focus {
		outline: none;
	}
	.success .hint {
		margin: 0;
		max-width: 60ch;
		line-height: 1.5;
	}
	.ok-icon {
		display: grid;
		place-items: center;
		width: 44px;
		height: 44px;
		border-radius: 14px;
		background: var(--c-state-done-bg);
		color: var(--c-state-done-ink);
	}
	.row {
		display: flex;
		flex-wrap: wrap;
		gap: 8px;
		margin-top: 6px;
	}
	.row a {
		text-decoration: none;
	}
	.help {
		padding: 18px 20px;
	}
	.help ol {
		display: flex;
		flex-direction: column;
		gap: 10px;
		margin: 12px 0 0;
		padding-left: 20px;
		color: var(--c-body);
		font-size: 13.5px;
		line-height: 1.5;
	}
	.help strong {
		color: var(--c-ink);
		font-weight: 560;
	}
	@media (max-width: 600px) {
		.form {
			padding: 16px;
		}
		.step-row {
			grid-template-columns: 22px minmax(0, 1fr) 40px;
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
	}
</style>
