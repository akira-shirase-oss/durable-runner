<script lang="ts">
	// A workflow's steps: name + the registered handler it calls (picked from the workspace's registry; typed when
	// the registry could not be read), in order. Add, remove, move up and down. The server validates the spec exactly
	// like a submit (an unknown handler is refused before anything is saved).
	import Icon from '../Icon.svelte';
	import type { StepSpec } from '../saas.ts';

	let {
		steps = $bindable([]),
		handlers,
		id,
		error = null,
	}: {
		steps?: StepSpec[];
		/** registered handler names; null when unknown */
		handlers: string[] | null;
		id: string;
		error?: { index: number; field: string; message: string } | null;
	} = $props();

	function add() {
		steps = [...steps, { name: `step-${steps.length + 1}`, handler: handlers?.[0] ?? '' }];
	}
	function remove(i: number) {
		steps = steps.filter((_, j) => j !== i);
	}
	function move(i: number, d: -1 | 1) {
		const j = i + d;
		if (j < 0 || j >= steps.length) return;
		const next = [...steps];
		[next[i], next[j]] = [next[j]!, next[i]!];
		steps = next;
	}
</script>

<fieldset class="steps-editor" aria-describedby={error?.field === 'list' ? `${id}-list-err` : undefined}>
	<legend class="field-label">Steps, in order</legend>
	<ol>
		{#each steps as s, i (i)}
			<li>
				<span class="n" aria-hidden="true">{i + 1}</span>
				<div class="field">
					<label class="sr" for="{id}-name-{i}">Step {i + 1} name</label>
					<input id="{id}-name-{i}" class="mono" bind:value={s.name} maxlength="128" autocomplete="off" spellcheck="false" placeholder="name" aria-invalid={error?.index === i && error.field === 'name' ? 'true' : undefined} />
				</div>
				<span class="arrow" aria-hidden="true">→</span>
				<div class="field">
					<label class="sr" for="{id}-handler-{i}">Step {i + 1} handler</label>
					{#if handlers && handlers.length > 0}
						<select id="{id}-handler-{i}" class="mono" bind:value={s.handler} aria-invalid={error?.index === i && error.field === 'handler' ? 'true' : undefined}>
							{#if !handlers.includes(s.handler)}<option value={s.handler}>{s.handler || 'Pick a handler'}</option>{/if}
							{#each handlers as h (h)}<option value={h}>{h}</option>{/each}
						</select>
					{:else}
						<input id="{id}-handler-{i}" class="mono" bind:value={s.handler} autocomplete="off" spellcheck="false" placeholder="handler name" aria-invalid={error?.index === i && error.field === 'handler' ? 'true' : undefined} />
					{/if}
				</div>
				<div class="tools">
					<button class="btn ghost sm" type="button" disabled={i === 0} onclick={() => move(i, -1)}><span aria-hidden="true">↑</span><span class="sr">Move step {i + 1} up</span></button>
					<button class="btn ghost sm" type="button" disabled={i === steps.length - 1} onclick={() => move(i, 1)}><span aria-hidden="true">↓</span><span class="sr">Move step {i + 1} down</span></button>
					<button class="btn ghost sm" type="button" disabled={steps.length === 1} onclick={() => remove(i)}><Icon name="x" /><span class="sr">Remove step {i + 1}</span></button>
				</div>
				{#if error?.index === i}<p class="field-hint bad err">{error.message}</p>{/if}
			</li>
		{/each}
	</ol>
	{#if error?.field === 'list'}<p class="field-hint bad" id="{id}-list-err">{error.message}</p>{/if}
	<button class="btn sm add" type="button" onclick={add}><Icon name="plus" />Add step</button>
	{#if handlers && handlers.length === 0}<p class="field-hint">No handlers are registered yet: <a href="#/handlers">register one</a> (or the sample) first.</p>{/if}
</fieldset>

<style>
	.steps-editor {
		display: flex;
		flex-direction: column;
		gap: 10px;
		min-width: 0;
		margin: 0;
		padding: 0;
		border: 0;
	}
	ol {
		display: flex;
		flex-direction: column;
		gap: 8px;
		margin: 0;
		padding: 0;
		list-style: none;
	}
	li {
		display: grid;
		grid-template-columns: 26px minmax(0, 1fr) auto minmax(0, 1fr) auto;
		align-items: center;
		gap: 8px;
	}
	.n {
		display: grid;
		place-items: center;
		width: 26px;
		height: 26px;
		border-radius: 8px;
		background: var(--c-surface);
		color: var(--c-body);
		font-size: 12.5px;
		font-variant-numeric: tabular-nums;
	}
	.arrow {
		color: var(--c-body);
	}
	.tools {
		display: flex;
		gap: 2px;
	}
	.tools .btn {
		width: 32px;
		padding: 0;
	}
	.err {
		grid-column: 2 / -1;
	}
	.add {
		align-self: flex-start;
	}
	.field-hint a {
		color: var(--c-brand-deep);
	}
	@media (max-width: 640px) {
		li {
			grid-template-columns: 26px minmax(0, 1fr) auto;
			padding-bottom: 10px;
			border-bottom: 1px solid var(--c-line);
		}
		.arrow {
			display: none;
		}
		li > .field:nth-of-type(2) {
			grid-column: 2 / 3;
		}
		.tools {
			grid-row: 1 / 3;
			grid-column: 3;
			flex-direction: column;
		}
	}
</style>
