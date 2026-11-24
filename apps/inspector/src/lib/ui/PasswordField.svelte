<script lang="ts">
	// A password input with Show/Hide, its label, a hint and the field's error. `autocomplete` tells password
	// managers which kind it is ("current-password" or "new-password").
	import type { Snippet } from 'svelte';

	let {
		id,
		label,
		value = $bindable(''),
		autocomplete,
		hint = '',
		error = null,
		aside,
	}: {
		id: string;
		label: string;
		value?: string;
		autocomplete: 'current-password' | 'new-password';
		hint?: string;
		error?: string | null;
		aside?: Snippet;
	} = $props();
	let shown = $state(false);
</script>

<div class="field">
	<div class="field-label-row">
		<label class="field-label" for={id}>{label}</label>
		{#if aside}{@render aside()}{/if}
	</div>
	<div class="input-with-action">
		<input
			{id}
			type={shown ? 'text' : 'password'}
			bind:value
			{autocomplete}
			spellcheck="false"
			autocapitalize="off"
			aria-invalid={error ? 'true' : undefined}
			aria-describedby={hint || error ? `${id}-hint` : undefined}
		/>
		<button class="btn" type="button" aria-pressed={shown} aria-controls={id} onclick={() => (shown = !shown)}>{shown ? 'Hide' : 'Show'}<span class="sr"> {label.toLowerCase()}</span></button>
	</div>
	{#if hint || error}<p class="field-hint" class:bad={!!error} id="{id}-hint">{error ?? hint}</p>{/if}
</div>
