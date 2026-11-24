<script lang="ts">
	// A destructive action with a confirm step in place: the trigger opens "Revoke ci? [Revoke] [Keep]" right
	// where it was pressed, focus lands on the safe answer, Escape (or Keep) puts focus back on the trigger.
	import { tick } from 'svelte';
	import type { Snippet } from 'svelte';
	import Icon, { type IconName } from '../Icon.svelte';

	let {
		label,
		question,
		confirmLabel,
		icon = 'trash',
		busy = false,
		disabled = false,
		tone = 'danger',
		onconfirm,
		detail,
	}: {
		label: string;
		question: string;
		confirmLabel: string;
		icon?: IconName;
		busy?: boolean;
		disabled?: boolean;
		tone?: 'danger' | 'plain';
		onconfirm: () => void | Promise<void>;
		detail?: Snippet;
	} = $props();

	let asking = $state(false);
	let trigger: HTMLButtonElement | undefined = $state();
	let keep: HTMLButtonElement | undefined = $state();

	async function ask() {
		asking = true;
		await tick();
		keep?.focus();
	}
	async function no() {
		asking = false;
		await tick();
		trigger?.focus();
	}
	async function yes() {
		await onconfirm();
		asking = false;
	}
	function key(e: KeyboardEvent) {
		if (e.key === 'Escape') {
			e.stopPropagation();
			void no();
		}
	}
</script>

{#if asking}
	<!-- svelte-ignore a11y_no_noninteractive_element_interactions -->
	<div class="confirm" role="group" aria-label={question} onkeydown={key}>
		<p>{question}</p>
		{#if detail}{@render detail()}{/if}
		<div class="confirm-row">
			<button class="btn sm {tone === 'danger' ? 'danger' : ''}" type="button" disabled={busy} onclick={yes}><Icon name={icon} />{busy ? 'Working…' : confirmLabel}</button>
			<button class="btn sm" type="button" bind:this={keep} onclick={no}>Keep</button>
		</div>
	</div>
{:else}
	<button class="btn sm {tone === 'danger' ? 'danger' : ''}" type="button" bind:this={trigger} {disabled} onclick={ask}><Icon name={icon} />{label}</button>
{/if}
