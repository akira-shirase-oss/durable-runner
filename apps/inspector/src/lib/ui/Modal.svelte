<script lang="ts">
	// A modal dialog on the native <dialog> (focus moves in and is held there, Escape closes, the page behind is
	// inert). `dismissible: false` is for shown-once secrets: only the dialog's own button closes it, so a stray
	// Escape or backdrop click cannot throw the value away. Opens with a one-shot fade (none under reduced motion).
	import type { Snippet } from 'svelte';
	import Icon from '../Icon.svelte';

	let {
		open = $bindable(false),
		title,
		id,
		dismissible = true,
		onclose,
		children,
	}: {
		open?: boolean;
		title: string;
		id: string;
		dismissible?: boolean;
		onclose?: () => void;
		children: Snippet;
	} = $props();

	let el: HTMLDialogElement | undefined = $state();

	$effect(() => {
		if (!el) return;
		if (open && !el.open) el.showModal();
		else if (!open && el.open) el.close();
	});

	function closed() {
		open = false;
		onclose?.();
	}
	function cancel(e: Event) {
		if (!dismissible) e.preventDefault();
	}
	function backdrop(e: MouseEvent) {
		if (dismissible && e.target === el) open = false;
	}
</script>

<!-- the click handler only closes on the backdrop; the keyboard path is the dialog's own Escape -->
<!-- svelte-ignore a11y_click_events_have_key_events, a11y_no_noninteractive_element_interactions -->
<dialog class="modal" bind:this={el} aria-labelledby="{id}-title" onclose={closed} oncancel={cancel} onclick={backdrop}>
	<div class="modal-card">
		<div class="modal-head">
			<h2 id="{id}-title">{title}</h2>
			{#if dismissible}
				<button class="btn ghost icon sm-icon" type="button" aria-label="Close" onclick={() => (open = false)}><Icon name="x" size={17} /></button>
			{/if}
		</div>
		<div class="modal-body">
			{@render children()}
		</div>
	</div>
</dialog>
