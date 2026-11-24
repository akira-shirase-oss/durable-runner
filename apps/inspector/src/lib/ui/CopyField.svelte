<script lang="ts">
	// A value to copy (a new key, the signing secret, an invite link): monospace, wraps instead of scrolling, one
	// Copy button that says when it worked. The value is never written anywhere else by this component.
	import Icon from '../Icon.svelte';

	let { value, label, id }: { value: string; label: string; id: string } = $props();
	let copied = $state(false);
	let failed = $state(false);
	let timer: ReturnType<typeof setTimeout> | null = null;

	async function copy() {
		try {
			await navigator.clipboard.writeText(value);
			copied = true;
			failed = false;
		} catch {
			failed = true;
		}
		if (timer) clearTimeout(timer);
		timer = setTimeout(() => (copied = false), 2500);
	}
</script>

<div class="copy-field">
	<span class="field-label" id="{id}-label">{label}</span>
	<div class="copy-row">
		<code class="copy-value" aria-labelledby="{id}-label">{value}</code>
		<button class="btn sm" type="button" onclick={copy} aria-describedby="{id}-label">
			<Icon name={copied ? 'check' : 'copy'} />{copied ? 'Copied' : 'Copy'}
		</button>
	</div>
	<span class="sr" role="status">{copied ? `${label} copied` : ''}</span>
	{#if failed}<p class="field-hint">The browser refused the clipboard: select the value and copy it by hand.</p>{/if}
</div>
