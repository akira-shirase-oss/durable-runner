<script lang="ts">
	// A snippet to paste (curl, fetch, a signature check): wraps rather than scrolls, so a phone never scrolls
	// sideways, with a Copy button. Placeholders like <key> stay placeholders: no secret is ever put in a snippet.
	import Icon from '../Icon.svelte';

	let { code, label }: { code: string; label: string } = $props();
	let copied = $state(false);
	let timer: ReturnType<typeof setTimeout> | null = null;

	async function copy() {
		try {
			await navigator.clipboard.writeText(code);
			copied = true;
		} catch {
			copied = false;
		}
		if (timer) clearTimeout(timer);
		timer = setTimeout(() => (copied = false), 2500);
	}
</script>

<figure class="code-block">
	<figcaption>
		<span>{label}</span>
		<button class="btn ghost sm" type="button" onclick={copy}><Icon name={copied ? 'check' : 'copy'} />{copied ? 'Copied' : 'Copy'}<span class="sr"> {label}</span></button>
	</figcaption>
	<pre><code>{code}</code></pre>
	<span class="sr" role="status">{copied ? `${label} copied` : ''}</span>
</figure>
