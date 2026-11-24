<script lang="ts">
	// One refusal, honestly: the sentence for its code (http.ts), the plan limit with its numbers, the code itself
	// (so a report can quote it), and Retry when trying again can help.
	import Icon from '../Icon.svelte';
	import type { ApiError } from '../api.ts';
	import { copyFor, planLimitCopy } from '../http.ts';

	let {
		error,
		what = '',
		retry,
		dismiss,
	}: {
		error: ApiError;
		what?: string;
		retry?: () => void;
		dismiss?: () => void;
	} = $props();

	const text = $derived(error.code === 'plan_limit' ? planLimitCopy(error.extra, error.retryAfterMs) : copyFor(error.code, error.message));
	const tone = $derived(error.code === 'plan_limit' || error.status === 403 ? 'warn' : 'error');
</script>

<p class="banner {tone}" role="alert">
	<Icon name={tone === 'warn' ? 'lock' : 'alert'} size={16} />
	<span class="msg">{what ? `${what}: ` : ''}{text}{#if error.code}{' '}<span class="code-tag">{error.code}</span>{:else if error.status}{' '}<span class="code-tag">HTTP {error.status}</span>{/if}</span>
	{#if retry}<button class="btn ghost sm act" type="button" onclick={retry}><Icon name="rotate" />Retry</button>{/if}
	{#if dismiss}<button class="btn ghost sm act" type="button" onclick={dismiss}>Dismiss</button>{/if}
</p>
