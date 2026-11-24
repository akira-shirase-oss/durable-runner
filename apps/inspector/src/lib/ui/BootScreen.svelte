<script lang="ts">
	// Before the mode is known (GET /auth/session in flight), or when it could not be decided: the brand and a
	// status line, or the error with its code and a Retry. Never a guessed mode.
	import AppIcon from '../AppIcon.svelte';
	import ErrorBanner from './ErrorBanner.svelte';
	import type { ApiError } from '../api.ts';

	let { error = null, retry }: { error?: ApiError | null; retry?: () => void } = $props();
</script>

<div class="boot">
	<AppIcon size={40} />
	{#if error}
		<main id="main" tabindex="-1" class="boot-card card">
			<h1>The dashboard cannot reach the server</h1>
			<p class="hint">It asked who is signed in (<code>GET /auth/session</code>) and got no usable answer, so it shows nothing rather than guess.</p>
			<ErrorBanner {error} {retry} />
		</main>
	{:else}
		<p class="boot-status" role="status">Loading Durable Runner…</p>
	{/if}
</div>

<style>
	.boot {
		display: flex;
		flex-direction: column;
		align-items: center;
		justify-content: center;
		gap: 18px;
		min-height: 100vh;
		min-height: 100dvh;
		padding: 24px 16px;
	}
	.boot-status {
		margin: 0;
		color: var(--c-body);
		font-size: 14px;
	}
	.boot-card {
		display: flex;
		flex-direction: column;
		gap: 12px;
		width: 100%;
		max-width: 520px;
		padding: 24px;
	}
	.boot-card:focus {
		outline: none;
	}
	h1 {
		margin: 0;
		font-size: 19px;
		font-weight: 600;
	}
	.hint {
		margin: 0;
		line-height: 1.5;
	}
	code {
		font-family: var(--font-mono);
		font-size: 0.92em;
	}
</style>
