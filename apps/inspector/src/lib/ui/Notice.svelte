<script lang="ts">
	// The one banner for a resource that could not load: a missing key (with a link to Settings), a route this
	// server does not have (an older durable-server), or an error with the server's message.
	import Icon from '../Icon.svelte';
	import type { ApiError } from '../api.ts';
	import { fmtAgo } from '../format.ts';
	import { live, type ResourceStatus } from '../live.svelte.ts';

	let {
		status,
		error,
		what,
		route,
		needsApi = false,
		stale = null,
	}: {
		status: ResourceStatus;
		error: ApiError | null;
		what: string;
		route: string;
		needsApi?: boolean;
		/** when the data on screen was last refreshed: an error then keeps it, and says so */
		stale?: number | null;
	} = $props();
</script>

{#if status === 'needs-key'}
	<p class="banner hint">
		<Icon name="key" size={16} />
		<span class="msg">
			{#if needsApi || error?.status === 403}{what} needs the API key.{:else}{what} needs a key: the read key (or the API key).{/if}
			<a href="#/settings">Add it in Settings</a>
		</span>
	</p>
{:else if status === 'unsupported'}
	<p class="banner warn">
		<Icon name="server" size={16} />
		<span class="msg"><strong>This server does not serve <code>{route}</code>.</strong> It answered 404: an older durable-server, from before the observability API. Upgrade the server to see {what.charAt(0).toLowerCase() + what.slice(1)} here; everything else works as before.</span>
	</p>
{:else if status === 'error' && error && stale !== null}
	<p class="banner warn">
		<Icon name="history" size={16} />
		<span class="msg"><strong>Showing data from {fmtAgo(live.now - stale)}.</strong> {what} did not refresh: {error.message}{error.status ? ` (${error.status})` : ''}. Retrying{live.retryAt ? ` in ${Math.max(1, Math.ceil((live.retryAt - live.now) / 1000))} s` : ''}.</span>
	</p>
{:else if status === 'error' && error}
	<p class="banner error"><Icon name="alert" size={16} /><span class="msg">{what}: {error.message}{error.status ? ` (${error.status})` : ''}</span></p>
{/if}

<style>
	code {
		font-family: var(--font-mono);
		font-size: 0.92em;
	}
</style>
