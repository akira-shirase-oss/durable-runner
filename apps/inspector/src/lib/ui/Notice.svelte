<script lang="ts">
	// The one banner for a resource that could not load. Compat mode: a missing key (with a link to Settings), a
	// route this server does not have (an older durable-server), or an error with the server's message. Signed
	// in: the refusal's code in words (a viewer's 403, an unverified account), a route this server does not serve
	// yet (the S3 routes land slice by slice), or an error with its code and a Retry.
	import Icon from '../Icon.svelte';
	import ErrorBanner from './ErrorBanner.svelte';
	import type { ApiError } from '../api.ts';
	import { fmtAgo } from '../format.ts';
	import { live, type ResourceStatus } from '../live.svelte.ts';
	import { session } from '../session.svelte.ts';

	let {
		status,
		error,
		what,
		route,
		needsApi = false,
		stale = null,
		retry,
	}: {
		status: ResourceStatus;
		error: ApiError | null;
		what: string;
		route: string;
		needsApi?: boolean;
		/** when the data on screen was last refreshed: an error then keeps it, and says so */
		stale?: number | null;
		retry?: () => void;
	} = $props();
</script>

{#if status === 'needs-key'}
	{#if session.signedIn && error}
		<ErrorBanner {error} {what} />
	{:else}
		<p class="banner hint">
			<Icon name="key" size={16} />
			<span class="msg">
				{#if needsApi || error?.status === 403}{what} needs the API key.{:else}{what} needs a key: the read key (or the API key).{/if}
				<a href="#/settings">Add it in Settings</a>
			</span>
		</p>
	{/if}
{:else if status === 'unsupported'}
	{#if session.signedIn}
		<p class="banner warn">
			<Icon name="server" size={16} />
			<span class="msg"><strong>This server does not serve <code>{route}</code> yet.</strong> It answered 404 with no error code: the route has not been deployed here. {what} will show here once it is.</span>
		</p>
	{:else}
		<p class="banner warn">
			<Icon name="server" size={16} />
			<span class="msg"><strong>This server does not serve <code>{route}</code>.</strong> It answered 404: an older durable-server, from before the observability API. Upgrade the server to see {what.charAt(0).toLowerCase() + what.slice(1)} here; everything else works as before.</span>
		</p>
	{/if}
{:else if status === 'error' && error && stale !== null}
	<p class="banner warn">
		<Icon name="history" size={16} />
		<span class="msg"><strong>Showing data from {fmtAgo(live.now - stale)}.</strong> {what} did not refresh: {error.message}{error.status ? ` (${error.status})` : ''}. Retrying{live.retryAt ? ` in ${Math.max(1, Math.ceil((live.retryAt - live.now) / 1000))} s` : ''}.</span>
	</p>
{:else if status === 'error' && error}
	{#if session.signedIn}
		<ErrorBanner {error} {what} {retry} />
	{:else}
		<p class="banner error"><Icon name="alert" size={16} /><span class="msg">{what}: {error.message}{error.status ? ` (${error.status})` : ''}</span></p>
	{/if}
{/if}

<style>
	code {
		font-family: var(--font-mono);
		font-size: 0.92em;
	}
</style>
