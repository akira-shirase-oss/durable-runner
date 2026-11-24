<script lang="ts">
	// An unknown hash route (#/nowhere) or a page that failed to load.
	import { page } from '$app/state';
	import Icon from '../lib/Icon.svelte';
</script>

<div class="page">
	<section class="card lost">
		<p class="code">{page.status}</p>
		<h2>{page.status === 404 ? 'No such page in the inspector' : 'This page failed to load'}</h2>
		<p class="hint">{page.error?.message ?? ''}</p>
		<a class="btn" href="#/"><Icon name="arrow-left" />Back to the overview</a>
	</section>
</div>

<style>
	.lost {
		display: flex;
		flex-direction: column;
		align-items: flex-start;
		gap: 8px;
		padding: 28px;
	}
	.code {
		margin: 0;
		color: var(--c-brand-deep);
		font-family: var(--font-mono);
		font-size: 14px;
		font-weight: 600;
	}
	.lost .hint {
		margin: 0 0 8px;
	}
	.lost a {
		text-decoration: none;
	}
</style>
