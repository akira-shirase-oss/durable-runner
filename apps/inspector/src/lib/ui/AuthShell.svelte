<script lang="ts">
	// The signed-out frame: the brand, one centred column for the page's card, the theme switch and the docs link.
	// No sidebar: nothing here needs navigation beyond the links each form carries.
	import type { Snippet } from 'svelte';
	import AppIcon from '../AppIcon.svelte';
	import Icon from '../Icon.svelte';
	import ThemeToggle from './ThemeToggle.svelte';

	let { children }: { children: Snippet } = $props();
</script>

<div class="auth">
	<header class="auth-head">
		<a class="auth-brand" href="#/sign-in"><AppIcon size={28} /><span>Durable Runner</span></a>
		<ThemeToggle />
	</header>
	<main id="main" tabindex="-1" class="auth-main">
		{@render children()}
	</main>
	<footer class="auth-foot">
		<a href="https://docs.durable-runner.404human.dev/" target="_blank" rel="noopener noreferrer"><Icon name="book" size={14} />Docs<span class="sr"> (opens in a new tab)</span></a>
		<a href="https://durable-runner.404human.dev/" target="_blank" rel="noopener noreferrer">About Durable Runner<span class="sr"> (opens in a new tab)</span></a>
	</footer>
</div>

<style>
	.auth {
		display: flex;
		flex-direction: column;
		min-height: 100vh;
		min-height: 100dvh;
		padding: 0 16px;
	}
	.auth-head {
		display: flex;
		align-items: center;
		justify-content: space-between;
		width: 100%;
		max-width: 1100px;
		margin: 0 auto;
		padding: 18px 0;
	}
	.auth-brand {
		display: inline-flex;
		align-items: center;
		gap: 10px;
		color: var(--c-ink);
		font-size: 16px;
		font-weight: 600;
		letter-spacing: -0.015em;
		text-decoration: none;
	}
	.auth-main {
		display: flex;
		flex: 1;
		flex-direction: column;
		align-items: center;
		padding: 4vh 0 40px;
	}
	.auth-main:focus {
		outline: none;
	}
	.auth-foot {
		display: flex;
		flex-wrap: wrap;
		justify-content: center;
		gap: 6px 20px;
		padding: 18px 0 24px;
		font-size: 13.5px;
	}
	.auth-foot a {
		display: inline-flex;
		align-items: center;
		gap: 6px;
		color: var(--c-body);
		text-decoration: none;
	}
	.auth-foot a:hover {
		color: var(--c-ink);
	}
</style>
