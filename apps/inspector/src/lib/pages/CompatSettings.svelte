<script lang="ts">
	// Compat settings (no account store): the two keys this tab holds (sessionStorage only, never rendered back: D-053), Forget keys, and the
	// theme (follow the device, or a fixed Light or Dark).
	import Icon from '../Icon.svelte';
	import { session } from '../session.svelte.ts';
	import { theme } from '../theme.svelte.ts';
	import type { Choice } from '../theme.ts';
	import type { KeyKind } from '../api.ts';

	let readDraft = $state('');
	let apiDraft = $state('');
	let saved = $state<string | null>(null);

	function save(kind: KeyKind, e: SubmitEvent) {
		e.preventDefault();
		const v = kind === 'read' ? readDraft : apiDraft;
		session.setKey(kind, v);
		if (kind === 'read') readDraft = '';
		else apiDraft = '';
		saved = v.trim() ? `${kind === 'read' ? 'Read key' : 'API key'} saved for this tab` : `${kind === 'read' ? 'Read key' : 'API key'} cleared`;
	}

	function clear(kind: KeyKind) {
		session.setKey(kind, '');
		saved = `${kind === 'read' ? 'Read key' : 'API key'} cleared`;
	}

	const THEMES: { id: Choice; label: string }[] = [
		{ id: 'system', label: 'System' },
		{ id: 'light', label: 'Light' },
		{ id: 'dark', label: 'Dark' },
	];
	let themeButtons: HTMLButtonElement[] = $state([]);
	function themeKey(e: KeyboardEvent, i: number) {
		let j = i;
		if (e.key === 'ArrowRight' || e.key === 'ArrowDown') j = (i + 1) % THEMES.length;
		else if (e.key === 'ArrowLeft' || e.key === 'ArrowUp') j = (i - 1 + THEMES.length) % THEMES.length;
		else return;
		e.preventDefault();
		theme.set(THEMES[j]!.id);
		themeButtons[j]?.focus();
	}
</script>

<div class="page narrow">
	{#if saved}
		<p class="banner notice" role="status"><Icon name="check" size={16} /><span class="msg">{saved}</span><button class="btn ghost sm act" onclick={() => (saved = null)}>Dismiss</button></p>
	{/if}
	{#if session.notice}
		<p class="banner notice"><Icon name="check" size={16} /><span class="msg">{session.notice}</span><button class="btn ghost sm act" onclick={() => (session.notice = null)}>Dismiss</button></p>
	{/if}

	<section class="card" aria-labelledby="keys-title">
		<div class="card-head ruled">
			<div>
				<h2 id="keys-title"><Icon name="key" size={17} />Keys</h2>
				<p class="sub">Held in this tab's session storage only, never shown again, sent only to this server.</p>
			</div>
			<button class="btn danger sm" disabled={!session.readKey && !session.apiKey} onclick={() => session.forget()}><Icon name="log-out" />Forget keys</button>
		</div>
		<div class="card-body keys">
			<form class="key" onsubmit={(e) => save('read', e)}>
				<div class="key-head">
					<label for="read-key" class="field-label">Read key</label>
					<em class="badge" class:set={!!session.readKey}>{session.readKey ? 'set' : 'not set'}</em>
				</div>
				<p class="field-hint">Views: the overview, runs, timelines and handlers. <span class="mono">DURABLE_READ_KEY</span> on the server.</p>
				<div class="key-row">
					<input id="read-key" type="password" autocomplete="off" placeholder={session.readKey ? 'Replace the read key' : 'Paste the read key'} bind:value={readDraft} />
					<button class="btn" type="submit" disabled={!readDraft.trim()}>Save</button>
					{#if session.readKey}<button class="btn ghost" type="button" onclick={() => clear('read')}>Clear</button>{/if}
				</div>
			</form>
			<form class="key" onsubmit={(e) => save('api', e)}>
				<div class="key-head">
					<label for="api-key" class="field-label">API key</label>
					<em class="badge" class:set={!!session.apiKey}>{session.apiKey ? 'set' : 'not set'}</em>
				</div>
				<p class="field-hint">Everything else: submit, cancel, reveal a spec, diagnostics, compact and backup. It can also read. <span class="mono">DURABLE_API_KEY</span>.</p>
				<div class="key-row">
					<input id="api-key" type="password" autocomplete="off" placeholder={session.apiKey ? 'Replace the API key' : 'Paste the API key'} bind:value={apiDraft} />
					<button class="btn" type="submit" disabled={!apiDraft.trim()}>Save</button>
					{#if session.apiKey}<button class="btn ghost" type="button" onclick={() => clear('api')}>Clear</button>{/if}
				</div>
			</form>
		</div>
	</section>

	<section class="card" aria-labelledby="theme-title">
		<div class="card-head">
			<div>
				<h2 id="theme-title"><Icon name={theme.scheme === 'dark' ? 'moon' : 'sun'} size={17} />Theme</h2>
				<p class="sub">System follows your device{theme.choice === 'system' ? `, which is ${theme.systemDark ? 'dark' : 'light'} now` : ''}. Stored in this browser.</p>
			</div>
		</div>
		<div class="card-body">
			<div class="seg" role="radiogroup" aria-labelledby="theme-title">
				{#each THEMES as t, i (t.id)}
					<button
						bind:this={themeButtons[i]}
						role="radio"
						aria-checked={theme.choice === t.id}
						tabindex={theme.choice === t.id ? 0 : -1}
						onclick={() => theme.set(t.id)}
						onkeydown={(e) => themeKey(e, i)}>{t.label}</button
					>
				{/each}
			</div>
		</div>
	</section>

	<section class="card" aria-labelledby="about-title">
		<div class="card-head">
			<h2 id="about-title"><Icon name="book" size={17} />About this page</h2>
		</div>
		<div class="card-body about">
			<p>Live pages refresh every 5 seconds while this tab is visible, one request per view at a time, and back off when the server is slow or refuses (honouring <span class="mono">Retry-After</span>). Pause them with the button beside “Live”.</p>
			<p>Charts come from the server's in-memory metrics (<span class="mono">GET /metrics</span>): bounded, rebuilt from the journal's events at start, never authoritative. <a href="https://docs.durable-runner.404human.dev/inspector/" target="_blank" rel="noopener noreferrer">Read the inspector docs<span class="sr"> (opens in a new tab)</span></a></p>
		</div>
	</section>
</div>

<style>
	.narrow {
		max-width: 820px;
	}
	.keys {
		display: flex;
		flex-direction: column;
		gap: 22px;
	}
	.key {
		display: flex;
		flex-direction: column;
		gap: 6px;
	}
	.key-head {
		display: flex;
		align-items: center;
		gap: 8px;
	}
	.key-head label {
		color: var(--c-ink);
		font-size: 14.5px;
	}
	.key-row {
		display: flex;
		gap: 8px;
		margin-top: 4px;
	}
	.key-row input {
		flex: 1;
	}
	.key-row .btn {
		height: 40px;
	}
	.about p {
		margin: 0 0 10px;
		color: var(--c-body);
		font-size: 14px;
		line-height: 1.55;
	}
	.about p:last-child {
		margin-bottom: 0;
	}
	.about a {
		color: var(--c-brand-deep);
		text-underline-offset: 3px;
	}
	@media (max-width: 600px) {
		.key-row {
			flex-wrap: wrap;
		}
		.key-row input {
			flex-basis: 100%;
		}
	}
</style>
