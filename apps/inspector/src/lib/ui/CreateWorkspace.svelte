<script lang="ts">
	// Create a workspace: a name, then POST /workspaces. The answer carries the workspace's handler signing secret,
	// the one time it is ever shown (D-069) — the shell's reveal dialog holds it until it is copied. The new
	// workspace becomes this tab's selection.
	import Icon from '../Icon.svelte';
	import ErrorBanner from './ErrorBanner.svelte';
	import { account } from '../account.svelte.ts';
	import { ApiError, createWorkspace } from '../api.ts';
	import { ui } from '../ui-state.svelte.ts';
	import { nameError } from '../validate.ts';
	import { goto } from '$app/navigation';

	let { idPrefix = 'cw', oncreated }: { idPrefix?: string; oncreated?: () => void } = $props();

	let name = $state('');
	let touched = $state(false);
	let busy = $state(false);
	let error = $state<ApiError | null>(null);
	const nameErr = $derived(nameError(name, 'workspace name'));

	async function submit(e: SubmitEvent) {
		e.preventDefault();
		touched = true;
		if (nameErr || busy) return;
		busy = true;
		error = null;
		try {
			const r = await createWorkspace(account.cred, name.trim());
			await account.refresh();
			if (r.public_id) account.select(r.public_id);
			oncreated?.();
			if (r.handler_secret) {
				ui.show({
					title: `${r.name ?? name.trim()} is ready`,
					label: 'Handler signing secret',
					value: r.handler_secret,
					note: 'The runner signs every call to your handlers with this secret. Copy it into your handler now: it is shown this once, and rotating it on the Handlers page is the only way to see a new one.',
				});
			}
			name = '';
			touched = false;
			void goto('#/overview');
		} catch (err) {
			error = err instanceof ApiError ? err : new ApiError(String(err), 0);
		} finally {
			busy = false;
		}
	}
</script>

<form class="form" onsubmit={submit} novalidate>
	{#if error}<ErrorBanner {error} />{/if}
	<div class="field">
		<label class="field-label" for="{idPrefix}-name">Workspace name</label>
		<input
			id="{idPrefix}-name"
			bind:value={name}
			autocomplete="off"
			maxlength="128"
			placeholder="e.g. Production"
			aria-invalid={touched && nameErr ? 'true' : undefined}
			aria-describedby="{idPrefix}-name-hint"
			onblur={() => (touched = name !== '' || touched)}
		/>
		<p class="field-hint" id="{idPrefix}-name-hint" class:bad={touched && !!nameErr}>{touched && nameErr ? nameErr : 'Runs, handlers, keys and members belong to one workspace. You can rename it later.'}</p>
	</div>
	<div class="form-actions">
		<button class="btn brand sm" type="submit" disabled={busy}><Icon name="plus" />{busy ? 'Creating…' : 'Create workspace'}</button>
	</div>
</form>
