<script lang="ts">
	// API keys (signed in): the workspace's keys — name, scope, prefix, created, last used, revoked — and, for
	// admins and the owner, create (shown once), rotate (the old key dies in the same step) and revoke. Only a
	// key's prefix is ever listed; the value exists once, in the reveal dialog. "Last used" is flushed by the
	// server about once a minute (D-067), so a brand-new key honestly reads "not yet".
	import Icon from '../../lib/Icon.svelte';
	import Confirm from '../../lib/ui/Confirm.svelte';
	import ErrorBanner from '../../lib/ui/ErrorBanner.svelte';
	import Notice from '../../lib/ui/Notice.svelte';
	import RoleNote from '../../lib/ui/RoleNote.svelte';
	import Skeleton from '../../lib/ui/Skeleton.svelte';
	import { account } from '../../lib/account.svelte.ts';
	import { ApiError, createKey, listKeys, revokeKey, rotateKey } from '../../lib/api.ts';
	import { fmtAgo, fmtDateTime } from '../../lib/format.ts';
	import { live, poll, Resource } from '../../lib/live.svelte.ts';
	import { listOf, normKey, type KeyRow, type Scope } from '../../lib/saas.ts';
	import { ui } from '../../lib/ui-state.svelte.ts';
	import { nameError } from '../../lib/validate.ts';

	const pid = account.pid ?? '';
	const res = new Resource((signal) => listKeys(account.cred, pid, signal), { interval: 30_000, live: false });
	poll(res);
	const keys = $derived(res.data ? listOf(res.data, 'keys').map(normKey).filter((k): k is KeyRow => k !== null) : []);
	const liveKeys = $derived(keys.filter((k) => !k.revoked));
	const revoked = $derived(keys.filter((k) => k.revoked));
	const admin = $derived(account.can('admin'));
	const origin = location.origin;

	let name = $state('');
	let scope = $state<Scope>('write');
	let tried = $state(false);
	let busy = $state<string | null>(null);
	let error = $state<ApiError | null>(null);
	let notice = $state<string | null>(null);
	const nameErr = $derived(nameError(name, 'key name'));

	function usage(s: Scope): string {
		return s === 'write'
			? `export DURABLE_KEY='<paste the key>'\ncurl -X POST ${origin}/jobs \\\n  -H "Authorization: Bearer $DURABLE_KEY" \\\n  -H "Content-Type: application/json" \\\n  -d '{"op_key":"hello-1","spec":{"version":1,"steps":[{"name":"echo","handler":"builtin:echo"}],"input_b64":""}}'`
			: `export DURABLE_KEY='<paste the key>'\ncurl ${origin}/jobs?limit=5 -H "Authorization: Bearer $DURABLE_KEY"`;
	}

	async function guard(id: string, fn: () => Promise<void>) {
		busy = id;
		error = null;
		notice = null;
		try {
			await fn();
			await res.refresh(true);
		} catch (e) {
			error = e instanceof ApiError ? e : new ApiError(String(e), 0);
		} finally {
			busy = null;
		}
	}

	function create(e: SubmitEvent) {
		e.preventDefault();
		tried = true;
		if (nameErr || busy) return;
		const n = name.trim();
		void guard('create', async () => {
			const r = await createKey(account.cred, pid, n, scope);
			ui.show({
				title: `API key “${n}” created`,
				label: `${scope === 'write' ? 'Write' : 'Read'} key`,
				value: r.key,
				note: 'Copy it now and keep it in your secret store. Only a fingerprint is kept on the server, so it can never be shown again; rotate the key if it is lost.',
				usage: usage(scope),
			});
			name = '';
			tried = false;
		});
	}

	const rotate = (k: KeyRow) =>
		guard(k.id, async () => {
			const r = await rotateKey(account.cred, pid, k.id);
			ui.show({
				title: `API key “${k.name || k.prefix}” rotated`,
				label: `New ${k.scope} key`,
				value: r.key,
				note: 'The old key stopped working at once. Put this one where the old one was; it is shown only now.',
				usage: usage(k.scope),
			});
		});
	const revoke = (k: KeyRow) =>
		guard(k.id, async () => {
			await revokeKey(account.cred, pid, k.id);
			notice = `Key “${k.name || k.prefix}” revoked. Requests with it are refused from now on.`;
		});
</script>

<div class="page narrow">
	<RoleNote need="admin" what="creating, rotating or revoking keys" />
	{#if notice}<p class="banner notice" role="status"><Icon name="check" size={16} /><span class="msg">{notice}</span><button class="btn ghost sm act" onclick={() => (notice = null)}>Dismiss</button></p>{/if}
	{#if error}<ErrorBanner {error} dismiss={() => (error = null)} />{/if}

	{#if admin}
		<section class="card" aria-labelledby="new-key">
			<div class="card-head">
				<div>
					<h2 id="new-key"><Icon name="plus" size={17} />New API key</h2>
					<p class="sub">For your code and CI. A key works only in {account.current?.name ?? 'this workspace'}; it can never manage members, keys or the workspace itself.</p>
				</div>
			</div>
			<div class="card-body">
				<form class="form" onsubmit={create} novalidate>
					<div class="field">
						<label class="field-label" for="key-name">Name</label>
						<input id="key-name" bind:value={name} maxlength="128" autocomplete="off" placeholder="e.g. ci-deploy" aria-invalid={tried && nameErr ? 'true' : undefined} aria-describedby="key-name-hint" />
						<p class="field-hint" class:bad={tried && !!nameErr} id="key-name-hint">{tried && nameErr ? nameErr : 'Say where it is used, so you know what breaks when you revoke it.'}</p>
					</div>
					<fieldset class="choices">
						<legend>Scope</legend>
						<label class="choice"><input type="radio" name="scope" value="write" bind:group={scope} /><span>Write<small>Submit and cancel jobs, manage handlers, send test calls, and read.</small></span></label>
						<label class="choice"><input type="radio" name="scope" value="read" bind:group={scope} /><span>Read<small>Runs, timelines, metrics and handlers. Changes nothing.</small></span></label>
					</fieldset>
					<div class="form-actions">
						<button class="btn brand sm" type="submit" disabled={busy !== null}><Icon name="key" />{busy === 'create' ? 'Creating…' : 'Create key'}</button>
					</div>
				</form>
			</div>
		</section>
	{/if}

	<section class="card" aria-labelledby="keys-title">
		<div class="card-head ruled">
			<div>
				<h2 id="keys-title"><Icon name="key" size={17} />Keys</h2>
				<p class="sub">{res.data ? `${liveKeys.length} live${revoked.length ? `, ${revoked.length} revoked` : ''}` : 'Only the prefix is shown; the value was shown once, at creation.'}</p>
			</div>
		</div>
		{#if res.status === 'unsupported' || res.status === 'needs-key' || (res.status === 'error' && !res.data)}
			<div class="card-body"><Notice status={res.status} error={res.error} what="The key list" route="GET /workspaces/{'{pid}'}/keys" retry={() => res.refresh(true)} /></div>
		{:else if res.loading}
			<Skeleton label="Loading keys" />
		{:else if keys.length === 0}
			<p class="empty">No API keys yet.{admin ? ' Create one above to send jobs from your code.' : ' An admin can create one.'}</p>
		{:else}
			<ul class="rows">
				{#each [...liveKeys, ...revoked] as k (k.id)}
					<li class:dim={k.revoked}>
						<div class="row-main">
							<p class="row-title">
								{k.name || 'Unnamed key'}
								<span class="badge {k.scope === 'write' ? 'warn' : ''}">{k.scope === 'write' ? 'Write' : 'Read'}</span>
								{#if k.revoked}<span class="badge bad">Revoked</span>{/if}
							</p>
							<p class="row-meta">
								<span class="mono">{k.prefix ? `${k.prefix}…` : '—'}</span>
								{#if k.createdAt || k.createdBy}
									<span title={k.createdAt ? fmtDateTime(k.createdAt, live.now) : ''}>Created{k.createdAt ? ` ${fmtAgo(live.now - k.createdAt)}` : ''}{k.createdBy ? (k.createdBy === account.me?.public_id ? ' by you' : ' by a teammate') : ''}</span>
								{/if}
								<span>Last used {k.lastUsedAt ? fmtAgo(live.now - k.lastUsedAt) : 'not yet'}</span>
							</p>
						</div>
						{#if admin && !k.revoked}
							<div class="row-actions">
								<Confirm label="Rotate" icon="rotate" tone="plain" question="Rotate “{k.name || k.prefix}”? The old key stops working at once." confirmLabel="Rotate" busy={busy === k.id} onconfirm={() => rotate(k)} />
								<Confirm label="Revoke" question="Revoke “{k.name || k.prefix}”? Requests with it are refused from now on." confirmLabel="Revoke" busy={busy === k.id} onconfirm={() => revoke(k)} />
							</div>
						{/if}
					</li>
				{/each}
			</ul>
		{/if}
	</section>
</div>

<style>
	.narrow {
		max-width: 960px;
	}
</style>
