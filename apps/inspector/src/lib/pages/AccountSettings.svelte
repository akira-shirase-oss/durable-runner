<script lang="ts">
	// Settings (signed in). The workspace: rename (admin and above), delete (owner, with the confirm phrase and what
	// deleting does and does not do — D-068). Personal: password change (re-enter the current one; every session
	// is signed out and this one renewed), the sessions list with revoke and "sign out everywhere else", theme.
	import Icon from '../Icon.svelte';
	import Confirm from '../ui/Confirm.svelte';
	import ErrorBanner from '../ui/ErrorBanner.svelte';
	import Notice from '../ui/Notice.svelte';
	import PasswordField from '../ui/PasswordField.svelte';
	import Skeleton from '../ui/Skeleton.svelte';
	import ThemeChoice from '../ui/ThemeChoice.svelte';
	import { goto } from '$app/navigation';
	import { account } from '../account.svelte.ts';
	import { ApiError, changePassword, deleteWorkspace, meSessions, renameWorkspace, revokeOtherSessions, revokeSession } from '../api.ts';
	import { fmtAgo, fmtDateTime } from '../format.ts';
	import { live, poll, Resource } from '../live.svelte.ts';
	import { deviceName, listOf, normSession, ROLE_LABEL, type SessionRow } from '../saas.ts';
	import { theme } from '../theme.svelte.ts';
	import { deletePhrase, nameError, passwordError } from '../validate.ts';

	const ws = $derived(account.current);
	const sessionsRes = new Resource((signal) => meSessions(account.cred, signal), { interval: 30_000, live: false });
	poll(sessionsRes);
	const sessions = $derived(sessionsRes.data ? listOf(sessionsRes.data, 'sessions').map(normSession).filter((s): s is SessionRow => s !== null) : []);
	const others = $derived(sessions.filter((s) => !s.current).length);

	let busy = $state<string | null>(null);
	let error = $state<ApiError | null>(null);
	let notice = $state<string | null>(null);
	async function guard(id: string, fn: () => Promise<void>) {
		busy = id;
		error = null;
		notice = null;
		try {
			await fn();
		} catch (e) {
			error = e instanceof ApiError ? e : new ApiError(String(e), 0);
		} finally {
			busy = null;
		}
	}

	// rename
	let name = $state(account.current?.name ?? '');
	const nameErr = $derived(nameError(name, 'workspace name'));
	function rename(e: SubmitEvent) {
		e.preventDefault();
		if (!ws || nameErr || name.trim() === ws.name) return;
		const pid = ws.public_id;
		const n = name.trim();
		void guard('rename', async () => {
			await renameWorkspace(account.cred, pid, n);
			await account.refresh();
			notice = `Workspace renamed to ${n}.`;
		});
	}

	// delete
	let phrase = $state('');
	const want = $derived(ws ? deletePhrase(ws.name) : '');
	const del = () => {
		if (!ws || phrase.trim() !== want) return;
		const pid = ws.public_id;
		return guard('delete', async () => {
			await deleteWorkspace(account.cred, pid, want);
			await account.refresh();
			void goto('#/overview');
		});
	};

	// password
	let current = $state('');
	let next = $state('');
	let tried = $state(false);
	const nextErr = $derived(passwordError(next));
	function change(e: SubmitEvent) {
		e.preventDefault();
		tried = true;
		if (!current || nextErr || busy) return;
		void guard('password', async () => {
			const r = await changePassword(account.cred, current, next);
			account.setCsrf(r?.csrf);
			current = '';
			next = '';
			tried = false;
			notice = 'Password changed. Every other session on this account was signed out; this one was renewed.';
			void sessionsRes.refresh(true);
		});
	}

	const revokeOne = (s: SessionRow) =>
		guard(s.id, async () => {
			await revokeSession(account.cred, s.id);
			notice = `Signed out ${deviceName(s.userAgent)}.`;
			await sessionsRes.refresh(true);
		});
	const revokeAll = () =>
		guard('all', async () => {
			const r = await revokeOtherSessions(account.cred);
			notice = `Signed out everywhere else${typeof r?.revoked === 'number' ? ` (${r.revoked} session${r.revoked === 1 ? '' : 's'})` : ''}.`;
			await sessionsRes.refresh(true);
		});
</script>

<div class="page narrow">
	{#if notice}<p class="banner notice" role="status"><Icon name="check" size={16} /><span class="msg">{notice}</span><button class="btn ghost sm act" onclick={() => (notice = null)}>Dismiss</button></p>{/if}
	{#if error}<ErrorBanner {error} dismiss={() => (error = null)} />{/if}

	{#if ws}
		<section class="card" aria-labelledby="ws-title">
			<div class="card-head ruled">
				<div>
					<h2 id="ws-title"><Icon name="box" size={17} />Workspace</h2>
					<p class="sub">You are {ws.role === 'admin' || ws.role === 'owner' ? 'the' : 'a'} {ROLE_LABEL[ws.role].toLowerCase()} here · {ws.plan.charAt(0).toUpperCase() + ws.plan.slice(1)} plan</p>
				</div>
			</div>
			<div class="card-body stack">
				<dl class="kv">
					<div><dt>Name</dt><dd>{ws.name}</dd></div>
					<div><dt>Workspace id</dt><dd class="mono">{ws.public_id}</dd></div>
				</dl>
				{#if account.can('admin')}
					<form class="form" onsubmit={rename} novalidate>
						<div class="field">
							<label class="field-label" for="ws-name">Rename</label>
							<div class="input-with-action">
								<input id="ws-name" bind:value={name} maxlength="128" autocomplete="off" aria-invalid={nameErr ? 'true' : undefined} aria-describedby={nameErr ? 'ws-name-err' : undefined} />
								<button class="btn" type="submit" disabled={busy !== null || !!nameErr || name.trim() === ws.name}>{busy === 'rename' ? 'Saving…' : 'Save'}</button>
							</div>
							{#if nameErr}<p class="field-hint bad" id="ws-name-err">{nameErr}</p>{/if}
						</div>
					</form>
				{:else}
					<p class="muted">Renaming needs the admin or owner role.</p>
				{/if}
			</div>
		</section>

		{#if account.can('owner')}
			<section class="card danger-zone" aria-labelledby="del-title">
				<div class="card-head">
					<div>
						<h2 id="del-title"><Icon name="trash" size={17} />Delete this workspace</h2>
						<p class="sub">Only the owner can.</p>
					</div>
				</div>
				<div class="card-body stack">
					<ul class="facts">
						<li>New jobs are refused and every API key of the workspace stops working at once; members lose access.</li>
						<li>Queued runs are cancelled. A step already sent to a handler cannot be undone.</li>
						<li>Finished run records are not erased on the spot: they expire like all finished history (7 days on the Free plan).</li>
						<li>The workspace itself is kept for 7 days in case this was a mistake, then purged.</li>
					</ul>
					<div class="field">
						<label class="field-label" for="del-phrase">Type <span class="mono">{want}</span> to confirm</label>
						<input id="del-phrase" bind:value={phrase} autocomplete="off" spellcheck="false" />
					</div>
					<div>
						<Confirm label="Delete workspace" question="Delete {ws.name} now?" confirmLabel="Delete" disabled={phrase.trim() !== want} busy={busy === 'delete'} onconfirm={() => del()} />
					</div>
				</div>
			</section>
		{/if}
	{/if}

	<section class="card" aria-labelledby="me-title">
		<div class="card-head ruled">
			<div>
				<h2 id="me-title"><Icon name="user" size={17} />Your account</h2>
				<p class="sub">Personal: the same in every workspace.</p>
			</div>
		</div>
		<div class="card-body stack">
			<dl class="kv">
				<div><dt>Email</dt><dd>{account.me?.email} {#if account.verified}<span class="badge ok">Verified</span>{:else}<span class="badge warn">Not verified</span>{/if}</dd></div>
			</dl>
			<form class="form" onsubmit={change} novalidate aria-labelledby="pw-title">
				<h3 id="pw-title">Change password</h3>
				<input type="text" name="username" autocomplete="username" value={account.me?.email ?? ''} hidden />
				<PasswordField id="pw-current" label="Current password" bind:value={current} autocomplete="current-password" error={tried && !current ? 'Enter your current password' : null} />
				<PasswordField id="pw-next" label="New password" bind:value={next} autocomplete="new-password" hint="At least 12 characters." error={tried ? nextErr : null} />
				<div class="form-actions">
					<button class="btn sm" type="submit" disabled={busy !== null}><Icon name="lock" />{busy === 'password' ? 'Changing…' : 'Change password'}</button>
				</div>
			</form>
		</div>
	</section>

	<section class="card" aria-labelledby="sess-title">
		<div class="card-head ruled">
			<div>
				<h2 id="sess-title"><Icon name="monitor" size={17} />Sessions</h2>
				<p class="sub">Where this account is signed in. Sessions end after 14 idle days, and after 30 days at most.</p>
			</div>
			{#if others > 0}
				<Confirm label="Sign out everywhere else" icon="log-out" question="Sign out the {others} other session{others === 1 ? '' : 's'}?" confirmLabel="Sign them out" busy={busy === 'all'} onconfirm={revokeAll} />
			{/if}
		</div>
		{#if sessionsRes.status === 'unsupported' || sessionsRes.status === 'needs-key' || (sessionsRes.status === 'error' && !sessionsRes.data)}
			<div class="card-body"><Notice status={sessionsRes.status} error={sessionsRes.error} what="The sessions list" route="GET /me/sessions" retry={() => sessionsRes.refresh(true)} /></div>
		{:else if sessionsRes.loading}
			<Skeleton label="Loading sessions" rows={2} />
		{:else}
			<ul class="rows">
				{#each sessions as s (s.id)}
					<li>
						<div class="row-main">
							<p class="row-title">{deviceName(s.userAgent)}{#if s.current}<span class="badge ok">This browser</span>{/if}</p>
							<p class="row-meta">
								{#if s.ip}<span class="mono">{s.ip}</span>{/if}
								<span title={s.createdAt ? fmtDateTime(s.createdAt, live.now) : ''}>Signed in {s.createdAt ? fmtAgo(live.now - s.createdAt) : '—'}</span>
								<span>Last seen {s.lastSeenAt ? fmtAgo(live.now - s.lastSeenAt) : '—'}</span>
							</p>
						</div>
						{#if !s.current}
							<div class="row-actions"><Confirm label="Sign out" icon="log-out" question="Sign out {deviceName(s.userAgent)}?" confirmLabel="Sign out" busy={busy === s.id} onconfirm={() => revokeOne(s)} /></div>
						{/if}
					</li>
				{/each}
			</ul>
		{/if}
	</section>

	<section class="card" aria-labelledby="theme-title">
		<div class="card-head">
			<div>
				<h2 id="theme-title"><Icon name={theme.scheme === 'dark' ? 'moon' : 'sun'} size={17} />Theme</h2>
				<p class="sub">System follows your device{theme.choice === 'system' ? `, which is ${theme.systemDark ? 'dark' : 'light'} now` : ''}. Stored in this browser.</p>
			</div>
		</div>
		<div class="card-body"><ThemeChoice labelledby="theme-title" /></div>
	</section>
</div>

<style>
	.narrow {
		max-width: 860px;
	}
	.stack {
		display: flex;
		flex-direction: column;
		gap: 18px;
	}
	.stack h3 {
		margin: 0;
	}
	.facts {
		display: flex;
		flex-direction: column;
		gap: 6px;
		margin: 0;
		padding-left: 18px;
		color: var(--c-ink-soft);
		font-size: 14px;
		line-height: 1.5;
	}
	.danger-zone {
		box-shadow:
			0 0 0 1px var(--c-danger-edge),
			0 1px 2px var(--c-tile-shadow);
	}
</style>
