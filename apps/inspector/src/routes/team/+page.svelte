<script lang="ts">
	// Team (signed in): members (email, role, since), pending invites, and — for admins and the owner — invite by
	// email + role, change a role, remove a member, revoke an invite. Members join only through an invite the
	// invited address accepts (never a silent add); the owner row cannot be removed or demoted. The invite link is
	// mailed and also shown once here, for sharing out of band.
	import Icon from '../../lib/Icon.svelte';
	import Confirm from '../../lib/ui/Confirm.svelte';
	import ErrorBanner from '../../lib/ui/ErrorBanner.svelte';
	import Notice from '../../lib/ui/Notice.svelte';
	import RoleNote from '../../lib/ui/RoleNote.svelte';
	import Skeleton from '../../lib/ui/Skeleton.svelte';
	import { account } from '../../lib/account.svelte.ts';
	import { ApiError, changeRole, inviteMember, listInvites, listMembers, removeMember, revokeInvite } from '../../lib/api.ts';
	import { fmtAgo, fmtDateTime } from '../../lib/format.ts';
	import { live, poll, Resource } from '../../lib/live.svelte.ts';
	import { listOf, normInvite, normMember, ROLE_HINT, ROLE_LABEL, type InviteRow, type MemberRow, type Role } from '../../lib/saas.ts';
	import { ui } from '../../lib/ui-state.svelte.ts';
	import { emailError } from '../../lib/validate.ts';

	const pid = account.pid ?? '';
	const membersRes = new Resource((signal) => listMembers(account.cred, pid, signal), { interval: 30_000, live: false });
	poll(membersRes);
	const invitesRes = new Resource((signal) => listInvites(account.cred, pid, signal), { interval: 30_000, live: false });
	poll(invitesRes);
	const members = $derived(membersRes.data ? listOf(membersRes.data, 'members').map(normMember).filter((m): m is MemberRow => m !== null) : []);
	const invites = $derived(invitesRes.data ? listOf(invitesRes.data, 'invites').map(normInvite).filter((i): i is InviteRow => i !== null) : []);
	const admin = $derived(account.can('admin'));
	const me = $derived(account.me?.public_id ?? '');
	const ORDER: Record<Role, number> = { owner: 0, admin: 1, member: 2, viewer: 3 };
	const sorted = $derived([...members].sort((a, b) => ORDER[a.role] - ORDER[b.role] || a.email.localeCompare(b.email)));
	const INVITE_ROLES: Role[] = ['admin', 'member', 'viewer'];

	let busy = $state<string | null>(null);
	let error = $state<ApiError | null>(null);
	let notice = $state<string | null>(null);
	async function guard(id: string, fn: () => Promise<void>) {
		busy = id;
		error = null;
		notice = null;
		try {
			await fn();
			await Promise.all([membersRes.refresh(true), invitesRes.refresh(true)]);
		} catch (e) {
			error = e instanceof ApiError ? e : new ApiError(String(e), 0);
		} finally {
			busy = null;
		}
	}

	let email = $state('');
	let role = $state<Role>('member');
	let tried = $state(false);
	const emailErr = $derived(emailError(email));
	function invite(e: SubmitEvent) {
		e.preventDefault();
		tried = true;
		if (emailErr || busy) return;
		const to = email.trim();
		const r = role;
		void guard('invite', async () => {
			const res = await inviteMember(account.cred, pid, to, r);
			if (res?.invite_url) {
				ui.show({
					title: `Invite sent to ${to}`,
					label: 'Invite link',
					value: res.invite_url,
					note: `It is on its way by mail. You can also share this link with ${to} yourself: it works once, for 14 days, and only for an account with that address.`,
				});
			} else notice = `Invite sent to ${to}.`;
			email = '';
			tried = false;
		});
	}

	const setRole = (m: MemberRow, r: Role) =>
		guard(m.accountId, async () => {
			await changeRole(account.cred, pid, m.accountId, r);
			notice = `${m.email} is now ${r === 'admin' ? 'an' : 'a'} ${ROLE_LABEL[r].toLowerCase()}. Their sessions renew with the new role.`;
		});
	const remove = (m: MemberRow) =>
		guard(m.accountId, async () => {
			await removeMember(account.cred, pid, m.accountId);
			notice = `${m.email} no longer has access to ${account.current?.name ?? 'this workspace'}.`;
		});
	const revoke = (i: InviteRow) =>
		guard(i.id, async () => {
			await revokeInvite(account.cred, pid, i.id);
			notice = `The invite to ${i.email} is revoked; its link no longer works.`;
		});
</script>

<div class="page narrow">
	<RoleNote need="admin" what="inviting, changing roles or removing members" />
	{#if notice}<p class="banner notice" role="status"><Icon name="check" size={16} /><span class="msg">{notice}</span><button class="btn ghost sm act" onclick={() => (notice = null)}>Dismiss</button></p>{/if}
	{#if error}<ErrorBanner {error} dismiss={() => (error = null)} />{/if}

	{#if admin}
		<section class="card" aria-labelledby="invite-title">
			<div class="card-head">
				<div>
					<h2 id="invite-title"><Icon name="user-plus" size={17} />Invite someone</h2>
					<p class="sub">They get a link by mail and join once they accept it with that address, verified.</p>
				</div>
			</div>
			<div class="card-body">
				<form class="form" onsubmit={invite} novalidate>
					<div class="field">
						<label class="field-label" for="inv-email">Email</label>
						<input id="inv-email" type="email" bind:value={email} autocomplete="off" spellcheck="false" placeholder="name@example.com" aria-invalid={tried && emailErr ? 'true' : undefined} aria-describedby={tried && emailErr ? 'inv-email-err' : undefined} />
						{#if tried && emailErr}<p class="field-hint bad" id="inv-email-err">{emailErr}</p>{/if}
					</div>
					<fieldset class="choices">
						<legend>Role</legend>
						{#each INVITE_ROLES as r (r)}
							<label class="choice"><input type="radio" name="inv-role" value={r} bind:group={role} /><span>{ROLE_LABEL[r]}<small>{ROLE_HINT[r]}</small></span></label>
						{/each}
					</fieldset>
					<div class="form-actions">
						<button class="btn brand sm" type="submit" disabled={busy !== null}><Icon name="send" />{busy === 'invite' ? 'Inviting…' : 'Send invite'}</button>
					</div>
				</form>
			</div>
		</section>
	{/if}

	<section class="card" aria-labelledby="members-title">
		<div class="card-head ruled">
			<div>
				<h2 id="members-title"><Icon name="users" size={17} />Members</h2>
				<p class="sub">{membersRes.data ? `${members.length} in ${account.current?.name ?? 'this workspace'}` : 'Who can open this workspace, and with which role.'}</p>
			</div>
		</div>
		{#if membersRes.status === 'unsupported' || membersRes.status === 'needs-key' || (membersRes.status === 'error' && !membersRes.data)}
			<div class="card-body"><Notice status={membersRes.status} error={membersRes.error} what="The member list" route="GET /workspaces/{'{pid}'}/members" retry={() => membersRes.refresh(true)} /></div>
		{:else if membersRes.loading}
			<Skeleton label="Loading members" />
		{:else}
			<ul class="rows">
				{#each sorted as m (m.accountId)}
					{@const self = m.accountId === me}
					<li>
						<div class="row-main">
							<p class="row-title"><span class="email">{m.email}</span>{#if self}<span class="badge">You</span>{/if}</p>
							<p class="row-meta">
								<span>{ROLE_LABEL[m.role]}</span>
								<span title={m.since ? fmtDateTime(m.since, live.now) : ''}>Member since {m.since ? fmtAgo(live.now - m.since) : '—'}</span>
							</p>
						</div>
						{#if admin && m.role !== 'owner' && !self}
							<div class="row-actions">
								<label class="sr" for="role-{m.accountId}">Role of {m.email}</label>
								<select id="role-{m.accountId}" class="role-select" value={m.role} disabled={busy !== null} onchange={(e) => setRole(m, (e.currentTarget as HTMLSelectElement).value as Role)}>
									{#each INVITE_ROLES as r (r)}<option value={r}>{ROLE_LABEL[r]}</option>{/each}
								</select>
								<Confirm label="Remove" question="Remove {m.email}? They lose access at once; their keys and runs stay." confirmLabel="Remove" busy={busy === m.accountId} onconfirm={() => remove(m)} />
							</div>
						{:else if m.role === 'owner'}
							<div class="row-actions"><span class="badge ok">Owner</span></div>
						{/if}
					</li>
				{/each}
			</ul>
		{/if}
	</section>

	<section class="card" aria-labelledby="inv-title">
		<div class="card-head ruled">
			<div>
				<h2 id="inv-title"><Icon name="mail" size={17} />Pending invites</h2>
				<p class="sub">Invites work for 14 days. Revoke one to kill its link.</p>
			</div>
		</div>
		{#if invitesRes.status === 'unsupported' || invitesRes.status === 'needs-key' || (invitesRes.status === 'error' && !invitesRes.data)}
			<div class="card-body"><Notice status={invitesRes.status} error={invitesRes.error} what="The invite list" route="GET /workspaces/{'{pid}'}/invites" retry={() => invitesRes.refresh(true)} /></div>
		{:else if invitesRes.loading}
			<Skeleton label="Loading invites" rows={1} />
		{:else if invites.length === 0}
			<p class="empty">No pending invites.</p>
		{:else}
			<ul class="rows">
				{#each invites as i (i.id)}
					<li>
						<div class="row-main">
							<p class="row-title"><span class="email">{i.email}</span></p>
							<p class="row-meta"><span>{ROLE_LABEL[i.role]}</span><span>{i.expiresAt ? (i.expiresAt > live.now ? `Expires in ${fmtAgo(i.expiresAt - live.now).replace(' ago', '')}` : 'Expired') : ''}</span></p>
						</div>
						{#if admin}
							<div class="row-actions"><Confirm label="Revoke" question="Revoke the invite to {i.email}?" confirmLabel="Revoke" busy={busy === i.id} onconfirm={() => revoke(i)} /></div>
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
	.email {
		overflow-wrap: anywhere;
	}
	.role-select {
		width: auto;
		min-height: 32px;
		padding: 4px 30px 4px 10px;
		border-radius: 10px;
		font-size: 13.5px;
	}
</style>
