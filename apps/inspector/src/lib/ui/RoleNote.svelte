<script lang="ts">
	// The read-only note for a page whose changes need more than this account has in the selected workspace: a
	// role (viewer, member) or a verified email. Pages stay fully readable; only the controls are gone.
	import Icon from '../Icon.svelte';
	import { account } from '../account.svelte.ts';
	import { ROLE_LABEL, type Action } from '../saas.ts';

	let { need, what }: { need: Action; what: string } = $props();
	const NEED: Record<Action, string> = { read: 'any role', write: 'the member role or above', admin: 'the admin or owner role', owner: 'the owner' };
</script>

{#if !account.can(need) && account.verified}
	<p class="banner hint">
		<Icon name="lock" size={16} />
		<span class="msg">You are {account.role === 'admin' ? 'an' : 'a'} <strong>{account.role ? ROLE_LABEL[account.role].toLowerCase() : 'guest'}</strong> in this workspace: {what} needs {NEED[need]}. You can see everything here.</span>
	</p>
{/if}
