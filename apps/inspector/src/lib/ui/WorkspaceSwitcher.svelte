<script lang="ts">
	// The workspace switcher at the top of the sidebar: the selected workspace (name, role, plan), and a list of
	// every membership to switch to, plus "New workspace". A disclosure (button + list of buttons), so it reads
	// and works the same with a keyboard, a screen reader and a pointer; Escape or a click outside closes it.
	import { goto } from '$app/navigation';
	import { page } from '$app/state';
	import Icon from '../Icon.svelte';
	import { account } from '../account.svelte.ts';
	import { ROLE_LABEL } from '../saas.ts';
	import { ui } from '../ui-state.svelte.ts';

	let open = $state(false);
	let root: HTMLDivElement | undefined = $state();
	let button: HTMLButtonElement | undefined = $state();

	const current = $derived(account.current);
	const initial = (name: string) => (name.trim()[0] ?? '?').toUpperCase();
	const plan = (p: string) => p.charAt(0).toUpperCase() + p.slice(1);

	function pick(pid: string) {
		open = false;
		if (pid === account.pid) return;
		account.select(pid);
		// a run or workflow belongs to the workspace it came from: go to the list instead
		const id = page.route.id ?? '';
		if (id.includes('[')) void goto(`#/${id.split('/')[1]}`);
		button?.focus();
	}
	function create() {
		open = false;
		ui.createOpen = true;
	}
	function onKey(e: KeyboardEvent) {
		if (e.key === 'Escape' && open) {
			open = false;
			button?.focus();
		}
	}
	function onDoc(e: MouseEvent) {
		if (open && root && !root.contains(e.target as Node)) open = false;
	}
</script>

<svelte:window onkeydown={onKey} onclick={onDoc} />

<div class="ws" bind:this={root}>
	<button class="ws-btn" bind:this={button} aria-expanded={open} aria-controls="ws-list" onclick={() => (open = !open)}>
		{#if current}
			<span class="mark" aria-hidden="true">{initial(current.name)}</span>
			<span class="text"><span class="name">{current.name}</span><span class="meta">{ROLE_LABEL[current.role]} · {plan(current.plan)} plan</span></span>
		{:else}
			<span class="mark blank" aria-hidden="true"><Icon name="box" size={15} /></span>
			<span class="text"><span class="name">No workspace yet</span><span class="meta">Create one to start</span></span>
		{/if}
		<Icon name="chevrons-up-down" size={15} />
		<span class="sr">: switch workspace</span>
	</button>
	{#if open}
		<div class="pop" id="ws-list">
			<p class="pop-title" id="ws-list-title">Workspaces</p>
			{#if account.workspaces.length > 0}
				<ul aria-labelledby="ws-list-title">
					{#each account.workspaces as w (w.public_id)}
						<li>
							<button class="item" aria-current={w.public_id === account.pid ? 'true' : undefined} onclick={() => pick(w.public_id)}>
								<span class="mark sm" aria-hidden="true">{initial(w.name)}</span>
								<span class="text"><span class="name">{w.name}</span><span class="meta">{ROLE_LABEL[w.role]}</span></span>
								<span class="badge">{plan(w.plan)}</span>
								{#if w.public_id === account.pid}<Icon name="check" size={15} />{/if}
							</button>
						</li>
					{/each}
				</ul>
			{:else}
				<p class="none">You are not in any workspace yet.</p>
			{/if}
			<button class="btn sm new" disabled={!account.verified} title={account.verified ? '' : 'Verify your email first'} onclick={create}><Icon name="plus" />New workspace</button>
		</div>
	{/if}
</div>

<style>
	.ws {
		position: relative;
	}
	.ws-btn {
		display: flex;
		align-items: center;
		gap: 10px;
		width: 100%;
		min-height: 52px;
		padding: 8px 10px;
		border: 1px solid var(--c-line-strong);
		border-radius: 14px;
		background: var(--c-card);
		color: var(--c-ink);
		font: inherit;
		text-align: left;
		cursor: pointer;
		transition: background-color 150ms ease-out;
	}
	.ws-btn:hover {
		background: var(--c-hover);
	}
	.ws-btn > :global(svg) {
		flex: none;
		color: var(--c-body);
	}
	.mark {
		display: grid;
		flex: none;
		place-items: center;
		width: 30px;
		height: 30px;
		border-radius: 9px;
		background: var(--c-brand-tint);
		color: var(--c-brand-deep);
		font-size: 14px;
		font-weight: 650;
	}
	.mark.sm {
		width: 24px;
		height: 24px;
		border-radius: 7px;
		font-size: 12px;
	}
	.mark.blank {
		background: var(--c-surface);
		color: var(--c-body);
	}
	.text {
		display: flex;
		flex: 1;
		flex-direction: column;
		min-width: 0;
		line-height: 1.25;
	}
	.name {
		overflow: hidden;
		font-size: 14px;
		font-weight: 580;
		text-overflow: ellipsis;
		white-space: nowrap;
	}
	.meta {
		color: var(--c-body);
		font-size: 12.5px;
	}
	.pop {
		position: absolute;
		top: calc(100% + 6px);
		right: 0;
		left: 0;
		z-index: 20;
		display: flex;
		flex-direction: column;
		gap: 6px;
		padding: 8px;
		border-radius: 14px;
		background: var(--c-card);
		box-shadow:
			0 0 0 1px var(--c-bar-edge),
			0 12px 28px -10px var(--c-shadow);
		animation: pop-in 120ms ease-out;
	}
	.pop-title {
		margin: 2px 6px 2px;
		color: var(--c-body);
		font-size: 12.5px;
		font-weight: 540;
	}
	ul {
		display: flex;
		flex-direction: column;
		gap: 2px;
		max-height: 260px;
		margin: 0;
		padding: 0;
		overflow-y: auto;
		list-style: none;
	}
	.item {
		display: flex;
		align-items: center;
		gap: 9px;
		width: 100%;
		min-height: 40px;
		padding: 6px 8px;
		border: 0;
		border-radius: 10px;
		background: transparent;
		color: var(--c-ink);
		font: inherit;
		text-align: left;
		cursor: pointer;
	}
	.item:hover {
		background: var(--c-hover);
	}
	.item[aria-current='true'] {
		background: var(--c-surface);
	}
	.item :global(svg) {
		flex: none;
		color: var(--c-brand-deep);
	}
	.none {
		margin: 4px 6px;
		color: var(--c-body);
		font-size: 13.5px;
	}
	.new {
		justify-content: flex-start;
	}
	@keyframes pop-in {
		from {
			opacity: 0;
			transform: translateY(-4px);
		}
	}
</style>
