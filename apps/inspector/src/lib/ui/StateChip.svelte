<script lang="ts">
	// A run state as the server reports it (`running:step:1:attempt:2`): the chip names the kind, the detail
	// beside it carries the rest (state.ts). Nothing is dropped.
	import { stateView } from '../state.ts';

	let { state, detail = true }: { state: string; detail?: boolean } = $props();
	const sv = $derived(stateView(state));
</script>

<span class="chip" data-state={sv.kind}><i class="dot" aria-hidden="true"></i>{sv.label}</span>{#if detail && sv.detail}<span
		class="state-detail">{sv.detail}</span
	>{/if}

<style>
	.state-detail {
		margin-left: 8px;
	}
</style>
