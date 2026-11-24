// Shell-level UI state that must outlive a page: the shown-once reveal (a new API key, the signing secret, an
// invite link) and the create-workspace dialog. Both render in the shell, outside the per-workspace page key, so
// switching to the workspace just created never throws its secret away before it was copied.

export interface Reveal {
	title: string;
	label: string;
	value: string;
	/** why it is shown once and what to do with it */
	note: string;
	/** the snippet below the value, e.g. how to use the key */
	usage?: string;
}

class UiState {
	reveal = $state<Reveal | null>(null);
	createOpen = $state(false);

	show(r: Reveal) {
		this.reveal = r;
	}
	dismiss() {
		this.reveal = null;
	}
}

export const ui = new UiState();
