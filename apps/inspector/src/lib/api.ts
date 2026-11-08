// The API base is the server origin minus the /inspector mount — the UI is
// served same-origin by durable-server, so plain relative calls work.
export interface RunItem {
	id: number;
	key: string;
	state: string;
	steps: number;
	finished_at: number | null;
}

const API = '';

export async function listRuns(): Promise<RunItem[]> {
	const r = await fetch(`${API}/jobs`);
	if (!r.ok) throw new Error(`list failed: ${r.status}`);
	const body = (await r.json()) as { runs: RunItem[] };
	return body.runs;
}

export async function cancelRun(id: number): Promise<void> {
	const r = await fetch(`${API}/jobs/${id}/cancel`, { method: 'POST' });
	if (!r.ok) throw new Error(`cancel failed: ${r.status}`);
}
