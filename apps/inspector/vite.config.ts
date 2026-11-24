import adapter from '@sveltejs/adapter-static';
import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';

// `build/` is what durable-server serves via --ui-dir. One build, mounted at /app (the SaaS origin answers
// /inspector with a 308 to it). The mount is a build-time choice: DURABLE_UI_BASE=/inspector npm run build makes a
// build for a server that still mounts the old path. SvelteKit wants a leading slash and no trailing one.
const out = 'build';
// (read through globalThis: this package ships no Node type definitions)
const env = (globalThis as { process?: { env: Record<string, string | undefined> } }).process?.env ?? {};
const base = (env.DURABLE_UI_BASE ?? '/app') as `/${string}`;
if (!/^\/[A-Za-z0-9_-]+(\/[A-Za-z0-9_-]+)*$/.test(base)) {
	throw new Error(`DURABLE_UI_BASE must look like /app (got ${JSON.stringify(base)})`);
}

export default defineConfig({
	plugins: [
		sveltekit({
			compilerOptions: {
				runes: ({ filename }) =>
					filename.split(/[/\\]/).includes('node_modules') ? undefined : true,
			},
			// A static single-page app served by durable-server under its mount (/app).
			// Routes live in the hash (#/runs/10), so the one index.html SvelteKit
			// writes serves them all (durable-server also answers any other path
			// under the mount with it), and durable-server's CSP hashes the
			// attribute-less scripts it finds there (inspector_csp). Assets load
			// from the mount, whatever the path.
			adapter: adapter({ pages: out, assets: out }),
			// relative: false — the bootstrap names the mount instead of deriving it from the page URL: a mailed
			// link to /app#/verify (no trailing slash) would otherwise resolve the base to "" and boot nothing.
			paths: { base, relative: false },
			router: { type: 'hash' },
		}),
	],
	server: {
		host: '127.0.0.1',
		port: 59100,
		strictPort: true,
		// In development, the API is a durable-server on its default port (59000).
		proxy: Object.fromEntries(
			[
				'/jobs', '/health', '/ready', '/admin', '/metrics', '/events', '/diagnostics', '/whoami', '/handlers', '/handler-secret',
				'/auth', '/me', '/workspaces', '/invites',
			].map((p) => [p, 'http://127.0.0.1:59000']),
		),
	},
	preview: { host: '127.0.0.1', port: 59100, strictPort: true },
});
