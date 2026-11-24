import adapter from '@sveltejs/adapter-static';
import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';

// `build/` is what durable-server serves at /inspector via --ui-dir.
const out = 'build';

export default defineConfig({
	plugins: [
		sveltekit({
			compilerOptions: {
				runes: ({ filename }) =>
					filename.split(/[/\\]/).includes('node_modules') ? undefined : true,
			},
			// A static single-page app served by durable-server under /inspector/.
			// Routes live in the hash (#/runs/10): one index.html serves them all,
			// so durable-server's CSP hashes the one bootstrap script it finds there
			// (inspector_csp). The fallback page cannot use relative asset paths
			// (it is served at any depth), so the base is the mount.
			adapter: adapter({ pages: out, assets: out, fallback: 'index.html' }),
			paths: { base: '/inspector' },
			router: { type: 'hash' },
		}),
	],
	server: {
		host: '127.0.0.1',
		port: 59100,
		strictPort: true,
		// In development, the API is a durable-server on its default port (59000).
		proxy: Object.fromEntries(
			['/jobs', '/health', '/ready', '/admin', '/metrics', '/events'].map((p) => [p, 'http://127.0.0.1:59000']),
		),
	},
	preview: { host: '127.0.0.1', port: 59100, strictPort: true },
});
