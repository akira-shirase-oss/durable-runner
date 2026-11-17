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
			// A static single-page app served by durable-server under /inspector/;
			// index.html answers every route. The fallback page cannot use relative
			// asset paths (it is served at any depth), so the base is the mount.
			adapter: adapter({ pages: out, assets: out, fallback: 'index.html' }),
			paths: { base: '/inspector' },
		}),
	],
	server: {
		host: '127.0.0.1',
		port: 59100,
		strictPort: true,
		// In development, the API is a durable-server on its default port (59000).
		proxy: { '/jobs': 'http://127.0.0.1:59000', '/health': 'http://127.0.0.1:59000', '/admin': 'http://127.0.0.1:59000' },
	},
	preview: { host: '127.0.0.1', port: 59100, strictPort: true },
});
