import { defineConfig } from 'vite'

export default defineConfig({
  root: '.',
  // Static assets live in static/ — Vite dev server serves them at /static/*.
  // FastAPI mounts /static → frontend/static/ for production.
  publicDir: 'static',
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    rollupOptions: {
      // This SPA uses CDN scripts and is served directly by FastAPI.
      // Vite is only used as a dev proxy. Build copies publicDir (static/) to dist/.
      input: 'static/js/app.js',
      output: {
        entryFileNames: 'js/[name].js',
      },
      external: /^(\/static\/|https?:\/\/)/,
    },
    reportCompressedSize: false,
  },
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://localhost:8081',
        changeOrigin: true,
      },
      '/ws': {
        target: 'ws://localhost:8081',
        ws: true,
        changeOrigin: true,
      },
    },
  },
})
