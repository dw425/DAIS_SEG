import { defineConfig } from 'vite';
import { viteSingleFile } from 'vite-plugin-singlefile';

export default defineConfig({
  plugins: [viteSingleFile()],
  build: {
    target: 'es2020',
    outDir: 'dist',
    minify: 'esbuild',
    rollupOptions: {
      input: 'index.dev.html'
    }
  },
  test: {
    environment: 'jsdom',
    include: ['test/**/*.test.js'],
    globals: true
  }
});
