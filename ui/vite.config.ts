/// <reference types="vitest" />
import { defineConfig } from "vite";
import { svelte } from "@sveltejs/vite-plugin-svelte";

export default defineConfig({
  plugins: [svelte()],
  server: {
    port: 5173,
    proxy: {
      "/api": "http://127.0.0.1:8765",
    },
  },
  build: {
    target: "es2022",
    sourcemap: false,
    chunkSizeWarningLimit: 1024,
  },
  test: {
    environment: "jsdom",
    globals: false,
    include: ["src/**/*.test.ts"],
    setupFiles: ["src/test-setup.ts"],
    // Resolve svelte to its browser entry (not the server one) so
    // mount() is available inside jsdom.
    server: {
      deps: {
        inline: ["svelte"],
      },
    },
  },
  resolve: {
    conditions: process.env.VITEST ? ["browser"] : [],
  },
});
