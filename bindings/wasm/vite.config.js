import { defineConfig } from "vite";
import wasm from "vite-plugin-wasm";

export default defineConfig({
  plugins: [wasm()],
  test: {
    globals: true,
    environment: "happy-dom",
    setupFiles: ["./test/setup.js"],
    include: ["test/*.test.js"],
    sequence: {
      shuffle: false,
      concurrent: false,
    },
  },
  server: {
    headers: {
      "Cross-Origin-Embedder-Policy": "require-corp",
      "Cross-Origin-Opener-Policy": "same-origin",
      "Cross-Origin-Resource-Policy": "cross-origin",
    },
  },
  worker: {
    format: "es",
    rollupOptions: {
      output: {
        format: "es",
      },
    },
  },
  // worker: {
  //   format: "es",
  // },
});
