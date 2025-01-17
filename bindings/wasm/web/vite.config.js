import { defineConfig } from "vite";
import wasm from "vite-plugin-wasm";

export default defineConfig({
  publicDir: "./html",
  root: "./",
  plugins: [wasm()],
  test: {
    globals: true,
    setupFiles: ["./test/setup.js"],
    include: ["test/*.test.js"],
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
});
