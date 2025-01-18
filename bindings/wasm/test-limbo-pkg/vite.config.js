import { defineConfig } from "vite";
import wasm from "vite-plugin-wasm";

export default defineConfig({
  plugins: [wasm()],
  server: {
    headers: {
      "Cross-Origin-Embedder-Policy": "require-corp",
      "Cross-Origin-Opener-Policy": "same-origin",
      "Cross-Origin-Resource-Policy": "cross-origin",
    },
    fs: {
      allow: ["../web/dist"],
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
