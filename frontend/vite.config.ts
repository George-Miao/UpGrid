import { defineConfig } from "vite";
import { resolve } from "node:path";

const apiUrl = process.env.UPGRID_API_URL ?? "http://127.0.0.1:8080";

export default defineConfig({
  server: {
    proxy: {
      "/api": {
        target: apiUrl,
      },
    },
  },
  build: {
    emptyOutDir: true,
    rollupOptions: {
      input: {
        app: resolve(import.meta.dirname, "index.html"),
      },
      output: {
        entryFileNames: "assets/upgrid.js",
        chunkFileNames: "assets/[name].js",
        assetFileNames: "assets/[name][extname]",
      },
    },
  },
});
