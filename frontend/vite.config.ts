import { defineConfig } from "vite";
import { resolve } from "node:path";

const apiUrl = process.env.UPGRID_API_URL ?? "http://127.0.0.1:8080";
const username = process.env.UPGRID_USERNAME ?? "admin";
const password = process.env.UPGRID_PASSWORD ?? "upgrid";

export default defineConfig({
  server: {
    proxy: {
      "/api": {
        target: apiUrl,
        headers: {
          authorization: `Basic ${Buffer.from(`${username}:${password}`).toString("base64")}`,
        },
      },
    },
  },
  build: {
    emptyOutDir: true,
    rollupOptions: {
      input: {
        app: resolve(import.meta.dirname, "index.html"),
        setup: resolve(import.meta.dirname, "setup.html"),
      },
      output: {
        entryFileNames: (chunk) => `assets/${chunk.name}.js`,
        chunkFileNames: "assets/[name].js",
        assetFileNames: "assets/[name][extname]",
      },
    },
  },
});
