import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./tests",
  fullyParallel: false,
  workers: 1,
  retries: 0,
  reporter: "line",
  use: {
    baseURL: process.env.UPGRID_UI_URL ?? "http://127.0.0.1:18080",
    storageState: process.env.UPGRID_STORAGE_STATE,
    permissions: ["clipboard-read", "clipboard-write"],
    httpCredentials: process.env.UPGRID_SETUP_USERNAME
      ? {
          username: process.env.UPGRID_SETUP_USERNAME,
          password: process.env.UPGRID_SETUP_PASSWORD ?? "",
        }
      : undefined,
    trace: "retain-on-failure",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
});
