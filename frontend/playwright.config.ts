import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./tests",
  fullyParallel: false,
  retries: 0,
  reporter: "line",
  use: {
    baseURL: process.env.UPGRID_UI_URL ?? "http://127.0.0.1:18080",
    httpCredentials: {
      username: process.env.UPGRID_USERNAME ?? "admin",
      password: process.env.UPGRID_PASSWORD ?? "test-password",
    },
    permissions: ["clipboard-read", "clipboard-write"],
    trace: "retain-on-failure",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
});
