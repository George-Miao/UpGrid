import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: './tests',
  reporter: 'line',
  use: {
    baseURL: 'http://127.0.0.1:4323',
  },
  webServer: {
    command: 'pnpm build && python3 -m http.server 4323 --directory dist --bind 127.0.0.1',
    url: 'http://127.0.0.1:4323',
    reuseExistingServer: true,
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
});
