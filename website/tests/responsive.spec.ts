import { expect, test } from '@playwright/test';

test('keeps configuration table values intact on mobile', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('/reference/configuration/');

  const table = page.locator('table');
  const longestFlag = table.getByText('--history-retention-hours', { exact: true });
  await expect(longestFlag).toBeVisible();
  const lineCount = await longestFlag.evaluate((element) => {
    const range = document.createRange();
    range.selectNodeContents(element);
    return range.getClientRects().length;
  });

  expect(lineCount).toBe(1);
  expect(await table.evaluate((element) => element.scrollWidth > element.clientWidth)).toBe(true);
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
});
