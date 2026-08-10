import { expect, test } from '@playwright/test';

test('keeps the WebUI showcase within the mobile viewport', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('/');

  const showcase = page.getByAltText(/overview dashboard in bright and dark themes/i);
  await expect(showcase).toBeVisible();
  await expect(showcase).toHaveJSProperty('complete', true);
  const source = await showcase.evaluate((image: HTMLImageElement) => ({
    height: image.naturalHeight,
    url: image.currentSrc,
    width: image.naturalWidth,
  }));

  expect(source.width).toBeGreaterThanOrEqual(2560);
  expect(source.height).toBeGreaterThanOrEqual(1440);
  expect(new URL(source.url).pathname).toMatch(/\.png$/);

  const bounds = await showcase.boundingBox();
  expect(bounds).not.toBeNull();
  expect(bounds!.x).toBeGreaterThanOrEqual(0);
  expect(bounds!.x + bounds!.width).toBeLessThanOrEqual(390);
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
});

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
