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

test('wraps long reference examples on mobile', async ({ page }) => {
  await page.setViewportSize({ width: 320, height: 844 });
  await page.goto('/reference/script-assertions/');

  const codeBlocks = page.locator('.expressive-code pre');
  expect(await codeBlocks.count()).toBeGreaterThan(0);
  for (const codeBlock of await codeBlocks.all()) {
    expect(await codeBlock.evaluate((element) => element.scrollWidth <= element.clientWidth)).toBe(true);
  }

  const tableFrame = page.locator('.table-frame');
  expect(await tableFrame.evaluate((element) => element.scrollWidth <= element.clientWidth)).toBe(true);
});

test('stretches compact reference tables to their frame', async ({ page }) => {
  await page.setViewportSize({ width: 1280, height: 900 });
  await page.goto('/reference/script-assertions/');

  const table = page.locator('table');
  const fillsFrame = await table.evaluate((element) => {
    if (!(element instanceof HTMLTableElement)) return false;
    const header = element.tHead;
    return header !== null && header.getBoundingClientRect().width >= element.clientWidth - 2;
  });

  expect(fillsFrame).toBe(true);
});

test('keeps configuration table values intact on mobile', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('/reference/configuration/');

  const tableFrame = page.locator('table').locator('..');
  const longestFlag = tableFrame.getByText('--history-retention-hours', { exact: true });
  await expect(longestFlag).toBeVisible();
  const lineCount = await longestFlag.evaluate((element) => {
    const range = document.createRange();
    range.selectNodeContents(element);
    return range.getClientRects().length;
  });

  expect(lineCount).toBe(1);
  expect(await tableFrame.evaluate((element) => element.scrollWidth > element.clientWidth)).toBe(true);
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
});
