import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";

test("Target form meets WCAG 2.2 A and AA checks", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "Add target" }).click();

  const results = await new AxeBuilder({ page })
    .withTags(["wcag2a", "wcag2aa", "wcag21a", "wcag21aa", "wcag22aa"])
    .analyze();

  expect(results.violations).toEqual([]);
});
