import { expect, test } from "@playwright/test";

for (const path of ["/", "/alerts", "/cluster", "/trash"]) {
  test(`${path} separates its title from the page content`, async ({ page }) => {
    await page.goto(path);
    const heading = page.locator("section.heading");
    const content = heading.locator("+ *");
    const [headingBox, contentBox] = await Promise.all([heading.boundingBox(), content.boundingBox()]);

    expect(headingBox).not.toBeNull();
    expect(contentBox).not.toBeNull();
    expect(contentBox!.y - headingBox!.y - headingBox!.height).toBeGreaterThanOrEqual(30);
  });
}
