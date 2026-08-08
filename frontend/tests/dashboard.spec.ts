import { expect, test } from "@playwright/test";

test("creates a target from the embedded dashboard", async ({ page }) => {
  await page.goto("/");

  await expect(page.getByRole("heading", { name: "Overview" })).toBeVisible();
  await page.getByRole("button", { name: "Add target" }).click();
  await page.getByLabel("Name").fill("Playwright target");
  await page.getByLabel("URL").fill("https://example.com/health");
  await page.getByRole("button", { name: "Create target" }).click();

  await expect(page.getByText("Playwright target")).toBeVisible();
  await expect(page.getByText("https://example.com/health")).toBeVisible();
});

test("edits, inspects, and deletes a target", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "Add target" }).click();
  await page.getByLabel("Name").fill("Target lifecycle");
  await page.getByLabel("URL").fill("http://127.0.0.1:18080/healthz");
  await page.getByRole("button", { name: "Create target" }).click();

  await page.getByRole("button", { name: "Target lifecycle" }).click();
  await expect(page.getByRole("heading", { name: "Target details" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Evaluation history" })).toBeVisible();
  const details = page.getByRole("dialog", { name: "Target details" });
  await details.getByLabel("Name", { exact: true }).fill("Renamed lifecycle target");
  await details.getByLabel("Failures before Down").fill("5");
  await details.getByRole("button", { name: "Save changes" }).click();

  await expect(page.getByText("Renamed lifecycle target")).toBeVisible();
  await page.getByRole("button", { name: "Renamed lifecycle target" }).click();
  page.once("dialog", (dialog) => dialog.accept());
  await page.getByRole("dialog", { name: "Target details" }).getByRole("button", { name: "Delete target" }).click();
  await expect(page.getByText("Renamed lifecycle target")).not.toBeVisible();
});
