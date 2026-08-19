import { expect, test } from "@playwright/test";

test("marks an Up target suspicious while failures accumulate", async ({ page }) => {
  await page.goto("/");
  const healthUrl = new URL("/healthz", page.url()).href;
  await page.getByRole("button", { name: "Add target" }).click();
  const addTarget = page.getByRole("dialog", { name: "Add target" });
  await addTarget.getByLabel("Name").fill("Suspicious threshold target");
  await addTarget.getByLabel("URL").fill(healthUrl);
  await addTarget.getByRole("tab", { name: "Evaluation" }).click();
  await addTarget.getByLabel("Interval (seconds)").fill("1");
  await addTarget.getByLabel("Failures before down").fill("100");
  await addTarget.getByRole("button", { name: "Create target" }).click();

  const target = page.getByRole("button", { name: "Suspicious threshold target" });
  await expect(target.locator(".state")).toHaveClass(/up/, { timeout: 15_000 });
  await target.click();
  const details = page.getByRole("dialog", { name: "Target details" });
  await details.getByRole("tab", { name: "General" }).click();
  await details.getByLabel("URL").fill("http://127.0.0.1:19090/");
  await details.getByRole("button", { name: "Save changes" }).click();

  await expect(target.locator(".state")).toHaveClass(/suspicious/, { timeout: 15_000 });
  await expect(target.locator(".state")).toHaveCSS("background-color", "rgb(154, 103, 0)");
  await target.click();
  page.once("dialog", (dialog) => dialog.accept());
  await page.getByRole("dialog", { name: "Target details" }).getByRole("button", { name: "Move target to trash" }).click();
  await expect(target).not.toBeVisible();
});
