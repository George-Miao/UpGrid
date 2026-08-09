import { expect, test } from "@playwright/test";

test("follows the system color scheme", async ({ page }) => {
  await page.emulateMedia({ colorScheme: "light" });
  await page.goto("/");
  const app = page.locator("upgrid-app");
  await expect(page.getByRole("button", { name: "Theme: System" })).toBeVisible();
  await expect(app).toHaveCSS("color-scheme", "light");
  await expect(app).toHaveCSS("color", "rgb(22, 33, 28)");

  await page.emulateMedia({ colorScheme: "dark" });
  await expect(app).toHaveCSS("color-scheme", "dark");
  await expect(app).toHaveCSS("color", "rgb(237, 247, 242)");
});

test("shows an icon theme control and live status in the brand", async ({ page }) => {
  await page.goto("/");
  const app = page.locator("upgrid-app");
  const theme = page.getByRole("button", { name: "Theme: System" });
  await expect(theme.locator("iconify-icon")).toBeVisible();
  await expect(app.locator(".brand .live")).toBeVisible();
  await expect(app.locator(".actions .live")).toHaveCount(0);
});

test("cycles and remembers an explicit theme", async ({ page }) => {
  await page.emulateMedia({ colorScheme: "light" });
  await page.goto("/");
  const app = page.locator("upgrid-app");
  await page.getByRole("button", { name: "Theme: System" }).click();
  await expect(page.getByRole("button", { name: "Theme: Dark" })).toBeVisible();
  await expect(app).toHaveCSS("color-scheme", "dark");

  await page.reload();
  await expect(page.getByRole("button", { name: "Theme: Dark" })).toBeVisible();
  await expect(app).toHaveCSS("color-scheme", "dark");

  await page.getByRole("button", { name: "Theme: Dark" }).click();
  await expect(page.getByRole("button", { name: "Theme: Bright" })).toBeVisible();
  await expect(app).toHaveCSS("color-scheme", "light");

  await page.getByRole("button", { name: "Theme: Bright" }).click();
  await expect(page.getByRole("button", { name: "Theme: System" })).toBeVisible();
});

test("fills the viewport without browser-default padding", async ({ page }) => {
  await page.goto("/");
  await expect(page.locator("body")).toHaveCSS("margin", "0px");
  await expect(page.locator("upgrid-app")).toHaveCSS("min-height", "720px");
});

test("disables maximum redirects when redirects are not followed", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "Add target" }).click();
  const addTarget = page.getByRole("dialog", { name: "Add target" });
  await addTarget.getByLabel("Name").fill("Redirect settings");
  await addTarget.getByLabel("URL").fill("https://example.com");
  await addTarget.getByRole("button", { name: "Create target" }).click();

  await page.getByRole("button", { name: "Redirect settings" }).click();
  const details = page.getByRole("dialog", { name: "Target details" });
  const followRedirects = details.getByLabel("Follow redirects");
  const maxRedirects = details.getByLabel("Maximum redirects");
  await expect(maxRedirects).toBeEnabled();
  await followRedirects.uncheck();
  await expect(maxRedirects).toBeDisabled();
  await followRedirects.check();
  await expect(maxRedirects).toBeEnabled();
});

test("creates a target from the embedded dashboard", async ({ page }) => {
  await page.goto("/");

  await expect(page.getByRole("heading", { name: "Overview" })).toBeVisible();
  await page.getByRole("button", { name: "Add target" }).click();
  const addTarget = page.getByRole("dialog", { name: "Add target" });
  await addTarget.getByLabel("Name").fill("Playwright target");
  await addTarget.getByLabel("URL").fill("https://example.com/health");
  await addTarget.getByRole("button", { name: "Create target" }).click();

  await expect(page.getByText("Playwright target")).toBeVisible();
  await expect(page.getByText("https://example.com/health")).toBeVisible();
});

test("edits, inspects, and deletes a target", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "Add target" }).click();
  const addTarget = page.getByRole("dialog", { name: "Add target" });
  await addTarget.getByLabel("Name").fill("Target lifecycle");
  await addTarget.getByLabel("URL").fill("http://127.0.0.1:18080/healthz");
  await addTarget.getByRole("button", { name: "Create target" }).click();

  const target = page.getByRole("button", { name: "Target lifecycle" });
  await expect(target).not.toContainText("waiting", { timeout: 15_000 });
  await target.click();
  await expect(page.getByRole("heading", { name: "Target details" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Evaluation history" })).toBeVisible();
  const details = page.getByRole("dialog", { name: "Target details" });
  await expect(details.locator(".dialog-head p")).toHaveCount(0);
  await expect(details.getByRole("button", { name: "Close", exact: true })).toHaveCount(0);
  await expect(details.getByRole("button", { name: "Close target details" }).locator("iconify-icon")).toBeVisible();
  await expect(details.getByRole("button", { name: "Delete target" }).locator("iconify-icon")).toBeVisible();
  const pause = details.getByRole("button", { name: "Pause evaluations" });
  await expect(pause).toHaveClass(/warning/);
  await expect(pause).toHaveCSS("background-color", "rgba(0, 0, 0, 0)");
  await expect(pause.locator("iconify-icon")).toBeVisible();
  await expect(details.locator(".danger-actions").getByRole("button")).toHaveCount(2);
  const save = details.getByRole("button", { name: "Save changes" });
  await expect(save).toBeDisabled();
  const history = details.getByRole("list", { name: "Recent evaluation latency" });
  await expect(history).toBeVisible();
  await expect(history.getByRole("listitem").first()).toBeVisible();
  await details.getByLabel("Name", { exact: true }).fill("Renamed lifecycle target");
  await expect(save).toBeEnabled();
  await details.getByLabel("Failures before Down").fill("5");
  await details.getByRole("button", { name: "Save changes" }).click();

  await expect(page.getByText("Renamed lifecycle target")).toBeVisible();
  await page.getByRole("button", { name: "Renamed lifecycle target" }).click();
  page.once("dialog", (dialog) => dialog.accept());
  await page.getByRole("dialog", { name: "Target details" }).getByRole("button", { name: "Delete target" }).click();
  await expect(page.getByText("Renamed lifecycle target")).not.toBeVisible();
});

test("configures notification resources and creates a join command", async ({ page }) => {
  await page.goto("/");

  await page.getByRole("button", { name: "Add secret" }).click();
  await page.getByRole("dialog", { name: "Add secret" }).getByLabel("Name").fill("Webhook token");
  await page.getByRole("dialog", { name: "Add secret" }).getByLabel("Value").fill("not-returned-by-api");
  await page.getByRole("button", { name: "Create secret" }).click();
  await expect(page.getByText("Webhook token")).toBeVisible();

  await page.getByRole("button", { name: "Add channel" }).click();
  const channel = page.getByRole("dialog", { name: "Add channel" });
  await channel.getByLabel("Name").fill("Operations webhook");
  await channel.getByLabel("Webhook URL").fill("https://example.com/upgrid-hook");
  await channel.getByRole("button", { name: "Create channel" }).click();
  await expect(page.getByText("Operations webhook")).toBeVisible();

  page.once("dialog", (dialog) => dialog.accept());
  await page.getByRole("button", { name: "Delete channel Operations webhook" }).click();
  await expect(page.getByText("Operations webhook")).not.toBeVisible();
  page.once("dialog", (dialog) => dialog.accept());
  await page.getByRole("button", { name: "Delete secret Webhook token" }).click();
  await expect(page.getByText("Webhook token")).not.toBeVisible();

  await page.getByRole("button", { name: "Add node" }).first().click();
  const join = page.getByRole("dialog", { name: "Join a node" });
  await expect(join.getByText(/upgrid --join 'ups:\/\//)).toBeVisible();
});

test("pauses and resumes cluster-wide evaluations", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "Add target" }).click();
  const addTarget = page.getByRole("dialog", { name: "Add target" });
  await addTarget.getByLabel("Name").fill("Pausing target");
  await addTarget.getByLabel("URL").fill("https://example.com");
  await addTarget.getByRole("button", { name: "Create target" }).click();

  await page.getByRole("button", { name: "Pausing target" }).click();
  await page.getByRole("button", { name: "Pause evaluations" }).click();
  await expect(page.getByRole("button", { name: "Pausing target" })).toContainText("Paused");

  await page.getByRole("button", { name: "Pausing target" }).click();
  const resume = page.getByRole("button", { name: "Resume evaluations" });
  await expect(resume).toHaveClass(/success/);
  await expect(resume).toHaveCSS("background-color", "rgba(0, 0, 0, 0)");
  await resume.click();
  await expect(page.getByRole("button", { name: "Pausing target" })).not.toContainText("Paused");
});

test("shows the local Raft topology and leader", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("link", { name: "Cluster" }).click();

  const cluster = page.getByRole("region", { name: "Cluster topology" });
  await expect(cluster.getByText("up://127.0.0.1:18451")).toBeVisible();
  await expect(cluster.getByText("Leader")).toBeVisible();
  await expect(cluster.getByText("This node")).toBeVisible();
});

test("filters and bulk-pauses selected targets", async ({ page }) => {
  await page.goto("/");
  for (const name of ["Search alpha", "Search beta"]) {
    await page.getByRole("button", { name: "Add target" }).click();
    const dialog = page.getByRole("dialog", { name: "Add target" });
    await dialog.getByLabel("Name").fill(name);
    await dialog.getByLabel("URL").fill("https://example.com");
    await dialog.getByRole("button", { name: "Create target" }).click();
  }

  await page.getByLabel("Search targets").fill("alpha");
  await expect(page.getByRole("button", { name: "Search alpha" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Search beta" })).not.toBeVisible();
  await page.getByLabel("Select Search alpha").check();
  await page.getByRole("button", { name: "Pause selected" }).click();
  await expect(page.getByRole("button", { name: "Search alpha" })).toContainText("Paused");
});

test("navigation opens dedicated Alert and Cluster pages", async ({ page }) => {
  await page.goto("/");

  await expect(page.getByRole("link", { name: "Targets" })).toHaveCount(0);
  await page.getByRole("link", { name: "Alerts" }).click();
  await expect(page.getByRole("link", { name: "Alerts" })).toHaveClass(/active/);
  await expect(page.getByRole("heading", { name: "Alerts" })).toBeVisible();
  await expect(page.getByRole("region", { name: "Targets" })).toHaveCount(0);

  await page.getByRole("link", { name: "Cluster" }).click();
  await expect(page.getByRole("link", { name: "Cluster" })).toHaveClass(/active/);
  await expect(page.getByRole("region", { name: "Cluster topology" })).toBeInViewport();
  await expect(page.getByRole("heading", { name: "Alerts" })).toHaveCount(0);
});

test("copying a join command confirms success", async ({ page }) => {
  await page.goto("/");

  await page.getByRole("button", { name: "Add node" }).first().click();
  const join = page.getByRole("dialog", { name: "Join a node" });
  await join.getByRole("button", { name: "Copy command" }).click();
  await expect(join.getByRole("button", { name: "Copied" })).toBeVisible();
});

test("clicking a modal backdrop closes it", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "Add target" }).click();
  const dialog = page.getByRole("dialog", { name: "Add target" });
  await expect(dialog).toBeVisible();
  await page.mouse.click(4, 4);
  await expect(dialog).not.toBeVisible();
});

test("target hover highlights the checkbox and content as one row", async ({ page }) => {
  await page.goto("/");

  await page.getByRole("button", { name: "Add target" }).click();
  const addTarget = page.getByRole("dialog", { name: "Add target" });
  await addTarget.getByLabel("Name").fill("Hover target");
  await addTarget.getByLabel("URL").fill("https://example.com");
  await addTarget.getByRole("button", { name: "Create target" }).click();
  const target = page.getByRole("button", { name: "Hover target" });
  await target.hover();
  await expect
    .poll(() => target.evaluate((element) => getComputedStyle(element).backgroundColor))
    .not.toBe("rgba(0, 0, 0, 0)");
  const backgrounds = await target.evaluate((element) => ({
    button: getComputedStyle(element).backgroundColor,
    row: getComputedStyle(element.parentElement!).backgroundColor,
  }));
  expect(backgrounds.row).toBe(backgrounds.button);
  expect(backgrounds.row).not.toBe("rgba(0, 0, 0, 0)");
});
