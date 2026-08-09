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

test("centers the tab bar in the viewport", async ({ page }) => {
  await page.goto("/");

  const nav = await page.getByRole("navigation", { name: "Primary" }).boundingBox();
  const viewport = page.viewportSize();
  expect(nav).not.toBeNull();
  expect(viewport).not.toBeNull();
  expect(Math.abs(nav!.x + nav!.width / 2 - viewport!.width / 2)).toBeLessThan(2);
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
  await expect(target.locator(".mini-chart")).toBeVisible();
  await expect(target.locator(".mini-bar").first()).toBeVisible();
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
  await expect(save).toHaveCSS("cursor", "not-allowed");
  const followRedirects = details.getByLabel("Follow redirects");
  await followRedirects.uncheck();
  await expect(save).toBeEnabled();
  await followRedirects.check();
  await expect(save).toBeDisabled();
  const name = details.getByLabel("Name", { exact: true });
  await name.fill("Temporary name");
  await expect(save).toBeEnabled();
  await name.fill("Target lifecycle");
  await expect(save).toBeDisabled();
  const history = details.getByRole("list", { name: /Recent evaluation latency, 0 to/ });
  await expect(history).toBeVisible();
  await expect(history.getByRole("listitem").first()).toBeVisible();
  const topology = await (await page.request.get("/api/v1/cluster")).json();
  const executor = topology.members.find((member: { local: boolean }) => member.local).name;
  await expect(history.getByRole("listitem").first()).toHaveAttribute("aria-label", new RegExp(`Executed by ${executor}`));
  await expect(details.locator(".chart-scale span")).toHaveCount(3);
  await expect(details.locator(".chart-scale").getByText("0 ms")).toBeVisible();
  await name.fill("Renamed lifecycle target");
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

  await page.getByRole("link", { name: "Alerts" }).click();
  await page.getByRole("button", { name: "Add channel" }).click();
  const channel = page.getByRole("dialog", { name: "Add channel" });
  await channel.getByLabel("Name").fill("Operations webhook");
  await channel.getByLabel("Webhook URL").fill("https://example.com/upgrid-hook");
  await channel.getByRole("button", { name: "Create channel" }).click();
  await expect(page.getByText("Operations webhook")).toBeVisible();

  page.once("dialog", (dialog) => dialog.accept());
  await page.getByRole("button", { name: "Delete channel Operations webhook" }).click();
  await expect(page.getByText("Operations webhook")).not.toBeVisible();
  await page.getByRole("link", { name: "Overview" }).click();
  page.once("dialog", (dialog) => dialog.accept());
  await page.getByRole("button", { name: "Delete secret Webhook token" }).click();
  await expect(page.getByText("Webhook token")).not.toBeVisible();

  await page.getByRole("link", { name: "Cluster" }).click();
  await expect(page.getByRole("button", { name: "Add node" })).toHaveCount(0);
  await expect(page.getByRole("button", { name: "Join cluster" })).toHaveCount(0);
  await page.getByRole("button", { name: "Create token" }).click();
  const config = page.getByRole("dialog", { name: "Create Join Token" });
  await expect(config.getByLabel("Expiration (days)")).toHaveValue("1");
  await expect(config.getByLabel("Unit")).toHaveCount(0);
  const unlimited = config.getByRole("switch", { name: "Unlimited uses" });
  await expect(unlimited).not.toBeChecked();
  await expect(config.getByLabel("Maximum uses")).toHaveValue("1");
  await expect(config.getByLabel("Maximum uses")).toBeEnabled();
  await unlimited.check();
  await expect(config.getByLabel("Maximum uses")).toBeDisabled();
  await unlimited.uncheck();
  await expect(config.getByLabel("Maximum uses")).toBeEnabled();
  await config.getByLabel("Expiration (days)").fill("2");
  await config.getByLabel("Maximum uses").fill("2");
  await config.getByRole("button", { name: "Create token" }).click();
  const join = page.getByRole("dialog", { name: "Join Token Created" });
  await expect(join.getByText(/upgrid --join 'up:\/\//)).toBeVisible();
  await join.getByRole("button", { name: "Close" }).click();
  await expect(page.getByRole("region", { name: "Join tokens" }).getByText("1 stored")).toBeVisible();
  await expect(page.getByRole("region", { name: "Join tokens" }).getByText("2 uses left")).toBeVisible();
  page.once("dialog", (dialog) => dialog.accept());
  await page.getByRole("button", { name: /Revoke Join Token/ }).click();
  await expect(page.getByRole("region", { name: "Join tokens" }).getByText("0 stored")).toBeVisible();
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
  const pausedTarget = page.getByRole("button", { name: "Pausing target" });
  await expect(pausedTarget).toContainText("Paused");
  await expect(pausedTarget.locator(".state")).toHaveClass(/paused/);

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
  const expectedRaftUrl = process.env.UPGRID_EXPECTED_RAFT_URL ?? "up://127.0.0.1:18451";
  await expect(cluster.getByText(expectedRaftUrl)).toBeVisible();
  await expect(cluster.locator(".resource strong")).not.toBeEmpty();
  await expect(cluster.locator(".resource code")).toHaveText(expectedRaftUrl);
  await expect(cluster.getByText("Leader")).toBeVisible();
  await expect(cluster.getByText("This node")).toBeVisible();
});

test("dismisses a startup compatibility warning", async ({ page }) => {
  await page.goto(process.env.UPGRID_WARNING_URL ?? "http://127.0.0.1:18083");
  const warning = page.getByRole("status");
  await expect(warning).toContainText("Configured Join Token is invalid");
  await warning.getByRole("button", { name: "Dismiss" }).click();
  await expect(warning).toHaveCount(0);
  await page.reload();
  await expect(page.getByRole("status")).toHaveCount(0);
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
  const pauseSelected = page.getByRole("button", { name: "Pause selected" });
  await expect(page.getByRole("button", { name: "Unselect all" }).locator("iconify-icon")).toBeVisible();
  const actionsMargin = await page.locator(".bulk-actions").evaluate((element) =>
    Number.parseFloat(getComputedStyle(element).marginLeft),
  );
  expect(actionsMargin).toBeGreaterThan(0);
  await expect(pauseSelected).toHaveClass(/warning/);
  await expect(pauseSelected.locator("iconify-icon")).toBeVisible();
  await expect(page.getByRole("button", { name: "Resume selected" })).toHaveCount(0);
  await pauseSelected.click();
  await expect(page.getByRole("button", { name: "Search alpha" })).toContainText("Paused");

  await page.getByLabel("Select Search alpha").check();
  await expect(page.getByRole("button", { name: "Pause selected" })).toHaveCount(0);
  const resumeSelected = page.getByRole("button", { name: "Resume selected" });
  await expect(resumeSelected).toHaveClass(/success/);
  await expect(resumeSelected.locator("iconify-icon")).toBeVisible();
  await resumeSelected.click();
  await expect(page.getByRole("button", { name: "Search alpha" })).not.toContainText("Paused");

  await page.getByLabel("Select Search alpha").check();
  await page.getByRole("button", { name: "Unselect all" }).click();
  await expect(page.locator(".bulk")).toHaveCount(0);
  await expect(page.getByLabel("Select Search alpha")).not.toBeChecked();
});

test("navigation opens dedicated Alert and Cluster pages", async ({ page }) => {
  await page.goto("/");

  const summary = page.getByRole("region", { name: "Target summary" });
  const secrets = page.getByRole("region", { name: "Secrets" });
  const targets = page.getByRole("region", { name: "Targets" });
  const [summaryBox, secretsBox, targetsBox] = await Promise.all([
    summary.boundingBox(), secrets.boundingBox(), targets.boundingBox(),
  ]);
  expect(summaryBox).not.toBeNull();
  expect(secretsBox).not.toBeNull();
  expect(targetsBox).not.toBeNull();
  expect(summaryBox!.x).toBeLessThan(secretsBox!.x);
  expect(Math.abs(summaryBox!.y - secretsBox!.y)).toBeLessThan(2);
  expect(Math.abs(summaryBox!.width - secretsBox!.width)).toBeLessThan(2);
  expect(targetsBox!.y).toBeGreaterThanOrEqual(Math.max(
    summaryBox!.y + summaryBox!.height,
    secretsBox!.y + secretsBox!.height,
  ));
  await expect(page.getByRole("region", { name: "Notification channels" })).toHaveCount(0);

  await expect(page.getByRole("link", { name: "Targets" })).toHaveCount(0);
  await page.getByRole("link", { name: "Alerts" }).click();
  await expect(page).toHaveURL(/\/alerts$/);
  await expect(page.getByRole("link", { name: "Alerts" })).toHaveClass(/active/);
  await expect(page.getByRole("heading", { name: "Alerts" })).toBeVisible();
  await expect(page.getByRole("region", { name: "Targets" })).toHaveCount(0);
  await expect(page.getByRole("region", { name: "Notification channels" })).toBeVisible();

  await page.getByRole("link", { name: "Cluster" }).click();
  await expect(page).toHaveURL(/\/cluster$/);
  await expect(page.getByRole("link", { name: "Cluster" })).toHaveClass(/active/);
  await expect(page.getByRole("region", { name: "Cluster topology" })).toBeInViewport();
  await expect(page.getByRole("heading", { name: "Alerts" })).toHaveCount(0);
});

test("copying a join command confirms success", async ({ page }) => {
  await page.goto("/cluster");

  await page.getByRole("button", { name: "Create token" }).click();
  await page.getByRole("dialog", { name: "Create Join Token" }).getByRole("button", { name: "Create token" }).click();
  const join = page.getByRole("dialog", { name: "Join Token Created" });
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

test("keeps the first-run Cluster choice compact", async ({ page }) => {
  await page.setViewportSize({ width: 735, height: 560 });
  const setupUrl = process.env.UPGRID_NEW_SETUP_URL ?? "http://127.0.0.1:18082";
  await page.goto(setupUrl);
  const setup = page.getByRole("region", { name: "UpGrid setup" });
  await expect(setup.getByRole("textbox", { name: "Node name" })).toHaveCount(1);

  const divider = setup.getByText("Or", { exact: true });
  const [create, separator, join, token, joinButton] = await Promise.all([
    setup.locator(".cluster-create").boundingBox(),
    divider.boundingBox(),
    setup.locator(".cluster-join").boundingBox(),
    setup.getByLabel("Join Token").boundingBox(),
    setup.getByRole("button", { name: "Join Cluster" }).boundingBox(),
  ]);
  expect(create).not.toBeNull();
  expect(separator).not.toBeNull();
  expect(join).not.toBeNull();
  expect(token).not.toBeNull();
  expect(joinButton).not.toBeNull();
  expect(create!.y + create!.height).toBeLessThanOrEqual(separator!.y);
  expect(separator!.y + separator!.height).toBeLessThanOrEqual(join!.y);
  expect(join!.y + join!.height).toBeLessThanOrEqual(560);
  expect(token!.x + token!.width).toBeLessThanOrEqual(joinButton!.x - 9);
  expect(Math.abs(token!.height - joinButton!.height)).toBeLessThan(1);

  const [shell, header, flow] = await Promise.all([
    page.locator(".setup-shell").boundingBox(),
    page.locator(".setup-shell header").boundingBox(),
    setup.boundingBox(),
  ]);
  expect(shell).not.toBeNull();
  expect(header).not.toBeNull();
  expect(flow).not.toBeNull();
  const spaceAbove = flow!.y - (header!.y + header!.height);
  const spaceBelow = shell!.y + shell!.height - (flow!.y + flow!.height);
  expect(Math.abs(spaceAbove - spaceBelow)).toBeLessThan(30);

  const script = await page.request.get(`${setupUrl}/assets/upgrid.js`);
  expect(script.ok()).toBeTruthy();
  expect(script.headers()["cache-control"]).toBe("no-store");
});

test("keeps dashboard routes inside OOBE before Cluster setup", async ({ page }) => {
  const setupUrl = process.env.UPGRID_NEW_SETUP_URL ?? "http://127.0.0.1:18082";
  await page.goto(`${setupUrl}/cluster`);
  await expect(page).toHaveURL(/\/setup$/);
  await expect(page.getByRole("heading", { name: "Choose your Cluster" })).toBeVisible();
  await expect(page.getByRole("navigation", { name: "Primary" })).toHaveCount(0);
});

test("creates a Cluster and optional resources through OOBE", async ({ page }) => {
  await page.goto(process.env.UPGRID_NEW_SETUP_URL ?? "http://127.0.0.1:18082");
  await expect(page).toHaveURL(/\/setup$/);
  const setup = page.getByRole("region", { name: "UpGrid setup" });
  await setup.getByRole("textbox", { name: "Node name" }).first().fill("playwright-primary");
  page.once("dialog", (dialog) => dialog.accept());
  await setup.getByRole("button", { name: "Create new Cluster" }).click();

  await expect(page).toHaveURL(/\/setup\/channel$/, { timeout: 20_000 });
  await page.getByLabel("Name").fill("OOBE webhook");
  await page.getByLabel("Webhook URL").fill("https://example.com/hook");
  await page.getByRole("button", { name: "Create and continue" }).click();

  await expect(page).toHaveURL(/\/setup\/target$/);
  await page.getByLabel("Name").fill("OOBE target");
  await page.getByLabel("URL").fill("https://example.com/health");
  await page.getByLabel("OOBE webhook").check();
  await page.getByRole("button", { name: "Create and finish" }).click();

  await expect(page).toHaveURL(/\/$/);
  await expect(page.getByRole("heading", { name: "Overview" })).toBeVisible();
  await expect(page.getByText("OOBE target")).toBeVisible();
});

test("joins a fresh node to the Cluster from its WebUI", async ({ page, request }) => {
  const invitation = await request.post("/api/v1/join-tokens", {
    data: { expires_in_seconds: 600 },
  });
  expect(invitation.ok()).toBeTruthy();
  const token = await invitation.json();

  await page.goto(process.env.UPGRID_SETUP_URL ?? "http://127.0.0.1:18081");
  await expect(page).toHaveURL(/\/setup$/);
  await expect(page.getByRole("heading", { name: "Choose your Cluster" })).toBeVisible();
  await expect(page.getByRole("navigation", { name: "Primary" })).toHaveCount(0);
  await expect(page.getByRole("button", { name: "Create token" })).toHaveCount(0);
  const setup = page.getByRole("region", { name: "UpGrid setup" });
  await setup.getByRole("textbox", { name: "Node name" }).last().fill("playwright-worker");
  await setup.getByLabel("Join Token").fill(token.url);
  await setup.getByRole("button", { name: "Join Cluster" }).click();
  await expect(page).toHaveURL(/\/setup\/channel$/, { timeout: 20_000 });
  await expect(page.getByRole("heading", { name: "Add a notification channel" })).toBeVisible();
  await expect(page.getByText(/already configured/)).toBeVisible();
  await page.goto(`${process.env.UPGRID_SETUP_URL ?? "http://127.0.0.1:18081"}/setup`);
  await expect(page).toHaveURL(/\/setup\/channel$/);
  await page.getByRole("button", { name: "Skip" }).click();
  await expect(page).toHaveURL(/\/setup\/target$/);
  await expect(page.getByRole("heading", { name: "Monitor your first Target" })).toBeVisible();
  await expect(page.getByText(/already configured/)).toBeVisible();
  await page.getByRole("button", { name: "Skip" }).click();
  await expect(page).toHaveURL(/\/$/);
  await expect(page.getByRole("heading", { name: "Overview" })).toBeVisible();
  await page.getByRole("link", { name: "Cluster" }).click();
  await expect(page.getByRole("region", { name: "Cluster topology" }).locator(".resource")).toHaveCount(2);
  await expect(page.getByText("playwright-worker")).toBeVisible();
});
