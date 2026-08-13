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

test("shows project footer links with icons", async ({ page }) => {
  await page.setViewportSize({ width: 1280, height: 1200 });
  await page.goto("/");
  const footer = page.getByRole("contentinfo", { name: "Project information" });
  await expect(footer.getByRole("link", { name: "A Project by Pop" })).toHaveAttribute("href", "https://miao.dev");
  const github = footer.getByRole("link", { name: "GitHub" });
  const website = footer.getByRole("link", { name: "upgrid.rs" });
  const compio = footer.getByRole("link", { name: "Compio" });
  const openraft = footer.getByRole("link", { name: "OpenRaft" });
  await expect(github).toHaveAttribute("href", "https://github.com/George-Miao/UpGrid");
  await expect(website).toHaveAttribute("href", "https://upgrid.rs");
  await expect(compio).toHaveAttribute("href", "https://compio.rs/");
  await expect(openraft).toHaveAttribute("href", "https://github.com/databendlabs/openraft");
  await expect(footer).toContainText("Proudly powered by Compio and OpenRaft");
  await expect(compio).toHaveCSS("text-decoration-line", "underline");
  await expect(github.locator("iconify-icon")).toBeVisible();
  await expect(website.locator("iconify-icon")).toBeVisible();
  const bounds = await footer.boundingBox();
  expect(bounds).not.toBeNull();
  expect(Math.abs(bounds!.y + bounds!.height - 1200)).toBeLessThan(2);
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
  await details.getByRole("tab", { name: "General" }).click();
  const followRedirects = details.getByRole("switch", { name: "Follow redirects" });
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
  const [dialogHead, formTabs] = await Promise.all([addTarget.locator(".dialog-head").boundingBox(), addTarget.locator(".form-tabs").boundingBox()]);
  expect(dialogHead).not.toBeNull();
  expect(formTabs).not.toBeNull();
  expect(formTabs!.y).toBeGreaterThanOrEqual(dialogHead!.y);
  expect(formTabs!.y + formTabs!.height).toBeLessThanOrEqual(dialogHead!.y + dialogHead!.height);
  await addTarget.getByRole("tab", { name: "Notifications" }).click();
  await expect(addTarget.getByText("No notification channels are available.")).toBeVisible();
  await addTarget.getByRole("tab", { name: "General" }).click();
  await addTarget.getByLabel("Name").fill("Playwright target");
  await addTarget.getByLabel("URL").fill("https://example.com/health");
  await addTarget.getByRole("button", { name: "Create target" }).click();

  await expect(page.getByText("Playwright target")).toBeVisible();
  await expect(page.getByText("https://example.com/health")).toBeVisible();
});

test("creates and edits ordered HTTP assertions", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "Add target" }).click();
  const create = page.getByRole("dialog", { name: "Add target" });
  await create.getByLabel("Name").fill("Assertion target");
  await create.getByLabel("URL").fill(new URL("/healthz", page.url()).toString());
  await create.getByRole("tab", { name: "Assertions" }).click();
  await expect(create.locator("http-assertion-editor fieldset")).toHaveCount(0);
  await expect(create.getByText("No assertions.", { exact: true })).toBeVisible();

  for (let index = 0; index < 6; index += 1) {
    await create.getByRole("button", { name: "Add assertion" }).click();
  }
  const kinds = ["body_contains", "body_regex", "json_path", "response_header", "latency", "script"];
  for (const [index, kind] of kinds.entries()) {
    await create.getByLabel(`Assertion ${index + 1} type`).selectOption(kind);
  }
  await create.getByLabel("Assertion 1 required text").fill("ok");
  await create.getByLabel("Assertion 2 regular expression").fill("ok|healthy");
  await create.getByLabel("Assertion 3 JSONPath").fill("$.healthy");
  await create.getByLabel("Assertion 4 header name").fill("content-type");
  await create.getByLabel("Assertion 5 maximum milliseconds").fill("60000");
  await create.getByLabel("Assertion 6 script").fill("status >= 200");
  await create.getByRole("button", { name: "Create target" }).click();

  const created = (await (await page.request.get("/api/v1/targets")).json()).find((target: { name: string }) => target.name === "Assertion target");
  expect(created.assertions.map((assertion: { kind: string }) => assertion.kind)).toEqual(kinds);

  await page.getByRole("button", { name: "Assertion target" }).click();
  const edit = page.getByRole("dialog", { name: "Target details" });
  await edit.getByRole("tab", { name: "Assertions" }).click();
  await edit.getByLabel("Assertion 1 required text").fill("healthy");
  await expect(edit.getByRole("button", { name: "Save changes" })).toBeEnabled();
  await edit.getByLabel("Assertion 2 regular expression").fill("healthy|ready");
  await edit.getByLabel("Assertion 3 JSONPath").fill("$.ready");
  await edit.getByLabel("Assertion 3 expected value").fill("true");
  await edit.getByLabel("Assertion 4 header name").fill("server");
  await edit.getByLabel("Assertion 4 header value").fill("UpGrid");
  await edit.getByLabel("Assertion 5 maximum milliseconds").fill("30000");
  await edit.getByLabel("Assertion 6 script").fill("status == 200 && latency_ms < 30000");
  await edit.getByRole("button", { name: "Move assertion 6 up" }).click();
  await expect(edit.getByLabel("Assertion 5 script")).toBeVisible();
  await expect(edit.getByLabel("Assertion 6 maximum milliseconds")).toBeVisible();
  await edit.getByRole("button", { name: "Save changes" }).click();

  const updated = (await (await page.request.get(`/api/v1/targets/${created.id}`)).json()).assertions;
  expect(updated.map((assertion: { kind: string }) => assertion.kind)).toEqual(["body_contains", "body_regex", "json_path", "response_header", "script", "latency"]);
  expect(updated[2]).toMatchObject({ path: "$.ready", expected: "true" });
  expect(updated[3]).toMatchObject({ name: "server", value: "UpGrid" });
  expect(updated[4]).toMatchObject({ source: "status == 200 && latency_ms < 30000" });
});

test("omits advanced TLS controls during creation and edits existing TLS Secrets", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "Add target" }).click();
  const create = page.getByRole("dialog", { name: "Add target" });
  await expect(create.getByLabel("Custom CA bundle Secret")).toHaveCount(0);
  await expect(create.getByLabel("Client certificate Secret")).toHaveCount(0);
  await expect(create.getByLabel("Client private key Secret")).toHaveCount(0);
  await create.getByRole("button", { name: "Cancel" }).click();

  const createSecret = async (name: string, value: string) => {
    await page.getByRole("button", { name: "Add secret" }).click();
    const dialog = page.getByRole("dialog", { name: "Add secret" });
    await dialog.getByLabel("Name").fill(name);
    await dialog.getByLabel("Value").fill(value);
    await dialog.getByRole("button", { name: "Create secret" }).click();
  };
  await createSecret("Browser private CA", "private-ca-pem");
  await createSecret("Browser client certificate", "client-certificate-pem");
  await createSecret("Browser client private key", "client-private-key-pem");

  const secrets: { id: string; name: string }[] = await (await page.request.get("/api/v1/secrets")).json();
  const caSecretId = secrets.find((secret) => secret.name === "Browser private CA")?.id;
  const certificateSecretId = secrets.find((secret) => secret.name === "Browser client certificate")?.id;
  const privateKeySecretId = secrets.find((secret) => secret.name === "Browser client private key")?.id;
  expect(caSecretId).toBeDefined();
  expect(certificateSecretId).toBeDefined();
  expect(privateKeySecretId).toBeDefined();

  const response = await page.request.post("/api/v1/targets", {
    data: {
      name: "Mutual TLS target",
      kind: "http",
      url: "https://example.com/health",
      method: "GET",
      accepted_statuses: [{ start: 200, end: 299 }],
      follow_redirects: true,
      max_redirects: 5,
      interval_seconds: 60,
      timeout_seconds: 10,
      failure_threshold: 3,
      locations: 1,
      headers: {},
      body: null,
      assertions: [],
      skip_tls_verification: false,
      tls_ca_secret_id: caSecretId,
      tls_client_certificate_secret_id: certificateSecretId,
      tls_client_private_key_secret_id: privateKeySecretId,
      notification_channel_ids: [],
      use_default_channels: true,
    },
  });
  expect(response.ok()).toBeTruthy();
  const created = await response.json();
  await page.goto("/");

  await expect(page.getByText("private-ca-pem")).toHaveCount(0);
  await page.getByRole("button", { name: "Mutual TLS target" }).click();
  const edit = page.getByRole("dialog", { name: "Target details" });
  await edit.getByRole("tab", { name: "General" }).click();
  await expect(edit.getByLabel("Custom CA bundle Secret")).toHaveValue(created.tls_ca_secret_id);
  await expect(edit.getByLabel("Client certificate Secret")).toHaveValue(created.tls_client_certificate_secret_id);
  await expect(edit.getByLabel("Client private key Secret")).toHaveValue(created.tls_client_private_key_secret_id);
  await edit.getByLabel("Custom CA bundle Secret").selectOption("");
  await edit.getByRole("button", { name: "Save changes" }).click();

  const updated = await (await page.request.get(`/api/v1/targets/${created.id}`)).json();
  expect(updated.tls_ca_secret_id).toBeNull();
  expect(updated.tls_client_certificate_secret_id).toBe(created.tls_client_certificate_secret_id);
  expect(updated.tls_client_private_key_secret_id).toBe(created.tls_client_private_key_secret_id);
});

test("creates and edits a TCP-connect target", async ({ page }) => {
  await page.goto("/");
  const service = new URL(page.url());
  await page.getByRole("button", { name: "Add target" }).click();
  const addTarget = page.getByRole("dialog", { name: "Add target" });

  await addTarget.getByLabel("Method").fill("POST");
  await addTarget.getByLabel("Type").selectOption("tcp");
  await expect(addTarget.getByLabel("Method")).toBeHidden();
  await expect(addTarget.getByLabel("Method")).toBeDisabled();
  await expect(addTarget.getByLabel("URL")).toHaveAttribute("placeholder", "database.internal:5432");
  await addTarget.getByLabel("Name").fill("Local TCP service");
  await addTarget.getByLabel("URL").fill(`${service.hostname}:${service.port}`);
  await addTarget.getByRole("tab", { name: "Evaluation" }).click();
  await addTarget.getByLabel("Interval (seconds)").fill("1");
  await addTarget.getByLabel("Failures before Down").fill("1");
  await addTarget.getByRole("button", { name: "Create target" }).click();

  const target = page.getByRole("button", { name: "Local TCP service" });
  await expect(target).toContainText("TCP");
  await expect(target).toContainText(`tcp://${service.hostname}:${service.port}`);
  await expect(target.locator(".state")).toHaveClass(/up/, { timeout: 15_000 });
  await target.click();

  const details = page.getByRole("dialog", { name: "Target details" });
  await details.getByRole("tab", { name: "General" }).click();
  await expect(details.getByLabel("Type")).toHaveValue("TCP");
  await expect(details.getByLabel("Method")).toHaveCount(0);
  await details.getByLabel("Name").fill("Renamed TCP service");
  await details.getByRole("button", { name: "Save changes" }).click();

  await expect(page.getByRole("button", { name: "Renamed TCP service" })).toBeVisible();
});

test("creates DNS, ICMP, and TLS target kinds", async ({ page }) => {
  await page.goto("/");
  const service = new URL(page.url());
  for (const [kind, endpoint] of [
    ["dns", "localhost"],
    ["icmp", "192.0.2.1"],
    ["tls", `${service.hostname}:${service.port}`],
  ] as const) {
    await page.getByRole("button", { name: "Add target" }).click();
    const dialog = page.getByRole("dialog", { name: "Add target" });
    await dialog.getByLabel("Type").selectOption(kind);
    await dialog.getByLabel("Name").fill(`${kind.toUpperCase()} target`);
    await dialog.getByLabel("URL").fill(endpoint);
    await dialog.getByRole("button", { name: "Create target" }).click();

    const target = page.getByRole("button", { name: `${kind.toUpperCase()} target`, exact: true });
    await expect(target).toContainText(kind.toUpperCase());
    await expect(target).toContainText(`${kind}://${endpoint}`);
  }
});

test("configures multi-location target evaluation", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "Add target" }).click();
  const create = page.getByRole("dialog", { name: "Add target" });
  await create.getByLabel("Name").fill("Multi-location target");
  await create.getByLabel("URL").fill("https://example.com/health");
  await create.getByRole("tab", { name: "Evaluation" }).click();
  await create.getByLabel("Evaluation locations").fill("3");
  await create.getByRole("button", { name: "Create target" }).click();

  const created = (await (await page.request.get("/api/v1/targets")).json()).find((target: { name: string }) => target.name === "Multi-location target");
  expect(created.locations).toBe(3);

  await expect(page.getByRole("button", { name: "Multi-location target" })).toContainText("3 locations");
  await page.getByRole("button", { name: "Multi-location target" }).click();
  const edit = page.getByRole("dialog", { name: "Target details" });
  await edit.getByRole("tab", { name: "Evaluation" }).click();
  await expect(edit.getByLabel("Evaluation locations")).toHaveValue("3");
  await edit.getByLabel("Evaluation locations").fill("2");
  await edit.getByRole("button", { name: "Save changes" }).click();

  const updated = await (await page.request.get(`/api/v1/targets/${created.id}`)).json();
  expect(updated.locations).toBe(2);
  await expect(page.getByRole("button", { name: "Multi-location target" })).toContainText("2 locations");
});

test("trashes, restores, and permanently deletes a target", async ({ page }) => {
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
  await expect(page.getByRole("heading", { name: "Long-term summary" })).toBeVisible();
  const historyResponse = await page.request.get(`/api/v1/targets/${(await (await page.request.get("/api/v1/targets")).json()).find((item: { name: string }) => item.name === "Target lifecycle").id}/history?limit=1`);
  expect(historyResponse.ok()).toBe(true);
  const longTermHistory = await historyResponse.json();
  expect(longTermHistory.items).toHaveLength(1);
  expect(longTermHistory.items[0].samples).toBeGreaterThanOrEqual(1);
  expect(longTermHistory.items[0].successes + longTermHistory.items[0].failures).toBe(longTermHistory.items[0].samples);
  await expect(page.getByLabel("Long-term evaluation summary")).toContainText(`${longTermHistory.items[0].availability_percent.toFixed(2)}%`);
  const details = page.getByRole("dialog", { name: "Target details" });
  await expect(details.locator(".dialog-head p")).toHaveCount(0);
  await expect(details.getByRole("button", { name: "Close", exact: true })).toHaveCount(0);
  await expect(details.getByRole("button", { name: "Close target details" }).locator("iconify-icon")).toBeVisible();
  await expect(details.getByRole("button", { name: "Move Target to Trash" }).locator("iconify-icon")).toBeVisible();
  const pause = details.getByRole("button", { name: "Pause evaluations" });
  await expect(pause).toHaveClass(/warning/);
  await expect(pause).toHaveCSS("background-color", "rgba(0, 0, 0, 0)");
  await expect(pause.locator("iconify-icon")).toBeVisible();
  await expect(details.locator(".danger-actions").getByRole("button")).toHaveCount(2);
  await expect(details.getByRole("button", { name: "Save changes" })).toHaveCount(0);
  const history = details.getByRole("list", { name: /Recent evaluation latency, 0 to/ });
  await expect(history).toBeVisible();
  await expect(history.getByRole("listitem").first()).toBeVisible();
  const topology = await (await page.request.get("/api/v1/cluster")).json();
  const evaluation = await history.getByRole("listitem").first().getAttribute("aria-label");
  expect(topology.members.some((member: { name: string }) => evaluation?.includes(`Executed by ${member.name}`))).toBe(true);
  await expect(details.locator(".chart-scale span")).toHaveCount(3);
  await expect(details.locator(".chart-scale").getByText("0 ms", { exact: true })).toBeVisible();
  await details.getByRole("tab", { name: "General" }).click();
  const save = details.getByRole("button", { name: "Save changes" });
  await expect(save).toBeDisabled();
  await expect(save).toHaveCSS("cursor", "not-allowed");
  const followRedirects = details.getByRole("switch", { name: "Follow redirects" });
  await followRedirects.uncheck();
  await expect(save).toBeEnabled();
  await followRedirects.check();
  await expect(save).toBeDisabled();
  const name = details.getByLabel("Name", { exact: true });
  await name.fill("Temporary name");
  await expect(save).toBeEnabled();
  await name.fill("Target lifecycle");
  await expect(save).toBeDisabled();
  await name.fill("Renamed lifecycle target");
  await expect(save).toBeEnabled();
  await details.getByRole("tab", { name: "Evaluation" }).click();
  await details.getByLabel("Failures before Down").fill("5");
  await save.click();
  await expect(page.getByText("Renamed lifecycle target")).toBeVisible();
  await page.getByRole("button", { name: "Renamed lifecycle target" }).click();
  page.once("dialog", (dialog) => dialog.accept());
  await page.getByRole("dialog", { name: "Target details" }).getByRole("button", { name: "Move Target to Trash" }).click();
  await expect(page.getByText("Renamed lifecycle target")).not.toBeVisible();

  await page.getByRole("link", { name: "Trash" }).click();
  const retentionTooltip = page.locator("#trash-retention-help");
  await expect(retentionTooltip).toBeHidden();
  await page.getByRole("button", { name: "About deleted Target retention" }).focus();
  await expect(retentionTooltip).toBeVisible();
  const trashed = page.getByRole("region", { name: "Trashed Targets" }).getByText("Renamed lifecycle target", { exact: true });
  await expect(trashed).toBeVisible();
  page.once("dialog", (dialog) => dialog.accept());
  await page.getByRole("button", { name: "Restore" }).click();
  await expect(trashed).not.toBeVisible();

  await page.getByRole("link", { name: "Overview" }).click();
  const restored = page.getByRole("button", { name: "Renamed lifecycle target" });
  await expect(restored).toBeVisible();
  await restored.click();
  const restoredDetails = page.getByRole("dialog", { name: "Target details" });
  await expect(
    restoredDetails
      .getByRole("list", { name: /Recent evaluation latency, 0 to/ })
      .getByRole("listitem")
      .first(),
  ).toBeVisible();
  page.once("dialog", (dialog) => dialog.accept());
  await restoredDetails.getByRole("button", { name: "Move Target to Trash" }).click();

  await page.getByRole("link", { name: "Trash" }).click();
  await expect(page.getByText("Renamed lifecycle target", { exact: true })).toBeVisible();
  page.once("dialog", (dialog) => dialog.accept());
  await page.getByRole("button", { name: "Delete permanently" }).click();
  await expect(page.getByText("Renamed lifecycle target", { exact: true })).not.toBeVisible();
});

test("configures notification resources and creates a join command", async ({ page }) => {
  await page.goto("/");

  await page.getByRole("button", { name: "Add secret" }).click();
  await page.getByRole("dialog", { name: "Add secret" }).getByLabel("Name").fill("Webhook token");
  await page.getByRole("dialog", { name: "Add secret" }).getByLabel("Value").fill("not-returned-by-api");
  await page.getByRole("button", { name: "Create secret" }).click();
  await expect(page.getByRole("region", { name: "Secrets", exact: true }).getByText("Webhook token", { exact: true })).toBeVisible();

  await page.getByRole("link", { name: "Alerts" }).click();
  await page.getByRole("button", { name: "Add channel" }).click();
  const channel = page.getByRole("dialog", { name: "Add channel" });
  await channel.getByLabel("Name").fill("Operations webhook");
  await channel.getByLabel("Webhook URL").fill("https://example.com/upgrid-hook");
  await channel.getByRole("button", { name: "Create channel" }).click();
  await expect(page.getByRole("region", { name: "Notification channels" }).getByText("Operations webhook", { exact: true })).toBeVisible();

  page.once("dialog", (dialog) => dialog.accept());
  await page.getByRole("button", { name: "Delete channel Operations webhook" }).click();
  await expect(page.getByRole("region", { name: "Notification channels" }).getByText("Operations webhook", { exact: true })).not.toBeVisible();
  await page.getByRole("link", { name: "Overview" }).click();
  page.once("dialog", (dialog) => dialog.accept());
  await page.getByRole("button", { name: "Delete secret Webhook token" }).click();
  await expect(page.getByRole("region", { name: "Secrets", exact: true }).getByText("Webhook token", { exact: true })).not.toBeVisible();

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
  await expect(cluster.locator(".resource code")).toContainText(expectedRaftUrl);
  await expect(cluster.getByText("Leader")).toBeVisible();
  await expect(cluster.getByText("This node")).toBeVisible();
});

test("dismisses a startup compatibility warning", async ({ page }) => {
  await page.goto(process.env.UPGRID_WARNING_URL ?? "http://127.0.0.1:18083");
  await page.getByLabel("Username").fill("admin");
  await page.getByLabel("Password").fill("test-password");
  await page.getByRole("button", { name: "Sign in" }).click();
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
  await page.getByRole("checkbox", { name: "Select Search alpha" }).check();
  const pauseSelected = page.getByRole("button", { name: "Pause selected" });
  await expect(page.getByRole("button", { name: "Unselect all" }).locator("iconify-icon")).toBeVisible();
  const actionsMargin = await page.locator(".bulk-actions").evaluate((element) => Number.parseFloat(getComputedStyle(element).marginLeft));
  expect(actionsMargin).toBeGreaterThan(0);
  await expect(pauseSelected).toHaveClass(/warning/);
  await expect(pauseSelected.locator("iconify-icon")).toBeVisible();
  await expect(page.getByRole("button", { name: "Resume selected" })).toHaveCount(0);
  await pauseSelected.click();
  await expect(page.getByRole("button", { name: "Search alpha" })).toContainText("Paused");

  await page.getByRole("checkbox", { name: "Select Search alpha" }).check();
  await expect(page.getByRole("button", { name: "Pause selected" })).toHaveCount(0);
  const resumeSelected = page.getByRole("button", { name: "Resume selected" });
  await expect(resumeSelected).toHaveClass(/success/);
  await expect(resumeSelected.locator("iconify-icon")).toBeVisible();
  await resumeSelected.click();
  await expect(page.getByRole("button", { name: "Search alpha" })).not.toContainText("Paused");

  await page.getByRole("checkbox", { name: "Select Search alpha" }).check();
  await page.getByRole("button", { name: "Unselect all" }).click();
  await expect(page.locator(".bulk")).toHaveCount(0);
  await expect(page.getByRole("checkbox", { name: "Select Search alpha" })).not.toBeChecked();
});

test("navigation opens dedicated Alert and Cluster pages", async ({ page }) => {
  await page.goto("/");

  const summary = page.getByRole("region", { name: "Target summary" });
  const secrets = page.getByRole("region", { name: "Secrets" });
  const targets = page.getByRole("region", { name: "Targets" });
  const [summaryBox, secretsBox, targetsBox] = await Promise.all([summary.boundingBox(), secrets.boundingBox(), targets.boundingBox()]);
  expect(summaryBox).not.toBeNull();
  expect(secretsBox).not.toBeNull();
  expect(targetsBox).not.toBeNull();
  expect(summaryBox!.x).toBeLessThan(secretsBox!.x);
  expect(Math.abs(summaryBox!.y - secretsBox!.y)).toBeLessThan(2);
  expect(Math.abs(summaryBox!.width - secretsBox!.width)).toBeLessThan(2);
  expect(targetsBox!.y).toBeGreaterThanOrEqual(Math.max(summaryBox!.y + summaryBox!.height, secretsBox!.y + secretsBox!.height));
  await expect(page.getByRole("region", { name: "Notification channels" })).toHaveCount(0);

  await expect(page.getByRole("link", { name: "Targets" })).toHaveCount(0);
  await page.getByRole("link", { name: "Alerts" }).click();
  await expect(page).toHaveURL(/\/alerts$/);
  await expect(page.getByRole("link", { name: "Alerts" })).toHaveClass(/active/);
  await expect(page.getByRole("heading", { name: "Alerts" })).toBeVisible();
  await expect(page.getByRole("region", { name: "Targets" })).toHaveCount(0);
  await expect(page.getByRole("region", { name: "Notification channels" })).toBeVisible();
  const [alertBox, availabilityBox, channelsBox] = await Promise.all([page.getByRole("region", { name: "Alert history" }).boundingBox(), page.getByRole("region", { name: "Availability history" }).boundingBox(), page.getByRole("region", { name: "Notification channels" }).boundingBox()]);
  expect(alertBox).not.toBeNull();
  expect(availabilityBox).not.toBeNull();
  expect(channelsBox).not.toBeNull();
  expect(alertBox!.y + alertBox!.height).toBeLessThanOrEqual(availabilityBox!.y);
  expect(availabilityBox!.x).toBeLessThan(channelsBox!.x);
  expect(Math.abs(availabilityBox!.y - channelsBox!.y)).toBeLessThan(2);
  const [availabilityHead, channelsHead] = await Promise.all([page.getByRole("region", { name: "Availability history" }).locator(".panel-head").boundingBox(), page.getByRole("region", { name: "Notification channels" }).locator(".panel-head").boundingBox()]);
  expect(Math.abs(availabilityHead!.height - channelsHead!.height)).toBeLessThan(1);
  await expect(page.getByRole("region", { name: "Notification channels" }).getByRole("button", { name: "Add channel" })).toHaveCount(0);

  await page.getByRole("link", { name: "Cluster" }).click();
  await expect(page).toHaveURL(/\/cluster$/);
  await expect(page.getByRole("link", { name: "Cluster" })).toHaveClass(/active/);
  await expect(page.getByRole("region", { name: "Cluster topology" })).toBeInViewport();
  const [nodesBox, tokensBox] = await Promise.all([page.getByRole("region", { name: "Cluster topology" }).boundingBox(), page.getByRole("region", { name: "Join tokens" }).boundingBox()]);
  expect(nodesBox).not.toBeNull();
  expect(tokensBox).not.toBeNull();
  expect(nodesBox!.x).toBeLessThan(tokensBox!.x);
  expect(Math.abs(nodesBox!.y - tokensBox!.y)).toBeLessThan(2);
  await expect(page.getByRole("heading", { name: "Alerts" })).toHaveCount(0);
});

test("keeps primary navigation visible on mobile", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/");

  const navigation = page.getByRole("navigation", { name: "Primary" });
  await expect(navigation).toBeVisible();
  await expect(navigation.getByRole("link", { name: "Overview" })).toBeVisible();
  await expect(navigation.getByRole("link", { name: "Alerts" })).toBeVisible();
  await expect(navigation.getByRole("link", { name: "Cluster" })).toBeVisible();
  await page.evaluate(() => {
    (window as typeof window & { tabScrollCalls: number }).tabScrollCalls = 0;
    Element.prototype.scrollIntoView = () => {
      (window as typeof window & { tabScrollCalls: number }).tabScrollCalls += 1;
    };
  });
  await navigation.getByRole("link", { name: "Alerts" }).click();
  await expect(page).toHaveURL(/\/alerts$/);
  await expect.poll(() => page.evaluate(() => (window as typeof window & { tabScrollCalls: number }).tabScrollCalls)).toBe(0);
});

test("aligns compact Target rows on mobile", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/");

  const row = page.locator(".target-wrap", { has: page.getByText("Node", { exact: true }) }).first();
  await expect(row.locator(".mini-chart")).toBeVisible({ timeout: 15_000 });
  const [bounds, checkbox, title, state, chart, latency] = await Promise.all([row.boundingBox(), row.locator(".select-target").boundingBox(), row.locator("h3").boundingBox(), row.locator(".state").boundingBox(), row.locator(".mini-chart").boundingBox(), row.locator(".latency").boundingBox()]);
  for (const box of [bounds, checkbox, title, state, chart, latency]) expect(box).not.toBeNull();
  expect(Math.abs(checkbox!.y + checkbox!.height / 2 - (title!.y + title!.height / 2))).toBeLessThan(8);
  expect(Math.abs(state!.y + state!.height / 2 - (title!.y + title!.height / 2))).toBeLessThan(8);
  expect(Math.abs(chart!.y + chart!.height / 2 - (latency!.y + latency!.height / 2))).toBeLessThan(8);
  expect(bounds!.height).toBeLessThan(130);
  expect(bounds!.x + bounds!.width).toBeLessThanOrEqual(390);
});

test("renames a Node Target and shows its evaluation history", async ({ page }) => {
  await page.goto("/");

  const node = page.locator(".target-wrap", { has: page.getByText("Node", { exact: true }) }).first();
  await expect(node).toBeVisible();
  await expect(node.getByText("Node", { exact: true })).toBeVisible();
  await expect(node.getByText(/RPC · up:\/\//)).toBeVisible();
  await expect(node.locator(".state")).toHaveClass(/up/, { timeout: 15_000 });
  await expect(node.locator(".select-target")).toBeDisabled();
  const [name, badge] = await Promise.all([node.locator("h3").boundingBox(), node.getByText("Node", { exact: true }).boundingBox()]);
  expect(name).not.toBeNull();
  expect(badge).not.toBeNull();
  expect(badge!.x).toBeGreaterThan(name!.x + name!.width);
  await expect(node.locator(".mini-chart")).toBeVisible({ timeout: 15_000 });

  const originalName = (await node.locator("h3").textContent())!;
  const renamed = `${originalName}-renamed`;
  await node.locator("button.target").click();
  const details = page.getByRole("dialog", { name: "Node details" });
  await expect(details.getByRole("tab", { name: "Details" })).toHaveAttribute("aria-selected", "true");
  await expect(details.getByRole("listitem").first()).toHaveAttribute("aria-label", /reachable.*Executed by/);
  await expect(details.getByRole("button", { name: "Save changes" })).toHaveCount(0);
  await details.getByRole("tab", { name: "General" }).click();
  await expect(details.getByLabel("RPC URL")).toBeDisabled();
  await expect(details.getByRole("button", { name: "Save changes" })).toBeDisabled();
  await details.getByRole("textbox", { name: "Name", exact: true }).fill(renamed);
  await details.getByRole("button", { name: "Save changes" }).click();
  await expect(page.getByRole("button", { name: renamed })).toBeVisible();

  await page.getByRole("button", { name: renamed }).click();
  await page.getByRole("dialog", { name: "Node details" }).getByRole("tab", { name: "General" }).click();
  await page.getByRole("dialog", { name: "Node details" }).getByRole("textbox", { name: "Name", exact: true }).fill(originalName);
  await page.getByRole("dialog", { name: "Node details" }).getByRole("button", { name: "Save changes" }).click();
  await expect(page.getByRole("button", { name: originalName })).toBeVisible();
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
  await expect.poll(() => target.evaluate((element) => getComputedStyle(element).backgroundColor)).not.toBe("rgba(0, 0, 0, 0)");
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
  const [create, separator, join, token, joinButton] = await Promise.all([setup.locator(".cluster-create").boundingBox(), divider.boundingBox(), setup.locator(".cluster-join").boundingBox(), setup.getByLabel("Join Token").boundingBox(), setup.getByRole("button", { name: "Join Cluster" }).boundingBox()]);
  expect(create).not.toBeNull();
  expect(separator).not.toBeNull();
  expect(join).not.toBeNull();
  expect(token).not.toBeNull();
  expect(joinButton).not.toBeNull();
  expect(create!.y + create!.height).toBeLessThanOrEqual(separator!.y);
  expect(separator!.y + separator!.height).toBeLessThanOrEqual(join!.y);
  expect(join!.y + join!.height).toBeLessThanOrEqual(590);
  expect(token!.x + token!.width).toBeLessThanOrEqual(joinButton!.x - 9);
  expect(Math.abs(token!.height - joinButton!.height)).toBeLessThan(1);

  const [shell, header, flow] = await Promise.all([page.locator(".setup-shell").boundingBox(), page.locator(".setup-shell header").boundingBox(), setup.boundingBox()]);
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
  await page.context().clearCookies();
  await page.goto(process.env.UPGRID_NEW_SETUP_URL ?? "http://127.0.0.1:18082");
  await expect(page).toHaveURL(/\/setup$/);
  const setup = page.getByRole("region", { name: "UpGrid setup" });
  await setup.getByRole("textbox", { name: "Node name" }).first().fill("playwright-primary");
  await setup.getByRole("textbox", { name: "Administrator username" }).fill("playwright-admin");
  await setup.getByLabel("Administrator password").fill("playwright-password");
  page.once("dialog", (dialog) => dialog.accept());
  await setup.getByRole("button", { name: "Create new Cluster" }).click();

  await expect(page).toHaveURL(/\/setup\/channel$/, { timeout: 20_000 });
  await page.getByLabel("Name").fill("OOBE webhook");
  await page.getByLabel("Webhook URL").fill("https://example.com/hook");
  await page.getByRole("button", { name: "Create and continue" }).click();

  await expect(page).toHaveURL(/\/setup\/target$/);
  await page.getByLabel("Name").fill("OOBE target");
  await page.getByLabel("URL").fill("https://example.com/health");
  await page.getByRole("checkbox", { name: "OOBE webhook" }).check();
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

test("drains and removes a remote Cluster Node", async ({ page }) => {
  await page.goto("/cluster");
  const topology = page.getByRole("region", { name: "Cluster topology" });
  const member = topology.locator(".resource", { hasText: "playwright-worker" });
  await expect(member.getByRole("button", { name: "Replace failed" })).toBeVisible();

  await member.getByRole("button", { name: "Drain" }).click();
  await expect(member.getByText("Draining", { exact: true })).toBeVisible();
  await member.getByRole("button", { name: "Cancel drain" }).click();
  await expect(member.getByText("Draining", { exact: true })).toHaveCount(0);

  await member.getByRole("button", { name: "Drain" }).click();
  const remove = member.getByRole("button", { name: "Remove" });
  await expect(remove).toBeVisible({ timeout: 15_000 });
  page.once("dialog", (dialog) => dialog.accept());
  await remove.click();
  await expect(member).toHaveCount(0);
});

test("discovers and cleans up unused Secrets", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "Add secret" }).click();
  const dialog = page.getByRole("dialog", { name: "Add secret" });
  await dialog.getByLabel("Name").fill("Unused cleanup Secret");
  await dialog.getByLabel("Value").fill("delete-me");
  await dialog.getByRole("button", { name: "Create secret" }).click();

  const secrets = page.getByRole("region", { name: "Secrets", exact: true });
  const unused = secrets.locator(".resource", { hasText: "Unused cleanup Secret" });
  await expect(unused).toContainText("Unused");
  page.once("dialog", (confirmation) => confirmation.accept());
  await secrets.getByRole("button", { name: /Delete unused/ }).click();
  await expect(unused).not.toBeVisible();
});
