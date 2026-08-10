import { createServer, type Server } from "node:http";
import { type AddressInfo } from "node:net";
import { expect, test } from "@playwright/test";

let server: Server;
let webhookUrl: string;
let webhookBody = "";
let webhookBodies: string[] = [];

test.beforeAll(async () => {
  server = createServer((request, response) => {
    const chunks: Buffer[] = [];
    request.on("data", (chunk: Buffer) => chunks.push(chunk));
    request.on("end", () => {
      webhookBody = Buffer.concat(chunks).toString("utf8");
      webhookBodies.push(webhookBody);
      response.writeHead(204).end();
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  webhookUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}/hook`;
});

test.afterAll(async () => {
  await new Promise<void>((resolve, reject) =>
    server.close((error) => error ? reject(error) : resolve()),
  );
});

test("tests a channel and places its type beside the name", async ({ page }) => {
  await page.goto("/alerts");
  await page.getByRole("button", { name: "Add channel" }).click();
  const dialog = page.getByRole("dialog", { name: "Add channel" });
  await dialog.getByLabel("Webhook URL").fill(webhookUrl);
  await dialog.getByRole("button", { name: "Send test" }).click();
  await expect(dialog.getByRole("status")).toHaveText("Test sent");
  expect(JSON.parse(webhookBody)).toMatchObject({ event: "test" });

  await dialog.getByLabel("Name").fill("Browser test webhook");
  await dialog.getByRole("button", { name: "Create channel" }).click();
  const row = page.getByRole("region", { name: "Notification channels" })
    .locator(".resource", { hasText: "Browser test webhook" });
  const [name, kind] = await Promise.all([
    row.locator("strong").boundingBox(),
    row.getByText("webhook", { exact: true }).boundingBox(),
  ]);
  expect(name).not.toBeNull();
  expect(kind).not.toBeNull();
  expect(kind!.x).toBeGreaterThan(name!.x + name!.width);
  const remove = row.getByRole("button", { name: "Delete channel Browser test webhook" });
  await expect(remove.locator("iconify-icon")).toBeVisible();
  page.once("dialog", (confirmation) => confirmation.accept());
  await remove.click();
  await expect(row).not.toBeVisible();
});

test("uses trash icons for secret and Target deletion", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "Add secret" }).click();
  const secretDialog = page.getByRole("dialog", { name: "Add secret" });
  await secretDialog.getByLabel("Name").fill("Browser delete icon secret");
  await secretDialog.getByLabel("Value").fill("temporary");
  await secretDialog.getByRole("button", { name: "Create secret" }).click();
  const deleteSecret = page.getByRole("button", {
    name: "Delete secret Browser delete icon secret",
  });
  await expect(page.getByText("write-only", { exact: true })).toHaveCount(0);
  await expect(deleteSecret.locator("iconify-icon")).toBeVisible();
  page.once("dialog", (confirmation) => confirmation.accept());
  await deleteSecret.click();

  await page.getByRole("button", { name: "Add target" }).click();
  const targetDialog = page.getByRole("dialog", { name: "Add target" });
  await targetDialog.getByLabel("Name").fill("Browser delete icon target");
  await targetDialog.getByLabel("URL").fill("http://127.0.0.1:18080/healthz");
  await targetDialog.getByRole("button", { name: "Create target" }).click();
  await page.getByLabel("Select Browser delete icon target").check();
  const deleteSelected = page.getByRole("button", { name: "Delete selected" });
  await expect(deleteSelected.locator("iconify-icon")).toBeVisible();
  await page.getByRole("button", { name: "Unselect all" }).click();

  await page.getByRole("button", { name: "Browser delete icon target" }).click();
  const deleteTarget = page.getByRole("dialog", { name: "Target details" })
    .getByRole("button", { name: "Delete target" });
  await expect(deleteTarget.locator("iconify-icon")).toBeVisible();
  page.once("dialog", (confirmation) => confirmation.accept());
  await deleteTarget.click();
});

test("default channels deliver unless a Target opts out", async ({ page }) => {
  const suffix = Date.now();
  const channelName = `Default delivery webhook ${suffix}`;
  const targetName = `Default delivery target ${suffix}`;
  const optedOutName = `Default opt-out target ${suffix}`;
  webhookBody = "";
  webhookBodies = [];
  await page.goto("/alerts");
  await page.getByRole("button", { name: "Add channel" }).click();
  const channelDialog = page.getByRole("dialog", { name: "Add channel" });
  await channelDialog.getByLabel("Name").fill(channelName);
  await channelDialog.getByLabel("Webhook URL").fill(webhookUrl);
  await channelDialog.getByRole("switch", { name: "Default channel" }).check();
  await channelDialog.getByRole("button", { name: "Create channel" }).click();
  await expect(page.getByRole("switch", {
    name: `Default channel ${channelName}`,
  })).toBeChecked();

  await page.goto("/");
  await page.getByRole("button", { name: "Add target" }).click();
  const targetDialog = page.getByRole("dialog", { name: "Add target" });
  const defaultChannel = targetDialog.getByRole("checkbox", { name: channelName });
  const useDefaults = targetDialog.getByRole("switch", { name: "Use default channels" });
  const channelHeading = targetDialog.locator(".channel-fields legend");
  const channelSection = targetDialog.locator(".channel-fields");
  const nameInput = targetDialog.getByLabel("Name");
  await expect(defaultChannel).toBeChecked();
  await expect(defaultChannel).toBeDisabled();
  await expect(channelHeading).toHaveCSS("font-size", "14px");
  await expect(channelHeading).toHaveCSS("font-weight", "400");
  await expect(nameInput).toHaveCSS("font-size", "16px");
  await expect(nameInput).toHaveCSS("min-height", "44px");
  await nameInput.focus();
  await expect(nameInput).toHaveCSS("outline-style", "solid");
  const [headingBox, sectionBox] = await Promise.all([
    channelHeading.boundingBox(),
    channelSection.boundingBox(),
  ]);
  expect(headingBox).not.toBeNull();
  expect(sectionBox).not.toBeNull();
  expect(Math.abs(
    headingBox!.x + headingBox!.width / 2 - (sectionBox!.x + sectionBox!.width / 2),
  )).toBeLessThan(2);
  await useDefaults.uncheck();
  await expect(defaultChannel).not.toBeChecked();
  await expect(defaultChannel).toBeEnabled();
  await useDefaults.check();
  await expect(defaultChannel).toBeChecked();
  await expect(defaultChannel).toBeDisabled();
  await nameInput.fill(targetName);
  await targetDialog.getByLabel("URL").fill("http://127.0.0.1:19091/");
  await targetDialog.getByLabel("Interval (seconds)").fill("1");
  await targetDialog.getByLabel("Failures before Down").fill("1");
  await targetDialog.getByRole("button", { name: "Create target" }).click();

  const target = page.getByRole("button", { name: targetName });
  await expect(target.locator(".state")).toHaveClass(/down/, { timeout: 15_000 });
  await expect.poll(() => webhookBody && JSON.parse(webhookBody)).toMatchObject({
    event: "down",
    target_name: targetName,
  });

  await page.goto("/alerts");
  const transition = page.getByRole("region", { name: "Alert history" })
    .locator(".resource", { hasText: targetName });
  await expect(transition).toContainText("down");
  await expect(transition.locator(".state")).toHaveClass(/down/);
  await expect(transition.locator(".badge")).toHaveClass(/down/);
  await expect(transition.locator(".badge")).toHaveCSS("color", "rgb(197, 52, 52)");

  await page.goto("/");
  await page.getByRole("button", { name: "Add target" }).click();
  const optedOutDialog = page.getByRole("dialog", { name: "Add target" });
  await optedOutDialog.getByLabel("Name").fill(optedOutName);
  await optedOutDialog.getByLabel("URL").fill("http://127.0.0.1:19093/");
  await optedOutDialog.getByLabel("Interval (seconds)").fill("1");
  await optedOutDialog.getByLabel("Failures before Down").fill("1");
  await optedOutDialog.getByRole("switch", { name: "Use default channels" }).uncheck();
  await optedOutDialog.getByRole("button", { name: "Create target" }).click();
  const optedOut = page.getByRole("button", { name: optedOutName });
  await expect(optedOut.locator(".state")).toHaveClass(/down/, { timeout: 15_000 });
  await page.waitForTimeout(1_500);
  expect(webhookBodies.some((body) => body.includes(optedOutName))).toBe(false);

  await target.click();
  page.once("dialog", (confirmation) => confirmation.accept());
  await page.getByRole("dialog", { name: "Target details" })
    .getByRole("button", { name: "Delete target" }).click();
  await optedOut.click();
  page.once("dialog", (confirmation) => confirmation.accept());
  await page.getByRole("dialog", { name: "Target details" })
    .getByRole("button", { name: "Delete target" }).click();
  await page.goto("/alerts");
  const channel = page.getByRole("region", { name: "Notification channels" })
    .locator(".resource", { hasText: channelName });
  page.once("dialog", (confirmation) => confirmation.accept());
  await channel.getByRole("button", { name: `Delete channel ${channelName}` }).click();
});
