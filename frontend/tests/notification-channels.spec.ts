import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { createServer as createTcpServer, type Server as TcpServer } from "node:net";
import { expect, test } from "@playwright/test";

let server: Server;
let webhookUrl: string;
let webhookBody = "";
let webhookBodies: string[] = [];
let smtpServer: TcpServer;
let smtpPort = 0;
let smtpMessage = "";

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
  smtpServer = createTcpServer((socket) => {
    socket.setEncoding("utf8");
    socket.write("220 localhost ESMTP\r\n");
    let buffer = "";
    let receivingData = false;
    socket.on("data", (chunk: string) => {
      buffer += chunk;
      let newline = buffer.indexOf("\r\n");
      while (newline >= 0) {
        const line = buffer.slice(0, newline);
        buffer = buffer.slice(newline + 2);
        if (receivingData) {
          if (line === ".") {
            receivingData = false;
            socket.write("250 queued\r\n");
          } else {
            smtpMessage += `${line}\r\n`;
          }
        } else if (line.startsWith("EHLO ")) {
          socket.write("250-localhost\r\n250 AUTH PLAIN\r\n");
        } else if (line.startsWith("AUTH PLAIN ")) {
          socket.write("235 authenticated\r\n");
        } else if (line.startsWith("MAIL FROM:") || line.startsWith("RCPT TO:")) {
          socket.write("250 OK\r\n");
        } else if (line === "DATA") {
          receivingData = true;
          socket.write("354 End data with <CR><LF>.<CR><LF>\r\n");
        } else if (line === "QUIT") {
          socket.end("221 Bye\r\n");
        } else {
          socket.destroy(new Error(`Unexpected SMTP command: ${line}`));
        }
        newline = buffer.indexOf("\r\n");
      }
    });
  });
  await new Promise<void>((resolve) => smtpServer.listen(0, "127.0.0.1", resolve));
  smtpPort = (smtpServer.address() as AddressInfo).port;
});

test.afterAll(async () => {
  await Promise.all([new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve()))), new Promise<void>((resolve, reject) => smtpServer.close((error) => (error ? reject(error) : resolve())))]);
});

test("explains Secret usage from related forms", async ({ page }) => {
  await page.goto("/");
  const reusableHelp = page.getByRole("button", { name: "About reusable Secrets" });
  const reusableTooltip = page.locator("#secrets-help");
  await expect(reusableTooltip).toBeHidden();
  await reusableHelp.focus();
  await expect(reusableTooltip).toContainText("Target headers or bodies and webhook headers");
  await expect(reusableTooltip).toBeVisible();

  await page.getByRole("button", { name: "Add target" }).click();
  const targetDialog = page.getByRole("dialog", { name: "Add target" });
  const targetTooltip = targetDialog.locator("#target-secret-help");
  await expect(targetTooltip).toBeHidden();
  await targetDialog.getByRole("button", { name: "About Target Secrets" }).focus();
  await expect(targetTooltip).toContainText("headers and request bodies");
  await expect(targetTooltip).toBeVisible();
  await targetDialog.getByRole("button", { name: "Cancel" }).click();

  await page.getByRole("link", { name: "Alerts" }).click();
  await page.getByRole("button", { name: "Add channel" }).click();
  const channelDialog = page.getByRole("dialog", { name: "Add channel" });
  await channelDialog.getByLabel("Type").selectOption("telegram");
  await channelDialog.getByRole("button", { name: "About Telegram bot token storage" }).focus();
  await expect(channelDialog.locator("#telegram-token-help")).toContainText("automatically managed Secret");
  await expect(channelDialog.locator("#telegram-token-help")).toBeVisible();
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
  const row = page.getByRole("region", { name: "Notification channels" }).locator(".resource", { hasText: "Browser test webhook" });
  const [name, kind] = await Promise.all([row.locator("strong").boundingBox(), row.getByText("webhook", { exact: true }).boundingBox()]);
  expect(name).not.toBeNull();
  expect(kind).not.toBeNull();
  expect(kind!.x).toBeGreaterThan(name!.x + name!.width);
  const remove = row.getByRole("button", { name: "Delete channel Browser test webhook" });
  await expect(remove.locator("iconify-icon")).toBeVisible();
  page.once("dialog", (confirmation) => confirmation.accept());
  await remove.click();
  await expect(row).not.toBeVisible();
});
test("creates, tests, and edits an SMTP channel", async ({ page }) => {
  const suffix = Date.now();
  const channelName = `Browser SMTP ${suffix}`;
  smtpMessage = "";
  await page.goto("/alerts");
  await page.getByRole("button", { name: "Add channel" }).click();
  const dialog = page.getByRole("dialog", { name: "Add channel" });
  await dialog.getByLabel("Type").selectOption("smtp");
  await dialog.getByLabel("Name", { exact: true }).fill(channelName);
  await dialog.getByLabel("SMTP host").fill("127.0.0.1");
  await dialog.getByLabel("Port").fill(String(smtpPort));
  await dialog.getByLabel("Security").selectOption("none");
  await dialog.getByLabel("Username").fill("upgrid");
  await dialog.getByLabel("Password", { exact: true }).fill("smtp-secret");
  await dialog.getByLabel("From").fill("upgrid@example.com");
  await dialog.getByLabel("Recipient").fill("on-call@example.com");
  await dialog.getByRole("button", { name: "Send test" }).click();
  await expect(dialog.getByRole("status")).toHaveText("Test sent");
  expect(smtpMessage).toContain("Subject: UpGrid notification channel test");
  expect(smtpMessage).toContain("UpGrid notification channel test");

  await dialog.getByRole("button", { name: "Create channel" }).click();
  const row = page.getByRole("region", { name: "Notification channels" }).locator(".resource", {
    hasText: channelName,
  });
  await expect(row).toContainText("smtp");
  await row.getByRole("button", { name: `Edit channel ${channelName}` }).click();
  const editDialog = page.getByRole("dialog", { name: "Edit channel" });
  await expect(editDialog.getByLabel("Type")).toHaveValue("smtp");
  await expect(editDialog.getByLabel("Username")).toHaveValue("upgrid");
  await expect(editDialog.getByLabel("Password", { exact: true })).toHaveValue("");
  await editDialog.getByLabel("Recipient").fill("secondary@example.com");
  await editDialog.getByRole("button", { name: "Save changes" }).click();

  await row.getByRole("button", { name: `Edit channel ${channelName}` }).click();
  await expect(page.getByRole("dialog", { name: "Edit channel" }).getByLabel("Recipient")).toHaveValue("secondary@example.com");
  await editDialog.getByRole("button", { name: "Cancel" }).click();
  page.once("dialog", (confirmation) => confirmation.accept());
  await row.getByRole("button", { name: `Delete channel ${channelName}` }).click();
  await expect(row).not.toBeVisible();
});

test("edits a channel while preserving omitted configuration", async ({ page }) => {
  const suffix = Date.now();
  const createdName = `Editable webhook ${suffix}`;
  const updatedName = `Updated webhook ${suffix}`;
  const created = await page.request.post("/api/v1/channels", {
    data: {
      type: "webhook",
      name: createdName,
      url: webhookUrl,
      headers: { "x-upgrid-test": "retained" },
      default: false,
    },
  });
  expect(created.status()).toBe(201);
  const channel = (await created.json()) as { id: string };

  const retained = await page.request.put(`/api/v1/channels/${channel.id}`, {
    data: {
      type: "webhook",
      name: createdName,
      url: webhookUrl,
      default: false,
    },
  });
  expect(retained.status()).toBe(200);
  await expect(retained.json()).resolves.toMatchObject({
    headers: { "x-upgrid-test": { kind: "literal", value: "retained" } },
  });

  await page.goto("/alerts");
  const row = page.getByRole("region", { name: "Notification channels" }).locator(".resource", {
    hasText: createdName,
  });
  await row.getByRole("button", { name: `Edit channel ${createdName}` }).click();
  const dialog = page.getByRole("dialog", { name: "Edit channel" });
  await expect(dialog.getByLabel("Type")).toBeDisabled();
  await expect(dialog.getByLabel("Type")).toHaveValue("webhook");
  await expect(dialog.getByLabel("Name")).toHaveValue(createdName);
  await dialog.getByLabel("Name").fill(updatedName);
  await dialog.getByLabel("Webhook URL").fill(`${webhookUrl}?updated=${suffix}`);
  await dialog.getByRole("switch", { name: "Default channel" }).check();
  await dialog.getByRole("button", { name: "Save changes" }).click();

  const updated = page.getByRole("region", { name: "Notification channels" }).locator(".resource", {
    hasText: updatedName,
  });
  await expect(updated).toContainText(`?updated=${suffix}`);
  await expect(updated.getByRole("switch", { name: `Default channel ${updatedName}` })).toBeChecked();

  const missing = await page.request.put("/api/v1/channels/00000000-0000-0000-0000-000000000000", {
    data: {
      type: "webhook",
      name: "Missing",
      url: webhookUrl,
      default: false,
    },
  });
  expect(missing.status()).toBe(404);
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
  const deleteTarget = page.getByRole("dialog", { name: "Target details" }).getByRole("button", { name: "Delete target" });
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
  await expect(
    page.getByRole("switch", {
      name: `Default channel ${channelName}`,
    }),
  ).toBeChecked();

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
  const [headingBox, sectionBox] = await Promise.all([channelHeading.boundingBox(), channelSection.boundingBox()]);
  expect(headingBox).not.toBeNull();
  expect(sectionBox).not.toBeNull();
  expect(Math.abs(headingBox!.x + headingBox!.width / 2 - (sectionBox!.x + sectionBox!.width / 2))).toBeLessThan(2);
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
  const downMetric = page.getByRole("region", { name: "Target summary" }).locator(".metric.down");
  await expect(downMetric).toHaveClass(/active/);
  await expect(downMetric.locator("strong")).toHaveCSS("color", "rgb(197, 52, 52)");
  await expect
    .poll(() => webhookBody && JSON.parse(webhookBody))
    .toMatchObject({
      event: "down",
      target_name: targetName,
    });

  await page.goto("/alerts");
  const transition = page.getByRole("region", { name: "Alert history" }).locator(".resource", { hasText: targetName });
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
  await page.getByRole("dialog", { name: "Target details" }).getByRole("button", { name: "Delete target" }).click();
  await expect(target).not.toBeVisible();
  await optedOut.click();
  page.once("dialog", (confirmation) => confirmation.accept());
  await page.getByRole("dialog", { name: "Target details" }).getByRole("button", { name: "Delete target" }).click();
  await page.goto("/alerts");
  const channel = page.getByRole("region", { name: "Notification channels" }).locator(".resource", { hasText: channelName });
  page.once("dialog", (confirmation) => confirmation.accept());
  await channel.getByRole("button", { name: `Delete channel ${channelName}` }).click();
});
