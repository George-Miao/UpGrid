import { createServer, type Server } from "node:http";
import { type AddressInfo } from "node:net";
import { expect, test } from "@playwright/test";

let server: Server;
let webhookUrl: string;
let webhookBody = "";

test.beforeAll(async () => {
  server = createServer((request, response) => {
    const chunks: Buffer[] = [];
    request.on("data", (chunk: Buffer) => chunks.push(chunk));
    request.on("end", () => {
      webhookBody = Buffer.concat(chunks).toString("utf8");
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
