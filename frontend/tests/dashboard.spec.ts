import { expect, test } from "@playwright/test";

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
  await page.getByRole("button", { name: "Resume evaluations" }).click();
  await expect(page.getByRole("button", { name: "Pausing target" })).not.toContainText("Paused");
});

test("shows the local Raft topology and leader", async ({ page }) => {
  await page.goto("/");

  const cluster = page.getByRole("region", { name: "Cluster topology" });
  await expect(cluster.getByText("up://127.0.0.1:18451")).toBeVisible();
  await expect(cluster.getByText("Leader")).toBeVisible();
  await expect(cluster.getByText("This node")).toBeVisible();
});
