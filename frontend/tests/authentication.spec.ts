import { expect, test } from "@playwright/test";

test("requires an Operator Identity and supports logout", async ({ page }) => {
  await page.context().clearCookies();
  await page.goto("/");

  await expect(page.getByRole("heading", { name: "Sign in" })).toBeVisible();
  await page.getByLabel("Username").fill("admin");
  await page.getByLabel("Password").fill("test-password");
  await page.getByRole("button", { name: "Sign in" }).click();

  await expect(page.getByRole("heading", { name: "Overview" })).toBeVisible();
  await page.getByLabel("Account menu for admin").click();
  await expect(page.getByRole("menuitem", { name: "Change password" })).toBeVisible();
  await page.getByRole("heading", { name: "Overview" }).click();
  await expect(page.getByRole("menuitem", { name: "Change password" })).toBeHidden();
  await page.getByLabel("Account menu for admin").click();
  await page.getByRole("menuitem", { name: "Change password" }).click();
  await expect(page).toHaveURL(/\/admin\/change-password$/);
  await expect(page.getByRole("heading", { name: "Change password" })).toBeVisible();
  await page.getByLabel("Account menu for admin").click();
  await page.getByRole("menuitem", { name: "Logout" }).click();
  await expect(page.getByRole("heading", { name: "Sign in" })).toBeVisible();
});

test("controls public status access from Manage", async ({ page, browser, baseURL }) => {
  await page.goto("/admin/manage");
  await expect(page.getByRole("heading", { name: "Manage" })).toBeVisible();
  const publicAccess = page.getByRole("switch", { name: "Allow status viewing without login" });
  await expect(publicAccess).not.toBeChecked();

  const guest = await browser.newContext();
  await guest.clearCookies();
  const guestPage = await guest.newPage();
  await guestPage.clock.install();
  await guestPage.goto("/");
  await expect(guestPage.getByRole("heading", { name: "Sign in" })).toBeVisible();
  await expect((await guest.request.get(`${baseURL}/api/v1/status`)).status()).toBe(401);

  await publicAccess.check();
  await page.getByRole("button", { name: "Save changes" }).click();
  await expect(publicAccess).toBeChecked();

  const response = await guest.request.get(`${baseURL}/api/v1/status`);
  expect(response.status()).toBe(200);
  expect(Object.keys(await response.json())).toEqual(["targets"]);
  await guestPage.reload();
  await expect(guestPage.getByRole("heading", { name: "Status" })).toBeVisible();
  await expect(guestPage.getByRole("button", { name: "Sign in" })).toBeVisible();

  let markPollStarted!: () => void;
  const pollStarted = new Promise<void>((resolve) => {
    markPollStarted = resolve;
  });
  let releasePoll!: () => void;
  const pollReleased = new Promise<void>((resolve) => {
    releasePoll = resolve;
  });
  let markPollFinished!: () => void;
  const pollFinished = new Promise<void>((resolve) => {
    markPollFinished = resolve;
  });
  await guestPage.route("**/api/v1/status", async (route) => {
    markPollStarted();
    await pollReleased;
    const pollResponse = await route.fetch();
    await route.fulfill({ response: pollResponse });
    markPollFinished();
  });
  await guestPage.clock.fastForward(30_000);
  await pollStarted;
  await guestPage.getByRole("button", { name: "Sign in" }).click();
  await expect(guestPage.getByRole("heading", { name: "Sign in" })).toBeVisible();
  releasePoll();
  await pollFinished;
  await guestPage.clock.runFor(100);
  await expect(guestPage.getByRole("heading", { name: "Sign in" })).toBeVisible();
  await guestPage.unroute("**/api/v1/status");

  await publicAccess.uncheck();
  await page.getByRole("button", { name: "Save changes" }).click();
  await expect(publicAccess).not.toBeChecked();
  await guestPage.reload();
  await expect(guestPage.getByRole("heading", { name: "Sign in" })).toBeVisible();
  await guest.close();
});

test("manages replicated identities and revocable API Tokens", async ({ page }) => {
  const suffix = Date.now();
  const identityName = `playwright-operator-${suffix}`;
  const tokenName = `playwright-token-${suffix}`;
  await page.goto("/admin/users");
  await expect(page.getByRole("heading", { name: "Users" })).toBeVisible();

  await page.getByRole("button", { name: "Add user" }).click();
  const addUser = page.getByRole("dialog", { name: "Add user" });
  await addUser.getByLabel("Username").fill(identityName);
  await addUser.getByLabel("Password", { exact: true }).fill("secondary-password");
  await addUser.getByLabel("Confirm password").fill("different-password");
  await addUser.getByRole("button", { name: "Add user" }).click();
  await expect(addUser.getByLabel("Confirm password")).toHaveJSProperty("validationMessage", "Passwords do not match.");
  await addUser.getByLabel("Confirm password").fill("secondary-password");
  await addUser.getByRole("button", { name: "Add user" }).click();

  const identities = page.getByRole("region", { name: "Operator identities" });
  const identity = identities.locator(".resource").last();
  await expect(identity).toContainText(identityName);
  const identityCount = await identities.locator(".resource").count();
  await identity.getByRole("button", { name: `Edit user ${identityName}`, exact: true }).click();
  const editUser = page.getByRole("dialog", { name: "Edit user" });
  await expect(editUser.getByLabel("Username")).toHaveValue(identityName);
  await expect(editUser.getByLabel("Confirm new password")).toBeVisible();
  await editUser.getByRole("button", { name: "Cancel" }).click();

  await page.getByLabel("Account menu for admin").click();
  await page.getByRole("menuitem", { name: "API token" }).click();
  await expect(page).toHaveURL(/\/admin\/api-tokens$/);

  const tokens = page.getByRole("region", { name: "API tokens" });
  await page.getByRole("button", { name: "New token" }).click();
  const createToken = page.getByRole("dialog", { name: "New API token" });
  await createToken.getByLabel("Name").fill(tokenName);
  await createToken.getByLabel("Expires in days").fill("1");
  await createToken.getByRole("button", { name: "Create API token" }).click();
  const issued = tokens.getByRole("status");
  await expect(issued).toContainText("Copy this token now");
  await expect(issued.locator("code")).toContainText("upgrid_");
  await issued.getByRole("button", { name: "Dismiss" }).click();

  const token = tokens.locator(".resource", { hasText: tokenName });
  page.once("dialog", (dialog) => dialog.accept());
  await token.getByRole("button", { name: "Revoke" }).click();
  await expect(token).toHaveCount(0);

  await page.goto("/admin/users");
  const updatedIdentities = page.getByRole("region", { name: "Operator identities" });
  const updatedIdentity = updatedIdentities.locator(".resource").nth(identityCount - 1);
  page.once("dialog", (dialog) => dialog.accept());
  await updatedIdentity.getByRole("button", { name: `Delete user ${identityName}`, exact: true }).click();
  await expect(updatedIdentities.locator(".resource")).toHaveCount(identityCount - 1);
});
