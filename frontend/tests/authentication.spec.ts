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
  await expect(page.getByRole("menuitem", { name: "Change Password" })).toBeVisible();
  await page.getByRole("heading", { name: "Overview" }).click();
  await expect(page.getByRole("menuitem", { name: "Change Password" })).toBeHidden();
  await page.getByLabel("Account menu for admin").click();
  await page.getByRole("menuitem", { name: "Change Password" }).click();
  await expect(page).toHaveURL(/\/admin\/change-password$/);
  await expect(page.getByRole("heading", { name: "Change Password" })).toBeVisible();
  await page.getByLabel("Account menu for admin").click();
  await page.getByRole("menuitem", { name: "Logout" }).click();
  await expect(page.getByRole("heading", { name: "Sign in" })).toBeVisible();
});

test("manages replicated identities and revocable API Tokens", async ({ page }) => {
  const suffix = Date.now();
  const identityName = `playwright-operator-${suffix}`;
  const tokenName = `playwright-token-${suffix}`;
  await page.goto("/admin/users");
  await expect(page.getByRole("heading", { name: "Users" })).toBeVisible();

  await page.getByRole("button", { name: "Add User" }).click();
  const addUser = page.getByRole("dialog", { name: "Add User" });
  await addUser.getByLabel("Username").fill(identityName);
  await addUser.getByLabel("Password", { exact: true }).fill("secondary-password");
  await addUser.getByLabel("Confirm password").fill("different-password");
  await addUser.getByRole("button", { name: "Add User" }).click();
  await expect(addUser.getByLabel("Confirm password")).toHaveJSProperty("validationMessage", "Passwords do not match.");
  await addUser.getByLabel("Confirm password").fill("secondary-password");
  await addUser.getByRole("button", { name: "Add User" }).click();

  const identities = page.getByRole("region", { name: "Operator Identities" });
  const identity = identities.locator(".resource").last();
  await expect(identity).toContainText(identityName);
  const identityCount = await identities.locator(".resource").count();
  await identity.getByRole("button", { name: `Edit user ${identityName}`, exact: true }).click();
  const editUser = page.getByRole("dialog", { name: "Edit User" });
  await expect(editUser.getByLabel("Username")).toHaveValue(identityName);
  await expect(editUser.getByLabel("Confirm new password")).toBeVisible();
  await editUser.getByRole("button", { name: "Cancel" }).click();

  await page.getByLabel("Account menu for admin").click();
  await page.getByRole("menuitem", { name: "API Token" }).click();
  await expect(page).toHaveURL(/\/admin\/api-tokens$/);

  const tokens = page.getByRole("region", { name: "API Tokens" });
  await page.getByRole("button", { name: "New token" }).click();
  const createToken = page.getByRole("dialog", { name: "New API Token" });
  await createToken.getByLabel("Name").fill(tokenName);
  await createToken.getByLabel("Expires in days").fill("1");
  await createToken.getByRole("button", { name: "Create API Token" }).click();
  const issued = tokens.getByRole("status");
  await expect(issued).toContainText("Copy this token now");
  await expect(issued.locator("code")).toContainText("upgrid_");
  await issued.getByRole("button", { name: "Dismiss" }).click();

  const token = tokens.locator(".resource", { hasText: tokenName });
  page.once("dialog", (dialog) => dialog.accept());
  await token.getByRole("button", { name: "Revoke" }).click();
  await expect(token).toHaveCount(0);

  await page.goto("/admin/users");
  const updatedIdentities = page.getByRole("region", { name: "Operator Identities" });
  const updatedIdentity = updatedIdentities.locator(".resource").nth(identityCount - 1);
  page.once("dialog", (dialog) => dialog.accept());
  await updatedIdentity.getByRole("button", { name: `Delete user ${identityName}`, exact: true }).click();
  await expect(updatedIdentities.locator(".resource")).toHaveCount(identityCount - 1);
});
