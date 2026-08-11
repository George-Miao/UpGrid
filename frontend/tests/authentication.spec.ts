import { expect, test } from "@playwright/test";

test("requires an Operator Identity and supports logout", async ({ page }) => {
  await page.context().clearCookies();
  await page.goto("/");

  await expect(page.getByRole("heading", { name: "Sign in" })).toBeVisible();
  await page.getByLabel("Username").fill("admin");
  await page.getByLabel("Password").fill("test-password");
  await page.getByRole("button", { name: "Sign in" }).click();

  await expect(page.getByRole("heading", { name: "Overview" })).toBeVisible();
  await page.getByRole("button", { name: "Sign out" }).click();
  await expect(page.getByRole("heading", { name: "Sign in" })).toBeVisible();
});

test("manages replicated identities and revocable API Tokens", async ({ page }) => {
  const suffix = Date.now();
  const identityName = `playwright-operator-${suffix}`;
  const tokenName = `playwright-token-${suffix}`;
  await page.goto("/cluster");

  const identities = page.getByRole("region", { name: "Operator Identities" });
  const createIdentity = identities.getByRole("heading", { name: "Add administrator" }).locator("..");
  await createIdentity.getByLabel("Username").fill(identityName);
  await createIdentity.getByLabel("Password").fill("secondary-password");
  await createIdentity.getByRole("button", { name: "Add identity" }).click();
  const identity = identities.locator(".resource").last();
  await expect(identity.getByLabel("Username")).toHaveValue(identityName);
  const identityCount = await identities.locator(".resource").count();

  const tokens = page.getByRole("region", { name: "API Tokens" });
  const createToken = tokens.getByRole("heading", { name: "Create API Token" }).locator("..");
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

  page.once("dialog", (dialog) => dialog.accept());
  await identity.getByRole("button", { name: "Delete" }).click();
  await expect(identities.locator(".resource")).toHaveCount(identityCount - 1);
});
