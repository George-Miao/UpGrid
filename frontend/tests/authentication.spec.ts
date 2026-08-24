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
  const [headingBox, passwordPanelBox] = await Promise.all([page.getByRole("heading", { name: "Change password" }).boundingBox(), page.locator(".change-password-page .auth-panel").boundingBox()]);
  expect(headingBox).not.toBeNull();
  expect(passwordPanelBox).not.toBeNull();
  expect(Math.abs(headingBox!.x - passwordPanelBox!.x)).toBeLessThan(1);
  await page.getByLabel("Account menu for admin").click();
  await page.getByRole("menuitem", { name: "Logout" }).click();
  await expect(page.getByRole("heading", { name: "Sign in" })).toBeVisible();
});
test("refreshes an expired browser session without showing an error", async ({ page }) => {
  let sessionRequests = 0;
  let targetRequests = 0;
  const refreshedCookies: Array<string | null> = [];
  page.on("request", (request) => {
    if (new URL(request.url()).pathname === "/api/v1/auth/session") sessionRequests += 1;
  });
  page.on("response", (response) => {
    if (new URL(response.url()).pathname === "/api/v1/auth/session") {
      void response.headerValue("set-cookie").then((cookie) => refreshedCookies.push(cookie));
    }
  });
  await page.route("**/api/v1/targets", async (route) => {
    if (targetRequests++ === 0) {
      await route.fulfill({
        status: 401,
        contentType: "application/json",
        body: JSON.stringify({ error: "authentication required" }),
      });
      return;
    }
    await route.fallback();
  });

  await page.goto("/");

  await expect(page.getByRole("heading", { name: "Overview" })).toBeVisible();
  await expect.poll(() => sessionRequests).toBe(2);
  await expect.poll(() => refreshedCookies.some((cookie) => cookie?.includes("upgrid_session="))).toBe(true);
  await expect(page.getByText("authentication required", { exact: true })).toHaveCount(0);

  const refreshCookie = (await page.context().cookies()).find((cookie) => cookie.name === "upgrid_session_refresh");
  expect(refreshCookie).toBeDefined();
  await page.context().clearCookies({ name: "upgrid_session" });
  sessionRequests = 0;
  refreshedCookies.length = 0;

  await page.reload();

  await expect(page.getByRole("heading", { name: "Overview" })).toBeVisible();
  await expect.poll(() => refreshedCookies.some((cookie) => cookie?.includes("upgrid_session="))).toBe(true);
  await expect(page.getByText("authentication required", { exact: true })).toHaveCount(0);
});

for (const [clock, skew] of [
  ["behind", -24 * 60 * 60 * 1_000],
  ["ahead", 24 * 60 * 60 * 1_000],
] as const) {
  test(`rotates a browser session with the client clock ${clock}`, async ({ page }) => {
    await page.clock.install();
    await page.clock.setSystemTime(Date.now() + skew);
    let sessionRequests = 0;
    page.on("request", (request) => {
      if (new URL(request.url()).pathname === "/api/v1/auth/session") sessionRequests += 1;
    });

    await page.goto("/");
    await expect(page.getByRole("heading", { name: "Overview" })).toBeVisible();
    await expect.poll(() => sessionRequests).toBe(1);

    await page.clock.fastForward(14 * 60 * 1_000 + 5_000);
    await expect.poll(() => sessionRequests).toBe(2);
    await expect(page.getByText("authentication required", { exact: true })).toHaveCount(0);
  });
}

test("finishes an active session refresh before logout", async ({ page }) => {
  await page.goto("/admin/manage");
  await expect(page.getByRole("heading", { name: "Manage" })).toBeVisible();

  let releaseRefresh = () => {};
  const refreshReleased = new Promise<void>((resolve) => {
    releaseRefresh = resolve;
  });
  let markRefreshStarted = () => {};
  const refreshStarted = new Promise<void>((resolve) => {
    markRefreshStarted = resolve;
  });
  await page.route("**/api/v1/auth/session", async (route) => {
    markRefreshStarted();
    await refreshReleased;
    await route.fallback();
  });

  const publicAccess = page.getByRole("switch", { name: "Allow status viewing without login" });
  const initiallyEnabled = await publicAccess.isChecked();
  let settingsRequests = 0;
  await page.route("**/api/v1/settings", async (route) => {
    if (route.request().method() !== "PUT") {
      await route.fallback();
      return;
    }
    settingsRequests += 1;
    await route.fulfill({
      status: settingsRequests === 1 ? 401 : 200,
      contentType: "application/json",
      body: JSON.stringify(settingsRequests === 1 ? { error: "authentication required" } : { public_status_enabled: initiallyEnabled }),
    });
  });

  if (initiallyEnabled) await publicAccess.uncheck();
  else await publicAccess.check();
  await page.getByRole("button", { name: "Save changes" }).click();
  await refreshStarted;

  let logoutRequests = 0;
  page.on("request", (request) => {
    if (new URL(request.url()).pathname === "/api/v1/auth/logout") logoutRequests += 1;
  });
  await page.getByLabel("Account menu for admin").click();
  try {
    await page.getByRole("menuitem", { name: "Logout" }).click();
    expect(logoutRequests).toBe(0);
  } finally {
    releaseRefresh();
  }

  await expect.poll(() => logoutRequests).toBe(1);
  await expect(page.getByRole("heading", { name: "Sign in" })).toBeVisible();
  await expect.poll(async () => (await page.context().cookies()).some((cookie) => cookie.name === "upgrid_session" || cookie.name === "upgrid_session_refresh")).toBe(false);
});

test("redirects to sign in when session rotation fails", async ({ page }) => {
  await page.goto("/alerts");
  await expect(page.getByRole("heading", { name: "Alerts", level: 1 })).toBeVisible();
  await page.route("**/api/v1/auth/session", (route) =>
    route.fulfill({
      status: 503,
      contentType: "application/json",
      body: JSON.stringify({ error: "authentication required" }),
    }),
  );

  await page.reload();

  await expect(page).toHaveURL(/^https?:\/\/[^/]+\/$/);
  await expect(page.getByRole("heading", { name: "Sign in" })).toBeVisible();
  await expect(page.getByText("authentication required", { exact: true })).toHaveCount(0);
});

test("controls public status access from Manage", async ({ page, browser, baseURL }) => {
  await page.goto("/admin/manage");
  await expect(page.getByRole("heading", { name: "Manage" })).toBeVisible();
  const publicAccess = page.getByRole("switch", { name: "Allow status viewing without login" });
  await expect(publicAccess).not.toBeChecked();
  const saveChanges = page.getByRole("button", { name: "Save changes" });
  await expect(saveChanges).toBeDisabled();
  await expect(page.getByRole("main").getByRole("tooltip")).toBeHidden();

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
  const addUserSubmit = addUser.getByRole("button", { name: "Add user" });
  await expect(addUserSubmit).toBeDisabled();
  await addUser.locator("upgrid-form-submit").hover();
  await expect(addUser.getByRole("tooltip")).toContainText("Confirm password: Passwords do not match.");
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
