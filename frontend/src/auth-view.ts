import { html, nothing } from "lit";
import type { ApiToken, Identity } from "./api.ts";

export interface AuthActions {
  login: (event: SubmitEvent) => void;
  logout: () => void;
  createIdentity: (event: SubmitEvent) => void;
  openAddUser: () => void;
  closeAddUser: () => void;
  dismissDialog: (event: MouseEvent) => void;
  updateIdentity: (identity: Identity, event: SubmitEvent) => void;
  deleteIdentity: (identity: Identity) => void;
  createApiToken: (event: SubmitEvent) => void;
  revokeApiToken: (token: ApiToken) => void;
  dismissToken: () => void;
}

export function renderLogin(saving: boolean, error: string, actions: AuthActions) {
  return html`
    <main class="shell setup-shell">
      <header>
        <div class="brand"><img src="/favicon.svg" alt="" /><div><strong>UpGrid</strong><span>Distributed service monitoring</span></div></div>
      </header>
      <section class="panel auth-panel" aria-labelledby="login-title">
        <form class="choice" @submit=${actions.login}>
          <div><span class="eyebrow">Cluster access</span><h1 id="login-title">Sign in</h1><p class="meta">Use a replicated Operator Identity.</p></div>
          ${error ? html`<div class="notice" role="alert">${error}</div>` : nothing}
          <label>Username<input name="username" autocomplete="username" required autofocus /></label>
          <label>Password<input name="password" type="password" autocomplete="current-password" required /></label>
          <div class="dialog-actions"><button class="button" type="submit" ?disabled=${saving}>${saving ? "Signing in…" : "Sign in"}</button></div>
        </form>
      </section>
    </main>`;
}

export function renderChangePassword(identity: Identity | undefined, saving: boolean, actions: AuthActions) {
  if (!identity) return html`<div class="empty">Current identity unavailable.</div>`;
  return html`
    <div class="admin-page">
      <div class="heading"><div><span class="eyebrow">Administration</span><h1>Change Password</h1></div></div>
      <section class="panel auth-panel">
        <form class="choice" @submit=${(event: SubmitEvent) => actions.updateIdentity(identity, event)}>
          <input name="username" type="hidden" .value=${identity.username} />
          <label>Username<input .value=${identity.username} autocomplete="username" disabled /></label>
          <label>New password<input name="password" type="password" minlength="12" autocomplete="new-password" required autofocus /></label>
          <div class="dialog-actions"><button class="button" type="submit" ?disabled=${saving}>Change Password</button></div>
        </form>
      </section>
    </div>`;
}

export function renderUsersPage(identities: Identity[], currentIdentityId: string | undefined, saving: boolean, actions: AuthActions) {
  return html`
    <div class="admin-page">
      <div class="heading"><div><span class="eyebrow">Administration</span><h1>Users</h1></div><button class="button" type="button" @click=${actions.openAddUser}>Add User</button></div>
      <section class="panel" aria-label="Operator Identities">
        <div class="panel-head"><h2>Operator Identities</h2><span class="meta">${identities.length} administrators</span></div>
        ${identities.map(
          (identity) => html`
            <div class="resource access-resource">
              <form class="access-form" @submit=${(event: SubmitEvent) => actions.updateIdentity(identity, event)}>
                <label>Username<input name="username" .value=${identity.username} required /></label>
                <label>New password<input name="password" type="password" minlength="12" autocomplete="new-password" placeholder="Keep current password" /></label>
                <button class="button secondary" type="submit" ?disabled=${saving}>Save</button>
              </form>
              <button class="button danger" type="button" ?disabled=${identity.id === currentIdentityId || saving} @click=${() => actions.deleteIdentity(identity)}>Delete</button>
            </div>`,
        )}
      </section>
    </div>
    <dialog id="add-user-dialog" aria-labelledby="add-user-title" @click=${actions.dismissDialog}>
      <div class="dialog-head"><h2 id="add-user-title">Add User</h2><p>Create a replicated Operator Identity.</p></div>
      <form @submit=${actions.createIdentity}>
        <label>Username<input name="username" autocomplete="username" required autofocus /></label>
        <label>Password<input name="password" type="password" minlength="12" autocomplete="new-password" required /></label>
        <div class="dialog-actions"><button class="button secondary" type="button" @click=${actions.closeAddUser}>Cancel</button><button class="button" type="submit" ?disabled=${saving}>${saving ? "Adding…" : "Add User"}</button></div>
      </form>
    </dialog>`;
}

export function renderApiTokensPage(tokens: ApiToken[], newToken: string, saving: boolean, actions: AuthActions) {
  return html`
    <div class="admin-page">
      <div class="heading"><div><span class="eyebrow">Administration</span><h1>API Tokens</h1></div></div>
      <section class="panel" aria-label="API Tokens">
        <div class="panel-head"><h2>API Tokens</h2><span class="meta">${tokens.length} active</span></div>
        ${newToken ? html`<div class="notice token-value" role="status"><strong>Copy this token now.</strong><code>${newToken}</code><button class="button secondary" @click=${actions.dismissToken}>Dismiss</button></div>` : nothing}
        ${
          tokens.length
            ? tokens.map((token) => html`<div class="resource"><div><strong>${token.name}</strong><code>${token.expires_at_ms ? `Expires ${new Date(token.expires_at_ms).toLocaleString()}` : "Never expires"}</code></div><button class="button danger" @click=${() => actions.revokeApiToken(token)}>Revoke</button></div>`)
            : html`<div class="empty">No API Tokens.</div>`
        }
        <form class="choice compact-form" @submit=${actions.createApiToken}>
          <h3>Create API Token</h3>
          <label>Name<input name="name" placeholder="Automation" required /></label>
          <label>Expires in days<input name="expires_in_days" type="number" min="1" max="365" placeholder="Never" /></label>
          <button class="button" type="submit" ?disabled=${saving}>Create API Token</button>
        </form>
      </section>
    </div>`;
}
