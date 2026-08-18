import { html, nothing } from "lit";
import deleteIcon from "@iconify-icons/lucide/trash-2";
import type { ApiToken, Identity, ManageSettings } from "./api.ts";

export interface AuthActions {
  login: (event: SubmitEvent) => void;
  logout: () => void;
  createIdentity: (event: SubmitEvent) => void;
  openAddUser: () => void;
  closeAddUser: () => void;
  openEditUser: (identity: Identity) => void;
  closeEditUser: () => void;
  openApiToken: () => void;
  closeApiToken: () => void;
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
          <div><span class="eyebrow">Cluster access</span><h1 id="login-title">Sign in</h1><p class="meta">Use a replicated operator identity.</p></div>
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
    <div class="admin-page change-password-page">
      <div class="heading"><div><span class="eyebrow">Administration</span><h1>Change password</h1></div></div>
      <section class="panel auth-panel">
        <form class="choice" @submit=${(event: SubmitEvent) => actions.updateIdentity(identity, event)}>
          <input name="username" type="hidden" .value=${identity.username} />
          <label>Username<input .value=${identity.username} autocomplete="username" disabled /></label>
          <label>New password<input name="password" type="password" minlength="12" autocomplete="new-password" required autofocus /></label>
          <label>Confirm new password<input name="password_confirmation" type="password" minlength="12" autocomplete="new-password" required @input=${(event: Event) => (event.currentTarget as HTMLInputElement).setCustomValidity("")} /></label>
          <div class="dialog-actions"><button class="button" type="submit" ?disabled=${saving}>Change password</button></div>
        </form>
      </section>
    </div>`;
}

export function renderUsersPage(identities: Identity[], currentIdentityId: string | undefined, editingIdentity: Identity | undefined, saving: boolean, actions: AuthActions) {
  return html`
    <div class="admin-page">
      <div class="heading"><div><span class="eyebrow">Administration</span><h1>Users</h1></div><button class="button" type="button" @click=${actions.openAddUser}>Add user</button></div>
      <section class="panel" aria-label="Operator identities">
        <div class="panel-head"><h2>Operator identities</h2><span class="meta">${identities.length} administrators</span></div>
        ${identities.map(
          (identity) => html`
            <div class="resource user-resource">
              <button class="resource-main" type="button" aria-label=${`Edit user ${identity.username}`} ?disabled=${saving} @click=${() => actions.openEditUser(identity)}>
                <span>
                  <strong>${identity.username}</strong>
                  <code>Operator identity${identity.id === currentIdentityId ? " · current user" : ""}</code>
                </span>
              </button>
              <button class="button danger icon-button" type="button" aria-label=${`Delete user ${identity.username}`} title=${`Delete ${identity.username}`} ?disabled=${identity.id === currentIdentityId || saving} @click=${() => actions.deleteIdentity(identity)}><iconify-icon .icon=${deleteIcon} aria-hidden="true"></iconify-icon></button>
            </div>`,
        )}
      </section>
    </div>
    <dialog id="add-user-dialog" aria-labelledby="add-user-title" @click=${actions.dismissDialog}>
      <div class="dialog-head"><h2 id="add-user-title">Add user</h2></div>
      <form @submit=${actions.createIdentity}>
        <label>Username<input name="username" autocomplete="username" required autofocus /></label>
        <label>Password<input name="password" type="password" minlength="12" autocomplete="new-password" required /></label>
        <label>Confirm password<input name="password_confirmation" type="password" minlength="12" autocomplete="new-password" required @input=${(event: Event) => (event.currentTarget as HTMLInputElement).setCustomValidity("")} /></label>
        <div class="dialog-actions"><button class="button secondary" type="button" @click=${actions.closeAddUser}>Cancel</button><button class="button" type="submit" ?disabled=${saving}>${saving ? "Adding…" : "Add user"}</button></div>
      </form>
    </dialog>
    ${
      editingIdentity
        ? html`
          <dialog id="edit-user-dialog" aria-labelledby="edit-user-title" @click=${actions.dismissDialog}>
            <div class="dialog-head"><h2 id="edit-user-title">Edit user</h2></div>
            <form @submit=${(event: SubmitEvent) => actions.updateIdentity(editingIdentity, event)}>
              <label>Username<input name="username" .value=${editingIdentity.username} autocomplete="username" required autofocus /></label>
              <label>New password<input name="password" type="password" minlength="12" autocomplete="new-password" placeholder="Keep current password" /></label>
              <label>Confirm new password<input name="password_confirmation" type="password" minlength="12" autocomplete="new-password" placeholder="Keep current password" @input=${(event: Event) => (event.currentTarget as HTMLInputElement).setCustomValidity("")} /></label>
              <div class="dialog-actions"><button class="button secondary" type="button" @click=${actions.closeEditUser}>Cancel</button><button class="button" type="submit" ?disabled=${saving}>Save changes</button></div>
            </form>
          </dialog>`
        : nothing
    }`;
}

export function renderApiTokensPage(tokens: ApiToken[], newToken: string, saving: boolean, actions: AuthActions) {
  return html`
    <div class="admin-page">
      <div class="heading"><div><span class="eyebrow">Administration</span><h1>API tokens</h1></div><button class="button" type="button" @click=${actions.openApiToken}>New token</button></div>
      <section class="panel" aria-label="API tokens">
        <div class="panel-head"><h2>API tokens</h2><span class="meta">${tokens.length} active</span></div>
        ${newToken ? html`<div class="notice token-value" role="status"><strong>Copy this token now.</strong><code>${newToken}</code><button class="button secondary" @click=${actions.dismissToken}>Dismiss</button></div>` : nothing}
        ${
          tokens.length
            ? tokens.map((token) => html`<div class="resource"><div><strong>${token.name}</strong><code>${token.expires_at_ms ? `Expires ${new Date(token.expires_at_ms).toLocaleString()}` : "Never expires"}</code></div><button class="button danger" @click=${() => actions.revokeApiToken(token)}>Revoke</button></div>`)
            : html`<div class="empty">No API tokens.</div>`
        }
      </section>
    </div>
    <dialog id="api-token-dialog" aria-labelledby="api-token-title" @click=${actions.dismissDialog}>
      <div class="dialog-head"><h2 id="api-token-title">New API token</h2></div>
      <form @submit=${actions.createApiToken}>
        <label>Name<input name="name" placeholder="Automation" required autofocus /></label>
        <label>Expires in days<input name="expires_in_days" type="number" min="1" max="365" placeholder="Never" /></label>
        <div class="dialog-actions"><button class="button secondary" type="button" @click=${actions.closeApiToken}>Cancel</button><button class="button" type="submit" ?disabled=${saving}>${saving ? "Creating…" : "Create API token"}</button></div>
      </form>
    </dialog>`;
}

export function renderManagePage(settings: ManageSettings | undefined, saving: boolean, update: (event: SubmitEvent) => void) {
  return html`
    <div class="admin-page">
      <div class="heading"><div><span class="eyebrow">Administration</span><h1>Manage</h1></div></div>
      <section class="panel" aria-labelledby="public-access-title">
        <div class="panel-head"><h2 id="public-access-title">Public status access</h2></div>
        <form @submit=${update}>
          <label class="switch">
            <span class="setting-copy">
              Allow status viewing without login
              <small>External visitors can see target names, states, and recent evaluation metrics. URLs, configuration, alerts, cluster data, and administration remain private.</small>
            </span>
            <input
              class="switch-control"
              name="public_status_enabled"
              type="checkbox"
              role="switch"
              .checked=${settings?.public_status_enabled ?? false}
              ?disabled=${settings === undefined || saving}
            />
          </label>
          <div class="dialog-actions"><button class="button" type="submit" ?disabled=${settings === undefined || saving}>${saving ? "Saving…" : "Save changes"}</button></div>
        </form>
      </section>
    </div>`;
}
