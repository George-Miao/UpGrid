import { html, nothing } from "lit";
import "@/component/switch.ts";
import deleteIcon from "@iconify-icons/lucide/trash-2";
import type { ApiToken, Identity, ManageSettings } from "@/app/api.ts";
import "@/component/empty-state.ts";
import "@/component/icon-button.ts";
import { renderCard } from "@/component/card.ts";
import { renderFormSubmit } from "@/component/form-submit.ts";
import { renderHeaderBrand } from "@/component/header-brand.ts";

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
  changed: () => void;
}

function validatePasswordConfirmation(event: Event): void {
  const form = event.currentTarget as HTMLFormElement;
  const password = form.elements.namedItem("password");
  const confirmation = form.elements.namedItem("password_confirmation");
  if (!(password instanceof HTMLInputElement) || !(confirmation instanceof HTMLInputElement)) return;
  confirmation.setCustomValidity(confirmation.value && confirmation.value !== password.value ? "Passwords do not match." : "");
}

export function renderLogin(online: boolean, saving: boolean, error: string, actions: AuthActions) {
  return html`
    <main class="shell setup-shell">
      <header>
        ${renderHeaderBrand(online)}
      </header>
      ${renderCard({
        label: "Sign in",
        className: "auth-panel",
        content: html`
          <form class="choice" @submit=${actions.login} @input=${actions.changed}>
            <div><span class="eyebrow">Cluster access</span><h1 id="login-title">Sign in</h1><p class="meta">Use a replicated operator identity.</p></div>
            ${error ? html`<div class="notice" role="alert">${error}</div>` : nothing}
            <label>Username<input name="username" autocomplete="username" required autofocus /></label>
            <label>Password<input name="password" type="password" autocomplete="current-password" required /></label>
            <div class="dialog-actions">${renderFormSubmit({ label: saving ? "Signing in..." : "Sign in", busy: saving, error })}</div>
          </form>
        `,
      })}
    </main>`;
}

export function renderChangePassword(identity: Identity | undefined, saving: boolean, error: string, actions: AuthActions) {
  if (!identity) return html`<upgrid-empty-state>Current identity unavailable</upgrid-empty-state>`;
  return html`
    <div class="admin-page change-password-page">
      <div class="heading"><div><span class="eyebrow">Administration</span><h1>Change password</h1></div></div>
      ${renderCard({
        className: "auth-panel",
        content: html`
          <form class="choice" @submit=${(event: SubmitEvent) => actions.updateIdentity(identity, event)} @input=${(event: Event) => {
            validatePasswordConfirmation(event);
            actions.changed();
          }}>
            <input name="username" type="hidden" .value=${identity.username} />
            <label>Username<input .value=${identity.username} autocomplete="username" disabled /></label>
            <label>New password<input name="password" type="password" minlength="12" autocomplete="new-password" required autofocus /></label>
            <label>Confirm new password<input name="password_confirmation" type="password" minlength="12" autocomplete="new-password" required /></label>
            <div class="dialog-actions">${renderFormSubmit({ label: "Change password", busy: saving, error })}</div>
          </form>
        `,
      })}
    </div>`;
}

export function renderUsersPage(identities: Identity[], currentIdentityId: string | undefined, editingIdentity: Identity | undefined, saving: boolean, error: string, actions: AuthActions) {
  return html`
    <div class="admin-page">
      <div class="heading"><div><span class="eyebrow">Administration</span><h1>Users</h1></div><button class="button" type="button" @click=${actions.openAddUser}>Add user</button></div>
      ${renderCard({
        title: "Operator identities",
        metadata: `${identities.length} administrators`,
        content: html`
          ${identities.map(
            (identity) => html`
              <div class="resource user-resource">
                <button class="resource-main" type="button" aria-label=${`Edit user ${identity.username}`} ?disabled=${saving} @click=${() => actions.openEditUser(identity)}>
                  <span>
                    <strong>${identity.username}</strong>
                    <code>Operator identity${identity.id === currentIdentityId ? " · current user" : ""}</code>
                  </span>
                </button>
                <upgrid-icon-button .icon=${deleteIcon} label=${`Delete user ${identity.username}`} title=${`Delete ${identity.username}`} variant="danger" ?disabled=${identity.id === currentIdentityId || saving} @click=${() => actions.deleteIdentity(identity)}></upgrid-icon-button>
              </div>
            `,
          )}
        `,
      })}
    </div>
    <dialog id="add-user-dialog" aria-labelledby="add-user-title" @click=${actions.dismissDialog}>
      <div class="dialog-head"><h2 id="add-user-title">Add user</h2></div>
      <form @submit=${actions.createIdentity} @input=${(event: Event) => {
        validatePasswordConfirmation(event);
        actions.changed();
      }}>
        <label>Username<input name="username" autocomplete="username" required autofocus /></label>
        <label>Password<input name="password" type="password" minlength="12" autocomplete="new-password" required /></label>
        <label>Confirm password<input name="password_confirmation" type="password" minlength="12" autocomplete="new-password" required /></label>
        <div class="dialog-actions"><button class="button secondary" type="button" @click=${actions.closeAddUser}>Cancel</button>${renderFormSubmit({ label: saving ? "Adding..." : "Add user", busy: saving, error })}</div>
      </form>
    </dialog>
    ${
      editingIdentity
        ? html`
          <dialog id="edit-user-dialog" aria-labelledby="edit-user-title" @click=${actions.dismissDialog}>
            <div class="dialog-head"><h2 id="edit-user-title">Edit user</h2></div>
            <form @submit=${(event: SubmitEvent) => actions.updateIdentity(editingIdentity, event)} @input=${(event: Event) => {
              validatePasswordConfirmation(event);
              actions.changed();
            }}>
              <label>Username<input name="username" .value=${editingIdentity.username} autocomplete="username" required autofocus /></label>
              <label>New password<input name="password" type="password" minlength="12" autocomplete="new-password" placeholder="Keep current password" /></label>
              <label>Confirm new password<input name="password_confirmation" type="password" minlength="12" autocomplete="new-password" placeholder="Keep current password" /></label>
              <div class="dialog-actions"><button class="button secondary" type="button" @click=${actions.closeEditUser}>Cancel</button>${renderFormSubmit({ label: "Save changes", busy: saving, error, trackChanges: true })}</div>
            </form>
          </dialog>`
        : nothing
    }`;
}

export function renderApiTokensPage(tokens: ApiToken[], newToken: string, saving: boolean, error: string, actions: AuthActions) {
  return html`
    <div class="admin-page">
      <div class="heading"><div><span class="eyebrow">Administration</span><h1>API tokens</h1></div><button class="button" type="button" @click=${actions.openApiToken}>New token</button></div>
      ${renderCard({
        title: "API tokens",
        metadata: `${tokens.length} active`,
        content: html`
          ${newToken ? html`<div class="notice token-value" role="status"><strong>Copy this token now.</strong><code>${newToken}</code><button class="button secondary" @click=${actions.dismissToken}>Dismiss</button></div>` : nothing}
          ${
            tokens.length
              ? tokens.map((token) => html`<div class="resource"><div><strong>${token.name}</strong><code>${token.expires_at_ms ? `Expires ${new Date(token.expires_at_ms).toLocaleString()}` : "Never expires"}</code></div><button class="button danger" @click=${() => actions.revokeApiToken(token)}>Revoke</button></div>`)
              : html`<upgrid-empty-state>No API tokens</upgrid-empty-state>`
          }
        `,
      })}
    </div>
    <dialog id="api-token-dialog" aria-labelledby="api-token-title" @click=${actions.dismissDialog}>
      <div class="dialog-head"><h2 id="api-token-title">New API token</h2></div>
      <form @submit=${actions.createApiToken} @input=${actions.changed}>
        <label>Name<input name="name" placeholder="Automation" required autofocus /></label>
        <label>Expires in days<input name="expires_in_days" type="number" min="1" max="365" placeholder="Never" /></label>
        <div class="dialog-actions"><button class="button secondary" type="button" @click=${actions.closeApiToken}>Cancel</button>${renderFormSubmit({ label: saving ? "Creating..." : "Create API token", busy: saving, error })}</div>
      </form>
    </dialog>`;
}

export function renderManagePage(settings: ManageSettings | undefined, saving: boolean, error: string, update: (event: SubmitEvent) => void, changed: () => void) {
  return html`
    <div class="admin-page">
      <div class="heading"><div><span class="eyebrow">Administration</span><h1>Manage</h1></div></div>
      ${renderCard({
        title: "Public status access",
        content: html`
          <form @submit=${update} @input=${changed}>
            <upgrid-toggle-switch name="public_status_enabled" .checked=${settings?.public_status_enabled ?? false} ?disabled=${settings === undefined || saving}>
              <span class="setting-copy">
                Allow status viewing without login
                <small>External visitors can see target names, states, and recent evaluation metrics. URLs, configuration, alerts, cluster data, and administration remain private.</small>
              </span>
            </upgrid-toggle-switch>
            <div class="dialog-actions">${renderFormSubmit({ label: saving ? "Saving..." : "Save changes", busy: saving, blocked: settings === undefined, error, baselineKey: String(settings?.public_status_enabled), blockedMessage: "Settings are unavailable", trackChanges: true })}</div>
          </form>
        `,
      })}
    </div>`;
}
