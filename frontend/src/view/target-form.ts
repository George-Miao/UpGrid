import { html, nothing } from "lit";
import type { Channel, Secret, Target } from "@/app/api.ts";
import "@/component/empty-state.ts";
import "@/component/channel-type-icon.ts";
import { renderFormSubmit } from "@/component/form-submit.ts";
import "@/component/switch.ts";
import type { ToggleSwitch } from "@/component/switch.ts";

interface Actions {
  backdrop: (event: MouseEvent) => void;
  close: () => void;
  create: (event: SubmitEvent) => void;
  changed: () => void;
}

export function renderChannelFields(channels: Channel[], selected: string[] = [], useDefaults = true) {
  const updateDefaults = (event: Event) => {
    const toggle = event.currentTarget as ToggleSwitch;
    const fieldset = toggle.closest(".channel-fields");
    fieldset?.querySelectorAll<HTMLInputElement>('input[data-default="true"]').forEach((input) => {
      input.disabled = toggle.checked;
      input.checked = toggle.checked || input.dataset.explicit === "true";
    });
    toggle.form?.dispatchEvent(new Event("input", { bubbles: true }));
  };
  return html`
    <div class="channel-fields">
      <upgrid-toggle-switch compact name="use_default_channels" .checked=${useDefaults} @change=${updateDefaults}>Use default channels</upgrid-toggle-switch>
      <div class="channel-options">
        ${
          channels.length
            ? channels.map((channel) => {
                const explicit = selected.includes(channel.id);
                const inherited = useDefaults && channel.default;
                return html`
                  <label class="checkbox-option">
                    <span class="switch-label">${channel.name} <upgrid-channel-type-icon .kind=${channel.kind}></upgrid-channel-type-icon></span>
                    <input
                      class="checkbox-control"
                      name="channel_id"
                      type="checkbox"
                      value=${channel.id}
                      data-default=${String(channel.default)}
                      data-explicit=${String(explicit)}
                      .checked=${explicit || inherited}
                      ?disabled=${inherited}
                      @change=${(event: Event) => {
                        const input = event.currentTarget as HTMLInputElement;
                        input.dataset.explicit = String(input.checked);
                      }}
                    />
                  </label>
                `;
              })
            : html`<upgrid-empty-state>No notification channels are available</upgrid-empty-state>`
        }
      </div>
    </div>`;
}

function renderTlsSecretFields(secrets: Secret[], caSecretId: string | null = null, clientCertificateSecretId: string | null = null, clientPrivateKeySecretId: string | null = null) {
  const options = (selected: string | null) => html`
    <option value="">No secret configured</option>
    ${secrets.map((secret) => html`<option value=${secret.id} ?selected=${secret.id === selected}>${secret.name}</option>`)}
  `;
  return html`
    <fieldset class="tls-fields">
      <legend>HTTPS trust and mutual TLS</legend>
      <label>Custom CA bundle secret<select name="tls_ca_secret_id">${options(caSecretId)}</select></label>
      <div class="row">
        <label>Client certificate secret<select name="tls_client_certificate_secret_id">${options(clientCertificateSecretId)}</select></label>
        <label>Client private key secret<select name="tls_client_private_key_secret_id">${options(clientPrivateKeySecretId)}</select></label>
      </div>
      <p class="meta">PEM values stay encrypted. Client certificate and private key must be configured together.</p>
    </fieldset>
  `;
}

const endpointPlaceholders: Record<string, string> = {
  http: "https://example.com/health",
  tcp: "database.internal:5432",
  dns: "service.internal",
  icmp: "192.0.2.10",
  tls: "example.com:443",
};

interface TargetGeneralFieldsOptions {
  secrets: Secret[];
  target?: Target;
  kindChanged?: (event: Event) => void;
}

function formatAcceptedStatuses(target?: Target): string {
  return (target?.accepted_statuses ?? [{ start: 200, end: 299 }]).map((range) => (range.start === range.end ? range.start : `${range.start}-${range.end}`)).join(",");
}

function toggleMaxRedirects(event: Event): void {
  const follow = event.currentTarget as ToggleSwitch;
  const maximum = follow.form?.elements.namedItem("max_redirects") as HTMLInputElement | null;
  if (maximum) maximum.disabled = !follow.checked;
  follow.form?.dispatchEvent(new Event("input", { bubbles: true }));
}

export function renderTargetGeneralFields({ secrets, target, kindChanged }: TargetGeneralFieldsOptions) {
  const kind = target?.kind ?? "http";
  if (target?.kind === "node") {
    return html`
      <label>Name<input name="name" .value=${target.name} required /></label>
      <label>RPC URL<input .value=${target.url} disabled /></label>
    `;
  }
  const isHttp = kind === "http";
  const followRedirects = target?.follow_redirects ?? true;
  return html`
    <label>Name<input name="name" placeholder="Production API" .value=${target?.name ?? ""} required ?autofocus=${!target} /></label>
    <div class="row endpoint-row">
      <label>Type
        ${
          target
            ? html`<input .value=${kind.toUpperCase()} disabled />`
            : html`<select name="kind" @change=${kindChanged}><option value="http">HTTP</option><option value="tcp">TCP connect</option><option value="dns">DNS resolution</option><option value="icmp">ICMP echo</option><option value="tls">TLS certificate</option></select>`
        }
      </label>
      <label>URL / endpoint<input name="url" type=${isHttp ? "url" : "text"} placeholder=${endpointPlaceholders[kind]} .value=${target?.url ?? ""} required /></label>
    </div>
    ${
      !target || isHttp
        ? html`
          <div class="http-fields" data-http-only ?hidden=${!isHttp}>
            <div class="http-settings">
              <label>Method<input name="method" .value=${target?.method ?? "GET"} .defaultValue=${target?.method ?? "GET"} required /></label>
              <upgrid-toggle-switch compact name="follow_redirects" .checked=${followRedirects} @change=${toggleMaxRedirects}>Follow redirects</upgrid-toggle-switch>
              <label class="redirect-limit">Maximum redirects<input name="max_redirects" type="number" min="0" .value=${String(target?.max_redirects ?? 5)} .defaultValue=${String(target?.max_redirects ?? 5)} ?disabled=${!followRedirects} required /></label>
            </div>
            <upgrid-toggle-switch compact name="skip_tls_verification" .checked=${target?.skip_tls_verification ?? false}>Skip TLS verification</upgrid-toggle-switch>
            ${renderTlsSecretFields(secrets, target?.tls_ca_secret_id, target?.tls_client_certificate_secret_id, target?.tls_client_private_key_secret_id)}
          </div>
        `
        : nothing
    }
  `;
}
export function renderTargetAssertions(target?: Target) {
  const statuses = formatAcceptedStatuses(target);
  return html`
    <upgrid-http-assertion-editor name="assertions" target-id=${target?.id ?? "new"} .assertions=${target?.assertions ?? []}>
      <div slot="required" class="required-assertion">
        <label>Type<select aria-label="Status assertion type" disabled><option>Status code</option></select></label>
        <label>Expected status<input name="statuses" .value=${statuses} .defaultValue=${statuses} required /></label>
      </div>
    </upgrid-http-assertion-editor>
  `;
}

function targetTablist(form: HTMLFormElement) {
  return form.closest("dialog")?.querySelector<HTMLElement>(".form-tabs");
}

function activateTargetTab(form: HTMLFormElement, tabName: string) {
  targetTablist(form)
    ?.querySelectorAll<HTMLButtonElement>("[role='tab']")
    .forEach((tab) => {
      const selected = tab.dataset.tab === tabName;
      tab.setAttribute("aria-selected", String(selected));
      tab.tabIndex = -1;
    });
  form.querySelectorAll<HTMLElement>("[role='tabpanel']").forEach((panel) => {
    panel.hidden = panel.dataset.panel !== tabName;
  });
}

type ValidatableTargetControl = HTMLElement & {
  checkValidity: () => boolean;
  reportValidity: () => boolean;
};

function firstInvalidTargetControl(form: HTMLFormElement): ValidatableTargetControl | undefined {
  for (let index = 0; index < form.elements.length; index += 1) {
    const element = form.elements.item(index);
    if (element instanceof HTMLElement && "checkValidity" in element && typeof element.checkValidity === "function" && !element.checkValidity()) {
      return element as ValidatableTargetControl;
    }
  }
  return undefined;
}

export function submitTargetForm(event: SubmitEvent, submit: (event: SubmitEvent) => void) {
  event.preventDefault();
  const form = event.currentTarget as HTMLFormElement;
  const invalid = firstInvalidTargetControl(form);
  if (!invalid) {
    submit(event);
    return;
  }
  const panel = invalid.closest<HTMLElement>("[role='tabpanel']");
  if (panel) {
    form.closest("dialog")?.querySelector<HTMLButtonElement>(`[role='tab'][aria-controls='${panel.id}']`)?.click();
  }
  queueMicrotask(() => invalid.reportValidity());
}

function applyTargetKind(form: HTMLFormElement, kind: string) {
  const endpoint = form.elements.namedItem("url") as HTMLInputElement | null;
  if (endpoint) {
    endpoint.placeholder = endpointPlaceholders[kind];
    endpoint.type = kind === "http" ? "url" : "text";
  }
  const httpOnly = kind !== "http";
  form.querySelectorAll<HTMLElement>("[data-http-only]").forEach((element) => {
    element.hidden = httpOnly;
    element.querySelectorAll<HTMLElement>("input, select, textarea, upgrid-http-assertion-editor, upgrid-toggle-switch").forEach((control) => {
      control.toggleAttribute("disabled", httpOnly);
    });
  });
  const follow = form.elements.namedItem("follow_redirects") as ToggleSwitch | null;
  const maximum = form.elements.namedItem("max_redirects") as HTMLInputElement | null;
  if (maximum) maximum.disabled = httpOnly || !follow?.checked;
  const tablist = targetTablist(form);
  const selectedTab = tablist?.querySelector<HTMLButtonElement>("[role='tab'][aria-selected='true']")?.dataset.tab ?? "general";
  const assertionsTab = tablist?.querySelector<HTMLButtonElement>("[data-tab='assertions']");
  if (assertionsTab) assertionsTab.disabled = httpOnly;
  activateTargetTab(form, httpOnly && selectedTab === "assertions" ? "general" : selectedTab);
}

function selectTargetKind(event: Event) {
  const select = event.currentTarget as HTMLSelectElement;
  if (select.form) applyTargetKind(select.form, select.value);
}

function selectTargetTab(event: Event) {
  const tab = event.currentTarget as HTMLButtonElement;
  if (tab.form && tab.dataset.tab) activateTargetTab(tab.form, tab.dataset.tab);
}

function resetTargetKind(event: Event) {
  const form = event.currentTarget as HTMLFormElement;
  queueMicrotask(() => {
    applyTargetKind(form, "http");
    activateTargetTab(form, "general");
  });
}

export function renderTargetForm(channels: Channel[], secrets: Secret[], saving: boolean, error: string, actions: Actions) {
  return html`
    <dialog id="target-dialog" aria-labelledby="add-target-title" @click=${actions.backdrop}>
      <div class="dialog-head target-dialog-head">
        <h2 id="add-target-title">Add target</h2>
        <div class="form-tabs" role="tablist" aria-label="Target settings">
          <button id="target-general-tab" form="target-form" type="button" role="tab" data-tab="general" aria-controls="target-general-panel" aria-selected="true" tabindex="-1" @click=${selectTargetTab}>General</button>
          <button id="target-assertions-tab" form="target-form" type="button" role="tab" data-tab="assertions" aria-controls="target-assertions-panel" aria-selected="false" tabindex="-1" @click=${selectTargetTab}>Assertions</button>
          <button id="target-evaluation-tab" form="target-form" type="button" role="tab" data-tab="evaluation" aria-controls="target-evaluation-panel" aria-selected="false" tabindex="-1" @click=${selectTargetTab}>Evaluation</button>
          <button id="target-notifications-tab" form="target-form" type="button" role="tab" data-tab="notifications" aria-controls="target-notifications-panel" aria-selected="false" tabindex="-1" @click=${selectTargetTab}>Notifications</button>
        </div>
      </div>
      <form id="target-form" novalidate @submit=${(event: SubmitEvent) => submitTargetForm(event, actions.create)} @input=${actions.changed} @reset=${resetTargetKind}>
        <section id="target-general-panel" class="target-tab-panel" role="tabpanel" data-panel="general" aria-labelledby="target-general-tab">
          ${renderTargetGeneralFields({ secrets, kindChanged: selectTargetKind })}
        </section>
        <section id="target-assertions-panel" class="target-tab-panel" role="tabpanel" data-panel="assertions" data-http-only aria-labelledby="target-assertions-tab" hidden>
          ${renderTargetAssertions()}
        </section>
        <section id="target-evaluation-panel" class="target-tab-panel" role="tabpanel" data-panel="evaluation" aria-labelledby="target-evaluation-tab" hidden>
          <div class="row">
            <label>Interval (seconds)<input name="interval" type="number" min="1" value="60" required /></label>
            <label>Timeout (seconds)<input name="timeout" type="number" min="1" value="10" required /></label>
          </div>
          <div class="row">
            <label>Failures before down<input name="failures" type="number" min="1" value="3" required /></label>
            <label>Evaluation locations<input name="locations" type="number" min="1" max="32" value="1" required /></label>
          </div>
        </section>
        <section id="target-notifications-panel" class="target-tab-panel" role="tabpanel" data-panel="notifications" aria-labelledby="target-notifications-tab" hidden>
          ${renderChannelFields(channels)}
        </section>
        ${error ? html`<div class="notice" role="alert">${error}</div>` : nothing}
        <div class="dialog-actions">
          <button class="button secondary" type="button" @click=${actions.close}>Cancel</button>
          ${renderFormSubmit({ label: saving ? "Creating..." : "Create target", busy: saving, error })}
        </div>
      </form>
    </dialog>`;
}
