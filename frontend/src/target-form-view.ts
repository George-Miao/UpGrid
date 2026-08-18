import { html } from "lit";
import type { Channel, Secret } from "./api.ts";

interface Actions {
  backdrop: (event: MouseEvent) => void;
  close: () => void;
  create: (event: SubmitEvent) => void;
}

export function renderChannelFields(channels: Channel[], selected: string[] = [], useDefaults = true) {
  const updateDefaults = (event: Event) => {
    const toggle = event.currentTarget as HTMLInputElement;
    const fieldset = toggle.closest(".channel-fields");
    fieldset?.querySelectorAll<HTMLInputElement>('input[data-default="true"]').forEach((input) => {
      input.disabled = toggle.checked;
      input.checked = toggle.checked || input.dataset.explicit === "true";
    });
    toggle.form?.dispatchEvent(new Event("input", { bubbles: true }));
  };
  return html`
    <div class="channel-fields">
      <label class="switch">
        <span>Use default channels</span>
        <input
          class="switch-control"
          name="use_default_channels"
          type="checkbox"
          role="switch"
          .checked=${useDefaults}
          @change=${updateDefaults}
        />
      </label>
      <div class="channel-options">
        ${
          channels.length
            ? channels.map((channel) => {
                const explicit = selected.includes(channel.id);
                const inherited = useDefaults && channel.default;
                return html`
                  <label class="checkbox-option">
                    <span class="switch-label">${channel.name} <span class="badge">${channel.kind}</span></span>
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
            : html`<p class="meta">No notification channels are available.</p>`
        }
      </div>
    </div>`;
}

export function renderTlsSecretFields(secrets: Secret[], caSecretId: string | null = null, clientCertificateSecretId: string | null = null, clientPrivateKeySecretId: string | null = null) {
  const options = (selected: string | null) => html`
    <option value="">Not configured</option>
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

function targetTablist(form: HTMLFormElement) {
  return form.closest("dialog")?.querySelector<HTMLElement>(".form-tabs");
}

function activateTargetTab(form: HTMLFormElement, tabName: string) {
  targetTablist(form)
    ?.querySelectorAll<HTMLButtonElement>("[role='tab']")
    .forEach((tab) => {
      const selected = tab.dataset.tab === tabName;
      tab.setAttribute("aria-selected", String(selected));
      tab.tabIndex = selected ? 0 : -1;
    });
  form.querySelectorAll<HTMLElement>("[role='tabpanel']").forEach((panel) => {
    panel.hidden = panel.dataset.panel !== tabName;
  });
}

function applyTargetKind(form: HTMLFormElement, kind: string) {
  const endpoint = form.elements.namedItem("url") as HTMLInputElement | null;
  if (endpoint) {
    endpoint.placeholder = endpointPlaceholders[kind];
    endpoint.type = kind === "http" ? "url" : "text";
  }
  form.querySelectorAll<HTMLElement>("[data-http-only]").forEach((element) => {
    element.hidden = kind !== "http";
  });
  const tablist = targetTablist(form);
  const selectedTab = tablist?.querySelector<HTMLButtonElement>("[role='tab'][aria-selected='true']")?.dataset.tab ?? "general";
  const assertionsTab = tablist?.querySelector<HTMLButtonElement>("[data-tab='assertions']");
  if (assertionsTab) assertionsTab.disabled = kind !== "http";
  activateTargetTab(form, kind !== "http" && selectedTab === "assertions" ? "general" : selectedTab);
  const method = form.elements.namedItem("method") as HTMLInputElement | null;
  if (method) {
    method.disabled = kind !== "http";
    if (method.disabled) method.value = "GET";
  }
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

export function renderTargetForm(channels: Channel[], saving: boolean, actions: Actions) {
  return html`
    <dialog id="target-dialog" aria-labelledby="add-target-title" @click=${actions.backdrop}>
      <div class="dialog-head target-dialog-head">
        <h2 id="add-target-title">Add target</h2>
        <div class="form-tabs" role="tablist" aria-label="Target settings">
          <button id="target-general-tab" form="target-form" type="button" role="tab" data-tab="general" aria-controls="target-general-panel" aria-selected="true" @click=${selectTargetTab}>General</button>
          <button id="target-assertions-tab" form="target-form" type="button" role="tab" data-tab="assertions" aria-controls="target-assertions-panel" aria-selected="false" tabindex="-1" @click=${selectTargetTab}>Assertions</button>
          <button id="target-evaluation-tab" form="target-form" type="button" role="tab" data-tab="evaluation" aria-controls="target-evaluation-panel" aria-selected="false" tabindex="-1" @click=${selectTargetTab}>Evaluation</button>
          <button id="target-notifications-tab" form="target-form" type="button" role="tab" data-tab="notifications" aria-controls="target-notifications-panel" aria-selected="false" tabindex="-1" @click=${selectTargetTab}>Notifications</button>
        </div>
      </div>
      <form id="target-form" @submit=${actions.create} @reset=${resetTargetKind}>
        <section id="target-general-panel" class="target-tab-panel" role="tabpanel" data-panel="general" aria-labelledby="target-general-tab">
          <label>Name<input name="name" placeholder="Production API" required autofocus /></label>
          <div class="row endpoint-row">
            <label>Type<select name="kind" @change=${selectTargetKind}><option value="http">HTTP</option><option value="tcp">TCP connect</option><option value="dns">DNS resolution</option><option value="icmp">ICMP echo</option><option value="tls">TLS certificate</option></select></label>
            <label>URL / endpoint<input name="url" type="url" placeholder=${endpointPlaceholders.http} required /></label>
          </div>
          <label data-http-only>Method<input name="method" value="GET" required /></label>
        </section>
        <section id="target-assertions-panel" class="target-tab-panel" role="tabpanel" data-panel="assertions" data-http-only aria-labelledby="target-assertions-tab" hidden>
          <http-assertion-editor name="assertions" target-id="new"></http-assertion-editor>
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
        <div class="dialog-actions">
          <button class="button secondary" type="button" @click=${actions.close}>Cancel</button>
          <button class="button" type="submit" ?disabled=${saving}>${saving ? "Creating…" : "Create target"}</button>
        </div>
      </form>
    </dialog>`;
}
