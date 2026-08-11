import { html } from "lit";
import type { Channel } from "./api.ts";
import { renderHelpTooltip } from "./help-tooltip.ts";

interface Actions {
  backdrop: (event: MouseEvent) => void;
  close: () => void;
  create: (event: SubmitEvent) => void;
}

export function renderChannelFields(channels: Channel[], selected: string[] = [], useDefaults = true) {
  const updateDefaults = (event: Event) => {
    const toggle = event.currentTarget as HTMLInputElement;
    const fieldset = toggle.closest("fieldset");
    fieldset?.querySelectorAll<HTMLInputElement>('input[data-default="true"]').forEach((input) => {
      input.disabled = toggle.checked;
      input.checked = toggle.checked || input.dataset.explicit === "true";
    });
    toggle.form?.dispatchEvent(new Event("input", { bubbles: true }));
  };
  return html`
    <fieldset class="channel-fields">
      <legend>Notification channels</legend>
      <label class="switch">
        <span>Use default channels</span>
        <input
          name="use_default_channels"
          type="checkbox"
          role="switch"
          .checked=${useDefaults}
          @change=${updateDefaults}
        />
      </label>
      <div class="channel-options">
        ${channels.map((channel) => {
          const explicit = selected.includes(channel.id);
          const inherited = useDefaults && channel.default;
          return html`
            <label class="check">
              <input
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
              ${channel.name} <span class="badge">${channel.kind}</span>
            </label>
          `;
        })}
      </div>
    </fieldset>`;
}

const endpointPlaceholders: Record<string, string> = {
  http: "https://example.com/health",
  tcp: "database.internal:5432",
  dns: "service.internal",
  icmp: "192.0.2.10",
  tls: "example.com:443",
};

function applyTargetKind(form: HTMLFormElement, kind: string) {
  const endpoint = form.elements.namedItem("url") as HTMLInputElement | null;
  if (endpoint) {
    endpoint.placeholder = endpointPlaceholders[kind];
    endpoint.type = kind === "http" ? "url" : "text";
  }
  const httpOptions = form.querySelector<HTMLElement>("[data-http-options]");
  if (httpOptions) httpOptions.hidden = kind !== "http";
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

function resetTargetKind(event: Event) {
  const form = event.currentTarget as HTMLFormElement;
  queueMicrotask(() => applyTargetKind(form, "http"));
}

export function renderTargetForm(channels: Channel[], saving: boolean, actions: Actions) {
  return html`
    <dialog id="target-dialog" aria-labelledby="add-target-title" @click=${actions.backdrop}>
      <div class="dialog-head"><div class="title-with-help"><h2 id="add-target-title">Add target</h2>${renderHelpTooltip("target-secret-help", "About Target Secrets", "Advanced Target headers and request bodies can reference reusable Secrets through the HTTP API.")}</div><p>Start monitoring a service.</p></div>
      <form @submit=${actions.create} @reset=${resetTargetKind}>
        <label>Name<input name="name" placeholder="Production API" required autofocus /></label>
        <div class="row">
          <label>Type<select name="kind" @change=${selectTargetKind}><option value="http">HTTP</option><option value="tcp">TCP connect</option><option value="dns">DNS resolution</option><option value="icmp">ICMP echo</option><option value="tls">TLS certificate</option></select></label>
          <label>URL / endpoint<input name="url" type="url" placeholder=${endpointPlaceholders.http} required /></label>
        </div>
        <div data-http-options><label>Method<input name="method" value="GET" required /></label><http-assertion-editor name="assertions" target-id="new"></http-assertion-editor></div>
        <div class="row">
          <label>Interval (seconds)<input name="interval" type="number" min="1" value="60" required /></label>
          <label>Timeout (seconds)<input name="timeout" type="number" min="1" value="10" required /></label>
        </div>
        <label>Failures before Down<input name="failures" type="number" min="1" value="3" required /></label>
        ${renderChannelFields(channels)}
        <div class="dialog-actions">
          <button class="button secondary" type="button" @click=${actions.close}>Cancel</button>
          <button class="button" type="submit" ?disabled=${saving}>${saving ? "Creating…" : "Create target"}</button>
        </div>
      </form>
    </dialog>`;
}
