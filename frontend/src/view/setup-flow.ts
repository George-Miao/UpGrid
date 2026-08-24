import { LitElement, css, html, nothing } from "lit";
import { customElement, property, state } from "lit/decorators.js";
import { ApiRequestError, type Channel, type Setup, request } from "@/app/api.ts";
import "@/component/channel-form.ts";
import "@/component/empty-state.ts";
import { cardStyles, renderCard } from "@/component/card.ts";
import { renderFormSubmit } from "@/component/form-submit.ts";
import "@/component/switch.ts";
import { targetInput } from "@/util/resource-input.ts";
import { updatePasswordConfirmationValidity } from "@/util/form-validation.ts";

@customElement("upgrid-setup")
export class SetupFlow extends LitElement {
  @property({ attribute: false }) setup!: Setup;
  @state() private channels: Channel[] = [];
  @state() private saving = false;
  @state() private joining = false;
  @state() private error = "";
  @state() private additionalAddresses = [""];
  @state() private additionalDiscoveryUrls = [""];

  static styles = css`
    :host { display: block; }
    *, *::before, *::after { box-sizing: border-box; }
    .flow { width: min(760px, 100%); margin: 0 auto; }
    ${cardStyles}
    .eyebrow { color: var(--muted); font-size: 12px; letter-spacing: .16em; text-transform: uppercase; }
    h1 { margin: 5px 0 8px; font-size: clamp(30px, 5vw, 46px); letter-spacing: -.04em; }
    .visually-hidden { position: absolute; width: 1px; height: 1px; overflow: hidden; clip: rect(0 0 0 0); clip-path: inset(50%); white-space: nowrap; }
    .lead { margin: 0 0 16px; color: var(--muted); font-size: 15px; }
    .choice { display: grid; gap: 14px; padding: 22px; border-top: 1px solid var(--line); }
    .choice:first-child { border-top: 0; }
    .choice h2 { margin: 0; font-size: 17px; }
    .choice p { margin: -8px 0 0; color: var(--muted); }
    .cluster-identity, .cluster-create, .cluster-join { padding: 18px; }
    .cluster-identity { display: grid; grid-template-columns: minmax(0, 1fr) auto; align-items: end; gap: 10px; border-bottom: 1px solid var(--line); }
    .cluster-network { width: 100%; min-width: 0; align-self: end; }
    .cluster-network[open] { grid-column: 1 / -1; }
    .cluster-network summary { display: flex; min-height: 44px; align-items: center; gap: 10px; border: 1px solid var(--line); border-radius: 9px; padding: 9px 12px; color: var(--muted); cursor: pointer; user-select: none; list-style: none; }
    .cluster-network summary::-webkit-details-marker { display: none; }
    .cluster-network summary::after { width: 8px; height: 8px; margin: -4px 3px 0 auto; border-right: 2px solid currentColor; border-bottom: 2px solid currentColor; content: ""; transform: rotate(45deg); }
    .cluster-network[open] summary { border-radius: 9px 9px 0 0; color: var(--text); }
    .cluster-network[open] summary::after { margin-top: 4px; transform: rotate(225deg); }
    .cluster-network summary:hover { border-color: var(--button-hover-border); }
    .cluster-network-body { display: grid; gap: 12px; border: 1px solid var(--line); border-top: 0; border-radius: 0 0 9px 9px; padding: 12px; }
    .cluster-network-fields { display: grid; grid-template-columns: minmax(0, 1fr) 110px; gap: 10px; }
    .network-sources { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; }
    .address-list { display: grid; gap: 6px; }
    .up-address { display: flex; min-width: 0; }
    .up-prefix { display: flex; align-items: center; border: 1px solid var(--line); border-right: 0; border-radius: 9px 0 0 9px; background: var(--nav-bg); padding: 9px 10px; color: var(--muted); font-family: ui-monospace, monospace; }
    .up-address input:not([type="checkbox"]) { min-width: 0; border-radius: 0 9px 9px 0; }
    input:disabled { cursor: not-allowed; }
    .pending-address { margin: 0; color: var(--muted); font-size: 12px; }
    .address-actions { display: flex; justify-content: space-between; gap: 8px; }
    .address-actions button { min-height: 32px; padding: 4px 9px; font-size: 12px; }
    .cluster-create { display: grid; gap: 14px; }
    .cluster-create-fields { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 10px; }
    .cluster-copy h2 { margin: 0; font-size: 17px; }
    .cluster-copy p { margin: 2px 0 0; color: var(--muted); }
    .cluster-divider { display: flex; align-items: center; gap: 12px; color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: .12em; }
    .cluster-divider::before, .cluster-divider::after { height: 1px; flex: 1; background: var(--line); content: ""; }
    .cluster-join { display: grid; gap: 10px; }
    .cluster-join-fields { display: grid; grid-template-columns: minmax(0, 1fr) auto; align-items: end; gap: 10px; }
    .cluster-join-fields label { min-width: 0; }
    .cluster-join-fields button { height: 44px; white-space: nowrap; }
    form { display: grid; gap: 13px; }
    label { display: grid; gap: 6px; color: var(--muted); font-size: 14px; }
    fieldset { display: grid; gap: 8px; min-width: 0; margin: 0; border: 0; padding: 0; }
    legend { margin-bottom: 4px; padding: 0; color: var(--text); font-size: 14px; }
    input:not([type="checkbox"]), select { width: 100%; min-height: 44px; border: 1px solid var(--line); border-radius: 9px; outline: 0; background: var(--input-bg); color: var(--text); padding: 9px 10px; font: inherit; font-size: 16px; transition: border-color 160ms ease, opacity 160ms ease; }
    input:not([type="checkbox"]):focus, select:focus { border-color: var(--focus); }
    button:focus-visible, input:focus-visible, select:focus-visible, summary:focus-visible { outline: 2px solid var(--green); outline-offset: 2px; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 11px; }
    .actions { display: flex; justify-content: flex-end; gap: 9px; margin-top: 5px; }
    button { display: inline-flex; min-height: 44px; align-items: center; justify-content: center; border: 1px solid var(--button-border); border-radius: 9px; background: var(--button-bg); color: var(--button-text); padding: 9px 13px; cursor: pointer; user-select: none; font: inherit; transition: background-color 160ms ease, border-color 160ms ease, opacity 160ms ease, transform 120ms ease; }
    button:hover { border-color: var(--button-hover-border); }
    button:active { transform: translateY(1px); }
    button:disabled { border-color: var(--disabled-border); background: var(--disabled-bg); color: var(--disabled-text); cursor: not-allowed; opacity: 1; }
    .secondary { background: transparent; color: var(--muted); border-color: var(--line); }
    .notice { margin-bottom: 16px; border: 1px solid var(--notice-border); border-radius: 10px; background: var(--notice-bg); color: var(--notice-text); padding: 10px 12px; }
    .joining-status { margin: 0; color: var(--muted); font-size: 13px; }
    .count { display: inline-block; margin-top: 6px; color: var(--green); font-size: 12px; }
    @media (max-width: 620px) { .row, .cluster-identity, .cluster-network-fields, .network-sources, .cluster-create-fields, .cluster-join-fields { grid-template-columns: 1fr; } .cluster-network[open] { grid-column: auto; } .cluster-create upgrid-form-submit, .cluster-join upgrid-form-submit { justify-self: end; } }
    @media (max-height: 650px) and (min-width: 621px) {
      h1 { margin: 2px 0 4px; font-size: 30px; }
      .lead { margin-bottom: 8px; font-size: 13px; }
      .cluster-identity, .cluster-create, .cluster-join { padding: 8px 14px; }
      .cluster-create { grid-template-columns: minmax(0, 1fr) auto; gap: 8px; }
      .cluster-create .cluster-copy { grid-column: 1 / -1; }
      .cluster-create button { align-self: end; }
      .cluster-copy p { display: none; }
      .cluster-join { grid-template-columns: auto minmax(0, 1fr); align-items: end; }
      input:not([type="checkbox"]), button { min-height: 38px; }
      .cluster-network summary { min-height: 38px; }
      .cluster-join-fields button { height: 44px; }
    }
  `;

  connectedCallback(): void {
    super.connectedCallback();
    void this.loadChannels();
  }

  protected updated(changed: Map<PropertyKey, unknown>): void {
    if (!changed.has("setup")) return;
    void this.loadChannels();
  }

  private async loadChannels(): Promise<void> {
    if (!this.setup?.cluster_ready || this.setup.phase !== "target") return;
    try {
      this.channels = await request<Channel[]>("/api/v1/channels");
    } catch (error) {
      this.fail(error);
    }
  }

  private submittedNodeName(): string {
    return this.shadowRoot?.querySelector<HTMLInputElement>("#setup-node-name")?.value.trim() ?? "";
  }

  private submittedReachableAddresses(): string[] {
    return this.additionalAddresses
      .map((address) => address.trim())
      .filter(Boolean)
      .map((address) => `up://${address.replace(/^up:\/\//, "")}`);
  }

  private submittedDiscoveryUrls(): string[] {
    return this.additionalDiscoveryUrls.map((url) => url.trim()).filter(Boolean);
  }

  private updateReachableAddress(index: number, event: Event): void {
    const addresses = [...this.additionalAddresses];
    addresses[index] = (event.currentTarget as HTMLInputElement).value;
    this.additionalAddresses = addresses;
  }

  private addReachableAddress(): void {
    this.additionalAddresses = [...this.additionalAddresses, ""];
  }

  private removeReachableAddress(index: number): void {
    this.additionalAddresses = this.additionalAddresses.filter((_, current) => current !== index);
    if (this.additionalAddresses.length === 0) this.additionalAddresses = [""];
  }

  private updateDiscoveryUrl(index: number, event: Event): void {
    const urls = [...this.additionalDiscoveryUrls];
    urls[index] = (event.currentTarget as HTMLInputElement).value;
    this.additionalDiscoveryUrls = urls;
  }

  private addDiscoveryUrl(): void {
    this.additionalDiscoveryUrls = [...this.additionalDiscoveryUrls, ""];
  }

  private removeDiscoveryUrl(index: number): void {
    this.additionalDiscoveryUrls = this.additionalDiscoveryUrls.filter((_, current) => current !== index);
    if (this.additionalDiscoveryUrls.length === 0) this.additionalDiscoveryUrls = [""];
  }

  private async createCluster(event: SubmitEvent): Promise<void> {
    event.preventDefault();
    const form = event.currentTarget as HTMLFormElement;
    updatePasswordConfirmationValidity(form);
    if (!form.reportValidity()) return;
    if (!window.confirm("Create a new single-node cluster?")) return;
    const fields = new FormData(form);
    const adminUsername = String(fields.get("admin_username") ?? "").trim();
    const adminPassword = String(fields.get("password") ?? "");
    await this.choose(
      "/api/v1/setup/new-cluster",
      {
        node_name: this.submittedNodeName(),
        admin_username: adminUsername,
        admin_password: adminPassword,
        reachable_addresses: this.submittedReachableAddresses(),
        discovery_urls: this.submittedDiscoveryUrls(),
      },
      { username: adminUsername, password: adminPassword },
    );
  }

  private async joinCluster(event: SubmitEvent): Promise<void> {
    event.preventDefault();
    const form = event.currentTarget as HTMLFormElement;
    const fields = new FormData(form);
    this.joining = true;
    await this.choose("/api/v1/cluster/join", {
      node_name: this.submittedNodeName(),
      join_link: String(fields.get("join_link") ?? "").trim(),
      reachable_addresses: this.submittedReachableAddresses(),
      discovery_urls: this.submittedDiscoveryUrls(),
    });
  }

  private async choose(path: string, body: object, login?: { username: string; password: string }): Promise<void> {
    this.saving = true;
    this.error = "";
    try {
      await request(path, { method: "POST", body: JSON.stringify(body) });
      await this.waitForCluster(login);
    } catch (error) {
      this.fail(error);
      this.saving = false;
      this.joining = false;
    }
  }

  private async waitForCluster(login?: { username: string; password: string }): Promise<void> {
    for (let attempt = 0; attempt < 120; attempt += 1) {
      const { promise, resolve } = Promise.withResolvers<void>();
      window.setTimeout(resolve, 250);
      await promise;
      try {
        if (login) {
          await request("/api/v1/auth/login", {
            method: "POST",
            body: JSON.stringify(login),
          });
        }
        const setup = await request<Setup>("/api/v1/setup");
        if (setup.cluster_ready) {
          this.changed(setup);
          return;
        }
      } catch (error) {
        if (!login && error instanceof ApiRequestError && error.status === 401) {
          window.location.assign("/");
          return;
        }
        // The setup listener is replaced by the configured Cluster API.
      }
    }
    throw new Error("Cluster setup did not finish within 30 seconds");
  }

  private async createTarget(event: SubmitEvent): Promise<void> {
    event.preventDefault();
    const fields = new FormData(event.currentTarget as HTMLFormElement);
    const input = targetInput(fields, fields.getAll("channel_id").map(String));
    await this.createResource("/api/v1/targets", input);
  }

  private async createResource(path: string, body: object): Promise<void> {
    this.saving = true;
    try {
      await request(path, { method: "POST", body: JSON.stringify(body) });
      await this.next();
    } catch (error) {
      this.fail(error);
      this.saving = false;
    }
  }

  private async next(): Promise<void> {
    this.saving = true;
    try {
      this.changed(await request<Setup>("/api/v1/setup/next", { method: "POST" }));
    } catch (error) {
      this.fail(error);
      this.saving = false;
    }
  }

  private changed(setup: Setup): void {
    this.saving = false;
    this.joining = false;
    this.dispatchEvent(new CustomEvent("setup-changed", { detail: setup, bubbles: true, composed: true }));
  }

  private fail(error: unknown): void {
    this.error = error instanceof Error ? error.message : String(error);
  }

  protected render() {
    return html`<section class="flow" aria-label="UpGrid setup" @input=${() => (this.error = "")}>
      ${this.error ? html`<div class="notice" role="alert">${this.error}</div>` : nothing}
      ${this.setup.phase === "cluster" ? this.renderCluster() : this.setup.phase === "channel" ? this.renderChannel() : this.renderTarget()}
    </section>`;
  }

  private renderCluster() {
    const localHosts = this.setup.local_addresses.map(({ host }) => host);
    const initialPort = this.setup.local_addresses[0]?.port;

    return html`
      <span class="eyebrow">First-run setup</span><h1>Choose your cluster</h1>
      <p class="lead">Review this node’s name, then create a new cluster or use a join token.</p>
      ${renderCard({
        content: html`
          <div class="cluster-identity">
            <label for="setup-node-name">Node name<input id="setup-node-name" .value=${this.setup.node_name} ?disabled=${this.saving} required /></label>
            <details class="cluster-network">
              <summary>Network settings</summary>
              <div class="cluster-network-body">
                <div class="cluster-network-fields">
                  <label>Local IP addresses<input .value=${localHosts.join(", ")} disabled /></label>
                  <label>Raft port<input .value=${String(initialPort ?? "")} disabled /></label>
                </div>
                <div class="network-sources">
                  <fieldset>
                    <legend>Reachable addresses</legend>
                    <div class="address-list">
                      ${this.setup.reachable_addresses.map(
                        (address, index) => html`
                          <label>
                            <span class="visually-hidden">Configured reachable address ${index + 1}</span>
                            <span class="up-address"><span class="up-prefix">up://</span><input .value=${address.replace(/^up:\/\//, "")} disabled /></span>
                          </label>
                        `,
                      )}
                      ${this.additionalAddresses.map(
                        (address, index) => html`
                          <label>
                            <span class="visually-hidden">Additional reachable address ${index + 1}</span>
                            <span class="up-address">
                              <span class="up-prefix">up://</span>
                              <input
                                .value=${address}
                                placeholder="node.example:11451"
                                autocomplete="off"
                                @input=${(event: Event) => this.updateReachableAddress(index, event)}
                                ?disabled=${this.saving}
                              />
                            </span>
                          </label>
                          <div class="address-actions">
                            <button class="secondary" type="button" ?disabled=${this.saving} @click=${() => this.removeReachableAddress(index)}>Remove address</button>
                            ${index === this.additionalAddresses.length - 1 ? html`<button class="secondary" type="button" ?disabled=${this.saving} @click=${this.addReachableAddress}>Add address</button>` : nothing}
                          </div>
                        `,
                      )}
                      ${this.setup.reachable_addresses.length === 0 ? html`<p class="pending-address">No address is configured. You can leave this empty while discovery is pending.</p>` : nothing}
                    </div>
                  </fieldset>
                  <fieldset>
                    <legend>Discovery services</legend>
                    <div class="address-list">
                      ${this.setup.discovery_urls.map(
                        (url, index) => html`
                          <label>
                            <span class="visually-hidden">Configured discovery service URL ${index + 1}</span>
                            <input type="url" .value=${url} disabled />
                          </label>
                        `,
                      )}
                      ${this.additionalDiscoveryUrls.map(
                        (url, index) => html`
                          <label>
                            <span class="visually-hidden">Additional discovery service URL ${index + 1}</span>
                            <input
                              type="url"
                              .value=${url}
                              placeholder="https://discovery.example/nodes"
                              autocomplete="off"
                              @input=${(event: Event) => this.updateDiscoveryUrl(index, event)}
                              ?disabled=${this.saving}
                            />
                          </label>
                          <div class="address-actions">
                            <button class="secondary" type="button" ?disabled=${this.saving} @click=${() => this.removeDiscoveryUrl(index)}>Remove service</button>
                            ${index === this.additionalDiscoveryUrls.length - 1 ? html`<button class="secondary" type="button" ?disabled=${this.saving} @click=${this.addDiscoveryUrl}>Add service</button>` : nothing}
                          </div>
                        `,
                      )}
                    </div>
                  </fieldset>
                </div>
              </div>
            </details>
          </div>
          <form class="cluster-create" @submit=${this.createCluster} @input=${(event: Event) => updatePasswordConfirmationValidity(event.currentTarget as HTMLFormElement)}>
            <div class="cluster-copy"><h2>Start a new cluster</h2><p>Create its first replicated administrator identity.</p></div>
            <div class="cluster-create-fields">
              <label>Administrator username<input name="admin_username" autocomplete="username" value="admin" ?disabled=${this.saving} required /></label>
              <label>Administrator password<input name="password" type="password" minlength="12" autocomplete="new-password" ?disabled=${this.saving} required /></label>
              <label>Repeat password<input name="password_confirmation" type="password" minlength="12" autocomplete="new-password" ?disabled=${this.saving} required /></label>
            </div>
            ${renderFormSubmit({ label: this.saving ? "Setting up..." : "Create new cluster", busy: this.saving, error: this.error })}
          </form>
          <div class="cluster-divider"><span>Or</span></div>
          <form class="cluster-join" @submit=${this.joinCluster}>
            <div class="cluster-copy"><h2>Join an existing cluster</h2><p>Paste an <code>up://</code> join token from a current member.</p></div>
            ${this.joining ? html`<p class="joining-status" role="status">Joining cluster. Checking route connectivity.</p>` : nothing}
            <div class="cluster-join-fields">
              <label>Join token<input name="join_link" type="url" pattern="up://.*" placeholder="up://node.example/token" autocomplete="off" ?disabled=${this.saving} required /></label>
              ${renderFormSubmit({ label: this.joining ? "Joining cluster..." : "Join cluster", className: "secondary", busy: this.saving, error: this.error })}
            </div>
          </form>
        `,
      })}`;
  }

  private renderChannel() {
    return html`
      <span class="eyebrow">Optional · step 2 of 3</span><h1>Add a notification channel</h1>
      <p class="lead">Send availability transitions through Telegram, SMTP, or a webhook. <span class="count">${this.setup.channel_count} already configured</span></p>
      ${renderCard({
        content: html`<upgrid-notification-channel-form default-channel submit-label="Create and continue" cancel-label="Skip" .disabled=${this.saving} @channel-cancel=${this.next} @channel-saved=${this.next}></upgrid-notification-channel-form>`,
      })}`;
  }

  private renderTarget() {
    return html`
      <span class="eyebrow">Optional · step 3 of 3</span><h1>Monitor your first target</h1>
      <p class="lead">Configure an HTTP endpoint now or continue to the dashboard. <span class="count">${this.setup.target_count} already configured</span></p>
      ${renderCard({
        content: html`
          <form class="choice" @submit=${this.createTarget}>
            <label>Name<input name="name" placeholder="Production API" required /></label>
            <label>URL<input name="url" type="url" placeholder="https://example.com/health" required /></label>
            <div class="row"><label>Method<input name="method" value="GET" required /></label><label>Interval (seconds)<input name="interval" type="number" min="1" value="60" required /></label></div>
            <div class="row"><label>Timeout (seconds)<input name="timeout" type="number" min="1" value="10" required /></label><label>Failures before down<input name="failures" type="number" min="1" value="3" required /></label></div>
            ${this.channels.length ? html`<fieldset><legend>Notification channels</legend>${this.channels.map((channel) => html`<upgrid-toggle-switch name="channel_id" value=${channel.id}>${channel.name}</upgrid-toggle-switch>`)}</fieldset>` : html`<upgrid-empty-state>No notification channels are available</upgrid-empty-state>`}
            <div class="actions"><button class="secondary" type="button" @click=${this.next} ?disabled=${this.saving}>Skip</button>${renderFormSubmit({ label: "Create and finish", busy: this.saving, error: this.error })}</div>
          </form>
        `,
      })}`;
  }
}
