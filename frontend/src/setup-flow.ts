import { LitElement, css, html, nothing } from "lit";
import { customElement, property, state } from "lit/decorators.js";
import { type Channel, type Setup, type TargetInput, request } from "./api.ts";

@customElement("upgrid-setup")
export class UpgridSetup extends LitElement {
  @property({ attribute: false }) setup!: Setup;
  @state() private channelKind: "webhook" | "telegram" = "webhook";
  @state() private channels: Channel[] = [];
  @state() private saving = false;
  @state() private error = "";

  static styles = css`
    :host { display: block; }
    .flow { width: min(680px, 100%); margin: 8vh auto 0; }
    .eyebrow { color: var(--muted); font-size: 12px; letter-spacing: .16em; text-transform: uppercase; }
    h1 { margin: 5px 0 8px; font-size: clamp(30px, 5vw, 46px); letter-spacing: -.04em; }
    .lead { margin: 0 0 24px; color: var(--muted); font-size: 15px; }
    .panel { border: 1px solid var(--line); border-radius: 16px; background: var(--panel-surface); box-shadow: 0 16px 48px var(--panel-shadow); overflow: hidden; }
    .choice { display: grid; gap: 14px; padding: 22px; border-top: 1px solid var(--line); }
    .choice:first-child { border-top: 0; }
    .choice h2 { margin: 0; font-size: 17px; }
    .choice p { margin: -8px 0 0; color: var(--muted); }
    form { display: grid; gap: 13px; }
    label { display: grid; gap: 5px; color: var(--muted); font-size: 11px; }
    input, select { width: 100%; border: 1px solid var(--line); border-radius: 9px; outline: 0; background: var(--input-bg); color: var(--text); padding: 9px 10px; font: inherit; transition: border-color 160ms ease, opacity 160ms ease; }
    input:focus, select:focus { border-color: var(--focus); }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 11px; }
    .actions { display: flex; justify-content: flex-end; gap: 9px; margin-top: 5px; }
    button { border: 1px solid var(--button-border); border-radius: 9px; background: var(--button-bg); color: var(--button-text); padding: 9px 13px; cursor: pointer; font: inherit; transition: background-color 160ms ease, border-color 160ms ease, opacity 160ms ease, transform 120ms ease; }
    button:hover { border-color: var(--button-hover-border); }
    button:active { transform: translateY(1px); }
    button:disabled { cursor: not-allowed; opacity: .65; }
    .secondary { background: transparent; color: var(--muted); border-color: var(--line); }
    .notice { margin-bottom: 16px; border: 1px solid var(--notice-border); border-radius: 10px; background: var(--notice-bg); color: var(--notice-text); padding: 10px 12px; }
    .count { display: inline-block; margin-top: 6px; color: var(--green); font-size: 12px; }
    @media (max-width: 600px) { .flow { margin-top: 3vh; } .row { grid-template-columns: 1fr; } }
  `;

  connectedCallback(): void {
    super.connectedCallback();
    void this.loadChannels();
  }

  protected updated(changed: Map<PropertyKey, unknown>): void {
    if (changed.has("setup")) void this.loadChannels();
  }

  private async loadChannels(): Promise<void> {
    if (!this.setup?.cluster_ready || this.setup.phase !== "target") return;
    try {
      this.channels = await request<Channel[]>("/api/v1/channels");
    } catch (error) {
      this.fail(error);
    }
  }

  private submittedNodeName(form: HTMLFormElement): string {
    return String(new FormData(form).get("node_name") ?? "").trim();
  }

  private async createCluster(event: SubmitEvent): Promise<void> {
    event.preventDefault();
    if (!window.confirm("Create a new single-Node Cluster?")) return;
    const form = event.currentTarget as HTMLFormElement;
    await this.choose("/api/v1/setup/new-cluster", { node_name: this.submittedNodeName(form) });
  }

  private async joinCluster(event: SubmitEvent): Promise<void> {
    event.preventDefault();
    const form = event.currentTarget as HTMLFormElement;
    const fields = new FormData(form);
    await this.choose("/api/v1/cluster/join", {
      node_name: this.submittedNodeName(form),
      join_link: String(fields.get("join_link") ?? "").trim(),
    });
  }

  private async choose(path: string, body: object): Promise<void> {
    this.saving = true;
    this.error = "";
    try {
      await request(path, { method: "POST", body: JSON.stringify(body) });
      await this.waitForCluster();
    } catch (error) {
      this.fail(error);
      this.saving = false;
    }
  }

  private async waitForCluster(): Promise<void> {
    for (let attempt = 0; attempt < 120; attempt += 1) {
      await new Promise((resolve) => window.setTimeout(resolve, 250));
      try {
        const setup = await request<Setup>("/api/v1/setup");
        if (setup.cluster_ready) {
          this.changed(setup);
          return;
        }
      } catch {
        // The setup listener is replaced by the configured Cluster API.
      }
    }
    throw new Error("Cluster setup did not finish within 30 seconds");
  }

  private async createChannel(event: SubmitEvent): Promise<void> {
    event.preventDefault();
    const fields = new FormData(event.currentTarget as HTMLFormElement);
    const body = this.channelKind === "telegram"
      ? { type: "telegram", name: fields.get("name"), bot_token: fields.get("bot_token"), chat_id: fields.get("chat_id") }
      : { type: "webhook", name: fields.get("name"), url: fields.get("url"), headers: {} };
    await this.createResource("/api/v1/channels", body);
  }

  private async createTarget(event: SubmitEvent): Promise<void> {
    event.preventDefault();
    const fields = new FormData(event.currentTarget as HTMLFormElement);
    const input: TargetInput = {
      name: String(fields.get("name")),
      url: String(fields.get("url")),
      method: "GET",
      accepted_statuses: [{ start: 200, end: 299 }],
      follow_redirects: true,
      max_redirects: 5,
      interval_seconds: Number(fields.get("interval")),
      timeout_seconds: Number(fields.get("timeout")),
      failure_threshold: Number(fields.get("failures")),
      headers: {}, body: null, body_contains: null, skip_tls_verification: false,
      notification_channel_ids: fields.getAll("channel_id").map(String),
    };
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
    this.dispatchEvent(new CustomEvent("setup-changed", { detail: setup, bubbles: true, composed: true }));
  }

  private fail(error: unknown): void {
    this.error = error instanceof Error ? error.message : String(error);
  }

  protected render() {
    return html`<section class="flow" aria-label="UpGrid setup">
      ${this.error ? html`<div class="notice" role="alert">${this.error}</div>` : nothing}
      ${this.setup.phase === "cluster" ? this.renderCluster() : this.setup.phase === "channel" ? this.renderChannel() : this.renderTarget()}
    </section>`;
  }

  private renderCluster() {
    return html`
      <span class="eyebrow">First-run setup</span><h1>Choose your Cluster</h1>
      <p class="lead">Review this Node’s name, then create a new Cluster or use an invitation to join one.</p>
      <div class="panel">
        <form class="choice" @submit=${this.createCluster}>
          <h2>Start a new Cluster</h2><p>This Node becomes the first voting member.</p>
          <label>Node name<input name="node_name" .value=${this.setup.node_name} required /></label>
          <div class="actions"><button type="submit" ?disabled=${this.saving}>${this.saving ? "Setting up…" : "Create new Cluster"}</button></div>
        </form>
        <form class="choice" @submit=${this.joinCluster}>
          <h2>Join an existing Cluster</h2><p>Paste an <code>up://</code> Join Token from a current member.</p>
          <label>Node name<input name="node_name" .value=${this.setup.node_name} required /></label>
          <label>Join Token<input name="join_link" type="url" pattern="up://.*" placeholder="up://node.example/token" autocomplete="off" required /></label>
          <div class="actions"><button class="secondary" type="submit" ?disabled=${this.saving}>Join Cluster</button></div>
        </form>
      </div>`;
  }

  private renderChannel() {
    return html`
      <span class="eyebrow">Optional · Step 2 of 3</span><h1>Add a notification channel</h1>
      <p class="lead">Send availability transitions to Telegram or a webhook. <span class="count">${this.setup.channel_count} already configured</span></p>
      <div class="panel"><form class="choice" @submit=${this.createChannel}>
        <label>Type<select name="type" @change=${(event: Event) => (this.channelKind = (event.target as HTMLSelectElement).value as "webhook" | "telegram")}><option value="webhook">Webhook</option><option value="telegram">Telegram</option></select></label>
        <label>Name<input name="name" placeholder="On-call" required /></label>
        ${this.channelKind === "webhook"
          ? html`<label>Webhook URL<input name="url" type="url" placeholder="https://hooks.example.com/upgrid" required /></label>`
          : html`<label>Bot token<input name="bot_token" type="password" autocomplete="off" required /></label><label>Chat ID<input name="chat_id" required /></label>`}
        <div class="actions"><button class="secondary" type="button" @click=${this.next} ?disabled=${this.saving}>Skip</button><button type="submit" ?disabled=${this.saving}>Create and continue</button></div>
      </form></div>`;
  }

  private renderTarget() {
    return html`
      <span class="eyebrow">Optional · Step 3 of 3</span><h1>Monitor your first Target</h1>
      <p class="lead">Configure an HTTP endpoint now or continue to the dashboard. <span class="count">${this.setup.target_count} already configured</span></p>
      <div class="panel"><form class="choice" @submit=${this.createTarget}>
        <label>Name<input name="name" placeholder="Production API" required /></label>
        <label>URL<input name="url" type="url" placeholder="https://example.com/health" required /></label>
        <div class="row"><label>Interval (seconds)<input name="interval" type="number" min="1" value="60" required /></label><label>Timeout (seconds)<input name="timeout" type="number" min="1" value="10" required /></label></div>
        <label>Failures before Down<input name="failures" type="number" min="1" value="3" required /></label>
        ${this.channels.length ? html`<fieldset><legend>Notification channels</legend>${this.channels.map((channel) => html`<label><span><input name="channel_id" type="checkbox" value=${channel.id} /> ${channel.name}</span></label>`)}</fieldset>` : nothing}
        <div class="actions"><button class="secondary" type="button" @click=${this.next} ?disabled=${this.saving}>Skip</button><button type="submit" ?disabled=${this.saving}>Create and finish</button></div>
      </form></div>`;
  }
}
