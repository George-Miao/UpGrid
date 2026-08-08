import { LitElement, css, html, nothing } from "lit";
import { customElement, state } from "lit/decorators.js";
import {
  type Alert,
  type Channel,
  type Target,
  type TargetInput,
  request,
} from "./api.ts";

@customElement("upgrid-app")
export class UpgridApp extends LitElement {
  @state() private targets: Target[] = [];
  @state() private channels: Channel[] = [];
  @state() private alerts: Alert[] = [];
  @state() private error = "";
  @state() private live = false;
  @state() private saving = false;
  private events?: EventSource;

  static styles = css`
    :host {
      color-scheme: dark;
      --bg: #090d0c;
      --panel: #111715;
      --panel-2: #151d1a;
      --line: #27322e;
      --muted: #8fa099;
      --text: #edf7f2;
      --green: #58e29c;
      --red: #ff7575;
      --amber: #f2c264;
      display: block;
      min-height: 100vh;
      background:
        radial-gradient(circle at 12% -5%, #18392d 0, transparent 30%),
        linear-gradient(145deg, #090d0c 0%, #0c1210 55%, #09100d 100%);
      color: var(--text);
      font: 14px/1.5 Inter, ui-sans-serif, system-ui, sans-serif;
    }
    * { box-sizing: border-box; }
    button, input, select { font: inherit; }
    .shell { max-width: 1200px; margin: auto; padding: 28px 24px 72px; }
    header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 34px; }
    .brand, .actions, .live, nav { display: flex; align-items: center; }
    .brand { gap: 13px; }
    .brand img { width: 42px; height: 42px; filter: drop-shadow(0 0 18px #40d89035); }
    .brand strong { display: block; font-size: 19px; letter-spacing: .02em; }
    .brand span, .live, .eyebrow, .meta { color: var(--muted); font-size: 12px; }
    nav { gap: 4px; padding: 4px; border: 1px solid var(--line); border-radius: 11px; background: #0d1210aa; }
    nav a { color: var(--muted); padding: 7px 11px; text-decoration: none; border-radius: 7px; }
    nav a.active { color: var(--text); background: #202b27; }
    .actions { gap: 12px; }
    .live { gap: 7px; }
    .dot { width: 7px; height: 7px; border-radius: 50%; background: var(--amber); }
    .dot.on { background: var(--green); box-shadow: 0 0 10px var(--green); }
    .heading { display: flex; align-items: flex-end; justify-content: space-between; margin-bottom: 18px; }
    .heading h1 { margin: 2px 0 0; font-size: clamp(27px, 4vw, 38px); line-height: 1.1; letter-spacing: -.035em; }
    .eyebrow { text-transform: uppercase; letter-spacing: .16em; }
    .button { border: 1px solid #3e765a; border-radius: 9px; background: #1c4a35; color: #e8fff2; padding: 9px 13px; cursor: pointer; }
    .button:hover { border-color: #62b988; }
    .button:disabled { cursor: wait; opacity: .65; }
    .summary { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 20px; }
    .metric, .panel { border: 1px solid var(--line); background: #111715dc; box-shadow: 0 16px 48px #0002; }
    .metric { border-radius: 14px; padding: 17px 18px; }
    .metric span { display: block; color: var(--muted); font-size: 11px; letter-spacing: .11em; text-transform: uppercase; }
    .metric strong { display: block; margin-top: 5px; font-size: 29px; font-weight: 560; }
    .panel { border-radius: 16px; overflow: hidden; }
    .panel-head { display: flex; align-items: center; justify-content: space-between; padding: 17px 20px; border-bottom: 1px solid var(--line); }
    .panel-head h2 { margin: 0; font-size: 14px; }
    .target { display: grid; grid-template-columns: auto minmax(0, 1fr) auto; gap: 14px; align-items: center; padding: 17px 20px; border-bottom: 1px solid #202925; }
    .target:last-child { border-bottom: 0; }
    .state { width: 10px; height: 10px; border-radius: 50%; background: var(--amber); box-shadow: 0 0 12px currentColor; }
    .state.up { color: var(--green); background: var(--green); }
    .state.down { color: var(--red); background: var(--red); }
    .target h3 { margin: 0 0 3px; font-size: 14px; }
    .meta { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .latency { text-align: right; }
    .latency strong { display: block; font-weight: 500; }
    .latency span { color: var(--muted); font-size: 11px; }
    .empty { padding: 54px 20px; color: var(--muted); text-align: center; }
    .notice { margin: 0 0 16px; border: 1px solid #7b3937; border-radius: 10px; background: #391b1a; color: #ffb3af; padding: 10px 12px; }
    dialog { width: min(580px, calc(100% - 28px)); border: 1px solid var(--line); border-radius: 17px; background: var(--panel); color: var(--text); padding: 0; box-shadow: 0 28px 90px #000b; }
    dialog::backdrop { background: #040706cc; backdrop-filter: blur(5px); }
    .dialog-head { padding: 20px 22px 15px; border-bottom: 1px solid var(--line); }
    .dialog-head h2 { margin: 0; font-size: 18px; }
    .dialog-head p { margin: 4px 0 0; color: var(--muted); }
    form { display: grid; gap: 13px; padding: 20px 22px 22px; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 11px; }
    label { display: grid; gap: 5px; color: var(--muted); font-size: 11px; letter-spacing: .03em; }
    input, select { width: 100%; border: 1px solid var(--line); border-radius: 9px; outline: 0; background: #0c110f; color: var(--text); padding: 9px 10px; }
    input:focus, select:focus { border-color: #4b936c; }
    .dialog-actions { display: flex; justify-content: flex-end; gap: 8px; margin-top: 5px; }
    .secondary { background: transparent; color: var(--muted); border-color: var(--line); }
    @media (max-width: 720px) {
      .shell { padding: 20px 14px 60px; }
      nav { display: none; }
      .summary { grid-template-columns: 1fr 1fr; }
      .heading { align-items: flex-start; gap: 16px; }
      .target { grid-template-columns: auto minmax(0, 1fr); }
      .latency { grid-column: 2; text-align: left; }
    }
  `;

  connectedCallback(): void {
    super.connectedCallback();
    void this.refresh();
    this.events = new EventSource("/api/v1/events");
    this.events.addEventListener("state", () => void this.refresh());
    this.events.onopen = () => (this.live = true);
    this.events.onerror = () => (this.live = false);
  }

  disconnectedCallback(): void {
    this.events?.close();
    super.disconnectedCallback();
  }

  private async refresh(): Promise<void> {
    try {
      [this.targets, this.channels, this.alerts] = await Promise.all([
        request<Target[]>("/api/v1/targets"),
        request<Channel[]>("/api/v1/channels"),
        request<Alert[]>("/api/v1/alerts"),
      ]);
      this.error = "";
    } catch (error) {
      this.error = error instanceof Error ? error.message : String(error);
    }
  }

  private openTargetDialog(): void {
    this.renderRoot.querySelector<HTMLDialogElement>("#target-dialog")?.showModal();
  }

  private closeTargetDialog(): void {
    this.renderRoot.querySelector<HTMLDialogElement>("#target-dialog")?.close();
  }

  private async createTarget(event: SubmitEvent): Promise<void> {
    event.preventDefault();
    const form = event.currentTarget as HTMLFormElement;
    const fields = new FormData(form);
    const input: TargetInput = {
      name: String(fields.get("name")),
      url: String(fields.get("url")),
      method: String(fields.get("method")),
      accepted_statuses: [{ start: 200, end: 299 }],
      follow_redirects: true,
      max_redirects: 5,
      interval_seconds: Number(fields.get("interval")),
      timeout_seconds: Number(fields.get("timeout")),
      failure_threshold: Number(fields.get("failures")),
      headers: {},
      body: null,
      body_contains: null,
      skip_tls_verification: false,
      notification_channel_ids: [],
    };
    this.saving = true;
    try {
      await request<Target>("/api/v1/targets", {
        method: "POST",
        body: JSON.stringify(input),
      });
      form.reset();
      this.closeTargetDialog();
      await this.refresh();
    } catch (error) {
      this.error = error instanceof Error ? error.message : String(error);
    } finally {
      this.saving = false;
    }
  }

  protected render() {
    const up = this.targets.filter((target) => target.availability === "up").length;
    const down = this.targets.filter((target) => target.availability === "down").length;
    const pending = this.alerts.filter((alert) => alert.delivery === "pending").length;
    return html`
      <main class="shell">
        <header>
          <div class="brand">
            <img src="/favicon.svg" alt="" />
            <div><strong>UpGrid</strong><span>Distributed service monitoring</span></div>
          </div>
          <nav aria-label="Primary">
            <a class="active" href="#overview">Overview</a>
            <a href="#targets">Targets</a>
            <a href="#alerts">Alerts</a>
            <a href="#cluster">Cluster</a>
          </nav>
          <div class="actions">
            <div class="live"><i class="dot ${this.live ? "on" : ""}"></i>${this.live ? "live" : "connecting"}</div>
          </div>
        </header>
        <section class="heading" id="overview">
          <div><span class="eyebrow">Cluster status</span><h1>Overview</h1></div>
          <button class="button" @click=${this.openTargetDialog}>Add target</button>
        </section>
        ${this.error ? html`<div class="notice" role="alert">${this.error}</div>` : nothing}
        <section class="summary" aria-label="Target summary">
          <div class="metric"><span>Targets</span><strong>${this.targets.length}</strong></div>
          <div class="metric"><span>Up</span><strong>${up}</strong></div>
          <div class="metric"><span>Down</span><strong>${down}</strong></div>
          <div class="metric"><span>Pending alerts</span><strong>${pending}</strong></div>
        </section>
        <section class="panel" id="targets">
          <div class="panel-head"><h2>Targets</h2><span class="meta">${this.targets.length} configured</span></div>
          ${this.targets.length
            ? this.targets.map((target) => this.renderTarget(target))
            : html`<div class="empty">No targets yet. Add the first one to begin monitoring.</div>`}
        </section>
      </main>
      <dialog id="target-dialog">
        <div class="dialog-head"><h2>Add target</h2><p>Start monitoring an HTTP or HTTPS endpoint.</p></div>
        <form @submit=${this.createTarget}>
          <label>Name<input name="name" placeholder="Production API" required /></label>
          <label>URL<input name="url" type="url" placeholder="https://example.com/health" required /></label>
          <div class="row">
            <label>Method<input name="method" value="GET" required /></label>
            <label>Interval (seconds)<input name="interval" type="number" min="1" value="60" required /></label>
          </div>
          <div class="row">
            <label>Timeout (seconds)<input name="timeout" type="number" min="1" value="10" required /></label>
            <label>Failures before Down<input name="failures" type="number" min="1" value="3" required /></label>
          </div>
          <div class="dialog-actions">
            <button class="button secondary" type="button" @click=${this.closeTargetDialog}>Cancel</button>
            <button class="button" type="submit" ?disabled=${this.saving}>${this.saving ? "Creating…" : "Create target"}</button>
          </div>
        </form>
      </dialog>
    `;
  }

  private renderTarget(target: Target) {
    const latest = target.latest_evaluation;
    return html`
      <article class="target">
        <i class="state ${target.availability}" aria-label=${target.availability}></i>
        <div>
          <h3>${target.name}</h3>
          <div class="meta">${target.method} · ${target.url} · every ${target.interval_seconds}s</div>
        </div>
        <div class="latency">
          <strong>${latest ? `${latest.latency_ms} ms` : "—"}</strong>
          <span>${latest ? (latest.status_code ?? "network error") : "waiting"}</span>
        </div>
      </article>
    `;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "upgrid-app": UpgridApp;
  }
}
