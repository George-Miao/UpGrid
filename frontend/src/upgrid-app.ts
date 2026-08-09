import { LitElement, css, html, nothing } from "lit";
import { customElement, state } from "lit/decorators.js";
import darkIcon from "@iconify-icons/lucide/moon";
import systemIcon from "@iconify-icons/lucide/palette";
import pauseIcon from "@iconify-icons/lucide/pause";
import playIcon from "@iconify-icons/lucide/play";
import brightIcon from "@iconify-icons/lucide/sun";
import deleteIcon from "@iconify-icons/lucide/trash-2";
import closeIcon from "@iconify-icons/lucide/x";
import "iconify-icon";
import {
  type Alert,
  type Channel,
  type Cluster,
  type JoinLink,
  type JoinToken,
  type Secret,
  type Target,
  type TargetInput,
  request,
} from "./api.ts";

const themes = ["system", "dark", "bright"] as const;
type Theme = (typeof themes)[number];
const themeIcons = { system: systemIcon, dark: darkIcon, bright: brightIcon };

function storedTheme(): Theme {
  const theme = localStorage.getItem("upgrid-theme");
  return themes.includes(theme as Theme) ? (theme as Theme) : "system";
}

@customElement("upgrid-app")
export class UpgridApp extends LitElement {
  @state() private targets: Target[] = [];
  @state() private channels: Channel[] = [];
  @state() private alerts: Alert[] = [];
  @state() private secrets: Secret[] = [];
  @state() private cluster?: Cluster;
  @state() private joinTokens: JoinToken[] = [];
  @state() private error = "";
  @state() private live = false;
  @state() private saving = false;
  @state() private selected?: Target;
  @state() private channelKind: "webhook" | "telegram" = "webhook";
  @state() private joinCommand = "";
  @state() private search = "";
  @state() private statusFilter = "all";
  @state() private sort = "name";
  @state() private selectedIds = new Set<string>();
  @state() private activeSection = "overview";
  @state() private copied = false;
  @state() private theme = storedTheme();
  @state() private detailDirty = false;
  private events?: EventSource;
  private detailInitialState = "";
  private readonly systemTheme = matchMedia("(prefers-color-scheme: light)");
  private readonly systemThemeChanged = () => {
    if (this.theme === "system") this.applyTheme();
  };

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
      --page-background:
        radial-gradient(circle at 12% -5%, #18392d 0, transparent 30%),
        linear-gradient(145deg, #090d0c 0%, #0c1210 55%, #09100d 100%);
      --brand-shadow: #40d89035;
      --nav-bg: #0d1210aa;
      --active-bg: #202b27;
      --button-border: #3e765a;
      --button-bg: #1c4a35;
      --button-text: #e8fff2;
      --button-hover-border: #62b988;
      --panel-surface: #111715dc;
      --panel-shadow: #0002;
      --divider: #202925;
      --badge-border: #3c554a;
      --badge-text: #a7c3b7;
      --row-hover: #17201c;
      --notice-border: #7b3937;
      --notice-bg: #391b1a;
      --notice-text: #ffb3af;
      --bulk-bg: #16221d;
      --dialog-shadow: #000b;
      --backdrop: #040706cc;
      --input-bg: #0c110f;
      --focus: #4b936c;
      --danger-text: #ff9b97;
      --danger-border: #633b39;
      --warning-bg: #594315;
      --warning-text: #ffd778;
      --warning-border: #9c7625;
      --join-bg: #0b110e;
      display: block;
      min-height: 100vh;
      background: var(--page-background);
      color: var(--text);
      font: 14px/1.5 Inter, ui-sans-serif, system-ui, sans-serif;
      transition: background 220ms ease, color 180ms ease;
    }
    * { box-sizing: border-box; }
    button, input, select { font: inherit; }
    .shell { max-width: 1200px; margin: auto; padding: 28px 24px 72px; }
    header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 34px; }
    .brand, .actions, .live, nav { display: flex; align-items: center; }
    .brand { gap: 13px; }
    .brand-line { display: flex; align-items: center; gap: 12px; }
    .brand img { width: 42px; height: 42px; filter: drop-shadow(0 0 18px var(--brand-shadow)); }
    .brand strong { display: block; font-size: 19px; letter-spacing: .02em; }
    .brand span, .live, .eyebrow, .meta { color: var(--muted); font-size: 12px; }
    nav { gap: 4px; padding: 4px; border: 1px solid var(--line); border-radius: 11px; background: var(--nav-bg); }
    nav a { color: var(--muted); padding: 7px 11px; text-decoration: none; border-radius: 7px; transition: background-color 160ms ease, color 160ms ease; }
    nav a.active { color: var(--text); background: var(--active-bg); }
    .actions { gap: 12px; }
    .live { gap: 7px; }
    .dot { width: 7px; height: 7px; border-radius: 50%; background: var(--amber); transition: background-color 160ms ease, box-shadow 160ms ease; }
    .dot.on { background: var(--green); box-shadow: 0 0 10px var(--green); }
    .heading { display: flex; align-items: flex-end; justify-content: space-between; margin-bottom: 18px; }
    .heading h1 { margin: 2px 0 0; font-size: clamp(27px, 4vw, 38px); line-height: 1.1; letter-spacing: -.035em; }
    .eyebrow { text-transform: uppercase; letter-spacing: .16em; }
    .button { border: 1px solid var(--button-border); border-radius: 9px; background: var(--button-bg); color: var(--button-text); padding: 9px 13px; cursor: pointer; transition: background-color 160ms ease, border-color 160ms ease, color 160ms ease, opacity 160ms ease, transform 120ms ease; }
    .button:hover { border-color: var(--button-hover-border); }
    .button:active { transform: translateY(1px); }
    .button:disabled { cursor: not-allowed; opacity: .65; }
    .button[aria-busy="true"] { cursor: wait; }
    .icon-button { display: grid; width: 36px; height: 36px; place-items: center; padding: 0; }
    iconify-icon { display: inline-block; width: 18px; height: 18px; font-size: 18px; }
    .summary { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 20px; }
    .metric, .panel { border: 1px solid var(--line); background: var(--panel-surface); box-shadow: 0 16px 48px var(--panel-shadow); transition: background-color 180ms ease, border-color 180ms ease, box-shadow 180ms ease; }
    .metric { border-radius: 14px; padding: 17px 18px; }
    .metric span { display: block; color: var(--muted); font-size: 11px; letter-spacing: .11em; text-transform: uppercase; }
    .metric strong { display: block; margin-top: 5px; font-size: 29px; font-weight: 560; }
    .panel { border-radius: 16px; overflow: hidden; }
    .resources { display: grid; grid-template-columns: 1fr 1fr; gap: 18px; margin-top: 18px; }
    .resource { display: flex; align-items: center; justify-content: space-between; gap: 12px; padding: 13px 20px; border-bottom: 1px solid var(--divider); }
    .resource:last-child { border-bottom: 0; }
    .resource strong { display: block; font-size: 13px; }
    .resource code { color: var(--muted); font-size: 11px; }
    .badge { border: 1px solid var(--badge-border); border-radius: 999px; color: var(--badge-text); padding: 2px 7px; font-size: 10px; text-transform: uppercase; }
    .panel-head { display: flex; align-items: center; justify-content: space-between; padding: 17px 20px; border-bottom: 1px solid var(--line); }
    .panel-head h2 { margin: 0; font-size: 14px; }
    .target-wrap { display: grid; grid-template-columns: auto minmax(0, 1fr); align-items: center; border-bottom: 1px solid var(--divider); padding-left: 20px; }
    .target-wrap:last-child { border-bottom: 0; }
    .select-target { width: 15px; height: 15px; accent-color: var(--green); }
    .target { width: 100%; display: grid; grid-template-columns: auto minmax(0, 1fr) auto; gap: 14px; align-items: center; padding: 17px 20px 17px 14px; border: 0; background: transparent; color: var(--text); text-align: left; cursor: pointer; }
    .target-wrap, .target { transition: background-color 150ms ease; }
    .target-wrap:hover, .target-wrap:hover .target { background: var(--row-hover); }
    .state { width: 10px; height: 10px; border-radius: 50%; background: var(--amber); box-shadow: 0 0 12px currentColor; transition: background-color 160ms ease, color 160ms ease, box-shadow 160ms ease; }
    .state.up { color: var(--green); background: var(--green); }
    .state.down { color: var(--red); background: var(--red); }
    .state.paused { color: var(--muted); background: var(--muted); box-shadow: none; }
    .target h3 { margin: 0 0 3px; font-size: 14px; }
    .meta { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .latency { text-align: right; }
    .latency strong { display: block; font-weight: 500; }
    .latency span { color: var(--muted); font-size: 11px; }
    .target-side { display: flex; align-items: center; gap: 20px; }
    .mini-chart { display: flex; width: 88px; height: 32px; align-items: flex-end; gap: 2px; }
    .mini-bar { flex: 1; min-width: 2px; max-width: 7px; border-radius: 2px 2px 1px 1px; opacity: .75; transition: background-color 160ms ease, height 180ms ease, opacity 160ms ease; }
    .mini-bar.up { background: var(--green); }
    .mini-bar.down { background: var(--red); }
    .empty { padding: 54px 20px; color: var(--muted); text-align: center; }
    .notice { margin: 0 0 16px; border: 1px solid var(--notice-border); border-radius: 10px; background: var(--notice-bg); color: var(--notice-text); padding: 10px 12px; }
    .toolbar { display: grid; grid-template-columns: minmax(180px, 1fr) auto auto; gap: 8px; padding: 12px 20px; border-bottom: 1px solid var(--line); }
    .toolbar input, .toolbar select { padding: 7px 9px; }
    .bulk { display: flex; align-items: center; gap: 8px; padding: 10px 20px; border-bottom: 1px solid var(--line); background: var(--bulk-bg); }
    .bulk-actions { display: flex; align-items: center; gap: 8px; margin-left: auto; }
    .bulk, .bulk-actions .button { animation: reveal 160ms ease-out; }
    @keyframes reveal { from { opacity: 0; transform: translateY(-3px); } }
    dialog { width: min(580px, calc(100% - 28px)); border: 1px solid var(--line); border-radius: 17px; background: var(--panel); color: var(--text); padding: 0; box-shadow: 0 28px 90px var(--dialog-shadow); opacity: 0; transform: translateY(8px) scale(.985); transition: opacity 170ms ease, transform 170ms ease, overlay 170ms allow-discrete, display 170ms allow-discrete; }
    dialog[open] { opacity: 1; transform: translateY(0) scale(1); }
    dialog::backdrop { background: var(--backdrop); backdrop-filter: blur(5px); opacity: 0; transition: opacity 170ms ease, overlay 170ms allow-discrete, display 170ms allow-discrete; }
    dialog[open]::backdrop { opacity: 1; }
    @starting-style {
      dialog[open] { opacity: 0; transform: translateY(8px) scale(.985); }
      dialog[open]::backdrop { opacity: 0; }
    }
    .dialog-head { position: relative; padding: 20px 58px 15px 22px; border-bottom: 1px solid var(--line); }
    .dialog-head h2 { margin: 0; font-size: 18px; }
    .dialog-head p { margin: 4px 0 0; color: var(--muted); }
    form { display: grid; gap: 13px; padding: 20px 22px 22px; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 11px; }
    label { display: grid; gap: 5px; color: var(--muted); font-size: 11px; letter-spacing: .03em; }
    input, select { width: 100%; border: 1px solid var(--line); border-radius: 9px; outline: 0; background: var(--input-bg); color: var(--text); padding: 9px 10px; transition: background-color 160ms ease, border-color 160ms ease, color 160ms ease, opacity 160ms ease; }
    input:focus, select:focus { border-color: var(--focus); }
    input:disabled { cursor: not-allowed; opacity: .5; }
    .dialog-actions { display: flex; justify-content: flex-end; gap: 8px; margin-top: 5px; }
    .danger-actions { display: flex; gap: 8px; margin-right: auto; }
    .secondary { background: transparent; color: var(--muted); border-color: var(--line); }
    .danger { background: transparent; color: var(--danger-text); border-color: var(--danger-border); }
    .warning { background: transparent; color: var(--warning-text); border-color: var(--warning-border); }
    .warning:hover { border-color: var(--warning-text); }
    .success { background: transparent; color: var(--green); border-color: var(--green); }
    .success:hover { border-color: var(--button-text); }
    .dialog-close { position: absolute; top: 12px; right: 14px; }
    .check { display: flex; align-items: center; gap: 8px; }
    .check input { width: auto; }
    .history { margin: 0 22px 22px; border-top: 1px solid var(--line); padding-top: 18px; }
    .history-head, .chart-legend, .chart-legend span, .chart-axis { display: flex; align-items: center; }
    .history-head { justify-content: space-between; margin-bottom: 12px; }
    .history-head h3 { margin: 0; font-size: 14px; }
    .chart-plot { display: grid; grid-template-columns: 38px minmax(0, 1fr); gap: 7px; }
    .chart-scale { display: flex; height: 140px; flex-direction: column; justify-content: space-between; padding: 1px 0 7px; color: var(--muted); font-size: 9px; text-align: right; }
    .history-chart { display: flex; height: 140px; align-items: flex-end; gap: 3px; padding: 14px 10px 8px; border: 1px solid var(--line); border-radius: 10px; background: var(--input-bg); }
    .history-bar { flex: 1; min-width: 3px; max-width: 16px; border-radius: 3px 3px 1px 1px; opacity: .82; transform-origin: bottom; transition: opacity 120ms ease, transform 120ms ease; }
    .history-bar:hover { opacity: 1; transform: scaleX(1.15); }
    .history-bar.up { background: var(--green); }
    .history-bar.down { background: var(--red); }
    .chart-axis { justify-content: space-between; margin: 5px 0 0 45px; color: var(--muted); font-size: 10px; }
    .chart-legend { justify-content: flex-end; gap: 12px; margin-top: 9px; color: var(--muted); font-size: 10px; }
    .chart-legend span { gap: 5px; }
    .chart-legend i { width: 7px; height: 7px; border-radius: 2px; }
    .chart-legend .up { background: var(--green); }
    .chart-legend .down { background: var(--red); }
    .join-command { margin: 20px 22px; border: 1px solid var(--line); border-radius: 10px; background: var(--join-bg); color: var(--green); padding: 13px; overflow-wrap: anywhere; font: 12px/1.6 ui-monospace, SFMono-Regular, monospace; }
    :host([data-theme="bright"]) {
        color-scheme: light;
        --bg: #f4f8f6;
        --panel: #ffffff;
        --panel-2: #eef5f1;
        --line: #d3dfd9;
        --muted: #5d6e66;
        --text: #16211c;
        --green: #087a49;
        --red: #c53434;
        --amber: #9a6700;
        --page-background:
          radial-gradient(circle at 12% -5%, #d9f2e4 0, transparent 32%),
          linear-gradient(145deg, #fbfdfc 0%, #f3f8f5 55%, #edf5f1 100%);
        --brand-shadow: #159e5240;
        --nav-bg: #ffffffcc;
        --active-bg: #e4efe9;
        --button-border: #16764b;
        --button-bg: #087a49;
        --button-text: #ffffff;
        --button-hover-border: #075f3a;
        --panel-surface: #ffffffeb;
        --panel-shadow: #2745381a;
        --divider: #e3ebe7;
        --badge-border: #a6beb2;
        --badge-text: #426356;
        --row-hover: #e9f4ee;
        --notice-border: #e2aaa6;
        --notice-bg: #fff0ef;
        --notice-text: #9f2922;
        --bulk-bg: #e8f4ed;
        --dialog-shadow: #233b3050;
        --backdrop: #17251f66;
        --input-bg: #ffffff;
        --focus: #168655;
        --danger-text: #b42318;
        --danger-border: #dda29d;
        --warning-bg: #fff1bd;
        --warning-text: #805b00;
        --warning-border: #d4aa36;
        --join-bg: #eef8f2;
    }
    @media (prefers-reduced-motion: reduce) {
      :host, nav a, .button, .metric, .panel, .target-wrap, .target, .dot, .state, .mini-bar, .history-bar, dialog, dialog::backdrop, input, select { transition-duration: 0s; }
      .bulk, .bulk-actions .button { animation-duration: 0s; }
    }
    @media (max-width: 720px) {
      .shell { padding: 20px 14px 60px; }
      nav { display: none; }
      .summary { grid-template-columns: 1fr 1fr; }
      .resources { grid-template-columns: 1fr; }
      .toolbar { grid-template-columns: 1fr 1fr; }
      .toolbar input { grid-column: 1 / -1; }
      .heading { align-items: flex-start; gap: 16px; }
      .target { grid-template-columns: auto minmax(0, 1fr); }
      .target-side { grid-column: 2; justify-self: start; }
      .latency { text-align: left; }
    }
  `;

  connectedCallback(): void {
    super.connectedCallback();
    this.applyTheme();
    this.systemTheme.addEventListener("change", this.systemThemeChanged);
    void this.refresh();
    this.events = new EventSource("/api/v1/events");
    this.events.addEventListener("state", () => void this.refresh());
    this.events.onopen = () => (this.live = true);
    this.events.onerror = () => (this.live = false);
  }

  disconnectedCallback(): void {
    this.systemTheme.removeEventListener("change", this.systemThemeChanged);
    this.events?.close();
    super.disconnectedCallback();
  }

  private applyTheme(): void {
    const resolved = this.theme === "system"
      ? (this.systemTheme.matches ? "bright" : "dark")
      : this.theme;
    this.dataset.theme = resolved;
    document
      .querySelector<HTMLMetaElement>('meta[name="theme-color"]')
      ?.setAttribute("content", resolved === "bright" ? "#f4f8f6" : "#0b1110");
  }

  private cycleTheme(): void {
    this.theme = themes[(themes.indexOf(this.theme) + 1) % themes.length];
    localStorage.setItem("upgrid-theme", this.theme);
    this.applyTheme();
  }

  private async refresh(): Promise<void> {
    try {
      [
        this.targets,
        this.channels,
        this.alerts,
        this.secrets,
        this.cluster,
        this.joinTokens,
      ] = await Promise.all([
        request<Target[]>("/api/v1/targets"),
        request<Channel[]>("/api/v1/channels"),
        request<Alert[]>("/api/v1/alerts"),
        request<Secret[]>("/api/v1/secrets"),
        request<Cluster>("/api/v1/cluster"),
        request<JoinToken[]>("/api/v1/join-tokens"),
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

  private openTarget(target: Target): void {
    this.detailDirty = false;
    this.selected = target;
    void this.updateComplete.then(() => {
      const dialog = this.renderRoot.querySelector<HTMLDialogElement>("#detail-dialog");
      const form = dialog?.querySelector<HTMLFormElement>("form");
      if (form) this.detailInitialState = this.detailFormState(form);
      dialog?.showModal();
    });
  }

  private closeDetailDialog(): void {
    this.renderRoot.querySelector<HTMLDialogElement>("#detail-dialog")?.close();
    this.detailDirty = false;
    this.detailInitialState = "";
    this.selected = undefined;
  }

  private showDialog(id: string): void {
    this.renderRoot.querySelector<HTMLDialogElement>(`#${id}`)?.showModal();
  }

  private dismissOnBackdrop(event: MouseEvent): void {
    const dialog = event.currentTarget as HTMLDialogElement;
    if (event.target !== dialog) return;
    dialog.close();
    if (dialog.id === "detail-dialog") {
      this.detailDirty = false;
      this.detailInitialState = "";
      this.selected = undefined;
    }
  }

  private navigate(event: MouseEvent, section: string): void {
    event.preventDefault();
    this.activeSection = section;
    void this.updateComplete.then(() =>
      this.renderRoot
        .querySelector<HTMLElement>(`#${section}`)
        ?.scrollIntoView({ behavior: "smooth", block: "start" }),
    );
  }

  private closeDialog(id: string): void {
    this.renderRoot.querySelector<HTMLDialogElement>(`#${id}`)?.close();
  }

  private toggleMaxRedirects(event: Event): void {
    const followRedirects = event.currentTarget as HTMLInputElement;
    const maxRedirects = followRedirects.form?.elements.namedItem(
      "max_redirects",
    ) as HTMLInputElement | null;
    if (maxRedirects) maxRedirects.disabled = !followRedirects.checked;
    if (followRedirects.form) this.compareDetailForm(followRedirects.form);
  }

  private detailFormState(form: HTMLFormElement): string {
    return JSON.stringify([...new FormData(form).entries()]);
  }

  private compareDetailForm(form: HTMLFormElement): void {
    this.detailDirty = this.detailFormState(form) !== this.detailInitialState;
  }

  private updateDetailDirty(event: Event): void {
    this.compareDetailForm(event.currentTarget as HTMLFormElement);
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

  private async updateTarget(event: SubmitEvent): Promise<void> {
    event.preventDefault();
    if (!this.selected) return;
    const form = event.currentTarget as HTMLFormElement;
    const fields = new FormData(form);
    const statuses = String(fields.get("statuses"))
      .split(",")
      .map((part) => {
        const [start, end] = part.trim().split("-").map(Number);
        return { start, end: end || start };
      });
    const followRedirects = fields.get("follow_redirects") === "on";
    const input: TargetInput = {
      name: String(fields.get("name")),
      url: String(fields.get("url")),
      method: String(fields.get("method")),
      accepted_statuses: statuses,
      follow_redirects: followRedirects,
      max_redirects: followRedirects ? Number(fields.get("max_redirects")) : 0,
      interval_seconds: Number(fields.get("interval")),
      timeout_seconds: Number(fields.get("timeout")),
      failure_threshold: Number(fields.get("failures")),
      headers: Object.fromEntries(
        Object.entries(this.selected.headers).map(([name, value]) => [
          name,
          value.kind === "literal" ? value.value : { secret_id: value.secret_id },
        ]),
      ),
      body:
        this.selected.body?.kind === "literal"
          ? this.selected.body.value
          : this.selected.body
            ? { secret_id: this.selected.body.secret_id }
            : null,
      body_contains: String(fields.get("body_contains")) || null,
      skip_tls_verification: fields.get("skip_tls_verification") === "on",
      notification_channel_ids: this.selected.notification_channel_ids,
    };
    this.saving = true;
    try {
      await request<Target>(`/api/v1/targets/${this.selected.id}`, {
        method: "PUT",
        body: JSON.stringify(input),
      });
      this.closeDetailDialog();
      await this.refresh();
    } catch (error) {
      this.error = error instanceof Error ? error.message : String(error);
    } finally {
      this.saving = false;
    }
  }

  private async deleteTarget(): Promise<void> {
    if (!this.selected || !window.confirm("Delete this target and its history?")) return;
    this.saving = true;
    try {
      await request<void>(`/api/v1/targets/${this.selected.id}`, { method: "DELETE" });
      this.closeDetailDialog();
      await this.refresh();
    } catch (error) {
      this.error = error instanceof Error ? error.message : String(error);
    } finally {
      this.saving = false;
    }
  }

  private async setPaused(paused: boolean): Promise<void> {
    if (!this.selected) return;
    this.saving = true;
    try {
      await request<Target>(`/api/v1/targets/${this.selected.id}/${paused ? "pause" : "resume"}`, {
        method: "POST",
      });
      this.closeDetailDialog();
      await this.refresh();
    } catch (error) {
      this.error = error instanceof Error ? error.message : String(error);
    } finally {
      this.saving = false;
    }
  }

  private async createSecret(event: SubmitEvent): Promise<void> {
    event.preventDefault();
    const form = event.currentTarget as HTMLFormElement;
    const fields = new FormData(form);
    this.saving = true;
    try {
      await request<Secret>("/api/v1/secrets", {
        method: "POST",
        body: JSON.stringify({ name: fields.get("name"), value: fields.get("value") }),
      });
      form.reset();
      this.closeDialog("secret-dialog");
      await this.refresh();
    } catch (error) {
      this.error = error instanceof Error ? error.message : String(error);
    } finally {
      this.saving = false;
    }
  }

  private async createChannel(event: SubmitEvent): Promise<void> {
    event.preventDefault();
    const form = event.currentTarget as HTMLFormElement;
    const fields = new FormData(form);
    const body = this.channelKind === "telegram"
      ? {
          type: "telegram",
          name: fields.get("name"),
          bot_token: fields.get("bot_token"),
          chat_id: fields.get("chat_id"),
        }
      : {
          type: "webhook",
          name: fields.get("name"),
          url: fields.get("url"),
          headers: {},
        };
    this.saving = true;
    try {
      await request<Channel>("/api/v1/channels", {
        method: "POST",
        body: JSON.stringify(body),
      });
      form.reset();
      this.channelKind = "webhook";
      this.closeDialog("channel-dialog");
      await this.refresh();
    } catch (error) {
      this.error = error instanceof Error ? error.message : String(error);
    } finally {
      this.saving = false;
    }
  }

  private async createJoinLink(): Promise<void> {
    this.saving = true;
    try {
      const link = await request<JoinLink>("/api/v1/join-tokens", {
        method: "POST",
        body: JSON.stringify({ expires_in_seconds: 600 }),
      });
      this.joinCommand = `upgrid --join '${link.url}'`;
      this.copied = false;
      await this.refresh();
      this.showDialog("join-dialog");
    } catch (error) {
      this.error = error instanceof Error ? error.message : String(error);
    } finally {
      this.saving = false;
    }
  }

  private async revokeJoinToken(token: JoinToken): Promise<void> {
    if (!window.confirm("Revoke this Join Token? Nodes using it will no longer be admitted.")) return;
    this.saving = true;
    try {
      await request<void>(`/api/v1/join-tokens/${token.id}`, { method: "DELETE" });
      await this.refresh();
    } catch (error) {
      this.error = error instanceof Error ? error.message : String(error);
    } finally {
      this.saving = false;
    }
  }

  private async copyJoinCommand(): Promise<void> {
    let copied = false;
    try {
      await navigator.clipboard.writeText(this.joinCommand);
      copied = true;
    } catch {
      const field = document.createElement("textarea");
      field.value = this.joinCommand;
      field.style.position = "fixed";
      field.style.opacity = "0";
      document.body.append(field);
      field.select();
      copied = document.execCommand("copy");
      field.remove();
    }
    if (!copied) {
      this.error = "Could not copy the Join command";
      return;
    }
    this.copied = true;
    window.setTimeout(() => (this.copied = false), 2_000);
  }

  private toggleSelected(id: string, checked: boolean): void {
    const next = new Set(this.selectedIds);
    checked ? next.add(id) : next.delete(id);
    this.selectedIds = next;
  }

  private async bulkPause(paused: boolean): Promise<void> {
    this.saving = true;
    try {
      await Promise.all(
        [...this.selectedIds].map((id) =>
          request<Target>(`/api/v1/targets/${id}/${paused ? "pause" : "resume"}`, {
            method: "POST",
          }),
        ),
      );
      this.selectedIds = new Set();
      await this.refresh();
    } catch (error) {
      this.error = error instanceof Error ? error.message : String(error);
    } finally {
      this.saving = false;
    }
  }

  private async bulkDelete(): Promise<void> {
    if (!window.confirm(`Delete ${this.selectedIds.size} selected Targets and their history?`)) return;
    this.saving = true;
    try {
      await Promise.all(
        [...this.selectedIds].map((id) =>
          request<void>(`/api/v1/targets/${id}`, { method: "DELETE" }),
        ),
      );
      this.selectedIds = new Set();
      await this.refresh();
    } catch (error) {
      this.error = error instanceof Error ? error.message : String(error);
    } finally {
      this.saving = false;
    }
  }

  private async deleteResource(kind: "channels" | "secrets", id: string, name: string): Promise<void> {
    if (!window.confirm(`Delete ${name}?`)) return;
    try {
      await request<void>(`/api/v1/${kind}/${id}`, { method: "DELETE" });
      await this.refresh();
    } catch (error) {
      this.error = error instanceof Error ? error.message : String(error);
    }
  }

  protected render() {
    const up = this.targets.filter((target) => target.availability === "up").length;
    const down = this.targets.filter((target) => target.availability === "down").length;
    const pending = this.alerts.filter((alert) => alert.delivery === "pending").length;
    const visibleTargets = this.targets
      .filter((target) =>
        `${target.name} ${target.url}`.toLowerCase().includes(this.search.toLowerCase()),
      )
      .filter((target) =>
        this.statusFilter === "all"
          ? true
          : this.statusFilter === "paused"
            ? target.paused
            : target.availability === this.statusFilter,
      )
      .sort((left, right) =>
        this.sort === "status"
          ? left.availability.localeCompare(right.availability) || left.name.localeCompare(right.name)
          : left.name.localeCompare(right.name),
      );
    return html`
      <main class="shell">
        <header>
          <div class="brand">
            <img src="/favicon.svg" alt="" />
            <div>
              <div class="brand-line"><strong>UpGrid</strong><div class="live"><i class="dot ${this.live ? "on" : ""}"></i>${this.live ? "live" : "connecting"}</div></div>
              <span>Distributed service monitoring</span>
            </div>
          </div>
          <nav aria-label="Primary">
            ${["overview", "alerts", "cluster"].map(
              (section) => html`<a class=${this.activeSection === section ? "active" : ""} href=${`#${section}`} @click=${(event: MouseEvent) => this.navigate(event, section)}>${section[0].toUpperCase()}${section.slice(1)}</a>`,
            )}
          </nav>
          <div class="actions">
            <button class="button secondary icon-button" aria-label=${`Theme: ${this.theme[0].toUpperCase()}${this.theme.slice(1)}`} title=${`Theme: ${this.theme}. Click to switch.`} @click=${this.cycleTheme}><iconify-icon .icon=${themeIcons[this.theme]} aria-hidden="true"></iconify-icon></button>
            <button class="button secondary" @click=${this.createJoinLink} ?disabled=${this.saving}>Add node</button>
          </div>
        </header>
        ${this.error ? html`<div class="notice" role="alert">${this.error}</div>` : nothing}
        ${this.activeSection === "overview"
          ? this.renderOverview(visibleTargets, up, down, pending)
          : this.activeSection === "alerts"
            ? this.renderAlertsPage()
            : this.renderClusterPage()}
      </main>
      <dialog id="target-dialog" aria-labelledby="add-target-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="add-target-title">Add target</h2><p>Start monitoring an HTTP or HTTPS endpoint.</p></div>
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
      ${this.selected ? this.renderDetail(this.selected) : nothing}
      <dialog id="secret-dialog" aria-labelledby="secret-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="secret-title">Add secret</h2><p>The plaintext is encrypted before replication and never returned.</p></div>
        <form @submit=${this.createSecret}>
          <label>Name<input name="name" placeholder="Webhook token" required /></label>
          <label>Value<input name="value" type="password" autocomplete="new-password" required /></label>
          <div class="dialog-actions"><button class="button secondary" type="button" @click=${() => this.closeDialog("secret-dialog")}>Cancel</button><button class="button" type="submit" ?disabled=${this.saving}>Create secret</button></div>
        </form>
      </dialog>
      <dialog id="channel-dialog" aria-labelledby="channel-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="channel-title">Add channel</h2><p>Send transitions through Telegram or a generic webhook.</p></div>
        <form @submit=${this.createChannel}>
          <label>Type<select name="type" @change=${(event: Event) => (this.channelKind = (event.target as HTMLSelectElement).value as "webhook" | "telegram")}><option value="webhook">Webhook</option><option value="telegram">Telegram</option></select></label>
          <label>Name<input name="name" placeholder="On-call" required /></label>
          ${this.channelKind === "webhook"
            ? html`<label>Webhook URL<input name="url" type="url" placeholder="https://hooks.example.com/upgrid" required /></label>`
            : html`<label>Bot token<input name="bot_token" type="password" autocomplete="off" required /></label><label>Chat ID<input name="chat_id" required /></label>`}
          <div class="dialog-actions"><button class="button secondary" type="button" @click=${() => this.closeDialog("channel-dialog")}>Cancel</button><button class="button" type="submit" ?disabled=${this.saving}>Create channel</button></div>
        </form>
      </dialog>
      <dialog id="join-dialog" aria-labelledby="join-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="join-title">Join a node</h2><p>This reusable command contains Cluster credentials. Revoke it when no longer needed.</p></div>
        <div class="join-command">${this.joinCommand}</div>
        <div class="dialog-actions" style="padding: 0 22px 22px"><button class="button secondary" @click=${() => this.closeDialog("join-dialog")}>Close</button><button class="button" @click=${this.copyJoinCommand}>${this.copied ? "Copied" : "Copy command"}</button></div>
      </dialog>
    `;
  }

  private renderOverview(visibleTargets: Target[], up: number, down: number, pending: number) {
    const selectedTargets = this.targets.filter((target) => this.selectedIds.has(target.id));
    const canPauseSelected = selectedTargets.some((target) => !target.paused);
    const canResumeSelected = selectedTargets.some((target) => target.paused);
    return html`
      <section class="heading" id="overview">
        <div><span class="eyebrow">Cluster status</span><h1>Overview</h1></div>
        <button class="button" @click=${this.openTargetDialog}>Add target</button>
      </section>
      <section class="summary" aria-label="Target summary">
        <div class="metric"><span>Targets</span><strong>${this.targets.length}</strong></div>
        <div class="metric"><span>Up</span><strong>${up}</strong></div>
        <div class="metric"><span>Down</span><strong>${down}</strong></div>
        <div class="metric"><span>Pending alerts</span><strong>${pending}</strong></div>
      </section>
      <section class="panel" aria-label="Targets">
        <div class="panel-head"><h2>Targets</h2><span class="meta">${this.targets.length} configured</span></div>
        <div class="toolbar">
          <input aria-label="Search targets" type="search" placeholder="Search name or URL" .value=${this.search} @input=${(event: Event) => (this.search = (event.target as HTMLInputElement).value)} />
          <select aria-label="Filter targets" .value=${this.statusFilter} @change=${(event: Event) => (this.statusFilter = (event.target as HTMLSelectElement).value)}><option value="all">All states</option><option value="up">Up</option><option value="down">Down</option><option value="unknown">Unknown</option><option value="paused">Paused</option></select>
          <select aria-label="Sort targets" .value=${this.sort} @change=${(event: Event) => (this.sort = (event.target as HTMLSelectElement).value)}><option value="name">Sort by name</option><option value="status">Sort by status</option></select>
        </div>
        ${this.selectedIds.size ? html`<div class="bulk"><span class="meta">${this.selectedIds.size} selected</span><div class="bulk-actions"><button class="button secondary icon-button" aria-label="Unselect all" title="Unselect all" @click=${() => (this.selectedIds = new Set())}><iconify-icon .icon=${closeIcon} aria-hidden="true"></iconify-icon></button>${canPauseSelected ? html`<button class="button warning icon-button" aria-label="Pause selected" title="Pause selected" @click=${() => this.bulkPause(true)}><iconify-icon .icon=${pauseIcon} aria-hidden="true"></iconify-icon></button>` : nothing}${canResumeSelected ? html`<button class="button success icon-button" aria-label="Resume selected" title="Resume selected" @click=${() => this.bulkPause(false)}><iconify-icon .icon=${playIcon} aria-hidden="true"></iconify-icon></button>` : nothing}<button class="button danger" @click=${this.bulkDelete}>Delete selected</button></div></div>` : nothing}
        ${visibleTargets.length
          ? visibleTargets.map((target) => this.renderTarget(target))
          : html`<div class="empty">${this.targets.length ? "No Targets match these filters." : "No targets yet. Add the first one to begin monitoring."}</div>`}
      </section>
      <section class="resources" aria-label="Notification configuration">
        <section class="panel">
          <div class="panel-head"><h2>Notification channels</h2><button class="button secondary" @click=${() => this.showDialog("channel-dialog")}>Add channel</button></div>
          ${this.channels.length
            ? this.channels.map((channel) => html`<div class="resource"><div><strong>${channel.name}</strong><code>${channel.destination}</code></div><div class="actions"><span class="badge">${channel.kind}</span><button class="button danger" aria-label=${`Delete channel ${channel.name}`} @click=${() => this.deleteResource("channels", channel.id, channel.name)}>Delete</button></div></div>`)
            : html`<div class="empty">No notification channels.</div>`}
        </section>
        <section class="panel">
          <div class="panel-head"><h2>Secrets</h2><button class="button secondary" @click=${() => this.showDialog("secret-dialog")}>Add secret</button></div>
          ${this.secrets.length
            ? this.secrets.map((secret) => html`<div class="resource"><div><strong>${secret.name}</strong><code>${secret.id}</code></div><div class="actions"><span class="badge">write-only</span><button class="button danger" aria-label=${`Delete secret ${secret.name}`} @click=${() => this.deleteResource("secrets", secret.id, secret.name)}>Delete</button></div></div>`)
            : html`<div class="empty">No reusable Secrets.</div>`}
        </section>
      </section>
    `;
  }

  private renderAlertsPage() {
    return html`
      <section class="heading" id="alerts">
        <div><span class="eyebrow">Delivery history</span><h1>Alerts</h1></div>
      </section>
      <section class="panel" aria-label="Alert history">
        <div class="panel-head"><h2>Availability transitions</h2><span class="meta">${this.alerts.length} events</span></div>
        ${this.alerts.length
          ? this.alerts.map((alert) => html`<div class="resource"><div><strong>${alert.target_name}</strong><code>${new Date(alert.scheduled_at_ms).toLocaleString()}</code></div><span class="badge">${alert.kind} · ${alert.delivery}</span></div>`)
          : html`<div class="empty">No availability transitions.</div>`}
      </section>
    `;
  }

  private renderClusterPage() {
    return html`
      <section class="heading" id="cluster">
        <div><span class="eyebrow">Raft membership</span><h1>Cluster</h1></div>
        <button class="button" @click=${this.createJoinLink}>Add node</button>
      </section>
      <section class="panel" aria-label="Cluster topology">
        <div class="panel-head"><h2>Nodes</h2><span class="meta">${this.cluster?.members.length ?? 0} members</span></div>
        ${this.cluster?.members.map((member) => html`<div class="resource"><div><strong>${member.raft_url}</strong><code>${member.id}</code></div><div class="actions">${member.local ? html`<span class="badge">This node</span>` : nothing}${member.leader ? html`<span class="badge">Leader</span>` : nothing}</div></div>`)}
        ${this.cluster?.members.length ? nothing : html`<div class="empty">Cluster topology unavailable.</div>`}
      </section>
      <section class="panel" aria-label="Join tokens" style="margin-top: 18px">
        <div class="panel-head"><h2>Join Tokens</h2><span class="meta">${this.joinTokens.length} stored</span></div>
        ${this.joinTokens.length
          ? this.joinTokens.map((token) => html`
              <div class="resource">
                <div><strong>${token.id.slice(0, 12)}…</strong><code>Expires ${new Date(token.expires_at_ms).toLocaleString()}</code></div>
                <button class="button danger" aria-label=${`Revoke Join Token ${token.id.slice(0, 12)}`} @click=${() => this.revokeJoinToken(token)}>Revoke</button>
              </div>
            `)
          : html`<div class="empty">No Join Tokens.</div>`}
      </section>
    `;
  }

  private renderTarget(target: Target) {
    const latest = target.latest_evaluation;
    const history = target.history.slice(0, 16).reverse();
    const maxLatency = Math.max(1, ...history.map((item) => item.latency_ms));
    return html`
      <div class="target-wrap">
        <input class="select-target" type="checkbox" aria-label=${`Select ${target.name}`} .checked=${this.selectedIds.has(target.id)} @change=${(event: Event) => this.toggleSelected(target.id, (event.target as HTMLInputElement).checked)} />
        <button class="target" aria-label=${target.name} @click=${() => this.openTarget(target)}>
          <i class="state ${target.paused ? "paused" : target.availability}" aria-label=${target.paused ? "paused" : target.availability}></i>
          <div>
            <h3>${target.name}</h3>
            <div class="meta">${target.paused ? "Paused · " : ""}${target.method} · ${target.url} · every ${target.interval_seconds}s</div>
          </div>
          <div class="target-side">
            ${history.length
              ? html`<div class="mini-chart" aria-hidden="true">${history.map((item) => html`<i class="mini-bar ${item.succeeded ? "up" : "down"}" style=${`height: ${Math.max(12, item.latency_ms / maxLatency * 100)}%`}></i>`)}</div>`
              : nothing}
            <div class="latency">
              <strong>${latest ? `${latest.latency_ms} ms` : "—"}</strong>
              <span>${latest ? (latest.status_code ?? "network error") : "waiting"}</span>
            </div>
          </div>
        </button>
      </div>
    `;
  }

  private renderDetail(target: Target) {
    const statuses = target.accepted_statuses
      .map((range) => (range.start === range.end ? range.start : `${range.start}-${range.end}`))
      .join(",");
    const history = target.history.slice(0, 30).reverse();
    const maxLatency = Math.max(1, ...history.map((item) => item.latency_ms));
    const chartTime = (timestamp: number) => new Date(timestamp).toLocaleString(undefined, {
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });
    const chartLatency = (latency: number) => latency >= 1_000
      ? `${(latency / 1_000).toFixed(latency >= 10_000 ? 0 : 1)} s`
      : `${Math.round(latency)} ms`;
    return html`
      <dialog id="detail-dialog" aria-labelledby="target-detail-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head">
          <h2 id="target-detail-title">Target details</h2>
          <button class="button secondary icon-button dialog-close" type="button" aria-label="Close target details" title="Close" @click=${this.closeDetailDialog}><iconify-icon .icon=${closeIcon} aria-hidden="true"></iconify-icon></button>
        </div>
        <form @submit=${this.updateTarget} @input=${this.updateDetailDirty}>
          <label>Name<input name="name" .value=${target.name} required /></label>
          <label>URL<input name="url" type="url" .value=${target.url} required /></label>
          <div class="row">
            <label>Method<input name="method" .value=${target.method} required /></label>
            <label>Expected statuses<input name="statuses" .value=${statuses} required /></label>
          </div>
          <div class="row">
            <label>Interval (seconds)<input name="interval" type="number" min="1" .value=${String(target.interval_seconds)} required /></label>
            <label>Timeout (seconds)<input name="timeout" type="number" min="1" .value=${String(target.timeout_seconds)} required /></label>
          </div>
          <div class="row">
            <label>Failures before Down<input name="failures" type="number" min="1" .value=${String(target.failure_threshold)} required /></label>
            <label>Maximum redirects<input name="max_redirects" type="number" min="0" .value=${String(target.max_redirects)} ?disabled=${!target.follow_redirects} required /></label>
          </div>
          <label>Body must contain<input name="body_contains" .value=${target.body_contains ?? ""} /></label>
          <div class="row">
            <label class="check"><input name="follow_redirects" type="checkbox" .checked=${target.follow_redirects} @change=${this.toggleMaxRedirects} />Follow redirects</label>
            <label class="check"><input name="skip_tls_verification" type="checkbox" .checked=${target.skip_tls_verification} />Skip TLS verification</label>
          </div>
          <div class="dialog-actions">
            <div class="danger-actions">
              <button class="button danger icon-button" type="button" aria-label="Delete target" title="Delete target" @click=${this.deleteTarget}><iconify-icon .icon=${deleteIcon} aria-hidden="true"></iconify-icon></button>
              <button class=${`button ${target.paused ? "success" : "warning"} icon-button`} type="button" aria-label=${target.paused ? "Resume evaluations" : "Pause evaluations"} title=${target.paused ? "Resume evaluations" : "Pause evaluations"} @click=${() => this.setPaused(!target.paused)}><iconify-icon .icon=${target.paused ? playIcon : pauseIcon} aria-hidden="true"></iconify-icon></button>
            </div>
            <button class="button" type="submit" aria-busy=${this.saving ? "true" : "false"} ?disabled=${this.saving || !this.detailDirty}>Save changes</button>
          </div>
        </form>
        <section class="history">
          <div class="history-head"><h3>Evaluation history</h3>${history.length ? html`<span class="meta">Latest ${history.length}</span>` : nothing}</div>
          ${history.length
            ? html`
                <div class="chart-plot">
                  <div class="chart-scale" aria-hidden="true"><span>${chartLatency(maxLatency)}</span><span>${chartLatency(maxLatency / 2)}</span><span>0 ms</span></div>
                  <div class="history-chart" role="list" aria-label=${`Recent evaluation latency, 0 to ${chartLatency(maxLatency)}`}>
                    ${history.map((item) => {
                      const result = item.succeeded ? "Passed" : "Failed";
                      const status = item.status_code === null ? "network error" : `HTTP ${item.status_code}`;
                      const label = `${result} at ${new Date(item.recorded_at_ms).toLocaleString()}: ${item.latency_ms} ms, ${status}`;
                      return html`<span class="history-bar ${item.succeeded ? "up" : "down"}" role="listitem" aria-label=${label} title=${label} style=${`height: ${Math.max(8, item.latency_ms / maxLatency * 100)}%`}></span>`;
                    })}
                  </div>
                </div>
                <div class="chart-axis"><span>${chartTime(history[0].recorded_at_ms)}</span><span>${chartTime(history.at(-1)!.recorded_at_ms)}</span></div>
                <div class="chart-legend"><span><i class="up"></i>Passed</span><span><i class="down"></i>Failed</span><span>Height = latency</span></div>
              `
            : html`<p class="meta">No evaluations recorded yet.</p>`}
        </section>
      </dialog>
    `;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "upgrid-app": UpgridApp;
  }
}
