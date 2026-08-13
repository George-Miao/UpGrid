import { LitElement } from "lit";
import { state } from "lit/decorators.js";
import darkIcon from "@iconify-icons/lucide/moon";
import systemIcon from "@iconify-icons/lucide/palette";
import brightIcon from "@iconify-icons/lucide/sun";
import { ApiRequestError, type Alert, type ApiToken, type Channel, type Cluster, type HistoryPage, type Identity, type JoinToken, type Secret, type Session, type Setup, type Target, type TrashedTarget, type Transition, request } from "./api.ts";

const themes = ["system", "dark", "bright"] as const;
type Theme = (typeof themes)[number];
export const themeIcons = { system: systemIcon, dark: darkIcon, bright: brightIcon };

export const sectionPaths = {
  overview: "/",
  alerts: "/alerts",
  cluster: "/cluster",
  trash: "/trash",
  changePassword: "/admin/change-password",
  users: "/admin/users",
  apiTokens: "/admin/api-tokens",
} as const;
export type Section = keyof typeof sectionPaths;

export function serviceHealth(targets: Target[], connected: boolean) {
  if (!connected) return { tone: "pending", label: "connecting" };
  const monitored = targets.filter((target) => !target.paused);
  if (!monitored.length) return { tone: "pending", label: "ready" };
  const affected = monitored.filter((target) => target.availability === "down" || target.consecutive_failures > 0).length;
  if (!affected) return { tone: "up", label: "up" };
  return affected === monitored.length ? { tone: "down", label: "down" } : { tone: "degraded", label: "partially down" };
}

function sectionFromPath(): Section {
  return (Object.entries(sectionPaths).find(([, path]) => path === window.location.pathname)?.[0] ?? "overview") as Section;
}

function storedTheme(): Theme {
  const theme = localStorage.getItem("upgrid-theme");
  return themes.includes(theme as Theme) ? (theme as Theme) : "system";
}

export class AppState extends LitElement {
  @state() protected targets: Target[] = [];
  @state() protected trashedTargets: TrashedTarget[] = [];
  @state() protected channels: Channel[] = [];
  @state() protected alerts: Alert[] = [];
  @state() protected transitions: Transition[] = [];
  @state() protected secrets: Secret[] = [];
  @state() protected cluster?: Cluster;
  @state() protected joinTokens: JoinToken[] = [];
  @state() protected identities: Identity[] = [];
  @state() protected apiTokens: ApiToken[] = [];
  @state() protected session?: Session;
  @state() protected authReady = false;
  @state() protected newApiToken = "";
  @state() protected error = "";
  @state() protected live = false;
  @state() protected saving = false;
  @state() protected selected?: Target;
  @state() protected targetHistory?: HistoryPage;
  @state() protected historyLoading = false;
  @state() protected channelKind: "webhook" | "telegram" | "smtp" = "webhook";
  @state() protected editingChannel?: Channel;
  @state() protected channelTestMessage = "";
  @state() protected testingChannel = false;
  @state() protected joinCommand = "";
  @state() protected alertSearch = "";
  @state() protected alertDeliveryFilter: "all" | Alert["delivery"] = "all";
  @state() protected alertKindFilter: "all" | Alert["kind"] = "all";
  @state() protected alertAcknowledgedFilter: "all" | "yes" | "no" = "all";
  @state() protected search = "";
  @state() protected statusFilter = "all";
  @state() protected sort = "name";
  @state() protected selectedIds = new Set<string>();
  @state() protected activeSection: Section = sectionFromPath();
  @state() protected copied = false;
  @state() protected setupMode = false;
  @state() protected setup?: Setup;
  @state() protected warningDismissed = sessionStorage.getItem("upgrid-warning-dismissed") === "1";
  @state() protected unlimitedUses = false;
  @state() protected theme = storedTheme();
  @state() protected detailDirty = false;
  private events?: EventSource;
  private detailInitialState = "";
  private readonly systemTheme = matchMedia("(prefers-color-scheme: light)");
  private readonly systemThemeChanged = () => {
    if (this.theme === "system") this.applyTheme();
  };
  private readonly routeChanged = () => {
    if (this.setupMode && this.setup) {
      window.history.replaceState(null, "", this.setup.path);
      return;
    }
    this.activeSection = sectionFromPath();
  };

  connectedCallback(): void {
    super.connectedCallback();
    this.applyTheme();
    this.systemTheme.addEventListener("change", this.systemThemeChanged);
    window.addEventListener("popstate", this.routeChanged);
    void this.start();
  }

  disconnectedCallback(): void {
    this.systemTheme.removeEventListener("change", this.systemThemeChanged);
    window.removeEventListener("popstate", this.routeChanged);
    this.events?.close();
    super.disconnectedCallback();
  }

  private async start(): Promise<void> {
    try {
      const setup = await request<Setup>("/api/v1/setup");
      if (setup.cluster_ready) {
        this.session = await request<Session>("/api/v1/auth/session");
      }
      await this.activate(setup);
    } catch (error) {
      if (!(error instanceof ApiRequestError) || error.status !== 401) {
        this.error = error instanceof Error ? error.message : String(error);
      }
    }
    this.authReady = true;
  }

  private async activate(setup: Setup): Promise<void> {
    this.setup = setup;
    this.setupMode = setup.setup;
    if (this.setupMode) {
      window.history.replaceState(null, "", setup.path);
      if (setup.cluster_ready) {
        await this.refresh();
        this.connectEvents();
      } else {
        this.live = true;
      }
      return;
    }
    await this.refresh();
    this.connectEvents();
  }

  protected async login(event: SubmitEvent): Promise<void> {
    event.preventDefault();
    const fields = new FormData(event.currentTarget as HTMLFormElement);
    this.saving = true;
    this.error = "";
    try {
      this.session = await request<Session>("/api/v1/auth/login", {
        method: "POST",
        body: JSON.stringify({
          username: String(fields.get("username") ?? ""),
          password: String(fields.get("password") ?? ""),
        }),
      });
      await this.activate(await request<Setup>("/api/v1/setup"));
    } catch (error) {
      this.error = error instanceof Error ? error.message : String(error);
    } finally {
      this.saving = false;
    }
  }

  protected async logout(): Promise<void> {
    await request("/api/v1/auth/logout", { method: "POST" });
    this.events?.close();
    this.session = undefined;
    this.live = false;
    this.setupMode = false;
    window.history.replaceState(null, "", "/");
  }

  protected connectEvents(): void {
    this.events?.close();
    this.events = new EventSource("/api/v1/events");
    this.events.addEventListener("state", () => void this.refresh());
    this.events.onopen = () => (this.live = true);
    this.events.onerror = () => (this.live = false);
  }

  protected applyTheme(): void {
    const resolved = this.theme === "system" ? (this.systemTheme.matches ? "bright" : "dark") : this.theme;
    this.dataset.theme = resolved;
    document.querySelector<HTMLMetaElement>('meta[name="theme-color"]')?.setAttribute("content", resolved === "bright" ? "#f4f8f6" : "#0b1110");
  }

  protected cycleTheme(): void {
    this.theme = themes[(themes.indexOf(this.theme) + 1) % themes.length];
    localStorage.setItem("upgrid-theme", this.theme);
    this.applyTheme();
  }

  protected dismissWarning(): void {
    sessionStorage.setItem("upgrid-warning-dismissed", "1");
    this.warningDismissed = true;
  }

  protected async refresh(): Promise<void> {
    try {
      [this.targets, this.trashedTargets, this.channels, this.alerts, this.transitions, this.secrets, this.cluster, this.joinTokens, this.identities, this.apiTokens] = await Promise.all([
        request<Target[]>("/api/v1/targets"),
        request<TrashedTarget[]>("/api/v1/trash/targets"),
        request<Channel[]>("/api/v1/channels"),
        request<Alert[]>("/api/v1/alerts"),
        request<Transition[]>("/api/v1/transitions"),
        request<Secret[]>("/api/v1/secrets"),
        request<Cluster>("/api/v1/cluster"),
        request<JoinToken[]>("/api/v1/join-tokens"),
        request<Identity[]>("/api/v1/identities"),
        request<ApiToken[]>("/api/v1/api-tokens"),
      ]);
      this.error = "";
    } catch (error) {
      this.error = error instanceof Error ? error.message : String(error);
    }
  }

  protected openTargetDialog(): void {
    this.renderRoot.querySelector<HTMLDialogElement>("#target-dialog")?.showModal();
  }

  protected closeTargetDialog(): void {
    this.renderRoot.querySelector<HTMLDialogElement>("#target-dialog")?.close();
  }

  protected openTarget(target: Target): void {
    this.detailDirty = false;
    this.selected = target;
    this.targetHistory = undefined;
    this.historyLoading = true;
    void this.loadTargetHistory(target.id);
    void this.updateComplete.then(() => {
      const dialog = this.renderRoot.querySelector<HTMLDialogElement>("#detail-dialog");
      const form = dialog?.querySelector<HTMLFormElement>("form");
      if (form) this.detailInitialState = this.detailFormState(form);
      dialog?.showModal();
    });
  }

  private async loadTargetHistory(targetId: string): Promise<void> {
    try {
      const history = await request<HistoryPage>(`/api/v1/targets/${targetId}/history?limit=720`);
      if (this.selected?.id === targetId) this.targetHistory = history;
    } catch (error) {
      if (this.selected?.id === targetId) this.error = error instanceof Error ? error.message : String(error);
    } finally {
      if (this.selected?.id === targetId) this.historyLoading = false;
    }
  }

  protected closeDetailDialog(): void {
    this.renderRoot.querySelector<HTMLDialogElement>("#detail-dialog")?.close();
    this.detailDirty = false;
    this.detailInitialState = "";
    this.selected = undefined;
    this.targetHistory = undefined;
    this.historyLoading = false;
  }

  protected showDialog(id: string): void {
    this.renderRoot.querySelector<HTMLDialogElement>(`#${id}`)?.showModal();
  }

  protected dismissOnBackdrop(event: MouseEvent): void {
    const dialog = event.currentTarget as HTMLDialogElement;
    if (event.target !== dialog) return;
    dialog.close();
    if (dialog.id === "detail-dialog") this.closeDetailDialog();
  }

  protected navigate(event: MouseEvent, section: Section): void {
    event.preventDefault();
    this.activeSection = section;
    window.history.pushState(null, "", sectionPaths[section]);
    this.renderRoot.querySelector(".account-menu")?.removeAttribute("open");
  }

  protected closeDialog(id: string): void {
    this.renderRoot.querySelector<HTMLDialogElement>(`#${id}`)?.close();
  }

  protected toggleMaxRedirects(event: Event): void {
    const follow = event.currentTarget as HTMLInputElement;
    const maximum = follow.form?.elements.namedItem("max_redirects") as HTMLInputElement | null;
    if (maximum) maximum.disabled = !follow.checked;
    if (follow.form) this.compareDetailForm(follow.form);
  }

  private detailFormState(form: HTMLFormElement): string {
    return JSON.stringify([...new FormData(form).entries()]);
  }

  private compareDetailForm(form: HTMLFormElement): void {
    this.detailDirty = this.detailFormState(form) !== this.detailInitialState;
  }

  protected updateDetailDirty(event: Event): void {
    this.compareDetailForm(event.currentTarget as HTMLFormElement);
  }
}
