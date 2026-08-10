import { LitElement } from "lit";
import { state } from "lit/decorators.js";
import darkIcon from "@iconify-icons/lucide/moon";
import systemIcon from "@iconify-icons/lucide/palette";
import brightIcon from "@iconify-icons/lucide/sun";
import { type Alert, type Channel, type Cluster, type JoinToken, type Secret, type Setup, type Target, type Transition, request } from "./api.ts";

const themes = ["system", "dark", "bright"] as const;
type Theme = (typeof themes)[number];
export const themeIcons = { system: systemIcon, dark: darkIcon, bright: brightIcon };

export const sectionPaths = {
  overview: "/",
  alerts: "/alerts",
  cluster: "/cluster",
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
  @state() protected channels: Channel[] = [];
  @state() protected alerts: Alert[] = [];
  @state() protected transitions: Transition[] = [];
  @state() protected secrets: Secret[] = [];
  @state() protected cluster?: Cluster;
  @state() protected joinTokens: JoinToken[] = [];
  @state() protected error = "";
  @state() protected live = false;
  @state() protected saving = false;
  @state() protected selected?: Target;
  @state() protected channelKind: "webhook" | "telegram" = "webhook";
  @state() protected channelTestMessage = "";
  @state() protected testingChannel = false;
  @state() protected joinCommand = "";
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
    } catch (error) {
      this.error = error instanceof Error ? error.message : String(error);
    }
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
      [this.targets, this.channels, this.alerts, this.transitions, this.secrets, this.cluster, this.joinTokens] = await Promise.all([
        request<Target[]>("/api/v1/targets"),
        request<Channel[]>("/api/v1/channels"),
        request<Alert[]>("/api/v1/alerts"),
        request<Transition[]>("/api/v1/transitions"),
        request<Secret[]>("/api/v1/secrets"),
        request<Cluster>("/api/v1/cluster"),
        request<JoinToken[]>("/api/v1/join-tokens"),
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
    void this.updateComplete.then(() => {
      const dialog = this.renderRoot.querySelector<HTMLDialogElement>("#detail-dialog");
      const form = dialog?.querySelector<HTMLFormElement>("form");
      if (form) this.detailInitialState = this.detailFormState(form);
      dialog?.showModal();
    });
  }

  protected closeDetailDialog(): void {
    this.renderRoot.querySelector<HTMLDialogElement>("#detail-dialog")?.close();
    this.detailDirty = false;
    this.detailInitialState = "";
    this.selected = undefined;
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
