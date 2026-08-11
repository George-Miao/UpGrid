import { type Channel, type JoinLink, type JoinToken, type Secret, type Setup, type Target, type TargetInput, request } from "./api.ts";
import { AppState } from "./app-state.ts";
import { channelInput, targetInput } from "./resource-input.ts";

export class AppController extends AppState {
  protected async createTarget(event: SubmitEvent): Promise<void> {
    event.preventDefault();
    const form = event.currentTarget as HTMLFormElement;
    const fields = new FormData(form);
    const input = targetInput(fields, fields.getAll("channel_id").map(String), fields.get("use_default_channels") === "on");
    this.saving = true;
    try {
      await request<Target>("/api/v1/targets", { method: "POST", body: JSON.stringify(input) });
      form.reset();
      this.closeTargetDialog();
      await this.refresh();
    } catch (error) {
      this.error = error instanceof Error ? error.message : String(error);
    } finally {
      this.saving = false;
    }
  }

  protected async updateTarget(event: SubmitEvent): Promise<void> {
    event.preventDefault();
    if (!this.selected) return;
    const fields = new FormData(event.currentTarget as HTMLFormElement);
    let path = `/api/v1/nodes/${this.selected.id}`;
    let input: TargetInput | { name: string } = { name: String(fields.get("name")) };
    if (this.selected.kind === "http") {
      const follow = fields.get("follow_redirects") === "on";
      path = `/api/v1/targets/${this.selected.id}`;
      input = {
        name: String(fields.get("name")),
        url: String(fields.get("url")),
        method: String(fields.get("method")),
        accepted_statuses: String(fields.get("statuses"))
          .split(",")
          .map((part) => {
            const [start, end] = part.trim().split("-").map(Number);
            return { start, end: end || start };
          }),
        follow_redirects: follow,
        max_redirects: follow ? Number(fields.get("max_redirects")) : 0,
        interval_seconds: Number(fields.get("interval")),
        timeout_seconds: Number(fields.get("timeout")),
        failure_threshold: Number(fields.get("failures")),
        headers: Object.fromEntries(Object.entries(this.selected.headers).map(([name, value]) => [name, value.kind === "literal" ? value.value : { secret_id: value.secret_id }])),
        body: this.selected.body?.kind === "literal" ? this.selected.body.value : this.selected.body ? { secret_id: this.selected.body.secret_id } : null,
        body_contains: String(fields.get("body_contains")) || null,
        skip_tls_verification: fields.get("skip_tls_verification") === "on",
        notification_channel_ids: fields.getAll("channel_id").map(String),
        use_default_channels: fields.get("use_default_channels") === "on",
      };
    }
    this.saving = true;
    try {
      await request<unknown>(path, { method: "PUT", body: JSON.stringify(input) });
      this.closeDetailDialog();
      await this.refresh();
    } catch (error) {
      this.error = error instanceof Error ? error.message : String(error);
    } finally {
      this.saving = false;
    }
  }

  protected async deleteTarget(): Promise<void> {
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

  protected async setPaused(paused: boolean): Promise<void> {
    if (!this.selected) return;
    this.saving = true;
    try {
      await request<Target>(`/api/v1/targets/${this.selected.id}/${paused ? "pause" : "resume"}`, { method: "POST" });
      this.closeDetailDialog();
      await this.refresh();
    } catch (error) {
      this.error = error instanceof Error ? error.message : String(error);
    } finally {
      this.saving = false;
    }
  }

  protected async createSecret(event: SubmitEvent): Promise<void> {
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

  protected async createChannel(event: SubmitEvent): Promise<void> {
    event.preventDefault();
    const form = event.currentTarget as HTMLFormElement;
    const fields = new FormData(form);
    const editing = this.editingChannel;
    const body = channelInput(fields, this.channelKind, editing !== undefined);
    this.saving = true;
    try {
      await request<Channel>(editing ? `/api/v1/channels/${editing.id}` : "/api/v1/channels", {
        method: editing ? "PUT" : "POST",
        body: JSON.stringify(body),
      });
      form.reset();
      this.editingChannel = undefined;
      this.channelKind = "webhook";
      this.channelTestMessage = "";
      this.closeDialog("channel-dialog");
      await this.refresh();
    } catch (error) {
      this.error = error instanceof Error ? error.message : String(error);
    } finally {
      this.saving = false;
    }
  }

  protected openChannelDialog(channel?: Channel): void {
    this.editingChannel = channel;
    this.channelKind = channel?.kind ?? "webhook";
    this.channelTestMessage = "";
    this.showDialog("channel-dialog");
  }

  protected async setChannelDefault(channel: Channel, isDefault: boolean): Promise<void> {
    try {
      await request<Channel>(`/api/v1/channels/${channel.id}/default`, {
        method: "PUT",
        body: JSON.stringify({ default: isDefault }),
      });
      await this.refresh();
    } catch (error) {
      this.error = error instanceof Error ? error.message : String(error);
    }
  }

  protected async testChannel(event: MouseEvent): Promise<void> {
    const form = (event.currentTarget as HTMLButtonElement).form;
    if (!form) return;
    const required = [...form.querySelectorAll<HTMLInputElement>("[data-test-required]")];
    if (!required.every((field) => field.reportValidity())) return;
    this.testingChannel = true;
    this.channelTestMessage = "";
    try {
      const body = channelInput(new FormData(form), this.channelKind);
      await request<void>("/api/v1/channels/test", { method: "POST", body: JSON.stringify(body) });
      this.channelTestMessage = "Test sent";
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.channelTestMessage = `Test failed: ${message}`;
    } finally {
      this.testingChannel = false;
    }
  }

  protected openTokenDialog(): void {
    this.unlimitedUses = false;
    this.showDialog("token-config-dialog");
  }

  protected async createJoinToken(event: SubmitEvent): Promise<void> {
    event.preventDefault();
    const fields = new FormData(event.currentTarget as HTMLFormElement);
    this.saving = true;
    try {
      const link = await request<JoinLink>("/api/v1/join-tokens", {
        method: "POST",
        body: JSON.stringify({
          expires_in_seconds: Number(fields.get("expiration_days")) * 86_400,
          max_uses: this.unlimitedUses ? null : Number(fields.get("max_uses")),
        }),
      });
      this.joinCommand = `upgrid --join '${link.url}'`;
      this.copied = false;
      await this.refresh();
      this.closeDialog("token-config-dialog");
      this.showDialog("join-dialog");
    } catch (error) {
      this.error = error instanceof Error ? error.message : String(error);
    } finally {
      this.saving = false;
    }
  }

  protected async setupChanged(event: CustomEvent<Setup>): Promise<void> {
    const setup = event.detail;
    this.setup = setup;
    this.setupMode = setup.setup;
    window.history.replaceState(null, "", setup.path);
    if (setup.setup) {
      if (setup.cluster_ready) {
        await this.refresh();
        this.connectEvents();
      }
      return;
    }
    this.activeSection = "overview";
    await this.refresh();
    this.connectEvents();
  }

  protected async revokeJoinToken(token: JoinToken): Promise<void> {
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

  protected async copyJoinCommand(): Promise<void> {
    let copied = false;
    try {
      await navigator.clipboard.writeText(this.joinCommand);
      copied = true;
    } catch {
      const field = Object.assign(document.createElement("textarea"), { value: this.joinCommand });
      field.style.cssText = "position: fixed; opacity: 0";
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

  protected toggleSelected(id: string, checked: boolean): void {
    const next = new Set(this.selectedIds);
    checked ? next.add(id) : next.delete(id);
    this.selectedIds = next;
  }

  protected async bulkPause(paused: boolean): Promise<void> {
    this.saving = true;
    try {
      await Promise.all([...this.selectedIds].map((id) => request<Target>(`/api/v1/targets/${id}/${paused ? "pause" : "resume"}`, { method: "POST" })));
      this.selectedIds = new Set();
      await this.refresh();
    } catch (error) {
      this.error = error instanceof Error ? error.message : String(error);
    } finally {
      this.saving = false;
    }
  }

  protected async bulkDelete(): Promise<void> {
    if (!window.confirm(`Delete ${this.selectedIds.size} selected Targets and their history?`)) return;
    this.saving = true;
    try {
      await Promise.all([...this.selectedIds].map((id) => request<void>(`/api/v1/targets/${id}`, { method: "DELETE" })));
      this.selectedIds = new Set();
      await this.refresh();
    } catch (error) {
      this.error = error instanceof Error ? error.message : String(error);
    } finally {
      this.saving = false;
    }
  }

  protected async deleteResource(kind: "channels" | "secrets", id: string, name: string): Promise<void> {
    if (!window.confirm(`Delete ${name}?`)) return;
    try {
      await request<void>(`/api/v1/${kind}/${id}`, { method: "DELETE" });
      await this.refresh();
    } catch (error) {
      this.error = error instanceof Error ? error.message : String(error);
    }
  }
}
