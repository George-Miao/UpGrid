import { LitElement, css, html, nothing } from "lit";
import { customElement, property, state } from "lit/decorators.js";
import { type Channel, request } from "@/app/api.ts";
import { renderFormSubmit } from "@/component/form-submit.ts";
import "@/component/switch.ts";
import type { ToggleSwitch } from "@/component/switch.ts";
import { helpTooltipStyles, renderHelpTooltip } from "@/component/tooltip.ts";
import { type ChannelKind, channelInput } from "@/util/resource-input.ts";

@customElement("upgrid-notification-channel-form")
export class NotificationChannelForm extends LitElement {
  @property({ attribute: false }) channel?: Channel;
  @property({ type: Boolean, attribute: "default-channel" }) defaultChannel = false;
  @property({ attribute: "submit-label" }) submitLabel = "Create channel";
  @property({ attribute: "cancel-label" }) cancelLabel = "Cancel";
  @property({ type: Boolean }) disabled = false;

  @state() private kind: ChannelKind = "webhook";
  @state() private isDefault = false;
  @state() private saving = false;
  @state() private testing = false;
  @state() private message = "";
  @state() private messageIsError = false;

  static styles = css`
    ${helpTooltipStyles}
    :host { display: block; }
    *, *::before, *::after { box-sizing: border-box; }
    form { display: grid; gap: 13px; padding: 20px 22px 22px; }
    label { display: grid; gap: 6px; color: var(--muted); font-size: 14px; }
    input, select { width: 100%; min-height: 44px; border: 1px solid var(--line); border-radius: 9px; outline: 0; background: var(--input-bg); color: var(--text); padding: 9px 10px; font: inherit; font-size: 16px; transition: background-color 160ms ease, border-color 160ms ease, color 160ms ease, opacity 160ms ease; }
    input:focus, select:focus { border-color: var(--focus); }
    button:focus-visible, input:focus-visible, select:focus-visible { outline: 2px solid var(--green); outline-offset: 2px; }
    button, input[type="checkbox"], select { cursor: pointer; user-select: none; }
    button.button:disabled { border-color: var(--disabled-border); background: var(--disabled-bg); color: var(--disabled-text); cursor: not-allowed; opacity: 1; }
    input:disabled, select:disabled { cursor: not-allowed; opacity: .65; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 11px; }
    .button { min-height: 44px; border: 1px solid var(--button-border); border-radius: 9px; background: var(--button-bg); color: var(--button-text); padding: 9px 13px; white-space: nowrap; cursor: pointer; font: inherit; transition: background-color 160ms ease, border-color 160ms ease, color 160ms ease, opacity 160ms ease, transform 120ms ease; }
    .button:hover { border-color: var(--button-hover-border); }
    .button:active { transform: translateY(1px); }
    .secondary { border-color: var(--line); background: transparent; color: var(--muted); }
    .form-field { display: grid; gap: 6px; }
    .title-with-help { display: flex; align-items: center; gap: 6px; color: var(--muted); font-size: 14px; }
    .channel-test-message { margin: 5px 0 0; border: 1px solid var(--line); border-radius: 9px; background: var(--panel-2); color: var(--green); padding: 10px 12px; overflow-wrap: anywhere; white-space: normal; }
    .channel-test-message.error { border-color: var(--notice-border); background: var(--notice-bg); color: var(--notice-text); }
    .dialog-actions { display: flex; justify-content: flex-end; gap: 8px; margin-top: 5px; }
    @media (max-width: 620px) { .row { grid-template-columns: 1fr; } .dialog-actions { flex-wrap: wrap; } }
    @media (prefers-reduced-motion: reduce) { input, select, .button { transition-duration: 0s; } }
  `;

  protected willUpdate(changed: Map<PropertyKey, unknown>): void {
    if (changed.has("channel")) {
      this.kind = this.channel?.kind ?? "webhook";
      this.message = "";
      this.messageIsError = false;
    }
    if (changed.has("channel") || changed.has("defaultChannel")) {
      this.isDefault = this.channel?.default ?? this.defaultChannel;
    }
  }

  private changeKind(event: Event): void {
    this.kind = (event.target as HTMLSelectElement).value as ChannelKind;
    this.message = "";
    this.messageIsError = false;
  }

  private formChanged(): void {
    if (!this.messageIsError) return;
    this.message = "";
    this.messageIsError = false;
  }

  private async save(event: SubmitEvent): Promise<void> {
    event.preventDefault();
    const form = event.currentTarget as HTMLFormElement;
    const editing = this.channel !== undefined;
    this.saving = true;
    this.message = "";
    try {
      const channel = await request<Channel>(editing ? `/api/v1/channels/${this.channel?.id}` : "/api/v1/channels", {
        method: editing ? "PUT" : "POST",
        body: JSON.stringify(channelInput(new FormData(form), this.kind, editing)),
      });
      form.reset();
      this.kind = this.channel?.kind ?? "webhook";
      this.dispatchEvent(new CustomEvent("channel-saved", { detail: channel, bubbles: true, composed: true }));
    } catch (error) {
      this.showFailure("Save failed", error);
    } finally {
      this.saving = false;
    }
  }

  private cancel(): void {
    this.dispatchEvent(new CustomEvent("channel-cancel", { bubbles: true, composed: true }));
  }

  private async testConnection(event: MouseEvent): Promise<void> {
    const form = (event.currentTarget as HTMLButtonElement).form;
    if (!form) return;
    const required = [...form.querySelectorAll<HTMLInputElement>("[data-test-required]")];
    if (!required.every((field) => field.reportValidity())) return;
    const editing = this.channel !== undefined;
    const input = channelInput(new FormData(form), this.kind, editing);
    const body = this.channel ? { ...input, channel_id: this.channel.id } : input;
    this.testing = true;
    this.message = "";
    try {
      await request<void>("/api/v1/channels/test", { method: "POST", body: JSON.stringify(body) });
      this.message = "Test sent";
      this.messageIsError = false;
    } catch (error) {
      this.showFailure("Test failed", error);
    } finally {
      this.testing = false;
    }
  }

  private showFailure(prefix: string, error: unknown): void {
    this.message = `${prefix}: ${error instanceof Error ? error.message : String(error)}`;
    this.messageIsError = true;
  }

  protected render() {
    const busy = this.disabled || this.saving || this.testing;
    return html`<form @submit=${this.save} @input=${this.formChanged}>
      <label>Type<select name="type" .value=${this.kind} ?disabled=${this.channel !== undefined || busy} @change=${this.changeKind}><option value="webhook">Webhook</option><option value="telegram">Telegram</option><option value="smtp">SMTP email</option></select></label>
      <label>Name<input name="name" placeholder="On-call" .value=${this.channel?.name ?? ""} required /></label>
      ${this.renderFields()}
      <upgrid-toggle-switch name="default" .checked=${this.isDefault} ?disabled=${busy} @change=${(event: Event) => (this.isDefault = (event.currentTarget as ToggleSwitch).checked)}>Default channel</upgrid-toggle-switch>
      ${this.message ? html`<p class=${`channel-test-message${this.messageIsError ? " error" : ""}`} role="status">${this.message}</p>` : nothing}
      <div class="dialog-actions"><button class="button secondary" type="button" ?disabled=${busy} @click=${this.cancel}>${this.cancelLabel}</button><button class="button secondary" type="button" aria-busy=${this.testing} ?disabled=${busy} @click=${this.testConnection}>${this.testing ? "Sending..." : "Send test"}</button>${renderFormSubmit({ label: this.saving ? "Saving..." : this.submitLabel, busy: this.saving, blocked: this.disabled || this.testing, error: this.messageIsError ? this.message : "", baselineKey: this.channel?.id ?? "new", blockedMessage: this.testing ? "Channel test is in progress" : "Channel form is unavailable", trackChanges: this.channel !== undefined })}</div>
    </form>`;
  }

  private renderFields() {
    if (this.kind === "webhook") {
      return html`<label>Webhook URL<input name="url" type="url" placeholder="https://hooks.example.com/upgrid" .value=${this.channel?.destination ?? ""} data-test-required required /></label>`;
    }
    if (this.kind === "telegram") {
      return html`
        <label><span class="title-with-help">Bot token ${renderHelpTooltip("telegram-token-help", "About Telegram bot token storage", this.channel ? "Get a replacement token from Telegram's @BotFather. Leave this blank to keep the automatically managed secret, or enter the replacement token." : "Get a bot token from Telegram's @BotFather.")}</span><input name="bot_token" type="password" autocomplete="off" placeholder=${this.channel ? "Leave blank to keep current token" : ""} data-test-required ?required=${this.channel === undefined} /></label>
        <label>Chat ID<input name="chat_id" .value=${this.channel?.destination ?? ""} data-test-required required /></label>
      `;
    }
    return html`
      <label>SMTP host<input name="host" placeholder="smtp.example.com" .value=${this.channel?.destination ?? ""} data-test-required required /></label>
      <div class="row">
        <label>Port<input name="port" type="number" min="1" max="65535" .value=${String(this.channel?.port ?? 587)} data-test-required required /></label>
        <label>Security<select name="security" .value=${this.channel?.security ?? "start_tls"}><option value="start_tls">STARTTLS</option><option value="tls">Implicit TLS</option><option value="none">Plaintext</option></select></label>
      </div>
      <label>Username<input name="username" autocomplete="username" .value=${this.channel?.username ?? ""} /></label>
      <div class="form-field"><div class="title-with-help"><label for="smtp-password">Password</label>${renderHelpTooltip("smtp-password-help", "About SMTP password storage", this.channel ? "Leave this blank to keep the automatically managed secret. Clear the username to disable authentication." : "Enter a username and password together to enable authentication. The password is encrypted as an automatically managed secret.")}</div><input id="smtp-password" name="password" type="password" autocomplete="off" placeholder=${this.channel ? "Leave blank to keep current password" : "Optional"} /></div>
      <label>From<input name="from" placeholder="UpGrid <upgrid@example.com>" .value=${this.channel?.from ?? ""} data-test-required required /></label>
      <label>Recipient<input name="to" placeholder="on-call@example.com" .value=${this.channel?.to ?? ""} data-test-required required /></label>
    `;
  }
}
