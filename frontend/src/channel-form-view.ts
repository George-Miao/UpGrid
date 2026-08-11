import { html } from "lit";
import type { Channel } from "./api.ts";
import { renderHelpTooltip } from "./help-tooltip.ts";
import type { ChannelKind } from "./resource-input.ts";

export function renderChannelFields(kind: ChannelKind, channel?: Channel) {
  if (kind === "webhook") {
    return html`<label
      >Webhook URL<input
        name="url"
        type="url"
        placeholder="https://hooks.example.com/upgrid"
        .value=${channel?.destination ?? ""}
        data-test-required
        required
    /></label>`;
  }
  if (kind === "telegram") {
    return html`
      <label
        ><span class="title-with-help"
          >Bot token
          ${renderHelpTooltip(
            "telegram-token-help",
            "About Telegram bot token storage",
            channel ? "Leave this blank to keep the automatically managed Secret, or enter a replacement token." : "Creating the Channel encrypts this token as an automatically managed Secret. Test sends use the entered value without storing it.",
          )}</span
        ><input
          name="bot_token"
          type="password"
          autocomplete="off"
          placeholder=${channel ? "Leave blank to keep current token" : ""}
          ?required=${channel === undefined}
      /></label>
      <label
        >Chat ID<input name="chat_id" .value=${channel?.destination ?? ""} data-test-required required
      /></label>
    `;
  }
  return html`
    <label
      >SMTP host<input name="host" placeholder="smtp.example.com" .value=${channel?.destination ?? ""} required
    /></label>
    <div class="row">
      <label
        >Port<input
          name="port"
          type="number"
          min="1"
          max="65535"
          .value=${String(channel?.port ?? 587)}
          required
      /></label>
      <label
        >Security<select name="security" .value=${channel?.security ?? "start_tls"}>
          <option value="start_tls">STARTTLS</option>
          <option value="tls">Implicit TLS</option>
          <option value="none">Plaintext</option>
        </select></label
      >
    </div>
    <label
      >Username<input name="username" autocomplete="username" .value=${channel?.username ?? ""}
    /></label>
    <div class="form-field">
      <div class="title-with-help">
        <label for="smtp-password">Password</label>
        ${renderHelpTooltip(
          "smtp-password-help",
          "About SMTP password storage",
          channel ? "Leave this blank to keep the automatically managed Secret. Clear the username to disable authentication." : "Enter a username and password together to enable authentication. The password is encrypted as an automatically managed Secret.",
        )}
      </div>
      <input
        id="smtp-password"
        name="password"
        type="password"
        autocomplete="off"
        placeholder=${channel ? "Leave blank to keep current password" : "Optional"}
      />
    </div>
    <label
      >From<input name="from" placeholder="UpGrid <upgrid@example.com>" .value=${channel?.from ?? ""} required
    /></label>
    <label
      >Recipient<input name="to" placeholder="on-call@example.com" .value=${channel?.to ?? ""} required
    /></label>
  `;
}
