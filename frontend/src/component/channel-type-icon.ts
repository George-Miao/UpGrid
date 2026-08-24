import mailIcon from "@iconify-icons/lucide/mail";
import sendIcon from "@iconify-icons/lucide/send";
import webhookIcon from "@iconify-icons/lucide/webhook";
import { LitElement, css, html } from "lit";
import { customElement, property } from "lit/decorators.js";
import type { Channel } from "@/app/api.ts";
import "@/component/tooltip.ts";

const channelTypes = {
  telegram: { label: "Telegram", icon: sendIcon },
  webhook: { label: "Webhook", icon: webhookIcon },
  smtp: { label: "SMTP email", icon: mailIcon },
} satisfies Record<Channel["kind"], { label: string; icon: object }>;

@customElement("upgrid-channel-type-icon")
export class ChannelTypeIcon extends LitElement {
  @property({ reflect: true }) kind: Channel["kind"] = "webhook";

  static styles = css`
    :host { display: inline-flex; flex: none; color: var(--muted); }
    upgrid-tooltip { --tooltip-width: max-content; --tooltip-pointer-events: none; }
    .trigger { display: grid; width: 22px; height: 22px; place-items: center; border-radius: 5px; cursor: help; user-select: none; }
    .trigger:focus-visible { outline: 2px solid var(--green); outline-offset: 2px; }
    iconify-icon { display: inline-block; width: 17px; height: 17px; font-size: 17px; }
  `;

  protected render() {
    const type = channelTypes[this.kind] ?? channelTypes.webhook;
    return html`
      <upgrid-tooltip contained .message=${type.label}>
        <span slot="trigger" class="trigger" role="img" aria-label=${`${type.label} notification channel`} tabindex="0">
          <iconify-icon .icon=${type.icon} aria-hidden="true"></iconify-icon>
        </span>
      </upgrid-tooltip>
    `;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "upgrid-channel-type-icon": ChannelTypeIcon;
  }
}
