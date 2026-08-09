import { html } from "lit";
import deleteIcon from "@iconify-icons/lucide/trash-2";
import { type Channel, type Transition } from "./api.ts";

interface Actions {
  create: () => void;
  remove: (channel: Channel) => void;
  setDefault: (channel: Channel, isDefault: boolean) => void;
}

export function renderAlertsPage(
  transitions: Transition[],
  channels: Channel[],
  actions: Actions,
) {
  return html`
    <section class="heading" id="alerts">
      <div><span class="eyebrow">Delivery history</span><h1>Alerts</h1></div>
      <button class="button" @click=${actions.create}>Add channel</button>
    </section>
    <div class="page-columns">
      <section class="panel" aria-label="Alert history">
        <div class="panel-head"><h2>Availability transitions</h2><span class="meta">${transitions.length} events</span></div>
        ${transitions.length
          ? transitions.map((transition) => html`<div class="resource"><div><strong>${transition.target_name}</strong><code>${new Date(transition.scheduled_at_ms).toLocaleString()}</code></div><span class="badge">${transition.kind}</span></div>`)
          : html`<div class="empty">No availability transitions.</div>`}
      </section>
      <section class="panel" aria-label="Notification channels">
        <div class="panel-head"><h2>Notification channels</h2><span class="meta">${channels.length} configured</span></div>
        ${channels.length
          ? channels.map((channel) => html`
              <div class="resource">
                <div><div class="actions"><strong>${channel.name}</strong><span class="badge">${channel.kind}</span></div><code>${channel.destination}</code></div>
                <div class="actions">
                  <label class="switch"><span>Default</span><input type="checkbox" role="switch" aria-label=${`Default channel ${channel.name}`} .checked=${channel.default} @change=${(event: Event) => actions.setDefault(channel, (event.target as HTMLInputElement).checked)} /></label>
                  <button class="button danger icon-button" aria-label=${`Delete channel ${channel.name}`} title=${`Delete ${channel.name}`} @click=${() => actions.remove(channel)}><iconify-icon .icon=${deleteIcon} aria-hidden="true"></iconify-icon></button>
                </div>
              </div>
            `)
          : html`<div class="empty">No notification channels.</div>`}
      </section>
    </div>
  `;
}
