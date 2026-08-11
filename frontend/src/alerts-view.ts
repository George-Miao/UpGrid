import editIcon from "@iconify-icons/lucide/pencil";
import deleteIcon from "@iconify-icons/lucide/trash-2";
import { html, nothing } from "lit";
import type { Alert, Channel, Transition } from "./api.ts";

export interface AlertFilters {
  search: string;
  delivery: "all" | Alert["delivery"];
  kind: "all" | Alert["kind"];
  acknowledged: "all" | "yes" | "no";
}

interface Actions {
  create: () => void;
  edit: (channel: Channel) => void;
  remove: (channel: Channel) => void;
  setDefault: (channel: Channel, isDefault: boolean) => void;
  acknowledge: (alert: Alert) => void;
  retry: (alert: Alert) => void;
  setSearch: (value: string) => void;
  setDelivery: (value: AlertFilters["delivery"]) => void;
  setKind: (value: AlertFilters["kind"]) => void;
  setAcknowledged: (value: AlertFilters["acknowledged"]) => void;
}

function matches(alert: Alert, filters: AlertFilters): boolean {
  const search = filters.search.trim().toLocaleLowerCase();
  return (
    (!search || `${alert.target_name} ${alert.channel_name}`.toLocaleLowerCase().includes(search)) &&
    (filters.delivery === "all" || alert.delivery === filters.delivery) &&
    (filters.kind === "all" || alert.kind === filters.kind) &&
    (filters.acknowledged === "all" || (filters.acknowledged === "yes" ? alert.acknowledged_at_ms !== null : alert.acknowledged_at_ms === null))
  );
}

function deliveryDetail(alert: Alert): string {
  if (alert.delivery === "pending") {
    return alert.next_attempt_at_ms === null ? `${alert.attempts} attempts` : `${alert.attempts} attempts · next ${new Date(alert.next_attempt_at_ms).toLocaleString()}`;
  }
  if (alert.delivery === "failed") return alert.diagnostic ?? "Delivery failed";
  return alert.completed_at_ms === null ? "Delivered" : `Delivered ${new Date(alert.completed_at_ms).toLocaleString()}`;
}

export function renderAlertsPage(alerts: Alert[], transitions: Transition[], channels: Channel[], filters: AlertFilters, saving: boolean, actions: Actions) {
  const visibleAlerts = alerts.filter((alert) => matches(alert, filters));
  return html`
    <section class="heading" id="alerts">
      <div><span class="eyebrow">Delivery history</span><h1>Alerts</h1></div>
      <button class="button" @click=${actions.create}>Add channel</button>
    </section>
    <section class="panel alert-history" aria-label="Alert history">
      <div class="panel-head"><h2>Notification deliveries</h2><span class="meta">${visibleAlerts.length} of ${alerts.length} alerts</span></div>
      <div class="alert-filters">
        <label>Search<input type="search" .value=${filters.search} placeholder="Target or channel" @input=${(event: Event) => actions.setSearch((event.target as HTMLInputElement).value)} /></label>
        <label>Delivery<select .value=${filters.delivery} @change=${(event: Event) => actions.setDelivery((event.target as HTMLSelectElement).value as AlertFilters["delivery"])}><option value="all">All</option><option value="pending">Pending</option><option value="delivered">Delivered</option><option value="failed">Failed</option></select></label>
        <label>Transition<select .value=${filters.kind} @change=${(event: Event) => actions.setKind((event.target as HTMLSelectElement).value as AlertFilters["kind"])}><option value="all">All</option><option value="down">Down</option><option value="recovered">Recovered</option></select></label>
        <label>Acknowledged<select .value=${filters.acknowledged} @change=${(event: Event) => actions.setAcknowledged((event.target as HTMLSelectElement).value as AlertFilters["acknowledged"])}><option value="all">All</option><option value="no">No</option><option value="yes">Yes</option></select></label>
      </div>
      ${
        visibleAlerts.length
          ? visibleAlerts.map(
              (alert) => html`
                <div class="resource alert-resource">
                  <div class="alert-summary">
                    <div class="channel-title">
                      <strong>${alert.target_name}</strong>
                      <span class=${`badge ${alert.kind === "recovered" ? "up" : "down"}`}>${alert.kind}</span>
                      <span class="badge">${alert.delivery}</span>
                      ${alert.acknowledged_at_ms === null ? nothing : html`<span class="badge">acknowledged</span>`}
                    </div>
                    <code>${alert.channel_name} · ${new Date(alert.scheduled_at_ms).toLocaleString()}</code>
                    <span class="meta">${deliveryDetail(alert)}</span>
                  </div>
                  <div class="alert-actions">
                    ${alert.delivery === "failed" ? html`<button class="button secondary" ?disabled=${saving} @click=${() => actions.retry(alert)}>Retry</button>` : nothing}
                    ${alert.acknowledged_at_ms === null ? html`<button class="button secondary" ?disabled=${saving} @click=${() => actions.acknowledge(alert)}>Acknowledge</button>` : nothing}
                  </div>
                </div>
              `,
            )
          : html`<div class="empty">No alerts match these filters.</div>`
      }
    </section>
    <div class="page-columns">
      <section class="panel" aria-label="Availability history">
        <div class="panel-head"><h2>Availability transitions</h2><span class="meta">${transitions.length} events</span></div>
        ${
          transitions.length
            ? transitions.map((transition) => {
                const status = transition.kind === "recovered" ? "up" : "down";
                return html`
                <div class="resource">
                  <div class="transition-main">
                    <span class=${`state ${status}`} aria-hidden="true"></span>
                    <div>
                      <strong>${transition.target_name}</strong>
                      <code>${new Date(transition.scheduled_at_ms).toLocaleString()}</code>
                    </div>
                  </div>
                  <span class=${`badge ${status}`}>${transition.kind}</span>
                </div>
              `;
              })
            : html`<div class="empty">No availability transitions.</div>`
        }
      </section>
      <section class="panel" aria-label="Notification channels">
        <div class="panel-head"><h2>Notification channels</h2><span class="meta">${channels.length} configured</span></div>
        ${
          channels.length
            ? channels.map(
                (channel) => html`
              <div class="resource channel-resource">
                <div class="channel-summary"><div class="channel-title"><strong>${channel.name}</strong><span class="badge">${channel.kind}</span></div><code>${channel.destination}</code></div>
                <div class="channel-actions">
                  <label class="switch"><span>Default</span><input type="checkbox" role="switch" aria-label=${`Default channel ${channel.name}`} .checked=${channel.default} @change=${(event: Event) => actions.setDefault(channel, (event.target as HTMLInputElement).checked)} /></label>
                  <button class="button secondary icon-button" aria-label=${`Edit channel ${channel.name}`} title=${`Edit ${channel.name}`} @click=${() => actions.edit(channel)}><iconify-icon .icon=${editIcon} aria-hidden="true"></iconify-icon></button>
                  <button class="button danger icon-button" aria-label=${`Delete channel ${channel.name}`} title=${`Delete ${channel.name}`} @click=${() => actions.remove(channel)}><iconify-icon .icon=${deleteIcon} aria-hidden="true"></iconify-icon></button>
                </div>
              </div>
            `,
              )
            : html`<div class="empty">No notification channels.</div>`
        }
      </section>
    </div>
  `;
}
