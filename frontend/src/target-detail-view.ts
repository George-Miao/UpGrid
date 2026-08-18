import { html, nothing } from "lit";
import closeIcon from "@iconify-icons/lucide/x";
import deleteIcon from "@iconify-icons/lucide/trash-2";
import pauseIcon from "@iconify-icons/lucide/pause";
import playIcon from "@iconify-icons/lucide/play";
import type { Channel, ClusterMember, HistoryPage, Secret, Target } from "./api.ts";
import { renderChannelFields, renderTlsSecretFields } from "./target-form-view.ts";
export type TargetDetailTab = "details" | "general" | "assertions" | "evaluation" | "notifications";

interface Actions {
  backdrop: (event: MouseEvent) => void;
  close: () => void;
  update: (event: SubmitEvent) => void;
  changed: (event: Event) => void;
  redirects: (event: Event) => void;
  delete: () => void;
  pause: (paused: boolean) => void;
  selectTab: (tab: TargetDetailTab) => void;
}

export function renderTargetDetail(target: Target, longTermHistory: HistoryPage | undefined, historyLoading: boolean, saving: boolean, dirty: boolean, activeTab: TargetDetailTab, members: ClusterMember[], channels: Channel[], secrets: Secret[], actions: Actions) {
  const isNode = target.kind === "node";
  const isHttp = target.kind === "http";
  const statuses = target.accepted_statuses.map((range) => (range.start === range.end ? range.start : `${range.start}-${range.end}`)).join(",");
  const history = target.history.slice(0, 30).reverse();
  const maxLatency = Math.max(1, ...history.map((item) => item.latency_ms));
  const rollups = longTermHistory?.items ?? [];
  const rollupSamples = rollups.reduce((total, rollup) => total + rollup.samples, 0);
  const rollupSuccesses = rollups.reduce((total, rollup) => total + rollup.successes, 0);
  const rollupLatency = rollups.reduce((total, rollup) => total + rollup.latency_total_ms, 0);
  const availability = rollupSamples ? `${((rollupSuccesses / rollupSamples) * 100).toFixed(2)}%` : "—";
  const nodeNames = new Map(members.map((member) => [member.id, member.name]));
  const chartTime = (timestamp: number) =>
    new Date(timestamp).toLocaleString(undefined, {
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });
  const chartLatency = (latency: number) => (latency >= 1_000 ? `${(latency / 1_000).toFixed(latency >= 10_000 ? 0 : 1)} s` : `${Math.round(latency)} ms`);
  const averageLatency = rollupSamples ? chartLatency(rollupLatency / rollupSamples) : "—";
  const tabs: { id: TargetDetailTab; label: string }[] = [
    { id: "details", label: "Details" },
    { id: "general", label: "General" },
    ...(isHttp ? [{ id: "assertions" as const, label: "Assertions" }] : []),
    ...(!isNode
      ? [
          { id: "evaluation" as const, label: "Evaluation" },
          { id: "notifications" as const, label: "Notifications" },
        ]
      : []),
  ];
  const tab = tabs.some(({ id }) => id === activeTab) ? activeTab : "details";
  return html`
    <dialog id="detail-dialog" aria-labelledby="target-detail-title" @click=${actions.backdrop}>
      <div class="dialog-head target-dialog-head detail-dialog-head">
        <h2 id="target-detail-title">${isNode ? "Node details" : "Target details"}</h2>
        <div class="form-tabs" role="tablist" aria-label=${`${isNode ? "Node" : "Target"} details`}>
          ${tabs.map(({ id, label }) => html`<button form="detail-form" type="button" role="tab" aria-controls=${`target-${id}-panel`} aria-selected=${String(tab === id)} tabindex=${tab === id ? "0" : "-1"} @click=${() => actions.selectTab(id)}>${label}</button>`)}
        </div>
        <button class="button secondary icon-button dialog-close" type="button" aria-label=${`Close ${isNode ? "Node" : "Target"} details`} title="Close" @click=${actions.close}><iconify-icon .icon=${closeIcon} aria-hidden="true"></iconify-icon></button>
      </div>
      <form id="detail-form" class="detail-form" @submit=${actions.update} @input=${actions.changed}>
        <section id="target-details-panel" class="target-tab-panel details-panel" role="tabpanel" aria-label="Details" ?hidden=${tab !== "details"}>
          <section class="history">
            <div class="history-head"><h3>Long-term summary</h3><span class="meta">Last 30 days</span></div>
            ${
              historyLoading
                ? html`<p class="meta">Loading long-term history…</p>`
                : rollupSamples
                  ? html`
                    <div class="history-summary" aria-label="Long-term evaluation summary">
                      <div><span>Availability</span><strong>${availability}</strong></div>
                      <div><span>Average latency</span><strong>${averageLatency}</strong></div>
                      <div><span>Evaluations</span><strong>${rollupSamples.toLocaleString()}</strong></div>
                    </div>
                  `
                  : html`<p class="meta">No long-term history recorded yet.</p>`
            }
          </section>
          <section class="history">
            <div class="history-head"><h3>Evaluation history</h3>${history.length ? html`<span class="meta">Latest ${history.length}</span>` : nothing}</div>
            ${
              history.length
                ? html`
              <div class="chart-plot">
                <div class="chart-scale" aria-hidden="true"><span>${chartLatency(maxLatency)}</span><span>${chartLatency(maxLatency / 2)}</span><span>0 ms</span></div>
                <div class="history-chart" role="list" aria-label=${`Recent evaluation latency, 0 to ${chartLatency(maxLatency)}`}>
                  ${history.map((item) => {
                    const result = item.succeeded ? "Passed" : "Failed";
                    const status = isNode || !isHttp ? (item.succeeded ? "reachable" : "unreachable") : item.status_code === null ? "network error" : `HTTP ${item.status_code}`;
                    const executor = nodeNames.get(item.executor_node_id) ?? `Node ${item.executor_node_id.slice(0, 8)}`;
                    const label = `${result} at ${new Date(item.recorded_at_ms).toLocaleString()}: ${item.latency_ms} ms, ${status}. Executed by ${executor}`;
                    return html`<span class="history-bar ${item.succeeded ? "up" : "down"}" role="listitem" aria-label=${label} title=${label} style=${`height: ${Math.max(8, (item.latency_ms / maxLatency) * 100)}%`}></span>`;
                  })}
                </div>
              </div>
              <div class="chart-axis"><span>${chartTime(history[0].recorded_at_ms)}</span><span>${chartTime(history[history.length - 1].recorded_at_ms)}</span></div>
              <div class="chart-legend"><span><i class="up"></i>Passed</span><span><i class="down"></i>Failed</span><span>Height = latency</span></div>
            `
                : html`<p class="meta">No evaluations recorded yet.</p>`
            }
          </section>
        </section>
        <section id="target-general-panel" class="target-tab-panel" role="tabpanel" aria-label="General" ?hidden=${tab !== "general"}>
          <label>Name<input name="name" .value=${target.name} required /></label>
          ${
            isNode
              ? html`<label>RPC URL<input .value=${target.url} disabled /></label>`
              : html`
                <div class="row"><label>Type<input .value=${target.kind.toUpperCase()} disabled /></label><label>URL / endpoint<input name="url" .value=${target.url} required /></label></div>
                ${
                  isHttp
                    ? html`
                      <div class="row"><label>Method<input name="method" .value=${target.method} required /></label><label>Expected statuses<input name="statuses" .value=${statuses} required /></label></div>
                      <div class="row"><label class="switch"><span>Follow redirects</span><input class="switch-control" name="follow_redirects" type="checkbox" role="switch" .checked=${target.follow_redirects} @change=${actions.redirects} /></label><label>Maximum redirects<input name="max_redirects" type="number" min="0" .value=${String(target.max_redirects)} ?disabled=${!target.follow_redirects} required /></label></div>
                      <label class="switch"><span>Skip TLS verification</span><input class="switch-control" name="skip_tls_verification" type="checkbox" role="switch" .checked=${target.skip_tls_verification} /></label>
                      ${renderTlsSecretFields(secrets, target.tls_ca_secret_id, target.tls_client_certificate_secret_id, target.tls_client_private_key_secret_id)}
                    `
                    : nothing
                }
              `
          }
        </section>
        ${
          isHttp
            ? html`<section id="target-assertions-panel" class="target-tab-panel" role="tabpanel" aria-label="Assertions" ?hidden=${tab !== "assertions"}>
                <http-assertion-editor name="assertions" target-id=${target.id} .assertions=${target.assertions}></http-assertion-editor>
              </section>`
            : nothing
        }
        ${
          !isNode
            ? html`
              <section id="target-evaluation-panel" class="target-tab-panel" role="tabpanel" aria-label="Evaluation" ?hidden=${tab !== "evaluation"}>
                <div class="row"><label>Interval (seconds)<input name="interval" type="number" min="1" .value=${String(target.interval_seconds)} required /></label><label>Timeout (seconds)<input name="timeout" type="number" min="1" .value=${String(target.timeout_seconds)} required /></label></div>
                <div class="row"><label>Failures before down<input name="failures" type="number" min="1" .value=${String(target.failure_threshold)} required /></label><label>Evaluation locations<input name="locations" type="number" min="1" max="32" .value=${String(target.locations)} required /></label></div>
              </section>
              <section id="target-notifications-panel" class="target-tab-panel" role="tabpanel" aria-label="Notifications" ?hidden=${tab !== "notifications"}>
                ${renderChannelFields(channels, target.notification_channel_ids, target.use_default_channels)}
              </section>
            `
            : nothing
        }
        ${
          tab === "details"
            ? isNode
              ? nothing
              : html`<div class="dialog-actions"><div class="danger-actions">
                  <button class="button danger icon-button" type="button" aria-label="Move target to trash" title="Move to trash" @click=${actions.delete}><iconify-icon .icon=${deleteIcon} aria-hidden="true"></iconify-icon></button>
                  <button class=${`button ${target.paused ? "success" : "warning"} icon-button`} type="button" aria-label=${target.paused ? "Resume evaluations" : "Pause evaluations"} title=${target.paused ? "Resume evaluations" : "Pause evaluations"} @click=${() => actions.pause(!target.paused)}><iconify-icon .icon=${target.paused ? playIcon : pauseIcon} aria-hidden="true"></iconify-icon></button>
                </div></div>`
            : html`<div class="dialog-actions"><button class="button" type="submit" aria-busy=${saving ? "true" : "false"} ?disabled=${saving || !dirty}>Save changes</button></div>`
        }
      </form>
    </dialog>`;
}
