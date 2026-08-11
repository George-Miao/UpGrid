import { html, nothing } from "lit";
import closeIcon from "@iconify-icons/lucide/x";
import deleteIcon from "@iconify-icons/lucide/trash-2";
import pauseIcon from "@iconify-icons/lucide/pause";
import playIcon from "@iconify-icons/lucide/play";
import type { Channel, ClusterMember, Secret, Target } from "./api.ts";
import { renderChannelFields, renderTlsSecretFields } from "./target-form-view.ts";

interface Actions {
  backdrop: (event: MouseEvent) => void;
  close: () => void;
  update: (event: SubmitEvent) => void;
  changed: (event: Event) => void;
  redirects: (event: Event) => void;
  delete: () => void;
  pause: (paused: boolean) => void;
}

export function renderTargetDetail(target: Target, saving: boolean, dirty: boolean, members: ClusterMember[], channels: Channel[], secrets: Secret[], actions: Actions) {
  const isNode = target.kind === "node";
  const isHttp = target.kind === "http";
  const statuses = target.accepted_statuses.map((range) => (range.start === range.end ? range.start : `${range.start}-${range.end}`)).join(",");
  const history = target.history.slice(0, 30).reverse();
  const maxLatency = Math.max(1, ...history.map((item) => item.latency_ms));
  const nodeNames = new Map(members.map((member) => [member.id, member.name]));
  const chartTime = (timestamp: number) =>
    new Date(timestamp).toLocaleString(undefined, {
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });
  const chartLatency = (latency: number) => (latency >= 1_000 ? `${(latency / 1_000).toFixed(latency >= 10_000 ? 0 : 1)} s` : `${Math.round(latency)} ms`);
  return html`
    <dialog id="detail-dialog" aria-labelledby="target-detail-title" @click=${actions.backdrop}>
      <div class="dialog-head">
        <h2 id="target-detail-title">${isNode ? "Node details" : "Target details"}</h2>
        <button class="button secondary icon-button dialog-close" type="button" aria-label=${`Close ${isNode ? "Node" : "Target"} details`} title="Close" @click=${actions.close}><iconify-icon .icon=${closeIcon} aria-hidden="true"></iconify-icon></button>
      </div>
      <form @submit=${actions.update} @input=${actions.changed}>
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
                    <http-assertion-editor name="assertions" target-id=${target.id} .assertions=${target.assertions}></http-assertion-editor>
                    <div class="row"><label class="check"><input name="follow_redirects" type="checkbox" .checked=${target.follow_redirects} @change=${actions.redirects} />Follow redirects</label><label>Maximum redirects<input name="max_redirects" type="number" min="0" .value=${String(target.max_redirects)} ?disabled=${!target.follow_redirects} required /></label></div>
                    <label class="check"><input name="skip_tls_verification" type="checkbox" .checked=${target.skip_tls_verification} />Skip TLS verification</label>
                    ${renderTlsSecretFields(secrets, target.tls_ca_secret_id, target.tls_client_certificate_secret_id, target.tls_client_private_key_secret_id)}
                  `
                  : nothing
              }
              <div class="row"><label>Interval (seconds)<input name="interval" type="number" min="1" .value=${String(target.interval_seconds)} required /></label><label>Timeout (seconds)<input name="timeout" type="number" min="1" .value=${String(target.timeout_seconds)} required /></label></div>
              <label>Failures before Down<input name="failures" type="number" min="1" .value=${String(target.failure_threshold)} required /></label>
              ${renderChannelFields(channels, target.notification_channel_ids, target.use_default_channels)}
            `
        }
        <div class="dialog-actions">
          ${
            isNode
              ? nothing
              : html`<div class="danger-actions">
            <button class="button danger icon-button" type="button" aria-label="Delete target" title="Delete target" @click=${actions.delete}><iconify-icon .icon=${deleteIcon} aria-hidden="true"></iconify-icon></button>
            <button class=${`button ${target.paused ? "success" : "warning"} icon-button`} type="button" aria-label=${target.paused ? "Resume evaluations" : "Pause evaluations"} title=${target.paused ? "Resume evaluations" : "Pause evaluations"} @click=${() => actions.pause(!target.paused)}><iconify-icon .icon=${target.paused ? playIcon : pauseIcon} aria-hidden="true"></iconify-icon></button>
          </div>`
          }
          <button class="button" type="submit" aria-busy=${saving ? "true" : "false"} ?disabled=${saving || !dirty}>Save changes</button>
        </div>
      </form>
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
    </dialog>`;
}
