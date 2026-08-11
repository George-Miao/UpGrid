import infoIcon from "@iconify-icons/lucide/info";
import { html } from "lit";

export function renderHelpTooltip(id: string, label: string, message: string) {
  return html`
    <span class="help-tooltip-wrap">
      <button class="help-tooltip-trigger" type="button" aria-label=${label} aria-describedby=${id}>
        <iconify-icon .icon=${infoIcon} aria-hidden="true"></iconify-icon>
      </button>
      <span class="help-tooltip" id=${id} role="tooltip">${message}</span>
    </span>
  `;
}
