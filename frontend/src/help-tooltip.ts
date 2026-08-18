import infoIcon from "@iconify-icons/lucide/info";
import { css, html } from "lit";

export const helpTooltipStyles = css`
  .form-field { display: grid; gap: 6px; color: var(--muted); font-size: 14px; }
  .title-with-help { display: flex; align-items: center; gap: 3px; }
  .help-tooltip-wrap { position: relative; display: inline-flex; align-items: center; }
  .help-tooltip-trigger { display: grid; width: 28px; height: 28px; place-items: center; border: 0; border-radius: 7px; background: transparent; color: var(--muted); padding: 0; cursor: pointer; user-select: none; transition: background-color 160ms ease, color 160ms ease; }
  .help-tooltip-trigger:hover { background: var(--panel-2); color: var(--text); }
  .help-tooltip-trigger iconify-icon { width: 16px; height: 16px; font-size: 16px; }
  .help-tooltip { position: absolute; top: calc(100% + 6px); left: -60px; z-index: 10; width: 280px; max-width: calc(100vw - 64px); border: 1px solid var(--line); border-radius: 9px; background: var(--panel-2); color: var(--text); box-shadow: 0 10px 30px var(--dialog-shadow); padding: 9px 10px; font-size: 12px; font-weight: 400; line-height: 1.45; opacity: 0; visibility: hidden; transform: translateY(-3px); pointer-events: none; transition: opacity 140ms ease, transform 140ms ease, visibility 140ms; }
  .help-tooltip a { display: inline-block; margin-top: 5px; color: var(--green); font-weight: 600; }
  .help-tooltip-wrap:hover .help-tooltip, .help-tooltip-wrap:focus-within .help-tooltip { opacity: 1; visibility: visible; transform: translateY(0); pointer-events: auto; }
`;

export function renderHelpTooltip(id: string, label: string, message: string, link?: { href: string; label: string }) {
  return html`
    <span class="help-tooltip-wrap">
      <button class="help-tooltip-trigger" type="button" aria-label=${label} aria-describedby=${id}>
        <iconify-icon .icon=${infoIcon} aria-hidden="true"></iconify-icon>
      </button>
      <span class="help-tooltip" id=${id} role="tooltip">
        ${message}
        ${link ? html`<a href=${link.href} target="_blank" rel="noreferrer">${link.label}</a>` : null}
      </span>
    </span>
  `;
}
