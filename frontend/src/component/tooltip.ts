import infoIcon from "@iconify-icons/lucide/info";
import { LitElement, css, html } from "lit";
import { customElement, property } from "lit/decorators.js";

@customElement("upgrid-tooltip")
export class UpgridTooltip extends LitElement {
  @property({ type: Boolean, reflect: true }) disabled = false;
  @property({ type: Boolean }) focusable = false;
  @property({ type: Boolean, reflect: true }) contained = false;
  @property({ reflect: true }) placement: "top" | "bottom" = "top";
  @property() label = "";
  @property() message = "";

  static styles = css`
    :host {
      position: relative;
      display: inline-flex;
      align-items: center;
    }
    :host([contained]) { position: static; }


    .popup {
      position: absolute;
      right: 0;
      z-index: 30;
      width: min(280px, calc(100dvw - 72px));
      border: 1px solid var(--line);
      border-radius: 9px;
      background: var(--panel-2);
      color: var(--text);
      box-shadow: 0 10px 30px var(--dialog-shadow);
      padding: 9px 10px;
      font-size: 12px;
      font-weight: 400;
      line-height: 1.45;
      opacity: 0;
      visibility: hidden;
      pointer-events: none;
      transition: opacity 140ms ease, visibility 140ms;
    }
    :host([contained]) .popup { width: min(280px, 100%); }

    :host([placement="top"]) .popup { bottom: calc(100% + 6px); }
    :host([placement="bottom"]) .popup { top: calc(100% + 6px); }
    :host(:not([disabled]):hover) .popup,
    :host(:not([disabled]):focus-within) .popup {
      opacity: 1;
      visibility: visible;
      pointer-events: auto;
    }

    @media (prefers-reduced-motion: reduce) {
      .popup { transition-duration: 0s; }
    }
  `;

  protected updated(): void {
    this.tabIndex = this.focusable && !this.disabled ? 0 : -1;
    if (this.label && !this.disabled) {
      this.setAttribute("aria-label", "Why this action is disabled");
      this.setAttribute("aria-description", this.label);
    } else {
      this.removeAttribute("aria-label");
      this.removeAttribute("aria-description");
    }
  }

  protected render() {
    return html`<slot name="trigger"></slot><span class="popup" role="tooltip">${this.message || html`<slot></slot>`}</span>`;
  }
}

export const helpTooltipStyles = css`
  .form-field { display: grid; gap: 6px; color: var(--muted); font-size: 14px; }
  .title-with-help { position: relative; display: flex; align-items: center; gap: 3px; }
  .help-tooltip-trigger { display: grid; width: 28px; height: 28px; place-items: center; border: 0; border-radius: 7px; background: transparent; color: var(--muted); padding: 0; cursor: pointer; user-select: none; transition: background-color 160ms ease, color 160ms ease; }
  .help-tooltip-trigger:hover { background: var(--panel-2); color: var(--text); }
  .help-tooltip-trigger iconify-icon { width: 16px; height: 16px; font-size: 16px; }
  .help-tooltip-content a { display: inline-block; margin-top: 5px; color: var(--green); font-weight: 600; }
`;

export function renderHelpTooltip(id: string, label: string, message: string, link?: { href: string; label: string }) {
  return html`
    <upgrid-tooltip placement="bottom" contained>
      <button slot="trigger" class="help-tooltip-trigger" type="button" aria-label=${label} aria-describedby=${id}>
        <iconify-icon .icon=${infoIcon} aria-hidden="true"></iconify-icon>
      </button>
      <span class="help-tooltip-content" id=${id}>
        ${message}
        ${link ? html`<a href=${link.href} target="_blank" rel="noreferrer">${link.label}</a>` : null}
      </span>
    </upgrid-tooltip>
  `;
}
