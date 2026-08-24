import infoIcon from "@iconify-icons/lucide/info";
import { LitElement, css, html } from "lit";
import { customElement, property } from "lit/decorators.js";

function closestDialog(element: Element): HTMLDialogElement | null {
  let current: Element | null = element;
  while (current) {
    if (current instanceof HTMLDialogElement) return current;
    if (current.parentElement) {
      current = current.parentElement;
      continue;
    }
    const root = current.getRootNode();
    current = root instanceof ShadowRoot ? root.host : null;
  }
  return null;
}

@customElement("upgrid-tooltip")
export class Tooltip extends LitElement {
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

    .popup {
      box-sizing: border-box;
      position: fixed;
      inset: auto;
      z-index: 30;
      width: var(--tooltip-width, min(280px, calc(100dvw - 24px)));
      margin: 0;
      border: 1px solid var(--line);
      border-radius: 9px;
      background: var(--panel-2);
      color: var(--text);
      box-shadow: 0 10px 30px var(--dialog-shadow);
      padding: 9px 10px;
      font-size: 12px;
      font-weight: 400;
      line-height: 1.45;
      pointer-events: var(--tooltip-pointer-events, auto);
    }
    .popup:not(:popover-open) { display: none; }
  `;

  private readonly reveal = (): void => {
    if (this.disabled) return;
    void this.updateComplete.then(() => {
      const popup = this.renderRoot.querySelector<HTMLElement>(".popup");
      if (!popup) return;
      if (!popup.matches(":popover-open")) popup.showPopover();
      this.positionPopup();
      window.addEventListener("resize", this.positionPopup);
      document.addEventListener("scroll", this.positionPopup, true);
    });
  };

  private readonly conceal = (): void => {
    queueMicrotask(() => {
      if (this.matches(":hover") || this.matches(":focus-within")) return;
      this.hidePopup();
    });
  };

  private readonly positionPopup = (): void => {
    const popup = this.renderRoot.querySelector<HTMLElement>(".popup");
    if (!popup?.matches(":popover-open")) return;
    const edge = 12;
    const gap = 6;
    const container = this.contained ? closestDialog(this)?.getBoundingClientRect() : undefined;
    const minLeft = Math.max(edge, container?.left ?? edge);
    const maxRight = Math.min(window.innerWidth - edge, container?.right ?? window.innerWidth - edge);
    const minTop = Math.max(edge, container?.top ?? edge);
    const maxBottom = Math.min(window.innerHeight - edge, container?.bottom ?? window.innerHeight - edge);
    popup.style.maxWidth = this.contained ? `${Math.max(0, maxRight - minLeft)}px` : "";
    const anchor = this.getBoundingClientRect();
    const bounds = popup.getBoundingClientRect();
    const above = anchor.top - bounds.height - gap;
    const below = anchor.bottom + gap;
    const preferBelow = this.placement === "bottom";
    const top = preferBelow ? (below + bounds.height <= maxBottom || above < minTop ? below : above) : above >= minTop || below + bounds.height > maxBottom ? above : below;
    const left = Math.min(Math.max(minLeft, anchor.right - bounds.width), maxRight - bounds.width);
    const maxTop = Math.max(minTop, maxBottom - bounds.height);
    popup.style.left = `${left}px`;
    popup.style.top = `${Math.min(Math.max(minTop, top), maxTop)}px`;
  };

  private hidePopup(): void {
    const popup = this.renderRoot.querySelector<HTMLElement>(".popup");
    if (popup?.matches(":popover-open")) popup.hidePopover();
    window.removeEventListener("resize", this.positionPopup);
    document.removeEventListener("scroll", this.positionPopup, true);
  }

  connectedCallback(): void {
    super.connectedCallback();
    this.addEventListener("mouseenter", this.reveal);
    this.addEventListener("mouseleave", this.conceal);
    this.addEventListener("focusin", this.reveal);
    this.addEventListener("focusout", this.conceal);
  }

  disconnectedCallback(): void {
    this.hidePopup();
    this.removeEventListener("mouseenter", this.reveal);
    this.removeEventListener("mouseleave", this.conceal);
    this.removeEventListener("focusin", this.reveal);
    this.removeEventListener("focusout", this.conceal);
    super.disconnectedCallback();
  }

  protected updated(): void {
    this.tabIndex = this.focusable && !this.disabled ? 0 : -1;
    if (this.disabled) this.hidePopup();
    if (this.label && !this.disabled) {
      this.setAttribute("aria-label", "Why this action is disabled");
      this.setAttribute("aria-description", this.label);
    } else {
      this.removeAttribute("aria-label");
      this.removeAttribute("aria-description");
    }
  }

  protected render() {
    return html`<slot name="trigger"></slot><span class="popup" popover="manual" role="tooltip">${this.message || html`<slot></slot>`}</span>`;
  }
}

export const helpTooltipStyles = css`
  .form-field { display: grid; gap: 6px; color: var(--muted); font-size: 14px; }
  .title-with-help { position: relative; display: flex; align-items: center; gap: 3px; }
  .help-tooltip-trigger { display: grid; width: 28px; height: 28px; place-items: center; border: 0; border-radius: 7px; background: transparent; color: var(--muted); padding: 0; cursor: pointer; user-select: none; }
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
