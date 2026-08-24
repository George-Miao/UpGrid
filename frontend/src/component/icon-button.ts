import "iconify-icon";
import { LitElement, css, html, nothing } from "lit";
import { customElement, property } from "lit/decorators.js";

export type IconButtonVariant = "secondary" | "danger" | "warning" | "success" | "move";

@customElement("upgrid-icon-button")
export class IconButton extends LitElement {
  static styles = css`
    :host {
      display: inline-flex;
      flex: none;
      line-height: 0;
    }
    button {
      display: grid;
      box-sizing: border-box;
      width: 44px;
      min-width: 44px;
      height: 44px;
      min-height: 44px;
      place-items: center;
      border: 1px solid var(--button-border);
      border-radius: var(--icon-button-radius, 9px);
      background: var(--button-bg);
      color: var(--button-text);
      padding: 0;
      cursor: pointer;
      font: inherit;
      user-select: none;
      transition:
        background-color 160ms ease,
        border-color 160ms ease,
        color 160ms ease,
        opacity 160ms ease,
        transform 120ms ease;
    }
    button.secondary:hover:not(:disabled) {
      border-color: var(--button-hover-border);
    }
    button:active:not(:disabled) {
      transform: translateY(1px);
    }
    button:focus-visible {
      outline: 2px solid var(--green);
      outline-offset: 2px;
    }
    button.secondary {
      border-color: var(--line);
      background: transparent;
      color: var(--muted);
    }
    button.danger {
      border-color: var(--danger-border);
      background: transparent;
      color: var(--danger-text);
    }
    button.danger:hover:not(:disabled) {
      border-color: var(--danger-text);
    }
    button.warning {
      border-color: var(--warning-border);
      background: transparent;
      color: var(--warning-text);
    }
    button.warning:hover:not(:disabled) {
      border-color: var(--warning-text);
    }
    button.success {
      border-color: var(--green);
      background: transparent;
      color: var(--green);
    }
    button.success:hover:not(:disabled) {
      border-color: var(--button-hover-border);
    }
    button.move {
      border-color: var(--line);
      background: var(--panel-2);
      color: var(--green);
    }
    button.move:hover:not(:disabled) {
      border-color: var(--green);
    }
    button:disabled {
      border-color: var(--disabled-border);
      background: var(--disabled-bg);
      color: var(--disabled-text);
      cursor: not-allowed;
      opacity: 1;
    }
    iconify-icon {
      display: inline-block;
      width: 18px;
      height: 18px;
      font-size: 18px;
    }
    @media (prefers-reduced-motion: reduce) {
      button {
        transition-duration: 0s;
      }
    }
  `;

  @property({ attribute: false })
  icon: object | string = "";

  @property()
  label = "";

  @property({ reflect: true })
  variant: IconButtonVariant = "secondary";

  @property({ type: Boolean, reflect: true })
  disabled = false;

  protected render() {
    return html`
      <button class=${this.variant} type="button" aria-label=${this.label || nothing} title=${this.title || this.label} ?disabled=${this.disabled}>
        <iconify-icon .icon=${this.icon} aria-hidden="true"></iconify-icon>
      </button>
    `;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "upgrid-icon-button": IconButton;
  }
}
