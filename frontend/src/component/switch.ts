import { LitElement, css, html, nothing } from "lit";
import { customElement, property, state } from "lit/decorators.js";

@customElement("upgrid-toggle-switch")
export class ToggleSwitch extends LitElement {
  static formAssociated = true;

  static styles = css`
    :host { display: block; min-width: 0; color: var(--muted); font-size: 14px; }
    :host([hidden]) { display: none; }
    :host([compact]) { display: inline-block; width: fit-content; max-width: 100%; align-self: start; justify-self: start; }
    label { display: flex; align-items: center; justify-content: space-between; gap: 12px; font: inherit; cursor: pointer; user-select: none; }
    .label { min-width: 0; }
    :host([compact]) label { justify-content: flex-start; }
    input { box-sizing: border-box; display: flex; width: 42px; min-height: 24px; height: 24px; flex: none; align-items: center; margin: 0; appearance: none; border: 1px solid var(--line); border-radius: 999px; outline: 0; background: var(--input-bg); padding: 2px; cursor: pointer; }
    input::after { display: block; width: 16px; height: 16px; border-radius: 50%; background: var(--muted); content: ""; transition: background-color 160ms ease, transform 160ms ease; }
    input:checked { border-color: var(--button-border); background: var(--button-bg); }
    input:checked::after { background: var(--button-text); transform: translateX(18px); }
    input:focus-visible { outline: 2px solid var(--green); outline-offset: 2px; }
    input:disabled, label.disabled { cursor: not-allowed; }
    input:disabled { opacity: .65; }
    @media (prefers-reduced-motion: reduce) { input, input::after { transition-duration: 0s; } }
  `;

  @property({ type: Boolean }) checked = false;
  @property({ type: Boolean, reflect: true }) disabled = false;
  @property({ reflect: true }) name = "";
  @property({ reflect: true }) value = "on";
  @property({ attribute: "aria-label" }) ariaLabel = "";
  @property({ type: Boolean, reflect: true }) compact = false;

  @state() private formDisabled = false;

  private readonly internals = this.attachInternals();
  private initialChecked?: boolean;

  get form(): HTMLFormElement | null {
    return this.internals.form;
  }

  get validity(): ValidityState {
    return this.internals.validity;
  }

  get validationMessage(): string {
    return this.internals.validationMessage;
  }

  get willValidate(): boolean {
    return this.internals.willValidate;
  }

  checkValidity(): boolean {
    return this.internals.checkValidity();
  }

  reportValidity(): boolean {
    return this.internals.reportValidity();
  }

  protected updated(): void {
    this.initialChecked ??= this.checked;
    this.updateFormValue();
  }

  formDisabledCallback(disabled: boolean): void {
    this.formDisabled = disabled;
    this.updateFormValue();
  }

  formResetCallback(): void {
    this.checked = this.initialChecked ?? false;
    this.updateFormValue();
  }

  formStateRestoreCallback(state: string | File | FormData | null): void {
    this.checked = state === "checked";
    this.updateFormValue();
  }

  private get controlDisabled(): boolean {
    return this.disabled || this.formDisabled;
  }

  private updateFormValue(): void {
    this.internals.setFormValue(this.checked && !this.controlDisabled ? this.value : null, this.checked ? "checked" : "unchecked");
  }

  private forward(event: Event): void {
    event.stopPropagation();
    this.checked = (event.currentTarget as HTMLInputElement).checked;
    this.updateFormValue();
    this.dispatchEvent(new Event(event.type, { bubbles: true, composed: true }));
  }

  protected render() {
    return html`
      <label class=${this.controlDisabled ? "disabled" : ""}>
        <span class="label"><slot></slot></span>
        <input type="checkbox" role="switch" .checked=${this.checked} ?disabled=${this.controlDisabled} aria-label=${this.ariaLabel || nothing} @input=${this.forward} @change=${this.forward} />
      </label>
    `;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "upgrid-toggle-switch": ToggleSwitch;
  }
}
