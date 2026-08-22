import { LitElement, css, html } from "lit";
import { customElement, property, state } from "lit/decorators.js";
import "@/component/tooltip.ts";
import { controlValidationMessage, type ValidatableControl } from "@/util/form-validation.ts";

function firstInvalidControl(form: HTMLFormElement): ValidatableControl | undefined {
  for (let index = 0; index < form.elements.length; index += 1) {
    const element = form.elements.item(index);
    if (element instanceof HTMLElement && "validity" in element && !(element as ValidatableControl).validity.valid) {
      return element as ValidatableControl;
    }
  }
  return undefined;
}

function formSnapshot(form: HTMLFormElement): string {
  return JSON.stringify(Array.from(new FormData(form), ([name, value]) => [name, typeof value === "string" ? value : `${value.name}:${value.size}:${value.lastModified}`]));
}

@customElement("upgrid-form-submit")
export class FormSubmit extends LitElement {
  @property({ type: Boolean }) busy = false;
  @property({ type: Boolean }) blocked = false;
  @property({ attribute: false }) changed: boolean | undefined;
  @property({ type: Boolean }) trackChanges = false;
  @property() error = "";
  @property({ attribute: "baseline-key" }) baselineKey = "";
  @property({ attribute: "blocked-message" }) blockedMessage = "Form is unavailable";
  @state() private message = "";

  private baseline = "";
  private form: HTMLFormElement | null = null;
  private button: HTMLButtonElement | null = null;

  static styles = css`
    :host { display: inline-flex; }
    :host([disabled]) { cursor: not-allowed; }
    ::slotted(button[slot="trigger"]:disabled) { pointer-events: none; }
  `;

  private readonly formChanged = (): void => this.updateState();
  private readonly formReset = (): void => queueMicrotask(() => this.captureBaseline());

  protected firstUpdated(): void {
    this.form = this.closest("form");
    this.button = this.querySelector<HTMLButtonElement>('button[type="submit"]');
    this.form?.addEventListener("input", this.formChanged);
    this.form?.addEventListener("change", this.formChanged);
    this.form?.addEventListener("reset", this.formReset);
    this.captureBaseline();
  }

  disconnectedCallback(): void {
    this.form?.removeEventListener("input", this.formChanged);
    this.form?.removeEventListener("change", this.formChanged);
    this.form?.removeEventListener("reset", this.formReset);
    super.disconnectedCallback();
  }

  protected updated(changedProperties: Map<PropertyKey, unknown>): void {
    if (changedProperties.get("busy") === true && !this.busy && !this.error) {
      queueMicrotask(() => this.captureBaseline());
    } else if (changedProperties.has("baselineKey") && changedProperties.get("baselineKey") !== undefined) {
      queueMicrotask(() => this.captureBaseline());
    }
    this.updateState();
  }

  private captureBaseline(): void {
    if (!this.form || !this.trackChanges || this.changed !== undefined) return;
    this.baseline = formSnapshot(this.form);
    this.updateState();
  }

  private updateState(): void {
    if (!this.form || !this.button) return;
    const invalid = firstInvalidControl(this.form);
    const formChanged = !this.trackChanges || (this.changed ?? formSnapshot(this.form) !== this.baseline);
    this.message = this.error.trim() || (this.blocked ? this.blockedMessage : "") || (invalid ? controlValidationMessage(invalid) : "");
    const disabled = this.busy || this.message.length > 0 || !formChanged;
    this.button.disabled = disabled;
    this.toggleAttribute("disabled", disabled);
  }

  protected render() {
    return html`
      <upgrid-tooltip .disabled=${!this.message} .focusable=${Boolean(this.message)} .label=${this.message} .message=${this.message}>
        <slot name="trigger" slot="trigger"></slot>
      </upgrid-tooltip>
    `;
  }
}

interface FormSubmitOptions {
  label: string;
  className?: string;
  busy?: boolean;
  blocked?: boolean;
  changed?: boolean;
  trackChanges?: boolean;
  error?: string;
  baselineKey?: string;
  blockedMessage?: string;
}

export function renderFormSubmit({ label, busy = false, className = "button", blocked = false, changed, error = "", baselineKey = "", blockedMessage = "Form is unavailable", trackChanges = false }: FormSubmitOptions) {
  return html`
    <upgrid-form-submit
      .busy=${busy}
      .blocked=${blocked}
      .changed=${changed}
      .error=${error}
      .trackChanges=${trackChanges || changed !== undefined}
      .baselineKey=${baselineKey}
      .blockedMessage=${blockedMessage}
    >
      <button slot="trigger" class=${className} type="submit" aria-busy=${busy ? "true" : "false"}>${label}</button>
    </upgrid-form-submit>
  `;
}
