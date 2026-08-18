import downIcon from "@iconify-icons/lucide/arrow-down";
import upIcon from "@iconify-icons/lucide/arrow-up";
import deleteIcon from "@iconify-icons/lucide/trash-2";
import { css, html, LitElement, nothing, type PropertyValues } from "lit";
import { customElement, property, state } from "lit/decorators.js";
import type { HttpAssertion } from "./api.ts";
import { helpTooltipStyles, renderHelpTooltip } from "./help-tooltip.ts";

type AssertionKind = HttpAssertion["kind"];

const labels: Record<AssertionKind, string> = {
  body_contains: "Body contains",
  body_regex: "Body regex",
  json_path: "JSONPath",
  response_header: "Response header",
  latency: "Latency threshold",
  script: "Script",
};

@customElement("http-assertion-editor")
export class HttpAssertionEditor extends LitElement {
  static formAssociated = true;

  static styles = css`
    ${helpTooltipStyles}
    :host { display: grid; gap: 10px; }
    .assertions, .assertion-list { display: grid; gap: 10px; }
    .assertion-list { max-height: min(420px, 50vh); overflow-y: auto; padding-right: 4px; scrollbar-gutter: stable; }
    .assertion { display: grid; grid-template-columns: minmax(140px, 0.7fr) minmax(180px, 1.3fr) auto; gap: 8px; align-items: end; }
    .fields { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px; }
    .fields.single { grid-template-columns: 1fr; }
    label { display: grid; gap: 5px; color: var(--muted); font-size: 12px; }
    input, select, textarea { box-sizing: border-box; width: 100%; border: 1px solid var(--line); border-radius: 7px; background: var(--panel-2); color: var(--text); padding: 8px 9px; font: inherit; }
    textarea { min-height: 72px; resize: vertical; font-family: ui-monospace, monospace; }
    .actions { display: flex; gap: 4px; }
    button { border: 1px solid var(--line); border-radius: 7px; background: var(--panel-2); color: var(--text); padding: 8px 10px; cursor: pointer; user-select: none; }
    button:disabled { cursor: not-allowed; opacity: 0.45; }
    .icon-button { display: grid; width: 34px; height: 34px; min-height: 34px; place-items: center; padding: 0; }
    .icon-button iconify-icon { display: inline-block; width: 16px; height: 16px; font-size: 16px; }
    .icon-button.move { color: var(--green); }
    .icon-button.danger { color: var(--danger-text); }
    .add { display: inline-flex; min-height: 34px; align-items: center; gap: 6px; justify-self: end; border-color: var(--line); background: var(--panel-2); color: var(--text); padding: 6px 10px; font-size: 13px; font-weight: 600; transition: background-color 160ms ease, border-color 160ms ease, color 160ms ease, transform 120ms ease; }
    .add::before { color: var(--green); content: "+"; font-size: 18px; font-weight: 400; line-height: 12px; }
    .add:hover { border-color: var(--green); color: var(--green); }
    .add:active { transform: translateY(1px); }
    .empty { margin: 0; border: 1px dashed var(--line); border-radius: 9px; color: var(--muted); padding: 22px 16px; font-size: 13px; text-align: center; }
    @media (max-width: 720px) { .assertion { grid-template-columns: 1fr; } .fields { grid-template-columns: 1fr; } }
  `;

  @property({ attribute: false })
  assertions: HttpAssertion[] = [];

  @property({ attribute: "target-id" })
  targetId = "new";

  @state()
  private draft: HttpAssertion[] = [];

  private loadedTarget = "";
  private readonly internals = this.attachInternals();

  get value(): HttpAssertion[] {
    return structuredClone(this.draft);
  }

  protected willUpdate(changed: PropertyValues<this>) {
    if (changed.has("targetId") && this.loadedTarget !== this.targetId) {
      this.loadedTarget = this.targetId;
      this.draft = structuredClone(this.assertions);
    }
  }

  protected updated() {
    this.internals.setFormValue(JSON.stringify(this.draft));
  }

  formResetCallback() {
    this.draft = structuredClone(this.assertions);
    this.internals.setFormValue(JSON.stringify(this.draft));
  }

  private add() {
    this.draft = [...this.draft, emptyAssertion("body_contains")];
    this.changed();
  }

  private removeAssertion(index: number) {
    this.draft = this.draft.filter((_, current) => current !== index);
    this.changed();
  }

  private move(index: number, offset: number) {
    const next = index + offset;
    if (next < 0 || next >= this.draft.length) return;
    const draft = [...this.draft];
    [draft[index], draft[next]] = [draft[next], draft[index]];
    this.draft = draft;
    this.changed();
  }

  private setKind(index: number, event: Event) {
    const kind = (event.currentTarget as HTMLSelectElement).value as AssertionKind;
    this.replace(index, emptyAssertion(kind));
  }

  private set(index: number, field: string, event: Event) {
    const input = event.currentTarget as HTMLInputElement | HTMLTextAreaElement;
    const assertion = { ...this.draft[index], [field]: field === "max_ms" ? Number(input.value) : input.value || null } as HttpAssertion;
    this.replace(index, assertion);
  }

  private replace(index: number, assertion: HttpAssertion) {
    this.draft = this.draft.map((current, position) => (position === index ? assertion : current));
    this.changed();
  }

  private changed() {
    this.internals.setFormValue(JSON.stringify(this.draft));
    this.dispatchEvent(new Event("input", { bubbles: true, composed: true }));
  }

  protected render() {
    return html`
      <div class="assertions">
        <button class="add" type="button" aria-label="Add assertion" @click=${this.add}>Add assertion</button>
        <div class="assertion-list">
          ${this.draft.length ? this.draft.map((assertion, index) => this.renderAssertion(assertion, index)) : html`<p class="empty">No assertions.</p>`}
        </div>
      </div>
    `;
  }

  private renderAssertion(assertion: HttpAssertion, index: number) {
    return html`
      <div class="assertion">
        <label>Type<select aria-label=${`Assertion ${index + 1} type`} .value=${assertion.kind} @change=${(event: Event) => this.setKind(index, event)}>${Object.entries(labels).map(([kind, label]) => html`<option value=${kind}>${label}</option>`)}</select></label>
        ${this.renderFields(assertion, index)}
        <div class="actions">
          <button class="icon-button move" type="button" aria-label=${`Move assertion ${index + 1} up`} title="Move up" ?disabled=${index === 0} @click=${() => this.move(index, -1)}><iconify-icon .icon=${upIcon} aria-hidden="true"></iconify-icon></button>
          <button class="icon-button move" type="button" aria-label=${`Move assertion ${index + 1} down`} title="Move down" ?disabled=${index === this.draft.length - 1} @click=${() => this.move(index, 1)}><iconify-icon .icon=${downIcon} aria-hidden="true"></iconify-icon></button>
          <button class="icon-button danger" type="button" aria-label=${`Remove assertion ${index + 1}`} title="Remove assertion" @click=${() => this.removeAssertion(index)}><iconify-icon .icon=${deleteIcon} aria-hidden="true"></iconify-icon></button>
        </div>
      </div>
    `;
  }

  private renderFields(assertion: HttpAssertion, index: number) {
    switch (assertion.kind) {
      case "body_contains":
        return html`<div class="fields single"><label>Required text<input aria-label=${`Assertion ${index + 1} required text`} .value=${assertion.value} required @input=${(event: Event) => this.set(index, "value", event)} /></label></div>`;
      case "body_regex":
        return html`<div class="fields single"><label>Regular expression<input aria-label=${`Assertion ${index + 1} regular expression`} .value=${assertion.pattern} required @input=${(event: Event) => this.set(index, "pattern", event)} /></label></div>`;
      case "json_path":
        return html`<div class="fields"><label>Path<input aria-label=${`Assertion ${index + 1} JSONPath`} .value=${assertion.path} placeholder="$.status" required @input=${(event: Event) => this.set(index, "path", event)} /></label><label>Expected value (optional)<input aria-label=${`Assertion ${index + 1} expected value`} .value=${assertion.expected ?? ""} @input=${(event: Event) => this.set(index, "expected", event)} /></label></div>`;
      case "response_header":
        return html`<div class="fields"><label>Header name<input aria-label=${`Assertion ${index + 1} header name`} .value=${assertion.name} placeholder="content-type" required @input=${(event: Event) => this.set(index, "name", event)} /></label><label>Exact value (optional)<input aria-label=${`Assertion ${index + 1} header value`} .value=${assertion.value ?? ""} @input=${(event: Event) => this.set(index, "value", event)} /></label></div>`;
      case "latency":
        return html`<div class="fields single"><label>Maximum milliseconds<input aria-label=${`Assertion ${index + 1} maximum milliseconds`} type="number" min="1" .value=${String(assertion.max_ms)} required @input=${(event: Event) => this.set(index, "max_ms", event)} /></label></div>`;
      case "script":
        return html`<div class="fields single"><label><span class="title-with-help">Boolean Rhai expression ${renderHelpTooltip(`script-assertion-${index + 1}-help`, `About script assertion ${index + 1}`, "Return true to pass. The script can read the response status, latency, body, final URL, and headers.", { href: "https://upgrid.rs/reference/script-assertions/", label: "Read the script assertion reference" })}</span><textarea aria-label=${`Assertion ${index + 1} script`} required @input=${(event: Event) => this.set(index, "source", event)}>${assertion.source}</textarea></label></div>`;
      default:
        return nothing;
    }
  }
}

function emptyAssertion(kind: AssertionKind): HttpAssertion {
  switch (kind) {
    case "body_contains":
      return { kind, value: "" };
    case "body_regex":
      return { kind, pattern: "" };
    case "json_path":
      return { kind, path: "$", expected: null };
    case "response_header":
      return { kind, name: "", value: null };
    case "latency":
      return { kind, max_ms: 1_000 };
    case "script":
      return { kind, source: "status == 200" };
  }
}
