import inboxIcon from "@iconify-icons/lucide/inbox";
import "iconify-icon";
import { LitElement, css, html } from "lit";
import { customElement } from "lit/decorators.js";

@customElement("upgrid-empty-state")
export class EmptyState extends LitElement {
  static styles = css`
    :host {
      display: block;
      margin: 14px 0;
    }

    .state {
      box-sizing: border-box;
      display: grid;
      min-height: 132px;
      place-content: center;
      justify-items: center;
      gap: 11px;
      padding: 22px 18px;
      color: var(--muted);
      text-align: center;
    }

    .illustration {
      display: grid;
      place-items: center;
      color: var(--green);
    }

    iconify-icon {
      width: 23px;
      height: 23px;
      font-size: 23px;
    }

    p {
      max-width: 34ch;
      margin: 0;
      font-size: 13px;
      line-height: 1.45;
    }
  `;

  protected render() {
    return html`<div class="state"><span class="illustration" aria-hidden="true"><iconify-icon .icon=${inboxIcon}></iconify-icon></span><p><slot></slot></p></div>`;
  }
}
