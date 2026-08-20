import { css, html, nothing, type TemplateResult } from "lit";
import { repeat } from "lit/directives/repeat.js";
import { renderHelpTooltip } from "@/component/tooltip.ts";

export type CardActionVariant = "primary" | "secondary" | "danger" | "warning" | "success";

export interface CardAction {
  label: string;
  key?: string;
  onClick: (event: MouseEvent) => unknown;
  variant?: CardActionVariant;
  disabled?: boolean;
  busy?: boolean;
  ariaLabel?: string;
  title?: string;
}

export interface CardTooltip {
  id: string;
  label: string;
  message: string;
  link?: {
    href: string;
    label: string;
  };
}

export interface CardOptions {
  title?: string;
  label?: string;
  tooltip?: CardTooltip;
  metadata?: string | number;
  actions?: readonly CardAction[];
  content: TemplateResult;
  footer?: TemplateResult;
  className?: string;
}

export const cardStyles = css`
  .panel {
    overflow: hidden;
    border: 1px solid var(--line);
    border-radius: 16px;
    background: var(--panel-surface);
    box-shadow: 0 16px 48px var(--panel-shadow);
    transition: background-color 180ms ease, border-color 180ms ease, box-shadow 180ms ease;
  }
  .panel-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    border-bottom: 1px solid var(--line);
    padding: 17px 20px;
  }
  .panel-head h2 {
    margin: 0;
    font-size: 14px;
  }
  .card-meta {
    color: var(--muted);
    font-size: 12px;
    white-space: nowrap;
  }
  .card-actions,
  .card-footer {
    display: flex;
    align-items: center;
    justify-content: flex-end;
    gap: 12px;
  }
  .card-footer {
    border-top: 1px solid var(--line);
    padding: 14px 20px;
  }
  @media (prefers-reduced-motion: reduce) {
    .panel {
      transition-duration: 0s;
    }
  }
`;

function renderAction(action: CardAction): TemplateResult {
  const className = action.variant && action.variant !== "primary" ? `button ${action.variant}` : "button";
  return html`
    <button
      class=${className}
      type="button"
      ?disabled=${action.disabled}
      aria-busy=${action.busy ? "true" : nothing}
      aria-label=${action.ariaLabel ?? nothing}
      title=${action.title ?? nothing}
      @click=${action.onClick}
    >
      ${action.label}
    </button>
  `;
}

export function renderCard({ title, label, tooltip, metadata, actions = [], content, footer, className }: CardOptions): TemplateResult {
  const accessibleLabel = label ?? title;
  const classes = className ? `panel ${className}` : "panel";
  return html`
    <section class=${classes} aria-label=${accessibleLabel ?? nothing}>
      ${
        title
          ? html`
            <div class="panel-head">
              ${tooltip ? html`<div class="title-with-help"><h2>${title}</h2>${renderHelpTooltip(tooltip.id, tooltip.label, tooltip.message, tooltip.link)}</div>` : html`<h2>${title}</h2>`}
              ${metadata !== undefined || actions.length ? html`<div class="card-actions">${metadata !== undefined ? html`<span class="card-meta">${metadata}</span>` : nothing}${repeat(actions, (action) => action.key ?? action.ariaLabel ?? action.label, renderAction)}</div>` : nothing}
            </div>
          `
          : nothing
      }
      ${content}
      ${footer ? html`<div class="card-footer">${footer}</div>` : nothing}
    </section>
  `;
}
