import { html, nothing } from "lit";

export function renderHeaderBrand(online: boolean, navigate?: (event: MouseEvent) => void) {
  return html`
    <div class="brand">
      <a class="brand-link" href="/" aria-label="UpGrid overview" @click=${navigate ?? nothing}><img src="/favicon.svg" alt="UpGrid" /></a>
      <span class="live"><i class=${`status-dot${online ? " online" : ""}`}></i>${online ? "Online" : "Offline"}</span>
    </div>
  `;
}
