import githubIcon from "@iconify-icons/lucide/github";
import websiteIcon from "@iconify-icons/lucide/globe-2";
import { html } from "lit";

export function renderFooter() {
  return html`
    <footer aria-label="Project information">
      <a href="https://miao.dev">A Project by Pop</a>
      <span aria-hidden="true">|</span>
      <a href="https://github.com/George-Miao/UpGrid">
        <iconify-icon .icon=${githubIcon} aria-hidden="true"></iconify-icon>GitHub
      </a>
      <span aria-hidden="true">|</span>
      <a href="https://upgrid.rs">
        <iconify-icon .icon=${websiteIcon} aria-hidden="true"></iconify-icon>upgrid.rs
      </a>
    </footer>
  `;
}
