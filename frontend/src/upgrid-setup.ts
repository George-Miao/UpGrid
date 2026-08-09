import { LitElement, css, html, nothing } from "lit";
import { customElement, state } from "lit/decorators.js";

@customElement("upgrid-setup")
export class UpgridSetup extends LitElement {
  @state() private error = "";
  @state() private joining = false;

  static styles = css`
    :host {
      color-scheme: dark;
      --bg: #090d0c;
      --panel: #111715;
      --line: #27322e;
      --muted: #8fa099;
      --text: #edf7f2;
      --green: #58e29c;
      --red: #ff9b97;
      --input: #0c110f;
      display: grid;
      min-height: 100vh;
      place-items: center;
      padding: 24px;
      background:
        radial-gradient(circle at 20% 0%, #18392d 0, transparent 34%),
        linear-gradient(145deg, var(--bg), #0c1210 60%, #09100d);
      box-sizing: border-box;
      color: var(--text);
      font: 14px/1.5 Inter, ui-sans-serif, system-ui, sans-serif;
    }
    * { box-sizing: border-box; }
    main {
      width: min(520px, 100%);
      border: 1px solid var(--line);
      border-radius: 18px;
      background: color-mix(in srgb, var(--panel) 94%, transparent);
      padding: 28px;
      box-shadow: 0 28px 90px #0008;
    }
    header { display: flex; align-items: center; gap: 14px; margin-bottom: 26px; }
    img { width: 48px; height: 48px; filter: drop-shadow(0 0 18px #40d89035); }
    h1 { margin: 0; font-size: 25px; letter-spacing: -.025em; }
    p { margin: 5px 0 0; color: var(--muted); }
    form { display: grid; gap: 16px; }
    label { display: grid; gap: 7px; color: var(--muted); font-size: 12px; }
    textarea {
      width: 100%;
      min-height: 112px;
      resize: vertical;
      border: 1px solid var(--line);
      border-radius: 10px;
      outline: 0;
      background: var(--input);
      color: var(--text);
      padding: 11px;
      font: 12px/1.55 ui-monospace, SFMono-Regular, monospace;
      transition: border-color 160ms ease, opacity 160ms ease;
    }
    textarea:focus { border-color: var(--green); }
    textarea:disabled { opacity: .55; }
    button {
      justify-self: end;
      border: 1px solid #3e765a;
      border-radius: 9px;
      background: #1c4a35;
      color: #e8fff2;
      padding: 10px 15px;
      cursor: pointer;
      transition: border-color 160ms ease, opacity 160ms ease, transform 120ms ease;
    }
    button:hover { border-color: #62b988; }
    button:active { transform: translateY(1px); }
    button:disabled { cursor: wait; opacity: .65; }
    .notice { border: 1px solid #633b39; border-radius: 9px; color: var(--red); padding: 10px 12px; }
    .progress { color: var(--green); }
    @media (prefers-color-scheme: light) {
      :host {
        color-scheme: light;
        --bg: #f4f8f6;
        --panel: #ffffff;
        --line: #d3dfd9;
        --muted: #5d6e66;
        --text: #16211c;
        --green: #087a49;
        --red: #b42318;
        --input: #ffffff;
        background:
          radial-gradient(circle at 20% 0%, #d9f2e4 0, transparent 34%),
          linear-gradient(145deg, #fbfdfc, var(--bg) 60%, #edf5f1);
      }
      main { box-shadow: 0 28px 90px #233b3035; }
      button { border-color: #16764b; background: #087a49; color: #ffffff; }
    }
    @media (prefers-reduced-motion: reduce) {
      textarea, button { transition-duration: 0s; }
    }
  `;

  private async join(event: SubmitEvent): Promise<void> {
    event.preventDefault();
    const form = event.currentTarget as HTMLFormElement;
    const link = String(new FormData(form).get("join_link") ?? "").trim();
    this.error = "";
    this.joining = true;
    try {
      const response = await fetch("/setup/join", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ join_link: link }),
      });
      if (!response.ok) {
        const body = await response.json().catch(() => ({ error: response.statusText }));
        throw new Error(body.error || response.statusText);
      }
      void this.waitUntilReady();
    } catch (error) {
      this.error = error instanceof Error ? error.message : String(error);
      this.joining = false;
    }
  }

  private async waitUntilReady(): Promise<void> {
    for (;;) {
      await new Promise((resolve) => window.setTimeout(resolve, 300));
      try {
        const response = await fetch("/api/v1/cluster", { cache: "no-store" });
        if (response.ok) {
          window.location.replace("/");
          return;
        }
      } catch {
        // The setup listener closes while the Cluster API starts on the same port.
      }
    }
  }

  protected render() {
    return html`
      <main>
        <header>
          <img src="/favicon.svg" alt="" />
          <div><h1>Join an UpGrid cluster</h1><p>Paste a fresh invitation from an existing Cluster.</p></div>
        </header>
        ${this.error ? html`<div class="notice" role="alert">${this.error}</div>` : nothing}
        <form @submit=${this.join}>
          <label>
            Join Link
            <textarea name="join_link" placeholder="up://node.example:11451/…" required ?disabled=${this.joining}></textarea>
          </label>
          ${this.joining
            ? html`<p class="progress" role="status">Joining the Cluster…</p>`
            : html`<p>The invitation contains Cluster credentials. Do not save or share it; its issuer can revoke it.</p>`}
          <button type="submit" ?disabled=${this.joining}>${this.joining ? "Joining…" : "Join Cluster"}</button>
        </form>
      </main>
    `;
  }
}
