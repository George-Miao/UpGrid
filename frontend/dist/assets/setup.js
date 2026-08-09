import{i as l,r as c,a as f,A as g,b as n,t as u}from"./state.js";var b=Object.defineProperty,x=Object.getOwnPropertyDescriptor,p=(t,i,o,r)=>{for(var e=r>1?void 0:r?x(i,o):i,s=t.length-1,d;s>=0;s--)(d=t[s])&&(e=(r?d(i,o,e):d(e))||e);return r&&e&&b(i,o,e),e};let a=class extends f{constructor(){super(...arguments),this.error="",this.joining=!1}async join(t){t.preventDefault();const i=t.currentTarget,o=String(new FormData(i).get("join_link")??"").trim();this.error="",this.joining=!0;try{const r=await fetch("/setup/join",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({join_link:o})});if(!r.ok){const e=await r.json().catch(()=>({error:r.statusText}));throw new Error(e.error||r.statusText)}this.waitUntilReady()}catch(r){this.error=r instanceof Error?r.message:String(r),this.joining=!1}}async waitUntilReady(){for(;;){await new Promise(t=>window.setTimeout(t,300));try{if((await fetch("/api/v1/cluster",{cache:"no-store"})).ok){window.location.replace("/");return}}catch{}}}render(){return n`
      <main>
        <header>
          <img src="/favicon.svg" alt="" />
          <div><h1>Join an UpGrid cluster</h1><p>Paste a fresh invitation from an existing Cluster.</p></div>
        </header>
        ${this.error?n`<div class="notice" role="alert">${this.error}</div>`:g}
        <form @submit=${this.join}>
          <label>
            Join Link
            <textarea name="join_link" placeholder="up://node.example:11451/…" required ?disabled=${this.joining}></textarea>
          </label>
          ${this.joining?n`<p class="progress" role="status">Joining the Cluster…</p>`:n`<p>The invitation contains Cluster credentials. Do not save or share it; its issuer can revoke it.</p>`}
          <button type="submit" ?disabled=${this.joining}>${this.joining?"Joining…":"Join Cluster"}</button>
        </form>
      </main>
    `}};a.styles=l`
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
  `;p([c()],a.prototype,"error",2);p([c()],a.prototype,"joining",2);a=p([u("upgrid-setup")],a);
