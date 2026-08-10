import { html } from "lit";
import { type Channel } from "./api.ts";

interface Actions {
  backdrop: (event: MouseEvent) => void;
  close: () => void;
  create: (event: SubmitEvent) => void;
}

export function renderChannelFields(
  channels: Channel[],
  selected: string[] = [],
  useDefaults = true,
) {
  const updateDefaults = (event: Event) => {
    const toggle = event.currentTarget as HTMLInputElement;
    const fieldset = toggle.closest("fieldset");
    fieldset?.querySelectorAll<HTMLInputElement>('input[data-default="true"]')
      .forEach((input) => {
        input.disabled = toggle.checked;
        input.checked = toggle.checked || input.dataset.explicit === "true";
      });
    toggle.form?.dispatchEvent(new Event("input", { bubbles: true }));
  };
  return html`
    <fieldset class="channel-fields">
      <legend>Notification channels</legend>
      <label class="switch">
        <span>Use default channels</span>
        <input
          name="use_default_channels"
          type="checkbox"
          role="switch"
          .checked=${useDefaults}
          @change=${updateDefaults}
        />
      </label>
      <div class="channel-options">
        ${channels.map((channel) => {
          const explicit = selected.includes(channel.id);
          const inherited = useDefaults && channel.default;
          return html`
            <label class="check">
              <input
                name="channel_id"
                type="checkbox"
                value=${channel.id}
                data-default=${String(channel.default)}
                data-explicit=${String(explicit)}
                .checked=${explicit || inherited}
                ?disabled=${inherited}
                @change=${(event: Event) => {
                  const input = event.currentTarget as HTMLInputElement;
                  input.dataset.explicit = String(input.checked);
                }}
              />
              ${channel.name} <span class="badge">${channel.kind}</span>
            </label>
          `;
        })}
      </div>
    </fieldset>`;
}

export function renderTargetForm(
  channels: Channel[],
  saving: boolean,
  actions: Actions,
) {
  return html`
    <dialog id="target-dialog" aria-labelledby="add-target-title" @click=${actions.backdrop}>
      <div class="dialog-head"><h2 id="add-target-title">Add target</h2><p>Start monitoring an HTTP or HTTPS endpoint.</p></div>
      <form @submit=${actions.create}>
        <label>Name<input name="name" placeholder="Production API" required /></label>
        <label>URL<input name="url" type="url" placeholder="https://example.com/health" required /></label>
        <div class="row">
          <label>Method<input name="method" value="GET" required /></label>
          <label>Interval (seconds)<input name="interval" type="number" min="1" value="60" required /></label>
        </div>
        <div class="row">
          <label>Timeout (seconds)<input name="timeout" type="number" min="1" value="10" required /></label>
          <label>Failures before Down<input name="failures" type="number" min="1" value="3" required /></label>
        </div>
        ${renderChannelFields(channels)}
        <div class="dialog-actions">
          <button class="button secondary" type="button" @click=${actions.close}>Cancel</button>
          <button class="button" type="submit" ?disabled=${saving}>${saving ? "Creating…" : "Create target"}</button>
        </div>
      </form>
    </dialog>`;
}
