import { css, html, nothing } from "lit";
import { customElement } from "lit/decorators.js";
import pauseIcon from "@iconify-icons/lucide/pause";
import playIcon from "@iconify-icons/lucide/play";
import deleteIcon from "@iconify-icons/lucide/trash-2";
import closeIcon from "@iconify-icons/lucide/x";
import userIcon from "@iconify-icons/lucide/user";
import "iconify-icon";
import "./setup-flow.ts";
import "./http-assertion-editor.ts";
import type { ClusterMember, PublicStatusTarget, Target, TrashedTarget } from "./api.ts";
import { AppController } from "./app-controller.ts";
import { renderAlertsPage } from "./alerts-view.ts";
import { type Section, sectionPaths, serviceHealth, themeIcons } from "./app-state.ts";
import { type AuthActions, renderApiTokensPage, renderChangePassword, renderLogin, renderManagePage, renderUsersPage } from "./auth-view.ts";
import { renderChannelFields } from "./channel-form-view.ts";
import { renderFooter } from "./footer-view.ts";
import { helpTooltipStyles, renderHelpTooltip } from "./help-tooltip.ts";
import type { ChannelKind } from "./resource-input.ts";
import { renderTargetDetail } from "./target-detail-view.ts";
import { renderTargetForm } from "./target-form-view.ts";
@customElement("upgrid-app")
export class UpgridApp extends AppController {
  static styles = css`
    :host {
      color-scheme: dark;
      --bg: #090d0c;
      --panel: #111715;
      --panel-2: #151d1a;
      --line: #27322e;
      --muted: #8fa099;
      --text: #edf7f2;
      --green: #58e29c;
      --red: #ff7575;
      --amber: #f2c264;
      --page-background:
        radial-gradient(circle at 12% -5%, #18392d 0, transparent 30%),
        linear-gradient(145deg, #090d0c 0%, #0c1210 55%, #09100d 100%);
      --brand-shadow: #40d89035;
      --nav-bg: #0d1210aa;
      --active-bg: #202b27;
      --button-border: #3e765a;
      --button-bg: #1c4a35;
      --button-text: #e8fff2;
      --button-hover-border: #62b988;
      --panel-surface: #111715dc;
      --panel-shadow: #0002;
      --divider: #202925;
      --badge-border: #3c554a;
      --badge-text: #a7c3b7;
      --row-hover: #17201c;
      --notice-border: #7b3937;
      --notice-bg: #391b1a;
      --notice-text: #ffb3af;
      --bulk-bg: #16221d;
      --dialog-shadow: #000b;
      --backdrop: #040706cc;
      --input-bg: #0c110f;
      --focus: #4b936c;
      --danger-text: #ff9b97;
      --danger-border: #633b39;
      --warning-bg: #594315;
      --warning-text: #ffd778;
      --warning-border: #9c7625;
      --join-bg: #0b110e;
      display: flex; flex-direction: column;
      min-height: 100vh;
      background: var(--page-background);
      color: var(--text);
      font: 14px/1.5 Inter, ui-sans-serif, system-ui, sans-serif;
      transition: background 220ms ease, color 180ms ease;
    }
    * { box-sizing: border-box; }
    button, input, select { font: inherit; }
    .shell { flex: 1 0 auto; width: 100%; max-width: 1200px; margin: auto; padding: 28px 24px 48px; }
    .setup-shell { display: grid; grid-template-rows: auto minmax(0, 1fr); padding-top: 20px; padding-bottom: 20px; }
    .setup-shell header { margin-bottom: 18px; } .setup-shell upgrid-setup { align-self: center; }
    header { display: grid; grid-template-columns: minmax(0, 1fr) auto minmax(0, 1fr); align-items: center; margin-bottom: 34px; }
    .brand, .actions, .live, nav { display: flex; align-items: center; }
    .brand { gap: 13px; }
    header > .brand { justify-self: start; }
    header > nav { justify-self: center; }
    header > .actions { justify-self: end; }
    .brand-line { display: flex; align-items: center; gap: 12px; }
    .brand img { width: 42px; height: 42px; filter: drop-shadow(0 0 18px var(--brand-shadow)); }
    .brand strong { display: block; font-size: 19px; letter-spacing: .02em; }
    .brand span, .live, .eyebrow, .meta { color: var(--muted); font-size: 12px; }
    nav { gap: 4px; padding: 4px; border: 1px solid var(--line); border-radius: 11px; background: var(--nav-bg); }
    nav a { color: var(--muted); padding: 7px 11px; text-decoration: none; border-radius: 7px; transition: background-color 160ms ease, color 160ms ease; }
    nav a.active { color: var(--text); background: var(--active-bg); }
    .actions { gap: 12px; }
    .live { gap: 7px; }
    .dot { width: 7px; height: 7px; border-radius: 50%; background: var(--amber); transition: background-color 160ms ease, box-shadow 160ms ease; }
    .dot.up { background: var(--green); box-shadow: 0 0 10px var(--green); }
    .dot.degraded { background: var(--amber); box-shadow: 0 0 10px var(--amber); }
    .dot.down { background: var(--red); box-shadow: 0 0 10px var(--red); }
    .heading { display: flex; align-items: flex-end; justify-content: space-between; margin-bottom: 30px; }
    .heading h1 { margin: 2px 0 0; font-size: clamp(27px, 4vw, 38px); line-height: 1.1; letter-spacing: -.035em; }
    .eyebrow { text-transform: uppercase; letter-spacing: .16em; }
    .button { min-height: 44px; border: 1px solid var(--button-border); border-radius: 9px; background: var(--button-bg); color: var(--button-text); padding: 9px 13px; white-space: nowrap; cursor: pointer; transition: background-color 160ms ease, border-color 160ms ease, color 160ms ease, opacity 160ms ease, transform 120ms ease; }
    .button:hover { border-color: var(--button-hover-border); }
    .button:active { transform: translateY(1px); }
    .button:disabled { cursor: not-allowed; opacity: .65; }
    .button[aria-busy="true"] { cursor: wait; }
    .icon-button { display: grid; width: 44px; height: 44px; min-height: 44px; place-items: center; padding: 0; }
    iconify-icon { display: inline-block; width: 18px; height: 18px; font-size: 18px; }
    .account-menu { position: relative; }
    .account-menu summary { list-style: none; }
    .account-menu summary::-webkit-details-marker { display: none; }
    .account-dropdown { position: absolute; top: calc(100% + 8px); right: 0; z-index: 20; display: grid; width: max-content; min-width: 180px; gap: 2px; border: 1px solid var(--line); border-radius: 10px; background: var(--panel); padding: 6px; box-shadow: 0 16px 40px var(--dialog-shadow); }
    .account-dropdown .button { display: flex; width: 100%; min-height: 44px; align-items: center; justify-content: flex-start; box-sizing: border-box; border: 0; background: transparent; padding: 9px 13px; color: var(--muted); font: inherit; line-height: 1.2; text-align: left; text-decoration: none; }
    .account-dropdown .button:hover, .account-dropdown .button:focus-visible { background: var(--row-hover); color: var(--text); }
    .account-separator { height: 1px; margin: 4px 0; background: var(--divider); }
    .account-dropdown .danger { color: var(--danger-text); }
    .account-dropdown .danger:hover, .account-dropdown .danger:focus-visible { background: var(--notice-bg); color: var(--danger-text); }
    ${helpTooltipStyles}
    .auth-panel { width: min(440px, 100%); margin: auto; }
    .admin-page { width: min(760px, 100%); margin: auto; }
    .token-value { margin: 14px; overflow-wrap: anywhere; }
    .token-value code { display: block; margin: 8px 0; }
    .overview-top { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 18px; margin-bottom: 18px; }
    .page-columns { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 18px; }
    .summary { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
    .metric, .panel { border: 1px solid var(--line); background: var(--panel-surface); box-shadow: 0 16px 48px var(--panel-shadow); transition: background-color 180ms ease, border-color 180ms ease, box-shadow 180ms ease; }
    .metric { border-radius: 14px; padding: 17px 18px; }
    .metric span { display: block; color: var(--muted); font-size: 11px; letter-spacing: .11em; text-transform: uppercase; }
    .metric strong { display: block; margin-top: 5px; font-size: 29px; font-weight: 560; }
    .metric.down.active span, .metric.down.active strong { color: var(--red); }
    .panel { border-radius: 16px; overflow: hidden; }
    .resource { display: flex; align-items: center; justify-content: space-between; gap: 12px; padding: 13px 20px; border-bottom: 1px solid var(--divider); }
    .resource:last-child { border-bottom: 0; }
    .resource strong { display: block; font-size: 13px; }
    .resource code { color: var(--muted); font-size: 11px; }
    .user-resource { padding: 0 20px 0 0; transition: background-color 150ms ease; }
    .user-resource:hover { background: var(--row-hover); }
    .resource-main { display: block; min-width: 0; flex: 1; border: 0; background: transparent; color: var(--text); padding: 13px 20px; text-align: left; }
    .badge { border: 1px solid var(--badge-border); border-radius: 999px; color: var(--badge-text); padding: 2px 7px; font-size: 10px; text-transform: uppercase; }
    .badge.up { border-color: var(--green); color: var(--green); }
    .badge.down { border-color: var(--red); color: var(--red); }
    .transition-main { display: flex; align-items: center; gap: 12px; }
    .channel-resource { display: grid; grid-template-columns: minmax(0, 1fr) auto; }
    .channel-summary { min-width: 0; }
    .channel-title, .channel-actions { display: flex; align-items: center; gap: 10px; }
    .channel-summary code { display: block; margin-top: 2px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .channel-actions .switch span { font-size: 12px; }
    .panel-head { display: flex; align-items: center; justify-content: space-between; padding: 17px 20px; border-bottom: 1px solid var(--line); }
    .panel-head h2 { margin: 0; font-size: 14px; }
    .alert-history { margin-bottom: 20px; }
    .alert-filters { display: grid; grid-template-columns: minmax(180px, 1fr) repeat(3, minmax(120px, auto)); gap: 10px; padding: 14px 20px; border-bottom: 1px solid var(--line); }
    .alert-filters label { display: grid; gap: 5px; color: var(--muted); font-size: 11px; }
    .alert-resource { display: grid; grid-template-columns: minmax(0, 1fr) auto; }
    .alert-summary { display: grid; min-width: 0; gap: 4px; }
    .alert-summary code, .alert-summary .meta { font-size: 11px; }
    .alert-summary .meta { color: var(--muted); white-space: normal; }
    .alert-actions { display: flex; align-items: center; gap: 8px; }
    .target-wrap { display: grid; grid-template-columns: auto minmax(0, 1fr); align-items: center; border-bottom: 1px solid var(--divider); padding-left: 20px; }
    .target-wrap:last-child { border-bottom: 0; }
    .select-target { width: 18px; min-height: 18px; height: 18px; margin: 0; accent-color: var(--button-bg); cursor: pointer; }
    .target { width: 100%; display: grid; grid-template-columns: auto minmax(0, 1fr) auto; gap: 14px; align-items: center; padding: 17px 20px 17px 14px; border: 0; background: transparent; color: var(--text); text-align: left; cursor: pointer; }
    .target-wrap, .target { transition: background-color 150ms ease; }
    .target-wrap:hover, .target-wrap:hover .target { background: var(--row-hover); }
    .node-target { cursor: pointer; }
    .state { width: 10px; height: 10px; border-radius: 50%; color: var(--amber); background: var(--amber); box-shadow: 0 0 12px currentColor; transition: background-color 160ms ease, color 160ms ease, box-shadow 160ms ease; }
    .state.up { color: var(--green); background: var(--green); }
    .state.down { color: var(--red); background: var(--red); }
    .state.paused { color: var(--muted); background: var(--muted); box-shadow: none; }
    .target h3 { margin: 0 0 3px; font-size: 14px; }
    .target-title { display: flex; align-items: center; gap: 8px; margin-bottom: 3px; }
    .target-title h3 { margin: 0; }
    .meta { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .latency { text-align: right; }
    .latency strong { display: block; font-weight: 500; }
    .latency span { color: var(--muted); font-size: 11px; }
    .target-side { display: flex; align-items: center; gap: 20px; }
    .mini-chart { display: flex; width: 88px; height: 32px; align-items: flex-end; gap: 2px; }
    .mini-bar { flex: 1; min-width: 2px; max-width: 7px; border-radius: 2px 2px 1px 1px; opacity: .75; transition: background-color 160ms ease, height 180ms ease, opacity 160ms ease; }
    .mini-bar.up { background: var(--green); }
    .mini-bar.down { background: var(--red); }
    .empty { padding: 54px 20px; color: var(--muted); text-align: center; }
    .notice { margin: 0 0 16px; border: 1px solid var(--notice-border); border-radius: 10px; background: var(--notice-bg); color: var(--notice-text); padding: 10px 12px; }
    .toolbar { display: grid; grid-template-columns: minmax(180px, 1fr) auto auto; gap: 8px; padding: 12px 20px; border-bottom: 1px solid var(--line); }
    .toolbar input, .toolbar select { padding: 7px 9px; }
    .toolbar select { appearance: none; padding-right: 38px; background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24'%3E%3Cpath d='m6 9 6 6 6-6' fill='none' stroke='%235f7168' stroke-linecap='round' stroke-linejoin='round' stroke-width='2'/%3E%3C/svg%3E"); background-position: right 14px center; background-repeat: no-repeat; background-size: 16px; }
    .bulk { display: flex; align-items: center; gap: 8px; padding: 10px 20px; border-bottom: 1px solid var(--line); background: var(--bulk-bg); }
    .bulk-actions { display: flex; align-items: center; gap: 8px; margin-left: auto; }
    .bulk, .bulk-actions .button { animation: reveal 160ms ease-out; }
    @keyframes reveal { from { opacity: 0; transform: translateY(-3px); } }
    dialog { width: min(580px, calc(100% - 28px)); max-height: calc(100dvh - 28px); overflow-y: auto; border: 1px solid var(--line); border-radius: 17px; background: var(--panel); color: var(--text); padding: 0; scrollbar-gutter: stable both-edges; box-shadow: 0 28px 90px var(--dialog-shadow); opacity: 0; transform: translateY(8px) scale(.985); transition: opacity 170ms ease, transform 170ms ease, overlay 170ms allow-discrete, display 170ms allow-discrete; }
    #target-dialog { width: min(720px, calc(100% - 28px)); }
    dialog[open] { opacity: 1; transform: translateY(0) scale(1); }
    dialog::backdrop { background: var(--backdrop); backdrop-filter: blur(5px); opacity: 0; transition: opacity 170ms ease, overlay 170ms allow-discrete, display 170ms allow-discrete; }
    dialog[open]::backdrop { opacity: 1; }
    @starting-style {
      dialog[open] { opacity: 0; transform: translateY(8px) scale(.985); }
      dialog[open]::backdrop { opacity: 0; }
    }
    .dialog-head { position: relative; padding: 20px 58px 15px 22px; border-bottom: 1px solid var(--line); }
    .dialog-head h2 { margin: 0; font-size: 18px; }
    .dialog-head p { margin: 4px 0 0; color: var(--muted); }
    .target-dialog-head { display: flex; align-items: center; justify-content: space-between; gap: 16px; padding: 12px 12px 12px 22px; }
    .target-dialog-head h2 { flex: none; }
    .detail-dialog-head { padding-right: 68px; }
    form { display: grid; gap: 13px; padding: 20px 22px 22px; }
    .form-tabs { display: flex; width: fit-content; min-width: 0; max-width: 100%; gap: 4px; border: 1px solid var(--line); border-radius: 11px; background: var(--nav-bg); padding: 4px; overflow-x: auto; }
    .form-tabs button { min-height: 34px; border: 0; border-radius: 7px; background: transparent; color: var(--muted); padding: 7px 11px; white-space: nowrap; cursor: pointer; transition: background-color 160ms ease, color 160ms ease; }
    .form-tabs button[aria-selected="true"] { background: var(--active-bg); color: var(--text); }
    .form-tabs button:disabled { cursor: not-allowed; opacity: .45; }
    .target-tab-panel { display: grid; gap: 13px; min-height: 190px; align-content: start; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 11px; }
    .endpoint-row { grid-template-columns: minmax(140px, 1fr) minmax(0, 2fr); }
    label { display: grid; gap: 6px; color: var(--muted); font-size: 14px; }
    [hidden] { display: none !important; }
    input, select { width: 100%; min-height: 44px; border: 1px solid var(--line); border-radius: 9px; outline: 0; background: var(--input-bg); color: var(--text); padding: 9px 10px; font-size: 16px; transition: background-color 160ms ease, border-color 160ms ease, color 160ms ease, opacity 160ms ease; }
    input:focus, select:focus { border-color: var(--focus); }
    button:focus-visible, a:focus-visible, .target:focus-visible, input:focus-visible, select:focus-visible { outline: 2px solid var(--green); outline-offset: 2px; }
    button, a, summary, [role="button"], [role="tab"], input[type="checkbox"], input[type="radio"], select, .target, .switch, .checkbox-option { cursor: pointer; user-select: none; }
    button:disabled, input:disabled, select:disabled { cursor: not-allowed; }
    input:disabled { cursor: not-allowed; opacity: .5; }
    .dialog-actions { display: flex; justify-content: flex-end; gap: 8px; margin-top: 5px; }
    .channel-test-message { margin: 5px 0 0; border: 1px solid var(--line); border-radius: 9px; background: var(--panel-2); color: var(--green); padding: 10px 12px; overflow-wrap: anywhere; white-space: normal; }
    .channel-test-message.error { border-color: var(--notice-border); background: var(--notice-bg); color: var(--notice-text); }
    .danger-actions { display: flex; gap: 8px; margin-right: auto; }
    .secondary { background: transparent; color: var(--muted); border-color: var(--line); }
    .danger { background: transparent; color: var(--danger-text); border-color: var(--danger-border); }
    .warning { background: transparent; color: var(--warning-text); border-color: var(--warning-border); }
    .warning:hover { border-color: var(--warning-text); }
    .success { background: transparent; color: var(--green); border-color: var(--green); }
    .success:hover { border-color: var(--button-text); }
    .dialog-close { position: absolute; top: 12px; right: 14px; }
    .switch { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
    .setting-copy { display: grid; gap: 3px; color: var(--text); }
    .setting-copy small { color: var(--muted); font-size: 12px; font-weight: 400; }
    .channel-fields, .tls-fields { display: grid; gap: 10px; margin: 8px 0 0; border: 0; padding: 0; }
    .tls-fields legend { display: flex; width: 100%; align-items: center; gap: 12px; margin: 0 0 4px; padding: 0; color: var(--text); font-size: 14px; font-weight: 400; text-align: center; }
    .tls-fields legend::before, .tls-fields legend::after { height: 1px; flex: 1; background: var(--line); content: ""; }
    .tls-fields .meta { white-space: normal; }
    form .badge { font-size: 12px; }
    .channel-options { display: grid; gap: 6px; }
    .channel-options .checkbox-option { min-height: 36px; border-radius: 8px; padding: 5px 8px; background: var(--panel-2); }
    .switch-label { display: flex; min-width: 0; align-items: center; gap: 8px; }
    .switch-label .badge { margin-left: 0; }
    .checkbox-option { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
    .checkbox-control { width: 18px; min-height: 18px; height: 18px; flex: none; accent-color: var(--button-bg); cursor: pointer; }
    .switch-control { width: 42px; min-height: 24px; height: 24px; flex: none; appearance: none; border-radius: 999px; background: var(--input-bg); padding: 2px; cursor: pointer; }
    .switch-control::after { display: block; width: 16px; height: 16px; border-radius: 50%; background: var(--muted); content: ""; transition: background-color 160ms ease, transform 160ms ease; }
    .switch-control:checked { border-color: var(--button-border); background: var(--button-bg); }
    .switch-control:checked::after { background: var(--button-text); transform: translateX(18px); }
    footer { display: flex; flex: 0 0 auto; width: calc(100% - 48px); max-width: 1152px; flex-direction: column; align-items: center; justify-content: center; gap: 8px; margin: 0 auto; border-top: 1px solid var(--line); padding: 20px 0 24px; color: var(--muted); font-size: 12px; }
    .footer-links, .footer-powered { display: flex; align-items: center; justify-content: center; flex-wrap: wrap; gap: 10px; text-align: center; }
    footer a { display: inline-flex; align-items: center; gap: 4px; border-radius: 4px; color: var(--muted); text-decoration: underline; text-decoration-thickness: 1px; text-underline-offset: 3px; transition: color 160ms ease; }
    footer a:hover { color: var(--text); }
    footer iconify-icon { width: 14px; height: 14px; font-size: 14px; }
    .history { margin: 0 22px 22px; border-top: 1px solid var(--line); padding-top: 18px; }
    .detail-form .details-panel { gap: 0; }
    .detail-form .history { margin: 0; }
    .detail-form .history:first-child { border-top: 0; padding-top: 0; }
    .detail-form .history + .history { margin-top: 18px; }
    .history-head, .chart-legend, .chart-legend span, .chart-axis { display: flex; align-items: center; }
    .history-head { justify-content: space-between; margin-bottom: 12px; }
    .history-head h3 { margin: 0; font-size: 14px; }
    .history-summary { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 8px; }
    .history-summary div { padding: 12px; border: 1px solid var(--line); border-radius: 9px; background: var(--input-bg); }
    .history-summary span { display: block; color: var(--muted); font-size: 10px; letter-spacing: .08em; text-transform: uppercase; }
    .history-summary strong { display: block; margin-top: 5px; font-size: 18px; font-weight: 560; }
    .chart-plot { display: grid; grid-template-columns: 38px minmax(0, 1fr); gap: 7px; }
    .chart-scale { display: flex; height: 140px; flex-direction: column; justify-content: space-between; padding: 1px 0 7px; color: var(--muted); font-size: 9px; text-align: right; }
    .history-chart { display: flex; height: 140px; align-items: flex-end; gap: 3px; padding: 14px 10px 8px; border: 1px solid var(--line); border-radius: 10px; background: var(--input-bg); }
    .history-bar { flex: 1; min-width: 3px; max-width: 16px; border-radius: 3px 3px 1px 1px; opacity: .82; transform-origin: bottom; transition: opacity 120ms ease, transform 120ms ease; }
    .history-bar:hover { opacity: 1; transform: scaleX(1.15); }
    .history-bar.up { background: var(--green); }
    .history-bar.down { background: var(--red); }
    .chart-axis { justify-content: space-between; margin: 5px 0 0 45px; color: var(--muted); font-size: 10px; }
    .chart-legend { justify-content: flex-end; gap: 12px; margin-top: 9px; color: var(--muted); font-size: 10px; }
    .chart-legend span { gap: 5px; }
    .chart-legend i { width: 7px; height: 7px; border-radius: 2px; }
    .chart-legend .up { background: var(--green); }
    .chart-legend .down { background: var(--red); }
    .join-command { margin: 20px 22px; border: 1px solid var(--line); border-radius: 10px; background: var(--join-bg); color: var(--green); padding: 13px; overflow-wrap: anywhere; font: 12px/1.6 ui-monospace, SFMono-Regular, monospace; }
    :host([data-theme="bright"]) {
        color-scheme: light;
        --bg: #f4f8f6;
        --panel: #ffffff;
        --panel-2: #eef5f1;
        --line: #d3dfd9;
        --muted: #5d6e66;
        --text: #16211c;
        --green: #087a49;
        --red: #c53434;
        --amber: #9a6700;
        --page-background:
          radial-gradient(circle at 12% -5%, #d9f2e4 0, transparent 32%),
          linear-gradient(145deg, #fbfdfc 0%, #f3f8f5 55%, #edf5f1 100%);
        --brand-shadow: #159e5240;
        --nav-bg: #ffffffcc;
        --active-bg: #e4efe9;
        --button-border: #16764b;
        --button-bg: #087a49;
        --button-text: #ffffff;
        --button-hover-border: #075f3a;
        --panel-surface: #ffffffeb;
        --panel-shadow: #2745381a;
        --divider: #e3ebe7;
        --badge-border: #a6beb2;
        --badge-text: #426356;
        --row-hover: #e9f4ee;
        --notice-border: #e2aaa6;
        --notice-bg: #fff0ef;
        --notice-text: #9f2922;
        --bulk-bg: #e8f4ed;
        --dialog-shadow: #233b3050;
        --backdrop: #17251f66;
        --input-bg: #ffffff;
        --focus: #168655;
        --danger-text: #b42318;
        --danger-border: #dda29d;
        --warning-bg: #fff1bd;
        --warning-text: #805b00;
        --warning-border: #d4aa36;
        --join-bg: #eef8f2;
    }
    @media (prefers-reduced-motion: reduce) {
      :host, nav a, .button, .metric, .panel, .target-wrap, .target, .dot, .state, .mini-bar, .history-bar, dialog, dialog::backdrop, input, select, .help-tooltip-trigger, .help-tooltip { transition-duration: 0s; }
      .bulk, .bulk-actions .button { animation-duration: 0s; }
    }
    @media (max-width: 720px) {
      .shell { padding: 20px 14px 60px; }
      header { grid-template-columns: minmax(0, 1fr) auto; row-gap: 16px; }
      header > nav { display: flex; grid-column: 1 / -1; grid-row: 2; justify-self: center; }
      .overview-top { grid-template-columns: 1fr; }
      .page-columns { grid-template-columns: 1fr; }
      .toolbar { grid-template-columns: 1fr 1fr; }
      .toolbar input { grid-column: 1 / -1; }
      .heading { align-items: flex-start; gap: 16px; }
      .target-wrap { align-items: start; padding-left: 14px; } .select-target { align-self: start; margin-top: 6px; } .target { grid-template-columns: auto minmax(0, 1fr); gap: 10px; padding: 12px 14px 12px 10px; }
      .target-side { grid-column: 2; display: grid; grid-template-columns: minmax(88px, 1fr) auto; width: 100%; gap: 18px; margin-top: 4px; } .target > .state { align-self: start; margin-top: 5px; } .mini-chart { width: 100%; max-width: 140px; height: 28px; }
      .latency { min-width: 72px; text-align: right; }
      .channel-resource { grid-template-columns: 1fr; }
      .alert-filters { grid-template-columns: 1fr 1fr; }
      .alert-resource { grid-template-columns: 1fr; }
      .alert-actions { margin-top: 8px; }
      .channel-actions { justify-content: space-between; margin-top: 10px; }
      .target-dialog-head { gap: 8px; }
      .form-tabs { gap: 0; padding: 2px; }
      .form-tabs button { min-height: 30px; padding: 5px 2px; font-size: 12px; }
    }
  `;
  protected render() {
    const up = this.targets.filter((target) => target.availability === "up").length;
    const down = this.targets.filter((target) => target.availability === "down").length;
    const pending = this.alerts.filter((alert) => alert.delivery === "pending").length;
    const health = serviceHealth(this.targets, this.live);
    const sections: Section[] = ["overview", "alerts", "cluster", "trash"];
    const visibleTargets = this.targets
      .filter((target) => `${target.name} ${target.url}`.toLowerCase().includes(this.search.toLowerCase()))
      .filter((target) => (this.statusFilter === "all" ? true : this.statusFilter === "paused" ? target.paused : target.availability === this.statusFilter))
      .sort((left, right) => (this.sort === "status" ? left.availability.localeCompare(right.availability) || left.name.localeCompare(right.name) : left.name.localeCompare(right.name)));
    const authActions: AuthActions = {
      login: (event) => void this.login(event),
      logout: () => void this.logout(),
      createIdentity: (event) => void this.createIdentity(event),
      openAddUser: () => this.showDialog("add-user-dialog"),
      closeAddUser: () => this.closeDialog("add-user-dialog"),
      openEditUser: (identity) => {
        this.editingIdentity = identity;
        void this.updateComplete.then(() => this.showDialog("edit-user-dialog"));
      },
      closeEditUser: () => {
        this.closeDialog("edit-user-dialog");
        this.editingIdentity = undefined;
      },
      openApiToken: () => this.showDialog("api-token-dialog"),
      closeApiToken: () => this.closeDialog("api-token-dialog"),
      dismissDialog: (event) => this.dismissOnBackdrop(event),
      updateIdentity: (identity, event) => void this.updateIdentity(identity, event),
      deleteIdentity: (identity) => void this.deleteIdentity(identity),
      createApiToken: (event) => void this.createApiToken(event),
      revokeApiToken: (token) => void this.revokeApiToken(token),
      dismissToken: () => (this.newApiToken = ""),
    };
    if (this.authReady && !this.setupMode && !this.session && !this.publicStatus) {
      return html`${renderLogin(this.saving, this.error, authActions)}${renderFooter()}`;
    }
    if (this.setupMode && this.setup) {
      return html`
        <main class="shell setup-shell">
          <header>
            <div class="brand">
              <img src="/favicon.svg" alt="" />
              <div><div class="brand-line"><strong>UpGrid</strong><div class="live"><i class="dot ${this.live ? "up" : ""}"></i>${this.live ? "ready" : "connecting"}</div></div><span>Distributed service monitoring</span></div>
            </div>
            <div></div>
            <div class="actions"><button class="button secondary icon-button" aria-label=${`Theme: ${this.theme}`} title=${`Theme: ${this.theme}. Click to switch.`} @click=${this.cycleTheme}><iconify-icon .icon=${themeIcons[this.theme]} aria-hidden="true"></iconify-icon></button></div>
          </header>
          ${this.error ? html`<div class="notice" role="alert">${this.error}</div>` : nothing}
          <upgrid-setup .setup=${this.setup} @setup-changed=${this.setupChanged}></upgrid-setup>
        </main>${renderFooter()}`;
    }
    if (!this.session && this.publicStatus) return this.renderPublicStatusPage(this.publicStatus.targets);
    return html`
      <main class="shell">
        <header>
          <div class="brand">
            <img src="/favicon.svg" alt="" />
            <div>
              <div class="brand-line"><strong>UpGrid</strong><div class="live"><i class="dot ${health.tone}"></i>${health.label}</div></div>
              <span>Distributed service monitoring</span>
            </div>
          </div>
          <nav aria-label="Primary">
            ${sections.map((section) => html`<a class=${this.activeSection === section ? "active" : ""} href=${sectionPaths[section]} @click=${(event: MouseEvent) => this.navigate(event, section)}>${section[0].toUpperCase()}${section.slice(1)}</a>`)}
          </nav>
          <div class="actions">
            <button class="button secondary icon-button" aria-label=${`Theme: ${this.theme}`} title=${`Theme: ${this.theme}. Click to switch.`} @click=${this.cycleTheme}><iconify-icon .icon=${themeIcons[this.theme]} aria-hidden="true"></iconify-icon></button>
            <details class="account-menu">
              <summary class="button secondary icon-button" aria-label=${`Account menu for ${this.session?.username}`} title=${`Account: ${this.session?.username}`}><iconify-icon .icon=${userIcon} aria-hidden="true"></iconify-icon></summary>
              <div class="account-dropdown" role="menu">
                <a class="button secondary" role="menuitem" href=${sectionPaths.manage} @click=${(event: MouseEvent) => this.navigate(event, "manage")}>Manage</a>
                <a class="button secondary" role="menuitem" href=${sectionPaths.changePassword} @click=${(event: MouseEvent) => this.navigate(event, "changePassword")}>Change password</a>
                <a class="button secondary" role="menuitem" href=${sectionPaths.users} @click=${(event: MouseEvent) => this.navigate(event, "users")}>Manage user</a>
                <a class="button secondary" role="menuitem" href=${sectionPaths.apiTokens} @click=${(event: MouseEvent) => this.navigate(event, "apiTokens")}>API token</a>
                <div class="account-separator" role="separator"></div>
                <button class="button danger" role="menuitem" type="button" @click=${() => void this.logout()}>Logout</button>
              </div>
            </details>
          </div>
        </header>
        ${this.error ? html`<div class="notice" role="alert">${this.error}</div>` : nothing}
        ${this.setup?.warning && !this.warningDismissed ? html`<div class="notice" role="status">${this.setup.warning}<button class="button secondary" style="float: right; margin: -6px" @click=${this.dismissWarning}>Dismiss</button></div>` : nothing}
        ${
          this.activeSection === "overview"
            ? this.renderOverview(visibleTargets, up, down, pending)
            : this.activeSection === "alerts"
              ? renderAlertsPage(
                  this.alerts,
                  this.transitions,
                  this.channels,
                  {
                    search: this.alertSearch,
                    delivery: this.alertDeliveryFilter,
                    kind: this.alertKindFilter,
                    acknowledged: this.alertAcknowledgedFilter,
                  },
                  this.saving,
                  {
                    create: () => this.openChannelDialog(),
                    edit: (channel) => this.openChannelDialog(channel),
                    remove: (channel) => void this.deleteResource("channels", channel.id, channel.name),
                    setDefault: (channel, isDefault) => void this.setChannelDefault(channel, isDefault),
                    acknowledge: (alert) => void this.acknowledgeAlert(alert),
                    retry: (alert) => void this.retryAlert(alert),
                    setSearch: (value) => (this.alertSearch = value),
                    setDelivery: (value) => (this.alertDeliveryFilter = value),
                    setKind: (value) => (this.alertKindFilter = value),
                    setAcknowledged: (value) => (this.alertAcknowledgedFilter = value),
                  },
                )
              : this.activeSection === "cluster"
                ? this.renderClusterPage()
                : this.activeSection === "trash"
                  ? this.renderTrashPage()
                  : this.activeSection === "manage"
                    ? renderManagePage(this.settings, this.saving, (event) => void this.updateSettings(event))
                    : this.activeSection === "changePassword"
                      ? renderChangePassword(
                          this.identities.find((identity) => identity.id === this.session?.identity_id),
                          this.saving,
                          authActions,
                        )
                      : this.activeSection === "users"
                        ? renderUsersPage(this.identities, this.session?.identity_id, this.editingIdentity, this.saving, authActions)
                        : renderApiTokensPage(this.apiTokens, this.newApiToken, this.saving, authActions)
        }
      </main>${renderFooter()}
      ${renderTargetForm(this.channels, this.saving, {
        backdrop: (event) => this.dismissOnBackdrop(event),
        close: () => this.closeTargetDialog(),
        create: (event) => void this.createTarget(event),
      })}
      ${
        this.selected
          ? renderTargetDetail(this.selected, this.targetHistory, this.historyLoading, this.saving, this.detailDirty, this.detailTab, this.cluster?.members ?? [], this.channels, this.secrets, {
              backdrop: (event) => this.dismissOnBackdrop(event),
              close: () => this.closeDetailDialog(),
              update: (event) => void this.updateTarget(event),
              changed: (event) => this.updateDetailDirty(event),
              redirects: (event) => this.toggleMaxRedirects(event),
              delete: () => void this.deleteTarget(),
              selectTab: (tab) => this.selectDetailTab(tab),
              pause: (paused) => void this.setPaused(paused),
            })
          : nothing
      }
      <dialog id="secret-dialog" aria-labelledby="secret-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><div class="title-with-help"><h2 id="secret-title">Add secret</h2>${renderHelpTooltip("add-secret-help", "About adding a secret", "Create an encrypted, write-only value to reference from target requests or webhook headers through the HTTP API.")}</div></div>
        <form @submit=${this.createSecret}>
          <label>Name<input name="name" placeholder="Webhook token" required autofocus /></label>
          <label>Value<input name="value" type="password" autocomplete="new-password" required /></label>
          <div class="dialog-actions"><button class="button secondary" type="button" @click=${() => this.closeDialog("secret-dialog")}>Cancel</button><button class="button" type="submit" ?disabled=${this.saving}>Create secret</button></div>
        </form>
      </dialog>
      <dialog id="channel-dialog" aria-labelledby="channel-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="channel-title">${this.editingChannel ? "Edit channel" : "Add channel"}</h2><p>${this.editingChannel ? "Update this destination without changing its channel type." : "Send transitions through Telegram, SMTP, or a generic webhook."}</p></div>
        <form @submit=${this.createChannel}>
          <label>Type<select name="type" .value=${this.channelKind} ?disabled=${this.editingChannel !== undefined} @change=${(event: Event) => {
            this.channelKind = (event.target as HTMLSelectElement).value as ChannelKind;
            this.channelTestMessage = "";
          }}><option value="webhook">Webhook</option><option value="telegram">Telegram</option><option value="smtp">SMTP email</option></select></label>
          <label>Name<input name="name" placeholder="On-call" .value=${this.editingChannel?.name ?? ""} required /></label>
          ${renderChannelFields(this.channelKind, this.editingChannel)}
          <label class="switch"><span>Default channel</span><input class="switch-control" name="default" type="checkbox" role="switch" .checked=${this.editingChannel?.default ?? false} /></label>
          ${this.channelTestMessage ? html`<p class=${`channel-test-message${this.channelTestMessage.startsWith("Test failed:") ? " error" : ""}`} role="status">${this.channelTestMessage}</p>` : nothing}
          <div class="dialog-actions"><button class="button secondary" type="button" @click=${() => {
            this.editingChannel = undefined;
            this.closeDialog("channel-dialog");
          }}>Cancel</button>${this.editingChannel ? nothing : html`<button class="button secondary" type="button" aria-busy=${this.testingChannel} ?disabled=${this.testingChannel || this.saving} @click=${this.testChannel}>${this.testingChannel ? "Sending…" : "Send test"}</button>`}<button class="button" type="submit" ?disabled=${this.saving || this.testingChannel}>${this.editingChannel ? "Save changes" : "Create channel"}</button></div>
        </form>
      </dialog>
      <dialog id="token-config-dialog" aria-labelledby="token-config-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="token-config-title">Create join token</h2><p>Choose how many days the token remains valid and whether it can be reused.</p></div>
        <form @submit=${this.createJoinToken}>
          <label>Expiration (days)<input name="expiration_days" type="number" min="1" step="1" value="1" required /></label>
          <label class="switch"><span>Unlimited uses</span><input class="switch-control" type="checkbox" role="switch" .checked=${this.unlimitedUses} @change=${(event: Event) => (this.unlimitedUses = (event.target as HTMLInputElement).checked)} /></label>
          <label>Maximum uses<input name="max_uses" type="number" min="1" step="1" value="1" ?disabled=${this.unlimitedUses} required /></label>
          <div class="dialog-actions"><button class="button secondary" type="button" @click=${() => this.closeDialog("token-config-dialog")}>Cancel</button><button class="button" type="submit" ?disabled=${this.saving}>${this.saving ? "Creating…" : "Create token"}</button></div>
        </form>
      </dialog>
      <dialog id="join-dialog" aria-labelledby="join-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="join-title">Join token created</h2><p>This command contains cluster credentials. Revoke the token when no longer needed.</p></div>
        <div class="join-command">${this.joinCommand}</div>
        <div class="dialog-actions" style="padding: 0 22px 22px"><button class="button secondary" @click=${() => this.closeDialog("join-dialog")}>Close</button><button class="button" @click=${this.copyJoinCommand}>${this.copied ? "Copied" : "Copy command"}</button></div>
      </dialog>
    `;
  }
  private renderPublicStatusPage(targets: PublicStatusTarget[]) {
    const up = targets.filter((target) => target.availability === "up" && !target.paused).length;
    const down = targets.filter((target) => target.availability === "down" && !target.paused).length;
    const paused = targets.filter((target) => target.paused).length;
    const health = serviceHealth(targets, this.live);
    return html`
      <main class="shell">
        <header>
          <div class="brand">
            <img src="/favicon.svg" alt="" />
            <div>
              <div class="brand-line"><strong>UpGrid</strong><div class="live"><i class="dot ${health.tone}"></i>${health.label}</div></div>
              <span>Distributed service monitoring</span>
            </div>
          </div>
          <nav aria-label="Primary"><a class="active" href="/">Status</a></nav>
          <div class="actions">
            <button class="button secondary icon-button" aria-label=${`Theme: ${this.theme}`} title=${`Theme: ${this.theme}. Click to switch.`} @click=${this.cycleTheme}><iconify-icon .icon=${themeIcons[this.theme]} aria-hidden="true"></iconify-icon></button>
            <button class="button secondary" type="button" @click=${this.showLogin}>Sign in</button>
          </div>
        </header>
        <section class="heading">
          <div><span class="eyebrow">Public status</span><h1>Status</h1></div>
        </section>
        <section class="summary" aria-label="Target summary">
          <div class="metric"><span>Targets</span><strong>${targets.length}</strong></div>
          <div class="metric"><span>Up</span><strong>${up}</strong></div>
          <div class=${`metric down ${down ? "active" : ""}`}><span>Down</span><strong>${down}</strong></div>
          <div class="metric"><span>Paused</span><strong>${paused}</strong></div>
        </section>
        <section class="panel" aria-label="Public target status" style="margin-top: 18px">
          <div class="panel-head"><h2>Targets</h2><span class="meta">${targets.length} monitored</span></div>
          ${
            targets.length
              ? targets.map((target) => {
                  const latest = target.latest_evaluation;
                  const state = target.paused ? "paused" : target.availability === "down" ? "down" : target.consecutive_failures > 0 ? "suspicious" : target.availability;
                  const detail = target.paused ? "Paused" : latest ? `${latest.latency_ms} ms · ${latest.status_code ?? (latest.succeeded ? "reachable" : "unreachable")}` : "Waiting for an evaluation";
                  return html`<div class="resource"><div><strong>${target.name}</strong><code>${target.kind.toUpperCase()} · ${detail}</code></div><span class=${`badge ${state}`}>${state}</span></div>`;
                })
              : html`<div class="empty">No targets are configured.</div>`
          }
        </section>
      </main>${renderFooter()}`;
  }

  private renderOverview(visibleTargets: Target[], up: number, down: number, pending: number) {
    const selectedTargets = this.targets.filter((target) => this.selectedIds.has(target.id));
    const canPauseSelected = selectedTargets.some((target) => !target.paused);
    const canResumeSelected = selectedTargets.some((target) => target.paused);
    const unusedSecrets = this.secrets.filter((secret) => !secret.referenced);
    return html`
      <section class="heading" id="overview">
        <div><span class="eyebrow">Cluster status</span><h1>Overview</h1></div>
        <button class="button" @click=${this.openTargetDialog}>Add target</button>
      </section>
      <section class="overview-top">
        <section class="summary" aria-label="Target summary">
          <div class="metric"><span>Targets</span><strong>${this.targets.length}</strong></div>
          <div class="metric"><span>Pending alerts</span><strong>${pending}</strong></div>
          <div class="metric"><span>Up</span><strong>${up}</strong></div>
          <div class=${`metric down ${down ? "active" : ""}`}><span>Down</span><strong>${down}</strong></div>
        </section>
        <section class="panel" aria-label="Secrets">
          <div class="panel-head"><div class="title-with-help"><h2>Secrets</h2>${renderHelpTooltip("secrets-help", "About reusable secrets", "Reusable secrets are encrypted and write-only. Reference them from target headers or bodies and webhook headers or other notification channel credentials. UpGrid reports whether each secret is referenced by an active or trashed target or a notification channel.")}</div><div class="actions">${unusedSecrets.length ? html`<button class="button danger" ?disabled=${this.saving} @click=${() => this.cleanupSecrets()}>Delete unused (${unusedSecrets.length})</button>` : nothing}<button class="button secondary" @click=${() => this.showDialog("secret-dialog")}>Add secret</button></div></div>
          ${
            this.secrets.length
              ? this.secrets.map(
                  (secret) =>
                    html`<div class="resource"><div><strong>${secret.name}</strong><code>${secret.id} · ${secret.referenced ? "In use" : "Unused"}</code></div><button class="button danger icon-button" aria-label=${`Delete secret ${secret.name}`} title=${`Delete ${secret.name}`} @click=${() => this.deleteResource("secrets", secret.id, secret.name)}><iconify-icon .icon=${deleteIcon} aria-hidden="true"></iconify-icon></button></div>`,
                )
              : html`<div class="empty">No reusable secrets.</div>`
          }
        </section>
      </section>
      <section class="panel" aria-label="Targets">
        <div class="panel-head"><h2>Targets</h2><span class="meta">${this.targets.length} configured</span></div>
        <div class="toolbar">
          <input aria-label="Search targets" type="search" placeholder="Search name or URL" .value=${this.search} @input=${(event: Event) => (this.search = (event.target as HTMLInputElement).value)} />
          <select aria-label="Filter targets" .value=${this.statusFilter} @change=${(event: Event) => (this.statusFilter = (event.target as HTMLSelectElement).value)}><option value="all">All states</option><option value="up">Up</option><option value="down">Down</option><option value="unknown">Unknown</option><option value="paused">Paused</option></select>
          <select aria-label="Sort targets" .value=${this.sort} @change=${(event: Event) => (this.sort = (event.target as HTMLSelectElement).value)}><option value="name">Sort by name</option><option value="status">Sort by status</option></select>
        </div>
        ${this.selectedIds.size ? html`<div class="bulk"><span class="meta">${this.selectedIds.size} selected</span><div class="bulk-actions"><button class="button secondary icon-button" aria-label="Unselect all" title="Unselect all" @click=${() => (this.selectedIds = new Set())}><iconify-icon .icon=${closeIcon} aria-hidden="true"></iconify-icon></button>${canPauseSelected ? html`<button class="button warning icon-button" aria-label="Pause selected" title="Pause selected" @click=${() => this.bulkPause(true)}><iconify-icon .icon=${pauseIcon} aria-hidden="true"></iconify-icon></button>` : nothing}${canResumeSelected ? html`<button class="button success icon-button" aria-label="Resume selected" title="Resume selected" @click=${() => this.bulkPause(false)}><iconify-icon .icon=${playIcon} aria-hidden="true"></iconify-icon></button>` : nothing}<button class="button danger icon-button" aria-label="Delete selected" title="Delete selected" @click=${this.bulkDelete}><iconify-icon .icon=${deleteIcon} aria-hidden="true"></iconify-icon></button></div></div>` : nothing}
        ${visibleTargets.length ? visibleTargets.map((target) => this.renderTarget(target)) : html`<div class="empty">${this.targets.length ? "No targets match these filters." : "No targets yet. Add the first one to begin monitoring."}</div>`}
      </section>
    `;
  }

  private renderTrashPage() {
    return html`
      <section class="heading" id="trash">
        <div><span class="eyebrow">Recover deleted monitors</span><h1>Trash</h1></div>
      </section>
      <section class="panel" aria-label="Trashed targets">
        <div class="panel-head"><div class="title-with-help"><h2>Deleted targets</h2>${renderHelpTooltip("trash-retention-help", "About deleted target retention", "Settings and history remain recoverable until the retention deadline.")}</div><span class="meta">${this.trashedTargets.length} stored</span></div>
        ${this.trashedTargets.length ? this.trashedTargets.map((target) => this.renderTrashedTarget(target)) : html`<div class="empty">Trash is empty.</div>`}
      </section>
    `;
  }

  private renderTrashedTarget(target: TrashedTarget) {
    return html`
      <div class="resource">
        <div>
          <strong>${target.name}</strong>
          <code>${target.kind.toUpperCase()} · deleted ${new Date(target.deleted_at_ms).toLocaleString()} · permanently deleted ${new Date(target.purge_at_ms).toLocaleString()}</code>
        </div>
        <div class="actions">
          <button class="button secondary" ?disabled=${this.saving} @click=${() => this.restoreTarget(target)}>Restore</button>
          <button class="button danger" ?disabled=${this.saving} @click=${() => this.purgeTarget(target)}>Delete permanently</button>
        </div>
      </div>
    `;
  }

  private renderClusterMember(member: ClusterMember) {
    return html`
      <div class="resource">
        <div>
          <strong>${member.name}</strong>
          <code>${member.raft_url} · ${member.active_assignments} active assignments</code>
        </div>
        <div class="actions">
          ${member.local ? html`<span class="badge">This node</span>` : nothing}
          ${member.leader ? html`<span class="badge">Leader</span>` : nothing}
          ${member.draining ? html`<span class="badge">Draining</span>` : nothing}
          ${
            member.local
              ? nothing
              : html`
                <button class="button secondary" ?disabled=${this.saving} @click=${() => this.setNodeDrain(member, !member.draining)}>${member.draining ? "Cancel drain" : "Drain"}</button>
                ${member.draining && member.active_assignments === 0 ? html`<button class="button danger" ?disabled=${this.saving} @click=${() => this.removeNode(member, false)}>Remove</button>` : nothing}
                <button class="button danger" ?disabled=${this.saving} @click=${() => this.removeNode(member, true)}>Replace failed</button>
              `
          }
        </div>
      </div>
    `;
  }

  private renderClusterPage() {
    return html`
      <section class="heading" id="cluster">
        <div><span class="eyebrow">Raft membership</span><h1>Cluster</h1></div>
        <div class="actions">
          <button class="button" @click=${this.openTokenDialog}>Create token</button>
        </div>
      </section>
      <div class="page-columns">
      <section class="panel" aria-label="Cluster topology">
        <div class="panel-head"><div class="title-with-help"><h2>Nodes</h2>${renderHelpTooltip("nodes-removal-help", "About removing nodes", "Drain healthy nodes before removal. Replace failed nodes only after confirming the old process is permanently stopped.")}</div><span class="meta">${this.cluster?.members.length ?? 0} members</span></div>
        ${this.cluster?.members.map((member) => this.renderClusterMember(member))}
        ${this.cluster?.members.length ? nothing : html`<div class="empty">Cluster topology unavailable.</div>`}
      </section>
      <section class="panel" aria-label="Join tokens">
        <div class="panel-head"><h2>Join tokens</h2><span class="meta">${this.joinTokens.length} stored</span></div>
        ${
          this.joinTokens.length
            ? this.joinTokens.map(
                (token) => html`
              <div class="resource">
                <div><strong>${token.id.slice(0, 12)}…</strong><code>Expires ${new Date(token.expires_at_ms).toLocaleString()} · ${token.remaining_uses === null ? "unlimited uses" : `${token.remaining_uses} uses left`}</code></div>
                <button class="button danger" aria-label=${`Revoke join token ${token.id.slice(0, 12)}`} @click=${() => this.revokeJoinToken(token)}>Revoke</button>
              </div>
            `,
              )
            : html`<div class="empty">No join tokens.</div>`
        }
      </section>
      </div>
    `;
  }

  private renderTarget(target: Target) {
    const isNode = target.kind === "node";
    const isHttp = target.kind === "http";
    const latest = target.latest_evaluation;
    const history = target.history.slice(0, 16).reverse();
    const maxLatency = Math.max(1, ...history.map((item) => item.latency_ms));
    const state = target.paused ? "paused" : target.availability === "down" ? "down" : target.consecutive_failures > 0 ? "suspicious" : target.availability;
    return html`
      <div class="target-wrap">
        ${
          !isNode
            ? html`<input class="select-target" type="checkbox" aria-label=${`Select ${target.name}`} .checked=${this.selectedIds.has(target.id)} @change=${(event: Event) => this.toggleSelected(target.id, (event.target as HTMLInputElement).checked)} />`
            : html`<input class="select-target" type="checkbox" aria-label=${`Select ${target.name}`} disabled />`
        }
        <button class=${`target ${isNode ? "node-target" : ""}`} aria-label=${target.name} @click=${() => this.openTarget(target)}>
          <i class="state ${state}" aria-label=${state}></i>
          <div>
            <div class="target-title"><h3>${target.name}</h3><span class="badge">${isNode ? "Node" : target.kind.toUpperCase()}</span></div>
            <div class="meta">${target.paused ? "Paused · " : ""}${isHttp || isNode ? `${target.method} · ` : ""}${target.url} · every ${target.interval_seconds}s${isNode ? "" : ` · ${target.locations} ${target.locations === 1 ? "location" : "locations"}`}</div>
          </div>
          <div class="target-side">
            ${history.length ? html`<div class="mini-chart" aria-hidden="true">${history.map((item) => html`<i class="mini-bar ${item.succeeded ? "up" : "down"}" style=${`height: ${Math.max(12, (item.latency_ms / maxLatency) * 100)}%`}></i>`)}</div>` : nothing}
            <div class="latency">
              <strong>${latest ? `${latest.latency_ms} ms` : "—"}</strong>
              <span>${latest ? (isHttp ? (latest.status_code ?? "network error") : latest.succeeded ? "reachable" : "unreachable") : "waiting"}</span>
            </div>
          </div>
        </button>
      </div>
    `;
  }
}
declare global {
  interface HTMLElementTagNameMap {
    "upgrid-app": UpgridApp;
  }
}
