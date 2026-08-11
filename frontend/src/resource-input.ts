import type { TargetInput, TargetKind } from "./api.ts";

export type ChannelKind = "webhook" | "telegram" | "smtp";

export function channelInput(fields: FormData, kind: ChannelKind, update = false) {
  if (kind === "telegram") {
    const botToken = String(fields.get("bot_token") ?? "");
    return {
      type: "telegram",
      name: fields.get("name"),
      bot_token: update && !botToken ? undefined : botToken,
      chat_id: fields.get("chat_id"),
      default: fields.get("default") === "on",
    };
  }
  if (kind === "smtp") {
    const username = String(fields.get("username") ?? "");
    const password = String(fields.get("password") ?? "");
    return {
      type: "smtp",
      name: fields.get("name"),
      host: fields.get("host"),
      port: Number(fields.get("port")),
      security: fields.get("security"),
      username: username || undefined,
      password: password || undefined,
      from: fields.get("from"),
      to: fields.get("to"),
      default: fields.get("default") === "on",
    };
  }
  return {
    type: "webhook",
    name: fields.get("name"),
    url: fields.get("url"),
    headers: update ? undefined : {},
    default: fields.get("default") === "on",
  };
}

export function targetInput(fields: FormData, notificationChannelIds: string[] = [], useDefaultChannels = true, kind = String(fields.get("kind") ?? "http") as TargetKind): TargetInput {
  const endpoint = String(fields.get("url"));
  const url = kind === "http" ? endpoint : `${kind}://${endpoint.replace(/^[a-z][a-z0-9+.-]*:\/\//i, "")}`;
  return {
    name: String(fields.get("name")),
    kind,
    url,
    method: String(fields.get("method") ?? "GET"),
    accepted_statuses: [{ start: 200, end: 299 }],
    follow_redirects: true,
    max_redirects: 5,
    interval_seconds: Number(fields.get("interval")),
    timeout_seconds: Number(fields.get("timeout")),
    failure_threshold: Number(fields.get("failures")),
    headers: {},
    body: null,
    body_contains: null,
    skip_tls_verification: false,
    notification_channel_ids: notificationChannelIds,
    use_default_channels: useDefaultChannels,
  };
}
