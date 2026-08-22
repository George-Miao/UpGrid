import type { HttpAssertion, TargetInput, TargetKind } from "@/app/api.ts";

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

function acceptedStatuses(fields: FormData): Array<{ start: number; end: number }> {
  return String(fields.get("statuses") ?? "200-299")
    .split(",")
    .map((part) => {
      const [startText, endText] = part.trim().split("-");
      const start = Number(startText);
      return { start, end: endText === undefined ? start : Number(endText) };
    });
}

export function targetInput(fields: FormData, notificationChannelIds: string[] = [], useDefaultChannels = true, kind = String(fields.get("kind") ?? "http") as TargetKind, assertions: HttpAssertion[] = []): TargetInput {
  const endpoint = String(fields.get("url"));
  const isHttp = kind === "http";
  const followRedirects = !isHttp || fields.get("follow_redirects") === "on";
  const url = isHttp ? endpoint : `${kind}://${endpoint.replace(/^[a-z][a-z0-9+.-]*:\/\//i, "")}`;
  return {
    name: String(fields.get("name")),
    kind,
    url,
    method: String(fields.get("method") ?? "GET"),
    accepted_statuses: isHttp ? acceptedStatuses(fields) : [{ start: 200, end: 299 }],
    follow_redirects: followRedirects,
    max_redirects: isHttp ? (followRedirects ? Number(fields.get("max_redirects") ?? 5) : 0) : 5,
    interval_seconds: Number(fields.get("interval")),
    timeout_seconds: Number(fields.get("timeout")),
    failure_threshold: Number(fields.get("failures")),
    locations: Number(fields.get("locations") ?? 1),
    headers: {},
    body: null,
    assertions,
    skip_tls_verification: isHttp && fields.get("skip_tls_verification") === "on",
    tls_ca_secret_id: optionalId(fields, "tls_ca_secret_id"),
    tls_client_certificate_secret_id: optionalId(fields, "tls_client_certificate_secret_id"),
    tls_client_private_key_secret_id: optionalId(fields, "tls_client_private_key_secret_id"),
    notification_channel_ids: notificationChannelIds,
    use_default_channels: useDefaultChannels,
  };
}

function optionalId(fields: FormData, name: string): string | null {
  const value = String(fields.get(name) ?? "");
  return value || null;
}
