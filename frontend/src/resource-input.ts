import { type TargetInput } from "./api.ts";

export type ChannelKind = "webhook" | "telegram";

export function channelInput(fields: FormData, kind: ChannelKind) {
  return kind === "telegram"
    ? {
        type: "telegram",
        name: fields.get("name"),
        bot_token: fields.get("bot_token"),
        chat_id: fields.get("chat_id"),
      }
    : {
        type: "webhook",
        name: fields.get("name"),
        url: fields.get("url"),
        headers: {},
      };
}

export function targetInput(
  fields: FormData,
  notificationChannelIds: string[] = [],
): TargetInput {
  return {
    name: String(fields.get("name")),
    url: String(fields.get("url")),
    method: String(fields.get("method")),
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
  };
}
