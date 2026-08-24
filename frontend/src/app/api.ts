export interface Evaluation {
  scheduled_at_ms: number;
  recorded_at_ms: number;
  executor_node_id: string;
  succeeded: boolean;
  status_code: number | null;
  latency_ms: number;
  diagnostic: string | null;
}

export interface EvaluationRollup {
  bucket_start_ms: number;
  bucket_end_ms: number;
  samples: number;
  successes: number;
  failures: number;
  availability_percent: number;
  latency_total_ms: number;
  latency_average_ms: number;
  latency_min_ms: number;
  latency_max_ms: number;
}

export interface HistoryPage {
  items: EvaluationRollup[];
  next_cursor_ms: number | null;
}

export type HttpAssertion = { kind: "body_contains"; value: string } | { kind: "body_regex"; pattern: string } | { kind: "json_path"; path: string; expected: string | null } | { kind: "response_header"; name: string; value: string | null } | { kind: "latency"; max_ms: number } | { kind: "script"; source: string };

export type TargetKind = "http" | "tcp" | "dns" | "icmp" | "tls";

export interface Target {
  id: string;
  kind: TargetKind | "node";
  name: string;
  url: string;
  method: string;
  headers: Record<string, ConfigValue>;
  body: ConfigValue | null;
  accepted_statuses: Array<{ start: number; end: number }>;
  follow_redirects: boolean;
  max_redirects: number;
  assertions: HttpAssertion[];
  skip_tls_verification: boolean;
  tls_ca_secret_id: string | null;
  tls_client_certificate_secret_id: string | null;
  tls_client_private_key_secret_id: string | null;
  availability: "unknown" | "up" | "down";
  consecutive_failures: number;
  interval_seconds: number;
  timeout_seconds: number;
  failure_threshold: number;
  locations: number;
  latest_evaluation: Evaluation | null;
  history: Evaluation[];
  notification_channel_ids: string[];
  use_default_channels: boolean;
  paused: boolean;
}

export interface PublicEvaluation {
  scheduled_at_ms: number;
  succeeded: boolean;
  status_code: number | null;
  latency_ms: number;
}

export interface PublicStatusTarget {
  kind: TargetKind | "node";
  name: string;
  availability: "unknown" | "up" | "down";
  consecutive_failures: number;
  latest_evaluation: PublicEvaluation | null;
  paused: boolean;
}

export interface PublicStatus {
  targets: PublicStatusTarget[];
}

export interface ManageSettings {
  public_status_enabled: boolean;
}

export interface TrashedTarget extends Target {
  deleted_at_ms: number;
  purge_at_ms: number;
}

export type ConfigValue = { kind: "literal"; value: string } | { kind: "secret"; secret_id: string };

export interface Channel {
  id: string;
  name: string;
  kind: "telegram" | "webhook" | "smtp";
  destination: string;
  headers: Record<string, ConfigValue>;
  port?: number;
  security?: "none" | "start_tls" | "tls";
  username?: string;
  from?: string;
  to?: string;
  default: boolean;
}

export interface Alert {
  target_id: string;
  channel_id: string;
  kind: "down" | "recovered";
  target_name: string;
  channel_name: string;
  scheduled_at_ms: number;
  delivery: "pending" | "delivered" | "failed";
  attempts: number;
  next_attempt_at_ms: number | null;
  completed_at_ms: number | null;
  diagnostic: string | null;
  acknowledged_at_ms: number | null;
}

export interface AvailabilityTransition {
  target_id: string;
  kind: "down" | "recovered";
  target_name: string;
  scheduled_at_ms: number;
}

export interface Secret {
  id: string;
  name: string;
  referenced: boolean;
}

export interface SecretCleanup {
  deleted_ids: string[];
}
export interface JoinLink {
  id: string;
  url: string;
  expires_at_ms: number;
  remaining_uses: number | null;
}

export interface JoinToken {
  id: string;
  expires_at_ms: number;
  remaining_uses: number | null;
}

export interface ClusterMember {
  id: string;
  name: string;
  reachable_addresses: string[];
  leader: boolean;
  local: boolean;
  draining: boolean;
  active_assignments: number;
}

export interface LocalAddress {
  host: string;
  port: number;
}

export interface Setup {
  setup: boolean;
  phase: "cluster" | "channel" | "target" | "complete";
  path: string;
  cluster_ready: boolean;
  node_name: string;
  warning: string | null;
  local_addresses: LocalAddress[];
  reachable_addresses: string[];
  discovery_urls: string[];
  channel_count: number;
  target_count: number;
}

export interface Session {
  identity_id: string;
  username: string;
  expires_at_ms: number;
  refresh_after_ms: number | null;
}

export interface Identity {
  id: string;
  username: string;
  created_at_ms: number;
}

export interface ApiToken {
  id: string;
  identity_id: string;
  name: string;
  created_at_ms: number;
  expires_at_ms: number | null;
}

export interface CreatedApiToken extends ApiToken {
  value: string;
}

export interface Cluster {
  leader_node_id: string | null;
  local_node_id: string;
  members: ClusterMember[];
  degraded: boolean;
  connectivity_failures: Array<{ source_node_id: string; destination_node_id: string }>;
}

export interface TargetInput {
  name: string;
  kind: TargetKind;
  url: string;
  method: string;
  accepted_statuses: Array<{ start: number; end: number }>;
  follow_redirects: boolean;
  max_redirects: number;
  interval_seconds: number;
  timeout_seconds: number;
  failure_threshold: number;
  locations: number;
  headers: Record<string, string | { secret_id: string }>;
  body: string | { secret_id: string } | null;
  assertions: HttpAssertion[];
  skip_tls_verification: boolean;
  tls_ca_secret_id: string | null;
  tls_client_certificate_secret_id: string | null;
  tls_client_private_key_secret_id: string | null;
  notification_channel_ids: string[];
  use_default_channels: boolean;
}

export class ApiRequestError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = "ApiRequestError";
  }
}

export const sessionExpiredEvent = "upgrid-session-expired";

let sessionRefresh: Promise<Session> | undefined;
let sessionEnding = false;

async function responseBody<T>(response: Response): Promise<T> {
  if (!response.ok) {
    const body = await response.json().catch(() => ({ error: response.statusText }));
    throw new ApiRequestError(response.status, body.error || response.statusText);
  }
  return response.status === 204 ? (undefined as T) : response.json();
}

export function refreshBrowserSession(): Promise<Session> {
  if (sessionEnding) return Promise.reject(new ApiRequestError(401, ""));
  if (!sessionRefresh) {
    sessionRefresh = fetch("/api/v1/auth/session")
      .then((response) => responseBody<Session>(response))
      .finally(() => {
        sessionRefresh = undefined;
      });
  }
  return sessionRefresh;
}

export async function logoutBrowserSession(): Promise<void> {
  sessionEnding = true;
  try {
    await sessionRefresh?.catch(() => undefined);
    await responseBody<void>(await fetch("/api/v1/auth/logout", { method: "POST" }));
  } finally {
    sessionEnding = false;
  }
}

function expireSession(): ApiRequestError {
  window.dispatchEvent(new Event(sessionExpiredEvent));
  return new ApiRequestError(401, "");
}

export async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const fetchRequest = () =>
    fetch(path, {
      ...init,
      headers: {
        ...(init?.body ? { "content-type": "application/json" } : {}),
        ...init?.headers,
      },
    });
  let response = await fetchRequest();
  if (response.status === 401 && !path.startsWith("/api/v1/auth/")) {
    await response.body?.cancel();
    try {
      await refreshBrowserSession();
    } catch {
      throw expireSession();
    }
    response = await fetchRequest();
    if (response.status === 401) {
      await response.body?.cancel();
      throw expireSession();
    }
  }
  return responseBody<T>(response);
}
