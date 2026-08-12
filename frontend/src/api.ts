export interface Evaluation {
  scheduled_at_ms: number;
  recorded_at_ms: number;
  executor_node_id: string;
  succeeded: boolean;
  status_code: number | null;
  latency_ms: number;
  diagnostic: string | null;
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

export interface Transition {
  target_id: string;
  kind: "down" | "recovered";
  target_name: string;
  scheduled_at_ms: number;
}

export interface Secret {
  id: string;
  name: string;
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
  raft_url: string;
  leader: boolean;
  local: boolean;
  draining: boolean;
  active_assignments: number;
}

export interface Setup {
  setup: boolean;
  phase: "cluster" | "channel" | "target" | "complete";
  path: string;
  cluster_ready: boolean;
  node_name: string;
  warning: string | null;
  channel_count: number;
  target_count: number;
}

export interface Session {
  identity_id: string;
  username: string;
  expires_at_ms: number;
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

export async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, {
    ...init,
    headers: {
      ...(init?.body ? { "content-type": "application/json" } : {}),
      ...init?.headers,
    },
  });
  if (!response.ok) {
    const body = await response.json().catch(() => ({ error: response.statusText }));
    throw new ApiRequestError(response.status, body.error || response.statusText);
  }
  return response.status === 204 ? (undefined as T) : response.json();
}
