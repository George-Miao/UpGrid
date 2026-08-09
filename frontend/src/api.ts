export interface Evaluation {
  scheduled_at_ms: number;
  recorded_at_ms: number;
  executor_node_id: string;
  succeeded: boolean;
  status_code: number | null;
  latency_ms: number;
  diagnostic: string | null;
}

export interface Target {
  id: string;
  name: string;
  url: string;
  method: string;
  headers: Record<string, ConfigValue>;
  body: ConfigValue | null;
  accepted_statuses: Array<{ start: number; end: number }>;
  follow_redirects: boolean;
  max_redirects: number;
  body_contains: string | null;
  skip_tls_verification: boolean;
  availability: "unknown" | "up" | "down";
  consecutive_failures: number;
  interval_seconds: number;
  timeout_seconds: number;
  failure_threshold: number;
  latest_evaluation: Evaluation | null;
  history: Evaluation[];
  notification_channel_ids: string[];
  paused: boolean;
}

export type ConfigValue =
  | { kind: "literal"; value: string }
  | { kind: "secret"; secret_id: string };

export interface Channel {
  id: string;
  name: string;
  kind: "telegram" | "webhook";
  destination: string;
}

export interface Alert {
  target_id: string;
  channel_id: string;
  kind: "down" | "recovered";
  target_name: string;
  scheduled_at_ms: number;
  delivery: "pending" | "delivered" | "failed";
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

export interface Cluster {
  leader_node_id: string | null;
  local_node_id: string;
  members: ClusterMember[];
}

export interface TargetInput {
  name: string;
  url: string;
  method: string;
  accepted_statuses: Array<{ start: number; end: number }>;
  follow_redirects: boolean;
  max_redirects: number;
  interval_seconds: number;
  timeout_seconds: number;
  failure_threshold: number;
  headers: Record<string, string | { secret_id: string }>;
  body: string | { secret_id: string } | null;
  body_contains: string | null;
  skip_tls_verification: boolean;
  notification_channel_ids: string[];
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
    throw new Error(body.error || response.statusText);
  }
  return response.status === 204 ? (undefined as T) : response.json();
}
