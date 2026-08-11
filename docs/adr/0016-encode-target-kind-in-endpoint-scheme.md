# Encode Target kind in the endpoint scheme

UpGrid's replicated `Target` was originally serialized as an HTTP configuration inside Raft log entries, state-machine checkpoints, and snapshots. Replacing that field with a newly tagged enum would make existing durable state undecodable or require duplicating every historical state version solely to migrate the nested type.

Target kind is therefore encoded by the endpoint URL scheme. `http` and `https` identify HTTP Targets; `tcp`, `dns`, `icmp`, and `tls` identify the corresponding network Target. The domain exposes a typed `TargetKind`, validates the endpoint shape and allowed configuration for that kind, and prevents non-HTTP Targets from carrying HTTP request options. The API and WebUI present the kinds explicitly rather than asking operators to infer them from the scheme.

The persisted envelope remains byte-compatible with existing HTTP Targets. New network Targets use the same envelope with default HTTP-only fields, so old snapshots continue to decode without a state-version migration. This intentionally keeps persistence representation separate from the operator-facing model; probe dispatch and API responses must use the validated `TargetKind`, not inspect arbitrary request fields.
