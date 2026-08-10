# UpGrid TODO

The MVP is complete. This file records unfinished work for future agile iterations; ordering is provisional until an item is selected.

## Near-Term Hardening

- [ ] Replace the static shared administrator username and password with replicated identities, API tokens or sessions, password hashing, and role-based authorization.
- [ ] Add safe Node drain, membership removal, and failed-Node replacement workflows.
- [ ] Support editing Notification Channels and sending explicit test deliveries.
- [ ] Add alert acknowledgement, manual delivery retry, and richer alert-history filters.

## Monitoring Capabilities

- [ ] Add TCP-connect, DNS, ICMP, and TLS-certificate Target types.
- [ ] Add regex, JSONPath, response-header, latency-threshold, and scripted HTTP assertions.
- [ ] Support custom HTTPS CA bundles and mutual-TLS Target credentials.
- [ ] Evaluate cron or calendar schedules after fixed-interval usage is understood.
- [ ] Add optional multi-location evaluation and result aggregation.

## Data Lifecycle

- [ ] Add long-term history aggregation and external archival.
- [ ] Add configurable trash or restore behavior for deleted Targets.
- [ ] Provide safe discovery and cleanup of unreferenced Secrets.

## Iteration Policy

Before starting an item, define its acceptance criteria and move only that coherent slice into the active iteration. Remove completed items after their implementation and acceptance evidence are committed.
