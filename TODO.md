# UpGrid TODO

The MVP is complete. This file records unfinished work for future agile iterations. Ordered from higher to lower priority.

## Active Iteration

- [x] SMTP Notification Channels support plaintext, STARTTLS, and implicit TLS delivery with optional username and encrypted password.
- [x] Operators can create, test, and edit SMTP Channels in the WebUI; omitted passwords preserve the managed Secret.
- [x] Domain, API, notification, and browser tests verify SMTP configuration, delivery, editing, and failures against a local SMTP fixture.

## Backlog

- [ ] Add TCP-connect, DNS, ICMP, and TLS-certificate Target types.
- [ ] Replace the static shared administrator username and password with replicated identities, API tokens and JWT.
- [ ] Add safe Node drain, membership removal, and failed-Node replacement workflows.
- [ ] Add alert acknowledgement, manual delivery retry, and richer alert-history filters.
- [ ] Add regex, JSONPath, response-header, latency-threshold, and scripted HTTP assertions.
- [ ] Support custom HTTPS CA bundles and mutual-TLS Target credentials.
- [ ] Add optional multi-location evaluation and result aggregation.
- [ ] Add long-term history aggregation and external archival.
- [ ] Add configurable trash or restore behavior for deleted Targets.
- [ ] Provide safe discovery and cleanup of unreferenced Secrets.
- [ ] Evaluate cron or calendar schedules after fixed-interval usage is understood.

## Iteration Policy

Before starting an item, define its acceptance criteria and move only that coherent slice into the active iteration. Remove completed items after their implementation and acceptance evidence are committed.
