# UpGrid TODO

The MVP is complete. This file records unfinished work for future agile iterations. Ordered from higher to lower priority.

## Active Iteration

- [ ] Operator identities and their password verifiers are replicated; new Clusters create the first administrator during setup and existing Clusters migrate their configured administrator once.
- [ ] Login issues short-lived signed JWT sessions, protected routes accept JWTs or revocable API tokens, and Basic authentication no longer guards a running Cluster.
- [ ] Administrators can manage identities and API tokens through the HTTP API and WebUI; domain, API, migration, and browser tests cover authentication, revocation, and secret redaction.

## Backlog

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
