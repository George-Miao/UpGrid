# UpGrid TODO

The MVP is complete. This file records unfinished work for future agile iterations. Ordered from higher to lower priority.

## Active Iteration

- [ ] HTTPS Targets reference Secret-backed custom CA bundles and complete client-certificate/private-key pairs; validation rejects non-HTTPS use, missing Secrets, partial identities, and insecure-verification combinations before replication, with backward-compatible state migration.
- [ ] Probe workers augment public roots with validated custom CA PEM and present validated PEM client identities; private-CA and mutual-TLS integration tests cover success plus actionable malformed, mismatched, and untrusted credential failures.
- [ ] The HTTP API and WebUI create and edit custom CA and mutual-TLS Secret references without exposing plaintext; domain, API, browser, generated OpenAPI, and operator documentation cover the workflow.

## Backlog

- [ ] Add optional multi-location evaluation and result aggregation.
- [ ] Add long-term history aggregation and external archival.
- [ ] Add configurable trash or restore behavior for deleted Targets.
- [ ] Provide safe discovery and cleanup of unreferenced Secrets.
- [ ] Evaluate cron or calendar schedules after fixed-interval usage is understood.

## Iteration Policy

Before starting an item, define its acceptance criteria and move only that coherent slice into the active iteration. Remove completed items after their implementation and acceptance evidence are committed.
