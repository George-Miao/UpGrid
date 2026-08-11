# UpGrid TODO

The MVP is complete. This file records unfinished work for future agile iterations. Ordered from higher to lower priority.

## Active Iteration

- [ ] HTTP Targets store ordered regex, JSONPath, response-header, latency-threshold, and sandboxed script assertions with validation and backward-compatible state migration.
- [ ] Probe workers evaluate every configured assertion deterministically with bounded resources and actionable failure diagnostics.
- [ ] The HTTP API and WebUI create and edit every assertion kind; domain, probe, API, and browser tests plus operator documentation cover the workflow.

## Backlog

- [ ] Support custom HTTPS CA bundles and mutual-TLS Target credentials.
- [ ] Add optional multi-location evaluation and result aggregation.
- [ ] Add long-term history aggregation and external archival.
- [ ] Add configurable trash or restore behavior for deleted Targets.
- [ ] Provide safe discovery and cleanup of unreferenced Secrets.
- [ ] Evaluate cron or calendar schedules after fixed-interval usage is understood.

## Iteration Policy

Before starting an item, define its acceptance criteria and move only that coherent slice into the active iteration. Remove completed items after their implementation and acceptance evidence are committed.
