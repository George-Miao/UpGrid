# UpGrid TODO

The MVP is complete. This file records unfinished work for future agile iterations. Ordered from higher to lower priority.

## Active Iteration

- [ ] Draining a Node excludes it from new evaluation assignments while in-flight work finishes; forced drain releases failed-Node assignments for immediate reassignment.
- [ ] Membership removal rejects the local Node and final voter, requires a completed drain unless forced, and converges Raft membership plus derived Node Targets.
- [ ] The HTTP API and WebUI expose drain, cancel, removal, and failed-Node replacement guidance; domain, scheduler, API, and live three-Node tests cover the workflow.

## Backlog

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
