# UpGrid TODO

The MVP is complete. This file records unfinished work for future agile iterations. Ordered from higher to lower priority.

## Active Iteration

- [ ] Accepted Target and Node evaluations update deterministic one-hour rollups with sample, success, failure, and latency aggregates; replicated rollups use a configurable long-term retention window, with backward-compatible state migration.
- [ ] `GET /api/v1/targets/{id}/history` pages rollups chronologically with bounded time and result limits so external systems can archive them without reading raw Raft state.
- [ ] The WebUI displays long-term availability and latency summaries; domain, API, browser, generated OpenAPI, configuration, architecture, and operator documentation cover aggregation, retention, and export.

## Backlog

- [ ] Add long-term history aggregation and external archival.
- [ ] Add configurable trash or restore behavior for deleted Targets.
- [ ] Provide safe discovery and cleanup of unreferenced Secrets.
- [ ] Evaluate cron or calendar schedules after fixed-interval usage is understood.

## Iteration Policy

Before starting an item, define its acceptance criteria and move only that coherent slice into the active iteration. Remove completed items after their implementation and acceptance evidence are committed.
