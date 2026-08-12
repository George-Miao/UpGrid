# UpGrid TODO

The MVP is complete. This file records unfinished work for future agile iterations. Ordered from higher to lower priority.

## Active Iteration

- [ ] Target deletion atomically moves state, location settings, and history into replicated trash, releases assignments, prunes expired entries under a configurable retention window, and restores unexpired Targets without loss through backward-compatible state migration.
- [ ] Authenticated trash endpoints list, restore, and permanently delete trashed Targets; the WebUI exposes the same lifecycle with explicit confirmations.
- [ ] Domain, API, browser, generated OpenAPI, configuration, architecture, and operator documentation cover Target trash retention, restore, expiry, and permanent deletion.

## Backlog

- [ ] Add configurable trash or restore behavior for deleted Targets.
- [ ] Provide safe discovery and cleanup of unreferenced Secrets.
- [ ] Evaluate cron or calendar schedules after fixed-interval usage is understood.

## Iteration Policy

Before starting an item, define its acceptance criteria and move only that coherent slice into the active iteration. Remove completed items after their implementation and acceptance evidence are committed.
