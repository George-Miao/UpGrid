# UpGrid TODO

The MVP is complete. This file records unfinished work for future agile iterations. Ordered from higher to lower priority.

## Active Iteration

- [ ] Replicated state discovers Secret references from active and trashed Targets plus Notification Channels; one atomic cleanup command deletes only currently unreferenced Secrets.
- [ ] The authenticated Secret API reports reference status and exposes bulk cleanup; the WebUI previews unused Secrets and requires explicit confirmation before cleanup.
- [ ] Domain, API, browser, generated OpenAPI, architecture, and operator documentation cover safe Secret discovery and cleanup.

## Backlog

- [ ] Evaluate cron or calendar schedules after fixed-interval usage is understood.

## Iteration Policy

Before starting an item, define its acceptance criteria and move only that coherent slice into the active iteration. Remove completed items after their implementation and acceptance evidence are committed.
