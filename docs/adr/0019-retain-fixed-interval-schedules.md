# Retain fixed-interval Evaluation schedules

## Context

Targets currently specify one positive interval. A stable phase derived from the Target ID spreads due work across that interval. The leader plans only the latest due slot, so downtime or leader changes do not create catch-up bursts. Evaluation IDs contain the scheduled timestamp, which keeps assignment retries, multi-location aggregation, history, failure thresholds, and alert transitions deterministic.

The reference workload exercises 1,000 Targets at 60-second intervals and verifies that at least 99 percent complete within one interval. Repository workflows and operator documentation use the same periodic model. UpGrid does not collect deployment telemetry, so there is no evidence yet of demand for wall-clock schedules.

Cron or calendar schedules would add operator-visible choices that the existing interval field does not answer:

- named time zone versus UTC and how time-zone database updates propagate;
- daylight-saving gaps and repeated local times;
- whether downtime skips, coalesces, or replays missed occurrences;
- how irregular samples affect consecutive-failure thresholds and availability expectations;
- whether business-hour intent is a probe schedule, a maintenance window, or notification suppression.

Implementing a cron parser before these semantics are chosen would expose a shallow interface while distributing calendar edge cases through the domain, API, WebUI, scheduler, migrations, and documentation.

## Decision

Keep fixed intervals as the only Target schedule. Do not add cron syntax or calendar dependencies without observed operator demand and an explicit semantic contract.

Revisit this decision when at least one deployment requires wall-clock execution that cannot be represented by an interval. Before implementation, define:

1. UTC or IANA time-zone behavior, including daylight-saving gaps and repetitions;
2. missed-occurrence behavior after downtime and leader changes;
3. interaction with failure thresholds, maintenance windows, and notification routing;
4. a reference workload containing many coincident calendar occurrences.

The future seam is a deep scheduling module with one interface: given a Target ID, schedule value, last committed occurrence, and current time, return at most one due scheduled timestamp. Fixed interval and calendar adapters would remain inside that module. The scheduler, assignment model, and Evaluation ID continue to consume only the returned timestamp.

## Consequences

The current scheduler remains deterministic, load-smoothed, dependency-free, and backward compatible. UpGrid does not yet support business-hour, daily, weekly, or cron-expression probes. This is an intentional product decision rather than an unimplemented parser.
