# Use the leader clock for cluster time

The leader assigns scheduled and recorded UTC timestamps, while executor Nodes report only durations measured with a monotonic clock. Operators must synchronize Node clocks externally. After leadership changes, committed Evaluation IDs suppress slots repeated by a clock moving backward, while a clock moving forward skips missed slots under the normal no-catch-up policy. This keeps replicated state deterministic without inventing a distributed clock service.
