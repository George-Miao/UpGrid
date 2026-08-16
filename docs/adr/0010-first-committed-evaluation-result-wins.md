# Accept the first committed evaluation result

An evaluation is identified by its target ID and scheduled timestamp. If reassignment causes multiple nodes to execute it, the first result committed through Raft becomes authoritative and every later duplicate is discarded. This gives failure streaks, history, and alerts one deterministic outcome despite unavoidable duplicate external requests.
