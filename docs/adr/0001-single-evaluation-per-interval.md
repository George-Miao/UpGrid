# Assign one node per evaluation interval

For the MVP, each scheduled target interval produces one cluster-wide evaluation. The leader assigns it to one available node, reassigns work that times out, and commits the result through Raft. This was chosen over evaluation by every node to avoid duplicate traffic and alerts, give each interval one authoritative outcome, and distribute work horizontally; explicit multi-location evaluation and result aggregation remain possible post-MVP extensions.
