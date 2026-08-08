# Forward mutations transparently to the leader

Every Node exposes the same Cluster API and transparently forwards mutations to the current Raft leader, returning the leader's response through the original client connection. Clients do not follow leader redirects or perform leader discovery; when leadership cannot be established within the request deadline, the receiving Node returns `503 Service Unavailable`. This simplifies clients at the cost of an extra internal hop for follower writes.
