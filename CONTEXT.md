# UpGrid monitoring

UpGrid provides highly available service monitoring within a single operational domain.

## Language

**Deployment**:
A complete UpGrid installation operated as one administrative unit and containing exactly one cluster.
_Avoid_: network, federation

**Cluster**:
The group of UpGrid nodes that collectively owns a deployment's monitoring data and availability.

**Node**:
A participating member of a cluster, durably identified by its Node ID across restarts. Network addresses are mutable reachability metadata; a replacement is a new node.
_Avoid_: instance, server, peer

**Node ID**:
The immutable identifier assigned to a node when it is created and retained across restarts.
_Avoid_: Raft ID, node address

**Local address**:
A locally owned host and port on which a node accepts cluster traffic. A node can have multiple local addresses, and they are not part of its identity.
_Avoid_: bind address, public address

**Reachable address candidate**:
A complete `up://` locator discovered for a node but not yet proven by a successful connection from another node.
_Avoid_: observed address, unverified address

**Reachable address**:
A cluster-owned `up://` locator that another node can use to establish a connection. A node can have multiple reachable addresses, supplied by an operator or promoted from verified candidates.
_Avoid_: advertised address, public address, rendezvous address

**Route connectivity**:
A process-local observation of whether one source node can connect to one reachable address. Connectivity from one node to another exists when at least one route to the destination succeeds.
_Avoid_: route health, target evaluation

**Cluster API**:
The consistent user-facing interface exposed by every node for interacting with deployment-owned data.
_Avoid_: node API, leader API

**Operator identity**:
A replicated human administrator account that authenticates to the cluster API and owns its API tokens.
_Avoid_: user, shared administrator, node identity

**Session**:
A short-lived login authorization for one operator identity, encoded as a signed JWT and invalidated when that identity's authentication version changes.
_Avoid_: login token, cookie

**API token**:
A named, revocable bearer authorization owned by one operator identity; only its verifier is retained after issuance.
_Avoid_: API key, personal access token, join token

**Secret**:
A named confidential value referenced by target or notification configuration and never revealed after submission.
_Avoid_: credential, token, password

**Notification channel**:
A reusable Telegram, SMTP email, or webhook destination that receives availability-transition alerts from referenced targets.
_Avoid_: contact, notifier, integration

**Alert**:
A durable notice created when a target enters down or returns to up and addressed to one notification channel.
_Avoid_: notification, message, delivery attempt

**Join token**:
An expiring and revocable authorization for admitting a limited or unlimited number of nodes to a cluster.
_Avoid_: invite, cluster password

**Target**:
A configured endpoint together with its evaluation schedule, timeout, failure threshold, and success criteria.
_Avoid_: monitor, service, check

**Target ID**:
The immutable identity assigned to a target by the cluster. Names, URLs, and other mutable attributes are not identities.
_Avoid_: target name, URL

**HTTP target**:
A target identified by an HTTP or HTTPS URL and evaluated using HTTP response criteria.
_Avoid_: website, URL monitor

**TCP target**:
A target that succeeds when a TCP connection can be established to its host and port.
_Avoid_: port check

**DNS target**:
A target that succeeds when its hostname resolves to at least one address.
_Avoid_: DNS check

**ICMP target**:
A target that succeeds when its host answers an ICMP echo request.
_Avoid_: ping

**TLS target**:
A target that succeeds when it completes a TLS handshake with a valid certificate for its hostname.
_Avoid_: SSL check, certificate check

**Evaluation**:
A single cluster-wide assessment scheduled for a target interval. Reassignment may execute it more than once, but only one result is authoritative.
_Avoid_: check, probe, ping

**Evaluation history**:
The bounded sequence of recent evaluations retained by the cluster for each target.
_Avoid_: audit log, event log

**Availability state**:
The current cluster-wide conclusion about a target: unknown, up, or down. Unknown means no conclusion has yet been reached.
_Avoid_: health, evaluation result
