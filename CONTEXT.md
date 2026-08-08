# UpGrid Monitoring

UpGrid provides highly available service monitoring within a single operational domain.

## Language

**Deployment**:
A complete UpGrid installation operated as one administrative unit and containing exactly one cluster.
_Avoid_: Network, federation

**Cluster**:
The group of UpGrid nodes that collectively owns a deployment's monitoring data and availability.

**Node**:
One durably identified participating member of a cluster. A Node retains its identity across restarts; a replacement is a new Node.
_Avoid_: Instance, server, peer

**Cluster API**:
The consistent user-facing interface exposed by every node for interacting with deployment-owned data.
_Avoid_: Node API, leader API

**Secret**:
A named confidential value referenced by Target or notification configuration and never revealed after submission.
_Avoid_: Credential, token, password

**Notification Channel**:
A reusable Telegram or webhook destination that receives availability-transition alerts from referenced Targets.
_Avoid_: Contact, notifier, integration

**Alert**:
A durable notice created when a Target enters Down or returns to Up and addressed to one Notification Channel.
_Avoid_: Notification, message, delivery attempt

**Join Token**:
A single-use, expiring authorization for admitting one new Node to a Cluster.
_Avoid_: Invite, cluster password

**Target**:
A configured endpoint together with its evaluation schedule, timeout, failure threshold, and success criteria.
_Avoid_: Monitor, service, check

**Target ID**:
The immutable identity assigned to a Target by the Cluster. Names, URLs, and other mutable attributes are not identities.
_Avoid_: Target name, URL

**HTTP Target**:
A target identified by an HTTP or HTTPS URL and evaluated using HTTP response criteria.
_Avoid_: Website, URL monitor

**Evaluation**:
A single cluster-wide assessment scheduled for a Target interval. Reassignment may execute it more than once, but only one result is authoritative.
_Avoid_: Check, probe, ping

**Evaluation History**:
The bounded sequence of recent evaluations retained by the cluster for each target.
_Avoid_: Audit log, event log

**Availability State**:
The current cluster-wide conclusion about a target: Unknown, Up, or Down. Unknown means no conclusion has yet been reached.
_Avoid_: Health, evaluation result
