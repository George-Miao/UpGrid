# First-run cluster OOBE

Status: implemented. This document records the accepted behavior and its original motivation.

## Problem statement

Before OOBE was introduced, a fresh UpGrid node required an operator to understand startup flags before it could safely create or join a cluster. The browser onboarding path only supported joining, while ordinary startup silently created a cluster. Operators also saw cluster actions that were invalid for the node's lifecycle state, and optional first target and notification channel setup was disconnected from provisioning.

## Solution

Give every fresh, unconfigured node an authenticated out-of-box experience (OOBE). The required first step reviews the node name and chooses either a new cluster or a join token. After the node has durable cluster membership, OOBE offers skippable notification channel and target steps before entering the normal dashboard. Automation can bypass OOBE with explicit CLI or environment configuration.

## User stories

1. As a new operator, I want a fresh node to open OOBE by default, so that startup does not silently make an architectural choice for me.
2. As an operator creating a deployment, I want to choose **Create new cluster**, so that I can bootstrap the first node without a terminal command.
3. As an operator expanding a deployment, I want to paste a join token, so that I can attach a fresh node from the browser.
4. As an automation author, I want `--new-cluster`, so that unattended provisioning can bypass OOBE.
5. As an automation author, I want `UPGRID_NEW_CLUSTER=true`, so that container configuration has parity with the CLI.
6. As an automation author, I want `--join` and `UPGRID_JOIN`, so that a node can join without browser interaction.
7. As an operator, I want cluster creation and joining to be mutually exclusive, so that the node receives one unambiguous lifecycle instruction.
8. As an operator, I want to review or edit the node name before membership is established, so that topology is recognizable immediately.
9. As an operator who leaves the name unset, I want a persisted friendly two-word name, so that nodes remain distinguishable without manual naming.
10. As a security-conscious operator, I want OOBE protected by configured basic auth, so that join tokens and provisioning actions are not public.
11. As an operator, I want confirmation before creating a cluster, so that an accidental click does not commit durable Raft state.
12. As an operator, I want an unconfigured node prevented from creating targets or channels, so that replicated writes happen only after membership exists.
13. As an operator, I want an optional channel step after membership, so that alert delivery can be configured during first run.
14. As an operator, I want an optional target step after membership, so that monitoring can begin during first run.
15. As an operator, I want to skip either optional step, so that OOBE never forces placeholder resources.
16. As an operator joining an established cluster, I want existing resource counts shown, so that I avoid creating duplicates.
17. As an operator, I want target and channel OOBE forms to behave like dashboard forms, so that validation and capabilities are consistent.
18. As an operator interrupted during setup, I want OOBE progress to survive restart, so that I resume at the unfinished step.
19. As an operator, I want setup steps represented by `/setup`, `/setup/channel`, and `/setup/target`, so that refresh and navigation are predictable.
20. As an operator, I want browser back blocked from repeating a durable cluster choice, so that the node cannot accidentally reinitialize.
21. As an operator, I want normal navigation hidden until OOBE is completed or skipped, so that the setup sequence has a clear finish.
22. As an operator restarting a member, I want durable Raft membership to override stale OOBE state, so that restart always reaches the dashboard.
23. As an operator restarting a member with `--new-cluster`, I want the option ignored, so that an idempotent deployment declaration cannot destroy membership.
24. As an operator restarting with a join token for a current member URL, I want it ignored, so that repeated provisioning remains harmless.
25. As an operator restarting with an external or unverifiable join token, I want startup to continue with a WebUI warning, so that stale configuration does not cause downtime.
26. As an operator, I want the stale-token warning dismissible for my browser session, so that it remains informative without permanently obstructing work.
27. As an operator of a configured cluster, I want **Create token** to be the primary cluster action, so that the valid expansion workflow is prominent.
28. As an operator of a configured cluster, I do not want **Join cluster** displayed, so that the UI does not offer an unsafe in-place switch.
29. As an operator of an unconfigured node, I want only **Join cluster** and **Create new cluster** choices, so that unavailable cluster operations are hidden.
30. As an operator, I want completing OOBE to redirect to overview, so that the transition into normal operation is explicit.
31. As an operator, I want to review configured network sources and add reachable addresses or discovery services before cluster creation or joining, so that the node can work across its deployment network.

## Implementation decisions

- Durable Raft membership is authoritative. Local OOBE state controls only first-run presentation and optional-step progress.
- Fresh data with neither join nor new-cluster intent starts OOBE. Existing membership resumes normally regardless of unfinished OOBE state.
- Remove `--setup`. Add mutually exclusive `--new-cluster` and `--join`; Figment exposes equivalent `UPGRID_NEW_CLUSTER` and `UPGRID_JOIN` values.
- Persist the phases `cluster`, `channel`, `target`, and `complete` in the node data directory using the existing durable replacement mechanism.
- The pre-membership HTTP server exposes only authenticated OOBE status and cluster-choice operations plus static assets. It cannot expose replicated target or channel mutation routes.
- Cluster choice accepts the validated node name. New-cluster creation requires a browser confirmation; join token submission retains the current typed `up://` parsing and deployment-key checks.
- After membership starts, the ordinary cluster API exposes local OOBE phase reads and phase advancement. Channel and target creation continue through their existing replicated endpoints.
- OOBE routes are `/setup`, `/setup/channel`, and `/setup/target`. Advancing past cluster choice replaces browser history. Completing or skipping the last step redirects to `/`.
- If a joined cluster already contains channels or targets, OOBE shows existing counts and still offers add or skip.
- On restart, a configured join token is compared with persisted membership by issuer node ID and reachable remote address. A match is ignored. A mismatch becomes process-local warning state and never blocks startup or enters Raft.
- Startup warnings are exposed through a local API response, displayed prominently, and dismissible in browser session state.
- Configured cluster pages show **Create token** as the primary action and never render **Join cluster**. Unconfigured OOBE renders the cluster choices instead.
- OOBE node-name edits are validated and durably stored when cluster choice is submitted, including when joining subsequently fails.
- OOBE shows configured reachable addresses and discovery services as fixed values. Operators can add sources before either cluster choice. The accepted values are validated and stored before bootstrap.

## Testing decisions

- Tests observe public behavior rather than private helpers.
- Playwright is the primary OOBE seam: cover fresh default startup, create-cluster confirmation, browser joining, network source review and entry, URL phases, skips, resource creation, existing-resource summaries, navigation visibility, warning dismissal, and final redirect.
- The local three-node verifier covers non-interactive `--new-cluster`, reusable join token admission, configured node names, and normal cluster topology.
- Focused configuration tests cover Clap conflicts and Figment environment parity.
- Focused durable-state tests cover OOBE phase persistence and restart behavior.
- State-machine migration tests remain the regression seam for previously persisted cluster data.
- Existing browser and generated-OpenAPI tests are extended instead of introducing a second UI or API harness.

## Out of scope

- Switching an initialized node to another cluster in place.
- Clearing cluster data from the WebUI.
- Replacing basic auth with the future identity and role system.
- Per-node targets or notification channels; these remain cluster-wide replicated resources.
- Editing local bind addresses or TLS paths from OOBE.
- Automatically creating sample targets or channels.

## Further notes

Join tokens remain bearer credentials and must not be logged or retained in browser storage. The OOBE marker is local operational state, not replicated domain state. Normal restarts should use no lifecycle flag; compatibility handling exists to keep stale declarative configuration non-disruptive.
