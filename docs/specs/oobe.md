# First-Run Cluster OOBE

Status: implemented. This document records the accepted behavior and its original motivation.

## Problem Statement

Before OOBE was introduced, a fresh UpGrid Node required an operator to understand startup flags before it could safely create or join a Cluster. The browser onboarding path only supported joining, while ordinary startup silently created a Cluster. Operators also saw cluster actions that were invalid for the Node's lifecycle state, and optional first Target and Notification Channel setup was disconnected from provisioning.

## Solution

Give every fresh, unconfigured Node an authenticated out-of-box experience (OOBE). The required first step reviews the Node name and chooses either a new Cluster or a Join Token. After the Node has durable Cluster membership, OOBE offers skippable Notification Channel and Target steps before entering the normal dashboard. Automation can bypass OOBE with explicit CLI or environment configuration.

## User Stories

1. As a new operator, I want a fresh Node to open OOBE by default, so that startup does not silently make an architectural choice for me.
2. As an operator creating a deployment, I want to choose **Create new cluster**, so that I can bootstrap the first Node without a terminal command.
3. As an operator expanding a deployment, I want to paste a Join Token, so that I can attach a fresh Node from the browser.
4. As an automation author, I want `--new-cluster`, so that unattended provisioning can bypass OOBE.
5. As an automation author, I want `UPGRID_NEW_CLUSTER=true`, so that container configuration has parity with the CLI.
6. As an automation author, I want `--join` and `UPGRID_JOIN`, so that a Node can join without browser interaction.
7. As an operator, I want cluster creation and joining to be mutually exclusive, so that the Node receives one unambiguous lifecycle instruction.
8. As an operator, I want to review or edit the Node name before membership is established, so that topology is recognizable immediately.
9. As an operator who leaves the name unset, I want a persisted friendly two-word name, so that Nodes remain distinguishable without manual naming.
10. As a security-conscious operator, I want OOBE protected by configured Basic Auth, so that Join Tokens and provisioning actions are not public.
11. As an operator, I want confirmation before creating a Cluster, so that an accidental click does not commit durable Raft state.
12. As an operator, I want an unconfigured Node prevented from creating Targets or Channels, so that replicated writes happen only after membership exists.
13. As an operator, I want an optional Channel step after membership, so that alert delivery can be configured during first run.
14. As an operator, I want an optional Target step after membership, so that monitoring can begin during first run.
15. As an operator, I want to skip either optional step, so that OOBE never forces placeholder resources.
16. As an operator joining an established Cluster, I want existing resource counts shown, so that I avoid creating duplicates.
17. As an operator, I want Target and Channel OOBE forms to behave like dashboard forms, so that validation and capabilities are consistent.
18. As an operator interrupted during setup, I want OOBE progress to survive restart, so that I resume at the unfinished step.
19. As an operator, I want setup steps represented by `/setup`, `/setup/channel`, and `/setup/target`, so that refresh and navigation are predictable.
20. As an operator, I want browser Back blocked from repeating a durable cluster choice, so that the Node cannot accidentally reinitialize.
21. As an operator, I want normal navigation hidden until OOBE is completed or skipped, so that the setup sequence has a clear finish.
22. As an operator restarting a member, I want durable Raft membership to override stale OOBE state, so that restart always reaches the dashboard.
23. As an operator restarting a member with `--new-cluster`, I want the option ignored, so that an idempotent deployment declaration cannot destroy membership.
24. As an operator restarting with a Join Token for a current member URL, I want it ignored, so that repeated provisioning remains harmless.
25. As an operator restarting with an external or unverifiable Join Token, I want startup to continue with a WebUI warning, so that stale configuration does not cause downtime.
26. As an operator, I want the stale-token warning dismissible for my browser session, so that it remains informative without permanently obstructing work.
27. As an operator of a configured Cluster, I want **Create token** to be the primary Cluster action, so that the valid expansion workflow is prominent.
28. As an operator of a configured Cluster, I do not want **Join cluster** displayed, so that the UI does not offer an unsafe in-place switch.
29. As an operator of an unconfigured Node, I want only **Join cluster** and **Create new cluster** choices, so that unavailable Cluster operations are hidden.
30. As an operator, I want completing OOBE to redirect to Overview, so that the transition into normal operation is explicit.

## Implementation Decisions

- Durable Raft membership is authoritative. Local OOBE state controls only first-run presentation and optional-step progress.
- Fresh data with neither join nor new-cluster intent starts OOBE. Existing membership resumes normally regardless of unfinished OOBE state.
- Remove `--setup`. Add mutually exclusive `--new-cluster` and `--join`; Figment exposes equivalent `UPGRID_NEW_CLUSTER` and `UPGRID_JOIN` values.
- Persist the phases `cluster`, `channel`, `target`, and `complete` in the Node data directory using the existing durable replacement mechanism.
- The pre-membership HTTP server exposes only authenticated OOBE status and cluster-choice operations plus static assets. It cannot expose replicated Target or Channel mutation routes.
- Cluster choice accepts the validated Node name. New-cluster creation requires a browser confirmation; Join Token submission retains the current typed `up://` parsing and deployment-key checks.
- After membership starts, the ordinary Cluster API exposes local OOBE phase reads and phase advancement. Channel and Target creation continue through their existing replicated endpoints.
- OOBE routes are `/setup`, `/setup/channel`, and `/setup/target`. Advancing past cluster choice replaces browser history. Completing or skipping the last step redirects to `/`.
- If a joined Cluster already contains Channels or Targets, OOBE shows existing counts and still offers add or skip.
- On restart, a configured Join Token is compared with persisted membership by advertised remote URL. A match is ignored. A mismatch becomes process-local warning state and never blocks startup or enters Raft.
- Startup warnings are exposed through a local API response, displayed prominently, and dismissible in browser session state.
- Configured Cluster pages show **Create token** as the primary action and never render **Join cluster**. Unconfigured OOBE renders the cluster choices instead.
- OOBE Node-name edits are validated and durably stored when cluster choice is submitted, including when joining subsequently fails.

## Testing Decisions

- Tests observe public behavior rather than private helpers.
- Playwright is the primary OOBE seam: cover fresh default startup, create-cluster confirmation, browser joining, URL phases, skips, resource creation, existing-resource summaries, navigation visibility, warning dismissal, and final redirect.
- The local three-Node verifier covers non-interactive `--new-cluster`, reusable Join Token admission, configured Node names, and normal Cluster topology.
- Focused configuration tests cover Clap conflicts and Figment environment parity.
- Focused durable-state tests cover OOBE phase persistence and restart behavior.
- State-machine migration tests remain the regression seam for previously persisted Cluster data.
- Existing browser and generated-OpenAPI tests are extended instead of introducing a second UI or API harness.

## Out of Scope

- Switching an initialized Node to another Cluster in place.
- Clearing Cluster data from the WebUI.
- Replacing Basic Auth with the future identity and role system.
- Per-Node Targets or Notification Channels; these remain Cluster-wide replicated resources.
- Editing bind addresses, advertised Raft URLs, or TLS paths from OOBE.
- Automatically creating sample Targets or Channels.

## Further Notes

Join Tokens remain bearer credentials and must not be logged or retained in browser storage. The OOBE marker is local operational state, not replicated domain state. Normal restarts should use no lifecycle flag; compatibility handling exists to keep stale declarative configuration non-disruptive.
