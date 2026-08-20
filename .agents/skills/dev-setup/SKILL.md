---
name: dev-setup
description: Start or clean up a fresh, uninitialized UpGrid development stack. Use when the user asks for a fresh backend and development frontend, requests the development website with it, or asks to stop the stack and delete its temporary data.
---

# Manage a Fresh Development Stack

Use the supervised process names `upgrid-dev-backend`, `upgrid-dev-frontend`, and `upgrid-dev-website`. Store the active data directory path in `${TMPDIR:-/tmp}/upgrid-dev.current`.

A fresh node uses a new temporary data directory and remains on the setup screen. Verification ends at that screen. Never submit a setup action or create the cluster through the WebUI.

## Start

1. Read the repository instructions and inspect the three supervised process names plus the state file. When a recorded stack exists, run the cleanup branch before the new start. Complete this step when the old services and recorded data are absent.
2. Prefer API port `28080`, Raft port `29080`, frontend port `5173`, and website port `4321`. When an unrelated process owns a preferred port, select the first free higher port. Complete this step when every selected port is free and recorded.
3. Run `mktemp -d -t upgrid-dev.XXXXXX` once. Write its absolute output path to the state file and use it as the backend `--data-dir`. Complete this step when the path exists, is empty, is a direct child of the temporary root, and is recorded.
4. From the repository root, run `nix develop --command pnpm --dir frontend install --frozen-lockfile`. When the user requested the website, run the same locked install for `website`. Complete this step when every required install exits successfully.
5. Start the backend in setup mode from the repository root with the current agent's supervised process tool:

   ```text
   nix develop --command cargo run -p upgrid -- --bind 127.0.0.1:<api-port> --raft-url up://127.0.0.1:<raft-port> --data-dir <temp-dir> --node-name dev
   ```

   Wait for the `WebUI ready for cluster setup` log and the API port. Request `http://127.0.0.1:<api-port>/api/v1/setup`. Complete this step when the response has `setup: true`, `phase: "cluster"`, and `cluster_ready: false`, and the supervisor reports the backend as ready.
6. Start the frontend under supervision from the repository root. Set `UPGRID_API_URL=http://127.0.0.1:<api-port>` and run:

   ```text
   nix develop --command pnpm --dir frontend dev --host 127.0.0.1 --port <frontend-port> --strictPort
   ```

   Wait for the local URL log and the frontend port. Open `http://127.0.0.1:<frontend-port>/setup` in a browser. Complete this step when the fresh node setup screen is visible and still awaits user input.
7. Run this step only when the user requested the development website. Start it under supervision from the repository root:

   ```text
   nix develop --command pnpm --dir website dev --host 127.0.0.1 --port <website-port>
   ```

   Wait for the local URL log and the website port. Open `http://127.0.0.1:<website-port>` in a browser. Complete this step when the website renders.
8. Leave the requested services running. Report each service URL, the Raft URL, the temporary data directory, and the readiness checks. Complete the start when the supervisor reports every requested service as ready.

If any start step fails after the state file is written, run the cleanup branch before reporting the failure.

## Stop and Clean Up

1. Read the state file when it exists. Stop `upgrid-dev-website`, `upgrid-dev-frontend`, then `upgrid-dev-backend`, and wait for each process to exit. Complete this step when none of the three supervised processes is live.
2. Validate the recorded data path before deletion. It must be an absolute, non-symlink directory owned by the current user, its canonical parent must equal the canonical temporary root, and its name must start with `upgrid-dev.`. Complete this step when every condition is proven. If any condition fails, preserve the path and report the failed condition.
3. Recursively delete the validated data directory, then delete the state file. Complete cleanup when both paths are absent and the three supervised processes remain stopped.
4. Report the stopped services and deleted data directory. When no state file existed, report that the services stopped and no recorded data directory was available.
