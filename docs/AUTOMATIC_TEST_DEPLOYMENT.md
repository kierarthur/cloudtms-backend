# Automatic TEST deployment: operating contract

## What the user says

“Deploy these completed changes to TEST.” Codex owns the steps below. Do not ask the user to choose full, component or application-only installation. Planning alone never authorises publication. This system is restricted to CloudTMS agency TEST; it is not a LIVE, new-agency, mobile-store or secret-management tool.

## Canonical entry points

- `npm run release:test -- --connections-proof=<absolute JSON path> [--office-path=<reviewed clean Office worktree>]`: desktop coordinator. Uses existing GitHub SSH and `gh` authentication; does not obtain or rotate tokens. Requires reviewed source already committed and pushed to backend `test`. Never commits, merges, resets, cleans or force-pushes somebody's work.
- Protected `database-release.yml`, environment TEST, mode UPGRADE, phase AUTO: automatically chooses the database route using fresh installed identity, ledger, latest release status and actual contract. PLAN/APPLY remain available for explicit controlled operations; AUTO is the normal managed TEST entry.
- `npm run release:test:plan`: read-only machine-local planning with approved process-local TEST database configuration. Never print credentials. Prefer protected workflow planning where local credentials are unavailable.
- Protected phase `AUTO_PLAN` previews the same automatic decision and uploads its receipt without installing SQL or publishing applications. It still requires the exact managed TEST target and passes the repository source gates.

## Before every release

1. Read workspace/repository AGENTS, this document and DATABASE_RELEASE_BIBLE. Preserve dirty and unrelated work. Obtain the user's TEST publication authority. Finish local regression checks. Record exact backend and optional Office commits.
2. Verify canonical repositories, current remote refs and non-forced ancestry. Fetch only as needed. Keep pending application changes isolated until the release-branch connection described below is proved.
3. Through Cloudflare API inspect all four TEST build triggers: repository, branch, root, deploy command, managed build-token name and active deployed version. Never display a token value. Resolve each active version through the Builds-by-version endpoint to its successful build's exact Git commit where available. Record a non-secret proof JSON with `environment:"TEST"`, `backendCommit:<exact SHA>`, `checkedAt:<current ISO time>`, and `workers:[{worker,branch,repository:"kierarthur/cloudtms-backend",verified:true,branchCommit:<remote 40-character SHA>,activeVersion:<active version UUID>,activeCommit:<proved build SHA or null>}]`. Only mark verified after actual inspection. It expires after 15 minutes at coordinator start. Missing historical commit evidence (for example, a prior manual deploy) never qualifies as unchanged: the coordinator performs a fresh ordered build. A branch already at the proposed SHA with unproved deployment requires explicit exact-trigger rebuild/read-back; a no-op push is not success. Store under ignored `.codex-tmp`; this is agent-created inspection evidence, not a user-authored permission bypass.
4. Run the canonical desktop command. It captures Office HEAD before starting, source-gates the exact backend commit, waits for the protected database result, then publishes changed runtime targets in dependency order. Existing unchanged target source is retained. Each changed Worker must report a successful Cloudflare GitHub check for the promoted commit before the next target moves. Office main must still equal its captured starting commit before its non-forced push; GitHub Pages must report that exact Office commit built.
5. Confirm active deployment identities through Cloudflare and the served Office asset/version. The coordinator's build receipts do not substitute for application acceptance. Respect a user-requested pause before functional tests; report `DEPLOYED_ACCEPTANCE_PENDING`, not “everything tested”.

## Deployment branch registry

Source branch is `cloudtms-backend/test`. Normal backend publishing must be decoupled from that branch before using the coordinator:

| Order | Worker | Git deployment branch | Deploy command |
|---|---|---|---|
| 1 | test-cloudtms-backend | deploy/cloudflare/test-cloudtms-backend | npx wrangler deploy --env test |
| 2 | test-cloudtms-candidate-private-api | deploy/cloudflare/test-candidate-private-api | npx wrangler deploy --config candidate-private-api/wrangler.jsonc --keep-vars |
| 3 | test-cloudtms-candidate-synthetic-private-api | deploy/cloudflare/test-candidate-synthetic-private-api | npx wrangler deploy --config candidate-synthetic-private-api/wrangler.jsonc --keep-vars |
| 4 | test-cloudtms-candidate-broker | deploy/cloudflare/test-candidate-broker | npx wrangler deploy --config candidate-broker/wrangler.jsonc --keep-vars |

One-time bootstrap: save this configuration to GitHub first. Create the new backend deployment branch at the currently deployed known-good source (no force). Change only that Worker's existing trigger branch from test to the dedicated branch, preserving repository, token, commands and other settings. Prove the saved trigger. Do not modify LIVE or another account's connector. Existing private/broker deployment branches are preserved. A wrong/missing trigger is a deployment failure, not permission to publish around it.

## Automatic database decision

1. Wrong database, identity or immutable ledger: stop.
2. Latest release FAILED/APPLYING/missing: full protected UPGRADE. An installed definition is not VERIFIED. Never route around an outstanding full verification failure with a smaller component.
3. No pending SQL, latest release VERIFIED, full installed contract equal, and all database/release authority unchanged since that verified commit: no database reinstall and no full SQL-test replay. Source gates still run. This is the fast application-only route.
4. An exact registered component may run only when every pending definition is covered, no migration is pending, its current source and installed before/after closure hashes pass, and no unrelated authority changed. The first adapter is the historical exact two-file invoice-evidence component; it will fail eligibility after its pinned source changes. Its existing engine performs plan, rollback rehearsal, scoped install and verification. Do not broaden an old manifest merely to select this route.
5. Everything else: full UPGRADE. Apply only genuinely pending migrations/changed closures using existing ledgers, then run every required verifier and exact contract comparison. No automatic NEW, ADOPT, LEGACY_UPGRADE or LIVE.

New component registration requires reviewed exact closure coverage, before/after identities, security and contract boundaries, its protected engine, runtime dependencies and positive/negative routing tests. Unknown scope falls back to full verification, not guessed selective tests. Native Android/iOS build/store requests remain governed by the MyTMS runbooks; do not rebuild a phone app for an Office-only or database-only change.

## Failure, resume and evidence

- `.codex-tmp/automatic-test-release.json`: protected database decision, installed predecessor, exact source, pending files, stage status and elapsed time; uploaded as a bounded workflow artifact.
- `.codex-tmp/test-deployment-receipt.json`: desktop coordinator's exact commits, workflow/check IDs and completed stages.
- Verifier logs now emit `VERIFY START/PASS/FAILED`, exact file and elapsed milliseconds. First failure stops immediately. No blind retries, increased timeouts, reduced test cycles or manual VERIFIED-ledger edits.
- Resume means re-read current state first. Installed migrations/closures are not needlessly reapplied; successful application targets with unchanged source are not rebuilt. **Failed SQL verification is rerun in full.** Data-dependent verifier passes are not cached across runs because TEST data can change. A future finer-grained verification cache requires a separately reviewed invalidation model.
- Coordinator timeout does not mean the remote job stopped. Inspect the saved run before dispatching anything else. Do not start a duplicate job. If an application stage fails after a verified database release, reconcile that exact target and continue only compatible stages; never roll back unrelated data or force old branches over newer work.
- Record deployment evidence and remaining acceptance tests in the task's existing consolidated release report. GitHub is authoritative for every permanent installed change; rollback diagnostic SQL is never an installation.

## Limits, not hidden promises

The system automatically selects and executes database routes and orders the registered backend/Office publication. Cloudflare connector inspection/active-version read-back remain Codex-operated preflight/postflight because the existing GitHub workflow intentionally has no Cloudflare management token. It does not silently add credentials or permissions. It does not fix failing business tests, generate unreviewed component scopes, publish to stores, run payment operations, or claim functional acceptance from a green build.
