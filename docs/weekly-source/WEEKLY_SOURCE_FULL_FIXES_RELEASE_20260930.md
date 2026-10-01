# Weekly Source full fixes — release evidence

## Automatic import outreach and replay correction — 1 October 2026

The additional approved automatic outreach is deployed to TEST. Accepted imports start eligible missing-timesheet and signed-hours checks through the existing policy, generation and delivery owners. Office buttons remain optional interventions. Explicit manager-direct activation is preserved; normal initial discrepancy contact does not immediately email a manager.

Unchanged source facts under a different upload/publication/resolution identity retain the original missing-timesheet request, membership and deadline and create no additional initial message. A material break change creates one new request and retires the obsolete request; replaying that change creates no further intent. Existing scheduled reminders/escalations remain policy-controlled and are not duplicate import outreach.

Latest local proof: 1,442 backend tests passed; 300 Weekly Source tests passed, two optional tests skipped; all 42 selected Weekly Source/browser-isolation SQL verifiers passed in the disposable PostgreSQL 17 database. No real candidate or manager messages were sent by these rollback-contained tests. The read-projections stale-view fixture now deliberately changes its cycle version because automatic initial activation makes the former Office ASK step idempotent; the stale-write rejection remains required.

Protected run `36878555299` passed its verifiers but failed final contract comparison for two delivery functions. The earlier query repeatable had overwritten their later canonical definitions (comment/format identity differences). The earlier owner now includes the existing later delivery-target authority, preserving canonical per-target transport on incremental replay. Local contract read-back confirms both delivery definitions equal the previously approved contract. The new contract changes only nine Weekly Source routine entries (four new private helpers and five existing definitions), with no other catalogue section changed. Contract SHA-256: `ec93f4d4d374da9ed552f361aae7eed7b9c5299af7b076badba0adf7fdd01349`.

Protected run `36884277492` completed successfully: `VERIFIED UPGRADE release 20260926-banking-pay-stage2-h8-upgrade-5acb69905d4a for TEST`. All 147 hosted verifiers passed, taking 1,601.942 seconds (26 minutes 42 seconds); preparation/install/final comparison account for the remaining approximately three minutes. Only changed definitions were installed, but the previous failed release and unclassified database change required full verification. The automatic deployment system does not yet provide a broadly approved selective-verification fast path for this kind of database change.

Backend source `5acb69905d4abc7eff054ff6b24eca96eff005b5` deployed in the required order. Read-back confirmed all four active versions at 100%, each linked to a successful build of that exact commit:

| Worker | Active version | Build |
|---|---|---|
| test-cloudtms-backend | 8c2b21f5-a559-491a-864f-5f87990513ca | 13749903-e534-4c75-ad26-6b173f456eee |
| test-cloudtms-candidate-private-api | ff4bd78f-8cd8-493a-a3bf-a57aa743b7a4 | 5908ca19-891d-44df-8b02-eba4ca15b18f |
| test-cloudtms-candidate-synthetic-private-api | 68a23cbc-f4a6-4a87-a98f-cf1710c1af01 | 33fd12d9-c536-429a-8c5c-28bedc730b91 |
| test-cloudtms-candidate-broker | e1189445-4818-4b62-868a-f03d1d46bb97 | ba2387c2-15a1-49a9-9bee-b10b8b942836 |

Office `1a4c42039f57ca4e6aec3cd50491ec253dc4cc13` deployed through Pages run `36888823411`; coordinator completed at `2026-10-01T16:02:42.176Z`. Read-only HTTP comparisons proved the served index and all seven changed CSS/JavaScript assets match the saved release (normalised line endings only). Both previously mismatched dispatch functions were independently read from TEST and exactly matched the local PostgreSQL definitions. Status: **DEPLOYED_ACCEPTANCE_PENDING**. No hosted business-journey tests, real imports or test messages were performed after deployment.

Separate CI limitation, not concealed by the protected release success: Candidate PostgreSQL runtime fixture run `36884193671` fails on PostgreSQL 17.6 and 18.1 because its installation omits `private._weekly_source_effective_policy_v1(uuid,uuid,date)`. The immediately preceding commit's run `36878411218` fails identically at `tests/08082026_1040_candidate_app_policy_corrections_runtime_verification.sql:254`. This fixture issue remains outstanding; do not claim every GitHub check is green. Office's separate App-Ready job was still running at deployment read-back.

Cleanup after proofs: verified no other database clients and no other container sharing the volume, then removed only task-owned disposable container `codex-combined-workspace-20261001` and its anonymous volume `9dc39feda48ff3c11d847dde97602faa14d54e30f3736120aee4a1357ddc1c3c`. Docker volume use fell from 15.65 GB to 15.39 GB (about 260 MB). This local fixture is recreatable from Git; source/evidence, shared images, other tasks' containers and all hosted databases were preserved. No Docker virtual-disk compaction was attempted.

## Combined workspace release on 1 October 2026

The combined workspace, protected shift editor and resolved-question retention changes are now deployed with the automatic outreach release recorded above. The older deployment sections below are retained as historical evidence.

Source is isolated on `codex/weekly-source-combined-workspace-20261001` in both repositories, based on backend `fbf252fe7e32d25a59314720dc7ac60dc7b7b9c3` and Office `d85106b02a47f30ee6950e508c4eabdc187914bf`. The controlling addition is `WEEKLY_SOURCE_COMBINED_WORKSPACE_POLICY_20261001.md`.

Local evidence:

- Backend main suite: 1,442 passed. Weekly Source suite: 296 passed, two opt-in concurrency fixtures skipped, zero failed. Run the disposable-database suite with `--test-concurrency=1`: concurrent fixtures sharing application change counters caused one local deadlock; the complete sequential rerun passed without weakening an assertion.
- Office unit suite: 166 passed. Import-workspace Playwright: 39 local fixture tests passed; the three combined-view/file-detail checks passed again after final presentation changes. No hosted business acceptance was performed.
- All 44 Weekly Source SQL verification files passed across the initial and resumed local runs. The ordinary-pay fixture now removes only its own empty automatically created successor cycle before inserting the same period explicitly; its business assertions remain intact.
- Release-system and routing tests: 40 passed. Source integrity: 255 migrations and 677 repeatables. Local PostgreSQL 17 contract: `449913fcd32284836af92d38b28b0a8d05e3382ddd66df02a0a90bc66c55417e`.
- Contract review found only Weekly Source routine changes, the cycle client scope and protected-shift link columns/constraints. Existing enums, schemas, RLS policies, triggers, extensions and default grants are unchanged. No Banking Pay routine definition changed.
- Real disposable-PostgreSQL tests cover independent two-client publication/replacement, prepared-versus-checking finalisation, protected shift context without imports/submissions, changed-hours matching, unchanged resolved decisions in direct and signed-week comparison paths, and renewed questions for changed facts. Local browser fixtures cover exact independent batch requests, blocked exclusions, full-result seek and responsive layout. These do not prove real notification delivery or hosted finalisation/payment behaviour.

Publish using the existing automatic TEST coordinator, with fresh Cloudflare connection evidence, protected database verification, ordered Worker builds, Office publication and identity read-back. No LIVE, payment execution, token rotation or mobile-store build belongs to this batch. Hosted acceptance remains reserved for the user after deployment.

### Presentation review and first protected attempt

The user rejected the simplified fixture screenshot. Office publication was held;
the local coordinator was stopped, without cancelling the already-running protected
database workflow. The combined views are now checked in the actual Office shell,
not just the minimal fixture. Corrections cover flat sortable headings, filter and
cutoff spacing, selected-tab styling on programmatic navigation, compact stacked
actions and the protected-shift form. File details and protected-shift entry have
real-shell screenshots; these use local fixtures, not hosted business data.

Protected run `36876333760` failed the general RPC isolation census at backend
commit `90faa74e9352c2d2c87ccee0c38006f2a4ec6b02`. It measured
`802 / 76 / 0 / 8bff6786009ac9a493eee90f6ce92574`, against the old
`795 / 76 / 0 / 7107ce270790a01db97e27a8dbedb222` seal. A disposable
PostgreSQL 17 read-back reproduced both: removing only the seven new public
Weekly Source entry points from the census reproduced the old seal exactly.
The verifier now records that additive inventory; the unchanged service-missing
count and zero browser-executable requirement remain enforced. The complete
isolation verifier passes locally. This is not yet proof of a successful retry
or of application publication. The failed install may have applied definitions
before verification stopped; its ledger must be recovered through the protected
workflow, never manually marked verified.

After the visual correction, all 40 local import-workspace Playwright checks and
all 166 Office unit tests passed. The 40 release-system/routing tests and database
source/contract coupling checks passed again. Real-shell captures include the
combined Finalise/Queries views, file details and protected-shift entry at 390px
and 1700px. These do not claim hosted acceptance.

## Previous release status on 1 October 2026

**DEPLOYED_ACCEPTANCE_PENDING.** The automatic coordinator completed the actual
protected TEST release and every ordered application publication. The historical
failed-run notes below are retained as an audit trail, not the current status.

- Protected run: https://github.com/kierarthur/cloudtms-backend/actions/runs/36794968271 — SUCCESS.
- Installed release: `20260926-banking-pay-stage2-h8-upgrade-083bb9d6a117`, **VERIFIED**, read back from `cloudtms_test_clone`; completed `2026-10-01T00:41:19.921Z`.
- Database/runtime commit: `083bb9d6a117229d9e1f256f340cf4aa4ae0a3e9`. All **147 UPGRADE verifiers passed**, zero failed; contract `89cbb1631aeaffa2001cd057161c19cd062e1008bd1b9fd66085c6ba4618c2a6` matched. Zero pending migration/repeatable files were reinstalled.
- The previously failing pending-entitlement verifier passed in 26.175 seconds overall. Its eighty-cycle assertion body remains byte-identical after LF normalisation: SHA-256 `568878abbb5e3c84cc77866ddc4e8800e24d438d91b8eaf55d8673fc1ad7ef7f`.
- Office commit `d85106b02a47f30ee6950e508c4eabdc187914bf`, Pages build `1251394008`, built successfully. Served `index.html`, Weekly Source CSS and both changed JavaScript files returned 200 and matched the reviewed files after LF normalisation.
- Coordinator finished `2026-10-01T00:46:39.844Z`. Active version read-back showed all four Workers at 100% on successful builds of the exact runtime commit, in the order below.

| TEST Worker | Active version | Successful build |
|---|---|---|
| test-cloudtms-backend | ca4536eb-9ad6-4bfd-9259-b3bd2b0ac5fe | 6a6242c5-4c79-458a-a140-36191a205606 |
| test-cloudtms-candidate-private-api | 9d1cf5f9-8637-4490-8aa9-1d7112700677 | 05360151-a85f-4a1a-8f09-d5b33a2654eb |
| test-cloudtms-candidate-synthetic-private-api | 4f1741cb-60f2-498c-a9de-d0502cb8054d | 7777491c-d845-4828-b3c3-dfc493c0c003 |
| test-cloudtms-candidate-broker | 72480aec-ff2b-4947-89f7-6b9323676d45 | 4658f77e-ecaf-4332-b07d-716c58f36a5a |

The new coordinator's full-release path is now proved end-to-end. Route-selection
negatives and application-only/component decisions passed local unit tests;
this run does not claim an actual hosted execution of every alternative route.
Instructions are in `docs/AUTOMATIC_TEST_DEPLOYMENT.md` and linked by AGENTS/Bible.
No existing tokens were rotated; no other account's connector or LIVE was changed.
Arthur's diagnostic Worker source/dependencies/configuration are preserved as an
unchanged, credential-free snapshot under
`codex_outputs/arthurrai2006-test-lab-source-20261001/` with a file-hash manifest.
The shared original files in the primary dirty worktree were not modified.

Local cleanup: after proving no other client used it, removed only owned container
`codex-ws-full-fixes-20260930` and its disposable volume
`1c4f29ea1aafa672b4396642178db8e6df84e42c4207851d713673bf2fa93a7e`.
Docker's total volume inventory fell from 16.32 GB to 15.39 GB during cleanup.
Source and evidence remain; the disposable database can be recreated from Git.
Other containers/shared image were preserved, and no Docker virtual disk was
compacted while other tasks were running.

**Stop here for the requested model switch.** No hosted functional browser/phone
acceptance was run. Real imports, linking, notifications/manager routing,
HealthRoster choices, dropdown/layout journeys and device responses remain the
explicit next acceptance stage; deployment identity checks do not prove them.

## Scope and authority

User-approved TEST-only release. Deploy database authority first, compatible normal TEST backend second, Office frontend last. Pause after deployment confirmation; hosted browser/phone functional acceptance is reserved for the user's model switch. No LIVE, payment execution, diagnostic outgoing messages, historical route reset or unrelated changes.

Backend base: `2841abbfc5e799e72787d6e09acfa0e2c2de7d9d`.
Office base: `fd90180e1d8ca0c262155bc4b0c7b7032650f7a8`.

## Implemented boundaries

- Explicit checking/preparation use; NHSP previously released files never become finalisation reports. Current-file preparation is version/hash/permission bound.
- HealthRoster early preparation/finalisation with explicit exclusion acknowledgement for non-finalised positive shifts, while retaining prior-position reversals and real blockers.
- Queries includes Office checks for unlinked/inactive source identities, client/contract/band/source problems and existing rate warnings. Source name, CCR, Trust, reference, date, actual hours and breaks are visible. Eligible linking/contract choices recheck the immutable row through existing projection owners.
- Independent current generations for missing-week submissions and signed-hours questions. Exact requested-week suppression and manager exception; genuine Office manager-direct routes are preserved. Reminders use the current relevant request and retain cooldowns.
- Policy amendment and editable policy successor; wider desktop Queries, stacked actions, non-wrapping status, clearable Trust and refresh-safe keyboard focus. Contract review loads fresh full contract details before opening.

## Pre-release evidence

- PostgreSQL 17.11 local task container `codex-ws-full-fixes-20260930`, loopback only. No hosted diagnostic data mutations.
- All 148 NEW portable verifier files passed against the final source definitions, including security/ACL, source finalisation, notification delivery and first-use checks.
- The first full rehearsal and an isolated unchanged pending-entitlement verifier timed out with local default `jit=on`. Repeating the unchanged verifier with process-local `jit=off` passed in 7.973 seconds; the full 148-file run then passed with `jit=off`. No verifier assertions, application timeouts or financial function definitions were altered for this.
- Generated contract hash: `89cbb1631aeaffa2001cd057161c19cd062e1008bd1b9fd66085c6ba4618c2a6`.
- Reviewed contract delta: 6 added Weekly Source routines; changed Weekly Source routine definitions only; 3 new private tables and their identity sequence; the independent request pointer/index and scoped manager-event foreign key. No unrelated routine, existing ACL, RLS, trigger, enum, extension or default-privilege drift.
- Backend main suite passed 1,442 tests. Weekly Source suite passed all 283 tests with both concurrency fixtures enabled on the task-owned local container (none skipped). The 31 release-system tests and contract/source coupling passed. Source integrity: 253 migrations, 676 repeatables. Toolchain, dependency provenance and tracked credential scan passed.
- Office: 46 focused unit tests and 37 exact local import-workspace Playwright tests passed. These are mocked/local shell checks, not hosted acceptance. An accidentally broad browser invocation was cancelled; its unrelated failures are not counted as feature results. The local configuration is now limited to Weekly Source specs.
- Policy renderer generated 33 images from 30 screens with its policy assertions enabled. Committed examples are labelled mockups, not hosted screenshots.
- Fresh read-only TEST performance check: 7 sessions, no lock waiters, no ungranted locks. The original connection exhaustion's initiator remains unproved; this release does not claim a permanent connection-pool repair.

## Deployment

- Clean NEW engine release `20260926-banking-pay-stage2-h8-new-1162454c55fa` finished VERIFIED in disposable PostgreSQL 17, including all verifiers and exact contract comparison. Its empty database was created with the repository's local provider-prerequisite fixture before applying the complete release. No installed application state or business data was copied from hosted TEST.
- Database commit: `1162454c55fa4c166d21d9b8c736a30059d4def4`, published to backend `test`.
- Protected TEST UPGRADE run `36782317336` failed at 22:21 UTC in the unchanged pending-entitlement verifier, line 2943 (`verify_b42_audit_growth`), with a statement timeout while its temporary audit counter was updated. All 253 migrations and 676 repeatables had installed, but this is not a VERIFIED release. Runtime and Office remain held back. The next repair is TEST release-session-only `jit=off`, matching the successful complete local NEW profile; no verifier, timeout or application definition is weakened.
- Release-profile correction `eb57b115a11cbdb38ac2adb821379965e97c2b5f` adds `jit=off` only to protected TEST release connections and explicitly checks that setting. The unchanged complete pending-entitlement verifier passed locally in 2.46 seconds with rollback; release-system tests passed 32/32 and source integrity/contract coupling passed. No SQL definitions, application settings, timeouts or assertions changed.
- Protected retry `36785406850` FAILED at 22:51 UTC at the same verifier line 2943. The workflow confirmed `jit=off`; therefore JIT alone is not an established explanation or sufficient hosted repair. The cancelled statement was the B4.2 audit-growth DO block (40 old-path cycles plus 40 shipped-watch cycles); cancellation surfaced in `ws_verify_capture` while inserting a tracked key. That location does not prove the capture table is the bottleneck. No assertion mismatch was reported. Do not repeat full APPLY unchanged, increase timeouts, reduce the 80-cycle proof, change financial functions or mark VERIFIED.
- Next diagnostic: time the exact unchanged failing verifier's sections on hosted TEST within the existing rollback safety boundary, inspect relevant plans/settings, and compare with local PostgreSQL. The active arthurrai2006 Operations connector is fixed/read-only. The currently available SQL rollback tool belongs to Kier Codex and must not be borrowed. A separate account-appropriate transaction lab or an explicitly approved narrowly scoped protected diagnostic workflow is needed for this targeted hosted execution.
- Backend `test` remains at `eb57b115a11cbdb38ac2adb821379965e97c2b5f` (database source and release profile, not the new runtime). Office `main` has not been changed by this task. The complete runtime is preserved on `codex/weekly-source-profile-tie-20260930`; Office on `codex/weekly-source-import-ui-20260930`. The owned local Docker proof remains required for diagnosis and is deliberately retained. No hosted browser/phone acceptance has begun.
- Compatible backend runtime commit saved on the feature branch: `60e4364864d82a27587694433b12a18f84af5591`; awaiting verified DB release before publishing to `test`.
- Office feature commit: `38ebc46fdc5153de6c815463082eae1ae9e0ea4f`, followed by `d85106b` to refresh the three changed CSS/JavaScript cache-version labels. The 37 import-workspace browser checks passed again after this update. Awaiting DB and backend release before publishing to `main`.

## Hosted acceptance reserved for the model switch

### Automatic release controls, 1 October — local implementation

Timeout root cause corrected: the earlier assumption that B4.2 exhausted the 120-second harness budget was wrong. Fresh rollback evidence showed the entry budget at 120 seconds, then an earlier real RPC left `statement_timeout=3s` in the transaction. B4.2 measures eighty calls in one DO statement and needs approximately 5.3 seconds on hosted TEST. The verifier now captures its entry budget (never above 120 seconds or a smaller existing nonzero limit), restores it only around that aggregate measurement, then restores the inherited application limit. No application definition, census pin, loop count, assertion or runtime timeout changed. The entire verifier passed against hosted TEST even with a deliberately lower 20-second entry budget; fresh rollback verification confirmed the fixture policy disappeared. Local release/routing tests: 40 passed; integrity: 253 migrations / 676 repeatables. This is rehearsal proof, not yet a VERIFIED install.

GitHub rejected the first `test` push because the saved feature branch contained merge commit `cac7e578`; it changed no target source. Preserved that feature branch and rebased a new `codex/automatic-test-release-20261001` branch onto remote TEST. Its tree was proved identical, then published as `c528775cdb5b38fbaf18d07be0138410ec7e91b9`. Protected read-only AUTO_PLAN run `36793405003` passed both source and hosted planning gates. Receipt proves zero pending migrations/repeatables, exact prior FAILED release `eb57b115…`, and automatic FULL_UPGRADE selection. Thus the new system does not reapply unchanged definitions but correctly refuses to treat installed-but-unverified SQL as success. No further full APPLY or application deployment has been performed.

Workflow implementation committed/pushed as `9be464cc`. Bootstrap read-back: normal backend active version `9a2a1961-4a07-4d65-9fb0-d79816b0f1d4` maps to successful Cloudflare build `e81e7a8a-4037-4608-a5a9-63193c405495`, source `eb57b115a11cbdb38ac2adb821379965e97c2b5f`. Created `deploy/cloudflare/test-cloudtms-backend` at exactly that source, then changed only trigger `5e1207d4-e550-4a74-8135-15d68b2060fa` branch filter. Read-back confirms new branch and unchanged token identity, repository, root and deploy command. No new runtime was deployed by this bootstrap.

Added a TEST-only route selector, protected AUTO entry and desktop coordinator. The selector requires installed identity, immutable ledgers, actual contract and verification-authority evidence; an interrupted full release cannot be bypassed with a smaller component. The coordinator holds application branches until database success, promotes private Candidate authority before its broker, then Office, and records exact workflow/build identities. No tokens are added or rotated by this workflow. Instructions are in `docs/AUTOMATIC_TEST_DEPLOYMENT.md`, linked from repository AGENTS and the release Bible. The normal backend's current `test` trigger must be decoupled to its documented dedicated branch before publishing combined runtime source.

Local route/release tests: 39/39 passed. Protected source inventory remains 253 migrations/676 repeatables. This is not yet an end-to-end deployment proof. Hosted rollback diagnostics reproduced the unchanged 80-cycle timeout with JIT off and both generic/custom plan modes; a diagnostic one-cycle variant completed the remaining checks but is not release proof. No application definition or verifier assertion was weakened, and every diagnostic confirmed rollback and fresh-connection fixture-policy absence.

### Account-specific diagnostic connection added 1 October

User approved Arthur's separate rollback-only connection and new credential, with no existing token rotation. Installed direct MCP/plugin `cloudtms-miget-transaction-lab-arthurrai2006`; Worker `codex-arthurrai2006-miget-transaction-lab`; Windows user variable `CLOUDTMS_MIGET_TRANSACTION_LAB_ARTHURRAI2006_TOKEN`. It reuses the existing transaction lab source unchanged and binds only agency TEST. Existing Operations, Kier and shared lab routes remain untouched. Authenticated HTTP discovery and rollback/fresh-connection absence proof passed on cloudtms_test_clone; anonymous access returned401. Restart Codex fully and start a new task to load the native tool before continuing the exact failing-verifier diagnosis. No further protected release was run and runtime/Office remain held. New configuration and proof README are in the primary backend's `infra/miget/cloudtms-miget-transaction-lab-arthurrai2006`; workspace AGENTS records account routing. Preserve unrelated primary-clone changes. Do not infer full feature deployment from this connector setup.

Do not call these proved by local fixtures: real-file upload and subsequent linking/recheck; real candidate request/push delivery; manager routing under each genuine authority; HealthRoster early finalisation/exclusion; device response/stale-request behaviour; actual Office keyboard/dropdown/layout journeys. Preserve the user's real data and obtain narrowly scoped authority before acceptance tests that create messages, finalisation or financial state. Check installed definitions/deployment identities first and use read-only observations where possible.
