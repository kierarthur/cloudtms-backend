# Weekly Source full fixes — release evidence

## Latest TEST release and read-only acceptance — 3 October 2026

This section supersedes the historical "acceptance pending" and "stop for model
switch" instructions below. The code and deployment described here are complete;
the older sections remain as an audit trail of earlier attempts, not the current
deployment status.

- Backend/TEST database source: `bf80f7e5e20fffdaac38a244282989b7278c7d8b`, pushed to the feature and `test` branches. Protected database run `37095471259` passed full verification and final installed-contract comparison. The subsequent managed release run `37099847641` returned `VERIFIED_OR_EXACT_UNCHANGED` for that same source. The database release was not bypassed for Office-only follow-up changes.
- TEST runtime remains on the four successful versions of that exact backend commit: backend `b96204ff-2e42-4763-81e2-1412f4065710`; candidate-private `2a7f03da-56fd-4992-b52c-a43fc3e97dac`; candidate-synthetic `8b08bd95-11fe-4d41-9425-90d211fe33ba`; candidate-broker `a87c3cbc-bb4a-4127-aee9-fdb3e0d8a0ba`. The last coordinator correctly reported each `UNCHANGED`, rather than republishing it.
- Office `36d2b7eb5e32b4afd2c1ea24f3b2ef2e1d6c440d` was pushed to `main` and Pages build `1256650590` succeeded at `2026-10-03T05:29:43.859Z`. The managed receipt is `.codex-tmp/test-deployment-receipt.json` in the release worktree and reports `DEPLOYED_ACCEPTANCE_PENDING` until functional business journeys are exercised.
- Local verification: backend main suite 1,443/1,443 passed after the canonical SQL-generation correction; targeted Office Playwright suite 49/49 passed after the latest candidate-picker context and source-reference change. Those tests did not send messages, finalise a report or authorise pay.
- Live signed-in TEST Office, read-only browser acceptance: the Imports list shows the current previously released NHSP checking-hours file and current backing report, while obsolete files are absent. Queries shows Kier's hours disagreement separately from Office checks. The Baljit Office-check row names Rai-Baptiste Baljit, source ref `CCR-02611`, booking `155154209`, Trust, date and imported hours. Opening **Link candidate** prefills `Baljit`, shows both source identifiers and produces one candidate search result; **Apply was not clicked**. No candidate link was changed.
- The pending 27 September report is in Finalise (one ready report, zero blocked reports); its detail lists two ready shifts, marks the 30 September cutoff as passed/not finalised, and the **Send back to Queries** action fits in the 1193px viewport with zero table overflow. The already-finalised 20 September report appears in History with period and finalisation-date filters and a View report action, not as a pending report. The browser console showed no errors in that acceptance pass.
- No real import was uploaded, no candidate/manager contact was sent, no finalisation/pay/payment action was taken, and no MyTMS code changed in this release. Therefore live outbound notification delivery, manager replies, HealthRoster early-finalisation choices, real-file reconciliation, mobile response and financial state transitions are **not** claimed as end-to-end proved. A USB-phone run was not necessary to validate these backend/Office-only follow-up commits; it would require separate non-destructive device acceptance and, for the response journey, a controlled test message.
- No LIVE resource, token, other account route or unrelated MyTMS worktree was changed.
- The earlier Candidate PostgreSQL runtime CI failure was an isolated fixture mismatch, not a hosted TEST install failure. Its Candidate-only database intentionally omits Weekly Source; the fixture now supplies empty outreach read shapes, an `IMPORT_ONLY` view-only policy for its import-authoritative test record, and a deny-only completed-pack mail predicate, plus the two mail-outbox columns already present in the real schema. No release SQL or Worker code changed in this CI repair. GitHub run `37100838967` passed its complete Candidate runtime suite on PostgreSQL 17.6 and 18.1; backend source tests also passed locally (1,443/1,443). The later `test` commits are documentation/fixture-only and do not change the deployed runtime or installed database definitions.

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

## Protected-pay retry repair, 4 October 2026

The hosted browser reproduced Kier Arthur's manually queried imported shift on
8 September (01:00–04:00, 30-minute break), extended to 05:00. The staged request
failed with `WEEKLY_PROTECTED_C1_INITIAL_ACTION_INVALID`: an earlier failed
preparation had already created the family but had not published a generation.
The prepare owner incorrectly classified every existing family as an amendment.

The prepare owner now distinguishes an empty family from a published decision.
A fresh request for an empty family remains its first approval. Exact retries
retain their immutable request kind; the C1 owner accepts an older misclassified
AMEND only when the family has no generation, generation number zero,
PENDING_APPROVAL lifecycle and no historical generation row. It still publishes
generation one as INITIAL_APPROVAL. Existing published decisions retain the
normal amendment path. No run, entitlement or ledger is rewritten to conceal
the earlier failure; identity, overlap, authorisation and C1 guards remain.

Pre-install evidence: backend suite 1,445 passed; Weekly Source unit harness
passed; the four focused C1 SQL contract tests passed. Arthur's account-specific
rollback lab executed the revised owners and preparation verifier, including a
new request retrying the same previously prepared event: all 36 statements
passed on cloudtms_test_clone, rollback succeeded, and a fresh connection proved
fixture absence and restoration of both installed definitions. The generated
contract delta contains only the two intended routine definition hashes, with
all signatures, ACLs, security settings and other catalogue objects unchanged.
The second clean PostgreSQL 17 NEW replay completed VERIFIED with the reviewed
contract hash 29b2b32b504679285e10839f15f35afa02dd717e32065cbcc25f7bedd6467ced;
every portable verifier passed. Release-system/routing/export tests passed 41/41.

The user requires the real Kier browser save to pass before the next Office
publication and Play release. Those acceptance stages remain pending until the
protected TEST database installation completes; this section is not a claim
that either the save or mobile-store publication is already complete. No LIVE,
provider, payment execution, outgoing diagnostic message or Policy X change.

## Self-bill expenses and populated-database invoice verification, 6 October

The user confirmed that self-billed Candidate expenses must be separate even
when ordinary hours are not import-authoritative. The shared effective-settings
resolver now enforces that rule using the existing Client/Contract override
precedence. Ordinary Weekly/Daily hour entry remains ordinary; import authority
and frozen financial snapshots are unchanged. No Banking guard or economic owner
is altered. The user will withdraw the existing approved Arthur Rai claim;
this release does not repair, withdraw or rewrite that claim.

The earlier protected release failed because the invoice fixture asserted that
the entire populated TEST database had no first-authorised/TARGET history and
exactly one current financial row. Its own first-preparation assertions and
projection receipt lookups now use exact fixture identities. Insert capture
rejects changes to unrelated rows, and complete watched-row fingerprints must
remain unchanged after the entire invoice integration. All existing invoice
rotation, ordinary positive controls and protected-family exclusions remain.

Pre-publication proof: the full effective-settings regression passed in Arthur's
server-owned agency TEST rollback lab with a fresh connection proving restoration
and fixture absence. In an isolated PostgreSQL 17.11 clone, the staged resolver
produced exactly one routine-definition change in the generated contract; all
business rows, ledgers and other security/catalogue authority stayed unchanged.
Both complete SQL regressions passed. Invoice integration also passed with genuine
unrelated Source financial history; the old global assertion failed on that same
history. Namespace collision and unrelated-row mutation were correctly rejected.
Every case rolled back. The owned clone was removed; the shared golden database,
role/HBA and complete fresh snapshot were unchanged. The native test window was
returned to Handover 2. Local main suite: 1,450 passed; focused policy/fixture suite:
25 passed; integrity and generated-contract coupling passed. This is pre-install
evidence, not a claim that the protected hosted release has succeeded.

## Complete historical-row snapshots, 6 October — verifier-only correction

Protected run `37457085853` passed the self-bill authority and corrected invoice
integration checks, then stopped at read-projections line 4162 with PostgreSQL
`54000`: total JSONB array elements exceeded 268435455 bytes. Arthur's agency
TEST rollback lab reproduced that exact error when aggregating all historical
Workbench jobs. The populated table had 34,647 jobs. This was a verification
snapshot representation failure, not an application or payment failure.

All full-history copies in the read-projections, Candidate-view and audit/export
verifiers now retain each pre-boundary job as an individual temporary row. Exact
before/after job IDs and the original permitted-metadata checks remain. Later
complete-history comparisons use full-row SHA256 digests, count and multiplicity;
the Candidate-variant savepoint comparison uses the same method. Economic-row
comparisons in those three verifiers and their ordinary-pay companion share
`support/06102026_1410_source_full_row_fingerprints.sql`. Every complete row is
hashed before aggregation: no sampling, truncation, DISTINCT or LIMIT. All
existing financial-drift, non-target-job and worker-completion guards remain.
The helper exists only in pg_temp inside rollback verification; no installed
routine, payment policy, Banking owner, timeout or verifier path changes.

The complete ordinary-pay, read-projections, Candidate-view and audit/export
verifiers passed in an isolated PostgreSQL 17.11 clone at the current self-bill
authority. Update, duplicate-multiplicity, same-count replacement, restoration
and empty-count fingerprint controls passed. Every run rolled back and its full
fresh snapshot matched. The owned clone was removed; shared golden, roles and
HBA were unchanged. Receipt `READ_FINGERPRINT_QUALIFICATION_f7a375e4-b2a5-4a4a-9ce1-0e57d343bea0.json`
SHA256 `b7f541263b82df8fd2354ca1d8d9a3c48323fa47b2377202dcb62b06cbd9374d`.
Arthur's actual populated-history proof also passed with complete per-job rows
and fresh-connection absence after rollback. Local source controls: six passed;
main suite: 1,450 passed; Weekly Source pure harness, integrity and contract
coupling passed. Remaining full-row aggregates were reviewed and are bounded to
exact fixture heads, Candidates, Timesheets or evidence—not complete history.

The next deployment must rerun all 149 protected hosted verifiers through
`npm run release:test`, using fresh connection evidence. No smaller route or
assertion bypass is authorised. Runtime and Office remain unpublished by this
retry until that full protected release is VERIFIED. The actual Kier protected
pay save must then pass before Office publication, as the user required.

## Hosted economic-drift diagnosis, 6 October

Managed run `37462230684` at `9d4eb39632cd14362f81dea5ecaf50b351741ccd`
passed standalone ordinary-pay (189,978 ms), finalisation (21,281 ms) and
correct-final-source (211,903 ms), then failed invoice-admission (363,455 ms).
Its included ordinary-pay fixture raised `PAID_FIXTURE_FANOUT_ECONOMIC_ROW_DRIFT`.
Runtime and Office publication were held. This is not yet attributed to a
fixture defect, concurrent data change or application mutation.

The complete invoice-admission closure passed in an owned PostgreSQL 17.11
clone in 11,579 ms with every original assertion retained; only an in-memory
exception DETAIL added changed relation names. Receipt
`INVOICE_FINGERPRINT_DIAGNOSIS_d18b5bc2-b5fb-45c2-be1b-21b932b3cc73.json`.
The clone was removed and the original golden, role/HBA/profile and complete
fresh snapshot were unchanged. Handover 2 reported no hosted business writes
during the failed release window; its shared native window was returned.

Arthur's hosted rollback lab showed stable complete-row fingerprints for all
eleven economic relations during a short read-only diagnostic. A separate
234-statement NET-fixture rehearsal retained every predicate but hit its existing
120-second statement limit in `ws_verify_other_jobs_fingerprint`; it is NOT a
passed reproduction or grounds to extend the timeout. Both rehearsals rolled
back and fresh connections proved the temporary/fixture objects absent.

Both ordinary-pay economic-drift exceptions now retain their original SQLSTATE,
message and unchanged full before/after predicates, adding DETAIL containing
only changed relation names. No hashes, row values or financial payloads are
disclosed. Hosted rollback positive/negative diagnostic controls passed; seven
fingerprint source controls and the 1,450-test main suite passed, with integrity
and contract coupling unchanged. The next managed release uses these diagnostics
without bypassing the outstanding full verification or publishing Office early.

Separately verified in installed agency TEST: expense-invoice routing is enabled;
the installed delivery owner chooses the frozen configured expense email for
the EXPENSE stream, preserves self-bill hours suppression and blocks missing
expense recipients. The self-bill separation correction does not change this
existing delivery owner, configured addresses or frozen historical claims.

## Invoice-issue fixture statement boundary, 6 October

Managed run `37466322099` at `b651b28446e16698e4860170b6925a1aeb302737`
passed invoice admission (391,740 ms), invoice batch integration (91,417 ms)
and the previously oversized read-projection fixture (287,588 ms). It later
stopped in `02092026_1833_weekly_source_invoice_issue_validator_v1.sql`:
the single WP-33 scenario block exceeded the statement timeout while checking
case A5. This is not an economic-drift assertion. The user confirmed no TEST
edits during the earlier drift window; that earlier cause remains unproved.

The verifier now pins the same two subjects once in a temporary singleton row
and invokes each of the 16 original scenario sections in a separate SQL
statement, all inside the same existing outer rollback-only transaction.
Every original scenario assertion and raise-to-undo subtransaction is retained
byte-for-byte and in order (scenario-body SHA-256
`fd907b7225ff1e750efaecc6b4ba716562435307a92a84e2c0443c3afe3b267a`).
There is no runtime-function, economic-policy, timeout, owner, contract, grant,
release-ledger or verifier-selection change. Source tests prevent regrouping
the calls into one long statement and prevent state-dependent subject reselection.

The complete original verifier and the complete split verifier both passed
in an independently owned PostgreSQL 17.11 container at the same current
self-bill authority: 14,511 ms and 14,971 ms respectively. The split run also
proved refusal of unknown cases and false/null assertions. Receipt
`INVOICE_ISSUE_STATEMENT_QUALIFICATION_3524a3bf-02e5-44f7-be73-e3ef5b4d82cd.json`.
The source golden was copied only through a read-only consistent dump, without
using or modifying Handover 2's active clone. Both task-owned containers/volumes
from qualification were removed; the existing six containers were preserved.
No separate image or build cache was created. The 1,450-test main suite,
16 focused invoice/scenario tests, integrity and contract coupling passed.

This is local qualification, not a completed hosted release. Runtime/Office
publication and the actual Kier Save acceptance remain held until the full
managed hosted verifier set succeeds at the new reviewed commit.

## Import Review generic-plan correction, 6 October

Managed run `37472572598` at `b066d63cabf7424552d22ab745bebefb5c20fc8a`
passed invoice admission and the preceding protected-pay checks, then stopped
in the complete Mode A dispatch verifier after 54,660 ms. PostgreSQL raised
SQLSTATE `54000`: a string buffer already containing 1,073,741,809 bytes could
not grow. Its stack identified the real Import Review catalogue INSERT, not
the financial-fingerprint assertion. Runtime and Office publication stopped.

Arthur's rollback-only lab reproduced that exact buffer error with the complete
original verifier under `force_generic_plan`; the default-plan original passed.
The smallest tested correction is `classified AS MATERIALIZED`: PostgreSQL
classifies the already bounded source rows once and reuses those results instead
of expanding the lateral classification joins into downstream query plans.
The corrected complete verifier passed with the forced generic plan, and a fresh
connection proved restoration of the original routine plus absence of fixtures
and capture triggers. This establishes a reproduced planning condition and its
tested correction; it does not establish that every previous failure had this cause.

The new complete replacement authority preserves the previous repository body
byte-for-byte apart from that one materialization keyword. The predecessor
canonical definition MD5 `efbe6d963014519a39f9588e54b39ed2` matches both hosted
TEST and the source-derived PostgreSQL 17 golden; the original body SHA256 is
`396167f93338a59c9ca1f668253a5931e6052bf0dc81c15506d93ba3f8e7ddfe`.
Signature, owner, SECURITY DEFINER, search path, volatility, browser/service
denial, 501-row classification boundary and 5,000-action ceiling are preserved.
No Banking Pay, protected-pay, invoice or source-resolution rule changes.
No runtime query-plan setting, timeout, memory allocation or verifier exclusion.

Local PostgreSQL 17.11 qualification compiled the exact staged LF replacement,
compared its full generated predecessor contract, then proved exactly one
routine-definition hash change. Complete original custom-plan and corrected
custom/generic/automatic Mode A checks returned identical decisions (896/948/
735/970 ms respectively). Current Daily compatibility security passed; the
complete split invoice-issue verifier also passed under the generic plan
(20,818 ms). Receipt `CATALOG_PLAN_QUALIFICATION_de814a8d-9f54-48bb-8612-9b8a21413327.json`.
The local seal generated contract `a3e6bac5b0c6a09cca601acbd9b8fde6f6b4afd5086383b73999f148c3e50980`.
Every task-owned diagnostic container and volume was removed; the shared golden
was read only. An additional historical, non-release Daily diagnostic refuses
the provider-mapped owner because it hardcodes `postgres`; it is not a substitute
for the current provider-neutral mandatory security verifier, which passed.

The existing mandatory Mode A verifier now forces a generic plan only inside
its rollback transaction, retaining all scenarios and assertions so the exact
planning regression is continuously exercised. All 149 hosted verifiers still
have to pass in the canonical managed release. Actual Kier Save and ordered
runtime/Office acceptance remain required before publication is claimed complete.

## Same-file Office recheck recovery, 7 October

An Office candidate choice was committed before the replacement source comparison
failed. The row builder correctly retained an established work event, but the SQL
lineage guard rejected an older published resolution of the *same immutable row*.
The recheck had already invalidated the previous publication, so the combined
workspace incorrectly displayed zero Office checks. No successful candidate
recheck or resolved charge warning can be inferred from that empty display.

The corrected guard admits same-row lineage only from an older published
generation of the same upload with its unchanged source fingerprint. All existing
candidate, client, group, contract and current-generation authority fences remain.
A chronological successor repeats exactly the two changed public definitions,
preserving their service-only execution and owner/security configuration.

While a saved recheck remains incomplete, the permitted previous source rows stay
visible in Office checks, explicitly labelled as incomplete. Their stale ordinary
actions are replaced by an exact saved-request retry for the original Office
actor. The attention panel cannot falsely say that no decisions remain. Later
failures report that the selection was saved but the comparison did not finish;
a refusal before saving does not make that claim. Retrying does not close Queries
or manufacture a new choice, request identity or work event.

The exact affected two-row TEST upload passed a server-owned rollback rehearsal:
both rows visible before recovery, the established charge warning preserved
afterwards, and the candidate-linked row progressing to its remaining client
eligibility check. Fresh-connection rollback verification restored the original
unpublished state. Client membership, pay, invoices and the original raw source
are not altered by the repair. Hosted installation and exact saved-request
recovery remain pending until the managed release passes.

Local verification: 1,476 general backend tests; 393 weekly-source tests passed
(13 independent opt-in harness tests skipped); 215 Office unit tests; all 61
Office import browser tests passed. Desktop and 390px warning/recovery captures
were visually inspected. The cross-repository delivery test used the existing
reviewed MyTMS source worktree rather than a missing default sibling directory.

Two clean PostgreSQL 17 NEW installations passed, including the fresh replay
against the reviewed contract seal. The contract delta is exactly the two
intended routine definition hashes, with no permissions or other schema change.

### Hosted installation and original-request acceptance

Backend commit `1860952f620aa503440e179bc15e67de9f908899` passed protected
TEST release run `37691061087`, including all 150 required verifiers. Installed
and approved contract hashes both equal
`2b63b62b76640d38472152229148c4bedaaf88bb2e77a037d9db8a979eb182bc`.
The coordinator then published the normal backend, private, synthetic and public
Candidate Workers in order; API read-back proves all four serving that exact
successful build at 100%. Office commit `d35c40c57f5edb8a8e7fd205fd05ed85583f95f5`
was built by Pages and its three recovery asset versions were read back.

The original Office actor retried the exact previously saved request once in
the real TEST browser at 22:39:37 UTC. The replacement became CURRENT at
generation 6, Queries stayed open and both Office checks remained visible.
The active candidate choice is now linked; its remaining CLIENT_NOT_FOUND is a
genuine absence of eligible client membership, not another candidate failure.
The established other row retains its original work-event identity and complete
row fingerprint, with its provisional charge warning still visible. No final
source snapshot or billing movement was created, and no client eligibility or
charge decision was changed. Fresh read-only database assertions confirmed this.

Actual before/after browser captures and exact deployment/acceptance receipts
are retained in ignored task evidence. The hosted Candidate/Client browser
regression also passed. Its accompanying Office unit job exposed cache-version
expectations already stale in the predecessor release. The final maintenance
amendment updates only those exact test expectations and records acceptance;
all 235 Office/UI tests pass locally, without changing deployed runtime assets,
SQL, security or financial behaviour. Its managed publication and hosted CI
result are recorded separately in the task receipt.
