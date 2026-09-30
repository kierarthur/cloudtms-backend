# Weekly Source full fixes — release evidence

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
- Compatible backend runtime commit saved on the feature branch: `60e4364864d82a27587694433b12a18f84af5591`; awaiting verified DB release before publishing to `test`.
- Office feature commit: `38ebc46fdc5153de6c815463082eae1ae9e0ea4f`, followed by `d85106b` to refresh the three changed CSS/JavaScript cache-version labels. The 37 import-workspace browser checks passed again after this update. Awaiting DB and backend release before publishing to `main`.

## Hosted acceptance reserved for the model switch

Do not call these proved by local fixtures: real-file upload and subsequent linking/recheck; real candidate request/push delivery; manager routing under each genuine authority; HealthRoster early finalisation/exclusion; device response/stale-request behaviour; actual Office keyboard/dropdown/layout journeys. Preserve the user's real data and obtain narrowly scoped authority before acceptance tests that create messages, finalisation or financial state. Check installed definitions/deployment identities first and use read-only observations where possible.
