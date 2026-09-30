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
- Backend main suite previously passed 1,442 tests; Weekly Source suites and focused first-use tests passed. Source integrity: 253 migrations, 676 repeatables. Toolchain, dependency provenance and tracked credential scan passed.
- Office: 46 focused unit tests and 37 exact local import-workspace Playwright tests passed. These are mocked/local shell checks, not hosted acceptance. An accidentally broad browser invocation was cancelled; its unrelated failures are not counted as feature results. The local configuration is now limited to Weekly Source specs.
- Policy renderer generated 33 images from 30 screens with its policy assertions enabled. Committed examples are labelled mockups, not hosted screenshots.
- Fresh read-only TEST performance check: 7 sessions, no lock waiters, no ungranted locks. The original connection exhaustion's initiator remains unproved; this release does not claim a permanent connection-pool repair.

## Deployment

Pending. This document does not claim a successful push, protected APPLY or hosted deployment. Record exact commits and workflow/build identities after they succeed.
