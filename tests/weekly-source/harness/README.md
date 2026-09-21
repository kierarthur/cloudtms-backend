# Weekly Source executable test harness

This directory is TEST-only infrastructure for Plan 6.2. It does not ship in a Worker and it does not expose a fixture route.

## Commands

The repository exposes the ten controlling commands:

- `npm run test:weekly-source:builders`
- `npm run test:weekly-source:unit`
- `npm run test:weekly-source:db:new`
- `npm run test:weekly-source:db:upgrade`
- `npm run test:weekly-source:db:component`
- `npm run test:weekly-source:service`
- `npm run test:weekly-source:browser`
- `npm run test:weekly-source:differential`
- `npm run test:weekly-source:model`
- `npm run test:weekly-source:all`

`all` always uses that exact order. It refuses a missing adapter, missing controlling pack, missing result evidence, incomplete cleanup, incomplete coverage of any controlling set or an emulator standing in for final C1 evidence.

`db:component` is deliberately separate from `all`. It restores the reviewed
pre-Plan-6.2 PostgreSQL 17.11 snapshot, proves its exact legacy import permissions,
installs only the 7 Weekly Source migrations and the bounded CloudTMS-owned changed/new
repeatables, and executes every HANDOVER-2-independent Weekly Source database
verifier. It explicitly excludes the two HANDOVER 2-owned Workbench definitions
and names the exact dependent verifiers and three Workbench concurrency suites
that remain pending. Its result is `PASS_WITH_HANDOVER2_PENDING`; it never
writes a release-eligible database envelope and can never be reported as a
complete release pass.

The component command requires `CLOUDTMS_WEEKLY_SOURCE_RUN_LOCAL_DB=1`, a unique
`CLOUDTMS_WEEKLY_SOURCE_COMPONENT_RUN_ID`, a new or empty
`CLOUDTMS_WEEKLY_SOURCE_COMPONENT_RESULT_DIR`, and the reviewed snapshot path and
SHA-256 in the ordinary UPGRADE snapshot variables. It creates and removes only
its own labelled local Docker container and volume.

## Required local inputs

Complete execution requires all inputs up front, before any database resource is
created:

- `CLOUDTMS_WEEKLY_SOURCE_PACK_ROOT`: the reviewed Plan 6.2 pack whose annex and proof hashes match `controlling-ledger-manifest.json`;
- `CLOUDTMS_WEEKLY_SOURCE_RESULT_DIR`: an isolated directory for scenario result envelopes;
- `CLOUDTMS_WEEKLY_SOURCE_EVIDENCE_DIR`: a new or empty directory for phase evidence;
- `CLOUDTMS_WEEKLY_SOURCE_RUN_LOCAL_DB=1` and a unique `CLOUDTMS_WEEKLY_SOURCE_DB_RUN_ID`;
- `CLOUDTMS_WEEKLY_SOURCE_DB_ADAPTER`: a regular module inside `tests/weekly-source` exporting `executeWeeklySourceDatabaseScenarios`;
- `CLOUDTMS_WEEKLY_SOURCE_REAL_WORLD_DB_ADAPTER`: a regular module inside
  `tests/weekly-source` exporting `createRealWorldScenarioDependencies`; the
  NEW and UPGRADE database phases refuse to pass unless this adapter drives
  every declared action in both populated NHSP/Roster journeys through the
  shipped product owners and independently reads the resulting database state;
- service, browser and differential adapter paths inside `tests/weekly-source`, exporting `runWeeklySourceHarnessPhase`;
- for UPGRADE, the reviewed frozen snapshot path and SHA-256 plus its exact
  immutable `CLOUDTMS_WEEKLY_SOURCE_UPGRADE_EXPECTED_ENVIRONMENT` and
  `CLOUDTMS_WEEKLY_SOURCE_UPGRADE_EXPECTED_CUSTOMER_KEY` identity;
- the exact `postgres:17.11-bookworm` image, or the explicit local-test image-pull opt-in.

The result and phase-evidence directories must both be new or empty and must be
separate, non-nested locations. This prevents a stale envelope or phase file
from satisfying a later coverage run.

Adapters receive the scenario directory and result directory. They must call real product owners for feature transitions, return bounded PASS evidence, and prove cleanup. A foundation adapter may seed prerequisites only. It may not seed source movements, final source, queries, protected hours, invoice lines or C1 publications.

### Where the sealed pack lives

The Plan 6.2 pack is sealed and read-only. It is **never copied into this repository and
never edited**. `CLOUDTMS_WEEKLY_SOURCE_PACK_ROOT` names the sealed pack root in place, and
the harness opens every pinned file for reading only. Two spellings are proved to deliver
identical bytes, so the pinned SHA-256 values hold for either:

- `P:\` — a Windows `subst` of the pack directory;
- `…\.codex-worktrees\weekly-source-plan6-20260915\plan6-pack-audit-20260916\CloudTMS_Weekly_Source_Reconciliation_Pre_Implementation_Pack_Plan_6_2026-09-15`.

That directory's *name* still says Plan 6 / 2026-09-15; its *content* is Plan 6.2. A pack
root inside the backend worktree is refused with `COVERAGE_PACK_ROOT_INSIDE_REPOSITORY`, so
the gate cannot be satisfied from a duplicate that nobody reseals.

## Database safety

The controller accepts only a task-owned container and volume with the `codex-weekly-source-harness-` prefix, exact ownership labels, local `127.0.0.1`, database `banking_modal_v2_test`, user `postgres`, no password and exact server version `17.11`. It invokes the repository release owner for NEW or UPGRADE. It never contacts Miget, hosted TEST or LIVE, never runs a broad prune, and never removes the existing `codex-weekly-source-plan6-pg17` container or volume.

Every database run removes only its exact labelled container and volume and performs a fresh absence probe. A release without database scenarios, or scenarios without row-cleanup proof, is a failure.

An UPGRADE restores the frozen snapshot and proves its installed database
identity before invoking the repository release owner. The harness passes that
same reviewed identity to the release owner; it never rewrites the identity to
the current run ID and never suppresses the release owner's own identity check.

### Named concurrent sessions

`23A` section 13 requires advisory-lock and concurrency proofs to run "in a dedicated serial
group with two or more named connections". `named-connections.mjs` provides that, and the
scenario executor receives it as `openNamedSessionGroup` bound to the target the controller
has already proved. A scenario cannot supply its own `baseConnectionUrl` or `expectedPort`.

Each session is a long-lived `psql` process framed by per-statement sentinels on both
streams, so a statement that is waiting on a lock simply leaves its promise pending. Password
material never appears in a URL: `psql` reads `PGPASSWORD` from the environment. psql
meta-commands are refused, so a statement cannot reach the shell or the filesystem.
`waitUntilBlocked` proves from a third session that a contender is genuinely in a `Lock` wait
rather than merely slow, and a real deadlock is returned as an error rather than hanging the
group. This is what `R12`, `R15`, `R37`, `R42`, `ROT-002`, `ROT-003`, `ROT-012` and `UNA-014`
need.

## Evidence boundary

The C1 emulator validates a sealed request/declared response only. It cannot calculate residual pay, create a Draft or satisfy final release evidence. Complete coverage is derived only from passing `WEEKLY_SOURCE_TEST_RESULT_V1` envelopes whose cleanup and digest validate.

The `model` command counts **eleven** controlling sets, not the Plan 6 four. Every one must be
covered by executed evidence before the harness reports completion:

| Control set | Envelope field | Required | Authority |
| --- | --- | --- | --- |
| acceptance | `acceptanceIds` | 967 | `annexes/acceptance-tests.csv` |
| atomic requirements | `requirementIds` | 64 | `annexes/live-implementation-ledger.csv` |
| protected areas | `protectedIds` | 29 | `annexes/protected-functionality-matrix.csv` |
| combination models | `modelIds` | 25 | `annexes/combination-coverage-matrix.csv` |
| source/pay/invoice scenarios | `spiIds` | 106 | `annexes/source-pay-invoice-scenario-matrix.csv` |
| invoice issue routes | `issIds` | 14 | `annexes/invoice-issue-real-route-matrix.csv` |
| UI lifecycle states | `uiStateIds` | 22 | `annexes/ui-lifecycle-state-matrix.csv` |
| financial touchpoints | `ftiIds` | 60 | `annexes/financial-touchpoint-impact-matrix.csv` |
| cross-system gaps | `xsgIds` | 41 | `annexes/cross-system-gap-ledger.csv` |
| HANDOVER 2 contract | `h2Ids` | 41 | `annexes/handover2-acceptance-contract.csv` |
| release proofs `R1`–`R44` | `proofIds` | 44 | `proof/32 §12` (no CSV exists; the ids are derived from the pinned proof file) |

Also pinned but not gated: `generated-case-ledger.csv` (967 rows) and
`testing-harness-work-items.csv` (31 rows, `TH-001`…`TH-031`).

`controllingRequirementIds` is the generated-case ledger's own requirement namespace
(`SRC-*`, `QRY-*`, `ROT-001`, `UNA-001`, …). It is validated against that ledger but is not a
gated set; the 64-row atomic ledger remains the gated requirement set.

### Rows the pack has not yet marked covered

Plan 6.2 adds 41 acceptance rows and rewrites 3 more, so 44 generated-case rows are not
`COVERED`. The loader accepts their enumerated `coverage_status` prefixes instead of aborting,
and carries them as a named must-execute list. They are **not** excused: `verifyExecutedCoverage`
reports `missing.pendingExecution` and fails until executed evidence names each one.

`R40` is classified in the pack as "Implementation gate (not a database test)". That is
recorded on the row and changes nothing: it still needs executed evidence like every other id.

133 acceptance ids are reachable from no atomic requirement in the 64-row ledger. That is a
pack linkage gap, reported as `acceptanceWithoutAtomicRequirement`, never hidden.

## Differential protection

`differential-protection-contract.mjs` holds the 23A section 12 comparison owner: the eight
compared surfaces, the before/after capture shape, the rule that only an atomic Requirement ID
may authorise a difference, and the refusal of the wildcard reasons `all`, `miscellaneous`,
`layout update` and `source feature`.

The populated database limb records its exact baseline and authority mapping,
but it deliberately does not claim the other seven protected surfaces.  Those
surfaces need their own exact executed envelopes before a Protection ID is
complete.
