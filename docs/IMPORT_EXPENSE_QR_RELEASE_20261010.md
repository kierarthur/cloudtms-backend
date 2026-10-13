# Import-authoritative expenses-only QR approval

## Scope

Effective client/contract paper permission may enable printed QR approval for
`CONTRACT_EXPENSE` on import-authoritative weekly contracts. Imported hours and
combined hours/expense submissions remain prohibited from paper approval.
The general hours policy and route-family resolver are unchanged.

Two private purpose-specific resolvers are installed with complete replacements
of the workflow transition, submission finaliser and Timesheet-detail RPCs.
The exact pending expense workflow receives current expense approval permission;
ordinary hours detail retains its existing hours capability. Existing non-paper
expense workflows retain their historical policy fingerprint compatibility.
No new public API operation or response field is introduced.

## Qualification checkpoint (not a deployment receipt)

- Arthur's account-owned rollback lab accepted the complete pending SQL and
  expense-only pack fixture on `cloudtms_test_clone`, rolled back, and confirmed
  fresh-connection absence of its temporary objects and fixture rows.
- The same focused fixture passes on the complete disposable PostgreSQL17
  database rebuilt from the repository's migrations and repeatables.
- Tests cover client permission, contract override, disabled permission at
  preparation, hours/combined denial, exact-workflow detail, preparation replay,
  a three-page mileage pack with no hours page, held—not dispatched—email, and
  unchanged worked Timesheet/financial/contract-week state.
- Candidate app: 70 suites / 627 tests, type-check and contract/network/storage
  guards pass. Twelve responsive browser checks cover permitted expenses,
  disabled expenses and imported hours across four viewports, including an older
  device receipt refreshed from the exact server workflow/generation.
- Backend Candidate boundary suite: 240 tests pass.
- Complete backend JavaScript regression suite: 1,485 tests pass.
- Managed-release, database-release, contract-export and automatic-routing suites:
  67 tests pass. The separate historical invoice-evidence component suite has
  two existing failures because its pinned read-projection source no longer
  matches the current source. Those component files are unchanged by this fix;
  the component remains ineligible and must not be repinned to bypass the full
  release route.

The local NEW rehearsal encountered a missing local test-owner grant for
`session_replication_role` in an existing entitlement verifier. Only the owned
disposable PostgreSQL container was granted that test permission before retry.
No hosted role permission was changed.

All portable NEW verifier files were exercised on the complete local rebuild.
After the intended function edit, the installed-writer census correctly rejected
the old transition hash. Review of the complete diff showed its two dynamic
EXECUTE statements and financial write paths unchanged. Only that exact
acknowledgement was repinned; the census and remaining verifiers then passed.
The sealed local contract changes precisely two new private helpers and three
public definition hashes. Contract comparison and source coupling pass at
`c575e7f65bb302ccb171c548767b9bb3bfe888d93b5a7a4d886f9809f2eb6eaf`.

Hosted installation, exact source commits and native/store installation must
be recorded separately after they occur. Source or mock browser qualification
does not prove that an existing installed native app contains the new code.

## Installed and store-submission evidence — 10 October 2026

- Protected agency TEST database release run 38056026756 passed at 14:22:09 UTC.
  Installed QR repeatable SHA256: `171d41d8ed2f958cb75ae65573bb0c51fd54de1c0f87e46c82f594d2484fb03b`.
  Runtime Workers were unchanged; the fix changes SQL and Candidate application source.
- Android `MyTMS TEST 0.1.0 (39)`, source `ee30351e24ce0c2d81291fcd963da2f2cd20065f`,
  published to internal testing at 16:09 UK. The same binary was submitted to the
  existing Closed testing — Alpha track, preserving the tester cohort and opt-ins.
  Google showed Changes in review, pending automated checks, after submission.
- iOS `0.1.5 (24)`, source `abfc6ab5f96fa2b60b7f9246600563723d80617e`,
  submitted; Apple showed Waiting for Review, one item submitted. Submission
  `34436ee4-9f83-4e12-b7d5-801e05ce9830`. No new EAS build was consumed.
- On the USB phone the existing affected expense claim offered Printed documents
  after installation. Physical printing and camera-return acceptance remains
  outstanding; store review is not proof of that workflow.
