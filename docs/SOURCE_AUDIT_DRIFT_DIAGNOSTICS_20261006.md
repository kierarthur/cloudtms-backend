# Source audit/export failure diagnostics — 6 October 2026

This is diagnostic instrumentation, not a claim that the original failure is
fixed or that TEST has been deployed. Banking runtime definitions and Policy X
are unchanged.

## Observed failure

Protected run `37481875859`, commit
`37504fb1a8bcfbe90d1bade5c90749900ae8244b`, failed at15:39:11 UTC:
`17092026_1200_weekly_source_audit_and_export_v1.sql` raised
`PAID_FIXTURE_TWELVE_ECONOMIC_ROW_DRIFT` in its facts-only setup, before its first
import. 140 of149 mandatory verifiers passed. Runtime/Office publication remains
held; installed definitions are not a VERIFIED release.

The original assertion had no table/field detail. Do not infer payment-policy
drift, blame user activity, assume concurrent updates, or weaken the assertion.

## Evidence so far

- Original complete setup prefix passes on an independently owned PostgreSQL17
  clone with the exact reviewed current contract.
- Hosted rollback diagnostics passed the original financial comparisons after
  all three bounded setup calls and their terminal owned-job status check. The
  final repeated historical Banking scan hit the lab's120-second statement
  ceiling. This is not a complete-verifier pass or a reproduction of the original
  economic assertion. Each run rolled back; fresh fixture absence was checked.
- Read-only metadata-driven timestamp counts found no persisted timestamp in
  the inspected financial guard relations since15:00 UTC. This does not prove
  that no concurrent or transaction-time-stamped update occurred.
- Every completed task-owned diagnostic container and volume was removed. The
  shared source container was used only for a consistent read-only dump.

## Diagnostic change

The original complete-row fingerprints, relation list, failure predicates,
three-call bound, job ownership checks and finalizer identity checks remain.
No verifier, row, field, timestamp or economic assertion is excluded.

A rollback-only temporary helper stores only SHA256 digests of each primary
key, complete row and field. Its count/multiplicity/full-row aggregate is checked
against the original baseline. A baseline that changes during capture fails
closed before worker execution.

On the original financial failure, DETAIL identifies relation names,
added/removed/changed row counts, changed-field row counts and worker call.
It reports whether both diagnostic snapshots match the exact original guard
fingerprints. A later read-back mismatch is explicitly marked; never attribute
its field counts to the original comparison as if they matched. No customer
values, identifiers, amounts, credentials or financial payloads are returned.

## Qualification and next step

Mandatory in-transaction synthetic controls cover unchanged rows, identical
complete hashes, additions/deletions/field updates, composite primary keys,
NULL/empty value swaps, privacy canaries and a changed subsequent read-back.
The standalone controls and original setup prefix passed on PostgreSQL17;
source regressions and the complete backend suite passed (1,456 tests).
Arthur's hosted rollback lab also passed these controls and exact aggregate
equality on all11 current financial relations, then proved the temporary
diagnostic objects absent on a fresh connection. The current repository
contract remained exact after the local setup transaction rolled back.

Publish diagnostic source only after its source/release gates pass. Any next
deployment uses the canonical commit-bound `release:test` coordinator with
fresh Cloudflare connection evidence and reruns all149 mandatory verifiers.
This is an instrumented investigation, not a blind assertion-suppressing retry.
There is no component, cache, ledger repair, timeout increase or runtime/Office
shortcut. If it fails again, use the new exact matching field/count evidence to
determine the cause before proposing a runtime or shared-owner change.
