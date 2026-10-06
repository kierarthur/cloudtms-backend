# Rollback Source verifier snapshot — 6 October 2026

## Evidence and limits

Protected TEST run `37496342914`, reviewed commit
`323cce5ba4f1594f256c5c2490b1c57407004596`, passed the source/security gate and
stopped at the ordinary-pay verifier with `PAID_FIXTURE_FANOUT_ECONOMIC_ROW_DRIFT`
and relation-only detail `public.pay_batches`. The failed statement ran from
16:37:13.643 to16:40:06.370 UTC. No runtime or Office promotion followed.

Arthur's account-specific rollback lab confirmed five persisted payment batches
recorded completion-notice attempts in that interval. The normal scheduled
runtime independently patches `completion_notice_last_attempt_at_utc` and other
notification metadata; the complete-row fingerprint includes those fields.
However, the failed run did not retain per-field evidence. A later110-second
read-only observation without any worker call saw no drift. Therefore those
attempts are a plausible explanation, not proof of the failed field changes.
No hosted worker, payment, queue, or email was invoked by these diagnostics.

The independently owned PostgreSQL17 current-contract clone passed the unchanged
ordinary setup under its old READ COMMITTED transaction. A deterministic
two-connection control then proved the isolation defect: committed external
notice-field changes, insertion and deletion after capture alter the original
complete-row fingerprint under READ COMMITTED even when the first transaction
has made no corresponding write.

## Narrow verification correction

Establish REPEATABLE READ at transaction start for the four direct full-financial
fixture roots: ordinary projection, Office reads, Candidate view and audit/export.
The ordinary root is also included by Correct Final, invoice admission and
invoice issue; all their outer transactions use the same isolation, including
both independent FULL and NET invoice cycles. A transaction-local assertion
rejects an incorrectly configured included caller before fixture preparation.

Only verification transactions change. No installed routine, application request
isolation, payment economics, Policy X, ownership, ACL, trigger, scheduler,
provider, recovery or Banking interface changes. No field is excluded, including
notification fields; the complete-row helper and every original financial,
non-target, namespace, named-finalizer and three-call/twelve-job guard remain
unchanged. Fixtures still see every one of their own writes. Concurrent writes
to a row the fixture tries to modify still fail closed with a serialization
failure; the release does not automatically conceal or retry that failure.
Each existing rollback boundary remains intact and releases its snapshot.

This is the expected PostgreSQL17 snapshot behaviour, not a replacement business
policy: [PostgreSQL transaction isolation](https://www.postgresql.org/docs/17/transaction-iso.html).
The hosted lab has already issued an identity query before accepting SQL, so it
cannot safely change its transaction isolation late. It was not used to escape
or reconfigure its server-owned outer transaction.

## Qualification

Local receipt `AUDIT_SETUP_DIAGNOSIS_fc016e0e-b9f5-4a30-a176-10da7ed92333.json`
proved the old ordinary prefix and the two-connection controls. Successor
`AUDIT_SETUP_DIAGNOSIS_25a583c8-0f80-4e15-857e-ef675a07801a.json` proved:

- READ COMMITTED sees deliberate external committed full-row drift.
- REPEATABLE READ retains the baseline after those external commits.
- Its own amount change, notification-field change, inserted row and deleted
  row each still produce a different complete fingerprint; rollback to each
  control savepoint restores exact equality.
- An attempted update of the externally changed row raises the expected
  serialization failure, rather than hiding a conflicting fixture mutation.
- All seven complete registered verifiers pass with their original assertions,
  including both invoice cycles. Individual elapsed times were4,632,7,049,
  10,823,20,958,4,227,2,240 and3,243 ms respectively.
- A fresh connection after every complete verifier proves fixture absence and
  exact complete current-contract restoration.

Source-shape tests additionally cover all recursive ordinary consumers, both
invoice transactions, unchanged guards and fail-closed caller configuration.
Arthur's hosted rollback lab also executed the exact isolation assertion under
its existing READ COMMITTED transaction and returned the expected
`SOURCE_VERIFIER_TRANSACTION_SNAPSHOT_REQUIRED` / `P0001`; unconditional rollback
and a fresh read-only connection to `cloudtms_test_clone` were confirmed. This
negative control did not touch application data or certify a hosted full pass.
The pre-existing isolation source test referenced an obsolete direct include
and an obsolete movement-write counter. It now follows the actual shared
include and asserts the stronger existing count AND full movement hash pair;
the verifier's movement assertions themselves were not changed.

Every newly owned diagnostic container and named volume was removed. Existing
Banking containers, volumes and the read-only golden database were preserved.
These local proofs do not establish hosted VERIFIED status or real Kier Save
acceptance. A full protected TEST release is still required before publication.
