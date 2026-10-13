# SEND6 fixture forward source review — 13 October 2026

Independent read-only review of the latest publication0300, first-authorisation0600 and withdrawal0610 verifier source/diffs against the actual native I5/I4, receipt/inventory schema and existing guards. Root owns fixture fixes and the active rollback rehearsal. This reviewer made no database, SQL/RPC/business, network or test call; the scoped Git diff was read-only. Prior design/source reviews remain preserved.

**Final verdict: SOURCE PASS for all three current fixture changes.** No remaining concrete source blocker was found in this scope. Native production owners, CHECKs/FKs, receipt/inventory guards, actor permissions and existing behavioral/ACL assertions are not weakened. Execution is not proved by this receipt.

| Saved verifier | SHA256 |
|---|---|
| `17092026_0300_weekly_source_entitlement_publication_v1.sql` | `9043CA8E254CDEA64867C9DF0EC7FDF55A21B2C29AE12907B2B87BECEA4EC646` |
| `17092026_0600_weekly_source_first_authorisation_v1.sql` | `54EB02A26FD3E65D93696E31CD0E462E4221BDCCE6A241EB8B37BDD9725CEE0F` |
| `17092026_0610_weekly_source_withdrawal_supersession_v1.sql` | `98B819B77FF45889F559C772E10E5A4763D6272CDDA86119F560CAC1031DD557` |

## Fresh D10 — source gap closed

The old candidate/latest-pending lookup is removed. D10 now reads A's unique COMMITTED_CURRENT family head with INTO STRICT and requires a real certified-zero/count0/no-component inventory. It constructs a new single-root request with dedicated bundle/head/decision/component IDs, the exact current expected head, empty before IDs and one genuine component leaf. The existing builder's Candidate/A-root/Contract/Source-final-revision/manifest/policy remain unchanged; the native core still independently validates the physical root/family/Candidate and expected-head relationship. A missing/duplicate/wrong head cannot pass the positive control.

`mk_bundle` sees the complete final request before its immutable acceptance is written. The unchanged real I5 Save receives that request, matching lock_result_a and its explicit FROZEN fixture vector; this is the existing I5/core fixture contract, not a new release-worker or real-census execution claim. Native I5 canonicalizes DEFERRED with its generated pending ID, verifies the accepted decision/approval digests and stores the original request. The new call asserts successful creation/PENDING and uses returned pending_bundle_id directly. A created native result cannot be a replay on the unchanged I5 path. Save supersedes only the same decision_bundle_id, so the new D10 bundle leaves old12b's bundle untouched.

The exact new row's stored request is recomputed with DEFERRED/that pending ID and compared with its stored digest. The subsequent untampered call still requires actual ok/published/nonreplay and exact returned receipt bundle/pending/revision, persisted receipt ID and COMMITTED decision/current heads. Assert_true rejects NULL. Its exact sentinel rolls publication back, and no new head write/receipt is allowed before tampering. The proposed one-component pay_ex_vat leaf exists and differs from the later9999.00 corruption, so the fixed negative is effective rather than an empty-array no-op. The immutable-request UPDATE negative, replica-only deliberate corruption/restored origin, native stored-digest refusal, permanent MANUAL_REVIEW/nonretryable/no-write checks and independent committed-revision/replay tests remain unchanged.

This source change closes the stale-before-position coupling identified in `SEND6-D10-fresh-fixture-design.md`. The first round's generic failed positive assertion did not identify its actual returned reason; the earlier HEAD_CAS_CONFLICT attribution remains a source prediction for the successful preceding path, not reconstructed runtime evidence.

## D8 zero-head receipt — source gap closed

The added private immutable fixture receipt is inserted after the accepted decision and before the COMMITTED_CURRENT zero head. It matches the exact bundle/revision, Candidate106, physical root306/familyWP07-BK-06/version2, proposed headfc1, actor, tokenfe1 and sha256('receipt') digest. The head uses the same receipt digest/token and real zero inventory, so both native receipt and inventory guards have matching facts before a potential immediate check. The receipt's IMMEDIATE shape has NULL pending/release-worker fields by default, non-NULL aligned one-member arrays and object census/proof; actor and Candidate FKs refer to the existing fixture rows.

This is a physical matching immutable **fixture receipt**, not a claim that I4 generated it or that its literal fixture digest is a canonical publication request. D8 tests real authorisation generation/pointer/withdrawal behavior against a genuine guarded row shape. The withdrawn-generation immutability, no reattachment, live generation uniqueness, identity/binding checks and real route assertions remain. No receipt guard is deferred, disabled or replaced by this change.

## Withdrawal fixture construction — named timing gap closed

The intermediate saved checkpoint `A689E8B23BA4314828E68F2F9D593B752AA48B9C710D2FAB90A3F90906ECCB19` had correct STAGED ordering but no explicit inventory mode; the native guard checks STAGED heads too and would reject declared2/actual0 under an inherited IMMEDIATE mode. That finding was sent to Root and is closed in the final98B819B checkpoint.

The existing `pg_temp.publish_head` helper now uses PL/pgSQL solely to perform explicit named constraint control around its native construction. It defers only `public.weekly_source_entitlement_head_inventory_assert` and `public.weekly_source_entitlement_head_component_inventory_assert` before writing the PROPOSED decision/STAGED head. It then inserts exactly generate_series(1,p_component_count) real component rows with the matching head/bundle/revision, writes the physical immutable receipt, activates the head with matching receipt digest/token, commits the decision, points the live root generation, and forces those same two inventory guards IMMEDIATE. The actual head/component compound FKs, unique component ordinal/identity and all state/receipt/identity guards remain active. Receipt construction precedes any non-NULL head receipt digest, so an immediate receipt check can pass without bypass.

There is no ALL DEFERRED, initially-IMMEDIATE constraint alteration, trigger disabling or exception that swallows a guard failure. Pending inventory events are actually flushed before the helper returns; count/certified-zero discrepancies still fail. Other guards retain their parent mode. The preexisting always-true “fixture note” is replaced with an actual nonzero/count2/physical rows2 assertion, strengthening coverage. Safe withdrawal, financial-effect refusal/no-write, replay, five-field authorisation digest, service/browser ACL and the untouched call-only owner pins remain unchanged except the already-approved Authorise pin/comment and legitimate actor flags.

## Remaining qualification

Root reports round1 explicitly rolled back, fresh18 fingerprints matched and5910 exact statement IDs were received, while12 latent errors were recorded; this reviewer did not independently execute that run. Round2 and its four core verifiers/invoice context are Root's separate runtime work. A SOURCE PASS here must not be described as successful native publication, withdrawal, all internal assertion coverage, complete rehearsal or installation readiness. Item6 remains instrumented/open unless its separate observed count evidence diagnoses and repairs it. SEND6 continues to hold promotion/installation.
