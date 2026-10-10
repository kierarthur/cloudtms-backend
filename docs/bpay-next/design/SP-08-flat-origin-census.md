# SP-08 independent flat-origin census and seam design check

Source-only review, 10 October 2026. Full card `05_SOURCE_AND_PROTECTED_PAY.md:305–329`, all six SP-08 checklist rows and `SP-08-SCOPE.md` were read. No SQL, tests, runtime, fixtures or hosted reads were invoked. This records source composition; native acceptance scenarios and hosted installation remain unobserved.

## Exact existing owners

| Object | Current source and evidence |
| --- | --- |
| Flat Local codec | `05102026_0201_weekly_source_local_origin_canonical_v2.sql:35–38` admits exactly the twelve committed-HEAD witness keys below; lines 44–79 validate the closed shape and preserve the referenced HEAD's full stored source-generation digest. |
| Qualified Local context | `05102026_0212_weekly_source_local_publication_context_v2.sql:172–183` compares the complete sealed prior inventory with live I1 and exact scope before flattening. Lines 219–224 remove only `source_revision` from the witness after that comparison. |
| Local proposal | `05102026_0224_weekly_source_local_entitlement_proposal_v2.sql:29–51` requires the one genuine context and exact receipt/request/generation/digests. Lines 54–73 retain full component and prior-inventory proofs. |
| Local completion | `04102026_1257_weekly_source_local_protected_decision_v1.sql:355–382` uses that codec/context and stages/publishes the genuine proposal. Lines 403–427 retain published evidence while retiring the original C1 request and completing its real run/local receipt. No invented Final HEAD. |
| Current publisher and HEAD capture | `04102026_0556_banking_pay_next_source_factual_legacy_boundary_v1.sql:608–647` qualifies actual Local context; lines 1611–1647 store the canonical source-origin object with its original digest in the new STAGED HEAD. Line 1699 performs the immediate inventory assertion before the existing detail call at 1704–1709. This frozen publisher is reused unchanged. |
| HEAD origin immutability and generic I1 | `04102026_2120_weekly_source_approval_basis_v2.sql:15–35` refuses changed origin evidence and verifies its hash against the stored HEAD digest. Lines 324–377 reconstruct the generic committed-HEAD basis from immutable stored evidence, including its full `source_revision`; this generic I1 is unchanged. Nullable historical origin evidence is unavailable, not backfilled. |
| Initial authorised TSFIN tail | The registry selects `09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql` for `public.weekly_source_first_authorise_v1(uuid,uuid,text,uuid)`. Its genuine outer core, Source stage and publication at 3592–3606 precede `bpay_next_initial_tsfin_bind_v1` at 3609. The old physical 0557 alone is not the winning tail. |
| Initial binding certificate | `07102026_0018_banking_pay_next_initial_tsfin_daily_evidence_v1.sql:26–43` proves the actual authorised headless root and refuses any existing HEAD. Lines 50–102 bind genuine publication/position evidence and the complete original component proof. Binding replay at 131–146 precedes current discovery; the original 32-byte inventory digest is captured once at 160–175. |
| Source book | The canonical locked 1245 schema is `09102026_2010_banking_pay_next_source_book_binding.sql`, with initial-origin columns in 2011. Current `06102026_1246_banking_pay_next_source_book_binding_v1.sql` retains exact immutable bindings, native SOURCE cache and explicit pending/unbound states. Its seed proof copies guarded actual R/H values; it does not sum history or infer book readiness from registration. |
| Publication receipt binding | `06102026_1301_banking_pay_next_source_receipt_binding_v1.sql:28–84` resolves real one/two-member roots, orders locks, publishes the real pair and requires every actual component registration. REGISTERED is not a claim that PositionApply or SOURCE-book seeding has completed. |
| Stage-current consumer | `03102026_1550_banking_pay_next_source_stage_v1.sql:171–218` uses the real committed HEAD and generic I1 before Bank inserts; chosen detail at 469–478 is read-only. It does not manufacture absent evidence. |

The twelve committed-HEAD witness keys are `kind`, `head_id`, `head_revision`, `decision_bundle_id`, `bundle_revision`, `root_authorisation_id`, `authorisation_generation`, `root_timesheet_id`, `root_version`, `source_generation_digest`, `inventory_digest`, `entitlement_digest`. `source_revision` and copied ancestry are absent from this flat witness. The referenced HEAD's full stored source-generation digest is retained unchanged. `CURRENT_FINAL_SOURCE_V1` is a publication-source variant, not a fourth ApprovalOriginV2 arm; the three approved arms remain INITIAL_AUTHORISED_TSFIN_V1, COMMITTED_SOURCE_HEAD_V1 and PROTECTED_LOCAL_DECISION_V1.

## Confirmed SRQ304 mismatch and bounded design PASS

The former `05102026_0302_banking_pay_next_local_source_chosen_detail_v1.sql:250–269` proves current transaction identity and the exact staged context, but has no consumed state. Its COMPLETE return at 225–234 is a genuine retained read; its fresh path can otherwise reach idempotent existing detail, including a zero-WORK vector. Transaction identity is insufficient evidence of single use.

The generic scope token does not close this gap. Current `03102026_0500_h1h2_successor_authorities_after_retry.sql:8438–8469` deliberately returns the same PENDING token and refuses only an already FINALIZED/NOOP token. The Local detail call in 0556 precedes its scope-token allocation at 1756, and the factual helper explicitly permits reuse of a controlling token. Early constraint finalization and Local seam consumption are distinct source facts.

The revised root design in `SP-08-single-use-seam.md` is **PASS for this bounded mismatch**: preserve frozen 0556; use the actual proposed HEAD UUID and its existing, validated current-xid context as the private seam identity. After all original fresh receipt/head/component/vector/context checks, the 0302 successor inserts an explicitly `consumed=true` private marker with `ON CONFLICT DO NOTHING`; no inserted row refuses reuse before fresh detail writes. A private guard accepts only current-xid, consumed=true INSERT and refuses UPDATE/DELETE/TRUNCATE. The table has the actual HEAD foreign key, owner-only FORCE RLS and no ordinary API-role grants. A rollback removes the rejected statement's marker; constraint mode changes cannot rearm a retained marker.

Finite final-source acceptance conditions: COMPLETE replay stays outside the marker-insertion branch; fresh zero-WORK also consumes; no UPSERT, reset, historical backfill or new caller token; frozen 0556 and generic I1 remain byte-identical; all original 0302 checks and detail operations remain intact; logical owner, service-only function boundary and idempotent private attachments remain closed. This design verdict does not assert that the successor has yet passed its independent final-source review.

## Closing-document evidence and limits

T26: current `05102026_0114_weekly_source_pay_query_evidence_v1.sql` contains the retained SOURCE_LOCAL reader corrections. Lines 88–116 use the original bounded physical version and retired request; 231–256 bind the immutable publication/HEAD; 257–279 canonicalize dates and decimal fields consistently; 343–352 use the actual immutable family event's `office_actor_user_id`; 356–374 compare the original sealed prior inventory and its flat witness. This is a finite reader-source finding, not approval of the whole factual gate or an app rehearsal.

C29: `05102026_0355_weekly_source_local_flat_origin_cutover_v1.sql:9–23` is a locked once-only migration that fails on existing old Local evidence without rewriting it. The local compiled checkpoint inventory contains one matching migration path/hash; that inventory is not an independently inspected database ledger or proof of hosted one-time installation. There is no standing repeatable ban on future genuine Local publication.

C30/PRIM: the supplied Source-book correction withdraws the old assertion that a null seed effect is permitted only for ZERO. Current 1246 supports a genuine held-only basis with R=0/H>0, an actual ACTIVE hold and nullable paid effect; R>0 needs actual paid evidence, both positive need both witnesses. Qualified zero balance and historical net-zero are not invented absence of history. Seed copies current authoritative native R/H once, retaining provenance; no historical SUM or equality of one effect to the aggregate is introduced.

The headless first-authorisation/paid-WORK scenario, repeated protected 8→7→6→7 amendments, observed HEAD-size bounds, first-paid SOURCE book and actual second-consumption refusal are **NOT_RUN** here. Root owns the final closing note and any separately authorized compilation/ledger checks.

## Source identities

| Source | SHA-256 |
| --- | --- |
| 0201 codec | `641e417ad4fb2669f5bb59af583e3a18373c80bbf0cb3431c77d4df225aa1812` |
| 0212 context | `209a157af5d7ec1e67568905b5cb828638aab09a11a1d8c4a8ebd59bca8c1d36` |
| 0224 proposal | `bdfcf633afa089d5307ca66b1ea70f51ff7b5b2dba9b086596bd136fb2c4e81f` |
| Prior 0302 detail | `4318b30d109a3ebbb1d7afda457a616874fc174f5e7a81061ca0978ac5f89001` |
| Current 0114 reader | `bb6290374ea7857d1ec995565c78267ff9dc761b7d7f4fe05f460ed2903f9b2f` |
| Locked 0355 cutover | `0d74a542023102d246fedbf26cceda01c4b88f94c79cf1dbb01eb7eea35be98f` |

Design review follows the root's narrower append-only consume-at-0302 refinement received during this review; final source hashes are to be recorded in `reviews/SP-08-sql-review.md` after rereading that saved successor.
