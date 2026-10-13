# F-13 native disposition review

**PASS: completed DRAFT_CREATE SELF + native batch + D02 receipt, with deliberate conservative retention of all Workbench wakes.** No blocking source defect found in this scope. SESSION/CANDIDATE/GLOBAL retention is the completed safe disposition behavior for these broader transports, not an unfinished positive qualifier. Money ROOT/CHILD and every unknown/nonterminal obligation remain retained. No application/queue/DB proof was run; this is a source review, not installation or application acceptance.

Read-only review on 10 October 2026. Root alone writes product; this investigator executed no tests, SQL, runtime, DB/network/provider/secret operation and wrote no checklist. Reviewed saved BUILD-BE HEAD `6d53e110059ea631cd286c0eb7b495857eb567e8` plus current saved adapter/index bytes below. Authority: full F-13 card lines 748–767 and checklist lines 432–450, especially D01 exact schema + durable terminal/reconciled disposition, D02 receipt before acknowledgement, DB:none, and no guessed global empty/metadata-only ACK. Existing `F-13-native-retirement-qualification.md` supplies the bounded scope design.

## Real completion evidence, not a terminal label alone

The final registered `public.banking_pay_operation_finish(uuid,text,jsonb,jsonb)` has final physical `supabase/repeatable/09082026_1128_banking_pay_operation_finish_post_draft_authority.sql`, replay root `05092026_1200_banking_pay_draft_v8_final_authority_closure.sql`. It locks the actual operation (100–104); an existing terminal operation is not rewritten (111–113). For DRAFT_CREATE→COMPLETE:

- 116–128 requires FROZEN, seeded scope, real frozen scope generation/time/hash, positive Candidate/selected-row counts and accepted source session/snapshot.
- 131–175 counts **every** native candidate-scope row, counts distinct selected IDs, checks all scope rows’ accepted session/version/snapshot and actual batch provenance, recomputes the original scope hash, and refuses cardinality/hash/provenance disagreement.
- 177–190 requires at least one CANDIDATE_SCOPE chunk and each COMPLETE with completed_count=unit_count and failed_count=0.
- 412 onward writes actual COMPLETE runner/status, clears both lease and lock owners, and retains real completion time. It does not invent a later completion reader.

The adapter therefore reads a genuine native completed owner's durable state; it does not call `banking_pay_operation_finish`, rerun a legacy writer, recalculate money, or certify a COMPLETE string for arbitrary money operations.

## Whole bounded Draft universe

The positive path is only exact recognised OPERATION_V1 **DRAFT_CREATE + SELF**. The actual operation must agree with the envelope IDs/type, be COMPLETE/runner COMPLETE, have requires_user_action=false, real completed_at_utc, no lease_owner/locked_by, accepted frozen source facts, and positive frozen counts. Null root/batch identities must match exactly; nothing is silently filled from a different owner.

Native candidate scope has real `id`, `operation_id`, `candidate_id`, `pay_channel`, `workbench_session_id`, `pay_batch_id`, `source_session_version`, `source_snapshot_run_id`, selected sets and scope_hash. Structural finishing provides primary key id (3984), operation FK (6749), actual batch FK (6752), and unique `(operation_id,candidate_id,pay_channel)` (9980). The adapter's selected seven scalar columns exist. It reads **all rows for the exact operation**, ordered by id, with limit101 and admits only the complete row count equal to frozen_candidate_scope_count (1..100). It checks row IDs, exact operation/session/source version/snapshot, each nonnull batch and ALLOCATED/DRAFTED state. Candidate/channel columns are the native identity enforced by the table's unique constraint and the completed owner's frozen hash; the adapter does not claim one unrelated Candidate covers another.

It derives every distinct actual scope batch, not just top-level operation.pay_batch_id, and requires any nonnull top-level batch belong to that universe. For every batch it requires exactly one positively matched returned F-18 item with exact native ID, OBSOLETE_BOOKKEEPING and blocking:false. Missing/overflow/mismatched/multiple native rows, >100 scopes, absent batch on the first100 census page, or any unqualified batch retain the wake. The current single census page may conservatively retain a fully unused batch outside that page; it cannot accidentally ACK it. No empty category/database_ready/global-zero substitute is used.

## F-18 meaning and queued-money boundary

Existing `private.bpay_next_readiness_unused_batch_v1(uuid)` in 2109:110–134 requires the actual DRAFT/DRAFT_CREATED + NOT_SUBMITTED batch and no CSV/schedule/execution/commit/Monzo evidence, authorised request, reserved/committed advance, settled/partial/failed Candidate or entered net, PAYE input, provider request/attempt/event, pending/failed settlement, active correction or advice admission. Its public LEGACY_BATCHES category labels the **actual existing batch** positively at 161–165. Pending untouched transfers/READY transfer scopes by themselves are not a statement that all future finance has completed.

This batch predicate deliberately is **not** a universal money/root/child/Candidate disposition. In particular, a queued PAYMENT_EXECUTE could be an integrity-review operation before any provider request; the adapter must not call that owner complete because its Draft batch is unused. It does not: only the exact completed DRAFT_CREATE SELF delivery receives this result. Every other operation type/relation falls through to retained/no positive qualification. A separate financial envelope or durable operation remains intact; no operation, batch, Candidate, child or root row is deleted or advanced here.

The original `deriveBankingPayContinuationDescriptor` at index:40895–40921 is the relevant delivery boundary: terminal PAYMENT_SETTLEMENT/REMITTANCE_QUEUE specifically wake their PAYMENT_EXECUTE root (40910–40911); terminal DRAFT reaches required:false/NONE (40913–40914), before child/more-work routing. Thus this old completed DRAFT SELF wake has no original money successor to execute. Do not extend the positive rule to terminal settlement/remittance strings or use F-18 batch classification as whole-agency readiness. Unknown genuine old obligations continue through approved reconciliation, without an operational old engine.

## D02 actual durable receipt and ACK seam

The deterministic receipt ID retains all128 native hex/UUID message-ID bits. Unsupported native IDs retain. The small audit packet contains only message ID/family and qualified native operation/session/batch IDs/reason, not raw envelope/payload, lease, bank/provider data, invented digest, witness or finance snapshot. Existing audit_events has the UUID PK at finishing:3951 and real ts_utc default now() at370, so the source INSERT fields are valid.

The adapter qualifies native state **before** reusing a receipt. It reads by exact PK and exact field allowlist; reuse requires complete packet equality. A collision/conflicting or missing/multiple receipt retains. It awaits actual INSERT (`Prefer:return=minimal`), accepts only success or a409 that can be verified, then performs a fresh exact GET and requires complete equality. A lost insert reply cannot itself retire: queue retry reuses the exact native identity and only a verified row permits completion. Eight-second/read-byte/receipt-byte caps fail toward retention.

The index seam admits only actual NEXT, invokes this qualifier, and on retired:true re-reads owner; ACK requires NEXT and the exact same owner_epoch. If owner read/qualification/receipt/readback/epoch fails, the delivery explicitly retries30seconds. Batch-size drift retries every message; damaged ACK/retry interfaces throw to preserve delivery. The once-only durable audit row is the required bounded log; console warnings contain no payload. This has no new proof framework or SQL owner.

## Certificate and deliberate Workbench retention boundary

The existing `public.pay_workbench_settled_certificate_status_v8(uuid,uuid)` has only one repeatable definition, 2301:2519–2640. It reads the exact session+actor and current session/version/progress/fence-matched certificate, requires current OPEN/READY un-replaced/undiscarded scope and SEALED_CURRENT + genuine V2 certification/digest format, and is a native read-only status owner. The adapter requires the exact envelope certificate UUID, complete finite response keys and all positive current-ready/no-recovery flags. This branch does not infer a global/financial disposition from a certificate. The exact CERTIFICATE_V8 envelope is distinct from a Workbench drain wake: its requested certificate identity has a native completed build/status owner. This review confirms that physical source and bounded separation, without treating certificate readiness as completion of a Workbench drain.

All `LEGACY_WORKBENCH_WAKE_V1` envelopes deliberately fall through to null qualification and bounded retry, whether their scope_kind is SESSION, CANDIDATE or GLOBAL. This is the card’s conservative retention behavior. The actual original `public.pay_workbench_worker_drain_chunk_revalidated_v1` in `supabase/repeatable/19072026_1405_revalidate_recovery_headroom_after_materialisation.sql:1520–1541` first calls `pay_workbench_scope_reconcile_ensure_v1()` and `pay_workbench_scope_reconcile_drain_one_v1(25, worker_id||':SCOPE')` **without session/Candidate filters**. It only then passes p_session_id/p_candidate_id to `pay_workbench_worker_drain_chunk`. A locally READY session, completed Candidate rows or even that session’s current sealed certificate therefore cannot prove the broader transport’s global reconciliation obligation complete. No native full-drain completion receipt keyed to wake_id/previous_drain_invocation_id has been established. The adapter correctly creates no retirement receipt and performs no ACK for these wakes. There is no pending positive-qualification implementation to add from those local readiness facts. Preserve all unknown or nonterminal work, without inventing a broad new owner, witness, framework, SQL, fixture or external barrier.

## Current source receipts

| Source | Raw SHA-256 |
|---|---|
| `broker/src/bpay-next-legacy-retirement.js` | `8c0da37eccedf87173fe9942c630817d4f97fec09e91e356b82eec1a66764e24` |
| `broker/src/index.js` | `57893c36c37e2ea69c01a3cc8d11dcbf35ed655f89bc258c3a4eb690a1ad1825` |
| `supabase/repeatable/09082026_1128_banking_pay_operation_finish_post_draft_authority.sql` | `d90ee3bd93f2da774046980e68a808dcb0c98e7bbf4cb25b6f2010d8a391cd1d` |
| `supabase/repeatable/09102026_2109_banking_pay_next_cutover_readiness_page_v1.sql` | `980a79151e8cc49f3db8c5c4e1f8cd99aac6039600405e25ec1803d3b7c91add` |
| `supabase/repeatable/02092026_2301_banking_pay_workbench_settled_certificate_build_v8.sql` | `081e490c359080ba296f06548a845100ee242cf5fafb10dd50d2aaa77fc079cf` |
| `supabase/baseline/22082026_1500_cloudtms_test_structural_baseline.sql` | `aec4d8985fed34c08cfd61521db4ad27b273a85e90251ccd8007eee0489b7730` |
| `supabase/baseline/22082026_1501_cloudtms_test_structural_finishing.sql` | `367d70659e4ab05a7f18a86fd29b3d74d018d3f916a1e1e38f23e63243bfb304` |
| `docs/bpay-next/final-definer-closures.json` | `875f3b481cdd9e617a408ff49ff66556f14714ae7b5d1de421351858d149c28a` |

| `supabase/repeatable/19072026_1405_revalidate_recovery_headroom_after_materialisation.sql` | `aa6f1bfafd691536bb04a8c7bdfa12f075b1bdc31ee28b00f50c7160a36eb5ba` |

Correction on 10 October 2026: the earlier SESSION/CANDIDATE-pending wording was stale and is replaced by the actual unfiltered global-reconcile-before-filter source boundary. Completed-Draft and receipt findings are unchanged. No product file was modified.

Source-only final scope result: PASS. Later installation/application checks remain separate.
