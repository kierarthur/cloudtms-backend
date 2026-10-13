# WB-29 W2 receipt variant design review

12 October 2026. Independent read-only review of the receipt section in `design/WB-29-w2-native-command-design.md` against native migrations **10102026_2150**, **11102026_2259**, **11102026_2311**, current public W2 **0312**, and current retry bodies **2259**. **DESIGN PASS with the finite conditional requirements below.** Implementation remains separately reviewable; no SQL/catalogue/business/runtime/test operation was run and no product was edited.

## Exact original schema and minimum delta

The actual table is `private.bpay_next_workbench_command_receipt`. Original2150 supplies immutable request UUID, non-null actor/Candidate, native Work FK, non-null job identity without job FK (terminal cleanup deliberately allowed), positive module epoch, object `request_body<=4096`, object `result<=8192`, captured timestamp, private owner-only/RLS policy and UPDATE/DELETE/TRUNCATE rejection.2259 makes Work nullable and adds five nullable retry identities/revisions; its named execution CHECK permits either all five NULL or all five present with captured>0, expected>0, target=expected+1 and captured<=expected.2311 corrects the Work CHECK to exactly `work_id IS NOT NULL OR (result->>'retry_kind') IS NOT DISTINCT FROM 'PREPARE'`. Preserve that null-safe expression in the retry arm; ordinary `=` would reintroduce the already-fixed missing-kind CHECK loophole.

The proposed action discriminator is sufficient: NOT NULL, default historical `RETRY_WORK_JOB`, closed values `RETRY_WORK_JOB`, `RESOLUTION_SAVE`, `RESOLUTION_CLEAR`. Original retry inserts omit it and retain their original meaning; no historical row rewrite is needed. Drop job NOT NULL only with an explicit conditional CHECK that requires job_id non-null for RETRY and null for both resolution actions. Keep native Work FK when non-null.

Replace only the two original generated object/byte CHECKs and the named Work-variant CHECK, adding explicit resolution shape constraints. Retain the existing five-field execution CHECK unchanged; add a resolution-only rule requiring **all five retry fields NULL**. Thus resolution cannot accidentally become a PF08 execution-revision receipt while retry retains every original legitimate shape, including projection-only all-NULL execution fields. A retry with null Work is still PREPARE-qualified by2311; do not require a real Work for every retry or require five retry fields on every PREPARE.

For a resolution, real single-Work `work_id` must match its one explicit request Work; many-Work may be NULL only when the validated request contains more than one distinct Work and the exact result covers that original set. An empty/no-op CLEAR still retains its original explicit Work scope; absence of a publication is not absence of a Work. Result action must equal the discriminator; all result Work/child publication/decision identities are actual retained outputs, with no dummy job/command/sequence. Request-body/result matching remains the existing owner's finite admission responsibility, not a new storage/protocol. JSON checks must fail closed on missing/null/type mismatch (`IS TRUE`, `IS NOT DISTINCT FROM`, or a typed CASE), rather than let SQL UNKNOWN pass. Guard array-length calls by positive array shape so malformed JSON is refused cleanly.

## Audited CHECK discovery

2150's request/result checks are **unnamed in their source**, so do not guess `...request_body_check` or `...result_check`, assume a generated suffix, or drop every CHECK touching these columns. Discover CHECKs only on this exact table by their audited expressions: object type plus `octet_length(request_body::text)<=4096`, and object type plus `octet_length(result::text)<=8192`. Inspect `pg_constraint`/`pg_get_expr` or `pg_get_constraintdef` under the migration's catalog context, require exactly one match for each complete original expression, and drop only the discovered names with `%I`. Fail migration if absent/ambiguous/different. The reviewed source establishes the expected expressions; this reviewer has not observed a live deparser representation and does not invent its formatting.

The named `bpay_next_workbench_retry_work_ck` should likewise be verified against the **2311** expression before replacing it. Keep the five-field `bpay_next_workbench_retry_execution_ck` and immutable trigger/ACL/RLS unchanged. No broad pattern, column-only search or older2259 `='PREPARE'` assumption is a substitute for the exact audited original.

## Byte limits and replay

**32KiB resolution args /64KiB stored result are valid as ceilings**, with the original RETRY request/result ceilings4096/8192 in its action arm. The public0312 outer request already caps the whole `{actor_user_id,action,args,readback}` object at32768; therefore a32768-byte args object may be rejected once wrapped. Worker must check the actual serialized RPC envelope after trusted actor injection, not only its shorter browser body/args. The advertised usable request must fit that envelope; the100-row count bound is additional and not a promise every100-row payload fits.

Similarly, a65536-byte stored result gains received/replay/action/receipt wrapper bytes. The current Worker retry decoder cap16384 cannot be reused unchanged for this variant. Resolution-only decoding needs a separately closed bounded envelope (the existing128KiB transport ceiling is sufficient), and the final serialized response must be checked after wrappers. Keep retry's existing decoder and8-second mutation timeout behavior unchanged. Do not silently truncate resolution result Work/decision rows to make the receipt fit; refuse before committing if the complete immutable result exceeds its bound.

Resolution replay compares request UUID, discriminator, actor, Candidate, original module epoch and exact original validated args before fresh currentness/seed/clock/publication. Readback without receipt returns not-received without mutation. The existing retry helpers select by request UUID and compare exact actor/args; their required eight-key shape is disjoint from the proposed resolution `{request_id,candidate_id,context,works,reason}`, so this discriminator extension does not require a rewrite of retry replay or active-job guard. New resolution branches must still compare action explicitly and never return another action's receipt. Existing `retry_input_job_id`/revision proofs remain genuine PF08-only associations.

## Current source hashes

| File | SHA-256 |
| --- | --- |
| `design/WB-29-w2-native-command-design.md` | `dd9e0a5bfcd187c4b487da24cb2354b3911ad4bff2cb8a18e15c41df5a207744` |
| `10102026_2150_banking_pay_next_workbench_retry_receipts.sql` | `fba527a2a935213329c321c7713c97394e7f75a86216772978ea497b3b6cae0e` |
| `11102026_2259_banking_pay_next_prepare_retry.sql` | `383dd587f7783662fa980871b85b597562544c12ab4a1e924325a025aca0d8f3` |
| `11102026_2311_banking_pay_next_prepare_retry_receipt_null_guard.sql` | `5db2d3bb2522a1352fb45e5f25dd6d0f85c651d45c0df6179bf581dd1ce48f61` |

No concrete design blocker was found for this narrow receipt reuse. Conditional CHECK discovery, full-envelope limits and new source implementation remain required checks; this receipt does not approve whole WB-29 or joined acceptance.
