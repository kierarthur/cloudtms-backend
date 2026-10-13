# FC-15 independent investigation — 12 October 2026

**Read-only findings saved before product implementation. No whole-item or runtime approval.**

## Immediate scope finding

The binding owner file `MESSAGE_TO_BANKING_PAY_BUILDER_NOW_FC15_SP10.md` requires one net-difference overpayment after every paid reduction, including ordinary PAYE, Umbrella/non-PAYE, Weekly Source, protected/unprotected and removed accepted Source shifts. Where automatic collection is unavailable, it requires a recorded manual-collection obligation; neither dropping it nor REVIEW instead of that obligation is allowed. Increases remain only the extra at next capture; frozen Drafts remain unchanged.

This is a genuine **coverage conflict** with plan r3 `03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:1346–1347,1356–1362`: its new producers are ordinary PAYE only, with Umbrella assigned to CM-13a and further Source work assigned to SP-09. The owner explicitly says to log conflicts BLOCKED with their reason and not choose silently. Record that conflict/amendment in the build boundary before product. The new manual-record fallback does not authorize installing the later CM-13a automatic collection or replacing SP-09's Source owner now.

There is **no principal-formula conflict**. Original creation remains `greatest(realised_source_ex_vat-approved_source_ex_vat,0)`, with exact complete segment attribution where applicable. Later updates retain recovered/write-off/active-hold protection. “Net difference” is the liability delta, not the whole paid amount. Same-Candidate Draft offset and post-payment residual are separate from original principal. Preserve Policy X, OR-44 and the existing financial owner.

## Exact current saved owners

Registry: `docs/bpay-next/final-definer-closures.json`, read on 12 October.

| Signature | Current final physical source |
| --- | --- |
| `private.bpay_next_work_collection_origin_v1(uuid,uuid,uuid)` | `10102026_1236_banking_pay_next_segment_authority_final_v1.sql:4461` |
| `private.bpay_next_reconcile_work_collection_v1(uuid,uuid,text,uuid)` | `11102026_0814_banking_pay_next_preparation_discard_v1.sql:885` |
| `private.bpay_next_work_collection_guard_v1()` | same `0814:1208` |
| `private.bpay_next_work_collection_capture_guard_v1()` | `11102026_0559_banking_pay_next_worker_reprepare_v1.sql:5548` |
| `private.bpay_next_expired_work_collection_scope_v1(uuid,uuid,uuid)` | same `0559:6674` |
| `private.bpay_next_apply_work_collection_recovery_v1(uuid,uuid)` | `10102026_0316_banking_pay_next_csv_umbrella_row_projection_v1.sql:414` |
| `private.bpay_next_reconcile_cancelled_work_collection_v1(uuid,uuid)` | `04102026_1519_banking_pay_next_work_collection_v1.sql` |
| `private.bpay_next_reconcile_expired_work_collection_v1(uuid,uuid)` | `04102026_0510_banking_pay_next_preparation_expiry_v1.sql` |

These files are under `supabase/repeatable/`. The original table is in `supabase/migrations/04102026_1519_banking_pay_next_work_collection.sql`. Do not use r3 line numbers or an earlier 1519 body as the final owner.

## Proven gaps and minimum necessary boundary

1. Original table admits only PAYE source and qualifies/links only TAXABLE originals. FC-15's own migration must widen the two audited NULL-safe tax CHECK expressions for genuine PAYE TAXABLE/NON_TAXABLE, retaining historical unknown classification without inferred backfill. Manual unsupported-origin storage needs a separately explicit finite state/binding; it must not masquerade as an automatically qualified collection.
2. Final reconcile `0814:981–996` admits ordinary or exact Source/protected-head PAYE WORK with TAXABLE paid basis. `1036–1039` returns without retaining a new basis when the actual paid origin is uncapturable. This is the concrete place that cannot satisfy the new “never dropped” manual fallback.
3. Final origin `1236:4480,4496–4509` hard-codes PAYE/TAXABLE/WORK/GROSS_DEDUCT. Widen only the card's exact classified PAYE ordinary Additional/expense/mileage forms, preserving Source head/component identities and rule/component consistency. Unknown original tax remains unknown.
4. Original reconcile principal `0814:1136–1137,1165–1168` already applies net difference and genuine complete segment basis. Preserve it, including W, R and active H; do not create a second negative line or liability owner.
5. Expiry/release/discard/cancel and apply must retain exact native job/effect/hold/capacity/proof scope. Any new NON_TAXABLE instruction requires the matching NET_DEDUCT/NET_RECOVERY_CAPACITY triplet, not a blanket tax or channel allowance. Later automatic Umbrella/Source writers remain their owning-card work.

## Evidence and limits

Read full FC-15 card and all FC-15 checklist rows D01–D13; D01/D09 are context, not separate product work. The actual deliverables include basis guard, reconcile, expiry scope, recovery apply, table migration, gap/WORK documentation and exact successor copies. Dependencies remain F-05, FC-01, WB-08, CM-04, DR-38 and DR-08.

No Underpayment producer is authorized: existing/carried cases stay visible; an increase is ordinary extra pay. The later reversal/replacement-chain request is explicitly forthcoming, so preserve identity seams without implementing a new chain now. Further exact guard/fallback observations follow below after the current-body census.

No product edits, SQL invocation, business data, fixtures, tests, screenshots, hosted calls or commits were performed by this reviewer.

## Completed current-body census and exact supporting seams

The final registry, not a copied predecessor, selects these additional owners:

| Required supporting seam | Exact saved evidence and restriction |
| --- | --- |
| Current Case preparation/Preview plan `private.bpay_next_case_allocation_plan_v1(jsonb)` | Final `12102026_1536_banking_pay_next_case_restructure_v1.sql:2607`. Its inline automatic collection proof `2716–2745` still duplicates PAYE/TAXABLE/WORK/GROSS_DEDUCT, so changing origin1236 alone is insufficient. Match the new original kind/tax and corresponding recovery stage, preserving all FC12 restructure and original creation branches. The previous0207 plan is not the final body. |
| Native worker release scope `private.bpay_next_worker_released_collection_scope_v1(uuid,uuid,uuid)` | `11102026_0559...:328,347–348` hard-codes TAXABLE/GROSS_DEDUCT/GROSS_RECOVERY. Admit the exact NON_TAXABLE/NET_DEDUCT/NET_RECOVERY_CAPACITY triplet only, with the same release request, DRAFT allocation/result, released hold/capacity, cursor/count and revision proof. |
| Native discard collection scope `private.bpay_next_discarded_work_collection_scope_v1(uuid,uuid,uuid)` | `11102026_0814...:541,582–583` has the same tax/stage/purpose restriction and needs the corresponding narrow triplet. Keep original discard ownership and release proof. |
| Native expiry collection scope | `11102026_0559...:6716–6717`: same narrow matching triplet, retaining all deadline, command, job, revision, phase, released hold and no-transfer/no-projection constraints. |
| Current Workbench Case projection `private.bpay_next_workbench_refresh_case_v1(uuid)` | Final `12102026_0207...:570–572` limits the origin-WAIT tuple to GROSS_DEDUCT. Newly supported NET_DEDUCT automatic Overpayment must retain the same qualified origin WAIT behavior, without revaluing an accepted portion or treating manual records as selectable recoveries. |
| Current plan's WAIT annotation | Final `1536:3083–3087` accepts only the GROSS tuple. Extend only for the same qualified NON_TAXABLE automatic zero annotation, preserving zero nominal/capacity/taken and unchanged E/H. This is annotation of the original allocation kernel, not a new formula. |

Do **not** blindly replace every GROSS_DEDUCT literal. The final plan `1536:2752–2755` and prepare pass `3458–3472` use it to certify `weekly_binding_state='NOT_AFFECTED'`. A NET_DEDUCT recovery needs the existing genuine PAYE NET/week authority; it is not a gross-only exemption. The new shapes must follow existing NET intake/projection and reserve-at-Draft semantics without weakening that proof.

The apply owner `0316:414–490` has no tax/stage literal requiring a new calculator. It requires an actual posted Case recovery with source=target ex, VAT0, inc=ex, exact realised hold/capacity/member/outcome and original operation identity; creates one idempotent WORK_COLLECTION_RECOVERED effect; then decreases the source position and reconciles. Those invariants are valid for supported PAYE reimbursement NET recovery too. Preserve its Source-book binding metadata and replay equality; do not widen it for manual/Umbrella collection now.

## Minimum manual-record design that still has to be specified

The new ruling requires a **retained liability**, not merely an error/gap code. Current `qualification_state` describes automatic proof, and `PAID_BASIS_UNBOUND` with no Case/amount record is insufficient. Likewise, setting Worker REVIEW/FINANCIAL_RESOLUTION_REQUIRED is not an acceptable substitute for recording the overpayment.

Reuse the same work/component liability identity and original native financial facts. Explicitly distinguish a manual-only recorded obligation from an automatically eligible collection. A finite manual mode/reason and the exact debt binding can be subordinate support of this one owner; it must not become another Manual Debt CASE_CREATE command, duplicate negative Work line, payout/refund command, generic resolver or money formula. Where original classification/channel is known, existing OVERPAYMENT Case/rule/component storage can hold its genuine principal, but the ordinary resolution/admission path must not silently make it automatically selectable. Where classification is genuinely unknown, do not choose TAXABLE or NON_TAXABLE merely to satisfy existing NOT NULL Case CHECKs; retain manual evidence without false tax authority.

Important existing-history edge: the current guard `0814:1240–1243` permits a new collection only under the original settlement job. Unsupported already-paid origins therefore lack a collection row. Widening **future settlement capture alone** cannot meet the owner's requirement when a later current approval is reduced. Any needed new manual fallback at current POSITION_APPLY must bind the original actual paid effect/line/hold/transfer under the current Candidate/job owner, rather than pretending that current approval is the original paid classification. Core `03102026_1324...:389–415` supplies operation/item, Candidate, Work/component, original Timesheet/transfer and source/target amounts; use the actual `bpay_next_effect_work_history_idx` at `1011–1012` (`work_id,component_key,occurred_at_utc,id`). Use finite indexed lookup/continuation if a whole original proof is required; no unbounded history aggregate or fabricated historical backfill.

For removed components, final native position application `12102026_0016...:243–253` sets the omitted key's approved position to0 and invokes this same reconciler in its REMOVED phase. That is an existing authoritative difference seam. It does not justify treating withdrawn approval as authorised zero, rekeying DAY to AGGREGATE without complete attribution, ignoring old active H, or changing the frozen Draft. Final segment basis `1236:4105–4124` certifies ordinary PAYE SHIFT/certified-zero and exact parent/segment totals only; it does not prove all Source/headless-protected/Umbrella paths.

The exact fallback storage shape, immutable original-paid binding, automatic-ineligibility rule, visible “manual collection” mapping, repeated-reduction update and later same-liability promotion must be written in the root design before SQL. This is necessary scope precision, not a request for a new capability or a permission gate.

## Explicit later-owner boundary and findings closure

R3 SP-09 is the Direct protected-family certificate/forward-capture card (`05_SOURCE_AND_PROTECTED_PAY.md:331`), with SP-10 later Source changes at359. CM-13a `06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:570–577` owns ordinary Umbrella automatic producer/channel widening. FC15 can retain the owner-required manual liability now without prebuilding those automatic owners. The later correction-chain request is not yet an implementation requirement: preserve original Work/component/Source binding identities and avoid conflating independent replacement Work, but add no new netting protocol.

**Investigation complete; actionable design gaps are recorded above.** Product correctness, installed catalogue, app journeys and manual-collection implementation are not reviewed or claimed by this document.

### Read-byte identities (SHA-256)

| Evidence | SHA-256 |
| --- | --- |
| Owner12Oct file | `9DC43C7B561431A51138A11EC838C13EACC8E62EFFBDC7B01C8AD345B8888B27` |
| Final-definer registry | `52F0FAF86533D88303CE5F655829EC9FC4F63EC46845E8C00533FBD49DBF0A2E` |
| Current origin1236 | `F74EA457BF77F0D85CE7F3F21089BDECF9D9D9121D4005850DE430D50AA7379D` |
| Current reconcile/guard0814 | `29F3099EBFB8A7E11D8C23A8996C5351EB9330316D8BDB6FD6D3A5136D3F379C` |
| Current capture/expiry/release0559 | `514FB6A069DFE860D06C76D98302554BF3ED93E8E027CDBAC460BF8CEA7DA9A6` |
| Current Workbench Case0207 | `15518A516D7E4E1264E44B949EE432B694418DAAFD9DF9FDAF6FED22ABCA9F27` |
| Current Case plan/restructure1536 | `14364DA406573B3031C30E9F2F74148862109EB2C03613E0035B3897B6F0665B` |
| Current collection apply0316 | `98067ED7E0CE4A633769F46DF1582227CF50239BFC54402BC93D14ED261914A6` |
