# FC-15 owner ruling and current boundary — 12 October 2026

Read-only investigation at sequence135. Product, catalogue and joined application acceptance are **NOT_RUN** by this reviewer. Root alone writes product. This note supplements, rather than replaces, `FC-15-investigation.md`; its earlier1536 plan-owner reference is now historical.

## Binding requirement and explicit conflict

The ordinary-backend owner file `codex_outputs/claude-stage2-continuation-20260925/MESSAGE_TO_BANKING_PAY_BUILDER_NOW_FC15_SP10.md:3–14` requires every paid reduction to become one overpayment of the net difference, including ordinary PAYE, Umbrella/non-PAYE, Weekly Source, protected/unprotected and a removed shift after Accept source. An unavailable automatic collection must leave a recorded manual-collection obligation; neither omission nor REVIEW in place of the liability satisfies it. Lines16–18 retain extra-only increases and unchanged frozen Drafts. Line20 describes a **forthcoming** replacement-chain request, not an authority to implement a new chain now. Line22 explicitly requires conflicts to be logged **BLOCKED**.

**BLOCKED — coverage conflict requiring a recorded build-boundary amendment:** approved card03:1346–1347,1356–1362 restricts this first1519 chain to PAYE origins and ordinary WORK/ADDITIONAL/EXPENSE/MILEAGE; ordinary Umbrella automatic collection belongs to CM-13a and further Source qualification to SP-09. The later owner requires all-origin liability retention now. Do not silently equate PAYE-only automatic qualification with full owner completion, or install future automatic owners to conceal this conflict. The minimum separable requirement is retention of the same genuinely paid Work/component liability in a finite manual-only state when automatic qualification is unavailable; its exact schema, authority and UI mapping need the root design before implementation.

There is no conflict over the principal formula: the current native reconciler computes the paid-minus-approved difference (or the exact complete segment negative amount), preserving recovered, written-off and active reserved amounts. “Net difference” means the correction delta here; it does not invent per-shift PAYE take-home NET. Bank-return cash remains its own original obligation. See card03:1345,1348–1351,1368; R2:18–23,80,88.

## Exact card deliverables and current successors

Re-read the full FC-15 card03:1339–1370 and all checklist rows D01–D13. D01 and D09 are CONTEXT. D02/D11 cover the same basis guard; D03/D12 the same reconciler; D04/D13 the same expiry scope; D10 is the actual origin successor. D05 is recovery apply; D06 is the proposed `banking_pay_next_work_collection_non_work` migration; D07/D08 retain the ORIGINAL_TAX_UNBOUND and WORK contracts. Duplicate semantic rows do not authorize duplicate physical owners.

| Exact signature | Current registry-selected parent under `supabase/repeatable/` |
| --- | --- |
| `private.bpay_next_work_collection_origin_v1(uuid,uuid,uuid)` | `10102026_1236_banking_pay_next_segment_authority_final_v1.sql:4461` |
| `private.bpay_next_reconcile_work_collection_v1(uuid,uuid,text,uuid)` | `11102026_0814_banking_pay_next_preparation_discard_v1.sql:885` |
| `private.bpay_next_work_collection_guard_v1()` | same0814:1208 |
| `private.bpay_next_expired_work_collection_scope_v1(uuid,uuid,uuid)` | `11102026_0559_banking_pay_next_worker_reprepare_v1.sql:6674` |
| `private.bpay_next_apply_work_collection_recovery_v1(uuid,uuid)` | `10102026_0316_banking_pay_next_csv_umbrella_row_projection_v1.sql:414` |
| `private.bpay_next_work_collection_capture_guard_v1()` | 0559:5548 — supporting immutable capture guard, not a new liability owner |
| `private.bpay_next_worker_released_collection_scope_v1(uuid,uuid,uuid)` | 0559:328 — narrow cleanup support |
| `private.bpay_next_discarded_work_collection_scope_v1(uuid,uuid,uuid)` | 0814:541 — narrow cleanup support |
| `private.bpay_next_case_allocation_plan_v1(jsonb)` | **`12102026_1646_banking_pay_next_case_channel_resolution_v1.sql:3047`** — preserve FC13 and all prior FC12/FC09/WB41/DR14 branches |
| `private.bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer)` | `12102026_1536_banking_pay_next_case_restructure_v1.sql` — still current preparation parent |
| `private.bpay_next_workbench_refresh_case_v1(uuid)` | `12102026_0207_banking_pay_next_origin_wait_recovery_v1.sql` |
| `private.bpay_next_apply_position_page_v1(uuid,uuid,bigint,text,text,integer)` | `12102026_0016_banking_pay_next_stale_snooze_publication_v1.sql` — current caller, not an authority to redesign publication |

Original collection table: `supabase/migrations/04102026_1519_banking_pay_next_work_collection.sql:9–51`. Original cancel reconciliation stays1519 and original expiry reconciliation stays0510. Merge actual current body, installed predecessor and local intended1519 delta; do not treat a local historical1519 file as the final definer.

## Proven current gaps and minimum modification

1. The original collection table permits only sourcePAYE (`1519 migration:19`), and links/qualifies only TAXABLE (`48–49`). The card requires exactly its two audited NULL-safe tax CHECK replacements; retain original source restriction for **automatic** PAYE qualification. All-origin manual retention needs explicit subordinate support, not false TAXABLE/NON_TAXABLE classification or a duplicate CASE_CREATE/negative Work debt.
2. Current settlement capture only admits ordinaryPAYE WORK or exact committed Source/protected-head PAYE WORK (`0814:981–996`). For a missing basis, `1036–1039` returns whenever there is no effect or the origin is uncapturable. Therefore unsupported paid origins and an already-paid reduction first seen at POSITION_APPLY can disappear without a retained liability. The new manual-only path must close this real gap, with actual immutable paid effect/run-line/revision/transfer evidence; today’s approval or Candidate channel is not an original-paid authority.
3. Current origin `1236:4480–4509` hard-codes TAXABLE/WORK/GROSS_DEDUCT. Match genuine original ordinary WORK or ADDITIONAL TAXABLE and EXPENSE/MILEAGE NON_TAXABLE, preserving the original native component identity. Source branch remains genuine WORK under its existing authority. Tax, Case/rule/component and stage must agree; historical unknown remains unknown. New NON_TAXABLE recovery is NET_DEDUCT / NET_RECOVERY_CAPACITY, never a gross exemption or ERNI conversion.
4. Keep `0814:1136–1168` principal/conservation and one `(work_id,component_key)` identity. Repeated decreases update that liability; increases expose only later ordinary positive residual. Same-Candidate Draft offset, actual outstanding, recovered/write-off, reserved and recorded Draft recoverable remainder are different facts. Do not overwrite the principal with the remaining Draft amount.
5. The original origin helper is not the only automatic proof: current plan1646 repeats the collection tuple at3173 onward. Release0559, discard0814 and expiry0559 also certify tax/stage/purpose. Widen only exact matching PAYE NON_TAXABLE tuples, preserving genuine job/revision/hold/capacity/lease/deadline proof. Do not blindly replace every GROSS_DEDUCT; NET requires its existing weekly authority. Preserve the single allocation kernel.
6. Existing recovery apply0316 is channel-neutral within PAYE source=target/VAT0 conservation. No new formula is justified. Required same-transaction Work/Case Workbench refresh and supported-unavailable contract must make retained manual liabilities visible without making them automatically selectable or replacing them with generic REVIEW.

## Later-owner and frozen-Draft boundaries

Master08:59,64 orders expiry DR08→DR05→FC15→CM13a→CM15 and collection FC15→SP09→CM13a→CM13→CM21. SP10 sequence136 owns later Source change handling; CM13a sequence227 owns the automatic Umbrella producer/channel extension. Owner all-origin manual retention is required now, but is not promotion of either later automatic owner. Withdrawal is not an authorised zero; a genuinely removed accepted shift requires its native exact source evidence. Do not weaken Source certificates, relabel protected/headless evidence, seed historical unknowns or create new Underpayment Cases.

RuleB accepted choices/revisions, captured channel/floor/rules and original query cut remain the frozen Draft authority; real held/realised balances remain live under the existing financial locks. Parent Candidate order and short Work-before-Case locks, current DR37 Candidate fence, exact receipt replay and original operation IDs stay intact. A post-click correction affects a later capture, including PINNING; it never reprices the retained Draft. R2§4’s parked2153 accepted-input proposal is not the current RuleB implementation and is not a new storage/protocol task here.

## Investigation status

Initial boundary findings were saved immediately; the current-source census is complete below. No whole-item PASS is implied; the explicit coverage conflict and minimum manual-record contract must remain visible. No product, checklist, database, hosted or business actions were performed.

## Complete narrow current-source matrix

| Current point | Required delta or explicit reason to preserve it |
| --- | --- |
| Origin1236:4461–4516 | Card-owned successor: original immutable kind/tax equality, matching Case/rule/component tax and stage; preserve applied-position money, exact Source identity, original policy and Case/component conservation. |
| Reconcile0814:885–1206 | Card-owned successor: four ordinary kinds at981; genuine non-NULL supported classification at996/1047; unknown-only tax branch1092; original kind/tax at1103; tax/stage insertions1141/1144/1151. Preserve Source native binding1110–1133 and complete attribution. Add the owner-required finite manual retention instead of the missing-basis return1039, under the same actual job/Candidate order. |
| Guard0814:1208–1360 | Card-owned successor: original kinds1262 and tax/stage1282–1289; original INSERT scope1240–1243 currently permits settlement only. The manual fallback from current POSITION_APPLY requires an **exact new branch** proving the old paid tuple, not removal of the settlement-owner guard or acceptance of arbitrary historical rows. Original identity remains immutable; a disqualified paid basis cannot be silently upgraded (`1230`). |
| Plan1646:3047–3561 | Essential same-kernel support: inline automatic origin3161–3206 and zero-WAIT tuple3540–3549. Preserve the new FC13 ordered decision branch3138–3149, the exact Source comparison refusal3154–3158 and all older restructure/edit/funding rules. The original GROSS-only NOT_AFFECTED proof3209–3212 remains GROSS-only; NET recovery uses genuine weekly binding. |
| Prepare1536:3106 onward | Native flow already derives hold purpose and accumulates gross versus NET deductions separately. Its selected-set GROSS-only weekly exemption3262–3271 and frozen-instruction exemption3458–3479 must **not** be expanded to exempt NET. No new allocation formula or wholesale preparation replacement is required. |
| Release scope0559:328–355 | Essential narrow matching tax/stage/purpose alternative at347–348; retain the exact RELEASE request, economic capture identity, DRAFT0 result, released hold/use, remaining counter/cursor and same amounts. |
| Discard scope0814:541 onward | Same narrow alternative582–583, with existing discard ownership and completed release proof unchanged. |
| Expiry scope0559:6674–6728 | Card-owned exact alternative6716–6717; preserve original/current job, epoch/lease, request,72-hour fence, original selection counts, financial/execution revisions, released hold/use and no transfer/projection. No expiry policy rewrite. |
| Capture guard0559:5548 onward | Calls original origin at5584/5597/5634; no independent tax-stage literal was found in those automatic-origin checks. Preserve it; adapt only if the chosen manual-only row shape otherwise admits an automatic instruction. |
| Recovery apply0316:414–490 | Preserve complete existing owner. Genuine event/member/REALISED hold+capacity proof426–444; one signed effect447–459; exact unknown-result replay463–475; conserved PAYE source=target/VAT0 position479–487. This owner is valid for the new PAYE reimbursement NET shape without a new money formula. |
| Workbench Case0207:443 onward | Essential native projection support if new NET automatic origin is to retain WB41's behavior: matching tuple570–572 only, preserve accepted and RESERVED portions (`v_base`,565–590). Manual obligations need their explicit visible manual-only classification; an unknown or ineligible record must not enter this automatic-WAIT path. |
| Current position0016:172–253 | Existing NEW key and REMOVED-key phases already call the named reconciler200/253. Omitted native key receives approved0 at248–251; this is exact accepted-revision removal authority, not withdrawal/NULL approval. Existing expected phase/cursor readback151–157 prevents a repeated financial write. Do not redesign its snooze/publication continuation. |
| Current settlement1419:1837–1919 | Already posts genuine positive Work settlement in both target channels, then invokes the same reconciler1918. PAYROLL_SETTLED1889 stores the actual source-reserved amount and all target values before the hold becomes REALISED1916. Preserve DR38's classification/conversion and Source book seeding; unsupported collection retention belongs in the reconciler, not a second settlement writer. |

### Minimum all-origin manual fallback to specify in the root boundary

Prefer extending the **existing one Work/component collection identity** with an explicit manual-only retained liability and finite reason, rather than a second resolver/store/CASE_CREATE command. The design must specify these properties together:

- Actual original effect/run-line/hold/transfer/revision/approved-line/channel/classification/policy identity is retained, with nullable unknown classification; the recorded correction amount and observation revision are genuine native financial facts. Unsupported automatic eligibility never changes those facts. A new manual-only channel allowance must be conditional, leaving the card's automatic PAYE restriction intact; it is part of the explicit coverage-conflict amendment, not a silent general widening.
- Future actual settlement can retain the original paid anchor in the existing same transaction. Already-paid unsupported origins first reduced at POSITION_APPLY need a finite exact native lookup, proving PAYROLL_SETTLED + original REALISED hold + immutable transfer Work member. The ordinary effect schema supplies original IDs and all source/target amounts (`core migration1324:389–415`); the existing `(work_id,component_key,occurred_at_utc,id)` index1011–1012 supplies scoped keyset access. Do not aggregate agency/Candidate history, guess the original from the current approval, or use bank cash as paid gross.
- Repeat corrections retain one identity and the native principal protection; actual recovery/write-off/reserved amounts are not reclassified into new debt. Where same-day attribution is required, reuse the genuine complete native segment basis; its current proof1236:4105–4124 is ordinaryPAYE SHIFT/certified-zero only. It does **not** prove Umbrella, headless protected or every Source origin. Mixed segment history must not be reduced to an invented whole-day attribution.
- A manual-only amount record must be visible as an overpayment/manual-collection obligation through the existing narrow Work/Case contracts. A lone PAID_BASIS_UNBOUND/ORIGINAL_TAX_UNBOUND gap or Worker REVIEW is insufficient. If a truthful Case cannot be constructed because original tax/classification is unknown, preserve that unknown with the liability; do not invent a finance Case's non-NULL classification. The exact representation is a design requirement still open, not a completed schema decision.
- Automatic origin, plan, capture and resolution readers must refuse manual-only rows as automatic recovery authority. Future promotion, if later explicitly implemented, must consume the same identity and original evidence rather than double the debt. FC13 quote1646:123–129 already requires READY/qualified and original tuple equality; its later cross-channel support is not authority to qualify manual records here.

The current sources therefore prove both a feasible narrow automatic PAYE widening and a genuine missing manual-retention branch. They do **not** prove an existing all-origin automatic collector. The owner permits progressing with a logged conflict; there is no new permission question or authority to reopen completed producers. Exact manual representation and unsupported-source amount qualification must be explicit before claiming the owner requirement complete.

## Read-byte checkpoint (SHA-256)

These are saved-source hashes, not installed/runtime evidence. Owner file and plan are on ordinary backend disk; implementation paths are in BUILD-BE.

| Evidence | SHA-256 |
| --- | --- |
| Owner12Oct FC15/SP10 | `9DC43C7B561431A51138A11EC838C13EACC8E62EFFBDC7B01C8AD345B8888B27` |
| Full card03 | `C3053A5E5BAE73A933488D92A4E5E4098BA6046532852AED01D3D96FA38A18CB` |
| Master08 | `64DB4B6328C01336213F02DA1CACA0889DB119C0FA06B803ABBDA0756C30FC14` |
| R2 exact connections | `52DDE331F580D19E2A80C3866B79A0625F080D674B08D0D929E1AC72C048F125` |
| Current working checklist | `83CB44CAE5763962A205B3E6FEB771313F2453F3539B599A8803F3611998E7CE` |
| Final-definer registry | `86C3660AE376EC92E26432FA9C49123FDAAAB0143ADEF8693BBA219C46A53176` |
| Original collection migration1519 | `924FE1D8FEB9B3767BAE3261BA711B7837B4C2479CF689C780F3E651E28875D3` |
| Origin/segment1236 | `F74EA457BF77F0D85CE7F3F21089BDECF9D9D9121D4005850DE430D50AA7379D` |
| Reconcile/guard/discard0814 | `29F3099EBFB8A7E11D8C23A8996C5351EB9330316D8BDB6FD6D3A5136D3F379C` |
| Release/capture/expiry0559 | `514FB6A069DFE860D06C76D98302554BF3ED93E8E027CDBAC460BF8CEA7DA9A6` |
| Recovery apply0316 | `98067ED7E0CE4A633769F46DF1582227CF50239BFC54402BC93D14ED261914A6` |
| Current plan/FC13 1646 | `FEC5DD533F37C45F26E46011A551009AE8D62FB93A18862E082B2ACF74197456` |
| Current prepare/FC12 1536 | `14364DA406573B3031C30E9F2F74148862109EB2C03613E0035B3897B6F0665B` |
| Current Case projection0207 | `15518A516D7E4E1264E44B949EE432B694418DAAFD9DF9FDAF6FED22ABCA9F27` |
| Current position0016 | `2D9822DA9881179DACE5A2FF97C65FCAAF1DFBAAD920337A4701133D8FE487AE` |
| Current settlement1419 | `16E437BB839CC8A91CD499303CD0D6D574797E3FB0DA6570AB24858D42B6CE87` |

**Investigation complete.** Source findings and the explicit conflict are supplied for root's design boundary; no implementation or joined acceptance is approved by this note.
