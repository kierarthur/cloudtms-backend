# DY-04 current source investigation

Read-only design investigation, 11 October 2026. Approved sequence **90**. No product changes, SQL execution, business rows, fixtures, hosted calls or app acceptance were performed. This is an implementation map, not an item completion receipt.

## Binding scope and current owners

The full card is `06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:954–980`; all DY-04 checklist rows are `IMPLEMENTATION_CHECKLIST.md:2514–2538` (D01/D02/D03 at 2524–2526), sequence `IMPLEMENTATION_SEQUENCE.md:97`, and the Daily amendment is `review/R2.md:158`. Dependencies are F-05, DY-02, DY-03 and WB-03. The required result is the complete **contractless-late joint closure**, preserving later current owners. A contracted-only overlay does not complete this card.

The canonical Daily calculator → Process → ordinary Office Authorise path remains the approval authority. RECEIVED is not finalisation. Genuine Daily is HOURS-only, retains its actual physical UUID/version and London worked date, and has no invented Contract, Source HEAD or Source binding. The common revision/position/publication and contribution refresh path remains the owner. The before-click/after-click accepted-input boundary and frozen Draft evidence remain unchanged. Source joins, pay-query producers, MyTMS approved display, money formulas, post-Draft live rate enrichment and broader expense/mileage admission are outside this change.

Current final-definer registry `docs/bpay-next/final-definer-closures.json` selects:

| Exact object | Current physical definition | Required narrow merge |
| --- | --- | --- |
| `private.bpay_next_approval_route_v1(uuid)` | `supabase/repeatable/03102026_1450_banking_pay_next_producer_route_v1.sql` | Retain complete current route; add the explicit positive Contractless DAILY branch before contracted classification. |
| `private.bpay_next_publish_ordinary_from_financial_v1(uuid)` | `supabase/repeatable/10102026_1236_banking_pay_next_segment_authority_final_v1.sql:333–1020` | Compose the pack's conditional identity/date/frozen-evidence spans into this complete current publisher. Do not restore the old 1530 body. |
| `private.bpay_next_ordinary_authorisation_trigger_v1()` | `supabase/repeatable/10102026_1332_banking_pay_next_withdrawal_projection_v1.sql:287–364` | Add DAILY_HOURS publication and NULL-safe factual withdrawal identity; retain current Workbench refresh. |
| `private.bpay_next_revision_guard_v1()` | `supabase/repeatable/03102026_1342_banking_pay_next_guards_v1.sql:12–86` | Add pack's positive Daily evidence/current physical identity branch; retain all existing seal, totals and immutable-row checks. |
| `private.bpay_next_work_insert_guard_v1()` | `03102026_1342_banking_pay_next_guards_v1.sql:171–186` | Add pack's positive original Contractless Daily physical Timesheet/financial proof. |
| `private.bpay_next_work_pointer_guard_v1()` | `supabase/repeatable/11102026_0928_banking_pay_next_daily_work_identity_guard_v1.sql` | DY-03 already adds immutable origin kind/date. Preserve this current owner; no second pointer rewrite is required for the pack's identical identity requirement. |
| `private.bpay_next_daily_approval_evidence_v1(uuid)` | `supabase/repeatable/11102026_0925_banking_pay_next_daily_five_standard_rate_rows_v1.sql` | Reuse DY-02's current five-row helper. Do not replace it with the pack's earlier all-NULL-row filter. |
| `public.timesheet_authorise_generic_atomic(uuid,uuid,uuid,timestamptz,text)` | `supabase/repeatable/10102026_0931_weekly_source_pay_query_outer_entry_admission_v1.sql:1753` | Only the proved missing-bucket admission refusal support below is needed; all current pay-query admission, lifecycle mutation and response behavior stays. |

DY-03's current `11102026_0928_banking_pay_next_daily_work_origin_identity.sql` supplies the nullable Contract conditional CHECK, origin fields and nullable-object revision evidence. It does not itself prove physical DAILY scope; the named guards below do. No further schema or public trigger is needed for this joint closure.

## Supplied joint closure and exact retained differences

Source package: `codex_outputs/claude-stage2-continuation-20260925/codex-review-20260929/fresh-requirements-20261003/DAILY_AVAILABLE_RATE_CAPTURE_V1_20261006`. READY.md and the entire 1,190-line `contractless-late.sql` were read. READY explicitly describes source readiness/native NOT_RUN and distinguishes the joint candidate from the contracted-only reference; its historical fixture/test instructions are not acceptance evidence for this task.

Use these pack spans as the feature delta, not as replacement current product:

- Route `contractless-late.sql:143–240`: LEFT JOIN Contract; positive Contractless DAILY/HOURS/current/nonadjustment branch at 183–190; all contracted expense, imported correction and Source classification retained. Do not admit absent Contract by ordinary fallback.
- Publisher `:248–911`: exact current TF/physical TS; conditional Candidate/Client/role/band/Contract identity at 331–350; actual Daily date; origin identity insertion/reuse at 468–484; frozen Daily approval evidence at 486–499; conditional real-day aggregate position at 677–706; actual Daily shift/break evidence at 717–735.
- Trigger `:913–997`: admit DAILY_HOURS at 945; withdraw using actual financial Candidate when Contract is absent at 953–961; original Work/physical-version matching at 974–984. Preserve the current trigger's transition/module early return `1332:302–315`, original withdrawal locking `:329–334`, and **financial revision bump followed by Workbench refresh `:356–360`**. The pack stops after the bump and would lose this current refresh.
- Revision guard `contractless-late.sql:1016–1107`: Daily Work requires ORDINARY source plus the exact Daily JSON version, financial/Timesheet IDs and version, Candidate, booking and original date at 1072–1083; non-Daily Work must not carry Daily evidence at 1085–1086. Preserve current whole-row immutability and native detail/totals checks.
- Work-insert guard `:1156–1184`: exact original physical Timesheet is Contractless DAILY/HOURS with London start date matching origin date; matching current, nonstale, authorised SELF_REPORTED financial Candidate/version is required at 1171–1179.
- The existing public financial authorisation trigger binding already invokes the same private function. Pack `:999–1003` DROP/CREATE is unnecessary for replacing its body and should not introduce a new public trigger. Keep existing binding and original function signatures, fixed search paths, ACL/revokes, module/epoch and lock order.

The current publisher contains later retained changes the pack must not overwrite:

1. Current `1236:413–425` STRICT Contract/common qualification becomes one explicit conditional Daily identity branch. Current Candidate channel/Umbrella locks and exact source Umbrella qualification at 429–437 stay, using the real TF Candidate for Contractless Daily; Client display lookup at 440–441 follows the same conditional factual Client.
2. Keep normalized Additional-code cardinality at `:475–478`, original expense carrier/totals and mileage precedence/refusal at `:480–529`. Daily evidence's HOURS-only exclusion remains `0925:47–51`; availability does not authorise ancillary work.
3. Keep the current **single available-rate array** at `1236:531–567`, including the current fallback/conflict protections and used-only Additional identities. Current DY-02 helper `0925:119–132` returns all five Standard rows and actual fifteen nullable cells. Do not restore pack's older Daily/non-Daily count or schedule split.
4. Keep current revision `1236:589–610` fields `source_umbrella_id`, `source_rate_authority_kind`, `source_rate_override_id` and `source_rate_default_id`; add Daily JSON beside them. Pack `:486–499` omits these later authority fields.
5. Keep current rate insertion `1236:611–632` from that same counted array, including approved pay/charge rates. Keep native SEGMENTS `:635–797`, aggregate missing-rate/precision guards `:804–813`, actual Additional missing-rate refusal `:939–976`, totals `:977–1013` and native staged publication `:1014–1015`. Only conditional Daily aggregate key/date/actual shift detail is inserted; no second calculation body.
6. Current genuine Daily helper `0925:23–76` qualifies exact physical/current/authorised SELF_REPORTED TF, factual identity, clocks, no ancillary amounts, original week/date and current context. Its existing dated resolver/current selected-authority checks at `:78–99` stay. Opposite-channel availability is captured at publication and can remain NULL; it is not fabricated as old TF evidence or resolved later in a Draft.

## Proved missing-authority Authorise bucket gap and minimum support

No Candidate override/default is not a zero-rate success. Current `broker/src/daily-canonical-authority.js:17–25` detects each **used** bucket when selected pay or charge is NULL; `:161` evaluates the same native calculation, and `:207–210` records RATE_MISSING (or retained UNPROCESSED) with `has_rate_issue=true`.

The current ordinary authoriser already locks requested/current Timesheet `0931:1855–1896` and exact current TF `:1925–1935`, preserves invoice/archive/already-authorised checks `:2089–2098` and expected signature `:2101–2127`. It then refuses every status except PENDING_AUTH/READY_FOR_HR at **2145–2146**, before authorised TF update **2200–2207**. Consequently the current publisher's bucket refusal `1236:804–806` cannot cover genuine current RATE_MISSING Daily rows. The Daily approval helper also correctly refuses invalid approved provenance `0925:37–52`; a stale-authority error is not this required bucket refusal.

Smallest concrete support: in a complete successor of the **existing generic Authorise function**, immediately before its original status refusal, add a read-only NEXT/genuine DAILY/HOURS, exact current physical/version/nonstale SELF_REPORTED missing-used-bucket qualification using the already locked current TF's five bucket quantities and selected pay/charge cells. Choose the native used bucket deterministically; raise the existing definite `BPAY_NEXT_ORDINARY_USED_RATE_MISSING` with native Daily identity and bucket. Never change RATE_MISSING to authorisable, stamp approval, reprocess, calculate money, invoke a new resolver/calculator or weaken earlier lifecycle gates. Other statuses or unqualified stale/invalid rows retain the original refusal.

Current `broker/src/index.js:93941–93961` recognizes that exact error but requires a Contract UUID at 93955–93957 and always tells the user to set the Contract rate at 93960. Extend this existing parser with one finite positively qualified DAILY evidence branch (actual Timesheet UUID, DAILY/HOURS, factual bucket and actual nullable Contract identity; no synthetic Contract), retaining the original ordinary contracted UUID branch and definite nonretryable result. Daily Standard authority comes from its Candidate override/Client default even when a factual Contract exists; the Daily branch must therefore precede the ordinary Contract guidance. Return Daily-appropriate factual rate-authority guidance through the existing single and bulk authorise handlers (`:168096–168099`, `:169919`, `:94517`). This is a small supporting fix within checklist D02's existing authorise path; no new route, command, general protocol or pay-query producer rewrite. Exact existing error recognition still fails closed for arbitrary payloads.

## Closure boundary

Build only the named current owners and this indispensable refusal/parser support. Keep current registry/final-definer and finite source-gate entries aligned so no later file restores an earlier owner. No extra public trigger, Source binding, new rate calculator, generic admission framework, headless/test programme, producer change or unrelated money owner is required.

Source review can establish composition and absence of overwrite; it does not establish runtime acceptance. Real Contractless authorise → common revision/positions/items/summary, ordinary/Source/expense regressions, multi-Candidate enrollment, auto-finalisation versus RECEIVED, exact before/after-click isolation and frozen Draft readback all remain **NOT_RUN**. No checklist state was changed.

