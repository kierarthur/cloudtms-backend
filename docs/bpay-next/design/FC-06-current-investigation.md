# FC-06 current investigation — sequence 106

Read-only source/design investigation, 11 October 2026. This is not implementation approval, a runtime receipt or closure of FC-06. No product/checklist changes, database calls, tests, fixtures, rendering or hosted actions were performed.

## Controlling scope

The full approved card is ordinary backend `codex_outputs/claude-stage2-continuation-20260925/BPAY_NEXT_BUILD_PLAN_20261009_codex_r3/03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:1130–1147`. It requires a New Manual Debt window with principal/outstanding, explicitly chosen tax treatment, required comment, optional note, repayment by weeks or weekly amount, Monday start, threshold/floor, mandatory original-liability reference and validated current pay method. The reference must be visible on the Case. It forbids recasting imported underpayment or creating a negative Timesheet. TAXABLE means GROSS_DEDUCT at PAYE Draft; NON_TAXABLE means NET_DEDUCT reserved at Draft and taken from entered NET. Later Umbrella recovery remains CM-13a's owner.

Local `docs/bpay-next/IMPLEMENTATION_CHECKLIST.md:3129–3148` and its six FC-06 CSV rows distinguish owning D03 (CASE_CREATE contract) and D04 (New Manual Debt screen) from contextual D01/D02 (existing accept/apply), D05 (card) and ITEM. Dependencies are FC-03, WB-08 and F-16. The card says no DB/Worker beyond FC-03: reuse the creation owner and only correct proven omissions in that existing contract. No new command, financial owner, storage, import path, Case list screen or lifecycle framework is required.

## Current exact owners and reusable behaviour

The current final-definer registry, not the card's old `0330` literal, identifies:

| Existing owner/signature | Current physical source |
| --- | --- |
| `private.bpay_next_case_create_terms_v1(jsonb)` | `supabase/repeatable/11102026_1536_banking_pay_next_case_create_intake_v1.sql:5` |
| `private.bpay_next_case_create_context_v1(uuid,uuid)` | same `1536:101` |
| `private.bpay_next_case_create_preview_v1(uuid,jsonb)` | same `1536:128` |
| `private.bpay_next_accept_case_create_v1(uuid,uuid,uuid,text,text,text,numeric,date,date,numeric,bigint,numeric,numeric,text,text,text,jsonb)` | same `1536:147` |
| Retained 14-argument same-name accept | same `1536:273`, wrapper into the 17-argument owner |
| `private.bpay_next_apply_case_create_v1(uuid,uuid,bigint)` | same `1536:289` |
| `private.bpay_next_current_case_component_page_v1(uuid,uuid,bigint,integer)` | `supabase/repeatable/10102026_2026_banking_pay_next_current_group_details_v1.sql:557` |
| `private.bpay_next_case_list_row_v1(uuid,jsonb)` | `supabase/repeatable/11102026_1738_banking_pay_next_case_list_v1.sql:101` |
| `public.bpay_next_preparation_page_v1(uuid,text,jsonb)` | same `1738`; CASE_PREVIEW at425, CASE_COMPONENTS at506 |

The 15 entered creation operands already map directly to the required form: Candidate, MANUAL_DEBT kind/subtype, explicit TAXABLE/NON_TAXABLE, principal, nullable due_date (unused here), start_monday, weekly_due or week_count, nullable threshold/floor, required reason/comment, optional note and original_liability_reference. CASE_CREATE adds command_id and preview_token. Do not add a second outstanding amount: the new native Manual Debt starts with its approved principal and has no prior recovery.

Terms `1536:5–99` already enforce closed input, positive penny amounts, genuine dates/Monday, positive bigint weeks, threshold/floor bounds and required comment. Server derivation at81–93 supplies ceil-to-penny weekly amount, sufficient weeks, partial final instalment and representable last Monday. The browser must display this returned schedule, not reuse the legacy JavaScript calculator. Context at101–126 supplies actual current Candidate channel, nullable Candidate default floor and native policy/version context; a null default floor stays unknown.

Preview is STABLE and write-free. The accept owner takes module/admin SHARE, Candidate SHARE, then Candidate worker_control UPDATE, command-specific advisory/replay point, policy/context checks and only then the short command-clock allocation (`1536:183–267`). Exact accepted replay compares every entered operand, actor and token before later current-channel/context refusal (`203–218`). Apply consumes immutable request facts; MANUAL_DEBT becomes a RECOVERY/DEDUCTION component with GROSS_DEDUCT or NET_DEDUCT (`336–338`), opens its native period (`344–348`), records approval and increments FVR/refreshes the exact Case in the same transaction (`361–367`). These money/event/queue owners need no new formula.

FC-04 supplies the actual native creation pattern in `js/bpay-next-payment-advance.js`, qualified main opener, current Candidate picker and `CloudTMSBpayNextOffice.mountCasePreviewFooter` (`js/bpay-next-office.js:1934–1950`). Reuse original Candidate-scoped journal/UUID/bytes, context invalidation, write-free Preview, targeted CASE_COMPONENTS acknowledgement, accepted closure and exact unknown retry. Native API const within the page is the existing endpoint boundary; no new cross-page endpoint certificate is part of this card.

## Two essential existing-contract omissions

**1. Mandatory reference is not yet enforced for a new Manual Debt.** Terms `1536:36–39` validate reference only when non-null. The metadata migration `11102026_1536_banking_pay_next_case_notes.sql:9–10` permits null. BE `broker/src/bpay-next-case-contract.js:20–21` and FE `js/bpay-next-case-preview.js:22–23` permit a null reference. Therefore a required HTML field alone cannot meet the approved server contract.

Smallest safe correction: refuse a missing/blank reference on Manual Debt Preview, and on first new acceptance in the existing 17-argument owner, while retaining its exact accepted replay before the new requirement. No new column or historical NOT NULL/backfill is needed. Blindly making the common terms validator reject null before the accept owner's replay point would also reject previously accepted exact requests; keep that distinction explicit. Preserve the retained 14-argument signature and its genuine stored-credit caller (`04102026_1827_banking_pay_next_candidate_destination_group_v1.sql:26,64`, CREDIT only). Any fresh Manual Debt call through that wrapper must still meet the new admission requirement rather than silently bypass it. Closed BE/FE validation and form prevalidation must agree, without making an already accepted original receipt unreadable.

**2. Saved reference is not returned by the existing Case readers.** Current CASE_COMPONENTS `2026:584–607` returns identity, instruction, balances, rule/schedule and due facts, but no reference. Current CASE_LIST row `1738:208–229` likewise has no reference. The older CURRENT_CASES picker `0585:22–26` is deliberately a compact factual list; it need not grow unrelated metadata.

Expose only the saved `original_liability_reference` as a bounded nullable factual field through the two named existing reader owners above, and their exact closed Worker/Office row contracts. Use the original immutable request's exact Case/Candidate binding. `04102026_0320_banking_pay_next_case_create_request.sql:11` already has unique case_id, so an indexed point lookup suffices; no supporting index/table is missing. Cases without that provenance return null. CASE_LIST needs it for the later Case surface; CASE_COMPONENTS is the current exact targeted post-create reader and existing component/detail route. No money, sort/count key, extra query kind or all-Case inventory changes are justified. Note/comment are already immutably captured by FC-03; this finding does not request unrelated provenance-field expansion.

## Supplied SCR-119 and narrow screen obligations

Read-only supplied index `SCREEN_DESIGN_POLICY_20261010/DESIGN_INDEX.md:134`, B9 spec `specs/SCR-119_new_manual_debt.json`, HTML source and individually viewed `png/SCR-119_new_manual_debt_dark.png`. It is a small native dark New Manual Debt window: Candidate identity/channel, amount, tax radios, comment, original reference, repayment-mode selector, weeks/weekly amount, Monday start, affordability inputs, optional note and shared Preview/Apply footer. Its pictured selected tax radio is example data, not permission to default tax treatment. Its Wednesday/pink inline error retains all entered values and leaves Apply disabled.

Build only the form/shared footer and qualified reachable creation entry. Current actual legacy opener is `js/main.js:15575`; dispatcher `102691` reaches it through `openFinanceCreateModal`. Preserve the ordinary legacy/edit branch and separately owned bank intent. Do not reuse legacy `Number/Math.round` weekly derivation. Current native payment-advance opener demonstrates the owner/actor/parent-stack checks and minimal accepted-return refresh; no future FC-02 page or FC-04 form duplication is needed. The Case page behind the supplied modal is context, not an early-build instruction.

Required design conditions: no tax default; comment/reference required; null versus genuine £0 retained for threshold/floor; explicit Candidate-default floor; actual channel-specific copy (do not promise PAYE gross/net execution on an Umbrella Candidate); no automatic imported debt or current totals guess; appropriate field error with entered values kept; only returned native schedule in Preview; one primary action; unknown/accepted retained requests keep original bytes and terms. Phone controls/Close must be at least44px with reachable scrolling/footer. Later A4 must compare every actual visible difference with this supplied PNG and shared SCR-116 under current owner policy.

The card's £120/4 weeks/INV-2291 example, PAYE gross/NET outcomes, saved-reference visibility and later Umbrella recovery are **NOT_RUN joined acceptance**, not proved by this static investigation. No requirement to create business fixtures follows from this note.

## Read-byte evidence

SHA-256 at investigation save; source files are authoritative at these bytes:

| File | SHA-256 |
| --- | --- |
| creation repeatable1536 | `853ddce45455defed9f715a2593267794f3b224e469a9eaccde2aeca6e44a470` |
| current Case component2026 | `278bd4d3dccc1e7453a69d1d2e0e4bdcde7075e5fe5181c5fe090c4a9dfc7e8a` |
| current Case list1738 | `ab912386daf210e25a2d5b6f37038b00aac8081ca0ba27e059deffdb17f47231` |
| existing notes migration1536 | `a4feb96c06df1a436378384fb504dec67ef80a33e963a7c5d4d223829adb3211` |
| BE shared case contract | `91d2bc7499d1b786ef743e5ae1a9997446d90d4fe1fe84647e84d0892b3fc135` |
| BE preparation transport | `40c02030afe7fbf15366d2ea9cfbfd19745a9a8e5c399b85b6bc04156025c646` |
| FE shared footer/contract | `16914287a7f09824a4a9d9e8f99533209918970ae7a311660f9c17828a792cb2` |
| FE Office | `cbdb844f11509a334cab8e87fc0cdc247b6e73ebe3f741ac8cb5be789e700745` |
| FE payment advance reuse | `cccc07b702b6de26af6e9a13d2423c910dbfbde5e9fb74dc0eacb19f93a5d46b` |
| FE main | `92da2244129e65fec443411019ec06d8729ec0f1287752166ab2b037f92ac32b` |
| supplied SCR-119 PNG | `6ace374ba0b1484373f80f8e9366d92a19ab60a152bf7d00c2ff1b4bb11453c9` |
| supplied SCR-119 spec | `0e3caa758f8aaf6bb3e6b456b57e86e4fe091b9473f0e91fd562c6e93755c0eb` |

Recommendation: design-first form reuse with the two exact contract omissions closed; preserve existing money owners and all accepted replay. FC-06 remains unbuilt.

