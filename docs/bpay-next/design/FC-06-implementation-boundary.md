# FC-06 implementation boundary — BD118

Sequence106, 11 October 2026. Authored by the read-only investigation/review agent at the builder's explicit direction; the check below is an author scope self-check, not independent review. Root alone implements product. This document does not mark FC-06 built, installed or app tested.

## Authority and exact deliverables

Full approved FC-06 card: ordinary backend `codex_outputs/claude-stage2-continuation-20260925/BPAY_NEXT_BUILD_PLAN_20261009_codex_r3/03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:1130–1147`. Current exact checklist `docs/bpay-next/IMPLEMENTATION_CHECKLIST.md:3131–3154` has owning D03 CASE_CREATE contract and D04 New Manual Debt screen; D01/D02 existing accept/apply and D05 reviewed card are contextual evidence. Dependencies FC-03/WB-08/F-16 remain. Source census and read-byte hashes: [FC-06-current-investigation.md](FC-06-current-investigation.md), SHA-256 `a3a89c973082156ef21bda7434565891dd22bb046374c282343f30bf92f81e57`.

Use the existing MANUAL_DEBT/MANUAL_DEBT CASE_CREATE, its atomic immutable metadata and shared Preview/Apply. No new command/RPC, table, index, formula, queue, public trigger, provenance framework, imported debt path, negative Timesheet or future Case-page build. The card's “none beyond FC-03” DB/Worker scope permits only the two demonstrated omissions in the existing FC-03 contract below.

## Exact SQL support, with original parents

Create one complete successor for only these four current named bodies, preserving their signatures, owner-only ACLs and unrelated behaviour; registry/provenance and finite source-gate facts must identify the exact successors.

1. `private.bpay_next_case_create_preview_v1(uuid,jsonb)`, current `11102026_1536_banking_pay_next_case_create_intake_v1.sql:128`: new MANUAL_DEBT Preview must require a non-null, nonblank original_liability_reference within the existing2048 UTF-8-byte bound. Keep the original closed input validation and genuinely write-free STABLE path. No authority helper, receipt, command or business lock is added.
2. `private.bpay_next_accept_case_create_v1(uuid,uuid,uuid,text,text,text,numeric,date,date,numeric,bigint,numeric,numeric,text,text,text,jsonb)`, current same1536:147: enforce that requirement **after the original exact prior-request replay return at203–218**, before accepting a fresh Manual Debt. Keep all original typed operands, context equality, lock order, UUIDs and receipt. Retain the14-argument compatibility wrapper unchanged: CREDIT callers continue to work, while a fresh Manual Debt through it cannot bypass the same17-argument admission. Previously accepted exact null-reference requests remain replayable. Do not move a mandatory-reference check into common terms/token validation ahead of accepted replay.
3. `private.bpay_next_current_case_component_page_v1(uuid,uuid,bigint,integer)`, current `10102026_2026_banking_pay_next_current_group_details_v1.sql:557`: append only the factual nullable original_liability_reference read from the original immutable creation request bound to the exact Case and Candidate.
4. `private.bpay_next_case_list_row_v1(uuid,jsonb)`, current `11102026_1738_banking_pay_next_case_list_v1.sql:101`: append the same saved factual field so the existing Case list/detail contract can display it later.

`04102026_0320_banking_pay_next_case_create_request.sql:11` already supplies unique case_id for indexed point lookup. No request means null; old null-reference history remains honestly null. Match Case/Candidate and MANUAL_DEBT provenance rather than borrowing another row's reference. No new note/comment reader fields, list filtering/sort/count keys or whole-Candidate reads. Preserve current component100-row/120000-byte limits and Case-list90000-row/119936-assembled-body bounds. Public preparation-page and intake bodies already dispatch to these owners and need no copied replacement.

Reuse unchanged terms/context/apply owners1536, all request immutability, original command ordering and WB-08 refresh. The accept order remains module/admin SHARE → Candidate SHARE → worker_control UPDATE → exact command replay/advisory → current policy/context → short command clock. Apply still creates one Manual Debt recovery component and period using captured terms; it does not recalculate from current UI.

## Closed transport and saved metadata

No new creation operand: the existing14-key input retains Candidate, kind/subtype, tax treatment, principal, due_date, start_monday, weekly_due, week_count, threshold/floor, reason, note and reference. For this form due_date is null, comment maps to reason, note is optional, reference is required. CASE_CREATE adds the original command_id and preview_token; CASE_PREVIEW stays input-only. Keep existing32KiB creation,12288-byte entered input and24576-byte token limits, exact input/version echo and original receipt.

Update only the existing BE CASE_COMPONENTS/CASE_LIST and FE Office CASE_COMPONENTS closed row schemas for the new **required member with nullable value**, original_liability_reference. Non-null is a nonblank string without NUL, at most2048 UTF-8 bytes, and only valid for a MANUAL_DEBT row. Null is legitimate for other origins and old history; a missing field is not silently treated as null. Keep exact row identities, ordering, cursors and complete/byte checks. No FE Case-list UI is built early.

Common input/token parsers must retain historical null-reference readability so the accepted journal/receipt route is not retroactively rejected. The new Manual Debt form enforces fresh required input; the native Preview/new acceptance owns the server requirement. Use existing typed input/metadata refusal and finite decoders rather than generic context-error relabelling.

## Native New Manual Debt window

Reuse the actual FC-04 native Payment Advance pattern, current NEXT owner/actor/parent-stack admission, native Candidate picker, schedule/affordability controls and existing Office shared Preview/Apply footer. A separate small Manual Debt form may own these fields; do not create a new generic finance framework. Preserve the ordinary legacy and edit path through the actual `openBankingManualDebtAdjustmentModal` entry, whose current saved location is `js/main.js:15575`.

The form presents actual Candidate identity and pay channel; principal/outstanding is one initial positive approved amount, not two independent money operands. Require an explicit TAXABLE or NON_TAXABLE choice with neither radio selected by default, required Comment, and required Original reference. Add optional Note, weeks/weekly-amount modes, a Monday Start week, nullable earnings threshold and nullable take-home floor using Candidate default. £0 is distinct from blank/null. Maintain existing numeric/date/bigint/UTF-8 bounds and field-specific inline errors/aria links while retaining all entered values.

No browser financial derivation: only display server Preview's weekly amount/weeks/first-last Monday/total/effective floor. PAYE wording explains GROSS_DEDUCT versus entered-NET recovery; an Umbrella Candidate must not be promised PAYE processing, and later recovery stays CM-13a. Imported underpayment is never inferred or converted into this new Case.

All edits, tax/mode/Candidate changes invalidate Preview. Picker return/remount retains current control ownership and does not recreate a pending request. Unknown outcome/reopen uses the same original Candidate-scoped journal, command UUID, bytes/token and captured form terms; no replacement Preview or new command occurs while it is pending. Shared targeted CASE_COMPONENTS acknowledgement remains authoritative, including the saved reference, before accepted closure. Old accepted null-reference recovery remains readable. Preserve the exact actor/NEXT/parent and page-constant endpoint fences; do not add endpoint certificate/storage.

## Supplied design and evidence boundary

Use supplied read-only B9 SCR-119 spec/PNG/index and existing SCR-116 shared footer. The supplied selected tax radio is sample data, not a default. The pictured Wednesday error retains entered values and disables Apply; mirror its single inline error/next-step behaviour. Its background Case page is future context. Any necessary Candidate picker, server-derived Preview, truthful unknown/accepted state or channel-qualified wording variation must be documented and examined in actual A4 screenshots. Use native dark window, reachable scrolling/footer,44px phone controls/Close, current owner copy rules and one primary action.

## Author scope self-check

The exact card and all six FC-06 checklist rows were reread before authoring. The boundary covers explicit tax/comment/reference, schedule/affordability/pay-method validation, original immutable note/reference, shared Preview/Apply and reference visibility. It limits support to four existing SQL owner successors plus closed reader contracts and the actual native form. It preserves captured economic inputs, original14-argument compatibility, old exact replay, all money owners and legacy routing. No essential scope contradiction found in this author self-check.

An independent source/design review must identify itself separately after root authors product. The £120/4 weeks/INV-2291 PAYE Draft £30 gross-stage outcome, NET-stage recovery and later Umbrella recovery remain joined acceptance **NOT_RUN**. Actual UI/policy/A4 review, local metadata compilation/contract checks and owner-authorised release evidence remain separate; this note provides none of those outcomes.

