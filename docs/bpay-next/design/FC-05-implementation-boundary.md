# FC-05 sequence105 — implementation boundary BD117

11 October 2026. Design before product build. Read with the complete FC-05 card at approved plan03:1109–1128, all seven working checklist rows, `FC-05-current-investigation.md`, current FC-03/FC-04 boundaries and original supplied B9 SCR-118/SCR-116. This note assigns only **FC-05-D03 DOC CASE_CREATE (FC-05)** and **FC-05-D04 SCREEN**. D01/D02/D05 are context, not another SQL implementation. FC-04 final review is pending; FC-05 product waits for that boundary. Root alone writes product.

## Existing owner chain and limited supporting source

Reuse the installed FC-03 typed creator and immutable receipts: current `11102026_1536_banking_pay_next_case_create_intake_v1.sql` terms/context/preview, original 14-argument accept plus existing 17-argument note/reference/preview-token overload, apply and financial intake. Reuse `public.bpay_next_preparation_page_v1(uuid,text,jsonb)` under current `11102026_1738_banking_pay_next_case_list_v1.sql` for CASE_PREVIEW and exact CASE_COMPONENTS readback. The current final-definer registry controls these owners. Their original Case/component, ledger, job, funding and FC-16 guard chain remains authoritative.

Existing Worker `bpay-next-command.js` CASE_CREATE and `bpay-next-preparation.js` CASE_PREVIEW/CASE_COMPONENTS already have the exact Credit input/token/receipt contracts. FC-04's narrow preservation of the exact native source-channel refusal is shared FC-03 support, not an FC-05 Worker feature. **No SQL, schema object, new reader, route, action, proof framework, queue or money formula is needed by FC-05.** D03 records the concrete reusable contract and source provenance; it does not claim new DB ownership merely because the generated row's generic wording mentions it.

The small product surface may be one FE `js/bpay-next-manual-credit.js`, native main entry/menu and existing asset/CSS integration, following current FC-04's actual `bpay-next-payment-advance.js`. The only shared preview support is the Credit presentation described below. Do not turn these windows into a new generic form/controller architecture. Preserve FC-04 fields, default term behavior, receipt recovery and native ownership.

## Native entry and scope

Use the existing `openBankingManualCreditAdjustmentModal` creation seam, with a qualified NEXT branch **before** its legacy Banking state/session/bank/details reads and `/manual-credit-adjustments` call. Follow current FC-04's real actor/admin, originating native frame, `bankingNextOwnerClient` owner/epoch, current-page API and native adapter/picker pattern. Qualification failure must give the native opening refusal; it must not fall through to the retired financial route. Preserve non-NEXT behavior at its original boundary.

Expose only this own New Manual Credit entry alongside the current NEXT New Payment Advance entry until FC-02 builds the full Cases host. The existing `openFinanceCreateModal` and `banking:finance:newManualCredit` are current callable seams. A seed with an existing Case/edit identity is not a new Candidate creation; do not treat generic `id` as `candidate_id` or implement later edit here. A real seeded Candidate is prefilled, and unseeded entry uses the existing Candidate picker. No invented name, reference or PAYE default. Pay channel is read-only from actual picker/caller facts, then the validated Preview context is authoritative.

Use the native small window titled **New Manual Credit**, header Close, native footer/focus/keyboard behavior and same frame identity. Picker onPick arrives before its child closes: retain the selection only for the current original actor/frame, then mount/refresh on the parent's native onMount. Late picker, Preview and readback results require the same actor, owner/epoch, modal, Candidate and instance generation. Do not repaint the parent over an active picker or unrelated native window.

## Entered contract and server-owned consequence

The form follows SCR-118: Candidate and actual channel, Amount, Tax radios, required Comment, optional Note, then shared Preview and native footer.

- Start with **neither tax radio selected**. Taxable maps to TAXABLE; Not taxable maps to NON_TAXABLE. No falsey/default/omitted tax choice becomes an accepted request. Preview and Apply stay disabled until the entered form is valid, with a short visible reason.
- Fix `case_kind=CREDIT`, `case_subtype=MANUAL_CREDIT`. Principal is the entered positive decimal string with at most two decimal places (whole pence); reject invalid, zero, negative, subpenny or overlong input rather than round, truncate or normalize through floating point.
- Submit `due_date`, `start_monday`, `weekly_due`, `week_count`, `minimum_earnings_threshold`, `take_home_floor_override` and `original_liability_reference` as explicit null. No start/week/term/threshold/floor/date fields appear. The existing Credit repayment-field refusal remains authoritative.
- **Comment maps to reason**, nonblank and <=2048 UTF-8 bytes, excluding NUL. Optional Note maps to nullable note, <=2048 UTF-8 bytes, excluding NUL. Preserve actual entered content; no generic auto-comment or copied example. Empty optional Note is null. Labels remain Comment and Note, not a second competing Reason field.
- Every input, tax or Candidate change invalidates the displayed Preview/token. Apply requires the same exact validated input and native token. Validation identifies the actual field with the existing inline error/accessibility pattern; errors remain one sentence with the staff next step. Busy/pending/accepted state prevents changes and conflicting creation.

Existing 1536 terms:9–99 and apply:336–343 own the economic distinction: taxable PAYE CREDIT is GROSS_ADD and non-taxable CREDIT is NET_ADD with its reimbursement fixed. A Preview principal is not a payroll net/bank-cash estimate or evidence of payout. Create works for genuine PAYE and Umbrella independently of future CM-13a payment allocation. Do not add a client calculator or native money owner.

## Credit preview and supplied design differences

Current shared `bpay-next-case-preview.js:74–78` labels no-week previews “£x to pay” and adds Threshold/Take-home floor. Add only a Credit-specific presentation branch/option: heading such as **£40.00 credit**, actual Total and chosen tax treatment; omit all repayment/affordability/date rows. Use current closed Preview total/input/context only. No fake repayment amount, final net, fundable amount, headroom, future Draft/execution date or new response field.

Keep the supplied **Taxable / Not taxable** choices. For a real PAYE Candidate, SCR-118's “Added to PAYE gross” / “Added after PAYE net” are accurate stage hints. For Umbrella/unknown channel, use neutral tax wording or omit those PAYE-specific hints; do not label an Umbrella credit as PAYE payroll. A successful Preview binds the native actual channel. If the original Preview returns EXISTING_REPAYMENTS, render only that actual existing warning, without the illustrative aggregate or a new comparison.

The original read-only pack is `SCREEN_DESIGN_POLICY_20261010/batches/B9/{specs,html,png}/SCR-118_new_manual_credit.*`; its overlay is small, has neither radio checked and footer hint “Choose taxable or not taxable.” Its Candidate/£40/Missed mileage values are examples. The required source/pixel differences are:

1. A real Candidate picker/seed and actual read-only channel replace the example text input/person; unselected entry has a genuine disabled reason.
2. Actual native Banking background is used while future FC-02 Cases list/counts/filter/actions are absent. Do not implement the illustrated background to fill a capture.
3. PAYE hints appear only for actual PAYE; Umbrella creation has truthful neutral tax copy and no claim that CM-13a payment exists.
4. Preview appears only after an actual successful original reader response. Shared SCR-116 styling is reused, with Credit-specific total/tax facts and no repayment/affordability rows.
5. Required validation, checking/saving, refusal, retained unknown and accepted/readback states add only the needed live status/recovery copy and original controls; they do not authorize future features or server fields.

Every later visible difference must be recorded and justified against the owning card/owner, not silently excused by this list. The native appearance/tokens, small600 frame, labels, typography and spacing follow original SCR-118 and supplied policy; phone controls are >=44px with reachable fields/footer and no overflow.

## Original journal, family qualification and completion

Reuse the **same original Candidate-scoped `.case-create.<Candidate>` journal** and `Office.createController` / `Office.mountCasePreviewFooter`, with the current original canonical FC-03 journal. No parallel family journal, census, endpoint metadata or cross-page transport assurance is added. Existing current-page API/actor/NEXT/token qualification remains; endpoint-change joined acceptance is NOT_RUN.

FC-04's current source has closed the historical foreign-family hydration gap: `bpay-next-payment-advance.js:75–79` checks ADVANCE/PAYMENT_ADVANCE before restoring fields, otherwise hides the form and asks staff to finish the original Case change. FC-05 must mirror that exact narrow support for **CREDIT/MANUAL_CREDIT**, restoring its explicit retained tax/comment/note/principal only after family qualification. A retained other-family pending request remains byte-identical and blocked from fresh creation with truthful guidance; never reinterpret, clear or replace it or add a new UUID. Do not create an action to complete the other family from this form.

An accepted-awaiting-read journal has the original Case/Candidate identity without all terms: hide editable/fresh form defaults, retain actual Candidate identity and use the existing generic accepted Case Refresh/read path. Do not claim that today's default form or this window's title proves the accepted family. Checking shows Checking…; Apply shows Saving…; unknown offers staff **Try again** of the original immutable bytes; accepted read failure offers **Refresh** of the exact Case, never another Create. Native Close retains pending/accepted operation. No automatic mutation retry.

Exact calls remain GET CASE_PREVIEW, POST CASE_CREATE, then GET CASE_COMPONENTS for original accepted Candidate/Case. ACCEPTED_PENDING_CASE proves neither funding nor paid cash. An empty exact read says “This Case is being added”; failed read preserves acceptance; only a matching Case/page completes. Return the accepted receipt/page through existing onCaseCreated and `{bpay_next:true}` result. Current main's `childResultShouldRefreshPayWorkbench` suppresses the old full Pay refresh for that result; reuse it. Update only an actual owning Case host, without fabricating future FC-02 or claiming an unperformed Candidate-record entry.

## Later owners and required evidence

Do not add one-off bank details or treat a legacy stored one-off origin as destination authority. FC-07/FC-09 own later edit/bank controls, FC-08 funding and CM-13a Umbrella allocation. Payout-committed amount/tax/comment read-only and switching-to-taxable removal of one-off data remain original RQ-087 context for those owners. There is no Case action/funding/reauth workflow beyond the named Create contract here.

Before product closure, source review covers explicit empty tax, null repayment operands, required Comment/byte limits, dirty Preview, actual channel, current native entry and frame/picker guards, original retained same-/other-family bytes, accepted readback and targeted return. Actual local A4 then compares original SCR-118/SCR-116 beside unselected/seeded forms, both tax choices, Comment failure, successful/dirty Preview, checking/refusal, real Umbrella, pending recovery, foreign-family refusal, accepted queued/read-failed/complete and phone footer reachability. States may combine only if each required difference is visible.

Apply the full42 policy checklist, manual policy.json wording and actual-image checks23/24; owner-forbidden pack regeneration25 and readerAdditions alternative26 are N/A. Pack/specs/PNGs are not edited. Source/A4 use authorized fixed local responses only; no business fixture or new joined test program is requested by this note. The £40 non-taxable next-Draft NET_ADD, due-once, original mutation/replay, queue, funding and payout journeys remain **AUTHORED_NOT_RUN / app acceptance NOT_RUN**. This design does not tick or install FC-05.
