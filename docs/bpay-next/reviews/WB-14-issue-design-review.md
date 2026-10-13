# WB-14 issue source/design review — 10 October 2026

Independent read-only review of full WB14 card `02_WORKBENCH.md:591–614`, ITEM/D01/D02, shared W12–W14, BD069/A2/A3/A4/A5, indexed SCR05/06/SCR08/b and the existing policy. Read the complete current `mountIssueView`, `openIssueWindow`, reason/amount/action helpers; actual Office mounts, native main Timesheet callback, standard window adapter and scoped issue CSS. Compared the relevant1806 reader filtering/count/keyset contracts. Only this review and the earlier investigation correction were written. No product/pack/spec changes, tests, runtime, SQL/database, fixtures, network, hosted or business actions were executed.

**Final verdict: implemented read-only source/design and A4 screen scope PASS. All finite findings below are CLOSED after fresh source and every final product PNG were inspected.** This is not a full WB14 ITEM, app or full-plan acceptance claim. The exact three-Candidate shared Umbrella-change journey remains explicitly BLOCKED under BD069; genuine later financial/Source-query action-owner joins remain OPEN/NOT BUILT. Current native groups/counts must not be fabricated into the sample.

## Original findings — all closed; original snapshot retained for traceability

| Finding | Actual source | Required finite correction |
|---|---|---|
|H01 — Refresh cannot recover a changed revision|`bpay-next-workbench.js:788,825`: the child always compares each fresh reply with `parentContext:()=>issue.context`. Reset clears its own context but retains that immutable original parent comparison. Once the server revision changes, every Refresh returns another stale refusal.|Bind the first opening to the captured context, then let an explicit child Refresh adopt a genuine new reply for the same group/reason/section/filter. Subsequent continuation stays bound to that adopted context. Refresh remains ISSUE_DETAIL-only; do not change parent selection, fabricate context or fetch/rebuild money.|
|H02 — Fake child Open for Case rows|`:772,774,825` routes every child double-click/Enter and phone Open to `issueOwners.timesheet`. The actual main callback at7167–7169 rejects a null physical Timesheet, which native Case rows legitimately have.|Offer/invoke the native Timesheet action only when that row has its real physical_timesheet_id, or a genuinely available distinct owner. Omit unsupported future Case navigation rather than render an enabled Open that throws. Main Open continues to open the issue detail for every genuine issue row.|
|H03 — Failed continuation hides last successful facts|`:776` hides the entire table whenever `usable` becomes false on error. A failed later page therefore removes all successfully loaded figures. Policy SDP-58 requires retained figures greyed, a danger notice and Try again; no failed-read empty/zero claim.|Retain actual last-good rows visibly greyed and unavailable for actions on failure; show the inline retry. First-load failure can show unavailable without rows. Keep genuine empty dependent on a successful coherent response.|
|H04 — Exact withdrawal label regressed|`:524` changes APPROVAL_WITHDRAWN to `Approval withdrawn`. WB02 card123/141/145 explicitly requires `Missing approval — approval withdrawn`.|Restore the exact existing withdrawal label. The paid `Paid — awaiting re-authorisation` label is already preserved. No classifier/financial owner change is needed.|

## Source conclusions

The view genuinely renders five columns and supplies four actual server sorts. REASON uses stored priority, reason and native item UUID in1806 and both closed mirrors. The browser does not sort the financial rows. Payment/amount labels omit raw component keys and use exact existing money formatting; supplied nominal zero remains£0.00, unknown stays a dash. The amounts are the genuine named source/approved/reserved/deduction/recovery values, with no browser headroom/subtraction or financial decision.

Main issue lists now use bounded continuous scrolling, at most three retained100-row pages and32KiB compact cursor history, scoped native read keys, loading-more and explicit typed-stale Refresh. They replace old page controls at the A5 seam. Single click highlights, double-click/Enter opens, keyboard arrows/Home/End move through loaded rows, and phone uses the standard universal card styles and44px Open/sort controls. Scoped CSS retains standard dark/modal geometry; actual pixels still require A4.

The Office controller supplies the existing authenticated client, initiating/current actor, owner check and standard frame adapter to the internal child. It keeps the parent controller/list rather than closing Office on issue open. The real Timesheet callback uses the native `timesheet_id` operand and checks actor/NEXT before opening the existing record. Standard modal stack Close/Escape reattaches the parent; the child saves the original opener for focus restoration. Actual frame/client behavior has been inspected as source only, not executed.

The final action registry retains only actual Timesheet and factual Source-check callback slots; only the real Timesheet callback is attached now. All guessed rates/gross/restructure/snooze/hold/restore branches were removed until genuine typed owners are built. Thus generic CASE_REVIEW_REQUIRED cannot dispatch a guessed taxable restructure, and no retired route, placeholder mutation or unbuilt fix control is exposed. Current query display actor/time fields are not supplied to ISSUE_DETAIL, so no sample raised-by/time is fabricated. Source Queries navigation remains a later genuine join when its exact root/context is transported.

The current group is native WORK:UUID or CASE:UUID. Removing the optional item filter from the child request lets the reader return the whole real reason/group; count comes from `count(distinct candidate_id)` over that scope before cursor continuation, not loaded row count. Ordinary source_umbrella_id and current target Umbrella metadata do exist, but neither establishes an immutable shared change/cause across groups. BD069 correctly blocks the particular three-Candidate illustration while preserving actual one-Candidate scope and other work.

Opening identity still consists of the captured group/reason/section/common context. The current child supplies `item_id:null` so it does not accidentally reduce the whole affected group to the opening row. The SQL optional nonnull item field remains a row filter; it is not a separate anchor validation mechanism. No new aggregate cause identity should be inferred from that field.

## Inspected snapshot hashes

| Source | SHA256 |
|---|---|
|FE/js/bpay-next-workbench.js|8f2b8020b6330bc80ff63b815b38892a486ed3f983c7ef03ae122cc94b43d0d1|
|FE/js/bpay-next-office.js|3063789410b4f24bb0e251cdaa05758b9043da7adacb83ebdff2f979185a85a3|
|FE/js/bpay-next-ui.js|dbdcb12ed8ec027b0df39c52ef6f50b6d91ca93e2cb74451a59e09a6ba029272|
|FE/js/main.js|cd8cd5077d61d5d48182904fbd7c5b4b35efa4df1177994101d6c63f0cbb583f|
|FE/css/bpay-next-office.css|b9a0f77ccedcf3f89dab019c2e2b5832fd61cb36ec2cc63d06b6485ad5ea613b|
|BE/supabase/repeatable/10102026_1806_banking_pay_next_issue_reason_reader_v1.sql|bbde6d21a6aad4eeb5bfa0741976246f58fbdebd6410c163d759fcf3321c8329|

These initial hashes identify the original findings snapshot. The final closure/source hashes below supersede its pending verdict.

## Final fresh source and A4 closure

- H01 CLOSED: child first read remains bound to openingContext. Explicit footer Refresh and the inline typed-stale Refresh now call the same refreshDetail callback, clear that opening comparison and re-read only the same ISSUE_DETAIL group/reason/section/scope. Subsequent pages retain adopted common context. A genuinely changed context invalidates both parent issue view and shell; it never changes selection or quotes/rebuilds money.
- H02 CLOSED: child Timesheet invoke, Enter/double-click and phone Open require the actual nonnull physical identity. Native Case rows with no real owner have no fake Open. Existing real Open Timesheet remains on main and child; child phone avoids a duplicate Timesheet action.
- H03 CLOSED: ordinary continuation and explicit root Refresh preserve last-good pages/header/count on failure, grey and disabled. First load has no fake rows; genuine coherent empty stays distinct. Scope/kind/search changes deliberately clear unrelated old rows.
- H04 CLOSED: exact Missing approval — approval withdrawn label restored; paid reauthorisation unchanged.
- A4 coherent-empty finding CLOSED: minimal empty fixed response has Blocked HEADER/count0 and rows0. Final SCR06 image shows tab0/footer0/No payments are blocked, without a false nonzero count.
- A4 danger-token finding CLOSED: actual scoped alert uses existing --ctms-modal-danger; final failed image shows red inline error, grey£160.00 last-good facts/disabled action and Try again.
- Final focus/selected-token finding CLOSED: selected row uses actual standard panel-raised/accent inset; keyboard outline uses accent-bright and small text uses sub. The child no longer depends on undeclared next-selected/next-muted or a next-focus alias only present in its parent.

Every final saved product image was individually inspected after the last stylesheet recapture at18:20: SCR05 dark/device-light/phone; SCR06 dark/device-light/phone/empty/failed; SCR08 dark/device-light/phone. Desktop/device-light pairs are byte-identical; phone uses standard universal cards, genuine wrapped reasons and reachable controls. Actual capture was through full index/main/CloudTMSBpayNextOffice.open and standard real modal, minimal fixed read responses and blocked hosted network, with no database. Reviewer executed no rendering/app/browser/test/SQL/business/generator/pack tools.

All42 independent results are saved separately in screen-designs/SCR-05/review.md, SCR-06/review.md and SCR-08/review.md, each37 PASS/0 FAIL/5 N/A with final PNG/JSON/source hashes. Item16 is manual policy.json wording review, item25 pack-generation programme N/A under A4 and item26 readerAdditions/spec edits N/A while genuine reader-field mapping remains applicable. Those verdicts approve the implemented read-only views only; unavailable financial action joins and the specific shared three-Candidate cause are not claimed built. No execution of keyboard/hundreds-row/joined business journeys is inferred.

## Final source hashes

| Source | SHA256 |
|---|---|
| FE/js/bpay-next-workbench.js | 906c966bc4a671b1cc72a822071367172a1c1aff33764b6f9981bb25db845f3b |
| FE/js/bpay-next-office.js | 73239f720c497fd17daea4e09cfbe2fead8a0a0443d0067796c2ad7e8a1344f1 |
| FE/js/bpay-next-ui.js | dbdcb12ed8ec027b0df39c52ef6f50b6d91ca93e2cb74451a59e09a6ba029272 |
| FE/js/main.js | cd8cd5077d61d5d48182904fbd7c5b4b35efa4df1177994101d6c63f0cbb583f |
| FE/css/bpay-next-office.css | e80c90f378ca725671040f554524ca406861804c0414c049a53f9ea81d8d4ec6 |
| FE/css/modal-modernisation.css | a4003bf69088f282c7883ab394a5b679826e8937707da0593456a5045b8e3606 |
| FE/index.html | fc9d636114faacf62fb6bc1f124ed95689ff9f8c7aa11f7551813737330a444a |
| BE/supabase/repeatable/10102026_1806_banking_pay_next_issue_reason_reader_v1.sql | bbde6d21a6aad4eeb5bfa0741976246f58fbdebd6410c163d759fcf3321c8329 |
