# WB-14 screen scope investigation — 10 October2026

Read-only full WB14 card `02_WORKBENCH.md:591–620`, all three ITEM/D01/D02 checklist rows, shared W12–W14, owner A3/A4/A5, actual current UI/1743 reader, and read-only policy/design index. Viewed indexed T04b/T04c and SCR08/SCR08b PNGs; read their actual JSON including overlay (the child design is in overlay, not the parent window). Coordinated the actual reader/group/query scope with foundation_inventory. No product/pack/spec edits, app/runtime/tests/SQL/data/fixtures/hosted/network/secret operations. Root alone implements.

## Exact ownership and design index

D01 is the single physical `Five-column reason-driven issue detail in js/bpay-next-workbench.js`. D02 is the output-only reference card. The card requires genuine ISSUE_DETAIL identity/reason, server nominal/unresolved amounts and affected Candidate count, reason-specific navigation to real owners, child Refresh, standard Close/Escape and preserved parent position. It does not introduce a decision command.

INDEX maps **SCR05** to the main Action required list (T04b), **SCR06** to Blocked (T04c), and **SCR08** to the actual WB14 issue detail child. SCR08b is the Pay Query variant. SCR05/06 remain relevant because A5 retained the old WB13 paged/technical body only until this WB14 screen touch; its finite screen corrections now become due. There is no authority to leave that old body untouched while claiming the new child alone completes the screen policy.

Owner A3 supersedes Previous/Next row paging and click-to-open: continuous bounded scrolling; click highlights, double-click/Enter opens; phone44px Open; main rows never expand; dark even device-light; every visible data header server-sorts except Actions/tick/fixed exclusions; no pay date before execute. A4 requires actual real Office/index/standard-modal product PNGs with minimal local read responses, dark/device-light/phone/empty/failure as applicable and all42 independent results. No pack tools/spec regeneration. A5's exception ends only for the actual WB14-touched issue surfaces; no Ready or later unrelated body rewrite.

## Actual appearance and controls

| Surface | Required current appearance | Omit until actual owner/data |
|---|---|---|
|SCR05 Action required|Main three tabs; real section count/scope/search/Refresh; flat five columns Candidate, Payment, What needs attention, Amount ex VAT, Actions. Candidate name/TMS subline; real payment/week; plain reason; factual amount; genuine action buttons. Failure keeps valid old figures grey and real retry, no£0.00 substitute.|Unsupplied Updated timestamp, per-Candidate progress/WB19 or failed-save examples without actual action; Create/Export not owned by WB14; no made-up rates or row controls.|
|SCR06 Blocked|Same standard flat list/card shape, plain reason with pay-hold text if supplied; no ticks. Bank details are warning only on Ready, never Blocked. Unknown query status gives honest reason/retry.|No artificial bank blockage or insufficient-pay repayment blockage; no unsupplied gate/query facts or guessed action.|
|SCR08 child|Separate standard wide1320px modal, plain issue title and actual affected Candidate count; five columns Candidate, Payment, What needs attention, Amount ex VAT, Actions; footer Refresh and sole header Close. Refresh reads this detail identity only. Read/query navigation does not decide money.|Illustrated Review rates/Resolve gross/Snooze/suggested restructure/hold controls when no built genuine callback exists. No disabled placeholder stubs/new producer/protocol.|
|SCR08b query child|Plain Pay query title, actual Candidate/ref, exact available reason/raised-by/time; Source checks navigation when a genuine existing destination is available; existing Timesheet navigation. Bounded query rows may be a simple read-only detail under actual current typed facts.|No guessed actor/time, arbitrary held-back/current-paid amount, no money subtraction or claim query gate truth. Do not duplicate a card hierarchy merely to imitate the sample. Read only what exists.|

SCR08 sample has4 payments/3 Candidates, client/bucket sublines and several future-owner fix actions. Those are illustration, not evidence these fields or owners exist today. The current ISSUE_DETAIL nominal is retained even if recoverable is zero: use the genuine server value, not truthy-value fallback or browser-selected debt amount. Null/unavailable stays a dash with plain reason; never blank/zero guessed. Money text must use existing exact formatter and preserve gross/NET provenance with plain display labels. Dates, where actually supplied (week/query timestamp), use the existing date formatter; official pay date remains internal context only.

## Actual implementation seams and finite remaining screen work

Current `mountIssueLists` is at FE `js/bpay-next-workbench.js:738–809`; Office mounts it with external tabs at `bpay-next-office.js:570`. It already captures item_id/group_key/Candidate/work/Case/component/physical-Timesheet/reason and common context, plus scope/kind/search/sort/after/scrollTop for navigation. This capture should be reused, not rebuilt from visible labels or current Candidate data.

Current main callback `main.js:7165–7175` probes a nonexistent `openIssueWindow`, then closes the whole Office before opening legacy Timesheet/Candidate. The exports contain no issue window. WB14 must supply the real child with trusted initiating actor, owner/current-actor, client/standard adapter and exact captured identity/context from the existing controller. Avoid an options-free global that loses auth/frame/context or guesses current page state. Keep the parent/controller/list mounted; child Close/Escape returns to the same exact parent and scroll/focus position. Navigation to an actual external owner may deliberately replace the child using existing standard callbacks, but closing the detail alone must not close Banking Pay.

Minimum issue-list changes at the A5 seam:
- Replace old Previous/Next/First-page controls with the same finite continuous read/cursor window pattern used by current Ready/pickers; retain bounded native keyset context/search/sort, loading-more and user-triggered stale Refresh. No whole-list load/history sweep.
- Flat universal five-column table/cards and real header sort controls. Current names are Decision needed / Why it is blocked; approved common detail header is What needs attention. Main lists may retain its appropriate plain blocked reason wording where the actual guide requires it.
- Single click highlight plus row arrows/Home/End and existing double-click/Enter; phone Open, standard focus/parent guards, no main-row expansion. Current row only has double-click/Enter/Open item.
- Remove raw component_key subline and internal SOURCE/Frozen/NET-adjustment lecture/footer. Use real Timesheet/reference/week and finite plain payment kinds only where typed identity establishes them; do not map arbitrary component strings to an invented business type. Existing actual amounts remain unchanged, renamed plainly (Amount ex VAT / As authorised / Reserved / deduction balance as appropriate) without collapsing different money bases.
- Current dropdown only supports NAME/PAYMENT/AMOUNT; no What-needs-attention sort exists. A3 therefore needs a narrow actual reason sort in the existing reader/closed mirrors if that data column is shown, rather than browser sorting or a fake clickable heading. Actions are exempt.
- Keep old successful figures/rows visibly out of date on failure with real Try again, block interactions, and distinguish a genuine empty result from unavailable. Current old error.message exposes raw code and repeated unavailable/chatter; replace only finite visible error presentation, keep typed refusal/reload semantics.
- Existing issueReason channel wording `PAYE>UMBR`, SOURCE/Frozen amount labels and component identifiers violate manual wording policy. Plain PAYE to Umbrella / Umbrella to PAYE and supplied pay-hold/future-week/withdrawal/reauthorisation sentences suffice; no new reason classifier.

The exact child should request ISSUE_DETAIL with genuine group_key+reason_code, optional item_id only if the scope is genuinely one selected member. Passing captured item_id unconditionally would shrink the requested affected group to one row. Preserve the opening common-context comparison, appropriate actual section and immutable native identity; fresh mismatching context requires user Refresh/adoption, never automatic new identity.

## Reader facts and genuine omissions — coordinated with foundation_inventory

| Current fact | Actual authority / constraint | Screen consequence |
|---|---|---|
|Native group|1743 uses WORK:UUID /CASE:UUID, each native owner belongs to one Candidate.|Existing affected_candidate_count exists→0/1 is truthful only for that current group.|
|Shared3-Candidate cause|Ordinary revision genuinely captures source_umbrella_id (1009 migration/current1236 producer), and1429 target metadata captures target Umbrella. Neither records an exact shared change/cause connecting separate native WORK/CASE groups.|Do **not** group unrelated same reason/current Umbrella or use sample3. Under root's explicit BD069 choice, only the exact three-Candidate shared-cause Done journey is BLOCKED; all other work proceeds with genuine group/reason and count. A fake economic group is prohibited.|
|Affected amounts|ISSUE_DETAIL supplies nominal_source_ex_vat/amount_known and named approved/reserved/deduction/recovery/included/gross-due values, native direction/stage.|Use explicit supplied values including zero. No recoverable/headroom estimate, preview or current-money substitution.|
|Payment/client/rate detail|Current detail has Timesheet ref/week/member identity; no client display, actual chosen/suggested rate or action-authority fields.|Omit sample Client/Day/Night/suggestion data unless a narrow genuine existing projection supplies it. No mockups.|
|Pay Query display|Private0956 open_display provides informational OPEN manual/incident rows for the retained genuine Source root; not joined by current1743 ISSUE_DETAIL.|Use existing typed display owner through genuine root proof, not the full gate/facts or recomputed qualification. Bounded raw-page/actual continued query rows, no hidden truncation.|
|Manual query who/when|0956 joins tms_users and returns opened_by_name plus opened_at_utc, actual office_reason;8keys include query_kind/query_id/reason/opened_by_user_id/opened_by_name/opened_at_utc/work_date/evidence_available.|Manual Raised by can show actual name; Raised uses actual timestamp. Never show UUID.|
|Incident query who/when|0956 actor fields deliberately NULL; timestamp is actual incident.created_at_utc and reason from comparison family.|Omit unavailable Raised by or display a plain unavailable value; do not borrow today's actor/manual person. Its reason/time are informational only.|
|Source root|Foundation traced genuine retained revision.physical_timesheet_id and1438 physical-scope equality.|Do not pass an arbitrary today's Timesheet UUID to private display. Never turn informational OPEN into new payment blocking decision.|

Root implements exact supporting reader choices; this note does not claim shared-cause/count/query/format gaps already closed. No F25 parked producer/PIN/enroller/clock/protocol is implicated.

## Real action destinations and later ownership

Review rates WB30/WB31, Resolve gross WB31, suggested restructure FC/CM owners, snooze WB38 and hold WB40 must use their true later callbacks when built; do not add command transports or perform their decisions here. Where current real actions exist, use them rather than inventing:
- Existing `main.openTimesheet({timesheet_id: issue.physical_timesheet_id})` can open the genuine physical Timesheet. Its native fast-open parser reads timesheet_id, not id. This is not prematurely building WB17's new Timesheets View feature.
- Existing Candidate record can show actual bank data; warning is a Ready badge, not a new blocked fix.
- Actual Source import workspace exports `CloudTMSWeeklySourceImportWorkspaceV1.open(initialTab,options)` and supports tabs `imports/queries/finalise/history` (`import-workspace.js:20,1845`). Its existing Queries tab is the current Source checks navigation opportunity. Preserve existing F15 owner client and initiating/current actor bridge from `main.js:343251`; do not invent unsupported per-payment filter/preselect or silently default an invalid “checks” tab to Imports.
- Detail/query Refresh is an explicit read of the exact opening scope; generic retry retains failed read; typed stale uses explicit Refresh. No command nonce, automatic business retry, mutation or side effect on open.

## Required A4 evidence after implementation

Actual main SCR05/06 and child SCR08 normal dark/device-light/phone plus genuine empty/failure/query variant as implemented, under actual Office/controller/standard modal and smallest fixed local reader responses. Inspect all applicable42 policy/user-eyes points, including server header sort, parent position on closing, exact nominal zero/unknown presentation, genuinely supplied raised-by/time, no future-control stubs, phone44px Open and dark theme. Source support is not executed keyboard/large-data/joined business proof. No new test programme, timing fixture, pack tool, spec edit or hosted action is proposed.

## Current source hashes

| File | SHA256 |
|---|---|
|FE/js/bpay-next-workbench.js|4cdf12a964f25b4c8ebd2d6ca05d03c7387cd36bf8d0c45c3857a81f570adbd8|
|FE/js/bpay-next-office.js|d377ce9c8eb7073169f045d997200c19c47a828bcf88ff5fcdf9b9df9696c189|
|FE/js/main.js|7cde52d8f113f9c35f19f2f0be198390e2d742118c991b98e29ca06dcc645845|
|BE/supabase/repeatable/10102026_1743_banking_pay_next_ready_candidate_counts_v1.sql|727d80356ecfd7ef7e8c822c3181403602910ab4cef5bb50f4677dd32df2b760|
|BE/supabase/repeatable/10102026_0956_weekly_source_pay_query_open_display_v1.sql|e0c83140e923ab4d7fd856abb09ca6419c43c0c6d1c9056f03a7013e86f74a9b|
