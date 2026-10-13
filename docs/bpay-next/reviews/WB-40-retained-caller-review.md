# WB-40 retained route and caller source review

11 October 2026, sequence76. Independent read-only review of the authored, not-yet-loaded0426 migration/three repeatables and their retained Worker/Office callers, against the full WB-40 card/checklist, R2-F01/F02/WB-40, current E1/E3 correction, BD085 and `design/WB-40-build-scope.md`. No product edits, SQL/database execution, tests, fixtures, browser/screenshots, hosted calls or commits were performed.

## Verdict

**PASS for the retained route/UI and reviewed control/projection/accepted-choice joins. No essential caller source defect found.** This is static source evidence. Independent SQL structural review and the builder's compile/check evidence remain separate; actual route, rotation, CREATE ordering, queue/minute and application behavior are NOT_RUN by this reviewer.

## Retained route, fence and Timesheet UI

- Saved BE `broker/src/index.js`, `bpay-next-legacy-fence.js`, `bpay-next-preparation.js` and `bpay-next-workbench.js` have no current diff against HEAD. Saved FE `js/main.js`, `bpay-next-workbench.js` and `bpay-next-office.js` likewise have no current diff. WB-40 adds no Worker route/action or replacement Timesheet screen.
- Actual winner is `PATCH /api/timesheets/:id/pay-hold`, `broker/src/index.js:200178–200179`, calling the retained `handleTimesheetPayHold:109998`. It authenticates admin109999, preserves JSON/current-row refusal handling and `guardCurrentTimesheetWrite:110084`, then PATCHes only current TSFIN for the resolved current Timesheet110115–110117. A failed native write returns the existing error110119–110126 before updated:true; success retains native row/current identity/audit110128–110165. The new native hook therefore commits control/projection with the actual PATCH transaction, without a second refresh request or mutation-success claim after a SQL refusal.
- Global fence call is index198591. The actual matcher `broker/src/bpay-next-legacy-fence.js:162–184` fences named Banking/retired writers and leaves this Timesheet PATCH unfenced, including its normalized slash routing. The retained pay-hold route must remain documented as an exemption in the F-12 completion evidence.
- FE `js/main.js:330653–330687` retains staged hold/reason inputs;379088–379116 sends PATCH with on_hold/reason and expected_timesheet_id, then preserves current-row adoption. Whole-Timesheet stage364900–364901 and segment-level All/Some exclusion stage364602–364614 remain untouched. The latter still reads exclude_from_pay and is not remapped into the new whole-Timesheet control.

## Exact NEXT control and native projection joins

The migration `11102026_0426_banking_pay_next_pay_hold_control_fields.sql` adds only the bounded native control tuple to existing private Work and work_choice: version, changed UTC time, canonical Timesheet/financial IDs, effective boolean/reason. No new table or public monetary owner is introduced.

`11102026_0426_banking_pay_next_pay_hold_controls_v1.sql:7–84` is the card-named private hook. It returns NEW before other work under non-NEXT13, permits only the two named native tables14–15, skips unchanged/historical row mutations16–36, resolves exact old/new booking Work IDs38, and holds Candidate control then Work42–47. Canonical public lookup49–51 is Work booking -> current Timesheet -> that Timesheet's current TSFIN, with plain MVCC reads. No original/historical fallback or public row lock is introduced after Work. A genuine changed tuple advances version and stamps clock_timestamp after those locks55–60, then calls the final refresh wrapper62. Native currentness/hold bindings72–77 stay on the explicitly required Timesheets/TSFIN tables; unrelated legacy callbacks and approval/Source producers remain intact.

`11102026_0426_banking_pay_next_pay_hold_classification_v1.sql:24–29` uses the same canonical current join and distinguishes a missing current row from an unheld current row. Existing item pay_hold/pay_hold_reason are populated185. The new positive-offer branch192–197 applies missing canonical authority as DATA_UNAVAILABLE priority15 and PAY_ON_HOLD priority40, retaining stronger existing evidence/approval/Processing priorities. It recomputes only selectability/section/reason, not money. Existing before/after contribution replacement supplies the exact Work/group/Candidate summary update; current wrapper1438 is retained.

The saved pre-branch v_base186 preserves original H/R and accepted run/command state. The branch excludes is_selected native accepted portions192/194. Reserved handling261–270 restores v_base and remains Waiting/nonselectable; recovery255–258 is unchanged. Case collections/recovery are not paused by pay hold. The original informational no-member CERTIFIED_ZERO fallback287–314 is retained and is nonselectable292; no synthetic money or selection is introduced there.

Existing BE/FE closed Workbench schemas already accept nullable pay_hold/pay_hold_reason. FE `js/bpay-next-workbench.js:688` renders `Pay on hold: <reason>` and1494 offers native Work controls only in READY with actual eligibility/basis; BLOCKED rows remain outside that selection branch. No transport broadening or per-tick request is required.

## Original accepted control and skip behavior

`11102026_0426_banking_pay_next_pay_hold_accepted_choice_v1.sql:23–32` captures the observed current native control tuple only at original work_choice INSERT, under exact Work SHARE. UPDATE protection35–40 covers OPEN SUBSET choices as well as SEALED choices, while original segment/component counters and sealing remain permitted41–97. A later native hold change cannot refill these accepted columns.

The skip helper preserves original accepted member skip111–119, including held Work with no work_choice, before any current lookup. Only the legacy non-descriptor branch retains current admission, with canonical absence refused133–136 and the exact new `BPAY_NEXT_SELECTION_ON_HOLD` code137–140. Descriptor CREATE uses the same exact current join519–527 and writes its original run_choice_member.skip_code573 before accepted Work/Candidate membership. Original accepted upload acknowledgement retains its receipt and never requalifies against a later hold.

BE preparation164–165 and FE Office192 already decode both exact ON_HOLD and retained historical PAY_ON_HOLD codes within the closed <=100 per-page requested Work membership. New output is the card's ON_HOLD code; historical receipt compatibility remains intact. Existing SQL/Worker module and actor guards are retained; the private hook's LEGACY early return is separate from read/command NEXT admission.

Registry finals still point PIN to0151, capture/reservation to1236, shared Case preparation to0205 and native refresh wrapper to1438. No pay_hold/PAY_ON_HOLD read was found in those PIN/capture/Case-preparation bodies. WB-40 adds no delayed capture/PIN hold refusal, Draft invalidation, live financial substitution, new command/protocol or producer; live balances remain owned by their original locks/formulas. The earlier plan's capture-time alternative is superseded by the current E1/E3 skip-only correction.

## Evidence limits

The0426 product files were authored and not loaded at this review request. This reviewer performed source reads and ordinary Git diff/status inspection only. Native set/clear/no-op behavior, current Timesheet/financial-row rotation, concurrent CREATE/hold ordering, actual current-route error/result delivery, installed security/trigger state, queue/minute completion and all WB-40 app acceptance remain **NOT_RUN**. No catalogue, compile, hosted or business execution PASS is inferred from this source verdict.
