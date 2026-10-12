# WB-29 saved-rate snooze warning implementation

12 October 2026; seq140. **Read-only implementation findings; no SOURCE/A4/runtime approval.** Root alone builds. Authority: full WB29 card02:1073–1137, especially1116–1119; current0312 snooze context, native1100 action,2100 resolution schema/basis and2044/2303 decision indexes; supplied root SCR32/T03 and SCREEN_DESIGN_POLICY SDP50/51/54/57. No new screen, route, money owner or decision-clearing behavior.

## Exact warning and native predicate

At current private.bpay_next_snooze_context_v1(uuid,jsonb), repeatable11102026_0312_banking_pay_next_snooze_context_controls_v1.sql:148–153, add the closed PRE_OPEN code **RATE_RESOLUTION_REQUIRES_REVIEW_AFTER_UNSNOOZE**. Preserve the existing PRE_SAVE date CASE and warning_context keys unchanged. The card copy is:

“This shift/timesheet has a saved rate decision. It stays in place after you unsnooze; review it if it is paid in a different pay week.”

This exact card wording replaces the legacy claim that a rate is cleared/re-resolved after cross-week unsnooze. SNOOZE_CLEAR must leave resolution rows/events untouched. No comparison with today's target/pay week is required to display the warning.

Only already-qualified **WHOLE_TIMESHEET or SEGMENT** Work scopes qualify. Keep current context admission/service/admin/NEXT, physical Timesheet/version mapping, same Candidate/Work/current expected revision, sealed native revision, exact scope/basis and conflict checks. Do not add current==applied as a new snooze read condition or relax existing expected/current guards.

Define mapped ACTIVE membership using two separate indexed anchor branches:

- d.work_id=v_work.id AND d.candidate_id=v_candidate AND d.state='ACTIVE' AND d.based_on_revision_id=v_revision.id AND d.approved_line_id=a.id.
- Same Work/Candidate/state, d.published_revision_id=v_revision.id AND d.published_line_id=a.id.

In both branches a.revision_id=v_revision.id and a.component_key=d.component_key. Use the actual current native approved-line PK, not an original root line from another revision or a matching label/date. This reads the boundary/published ACTIVE indexes from2044, which2303 retains. Separate EXISTS branches preserve their keyed access; no latest-history walk.

WHOLE_TIMESHEET: warning when either branch has at least one genuine mapped decision for its actual native line/family identity: STANDARD on WORK/PROTECTED_WORK with existing rate_detail(approved_line_id,bucket=d.rate_code); ADDITIONAL on component_kind=ADDITIONAL and component_key='ADDITIONAL:'||d.rate_code; ADJUSTMENT on component_kind=ADJUSTMENT and component_key=d.rate_code. The saved native decision guard already validates taxable basis. Do not warn because a stale unrelated line or current reimbursement has a similar text key.

SEGMENT: use the already selected v_segment.id/revision_id/approved_line_id/component_key. Require mapped line=a.id=v_segment.approved_line_id, d.component_key=v_segment.component_key, family='STANDARD' and the exact positive native segment bucket:
DAY→hours_day>0, NIGHT→hours_night>0, SAT→hours_sat>0, SUN→hours_sun>0, BH→hours_bh>0.
Use a literal CASE or five literal OR branches, with native rate_detail existence for that bucket. A Day decision must not warn a Night-only shift on the same parent; another shift/date's decision is not evidence. Native segment index(revision_id,component_key,segment_key) and unique(revision_id,segment_key) already supply the exact scope.

CASE, EXPENSE_LINE and EXPENSES_ALL are excluded. Reimbursement does not acquire a rate decision, and a whole-expense batch cannot borrow a taxable sibling's warning. No target amount, quote, policy conversion, rate comparison or paid-week inference is needed for this membership check.

**Do not call component_resolution_active_v1 with today's channel/Umbrella/VAT/policy/date.** Its2100:206–225 compatibility filters intentionally hide historical target/date variants, precisely those the card says should remain visible and be reviewed after unsnooze. Read all mapped ACTIVE variants for the scope; cleared/unmapped decisions do not warn. Inclusive carry is already BD161's owner, not a snooze-side carry or clear.

Use keyed existence, not count/aggregate of Candidate history or an output decision list. EXISTS can stop at the first positive member. If an implementation materializes mapped decisions instead, retain the declared bounded scope/sentinel and refuse overflow; never inspect first100 and report warning=false for the rest. The warning does not disclose or select a financial decision.

## Closed transport changes

Both broker/src/bpay-next-snooze.js:100–107 and FE js/bpay-next-snooze-window.js:105–112 currently admit only NULL and the two date codes. Extend that **context** enum with the one rate code and validate phase/code/scope together:

| Context phase | Allowed warning code | Scope / consequence |
|---|---|---|
| PRE_OPEN | NULL or RATE_RESOLUTION_REQUIRES_REVIEW_AFTER_UNSNOOZE | New code only WHOLE_TIMESHEET/SEGMENT with actual Work/revision identity; advisory display. |
| PRE_SAVE | NULL or SNOOZE_DATE_BEFORE_NEXT_PAY_RUN or SNOOZE_INCLUDES_NEXT_PAY_RUN | Existing dated save/ack flow unchanged; rate code refused. |

warning_required remains exactly(code!=NULL); warning.phase must equal normalized request.phase (default PRE_SAVE), business_date remains the outer context's same native date. Keep all seven warning_context keys, exact context key set, finite strings/dates/UUIDs and original8KiB request /4KiB snooze arguments /existing finite response ceilings. No new top-level boolean or decision metadata is necessary.

For ACK receipt validation, require PRE_SAVE and the original date-only code domain. Keep normaliseBpayNextSnoozeMany's PRE_SAVE/date-only warning_context untouched; neither all-expense request nor CLEAR_MANY admits the saved-rate code. NormaliseBpayNextSnoozeAction should also reject a fresh ACK_ISSUE request that explicitly asks for PRE_OPEN. Do not globally enlarge every response/error enum.

Native **private.bpay_next_snooze_action_v1(uuid,text,jsonb)** is still final10102026_1100_banking_pay_next_snooze_v1.sql, not0312 context. At its ACK insertion branch:267–274 the old body currently accepts a PRE_OPEN context too. Adding the warning without a guard could create a rate-warning token through direct native ACK dispatch. Required small supporting guard: fresh ACK_ISSUE must have context.phase=PRE_SAVE and only original date-warning codes before inserting any acknowledgement. Preserve exact original receipt replay first, original actor/request/epoch identity, original worker_pay_basis/expected_version/context comparison, expiry30minutes and consumed_by_request_id. Do not add a saved-rate ACK table/token or alter SNOOZE_SET's PRE_SAVE authority.

## Minimal native window rendering

Current FE only displays a warning from understands(checked,args) after PRE_SAVE (lines307–310). Initial PRE_OPEN completion:371–381 never paints warning_context, and date/indefinite/Note input:370 always hides it. Merely admitting the code leaves the required warning invisible.

Retain a distinct opening-warning state derived only from context.warning_context. Paint the exact card copy in the existing small Snooze window before the subject/facts/fields using its native warning tone and data-next-live-copy marker. Do not reuse understands()'s two-way date-copy ternary, which would mislabel the new rate code as “This snooze includes the next Draft.”

SDP50 and viewed T03c require an “I understand” tick before Apply snooze when the check supplies a warning. Provide **local opening-warning acknowledgement for Apply/Save**, independent of existing PRE_SAVE date consent and token. Reuse existing checkbox presentation where possible, but preserve separate state: editing Note/date/indefinite must not erase the opening notice or accidentally satisfy a changed date warning. A date warning can replace the checkbox's current consent purpose while the opening notice stays visible; its existing exact warningInput/context equality remains the only date ACK authority.

The opening notice is advisory about the retained rate: do not add a server financial condition, reset/clear decision, automatically open Rates, require a new ACK command or block the existing direct CLEAR request. Unsnooze/Cancel snooze retain their existing confirmation and exact request journal; the PRE_OPEN notice may be shown alongside that confirmation, without another server acknowledgement. When only opening warning is present, accepting its local tick issues no POST. PRE_SAVE still runs before a dated SET and can independently require its date tick/ACK. Pending original requests continue with identical saved bytes and original recovery semantics.

Keep original native subject, actual shift/date/time, blank/create date, optional Note, earliest-date hint, active amend/cancel controls and one footer primary. No artificial “Review rates” button or guessed pay date/cash is added. Warning paragraph uses status/live announcement; failed check/refusal remains alert and precedes facts. Apply disabled by unchecked local opening tick needs visible “Tick I understand.” guidance; do not duplicate error copy in footer. New CSS, if needed, uses existing native modal tokens and #modal.ctms-modern-modal scope, no!important or light recolouring. Header/footer/checkbox targets remain44px on390px phone where applicable.

## Supplied design and actual evidence plan

Read-only pack root:
C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/cloudtms-backend/codex_outputs/claude-stage2-continuation-20260925/SCREEN_DESIGN_POLICY_20261010.

DESIGN_INDEX.md:46 maps SCR32 to root png/specs/T03a_snooze_create, T03b_snooze_amend, T03c_snooze_warning and T03d_snooze_refused. T03a and T03c PNGs were actually opened together during this investigation. T03c's warning paragraph + I understand + invalid-date/footer pattern is the relevant supplied shape; its pictured Draft-in-progress cause and sample money are not this reader's rate-warning facts. Record that exact card-required copy/cause difference. Existing SCR34/T04a owns Unsnooze confirmation; do not redesign it as another rates screen.

Root's ignored existing helper is **.codex-local/WB38-screenshots.cjs**. It reads SCR10/WB25-fixed-read-responses.json, renders actual native Office/shared Snooze, and produces exact context via nativeContext(args):8–13; current warning scenario is PRE_SAVE/date-only:45. Existing current receipts/samples:
- screen-designs/SCR-32/WB38-fixed-responses.json and WB38-{create,expense-create,past-date,warning,conflict}-{dark,device-light,phone-dark}.png.
- screen-designs/SCR-34 WB38 Unsnooze and WB39 confirm captures, plus their small fixed replies.
- Ignored WB39-screenshots.cjs/FC02-screenshots.cjs also reuse this native shared window; no new capture framework is needed.

Root should capture smallest fixed-read-only states in dark/device-light/390phone: current whole Work ACTIVE warning; exact affected segment warning; an unaffected bucket/segment/no mapped decision without warning; existing dated PRE_SAVE warning after local opening acknowledgement; active Amend/Unsnooze with retained opening copy; expense/Case exclusion if rendered path changes. At least one phone view must expose warning, tick and Apply together or use truthful scrolled companion. No financial action needed. Any demonstrated date ACK/result must be clearly local fixed response, with no joined/runtime acceptance.

Final A4 must inspect actual new PNG bytes beside readonly T03a/T03c, preserve old SCR32 review prefix and append WB29 receipt. Full42 policy, item16 manual tools/policy.json wording,25/readerAdditions26 N/A perowner and23/24 actual captures. List every visible difference and every control purpose. No current A4 PASS is given by this investigation.

Source reads and this Markdown write only; no product/sample/helper/pack changes, SQL execution, business data, screenshots, hosted actions, tests or commits.

