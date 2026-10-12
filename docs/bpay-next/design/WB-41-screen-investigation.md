# WB-41 screen investigation — before product changes

12 October 2026. Read-only investigation of sequence 118 / BD131, the full WB-41 card (`02_WORKBENCH.md:1465–1504`), all ten exact checklist deliverables, current build note, applicable workspace rules, current native Workbench source, supplied design index/specs/PNGs and the policy checklist. D09 owns the screen integration. This is an investigation, **not A4 approval**; no WB-41 product captures have been reviewed yet. No product, pack, database, capture or test action was performed.

## Smallest required changes

1. Add `ORIGIN_UNAUTHORISED_WAIT` to `estimateCapText` (`js/bpay-next-workbench.js:1381`). It currently becomes “Amount limited”. Reuse the card's “Waiting for re-authorisation — recovery paused”; do not expose the code, infer the state from a pay query, or alter money in JavaScript. Both the parent amount and useful Case detail already call this one mapper.
2. Preserve the Action required row's factual outstanding amount before an estimate. The supplied T04b explicitly shows the affected overpayment at **£60.00**, alongside the paused-recovery reason. Current Candidate line1504 routes every unaccepted deduction Case through `CASE_COMPONENTS.amount_due`, including ACTION. Verify the new WAIT row against that branch: the card requires its £60 outstanding to remain £60, not become an unknown/zero due amount. If the fields differ, the narrow correction is an exact ACTION / `ORIGIN_UNAUTHORISED_WAIT` presentation branch using the existing reader's `group_amount` outstanding fact, with a clear Outstanding caption. This is a source-supported presentation risk, not a proved current pixel failure. Keep ordinary Ready deductions' factual Due presentation.
3. After explicit Show estimate, display only the native `recoverable_this_run` and `cap_reason`. The paused recovery then legitimately shows **£0.00 · This Draft**, with the plain paused reason; this is an actual zero estimate, not a failed-read fallback. The unrelated Advance retains its own actual estimate. The information-only WAIT component remains untickable and never enters Create selection/counts through the browser. Unknown and pending evidence remain a dash/plain state. No new amount, recovery formula, channel, reader or action is needed for this screen.

The new bounded information-only membership and fresh native WAIT qualification in BD131 are the server owner's work. The browser already keys estimates by `case_component_id`; it must consume the existing finite result rather than recompute WAIT or collect Action required Cases. Accepted In Draft rows retain the original accepted presentation; a later live withdrawal is not used to rewrite their captured terms.

## Owner 11 October corrections already present — retain and inspect

Current source already implements the required pattern, so WB-41 does not need a replacement Candidate screen:

| Existing seam | Necessary behaviour |
|---|---|
| `hasDetail`, line1397; payment name caret, line1503 | Caret only for actual Timesheet detail, Overpayment recovery detail, or a Case with more than one component; no Expand text. Stand-alone Manual Credit/one-line Case has no redundant parent/child row. |
| `renderCase`, lines1302–1318 | A one-component overpayment opens useful Original/Outstanding facts without repeating the parent. Multiple components use one native table, with factual Due and native This Draft amounts. Payment Advance avoids an irrelevant full-balance column and retains its factual Weekly limit. |
| `mountPaymentDetail`, lines1233/1281 | A complete single shift is one combined row; multiple-shift detail remains genuinely useful. Do not collapse an incomplete bounded page as if it were complete. |
| Candidate footer, lines1460–1465 | Include-all is a native choice; Export CSV is secondary; Show estimate is the one rightmost primary. The exact candidate-export exception is already in the native modernizer, with scoped secondary CSS. |
| Row actions, lines1507–1509 | Open Timesheet plus More stays inline. Phone uses Open as required by owner A3j. More contains only existing applicable Pay lines/Snooze/Amend/Unsnooze controls; do not add future Case/Source capabilities. |
| Estimate state, line1486 and amount line1504 | Updating, Outdated and Pending are truthful; tick changes do not recalculate money. Retained outdated figures remain visibly outdated. |

The existing main Action required renderer already maps the card reason at line687. Its no-tick Action row must remain unselectable and retain the outstanding amount supplied by that reader. A query alone must not display the WB-41 pause explanation. All current actor/epoch/context, selection-memory, component revision, native modal lifetime and child-return guards remain in force.

## Read-only supplied references examined

The authoritative pack remains at `cloudtms-backend/codex_outputs/claude-stage2-continuation-20260925/SCREEN_DESIGN_POLICY_20261010`; nothing was copied or edited.

| Screen | Exact supplied reference and relevance |
|---|---|
| SCR-05 | `png/T04b_action_required_dark.png` / `specs/T04b_action_required.json`: WAIT overpayment reason, £60 outstanding, Open Timesheet; retained failed-read facts; secondary Export and primary Create Draft. |
| SCR-09 | `png/SCR-09b_candidate_action_required_dark.png` / matching spec: Candidate Action required reason/amount/actions; `T02_candidate_window_dark.png` and `T02_candidate_window_devicelight.png` / matching spec: Candidate frame, carets, selection and footer. Both PNGs were opened; the supplied device-light image itself remains dark. |
| SCR-10 | The same T02 shows complete single-shift versus multiple-shift rows, expenses and only applicable Snooze controls. This shared detail is touched only if a necessary WB-41 presentation correction requires it. |
| SCR-11 | The same T02 shows Payment Advance weekly limit, recovery Original/Outstanding and paused repayment. The latest owner corrections govern Due/This Draft and useful detail instead of reproducing any redundant design row. |
| SCR-12 | `png/T02b_candidate_estimate_dark.png` / matching spec: explicit estimate, actual component caps, secondary Export and Show estimate; `T02d_candidate_estimate_outdated_dark.png` / matching spec is the retained estimate state reference. |
| Phone pattern | `png/T06_phone_ready_phone_dark.png` / matching spec: native cards, no page-wide horizontal scroll, Sort by, 44px targets and reachable footer. |

T04b's £60 WAIT row and T02/T02b were individually viewed alongside the source investigation. Illustrated people, counts, totals and dates are examples, not permission to fabricate reader values. The current estimate reader supplies gross earnings ex VAT, gross deductions, net additions and net deductions; the design's estimated net pay/cash and shown-at timestamp are not invented where absent. No future DR13/DR15 review composition or Source financial withdrawal flow is built here. Source paid-withdrawal end-to-end remains BLOCKED_ON_CM02/SP04 / NOT_RUN.

## Required actual local capture evidence after implementation

Use the existing actual native Office/Workbench and smallest fixed local read responses, with hosted traffic blocked, in desktop dark, device-light and 390px phone. The finite evidence should show:

- Main Action required: affected automatic recovery with unchanged outstanding and exact paused wording, unrelated payment present, no selectable WAIT tick.
- Candidate Action required before estimate and after explicit estimate: factual outstanding first, then the native zero This Draft plus reason; the unrelated Advance remains unchanged.
- Ready/reauthorised counterpart and query-only counterpart: native readiness/recovery truth without the WAIT explanation where it does not apply.
- Useful overpayment detail and the touched shared Candidate Timesheet/stand-alone controls: no duplicate one-line child, correct caret/actions, actual Due/Weekly limit and no fictitious money.
- Outdated/pending or failed estimate/read: retained grey/amber facts or unknown dash, visible recovery action, never a zero caused by failure. Include accepted In Draft preservation if the shared branch changes.
- Phone bottom/scrolled or More-open evidence wherever the actual first viewport does not reveal the final controls; do not substitute a capture of an offscreen target.

These are fixed-response screen states, not a joined withdrawal/reauthorisation, preparation, Source or business acceptance test. Do not claim hundreds-row/joined/runtime coverage without performing it under separate authority.

Final A4 must open every real PNG next to its supplied reference, record **every visible difference and every control's payroll purpose**, and give all 42 policy results. Item16 is checked manually against `tools/policy.json` never-show/chatter/format lists (20-word sentence limit), with the owner's stricter plain one-sentence error requirement. Item25 is N/A under the owner instruction to use supplied specs and real product captures; item26 must map real values to native fields, with only its readerAdditions alternative N/A. Items23/24 require actual phone/device-light pixels. Current source patterns are not pixel approval.

## Investigation provenance

Saved disk bytes at investigation time; root may subsequently change product. These hashes do not approve a later implementation:

| File | SHA-256 |
|---|---|
| FE `js/bpay-next-workbench.js` | `D3669BC4EB4E7EE68CBBD7D19699C40D0616790E39670528EF4F900C53F72DF3` |
| FE `js/modal-modernisation.js` | `0CDB7F981C039AFD8B86C14124D728CD1AD824CEFD18CB294A313393112F642B` |
| FE `css/bpay-next-office.css` | `A2DBEE744D9A8A00A285435A5B574DCEFA2116379729DCD38610D7B67149616C` |
| FE `index.html` | `EFC728323B863B5CBE48BF463AEF9A0FD7A95C037B1BF15D852ADA873855393D` |
| BE `design/WB-41-build.md` | `DA412E827AC4EADDA7FC782179B27086E1C8B5C2FFAFB70ABAD66FBF59C77B7E` |

**Result:** ready for the root's narrow implementation, subject to the Action required outstanding/Due check and actual A4 evidence. No A4 PASS, database/runtime/test or whole-item completion is claimed.
