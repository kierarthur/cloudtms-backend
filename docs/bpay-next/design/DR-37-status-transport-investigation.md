# DR-37 STATUS, worksheet and receipt transport seams

2026-10-12. **SOURCE INVESTIGATION COMPLETE; integration and actual A4 remain pending.** Root alone edits product. No runtime, business-data, fixture, test, hosted or capture action was performed.

## Immediate required changes

- **T01:** both strict STATUS headers and worksheet rows still have the old closed key sets. New SQL fields currently fail their exact decoders. Mirror the finite additions below in Worker and Office; preserve existing routes, input/cursor contracts and bounds.
- **T02:** STATUS `net_pending === (request != projection)` is obsolete. Expose the real nullable `net_replacement_pending_id` and validate pending as that pointer OR request/projection inequality. Latest `replacement_id` alone cannot distinguish a retained CANCELLED receipt from a certified cancellation still awaiting the original SIMPLE_CANCEL.
- **T03:** CASE_NET job receipt still allows only FINANCE_ALLOCATE/PROJECTED/REVIEW and the old key set. New REFUSED/DONE terminals plus replacement identity/counts must be decoded as exact finite variants.
- **T04:** Case PAYE_NET/CASE_PAYOUT admission replay returns new terminal outcomes and four receipt fields. Current public financial intake 2041:435–445 strips those four fields while forwarding the new phase, which existing Worker/Office enums reject. Narrowly preserve the Case variant in that same public function and mirror its exact receipt. Keep plain no-Case PAYE output unchanged.
- **T05:** use the actual worksheet renderer to distinguish old accepted NET, requested NET, a refused request and cancellation handoff. Do not label a certified handoff “Updating” after its NET job is DONE, or add DR22 Retry/EX27 Cancel actions early.

## Exact source anchors

| Owner | Current seam |
|---|---|
| BE `broker/src/bpay-next-preparation.js` | WORKSHEET_CONTEXT/ROW:58–59; worksheetField:69–87; worksheetResult:100–128; STATUS_HEADER:387–390; CHANNEL_STATUS delegates to STATUS:934–936; STATUS validation:976–983 |
| FE `js/bpay-next-office.js` | WORKSHEET_CONTEXT/ROW:55–56; worksheetField:66–84; worksheetResult:97ff; STATUS_HEADER:317–319; STATUS/CHANNEL_STATUS validation:1207–1210 |
| BE `broker/src/bpay-next-job-step.js` | CASE_NET_OUTPUT:99–100; caseNetPhase/Issue:151–152; exact variant selection:221–224; phase/done/cursor/issue checks:248–254 |
| BE `broker/src/bpay-next-command.js` | PAYE_NET/CASE_PAYOUT outputs:86–87; netPhase:165; commandResult:320ff |
| FE `js/bpay-next-office.js` | existing PAYE_NET RECEIPT:288; net phase enum:693; receipt validation:744ff; same-request journal calls that validator |
| Existing public financial intake | registered parent `11102026_2041_banking_pay_next_case_pause_v1.sql`, Case PAYE_NET branch:418–438 and CASE_PAYOUT:439–445 |
| Native STATUS successor | `.codex-local/DR37-status.sql`:31–36 emitted replacement block,54–57 exact latest request joins |
| Native worksheet successor | `.codex-local/DR37-worksheet-view.sql` frozen/pending/projection_status branches and latest high-water join; `DR37-worksheet-page.sql`:49–53 emitted fields |
| Native Case receipt | `DR37-intake.sql`:48–58 terminal replay and final admission return |
| Native paged result/public dispatch | `DR37-owner.sql`:49–61 legacy/new terminal replay and463–468 final result; `DR37-dispatch.sql`:505–522 native result qualification,574–580 outward key projection |

These are current saved anchors, not permission to edit historical parents. Final repeatable/public successors must preserve each entire original parent apart from the named finite join.

## Finite STATUS/worksheet fields

Add the same exact semantic types to both existing closed read shapes. Use canonical bounded bigint strings, finite enum values, nullable UUIDs/money and explicit NULL absence; do not accept arbitrary objects or infer money.

| Field | Type / meaning |
|---|---|
| `net_request_high_water` | nonnegative bigint string, always present; latest numbering authority for new replacements |
| `net_replacement_pending_id` | nullable UUID; actual blocking Worker pointer, required addition to the currently authored SQL read |
| `replacement_id` | nullable UUID; latest replacement at Worker high-water, retained after terminal completion |
| `replacement_stage` | nullable closed enum: PRECHECK, STAGING, COMMITTING, CANCEL_REQUESTED, CANCELLING, COMPLETE, REFUSED, CANCELLED |
| `requested_net` | nullable money string; genuine request operand, distinct from accepted NET; NULL is legitimate CASE_PAYOUT or absent request, never £0 |
| `replacement_request_no` | nullable positive bigint string; equals high-water for a returned new replacement |
| `replacement_cursor` | nullable canonical positive checkpoint string; genuine original job cursor, not instruction count |
| `replacement_expected_count` | nullable nonnegative bigint string |
| `replacement_staged_count` | nullable nonnegative bigint string |
| `replacement_validated_count` | nullable nonnegative bigint string |
| `replacement_committed_count` | nullable nonnegative bigint string |
| `net_refused_code` | nullable one of CASE_INPUT_UNBOUND, CASE_WEEK_BASIS_UNBOUND, CASE_RETURN_FLOOR_UNBOUND; joined immutable latest refusal |

In STATUS retain `entered_paye_net`, accepted recoveries/additions, cash and projection fields from the accepted pointer. In worksheet retain `accepted_net`, `pending_net`, old financial rows/sorts and actual server projection status. The added `requested_net` is not a replacement for the sorted accepted NET column.

### Read invariants

1. Preserve Run/Worker/Candidate/channel, actor/owner and context equality. New replacement identity must be this Worker's current high-water point; no MAX history join or current Candidate inference.
2. For a non-null replacement all stage/request/count metadata must be present and coherent: committed <= validated <= staged <= expected. For absent replacement the replacement identity/stage/request/count block is NULL. Legacy/no-Case requests may have high-water zero and existing nonzero request/projection revisions; do not require a backfill or apply a global high-water >= legacy projection rule.
3. Pending pointer, if present, must equal the returned current replacement. `net_pending = (pending pointer != NULL OR request != projection)`. Active replacement stages do not imply accepted cash. PRECHECK before first claim may legitimately have NULL job cursor.
4. REFUSED precommit replacement has zero committed rows, a genuine immutable closed refusal and restored request/projection equality. The **current READY cache** requires Worker READY, pending NULL and high-water greater than accepted projection. A retained refusal may later coexist with a cancelled Worker; do not force every historical latest refusal fact back to READY.
5. Certified CANCELLED replacement is the original NET job's terminal cleanup/handoff, not payment cancellation. Its pending pointer can remain non-null with request/projection equality until ordered SIMPLE_CANCEL consumes the exact certificate. It must continue blocking execution/new intake.
6. A COMPLETE replacement has equal expected/staged/validated/committed counts. Its original immutable receipt can be replayed after later requests or retirement; do not require an older terminal command's request number to equal the currently read Worker high-water.
7. Existing absent-projection nullable-money checks, genuine zero semantics, payout-only distinction, reservations, page ordering, response/cursor byte ceilings and stale-context handling remain. A refusal preserves previous accepted financial facts; no accepted NET remains “PAYE net not entered”, rather than fabricated accepted zero.
8. Existing totals_revision invalidation already covers replacement checkpoints through the current-only revision trigger. Preserve worksheet context/cursor exact equality; stage/count changes must not be appended to a previously retained context as if unchanged.

## CASE_NET and command receipt variants

The existing job route remains PAYE_NET_ENTRY, expected_phase FINANCE_ALLOCATE and its original integer continuation. PRECHECK/STAGING/COMMITTING/cancellation are **replacement stages**, not new queue phases or job kinds.

CASE_NET adds seven keys: `replacement_id` (UUID?), `replacement_stage` (closed enum?), `request_no` (positive bigint?), `staged_count`, `validated_count`, `committed_count`, `expected_count` (nonnegative bigint?). All are required keys in the new outward closed schema; the complete NULL extension is only for exact retained legacy terminal PROJECTED/REVIEW replay, because current public dispatch always projects these keys and legacy immutable records were not fabricated.

- FINANCE_ALLOCATE: done false, cursor non-null, new replacement block complete, issue NULL, projection NULL; original staged counts are not allocated/committed money.
- PROJECTED: done true, cursor NULL, projection UUID; new block stage COMPLETE and all four counts equal, or exact legacy terminal extension NULL.
- REFUSED: done true, cursor NULL, projection NULL, stage REFUSED, issue one of the three codes, committed zero. This completes the original NET job while Worker stays READY.
- DONE: done true, cursor NULL, stage CANCELLED, issue NULL. It certifies NET cleanup; it does not certify Worker/payment CANCELLED or clear the pending pointer.
- REVIEW remains solely the retained legacy terminal shape; do not emit it for new DR37 business refusal.
- Preserve rows_visited <= call.limit, replay implies zero rows visited, exact job/Worker identity and existing common/lease fields. Do not accept partial extension, arbitrary stages or oversized encoded payloads.

For the existing command route, Case PAYE_NET and CASE_PAYOUT add `replacement_id`, `replacement_stage`, `projection_id`, `issue_code`. Phase ACCEPTED_PENDING_PROJECTION continues admission/running replay; terminal replay is PROJECTED/REFUSED/CANCELLED with exact stage/amount identity. The private helper at intake:54–58 already returns that identity. The existing public constructor must preserve it only for this Case variant. Worker OUTPUTS and Office's existing PAYE_NET receipt validator/journal must accept that exact variant and compare original command/Worker/request identity. Plain no-Case admission keeps its original six-key receipt; exact historical Case replay may retain its old shape, explicitly distinguished rather than widening every output.

Office does not currently expose CASE_PAYOUT as a new UI action; do not add one merely to mirror the server enum. No new public route/command or journal framework is needed. Unknown replay returns the original command's immutable outcome; a later current STATUS read must not replace it with a different request's outcome.

## Actual worksheet/render hook and future ownership

Native `mountPayeWorksheet` uses eight server-sorted columns at Office:2212. State text/tone are at2221/2274; PAYE net cell at2274 already keeps `accepted_net` as its main amount and renders `pending_net` underneath (“pending” or “not saved”). Extend those existing nodes with the truthful stage/refusal copy. Do not overwrite main accepted amount with requested NET, sum deductions locally, duplicate a one-row parent, or create a second worksheet.

Use the same existing status cell for “Updating…” during PRECHECK/STAGING/COMMITTING; use explicit cancellation/handoff copy for CANCEL_REQUESTED/CANCELLING/CANCELLED with pending pointer. A refused request needs the closed safe reason and a separate requested/not-saved value, preserving accepted state or missing-NET truth. Display only factual server checkpoints/counts; no invented percent or successful allocation count from staged rows.

Existing generic accepted-worker view at Office:3022 and poll decision:3059 also use `net_pending`; they must preserve accepted facts and stop calling a still-blocking certified handoff “current”. Its Build original transfer button already checks pending at3027; updating the truthful decoder protects that guard without adding capability. Generic table “Review required” correction is tracked as header H02.

DR22 owns deliberate Retry and EX27 owns whole-Candidate Cancel. This investigation supplies no early buttons, cancel intent or new financial action. Actual SCR58/SCR60/worksheet screenshots, dark/device-light/phone, pending-before-claim, partial commit, READY refusal with/without prior NET, successful terminal and certified handoff are A4 evidence to obtain only when the actual owning host can render. No screen approval is claimed here.

## Source evidence

| File read | SHA-256 at read |
|---|---|
| BE preparation.js | 5566476528F95AF3A0F28634D9AB1C7A4C19D73E6FCC4F464DDBD363D79FB22B |
| BE job-step.js | 01374C96E62F6E78242F53B75A4EF13B8A624024A1763F5B10B063D1A71C8FAA |
| BE command.js | 5E9126530F6E4D3E28F49A3BF2C40096C40B83E6F0D43195EC312EBEED2DF85F |
| Authored DR37-status.sql | DDB30AA3F437FE67F966D1AC1A99B10B42D02B39EA68D4AADB366A081DD03E15 |
| Authored DR37-worksheet-view.sql | 5737BDE75D66673091E260B873C1864DD168934C0509CF3DA53686AC49DA4ED8 |
| Authored DR37-worksheet-page.sql | B0EF63955B0AA5747D4F59E43E31AD382002E8BF76AFC49B466DA74A95502F94 |

Exact final-source review is required after root integrates these deltas. These hashes identify inspected bytes, not installed or joined acceptance.
