# WB-16 detail UI draft source review

**Draft review: findings remain; no implementation/A4 verdict.** Reviewed BE `.codex-local/WB16-detail-ui.txt` SHA256 `a895968a89970cc7dbdde94699dbbba7a5362d05d967ecc2ff1b43dbfca2c477`, full WB16 card02:655–700 and exact D02/D03/D07 rows, SCR10 index/T02/T02c/T02e context, current native component/rate/segment contracts. Only this requested review document was written. No product/draft edit, tests, SQL, runtime, browser, fixtures, hosted request or pack generation.

## Concrete remaining findings

### H01 — exact Case lookup stops at the first component page

`readCase` reads CASE_COMPONENTS with after_component_ordinal=null/limit100 once and then finds the visible native component ID. Native0300 has unique `(case_id,component_ordinal)` but no100-component Case cardinality cap. A legitimate later component is falsely treated as an invalid response. Keep each native request bounded and use its actual continuation until the exactID/end, or transport the genuine native ordinal already attached to the displayed component and request its precise predecessor+limit1. Do not pick the first/latest component or infer ordinal from the visible row position. Bound lifecycle/work per continuation, rather than issuing an unbounded recursive fetch on open.

### H02 — aggregate detail Amount is not an attributed current payable share

The aggregate branch says it has no per-shift payable amount but renders `s.segment_pay_ex_vat` in the same Amount column as the exact current selected/included amount. That is original approved detail money; after realised/held/snoozed changes it need not sum to the maintained day remainder. Retain its approved-evidence meaning explicitly or show unknown for payable share. Do not imply its sum equals current group payable, divide/revalue it or manufacture a segment entitlement. Stable approved-segment rows can continue displaying exact member amount from the maintained projection.

### H03 — rate facts use a two-decimal money formatter

`cash` delegates to current FE `displayPounds`, which explicitly rounds presentation to pence. It is used for original unit/source pay/approved charge and available PAYE/umbrella/charge rates, whose native contract is numeric18,6. E.g. actual20.123456 is displayed20.12 as if the original rate. Use a finite decimal-text rate presenter that retains genuine supplied precision and keeps null unknown; continue two-decimal money formatting for actual currency amounts. Never derive a new amount from the displayed rate. Suggested/margin cells correctly remain unavailable until genuine future owner evidence exists.

### H04 — stale detail retry cannot refresh its retained context

`check` pins every GROUP_LINES response to `address.context`. On reloadRequired, evidence/main/Pay-lines paths show “Payments changed; refresh to continue” but the offered Try again resends the same address/context, or deletes evidence then enqueues the same stale member. There is no finite refresh callback in this draft. Route that state to the existing Candidate's explicit same-identity Refresh/adoption path (retaining ticks and parent frame), or a named Refresh action that closes/reopens only this detail under a newly validated context. Do not silently adopt new financial facts or repeatedly label stale reads ordinary network retries. Generic transient reads may retain exact request retry.

### H05 — genuine correction type needs distinct factual wording

The draft renders only `is_adjustment===true` → “Adjustment”, even when the exact physical carrier supplies a genuine correction kind/original reference. Card RQ020/RQ021 requires Correction for a real correction carrier. Transport/render only a real typed current physical relation, as the detail-support note specifies. Do not relabel every adjustment Correction, derive it from revision/sign, or create selectable retired import reversal/replacement children. Absent eligible correction proof remains an open later factual join, not a invented badge.

## Root-declared fixes underway at this review boundary

The parent already identified these; this note records their current draft manifestation and does not claim they are fresh discoveries:

- **Case evidence key pruning:** render's retained native set uses evidenceKey(Case) but renderCase stores `CASE:<componentID>`; later paint deletes valid Case evidence, provoking repeated reads. Use the same native key for both retention and lookup.
- **Backward bounded window:** main state retains previous cursors but no top-scroll restoration exists; Breaks/Pay-lines evict prior pages without restoring them. Keep bounded DOM and genuine upward continuation; do not solve by accumulating unlimited pages.
- **Aggregate forward continuation:** moreShifts replaces the current detail rows and adds no break reads; the separate “Scroll for more shifts” div has no actual guaranteed scroll range. Keep bounded native page window + recoverable cursor history, actual scroll trigger and genuine BREAKS support for each newly adopted shift. Do not lose or duplicate earlier evidence.

## Correct scope and native reuse in the draft

- Reads actual GROUP_LINES per maintained section and validates retained Workbench context; every factual reader is followed by a point GROUP_LINES current check. SHIFTS target uses genuine approved_segment_id, segment_key, shift_detail_id and actual detail cursor, not same-date or ordinal identity alone.
- AVAILABLE/RESERVED portions are grouped under native member identity rather than duplicate shifts. Day presentation groups by Work/revision/approved-line/component, not date across unrelated parents. Native component totals are consumed rather than recomputed in the browser.
- COMPONENTS/SHIFTS/BREAKS/RATES are the real pre-Draft transport; no run_id/frozen route fallback, command, quote, preview or dormant ACCEPTED_* call. Case reads use actual Candidate/Case scope.
- Local textContent/DOM construction avoids raw HTML. Native IDs are retained; submitted/supplied clock evidence remains separate; duration-only break can stay without invented endpoints. Current actor/frame/owner guards and read tickets stop post-close painting; final integration must preserve those guards around the actual standard modal adapter.
- Pay lines uses fixed DAY/NIGHT/SAT/SUN/BH business order; available schedule rows stay separate from actual used rate rows. Standard schedule is fixed<=5, not an arbitrary entire revision snapshot. Source unknown used/approved charge remains unknown.
- Stored weekly/floor/minimum terms are shown as stored cap facts. `Show estimate` is plain unavailable text, not a fake calculator/button; recoverable remains future WB27. No insufficient-pay inference is made on opening.
- Actual later owners remain WB25 ticks, WB38 snooze actions, WB27 estimate and WB29/45 persisted resolution/suggested rates. Their missing joins must remain explicit; do not tick complete whole Done journey from this bounded draft.

## Design/acceptance still pending

The draft does not yet prove actual Candidate expansion mounting/Pay-lines opener, standard window stack/focus, phone card layout, no parent reload, five-day summed native amounts or SCR10's actual pixels. No screenshots were supplied at this task boundary. A4 actual product screenshots and all42 checks remain pending. T02c illustrative Original/New and Correction figures must appear only when their real authority exists; no fixture or preview values may be added to product to imitate the design.

The current draft's scope is acceptable with these finite corrections. Re-read its new hash and actual installed reader/mirror schemas before final source review; 2019/2026 are evolving current WB16 successors, so a reference to0585/2018 is predecessor provenance, not a promise that those remain the final definitions.
