# FC-04 sequence104 — current screen investigation

11 October 2026. Independent read-only investigation before product build; no A4 approval. Full FC-04 card, exact current CSV rows, FC-03 boundary/source, native legacy entry and supplied B9 SCR-117/SCR-116 PNGs/specs inspected. Product, pack and checklist are unchanged. No rendering, tests, data, database, hosted or business actions were run.

## Exact scope

FC-04-D06 is the sole revised build deliverable: the New Payment Advance window and its actual NEXT entry/integration. D01/D02 are retained original creation/apply function context; D03 ADVANCE, D04 PAYMENT_ADVANCE, D05 CASE_CREATE and D07 reviewed card are context, not extra SQL/Worker work. Dependencies are FC-03, WB-08 and F-16. Card says explicitly “DB: none beyond FC-03” and “Worker: none beyond FC-03”.

Create only ADVANCE / PAYMENT_ADVANCE / NOT_APPLICABLE. Do not offer Loan, choose another tax treatment, create a new command/kind/reader, add bank verification or rewrite money owners. Candidate's current channel is server authority; never make a blank channel PAYE by default. Approved principal is not funded debt: an accepted request or new unfunded Advance is not a completed payout. Existing original order, immutable request/token, actor/NEXT qualification, exact replay, Policy X and protected Draft inputs remain intact.

FC-02's full Payment Advances / Snoozes screen is sequence117, after this item104. The supplied background Cases page is therefore later context, not authority to build its filters/table/snoozes/export now. Build the named window and reachable current F-16/native entry; retain a callable integration for FC-02 and seeded Candidate use. Do not count a legacy financial modal/endpoint or future full Cases page as this item's NEXT surface.

## Required form and mapping

| User field / state | Exact existing CASE input or reader fact |
| --- | --- |
| Candidate picker; pre-filled when invoked with a Candidate | candidate_id; human name/reference from the actual native selection/caller. Changing Candidate requires a separately scoped controller. |
| Read-only Pay channel | Actual Candidate/caller fact, then preview_token.context.pay_channel; no editable channel or default inference. |
| Amount / principal | principal: exact positive decimal string, maximum two pence; no floating-point coercion or silent sub-penny rounding. |
| Repay radio, default By number of weeks | Submit week_count with weekly_due=null, or weekly_due with week_count=null. Do not submit a browser-derived second term. |
| Number of weeks / Weekly amount | Only the selected positive term is entered; the other figure appears from CASE_PREVIEW. |
| Start week | start_monday, actual next Monday by UK calendar; display DD/MM/YYYY / Mon DD/MM/YYYY, validated as a real Monday. |
| Affordability, open by default | minimum_earnings_threshold: blank null means none, explicit0 allowed; take_home_floor_override: blank null means Candidate default, explicit0 is an override. |
| Required Reason / comment | reason: current FC-03 nonblank creation approval comment, <=2048 UTF-8 bytes. This field is required even though the supplied image omits it; optional Note must not replace it. |
| Optional Note | note: nullable, <=2048 UTF-8 bytes; original_liability_reference=null for this family. |
| Unused date/type operands | due_date=null; fixed kind/subtype/tax above. Do not invent another date or funding status. |
| Preview | weekly_due, week_count, first_due_monday, last_due_monday, total, threshold, override/effective floor, warnings and exact preview_token. |
| Apply result / exact refresh | Original accepted Case receipt, then CASE_COMPONENTS for only that candidate_id/case_id; actual empty read is still being added, not a completed Case/payout. |

Every input change invalidates the earlier preview and disables Apply. Show an explicit disabled reason. Preview/Apply retain their shared same-input token gate, one primary and native header Close. Only the actual derived terms are displayed; money remains decimal-string formatted, missing values stay “—”. Start/first/last due dates are repayment dates, not a pre-execution pay date.

## Current seams and endpoints

- FE js/bpay-next-case-preview.js owns the closed input, preview/token mirror and mount(). It provides exact changed-input invalidation, Preview, Apply, retained Try again and accepted exact-Case Refresh states. Reuse it rather than copy financial calculation or create another journal.
- FE js/bpay-next-office.js:1934 mountCasePreviewFooter captures one Candidate, creates the existing controller with .case-create.<Candidate> journal qualifier, and calls the actual transport. Its preview is CASE_PREVIEW; submit is CASE_CREATE with one native command_id and the exact preview_token; readCase is CASE_COMPONENTS with that candidate_id/case_id and limit100. Its successful onApplied callback finishes the creation journal only after the owning host's exact Case update.
- Actual read endpoint: GET /api/banking/pay/next/preparation-page?kind=CASE_PREVIEW&input=<JSON>. It is read-only, no command or wake.
- Actual Apply endpoint: POST /api/banking/pay/next/command with action CASE_CREATE and the existing exact input, command_id and preview_token. Existing CASE_CREATE alone owns its ENROLL wake.
- Exact row refresh uses the same preparation-page route with kind=CASE_COMPONENTS, candidate_id, case_id, after_component_ordinal=null, limit=100. No legacy refreshPayWorkbench or whole Banking Pay reload.
- Native legacy source is main.js:12979 openBankingPaymentAdvanceModal, main.js:11802 New Payment Advance action, and main.js:100489 /102632 create dispatch. Existing picker is openCandidatePicker at main.js:288127; the legacy form uses its actual picked id/label/channel (main.js:13580). Reuse that native picker capability rather than introduce a new Candidate search service.
- The legacy modal also handles edits, old workbench sessions, stored resolutions and one-off bank details. Those are not the FC-04 NEXT form. Route qualified NEXT creation before those reads/legacy POST; do not maintain the old operational engine or copy its broad refresh.
- No direct Candidate-record create caller was established by the inspected openBankingPaymentAdvanceModal call sites; they are finance create/edit dispatch. Preserve pre-filled support and qualify any real Candidate caller before claiming that path tested.

Candidate changes before dispatch must invalidate/destroy the old preview/controller and remount under the new Candidate identity, with a current actor/modal/Candidate presentation guard for late replies. During retained unknown or accepted-awaiting-read states, prevent fresh terms/Candidate choices from replacing the original operation; retain exact bytes and expose only recovery. Closing preserves uncertainty, never silently retries or reports success.

## Concrete integration gaps to address

1. The old form defaults start_week_start to today (main.js:13356), not the next Monday. FC-04 needs an actual next Monday default; do not copy that default or derive a fake ISO date from displayed text. A Wednesday must show “Start week must be a Monday” and preserve entered values.
2. FC-03 shared Preview currently catches its errors generically. FC-04 must present the required source-channel-unbound instruction (“Set a pay channel on the Candidate, then preview again.”) for the native BPAY_NEXT_CASE_CREATE_SOURCE_CHANNEL_UNBOUND refusal, including stale caller information. This is a finite UI refusal mapping, not a server feature or fabricated channel.
3. The supplied picture omits the required creation Reason. Add the actual staff-entered field and record the card-required visible difference; do not supply an automatic generic approval comment.
4. Preview's warnings only include EXISTING_REPAYMENTS. Use the already implemented “This Candidate already has repayments.”; the illustrated named £150 weekly total is unavailable and must not be fabricated.
5. Current closed Preview has no per-week instalment array or last-instalment amount. Original terms permit a smaller final repayment. Do not calculate that amount in the browser or imply every week equals weekly_due when it does not. Keep the native weekly term, count, total and first/last dates; record the unavailable exact final-instalment fact under owner A2 and use truthful qualifying copy if needed. No new server field/reader is authorised by D06.
6. Shared pending/failed/busy presentation needs actual native frame verification: appropriate alert/status roles, plain visible disabled reason, no empty paragraphs removed by the moderniser, reachable phone footer, and money-format marker where the native table adapter would otherwise strip formatting. These are required integration checks, not current screenshot findings.
7. Accepted receipt phase is ACCEPTED_PENDING_CASE. Do not close with “Payment made”, show the principal as funded outstanding debt, or label a still-empty exact Case read complete. Preserve the shared explicit Refresh recovery on queued or failed read and update only the actual Case after it exists.
8. No one-off bank input, account-change creation block, funding button, payout/settlement action or future CM-13a logic belongs here. PAYE and Umbrella both create; Umbrella payout/repayments remain later CM-13a scope.

## Supplied design and planned A4 evidence

Read-only DESIGN_INDEX.md:132 maps FC-04 → SCR-117 New Payment Advance with SCR-116 shared Preview/Apply, both B9. SCR-117 is a small600 window: Candidate/channel, Amount, Repay radios, chosen term, Monday start, open affordability, Note, unknown weekly/last values, Preview and disabled Apply. SCR-116 substitutes the actual preview card and enables Apply. One tab-free child, header Close, ordinary form groups and one preview card are sufficient. No new full-page navigation, extra control system or nested form-card containers.

Each difference must be listed and either fixed or justified by the current card/owner facts: required Reason; unavailable illustrated existing-repayment £150; unavailable exact final-instalment amount; actual native frame/background; actual current Candidate/default/date; later FC-02 background; queued/unknown/failure truth. Supplied numeric/date/person examples are examples, not defaults.

Minimum distinct local actual-render states to cover after implementation, each dark/device-light/390px phone:

- unselected Candidate and pre-filled Candidate; default weeks mode, next Monday, open affordability and Preview-first reason;
- valid weeks Preview (£300/6 → actual£50.00 and first/last dates), and weekly-amount mode with only that input;
- Wednesday refusal; invalid principal/precision or weeks; threshold blank versus0 and floor default versus explicit override;
- existing-repayment warning; missing channel with explicit Candidate next step; genuine Umbrella create without bank-detail gate;
- changed input after Preview; checking Preview; failed/stale Preview preserving input; actor/modal invalidation inspected source-only unless actual captured;
- Saving, retained unknown/Try again, accepted-awaiting-Case/Refresh, failed exact read/Refresh, and actual confirmed unfunded Case fact;
- phone body scrolled to lower fields/preview and reachable footer, with every interactive target44px and no horizontal overflow.

Use the smallest fixed local replies/retained local states for actual existing native UI; no business command, database row or invented future host. Source review and actual screenshot A4 are separate. Full42 policy rows apply; wording lists reviewed manually (item16), pack generation/copy item25 and readerAdditions alternative26 N/A per owner. Record every control's purpose, every visible difference, UK formats, radio/field keyboard source, alert/status, genuine null/zero distinction, identical device-light dark appearance, phone geometry and final image/source hashes. No A4 PASS is claimed in this investigation; joined creation/idempotence/funding/queue journeys remain AUTHORED_NOT_RUN.

## Source checkpoint SHA-256

| Saved source | SHA-256 |
| --- | --- |
| BE IMPLEMENTATION_CHECKLIST.csv | debc7704fdbce709c2d2eb1aa1d2b53120b29ba94b11465d1569fc70e9d03119 |
| BE11102026_1536 case_create_intake repeatable | 853ddce45455defed9f715a2593267794f3b224e469a9eaccde2aeca6e44a470 |
| FE js/bpay-next-case-preview.js | d7551956a993482a33264f39789cf2ce4d83342475dbabb53d7fff54ef4788e8 |
| FE js/bpay-next-office.js | cbdb844f11509a334cab8e87fc0cdc247b6e73ebe3f741ac8cb5be789e700745 |
| FE js/main.js | 35073a4ed6ca21d8dbc7bdf40d6970f175656f258567282a57eebbd106bfc243 |
| B9 SCR-117 spec | 93bb975ff00c1a623b80098072d1c4c12f90646b16fd22ec0187a5377b13c446 |
| B9 SCR-117 PNG | 2fb527d77399c503b5bd9bbfce11b3155cc230f7d586dbb995beec9a5630b210 |
| B9 SCR-116 spec | 59bed32905ab710856e0df68b04c7875451425287fadfa5d12efaa3bf1622182 |
| B9 SCR-116 PNG | 866940964ca608c09feebdc57138ae51c015781e17c78ab9c1ac76bfe47d297f |

Source snapshot is investigation evidence only. Root owns any later implementation, tests, captures and commits.
