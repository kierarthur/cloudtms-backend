# SP-15 Office and Source command source review

**SOURCE PASS — scoped SP-15 D03/D04/D05, 11 October 2026.** No actionable source defect remains in the inspected changes. This is static saved-source evidence, not database, Source command, payment, hosted or joined browser acceptance. The actual Timesheet child journey remains AUTHORED / NOT_RUN; local captures assert only qualified callback arguments.

## Inspected boundary

Read the full SP-15 card (05_SOURCE_AND_PROTECTED_PAY.md:515–542), exact implementation rows, current investigation and owner E2/A2 boundaries. Reviewed the actual Office/main diff, original reader schemas/validation, native physical opener, and existing Source workspace/Worker command paths. No product, fixture, render tool, business row, hosted request or pack file was changed by this reviewer.

| Check | Result and evidence |
| --- | --- |
| Closed code ownership | PASS. Office:1491–1495 freezes exactly SOURCE_PAY_QUERY_OPEN and SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE with the card's plain messages. Other issues retain the existing generic preparation message. Codes/UUIDs are not displayed. |
| Existing readers only | PASS. openReviewTimesheet at Office:1527 uses STATUS limit 1, then existing CURRENT_WORK Candidate pages limit 100 and their monotonic cursor until the exact Work matches. The existing transport verifies STATUS run/worker identity and CURRENT_WORK Candidate identity; there is no first-row fallback or new reader. |
| Fresh issue association | PASS. The first STATUS must belong to the row's Candidate, be REVIEW and supply an owned code plus non-null Work. Fresh STATUS after lookup must retain original run/worker/Candidate, REVIEW state, preparation revision, code and exact Work. Missing/stale/refused reads do not call the opener. The local missing/changed samples have empty qualifiedOpenerArguments. |
| Actor/owner/pending guard | PASS. current() checks original actor, current NEXT owner qualification and accepted-run namespace. It is rechecked after reads; pending journal/worker action blocks navigation. Original actor identity is retained in the frozen callback context. |
| Native physical opener | PASS. main:7179–7228 adds a separate reviewTimesheet callback and extracts only the existing shared navigation lifecycle. The original pre-Create timesheet callback still rejects accepted_run_id. Review requires finite valid run/worker/Work/Candidate/physical UUIDs and one of the two owned codes. Parent must be the current native bpay-next-read frame. Physical ID is CURRENT_WORK.original_timesheet_id, never Work ID; optional real reference is retained and unavailable version is null. The native exact-ID/version guard remains intact. |
| Parent/child ownership | PASS by source. Existing native child/frame checks, temporary evidence chrome, dismissal restoration and actor/NEXT/live-parent recheck remain. No legacy fallback, approval, Source resolution, recovery pause, recapture or financial command is added by this link. Actual child opening/closing is NOT_RUN. |
| E2 release boundary | PASS. UI does not assign worker state. Only already supplied compatible REVIEW rows get the closed reason/link; CANCELLED renders Not in this Draft without query retry/navigation actions. The compatible fixed REVIEW samples do not prove a pure query becomes REVIEW. Query auto-release and its genuine business outcome are reviewed by the separate SQL/source owner, not claimed here. DR15 later owns the complete exclusion/reason composition. |
| Keyboard/feedback | PASS by source/pixels. The quiet row button and row Enter/double-click share the same guarded opener; button double-click is excluded from row dispatch. Current generic safe error is one sentence with Refresh, role alert, before tools/table. Loading/results use role status; continuity and original native dialog ownership remain. |
| D03 existing Worker commands | PASS by source. routes.js:24–25 retains original OPEN/RESOLVE RPC names. handleCommand:569–575 preserves payload fields including command_id and injects authenticated actor; the existing direct-RPC/refusal-recording path at 664–673 is retained. No new Source route/financial payload/command or invoice/payment mutation is authored by this UI change. |
| D05 same-body retries | PASS by source. workspace-actions:1068 creates OPEN's source_row_id/trimmed Reason/command_id once in state.pending; retries send that same object, and pending Reason is locked. openCommand:922–951 creates RESOLVE's UUID once outside its retry handler and reuses the same cloned payload. Original actor/frame guards remain. |
| D05 remedies/hide | PASS by source. Existing RESOLVE authority admits OFFICE_ACCEPTED_SOURCE or PROTECTED_PAY with the original evidence requirement. Upload detail:1014 requires Source row identity and hides Send to Pay Queries when pay_query_open is true. No synthetic resolution or amended approved pay/invoice is introduced. Genuine relevant-query, remedy/replay/finalisation behavior is NOT_RUN. |

## Limits and receipt link

The two Source messages and quiet Timesheet action are approved in the current native Review host. Current WORKERS/STATUS do not provide human Candidate name/reference; the generic label is retained under owner A2. Full DR15 who/summary/exclusion composition is DEFERRED, rather than fabricated or approved. Frozen/REVISION_COMPARISON READY-only guards were not weakened to obtain an address.

The independent actual-image receipt is ../screen-designs/SCR-45/review.md: 26 individually viewed PNGs, full 42-row policy, manual wording and every visible design difference/control purpose. No 100-Candidate, queue, paid£100/new£20, Source mutation, concurrency, actual native child journey or installed runtime acceptance is inferred.

## SHA-256 saved source

| File | SHA-256 |
| --- | --- |
| BUILD-FE js/bpay-next-office.js | 0f505fb70f79d5d9b740632c6c50cf85ebe99aabffd6a69c55e544e8fea46262 |
| BUILD-FE js/main.js | 898a9bd1b0468226720ac9d87f735c1104b4ac112307f39ee1ee00efb49500c7 |
| BUILD-FE css/bpay-next-office.css (unchanged presentation dependency) | 1c8f3ab6da7971c4dcdfcd126fdb01afb9673906b10a7998323a554162223e00 |
| BUILD-FE js/weekly-source/workspace-actions.js | 66345020ef21fdb2b71db996b8d9fbff100f1bbc9d4694dc2659b60cab5b416a |
| BUILD-FE js/weekly-source/import-workspace.js | 7d4621851bf5c5e5b14c0dbd2f149291a16f34ae050778a952cfc203855d9bb1 |
| BUILD-BE broker/src/weekly-source/routes.js | 3641498765ed87d9d2c450e07cfc3dca05a9601725976d2e82e315f40dde4b11 |
