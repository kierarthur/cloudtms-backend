# Combined Weekly Source workspace

Status: implementation authorised by the user on 1 October 2026. This is a successor to `WEEKLY_SOURCE_IMPORT_QUERY_POLICY_AMENDMENT_20260930.md`; it does not claim installed functionality.

## Scope

Apply the combined journey only to import-authoritative sources. Preserve the existing timesheet-authoritative roster journey, candidate/manager authority, pricing, invoice valuation, protected-pay ownership, Policy X and payment execution boundaries.

## Upload and ownership

- Non-NHSP files are uploaded individually. Each upload identifies its client and explicitly chooses checking or preparation for finalisation; checking is the default. Return to the same workspace ready for the next file.
- NHSP previously released files remain checking evidence; final backing formats identify finalisation work. Preserve single-file and multiple-backing-file uploads.
- One non-NHSP file never represents several clients. Current source ownership must be independent for each client and finalisation period. A new file for one client must not supersede another client's file, questions or prepared report.
- Preserve immutable accepted files and historical outcomes. Do not revive previously superseded files or reinterpret old financial authority.
- Old outstanding periods and the current calendar period must coexist. Identify the actual finalisation period explicitly; never infer an old report belongs to this week merely because it was uploaded today.

## Working views

- Imports defaults to relevant current checking files, pending prepared reports and actionable failed/incomplete attempts. Superseded attempts and completed historical work remain accessible in History. A fresh rejection remains visible with its reason.
- File details show purpose, client, coverage, upload time, acceptance outcome, specific issues, affected shifts and available finalisation history. Empty report numbers are not errors on checking files.
- Queries combines hours questions and Office checks across eligible clients/sources/periods while preserving each action's exact authority. Existing rules for notification timing and obsolete questions remain unchanged.
- Finalise contains Ready, Blocked and Complete. Only prepared finalisation work enters Ready/Blocked. Completed shifts are read from final authority, not mutable current import rows. Show finalisation time and any independently outstanding approved-hours recovery.
- A zero-count tab is visually subdued. Mixed clients can populate all three tabs. A client/report with a blocker cannot partially finalise its otherwise-ready siblings.
- The default combined shift sort is client, candidate surname, shift date, stable identity. A single-client filter hides the client column and defaults to surname/date. Column selection changes the primary sort.
- Continuous scrolling, no page numbers. Keyboard type-to-jump seeks across the entire server result, not just loaded rows. Only handle typing while the table has focus; retain normal editing in fields. Preserve sort, filters and scroll position across child dialogs.

## Timing and outstanding reports

- Show this week's exact finalisation cutoff and whether the clock has passed it, independently of the selected report's stored state. Display dates as `7 Oct 2026, 15:00` in the configured timezone.
- Separately count older missing finalisation reports and received reports awaiting finalisation. Counts represent client/week obligations, respect filters and effective membership, and disappear when zero.
- Selecting a count opens its clients and weeks. A recorded zero return satisfies its exact obligation. Never infer a zero return from a missing file.
- Mixed cutoff schedules must be labelled as such rather than assigning one deadline to all sources.

## Finalise eligible clients

- Allow one client, selected eligible clients, or all eligible clients represented by prepared reports in the review. Show exact clients, periods and excluded blocked/missing reports before confirmation.
- Preserve NHSP cutoff/report identity and each non-NHSP profile's existing finalisation authority. HealthRoster early completion and explicit non-finalised-shift exclusions retain the 30 September rules.
- Run independently replay-safe client/report finalisations; keep successful completions if another fails. Recheck source versions, blockers, acknowledgements and permission per client immediately before mutation. A stale preview must not broaden or silently change the confirmed set.
- Completed reports have no ordinary Finalise action. Corrections retain their dedicated existing process. Source finalisation does not claim the downstream approved-hours recovery is finished.
- `No shifts to import` remains a separate explicit client/week declaration with actor/time. Align displayed availability with server refusal checks; show a useful explanation when existing source, unresolved ownership or open issues prevent declaration.

## Protected shift pay — approved addition

Authorised by the user on 1 October 2026 together with the combined workspace. These are implementation requirements, not a statement that the UI or hosted system already satisfies them.

- Queries provides `Add protected shift` independently of imported rows, candidate submissions and existing Timesheets. It remains available for an eligible client/candidate with no import at all. An existing discrepancy provides `Protect pay` and prepopulates the same form.
- One `Protect shift pay` child modal contains Client, Candidate, Work date, Contract, Start, Finish, Break (minutes), Reason and the resulting net hours. No break-start/end controls. Zero break must be explicit; validate overnight duration and positive worked time. Preserve parent filters and scroll on close.
- Require a server-qualified contract for the candidate/client/date and import-authoritative route. Select and display a unique choice; require an explicit choice among several; offer the existing Create contract journey when none qualifies. No invented booking reference or dummy import. Establish the required source cycle and contract-week context through their existing owners.
- Retain candidate-submitted, source-imported and Office-approved hours as distinct evidence. Label missing signed submission and absent source plainly. Never fabricate a signature, replace source facts, or make protected work invoiceable.
- Delegate approval/amendment to the existing protected-family and action orchestrators. Preserve permission, concurrency, stale-version, reason, idempotency, calculation, entitlement and audit boundaries. The editor does not accept money, rates, invoice values or Banking commands.
- Queries keeps protected work visible without an import: `Protected pay — awaiting source`. Open provides `Change protected shift` and permitted existing follow-up actions. History retains actor, time, reason and before/after approved schedule.
- Later source matching uses actual client, candidate, work date, compatible contract and shift evidence; exact clock-time or break equality is not required. Prefer a proved source reference/retained identity when available. Do not use similarity to silently join competing same-day shifts. Office confirms ambiguous matches from a server-qualified shortlist; confirmed links persist and cannot duplicate existing work/payment ownership.
- Queries displays `Confirm shift match` for unresolved identity and `Ready to reconcile` only after the identity and current source authority are established. Open shows protected and imported schedules side by side. `Accept system hours and reconcile` delegates to the existing reconciliation owner; `Wait` retains the approved schedule. Re-import alone must not erase protection or create another payable copy.
- No changes to Banking Pay economics, Draft/payment/provider/settlement execution, invoice entitlement or timesheet-authoritative roster behaviour. Existing reconciliation handles prior paid amounts; do not calculate a new payment delta in the browser.

## Acceptance and release gates

### Resolved questions — additional user authority, 1 October 2026

- A resolved decision accepting the reviewed source facts must not reopen for the same work identity and unchanged material facts. This applies to Office acceptance, manager confirmation and the candidate accepting that their claim was wrong. Preserve the original decision and audit; do not rewrite signed evidence.
- Compare the exact client/candidate/work identity, contract, issue kind, source presence, and both candidate and source start, finish and break minutes. A new upload ID, report type, filename, or unchanged re-signing of the week is not a changed shift. Moving from checking to final authority alone is not a new hours discrepancy.
- Materially changed facts are compared again. Raise another question only if a discrepancy remains. If the values now match, do not raise an hours question. A query resolved because the source actually matched must not suppress a later recurrence using the obsolete mismatch that preceded that resolution.
- Only the most recent incident decision for the shift may apply; an earlier decision must not conceal a later different episode. Changed unanswered questions retain the existing obsolete-answer and timing protections. Do not modify notification timing or manager-direct policy.
- Prove both source-driven rechecks and candidate-submission comparison paths, changed candidate/source breaks and hours, missing shifts, checking-to-final transition, unchanged repeat import, and a genuine recurrence after an actual match.

Prove two non-NHSP clients remain independently current; replacing A preserves B; prior unfinished periods survive current-period work; NHSP trust separation; checking-only exclusion; blocked-client isolation; concurrent/replayed finalisation; partial batch failure/resume; zero-return refusal and completion; full-result sorting/seeking; and unchanged timesheet-authoritative roster behaviour. Use disposable PostgreSQL and browser fixtures before protected TEST deployment. Retain exact source and deployed identities in GitHub and release evidence.

Additionally prove protected-shift creation with no import, no candidate submission and no prior Timesheet; zero/one/many eligible contracts; breaks and overnight shifts; amended hours; different-hours future matches; ambiguous and multiple same-day shifts; retained identity on repeat import; replay/concurrent approval; and no duplicate pay. Hosted user testing follows deployment at the user's request; mandatory release/security verification is not deferred.
