# Weekly Source imports, queries and finalisation: policy amendment

Date: 30 September 2026. Status: user-confirmed business requirements recorded before implementation and modal redesign. The implementation mechanics and regenerated designs below are still to be reviewed; this document does not claim deployed behaviour or authorise a release.

## Authority and scope

Read alongside the accepted `CloudTMS_Weekly_Source_HANDOVER_2026-09-19.zip`, particularly `02_CONTROLLING_POLICY.md`, `04_MODAL_DESIGN_POLICY.md` / `04_MODAL_POLICY.json`, `04A_MANAGER_EMAIL_POLICY.md`, `14_LOCKED_SOURCE_PROFILES_AND_PROTECTED_SHIFT_PAY.md`, and the superseding financial authority in file 24 and the R8 owner corrections. Preserve the accepted archive unchanged.

This dated amendment records the user's explicit 30 September decisions. It supersedes the blanket before-cutoff finalisation prohibition for import-authoritative HealthRoster only. It clarifies checking-versus-finalisation use, query visibility, exact notification scope and the requested layout. All unmentioned policies remain unchanged. In particular, it does not amend Banking Pay, Policy X, settlement, payment execution, invoice valuation, ordinary Timesheet authorisation, or independently approved pricing-warning treatment. Ordinary non-source-authoritative HealthRoster Timesheets are outside this change.

Before UI implementation, reconcile these requirements into an editable successor of the pack's policy JSON and its renderer. Regenerate affected policy designs through the policy-owned renderer; do not retouch PNGs or silently change the sealed archive. Current-browser screenshots are diagnostic evidence, not regenerated proposed designs.

## 1. Choosing what an upload is for

- Keep NHSP previously released shifts and final backing reports as distinct formats. A previously released file is checking evidence, never a finalisation report.
- For an import-authoritative source whose checking and final exports share a format, Office must explicitly choose `Check hours and resolve queries` or `Prepare for finalisation`. Checking is the default before and after cutoff, including after a prior finalisation. Do not infer this choice from the clock or an individual row's finalised flag.
- File use is distinct from the existing ordinary-upload versus final-source-correction purpose. Persist and audit both decisions against the exact accepted file/version. Selecting Prepare is not the act of finalising and creates no final financial authority.
- Only a current eligible complete file may be prepared. A newer checking upload must not leave a superseded older file silently finalisable; show the need for a current finalisation review. Preserve existing scope/coverage rules and never revive a superseded upload. Explicit preparation of the current file must have the same server checks as preparation during upload.
- Checking after a closed finalisation does not amend its final authority. Preserve the existing new-cycle/correction boundaries.

## 2. Source-specific finalisation

### NHSP

Only the actual final backing report belongs in Finalise report. Preserve supplier report identity, Trust separation, physical movement rules and cutoff validation. There is no HealthRoster-style early-finalisation exception for NHSP.

### Import-authoritative HealthRoster

Office may choose Prepare for finalisation before cutoff and explicitly finalise then. A valid source-finalised row is eligible; source validity and existing authorisation rules still apply.

If recognised non-finalised rows remain, show their count and details and require an explicit acknowledgement that they are not being included as positive worked shifts. Their existence alone does not block valid finalised siblings. Do not relabel malformed rows claiming finalisation as harmless non-finalised rows or introduce a bypass for genuine blockers. Preserve the controlling profile's admission granularity.

The review must separately show reversals: a previously finalised shift that is now absent or non-finalised within confirmed coverage can remove a previously finalised source position. The acknowledgement must not imply those earlier positions are simply left unchanged.

Only the exact locked source layout's actual values and finalisation evidence govern eligibility. Planned hours are not a fallback. Non-finalised rows remain query evidence, do not create a positive invoice line or ordinary source-pay proposal, and retain the existing independent `Protect pay` route. Finalised hours are not an automatic bank payment: ordinary authorisation and downstream approved processes remain mandatory.

### Other same-format sources, including configured Magnit profiles

Use the explicit checking/preparation choice. Retain each profile's existing row eligibility and cutoff authority. This amendment does not automatically extend HealthRoster's early-finalisation exception to another source.

## 3. Where problems appear

Queries must provide both `Hours questions` and `Office checks` without confusing their permissions or actions.

Hours questions retain the established candidate/client/manager grouping and outreach rules. Office checks must also show unresolved source rows that have no candidate match or no discrepancy incident yet: identity, Trust/client, contract, band, source validation and pricing issues. Keep warnings distinct from blockers under the current pricing authority. A problem not yet assessable because an earlier dependency is unresolved is not a passed check.

Show the name supplied in the file, source worker identifier, Trust/client, booking reference, work date, actual start/end and break, and the exact problem. In linking dialogs, show source details alongside eligible CloudTMS choices. An inactive candidate must be identified as inactive, not silently reactivated. Reuse existing candidate/client/contract lifecycle and eligibility controls; recheck remaining issues after an authorised resolution. Ambiguous choices must show the actual alternatives, not guess one.

Office checks alone do not authorise candidate questions, manager emails or financial changes. Retain an attention indicator and review link on Imports, including independently required pricing warnings.

Finalise report/source/week must contain only actual prepared finalisation work, never Ready/Blocked candidate lists from checking-only uploads. Show an explanatory empty state when none is prepared. A prepared report's remaining blockers must be visible there as well as in Queries. Keep source-specific scopes and immutable history intact.

## 4. Notification routes and requested weeks

### Automatic contact on import

User clarification on 1 October 2026: accepting a pre-finalisation import must automatically start eligible missing-Timesheet requests and signed-hours discrepancy questions. Office does not have to select Ask candidates. Apply the existing contract/client settings, global timing, source-authority eligibility, candidate availability and manager routing policy. The manual controls remain optional interventions, not prerequisites for normal contact. Unmatched candidates and other Office-only blockers do not authorise guessed recipients or messages.

An unchanged subsequent import must not send another initial candidate or manager message or restart a clock. New upload, publication, resolution, or evidence identifiers alone are not material changes. Preserve resolved decisions and accepted messages. Only changes to the relevant business facts under the existing comparison policy may supersede an old question and start new work; stale questions must not remain answerable. Keep publication-bound safety fingerprints separate from the semantic comparison used to decide whether contact is new.

This clarification authorises implementation, not a claim of completion. Rollback-contained runtime proof must cover first automatic contact, disabled settings, missing versus signed weeks, preserved manager-direct routes, unchanged re-imports, changed facts, and unchanged reminder deadlines before publication.

Keep three distinct reasons for contacting a manager:

1. An explicit Office-selected manager-direct route, with actor and audit evidence.
2. The permitted exception for the exact signed week submitted in response to a missing-Timesheet request.
3. The ordinary candidate-first journey and its existing timed manager escalation.

The second reason must not create or rewrite the first. Preserve genuine Office overrides, contact preferences, accepted messages and historical audit. Do not mass-reset existing routes or resend already accepted emails. Existing routes without reliable provenance require individual review, not a guessed conversion.

A missing-Timesheet request may suppress an hours-check question only for its matching still-outstanding membership: exact agency/cycle, candidate, client, contract/week and current eligibility. Another week or client must not suppress a question for a signed week. Missing-week submission and hours-check requests must coexist without one cancelling the other. Preserve the missing request's immutable original membership, date order, completed scopes and original deadlines.

`Remind candidate` must refer to the actual outstanding request relevant to the displayed work. If a missing-week request is relevant elsewhere, expose it under its own clear week/request label. Do not present a reminder for a question that has never been created. Validate request identity, current membership, permission, cooldown and replay on the server.

Retain policy timing: six-hour candidate reminder and eligible answered-only manager tranche, twelve-hour silent remainder for signed-hours queries, sixty-minute manual candidate-reminder cooldown, five-minute manual manager-send cooldown. An absent requested Timesheet remains Office work after twelve hours; silence does not send it to the manager. A late signed mismatching requested week follows the existing immediate manager exception without a second candidate check. Do not fabricate a candidate confirmation merely because the candidate signed their Timesheet.

When facts change, an old answer cannot apply to the old hours. Refresh only still-authorised unanswered same-incident work; retain the existing new/reopened-incident versus changed-unresolved-incident distinction. Do not reset timers or send duplicate requests on unchanged imports or mere comparison updates. In-app request creation and push delivery remain separate statuses; notification opt-out or absent devices do not erase the request.

## 5. Modal and dropdown design requirements

- Use the existing wide CloudTMS modal shell and visual tokens, with viewport margins. Fit desktop Queries without horizontal scrolling at the reviewed desktop sizes; use the existing responsive approach on small screens. Do not hide overflow or shrink text to disguise clipping.
- Keep the status badge readable. Stack Open above the relevant reminder/action inside the Actions column, with consistent widths. Align the header and body right edge; no unexplained trailing gap or overflowing button.
- Preserve sticky headers, continuous loading, sorting, the two separate selection planes, keyboard access, and state when returning from a child modal.
- Distinguish optional filters from required command context. An optional Trust/client filter needs a clear all-clients state that can be selected again. Where an exact Trust is required for finalisation, an all-clients filter never grants command authority.
- Preserve valid dropdown choices across tab changes; clear dependent context only where required and make that change evident. No stale response may replace a newer choice. Preserve the existing non-empty-selection confirmation.
- Long Trust labels need a readable full-label presentation without widening the grid. All supported choices, keyboard operation, focus after refresh, empty/loading/error states and narrow viewports need browser proof.
- Continue readable dates such as `21 Sep 2026`; no browser financial calculation.

## 6. Implementation and verification sequence

1. Review and reconcile this amendment with the successor policy JSON; render proposed screenshots for review before modifying production modal layout.
2. Add server-owned file-use authority and correct checking/finalisation projections and command guards together. Preserve prior date-range and unrelated-query fixes.
3. Expose and wire eligible Office-resolution actions, with exact file/version checks and accurate dependency status.
4. Repair notification reason/scope, independent missing-week and hours-check request ownership, and exact reminder binding. Inspect existing uniqueness/current-generation assumptions; a narrower WHERE clause alone is not sufficient.
5. Implement the agreed layout and dropdown behaviour against those server contracts.
6. Prove NHSP checking never enters finalisation; HealthRoster early all-finalised and acknowledged mixed cases; malformed-finalised refusal under its policy; configured generic checking after cutoff; no unintended source/pay/invoice change; inactive/unmatched candidate and stale linking; all notification routes and clocks; two concurrent week journeys; unchanged import idempotency; changed-fact stale answers; desktop/mobile and keyboard UI.

Record local tests separately from hosted proof. Any database/application release follows the existing protected TEST process and current user authority. No LIVE work, payment execution, outgoing diagnostic messages, historical record repair or configuration change is authorised by this document.
