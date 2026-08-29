# CloudTMS route-conversion warning catalogue

Status: controlling future CloudTMS frontend wording. Updated and approved 11 August 2026. Simple Timesheet, Bulk Process and Bulk Authorise must consume one server preflight and one shared warning renderer keyed by `warning_code`. A route mutation must never run on the first click.

## W01 — ELECTRONIC, nobody signed

Warning code: `ELECTRONIC_UNSIGNED_TO_MANUAL`

Title:

> **Convert this timesheet to Manual?**

Body:

> The candidate will no longer be able to submit this version electronically. CloudTMS staff will be responsible for entering and processing the timesheet.
>
> Use this only where the candidate has supplied the timesheet outside the electronic route, or CloudTMS staff need to intervene.

Buttons:

- **Go Back**
- **Convert to Manual**

A mandatory intervention reason is shown before final confirmation.

## W02 — candidate signed, manager approval pending

Warning code: `CANDIDATE_SIGNED_MANAGER_PENDING_TO_MANUAL`

Title:

> **The candidate has already signed this electronic timesheet**

Body:

> It is awaiting hiring-manager approval. Converting it to Manual will cancel the current electronic approval request and the manager’s approval link will stop working.
>
> The candidate’s signed electronic submission will remain in the audit history but will not apply to the new Manual version.
>
> Use **Reject Candidate Submission** instead where the candidate can simply correct and resubmit the timesheet.

Buttons:

- **Go Back**
- **Continue to Manual conversion**

A mandatory intervention reason is shown before final confirmation.

## W03 — hiring manager approved; CloudTMS not yet authorised

Warning code: `MANAGER_APPROVED_TO_MANUAL`

Title:

> **The hiring manager has already signed and approved this electronic timesheet**

Body:

> Converting it to Manual will retire the completed electronic approval. The candidate and manager signatures and the signed electronic timesheet will remain in the audit history, but they will not apply to the new Manual version.
>
> This action should be used only where the candidate or hiring manager has reported that the submitted hours are wrong, or another exceptional office intervention is required.

Buttons:

- **Go Back**
- **Continue to Manual conversion**

A mandatory intervention reason is shown before final confirmation.

## W04 — CloudTMS-authorised

Warning code: `ROUTE_CHANGE_REQUIRES_UNAUTHORISE`

Title:

> **Unauthorise this timesheet first**

Body:

> This timesheet has been authorised in CloudTMS. It cannot be converted to Manual until the existing Unauthorise process has been completed. Unprocess it afterwards where the normal lifecycle requires this.

Button:

- **Close**

Where an existing Unauthorise action is available in the same modal, the UI may show that existing action. The route conversion itself remains blocked.

## W05 — invoiced or paid

Warning code: `ROUTE_CHANGE_FINANCIAL_HISTORY_BLOCK`

Title:

> **This timesheet cannot be converted**

Body:

> This timesheet has financial history and its submission route cannot be changed. Use the appropriate additional-timesheet, correction, credit or reversal process.

Button:

- **Close**

## W06 — import-authoritative

Warning code: `ROUTE_CHANGE_IMPORT_AUTHORITATIVE_BLOCK`

Title:

> **This timesheet is controlled by an import**

Body:

> Candidate-entered hours are not permitted for this timesheet. The submission route cannot be changed. Expenses must use the separate expense-timesheet route.

Button:

- **Close**

## W07 — decision between rejection and office intervention

Show before the signed-state warning where the current submission could simply be returned to the candidate.

Title:

> **Does the candidate need to resubmit instead?**

Body:

> Use **Reject Candidate Submission** where the candidate can correct and resubmit the timesheet themselves.
>
> Convert to Manual only when CloudTMS staff need to enter or process the replacement timesheet on the candidate’s behalf.

Buttons:

- **Go Back**
- **Use Reject Candidate Submission**
- **Continue to Manual conversion**

## W08 — QR/paper pack issued but not returned

Warning code: `QR_ISSUED_TO_MANUAL`

Title:

> **A timesheet pack has already been issued**

Body:

> The candidate may already have printed the documents. Converting this timesheet to Manual will invalidate the current code and the issued pack can no longer be returned.
>
> CloudTMS staff will become responsible for entering and processing the replacement timesheet. The issued pack will remain in the audit history.

Buttons:

- **Go Back**
- **Continue to Manual conversion**

A mandatory intervention reason is shown before final confirmation.

## W09 — signed QR/paper evidence returned

Warning code: `QR_SIGNED_TO_MANUAL`

Title:

> **A signed timesheet has already been returned**

Body:

> Converting it to Manual will retire the signed returned evidence. The signed document will remain in the audit history but will not apply to the new Manual generation.
>
> Continue only if the candidate or hiring manager has reported that the submitted hours are wrong, or another exceptional office intervention is required.

Buttons:

- **Go Back**
- **Continue to Manual conversion**

A mandatory intervention reason is shown before final confirmation.

## W10 — MANUAL to fresh ELECTRONIC

Warning code: `FRESH_ELECTRONIC_RESUBMISSION_REQUIRED`

Title:

> **Switch this timesheet back to Electronic?**

Body:

> The current timesheet will be opened as a fresh electronic submission. Previous signatures and signed documents will remain in the audit history and will not be reused for changed content.
>
> The worker will be notified that the timesheet must be reviewed and resubmitted.

Buttons:

- **Go Back**
- **Switch to Electronic and notify worker**

Worker notification title:

> **Your timesheet needs to be resubmitted**

Worker notification body:

> Open the app to review and submit your timesheet again.

Deep-link to the new current timesheet/workflow.

## W11 — MANUAL to fresh QR/paper submission

Warning code: `FRESH_PAPER_RESUBMISSION_REQUIRED`

Office title:

> **Create new signing documents?**

Office body:

> A new timesheet pack and a new code will be created for the current hours. Any older pack or code will remain historical and cannot be returned.
>
> The worker will be notified that new documents are ready and the timesheet must be resubmitted.

Buttons:

- **Go Back**
- **Create new documents and notify worker**

Worker notification title:

> **Your timesheet needs to be resubmitted**

Worker notification body:

> New documents are ready for signing. Open the app to continue.

Candidate-facing wording does not use “QR”. Notification is released only after durable pack readiness.

## W12 — QR replacement/reissue

Warning code: `QR_REPLACEMENT_PACK_REQUIRED`

Title:

> **Issue a replacement timesheet pack?**

Body:

> The current pack and code will be invalidated. Any printed copy can no longer be returned. A replacement pack with a new code will be generated.
>
> Please remember to tell the worker to sign the replacement timesheet before returning it.

Buttons:

- **Go Back**
- **Issue replacement pack**

The existing approved email sentence remains:

> Please remember to sign the replacement timesheet before returning it.

Recommended worker push after replacement-pack readiness:

Title:

> **Replacement documents are ready**

Body:

> Please sign and return the replacement timesheet.

## W13 — narrowly controlled exact ELECTRONIC undo

This must not appear in ordinary route actions.

Warning code: `EXACT_ELECTRONIC_RESTORE_PROVEN`

Title:

> **Restore the previous electronic approval?**

Body:

> CloudTMS has proved that the current hours, signatures, signed document and financial content are identical to the previous electronic submission. Restoring it will make that exact approved electronic generation current again.
>
> No worker resubmission will be requested.

Buttons:

- **Go Back**
- **Restore exact electronic version**

Where exact proof fails, use W10 and create a fresh ELECTRONIC generation.

## W14 — remove one incomplete expense claim and continue to MANUAL

Warning code: `CANDIDATE_INCOMPLETE_EXPENSE_CLAIM_REMOVE_CONFIRM`

This warning is used only when an ELECTRONIC or QR/PAPER timesheet is being switched to MANUAL and exactly one separate, incomplete expense claim would otherwise be left active against the outgoing timesheet version. It includes amendable `REFUSED`.

Title:

> **Remove the incomplete expense claim?**

Body:

> The candidate has started an expense claim but has not completed it. Do you want to remove the incomplete claim and continue?

Buttons:

- **No**
- **Yes — remove claim and continue**

**No** closes the modal and performs no mutation. **Yes** submits the exact preview context and required intervention reason. CloudTMS must atomically supersede the incomplete claim and mutable approval/component lineage, retire obsolete PAPER/QR delivery authority where applicable, retain immutable sent/signed/refusal history, and only then rotate the timesheet to MANUAL. More than one affected workflow, ambiguous identity, unrelated claims, protected financial history or a live provider handoff remains fail-closed.

The office frontend must render W14 using the established styled `uiConfirmModal` family. Native browser or Windows alerts are prohibited. The modal must be visually verified with realistic populated data at desktop and narrow widths.

## Confirmed transition contract

Preview returns the warning code, permitted action, reason requirement, lifecycle row signature and `context_sha256`. Confirmation submits the exact current timesheet, expected row signature, expected context hash, target action and required reason/note. CloudTMS locks and recomputes the same context before changing route state.

Closed exceptional-intervention reasons:

- `CANDIDATE_SUPPLIED_MANUAL_TIMESHEET`
- `CANDIDATE_REPORTED_HOURS_INCORRECT`
- `HIRING_MANAGER_REPORTED_HOURS_INCORRECT`
- `ELECTRONIC_SUBMISSION_TECHNICAL_FAILURE`
- `OTHER_EXCEPTIONAL_OFFICE_INTERVENTION` — note required

When a live email manager request is withdrawn during an exceptional Manual takeover, the approved cancellation email is:

Subject:

> Timesheet approval request withdrawn

Body:

> The approval request for this timesheet has been withdrawn by CloudTMS. No further action is required.
