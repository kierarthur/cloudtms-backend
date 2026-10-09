# F-12 one-writer source review

Independent read-only review, 9 October 2026. Re-read F-12/F12-D01–D05, the owner start sheet, `design/F-12-one-writer-fence.md`, `F-12-wiring-provenance.json`, the full scoped index diff and surrounding entry/mail/auth/cron sections, then re-read root's completed corrections. Compared retained behaviour with saved RESET and current upstream source. No product/test/SQL edits, tests, mock callers, database/SQL execution, runtime, Docker or hosted calls.

**PASS for F12-D01–D04 source containment after correction.** Fresh reviewed index raw SHA256 is `8e3b8c8384241e55dde39a02dfcad3bb760f5469967b1c1405ef046c22c68834`, matching the updated provenance postimage and its 20 bounded logical changes. D05 remains a distinct pending release mechanism and is not claimed built by this review.

## Correction verified

The first review identified an unguarded retained sent-remittance state retry after the claimed-mail hold. It could call the old financial `pay_remittance_mark_sent` writer without fresh LEGACY admission and increment its retry counter after an internally caught writer failure. RESET had the same omission; hunk parity alone did not satisfy the card.

Root corrected the saved target. `fetchSentRemittanceRowsForStateRetry` now refuses the fetch unless claim-time and fresh owner admission are LEGACY; the sent-row loop rechecks the same admission before any state read/reconciliation. `markRemittanceDrainResult` independently rechecks fresh LEGACY before its old owner RPC, returns true only after the actual RPC succeeds, and false on the caught writer failure. `sentStateRetryCount` increments only for `recorded === true`. Held rows retain their existing stored financial/delivery facts and obligations. The unchanged 0550 protected-table backstop remains intact.

Root also added the actual stored `purpose` column to `sbGet2faChallengeById`'s explicit SELECT. The resend fence therefore evaluates the stored payment purpose rather than an omitted property. The root correction does not weaken challenge/IP/used/active-user validation or ordinary login resend. Both corrections were re-read in the full scoped diff before this final verdict.

## Source findings otherwise accepted

| Deliverable | Review result / scope |
| --- | --- |
| F12-D01: 0550 database fence | Retained source accepted: unchanged exact 33 relations, ALWAYS row/TRUNCATE triggers, FOR SHARE owner backstop; no SQL installation/app claim |
| F12-D02: Worker financial-route fence | Accepted: fresh bounded owner status, deliberate 410 retirement and 503 disabled/unavailable response; exact normalized method/path exemptions; Office auth before lookup and provider-specific authentication retained |
| F12-D03: legacy email hold | Accepted: claim/render/send/generic-email/payment-2FA boundaries plus fresh sent-state/outcome admission and truthful completed-retry counter |
| F12-D04: financial cron retirement | Accepted: operations entry asserts fresh LEGACY before work; main banking cron skips NEXT/DISABLED and owner failure cannot reach financial work |
| F12-D05: external admission quiet window | **PENDING** F-17/F-18/F-19 mechanism and genuine in-flight obligation reconciliation; correctly not marked built/proved |

The global fence is before retained auth/financial route dispatch, with exact payment reauth handling and no generic `/next/*` bypass. Candidate private entry and unrelated weekly Source/invoice internal routes remain in their current upstream positions. The F-09 classifier retains only exact factual bank-name override/clear exceptions and the exact six later NEXT route shapes.

Mail release uses the existing lease-only PATCH conditioned on BOTH row ID and this drain's `attemptLeaseToken`. The body clears only the three attempt-lease fields. A failed release produces a static uncertainty error and leaves expiry to the original lease; it does not alter mail status, delivery times, provider facts, financial facts, SENT/FAILED state or a MyTMS receipt. Claim-time owner failure holds financial/malformed metadata while factual mail proceeds. Row render and provider-batch send recheck ownership. Cancellation-notice recovery is skipped when claim-time/fresh admission is not LEGACY.

The provider send loop retains current upstream `candidatePaperProviderAuthorityCurrent` and `candidateManagerProviderAuthorityCurrent` checks with the same claimed row/current token/fetch/headers, revoke/defer handling, per-item mapping and awaited `postToPowerAutomate`/batch reconciliation. It does not copy RESET's older send loop over these newer checks. Existing `recordMyTmsOutcomeSafe`, failed/deferred/sent helpers and `recordMyTmsInvitationOutboxOutcome` calls remain in place. Post-provider outbox/MyTMS receipt recording remains available for actual admitted outcomes; only the old financial-state marking now rechecks LEGACY. D05 must reconcile any genuine obligation before switching ownership. No Source, Invoice, Candidate or MyTMS feature body was replaced in the diff.

Generic email authenticates admin and validates its payload before the financial metadata guard. The stored payment-purpose resend checks exactly PAYMENT_SCHEDULE, PAYE_SAME_WEEK_OVERRIDE and PAYMENT_REVERSAL after challenge identity/IP/used validation and before changing codes or sending; ordinary login resend retains its existing path. Its owner guard is additional admission, not a substitute for the existing active-user/challenge checks.

Fresh status uses the F-09 fixed zero-argument, service-only transport: 1 KiB response, 8 second deadline, finite owner and lossless positive epoch, no status cache. Current primary [Workers best practices](https://developers.cloudflare.com/workers/best-practices/workers-best-practices/) guidance was checked during the preceding module review; the new business calls here are awaited and no request-scoped global cache is introduced. This is not a network lease. A provider/mail action already admitted before owner change still requires D05 quiet-window/reconciliation before the final switch.

Bookkeeping: re-read the corrected design prose, which now agrees with the final provenance's actual 20 bounded logical changes, including generic mail, stored-purpose resend/fetch and financial recovery fixes. The scoped code diff is the reviewed authority. Root reports syntax PASS; actual JS/source-gate results belong to root's checks and were not executed independently here.

Required feature follow-ups remain recorded: retired payment reauth, snooze, pay-method change and remittance have no replacement in F12; PATCH Timesheet pay-hold is not globally fenced while NEXT ignores the old hold. F-13 owns obsolete envelope retirement and genuine obligation dispositions. No old engine runtime availability/parity, provider/email proof or final cutover is claimed.
