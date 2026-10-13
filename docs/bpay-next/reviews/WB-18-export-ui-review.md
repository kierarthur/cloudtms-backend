# WB-18 export UI source review

Reviewed saved BUILD-FE source on 10 October 2026, against WB-18 in `02_WORKBENCH.md:727–746`, every WB-18 checklist row, the supplied SCR-15/SCR-15b designs, SCREEN_DESIGN_POLICY, and the existing standard child-window adapter. No browser, render, test, application, SQL, fixture, hosted request or export was executed by this reviewer. The later actual twelve saved A4 captures were individually inspected; the full 42-point review is recorded in `screen-designs/SCR-15/review.md`.

## Current verdict

The export request/download lifecycle and the bounded source/layout corrections pass this scoped source review. EW01–EW04 are closed in fresh saved source. Actual A4 UI approval is now **33 PASS / 0 FAIL / 9 N/A**, including the corrected export-only phone Close target. The 1,000-row/three-unticked/Included-total/Candidate-only business acceptance journey has not been executed by this reviewer; whole-card execution is not claimed here.

### EW04 — CLOSED: header strip limited to two secondary buttons

Fresh `mountWorkbenchShell` appends only Filters and Export CSV to `bpay-next-header-actions`. The existing real Refresh control and handler are retained in the root's post-list `bpay-next-window-actions` area. The Candidate strip retains Refresh and Export only. The header therefore meets SDP-16's two-secondary-button limit while retaining WB-18's header Export action. Actual placement/phone reachability will be checked in A4 pixels.

## Corrections verified in fresh source

| Finding | Current source evidence | Status |
|---|---|---|
| EW01: a late attempt referenced the next attempt's mutable controller | `run` creates `attemptController` and `attemptTimer`; fetch, timeout, pre/post-read, post-race and cleanup use that exact attempt. The outer controller is only the active Cancel/dismiss handle. | CLOSED |
| EW02: close/timeout could leave a pending body read or an unclaimed late response | Abort registers the finite `cancelRead` callback; the race includes interruption; a rejected/late response cancels its unclaimed body. Read cancellation, timeout clearing, listener removal and lock release are confined to the attempt. | CLOSED |
| EW03: success added a second Close in the footer | Both footer buttons are hidden after successful download. The real standard header Close remains. Confirmation styling is removed after success. | CLOSED |
| EW04: three secondary header controls | Main header now contains Filters and Export only; Refresh is retained outside that strip. Candidate strip remains Refresh and Export. | CLOSED |

## Request, scope and financial authority

- `openWorkbenchExport` freezes the real choice store once and serializes the complete namespace/intent/context/Candidate/sort descriptor into one body before any request. Its 32 KiB bound is explicit. Manual Try again reuses that exact string; the handler never rebuilds the request from subsequently changed ticks.
- The whole-list action requires coherent Ready-list and header contexts and receives the real store plus current Candidate-list sort. The Candidate action uses that same store namespace and its exact Candidate UUID, with the current group sort. Neither action enumerates only loaded browser rows.
- Existing complete intent distinguishes Candidate, Work/revision, approved-line and Case-component choices. Export neither clears local intent nor manufactures a command, run, acknowledgment, amount or bank detail. Rule B and the parked F-25 owners remain untouched.
- The browser receives CSV bytes, accumulates only the bounded response and downloads them. It does not calculate money, Included totals, VAT, deductions or selected membership. The server/Worker exact 19-column mapping is independently owned; this source review does not infer that the future financial-total acceptance has passed.
- The supplied design's sample count is omitted because this UI has no exact count reader for that whole export scope. It does not display fabricated counts. The scope label uses the captured actual channel; names and financial fields remain server-owned.

## Lifecycle, streaming and parent safety

- `current()` checks the original actor, positive owner readiness, the actual export frame and the supplied parent-current predicate. Dispatch also requires the active frame; success checks current identity after every body read and immediately before creating the downloadable object.
- The standard child adapter preserves the real parent frame and restores it on close. Cancel/Close aborts the active attempt, marks the child closed, clears the current timer and removes the scoped owner listener. No automatic request, refresh, selection replacement or retry follows dismissal.
- HTTP success, `text/csv`, a real body, a non-aborted attempt, complete EOF and nonempty content are required. The browser refuses content beyond 32 MiB. The 120-second timer and explicit abort interruption prevent a dismissed/timed-out attempt from keeping the dialog busy.
- Only after complete EOF does the UI create a Blob/URL and click the download anchor. An error after any number of chunks produces the inline explicit message “The export could not be completed; no file was produced.” and a manual Try again control. No partial file is offered. Temporary chunks, stream locks and object URLs are cleaned up.
- The dialog uses text nodes, standard confirmation frame/footer, Cancel before Export CSV, status/alert roles and retained unknown-safe failure behavior. No payload/nonces, technical IDs or fabricated money appear in its copy.

## A4 and future joins

SCR-15 confirmation, Candidate confirmation, SCR-15b failure and completion now have actual reviewed dark/device-light/phone captures. The full A4 review passes all applicable checks and retains exact image/source hashes. The one phone completion Close finding was fixed with a narrow export-only 44px selector and verified in fresh pixels. Counts remain omitted under BD074. Later selected-estimate/action/membership joins and whole-card application acceptance retain their real owners; this note supplies no business execution evidence.

The fresh Candidate export correction first validates the current child against the real retained parent context using `sameWorkbenchRevision`. It then freezes that actual parent `address.context` and passes the exact Candidate UUID separately. This preserves the existing namespace without fabricated retagging, current-input refresh or competing snapshot authority; stale child/parent revision is refused until actual Refresh.

## Reviewed source identities

SHA-256 at this review:

| Source | SHA-256 |
|---|---|
| BUILD-FE `js/bpay-next-workbench.js` | `d03d7da007c80602449795d9c4c34111c20909f040c2446bcad1b722bda4b684` |
| BUILD-FE `js/bpay-next-office.js` | `62b5df74a6307cccb5eb8e47dee9b89648ff913030dc30772e26809139a40a82` |
| BUILD-FE `js/bpay-next-ui.js` — retained standard-window adapter | `e72d3c6280cd44dfb34346613724f05e0e44f2c4bcf33cdc174862834295ecc5` |
| BUILD-FE `css/bpay-next-office.css` | `5f9101ab09eb4ede8997f853cf8cd555a1217dcf83914f7fd06819503d2b5651` |

Source was inspected directly from saved disk. These hashes bind the source verdict above; the associated final screenshots are individually bound in `screen-designs/SCR-15/review.md`.
