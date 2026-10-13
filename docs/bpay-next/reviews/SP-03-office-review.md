# SP-03 Office source review

**PASS — D04 source scope only.** Fresh saved BUILD-FE `js/main.js`, its complete diff from HEAD, actual single/Contract Week/selected-bulk callers, `index.html` and Bible note reviewed on 10 October 2026. Full card `05_SOURCE_AND_PROTECTED_PAY.md:159–187` and all four deliverable rows were reread during the census. No product edits, tests, SQL, runtime, fixtures, application or hosted actions were performed. Business/joined acceptance remains **NOT_RUN**. D01/D03's separate permission/WAIT gate and Worker refusal-transport fixes are not approved by this Office verdict.

## Reviewed behavior

- The finite helper recognizes exactly `WEEKLY_SOURCE_CANDIDATE_BUSY` and `BPAY_NEXT_SOURCE_FIRST_ADDITIONAL_UNMAPPED`, using real refusal fields or the exact code message. Successful replies are not remapped. No arbitrary substring, amount, financial field or browser owner decision is introduced.
- A positive current-actor F-15 `NEXT` peek is required for both screen labels. Unavailable/LEGACY/DISABLED status retains the existing refusal; it cannot change authorisation eligibility or route selection. Existing F-15 client qualification occurs only after an actual known refusal.
- Candidate busy is explicitly retryable with the exact card sentence: “A payment step for this Candidate is running. Try again in a moment.” Additional copy names an Additional item and its missing/mismatched approved units without inventing an offending code. The bare 2120 refusal provides no item-specific DETAIL.
- `authoriseTimesheet`, `contractWeekAuthorise` and `authoriseSelectedTimesheets` capture `requestActor` at entry before their first asynchronous work and pass it through error/result mapping. Qualification rejects a changed actor before its status read and after the await; presentation checks the same actor again. The earlier finding that an old request could borrow a newly signed-in actor's NEXT status is **closed** in the final bytes.
- Single Timesheet mapping occurs before the existing lifecycle/moved reconciliation and final throw. The error copy preserves the original structured error/status/body; the original expense/pair confirmations, expected IDs/signatures, genuine authorisation POST and footer behavior are retained. Busy adds no loop continuation or automatic retry.
- Contract Week's narrow catch maps/rethrows the known error; its existing expected-ID/signature request, evidence warning, actual affected-row refresh and mutation-in-flight `finally` remain intact.
- Whole-bulk known errors are mapped/rethrown in the existing wrapper. After original operation polling, both `results` and explicit `failed_items` arrays receive the same actor-qualified presentation. Success flags, counts, original row identities, operation IDs, signatures, stale classification and durable operation polling are unchanged.
- The active `handleBulkAuthoriseSelected` path captures its actor before the POST and partitions only visible known NEXT busy rows into “Waiting — try again in a moment.” Each waiting row receives the exact sentence. Other errors remain in the existing error group. Mixed successful/refused/waiting results retain actual unsuccessful counts and refresh/selection behavior; busy is not a visible “Failures” entry. Internal `failure_count`/`failed_items` names remain truthful existing transport bookkeeping, not a new success claim.

## Caller and older-path census

Current selected bulk dispatch calls `authoriseSelectedTimesheets` directly (around `main.js:261055`) and builds the corrected completion summary (around 261169). Its row action uses the same handler. The actual wrapper POST remains `/api/timesheets/bulk-authorise-selected`; single and Contract Week endpoints remain their existing authorisation routes. No new endpoint or Worker authority is introduced here.

The old `runBulkTimesheetSelectionChunks` helper (18632) and three same-name `openBulkTimesheetActionProgressModal` definitions were inspected. The helper passes through mapped executor arrays, but its dormant progress text at 18915–18917 and the final modal's generic failure labels are unchanged. A census of current application `.js`/`.html` files, excluding dependencies/tests, found **no call site** for the chunk helper: only its definition, internal error strings and its own progress-modal call. These are not the active selected-bulk UI path; no unrelated activation/rewiring is requested by this source review. If a later card activates that helper, its visible busy classification must be supplied then. Existing unauthorise summaries are outside SP-03.

There is no current `normalizeRawFailures` helper; the nearby helper is `collectFailedKeys`. Server legacy row adapters and their finite code retention remain D03's independent review responsibility. This file does not infer them correct merely because a request reaches the intended DB owner.

## Assets and source receipts

The actual `index.html:3051` main-script URL includes `bpay-next-source-authorise=20261010-r1`. The Bible records only local source behavior and explicitly leaves joined acceptance NOT_RUN. The complete current FE diff is confined to this main presentation support, its asset marker and that note.

| File | Fresh SHA256 |
| --- | --- |
| `js/main.js` | `4e65bb35b89748c74c60eda943cb6564c23c2b7b9182bc774722bc543f104fa0` |
| `index.html` | `b84f7bd9fc2f4c69cecc99e480f9351fbb6bcb7a21890c7efbdd974ee9cc787f` |
| `BANKING_PAY_BIBLE.md` | `9a53dabc59af28be85e4dc1d3b5ad1627f74bb9ba6bdb01b0742d361f47d9977` |

No actionable D04 finding remains in these saved bytes. This source verdict does not establish the real busy/retry, initial 7.5-hour WORK position, permission-negative, open-review, missing-signature allowance or paid-read-outage journeys; those are the card's future joined observations.
