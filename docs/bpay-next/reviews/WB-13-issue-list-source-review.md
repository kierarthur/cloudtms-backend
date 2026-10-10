# WB-13 issue-list source review

Verdict: SOURCE PASS for the WB13 issue-list component and its current Office/existing-record wiring. No open source finding remains in this reviewed subset. App, browser, timing and joined WB14 decision-window behavior: NOT_RUN. This does not close the whole WB13 item or its later owner joins.

Reviewed scope: full WB13 card `02_WORKBENCH.md:541-589`, all six WB13 deliverables, R2 amount provenance and bounded-page requirements, and `design/WB-13-scope-investigation.md`. Review reads saved source only; no product changes or code/SQL/tests executed.

## Final closure of finite findings

1. **CLOSED — external component actor/owner lifetime.** Fresh `js/bpay-next-workbench.js:602-604` checks `current()` first in `setScope`, `adoptAffected` and `invalidate`; stale/closed calls refuse before state changes, callbacks or render. `close` captures current ownership, clears the bundle and calls availability only while captured current. Each read await and finally retains its independent generation/actor/owner guard.
2. **CLOSED — existing-record fallback reachability.** Fresh `js/main.js:7169` closes the Office sheet before either existing Timesheet/Candidate modal owner is opened. This follows the existing close-before-record pattern and avoids placing the ordinary modal underneath Office z-index 100100. The future WB14 branch retains its parent and remains a separate, explicitly open join. This interim record fallback does not implement the future decision controls or child return receipt.

## Reviewed source behavior

| Requirement | Source finding |
| --- | --- |
| Two coherent section badges plus page | `sameRevision` compares agency, actor, epoch, official pay date, Workbench revision and adoption revision. The page, Action HEADER and Blocked HEADER are joined before a single bundle adoption; each await rechecks generation and current actor/owner. Mismatch is a typed stale-cursor refusal with an explicit Reload. |
| Finite processing/memory | One 100-row page is retained; headers are bounded single-row reads. Previous cursor history is limited to 32768 encoded bytes; discarded history is disclosed and First page returns to the start. No automatic pagination, page retry, all-row cache or financial calculation. |
| Exact retry and position | `pending` retains cursor/reset/previous-page/expected revision and original scroll value. Try again reuses them. Search/sort/kind changes start a new list; a stale revision requires explicit reload. Actual `.bpay-next-issue-table` is bounded at 45vh with overflow:auto. |
| Amount provenance and unavailable money | Nominal, last-approved amount, deduction balance, active reservation and recovery are distinct stored fields. Paid/withdrawn nominal is labelled last approved SOURCE amount; NET_ADD/NET_DEDUCT approved/reserved facts are labelled NET adjustment. Missing current amount says Unavailable, never GBP0. No summing, NET ratio, headroom estimate or current-rate reconstruction. |
| Native reasons | Hold text uses the actual hold reason; pay-channel direction uses actual source/target channels; Future Week uses the stored date. Paid awaiting re-authorisation, query unavailable/open, recovery paused, in preparation and snoozed states have explicit labels. Unknown reasons retain a review message. Bank warning is not invented as a blocked-pay reason. |
| Visible table/keyboard | Both lists have exactly five columns and explicit empty copy; Action introduction is the card wording. Search is capped at 200, native sort choices are finite, stable typed row order comes from the existing reader. Native buttons, row Enter, tab keyboard navigation, alert/live status, focus keys and horizontal table scrolling are present. Actual 375px/theme/keyboard execution remains NOT_RUN. |
| Owner/actor binding | Mandatory actor UUID/currentActorId and ownerReady are checked even when a client is supplied. Fresh actor mismatch after a read removes the old component without availability callback, render or focus. The external component methods also refuse stale lifetime before state/callback/DOM changes. |
| Office availability | A persistent issueHost is mounted through existing capabilities admission. Availability false disables preparation controls before an unsealed selection. Office close closes the component. Main exposes exact retained physical Timesheet/Candidate record IDs as the interim fallback and closes the Office layer before opening the record. Future WB14 child controls are not represented as complete. |
| Supporting scope | The eight typed issue fields are a necessary reader/Worker/FE projection support for truthful amount labels. No classifier, money owner, producer, schema rewire, Rule B accepted-input change or parked F25 protocol is part of this component review. |

## Corrections independently reread

The initial UTF-8 defect is corrected; the reviewed files decode as UTF-8. Initial wrong unpaid-difference label, stale badge display, missing per-await guards, actor-change finally callback/render, missing supplied-client actor validation, hard-coded gross label for NET fields, missing scroller, silent bounded-history truncation, external method lifetime and record fallback layering are closed in the fresh source. Original retry scroll is retained. Earlier checkpoints are history, not the current authority.

## Final snapshot SHA-256

- `js/bpay-next-workbench.js`: `aa3215c714152310ccd26f8430353ccafa4ed6c3e6adebe2ce72e61bd58f6563`
- `js/bpay-next-office.js`: `a1b901e812309bed55f368e23a30c213f2ec82f35590459d971aee7bbebdeacd`
- `js/main.js`: `366b867bc828d6014ed23399f85c770d64929724405151232a4390f99b561759`
- `css/bpay-next-office.css`: `a2acb9f831f878aaaad31aec450c9504747e0ed00581f8ccc5abf81724670444`
- `index.html`: `91f6a8720be153470278f282c80cf4b00149c09525a682d32f968cf7c4a211b9`

The three existing reader kinds remain typed; the source component joins only one page and the two same-revision section headers. WB14/WB17 actual decision-window/return joins and later mutation owners remain open in their own ordered cards. No new proof framework, fixtures, runtime scenario or testing programme was added by this review.
