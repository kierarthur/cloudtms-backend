# WB-11 independent design/scope review — final A4 PASS, 10 October 2026

Verdict: **PASS — exact current WB11 scope, source and all applicable actual-screen checks.** No actionable finding remains. This final review supersedes the earlier source-preparation and pre-correction visual verdicts.

Full WB11 card02:464–497, all seven checklist rows, owner BD067/A3/A4/A5, SCREEN_DESIGN_POLICY and indexed SCR01/T01, SCR02 and SCR03 designs were reviewed. The read-only pack was not edited. The reviewer inspected all16 final saved actual product images through view_image: SCR01 normal dark/device-light/390px phone and failed refresh; SCR02 normal dark/device-light/phone and failed count; SCR03 Candidate and Client dark/device-light/phone, failed picker and empty picker. The author’s capture uses the real frontend index/main, standard modal and actual CloudTMSBpayNextOffice.open entry with only minimal fixed read responses and blocked hosted network. The reviewer did not execute the application, browser scenarios, tests, SQL/DB, timing, rendering, generators or pack tools.

Each screen’s complete42-point POLICY CHECKLIST and user-eyes review is saved independently:

| Screen | Applicable PASS | FAIL | Not applicable | Review |
|---|---:|---:|---:|---|
| SCR01 Banking Pay header |31|0|11| `docs/bpay-next/screen-designs/SCR-01/review.md` |
| SCR02 Filters |33|0|9| `docs/bpay-next/screen-designs/SCR-02/review.md` |
| SCR03 Candidate / Client pickers |34|0|8| `docs/bpay-next/screen-designs/SCR-03/review.md` |

N/A is a scope decision, not an omitted failure. Current WB11 contains the header/filter/scoped pickers; Ready rows, selection/ticks/count, Create Draft, Export, Reset selection and Clear decisions remain their later named owners and are omitted without placeholders. Owner A3 removes pay dates before execution and main Waiting, requires dark appearance, continuous scrolling and row/phone opening behavior. Owner A5 specifically retains the older WB13 paged issue body until WB14; this review does not approve or rewrite that body. Item16 is manual tools/policy.json v1.2 wording review. Item25's generated-spec programme and readerAdditions under item26 are N/A under A4; actual reader-field mapping under item26 passes.

## Exact source and screen closure

The new shell and children use the real standard modal/frame/Close/Escape stack and shared tokens. Main desktop spans x140–1460, exactly1320px; children are600px, with390px phone layouts retaining accessible controls. No newly added !important or alternate frame palette remains. Desktop device-light settings retain dark styling. Header shows Banking Pay, Ready/Action required/Blocked, exact server Ready amount/count and channel/filter scope; it does not invent dates, updated-at values, money or missing counts. Failed refresh keeps the last same-scope figures grey with a plain visible retry notice, and initial unknown/failed count stays a dash.

Filters use actual native radios, two scoped search pickers and whole server count/amount; PAYE only radio and1 Candidate /£1,284.50 card agree in the final pixels. A count failure supplies Try again and disables Apply. Pickers use genuine Candidate name/TMS reference or Client display name, bounded continuation, click selection/highlight, arrow/Enter handling and phone Open. An unselected nonempty list visibly explains disabled Choose; failed/empty lists supply their own reason. Source guards block stale choices during error/loading; typed stale continuation explicitly resets only that picker’s context/pages/history/pick via Refresh. Generic transient retry retains its actual pending read. There is no business dispatch or automated money retry.

The actual reader/transport remains unchanged: existing1634 service, closed decoder and common-context comparison. Visible counts/amounts come from HEADER; no browser sum/revalue/financial command/session/rebuild/producer is introduced. Current actor, positive NEXT owner, exact frame and disposal guards protect reads, results, child actions and focus. Supporting modal changes are finite: preserve explicitly preformatted exact money, preserve marked live-copy nodes and recognize this read kind as specialised. Index changes are cache URL markers/static dependency order; no body/protocol/auth/money contract change.

| Earlier finding | Final evidence / resolution |
|---|---|
|H01 standard frame/styles|Actual Office entry uses standard modal, shared tokens and scoped CSS; final normal/phone frames fit.|
|H02 picker failure actionable stale choice / keyboard retry|Error blocks Choose/Open; real retry button and finite stale Refresh; source keyboard paths.|
|H03 missing count retry|Actual failed-count picture has Try again and disabled Apply.|
|H04 channel filter summary|Channel-only active predicate and actual PAYE only scope are visible.|
|H05 arrows / first focus / radio target|Finite source row arrows/Home/End/Enter, standard useful first focus,44px radio labels; phone fits.|
|H06 lost figures on refresh failure|Actual failed-refresh retains grey last values; changed scope/actor/owner still clears them.|
|H07 obsolete cursor retry|Typed stale reset clears only picker context/pages/history/pick; user Refresh starts null cursor.|
|H08 continuation / error role|Loading more… and role=alert in source.|
|H09 radio transition artifact|Final stable pixels show selected PAYE only and matching count card.|
|H10 missing live error text|Marked live-copy survives standard cleanup; final failure notice visible.|
|H11 empty alert bar|Scoped hidden notice rule; final normal header has no empty bar.|
|H12 wrong main width|Final pixels prove1320px; higher read-kind specificity without !important.|
|H13 disabled Choose reason|Final Candidate/Client desktop and phone show contextual Choose a Candidate./Choose a Client. hints.|

## Final source snapshot

| File | SHA256 |
|---|---|
| FE/js/bpay-next-workbench.js | 2208826185c9947f0d010e806e4c98ef5095be4b042b818ac88a123462e788af |
| FE/js/bpay-next-office.js | 34ec7991ec930be29f260b71522ab7d01c370f7430742e09b9755a972769efff |
| FE/js/bpay-next-ui.js | dbdcb12ed8ec027b0df39c52ef6f50b6d91ca93e2cb74451a59e09a6ba029272 |
| FE/js/main.js | 7cde52d8f113f9c35f19f2f0be198390e2d742118c991b98e29ca06dcc645845 |
| FE/js/modal-modernisation.js | 9bf06878d57960ae82e1967e71452ceadec0cf3a603c946effc1df49d88df36e |
| FE/css/bpay-next-office.css | 52203a80b9dbc464d3d87f7d02cdfe3e4d6ce3f2c59fa253e96dd66dad85e145 |
| FE/index.html | d67ad31b78c1f66de0bd9ece500f64d1496e2c2810179fd4c88f4ce7910d9f91 |

Per-screen reviews contain the fresh SHA256 of every actual PNG and fixed-read response JSON. Fixed samples now explicitly include factual capabilities, empty issues, a typed503 failure and empty picker where relevant; they contain only artificial local render values. Client dark/device-light focus-ring appearance differs but both remain dark with the same layout. These are visual/source findings only; no application, business, DB or300-row timing verification is inferred.
