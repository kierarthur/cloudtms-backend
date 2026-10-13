# SCR-03 — Choose Candidate / Choose Client: independent42-point review

Verdict: **PASS — all applicable WB11 checks.** 34 Pass / 0 Fail / 8 Not applicable.

Independent read-only review, 10 October 2026. Owner A4: actual full saved frontend index/main standard modal and actual CloudTMSBpayNextOffice.open controller entry and Workbench component, fixed local read responses, network blocked by the author's capture script. I inspected the saved PNG pixels with view_image and read the actual adapter/shell/controller source; I did not run the app, tests, SQL, browser scenarios, pack tools, generation or rendering. Pack remains unchanged.

Scope is WB11 header/filter/scoped pickers only. Ready rows, selection/ticks, Create Draft, Export, Reset selection and Clear decisions belong to later named cards and are deliberately omitted. Owner A5 defers the earlier WB13 paged issue body until WB14. Owner A3 hides all pay dates before execution, removes main Waiting and requires dark appearance. Item16 is manual policy.json wording review; item25's generated-spec programme and readerAdditions under item26 are N/A under owner A4. Item26's real-field check remains applicable.

PASS means satisfied by named visual/source evidence; N/A means genuinely outside this screen/current owner scope. Source evidence is not a claim of executed keyboard, hundreds-of-rows, failure or business tests. Any FAIL blocks this screen's approval.

## Current findings and closure

All prior H01–H13 findings affecting this screen are closed. Final Candidate and Client desktop/device-light/phone pixels visibly show Choose a Candidate. /Choose a Client. beside disabled Choose when the nonempty list has no selection; source hides that hint once selected/error/loading/empty. Failed and empty lists display their own reason and do not permit Choose. Typed stale handling exposes Refresh and clears old pages/history/context/pick for a user-triggered reset; transient failure exposes Try again, continuation says Loading more…, and errors use role=alert.

This final table supersedes every earlier pre-correction FAIL snapshot. All16 final saved product images across SCR01–03 were individually inspected after the last corrections. No approval extends to the later Ready/payment/Draft bodies or the owner-A5 deferred WB13 body.

##42 checklist results

| # | Result | Evidence / applicability |
|---|---|---|
| 1 | PASS | Scoped Candidate/name+TMS search and Client/name search, All option, Choose, row Enter/double-click and phone Open. |
| 2 | PASS | Named legacy filtering/navigation capability is retained through current read-only UI; no new financial work. |
| 3 | PASS | Choose Candidate/Client, Search name or TMS ref/Search Client, All/Choose/Open and plain errors. |
| 4 | PASS | Prepared server values are displayed; no local revalue, money action, session or rebuild. |
| 5 | PASS | Actual600px standard picker child with scope subtitle and sole header Close; source exact top-frame identity. |
| 6 | PASS | One main tab row; Filters and picker children contain no extra tab row. |
| 7 | PASS | Simple header/facts/actions or field/list/card order; one finite filter count card, no nested subject cards. |
| 8 | PASS | Actual universal table/list with sticky heading; Candidate name/TMS and Client name. No picker money. |
| 9 | N/A | Picker has no status badge; active row uses textual/name context and aria-selected. |
| 10 | PASS | H13 closed: one primary Choose, secondary All, and visible Candidate/Client hint explain the disabled unselected state; errors/empty give their own reason. |
| 11 | PASS | New WB11 styles use shared modal tokens/classes and add no !important/frame palette. |
| 12 | PASS | H08 closed: source continuous bounded forward/back reads say Loading more…, no paging buttons. |
| 13 | N/A | Picker/dialog is explicit sorting exception; no current WB11 main data columns. Client ordering is not invented. |
| 14 | N/A | Tick/select-all/selection body belongs to WB12; no fake exact browser count. |
| 15 | N/A | No invented result total or per-Client Candidate count; actual rows are existing reader values. |
| 16 | PASS | Manual tools/policy.json review: no prohibited term/chatter; short plain messages, pounds/commas/pence. |
| 17 | PASS | H07/H08 closed: transient Try again repeats exact page; typed stale shows Refresh and resets context/pages/history/pick to null cursor; errors role=alert. |
| 18 | N/A | Read-only scope/picker actions require no financial confirmation. |
| 19 | PASS | Actual failed-picker/empty-picker show plain error+Try again / No matches.; Choose disabled; no money or false zero. |
| 20 | PASS | No loan, UUID, error code or internal terminology in current WB11 visible text. |
| 21 | PASS | Names and TMS refs retain text, nullable missing names use plain Candidate/Client; no dates/money shown. |
| 22 | PASS | Source search/row/focus keys; ArrowUp/Down/Home/End/Enter, double-click, phone Open, Choose/All/Close and Refresh; error blocks stale selection. |
| 23 | PASS | 390px stacked name/ref/Open rows fit without sideways scroll; primary reachable; dialog Sort by is exempt. |
| 24 | PASS | Device-light pixels use the same dark theme/layout; focus/caret differences do not alter the theme. |
| 25 | N/A | Owner A4 requires actual product PNG+minimal samples+review, superseding generated spec/tools programme. |
| 26 | PASS | Candidate candidate_sort_name+tms_ref and Client client_display_name map to real named typed readers; UUIDs internal, no added fields. |
| 27 | PASS | No official/pay date appears in any header/filter/picker. |
| 28 | PASS | Visible plain payroll labels, names, TMS refs, channel, counts and pounds are understandable. |
| 29 | PASS | Filters/Refresh beside facts; picker Search/list/Choose and count retry beside affected content. |
| 30 | PASS | Search then click/Choose or phone Open is obvious; All removes just current dimension. |
| 31 | PASS | Identifies genuine Candidate or Client and current channel/filter subtitle; no payment-row economics demanded of read-only picker. |
| 32 | N/A | No new WB11 Waiting/Blocked/Action row; A5 older issue body is deferred to WB14. |
| 33 | PASS | Choosing/All closes exact child and updates parent label/count; no toast or background payment. |
| 34 | PASS | Only current necessary header/filter/picker fields shown; unsupported figures/later controls omitted. |
| 35 | PASS | Candidate/Client/PAYE/Umbrella/Banking Pay/Filters names are consistent. |
| 36 | PASS | All actions are bounded reads/local filter edits; no irreversible or payment click. |
| 37 | PASS | Close leaves parent draft unchanged; Choose/All changes only current field, then Apply decides parent scope. |
| 38 | PASS | Direct Filters→field/search→Choose→Apply flow; All/Clear avoid detours. |
| 39 | PASS | Displayed Amelia/Bethany/TMS and Riverside/Southbank match exact typed fixed rows; no invented row counts/money. |
| 40 | N/A | Create Draft confidence requires later WB12/WB21 UI; absent control is intentional. |
| 41 | PASS | Actual Candidate+Client phone, failure and empty images inspected; source bounded3×100 rows/32768byte history, stale reset and keyboard paths checked. No hundreds-row execution claimed. |
| 42 | PASS | Banking Pay title, active tab, channel/filter chip or picker subtitle preserve visible context. |

## Actual artifact evidence

Listed normal PNGs were individually inspected, including both Candidate and Client variants. Desktop1600×1000, phone390×844. Desktop dark/device-light retain the same dark theme; the Candidate pair is byte-identical, while Client focus appearance differs without a theme/layout change. Failure/empty pictures were individually inspected where listed. This is visual and source review, not DB/test/timing/financial execution.

| File | SHA256 |
|---|---|
| candidate-dark.png | 67cbe8b4bed34cba1473dd50db2c84237a5167b1188e30de34311bec6fd69ea9 |
| candidate-device-light.png | 67cbe8b4bed34cba1473dd50db2c84237a5167b1188e30de34311bec6fd69ea9 |
| candidate-phone-dark.png | b65cce83061968fa98fc9751ca1977440d752acaabe6f02cb2a80f7b10f1ca15 |
| client-dark.png | bd2625ff0d9a6d84d756c8793591e5511b749152468d9fb691105ca1a61b4c94 |
| client-device-light.png | 98c22b0043440d1168923f11153e851419d907c5e3a1fef8828c0d336a699aaf |
| client-phone-dark.png | 815f06facf6a1d2a6c786533f49484671419a9b6305d402bc443d5f0fb3b9d5c |
| empty-picker-dark.png | 574d585becd2837e8ccc485baa1e9241afdb8ce66c9494a92ead288a8e34d903 |
| failed-picker-dark.png | 85bf17bcee8be890d8af42f52caa014f20fc8f99937ff9215540c850eff90d6e |
| fixed-read-responses.json | 0931abc1753fee7d733d76cdc9d940f8abe6c315d1e1167bd76c5cfba795aadb |

## Source snapshot

| File | SHA256 |
|---|---|
| FE/js/bpay-next-workbench.js | 2208826185c9947f0d010e806e4c98ef5095be4b042b818ac88a123462e788af |
| FE/js/bpay-next-office.js | 34ec7991ec930be29f260b71522ab7d01c370f7430742e09b9755a972769efff |
| FE/js/bpay-next-ui.js | dbdcb12ed8ec027b0df39c52ef6f50b6d91ca93e2cb74451a59e09a6ba029272 |
| FE/js/main.js | 7cde52d8f113f9c35f19f2f0be198390e2d742118c991b98e29ca06dcc645845 |
| FE/js/modal-modernisation.js | 9bf06878d57960ae82e1967e71452ceadec0cf3a603c946effc1df49d88df36e |
| FE/css/bpay-next-office.css | 52203a80b9dbc464d3d87f7d02cdfe3e4d6ce3f2c59fa253e96dd66dad85e145 |
| FE/index.html | d67ad31b78c1f66de0bd9ece500f64d1496e2c2810179fd4c88f4ce7910d9f91 |

Manual wording source: read-only pack tools/policy.json version1.2. Its neverShow/chatter/20word/2sentence/UK-format rules were checked against the current visible labels and finite loading/error/empty copy; no tool ran. Existing legacy !important CSS is outside new WB11 styles; no new one was added. Actual modal-modernisation support skips only explicitly preformatted exact money, retains marked live-copy placeholders and recognizes this read kind as specialised layout. Unknown values are not hidden zeros. Current-frame/current-actor/positive NEXT checks guard reads, results, child actions and focus restoration.
