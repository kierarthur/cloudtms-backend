# SCR-01 — Banking Pay header: independent42-point review

Verdict: **PASS — all applicable WB11 checks.** 31 Pass / 0 Fail / 11 Not applicable.

Independent read-only review, 10 October 2026. Owner A4: actual full saved frontend index/main standard modal and actual CloudTMSBpayNextOffice.open controller entry and Workbench component, fixed local read responses, network blocked by the author's capture script. I inspected the saved PNG pixels with view_image and read the actual adapter/shell/controller source; I did not run the app, tests, SQL, browser scenarios, pack tools, generation or rendering. Pack remains unchanged.

Scope is WB11 header/filter/scoped pickers only. Ready rows, selection/ticks, Create Draft, Export, Reset selection and Clear decisions belong to later named cards and are deliberately omitted. Owner A5 defers the earlier WB13 paged issue body until WB14. Owner A3 hides all pay dates before execution, removes main Waiting and requires dark appearance. Item16 is manual policy.json wording review; item25's generated-spec programme and readerAdditions under item26 are N/A under owner A4. Item26's real-field check remains applicable.

PASS means satisfied by named visual/source evidence; N/A means genuinely outside this screen/current owner scope. Source evidence is not a claim of executed keyboard, hundreds-of-rows, failure or business tests. Any FAIL blocks this screen's approval.

## Current findings and closure

All prior H01–H13 findings affecting this screen are closed. Fresh final actual Office-entry desktop pixels confirm the wide frame spans x140–1460, exactly1320px, and the normal empty notice is absent. Failed refresh retains grey £2,746.50 /2 with the visible plain retry notice. The390px phone preserves the header, tabs, facts, scope and actions without overflow.

This final table supersedes every earlier pre-correction FAIL snapshot. All16 final saved product images across SCR01–03 were individually inspected after the last corrections. No approval extends to the later Ready/payment/Draft bodies or the owner-A5 deferred WB13 body.

##42 checklist results

| # | Result | Evidence / applicability |
|---|---|---|
| 1 | PASS | All current header capabilities and direct Filters/Refresh entry are reachable; displayed pay date is owner-overridden. |
| 2 | PASS | Named legacy filtering/navigation capability is retained through current read-only UI; no new financial work. |
| 3 | PASS | Banking Pay/Ready/Action required/Blocked/Filters/Choose labels follow current wording. |
| 4 | PASS | Prepared server values are displayed; no local revalue, money action, session or rebuild. |
| 5 | PASS | H12 closed: final actual standard main frame is1320px wide (x140–1460); real title, sole Close and phone frame are retained. |
| 6 | PASS | One main tab row; Filters and picker children contain no extra tab row. |
| 7 | PASS | Simple header/facts/actions or field/list/card order; one finite filter count card, no nested subject cards. |
| 8 | N/A | Ready data table belongs to WB12; current facts use tabular numbers. |
| 9 | PASS | Channel/filter chips and counts have text; no unsupported status badge or colour-only meaning. |
| 10 | N/A | Current header has Filters/Refresh; later primary payment action is deliberately absent. |
| 11 | PASS | New WB11 styles use shared modal tokens/classes and add no !important/frame palette. |
| 12 | N/A | Current header has no list; reachable new pickers independently satisfy scrolling in SCR03. A5 issue body deferred. |
| 13 | N/A | Picker/dialog is explicit sorting exception; no current WB11 main data columns. Client ordering is not invented. |
| 14 | N/A | Tick/select-all/selection body belongs to WB12; no fake exact browser count. |
| 15 | PASS | HEADER and filter card use whole server scope; no browser sum or loaded-row total. |
| 16 | PASS | Manual tools/policy.json review: no prohibited term/chatter; short plain messages, pounds/commas/pence. |
| 17 | PASS | Actual failed-refresh PNG now visibly says Could not refresh payments; try again. Refresh is reachable. |
| 18 | N/A | Read-only scope/picker actions require no financial confirmation. |
| 19 | PASS | Actual failed-refresh shows retained grey £2,746.50 and2, no false zero; initial unknown source uses dash. |
| 20 | PASS | No loan, UUID, error code or internal terminology in current WB11 visible text. |
| 21 | PASS | £2,746.50/£1,284.50 style matches exact response strings; no dates or invented values displayed. |
| 22 | PASS | Real standard frame lifecycle; tab arrows, search, row arrows/Enter, retry/Close/Apply/Choose keyboard paths; visible focus. |
| 23 | PASS | 390×844 header wraps controls and preserves Close; no overflow. Current data cards/sort are later WB12. |
| 24 | PASS | Device-light pixels use the same dark theme/layout; focus/caret differences do not alter the theme. |
| 25 | N/A | Owner A4 requires actual product PNG+minimal samples+review, superseding generated spec/tools programme. |
| 26 | PASS | Visible values map to HEADER candidate_count/amount and genuine picker fields; readerAdditions N/A. |
| 27 | PASS | No official/pay date appears in any header/filter/picker. |
| 28 | PASS | Visible plain payroll labels, names, TMS refs, channel, counts and pounds are understandable. |
| 29 | PASS | Filters/Refresh beside facts; picker Search/list/Choose and count retry beside affected content. |
| 30 | N/A | Main Ready payment-row action belongs to WB12/WB21; Filters/Refresh are clear current controls. |
| 31 | N/A | Full who/why payment rows await WB12; exact current header answers Ready amount/count/channel. |
| 32 | N/A | No new WB11 Waiting/Blocked/Action row; A5 older issue body is deferred to WB14. |
| 33 | PASS | Apply/Clear/Choose update committed filter scope/count/chip; no toast or money dispatch. |
| 34 | PASS | H11 closed: actual normal images no longer expose the empty notice; unnecessary later controls are omitted. |
| 35 | PASS | Candidate/Client/PAYE/Umbrella/Banking Pay/Filters names are consistent. |
| 36 | PASS | All actions are bounded reads/local filter edits; no irreversible or payment click. |
| 37 | PASS | Header Close/Escape returns original parent; un-applied filter draft is discarded safely. |
| 38 | PASS | Direct Filters→field/search→Choose→Apply flow; All/Clear avoid detours. |
| 39 | PASS | Rendered£2,746.50/2 matches HEADER fixed sample; five read responses compare actual common context. |
| 40 | N/A | Create Draft confidence requires later WB12/WB21 UI; absent control is intentional. |
| 41 | PASS | Actual phone and failed-refresh pictures inspected; source bounds/keyboard/unknown lifetime checked, no business/scenario execution. |
| 42 | PASS | Banking Pay title, active tab, channel/filter chip or picker subtitle preserve visible context. |

## Actual artifact evidence

Listed normal PNGs were individually inspected, including both Candidate and Client variants. Desktop1600×1000, phone390×844. Desktop dark/device-light retain the same dark theme; the Candidate pair is byte-identical, while Client focus appearance differs without a theme/layout change. Failure/empty pictures were individually inspected where listed. This is visual and source review, not DB/test/timing/financial execution.

| File | SHA256 |
|---|---|
| dark.png | c0c3accde94cdea4d2c2d8fbae442f770fdec2327003ac9cf40fff2915510245 |
| device-light.png | c0c3accde94cdea4d2c2d8fbae442f770fdec2327003ac9cf40fff2915510245 |
| failed-refresh-dark.png | 3707569a300a39d8f5f8511810129d16aec9fcdfb2ef1b770a785ecee36ba5ac |
| fixed-read-responses.json | ac92a96dbddb756b19877266f7e942b8dc0e227ac32110034022c65f57af2644 |
| phone-dark.png | f163c835a622b12fc8783bd506da634c8caba1150c1be616888917698c883052 |

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
