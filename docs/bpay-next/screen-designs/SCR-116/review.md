# FC-03 shared Preview/Apply — independent A4 review

11 October 2026. Scope is the reusable FC-03 panel/footer in the real native Office host, not the future FC-04 creation form or FC-01 list. Read-only review: supplied PNG/spec/policy were inspected without copying or changing them. All 21 actual PNGs were individually viewed, then individually reopened after capture loop 40992. No screenshot, browser journey, fixture, SQL, database, hosted call or test was created or executed by this reviewer.

**Checkpoint verdict: FAIL, pending replacement phone pixels.** All fourteen desktop/device-light pictures pass. All seven phone pictures have correct facts, wrapping, current error/recovery copy and next-step controls, but header Close and footer buttons still appear about 36px tall rather than the required 44px. The original class-only CSS correction did not affect those pixels. Root has now saved a narrow `#modal.ctms-modern-modal.ctms-kind-bpay-next-read` rule; fresh phone captures must demonstrate its effect before this visual failure is closed. Earlier two-sentence error copy and the pending “Preview first” message are fixed in these 21 pictures.

## Supplied design and every visible difference

The supplied `SCR-116_preview_apply_dark.png` was viewed beside the actual preview, with its matching spec and B9 `tools/policy.json`. Differences were checked against `FC03-supplied-design-differences.md`, FC-03 D09 and the owner directions.

1. The supplied picture includes the future creation form (Candidate reference/channel, amount, repayment method, start date, threshold, floor and note) and a blurred future Case list. Those surrounding forms, inputs, menus and list totals are omitted from this component-only batch. The actual native host uses the minimal caller subtitle “Bethany Clarke · £300.00 · 6 weeks”. This is the card boundary, not a replacement design for FC-04. The native background remains blurred and the active window remains clear.
2. The actual window is shorter and vertically centred because it contains only the shared panel/footer. Its native width/header/Close arrangement, dark colours, Preview facts, amber warning and footer hierarchy match the supplied component. The supplied export caption is not a product control and is absent. The panel uses radius 13 and footer gap 8.
3. First/last repayment dates are 12/10/2026 and 16/11/2026, rather than the supplied sample's 19/10/2026 and 23/11/2026. These are the card's six-week example and the actual returned schedule dates. Weekly £50.00, six weeks and total £300.00 remain consistent.
4. The warning says “This Candidate already has repayments.” The native fact is EXISTING_REPAYMENTS. The reader does not supply the illustration's £150 weekly aggregate, so that quantity and the repeated Candidate name are omitted. The warning is amber and Apply remains available after a successful Preview.
5. Initial/changed input, failed Preview, unresolved command, accepted readback pending and failed readback add one inline state sentence. The actual error strings use a semicolon within one sentence, as required by the owner wording correction. An unresolved command says “Could not confirm the result; try again.” Accepted-but-not-yet-visible state uses Refresh and does not claim completion.
6. Phone keeps the same facts in two columns, with the threshold label wrapping and no horizontal clipping. Its footer contains only the relevant existing actions. No list, Sort by or pagination control belongs here. At this checkpoint the phone target height is the sole unresolved visual defect.
7. Device-light retains the same dark screen. Six desktop pairs are byte-identical; the refresh-error pair has a small capture-byte difference, but both were individually inspected and no visual theme difference was found.

Every control is needed: **Preview** obtains the server consequence for the current input; **Apply** sends only that successfully previewed input; **Try again** reuses the exact unresolved command; **Refresh** reads the accepted Case without another mutation; **Close** exits safely while preserving its existing journal. There is no extra Expand, export, paging, technical diagnostic, money calculation or pay-date control.

## All 42 policy checks

Status is for the individually viewed loop-40992 pictures and current source, with the phone correction awaiting pixels. “N/A” is a scoped applicability decision, not a missing delivery. Check 25 and the readerAdditions part of 26 are N/A under the explicit owner A4 direction. Wording/keyboard/busy mechanics were read in existing source; this reviewer did not run the policy tools or a keyboard/browser test.

| # | Policy wording | Verdict and evidence |
| --- | --- | --- |
| 1 | Every capability in owning cards' User can / Screen bullets is reachable | PASS — shared Preview/Apply, invalidation, exact retry and targeted readback are present; surrounding future forms remain later-owned. |
| 2 | Every cited legacy capability / LUI id is reachable | N/A — no legacy window replacement in this shared component. |
| 3 | Card wording, rewritten only where SDP-70 requires | PASS — Preview, Apply and Payment Advance; owner one-sentence errors override illustrated wording. |
| 4 | Section B behaviour, warnings not blocking | PASS — server figures only; changed inputs invalidate Preview; repayments warning does not disable valid Apply. |
| 5 | Standard frame/title/subtitle; Close only in header | PASS — native small host and meaningful caller subtitle; no footer Close duplicate. |
| 6 | At most one tab row; second level segmented | PASS — no tab row added. |
| 7 | Body order/cards; no box in a box | PASS — one Preview card, factual warning and native footer. |
| 8 | Universal sticky table; money right-aligned/tabular | N/A — a supplied definition-list panel, not a table/list. |
| 9 | Five badge tones correctly, with text | PASS — amber factual warning and neutral status/error text; no false success badge. |
| 10 | One primary next step; at most two other footer controls; destructive left | PASS — Apply/Try again/Refresh is the current primary, Preview secondary; no destructive action here. |
| 11 | Listed SDP pieces only; no !important; policy tokens | FAIL at checkpoint — radius/gap and scoped styles are correct, but captured phone targets do not yet meet policy 44px. |
| 12 | Continuous scroll / Loading more; no paging | N/A — no list. |
| 13 | All data headings sort; active arrow/default | N/A — no table columns. |
| 14 | Tick first / three-state / whole matching select-all count | N/A — no selection list. |
| 15 | Whole-list server counts/totals | N/A — no list aggregates. |
| 16 | check-wording.cjs passes; plain short wording / UK formats | PASS by manual wording/source inspection — no forbidden technical copy; one sentence, short messages, UK dates and pounds; no tool-run claim. |
| 17 | One plain error sentence and what to do, inline | PASS — failed Preview/readback and unknown command each name their immediate recovery action. |
| 18 | Confirmation title/consequence/two safe-first buttons | N/A — no separate destructive confirmation. Preview provides the consequence before Apply. |
| 19 | Empty/loading/failed-read defined; failure never £0.00 | PASS — initial/changed state, busy source states, failed Preview/readback and accepted pending are distinct; absent Preview does not invent money. |
| 20 | Payment Advance, never loan; no codes/IDs visible | PASS — plain product wording throughout viewed screens. |
| 21 | DD/MM/YYYY / 24h / £ formats; missing dash/plain state | PASS — actual dates and £50.00/£300.00; None and Candidate default are meaningful native states. |
| 22 | Keyboard/focus/roles/aria and main-row entry | PASS for applicable controls by source — native buttons, focus handling and live result role retained; table/row aria parts N/A. |
| 23 | Phone cards/no sideways scroll/Sort by/primary reachable | FAIL at checkpoint — no clipping and primary reachable; list-only parts N/A, but Close and action targets remain visibly below 44px. |
| 24 | Device-light identical dark | PASS — all seven pairs individually inspected; same dark theme. |
| 25 | Folder spec/page/PNG regeneration unchanged | N/A — explicit owner A4 instruction; actual native captures were reviewed, supplied pack stayed read-only. |
| 26 | Every value named reader field or owned readerAdditions; no invention | PASS — native Preview output/input/context and exact CASE_COMPONENTS readback; readerAdditions N/A, none added. |
| 27 | No pay date before execute | PASS — first/last due are Case repayment schedule dates, no execution pay date. |
| 28 | Untrained payroll user understands every label/number | PASS — repayment facts and plain next-step messages. |
| 29 | Actions naturally placed beside affected thing | PASS — Preview/Apply footer, inline recovery state, header Close. |
| 30 | Next step obvious/main button natural | PASS — enabled action tracks initial/preview/pending/accepted state. |
| 31 | Who/how much/why/next answered | PASS — Candidate/amount/weeks subtitle and server repayment facts; warning and recovery reason clear. |
| 32 | Every Waiting/Blocked/Action required row has reason/action | N/A — no Workbench row collection; inline pending states are separately checked at 17/19/42. |
| 33 | Action result visible in current window | PASS — Preview facts, invalidated state, unknown retry and accepted readback message remain inline. |
| 34 | Only information needed for task | PASS — no early creation form, list or fictional warning aggregate. |
| 35 | Same names consistently | PASS — Preview/Apply/Refresh/Try again and Payment Advance match native task meaning. |
| 36 | Costly mistake hard; safe confirmations/destructive placement | PASS — Apply requires a matching current Preview; no cash execution or destructive control. |
| 37 | Safe back-out at every point | PASS — native Close preserves unresolved/accepted journal and suppresses stale component effects. |
| 38 | Fewest common-task clicks / no detour | PASS — Preview then Apply; recovery is the direct original action/read. |
| 39 | Figures visibly add up | PASS — supplied six-week example: £50.00 × 6 = £300.00, returned dates six Mondays apart. |
| 40 | Confidence before Create Draft | N/A — shared Case creation consequence, not Create Draft. |
| 41 | Busy/empty/loading/slow/failed/phone/keyboard | FAIL at checkpoint — bounded component and source busy/recovery handling are correct, but final captured phone target sizing remains below 44px. |
| 42 | Return later: location and pending next step clear | PASS — recovered unresolved command says Try again; accepted Case says Refresh rather than new Apply. |

Checkpoint total: **29 PASS, 3 FAIL, 10 N/A**. The three failures are the same phone target defect; they are not three separate scope additions.

## Viewed PNG hashes

Every named file below was individually reopened after loop 40992.

| Actual PNG | SHA-256 | Visual verdict |
| --- | --- | --- |
| FC03-first-dark.png | 0b4b1d0d1f08b0bb7e65c1386919360553564de4662ae70cd4dc6ce778453ff4 | PASS |
| FC03-first-device-light.png | 0b4b1d0d1f08b0bb7e65c1386919360553564de4662ae70cd4dc6ce778453ff4 | PASS |
| FC03-first-phone-dark.png | 8e6e1f098298ebf40806cf06c5fdeb7c08cc53d171e0c41f6d58afa4e0330c1e | FAIL — target height |
| FC03-preview-dark.png | 0d2f71aab31e64ba6683ea2f011d0ea5c819029e1cf0699f4bc522e05f980364 | PASS |
| FC03-preview-device-light.png | 0d2f71aab31e64ba6683ea2f011d0ea5c819029e1cf0699f4bc522e05f980364 | PASS |
| FC03-preview-phone-dark.png | 8dae03d8f4f6c53117b3615fa52fcbc339bdbfcf16e7a1d13e6c6f89ad5bf43e | FAIL — target height |
| FC03-changed-dark.png | bc7e7b28701a04d6772ad25665ad6f00f028b230ec2b03a4c93db130b00e72ca | PASS |
| FC03-changed-device-light.png | bc7e7b28701a04d6772ad25665ad6f00f028b230ec2b03a4c93db130b00e72ca | PASS |
| FC03-changed-phone-dark.png | 13f6d2f2c1e591d2582a016a266e62d7b2607b0e9b1b5eca3b00d54f6fec958a | FAIL — target height |
| FC03-preview-error-dark.png | 49b537a165cd614a683bb19f9177cc961c9244b90e4ea03878e753ec452ef1db | PASS |
| FC03-preview-error-device-light.png | 49b537a165cd614a683bb19f9177cc961c9244b90e4ea03878e753ec452ef1db | PASS |
| FC03-preview-error-phone-dark.png | 3872b5b3b6cc5565470fe4a4469499c9d7ae005ac64e15da6d021feedd170d5c | FAIL — target height |
| FC03-pending-dark.png | eb0d71f664d4e708f87ba7ec8b696b88b727845d2efd8ea03ddc6f3cb2a02fcf | PASS |
| FC03-pending-device-light.png | eb0d71f664d4e708f87ba7ec8b696b88b727845d2efd8ea03ddc6f3cb2a02fcf | PASS |
| FC03-pending-phone-dark.png | 322b4968ba86b2327a0cd09b94a69f5ea663752e4cfe2d3eb77881157dc766a8 | FAIL — target height |
| FC03-accepted-dark.png | 003b0713f37afd26170170389310740e154e9f19d6a75072e12bc8c8c7f3aa99 | PASS |
| FC03-accepted-device-light.png | 003b0713f37afd26170170389310740e154e9f19d6a75072e12bc8c8c7f3aa99 | PASS |
| FC03-accepted-phone-dark.png | 8336b10fbf4dcacc1ce60075b72bbde28afcbb356e6148aa7e0dc2abd16f7a43 | FAIL — target height |
| FC03-refresh-error-dark.png | b2b71b926ac88a0107dd6c04346025495f515fde8ce5db7fd95cdf85d7274cef | PASS |
| FC03-refresh-error-device-light.png | a44d6e9fb30d77b1de77b80c5ec2e7c7d6d0afcce3b15da645f111dd239ecc56 | PASS |
| FC03-refresh-error-phone-dark.png | 1cd12ca74cb411e479a459f2323317e24a6cf2fb9fb2809a95ebb8548bf150f5 | FAIL — target height |

Supplied immutable PNG SHA-256: `866940964ca608c09feebdc57138ae51c015781e17c78ab9c1ac76bfe47d297f`; matching spec: `59bed32905ab710856e0df68b04c7875451425287fadfa5d12efaa3bf1622182`; B9 tools/policy.json: `45036d841d4568aa2d4d7adf4f063d6d17b70288e289ee0b285068797a87369d`.

## Current source checkpoint

The source interaction/contract PASS in `docs/bpay-next/reviews/FC-03-office-source-review.md` is retained. Radius 13, gap 8, one-sentence errors and truthful pending-line fixes are present. Current source also contains the newly prefixed phone 44px rule, which awaits replacement pixels; source declaration alone does not close A4.

| Current file | SHA-256 |
| --- | --- |
| FE js/bpay-next-case-preview.js | d7551956a993482a33264f39789cf2ce4d83342475dbabb53d7fff54ef4788e8 |
| FE js/bpay-next-office.js | cbdb844f11509a334cab8e87fc0cdc247b6e73ebe3f741ac8cb5be789e700745 |
| FE css/bpay-next-office.css | 043e1f6594a31c80f872f4107c362c18c47f74265a36f0f6a79c083bfcc537bc |
| FE index.html | e2c4a8003c1f45d2b9b575568a452aea83e80f9a651645c968a6df1cfdfedc6c |

A final appended section must supersede this checkpoint after the seven phone files are replaced and individually inspected.

## Final A4 closure — capture loop 71817

**Final verdict: PASS.** All 21 final PNG variants were individually reopened after root confirmed loop 71817 complete; the supplied PNG was reopened beside the final variants. Header Close and every visible phone footer action now occupy 44px targets. Phone facts remain clear in two columns, with no clipping or sideways scroll. All seven dark desktop and all seven device-light pictures pass. The actual source and pixels also show radius 13, footer gap 8, one-sentence semicolon error copy and the truthful unresolved-command Try again state. The checkpoint phone failures above are superseded. The checkpoint manifest was collected while replacement files were arriving; the final inspected pixels and manifest below govern approval, including where the saved hash is the same.

The seven supplied-design differences and every control-purpose explanation above remain accepted within FC-03 D09. For precision, the native host begins with the shorter component-only body centred; adding the Preview facts increases the body downward from that native position. It stays entirely visible. This is a native host geometry difference from the supplied tall future form, not an extra form or control. No unexplained visible difference remains. This PASS does not claim the future FC-04 form/FC-01 list is built, nor a database/business/runtime test by this reviewer.

Final policy result: **32 PASS, 0 FAIL, 10 N/A**. The complete final 42-status table follows; scope/owner caveats in the original table still apply. Check 25 and readerAdditions under 26 remain owner-directed N/A; actual named native values under 26 pass.

| # | Final status | Current evidence |
| --- | --- | --- |
| 1 | PASS | FC-03 shared Preview/Apply capability reached through the native component. |
| 2 | N/A | No cited legacy window replacement. |
| 3 | PASS | Required labels and owner one-sentence wording. |
| 4 | PASS | Native figures, invalidation and non-blocking repayment warning. |
| 5 | PASS | Standard native header, subtitle and header-only Close. |
| 6 | PASS | No additional tab row. |
| 7 | PASS | One Preview panel and native footer. |
| 8 | N/A | Definition-list facts; no table/list. |
| 9 | PASS | Amber warning; plain truthful result states. |
| 10 | PASS | One current primary and one secondary. |
| 11 | PASS | Scoped native styles, radius 13, gap 8, no !important, visible phone 44px controls. |
| 12 | N/A | No list or pagination. |
| 13 | N/A | No sortable columns. |
| 14 | N/A | No ticks/select-all. |
| 15 | N/A | No list aggregate. |
| 16 | PASS | Manual actual-copy inspection; no policy-tool execution claim. |
| 17 | PASS | One-sentence inline failure and clear recovery. |
| 18 | N/A | No destructive confirmation. |
| 19 | PASS | Initial, changed, failure, unknown and accepted-pending distinct; no invented zero. |
| 20 | PASS | Payment Advance, plain wording, no technical IDs/codes. |
| 21 | PASS | UK dates/pounds and meaningful None/Candidate default. |
| 22 | PASS | Applicable native button/focus/live-role source; no new row/table controls. |
| 23 | PASS | Phone has no clipping, all actions reachable, Close/footer 44px; list parts N/A. |
| 24 | PASS | All seven device-light variants individually match dark theme. |
| 25 | N/A | Owner A4 direction; supplied pack remains read-only. |
| 26 | PASS | Named native Preview/input/context and CASE_COMPONENTS facts; readerAdditions N/A. |
| 27 | PASS | Case due schedule only; no execute pay date. |
| 28 | PASS | Plain understandable facts and messages. |
| 29 | PASS | Footer next steps and header Close placed naturally. |
| 30 | PASS | Correct enabled action for each current state. |
| 31 | PASS | Candidate/amount/weeks, consequence, warning and recovery reason visible. |
| 32 | N/A | No Waiting/Blocked row collection. |
| 33 | PASS | Current result visible inline. |
| 34 | PASS | Only shared component information and necessary controls. |
| 35 | PASS | Consistent labels. |
| 36 | PASS | Matching Preview required before Apply; no payment execution/destructive action. |
| 37 | PASS | Close safely preserves journal and current-instance fencing. |
| 38 | PASS | Preview then Apply; direct retained retry/readback. |
| 39 | PASS | £50/week, six weeks, £300 and six returned Mondays are consistent. |
| 40 | N/A | Not Create Draft. |
| 41 | PASS | Phone target fix visible; source busy/recovery/keyboard mechanics retained; no unbounded list. |
| 42 | PASS | Recovered pending says Try again; accepted Case says Refresh. |

### Final individually inspected files

| Final actual PNG | SHA-256 | Verdict |
| --- | --- | --- |
| FC03-accepted-dark.png | 003b0713f37afd26170170389310740e154e9f19d6a75072e12bc8c8c7f3aa99 | PASS |
| FC03-accepted-device-light.png | 003b0713f37afd26170170389310740e154e9f19d6a75072e12bc8c8c7f3aa99 | PASS |
| FC03-accepted-phone-dark.png | 8336b10fbf4dcacc1ce60075b72bbde28afcbb356e6148aa7e0dc2abd16f7a43 | PASS |
| FC03-changed-dark.png | bc7e7b28701a04d6772ad25665ad6f00f028b230ec2b03a4c93db130b00e72ca | PASS |
| FC03-changed-device-light.png | bc7e7b28701a04d6772ad25665ad6f00f028b230ec2b03a4c93db130b00e72ca | PASS |
| FC03-changed-phone-dark.png | 13f6d2f2c1e591d2582a016a266e62d7b2607b0e9b1b5eca3b00d54f6fec958a | PASS |
| FC03-first-dark.png | 0b4b1d0d1f08b0bb7e65c1386919360553564de4662ae70cd4dc6ce778453ff4 | PASS |
| FC03-first-device-light.png | 0b4b1d0d1f08b0bb7e65c1386919360553564de4662ae70cd4dc6ce778453ff4 | PASS |
| FC03-first-phone-dark.png | 8e6e1f098298ebf40806cf06c5fdeb7c08cc53d171e0c41f6d58afa4e0330c1e | PASS |
| FC03-pending-dark.png | eb0d71f664d4e708f87ba7ec8b696b88b727845d2efd8ea03ddc6f3cb2a02fcf | PASS |
| FC03-pending-device-light.png | eb0d71f664d4e708f87ba7ec8b696b88b727845d2efd8ea03ddc6f3cb2a02fcf | PASS |
| FC03-pending-phone-dark.png | 322b4968ba86b2327a0cd09b94a69f5ea663752e4cfe2d3eb77881157dc766a8 | PASS |
| FC03-preview-dark.png | 0d2f71aab31e64ba6683ea2f011d0ea5c819029e1cf0699f4bc522e05f980364 | PASS |
| FC03-preview-device-light.png | 0d2f71aab31e64ba6683ea2f011d0ea5c819029e1cf0699f4bc522e05f980364 | PASS |
| FC03-preview-error-dark.png | 49b537a165cd614a683bb19f9177cc961c9244b90e4ea03878e753ec452ef1db | PASS |
| FC03-preview-error-device-light.png | 49b537a165cd614a683bb19f9177cc961c9244b90e4ea03878e753ec452ef1db | PASS |
| FC03-preview-error-phone-dark.png | 3872b5b3b6cc5565470fe4a4469499c9d7ae005ac64e15da6d021feedd170d5c | PASS |
| FC03-preview-phone-dark.png | 8dae03d8f4f6c53117b3615fa52fcbc339bdbfcf16e7a1d13e6c6f89ad5bf43e | PASS |
| FC03-refresh-error-dark.png | b2b71b926ac88a0107dd6c04346025495f515fde8ce5db7fd95cdf85d7274cef | PASS |
| FC03-refresh-error-device-light.png | a44d6e9fb30d77b1de77b80c5ec2e7c7d6d0afcce3b15da645f111dd239ecc56 | PASS |
| FC03-refresh-error-phone-dark.png | 1cd12ca74cb411e479a459f2323317e24a6cf2fb9fb2809a95ebb8548bf150f5 | PASS |

The supplied PNG/spec/policy hashes recorded above remain unchanged. Six final dark/device-light pairs have identical file bytes. The refresh-error pair differs in file bytes; each was viewed and its dark theme, copy, controls and placement match.

### Final source closure

**SOURCE PASS retained.** Fresh final Office/component/CSS/index hashes below are authoritative for this closure; the final component changes are wording/pending-state copy, and CSS adds the exact native-modal phone specificity.

The current component/CSS were re-read for the final presentation changes; Office and index hashes remain as the interaction PASS. The exact native-modal phone selector fixes specificity without broadening the component, the journal or transport. No money derivation, new field/reader, automatic mutation retry or future form was added.

| Final FE file | SHA-256 |
| --- | --- |
| js/bpay-next-case-preview.js | d7551956a993482a33264f39789cf2ce4d83342475dbabb53d7fff54ef4788e8 |
| js/bpay-next-office.js | cbdb844f11509a334cab8e87fc0cdc247b6e73ebe3f741ac8cb5be789e700745 |
| css/bpay-next-office.css | 043e1f6594a31c80f872f4107c362c18c47f74265a36f0f6a79c083bfcc537bc |
| index.html | e2c4a8003c1f45d2b9b575568a452aea83e80f9a651645c968a6df1cfdfedc6c |
