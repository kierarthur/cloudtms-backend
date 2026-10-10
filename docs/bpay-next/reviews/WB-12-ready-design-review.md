# WB-12 Ready Candidate table — final independent A4 design/source PASS, 10 October 2026

Verdict: **PASS — exact current WB12 scope/source and all applicable actual-screen checks.** No actionable finding remains. Final SCR04 review records all42 results:37 Pass /0 Fail /5 Not applicable, fresh source/PNG/sample hashes. This supersedes the earlier source-only preparation verdict; it is not installation, business scenario,230-row or timing execution evidence.

Read full WB12 card `02_WORKBENCH.md:499–539`, all eight ITEM/D01–D07 checklist rows, current BD068, WB12 current-scope and count/status notes, shared W12–W14/R2 constraints, and the read-only SCREEN_DESIGN_POLICY/index. Inspected actual T01 Ready, T04d empty and T06 phone JSON plus their indexed PNG guides through view_image. The sole physical deliverable is D06 `Workbench Ready candidate table in js/bpay-next-workbench.js`; D01–D05/D07 are reference contexts. Existing reader/count/status support is narrowly supporting D06, not a second money owner. Actual saved dark/device-light/phone/empty/failed SCR04 product PNGs were then individually viewed. No product/spec/pack edits, tests, runtime, DB/SQL, business fixtures, network, credentials or screenshot-generation tools were used by the reviewer.

## Exact current scope

Current visible data headers are Candidate (TMS reference as subline), Deductions, Amount ex VAT and Status, each using its finite server sort in both directions. Tick/action cells and phone Open are sorting exclusions. NAME_ASC is the default. A3 overrides the card’s Previous/Next and single-click opening: continuous bounded scrolling, single click highlights, double-click/Enter opens, phone Open, dark even device-light, and no displayed pay date. Amount ex VAT is the approved short label; the stored SOURCE amount is not presented as a calculated final cash estimate.

Timesheets View belongs to WB17, per-Candidate progress/Retry to WB19, Create/confirmation to WB21, storage to WB22, bulk selection to WB23, mixed child membership to WB24, and full Candidate Banking to WB15. Their T01/T06 illustration is not authority to build early placeholders or fake fields. Updated is unsupplied and omitted. Only genuine bank_warning_code produces an informational bank badge; no invented Updating/Failed/rate-set chip. Existing Candidate record is the present row/bank navigation destination until its named later child owner.

## Source evidence

- Actual `mountReadyCandidates` is mounted into WB11 readyHost by the existing Office controller after factual environment capabilities. Real standard modal/header remains unchanged.
- CANDIDATES + HEADER are separately strictly decoded, share the same actor/agency/owner/workbench/adoption/date context, and must join the shell HEADER context before usable ticks/Open. Context mismatch is unavailable, not a guessed local recalculation. All source reads remain read-only.
- Natural server ALL/client/channel summaries provide the complete Candidate universe and combined Both amount. Only the server whole-scope HEADER provides totals; the browser never sums visible rows.
- Up to100 rows per read, at most three retained pages and32KiB cursor-key history, with source next_cursor and measured removed/added row-height compensation. Sorting resets cursor/page/highlight while preserving Candidate exclusion intent. No whole-agency row cache.
- Candidate tick intent is finite ALL/NONE plus exact native UUID exceptions, bounded by the existing32KiB request. Current displayed “All except N” is explicitly local intent, not a fabricated exact Candidate/segment count. The exposed countSelection reads existing HEADER and binds intent revision/context; exact selected_candidate_count/selected_member_count is server-owned for its later confirmation caller.
- E6: `payable_count=0` deductions-only rows remain listed, show “Nothing to pay this week”, and have an unchecked disabled tick. READY HEADER count/eligible-member denominator includes only positive-payable Candidates. Their deduction evidence remains visible and outstanding; no collection/financial mutation is inferred.
- Candidate/TMS, has_deduction, nullable stored amount and ready_net_add_amount are actual typed fields. Exact decimal text is formatted through BigInt presentation only; NET Payment addition stays a separate subline instead of being combined with SOURCE. Null stays a dash. Deductions uses server evidence and the whole-Candidate local exclusion; later individual child-override/mixed truth remains WB24.
- Native bank missing/changed code maps to a warning badge opening the existing Candidate record; it does not disable earned payment. Status sort uses the matching typed native payable/bank/name/UUID tuple, not browser labels or local sort.
- Source covers stable focus keys, aria-sort, native ticks, row arrows/Home/End/Enter/Space, standard frame and phone Open. Actor/positive NEXT/disposal/current context guard reads and interactions. Error/loading disables row actions; typed stale Refresh clears old page/context; transient Try again retains the failed read. No financial retry.
- New table/card CSS is scoped under the standard #modal, uses shared tokens and adds no !important declarations. The final supporting phone fix extends thirteen existing standard card selectors to the exact read kind, preserving their declarations; Ready-only label normalization removes the sort arrow from phone labels. No public closed financial contract, producer, command kind, PIN/enroller, F25 parked file or money computation is introduced.

## Final actual-screen review

The complete42-point POLICY CHECKLIST and user-eyes review is saved in `docs/bpay-next/screen-designs/SCR-04/review.md`:37 PASS,0 FAIL,5 N/A. All five final actual product PNGs were individually inspected after the phone-card/label and coherent-empty corrections. The main frame is1320px, dark/device-light identical, phone Candidate block and44px Open fit, the deduction-only row has the truthful disabled reason, empty is genuine zero, and failure has real retry without false empty. Manual policy.json wording passes. Future owned controls and exact N/A scope remain as above; no business or scenario execution is inferred.

## Final source snapshot

| File | SHA256 |
|---|---|
| FE/js/bpay-next-workbench.js | 4cdf12a964f25b4c8ebd2d6ca05d03c7387cd36bf8d0c45c3857a81f570adbd8 |
| FE/js/bpay-next-office.js | d377ce9c8eb7073169f045d997200c19c47a828bcf88ff5fcdf9b9df9696c189 |
| FE/js/bpay-next-ui.js | dbdcb12ed8ec027b0df39c52ef6f50b6d91ca93e2cb74451a59e09a6ba029272 |
| FE/js/main.js | 7cde52d8f113f9c35f19f2f0be198390e2d742118c991b98e29ca06dcc645845 |
| FE/js/modal-modernisation.js | 98ea1f99c329868d773ab99669349efab150b9077ebdb1f94dd5a5097589bcdb |
| FE/css/bpay-next-office.css | 158a6e2f556e1bb8d8bc3c57e4267a5dae15602be8a9d8b6294ab69f68f357e4 |
| FE/css/modal-modernisation.css | a4003bf69088f282c7883ab394a5b679826e8937707da0593456a5045b8e3606 |
| FE/index.html | 6fe220c0084a7adb37204fb62075497c17dfe89422ce572940d4de2bd8501b19 |
| BE/broker/src/bpay-next-workbench.js | 19ba3f30fd416604b22bf92f6e5e5b5b7c966bf7b100e67d0941cfa6512a6910 |
| BE/supabase/repeatable/10102026_1743_banking_pay_next_ready_candidate_counts_v1.sql | 727d80356ecfd7ef7e8c822c3181403602910ab4cef5bb50f4677dd32df2b760 |

## Final artifact snapshot

| File | SHA256 |
|---|---|
| dark.png | 2251e665c768be6b2d3b695440c269f5b7226f13439247d550b1523fbd753a3f |
| device-light.png | 2251e665c768be6b2d3b695440c269f5b7226f13439247d550b1523fbd753a3f |
| empty-dark.png | 0152968c3f992af4b3e4fef2f2afbe328ea9feac3f84f379f518b0e470203221 |
| failed-dark.png | 2dbe6a48ba28c8c7ca68404833dfce5453bed1faa782c58adc380f98206e0303 |
| fixed-read-responses.json | a0615c95e3cbfc268c1da77cc832b6f5ea312256e74822e128caa2f6cdb40d19 |
| phone-dark.png | a1feebf2ba29f81f51e92b08a7595ad9c42f3aaab7a05235eab6b3640b314eb8 |

These fresh hashes bind the actual saved source/pixels inspected; source/actual-screen review only. No tests, SQL/DB, application/browser scenario,230-Candidate timing, hosted data or financial actions were executed by this reviewer. No additional test programme is proposed.
