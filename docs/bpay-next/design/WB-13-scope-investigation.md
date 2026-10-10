# WB-13 scope investigation — sequence56

Read-only source investigation saved10 October2026. No product, checklist, SQL/runtime/test, app, fixture, data, hosted or network action was performed. Root alone authors product. This note identifies the smallest complete current work and explicit later joins; it is not whole-item acceptance.

Authority read: fresh normal-plan `02_WORKBENCH.md:541–589` in full, shared W1–W14, `review/R2.md:92–96,120–124,192–198`, master sequence56/dependencies, BUILD-BE checklist WB-13-D01–D06 and item row, approved builder start sheet in full, owner answers/B4.4 and binding BD019/BD020. Scope guard is recorded in `design/F-11-SCOPE.md:14`: exact listed objects plus the smallest support needed for all Rules/Done. The named classifier chain remains controlling. Rule B stays closed: selected expected_revision_id/captured Candidate/query snapshot/PIN_v2; no parked F25 protocol or resolver.

## Exact deliverables and current reuse

| Row | Object | Current source / disposition |
| --- | --- | --- |
| D01 | private.bpay_next_workbench_item | Existing typed maintained contribution table, migration10102026_1252…summary.sql:67–181, including unique native item UUID and component/member/portion identities. Work classifier current final1438…origin_projection_v1.sql; Case classifier final1252…summary_v1.sql. Reuse; do not reclassify or sum in the browser. |
| D02 | private.bpay_next_workbench_candidate_summary | Existing1252 migration:183 and natural ALL/client/channel/section rollups; scope summary starts222. The established replacement/delta owners supply the counts and provenance. Reuse instead of browser totals or scanning all pages. |
| D03 | public.bpay_next_workbench_page_v1(jsonb) | Registered current final1520…workbench_reader_v1.sql, with private kind_page(text,jsonb,jsonb,integer), action_required_page(jsonb,jsonb,integer), blocked_page(jsonb,jsonb,integer). ACTION_REQUIRED and BLOCKED are built; each reads current item section, exact filters/search and unique non-null native item UUID closure. Only genuine missing presentation fields need the proposed small successor below. |
| D04 | GET /api/banking/pay/next/workbench-page | Existing `broker/src/bpay-next-workbench.js` fixed RPC/admin/typed transport, `index.js:198775–198834` admin/environment before RPC, exact GET exemption in legacy-fence171. FE current helper already supplies actor/environment/deadline and typed failure. Reuse. |
| D05 | Workbench Action/Blocked/Waiting typed issue tables in js/bpay-next-workbench.js | **Missing principal screen child.** The current487-line file exports only createWorkbenchClient/loadWorkbenchPage/WorkbenchReadError/ROWS; it creates no DOM/list/opener. Build the two exact card tabs, reason/provenance rendering, search/sort/Previous/Next/footer/error behavior and real caller join. The card's WAIT requirement is an Action Required reason/state, not a mandate for a third whole-scope WAITING tab or new reader kind. |
| D06 | WB-13 output-only reviewed card | Specification evidence, not another product owner. Record actual source review and later in-app checks honestly. |

Current final registry: work refresh(uuid) and core refresh(uuid)→1438 origin_projection; Case refresh(uuid)→1252 summary_v1; private kind/action/blocked readers and public page RPC→1520 reader. WB13 has no classifier successor position. WB07/query and WB02/withdrawal source branches are already carried by1438; WB40/pay-hold, WB41/recovery pause and WB29/resolution classification are later named owners, not WB13 money/producers.

## Required screen behavior

* Two tabs labelled **Action Required** and **Blocked for Pay**, with server count badges from one coherent maintained revision. Action intro exactly “Payments that need a decision before they can be included.” Default Candidate A–Z; Candidate, Payment and Amount sort controls select existing NAME/PAYMENT/AMOUNT ASC/DESC server values. Search Candidate name, TMS reference or Timesheet reference, maximum200 characters, existing prefix semantics. Never sort/filter a returned page as a substitute for server membership.
* Five presentation columns: Candidate, Payment, What needs attention / Why it is blocked, Amount, Actions. Display one returned native item identity once, including CASE components and disjoint native RESERVED/RECOVERY/AVAILABLE portions. Do not use nullable work_id as row/cursor key, aggregate a Timesheet into a single replacement reason, deduplicate distinct genuine portions, or duplicate one row into both sections.
* Explicit100-row pages, truthful complete/next_cursor, Previous/Next, page/section counts and empty text. Keep only current rows and a **bounded** receipt/cursor history. Current RPC is forward-after keyset only; Previous requires retained page-start receipts. Do not implement an unbounded cursor/row cache, recrawl from page1, or claim an evicted earlier anchor is the first page. If a finite journal cannot satisfy required adjacent Previous behavior, an exact bounded backward-page support in this named reader is genuine small support; no global page/proof framework.
* Exact typed `openIssue` context must carry item_id/group_key/candidate_id/work_id or case_id/case_component_id, reason and revision/scope. Open by keyboard/click without duplicate child bubbling; preserve the list's scroll/filter/page anchor when returning. WB14 owns the later full separate issue window/actions; WB13 must not implement its rate/clear/snooze/cancel/restore owners early or call the retired legacy Workbench.
* Read failures show plain text and Try again, preserving filter/search/sort/page/scroll. A stale cursor is an explicit reload refusal; do not silently turn Try again into a fresh financial scope/reprice/retry. Missing amounts/evidence remain unavailable; neverGBP0. Processing/error adoption invalidates stale actions/Create eligibility and old cursor scope before asynchronous replacement; publish coherent replacement rows/counts together, with captured actor/generation guards. WB34/WB19 supply later action completion/administrative retry owners.
* Reuse existing `CloudTMSBpayNextUi` keyboard/focus helpers, existing date-only/London display helper and `css/bpay-next-office.css` responsive/theme conventions. DOM textContent for business data; no new browser calculator, direct service RPC, payload logs, polling/new command protocol or per-checkbox networking.

## Small necessary amount-provenance support

The current issue-kind projection/ROWS contains only `nominal_source_ex_vat`, amount_known and member/portion metadata; it cannot directly convey all R2's distinct direction/stage/approved/reserved/outstanding/recovery facts. Root's proposed additive projection is justified by the named D03 page reader and D05 presentation, not an expansion of financial authority:

`direction`, `payroll_stage`, `approved_source_ex_vat`, `active_reserved_source_ex_vat`, `deduction_balance_source_ex_vat`, `recovery_source_ex_vat`, `included_source_ex_vat`, `gross_due_source_ex_vat`.

Add these exact **existing stored item columns** only to ACTION_REQUIRED/BLOCKED/ISSUE_DETAIL JSON and matching closed BE/FE ROWS types, with nullable decimal text and typed direction/stage. The common1520 private kind helper is the actual physical owner; author a new successor once1520 is installed/locked, preserving its full other-kind/query/budget/auth behavior and registry chain. No new column, classifier, money owner or producer is required. The proposed successor was not yet observed as saved product by this investigation.

Labels use real source facts: approved/last approved, frozen reservation, outstanding deduction balance, managed recovery, included/gross due, unknown evidence, or zero-this-run are distinct. Do not infer payroll NET/headroom/cash from nominal gross, subtract stored amounts in JavaScript, use channel ratios or turn a missing figure into zero. The existing GROUP_LINES typed reader already supplies full retained A/C/H/residual detail for the exact item if a bounded read-only opening needs it. Formal WB14 remedy controls remain later.

## Count/revision and search boundaries

1520 HEADER performs an exact natural `(client_scope,pay_channel,section)` summary lookup; it does not return simultaneous ACTION/BLOCKED count fields, and HEADER search must be empty. The narrow reuse path is two bounded HEADER reads for ACTION/BLOCKED plus the active list page. Join exact common agency_id, actor_user_id, owner_epoch, official_pay_date, workbench_revision and adoption_revision, and the common client/Candidate/channel scope; kind/section/search differ deliberately. Adopt the resulting badges/footer/current page in one DOM transition only after all agree. On disagreement show unavailable/stale and retain the user's input; do not publish one tab's newer count beside older rows or fabricate partial totals.

Header item_count is an exact whole-section scope count. It is **not a filtered search total**. Label whole-section totals and current returned-page counts truthfully; do not relabel unfiltered item_count as the number of search matches or calculate a purported full total from loaded rows. If product needs an exact filtered total, that is a missing named-reader output with bounded/indexed semantics to resolve explicitly, not an all-page fetch.

Existing native search1520:145 uses maintained candidate_display_name/candidate_reference/timesheet_reference; current work projection1438:96 writes metadata.candidate_sort_name/tms_ref to those display fields. Existing issue sort366–392 has exact name+CandidateUUID+itemUUID, Payment text+itemUUID, or unknown-amount flag+numeric+itemUUID, allCOLLATE C/native closed ties. R2 supersedes the old nullable work-only tail with genuine item identity; retain these actual deterministic keys in UI receipts. Later WB10 owns explicit matching performance/index refinement; WB13 does not claim a2-second app timing proof or add a test programme.

## Finite reason renderer and later joins

Reuse the reviewed WB08 finite table (`WB-08-workbench-projection.md:143–174`) and server classification priority, not text-based classification. Minimum words:

| Code / actual fact | Display |
| --- | --- |
| AWAITING_REAUTHORISATION | Paid — awaiting re-authorisation (real paid withdrawal is included in ACTION count) |
| APPROVAL_WITHDRAWN | Missing approval — approval withdrawn |
| MISSING_APPROVAL | Awaiting authorisation |
| PAY_QUERY_OPEN | Pay Query open |
| PAY_QUERY_UNAVAILABLE | Pay Query evidence unavailable |
| PAY_ON_HOLD + typed reason | Pay on hold: actual reason (later WB40 supplies its branch) |
| RATE_RESOLUTION_REQUIRED / GROSS_RESOLUTION_REQUIRED | Pay channel changed PAYE>UMBR or UMBR>PAYE — choose the rate, using actual source/target channels; never CHANNEL - NONE (later WB29 supplies resolution owner) |
| ORIGIN_UNAUTHORISED_WAIT | Waiting for re-authorisation — recovery paused (later WB41 exact branch) |
| DATA_UNAVAILABLE / SEGMENT_CREDIT_ATTRIBUTION_UNAVAILABLE | Required pay data is missing / Original shift credit needs review |
| APPLY_PENDING | Pay facts are being updated |
| CASE_REVIEW_REQUIRED / RECOVERY_MANAGED | Case needs review / Recovery managed by the linked Case, using the actual stored identity |
| IN_PREPARATION / RESERVED / CASE_PAUSED / FUTURE_WEEK / SNOOZED | Explicit current Waiting/control fact, if returned by a named current view; no invented selectable Ready offer |

Unknown future codes get explicit unavailable/review copy with no invented action or value. Bank warning remains a warning on Ready, never a Blocked/Action reason or an execution permission decision. No insufficient-funds/headroom reason: a true zero-recoverable repayment is Ready/tickable under its later selection/estimate owners. WB13 must never create a synthetic zero or make an informational WORK recovery a second selectable Case deduction.

Current reachable host is `main.js:7154–7166`→CloudTMSBpayNextOffice.open, with current actor/positive ownerReady. Office531 enforces that admission; its render578 onward remains the thin preparation flow and has no Workbench issue mounting. Static workbench include is already present. A reusable `mountIssueLists({openIssue,…})` is a sound narrow implementation seam. **Unmounted lists or an absent real openIssue caller are component-only completion:** record Office/WB14 integration pending; do not mark whole WB13 user's Switch/Open behavior complete until there is an actual reachable list and a genuine selected-item opening. WB11 later owns full filter/header shell, WB14 full issue detail, WB19 job retry/progress, WB21 local selection/Create, WB34 own-action adoption. Do not hide an unfinished join with a working-sounding disabled stub.

## Current source hashes

| Source | SHA-256 |
| --- | --- |
| BE1252 summary migration | aa9a42864893856c9ae02479edb1172cdf39c764a6ca2e9e77f2fd11253affab |
| BE1438 origin projection/current work classifier | b8d445c4a4cfc02978e8ac77a159350b0e1978aaff9a1172da8e7e157a6414f5 |
| BE1252 summary_v1/current Case classifier | 2fe6c4c816613aaa554d012289124c47219fd228a2af229c2ceabdb8f0cc305b |
| BE1520 reader | eebe1a463cac604165a8b9e460ba2a5e82c95d45edd118806d96ae3d03736409 |
| BE workbench transport | c96e79af5bb4a684ebe842eca71131d6218f9032d575e79d29a1550433d40aed |
| FE workbench helper | d043df552d24a05bd0aa0cef988a3450dba832ab98fcfbc20f22df093a847572 |
| FE Office host | 9a946927ad0329c5da6e8da550543ec0cce6d34ac6dfd6b9e0e188a21af6bab4 |
| FE main | 4c83cfbcbc253050387fe9840a5e6d0230d5f558e4f642eb8569ddf987f6d44a |
| FE Office CSS | 16f273ad1bdc141f3d44813eb2653f8ea1be8fc771abb799d5f9d4a04f4b861e |

All actual in-app Done clauses (channel-change display, paid withdrawal badge, pay-hold rows, bank-warning placement, zero-headroom Ready,1/17/100 order, paging and matching badge/footer counts), phone/theme/keyboard and timing remain NOT_RUN. A reviewed specification or helper/source compile is not their proof.
