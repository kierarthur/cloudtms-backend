# WB-11 current scope investigation — 10 October 2026

Read-only investigation of seq59. No product, design-pack, database, application, test, configuration or hosted change/execution. Parent is the sole product writer. This note does not claim a rendered-screen or installation PASS.

## Binding scope and deliverables

Read full `02_WORKBENCH.md:464–497`, all WB-11 checklist rows (ITEM, D01–D06), master seq59/dependencies WB-09 + F-16, start-sheet sections 1–6, current BUILD_LOG owner boundary lines610–616, screen policy in full including its42-point checklist, applicable inventory/index rows, and the indexed three JSON/PNG designs. Parent supplied the latest owner A3 directions directly: no pay date before execution; main tabs Ready / Action required / Blocked; Waiting only in Candidate Banking; continuous scrolling; dark only; single click highlights, double-click/Enter opens, phone Open; all data columns server-sort except the finite exclusions. A4 requires actual local rendered screenshots and independent42-item review. The owner cancels the former local300 timing exercise; no business dataset, fixtures, `wrangler dev` or DB/hosted requests are part of this work.

These directions supersede WB-11's visible-official-date bullets and policy SDP-92/93/96 instructions to generate/copy specs. Read the pack, build the actual screen, save minimal fixed-response files/screenshots in its SCR artifact directory, and review the actual render. Do not alter/copy the pack, author another design spec, or run its generator/render/check tools. The original instruction permitting a screenshot does not permit broader scenario/testing work.

| Row | Exact current object / necessary work |
|---|---|
| D01 | `private.bpay_next_workbench_candidate_summary`: reuse WB08 native Candidate/client/channel/section ALL rollups. WB11 needs no money/refresher rewrite. |
| D02 | `private.bpay_next_workbench_revision`: reuse WB44 adoption/dirty/task barrier and existing actor/owner/date/revision cursor context; never treat a page from another context as current. |
| D03 | `public.bpay_next_workbench_page_v1(jsonb)`: current final1634 retains bounded HEADER/PICK_CANDIDATES/PICK_CLIENTS and typed refusals. Preserve the full current closed contract, ten kinds and5s settings. |
| D04 | Exact admin GET `/api/banking/pay/next/workbench-page`: reuse current WB09 transport, environment/auth/fence order,4s/128KiB and byte observation; no new route or Worker owner decision. |
| D05 | Actual FE `js/bpay-next-workbench.js` header/filter widgets plus minimum existing `bpay-next-office.js`/`main.js` mounting support. Remove the editable Pay date input/send from the NEXT opening flow. No CREATE/open/get/session/rebuild on open or Apply/Clear. |
| D06 | Existing output-only reviewed card does not prove implementation. Close only after actual scope/source/required screenshot review; later screen actions keep their named owners. |

Rule B/PIN/enroller/producers/parked F25 remain unchanged. WB12 owns the full Ready list and genuine selected Candidate count, WB21 Create confirmation/selection, WB28 Reset selection/Clear decisions, WB18 Export, WB19 richer refresh/progress states, and WB42 final navigation. The shared SCR01/02 designs include these later actions; they are not authority to invent their implementation in WB11.

## Approved screen references actually viewed

Pack: ordinary backend `codex_outputs/claude-stage2-continuation-20260925/SCREEN_DESIGN_POLICY_20261010/`.

* SCR-01 index15 → `specs/T01_ready_list.json`, `png/T01_ready_list_dark.png` and `T01_ready_list_devicelight.png`. Viewed the dark PNG: standard Banking Pay frame; one three-tab row; facts strip, PAYE + Umbrella chip, Filters/Refresh; no pay date. The strip is not a second title. WB11 builds its header portion, not the later Ready table/ticking/payment actions.
* SCR-02 index16 → `specs/SCR-02_filters.json`, `png/SCR-02_filters_dark.png`. Viewed PNG: small standard child Filters window; Pay channel radios; Candidate/Client controls; count/amount subject card; Clear filters and Apply footer. Choices stay local until Apply. Later WB28 Reset selection/Clear all decisions are not substitute filter clears.
* SCR-03 index17 → `specs/SCR-03_candidate_picker.json`, `png/SCR-03_candidate_picker_dark.png`. Viewed PNG: small Choose Candidate child; parent filter subtitle; name/TMS-ref search; focusable rows, continuous scroll Loading more, Choose footer. Client variant uses real Client fields. No money/result count is supplied for the Candidate picker.

Use existing Office dark tokens, standard `#modalBack` / `#modal.ctms-modern-modal` / `.modal-h` / `#modalTabs` / `#modalBody` / `.actions`, universal table, existing badges/chips and scoped small support CSS. Parent/child frame preservation and visible keyboard focus must survive repaint, async results, Escape and Close. No toast, whole-screen rebuilding indicator, IDs/codes/technical explanatory prose, light palette or sideways phone page scroll.

## Exact reader contract and widget mapping

Final registry resolves common kind reader, ten private wrappers and public wrapper to `supabase/repeatable/10102026_1634_banking_pay_next_workbench_read_limits_v1.sql`. Public service-only STABLE definer checks active admin, NEXT owner, agency identity, adopted/current revision with zero dirty Candidate/group/task counts. Each request is a single read snapshot. Closed input is trusted `actor_user_id` plus kind and twelve finite fields; the browser supplies kind plus scope fields through the existing client. Limits1..100, trimmed search≤200 characters/800UTF8 bytes,32KiB request,120000-byte SQL reply,128KiB transport,4s caller deadline remain.

`HEADER` one row provides integer-text Candidate/item/contribution/segment/payable/selected/snoozed/held/queried/blocked/deduction/unknown counts, nullable exact decimal-text amount families and has_deduction. Official date is in `context.official_pay_date`, not the row. The context also contains agency/actor/owner_epoch/workbench_revision/adoption_revision and exact scope. Hide the date from these screens; preserve it internally for existing cursor consistency. Do not display revision/epoch/IDs.

* Both / PAYE / Umbrella → `channel: ALL / PAYE / UMBRELLA`; optional Candidate/client UUIDs are actual filter identities. No run/session mutation. Main filter chip uses retained labels from an actually chosen typed row, not UUID text.
* A scope count card uses the existing HEADER for the staged scope; for ready-money wording use section READY. Counts/amount cover the entire server scope, not loaded picker rows. Unknown amount remains `—`; unavailable read must not become zero. Never call HEADER.selected_count an exact current browser-tick count: local exclusions and future WB12 count owner are separate.
* Channel-specific badges/counts, if displayed, require HEADER reads for each needed channel and section; no per-channel array is currently returned. Compare common context fields agency/actor/owner/date/workbench/adoption across a multi-read bundle before displaying it, as existing issue-list `sameRevision` does. Mixed contexts are unavailable/reload, not sums.
* `PICK_CANDIDATES` returns exactly candidate_id, candidate_sort_name nullable, tms_ref nullable; maintained Candidate summary with exact client_scope/channel/section, optional Candidate and escaped prefix name/ref search. Current kind requires NAME_ASC. Do not use main's general `openCandidatePicker(...ignoreMembership:true)` as the WB11 picker: it is not scoped to current work. No row amount or full result count exists.
* `PICK_CLIENTS` returns exactly client_id and factual client_display_name. Scope-summary item_count>0, exact channel/section, optional Candidate qualification and bound prefix Client-name search. Current cursor order is client_scope/native UUID, not displayed Client name; current sort selector does not change that. Dialogs are excluded from A3e data-column sorting. Do not promise name-sorted Clients, or render a sortable name header that merely sorts loaded rows. No per-Client Candidate count exists: omit that mockup column/value.
* Preserve the current picker constraint when re-opening: choosing a different Candidate/Client requires clearing that picker-specific identity in its draft request while retaining the other filters; otherwise the current optional identity predicate returns only the existing selection.
* Open Filters starts from committed scope. Picker choices/search/count are draft state. Apply installs exact draft scope and re-reads with after=null; Clear resets ALL/null/null and re-reads. Closing without Apply preserves committed scope. No app-wide financial selection/journal overwrite. Keep current screen scroll/filter state on child close, discard cursors after scope/search change, protect results with scope-generation + current actor/frame/owner checks, and dispose listeners/observers on Close.
* Continuous picker chunks use existing typed next_cursor/page_receipt. Distinguish complete from unavailable; reject duplicates/context change through existing decoder. Bind late requests to the exact current draft scope. No automatic business retry.

### Design values with no field: omit instead of extend the server

Mockup literals are illustrations. Picker result totals, per-Client Candidate counts, a server updated-at timestamp, and exact locally ticked amount/count are absent from these replies. Omit missing figures/columns. `Updated …` may mean the local time of successful fresh read (policy AGREED_BEYOND_PLAN explicitly permits this presentation); it must not imply a DB last-change timestamp. Labels of chosen Candidate/Client come from typed rows. Do not fabricate header zero/count/financial data from a missing response or request a new feature solely to reproduce a mockup.

## Exact current FE mounting seams

`bpay-next-office.js:540–564,591–614,681–686` currently appends its own `section.bpay-next-office` overlay directly to body, titled Banking Pay NEXT, mounts `mountIssueLists` after capabilities, and exposes editable Pay date / Start selection. Its opening request is capabilities plus issue reads, not CREATE. D05 needs real standard-frame mounting support and no editable-date authority; retain controller/journal exact pending bytes and every existing financial transport function. CREATE already accepts optional pay_date after DR01: omit it on the new flow rather than derive a browser date.

`main.js:7159–7175` supplies api/authFetch/initiating actor/currentActorId, general Candidate picker, Candidate-record/issue callbacks, ownerReady and stale invalidation. The existing standard-modal seam is `showModal`, used by `bpay-next-payment-history.js:430–434,509–513`, with real `window.__getModalFrame` identity and parent restore. Passing minimum showModal/frame support through this existing Office handoff is narrower than creating another overlay framework. Preserve owner/admin environment qualification and initiating actor before/after every await. Current `mountIssueLists(options):541–609` already accepts exact {candidate_id,client_id,channel} and exposes setScope/snapshot/close; reuse its scoped reads/issue handoff, while current two tabs/Previous/Next/body wording need A3 correction at the named screen touch. No main-page Waiting tab.

## Official-date source discrepancy

Confirmed source only: `26052026_2100HRS_NEW_FUNCTIONS.sql:185280–185379` is the actual read-only STABLE service-only helper. It reads settings_defaults weekday+updated_at and computes first configured weekday on/after London date, next=same, following=+7, current pay week end=date/start=date−6. It does **not** consult bh_list/bh_feed_list or skip holidays/weekends. No later replacement was found. The statement that it already applies the holiday feed is false in current source.

Existing real holiday authority is settings_defaults.bh_list + bh_feed_list; `03092026_1641_contract_settings_effective_authority_v1.sql:214–228` combines them for Contract settings, and feed complete:534–543 writes validated feed dates. That is not an official-date algorithm. Do not silently import Contract economics/refresh/feed writers into WB11. A3a removes the shown-date done-means; current internal date remains existing authority unless root records a narrowly justified named-helper successor. No holiday behavior has been executed/proved. Rollover now naturally invalidates dated cursors through existing context. Automatic visible refresh/date promotion remains its named later owner; opening never rebuilds.

## Minimal actual local screenshot method — identified, not run

Installed source inventory confirms bundled Node at `C:/Users/KierArthur/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin/node.exe`, bundled `dependencies/node/node_modules/playwright` version1.62.1; matching browser manifest revision1234 and installed `C:/Users/KierArthur/AppData/Local/ms-playwright/chromium_headless_shell-1234/chrome-headless-shell-win64/chrome-headless-shell.exe`. Ordinary TEST-Frontend also has Playwright. BUILD-FE package declares @playwright/test but has no proved own node_modules; use the already installed absolute bundle. No install/global PATH/browser settings/login/storage state needed.

After actual product implementation, use a one-off local browser invocation, not a reusable harness/tool. Load actual BUILD-FE JS/CSS via absolute local paths; mount the actual exported product entry in minimal standard-frame DOM with fixed actor/owner callbacks. Supply fixed **closed HTTP read responses** through the entry's existing authFetch option so the real transport/decoder/render runs; permit only known GET capabilities/Workbench reads and reject every mutation/external request. Do not bypass decoder by handing invented rows directly to a separate mock renderer; do not render/copy design-pack HTML as implementation proof.

Save only the minimal fixed-response JSON(s) needed for each genuine state alongside output under `docs/bpay-next/screen-designs/SCR-01/`, SCR-02/ and SCR-03/ (Candidate and Client). This is visual sample transport, no business rows/fixtures/seeds. Reuse current exact response context/row schemas and coherent money/counts, use unambiguous fictional sample labels. Capture actual desktop1600-wide dark, same desktop device-light (must stay dark), and phone390×844 for lists/windows, including Filters and each picker where applicable. Record product hashes/entry/response files and independent42-item PASS/FAIL/N/A review; screenshots prove visual source rendering only, not live integration/latency/money behavior. No application/runtime/scenario execution was performed by this investigator.

## Fresh source hashes

| Source | SHA256 |
|---|---|
| FE js/bpay-next-office.js | 48c9f4c014fc62f5d1b3c4f633d138f31d43aeb91124c66b41e10054b67505f2 |
| FE js/bpay-next-workbench.js | beea3265774d368d8b3332014235eb152cd48573997ca6775d24fc38e18fc14c |
| FE js/main.js | 366b867bc828d6014ed23399f85c770d64929724405151232a4390f99b561759 |
| FE css/bpay-next-office.css | a2acb9f831f878aaaad31aec450c9504747e0ed00581f8ccc5abf81724670444 |
| BE current1634 reader | 391f32cbecaf4b32a311931d69ed7cbdb0dd85c0bae11a2cdf6a3a07a2e9c02d |
| Pack SCREEN_DESIGN_POLICY.md | 8bc212b174469236f2b46dbf70eadc3664a946636458321cd3c77b7146323de5 |
| Pack DESIGN_INDEX.md | cba0ef6e87c8937d558b20ef9c9ee010fcc9f9a8b479b626351206d07076cffe |
| Pack T01_ready_list.json | 3feb615e970639be63a6e5b6f23d74848e3fa02b8b5e19c81458a53abd86fade |
| Pack SCR-02_filters.json | 9c442863ee17ea03c60b813858c09ee1c797b4f668b21cde0a031df2d92d88dc |
| Pack SCR-03_candidate_picker.json | fb36475511e0bec8e643640c63a934062bb3e783c8f5c24e53d2f0bb3614e32e |

Re-read saved source immediately before implementation/review because root may change it concurrently. No acceptance inferred from source hashes, existing units, fixed samples, or read-only investigation.
