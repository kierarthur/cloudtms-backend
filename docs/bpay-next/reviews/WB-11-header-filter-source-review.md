# WB-11 header/filter source review — 10 October 2026

SOURCE REVIEW: PASS for the WB11 current source scope — F1–F6 closed in the final saved snapshot. Historical findings/correction receipts remain below. Rendered-screen/screenshot acceptance NOT_RUN by this reviewer. Read-only review; no product edits, code/tests/application/SQL/runtime/fixtures or hosted calls. Parent alone writes product.

## Scope read

Read full approved `02_WORKBENCH.md:464–497`, all WB-11 ITEM/D01–D06 checklist rows, current `design/WB-11-current-scope-investigation.md`, applicable R2 read/revision/finite-scope clauses and owner A3 supplied to this task. A3 hides pay date before execution, requires Ready / Action required / Blocked main tabs, dark appearance even device-light, continuous scoped pickers and meaningful keyboard/phone opening. Ready table/ticks, Create, Export, Reset and later actual child issue-window joins remain their named subsequent card owners. This review does not reopen Rule B or any money/producers/parked F25 work.

## Findings requiring closure

### F1 — P1: header and visible issue body can adopt different revisions

`FE js/bpay-next-workbench.js:578–586` adopts its own coherent three-section/two-channel HEADER bundle. `:560` Refresh calls only this `load()`. The independently mounted `mountIssueLists` adopts its page plus two HEADERs at `:683–695`. `FE js/bpay-next-office.js:561–564` connects scope/kind only; its issue availability callback ignores `value.context`, and no shell/header availability is connected. Thus Refresh can show new Action/Blocked counts above an old still-visible issue page, or a separately loaded issue page can succeed at a newer revision than the shell. Each bundle is individually checked, but the actual composed screen is not checked across bundles. This affects the current header/filter join, independent of future WB14 child-window work.

Minimal closure: join the existing components' current availability/context and same six-field revision test; hide/refuse the composed factual screen while either required bundle is unavailable or mismatched. A real Refresh must re-read the active issue body too, with no automatic business retry. Keep the existing independent per-request cursor/decoder checks and use only the current scoped read owners. Do not manufacture matching counts.

### F2 — P2: continuous picker retains an unbounded all-page list

`FE js/bpay-next-workbench.js:614,625–628` appends every page with `rows=[...rows,...page.rows]`; `:620–623` rebuilds a DOM row for every retained row. The RPC limit is100, but accumulated client memory and DOM have no bound. The read is not capped at a product total, so continuous scrolling through a large real scope grows without limit. Foundation bounded-read rules and R2-F06's finite-client principle require bounded retained state, while A3 forbids replacing continuity with a product count cap/hidden Load more.

Minimal closure: a finite continuous viewport/window with genuine typed cursor continuation and bounded navigation state; retain chosen identity/label independently and explicitly discard off-window row data/DOM. Preserve search/scope changes, no client sort, native Client cursor order and exact stale refusals. Do not build an all-agency cache.

### F3 — P2: new shell misses actor/lifetime guards already required by its consumer

`mountWorkbenchShell` validation at `:551` does not require a UUID actor or validate an injected client's `loadWorkbenchPage`; `mountIssueLists` correctly does both at `:642–644`. Consequently a supplied client plus two empty actor callbacks can satisfy shell `current()`. Its load finally at `:586` checks ticket/closed only and renders/notifies after the actor/owner becomes stale. `:638` invalidate similarly renders/notifies without current guard. Child close at `:592` always restores focus to a connected previous element, including stale-actor/owner cleanup at `:636` and close at `:638`.

Minimal closure: same mandatory actor/client validation as the existing issue component; current-frame/actor guard before late publication, externally callable invalidation and availability callbacks. On stale teardown remove/disable factual content without publishing an old availability callback or moving focus into an obsolete parent. Restore child opener focus only when the actual original parent and initiating actor/owner are still current. No journal changes are needed.

### F4 — P2: failed filter count has no retry action; picker retry and append are not keyboard-safe

`FE js/bpay-next-workbench.js:607–611` leaves Apply disabled after a failed count and displays “try again” with no actual retry control; users must close/reopen or change a filter to repeat the same count. Picker `:632` installs retry on a plain `p role=status`, with no button, tabindex or keyboard handler. In addition, `renderRows` replaces the entire picker table at `:623` on each continuation; a focused row/Open button is removed without restoring its actual row identity, so keyboard focus can leave the active child during scrolling.

Minimal closure: visible native Try again buttons for the exact failed scoped read/count; same cursor and draft scope on retry. Preserve focus by actual selected/focused typed row identity when repainting the retained window, falling back inside the current child only if that row was honestly discarded. Keep Close/Escape and parent inert restoration bounded by F3.

## Source behavior already supported

- Three main tabs are implemented; no main Waiting tab or editable/shown pay date is introduced. CREATE call omits pay_date, preserving server authority and the DR01 optional transport contract.
- Header figures are server READY count/amount, with unknown represented as a dash, not GBP0. Five HEADER reads are adopted together only after common agency/actor/epoch/date/workbench/adoption equality. Browser formatting performs decimal display rounding only, not financial selection/repricing.
- Filters stage channel/Candidate/Client independently until Apply. Clear restores ALL/null/null and re-reads; Close discards only draft filter edits. Picker requests clear only their own identity and retain the other scoped filters; all requests limit100 and use existing typed client/decoder.
- Single click selects picker rows; double-click/Enter and phone Open choose; dialog Escape and focus trap exist, and nested parent inert state is captured. F3/F4 identify the remaining lifetime/accessibility gaps.
- CSS removes the light palette and provides dark scoped header/filter/picker support and bounded-height scrolling. Index changes only cache markers for the actual CSS/Office/Workbench assets. Main source is unchanged. Actual desktop/phone/rendered spacing, visual focus and screenshot42-item review remain NOT_RUN.
- Office's unsealed path mounts the new read-only shell. Its former sealed thin preparation/payment journal path remains; opening/Apply/Clear use existing reads and do not create/rebuild financial state. Later Ready controls and true issue child-window/return joins are not claimed complete here.

## Reviewed source hashes

- FE `js/bpay-next-workbench.js`: `02b92b27b5bc0efea2e8fc2d19481d01ca84c415b160f10cc13b9f4f2b7316bf`.
- FE `js/bpay-next-office.js`: `3a36115a79e22f9b2c2cdcd6ec36dcb50efa117036fadcef8bd2c7742b79bb05`.
- FE `css/bpay-next-office.css`: `0e88fe3f8d46bc6e3c5d3ac3d09b7c61a551ff1740d3e9761101064b52396ebf`.
- FE `index.html`: `9853c1e5051cd0ad5366e932d1197a95c4db36ed891b9d9b46a6c3d083dec6c8`.
- FE `js/main.js`: `366b867bc828d6014ed23399f85c770d64929724405151232a4390f99b561759`.
- BE saved design: `fc67248ea5f091f066003a0938fe74dbd55a519d7e81f524bffed1e5119d7fc6`.

These are the inspected disk snapshot; later root edits require fresh source review. No source PASS or runtime/screenshot PASS is inferred from hashes or parent-reported tests.

## Fresh correction review — standard frame / bounded picker snapshot

Read the full new shell and picker, scoped Office mount/render/lifecycle, full61-line shared UI adapter, actual main caller and existing standard showModal post-mount/Close seams. This is source-only, no screenshot/render claim.

- F2: CLOSED for accumulated row/DOM retention. Picker now stores at most three100-row pages plus at most32KiB of compact native start keys (`workbench.js:620,650–651`), chosen row separately and one pending retry cursor. Backward scroll reconstructs the exact cursor with the retained full context and native key; successful back removes that one history key and drops an end page. Forward scroll saves the removed page's actual starting key, not a guessed row key. Byte/row/type/context/receipt validation now also runs for injected client replies in readPage. Search resets context and finite state. This is a finite continuous window, not a product-total cap. On history trimming, an accessible same-search reset exists through Search; a short truthful discarded-history indication would improve usability without extending scope.
- F4: Count and picker now have native visible Try again buttons and retry the same current draft count / exact pending cursor. Picker row focus identity uses data-next-focus-key, with arrow/Home/End movement and ui.capture/restore. The current phone Open button has no focus key of its own: when it is focused during continuation repaint, restore falls back to Search. Add an exact native row-derived Open focus key if that control's continuity is required; row focus itself is retained.
- F3: Mandatory UUID actor, pure full injected-response validation, actor checks before/after read and current() in shell load finally are CLOSED. Children now use the existing standard modal adapter, alive retained parent and initiating-actor focus guard instead of another overlay. The shell external invalidate remains unguarded at workbench.js:662; ownerChanged at:660 still renders/notifies after current becomes false. Do not let a closed/stale shell invoke caller availability/selection callbacks. Perform only factual teardown in that case.

### F1 remains OPEN: comparison does not yet control factual display

Office now wires Refresh to issueLists.reload and calculates issueReadAvailable from sameWorkbenchRevision in both callbacks (`office.js:563–567`). This closes the omitted refresh call, but the variable still only affects the legacy money-control locked expression (`:600`). The unsealed render (`:603–607`) always appends the unchanged issueHost, and shell render decides header availability using only its own bundle (`workbench.js:568–577`). Therefore a header/issue mismatch is detected internally but both independently current factual bodies remain visible. Connect this exact joined availability to the actual displayed body/header, or make the existing issue component refuse/hide until the shell context matches. A false boolean unused by presentation does not close the composed source contract.

### F5 — P1: an alive standard child can permanently dispose the issue component

The standard adapter reuses one modal body and retains parent content while a child is topmost. Shell current() explicitly accepts readWindow.alive (`workbench.js:565`, Office:563). But mountIssueLists still requires root.isConnected (`workbench.js:685`), and Office does not pass the alive callback to it (`office.js:566–567`). If an issue read is in progress when Filters/Picker replaces the parent body, its finally treats the detached retained parent as stale, closes the component and removes its root (`workbench.js:720`). Closing the child cannot restore that disposed issue root. Filters can open while its independently loaded issue page is pending, so this is a real async seam, not a hypothetical unsupported call.

Minimal closure: pass the actual same Office frame alive callback to this existing child component and separate still-alive retained parent from closed/stale actor; guard post-await publication with that real lifetime. Do not add another frame or protocol. Restore focus only when the real parent is again active; detached parent data adoption can remain private until its standard frame remounts.

### Fresh snapshot hashes

- FE `js/bpay-next-workbench.js`: `4dd7f73f1cfc36cc911dee2c6a67b9d86cd6d961c16a608f2b1bf3e6761aa44f`.
- FE `js/bpay-next-office.js`: `980055953f2be0301f10b445746979a3782ff67d9058270dd40f33860ef2bb57`.
- FE `js/bpay-next-ui.js`: `43f4f26fa3932f8dc00bac1c24a00e0d19e1ddd937067741570c8f8ecf720aef`.
- FE `js/main.js`: `ca8a67365f8c130674567e84aa50d3d944a8107a504c066d767a61ae3f5f6e53`.
- FE `css/bpay-next-office.css`: `1d50ba06fc322293a13c1d45e3eef772365f3381c66c64d557ec9153afbd99af`.
- FE `index.html`: `9853c1e5051cd0ad5366e932d1197a95c4db36ed891b9d9b46a6c3d083dec6c8`.

Current source verdict remains OPEN for F1/F3/F5 and the narrow phone focus continuity above. Screenshots/application/timing acceptance NOT_RUN by this reviewer. Original findings are preserved as provenance, not a claim that corrected source still has their superseded form.

## Final correction snapshot — original findings closed; one remaining async seam

Fresh full shell/picker and scoped Office/issue lifecycle inspection confirms:

- F1 CLOSED: shell now stores issue availability, joins the exact common context and hides the real issueHost unless coherent (`workbench.js:568,578,663`); Office passes issue availability into that setter (`office.js:567`). Refresh reloads both current components. Mixed independent header/body revisions can no longer display a positive joined issue body. This is a presentation join only, no finance or producer rewrite.
- F3 CLOSED: externally invoked shell invalidate requires current; owner change clears/removes factual shell without rendering/notifying stale caller data. The required actor and full typed injected response checks before/after awaits remain; shell load finally checks current. Standard child opener focus restoration checks the initiating actor and original parent currentness.
- F5 CLOSED: Office passes actual readWindow.alive to mountIssueLists; current() accepts that still-retained parent lifetime plus the unchanged initiating actor/owner. A child temporarily replacing the standard body no longer permanently disposes the in-flight issue component. Closed/stale actor still removes it.
- F2/F4 CLOSED as scoped: finite three-page row/DOM window,32KiB native-key journal, genuine backward/forward cursors, native count/picker retry controls and exact focused row/phone Open identity are present. Phone button now has `picker-open:<actual native ID>` and the shared capture/restore preserves it. The same-search Search action genuinely resets the bounded walk. There is no total product row cap or all-page agency cache.

### F6 — P2: pending Filter count is lost while its picker child is active

Filter count at `workbench.js:611–613` admits result/error only if `entry.active()`. Candidate/Client controls remain available while the initial count is pending. Opening either picker makes Filters an alive inactive retained parent. If its HEADER completes during that child, the successful count is discarded by the active check. Closing the picker without choosing triggers no count refresh; parent remains “Loading…”, Apply disabled and Retry hidden. An error arriving during the child is similarly discarded. This is the same actual standard-parent lifetime distinction already fixed for the issue component, within the new WB11 filter flow.

Minimal closure: expose `entry.alive()` from the existing standard frame and use alive + current actor/frame + count ticket/closedFilter guards when adopting the private detached Filter count/error. Keep active checks for user interactions and focus. A chosen picker still starts a new count ticket, so old scoped replies remain discarded. Do not add automatic business retry or another window protocol.

Source verdict at this snapshot: OPEN only for F6. No further money/schema/Worker/producers or future Ready/child-window actions are required by this finding. Screen42-item rendering review remains separate and NOT_RUN by this reviewer.

### Current source hashes

- FE `js/bpay-next-workbench.js`: `b7a87f8d4069d0aa5100442187291995b25a6f9a421ac47773aba8ac3dd96df6`.
- FE `js/bpay-next-office.js`: `980055953f2be0301f10b445746979a3782ff67d9058270dd40f33860ef2bb57`.
- FE `js/bpay-next-ui.js`: `8ed3df981b2d100a6c559b6388027917acb62fce8ef504637283783d3df7e857`.
- FE `js/main.js`: `75cc6ad369cb7a41231b1e0e200759495652a8e1508ce9e4e482c6cd5e405200`.
- FE `css/bpay-next-office.css`: `916f0afb2d7dd852c54f24b299df51c5b450e12f71017325fdc2eadc031d31d4`.
- FE `index.html`: `9853c1e5051cd0ad5366e932d1197a95c4db36ed891b9d9b46a6c3d083dec6c8`.

## Final source closure — PASS

Fresh re-read confirms `child()` now exposes the actual standard frame alive method, and both Filter count success/error paths use alive + current actor + exact count ticket (and closedFilter for success). The private retained Filter parent can accept its scoped count while the picker is topmost, without focus publication; active checks remain on actual picker interactions and focus. Choosing a new identity still increments the count ticket and rejects an older reply. F6 CLOSED.

Together with the preceding fresh closures, no concrete open source finding remains in this scoped header/filter/picker and actual Office/standard-frame join. D01–D04 existing bounded typed readers/transports are reused; D05 source implements the owner-A3 header/tabs/dark/staged-filter/read-only opening contract. This does not close later WB12/14/17/18/21/28 actions, visual42-item screenshot review, app timing, runtime/data or whole ITEM acceptance. No product edits or tests/SQL/app/scenarios were performed by this reviewer.

Final inspected hashes:

- FE `js/bpay-next-workbench.js`: `b7a87f8d4069d0aa5100442187291995b25a6f9a421ac47773aba8ac3dd96df6`.
- FE `js/bpay-next-office.js`: `980055953f2be0301f10b445746979a3782ff67d9058270dd40f33860ef2bb57`.
- FE `js/bpay-next-ui.js`: `8ed3df981b2d100a6c559b6388027917acb62fce8ef504637283783d3df7e857`.
- FE `js/main.js`: `75cc6ad369cb7a41231b1e0e200759495652a8e1508ce9e4e482c6cd5e405200`.
- FE `css/bpay-next-office.css`: `916f0afb2d7dd852c54f24b299df51c5b450e12f71017325fdc2eadc031d31d4`.
- FE `index.html`: `9853c1e5051cd0ad5366e932d1197a95c4db36ed891b9d9b46a6c3d083dec6c8`.


## Refreshed current seven-file closure — SOURCE PASS

Fresh read of the current scoped diff and saved shell/picker/Office adapter, shared modal utility branches and modernisation seams confirms no new concrete source finding. This receipt supersedes the preceding final source hashes only; earlier findings and correction history remain provenance.

- H12 size classes: the real standard frame toggles exactly one small/wide class at mount (`ui.js:43–44`); dismissal removes both (`:53`). The exact Banking NEXT selectors use 600px/1320px maxima within the existing viewport width (`css:71–72`). Parent remount restores its own size and hidden-tab configuration; no extra overlay is introduced.
- H13 Choose hint: Candidate/Client selection remains disabled until a genuine current native row is chosen. The retained hint appears only for successfully loaded nonempty pages with no pick (`workbench.js:625–627,641`); current single-click/Enter/double-click/phone Open guards and row-derived focus identity remain intact.
- Typed stale-page refusal: the picker catches the transport's reloadRequired/reload distinction, disables Choose and displays “Payments changed; refresh to continue.” Refresh clears old context, retained pages, native-key history and selected identity before requesting the same scoped first page (`:626,654`). Ordinary read failures still retry the exact pending direction/cursor. No automatic retry or money action is added. Successful/backward/forward rendering remains three pages plus a 32KiB compact cursor-key journal.
- Accessible live errors: the real header, Filter error and picker status nodes carry the retained live-copy marker; picker status changes to role=alert on refusal (`:560,607,624,641,654`). Modal copy cleanup now preserves these initially empty nodes (`modal-modernisation.js:1266–1268`), so late factual failure text retains a connected recipient. No error payload or internal identity is substituted into display.
- Existing modal protections: text-node money formatting skips only explicitly already-formatted descendants (`modal-modernisation.js:350`); NEXT header and Filter count mark their exact formatted facts. Banking NEXT uses the specialised layout branch (`:1371–1374`). The exact utility-kind additions in main.js preserve component-owned disabled states and standard Close/Save behavior; the current-frame keyboard guard prevents retained parent Escape/Tab handlers from acting on a child (`main.js:337904–337929`). The same initiating-actor, alive/active frame, ticket, injected-response and finally guards reviewed above remain present.
- Office still mounts this read-only shell into the existing actual standard frame, passes the same alive callback to shell/issues and joins exact common revision context. Index includes the current UI/Office/Workbench/CSS/main/modernisation cache markers. The additional existing weekly-source include query strings are cache markers only; their source files are unchanged in this seven-file diff.

Current-source verdict: PASS for the scoped WB11 source subset with F1–F6 closed. The exact current seven-file raw SHA-256 snapshot is below. This does not certify visual/screenshot policy, browser/app timing, business/runtime acceptance, whole ITEM completion or later Ready/Create/Export/Reset/issue-window cards. No execution, tests, SQL, product edits or hosted operations were performed.

| FE saved file | Raw SHA-256 |
|---|---|
| `css/bpay-next-office.css` | `52203a80b9dbc464d3d87f7d02cdfe3e4d6ce3f2c59fa253e96dd66dad85e145` |
| `index.html` | `d67ad31b78c1f66de0bd9ece500f64d1496e2c2810179fd4c88f4ce7910d9f91` |
| `js/bpay-next-office.js` | `34ec7991ec930be29f260b71522ab7d01c370f7430742e09b9755a972769efff` |
| `js/bpay-next-ui.js` | `dbdcb12ed8ec027b0df39c52ef6f50b6d91ca93e2cb74451a59e09a6ba029272` |
| `js/bpay-next-workbench.js` | `2208826185c9947f0d010e806e4c98ef5095be4b042b818ac88a123462e788af` |
| `js/main.js` | `7cde52d8f113f9c35f19f2f0be198390e2d742118c991b98e29ca06dcc645845` |
| `js/modal-modernisation.js` | `9bf06878d57960ae82e1967e71452ceadec0cf3a603c946effc1df49d88df36e` |
