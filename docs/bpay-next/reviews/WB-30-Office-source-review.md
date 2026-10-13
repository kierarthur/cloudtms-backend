# WB-30 Office source review

13 October 2026. Independent saved-source review of the new, not-yet-wired native `js/bpay-next-rates.js`, against the complete WB-30 card, reviewed build/native-reader boundary and original W1/W2 closed contracts. Root alone edits product. No product/checklist change, execution, SQL/RPC, business/database/data, fixture, test, runtime/render/capture or hosted action was performed. SOURCE-only; UI wiring and A4 remain separate.

## Initial findings checkpoint

**O30-01 — linked-off quantity qualification, CLOSED on fresh source.** Initial read found `g.root_units` absent from the preceding reviewed closed DTO, and root was notified immediately. The current native reader and BE/FE closed DTO now expose and validate this genuine server root quantity; the window consumes it without browser aggregation. This supersedes the initial absence observation; no new financial authority is created.

Root separately identified startup recovery-before-reader and zero-consent-before-dialog-close corrections that are pending in the first reviewed rates bytes. Source entry/CSS are not yet wired; no joined or visual PASS is claimed. Remaining source checks continue; no overall PASS at this checkpoint.

**O30-02 — retained evidence loses earlier pages.** `loadEvidence` drops the oldest page after three, retaining only the forward `after`; there is no backward page/cursor history or re-fetch path. Earlier details become permanently unreachable until collapse/reload, which merely toggles the same retained object. `paintEvidence` restores the old numeric scroll offset after shortening content, so the visible anchor also changes. Preserve bounded current pages with bounded native cursor history and a backward re-fetch/row-anchor path. This is the required continuous bounded child browsing, not an all-page array or new reader.

**O30-03 — accepted receipt must survive parent refresh failure truthfully.** `prepare`, `recover` and `retryOriginal` set the validated accepted `result` and invoke `options.onChanged` inside the financial try. If the parent callback throws, the outer catch reports a rates-save failure despite the native accepted receipt. Catch callback/refresh failure separately and retain success as authoritative. Do not reset the original journal or issue a replacement command.

O30-02/O30-03 sent to root immediately. The original command controller still owns persistence-before-dispatch, immutable exact request bytes and receipt/readback-first retry. Local input handlers currently call no reader or financial command.

Initial current component SHA-256: `2cec274b28bfff04447d27cca8ecb30efa9de3aee5d806b739d4bb7e624746da`; current FE closed-resolution helper SHA-256: `e1b5511476a3b7324144e14dd25a444899caaaE7b88c50105a3704f96ce64c51` (case-insensitive hex). Overall source remains OPEN pending these fixes, startup/zero correction and real entry wiring.

**O30-04 — retain the actual caller/parent fence.** Component `current()` checks owner/actor and frame lifetime but omits the supplied `options.current()` callback. It passes `{...options,current}` to the original command controller, replacing the host fence with this weaker check. A replaced caller/namespace under the same actor and NEXT owner can therefore accept late read/recovery effects. Require the existing captured caller callback and include its literal true result in `current()`. Existing native forms retain this seam; no new context framework is needed. Root notified immediately.

Presentation advisory: initially blank result/header nodes should use the existing live-copy marker where the native modernizer removes empty content. A4 owns actual pixel verification; no visual finding is asserted from source alone.

## Fresh partial closure

O30-03 CLOSED: the shared `acceptReceipt` retains validated receipt success and catches parent refresh failure separately. Startup now reads the original closed journal before live discovery, allowing Processing reconnect; the original controller still validates namespace/body and uses exact readback. Unknown requests can close and reopen without deletion. Zero confirmation disables the confirmation action before its window closes and rechecks consent before dispatch.

O30-02 now has bounded three-page retention, 32KiB cursor history, native backward/reset fetches and exact row-anchor restoration. **O30-05 — retry must preserve the failed evidence tuple:** current retry always invokes default forward/current-tail instead of the failed back/reset request. A terminal retained tail makes that retry return immediately through `detail.done`, leaving the error unrecoverable. Preserve/retry exact failed `{direction,requestedAfter}`; successful navigation may clear it. Root notified. O30-04 remains OPEN in the fresh bytes pending caller wiring.

O30-04 CLOSED in later saved source: `options.current` is required and retained in `current`; issue/detail/Candidate entry paths supply their actual bounded native lifetime/context closures. The live-copy markers are present. Accepted receipt rendering is permitted through the still-owned view after the parent refresh invalidates the mutation context, while new mutation remains fenced. O30-05 is the current essential Office blocker.

Consent advisory sent to root: the confirmation prints local input rates after W1 preview, while the preview loop checks only nonnull preview. Suggested preview is already qualified against the new native suggestion by the shared DTO, but a finite old-display versus actual preview-rate equality check (or displaying the exact native rate) closes the visible consent seam without pricing or a new revision. Preserve inputs on changed quote and refuse/Refresh; do not silently adopt a new rate.

**O30-06 — journal-first recovery needs a reachable current entry.** Every newly wired issue/detail/Candidate Review-rates control requires the current `RATE_RESOLUTION_REQUIRED` reason. A possibly committed request can put the Work into Processing and remove that reason; after close/reload the startup journal-first recovery then has no reachable control. Add a finite retained-journal-aware entry for its exact original Candidate/Work through an existing native menu, with the original controller retaining all closed namespace/body validation and readback-before-retry. Do not open fresh rates for every Processing row or make a new store. Root notified immediately. O30-05/O30-06 currently OPEN.

## Completed initial source census / OPEN verdict

All initial source paths were read. **OPEN only for O30-05/O30-06 and the actual-preview consent amendment accepted by root.** O30-01–04, original startup ordering and pre-close zero gate are closed in the current bytes. Root acknowledged and is correcting the remaining finite seams. Scoped CSS and A4 are not yet complete, so no pixel/whole-card PASS is claimed.

Input and reason edits remain local; no typing handler calls a reader or mutation. Apply derives only disclosed native Work/revision/line/STANDARD/bucket rows, retains exact expected decision ID/version pairs, obeys the explicit linked checkbox and uses the original common write context. Manual versus suggested selection uses exact decimal equality only; all prices, quantities and margins displayed originate in the native reader. Exact original W1 preview revalidates the requested rows and context before confirmation. A missing suggestion remains blank, blank/invalid rates refuse, manual reason is required by the original closed validator, and intentional zero cannot dispatch without literal consent. Material typed changes warn before close; discarded edits make no financial call.

Original controller request normalization retains aggregate 100-row/32KiB limits, native actor/Candidate/agency/owner/context qualification and journal persist-before-dispatch. It owns exact readback before original-byte retry and preserves unknown outcomes without new UUID/rebase. Component evidence uses native current context and qualified parent IDs, three retained pages, bounded 32KiB cursor history and row anchors; only the failed-direction retry needs amendment. Accepted per-Work results describe Processing; callback refresh failure does not replace native success. Late effects and new commands require the actual parent closure; accepted display can remain in the owned view after parent refresh invalidates mutation eligibility.

### Current checkpoint SHA-256

| Saved FE file | SHA-256 |
| --- | --- |
| js/bpay-next-rates.js | `8b1e758183ad3f8fe259efb54c78eeb3b9d0c5ccdd4b2b9d15ae800acf7c9ffa` |
| js/bpay-next-workbench.js | `19e0295d41464853681051fb0180ee046bf48265079f062bb5fff9651e37b8ce` |
| js/bpay-next-resolution.js | `e1b5511476a3b7324144e14dd25a444899caaae7b88c50105a3704f96ce64c51` |
| css/bpay-next-office.css, before new scoped CSS | `a046a6ea08037328c966588d912775a3cc83404341b228ed76021a05f524fe14` |
| index.html | `737d733daca04c554874c1a51132054d903c035dc402624d015e2d84ecf28c4f` |

This reviewer performed source reads and review-document writes only; AUTHORED_NOT_RUN. Root alone owns the product corrections and normal checks.

## Saved delta review

O30-05 CLOSED: evidence stores the actual failed `{direction,requestedAfter}`, retries exactly that tuple and clears it only after a successful page. Back/reset retry no longer becomes a default forward call blocked by a terminal tail. Existing row anchors, three pages and 32KiB history remain bounded.

The actual-rate consent advisory is CLOSED: every W1 preview row must have a native quote rate exactly decimal-equivalent to the chosen value at the disclosed Work/line/bucket key. A mismatch takes the existing stale refusal with inputs retained. The shared typed response already qualifies the explicit rows/intent; confirmation cannot silently adopt a newer suggestion. Negative fractional margins retain `-0` in display, and linked-only groups no longer borrow the root job/band label.

O30-06 now has the required read-only `hasRetainedRequest` using the existing journal key and original controller validation. Candidate and detail entry paths bind actual context and original Work correctly; opening recovers original bytes before live discovery. **One finite issue-path remainder:** `issueActions` receives a raw row without context, and the mount caller does not supply its local context in options. The helper therefore returns false for a Processing issue row after reload. Pass the existing context in that one call; no new field/store/reader is required. Root was notified. This is the sole current source finding; scoped CSS/A4 remain separate.

Delta raw SHA-256: rates `757a7ee3a84516adca2016986e60ee91470e5b104ba101bc85478d98020e1bac`; Workbench `708bce00e735e2d13c95d7d28cac02f32689d92bc764676ecdafefc74f636e97`; FE resolution helper, pre-CSS stylesheet and index remain at the preceding checkpoint hashes.

## Final behavioural source override

**SOURCE PASS for the current rates component, its closed DTO and existing native caller entries; O30-01–06 CLOSED, no remaining essential source finding in this scope.** This supersedes prior OPEN statuses. The final issue-path call now passes its actual local `context` to the read-only retained-request test, so all three native entries can reach original-journal recovery even when the Work is Processing. No new store, request, rebase or financial command was added. The original controller supplies closed namespace/body validation; detection merely reads and qualifies the existing original Work. Startup recovers original bytes before any live discovery.

Exact failed evidence direction/cursor, actual preview-rate consent, root native quantity, parent caller fence and authoritative accepted-result handling were re-read in the saved bytes. Every preview quote must equal the chosen exact native Work/line/bucket rate before confirmation. No typed edit calls the server. Missing, zero, signed margins and manual-reason semantics retain the original closed contract. Backward paging/row anchors retain bounded native pages/history; retry follows the failed tuple.

Static text comparison confirms the current FE validation block still exactly matches BE after export syntax removal, and the complete original FE command-controller body still exactly matches HEAD after newline normalization. Its persistent original bytes and readback/retry semantics are unchanged. Root-only normal checks and joined browser/runtime acceptance are not claimed by this reviewer. New scoped CSS and independent A4 are still in progress and are outside this behavioural source PASS.

Final reviewed behavioural raw hashes:

| Saved file | SHA-256 |
| --- | --- |
| FE js/bpay-next-rates.js | `757a7ee3a84516adca2016986e60ee91470e5b104ba101bc85478d98020e1bac` |
| FE js/bpay-next-workbench.js | `4adce5296e9148e47500a5ee064e2a8ce298aeb4452fc61e2408fc4328baa26e` |
| FE js/bpay-next-resolution.js | `e1b5511476a3b7324144e14dd25a444899caaae7b88c50105a3704f96ce64c51` |
| BE broker/src/bpay-next-resolution.js | `e638be80f1cf96e31cad575dbb69a1f003480cb6627b33a1b88761624a144309` |
| FE index.html | `737d733daca04c554874c1a51132054d903c035dc402624d015e2d84ecf28c4f` |


## Final UI source amendment

**SOURCE PASS retained; O30-01–06 remain CLOSED.** The final added component evidence uses the existing closed `original_basis.work_date`, `source_units`, `source_rate`, `source_charge_rate`, native Work week and native suggestion quote rate. Its table appears only for more than one disclosed component. Those fields and exact Work/line/bucket membership were checked against the shared validator; missing native values remain a dash. No browser pricing, unit aggregation or expanded command scope was added.

Confirmation now labels the actual selected rates for each exact requested Work/line/bucket. Every value already passes the native W1 preview-rate equality check before consent. Normal accepted results join each validated receipt change back to that same retained choice. Cold journal recovery omits rates when the original offer is unavailable, rather than guessing from a new reader. Root job/band qualifiers appear only on groups containing an original Work member. Negative fractional values preserve their signed `-0` prefix. Existing actor/parent fences, exact persistent request bytes, unknown recovery, bounded evidence pages and failed-tuple retry remain unchanged.

Native button/badge classes and scoped rates/confirmation/phone styles were read. The absent warning-token advisory is CLOSED: the warning border now uses the existing native warning value `#f6bd52`. CSS changes do not change financial values or command predicates. This is source review only; root/independent A4 own actual screenshots and whole-card closure. No product, database, browser, fixture, test or business execution was performed by this reviewer.

Final saved source SHA-256 (latest rates bytes re-read after the in-progress presentation save):

| Saved file | SHA-256 |
| --- | --- |
| FE js/bpay-next-rates.js | `0c278f4fdf8057d55aaeded06131941186b2660818e35f450db92396d45cba3c` |
| FE js/bpay-next-workbench.js | `4adce5296e9148e47500a5ee064e2a8ce298aeb4452fc61e2408fc4328baa26e` |
| FE js/bpay-next-resolution.js | `e1b5511476a3b7324144e14dd25a444899caaae7b88c50105a3704f96ce64c51` |
| FE css/bpay-next-office.css | `cd4cdd69bd4ce4ee5856fbfba595f94f971e98318ef71f783dcae8c1c8d57e69` |
| FE index.html | `737d733daca04c554874c1a51132054d903c035dc402624d015e2d84ecf28c4f` |

## Final A4-driven source closure

**SOURCE PASS; zero open source findings.** Re-read the final rates/CSS delta after A4. Money display pads to at least two pence digits without rounding or removing any of the native six fractional digits, and preserves signed `-0`. The evidence region has a plain accessible rate-details name; failed reads put an alert and exact-tuple retry before the retained facts, while loading uses status and the native live-copy marker. Accepted-result footer hiding requires an actual result with neither notice nor error. Pending recovery retains its original controls; missing human Timesheet/rate fields after cold journal recovery are explicitly disclosed rather than reconstructed.

The local pre-dispatch normalization catch now preserves the original `SCOPE_TOO_LARGE` refusal and separately reports a reason exceeding the original 2,000 UTF-8-byte limit. It changes neither the closed request, owner/context comparison, W1 preview, persistent original bytes nor native command dispatch. Child phone target sizes, flat evidence tables, native amber line and result wrappers are scoped presentation changes only. Existing selection/zero consent and six closed findings remain unchanged. This concludes the authorized source audit; actual A4 and runtime conclusions belong to their separate receipts.

Latest final SHA-256:

| Saved FE file | SHA-256 |
| --- | --- |
| js/bpay-next-rates.js | `d00310c17354d771b9b6a29e5c1b15402721a6f8ff2be82f82f8ec3dccbf5e35` |
| css/bpay-next-office.css | `ea6043f194857e93d45a50291330604779bb7c951c5f49142c81199b23968395` |
| js/bpay-next-workbench.js | `4adce5296e9148e47500a5ee064e2a8ce298aeb4452fc61e2408fc4328baa26e` |
| js/bpay-next-resolution.js | `e1b5511476a3b7324144e14dd25a444899caaae7b88c50105a3704f96ce64c51` |
| index.html | `737d733daca04c554874c1a51132054d903c035dc402624d015e2d84ecf28c4f` |

New CSS arrived during the final saved-source read (raw hash `d314efa85845bc0fc7277a2e78940e65c14aa1ce004223794afcb9fb7dca9767`). It confines rates table/evidence/header/phone additions to native NEXT modal/rates classes and preserves earlier stylesheet rules. Presentation advisory sent to root: `--ctms-modal-warning` used by the new warning border is absent from the current native token inventory and needs an existing native tone/value. Root owns correction and A4; no pixel PASS follows from this source receipt.

## Last CSS-only source override

**SOURCE PASS retained, zero open findings or advisories.** Final stylesheet read confirms the main rate table's first/last sticky header cells receive scoped 13px top corners. The phone confirmation selector now targets its actual native `.bpay-next-confirmation-footer .btn` controls with 44px minimum dimensions. These are presentation-only corrections; no values, guards, command or journal code changes. This current verdict supersedes the historical warning-token advisory above, already corrected to native amber. No visual or runtime check was performed by this reviewer.

Current FE `css/bpay-next-office.css` SHA-256: `9f07e5308a133a7caf06e5ffc7f50793489ec5ee1cfd2584253840b37a645805`. All other final hashes and source conclusions remain as recorded in the preceding closure.
