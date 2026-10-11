# FC-14 native read windows and retained callers — source review

Current verdict: **SOURCE PASS for the native viewer and retained caller seams.** Both historical findings below are closed on the final source amendment. No remaining essential source defect was found in this scope. Source only: no product edits, SQL/database/business calls, tests, fixtures, hosted actions, browser actions, rendering or screenshots. A4 and joined/native interaction acceptance are NOT_RUN.

## Historical findings — both closed below

**F14-N01: restored parent retains its busy DOM.** `js/bpay-next-case-audit.js:58` now clears its logical busy flag even while covered by a child. However, the initial `onMount` at :64 only starts a read when `context === null`; it does not repaint an existing context. If a user opens a child Audit/Timesheet during an append, the parent discards the late response while inactive, clears busy without painting, then returns with disabled sort/filter and the old Loading more state. Repaint the current parent on mount before the optional native resumed read. No new frame or navigation protocol is needed.

**F14-N02: known stale-context Retry retains the obsolete cursor.** After an append refusal `BPAY_NEXT_CASE_FINANCE_CONTEXT_CHANGED`, the generic catch keeps the original after/context and Retry invokes `read(failedAppend)` again (:36,52–58). Every explicit Retry therefore resubmits the same now-invalid cursor. For that exact known stale-context refusal, use the existing user Retry to request the first page/current count and summary again. Keep ordinary transient append retries at their existing cursor. This is user-driven recovery, not an automatic retry or new control.

## Checked narrow native seams

`js/main.js:49726–49741` qualifies current owner before any Case-audit legacy lookup; only genuine LEGACY falls through. NEXT requires admin, the native viewer and the current actor/parent-prefix/context. Failure is handled locally and does not fall into legacy. The Case/Candidate report seed is normalized and checked before the native reader is called.

Candidate report opening has the same first NEXT branch at `main.js:277918–277919`. Eager Candidate-finance loading first calls `candidateFinanceOwner` before inspecting legacy app-state or in-flight data (`:277563–277574`). The Pay tab replaces old figures with a launcher before this fresh owner read (`:329654–329665`), and `updateCandidateAdvancesUI` checks qualified actor/owner before rendering legacy cache (`:280077–280080`). Owner-unavailable Retry calls the same freshly qualified fetch entry (:49711–49724). No eager legacy finance totals, fake zero or second report reader is introduced under NEXT.

The native window uses the existing standard frame and actual read fields. Fixed sections, headings, server sort, Active only, count/footer and the three summary facts are preserved. Monetary values are formatted from their decimal strings, without Number conversion. Case report rows retain an Audit action using exact Case/Candidate IDs; native Timesheet rows open their actual physical Timesheet ID. Unknown fields are shown as unavailable, and the stored native count/summary is not reconstructed from loaded rows.

Every read is bounded to100 rows and uses the shared finite contract; it validates native actor/scope/cursor ordering again before displaying rows. Sorting/filter changes restart the server scope, and continuous scrolling follows its native cursor. No financial POST, command journal, queue action, money calculation, export control or future FC02/EX50 behavior is added by this viewer.

Historical source snapshots at the initial checkpoint: viewer `ABD8D3C0F02483CF83B567ABCF6112C487F28965725E141623DF462857293E49`; main `B8C4EA867FD86CBBE510D709FFA13EE3FBB49FCA16F0A2E2765AB9DE478390EB`. These are superseded by the final amendment below.

## Final source amendment

F14-N01 is closed: the saved viewer's `finally` clears busy while current even when covered, and its `onMount` now calls `paint()` for every current restoration before the optional initial/resumed read (:58,64). No late covered response is installed; a restored parent regains its current enabled controls and native count/footer.

F14-N02 is closed: the saved catch receives the sanitized error and sets `failedAppend=false` for the exact native `BPAY_NEXT_CASE_FINANCE_CONTEXT_CHANGED` code (:57). The existing explicit Retry therefore sends `after:null`, `include_count:true` through `read(false)`, replacing rows/context/count/summary only after a valid current first-page result. Ordinary transient append failure keeps its original cursor. No automatic Retry, timer, Refresh control or new read kind is added.

Final reviewed source: viewer `85E4EE5F58149CD606A8FD68A3D71D551963B9A13ED9BA8C2CAE7AF3FE8D3AAA`; main `358B817D22CA9B0AFFAEB210486D655600595E60628AFDBF967CCF0CEDC072E8`; index `8F4561AD8B84AE49BE7793662BBE8E2E4480BC0AAA49BBA64DC74EAEFA68913F`. Native caller checks preserve the exact original actor/parent stack prefix/context and current NEXT epoch before opening and throughout reads. A genuine LEGACY result alone enters the original legacy owners. The new read viewer contains no mutation path, uses textContent for native values and keeps exact money strings/unknowns. Native visual, queue, database and joined behavior remain NOT_RUN; no item completion is implied by this source receipt.

## Late saved-byte source amendment

**SOURCE PASS continues on the complete current viewer**, SHA256 `ECC2F2C7C6512CB4696FC4A7F9D46AF9F2467F2596D3EA7B1F742CD664AF9BF5`. The former viewer hash immediately above is a historical checkpoint. Main, Office, finite contract and index hashes remain those already recorded; CSS at this source checkpoint is `CE94FEA2BED38497172819488F0346C59ADEBA2E03DDC303B6122BBFAB858D1D`. No remaining essential source defect was found in the late changes.

The initial-read failure now hides filters/table and supplies one primary Retry in the owned footer; a failed append retains the existing rows and its inline Retry (:38–39,52). The existing explicit retry behavior and actor/frame/generation checks are unchanged. Subtitle/footer live-copy markers preserve their actual native text; formatted-money markers retain exact decimal-string formatting for summaries, amounts and the four balance facts (:20,35,46,48). Status uses the existing native badge classes, and sort controls carry the native `data-ws-sort` exemption (:46,42). These are presentation seams, not additional financial facts or actions.

The Snoozed subtitle applies only to `TIMESHEET_SNOOZE` and formats its actual `created_at_utc` in London (:47,54); other rows keep Created. SQL's report source supplies that native timestamp and its Active/Paused status labels. Arrow/Home/End navigation focuses/highlights only loaded native rows (:67), while Enter/Open/Audit retain the existing current-frame native-ID guard. The covered-parent repaint and explicit stale-cursor recovery remain intact (:60–61,68). No new command, cursor protocol, automatic failure retry, calculated money or fake Case/Timesheet identity was introduced.

This amendment certifies source bytes and retained seams only. Actual screenshots/design differences are owned by the separate A4 review; its local-response label/actor corrections do not change this source verdict. Visual, database, native interaction and joined acceptance remain **NOT_RUN** here.
