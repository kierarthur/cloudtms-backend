# WB-22 Office and transport source review

Independent saved-source review, 10 October 2026. No product edits, module execution, tests, SQL, application calls, hosted reads or fixtures. This reviews the current WB-22 draft against its card and selection-memory design; it does not approve the whole item or its future joined acceptance.

Final current source verdict: **SOURCE PASS** for the reviewed WB-22 transports/store/Office changes. W22-J01–J09 and the remembered-NONE correction are closed by the final reread below. Earlier checkpoint findings are retained as evidence. This does not claim whole-item joined/runtime acceptance.

## Findings

### W22-J01 — Filters picker invokes the Ready-only store

FE `js/bpay-next-workbench.js:908` calls `choiceStore.adopt(context,header.selection_mode)` and observes Candidates inside the `PICK_CANDIDATES` / `PICK_CLIENTS` read callback. This callback belongs to `mountWorkbenchShell` / `openPicker`, which has neither the Ready `choiceStore` nor its HEADER. The picker row contracts also lack `selection_basis`. Thus a valid picker reply reaches a reference error, falls into its normal read catch, and displays a failed list. Remove these two mistaken operations from this picker callback; retain the actual Ready store adoption/observation at the Ready read seam.

### W22-J02 — Completion does not pass the accepted baseline run

FE `js/bpay-next-office.js:669` initially called `store.acknowledge(state.descriptor.revision)`. The new store method (`js/bpay-next-workbench.js:966`) requires `(revision,runId)` and only saves when `runId` is a UUID. Consequently a factual nonempty COMPLETE acknowledgment retired the submission journal while leaving the local choice store dirty and on its previous baseline. Pass the actual retained CREATE run identity, retaining exact revision equality and the existing terminal-receipt requirement; do not infer the baseline from a mutable workbench response. EMPTY has no real agency command or acknowledged server baseline: retiring its factual journal while keeping local choices is correct and is not this finding.

### W22-J03 — Candidate child read is unbound and invokes nonexistent locals

At the initial checkpoint, `openCandidateWindow` defines `store` and `headers`, but its read tail (`:1358`) invokes nonexistent `choiceStore` and `header`. A valid child reply therefore fails like the picker. Its actual page request (`:1351`) and four HEADER reads (`:1355`) also omit `selection_memory`; the reference-bearing `args()` function (`:1311`) is unused. This substitutes the narrowed Candidate's latest own namespace for the parent store's original namespace and can drift from dirty Keep. Use the actual store, pass its exact reference to every child read, bind actual response context/HEADER memory to it, and observe native Work/Case identities according to their typed basis rather than observing every group as a Candidate parent.

### W22-J04 — New baseline refresh retains old page defaults

`sameWorkbenchRevision` (`:670`) compares financial/adoption revisions, agency, actor, epoch and date, but omits actual `selection_memory`. Ready refresh (`:1044–1045`) clears retained blocks only when those revisions change or a cursor restart occurs. If another sealed run acknowledges a new baseline while the maintained projection revision is unchanged, refreshing the first block adopts the new baseline and keeps the second/third blocks with old `default_selected` values. Treat baseline change as a retained-page invalidation at this selection-bearing seam. Never retag old cursors or silently reuse their defaults.

### W22-J05 — Export Case qualification disagrees with remembered defaults

Worker `exportIncluded` (`broker/src/bpay-next-workbench.js:661–663`) now uses `row.default_selected` for the baseline-aware descriptor. However its qualification walk (`:706–709`) and untouched-Candidate qualification at output (`:725`) still use raw ALL_MATCHING/NONE. For an acknowledged NONE baseline, a new eligible Work and Case credit can both have genuine default-selected rows with no local overrides: Work is included, but the Case credit is marked excluded because untouched NONE never qualifies the Candidate. Derive the bounded qualification from the same effective remembered defaults/overrides as output selection. Preserve Candidate false dominance and the existing requirement for a selected payable Work/credit; component intent alone must not promote Work.

### Additional confirmed accidental issue-view insertion

The initial `mountIssueView:1113` contained the same `choiceStore`/`header` adoption and Candidate observation as W22-J01, although that view owns neither and its issue rows have no memory basis. Root has since removed this insertion on disk. Picker and issue corrections will be hash-bound in the closing reread.

### W22-J06 — New reply fields are typed but not correlated

Both W1 mirrors currently shape-check HEADER `selection_memory`, `selection_mode` and `remembered_intent`, without requiring the row's reference to equal `context.selection_memory`, or the retained nonnull intent's mode to match `selection_mode`. A well-typed conflicting HEADER can initialize/adopt the wrong mode under the actual baseline. Likewise the new per-row counters lack `choice_selected_count <= choice_member_count`, and the generic basis union is not correlated with native row kind/identity. Add finite cross-field checks in both mirrors, retaining legitimate nullable/ineligible cases and native canonical identities. This is a closed-reply validation correction, not new financial evidence.

### W22-J07 — Detached Clear all exports fresh ALL defaults

`setMode('NONE')` deliberately creates `memory_baseline_run_id:null`. The detached EXPORT_ROWS read has fresh native defaults (eligible rows default true, since its helper receives no local intent), but `exportIncluded:661` treats presence of the scalar as remembered memory. It therefore uses those true defaults and marks untouched Work included after Clear all. Distinguish a nonnull acknowledged baseline from an explicitly detached null reference: the latter must use the descriptor's fresh ALL_MATCHING/NONE mode, just like Count/Create. Keep descriptor-format awareness separate from actual-baseline presence for the permitted hidden false-parent component memory.

## First correction reread

FE workbench `5e93e98675733891c491eda96f637297d8f422c61b1e91c0806b7d3a558caeaa` and Office `8d0016048c316d8e6d911f6720be9f1cd1ac7096d3f2f31bc3ae3f7b83b99cf7` close W22-J01–J04. Picker/issue accidental operations are gone. Child page, all four HEADER reads and Timesheet-return check carry the original store reference and fence local revision/reference; the child observes Work and Case-row basis without adopting a narrowed HEADER. Ready invalidates retained blocks on actual baseline change. COMPLETE acknowledges the exact retained run ID; EMPTY correctly creates no acknowledged baseline. Remembered NONE Discard remains corrected. W22-J05–J07 remain open at this checkpoint.

### W22-J08 — Hidden dirty basis is not qualified before count/freeze

Normal source-path example: untick a Candidate on an older page and Keep; its stored original basis is FV1 with no unresolved entries. That Candidate then changes to FV2. Reopening reads only the first Ready page, which does not contain the retained untick. `observe` never checks it, and initial `countIntent`/`freeze` accepted the old false override. Original local FV1 is not in Count/Create transport, so a current server capture cannot detect or reconstruct that original local observation. Missing/off-screen/unobserved dirty identities must remain unqualified until an actual current own-row witness matches their originally observed basis; null or missing original basis cannot be replaced with today's basis. Restore validation also must not trust an empty saved unresolved list as proof of qualification.

Root's conservative reset plus matched-only observation closes the silent admission, but the first implementation runs the reset on every `createChoiceStore` call. Ready constructs an unused same-key handle on every page continuation. Untick A on page one, then read page two: that reset makes A unresolved despite the retained same-coherent page-one witness. Restrict invalidation to the actual renewed reader/coherent refresh lifetime, preserving valid continuation witnesses. Recheck resolution after an awaited count, since observing a changed basis can update unresolved state without changing the local edit revision. These are ordinary store/read lifetime corrections; no producer, version writer, certificate or protocol is needed.

### W22-J09 — Memory-aware ALL Work component exclusion is ignored by export

Current Create `2306:373–377` and Count `2340:145–148` accept an exact Work parent of either mode in the memory-aware descriptor. Native choices `2306:128–130` give component selected/false precedence for ALL Work; the accepted native implicit SUBSET represents the result. Worker `exportIncluded` initially consults component selection only for SUBSET mode. A valid memory-aware ALL/true Work with a false component therefore exports that component as included although Count/Create exclude it. Mirror the installed finite choice precedence and only qualify a payable Candidate from actually selected payable units. Preserve Candidate/Work false dominance and old descriptor semantics; do not alter a financial owner to make export agree.

## Second correction reread

At FE workbench `f423c879f55dfe4f44046b30872a7776219e73f3f00142d0535061a4ca752b46` / Worker workbench `1ba6c879476cb265bfaa2e36681e3eec2b0ec3bb91d761336cd3eaa76dac39be`, J06 and J07 are closed: HEADER memory/mode and native basis/count relations are checked in both mirrors; actual-baseline selection uses a nonnull scalar, while detached null obeys fresh raw mode. J05 has its bounded server `candidate_default_pay_selected` fact and same-default touched walk, but the new J09 effective component precedence still needs correction before export parity can pass. J08 continuation/reset/count-after-await remains under root correction.

### Remembered NONE mode correction

The current source now retains `remembered_mode` and Discard restores `empty(state.remembered_mode)` (`:937`, `:956`, `:966–967`). This closes the specific queued defect where Discard incorrectly replaced an acknowledged NONE mode with ALL_MATCHING. Its interaction with later baseline refresh remains in the ongoing review.

## Source checkpoint

| File | SHA-256 |
| --- | --- |
| FE `js/bpay-next-workbench.js` | `d0280898822f05f542bb1d8a4562018fb0705c8350d4cca7c81b4df5c9100cf8` |
| FE `js/bpay-next-office.js` | `3696ec5c1c0dd7e2864a21d53cbd689b2056cf64bd420b2701b9d27845d9e9a5` |
| BE `broker/src/bpay-next-workbench.js` | `f82f0a8f0557d17180448886cbf6633bc7b70691b9d19176923579a404599795` |
| BE `broker/src/bpay-next-preparation.js` | `6ce3f44c9b5f71b5eed14fb079422be6a95e7ef2105544d0e862f8f3193a9edc` |
| `design/WB-22-selection-memory.md` | `42d250250f540508c8c27e3d2e265dd1609781e0fbb9f722b934b9b70dbeea77` |

These are checkpoint hashes, not an approval of subsequent concurrent edits. The immutable WB-23 pending request bytes and parked F-25 owners remain required boundaries.

## Final correction reread and source verdict

**SOURCE PASS; no remaining concrete blocker in this reviewed slice.**

- J01–J04 remain closed: only Ready uses its store adoption; child page/HEADER/return-check reads bind the original parent memory reference and local generation; a new memory generation invalidates retained Ready blocks.
- J05–J07 are closed. Both strict mirrors bind HEADER memory/mode and native basis/count relationships. Actual nonnull acknowledged baseline uses server defaults, while detached null and old omitted descriptors preserve fresh raw ALL_MATCHING/NONE behavior. The finite `candidate_default_pay_selected` export boolean is sourced from the same native choices helper; it avoids accumulating every agency Candidate in Worker memory. Touched qualification retains actual selection and false-parent dominance.
- J08 is closed. Restored/renewed dirty readers make every retained raw identity unresolved before qualification; an unchanged actual originally observed basis clears that identity, while changed/missing/null basis stays unresolved. Null observation now deletes its stale `seen` entry, so a retick cannot reuse an older witness. Same coherent continuations retain their witness lifetime. Count checks reconciliation again after await, independently of the edit revision. Raw hidden four-kind intent and original observations are retained rather than assigned today's basis.
- J09 is closed. Memory-aware ALL Work export honors component false/true; touched payable qualification uses the effective inclusion decision. Positive SUBSET validation is restricted to actual SUBSET parents, so permitted ALL/true component memory is not incorrectly invalidated. The original non-memory descriptor branch remains.
- Discard restores its own retained baseline and `remembered_mode`; fresh Select all/Clear all detach the baseline and clear all four overlays. An inherited baseline is presented as `Saved selection` with bounded local changes, and its global tick is indeterminate rather than falsely declaring all/none actual members selected. No new count system or per-click RPC is introduced.
- Worker/Office CREATE validators admit the optional nullable scalar without adding it to an old descriptor. The journal still retains exact original CREATE/pending/last bytes and original run/command identity. Native SEALED implicit subsets use their retained actual page/count inputs instead of reconstructing descriptor component IDs. Only COMPLETE of a memory-aware descriptor acknowledges the retained run at the same local edit revision; EMPTY and pre-WB22 accepted bodies retain local choices and invent no server baseline.

The review used saved source, source diffs and finite field/body inspection only. Compile, ordinary checks, screenshots and any eventual application/financial acceptance are separate root-owned evidence; none was run by this reviewer. No parked F-25 producers, accepted-input protocol, financial calculation owner, public route or browser money authority was added by these JS changes.

| Final reviewed file | SHA-256 |
| --- | --- |
| FE `js/bpay-next-workbench.js` | `92d36f543e96a28da723acd8e191b5483419373a07760c321f6e268dc7488a69` |
| FE `js/bpay-next-office.js` | `c9927ddf2b085e227c543ed284da1affd3e5443e9f202510cc34189720373433` |
| BE `broker/src/bpay-next-workbench.js` | `97a56640d2e07b50412390ebd18987e69d168383a6ab14becb5cc6e462b13ace` |
| BE `broker/src/bpay-next-preparation.js` | `6ce3f44c9b5f71b5eed14fb079422be6a95e7ef2105544d0e862f8f3193a9edc` |
| Supporting `2340` count/reader source inspected for actual choice/export semantics | `545ab73faec8f1331f33986b91ee743340f96be7bcecfb36b8a9c4ed12b38b14` |
| Design | `42d250250f540508c8c27e3d2e265dd1609781e0fbb9f722b934b9b70dbeea77` |
