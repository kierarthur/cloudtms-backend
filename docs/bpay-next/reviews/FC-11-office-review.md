# FC-11 independent Office source review

12 October 2026. **Final SOURCE PASS; O11-01–03 closed.** Read-only review against the complete FC-11 card03:1238–1266, checklist D01–D15, current BD146 and native CASE_COMPONENTS/WRITE_OFF history contracts. Root alone edits product. No product, checklist, database, business RPC, tests, fixtures, hosted or rendering actions were performed. A4 and joined application acceptance remain separate.

## Findings and closure

**O11-02 — closed:** current `js/bpay-next-write-off.js:26` retains the initially blank required Reason/Amount paragraph with `data-next-live-copy`. It therefore survives native `js/modal-modernisation.js:1292–1294` and remains available for the later inline validation.

**O11-03 — closed:** the parent `onCaseChanged` callback now has its own nested catch after exact WRITTEN_OFF record/result validation. A rejected parent refresh no longer reaches the history-uncertainty catch; terminal amounts/date and successful result remain authoritative.

**O11-01 — closed:** the first journal save previously left an in-memory request available for dispatch after storage failure. Current `js/bpay-next-write-off.js:93–101` requires `persisted`, clears only the new pre-dispatch in-memory journal on failure, and saves uncertainty before every mutation; a failed subsequent save returns before network. No irreversible write-off is dispatched without its retained bytes.

The adjoining definite-refusal handling now matches the original Office contract. A first definite SERVER_REFUSED/AUTH_REQUIRED with `outcome_unknown=false` removes only the never-uncertain request. Previously uncertain/accepted or restored requests retain the original UUID, token and exact serialized body. Restore conservatively treats an unaccepted old journal as uncertain; retry does not obtain a fresh token or command.

## Native authority and caller checks

The component read requires a complete bounded CASE_COMPONENTS page, exactly one RECOVERY/DEDUCTION LOAN/ADVANCE/MANUAL_DEBT/OVERPAYMENT target and exact Case/Candidate identity. Existing Office decoding binds nested pause/edit state to those same identities and enforces component ordering/cursors. The window reuses native outstanding and explicit RECOVERY-purpose unreserved capacity, preserving NULL, with only input/receipt penny comparisons in the browser.

Native history0653:115,366–370 emits WRITE_OFF facts as STRING_OR_NULL. `input_amount` is the original text column (1647 migration:22), so its equality with the retained entered amount is exact and does not compare against a rescaled numeric value. COMMAND membership remains Candidate-qualified. The window validates original command, Case, component, actor, revision, scope, input amount and reason, then terminal event/issue/amount relationships before displaying the saved `completed_at_utc`. It never uses today's date or an invented Work collection.

FC02 CaseList More opens the same module through the existing parent/actor/owner transport and refresh callback. The genuine Banking write-off caller adds a NEXT/admin owner and native CASE_LIST namespace point before entering this module; LEGACY keeps the old route. The original shared EX01 preview/START/proof helper is reused. No new command, reader, money formula, producer or automatic reprepare is introduced.

## Source checkpoint

Checkpoint hashes (source was being finalized during review):

| File | SHA-256 |
|---|---|
| FE `js/bpay-next-write-off.js` | `a6f7304f0d8875c7eb6154bb54ff886a1f3837f852ae2d3ed2d4d00a82ff49d8` |
| FE `js/bpay-next-office.js` | `12372158e0c1fcfbd2e3d592f3328a5424cbd97203406fb21d8eaab0b8ab9cce` |
| FE `js/main.js` | `608bba49ef1153c5a49144cea40cf75d591109113a1cae39b50effa36f06992d` |
| FE `js/bpay-next-case-list.js` | `8c7f1253d60dc5503654fc3da87399a87f04a1d3a679544aa3f724d88ca286fc` |
| FE `css/bpay-next-office.css` | `2858be7dee9acab4f8d87700b1cdb67b5838a32b80337d216bb43e114cc6e210` |
| FE `index.html` | `4453be4e48facdc6be8bcdfc62b34633e91f773e7e63724be614a4462ec8231f` |
| BE `design/FC-11.md` | `7b7f2e9dc53a5b13b88e6fbb2f066e80fd41d102e66eaf6d90a3b81e91d14012` |

This receipt does not mark the whole item built or prove its business journeys. Final source closure follows the remaining saved UI/caller checks; screenshots are reviewed separately.

## Current source amendment

O11-01 and O11-02 are closed in module SHA-256 `28ccd80b10d957e53f761fcda71aa162e889656020acc06f4080164f35d02fa0`; current scoped CSS is `1154948f3d27e36df9b913f269673bb2d98cabd9475837538657729dcd406faf`. Office/main/CaseList/index hashes above remain current. The final native header subject, destructive marker/left alignment, hidden accepted footer, success/alert roles and NULL-capacity Refresh are source-consistent; their pixels remain separately reviewed. O11-03 remains the sole identified source closure condition.

## Final override — SOURCE PASS

Fresh complete module read confirms O11-03's isolated callback catch; native history date is displayed DD/MM/YYYY from the validated saved timestamp. Nullable channel no longer defaults to Umbrella. Pending/uncertain footer retains its one recovery action and native header close; accepted success has no duplicate footer close. Existing actor/agency/owner-epoch journal namespace, exact body/UUID/token retry and native readback bindings remain intact. No identified source defect remains within this review's scope.

Final FE `js/bpay-next-write-off.js` SHA-256 `f78cc0b68605416a8dfc470cd8d9009cde0be9f5cc2720cd1ba81a33f8fbe7f5`; CSS `41422c7218676e9ce6a7bfa1db665a7f5a4687809f9b115f3d46711f81457dbc`. Office/main/CaseList/index hashes in the checkpoint table remain current. This final override supersedes the interim OPEN/hash paragraphs above. SOURCE PASS does not assert A4, hosted installation or joined business acceptance.

## Final display amendment — SOURCE PASS retained

Fresh read of `js/bpay-next-write-off.js:40–67` confirms the zero-balance state hides the form and authentication copy within it, suppresses the warning, states the genuine native zero balance and offers Refresh. The existing journal branch precedes zero-balance footer handling, so pending or uncertain requests retain their original Refresh/Try again recovery action. Their saved scope/amount and Reason are displayed read-only from the retained request bytes; no new input or authorization is created. Loading uses only a positively known component/seed kind, otherwise Case. INPUT_PENDING states that the Draft balance is unavailable; a NULL channel is labelled Draft without inventing Umbrella. CaseList now passes its native kind/reference into the seed. The scoped hidden rule preserves these states under native modal styling.

These are presentation and seed changes. Original acceptance, terminal history qualification, exact command/body/token retry, actor/owner fences and storage-before-dispatch conditions remain intact. No additional source defect was identified; A4 and joined business checks remain separate.

Current final SHA-256 values supersede earlier checkpoints:

| FE file | SHA-256 |
|---|---|
| `js/bpay-next-write-off.js` | `38fbbea83062503849f0d86f2791627deaa6af41a842f48c8bc4c0cccd04af30` |
| `js/bpay-next-office.js` | `12372158e0c1fcfbd2e3d592f3328a5424cbd97203406fb21d8eaab0b8ab9cce` |
| `js/main.js` | `608bba49ef1153c5a49144cea40cf75d591109113a1cae39b50effa36f06992d` |
| `js/bpay-next-case-list.js` | `3af4293c8cfb8d24ccbda09115e74375e172c5baa4a182d28f49b5845306726f` |
| `css/bpay-next-office.css` | `b1a24e20d0c51c217a80d0dba411fd562d58381e13efd95c173a8dd9a8e11dd0` |
| `index.html` | `4453be4e48facdc6be8bcdfc62b34633e91f773e7e63724be614a4462ec8231f` |
