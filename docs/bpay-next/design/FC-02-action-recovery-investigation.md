# FC-02 Candidate Banking and retained bulk-snooze action seams

Source-only investigation, 12 October 2026. Product is being authored by the root agent. No product change, SQL invocation, business data, test, hosted request or screen generation was performed. All joined/reload acceptance remains **NOT_RUN**. Source anchors below are the saved files at this investigation, and may move as FC-02 is built.

## 1. Existing Candidate Banking API

The actual export is `CloudTMSBpayNextWorkbench.openCandidateWindow(options,address)`, FE `js/bpay-next-workbench.js:1383–1387`. Its address is:

```js
{
  candidate_id: row.candidate_id,
  candidate_name: row.candidate_display_name ?? null,
  candidate_reference: row.candidate_reference ?? null,
  context: qualifiedWorkbenchHeader.context,
  scope: {candidate_id, client_id, channel}
}
```

The address's Candidate is the clicked native Candidate UUID. The address's scope is the **choice-store business scope**, which may have candidate_id null for an existing broader Banking Pay scope. Those two values are intentionally independent: the existing Ready opener passes the clicked Candidate plus its unchanged parent scope at workbench:1063; the child narrows its actual reads to the clicked Candidate at :1413. Client/channel must be actual Banking Pay scope operands, not Case-only View, Case-type or Snooze-type filters.

The context must be the native Workbench closed CONTEXT: `kind,agency_id,actor_user_id,owner_epoch,official_pay_date,workbench_revision,adoption_revision,scope,selection_memory` (:421). SNOOZES and CASE_LIST contexts are different contracts and cannot be substituted or filled with guessed fields. Obtain this through the **existing** `createWorkbenchClient(options).loadWorkbenchPage('HEADER',{...scope,section:'READY',selection_memory})` (:646–679; native use :1099). The client already validates actor and the closed result. Keep parent/actor/NEXT-owner current checks around the asynchronous read and before opening.

Use the existing `createChoiceStore({context:header.context,window:win,storage:options.choiceStorage,selectionMode:header.rows[0].selection_mode,requalify:...})` (:972–1009; actual caller :1102). Its precise persistent namespace is:
```js
{
  agency_id: context.agency_id,
  owner_epoch: context.owner_epoch,
  actor_user_id: context.actor_user_id,
  official_pay_date: context.official_pay_date,
  scope: {
    candidate_id: context.scope.candidate_id,
    client_id: context.scope.client_id,
    channel: context.scope.channel
  }
}
```
Its key is `cloudtms.bpay-next.choices.v1.` plus JSON.stringify(namespace) (:975–976). There is **no environment field** in this choice-store namespace. Reuse the same existing memory/storage implementation and namespace; do not create a separate FC-02 selection store.

Important retained-selection step: if the restored store is dirty and its memoryReference differs from the fresh HEADER memory, reread the same existing HEADER with `selection_memory:store.memoryReference()` and retain that exact context/reference. The Ready owner already does the paired reread at :1103–1114. Do not adopt the newly returned remembered selection over dirty intent. The child validates store namespace against address at :1387 and validates its current page's exact selection_memory against the store at :1525–1527.

Required options are the actual existing authenticated Office/native host options, with `document/window,actorId,currentActorId,ownerReady,showReadWindow,authFetch,api`, plus `client,choiceStore,preparationTransport`. Existing parent wiring is Office :2134–2136. `host` is required only if document is absent (:1385), but carrying the actual native host is consistent with the existing adapter. Pass actual existing issue/open-Draft handlers if those child actions should remain available; a missing preparationTransport disables native component detail rather than creating another reader. Keep the native child frame and its parent's state/scroll.

The smallest integration therefore performs one qualified native HEADER join, restores/reuses the native choice store and opens this existing Candidate window. No new route, reader kind, command, selection protocol, financial operation or separate store is needed. If a broader Banking Pay store/context already exists, reuse it directly after its currentness check instead of choosing another namespace.

## 2. What existing MANY recovery already preserves

`js/bpay-next-snooze-window.js` owns the operation. Its journal is exactly version 2 with `namespace,pending,progress`, bounded to 32 KiB (:276–278, :369–371). The native namespace is `environment:'TEST',agency_id,owner_epoch:context.module_epoch,actor_user_id,scope_kind,scope_identity`; key prefix `cloudtms.bpay-next.snooze.v1.` (:368). For EXPENSES_ALL the server's scope_identity is the native Work UUID text: current context 0312 :103, :122–126 and MANY frozen aggregate lookup 0316 :113.

Pending is the **original serialized** `{action,readback:false,args}` body. Restore retains that string (:370). `execute` (:292–293) first sends the original action/args with readback:true; if unreceived it sends the original action/args with readback:false. It does not construct another request UUID or take another selection cut. Transport failure retains pending; a definite parsed non-unknown mutation refusal clears it (:288–290). The original page receipt, not current membership, is authoritative.

Progress is `{action,next,complete,summary}`; next retains request_id, Candidate/Work/revision/basis/control-cut/warning and page continuation. Existing `boundArgs(...,true)` checks Candidate and Work while deliberately permitting an older expected_revision_id for retained MANY (:279–286). Continue uses `execute(progress.action,progress.next,true)` (:319). Only a brand-new operation executes PRE_SAVE and constructs a new request/cut at :321–323.

Existing results retain at most three visible pages. Per-page receipt bodies and failed-line retry receipt bodies use exact request/page keys (:330–339). Previous results use those saved bodies with readback:true (:357–362). Failed-only Retry uses the same parent request, next page and exact retained line numbers (:356); it does not collect a fresh browser array of expenses. Original native MANY args/receipt validation is already closed and bounded (:141–178).

Server reuse is real: registered `private.bpay_next_snooze_many_v1(uuid,text,jsonb,boolean)`, `supabase/repeatable/11102026_0316_banking_pay_next_snooze_many_v1.sql`, validates actor/admin/NEXT ownership (:31–35), then matches immutable parent actor/action/module_epoch/request-body and exact page-body (:71–94). It returns an existing page before current Work qualification. Only an absent parent performs new current PRE_SAVE membership/basis/control-cut qualification (:96–114). An accepted parent continues its original frozen native lines. A changed revision can cause individual original child mutations to refuse, but cannot silently replace the frozen membership.

## 3. Real frontend recovery gap

Current `Snooze.open` does current PRE_OPEN before discovering its journal (:228, :368–371). An old expected_revision_id, withdrawn Work or now-inactive aggregate can therefore prevent restoration before the retained replay path is reached. The management path cannot repair MANY recovery: it expressly rejects `subject.management && subject.many` at :227, permits only SNOOZE_CLEAR in boundArgs at :280, and rejects any retained progress at :371.

`CloudTMSBpayNextSnoozeActions.create` exposes only openSnoozeFor, amendSnooze and unsnooze (`js/bpay-next-snooze-actions.js:83–107`). Its unsnooze adapter performs an exact existing SNOOZES point read and opens a **single-control** management clear (:103–106). There is no exported MANY recovery entry. Do not call this single adapter and describe it as Unsnooze all expenses.

Exact inactive-control readback is already supported by SNOOZES point lookup: actions :86–88 validates one native snooze_id result, and response validation :73 exempts exact-id results from active/history eligibility filtering. The native row has stable Candidate/Work/scope_identity, stored expected revision and current display revision separately. Row helper 0041 :28–35 selects current/applied/stored revision labels; :45–55 returns original control identity and active/can_amend/can_unsnooze. Inactive means no fresh single clear, but it need not erase the exact old MANY operation journal.

## 4. Small required seam in the existing owner

Extend the existing shared Snooze owner with a **retained-only MANY opening path** which derives one exact existing journal key from freshly qualified agency/actor/NEXT epoch plus native Candidate/Work and EXPENSES_ALL scope_identity. The caller should carry the actual fresh reader context/native identity; do not invent a current financial Snooze CONTEXT, basis, expense count, control-cut or membership to pass its validator.

Read and validate that one version-2 journal before any current PRE_OPEN. Restore only if its original pending/progress is SNOOZE_CLEAR_MANY (or the already-owned matching MANY action), belongs to the native Candidate/Work/namespace, and satisfies the existing strict MANY schemas. Recovery uses retained args for readback, next-page continuation and failed-line Retry, with existing current-frame/actor/owner checks. Its display-only retained identity must not authorize a new initial mutation. If no qualifying journal exists, fall through to the ordinary fresh PRE_OPEN flow; a stale current revision then remains a normal refusal requiring Refresh.

A changed actor/agency/module epoch must not reuse another namespace; server replay already compares module epoch exactly. A malformed journal must produce a refusal, not be deleted and converted to a new operation. A successful fully completed zero-failure operation already removes the active journal (:341). Completed failures retain the original results and failed-only Retry.

For reload navigation, use the existing FC-02 retained exact row/control identity or a native History/exact-id point read. If every relevant row is excluded by the current view and no original Work/control locator is retained, the current code exposes no recovery discovery API; this is a real reachability gap. Do not solve it with a scan of all local-storage journals or a new server store. Any necessary retained navigation locator should be one qualified pointer owned by this existing FC-02/shared Snooze flow, and merely locate the existing version-2 journal; it must not store/reconstruct membership or authorize a fresh cut.

## 5. Boundaries and verdict

Candidate Banking: existing native API complete; a small FC-02 header/store join is required. MANY engine and native replay: existing exact frozen operation is reusable. MANY reopening after changed current facts: **current frontend gap**, requiring the retained-before-PRE_OPEN seam above in its existing owner. This is source design guidance, not an implementation or acceptance PASS.

Current registry observations: Snooze context 0312; MANY/guard 0316; management row 0041; management page now 0108 while FC-02 server sorting is being authored. No historical-only file is proposed as a replacement current owner. All runtime, reload, expired/stale revision, unknown-response and joined-app checks remain **NOT_RUN**.

## 6. Native child openers and return values (follow-up source census)

**Timesheet:** main `js/main.js:7179–7218` defines the actual current `openNextIssueTimesheet(issue,review=false)`. It is injected into ordinary WB screens as `issueOwners.timesheet` and `issueOwners.reviewTimesheet` at :7226–7228. Ordinary opening requires matching `issue.context.actor_user_id`, current NEXT owner, an actual `physical_timesheet_id` and no accepted_run_id. Its native call is:
```js
openTimesheet({
  timesheet_id: issue.physical_timesheet_id,
  __bpayNextExactPhysicalId: issue.physical_timesheet_id,
  __bpayNextExactPhysicalVersion: issue.physical_timesheet_version ?? null,
  candidate_id: issue.candidate_id,
  reference_number: issue.timesheet_reference ?? null,
  source_root_timesheet_id: issue.source_root_timesheet_id ?? null,
  source_week_ending_date: issue.source_week_ending_date ?? null
})
```
The exact physical version is optional because native CANDIDATE_GROUPS supplies the physical ID but does not supply physical_timesheet_version (:359, :392). Do not infer it from a Work revision UUID. The special review=true path additionally requires the exact accepted run/worker/Work/Candidate/physical UUID tuple and one of the two Source-query review reasons; do not reuse that path for arbitrary accepted Waiting rows.

The handler requires an existing native bpay-next-read parent frame, invokes original openTimesheet, verifies a real timesheets child, removes only the managed parent footer while the child is active, and restores normal child chrome. It resolves **Promise<void> after the native Timesheet child is dismissed**, followed by actor/NEXT/parent-stack checks. It is not a returned viewer/controller.

The Candidate owner already chooses `options.issueOwners.timesheet({...row,context})` when no openGroup override is installed and the row is unaccepted (:1472). It awaits that handler, then rereads existing HEADER using the same retained choice-memory reference, marks newer information and preserves scroll if context changed (:1473–1481). Therefore pass this exact injected handler into Candidate Banking. Avoid installing a blanket openGroup that bypasses this existing Timesheet/currentness path. Expose the main helper to the FC-02 invocation through the same native closure/options; no new Timesheet route, source lookup or viewer is required.

**Waiting accepted run:** the Candidate owner emits only `{run_id,command_id,module_epoch,candidate_id,context}` at workbench :1472. The current ordinary Office Candidate injection at Office :2136 has no openDraft callback of its own. These address fields do not include run_worker_id, accepted channel or current run status. No hidden native callback should be assumed.

Existing exported native modules/returns are:
- `Office.openWorkerReview(options,runId)` (:1676–1687, :1812–1816) returns a native **frame** with active/alive/focus/close; it reads current PROGRESS and original WORKERS. It invokes existing REVIEW only for its supported unconfirmed preparation flow (:1730–1738).
- `Office.openBpayNextPreparationProgress(options,runId)` (:1835–1853, :1988–1990) returns the native frame plus `run_id,submissionRecovered(),submissionFailure()`. It requires `progressNamespace` containing native agency/actor/owner_epoch and optionally checks `progressCurrentNamespace()`. It uses the existing PROGRESS page; it does not receive accepted command identity as a separate parameter.
- `Office.openCaseProtectedDraft(options,protection,candidateId)` (:2310–2323) consumes an **actual protection tuple**. For DRAFT it requires native run_id/channel/run_worker_id, checks CHANNEL_DRAFTS/CHANNEL_STATUS Candidate identity, and uses `sheet.selectDraft` if already open. Otherwise it seeds the existing run-qualified controller and opens ordinary Office. For non-DRAFT it delegates to openWorkerReview. It returns that frame or existing Office **sheet**; sheet has focus/close/controller/selectDraft (:2244–2259).
- Native frame.close returns Promise<boolean>; neither frame nor the Candidate window export exposes a done promise (`js/bpay-next-ui.js:71–73`; Workbench :1540). The Timesheet helper's explicit dismissal promise is its existing specialised seam.

Minimal accepted-address qualification can use the already-owned `transport.page('ACCEPTED_CANDIDATES',{run_id,after_candidate_id:null,limit:1})` to compare its native header `run_id,prepare_command_id,accepted_module_epoch` to the emitted run/command/module tuple. Its header also supplies current run/command status (Office :200, :215; validator :829–836). This is the same native reader, not a new endpoint. A fresh PROGRESS read supplies current run state. Preserve the current actor/agency/NEXT/epoch scope rather than accepting an address from another qualified context.

For an unconfirmed run, reuse native preparation progress/review with the real namespace/current-namespace options. Do not set submissionComplete=true merely because a Waiting row contains an accepted run: it is a browser readback completion assertion and must come from the existing accepted-seal/readback evidence. For a confirmed DRAFT, obtain the actual worker/channel tuple from original accepted membership or original channel-worker rows before using openCaseProtectedDraft/selectDraft. ACCEPTED_CANDIDATES already carries native run_worker_id/target_pay_channel and is boundedly paged (:188, :215). Its initial header read alone cannot supply the clicked Candidate's worker when that Candidate is outside the returned page.

A direct openWorkerReview call is not an all-status Draft opener: current private review_simple_run 2259 :1664–1684 replays REVIEW and admits PREPARING/SEALED, but rejects DRAFT. Likewise progress treats DRAFT as terminal and does not expose confirmed payment actions (:1974–1983). Do not present either as a completed confirmed-Draft payment viewer. The genuine minimal adapter must select the existing native target according to these fresh native facts; no early payment owner or fabricated worker/channel may be added.

## 7. Minimal retained-only CLEAR_MANY UI context (follow-up)

Existing `boundArgs(action,args,true)` requires only matching context.candidate_id and context.work_id for MANY; it deliberately skips comparing the retained expected revision (:279–286). The exact original arguments already contain revision/basis/cut/date/note/warning; replay and continuation must consume those arguments, not new context fields.

For **retained SNOOZE_CLEAR_MANY with subject.clear=true and subject.many=true**, the same window's remaining field needs are:

| Existing operation | Actual retained-path context operands |
| --- | --- |
| Exact existing journal key :368 | agency_id, module_epoch, scope_kind='EXPENSES_ALL', scope_identity=actual Work UUID; actor is captured from authenticated options |
| Retained body validation :279–286 | candidate_id, work_id |
| Receipt/display/event :340, :366, :374 | candidate_id, work_id |
| Heading and form controls :250–263 | CLEAR/MANY heading needs no expense_count; form is hidden and retained original inputs are locked |
| Loaded notice :375 | conflict only controls the new-proposal notice; retained mode should bypass this current-conflict branch |
| Hidden date hint :373 | business_date is only presentation of the ordinary new form; omit/hide that assignment in retained clear mode |
| Readback/send :266–293 | authenticated actor/current native frame/owner guard and original normalized args; no current basis, revision or warning lookup |

Thus only six native identity/namespace fields plus authenticated actor are necessary to build the retained-mode UI binding: `agency_id,module_epoch,candidate_id,work_id,scope_kind,scope_identity`. Use them as a private display/recovery binding, **not** a fabricated object passed through validateBpayNextSnoozeContext. Do not fill missing current facts with zero expense_count, a guessed worker_pay_basis, false financial eligibility or a fake warning context.

Guard the retained-only mode so it cannot enter proposal(), PRE_SAVE check(), compare(), understands(), warning ACK, or fresh first-page UUID construction. Its allowed actions are original pending readback/replay (:317), original progress.next (:319), original saved-page readback and failed-only Retry (:356–362). If a definite refusal clears the unknown pending body and no progress survives, the recovery-only window must remain refused/closeable; it must not fall through on the next click to the fresh mutation branch with missing current context.

The shared window already has current-frame/actor/NEXT checks at :245, :267–269 and :355. The new retained namespace entry must additionally require the fresh qualified owner epoch matches the original namespace; server 0316 :73–75 rejects epoch mismatch. Fresh SNOOZES context has `actor_user_id,agency_id,owner_epoch,...` (snooze-actions :30–32), so map owner_epoch to the native journal's module_epoch slot, without substituting its reader scope object for EXPENSES_ALL identity.

This is the smallest existing-window adaptation: select and validate the exact old journal before current PRE_OPEN, branch only its initialisation/render assignments that demand a fresh proposal context, then use the existing engine unchanged. It does not require any new DB object, command, page kind, membership field or protocol.

## 8. Current retained CLEAR_MANY implementation source review

Reviewed saved root-authored source, source-only. Observed SHA-256:
- FE `js/bpay-next-snooze-window.js`: `64C072A05C97696D222710F7D84187AA17C0DDB23C891ECC3C7C6F406B728FE4`.
- FE `js/bpay-next-case-list.js`: `B229840E47EEDF21A68DCFA14E4E03B9190B9608872853F6E985E50D86307DF0`.
- FE `js/bpay-next-snooze-actions.js`: `2E66B0D1D959EBE4018EAB02283509ECB7E7E4B0C0F087522F7C0CB89F195AA4`.

The earlier PRE_OPEN recovery gap is now addressed in the actual shared owner, rather than another window or store. `subject.retainedMany` is a closed six-field native identity at snooze-window :228–229. Initialisation :371 uses it to find the exact version-2 original journal before any current PRE_OPEN; it does not construct a financial context. Both restored pending and progress must be SNOOZE_CLEAR_MANY (:373–374), use the original native args validation and bind the actual Candidate/Work. Absent pending/progress is refused (:376). The hidden fresh date hint is bypassed (:377), and the new-proposal conflict notice is bypassed (:379).

The ordinary existing pending-first engine is preserved (:294–295, :319–321). A retained unknown page first sends readback:true with its original args, then only the identical unreceived request is replayed. Progress, previous-page readback and failed-line retry still use the original request/cut and existing receipt keys. No fresh revision, browser membership array, warning ACK or UUID is constructed in this retained-only branch.

A definite parsed non-unknown mutation refusal clears pending (:291). When no progress remains, the Apply button is disabled (:257) and save() explicitly refuses before proposal/PRE_SAVE (:323). When an accepted progress record survives, only that original parent continuation remains possible. This addresses the future-fresh-proposal concern. Unknown/refused bytes are not reinterpreted as a successful receipt.

FC-02 discovery is finite and native: case-list :126–133 derives one exact existing namespace from each actual current/history Snooze row and qualified reader identity; it does not enumerate storage keys. MANY is offered only for expense-line/aggregate rows (:97). recoverMany :135–136 rereads the exact original snooze_id through existing SNOOZES, rechecks native Candidate/Work, supplies fresh agency/epoch plus original Work namespace, and awaits the existing shared owner's done promise. Its expected revision is the row's stored native expected revision; the retained request still supplies its own original revision/basis/control-cut.

Case CLEAR discovery :118–123 gets actual Case component IDs through existing CASE_COMPONENTS, checks their exact native journal keys, then the action :88 rereads the original snooze ID and verifies native Case/component before delegating to the original single-clear management owner. No Case control UUID or component identity is guessed. The main/current guards and native child frame remain authoritative; recovery itself adds no public route, command, DB object or money owner.

**Two finite validation findings remain at these observed bytes:**
1. Shared restore :372 JSON-parses the whole saved journal without enforcing the whole-journal UTF-8 byte ceiling used by store() :278. FC-02 discovery :128 enforces JavaScript string length rather than UTF-8 byte length, and Case discovery :121 does likewise. Check the existing 32768-byte whole-journal bound before parsing; reject while retaining malformed bytes. Pending-only 32768-byte and MANY-args 24576-byte checks do not establish the combined journal bound.
2. When pending and progress both exist, :373–374 validate each separately but do not verify they refer to one parent operation. Require the same CLEAR_MANY action/request_id and equal original immutable MANY operands, excluding the existing page_no/after_line_no/retry_line_nos continuation fields. Valid owner-generated unknown-next-page journals satisfy this equality. This closes a damaged/mixed journal's original-operation ambiguity without changing the native protocol or freezing another membership.

Verdict at these observed bytes: **retained-path source logic sound; OPEN for these two strict journal checks**. This is not a full FC-02 source/UI receipt. No business invocation, browser POST, reload/unknown-response acceptance or A4 capture was performed; all remain **NOT_RUN**.

## 9. Final recovery-source amendment: both findings closed

Re-read the saved files after the root's fixes; no execution was performed.

- `js/bpay-next-snooze-window.js` SHA-256 `3F9DE5EEF79B1205091A0EA1718263768DB58FB5C54810E1211A215F998C2010`.
- `js/bpay-next-case-list.js` SHA-256 `7BD5FCBEDC4AC6DE9AC232F6BD7FF59DA956C1B5AB75143906E9AEAF1DD9E93B`.
- Unchanged `js/bpay-next-snooze-actions.js` SHA-256 `2E66B0D1D959EBE4018EAB02283509ECB7E7E4B0C0F087522F7C0CB89F195AA4`.

Whole-journal UTF-8 bounds now precede JSON.parse in shared restoration :372, exact Case-component discovery :122 and actual Snooze-row discovery :129. Oversized/damaged original storage is retained and never becomes a fresh operation.

Shared restoration :375 and FC-02 discovery :131 now require matching action and the complete original immutable MANY operands. They exclude only `page_no,after_line_no,retry_line_nos`; request_id remains part of the comparison. The existing unknown-next-page and failed-only-retry continuations therefore remain valid, while a mixed-parent pending/progress entry refuses.

The previously reviewed retained-before-PRE_OPEN path, current native identity/namespace checks, original request readback/replay, failed-only receipt reuse and explicit refusal before fresh proposal remain intact. **SOURCE PASS for this narrow FC-02 retained action-recovery integration.** Sections 3/8 describe historical gaps and findings; this final amendment records their closure at the hashes above. This does not pass the whole FC-02 item or its screen review. All runtime, joined, reload/unknown-response, business SQL and browser POST acceptance remains **NOT_RUN**.




