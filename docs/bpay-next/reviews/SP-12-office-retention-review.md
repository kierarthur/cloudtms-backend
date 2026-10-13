# SP-12 Office request retention review

Read-only independent source review, 10 October 2026. Product changes belong to the root builder. No tests, SQL, runtime, browser, hosted access, fixtures or business operations were executed. Verdict at this checkpoint: **changes required** for the concrete existing handlers below. Future contention and application acceptance remain NOT_RUN.

## Open findings

### OR01 — Combined finalise journal lacks its original caller boundary

`js/weekly-source/import-workspace.js:1316–1319` passes raw `issueCommand` to the serial journal runner. Neither this call nor `finalise-batch.js:67–99` captures the original actor, open generation or modal frame before each POST. `open():1821–1831` clears the new single-action pending fields but retains `session.combined.batch`.

Consequently a completed/busy/unknown journal can survive a new workspace open and be explicitly run under its replacement actor/context. Closing or replacing the original modal while an earlier item awaits a response also does not stop the runner from posting subsequent items. The immutable request body does not establish the same authenticated caller. Retain the existing journal and confirmed-item skip semantics, but bind its dispatcher to the original actor/open/frame and check that boundary before every item and before updating replacement UI. A replacement open must not acquire the old journal's retry authority. Do not silently discard unresolved outcomes or recreate requests from current rows.

### OR02 — Header recheck retains an unqualified request across a new open

`import-workspace.js:1579` prefers `session.pendingRecheck` over the current workspace payload. The request is kept on any failure, but `open():1821–1831` does not clear or qualify it. The handler at `1577–1587` has no original actor/open/frame predicate before its POST or its completion/load.

A recheck failure followed by a new actor/scope workspace open therefore leaves the old payload eligible for the replacement header's click. Bind the retained request to its original current boundary and prevent old callbacks from clearing or refreshing a replacement open. Preserve its existing exact request ID/body and unknown-outcome handling.

### OR03 — Contract chooser busy retry can change the original body

`workspace-actions.js:708–719` reconstructs either `RECHECK_SOURCE` with the current `state.selected` contract or upload acceptance with a newly built `contract_selections` object on each click. Its catch only sets `failed`; the choice inputs are guarded by `state.busy`, which becomes false after the refusal. A busy response followed by selecting a different contract sends a different payload under the original request/file identity while the button says Try again.

Store the first completed action/body on the exact busy refusal, use it for deliberate retry, lock the selection while that intent is retained, and apply the existing actor/frame guard. A later unknown response must use unknown wording and retain the same original body rather than claim rollback.

### OR04 — Other existing recheck/correction handlers still rebuild busy inputs

The Candidate/Client picker callback at `workspace-actions.js:1314–1327` recreates the original recheck payload with the latest selected record ID on each callback. After an exact busy failure the picker remains eligible for a different choice under the same request ID; there is no retained first body/choice lock.

The correction modal has three existing admitted Source calls. At `746–776`, retrying review re-uploads the bytes and rebuilds the replacement request; retrying its existing blocker recheck increments `:recheck:N` again. At `797–811`, retrying apply rebuilds its body from editable reason/confirmation under the same `:confirm` key. None stores the exact first completed request on busy or shows the required busy Try again state. Retain these finite existing commands and their actual keys/bodies, including the uploaded file reference, rather than introducing new command identities or a generic reissuer.

The apply catch at `811` also claims “The final source was not changed” for every error. That is accurate only for a definite rejected/rolled-back attempt; an unknown response cannot establish it. In particular, a busy first attempt followed by an unknown manual retry must stop using the original rollback claim. Preserve the retained original request and use unknown-result wording.

### OR05 — Manual/match/command completion can act on a replacement modal

`openProtectedMatch` (`932–943`), `openManualReview` (`1004–1013`) and `openCommand` (`901–908`) retain their native request/UUID but lack the captured actor/frame predicate used by `protectedCaller` (`446–449`). Their awaited success invokes unguarded `finishAction()`; that helper marks the current weekly-source child clean and closes it (`452–460`). Closing the original child while a POST is outstanding and opening another weekly-source child therefore allows an old result to close/refresh the replacement. A retained busy intent also has no actor predicate before retry. Apply the existing captured actor/frame predicate before each dispatch and after await; do not replace these native identities.

The generic command handler additionally checks only confirmation, not `state.busy`, at `902`, while its checkbox listener at `900` can re-enable the run button during a POST. This is a concrete second-dispatch path; the same local in-flight guard used by manual/protected handlers is sufficient.

### OR06 — A repaint can re-enable a retained single-action retry while it is in flight

The new single outreach (`1415–1433`), single Finalise (`1659–1673`) and rate (`1746–1763`) handlers disable only their clicked DOM button. They have no local in-flight predicate. When a retained intent is present, `lockSourceRequest` (`963–969`) unconditionally enables the matching retry during any repaint. Existing asynchronous page/load/repaint paths can therefore replace the disabled button while the manual retry is still pending; clicking the replacement sends the same command concurrently. Combined outreach already has a `state.busy` guard, and preview/protected handlers already have equivalent guards. Use the same finite in-flight protection on these three named handlers and keep the retry disabled until that exact attempt settles.

## Reviewed passing slices

- The exact busy predicate requires HTTP 409, the named admission code, `retryable:true`, `failed_rpc_outcome:'ROLLED_BACK'` and `automatic_retry:false`. `requestJson` preserves these fields only for that actual envelope; generic errors retain their existing semantics.
- Single outreach, single Finalise and rate acceptance keep deep JSON copies, their original command/action and a current predicate. Combined outreach retains the original request array and failed index; it advances/removes keys only after an actual `ok:true`, preserving already confirmed owners.
- Uploaded single/batch previews retain actual acceptance bodies without re-uploading bytes. They check captured actor/modal frame before dispatch and after response and skip confirmed accepted entries.
- Protected review/save retain their existing request identity; preparation now keeps its original payload independently. The source busy marker is recomputed on a later failure, so an unknown retry is not represented as proven rollback.
- `finalise-batch.js:88–92` now marks only exact busy as READY with the safe copy. An unknown result becomes CHECK_REQUIRED. The journal's mixed state label prioritises Check saved results over Try again. This closes the earlier unused-helper concern; OR01 remains a separate caller-lifetime defect.

## Fresh reviewed hashes

| BUILD-FE file | SHA-256 |
|---|---|
| `js/main.js` | `b920c598768eb6c68cffc198b25cc31d80775d534759fafd96f3168c50f2d057` |
| `js/weekly-source-presentation-v1.js` | `1c3fb0aee410480e605e1e0e0dba151818b6eb41db69166bcf1f7565e0f44bf4` |
| `js/weekly-source/combined-finalise.js` | `255c287d19cac8a2a267c56d5fab285c31b738b82dce613c4d72c51e0fe977a7` |
| `js/weekly-source/finalise-batch.js` | `bc10bffe8a0175046f2ea88094e7c14ca96a4e752bed0681e9327606dd2aac94` |
| `js/weekly-source/import-workspace.js` | `498483b331cb124a00134f4b9d271310ef1162c6a44c29e66f7fbe08a698a326` |
| `js/weekly-source/protected-shift-editor.js` | `b19c1f6063a1ef90f3b6b51a0e49dd9a9d5cbfa23fa02addd5f9e3ec87cbb26e` |
| `js/weekly-source/workspace-actions.js` | `52316d9186f36e4421485959f36c247a2e98677a252853b440123ebd19b0d4e2` |

The review covers the current named Source workspace, modal, upload, protected and finalise journal handlers and their shared dispatch. The open findings are finite existing seams; no producer/version/certificate protocol is proposed. This is not a complete SP-12 acceptance claim.

## First follow-up source reread

Fresh `import-workspace.js` SHA `66fafa984c72fb2b7397ae6e8670dbbaddf7a464a8d1e24ac57833fe0d7ce110`; `finalise-batch.js` SHA `278da964135dbe5f137f1dfacca277cdc0be27b88be53d59b5349ea25df1ef81`. The remaining product hashes above were unchanged at this reread.

- **OR01 closed for dispatch/UI boundaries.** Journal creation now binds `batch.current`; run checks it before starting and passes it into the runner, which checks before each item and immediately before its POST. Progress/load/catch UI updates are guarded. It preserves the actual immutable result/journal and original safe dismissal conditions, without granting a replacement open the old dispatcher. No runtime recovery/liveness assertion is made.
- **OR02 closed.** The header retains a deep copy and `pendingRecheckCurrent`, checks both original current predicates before POST/after await, distinguishes busy, and clears that local pending request on a fresh open. The original request ID is retained for an in-context retry.
- **OR03–OR06 remain open** in the fresh source. Root is implementing those finite existing-handler corrections; this review does not claim them closed before rereading the saved changes.

## Final product source reread

**PASS for the SP-12 Office source retention slice. No remaining concrete product defect found in the saved sources below.** This supersedes the earlier open source findings; it does not establish runtime contention, liveness, hosted publication or SP-36 completion.

| Fresh BUILD-FE file | SHA-256 |
|---|---|
| `js/weekly-source/workspace-actions.js` | `66345020ef21fdb2b71db996b8d9fbff100f1bbc9d4694dc2659b60cab5b416a` |
| `js/weekly-source/import-workspace.js` | `7d4621851bf5c5e5b14c0dbd2f149291a16f34ae050778a952cfc203855d9bb1` |
| `js/weekly-source/finalise-batch.js` | `278da964135dbe5f137f1dfacca277cdc0be27b88be53d59b5349ea25df1ef81` |
| `js/main.js` | `b920c598768eb6c68cffc198b25cc31d80775d534759fafd96f3168c50f2d057` |
| `js/weekly-source-presentation-v1.js` | `1c3fb0aee410480e605e1e0e0dba151818b6eb41db69166bcf1f7565e0f44bf4` |
| `js/weekly-source/combined-finalise.js` | `255c287d19cac8a2a267c56d5fab285c31b738b82dce613c4d72c51e0fe977a7` |
| `js/weekly-source/protected-shift-editor.js` | `b19c1f6063a1ef90f3b6b51a0e49dd9a9d5cbfa23fa02addd5f9e3ec87cbb26e` |
| `index.html` | `4c781a545dbf117b582e12c401d349ca85128d8f980f63883691f114f0e5259b` |

- **OR03 closed:** contract selection now retains the completed deep-copied command/body, locks choice/creation while pending, uses a real in-flight guard, and qualifies dispatch/completion with its original actor/frame. Known busy copy and subsequent unknown copy are distinct.
- **OR04 closed:** correction keeps `pendingReview`/`pendingApply`, retaining the uploaded reference and native recheck/confirm keys across retry. It neither uploads again nor increments the recheck key for a held request. Review/apply controls are locked; their error heading no longer asserts no change for an unknown apply outcome. Candidate/Client pickers now only validate the selection and create the literal recheck request. Their timer opens the existing confirmation screen after the actual picker closes; it makes no business call. That screen holds the completed deep copy and dispatches only on its separate deliberate confirmation click.
- **OR05 closed:** manual, match, generic command and correction/contract callers now reject actor/frame replacement before business calls and qualify awaited completion through `finishAction`. The generic command also rejects `state.busy`; checkbox changes cannot re-enable a second in-flight dispatch. Original manual/resolve command UUIDs are still created once.
- **OR06 closed:** each single outreach/Finalise/rate request is installed as the pending intent before its POST and carries `inFlight`. Click handlers reject that flag; repaint locks honour it. The flag is released only as its own attempt settles. Busy and later unknown results retain the original body; no automatic retry was introduced.
- **Additional partial-outcome finding closed:** the new confirmation owner initially lost the old `WEEKLY_SOURCE_RECHECK_INCOMPLETE` branch. Fresh `openCommand:948–957` now recognises that exact structured code, preserves the existing saved-selection/recheck-incomplete guidance and performs the original read refresh under its captured caller. It retains the exact original recheck body and does not classify this partial outcome as rollback busy.

### Actual workspace identity boundary

Several callbacks call `protectedCaller(...)()` afresh; consequently the helper's `workspaceApi() === workspace` comparison inside such a freshly constructed predicate does not by itself retain the original API object. That is redundant in the actual browser installation: `import-workspace.js:5–8` installs `CloudTMSWeeklySourceImportWorkspaceV1` as a non-configurable, non-writable property, and returns a frozen API. A legitimate browser workspace open changes private session state, not that API object. No real mutable API replacement path was found.

The meaningful current boundaries are the captured actor and exact modal frame. A new modal/open replaces the frame; the workspace-owned handlers additionally capture open generation, original selected scope and Source filters. Child handlers retain their own original native IDs/body, rather than rebuilding from the mutable workspace. A newer database Source revision is handled by that original request's existing stale/replay semantics. No new live-state authority check or protocol is needed merely to strengthen an unreachable API replacement comparison. A test must not treat replacing an otherwise immutable browser API property as an established product actor/scope transition.

### Minimal existing source-unit adaptations

Read-only test source SHA `1e1962cd9ebbf3925c1ab03c4ffb71c033d952821687c44876c596c2278a4739`. No test was executed or edited by this reviewer.

The existing `tests/unit/weekly-source-workspace-actions.test.cjs` cases retain useful contracts and should follow the actual control path:

1. **Linking case, lines 121–141:** provide the actual actor and parent/picker frames in its existing local mock; invoke the picker selection, remove only that picker frame, flush the controlled screen timer, then confirm/click the existing command screen. Assert no business POST during picker selection/timer, then the exact original row/request ID plus chosen Candidate on the one confirmed POST, and the existing refresh after success. Keep the source context/seed assertions.
2. **Inactive Candidate case, lines 143–163:** provide the same valid actor/picker context so the negative admission check is actually reached. The callback remains async; keep `assert.rejects`, no POST/no refresh, and the reactivation guidance. Do not reduce it to a pure wording check or remove the inactive negative.
3. **Saved recheck case, lines 165–176:** drive its existing confirmation child and deliberate click instead of expecting `handleAction` to POST directly. Keep exact payload equality, a single RPC/read refresh, and preservation of the original parent while the completed confirmation child closes. This is still a saved-request retry.
4. **Partial recheck case, lines 178–194:** follow picker → confirmation → deliberate click, then inject the existing structured incomplete error at the actual command seam. Keep exact original selected payload, one refresh, saved-selection/incomplete wording and exact-request retry availability; do not weaken this substantive partial-outcome assertion to generic unknown wording.
5. **Contract checkbox/creation case, lines 334–401:** add `SESSION.user.id` to its existing VM root and maintain its actual modal frame. Preserve every exclusive selection/untick/create, no-selection/no-dispatch and double-submit assertion. The new guard must be tested under a valid caller rather than bypassed or removed.

These are finite adaptations of existing cases and mocks. They require no new fixture files, test tools, protocol, business scenario or runtime programme. Seven changed Office assets have explicit cache suffixes in the existing index; load order and existing modules remain intact.

## Existing source-unit alignment review

**PASS — no substantive assertion weakening found.** Fresh `tests/unit/weekly-source-workspace-actions.test.cjs` SHA-256: `4c634c7b1f7053d1dd1b2ed9477714150b9883f02f9bfa660345b48b25aeb9d8`. Product `workspace-actions.js` remains the final reviewed `66345020ef21fdb2b71db996b8d9fbff100f1bbc9d4694dc2659b60cab5b416a`.

The diff changes the same five existing cases. The file still contains its existing 25 test cases. A finite local `sourceActionModal` mock drives the existing module in `node:vm` with actor, original parent, real picker/confirmation frame kinds, registered controls and a controlled screen-timer queue. It introduces no external test tool, fixture file, CLI, business data or new case. JSON copying in cross-VM equality assertions removes the realm prototype difference while preserving every original command/payload key and value.

- The linking case keeps original row/request/chosen Candidate and picker context assertions. It now additionally requires zero business calls during selection and screen scheduling, then exactly one command and read refresh after deliberate confirmation, with the original parent preserved.
- The inactive case still reaches the actual async negative branch and uses `assert.rejects`; the recorded calls remain empty, covering both no RPC and no refresh, and reactivation wording remains asserted.
- Saved recheck still asserts the exact original body, one command/read refresh and original parent preservation, through the actual confirmation seam.
- Partial recheck still requires the saved selection/incomplete wording, exact selected body and single refresh. It now explicitly requires Retry exact request and the unfinished confirmation remaining above its original parent. It is not downgraded to generic unknown handling.
- Contract choice receives only the missing actor input. All original exclusive selection/untick/create/no-selection, double-submit, body and disabled-control assertions are unchanged.

Root reports the normal source suite passed 129/129. This reviewer performed a source/diff/hash review only and did not execute that suite. The product source PASS and future NOT_RUN boundaries above remain unchanged.
