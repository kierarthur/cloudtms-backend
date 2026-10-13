# SP-12 Office source review

**Current verdict: source PASS for the expanded D07/D08 Office batch; H01–H04 closed after fresh readback.** The hashes below identify the current reviewed source and supersede the earlier source receipt. This is independent saved-source review on 10 October 2026. In-app contention, eventual completion, starvation, Candidate app/SP-36, and joined Worker/database acceptance remain **NOT_RUN**. This review does not certify D01–D06 writer/admission implementation or installation.

Read the full SP-12 card (`05_SOURCE_AND_PROTECTED_PAY.md:420–453`), all current SP-12 checklist objects, `SP-12-caller-census.md`, `SP-12-office-retention-seams.md`, the complete current seven JavaScript file diffs, their actual controlling handlers/renderers, and the eight indexed cache identities. Root alone made product changes. No test, fixture, business action, SQL, runtime, browser, hosted call or product edit was performed by this reviewer.

## Required caller behavior

- The finite UI predicates require actual HTTP 409, the exact `WEEKLY_SOURCE_PAY_QUERY_ADMISSION_BUSY` code and all three true typed transport facts: `retryable:true`, `failed_rpc_outcome:'ROLLED_BACK'`, `automatic_retry:false`. The workspace transport preserves only those known primitive facts and replaces its message with “Another Source update is in progress. Try again in a moment.” Message text, arbitrary 409/55000, and retryable alone do not establish rollback.
- Single outreach, single Finalise and rate acceptance retain their first completed native command/payload on this exact busy condition. Later deliberate retries use that same copy instead of rebuilding live selections, publication/version proofs, warning keys or acknowledgements. Their real builders have no browser-generated command UUID to replace or fabricate. Pending state survives repaint; actor/open-generation/frame and original scope checks reject replacement-context dispatch. Locked controls prevent changing a retained intent. `pendingSourceFinalise` is separate from the pre-existing finalised-pay recovery task.
- Combined outreach retains the original request array and failed `nextIndex`. Each confirmed owner's keys are removed once; a busy catch stops that loop. Explicit retry resumes the same owner and payload, skips confirmed owners and does not switch ASK/SEND action. A later unknown clears the busy marker while retaining the already-held request. No timer or automatic request reissue was added.
- Single upload preview retains the actual first acceptance body, locks coverage/import-use/confirmation edits, and bypasses rebuilding on retry. Batch upload keeps separate original bodies per affected entry and skips already ACCEPTED entries. The new submission paths check their actual initiating actor and exact child frame before and after awaits. Successful acceptance follows the existing scope/close/refresh paths rather than uploading or parsing again.
- Office Checks use the actual shared workspace action controllers. Existing command, manual-review and match modals already hold their original payload/command UUID; the changes use that existing retention and safe busy copy rather than introducing a second receipt or protocol. Protected review retains its original key/body and decision mode. The protected editor additionally retains the exact busy `PREPARE_PROTECTED_EDITOR` selection while `pendingPreparation` locks its fields; confirmed preparation then proceeds to the original protected Save. It makes one original Save POST. Current SP-06 actor/frame guards, closed actual-result confirmation and known refusal handling remain in place.
- The real main approved-hours modal uses the same finite predicate. Proven busy says Try again, while a later unknown says Retry exact request and uses its existing literal command/key. There is no new UUID, review/drain owner, fallback owner or inferred financial receipt.
- Existing parent modal permission handling explicitly leaves the workspace and `.ws-child[data-wsa-screen]` controls under their own gates; the newly disabled acceptance/selection controls therefore survive the real shell's edit-state processing. Protected controls retain their existing intentional-lock attributes. The index updates all seven affected JavaScript cache identities together.

## Findings resolved in current source

**H01 — combined Finalise busy classification (closed).** The initial catch always used CHECK_REQUIRED even though a busy helper had been added. Fresh `finalise-batch.js::run` now sets `sourceAdmissionBusy` from the exact predicate, uses READY and fixed busy copy only for that condition, and otherwise keeps CHECK_REQUIRED. Every deliberate attempt resets the marker. The original frozen `item.request`, actual success checks, terminal-item skipping and 401/403 stop remain unchanged. `combined-finalise.js` presents Try again for the busy item, preserves “Check saved results” precedence for a mixed unknown batch, and does not relabel unknown results as rollback.

**H02 — upload busy retry followed by unknown (closed).** The initial batch render called that later result “Not accepted”. Fresh `renderBatchPreview` instead displays “Result not confirmed” whenever a FAILED entry retains its original acceptance payload but is no longer proved busy. The batch button gives “Retry exact request” precedence over another entry's Try again marker. This preserves the original body without claiming that the later unknown or the entire workflow rolled back. Confirmed entries retain their accepted progress.

A failed RPC's rollback is never used as proof that preceding Source/upload/finalisation requests had no effect. Database writer closure, Candidate transport compatibility, STOP-CHECK and later real application acceptance have their own required evidence.

## Fresh expanded Office review

Re-read all seven current JavaScript diffs, not just the earlier source receipt. The new actual CorrectFinal, contract-chooser, command/manual/match, linking and in-flight caller changes are within the named Source UI scope; no new database owner, receipt framework, automatic submit or replacement key is introduced.

- CorrectFinal stores `pendingReview` before its PREVIEW request, including the original uploaded R2 file reference, replacement context, base native authority and original idempotency key. A later retry skips upload and uses that same body. A blocked-review recheck increments its existing suffix once for the new reviewed attempt, then holds that exact context/key across retries. `pendingApply` retains the original apply context, reason, confirmation text and `:confirm` key. File/reason/confirmation edits are locked when their corresponding request is held; unknown replies no longer claim the final source was unchanged. Initiating actor and exact child frame guard post-await presentation/close.
- Contract chooser retains the complete actual RECHECK_SOURCE or ACCEPT_UPLOAD request, including the original selected contract and existing selection map. Busy/retained choices cannot be edited; unknown-after-busy retains the same native body. Actual successful finish is guarded against actor/frame replacement.
- `openCommand` deep-copies its original payload once, keeps its existing manual-resolution UUID, checks actor/frame and `state.busy` before dispatch, and uses the same body on deliberate retry. Manual review and protected match retain their original command/key/body, now guard actor/frame and distinguish busy from unknown labels. None sends automatically.
- Single outreach/Finalise/rate put their request in the existing pending field and set `inFlight` before awaiting the POST. Repaint's locks and the handler's retained-inFlight check prevent another same-action submission. Combined outreach uses its existing state.busy; combined Finalise now carries a current-context callback through each original item so no later item starts after the original actor/frame/scope ceases to be current.
- Linking uses the actual existing Candidate/client picker kinds. After the original picker callback succeeds, the actual modal save path closes that picker synchronously before the zero-delay screen callback runs. The callback then requires the original parent/workspace/actor and opens the existing RECHECK_SOURCE confirmation modal; its timer sends no business request. A separate deliberate confirmation click sends the retained selected choice.

**H03 — first Finalise/rate dispatch current-context check (closed).** Fresh readback of both actual handlers confirms `if (!request.current()) return;` immediately before inFlight/pending assignment and before the POST. This applies to first submissions as well as retained retries. The existing original actor/open-generation/frame/scope predicate rejects stale dispatch; the post-await checks remain. The finding came from an earlier read during concurrent editing; this saved-source readback is controlling.

**H04 — picker refusal lifetime ordering (closed).** Fresh readback confirms the initiating actor/workspace/original parent/actual picker guard precedes the inactive-candidate refusal. The callback is async so that refusal remains an ordinary rejected callback handled by the existing awaiting picker. Its later screen-only timer still checks the original parent/workspace/actor, and business dispatch remains a separate deliberate guarded confirmation click.

All eight fresh hashes were read with these corrected seams. No actionable source finding remains within the expanded reviewed Office scope. No tests, runtime or product edits were performed for this closure.

## Fresh saved-source hashes

Paths below are relative to BUILD-FE (`.codex-worktrees/bpay-next-build-frontend-20261009`).

| File | SHA-256 |
|---|---|
| js/weekly-source/import-workspace.js | 7d4621851bf5c5e5b14c0dbd2f149291a16f34ae050778a952cfc203855d9bb1 |
| js/weekly-source/workspace-actions.js | 66345020ef21fdb2b71db996b8d9fbff100f1bbc9d4694dc2659b60cab5b416a |
| js/weekly-source/protected-shift-editor.js | b19c1f6063a1ef90f3b6b51a0e49dd9a9d5cbfa23fa02addd5f9e3ec87cbb26e |
| js/weekly-source/finalise-batch.js | 278da964135dbe5f137f1dfacca277cdc0be27b88be53d59b5349ea25df1ef81 |
| js/weekly-source/combined-finalise.js | 255c287d19cac8a2a267c56d5fab285c31b738b82dce613c4d72c51e0fe977a7 |
| js/weekly-source-presentation-v1.js | 1c3fb0aee410480e605e1e0e0dba151818b6eb41db69166bcf1f7565e0f44bf4 |
| js/main.js | b920c598768eb6c68cffc198b25cc31d80775d534759fafd96f3168c50f2d057 |
| index.html | 4c781a545dbf117b582e12c401d349ca85128d8f980f63883691f114f0e5259b |
