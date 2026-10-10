# SP-12 Office busy-only request retention seams

Read-only source design, 10 October 2026. This refines D07/D08 of SP-12 without changing product, tests, SQL, runtime or existing unknown-outcome behavior. The only new transient classification is the real 409 `WEEKLY_SOURCE_PAY_QUERY_ADMISSION_BUSY` envelope proving `failed_rpc_outcome:'ROLLED_BACK'`, `retryable:true`, `automatic_retry:false`. Copy: **“Another Source update is in progress. Try again in a moment.”** Retry is deliberate and uses the literal original request. A failed RPC rollback is not proof that earlier requests in the workflow rolled back.

## Shared finite mechanics

`import-workspace::requestJson` currently exposes HTTP status and `error.code`, but drops the three busy metadata fields. Preserve their actual primitive values for the exact typed error, then use one finite code/status/metadata predicate for these UI branches. Do not infer known rollback from message text, 409 alone or `retryable:true` alone. Existing unknown handling remains the fallback.

For a rebuilt caller, keep its local first completed request while awaiting its one POST. On the exact busy catch, retain that same request in its existing state; on manual Try again use it instead of rebuilding from live selections/workspace/controls. Preserve the original JSON inputs and insertion order; nested selections/options must not alias editable state. Storing the actual serialized body or an immutable copy of that same JSON input is sufficient. Do not add a UUID, digest, proof packet, command protocol or generic blind reissuer.

Clear this busy retention on confirmed success, or explicit cancellation of the still-proven-busy intent. A repaint is not cancellation. A context/actor/frame change must never dispatch the old retained request into the replacement context. Restore selection editing only when the original intent is explicitly dismissed or completed. Use the existing actor/open-generation/frame boundaries; do not retain one intent across logout/new workspace opens. If a deliberate busy retry subsequently has an unknown result, the earlier busy does **not** prove that new attempt rolled back: use the existing unknown wording/handling, retain the original input already held where needed for that recovery, and do not keep claiming operational busy or silently generate a different request.

## Existing identity: no missing browser nonce to invent

The current outreach payload, single Finalise payload, rate acceptance and upload acceptance contain **no generated browser command UUID/idempotency key** in their builders. Their existing native identity is in the source/publication/version/selection proofs and original input. Do not fabricate a nonce merely to make retention resemble a different command. By contrast, the existing manual-review modal creates its command UUID once; protected actions already retain their original key under SP-06. Reuse those current identities.

## Import workspace handlers

### Single query outreach — `bindQueries`, around 1371

`buildOutreachRequest(queries, selectionState, command)` around 619 copies the server `action.request`, then replaces its selection. The real request carries actor/source cycle/projection publication/expected workspace version/action plus mode, group/exclusion/incident IDs, filters, sort and selection proof. Each click currently rebuilds from `selectionSpec()`.

Minimal change: one pending busy request for this actual action in `session`, containing the exact command and completed payload. Build once for the attempted POST; assign it to that field only in the exact busy catch. The matching Try again button bypasses `buildOutreachRequest` and uses those original values. Clear it after success before the existing selection-clear/load. While it is held, disable changing the selected set and the other outreach action; a SEND_MANAGER_NOW click must not silently replay an ASK_CANDIDATES request. An explicit cancel can discard only this still-proven-busy intent. Do not re-fetch the workspace as a substitute for its retry proof.

### Combined outreach — `bindCombinedReview`, around 1121–1141

The existing per-tab `state = session.reviews[tab]` builds `{payload,keys}` entries per owner from the selected `combined_key` rows. It removes keys after each confirmed success. Currently the next click rebuilds the remaining request array, potentially changing proofs/selections.

Minimal change: retain `{command,requests,nextIndex}` on this existing per-tab state after exact busy. The local first request array is already available. Increment `nextIndex` and remove its original `keys` only after that owner's actual `ok:true`. A busy catch stores the failed index and stops the current loop; Try again starts at that same index with the original payload. Never rerun previously confirmed owners, rebuild the remainder from current rows, switch to the other outreach action or automatically retry the busy owner. Finish with the existing `loadCombinedReview` only after the retained array completes. Clear the retained array on complete or explicit safe cancellation, not on repaint/filter rendering. Keep a first non-busy/unknown failure on its original handling path; don't label it busy.

### Single Finalise — `bindFinalise`, around 1604

The actual first request is `{...session.workspace.finalise.finalise_payload, ...(exclusion_confirmation ? {exclude_unfinalised_acknowledged:true}:{})}`. Its native authority is source cycle/scope/upload/projection publication/expected authority version and three original manifest hashes. It has no browser nonce. Existing `session.finaliseRetry` is a **different** already-finalised approved-hours recovery task (`task_id/run_id/expected_task_version/confirm_retry`); do not reuse it for admission contention.

Minimal change: retain this completed first FINALISE_WEEK payload in a separate finite pending-busy field after exact busy. The manual retry bypasses current `canFinalise` reconstruction and reuses that retained request, subject to original actor/frame/scope still being current. Do not replace manifest/version fields from a newly loaded workspace or drop the original acknowledgement. Clear after genuine successful response and run the original load. Unknown or post-finalisation recovery remains the existing flow; no new source-financial state or full-refresh project is required.

### Rate acceptance — around 1683

`buildRateWarningAcceptancePayload` around 801 returns the real acceptance action plus server base payload, selected `warning_keys`, and the existing `selection_proof`. No UUID is generated. Today every click rebuilds current visible selections.

Minimal change: retain that actual `{action,payload}` on exact busy. Manual retry uses the original selected key list, original proof and original projection IDs even if the UI could otherwise reselect rows. Lock selection/confirmation for this retained intent; clear it only after success before the existing selection-clear/load, or deliberate safe cancellation. A definite stale proof follows the existing refusal/review path rather than silently refreshing and reusing a modified body.

## Workspace upload acceptance

### Single preview — `openPreview`, around 550–582

`buildUploadAcceptancePayload` around 244 returns file key/name/source group/cycle/client/report/profile, import-use, parser options and (for non-final reports) coverage dates/proof kind/shrink acknowledgement. No key is generated. The click currently calls `sync()` and rebuilds this body from editable coverage/intent.

Minimal change: retain the first built acceptance payload in this modal's existing `state` after exact busy. Try again bypasses `sync`-based rebuilding and posts that exact body to the existing `acceptUpload`. Disable coverage/import-use/shrink/confirmation edits while retained. Success follows the existing close/refresh. Do not upload bytes again, invoke the parser again from UI, re-create the file key or claim that the whole upload workflow had no effect.

### Batch preview — `openBatchPreview`, around 498–547

Each actual `entry.model` is retained, and confirmed entries already become ACCEPTED, are deselected and skipped later. Currently a failed entry rebuilds acceptance payload from that model on each batch click and is generically displayed FAILED.

Minimal change: each exact busy entry retains its own first built payload and a finite transient busy presentation marker. A later user run retries only the selected unfinished entry with that retained payload; confirmed entries remain skipped. Fixed copy and Try again distinguish busy from malformed input/unknown failure. Keep the original review models and keys; do not rebuild or change all entries, erase accepted progress, or declare the entire batch rolled back. No automatic same-entry reissue inside this run. Preserve the current non-busy/unknown classification and ambiguity behavior.

## Existing combined Finalise journal

`finalise-batch.js::create` already freezes each `item.request` from the original authority payload plus original exclusion acknowledgement. The nine allowed authority fields contain no generated nonce. `run` already uses the same `item.request`, skips `COMPLETE/SOURCE_COMPLETE_PAY_PENDING`, makes one call per item and retries unfinished items only on a later explicit run. **Do not add a second journal or request store.**

The remaining seam is presentation: the catch currently assigns all errors CHECK_REQUIRED; `combined-finalise.js:44` displays “Result needs checking” and “Check saved results”. For exact busy, retain a finite `item` marker (or existing error code) alongside its original request, use fixed busy copy, and show Try again. This is local UI presentation, not a new SQL Stage. Non-busy/unknown items keep their current CHECK_REQUIRED text/path. A mixed batch must not present an unknown item as proven rolled back simply because another item was busy.

Clear the marker when a deliberate new attempt begins or succeeds; a new unknown result must use the existing unknown branch. A later explicit run naturally reuses frozen unfinished requests and skips confirmed ones. Existing 401/403 stop behavior and original acceptance/result checks remain intact. If root elects to stop the batch on busy, keep the untouched later items READY; neither option may automatically retry the failed item or lose previously confirmed progress.

`import-workspace` combined-finalise `state.batch` persists through ordinary repaint/load and is dismissed only by the existing safe Done/Cancel conditions. Keep those conditions; do not let a busy render rebuild `create(...)`, change original selected scopes or erase the journal.

## Existing correct retention to reuse

`openCommand` holds command/payload once; `openManualReview` keeps its one generated command_id in `state.pending`; `openProtectedMatch` retains its original RECHECK_SOURCE inputs; `session.pendingRecheck` keeps its request_id; protected Save/review/main approved-hours hold their SP-06 literal requests. These need safe structured busy copy/Try again, not new request stores, replacement keys or recovery owner calls. Unknown stays distinct from busy, and no Source status is fabricated from an HTTP error.

## Read-only source receipts

| File | SHA-256 |
|---|---|
| FE js/weekly-source/import-workspace.js | bbaeea7cdc62d110b27b940483a6d565ffdc90e9a51cc2e032940cc09cc4061e |
| FE js/weekly-source/workspace-actions.js | 4c86b1b39870169db9e5cbbf3d9fe5f9d8795d8d68e13a86fbc296302bfb0b17 |
| FE js/weekly-source/finalise-batch.js | f099aba88dec4007347069b6021e9db73048579194bd4ff7b910d59bf8320231 |
| FE js/weekly-source/combined-finalise.js | da0fa4532a4a5c0282f8eee5ad8dae2b9fa2cb53739052304166e91a52ac11ba |

No tests, fixtures, code execution, business action, SQL, hosted call or product edit was performed. App and contention/liveness acceptance remain NOT_RUN.
