# SP-12 caller and error-transport census

Read-only design, 10 October 2026. Full card `05_SOURCE_AND_PROTECTED_PAY.md:420–453` and checklist D05–D08/D10 read. This note covers only the named error transport and genuine Office callers. The Source/Banking writer census, entry placement and TRUNCATE qualification are separate D01–D04 work. Binding Rule B remains in force: no Candidate producer/materialise/compare rewrites, parked F25 protocol, new command or automatic retry. SP-36 owns the fresh complete MyTMS app consumer; application/installed/whole Worker acceptance is NOT_RUN.

## Exact server busy contract

`broker/src/bpay-busy.js` currently recognizes only structured PostgREST `error.json.code === '55P03'` plus one exact message (`BPAY_SOURCE_FAMILY_BUSY` or `BPAY_APPROVAL_EVIDENCE_BUSY`), with an actual integer HTTP 4xx/5xx status. Extend that same finite map with **only** `WEEKLY_SOURCE_PAY_QUERY_ADMISSION_BUSY` and copy **“Another Source update is in progress. Try again in a moment.”** Retain existing two-code behavior. Do not classify arbitrary 55000, raw string regexes, HTTP 409 alone, timeout or unknown transport as known rollback.

The Source busy response must retain clean 409, `ok:false`, this exact `error_code`, `failed_rpc_outcome:'ROLLED_BACK'`, `automatic_retry:false`, and `retryable:true`. Earlier requests in a workflow may have committed; `workflow_outcome:'NOT_ESTABLISHED'` remains truthful. A shared-map addition must not claim an entire workflow rollback. The existing protected-only adapter in `weekly-source/routes.js:979` already recognizes this exact SQLSTATE/message pair; the ordinary `routeErrorResponse` still relies on the shared map and otherwise uses generic conflict/raw transport text. Reuse one finite shared classification rather than widen the generic fallback.

## Candidate error boundary: actual missing public transport

1. `broker/src/candidate-app-backend.js::errorResponse` (`:836`) already returns the shared busy envelope as a clean 409 plus `retryable:true` and `request_id`; it currently omits `retry_class`. Add **`retry_class:'RETRY_SAME_REQUEST'` only when the new exact Source admission busy was proven by that recognizer**. Existing Banking busy codes, arbitrary SQL exceptions, unknown transport and Candidate stale-context errors do not inherit this class.
2. The actual Source request handlers are `handleCandidateWeeklySourceRequest` (`:6324`, APP draft/submit) and `handleCandidateWeeklySourceSelfSubmit` (`:6348`). These retain body/idempotency input and use the existing RPC boundary; no owner or request-shape change is needed for this transport work. Do not edit their business semantics or their DB producers.
3. **Supporting public-broker correction is genuinely required.** `candidate-broker/src/candidate-broker.js::publicSafePrivateResponse` (`:1151–1208`) rebuilds the generic private error with only `ok,error_code,request_id` and drops retryable/retry_class/rollback/copy/automatic_retry. The Source request/self-submit paths use this generic adapter, not the Daily-specific error schemas. Adding retry_class only to the private backend will therefore not reach MyTMS.
4. Add a narrow branch to that existing generic public error adapter for the exact expected private 409 Source busy envelope, including original `ok:false`, exact code, `retryable:true`, `retry_class:'RETRY_SAME_REQUEST'`, `failed_rpc_outcome:'ROLLED_BACK'`, and `automatic_retry:false`. Rebuild only its finite safe public fields/fixed copy; never forward an arbitrary private body, raw DB details or all retry classes. Malformed/unproven envelopes retain the existing fail-closed generic behavior. This is transport support for D06, not a new public route/command, protocol or producer.
5. Real private `candidate-private-api/wrangler.jsonc` points at `broker/src/candidate-private-worker.js`, which imports the current Candidate backend directly. The normal backend shares that implementation. `candidate-synthetic-private-api/src/index.js` currently implements only the APP_READY private proof route; it has no Source submission/errorResponse business path. Preserve its current proof/error contract; do not add a dormant duplicated Source handler. Future release must prove compatible real/synthetic private versions before the public broker (B2.17). No runtime/config/deployment evidence was obtained in this review.

## Office: safe copy and actual retained inputs

Office Checks has no independent JS controller. `import-workspace.js:690` renders its rows/actions; the shared `.ws-row-action` dispatch (`:1529`) forwards their server payload/context to `workspace-actions::handleAction`. Its actual manual/protected/recheck owners below must provide the retry behavior; a standalone unused helper would not satisfy the screen row.

| Actual caller | Current retention | Minimum scoped correction |
|---|---|---|
| `workspace-actions::openCommand` (`:846`) | One modal retains `command,payload`; RESOLVE_MANUAL_REVIEW creates its UUID once at open. Failed button already says Try again. | Map exact typed Source busy to fixed copy without treating it as failed evidence/REVIEW. Keep same payload and key on explicit retry. |
| `openManualReview` (`:951`) | `state.pending` holds original source row, reason and one command UUID; reason locks once pending. | Fixed code-driven busy copy and Try again label; preserve pending and original command. |
| `openProtectedMatch` (`:874`) | `state.pending ||= ...` retains the exact RECHECK_SOURCE request; selection/reason lock on pending. | Fixed busy copy/Try again; no new key or comparison request. |
| Protected shift/review saves | SP-06 now holds original action/payload/key and makes one POST per explicit click. Main approved-hours modal holds its original frozen command. | Distinguish proven busy copy + Try again from unknown “Retry exact request”. Keep existing unknown retention, typed refusal and actor/frame guards. |
| `import-workspace` RECHECK_SOURCE (`:1520`) | `session.pendingRecheck` already retains the original request_id/payload until success. | Code-driven copy + explicit retry; do not rebuild from a refreshed workspace. Do not discard its original scope as a side effect of busy repaint. |
| Query outreach bulk (`:1371`) | `buildOutreachRequest` uses the server action request but rebuilds current selection every click. | Retain the actual action and completed payload before POST; a busy retry bypasses current selection rebuilding. Lock only the original failed intent until explicit retry/cancel. |
| Combined review outreach (`:1121`) | Per-owner `requests` array is rebuilt from the current selected rows on every click; successful rows are removed. | Retain the actual remaining per-owner request array and current failed index; explicitly retry that same original payload, retaining successful earlier owners and never automatically resubmitting them. |
| Single FINALISE_WEEK (`:1604`) | Rebuilds payload/acknowledgement from current workspace each click. | Retain actual first payload/command before POST and reuse on deliberate busy retry. Do not overload `session.finaliseRetry`, which is an already-finalised pay-follow-up task. |
| Rate warning acceptance (`:1683`) | Builder recreates the current selected key list plus the server acceptance payload/proof. | If this request is a census participant, retain its actual completed first request before POST; explicit busy retry uses that request. No financial recalculation or new acceptance identity. |
| Upload acceptance: `openPreview` (`:575`) / `openBatchPreview` (`:531`) | Rebuilds upload acceptance from editable current coverage/intent (batch from entry model) per click. | For an actual busy participant, retain the literal acceptance payload per current preview/entry before POST; reuse on explicit retry and preserve accepted entries. Do not rerun upload/parsing or generate a new key. |
| Combined finalisation | `finalise-batch.js::create/run` already retains frozen `item.request`, skips completed items and retries only on a later explicit run. | Reuse this retention. Its current busy catch becomes CHECK_REQUIRED and visible “Result needs checking”/“Check saved results” in `combined-finalise.js:44`; give exact busy a finite Try again presentation rather than evidence/unknown wording. No new DB Stage or automatic same-item resubmit. |

These additions are local retained-request UI support, not a new command journal/replay protocol. For rebuilt import actions, retain the **actual first endpoint/action/body** and original continuation; do not implement a generic blind global reissue that loses the caller's success/partial-batch behavior. Only confirmed busy gets the transient copy. Unknown responses keep their existing original-request recovery/uncertainty behavior; a receipt parser failure must not be turned into a rollback assertion.

`import-workspace::requestJson` (`:945`) currently exposes status, `error_code` as `error.code`, and details but drops retryable/rollback/automatic_retry. Preserve only the exact finite busy metadata needed for code-driven UI classification, retaining original unknown transport fields. `friendlyWorkspaceError` has no busy mapping today and returns generic temporary-unavailable copy. `workspace-actions::plainMessage` also lacks the busy mapping and callers frequently pass only `error.message`; map from the actual structured `error.code` (and proven envelope facts where available), not text extraction alone. Use the exact fixed copy before any generic sanitiser so no raw DB body reaches the screen. Existing busy-disabled controls, initiating actor/frame checks and one-attempt behavior remain intact.

## Frozen STOP-CHECK source receipt

The saved `supabase/release/protected-boundary-lock.json` has exactly five declared frozen files. Fresh raw SHA-256 comparisons of all five saved files match their declarations:

| Frozen file | SHA-256 |
|---|---|
| migrations/22082026_0951_candidate_mytms_browser_isolation.sql | 98b2ed33447cf3c659a81f2772b5f097516158415d6fbcf95faa873ae601a9a4 |
| verification/22082026_0952_candidate_mytms_browser_isolation_verification.sql | 0feb5cdf864c893a9015e2955f5051673ca57d0eb1323b711dbac939a8d62d02 |
| migrations/22082026_1402_candidate_named_legacy_relation_isolation.sql | c5bbd78e2a2b4880bd41df421ef7ed93aae4e8de818a493767c81fbd4bd98db0 |
| repeatable/22082026_1402_candidate_named_security_definer_browser_isolation.sql | 48a585617017a518316fea11e15e502aa603ffe98b872838ca9abfda0157b080 |
| verification/22082026_1402_candidate_named_legacy_security_verification.sql | b013a4b81152d4461c6c8410e2466064ad200b3cf47ecb6279fa4ed7fe1e23ba |

This is a read-only saved-file hash audit, not an executed release/security verification, installed seal or permission to weaken any boundary. The named Worker error files are not entries in this frozen list. Keep the lock and all five files unchanged; any locked Candidate definer/file conflict stops that part for the root/owner. No Candidate SECURITY DEFINER was added/removed by this investigation.

## Source receipts

| Source | SHA-256 |
|---|---|
| BE broker/src/bpay-busy.js | c2fbf3b00a2d5f10811703fa2548587317a737eaabb5beef8ef2e5c80fd3ad7c |
| BE broker/src/weekly-source/routes.js | 3641498765ed87d9d2c450e07cfc3dca05a9601725976d2e82e315f40dde4b11 |
| BE broker/src/candidate-app-backend.js | 45826c5fd1d8232f2d9321ad4f4895b418fe302951e35d823e95bd68ceb2fe83 |
| BE candidate-broker/src/candidate-broker.js | a5b1ea663a73d5474f53a0e6393e206267bbc9a7582aac1a3cd519ac88d86a84 |
| BE broker/src/candidate-private-worker.js | 096be554abd0426ded5743ad7ac345dd22d7bc5da04e833e366bfe056800f671 |
| BE candidate-synthetic-private-api/src/index.js | a281b4b61ec8cce3575758ce3ff819c43d0655a78e5b0c2096e9920475d5c012 |
| BE protected-boundary-lock.json | 60d2d3d8a74535cf52c5213803d19b0654af48955b6096726e2b7a882545f30c |
| FE weekly-source/workspace-actions.js | 4c86b1b39870169db9e5cbbf3d9fe5f9d8795d8d68e13a86fbc296302bfb0b17 |
| FE weekly-source/import-workspace.js | bbaeea7cdc62d110b27b940483a6d565ffdc90e9a51cc2e032940cc09cc4061e |
| FE weekly-source/protected-shift-editor.js | a9725b6713da4ee875f67c7b48d9a5509ad9c0b54589949d97f761662a26d81e |
| FE weekly-source/finalise-batch.js | f099aba88dec4007347069b6021e9db73048579194bd4ff7b910d59bf8320231 |
| FE main.js | 3a51abe4360398ad21136c54587c89e1ee8c0f3197ecc4bef763208b61c38a27 |

No product source was written, and no test, SQL, runtime, fixture, hosted, provider or secret operation was performed. Future SP-12/SP-14/SP-15/SP-36 OPEN/PIN both-order/replay/page/timing/eventual-progress acceptance remains NOT_RUN; no bespoke fixture/hash/lab programme is proposed.
