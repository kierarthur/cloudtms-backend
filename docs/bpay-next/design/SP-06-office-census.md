# SP-06 Office protected-action census

Read-only source investigation, 10 October 2026. This covers card `05_SOURCE_AND_PROTECTED_PAY.md:248–276`, checklist D04/D05 and their actual supporting callers. DB/Worker completion is subject to the separate D01–D03 reviews; routing alone does not prove those deliverables. Application scenarios are NOT_RUN.

## Actual callers and transport

* `js/weekly-source/protected-shift-editor.js` builds factual protected-hours requests and renders both the editor and protected review. Existing pending approval recovery already retains the original request and key. Its old “Refresh result” label is not the card's required unknown-outcome retry.
* `js/weekly-source/workspace-actions.js::saveProtectedOnce`, `openProtectedShift`, and `openProtectedReview` are the real editor owners. Before this batch they accepted any `ok:true`; the save helper could automatically issue a second request with `recover_unknown_outcome:true`, followed by another original request. Replace that finite path with one awaited original action/payload per deliberate click. Retain the pending object on uncertainty; do not recalculate or reconstruct it from a refreshed context.
* `js/main.js::openWeeklySourceApprovedHoursModal` uses `CloudTMSWeeklySourcePresentationV1.buildApprovedHoursCommand`. That builder returns a frozen `{endpoint,body:{action,payload}}` including the exact key and expected record version. Retain this actual command once before the first POST, then replay it unchanged. The old modal generated a new key on every click and closed on every 409, which could misreport a refusal or unknown outcome as a confirmed save.
* `js/weekly-source/import-workspace.js::issueCommand` serializes `{action,payload}` with JSON.stringify; its `requestJson` exposes HTTP status, `error_code` as `error.code`, and details. Main's existing POST transport also preserves structured HTTP errors. No transport protocol or public request fields are needed.

## Genuine confirmation shapes

Use one finite pure presentation/validation helper in the existing `weekly-source-presentation-v1.js`, consumed by these real controllers before they clear pending state or close. Do not accept a generic C1 `ok:true/PUBLISHED` response.

1. Flat LOCAL: the existing orchestrator `localResult` contract is `ok:true`, outcome `PUBLISHED` or `SAVED_PENDING_FREEZE`, boolean replay, matching family UUID, generation/publication-request UUIDs, boolean `requires_first_authorisation`, and authority `UNAUTHORISED_TSFIN` or `COMMON_ENTITLEMENT_HEAD`. First authorisation must agree with the authority. State is `LIVE` or `PENDING` respectively; a pending freeze cannot require first authorisation. Real publication/deferred-release results carry extra fields, so do not impose an invented exact small key set.
2. Mapped NEXT: the real exact 14-field result is `ok,outcome,idempotent_replay,family_id,orchestration_run_id,approval_id,generation_id,work_id,revision_id,command_id,agency_sequence,new_family_bound_version,approved_pay_ex_vat,state`; outcome is `PUBLISHED_NEXT`, state `NEXT_PUBLISHED`, UUIDs are native UUID strings, sequences are positive int64 decimal text, and the target amount is exact two-decimal text. This confirms publication, not Banking APPLIED.
3. WAIT: the actual retained response has five fields `ok,outcome,idempotent_replay,family_id,family_bound_version`; fresh success adds `target_vector_sha256`. Outcome is `WAITING_FOR_SOURCE`. The existing SQL returns the version as JSON numeric; require a safe positive integer if numeric and preserve canonical text if available. Never run a calculator or substitute zero.
4. Optional closed SOURCE_LOCAL status: exact outer `{ok,route,published,receipt}` and exact 11-field SOURCE_LOCAL receipt; receipt `COMPLETE` or `PENDING_FREEZE` plus a consistent nested LOCAL result confirms the save even when `published:false`. Published is a display fact. The normal action orchestrator already returns the flat LOCAL result; the Office must not require an outer envelope on that path. The existing optional Banking receipt can truthfully be unavailable or incompatible while the original local save remains confirmed.

Malformed/unrecognized replies, lost connections and unsuccessful result parsing remain unknown and retain the exact original request. A definite typed refusal is not confirmation; expose `LOCAL_OWNER_REQUIRED` plainly. Accept only the actual native family/identity ties available in the caller, not fabricated provenance.

## Bounded UI correction

Unknown outcomes disable mutable controls and other actions, with one deliberate “Retry exact request” action. A retry sends the same endpoint, action, key, literal payload and expected version exactly once. No automatic retry, owner fallback, check-result button, new journal, Stage, polling or new command protocol is required.

Confirmed LOCAL/NEXT/WAIT results clear pending, close the editor and use the existing parent-refresh hook. A parent-refresh failure does not make a confirmed business decision eligible for resubmission. SP-35 owns later targeted refresh work. Keep approved, submitted, authorised, payment, invoice and protected status facts distinct. Before any late UI effect, retain the existing actual actor/frame lifetime checks; do not close a replacement frame or refresh for a different actor.

Only genuinely definite stale-context conflicts may retain the old conflict close/refresh behavior; a blanket HTTP 409 rule is insufficient. Preserve all factual schedule/contract inputs and existing permission authority. This census creates no SQL, Worker, money-owner, producer or Rule-B changes.

## Investigation receipts (pre-implementation bytes)

| Source | SHA-256 |
|---|---|
| protected-shift-editor.js | b2cf7f2775608588c5f9dc6137f271ecda89d7154ec26bbbe9f875792de68f46 |
| weekly-source-presentation-v1.js | fbf280c1f01c5b1f1363226142256b11db51c4d9d30a41132e6a24d728911713 |
| weekly-source/workspace-actions.js | 35c53fd6f78e11f73bd68383e347b6ad464cb3960113b9592746a9d60af08b22 |
| main.js | 4e65bb35b89748c74c60eda943cb6564c23c2b7b9182bc774722bc543f104fa0 |
| protected-action-orchestrator.mjs | 5352ef8bcc66e87b090d5a1d4f40ba872cf7a542d38f60aaf690ec1eea80f3c3 |

No product source was changed and no tests, SQL, runtime, fixtures or hosted actions were executed for this investigation.

## Finite caller-boundary choice after census

Root's BD044 choice removes the unused SOURCE_LOCAL status-envelope parser from Office. This is consistent with the real Save boundary: the Worker validates `localReceipt` and returns its flat `.result`. No current command caller receives the closed status envelope, so Office does not need a parallel Banking receipt proof decoder. The named accepted SOURCE_LOCAL behavior is fulfilled through that actual validated flat result, independently of published/application status.

For exact flat LOCAL parsing, `04102026_1257:396–400` supplies eight common keys (`ok,outcome,idempotent_replay,family_id,generation_id,publication_request_id,state,family_bound_version`). UNAUTHORISED_TSFIN adds `authority,timesheet_financials_id,requires_first_authorisation` (11 keys); COMMON_ENTITLEMENT_HEAD adds `authority,decision_bundle_id,publication,requires_first_authorisation` (12 keys). Its genuine deferred-release PUBLISHED replay appends `deferred_release_receipt_id` (13 keys), retaining the original common-head result. `05102026_0624:441–447` validates that precise retained original-result transformation. Closed parsing must include this actual third variant rather than fabricate or discard it.
