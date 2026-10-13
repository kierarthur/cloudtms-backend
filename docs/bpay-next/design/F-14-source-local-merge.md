# F-14 exact Source-local/NEXT merge

Independent read-only design, 9 October 2026, Sol 6.1 Extra High subagent. Re-read F-14/R3 checklist and the saved wiring design, the current BUILD-BE orchestrator and RESET donor, their preview module and relevant saved SQL return contracts. No product/test edits, tests, SQL, database, runtime, fixture or hosted execution. This note provides implementation blocks for the parent builder; the saved implementation still needs independent source review.

## Narrow source boundary

Target: BUILD-BE `broker/src/weekly-source/protected-action-orchestrator.mjs`, current export at 385, dependency helper 136, local completion 284, root helper 347 and final stage 533. Donor: RESET same file, `nextReceipt` 379, local validators 398–500, strengthened local completion 502, route/replay 539–587, NEXT publisher 629–647. These are pre-edit line anchors; use function names after insertion.

Apply to the current shared Source file. Keep current request normalization/preparation/context, genuine Source component/stream/publication composition, staged local replay and `externalC1RequiredForSave:false`. Add only the NEXT route/target and exact successful receipt validation. Do not bring donor's durable external C1 imports (5–9), `durablePublicationInput` (290–299), c1RawRpc gate (566–568), external replay (572–575), `durableInput` (725–732), or external final branch (741). F-14 has no DB edit.

The current local Source owner is not an operational old Banking Pay engine. Its historical C1 request/stream names are immutable evidence/staging names; retaining them does not authorize external START/status/publish or a C1 wait gate.

## Block A — import and exact success parsers

After current target-schedule import at 11–13:

```js
import { buildWeeklyProtectedNextTarget } from './protected-next-preview.mjs';
```

Carry donor `nextReceipt:379–396`, `closedObject:398–401`, `positiveBigintText:403–406`, `nonnegativeBigintText:408–411`, `localResult:413–429` and `localReceipt:431–500` as complete bodies. Place them before the exported orchestrator; declaration hoisting permits strengthened local completion to call `localResult`. Do not simplify away nested Banking receipt checks or close the variable-shape local result itself.

Exact success contracts to retain:

| Parser | Fields/types/relationships |
| --- | --- |
| NEXT receipt | Exactly fourteen fields: ok/outcome/idempotent_replay/family_id/orchestration_run_id/approval_id/generation_id/work_id/revision_id/command_id/agency_sequence/new_family_bound_version/approved_pay_ex_vat/state. `ok:true`, outcome PUBLISHED_NEXT, state NEXT_PUBLISHED, boolean replay; exact prepared family/run; seven UUID identities; positive bigint strings bounded to int64; signed two-decimal amount string. Concurrent identical publisher calls may legitimately return replay=true. A status-read saved replay specifically requires true. |
| Local result | Success only, outcome PUBLISHED or SAVED_PENDING_FREEZE, boolean replay, exact prepared family and real generation/publication UUIDs. `requires_first_authorisation` equals authority UNAUTHORISED_TSFIN versus COMMON_ENTITLEMENT_HEAD; state LIVE versus PENDING matches outcome. SAVED_PENDING_FREEZE cannot simultaneously require first authorisation. Keep additional real fields such as timesheet_financials_id, publication, family_bound_version and decision_bundle_id; they differ by actual owner/authority. |
| Local saved receipt | Exactly eleven envelope fields: authority/family_id/orchestration_run_id/publication_request_id/request_sha256/generation_id/state/requires_first_authorisation/idempotent_replay/result/banking_receipt. authority SOURCE_LOCAL; exact prepared family/run; real IDs/hash; COMPLETE or PENDING_FREEZE; replay true; nested result replay true. Generation/request/requires-first-authorisation/state must agree with the nested result. |
| Optional Banking mapping | Exact seven fields source_head_id/work_id/revision_id/command_id/agency_sequence/publication_status/application. `published` iff this mapping is nonnull. No mapping for unauthorised first save or pending freeze. Publication status only QUEUED/APPLYING/APPLIED. |
| Mapping application | Exact seventeen fields: ok/code/work_id/owner_epoch/current_revision_id/applied_revision_id/source_event_id/source_head_id/financial_snapshot_id/physical_root_id/physical_root_version/revision_seal/source_inventory_digest/application_state/command_id/agency_sequence/financial_view_revision. Preserve exact mapping identities, positive/nonnegative string epochs/revisions, nullable real UUIDs, positive safe root version, six-digit UTC seal and digest shape; correlate OK with PENDING/APPLIED and UNAVAILABLE/INCOMPATIBLE with their state. APPLIED must match applied revision and publication status; PENDING must not claim the same applied revision/APPLIED status. |

The local saved-result parser returns `value.result`, preserving the existing Source response rather than exposing the internal envelope as a new UI contract. A historical mapping becoming INCOMPATIBLE after a legitimate newer revision does not erase the original local receipt or permit another publication. PUBLISHED local save may still mean unauthorised TSFIN save; only its real nonnull separately sealed Banking mapping proves that publication.

## Block B — preserve typed refusal, then validate successful local identity

Replace only current `completeLocalDecision:284–297` and pass `prepared` at its two call sites. This is the complete intended block:

```js
async function completeLocalDecision(normalized, prepared, status, dependencies) {
  const result = await rpc(dependencies, 'weekly_exceptional_pay_complete_local_v1', {
    schema_version: 'WEEKLY_PROTECTED_LOCAL_COMPLETE_V1',
    actor_user_id: normalized.actorUserId,
    publication_request_id: status.publication_request_id,
    expected_request_sha256: status.request_sha256,
    idempotency_key: `${normalized.idempotencyKey}:local-complete`,
  });
  // Preserve the current Source returned-refusal code/message/details first.
  if (result?.ok !== true || !['PUBLISHED', 'SAVED_PENDING_FREEZE'].includes(result.outcome)) {
    fail(result?.code ?? 'WEEKLY_PROTECTED_LOCAL_COMPLETE_REFUSED',
      'The protected hours could not be saved. Your changes have been kept.', result ?? {});
  }
  const accepted = localResult(result, prepared);
  if (accepted.publication_request_id !== status.publication_request_id) {
    fail('WEEKLY_PROTECTED_LOCAL_RECEIPT_INVALID',
      'The saved protected-hours result belongs to another request.');
  }
  return accepted;
}
```

There is no catch around the RPC. An actual thrown SQL/PostgREST refusal retains its original status/body/json/code and existing rollback guard-record wrapper behavior. In particular current protected admission's PT409 remains HTTP 409 through `weekly-source/routes.js:953–977`. Calling donor `localResult` before the current returned-refusal branch would mask a genuine `{ok:false,code:...}` as generic receipt-invalid; avoid that regression. A malformed purported success gets the typed parser refusal, not success or a second owner call.

## Block C — database route, immutable replay, then current local fallback

Keep current normalization/prepare/kind checks at 385–399. Replace current 401–420 with this flow. `closedObject` comes from Block A; no environment module-owner read or Worker family-owner inference is added.

```js
let nextRoute = false;
if (normalized.kind !== 'WAIT') {
  const route = await rpc(dependencies, 'weekly_exceptional_pay_next_publication_status_v1', {
    schema_version: 'WEEKLY_PROTECTED_NEXT_STATUS_V1',
    actor_user_id: normalized.actorUserId,
    family_id: prepared.family_id,
    orchestration_run_id: prepared.orchestration_run_id,
  });
  if (!closedObject(route, ['ok', 'route', 'published', 'receipt'])
      || route.ok !== true || !['NEXT', 'C1', 'SOURCE_LOCAL'].includes(route.route)
      || typeof route.published !== 'boolean'
      || (route.route === 'C1' && (route.published || route.receipt !== null))
      || (route.route === 'NEXT' && !route.published && route.receipt !== null)
      || (route.route === 'SOURCE_LOCAL' && route.published && route.receipt === null)) {
    fail('WEEKLY_PROTECTED_NEXT_STATUS_INVALID', 'The protected-hours owner is unavailable.');
  }
  if (route.route === 'SOURCE_LOCAL' && route.receipt !== null) {
    return localReceipt(route.receipt, prepared, route.published);
  }
  nextRoute = route.route === 'NEXT';
  if (route.published) {
    // All non-NEXT published states were handled/rejected above.
    if (!nextRoute || route.receipt?.idempotent_replay !== true) {
      fail('WEEKLY_PROTECTED_NEXT_RECEIPT_INVALID',
        'The saved protected-hours receipt is invalid.');
    }
    return nextReceipt(route.receipt, prepared);
  }
  if (!nextRoute) {
    const status = await publicationStatus(normalized, prepared, dependencies);
    if (status.staged) {
      // Same current local receipt; no context/calculation/external C1 call.
      return completeLocalDecision(normalized, prepared, status, dependencies);
    }
    if (prepared.run_state === 'COMPLETE' && prepared.idempotent_replay === true) {
      fail('WEEKLY_PROTECTED_PUBLICATION_STATUS_INVALID',
        'The protected-hours publication status is unavailable.');
    }
    if (normalized.recover) {
      fail('C1_DURABLE_RECOVERY_NOT_REQUIRED',
        'There is no unresolved protected-hours publication to recover.');
    }
  } else if (prepared.run_state === 'COMPLETE') {
    // Defensive contradiction: DB says direct NEXT but returns no saved receipt.
    fail('WEEKLY_PROTECTED_NEXT_RECEIPT_INVALID',
      'The saved protected-hours receipt is invalid.');
  }
}
```

The only direct NEXT choice is the real status RPC's route. Its existing SQL (`04102026_0345:203–277`) verifies actual run/family/actor and calls the installed private discriminator for a fresh active family. It returns real prior NEXT receipts before consulting current module activity; accepted receipt replay is independent of current activation. SOURCE_LOCAL saved replay is likewise read from the real `weekly_source_local_saved_status_v1`, including pending/first-authorisation/effective state, before any recalculation.

Historical `route:'C1',published:false,receipt:null` still exists for fresh inactive LEGACY (`0345:258–264`). Treat that label only as the current non-NEXT Source-local completion path. Do not revive external C1, require c1RawRpc, choose owner from active_owner in Worker, or change SQL just to rename this label. SOURCE_LOCAL with null receipt is a valid fresh local action; SOURCE_LOCAL with saved receipt returns it whether published is true or false, after the nested validator checks why.

## Block D — WAIT bypasses financial preparation; retain current local first save

Change current context/root block 421–422 only:

```js
let context = await loadContext(normalized, prepared, dependencies);
if (!nextRoute && normalized.kind !== 'WAIT') {
  context = await ensureRootFinancial(normalized, prepared, context, dependencies);
}
```

Keep current target composition and WAIT atomic request at 424–449 unchanged. WAIT skips owner status/replay and root-financial preparation, then returns the real `weekly_exceptional_pay_wait_atomic_v1` before calculation/TSFIN staging/NEXT publication. WAIT may persist its own Source decision/observation through that existing owner; “non-writing” here means no financial-root creation, TSFIN write or entitlement/payment publication, rather than replacing its real atomic owner with a fabricated read-only response.

For a genuine fresh non-NEXT local action, `ensureRootFinancial` remains exactly current: if needed, the existing non-writing empty calculator snapshot feeds the actual Source managed-root prepare owner, then reloads current context. Do not carry NEXT facts into this zero-root helper, skip its real Source first-save prerequisite, or manufacture financial readiness.

## Block E — complete NEXT target before the local-only financial/stream block

At current calculator call 451–455, add the conditional flag:

```js
const calculation = await dependencies.calculateWeeklySnapshot({
  context,
  schedule: target.segments,
  zeroRoot: false,
  ...(nextRoute ? { retainNextPreviewFacts: true } : {}),
});
const financialSnapshot = stageSnapshot(calculation, context);
```

Immediately after current stageSnapshot at 456, before `object(context.current_financial)` at 457, insert donor NEXT publisher block 629–647 with its complete field set:

```js
if (nextRoute) {
  const targetSnapshot = buildWeeklyProtectedNextTarget({ context, target, calculation });
  const result = await rpc(dependencies, 'weekly_exceptional_pay_publish_next_v1', {
    schema_version: 'WEEKLY_PROTECTED_NEXT_PUBLISH_V1',
    actor_user_id: normalized.actorUserId,
    family_id: context.family_id,
    orchestration_run_id: context.orchestration_run_id,
    source_cycle_id: context.source_cycle_id,
    client_id: context.client_id,
    work_event_id: context.work_event_id,
    evidence_timesheet_id: context.candidate_submission?.submission_id ?? null,
    expected_family_bound_version: context.family_bound_version,
    protected_schedule: context.protected_schedule,
    rate_classification: {
      schema_version: 'WEEKLY_PROTECTED_SERVER_CALCULATION_V1',
      calculator_owner: 'buildWeeklyScheduleSegmentsSnapshot',
      policy_sha256: context.zero_rate_source_refs?.effective_policy_sha256 ?? null,
    },
    source_proposal: context.source_proposal,
    target_snapshot: targetSnapshot,
    current_comparison_revision_id: context.current_comparison_revision_id,
    current_final_revision_id: context.current_final_revision_id,
    reason: normalized.reason,
    idempotency_key: `${normalized.idempotencyKey}:next`,
  });
  return nextReceipt(result, prepared);
}
```

No catch/retry/fallback. The saved SQL publisher rechecks current NEXT owner under shared lock, real family owner, approved physical root, exact prepared/request fingerprints, source manifest and complete target, and returns exact publication receipt or raises a typed SQL refusal. Its current failure paths raise exceptions; generic success-parser failure must not catch those exceptions. A concurrent identical publisher replay may return true and is valid.

The NEXT branch must precede the local requirement for `context.current_financial.id` and the C1 component/stream/stage block. NEXT uses its authorized physical root and genuine Source vectors; it must not create a legacy zero financial row to satisfy an irrelevant local stream requirement. Keep current local `financialSnapshot` and everything at 457–537 unchanged. Change final local call at 538 to:

```js
return completeLocalDecision(normalized, prepared, staged, dependencies);
```

Keep its existing staged success/request-ID/hash equality preflight. The local path retains original request IDs and `:stage`/`:local-complete` keys, so lost first-save response reuses the real immutable receipt without recalculation or second publication.

## Block F — the three index seams and genuine evidence

1. Import `retainWeeklyProtectedPreviewFacts` alongside current component-identity import; keep current paid-evidence import. Add `retain_protected_next_preview_facts: input.retainNextPreviewFacts === true` in protected calculator options (original target 3055; RESET 3072). Current calculation return/projection (3061–3063) stays intact: direct NEXT returns retained calculation; Source-local projects the genuine approved Source component identities.
2. In `buildWeeklyScheduleSegmentsSnapshot` options, add default `retain_protected_next_preview_facts=false` (original target 172370; RESET 172512). Keep current no-writing preview guard and add donor 172518–172520: retained facts require `write_now === false` and `ignore_locked_segments_for_preview === true`.
3. At the real per-shift `segments.push`, add donor spread 173325–173327 using actual `seg`, `shiftFinancials`, `pay` and `chg`; no browser money or invented rate. Other calculator callers retain their existing result shape/behavior.

Port the complete F-14-D01 `protected-next-preview.mjs` from RESET (180 lines), with its exact exported retainer and target builder. The builder bounds rows to 100 and target JSON to 120000 bytes, requires exact schedule/event/preview membership and physical-root identity, preserves protected-event server rates/minutes, and uses accepted Source `next_source_segments` pay/charge vectors plus actual movement/final/resolution/economic provenance for imported work. It does not reprice imported Source work from preview totals. Expenses use Source expense/final/document authority, not browser additions. No totals/residual/pay-on-hold/old Workbench logic is introduced.

## Read-only review checklist after the parent saves

- Only DB route NEXT selects direct NEXT; historical C1 and fresh SOURCE_LOCAL keep the existing local path. Both saved receipt types return before context/calculation. No durable external C1 import/call/dependency remains in the orchestrator.
- Returned local refusal keeps current code/details; thrown SQL refusal remains untouched. No catch converts PT409 into 500 or restarts stale input. Exact success identity parsers reject malformed purported success.
- WAIT skips status/root prepare/calculation/publish and uses the unchanged Source WAIT owner. Direct NEXT skips zero-root/local-current-financial/stream staging. Local first save retains genuine prepare/stream/stage/local completion.
- New preview flag is conditional server-only, strictly non-writing and off by default; current component identity and newer paid/factual Source branches remain intact.
- Original idempotency suffixes stay stable; direct NEXT publication does not enqueue execution, drain jobs, call a provider or require an operational old payment engine.
- Contract continues `externalC1RequiredForSave:false`; comments describe current Source-local/NEXT behavior. Update the frozen-boundary log explicitly, then source-review the complete final diff. This reviewer will not run tests or runtime.
