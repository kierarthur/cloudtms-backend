# SP-10 narrow transport and retained-request design

12 October2026. Independent source/design check of the complete existing Weekly Source route and reachable Office decision caller. Root alone writes product. This slice changes no SQL, financial owner, Source producer, schema, command kind or screen layout. Card05:374/385–388 and checklist SP-10-D04/D05 control. D03's factual S3 enqueue mismatch and unbound paid Source-removal authority remain explicitly BLOCKED as recorded in `SP-10-investigation.md`; this design does not close them.

## Exact Worker seam

Add one explicit `LATER_CHANGE_DECISION` branch in `broker/src/weekly-source/routes.js::handleCommand` (currently570–722), before any generic dispatch. Keep the single POST `/api/weekly-source/v1/commands` boundary and `requireOfficeUser` at :260–267/:1038. Close the outer envelope to action/payload for this action and the payload to the seven server-descriptor operands below; do not add it to an unrestricted RPC-name map. Existing recursive `assertNoBrowserFinancialFacts` remains before dispatch. If root retains a browser actor field, it must be the sole explicitly permitted compatibility field and be ignored/replaced, never authoritative; the simplest new wrapper carries no actor field.

| Native RPC field | Exact authority / transport rule |
|---|---|
| `schema_version` | Literal `WEEKLY_SOURCE_LATER_CHANGE_DECISION_V1`. |
| `decision_bundle_id`, `root_timesheet_id`, `final_revision_id` | Original reader UUID strings, qualified using existing native UUID validation; never infer from selected rows or history. |
| `bundle_revision` | Positive original reader revision; preserve its current safe-integer representation, never round an unsafe bigint. |
| `decision` | Exactly APPROVE_UPDATED_HOURS or KEEP_CURRENTLY_APPROVED_HOURS offered by the original descriptor. |
| `idempotency_key` | Original nonempty16–200-character native key; create once and retain unchanged. |
| `actor_user_id` | Authenticated Office `user.id`, injected by Worker to make the RPC's exact eight-key object. |

Reuse the fixed `public.weekly_source_later_change_decide_atomic_v1(jsonb)` through existing `callRpc` (:269–275), with its sole named `p_request` argument and current bounded timeout. The original native owner already checks the eight-key schema, authority, exact actor/full-input request hash, early receipt replay and atomic witness/publication. Do not append module epoch, financial numbers, new proof or another acceptance key to that closed request. The small shape is bounded by its fields and the existing command-body byte limit; unrelated command limits remain unchanged.

## F12 authority and error boundary

Expose only the existing `bpayNextOwnerStatusRpc(env)` factory at index.js:40995–40997 as a dependency in the existing Weekly Source dependency object at :198861–198864. The route invokes the fixed zero-argument dependency for this action and validates its result with existing `normalizeBpayNextOwnerStatus`. This reuses the genuine service-only1024-byte/8-second closed `{active_owner,owner_epoch}` transport in `bpay-next-legacy-fence.js:23–33/60–136`. Only NEXT reaches the decision RPC. LEGACY remains deliberate unsupported; DISABLED or malformed/failed/missing authority returns a bounded plain refusal. Do not cache ownership, treat an old route result as permission, invoke an old financial engine or expose the service owner RPC to the browser.

F12: this status read is admission evidence, not a lease across the next network call; existing database/module gates and the separately required quiet-switch/reconciliation remain authoritative. No transport-only atomic epoch guarantee is claimed. The Source admission key, family/bundle locks and command ordering stay inside the existing SQL owner.

Keep new error mapping action-specific. A definite pre-RPC refusal proves only that this attempt did not dispatch. For a dispatched RPC, use exact native SQLSTATE/message pairs or the native returned refusal; do not classify every409/500/network error as a definite failed decision. A lost/empty/malformed reply, timeout or infrastructure error is unknown. Preserve the original managed-root rollback recorder where applicable. Do not broaden `routeErrorResponse` or alter other action outcomes. No queue wake failure or post-acceptance presentation refresh may turn an original validated accepted result into a rejected decision.

## Office wrapper and namespace

`weekly-source-presentation-v1.js:1596–1620` already qualifies the real descriptor against root/bundle/revision/final and exact two actions. Keep it. `buildLaterChangeDecisionCommand` (:1905–1924) returns the same fixed endpoint with `{action:'LATER_CHANGE_DECISION',payload:{...decision.command_payload,decision,idempotency_key}}`. No money values or extra authority metadata enter the request. Generate the key only for a new user decision. A pending retry must not rebuild the command from a later view model or re-run fresh-decision eligibility against a proposal that may already have been accepted.

The delegated main caller at :179344–179399 owns a small in-closure pending record. Bind it to the actual signed `SESSION.user.id` (existing Source modal precedent :179087–179094), initiating modal frame/stack prefix and context identity, fixed API endpoint, native root and original decision tuple. Bulk also captures the exact bulk state and active-row subject; Simple captures its existing Timesheet modal context/root. Reject a missing actor rather than use `server-resolved` or stale window aliases. Do not let a different actor, Timesheet, bulk row or replaced frame dispatch the old request. Re-rendering the same native parent may recover its pending record; a different subject may not adopt it. No persistent store/table or cross-reload recovery claim is added.

Retain the frozen original command and serialized JSON before dispatch. `apiPostJson` currently serializes an object at main.js:3802; replay that same immutable object/property order and verify its serialization is unchanged rather than passing an already serialized string and double-encoding it. Reuse its existing authenticated transport. Capture busy synchronously before any await so another delegated click cannot admit an opposite decision. Restrict disabled controls/status updates to the owning surface, not every Weekly Source button in the document.

## Exact outcome lifecycle

1. New decision: capture namespace and original request/key, mark its record in-flight, then dispatch only while that namespace is current.
2. Unknown: keep the original record and disable the opposite decision; the only next financial action is Try again with identical endpoint/body/key. The needed inline sentence may read “The decision could not be confirmed; try again to check the same decision.” Preserve the required live-copy marker so native remount does not discard an initially empty notice. Do not tell the user to create a new decision or replace the original key after Refresh.
3. Validated native success: exact decision/bundle match. Native retained KEEP uses original `bundle_revision` with `retained:true/published:false` (:1894–1915); a publishing successor uses original `reviewed_bundle_revision` and next bundle revision (:2034–2040). Require the actual literal boolean qualifiers and `idempotent_replay` rather than treating a generic `ok:true` as the original receipt. Clear pending acceptance uncertainty before the independent presentation refresh; if that refresh fails, keep the acceptance and offer a plain Refresh notice.
4. Definite original refusal: release fresh-decision controls only when native evidence establishes the original outcome. A later pre-dispatch owner/auth refusal cannot erase an already-unknown original attempt; retain it for the original actor/native namespace. An opposite click, stale proposal or refreshed final must not replace it.

After every await check captured actor/endpoint/frame/context/native subject before dispatch, touching controls, refreshing Bulk/Simple, focusing or showing a toast. A validated result may settle its own retained record even when its frame is no longer visible; it must not refresh or announce success on a different screen. Existing Simple refresh at :179009–179054 has a same-context check after its fetch; existing Bulk refresh at :179057–179073 takes an explicit state. Check the captured caller before invoking either and again after it returns. Keep accepted truth independent of refresh completion. No new dialog, approval flow or design-only control is required.

## Source/design verdict

This is the smallest card-complete D04/D05 transport slice provided the above exact request retention and stale-caller fences hold in the saved product. Source review of the actual implementation remains pending; all joined/business acceptance is NOT_RUN. No product edits, execution, fixtures, database calls or tests were performed for this check.
