# SP-02 Source retirement boundary: finite build design

Read-only saved-source investigation, 10 October 2026. Card: normal plan `05_SOURCE_AND_PROTECTED_PAY.md:128–153`; sequence 37; exact checklist D01–D06 at `docs/bpay-next/IMPLEMENTATION_CHECKLIST.md:940–945`. Dependencies F-03/F-12/F-15/F-16/F-18. Root alone authors product. No SQL, runtime, fixtures, tests, business operations or hosted reads were performed. F-25 Rule B remains closed.

## Decision for the builder

Keep generic Source finalisation and **all shared 2336 open/task and ordinary TSFIN/proposal facts**. Do not fence a routine because its name contains “pay”. Retire only the separate RECOVER_FINALISED_PAY entry and the external legacy C1 transport. Root's proposed BD041 one zero-argument service-only `public.bpay_next_source_legacy_finance_admit_v1()` is sufficient as the small supporting database authority for both entries, provided the exact admission response/refusal survives their existing transports. No Source C1 stage/read/complete-local rewrite is required merely to install this entry boundary.

The database, not Worker state/configuration or browser owner display, decides retirement. A successful standalone admission transaction is a preflight; it is not an enduring permission token. Existing 0550 database write fences remain the final backstop when ownership changes between that preflight and a later legacy financial call. Never describe the preflight as atomic with a separate HTTP transaction or claim an inspected saved C1 core body where none is present in this source tree.

## Six exact deliverables

| Row | Required object | Reuse / minimal change |
|---|---|---|
| D01 | 0550/0555/0556 boundary | Reuse actual installed-source definitions unchanged. Facts and approved NEXT publication stay writable; legacy callbacks skip, legacy financial tables refuse. Include 0557 first-authorisation wrapper in the effective owner census. |
| D02 | Source retired financial entry successors / fanout census | Add the one service-only BD041 admission routine and final-definer entry. Call it only at real Recover Pay and external C1 entries. Preserve 2336 and ordinary apply, no fake retirement receipt, no new producer. Existing fanout callback suppression below is sufficient; no blanket timesheet-authorisation refusal. |
| D03 | 17092026_0100 absence notice / arrival semantics | Narrow latest successor for NEXT-specific diagnostic wording/gate, preserving LEGACY classifier branch and stored declaration facts. Distinguish “legacy integration absent” from actual NEXT arrival; see the explicit alternatives below. No setting mutation from UI or installation. |
| D04 | finalisation-pay-orchestrator.mjs | `recoverWeeklySourceFinalisationPayProjection()` calls fixed database admission before its current `pay_open` RPC. `orchestrateWeeklySourceFinalisation()` does not call it: genuine finalisation and preparation receipt survive. Normalize only exact genuine database retirement/disabled/unavailable refusals. |
| D05 | weekly-source-c1-raw-rpc.mjs | Fixed admission request before original requested external C1 RPC, without parsing/re-encoding the raw business request/result. LEGACY raw request still goes to the genuine C1 owner once; NEXT stops before that call. Known refusal stays known; timeout/loss stays ambiguous. |
| D06 | FE weekly-source/import-workspace.js | Owner-display qualification through existing F15 `CloudTMSBpayNextOwner`, actor/scope freshness and plain NEXT-only text. Do not disable finalisation/import/query/auth facts. Preserve LEGACY recovery controls and existing retry identity. |

## 2336 task census: facts versus retired financial entry

Physical current owners are all in `supabase/repeatable/15092026_2336_weekly_source_finalisation_pay_orchestration_v1.sql`; current-source definition census found no later competing definitions of these names. There is **no task_kind column or dispatch discriminator**: each per-root task follows one ordinary Source projection pipeline. No task creates a Banking Draft, bank transfer or payment effect by itself.

| Exact signature | Source lines | Actual work / NEXT treatment |
|---|---:|---|
| `private.weekly_source_finalisation_pay_context_v1(uuid,uuid)` | 10–184 | Reads actual final revision, client manifest, roots, source units/segments/expenses and bound hashes. Retain. |
| `private.weekly_source_finalisation_pay_task_json_v1(public.weekly_source_finalisation_pay_tasks)` | 186–207 | Finite task receipt projection. Retain. |
| `private.weekly_source_finalisation_pay_run_refresh_v1(uuid)` | 209–266 | Recomputes genuine task progress; does not fabricate financial success. Retain. |
| `private.weekly_source_finalisation_pay_run_json_v1(public.weekly_source_finalisation_pay_runs)` | 268–294 | Existing run/task projection. Retain. |
| `public.weekly_source_finalisation_pay_open_atomic_v1(jsonb)` | 296–463 | Original service/actor/client authority; locks exact final revision; creates retained run/context task facts (417–455), replay refresh (374–385). Retain for finalisation; Recover Pay admission precedes entry so NEXT does not open a recovery run. |
| `public.weekly_source_finalisation_pay_task_start_atomic_v1(jsonb)` | 465–565 | Exact task/context/version revalidation; records SUBMISSION_STARTED and original projection idempotency key, not bank payment. Retain. |
| `public.weekly_source_finalisation_pay_task_finish_atomic_v1(jsonb)` | 567–676 | Links actual immutable ordinary projection receipt; updates original transition and task state. Retain. No manufactured terminal receipt. |
| `public.weekly_source_finalisation_pay_task_unknown_atomic_v1(jsonb)` | 678–750 | Retains genuine uncertainty in task/run metadata. Retain. Do not convert a known NEXT entry refusal into UNKNOWN. |
| `public.weekly_source_finalisation_pay_task_recover_atomic_v1(jsonb)` | 752–886 | Existing terminal replay, genuine receipt reconciliation and explicitly confirmed READY retry (873–884). Retain shared owner; NEXT external action stopped earlier by BD041, so it cannot reopen/rearm by that route. |

Terminal actual outcomes: `PREPARED_FOR_AUTHORISATION`, `PROPOSED`, `NO_OP_FIRST_NEGATIVE`, `TARGET_MANAGED_SUPPRESSED`, plus FAILED as terminal actionable state. `SUBMISSION_STARTED` and `RECOVERY_REQUIRED` are uncertainty, never retired by inference. `run_refresh` counts these actual states (209–266); Source finalisation receipt is separate.

`public.weekly_source_finalise_atomic_v1(jsonb)` physical owner remains `15092026_1534_weekly_source_finalisation_v1.sql:2796`; Worker calls it first and deliberately leaves indeterminate responses uncaught (orchestrator 976–982). `sourceFinalisedResult()` 719–728 preserves `source_finalised`, `invoice_authority_committed` and genuine `source_finalisation`. The existing 985–1007 second-phase catch returns FINALISED_PAY_CHECKPOINT_REQUIRED; it must not be the path for a new blanket NEXT refusal in generic finalisation.

`public.weekly_source_ordinary_pay_projection_apply_atomic_v1(jsonb)` remains `15092026_1534_weekly_source_ordinary_pay_projection_v1.sql:2334`. Original exact receipt replay 2446–2458 is preserved. Concrete branches:

- TARGET_MANAGED_SUPPRESSED: actual target-family evidence, immutable receipt and Source transition publication (2861–2886).
- NO_OP_FIRST_NEGATIVE: actual unit predicate, immutable no-op receipt (2892–2906).
- Never-authorised root: original Candidate evidence check; factual `timesheets.actual_schedule_json` update and bounded `tsfin_write_current_snapshot_single_bounded` (2955–2994), outcome PREPARED_FOR_AUTHORISATION. Comment 2996 explicitly excludes `timesheet_authorise_generic_atomic`.
- Already-authorised root: actual complete entitlement components/proposed Source decision/head (3008–3033), outcome PROPOSED; no unauthorise/reauthorise or live TSFIN rewrite.
- Source transitions/receipt remain actual (3036–3060). These are not grounds for retiring the shared apply function.

## Existing final boundary owners and fanout

0550 `private.bpay_next_assert_legacy_writer_v1()` and `private.bpay_next_legacy_writer_fence_v1()` use the actual id=1 module row `FOR SHARE`, owner/epoch validation and 66 ENABLE ALWAYS trigger attachments over 33 legacy financial tables. `banking_pay_scope_change_transactions` is deliberately factual and excluded. The helper rejects non-LEGACY; those trigger errors are the old 55000 backstop, not necessarily the new HTTP typed entry result.

0555 `private.bpay_next_legacy_callback_enabled_v1()` 12–28 is the correct shared owner decision: validates actual row, owner and positive epoch while holding `FOR SHARE`; true only for LEGACY. Its eight preserved full-body final callbacks are:

1. `public.pay_timesheet_summary_pay_state_refresh(uuid[],uuid)` line58.
2. `public.pay_timesheet_summary_pay_state_refresh_trigger()` line840.
3. `public.pay_timesheet_summary_advance_state_refresh(uuid,uuid)` line1691.
4. `public.pay_workbench_mark_candidate_dirty()` line1841.
5. `public.pay_workbench_dirty_event_enqueue(text,text,text,uuid,uuid[],uuid[],jsonb,text,integer,timestamp with time zone)` line3068.
6. `private.weekly_source_entitlement_head_ledger_mark_v1()` line3390.
7. `public.pay_workbench_scope_counter_stage_trg_v1()` line3502.
8. `private.pay_workbench_scope_invalidate_v1(uuid[],uuid[],text,uuid,jsonb)` line3566.

These skip retired financial projection/fanout under NEXT/DISABLED while returning genuine factual trigger tuples. Do not disable factual triggers. Do not replace them with older 2609 bodies.

0556 final `private.weekly_source_entitlement_publish_core_v1(jsonb,text,jsonb,uuid,text,uuid,jsonb,jsonb)` line81 and `public.weekly_source_first_authorisation_withdraw_v1(uuid,uuid,text,uuid)` line2126 retain factual receipt/audit/head/publication semantics. Its `private.bpay_next_source_factual_contract_v1(uuid,uuid,uuid[],text)` line12 proves declared factual frame/token and absence of old financial effects. Exact fresh publication Source admission occurs before callback's module SHARE (329–332); immutable replay is earlier. Preserve this Source→module ordering and all current Rule B producer contracts.

0557 actual first-authorise wrapper `public.weekly_source_first_authorise_v1(uuid,uuid,text,uuid)` line92 invokes genuine original authority, then stages/publishes actual NEXT approved source pair (164–173), no dummy old pay receipt. Current ordinary `public.timesheet_authorise_generic_atomic(uuid,uuid,uuid,timestamp with time zone,text)` and `public.pay_workbench_contract_client_dirty_fanout_chunk(uuid,jsonb,integer)` registry physical winner is `03102026_0500_h1h2_successor_authorities_after_retry.sql`; do not overwrite with historical 26092026_0202 excerpts. The callback enqueue/invalidation boundary, not a blanket authorise refusal, retires the Workbench fanout.

## BD041 exact entry protocol and refusal transport

Proposed sole new routine: `public.bpay_next_source_legacy_finance_admit_v1() RETURNS boolean`, service-only; same audited logical owner mapping, private/module search path, VOLATILE because the actual owner helper takes a row SHARE. Service JWT-role validation; revoke PUBLIC/anon/authenticated, grant service_role only. No trigger, table, producer, command, financial receipt or GUC permission.

Use existing 0555 callback owner validation. LEGACY -> exactly true. If false, distinguish actual NEXT -> SQLSTATE PT410 / message `BPAY_NEXT_LEGACY_WRITER_RETIRED`; actual DISABLED -> PT503 / `BPAY_NEXT_LEGACY_WRITER_DISABLED`; missing/invalid owner/epoch -> the existing owner-unavailable rejection, normalized to an unavailable HTTP refusal, never LEGACY. The same module SHARE already held protects a second exact owner read within the helper transaction. Do not invent a bool false “success” transport.

- Recover worker: original request/actor validation first (1024–1035); call fixed admission with empty named args before existing `weekly_source_finalisation_pay_open_atomic_v1` at1037. NEXT refusal exits before run/task refresh/writes or calculation, does not fabricate sourceFinalisation at1068. LEGACY continues unchanged. No client owner field, no broad fence on `/api/weekly-source/v1/commands`.
- Raw C1 worker: after original RPC-name/raw-size/no-retry options validation (94–111), before original HTTP at119, make exactly the fixed small admission RPC. Do not call F12 Worker `assertLegacyWriter` as the decision for this Source action. Preserve raw `parametersJsonText` verbatim. Never retry an indeterminate admission automatically; it is read-only but must be unavailable, not proof of permission.
- Preserve the raw known/unknown distinction. Received genuine PT410 database response is known nonexecution; normalize exact database `message` plus allowed HTTP/status contract to the typed code. Never infer retirement from arbitrary error text or `error.code=55000` alone. Ordinary sbRpc errors may carry PostgreSQL code in `error.code` and typed message in `error.json.message`; normalize exact shape before the route's generic error handler. Existing `routes.js:953–976` otherwise exposes generic code or raw SQLSTATE.
- In C1 raw known-error path, preserve bounded raw body/status/rpcName and `outcomeKnown=true`, `unknownOutcome=false`; add typed code only for the exact admission refusal. Adapter `#invokePrepared` 874–882 passes known errors, so they must not become durable UNKNOWN checkpoints. Do not stringify parsed raw C1 business replies; lossless bigint streams stay untouched.

F12's existing route predicate covers `/api/banking/{pay|finance}` and selected old money setters, but explicitly does not cover the shared Weekly Source commands route. This is why BD041 is necessary. Index `198605–198641` correctly wires protected actions and finalisation/recovery to their real transports; no new owner selection belongs in this seam.

## C1 entry / retained local evidence census

`15092026_1534_weekly_source_protected_pay_c1_publication_v1.sql` owns actual local service JSONB routines: stage903, read719, complete1909; exact signatures all `(jsonb)`. The same file owns checkpoint, unknown and recovery recording. Stage writes genuine approval/generation/family/source/component/request facts; read is STABLE and returns exact scoped immutable request (754–767); complete consumes real original C1 publication/checkpoint and records local LIVE lineage, rather than creating a fictitious Banking receipt. Keep these facts and their guards, including real immutable published replay (1984 onward). Do not forbid source staging by finance-sounding names.

`weekly-source-c1-durable-publication.mjs:10–16` maps stage/read/checkpoint/unknown/recovery/complete. At210 it reads the durable request; PUBLISHED replay216–218 returns existing receipt, unresolved unknown219–223 refuses; only then adapter at227 reaches C1. External entry admission belongs in the raw transport, so local evidence remains available.

Actual fixed external RPC call census (`weekly-source-c1-adapter.mjs:52–70`):

- publication: `weekly_source_start_c1`, `weekly_source_stage_c1`, `weekly_source_continue_c1`, `weekly_source_certify_c1`, `weekly_source_publish_c1`, `weekly_source_pending_c1`;
- lifecycle: `weekly_source_abort_c1`, `weekly_source_retire_c1`, `weekly_source_repair_c1`, `weekly_source_status_c1`;
- scope/Workbench: `weekly_source_scope_start_c1`, `weekly_source_scope_continue_c1`, `weekly_source_begin_r01_c1`, `weekly_source_scope_status_c1`, `weekly_source_scope_abort_c1`, `weekly_source_scope_release_c1`, `weekly_source_workbench_execute_c1`, `weekly_source_cleanup_c1`.

All use the one raw transport; the fixed admission should precede each external legacy call under this Source route. Their actual complete core DDL definitions/signatures are **not present in the saved agency repeatables nor this final-definer registry** (only calls/allowlisted owner evidence). Do not manufacture sealed-core successors or claim their current installed bodies from names. Existing 0550 fences still cover actual legacy Banking tables; the absent core-body census is evidence to F18, not an assertion of no in-flight state. Native F18 reconciliation must retain its actual C1 evidence/read authority independently; a denied raw continuation does not prove an old C1 operation completed.

RETIRED compatibility requires exact local request/run/digest/generation/family completion evidence per card. Active/unknown/missing evidence remains unresolved. Known cited staged request `7dce830b-e8e6-8621-98e6-20e775ffa555`, run `aaeecf33-3ad7-413e-8cf0-e83ab21ac8fd`, is a prior source citation, **not a fresh hosted observation**. List it for F18; do not retry, cancel, delete, migrate or report it retired. Do not invent an in-flight fixture or ignore it because NEXT admission refuses.

## Absence declaration / actual NEXT arrival

Original0100 owners: branch_probe(text)104; integration_arrived()164; absence()227; boundary_notice()318; absence_declare(text,text)354; absence_clear()388. All private API execution is revoked. The setting is read only by absence(); absent/off/unknown/nonTEST return declared=false; declared=true probes **legacy** classifier two exact branches and raises if arrived. No Source owner calls the notice (314); notice is informational and not a payment stop. Original text335–339 about “not connected yet” becomes wrong for an actual NEXT owner.

Minimal exact successor can return a NEXT notice **before** initializing/calling old absence(), using actual validated module owner/epoch. Headline/message: **“Pay is published through Banking Pay NEXT; there is nothing to recover here”**. Finite reason `BANKING_PAY_NEXT_SOURCE_PUBLICATION`, informational severity, changes_no_outcome=true. Do not claim Source approval or completion merely from NEXT owner. LEGACY path retains original absence call, exact classifier checks, setting-off NULL result and original disclosure. DISABLED/unavailable must not render NEXT copy.

For the arrival detector, document/implement NEXT as its own actual-owner integration arrival: `arrived=true` with explicit NEXT reason/owner epoch, before the old legacy classifier probes. Under LEGACY retain the old typed shape and both reason-specific branches unchanged. Then a stored declared absence is truthfully obsolete under NEXT: direct absence()/declare() refuse it as no-longer-true; the informational NEXT notice can still be returned without executing that stale legacy declaration. No migration silently clears or relabels existing declaration. If the builder instead leaves the old detector explicitly named as a legacy-only diagnostic, record that scope and add a NEXT-specific notice branch; do not falsely make a legacy classifier result prove NEXT absence/arrival. The card requires a settled choice, not both alternatives in completed product documentation.

## Office exact seams

Current FE import-workspace has no owner-status integration. `friendlyWorkspaceError`68–78 knows only WEEKLY_SOURCE codes; required typed retirement copy is missing. Current approved-hours follow-up134 permits RECOVER_FINALISED_PAY; handlers1600–1635 retain exact recovery request/retry metadata. Add NEXT-only display replacement/removal of the recovery action while retaining actual Source status/detail/history, not a top-level finalise-disable.

Use existing F15 `js/bpay-next-owner.js::create`: fixed bounded GET owner-status, active actor callback, one pending open read, scope/generation late-response rejection. Qualify a Source workspace open scope and discard it on close/logout/actor change; do not use an arbitrary cached/global boolean. `active_owner==='NEXT'` is the sole display condition. Unknown/DISABLED do not imply NEXT. On genuine retirement refusal show the plain text after qualification; do not display raw code. Preserve legacy actions when actual owner LEGACY, and do not send or trust owner/epoch input in original Source command payload.

The main Banking owner client is admin-scoped. Source may have its own legitimate Office authority: a display lookup failure must not block factual imports/finalise. Reuse the public actor-bound F15 transport without widening permission just to show retirement text. Joined D06 evidence must explicitly account for the actual permitted Source user roles and a lookup-unavailable result.

## Completion boundary

Source-only design is complete. No joined acceptance was executed. D01 source reuse is evidenced; D02–D06 implementation remains root-owned. Ordinary existing source units can verify exact admission-before-call/error mapping and factual source preservation, but this task authorizes no new test programme, scenarios, C1 fixtures or hosted actions. Future joined checks use genuine already-permitted Source receipts and actual F18 C1 inventory, not invented operations.

## Fresh raw source SHA-256

BE root: `C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/bpay-next-build-backend-20261009`.
FE root: corresponding `bpay-next-build-frontend-20261009`. Hashes are saved source bytes at this investigation, not deployed/installed verification.

| Tree | Physical source | Raw SHA-256 |
|---|---|---|
| BE | `supabase/repeatable/04102026_0550_banking_pay_next_legacy_writer_fence_v1.sql` | `de1314ab23759fe8b584e6757bdca49addb18267d060aab544d98fc015bb0145` |
| BE | `supabase/repeatable/04102026_0555_banking_pay_next_factual_legacy_projection_boundary_v1.sql` | `eba6b3596434aa6eecf133594579fb7833a7e3acdbe7d6fbd3676fbfb913db86` |
| BE | `supabase/repeatable/04102026_0556_banking_pay_next_source_factual_legacy_boundary_v1.sql` | `64ef7e6d04e1b652af722cb9dd050310fcd8e1cd249f59b9359428a310b70933` |
| BE | `supabase/repeatable/04102026_0557_banking_pay_next_source_owner_completion_v1.sql` | `81e031231480d67ac68c993984737f5f3342fd279629416ba670c8a95bed4276` |
| BE | `supabase/repeatable/15092026_2336_weekly_source_finalisation_pay_orchestration_v1.sql` | `2d0f9225dc4607932b2d731b453f07ce2665a5208ddc6ee764ddb8aa02488709` |
| BE | `supabase/repeatable/15092026_1534_weekly_source_finalisation_v1.sql` | `19018ea7be5e06a510fdd748409bcd84a18d0e31724daa19afb0a4471349a95b` |
| BE | `supabase/repeatable/15092026_1534_weekly_source_ordinary_pay_projection_v1.sql` | `665ddd27108a867f92e1f04dd346130736df27031a577f108b7f9c1bf6807900` |
| BE | `supabase/repeatable/15092026_1534_weekly_source_protected_pay_c1_publication_v1.sql` | `c74ea692273600ce753ee92c09efca5e812d8a61c4a536058c2a405c9be3f790` |
| BE | `supabase/repeatable/17092026_0100_weekly_source_banking_pay_absence_v1.sql` | `191cfbf4427c4dc4a35d50fcd679e6e09c06189b3f8561473f4bdc2f0c75c6a8` |
| BE | `supabase/repeatable/03102026_0500_h1h2_successor_authorities_after_retry.sql` | `2246174a1df3ffb20da053b548b2224fe8b67afcc6f0e4fe3fabf3b35f876b67` |
| BE | `broker/src/weekly-source/finalisation-pay-orchestrator.mjs` | `874a9cddebd25d69dfb2e312870c4463e0dd85b9839f5b82ae4ef5abd1db05a4` |
| BE | `broker/src/banking-pay/weekly-source-c1-raw-rpc.mjs` | `f290730871fdd1654b7d871a94712ddb4ad2376d58467e1a9773c1dd04719f74` |
| BE | `broker/src/banking-pay/weekly-source-c1-adapter.mjs` | `0af84a1c0e190de3813160cfb05a870488d511446818a89a3518bb65da5abc59` |
| BE | `broker/src/banking-pay/weekly-source-c1-durable-publication.mjs` | `cb39107459dd0e002234db92316ecb8a91a0b80f6874f1e2ee7275ab233d5e18` |
| BE | `broker/src/weekly-source/routes.js` | `606615afec53a10c6507f86cb0c39605c056d06e2a322a2addcf28645bacfa81` |
| BE | `broker/src/bpay-next-legacy-fence.js` | `afdbdda4ac440ada0a6267523a4a60398ae47feec85fcf8772f0fb2a2c6dfef8` |
| FE | `js/weekly-source/import-workspace.js` | `ad0952f73c3cb06ad1cb079f9858329948694ff72968b89243eee0128b491a27` |
| FE | `js/bpay-next-owner.js` | `4947d2e61cd4fa734cbaa99b05c415bad8552865d286233123cc48f34e1ca922` |
