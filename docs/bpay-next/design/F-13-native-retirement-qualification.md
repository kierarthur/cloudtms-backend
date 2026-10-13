# F-13 native retirement qualification

Read-only investigation, 10 October 2026. No product/schema edits, SQL/DB, tests, fixtures, runtime, network or secret operations. This supplements `F-13-current-scope.md` under the latest owner scope guard: **no invented digests, witnesses, disposition protocol or external barrier**. Its prior digest-packet proposal is not an implementation requirement. D02 may use the existing audit UUID PK and actual queue message ID for a small transport receipt, after positive native qualification.

**Scope correction after reading the actual original wake consumer and current final SQL wrapper:** SESSION/CANDIDATE READY plus a current sealed certificate is insufficient for retirement. The queued drain also runs a global scope coordinator before its filtered job claim. Immediate session nudges additionally allowed a global tail. The SESSION/CANDIDATE rows below and their section are corrected; see `F-13-wake-scope-correction.md` for exact source seams. CERTIFICATE_V8's exact certificate-only positive path is unchanged.

## Finite qualification map

| Exact legacy family | Positive existing path | Safe retained cases |
| --- | --- | --- |
| OPERATION SELF DRAFT_CREATE | Actual completed operation; exact complete native candidate-scope/batch relationships; each actual bound batch positively returned by F-18 as OBSOLETE_BOOKKEEPING, blocking:false. | Missing row, incomplete scope page, mismatch, active lease, unresolved money, nonterminal operation, or multi-batch proof that only covers one batch. |
| OPERATION ROOT/CHILD or money kinds | Real native provider/settlement/remittance/correction evidence and complete parent/child scope are necessary; F-18 provides existing positive predicates/read categories. | Until that complete compound evidence is verified, retain. Do not reuse the minimal unused-Draft rule or generic terminal strings. |
| CERTIFICATE_V8 | Existing read-only status RPC returns the exact requested session/actor and envelope certificate UUID with SEALED_CURRENT and certificate_ready_for_draft:true. | MISSING, BUILDING, BUILD_FAILED, REVOKED, superseded/current-generation mismatch, or another certificate UUID. |
| WORKBENCH WAKE SESSION | No sufficient existing identity-specific complete path established: session/certificate completion does not cover the queued drain's global coordinator. | Retain. Any future positive path must additionally cover the exact session's full native operation/batch obligations, all relevant jobs and the actual global adjunct; no guessed global completion is authorized. |
| WORKBENCH WAKE CANDIDATE with actual session | Exact Candidate READY rows remain useful evidence but do not restrict the global coordinator or prove the session's financial obligations complete. | Retain for the same scope gap as SESSION; one Candidate's completed rows cannot qualify the broader wake. |
| WORKBENCH WAKE CANDIDATE without session | No exact original job/build identity or universal Candidate completion receipt is carried by this envelope; the global coordinator is also unbound. | Retain. Never choose the latest arbitrary job/session or infer complete global scope from one Candidate. |
| WORKBENCH WAKE GLOBAL | No existing global drain completion row keyed by wake_id/previous_drain_invocation_id was found. | Retain. Empty categories, a completed unrelated job or one ready session do not qualify a global wake. No global witness is to be invented. |

All classifier outputs therefore have real behavior: positively qualifiable -> verified existing-audit receipt -> ACK; everything else -> bounded retry/preservation. Supporting no positive qualifier for a particular received identity is a legitimate retained outcome, not permission for a mock RETIRABLE result or a broader new framework. A clean launch requires no seeded old work.

## Exact completed Draft operation and batch relationships

Use a bounded exact read of `public.banking_pay_operations` selecting `id,operation_type,status,runner_state,requires_user_action,workbench_session_id,pay_batch_id,root_operation_id,completed_at_utc,lease_owner,lease_expires_at_utc,locked_by,lock_expires_at_utc,scope_freeze_status,source_scope_seed_complete,frozen_candidate_scope_count,frozen_selected_row_count,frozen_source_session_version,frozen_source_snapshot_run_id`.

The minimal route is SELF plus DRAFT_CREATE, exact envelope IDs/type, `status='COMPLETE'`, `runner_state='COMPLETE'`, `requires_user_action=false`, nonnull completed_at_utc, and no current lease/lock. Do not widen this route to FAILED/REVIEW_REQUIRED/CANCELLED or money kinds. Null envelope identities must agree with the actual row rather than be silently filled from another operation.

Final `public.banking_pay_operation_finish(uuid,text,jsonb,jsonb)` is `09082026_1128_banking_pay_operation_finish_post_draft_authority.sql`. For completed Drafts it positively validates FROZEN source scope, seed completion, accepted source session/snapshot, candidate/selection cardinalities, candidate-scope provenance into actual pay_batches, and COMPLETE CANDIDATE_SCOPE chunks with completed_count=unit_count and failed_count=0. At lines 412–435 it writes actual COMPLETE runner/status, completed timestamp and clears lease/lock/run_after. Do not invoke this writer from F-13; read its existing completed facts.

**A completed Draft can have multiple batches.** The owner validates `public.banking_pay_operation_candidate_scope` rows, each with its own pay_batch_id. The top-level operation pay_batch_id is not the complete batch universe. Read that exact operation's bounded candidate scope with `id,operation_id,candidate_id,pay_channel,pay_batch_id,workbench_session_id,source_session_version,source_snapshot_run_id,status`. Require complete row count equal to frozen_candidate_scope_count, nonnull actual batch IDs, accepted session/snapshot identities and ALLOCATED/DRAFTED scope state. Either restrict the initial path to a positively verified single distinct batch, or qualify every distinct batch in the complete bounded set. Overflow or incomplete rows retain the wake; do not ACK after proving only one batch.

For every real batch, use existing `public.bpay_next_cutover_readiness_page_v1(jsonb)` with LEGACY/LEGACY_BATCHES bounded native-ID pages. Require the returned item **actually has the exact batch ID**, classification OBSOLETE_BOOKKEEPING and blocking:false. A page that does not contain the ID, EMPTY summary, overall database_ready or missing row is insufficient. The native batch itself must exist and the operation/scope relationship must match. No replacement monetary predicate or new SQL owner is needed.

The existing positive unused-batch authority is `private.bpay_next_readiness_unused_batch_v1(uuid)` in 09102026_2109, lines 110–134, reached through that public category. It requires the actual row DRAFT/DRAFT_CREATED + NOT_SUBMITTED and no CSV/schedule/execution/commit/Monzo evidence; no authorised request/reserved or committed advance/settled Candidate/PAYE input/provider request or attempt/event/pending settlement/active correction/advice admission. At lines 161–164 the category positively labels this exact existing batch OBSOLETE_BOOKKEEPING. This is native read-only evidence; the private helper itself is not a service-callable RPC.

## Money operation evidence is compound

The same F-18 file supplies exact existing positive terminal predicates:

- `private.bpay_next_readiness_scope_terminal_v1(uuid)`, lines 35–65: actual transfer scope joins actual transfer by ID and batch; matching authenticated terminal event, provider request/transaction/time identity, actual COMPLETED/returned/reverted/no-money-failure disposition; rejects a later submitted/pending/processing/unknown event for that identity.
- `private.bpay_next_readiness_event_terminal_v1(uuid)`, lines 90–108: actual matched transfer event joins actual transfer, signature qualification, native terminal status/movement and no open/later uncertain event.
- Exact `banking_pay_operation_settlement_scope` fields are `id,operation_id,pay_batch_id,pay_batch_candidate_id,candidate_id,pay_channel,settlement_key,status,settlement_event_id`; pending/failed scope remains a genuine blocker. A terminal label without its real settlement event is not a new qualification rule.
- Exact `banking_pay_operation_remittance_scope` fields are `id,operation_id,pay_batch_id,pay_batch_candidate_id,candidate_id,recipient_kind,recipient_id,remittance_type,deterministic_outbox_key,status,outbox_id`. The scope-specific SENT/outbox evidence retained by F-18 must cover that exact scope, not one recipient's unrelated SENT row.

Public category pages and bounded exact table reads can expose those real typed facts, without calling provider/reconciliation writers. No existing public universal operation-disposition RPC was found. `deriveBankingPayContinuationDescriptor` deliberately wakes a PAYMENT_EXECUTE root after a terminal PAYMENT_SETTLEMENT/REMITTANCE_QUEUE child (index 40907–40910); child completion alone cannot retire the root's remaining work. Until a complete actual compound path is implemented, those envelopes are retained. This is not a reason to reopen the old operation engine.

## Certificate RPC is genuinely read-only and available under NEXT

The sole current saved definition of `public.pay_workbench_settled_certificate_status_v8(uuid,uuid)` is `02092026_2301_banking_pay_workbench_settled_certificate_build_v8.sql:2519–2640`. It is STABLE SECURITY DEFINER with empty search_path. Its only PERFORM calls set transaction-local timeout settings; the body is SELECT plus JSON construction. It calls no claim/build/append/seal/money routine. It joins the actual session/actor and current version/progress/fence certificate. Grants at 2875/2886/2896 retain service_role execution and revoke browser/PUBLIC roles.

0550 enforces BEFORE row/TRUNCATE writes on its exact legacy table list; SELECT is unaffected. Private certificate tables are not directly service-readable (2300 migration revokes service access), so this existing public read RPC is the correct path. The Worker `sbRpc` transport adds PREVIEW_PROGRESS/deadline headers and does not introduce a legacy-writer check for a fixed internal read (index 164836 onward). The existing wrapper at 16647 uses this exact RPC with p_workbench_session_id/p_actor_user_id and timeout 6000. HTTP legacy route retirement does not make the internal service read a financial writer.

Current `final-definer-closures.json` has no dedicated chain entry for this unchanged status routine. The saved SQL census found one definition and no later successor; its physical source is an existing release input. Preserve it; do not invent an alternate definer/body or claim a chain entry exists. Any dependency/source receipt should name this exact physical reader and the existing 2300 schema, not a parked F-25 file.

Exact reply contains 21 keys:

```text
ok, workbench_session_id, session_version, progress_counter_version,
authority_fence_generation, selected_constituent_count, session_current_ready,
certificate_lifecycle, certificate_uuid, certificate_contract, certification_id,
overall_digest_sha256, certificate_ready_for_draft, certificate_build_pending,
certificate_continue_polling, certificate_recovery_required,
certificate_error_code, certificate_error_message, maximum_selected_constituents,
no_candidate_locks, read_only
```

UUIDs are JSON strings (certificate_uuid nullable); version/progress/fence/count/max are JSON numbers; flags are booleans; lifecycle/contract are text; certification/digest/error fields are nullable strings. Strict parsing must reject extra/missing/duplicate or wrong types, unsafe integers and ID mismatch. Positive certificate qualification requires actual `ok:true`, exact session and envelope certificate UUID, current-ready true, lifecycle SEALED_CURRENT, contract WORKBENCH_SETTLED_CERTIFICATION_V2, ready-for-draft true, build/continue/recovery false, no_candidate_locks/read_only true, and the **existing** certification identity consistency. This reuses native certificate identity; it does not create a new digest. Selected count is 1..50000; positive versions/fence are required.

The original 2300 certificate schema binds FK workbench_session_id and positive session/progress/fence; its constraints equate scope total/seeded/ready, line total/ready and selected/eligible/constituent counts. SEALED_CURRENT requires existing certification identity and nonnull sealed_at. BUILDING and failed states cannot satisfy completion. Status chooses only the current session version/progress/fence certificate; an old envelope whose UUID differs must be retained rather than adopting the newer UUID.

## Session/Candidate evidence is necessary but insufficient

For a SESSION wake, actual `banking_pay_workbench_sessions` by exact session_id (limit 2, require one) exposes `id,actor_user_id,status,version,progress_state,progress_counter_version,authority_fence_generation,discarded_at_utc,replacement_session_id,scope_seed_complete,scope_total_count,scope_seeded_count,scope_ready_count,scope_pending_count,scope_failed_count,line_units_total,line_units_ready,line_units_pending,line_units_failed`. Actor match, OPEN/READY, undiscarded/unreplaced, seed completion, exact count equalities, pending/failed zero and a positive current certificate establish only current preview/certificate readiness. **They do not establish the complete native work carried by this wake and cannot authorize a D02 retirement receipt or ACK.**

For a CANDIDATE wake with that exact session, `banking_pay_workbench_session_scope` by session_id+candidate_id exposes `id,session_id,candidate_id,status,seeded,dirty,pending_job_id`; matching `banking_pay_workbench_session_candidate_state` exposes `id,session_id,candidate_id,status,session_version,pending_job_id,last_recomputed_at_utc`. READY, seeded:true, dirty:false, exact session version and real recompute completion do not prove the full session's obligations or the global coordinator completed. Missing/multiple/mismatched rows also retain. Do not read financial fragments or choose another Candidate/session's completion.

An existing reader `public.pay_workbench_session_get_progress_light(uuid)` is defined only in `26052026_2100HRS_NEW_FUNCTIONS.sql:149708–150546`. It performs native reads and timeout setup, then appends `pay_workbench_scope_progress_v1(uuid)` (final body in 31072026_2350:2829–2928, with current-generation/blocker readers). It exposes useful READY/pending/recovery/count fields but is not a global completion owner. Its `read_only` reply field means a UI obsolete/terminal-failure state, **not whether SQL executed writes**; normal ready replies may have read_only:false. Its ready flag does not independently require raw total=seeded=ready and uses clamped counters. Do not use ready, zero active jobs, its synthetic line-complete count or even a current sealed certificate alone as an ACK proof.

Candidate-only and GLOBAL wakes have no original session/job/certificate generation. A completed latest job cannot prove that the exact old wake's full scope completed, and a global timestamp is not a completion receipt. F-18's all-existing Workbench jobs/sessions OBsolete classifications are census evidence, not an identity-specific global completion. Those messages remain bounded-retry/reconciliation outcomes until positively matched real facts exist. No new global witness, digest, journal or drain framework is authorized.

## Small D02 support

Use the actual stable bounded hexadecimal `message.id` as the receipt correlation identity. Its deterministic audit UUID may derive from those existing ID bytes with a fixed domain/version convention; compare exact correlation ID, family, native IDs and bounded reason on an existing-row collision before ACK. Do not add an envelope/request digest or a financial snapshot. Existing audit PK gives one actual receipt row. Audit INSERT/read-back must be awaited and verified; generic best-effort writeAudit does not suffice. A lost insert reply retries the same identity; missing/mismatching receipt never retires the delivery. The once-only audit event is the durable log; console output is supplementary.

## Source receipts

| Physical source | SHA-256 |
| --- | --- |
| current index | `29cfa90d56aa10b6d86c50c687445a4649c876d3799d64a7287b8a6162d623ec` |
| 02092026_2301 certificate build/status | `081e490c359080ba296f06548a845100ee242cf5fafb10dd50d2aaa77fc079cf` |
| 02092026_2300 certificate schema | `726e55525a9602903c72f683c61c08380b54e495fdbe8c664806691154f34723` |
| 09082026_1128 operation finish | `d90ee3bd93f2da774046980e68a808dcb0c98e7bbf4cb25b6f2010d8a391cd1d` |
| 26052026_2100HRS original progress reader | `8b3cb3e112ae227a80bf2e661272264c3d6145d0e2ff6ad8d88e8eee2db1553f` |
| 09102026_2109 F-18 census | `980a79151e8cc49f3db8c5c4e1f8cd99aac6039600405e25ec1803d3b7c91add` |
