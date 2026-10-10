# F-13 Workbench wake scope correction

Read-only source investigation, 10 October 2026. No product edits, SQL/DB calls, tests, runtime, fixtures, network or secrets. This supersedes the SESSION/CANDIDATE positive qualification proposed in `F-13-native-retirement-qualification.md`; that file is corrected too.

## Required decision

**Retain SESSION/CANDIDATE Workbench wakes until their full actual carried scope is positively qualified.** An OPEN/READY session and SEALED_CURRENT native certificate certify current preview readiness, not completion of the global coordinator invoked by the queued drain or all real operation/batch obligations associated with the session. CERTIFICATE_V8 remains separately qualifiable through its exact immutable certificate identity and current read-only status.

## Exact original and current scope semantics

1. Current `broker/src/index.js:40792–40812`, `scheduleBankingPayWorkbenchDrainWithDurableWake`, emits PRE_ENTRY_COMMITTED_WORK. It preserves session_id/candidate_id/actor_user_id, origin and profile, but **drops operationId/pay_batch_id/mutation_type/post_action_refresh** from its durable envelope. The same schema is used by CreateDraft (30492), PAYMENT_EXECUTE (43909), PAYMENT_SETTLE (44994), PAYMENT_CANCEL (183953) and payment correction (61585). Origin is diagnostic text, not a complete durable native operation identity.
2. Original RESET queue consumer `handleBankingPayContinuationQueue`, RESET index 42580–42600, calls `bankingPayWorkbenchCronTick` directly with the wake's session/Candidate filters and globalTailPass:true. The original cron passes those filters into `drainBankingPayWorkbenchJobs`; its direct body does not consume globalTailPass. Therefore **do not claim that this flag itself performs the immediate-nudge global tail on the queue branch**.
3. Original RESET `nudgeBankingPayWorkbenchDrain`, 19276–19314, separately allowed a global tail by default after a scoped nudge: it explicitly clears sessionId/session_id/candidateId/candidate_id and calls the cron again. Correction caller explicitly requests it. A READY scoped preview does not prove that broader immediate pass's work completed.
4. More decisively, both filtered and unfiltered drains call the existing public wrapper `pay_workbench_worker_drain_chunk_revalidated_v1(integer,timestamptz,uuid,uuid,text[],text,integer)` in `19072026_1405_revalidate_recovery_headroom_after_materialisation.sql:1519–1541`. Before forwarding p_session_id/p_candidate_id to the base drain, it calls **unfiltered** `pay_workbench_scope_reconcile_ensure_v1()` and `pay_workbench_scope_reconcile_drain_one_v1(25,worker)`.
5. The current registry's final `scope_reconcile_drain_one_v1(integer,text)` is `03102026_0500_h1h2_successor_authorities_after_retry.sql:17989`. At 18032–18040 it claims any due QUEUED WORKBENCH_SCOPE_RECONCILE job ordered by priority/run time, with **no session or Candidate filter**. Scope ensure's final physical body is 31072026_2350 and final root is 05092026_1200. Neither call receives this wake's identity.
6. Base `pay_workbench_worker_drain_chunk` final physical is `04082026_1219_pay_workbench_worker_drain_chunk.sql`, final root 05092026_1200. Its ordinary due-job filter and claim forward p_session_id/p_candidate_id (748–771). Its finite dispatch (1579–1663) is scope seed, Candidate source/delta, clone/rebase, line seed/process, preview materialisation and contract dirty fanout. **No direct banking_pay_operation claim/provider execution was found in this base drain.** Financial commands can produce the wake for post-action refresh; that is not evidence that the wake directly runs the financial continuation engine. The proven scope problem is its global preparation/coordinator adjunct and loss of the originating financial identity.

## Conservative native conditions before any future scoped qualifier

These are necessary conditions, not a new sufficient or fabricated completion protocol:

- Exact session and optional actor identity, real current preview/certificate readiness and raw count equalities as already documented.
- Complete bounded set of actual session operations: include `banking_pay_operations.workbench_session_id=exactSession`, all actual `banking_pay_operation_candidate_scope.workbench_session_id=exactSession` operation IDs, and their real root/children so an operation with null/different direct session is not lost. Overflow, incomplete pages or contradictory parent links retain. Do not select only the latest operation or one Candidate. Native operation fields are id/type/status/runner_state/requires_user_action/workbench_session_id/pay_batch_id/root_operation_id/completed_at_utc/lease_owner/lease_expires_at_utc/locked_by/lock_expires_at_utc and frozen scope completion fields already listed in the preceding note.
- Each actual relevant operation must positively meet its native completed obligations. COMPLETE/COMPLETE, no user action, actual completed timestamp and no live lease/lock are the minimal unused-Draft path; generic COMPLETE alone does not prove money kinds' settlement/remittance scope. Until all compound money obligations are actually qualified, retain.
- Read the complete actual operation candidate scope and every distinct native pay_batch_id; a single top-level batch does not cover multiple Candidate scopes. For the bounded conservative unused-Draft path, every actual bound batch must positively exist and be returned by F-18 LEGACY_BATCHES as OBSOLETE_BOOKKEEPING + blocking:false. Any actual execution/settlement/advice/active correction obligation retains.
- No QUEUED/RUNNING job in the exact complete session scope, with no Candidate-only narrowing; future run_at does not make queued work complete. FAILED/DEAD without positive native reconciliation also retains. Zero queued jobs is negative evidence only and cannot replace positive native completion.
- The queued wrapper's global coordinator still needs actual positive complete disposition. The wake does not name that coordinator job or its generation; one scoped certificate and a zero scoped job count cannot bind it. No existing identity-specific global completion was established here. **Therefore the above conditions do not currently close the gap: retain the wake.** Do not invent a digest, global journal, timestamp witness, universal retirement RPC or broaden F-13 to add a new money owner.

This is a safe exact classifier outcome under D02: known schema with unproved full scope is retained/retried, without an audit retirement receipt or ACK. It is not permission to fall back to the old engine. Root can implement the already positive exact CERTIFICATE and completed unused-Draft OPERATION paths independently.

## Source receipts

| Source read | SHA-256 |
| --- | --- |
| BUILD index at investigation | `57893c36c37e2ea69c01a3cc8d11dcbf35ed655f89bc258c3a4eb690a1ad1825` |
| RESET original index | `664dd138e70a5e43b387ca33f8a814d27b684ec57228a608217cfdfeb431cc65` |
| Existing revalidated wrapper 19072026_1405 | `aa6f1bfafd691536bb04a8c7bdfa12f075b1bdc31ee28b00f50c7160a36eb5ba` |
| Current final physical base drain 04082026_1219 | `e39eb1881dbc172bee73189b4c44cffd0dba8d80440f885c5e6e258f0e23afad` |

BUILD source is concurrently being edited by root; line numbers/hash identify the saved bytes actually read, not an assertion that a later edit is unchanged.
