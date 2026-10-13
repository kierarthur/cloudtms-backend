# WB-19 retry investigation

Read-only current source investigation,10 October2026. No product edits, SQL/DB/runtime/tests/app/fixtures/network/hosted operations. Rule B remains closed; no parked2153/resolver, producers, new financial command kind or legacy job import is proposed.

## Controlling scope

Read full card `02_WORKBENCH.md:749–785`, W2/W8/W10/W12–14 at38/44/46/49–51, `review/R2.md:130–132,216–218`, master seq67 and all20 child rows+ITEM in BUILD-BE checklist. Depends WB09/12/13/14/15/16/F16. Required new/delta rows **D06,D11,D12,D13,D15**; D01–05,D07–10,D14,D16–20 are context/reference reuse. Their old aggregate remaining prose does not authorize unrelated rewrites now. Later W2 action owners and WB34 bounded completion adoption remain separate actual joins.

R2-F08 requires an exact administrative request receipt: definite failed retry becomes claimable READY, resets lease/due invariants, retains phase/cursor/attempt/original error, and wakes existing adapter. UNKNOWN outcome requires receipt/native cursor readback before identical retry. Unsupported contract/epoch uses actual bounded canonical repair/refusal, never guessed financial inputs. Changed projection notice requires an actual read/event/receipt, not passive knowledge.

## Current exact final owners

| Object | Final physical/root |
|---|---|
| `private.bpay_next_apply_position_page_v1(uuid,uuid,bigint,text,text,integer)` | `supabase/repeatable/10102026_1346_banking_pay_next_position_projection_boundary_v1.sql` |
| `private.bpay_next_claim_position_job_v1(uuid,integer)` | `supabase/repeatable/03102026_1430_banking_pay_next_position_writer_v1.sql` |
| `public.bpay_next_enroll_service_v1()` | `supabase/repeatable/04102026_0560_banking_pay_next_queue_recovery_service_v1.sql` |
| `public.bpay_next_job_wake_page_v1(text,integer)` | `supabase/repeatable/04102026_0560_banking_pay_next_queue_recovery_service_v1.sql` |
| `public.bpay_next_financial_job_v1(text,uuid,text,jsonb)` | `supabase/repeatable/04102026_0280_banking_pay_next_financial_job_service_v1.sql` |
| `public.bpay_next_financial_intake_v1(uuid,text,jsonb)` | `supabase/repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql` |

No registered/current `public.bpay_next_workbench_command_v1(jsonb)` exists. Private common/public reader currently use2128; original ten-kind dispatch remains. Rule-B/WB21 Create/choices/PIN authority is not retry input to recompute.

## Required deliverable map

| Row | Current evidence | Exact remaining |
|---|---|---|
| D06 intake |2018:3044–3324 service-only active admin+NEXT;12 fixed actions,2048byte args, typed native receipt/120000byte output. No RETRY_WORK_JOB. | Minimal finite retry/control dispatch support retaining every existing action and native receipt. One authoritative implementation shared by new wrapper, no money alias/reset. |
| D11 command | Absent. | New closed service-only NEXT/admin W2 owner; stored request_id/body identity receipt with changed-body reuse refusal, initial RETRY_WORK_JOB and genuine readback separation. |
| D12 route | index198881–198903 only `/next/command`; fence162–179 has exact existing exemptions but no workbench-command. | Exact POST route/exemption, authenticated/environment guard, actor injection, bounds/deadline/closed actions, typed known refusal vs unknown; existing IDs-only wake after durable acceptance. |
| D13 UI | Existing shell/read failure/reload/adoptAffected, Office exact mutation journal; no job Retry controls or receipt/status. | Native Processing/Failed/Retry, last/first causal code and actual timestamps; exact request journal/readback; affected Candidate+HEADER adoption retaining page/scroll/filter/ticks, no long polling or financial auto-retry. |
| D15 adapter | command-rpc:4–35,13 fixed actions (12 intake+stored-credit dedicated), fixed endpoints/actor/signal/redirect:error; no retry. | Extend only necessary finite retry dispatch to exact owner/args. No arbitrary RPC, body money or replacement IDs. |

## Actual native retry behavior and lock seam

- Position claim1430:397–451 discovers exact Candidate, module NEXT/epoch413–417, then Candidate worker_control UPDATE422–423 **before** job UPDATE424–425. READY/expired LEASED only426–429; active lease never adopted. Earlier same-Candidate command pending431–435 refuses. Native claim increments owner epoch and makes a genuine new nonce437–445. FAILED cannot claim.
- Wake0560:54–82 discovers READY/BLOCKED or expired LEASED, bounded100 indexed IDs; rotates only queue_wake_after. Queue schema0502:10–15 has exact READY/BLOCKED and expired-LEASED partial indexes. FAILED+new due time remains invisible.
- Final apply1346:238 onward retains exact module/Candidate/job/publication/work authority and100row clamp. DONE replay276–280 requires actual job DONE/phaseDONE and matching APPLIED publication/command/Candidate. Native phase/cursor mismatch returns current progress. Preserve original publication/revision, phase/cursor, applied_line_count and economic uniqueness; never restart from guessed inputs.
- Financial_job0280:45–78 has CLAIM/STEP,13 kinds,2048byte args; fresh classifier258–305 returns actual terminal/admissibility/predecessor/expired-lease facts. It is not a FAILED repair owner.
- Job-step328–355 already distinguishes known rollback40001/40P01/55P03/57014 from unknown CLAIM/STEP. Lost CLAIM waits lease expiry; lost STEP keeps exact job/kind/nonce/epoch/phase/cursor. Financial-queue179–206 preserves the original message and awaits continuation before ACK. Reuse these rather than reset normal transaction/transport failures.
- Actual index factory40998–41013 supplies runtime/expiry/job adapters and queue. `onFailure` logs closed code/recovery and returns RETRY; it does **not** durably record failed status/error. Financial-queue91–102 may ACK only after durable failure recording, which this factory does not provide. Failed UI cannot be inferred from log messages. Background127–166 already recovers ENROLL,READY/expired,EXPIRY,CLEANUP and WB44 DUE/current tasks; no new drain protocol needed.

## Minimal retry authority recommendation

This is a design recommendation, not authored product contract.

1. Bind exact request_id/authenticated actor/Candidate/job/module epoch and observed kind/phase/cursor. Store body identity and immutable receipt; same request_id with changed body refuses. Accepted readback uses the original receipt, not today's revaluation.
2. Preserve module→Candidate worker_control→job lock order and re-prove exact command_member/command_sequence/publication/work/revision. If a supporting projection barrier needs Work/Case parents, retain existing Candidate/business→projection order, never late business locks or job-first invocation of Candidate-first owner.
3. Proved definite compatible POSITION_APPLY FAILED →READY, clear both lease fields, make available/wake due now. Preserve phase/cursor/counts/input revision/attempts/error; return no nonce. Existing genuine CLAIM creates next authority and obeys predecessors. Active leased or outcome-unknown work must not be reset. DONE readback is truthful terminal evidence, not READY.
4. Keep the original command sequence; administrative scheduling is not a new financial obligation/command. Wrong epoch/phase/missing immutable input refuses repair-needed/unavailable with retained identity; do not copy today's module_epoch or current revision. Actual canonical old-contract repair needs original accepted shape+current rejection+real owner evidence first.
5. Store receipt before return: exact affected Candidate/work/job/original command+sequence, native resulting status/progress and actual WB/adoption revision when available. Post-commit wake uses existing CLAIM/recovery IDs. Two Retry clicks/lost reply retain one exact request; failed queue send does not invalidate a durable result.

## Required factual reader/error support

The card Build768–770 explicitly requires these small supporters despite context reader rows:

- Job core1324:967–991 has last_error_code only, attempt_count,phase/cursor and lease identity. There is no original/first causal error field. Current group progress_state/error_code is not native job provenance.2128 item JSON and Worker/FE mirrors have no processing/failed_job_code/job_id; HEADER has no oldest_pending_age_seconds. Add only exact existing owner/mirror metadata, no new money/classifier/producer/read kind.
-0352 lifecycle migration records created/start/finish and explicitly leaves historical clocks NULL. Age/first-error history must remain unknown for missing evidence, not backfilled. Exact oldest pending selection needs a suitable index or small additive partial index, not an opening agency scan.
- Public reader2128:797–799 refuses while global adoption/dirty/pending is noncurrent. Adding row metadata below it alone is unreachable during real Processing. Preserve last figures greyed; expose a bounded exact affected-job status/readback through a finite existing refusal or W2 read branch. It must not declare dirty financial totals CURRENT or offer an intermediate page Ready. Do not remove the global readiness barrier.
- First and recent errors require genuine durable failure evidence. Current log-only onFailure is insufficient. A smallest exact recorder, if needed, must bind real job/nonce/epoch/phase/cursor and known rolled-back owner error, distinguish first-known cause from last attempt, and preserve unknown outcome semantics. A legacy row with only last_error cannot be described as storing original first cause. This is necessary card support, not a broad failure framework.

## Compatibility and acceptance boundary

Saved `design/WB-44-compatibility-closure-investigation.md:29–55` proves current native transaction/cursor/lease/indexed recovery and specifically leaves future WB19 administrative retry open. F03/F05/F06 do not prove absent old business obligations. No specific formerly valid old NEXT instruction is evidenced as broken here; do not manufacture a generic old-job repair or port legacy job families. A concrete persisted old contract remains retained until exact canonical repair/refusal evidence exists.

Own retry can adopt affected Candidate+HEADER while preserving actual navigation now; full pending-to-complete adoption is WB34's later join if not already implemented. Snooze page3 journey uses its actual named control owner later, not new snooze in WB19. Actual hosted queue/consumer/minute/app/timing and Failed→Ready journey remain NOT_RUN. No testing programme is proposed.

## Snapshot hashes

| Source | Raw SHA-256 |
|---|---|
| `supabase/repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql` | `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b` |
| `supabase/repeatable/03102026_1430_banking_pay_next_position_writer_v1.sql` | `395ead5cf46a1399ef4859f056d89508b0862f9b0fda8dde92454107f17602fa` |
| `supabase/repeatable/10102026_1346_banking_pay_next_position_projection_boundary_v1.sql` | `8934065d152ab69bbe2ec8ee298bd03b66f4c20a82fbbcdced78567db37bb69f` |
| `supabase/repeatable/04102026_0560_banking_pay_next_queue_recovery_service_v1.sql` | `03daf223a8b572b37605f1ddc15fb777a3a543ff89140b8cff40238ec9a139fa` |
| `supabase/repeatable/04102026_0280_banking_pay_next_financial_job_service_v1.sql` | `6fdd30c7b53b4e2d18d048aca46fbaadc7733a3cd5edefe4773f11b50288334d` |
| `supabase/repeatable/10102026_2128_banking_pay_next_workbench_export_v1.sql` | `92e5021fd277fb71c07947fe33b1ad46e73838313b5f8bc1e31964219a5c55e4` |
| `broker/src/bpay-next-command-rpc.js` | `060ce9bdab50576aec731d0b9b3c6499a608e3cd71c17a3c9bdf54483db1b68e` |
| `broker/src/bpay-next-command.js` | `95f082f6fef61932b0498ea007d47966692ec15094d954fc9cbbf4bd1d7ed059` |
| `broker/src/bpay-next-workbench.js` | `c9b5d2c6116ff8ce12c4b294344989fc829f04be6335adb5d795178263c9f87d` |
| `broker/src/bpay-next-job-step.js` | `53e2f466929e6640de7be4823aebf4979b81510c3de38931d198897e2de2c810` |
| `broker/src/bpay-next-background.js` | `e7037e62639d38c9311db907f8cbd7d5d3de9366564addfc416f79a353617572` |
| `broker/src/bpay-next-financial-queue.js` | `59bb0fc90f37d3e211f9bcdbf9a17ee3f1ecbfbd6e6d3b94197cf2369ba38e20` |
| `BUILD-FE/js/bpay-next-office.js` | `62b5df74a6307cccb5eb8e47dee9b89648ff913030dc30772e26809139a40a82` |
| `BUILD-FE/js/bpay-next-workbench.js` | `d03d7da007c80602449795d9c4c34111c20909f040c2446bcad1b722bda4b684` |

## Minimum per-item progress and failure-recorder supplement

Fresh source seams read after the initial save: publication schema1324:934–955 and work-order index1069–1070; final apply1346:265–306,341–365,394–415; queue failureSummary71–79 and failure91–102; Worker REFUSAL401/validate492; actual index40998–41013. No execution.

### Reader continuation without stale Ready

Preferred finite support preserves the current public2128 noncurrent refusal rather than returning successful stale financial rows. Add one **metadata-only progress subpage** to that refusal/HEADER path, using an own typed continuation. It is a supporting branch of the existing own reader, not a new financial owner/read kind, and no financial tables are mutated. Normal CURRENT financial page results retain original validation and amount authority.

- First seed only ≤limit+1 native POSITION_APPLY job identities under normalized Candidate/Client/channel scope, ordered by native immutable `(command_sequence,id)`. Add the smallest appropriate partial seek index if the actual current job indexes cannot support it. Do not do an unbounded window ordinal, all-job JSON aggregate or Candidate-history walk.
- Join exact `job.command_id/candidate_id`→publication PK and Candidate→Work/immutable revision, command_member and command `(id,agency_sequence,module_epoch,command_kind='POSITION_APPLY')`. Keep all same-scope non-DONE obligations; filtering only Work.current_revision hides a genuine older blocker while newer commands wait. Exact requested Candidate/Work reads can use publication `(work_id,revision_no)` and job unique `(command_id,candidate_id,job_kind)` indexes.
- Return finite native metadata: Candidate/work/revision/job/command IDs, command_sequence, module_epoch, job_status, phase, cursor, attempt_count, first_error_code and last_error_code (nullable actual first fact), actual created/start/finish timestamps and recorded retry request receipt when requested. Display names may come from exact immutable revision/actual metadata under existing text/byte limits. **No amounts, payable/selectable, synthetic READY or inferred financial progress percentage.** An unsupported/missing original scope is an explicit evidence-unavailable row/refusal, not a dropped job.
- Cursor uses typed sequence+UUID, exact agency/actor/module/scope/actual projection barrier identity and a distinct progress discriminator. Never treat original money-page cursor as progress, or retag it after a version change. At most100 emitted rows; limit+1 and120000byte envelope still apply. Empty progress does not mean global projection CURRENT: metadata seed/dirty task may still be pending without a POSITION_APPLY job.
- Existing Worker REFUSAL401 and validate492 are closed, and FE also throws a typed noncurrent read error. Extend these exact decoders/error object fields so the metadata page reaches the screen rather than being stripped/rejected. Keep previously read figures grey and Create/selection disabled. Processing/Failed controls live in the separate metadata status view; metadata success must never call ordinary financial-page adoption or enable stale Ready.
- For read-only continuation, expose a progress cursor accepted only by this metadata branch. Current HEADER after is generally forbidden and normal HEADER is not a list; its narrowly conditional continuation must be defined explicitly in the public validator. An exact W2 READ_REQUEST/JOB_STATUS branch is a viable alternative for already known work/job IDs, but by itself it does not satisfy initial opening while dirty with no earlier cached page. Do not add a new agency scan or long poll to compensate.
- HEADER oldest_pending_age_seconds uses genuine created_at for actual READY/LEASED/BLOCKED obligations, DB clock and exact scope. Historical NULL creation evidence makes true oldest age unavailable; a minimum of known timestamps cannot be labelled oldest when an unknown historical pending row may be older. An appropriate partial oldest/unknown probe or existing maintained summary is required; ordinary open must not scan all agency jobs. FAILED age is separate failure state. Do not use queue rotation time as original creation.

### Definite failure recording without changing0280

A small service-only recorder called by the existing queue callback is genuine support for D13's actual failure/error metadata. It need not rewrite0280, introduce a command kind, enrollment lane, producer or new financial protocol.

Current failureSummary71–79 is intentionally token-free: message ID/attempts/action/job/kind/error/retryable/unknown/recovery only. That is safe to log but insufficient mutation authority. To record a definite failure safely, pass a separate closed **internal** evidence payload from the already retained original STEP call: exact job/kind/nonce/owner epoch/expected phase/cursor plus bounded original DB refusal code. Do not log nonce/payload. Recorder module→Candidate→job locks and rechecks all identity/checkpoint fields; it never takes job lock first then Candidate.

- Only a genuinely dispatched POSITION_APPLY STEP with an explicit structured, nonretryable, transaction-refused DB result qualifies. Known rollback transient40001/40P01/55P03/57014 remains automatic native transaction retry, not Failed. CLAIM failures, service/auth/request transport failures, active-lease/predecessor waits, unknown network/deadline/parse/oversize replies and invalid queue envelopes must never mark a job FAILED.
- If actual job advanced phase/cursor, was reclaimed by another nonce/epoch, completed, or module changed, return factual `recorded=false/stale` without mutation and preserve original queue retry/readback. A generic delivery refusal is not proof that the economic STEP failed. Idempotent identical failure recording may replay its existing stored failure evidence but cannot overwrite first cause.
- Under exact same lease/checkpoint, record original first-known causal code once and most recent actual code separately, set definite FAILED and clear both leases. Preserve cursor/counts/publication and all effects. Projection remains dirty/Processing-or-Failed, never payable. A durable record supports ACK_RECORDED only after the callback has actually received that durable result; otherwise existing retry remains.
- Current dependencyError328–355 collapses some original DB messages into JOB_NOT_ELIGIBLE/JOB_CONFLICT; keep a finite original safe cause internally before normalization if exact first causal diagnostics are required. Do not expand returned arbitrary SQL/log text or raw business error data. An earlier row with only last_error_code retains first_error unknown.

### Canonical publication twin repair

Current full owner updates publication+job phase/cursor together at1346:349–362 and411–414. Their exact retained checkpoint twin is a legitimate finite compatibility authority; guessed latest approval is not. The existing publication's command/work/revision/Candidate and predecessor join, native NEW/REMOVED/DONE phase, cursor and retained input preserve economics. Repair only an evidenced old job contract whose native publication checkpoint is genuinely valid, retaining applied_line_count and all first/last errors. If those counts/inputs cannot be proved compatible, refuse repair-needed instead of resetting to zero. An APPLIED publication/DONE job pair is terminal readback; do not force READY or re-run money. Do not copy current module epoch. This is the exact bounded administrative support described by R2-F08, not a new finance pipeline.
