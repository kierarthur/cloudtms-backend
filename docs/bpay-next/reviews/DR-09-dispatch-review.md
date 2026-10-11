# DR-09 enrollment and financial dispatcher source review

11 October2026. Independent read-only review of unloaded `supabase/repeatable/11102026_0814_banking_pay_next_preparation_discard_v1.sql`, its additive private binding migration and the current parent definitions. Focus: original partial retirement, native ordering, separately typed retired PREPARE receipts, own discard lane, mixed results and retained pointers. Only this note is written. No product edits, database/SQL execution, tests, fixtures, hosted actions or commits. Root is still completing progress/ACL/triggers/Worker closed maps. **Essential findings remain; no source/item/runtime PASS.** Application acceptance **NOT_RUN**.

1. **Command/run blocking inversion during partial enrollment.** New admission0814:61/99 owns run UPDATE before original PREPARE command UPDATE; enroll_one1602–1605 owns the enrollment clock/next command before blocking on run SHARE1707–1708. A concurrent partially ENROLLING original command can therefore wait for run while discard admission waits for that command. The parent expiry body had the same blocking pattern, but this new operation must not introduce another unbounded command→run wait. Narrow supporting fix: finite NOWAIT/backoff at the existing original ENROLLING run SHARE, as already used by enroll_worker_action1990; rollback preserves original counts/clock. Keep RECEIVED/zero-retired plain run reads1650–1705 and normal order.

2. **Later due expiry cannot exclude newly discard-removed members of a PARTIAL run.** finish_member40–42 restores PARTIAL to REVIEW, while normal removed members reach worker CANCELLED495 and increment native cancellation counts523–527. Current expiry enrolment0559:6506–6519 recognises only the exact completed DR05 worker RELEASE via worker_release_complete and otherwise refuses a CANCELLED worker. A later72h expiry of PARTIAL therefore stalls on a member already removed by the new operation. Required narrow support is an exact completed native discard-member exclusion in the current expiry enrollment/binding guard: own request/member/job/worker/economic/current revision/count tuple, REMOVED/ALREADY_REMOVED only, zero new holds/releases and no second parent count. Do not relax the original due/72h fence or treat a BLOCKED member as released.

3. **Retired PREPARE replay loses its own terminal proof after a later cleanup revision.** The discard fence0814:15 requires root review_revision=accepted+1. A later expiry increments that revision, so public financial2637–2641 no longer chooses the retained discard receipt. The current expired_prepare_receipt0559:6747 also excludes a native old job phaseDISCARDED. Preserve the exact terminal discard receipt for its retired original/active jobs after subsequent ordered cleanup, using immutable native discard membership/provenance; do not restart capture, adopt a lease or relabel the old receipt as expiry. This is specifically the original unknown/replay boundary, not a new retry protocol.

The root independently identified the header CANCELLED invariant firing too early; that pending correction is not duplicated as a new reviewer finding. Remaining focused checks are underway in the actual source. Policy X: no financial drift proposed.

Root's selected correction for findings2/3 is narrower than the initially suggested expiry exclusion bridge: first accepted discard owns the root, including a terminal PARTIAL result. Exclude any exact existing discard request from the native due0520 reader; explicit expiry admission must refuse `BPAY_NEXT_EXPIRY_DISCARD_ALREADY_BOUND` under the same run lock. Confirm must also refuse an existing discard request, preserving blocked survivors as unresolved with their original figures/holds/counters and keeping old financial jobs inert. This is consistent with card257's same-header first-commit exclusion. It needs no completed-discard expiry release/revision crossover, no fake EXPIRED receipt and no new repair policy. **Architecture resolution accepted; source correction still pending inspection.** The initial findings above remain recorded as the uncorrected0814 behavior.

Current-source amendment (0814 SHA-256 `42470BBAF3C27E160F147BF1F107845C354D06E417B651F824AE38F9C1208C52`): original ENROLLING run SHARE is now NOWAIT1724, closing finding1 without count/clock changes. Confirm2896–2897 refuses the exact bound discard before readiness; due3262 excludes existing discard requests; expiry admission3317–3318 refuses the distinct closed55000 code under its original header lock. These observed branches close findings2/3 by the selected first-discard-wins architecture. The original expiry due/deadline equality/72h guards remain in the copied admission, and no expiry release bridge was introduced. The header's intermediate CANCELLING count check now tolerates the same-transaction member/count update before the request's completed counter, while terminal PARTIAL/DISCARDED retains exact native count equality1367–1372.

4. **Remaining finite-work concern in the due exclusion.** The due query3262 anti-joins discard requests before LIMIT, but its current native index0501:23–24 still includes every terminal PARTIAL REVIEW run. A growing prefix of permanently excluded PARTIAL runs can therefore be scanned on every minute before finding the next at-most100 returned eligible runs. If a strict visited-row bound is required, retain native indexed eligibility with an exact discard-qualified run pointer/flag and matching partial index, or take a finite native due slice before filtering and advance the existing sweep across skipped rows. The latter requires precise support because current0560:115–126 advances only from returned items and ignores the page cursor. This concern does not justify a new route/cursor protocol or inventory sweep. Root is assessing the smallest required support.

Observed dispatcher/enrollment behavior otherwise matches the narrow design: original partial members become metadata-only DONE/DISCARDED; original and exact active PREPARE checkpoints/cursors remain preserved; accepted-but-not-enrolled retry requests retire through their own ordered receipt; discard claims obtain a fresh Candidate owner epoch/nonce and refuse earlier open Candidate commands. The public financial wrapper checks the committed native phase/checkpoint and yields only that page's own lease. DONE replay returns zero work/releases; blocked outcomes arise only from the explicit protected money/state predicates, preserve original figures and do not reinterpret structural/lease errors as BLOCKED. Whole-run release retains worker economic/current execution identities and active pointer, and calls the existing WORK/CASE mutation/weekly projection ownership rather than capturing or repricing inputs. Original selection projection and pending native ORIGIN waits stay bounded as reviewed in DR08.

Pending Worker-map reminder: the separate retired-discard receipt returns the actual current protected worker status, potentially DRAFT/ISSUED/COMPLETE for a BLOCKED member. Its separate closed schema must reflect the actual native status domain while retaining strict done/replay/zero-visit/nullability; the existing expired receipt's smaller status subset must not weaken ordinary PREPARE or fabricate a CANCELLED worker. Worker/progress/ACL/trigger finalization and compile/application behavior remain outside this saved source snapshot and **NOT_RUN** by this reviewer.

## Requested follow-up: running per-Candidate Remove through WORKER_RELEASE

The existing action is reusable **once a native run_worker exists**. Its current command/input/job/revision shape already supports a frozen partial prefix: request migration0559 permits original_worker_statusPREPARING, retains both economic/current execution revision, original/previous job identities, exact captured line/active Case hold counts and optional Case state. The worker-execution guard6771–6793 already allows the same Office accepted RELEASE revision/pointer transition. No new action, job kind, request table, money formula or execution-guard exemption is required for that existing-worker scope. Selected but not-yet-created workers cannot be represented by the current run_worker_id action; do not invent their UUIDs or claim that extension is covered.

Exact required successors, preserving REPREPARE and the six-code SYSTEM branch:

1. **Admission0559:96–119:** add an Office RELEASE-only branch for a native PREPARING worker and its exact active PREPARE job in READY/LEASED/BLOCKED; include READY/REVIEW workers whose original projection job is still open if the same protected-money guards pass. Under the existing run→Candidate→job→worker locks, prove original command/member/input job, module epoch, worker current/economic revision and zero realised/NET/transfer protection; then retire only that exact old job to DONE/DISCARDED, clear its lease and preserve its committed phase/cursors/counts. It is deliberate retirement, not adoption of a lease. Same-command replay stays before fresh eligibility. Refuse a different already-pending worker action and any root discard/expiry winner.
2. **Partial original enrollment0559:101–103:** a RELEASE-only exception may admit original PREPARE ENROLLING with expected count equal to immutable run.selected_candidate_count and this Candidate already enrolled under its exact command_member/input job. Do not require every other Candidate enrolled; do not advance or seal that original command. The normal agency enroller still finishes original members before the later ordered cleanup command and other Candidates continue independently. Keep the original SEALED/full-count requirement for REPREPARE/SYSTEM unless their existing branch applies.
3. **Maintained counters0559:141–144:** Office admission currently adds1 to preparing regardless of original status. For original_worker_statusPREPARING, add0 and retain its existing preparing count; for READY/REVIEW preserve the original +1 and ready/review decrement. Native cleanup final526–527 then decrements preparing once and increments removed once. Otherwise an accepted partial removal leaves a phantom preparing Candidate after completion.
4. **Retired financial reply:** normal PREPARE CANCELLED output cannot represent the old job while its worker is CANCELLING, and the current completed-release replay requires request.job_id=p_job_id. Add the exact Office RELEASE previous_job_id/input provenance alternative to the new typed retired-discard receipt/public PREPARE dispatch branch. Prove the original/current worker/request/command/module/revisions and old job DONE/DISCARDED; return retained cursors, done/replay, zero visited rows and actual nullable worker status through the separate DISCARDED schema. Never use a root discard or expiry receipt for this per-Candidate action. Its newly enrolled PREPARE cleanup job and existing final CANCELLED receipt remain the normal DR05 path.
5. **Required release/projection safety:** existing prepare_projection_page0559:2458–2478 already walks original run_selection and Case selection, so it covers uncaptured original suffixes without a new manifest. Existing worker_release_page505–511, however, invokes the Case helper before actual collection Work prelocks and refreshes only Case IDs; its FVR bump539 follows refresh. For this running Remove scope, apply the exact DR08 bounded Work-before-Case collection-origin prelock and bump-before-refresh pattern to that named release owner. Project genuine selected Case component→collection Work origins as well as original Work. Before the request becomes COMPLETE2488–2492, await any exact native ORIGIN tasks scheduled by those bounded pages using the retained current-tuple pattern from0746. Any added continuation is only the bounded pending Work IDs on the existing worker-action request, not a new queue/proof framework. Admission retirement alone does not prove WB08 summary completion.

## Requested follow-up: bounded due traversal without a run flag

Reuse the existing due index0501:23–24 and existing due/sweep cursor owners. Public due0520 can take LIMIT p_limit original deadline/id rows **before** checking discard membership, skip bound rows within that finite slice, and retain its existing internal page cursor as the last native visited tuple. Supporting dispatch0560:115–134 then stores that visited tuple independently of the last eligible returned item; an all-skipped but visited page must advance the durable cursor, not finish/restart the sweep. Finish only on no visited tuple or a visited deadline beyond the existing fixed sweep_through. Continue returning the Worker envelope cursor as its last eligible returned item, or null with no items, so runtime-rpc254–270 remains unchanged. A zero-item page may stop the current tick; the next minute resumes the durable native cursor. This changes no route, field, queue body, native index, run flag or financial owner and avoids an unbounded anti-join prefix. The internal due page and durable dispatcher must ship together; changing only the filtering order would otherwise strand later due runs.

### Exact minimal admission/receipt edits for root implementation

These are source proposals in this review note, not installed/product edits. Use current0559 variables/owners; retain the existing normal DONE path and unchanged REPREPARE/SYSTEM predicates. Set one flag before the common admission check:

```sql
v_partial_release := not v_system and p_action='RELEASE'
  and v_worker_status in ('PREPARING','REVIEW','READY')
  and v_job_status in ('READY','LEASED','BLOCKED');
```

At113–119 replace only `v_job_status<>'DONE'` with `(v_job_status<>'DONE' and not v_partial_release)`, and permit RELEASE's original_worker_statusPREPARING only when that flag is true. Retain active admin, zero realised/NET/transfers, exact worker/current economic revision, same module, and the existing refusal of a different pending action. At101–103 the RELEASE-only ENROLLING alternative must be an exact point proof: original command has expected_member_count=v_selected; command_member `(v_prepare,v_candidate)` exists with member_no<=enrolled_member_count; input PREPARE job `v_input` belongs to that same original command/Candidate/module. The original SEALED/full-count branch remains unchanged. Before receiving the new Office action command, after all these proofs, retire only the locked old job:

```sql
if v_partial_release then
  update private.bpay_next_job
    set status='DONE',phase='DISCARDED',lease_nonce=null,lease_until_utc=null,
        last_error_code='BPAY_NEXT_PREPARATION_REMOVED'
    where id=v_active and command_id=v_job_command and candidate_id=v_candidate
      and job_kind='PREPARE' and module_epoch=v_epoch and status=v_job_status;
  if not found then
    raise exception using errcode='23514',message='BPAY_NEXT_WORKER_RELEASE_PREVIOUS_JOB_CHANGED';
  end if;
end if;
```

Do not clear cursor_key, position_work_cursor, position_component_cursor, retained checkpoint columns or applied_line_count. The phase deliberately becomes terminal DISCARDED; no financial cursor is reset. Existing request128–139 retains `previous_job_id=v_active`, input original job, current/economic revisions and exact captured counts. Replace the parent increment142 with:

```sql
preparing_candidate_count=preparing_candidate_count
  +case when v_system or v_worker_status='PREPARING' then 0 else 1 end,
```

The already-present READY/REVIEW decrements143–144 remain. Existing execution guard6771–6793 allows the new Office ACCEPTED request before the same worker revision+1/active-null transition146–152. Pending REPREPARE requests stay refused by the current exact pending-action check; cancelling an accepted or running retry needs its own proven retirement branch and is not silently covered by this flag.

The retired-previous-job receipt must qualify the existing worker-action request, not create a root discard. Minimal point proof for its own branch uses current request/worker/job relations:

```sql
select x.*,w.status as current_worker_status into strict v_release
from private.bpay_next_worker_reprepare_request x
join private.bpay_next_run_worker w on w.id=x.run_worker_id
  and w.run_id=x.run_id and w.candidate_id=x.candidate_id
  and w.preparation_revision=x.target_preparation_revision
  and w.captured_preparation_revision=x.captured_preparation_revision
  and w.case_selection_revision=x.selection_revision
join private.bpay_next_command c on c.id=x.command_id
  and c.command_kind='PREPARE' and c.module_epoch=x.module_epoch
where x.previous_job_id=p_job_id and x.action='RELEASE' and x.initiator_kind='OFFICE'
  and x.status in ('ACCEPTED','RUNNING','COMPLETE')
  and x.module_epoch=v_job.module_epoch
order by x.target_preparation_revision limit 1;
```

Also assert `v_job.status='DONE'`, `v_job.phase='DISCARDED'`, exact Candidate, module NEXT/current epoch, unconfirmed root PREPARING/REVIEW and unchanged original run_command/input PREPARE membership. Use a point-select field list instead of `x.*` if the eventual target is a typed record. Return the old job's retained cursor/position pair, phaseDISCARDED, done/replay=true, rows_visited0 and current worker identity/status/issue facts. Public financial PREPARE chooses this exact branch before ordinary active capture, and its post-call expected phase is DISCARDED. The new separate retired schema already needed for whole-run discard can carry this receipt; ordinary CANCELLED and expiry outputs remain unchanged. Original unknown requests still send their original bytes/phase/cursors; the retired output describes the committed terminal fact.

### Exact Work-before-Case / bump-before-refresh supports

In existing worker_release_page WORK491–498, collect the bounded line-page Work IDs and lock them sorted before calling the unchanged release helper; after actual release/count progress bump the Candidate financial revision **before** refresh_many. Remove its later unconditional duplicate bump539. Do not bump on replay, native ORIGIN wait, or a projection-only page; retain a single explicit FINAL invalidation for actual weekly exclusion as in0746.

For CASE504–511 use at most50 active Case hold IDs, preserving `(run_worker_id,id)` seek. Obtain actual Work IDs through the frozen instruction/work_collection tuple for the same worker/Candidate/Case/component. A native Work collection is at most one per Case component. The bounded point pattern is:

```sql
select array_agg(h.id order by h.id),array_agg(distinct h.case_id),
       array_agg(distinct wc.work_id) filter(where wc.work_id is not null)
into v_hold_ids,v_case_ids,v_work_ids
from (select id,case_id,instruction_id from private.bpay_next_case_hold
      where run_worker_id=v_worker and status='ACTIVE'
        and(v_binding.case_hold_cursor is null or id>v_binding.case_hold_cursor)
      order by id limit least(p_limit,50)) h
join private.bpay_next_run_case_instruction i on i.id=h.instruction_id
  and i.run_worker_id=v_worker and i.candidate_id=v_candidate and i.case_id=h.case_id
left join private.bpay_next_work_collection wc on wc.id=i.work_collection_id
  and wc.candidate_id=v_candidate and wc.case_id=i.case_id
  and wc.case_component_id=i.case_component_id;
perform 1 from private.bpay_next_work where id=any(v_work_ids) order by id for update;
```

Then take the same bounded related-Case expansion/lock ordering from0746 before any Case helper; do not refresh a related Work after first taking a Case lock. Run unchanged release helpers in native hold order, bump financial revision for actual release, then refresh_many(v_work_ids,v_case_ids). CASE projection2474–2478 needs the same native selected-component→collection Work join and <=50 mixed-page bound. After refresh retain only exact pending ORIGIN Work IDs<=100; the next original WORKBENCH phase page must prelock those IDs and verify native task DONE/current tuple, requeue a stale DONE through the existing queue helper, and wait with checkpoint-only progress until complete. Original run_selection/Case cursors are already present on the PREPARE job; no new all-page selection list is needed. Only a nullable bounded pending Work array on the existing worker-action request is additional durable support if these waits are required for the card's completion proof.

### Observed bounded due correction

Current0814 SHA-256 `B3A33D3DB32FD2E0289CE73015580A5871826BCEFA1E8C6C13B2AC50F9497B02`: due3294–3308 now takes the original native deadline/id LIMIT slice before the discard point check, records the last visited tuple even when that row is skipped, and returns that internal tuple. The paired dispatcher3433–3485 reads the visited tuple3470–3472, advances the durable cursor from it3478–3480, and retains the last eligible returned tuple only for the existing Worker reply3483–3485. An all-skipped page therefore advances the next minute without prematurely restarting; an empty native slice or a visited deadline beyond the fixed sweep cut ends the cycle. This closes finding4 without a run flag, new index, route, client cursor or protocol. The original partial index is actually `bpay_next_preparation_due_idx` in migration `04102026_0501_banking_pay_next_preparation_expiry.sql`:23–24. This is a source-only PASS for the paired bounded selector correction; unfinished per-Candidate support and item/runtime acceptance are not claimed.

## Final current-source amendment — 11 October 2026

**SOURCE PASS for the reviewed dispatcher, enrollment, retirement and closed Worker seams.** No essential defect remains in this review's scope. The earlier hashes, proposals and unfinished per-Candidate statements above are historical; this amendment records the final reviewed bytes. The companion independent SQL receipt is [DR-09-sql-review.md](DR-09-sql-review.md), including its whole-definition comparison of the alias-only0847 successor.

Findings1–4 are closed. The original enroller and dedicated discard/worker-action enrollers acquire the Run SHARE lock with NOWAIT (`0814:166,1740,2022`), avoiding the identified command/run blocking cycle. A retained discard binding, including terminal PARTIAL, excludes Confirm (`2930`) and expiry acceptance (`3353`, `BPAY_NEXT_EXPIRY_DISCARD_ALREADY_BOUND`); the due reader skips that binding within the original bounded native slice. This is the first committed header action's exclusion, with blocked survivors retained. Expiry does not consume discard releases or relabel their receipts EXPIRED. The paired native due selector/dispatcher keeps last-visited durable traversal independently of the externally returned last-eligible cursor (`3294–3308`, `3470–3485`), so an all-skipped page advances on the next minute without an unbounded skipped prefix.

The requested existing-worker PREPARING Remove path is now present (`0814:3586–3714`). It admits only the exact native Worker/original PREPARE member and current job/revision under the existing locks, preserves same-command replay and the normal REPREPARE/SYSTEM restrictions, retires only that original job to DONE/DISCARDED (`3673–3676`), and retains its committed cursors/checkpoints. A PREPARING Worker adds0 to the preparing count (`3701`); cleanup debits it once (`3853`). This does not manufacture a Worker UUID for an unenrolled Candidate. The separately qualified previous-job receipt (`615–643`) and public financial dispatch preserve original unknown-request identity while returning terminal retained facts; economic Case identity remains distinct from execution revision.

Release/projection uses bounded genuine Work identities before Case locks, financial revision invalidation before refresh, original selection traversal and exact native ORIGIN completion before final completion (`0814:3727–3944`). The CASE page's distinct Case array is collected once from the <=50 native hold slice before lock budgeting and is not appended again inside the release loop (`3814–3834`). The original release formulas, protected realised/NET/transfer checks and native capacity ownership remain intact. These final implementations close the source gaps described in the historical proposals above.

All six current Worker files were reread. Queue wake bodies remain exactly `{id,kind}`; the finite PREPARATION_DISCARD kind uses fresh native CLAIM→one bounded STEP, and continuation is sent before ACK (`bpay-next-financial-queue.js:34–45,156–176`). No lease token is encoded in a wake or adopted from an unrelated active lease. Lost/unknown outcomes retain native replay/expiry recovery. Fixed RPC and phase maps, the separate nullable retained DISCARDED PREPARE schema, discard member-count conservation, qualified ENROLL wake and paired advancing internal DUE cursor remain closed (`bpay-next-job-step.js:81–95,214–280`; `bpay-next-preparation.js:313–325,551,585`; `bpay-next-preparation-expiry-rpc.js:171–181`). Ordinary PREPARE and expiry schemas retain their own narrower invariants.

The installed0814 source is immutable. The final0847 file changes ten definitions only by `SELECT *`→`SELECT dr09_row.*` and its matching native table alias, plus whitespace; the companion review's exact reversal comparison found every resulting definition equal to its0814 parent. The other22 definitions retain0814 ownership. No new formula, generic protocol, financial producer or provider connection is introduced.

Fresh SHA-256 of the saved sources:

| Saved source | SHA-256 |
|---|---|
| `supabase/repeatable/11102026_0814_banking_pay_next_preparation_discard_v1.sql` | `29F3099EBFB8A7E11D8C23A8996C5351EB9330316D8BDB6FD6D3A5136D3F379C` |
| `supabase/repeatable/11102026_0847_banking_pay_next_discard_row_projection_v1.sql` | `86343D45963A9335DE8F3AF0A3465CBBCDD5C4C2FD3EEEF69C628060C8DF1C47` |
| `broker/src/bpay-next-financial-queue.js` | `933990513DAD238624473628EF20A774EB1AE2D766399E4049B1A37AF79CFCB3` |
| `broker/src/bpay-next-job-rpc.js` | `28A8EF749A277A7FBCD10A6E5A9CAABD50A6B94AB795DF411C7BFDF36FDA676F` |
| `broker/src/bpay-next-job-step.js` | `70A989B65DDF08DF2B6F5F5133AC0521300708021382870730A9F58051907651` |
| `broker/src/bpay-next-runtime-rpc.js` | `62E0EA2E586FCEE1C097984A8793E0084C6FF3869E55815DF12E404624EAE73C` |
| `broker/src/bpay-next-preparation.js` | `572DEE1FBA45C21C01AE01CD60F5F3675E0B42363450625363BB7473F71EED20` |
| `broker/src/bpay-next-preparation-expiry-rpc.js` | `F173C7E0A9B1F037F32C324B711581115B72DC64162FCFD1582571C680D8FEC4` |

This amendment is read-only source review. Compilation, source-gate execution, SQL/database/business actions, fixtures, hosted calls, native UI/A4 and application acceptance were **NOT_RUN** by this reviewer. Root's separate installation/check reports are not independent runtime proof; no item/runtime PASS is asserted here.
