# F-25 weekly economic-origin proof

Read-only focused design, 9 October 2026. Current saved schema includes root's new `09102026_2153_banking_pay_next_accepted_input_v1.sql`; it is source, not an installed claim. Authority remains F-25/R2 §4 and the previous accepted-cut/case-cut notes. Root alone writes product. No SQL, runtime, tests, fixtures, data or hosted calls were executed. SQL below is proposed source-selection logic, not execution evidence.

## Required meaning of the current child

At 2153:246–293, `private.bpay_next_accepted_week_input` already retains exact contribution/Candidate/week/original worker identity, original pay-date/creation tuple, `economic_basis_command_id`, `economic_basis_sequence`, `economic_basis_sha256`, optional `accepted_projection_id`, basis kind, payment/eligibility state, eligible arranged operand, original transfer/return/reissue IDs and binding revision/ref. The child is a retained **economic input to existing H/floor calculation**. It must not freeze cash owed/reissued/paid, principal, funded, recovered, held or realised counters.

Define `economic_basis_command_id/sequence` as the **latest applicable PREPARE or PAYE_NET_ENTRY economic basis below this accepted PREPARE**, not the latest arbitrary Candidate command or latest settlement. Later outcome/cancel/expiry determines **eligibility**, independently. Retain that exact eligibility command/job/request/outcome identity inside the closed canonical origin proof hashed by `economic_basis_sha256`; a hash without reconstructible immutable referenced evidence is not provenance. If root later adds explicit eligibility-command columns, they must preserve this same distinction rather than redefine the basis fields.

Validation must ensure `economic_basis_sequence < accepted_input.accepted_sequence`, same Candidate/run-worker/week and exact origin. The current child has a composite command sequence FK but no standalone inequality against the parent; its append guard/owning insert must supply that cross-row check. Contribution revision alone is a mutable observation, not old-version authority.

## Exact source column relationships

| Existing object | Exact relationship/evidence |
| --- | --- |
| `bpay_next_worker_week_contribution` | `id,candidate_id,pay_week_start,original_run_worker_id,original_pay_date,original_created_at_utc,contribution_revision,basis_kind,original_gross_amount,original_payroll_net,payment_state,eligibility_state,eligible_arranged_amount,primary_binding_revision,return_floor_binding_ref,original_transfer_id,returned_cash_id,reissue_transfer_id`; 0380 adds `accepted_projection_id,accepted_bank_cash`. UNIQUE(original_run_worker_id), plus root 2153 composite identity key. |
| `bpay_next_run_worker` / `bpay_next_pay_run` / `bpay_next_run_command` | Contribution's original worker joins exact Candidate/run; original tuple equals `pay_run.pay_date,created_at_utc`; run_command gives original PREPARE command. `run_worker.case_selection_revision` distinguishes no-case from exact case selection; original gross must match the genuine completed economic output, not an arbitrary latest row. |
| `bpay_next_command` / `command_member` / `job` | Original command `command_kind='PREPARE'`, same agency sequence/module identity, original selected Candidate member; exact Candidate job `(command_id,candidate_id,job_kind='PREPARE')`, `command_sequence` equal command sequence. Genuine completed READY computation is job DONE/READY and exact completed owner output. **Do not require command.status='COMPLETE'**: 1630 and 1958 mark Candidate job DONE/READY but do not complete the shared multi-Candidate PREPARE command. |
| `bpay_next_run_line` | Original no-case frozen WORK economics: `id,run_worker_id,line_no,captured_revision_id,approved_line_id,work_id,component_key,source_consumed_ex_vat,valuation_policy_id,policy_window_id,frozen_ex_vat,frozen_vat,frozen_inc_vat`. Exact retained original line/count seals prove completed gross; no Timesheet/payment-history SUM. |
| `bpay_next_case_allocation_state`, `run_case_instruction`, `case_allocation_result` | Original case DRAFT completion: exact worker/PREPARE/job/preparation/selection revision, pass DRAFT/projection 0, status/prepare_stage COMPLETE, count seals and genuine frozen allocation outputs. Gross is `captured_work_gross + allocated_gross_additions - allocated_gross_deductions` at 1958:586; frozen instructions/results and original output validate it. Case-only NET additions are not positive payroll gross. |
| `bpay_next_paye_net_request` | `command_id,run_worker_id,candidate_id,request_no,input_kind,entered_paye_net,frozen_gross_inc_vat,case_draft_state_id,preparation_revision,selection_revision,expected_projection_revision`; immutable request belongs to PAYE_NET_ENTRY command/member/Candidate. Case request also pins exact DRAFT allocation-state. |
| `bpay_next_net_projection` | `id,run_worker_id,request_command_id,projection_no,input_kind,gross_ex_vat,gross_vat,gross_inc_vat,entered_paye_net,accepted_recoveries,cash_amount,accepted_net_additions,accepted_gross_additions,accepted_gross_deductions,retired_at_utc`. Projection binds request by command, worker and request_no. Current 0390:183–197 guard allows only retirement timestamp UPDATE; all economic fields remain unchanged. A later retirement alone does not invalidate the earlier accepted origin. |
| `bpay_next_net_destination_state` | Exact sealed projection's Candidate-own earnings: `state_id,run_worker_id,projection_id,external_credit_count,external_leg_count,external_amount,own_amount,cash_amount,completed_at_utc`; exact NET allocation-state/request/command relation. 1827:178–194 seals own_amount=cash_amount-external_amount; guard refuses state mutation after projection_id is set. Retain IDs, sums and scope digests, never bank details/hash values in this report. |
| `bpay_next_outcome_request` / `transfer_outcome` / `transfer` | Outcome request `command_id,outcome_id,transfer_id,run_worker_id,candidate_id,posted_member_count,posting_complete` joins exact CSV_SETTLEMENT or INTERNAL_SETTLEMENT command/job. Whole transfer amount/outcome kind and original/reissue transfer/projection ancestry must match. An accepted request not completely posted cannot count as applied eligibility. |
| `bpay_next_return_request` / `return_cash` | Return request `command_id,outcome_id,transfer_id,run_worker_id,candidate_id,posting_complete` joins CSV_RETURN command/job; transfer identifies original/reissue ancestor and exact immutable return outcome. Return_cash identity is original_transfer_id/Candidate, but its amount_owed/reissued counters remain live money, not captured week basis. |
| `bpay_next_cancel_request` | Exact SIMPLE_CANCEL command, original worker/Candidate, status CANCELLED, released_line_count=expected_line_count, exact completed job and applicable case/destination cancel certificate. Requested/blocked/in-progress cancellation is not exclusion proof. |
| `bpay_next_preparation_expiry_request` / `preparation_expiry_worker` | PREPARATION_EXPIRY command/run/original_prepare_command_id; exact Candidate worker binding/job; status EXPIRED/stage COMPLETE, exact released counts and original worker/job/preparation/selection evidence. A partial non-READY preparation need not have a contribution; missing genuine READY case fact is a refusal, never zero. |

Original GROSS_FALLBACK sources are **1630:78–85 → 0420:99–105** for no-case and **1958:605–617** for case preparation. Existing frozen output/positive count/completion is the origin. Root may hash these genuine output identities and amounts at the one acceptance transaction; do not manufacture a historical gross command or reconstruct agency payment history. An unversioned missing original completion witness remains unbound/refused.

## NET operand is bank earnings, not entered payroll NET

This is an important current-contract distinction:

- 0380:82–87 defines ELIGIBLE amount from `accepted_bank_cash` when accepted_projection_id exists, otherwise original_gross_amount.
- 0390:134–175 projection guard verifies exact request/projection/gross and uses projection.cash_amount as initial bank basis, then **sealed destination.own_amount** if an external credit/destination split exists. It proves own+external=cash and exact completed same-worker projection scope. The eligible operand can differ from entered_paye_net **and** total projection.cash_amount.
- 0390:491–518 computes case cash from actual entered NET + frozen NET additions - frozen recoveries, seals destination, then stores v_own_cash as eligible arranged operand. Repeating a deduction/addition or using whole cash would double-count another destination.
- CASE_PAYOUT keeps `basis_kind='GROSS_FALLBACK'`, accepted_projection_id present, `eligibility_state='EXCLUDED'`, operand=0, even when cash is positive. Do not assume GROSS_FALLBACK always means projection NULL.
- Simple no-case PAYE_MANUAL 0420 uses EXCLUDED when cash=0; case NET 0390 assigns ELIGIBLE for non-CASE_PAYOUT even if own_amount=0. Preserve that exact legitimate distinction; ELIGIBLE zero is valid.

The hash/origin proof therefore pins request command+projection, exact own-destination state where present, input_kind and original completion. It does not capture paid/cash-return counters or imply external funds moved.

## Latest applicable basis predicate

Inputs below are trusted values from the existing acceptance transaction: `p_candidate_id`, `p_pay_week_start`, original current run's `p_pay_date,p_created_at_utc`, and eventual accepted `p_prepare_sequence`. Acquire the exact run/Candidate/weekly/input/FK evidence business locks **before** the agency-clock final tail. Compute/validate these references once at acceptance; later replay reads the child, never reruns this query against today's contribution.

Current 1958 prior ordering is `(original_pay_date,original_created_at_utc,original_run_worker_id) < (current pay_date,current created_at,current worker_id)`. At Enqueue the current worker may not yet exist: final 2018 CLAIM creates it at 706. For unequal first two tuple components the prior decision is determined. If a different run has an exact `(pay_date,created_at)` tie, refuse that narrow ambiguous scope unless an exact future worker UUID is already reserved/retained; do not invent its UUID ordering or silently replace the existing tie-breaker. This is a bounded honest edge-case refusal, not a new sort policy.

Proposed selector core, using only existing objects; completion and hash checks described below remain mandatory:

```sql
-- Source design only. Run as part of the prelocked acceptance owner, not a
-- standalone current-row fallback at PIN. Reject exact timestamp ties first.
with original_scope as (
  select x.*, w.run_id, w.case_selection_revision,
         pc.id as original_prepare_command_id,
         pc.agency_sequence as original_prepare_sequence,
         pj.id as original_prepare_job_id,
         pj.status as original_prepare_job_status,
         pj.phase as original_prepare_job_phase
  from private.bpay_next_worker_week_contribution x
  join private.bpay_next_run_worker w
    on w.id=x.original_run_worker_id and w.candidate_id=x.candidate_id
  join private.bpay_next_pay_run r on r.id=w.run_id
  join private.bpay_next_run_command rc on rc.run_id=r.id
  join private.bpay_next_command pc
    on pc.id=rc.command_id and pc.command_kind='PREPARE'
  join private.bpay_next_command_member pm
    on pm.command_id=pc.id and pm.candidate_id=x.candidate_id
  join private.bpay_next_job pj
    on pj.command_id=pc.id and pj.candidate_id=x.candidate_id
   and pj.job_kind='PREPARE' and pj.command_sequence=pc.agency_sequence
   and pj.module_epoch=pc.module_epoch
  where x.candidate_id=p_candidate_id and x.pay_week_start=p_pay_week_start
    and (x.original_pay_date,x.original_created_at_utc)
        =(r.pay_date,r.created_at_utc)
    and r.pay_date>=p_pay_week_start and r.pay_date<p_pay_week_start+7
    and (r.pay_date,r.created_at_utc)<(p_pay_date,p_created_at_utc)
    and pc.agency_sequence<p_prepare_sequence
    and pc.status in ('SEALED','COMPLETE') and pc.sealed_at_utc is not null
    and pc.expected_member_count=pc.enrolled_member_count
), selected_request as (
  select s.*, n.command_id as latest_net_command_id,
         n.request_no, n.input_kind, n.case_draft_state_id,
         n.frozen_gross_inc_vat, n.entered_paye_net,
         n.net_sequence, n.net_job_id, n.net_status, n.net_phase
  from original_scope s
  left join lateral (
    -- Choose latest ACCEPTED request, not only those with a ready projection.
    -- An earlier pending latest request must refuse; do not fall back to old NET.
    select q.*, c.agency_sequence as net_sequence,
           j.id as net_job_id,j.status as net_status,j.phase as net_phase
    from private.bpay_next_paye_net_request q
    join private.bpay_next_command c
      on c.id=q.command_id and c.command_kind='PAYE_NET_ENTRY'
    join private.bpay_next_command_member m
      on m.command_id=c.id and m.candidate_id=s.candidate_id
    left join private.bpay_next_job j
      on j.command_id=c.id and j.candidate_id=s.candidate_id
     and j.job_kind='PAYE_NET_ENTRY'
     and j.command_sequence=c.agency_sequence and j.module_epoch=c.module_epoch
    where q.run_worker_id=s.original_run_worker_id
      and q.candidate_id=s.candidate_id
      and c.agency_sequence>s.original_prepare_sequence
      and c.agency_sequence<p_prepare_sequence
    order by c.agency_sequence desc limit 1
  ) n on true
)
select s.*, p.id as selected_projection_id,
       p.request_command_id,p.projection_no,p.gross_inc_vat,
       p.cash_amount,p.accepted_recoveries,p.accepted_net_additions,
       d.state_id as destination_state_id,d.own_amount,d.external_amount,
       d.completed_at_utc as destination_completed_at_utc
from selected_request s
left join private.bpay_next_net_projection p
  on p.request_command_id=s.latest_net_command_id
 and p.run_worker_id=s.original_run_worker_id and p.projection_no=s.request_no
 and p.input_kind=s.input_kind
left join private.bpay_next_net_destination_state d on d.projection_id=p.id;
```

Do not silently drop inconsistent rows through inner joins and report a smaller complete set. Before this selector, enumerate the bounded expected prior original-worker set below the cut from the indexed current-week runs/commands and compare exact count/identities with contribution/proof set. A prior READY/DRAFT/ISSUED/COMPLETE worker with no legitimate contribution still means CASE_WEEK_BASIS_UNBOUND; no later transfer can backfill it. Explicit completed cancellation/expiry can retain an excluded zero witness; unsupported omissions are refused. No all-agency sweep.

For each result:

1. Prove exact original completed PREPARE economic output; do not select its command by an arbitrary latest PREPARE for the Candidate. Command member, job sequence, original run and Candidate must all agree. Current DONE/READY proof is sufficient for its original Candidate completion with exact frozen output. If later expiry rewrote the original job phase, use only its genuine bound snapshot proving original READY/completion; an EXPIRED original job does not by itself prove previous READY. Otherwise refuse.
2. No prior NET request: basis is original PREPARE, GROSS_FALLBACK; require genuine original gross equals exact completed output. A projection/request existing only **after** this cut cannot replace it.
3. Prior NET request exists: require exact latest request's job DONE/PROJECTED, projection and request/input kind/no/worker/gross agreement; exact case NET allocation-state COMPLETE/projection/request/preparation/selection counts where case_draft_state_id exists; simple path's no-case scope when absent. A missing job or projection is pre-accept pending/refusal. Same command.status COMPLETE is not required for the simple owner either; it marks job DONE/PROJECTED.
4. PAYE_MANUAL operand is sealed destination.own_amount if a matching destination state exists and is valid; otherwise exact projection.cash_amount. CASE_PAYOUT operand=0/EXCLUDED. Preserve legitimate case/simple zero state distinctions. Do not use `entered_paye_net` as bank earnings.
5. Do not filter `retired_at_utc is null` as today's winner. The selected historical economic fields remain immutable; ensure every retirement/successor is genuinely explained by accepted request/projection sequence. Unknown retirement without that lineage refuses; timestamps do not establish order.
6. Cross-check current contribution tuple against this selected genuine origin while original owner locks are held, or against an already-retained immutable predecessor witness. At fresh acceptance all applicable known mutations are below its final clock sequence; this current consistency check is not later historical reconstruction. `contribution_revision` is only the exact observed checkpoint included in the child/hash.

## Eligibility is a second constrained selection under the same clock

Build a finite union of **actual same-original-worker** command receipts below p_prepare_sequence, ordered by agency_sequence. Initial original PREPARE/basis projects ARRANGED eligibility. Candidate membership in an unrelated command is not enough. SQL predicate skeleton for the exact eligibility command set:

```sql
-- Source design: collect every applicable ACCEPTED receipt below the cut,
-- retaining missing jobs/pending states. Do not silently pick only DONE jobs.
with eligibility_commands as (
  select c.id as command_id,c.agency_sequence,c.command_kind,
         r.run_worker_id,r.candidate_id,r.outcome_id,r.transfer_id,
         r.posting_complete and r.posted_member_count=t.member_count
           as receipt_complete,
         null::uuid as expiry_binding_job_id
  from private.bpay_next_outcome_request r
  join private.bpay_next_command c on c.id=r.command_id
  left join private.bpay_next_transfer t on t.id=r.transfer_id
  where r.run_worker_id=p_original_worker_id and r.candidate_id=p_candidate_id
    and c.command_kind in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT')
    and c.agency_sequence<p_prepare_sequence
  union all
  select c.id,c.agency_sequence,c.command_kind,
         r.run_worker_id,r.candidate_id,r.outcome_id,r.transfer_id,
         r.posting_complete,null::uuid
  from private.bpay_next_return_request r
  join private.bpay_next_command c on c.id=r.command_id
  where r.run_worker_id=p_original_worker_id and r.candidate_id=p_candidate_id
    and c.command_kind='CSV_RETURN' and c.agency_sequence<p_prepare_sequence
  union all
  select c.id,c.agency_sequence,c.command_kind,
         r.run_worker_id,r.candidate_id,null::uuid,null::uuid,
         r.status='CANCELLED' and r.released_line_count=r.expected_line_count,
         null::uuid
  from private.bpay_next_cancel_request r
  join private.bpay_next_command c on c.id=r.command_id
  where r.run_worker_id=p_original_worker_id and r.candidate_id=p_candidate_id
    and c.command_kind='SIMPLE_CANCEL' and c.agency_sequence<p_prepare_sequence
  union all
  select c.id,c.agency_sequence,c.command_kind,
         x.run_worker_id,x.candidate_id,null::uuid,null::uuid,
         x.status='EXPIRED' and x.stage='COMPLETE'
           and x.released_work_count=x.expected_work_count
           and x.released_case_hold_count=x.expected_case_hold_count,
         x.job_id
  from private.bpay_next_preparation_expiry_worker x
  join private.bpay_next_preparation_expiry_request r
    on r.command_id=x.command_id and r.run_id=x.run_id
   and r.original_prepare_command_id=p_original_prepare_command_id
  join private.bpay_next_command c on c.id=x.command_id
  where x.run_worker_id=p_original_worker_id and x.candidate_id=p_candidate_id
    and c.command_kind='PREPARATION_EXPIRY'
    and c.agency_sequence<p_prepare_sequence
)
select e.*,m.candidate_id as enrolled_candidate_id,
       j.id as job_id,j.status,j.phase
from eligibility_commands e
left join private.bpay_next_command c on c.id=e.command_id
left join private.bpay_next_command_member m
  on m.command_id=c.id and m.candidate_id=e.candidate_id
left join private.bpay_next_job j
  on j.command_id=c.id and j.candidate_id=e.candidate_id
 and j.command_sequence=c.agency_sequence and j.module_epoch=c.module_epoch
 and j.job_kind=c.command_kind
 and (e.expiry_binding_job_id is null or j.id=e.expiry_binding_job_id)
order by e.agency_sequence desc;
-- Validate exact original projection/transfer/outcome/cancel/expiry scope next.
-- A missing member/job or receipt_complete IS NOT TRUE is refusal, never drop.
-- CSV/INTERNAL_SETTLEMENT/CSV_RETURN completed phase is COMPLETE;
-- SIMPLE_CANCEL completed phase is CANCELLED; expiry completed phase EXPIRED.
-- After proving all earlier applicable events complete, select the latest
-- applicable event above the selected economic-basis sequence, or retain the
-- basis's original ARRANGED state. ONEOFF is not a weekly earnings event.
```

The returned `command_kind` is the exact existing owner kind. Compare the latest **applicable** event with the retained latest basis sequence; do not mistake an unrelated/ONEOFF leg for Candidate earnings. Require exact corresponding request/job state and original projection/transfer ancestry, then derive the following closed states:

| Applicable earlier command/owner | Genuine economic eligibility result |
| --- | --- |
| Original PREPARE / completed NET basis, no later applicable outcome | ARRANGED; economic basis's exact ELIGIBLE/EXCLUDED and operand. |
| CSV_SETTLEMENT original transfer, final **2018:3331** `apply_week_outcome_v1(uuid,uuid)` | Exact SETTLED whole-transfer/posting-complete receipt and job. Marks PAID without changing prior earnings basis; preserve its original eligibility/operand. One-off destination leg is not Candidate weekly earnings. |
| INTERNAL_SETTLEMENT **0490:154** `complete_internal_zero_v1(uuid,bigint)` | Exact INTERNAL_ZERO receipt/projection, fully posted zero cash and DONE/COMPLETE; marks PAID with original zero eligibility. No positive payroll-net adoption for zero bank cash. |
| CSV_RETURN, final 2018:3511–3514 | Actual original/reissue returned receipt and return_cash identity; non-CASE_PAYOUT becomes UNBOUND and operand NULL. New accepted_week_input permits only ELIGIBLE/EXCLUDED, so return-floor scope truthfully refuses until an actual earlier immutable binding owner exists. Do not convert to EXCLUDED zero or use return_cash counters as income proof. |
| CSV_SETTLEMENT of reissue, final 2018:3492–3514 | Marks REISSUED_PAID but non-CASE_PAYOUT remains UNBOUND. Reissue money does not restore floor eligibility automatically. No current return-floor-binding writer was found in the inspected canonical repeatables. |
| SIMPLE_CANCEL **0430:196** `apply_cancelled_week_v1(uuid)` | Fully cancelled exact request/job plus genuine case/destination cancel certificate. CANCELLED/EXCLUDED/0. Requested/CANCELLING/BLOCKED is pending or unsupported, not a zero receipt. |
| PREPARATION_EXPIRY **0510:191** `apply_expired_week_v1(uuid)` | Exact bound EXPIRED/COMPLETE worker/request/job and original scope. CANCELLED/EXCLUDED/0 for an actual contribution; genuine partial preparation/no contribution remains absence, never manufactured zero. |
| FAILED | Schema allows FAILED, but no legitimate contribution FAILED writer was found in this bounded current-owner census. Do not infer it from transfer status alone; typed unsupported receipt refusal until its owning branch exists. |

A conservative bounded pre-acceptance guard may also retain the existing Candidate-order predicate `EXISTS (SELECT 1 FROM private.bpay_next_job j WHERE j.candidate_id=p_candidate_id AND j.command_sequence<p_prepare_sequence AND j.status<>'DONE')` as an explicit pending refusal. It must also check received applicable request rows with no job yet, because job absence is not completion. Source-qualified exact receipt predicates above can narrow the weekly proof without weakening the actual Candidate barrier.

Do not choose MAX among only DONE jobs: that would hide a latest earlier accepted pending outcome/cancellation/expiry. An accepted applicable earlier command with no fully completed exact owner receipt is a typed **pre-acceptance pending/refusal**. The original PREPARE barrier remains; this extra refusal prevents capturing an incomplete weekly basis and silently refreshing it later.

Current root schema 2153:288–289 requires return_floor_binding_ref for every RETURNED_OWED/REISSUED_PAID row. Current 2018 legitimately leaves CASE_PAYOUT EXCLUDED zero without creating that binding. Until root supplies a narrow real non-earnings supported receipt contract, that branch cannot be inserted as if bound: refuse the exact branch or retain an explicitly certified excluded scope through its approved schema route. Never manufacture return_floor_binding_ref or copy financial counters to satisfy the constraint.

## Closed canonical hash contract

Suggested domain `BPAY_NEXT_ACCEPTED_WEEK_ORIGIN_V1`. Hash only a closed canonical object with exact original completion identity/digest, Candidate/week/original run-worker tuple, original PREPARE ID/sequence/module/member/job/completion witness, selected economic basis command ID/sequence, genuine latest NET request+projection IDs and economic output digest when present, sealed own-destination state/digest when present, applicable eligibility command/job/request/outcome/transfer/return/expiry/cancel witness IDs/sequences/digests, basis/payment/eligibility enums and the validated economic operand. Include explicit null/not-applicable fields. No bank/account fields, personal data, wall-clock cutoff or money counter snapshot.

Relevant existing immutable request/projection/economic outputs may have their bounded contents hashed at acceptance, but the linked IDs must let the owner independently validate that same content later. The hash is not a replacement for missing owner evidence and cannot authenticate a caller-supplied amount. Multiple identities/count mismatch, unsupported lane, wrong Candidate or unknown predecessor gives typed refusal. Child INSERT remains append-only; later replay returns identical original references/hash/operand.

## Bounded honest refusals and source review list

- Exact prior run timestamp tie without retained future worker identity; missing original run_command/member/Candidate/job completion or source count seal; worker ready but original legitimate weekly fact absent.
- Latest earlier NET request is pending/missing projection, wrong Candidate/no/expected predecessor or case state incomplete; unknown retirement; missing/contradictory destination seal; CASE_PAYOUT treated as earnings.
- Any accepted earlier applicable outcome/cancel/expiry not completely applied; only current status without exact request/job/transfer lineage; unrelated Candidate command used as provenance.
- Non-payout returned/reissued floor basis UNBOUND; unsupported FAILED producer; returned CASE_PAYOUT cannot meet current typed-child constraint without truthful non-earnings contract.
- Observed contribution revision/state/amount cannot be matched to retained genuine basis/eligibility evidence; current mutable counter/hash used as historical version.
- Arbitrary LIMIT truncation without complete expected-count/refusal proof. Retain bounded current-week keyset pages or refuse an explicitly finite unsupported scope; do not silently drop older arrangements.

These refusals happen before durable acceptance. NOT_AFFECTED ordinary OVERPAYMENT continues the exact nonempty proof and does not acquire a new week dependency. Later monetary outcome/case capacity still runs under original live owner guards. No new clock, event money ledger or financial calculator is proposed.

## Fresh raw source hashes

Source bytes observed during this read. Root may subsequently edit; re-read before implementation/review.

| Source under BUILD-BE | Raw SHA-256 |
| --- | --- |
| `supabase/migrations/09102026_2153_banking_pay_next_accepted_input_v1.sql` | `5af3018f70408d76d870354383100f5b16ff77306e02c760a1323f4be23cee9c` |
| `supabase/migrations/03102026_1324_banking_pay_next_core.sql` | `f38878c7b09e3fbac2d21acbf96c4e88bdac1d4c32abac7ee98d2672df7ba52c` |
| `supabase/migrations/04102026_0300_banking_pay_next_case_instruction_core.sql` | `04b44425d288e714037012c3b4eb4d27679137905e1c380682812cc8273ad40e` |
| `supabase/migrations/04102026_0380_banking_pay_next_case_net_binding.sql` | `9b750149eb86e7ddd6cff218783cbff2674c64abf3a117f20e66579d5f3d291e` |
| `supabase/migrations/04102026_0501_banking_pay_next_preparation_expiry.sql` | `c9758121675a070ae40f1be352a40247f357309ae94efca59baf6de002cac845` |
| `supabase/migrations/04102026_1827_banking_pay_next_candidate_destination_group.sql` | `af9849f6e83784b0064ee58179aabc740d396764c3639ef7132967f394c9512d` |
| `supabase/repeatable/03102026_1630_banking_pay_next_no_case_completion_v1.sql` | `3816b38a9b0776e033abbc55fa010cf62523294070c09a9c8508e36ac2d63d9c` |
| `supabase/repeatable/04102026_0420_banking_pay_next_no_case_week_fact_v1.sql` | `335f80467b7eb434034c3aa67295355aa8af04cc9c0834e313a67b056d10766b` |
| `supabase/repeatable/04102026_0110_banking_pay_next_simple_paye_projection_v1.sql` | `d74d8a9629ef245eb8940c4026e0ebd2b99d9d81a72b96708c72673164669c1e` |
| `supabase/repeatable/04102026_0390_banking_pay_next_case_paye_net_v1.sql` | `9622c5b9259f80e69a4fb661425c0016a4f3ee69ee8b02fcb3a7baa7ba389fbb` |
| `supabase/repeatable/04102026_0430_banking_pay_next_week_lifecycle_v1.sql` | `fa496325a5df42973d25eb7b467c3751847df49786442f7540106e48e2f82987` |
| `supabase/repeatable/04102026_0490_banking_pay_next_internal_zero_settlement_v1.sql` | `88a5679e4daeb440c36402f11c866394030af43509fe3520d71bb5399e5ef47c` |
| `supabase/repeatable/04102026_0510_banking_pay_next_preparation_expiry_v1.sql` | `f4eb08c3c963c1850396642fd98947cf27722d96e20b6bb77db394635c3341aa` |
| `supabase/repeatable/04102026_1827_banking_pay_next_candidate_destination_group_v1.sql` | `33891038c2865684c091f7bd3df4ce188bb42e026129163216e3bc341feb198c` |
| `supabase/repeatable/09102026_1958_banking_pay_next_ordinary_case_week_applicability_v1.sql` | `c6f7d426fa60d3c6cf5db4d5c386aeb8111f974e455bd475a10dcfecf0e5851b` |
| `supabase/repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql` | `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b` |
| `supabase/repeatable/04102026_0230_banking_pay_next_simple_csv_return_v1.sql` | `0b4085def3cfcc9c6b8f7d1aae05716dea85578bd6433ed28f70038f98cc145d` |
