# WB-21 exact PREPARE lifecycle seams

Read-only extraction, 10 October 2026. These are design fragments for new successor definitions, not installed/product SQL. Full original functions/ACLs stay intact outside the identified blocks. No DB/runtime/tests/fixtures executed. This is the explicitly accepted small WB21 ordered-CREATE support, not parked F25 machinery. Use the actual existing command, run_command, enrollment clock and expiry request; no new command kind, clock or protocol.

CREATE must reserve PREPARE only when its actual eligible Candidate count is positive. An empty/skipped-only offer retains the planned UUID without a command FK, has null sequence/explicit empty receipt and never inserts run_command. The snippets below apply to genuinely reserved positive offers. `accepted_choice_descriptor` and `accepted_choice_command_id` mean the actual new1832 fields; no placeholder historical receipt or accepted_input table is used.

## 1.1590 enqueue — transition prebound RECEIVED exactly once

Source `03102026_1590_banking_pay_next_selection_manifest_v1.sql:134–186`, signature `private.bpay_next_enqueue_preparation_v1(uuid,uuid)`. Add declarations:

```sql
v_command private.bpay_next_command%rowtype;
v_epoch bigint;
```

After original input validation and BEFORE original run FOR UPDATE, add existing module qualification:

```sql
select m.owner_epoch into v_epoch
  from private.bpay_next_module_control m
  where m.id=1 and m.active_owner='NEXT' for share;
if v_epoch is null then
  raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
end if;
```

Replace ONLY existing-binding block153–162 with the following. The original absent-binding path163–184 remains byte-for-byte; do not remove its original receive/binding/ENROLLING transition.

```sql
if found then
  if v_existing_command<>p_command_id then
    raise exception using errcode='23514',
      message='BPAY_NEXT_PREPARE_COMMAND_REPLAY_CONFLICT';
  end if;
  -- Plain read: replay must not take command UPDATE while holding run UPDATE.
  select c.* into strict v_command
    from private.bpay_next_command c where c.id=p_command_id;
  if v_command.command_kind is distinct from 'PREPARE'
     or v_command.module_epoch is distinct from v_epoch then
    raise exception using errcode='23514',
      message='BPAY_NEXT_PREPARE_COMMAND_STATE_INVALID';
  end if;
  v_sequence:=v_command.agency_sequence;
  if v_command.status='RECEIVED' then
    if v_run.accepted_choice_descriptor is null
       or v_run.accepted_choice_command_id is distinct from p_command_id
       or v_command.expected_member_count is not null
       or v_command.enrolled_member_count<>0
       or v_command.sealed_at_utc is not null
       or exists(select 1 from private.bpay_next_command_member cm
         where cm.command_id=p_command_id)
       or exists(select 1 from private.bpay_next_job j
         where j.command_id=p_command_id) then
      raise exception using errcode='23514',
        message='BPAY_NEXT_PREPARE_COMMAND_STATE_INVALID';
    end if;
    -- Identical original1590 sealed/count/Candidate predicate.
    if v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED'
       or v_run.work_choice_count<>v_run.selection_count
       or v_run.sealed_work_choice_count<>v_run.selection_count
       or v_run.selected_candidate_count<1
       or (v_run.selection_count=0 and (v_run.selection_page_count<>0
         or v_run.sealed_case_candidate_count<>v_run.selected_candidate_count)) then
      raise exception using errcode='55000',
        message='BPAY_NEXT_PREPARE_SELECTION_NOT_SEALED';
    end if;
    update private.bpay_next_command
      set expected_member_count=v_run.selected_candidate_count,status='ENROLLING'
      where id=p_command_id and status='RECEIVED'
        and module_epoch=v_epoch and expected_member_count is null
        and enrolled_member_count=0 and sealed_at_utc is null;
    if not found then
      raise exception using errcode='23514',
        message='BPAY_NEXT_PREPARE_COMMAND_STATE_INVALID';
    end if;
    return jsonb_build_object('run_id',p_run_id,'command_id',p_command_id,
      'agency_sequence',v_sequence,'replay',false);
  end if;
  -- Do not return an abandoned zero-job receipt as successful enqueue.
  if v_command.status not in ('ENROLLING','SEALED','COMPLETE')
     or (v_command.status='COMPLETE' and v_command.expected_member_count=0) then
    raise exception using errcode='23514',
      message='BPAY_NEXT_PREPARE_COMMAND_STATE_INVALID';
  end if;
  return jsonb_build_object('run_id',p_run_id,'command_id',p_command_id,
    'agency_sequence',v_sequence,'replay',true);
end if;
```

## 2.1430 enroller — two metadata alternatives before run SHARE

Source `03102026_1430_banking_pay_next_position_writer_v1.sql:106 onward`, signature `private.bpay_next_enroll_one_v1()`. Insert immediately AFTER original run_command lookup135–137 and BEFORE original run SHARE138–139. Preserve every original ENROLLING PREPARE statement/member/job/clock transition and all other command-kind branches.

```sql
if v_command.status in ('RECEIVED','COMPLETE') then
  -- NO FOR SHARE/UPDATE here. Seal/expiry acquire run then command;
  -- waiting/retired metadata must not add command -> run locking.
  select r.* into strict v_run from private.bpay_next_pay_run r
    where r.id=v_run_id;
  if v_command.status='RECEIVED' then
    if v_command.module_epoch is distinct from v_epoch
       or v_run.status is distinct from 'PREPARING'
       or v_run.selection_state is distinct from 'OPEN'
       or v_run.confirmed_at_utc is not null
       or v_run.accepted_choice_descriptor is null
       or v_run.accepted_choice_command_id is distinct from v_command.id
       or v_command.expected_member_count is not null
       or v_command.enrolled_member_count<>0
       or v_command.sealed_at_utc is not null
       or exists(select 1 from private.bpay_next_command_member cm
         where cm.command_id=v_command.id)
       or exists(select 1 from private.bpay_next_job j
         where j.command_id=v_command.id)
       or exists(select 1 from private.bpay_next_run_worker rw
         where rw.run_id=v_run_id) then
      raise exception using errcode='23514',
        message='BPAY_NEXT_PREPARE_ENROLLMENT_INVALID';
    end if;
    return null;
  end if;
  -- Only an exact expired, never-enrolled positive CREATE may retire0.
  if v_command.module_epoch is distinct from v_epoch
     or v_command.expected_member_count is distinct from 0::bigint
     or v_command.enrolled_member_count<>0
     or v_command.sealed_at_utc is null
     or v_run.selection_state is distinct from 'OPEN'
     or v_run.status not in ('CANCELLING','CANCELLED')
     or v_run.confirmed_at_utc is not null
     or v_run.accepted_choice_descriptor is null
     or v_run.accepted_choice_command_id is distinct from v_command.id
     or not private.bpay_next_preparation_expiry_fenced_v1(v_run_id,v_command.id)
     or exists(select 1 from private.bpay_next_command_member cm
       where cm.command_id=v_command.id)
     or exists(select 1 from private.bpay_next_job j
       where j.command_id=v_command.id)
     or exists(select 1 from private.bpay_next_run_worker rw
       where rw.run_id=v_run_id) then
    raise exception using errcode='23514',
      message='BPAY_NEXT_PREPARE_ENROLLMENT_INVALID';
  end if;
  update private.bpay_next_enrollment_clock
    set last_enrolled_sequence=v_command.agency_sequence
    where id=1 and last_enrolled_sequence=v_last
      and v_command.agency_sequence=v_last+1;
  if not found then
    raise exception using errcode='23514',
      message='BPAY_NEXT_PREPARE_ENROLLMENT_INVALID';
  end if;
  return null;
end if;
```

The original command is already UPDATE-locked under the enrollment clock. Plain run/request reads are sufficient for a branch emitting no job/business mutation: seal cannot transition RECEIVED while that command is locked; committed expiry makes COMPLETE0 terminal. Existing service0560 already returns progressed when the enrollment clock advances with null job. Do not auto-expire from this enroller, skip a FAILED command, jump to the later expiry sequence, or admit an OPEN job to CLAIM.

## 3.0510 expiry acceptance — retire original RECEIVED with actual native counts

Signature `private.bpay_next_accept_preparation_expiry_v1(uuid,uuid,uuid,timestamptz,timestamptz)`. Add declaration:

```sql
v_prepare_command private.bpay_next_command%rowtype;
```

Keep original replay/auth/deadline/due/run locks21–66. Replace ONLY original binding validation67–70 with:

```sql
if v_prepare is not null then
  select c.* into strict v_prepare_command
    from private.bpay_next_command c where c.id=v_prepare;
  if v_prepare_command.status='RECEIVED' then
    -- Original run UPDATE is already held. Enroller RECEIVED takes no run lock.
    select c.* into strict v_prepare_command
      from private.bpay_next_command c where c.id=v_prepare for update;
    if v_prepare_command.command_kind is distinct from 'PREPARE'
       or v_prepare_command.module_epoch is distinct from v_epoch
       or v_prepare_command.status is distinct from 'RECEIVED'
       or v_prepare_command.expected_member_count is not null
       or v_prepare_command.enrolled_member_count<>0
       or v_prepare_command.sealed_at_utc is not null
       or v_run.status is distinct from 'PREPARING'
       or v_run.selection_state is distinct from 'OPEN'
       or v_run.accepted_choice_descriptor is null
       or v_run.accepted_choice_command_id is distinct from v_prepare
       or exists(select 1 from private.bpay_next_command_member cm
         where cm.command_id=v_prepare)
       or exists(select 1 from private.bpay_next_job j
         where j.command_id=v_prepare)
       or exists(select 1 from private.bpay_next_run_worker rw
         where rw.run_id=p_run_id) then
      raise exception using errcode='23514',
        message='BPAY_NEXT_EXPIRY_PREPARE_BINDING_INVALID';
    end if;
    update private.bpay_next_command
      set expected_member_count=0,status='COMPLETE',sealed_at_utc=v_now
      where id=v_prepare and status='RECEIVED';
    if not found then
      raise exception using errcode='23514',
        message='BPAY_NEXT_EXPIRY_PREPARE_BINDING_INVALID';
    end if;
  elsif not exists(select 1 from private.bpay_next_command
      where id=v_prepare and command_kind='PREPARE' and module_epoch=v_epoch
        and status in ('ENROLLING','SEALED')
        and expected_member_count=v_run.selected_candidate_count) then
    -- Original normal binding predicate, unchanged.
    raise exception using errcode='23514',
      message='BPAY_NEXT_EXPIRY_PREPARE_BINDING_INVALID';
  end if;
end if;
```

Original71–86 then receives the SAME existing PREPARATION_EXPIRY kind, retains original_prepare_command_id, copies actual native selection/page/review/Candidate counts, writes real CANCELLING/CANCELLED and real request counters. Never set native selected_candidate_count0 merely because the original PREPARE created no jobs. New accepted Candidate settings can exist before any upload; those actual Candidate identities are handled by the existing bounded zero-work expiry route. Existing due service0520/deterministic request identity requires no rewrite.

Expiry does not UPDATE enrollment_clock: it holds run/original command, opposite of enrollment. Only enroller section2 advances the next exact original sequence. No command_clock operation follows a new business read/lock: after the original receive call, retain only existing new-request/run/cmd bookkeeping already owned in0510.

## 4.0510 expiry enrollment — preserve NULL original job only for proved COMPLETE0

Signature `private.bpay_next_enroll_preparation_expiry_one_v1(uuid)`. Add declaration:

```sql
v_original_unenrolled boolean:=false;
```

Replace ONLY original125–133 with the following. All original request/run/expiry-command locks, native member ordinal/counts, actual expiry job creation and worker binding remain unchanged.

```sql
if v_request.original_prepare_command_id is not null then
  v_original_unenrolled:=
    v_run.selection_state='OPEN'
    and v_run.accepted_choice_descriptor is not null
    and v_run.accepted_choice_command_id=v_request.original_prepare_command_id
    and private.bpay_next_preparation_expiry_fenced_v1(
      v_run_id,v_request.original_prepare_command_id)
    and exists(select 1 from private.bpay_next_command c
      where c.id=v_request.original_prepare_command_id
        and c.command_kind='PREPARE' and c.module_epoch=v_epoch
        and c.status='COMPLETE' and c.expected_member_count=0
        and c.enrolled_member_count=0 and c.sealed_at_utc is not null)
    and not exists(select 1 from private.bpay_next_command_member cm
      where cm.command_id=v_request.original_prepare_command_id)
    and not exists(select 1 from private.bpay_next_job j
      where j.command_id=v_request.original_prepare_command_id)
    and not exists(select 1 from private.bpay_next_run_worker rw
      where rw.run_id=v_run_id);
  if not coalesce(v_original_unenrolled,false) then
    -- Complete original ordinary-job retirement block follows unchanged.
    select * into strict v_original from private.bpay_next_job
      where command_id=v_request.original_prepare_command_id
        and candidate_id=v_candidate and job_kind='PREPARE' for update;
    if v_original.module_epoch<>v_epoch then
      raise exception using errcode='23514',
        message='BPAY_NEXT_EXPIRY_ORIGINAL_JOB_INVALID';
    end if;
    if v_original.status<>'DONE' then
      update private.bpay_next_job set status='DONE',phase='EXPIRED',
        lease_nonce=null,lease_until_utc=null,
        last_error_code='BPAY_NEXT_PREPARATION_EXPIRED' where id=v_original.id;
    end if;
  end if;
end if;
```

`v_original` starts NULL by the original declaration and is not fabricated on the new path. Original134–141 already rejects a real run worker without actual original job and does not select allocation state for NULL worker. The existing zero-work cleanup paths, lease guard, counters and `apply_expired_week_v1:206` NULL-worker return remain unchanged. Actual partial Candidate membership is metadata being cancelled, not invented financial preparation.

## 5.0510 expiry guard — narrowly admit the nonnull-original/NULL-job tuple

Signature `private.bpay_next_preparation_expiry_guard_v1()`. Add declaration:

```sql
v_original_unenrolled boolean:=false;
```

Within INSERT worker branch, AFTER original request/run SELECT578–579 and BEFORE its existing large IF, compute:

```sql
v_original_unenrolled:=
  v_request.original_prepare_command_id is not null
  and new.original_job_id is null and new.run_worker_id is null
  and new.expected_work_count=0 and new.expected_case_hold_count=0
  and v_run.selection_state='OPEN'
  and v_run.accepted_choice_descriptor is not null
  and v_run.accepted_choice_command_id=v_request.original_prepare_command_id
  and private.bpay_next_preparation_expiry_fenced_v1(
    new.run_id,v_request.original_prepare_command_id)
  and exists(select 1 from private.bpay_next_command c
    join private.bpay_next_job expiry on expiry.id=new.job_id
      and expiry.module_epoch=c.module_epoch
    where c.id=v_request.original_prepare_command_id
      and c.command_kind='PREPARE' and c.status='COMPLETE'
      and c.expected_member_count=0 and c.enrolled_member_count=0
      and c.sealed_at_utc is not null)
  and not exists(select 1 from private.bpay_next_command_member cm
    where cm.command_id=v_request.original_prepare_command_id)
  and not exists(select 1 from private.bpay_next_job j
    where j.command_id=v_request.original_prepare_command_id)
  and not exists(select 1 from private.bpay_next_run_worker rw
    where rw.run_id=new.run_id);
```

Replace ONLY null-pair clause587:

```sql
or (((v_request.original_prepare_command_id is null)
       is distinct from (new.original_job_id is null))
    and not coalesce(v_original_unenrolled,false))
```

Every other original worker INSERT predicate580–594 stays unchanged. In particular actual run/request/Candidate/member/job binding and original_job nonnull validation remain. Existing DDL0501 already requires all original-job metadata NULL when original_job_id NULL, and all original-worker metadata NULL when run_worker_id NULL. The added branch explicitly requires expected work/Case counts0 rather than trusting nullable metadata. Do not relax any immutable UPDATE/DELETE, captured deadline or request/count guard.

The request INSERT guard568–576 need not alter original identity/count semantics: expiry acceptance retires the original before inserting the request, existing original FK remains, and original request checks still bind its full native scope/initial0 counters. If adding an owner-admission check for this new retired-original state, use the same exact OPEN/accepted-choice/COMPLETE0/no-members/no-jobs/no-workers predicate; do not require a new proof record or alter ordinary original-job state acceptance.

## Preserved boundaries / verification inputs

- Full original definitions are the successors' source: do not hand-rebuild unrelated CASE_WRITE_OFF/CASE_CREATE/POSITION/enrollment dispatch, expired financial release or Source semantics.
- SAME existing logical owners, security/search_path and ACLs. Enqueue remains private ungranted; enroller/expiry helpers retain private original grants. No new public surface.
- No change to RuleB CLAIM/PIN, ordinary PREPARE member/job creation, command kinds, clocks, financial balances, producer callbacks or parked files.
- Lifetime: module→run→command for new acceptance/expiry metadata; enrollment clock→command and PLAIN run for wait/retired branches; ordinary ENROLLING branch retains its existing locks. Never add run SHARE to RECEIVED/retired paths or enrollment-clock UPDATE to expiry acceptance.
- CREATE remains one immutable planned UUID/descriptor; nonempty acceptance has one existing command sequence. Empty acceptance has no command/run_command and cannot block the clock.
- The builder must independently compile and review the new full successors and actual manifest materialisers/typed callers. These source fragments do not constitute execution proof or whole WB21 completion.

## Original source raw hashes

| Original | SHA256 |
|---|---|
| `supabase/repeatable/03102026_1590_banking_pay_next_selection_manifest_v1.sql` | `47a0df6a06bd7816b0102c1aa5fca0bddd97ebc08a0a04d786254db3179967b3` |
| `supabase/repeatable/03102026_1430_banking_pay_next_position_writer_v1.sql` | `395ead5cf46a1399ef4859f056d89508b0862f9b0fda8dde92454107f17602fa` |
| `supabase/repeatable/04102026_0510_banking_pay_next_preparation_expiry_v1.sql` | `f4eb08c3c963c1850396642fd98947cf27722d96e20b6bb77db394635c3341aa` |
| `supabase/migrations/04102026_0501_banking_pay_next_preparation_expiry.sql` | `c9758121675a070ae40f1be352a40247f357309ae94efca59baf6de002cac845` |
| `supabase/repeatable/04102026_0560_banking_pay_next_queue_recovery_service_v1.sql` | `03daf223a8b572b37605f1ddc15fb777a3a543ff89140b8cff40238ec9a139fa` |
| `supabase/repeatable/04102026_0520_banking_pay_next_preparation_expiry_service_v1.sql` | `81ffc53b673cfb5c0d5fa352c4508f14ef347fd73582c8a09022f210f92c7b9d` |


## Current CREATE lock-order correction

The original 1236 PayRun INSERT guard already holds command-clock SHARE. Its acquisition is earlier than PREPARE receipt. Do not infer that the lifecycle snippets authorize later native parent/projection locks. See [WB-21-create-cut-lock-order.md](WB-21-create-cut-lock-order.md) for the actual publication and native Snooze inverse edges, implicit FK parents, SHARE-upgrade race and narrow early existing-clock UPDATE/single-snapshot recommendation. The enroller/expiry exact branch snippets above remain unchanged; this correction concerns fresh CREATE input capture only.
