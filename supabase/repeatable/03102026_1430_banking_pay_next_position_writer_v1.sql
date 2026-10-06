-- Repeatable authority for the replacement's exact-work approval handoff.
-- All entry points are owner-only until the old-writer fence is installed.
\set ON_ERROR_STOP on

begin;

create or replace function private.bpay_next_publish_staged_revision_v1(
  p_work_id uuid, p_revision_id uuid, p_command_id uuid
) returns bigint
language plpgsql
set search_path = pg_catalog, private
as $function$
declare
  v_work private.bpay_next_work%rowtype;
  v_revision private.bpay_next_work_revision%rowtype;
  v_predecessor uuid;
  v_sequence bigint;
  v_existing_work uuid;
  v_existing_revision uuid;
  v_physical_booking_id text;
  v_physical_version integer;
  v_physical_current boolean;
  v_physical_archived_at timestamptz;
  v_physical_revoked_at timestamptz;
begin
  if p_work_id is null or p_revision_id is null or p_command_id is null then
    raise exception using errcode='22023', message='BPAY_NEXT_PUBLICATION_INPUT_INVALID';
  end if;
  if (select active_owner from private.bpay_next_module_control
        where id=1 for share) <> 'NEXT' then
    raise exception using errcode='55000', message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select * into strict v_work from private.bpay_next_work
    where id=p_work_id for update;
  select p.work_id,p.revision_id,c.agency_sequence
    into v_existing_work,v_existing_revision,v_sequence
    from private.bpay_next_publication p
    join private.bpay_next_command c on c.id=p.command_id
    where p.command_id=p_command_id;
  if found then
    if (v_existing_work,v_existing_revision) is distinct from
       (p_work_id,p_revision_id) then
      raise exception using errcode='23514', message='BPAY_NEXT_PUBLICATION_REPLAY_CONFLICT';
    end if;
    return v_sequence;
  end if;
  select * into strict v_revision from private.bpay_next_work_revision
    where id=p_revision_id and work_id=p_work_id for update;
  select t.booking_id,t.version,t.is_current,t.archived_at_utc,t.revoked_at
    into v_physical_booking_id,v_physical_version,v_physical_current,
         v_physical_archived_at,v_physical_revoked_at
    from public.timesheets t where t.timesheet_id=v_revision.physical_timesheet_id;
  if v_revision.sealed_at_utc is not null
     or v_revision.revision_no <> v_work.current_revision_no+1
     or v_physical_booking_id is distinct from v_work.booking_id
     or v_physical_version is distinct from v_revision.physical_timesheet_version
     or v_physical_current is distinct from true
     or v_physical_archived_at is not null
     or v_physical_revoked_at is not null
     or v_revision.week_ending_date <> v_work.week_ending_date then
    raise exception using errcode='23514', message='BPAY_NEXT_PUBLICATION_REVISION_MISMATCH';
  end if;
  -- An approved immutable work revision retains this physical Timesheet.
  -- Tell the installed bounded removal preview that Archive, not permanent
  -- Delete, is the available lifecycle route. One exact identity is marked;
  -- there is no version-family or Candidate-history scan.
  perform public.timesheet_financial_retention_mark_v1(
    array[v_revision.physical_timesheet_id]);
  select revision_id into v_predecessor
    from private.bpay_next_publication
    where work_id=p_work_id order by revision_no desc limit 1;
  if v_work.current_revision_no>0 and v_predecessor is null then
    raise exception using errcode='23514', message='BPAY_NEXT_PUBLICATION_PREDECESSOR_MISSING';
  end if;
  update private.bpay_next_work_revision
    set approved_at_utc=pg_catalog.transaction_timestamp(),
        sealed_at_utc=pg_catalog.transaction_timestamp()
    where id=p_revision_id;
  update private.bpay_next_work
    set current_revision_id=p_revision_id,
        current_revision_no=v_revision.revision_no,
        approval_state='APPROVED',
        updated_at_utc=pg_catalog.transaction_timestamp()
    where id=p_work_id;

  -- Clock receipt is deliberately last in the upstream lock chain. Failure
  -- rolls back the pointer, sealed revision, member and command together.
  v_sequence := private.bpay_next_receive_command_v1(p_command_id,'POSITION_APPLY');
  insert into private.bpay_next_command_member
    (command_id,candidate_id,member_no)
    values (p_command_id,v_work.candidate_id,1);
  insert into private.bpay_next_publication
    (command_id,work_id,candidate_id,revision_id,predecessor_revision_id,revision_no)
    values (p_command_id,p_work_id,v_work.candidate_id,p_revision_id,
            v_predecessor,v_revision.revision_no);
  update private.bpay_next_command
    set expected_member_count=1,status='SEALED',
        sealed_at_utc=pg_catalog.transaction_timestamp()
    where id=p_command_id;
  return v_sequence;
end
$function$;

-- One enroller can run at a time; it cannot skip a prior unsealed command.
-- No worker, work, Timesheet or Source row is locked here.
create or replace function private.bpay_next_enroll_one_v1()
returns uuid
language plpgsql
set search_path = pg_catalog, private
as $function$
declare
  v_last bigint;
  v_command private.bpay_next_command%rowtype;
  v_member private.bpay_next_command_member%rowtype;
  v_job_id uuid;
  v_epoch bigint;
  v_run_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_selected_candidate uuid;
  v_expired_preparation boolean;
begin
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then
    raise exception using errcode='55000', message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select last_enrolled_sequence into strict v_last
    from private.bpay_next_enrollment_clock where id=1 for update;
  select * into v_command from private.bpay_next_command
    where agency_sequence=v_last+1 for update;
  if not found then
    return null;
  end if;
  if v_command.command_kind='PREPARE' then
    select rc.run_id into strict v_run_id
      from private.bpay_next_run_command rc
      where rc.command_id=v_command.id;
    select * into strict v_run from private.bpay_next_pay_run
      where id=v_run_id for share;
    v_expired_preparation:=private.bpay_next_preparation_expiry_fenced_v1(
      v_run_id,v_command.id);
    if v_command.module_epoch<>v_epoch
       or v_command.status<>'ENROLLING'
       or (v_run.status<>'PREPARING' and not
           (v_run.status='CANCELLING' and v_expired_preparation))
       or v_run.selection_state<>'SEALED'
       or v_command.expected_member_count<>v_run.selected_candidate_count
       or v_command.enrolled_member_count>=v_command.expected_member_count then
      raise exception using errcode='23514',
        message='BPAY_NEXT_PREPARE_ENROLLMENT_INVALID';
    end if;
    select sc.candidate_id into strict v_selected_candidate
      from private.bpay_next_selection_candidate sc
      where sc.run_id=v_run_id
        and sc.member_no=v_command.enrolled_member_count+1;
    insert into private.bpay_next_command_member
      (command_id,candidate_id,member_no)
      values(v_command.id,v_selected_candidate,
             v_command.enrolled_member_count+1);
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,
       status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,
             v_selected_candidate,'PREPARE',
             case when v_expired_preparation then 'DONE' else 'READY' end,
             case when v_expired_preparation then 'EXPIRED' else 'NEW' end,1)
      returning id into v_job_id;
    update private.bpay_next_command
      set enrolled_member_count=enrolled_member_count+1,
          status=case when enrolled_member_count+1=expected_member_count
            then 'SEALED' else 'ENROLLING' end,
          sealed_at_utc=case when enrolled_member_count+1=expected_member_count
            then pg_catalog.transaction_timestamp() else null end
      where id=v_command.id;
    if v_command.enrolled_member_count+1=v_command.expected_member_count then
      update private.bpay_next_enrollment_clock
        set last_enrolled_sequence=v_command.agency_sequence where id=1;
    end if;
    return v_job_id;
  end if;
  if v_command.command_kind='PREPARATION_EXPIRY' then
    -- Exact header fence permits only metadata enrollment here. The helper
    -- retires unfinished original PREPARE jobs and binds ONE cleanup member;
    -- financial holds are released later by its normally ordered worker.
    -- The helper, not this caller, owns member counts and command sealing.
    v_job_id:=private.bpay_next_enroll_preparation_expiry_one_v1(v_command.id);
    if exists(select 1 from private.bpay_next_command c
        where c.id=v_command.id
          and c.enrolled_member_count=c.expected_member_count
          and c.status in ('SEALED','COMPLETE')) then
      update private.bpay_next_enrollment_clock
        set last_enrolled_sequence=v_command.agency_sequence where id=1;
    end if;
    return v_job_id;
  end if;
  if v_command.status <> 'SEALED' then
    return null;
  end if;
  if v_command.command_kind='CASE_WRITE_OFF' then
    if v_command.module_epoch<>v_epoch or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_ENROLLMENT_INVALID';
    end if;
    select * into strict v_member from private.bpay_next_command_member
      where command_id=v_command.id and member_no=1;
    if not exists(select 1 from private.bpay_next_write_off_request r
       where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id and r.status='REQUESTED') then
      raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_REQUEST_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,v_member.candidate_id,
        'CASE_WRITE_OFF','READY','NEW',1) returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1 where id=v_command.id;
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind='CASE_CREATE' then
    if v_command.module_epoch<>v_epoch or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_CREATE_ENROLLMENT_INVALID';
    end if;
    select * into strict v_member from private.bpay_next_command_member
      where command_id=v_command.id and member_no=1;
    if not exists(select 1 from private.bpay_next_case_create_request r
       where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id) then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_CREATE_REQUEST_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,v_member.candidate_id,
        'CASE_CREATE','READY','NEW',1) returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1 where id=v_command.id;
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind='REISSUE_CANCEL' then
    if v_command.module_epoch<>v_epoch or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_REISSUE_CANCEL_ENROLLMENT_INVALID';
    end if;
    select * into strict v_member from private.bpay_next_command_member
      where command_id=v_command.id and member_no=1;
    if not exists(select 1 from private.bpay_next_reissue_cancel_request r
       where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id and r.status='REQUESTED') then
      raise exception using errcode='23514',message='BPAY_NEXT_REISSUE_CANCEL_REQUEST_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,v_member.candidate_id,
        'REISSUE_CANCEL','READY','NEW',1) returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1 where id=v_command.id;
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind in ('CSV_SETTLEMENT','CSV_RETURN','CASH_REISSUE','INTERNAL_SETTLEMENT') then
    if v_command.module_epoch<>v_epoch
       or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',
        message='BPAY_NEXT_OUTCOME_ENROLLMENT_INVALID';
    end if;
    select * into strict v_member from private.bpay_next_command_member
      where command_id=v_command.id and member_no=1;
    if v_command.command_kind='INTERNAL_SETTLEMENT' and not exists(select 1 from private.bpay_next_outcome_request r
                  where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id
                    and r.outcome_id is null and r.internal_receipt_id is not null)
       or v_command.command_kind='CSV_SETTLEMENT' and not exists(select 1 from private.bpay_next_outcome_request r
                  where r.command_id=v_command.id
                    and r.candidate_id=v_member.candidate_id
                    and r.outcome_id is not null and r.internal_receipt_id is null)
       or v_command.command_kind='CSV_RETURN' and not exists(select 1 from private.bpay_next_return_request r
                  where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id)
       or v_command.command_kind='CASH_REISSUE' and not exists(select 1 from private.bpay_next_reissue_request r
                  where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id) then
      raise exception using errcode='23514',
        message='BPAY_NEXT_OUTCOME_REQUEST_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,
       status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,
             v_member.candidate_id,v_command.command_kind,'READY','MEMBERS',1)
      returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1
      where id=v_command.id;
    update private.bpay_next_enrollment_clock
      set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind='SIMPLE_CANCEL' then
    if v_command.module_epoch<>v_epoch or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_CANCEL_ENROLLMENT_INVALID';
    end if;
    select * into strict v_member from private.bpay_next_command_member
      where command_id=v_command.id and member_no=1;
    if not exists(select 1 from private.bpay_next_cancel_request r
      where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id
        and r.status='REQUESTED') then
      raise exception using errcode='23514',message='BPAY_NEXT_CANCEL_REQUEST_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,v_member.candidate_id,
        'SIMPLE_CANCEL','READY','NEW',1) returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1 where id=v_command.id;
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind='PAYE_NET_ENTRY' then
    if v_command.module_epoch<>v_epoch
       or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',
        message='BPAY_NEXT_PAYE_NET_ENROLLMENT_INVALID';
    end if;
    select * into strict v_member from private.bpay_next_command_member
      where command_id=v_command.id and member_no=1;
    if not exists(select 1 from private.bpay_next_paye_net_request r
                  where r.command_id=v_command.id
                    and r.candidate_id=v_member.candidate_id) then
      raise exception using errcode='23514',
        message='BPAY_NEXT_PAYE_NET_REQUEST_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,
       status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,
             v_member.candidate_id,'PAYE_NET_ENTRY','READY',
             case when exists(select 1 from private.bpay_next_paye_net_request r
               where r.command_id=v_command.id and r.case_draft_state_id is not null)
               then 'FINANCE_ALLOCATE' else 'NEW' end,1)
      returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1
      where id=v_command.id;
    update private.bpay_next_enrollment_clock
      set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind='TRANSFER_BUILD' then
    if v_command.module_epoch<>v_epoch
       or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',
        message='BPAY_NEXT_TRANSFER_ENROLLMENT_INVALID';
    end if;
    select * into strict v_member from private.bpay_next_command_member
      where command_id=v_command.id and member_no=1;
    if not exists(select 1 from private.bpay_next_transfer t
                  where t.build_command_id=v_command.id
                    and t.candidate_id=v_member.candidate_id
                    and t.status='BUILDING') then
      raise exception using errcode='23514',
        message='BPAY_NEXT_TRANSFER_BUILD_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,
       status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,
             v_member.candidate_id,'TRANSFER_BUILD','READY','WORK',1)
      returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1
      where id=v_command.id;
    update private.bpay_next_enrollment_clock
      set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.module_epoch<>v_epoch
     or v_command.command_kind<>'POSITION_APPLY'
     or v_command.expected_member_count<>1
     or v_command.enrolled_member_count<>0 then
    raise exception using errcode='23514', message='BPAY_NEXT_ENROLLMENT_COMMAND_INVALID';
  end if;
  select * into strict v_member from private.bpay_next_command_member
    where command_id=v_command.id and member_no=1;
  if not exists (select 1 from private.bpay_next_publication p
                 where p.command_id=v_command.id and p.candidate_id=v_member.candidate_id) then
    raise exception using errcode='23514', message='BPAY_NEXT_ENROLLMENT_PUBLICATION_MISSING';
  end if;
  insert into private.bpay_next_job
    (command_id,command_sequence,module_epoch,candidate_id,job_kind,
     status,phase,owner_epoch)
    values (v_command.id,v_command.agency_sequence,v_epoch,
            v_member.candidate_id,'POSITION_APPLY','READY','NEW',1)
    returning id into v_job_id;
  update private.bpay_next_command set enrolled_member_count=1
    where id=v_command.id;
  update private.bpay_next_enrollment_clock
    set last_enrolled_sequence=v_command.agency_sequence where id=1;
  return v_job_id;
end
$function$;

-- Claim after enrollment, in per-candidate order. A later job may be enrolled
-- but cannot claim while an earlier job for that candidate is unfinished.
create or replace function private.bpay_next_claim_position_job_v1(
  p_job_id uuid, p_lease_seconds integer default 120
) returns table(lease_nonce uuid, owner_epoch bigint)
language plpgsql
set search_path = pg_catalog, private
as $function$
declare
  v_candidate uuid;
  v_job private.bpay_next_job%rowtype;
  v_epoch bigint;
  v_owner_epoch bigint;
  v_nonce uuid;
begin
  if p_job_id is null or p_lease_seconds not between 1 and 120 then
    raise exception using errcode='22023', message='BPAY_NEXT_LEASE_INPUT_INVALID';
  end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then
    raise exception using errcode='55000', message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select candidate_id into strict v_candidate from private.bpay_next_job
    where id=p_job_id;
  insert into private.bpay_next_worker_control(candidate_id)
    values (v_candidate) on conflict (candidate_id) do nothing;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_candidate for update;
  select * into strict v_job from private.bpay_next_job
    where id=p_job_id for update;
  if v_job.module_epoch<>v_epoch or v_job.job_kind<>'POSITION_APPLY'
     or v_job.status not in ('READY','LEASED')
     or (v_job.status='LEASED' and v_job.lease_until_utc>pg_catalog.clock_timestamp()) then
    raise exception using errcode='55000', message='BPAY_NEXT_JOB_NOT_CLAIMABLE';
  end if;
  if exists (select 1 from private.bpay_next_job earlier
             where earlier.candidate_id=v_candidate
               and earlier.command_sequence<v_job.command_sequence
               and earlier.status<>'DONE') then
    raise exception using errcode='55000', message='BPAY_NEXT_EARLIER_WORKER_COMMAND_PENDING';
  end if;
  update private.bpay_next_worker_control
    set active_owner_epoch=active_owner_epoch+1,
        updated_at_utc=pg_catalog.transaction_timestamp()
    where candidate_id=v_candidate returning active_owner_epoch into v_owner_epoch;
  v_nonce := pg_catalog.gen_random_uuid();
  update private.bpay_next_job
    set status='LEASED',owner_epoch=v_owner_epoch,lease_nonce=v_nonce,
        lease_until_utc=pg_catalog.clock_timestamp()+pg_catalog.make_interval(secs=>p_lease_seconds),
        attempt_count=attempt_count+1
    where id=p_job_id;
  lease_nonce:=v_nonce;
  owner_epoch:=v_owner_epoch;
  return next;
end
$function$;

-- One bounded step. The work pointer remains pending throughout intermediate
-- pages, including the separate current-position removal pass. Readers must
-- refuse amounts until current_revision_id equals applied_revision_id.
create or replace function private.bpay_next_apply_position_page_v1(
  p_job_id uuid, p_lease_nonce uuid, p_owner_epoch bigint,
  p_expected_phase text, p_expected_cursor text, p_limit integer default 128
) returns table(next_phase text,next_cursor text,rows_visited integer,done boolean)
language plpgsql
set search_path = pg_catalog, private
as $function$
declare
  v_candidate uuid;
  v_job private.bpay_next_job%rowtype;
  v_publication private.bpay_next_publication%rowtype;
  v_work private.bpay_next_work%rowtype;
  v_source_pay_channel text;
  v_epoch bigint;
  v_row record;
  v_seen integer:=0;
  v_cursor text;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null
     or p_limit not between 1 and 256 then
    raise exception using errcode='22023', message='BPAY_NEXT_POSITION_STEP_INPUT_INVALID';
  end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then
    raise exception using errcode='55000', message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select candidate_id into strict v_candidate from private.bpay_next_job
    where id=p_job_id;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_candidate for update;
  select * into strict v_job from private.bpay_next_job
    where id=p_job_id for update;
  -- A lost final page reply is an exact terminal readback, not a second
  -- position write. A later approval may already have changed the WORK;
  -- retain this publication's own completed identity rather than inspecting
  -- or rebuilding the latest revision. No lease authority is adopted.
  if v_job.job_kind='POSITION_APPLY' and v_job.module_epoch=v_epoch
     and v_job.status='DONE' and v_job.phase='DONE'
     and exists(select 1 from private.bpay_next_publication p
       where p.command_id=v_job.command_id and p.candidate_id=v_candidate
         and p.status='APPLIED' and p.phase='DONE') then
    next_phase:='DONE';next_cursor:=null;rows_visited:=0;done:=true;
    return next;return;
  end if;
  if v_job.status<>'LEASED' or v_job.lease_nonce<>p_lease_nonce
     or v_job.owner_epoch<>p_owner_epoch or v_job.module_epoch<>v_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp() then
    raise exception using errcode='55000', message='BPAY_NEXT_POSITION_LEASE_STALE';
  end if;
  if v_job.phase is distinct from p_expected_phase
     or v_job.cursor_key is distinct from p_expected_cursor then
    -- A lost response is a readback, never a repeated financial write.
    next_phase:=v_job.phase; next_cursor:=v_job.cursor_key;
    rows_visited:=0; done:=false; return next;
    return;
  end if;
  select * into strict v_publication from private.bpay_next_publication
    where command_id=v_job.command_id for update;
  select * into strict v_work from private.bpay_next_work
    where id=v_publication.work_id for update;
  if v_publication.candidate_id<>v_candidate
     or v_publication.status='APPLIED'
     or v_work.applied_revision_id is distinct from v_publication.predecessor_revision_id then
    raise exception using errcode='55000', message='BPAY_NEXT_POSITION_PREDECESSOR_MISMATCH';
  end if;
  select r.source_pay_channel into strict v_source_pay_channel
    from private.bpay_next_work_revision r
    where r.id=v_publication.revision_id and r.work_id=v_work.id;
  v_cursor:=v_job.cursor_key;
  if v_job.phase='NEW' then
    for v_row in
      select l.component_key,l.source_pay_ex_vat
        from private.bpay_next_approved_line l
        where l.revision_id=v_publication.revision_id
          and (v_cursor is null or l.component_key>v_cursor)
        order by l.component_key limit p_limit
    loop
      insert into private.bpay_next_position
        (work_id,component_key,applied_revision_id,
         source_basis_channel,approved_source_ex_vat)
        values (v_work.id,v_row.component_key,v_publication.revision_id,
                v_source_pay_channel,v_row.source_pay_ex_vat)
      on conflict (work_id,component_key) do update
        set applied_revision_id=excluded.applied_revision_id,
            source_basis_channel=case
              when private.bpay_next_position.realised_source_ex_vat<>0
                or private.bpay_next_position.held_source_ex_vat<>0
                or private.bpay_next_position.realised_target_ex_vat<>0
                or private.bpay_next_position.realised_target_vat<>0
                or private.bpay_next_position.realised_target_inc_vat<>0
                or private.bpay_next_position.held_target_ex_vat<>0
                or private.bpay_next_position.held_target_vat<>0
                or private.bpay_next_position.held_target_inc_vat<>0
                then private.bpay_next_position.source_basis_channel
              else excluded.source_basis_channel end,
            approved_source_ex_vat=excluded.approved_source_ex_vat,
            updated_at_utc=pg_catalog.transaction_timestamp();
      perform private.bpay_next_reconcile_work_collection_v1(p_job_id,v_work.id,v_row.component_key);
      v_cursor:=v_row.component_key;
      v_seen:=v_seen+1;
    end loop;
    if v_seen<p_limit then
      if v_job.applied_line_count+v_seen <>
         (select r.expected_line_count
            from private.bpay_next_work_revision r
            where r.id=v_publication.revision_id) then
        raise exception using errcode='23514',
          message='BPAY_NEXT_POSITION_APPROVED_LINE_COUNT_MISMATCH';
      end if;
      update private.bpay_next_publication
        set status='APPLYING',phase='REMOVED',cursor_key=null
        where command_id=v_publication.command_id;
      update private.bpay_next_job
        set phase='REMOVED',cursor_key=null,
            applied_line_count=applied_line_count+v_seen
        where id=p_job_id;
      next_phase:='REMOVED'; next_cursor:=null;
    else
      update private.bpay_next_publication
        set status='APPLYING',cursor_key=v_cursor
        where command_id=v_publication.command_id;
      update private.bpay_next_job
        set cursor_key=v_cursor,
            applied_line_count=applied_line_count+v_seen
        where id=p_job_id;
      next_phase:='NEW'; next_cursor:=v_cursor;
    end if;
  elsif v_job.phase='REMOVED' then
    for v_row in
      select p.component_key from private.bpay_next_position p
        where p.work_id=v_work.id
          and (p.approved_source_ex_vat<>0
            or p.realised_source_ex_vat<>0 or p.held_source_ex_vat<>0
            or p.realised_target_ex_vat<>0 or p.realised_target_vat<>0
            or p.realised_target_inc_vat<>0
            or p.held_target_ex_vat<>0 or p.held_target_vat<>0
            or p.held_target_inc_vat<>0)
          and (v_cursor is null or p.component_key>v_cursor)
        order by p.component_key limit p_limit
    loop
      if not exists (
        select 1 from private.bpay_next_approved_line l
          where l.revision_id=v_publication.revision_id
            and l.component_key=v_row.component_key
      ) then
        update private.bpay_next_position
          set applied_revision_id=v_publication.revision_id,
              approved_source_ex_vat=0,
              updated_at_utc=pg_catalog.transaction_timestamp()
          where work_id=v_work.id and component_key=v_row.component_key;
        perform private.bpay_next_reconcile_work_collection_v1(p_job_id,v_work.id,v_row.component_key);
      end if;
      v_cursor:=v_row.component_key;
      v_seen:=v_seen+1;
    end loop;
    if v_seen<p_limit then
      update private.bpay_next_publication
        set status='APPLIED',phase='DONE',cursor_key=null,
            applied_at_utc=pg_catalog.transaction_timestamp()
        where command_id=v_publication.command_id;
      update private.bpay_next_work
        set applied_revision_id=v_publication.revision_id,
            updated_at_utc=pg_catalog.transaction_timestamp()
        where id=v_work.id;
      update private.bpay_next_job
        set status='DONE',phase='DONE',cursor_key=null,
            lease_nonce=null,lease_until_utc=null
        where id=p_job_id;
      update private.bpay_next_command set status='COMPLETE'
        where id=v_publication.command_id;
      next_phase:='DONE'; next_cursor:=null;
    else
      update private.bpay_next_publication
        set status='APPLYING',cursor_key=v_cursor
        where command_id=v_publication.command_id;
      update private.bpay_next_job set cursor_key=v_cursor where id=p_job_id;
      next_phase:='REMOVED'; next_cursor:=v_cursor;
    end if;
  else
    raise exception using errcode='23514', message='BPAY_NEXT_POSITION_PHASE_INVALID';
  end if;
  if v_seen>0 or next_phase='DONE' then
    update private.bpay_next_worker_control
      set financial_view_revision=financial_view_revision+1,
          updated_at_utc=pg_catalog.transaction_timestamp()
      where candidate_id=v_candidate;
  end if;
  rows_visited:=v_seen;
  done:=(next_phase='DONE');
  return next;
end
$function$;

create or replace function private.bpay_next_work_payable_state_v1(p_work_id uuid)
returns text
language sql stable
set search_path = pg_catalog, private
as $function$
  select case when w.approval_state<>'APPROVED' then 'NOT_APPROVED'
              when w.applied_revision_id is distinct from w.current_revision_id
                then 'APPLY_PENDING'
              else 'READY' end
    from private.bpay_next_work w where w.id=p_work_id
$function$;

alter function private.bpay_next_publish_staged_revision_v1(uuid,uuid,uuid) owner to postgres;
alter function private.bpay_next_enroll_one_v1() owner to postgres;
alter function private.bpay_next_claim_position_job_v1(uuid,integer) owner to postgres;
alter function private.bpay_next_apply_position_page_v1(uuid,uuid,bigint,text,text,integer) owner to postgres;
alter function private.bpay_next_work_payable_state_v1(uuid) owner to postgres;
revoke all on function
  private.bpay_next_publish_staged_revision_v1(uuid,uuid,uuid),
  private.bpay_next_enroll_one_v1(),
  private.bpay_next_claim_position_job_v1(uuid,integer),
  private.bpay_next_apply_position_page_v1(uuid,uuid,bigint,text,text,integer),
  private.bpay_next_work_payable_state_v1(uuid)
  from public, anon, authenticated, service_role;

commit;
