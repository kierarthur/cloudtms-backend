-- F-25 accepted-input successors; original leases, enrollment order and
-- all unchanged current product branches retained. No frozen-row rewrite.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_claim_prepare_job_v1(
  p_job_id uuid,p_lease_seconds integer default 120
) returns table(lease_nonce uuid,owner_epoch bigint)
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_job private.bpay_next_job%rowtype;
  v_run private.bpay_next_pay_run%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_run_id uuid;
  v_module_epoch bigint;
  v_channel text;
  v_umbrella_id uuid;
  v_umbrella_vat_chargeable boolean;
  v_owner_epoch bigint;
  v_nonce uuid;
begin
  if p_job_id is null or p_lease_seconds not between 1 and 120 then
    raise exception using errcode='22023',
      message='BPAY_NEXT_PREPARE_LEASE_INPUT_INVALID';
  end if;
  select m.owner_epoch into v_module_epoch
    from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_module_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select j.candidate_id,rc.run_id into strict v_job.candidate_id,v_run_id
    from private.bpay_next_job j
    join private.bpay_next_run_command rc on rc.command_id=j.command_id
    where j.id=p_job_id and j.job_kind='PREPARE';
  select * into strict v_run from private.bpay_next_pay_run
    where id=v_run_id for share;
  insert into private.bpay_next_worker_control(candidate_id)
    values(v_job.candidate_id) on conflict(candidate_id) do nothing;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_job.candidate_id for update;
  select * into strict v_job from private.bpay_next_job
    where id=p_job_id for update;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED'
     or v_job.job_kind<>'PREPARE' or v_job.module_epoch<>v_module_epoch
     or not exists(select 1 from private.bpay_next_command command
       where command.id=v_job.command_id and command.agency_sequence=v_job.command_sequence
         and command.module_epoch=v_module_epoch and command.command_kind='PREPARE'
         and command.status='SEALED' and command.sealed_at_utc is not null
         and command.expected_member_count=v_run.selected_candidate_count
         and command.enrolled_member_count=command.expected_member_count)
     or v_job.status not in ('READY','LEASED')
     or (v_job.status='LEASED'
       and v_job.lease_until_utc>pg_catalog.clock_timestamp()) then
    raise exception using errcode='55000',
      message='BPAY_NEXT_PREPARE_JOB_NOT_CLAIMABLE';
  end if;
  if exists(select 1 from private.bpay_next_job earlier
      where earlier.candidate_id=v_job.candidate_id
        and earlier.command_sequence<v_job.command_sequence
        and earlier.status<>'DONE') then
    raise exception using errcode='55000',
      message='BPAY_NEXT_EARLIER_WORKER_COMMAND_PENDING';
  end if;
  select * into v_worker from private.bpay_next_run_worker
    where run_id=v_run_id and candidate_id=v_job.candidate_id for update;
  if not found then
    select i.target_pay_channel,i.target_umbrella_id,i.umbrella_vat_chargeable
      into strict v_channel,v_umbrella_id,v_umbrella_vat_chargeable
      from private.bpay_next_accepted_input i
      where i.prepare_command_id=v_job.command_id and i.candidate_id=v_job.candidate_id
        and i.run_id=v_run_id and i.accepted_sequence=v_job.command_sequence and i.module_epoch=v_job.module_epoch;
    if v_channel not in ('PAYE','UMBRELLA') then
      raise exception using errcode='23514',
        message='BPAY_NEXT_PREPARE_PAY_CHANNEL_UNSUPPORTED';
    end if;
    insert into private.bpay_next_run_worker
      (run_id,candidate_id,target_pay_channel,target_umbrella_id,
       target_umbrella_vat_chargeable,status)
      values(v_run_id,v_job.candidate_id,v_channel,
        case when v_channel='UMBRELLA' then v_umbrella_id else null end,
        case when v_channel='UMBRELLA' then v_umbrella_vat_chargeable
          else null end,'PREPARING')
      returning * into v_worker;
  elsif v_worker.status<>'PREPARING' then
    raise exception using errcode='55000',
      message='BPAY_NEXT_PREPARE_WORKER_STATE_CHANGED';
  end if;
  update private.bpay_next_worker_control
    set active_owner_epoch=active_owner_epoch+1,
        updated_at_utc=pg_catalog.transaction_timestamp()
    where candidate_id=v_job.candidate_id
    returning active_owner_epoch into v_owner_epoch;
  v_nonce:=pg_catalog.gen_random_uuid();
  update private.bpay_next_job
    set status='LEASED',owner_epoch=v_owner_epoch,lease_nonce=v_nonce,
        lease_until_utc=pg_catalog.clock_timestamp()
          +pg_catalog.make_interval(secs=>p_lease_seconds),
        attempt_count=attempt_count+1
    where id=p_job_id;
  lease_nonce:=v_nonce;owner_epoch:=v_owner_epoch;
  return next;
end
$function$;


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
  if v_command.command_kind='INPUT_VERSION' then
    if v_command.module_epoch<>v_epoch or v_command.status<>'SEALED'
      or v_command.expected_member_count<>0 or v_command.enrolled_member_count<>0
      or v_command.sealed_at_utc is null
      or (select count(*) from private.bpay_next_input_version_receipt r
        join private.bpay_next_input_version_event e on e.id=r.event_id
        where r.command_id=v_command.id and r.agency_sequence=v_command.agency_sequence
          and r.module_epoch=v_command.module_epoch and e.module_epoch=r.module_epoch)<>1
      or exists(select 1 from private.bpay_next_command_member m where m.command_id=v_command.id)
      or exists(select 1 from private.bpay_next_job j where j.command_id=v_command.id) then
      raise exception using errcode='23514',message='BPAY_NEXT_INPUT_VERSION_ENROLLMENT_INVALID';
    end if;
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence
      where id=1 and last_enrolled_sequence=v_command.agency_sequence-1;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_INPUT_VERSION_ORDER_CHANGED';end if;
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
    -- Acceptance already retained immutable member identities, not jobs.
    select m.* into strict v_member from private.bpay_next_command_member m
      where m.command_id=v_command.id and m.candidate_id=v_selected_candidate;
    if v_member.member_no<>v_command.enrolled_member_count+1 or not exists(
      select 1 from private.bpay_next_accepted_input i where i.prepare_command_id=v_command.id
        and i.candidate_id=v_selected_candidate and i.run_id=v_run_id
        and i.accepted_sequence=v_command.agency_sequence and i.module_epoch=v_command.module_epoch) then
      raise exception using errcode='23514',message='BPAY_NEXT_PREPARE_ACCEPTED_MEMBER_MISMATCH';
    end if;
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


create or replace function private.bpay_next_pin_selected_work_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,
  p_expected_cursor bigint
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private
as $function$
declare
  v_job private.bpay_next_job%rowtype;
  v_run_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_worker_id uuid;
  v_worker private.bpay_next_run_worker%rowtype;
  v_selection private.bpay_next_run_selection%rowtype;
  v_choice private.bpay_next_work_choice%rowtype;
  v_work private.bpay_next_work%rowtype;
  v_revision private.bpay_next_work_revision%rowtype;
  v_current_cursor bigint;
  v_module_epoch bigint;
  v_cut_id uuid;
  v_accepted_work private.bpay_next_accepted_work_input%rowtype;
  v_source_classified boolean;
  v_source_origin record;
  v_query_gate jsonb;
  v_query_scope jsonb;
  v_query_disposition text;
  v_query_digest bytea;
  v_run_work_id uuid;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null
     or (p_expected_cursor is not null and p_expected_cursor<1) then
    raise exception using errcode='22023',
      message='BPAY_NEXT_PIN_INPUT_INVALID';
  end if;
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id and j.job_kind='PREPARE';
  select rc.run_id into strict v_run_id from private.bpay_next_run_command rc where rc.command_id=v_job.command_id;
  -- Accepted query authority is retained. Delayed PIN never admits or reads
  -- a fresh Source gate/current authorisation on a correctly accepted cut.
  select m.owner_epoch into v_module_epoch
    from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_module_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select * into strict v_run from private.bpay_next_pay_run
    where id=v_run_id for share;
  select candidate_id into strict v_job.candidate_id
    from private.bpay_next_job where id=p_job_id;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_job.candidate_id for update;
  select * into strict v_job from private.bpay_next_job
    where id=p_job_id for update;
  if v_job.status='DONE' and v_job.phase='DONE'
     and v_job.job_kind='PREPARE' and v_job.owner_epoch=p_owner_epoch then
    select * into strict v_worker from private.bpay_next_run_worker
      where run_id=v_run_id and candidate_id=v_job.candidate_id;
    if v_worker.status='REVIEW' and v_worker.review_issue_code is not null then
      return pg_catalog.jsonb_build_object('done',true,'phase','REVIEW',
        'issue_code',v_worker.review_issue_code,
        'work_id',v_worker.review_issue_work_id,'replay',true);
    end if;
  end if;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED'
     or v_job.status<>'LEASED' or v_job.job_kind<>'PREPARE'
     or v_job.module_epoch<>v_module_epoch
     or v_job.lease_nonce<>p_lease_nonce
     or v_job.owner_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp() then
    raise exception using errcode='55000',message='BPAY_NEXT_PIN_LEASE_STALE';
  end if;
  if v_job.phase='RESERVE' then
    return pg_catalog.jsonb_build_object('done',true,'phase','RESERVE',
      'replay',true);
  end if;
  if v_job.phase not in ('NEW','PINNING') then
    raise exception using errcode='23514',message='BPAY_NEXT_PIN_PHASE_INVALID';
  end if;
  v_current_cursor:=v_job.cursor_key::bigint;
  if v_current_cursor is distinct from p_expected_cursor then
    return pg_catalog.jsonb_build_object('done',false,'phase',v_job.phase,
      'next_cursor',v_current_cursor,'replay',true);
  end if;
  select * into strict v_worker from private.bpay_next_run_worker
    where run_id=v_run_id and candidate_id=v_job.candidate_id for update;
  v_worker_id:=v_worker.id;
  select * into v_selection from private.bpay_next_run_selection
    where run_id=v_run_id and candidate_id=v_job.candidate_id
      and selection_no>coalesce(v_current_cursor,0)
    order by selection_no limit 1;
  if not found then
    update private.bpay_next_job set phase='RESERVE',cursor_key=null
      where id=p_job_id;
    return pg_catalog.jsonb_build_object('done',true,'phase','RESERVE',
      'replay',false);
  end if;
  select * into strict v_work from private.bpay_next_work
    where id=v_selection.work_id for share;
  if v_work.candidate_id<>v_job.candidate_id then
    raise exception using errcode='23514',
      message='BPAY_NEXT_PIN_CANDIDATE_MISMATCH';
  end if;
  select * into strict v_choice from private.bpay_next_work_choice
    where run_id=v_run_id and work_id=v_work.id;
  if v_choice.selection_state<>'SEALED' then
    raise exception using errcode='55000',
      message='BPAY_NEXT_SELECTION_COMPONENT_CHOICES_NOT_SEALED';
  end if;
  v_cut_id:=private.bpay_next_seal_effective_work_cut_v1(p_job_id,p_lease_nonce,p_owner_epoch,v_work.id);
  select i.* into strict v_accepted_work from private.bpay_next_effective_work_cut cut
    join private.bpay_next_accepted_work_input i on i.id=cut.accepted_work_input_id
    where cut.id=v_cut_id and cut.prepare_job_id=p_job_id and cut.work_id=v_work.id;
  select r.* into strict v_revision from private.bpay_next_work_revision r
    where r.id=v_accepted_work.effective_revision_id and r.work_id=v_work.id for key share;
  v_query_disposition:=v_accepted_work.query_disposition;
  v_query_scope:=v_accepted_work.query_scope;
  v_query_digest:=v_accepted_work.query_state_sha256;
  insert into private.bpay_next_run_work
    (run_worker_id,candidate_id,work_id,captured_revision_id)
    values(v_worker_id,v_job.candidate_id,v_work.id,v_revision.id)
    returning id into v_run_work_id;
  if v_query_disposition is not null then
    insert into private.bpay_next_run_work_query_pin_v2
      (run_work_id,run_worker_id,candidate_id,work_id,captured_revision_id,
       prepare_job_id,module_epoch,gate_version,disposition,scope,query_state_sha256)
      values(v_run_work_id,v_worker_id,v_job.candidate_id,v_work.id,v_revision.id,
        p_job_id,v_module_epoch,'PAY_QUERY_STATE_V3',v_query_disposition,v_query_scope,v_query_digest);
  end if;
  if v_choice.selection_mode='SUBSET' then
    update private.bpay_next_run_worker
      set expected_selected_component_count=
        expected_selected_component_count+v_accepted_work.selected_line_count
      where id=v_worker_id;
  end if;
  update private.bpay_next_job
    set phase='PINNING',cursor_key=v_selection.selection_no::text
    where id=p_job_id;
  return pg_catalog.jsonb_build_object('done',false,'phase','PINNING',
    'next_cursor',v_selection.selection_no,'work_id',v_work.id,
    'captured_revision_id',v_revision.id,'replay',false);
end
$function$;


create or replace function public.bpay_next_preparation_intake_v1(p_actor_user_id uuid,p_action text,p_args jsonb)
returns jsonb language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_keys text[];v_run uuid;v_work uuid;v_revision uuid;v_candidate uuid;v_result jsonb;v_output jsonb;
  v_ids uuid[];v_selected boolean[];v_entry jsonb;v_header private.bpay_next_pay_run%rowtype;
begin
  -- Pure authority probe preserves auth-before-arguments and admits Source
  -- before the original locked authority check. Recorded acceptance skips it.
  if p_action='SEAL_ENQUEUE' then
    if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
      nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
      or p_actor_user_id is null or not exists(select 1 from public.tms_users u
        where u.id=p_actor_user_id and u.is_active is true and u.role::text='admin') then
      raise exception using errcode='42501',message='BPAY_NEXT_PREPARATION_FORBIDDEN';
    end if;
    if pg_catalog.jsonb_typeof(p_args)='object' and p_args->>'run_id' ~* '^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
      and not exists(select 1 from private.bpay_next_run_command rc where rc.run_id=(p_args->>'run_id')::uuid) then
      perform private.weekly_source_pay_query_admit_v2();
    end if;
  end if;
  perform private.bpay_next_preparation_authority_v1(p_actor_user_id);
  v_keys:=case p_action
    when 'CREATE' then array['run_id','pay_date']
    when 'WORK_PAGE' then array['run_id','page_no','choices']
    when 'COMPONENT_PAGE' then array['run_id','work_id','expected_revision_id','page_no','approved_line_ids']
    when 'COMPONENT_SEAL' then array['run_id','work_id','expected_revision_id','expected_pages','expected_count']
    when 'CASE_PAGE' then array['run_id','candidate_id','selection_revision','page_no','request_id','component_ids','selected']
    when 'CASE_SEAL' then array['run_id','candidate_id','selection_revision','expected_pages','expected_items']
    when 'SEAL_ENQUEUE' then array['run_id','command_id','expected_pages','expected_count']
    when 'REVIEW' then array['run_id']
    when 'CONFIRM' then array['run_id','review_revision'] else null end;
  if v_keys is null then raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
  perform private.bpay_next_preparation_args_v1(p_args,v_keys);
  v_run:=(p_args->>'run_id')::uuid;v_work:=(p_args->>'work_id')::uuid;
  v_revision:=(p_args->>'expected_revision_id')::uuid;v_candidate:=(p_args->>'candidate_id')::uuid;
  if p_action in ('COMPONENT_PAGE','CASE_PAGE') then
    for v_entry in select value from pg_catalog.jsonb_array_elements(
      p_args->case when p_action='CASE_PAGE' then 'component_ids' else 'approved_line_ids' end) loop
      if pg_catalog.jsonb_typeof(v_entry) is distinct from 'string'
        or (v_entry#>>'{}')!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
      end if;
      v_ids:=pg_catalog.array_append(v_ids,(v_entry#>>'{}')::uuid);
    end loop;
    if p_action='CASE_PAGE' then
      if pg_catalog.jsonb_array_length(p_args->'selected')<>pg_catalog.cardinality(v_ids) then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
      end if;
      for v_entry in select value from pg_catalog.jsonb_array_elements(p_args->'selected') loop
        if pg_catalog.jsonb_typeof(v_entry) is distinct from 'boolean' then
          raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
        end if;
        v_selected:=pg_catalog.array_append(v_selected,(v_entry#>>'{}')::boolean);
      end loop;
    end if;
  end if;
  case p_action
  when 'CREATE' then
    v_result:=private.bpay_next_create_preparation_v1(v_run,p_actor_user_id,(p_args->>'pay_date')::date);
    select r.* into strict v_header from private.bpay_next_pay_run r where r.id=v_run;
    v_output:=pg_catalog.jsonb_build_object('run_id',v_run,'status',v_result->>'status',
      'selection_state',v_result->>'selection_state','selection_count',v_header.selection_count::text,
      'selected_candidate_count',v_header.selected_candidate_count::text);
  when 'WORK_PAGE' then
    v_output:=private.bpay_next_append_work_choices_v1(v_run,(p_args->>'page_no')::bigint,p_args->'choices');
  when 'COMPONENT_PAGE' then
    v_output:=private.bpay_next_append_component_selection_page_v1(v_run,v_work,v_revision,(p_args->>'page_no')::bigint,v_ids);
  when 'COMPONENT_SEAL' then
    v_output:=private.bpay_next_seal_component_selection_v1(v_run,v_work,v_revision,
      (p_args->>'expected_pages')::bigint,(p_args->>'expected_count')::bigint);
  when 'CASE_PAGE' then
    v_result:=private.bpay_next_append_case_selection_page_v1(v_run,v_candidate,(p_args->>'selection_revision')::bigint,
      (p_args->>'page_no')::bigint,(p_args->>'request_id')::uuid,v_ids,v_selected);
    v_output:=pg_catalog.jsonb_build_object('run_id',v_run,'candidate_id',v_candidate,
      'selection_revision',p_args->>'selection_revision','page_no',v_result->>'page_no',
      'item_count',v_result->>'item_count','replay',v_result->'replay');
  when 'CASE_SEAL' then
    v_result:=private.bpay_next_seal_case_selection_v1(v_run,v_candidate,(p_args->>'selection_revision')::bigint,
      (p_args->>'expected_pages')::bigint,(p_args->>'expected_items')::bigint);
    v_output:=pg_catalog.jsonb_build_object('run_id',v_run,'candidate_id',v_candidate,
      'selection_revision',v_result->>'selection_revision','sealed',v_result->'sealed',
      'selected_count',v_result->>'selected_count','excluded_count',v_result->>'excluded_count','replay',v_result->'replay');
  when 'SEAL_ENQUEUE' then
    perform private.bpay_next_seal_selection_v1(v_run,(p_args->>'expected_pages')::bigint,(p_args->>'expected_count')::bigint);
    v_result:=private.bpay_next_enqueue_preparation_v1(v_run,(p_args->>'command_id')::uuid);
    v_output:=pg_catalog.jsonb_build_object('run_id',v_run,'command_id',v_result->>'command_id',
      'agency_sequence',v_result->>'agency_sequence','replay',v_result->'replay');
  when 'REVIEW' then
    v_result:=private.bpay_next_review_simple_run_v1(v_run);
    select r.* into strict v_header from private.bpay_next_pay_run r where r.id=v_run;
    v_output:=pg_catalog.jsonb_build_object('run_id',v_run,'phase',v_result->>'phase','reason',v_result->>'reason',
      'review_revision',v_header.review_revision::text,'all_ready',v_result->'all_ready',
      'replay',coalesce(v_result->'replay','false'::jsonb));
  when 'CONFIRM' then
    v_result:=private.bpay_next_confirm_simple_run_v1(v_run,(p_args->>'review_revision')::bigint);
    v_output:=pg_catalog.jsonb_build_object('run_id',v_run,'phase',v_result->>'phase',
      'review_revision',v_result->>'review_revision','replay',v_result->'replay','drafts',v_result->'drafts');
  end case;
  if pg_catalog.octet_length(v_output::text)>120000 then
    raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_RESPONSE_TOO_LARGE';
  end if;
  return v_output;
end
$function$;

create or replace function private.bpay_next_run_work_query_pin_guard_v2()
returns trigger language plpgsql volatile security definer set search_path=pg_catalog,private
as $function$
declare v_work private.bpay_next_accepted_work_input%rowtype;
begin
  if tg_op<>'INSERT' then raise exception using errcode='55000',message='BPAY_NEXT_QUERY_PIN_IMMUTABLE';end if;
  select input_work.* into strict v_work
    from private.bpay_next_run_work rw
    join private.bpay_next_run_worker worker on worker.id=rw.run_worker_id and worker.candidate_id=rw.candidate_id
    join private.bpay_next_accepted_input input on input.run_id=worker.run_id and input.candidate_id=worker.candidate_id
    join private.bpay_next_accepted_work_input input_work on input_work.accepted_input_id=input.id and input_work.work_id=rw.work_id
    join private.bpay_next_effective_work_cut cut on cut.accepted_work_input_id=input_work.id and cut.accepted_input_id=input.id
    join private.bpay_next_job job on job.id=cut.prepare_job_id and job.command_id=input.prepare_command_id
      and job.candidate_id=input.candidate_id and job.command_sequence=input.accepted_sequence and job.module_epoch=input.module_epoch
    join private.bpay_next_worker_control owner on owner.candidate_id=input.candidate_id
    join private.bpay_next_module_control module on module.id=1 and module.active_owner='NEXT' and module.owner_epoch=input.module_epoch
    join private.bpay_next_pay_run run on run.id=input.run_id
    where rw.id=new.run_work_id and rw.run_worker_id=new.run_worker_id and rw.candidate_id=new.candidate_id
      and rw.work_id=new.work_id and rw.captured_revision_id=new.captured_revision_id
      and input_work.effective_revision_id=rw.captured_revision_id and cut.effective_revision_id=rw.captured_revision_id
      and cut.work_id=rw.work_id and cut.prepare_job_id=new.prepare_job_id and job.module_epoch=new.module_epoch
      and worker.status='PREPARING' and run.status='PREPARING' and run.selection_state='SEALED'
      and job.job_kind='PREPARE' and job.status='LEASED' and job.phase in ('NEW','PINNING')
      and job.lease_nonce is not null and job.lease_until_utc>pg_catalog.clock_timestamp()
      and job.owner_epoch=owner.active_owner_epoch and input_work.source_kind in ('SOURCE','PROTECTED')
      and input_work.query_disposition='CLEAR' and input_work.evidence_sha256=cut.evidence_sha256;
  if (new.gate_version,new.disposition,new.scope,new.query_state_sha256)
    is distinct from ('PAY_QUERY_STATE_V3','CLEAR',v_work.query_scope,v_work.query_state_sha256) then
    raise exception using errcode='23514',message='BPAY_NEXT_QUERY_PIN_ACCEPTED_EVIDENCE_MISMATCH';
  end if;
  new.captured_at_utc:=pg_catalog.clock_timestamp();return new;
end
$function$;

alter function private.bpay_next_claim_prepare_job_v1(uuid,integer) owner to postgres;
revoke all on function private.bpay_next_claim_prepare_job_v1(uuid,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_enroll_one_v1() owner to postgres;
revoke all on function private.bpay_next_enroll_one_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_pin_selected_work_v1(uuid,uuid,bigint,bigint) owner to postgres;
revoke all on function private.bpay_next_pin_selected_work_v1(uuid,uuid,bigint,bigint) from public,anon,authenticated,service_role;
alter function public.bpay_next_preparation_intake_v1(uuid,text,jsonb) owner to postgres;
revoke all on function public.bpay_next_preparation_intake_v1(uuid,text,jsonb) from public,anon,authenticated,service_role;
alter function private.bpay_next_run_work_query_pin_guard_v2() owner to postgres;
revoke all on function private.bpay_next_run_work_query_pin_guard_v2() from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_preparation_intake_v1(uuid,text,jsonb) to service_role;
commit;
