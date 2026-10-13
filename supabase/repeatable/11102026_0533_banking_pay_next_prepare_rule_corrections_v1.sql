-- DR-04: native bounded preparation closure and ID-only queue lease yield.
-- Exact current parents retained; accepted Rule B, original money and Source producers unchanged.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_begin_prepare_projection_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,p_rows_visited integer
) returns jsonb language plpgsql security definer
set search_path=pg_catalog,private
as $function$
declare v_job private.bpay_next_job%rowtype;v_worker private.bpay_next_run_worker%rowtype;
begin
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id;
  select w.* into strict v_worker from private.bpay_next_run_command rc
    join private.bpay_next_run_worker w on w.run_id=rc.run_id and w.candidate_id=v_job.candidate_id
    where rc.command_id=v_job.command_id;
  if v_job.job_kind<>'PREPARE' or v_job.status<>'LEASED'
    or v_job.lease_nonce is distinct from p_lease_nonce or v_job.owner_epoch is distinct from p_owner_epoch
    or v_worker.status not in ('READY','REVIEW') or p_rows_visited is null or p_rows_visited not between 0 and 100
    or v_job.phase in ('WORKBENCH_WORK','WORKBENCH_CASE') then
    raise exception using errcode='23514',message='BPAY_NEXT_JOB_STEP_RESULT_INVALID';
  end if;
  update private.bpay_next_pay_run set preparing_candidate_count=preparing_candidate_count-1,
    ready_candidate_count=ready_candidate_count+case when v_worker.status='READY' then 1 else 0 end,
    review_candidate_count=review_candidate_count+case when v_worker.status='REVIEW' then 1 else 0 end
    where id=v_worker.run_id;
  update private.bpay_next_job set phase='WORKBENCH_WORK',cursor_key=null,
    position_work_cursor=null,position_component_cursor=null where id=p_job_id;
  return pg_catalog.jsonb_build_object('done',false,'phase','WORKBENCH_WORK','cursor',null,
    'rows_visited',p_rows_visited::text,'replay',false,'run_worker_id',v_worker.id);
end
$function$;

create or replace function private.bpay_next_prepare_projection_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,p_expected_phase text,p_expected_cursor bigint,p_limit integer
) returns jsonb language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_job private.bpay_next_job%rowtype;v_run private.bpay_next_pay_run%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;v_module_epoch bigint;v_run_id uuid;
  v_ids uuid[];v_seen integer;v_cursor bigint;v_revision bigint;v_phase text;v_done boolean:=false;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null
    or p_expected_phase is null or p_limit is null
    or p_expected_phase not in ('WORKBENCH_WORK','WORKBENCH_CASE') or p_limit not between 1 and 100
    or (p_expected_cursor is not null and p_expected_cursor<1) then
    raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
  end if;
  select m.owner_epoch into v_module_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_module_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id;
  select rc.run_id into strict v_run_id from private.bpay_next_run_command rc where rc.command_id=v_job.command_id;
  select r.* into strict v_run from private.bpay_next_pay_run r where r.id=v_run_id for update;
  perform 1 from private.bpay_next_worker_control c where c.candidate_id=v_job.candidate_id for update;
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id for update;
  select w.* into strict v_worker from private.bpay_next_run_worker w
    where w.run_id=v_run_id and w.candidate_id=v_job.candidate_id for update;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED'
    or private.bpay_next_preparation_expiry_fenced_v1(v_run_id,v_job.command_id)
    or v_job.job_kind<>'PREPARE' or v_job.status<>'LEASED' or v_job.module_epoch<>v_module_epoch
    or v_job.lease_nonce is distinct from p_lease_nonce or v_job.owner_epoch is distinct from p_owner_epoch
    or v_job.lease_until_utc<=pg_catalog.clock_timestamp() or v_worker.status not in ('READY','REVIEW') then
    raise exception using errcode='55000',message='BPAY_NEXT_PIN_LEASE_STALE';
  end if;
  if v_job.phase not in ('WORKBENCH_WORK','WORKBENCH_CASE') then
    raise exception using errcode='23514',message='BPAY_NEXT_PIN_PHASE_INVALID';
  end if;
  if v_job.phase is distinct from p_expected_phase or v_job.cursor_key::bigint is distinct from p_expected_cursor then
    return pg_catalog.jsonb_build_object('done',false,'phase',v_job.phase,'cursor',v_job.cursor_key,'rows_visited','0','replay',true);
  end if;
  v_phase:=v_job.phase;
  if v_phase='WORKBENCH_WORK' then
    select pg_catalog.array_agg(p.work_id order by p.selection_no),count(*)::integer,max(p.selection_no)
      into v_ids,v_seen,v_cursor from (select s.work_id,s.selection_no from private.bpay_next_run_selection s
        where s.run_id=v_run_id and s.candidate_id=v_job.candidate_id
          and s.selection_no>coalesce(p_expected_cursor,0) order by s.selection_no limit p_limit) p;
    if v_seen>0 then perform private.bpay_next_workbench_refresh_many_v1(v_ids,null);end if;
    if v_seen<p_limit then v_phase:='WORKBENCH_CASE';v_cursor:=null;end if;
  else
    v_revision:=nullif(v_worker.case_selection_revision,0);
    if v_revision is null then
      select s.selection_revision into v_revision from private.bpay_next_case_selection s
        where s.run_id=v_run_id and s.candidate_id=v_job.candidate_id and s.status='SEALED'
        order by s.selection_revision desc limit 1;
    elsif not exists(select 1 from private.bpay_next_case_selection s where s.run_id=v_run_id
        and s.candidate_id=v_job.candidate_id and s.selection_revision=v_revision and s.status='SEALED') then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_SELECTION_REQUIRED';
    end if;
    select pg_catalog.array_agg(distinct p.case_id),count(*)::integer,max(p.selection_no)
      into v_ids,v_seen,v_cursor from (select s.case_id,s.selection_no from private.bpay_next_case_selection_item s
        where s.run_id=v_run_id and s.candidate_id=v_job.candidate_id and s.selection_revision=v_revision
          and s.selection_no>coalesce(p_expected_cursor,0) order by s.selection_no limit p_limit) p;
    if v_seen>0 then perform private.bpay_next_workbench_refresh_many_v1(null,v_ids);end if;
    if v_seen<p_limit then
      v_done:=true;v_phase:=case when v_worker.status='READY' then 'READY' else 'DONE' end;v_cursor:=null;
    end if;
  end if;
  update private.bpay_next_job set phase=v_phase,cursor_key=v_cursor::text,
    status=case when v_done then 'DONE' else 'LEASED' end,
    lease_nonce=case when v_done then null else lease_nonce end,
    lease_until_utc=case when v_done then null else lease_until_utc end where id=p_job_id;
  if v_done then perform private.bpay_next_review_simple_run_v1(v_run_id);end if;
  return pg_catalog.jsonb_build_object('done',v_done,'phase',case when v_phase='DONE' then 'REVIEW' else v_phase end,
    'cursor',v_cursor::text,'rows_visited',v_seen::text,'replay',false);
end
$function$;


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
    where id=v_run_id for update;
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
    if v_job.phase in ('WORKBENCH_WORK','WORKBENCH_CASE') then
      raise exception using errcode='23514',message='BPAY_NEXT_PREPARE_WORKER_STATE_CHANGED';
    end if;
    select selected.target_pay_channel,selected.target_umbrella_id,selected.target_umbrella_vat_chargeable
      into strict v_channel,v_umbrella_id,v_umbrella_vat_chargeable
      from private.bpay_next_selection_candidate selected
      where selected.run_id=v_run_id and selected.candidate_id=v_job.candidate_id;
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
    update private.bpay_next_pay_run set preparing_candidate_count=preparing_candidate_count+1 where id=v_run_id;
  elsif v_worker.status<>'PREPARING' and not (v_job.phase in ('WORKBENCH_WORK','WORKBENCH_CASE') and v_worker.status in ('READY','REVIEW')) then
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
  v_query_admitted boolean:=false;
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
  -- Authorised direct callers use the same outer entry as 0280. Discover
  -- without a job/module/run lock; do not newly admit retained replay/stale
  -- inputs. This read never substitutes for the original locked owner checks.
  select j.* into strict v_job from private.bpay_next_job j
    where j.id=p_job_id and j.job_kind='PREPARE';
  select rc.run_id into strict v_run_id from private.bpay_next_run_command rc
    where rc.command_id=v_job.command_id;
  if v_job.status='LEASED' and v_job.phase in ('NEW','PINNING')
     and v_job.lease_nonce is not distinct from p_lease_nonce
     and v_job.owner_epoch is not distinct from p_owner_epoch
     and v_job.lease_until_utc>pg_catalog.clock_timestamp()
     and v_job.cursor_key::bigint is not distinct from p_expected_cursor then
    perform private.weekly_source_pay_query_admit_v2();
    v_query_admitted:=true;
  end if;
  select m.owner_epoch into v_module_epoch
    from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_module_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select * into strict v_run from private.bpay_next_pay_run
    where id=v_run_id for update;
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
  if not v_query_admitted then
    -- A changed discovery cannot become a fresh mutation after Bank locks.
    -- Roll back and retry the original STEP via the existing 40001 channel;
    -- never acquire the Source key late, fabricate REVIEW or adopt a nonce.
    raise exception using errcode='40001',message='BPAY_NEXT_PIN_ADMISSION_CHANGED';
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
  if v_work.approval_state<>'APPROVED'
     or v_work.current_revision_id is null
     or v_choice.expected_revision_id is null
     or v_choice.expected_revision_id is distinct from v_work.current_revision_id
     or v_work.current_revision_id is distinct from v_work.applied_revision_id then
    -- A newly published revision may be waiting behind this PREPARE command.
    -- End only this worker's non-executable attempt so its later position job
    -- can run. Do not silently omit the work or retain an executable offer.
    update private.bpay_next_run_worker
      set status='REVIEW',review_issue_code=case
        when v_work.approval_state<>'APPROVED' or v_work.current_revision_id is null
          or v_choice.expected_revision_id is null
          then 'SELECTED_WORK_NOT_APPROVED'
        when v_choice.expected_revision_id is distinct from v_work.current_revision_id
          then 'SELECTED_WORK_REVISION_CHANGED'
        else 'SELECTED_WORK_POSITION_PENDING' end,
        review_issue_work_id=v_work.id
      where id=v_worker_id;
    return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,1);
  end if;
  select * into strict v_revision from private.bpay_next_work_revision
    where id=v_choice.expected_revision_id and work_id=v_work.id for share;
  if v_revision.sealed_at_utc is null or v_revision.approved_at_utc is null then
    raise exception using errcode='23514',
      message='BPAY_NEXT_PIN_APPROVAL_NOT_SEALED';
  end if;
  if v_work.work_kind='SOURCE' and v_revision.source_kind in ('SOURCE','PROTECTED') then
    -- Exact point qualification of the approved captured origin. No I3
    -- financial-completion reader, inventory/history rebuild or live price.
    -- Initial authorised TSFIN, committed HEAD and genuine headless direct
    -- NEXT are distinct lanes; none may manufacture the other's authority.
    -- Evaluate the actual classifier as its own statement even for self-bill:
    -- OR/CASE short-circuiting must not hide its permission/integrity errors.
    -- Current domain facts qualify a returned Scope, not immutable provenance
    -- when Source explicitly reports that the current scope is unavailable.
    v_source_classified:=private._candidate_expense_source_family_v1(v_revision.physical_timesheet_id);
    select t.timesheet_id,t.version,t.booking_id,c.client_id,
           family.id as family_id,family.bound_version,
           (t.sheet_scope='WEEKLY'::public.timesheet_scope_enum
            and t.contract_id=v_work.contract_id and c.candidate_id=v_work.candidate_id
            and t.week_ending_date=v_work.week_ending_date
            and t.version=v_revision.physical_timesheet_version
            and t.booking_id=v_work.booking_id and c.client_id is not null
            and v_revision.week_ending_date=v_work.week_ending_date) as root_exact,
           (t.line_type='HOURS'::public.timesheet_line_type_enum and not t.is_adjustment
            and (c.self_bill is true or v_source_classified is true)
            and cw.timesheet_id=t.timesheet_id and not cw.is_adjustment
            and (family.id is null or (family.root_timesheet_id=t.timesheet_id
              and family.root_family_booking_id=t.booking_id
              and family.candidate_id=v_work.candidate_id
              and family.contract_id=v_work.contract_id
              and family.week_ending_date=v_work.week_ending_date
              and family.bound_version>0))) as scope_exact,
           case when v_revision.source_head_id is not null then
             h.id=v_revision.source_event_id and h.root_timesheet_id=t.timesheet_id
             and h.root_timesheet_version=t.version and h.root_family_booking_id=t.booking_id
             and h.candidate_id=v_work.candidate_id and h.contract_id=v_work.contract_id
             and h.week_ending_date=v_work.week_ending_date and h.state in ('COMMITTED_CURRENT','SUPERSEDED')
             and h.committed_at_utc is not null and pg_catalog.isfinite(h.committed_at_utc)
             and h.authority_kind=case when v_revision.source_kind='PROTECTED'
               then 'PROTECTED' else 'LOCKED_FINAL_SOURCE' end
           when v_revision.source_kind='SOURCE' then
             auth.id=v_revision.source_event_id and auth.root_timesheet_id=t.timesheet_id
             and auth.family_booking_id=t.booking_id and auth.timesheet_version=t.version
             and auth.authorised_at_utc is not null and pg_catalog.isfinite(auth.authorised_at_utc)
             and tf.id=v_revision.financial_snapshot_id and tf.timesheet_id=t.timesheet_id
             and tf.timesheet_version=t.version and tf.candidate_id=v_work.candidate_id
             and tf.client_id=c.client_id and tf.authorised_at_utc is not null
             and pg_catalog.isfinite(tf.authorised_at_utc)
           else
             v_revision.source_head_id is null and v_revision.financial_snapshot_id is null
             and receipt.approval_id=v_revision.source_event_id
             and receipt.work_id=v_work.id and receipt.revision_id=v_revision.id
             and receipt.command_id=publication.command_id and receipt.family_id=family.id
             and receipt.agency_sequence=command.agency_sequence
             and receipt.accepted_family_bound_version<=family.bound_version
             and approval.creation_orchestration_run_id=receipt.orchestration_run_id
             and approval.pay_target_family_id=family.id and approval.candidate_id=v_work.candidate_id
             and approval.client_id=c.client_id and approval.contract_id=v_work.contract_id
             and approval.week_ending=v_work.week_ending_date
             and generation.family_id=family.id and generation.lifecycle_state in ('PUBLISHED','SUPERSEDED')
             and generation.published_at_utc is not null
             and generation.result_hash=receipt.publication_request_sha256
             and orchestration.family_id=family.id and orchestration.state='COMPLETE'
             and orchestration.requested_by_user_id=receipt.actor_user_id
             and orchestration.request_fingerprint=receipt.prepared_request_sha256
             and orchestration.after_state_fingerprint=receipt.publication_request_sha256
             and orchestration.completed_at_utc is not null
           end as origin_exact,
           case when v_revision.source_head_id is not null then
             h.state='COMMITTED_CURRENT' and live_auth.current_entitlement_head_id=h.id
           when v_revision.source_kind='SOURCE' then
             live_auth.id=auth.id and live_auth.current_entitlement_head_id is null
             and not exists(select 1 from public.weekly_source_entitlement_heads current_head
               where pg_catalog.btrim(current_head.root_family_booking_id)=pg_catalog.btrim(t.booking_id)
                 and current_head.state='COMMITTED_CURRENT')
           else approval.withdrawn_at_utc is null end as financial_current,
           (publication.candidate_id=v_work.candidate_id
             and publication.revision_no=v_revision.revision_no
             and publication.status='APPLIED' and publication.phase='DONE'
             and command.command_kind='POSITION_APPLY' and command.module_epoch=v_module_epoch
             and command.id=private.bpay_next_source_command_id_v1(v_revision.source_event_id)
             and command.status='COMPLETE' and command.sealed_at_utc is not null
             and command.expected_member_count=command.enrolled_member_count
             and member.member_no is not null) as publication_exact
      into v_source_origin
      from public.timesheets t
      join public.contracts c on c.id=t.contract_id
      -- uq_contract_week makes this one indexed ordinary configured-week
      -- point; a same-contract/week row for another physical root is not scope.
      left join public.contract_weeks cw
        on cw.contract_id=t.contract_id and cw.week_ending_date=t.week_ending_date
          and cw.additional_seq=0
      left join public.weekly_exceptional_pay_target_families family
        on pg_catalog.btrim(family.root_family_booking_id)=pg_catalog.btrim(t.booking_id)
      left join public.weekly_source_entitlement_heads h on h.id=v_revision.source_head_id
      left join public.weekly_source_root_authorisations auth
        on auth.id=v_revision.source_event_id and v_revision.source_head_id is null
          and v_revision.source_kind='SOURCE'
      left join public.weekly_source_root_authorisations live_auth
        on live_auth.root_timesheet_id=t.timesheet_id and live_auth.withdrawn_at_utc is null
      left join public.timesheets_financials tf on tf.id=v_revision.financial_snapshot_id
      left join private.bpay_next_protected_source_receipt receipt
        on receipt.revision_id=v_revision.id and receipt.work_id=v_work.id
      left join public.weekly_exceptional_payment_approvals approval on approval.id=receipt.approval_id
      left join public.weekly_exceptional_pay_generations generation on generation.id=receipt.generation_id
      left join public.weekly_exceptional_orchestration_runs orchestration on orchestration.id=receipt.orchestration_run_id
      left join private.bpay_next_publication publication
        on publication.work_id=v_work.id and publication.revision_id=v_revision.id
      left join private.bpay_next_command command on command.id=publication.command_id
      left join private.bpay_next_command_member member
        on member.command_id=command.id and member.candidate_id=v_work.candidate_id
      where t.timesheet_id=v_revision.physical_timesheet_id;
    if not found or v_source_origin.root_exact is not true
       or v_source_origin.origin_exact is not true or v_source_origin.publication_exact is not true then
      raise exception using errcode='23514',message='BPAY_NEXT_PIN_SOURCE_ORIGIN_INVALID';
    end if;
    -- Consume the raw Source-owned gate captured for this exact ticked revision.
    select snapshot.gate_result into v_query_gate
      from private.bpay_next_run_work_query_snapshot snapshot
      where snapshot.run_id=v_run_id and snapshot.work_id=v_work.id
        and snapshot.expected_revision_id=v_revision.id
        and snapshot.physical_timesheet_id=v_revision.physical_timesheet_id;
    if not found then
      raise exception using errcode='23514',message='BPAY_NEXT_PIN_QUERY_SNAPSHOT_MISSING';
    end if;
    if (pg_catalog.jsonb_typeof(v_query_gate)='object'
        and v_query_gate ?& array['ok','code','scope','blocked','query_state_sha256']
        and v_query_gate-array['ok','code','scope','blocked','query_state_sha256']='{}'::jsonb
        and pg_catalog.jsonb_typeof(v_query_gate->'ok')='boolean'
        and pg_catalog.jsonb_typeof(v_query_gate->'code')='string') is not true then
      raise exception using errcode='23514',message='BPAY_NEXT_PIN_QUERY_RESPONSE_INVALID';
    end if;
    v_query_scope:=nullif(v_query_gate->'scope','null'::jsonb);
    if v_query_scope is not null then
      if (v_source_origin.scope_exact is true
          and pg_catalog.jsonb_typeof(v_query_scope)='object'
          and v_query_scope ?& array['root_timesheet_id','family_booking_id','target_family_id',
            'candidate_id','client_id','contract_id','week_ending_date','root_version','family_bound_version']
          and v_query_scope-array['root_timesheet_id','family_booking_id','target_family_id',
            'candidate_id','client_id','contract_id','week_ending_date','root_version','family_bound_version']='{}'::jsonb
          and pg_catalog.jsonb_typeof(v_query_scope->'root_timesheet_id')='string'
          and pg_catalog.jsonb_typeof(v_query_scope->'family_booking_id')='string'
          and pg_catalog.jsonb_typeof(v_query_scope->'candidate_id')='string'
          and pg_catalog.jsonb_typeof(v_query_scope->'client_id')='string'
          and pg_catalog.jsonb_typeof(v_query_scope->'contract_id')='string'
          and pg_catalog.jsonb_typeof(v_query_scope->'week_ending_date')='string'
          and pg_catalog.jsonb_typeof(v_query_scope->'root_version')='string'
          and v_query_scope->>'root_timesheet_id'=v_revision.physical_timesheet_id::text
          and v_query_scope->>'root_version'=v_revision.physical_timesheet_version::text
          and v_query_scope->>'family_booking_id'=v_work.booking_id
          and v_query_scope->>'candidate_id'=v_work.candidate_id::text
          and v_query_scope->>'client_id'=v_source_origin.client_id::text
          and v_query_scope->>'contract_id'=v_work.contract_id::text
          and v_query_scope->>'week_ending_date'=v_work.week_ending_date::text
          and v_query_scope->'target_family_id'=coalesce(pg_catalog.to_jsonb(v_source_origin.family_id::text),'null'::jsonb)
          and v_query_scope->'family_bound_version'=coalesce(pg_catalog.to_jsonb(v_source_origin.bound_version::text),'null'::jsonb)) is not true then
        raise exception using errcode='23514',message='BPAY_NEXT_PIN_QUERY_SCOPE_MISMATCH';
      end if;
    end if;
    v_query_digest:=null;
    if v_query_gate->'ok'='true'::jsonb then
      if (v_query_gate->>'code'='OK' and v_query_scope is not null
          and pg_catalog.jsonb_typeof(v_query_gate->'blocked')='boolean'
          and pg_catalog.jsonb_typeof(v_query_gate->'query_state_sha256')='string'
          and v_query_gate->>'query_state_sha256' ~ '^[0-9a-f]{64}$') is not true then
        raise exception using errcode='23514',message='BPAY_NEXT_PIN_QUERY_RESPONSE_INVALID';
      end if;
      -- The query Scope has no HEAD id. An available/current query root must
      -- never make a superseded or otherwise unmapped financial origin pay.
      -- Conversely an actual defined unavailable current scope retains its
      -- genuine immutable origin and reaches positive-WORK REVIEW below.
      if v_source_origin.financial_current is not true then
        raise exception using errcode='23514',message='BPAY_NEXT_PIN_SOURCE_AUTHORITY_CHANGED';
      end if;
      v_query_digest:=pg_catalog.decode(v_query_gate->>'query_state_sha256','hex');
      v_query_disposition:=case when v_query_gate->'blocked'='true'::jsonb then 'BLOCKED' else 'CLEAR' end;
    elsif ((v_query_gate->>'code'='WEEKLY_SOURCE_PAY_QUERY_SCOPE_UNAVAILABLE' and v_query_scope is null)
        or (v_query_gate->>'code'='WEEKLY_SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE' and v_query_scope is not null))
        and v_query_gate->'blocked'='null'::jsonb and v_query_gate->'query_state_sha256'='null'::jsonb then
      v_query_disposition:='UNAVAILABLE';
    else
      raise exception using errcode='23514',message='BPAY_NEXT_PIN_QUERY_RESPONSE_INVALID';
    end if;
  else
    v_query_disposition:=null;
  end if;
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
        expected_selected_component_count+v_choice.component_count
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

create or replace function private.bpay_next_capture_position_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,
  p_expected_work_cursor uuid,p_expected_component_cursor text,
  p_limit integer default 100
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private
as $function$
declare
  v_run_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_job private.bpay_next_job%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_module_epoch bigint;
  v_row record;
  v_member record;
  v_seen integer:=0;
  v_selected_seen integer:=0;
  v_resolution_count integer:=0;
  v_managed_seen integer:=0;
  v_needs_resolution boolean;
  v_collection private.bpay_next_work_collection%rowtype;
  v_work_cursor uuid;
  v_component_cursor text;
  v_issue text;
  v_issue_work_id uuid;
  v_query_pin private.bpay_next_run_work_query_pin_v2%rowtype;
  v_binding private.bpay_next_source_component_binding_version%rowtype;v_book record;
  v_segments bigint;v_snooze jsonb;v_segment_result jsonb;v_segment_visited integer:=0;
  v_seed_effect uuid;v_seed_hold uuid;v_seed_holds uuid[];v_seed record;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null
     or p_limit not between 1 and 100
     or (p_expected_work_cursor is null and p_expected_component_cursor is not null) then
    raise exception using errcode='22023',
      message='BPAY_NEXT_CAPTURE_POSITION_INPUT_INVALID';
  end if;
  select rc.run_id into strict v_run_id
    from private.bpay_next_job j
    join private.bpay_next_run_command rc on rc.command_id=j.command_id
    where j.id=p_job_id and j.job_kind='PREPARE';
  select m.owner_epoch into v_module_epoch
    from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_module_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select * into strict v_run from private.bpay_next_pay_run
    where id=v_run_id for update;
  select candidate_id into strict v_job.candidate_id
    from private.bpay_next_job where id=p_job_id;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_job.candidate_id for update;
  select * into strict v_job from private.bpay_next_job
    where id=p_job_id for update;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED'
     or v_job.job_kind<>'PREPARE' or v_job.status<>'LEASED'
     or v_job.phase not in ('RESERVE','ALLOCATE')
     or v_job.module_epoch<>v_module_epoch
     or v_job.lease_nonce<>p_lease_nonce
     or v_job.owner_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp() then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CAPTURE_POSITION_LEASE_STALE';
  end if;
  if v_job.phase='ALLOCATE' then
    return pg_catalog.jsonb_build_object('done',true,'phase','ALLOCATE',
      'replay',true,'rows_visited',0);
  end if;
  if v_job.position_work_cursor is distinct from p_expected_work_cursor
     or v_job.position_component_cursor is distinct from p_expected_component_cursor then
    return pg_catalog.jsonb_build_object('done',false,'phase','RESERVE',
      'replay',true,'rows_visited',0,
      'work_cursor',v_job.position_work_cursor,
      'component_cursor',v_job.position_component_cursor);
  end if;
  select * into strict v_worker from private.bpay_next_run_worker
    where run_id=v_run_id and candidate_id=v_job.candidate_id for update;
  if v_worker.status<>'PREPARING' or v_worker.review_issue_code is not null then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CAPTURE_POSITION_WORKER_NOT_PREPARING';
  end if;
  v_work_cursor:=v_job.position_work_cursor;
  v_component_cursor:=v_job.position_component_cursor;
  for v_member in
    select s.* from private.bpay_next_selected_position_keys_v1(v_worker.id,v_work_cursor,v_component_cursor,p_limit) s
  loop
    -- The Candidate control is already exclusively held. Lock this exact
    -- selected WORK before its position, matching the reentrant seed order.
    perform 1 from private.bpay_next_work w where w.id=v_member.work_id
      and w.candidate_id=v_worker.candidate_id for update;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_CAPTURE_POSITION_WORK_SCOPE_INVALID';end if;
    -- Read and lock the exact position after choosing the bounded member.
    -- Do not FOR SHARE the nullable side of an outer join (PostgreSQL refuses
    -- that), and never turn a missing selected position into an omitted line.
    select v_member.run_work_id as run_work_id,v_member.work_id as work_id,
           v_member.captured_revision_id as captured_revision_id,
           p.component_key,p.applied_revision_id,
           p.approved_source_ex_vat,p.realised_source_ex_vat,
           p.held_source_ex_vat,p.realised_target_ex_vat,
           p.realised_target_vat,p.realised_target_inc_vat,
           p.held_target_ex_vat,p.held_target_vat,
           p.held_target_inc_vat,p.source_basis_channel,
           rev.source_pay_channel,rev.source_kind,rev.source_event_id,
           rev.source_head_id,rev.financial_snapshot_id,
           rev.physical_timesheet_id,rev.physical_timesheet_version,
           w.work_kind,w.candidate_id as origin_candidate_id,
           l.component_kind,l.id as approved_line_id,
           l.source_pay_ex_vat as approved_line_source_ex_vat,l.tax_treatment,
           p.source_book_state,p.source_book_revision
      into v_row
      from private.bpay_next_position p
      join private.bpay_next_work_revision rev
        on rev.id=v_member.captured_revision_id and rev.work_id=v_member.work_id
      join private.bpay_next_work w on w.id=rev.work_id
      left join private.bpay_next_approved_line l
        on l.revision_id=v_member.captured_revision_id
       and l.component_key=v_member.component_key
      where p.work_id=v_member.work_id and p.component_key=v_member.component_key
      for update of p;
    v_issue:=null;
    if not found
       or v_row.applied_revision_id is distinct from v_member.captured_revision_id then
      v_issue:='CAPTURE_POSITION_CHANGED';
    elsif v_row.approved_line_id is null
       and v_row.approved_source_ex_vat<>0 then
      v_issue:='POSITION_COMPONENT_EVIDENCE_MISSING';
    elsif v_row.approved_line_id is not null
       and v_row.approved_line_source_ex_vat is distinct from
         v_row.approved_source_ex_vat then
      v_issue:='POSITION_APPROVAL_AMOUNT_MISMATCH';
    end if;
    if v_worker.target_pay_channel='UMBRELLA' and v_issue is null and
       (v_row.source_pay_channel is distinct from 'UMBRELLA'
        or v_row.work_kind not in ('ORDINARY','SOURCE')
        or v_row.component_kind is distinct from 'WORK'
        or v_row.tax_treatment is distinct from 'TAXABLE') then
      v_issue:='UMBRELLA_WORK_EXECUTION_UNSUPPORTED';
    end if;
    if v_issue is not null then
      update private.bpay_next_run_worker
        set status='REVIEW',review_issue_code=v_issue,
            review_issue_work_id=v_member.work_id
        where id=v_worker.id;
      return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,v_seen);
    end if;
    -- Typed initial I-1 is the ONLY new no-HEAD binding arm. Initial and
    -- common-HEAD captures require self-canonical same-channel TAXABLE WORK.
    v_binding:=null;v_book:=null;
    if v_row.work_kind='SOURCE' and (v_row.source_head_id is not null
      or (v_row.source_kind='SOURCE' and v_row.source_event_id is not null
        and v_row.financial_snapshot_id is not null)) then
      select v.* into v_binding from private.bpay_next_source_component_binding b
        join private.bpay_next_source_component_binding_version v on v.id=b.current_binding_version_id
        where b.canonical_work_id=v_row.work_id and b.canonical_component_key=v_row.component_key
          and v.approval_work_id=v_row.work_id and v.approval_revision_id=v_row.captured_revision_id
          and v.approval_component_key=v_row.component_key
          and ((v_row.source_head_id is not null and v.origin_kind='COMMON_HEAD_RECEIPT_V1'
              and v.head_id=v_row.source_head_id)
            or (v_row.source_head_id is null and v.origin_kind='INITIAL_AUTHORISED_TSFIN_V1'
              and v.initial_root_authorisation_id=v_row.source_event_id
              and v.initial_financial_snapshot_id=v_row.financial_snapshot_id
              and v.initial_root_timesheet_id=v_row.physical_timesheet_id
              and v.initial_root_version=v_row.physical_timesheet_version));
      if v_binding.id is null or v_binding.publication_command_id_2 is not null
         or v_row.source_pay_channel not in ('PAYE','UMBRELLA')
         or v_worker.target_pay_channel is distinct from v_row.source_pay_channel
         or v_row.component_kind is distinct from 'WORK' or v_row.tax_treatment is distinct from 'TAXABLE' then
        v_issue:='SOURCE_BOOK_APPROVAL_UNBOUND';
      else
        if v_row.source_book_state='UNBOUND' then
          -- Capture is already a bounded selected point with real APPLIED/DONE
          -- authority. Adoption never borrows a queued writer's lease here.
          v_seed_effect:=null;v_seed_hold:=null;v_seed_holds:=null;
          if v_row.realised_source_ex_vat>0 then
            select b.original_effect_id into v_seed_effect from private.bpay_next_work_collection b
              where b.work_id=v_row.work_id and b.component_key=v_row.component_key
                and b.candidate_id=v_worker.candidate_id and b.paid_basis_qualified;
          end if;
          if v_row.held_source_ex_vat>0 then
            select pg_catalog.array_agg(h.id order by h.id) into v_seed_holds from (
              select id from private.bpay_next_hold where work_id=v_row.work_id
                and component_key=v_row.component_key and status='ACTIVE' order by id limit 2) h;
            if pg_catalog.cardinality(v_seed_holds) is distinct from 1 then
              raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_ACTIVE_HOLD_WITNESS_AMBIGUOUS';
            end if;
            v_seed_hold:=v_seed_holds[1];
          end if;
          select seed.* into strict v_seed from private.bpay_next_source_disposition_seed_v1(
            v_row.work_id,v_row.component_key,v_row.captured_revision_id,v_seed_effect,v_seed_hold) seed;
          if v_seed.qualification_state is distinct from 'READY' then
            raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_SELECTED_SEED_NOT_READY';
          end if;
          select p.source_book_state,p.source_book_revision into v_row.source_book_state,v_row.source_book_revision
            from private.bpay_next_position p where p.work_id=v_row.work_id and p.component_key=v_row.component_key;
        end if;
        select s.* into strict v_book from private.bpay_next_bound_source_snapshot_v1(
          v_binding.id,v_row.source_book_revision) s;
        if v_book.qualification_state is distinct from 'READY'
           or (v_book.candidate_id,v_book.canonical_work_id,v_book.canonical_component_key,
               v_book.approval_work_id,v_book.approval_revision_id,v_book.approved_line_id,
               v_book.source_channel,v_book.tax_treatment,v_book.approved_source_ex_vat,
               v_book.native_realised_paye,v_book.native_held_paye,v_book.native_realised_umbrella,v_book.native_held_umbrella)
             is distinct from (v_worker.candidate_id,v_row.work_id,v_row.component_key,
               v_row.work_id,v_row.captured_revision_id,v_row.approved_line_id,
               v_row.source_pay_channel,'TAXABLE'::text,v_row.approved_source_ex_vat,
               case when v_row.source_pay_channel='PAYE' then v_row.realised_source_ex_vat else 0::numeric end,
               case when v_row.source_pay_channel='PAYE' then v_row.held_source_ex_vat else 0::numeric end,
               case when v_row.source_pay_channel='UMBRELLA' then v_row.realised_source_ex_vat else 0::numeric end,
               case when v_row.source_pay_channel='UMBRELLA' then v_row.held_source_ex_vat else 0::numeric end) then
          v_issue:='SOURCE_BOOK_APPROVAL_UNBOUND';
        end if;
      end if;
      if v_issue is not null then
        update private.bpay_next_run_worker set status='REVIEW',review_issue_code=v_issue,
          review_issue_work_id=v_row.work_id where id=v_worker.id;
        return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,v_seen);
      end if;
    end if;
    v_collection:=null;
    -- Only the exact typed original ordinary PAYE liability can own a
    -- negative residual. Unknown signed/cross-basis/held shapes still REVIEW.
    if v_worker.target_pay_channel='PAYE' and v_row.source_pay_channel='PAYE'
       and v_row.approved_source_ex_vat-v_row.realised_source_ex_vat<0
       and (v_row.held_source_ex_vat,v_row.held_target_ex_vat,v_row.held_target_vat,v_row.held_target_inc_vat)
         is not distinct from (0::numeric,0::numeric,0::numeric,0::numeric) then
      select b.* into v_collection from private.bpay_next_work_collection b
        where b.work_id=v_row.work_id and b.component_key=v_row.component_key;
      if v_collection.case_id is not null then
        v_collection:=private.bpay_next_work_collection_origin_v1(
          v_collection.case_id,v_collection.case_component_id,v_worker.candidate_id);
      else v_collection:=null;end if;
      if v_collection.id is not null and
         (v_collection.observed_revision_id,v_collection.observed_approved_source_ex_vat,v_collection.observed_realised_source_ex_vat)
           is distinct from (v_row.captured_revision_id,v_row.approved_source_ex_vat,v_row.realised_source_ex_vat) then
        v_collection:=null;
      end if;
    end if;
    v_needs_resolution:=v_collection.id is null and (
       v_row.approved_line_id is null
       or v_row.approved_source_ex_vat<0
       or ((v_row.realised_source_ex_vat<>0
          or v_row.held_source_ex_vat<>0
          or v_row.realised_target_inc_vat<>0
          or v_row.held_target_inc_vat<>0)
         and v_row.source_basis_channel is distinct from v_row.source_pay_channel)
       or v_row.realised_source_ex_vat<0
       or v_row.realised_target_ex_vat<0 or v_row.realised_target_vat<0 or v_row.realised_target_inc_vat<0
       or (v_row.held_source_ex_vat>0 and v_row.approved_source_ex_vat-v_row.realised_source_ex_vat-v_row.held_source_ex_vat<=0)
       or (v_row.held_source_ex_vat=0 and v_row.held_target_inc_vat<>0)
       or v_row.approved_source_ex_vat-v_row.realised_source_ex_vat-v_row.held_source_ex_vat<0);
    -- Only genuine positive Source WORK consumes the retained disposition.
    -- Expense/additional/ordinary, zero, realised/held-away, managed negative
    -- and already-unresolved financial shapes do not become query holds.
    -- The actual pin validated the typed origin before this immutable row;
    -- missing evidence is REVIEW, never a lazy live Source reader fallback.
    if v_row.work_kind='SOURCE' and v_row.source_kind in ('SOURCE','PROTECTED')
       and v_row.source_event_id is not null
       and v_row.component_kind in ('WORK','PROTECTED_WORK')
       and not v_needs_resolution
       and v_row.approved_source_ex_vat-v_row.realised_source_ex_vat-v_row.held_source_ex_vat>0 then
      select qp.* into v_query_pin from private.bpay_next_run_work_query_pin_v2 qp
        where qp.run_work_id=v_row.run_work_id;
      if found and (v_query_pin.run_worker_id,v_query_pin.candidate_id,
          v_query_pin.work_id,v_query_pin.captured_revision_id,
          v_query_pin.prepare_job_id,v_query_pin.module_epoch,v_query_pin.gate_version)
          is distinct from (v_worker.id,v_worker.candidate_id,
          v_row.work_id,v_row.captured_revision_id,p_job_id,v_module_epoch,'PAY_QUERY_STATE_V3'::text) then
        raise exception using errcode='23514',message='BPAY_NEXT_CAPTURE_QUERY_PIN_MISMATCH';
      end if;
      if not found or v_query_pin.disposition='UNAVAILABLE' then
        v_issue:='SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE';
      elsif v_query_pin.disposition='BLOCKED' then
        v_issue:='SOURCE_PAY_QUERY_OPEN';
      elsif v_query_pin.disposition<>'CLEAR' or v_query_pin.scope is null
         or v_query_pin.query_state_sha256 is null
         or pg_catalog.octet_length(v_query_pin.query_state_sha256)<>32 then
        raise exception using errcode='23514',message='BPAY_NEXT_CAPTURE_QUERY_PIN_INVALID';
      end if;
      if v_issue is not null then
        update private.bpay_next_run_worker
          set status='REVIEW',review_issue_code=v_issue,review_issue_work_id=v_row.work_id
          where id=v_worker.id;
        return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,v_seen);
      end if;
    end if;
    select count(*) into v_segments from (
      select s.segment_key from private.bpay_next_approved_segment s
        where s.revision_id=v_row.captured_revision_id and s.component_key=v_row.component_key
      union select s.segment_key from private.bpay_next_work_segment_origin_share s
        where s.work_id=v_row.work_id and s.component_key=v_row.component_key) members;
    if v_segments>0 and v_row.work_kind='ORDINARY' then
      -- Exact segment negative ownership is sealed after complete child capture.
      -- Initial native net-negative classification cannot bypass that census.
      if v_collection.id is not null then v_needs_resolution:=true;end if;
      v_collection:=null;
    end if;
    v_snooze:=null;
    if v_segments=0 then
      v_snooze:=private.bpay_next_snooze_at_cut_v1(v_row.work_id,v_row.captured_revision_id,
        null,v_row.component_key,v_run.snooze_control_sequence,v_run.snooze_business_date);
    end if;
    insert into private.bpay_next_run_position
      (run_worker_id,run_work_id,work_id,captured_revision_id,
       component_key,approved_line_id,source_pay_channel,
       source_basis_channel,
       approved_source_ex_vat,realised_source_ex_vat,held_source_ex_vat,
       residual_source_ex_vat,realised_target_ex_vat,realised_target_vat,
       realised_target_inc_vat,held_target_ex_vat,held_target_vat,
       held_target_inc_vat,needs_financial_resolution,work_collection_id,work_collection_revision,
       source_binding_version_id,canonical_source_work_id,canonical_source_component_key,source_book_revision,
       segment_expected_member_count,segment_attribution_complete,snooze_id,snooze_version,snooze_excluded_positive_ex_vat)
      values(v_worker.id,v_row.run_work_id,v_row.work_id,
       v_row.captured_revision_id,v_row.component_key,v_row.approved_line_id,
       v_row.source_pay_channel,v_row.source_basis_channel,
       v_row.approved_source_ex_vat,
       v_row.realised_source_ex_vat,v_row.held_source_ex_vat,
       v_row.approved_source_ex_vat-v_row.realised_source_ex_vat
         -v_row.held_source_ex_vat,
       v_row.realised_target_ex_vat,v_row.realised_target_vat,
       v_row.realised_target_inc_vat,v_row.held_target_ex_vat,
       v_row.held_target_vat,v_row.held_target_inc_vat,
       v_needs_resolution,v_collection.id,v_collection.reconcile_revision,
       v_binding.id,v_binding.canonical_work_id,v_binding.canonical_component_key,
       case when v_binding.id is null then null else v_row.source_book_revision end,
       v_segments,v_segments=0,(v_snooze->>'id')::uuid,(v_snooze->>'version')::bigint,
       case when v_snooze is not null then greatest(v_row.approved_source_ex_vat-v_row.realised_source_ex_vat-v_row.held_source_ex_vat,0) else 0 end);
    if v_needs_resolution then
      v_resolution_count:=v_resolution_count+1;
    end if;
    if v_collection.id is not null then v_managed_seen:=v_managed_seen+1;end if;
    v_work_cursor:=v_row.work_id;
    v_component_cursor:=v_row.component_key;
    v_seen:=v_seen+1;
    if v_member.selection_mode='SUBSET' then
      v_selected_seen:=v_selected_seen+1;
    end if;
  end loop;
  if v_resolution_count>0 then
    update private.bpay_next_run_worker
      set financial_resolution_count=financial_resolution_count+v_resolution_count
      where id=v_worker.id;
  end if;
  if v_seen>0 then
    update private.bpay_next_run_worker
      set captured_position_count=captured_position_count+v_seen,
          expected_managed_position_count=expected_managed_position_count+v_managed_seen,
          captured_selected_component_count=
            captured_selected_component_count+v_selected_seen
      where id=v_worker.id;
  end if;
  if v_seen<p_limit then
    if v_worker.captured_selected_component_count+v_selected_seen
       <>v_worker.expected_selected_component_count then
      select rw.work_id into strict v_issue_work_id
        from private.bpay_next_run_work rw
        join private.bpay_next_work_choice choice
          on choice.run_id=v_run_id and choice.work_id=rw.work_id
         and choice.selection_mode='SUBSET'
        where rw.run_worker_id=v_worker.id
        order by rw.work_id limit 1;
      update private.bpay_next_run_worker
        set status='REVIEW',review_issue_code='CAPTURE_POSITION_CHANGED',
            review_issue_work_id=v_issue_work_id
        where id=v_worker.id;
      return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,v_seen);
    end if;
    v_segment_result:=private.bpay_next_capture_segment_page_v1(v_worker.id,p_limit-v_seen);
    v_segment_visited:=(v_segment_result->>'rows_visited')::integer;
    select w.* into strict v_worker from private.bpay_next_run_worker w where w.id=v_worker.id;
    if v_worker.status='REVIEW' then
      return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,v_seen+v_segment_visited);
    end if;
    if not (v_segment_result->>'done')::boolean then
      update private.bpay_next_job set position_work_cursor=v_work_cursor,position_component_cursor=v_component_cursor where id=p_job_id;
      return jsonb_build_object('done',false,'phase','RESERVE','rows_visited',v_seen+v_segment_visited,
        'work_cursor',v_work_cursor,'component_cursor',v_component_cursor,'replay',false);
    end if;
    update private.bpay_next_job
      set phase='ALLOCATE',position_work_cursor=null,
          position_component_cursor=null
      where id=p_job_id;
    return pg_catalog.jsonb_build_object('done',true,'phase','ALLOCATE',
      'rows_visited',v_seen+v_segment_visited,'replay',false);
  end if;
  update private.bpay_next_job
    set position_work_cursor=v_work_cursor,
        position_component_cursor=v_component_cursor
    where id=p_job_id;
  return pg_catalog.jsonb_build_object('done',false,'phase','RESERVE',
    'rows_visited',v_seen+v_segment_visited,'work_cursor',v_work_cursor,
    'component_cursor',v_component_cursor,'replay',false);
end
$function$;

create or replace function private.bpay_next_hold_fresh_position_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,
  p_expected_work_cursor uuid,p_expected_component_cursor text,
  p_limit integer default 100
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_job private.bpay_next_job%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_module_epoch bigint;
  v_issue_work_id uuid;
  v_row record;
  v_position private.bpay_next_position%rowtype;
  v_value record;v_book record;
  v_run_line_id uuid;v_hold_id uuid;v_included_source numeric(18,2);v_freeze_zero boolean;
  v_seen integer:=0;
  v_affected_work_ids uuid[]:=array[]::uuid[];
  v_work_cursor uuid;
  v_component_cursor text;
  v_line_count bigint;
  v_managed_count bigint;
  v_total_ex numeric(18,2);
  v_total_vat numeric(18,2);
  v_total_inc numeric(18,2);
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null
     or p_limit not between 1 and 100
     or (p_expected_work_cursor is null and p_expected_component_cursor is not null) then
    raise exception using errcode='22023',message='BPAY_NEXT_HOLD_INPUT_INVALID';
  end if;
  select rc.run_id into strict v_run_id
    from private.bpay_next_job j
    join private.bpay_next_run_command rc on rc.command_id=j.command_id
    where j.id=p_job_id and j.job_kind='PREPARE';
  select m.owner_epoch into v_module_epoch
    from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_module_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select * into strict v_run from private.bpay_next_pay_run
    where id=v_run_id for update;
  select candidate_id into strict v_job.candidate_id
    from private.bpay_next_job where id=p_job_id;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_job.candidate_id for update;
  select * into strict v_job from private.bpay_next_job
    where id=p_job_id for update;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED'
     or v_job.job_kind<>'PREPARE' or v_job.status<>'LEASED'
     or v_job.phase not in ('ALLOCATE','FINANCE_ALLOCATE')
     or v_job.module_epoch<>v_module_epoch
     or v_job.lease_nonce<>p_lease_nonce
     or v_job.owner_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp() then
    raise exception using errcode='55000',message='BPAY_NEXT_HOLD_LEASE_STALE';
  end if;
  if v_job.phase='FINANCE_ALLOCATE' then
    return pg_catalog.jsonb_build_object('done',true,
      'phase','FINANCE_ALLOCATE','rows_visited',0,'replay',true);
  end if;
  if v_job.position_work_cursor is distinct from p_expected_work_cursor
     or v_job.position_component_cursor is distinct from p_expected_component_cursor then
    return pg_catalog.jsonb_build_object('done',false,'phase','ALLOCATE',
      'rows_visited',0,'replay',true,
      'work_cursor',v_job.position_work_cursor,
      'component_cursor',v_job.position_component_cursor);
  end if;
  select * into strict v_worker from private.bpay_next_run_worker
    where run_id=v_run_id and candidate_id=v_job.candidate_id for update;
  if v_worker.status<>'PREPARING' or v_worker.review_issue_code is not null then
    raise exception using errcode='55000',
      message='BPAY_NEXT_HOLD_WORKER_NOT_PREPARING';
  end if;
  if v_worker.financial_resolution_count>0 then
    select rp.work_id into strict v_issue_work_id
      from private.bpay_next_run_position rp
      where rp.run_worker_id=v_worker.id and rp.needs_financial_resolution
      order by rp.work_id,rp.component_key limit 1;
    update private.bpay_next_run_worker
      set status='REVIEW',review_issue_code='FINANCIAL_RESOLUTION_REQUIRED',
          review_issue_work_id=v_issue_work_id
      where id=v_worker.id;
    return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,0);
  end if;
  if v_worker.target_pay_channel='UMBRELLA'
     and (v_worker.target_umbrella_id is null
       or v_worker.target_umbrella_vat_chargeable is null) then
    select rp.work_id into v_issue_work_id
      from private.bpay_next_run_position rp
      where rp.run_worker_id=v_worker.id
      order by rp.work_id,rp.component_key limit 1;
    if v_issue_work_id is not null then
      update private.bpay_next_run_worker
        set status='REVIEW',review_issue_code='TARGET_UMBRELLA_MISSING',
            review_issue_work_id=v_issue_work_id
        where id=v_worker.id;
      return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,0);
    end if;
  end if;
  v_work_cursor:=v_job.position_work_cursor;
  v_component_cursor:=v_job.position_component_cursor;
  v_line_count:=v_worker.captured_line_count;
  v_managed_count:=v_worker.captured_managed_position_count;
  v_total_ex:=v_worker.gross_ex_vat;
  v_total_vat:=v_worker.gross_vat;
  v_total_inc:=v_worker.gross_inc_vat;
  for v_row in
    select rp.*,rw.id as pinned_work_id,rev.valuation_policy_id
      from private.bpay_next_run_position rp
      join private.bpay_next_run_work rw on rw.id=rp.run_work_id
      join private.bpay_next_work_revision rev
        on rev.id=rp.captured_revision_id and rev.work_id=rp.work_id
      where rp.run_worker_id=v_worker.id
        and (v_work_cursor is null or rp.work_id>v_work_cursor
          or (rp.work_id=v_work_cursor
            and rp.component_key>v_component_cursor))
      order by rp.work_id,rp.component_key limit p_limit
  loop
    if not v_row.segment_attribution_complete then
      raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_CAPTURE_INCOMPLETE';
    end if;
    v_included_source:=case when v_row.segment_member_count>0 then v_row.segment_included_positive_ex_vat
      else greatest(v_row.residual_source_ex_vat,0)-v_row.snooze_excluded_positive_ex_vat end;
    v_freeze_zero:=v_row.approved_line_id is not null and v_row.approved_source_ex_vat=0 and v_row.realised_source_ex_vat=0 and v_row.held_source_ex_vat=0
      and v_row.snooze_id is null and (v_row.segment_member_count=0 or exists(
        select 1 from private.bpay_next_run_work_segment_parent p join private.bpay_next_run_work_segment s on s.id=p.run_work_segment_id
          where p.run_work_id=v_row.run_work_id and p.component_key=v_row.component_key and s.disposition='INCLUDED'));
    if v_row.needs_financial_resolution
       or (v_row.work_collection_id is null and
         ((v_row.approved_line_id is null and (v_row.segment_member_count=0 or v_included_source>0)) or v_row.residual_source_ex_vat<0)) then
      raise exception using errcode='23514',
        message='BPAY_NEXT_HOLD_UNEXPECTED_POSITION_SHAPE';
    end if;
    select * into strict v_position from private.bpay_next_position
      where work_id=v_row.work_id and component_key=v_row.component_key
      for update;
    if v_position.applied_revision_id is distinct from v_row.captured_revision_id
       or v_position.approved_source_ex_vat is distinct from
         v_row.approved_source_ex_vat
       or v_position.realised_source_ex_vat is distinct from
         v_row.realised_source_ex_vat
       or v_position.held_source_ex_vat is distinct from
         v_row.held_source_ex_vat
       or v_position.source_basis_channel is distinct from
         v_row.source_basis_channel
       or v_position.realised_target_ex_vat is distinct from
         v_row.realised_target_ex_vat
       or v_position.realised_target_vat is distinct from
         v_row.realised_target_vat
       or v_position.realised_target_inc_vat is distinct from
         v_row.realised_target_inc_vat
       or v_position.held_target_ex_vat is distinct from
         v_row.held_target_ex_vat
       or v_position.held_target_vat is distinct from
         v_row.held_target_vat
       or v_position.held_target_inc_vat is distinct from
         v_row.held_target_inc_vat then
      raise exception using errcode='55000',
        message='BPAY_NEXT_HOLD_CURRENT_POSITION_CHANGED';
    end if;
    if v_row.source_binding_version_id is not null then
      select s.* into strict v_book from private.bpay_next_bound_source_snapshot_v1(
        v_row.source_binding_version_id,v_row.source_book_revision) s;
      if v_book.qualification_state is distinct from 'READY'
         or (v_book.canonical_work_id,v_book.canonical_component_key,v_book.approval_work_id,
             v_book.approval_revision_id,v_book.approved_line_id,v_book.source_channel,v_book.tax_treatment,
             v_book.approved_source_ex_vat,v_book.native_realised_paye,v_book.native_held_paye,
             v_book.native_realised_umbrella,v_book.native_held_umbrella)
           is distinct from (v_row.work_id,v_row.component_key,v_row.work_id,
             v_row.captured_revision_id,v_row.approved_line_id,v_row.source_pay_channel,'TAXABLE'::text,
             v_row.approved_source_ex_vat,
             case when v_row.source_pay_channel='PAYE' then v_row.realised_source_ex_vat else 0::numeric end,
             case when v_row.source_pay_channel='PAYE' then v_row.held_source_ex_vat else 0::numeric end,
             case when v_row.source_pay_channel='UMBRELLA' then v_row.realised_source_ex_vat else 0::numeric end,
             case when v_row.source_pay_channel='UMBRELLA' then v_row.held_source_ex_vat else 0::numeric end)
         or v_row.source_pay_channel not in ('PAYE','UMBRELLA')
         or v_worker.target_pay_channel is distinct from v_row.source_pay_channel
         or v_row.canonical_source_work_id is distinct from v_row.work_id
         or v_row.canonical_source_component_key is distinct from v_row.component_key then
        raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_CAPTURE_CHANGED';
      end if;
    end if;
    if v_row.work_collection_id is not null then
      -- Classification and original provenance were frozen by RESERVE. Never
      -- reread a newer WORK approval or create an ordinary negative/zero hold.
      update private.bpay_next_run_position set managed_capture_completed=true
        where id=v_row.id and not managed_capture_completed;
      if not found then raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_CAPTURE_ALREADY_SPENT';end if;
      v_managed_count:=v_managed_count+1;
    end if;
    if v_included_source>0 or v_freeze_zero then
    select * into strict v_value
      from private.bpay_next_value_source_for_target_v1(
        v_included_source,v_row.source_pay_channel,
        v_worker.target_pay_channel,
        case when v_worker.target_pay_channel='UMBRELLA'
          then v_worker.target_umbrella_vat_chargeable else false end,
        v_row.valuation_policy_id,v_run.pay_date);
    v_line_count:=v_line_count+1;
    insert into private.bpay_next_run_line
      (run_worker_id,run_work_id,line_no,approved_line_id,
       captured_revision_id,work_id,component_key,source_consumed_ex_vat,
       source_pay_channel,target_pay_channel,valuation_policy_id,
       policy_window_id,frozen_ex_vat,frozen_vat,frozen_inc_vat,source_binding_version_id,
       canonical_source_work_id,canonical_source_component_key,source_book_revision,updates_original_source_scalar)
      values(v_worker.id,v_row.run_work_id,v_line_count,
       v_row.approved_line_id,v_row.captured_revision_id,v_row.work_id,
       v_row.component_key,v_included_source,
       v_row.source_pay_channel,v_worker.target_pay_channel,
       v_row.valuation_policy_id,v_value.policy_window_id,
       v_value.target_ex_vat,v_value.target_vat,v_value.target_inc_vat,
       v_row.source_binding_version_id,v_row.canonical_source_work_id,v_row.canonical_source_component_key,
       v_row.source_book_revision,case when v_row.source_binding_version_id is null then null else true end)
      returning id into v_run_line_id;
    -- An authorised zero still has immutable shift/rate advice detail. It
    -- becomes a frozen zero line, never a zero-valued financial hold.
    if v_included_source>0 then
      insert into private.bpay_next_hold
        (run_line_id,work_id,component_key,source_reserved_ex_vat,
         target_amount_ex_vat,target_amount_vat,target_amount_inc_vat,status)
        values(v_run_line_id,v_row.work_id,v_row.component_key,
         v_included_source,v_value.target_ex_vat,
         v_value.target_vat,v_value.target_inc_vat,'ACTIVE') returning id into v_hold_id;
      perform private.bpay_next_record_work_segment_origin_shares_v1(v_run_line_id,v_hold_id);
      update private.bpay_next_position
        set held_source_ex_vat=held_source_ex_vat+v_included_source,
            held_target_ex_vat=held_target_ex_vat+v_value.target_ex_vat,
            held_target_vat=held_target_vat+v_value.target_vat,
            held_target_inc_vat=held_target_inc_vat+v_value.target_inc_vat,
            updated_at_utc=pg_catalog.transaction_timestamp()
        where work_id=v_row.work_id and component_key=v_row.component_key;
    end if;
    v_total_ex:=v_total_ex+v_value.target_ex_vat;
    v_total_vat:=v_total_vat+v_value.target_vat;
    v_total_inc:=v_total_inc+v_value.target_inc_vat;
    end if;
    v_affected_work_ids:=array_append(v_affected_work_ids,v_row.work_id);
    v_work_cursor:=v_row.work_id;
    v_component_cursor:=v_row.component_key;
    v_seen:=v_seen+1;
  end loop;
  if v_seen>0 then
    update private.bpay_next_run_worker
      set captured_line_count=v_line_count,
          captured_managed_position_count=v_managed_count,
          gross_ex_vat=v_total_ex,gross_vat=v_total_vat,
          gross_inc_vat=v_total_inc
      where id=v_worker.id;
    update private.bpay_next_worker_control
      set financial_view_revision=financial_view_revision+1,
          updated_at_utc=pg_catalog.transaction_timestamp()
      where candidate_id=v_job.candidate_id;
    perform private.bpay_next_workbench_refresh_many_v1(v_affected_work_ids,null);
  end if;
  if v_seen<p_limit then
    if v_managed_count<>v_worker.expected_managed_position_count or exists(
      select 1 from private.bpay_next_run_position where run_worker_id=v_worker.id
        and work_collection_id is not null and not managed_capture_completed) then
      raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_CAPTURE_COUNT_MISMATCH';
    end if;
    update private.bpay_next_job
      set phase='FINANCE_ALLOCATE',position_work_cursor=null,
          position_component_cursor=null
      where id=p_job_id;
    return pg_catalog.jsonb_build_object('done',true,
      'phase','FINANCE_ALLOCATE','rows_visited',v_seen,'replay',false);
  end if;
  update private.bpay_next_job
    set position_work_cursor=v_work_cursor,
        position_component_cursor=v_component_cursor
    where id=p_job_id;
  return pg_catalog.jsonb_build_object('done',false,'phase','ALLOCATE',
    'rows_visited',v_seen,'work_cursor',v_work_cursor,
    'component_cursor',v_component_cursor,'replay',false);
end
$function$;

create or replace function private.bpay_next_complete_no_case_worker_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_job private.bpay_next_job%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_module_epoch bigint;v_accepted_empty boolean;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null then
    raise exception using errcode='22023',
      message='BPAY_NEXT_NO_CASE_COMPLETION_INPUT_INVALID';
  end if;
  select rc.run_id into strict v_run_id
    from private.bpay_next_job j
    join private.bpay_next_run_command rc on rc.command_id=j.command_id
    where j.id=p_job_id and j.job_kind='PREPARE';
  select owner_epoch into v_module_epoch
    from private.bpay_next_module_control
    where id=1 and active_owner='NEXT' for share;
  if v_module_epoch is null then
    raise exception using errcode='55000',
      message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select * into strict v_run from private.bpay_next_pay_run
    where id=v_run_id for update;
  select candidate_id into strict v_job.candidate_id
    from private.bpay_next_job where id=p_job_id;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_job.candidate_id for update;
  select * into strict v_job from private.bpay_next_job
    where id=p_job_id for update;
  select * into strict v_worker from private.bpay_next_run_worker
    where run_id=v_run_id and candidate_id=v_job.candidate_id for update;
  if v_job.status='DONE' and v_job.phase='READY'
     and v_worker.status in ('READY','DRAFT','ISSUED','COMPLETE','CANCELLING','CANCELLED') then
    return pg_catalog.jsonb_build_object('done',true,'phase','READY',
      'worker_status',v_worker.status,
      'replay',true,'run_worker_id',v_worker.id);
  end if;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED'
     or v_job.job_kind<>'PREPARE' or v_job.status<>'LEASED'
     or v_job.phase<>'FINANCE_ALLOCATE'
     or v_job.module_epoch<>v_module_epoch
     or v_job.lease_nonce<>p_lease_nonce
     or v_job.owner_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or v_worker.status<>'PREPARING'
     or v_worker.review_issue_code is not null
     or v_worker.financial_resolution_count<>0 then
    raise exception using errcode='55000',
      message='BPAY_NEXT_NO_CASE_COMPLETION_STATE_INVALID';
  end if;
  v_accepted_empty:=v_run.accepted_choice_descriptor is not null
    and exists(select 1 from private.bpay_next_selection_candidate selected where selected.run_id=v_run_id and selected.candidate_id=v_job.candidate_id)
    and not exists(select 1 from private.bpay_next_run_choice_member member where member.run_id=v_run_id
      and member.member_kind='CASE_COMPONENT' and member.candidate_id=v_job.candidate_id)
    and not exists(select 1 from private.bpay_next_case_selection selection where selection.run_id=v_run_id and selection.candidate_id=v_job.candidate_id);
  if v_worker.captured_position_count=0
     or (not v_accepted_empty and v_worker.captured_position_count<>v_worker.captured_line_count)
     or (v_accepted_empty and (v_worker.captured_line_count=0
       or v_worker.captured_position_count<>v_worker.captured_line_count+v_worker.captured_managed_position_count
       or v_worker.expected_managed_position_count<>v_worker.captured_managed_position_count
       or exists(select 1 from private.bpay_next_run_position where run_worker_id=v_worker.id
         and work_collection_id is not null and not managed_capture_completed))) then
    raise exception using errcode='23514',
      message='BPAY_NEXT_NO_CASE_COMPLETION_LINE_COUNT_MISMATCH';
  end if;
  -- Any case, including a paused or closed one, is conservatively deferred
  -- to the common case-allocation owner. This uses candidate_id index EXISTS,
  -- not a scan of payment or Timesheet history.
  if not v_accepted_empty and exists(select 1 from private.bpay_next_finance_case c
       where c.candidate_id=v_job.candidate_id) then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CASE_ALLOCATION_REQUIRED';
  end if;
  update private.bpay_next_run_worker set status='READY'
    where id=v_worker.id;
  perform private.bpay_next_publish_no_case_week_fact_v1(v_worker.id,null);
  update private.bpay_next_worker_control
    set financial_view_revision=financial_view_revision+1,
        updated_at_utc=pg_catalog.transaction_timestamp()
    where candidate_id=v_job.candidate_id;
  return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,0);
end
$function$;

create or replace function private.bpay_next_prepare_case_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,
  p_expected_cursor bigint default null,p_limit integer default 100
) returns jsonb
language plpgsql security invoker
set search_path=pg_catalog,private,public
as $function$
declare
  v_plan jsonb;v_plan_period jsonb;v_plan_row jsonb;v_plan_instructions jsonb;
  v_epoch bigint;v_run_id uuid;v_candidate uuid;v_saved_cursor bigint;
  v_run private.bpay_next_pay_run%rowtype;
  v_control private.bpay_next_worker_control%rowtype;
  v_job private.bpay_next_job%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_selection private.bpay_next_case_selection%rowtype;
  v_state private.bpay_next_case_allocation_state%rowtype;
  v_channel private.bpay_next_case_allocation_channel%rowtype;
  v_week private.bpay_next_worker_week%rowtype;
  v_case private.bpay_next_finance_case%rowtype;
  v_component private.bpay_next_case_component%rowtype;
  v_rule private.bpay_next_case_rule%rowtype;
  v_period_rule private.bpay_next_case_rule%rowtype;
  v_origin private.bpay_next_case_create_request%rowtype;
  v_collection private.bpay_next_work_collection%rowtype;
  v_policy_id uuid;v_case_cut jsonb;v_memory_cut boolean;v_case_due date;v_case_status text;
  v_automatic boolean;v_week_not_affected boolean;
  v_period private.bpay_next_case_period%rowtype;
  v_item record;v_prior record;v_value record;v_row record;
  v_instruction private.bpay_next_run_case_instruction%rowtype;
  v_floor numeric;v_outstanding numeric;v_case_outstanding numeric;
  v_nominal numeric;v_opening_due numeric;v_capacity numeric;v_taken numeric;v_e numeric;v_h numeric;
  v_next_e numeric;v_next_h numeric;
  v_gross numeric;v_week_start date;v_age timestamptz;v_age_key bigint;
  v_purpose text;v_explanation text;v_issue text;v_seen bigint:=0;
  v_result_id uuid;v_hold_id uuid;v_instruction_id uuid;v_has_more boolean;v_terminal boolean:=false;
  v_cap_reason text;v_affordability_reason text;v_capacity_reason text;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null or p_owner_epoch<1
     or p_expected_cursor<0 or p_limit is null or p_limit not between 1 and 100 then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_PREPARE_INPUT_INVALID';
  end if;
  select rc.run_id,j.candidate_id into strict v_run_id,v_candidate
    from private.bpay_next_job j join private.bpay_next_run_command rc on rc.command_id=j.command_id
    where j.id=p_job_id and j.job_kind='PREPARE';
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  -- Same lock order as the real lane; root UPDATE fences confirmation/cancel.
  select r.* into strict v_run from private.bpay_next_pay_run r where r.id=v_run_id for update;
  select c.* into strict v_control from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update;
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id for update;
  select w.* into strict v_worker from private.bpay_next_run_worker w
    where w.run_id=v_run_id and w.candidate_id=v_candidate for update;
  select s.* into v_state from private.bpay_next_case_allocation_state s where s.job_id=p_job_id
    and s.run_worker_id=v_worker.id and s.pass_kind='DRAFT' and s.projection_no=0 for update;
  if v_job.module_epoch<>v_epoch then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_PREPARE_OWNER_STALE';
  end if;
  if v_job.status='DONE' and v_job.phase='READY' and v_state.status='COMPLETE'
     and v_worker.status in ('READY','DRAFT','ISSUED','COMPLETE','CANCELLING','CANCELLED') then
    return pg_catalog.jsonb_build_object('done',true,'phase','READY','cursor',null,'rows_visited','0',
      'replay',true,'run_worker_id',v_worker.id,'worker_status',v_worker.status);
  end if;
  if v_job.status='DONE' and v_job.phase='DONE' and v_worker.status='REVIEW'
     and v_worker.review_issue_code in ('CASE_SELECTION_REQUIRED','CASE_TARGET_UNSUPPORTED','CASE_INPUT_UNBOUND',
       'CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND','CASE_NO_PAYABLE_AMOUNT') then
    return pg_catalog.jsonb_build_object('done',true,'phase','REVIEW','cursor',null,'rows_visited','0',
      'replay',true,'run_worker_id',v_worker.id,'worker_status',v_worker.status,'issue_code',v_worker.review_issue_code);
  end if;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED' or v_job.status<>'LEASED'
     or v_job.phase<>'FINANCE_ALLOCATE' or v_job.lease_nonce<>p_lease_nonce or v_job.owner_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp() or v_control.active_owner_epoch<>p_owner_epoch
     or v_worker.status<>'PREPARING' or v_worker.review_issue_code is not null
     or v_worker.financial_resolution_count<>0 then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_PREPARE_STATE_INVALID';
  end if;
  if v_worker.captured_position_count<>v_worker.captured_line_count+v_worker.captured_managed_position_count
     or v_worker.expected_managed_position_count<>v_worker.captured_managed_position_count
     or exists(select 1 from private.bpay_next_run_position where run_worker_id=v_worker.id
       and work_collection_id is not null and not managed_capture_completed) then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREPARE_LINE_COUNT_MISMATCH';
  end if;
  if v_job.cursor_key is not null and v_job.cursor_key !~ '^[0-9]{1,19}$' then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREPARE_CURSOR_CORRUPT';
  end if;
  v_saved_cursor:=coalesce(v_job.cursor_key::bigint,0);
  if coalesce(p_expected_cursor,0)<v_saved_cursor then
    -- Lost STEP reply: read back the persisted checkpoint without a second
    -- capture, hold, cursor spend or financial-view bump. Ahead is not replay.
    return pg_catalog.jsonb_build_object('done',false,'phase','FINANCE_ALLOCATE','cursor',v_job.cursor_key,
      'rows_visited','0','replay',true,'run_worker_id',v_worker.id,'worker_status',v_worker.status);
  end if;
  if coalesce(p_expected_cursor,0)>v_saved_cursor then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_PREPARE_CURSOR_STALE';
  end if;
  -- Only a positively proved no-case context may retain the old owner path.
  if v_state.id is null and not exists(select 1 from private.bpay_next_case_selection s where s.run_id=v_run_id and s.candidate_id=v_candidate)
     and (not exists(select 1 from private.bpay_next_finance_case c where c.candidate_id=v_candidate)
       or (v_run.accepted_choice_descriptor is not null
         and exists(select 1 from private.bpay_next_selection_candidate selected where selected.run_id=v_run_id and selected.candidate_id=v_candidate)
         and not exists(select 1 from private.bpay_next_run_choice_member member where member.run_id=v_run_id
           and member.member_kind='CASE_COMPONENT' and member.candidate_id=v_candidate))) then
    return private.bpay_next_complete_no_case_worker_v1(p_job_id,p_lease_nonce,p_owner_epoch);
  end if;

  <<prepare_page>>
  begin
    -- A resumed weekly/capture/allocation page uses its exact frozen selection
    -- revision. Refuse Source origins before any stage writes, including when
    -- the collection's current qualification reader would return no row.
    if v_state.id is not null and exists(
      select 1 from private.bpay_next_case_selection_item si
      join private.bpay_next_work_collection c on c.case_id=si.case_id
        and c.case_component_id=si.case_component_id and c.candidate_id=si.candidate_id
      join private.bpay_next_work w on w.id=c.work_id and w.candidate_id=si.candidate_id
      where si.run_id=v_run_id and si.candidate_id=v_candidate
        and si.selection_revision=v_state.selection_revision and si.is_selected
        and w.work_kind='SOURCE') then
      raise exception using errcode='55000',message='BPAY_NEXT_COMPARISON_NOT_INSTALLED';
    end if;
    if v_state.id is null then
      select s.* into v_selection from private.bpay_next_case_selection s
        where s.run_id=v_run_id and s.candidate_id=v_candidate order by s.selection_revision desc limit 1;
      if v_selection.run_id is null or v_selection.status<>'SEALED' then
        v_issue:='CASE_SELECTION_REQUIRED';exit prepare_page;
      end if;
      -- Comparison is owned by the later CM chain. Existing immutable Source
      -- origin identity is enough to refuse it; no absent pack column/type or
      -- fabricated financial certificate is consulted or created here.
      if exists(select 1 from private.bpay_next_case_selection_item si
        join private.bpay_next_work_collection c on c.case_id=si.case_id
          and c.case_component_id=si.case_component_id and c.candidate_id=si.candidate_id
        join private.bpay_next_work w on w.id=c.work_id and w.candidate_id=si.candidate_id
        where si.run_id=v_run_id and si.candidate_id=v_candidate
          and si.selection_revision=v_selection.selection_revision and si.is_selected
          and w.work_kind='SOURCE') then
        raise exception using errcode='55000',message='BPAY_NEXT_COMPARISON_NOT_INSTALLED';
      end if;
      if v_worker.target_pay_channel<>'PAYE' or v_worker.gross_vat<>0
         or v_worker.gross_ex_vat<>v_worker.gross_inc_vat then
        v_issue:='CASE_TARGET_UNSUPPORTED';exit prepare_page;
      end if;
      select selected.min_take_home_wtd into strict v_floor from private.bpay_next_selection_candidate selected
        where selected.run_id=v_run_id and selected.candidate_id=v_candidate;
      if v_floor is null or v_floor::text in ('NaN','Infinity','-Infinity') or v_floor<0
         or v_floor<>pg_catalog.trunc(v_floor,2) then
        v_issue:='CASE_INPUT_UNBOUND';exit prepare_page;
      end if;
      v_week_start:=v_run.pay_date-(extract(isodow from v_run.pay_date)::integer-1);
      -- OVERPAYMENT gross recovery does not consume the protected-debt H/floor
      -- operand (0290). Prove the exact nonempty SEALED selected set, not merely
      -- that no protected row happened to be found. Missing identities/counts
      -- remain week-dependent; instruction capture and allocation re-prove it.
      select count(*)=v_selection.selected_count and count(*)>0
        and coalesce(bool_and((fc.case_kind,cc.case_kind,cc.instruction_kind,cc.payroll_stage)
          is not distinct from ('OVERPAYMENT','OVERPAYMENT','RECOVERY','GROSS_DEDUCT')),false)
        into v_week_not_affected
        from private.bpay_next_case_selection_item si
        left join private.bpay_next_case_component cc
          on cc.id=si.case_component_id and cc.case_id=si.case_id and cc.candidate_id=si.candidate_id
        left join private.bpay_next_finance_case fc on fc.id=si.case_id and fc.candidate_id=si.candidate_id
        where si.run_id=v_selection.run_id and si.candidate_id=v_selection.candidate_id
          and si.selection_revision=v_selection.selection_revision and si.is_selected;
      -- Exact indexed metadata absence is required BEFORE establishing a new
      -- zero week. Missing prior NEW facts are unresolved, not zero/history SUM.
      if not v_week_not_affected and exists(select 1 from private.bpay_next_run_worker w
        join private.bpay_next_pay_run r on r.id=w.run_id
        where w.candidate_id=v_candidate and w.status in ('READY','DRAFT','ISSUED','COMPLETE')
          and r.pay_date>=v_week_start and r.pay_date<v_week_start+7
          and (r.pay_date,r.created_at_utc,w.id)<(v_run.pay_date,v_run.created_at_utc,v_worker.id)
          and not exists(select 1 from private.bpay_next_worker_week_contribution x where x.original_run_worker_id=w.id)) then
        v_issue:='CASE_WEEK_BASIS_UNBOUND';exit prepare_page;
      end if;
      insert into private.bpay_next_worker_week(candidate_id,pay_week_start,period_revision,
        default_floor,default_floor_revision,resolved_arranged_take_home)
        values(v_candidate,v_week_start,1,v_floor,1,0) on conflict(candidate_id,pay_week_start) do nothing;
      select w.* into strict v_week from private.bpay_next_worker_week w
        where w.candidate_id=v_candidate and w.pay_week_start=v_week_start for update;
      if not v_week_not_affected and v_week.unresolved_contribution_count<>0 then v_issue:='CASE_WEEK_BASIS_UNBOUND';exit prepare_page;end if;
      if v_week.default_floor<>v_floor then
        update private.bpay_next_worker_week set default_floor=v_floor,default_floor_revision=default_floor_revision+1,
          period_revision=period_revision+1 where candidate_id=v_candidate and pay_week_start=v_week_start returning * into v_week;
      end if;
      update private.bpay_next_run_worker set case_selection_revision=v_selection.selection_revision where id=v_worker.id;
      v_worker.case_selection_revision:=v_selection.selection_revision;
      insert into private.bpay_next_case_allocation_state
        (run_worker_id,run_id,candidate_id,selection_revision,preparation_revision,pass_kind,projection_no,
         command_id,job_id,module_epoch,owner_epoch,financial_view_revision,status,weekly_binding_state,
         expected_instruction_count,initial_worker_take_home,remaining_worker_take_home,prepare_stage,
         captured_default_floor,captured_default_floor_revision,pay_week_start,captured_week_revision,captured_work_gross)
        values(v_worker.id,v_run_id,v_candidate,v_selection.selection_revision,v_worker.preparation_revision,'DRAFT',0,
         v_job.command_id,p_job_id,v_epoch,p_owner_epoch,v_control.financial_view_revision,'BUILDING',
         case when v_week_not_affected then 'NOT_AFFECTED' else 'UNBOUND' end,
         v_selection.selected_count,v_worker.gross_ex_vat,v_worker.gross_ex_vat,
         case when v_week_not_affected then 'INSTRUCTION_CAPTURE' else 'WEEK_CAPTURE' end,
         v_floor,v_week.default_floor_revision,v_week_start,v_week.period_revision,v_worker.gross_ex_vat)
        returning * into v_state;
      insert into private.bpay_next_case_allocation_channel(state_id,allocation_channel,initial_headroom,remaining_headroom)
        values(v_state.id,'PAYE',v_worker.gross_ex_vat,v_worker.gross_ex_vat);
    else
      if v_state.status<>'BUILDING' or v_state.preparation_revision<>v_worker.preparation_revision
         or v_state.selection_revision<>v_worker.case_selection_revision or v_state.prepare_stage='NOT_CASE_PREPARE' then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREPARE_BINDING_MISMATCH';
      end if;
      -- Lease takeover does not reset the captured economic cursors/headroom.
      if v_state.owner_epoch<>p_owner_epoch then
        update private.bpay_next_case_allocation_state set owner_epoch=p_owner_epoch where id=v_state.id;
        v_state.owner_epoch:=p_owner_epoch;
      end if;
    end if;

    if v_state.prepare_stage='WEEK_CAPTURE' then
      for v_prior in select x.* from private.bpay_next_worker_week_contribution x
        where x.candidate_id=v_candidate and x.pay_week_start=v_state.pay_week_start
          and (x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id)
            <(v_run.pay_date,v_run.created_at_utc,v_worker.id)
          and (v_state.prior_worker_cursor is null or
            (x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id)
              >(v_state.prior_pay_date_cursor,v_state.prior_created_at_cursor,v_state.prior_worker_cursor))
        order by x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id limit p_limit
      loop
        v_seen:=v_seen+1;
        if v_prior.eligibility_state='UNBOUND' then
          v_issue:=case when v_prior.payment_state in ('RETURNED_OWED','REISSUED_PAID')
            then 'CASE_RETURN_FLOOR_UNBOUND' else 'CASE_WEEK_BASIS_UNBOUND' end;
          exit prepare_page;
        end if;
        v_state.captured_prior_take_home:=v_state.captured_prior_take_home+v_prior.eligible_arranged_amount;
        v_state.initial_worker_take_home:=v_state.initial_worker_take_home+v_prior.eligible_arranged_amount;
        v_state.remaining_worker_take_home:=v_state.remaining_worker_take_home+v_prior.eligible_arranged_amount;
        v_state.prior_pay_date_cursor:=v_prior.original_pay_date;
        v_state.prior_created_at_cursor:=v_prior.original_created_at_utc;
        v_state.prior_worker_cursor:=v_prior.original_run_worker_id;
      end loop;
      select exists(select 1 from private.bpay_next_worker_week_contribution x
        where x.candidate_id=v_candidate and x.pay_week_start=v_state.pay_week_start
          and (x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id)<(v_run.pay_date,v_run.created_at_utc,v_worker.id)
          and (v_state.prior_worker_cursor is null or (x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id)
            >(v_state.prior_pay_date_cursor,v_state.prior_created_at_cursor,v_state.prior_worker_cursor))) into v_has_more;
      update private.bpay_next_case_allocation_state set captured_prior_take_home=v_state.captured_prior_take_home,
        initial_worker_take_home=v_state.initial_worker_take_home,remaining_worker_take_home=v_state.remaining_worker_take_home,
        prior_pay_date_cursor=v_state.prior_pay_date_cursor,prior_created_at_cursor=v_state.prior_created_at_cursor,
        prior_worker_cursor=v_state.prior_worker_cursor,weekly_binding_state=case when v_has_more then 'UNBOUND' else 'BOUND' end,
        prepare_stage=case when v_has_more then 'WEEK_CAPTURE' else 'INSTRUCTION_CAPTURE' end where id=v_state.id;
      exit prepare_page;
    end if;

    if v_state.prepare_stage='INSTRUCTION_CAPTURE' then
      for v_item in select i.* from private.bpay_next_case_selection_item i
        where i.run_id=v_run_id and i.candidate_id=v_candidate and i.selection_revision=v_state.selection_revision
          and i.is_selected and i.selection_no>v_state.instruction_capture_cursor order by i.selection_no limit p_limit
      loop
        v_seen:=v_seen+1;
        select c.* into strict v_case from private.bpay_next_finance_case c where c.id=v_item.case_id and c.candidate_id=v_candidate for update;
        select c.* into strict v_component from private.bpay_next_case_component c
          where c.id=v_item.case_component_id and c.case_id=v_case.id and c.candidate_id=v_candidate for update;
        -- Original Case/component locks precede the original period lock.
        perform 1 from private.bpay_next_case_period p
          where p.case_component_id=v_component.id and p.pay_week_start=v_state.pay_week_start for update;
        begin
          v_plan:=private.bpay_next_case_allocation_plan_v1(jsonb_build_object('mode','DERIVE',
            'candidate_id',v_candidate,'case_id',v_case.id,'case_component_id',v_component.id,
            'pay_date',v_run.pay_date,'pay_week_start',v_state.pay_week_start,
            'target_pay_channel',v_worker.target_pay_channel,'captured_default_floor',v_state.captured_default_floor,
            'captured_default_floor_revision',v_state.captured_default_floor_revision,
            'weekly_binding_state',v_state.weekly_binding_state,'accepted_run_id',v_run_id));
        exception when check_violation then
          if sqlerrm in ('BPAY_NEXT_CASE_INPUT_UNBOUND','BPAY_NEXT_CASE_TARGET_UNSUPPORTED','BPAY_NEXT_CASE_WEEK_BASIS_UNBOUND') then
            v_issue:=substring(sqlerrm from 11);exit prepare_page;
          else raise;end if;
        end;
        v_plan_period:=v_plan->'period';
        if v_plan->>'period_operation'='INSERT' then
          insert into private.bpay_next_case_period(case_component_id,case_id,candidate_id,pay_week_start,rule_id,
            period_revision,opening_outstanding_source_ex_vat,opening_due_source_ex_vat,work_collection_id)
            values(v_component.id,v_case.id,v_candidate,v_state.pay_week_start,(v_plan_period->>'rule_id')::uuid,
              (v_plan_period->>'period_revision')::bigint,(v_plan_period->>'opening_outstanding_source_ex_vat')::numeric,
              (v_plan_period->>'opening_due_source_ex_vat')::numeric,(v_plan_period->>'work_collection_id')::uuid)
            on conflict(case_component_id,pay_week_start) do nothing;
        end if;
        select p.* into strict v_period from private.bpay_next_case_period p
          where p.case_component_id=v_component.id and p.pay_week_start=v_state.pay_week_start for update;
        if v_plan->>'period_operation'='FUNDING_ZERO' then
          update private.bpay_next_case_period set rule_id=(v_plan_period->>'rule_id')::uuid,
            opening_outstanding_source_ex_vat=(v_plan_period->>'opening_outstanding_source_ex_vat')::numeric,
            opening_due_source_ex_vat=(v_plan_period->>'opening_due_source_ex_vat')::numeric,period_revision=period_revision+1
            where case_component_id=v_component.id and pay_week_start=v_state.pay_week_start
              and period_revision=(v_plan_period->>'period_revision')::bigint-1
              and (opening_outstanding_source_ex_vat,opening_due_source_ex_vat,realised_recovery_source_ex_vat,
                active_unrealised_recovery_source_ex_vat,active_payout_source_ex_vat)
                is not distinct from (0::numeric,0::numeric,0::numeric,0::numeric,0::numeric) returning * into v_period;
          if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';end if;
        end if;
        if to_jsonb(v_period) is distinct from v_plan_period then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_PLAN_PERIOD_CHANGED';end if;
        update private.bpay_next_case_component set resolution_state='RESOLVED',target_pay_channel='PAYE',
          target_ex_vat=(v_plan->'resolution'->>'target_ex_vat')::numeric,target_vat=(v_plan->'resolution'->>'target_vat')::numeric,
          target_inc_vat=(v_plan->'resolution'->>'target_inc_vat')::numeric,
          valuation_policy_id=(v_plan->'resolution'->>'valuation_policy_id')::uuid,
          valuation_window_id=(v_plan->'resolution'->>'valuation_window_id')::uuid,
          resolution_ref=v_plan->'resolution'->>'resolution_ref',component_revision=component_revision+1,
          updated_at_utc=pg_catalog.transaction_timestamp() where id=v_component.id returning * into v_component;
        v_plan:=jsonb_set(v_plan,'{instruction}',(v_plan->'instruction')||jsonb_build_object(
          'run_worker_id',v_worker.id,'run_id',v_run_id,'candidate_id',v_candidate,
          'selection_revision',v_state.selection_revision,'preparation_revision',v_worker.preparation_revision,
          'captured_case_revision',v_case.case_revision,'captured_component_revision',v_component.component_revision,
          'captured_period_revision',v_period.period_revision));
        insert into private.bpay_next_run_case_instruction
          (run_worker_id,run_id,candidate_id,selection_revision,preparation_revision,case_component_id,case_id,
           captured_case_revision,captured_component_revision,captured_period_revision,rule_id,rule_revision,
           case_kind,case_subtype,tax_treatment,instruction_kind,direction,payroll_stage,hold_purpose,
           age_key,debt_age_at_utc,component_ordinal,pay_week_start,source_pay_channel,target_pay_channel,allocation_channel,currency,
           nominal_source_ex_vat,nominal_target_ex_vat,nominal_target_vat,nominal_target_inc_vat,
           capacity_source_ex_vat,capacity_target_ex_vat,capacity_target_vat,capacity_target_inc_vat,
           minimum_earnings_threshold,take_home_floor_override,captured_default_floor,captured_default_floor_revision,
           resolved_take_home_floor,valuation_policy_id,valuation_window_id,resolution_ref,explanation,work_collection_id)
          select planned.run_worker_id,planned.run_id,planned.candidate_id,planned.selection_revision,planned.preparation_revision,planned.case_component_id,planned.case_id,planned.captured_case_revision,planned.captured_component_revision,planned.captured_period_revision,planned.rule_id,planned.rule_revision,planned.case_kind,planned.case_subtype,planned.tax_treatment,planned.instruction_kind,planned.direction,planned.payroll_stage,planned.hold_purpose,planned.age_key,planned.debt_age_at_utc,planned.component_ordinal,planned.pay_week_start,planned.source_pay_channel,planned.target_pay_channel,planned.allocation_channel,planned.currency,planned.nominal_source_ex_vat,planned.nominal_target_ex_vat,planned.nominal_target_vat,planned.nominal_target_inc_vat,planned.capacity_source_ex_vat,planned.capacity_target_ex_vat,planned.capacity_target_vat,planned.capacity_target_inc_vat,planned.minimum_earnings_threshold,planned.take_home_floor_override,planned.captured_default_floor,planned.captured_default_floor_revision,planned.resolved_take_home_floor,planned.valuation_policy_id,planned.valuation_window_id,planned.resolution_ref,planned.explanation,planned.work_collection_id from jsonb_populate_record(null::private.bpay_next_run_case_instruction,v_plan->'instruction') planned
          returning id into v_instruction_id;
        perform private.bpay_next_capture_instruction_destination_v1(v_instruction_id);
        v_state.instruction_capture_cursor:=v_item.selection_no;
        v_state.captured_instruction_count:=v_state.captured_instruction_count+1;
        if v_component.payroll_stage='NET_ADD' then v_state.payout_instruction_count:=v_state.payout_instruction_count+1;end if;
        update private.bpay_next_run_worker set case_instruction_count=case_instruction_count+1 where id=v_worker.id;
      end loop;
      select exists(select 1 from private.bpay_next_case_selection_item i where i.run_id=v_run_id
        and i.candidate_id=v_candidate and i.selection_revision=v_state.selection_revision and i.is_selected
        and i.selection_no>v_state.instruction_capture_cursor) into v_has_more;
      if not v_has_more and v_state.captured_instruction_count<>v_state.expected_instruction_count then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREPARE_CAPTURE_COUNT_MISMATCH';
      end if;
      update private.bpay_next_case_allocation_state set instruction_capture_cursor=v_state.instruction_capture_cursor,
        captured_instruction_count=v_state.captured_instruction_count,payout_instruction_count=v_state.payout_instruction_count,
        prepare_stage=case when v_has_more then 'INSTRUCTION_CAPTURE' else 'ALLOCATE' end where id=v_state.id;
      exit prepare_page;
    end if;

    if v_state.weekly_binding_state='NOT_AFFECTED' then
      -- Only frozen instructions are authoritative from this point. This
      -- scalar metadata proof is scoped to this exact run worker/revisions;
      -- it never reconstructs money or reads prior payment history.
      select count(*)=v_state.expected_instruction_count and count(*)>0
        and coalesce(bool_and((i.case_kind,i.instruction_kind,i.payroll_stage,i.hold_purpose)
          is not distinct from ('OVERPAYMENT','RECOVERY','GROSS_DEDUCT','GROSS_RECOVERY')
          and si.case_id is not null),false)
        into v_week_not_affected
        from private.bpay_next_run_case_instruction i
        left join private.bpay_next_case_selection_item si
          on si.run_id=i.run_id and si.candidate_id=i.candidate_id and si.selection_revision=i.selection_revision
            and si.case_id=i.case_id and si.case_component_id=i.case_component_id and si.is_selected
        where i.run_worker_id=v_worker.id and i.run_id=v_run_id and i.candidate_id=v_candidate
          and i.preparation_revision=v_worker.preparation_revision and i.selection_revision=v_state.selection_revision;
      if not v_week_not_affected
        or v_state.captured_instruction_count<>v_state.expected_instruction_count
        or v_worker.case_instruction_count<>v_state.expected_instruction_count
        or not exists(select 1 from private.bpay_next_case_selection cs where cs.run_id=v_run_id
          and cs.candidate_id=v_candidate and cs.selection_revision=v_state.selection_revision
          and cs.status='SEALED' and cs.selected_count=v_state.expected_instruction_count) then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREPARE_ALLOCATION_NOT_BOUND';
      end if;
    end if;
    if v_state.prepare_stage<>'ALLOCATE' or v_state.weekly_binding_state not in ('BOUND','NOT_AFFECTED') then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREPARE_ALLOCATION_NOT_BOUND';
    end if;
    select c.* into strict v_channel from private.bpay_next_case_allocation_channel c
      where c.state_id=v_state.id and c.allocation_channel='PAYE' for update;
    v_e:=v_channel.remaining_headroom;v_h:=v_state.remaining_worker_take_home;
    select coalesce(jsonb_agg(page.value),'[]'::jsonb) into v_plan_instructions from (
      select to_jsonb(i) as value from private.bpay_next_run_case_instruction i
      where i.run_worker_id=v_worker.id and i.preparation_revision=v_worker.preparation_revision
        and i.selection_revision=v_state.selection_revision
        and (v_state.cursor_case_component_id is null or (i.age_key,i.case_id,i.component_ordinal,i.case_component_id)
          >(v_state.cursor_age_key,v_state.cursor_case_id,v_state.cursor_component_ordinal,v_state.cursor_case_component_id))
      order by i.age_key,i.case_id,i.component_ordinal,i.case_component_id limit p_limit
    ) page;
    v_plan:=private.bpay_next_case_allocation_plan_v1(jsonb_build_object('mode','ALLOCATE',
      'instructions',v_plan_instructions,'channel_headroom',v_e,'worker_take_home',v_h));
    for v_plan_row in select value from jsonb_array_elements(v_plan->'rows') loop
      v_instruction:=jsonb_populate_record(null::private.bpay_next_run_case_instruction,v_plan_row->'instruction');
      v_seen:=v_seen+1;
      v_taken:=(v_plan_row->>'taken_amount')::numeric;v_cap_reason:=v_plan_row->>'cap_reason';
      v_affordability_reason:=v_plan_row->>'affordability_reason';v_capacity_reason:=v_plan_row->>'capacity_reason';
      v_next_e:=(v_plan_row->>'channel_headroom_after')::numeric;v_next_h:=(v_plan_row->>'worker_take_home_after')::numeric;
      insert into private.bpay_next_case_allocation_result
        (state_id,instruction_id,run_worker_id,candidate_id,preparation_revision,selection_revision,result_no,
         pass_kind,allocation_channel,hold_purpose,nominal_target_ex_vat,usable_capacity_target_ex_vat,
         allocated_source_ex_vat,allocated_target_ex_vat,allocated_target_vat,allocated_target_inc_vat,shortfall_target_ex_vat,
         cap_reason,affordability_reason,capacity_reason,channel_headroom_before,channel_headroom_after,worker_take_home_before,worker_take_home_after)
        values(v_state.id,v_instruction.id,v_worker.id,v_candidate,v_worker.preparation_revision,v_state.selection_revision,
         v_state.processed_instruction_count+1,'DRAFT','PAYE',v_instruction.hold_purpose,v_instruction.nominal_target_ex_vat,
         v_instruction.capacity_target_ex_vat,v_taken,v_taken,0,v_taken,v_instruction.nominal_target_ex_vat-v_taken,
         v_cap_reason,v_affordability_reason,v_capacity_reason,v_e,v_next_e,v_h,v_next_h)
        returning id into v_result_id;
      if v_taken>0 then
        select c.* into strict v_case from private.bpay_next_finance_case c where c.id=v_instruction.case_id for update;
        select c.* into strict v_component from private.bpay_next_case_component c where c.id=v_instruction.case_component_id for update;
        select p.* into strict v_period from private.bpay_next_case_period p
          where p.case_component_id=v_component.id and p.pay_week_start=v_instruction.pay_week_start for update;
        insert into private.bpay_next_case_hold
          (run_worker_id,candidate_id,case_id,amount,status,instruction_id,allocation_result_id,allocation_pass_kind,
           case_component_id,purpose,pay_week_start,source_reserved_ex_vat,target_amount_ex_vat,target_amount_vat,target_amount_inc_vat)
          values(v_worker.id,v_candidate,v_case.id,v_taken,'ACTIVE',v_instruction.id,v_result_id,'DRAFT',
           v_component.id,v_instruction.hold_purpose,v_instruction.pay_week_start,v_taken,v_taken,0,v_taken) returning id into v_hold_id;
        insert into private.bpay_next_case_capacity_use(case_hold_id,case_component_id,case_id,candidate_id,
          pay_week_start,purpose,status,source_amount_ex_vat)
          values(v_hold_id,v_component.id,v_case.id,v_candidate,v_instruction.pay_week_start,v_instruction.hold_purpose,'ACTIVE',v_taken);
        if v_instruction.hold_purpose='PAYOUT' then
          update private.bpay_next_case_component set active_payout_source_ex_vat=active_payout_source_ex_vat+v_taken,
            component_revision=component_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_component.id;
          update private.bpay_next_finance_case set active_payout_hold_amount=active_payout_hold_amount+v_taken,
            active_hold_amount=active_hold_amount+v_taken,case_revision=case_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_case.id;
          update private.bpay_next_case_period set active_payout_source_ex_vat=active_payout_source_ex_vat+v_taken,
            period_revision=period_revision+1 where case_component_id=v_component.id and pay_week_start=v_instruction.pay_week_start;
        else
          update private.bpay_next_case_component set active_recovery_source_ex_vat=active_recovery_source_ex_vat+v_taken,
            component_revision=component_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_component.id;
          update private.bpay_next_finance_case set active_recovery_hold_amount=active_recovery_hold_amount+v_taken,
            active_hold_amount=active_hold_amount+v_taken,case_revision=case_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_case.id;
          update private.bpay_next_case_period set active_unrealised_recovery_source_ex_vat=active_unrealised_recovery_source_ex_vat+v_taken,
            period_revision=period_revision+1 where case_component_id=v_component.id and pay_week_start=v_instruction.pay_week_start;
          v_state.recovered_source_total:=v_state.recovered_source_total+v_taken;
          v_state.recovered_target_total:=v_state.recovered_target_total+v_taken;
        end if;
        update private.bpay_next_run_worker set active_case_hold_count=active_case_hold_count+1 where id=v_worker.id;
      end if;
      if v_instruction.payroll_stage='GROSS_ADD' then v_state.allocated_gross_additions:=v_state.allocated_gross_additions+v_taken;
      elsif v_instruction.payroll_stage='GROSS_DEDUCT' then v_state.allocated_gross_deductions:=v_state.allocated_gross_deductions+v_taken;
      elsif v_instruction.payroll_stage='NET_ADD' then v_state.allocated_net_additions:=v_state.allocated_net_additions+v_taken;end if;
      v_e:=v_next_e;v_h:=v_next_h;
      v_state.processed_instruction_count:=v_state.processed_instruction_count+1;
      v_state.cursor_age_key:=v_instruction.age_key;v_state.cursor_case_id:=v_instruction.case_id;
      v_state.cursor_component_ordinal:=v_instruction.component_ordinal;v_state.cursor_case_component_id:=v_instruction.case_component_id;
      update private.bpay_next_run_worker set case_allocated_count=case_allocated_count+1 where id=v_worker.id;
    end loop;
    update private.bpay_next_case_allocation_channel set remaining_headroom=v_e where state_id=v_state.id and allocation_channel='PAYE';
    update private.bpay_next_case_allocation_state set processed_instruction_count=v_state.processed_instruction_count,
      remaining_worker_take_home=v_h,recovered_source_total=v_state.recovered_source_total,recovered_target_total=v_state.recovered_target_total,
      cursor_age_key=v_state.cursor_age_key,cursor_case_id=v_state.cursor_case_id,cursor_component_ordinal=v_state.cursor_component_ordinal,
      cursor_case_component_id=v_state.cursor_case_component_id,allocated_gross_additions=v_state.allocated_gross_additions,
      allocated_gross_deductions=v_state.allocated_gross_deductions,allocated_net_additions=v_state.allocated_net_additions where id=v_state.id;
    select exists(select 1 from private.bpay_next_run_case_instruction i where i.run_worker_id=v_worker.id
      and i.preparation_revision=v_worker.preparation_revision and i.selection_revision=v_state.selection_revision
      and (v_state.cursor_case_component_id is null or (i.age_key,i.case_id,i.component_ordinal,i.case_component_id)
        >(v_state.cursor_age_key,v_state.cursor_case_id,v_state.cursor_component_ordinal,v_state.cursor_case_component_id))) into v_has_more;
    if not v_has_more then
      if v_state.processed_instruction_count<>v_state.expected_instruction_count then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREPARE_ALLOCATION_COUNT_MISMATCH';
      end if;
      v_gross:=v_state.captured_work_gross+v_state.allocated_gross_additions-v_state.allocated_gross_deductions;
      if v_gross<0 or v_gross>=10000000000000000 or v_state.allocated_net_additions>=10000000000000000 then
        v_issue:='CASE_INPUT_UNBOUND';exit prepare_page;
      end if;
      -- Preserve certified-zero WORK/Source rows; case-only NET additions are
      -- real payable cash, not a made-up positive payroll gross or net.
      if v_worker.captured_line_count=0 and v_gross=0 and v_state.allocated_net_additions=0 then
        v_issue:='CASE_NO_PAYABLE_AMOUNT';exit prepare_page;
      end if;
      if v_worker.captured_line_count=0 and v_gross=0 and v_state.payout_instruction_count>0 then
        insert into private.bpay_next_case_payout_basis(run_worker_id,candidate_id,preparation_revision,currency,
          beneficiary_kind,beneficiary_id,expected_instruction_count,captured_instruction_count,source_total_ex_vat,
          target_total_ex_vat,target_total_vat,target_total_inc_vat,status)
          values(v_worker.id,v_candidate,v_worker.preparation_revision,'GBP','CANDIDATE',v_candidate,
            v_state.payout_instruction_count,v_state.payout_instruction_count,v_state.allocated_net_additions,
            v_state.allocated_net_additions,0,v_state.allocated_net_additions,'SEALED');
      end if;
      select w.* into strict v_week from private.bpay_next_worker_week w
        where w.candidate_id=v_candidate and w.pay_week_start=v_state.pay_week_start for update;
      insert into private.bpay_next_worker_week_contribution(candidate_id,pay_week_start,original_run_worker_id,
        original_pay_date,original_created_at_utc,contribution_revision,basis_kind,original_gross_amount,
        payment_state,eligibility_state,eligible_arranged_amount,primary_binding_revision)
        values(v_candidate,v_state.pay_week_start,v_worker.id,v_run.pay_date,v_run.created_at_utc,1,'GROSS_FALLBACK',
          v_gross,'ARRANGED',case when v_gross>0 then 'ELIGIBLE' else 'EXCLUDED' end,v_gross,1);
      update private.bpay_next_worker_week set resolved_arranged_take_home=resolved_arranged_take_home+v_gross,
        period_revision=period_revision+1 where candidate_id=v_candidate and pay_week_start=v_state.pay_week_start;
      update private.bpay_next_case_allocation_state set status='COMPLETE',prepare_stage='COMPLETE',
        completed_at_utc=pg_catalog.transaction_timestamp() where id=v_state.id;
      update private.bpay_next_run_worker set gross_ex_vat=v_gross,gross_vat=0,gross_inc_vat=v_gross,
        status='READY',case_pending_binding_count=0 where id=v_worker.id;

      v_terminal:=true;
    end if;
  end prepare_page;

  if v_issue is not null then
    -- Candidate-wide issue: WORK FK remains NULL. Earlier frozen WORK and any
    -- already-created ACTIVE reservations remain exact and non-confirmable.
    update private.bpay_next_run_worker set status='REVIEW',review_issue_code=v_issue,review_issue_work_id=null,
      financial_resolution_count=financial_resolution_count+1,case_pending_binding_count=
        case when v_issue in ('CASE_SELECTION_REQUIRED','CASE_INPUT_UNBOUND','CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND') then 1 else 0 end
      where id=v_worker.id;
    if v_state.id is not null then
      update private.bpay_next_case_allocation_state set prepare_stage='REVIEW',weekly_binding_state='UNBOUND',
        instruction_capture_cursor=v_state.instruction_capture_cursor,captured_instruction_count=v_state.captured_instruction_count,
        payout_instruction_count=v_state.payout_instruction_count where id=v_state.id;
    end if;

    v_terminal:=true;
  elsif not v_terminal then
    update private.bpay_next_job set cursor_key=(v_saved_cursor+1)::text where id=p_job_id;
  end if;
  update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1,
    updated_at_utc=pg_catalog.transaction_timestamp() where candidate_id=v_candidate;
  if v_terminal then
    return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,v_seen);
  end if;
  return pg_catalog.jsonb_build_object('done',v_terminal,'phase',case when v_issue is not null then 'REVIEW'
    when v_terminal then 'READY' else 'FINANCE_ALLOCATE' end,'cursor',case when v_terminal then null else (v_saved_cursor+1)::text end,
    'rows_visited',v_seen::text,'replay',false,'run_worker_id',v_worker.id,
    'worker_status',case when v_issue is not null then 'REVIEW' when v_terminal then 'READY' else 'PREPARING' end)
    ||case when v_issue is not null then pg_catalog.jsonb_build_object('issue_code',v_issue) else '{}'::jsonb end;
end
$function$;

create or replace function public.bpay_next_financial_job_v1(
  p_action text,p_job_id uuid,p_job_kind text,p_args jsonb
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_job private.bpay_next_job%rowtype;
  v_keys text[];
  v_key text;
  v_paged boolean;
  v_integer_cursor boolean;
  v_phase text;
  v_lease_seconds integer;
  v_nonce uuid;
  v_owner_epoch bigint;
  v_cursor bigint;
  v_text_cursor text;
  v_work_cursor uuid;
  v_component_cursor text;
  v_limit integer;
  v_claim record;
  v_claim_state record;
  v_page record;
  v_worker private.bpay_next_run_worker%rowtype;
  v_result jsonb;
  v_output jsonb;
  v_lease_until text;
  v_claim_failure text;
  v_case_net boolean:=false;
  v_expired_prepare boolean:=false;
  v_defer_pin_module boolean:=false;
begin
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
      nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception using errcode='42501',message='BPAY_NEXT_JOB_FORBIDDEN';
  end if;
  if p_action is null or p_action not in ('CLAIM','STEP') or p_job_id is null
     or p_job_kind is null or p_job_kind not in
       ('PAYE_NET_ENTRY','TRANSFER_BUILD','CSV_SETTLEMENT','CSV_RETURN','CASH_REISSUE','SIMPLE_CANCEL','POSITION_APPLY','PREPARE','REISSUE_CANCEL','INTERNAL_SETTLEMENT','PREPARATION_EXPIRY','CASE_CREATE','CASE_WRITE_OFF')
     or p_args is null or pg_catalog.jsonb_typeof(p_args) is distinct from 'object'
     or pg_catalog.octet_length(p_args::text)>2048 then
    raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
  end if;
  v_phase:=p_args->>'expected_phase';
  if p_action='STEP' and (p_job_kind in ('POSITION_APPLY','PREPARE')
      or (p_job_kind='PAYE_NET_ENTRY' and p_args ? 'expected_phase'))
     and (pg_catalog.jsonb_typeof(p_args->'expected_phase') is distinct from 'string'
       or not(v_phase=any(case p_job_kind when 'POSITION_APPLY' then array['NEW','REMOVED']
         when 'PAYE_NET_ENTRY' then array['FINANCE_ALLOCATE']
         else array['NEW','PINNING','RESERVE','ALLOCATE','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE'] end))) then
    raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
  end if;
  v_paged:=p_job_kind in ('TRANSFER_BUILD','CSV_SETTLEMENT','INTERNAL_SETTLEMENT','SIMPLE_CANCEL','POSITION_APPLY','PREPARATION_EXPIRY')
    or (p_job_kind='PAYE_NET_ENTRY' and v_phase='FINANCE_ALLOCATE')
    or (p_job_kind='PREPARE' and v_phase in ('RESERVE','ALLOCATE','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE'));
  v_integer_cursor:=p_job_kind in ('TRANSFER_BUILD','CSV_SETTLEMENT','INTERNAL_SETTLEMENT','SIMPLE_CANCEL','PREPARATION_EXPIRY')
    or (p_job_kind='PAYE_NET_ENTRY' and v_phase='FINANCE_ALLOCATE')
    or (p_job_kind='PREPARE' and v_phase in ('NEW','PINNING','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE'));
  v_keys:=case when p_action='CLAIM' then array['lease_seconds']
    when p_job_kind='POSITION_APPLY' then array['lease_nonce','owner_epoch','expected_phase','expected_cursor','limit']
    when p_job_kind='PAYE_NET_ENTRY' and v_phase='FINANCE_ALLOCATE' then array['lease_nonce','owner_epoch','expected_phase','expected_cursor','limit']
    when p_job_kind='PREPARE' and v_phase in ('NEW','PINNING') then array['lease_nonce','owner_epoch','expected_phase','expected_cursor']
    when p_job_kind='PREPARE' and v_phase in ('RESERVE','ALLOCATE') then array['lease_nonce','owner_epoch','expected_phase','expected_work_cursor','expected_component_cursor','limit']
    when p_job_kind='PREPARE' then array['lease_nonce','owner_epoch','expected_phase','expected_cursor','limit']
    when v_paged then array['lease_nonce','owner_epoch','expected_cursor','limit']
    else array['lease_nonce','owner_epoch'] end;
  if (select count(*) from pg_catalog.jsonb_object_keys(p_args))<>pg_catalog.array_length(v_keys,1)
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_args) k where not(k=any(v_keys))) then
    raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
  end if;
  if p_action='CLAIM' then
    if pg_catalog.jsonb_typeof(p_args->'lease_seconds') is distinct from 'number'
       or (p_args->>'lease_seconds')!~'^[1-9][0-9]{0,2}$' then
      raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
    end if;
    v_lease_seconds:=(p_args->>'lease_seconds')::integer;
    if v_lease_seconds not between 1 and 120 then
      raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
    end if;
  else
    if pg_catalog.jsonb_typeof(p_args->'lease_nonce') is distinct from 'string'
       or (p_args->>'lease_nonce')!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
      raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
    end if;
    foreach v_key in array array['owner_epoch','expected_cursor'] loop
      if v_key='expected_cursor' and (not coalesce(v_integer_cursor,false) or p_args->v_key='null'::jsonb) then
        continue;
      end if;
      if pg_catalog.jsonb_typeof(p_args->v_key) is distinct from 'string'
         or (p_args->>v_key)!~'^[1-9][0-9]{0,18}$' then
        raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
      end if;
      if (p_args->>v_key)::numeric>9223372036854775807 then
        raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
      end if;
    end loop;
    v_nonce:=(p_args->>'lease_nonce')::uuid;
    v_owner_epoch:=(p_args->>'owner_epoch')::bigint;
    if v_integer_cursor then v_cursor:=(p_args->>'expected_cursor')::bigint; end if;
    if p_job_kind='POSITION_APPLY' then
      if p_args->'expected_cursor'<>'null'::jsonb
         and (pg_catalog.jsonb_typeof(p_args->'expected_cursor') is distinct from 'string'
              or pg_catalog.char_length(p_args->>'expected_cursor') not between 1 and 256) then
        raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
      end if;
      v_text_cursor:=p_args->>'expected_cursor';
    elsif p_job_kind='PREPARE' and v_phase in ('RESERVE','ALLOCATE') then
      -- A cursor is the complete (UUID work, opaque retained component TEXT)
      -- pair, or both NULL. Component bounds are CHARACTERS, not bytes.
      if (p_args->'expected_work_cursor'='null'::jsonb) is distinct from
          (p_args->'expected_component_cursor'='null'::jsonb)
         or (p_args->'expected_work_cursor'<>'null'::jsonb
             and (pg_catalog.jsonb_typeof(p_args->'expected_work_cursor') is distinct from 'string'
                  or (p_args->>'expected_work_cursor')!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'))
         or (p_args->'expected_component_cursor'<>'null'::jsonb
             and (pg_catalog.jsonb_typeof(p_args->'expected_component_cursor') is distinct from 'string'
                  or pg_catalog.char_length(p_args->>'expected_component_cursor') not between 1 and 256)) then
        raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
      end if;
      v_work_cursor:=(p_args->>'expected_work_cursor')::uuid;
      v_component_cursor:=p_args->>'expected_component_cursor';
    end if;
    if v_paged then
      if pg_catalog.jsonb_typeof(p_args->'limit') is distinct from 'number'
         or (p_args->>'limit')!~'^[1-9][0-9]{0,2}$' then
        raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
      end if;
      v_limit:=(p_args->>'limit')::integer;
      if v_limit not between 1 and 100 then
        raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
      end if;
    end if;
  end if;

  -- Immutable dispatch identity, without an outer job lock. Owners acquire
  -- Candidate -> job for CLAIM, parent run -> Candidate -> job for STEP.
  -- Taking job FOR UPDATE here would invert that established order.
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id;
  if v_job.job_kind<>p_job_kind then
    raise exception using errcode='23514',message='BPAY_NEXT_JOB_KIND_MISMATCH';
  end if;
  -- Query admission is outside even the public module SHARE. Plain dispatch
  -- discovery is not lease/module authority; every owner rechecks under locks.
  -- Definite terminal/advanced/cursor/stale replies must not newly contend on
  -- the query key. Their independent owner locks/validates the module before
  -- any write, and this boundary reasserts it immediately after that call.
  if p_action='STEP' and p_job_kind='PREPARE' and v_phase in ('NEW','PINNING') then
    if v_job.status='LEASED' and v_job.phase in ('NEW','PINNING')
       and v_job.lease_nonce is not distinct from v_nonce
       and v_job.owner_epoch is not distinct from v_owner_epoch
       and v_job.lease_until_utc>pg_catalog.clock_timestamp()
       and v_job.cursor_key::bigint is not distinct from v_cursor then
      perform private.weekly_source_pay_query_admit_v2();
    else
      v_defer_pin_module:=true;
    end if;
  end if;
  if not v_defer_pin_module and not exists(
      select 1 from private.bpay_next_module_control m
      where m.id=1 and m.active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  if p_job_kind='PAYE_NET_ENTRY' then
    select r.case_draft_state_id is not null into strict v_case_net
      from private.bpay_next_paye_net_request r where r.command_id=v_job.command_id;
    if p_action='STEP' and v_case_net is distinct from (p_args ? 'expected_phase') then
      raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
    end if;
  end if;
  if p_action='CLAIM' then
    if (p_job_kind='POSITION_APPLY' and not(v_job.phase=any(case
          when v_job.status='DONE' then array['DONE'] else array['NEW','REMOVED'] end)))
       or (p_job_kind='PREPARE' and not(v_job.phase=any(case when v_job.status='DONE' then array['READY','DONE','EXPIRED']
          else array['NEW','PINNING','RESERVE','ALLOCATE','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE'] end)))
       or (p_job_kind='PREPARATION_EXPIRY' and not(v_job.phase=any(case
          when v_job.status='DONE' then array['EXPIRED'] else array['NEW','RELEASE'] end)))
       or (p_job_kind in ('CASE_CREATE','CASE_WRITE_OFF') and (v_job.phase is distinct from
          case when v_job.status='DONE' then 'DONE' else 'NEW' end or v_job.cursor_key is not null)) then
      raise exception using errcode='55000',message='BPAY_NEXT_JOB_PHASE_UNSUPPORTED';
    end if;
    -- Distinguish terminal and busy claims; never return the stored nonce.
    -- These read-only early checks are not ownership: owners recheck/lock.
    if v_job.status='DONE' then
      raise exception using errcode='55000',message='BPAY_NEXT_JOB_TERMINAL';
    end if;
    if v_job.status='LEASED' and v_job.lease_until_utc>pg_catalog.clock_timestamp() then
      raise exception using errcode='55000',message='BPAY_NEXT_JOB_LEASE_BUSY';
    end if;
    begin
      case p_job_kind
        when 'POSITION_APPLY' then
          select c.* into strict v_claim from private.bpay_next_claim_position_job_v1(p_job_id,v_lease_seconds) c;
        when 'PREPARE' then
          select c.* into strict v_claim from private.bpay_next_claim_prepare_job_v1(p_job_id,v_lease_seconds) c;
        when 'PREPARATION_EXPIRY' then
          select c.* into strict v_claim from private.bpay_next_claim_preparation_expiry_job_v1(p_job_id,v_lease_seconds) c;
        when 'CASE_CREATE' then
          select c.* into strict v_claim from private.bpay_next_claim_case_create_job_v1(p_job_id,v_lease_seconds) c;
        when 'CASE_WRITE_OFF' then
          select c.* into strict v_claim from private.bpay_next_claim_write_off_job_v1(p_job_id,v_lease_seconds) c;
        when 'PAYE_NET_ENTRY' then
          select c.* into strict v_claim from private.bpay_next_claim_simple_paye_net_job_v1(p_job_id,v_lease_seconds) c;
        when 'TRANSFER_BUILD' then
          select c.* into strict v_claim from private.bpay_next_claim_simple_transfer_job_v1(p_job_id,v_lease_seconds) c;
        when 'SIMPLE_CANCEL' then
          select c.* into strict v_claim from private.bpay_next_claim_simple_cancel_job_v1(p_job_id,v_lease_seconds) c;
        when 'REISSUE_CANCEL' then
          select c.* into strict v_claim from private.bpay_next_claim_reissue_cancel_job_v1(p_job_id,v_lease_seconds) c;
        else
          select c.* into strict v_claim from private.bpay_next_claim_simple_outcome_job_v1(p_job_id,v_lease_seconds) c;
      end case;
    exception when sqlstate '55000' then
      get stacked diagnostics v_claim_failure=message_text;
      if v_claim_failure not in
          ('BPAY_NEXT_PAYE_NET_JOB_NOT_CLAIMABLE','BPAY_NEXT_TRANSFER_JOB_NOT_CLAIMABLE',
           'BPAY_NEXT_OUTCOME_JOB_NOT_CLAIMABLE','BPAY_NEXT_CANCEL_JOB_NOT_CLAIMABLE',
           'BPAY_NEXT_JOB_NOT_CLAIMABLE','BPAY_NEXT_PREPARE_JOB_NOT_CLAIMABLE','BPAY_NEXT_REISSUE_CANCEL_JOB_NOT_CLAIMABLE','BPAY_NEXT_EXPIRY_JOB_NOT_CLAIMABLE','BPAY_NEXT_CASE_CREATE_JOB_NOT_CLAIMABLE','BPAY_NEXT_WRITE_OFF_JOB_NOT_CLAIMABLE','BPAY_NEXT_EARLIER_WORKER_COMMAND_PENDING') then
        raise;
      end if;
      if v_claim_failure='BPAY_NEXT_EARLIER_WORKER_COMMAND_PENDING'
         and p_job_kind not in ('POSITION_APPLY','PREPARE','REISSUE_CANCEL','PREPARATION_EXPIRY') then raise; end if;
      -- A loser may have read READY, then waited behind the winning owner.
      -- The failed subtransaction released its owner locks. Classify its
      -- now-visible exact job without acquiring job ahead of Candidate/run,
      -- retrying any owner or adopting the winner's stored lease nonce.
      -- Job and admission predicates share one fresh read snapshot. A
      -- predecessor finishing during separate reads must not manufacture a
      -- permanent eligibility failure for an already accepted financial job.
      select j.*,
        exists(select 1 from private.bpay_next_run_command rc
          join private.bpay_next_pay_run r on r.id=rc.run_id
          where rc.command_id=j.command_id and r.status='PREPARING' and r.selection_state='SEALED'
            and not exists(select 1 from private.bpay_next_run_worker w
              where w.run_id=r.id and w.candidate_id=j.candidate_id and w.status<>'PREPARING'
                and not (j.phase in ('WORKBENCH_WORK','WORKBENCH_CASE') and w.status in ('READY','REVIEW')))) as prepare_admissible,
        exists(select 1 from private.bpay_next_cancel_request r
               where r.command_id=j.command_id and r.candidate_id=j.candidate_id
                 and r.status in ('REQUESTED','CANCELLING')) as cancel_request_pending,
        exists(select 1 from private.bpay_next_preparation_expiry_request r
          join private.bpay_next_pay_run run on run.id=r.run_id
          where r.command_id=j.command_id and r.status='CANCELLING'
            and run.status='CANCELLING' and run.confirmed_at_utc is null) as expiry_request_pending,
        exists(select 1 from private.bpay_next_case_create_request r
          where r.command_id=j.command_id and r.candidate_id=j.candidate_id) as case_create_request_present,
        exists(select 1 from private.bpay_next_write_off_request r
          where r.command_id=j.command_id and r.candidate_id=j.candidate_id and r.status='REQUESTED') as write_off_request_pending,
        exists(select 1 from private.bpay_next_job earlier
               where earlier.candidate_id=j.candidate_id
                 and earlier.command_sequence<j.command_sequence
                 and earlier.status<>'DONE') as predecessor_pending
      into strict v_claim_state from private.bpay_next_job j where j.id=p_job_id;
      if v_claim_state.job_kind<>p_job_kind or v_claim_state.module_epoch is distinct from
          (select m.owner_epoch from private.bpay_next_module_control m
           where m.id=1 and m.active_owner='NEXT') then
        raise;
      end if;
      if v_claim_state.status='DONE' then
        raise exception using errcode='55000',message='BPAY_NEXT_JOB_TERMINAL';
      end if;
      if not(v_claim_state.phase=any(case p_job_kind
          when 'PAYE_NET_ENTRY' then array['NEW','FINANCE_ALLOCATE']
          when 'TRANSFER_BUILD' then array['WORK']
          when 'SIMPLE_CANCEL' then array['NEW','RELEASE']
          when 'REISSUE_CANCEL' then array['NEW']
          when 'POSITION_APPLY' then array['NEW','REMOVED']
          when 'PREPARE' then array['NEW','PINNING','RESERVE','ALLOCATE','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE']
          when 'PREPARATION_EXPIRY' then array['NEW','RELEASE']
          when 'CASE_CREATE' then array['NEW']
          when 'CASE_WRITE_OFF' then array['NEW']
          else array['MEMBERS'] end))
         or (p_job_kind='PREPARE' and not v_claim_state.prepare_admissible)
         or (p_job_kind='PREPARATION_EXPIRY' and not v_claim_state.expiry_request_pending)
         or (p_job_kind='CASE_CREATE' and (not v_claim_state.case_create_request_present
              or v_claim_state.cursor_key is not null))
         or (p_job_kind='CASE_WRITE_OFF' and (not v_claim_state.write_off_request_pending
              or v_claim_state.cursor_key is not null))
         or (p_job_kind='SIMPLE_CANCEL'
             and (v_claim_state.available_at_utc>pg_catalog.clock_timestamp()
                  or not v_claim_state.cancel_request_pending)) then
        raise;
      end if;
      if v_claim_state.status='LEASED' and v_claim_state.lease_until_utc>pg_catalog.clock_timestamp() then
        raise exception using errcode='55000',message='BPAY_NEXT_JOB_LEASE_BUSY';
      end if;
      if v_claim_state.status in ('READY','LEASED') and v_claim_state.predecessor_pending then
        raise exception using errcode='55000',message='BPAY_NEXT_JOB_WAIT_FOR_PREDECESSOR';
      end if;
      if v_claim_state.status in ('READY','LEASED') and not v_claim_state.predecessor_pending
         and (v_claim_state.status='READY'
              or v_claim_state.lease_until_utc<=pg_catalog.clock_timestamp()) then
        -- The predecessor just completed or the short lease just expired.
        -- Only signal a fresh CLAIM; never invoke the owner a second time.
        raise exception using errcode='55000',message='BPAY_NEXT_JOB_CLAIM_RETRY';
      end if;
      -- Other permanent scope/phase/request admission failures retain the
      -- exact original owner rejection, not a general automatic retry.
      raise;
    end;
    -- Only the owner-returned newly acquired token crosses this boundary.
    select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id;
    if v_job.status<>'LEASED' or v_job.lease_nonce is distinct from v_claim.lease_nonce
       or v_job.owner_epoch is distinct from v_claim.owner_epoch then
      raise exception using errcode='23514',message='BPAY_NEXT_JOB_CLAIM_RESULT_INVALID';
    end if;
    v_lease_until:=pg_catalog.to_char(v_job.lease_until_utc at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"');
    v_output:=pg_catalog.jsonb_build_object(
      'job_id',p_job_id,'job_kind',p_job_kind,'phase',v_job.phase,
      'lease_nonce',v_claim.lease_nonce,'owner_epoch',v_claim.owner_epoch::text,
      'lease_until_utc',v_lease_until,'cursor',v_job.cursor_key);
    if p_job_kind='PREPARE' then
      v_output:=v_output||pg_catalog.jsonb_build_object(
        'cursor',case when v_job.phase in ('NEW','PINNING','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE') then v_job.cursor_key else null end,
        'work_cursor',case when v_job.phase in ('RESERVE','ALLOCATE') then v_job.position_work_cursor else null end,
        'component_cursor',case when v_job.phase in ('RESERVE','ALLOCATE') then v_job.position_component_cursor else null end);
    end if;
  else
    -- Do not reject cleared DONE leases here. The owners' scoped DONE replay
    -- branches are authoritative and return the immutable retained result.
    case p_job_kind
      when 'POSITION_APPLY' then
        select p.* into strict v_page from private.bpay_next_apply_position_page_v1(
          p_job_id,v_nonce,v_owner_epoch,v_phase,v_text_cursor,v_limit) p;
        v_result:=pg_catalog.jsonb_build_object('phase',v_page.next_phase,
          'cursor',v_page.next_cursor,'rows_visited',v_page.rows_visited::text,'done',v_page.done);
      when 'PREPARE' then
        -- Terminal authority comes from the retained owner, never a fabricated
        -- READY row or an adopted active cursor/nonce. One owner per STEP.
        select private.bpay_next_preparation_expiry_fenced_v1(
          rc.run_id,v_job.command_id) into v_expired_prepare
          from private.bpay_next_run_command rc where rc.command_id=v_job.command_id;
        if coalesce(v_expired_prepare,false) then
          if v_job.status<>'DONE' then
            raise exception using errcode='55000',message='BPAY_NEXT_PREPARATION_EXPIRING';
          end if;
          -- May have been retired before its first CLAIM: no worker is invented.
          -- Only the exact expiry owner validates and returns this old receipt.
          v_result:=private.bpay_next_expired_prepare_receipt_v1(p_job_id);
        elsif v_job.status='DONE' and (exists(select 1 from private.bpay_next_case_allocation_state s
          where s.job_id=p_job_id and s.pass_kind='DRAFT')
          or exists(select 1 from private.bpay_next_run_command rc
            join private.bpay_next_run_worker w on w.run_id=rc.run_id and w.candidate_id=v_job.candidate_id
            where rc.command_id=v_job.command_id and w.status='REVIEW'
              and w.review_issue_code in ('CASE_SELECTION_REQUIRED','CASE_TARGET_UNSUPPORTED','CASE_INPUT_UNBOUND',
                'CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND','CASE_NO_PAYABLE_AMOUNT'))) then
          -- An original NEW/PINNING request has no page limit. Terminal
          -- read-back does no work, but the owner still requires a valid bound.
          v_result:=private.bpay_next_prepare_case_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,coalesce(v_limit,1));
        elsif v_job.status='DONE' and v_job.phase='READY' then
          v_result:=private.bpay_next_complete_no_case_worker_v1(p_job_id,v_nonce,v_owner_epoch);
        elsif v_job.status='DONE' and v_job.phase='DONE' then
          v_result:=private.bpay_next_pin_selected_work_v1(p_job_id,v_nonce,v_owner_epoch,null);
          v_result:=v_result||pg_catalog.jsonb_build_object('rows_visited','0');
        elsif v_phase in ('WORKBENCH_WORK','WORKBENCH_CASE') then
          v_result:=private.bpay_next_prepare_projection_page_v1(p_job_id,v_nonce,v_owner_epoch,v_phase,v_cursor,v_limit);
        elsif v_phase in ('NEW','PINNING') then
          v_result:=private.bpay_next_pin_selected_work_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor);
          -- This owner examines at most one selected work. It exposes no page
          -- count, but its exact work/replay receipt identifies zero vs one.
          v_result:=v_result||pg_catalog.jsonb_build_object('rows_visited',case
            when v_result->>'rows_visited' is not null then v_result->>'rows_visited'
            when v_result->>'replay'='true' then '0'
            when v_result->>'work_id' is not null then '1' else '0' end);
        elsif v_phase='RESERVE' then
          v_result:=private.bpay_next_capture_position_page_v1(p_job_id,v_nonce,v_owner_epoch,v_work_cursor,v_component_cursor,v_limit);
        elsif v_phase='ALLOCATE' then
          v_result:=private.bpay_next_hold_fresh_position_page_v1(p_job_id,v_nonce,v_owner_epoch,v_work_cursor,v_component_cursor,v_limit);
        elsif v_phase='FINANCE_ALLOCATE' then
          v_result:=private.bpay_next_prepare_case_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
        else
          raise exception using errcode='55000',message='BPAY_NEXT_JOB_PHASE_UNSUPPORTED';
        end if;
      when 'PAYE_NET_ENTRY' then
        if v_case_net then
          v_result:=private.bpay_next_apply_case_paye_net_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
        else
          v_result:=private.bpay_next_apply_simple_paye_net_v1(p_job_id,v_nonce,v_owner_epoch);
        end if;
      when 'TRANSFER_BUILD' then
        v_result:=private.bpay_next_build_simple_transfer_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
      when 'CSV_SETTLEMENT' then
        v_result:=private.bpay_next_post_simple_outcome_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
      when 'INTERNAL_SETTLEMENT' then
        v_result:=private.bpay_next_post_simple_outcome_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
      when 'CSV_RETURN' then
        v_result:=private.bpay_next_post_simple_return_v1(p_job_id,v_nonce,v_owner_epoch);
      when 'CASH_REISSUE' then
        v_result:=private.bpay_next_build_simple_reissue_v1(p_job_id,v_nonce,v_owner_epoch);
      when 'SIMPLE_CANCEL' then
        v_result:=private.bpay_next_cancel_simple_worker_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
      when 'REISSUE_CANCEL' then
        v_result:=private.bpay_next_apply_reissue_cancel_v1(p_job_id,v_nonce,v_owner_epoch);
      when 'PREPARATION_EXPIRY' then
        v_result:=private.bpay_next_expire_preparation_worker_page_v1(
          p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
      when 'CASE_CREATE' then
        -- One exact original approval application. The retained 0330 owner
        -- alone governs lease validation and DONE/DONE immutable replay.
        v_result:=private.bpay_next_apply_case_create_v1(p_job_id,v_nonce,v_owner_epoch);
      when 'CASE_WRITE_OFF' then
        v_result:=private.bpay_next_apply_write_off_v1(p_job_id,v_nonce,v_owner_epoch);
    end case;
    if v_defer_pin_module and not exists(
        select 1 from private.bpay_next_module_control m
        where m.id=1 and m.active_owner='NEXT' for share) then
      raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
    end if;
    select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id;
    if v_job.status not in ('LEASED','DONE')
       or (p_job_kind<>'POSITION_APPLY' and pg_catalog.jsonb_typeof(v_result->'replay') is distinct from 'boolean')
       or (p_job_kind='POSITION_APPLY' and ((v_result->>'done')::boolean is distinct from (v_job.status='DONE')
            or v_result->>'phase' is distinct from v_job.phase
            or v_result->>'cursor' is distinct from v_job.cursor_key))
      or (p_job_kind='PREPARE' and v_result->>'phase' is distinct from
            case when v_expired_prepare then 'EXPIRED'
              when v_job.status='DONE' and v_job.phase='DONE' then 'REVIEW' else v_job.phase end)
       or (p_job_kind='PREPARATION_EXPIRY' and (v_result->>'phase' is distinct from v_job.phase
            or v_result->>'cursor' is distinct from v_job.cursor_key))
       or (p_job_kind='CASE_CREATE' and (v_job.status<>'DONE' or v_job.phase<>'DONE'
            or v_job.cursor_key is not null or v_result->>'phase' is distinct from 'CASE_CREATED'
            or not exists(select 1 from private.bpay_next_case_create_request r
              where r.command_id=v_job.command_id and r.candidate_id=v_job.candidate_id
                and r.case_id::text=v_result->>'case_id')))
       or (p_job_kind='CASE_WRITE_OFF' and (v_job.status<>'DONE' or v_job.phase<>'DONE'
            or v_job.cursor_key is not null or not exists(select 1 from private.bpay_next_write_off_request r
              where r.command_id=v_job.command_id and r.candidate_id=v_job.candidate_id
                and r.status in ('WRITTEN_OFF','BLOCKED','REVIEW') and r.status=v_result->>'phase'
                and r.case_id::text=v_result->>'case_id' and r.case_component_id::text=v_result->>'case_component_id'
                and r.applied_amount is not distinct from (v_result->>'applied_amount')::numeric
                and r.remaining_amount is not distinct from (v_result->>'remaining_amount')::numeric
                and r.protected_amount is not distinct from (v_result->>'protected_amount')::numeric
                and r.event_id::text is not distinct from v_result->>'event_id'
                and r.issue_code is not distinct from v_result->>'issue_code')))
       or (v_case_net and (v_result->>'phase' is distinct from
            case when v_job.status='DONE' and v_job.phase='DONE' then 'REVIEW' else v_job.phase end
          or v_result->>'cursor' is distinct from v_job.cursor_key
          or (v_result->>'run_worker_id')::uuid is distinct from
            (select r.run_worker_id from private.bpay_next_paye_net_request r where r.command_id=v_job.command_id))) then
      raise exception using errcode='23514',message='BPAY_NEXT_JOB_STEP_RESULT_INVALID';
    end if;
    -- Yield only this successfully validated page's own lease. The next ID
    -- notification takes a fresh CLAIM from the committed native checkpoint.
    if v_job.status='LEASED' then
      if v_job.lease_nonce is distinct from v_nonce or v_job.owner_epoch is distinct from v_owner_epoch then
        raise exception using errcode='23514',message='BPAY_NEXT_JOB_STEP_RESULT_INVALID';
      end if;
      update private.bpay_next_job set status='READY',lease_nonce=null,lease_until_utc=null
        where id=p_job_id and status='LEASED' and lease_nonce=v_nonce and owner_epoch=v_owner_epoch
        returning * into strict v_job;
    end if;
    v_lease_until:=pg_catalog.to_char(v_job.lease_until_utc at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"');
    v_output:=pg_catalog.jsonb_build_object(
      'job_id',p_job_id,'job_kind',p_job_kind,'owner_epoch',v_job.owner_epoch::text,
      'lease_until_utc',v_lease_until,'done',v_job.status='DONE');
    if p_job_kind<>'POSITION_APPLY' then
      v_output:=v_output||pg_catalog.jsonb_build_object('replay',v_result->'replay');
    end if;
    case p_job_kind
      when 'POSITION_APPLY' then
        v_output:=v_output||pg_catalog.jsonb_build_object('phase',v_result->>'phase',
          'cursor',v_result->>'cursor','rows_visited',v_result->>'rows_visited');
      when 'PREPARE' then
        if coalesce(v_expired_prepare,false) then
          v_output:=v_output||pg_catalog.jsonb_build_object('phase','EXPIRED',
            'cursor',v_result->>'cursor','work_cursor',v_result->>'work_cursor',
            'component_cursor',v_result->>'component_cursor',
            'run_worker_id',v_result->>'run_worker_id','worker_status',v_result->>'worker_status',
            'issue_code',v_result->>'issue_code','issue_work_id',v_result->>'issue_work_id',
            'rows_visited',v_result->>'rows_visited');
        else
        select w.* into strict v_worker from private.bpay_next_run_command rc
          join private.bpay_next_run_worker w on w.run_id=rc.run_id and w.candidate_id=v_job.candidate_id
          where rc.command_id=v_job.command_id;
        v_output:=v_output||pg_catalog.jsonb_build_object('phase',v_result->>'phase',
          'cursor',case when v_job.phase in ('NEW','PINNING','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE') then v_job.cursor_key else null end,
          'work_cursor',case when v_job.phase in ('RESERVE','ALLOCATE') then v_job.position_work_cursor else null end,
          'component_cursor',case when v_job.phase in ('RESERVE','ALLOCATE') then v_job.position_component_cursor else null end,
          'run_worker_id',v_worker.id,'worker_status',v_worker.status,
          'issue_code',v_worker.review_issue_code,'issue_work_id',v_worker.review_issue_work_id,
          -- NULL means the exact owner did not report a count; do not fabricate
          -- zero for a REVIEW reached after earlier rows in the same page.
          'rows_visited',v_result->>'rows_visited');
        end if;
      when 'PAYE_NET_ENTRY' then
        v_output:=v_output||pg_catalog.jsonb_build_object(
          'phase',v_result->>'phase','projection_id',v_result->>'projection_id');
        if v_case_net then
          v_output:=v_output||pg_catalog.jsonb_build_object(
            'cursor',v_result->>'cursor','rows_visited',v_result->>'rows_visited',
            'run_worker_id',v_result->>'run_worker_id','issue_code',v_result->>'issue_code');
        end if;
      when 'TRANSFER_BUILD' then
        v_output:=v_output||pg_catalog.jsonb_build_object(
          'phase',v_result->>'phase','transfer_id',v_result->>'transfer_id',
          'transfer_status',coalesce(v_result->>'transfer_status',v_result->>'phase'),
          'cursor',v_job.cursor_key,'member_count',v_result->>'member_count',
          'rows_visited',coalesce(v_result->>'rows_visited','0'));
      when 'CSV_SETTLEMENT' then
        v_output:=v_output||pg_catalog.jsonb_build_object(
          'phase',case when v_job.status='DONE' then 'COMPLETE' else 'MEMBERS' end,
          'cursor',v_job.cursor_key,'posted_members',v_result->>'posted_members',
          'rows_visited',coalesce(v_result->>'rows_visited','0'));
      when 'INTERNAL_SETTLEMENT' then
        v_output:=v_output||pg_catalog.jsonb_build_object(
          'phase',case when v_job.status='DONE' then 'COMPLETE' else 'MEMBERS' end,
          'cursor',v_job.cursor_key,'posted_members',v_result->>'posted_members',
          'rows_visited',coalesce(v_result->>'rows_visited','0'));
      when 'CSV_RETURN' then
        v_output:=v_output||pg_catalog.jsonb_build_object(
          'phase','COMPLETE','return_cash_id',v_result->>'return_cash_id');
      when 'CASH_REISSUE' then
        v_output:=v_output||pg_catalog.jsonb_build_object(
          'phase','COMPLETE','transfer_id',v_result->>'transfer_id');
      when 'REISSUE_CANCEL' then
        v_output:=v_output||pg_catalog.jsonb_build_object('phase',v_result->>'phase',
          'transfer_id',v_result->>'transfer_id','blocked_code',v_result->>'blocked_code');
      when 'SIMPLE_CANCEL' then
        v_output:=v_output||pg_catalog.jsonb_build_object(
          'phase',v_result->>'phase','run_worker_id',v_result->>'run_worker_id',
          'cursor',v_job.cursor_key,'released_line_count',v_result->>'released_line_count',
          'rows_visited',coalesce(v_result->>'rows_visited','0'),
          'holds_released',coalesce(v_result->>'holds_released','0'),
          'blocked_code',v_result->>'blocked_code');
      when 'PREPARATION_EXPIRY' then
        v_output:=v_output||pg_catalog.jsonb_build_object('phase',v_result->>'phase',
          'cursor',v_result->>'cursor','rows_visited',v_result->>'rows_visited',
          'work_released',v_result->>'work_released','case_holds_released',v_result->>'case_holds_released',
          'run_worker_id',v_result->>'run_worker_id');
      when 'CASE_CREATE' then
        v_output:=v_output||pg_catalog.jsonb_build_object('phase',v_result->>'phase',
          'case_id',v_result->>'case_id');
      when 'CASE_WRITE_OFF' then
        v_output:=v_output||pg_catalog.jsonb_build_object('phase',v_result->>'phase',
          'case_id',v_result->>'case_id','case_component_id',v_result->>'case_component_id',
          'applied_amount',v_result->>'applied_amount','remaining_amount',v_result->>'remaining_amount',
          'protected_amount',v_result->>'protected_amount','event_id',v_result->>'event_id','issue_code',v_result->>'issue_code');
    end case;
  end if;
  if pg_catalog.octet_length(v_output::text)>120000 then
    raise exception using errcode='54000',message='BPAY_NEXT_JOB_RESPONSE_INVALID';
  end if;
  return v_output;
end
$function$;

create or replace function public.bpay_next_enroll_service_v1()
returns jsonb language plpgsql volatile security definer
set search_path=pg_catalog,private
as $function$
declare
  v_before bigint; v_after bigint; v_job_id uuid;
  v_job private.bpay_next_job%rowtype;
  v_error text;v_blocked_id uuid;
begin
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
    nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception using errcode='42501',message='BPAY_NEXT_RUNTIME_SERVICE_FORBIDDEN';
  end if;
  if not exists(select 1 from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select e.last_enrolled_sequence into strict v_before
    from private.bpay_next_enrollment_clock e where e.id=1 for update;
  begin
    v_job_id:=private.bpay_next_enroll_one_v1();
  exception when others then
    get stacked diagnostics v_error=returned_sqlstate;
    update private.bpay_next_command c set enrollment_error_code=v_error,
      enrollment_error_at_utc=pg_catalog.transaction_timestamp()
      where c.agency_sequence=v_before+1 returning c.id into v_blocked_id;
    if v_blocked_id is null then raise;end if;
    return pg_catalog.jsonb_build_object('progressed',false,'last_enrolled_sequence',v_before::text,
      'job_id',null,'job_kind',null,'job_status',null,'blocked_command_id',v_blocked_id,'enrollment_error_code',v_error);
  end;
  update private.bpay_next_command c set enrollment_error_code=null,enrollment_error_at_utc=null
    where c.agency_sequence=v_before+1 and c.enrollment_error_code is not null;
  select e.last_enrolled_sequence into strict v_after
    from private.bpay_next_enrollment_clock e where e.id=1;
  if v_job_id is not null then
    select j.* into strict v_job from private.bpay_next_job j where j.id=v_job_id;
  end if;
  return pg_catalog.jsonb_build_object(
    'progressed',v_job_id is not null or v_after>v_before,
    'last_enrolled_sequence',v_after::text,
    'job_id',v_job_id,'job_kind',v_job.job_kind,'job_status',v_job.status,
    'blocked_command_id',null,'enrollment_error_code',null);
end
$function$;

alter function private.bpay_next_begin_prepare_projection_v1(uuid,uuid,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_begin_prepare_projection_v1(uuid,uuid,bigint,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_prepare_projection_page_v1(uuid,uuid,bigint,text,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_prepare_projection_page_v1(uuid,uuid,bigint,text,bigint,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_claim_prepare_job_v1(uuid,integer) owner to postgres;
revoke all on function private.bpay_next_claim_prepare_job_v1(uuid,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_pin_selected_work_v1(uuid,uuid,bigint,bigint) owner to postgres;
revoke all on function private.bpay_next_pin_selected_work_v1(uuid,uuid,bigint,bigint) from public,anon,authenticated,service_role;
alter function private.bpay_next_capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer) owner to postgres;
revoke all on function private.bpay_next_capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_hold_fresh_position_page_v1(uuid,uuid,bigint,uuid,text,integer) owner to postgres;
revoke all on function private.bpay_next_hold_fresh_position_page_v1(uuid,uuid,bigint,uuid,text,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_complete_no_case_worker_v1(uuid,uuid,bigint) owner to postgres;
revoke all on function private.bpay_next_complete_no_case_worker_v1(uuid,uuid,bigint) from public,anon,authenticated,service_role;
alter function private.bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer) from public,anon,authenticated,service_role;
alter function public.bpay_next_financial_job_v1(text,uuid,text,jsonb) owner to postgres;
revoke all on function public.bpay_next_financial_job_v1(text,uuid,text,jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_financial_job_v1(text,uuid,text,jsonb) to service_role;
alter function public.bpay_next_enroll_service_v1() owner to postgres;
revoke all on function public.bpay_next_enroll_service_v1() from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_enroll_service_v1() to service_role;
notify pgrst, 'reload schema';
commit;
