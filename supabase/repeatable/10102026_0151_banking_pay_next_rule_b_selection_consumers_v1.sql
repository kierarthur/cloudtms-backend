-- F-25 owner rule B; four exact three-way surviving owner seams only.
-- Preserve original PIN REVIEW/validation, F-05 claim order and live money locks.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_seal_selection_v1(
  p_run_id uuid,p_expected_pages bigint,p_expected_count bigint
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private
as $function$
declare v_run private.bpay_next_pay_run%rowtype;v_selected record;
begin
  if p_run_id is null or p_expected_pages is null or p_expected_count is null
     or p_expected_pages<0 or p_expected_count<0
     or (p_expected_pages=0) is distinct from (p_expected_count=0) then
    raise exception using errcode='22023',
      message='BPAY_NEXT_SELECTION_SEAL_INPUT_INVALID';
  end if;
  select * into strict v_run from private.bpay_next_pay_run
    where id=p_run_id for update;
  if v_run.selection_page_count<>p_expected_pages
     or v_run.selection_count<>p_expected_count then
    raise exception using errcode='23514',
      message='BPAY_NEXT_SELECTION_SEAL_COUNT_MISMATCH';
  end if;
  if v_run.work_choice_count<>v_run.selection_count
     or v_run.sealed_work_choice_count<>v_run.selection_count then
    raise exception using errcode='55000',
      message='BPAY_NEXT_SELECTION_COMPONENT_CHOICES_NOT_SEALED';
  end if;
  if v_run.selection_state='SEALED' then
    return jsonb_build_object('run_id',p_run_id,'sealed',true,
      'selection_count',v_run.selection_count,'replay',true);
  end if;
  -- A case-only run contains genuine selected case components, not fictitious
  -- WORK rows. The root-locked seal counter is maintained once per Candidate
  -- by0330; checking it does not scan every Candidate at final confirmation.
  if p_expected_count=0 and (v_run.selected_candidate_count<1
     or v_run.sealed_case_candidate_count<>v_run.selected_candidate_count) then
    raise exception using errcode='55000',message='BPAY_NEXT_SELECTION_CASE_ONLY_NOT_SEALED';
  end if;
  if v_run.status<>'PREPARING' then
    raise exception using errcode='55000',
      message='BPAY_NEXT_SELECTION_SEAL_STATUS_INVALID';
  end if;
  -- Owner rule B: capture only the existing ticked expected revisions.
  -- Exact sealed replay above never refreshes settings or query state.
  perform private.weekly_source_pay_query_admit_v2();
  update private.bpay_next_selection_candidate selected
    set target_pay_channel=upper(candidate.pay_method),
        target_umbrella_id=candidate.umbrella_id,
        target_umbrella_vat_chargeable=umbrella.vat_chargeable,
        min_take_home_wtd=candidate.min_take_home_wtd
    from public.candidates candidate left join public.umbrellas umbrella on umbrella.id=candidate.umbrella_id
    where selected.run_id=p_run_id and selected.candidate_id=candidate.id;
  for v_selected in
    select choice.work_id,choice.expected_revision_id,revision.physical_timesheet_id
      from private.bpay_next_run_selection selected
      join private.bpay_next_work_choice choice on choice.run_id=selected.run_id and choice.work_id=selected.work_id
      join private.bpay_next_work work on work.id=selected.work_id
      join private.bpay_next_work_revision revision on revision.work_id=choice.work_id and revision.id=choice.expected_revision_id
      where selected.run_id=p_run_id and work.work_kind='SOURCE' and revision.source_kind in ('SOURCE','PROTECTED')
      order by selected.selection_no
  loop
    insert into private.bpay_next_run_work_query_snapshot(run_id,work_id,expected_revision_id,physical_timesheet_id,gate_result,captured_at_utc)
      values(p_run_id,v_selected.work_id,v_selected.expected_revision_id,v_selected.physical_timesheet_id,
        private.weekly_source_pay_query_gate_v1(v_selected.physical_timesheet_id),pg_catalog.transaction_timestamp());
  end loop;
  update private.bpay_next_pay_run
    set selection_state='SEALED',review_revision=review_revision+1
    where id=p_run_id;
  return jsonb_build_object('run_id',p_run_id,'sealed',true,
    'selection_count',v_run.selection_count,'replay',false);
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
    update private.bpay_next_job
      set status='DONE',phase='DONE',cursor_key=null,
          lease_nonce=null,lease_until_utc=null
      where id=p_job_id;
    return pg_catalog.jsonb_build_object('done',true,'phase','REVIEW',
      'issue_code',case
        when v_work.approval_state<>'APPROVED' or v_work.current_revision_id is null
          or v_choice.expected_revision_id is null
          then 'SELECTED_WORK_NOT_APPROVED'
        when v_choice.expected_revision_id is distinct from v_work.current_revision_id
          then 'SELECTED_WORK_REVISION_CHANGED'
        else 'SELECTED_WORK_POSITION_PENDING' end,
      'work_id',v_work.id,'replay',false);
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


create or replace function private.bpay_next_prepare_case_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,
  p_expected_cursor bigint default null,p_limit integer default 100
) returns jsonb
language plpgsql security invoker
set search_path=pg_catalog,private,public
as $function$
declare
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
  v_policy_id uuid;
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
  if v_state.id is null and not exists(select 1 from private.bpay_next_finance_case c where c.candidate_id=v_candidate)
     and not exists(select 1 from private.bpay_next_case_selection s where s.run_id=v_run_id and s.candidate_id=v_candidate) then
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
        select r.* into v_rule from private.bpay_next_case_rule r where r.id=v_case.current_rule_id
          and r.case_id=v_case.id and r.candidate_id=v_candidate and r.rule_revision=v_case.current_rule_revision;
        select r.* into v_origin from private.bpay_next_case_create_request r where r.case_id=v_case.id and r.candidate_id=v_candidate;
        -- Refuse the immutable selected identity before qualification: a stale
        -- Source collection must not disappear into an ordinary review path.
        if exists(select 1 from private.bpay_next_work_collection c
          join private.bpay_next_work w on w.id=c.work_id and w.candidate_id=c.candidate_id
          where c.case_id=v_case.id and c.case_component_id=v_component.id
            and c.candidate_id=v_candidate and w.work_kind='SOURCE') then
          raise exception using errcode='55000',message='BPAY_NEXT_COMPARISON_NOT_INSTALLED';
        end if;
        v_collection:=private.bpay_next_work_collection_origin_v1(v_case.id,v_component.id,v_candidate);
        v_automatic:=v_collection.id is not null;
        if v_state.weekly_binding_state='NOT_AFFECTED'
          and (v_case.case_kind,v_component.case_kind,v_component.instruction_kind,v_component.payroll_stage)
            is distinct from ('OVERPAYMENT','OVERPAYMENT','RECOVERY','GROSS_DEDUCT') then
          v_issue:='CASE_WEEK_BASIS_UNBOUND';exit prepare_page;
        end if;
        if v_rule.id is null or v_case.case_revision<1 or v_component.rule_id<>v_rule.id then
          v_issue:='CASE_INPUT_UNBOUND';exit prepare_page;
        end if;
        if v_automatic then
          -- Genuine paid WORK origin, not a fabricated CASE_CREATE receipt.
          -- The immutable original paid policy and the exact applied position
          -- are authority; a newer queued WORK approval is not a reprice.
          if v_origin.command_id is not null then v_issue:='CASE_INPUT_UNBOUND';exit prepare_page;end if;
          v_policy_id:=v_collection.original_valuation_policy_id;
        elsif v_origin.command_id is null
           or v_component.rule_id<>v_rule.id or v_origin.case_kind<>v_case.case_kind
           or v_origin.source_pay_channel<>v_component.source_pay_channel
           or v_origin.principal_source_ex_vat<>v_component.approved_source_ex_vat
           or (v_component.id<>v_origin.primary_component_id
             and (v_origin.recovery_component_id is null or v_component.id<>v_origin.recovery_component_id))
           or (v_component.id=v_origin.primary_component_id and v_component.instruction_kind<>
             case when v_case.case_kind in ('LOAN','ADVANCE') then 'PAYOUT'
               when v_case.case_kind='CREDIT' then 'CREDIT' else 'RECOVERY' end)
           or (v_component.id=v_origin.recovery_component_id and v_component.instruction_kind<>'RECOVERY') then
          v_issue:='CASE_INPUT_UNBOUND';exit prepare_page;
        else
          v_policy_id:=v_origin.valuation_policy_id;
        end if;
        if v_component.source_pay_channel<>'PAYE' or v_worker.target_pay_channel<>'PAYE' then
          v_issue:='CASE_TARGET_UNSUPPORTED';exit prepare_page;
        end if;
        v_explanation:='Exact approved case; same-channel PAYE current basis captured';
        v_outstanding:=case when v_component.case_kind in ('LOAN','ADVANCE') then v_component.funded_source_ex_vat
          else v_component.approved_source_ex_vat end-v_component.recovered_source_ex_vat-v_component.written_off_source_ex_vat;
        v_case_outstanding:=case when v_case.case_kind in ('LOAN','ADVANCE') then v_case.principal_funded
          else v_case.principal_approved end-v_case.principal_recovered-v_case.principal_written_off-v_case.principal_paid_credit;
        if v_component.instruction_kind='RECOVERY' then
          if v_automatic then
            -- Retained automatic OVERPAYMENT policy has no instalment. Its
            -- remaining principal already excludes realised recovery and W;
            -- capacity below subtracts each disjoint active hold only once.
            v_nominal:=least(v_outstanding,v_case_outstanding);
            v_explanation:='Exact original ordinary PAYE collection; remaining outstanding, no instalment';
          else
          if v_rule.weekly_due_source_ex_vat is null then v_issue:='CASE_INPUT_UNBOUND';exit prepare_page;end if;
          if v_case.case_kind in ('LOAN','ADVANCE') and v_case.principal_funded>0
             and (v_rule.original_funding_event_id is null or v_rule.original_funded_at_utc is null) then
            v_issue:='CASE_INPUT_UNBOUND';exit prepare_page;
          end if;
          v_nominal:=case when v_state.pay_week_start>=v_rule.next_due_monday
            then least(v_rule.weekly_due_source_ex_vat,v_outstanding,v_case_outstanding) else 0 end;
          if v_case.case_kind in ('LOAN','ADVANCE') and v_case.principal_funded=0 then
            v_nominal:=0;v_explanation:='Approved loan/advance is unfunded; repayment not eligible';
          elsif v_state.pay_week_start<v_rule.next_due_monday then
            v_explanation:='Repayment schedule is not yet due in the pay-date week';
          end if;
          end if;
          v_purpose:=case when v_component.payroll_stage='GROSS_DEDUCT' then 'GROSS_RECOVERY' else 'NET_RECOVERY_CAPACITY' end;
        else
          v_outstanding:=v_component.approved_source_ex_vat-v_component.funded_source_ex_vat
            -v_component.paid_credit_source_ex_vat-v_component.written_off_source_ex_vat;
          v_case_outstanding:=v_case.principal_approved-v_case.principal_funded
            -v_case.principal_paid_credit-v_case.principal_written_off;
          v_nominal:=least(v_outstanding,v_case_outstanding);v_purpose:='PAYOUT';
        end if;
        -- Weekly due exists independently of this particular arrangement's
        -- eligibility. An early/paused selection must not make the whole
        -- week's legitimate opening due permanently zero for later pay dates.
        v_opening_due:=case when v_component.instruction_kind='RECOVERY' then v_nominal else 0 end;
        if v_case.status<>'OPEN' or (v_case.due_date is not null and v_case.due_date>v_run.pay_date) then
          v_nominal:=0;v_explanation:='Selected case is not currently open/due; no reservation';
        end if;
        if v_outstanding<0 or v_case_outstanding<0 or v_nominal<0 then
          v_issue:='CASE_INPUT_UNBOUND';exit prepare_page;
        end if;
        -- Opening due is independent of later principal. Never subtract the
        -- same realised recovery twice or use max(overlapping counters).
        insert into private.bpay_next_case_period(case_component_id,case_id,candidate_id,pay_week_start,rule_id,
          period_revision,opening_outstanding_source_ex_vat,opening_due_source_ex_vat,work_collection_id)
          values(v_component.id,v_case.id,v_candidate,v_state.pay_week_start,v_rule.id,1,v_outstanding,
            v_opening_due,case when v_automatic then v_collection.id else null end)
          on conflict(case_component_id,pay_week_start) do nothing;
        select p.* into strict v_period from private.bpay_next_case_period p
          where p.case_component_id=v_component.id and p.pay_week_start=v_state.pay_week_start for update;
        if v_period.work_collection_id is distinct from (case when v_automatic then v_collection.id else null end) then
          v_issue:='CASE_INPUT_UNBOUND';exit prepare_page;
        end if;
        if v_period.rule_id<>v_rule.id then
          -- Genuine original funding advances an immutable loan rule solely
          -- to bind its age/evidence. It is NOT a schedule/policy reprice.
          select * into strict v_period_rule from private.bpay_next_case_rule where id=v_period.rule_id;
          if v_case.case_kind not in ('LOAN','ADVANCE') or v_rule.original_funding_event_id is null
             or (v_period_rule.case_id,v_period_rule.candidate_id,v_period_rule.case_kind,v_period_rule.case_subtype,
               v_period_rule.tax_treatment,v_period_rule.case_created_at_utc,v_period_rule.minimum_earnings_threshold,
               v_period_rule.take_home_floor_override,v_period_rule.weekly_due_source_ex_vat,v_period_rule.schedule_start_monday,
               v_period_rule.next_due_monday,v_period_rule.schedule_week_count)
               is distinct from (v_rule.case_id,v_rule.candidate_id,v_rule.case_kind,v_rule.case_subtype,
                 v_rule.tax_treatment,v_rule.case_created_at_utc,v_rule.minimum_earnings_threshold,v_rule.take_home_floor_override,
                 v_rule.weekly_due_source_ex_vat,v_rule.schedule_start_monday,v_rule.next_due_monday,v_rule.schedule_week_count)
             or not exists(select 1 from private.bpay_next_case_event e where e.id=v_rule.original_funding_event_id
               and e.case_id=v_case.id and e.event_kind='FUNDED' and e.occurred_at_utc=v_rule.original_funded_at_utc)
             or (v_period_rule.original_funding_event_id is not null and
               (v_period_rule.original_funding_event_id,v_period_rule.original_funded_at_utc)
                 is distinct from (v_rule.original_funding_event_id,v_rule.original_funded_at_utc)) then
            v_issue:='CASE_INPUT_UNBOUND';exit prepare_page;
          end if;
          if v_component.instruction_kind='RECOVERY' and v_period_rule.original_funding_event_id is null then
            if (v_period.opening_outstanding_source_ex_vat,v_period.opening_due_source_ex_vat,
                v_period.realised_recovery_source_ex_vat,v_period.active_unrealised_recovery_source_ex_vat,v_period.active_payout_source_ex_vat)
                is distinct from (0::numeric,0::numeric,0::numeric,0::numeric,0::numeric) then
              v_issue:='CASE_INPUT_UNBOUND';exit prepare_page;
            end if;
            -- Only this exact zero/unconsumed pay-week is made ready. Its
            -- frozen old zero instructions/results remain immutable and zero.
            update private.bpay_next_case_period set rule_id=v_rule.id,opening_outstanding_source_ex_vat=v_outstanding,
              opening_due_source_ex_vat=v_opening_due,period_revision=period_revision+1
              where case_component_id=v_component.id and pay_week_start=v_state.pay_week_start returning * into v_period;
          end if;
          -- A consumed funded period (or original payout period) retains its
          -- recorded opening rule and counters; only the new instruction's
          -- captured_rule_id/age advance to the genuine current funded rule.
        end if;
        v_capacity:=case when v_automatic then
          least(v_nominal,greatest(v_outstanding-v_component.active_recovery_source_ex_vat,0),
            greatest(v_case_outstanding-v_case.active_recovery_hold_amount,0))
          when v_component.instruction_kind='RECOVERY' then
          least(v_nominal,greatest(v_period.opening_due_source_ex_vat-v_period.realised_recovery_source_ex_vat
            -v_period.active_unrealised_recovery_source_ex_vat,0),
            greatest(v_outstanding-v_component.active_recovery_source_ex_vat,0),
            greatest(v_case_outstanding-v_case.active_recovery_hold_amount,0))
          else least(v_nominal,greatest(v_outstanding-v_component.active_payout_source_ex_vat,0),
            greatest(v_case_outstanding-v_case.active_payout_hold_amount,0)) end;
        begin
          select x.* into strict v_value from private.bpay_next_value_source_for_target_v1(
            v_component.approved_source_ex_vat,'PAYE','PAYE',null,v_policy_id,v_run.pay_date) x;
        exception when check_violation then
          if sqlerrm in ('BPAY_NEXT_TARGET_POLICY_WINDOW_MISSING','BPAY_NEXT_TARGET_POLICY_WINDOW_AMBIGUOUS') then
            v_issue:='CASE_INPUT_UNBOUND';exit prepare_page;
          else raise;end if;
        end;
        if v_value.target_ex_vat<>v_component.approved_source_ex_vat or v_value.target_vat<>0
           or v_value.target_inc_vat<>v_value.target_ex_vat then
          v_issue:='CASE_INPUT_UNBOUND';exit prepare_page;
        end if;
        update private.bpay_next_case_component set resolution_state='RESOLVED',target_pay_channel='PAYE',
          target_ex_vat=v_value.target_ex_vat,target_vat=0,target_inc_vat=v_value.target_inc_vat,
          valuation_policy_id=v_policy_id,valuation_window_id=v_value.policy_window_id,
          resolution_ref=case when v_automatic then 'WORK_COLLECTION:'||v_collection.id::text||':PAYE'
            else 'CASE_CREATE:'||v_origin.command_id::text||':PAYE' end,component_revision=component_revision+1,
          updated_at_utc=pg_catalog.transaction_timestamp() where id=v_component.id returning * into v_component;
        v_age:=coalesce(v_rule.original_funded_at_utc,v_rule.case_created_at_utc);
        -- A genuinely bound stored credit keeps its original creator age. The
        -- exact point sidecar is immutable, not a live legacy money read.
        if not v_automatic then
          select coalesce(o.original_created_at_utc,v_age) into v_age
            from (select 1) s left join private.bpay_next_stored_credit_origin o
              on o.command_id=v_component.id and o.candidate_id=v_candidate;
        end if;
        v_age_key:=(extract(epoch from v_age)*1000000)::bigint;
        insert into private.bpay_next_run_case_instruction
          (run_worker_id,run_id,candidate_id,selection_revision,preparation_revision,case_component_id,case_id,
           captured_case_revision,captured_component_revision,captured_period_revision,rule_id,rule_revision,
           case_kind,case_subtype,tax_treatment,instruction_kind,direction,payroll_stage,hold_purpose,
           age_key,debt_age_at_utc,component_ordinal,pay_week_start,source_pay_channel,target_pay_channel,allocation_channel,currency,
           nominal_source_ex_vat,nominal_target_ex_vat,nominal_target_vat,nominal_target_inc_vat,
           capacity_source_ex_vat,capacity_target_ex_vat,capacity_target_vat,capacity_target_inc_vat,
           minimum_earnings_threshold,take_home_floor_override,captured_default_floor,captured_default_floor_revision,
           resolved_take_home_floor,valuation_policy_id,valuation_window_id,resolution_ref,explanation,work_collection_id)
          values(v_worker.id,v_run_id,v_candidate,v_state.selection_revision,v_worker.preparation_revision,v_component.id,v_case.id,
           v_case.case_revision,v_component.component_revision,v_period.period_revision,v_rule.id,v_rule.rule_revision,
           v_component.case_kind,v_component.case_subtype,v_component.tax_treatment,v_component.instruction_kind,
           v_component.direction,v_component.payroll_stage,v_purpose,v_age_key,v_age,v_component.component_ordinal,
           v_state.pay_week_start,'PAYE','PAYE','PAYE','GBP',v_nominal,v_nominal,0,v_nominal,v_capacity,v_capacity,0,v_capacity,
           v_rule.minimum_earnings_threshold,v_rule.take_home_floor_override,v_state.captured_default_floor,
           v_state.captured_default_floor_revision,coalesce(v_rule.take_home_floor_override,v_state.captured_default_floor),
           v_policy_id,v_value.policy_window_id,v_component.resolution_ref,v_explanation,
           case when v_automatic then v_collection.id else null end)
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
    for v_instruction in select i.* from private.bpay_next_run_case_instruction i
      where i.run_worker_id=v_worker.id and i.preparation_revision=v_worker.preparation_revision
        and i.selection_revision=v_state.selection_revision
        and (v_state.cursor_case_component_id is null or (i.age_key,i.case_id,i.component_ordinal,i.case_component_id)
          >(v_state.cursor_age_key,v_state.cursor_case_id,v_state.cursor_component_ordinal,v_state.cursor_case_component_id))
      order by i.age_key,i.case_id,i.component_ordinal,i.case_component_id limit p_limit
    loop
      v_seen:=v_seen+1;
      if v_instruction.hold_purpose='PAYOUT' then
        v_taken:=least(v_instruction.nominal_target_ex_vat,v_instruction.capacity_target_ex_vat);
        v_cap_reason:=null;v_affordability_reason:=null;
        v_capacity_reason:=case when v_instruction.capacity_target_ex_vat<v_instruction.nominal_target_ex_vat then 'CASE_CAPACITY' else null end;
        v_next_e:=v_e;v_next_h:=v_h;
      else
        select x.* into strict v_row from private.bpay_next_case_allocation_row_v1(v_instruction.case_kind,
          v_instruction.nominal_target_ex_vat,v_instruction.capacity_target_ex_vat,v_e,v_h,
          v_instruction.minimum_earnings_threshold,v_instruction.take_home_floor_override,v_instruction.captured_default_floor) x;
        v_taken:=v_row.taken_amount;v_cap_reason:=v_row.cap_reason;
        v_affordability_reason:=v_row.affordability_reason;v_capacity_reason:=v_row.capacity_reason;
        v_next_e:=v_row.next_channel_headroom;v_next_h:=v_row.next_worker_take_home;
      end if;
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
      update private.bpay_next_job set status='DONE',phase='READY',cursor_key=null,lease_nonce=null,lease_until_utc=null,
        position_work_cursor=null,position_component_cursor=null where id=p_job_id;
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
    update private.bpay_next_job set status='DONE',phase='DONE',cursor_key=null,lease_nonce=null,lease_until_utc=null,
      position_work_cursor=null,position_component_cursor=null where id=p_job_id;
    v_terminal:=true;
  elsif not v_terminal then
    update private.bpay_next_job set cursor_key=(v_saved_cursor+1)::text where id=p_job_id;
  end if;
  update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1,
    updated_at_utc=pg_catalog.transaction_timestamp() where candidate_id=v_candidate;
  return pg_catalog.jsonb_build_object('done',v_terminal,'phase',case when v_issue is not null then 'REVIEW'
    when v_terminal then 'READY' else 'FINANCE_ALLOCATE' end,'cursor',case when v_terminal then null else (v_saved_cursor+1)::text end,
    'rows_visited',v_seen::text,'replay',false,'run_worker_id',v_worker.id,
    'worker_status',case when v_issue is not null then 'REVIEW' when v_terminal then 'READY' else 'PREPARING' end)
    ||case when v_issue is not null then pg_catalog.jsonb_build_object('issue_code',v_issue) else '{}'::jsonb end;
end
$function$;


alter function private.bpay_next_seal_selection_v1(uuid,bigint,bigint) owner to postgres;
revoke all on function private.bpay_next_seal_selection_v1(uuid,bigint,bigint) from public,anon,authenticated,service_role;
alter function private.bpay_next_claim_prepare_job_v1(uuid,integer) owner to postgres;
revoke all on function private.bpay_next_claim_prepare_job_v1(uuid,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_pin_selected_work_v1(uuid,uuid,bigint,bigint) owner to postgres;
revoke all on function private.bpay_next_pin_selected_work_v1(uuid,uuid,bigint,bigint) from public,anon,authenticated,service_role;
alter function private.bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer) from public,anon,authenticated,service_role;
-- The existing effect guard is a repeatable authority; attach only after it
-- exists on clean NEW. These hooks belong solely to the private snapshot.
drop trigger if exists bpay_next_run_work_query_snapshot_immutable on private.bpay_next_run_work_query_snapshot;
create trigger bpay_next_run_work_query_snapshot_immutable before update or delete
 on private.bpay_next_run_work_query_snapshot for each row execute function private.bpay_next_effect_immutable_v1();
drop trigger if exists bpay_next_run_work_query_snapshot_no_truncate on private.bpay_next_run_work_query_snapshot;
create trigger bpay_next_run_work_query_snapshot_no_truncate before truncate
 on private.bpay_next_run_work_query_snapshot for each statement execute function private.bpay_next_effect_immutable_v1();
notify pgrst,'reload schema';
commit;
