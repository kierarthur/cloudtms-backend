-- F-25 existing no-case owners consume retained scope/floor terms.
-- Live weekly arrangement and genuine later payroll-net ownership preserved.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_publish_no_case_week_fact_v1(
  p_run_worker_id uuid,p_projection_id uuid default null
) returns void
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_worker private.bpay_next_run_worker%rowtype;
  v_run private.bpay_next_pay_run%rowtype;
  v_projection private.bpay_next_net_projection%rowtype;
  v_prior private.bpay_next_worker_week_contribution%rowtype;
  v_accepted private.bpay_next_accepted_input%rowtype;
  v_week date;v_floor numeric;v_amount numeric;v_old_amount numeric:=0;
begin
  -- Callers already hold header -> Candidate control -> worker. Never enter
  -- this owner from an arbitrary row trigger with a different lock order.
  select * into strict v_worker from private.bpay_next_run_worker where id=p_run_worker_id;
  select * into strict v_run from private.bpay_next_pay_run where id=v_worker.run_id;
  if v_worker.case_selection_revision<>0 or v_worker.status<>'READY'
     or v_worker.financial_resolution_count<>0
     or v_run.status not in ('PREPARING','DRAFT') then
    raise exception using errcode='55000',message='BPAY_NEXT_NO_CASE_WEEK_OWNER_INVALID';
  end if;
  if v_worker.target_pay_channel<>'PAYE' then return;end if;
  select a.* into strict v_accepted from private.bpay_next_accepted_input a
    join private.bpay_next_run_command rc on rc.run_id=a.run_id and rc.command_id=a.prepare_command_id
    join private.bpay_next_command c on c.id=rc.command_id and c.command_kind='PREPARE'
      and c.agency_sequence=a.accepted_sequence and c.module_epoch=a.module_epoch
    where a.run_id=v_run.id and a.candidate_id=v_worker.candidate_id;
  if v_accepted.case_scope<>'NO_CASE' or v_accepted.case_selection_revision is not null
    or v_accepted.target_pay_channel<>v_worker.target_pay_channel
    or exists(select 1 from private.bpay_next_accepted_case_input x where x.accepted_input_id=v_accepted.id) then
    raise exception using errcode='23514',message='BPAY_NEXT_NO_CASE_ACCEPTED_SCOPE_INVALID';
  end if;
  v_week:=v_accepted.pay_week_start;
  v_floor:=case when v_accepted.floor_state='BOUND' then v_accepted.candidate_floor else null end;
  v_amount:=v_worker.gross_inc_vat;
  if p_projection_id is not null then
    select * into strict v_projection from private.bpay_next_net_projection where id=p_projection_id;
    if v_run.status<>'DRAFT' or v_run.confirmed_at_utc is null
       or v_projection.run_worker_id<>v_worker.id or v_projection.input_kind<>'PAYE_MANUAL'
       or v_projection.retired_at_utc is not null
       or v_projection.projection_no<>v_worker.net_projection_revision
       or v_projection.entered_paye_net is distinct from v_worker.entered_paye_net
       or v_projection.gross_inc_vat<>v_worker.gross_inc_vat
       or v_projection.cash_amount<>v_projection.entered_paye_net
       or v_projection.accepted_recoveries<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_NO_CASE_WEEK_PROJECTION_INVALID';
    end if;
    v_amount:=v_projection.cash_amount;
  elsif v_worker.net_projection_revision<>0 then
    raise exception using errcode='23514',message='BPAY_NEXT_NO_CASE_WEEK_NET_BINDING_REQUIRED';
  end if;
  -- A no-case payment does not depend on a debt take-home setting. If a new
  -- week cannot bind that setting, retain the existing absence/refusal evidence
  -- for a later case allocator; do not invent zero or block this payment.
  if (v_floor is null or v_floor::text in ('NaN','Infinity','-Infinity')
      or v_floor<0 or v_floor<>pg_catalog.trunc(v_floor,2))
     and not exists(select 1 from private.bpay_next_worker_week
       where candidate_id=v_worker.candidate_id and pay_week_start=v_week) then
    raise notice 'BPAY_NEXT_NO_CASE_WEEK_FLOOR_UNBOUND';
    return;
  end if;
  if not exists(select 1 from private.bpay_next_worker_week
     where candidate_id=v_worker.candidate_id and pay_week_start=v_week) then
    insert into private.bpay_next_worker_week(candidate_id,pay_week_start,period_revision,
      default_floor,default_floor_revision,resolved_arranged_take_home)
      values(v_worker.candidate_id,v_week,1,v_floor,1,0);
  end if;
  perform 1 from private.bpay_next_worker_week where candidate_id=v_worker.candidate_id
    and pay_week_start=v_week for update;
  select * into v_prior from private.bpay_next_worker_week_contribution
    where original_run_worker_id=v_worker.id for update;
  if v_prior.id is not null then
    if v_prior.candidate_id<>v_worker.candidate_id or v_prior.pay_week_start<>v_week
       or v_prior.original_pay_date<>v_run.pay_date
       or v_prior.original_created_at_utc<>v_run.created_at_utc
       or v_prior.original_gross_amount<>v_worker.gross_inc_vat
       or v_prior.payment_state<>'ARRANGED' or v_prior.eligibility_state='UNBOUND'
       or v_prior.original_transfer_id is not null or v_prior.returned_cash_id is not null then
      raise exception using errcode='23514',message='BPAY_NEXT_NO_CASE_WEEK_PRIOR_BINDING_INVALID';
    end if;
    -- Exact readback, including a genuine zero arrangement, spends no revision.
    if v_prior.accepted_projection_id is not distinct from p_projection_id
       and v_prior.eligible_arranged_amount=v_amount then return;end if;
    if p_projection_id is null or v_prior.accepted_projection_id is not null then
      raise exception using errcode='23514',message='BPAY_NEXT_NO_CASE_WEEK_SWITCH_INVALID';
    end if;
    v_old_amount:=v_prior.eligible_arranged_amount;
    update private.bpay_next_worker_week_contribution set basis_kind='PAYROLL_NET',
      original_payroll_net=v_projection.entered_paye_net,
      accepted_projection_id=p_projection_id,accepted_bank_cash=v_amount,
      eligibility_state=case when v_amount=0 then 'EXCLUDED' else 'ELIGIBLE' end,
      eligible_arranged_amount=v_amount,contribution_revision=contribution_revision+1,
      primary_binding_revision=primary_binding_revision+1 where id=v_prior.id;
  else
    if p_projection_id is not null then
      -- READY may have preserved an explicitly unbound setting. Never turn
      -- that absence into a guessed earlier contribution or reject net entry.
      -- The later case reader already refuses a missing prior fact, not zero.
      raise notice 'BPAY_NEXT_NO_CASE_WEEK_READY_FACT_UNBOUND';
      return;
    end if;
    insert into private.bpay_next_worker_week_contribution(candidate_id,pay_week_start,
      original_run_worker_id,original_pay_date,original_created_at_utc,contribution_revision,
      basis_kind,original_gross_amount,payment_state,eligibility_state,
      eligible_arranged_amount,primary_binding_revision)
      values(v_worker.candidate_id,v_week,v_worker.id,v_run.pay_date,v_run.created_at_utc,
        1,'GROSS_FALLBACK',v_worker.gross_inc_vat,'ARRANGED',
        case when v_amount=0 then 'EXCLUDED' else 'ELIGIBLE' end,v_amount,1);
  end if;
  update private.bpay_next_worker_week set
    resolved_arranged_take_home=resolved_arranged_take_home-v_old_amount+v_amount,
    period_revision=period_revision+1
    where candidate_id=v_worker.candidate_id and pay_week_start=v_week;
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
  v_module_epoch bigint;
  v_accepted private.bpay_next_accepted_input%rowtype;
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
    where id=v_run_id for share;
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
  if v_worker.captured_position_count=0
     or v_worker.captured_position_count<>v_worker.captured_line_count then
    raise exception using errcode='23514',
      message='BPAY_NEXT_NO_CASE_COMPLETION_LINE_COUNT_MISMATCH';
  end if;
  -- Exact accepted NO_CASE remains NO_CASE after a later case is created.
  select a.* into strict v_accepted from private.bpay_next_accepted_input a
    where a.run_id=v_run_id and a.prepare_command_id=v_job.command_id
      and a.candidate_id=v_job.candidate_id and a.module_epoch=v_job.module_epoch;
  if v_accepted.case_scope<>'NO_CASE' or v_accepted.case_selection_revision is not null
    or v_worker.case_selection_revision<>0 or exists(select 1 from private.bpay_next_accepted_case_input x
      where x.accepted_input_id=v_accepted.id) then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CASE_ALLOCATION_REQUIRED';
  end if;
  update private.bpay_next_run_worker set status='READY'
    where id=v_worker.id;
  perform private.bpay_next_publish_no_case_week_fact_v1(v_worker.id,null);
  update private.bpay_next_job
    set status='DONE',phase='READY',cursor_key=null,
        lease_nonce=null,lease_until_utc=null,
        position_work_cursor=null,position_component_cursor=null
    where id=p_job_id;
  update private.bpay_next_worker_control
    set financial_view_revision=financial_view_revision+1,
        updated_at_utc=pg_catalog.transaction_timestamp()
    where candidate_id=v_job.candidate_id;
  return pg_catalog.jsonb_build_object('done',true,'phase','READY',
    'replay',false,'run_worker_id',v_worker.id);
end
$function$;

alter function private.bpay_next_publish_no_case_week_fact_v1(uuid,uuid) owner to postgres;
alter function private.bpay_next_complete_no_case_worker_v1(uuid,uuid,bigint) owner to postgres;
revoke all on function private.bpay_next_publish_no_case_week_fact_v1(uuid,uuid),private.bpay_next_complete_no_case_worker_v1(uuid,uuid,bigint) from public,anon,authenticated,service_role;
commit;
