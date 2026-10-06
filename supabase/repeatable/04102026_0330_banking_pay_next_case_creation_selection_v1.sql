-- Owner-only case approval and explicit pre-allocation selection. No funding,
-- recovery, TARGET guess, WORK fabrication, legacy delegate or history scan.
-- CASE_CREATE enrollment/queue dispatch and genuine PREPARE binding are separate
-- connected owners. Policy X: this never amends an existing frozen instruction.
\set ON_ERROR_STOP on
begin;

drop trigger if exists bpay_next_case_create_request_immutable_v1 on private.bpay_next_case_create_request;
create trigger bpay_next_case_create_request_immutable_v1 before update or delete
  on private.bpay_next_case_create_request for each row
  execute function private.bpay_next_effect_immutable_v1();

create or replace function private.bpay_next_accept_case_create_v1(
  p_command_id uuid,p_candidate_id uuid,p_actor_user_id uuid,
  p_case_kind text,p_case_subtype text,p_tax_treatment text,p_principal numeric,
  p_due_date date,p_start_monday date,p_weekly_due numeric,p_week_count bigint,
  p_minimum_earnings_threshold numeric,p_take_home_floor_override numeric,p_reason text
) returns jsonb
language plpgsql security invoker
set search_path=pg_catalog,private,public
as $function$
declare
  v_epoch bigint;
  v_prior private.bpay_next_case_create_request%rowtype;
  v_sequence bigint;
  v_source_channel text;
  v_candidate_active boolean;
  v_policy uuid;
  v_weekly numeric;
  v_weeks bigint;
  v_required_weeks numeric;
  v_at timestamptz;
  v_week date;
  v_recovery_id uuid;
begin
  if p_command_id is null or p_candidate_id is null or p_actor_user_id is null
     or p_case_kind is null or p_case_subtype is null or p_tax_treatment is null
     or p_principal is null or p_reason is null
     or pg_catalog.octet_length(p_reason) not between 1 and 2048
     or pg_catalog.btrim(p_reason)='' then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_CREATE_INPUT_INVALID';
  end if;
  if not ((p_case_kind='LOAN' and p_case_subtype='LOAN' and p_tax_treatment='NOT_APPLICABLE')
    or (p_case_kind='ADVANCE' and p_case_subtype='PAYMENT_ADVANCE' and p_tax_treatment='NOT_APPLICABLE')
    or (p_case_kind='MANUAL_DEBT' and p_case_subtype='MANUAL_DEBT' and p_tax_treatment in ('TAXABLE','NON_TAXABLE'))
    or (p_case_kind='CREDIT' and p_case_subtype='MANUAL_CREDIT' and p_tax_treatment in ('TAXABLE','NON_TAXABLE'))) then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_CREATE_FAMILY_INVALID';
  end if;
  -- Domain operands are validated without numeric typmod rounding. Do it before
  -- arithmetic so NaN/Infinity/subpenny/negative input cannot affect a schedule.
  if p_principal::text in ('NaN','Infinity','-Infinity') or p_principal<=0
     or p_principal<>pg_catalog.trunc(p_principal,2)
     or (p_weekly_due is not null and (p_weekly_due::text in ('NaN','Infinity','-Infinity')
       or p_weekly_due<=0 or p_weekly_due<>pg_catalog.trunc(p_weekly_due,2)))
     or (p_minimum_earnings_threshold is not null and (p_minimum_earnings_threshold::text in ('NaN','Infinity','-Infinity')
       or p_minimum_earnings_threshold<0 or p_minimum_earnings_threshold<>pg_catalog.trunc(p_minimum_earnings_threshold,2)))
     or (p_take_home_floor_override is not null and (p_take_home_floor_override::text in ('NaN','Infinity','-Infinity')
       or p_take_home_floor_override<0 or p_take_home_floor_override<>pg_catalog.trunc(p_take_home_floor_override,2)))
     or (p_due_date is not null and not pg_catalog.isfinite(p_due_date))
     or (p_start_monday is not null and (not pg_catalog.isfinite(p_start_monday) or extract(isodow from p_start_monday)<>1))
     or (p_week_count is not null and p_week_count<1) then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_CREATE_VALUE_INVALID';
  end if;
  if p_case_kind='CREDIT' then
    if p_start_monday is not null or p_weekly_due is not null or p_week_count is not null
       or p_minimum_earnings_threshold is not null or p_take_home_floor_override is not null then
      raise exception using errcode='22023',message='BPAY_NEXT_CASE_CREDIT_REPAYMENT_RULE_FORBIDDEN';
    end if;
  else
    if p_start_monday is null or (p_weekly_due is null and p_week_count is null) then
      raise exception using errcode='22023',message='BPAY_NEXT_CASE_REPAYMENT_INPUT_REQUIRED';
    end if;
    v_weekly:=coalesce(p_weekly_due,pg_catalog.ceil((p_principal/p_week_count)*100)/100);
    v_required_weeks:=greatest(pg_catalog.ceil(p_principal/v_weekly),1);
    if v_required_weeks>9223372036854775807 or (p_week_count is not null and p_week_count<v_required_weeks) then
      raise exception using errcode='22023',message='BPAY_NEXT_CASE_SCHEDULE_DOES_NOT_COVER_PRINCIPAL';
    end if;
    v_weeks:=coalesce(p_week_count,v_required_weeks::bigint);
  end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  if not exists(select 1 from public.tms_users u where u.id=p_actor_user_id
    and u.is_active is true and u.role::text='admin' for share) then
    raise exception using errcode='42501',message='BPAY_NEXT_CASE_CREATE_ACTOR_INVALID';
  end if;
  -- Protect the source Candidate while the exact command guard serialises
  -- duplicate receipts. Replay uses its receipt even after channel changes.
  perform 1 from public.candidates c where c.id=p_candidate_id for share;
  if not found then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_CREATE_CANDIDATE_MISSING';
  end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('bpay-next-case-create:'||p_command_id::text,0));
  select r.* into v_prior from private.bpay_next_case_create_request r where r.command_id=p_command_id;
  if found then
    if v_prior.candidate_id<>p_candidate_id or v_prior.actor_user_id<>p_actor_user_id
       or v_prior.case_kind<>p_case_kind or v_prior.case_subtype<>p_case_subtype
       or v_prior.tax_treatment<>p_tax_treatment or v_prior.principal_source_ex_vat<>p_principal
       or v_prior.due_date is distinct from p_due_date or v_prior.input_start_monday is distinct from p_start_monday
       or v_prior.input_weekly_due is distinct from p_weekly_due or v_prior.input_week_count is distinct from p_week_count
       or v_prior.minimum_earnings_threshold is distinct from p_minimum_earnings_threshold
       or v_prior.take_home_floor_override is distinct from p_take_home_floor_override
       or v_prior.approval_reason<>p_reason then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_CREATE_REPLAY_CONFLICT';
    end if;
    select c.agency_sequence into strict v_sequence from private.bpay_next_command c
      where c.id=p_command_id and c.command_kind='CASE_CREATE' and c.module_epoch=v_epoch;
    return pg_catalog.jsonb_build_object('case_id',v_prior.case_id,'command_id',p_command_id,
      'sequence',v_sequence::text,'phase','ACCEPTED_PENDING_CASE','replay',true);
  end if;
  select upper(c.pay_method::text),c.active into strict v_source_channel,v_candidate_active
    from public.candidates c where c.id=p_candidate_id;
  if v_candidate_active is not true then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_CREATE_CANDIDATE_NOT_ACTIVE';
  end if;
  if v_source_channel is null or v_source_channel not in ('PAYE','UMBRELLA') then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_CREATE_SOURCE_CHANNEL_UNBOUND';
  end if;
  v_policy:=private.bpay_next_current_valuation_policy_id_v1();
  v_at:=pg_catalog.transaction_timestamp();
  v_week:=(v_at at time zone 'Europe/London')::date;
  v_week:=v_week-(extract(isodow from v_week)::integer-1);
  if p_case_kind in ('LOAN','ADVANCE') then v_recovery_id:=pg_catalog.gen_random_uuid();end if;
  -- The existing short agency clock is LAST in this receipt lock chain.
  v_sequence:=private.bpay_next_receive_command_v1(p_command_id,'CASE_CREATE');
  insert into private.bpay_next_command_member(command_id,candidate_id,member_no)
    values(p_command_id,p_candidate_id,1);
  insert into private.bpay_next_case_create_request
    (command_id,candidate_id,actor_user_id,case_id,rule_id,primary_component_id,recovery_component_id,
     case_kind,case_subtype,tax_treatment,principal_source_ex_vat,source_pay_channel,currency,valuation_policy_id,
     due_date,input_start_monday,input_weekly_due,input_week_count,weekly_due_source_ex_vat,schedule_week_count,
     minimum_earnings_threshold,take_home_floor_override,approval_reason,accepted_at_utc,opening_pay_week_start)
    values(p_command_id,p_candidate_id,p_actor_user_id,p_command_id,p_command_id,p_command_id,v_recovery_id,
     p_case_kind,p_case_subtype,p_tax_treatment,p_principal,v_source_channel,'GBP',v_policy,
     p_due_date,p_start_monday,p_weekly_due,p_week_count,v_weekly,v_weeks,
     p_minimum_earnings_threshold,p_take_home_floor_override,p_reason,v_at,v_week);
  update private.bpay_next_command set expected_member_count=1,status='SEALED',sealed_at_utc=v_at
    where id=p_command_id and status='RECEIVED';
  if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_CREATE_COMMAND_INVALID';end if;
  return pg_catalog.jsonb_build_object('case_id',p_command_id,'command_id',p_command_id,
    'sequence',v_sequence::text,'phase','ACCEPTED_PENDING_CASE','replay',false);
end
$function$;

create or replace function private.bpay_next_claim_case_create_job_v1(
  p_job_id uuid,p_lease_seconds integer default 120
) returns table(lease_nonce uuid,owner_epoch bigint)
language plpgsql security invoker
set search_path=pg_catalog,private
as $function$
declare v_epoch bigint;v_candidate uuid;v_job private.bpay_next_job%rowtype;v_owner bigint;v_nonce uuid;
begin
  if p_job_id is null or p_lease_seconds is null or p_lease_seconds not between 1 and 120 then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_CREATE_LEASE_INPUT_INVALID';
  end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select j.candidate_id into strict v_candidate from private.bpay_next_job j where j.id=p_job_id;
  insert into private.bpay_next_worker_control(candidate_id) values(v_candidate) on conflict(candidate_id) do nothing;
  perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id for update;
  if v_job.module_epoch<>v_epoch or v_job.job_kind<>'CASE_CREATE'
     or v_job.status not in ('READY','LEASED') or v_job.phase<>'NEW'
     or (v_job.status='LEASED' and v_job.lease_until_utc>pg_catalog.clock_timestamp())
     or exists(select 1 from private.bpay_next_job e where e.candidate_id=v_candidate
       and e.command_sequence<v_job.command_sequence and e.status<>'DONE') then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_CREATE_JOB_NOT_CLAIMABLE';
  end if;
  if not exists(select 1 from private.bpay_next_case_create_request r where r.command_id=v_job.command_id
      and r.candidate_id=v_candidate) then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_CREATE_REQUEST_MISSING';
  end if;
  update private.bpay_next_worker_control set active_owner_epoch=active_owner_epoch+1,
    updated_at_utc=pg_catalog.transaction_timestamp() where candidate_id=v_candidate returning active_owner_epoch into v_owner;
  v_nonce:=pg_catalog.gen_random_uuid();
  update private.bpay_next_job set status='LEASED',owner_epoch=v_owner,lease_nonce=v_nonce,
    lease_until_utc=pg_catalog.clock_timestamp()+pg_catalog.make_interval(secs=>p_lease_seconds),attempt_count=attempt_count+1
    where id=p_job_id;
  lease_nonce:=v_nonce;owner_epoch:=v_owner;return next;
end
$function$;

create or replace function private.bpay_next_apply_case_create_v1(p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint)
returns jsonb
language plpgsql security invoker
set search_path=pg_catalog,private
as $function$
declare
  v_epoch bigint;v_candidate uuid;v_control private.bpay_next_worker_control%rowtype;
  v_job private.bpay_next_job%rowtype;v_request private.bpay_next_case_create_request%rowtype;
  v_kind text;v_direction text;v_stage text;v_period date;v_opening_due numeric;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_CREATE_APPLY_INPUT_INVALID';
  end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select j.candidate_id into strict v_candidate from private.bpay_next_job j where j.id=p_job_id;
  select w.* into strict v_control from private.bpay_next_worker_control w where w.candidate_id=v_candidate for update;
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id for update;
  select r.* into strict v_request from private.bpay_next_case_create_request r
    where r.command_id=v_job.command_id and r.candidate_id=v_candidate;
  if v_job.job_kind<>'CASE_CREATE' or v_job.module_epoch<>v_epoch then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_CREATE_JOB_SCOPE_INVALID';
  end if;
  if v_job.status='DONE' and v_job.phase='DONE' then
    if not exists(select 1 from private.bpay_next_case_event e where e.id=v_request.command_id
      and e.case_id=v_request.case_id and e.event_kind='APPROVED'
      and e.approved_delta=v_request.principal_source_ex_vat) then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_CREATE_TERMINAL_EVIDENCE_MISSING';
    end if;
    return pg_catalog.jsonb_build_object('phase','CASE_CREATED','case_id',v_request.case_id,'replay',true);
  end if;
  if v_job.status<>'LEASED' or v_job.phase<>'NEW' or v_job.lease_nonce<>p_lease_nonce
     or v_job.owner_epoch<>p_owner_epoch or v_control.active_owner_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp() then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_CREATE_LEASE_STALE';
  end if;
  insert into private.bpay_next_finance_case(id,candidate_id,case_kind,tax_treatment,status,principal_approved,due_date,order_key,case_revision)
    values(v_request.case_id,v_candidate,v_request.case_kind,v_request.tax_treatment,'OPEN',v_request.principal_source_ex_vat,
      v_request.due_date,pg_catalog.floor(extract(epoch from v_request.accepted_at_utc)*1000000)::bigint,1);
  insert into private.bpay_next_case_rule
    (id,case_id,candidate_id,rule_revision,case_kind,case_subtype,tax_treatment,case_created_at_utc,
     minimum_earnings_threshold,take_home_floor_override,weekly_due_source_ex_vat,schedule_start_monday,next_due_monday,schedule_week_count)
    values(v_request.rule_id,v_request.case_id,v_candidate,1,v_request.case_kind,v_request.case_subtype,v_request.tax_treatment,
      v_request.accepted_at_utc,v_request.minimum_earnings_threshold,v_request.take_home_floor_override,v_request.weekly_due_source_ex_vat,
      v_request.input_start_monday,v_request.input_start_monday,v_request.schedule_week_count);
  update private.bpay_next_finance_case set current_rule_id=v_request.rule_id,current_rule_revision=1 where id=v_request.case_id;
  if v_request.case_kind in ('LOAN','ADVANCE') then v_kind:='PAYOUT';v_direction:='PAYMENT';v_stage:='NET_ADD';
  elsif v_request.case_kind='CREDIT' then v_kind:='CREDIT';v_direction:='PAYMENT';v_stage:=case when v_request.tax_treatment='TAXABLE' then 'GROSS_ADD' else 'NET_ADD' end;
  else v_kind:='RECOVERY';v_direction:='DEDUCTION';v_stage:=case when v_request.tax_treatment='TAXABLE' then 'GROSS_DEDUCT' else 'NET_DEDUCT' end;end if;
  insert into private.bpay_next_case_component
    (id,case_id,candidate_id,component_key,component_ordinal,component_revision,rule_id,case_kind,case_subtype,tax_treatment,
     instruction_kind,direction,payroll_stage,source_pay_channel,currency,approved_source_ex_vat,resolution_state)
    values(v_request.primary_component_id,v_request.case_id,v_candidate,v_kind,1,1,v_request.rule_id,v_request.case_kind,
      v_request.case_subtype,v_request.tax_treatment,v_kind,v_direction,v_stage,v_request.source_pay_channel,'GBP',v_request.principal_source_ex_vat,'REVIEW');
  v_period:=case when v_request.case_kind='MANUAL_DEBT' then v_request.input_start_monday else v_request.opening_pay_week_start end;
  v_opening_due:=case when v_request.case_kind='MANUAL_DEBT' then least(v_request.weekly_due_source_ex_vat,v_request.principal_source_ex_vat) else 0 end;
  insert into private.bpay_next_case_period
    (case_component_id,case_id,candidate_id,pay_week_start,rule_id,period_revision,opening_outstanding_source_ex_vat,opening_due_source_ex_vat)
    values(v_request.primary_component_id,v_request.case_id,v_candidate,v_period,v_request.rule_id,1,v_request.principal_source_ex_vat,v_opening_due);
  if v_request.recovery_component_id is not null then
    -- Two instructions describe ONE principal. Neither funded balance is
    -- effective until the original payout settlement owner records FUNDED.
    insert into private.bpay_next_case_component
      (id,case_id,candidate_id,component_key,component_ordinal,component_revision,rule_id,case_kind,case_subtype,tax_treatment,
       instruction_kind,direction,payroll_stage,source_pay_channel,currency,approved_source_ex_vat,resolution_state)
      values(v_request.recovery_component_id,v_request.case_id,v_candidate,'RECOVERY',2,1,v_request.rule_id,v_request.case_kind,
        v_request.case_subtype,v_request.tax_treatment,'RECOVERY','DEDUCTION','NET_DEDUCT',v_request.source_pay_channel,'GBP',v_request.principal_source_ex_vat,'REVIEW');
    insert into private.bpay_next_case_period
      (case_component_id,case_id,candidate_id,pay_week_start,rule_id,period_revision,opening_outstanding_source_ex_vat,opening_due_source_ex_vat)
      values(v_request.recovery_component_id,v_request.case_id,v_candidate,v_request.input_start_monday,v_request.rule_id,1,0,0);
  end if;
  insert into private.bpay_next_case_event(id,case_id,operation_id,operation_item_id,event_kind,approved_delta,occurred_at_utc)
    values(v_request.command_id,v_request.case_id,v_request.command_id,v_request.primary_component_id,'APPROVED',v_request.principal_source_ex_vat,v_request.accepted_at_utc);
  update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1,
    updated_at_utc=pg_catalog.transaction_timestamp() where candidate_id=v_candidate;
  update private.bpay_next_job set status='DONE',phase='DONE',lease_nonce=null,lease_until_utc=null where id=p_job_id;
  update private.bpay_next_command set status='COMPLETE' where id=v_job.command_id;
  return pg_catalog.jsonb_build_object('phase','CASE_CREATED','case_id',v_request.case_id,'replay',false);
end
$function$;

create or replace function private.bpay_next_append_case_selection_page_v1(
  p_run_id uuid,p_candidate_id uuid,p_selection_revision bigint,p_page_no bigint,p_request_id uuid,
  p_component_ids uuid[],p_selected boolean[]
) returns jsonb
language plpgsql security invoker
set search_path=pg_catalog,private
as $function$
declare
  v_run private.bpay_next_pay_run%rowtype;v_header private.bpay_next_case_selection%rowtype;
  v_page private.bpay_next_case_selection_page%rowtype;v_n integer;v_yes integer;v_i integer;v_case uuid;v_inserted integer;
begin
  v_n:=pg_catalog.cardinality(p_component_ids);
  if p_run_id is null or p_candidate_id is null or p_selection_revision is null or p_selection_revision<1
     or p_page_no is null or p_page_no<1 or p_request_id is null
     or p_component_ids is null or p_selected is null or v_n not between 1 and 100
     or pg_catalog.cardinality(p_selected)<>v_n or pg_catalog.array_ndims(p_component_ids)<>1 or pg_catalog.array_ndims(p_selected)<>1
     or pg_catalog.array_lower(p_component_ids,1)<>1 or pg_catalog.array_lower(p_selected,1)<>1 then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_SELECTION_INPUT_INVALID';
  end if;
  if pg_catalog.array_position(p_component_ids,null) is not null or pg_catalog.array_position(p_selected,null) is not null then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_SELECTION_INPUT_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select r.* into strict v_run from private.bpay_next_pay_run r where r.id=p_run_id for update;
  select p.* into v_page from private.bpay_next_case_selection_page p
    where p.run_id=p_run_id and p.candidate_id=p_candidate_id and p.selection_revision=p_selection_revision and p.page_no=p_page_no;
  if found then
    if v_page.request_id<>p_request_id or v_page.item_count<>v_n
       or exists(select 1 from private.bpay_next_case_selection_item i where i.run_id=p_run_id and i.candidate_id=p_candidate_id
         and i.selection_revision=p_selection_revision and i.page_no=p_page_no
         and (i.case_component_id is distinct from p_component_ids[i.item_no] or i.is_selected is distinct from p_selected[i.item_no])) then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_SELECTION_REPLAY_CONFLICT';
    end if;
    return pg_catalog.jsonb_build_object('page_no',p_page_no::text,'item_count',v_n::text,'replay',true);
  end if;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'OPEN' then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_SELECTION_NOT_OPEN';
  end if;
  select h.* into v_header from private.bpay_next_case_selection h
    where h.run_id=p_run_id and h.candidate_id=p_candidate_id and h.selection_revision=p_selection_revision for update;
  if not found then
    if p_page_no<>1 or exists(select 1 from private.bpay_next_case_selection h where h.run_id=p_run_id and h.candidate_id=p_candidate_id) then
      raise exception using errcode='55000',message='BPAY_NEXT_CASE_SELECTION_REVISION_NOT_INITIAL';
    end if;
    -- Exclusion-only manifests may accompany an already selected WORK worker,
    -- but must not invent a case-only selected Candidate with no selected case.
    if not (true=any(p_selected)) and not exists(select 1 from private.bpay_next_selection_candidate
      where run_id=p_run_id and candidate_id=p_candidate_id) then
      raise exception using errcode='22023',message='BPAY_NEXT_CASE_SELECTION_EMPTY_CANDIDATE';
    end if;
    insert into private.bpay_next_selection_candidate(run_id,candidate_id,member_no)
      values(p_run_id,p_candidate_id,v_run.selected_candidate_count+1) on conflict(run_id,candidate_id) do nothing;
    get diagnostics v_inserted=row_count;
    if v_inserted=1 then update private.bpay_next_pay_run set selected_candidate_count=selected_candidate_count+1 where id=p_run_id;end if;
    insert into private.bpay_next_case_selection(run_id,candidate_id,selection_revision,status)
      values(p_run_id,p_candidate_id,p_selection_revision,'OPEN') returning * into v_header;
  end if;
  if v_header.status<>'OPEN' or p_page_no<>v_header.page_count+1 then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_SELECTION_PAGE_NOT_APPENDABLE';
  end if;
  select count(*)::integer into v_yes from pg_catalog.unnest(p_selected) s(value) where s.value;
  insert into private.bpay_next_case_selection_page
    (run_id,candidate_id,selection_revision,page_no,request_id,first_selection_no,item_count,selected_count,excluded_count)
    values(p_run_id,p_candidate_id,p_selection_revision,p_page_no,p_request_id,v_header.item_count+1,v_n,v_yes,v_n-v_yes);
  for v_i in 1..v_n loop
    select c.case_id into strict v_case from private.bpay_next_case_component c
      where c.id=p_component_ids[v_i] and c.candidate_id=p_candidate_id;
    insert into private.bpay_next_case_selection_item
      (run_id,candidate_id,selection_revision,page_no,item_no,selection_no,case_component_id,case_id,is_selected)
      values(p_run_id,p_candidate_id,p_selection_revision,p_page_no,v_i,v_header.item_count+v_i,p_component_ids[v_i],v_case,p_selected[v_i]);
  end loop;
  update private.bpay_next_case_selection set page_count=p_page_no,item_count=item_count+v_n,
    selected_count=selected_count+v_yes,excluded_count=excluded_count+(v_n-v_yes)
    where run_id=p_run_id and candidate_id=p_candidate_id and selection_revision=p_selection_revision;
  return pg_catalog.jsonb_build_object('page_no',p_page_no::text,'item_count',v_n::text,
    'selected_count',v_yes::text,'excluded_count',(v_n-v_yes)::text,'replay',false);
end
$function$;

create or replace function private.bpay_next_seal_case_selection_v1(
  p_run_id uuid,p_candidate_id uuid,p_selection_revision bigint,p_expected_pages bigint,p_expected_items bigint
) returns jsonb
language plpgsql security invoker
set search_path=pg_catalog,private
as $function$
declare v_run private.bpay_next_pay_run%rowtype;v_header private.bpay_next_case_selection%rowtype;
begin
  if p_run_id is null or p_candidate_id is null or p_selection_revision is null or p_selection_revision<1
     or p_expected_pages is null or p_expected_pages<1 or p_expected_items is null or p_expected_items<1 then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_SELECTION_SEAL_INPUT_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select r.* into strict v_run from private.bpay_next_pay_run r where r.id=p_run_id for update;
  select h.* into strict v_header from private.bpay_next_case_selection h
    where h.run_id=p_run_id and h.candidate_id=p_candidate_id and h.selection_revision=p_selection_revision for update;
  if v_header.page_count<>p_expected_pages or v_header.item_count<>p_expected_items then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_SELECTION_SEAL_COUNT_MISMATCH';
  end if;
  if v_header.status='SEALED' then
    return pg_catalog.jsonb_build_object('sealed',true,'selection_revision',p_selection_revision::text,
      'selected_count',v_header.selected_count::text,'excluded_count',v_header.excluded_count::text,'replay',true);
  end if;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'OPEN' then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_SELECTION_SEAL_NOT_OPEN';
  end if;
  -- Counts were advanced by each complete bounded page. 0310 seal guards verify
  -- that append-only children match them; no growing final all-case serialisation.
  update private.bpay_next_case_selection set status='SEALED',sealed_at_utc=pg_catalog.transaction_timestamp()
    where run_id=p_run_id and candidate_id=p_candidate_id and selection_revision=p_selection_revision;
  -- Exact first-seal receipt only: replay returned above. This gives the
  -- all-case/no-WORK root a constant-time admission proof, not a final scan.
  if v_header.selected_count>0 then
    update private.bpay_next_pay_run set sealed_case_candidate_count=sealed_case_candidate_count+1
      where id=p_run_id;
  end if;
  return pg_catalog.jsonb_build_object('sealed',true,'selection_revision',p_selection_revision::text,
    'selected_count',v_header.selected_count::text,'excluded_count',v_header.excluded_count::text,'replay',false);
end
$function$;

alter function private.bpay_next_accept_case_create_v1(uuid,uuid,uuid,text,text,text,numeric,date,date,numeric,bigint,numeric,numeric,text) owner to postgres;
alter function private.bpay_next_claim_case_create_job_v1(uuid,integer) owner to postgres;
alter function private.bpay_next_apply_case_create_v1(uuid,uuid,bigint) owner to postgres;
alter function private.bpay_next_append_case_selection_page_v1(uuid,uuid,bigint,bigint,uuid,uuid[],boolean[]) owner to postgres;
alter function private.bpay_next_seal_case_selection_v1(uuid,uuid,bigint,bigint,bigint) owner to postgres;
revoke all on function
  private.bpay_next_accept_case_create_v1(uuid,uuid,uuid,text,text,text,numeric,date,date,numeric,bigint,numeric,numeric,text),
  private.bpay_next_claim_case_create_job_v1(uuid,integer),
  private.bpay_next_apply_case_create_v1(uuid,uuid,bigint),
  private.bpay_next_append_case_selection_page_v1(uuid,uuid,bigint,bigint,uuid,uuid[],boolean[]),
  private.bpay_next_seal_case_selection_v1(uuid,uuid,bigint,bigint,bigint)
  from public,anon,authenticated,service_role;

commit;
