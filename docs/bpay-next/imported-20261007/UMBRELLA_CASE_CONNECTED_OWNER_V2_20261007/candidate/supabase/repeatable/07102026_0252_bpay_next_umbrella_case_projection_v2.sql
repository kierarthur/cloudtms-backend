-- Existing projection/weekly owners, genuine NULL Umbrella NET. No fictitious
-- NET request/job/result and no current settings or original-money reconversion.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_umbrella_case_projection_snapshot_valid_v2(
  p_worker private.bpay_next_run_worker,p_projection private.bpay_next_net_projection)
returns boolean language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare d private.bpay_next_case_allocation_state%rowtype;
begin
  if p_worker.target_pay_channel<>'UMBRELLA' or p_worker.target_umbrella_id is null
    or p_worker.target_umbrella_vat_chargeable is null or p_worker.case_selection_revision<1
    or p_worker.entered_paye_net is not null or p_worker.net_request_revision<>0
    or p_projection.input_kind<>'UMBRELLA' or p_projection.entered_paye_net is not null
    or p_projection.request_command_id is not null
    or p_projection.run_worker_id<>p_worker.id or p_projection.umbrella_case_draft_state_id is null
    or p_projection.projection_no<>1 then return false;end if;
  select * into strict d from private.bpay_next_case_allocation_state where id=p_projection.umbrella_case_draft_state_id;
  return d.run_worker_id=p_worker.id and d.candidate_id=p_worker.candidate_id
    and d.preparation_revision=p_worker.preparation_revision and d.selection_revision=p_worker.case_selection_revision
    and d.pass_kind='DRAFT' and d.projection_no=0 and d.status='COMPLETE' and d.prepare_stage='COMPLETE'
    and d.expected_instruction_count=p_worker.case_instruction_count and d.processed_instruction_count=d.expected_instruction_count
    and (p_projection.gross_ex_vat,p_projection.gross_vat,p_projection.gross_inc_vat)
      is not distinct from (d.captured_work_gross+d.allocated_gross_additions-d.allocated_gross_deductions,
        d.captured_work_vat+d.allocated_gross_additions_vat-d.allocated_gross_deductions_vat,
        p_worker.gross_inc_vat)
    and (p_projection.gross_ex_vat,p_projection.gross_vat) is not distinct from (p_worker.gross_ex_vat,p_worker.gross_vat)
    and (p_projection.accepted_gross_additions,p_projection.accepted_gross_deductions,
      p_projection.accepted_net_additions,p_projection.accepted_recoveries)
      is not distinct from (d.allocated_gross_additions+d.allocated_gross_additions_vat,
        d.allocated_gross_deductions+d.allocated_gross_deductions_vat,
        d.allocated_net_additions+d.allocated_net_additions_vat,
        d.allocated_net_recoveries+d.allocated_net_recoveries_vat)
    and p_projection.cash_amount=p_projection.gross_inc_vat+p_projection.accepted_net_additions-p_projection.accepted_recoveries;
end $function$;

create or replace function private.bpay_next_umbrella_case_projection_valid_v2(
  p_worker private.bpay_next_run_worker,p_projection private.bpay_next_net_projection)
returns boolean language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
begin
  if p_projection.retired_at_utc is not null then return false;end if;
  return private.bpay_next_umbrella_case_projection_snapshot_valid_v2(p_worker,p_projection);
end $function$;

create or replace function private.bpay_next_capture_umbrella_case_projection_v2(p_worker_id uuid)
returns void language plpgsql volatile security invoker set search_path=pg_catalog,private
as $function$
declare w private.bpay_next_run_worker%rowtype;r private.bpay_next_pay_run%rowtype;
  d private.bpay_next_case_allocation_state%rowtype;p private.bpay_next_net_projection%rowtype;
  prior private.bpay_next_worker_week_contribution%rowtype;week private.bpay_next_worker_week%rowtype;
  earned numeric;cash numeric;
begin
  select * into strict w from private.bpay_next_run_worker where id=p_worker_id for update;
  select * into strict r from private.bpay_next_pay_run where id=w.run_id;
  if r.status<>'DRAFT' or r.confirmed_at_utc is null or w.status<>'READY'
    or w.target_pay_channel<>'UMBRELLA' or w.target_umbrella_id is null or w.target_umbrella_vat_chargeable is null
    or w.case_selection_revision<1 or w.case_pending_binding_count<>0 or w.financial_resolution_count<>0
    or w.entered_paye_net is not null or w.net_request_revision<>0 then
    raise exception 'BPAY_NEXT_UMBRELLA_CASE_PROJECTION_NOT_ELIGIBLE' using errcode='55000';end if;
  if w.net_projection_revision<>0 then
    select * into strict p from private.bpay_next_net_projection where run_worker_id=w.id and projection_no=w.net_projection_revision;
    if not private.bpay_next_umbrella_case_projection_valid_v2(w,p) then
      raise exception 'BPAY_NEXT_UMBRELLA_CASE_PROJECTION_REPLAY_INVALID' using errcode='23514';end if;
    return;
  end if;
  select * into strict d from private.bpay_next_case_allocation_state where run_worker_id=w.id
    and preparation_revision=w.preparation_revision and selection_revision=w.case_selection_revision
    and pass_kind='DRAFT' and projection_no=0 and status='COMPLETE' and prepare_stage='COMPLETE';
  cash:=w.gross_inc_vat+d.allocated_net_additions+d.allocated_net_additions_vat
    -d.allocated_net_recoveries-d.allocated_net_recoveries_vat;
  earned:=w.gross_ex_vat-d.allocated_net_recoveries;
  if cash<0 or cash>=10000000000000000 or earned<0 then
    raise exception 'BPAY_NEXT_UMBRELLA_CASE_CASH_RANGE_INVALID' using errcode='23514';end if;
  insert into private.bpay_next_net_projection(run_worker_id,request_command_id,projection_no,input_kind,
    gross_ex_vat,gross_vat,gross_inc_vat,entered_paye_net,accepted_recoveries,cash_amount,
    accepted_net_additions,accepted_gross_additions,accepted_gross_deductions,umbrella_case_draft_state_id)
    values(w.id,null,1,'UMBRELLA',w.gross_ex_vat,w.gross_vat,w.gross_inc_vat,null,
      d.allocated_net_recoveries+d.allocated_net_recoveries_vat,cash,d.allocated_net_additions+d.allocated_net_additions_vat,
      d.allocated_gross_additions+d.allocated_gross_additions_vat,d.allocated_gross_deductions+d.allocated_gross_deductions_vat,d.id)
    returning * into p;
  if not private.bpay_next_umbrella_case_projection_valid_v2(w,p) then
    raise exception 'BPAY_NEXT_UMBRELLA_CASE_PROJECTION_INVALID' using errcode='23514';end if;
  select * into strict week from private.bpay_next_worker_week where candidate_id=w.candidate_id and pay_week_start=d.pay_week_start for update;
  select * into strict prior from private.bpay_next_worker_week_contribution where original_run_worker_id=w.id for update;
  if prior.payment_state<>'ARRANGED' or prior.basis_kind<>'GROSS_FALLBACK' or prior.original_payroll_net is not null
    or prior.original_gross_amount<>w.gross_ex_vat or prior.accepted_projection_id is not null
    or prior.original_transfer_id is not null or prior.eligibility_state='UNBOUND' then
    raise exception 'BPAY_NEXT_UMBRELLA_CASE_WEEK_NOT_EXACT' using errcode='23514';end if;
  update private.bpay_next_worker_week_contribution set accepted_projection_id=p.id,accepted_bank_cash=cash,
    eligibility_state=case when earned>0 then 'ELIGIBLE' else 'EXCLUDED' end,eligible_arranged_amount=earned,
    contribution_revision=contribution_revision+1,primary_binding_revision=primary_binding_revision+1 where id=prior.id;
  update private.bpay_next_worker_week set resolved_arranged_take_home=resolved_arranged_take_home-prior.eligible_arranged_amount+earned,
    period_revision=period_revision+1 where candidate_id=w.candidate_id and pay_week_start=d.pay_week_start;
  update private.bpay_next_run_worker set net_projection_revision=1 where id=w.id;
end $function$;

create or replace function private.bpay_next_umbrella_case_projection_insert_guard_v2()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare w private.bpay_next_run_worker%rowtype;
begin
  if new.umbrella_case_draft_state_id is null then return new;end if;
  select * into strict w from private.bpay_next_run_worker where id=new.run_worker_id;
  if w.status<>'READY' or w.net_projection_revision<>0
    or not private.bpay_next_umbrella_case_projection_valid_v2(w,new)
    or not exists(select 1 from private.bpay_next_pay_run where id=w.run_id and status='DRAFT' and confirmed_at_utc is not null) then
    raise exception 'BPAY_NEXT_UMBRELLA_CASE_PROJECTION_INSERT_INVALID' using errcode='23514';end if;
  return new;
end $function$;
drop trigger if exists bpay_next_umbrella_case_projection_insert_guard_v2 on private.bpay_next_net_projection;
create trigger bpay_next_umbrella_case_projection_insert_guard_v2 before insert on private.bpay_next_net_projection
  for each row execute function private.bpay_next_umbrella_case_projection_insert_guard_v2();
alter function private.bpay_next_umbrella_case_projection_snapshot_valid_v2(private.bpay_next_run_worker,private.bpay_next_net_projection) owner to postgres;
alter function private.bpay_next_umbrella_case_projection_valid_v2(private.bpay_next_run_worker,private.bpay_next_net_projection) owner to postgres;
alter function private.bpay_next_capture_umbrella_case_projection_v2(uuid) owner to postgres;
alter function private.bpay_next_umbrella_case_projection_insert_guard_v2() owner to postgres;
revoke all on function private.bpay_next_umbrella_case_projection_snapshot_valid_v2(private.bpay_next_run_worker,private.bpay_next_net_projection),
  private.bpay_next_umbrella_case_projection_valid_v2(private.bpay_next_run_worker,private.bpay_next_net_projection),
  private.bpay_next_capture_umbrella_case_projection_v2(uuid),private.bpay_next_umbrella_case_projection_insert_guard_v2()
  from public,anon,authenticated,service_role;
commit;
