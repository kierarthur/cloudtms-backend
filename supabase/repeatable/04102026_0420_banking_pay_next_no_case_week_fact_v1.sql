-- Maintain one exact current weekly arrangement fact at its genuine no-case
-- owner transition. No previous-run SUM, history backfill or financial effect.
-- The joined umbrella/case contract is not inferred by this PAYE-only slice.
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
  v_week:=v_run.pay_date-(extract(isodow from v_run.pay_date)::integer-1);
  select min_take_home_wtd into strict v_floor from public.candidates where id=v_worker.candidate_id;
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
alter function private.bpay_next_publish_no_case_week_fact_v1(uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_publish_no_case_week_fact_v1(uuid,uuid)
  from public,anon,authenticated,service_role;
commit;
