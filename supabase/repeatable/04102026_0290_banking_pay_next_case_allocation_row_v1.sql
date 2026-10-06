-- Exact one-row recovery transition, not a case/due/stage/selection owner.
-- The caller filters exclusions/eligibility BEFORE calling, supplies bound
-- nominal and legitimate capacity, and carries E[channel] and shared H.
-- Preview/Prepare/NET must bind the same inputs; no legacy helper is called.
-- Policy X: a post-Draft caller supplies frozen rules, not live defaults.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_case_allocation_row_v1(
  p_case_kind text,
  p_nominal_due numeric,
  p_usable_capacity numeric,
  p_channel_headroom numeric,
  p_worker_take_home numeric,
  p_minimum_earnings_threshold numeric,
  p_take_home_floor_override numeric,
  p_captured_default_floor numeric
) returns table(
  taken_amount numeric,
  cap_reason text,
  affordability_reason text,
  capacity_reason text,
  next_channel_headroom numeric,
  next_worker_take_home numeric,
  protected_recovery boolean,
  effective_floor numeric,
  threshold_cap numeric,
  floor_cap numeric,
  affordability_cap numeric
)
language plpgsql immutable parallel safe security invoker
set search_path=pg_catalog
as $function$
declare
  v_amount numeric;
begin
  -- Only these already-bound recovery kinds belong to this transition.
  -- Payouts/credits are additions, never negative or fabricated repayments.
  if p_case_kind is null or p_case_kind not in
      ('LOAN','ADVANCE','MANUAL_DEBT','OVERPAYMENT') then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_ROW_KIND_INVALID';
  end if;
  -- A missing required capture is not a zero amount. A binder that resolves
  -- an absent worker default to zero must supply that explicit captured zero.
  if p_nominal_due is null or p_usable_capacity is null
     or p_channel_headroom is null or p_worker_take_home is null
     or p_captured_default_floor is null then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_ROW_AMOUNT_REQUIRED';
  end if;
  -- Seven fixed scalar operands only; no table, history or JSON input.
  -- Numeric typmods on function parameters would not validate callers, and
  -- casting to numeric(...,2) first could silently round a sub-penny input.
  foreach v_amount in array array[
    p_nominal_due,p_usable_capacity,p_channel_headroom,p_worker_take_home,
    p_minimum_earnings_threshold,p_take_home_floor_override,p_captured_default_floor
  ] loop
    if v_amount is not null then
      if v_amount::text in ('NaN','Infinity','-Infinity') then
        raise exception using errcode='22023',message='BPAY_NEXT_CASE_ROW_AMOUNT_NONFINITE';
      end if;
      if v_amount<>pg_catalog.trunc(v_amount,2) then
        raise exception using errcode='22023',message='BPAY_NEXT_CASE_ROW_AMOUNT_SCALE_INVALID';
      end if;
    end if;
  end loop;
  if p_nominal_due<0 or p_usable_capacity<0 or p_channel_headroom<0
     or p_minimum_earnings_threshold<0 or p_take_home_floor_override<0
     or p_captured_default_floor<0 then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_ROW_AMOUNT_NEGATIVE';
  end if;

  protected_recovery:=p_case_kind in ('LOAN','ADVANCE','MANUAL_DEBT');
  if protected_recovery then
    threshold_cap:=case when p_minimum_earnings_threshold is null
      then p_channel_headroom
      else greatest(p_channel_headroom-p_minimum_earnings_threshold,0) end;
    effective_floor:=case when p_take_home_floor_override is null
      then p_captured_default_floor else p_take_home_floor_override end;
    floor_cap:=greatest(p_worker_take_home-effective_floor,0);
    affordability_cap:=least(p_channel_headroom,threshold_cap,floor_cap);
  else
    -- Overpayment ignores protection, but supplied operands still must be
    -- valid. NULL floor_cap/effective_floor mean not applied, not zero floor.
    threshold_cap:=p_channel_headroom;
    floor_cap:=null;
    effective_floor:=null;
    affordability_cap:=p_channel_headroom;
  end if;

  taken_amount:=least(p_nominal_due,p_usable_capacity,affordability_cap);
  next_channel_headroom:=p_channel_headroom-taken_amount;
  -- H is signed carry. An allowed overpayment can take it below zero; do not
  -- clamp it or reject it on the next row/page. Protected floor_cap is zero.
  next_worker_take_home:=p_worker_take_home-taken_amount;

  -- Explain affordability independently of another run's reservation/due
  -- capacity. Preserve the retained floor > threshold > pay tie precedence.
  affordability_reason:=case
    when affordability_cap>=p_nominal_due then null
    when protected_recovery and floor_cap<p_channel_headroom
         and floor_cap<=threshold_cap then 'TAKE_HOME_FLOOR'
    when protected_recovery and threshold_cap<p_channel_headroom then 'EARNINGS_THRESHOLD'
    else 'PAY_HEADROOM' end;
  -- This names any capacity restriction relative to nominal, even when an
  -- equal/stricter affordability limit determines the actual amount taken.
  capacity_reason:=case when p_usable_capacity<p_nominal_due
    then 'CASE_CAPACITY' else null end;
  -- A strictly tighter capacity wins; an exact tie keeps the established
  -- affordability reason. Full nominal recovery has no cap reason.
  cap_reason:=case
    when taken_amount>=p_nominal_due then null
    when p_usable_capacity<least(p_nominal_due,affordability_cap) then 'CASE_CAPACITY'
    else affordability_reason end;
  return next;
end;
$function$;

alter function private.bpay_next_case_allocation_row_v1(
  text,numeric,numeric,numeric,numeric,numeric,numeric,numeric
) owner to postgres;
revoke all on function private.bpay_next_case_allocation_row_v1(
  text,numeric,numeric,numeric,numeric,numeric,numeric,numeric
) from public,anon,authenticated,service_role;
-- Intentionally no EXECUTE grant or public RPC wrapper in this slice.
commit;
