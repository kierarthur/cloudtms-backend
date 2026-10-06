-- Pre-Draft valuation of one approved SOURCE amount. The policy ID comes from
-- its immutable approved revision; no current settings or old Timesheet reads.
\set ON_ERROR_STOP on

begin;

create or replace function private.bpay_next_value_source_for_target_v1(
  p_source_ex_vat numeric,
  p_source_pay_channel text,
  p_target_pay_channel text,
  p_umbrella_vat_chargeable boolean,
  p_valuation_policy_id uuid,
  p_pay_date date
) returns table(
  policy_window_id uuid,
  target_ex_vat numeric,
  target_vat numeric,
  target_inc_vat numeric
)
language plpgsql
stable
security definer
set search_path = pg_catalog, private
as $function$
declare
  v_erni_pct numeric;
  v_vat_pct numeric;
  v_winning_count bigint;
  v_erni_fraction numeric;
  v_vat_fraction numeric;
begin
  if p_source_ex_vat is null or p_valuation_policy_id is null or p_pay_date is null
     or p_source_pay_channel not in ('PAYE','UMBRELLA')
     or p_target_pay_channel not in ('PAYE','UMBRELLA')
     or p_source_pay_channel is null or p_target_pay_channel is null
     or (p_target_pay_channel='UMBRELLA' and p_umbrella_vat_chargeable is null)
     or p_source_ex_vat <> round(p_source_ex_vat,2) then
    raise exception using errcode='22023', message='BPAY_NEXT_TARGET_VALUATION_INPUT_INVALID';
  end if;

  select w.source_window_id,w.erni_pct,w.vat_rate_pct,
         count(*) over (partition by w.date_from)
    into policy_window_id,v_erni_pct,v_vat_pct,v_winning_count
    from private.bpay_next_valuation_policy_window w
   where w.policy_id=p_valuation_policy_id
     and p_pay_date>=w.date_from
     and p_pay_date<=coalesce(w.date_to,'infinity'::date)
   order by w.date_from desc,w.source_window_id desc
   limit 1;
  if policy_window_id is null then
    raise exception using errcode='23514', message='BPAY_NEXT_TARGET_POLICY_WINDOW_MISSING';
  end if;
  if v_winning_count<>1 then
    raise exception using errcode='23514', message='BPAY_NEXT_TARGET_POLICY_WINDOW_AMBIGUOUS';
  end if;

  -- Retain the existing percentage convention: 15 means 15%; 0.15 also
  -- means 15%. These exact decimal rules are limited to pre-Draft capture.
  v_erni_fraction:=case when v_erni_pct>1 then v_erni_pct/100 else v_erni_pct end;
  v_vat_fraction:=case when v_vat_pct>1 then v_vat_pct/100 else v_vat_pct end;
  if p_source_pay_channel='PAYE' and p_target_pay_channel='UMBRELLA' then
    target_ex_vat:=round(p_source_ex_vat*(1+v_erni_fraction),2);
  elsif p_source_pay_channel='UMBRELLA' and p_target_pay_channel='PAYE' then
    target_ex_vat:=round(p_source_ex_vat/(1+v_erni_fraction),2);
  else
    target_ex_vat:=round(p_source_ex_vat,2);
  end if;
  target_vat:=case
    when p_target_pay_channel='UMBRELLA' and p_umbrella_vat_chargeable
      then round(target_ex_vat*v_vat_fraction,2)
    else 0::numeric end;
  target_inc_vat:=round(target_ex_vat+target_vat,2);
  return next;
end
$function$;

alter function private.bpay_next_value_source_for_target_v1(
  numeric,text,text,boolean,uuid,date) owner to postgres;
revoke all on function private.bpay_next_value_source_for_target_v1(
  numeric,text,text,boolean,uuid,date) from public,anon,authenticated,service_role;

commit;
