-- Turn one contract/financial-row rate snapshot into small typed rows. The
-- caller reads the contract once and freezes these rows on the approved
-- revision; this function does not inspect other Timesheets or work history.
\set ON_ERROR_STOP on

begin;

create or replace function private.bpay_next_exact_rate_v1(p_raw text)
returns numeric
language plpgsql immutable strict
set search_path = pg_catalog
as $function$
declare
  v_rate numeric;
begin
  if p_raw !~ '^[0-9]+(\.[0-9]+)?$' then
    raise exception using errcode='22023', message='BPAY_NEXT_RATE_INVALID';
  end if;
  v_rate:=p_raw::numeric;
  if v_rate<>round(v_rate,6) or v_rate>=1000000000000 then
    raise exception using errcode='22003', message='BPAY_NEXT_RATE_PRECISION_UNSUPPORTED';
  end if;
  return v_rate;
end
$function$;

create or replace function private.bpay_next_contract_rate_rows_v1(
  p_rates jsonb, p_bucket_labels jsonb, p_additional_rates jsonb,
  p_mileage_pay_rate numeric, p_mileage_charge_rate numeric
)
returns table(rate_family text,rate_code text,unit_label text,
              paye_rate numeric,umbrella_rate numeric,charge_rate numeric)
language plpgsql immutable
set search_path = pg_catalog, private
as $function$
declare
  v_additional jsonb;
  v_code text;
  v_label text;
  v_pay numeric;
  v_charge numeric;
  v_seen text[]:='{}'::text[];
  v_bucket record;
begin
  if p_rates is null or jsonb_typeof(p_rates)<>'object'
     or (p_bucket_labels is not null and jsonb_typeof(p_bucket_labels)<>'object')
     or (p_additional_rates is not null
         and jsonb_typeof(p_additional_rates)<>'array') then
    raise exception using errcode='22023', message='BPAY_NEXT_CONTRACT_RATE_SHAPE_INVALID';
  end if;
  for v_bucket in
    select * from (values
      ('DAY'::text,'day'::text),('NIGHT','night'),('SAT','sat'),
      ('SUN','sun'),('BH','bh')) bucket(code,key_name)
  loop
    rate_family:='STANDARD';
    rate_code:=v_bucket.code;
    unit_label:=nullif(btrim(p_bucket_labels->>v_bucket.key_name),'');
    paye_rate:=private.bpay_next_exact_rate_v1(nullif(p_rates->>('paye_'||v_bucket.key_name),''));
    umbrella_rate:=private.bpay_next_exact_rate_v1(nullif(p_rates->>('umb_'||v_bucket.key_name),''));
    charge_rate:=private.bpay_next_exact_rate_v1(nullif(p_rates->>('charge_'||v_bucket.key_name),''));
    if paye_rate is not null or umbrella_rate is not null or charge_rate is not null then
      return next;
    end if;
  end loop;

  for v_additional in
    select value from jsonb_array_elements(coalesce(p_additional_rates,'[]'::jsonb))
  loop
    if jsonb_typeof(v_additional)<>'object' then
      raise exception using errcode='22023', message='BPAY_NEXT_ADDITIONAL_RATE_SHAPE_INVALID';
    end if;
    v_code:=upper(btrim(coalesce(v_additional->>'code','')));
    if v_code !~ '^EX[1-5]$' or v_code=any(v_seen) then
      raise exception using errcode='22023', message='BPAY_NEXT_ADDITIONAL_RATE_CODE_INVALID';
    end if;
    v_seen:=array_append(v_seen,v_code);
    v_label:=nullif(btrim(coalesce(v_additional->>'unit_name',
                                  v_additional->>'bucket_name','')),'');
    v_pay:=private.bpay_next_exact_rate_v1(nullif(v_additional->>'pay_rate',''));
    v_charge:=private.bpay_next_exact_rate_v1(nullif(v_additional->>'charge_rate',''));
    if v_pay is not null or v_charge is not null then
      rate_family:='ADDITIONAL'; rate_code:=v_code; unit_label:=v_label;
      -- The current contract format has one additional-unit pay rate,
      -- shared by PAYE and umbrella; it has no separate channel keys.
      paye_rate:=v_pay; umbrella_rate:=v_pay; charge_rate:=v_charge;
      return next;
    end if;
  end loop;

  if p_mileage_pay_rate is not null or p_mileage_charge_rate is not null then
    if p_mileage_pay_rate<0 or p_mileage_charge_rate<0
       or p_mileage_pay_rate is distinct from round(p_mileage_pay_rate,6)
       or p_mileage_charge_rate is distinct from round(p_mileage_charge_rate,6) then
      raise exception using errcode='22003', message='BPAY_NEXT_MILEAGE_RATE_INVALID';
    end if;
    rate_family:='MILEAGE'; rate_code:='MILE'; unit_label:='mile';
    paye_rate:=p_mileage_pay_rate; umbrella_rate:=p_mileage_pay_rate;
    charge_rate:=p_mileage_charge_rate;
    return next;
  end if;
end
$function$;

alter function private.bpay_next_exact_rate_v1(text) owner to postgres;
alter function private.bpay_next_contract_rate_rows_v1(jsonb,jsonb,jsonb,numeric,numeric)
  owner to postgres;
revoke all on function private.bpay_next_exact_rate_v1(text)
  from public, anon, authenticated, service_role;
revoke all on function private.bpay_next_contract_rate_rows_v1(jsonb,jsonb,jsonb,numeric,numeric)
  from public, anon, authenticated, service_role;

commit;
