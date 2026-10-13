-- WB-01 successor of1510: canonical available identities, honest NULL and arbitrary codes.
\set ON_ERROR_STOP on
begin;

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
    -- A canonical unused bucket with unknown rates is a real missing row.
    return next;
  end loop;

  for v_additional in
    select value from jsonb_array_elements(coalesce(p_additional_rates,'[]'::jsonb))
  loop
    if jsonb_typeof(v_additional)<>'object' then
      raise exception using errcode='22023', message='BPAY_NEXT_ADDITIONAL_RATE_SHAPE_INVALID';
    end if;
    v_code:=upper(btrim(coalesce(v_additional->>'code','')));
    if v_code='' or char_length(v_code)>245 or v_code=any(v_seen) then
      raise exception using errcode='22023', message='BPAY_NEXT_ADDITIONAL_RATE_CODE_INVALID';
    end if;
    v_seen:=array_append(v_seen,v_code);
    v_label:=nullif(btrim(coalesce(v_additional->>'unit_name',
                                  v_additional->>'bucket_name','')),'');
    v_pay:=private.bpay_next_exact_rate_v1(nullif(v_additional->>'pay_rate',''));
    v_charge:=private.bpay_next_exact_rate_v1(nullif(v_additional->>'charge_rate',''));
    rate_family:='ADDITIONAL'; rate_code:=v_code; unit_label:=v_label;
    -- The actual Contract has one genuinely shared Additional pay field.
    paye_rate:=v_pay; umbrella_rate:=v_pay; charge_rate:=v_charge;
    return next;
  end loop;

  if p_mileage_pay_rate<0 or p_mileage_charge_rate<0
     or p_mileage_pay_rate>=1000000000000 or p_mileage_charge_rate>=1000000000000
     or p_mileage_pay_rate is distinct from round(p_mileage_pay_rate,6)
     or p_mileage_charge_rate is distinct from round(p_mileage_charge_rate,6) then
    raise exception using errcode='22003', message='BPAY_NEXT_MILEAGE_RATE_INVALID';
  end if;
  rate_family:='MILEAGE'; rate_code:='MILE'; unit_label:='mile';
  paye_rate:=p_mileage_pay_rate; umbrella_rate:=p_mileage_pay_rate;
  charge_rate:=p_mileage_charge_rate;
  return next;
end
$function$;

alter function private.bpay_next_contract_rate_rows_v1(jsonb,jsonb,jsonb,numeric,numeric) owner to postgres;
revoke all on function private.bpay_next_contract_rate_rows_v1(jsonb,jsonb,jsonb,numeric,numeric) from public, anon, authenticated, service_role;
notify pgrst, 'reload schema';
commit;
