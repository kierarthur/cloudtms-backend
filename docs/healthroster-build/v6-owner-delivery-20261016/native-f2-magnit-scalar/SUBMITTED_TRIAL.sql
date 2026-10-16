set local pg_show_plans.is_enabled=off;
create or replace function private.weekly_source_sha256_text_v1(
  p_domain text,
  p_value text
) returns bytea
language sql immutable
set search_path to 'pg_catalog','extensions','pg_temp'
as $function$
  select extensions.digest(
    pg_catalog.convert_to(
      coalesce(p_domain,'') || pg_catalog.chr(31) || coalesce(p_value,''),
      'UTF8'
    ),
    'sha256'
  );
$function$;
alter function private.weekly_source_sha256_text_v1(text,text) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_sha256_text_v1(text,text)
  from public,anon,authenticated,service_role;

create or replace function private.weekly_source_sha256_jsonb_v1(
  p_domain text,
  p_value jsonb
) returns bytea
language sql immutable
set search_path to 'pg_catalog','private','extensions','pg_temp'
as $function$
  select private.weekly_source_sha256_text_v1(
    p_domain,
    coalesce(p_value,'null'::jsonb)::text
  );
$function$;
alter function private.weekly_source_sha256_jsonb_v1(text,jsonb) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_sha256_jsonb_v1(text,jsonb)
  from public,anon,authenticated,service_role;

create or replace function private.weekly_source_final_value_vector_v1(p_vector jsonb)
returns jsonb
language sql immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select pg_catalog.jsonb_build_object(
    'row_sign',p_vector->'row_sign','paid_minutes',p_vector->'paid_minutes',
    'break_minutes',p_vector->'break_minutes','total_pence',p_vector->'total_pence',
    'hours',pg_catalog.jsonb_build_object(
      'day',p_vector#>'{hours,day}','night',p_vector#>'{hours,night}',
      'sat',p_vector#>'{hours,sat}','sun',p_vector#>'{hours,sun}','bh',p_vector#>'{hours,bh}'
    ),
    'rates',pg_catalog.jsonb_build_object(
      'day',p_vector#>'{rates,day}','night',p_vector#>'{rates,night}',
      'sat',p_vector#>'{rates,sat}','sun',p_vector#>'{rates,sun}','bh',p_vector#>'{rates,bh}'
    )
  );
$function$;
alter function private.weekly_source_final_value_vector_v1(jsonb) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_final_value_vector_v1(jsonb) from public,anon,authenticated,service_role;

create or replace function private.weekly_source_authority_subset_v1(p_policy jsonb)
returns jsonb
language plpgsql immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_subset jsonb;
begin
  v_subset:=pg_catalog.jsonb_build_object(
    'authority_mode',p_policy->'authority_mode',
    'self_bill_enabled',p_policy->'self_bill_enabled',
    'source_group_id',p_policy->'source_group_id',
    'self_bill_correction_presentation',p_policy->'self_bill_correction_presentation'
  );
  if pg_catalog.jsonb_typeof(p_policy) is distinct from 'object'
     or pg_catalog.jsonb_typeof(v_subset->'authority_mode') is distinct from 'string'
     or coalesce(v_subset->>'authority_mode','')=''
     or pg_catalog.jsonb_typeof(v_subset->'self_bill_enabled') is distinct from 'boolean'
     or pg_catalog.jsonb_typeof(v_subset->'source_group_id') is distinct from 'string'
     or coalesce(v_subset->>'source_group_id','') !~
       '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     or coalesce(v_subset->>'self_bill_correction_presentation','') not in (
       'FULL_REVERSAL_REPLACEMENT','NET_DIFFERENCE_PRESENTATION'
     ) then
    raise exception 'WEEKLY_SOURCE_AUTHORITY_SUBSET_INVALID' using errcode='22023';
  end if;
  return v_subset;
end;
$function$;
alter function private.weekly_source_authority_subset_v1(jsonb) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_authority_subset_v1(jsonb) from public,anon,authenticated,service_role;

create or replace function private.weekly_source_final_state_fingerprint_v1(p_state jsonb)
returns bytea
language sql immutable
set search_path to 'private','pg_catalog','pg_temp'
as $function$
  select private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_FINAL_VALUE_STATE_V2',
    pg_catalog.jsonb_build_object(
      'work_event_id',p_state->'work_event_id','candidate_id',p_state->'candidate_id',
      'client_id',p_state->'client_id','contract_id',p_state->'contract_id',
      'work_date',p_state->'work_date','start_at_local',p_state->'start_at_local',
      'end_at_local',p_state->'end_at_local','break_minutes',p_state->'break_minutes',
      'actual_net_minutes',p_state->'actual_net_minutes',
      'pay_vector',private.weekly_source_final_value_vector_v1(p_state->'pay_vector'),
      'charge_vector',private.weekly_source_final_value_vector_v1(p_state->'charge_vector'),
      'invoice_vat_rate_pct',p_state->'invoice_vat_rate_pct',
      'invoice_vat_chargeable',p_state->'invoice_vat_chargeable',
      'authority_subset',p_state->'authority_subset'
    )
  );
$function$;
alter function private.weekly_source_final_state_fingerprint_v1(jsonb) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_final_state_fingerprint_v1(jsonb) from public,anon,authenticated,service_role;

create or replace function private.weekly_source_invoice_economics_fingerprint_v1(
  p_source_money jsonb, p_line_vat_method text, p_vat_rate numeric
) returns bytea
language plpgsql immutable
set search_path to 'private','pg_catalog','pg_temp'
as $function$
declare v_key text; v_money jsonb:='{}'::jsonb; v_amount bigint;
begin
  if pg_catalog.jsonb_typeof(p_source_money) is distinct from 'object'
     or p_line_vat_method not in (
       'ROUND_EACH_LINE_FROM_NET','LINE_GROSS_MINUS_NET_FROM_SOURCE',
       'LINE_VAT_FROM_REPORT','ROUND_TOTAL_FROM_EXACT_NET'
     ) or p_line_vat_method is null or p_vat_rate is null or p_vat_rate<0 or p_vat_rate>100 then
    raise exception 'WEEKLY_SOURCE_INVOICE_ECONOMICS_NOT_READY' using errcode='55000';
  end if;
  foreach v_key in array array['source_shift_charge_pence','source_gross_pence','source_vat_pence']::text[] loop
    if coalesce(p_source_money->>v_key,'') !~ '^[+-]?[0-9]+$' then
      raise exception 'WEEKLY_SOURCE_INVOICE_ECONOMICS_NOT_READY' using errcode='55000';
    end if;
    v_amount:=(p_source_money->>v_key)::bigint;
    if v_amount not between -999999999999 and 999999999999 then
      raise exception 'WEEKLY_SOURCE_INVOICE_ECONOMICS_NOT_READY' using errcode='55000';
    end if;
    v_money:=v_money||pg_catalog.jsonb_build_object(v_key,v_amount);
  end loop;
  return private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_INVOICE_ECONOMICS_V1',
    v_money||pg_catalog.jsonb_build_object('line_vat_method',p_line_vat_method,'vat_rate_pct',p_vat_rate)
  );
exception when invalid_text_representation or numeric_value_out_of_range then
  raise exception 'WEEKLY_SOURCE_INVOICE_ECONOMICS_NOT_READY' using errcode='55000';
end;
$function$;
alter function private.weekly_source_invoice_economics_fingerprint_v1(jsonb,text,numeric) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_invoice_economics_fingerprint_v1(jsonb,text,numeric) from public,anon,authenticated,service_role;

create or replace function private.weekly_source_magnit_bucket_assert_v1(p_rule_values jsonb,p_economic_snapshot jsonb)
returns boolean language plpgsql immutable set search_path to 'pg_catalog','pg_temp'
as $function$
declare used integer; total integer;
begin
  if p_rule_values is null or p_rule_values->>'invoice_sender' is distinct from 'CLIENT_SELF_BILL'
    or p_rule_values->>'checking_profile_kind' is distinct from 'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1' then return false; end if;
  if p_economic_snapshot->>'rate_method' is distinct from 'WHOLE_SHIFT_START_DAY'
    or jsonb_typeof(p_economic_snapshot->'bucket_minutes') is distinct from 'object' then
    raise exception 'WEEKLY_SOURCE_MAGNIT_BUCKET_INVALID' using errcode='22023'; end if;
  select count(*) filter(where (p_economic_snapshot#>>array['bucket_minutes',bucket.name])::integer>0),
    sum((p_economic_snapshot#>>array['bucket_minutes',bucket.name])::integer)
    into used,total from (values('day'),('night'),('sat'),('sun'),('bh')) bucket(name);
  if used<>1 or total is distinct from (p_economic_snapshot->>'paid_minutes')::integer or total<=0 then
    raise exception 'WEEKLY_SOURCE_MAGNIT_BUCKET_INVALID' using errcode='22023'; end if;
  return true;
end;
$function$;
alter function private.weekly_source_magnit_bucket_assert_v1(jsonb,jsonb) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_magnit_bucket_assert_v1(jsonb,jsonb) from public,anon,authenticated,service_role;

create or replace function private.weekly_source_magnit_contract_price_assert_v1(p_rule_values jsonb,p_effective_policy jsonb)
returns boolean language plpgsql immutable set search_path to 'pg_catalog','pg_temp'
as $function$
begin
  if p_rule_values is null or p_rule_values->>'invoice_sender' is distinct from 'CLIENT_SELF_BILL'
    or p_rule_values->>'checking_profile_kind' is distinct from 'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1' then return false; end if;
  if p_rule_values->>'self_bill_source' is distinct from 'SAME_FILE'
    or p_rule_values->>'final_report_profile_kind' is distinct from 'NONE'
    or p_rule_values->>'invoice_membership' is distinct from 'FINALISED_IN_CYCLE_WINDOW'
    or p_rule_values->>'pay_membership' is distinct from 'ALL_FINALISED_IN_FILE'
    or p_rule_values->>'invoice_amount_basis' is distinct from 'CONTRACT_CALCULATION'
    or p_rule_values->>'source_amount_basis' is distinct from 'NONE'
    or p_rule_values->>'rate_difference_policy' is distinct from 'NOT_APPLICABLE'
    or p_rule_values->'earliest_final_weekday' is distinct from 'null'::jsonb
    or p_rule_values->'earliest_final_local_time' is distinct from 'null'::jsonb then
    raise exception 'WEEKLY_SOURCE_MAGNIT_PRESET_INVALID' using errcode='22023'; end if;
  if p_effective_policy->>'weekly_rate_classification_method' is distinct from 'WHOLE_SHIFT_START_DAY' then
    raise exception 'WEEKLY_SOURCE_MAGNIT_WHOLE_SHIFT_REQUIRED' using errcode='22023'; end if;
  return true;
end;
$function$;
alter function private.weekly_source_magnit_contract_price_assert_v1(jsonb,jsonb) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_magnit_contract_price_assert_v1(jsonb,jsonb) from public,anon,authenticated,service_role;

do $proof$
declare
  v_vector jsonb:='{"row_sign":1,"paid_minutes":600,"break_minutes":60,"hours":{"day":10,"night":0,"sat":0,"sun":0,"bh":0},"rates":{"day":10,"night":11,"sat":12,"sun":13,"bh":14},"total_pence":10000,"calculation_fingerprint":"before"}'::jsonb;
  v_authority jsonb:='{"authority_mode":"SOURCE_AUTHORITY","self_bill_enabled":true,"source_group_id":"00000000-0000-4000-8000-000000000001","self_bill_correction_presentation":"FULL_REVERSAL_REPLACEMENT"}'::jsonb;
  v_state jsonb;
  v_noise jsonb;
  v_patch jsonb;
  v_path text[];
  v_hash bytea;
  v_invoice bytea;
  v_money jsonb:='{"source_shift_charge_pence":10000,"source_gross_pence":12000,"source_vat_pence":2000}'::jsonb;
begin
  v_state:=pg_catalog.jsonb_build_object(
    'work_event_id','synthetic-event','candidate_id','synthetic-candidate',
    'client_id','synthetic-client','contract_id','synthetic-contract',
    'work_date','2026-09-06','start_at_local','2026-09-06T08:00:00',
    'end_at_local','2026-09-06T19:00:00','break_minutes',60,'actual_net_minutes',600,
    'pay_vector',v_vector,'charge_vector',v_vector,
    'invoice_vat_rate_pct',20,'invoice_vat_chargeable',true,'authority_subset',v_authority
  );
  v_hash:=private.weekly_source_final_state_fingerprint_v1(v_state);
  v_noise:=v_state||'{"contract_updated_at":"2026-10-13T12:00:00Z","client_settings_updated_at":"later","global_settings_updated_at":"later","bh_feed_list":"later","mapping_fingerprint":"after","rate_fingerprint":"after","policy_fingerprint":"after","source_fingerprint":"after"}'::jsonb;
  v_noise:=pg_catalog.jsonb_set(v_noise,'{pay_vector,calculation_fingerprint}','"after"'::jsonb);
  v_noise:=pg_catalog.jsonb_set(v_noise,'{charge_vector,calculation_fingerprint}','"after"'::jsonb);
  if private.weekly_source_final_state_fingerprint_v1(v_noise) is distinct from v_hash then
    raise exception 'F2_TIMESTAMP_OR_NESTED_HASH_NOISE';
  end if;
  for v_patch in select value from pg_catalog.jsonb_array_elements(
    '[{"work_event_id":"different"},{"candidate_id":"different"},{"client_id":"different"},{"contract_id":"different"},{"work_date":"2026-09-07"},{"start_at_local":"2026-09-06T08:30:00"},{"end_at_local":"2026-09-06T18:30:00"},{"break_minutes":30},{"actual_net_minutes":570},{"invoice_vat_rate_pct":0},{"invoice_vat_chargeable":false}]'::jsonb
  ) loop
    if private.weekly_source_final_state_fingerprint_v1(v_state||v_patch)=v_hash then
      raise exception 'F2_REAL_VALUE_CHANGE_IGNORED: %',v_patch;
    end if;
  end loop;
  for v_path in select route from (values
    (array['pay_vector','total_pence']::text[]),
    (array['pay_vector','hours','day']::text[]),
    (array['pay_vector','rates','day']::text[]),
    (array['charge_vector','total_pence']::text[]),
    (array['charge_vector','rates','bh']::text[])
  ) paths(route) loop
    if private.weekly_source_final_state_fingerprint_v1(pg_catalog.jsonb_set(v_state,v_path,'99'::jsonb))=v_hash then
      raise exception 'F2_VECTOR_VALUE_CHANGE_IGNORED: %',v_path;
    end if;
  end loop;
  for v_patch in select value from pg_catalog.jsonb_array_elements(
    '[{"authority_mode":"TIMESHEET_AUTHORITY"},{"self_bill_enabled":false},{"source_group_id":"00000000-0000-4000-8000-000000000002"},{"self_bill_correction_presentation":"NET_DIFFERENCE_PRESENTATION"}]'::jsonb
  ) loop
    if private.weekly_source_final_state_fingerprint_v1(
      pg_catalog.jsonb_set(v_state,'{authority_subset}',v_authority||v_patch)
    )=v_hash then raise exception 'F2_AUTHORITY_CHANGE_IGNORED: %',v_patch; end if;
  end loop;
  if private.weekly_source_authority_subset_v1(v_authority||'{"settings_updated_at":"later","unrelated_setting":true}'::jsonb)
     is distinct from v_authority then raise exception 'F2_AUTHORITY_PROJECTION_NOISE'; end if;
  v_invoice:=private.weekly_source_invoice_economics_fingerprint_v1(v_money,'LINE_GROSS_MINUS_NET_FROM_SOURCE',20);
  for v_patch in select value from pg_catalog.jsonb_array_elements(
    '[{"source_shift_charge_pence":10001},{"source_gross_pence":12001},{"source_vat_pence":2001}]'::jsonb
  ) loop
    if private.weekly_source_invoice_economics_fingerprint_v1(v_money||v_patch,'LINE_GROSS_MINUS_NET_FROM_SOURCE',20)=v_invoice then
      raise exception 'F2_SOURCE_MONEY_CHANGE_IGNORED: %',v_patch;
    end if;
    if private.weekly_source_final_state_fingerprint_v1(v_state||v_patch) is distinct from v_hash then
      raise exception 'F2_INVOICE_ONLY_MONEY_CONTAMINATED_PAY';
    end if;
  end loop;
  if private.weekly_source_invoice_economics_fingerprint_v1(v_money,'ROUND_EACH_LINE_FROM_NET',20)=v_invoice
     or private.weekly_source_invoice_economics_fingerprint_v1(v_money,'LINE_GROSS_MINUS_NET_FROM_SOURCE',0)=v_invoice then
    raise exception 'F2_INVOICE_VAT_METHOD_OR_RATE_IGNORED';
  end if;
  begin
    perform private.weekly_source_invoice_economics_fingerprint_v1(v_money-'source_gross_pence','LINE_GROSS_MINUS_NET_FROM_SOURCE',20);
    raise exception 'F2_MISSING_SOURCE_MONEY_ADMITTED';
  exception when sqlstate '55000' then
    if sqlerrm<>'WEEKLY_SOURCE_INVOICE_ECONOMICS_NOT_READY' then raise; end if;
  end;
end;
$proof$;

do $m1_qualification$
declare rules jsonb; snapshot jsonb; refused boolean; signature text;
begin
  rules:='{"checking_profile_kind":"ROSTER_WEEKLY_SUMMARY_ACTUAL_V1","invoice_sender":"CLIENT_SELF_BILL","self_bill_source":"SAME_FILE","final_report_profile_kind":"NONE","invoice_membership":"FINALISED_IN_CYCLE_WINDOW","pay_membership":"ALL_FINALISED_IN_FILE","invoice_amount_basis":"CONTRACT_CALCULATION","source_amount_basis":"NONE","rate_difference_policy":"NOT_APPLICABLE","earliest_final_weekday":null,"earliest_final_local_time":null}'::jsonb;
  if private.weekly_source_magnit_contract_price_assert_v1(rules,'{"weekly_rate_classification_method":"WHOLE_SHIFT_START_DAY"}') is distinct from true
    or private.weekly_source_magnit_contract_price_assert_v1(null,'{}') is distinct from false
    or private.weekly_source_magnit_contract_price_assert_v1('{"invoice_sender":"AGENCY_INVOICE"}','{}') is distinct from false then
    raise exception 'M1_RULE_QUALIFICATION_FAILED'; end if;
  refused:=false;
  begin perform private.weekly_source_magnit_contract_price_assert_v1(rules,'{"weekly_rate_classification_method":"SPLIT_RATE_WINDOWS"}');
  exception when sqlstate '22023' then
    if sqlerrm='WEEKLY_SOURCE_MAGNIT_WHOLE_SHIFT_REQUIRED' then refused:=true; else raise; end if;
  end;
  if not refused then raise exception 'M1_CONTRACT_OVERRIDE_NOT_REFUSED'; end if;
  refused:=false;
  begin perform private.weekly_source_magnit_contract_price_assert_v1(jsonb_set(rules,'{source_amount_basis}','"EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY"'),'{"weekly_rate_classification_method":"WHOLE_SHIFT_START_DAY"}');
  exception when sqlstate '22023' then
    if sqlerrm='WEEKLY_SOURCE_MAGNIT_PRESET_INVALID' then refused:=true; else raise; end if;
  end;
  if not refused then raise exception 'M1_SOURCE_COST_AUTHORITY_NOT_REFUSED'; end if;
  snapshot:='{"rate_method":"WHOLE_SHIFT_START_DAY","paid_minutes":450,"bucket_minutes":{"day":450,"night":0,"sat":0,"sun":0,"bh":0}}'::jsonb;
  if private.weekly_source_magnit_bucket_assert_v1(rules,snapshot) is distinct from true
    or private.weekly_source_magnit_bucket_assert_v1(null,snapshot) is distinct from false then
    raise exception 'M1_BUCKET_QUALIFICATION_FAILED'; end if;
  refused:=false;
  begin perform private.weekly_source_magnit_bucket_assert_v1(rules,jsonb_set(jsonb_set(snapshot,'{bucket_minutes,day}','400'),'{bucket_minutes,night}','50'));
  exception when sqlstate '22023' then
    if sqlerrm='WEEKLY_SOURCE_MAGNIT_BUCKET_INVALID' then refused:=true; else raise; end if;
  end;
  if not refused then raise exception 'M1_SPLIT_BUCKETS_NOT_REFUSED'; end if;
  foreach signature in array array['private.weekly_source_magnit_contract_price_assert_v1(jsonb,jsonb)',
    'private.weekly_source_magnit_bucket_assert_v1(jsonb,jsonb)'] loop
    if to_regprocedure(signature) is null
      or has_function_privilege('anon',signature,'EXECUTE')
      or has_function_privilege('authenticated',signature,'EXECUTE')
      or has_function_privilege('service_role',signature,'EXECUTE')
      or not exists(select 1 from pg_catalog.pg_proc p where p.oid=to_regprocedure(signature)
        and p.proowner=current_user::regrole::oid and not p.prosecdef and p.provolatile='i'
        and p.proconfig @> array['search_path=pg_catalog, pg_temp']) then
      raise exception 'M1_OWNER_ONLY_PURE_HELPER_ACL_FAILED'; end if;
  end loop;
end;
$m1_qualification$;
select jsonb_build_object('version','HEALTHROSTER_F2_MAGNIT_SCALAR_V1','complete_verifier_blocks',2,'result','PASS_IF_REACHED') as result;
