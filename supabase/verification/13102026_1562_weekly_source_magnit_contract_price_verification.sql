-- Data-free native helper/ACL verification; financial/expense/activation
-- journeys are separately required. This never creates business fixtures.
\set ON_ERROR_STOP on
begin;
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
rollback;
