\set ON_ERROR_STOP on
begin;
do $request_read_authority$
declare refused boolean:=false;
begin
  if has_function_privilege('anon','public.timesheet_correction_request_authority_v1(uuid,uuid)','EXECUTE')
    or has_function_privilege('authenticated','public.timesheet_correction_request_authority_v1(uuid,uuid)','EXECUTE')
    or not has_function_privilege('service_role','public.timesheet_correction_request_authority_v1(uuid,uuid)','EXECUTE') then
    raise exception 'CORRECTION_REQUEST_AUTHORITY_ACL_FAILED';
  end if;
  begin perform public.timesheet_correction_request_authority_v1(null,null);
  exception when sqlstate '42501' then refused:=true; end;
  if not refused then raise exception 'CORRECTION_REQUEST_AUTHORITY_NULL_ACTOR_ADMITTED'; end if;
end;
$request_read_authority$;
do $reversal_component_quantities$
declare original jsonb; reversal jsonb; changed jsonb; refused boolean;
begin
  original:='{"policy_snapshot_json":{"erni_pct":13.8},"rate_source_refs_json":{"kind":"FROZEN"},"pay_method":"PAYE","basis":"SELF_REPORTED","hours_day":1.25,"total_hours":1.25,"total_pay_ex_vat":12.50,"total_charge_ex_vat":17.50,"margin_ex_vat":3.27,"additional_units_json":{"SLEEP":{"unit_count":1.5,"pay_rate":2,"charge_rate":3,"pay_cost_ex_vat_including_erni":3.414,"pay_ex_vat":3,"charge_ex_vat":4.5,"margin_ex_vat":1.086}},"invoice_breakdown_json":{"mode":"AGGREGATE","base_hours":{"day":1.25,"night":0,"sat":0,"sun":0,"bh":0,"pay_rates":{"day":10},"charge_rates":{"day":14},"pay_ex_vat":12.50,"charge_ex_vat":17.50,"vat_amount":3.50,"total_inc_vat":21},"totals":{"total_pay_ex_vat":12.50,"total_charge_ex_vat":17.50,"margin_ex_vat":3.27}}}'::jsonb;
  reversal:='{"policy_snapshot_json":{"erni_pct":13.8},"rate_source_refs_json":{"kind":"FROZEN"},"pay_method":"PAYE","basis":"SELF_REPORTED","hours_day":-1.25,"total_hours":-1.25,"total_pay_ex_vat":-12.50,"total_charge_ex_vat":-17.50,"margin_ex_vat":-3.27,"additional_units_json":{"SLEEP":{"unit_count":-1.5,"pay_rate":2,"charge_rate":3,"pay_cost_ex_vat_including_erni":-3.414,"pay_ex_vat":-3,"charge_ex_vat":-4.5,"margin_ex_vat":-1.086}},"invoice_breakdown_json":{"mode":"AGGREGATE","base_hours":{"day":-1.25,"night":0,"sat":0,"sun":0,"bh":0,"pay_rates":{"day":10},"charge_rates":{"day":14},"pay_ex_vat":-12.50,"charge_ex_vat":-17.50,"vat_amount":-3.50,"total_inc_vat":-21},"totals":{"total_pay_ex_vat":-12.50,"total_charge_ex_vat":-17.50,"margin_ex_vat":-3.27}}}'::jsonb;
  perform private.timesheet_correction_reversal_assert_v1(original,reversal);
  perform private.timesheet_correction_reversal_assert_v1(original,
    private.timesheet_correction_native_original_reversal_v1(original,'90000000-0000-4000-8000-000000000099'));
  if private.timesheet_correction_native_original_reversal_v1(original,'90000000-0000-4000-8000-000000000099')->>'timesheet_id'
      is distinct from '90000000-0000-4000-8000-000000000099'
    or has_function_privilege('service_role','private.timesheet_correction_native_original_reversal_v1(jsonb,uuid)','EXECUTE')
    or has_function_privilege('authenticated','private.timesheet_correction_component_negate_v1(jsonb,text[])','EXECUTE') then
    raise exception 'CORRECTION_NATIVE_NEGATIVE_IDENTITY_OR_ACL_FAILED';
  end if;
  changed:=jsonb_set(reversal,'{invoice_breakdown_json,base_hours,day}','1.25'::jsonb);refused:=false;
  begin perform private.timesheet_correction_reversal_assert_v1(original,changed);
  exception when sqlstate '55000' then refused:=true; end;
  if not refused then raise exception 'CORRECTION_AGGREGATE_QUANTITY_NOT_REVERSED'; end if;
  changed:=jsonb_set(reversal,'{invoice_breakdown_json,base_hours,pay_rates,day}','-10'::jsonb);refused:=false;
  begin perform private.timesheet_correction_reversal_assert_v1(original,changed);
  exception when sqlstate '55000' then refused:=true; end;
  if not refused then raise exception 'CORRECTION_HISTORICAL_RATE_CHANGED'; end if;
  changed:=jsonb_set(reversal,'{additional_units_json,SLEEP,pay_cost_ex_vat_including_erni}','3.414'::jsonb);refused:=false;
  begin perform private.timesheet_correction_reversal_assert_v1(original,changed);
  exception when sqlstate '55000' then refused:=true; end;
  if not refused then raise exception 'CORRECTION_ERNICOST_NOT_REVERSED'; end if;
  -- These pure helper vectors do not create/approve a request or run the
  -- canonical financial writer. Nonzero native component acceptance is separate.
end;
$reversal_component_quantities$;
rollback;
