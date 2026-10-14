do $codec_guard$ begin if current_database()<>'cloudtms_test_clone' or not exists(select 1 from private.cloudtms_database_identity where singleton and environment='TEST') or not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='LEGACY' and owner_epoch=1) or exists(select 1 from private.cloudtms_database_releases where status='APPLYING') or to_regprocedure('private.weekly_source_report_prepared_financial_codec_v1(public.timesheets_financials)') is not null then raise exception 'CODEC_DATATYPE_TEST_GUARD_REFUSED'; end if; end; $codec_guard$;
select current_database() physical_database, (select md5(string_agg(n.nspname||'.'||p.proname||'('||pg_get_function_identity_arguments(p.oid)||')'||coalesce(p.prosrc,'')||coalesce(p.probin,'')||p.proowner::text||coalesce(p.proacl::text,'')||coalesce(p.proconfig::text,'')||p.prolang::text||p.prosecdef::text||p.provolatile::text,'|' order by p.oid)) from pg_proc p join pg_namespace n on n.oid=p.pronamespace where n.nspname in('public','private')) functions_hash, exists(select 1 from private.cloudtms_database_identity where singleton and environment='TEST') identity_test, (select active_owner||':'||owner_epoch::text from private.bpay_next_module_control where id=1) module, to_regprocedure('private.weekly_source_report_prepared_financial_codec_v1(public.timesheets_financials)') is null codec_absent, current_setting('array_nulls') array_nulls;
create or replace function private.weekly_source_report_prepared_financial_codec_v1(p_fin public.timesheets_financials)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
set array_nulls to 'on'
as $function$
declare v_output jsonb; v_key text;
begin
  if p_fin.id is null then raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='22023'; end if;
  -- All 90 actual native INSERT output fields plus identity/timestamps.
  -- Decimal amounts/rates/quantities and nested JSONB are serialized as exact
  -- PostgreSQL text; JavaScript floating-point decoding is never involved.
  v_output:=pg_catalog.jsonb_build_object(
    'accommodation_charge_ex_vat',p_fin.accommodation_charge_ex_vat::text,
    'accommodation_pay_ex_vat',p_fin.accommodation_pay_ex_vat::text,
    'actual_minutes_by_day_json',p_fin.actual_minutes_by_day_json::text,
    'actual_schedule_json',p_fin.actual_schedule_json::text,
    'additional_charge_ex_vat',p_fin.additional_charge_ex_vat::text,
    'additional_margin_ex_vat',p_fin.additional_margin_ex_vat::text,
    'additional_pay_ex_vat',p_fin.additional_pay_ex_vat::text,
    'additional_units_json',p_fin.additional_units_json::text,
    'authorised_at_utc',pg_catalog.to_jsonb(p_fin)->'authorised_at_utc',
    'authorised_by_user_id',pg_catalog.to_jsonb(p_fin)->'authorised_by_user_id',
    'band',pg_catalog.to_jsonb(p_fin)->'band',
    'basis',pg_catalog.to_jsonb(p_fin)->'basis',
    'break_end_iso',pg_catalog.to_jsonb(p_fin)->'break_end_iso',
    'break_minutes',pg_catalog.to_jsonb(p_fin)->'break_minutes',
    'break_start_iso',pg_catalog.to_jsonb(p_fin)->'break_start_iso',
    'candidate_assignment',pg_catalog.to_jsonb(p_fin)->'candidate_assignment',
    'candidate_id',pg_catalog.to_jsonb(p_fin)->'candidate_id',
    'charge_bh',p_fin.charge_bh::text,
    'charge_day',p_fin.charge_day::text,
    'charge_night',p_fin.charge_night::text,
    'charge_sat',p_fin.charge_sat::text,
    'charge_sun',p_fin.charge_sun::text,
    'client_id',pg_catalog.to_jsonb(p_fin)->'client_id',
    'computed_at_utc',pg_catalog.to_jsonb(p_fin)->'computed_at_utc',
    'created_at',pg_catalog.to_jsonb(p_fin)->'created_at',
    'expenses_charge_ex_vat',p_fin.expenses_charge_ex_vat::text,
    'expenses_description',pg_catalog.to_jsonb(p_fin)->'expenses_description',
    'expenses_evidence_manifest',p_fin.expenses_evidence_manifest::text,
    'expenses_evidence_r2_key',pg_catalog.to_jsonb(p_fin)->'expenses_evidence_r2_key',
    'expenses_pay_ex_vat',p_fin.expenses_pay_ex_vat::text,
    'external_source_rows_json',p_fin.external_source_rows_json::text,
    'has_pay_channel_issue',pg_catalog.to_jsonb(p_fin)->'has_pay_channel_issue',
    'has_rate_issue',pg_catalog.to_jsonb(p_fin)->'has_rate_issue',
    'hours_bh',p_fin.hours_bh::text,
    'hours_day',p_fin.hours_day::text,
    'hours_night',p_fin.hours_night::text,
    'hours_sat',p_fin.hours_sat::text,
    'hours_sun',p_fin.hours_sun::text,
    'hr_crosscheck_issues',p_fin.hr_crosscheck_issues::text,
    'hr_crosscheck_status',pg_catalog.to_jsonb(p_fin)->'hr_crosscheck_status'
  )||
  pg_catalog.jsonb_build_object(
    'id',pg_catalog.to_jsonb(p_fin)->'id',
    'invoice_breakdown_json',p_fin.invoice_breakdown_json::text,
    'is_current',pg_catalog.to_jsonb(p_fin)->'is_current',
    'is_stale',pg_catalog.to_jsonb(p_fin)->'is_stale',
    'margin_ex_vat',p_fin.margin_ex_vat::text,
    'mileage_charge_ex_vat',p_fin.mileage_charge_ex_vat::text,
    'mileage_charge_rate',p_fin.mileage_charge_rate::text,
    'mileage_evidence_manifest',p_fin.mileage_evidence_manifest::text,
    'mileage_evidence_r2_key',pg_catalog.to_jsonb(p_fin)->'mileage_evidence_r2_key',
    'mileage_pay_ex_vat',p_fin.mileage_pay_ex_vat::text,
    'mileage_pay_rate',p_fin.mileage_pay_rate::text,
    'mileage_units',p_fin.mileage_units::text,
    'nhsp_import_id',pg_catalog.to_jsonb(p_fin)->'nhsp_import_id',
    'occupant_key_norm',pg_catalog.to_jsonb(p_fin)->'occupant_key_norm',
    'other_charge_ex_vat',p_fin.other_charge_ex_vat::text,
    'other_pay_ex_vat',p_fin.other_pay_ex_vat::text,
    'pay_bh',p_fin.pay_bh::text,
    'pay_day',p_fin.pay_day::text,
    'pay_method',pg_catalog.to_jsonb(p_fin)->'pay_method',
    'pay_night',p_fin.pay_night::text,
    'pay_on_hold',pg_catalog.to_jsonb(p_fin)->'pay_on_hold',
    'pay_on_hold_reason',pg_catalog.to_jsonb(p_fin)->'pay_on_hold_reason',
    'pay_on_hold_since_utc',pg_catalog.to_jsonb(p_fin)->'pay_on_hold_since_utc',
    'pay_sat',p_fin.pay_sat::text,
    'pay_sun',p_fin.pay_sun::text,
    'pay_total_inc_vat_snapshot',p_fin.pay_total_inc_vat_snapshot::text,
    'pay_vat_amount_snapshot',p_fin.pay_vat_amount_snapshot::text,
    'pay_vat_rate_pct_snapshot',p_fin.pay_vat_rate_pct_snapshot::text,
    'pay_wtr_rate_pct_snapshot',p_fin.pay_wtr_rate_pct_snapshot::text,
    'payment_reference',pg_catalog.to_jsonb(p_fin)->'payment_reference',
    'po_number',pg_catalog.to_jsonb(p_fin)->'po_number',
    'policy_snapshot_json',p_fin.policy_snapshot_json::text,
    'processed_at_utc',pg_catalog.to_jsonb(p_fin)->'processed_at_utc',
    'processed_by_user_id',pg_catalog.to_jsonb(p_fin)->'processed_by_user_id',
    'processing_status',pg_catalog.to_jsonb(p_fin)->'processing_status',
    'rate_source_refs_json',p_fin.rate_source_refs_json::text,
    'remittance_last_sent_at_utc',pg_catalog.to_jsonb(p_fin)->'remittance_last_sent_at_utc',
    'remittance_send_count',pg_catalog.to_jsonb(p_fin)->'remittance_send_count',
    'role',pg_catalog.to_jsonb(p_fin)->'role',
    'stale_reason',pg_catalog.to_jsonb(p_fin)->'stale_reason'
  )||
  pg_catalog.jsonb_build_object(
    'timesheet_id',pg_catalog.to_jsonb(p_fin)->'timesheet_id',
    'timesheet_version',pg_catalog.to_jsonb(p_fin)->'timesheet_version',
    'total_charge_ex_vat',p_fin.total_charge_ex_vat::text,
    'total_hours',p_fin.total_hours::text,
    'total_pay_ex_vat',p_fin.total_pay_ex_vat::text,
    'travel_charge_ex_vat',p_fin.travel_charge_ex_vat::text,
    'travel_pay_ex_vat',p_fin.travel_pay_ex_vat::text,
    'updated_at',pg_catalog.to_jsonb(p_fin)->'updated_at',
    'worked_end_iso',pg_catalog.to_jsonb(p_fin)->'worked_end_iso',
    'worked_start_iso',pg_catalog.to_jsonb(p_fin)->'worked_start_iso'
  );
  if v_output is null or pg_catalog.jsonb_typeof(v_output) is distinct from 'object'
    or (select pg_catalog.array_agg(k order by k) from pg_catalog.jsonb_object_keys(v_output) k)
      is distinct from array['accommodation_charge_ex_vat','accommodation_pay_ex_vat','actual_minutes_by_day_json','actual_schedule_json','additional_charge_ex_vat','additional_margin_ex_vat','additional_pay_ex_vat','additional_units_json','authorised_at_utc','authorised_by_user_id','band','basis','break_end_iso','break_minutes','break_start_iso','candidate_assignment','candidate_id','charge_bh','charge_day','charge_night','charge_sat','charge_sun','client_id','computed_at_utc','created_at','expenses_charge_ex_vat','expenses_description','expenses_evidence_manifest','expenses_evidence_r2_key','expenses_pay_ex_vat','external_source_rows_json','has_pay_channel_issue','has_rate_issue','hours_bh','hours_day','hours_night','hours_sat','hours_sun','hr_crosscheck_issues','hr_crosscheck_status','id','invoice_breakdown_json','is_current','is_stale','margin_ex_vat','mileage_charge_ex_vat','mileage_charge_rate','mileage_evidence_manifest','mileage_evidence_r2_key','mileage_pay_ex_vat','mileage_pay_rate','mileage_units','nhsp_import_id','occupant_key_norm','other_charge_ex_vat','other_pay_ex_vat','pay_bh','pay_day','pay_method','pay_night','pay_on_hold','pay_on_hold_reason','pay_on_hold_since_utc','pay_sat','pay_sun','pay_total_inc_vat_snapshot','pay_vat_amount_snapshot','pay_vat_rate_pct_snapshot','pay_wtr_rate_pct_snapshot','payment_reference','po_number','policy_snapshot_json','processed_at_utc','processed_by_user_id','processing_status','rate_source_refs_json','remittance_last_sent_at_utc','remittance_send_count','role','stale_reason','timesheet_id','timesheet_version','total_charge_ex_vat','total_hours','total_pay_ex_vat','travel_charge_ex_vat','travel_pay_ex_vat','updated_at','worked_end_iso','worked_start_iso']::text[] then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'accommodation_charge_ex_vat') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'accommodation_charge_ex_vat')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'accommodation_pay_ex_vat') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'accommodation_pay_ex_vat')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'actual_minutes_by_day_json') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'actual_schedule_json') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'additional_charge_ex_vat') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'additional_charge_ex_vat')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'additional_margin_ex_vat') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'additional_margin_ex_vat')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'additional_pay_ex_vat') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'additional_pay_ex_vat')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'additional_units_json') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'additional_units_json')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'authorised_at_utc') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'authorised_by_user_id') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if v_output->>'authorised_by_user_id' is not null and v_output->>'authorised_by_user_id'!~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'band') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'basis') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'basis')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'break_end_iso') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'break_minutes') not in ('null','number') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'break_start_iso') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'candidate_assignment') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'candidate_assignment')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'candidate_id') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if v_output->>'candidate_id' is not null and v_output->>'candidate_id'!~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'charge_bh') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'charge_day') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'charge_night') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'charge_sat') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'charge_sun') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'client_id') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if v_output->>'client_id' is not null and v_output->>'client_id'!~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'computed_at_utc') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'computed_at_utc')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'created_at') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'created_at')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'expenses_charge_ex_vat') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'expenses_charge_ex_vat')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'expenses_description') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'expenses_evidence_manifest') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'expenses_evidence_r2_key') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'expenses_pay_ex_vat') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'expenses_pay_ex_vat')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'external_source_rows_json') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'has_pay_channel_issue') not in ('null','boolean') or pg_catalog.jsonb_typeof(v_output->'has_pay_channel_issue')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'has_rate_issue') not in ('null','boolean') or pg_catalog.jsonb_typeof(v_output->'has_rate_issue')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'hours_bh') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'hours_bh')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'hours_day') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'hours_day')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'hours_night') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'hours_night')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'hours_sat') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'hours_sat')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'hours_sun') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'hours_sun')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'hr_crosscheck_issues') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'hr_crosscheck_issues')='string' then
    begin
      -- Native array_out text preserves dimensions, non-1 lower bounds,
      -- NULL elements versus quoted NULL strings, quotes and backslashes.
      -- Fixed array_nulls=on is parsing configuration, never authority.
      if (((v_output->>'hr_crosscheck_issues')::text[])::text)
        is distinct from v_output->>'hr_crosscheck_issues' then
        raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
    exception when invalid_text_representation or array_subscript_error or program_limit_exceeded then
      raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000';
    end;
  end if;
  if pg_catalog.jsonb_typeof(v_output->'hr_crosscheck_status') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'id') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'id')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if v_output->>'id' is not null and v_output->>'id'!~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'invoice_breakdown_json') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'invoice_breakdown_json')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'is_current') not in ('null','boolean') or pg_catalog.jsonb_typeof(v_output->'is_current')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'is_stale') not in ('null','boolean') or pg_catalog.jsonb_typeof(v_output->'is_stale')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'margin_ex_vat') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'margin_ex_vat')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'mileage_charge_ex_vat') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'mileage_charge_ex_vat')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'mileage_charge_rate') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'mileage_evidence_manifest') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'mileage_evidence_r2_key') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'mileage_pay_ex_vat') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'mileage_pay_ex_vat')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'mileage_pay_rate') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'mileage_units') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'mileage_units')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'nhsp_import_id') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if v_output->>'nhsp_import_id' is not null and v_output->>'nhsp_import_id'!~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'occupant_key_norm') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'other_charge_ex_vat') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'other_charge_ex_vat')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'other_pay_ex_vat') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'other_pay_ex_vat')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'pay_bh') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'pay_day') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'pay_method') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'pay_night') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'pay_on_hold') not in ('null','boolean') or pg_catalog.jsonb_typeof(v_output->'pay_on_hold')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'pay_on_hold_reason') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'pay_on_hold_since_utc') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'pay_sat') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'pay_sun') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'pay_total_inc_vat_snapshot') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'pay_total_inc_vat_snapshot')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'pay_vat_amount_snapshot') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'pay_vat_amount_snapshot')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'pay_vat_rate_pct_snapshot') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'pay_wtr_rate_pct_snapshot') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'payment_reference') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'po_number') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'policy_snapshot_json') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'policy_snapshot_json')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'processed_at_utc') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'processed_by_user_id') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if v_output->>'processed_by_user_id' is not null and v_output->>'processed_by_user_id'!~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'processing_status') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'processing_status')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'rate_source_refs_json') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'rate_source_refs_json')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'remittance_last_sent_at_utc') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'remittance_send_count') not in ('null','number') or pg_catalog.jsonb_typeof(v_output->'remittance_send_count')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'role') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'stale_reason') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'timesheet_id') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'timesheet_id')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if v_output->>'timesheet_id' is not null and v_output->>'timesheet_id'!~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'timesheet_version') not in ('null','number') or pg_catalog.jsonb_typeof(v_output->'timesheet_version')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'total_charge_ex_vat') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'total_charge_ex_vat')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'total_hours') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'total_hours')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'total_pay_ex_vat') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'total_pay_ex_vat')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'travel_charge_ex_vat') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'travel_charge_ex_vat')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'travel_pay_ex_vat') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'travel_pay_ex_vat')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'updated_at') not in ('null','string') or pg_catalog.jsonb_typeof(v_output->'updated_at')='null' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'worked_end_iso') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  if pg_catalog.jsonb_typeof(v_output->'worked_start_iso') not in ('null','string') then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  for v_key in select pg_catalog.unnest(array['accommodation_charge_ex_vat','accommodation_pay_ex_vat','additional_charge_ex_vat','additional_margin_ex_vat','additional_pay_ex_vat','charge_bh','charge_day','charge_night','charge_sat','charge_sun','expenses_charge_ex_vat','expenses_pay_ex_vat','hours_bh','hours_day','hours_night','hours_sat','hours_sun','margin_ex_vat','mileage_charge_ex_vat','mileage_charge_rate','mileage_pay_ex_vat','mileage_pay_rate','mileage_units','other_charge_ex_vat','other_pay_ex_vat','pay_bh','pay_day','pay_night','pay_sat','pay_sun','pay_total_inc_vat_snapshot','pay_vat_amount_snapshot','pay_vat_rate_pct_snapshot','pay_wtr_rate_pct_snapshot','total_charge_ex_vat','total_hours','total_pay_ex_vat','travel_charge_ex_vat','travel_pay_ex_vat']::text[]) loop
    if pg_catalog.jsonb_typeof(v_output->v_key) is distinct from 'null' and
      (pg_catalog.jsonb_typeof(v_output->v_key) is distinct from 'string'
       or coalesce(v_output->>v_key,'')!~'^-?(0|[1-9][0-9]*)([.][0-9]+)?$') then
      raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  end loop;
  if coalesce(v_output->>'total_pay_ex_vat','')!~'^-?(0|[1-9][0-9]*)([.][0-9]+)?$'
    or coalesce(v_output->>'timesheet_version','')!~'^[1-9][0-9]*$' then
    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;
  return v_output;
end;
$function$;
alter function private.weekly_source_report_prepared_financial_codec_v1(public.timesheets_financials) owner to CURRENT_USER;
revoke all on function private.weekly_source_report_prepared_financial_codec_v1(public.timesheets_financials) from public,anon,authenticated,service_role;
set local array_nulls=off;
-- Unexecuted, read-only datatype cases for Root's later SEND_7 trial.
-- No financial record/receipt/publication is created or accepted here.
do $native_array_text_cases$
declare r record; restored text[]; encoded text;
  original_array_nulls text:=pg_catalog.current_setting('array_nulls');
begin
  for r in select * from (values
    ('TWO_DIMENSIONS',ARRAY[['RATE_A','RATE_B'],['RATE_C','RATE_D']]::text[]),
    ('NON_ONE_LOWER_BOUNDS',$array$[0:1][3:4]={{RATE_A,RATE_B},{RATE_C,RATE_D}}$array$::text[]),
    ('ONE_DIMENSION',ARRAY['RATE_A','RATE_B']::text[]),
    ('NULL_STRING_QUOTES_BACKSLASH',ARRAY[null::text,'NULL','a,b',$item$a"b$item$,$item$a\b$item$]::text[]),
    ('EMPTY_ARRAY',ARRAY[]::text[]),
    ('SQL_NULL_ARRAY',null::text[])
  ) cases(case_name,native_array) loop
    encoded:=r.native_array::text;
    -- Same fixed parser semantics as the two complete forward owners. This
    -- local setting is datatype configuration, never financial authority.
    perform pg_catalog.set_config('array_nulls','on',true);
    restored:=encoded::text[];
    if restored is distinct from r.native_array or restored::text is distinct from encoded
      or pg_catalog.array_dims(restored) is distinct from pg_catalog.array_dims(r.native_array)
      or pg_catalog.array_lower(restored,1) is distinct from pg_catalog.array_lower(r.native_array,1)
      or pg_catalog.array_lower(restored,2) is distinct from pg_catalog.array_lower(r.native_array,2) then
      raise exception 'REPORT_PREPARED_NATIVE_ARRAY_ROUNDTRIP_FAILED' using errcode='55000',detail=r.case_name;
    end if;
  end loop;
  perform pg_catalog.set_config('array_nulls',original_array_nulls,true);
exception when others then
  perform pg_catalog.set_config('array_nulls',original_array_nulls,true);
  raise;
end;
$native_array_text_cases$;

do $compiled_codec_cases$
declare r record; fin public.timesheets_financials; different_bounds public.timesheets_financials; encoded jsonb; second_output jsonb; checked integer:=0;
begin
 for r in select * from (values
 ('TWO_DIMENSIONS',ARRAY[['RATE_A','RATE_B'],['RATE_C','RATE_D']]::text[]),
 ('NON_ONE_LOWER_BOUNDS',$array$[0:1][3:4]={{RATE_A,RATE_B},{RATE_C,RATE_D}}$array$::text[]),
 ('ONE_DIMENSION',ARRAY['RATE_A','RATE_B']::text[]),
 ('NULL_STRING_QUOTES_BACKSLASH',ARRAY[null::text,'NULL','a,b',$item$a"b$item$,$item$a\b$item$]::text[]),
 ('EMPTY_ARRAY',ARRAY[]::text[]),('SQL_NULL_ARRAY',null::text[])
 ) cases(case_name,native_array) loop
 fin:=pg_catalog.jsonb_populate_record(null::public.timesheets_financials,$codec_sample${"accommodation_charge_ex_vat":0,"accommodation_pay_ex_vat":0,"additional_charge_ex_vat":0,"additional_margin_ex_vat":0,"additional_pay_ex_vat":0,"additional_units_json":{},"basis":"SELF_REPORTED","candidate_assignment":"UNASSIGNED","computed_at_utc":"2000-01-01T00:00:00+00:00","created_at":"2000-01-01T00:00:00+00:00","expenses_charge_ex_vat":0,"expenses_pay_ex_vat":0,"has_pay_channel_issue":false,"has_rate_issue":false,"hours_bh":0,"hours_day":0,"hours_night":0,"hours_sat":0,"hours_sun":0,"id":"13102026-2303-4000-8000-000000000001","invoice_breakdown_json":{},"is_current":false,"is_stale":false,"margin_ex_vat":0,"mileage_charge_ex_vat":0,"mileage_pay_ex_vat":0,"mileage_units":0,"other_charge_ex_vat":0,"other_pay_ex_vat":0,"pay_on_hold":false,"pay_total_inc_vat_snapshot":0,"pay_vat_amount_snapshot":0,"policy_snapshot_json":{},"processing_status":"UNASSIGNED","rate_source_refs_json":{},"remittance_send_count":0,"timesheet_id":"13102026-2303-4000-8000-000000000002","timesheet_version":1,"total_charge_ex_vat":0,"total_hours":0,"total_pay_ex_vat":0,"travel_charge_ex_vat":0,"travel_pay_ex_vat":0,"updated_at":"2000-01-01T00:00:00+00:00"}$codec_sample$::jsonb);
 fin.hr_crosscheck_issues:=r.native_array;
 encoded:=private.weekly_source_report_prepared_financial_codec_v1(fin);
 if encoded->>'hr_crosscheck_issues' is distinct from fin.hr_crosscheck_issues::text
 or pg_catalog.current_setting('array_nulls')<>'off' then
 raise exception 'COMPILED_CODEC_NATIVE_ARRAY_OR_SETTING_FAILED' using errcode='55000',detail=r.case_name; end if;
 checked:=checked+1;
 end loop;
 fin.hr_crosscheck_issues:=$array$[0:1]={RATE_A,RATE_B}$array$::text[];
 different_bounds:=fin;different_bounds.hr_crosscheck_issues:=ARRAY['RATE_A','RATE_B']::text[];
 encoded:=private.weekly_source_report_prepared_financial_codec_v1(fin);
 second_output:=private.weekly_source_report_prepared_financial_codec_v1(different_bounds);
 if pg_catalog.to_jsonb(fin) is distinct from pg_catalog.to_jsonb(different_bounds)
 or encoded is not distinct from second_output or checked<>6 then
 raise exception 'COMPILED_CODEC_NATIVE_ARRAY_BOUND_DRIFT_NOT_VISIBLE' using errcode='55000';end if;
 if pg_catalog.has_function_privilege('anon','private.weekly_source_report_prepared_financial_codec_v1(public.timesheets_financials)','EXECUTE')
 or pg_catalog.has_function_privilege('authenticated','private.weekly_source_report_prepared_financial_codec_v1(public.timesheets_financials)','EXECUTE')
 or pg_catalog.has_function_privilege('service_role','private.weekly_source_report_prepared_financial_codec_v1(public.timesheets_financials)','EXECUTE') then
 raise exception 'COMPILED_CODEC_PRIVATE_ACL_FAILED' using errcode='55000';end if;
end; $compiled_codec_cases$;
select 'DATATYPE_ONLY_NO_FINANCIAL_QUALIFICATION' scope, true native_codec_six_cases_and_bound_drift_pass;
do $compiled_codec_security_and_errors$
declare p pg_catalog.pg_proc%rowtype; fin public.timesheets_financials; refused boolean;
begin
 select * into strict p from pg_catalog.pg_proc where oid=pg_catalog.to_regprocedure('private.weekly_source_report_prepared_financial_codec_v1(public.timesheets_financials)');
 if p.proowner<>current_user::regrole::oid or not p.prosecdef or p.provolatile<>'s' or p.prokind<>'f'
 or p.pronargdefaults<>0 or p.prorettype<>'jsonb'::regtype::oid
 or p.proconfig is distinct from array['search_path=pg_catalog, pg_temp','array_nulls=on']::text[] then
 raise exception 'COMPILED_CODEC_EXACT_SECURITY_CONFIG_FAILED' using errcode='55000'; end if;
 if pg_catalog.current_setting('array_nulls')<>'off' then
 raise exception 'COMPILED_CODEC_CALLER_CONFIG_NOT_OFF' using errcode='55000';end if;
 refused:=false;
 begin
 perform private.weekly_source_report_prepared_financial_codec_v1(null::public.timesheets_financials);
 exception when sqlstate '22023' then
 if sqlerrm<>'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' then raise;end if;refused:=true;
 end;
 if not refused or pg_catalog.current_setting('array_nulls')<>'off' then
 raise exception 'COMPILED_CODEC_NULL_REFUSAL_CONFIG_FAILED' using errcode='55000';end if;
 fin:=pg_catalog.jsonb_populate_record(null::public.timesheets_financials,$codec_sample${"accommodation_charge_ex_vat":0,"accommodation_pay_ex_vat":0,"additional_charge_ex_vat":0,"additional_margin_ex_vat":0,"additional_pay_ex_vat":0,"additional_units_json":{},"basis":"SELF_REPORTED","candidate_assignment":"UNASSIGNED","computed_at_utc":"2000-01-01T00:00:00+00:00","created_at":"2000-01-01T00:00:00+00:00","expenses_charge_ex_vat":0,"expenses_pay_ex_vat":0,"has_pay_channel_issue":false,"has_rate_issue":false,"hours_bh":0,"hours_day":0,"hours_night":0,"hours_sat":0,"hours_sun":0,"id":"13102026-2303-4000-8000-000000000001","invoice_breakdown_json":{},"is_current":false,"is_stale":false,"margin_ex_vat":0,"mileage_charge_ex_vat":0,"mileage_pay_ex_vat":0,"mileage_units":0,"other_charge_ex_vat":0,"other_pay_ex_vat":0,"pay_on_hold":false,"pay_total_inc_vat_snapshot":0,"pay_vat_amount_snapshot":0,"policy_snapshot_json":{},"processing_status":"UNASSIGNED","rate_source_refs_json":{},"remittance_send_count":0,"timesheet_id":"13102026-2303-4000-8000-000000000002","timesheet_version":1,"total_charge_ex_vat":0,"total_hours":0,"total_pay_ex_vat":0,"travel_charge_ex_vat":0,"travel_pay_ex_vat":0,"updated_at":"2000-01-01T00:00:00+00:00"}$codec_sample$::jsonb);
 fin.total_pay_ex_vat:='Infinity'::numeric;
 refused:=false;
 begin
 perform private.weekly_source_report_prepared_financial_codec_v1(fin);
 exception when sqlstate '55000' then
 if sqlerrm<>'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' then raise;end if;refused:=true;
 end;
 if not refused or pg_catalog.current_setting('array_nulls')<>'off' then
 raise exception 'COMPILED_CODEC_NONFINITE_REFUSAL_CONFIG_FAILED' using errcode='55000';end if;
end;$compiled_codec_security_and_errors$;
