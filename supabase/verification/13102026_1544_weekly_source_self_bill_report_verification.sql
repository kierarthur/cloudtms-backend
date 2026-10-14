-- Root release verification proposal. No installation/acceptance is claimed.
\set ON_ERROR_STOP on
begin;
do $report_catalogue$
declare v_count integer;
begin
  if (select count(*) from public.weekly_source_format_profiles where active)<>6 then raise exception 'REPORT_PROFILE_CENSUS_EXPECTED_SIX'; end if;
  if not exists(select 1 from public.weekly_source_format_profiles where profile_code='ACACIUM_SELF_BILL_REPORT_V1' and version=1
    and active and final_authority_kind='SELF_BILL_REPORT' and omission_meaning='NO_INFERENCE' and not scheduled_hours_fallback
    and report_number_required and single_client_required and container_kind='XLSX'
    and profile_sha256=extensions.digest(convert_to(profile_json::text,'UTF8'),'sha256')) then raise exception 'REPORT_PROFILE_CONTRACT_FAILED'; end if;
  select count(*) into v_count from pg_catalog.pg_trigger where tgrelid='public.weekly_source_self_bill_reports'::regclass
    and not tgisinternal and tgname in ('weekly_source_immutable_record_guard','weekly_source_immutable_truncate_guard');
  if v_count<>2 then raise exception 'REPORT_METADATA_IMMUTABLE_GUARDS_MISSING'; end if;
  if not exists(select 1 from pg_catalog.pg_index i where i.indexrelid='public.weekly_source_report_scopes_cutoff_uq'::regclass
    and i.indisunique and pg_catalog.pg_get_expr(i.indpred,i.indrelid)='(self_bill_report_id IS NULL)') then raise exception 'NHSP_PARTIAL_CUTOFF_INDEX_MISSING'; end if;
  if exists(select 1 from pg_catalog.pg_constraint where conrelid='public.weekly_source_self_bill_reports'::regclass
    and contype='f' and confrelid='public.weekly_source_final_revisions'::regclass) then raise exception 'REPORT_REVISION_CIRCLE_REINTRODUCED'; end if;
  if has_function_privilege('anon','public.weekly_source_self_bill_report_scope_resolve_atomic_v1(jsonb)','EXECUTE')
    or has_function_privilege('authenticated','public.weekly_source_self_bill_report_scope_resolve_atomic_v1(jsonb)','EXECUTE')
    or not has_function_privilege('service_role','public.weekly_source_self_bill_report_scope_resolve_atomic_v1(jsonb)','EXECUTE')
    or has_function_privilege('service_role','private.weekly_source_self_bill_report_finalise_v1(jsonb,text)','EXECUTE') then
    raise exception 'REPORT_RPC_ACL_FAILED'; end if;
  if has_table_privilege('service_role','public.weekly_source_self_bill_reports','INSERT')
    or has_table_privilege('authenticated','public.weekly_source_self_bill_report_line_bindings','UPDATE') then
    raise exception 'REPORT_DIRECT_STORE_EXPOSURE_FAILED'; end if;
end;
$report_catalogue$;
\ir support/13102026_1544_report_row_vectors.sqlinc
rollback;
