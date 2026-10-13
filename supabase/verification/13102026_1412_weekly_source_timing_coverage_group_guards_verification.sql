-- Data-free helper vectors and role/definition checks. Root must add this file
-- to verificationFiles and newVerificationFiles in the reviewed release.
\set ON_ERROR_STOP on
\ir ../../tests/weekly-source/f8-admission-vectors.sql

do $security$
declare v_signature text; v_role text; v_oid oid; v_finaliser text;
begin
  foreach v_signature in array array[
    'private.weekly_source_final_upload_facts_v1(text,date,timestamp with time zone,timestamp with time zone,text,date,date,jsonb,date)',
    'private.weekly_source_final_upload_gate_v1(uuid)',
    'private.weekly_source_final_upload_assert_v1(uuid)',
    'private.weekly_source_rule_coverage_floor_v1(uuid,uuid)',
    'private.weekly_source_upload_coverage_context_v1(uuid,uuid)',
    'private.weekly_source_upload_coverage_floor_assert_v1(uuid,uuid,text,date)'
  ] loop
    v_oid:=to_regprocedure(v_signature);
    if v_oid is null then raise exception 'F8 helper missing: %',v_signature; end if;
    if exists(select 1 from pg_proc p where p.oid=v_oid and (p.proowner<>(current_user::regrole)::oid
      or p.proconfig is distinct from array['search_path=pg_catalog, pg_temp'])) then
      raise exception 'F8 owner/search path incorrect: %',v_signature;
    end if;
    foreach v_role in array array['anon','authenticated','service_role'] loop
      if has_function_privilege(v_role,v_oid,'EXECUTE') then raise exception 'F8 private helper exposed: % / %',v_role,v_signature; end if;
    end loop;
  end loop;
  foreach v_signature in array array['public.weekly_source_import_prepare_atomic_v1(jsonb)',
    'public.weekly_source_client_settings_save_atomic_v1(jsonb)', 'public.weekly_source_upload_context_v1(jsonb)',
    'public.weekly_source_upload_stage_begin_atomic_v1(jsonb)'] loop
    v_oid:=to_regprocedure(v_signature);
    if v_oid is null or not has_function_privilege('service_role',v_oid,'EXECUTE')
      or has_function_privilege('anon',v_oid,'EXECUTE') or has_function_privilege('authenticated',v_oid,'EXECUTE') then
      raise exception 'F8 public service boundary incorrect: %',v_signature;
    end if;
  end loop;
  if position('weekly_source_final_upload_assert_v1' in pg_get_functiondef(
    'public.weekly_source_import_prepare_atomic_v1(jsonb)'::regprocedure))=0 then raise exception 'F8 direct Prepare not guarded'; end if;
  if position('weekly_source_upload_coverage_floor_assert_v1' in pg_get_functiondef(
    'public.weekly_source_upload_stage_begin_atomic_v1(jsonb)'::regprocedure))=0 then raise exception 'F8 direct stage not guarded'; end if;
  if position('WEEKLY_SOURCE_CLIENT_GROUP_HAS_CURRENT_FINAL_REVISION' in pg_get_functiondef(
    'public.weekly_source_client_settings_save_atomic_v1(jsonb)'::regprocedure))=0 then raise exception 'F8 group move not guarded'; end if;
  v_oid:=to_regprocedure('private.weekly_source_finalise_engine_v1(jsonb,uuid,boolean)');
  if v_oid is null then raise exception 'F8 controlling finaliser missing'; end if;
  v_finaliser:=pg_get_functiondef(v_oid);
  if position('weekly_source_final_upload_assert_v1(v_upload.id)' in v_finaliser)=0 then
    raise exception 'F8 finaliser gate not integrated';
  end if;
  if v_finaliser !~ 'perform 1 from public.weekly_source_groups[[:space:]]+where id=\(select source_group_id from public.weekly_source_cycles where id=v_cycle_id\) for update;' then
    raise exception 'F8 G1 group fence not integrated';
  end if;
end;
$security$;
