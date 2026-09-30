-- Repeatable CloudTMS function/view authority: weekly_source_import_preparation_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

create or replace function private.weekly_source_import_is_prepared_v1(p_upload_id uuid)
returns boolean language sql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
  select coalesce((select
    profile.row_finalisation_capability<>'CHECKING_ONLY'
    and (profile.profile_code='NHSP_FINAL_BACKING_V1'
      or upload.purpose='FINAL_SOURCE_CORRECTION'
      or upload.file_metadata_json->>'import_use'='PREPARE_FINALISATION'
      or exists(select 1 from private.weekly_source_import_preparations preparation
        where preparation.upload_id=upload.id and preparation.row_manifest_hash=upload.row_manifest_hash))
    from public.weekly_source_uploads upload
    join public.weekly_source_format_profiles profile on profile.id=upload.source_format_profile_id
    where upload.id=p_upload_id),false);
$function$;

create or replace function public.weekly_source_import_prepare_atomic_v1(p_request jsonb)
returns jsonb language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_upload public.weekly_source_uploads%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_profile public.weekly_source_format_profiles%rowtype;
  v_publication public.weekly_source_projection_publications%rowtype;
  v_guard jsonb;
begin
  perform private.weekly_source_query_require_service_v1();
  perform pg_catalog.set_config('lock_timeout','5s',true);
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
    or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key where key not in
      ('actor_user_id','upload_id','projection_publication_id','expected_authority_scope_version','expected_row_manifest_hash')) then
    raise exception 'WEEKLY_SOURCE_PREPARATION_REQUEST_INVALID' using errcode='22023';
  end if;
  v_actor:=(p_request->>'actor_user_id')::uuid;
  select * into strict v_upload from public.weekly_source_uploads where id=(p_request->>'upload_id')::uuid;
  -- Same scope lock order as publication and finalisation; preparation cannot
  -- race a replacement into reviving a superseded file.
  select * into strict v_cycle from public.weekly_source_cycles where id=v_upload.source_cycle_id for update;
  select * into strict v_upload from public.weekly_source_uploads where id=v_upload.id for share;
  select * into strict v_profile from public.weekly_source_format_profiles where id=v_upload.source_format_profile_id;
  select * into strict v_publication from public.weekly_source_projection_publications
    where id=(p_request->>'projection_publication_id')::uuid for share;
  perform private.weekly_source_office_authority_v1(v_actor,'FINALISE_WEEK',v_cycle.source_group_id,
    nullif(v_upload.file_metadata_json->>'client_id','')::uuid,v_cycle.finalisation_week_ending);
  if v_upload.state<>'CURRENT' or v_upload.purpose<>'ORDINARY'
    or v_cycle.state in ('FINALISING','FINALISED')
    or v_profile.profile_code not in ('HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1',
      'HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1','ROSTER_WEEKLY_SUMMARY_ACTUAL_V1')
    or v_upload.coverage_state<>'COMPLETE'
    or v_upload.row_manifest_hash is distinct from private.weekly_source_hex32_v1(
      p_request->>'expected_row_manifest_hash','WEEKLY_SOURCE_ROW_MANIFEST_HASH_REQUIRED') then
    raise exception 'WEEKLY_SOURCE_IMPORT_NOT_PREPARABLE' using errcode='55000';
  end if;
  v_guard:=private.weekly_source_current_publication_guard_v1(v_cycle.id,
    v_publication.authority_scope_kind,v_publication.report_scope_id,v_upload.id,v_publication.id,
    (p_request->>'expected_authority_scope_version')::bigint);
  insert into private.weekly_source_import_preparations(upload_id,projection_publication_id,
    authority_scope_version,prepared_by_user_id,row_manifest_hash)
  values(v_upload.id,v_publication.id,v_publication.authority_scope_version,v_actor,v_upload.row_manifest_hash)
  on conflict(upload_id) do nothing;
  return pg_catalog.jsonb_build_object('ok',true,'status','PREPARED','upload_id',v_upload.id);
end;
$function$;

alter function private.weekly_source_import_is_prepared_v1(uuid) owner to postgres;
alter function public.weekly_source_import_prepare_atomic_v1(jsonb) owner to postgres;
revoke all on function private.weekly_source_import_is_prepared_v1(uuid) from public,anon,authenticated,service_role;
revoke all on function public.weekly_source_import_prepare_atomic_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_source_import_prepare_atomic_v1(jsonb) to service_role;

notify pgrst, 'reload schema';

commit;
