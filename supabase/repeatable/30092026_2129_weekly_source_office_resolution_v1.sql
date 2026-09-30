-- Repeatable CloudTMS function/view authority: weekly_source_office_resolution_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

create or replace function public.weekly_source_office_recheck_begin_v1(p_request jsonb)
returns jsonb language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_request_id uuid;
  v_hash bytea;
  v_upload public.weekly_source_uploads%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_scope public.weekly_source_report_scopes%rowtype;
  v_publication public.weekly_source_projection_publications%rowtype;
  v_prior private.weekly_source_office_rechecks%rowtype;
  v_row public.weekly_source_upload_rows%rowtype;
  v_choice private.weekly_source_office_row_choices%rowtype;
  v_resolution public.weekly_source_row_resolutions%rowtype;
  v_candidate uuid;
  v_client uuid;
  v_contract uuid;
  v_version bigint;
  v_current_publication uuid;
  v_current_upload uuid;
  v_begin jsonb;
begin
  perform private.weekly_source_query_require_service_v1();
  perform pg_catalog.set_config('lock_timeout','5s',true);
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
    or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key where key not in
      ('actor_user_id','request_id','upload_id','projection_publication_id',
       'expected_authority_scope_version','expected_row_manifest_hash','upload_row_id',
       'candidate_id','client_id','contract_id')) then
    raise exception 'WEEKLY_SOURCE_RECHECK_REQUEST_INVALID' using errcode='22023';
  end if;
  v_actor:=(p_request->>'actor_user_id')::uuid;
  v_request_id:=(p_request->>'request_id')::uuid;
  if v_actor is null or v_request_id is null then
    raise exception 'WEEKLY_SOURCE_RECHECK_REQUEST_INVALID' using errcode='22023';
  end if;
  v_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_OFFICE_RECHECK_V1',p_request);
  select * into strict v_upload from public.weekly_source_uploads where id=(p_request->>'upload_id')::uuid;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    pg_catalog.encode(v_upload.declared_scope_fingerprint,'hex'),73241837));
  select * into strict v_cycle from public.weekly_source_cycles where id=v_upload.source_cycle_id for update;
  select * into strict v_upload from public.weekly_source_uploads where id=v_upload.id;
  if v_upload.report_scope_id is null then
    v_version:=v_cycle.version; v_current_upload:=v_cycle.current_complete_upload_id;
    v_current_publication:=v_cycle.current_projection_publication_id;
  else
    select * into strict v_scope from public.weekly_source_report_scopes where id=v_upload.report_scope_id for update;
    v_version:=v_scope.version; v_current_upload:=v_scope.current_complete_upload_id;
    v_current_publication:=v_scope.current_projection_publication_id;
  end if;
  perform private.weekly_source_office_authority_v1(v_actor,'RECHECK_SOURCE',v_cycle.source_group_id,
    coalesce(v_scope.client_id,nullif(v_upload.file_metadata_json->>'client_id','')::uuid),v_cycle.finalisation_week_ending);
  if v_upload.purpose<>'ORDINARY' or v_upload.state<>'CURRENT'
    or v_current_upload is distinct from v_upload.id or v_cycle.state in ('FINALISING','FINALISED')
    or v_scope.state in ('FINALISING','FINALISED') then
    raise exception 'WEEKLY_SOURCE_RECHECK_NOT_CURRENT' using errcode='55000';
  end if;
  select * into v_prior from private.weekly_source_office_rechecks where request_id=v_request_id;
  if found then
    if v_prior.request_hash is distinct from v_hash or v_prior.actor_user_id<>v_actor then
      raise exception 'WEEKLY_SOURCE_RECHECK_REPLAY_CONFLICT' using errcode='23505';
    end if;
    select * into strict v_publication from public.weekly_source_projection_publications where id=v_prior.publication_id;
    if v_publication.authority_scope_version<>v_version or v_publication.state not in ('BUILDING','CURRENT') then
      raise exception 'WEEKLY_SOURCE_RECHECK_NOT_CURRENT' using errcode='55000';
    end if;
    return pg_catalog.jsonb_build_object('ok',true,'status',v_publication.state,
      'publication_id',v_publication.id,'upload_id',v_upload.id,'authority_scope_version',v_version,'idempotent',true);
  end if;
  if v_version is distinct from (p_request->>'expected_authority_scope_version')::bigint
    or v_current_publication is distinct from (p_request->>'projection_publication_id')::uuid
    or v_upload.row_manifest_hash is distinct from private.weekly_source_hex32_v1(
      p_request->>'expected_row_manifest_hash','WEEKLY_SOURCE_ROW_MANIFEST_HASH_REQUIRED') then
    raise exception 'WEEKLY_SOURCE_PREVIEW_STALE' using errcode='40001';
  end if;
  select * into strict v_publication from public.weekly_source_projection_publications where id=v_current_publication for update;
  if v_publication.state<>'CURRENT' or v_publication.upload_id<>v_upload.id then
    raise exception 'WEEKLY_SOURCE_PREVIEW_STALE' using errcode='40001';
  end if;
  if p_request ?| array['candidate_id','client_id','contract_id'] then
    select * into strict v_row from public.weekly_source_upload_rows
      where id=(p_request->>'upload_row_id')::uuid and upload_id=v_upload.id;
    select * into v_choice from private.weekly_source_office_row_choices
      where upload_row_id=v_row.id order by id desc limit 1;
    select * into v_resolution from public.weekly_source_row_resolutions
      where upload_row_id=v_row.id order by generation desc,id desc limit 1;
    v_candidate:=coalesce((p_request->>'candidate_id')::uuid,v_choice.candidate_id,v_resolution.candidate_id);
    v_client:=coalesce((p_request->>'client_id')::uuid,v_choice.client_id,v_resolution.client_id);
    v_contract:=case when p_request ? 'contract_id' then (p_request->>'contract_id')::uuid
      when p_request ?| array['candidate_id','client_id'] then null else v_choice.contract_id end;
    if v_candidate is not null and not exists(select 1 from public.candidates where id=v_candidate and active) then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_INACTIVE_OR_MISSING' using errcode='22023';
    end if;
    if v_client is not null and not exists(select 1 from public.weekly_source_group_clients
      where source_group_id=v_cycle.source_group_id and client_id=v_client
        and v_row.work_date between valid_from and coalesce(valid_to,'infinity'::date)) then
      raise exception 'WEEKLY_SOURCE_CLIENT_NOT_ELIGIBLE' using errcode='22023';
    end if;
    if v_scope.client_id is not null and v_client is distinct from v_scope.client_id then
      raise exception 'WEEKLY_SOURCE_CLIENT_NOT_ELIGIBLE' using errcode='22023';
    end if;
    if v_contract is not null and not exists(select 1 from public.contracts
      where id=v_contract and candidate_id=v_candidate and client_id=v_client
        and v_row.work_date between start_date and coalesce(end_date,'infinity'::date)) then
      raise exception 'WEEKLY_SOURCE_CONTRACT_NOT_ELIGIBLE' using errcode='22023';
    end if;
    insert into private.weekly_source_office_row_choices(upload_row_id,candidate_id,client_id,contract_id,actor_user_id)
      values(v_row.id,v_candidate,v_client,v_contract,v_actor);
  elsif p_request ? 'upload_row_id' then
    raise exception 'WEEKLY_SOURCE_RECHECK_REQUEST_INVALID' using errcode='22023';
  end if;
  update public.weekly_source_projection_publications set state='STALE' where id=v_publication.id;
  if v_upload.report_scope_id is null then
    update public.weekly_source_cycles set version=version+1,projection_state='REBUILDING',current_projection_publication_id=null
      where id=v_cycle.id returning version into v_version;
  else
    update public.weekly_source_report_scopes set version=version+1,projection_state='REBUILDING',current_projection_publication_id=null,
      updated_at_utc=pg_catalog.transaction_timestamp() where id=v_scope.id returning version into v_version;
  end if;
  v_begin:=public.weekly_source_projection_begin_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id',v_actor,'upload_id',v_upload.id,'expected_authority_scope_version',v_version));
  if v_begin->>'status'<>'BUILDING' then
    raise exception 'WEEKLY_SOURCE_RECHECK_BEGIN_FAILED' using errcode='55000';
  end if;
  -- This is a new comparison of an existing immutable upload, not its first
  -- publication. Keep earlier resolutions as history and explicitly identify
  -- the new generation; the publisher must census this generation only.
  if v_version>2147483647 then
    raise exception 'WEEKLY_SOURCE_PROJECTION_GENERATION_OVERFLOW' using errcode='22003';
  end if;
  update public.weekly_source_projection_publications
    set projection_generation=v_version::integer
    where id=(v_begin->>'publication_id')::uuid and state='BUILDING';
  v_begin:=v_begin||pg_catalog.jsonb_build_object('projection_generation',v_version::integer);
  insert into private.weekly_source_office_rechecks(request_id,actor_user_id,upload_id,prior_publication_id,publication_id,request_hash,request_json)
    values(v_request_id,v_actor,v_upload.id,v_publication.id,(v_begin->>'publication_id')::uuid,v_hash,p_request);
  return v_begin;
end;
$function$;
alter function public.weekly_source_office_recheck_begin_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_office_recheck_begin_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_source_office_recheck_begin_v1(jsonb) to service_role;

notify pgrst, 'reload schema';

commit;
