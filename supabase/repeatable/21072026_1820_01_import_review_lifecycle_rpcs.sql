-- Durable review lifecycle. All application calls are service-role-only.

create or replace function public.import_review_contract_version_get_v1()
returns jsonb
language sql
stable
security definer
set search_path to 'public', 'pg_temp'
as $function$
  select jsonb_build_object(
    'ok',true,
    'schema_contract_version','IMPORT_REVIEW_DB_V1',
    'apply_envelope_version','IMPORT_REVIEW_APPLY_V1',
    'apply_operation_version','IMPORT_APPLY_OPERATION_V2',
    'correction_operation_version','IMPORT_CORRECTION_OPERATION_V2',
    'follow_up_component_version','IMPORT_REVIEW_FOLLOW_UP_COMPONENT_V1',
    'tsfin_follow_up_settlement_version','IMPORT_REVIEW_TSFIN_SETTLEMENT_V1',
    'incremental_apply_version','IMPORT_REVIEW_INCREMENTAL_APPLY_V1',
    'review_ui_contract_version','IMPORT_REVIEW_UI_V6',
    'email_grouping_version','TIMESHEET_QUERY_RECIPIENT_EMAIL_V1',
    'legacy_contracts_supported',false
  )
$function$;

create or replace function public._import_review_create_core_v2(
  p_import_id uuid,
  p_coverage_mode text,
  p_coverage_start_date date,
  p_coverage_end_date date,
  p_scope_clients jsonb default '[]'::jsonb,
  p_scope_candidates jsonb default '[]'::jsonb,
  p_expected_source_file_sha256 text default null,
  p_expected_parser_version text default null,
  p_actor_user_id uuid default null,
  p_operation_key text default null,
  p_supersede_import_id uuid default null,
  p_expected_supersede_state_version bigint default null
)
returns jsonb
language plpgsql
security definer
set search_path to 'public', 'extensions', 'pg_temp'
as $function$
declare
  v_import public.hr_imports%rowtype;
  v_mode text:=upper(btrim(coalesce(p_coverage_mode,'')));
  v_clients jsonb:=coalesce(p_scope_clients,'[]'::jsonb);
  v_candidates jsonb:=coalesce(p_scope_candidates,'[]'::jsonb);
  v_operation_key text:=btrim(coalesce(p_operation_key,''));
  v_request_hash text; v_fingerprint text; v_revision_group uuid; v_revision_no integer;
  v_existing public.hr_imports%rowtype; v_state public.import_review_states%rowtype;
  v_supersede_import public.hr_imports%rowtype; v_supersede_state public.import_review_states%rowtype;
  v_refresh jsonb; v_overlap jsonb;
begin
  perform public._import_review_assert_actor_v1(p_actor_user_id);
  if p_import_id is null or v_mode not in ('COMPLETE_ALL','COMPLETE_SELECTED_CANDIDATES','PARTIAL')
     or p_coverage_start_date is null or p_coverage_end_date is null
     or p_coverage_start_date>p_coverage_end_date
     or p_coverage_end_date-p_coverage_start_date>366 then
    raise exception 'IMPORT_REVIEW_COVERAGE_INVALID' using errcode='22023';
  end if;
  if length(v_operation_key)<16 or length(v_operation_key)>256 then
    raise exception 'IMPORT_REVIEW_OPERATION_KEY_INVALID' using errcode='22023';
  end if;
  if jsonb_typeof(v_clients)<>'array' or jsonb_typeof(v_candidates)<>'array'
     or jsonb_array_length(v_clients)>100 or jsonb_array_length(v_candidates)>500
     or pg_column_size(v_clients)+pg_column_size(v_candidates)>262144 then
    raise exception 'IMPORT_REVIEW_SCOPE_LIMIT_EXCEEDED' using errcode='22023';
  end if;
  if v_mode='COMPLETE_SELECTED_CANDIDATES' and jsonb_array_length(v_candidates)=0 then
    raise exception 'IMPORT_REVIEW_SELECTED_CANDIDATES_REQUIRED' using errcode='22023';
  end if;
  if v_mode<>'COMPLETE_SELECTED_CANDIDATES' and jsonb_array_length(v_candidates)>0 then
    raise exception 'IMPORT_REVIEW_CANDIDATES_NOT_ALLOWED_FOR_MODE' using errcode='22023';
  end if;
  if exists(select 1 from jsonb_array_elements(v_clients) x where jsonb_typeof(x)<>'object'
    or nullif(btrim(x->>'source_client_key'),'') is null or length(btrim(x->>'source_client_key'))>512
    or length(coalesce(x->>'source_display_label',''))>512)
    or exists(select 1 from jsonb_array_elements(v_candidates) x where jsonb_typeof(x)<>'object'
    or nullif(btrim(x->>'source_candidate_key'),'') is null or length(btrim(x->>'source_candidate_key'))>512
    or length(coalesce(x->>'source_display_label',''))>512) then
    raise exception 'IMPORT_REVIEW_SCOPE_ITEM_INVALID' using errcode='22023';
  end if;
  if (select count(*) from jsonb_array_elements(v_clients))<>(select count(distinct btrim(x->>'source_client_key')) from jsonb_array_elements(v_clients)x)
    or (select count(*) from jsonb_array_elements(v_candidates))<>(select count(distinct btrim(x->>'source_candidate_key')) from jsonb_array_elements(v_candidates)x) then
    raise exception 'IMPORT_REVIEW_SCOPE_ITEM_DUPLICATE' using errcode='22023';
  end if;

  v_request_hash:=public._import_review_hash_v1((jsonb_build_object(
    'import_id',p_import_id,'coverage_mode',v_mode,'coverage_start_date',p_coverage_start_date,
    'coverage_end_date',p_coverage_end_date,'clients',(select coalesce(jsonb_agg(x order by x->>'source_client_key'),'[]') from jsonb_array_elements(v_clients)x),
    'candidates',(select coalesce(jsonb_agg(x order by x->>'source_candidate_key'),'[]') from jsonb_array_elements(v_candidates)x),
    'source_file_sha256',lower(btrim(coalesce(p_expected_source_file_sha256,''))),
    'parser_version',btrim(coalesce(p_expected_parser_version,'')))
    || case when p_supersede_import_id is null then '{}'::jsonb else jsonb_build_object(
      'supersede_import_id',p_supersede_import_id,
      'expected_supersede_state_version',p_expected_supersede_state_version) end)::text);

  select * into v_existing from public.hr_imports where coverage_operation_key=v_operation_key for update;
  if found then
    if v_existing.id<>p_import_id or v_existing.coverage_request_hash is distinct from v_request_hash then
      raise exception 'IMPORT_REVIEW_OPERATION_KEY_CONFLICT' using errcode='23505';
    end if;
    select * into v_state from public.import_review_states where import_id=p_import_id;
    return jsonb_build_object('ok',true,'schema_contract_version',v_state.schema_contract_version,
      'replay',true,'import_id',p_import_id,'status',v_state.status,
      'state_version',v_state.state_version,'preview_generation',v_state.preview_generation,
      'coverage_fingerprint',v_existing.coverage_fingerprint,'preview_fingerprint',v_state.preview_fingerprint);
  end if;

  select * into v_import from public.hr_imports where id=p_import_id for update;
  if not found then raise exception 'IMPORT_REVIEW_IMPORT_NOT_FOUND' using errcode='P0002'; end if;
  if v_import.applied_at is not null then raise exception 'IMPORT_REVIEW_IMPORT_ALREADY_APPLIED' using errcode='55000'; end if;
  if v_import.pruned_at is not null then raise exception 'IMPORT_REVIEW_IMPORT_PRUNED' using errcode='55000'; end if;
  if v_import.coverage_locked_at is not null then raise exception 'IMPORT_REVIEW_ALREADY_CREATED' using errcode='55000'; end if;
  if nullif(btrim(coalesce(v_import.source_file_sha256,'')),'') is null
     or nullif(btrim(coalesce(v_import.parser_version,'')),'') is null then
    raise exception 'IMPORT_REVIEW_STAGING_EVIDENCE_REQUIRED' using errcode='55000';
  end if;
  if lower(v_import.source_file_sha256) is distinct from lower(btrim(coalesce(p_expected_source_file_sha256,''))) then
    raise exception 'IMPORT_REVIEW_SOURCE_HASH_MISMATCH' using errcode='40001';
  end if;
  if v_import.parser_version is distinct from btrim(coalesce(p_expected_parser_version,'')) then
    raise exception 'IMPORT_REVIEW_PARSER_VERSION_MISMATCH' using errcode='40001';
  end if;

  if v_import.client_id is not null and jsonb_array_length(v_clients)=0 then
    v_clients:=jsonb_build_array(jsonb_build_object('source_client_key','client:'||v_import.client_id::text,
      'source_display_label',null,'client_id',v_import.client_id));
  end if;
  if exists(select 1 from jsonb_array_elements(v_clients)x where nullif(x->>'client_id','') is not null
    and not exists(select 1 from public.clients c where c.id=(x->>'client_id')::uuid))
    or exists(select 1 from jsonb_array_elements(v_candidates)x where nullif(x->>'candidate_id','') is not null
    and not exists(select 1 from public.candidates c where c.id=(x->>'candidate_id')::uuid and c.active)) then
    raise exception 'IMPORT_REVIEW_SCOPE_RESOLUTION_INVALID' using errcode='22023';
  end if;

  -- Serialize overlap decisions per route/scope. NHSP is a cross-client feed;
  -- HealthRoster is scoped by its immutable client list and Weekly/Daily route.
  perform pg_advisory_xact_lock(hashtextextended(concat_ws('|','IMPORT_REVIEW_OVERLAP',
    v_import.source_system::text,upper(coalesce(v_import.import_scope,v_import.source_system::text)),
    case when v_import.source_system='NHSP'::public.hr_source_enum then 'ALL_CLIENTS'
      else coalesce((select string_agg(coalesce(nullif(x->>'client_id',''),x->>'source_client_key'),','
        order by coalesce(nullif(x->>'client_id',''),x->>'source_client_key')) from jsonb_array_elements(v_clients)x),'NO_CLIENT') end),
    22072026));

  v_overlap:=public._import_review_overlap_preflight_core_v2(
    p_import_id,v_import.source_system,coalesce(v_import.import_scope,v_import.source_system::text),
    p_coverage_start_date,p_coverage_end_date,v_clients);

  if p_supersede_import_id is null then
    if jsonb_array_length(v_overlap)>0 then
      raise exception 'IMPORT_REVIEW_OVERLAP_CONFLICT' using errcode='55000',
        detail=jsonb_build_object('overlapping_unfinished_reviews',v_overlap)::text;
    end if;
  else
    if p_expected_supersede_state_version is null then
      raise exception 'IMPORT_REVIEW_REPLACE_VERSION_REQUIRED' using errcode='22023';
    end if;
    if jsonb_array_length(v_overlap)<>1
       or (v_overlap->0->>'import_id')::uuid is distinct from p_supersede_import_id then
      raise exception 'IMPORT_REVIEW_REPLACE_TARGET_CONFLICT' using errcode='40001',
        detail=jsonb_build_object('overlapping_unfinished_reviews',v_overlap)::text;
    end if;
    select * into v_supersede_import from public.hr_imports where id=p_supersede_import_id for update;
    select * into v_supersede_state from public.import_review_states where import_id=p_supersede_import_id for update;
    if v_supersede_import.id is null or v_supersede_state.import_id is null then
      raise exception 'IMPORT_REVIEW_REPLACE_TARGET_NOT_FOUND' using errcode='P0002';
    end if;
    if v_supersede_state.state_version<>p_expected_supersede_state_version then
      raise exception 'IMPORT_REVIEW_VERSION_CONFLICT' using errcode='40001',detail=v_supersede_state.state_version::text;
    end if;
    if v_supersede_state.status='APPLYING' then
      raise exception 'IMPORT_REVIEW_REPLACE_TARGET_APPLYING' using errcode='55000';
    end if;
    if v_supersede_state.status not in ('STAGED','IN_REVIEW','BLOCKED','READY') then
      raise exception 'IMPORT_REVIEW_REPLACE_NOT_ALLOWED' using errcode='55000';
    end if;
  end if;

  v_revision_group:=case when p_supersede_import_id is not null
    then coalesce(v_supersede_import.revision_group_id,v_supersede_import.id)
    else coalesce(v_import.revision_group_id,p_import_id) end;
  if p_supersede_import_id is null and v_import.revision_no is not null then v_revision_no:=v_import.revision_no;
  else select coalesce(max(hi.revision_no),0)+1 into v_revision_no from public.hr_imports hi where hi.revision_group_id=v_revision_group; end if;
  v_fingerprint:=public._import_review_hash_v1(jsonb_build_object('schema','IMPORT_REVIEW_COVERAGE_V1',
    'route',coalesce(v_import.import_scope,v_import.source_system::text),'mode',v_mode,
    'from',p_coverage_start_date,'to',p_coverage_end_date,
    'clients',(select coalesce(jsonb_agg(jsonb_build_object('key',btrim(x->>'source_client_key'),'client_id',x->>'client_id') order by x->>'source_client_key'),'[]') from jsonb_array_elements(v_clients)x),
    'candidates',(select coalesce(jsonb_agg(jsonb_build_object('key',btrim(x->>'source_candidate_key'),'candidate_id',x->>'candidate_id') order by x->>'source_candidate_key'),'[]') from jsonb_array_elements(v_candidates)x))::text);

  update public.hr_imports set revision_group_id=v_revision_group,revision_no=v_revision_no,
    supersedes_import_id=p_supersede_import_id,
    coverage_mode=v_mode,coverage_start_date=p_coverage_start_date,coverage_end_date=p_coverage_end_date,
    coverage_fingerprint=v_fingerprint,coverage_confirmed_by=p_actor_user_id,
    coverage_operation_key=v_operation_key,coverage_request_hash=v_request_hash
  where id=p_import_id;
  insert into public.import_review_scope_clients(import_id,source_client_key,source_display_label,client_id,created_by_user_id,resolved_at_utc,resolved_by_user_id)
  select p_import_id,btrim(x->>'source_client_key'),nullif(btrim(x->>'source_display_label'),''),nullif(x->>'client_id','')::uuid,p_actor_user_id,
    case when nullif(x->>'client_id','') is not null then now() end,case when nullif(x->>'client_id','') is not null then p_actor_user_id end
  from jsonb_array_elements(v_clients)x;
  insert into public.import_review_scope_candidates(import_id,source_candidate_key,source_display_label,candidate_id,created_by_user_id,resolved_at_utc,resolved_by_user_id)
  select p_import_id,btrim(x->>'source_candidate_key'),nullif(btrim(x->>'source_display_label'),''),nullif(x->>'candidate_id','')::uuid,p_actor_user_id,
    case when nullif(x->>'candidate_id','') is not null then now() end,case when nullif(x->>'candidate_id','') is not null then p_actor_user_id end
  from jsonb_array_elements(v_candidates)x;
  update public.hr_imports set coverage_locked_at=now() where id=p_import_id;
  insert into public.import_review_states(import_id,created_by_user_id,updated_by_user_id) values(p_import_id,p_actor_user_id,p_actor_user_id);
  insert into public.import_review_events(import_id,state_version,event_code,actor_user_id,event_context_json)
  values(p_import_id,1,'REVIEW_CREATED',p_actor_user_id,jsonb_build_object('coverage_mode',v_mode,'coverage_fingerprint',v_fingerprint,
    'coverage_start_date',p_coverage_start_date,'coverage_end_date',p_coverage_end_date,'operation_key_hash',public._import_review_hash_v1(v_operation_key)));

  if p_supersede_import_id is not null then
    update public.import_review_states set status='SUPERSEDED',state_version=state_version+1,
      superseded_at_utc=now(),superseded_by_user_id=p_actor_user_id,
      updated_at_utc=now(),updated_by_user_id=p_actor_user_id
    where import_id=p_supersede_import_id returning * into v_supersede_state;
    insert into public.import_review_events(import_id,state_version,event_code,actor_user_id,event_context_json)
    values(p_supersede_import_id,v_supersede_state.state_version,'REVIEW_SUPERSEDED',p_actor_user_id,
      jsonb_build_object('new_import_id',p_import_id,'atomic_replace',true));
    insert into public.import_review_events(import_id,state_version,event_code,actor_user_id,event_context_json)
    values(p_import_id,1,'REVIEW_SUPERSEDES_PRIOR',p_actor_user_id,
      jsonb_build_object('old_import_id',p_supersede_import_id,'revision_group_id',v_revision_group,
        'revision_no',v_revision_no,'atomic_replace',true));
  end if;
  v_refresh:=public._import_review_refresh_core_v1(p_import_id,1,p_actor_user_id,5000);
  return v_refresh||jsonb_build_object('schema_contract_version','IMPORT_REVIEW_DB_V1',
    'replay',false,'coverage_fingerprint',v_fingerprint,'overlapping_unfinished_reviews','[]'::jsonb,
    'superseded_import_id',p_supersede_import_id,
    'superseded_state_version',case when p_supersede_import_id is not null then v_supersede_state.state_version end);
end
$function$;

revoke all on function public._import_review_create_core_v2(uuid,text,date,date,jsonb,jsonb,text,text,uuid,text,uuid,bigint)
  from public,anon,authenticated,service_role;

create or replace function public.import_review_create_v1(
  p_import_id uuid,p_coverage_mode text,p_coverage_start_date date,p_coverage_end_date date,
  p_scope_clients jsonb default '[]'::jsonb,p_scope_candidates jsonb default '[]'::jsonb,
  p_expected_source_file_sha256 text default null,p_expected_parser_version text default null,
  p_actor_user_id uuid default null,p_operation_key text default null
)
returns jsonb language plpgsql security definer set search_path to 'public','pg_temp' as $function$
begin
  return public._import_review_create_core_v2(p_import_id,p_coverage_mode,p_coverage_start_date,p_coverage_end_date,
    p_scope_clients,p_scope_candidates,p_expected_source_file_sha256,p_expected_parser_version,
    p_actor_user_id,p_operation_key,null,null);
end $function$;

create or replace function public.import_review_replace_v1(
  p_import_id uuid,p_coverage_mode text,p_coverage_start_date date,p_coverage_end_date date,
  p_scope_clients jsonb,p_scope_candidates jsonb,p_expected_source_file_sha256 text,
  p_expected_parser_version text,p_actor_user_id uuid,p_operation_key text,
  p_supersede_import_id uuid,p_expected_supersede_state_version bigint
)
returns jsonb language plpgsql security definer set search_path to 'public','pg_temp' as $function$
begin
  return public._import_review_create_core_v2(p_import_id,p_coverage_mode,p_coverage_start_date,p_coverage_end_date,
    p_scope_clients,p_scope_candidates,p_expected_source_file_sha256,p_expected_parser_version,
    p_actor_user_id,p_operation_key,p_supersede_import_id,p_expected_supersede_state_version);
end $function$;

create or replace function public.import_review_list_v1(
  p_status_class text default 'ACTIVE',p_source_route text default null,p_client_id uuid default null,
  p_date_from date default null,p_date_to date default null,p_cursor_updated_at timestamptz default null,
  p_cursor_import_id uuid default null,p_page_size integer default 50
)
returns jsonb language plpgsql stable security definer
set search_path to 'public','pg_temp' as $function$
declare v_limit integer:=least(greatest(coalesce(p_page_size,50),1),100); v_items jsonb;
  v_last_updated_at timestamptz; v_last_import_id uuid; v_has_more boolean:=false;
begin
  if p_date_from is not null and p_date_to is not null and p_date_from>p_date_to then raise exception 'IMPORT_REVIEW_DATE_FILTER_INVALID' using errcode='22023'; end if;
  if upper(coalesce(p_status_class,'ACTIVE')) not in ('ACTIVE','COMPLETED','ABANDONED','SUPERSEDED','ALL') then raise exception 'IMPORT_REVIEW_STATUS_FILTER_INVALID' using errcode='22023'; end if;
  if length(coalesce(p_source_route,''))>128 then raise exception 'IMPORT_REVIEW_ROUTE_FILTER_INVALID' using errcode='22023'; end if;
  with page as (
    select s.*,hi.filename,hi.source_system,hi.import_scope,hi.coverage_mode,hi.coverage_start_date,hi.coverage_end_date,
      hi.coverage_fingerprint,hi.source_file_sha256,
      (select count(*) from public.import_review_decisions d where d.import_id=s.import_id and d.is_current and d.blocking) blocker_count,
      (select count(*) from public.import_review_decisions d where d.import_id=s.import_id and d.is_current and d.selected) selected_count,
      (select count(*) from public.import_review_decisions d where d.import_id=s.import_id and d.is_current and d.selected and d.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')) selected_email_count
    from public.import_review_states s join public.hr_imports hi on hi.id=s.import_id
    where (case upper(coalesce(p_status_class,'ACTIVE')) when 'ACTIVE' then s.status in ('STAGED','IN_REVIEW','BLOCKED','READY','APPLYING')
      when 'COMPLETED' then s.status='APPLIED' when 'ABANDONED' then s.status='ABANDONED' when 'SUPERSEDED' then s.status='SUPERSEDED' else true end)
      and (p_source_route is null or upper(coalesce(hi.import_scope,hi.source_system::text))=upper(p_source_route))
      and (p_client_id is null or hi.client_id=p_client_id or exists(select 1 from public.import_review_scope_clients sc where sc.import_id=hi.id and sc.client_id=p_client_id))
      and (p_date_from is null or hi.coverage_end_date>=p_date_from) and (p_date_to is null or hi.coverage_start_date<=p_date_to)
      and (p_cursor_updated_at is null or (s.updated_at_utc,s.import_id)<(p_cursor_updated_at,coalesce(p_cursor_import_id,'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid)))
    order by s.updated_at_utc desc,s.import_id desc limit v_limit+1
  ), limited as (select * from page order by updated_at_utc desc,import_id desc limit v_limit)
  select coalesce(jsonb_agg(jsonb_build_object('schema_contract_version',schema_contract_version,
    'import_id',import_id,'filename',filename,'source_system',source_system,'source_route',import_scope,
    'source_hash_prefix',left(source_file_sha256,12),'coverage_mode',coverage_mode,'coverage_start_date',coverage_start_date,'coverage_end_date',coverage_end_date,
    'status',status,'follow_up_status',follow_up_status,'state_version',state_version,'preview_generation',preview_generation,
    'blocker_count',blocker_count,'selected_count',selected_count,'selected_email_count',selected_email_count,
    'read_only',status in ('APPLYING','APPLIED','ABANDONED','SUPERSEDED'),'updated_at_utc',updated_at_utc) order by updated_at_utc desc,import_id desc),'[]'),
    (select count(*)>v_limit from page) into v_items,v_has_more from limited;
  if v_has_more and jsonb_array_length(v_items)>0 then
    select updated_at_utc,import_id into v_last_updated_at,v_last_import_id from public.import_review_states
    where import_id=(v_items->(jsonb_array_length(v_items)-1)->>'import_id')::uuid;
  end if;
  return jsonb_build_object('ok',true,'items',v_items,'page_size',v_limit,'next_cursor',case when v_has_more then jsonb_build_object('updated_at_utc',v_last_updated_at,'import_id',v_last_import_id) end);
end $function$;

create or replace function public.import_review_get_v1(
  p_import_id uuid,p_actor_user_id uuid default null,p_action_cursor text default null,p_action_limit integer default 100,
  p_event_cursor bigint default null,p_event_limit integer default 50
)
returns jsonb language plpgsql security definer set search_path to 'public','pg_temp' as $function$
declare v_action_limit integer:=least(greatest(coalesce(p_action_limit,100),1),200); v_event_limit integer:=least(greatest(coalesce(p_event_limit,50),1),100);
  v_state public.import_review_states%rowtype; v_import public.hr_imports%rowtype; v_actions jsonb; v_events jsonb; v_last_action text; v_last_event bigint;
  v_apply_envelope jsonb; v_apply_request_hash text; v_allowed_commands jsonb; v_confirmation jsonb; v_read_only boolean;
  v_can_apply boolean; v_batch_ids text[]; v_applied_outcome_count integer; v_deferred_count integer;
begin
  perform public._import_review_assert_actor_v1(p_actor_user_id);
  if (p_action_cursor is not null and (length(p_action_cursor)<>64 or p_action_cursor!~'^[0-9a-f]{64}$'))
    or (p_event_cursor is not null and p_event_cursor<0) then raise exception 'IMPORT_REVIEW_CURSOR_INVALID' using errcode='22023'; end if;
  update public.import_review_states set last_opened_at_utc=now(),last_opened_by_user_id=p_actor_user_id
  where import_id=p_import_id returning * into v_state;
  if not found then raise exception 'IMPORT_REVIEW_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_import from public.hr_imports where id=p_import_id;
  v_apply_envelope:=public._import_review_apply_envelope_core_v1(p_import_id);
  v_apply_request_hash:=public._import_review_hash_v1(v_apply_envelope::text);
  select coalesce(array_agg(value order by value),array[]::text[]) into v_batch_ids
  from jsonb_array_elements_text(coalesce(v_apply_envelope->'selected_action_ids','[]'::jsonb)) value;
  select count(*) into v_applied_outcome_count from public.import_review_action_outcomes o where o.import_id=p_import_id;
  select count(*) into v_deferred_count from public.import_review_decisions d
  where d.import_id=p_import_id and d.is_current and d.selectable and not d.selected;
  v_read_only:=v_state.status in ('APPLYING','APPLIED','ABANDONED','SUPERSEDED');
  v_can_apply:=v_state.status in ('BLOCKED','READY') and cardinality(v_batch_ids)>0
    and v_state.follow_up_status in ('NOT_REQUIRED','COMPLETE');
  v_allowed_commands:=to_jsonb(array_remove(array[
    case when v_state.status in ('STAGED','IN_REVIEW','BLOCKED','READY') then 'SAVE_SELECTIONS'::text end,
    case when v_state.status in ('STAGED','IN_REVIEW','BLOCKED','READY') then 'REFRESH'::text end,
    case when v_state.status in ('STAGED','IN_REVIEW','BLOCKED','READY') then 'ABANDON'::text end,
    case when v_state.status in ('STAGED','IN_REVIEW','BLOCKED','READY') then 'RESOLVE_DAILY_TIMESHEET'::text end,
    case when v_can_apply then 'APPLY'::text end,
    case when v_state.status in ('APPLYING','APPLIED') or v_state.last_operation_id is not null then 'VIEW_APPLY_STATUS'::text end,
    case when v_state.status in ('BLOCKED','READY','APPLIED') and v_state.follow_up_status='FAILED_RETRYABLE' then 'RETRY_FOLLOW_UP'::text end
  ],null));
  select jsonb_build_object(
    'selected_total',count(*) filter(where d.action_id=any(v_batch_ids)),
    'selected_change_count',count(*) filter(where d.action_id=any(v_batch_ids) and d.action_kind in ('INCLUDE_SHIFT','APPLY_AMENDMENT','APPLY_CANCELLATION','MARK_VALIDATION_ERROR')),
    'selected_email_count',count(*) filter(where d.action_id=any(v_batch_ids) and d.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')),
    'selected_email_issue_count',count(*) filter(where d.action_id=any(v_batch_ids) and d.action_kind='EMAIL_ISSUE'),
    'selected_email_reminder_count',count(*) filter(where d.action_id=any(v_batch_ids) and d.action_kind='EMAIL_REMINDER'),
    'selected_reference_invalidation_count',count(*) filter(where d.action_id=any(v_batch_ids) and d.action_kind='INVALIDATE_REFERENCE'),
    'blocking_count',count(*) filter(where d.blocking),
    'batch_blocking_count',0,
    'deferred_count',v_deferred_count,
    'applied_outcome_count',v_applied_outcome_count,
    'advisory_count',count(*) filter(where d.action_kind='ADVISORY'),
    'daily_resolution_count',count(*) filter(where d.action_kind='DAILY_TIMESHEET_RESOLUTION'),
    'reconfirmation_count',count(*) filter(where d.requires_reconfirmation),
    'action_kind_counts',coalesce((select jsonb_object_agg(k.action_kind,k.item_count) from (
      select d2.action_kind,count(*)::integer item_count from public.import_review_decisions d2
      where d2.import_id=p_import_id and d2.is_current and d2.action_id=any(v_batch_ids) group by d2.action_kind order by d2.action_kind
    ) k),'{}'::jsonb)
  ) into v_confirmation
  from public.import_review_decisions d where d.import_id=p_import_id and d.is_current;
  with p as (select * from public.import_review_decisions d where d.import_id=p_import_id and d.is_current and (p_action_cursor is null or d.action_id>p_action_cursor) order by d.action_id limit v_action_limit+1),
  l as (select * from p order by action_id limit v_action_limit)
  select coalesce(jsonb_agg(jsonb_build_object('action_id',action_id,'kind',action_kind,'category',action_category,'target_key',target_key,
    'evidence_fingerprint',evidence_fingerprint,'selectable',selectable,'default_selected',default_selected,'selected',selected,'blocking',blocking,
    'requires_reconfirmation',requires_reconfirmation,'summary',summary_json) order by action_id),'[]'),max(action_id) into v_actions,v_last_action from l;
  with p as (select * from public.import_review_events e where e.import_id=p_import_id and (p_event_cursor is null or e.id>p_event_cursor) order by e.id limit v_event_limit+1),
  l as (select * from p order by id limit v_event_limit)
  select coalesce(jsonb_agg(jsonb_build_object('id',id,'state_version',state_version,'operation_id',operation_id,'event_code',event_code,
    'actor_user_id',actor_user_id,'context',event_context_json,'created_at_utc',created_at_utc) order by id),'[]'),max(id) into v_events,v_last_event from l;
  return jsonb_build_object('ok',true,'import',jsonb_build_object('id',v_import.id,'filename',v_import.filename,'source_system',v_import.source_system,
    'source_route',v_import.import_scope,'source_file_sha256',v_import.source_file_sha256,'parser_version',v_import.parser_version,
    'coverage_mode',v_import.coverage_mode,'coverage_start_date',v_import.coverage_start_date,'coverage_end_date',v_import.coverage_end_date,
    'coverage_fingerprint',v_import.coverage_fingerprint,'revision_group_id',v_import.revision_group_id,'revision_no',v_import.revision_no,'supersedes_import_id',v_import.supersedes_import_id),
    'state',jsonb_build_object('schema_contract_version',v_state.schema_contract_version,
      'status',v_state.status,'follow_up_status',v_state.follow_up_status,'state_version',v_state.state_version,
      'follow_up_error_code',v_state.follow_up_error_code,'follow_up_error_message',v_state.follow_up_error_message,
      'follow_up_retry_count',v_state.follow_up_retry_count,
      'preview_generation',v_state.preview_generation,'preview_fingerprint',v_state.preview_fingerprint,'ui_state',v_state.ui_state_json,
      'last_opened_at_utc',v_state.last_opened_at_utc,'last_opened_by_user_id',v_state.last_opened_by_user_id,
      'last_operation_id',v_state.last_operation_id,
      'last_operation_request_hash',(select o.request_hash from public.import_apply_operations o
        where o.id=v_state.last_operation_id and o.import_id=p_import_id),
      'read_only',v_read_only,
      'partial_application',v_applied_outcome_count>0 and v_state.status<>'APPLIED',
      'applied_outcome_count',v_applied_outcome_count,'deferred_count',v_deferred_count,
      'editability',jsonb_build_object(
        'read_only',v_read_only,
        'can_edit_selections',v_state.status in ('STAGED','IN_REVIEW','BLOCKED','READY'),
        'can_resolve_daily_timesheet',v_state.status in ('STAGED','IN_REVIEW','BLOCKED','READY'),
        'can_refresh',v_state.status in ('STAGED','IN_REVIEW','BLOCKED','READY'),
        'can_abandon',v_state.status in ('STAGED','IN_REVIEW','BLOCKED','READY'),
        'can_apply',v_can_apply,
        'can_view_apply_status',v_state.status in ('APPLYING','APPLIED') or v_state.last_operation_id is not null,
        'can_retry_follow_up',v_state.status in ('BLOCKED','READY','APPLIED') and v_state.follow_up_status='FAILED_RETRYABLE',
        'allowed_commands',v_allowed_commands),
      'apply_contract',jsonb_build_object('selected_action_ids',v_apply_envelope->'selected_action_ids',
        'reference_invalidation_action_ids',v_apply_envelope->'reference_invalidation_action_ids',
        'request_envelope',v_apply_envelope,'request_hash',v_apply_request_hash)),
    'evidence',jsonb_build_object(
      'source_file_sha256',v_import.source_file_sha256,'parser_version',v_import.parser_version,
      'coverage_fingerprint',v_import.coverage_fingerprint,'preview_fingerprint',v_state.preview_fingerprint,
      'preview_generation',v_state.preview_generation),
    'confirmation_summary',v_confirmation,
    'actions',v_actions,'actions_next_cursor',case when jsonb_array_length(v_actions)=v_action_limit then v_last_action end,
    'events',v_events,'events_next_cursor',case when jsonb_array_length(v_events)=v_event_limit then v_last_event end);
end $function$;

create or replace function public.import_review_save_v1(
  p_import_id uuid,p_expected_state_version bigint,p_expected_preview_generation integer,p_expected_preview_fingerprint text,
  p_action_changes jsonb,p_ui_state_json jsonb default null,p_actor_user_id uuid default null,p_request_id uuid default null
)
returns jsonb language plpgsql security definer set search_path to 'public','extensions','pg_temp' as $function$
declare v_state public.import_review_states%rowtype; v_changes jsonb:=coalesce(p_action_changes,'[]'); v_ui jsonb; v_hash text; v_prior jsonb;
  v_changed integer; v_blockers integer; v_status text;
begin
  perform public._import_review_assert_actor_v1(p_actor_user_id);
  if p_request_id is null or jsonb_typeof(v_changes)<>'array' or jsonb_array_length(v_changes)>500 or pg_column_size(v_changes)>131072 then
    raise exception 'IMPORT_REVIEW_SAVE_INPUT_INVALID' using errcode='22023'; end if;
  if exists(select 1 from jsonb_array_elements(v_changes)x where jsonb_typeof(x)<>'object' or nullif(x->>'action_id','') is null
    or jsonb_typeof(x->'selected')<>'boolean' or (x-'action_id'-'selected')<>'{}'::jsonb)
    or (select count(*) from jsonb_array_elements(v_changes))<>(select count(distinct x->>'action_id') from jsonb_array_elements(v_changes)x) then
    raise exception 'IMPORT_REVIEW_ACTION_CHANGE_INVALID' using errcode='22023'; end if;
  v_ui:=case when p_ui_state_json is null then null else public._import_review_validate_ui_state_v1(p_ui_state_json) end;
  v_hash:=public._import_review_hash_v1(jsonb_build_object('changes',(select coalesce(jsonb_agg(x order by x->>'action_id'),'[]') from jsonb_array_elements(v_changes)x),'ui',v_ui)::text);
  select e.event_context_json into v_prior from public.import_review_events e where e.import_id=p_import_id and e.operation_id=p_request_id and e.event_code='SELECTION_SAVED' order by e.id desc limit 1;
  if found then if v_prior->>'request_hash'<>v_hash then raise exception 'IMPORT_REVIEW_SAVE_REQUEST_CONFLICT' using errcode='23505'; end if;
    return jsonb_build_object('ok',true,'schema_contract_version','IMPORT_REVIEW_DB_V1',
      'replay',true,'import_id',p_import_id,'state_version',(v_prior->>'resulting_state_version')::bigint,'status',v_prior->>'status'); end if;
  select * into v_state from public.import_review_states where import_id=p_import_id for update;
  if not found then raise exception 'IMPORT_REVIEW_NOT_FOUND' using errcode='P0002'; end if;
  if v_state.status not in ('IN_REVIEW','BLOCKED','READY','STAGED') then raise exception 'IMPORT_REVIEW_SAVE_NOT_ALLOWED' using errcode='55000'; end if;
  if v_state.state_version<>p_expected_state_version then raise exception 'IMPORT_REVIEW_VERSION_CONFLICT' using errcode='40001',detail=v_state.state_version::text; end if;
  if v_state.preview_generation<>p_expected_preview_generation or v_state.preview_fingerprint is distinct from p_expected_preview_fingerprint then
    raise exception 'IMPORT_REVIEW_PREVIEW_STALE' using errcode='40001'; end if;
  if exists(select 1 from jsonb_array_elements(v_changes)x left join public.import_review_decisions d on d.action_id=x->>'action_id' and d.import_id=p_import_id
    where d.action_id is null or not d.is_current or not d.selectable or d.preview_generation<>v_state.preview_generation) then
    raise exception 'IMPORT_REVIEW_ACTION_NOT_SELECTABLE_OR_STALE' using errcode='40001'; end if;
  update public.import_review_decisions d set selected=(x.value->>'selected')::boolean,requires_reconfirmation=false,
    selected_at_utc=now(),selected_by_user_id=p_actor_user_id
  from jsonb_array_elements(v_changes)x(value) where d.import_id=p_import_id and d.action_id=x.value->>'action_id'
    and (d.selected is distinct from (x.value->>'selected')::boolean or d.requires_reconfirmation);
  get diagnostics v_changed=row_count;
  select count(*) into v_blockers from public.import_review_decisions where import_id=p_import_id and is_current and blocking;
  v_status:=case when v_blockers>0 then 'BLOCKED' else 'READY' end;
  update public.import_review_states set status=v_status,state_version=state_version+1,ui_state_json=coalesce(v_ui,ui_state_json),
    updated_at_utc=now(),updated_by_user_id=p_actor_user_id where import_id=p_import_id returning * into v_state;
  insert into public.import_review_events(import_id,state_version,operation_id,event_code,actor_user_id,event_context_json)
  values(p_import_id,v_state.state_version,p_request_id,'SELECTION_SAVED',p_actor_user_id,jsonb_build_object('request_hash',v_hash,
    'changed_count',v_changed,'resulting_state_version',v_state.state_version,'status',v_status));
  return jsonb_build_object('ok',true,'schema_contract_version',v_state.schema_contract_version,
    'replay',false,'import_id',p_import_id,'state_version',v_state.state_version,'status',v_status,
    'preview_generation',v_state.preview_generation,'preview_fingerprint',v_state.preview_fingerprint,'changed_count',v_changed,'blocker_count',v_blockers);
end $function$;

create or replace function public.import_review_refresh_v1(p_import_id uuid,p_expected_state_version bigint,p_actor_user_id uuid default null,p_max_actions integer default 5000)
returns jsonb language plpgsql security definer
set search_path to 'public','pg_temp'
set plpgsql_check.mode to 'disabled'
as $function$
begin return public._import_review_refresh_core_v1(p_import_id,p_expected_state_version,p_actor_user_id,least(coalesce(p_max_actions,5000),5000)); end $function$;

create or replace function public.import_review_abandon_v1(p_import_id uuid,p_expected_state_version bigint,p_reason text,p_actor_user_id uuid default null)
returns jsonb language plpgsql security definer set search_path to 'public','pg_temp' as $function$
declare v public.import_review_states%rowtype;
begin perform public._import_review_assert_actor_v1(p_actor_user_id); if length(btrim(coalesce(p_reason,''))) not between 1 and 500 then raise exception 'IMPORT_REVIEW_ABANDON_REASON_INVALID' using errcode='22023'; end if;
  select * into v from public.import_review_states where import_id=p_import_id for update; if not found then raise exception 'IMPORT_REVIEW_NOT_FOUND' using errcode='P0002'; end if;
  if v.status='ABANDONED' then return jsonb_build_object('ok',true,'replay',true,'status',v.status,'state_version',v.state_version); end if;
  if v.state_version<>p_expected_state_version then raise exception 'IMPORT_REVIEW_VERSION_CONFLICT' using errcode='40001'; end if;
  if v.status not in ('STAGED','IN_REVIEW','BLOCKED','READY') then raise exception 'IMPORT_REVIEW_ABANDON_NOT_ALLOWED' using errcode='55000'; end if;
  update public.import_review_states set status='ABANDONED',state_version=state_version+1,abandoned_at_utc=now(),abandoned_by_user_id=p_actor_user_id,
    abandoned_reason=btrim(p_reason),updated_at_utc=now(),updated_by_user_id=p_actor_user_id where import_id=p_import_id returning * into v;
  insert into public.import_review_events(import_id,state_version,event_code,actor_user_id,event_context_json) values(p_import_id,v.state_version,'REVIEW_ABANDONED',p_actor_user_id,jsonb_build_object('reason',btrim(p_reason)));
  return jsonb_build_object('ok',true,'replay',false,'status',v.status,'state_version',v.state_version); end $function$;

create or replace function public.import_review_apply_guard_v1(
  p_import_id uuid,p_expected_state_version bigint,p_expected_coverage_fingerprint text,p_expected_preview_fingerprint text,
  p_operation_id uuid,p_request_hash text,p_selected_action_ids jsonb,p_reference_invalidation_action_ids jsonb,p_actor_user_id uuid
)
returns jsonb language plpgsql security definer set search_path to 'public','extensions','pg_temp' as $function$
declare v_s public.import_review_states%rowtype; v_i public.hr_imports%rowtype; v_o public.import_apply_operations%rowtype;
  v_ids text[]; v_db_ids text[]; v_invalidation_ids text[]; v_db_invalidation_ids text[];
  v_op_result jsonb; v_envelope jsonb; v_server_hash text; v_fresh_fingerprint text;
  v_guard_token text;
begin perform public._import_review_assert_actor_v1(p_actor_user_id);
  perform set_config('lock_timeout','1500ms',true);
  if p_operation_id is null or length(btrim(coalesce(p_request_hash,''))) not between 16 and 256
    or jsonb_typeof(coalesce(p_selected_action_ids,'[]'))<>'array'
    or jsonb_typeof(coalesce(p_reference_invalidation_action_ids,'[]'))<>'array'
    or jsonb_array_length(coalesce(p_selected_action_ids,'[]'))>5000
    or jsonb_array_length(coalesce(p_reference_invalidation_action_ids,'[]'))>5000
    or pg_column_size(coalesce(p_selected_action_ids,'[]'))+pg_column_size(coalesce(p_reference_invalidation_action_ids,'[]'))>2097152
  then raise exception 'IMPORT_REVIEW_APPLY_CONTRACT_INVALID' using errcode='22023'; end if;
  if exists(select 1 from jsonb_array_elements(coalesce(p_selected_action_ids,'[]'))x
      where jsonb_typeof(x)<>'string' or trim(both '"' from x::text)!~'^[0-9a-f]{64}$')
    or exists(select 1 from jsonb_array_elements(coalesce(p_reference_invalidation_action_ids,'[]'))x
      where jsonb_typeof(x)<>'string' or trim(both '"' from x::text)!~'^[0-9a-f]{64}$')
  then raise exception 'IMPORT_REVIEW_ACTION_ID_INVALID' using errcode='22023'; end if;
  select coalesce(array_agg(distinct value order by value),array[]::text[]) into v_ids from jsonb_array_elements_text(coalesce(p_selected_action_ids,'[]'))value;
  if cardinality(v_ids)<>jsonb_array_length(coalesce(p_selected_action_ids,'[]')) then raise exception 'IMPORT_REVIEW_ACTION_ID_DUPLICATE' using errcode='22023'; end if;
  select coalesce(array_agg(distinct value order by value),array[]::text[]) into v_invalidation_ids
  from jsonb_array_elements_text(coalesce(p_reference_invalidation_action_ids,'[]')) value;
  if cardinality(v_invalidation_ids)<>jsonb_array_length(coalesce(p_reference_invalidation_action_ids,'[]')) then
    raise exception 'IMPORT_REVIEW_INVALIDATION_ACTION_ID_DUPLICATE' using errcode='22023'; end if;
  select * into v_i from public.hr_imports where id=p_import_id for update; select * into v_s from public.import_review_states where import_id=p_import_id for update;
  if v_i.id is null or v_s.import_id is null then raise exception 'IMPORT_REVIEW_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_o from public.import_apply_operations where id=p_operation_id and import_id=p_import_id;
  if found and v_o.committed_at_utc is not null then
    if lower(btrim(p_request_hash))<>v_o.request_hash then raise exception 'IMPORT_REVIEW_OPERATION_REQUEST_MISMATCH' using errcode='23505'; end if;
    return jsonb_build_object('ok',true,'replay',true,'import_id',p_import_id,'operation_id',p_operation_id,
      'state_version',v_s.state_version,'operation_state',v_o.state,'stored_response',v_o.response_json);
  end if;
  if v_s.status not in ('BLOCKED','READY') or v_s.follow_up_status not in ('NOT_REQUIRED','COMPLETE')
    or v_s.state_version<>p_expected_state_version or v_i.coverage_fingerprint is distinct from p_expected_coverage_fingerprint
    or v_s.preview_fingerprint is distinct from p_expected_preview_fingerprint then raise exception 'IMPORT_REVIEW_APPLY_STALE_OR_NOT_READY' using errcode='40001'; end if;
  select coalesce(array_agg(action_id order by action_id),array[]::text[]) into v_db_ids
  from public._import_review_ready_action_ids_core_v1(p_import_id);
  if cardinality(v_db_ids)=0 then raise exception 'IMPORT_REVIEW_NO_READY_SELECTED_ACTIONS' using errcode='55000'; end if;
  if v_ids is distinct from v_db_ids then raise exception 'IMPORT_REVIEW_SELECTED_ACTION_SET_MISMATCH' using errcode='40001'; end if;
  select coalesce(array_agg(action_id order by action_id),array[]::text[]) into v_db_invalidation_ids
  from public.import_review_decisions where import_id=p_import_id and is_current and action_id=any(v_ids)
    and action_kind='INVALIDATE_REFERENCE';
  if v_invalidation_ids is distinct from v_db_invalidation_ids then
    raise exception 'IMPORT_REVIEW_INVALIDATION_ACTION_SET_MISMATCH' using errcode='40001'; end if;

  -- Freeze the reviewed reconciliation units first, then lock only their exact
  -- source/invoice/current-member scope before the final catalog re-attestation.
  v_envelope:=public._import_review_apply_envelope_core_v1(p_import_id);
  create temporary table if not exists pg_temp.import_review_reconciliation_units_v1(
    action_id text primary key,
    source_identity text not null,
    source_shift_id uuid,
    source_timesheet_id uuid,
    route text not null,
    unit_fingerprint text not null,
    unit_json jsonb not null
  ) on commit drop;
  truncate pg_temp.import_review_reconciliation_units_v1;
  insert into pg_temp.import_review_reconciliation_units_v1(action_id,source_identity,source_shift_id,source_timesheet_id,route,unit_fingerprint,unit_json)
  select u->>'action_id',u->>'source_identity',nullif(u->>'source_shift_id','')::uuid,
    nullif(u->>'source_timesheet_id','')::uuid,u->>'route',u->>'unit_fingerprint',u
  from jsonb_array_elements(coalesce(v_envelope->'reconciliation_units','[]'::jsonb)) u;
  if (select count(*) from pg_temp.import_review_reconciliation_units_v1)<>
     jsonb_array_length(coalesce(v_envelope->'reconciliation_units','[]'::jsonb)) then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_UNIT_INVALID' using errcode='22023';
  end if;
  perform 1 from public.invoices i where i.id in (
    select distinct x.value::uuid from pg_temp.import_review_reconciliation_units_v1 u
    cross join lateral jsonb_array_elements_text(coalesce(u.unit_json->'B_effective_invoice_ids','[]'::jsonb)) x(value)
  ) order by i.id for update;
  perform 1 from public.invoice_lines il where il.id in (
    select distinct x.value::uuid from pg_temp.import_review_reconciliation_units_v1 u
    cross join lateral jsonb_array_elements_text(coalesce(u.unit_json->'B_effective_invoice_line_ids','[]'::jsonb)) x(value)
  ) order by il.id for update;
  perform 1 from public.invoice_operations io where io.entity_id in (
    select distinct x.value::uuid from pg_temp.import_review_reconciliation_units_v1 u
    cross join lateral jsonb_array_elements_text(coalesce(u.unit_json->'B_effective_invoice_ids','[]'::jsonb)) x(value)
  ) and io.status not in ('COMPLETE','FAILED','CANCELLED') order by io.id for update;
  perform 1 from public.invoice_operation_chunks c where c.operation_id in (
    select io.id from public.invoice_operations io where io.entity_id in (
      select distinct x.value::uuid from pg_temp.import_review_reconciliation_units_v1 u
      cross join lateral jsonb_array_elements_text(coalesce(u.unit_json->'B_effective_invoice_ids','[]'::jsonb)) x(value)
    ) and io.status not in ('COMPLETE','FAILED','CANCELLED')
  ) and c.status not in ('COMPLETE','FAILED','CANCELLED') order by c.id for update;
  perform 1 from public.nhsp_shifts s where s.id in (
    select source_shift_id from pg_temp.import_review_reconciliation_units_v1 where source_shift_id is not null
  ) order by s.id for update;
  perform 1 from public.timesheets t
  where t.timesheet_id in (
    select source_timesheet_id from pg_temp.import_review_reconciliation_units_v1 where source_timesheet_id is not null
    union
    select x.value::uuid from pg_temp.import_review_reconciliation_units_v1 u
      cross join lateral jsonb_array_elements_text(coalesce(u.unit_json->'M_active_member_ids','[]'::jsonb)) x(value)
  ) and t.archived_at_utc is null
  order by t.timesheet_id for update;
  perform 1 from public.timesheets_financials tf
  where tf.is_current and tf.timesheet_id in (
    select source_timesheet_id from pg_temp.import_review_reconciliation_units_v1 where source_timesheet_id is not null
    union
    select x.value::uuid from pg_temp.import_review_reconciliation_units_v1 u
      cross join lateral jsonb_array_elements_text(coalesce(u.unit_json->'M_active_member_ids','[]'::jsonb)) x(value)
  )
  order by tf.timesheet_id,tf.id for update;
  perform 1 from public.contract_weeks cw where cw.timesheet_id in (
    select source_timesheet_id from pg_temp.import_review_reconciliation_units_v1 where source_timesheet_id is not null
    union
    select x.value::uuid from pg_temp.import_review_reconciliation_units_v1 u
      cross join lateral jsonb_array_elements_text(coalesce(u.unit_json->'M_active_member_ids','[]'::jsonb)) x(value)
  ) order by cw.id for update;

  create temporary table if not exists pg_temp.review_apply_fresh_actions on commit drop as
    select * from public._import_review_action_catalog_core_v1(p_import_id,v_s.preview_generation,500) with no data;
  truncate pg_temp.review_apply_fresh_actions;
  insert into pg_temp.review_apply_fresh_actions
    select * from public._import_review_action_catalog_core_v1(p_import_id,v_s.preview_generation,500);
  select public._import_review_hash_v1(coalesce(string_agg(action_id||':'||evidence_fingerprint,'|' order by action_id),''))
  into v_fresh_fingerprint from pg_temp.review_apply_fresh_actions;
  if v_fresh_fingerprint is distinct from v_s.preview_fingerprint then
    raise exception 'IMPORT_REVIEW_APPLY_EVIDENCE_STALE' using errcode='40001'; end if;
  if exists(
    select 1 from pg_temp.review_apply_fresh_actions b
    where b.blocking and exists (
      select 1 from public.import_review_decisions d
      where d.import_id=p_import_id and d.action_id=any(v_ids)
        and b.candidate_id=d.candidate_id
        and b.client_id=d.client_id
    )
  ) then raise exception 'IMPORT_REVIEW_APPLY_REFRESH_REQUIRED' using errcode='40001'; end if;
  if exists(
    select 1 from public.import_review_decisions d
    left join pg_temp.review_apply_fresh_actions n on n.action_id=d.action_id
    where d.import_id=p_import_id and d.is_current and d.action_id=any(v_ids)
      and (n.action_id is null or n.evidence_fingerprint is distinct from d.evidence_fingerprint
        or not n.selectable or n.blocking)
  ) then raise exception 'IMPORT_REVIEW_SELECTED_ACTION_STALE' using errcode='40001'; end if;
  if exists(select 1 from public.import_review_decisions d where d.import_id=p_import_id and d.is_current and d.action_id=any(v_ids)
    and d.action_kind in ('INCLUDE_SHIFT','APPLY_AMENDMENT','APPLY_CANCELLATION','INVALIDATE_REFERENCE','MARK_VALIDATION_ERROR') and d.timesheet_id is not null
    and coalesce((public._import_review_timesheet_protection_core_v1(d.timesheet_id)->>'active_pay_draft')::boolean,false)) then raise exception 'BLOCKED_ACTIVE_PAY_DRAFT' using errcode='55000'; end if;
  if v_i.source_system='HEALTHROSTER_DAILY'::public.hr_source_enum and exists(
    select 1 from public.import_review_daily_timesheet_resolutions r
    where r.import_id=p_import_id and r.status='CURRENT' and r.resolved_timesheet_id is not null
      and exists(select 1 from public.import_review_decisions d where d.import_id=p_import_id
        and d.hr_row_id=r.hr_row_id and d.action_id=any(v_ids))
      and coalesce((public._import_review_timesheet_protection_core_v1(r.resolved_timesheet_id)->>'active_pay_draft')::boolean,false)
  ) then raise exception 'BLOCKED_ACTIVE_PAY_DRAFT' using errcode='55000'; end if;
  if exists(select 1 from public.import_review_decisions d
    where d.import_id=p_import_id and d.is_current and d.action_id=any(v_ids) and d.action_kind='INVALIDATE_REFERENCE'
      and coalesce((public._import_review_timesheet_protection_core_v1(d.timesheet_id)->>'protected')::boolean,false)) then
    raise exception 'IMPORT_REVIEW_REFERENCE_INVALIDATION_PROTECTED' using errcode='55000'; end if;
  v_envelope:=public._import_review_apply_envelope_core_v1(p_import_id);
  if exists(
    select 1
    from pg_temp.import_review_reconciliation_units_v1 frozen
    where not exists (
      select 1
      from jsonb_array_elements(coalesce(v_envelope->'reconciliation_units','[]'::jsonb)) current_unit(u)
      where current_unit.u->>'action_id'=frozen.action_id
        and current_unit.u->>'unit_fingerprint'=frozen.unit_fingerprint
    )
  ) or (select count(*) from pg_temp.import_review_reconciliation_units_v1)<>
      jsonb_array_length(coalesce(v_envelope->'reconciliation_units','[]'::jsonb)) then
    raise exception 'IMPORT_REVIEW_SELECTED_ACTION_STALE' using errcode='40001';
  end if;
  v_server_hash:=public._import_review_hash_v1(v_envelope::text);
  if lower(btrim(p_request_hash))<>v_server_hash then
    raise exception 'IMPORT_REVIEW_APPLY_REQUEST_HASH_MISMATCH' using errcode='22023',detail=jsonb_build_object('server_request_hash',v_server_hash)::text;
  end if;
  v_guard_token:=encode(gen_random_bytes(32),'hex');
  perform set_config('cloudtms.import_reconciliation_guard_token',v_guard_token,true);
  perform set_config('cloudtms.import_reconciliation_operation_id',p_operation_id::text,true);
  perform set_config('cloudtms.import_reconciliation_request_hash',v_server_hash,true);
  perform set_config('cloudtms.import_reconciliation_unit_fingerprints',coalesce((
    select string_agg(unit_fingerprint,',' order by action_id) from pg_temp.import_review_reconciliation_units_v1
  ),''),true);
  v_op_result:=public._import_apply_operation_claim_core_v2(p_operation_id,p_import_id,v_i.source_system,
    concat_ws(':',coalesce(v_i.revision_group_id,v_i.id),coalesce(v_i.revision_no,1)),v_server_hash,p_actor_user_id,v_envelope);
  update public.import_review_states set status='APPLYING',state_version=state_version+1,last_operation_id=p_operation_id,updated_at_utc=now(),updated_by_user_id=p_actor_user_id where import_id=p_import_id returning * into v_s;
  insert into public.import_review_events(import_id,state_version,operation_id,event_code,actor_user_id,event_context_json) values(p_import_id,v_s.state_version,p_operation_id,'APPLY_STARTED',p_actor_user_id,jsonb_build_object('request_hash',btrim(p_request_hash),'selected_count',cardinality(v_ids),'batch_scope_units',v_envelope->'batch_scope_units'));
  return jsonb_build_object('ok',true,'import_id',p_import_id,'operation_id',p_operation_id,'state_version',v_s.state_version,'selected_action_ids',to_jsonb(v_ids),'operation_state',v_op_result->>'state'); end $function$;

create or replace function public.import_review_correction_generation_transition_v1(
  p_import_id uuid,
  p_operation_id uuid,
  p_request_hash text,
  p_action text,
  p_actor_user_id uuid,
  p_action_ids text[] default '{}'::text[],
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path to 'public','extensions','pg_temp'
as $function$
declare
  v_operation public.import_apply_operations%rowtype;
  v_action text:=upper(btrim(coalesce(p_action,'')));
  v_units jsonb:='[]'::jsonb;
  v_request_units jsonb:='[]'::jsonb;
  v_applied_units jsonb:='[]'::jsonb;
  v_policy_units jsonb:='[]'::jsonb;
  v_unit jsonb;
  v_balance jsonb;
  v_capability_token text;
  v_items jsonb;
  v_result jsonb;
  v_target_ids uuid[];
  v_pending_target_ids uuid[];
  v_member_count integer;
  v_bad_count integer;
  v_id uuid;
  v_signature jsonb;
  v_all_authorised boolean:=false;
  v_any_authorised boolean:=false;
  v_unit_fingerprints jsonb:='[]'::jsonb;
  v_expected_a_day numeric;
  v_expected_a_night numeric;
  v_expected_a_sat numeric;
  v_expected_a_sun numeric;
  v_expected_a_bh numeric;
  v_expected_a_total numeric;
  v_frozen_a_bucket_total numeric;
begin
  perform public._import_review_assert_actor_v1(p_actor_user_id);
  if session_user not in ('postgres','service_role') and coalesce(
      current_setting('request.jwt.claim.role',true),
      nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_OPERATION_INVALID' using errcode='42501';
  end if;
  if p_import_id is null or p_operation_id is null or length(btrim(coalesce(p_request_hash,''))) not between 16 and 256
     or v_action not in ('PREPARE','VALIDATE','AUTHORISE') or cardinality(coalesce(p_action_ids,array[]::text[]))>100 then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_OPERATION_INVALID' using errcode='22023';
  end if;
  perform set_config('lock_timeout','1500ms',true);
  select * into v_operation from public.import_apply_operations
  where id=p_operation_id and import_id=p_import_id for update;
  if v_operation.id is null or v_operation.request_hash<>lower(btrim(p_request_hash)) then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_OPERATION_INVALID' using errcode='40001';
  end if;
  if v_action in ('VALIDATE','AUTHORISE') and (
      v_operation.committed_at_utc is null
      or v_operation.state not in ('SOURCE_COMMITTED_TSFIN_PENDING','COMPLETE')) then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_OPERATION_INVALID' using errcode='40001';
  end if;
  perform set_config('cloudtms.import_reconciliation_operation_id',p_operation_id::text,true);

  if v_action='PREPARE' then
    if to_regclass('pg_temp.import_review_reconciliation_units_v1') is null
       or current_setting('cloudtms.import_reconciliation_operation_id',true) is distinct from p_operation_id::text
       or current_setting('cloudtms.import_reconciliation_request_hash',true) is distinct from v_operation.request_hash then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_GUARD_REQUIRED' using errcode='55000';
    end if;
    select coalesce(jsonb_agg(u.unit_json order by u.action_id),'[]'::jsonb) into v_units
    from pg_temp.import_review_reconciliation_units_v1 u
    where cardinality(coalesce(p_action_ids,array[]::text[]))=0 or u.action_id=any(p_action_ids);
  else
    v_request_units:=coalesce(v_operation.response_json#>'{request_envelope,reconciliation_units}','[]'::jsonb);
    v_applied_units:=coalesce(v_operation.response_json->'reconciliation_units','[]'::jsonb);
    v_policy_units:=coalesce(v_operation.response_json#>'{correction_operation_contract,correction_units}','[]'::jsonb);
    if cardinality(coalesce(p_action_ids,array[]::text[]))>0 then
      select coalesce(jsonb_agg(u order by u->>'action_id'),'[]'::jsonb) into v_request_units
      from jsonb_array_elements(v_request_units) u where u->>'action_id'=any(p_action_ids);
      select coalesce(jsonb_agg(u order by u->>'action_id'),'[]'::jsonb) into v_applied_units
      from jsonb_array_elements(v_applied_units) u where u->>'action_id'=any(p_action_ids);
      select coalesce(jsonb_agg(u order by u->>'action_id'),'[]'::jsonb) into v_policy_units
      from jsonb_array_elements(v_policy_units) u where u->>'action_id'=any(p_action_ids);
    end if;
    if exists(
      select 1 from jsonb_array_elements(v_request_units) request
      where request->>'route' in ('AMEND_PAID_UNINVOICED_SOURCE','AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT')
        and ((select count(*) from jsonb_array_elements(v_applied_units) applied where applied->>'action_id'=request->>'action_id')<>1
          or (select count(*) from jsonb_array_elements(v_policy_units) policy where policy->>'action_id'=request->>'action_id')<>1)
    ) then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_EVIDENCE_CONTRACT_INVALID' using errcode='40001';
    end if;
    select coalesce(jsonb_agg(
      request
      || case when applied is null then '{}'::jsonb else jsonb_build_object(
          'correction_id',applied->>'correction_id','applied_member_ids',applied->'applied_member_ids',
          'reversal_timesheet_id',applied->>'reversal_timesheet_id','replacement_timesheet_id',applied->>'replacement_timesheet_id',
          'parent_timesheet_id',applied->>'parent_timesheet_id','repair_identity_mode',applied->>'repair_identity_mode',
          'applied_result_fingerprint',applied->>'applied_result_fingerprint',
          'reviewed_unit_fingerprint',applied->>'reviewed_unit_fingerprint','applied_reconciliation_fingerprint',applied->>'reconciliation_fingerprint',
          'applied_timesheet_id',applied->>'applied_timesheet_id','rollover_mode',applied->>'rollover_mode',
          'historical_paid_tsfin_id',applied->>'historical_paid_tsfin_id','current_shell_tsfin_id',applied->>'current_shell_tsfin_id',
          'applied_intended_authorisation_action',applied->>'intended_authorisation_action') end
      || case when policy is null then '{}'::jsonb else jsonb_build_object(
          'operation_policy_envelope',policy->'policy_envelope',
          'operation_policy_fingerprint',policy->>'policy_envelope_fingerprint',
          'operation_policy_root_timesheet_id',policy->>'root_timesheet_id',
          'operation_policy_source_row_key',policy->>'source_row_key',
          'operation_policy_source_shift_id',policy->>'source_shift_id') end
      order by request->>'action_id'),'[]'::jsonb)
    into v_units
    from jsonb_array_elements(v_request_units) request
    left join lateral (select u applied from jsonb_array_elements(v_applied_units) u where u->>'action_id'=request->>'action_id' limit 1) a on true
    left join lateral (select u policy from jsonb_array_elements(v_policy_units) u where u->>'action_id'=request->>'action_id' limit 1) p on true;
  end if;
  if jsonb_array_length(v_units)=0 then
    return jsonb_build_object('ok',true,'action',v_action,'idempotent',true,'unit_count',0,'timesheet_ids','[]'::jsonb);
  end if;
  select coalesce(jsonb_agg(u->>'unit_fingerprint' order by u->>'action_id'),'[]'::jsonb)
  into v_unit_fingerprints from jsonb_array_elements(v_units) u;
  if exists(select 1 from jsonb_array_elements(v_units) u
      where u->>'schema_version'<>'IMPORT_AUTHORITATIVE_RECONCILIATION_V1'
        or nullif(u->>'unit_fingerprint','') is null
        or nullif(u->>'source_identity','') is null
        or u->>'route' not in ('AMEND_SOURCE','AMEND_PAID_UNINVOICED_SOURCE','AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT')) then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_UNIT_NOT_FOUND' using errcode='22023';
  end if;
  if v_action<>'PREPARE' and exists(
    select 1
    from jsonb_array_elements(v_units) u
    left join public.import_review_action_outcomes outcome
      on outcome.operation_id=p_operation_id and outcome.action_id=u->>'action_id'
    where outcome.action_id is null
       or u->>'unit_fingerprint' is distinct from public._import_review_hash_v1(concat_ws('|','unit-v2',
         u->>'action_id',u->>'source_identity',u->>'source_shift_id',u->>'route',u->>'reconciliation_mode',
         u->>'reconciliation_fingerprint',u->>'review_policy_basis_kind',u->>'review_policy_basis_fingerprint',
         outcome.evidence_fingerprint))
  ) then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_FINGERPRINT_MISMATCH' using errcode='40001';
  end if;
  if v_action<>'PREPARE' and exists(
    select 1 from jsonb_array_elements(v_units) u
    where u->>'route' in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT') and (
      nullif(u->>'correction_id','') is null
      or nullif(u->>'reviewed_unit_fingerprint','') is distinct from nullif(u->>'unit_fingerprint','')
      or nullif(u->>'applied_reconciliation_fingerprint','') is distinct from nullif(u->>'reconciliation_fingerprint','')
      or nullif(u->>'operation_policy_source_row_key','') is distinct from nullif(u->>'source_identity','')
      or nullif(u->>'operation_policy_source_shift_id','') is distinct from nullif(u->>'source_shift_id','')
      or nullif(u->>'operation_policy_root_timesheet_id','') is distinct from nullif(u->>'source_timesheet_id','')
      or jsonb_typeof(u->'applied_member_ids')<>'array' or jsonb_array_length(u->'applied_member_ids')<>2
      or coalesce(u->>'reversal_timesheet_id','')!~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      or coalesce(u->>'replacement_timesheet_id','')!~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      or u->>'reversal_timesheet_id'=u->>'replacement_timesheet_id'
      or not (u->'applied_member_ids' @> jsonb_build_array(u->>'reversal_timesheet_id')
        and u->'applied_member_ids' @> jsonb_build_array(u->>'replacement_timesheet_id'))
      or u->>'applied_result_fingerprint' is distinct from encode(digest(convert_to(jsonb_build_object(
        'correction_id',u->>'correction_id',
        'reversal_timesheet_id',(u->>'reversal_timesheet_id')::uuid,
        'replacement_timesheet_id',(u->>'replacement_timesheet_id')::uuid,
        'M_active_member_ids',u->'applied_member_ids',
        'applied_member_ids',u->'applied_member_ids',
        'parent_timesheet_id',(u->>'parent_timesheet_id')::uuid,
        'repair_identity_mode',u->>'repair_identity_mode',
        'reviewed_unit_fingerprint',u->>'reviewed_unit_fingerprint',
        'reconciliation_fingerprint',u->>'applied_reconciliation_fingerprint')::text,'UTF8'),'sha256'),'hex')
      or jsonb_typeof(u->'operation_policy_envelope')<>'object'
      or nullif(u->>'operation_policy_fingerprint','') is null
      or u#>>'{operation_policy_envelope,envelope_fingerprint}' is distinct from u->>'operation_policy_fingerprint'
      or encode(digest(convert_to(((u->'operation_policy_envelope')-'envelope_fingerprint')::text,'UTF8'),'sha256'),'hex')
        is distinct from u->>'operation_policy_fingerprint'
    )
  ) then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_EVIDENCE_CONTRACT_INVALID' using errcode='40001';
  end if;
  if v_action<>'PREPARE' and exists(
    select 1 from jsonb_array_elements(v_units) u
    where u->>'route'='AMEND_PAID_UNINVOICED_SOURCE' and (
      nullif(u->>'reviewed_unit_fingerprint','') is distinct from nullif(u->>'unit_fingerprint','')
      or nullif(u->>'applied_reconciliation_fingerprint','') is distinct from nullif(u->>'reconciliation_fingerprint','')
      or nullif(u->>'applied_timesheet_id','') is distinct from nullif(u->>'source_timesheet_id','')
      or nullif(u->>'applied_intended_authorisation_action','') is distinct from nullif(u->>'intended_authorisation_action','')
      or coalesce(u->>'rollover_mode','') not in ('CREATED_CURRENT_OPERATION_SHELL','REUSED_COMPLETED_OPERATION_SHELL')
      or coalesce(u->>'historical_paid_tsfin_id','')!~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      or coalesce(u->>'current_shell_tsfin_id','')!~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      or nullif(u->>'operation_policy_source_row_key','') is distinct from nullif(u->>'source_identity','')
      or nullif(u->>'operation_policy_source_shift_id','') is distinct from nullif(u->>'source_shift_id','')
      or nullif(u->>'operation_policy_root_timesheet_id','') is distinct from nullif(u->>'source_timesheet_id','')
      or jsonb_typeof(u->'operation_policy_envelope')<>'object'
      or nullif(u->>'operation_policy_fingerprint','') is null
      or u#>>'{operation_policy_envelope,envelope_fingerprint}' is distinct from u->>'operation_policy_fingerprint'
      or encode(digest(convert_to(((u->'operation_policy_envelope')-'envelope_fingerprint')::text,'UTF8'),'sha256'),'hex')
        is distinct from u->>'operation_policy_fingerprint'
      or u->>'applied_result_fingerprint' is distinct from encode(digest(convert_to(jsonb_build_object(
        'applied_timesheet_id',(u->>'applied_timesheet_id')::uuid,
        'rollover_mode',u->>'rollover_mode',
        'historical_paid_tsfin_id',(u->>'historical_paid_tsfin_id')::uuid,
        'current_shell_tsfin_id',(u->>'current_shell_tsfin_id')::uuid,
        'intended_authorisation_action',u->>'intended_authorisation_action',
        'reviewed_unit_fingerprint',u->>'reviewed_unit_fingerprint',
        'reconciliation_fingerprint',u->>'reconciliation_fingerprint')::text,'UTF8'),'sha256'),'hex')
    )
  ) then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_EVIDENCE_CONTRACT_INVALID' using errcode='40001';
  end if;

  if v_action='PREPARE' then
    select coalesce(array_agg(distinct x.value::uuid order by x.value::uuid),array[]::uuid[]) into v_target_ids
    from jsonb_array_elements(v_units) u
    cross join lateral jsonb_array_elements_text(coalesce(u->'M_active_member_ids','[]'::jsonb)) x(value)
    join public.timesheets t on t.timesheet_id=x.value::uuid and t.is_current and t.archived_at_utc is null
    left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
    left join public.contract_weeks cw on cw.timesheet_id=t.timesheet_id
    where t.authorised_at_server is not null or tf.authorised_at_utc is not null
       or cw.status='AUTHORISED'::public.contract_week_status_enum;
    if exists(select 1 from jsonb_array_elements(v_units) u
      cross join lateral jsonb_array_elements_text(coalesce(u->'M_active_member_ids','[]'::jsonb)) x(value)
      join public.timesheets t on t.timesheet_id=x.value::uuid where t.archived_at_utc is not null) then
      raise exception 'IMPORT_REVIEW_SELECTED_ACTION_STALE' using errcode='40001';
    end if;
    if cardinality(v_target_ids)>0 then
      v_capability_token:=encode(gen_random_bytes(32),'hex');
      create temporary table if not exists pg_temp.import_review_lifecycle_capability_v1(
        capability_token text not null,txid bigint not null,operation_id uuid not null,request_hash text not null,
        actor_user_id uuid not null,action text not null,action_id text not null,unit_fingerprint text not null,
        timesheet_id uuid not null,expected_timesheet_id uuid not null,expected_version integer,
        expected_row_signature text,expected_tsfin_id uuid,expected_contract_week_id uuid
      ) on commit drop;
      truncate pg_temp.import_review_lifecycle_capability_v1;
      foreach v_id in array v_target_ids loop
        v_signature:=public.timesheet_lifecycle_signature_v1(v_id,null,false);
        insert into pg_temp.import_review_lifecycle_capability_v1
        select v_capability_token,txid_current(),p_operation_id,v_operation.request_hash,p_actor_user_id,'UNAUTHORISE',
          u->>'action_id',u->>'unit_fingerprint',v_id,v_id,t.version,
          coalesce(v_signature->>'backend_row_signature',v_signature->>'row_signature',v_signature->>'signature'),
          tf.id,cw.id
        from jsonb_array_elements(v_units) u
        join lateral jsonb_array_elements_text(coalesce(u->'M_active_member_ids','[]'::jsonb)) x(value) on x.value::uuid=v_id
        join public.timesheets t on t.timesheet_id=v_id and t.is_current and t.archived_at_utc is null
        left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
        left join public.contract_weeks cw on cw.timesheet_id=t.timesheet_id;
      end loop;
      perform set_config('cloudtms.import_reconciliation_capability_token',v_capability_token,true);
      perform set_config('cloudtms.import_reconciliation_operation_id',p_operation_id::text,true);
      perform set_config('cloudtms.import_reconciliation_action','UNAUTHORISE',true);
      select jsonb_agg(jsonb_build_object('timesheet_id',x) order by x) into v_items from unnest(v_target_ids) x;
      v_result:=public.timesheet_unauthorise_bulk_atomic(v_items,p_actor_user_id,coalesce(p_now_utc,now()));
      if coalesce((v_result->>'ok')::boolean,false) is not true or coalesce((v_result->>'all_success')::boolean,false) is not true then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_PREPARE_INCOMPLETE' using errcode='55000',detail=coalesce(v_result->>'error_code','bulk unauthorise incomplete');
      end if;
      truncate pg_temp.import_review_lifecycle_capability_v1;
      perform set_config('cloudtms.import_reconciliation_capability_token','',true);
      perform set_config('cloudtms.import_reconciliation_action','',true);
    end if;
    if exists(select 1 from jsonb_array_elements(v_units) u
      cross join lateral jsonb_array_elements_text(coalesce(u->'M_active_member_ids','[]'::jsonb)) x(value)
      join public.timesheets t on t.timesheet_id=x.value::uuid and t.is_current and t.archived_at_utc is null
      left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
      left join public.contract_weeks cw on cw.timesheet_id=t.timesheet_id
      where t.authorised_at_server is not null or tf.authorised_at_utc is not null
         or cw.status='AUTHORISED'::public.contract_week_status_enum) then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_PREPARE_INCOMPLETE' using errcode='55000';
    end if;
    return jsonb_build_object('ok',true,'action','PREPARE','unit_count',jsonb_array_length(v_units),
      'timesheet_ids',to_jsonb(coalesce(v_target_ids,array[]::uuid[])),'bulk_result',v_result);
  end if;

  for v_unit in select value from jsonb_array_elements(v_units) loop
    perform 1 from public.invoices i where i.id in (
      select x.value::uuid from jsonb_array_elements_text(coalesce(v_unit->'B_effective_invoice_ids','[]'::jsonb)) x(value)
    ) order by i.id for update;
    perform 1 from public.invoice_lines il where il.id in (
      select x.value::uuid from jsonb_array_elements_text(coalesce(v_unit->'B_effective_invoice_line_ids','[]'::jsonb)) x(value)
    ) order by il.id for update;
    perform 1 from public.nhsp_shifts s where s.id=(v_unit->>'source_shift_id')::uuid for update;
    if not exists(select 1 from public.nhsp_shifts s where s.id=(v_unit->>'source_shift_id')::uuid
      and s.external_row_key=v_unit->>'source_identity' and s.cancelled_at_utc is null
      and s.source_system::text=v_unit->>'source_system') then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_SOURCE_MISMATCH' using errcode='40001';
    end if;
    perform 1 from public.timesheets t where t.correction_id=v_unit->>'correction_id'
      and t.is_current and t.archived_at_utc is null order by t.timesheet_id for update;
    perform 1 from public.timesheets_financials tf where tf.is_current and tf.timesheet_id in (
      select t.timesheet_id from public.timesheets t where t.correction_id=v_unit->>'correction_id'
        and t.is_current and t.archived_at_utc is null
    ) order by tf.timesheet_id,tf.id for update;
    perform 1 from public.contract_weeks cw where cw.timesheet_id in (
      select t.timesheet_id from public.timesheets t where t.correction_id=v_unit->>'correction_id'
        and t.is_current and t.archived_at_utc is null
    ) order by cw.id for update;
    select b.balance_json into v_balance
    from public._import_review_effective_invoice_balance_core_v1(p_import_id,jsonb_build_array(jsonb_build_object(
      'source_identity',v_unit->>'source_identity','source_system',v_unit->>'source_system',
      'source_shift_id',v_unit->>'source_shift_id','external_row_key',v_unit->>'source_identity',
      'hr_row_id',v_unit->>'hr_row_id','source_timesheet_id',v_unit->>'source_timesheet_id',
      'candidate_id',v_unit->>'candidate_id','client_id',v_unit->>'client_id','contract_id',v_unit->>'contract_id',
      'week_ending_date',v_unit->>'week_ending_date','invoice_stream',v_unit->>'invoice_stream',
      'authoritative_import_id',p_import_id,'authoritative_schedule_json',v_unit->'A_schedule_json',
      'authoritative_hours',v_unit->'A_hours')),100,512,256,128) b;
    if nullif(v_balance->>'blocking_code','') is not null then
      raise exception 'IMPORT_REVIEW_INVOICE_ACTIVITY_IN_PROGRESS' using errcode='55000',detail=v_balance->>'blocking_code';
    end if;
    -- The review-time effective invoice fingerprint also attests mutable role
    -- evidence.  That evidence is expected to change when this operation
    -- repairs the approved pair, so it is not a valid post-mutation equality
    -- check.  Re-attest the immutable B authority directly instead: the exact
    -- invoice and line identities, signed hours and money, and terminal
    -- schedule must all remain byte-for-byte equal to the reviewed envelope.
    if coalesce(v_balance->'effective_invoice_ids','[]'::jsonb)
          is distinct from coalesce(v_unit->'B_effective_invoice_ids','[]'::jsonb)
       or coalesce(v_balance->'effective_invoice_line_ids','[]'::jsonb)
          is distinct from coalesce(v_unit->'B_effective_invoice_line_ids','[]'::jsonb)
       or coalesce(v_balance->'B_hours','{}'::jsonb)
          is distinct from coalesce(v_unit->'B_hours','{}'::jsonb)
       or coalesce(v_balance->'B_financials','{}'::jsonb)
          is distinct from coalesce(v_unit->'B_financials','{}'::jsonb)
       or coalesce(v_balance->'B_standard_schedule_json','[]'::jsonb)
          is distinct from coalesce(v_unit->'B_standard_schedule_json','[]'::jsonb) then
      raise exception 'IMPORT_REVIEW_SELECTED_ACTION_STALE' using errcode='40001';
    end if;
    if v_unit->>'route' in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT') then
      select count(*) into v_member_count from public.timesheets t
      where t.correction_id=v_unit->>'correction_id' and t.is_current and t.archived_at_utc is null
        and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT');
      if v_member_count<>2 or not exists(select 1 from public.timesheets t where t.correction_id=v_unit->>'correction_id'
          and t.is_current and t.archived_at_utc is null and t.correction_kind='CHANGED_HOURS_REVERSAL')
        or not exists(select 1 from public.timesheets t where t.correction_id=v_unit->>'correction_id'
          and t.is_current and t.archived_at_utc is null and t.correction_kind='CHANGED_HOURS_REPLACEMENT')
        or exists(select 1 from public.timesheets t where t.correction_id=v_unit->>'correction_id'
          and t.is_current and t.archived_at_utc is null
          and not (v_unit->'applied_member_ids' @> jsonb_build_array(t.timesheet_id::text)))
        or not exists(select 1 from public.timesheets t where t.timesheet_id=(v_unit->>'reversal_timesheet_id')::uuid
          and t.correction_id=v_unit->>'correction_id' and t.correction_kind='CHANGED_HOURS_REVERSAL' and t.is_current and t.archived_at_utc is null)
        or not exists(select 1 from public.timesheets t where t.timesheet_id=(v_unit->>'replacement_timesheet_id')::uuid
          and t.correction_id=v_unit->>'correction_id' and t.correction_kind='CHANGED_HOURS_REPLACEMENT' and t.is_current and t.archived_at_utc is null) then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_MEMBER_SET_MISMATCH' using errcode='55000';
      end if;
      if exists(select 1 from public.timesheets t
        left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
        where t.correction_id=v_unit->>'correction_id' and t.is_current and t.archived_at_utc is null
          and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT') and (
            not coalesce(t.is_adjustment,false) or t.adjustment_origin<>'IMPORT_CORRECTION'
            or t.parent_timesheet_id is distinct from (v_unit->>'parent_timesheet_id')::uuid
            or t.contract_id is distinct from (v_unit->>'contract_id')::uuid
            or t.week_ending_date is distinct from (v_unit->>'week_ending_date')::date
            or t.sheet_scope<>'WEEKLY'::public.timesheet_scope_enum
            or tf.candidate_id is distinct from (v_unit->>'candidate_id')::uuid
            or tf.client_id is distinct from (v_unit->>'client_id')::uuid
            or (v_unit->>'source_system'='NHSP' and tf.basis<>'NHSP_ADJUSTMENT'::public.timesheet_fin_basis_enum)
            or (v_unit->>'source_system'='HEALTHROSTER' and tf.basis<>'HEALTHROSTER_ADJUSTMENT'::public.timesheet_fin_basis_enum)
            or t.candidate_hint_text#>>'{import_authoritative_reconciliation,operation_id}'<>p_operation_id::text
            or t.candidate_hint_text#>>'{import_authoritative_reconciliation,unit_fingerprint}'<>v_unit->>'unit_fingerprint'
            or t.candidate_hint_text#>>'{import_authoritative_reconciliation,source_identity}'<>v_unit->>'source_identity'
            or jsonb_typeof(t.actual_schedule_json)<>'array'
            or jsonb_array_length(t.actual_schedule_json)<>1
            or not t.actual_schedule_json @> jsonb_build_array(jsonb_build_object(
              'shift_id',v_unit->>'source_shift_id','external_row_key',v_unit->>'source_identity'))
            or (select count(*) from public.contract_weeks cw where cw.timesheet_id=t.timesheet_id)<>1
            or exists(select 1 from public.contract_weeks cw where cw.timesheet_id=t.timesheet_id
              and (not coalesce(cw.is_adjustment,false)
                or cw.contract_id is distinct from (v_unit->>'contract_id')::uuid
                or cw.week_ending_date is distinct from (v_unit->>'week_ending_date')::date))
          ))
         or not exists(select 1 from public.timesheets parent_ts
           where parent_ts.timesheet_id=(v_unit->>'parent_timesheet_id')::uuid
             and parent_ts.is_current and parent_ts.archived_at_utc is null) then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_SOURCE_MISMATCH' using errcode='40001';
      end if;
      if exists(select 1 from public.timesheets t
        where t.correction_id=v_unit->>'correction_id' and t.is_current and t.archived_at_utc is null
          and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
          and (coalesce(public._ctms_correction_policy_envelope_read_v1(t.timesheet_id)->>'envelope_fingerprint','')
            is distinct from coalesce(v_unit->>'operation_policy_fingerprint','')
            or public._ctms_correction_policy_envelope_read_v1(t.timesheet_id)
              is distinct from v_unit->'operation_policy_envelope'))
        or (select count(distinct public._ctms_correction_policy_envelope_read_v1(t.timesheet_id)->>'envelope_fingerprint')
            from public.timesheets t where t.correction_id=v_unit->>'correction_id'
              and t.is_current and t.archived_at_utc is null
              and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT'))<>1 then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_POLICY_MISMATCH' using errcode='40001';
      end if;
      select h.hours_day,h.hours_night,h.hours_sat,h.hours_sun,h.hours_bh,h.total_hours
      into v_expected_a_day,v_expected_a_night,v_expected_a_sat,v_expected_a_sun,v_expected_a_bh,v_expected_a_total
      from public.timesheets t
      join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
      cross join lateral public._wkimp_bucket_hours_from_policy(
        coalesce(tf.policy_snapshot_json,'{}'::jsonb),
        (v_unit#>>'{A_schedule_json,0,start_utc}')::timestamptz,
        (v_unit#>>'{A_schedule_json,0,end_utc}')::timestamptz,
        coalesce((v_unit#>>'{A_schedule_json,0,break_mins}')::integer,0)
      ) h
      where t.timesheet_id=(v_unit->>'replacement_timesheet_id')::uuid
        and t.correction_id=v_unit->>'correction_id'
        and t.correction_kind='CHANGED_HOURS_REPLACEMENT'
        and t.is_current and t.archived_at_utc is null
      limit 1;
      v_frozen_a_bucket_total:=coalesce((v_unit#>>'{A_hours,hours_day}')::numeric,0)
        +coalesce((v_unit#>>'{A_hours,hours_night}')::numeric,0)
        +coalesce((v_unit#>>'{A_hours,hours_sat}')::numeric,0)
        +coalesce((v_unit#>>'{A_hours,hours_sun}')::numeric,0)
        +coalesce((v_unit#>>'{A_hours,hours_bh}')::numeric,0);
      if v_expected_a_total is null
        or v_expected_a_total is distinct from coalesce((v_unit#>>'{A_hours,total_hours}')::numeric,0)
        or (v_frozen_a_bucket_total<>0 and (
          v_expected_a_day is distinct from coalesce((v_unit#>>'{A_hours,hours_day}')::numeric,0)
          or v_expected_a_night is distinct from coalesce((v_unit#>>'{A_hours,hours_night}')::numeric,0)
          or v_expected_a_sat is distinct from coalesce((v_unit#>>'{A_hours,hours_sat}')::numeric,0)
          or v_expected_a_sun is distinct from coalesce((v_unit#>>'{A_hours,hours_sun}')::numeric,0)
          or v_expected_a_bh is distinct from coalesce((v_unit#>>'{A_hours,hours_bh}')::numeric,0)
        )) then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_BALANCE_MISMATCH' using errcode='55000';
      end if;
      if exists(select 1 from public.timesheets t
        left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
        where t.correction_id=v_unit->>'correction_id' and t.is_current and t.archived_at_utc is null
          and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
          and (tf.id is null or tf.processing_status not in (
            'PENDING_AUTH'::public.ts_fin_processing_status_enum,
            'READY_FOR_HR'::public.ts_fin_processing_status_enum,
            'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum))) then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_TSFIN_NOT_SETTLED' using errcode='55000';
      end if;
      if exists(select 1 from public.timesheets t where t.correction_id=v_unit->>'correction_id'
        and t.is_current and t.archived_at_utc is null and (
          (t.correction_kind='CHANGED_HOURS_REVERSAL' and (
            (t.actual_schedule_json#>>'{0,start_utc}')::timestamptz is distinct from (v_unit#>>'{B_standard_schedule_json,0,start_utc}')::timestamptz
            or (t.actual_schedule_json#>>'{0,end_utc}')::timestamptz is distinct from (v_unit#>>'{B_standard_schedule_json,0,end_utc}')::timestamptz
            or coalesce((t.actual_schedule_json#>>'{0,break_mins}')::integer,0) is distinct from coalesce((v_unit#>>'{B_standard_schedule_json,0,break_mins}')::integer,0)))
          or (t.correction_kind='CHANGED_HOURS_REPLACEMENT' and (
            (t.actual_schedule_json#>>'{0,start_utc}')::timestamptz is distinct from (v_unit#>>'{A_schedule_json,0,start_utc}')::timestamptz
            or (t.actual_schedule_json#>>'{0,end_utc}')::timestamptz is distinct from (v_unit#>>'{A_schedule_json,0,end_utc}')::timestamptz
            or coalesce((t.actual_schedule_json#>>'{0,break_mins}')::integer,0) is distinct from coalesce((v_unit#>>'{A_schedule_json,0,break_mins}')::integer,0)))
        )) then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_BALANCE_MISMATCH' using errcode='55000';
      end if;
      select count(*) into v_bad_count
      from public.timesheets t join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
      where t.correction_id=v_unit->>'correction_id' and t.is_current and t.archived_at_utc is null
        and (coalesce(tf.is_stale,true) or coalesce(tf.has_rate_issue,false) or coalesce(tf.has_pay_channel_issue,false));
      if v_bad_count>0 then raise exception 'IMPORT_REVIEW_RECONCILIATION_TSFIN_NOT_SETTLED' using errcode='55000'; end if;
      select count(*) into v_bad_count
      from public.timesheets t
      join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
      where t.correction_id=v_unit->>'correction_id' and t.is_current and t.archived_at_utc is null
        and case t.correction_kind
          when 'CHANGED_HOURS_REVERSAL' then
            tf.hours_day<>-coalesce((v_unit#>>'{B_hours,hours_day}')::numeric,0)
            or tf.hours_night<>-coalesce((v_unit#>>'{B_hours,hours_night}')::numeric,0)
            or tf.hours_sat<>-coalesce((v_unit#>>'{B_hours,hours_sat}')::numeric,0)
            or tf.hours_sun<>-coalesce((v_unit#>>'{B_hours,hours_sun}')::numeric,0)
            or tf.hours_bh<>-coalesce((v_unit#>>'{B_hours,hours_bh}')::numeric,0)
            or tf.total_pay_ex_vat<>-coalesce((v_unit#>>'{B_financials,pay_ex_vat}')::numeric,0)
            or tf.total_charge_ex_vat<>-coalesce((v_unit#>>'{B_financials,charge_ex_vat}')::numeric,0)
          when 'CHANGED_HOURS_REPLACEMENT' then
            tf.hours_day<>v_expected_a_day or tf.hours_night<>v_expected_a_night
            or tf.hours_sat<>v_expected_a_sat or tf.hours_sun<>v_expected_a_sun
            or tf.hours_bh<>v_expected_a_bh
          else false
        end;
      if v_bad_count>0 then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_BALANCE_MISMATCH' using errcode='55000';
      end if;
    else
      if jsonb_array_length(coalesce(v_unit->'B_effective_invoice_ids','[]'::jsonb))<>0
         or jsonb_array_length(coalesce(v_unit->'B_effective_invoice_line_ids','[]'::jsonb))<>0
         or coalesce((v_unit#>>'{B_hours,hours_day}')::numeric,0)<>0
         or coalesce((v_unit#>>'{B_hours,hours_night}')::numeric,0)<>0
         or coalesce((v_unit#>>'{B_hours,hours_sat}')::numeric,0)<>0
         or coalesce((v_unit#>>'{B_hours,hours_sun}')::numeric,0)<>0
         or coalesce((v_unit#>>'{B_hours,hours_bh}')::numeric,0)<>0
         or coalesce((v_unit#>>'{B_financials,pay_ex_vat}')::numeric,0)<>0
         or coalesce((v_unit#>>'{B_financials,charge_ex_vat}')::numeric,0)<>0
         or coalesce((v_unit#>>'{B_financials,margin_ex_vat}')::numeric,0)<>0 then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_BALANCE_MISMATCH' using errcode='55000';
      end if;
      if not exists(select 1 from public.timesheets t join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
        where t.timesheet_id=(v_unit->>'source_timesheet_id')::uuid and t.is_current and t.archived_at_utc is null
          and t.contract_id=(v_unit->>'contract_id')::uuid
          and t.week_ending_date=(v_unit->>'week_ending_date')::date
          and t.sheet_scope='WEEKLY'::public.timesheet_scope_enum
          and tf.candidate_id=(v_unit->>'candidate_id')::uuid
          and tf.client_id=(v_unit->>'client_id')::uuid
          and ((v_unit->>'source_system'='NHSP' and tf.basis='NHSP'::public.timesheet_fin_basis_enum)
            or (v_unit->>'source_system'='HEALTHROSTER' and tf.basis='HEALTHROSTER'::public.timesheet_fin_basis_enum))
          and jsonb_typeof(t.actual_schedule_json)='array' and jsonb_array_length(t.actual_schedule_json)=1
          and (t.actual_schedule_json#>>'{0,start_utc}')::timestamptz=(v_unit#>>'{A_schedule_json,0,start_utc}')::timestamptz
          and (t.actual_schedule_json#>>'{0,end_utc}')::timestamptz=(v_unit#>>'{A_schedule_json,0,end_utc}')::timestamptz
          and coalesce((t.actual_schedule_json#>>'{0,break_mins}')::integer,0)=coalesce((v_unit#>>'{A_schedule_json,0,break_mins}')::integer,0)
          and t.actual_schedule_json @> jsonb_build_array(jsonb_build_object(
            'shift_id',v_unit->>'source_shift_id','external_row_key',v_unit->>'source_identity'))
          and (v_unit->>'route'<>'AMEND_PAID_UNINVOICED_SOURCE'
            or coalesce(tf.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint',
              tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}')
              =v_unit->>'operation_policy_fingerprint')
          and not coalesce(tf.is_stale,true) and not coalesce(tf.has_rate_issue,false) and not coalesce(tf.has_pay_channel_issue,false)
          and tf.processing_status in ('PENDING_AUTH'::public.ts_fin_processing_status_enum,
            'READY_FOR_HR'::public.ts_fin_processing_status_enum,'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum)
          and tf.hours_day=coalesce((v_unit#>>'{A_hours,hours_day}')::numeric,0)
          and tf.hours_night=coalesce((v_unit#>>'{A_hours,hours_night}')::numeric,0)
          and tf.hours_sat=coalesce((v_unit#>>'{A_hours,hours_sat}')::numeric,0)
          and tf.hours_sun=coalesce((v_unit#>>'{A_hours,hours_sun}')::numeric,0)
          and tf.hours_bh=coalesce((v_unit#>>'{A_hours,hours_bh}')::numeric,0)) then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_TSFIN_NOT_SETTLED' using errcode='55000';
      end if;
    end if;
  end loop;

  if v_action='VALIDATE' then
    if not exists(select 1 from public.audit_events ae where ae.action='IMPORT_REVIEW_RECONCILIATION_VALIDATED'
      and ae.after_json->>'operation_id'=p_operation_id::text
      and ae.after_json->>'request_hash'=v_operation.request_hash
      and ae.after_json->'unit_fingerprints'=v_unit_fingerprints) then
      perform public._audit_insert('import_apply_operations',p_operation_id::text,'IMPORT_REVIEW_RECONCILIATION_VALIDATED',null,
        jsonb_build_object('import_id',p_import_id,'operation_id',p_operation_id,'request_hash',v_operation.request_hash,
          'unit_fingerprints',v_unit_fingerprints),
        'IMPORT_REVIEW',p_actor_user_id);
    end if;
    return jsonb_build_object('ok',true,'action','VALIDATE','unit_count',jsonb_array_length(v_units),'idempotent',false);
  end if;

  select coalesce(array_agg(distinct q.timesheet_id order by q.timesheet_id),array[]::uuid[]) into v_target_ids
  from (
    select t.timesheet_id
    from jsonb_array_elements(v_units) u
    join public.timesheets t on u->>'route' in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT')
      and t.correction_id=u->>'correction_id' and t.is_current and t.archived_at_utc is null
      and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
    where coalesce(u->>'intended_authorisation_action','LEAVE_UNAUTHORISED') in ('AUTHORISE','REAUTHORISE')
    union all
    select t.timesheet_id
    from jsonb_array_elements(v_units) u
    join public.timesheets t on u->>'route' in ('AMEND_SOURCE','AMEND_PAID_UNINVOICED_SOURCE')
      and t.timesheet_id=(u->>'source_timesheet_id')::uuid and t.is_current and t.archived_at_utc is null
    where coalesce(u->>'intended_authorisation_action','LEAVE_UNAUTHORISED') in ('AUTHORISE','REAUTHORISE')
  ) q;
  if exists(
    select 1 from unnest(v_target_ids) x(timesheet_id)
    join public.timesheets t on t.timesheet_id=x.timesheet_id
    join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
    join public.contract_weeks cw on cw.timesheet_id=t.timesheet_id
    where (t.authorised_at_server is not null)::integer
        +(tf.authorised_at_utc is not null)::integer
        +(cw.status='AUTHORISED'::public.contract_week_status_enum)::integer not in (0,3)
  ) then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_LIFECYCLE_STATE_INVALID' using errcode='55000';
  end if;
  select coalesce(array_agg(x.timesheet_id order by x.timesheet_id),array[]::uuid[]) into v_pending_target_ids
  from unnest(v_target_ids) x(timesheet_id)
  join public.timesheets t on t.timesheet_id=x.timesheet_id
  join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
  join public.contract_weeks cw on cw.timesheet_id=t.timesheet_id
  where t.authorised_at_server is null and tf.authorised_at_utc is null
    and cw.status<>'AUTHORISED'::public.contract_week_status_enum;
  v_all_authorised:=cardinality(v_target_ids)>0 and cardinality(v_pending_target_ids)=0;
  v_any_authorised:=cardinality(v_target_ids)>cardinality(v_pending_target_ids);
  if cardinality(v_pending_target_ids)>0 then
    v_capability_token:=encode(gen_random_bytes(32),'hex');
    create temporary table if not exists pg_temp.import_review_lifecycle_capability_v1(
      capability_token text not null,txid bigint not null,operation_id uuid not null,request_hash text not null,
      actor_user_id uuid not null,action text not null,action_id text not null,unit_fingerprint text not null,
      timesheet_id uuid not null,expected_timesheet_id uuid not null,expected_version integer,
      expected_row_signature text,expected_tsfin_id uuid,expected_contract_week_id uuid
    ) on commit drop;
    truncate pg_temp.import_review_lifecycle_capability_v1;
    foreach v_id in array v_pending_target_ids loop
      v_signature:=public.timesheet_lifecycle_signature_v1(v_id,null,false);
      insert into pg_temp.import_review_lifecycle_capability_v1
      select v_capability_token,txid_current(),p_operation_id,v_operation.request_hash,p_actor_user_id,'AUTHORISE',
        u->>'action_id',u->>'unit_fingerprint',v_id,v_id,t.version,
        coalesce(v_signature->>'backend_row_signature',v_signature->>'row_signature',v_signature->>'signature'),tf.id,cw.id
      from jsonb_array_elements(v_units) u join public.timesheets t on t.timesheet_id=v_id
        and ((u->>'route' in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT') and t.correction_id=u->>'correction_id')
          or (u->>'route' in ('AMEND_SOURCE','AMEND_PAID_UNINVOICED_SOURCE') and t.timesheet_id=(u->>'source_timesheet_id')::uuid))
      left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
      left join public.contract_weeks cw on cw.timesheet_id=t.timesheet_id;
    end loop;
    perform set_config('cloudtms.import_reconciliation_capability_token',v_capability_token,true);
    perform set_config('cloudtms.import_reconciliation_operation_id',p_operation_id::text,true);
    perform set_config('cloudtms.import_reconciliation_action','AUTHORISE',true);
    select jsonb_agg(jsonb_build_object('timesheet_id',x) order by x) into v_items from unnest(v_pending_target_ids) x;
    v_result:=public.timesheet_authorise_bulk_atomic(v_items,p_actor_user_id,coalesce(p_now_utc,now()));
    if coalesce((v_result->>'ok')::boolean,false) is not true or coalesce((v_result->>'all_success')::boolean,false) is not true then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_AUTHORISE_INCOMPLETE' using errcode='55000',detail=coalesce(v_result->>'error_code','bulk authorise incomplete');
    end if;
    truncate pg_temp.import_review_lifecycle_capability_v1;
    perform set_config('cloudtms.import_reconciliation_capability_token','',true);
    perform set_config('cloudtms.import_reconciliation_action','',true);
  end if;
  if exists(
    select 1 from unnest(v_target_ids) x(timesheet_id)
    left join public.timesheets t on t.timesheet_id=x.timesheet_id and t.is_current and t.archived_at_utc is null
    left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
    left join public.contract_weeks cw on cw.timesheet_id=t.timesheet_id
    where t.timesheet_id is null or tf.id is null or cw.id is null
      or t.authorised_at_server is null or tf.authorised_at_utc is null
      or cw.status<>'AUTHORISED'::public.contract_week_status_enum
  ) then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_AUTHORISE_INCOMPLETE' using errcode='55000';
  end if;
  if not exists(select 1 from public.audit_events ae where ae.action='IMPORT_REVIEW_RECONCILIATION_AUTHORISED'
      and ae.after_json->>'operation_id'=p_operation_id::text and ae.after_json->>'request_hash'=v_operation.request_hash
      and ae.after_json->'unit_fingerprints'=v_unit_fingerprints) then
    perform public._audit_insert('import_apply_operations',p_operation_id::text,'IMPORT_REVIEW_RECONCILIATION_AUTHORISED',null,
      jsonb_build_object('import_id',p_import_id,'operation_id',p_operation_id,'request_hash',v_operation.request_hash,
        'unit_fingerprints',v_unit_fingerprints,'timesheet_ids',to_jsonb(coalesce(v_target_ids,array[]::uuid[]))),
      'IMPORT_REVIEW',p_actor_user_id);
  end if;
  return jsonb_build_object('ok',true,'action','AUTHORISE','unit_count',jsonb_array_length(v_units),
    'timesheet_ids',to_jsonb(coalesce(v_target_ids,array[]::uuid[])),
    'newly_authorised_timesheet_ids',to_jsonb(coalesce(v_pending_target_ids,array[]::uuid[])),
    'idempotent',v_all_authorised,'bulk_result',v_result);
end
$function$;

alter function public.import_review_correction_generation_transition_v1(uuid,uuid,text,text,uuid,text[],timestamptz) owner to postgres;
revoke all on function public.import_review_correction_generation_transition_v1(uuid,uuid,text,text,uuid,text[],timestamptz) from public,anon,authenticated;
grant execute on function public.import_review_correction_generation_transition_v1(uuid,uuid,text,text,uuid,text[],timestamptz) to service_role;

create or replace function public._import_review_apply_complete_core_v1(p_import_id uuid,p_operation_id uuid,p_actor_user_id uuid,p_response_json jsonb,p_follow_up_required boolean)
returns jsonb language plpgsql security definer set search_path to 'public','pg_temp' as $function$
declare
  v public.import_review_states%rowtype; o public.import_apply_operations%rowtype;
  v_tsfin_required boolean; v_email_required boolean; v_response jsonb; v_refresh jsonb;
  v_selected_ids text[]; v_remaining_blockers integer; v_remaining_selectable integer;
  v_terminal boolean; v_result_status text;
begin
  select * into v from public.import_review_states where import_id=p_import_id for update;
  select * into o from public.import_apply_operations where id=p_operation_id and import_id=p_import_id for update;
  if v.status<>'APPLYING' or v.last_operation_id<>p_operation_id or o.id is null then
    raise exception 'IMPORT_REVIEW_APPLY_COMPLETION_MISMATCH' using errcode='40001';
  end if;
  select coalesce(array_agg(value order by value),array[]::text[]) into v_selected_ids
  from jsonb_array_elements_text(coalesce(o.response_json#>'{request_envelope,selected_action_ids}','[]'::jsonb)) value;
  if cardinality(v_selected_ids)=0 then
    raise exception 'IMPORT_REVIEW_APPLY_COMPLETION_ACTION_SET_MISSING' using errcode='55000';
  end if;
  v_tsfin_required:=jsonb_typeof(coalesce(p_response_json->'affected_timesheet_ids','[]'::jsonb))='array'
    and jsonb_array_length(coalesce(p_response_json->'affected_timesheet_ids','[]'::jsonb))>0;
  v_email_required:=jsonb_typeof(coalesce(p_response_json->'post_commit_email_action_ids','[]'::jsonb))='array'
    and jsonb_array_length(coalesce(p_response_json->'post_commit_email_action_ids','[]'::jsonb))>0;
  v_response:=coalesce(p_response_json,'{}'::jsonb)||jsonb_build_object(
    'review_tsfin_follow_up_status',case when v_tsfin_required then 'PENDING' else 'NOT_REQUIRED' end,
    'review_email_follow_up_status',case when v_email_required then 'PENDING' else 'NOT_REQUIRED' end,
    'applied_action_ids',to_jsonb(v_selected_ids),'applied_action_count',cardinality(v_selected_ids));

  insert into public.import_review_action_outcomes(
    action_id,import_id,operation_id,action_kind,source_identity,candidate_id,client_id,contract_id,
    hr_row_id,timesheet_id,shift_id,evidence_fingerprint,completed_label,summary_json,applied_by_user_id
  )
  select d.action_id,p_import_id,p_operation_id,d.action_kind,d.source_identity,d.candidate_id,d.client_id,d.contract_id,
    d.hr_row_id,d.timesheet_id,d.shift_id,d.evidence_fingerprint,
    case d.action_kind
      when 'INCLUDE_SHIFT' then 'TMS added shift'
      when 'APPLY_AMENDMENT' then case
        when d.summary_json->>'amendment_route'='AMEND_PAID_UNINVOICED_SOURCE' then 'TMS amended the paid uninvoiced shift'
        when d.summary_json->>'amendment_route'='AMEND_EXISTING_REPLACEMENT' then 'TMS repaired the existing correction generation'
        when d.summary_json->>'amendment_route'='CREATE_REVERSAL_REPLACEMENT' then 'TMS created the required reversal and corrected-hours generation'
        else 'TMS amended the existing shift' end
      when 'APPLY_CANCELLATION' then case
        when coalesce((d.summary_json#>>'{protection,paid}')::boolean,false)
          or coalesce((d.summary_json#>>'{protection,invoice_locked}')::boolean,false)
        then 'TMS reversed shift' else 'TMS cancelled shift' end
      when 'MARK_VALIDATION_ERROR' then 'Timesheet validated'
      when 'INVALIDATE_REFERENCE' then 'Stored reference cleared'
      when 'DAILY_TIMESHEET_RESOLUTION' then 'Timesheet linked'
      when 'EMAIL_ISSUE' then 'Client query queued'
      when 'EMAIL_REMINDER' then 'Client query reminder queued'
      when 'NO_ACTION' then 'No action confirmed'
      else 'Review action completed' end,
    d.summary_json,p_actor_user_id
  from public.import_review_decisions d
  where d.import_id=p_import_id and d.is_current and d.action_id=any(v_selected_ids)
    and d.candidate_id is not null and d.client_id is not null
  on conflict(action_id) do nothing;
  if (select count(*) from public.import_review_action_outcomes x
      where x.import_id=p_import_id and x.operation_id=p_operation_id)<>cardinality(v_selected_ids) then
    raise exception 'IMPORT_REVIEW_APPLY_OUTCOME_SET_MISMATCH' using errcode='55000';
  end if;

  update public.import_review_daily_timesheet_resolutions r set status='APPLIED',applied_operation_id=p_operation_id,applied_at_utc=now(),updated_at_utc=now()
  where r.import_id=p_import_id and r.status='CURRENT' and exists(
    select 1 from public.import_review_decisions d where d.import_id=r.import_id and d.hr_row_id=r.hr_row_id
      and d.is_current and d.action_id=any(v_selected_ids));

  update public.import_review_states set status='IN_REVIEW',state_version=state_version+1,
    follow_up_status=case when v_tsfin_required or v_email_required or p_follow_up_required then 'PENDING' else 'NOT_REQUIRED' end,
    follow_up_error_code=null,follow_up_error_message=null,
    updated_at_utc=now(),updated_by_user_id=p_actor_user_id
  where import_id=p_import_id returning * into v;

  v_refresh:=public._import_review_refresh_core_v1(p_import_id,v.state_version,p_actor_user_id,5000);
  select count(*) filter(where d.blocking),count(*) filter(where d.selectable and not (
      d.action_kind='NO_ACTION' and exists(select 1 from public.import_review_action_outcomes x
        where x.import_id=d.import_id and x.source_identity=d.source_identity)))
    into v_remaining_blockers,v_remaining_selectable
  from public.import_review_decisions d where d.import_id=p_import_id and d.is_current;
  v_terminal:=v_remaining_blockers=0 and v_remaining_selectable=0;
  if v_terminal then
    update public.import_review_states set status='APPLIED',state_version=state_version+1,
      applied_at_utc=now(),applied_by_user_id=p_actor_user_id,
      updated_at_utc=now(),updated_by_user_id=p_actor_user_id
    where import_id=p_import_id returning * into v;
    update public.hr_imports set applied_at=coalesce(applied_at,now()) where id=p_import_id;
  else
    select * into v from public.import_review_states where import_id=p_import_id;
  end if;
  v_result_status:=v.status;
  v_response:=v_response||jsonb_build_object(
    'partial_application',not v_terminal,
    'review_status_after_commit',v_result_status,
    'remaining_blocker_count',v_remaining_blockers,
    'remaining_selectable_count',v_remaining_selectable);
  update public.import_apply_operations
  set state=case when v_tsfin_required or v_email_required or p_follow_up_required then 'SOURCE_COMMITTED_TSFIN_PENDING' else 'COMPLETE' end,
    committed_at_utc=coalesce(committed_at_utc,now()),
    finalised_at_utc=case when v_tsfin_required or v_email_required or p_follow_up_required then finalised_at_utc else coalesce(finalised_at_utc,now()) end,
    response_json=response_json||v_response,updated_at_utc=now()
  where id=p_operation_id;
  insert into public.import_review_events(import_id,state_version,operation_id,event_code,actor_user_id,event_context_json)
  values(p_import_id,v.state_version,p_operation_id,'APPLY_COMMITTED',p_actor_user_id,jsonb_build_object(
    'follow_up_status',v.follow_up_status,'partial_application',not v_terminal,
    'applied_action_count',cardinality(v_selected_ids),'remaining_blocker_count',v_remaining_blockers,
    'remaining_selectable_count',v_remaining_selectable));
  return jsonb_build_object('ok',true,'status',v.status,'follow_up_status',v.follow_up_status,
    'state_version',v.state_version,'partial_application',not v_terminal,
    'applied_action_count',cardinality(v_selected_ids),'remaining_blocker_count',v_remaining_blockers,
    'remaining_selectable_count',v_remaining_selectable);
end $function$;

create or replace function public._import_review_follow_up_reconcile_core_v1(p_import_id uuid,p_operation_id uuid,p_actor_user_id uuid)
returns jsonb language plpgsql security definer set search_path to 'public','pg_temp' as $function$
declare
  v public.import_review_states%rowtype; o public.import_apply_operations%rowtype;
  v_ts text; v_email text; v_aggregate text; v_error_code text; v_error_message text;
  v_ts_error_code text; v_ts_error_message text; v_email_error_code text; v_email_error_message text;
begin
  select * into v from public.import_review_states where import_id=p_import_id for update;
  select * into o from public.import_apply_operations where id=p_operation_id and import_id=p_import_id for update;
  if v.status not in ('IN_REVIEW','BLOCKED','READY','APPLIED')
    or v.last_operation_id is distinct from p_operation_id or o.id is null then
    raise exception 'IMPORT_REVIEW_FOLLOW_UP_RECONCILE_MISMATCH' using errcode='55000';
  end if;
  v_ts:=upper(coalesce(o.response_json->>'review_tsfin_follow_up_status','NOT_REQUIRED'));
  v_email:=upper(coalesce(o.response_json->>'review_email_follow_up_status','NOT_REQUIRED'));
  if v_ts not in ('PENDING','COMPLETE','FAILED_RETRYABLE','NOT_REQUIRED')
    or v_email not in ('PENDING','COMPLETE','FAILED_RETRYABLE','NOT_REQUIRED') then
    raise exception 'IMPORT_REVIEW_FOLLOW_UP_COMPONENT_STATE_INVALID' using errcode='23514';
  end if;
  v_ts_error_code:=nullif(o.response_json->>'review_tsfin_follow_up_error_code','');
  v_ts_error_message:=nullif(o.response_json->>'review_tsfin_follow_up_error_message','');
  v_email_error_code:=nullif(o.response_json->>'review_email_follow_up_error_code','');
  v_email_error_message:=nullif(o.response_json->>'review_email_follow_up_error_message','');
  v_aggregate:=case when v_ts='FAILED_RETRYABLE' or v_email='FAILED_RETRYABLE' then 'FAILED_RETRYABLE'
    when v_ts in ('COMPLETE','NOT_REQUIRED') and v_email in ('COMPLETE','NOT_REQUIRED')
      then case when v_ts='NOT_REQUIRED' and v_email='NOT_REQUIRED' then 'NOT_REQUIRED' else 'COMPLETE' end
    else 'PENDING' end;
  if v_aggregate='FAILED_RETRYABLE' then
    if v_email='FAILED_RETRYABLE' and v_ts='FAILED_RETRYABLE' then
      v_error_code:='MULTIPLE_FOLLOW_UP_COMPONENTS_FAILED';
      v_error_message:='Email and TSFIN follow-up require retry.';
    elsif v_email='FAILED_RETRYABLE' then
      v_error_code:=coalesce(v_email_error_code,'EMAIL_FOLLOW_UP_FAILED');
      v_error_message:=coalesce(v_email_error_message,'Query email follow-up failed and can be retried safely.');
    else
      v_error_code:=coalesce(v_ts_error_code,'TSFIN_FOLLOW_UP_FAILED');
      v_error_message:=coalesce(v_ts_error_message,'TSFIN follow-up failed and can be retried safely.');
    end if;
  end if;
  update public.import_review_states set follow_up_status=v_aggregate,
    follow_up_error_code=v_error_code,
    follow_up_error_message=v_error_message,
    state_version=state_version+case when follow_up_status is distinct from v_aggregate then 1 else 0 end,
    updated_at_utc=now(),updated_by_user_id=p_actor_user_id
  where import_id=p_import_id returning * into v;
  if v_aggregate in ('COMPLETE','NOT_REQUIRED') then
    update public.import_apply_operations set state='COMPLETE',finalised_at_utc=coalesce(finalised_at_utc,now()),updated_at_utc=now()
    where id=p_operation_id;
  end if;
  return jsonb_build_object('ok',true,'status',v.status,'follow_up_status',v.follow_up_status,
    'follow_up_error_code',v.follow_up_error_code,'follow_up_error_message',v.follow_up_error_message,
    'state_version',v.state_version,
    'tsfin_follow_up_status',v_ts,'tsfin_follow_up_error_code',v_ts_error_code,'tsfin_follow_up_error_message',v_ts_error_message,
    'email_follow_up_status',v_email,'email_follow_up_error_code',v_email_error_code,'email_follow_up_error_message',v_email_error_message);
end $function$;

create or replace function public.import_review_follow_up_component_update_v1(
  p_import_id uuid,p_operation_id uuid,p_request_hash text,p_component text,
  p_expected_component_status text,p_new_component_status text,
  p_error_code text default null,p_error_message text default null,p_actor_user_id uuid default null)
returns jsonb language plpgsql security definer set search_path to 'public','pg_temp' as $function$
declare
  v public.import_review_states%rowtype; o public.import_apply_operations%rowtype; v_result jsonb;
  v_component text:=upper(btrim(coalesce(p_component,'')));
  v_expected text:=upper(btrim(coalesce(p_expected_component_status,'')));
  v_new text:=upper(btrim(coalesce(p_new_component_status,'')));
  v_current text; v_status_key text; v_error_code_key text; v_error_message_key text; v_replay boolean:=false;
begin
  perform public._import_review_assert_actor_v1(p_actor_user_id);
  if p_import_id is null or p_operation_id is null or p_request_hash is null or btrim(p_request_hash)!~'^[0-9a-f]{64}$' then
    raise exception 'IMPORT_REVIEW_FOLLOW_UP_COMPONENT_INPUT_INVALID' using errcode='22023';
  end if;
  if v_component not in ('EMAIL','TSFIN') or v_expected not in ('PENDING','COMPLETE','FAILED_RETRYABLE','NOT_REQUIRED')
    or v_new not in ('PENDING','COMPLETE','FAILED_RETRYABLE','NOT_REQUIRED') then
    raise exception 'IMPORT_REVIEW_FOLLOW_UP_COMPONENT_INPUT_INVALID' using errcode='22023';
  end if;
  if length(coalesce(p_error_code,''))>128 or length(coalesce(p_error_message,''))>1000
    or (v_new='FAILED_RETRYABLE' and (nullif(btrim(coalesce(p_error_code,'')),'') is null or nullif(btrim(coalesce(p_error_message,'')),'') is null)) then
    raise exception 'IMPORT_REVIEW_FOLLOW_UP_COMPONENT_ERROR_INVALID' using errcode='22023';
  end if;
  select * into v from public.import_review_states where import_id=p_import_id for update;
  if not found then raise exception 'IMPORT_REVIEW_NOT_FOUND' using errcode='P0002'; end if;
  if v.status not in ('IN_REVIEW','BLOCKED','READY','APPLIED') or v.last_operation_id is distinct from p_operation_id then
    raise exception 'IMPORT_REVIEW_FOLLOW_UP_NOT_COMMITTED_REVIEW' using errcode='55000';
  end if;
  select * into o from public.import_apply_operations where id=p_operation_id and import_id=p_import_id for update;
  if not found or o.committed_at_utc is null then raise exception 'IMPORT_REVIEW_OPERATION_NOT_COMMITTED' using errcode='55000'; end if;
  if o.request_hash is distinct from lower(btrim(p_request_hash)) then
    raise exception 'IMPORT_REVIEW_FOLLOW_UP_REQUEST_HASH_MISMATCH' using errcode='40001';
  end if;
  v_status_key:=case when v_component='EMAIL' then 'review_email_follow_up_status' else 'review_tsfin_follow_up_status' end;
  v_error_code_key:=case when v_component='EMAIL' then 'review_email_follow_up_error_code' else 'review_tsfin_follow_up_error_code' end;
  v_error_message_key:=case when v_component='EMAIL' then 'review_email_follow_up_error_message' else 'review_tsfin_follow_up_error_message' end;
  v_current:=upper(coalesce(o.response_json->>v_status_key,'NOT_REQUIRED'));
  if v_current not in ('PENDING','COMPLETE','FAILED_RETRYABLE','NOT_REQUIRED') then
    raise exception 'IMPORT_REVIEW_FOLLOW_UP_COMPONENT_STATE_INVALID' using errcode='23514';
  end if;
  if v_current=v_new then
    v_replay:=true;
  elsif v_current in ('COMPLETE','NOT_REQUIRED') and v_expected='PENDING' and v_new in ('COMPLETE','FAILED_RETRYABLE') then
    v_replay:=true;
  else
    if v_current<>v_expected then raise exception 'IMPORT_REVIEW_FOLLOW_UP_COMPONENT_CONFLICT' using errcode='40001'; end if;
    if not ((v_current='PENDING' and v_new in ('COMPLETE','FAILED_RETRYABLE'))
      or (v_current='FAILED_RETRYABLE' and v_new='PENDING')) then
      raise exception 'IMPORT_REVIEW_FOLLOW_UP_COMPONENT_TRANSITION_INVALID' using errcode='22023';
    end if;
    update public.import_apply_operations set response_json=response_json||jsonb_build_object(
      v_status_key,v_new,
      v_error_code_key,case when v_new='FAILED_RETRYABLE' then btrim(p_error_code) end,
      v_error_message_key,case when v_new='FAILED_RETRYABLE' then btrim(p_error_message) end),
      updated_at_utc=now() where id=p_operation_id;
    if v_current='FAILED_RETRYABLE' and v_new='PENDING' then
      update public.import_review_states set follow_up_retry_count=follow_up_retry_count+1,
        updated_at_utc=now(),updated_by_user_id=p_actor_user_id where import_id=p_import_id;
    end if;
  end if;
  v_result:=public._import_review_follow_up_reconcile_core_v1(p_import_id,p_operation_id,p_actor_user_id);
  select * into v from public.import_review_states where import_id=p_import_id;
  if not v_replay then
    insert into public.import_review_events(import_id,state_version,operation_id,event_code,actor_user_id,event_context_json)
    values(p_import_id,v.state_version,p_operation_id,v_component||'_FOLLOW_UP_'||v_new,p_actor_user_id,
      jsonb_strip_nulls(jsonb_build_object('previous_component_status',v_current,'error_code',p_error_code)));
  end if;
  return v_result||jsonb_build_object('component',v_component,'component_status',case when v_replay then v_current else v_new end,
    'replay',v_replay,'retry_count',v.follow_up_retry_count);
end $function$;

create or replace function public.import_review_apply_status_get_v1(p_import_id uuid,p_operation_id uuid,p_request_hash text default null)
returns jsonb language plpgsql stable security definer set search_path to 'public','pg_temp' as $function$
declare s public.import_review_states%rowtype; o public.import_apply_operations%rowtype;
begin select * into s from public.import_review_states where import_id=p_import_id; if not found then raise exception 'IMPORT_REVIEW_NOT_FOUND' using errcode='P0002'; end if;
  if p_operation_id is not null then select * into o from public.import_apply_operations where id=p_operation_id and import_id=p_import_id; end if;
  if o.id is not null and p_request_hash is not null and o.request_hash<>btrim(p_request_hash) then return jsonb_build_object('ok',false,'status','OPERATION_REQUEST_MISMATCH'); end if;
  return jsonb_build_object('ok',true,'schema_contract_version',s.schema_contract_version,
    'import_id',p_import_id,'review_status',s.status,'follow_up_status',s.follow_up_status,'state_version',s.state_version,
    'follow_up_error_code',s.follow_up_error_code,'follow_up_error_message',s.follow_up_error_message,
    'follow_up_retry_count',s.follow_up_retry_count,
    'operation_id',o.id,'operation_state',o.state,'outcome',case when o.committed_at_utc is not null and s.follow_up_status in ('PENDING','FAILED_RETRYABLE') then 'COMMITTED_WITH_FOLLOW_UP_PENDING'
      when o.committed_at_utc is not null and s.status='APPLIED' then 'COMMITTED_APPLIED'
      when o.committed_at_utc is not null then 'COMMITTED_PARTIAL'
      when s.status='APPLYING' then 'IN_PROGRESS' when s.status in ('ABANDONED','SUPERSEDED') then s.status
      when o.id is null then 'NOT_STARTED' when o.state='FAILED_BEFORE_COMMIT' then 'FAILED_BEFORE_COMMIT' else 'NOT_COMMITTED' end,
    'stored_response',case when o.committed_at_utc is not null then o.response_json end,'committed_at_utc',o.committed_at_utc); end $function$;

create or replace function public.import_review_apply_failed_before_commit_recover_v1(
  p_import_id uuid,p_operation_id uuid,p_request_hash text,p_actor_user_id uuid default null)
returns jsonb language plpgsql security definer set search_path to 'public','pg_temp' as $function$
declare s public.import_review_states%rowtype; o public.import_apply_operations%rowtype; v_status text; v_blockers integer;
begin
  perform public._import_review_assert_actor_v1(p_actor_user_id);
  if p_import_id is null or p_operation_id is null or btrim(coalesce(p_request_hash,''))!~'^[0-9a-f]{64}$' then
    raise exception 'IMPORT_REVIEW_APPLY_RECOVERY_INPUT_INVALID' using errcode='22023';
  end if;
  select * into s from public.import_review_states where import_id=p_import_id for update;
  select * into o from public.import_apply_operations where id=p_operation_id and import_id=p_import_id for update;
  if s.import_id is null or o.id is null then raise exception 'IMPORT_REVIEW_APPLY_RECOVERY_NOT_FOUND' using errcode='P0002'; end if;
  if o.request_hash is distinct from lower(btrim(p_request_hash)) then
    raise exception 'IMPORT_REVIEW_APPLY_RECOVERY_REQUEST_MISMATCH' using errcode='40001';
  end if;
  if s.status<>'APPLYING' or s.last_operation_id is distinct from p_operation_id
     or o.state<>'FAILED_BEFORE_COMMIT' or o.committed_at_utc is not null then
    raise exception 'IMPORT_REVIEW_APPLY_RECOVERY_NOT_ALLOWED' using errcode='55000';
  end if;
  select count(*) into v_blockers from public.import_review_decisions d
  where d.import_id=p_import_id and d.is_current and d.blocking;
  v_status:=case when v_blockers>0 then 'BLOCKED' else 'READY' end;
  update public.import_review_states set status=v_status,state_version=state_version+1,
    updated_at_utc=now(),updated_by_user_id=p_actor_user_id
  where import_id=p_import_id returning * into s;
  insert into public.import_review_events(import_id,state_version,operation_id,event_code,actor_user_id,event_context_json)
  values(p_import_id,s.state_version,p_operation_id,'APPLY_FAILED_BEFORE_COMMIT_RECOVERED',p_actor_user_id,
    jsonb_build_object('operation_state',o.state,'source_committed',false));
  return jsonb_build_object('ok',true,'import_id',p_import_id,'operation_id',p_operation_id,
    'source_committed',false,'status',s.status,'state_version',s.state_version);
end $function$;

create or replace function public._import_review_state_guard_v1()
returns trigger
language plpgsql
security definer
set search_path to 'public','pg_temp'
as $function$
declare
  v_transition_allowed boolean:=false;
  v_old_without_allowed jsonb;
  v_new_without_allowed jsonb;
begin
  if tg_op='DELETE' then raise exception 'IMPORT_REVIEW_STATE_DELETE_BLOCKED' using errcode='55000'; end if;
  if new.import_id is distinct from old.import_id or new.schema_contract_version is distinct from old.schema_contract_version then
    raise exception 'IMPORT_REVIEW_STATE_IDENTITY_IMMUTABLE' using errcode='55000';
  end if;
  if new.state_version<old.state_version then raise exception 'IMPORT_REVIEW_STATE_VERSION_REGRESSION' using errcode='23514'; end if;
  if old.status in ('APPLIED','ABANDONED','SUPERSEDED') then
    if old.status='APPLIED' then
      v_old_without_allowed:=to_jsonb(old)-array['state_version','follow_up_status','follow_up_error_code','follow_up_error_message','follow_up_retry_count','last_opened_at_utc','last_opened_by_user_id','updated_at_utc','updated_by_user_id'];
      v_new_without_allowed:=to_jsonb(new)-array['state_version','follow_up_status','follow_up_error_code','follow_up_error_message','follow_up_retry_count','last_opened_at_utc','last_opened_by_user_id','updated_at_utc','updated_by_user_id'];
    else
      v_old_without_allowed:=to_jsonb(old)-array['last_opened_at_utc','last_opened_by_user_id','updated_at_utc','updated_by_user_id'];
      v_new_without_allowed:=to_jsonb(new)-array['last_opened_at_utc','last_opened_by_user_id','updated_at_utc','updated_by_user_id'];
    end if;
    if v_old_without_allowed is distinct from v_new_without_allowed then raise exception 'IMPORT_REVIEW_TERMINAL_STATE_IMMUTABLE' using errcode='55000'; end if;
    return new;
  end if;
  v_transition_allowed:=case old.status
    when 'STAGED' then new.status in ('STAGED','IN_REVIEW','BLOCKED','READY','ABANDONED','SUPERSEDED')
    when 'IN_REVIEW' then new.status in ('IN_REVIEW','BLOCKED','READY','ABANDONED','SUPERSEDED')
    when 'BLOCKED' then new.status in ('IN_REVIEW','BLOCKED','READY','APPLYING','ABANDONED','SUPERSEDED')
    when 'READY' then new.status in ('IN_REVIEW','BLOCKED','READY','APPLYING','APPLIED','ABANDONED','SUPERSEDED')
    when 'APPLYING' then new.status in ('APPLYING','IN_REVIEW','BLOCKED','READY','APPLIED')
    else false end;
  if not v_transition_allowed then
    raise exception 'IMPORT_REVIEW_STATUS_TRANSITION_INVALID' using errcode='23514',detail=jsonb_build_object('old_status',old.status,'new_status',new.status)::text;
  end if;
  if new.status is distinct from old.status and new.state_version<=old.state_version then raise exception 'IMPORT_REVIEW_STATUS_TRANSITION_REQUIRES_VERSION' using errcode='23514'; end if;
  if new.status='APPLYING' and new.last_operation_id is null then raise exception 'IMPORT_REVIEW_APPLYING_OPERATION_REQUIRED' using errcode='23514'; end if;
  if new.status='APPLIED' and (new.applied_at_utc is null or new.applied_by_user_id is null or new.last_operation_id is null) then raise exception 'IMPORT_REVIEW_APPLIED_METADATA_REQUIRED' using errcode='23514'; end if;
  if new.status='ABANDONED' and (new.abandoned_at_utc is null or new.abandoned_by_user_id is null or nullif(btrim(new.abandoned_reason),'') is null) then raise exception 'IMPORT_REVIEW_ABANDONED_METADATA_REQUIRED' using errcode='23514'; end if;
  if new.status='SUPERSEDED' and (new.superseded_at_utc is null or new.superseded_by_user_id is null) then raise exception 'IMPORT_REVIEW_SUPERSEDED_METADATA_REQUIRED' using errcode='23514'; end if;
  return new;
end
$function$;

create or replace function public._import_review_action_outcomes_immutable_guard_v1()
returns trigger
language plpgsql
security definer
set search_path to 'public','pg_temp'
as $function$
begin
  raise exception 'IMPORT_REVIEW_ACTION_OUTCOMES_ARE_APPEND_ONLY' using errcode='55000';
end
$function$;

do $outcome_trigger$
begin
  if to_regclass('public.import_review_action_outcomes') is not null then
    execute 'drop trigger if exists trg_import_review_action_outcomes_immutable on public.import_review_action_outcomes';
    execute 'create trigger trg_import_review_action_outcomes_immutable before update or delete on public.import_review_action_outcomes for each row execute function public._import_review_action_outcomes_immutable_guard_v1()';
  end if;
end
$outcome_trigger$;

revoke all on function public.import_review_contract_version_get_v1() from public,anon,authenticated;
grant execute on function public.import_review_contract_version_get_v1() to service_role;
revoke all on function public.import_review_create_v1(uuid,text,date,date,jsonb,jsonb,text,text,uuid,text) from public,anon,authenticated;
grant execute on function public.import_review_create_v1(uuid,text,date,date,jsonb,jsonb,text,text,uuid,text) to service_role;
revoke all on function public.import_review_replace_v1(uuid,text,date,date,jsonb,jsonb,text,text,uuid,text,uuid,bigint) from public,anon,authenticated;
grant execute on function public.import_review_replace_v1(uuid,text,date,date,jsonb,jsonb,text,text,uuid,text,uuid,bigint) to service_role;
revoke all on function public.import_review_list_v1(text,text,uuid,date,date,timestamptz,uuid,integer) from public,anon,authenticated;
grant execute on function public.import_review_list_v1(text,text,uuid,date,date,timestamptz,uuid,integer) to service_role;
revoke all on function public.import_review_get_v1(uuid,uuid,text,integer,bigint,integer) from public,anon,authenticated;
grant execute on function public.import_review_get_v1(uuid,uuid,text,integer,bigint,integer) to service_role;
revoke all on function public.import_review_save_v1(uuid,bigint,integer,text,jsonb,jsonb,uuid,uuid) from public,anon,authenticated;
grant execute on function public.import_review_save_v1(uuid,bigint,integer,text,jsonb,jsonb,uuid,uuid) to service_role;
revoke all on function public.import_review_refresh_v1(uuid,bigint,uuid,integer) from public,anon,authenticated;
grant execute on function public.import_review_refresh_v1(uuid,bigint,uuid,integer) to service_role;
revoke all on function public.import_review_abandon_v1(uuid,bigint,text,uuid) from public,anon,authenticated;
grant execute on function public.import_review_abandon_v1(uuid,bigint,text,uuid) to service_role;
revoke all on function public.import_review_apply_guard_v1(uuid,bigint,text,text,uuid,text,jsonb,jsonb,uuid) from public,anon,authenticated,service_role;
revoke all on function public._import_review_apply_complete_core_v1(uuid,uuid,uuid,jsonb,boolean) from public,anon,authenticated,service_role;
revoke all on function public._import_review_follow_up_reconcile_core_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;
revoke all on function public._import_review_state_guard_v1() from public,anon,authenticated,service_role;
revoke all on function public._import_review_action_outcomes_immutable_guard_v1() from public,anon,authenticated,service_role;
revoke all on function public.import_review_follow_up_component_update_v1(uuid,uuid,text,text,text,text,text,text,uuid) from public,anon,authenticated;
grant execute on function public.import_review_follow_up_component_update_v1(uuid,uuid,text,text,text,text,text,text,uuid) to service_role;
revoke all on function public.import_review_apply_status_get_v1(uuid,uuid,text) from public,anon,authenticated;
grant execute on function public.import_review_apply_status_get_v1(uuid,uuid,text) to service_role;
revoke all on function public.import_review_apply_failed_before_commit_recover_v1(uuid,uuid,text,uuid) from public,anon,authenticated;
grant execute on function public.import_review_apply_failed_before_commit_recover_v1(uuid,uuid,text,uuid) to service_role;
