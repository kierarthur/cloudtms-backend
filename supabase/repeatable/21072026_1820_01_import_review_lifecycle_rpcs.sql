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
    'review_ui_contract_version','IMPORT_REVIEW_UI_V3',
    'email_grouping_version','TIMESHEET_QUERY_RECIPIENT_EMAIL_V1',
    'legacy_contracts_supported',false
  )
$function$;

create or replace function public.import_review_create_v1(
  p_import_id uuid,
  p_coverage_mode text,
  p_coverage_start_date date,
  p_coverage_end_date date,
  p_scope_clients jsonb default '[]'::jsonb,
  p_scope_candidates jsonb default '[]'::jsonb,
  p_expected_source_file_sha256 text default null,
  p_expected_parser_version text default null,
  p_actor_user_id uuid default null,
  p_operation_key text default null
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

  v_request_hash:=public._import_review_hash_v1(jsonb_build_object(
    'import_id',p_import_id,'coverage_mode',v_mode,'coverage_start_date',p_coverage_start_date,
    'coverage_end_date',p_coverage_end_date,'clients',(select coalesce(jsonb_agg(x order by x->>'source_client_key'),'[]') from jsonb_array_elements(v_clients)x),
    'candidates',(select coalesce(jsonb_agg(x order by x->>'source_candidate_key'),'[]') from jsonb_array_elements(v_candidates)x),
    'source_file_sha256',lower(btrim(coalesce(p_expected_source_file_sha256,''))),
    'parser_version',btrim(coalesce(p_expected_parser_version,'')))::text);

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

  v_revision_group:=coalesce(v_import.revision_group_id,p_import_id);
  if v_import.revision_no is not null then v_revision_no:=v_import.revision_no;
  else select coalesce(max(hi.revision_no),0)+1 into v_revision_no from public.hr_imports hi where hi.revision_group_id=v_revision_group; end if;
  v_fingerprint:=public._import_review_hash_v1(jsonb_build_object('schema','IMPORT_REVIEW_COVERAGE_V1',
    'route',coalesce(v_import.import_scope,v_import.source_system::text),'mode',v_mode,
    'from',p_coverage_start_date,'to',p_coverage_end_date,
    'clients',(select coalesce(jsonb_agg(jsonb_build_object('key',btrim(x->>'source_client_key'),'client_id',x->>'client_id') order by x->>'source_client_key'),'[]') from jsonb_array_elements(v_clients)x),
    'candidates',(select coalesce(jsonb_agg(jsonb_build_object('key',btrim(x->>'source_candidate_key'),'candidate_id',x->>'candidate_id') order by x->>'source_candidate_key'),'[]') from jsonb_array_elements(v_candidates)x))::text);

  update public.hr_imports set revision_group_id=v_revision_group,revision_no=v_revision_no,
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

  v_overlap:=public._import_review_overlap_preflight_core_v2(
    p_import_id,v_import.source_system,coalesce(v_import.import_scope,v_import.source_system::text),
    p_coverage_start_date,p_coverage_end_date,v_clients);
  v_refresh:=public._import_review_refresh_core_v1(p_import_id,1,p_actor_user_id,500);
  return v_refresh||jsonb_build_object('schema_contract_version','IMPORT_REVIEW_DB_V1',
    'replay',false,'coverage_fingerprint',v_fingerprint,'overlapping_unfinished_reviews',v_overlap);
end
$function$;

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
  v_read_only:=v_state.status in ('APPLYING','APPLIED','ABANDONED','SUPERSEDED');
  v_allowed_commands:=to_jsonb(array_remove(array[
    case when v_state.status in ('STAGED','IN_REVIEW','BLOCKED','READY') then 'SAVE_SELECTIONS'::text end,
    case when v_state.status in ('STAGED','IN_REVIEW','BLOCKED','READY') then 'REFRESH'::text end,
    case when v_state.status in ('STAGED','IN_REVIEW','BLOCKED','READY') then 'ABANDON'::text end,
    case when v_state.status in ('STAGED','IN_REVIEW','BLOCKED','READY') then 'RESOLVE_DAILY_TIMESHEET'::text end,
    case when v_state.status='READY' then 'APPLY'::text end,
    case when v_state.status in ('APPLYING','APPLIED') then 'VIEW_APPLY_STATUS'::text end,
    case when v_state.status='APPLIED' and v_state.follow_up_status='FAILED_RETRYABLE' then 'RETRY_FOLLOW_UP'::text end
  ],null));
  select jsonb_build_object(
    'selected_total',count(*) filter(where d.selected),
    'selected_change_count',count(*) filter(where d.selected and d.action_kind in ('INCLUDE_SHIFT','APPLY_AMENDMENT','APPLY_CANCELLATION','MARK_VALIDATION_ERROR')),
    'selected_email_count',count(*) filter(where d.selected and d.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')),
    'selected_email_issue_count',count(*) filter(where d.selected and d.action_kind='EMAIL_ISSUE'),
    'selected_email_reminder_count',count(*) filter(where d.selected and d.action_kind='EMAIL_REMINDER'),
    'selected_reference_invalidation_count',count(*) filter(where d.selected and d.action_kind='INVALIDATE_REFERENCE'),
    'blocking_count',count(*) filter(where d.blocking),
    'advisory_count',count(*) filter(where d.action_kind='ADVISORY'),
    'daily_resolution_count',count(*) filter(where d.action_kind='DAILY_TIMESHEET_RESOLUTION'),
    'reconfirmation_count',count(*) filter(where d.requires_reconfirmation),
    'action_kind_counts',coalesce((select jsonb_object_agg(k.action_kind,k.item_count) from (
      select d2.action_kind,count(*)::integer item_count from public.import_review_decisions d2
      where d2.import_id=p_import_id and d2.is_current and d2.selected group by d2.action_kind order by d2.action_kind
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
      'last_operation_id',v_state.last_operation_id,'read_only',v_read_only,
      'editability',jsonb_build_object(
        'read_only',v_read_only,
        'can_edit_selections',v_state.status in ('STAGED','IN_REVIEW','BLOCKED','READY'),
        'can_resolve_daily_timesheet',v_state.status in ('STAGED','IN_REVIEW','BLOCKED','READY'),
        'can_refresh',v_state.status in ('STAGED','IN_REVIEW','BLOCKED','READY'),
        'can_abandon',v_state.status in ('STAGED','IN_REVIEW','BLOCKED','READY'),
        'can_apply',v_state.status='READY',
        'can_view_apply_status',v_state.status in ('APPLYING','APPLIED'),
        'can_retry_follow_up',v_state.status='APPLIED' and v_state.follow_up_status='FAILED_RETRYABLE',
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

create or replace function public.import_review_refresh_v1(p_import_id uuid,p_expected_state_version bigint,p_actor_user_id uuid default null,p_max_actions integer default 500)
returns jsonb language plpgsql security definer set search_path to 'public','pg_temp' as $function$
begin return public._import_review_refresh_core_v1(p_import_id,p_expected_state_version,p_actor_user_id,least(coalesce(p_max_actions,500),500)); end $function$;

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

create or replace function public.import_review_supersede_v1(p_old_import_id uuid,p_new_import_id uuid,p_expected_old_state_version bigint,p_actor_user_id uuid default null)
returns jsonb language plpgsql security definer set search_path to 'public','pg_temp' as $function$
declare old_i public.hr_imports%rowtype; new_i public.hr_imports%rowtype; old_s public.import_review_states%rowtype;
  new_s public.import_review_states%rowtype; v_revision_group uuid; v_next_revision integer;
begin perform public._import_review_assert_actor_v1(p_actor_user_id); if p_old_import_id is null or p_new_import_id is null or p_old_import_id=p_new_import_id then raise exception 'IMPORT_REVIEW_SUPERSEDE_IDS_INVALID' using errcode='22023'; end if;
  perform 1 from public.hr_imports where id in(p_old_import_id,p_new_import_id) order by id for update;
  select * into old_i from public.hr_imports where id=p_old_import_id; select * into new_i from public.hr_imports where id=p_new_import_id;
  select * into old_s from public.import_review_states where import_id=p_old_import_id for update;
  select * into new_s from public.import_review_states where import_id=p_new_import_id for update;
  if old_i.id is null or new_i.id is null or old_s.import_id is null or new_s.import_id is null then raise exception 'IMPORT_REVIEW_SUPERSEDE_NOT_FOUND' using errcode='P0002'; end if;
  if old_s.state_version<>p_expected_old_state_version then raise exception 'IMPORT_REVIEW_VERSION_CONFLICT' using errcode='40001'; end if;
  if old_s.status not in ('STAGED','IN_REVIEW','BLOCKED','READY') or new_s.status not in ('STAGED','IN_REVIEW','BLOCKED','READY')
    or new_i.applied_at is not null or new_i.supersedes_import_id is not null
  then raise exception 'IMPORT_REVIEW_SUPERSEDE_NOT_ALLOWED' using errcode='55000'; end if;
  if old_i.source_system<>new_i.source_system or coalesce(old_i.import_scope,'')<>coalesce(new_i.import_scope,'') then
    raise exception 'IMPORT_REVIEW_SUPERSEDE_ROUTE_MISMATCH' using errcode='22023'; end if;
  v_revision_group:=coalesce(old_i.revision_group_id,old_i.id);
  if new_i.revision_group_id is not null and new_i.revision_group_id not in (new_i.id,v_revision_group) then
    raise exception 'IMPORT_REVIEW_SUPERSEDE_REVISION_GROUP_MISMATCH' using errcode='22023'; end if;
  perform pg_advisory_xact_lock(hashtextextended('IMPORT_REVIEW_REVISION|'||v_revision_group::text,21072026));
  select coalesce(max(i.revision_no),0)+1 into v_next_revision from public.hr_imports i where i.revision_group_id=v_revision_group;
  update public.hr_imports set revision_group_id=v_revision_group,revision_no=v_next_revision,supersedes_import_id=old_i.id where id=new_i.id;
  update public.import_review_states set status='SUPERSEDED',state_version=state_version+1,superseded_at_utc=now(),superseded_by_user_id=p_actor_user_id,
    updated_at_utc=now(),updated_by_user_id=p_actor_user_id where import_id=old_i.id returning * into old_s;
  insert into public.import_review_events(import_id,state_version,event_code,actor_user_id,event_context_json) values(old_i.id,old_s.state_version,'REVIEW_SUPERSEDED',p_actor_user_id,jsonb_build_object('new_import_id',new_i.id));
  insert into public.import_review_events(import_id,state_version,event_code,actor_user_id,event_context_json)
  values(new_i.id,new_s.state_version,'REVIEW_SUPERSEDES_PRIOR',p_actor_user_id,jsonb_build_object('old_import_id',old_i.id,'revision_group_id',v_revision_group,'revision_no',v_next_revision));
  return jsonb_build_object('ok',true,'old_import_id',old_i.id,'new_import_id',new_i.id,'old_status',old_s.status,'old_state_version',old_s.state_version); end $function$;

create or replace function public.import_review_apply_guard_v1(
  p_import_id uuid,p_expected_state_version bigint,p_expected_coverage_fingerprint text,p_expected_preview_fingerprint text,
  p_operation_id uuid,p_request_hash text,p_selected_action_ids jsonb,p_reference_invalidation_action_ids jsonb,p_actor_user_id uuid
)
returns jsonb language plpgsql security definer set search_path to 'public','extensions','pg_temp' as $function$
declare v_s public.import_review_states%rowtype; v_i public.hr_imports%rowtype; v_o public.import_apply_operations%rowtype;
  v_ids text[]; v_db_ids text[]; v_invalidation_ids text[]; v_db_invalidation_ids text[];
  v_op_result jsonb; v_envelope jsonb; v_server_hash text; v_fresh_fingerprint text;
begin perform public._import_review_assert_actor_v1(p_actor_user_id);
  if p_operation_id is null or length(btrim(coalesce(p_request_hash,''))) not between 16 and 256
    or jsonb_typeof(coalesce(p_selected_action_ids,'[]'))<>'array'
    or jsonb_typeof(coalesce(p_reference_invalidation_action_ids,'[]'))<>'array'
    or jsonb_array_length(coalesce(p_selected_action_ids,'[]'))>500
    or jsonb_array_length(coalesce(p_reference_invalidation_action_ids,'[]'))>500
    or pg_column_size(coalesce(p_selected_action_ids,'[]'))+pg_column_size(coalesce(p_reference_invalidation_action_ids,'[]'))>262144
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
  if v_s.status='APPLIED' and v_s.last_operation_id=p_operation_id then
    select * into v_o from public.import_apply_operations where id=p_operation_id and import_id=p_import_id;
    if v_o.id is null then raise exception 'IMPORT_REVIEW_APPLIED_OPERATION_MISSING' using errcode='55000'; end if;
    if lower(btrim(p_request_hash))<>v_o.request_hash then raise exception 'IMPORT_REVIEW_OPERATION_REQUEST_MISMATCH' using errcode='23505'; end if;
    return jsonb_build_object('ok',true,'replay',true,'import_id',p_import_id,'operation_id',p_operation_id,
      'state_version',v_s.state_version,'operation_state',v_o.state,'stored_response',v_o.response_json);
  end if;
  if v_s.status<>'READY' or v_s.state_version<>p_expected_state_version or v_i.coverage_fingerprint is distinct from p_expected_coverage_fingerprint
    or v_s.preview_fingerprint is distinct from p_expected_preview_fingerprint then raise exception 'IMPORT_REVIEW_APPLY_STALE_OR_NOT_READY' using errcode='40001'; end if;
  if exists(select 1 from public.import_review_decisions where import_id=p_import_id and is_current and blocking) then raise exception 'IMPORT_REVIEW_HAS_BLOCKERS' using errcode='55000'; end if;
  select coalesce(array_agg(action_id order by action_id),array[]::text[]) into v_db_ids from public.import_review_decisions where import_id=p_import_id and is_current and selectable and selected;
  if v_ids is distinct from v_db_ids then raise exception 'IMPORT_REVIEW_SELECTED_ACTION_SET_MISMATCH' using errcode='40001'; end if;
  select coalesce(array_agg(action_id order by action_id),array[]::text[]) into v_db_invalidation_ids
  from public.import_review_decisions where import_id=p_import_id and is_current and selectable and selected
    and action_kind='INVALIDATE_REFERENCE';
  if v_invalidation_ids is distinct from v_db_invalidation_ids then
    raise exception 'IMPORT_REVIEW_INVALIDATION_ACTION_SET_MISMATCH' using errcode='40001'; end if;

  -- Deterministically lock the selected timesheet scope before the final
  -- protection read. Banking Pay keeps its own central freshness checks; these
  -- locks only ensure that a concurrent draft/import race has one winner.
  perform 1 from public.timesheets t
  where t.timesheet_id in (select d.timesheet_id from public.import_review_decisions d
    where d.import_id=p_import_id and d.is_current and d.selected and d.timesheet_id is not null)
  order by t.timesheet_id for update;
  perform 1 from public.timesheets_financials tf
  where tf.is_current and tf.timesheet_id in (select d.timesheet_id from public.import_review_decisions d
    where d.import_id=p_import_id and d.is_current and d.selected and d.timesheet_id is not null)
  order by tf.timesheet_id,tf.id for update;

  create temporary table if not exists pg_temp.review_apply_fresh_actions on commit drop as
    select * from public._import_review_action_catalog_core_v1(p_import_id,v_s.preview_generation,500) with no data;
  truncate pg_temp.review_apply_fresh_actions;
  insert into pg_temp.review_apply_fresh_actions
    select * from public._import_review_action_catalog_core_v1(p_import_id,v_s.preview_generation,500);
  select public._import_review_hash_v1(coalesce(string_agg(action_id||':'||evidence_fingerprint,'|' order by action_id),''))
  into v_fresh_fingerprint from pg_temp.review_apply_fresh_actions;
  if v_fresh_fingerprint is distinct from v_s.preview_fingerprint then
    raise exception 'IMPORT_REVIEW_APPLY_EVIDENCE_STALE' using errcode='40001'; end if;
  if exists(select 1 from pg_temp.review_apply_fresh_actions where blocking) then
    raise exception 'IMPORT_REVIEW_APPLY_REFRESH_REQUIRED' using errcode='40001'; end if;
  if exists(
    select 1 from public.import_review_decisions d
    left join pg_temp.review_apply_fresh_actions n on n.action_id=d.action_id
    where d.import_id=p_import_id and d.is_current and d.selected
      and (n.action_id is null or n.evidence_fingerprint is distinct from d.evidence_fingerprint
        or not n.selectable or n.blocking)
  ) then raise exception 'IMPORT_REVIEW_SELECTED_ACTION_STALE' using errcode='40001'; end if;
  if exists(select 1 from public.import_review_decisions d where d.import_id=p_import_id and d.is_current and d.selected
    and d.action_kind in ('INCLUDE_SHIFT','APPLY_AMENDMENT','APPLY_CANCELLATION','INVALIDATE_REFERENCE','MARK_VALIDATION_ERROR') and d.timesheet_id is not null
    and coalesce((public._import_review_timesheet_protection_core_v1(d.timesheet_id)->>'active_pay_draft')::boolean,false)) then raise exception 'BLOCKED_ACTIVE_PAY_DRAFT' using errcode='55000'; end if;
  if v_i.source_system='HEALTHROSTER_DAILY'::public.hr_source_enum and exists(
    select 1 from public.import_review_daily_timesheet_resolutions r
    where r.import_id=p_import_id and r.status='CURRENT' and r.resolved_timesheet_id is not null
      and coalesce((public._import_review_timesheet_protection_core_v1(r.resolved_timesheet_id)->>'active_pay_draft')::boolean,false)
  ) then raise exception 'BLOCKED_ACTIVE_PAY_DRAFT' using errcode='55000'; end if;
  if exists(select 1 from public.import_review_decisions d
    where d.import_id=p_import_id and d.is_current and d.selected and d.action_kind='INVALIDATE_REFERENCE'
      and coalesce((public._import_review_timesheet_protection_core_v1(d.timesheet_id)->>'protected')::boolean,false)) then
    raise exception 'IMPORT_REVIEW_REFERENCE_INVALIDATION_PROTECTED' using errcode='55000'; end if;
  v_envelope:=public._import_review_apply_envelope_core_v1(p_import_id);
  v_server_hash:=public._import_review_hash_v1(v_envelope::text);
  if lower(btrim(p_request_hash))<>v_server_hash then
    raise exception 'IMPORT_REVIEW_APPLY_REQUEST_HASH_MISMATCH' using errcode='22023',detail=jsonb_build_object('server_request_hash',v_server_hash)::text;
  end if;
  v_op_result:=public._import_apply_operation_claim_core_v2(p_operation_id,p_import_id,v_i.source_system,
    concat_ws(':',coalesce(v_i.revision_group_id,v_i.id),coalesce(v_i.revision_no,1)),v_server_hash,p_actor_user_id,v_envelope);
  update public.import_review_states set status='APPLYING',state_version=state_version+1,last_operation_id=p_operation_id,updated_at_utc=now(),updated_by_user_id=p_actor_user_id where import_id=p_import_id returning * into v_s;
  insert into public.import_review_events(import_id,state_version,operation_id,event_code,actor_user_id,event_context_json) values(p_import_id,v_s.state_version,p_operation_id,'APPLY_STARTED',p_actor_user_id,jsonb_build_object('request_hash',btrim(p_request_hash),'selected_count',cardinality(v_ids)));
  return jsonb_build_object('ok',true,'import_id',p_import_id,'operation_id',p_operation_id,'state_version',v_s.state_version,'selected_action_ids',to_jsonb(v_ids),'operation_state',v_op_result->>'state'); end $function$;

create or replace function public._import_review_apply_complete_core_v1(p_import_id uuid,p_operation_id uuid,p_actor_user_id uuid,p_response_json jsonb,p_follow_up_required boolean)
returns jsonb language plpgsql security definer set search_path to 'public','pg_temp' as $function$
declare v public.import_review_states%rowtype; v_tsfin_required boolean; v_email_required boolean; v_response jsonb;
begin select * into v from public.import_review_states where import_id=p_import_id for update; if v.status<>'APPLYING' or v.last_operation_id<>p_operation_id then raise exception 'IMPORT_REVIEW_APPLY_COMPLETION_MISMATCH' using errcode='40001'; end if;
  v_tsfin_required:=jsonb_typeof(coalesce(p_response_json->'affected_timesheet_ids','[]'::jsonb))='array'
    and jsonb_array_length(coalesce(p_response_json->'affected_timesheet_ids','[]'::jsonb))>0;
  v_email_required:=jsonb_typeof(coalesce(p_response_json->'post_commit_email_action_ids','[]'::jsonb))='array'
    and jsonb_array_length(coalesce(p_response_json->'post_commit_email_action_ids','[]'::jsonb))>0;
  v_response:=coalesce(p_response_json,'{}'::jsonb)||jsonb_build_object(
    'review_tsfin_follow_up_status',case when v_tsfin_required then 'PENDING' else 'NOT_REQUIRED' end,
    'review_email_follow_up_status',case when v_email_required then 'PENDING' else 'NOT_REQUIRED' end);
  update public.import_apply_operations
  set state=case when v_tsfin_required or v_email_required or p_follow_up_required then 'SOURCE_COMMITTED_TSFIN_PENDING' else 'COMPLETE' end,
    committed_at_utc=coalesce(committed_at_utc,now()),
    finalised_at_utc=case when v_tsfin_required or v_email_required or p_follow_up_required then finalised_at_utc else coalesce(finalised_at_utc,now()) end,
    response_json=response_json||v_response,updated_at_utc=now()
  where id=p_operation_id;
  update public.import_review_states set status='APPLIED',state_version=state_version+1,
    follow_up_status=case when v_tsfin_required or v_email_required or p_follow_up_required then 'PENDING' else 'NOT_REQUIRED' end,
    applied_at_utc=now(),applied_by_user_id=p_actor_user_id,updated_at_utc=now(),updated_by_user_id=p_actor_user_id where import_id=p_import_id returning * into v;
  update public.hr_imports set applied_at=coalesce(applied_at,now()) where id=p_import_id;
  update public.import_review_daily_timesheet_resolutions r set status='APPLIED',applied_operation_id=p_operation_id,applied_at_utc=now(),updated_at_utc=now()
  where r.import_id=p_import_id and r.status='CURRENT' and exists(
    select 1 from public.import_review_decisions d where d.import_id=r.import_id and d.hr_row_id=r.hr_row_id
      and d.is_current and d.action_kind='NO_ACTION' and d.selected);
  update public.import_review_daily_timesheet_resolutions r set status='STALE',stale_at_utc=now(),stale_reason_code='EXCLUDED_FROM_APPLY',updated_at_utc=now()
  where r.import_id=p_import_id and r.status='CURRENT';
  insert into public.import_review_events(import_id,state_version,operation_id,event_code,actor_user_id,event_context_json) values(p_import_id,v.state_version,p_operation_id,'APPLY_COMMITTED',p_actor_user_id,jsonb_build_object('follow_up_status',v.follow_up_status));
  return jsonb_build_object('ok',true,'status',v.status,'follow_up_status',v.follow_up_status,'state_version',v.state_version); end $function$;

create or replace function public._import_review_follow_up_reconcile_core_v1(p_import_id uuid,p_operation_id uuid,p_actor_user_id uuid)
returns jsonb language plpgsql security definer set search_path to 'public','pg_temp' as $function$
declare
  v public.import_review_states%rowtype; o public.import_apply_operations%rowtype;
  v_ts text; v_email text; v_aggregate text; v_error_code text; v_error_message text;
  v_ts_error_code text; v_ts_error_message text; v_email_error_code text; v_email_error_message text;
begin
  select * into v from public.import_review_states where import_id=p_import_id for update;
  select * into o from public.import_apply_operations where id=p_operation_id and import_id=p_import_id for update;
  if v.status<>'APPLIED' or v.last_operation_id is distinct from p_operation_id or o.id is null then
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
  if v.status<>'APPLIED' or v.last_operation_id is distinct from p_operation_id then
    raise exception 'IMPORT_REVIEW_FOLLOW_UP_NOT_APPLIED' using errcode='55000';
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
    'operation_id',o.id,'operation_state',o.state,'outcome',case when s.status='APPLIED' and s.follow_up_status in ('PENDING','FAILED_RETRYABLE') then 'COMMITTED_WITH_FOLLOW_UP_PENDING'
      when s.status='APPLIED' then 'COMMITTED_APPLIED' when s.status='APPLYING' then 'IN_PROGRESS' when s.status in ('ABANDONED','SUPERSEDED') then s.status
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

revoke all on function public.import_review_contract_version_get_v1() from public,anon,authenticated;
grant execute on function public.import_review_contract_version_get_v1() to service_role;
revoke all on function public.import_review_create_v1(uuid,text,date,date,jsonb,jsonb,text,text,uuid,text) from public,anon,authenticated;
grant execute on function public.import_review_create_v1(uuid,text,date,date,jsonb,jsonb,text,text,uuid,text) to service_role;
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
revoke all on function public.import_review_supersede_v1(uuid,uuid,bigint,uuid) from public,anon,authenticated;
grant execute on function public.import_review_supersede_v1(uuid,uuid,bigint,uuid) to service_role;
revoke all on function public.import_review_apply_guard_v1(uuid,bigint,text,text,uuid,text,jsonb,jsonb,uuid) from public,anon,authenticated,service_role;
revoke all on function public._import_review_apply_complete_core_v1(uuid,uuid,uuid,jsonb,boolean) from public,anon,authenticated,service_role;
revoke all on function public._import_review_follow_up_reconcile_core_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;
revoke all on function public.import_review_follow_up_component_update_v1(uuid,uuid,text,text,text,text,text,text,uuid) from public,anon,authenticated;
grant execute on function public.import_review_follow_up_component_update_v1(uuid,uuid,text,text,text,text,text,text,uuid) to service_role;
revoke all on function public.import_review_apply_status_get_v1(uuid,uuid,text) from public,anon,authenticated;
grant execute on function public.import_review_apply_status_get_v1(uuid,uuid,text) to service_role;
revoke all on function public.import_review_apply_failed_before_commit_recover_v1(uuid,uuid,text,uuid) from public,anon,authenticated;
grant execute on function public.import_review_apply_failed_before_commit_recover_v1(uuid,uuid,text,uuid) to service_role;
