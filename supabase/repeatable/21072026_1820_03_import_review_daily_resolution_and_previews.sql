-- Daily evidence association and bounded route previews.

create or replace function public.hr_daily_timesheet_resolution_save_v1(
  p_import_id uuid,p_hr_row_id uuid,p_timesheet_id uuid,p_expected_state_version bigint,
  p_expected_preview_generation integer,p_expected_evidence_fingerprint text,
  p_actor_user_id uuid default null,p_request_id uuid default null
)
returns jsonb language plpgsql security definer set search_path to 'public','extensions','pg_temp' as $function$
declare
  v_state public.import_review_states%rowtype; v_action public.import_review_decisions%rowtype;
  v_hr public.hr_rows%rowtype; v_ts public.v_timesheets_daily_match%rowtype;
  v_ts_row public.timesheets%rowtype; v_contract public.contracts%rowtype; v_contract_id uuid;
  v_existing public.import_review_daily_timesheet_resolutions%rowtype;
  v_mapping public.hr_daily_grade_role_mappings%rowtype; v_mapping_count integer; v_timesheet_evidence jsonb;
  v_refresh jsonb; v_hash text; v_prior jsonb;
begin
  perform public._import_review_assert_actor_v1(p_actor_user_id);
  if p_request_id is null or p_import_id is null or p_hr_row_id is null then
    raise exception 'HR_DAILY_RESOLUTION_INPUT_INVALID' using errcode='22023';
  end if;
  v_hash:=public._import_review_hash_v1(concat_ws('|','daily-resolution-v1',p_import_id,p_hr_row_id,p_timesheet_id,
    p_expected_state_version,p_expected_preview_generation,p_expected_evidence_fingerprint));
  select event_context_json into v_prior from public.import_review_events
  where import_id=p_import_id and operation_id=p_request_id and event_code='DAILY_TIMESHEET_RESOLUTION_SAVED'
  order by id desc limit 1;
  if found then
    if v_prior->>'request_hash'<>v_hash then raise exception 'HR_DAILY_RESOLUTION_REQUEST_CONFLICT' using errcode='23505'; end if;
    return jsonb_build_object('ok',true,'replay',true,'import_id',p_import_id,'hr_row_id',p_hr_row_id,
      'state_version',(v_prior->>'resulting_state_version')::bigint,'status',v_prior->>'status');
  end if;
  select * into v_state from public.import_review_states where import_id=p_import_id for update;
  if not found then raise exception 'IMPORT_REVIEW_NOT_FOUND' using errcode='P0002'; end if;
  if v_state.status not in ('BLOCKED','READY','IN_REVIEW') or v_state.state_version<>p_expected_state_version
    or v_state.preview_generation<>p_expected_preview_generation then
    raise exception 'HR_DAILY_RESOLUTION_REVIEW_STALE' using errcode='40001';
  end if;
  select * into v_action from public.import_review_decisions
  where import_id=p_import_id and hr_row_id=p_hr_row_id and is_current
    and action_kind in ('DAILY_TIMESHEET_RESOLUTION','ADVISORY','NO_ACTION')
  order by case action_kind when 'DAILY_TIMESHEET_RESOLUTION' then 0 when 'ADVISORY' then 1 else 2 end limit 1 for update;
  if not found or v_action.evidence_fingerprint<>p_expected_evidence_fingerprint then
    raise exception 'HR_DAILY_RESOLUTION_EVIDENCE_STALE' using errcode='40001';
  end if;
  select * into v_existing from public.import_review_daily_timesheet_resolutions
  where import_id=p_import_id and hr_row_id=p_hr_row_id for update;
  if found and v_existing.status='APPLIED' then
    if v_existing.resolved_timesheet_id is not distinct from p_timesheet_id then
      return jsonb_build_object('ok',true,'replay',true,'immutable',true,'import_id',p_import_id,'hr_row_id',p_hr_row_id,
        'timesheet_id',p_timesheet_id,'state_version',v_state.state_version,'status',v_state.status);
    end if;
    raise exception 'HR_DAILY_RESOLUTION_APPLIED_IMMUTABLE' using errcode='55000';
  end if;
  select * into v_hr from public.hr_rows where id=p_hr_row_id and import_id=p_import_id;
  if not found then raise exception 'HR_DAILY_RESOLUTION_ROW_NOT_FOUND' using errcode='P0002'; end if;

  if p_timesheet_id is null then
    insert into public.import_review_daily_timesheet_resolutions(import_id,hr_row_id,resolved_timesheet_id,resolution_method,status,
      evidence_fingerprint,preview_generation,state_version,selected_by_user_id,stale_at_utc,stale_reason_code)
    values(p_import_id,p_hr_row_id,null,'USER_SELECTED','CLEARED',v_action.evidence_fingerprint,v_state.preview_generation,v_state.state_version,
      p_actor_user_id,now(),'USER_CLEARED')
    on conflict(import_id,hr_row_id) do update set resolved_timesheet_id=null,resolution_method='USER_SELECTED',status='CLEARED',
      evidence_fingerprint=excluded.evidence_fingerprint,preview_generation=excluded.preview_generation,state_version=excluded.state_version,
      selected_at_utc=now(),selected_by_user_id=excluded.selected_by_user_id,stale_at_utc=now(),stale_reason_code='USER_CLEARED',updated_at_utc=now();
  else
    select count(*) into v_mapping_count
    from public.hr_daily_grade_role_mappings gm
    where gm.client_id=v_action.client_id and gm.active
      and gm.incoming_grade_norm=lower(btrim(coalesce(nullif(v_hr.assignment_grade_norm,''),
        v_hr.payload_json->>'grade_raw',v_hr.payload_json->>'Request_Grade','')));
    if v_mapping_count<>1 then
      raise exception 'HR_DAILY_RESOLUTION_GRADE_MAPPING_STALE' using errcode='40001';
    end if;
    select * into v_mapping from public.hr_daily_grade_role_mappings gm
    where gm.client_id=v_action.client_id and gm.active
      and gm.incoming_grade_norm=lower(btrim(coalesce(nullif(v_hr.assignment_grade_norm,''),
        v_hr.payload_json->>'grade_raw',v_hr.payload_json->>'Request_Grade','')))
    order by gm.updated_at desc,gm.id limit 1 for update;
    select * into v_ts from public.v_timesheets_daily_match t where t.timesheet_id=p_timesheet_id;
    if not found or v_ts.candidate_id is distinct from v_action.candidate_id or v_ts.client_id is distinct from v_action.client_id
      or v_ts.sheet_scope::text<>'DAILY'
      or (v_ts.worked_start_iso at time zone 'Europe/London')::date<>v_hr.date_local then
      raise exception 'HR_DAILY_RESOLUTION_TARGET_OUTSIDE_ROW_SCOPE' using errcode='22023';
    end if;
    if lower(btrim(coalesce(v_ts.tsfin_role,''))) is distinct from lower(btrim(coalesce(v_mapping.role_code,'')))
      or (nullif(btrim(coalesce(v_mapping.band_norm,'')),'') is not null
        and lower(btrim(coalesce(v_ts.tsfin_band,''))) is distinct from lower(btrim(v_mapping.band_norm))) then
      raise exception 'HR_DAILY_RESOLUTION_GRADE_ROLE_MISMATCH' using errcode='22023';
    end if;
    select * into v_ts_row from public.timesheets t
    where t.timesheet_id=p_timesheet_id and t.is_current and t.revoked_at is null
    order by t.updated_at desc limit 1 for update;
    if not found then
      raise exception 'HR_DAILY_RESOLUTION_TIMESHEET_STALE' using errcode='40001';
    end if;
    v_contract_id:=v_ts_row.contract_id;
    select * into v_contract from public.contracts c where c.id=v_contract_id for update;
    if not found
      or v_contract.candidate_id is distinct from v_action.candidate_id
      or v_contract.client_id is distinct from v_action.client_id
      or v_contract.start_date>v_hr.date_local
      or (v_contract.end_date is not null and v_contract.end_date<v_hr.date_local)
      or lower(btrim(coalesce(v_contract.role,''))) is distinct from lower(btrim(coalesce(v_mapping.role_code,'')))
      or (nullif(btrim(coalesce(v_mapping.band_norm,'')),'') is not null
        and lower(btrim(coalesce(v_contract.band,''))) is distinct from lower(btrim(v_mapping.band_norm)))
      or not coalesce((select a.route_eligible
        from public._import_review_effective_authority_core_v1(
          'HR_DAILY',v_contract.id,v_contract.client_id,v_hr.date_local) a),false) then
      raise exception 'HR_DAILY_RESOLUTION_CONTRACT_OUT_OF_SCOPE' using errcode='22023';
    end if;
    if v_action.contract_id is not null and v_contract_id is distinct from v_action.contract_id then
      raise exception 'HR_DAILY_RESOLUTION_CONTRACT_MISMATCH' using errcode='22023';
    end if;
    select jsonb_strip_nulls(jsonb_build_object('timesheet_id',t.timesheet_id,'updated_at',t.updated_at,
      'contract_id',t.contract_id,'contract_updated_at',v_contract.updated_at,
      'authority_fingerprint',(select a.authority_fingerprint
        from public._import_review_effective_authority_core_v1('HR_DAILY',v_contract.id,v_contract.client_id,v_hr.date_local) a),
      'candidate_id',v_ts.candidate_id,'client_id',v_ts.client_id,
      'worked_start_iso',v_ts.worked_start_iso,'worked_end_iso',v_ts.worked_end_iso,
      'worked_minutes',v_ts.worked_minutes,'break_minutes',v_ts.break_minutes,
      'tsfin_role',v_ts.tsfin_role,'tsfin_band',v_ts.tsfin_band))
      into v_timesheet_evidence from public.timesheets t where t.timesheet_id=p_timesheet_id and t.is_current;
    insert into public.import_review_daily_timesheet_resolutions(import_id,hr_row_id,resolved_timesheet_id,resolution_method,status,
      evidence_fingerprint,preview_generation,state_version,selected_by_user_id)
    values(p_import_id,p_hr_row_id,p_timesheet_id,'USER_SELECTED','CURRENT',v_action.evidence_fingerprint,v_state.preview_generation,v_state.state_version,p_actor_user_id)
    on conflict(import_id,hr_row_id) do update set resolved_timesheet_id=excluded.resolved_timesheet_id,resolution_method='USER_SELECTED',status='CURRENT',
      evidence_fingerprint=excluded.evidence_fingerprint,preview_generation=excluded.preview_generation,state_version=excluded.state_version,
      selected_at_utc=now(),selected_by_user_id=excluded.selected_by_user_id,stale_at_utc=null,stale_reason_code=null,updated_at_utc=now();
  end if;
  update public.import_review_states set state_version=state_version+1,status='IN_REVIEW',updated_at_utc=now(),updated_by_user_id=p_actor_user_id
  where import_id=p_import_id returning * into v_state;
  insert into public.import_review_events(import_id,state_version,operation_id,event_code,actor_user_id,event_context_json)
  values(p_import_id,v_state.state_version,p_request_id,'DAILY_TIMESHEET_RESOLUTION_SAVED',p_actor_user_id,jsonb_build_object(
    'request_hash',v_hash,'hr_row_id',p_hr_row_id,'timesheet_id',p_timesheet_id,
    'mapping_evidence',case when p_timesheet_id is null then null else jsonb_strip_nulls(jsonb_build_object(
      'mapping_id',v_mapping.id,'mapping_updated_at',v_mapping.updated_at,'mapped_role',v_mapping.role_code,'mapped_band',v_mapping.band_norm)) end,
    'timesheet_evidence',case when p_timesheet_id is null then null else v_timesheet_evidence end,
    'resulting_state_version',v_state.state_version,'status',v_state.status));
  v_refresh:=public._import_review_refresh_core_v1(p_import_id,v_state.state_version,p_actor_user_id,500);
  return v_refresh||jsonb_build_object('replay',false,'hr_row_id',p_hr_row_id,'timesheet_id',p_timesheet_id,
    'resolution_method',case when p_timesheet_id is null then 'CLEARED' else 'USER_SELECTED' end);
end $function$;

create or replace function public.nhsp_weekly_review_preview_v1(
  p_import_id uuid,p_after_action_id text default null,p_limit integer default 100
)
returns jsonb language plpgsql security definer set search_path to 'public','pg_temp' as $function$
declare v_limit integer:=least(greatest(coalesce(p_limit,100),1),200); v_state public.import_review_states%rowtype; v_actions jsonb; v_last text;
begin
  select * into v_state from public.import_review_states where import_id=p_import_id;
  if not found then raise exception 'IMPORT_REVIEW_NOT_FOUND' using errcode='P0002'; end if;
  with p as (select * from public.import_review_decisions d where d.import_id=p_import_id and d.is_current
    and d.action_kind in ('INCLUDE_SHIFT','APPLY_AMENDMENT','APPLY_CANCELLATION','ADVISORY','NO_ACTION')
    and (p_after_action_id is null or d.action_id>p_after_action_id) order by d.action_id limit v_limit+1),
  l as (select * from p order by action_id limit v_limit)
  select coalesce(jsonb_agg(jsonb_build_object('action_id',action_id,'action_kind',action_kind,'category',action_category,
    'evidence_fingerprint',evidence_fingerprint,'selectable',selectable,'default_selected',default_selected,'selected',selected,
    'blocking',blocking,'summary',summary_json) order by action_id),'[]'),max(action_id) into v_actions,v_last from l;
  return jsonb_build_object('ok',true,'import_id',p_import_id,'state_version',v_state.state_version,
    'preview_generation',v_state.preview_generation,'preview_fingerprint',v_state.preview_fingerprint,'actions',v_actions,
    'next_cursor',case when jsonb_array_length(v_actions)=v_limit then v_last end);
end $function$;

create or replace function public.hr_daily_validation_preview_v1(
  p_import_id uuid,p_after_action_id text default null,p_limit integer default 100
)
returns jsonb language plpgsql security definer set search_path to 'public','pg_temp' as $function$
declare v_limit integer:=least(greatest(coalesce(p_limit,100),1),200); v_state public.import_review_states%rowtype; v_actions jsonb; v_last text;
begin
  select * into v_state from public.import_review_states where import_id=p_import_id;
  if not found then raise exception 'IMPORT_REVIEW_NOT_FOUND' using errcode='P0002'; end if;
  with p as (select * from public.import_review_decisions d where d.import_id=p_import_id and d.is_current
    and (d.hr_row_id is not null or d.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER','INVALIDATE_REFERENCE','MARK_VALIDATION_ERROR'))
    and (p_after_action_id is null or d.action_id>p_after_action_id) order by d.action_id limit v_limit+1),
  l as (select * from p order by action_id limit v_limit)
  select coalesce(jsonb_agg(jsonb_build_object('action_id',action_id,'action_kind',action_kind,'category',action_category,
    'hr_row_id',hr_row_id,'timesheet_id',timesheet_id,'evidence_fingerprint',evidence_fingerprint,'selectable',selectable,
    'default_selected',default_selected,'selected',selected,'blocking',blocking,'summary',summary_json,
    'resolution',(select jsonb_build_object('timesheet_id',r.resolved_timesheet_id,'method',r.resolution_method,'status',r.status,
      'evidence_fingerprint',r.evidence_fingerprint) from public.import_review_daily_timesheet_resolutions r
      where r.import_id=p_import_id and r.hr_row_id=l.hr_row_id)) order by action_id),'[]'),max(action_id) into v_actions,v_last from l;
  return jsonb_build_object('ok',true,'import_id',p_import_id,'state_version',v_state.state_version,
    'preview_generation',v_state.preview_generation,'preview_fingerprint',v_state.preview_fingerprint,'actions',v_actions,
    'next_cursor',case when jsonb_array_length(v_actions)=v_limit then v_last end);
end $function$;

revoke all on function public.hr_daily_timesheet_resolution_save_v1(uuid,uuid,uuid,bigint,integer,text,uuid,uuid) from public,anon,authenticated;
grant execute on function public.hr_daily_timesheet_resolution_save_v1(uuid,uuid,uuid,bigint,integer,text,uuid,uuid) to service_role;
revoke all on function public.nhsp_weekly_review_preview_v1(uuid,text,integer) from public,anon,authenticated;
grant execute on function public.nhsp_weekly_review_preview_v1(uuid,text,integer) to service_role;
revoke all on function public.hr_daily_validation_preview_v1(uuid,text,integer) from public,anon,authenticated;
grant execute on function public.hr_daily_validation_preview_v1(uuid,text,integer) to service_role;
