-- Trigger helpers installed by the schema migration and retained here as
-- repeatable function definitions. They are owner-only.

create or replace function public.import_review_prune_guard_v1()
returns trigger language plpgsql security definer set search_path to 'public','pg_temp' as $function$
declare v_status text;
begin
  if old.pruned_at is null and new.pruned_at is not null then
    select s.status into v_status from public.import_review_states s where s.import_id=new.id;
    if v_status in ('STAGED','IN_REVIEW','BLOCKED','READY','APPLYING') then
      raise exception 'IMPORT_REVIEW_ACTIVE_PRUNE_BLOCKED' using errcode='55000',
        detail=jsonb_build_object('import_id',new.id,'status',v_status)::text;
    end if;
  end if;
  return new;
end
$function$;

create or replace function public._import_review_immutable_guard_v1()
returns trigger language plpgsql security definer set search_path to 'public','pg_temp' as $function$
declare
  v_locked timestamptz;
  v_import_id uuid;
  v_mode text;
  v_parent public.hr_imports%rowtype;
begin
  if tg_table_name='hr_imports' then
    if old.coverage_locked_at is not null and (
      new.coverage_mode is distinct from old.coverage_mode
      or new.coverage_start_date is distinct from old.coverage_start_date
      or new.coverage_end_date is distinct from old.coverage_end_date
      or new.coverage_fingerprint is distinct from old.coverage_fingerprint
      or new.coverage_locked_at is distinct from old.coverage_locked_at
      or new.coverage_operation_key is distinct from old.coverage_operation_key
      or new.coverage_request_hash is distinct from old.coverage_request_hash
      or new.source_file_sha256 is distinct from old.source_file_sha256
      or new.parser_version is distinct from old.parser_version
    ) then
      raise exception 'IMPORT_REVIEW_COVERAGE_IMMUTABLE' using errcode='55000';
    end if;
    if old.coverage_locked_at is not null and (
      new.revision_group_id is distinct from old.revision_group_id
      or new.revision_no is distinct from old.revision_no
      or new.supersedes_import_id is distinct from old.supersedes_import_id
    ) and (old.supersedes_import_id is not null or new.supersedes_import_id is null) then
      raise exception 'IMPORT_REVIEW_REVISION_IDENTITY_IMMUTABLE' using errcode='55000';
    end if;
    if new.supersedes_import_id is not null and (
      old.supersedes_import_id is distinct from new.supersedes_import_id
      or old.revision_group_id is distinct from new.revision_group_id
      or old.revision_no is distinct from new.revision_no
    ) then
      select p.* into v_parent from public.hr_imports p where p.id=new.supersedes_import_id;
      if not found or v_parent.id=new.id or v_parent.source_system is distinct from new.source_system
         or new.revision_group_id is distinct from coalesce(v_parent.revision_group_id,v_parent.id)
         or new.revision_no is null or new.revision_no<=coalesce(v_parent.revision_no,0) then
        raise exception 'IMPORT_REVIEW_SUPERSESSION_INCONSISTENT' using errcode='23514';
      end if;
    end if;
    if old.coverage_locked_at is null and new.coverage_locked_at is not null then
      if new.coverage_mode='COMPLETE_SELECTED_CANDIDATES' and not exists(
        select 1 from public.import_review_scope_candidates sc where sc.import_id=new.id) then
        raise exception 'IMPORT_REVIEW_SELECTED_CANDIDATE_SCOPE_REQUIRED' using errcode='23514';
      end if;
      if new.coverage_mode<>'COMPLETE_SELECTED_CANDIDATES' and exists(
        select 1 from public.import_review_scope_candidates sc where sc.import_id=new.id) then
        raise exception 'IMPORT_REVIEW_CANDIDATE_SCOPE_NOT_ALLOWED' using errcode='23514';
      end if;
    end if;
    return new;
  end if;
  if tg_op='DELETE' then v_import_id:=old.import_id; else v_import_id:=new.import_id; end if;
  if tg_table_name='import_review_scope_candidates' and tg_op<>'DELETE' then
    select hi.coverage_mode into v_mode from public.hr_imports hi where hi.id=new.import_id;
    if v_mode is distinct from 'COMPLETE_SELECTED_CANDIDATES' then
      raise exception 'IMPORT_REVIEW_CANDIDATE_SCOPE_NOT_ALLOWED' using errcode='23514';
    end if;
  end if;
  select hi.coverage_locked_at into v_locked from public.hr_imports hi where hi.id=v_import_id;
  if v_locked is null then if tg_op='DELETE' then return old; else return new; end if; end if;
  if tg_op in ('INSERT','DELETE') then raise exception 'IMPORT_REVIEW_SCOPE_IMMUTABLE' using errcode='55000'; end if;
  if tg_table_name='import_review_scope_clients' and (
    new.import_id is distinct from old.import_id or new.source_client_key is distinct from old.source_client_key
    or new.source_display_label is distinct from old.source_display_label) then
    raise exception 'IMPORT_REVIEW_SCOPE_IDENTITY_IMMUTABLE' using errcode='55000';
  end if;
  if tg_table_name='import_review_scope_candidates' and (
    new.import_id is distinct from old.import_id or new.source_candidate_key is distinct from old.source_candidate_key
    or new.source_display_label is distinct from old.source_display_label) then
    raise exception 'IMPORT_REVIEW_SCOPE_IDENTITY_IMMUTABLE' using errcode='55000';
  end if;
  return new;
end
$function$;

create or replace function public._import_review_state_guard_v1()
returns trigger language plpgsql security definer set search_path to 'public','pg_temp' as $function$
declare v_transition_allowed boolean:=false; v_old_without_allowed jsonb; v_new_without_allowed jsonb;
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
    if v_old_without_allowed is distinct from v_new_without_allowed then
      raise exception 'IMPORT_REVIEW_TERMINAL_STATE_IMMUTABLE' using errcode='55000';
    end if;
    return new;
  end if;
  v_transition_allowed:=case old.status
    when 'STAGED' then new.status in ('STAGED','IN_REVIEW','BLOCKED','READY','ABANDONED','SUPERSEDED')
    when 'IN_REVIEW' then new.status in ('IN_REVIEW','BLOCKED','READY','ABANDONED','SUPERSEDED')
    when 'BLOCKED' then new.status in ('IN_REVIEW','BLOCKED','READY','ABANDONED','SUPERSEDED')
    when 'READY' then new.status in ('IN_REVIEW','BLOCKED','READY','APPLYING','ABANDONED','SUPERSEDED')
    when 'APPLYING' then new.status in ('APPLYING','APPLIED') else false end;
  if not v_transition_allowed then
    raise exception 'IMPORT_REVIEW_STATUS_TRANSITION_INVALID' using errcode='23514',detail=jsonb_build_object('old_status',old.status,'new_status',new.status)::text;
  end if;
  if new.status is distinct from old.status and new.state_version<=old.state_version then
    raise exception 'IMPORT_REVIEW_STATUS_TRANSITION_REQUIRES_VERSION' using errcode='23514';
  end if;
  if new.status='APPLYING' and new.last_operation_id is null then raise exception 'IMPORT_REVIEW_APPLYING_OPERATION_REQUIRED' using errcode='23514'; end if;
  if new.status='APPLIED' and (new.applied_at_utc is null or new.applied_by_user_id is null or new.last_operation_id is null) then raise exception 'IMPORT_REVIEW_APPLIED_METADATA_REQUIRED' using errcode='23514'; end if;
  if new.status='ABANDONED' and (new.abandoned_at_utc is null or new.abandoned_by_user_id is null or nullif(btrim(new.abandoned_reason),'') is null) then raise exception 'IMPORT_REVIEW_ABANDONED_METADATA_REQUIRED' using errcode='23514'; end if;
  if new.status='SUPERSEDED' and (new.superseded_at_utc is null or new.superseded_by_user_id is null) then raise exception 'IMPORT_REVIEW_SUPERSEDED_METADATA_REQUIRED' using errcode='23514'; end if;
  return new;
end
$function$;

create or replace function public._import_review_daily_resolution_guard_v1()
returns trigger language plpgsql security definer set search_path to 'public','pg_temp' as $function$
begin
  if tg_op='DELETE' then
    if old.status='APPLIED' then raise exception 'IMPORT_REVIEW_APPLIED_RESOLUTION_IMMUTABLE' using errcode='55000'; end if;
    return old;
  end if;
  if new.import_id is distinct from old.import_id or new.hr_row_id is distinct from old.hr_row_id then
    raise exception 'IMPORT_REVIEW_RESOLUTION_IDENTITY_IMMUTABLE' using errcode='55000';
  end if;
  if old.status='APPLIED' and to_jsonb(new) is distinct from to_jsonb(old) then
    raise exception 'IMPORT_REVIEW_APPLIED_RESOLUTION_IMMUTABLE' using errcode='55000';
  end if;
  if old.status='APPLIED' then return new; end if;
  if not (case old.status when 'CURRENT' then new.status in ('CURRENT','STALE','CLEARED','APPLIED')
    when 'STALE' then new.status in ('STALE','CURRENT','CLEARED')
    when 'CLEARED' then new.status in ('CLEARED','CURRENT','STALE') else false end) then
    raise exception 'IMPORT_REVIEW_RESOLUTION_TRANSITION_INVALID' using errcode='23514',detail=jsonb_build_object('old_status',old.status,'new_status',new.status)::text;
  end if;
  if new.status='APPLIED' and (new.applied_operation_id is null or new.applied_at_utc is null) then
    raise exception 'IMPORT_REVIEW_APPLIED_RESOLUTION_METADATA_REQUIRED' using errcode='23514';
  end if;
  return new;
end
$function$;

create or replace function public._import_review_events_immutable_guard_v1()
returns trigger language plpgsql security definer set search_path to 'public','pg_temp' as $function$
begin raise exception 'IMPORT_REVIEW_EVENTS_ARE_APPEND_ONLY' using errcode='55000'; end
$function$;

revoke all on function public.import_review_prune_guard_v1() from public,anon,authenticated,service_role;
revoke all on function public._import_review_immutable_guard_v1() from public,anon,authenticated,service_role;
revoke all on function public._import_review_state_guard_v1() from public,anon,authenticated,service_role;
revoke all on function public._import_review_daily_resolution_guard_v1() from public,anon,authenticated,service_role;
revoke all on function public._import_review_events_immutable_guard_v1() from public,anon,authenticated,service_role;
