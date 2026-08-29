create or replace function public.hr_weekly_candidate_not_worked_resolution_save_v1(
  p_import_id uuid,
  p_action_id text,
  p_confirmed boolean,
  p_expected_state_version bigint,
  p_expected_preview_generation integer,
  p_expected_evidence_fingerprint text,
  p_actor_user_id uuid default null,
  p_request_id uuid default null
)
returns jsonb
language plpgsql
security definer
set search_path to 'public','extensions','pg_temp'
as $function$
declare
  v_state public.import_review_states%rowtype;
  v_action public.import_review_decisions%rowtype;
  v_existing public.import_review_weekly_validation_resolutions%rowtype;
  v_preview jsonb;
  v_preview_evidence jsonb;
  v_preview_timesheet_id uuid;
  v_match_count integer:=0;
  v_hash text;
  v_prior jsonb;
  v_refresh jsonb;
begin
  perform public._import_review_assert_actor_v1(p_actor_user_id);
  if p_import_id is null or p_request_id is null or p_confirmed is null
     or coalesce(p_action_id,'')!~'^[0-9a-f]{64}$'
     or coalesce(p_expected_evidence_fingerprint,'')!~'^[0-9a-f]{64}$' then
    raise exception 'HR_WEEKLY_CANDIDATE_NOT_WORKED_INPUT_INVALID' using errcode='22023';
  end if;

  v_hash:=public._import_review_hash_v1(concat_ws('|','hr-weekly-candidate-not-worked-resolution-v1',
    p_import_id,p_action_id,p_confirmed,p_expected_state_version,p_expected_preview_generation,
    p_expected_evidence_fingerprint));

  select event_context_json into v_prior
  from public.import_review_events
  where import_id=p_import_id and operation_id=p_request_id
    and event_code='WEEKLY_CANDIDATE_NOT_WORKED_RESOLUTION_SAVED'
  order by id desc limit 1;
  if found then
    if v_prior->>'request_hash'<>v_hash then
      raise exception 'HR_WEEKLY_CANDIDATE_NOT_WORKED_REQUEST_CONFLICT' using errcode='23505';
    end if;
    return jsonb_build_object('ok',true,'replay',true,'import_id',p_import_id,
      'hr_row_id',v_prior->>'hr_row_id','confirmed',(v_prior->>'confirmed')::boolean,
      'state_version',(v_prior->>'resulting_state_version')::bigint,'status',v_prior->>'status');
  end if;

  select * into v_state
  from public.import_review_states where import_id=p_import_id for update;
  if not found then raise exception 'IMPORT_REVIEW_NOT_FOUND' using errcode='P0002'; end if;
  if v_state.status not in ('BLOCKED','READY','IN_REVIEW')
     or v_state.state_version<>p_expected_state_version
     or v_state.preview_generation<>p_expected_preview_generation then
    raise exception 'HR_WEEKLY_CANDIDATE_NOT_WORKED_REVIEW_STALE' using errcode='40001';
  end if;

  select * into v_action
  from public.import_review_decisions d
  where d.import_id=p_import_id and d.action_id=p_action_id and d.is_current
    and d.hr_row_id is not null and d.timesheet_id is not null
    and d.summary_json->>'source_route'='HR_WEEKLY'
    and d.summary_json->>'authority_mode'='VALIDATION_ONLY'
    and d.summary_json->>'resolution_kind'='WEEKLY_CANDIDATE_DID_NOT_WORK'
    and (
      (p_confirmed and d.action_kind='ADVISORY' and d.summary_json->>'reason_code'='WEEKLY_SHIFT_ABSENT_FROM_TIMESHEET')
      or (not p_confirmed and d.action_kind='NO_ACTION' and d.summary_json->>'reason_code'='CANDIDATE_DID_NOT_WORK_CONFIRMED')
    )
  for update;
  if not found or v_action.evidence_fingerprint<>p_expected_evidence_fingerprint then
    raise exception 'HR_WEEKLY_CANDIDATE_NOT_WORKED_EVIDENCE_STALE' using errcode='40001';
  end if;

  -- Re-run the bounded server comparison.  The option exists only when an
  -- actual submitted Weekly schedule is present and one exact HR row is absent.
  v_preview:=public.hr_weekly_validation_preview(p_import_id);
  with preview_evidence as (
    select nullif(row_json->>'timesheet_id','')::uuid timesheet_id,cx.value evidence_json
    from jsonb_array_elements(coalesce(v_preview->'rows','[]'::jsonb)) rows(row_json)
    cross join lateral jsonb_array_elements(coalesce(row_json->'comparisons','[]'::jsonb)) cx(value)
    where cx.value->>'match_status'='HR_ONLY'
      and nullif(cx.value->>'hr_row_id','')::uuid=v_action.hr_row_id
    union all
    select nullif(row_json->>'timesheet_id','')::uuid timesheet_id,cx.value evidence_json
    from jsonb_array_elements(coalesce(v_preview->'rows','[]'::jsonb)) rows(row_json)
    cross join lateral jsonb_array_elements(coalesce(row_json->'confirmed_exceptions','[]'::jsonb)) cx(value)
    where nullif(cx.value->>'hr_row_id','')::uuid=v_action.hr_row_id
  )
  select count(*),min(timesheet_id::text)::uuid,min(evidence_json::text)::jsonb
  into v_match_count,v_preview_timesheet_id,v_preview_evidence
  from preview_evidence;

  if v_match_count<>1
     or v_preview_timesheet_id is distinct from v_action.timesheet_id
     or coalesce(v_preview_evidence->>'exception_evidence_fingerprint',v_preview_evidence->>'evidence_fingerprint')
        is distinct from v_action.evidence_fingerprint then
    raise exception 'HR_WEEKLY_CANDIDATE_NOT_WORKED_EVIDENCE_STALE' using errcode='40001';
  end if;

  if not exists (
    select 1
    from public.timesheets t
    join public.contracts c on c.id=t.contract_id
    left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
    where t.timesheet_id=v_action.timesheet_id and t.is_current
      and t.revoked_at is null and t.archived_at_utc is null
      and t.sheet_scope='WEEKLY'::public.timesheet_scope_enum
      and c.candidate_id=v_action.candidate_id and c.client_id=v_action.client_id
      and t.week_ending_date=nullif(v_action.summary_json->>'week_ending_date','')::date
      and (
        (jsonb_typeof(t.actual_schedule_json)='array' and jsonb_array_length(t.actual_schedule_json)>0)
        or (
          jsonb_typeof(tf.invoice_breakdown_json)='object'
          and jsonb_typeof(tf.invoice_breakdown_json->'segments')='array'
          and jsonb_array_length(tf.invoice_breakdown_json->'segments')>0
        )
      )
  ) then
    raise exception 'HR_WEEKLY_CANDIDATE_NOT_WORKED_TIMESHEET_NOT_SUBMITTED' using errcode='40001';
  end if;

  select * into v_existing
  from public.import_review_weekly_validation_resolutions
  where import_id=p_import_id and hr_row_id=v_action.hr_row_id for update;
  if found and v_existing.status='APPLIED' then
    if p_confirmed and v_existing.evidence_fingerprint=v_action.evidence_fingerprint then
      return jsonb_build_object('ok',true,'replay',true,'immutable',true,'import_id',p_import_id,
        'hr_row_id',v_action.hr_row_id,'confirmed',true,'state_version',v_state.state_version,'status',v_state.status);
    end if;
    raise exception 'HR_WEEKLY_CANDIDATE_NOT_WORKED_APPLIED_IMMUTABLE' using errcode='55000';
  end if;

  insert into public.import_review_weekly_validation_resolutions(
    import_id,hr_row_id,timesheet_id,resolution_code,status,evidence_fingerprint,
    preview_generation,state_version,selected_by_user_id,stale_at_utc,stale_reason_code
  ) values (
    p_import_id,v_action.hr_row_id,v_action.timesheet_id,'CANDIDATE_DID_NOT_WORK',
    case when p_confirmed then 'CURRENT' else 'CLEARED' end,
    v_action.evidence_fingerprint,v_state.preview_generation,v_state.state_version,p_actor_user_id,
    case when p_confirmed then null else now() end,
    case when p_confirmed then null else 'USER_CLEARED' end
  )
  on conflict(import_id,hr_row_id) do update set
    timesheet_id=excluded.timesheet_id,resolution_code=excluded.resolution_code,status=excluded.status,
    evidence_fingerprint=excluded.evidence_fingerprint,preview_generation=excluded.preview_generation,
    state_version=excluded.state_version,selected_at_utc=now(),selected_by_user_id=excluded.selected_by_user_id,
    stale_at_utc=excluded.stale_at_utc,stale_reason_code=excluded.stale_reason_code,updated_at_utc=now();

  update public.import_review_states
  set state_version=state_version+1,status='IN_REVIEW',updated_at_utc=now(),updated_by_user_id=p_actor_user_id
  where import_id=p_import_id returning * into v_state;

  insert into public.import_review_events(import_id,state_version,operation_id,event_code,actor_user_id,event_context_json)
  values(p_import_id,v_state.state_version,p_request_id,'WEEKLY_CANDIDATE_NOT_WORKED_RESOLUTION_SAVED',p_actor_user_id,
    jsonb_build_object('request_hash',v_hash,'action_id',p_action_id,'hr_row_id',v_action.hr_row_id,
      'timesheet_id',v_action.timesheet_id,'confirmed',p_confirmed,
      'evidence_fingerprint',v_action.evidence_fingerprint,
      'resulting_state_version',v_state.state_version,'status',v_state.status));

  v_refresh:=public._import_review_refresh_core_v1(p_import_id,v_state.state_version,p_actor_user_id,500);
  return v_refresh||jsonb_build_object('replay',false,'hr_row_id',v_action.hr_row_id,
    'timesheet_id',v_action.timesheet_id,'confirmed',p_confirmed);
end
$function$;

alter function public.hr_weekly_candidate_not_worked_resolution_save_v1(
  uuid,text,boolean,bigint,integer,text,uuid,uuid
) owner to postgres;
revoke all on function public.hr_weekly_candidate_not_worked_resolution_save_v1(
  uuid,text,boolean,bigint,integer,text,uuid,uuid
) from public,anon,authenticated;
grant execute on function public.hr_weekly_candidate_not_worked_resolution_save_v1(
  uuid,text,boolean,bigint,integer,text,uuid,uuid
) to postgres,service_role;
