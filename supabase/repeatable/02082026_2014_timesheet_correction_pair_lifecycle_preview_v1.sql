create or replace function public.timesheet_correction_pair_lifecycle_preview_v1(
  p_items jsonb,
  p_action text,
  p_actor_user_id uuid,
  p_max_members integer default 100
)
returns jsonb
language plpgsql
security definer
set search_path to 'public','extensions','pg_temp'
as $function$
declare
  v_action text:=upper(btrim(coalesce(p_action,'')));
  v_item jsonb;
  v_id_text text;
  v_id uuid;
  v_class jsonb;
  v_transition jsonb;
  v_group jsonb;
  v_groups jsonb:='[]'::jsonb;
  v_seen text[]:=array[]::text[];
  v_selected_count integer:=0;
  v_affected_count integer:=0;
  v_ordinary_count integer:=0;
  v_valid boolean:=true;
  v_repairable_mixed boolean:=false;
  v_already_target_state boolean:=false;
  v_group_ready boolean:=false;
  v_group_fingerprint text;
  v_members jsonb;
  v_errors jsonb;
begin
  if v_action not in ('AUTHORISE','UNAUTHORISE') then
    raise exception 'CORRECTION_LIFECYCLE_ACTION_INVALID' using errcode='22023';
  end if;
  if p_actor_user_id is null then
    raise exception 'CORRECTION_LIFECYCLE_ACTOR_REQUIRED' using errcode='22023';
  end if;
  if p_max_members<1 or p_max_members>100
     or jsonb_typeof(coalesce(p_items,'[]'::jsonb))<>'array'
     or jsonb_array_length(coalesce(p_items,'[]'::jsonb))>p_max_members then
    raise exception 'CORRECTION_LIFECYCLE_ITEMS_INVALID' using errcode='22023';
  end if;

  v_selected_count:=jsonb_array_length(coalesce(p_items,'[]'::jsonb));
  for v_item in select value from jsonb_array_elements(coalesce(p_items,'[]'::jsonb))
  loop
    v_id_text:=nullif(btrim(coalesce(
      v_item->>'timesheet_id',v_item->>'timesheetId',
      v_item->>'current_timesheet_id',v_item->>'currentTimesheetId',
      v_item->>'requested_timesheet_id',v_item->>'requestedTimesheetId',
      case when coalesce(v_item->>'row_key','') like 'timesheet:%'
        then substring(v_item->>'row_key' from 11) end,'')), '');
    if v_id_text is null or v_id_text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
      v_valid:=false;
      v_groups:=v_groups||jsonb_build_array(jsonb_build_object(
        'valid',false,'action_ready',false,'errors',jsonb_build_array(
          jsonb_build_object('code','TIMESHEET_ID_REQUIRED'))));
      continue;
    end if;

    v_id:=v_id_text::uuid;
    v_class:=public._ctms_import_correction_classify_v1(v_id);
    if coalesce((v_class->>'is_import_authoritative_correction')::boolean,false) is not true then
      v_ordinary_count:=v_ordinary_count+1;
      continue;
    end if;
    if nullif(v_class->>'correction_id','')=any(v_seen) then continue; end if;
    v_seen:=array_append(v_seen,v_class->>'correction_id');

    v_transition:=public.timesheet_correction_pair_transition_v1(
      v_id,v_action,p_actor_user_id,null::uuid,null::text,false,p_max_members);
    v_errors:=coalesce(v_transition->'errors','[]'::jsonb);
    v_repairable_mixed:=
      coalesce((v_transition->>'expected_member_count')::integer,0)=2
      and coalesce((v_transition->>'authorised_count')::integer,0)>0
      and coalesce((v_transition->>'unauthorised_count')::integer,0)>0
      and coalesce((v_transition->>'authorised_count')::integer,0)
          +coalesce((v_transition->>'unauthorised_count')::integer,0)=2
      and coalesce((v_transition->>'paid_count')::integer,0)=0
      and coalesce((v_transition->>'invoice_lined_count')::integer,0)=0
      and (select count(*)=1 and bool_and(e->>'code'='CORRECTION_UNIT_ACTION_STATE_INVALID')
           from jsonb_array_elements(v_errors) e)
      and (v_action='UNAUTHORISE'
        or coalesce((v_transition->>'ready_count')::integer,0)=2);
    v_already_target_state:=coalesce((v_transition->>'expected_member_count')::integer,0)=2
      and coalesce((v_transition->>'paid_count')::integer,0)=0
      and coalesce((v_transition->>'invoice_lined_count')::integer,0)=0
      and ((v_action='AUTHORISE' and coalesce((v_transition->>'authorised_count')::integer,0)=2)
        or (v_action='UNAUTHORISE' and coalesce((v_transition->>'unauthorised_count')::integer,0)=2));
    v_group_ready:=coalesce((v_transition->>'action_ready')::boolean,false)
      or v_repairable_mixed or v_already_target_state;

    select coalesce(jsonb_agg(
      row_json||jsonb_build_object(
        'signed_hours',jsonb_build_object(
          'day',coalesce(tf.hours_day,0),'night',coalesce(tf.hours_night,0),
          'sat',coalesce(tf.hours_sat,0),'sun',coalesce(tf.hours_sun,0),
          'bh',coalesce(tf.hours_bh,0),
          'total',coalesce(tf.hours_day,0)+coalesce(tf.hours_night,0)+coalesce(tf.hours_sat,0)+coalesce(tf.hours_sun,0)+coalesce(tf.hours_bh,0)),
        'current_state',case when coalesce((row_json->>'timesheet_authorised')::boolean,false)
          and coalesce((row_json->>'tsfin_authorised')::boolean,false) then 'AUTHORISED' else 'UNAUTHORISED' end,
        'resulting_state',case when v_action='AUTHORISE' then 'AUTHORISED' else 'UNAUTHORISED' end)
      order by case row_json->>'correction_kind' when 'CHANGED_HOURS_REVERSAL' then 1 else 2 end),
      '[]'::jsonb)
    into v_members
    from jsonb_array_elements(coalesce(v_transition->'pair_rows','[]'::jsonb)) row_json
    left join public.timesheets_financials tf
      on tf.timesheet_id=nullif(row_json->>'timesheet_id','')::uuid and tf.is_current=true;

    v_group_fingerprint:=public._import_review_hash_v1(concat_ws('|',
      'correction-lifecycle-preview-v1',v_action,v_transition->>'chain_fingerprint',
      v_transition->>'correction_id',v_members::text,v_group_ready));
    v_group:=v_transition||jsonb_build_object(
      'valid',v_group_ready,'action_ready',v_group_ready,
      'repairing_legacy_mixed_state',v_repairable_mixed,
      'already_in_target_state',v_already_target_state,
      'source_system',case
        when exists(select 1 from jsonb_array_elements(v_members) m
          join public.timesheets t on t.timesheet_id=(m->>'timesheet_id')::uuid
          left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
          where upper(coalesce(
            t.candidate_hint_text#>>'{correction_financials_policy_envelope,classification,source_system}',
            tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,classification,source_system}',
            tf.rate_source_refs_json#>>'{correction_financials_policy_envelope,classification,source_system}',
            case when tf.basis in ('NHSP','NHSP_ADJUSTMENT') then 'NHSP' end,''))='NHSP')
        then 'NHSP' else 'HealthRoster' end,
      'members',v_members,'pair_fingerprint',v_group_fingerprint,
      'errors',case when v_repairable_mixed or v_already_target_state then '[]'::jsonb else v_errors end);
    v_groups:=v_groups||jsonb_build_array(v_group);
    v_affected_count:=v_affected_count+coalesce((v_transition->>'expected_member_count')::integer,0);
    if not v_group_ready then v_valid:=false; end if;
  end loop;

  return jsonb_build_object(
    'ok',true,'valid',v_valid,'action',v_action,
    'selected_count',v_selected_count,
    'ordinary_timesheet_count',v_ordinary_count,
    'correction_pair_count',jsonb_array_length(v_groups),
    'affected_timesheet_count',v_ordinary_count+v_affected_count,
    'groups',v_groups,
    'preview_fingerprint',public._import_review_hash_v1(concat_ws('|',
      'correction-lifecycle-batch-preview-v1',v_action,v_selected_count,v_groups::text)));
end;
$function$;

comment on function public.timesheet_correction_pair_lifecycle_preview_v1(jsonb,text,uuid,integer) is
  'Read-only server-authoritative preview for atomic NHSP/HealthRoster import-correction pair authorise and unauthorise actions.';
alter function public.timesheet_correction_pair_lifecycle_preview_v1(jsonb,text,uuid,integer) owner to postgres;
revoke all on function public.timesheet_correction_pair_lifecycle_preview_v1(jsonb,text,uuid,integer) from public,anon,authenticated;
grant execute on function public.timesheet_correction_pair_lifecycle_preview_v1(jsonb,text,uuid,integer) to service_role;
