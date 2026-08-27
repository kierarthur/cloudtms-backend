do $verification$
declare
  v_definition text;
begin
  select pg_get_functiondef(
    'private._candidate_timesheet_reject_rotate_v1(uuid,uuid,text,uuid,timestamptz)'::regprocedure
  ) into v_definition;
  if position("'ELECTRONIC'" in v_definition)=0
     or position("'MANUAL'" in v_definition)>0 then
    raise exception 'Candidate electronic rejection rotation body is not current';
  end if;

  if has_function_privilege(
       'public','private._candidate_timesheet_reject_rotate_v1(uuid,uuid,text,uuid,timestamptz)','EXECUTE'
     )
     or has_function_privilege(
       'anon','private._candidate_timesheet_reject_rotate_v1(uuid,uuid,text,uuid,timestamptz)','EXECUTE'
     )
     or has_function_privilege(
       'authenticated','private._candidate_timesheet_reject_rotate_v1(uuid,uuid,text,uuid,timestamptz)','EXECUTE'
     )
     or has_function_privilege(
       'service_role','private._candidate_timesheet_reject_rotate_v1(uuid,uuid,text,uuid,timestamptz)','EXECUTE'
     ) then
    raise exception 'Candidate electronic rejection rotation helper is externally executable';
  end if;

  if exists (
    select 1
    from public.timesheets current_timesheet
    join public.contract_weeks week_row
      on week_row.timesheet_id=current_timesheet.timesheet_id
    join public.audit_events rotation_audit
      on rotation_audit.object_type='timesheet'
     and rotation_audit.object_id_text=current_timesheet.timesheet_id::text
     and rotation_audit.action='CANDIDATE_ELECTRONIC_REJECTED_VERSION_ROTATED'
     and rotation_audit.after_json->>'new_timesheet_id'=current_timesheet.timesheet_id::text
    join public.timesheets previous_timesheet
      on previous_timesheet.timesheet_id=
        case
          when rotation_audit.before_json->>'old_timesheet_id'
            ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
          then (rotation_audit.before_json->>'old_timesheet_id')::uuid
          else null
        end
     and previous_timesheet.submission_mode='ELECTRONIC'
    where current_timesheet.is_current=true
      and current_timesheet.submission_mode='MANUAL'
      and current_timesheet.status='RECEIVED'
      and current_timesheet.archived_at_utc is null
      and current_timesheet.authorised_at_server is null
      and week_row.status='OPEN'
  ) then
    raise exception 'Unrepaired electronic rejection replacement remains installed';
  end if;
end;
$verification$;
