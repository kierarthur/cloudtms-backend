do $verification$
declare
  v_transition_definition text;
  v_replacement_definition text;
begin
  select pg_get_functiondef(
    'public.candidate_workflow_transition_atomic_v1(uuid,text,uuid,text,integer,jsonb,text,timestamptz)'::regprocedure
  ) into v_transition_definition;

  if position(
       'v_source_workflow.state not in (''REJECTED'',''REFUSED'')'
       in v_transition_definition
     )=0 then
    raise exception 'Candidate manager-refused resubmission authority is not installed';
  end if;

  if position(
       'current_week.id=v_source_workflow.contract_week_id'
       in v_transition_definition
     )=0
     or position(
       'current_week.contract_id is not distinct from v_source_workflow.contract_id'
       in v_transition_definition
     )=0
     or position(
       'current_week.week_ending_date is not distinct from v_source_workflow.week_ending_date'
       in v_transition_definition
     )=0
     or position(
       'v_source_workflow.creation_identity_json#>>''{derived,daily_booking_id}'''
       in v_transition_definition
     )=0 then
    raise exception 'Candidate manager-refused resubmission does not resolve the current weekly/Daily authority';
  end if;

  if position(
       'from public.contract_weeks current_week'
       in v_transition_definition
     )=0
     or position(
       'on current_timesheet.timesheet_id=current_week.timesheet_id'
       in v_transition_definition
     )=0 then
    raise exception 'Candidate manager-refused weekly resubmission is not bound to the exact Contract Week Timesheet';
  end if;

  if position(
       'CANDIDATE_WORKFLOW_ANCHOR_MISMATCH'' using errcode=''55000'''
       in v_transition_definition
     )=0 then
    raise exception 'Candidate manager-refused anchor mismatch remains a retryable serialization failure';
  end if;

  select pg_get_functiondef(
    'private._candidate_rejection_replaced_v1(uuid)'::regprocedure
  ) into v_replacement_definition;

  if position(
       'v_rejected.state not in (''REJECTED'',''REFUSED'')'
       in v_replacement_definition
     )=0 then
    raise exception 'Candidate manager-refused replacement guard is not installed';
  end if;

  if v_transition_definition~*'pg_catalog\.(coalesce|nullif|least|greatest)\s*\('
     or v_replacement_definition~*'pg_catalog\.(coalesce|nullif|least|greatest)\s*\(' then
    raise exception 'Candidate manager-refused resubmission authority contains an illegal conditional-expression prefix';
  end if;
end;
$verification$;
