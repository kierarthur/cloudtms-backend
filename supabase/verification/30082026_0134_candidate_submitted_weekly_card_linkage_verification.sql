do $verification$
declare
  v_definition text;
  v_state text;
begin
  select pg_get_functiondef(p.oid)
  into v_definition
  from pg_proc p
  join pg_namespace n on n.oid=p.pronamespace
  where n.nspname='public'
    and p.proname='candidate_app_timesheet_page_v1'
    and pg_get_function_identity_arguments(p.oid)=
      'p_session_id uuid, p_environment text, p_view text, p_cursor text, p_limit integer, p_now_utc timestamp with time zone';

  if v_definition is null then
    raise exception 'CANDIDATE_SUBMITTED_WEEKLY_CARD_FUNCTION_MISSING';
  end if;

  foreach v_state in array array[
    'CREATED','WORKER_DRAFT','WORKER_SUBMITTED',
    'WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT','READY_FOR_MANAGER_APPROVAL',
    'AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED',
    'MANAGER_APPROVED_PENDING_FINAL_DOCUMENT','READY_TO_FINALISE',
    'AWAITING_PAPER_RETURN','RECEIVED','REFUSED'
  ] loop
    if position(quote_literal(v_state) in v_definition)=0 then
      raise exception 'CANDIDATE_SUBMITTED_WEEKLY_CARD_STATE_MISSING:%',v_state;
    end if;
  end loop;

  if position('classified.target_timesheet_id is null' in lower(v_definition))=0
     or position('classified.anchor_timesheet_id is null' in lower(v_definition))=0
     or position('draft_week.id=classified.contract_week_id' in lower(v_definition))=0 then
    raise exception 'CANDIDATE_SUBMITTED_WEEKLY_CARD_EXACT_WEEK_LINK_MISSING';
  end if;

  if position('{hours_submission,canonical_tsfin_snapshot,total_hours}' in lower(v_definition))=0
     or position('{expense_submission,canonical_tsfin_snapshot,expenses_pay_ex_vat}' in lower(v_definition))=0
     or position('base.timesheet_id is null' in lower(v_definition))=0
     or position('overlay_total_hours' in lower(v_definition))=0 then
    raise exception 'CANDIDATE_SUBMITTED_WEEKLY_CARD_FACTUAL_OVERLAY_MISSING';
  end if;

  if has_function_privilege('anon',
       'public.candidate_app_timesheet_page_v1(uuid,text,text,text,integer,timestamp with time zone)',
       'EXECUTE')
     or has_function_privilege('authenticated',
       'public.candidate_app_timesheet_page_v1(uuid,text,text,text,integer,timestamp with time zone)',
       'EXECUTE') then
    raise exception 'CANDIDATE_SUBMITTED_WEEKLY_CARD_BROWSER_EXECUTE_EXPOSED';
  end if;
  if not has_function_privilege('service_role',
       'public.candidate_app_timesheet_page_v1(uuid,text,text,text,integer,timestamp with time zone)',
       'EXECUTE') then
    raise exception 'CANDIDATE_SUBMITTED_WEEKLY_CARD_SERVICE_EXECUTE_MISSING';
  end if;
end;
$verification$;
