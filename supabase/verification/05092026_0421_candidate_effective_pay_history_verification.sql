-- Candidate Current/History must use the same effective payment authority as Office.
do $candidate_effective_pay_history$
declare
  v_page_definition text;
  v_detail_definition text;
begin
  select pg_get_functiondef(
    'public.candidate_app_timesheet_page_v1(uuid,text,text,text,integer,timestamptz)'::regprocedure
  ) into v_page_definition;
  select pg_get_functiondef(
    'public.candidate_app_timesheet_detail_v1(uuid,text,uuid,uuid,uuid,timestamptz)'::regprocedure
  ) into v_detail_definition;

  if v_page_definition not like '%timesheet_summary_pay_state_cache%'
     or v_page_definition not like '%summary_state_applies%'
     or v_page_definition not like '%timesheet_pay_state%'
     or v_page_definition not like '%pay_status_code=''PAID''%' then
    raise exception 'CANDIDATE_PAGE_EFFECTIVE_PAY_AUTHORITY_MISSING';
  end if;
  if v_detail_definition not like '%timesheet_summary_pay_state_cache%'
     or v_detail_definition not like '%summary_state_applies%'
     or v_detail_definition not like '%timesheet_pay_state%'
     or v_detail_definition not like '%v_effective_pay_status_code%'
     or v_detail_definition not like '%v_effective_paid_at_utc%' then
    raise exception 'CANDIDATE_DETAIL_EFFECTIVE_PAY_AUTHORITY_MISSING';
  end if;

  if has_function_privilege('anon',
       'public.candidate_app_timesheet_page_v1(uuid,text,text,text,integer,timestamptz)',
       'EXECUTE')
     or has_function_privilege('authenticated',
       'public.candidate_app_timesheet_page_v1(uuid,text,text,text,integer,timestamptz)',
       'EXECUTE')
     or has_function_privilege('anon',
       'public.candidate_app_timesheet_detail_v1(uuid,text,uuid,uuid,uuid,timestamptz)',
       'EXECUTE')
     or has_function_privilege('authenticated',
       'public.candidate_app_timesheet_detail_v1(uuid,text,uuid,uuid,uuid,timestamptz)',
       'EXECUTE') then
    raise exception 'CANDIDATE_EFFECTIVE_PAY_BROWSER_EXECUTE_EXPOSED';
  end if;
end
$candidate_effective_pay_history$;
