do $verification$
declare
  v_definition text;
  v_field text;
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
    raise exception 'CANDIDATE_TIMESHEET_PAGE_FUNCTION_MISSING';
  end if;

  foreach v_field in array array[
    'expenses_pay_ex_vat',
    'mileage_pay_ex_vat',
    'travel_pay_ex_vat',
    'accommodation_pay_ex_vat',
    'other_pay_ex_vat'
  ] loop
    if position(
      format('COALESCE(totals.%1$s, base.%1$s, 0::numeric)',v_field)
      in v_definition
    )=0 and position(
      format('coalesce(totals.%1$s,base.%1$s,0)',v_field)
      in lower(v_definition)
    )=0 then
      raise exception 'CANDIDATE_TIMESHEET_CARD_BASE_EXPENSE_FALLBACK_MISSING:%',v_field;
    end if;
  end loop;

  if has_function_privilege(
       'anon',
       'public.candidate_app_timesheet_page_v1(uuid,text,text,text,integer,timestamp with time zone)',
       'EXECUTE'
     ) or has_function_privilege(
       'authenticated',
       'public.candidate_app_timesheet_page_v1(uuid,text,text,text,integer,timestamp with time zone)',
       'EXECUTE'
     ) then
    raise exception 'CANDIDATE_TIMESHEET_PAGE_BROWSER_EXECUTE_EXPOSED';
  end if;
  if not has_function_privilege(
       'service_role',
       'public.candidate_app_timesheet_page_v1(uuid,text,text,text,integer,timestamp with time zone)',
       'EXECUTE'
     ) then
    raise exception 'CANDIDATE_TIMESHEET_PAGE_SERVICE_EXECUTE_MISSING';
  end if;
end;
$verification$;
