-- A weekly whole-claim cancellation first returns the Contract Week to an
-- editable OPEN state. That deliberately makes the submitted Timesheet a
-- non-current REVOKED history row before the workflow is marked CANCELLED.
-- Expense-component terminalisation must therefore accept that exact retained
-- historical owner while continuing to require a current owner for every live
-- workflow state.

\set ON_ERROR_STOP on

begin;

do $candidate_terminal_expense_owner_sync$
declare
  v_definition text;
  v_old text:=E'  if v_workflow.target_timesheet_id is not null then\n    select * into v_fin from public.timesheets_financials financial\n    where financial.timesheet_id=v_workflow.target_timesheet_id and financial.is_current\n    order by financial.computed_at_utc desc nulls last,financial.updated_at desc,financial.id desc limit 1;\n    select * into v_timesheet from public.timesheets timesheet\n    where timesheet.timesheet_id=v_workflow.target_timesheet_id and timesheet.is_current;\n    if not found\n       or v_timesheet.archived_at_utc is not null\n       or v_timesheet.sheet_scope<>''WEEKLY''::public.timesheet_scope_enum then\n      raise exception ''CANDIDATE_EXPENSE_COMPONENT_OWNER_SCOPE_INVALID''\n        using errcode=''23514'';\n    end if;\n  end if;';
  v_new text:=E'  if v_workflow.target_timesheet_id is not null then\n    if v_workflow.state in (''CANCELLED'',''EXPIRED'',''SUPERSEDED'',''REJECTED'',''REFUSED'') then\n      -- Terminal workflow history keeps its exact submitted Timesheet owner.\n      -- Weekly withdrawal has already made that row non-current by design.\n      select * into v_fin from public.timesheets_financials financial\n      where financial.timesheet_id=v_workflow.target_timesheet_id\n      order by financial.is_current desc,financial.computed_at_utc desc nulls last,\n        financial.updated_at desc,financial.id desc limit 1;\n      select * into v_timesheet from public.timesheets timesheet\n      where timesheet.timesheet_id=v_workflow.target_timesheet_id;\n      if not found\n         or v_timesheet.archived_at_utc is not null\n         or v_timesheet.sheet_scope<>''WEEKLY''::public.timesheet_scope_enum then\n        raise exception ''CANDIDATE_EXPENSE_COMPONENT_OWNER_SCOPE_INVALID''\n          using errcode=''23514'';\n      end if;\n    else\n      select * into v_fin from public.timesheets_financials financial\n      where financial.timesheet_id=v_workflow.target_timesheet_id and financial.is_current\n      order by financial.computed_at_utc desc nulls last,financial.updated_at desc,financial.id desc limit 1;\n      select * into v_timesheet from public.timesheets timesheet\n      where timesheet.timesheet_id=v_workflow.target_timesheet_id and timesheet.is_current;\n      if not found\n         or v_timesheet.archived_at_utc is not null\n         or v_timesheet.sheet_scope<>''WEEKLY''::public.timesheet_scope_enum then\n        raise exception ''CANDIDATE_EXPENSE_COMPONENT_OWNER_SCOPE_INVALID''\n          using errcode=''23514'';\n      end if;\n    end if;\n  end if;';
  v_offset integer;
begin
  select pg_catalog.pg_get_functiondef(
    'private._candidate_expense_components_sync_v1(uuid,timestamptz)'::regprocedure
  ) into v_definition;
  v_definition:=pg_catalog.replace(v_definition,E'\r\n',E'\n');
  if pg_catalog.strpos(v_definition,v_new)>0 then
    return;
  end if;
  v_offset:=pg_catalog.strpos(v_definition,v_old);
  if v_offset=0
     or pg_catalog.strpos(
       pg_catalog.substr(v_definition,v_offset+pg_catalog.length(v_old)),v_old
     )>0 then
    raise exception 'CANDIDATE_TERMINAL_EXPENSE_OWNER_SYNC_DRIFT'
      using errcode='55000';
  end if;
  execute pg_catalog.replace(v_definition,v_old,v_new);
end;
$candidate_terminal_expense_owner_sync$;

alter function private._candidate_expense_components_sync_v1(uuid,timestamptz)
  owner to postgres;
revoke all on function private._candidate_expense_components_sync_v1(uuid,timestamptz)
  from public,anon,authenticated,service_role;

notify pgrst, 'reload schema';

commit;
