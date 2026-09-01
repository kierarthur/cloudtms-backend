-- Installed-definition and ACL proof for current-payable Candidate Banking
-- groups. Historical Timesheet context without a selectable Ready member is
-- not a visible group; current single-row financial presentations remain.
\set ON_ERROR_STOP on

do $verify$
declare
  outer_definition text;detail_definition text;
  outer_signature regprocedure:='public.pay_workbench_session_get_candidate_ready_page_v1(uuid,uuid,jsonb,uuid,text,integer)'::regprocedure;
  detail_signature regprocedure:='public.pay_workbench_session_get_candidate_ready_group_page_v1(uuid,uuid,jsonb,uuid,text,text,text,integer)'::regprocedure;
begin
  select pg_get_functiondef(outer_signature) into strict outer_definition;
  select pg_get_functiondef(detail_signature) into strict detail_definition;
  if position('WHERE r.presentation_group_kind=''ROW'' OR f.group_kind IS NOT NULL' in outer_definition)=0
     or position('''selection_group_display_amount'',CASE WHEN r.group_kind IS NOT NULL THEN to_char(r.full_display_amount' in outer_definition)=0
     or position('''selection_group_selected_display_amount'',CASE WHEN r.group_kind IS NOT NULL THEN to_char(r.selected_display_amount' in outer_definition)=0
     or position('p_group_kind=''ROW'' OR (g.group_kind=p_group_kind AND g.group_key=p_group_key)' in detail_definition)=0
     or position('''selection_group_display_amount'',CASE WHEN r.group_kind IS NOT NULL THEN to_char(f.full_display_amount' in detail_definition)=0
     or position('''selection_group_selected_display_amount'',CASE WHEN r.group_kind IS NOT NULL THEN to_char(f.selected_display_amount' in detail_definition)=0 then
    raise exception 'BANKING_PAY_CURRENT_PAYABLE_GROUPS_DEFINITION_MISSING';
  end if;
  if outer_definition~*'\m(update|insert|delete|merge)\M[[:space:]]+public\.'
     or detail_definition~*'\m(update|insert|delete|merge)\M[[:space:]]+public\.' then
    raise exception 'BANKING_PAY_CURRENT_PAYABLE_GROUPS_WRITE_FOUND';
  end if;
  if position('SET search_path TO ''''' in outer_definition)=0
     or position('SET statement_timeout TO ''3s''' in outer_definition)=0
     or position('SET lock_timeout TO ''1s''' in outer_definition)=0
     or position('SET search_path TO ''''' in detail_definition)=0
     or position('SET statement_timeout TO ''3s''' in detail_definition)=0
     or position('SET lock_timeout TO ''1s''' in detail_definition)=0 then
    raise exception 'BANKING_PAY_CURRENT_PAYABLE_GROUPS_SECURITY_ENVELOPE_DRIFT';
  end if;
  if has_function_privilege('anon',outer_signature,'EXECUTE')
     or has_function_privilege('authenticated',outer_signature,'EXECUTE')
     or not has_function_privilege('service_role',outer_signature,'EXECUTE')
     or has_function_privilege('anon',detail_signature,'EXECUTE')
     or has_function_privilege('authenticated',detail_signature,'EXECUTE')
     or not has_function_privilege('service_role',detail_signature,'EXECUTE') then
    raise exception 'BANKING_PAY_CURRENT_PAYABLE_GROUPS_ACL_DRIFT';
  end if;
end;
$verify$;
