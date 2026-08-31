-- Installed-definition and ACL proof for the Candidate Banking display-only
-- detail-row selection tuple correction.
\set ON_ERROR_STOP on

do $verify$
declare
  definition text;
  signature regprocedure:='public.pay_workbench_session_get_candidate_ready_group_page_v1(uuid,uuid,jsonb,uuid,text,text,text,integer)'::regprocedure;
begin
  select pg_get_functiondef(signature) into strict definition;
  if position('''selection_group_member_count'',CASE WHEN r.group_kind IS NULL THEN 0 ELSE COALESCE(f.member_count,0) END' in definition)=0
     or position('''selection_group_selected_count'',CASE WHEN r.group_kind IS NULL THEN 0 ELSE COALESCE(f.selected_count,0) END' in definition)=0
     or position('''selection_group_display_amount'',CASE WHEN r.group_kind IS NOT NULL THEN to_char(f.full_display_amount' in definition)=0
     or position('''selection_group_selected_display_amount'',CASE WHEN r.group_kind IS NOT NULL THEN to_char(f.selected_display_amount' in definition)=0
     or position('''selection_group_state'',CASE WHEN r.group_kind IS NULL THEN NULL' in definition)=0 then
    raise exception 'BANKING_PAY_DISPLAY_ONLY_SELECTION_TUPLE_DEFINITION_MISSING';
  end if;
  if position('SET search_path TO ''''' in definition)=0
     or position('SET statement_timeout TO ''3s''' in definition)=0
     or position('SET lock_timeout TO ''1s''' in definition)=0 then
    raise exception 'BANKING_PAY_DISPLAY_ONLY_SELECTION_TUPLE_SECURITY_ENVELOPE_DRIFT';
  end if;
  if has_function_privilege('anon',signature,'EXECUTE')
     or has_function_privilege('authenticated',signature,'EXECUTE')
     or not has_function_privilege('service_role',signature,'EXECUTE') then
    raise exception 'BANKING_PAY_DISPLAY_ONLY_SELECTION_TUPLE_ACL_DRIFT';
  end if;
end;
$verify$;
