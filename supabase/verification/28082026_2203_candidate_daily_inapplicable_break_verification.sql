-- Rollback-contained real first use; no application rows or owners are changed.
\set ON_ERROR_STOP on
begin;
do $verification$
declare
  v_missing uuid:=gen_random_uuid();
  v_result jsonb;
  v_same jsonb;
  v_changed jsonb;
begin
  if exists(select 1 from public.clients where id=v_missing) then
    raise exception 'Disposable missing-client identity unexpectedly exists';
  end if;
  v_result:=private._candidate_daily_break_entry_v1(
    'verification-view-only','2026-04-03',v_missing,null,false);
  if v_result->'applicable'<>'false'::jsonb or v_result->'mode'<>'null'::jsonb
    or v_result->>'source'<>'NOT_APPLICABLE'
    or v_result->>'reason'<>'NOT_CANDIDATE_EDITABLE' then
    raise exception 'View-only Daily record did not retain a closed non-editable break context';
  end if;
  v_same:=private._candidate_daily_break_entry_v1(
    'verification-view-only','2026-04-03',v_missing,null,null);
  v_changed:=private._candidate_daily_break_entry_v1(
    'verification-view-only','2026-04-04',v_missing,null,false);
  if v_same is distinct from v_result
    or v_changed->>'context_token'=v_result->>'context_token' then
    raise exception 'Daily non-editable context lost null/date identity semantics';
  end if;
  begin
    perform private._candidate_daily_break_entry_v1(
      'verification-editable','2026-04-03',v_missing,null,true);
    raise exception 'Editable Daily incorrectly ignored missing Client settings';
  exception when no_data_found then
    if sqlerrm<>'CLIENT_OR_SETTINGS_NOT_FOUND' then raise; end if;
  end;
  v_result:=private._candidate_daily_break_entry_v1(
    'verification-unresolved','2026-04-03',null,null,true);
  if v_result->'applicable'<>'true'::jsonb or v_result->>'mode'<>'START_END_TIMES' then
    raise exception 'Unresolved Daily factual-entry rule changed';
  end if;
  begin
    perform private._candidate_daily_break_entry_v1('',null,v_missing,null,false);
    raise exception 'Non-editable context bypassed booking/date identity';
  exception when invalid_parameter_value then
    if sqlerrm<>'CANDIDATE_DAILY_IDENTITY_INVALID' then raise; end if;
  end;
  if has_function_privilege('service_role','private._candidate_daily_break_entry_v1(text,date,uuid,uuid,boolean)','EXECUTE')
    or has_function_privilege('anon','private._candidate_daily_break_entry_v1(text,date,uuid,uuid,boolean)','EXECUTE')
    or has_function_privilege('authenticated','private._candidate_daily_break_entry_v1(text,date,uuid,uuid,boolean)','EXECUTE') then
    raise exception 'Private Daily helper execution boundary changed';
  end if;
end;
$verification$;
rollback;
