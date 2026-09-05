\set ON_ERROR_STOP on

-- Verify the route/capability fast path remains private, retains the complete
-- response contract and evaluates representative installed data without
-- recursively querying the full Office Timesheet Summary view.
begin;
set local statement_timeout='120s';

do $verification$
declare
  v_signature regprocedure:=to_regprocedure(
    'private._candidate_route_family_v1(uuid,uuid)'
  );
  v_definition text;
  v_config text[];
  v_record record;
  v_route jsonb;
begin
  if v_signature is null then
    raise exception 'Candidate route fast path is missing';
  end if;

  select lower(pg_get_functiondef(v_signature)),p.proconfig
  into v_definition,v_config
  from pg_proc p
  where p.oid=v_signature;

  if position('security definer' in v_definition)=0
     or not coalesce(
       v_config @> array['search_path=pg_catalog, public, private, pg_temp']::text[],
       false
     ) then
    raise exception 'Candidate route fast path security contract drifted';
  end if;

  if position('private._contract_settings_effective_core_v1' in v_definition)=0
     or position('private._candidate_import_authoritative_v1' in v_definition)=0
     or position('public.v_timesheets_summary' in v_definition)>0 then
    raise exception 'Candidate route fast path dependency contract drifted';
  end if;

  if has_function_privilege('anon',v_signature,'EXECUTE')
     or has_function_privilege('authenticated',v_signature,'EXECUTE')
     or has_function_privilege('service_role',v_signature,'EXECUTE') then
    raise exception 'Candidate route fast path escaped its private boundary';
  end if;

  for v_record in
    select cw.id,cw.timesheet_id
    from public.contract_weeks cw
    order by cw.updated_at desc nulls last,cw.id
    limit 500
  loop
    v_route:=private._candidate_route_family_v1(
      v_record.timesheet_id,v_record.id
    );
    if jsonb_typeof(v_route)<>'object'
       or not v_route ?& array[
         'route_family','effective_submission_mode','pending_route_intent',
         'import_authoritative','import_source_family','qr_backed',
         'electronic_paper_fallback_enabled',
         'candidate_hours_submission_allowed','candidate_expenses_allowed',
         'candidate_paper_submission_allowed','candidate_no_work_allowed','policy'
       ]
       or v_route->>'route_family' not in (
         'IMPORT_AUTHORITATIVE','QR','ELECTRONIC','MANUAL_NON_QR'
       )
       or jsonb_typeof(v_route->'policy')<>'object'
       or coalesce(v_route#>>'{policy,policy_fingerprint}','')
          !~ '^[0-9a-f]{64}$'
       or coalesce(v_route#>>'{policy,authority_fingerprint}','')
          !~ '^[0-9a-f]{64}$' then
      raise exception 'Candidate route fast path returned an invalid contract: %',
        v_route;
    end if;
  end loop;
end;
$verification$;

rollback;
