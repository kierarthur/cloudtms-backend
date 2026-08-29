-- Mandatory NEW/UPGRADE proof for the four-column Action Required list.
-- Every row is synthetic, unusable and rolled back. No Draft/provider action runs.

\set ON_ERROR_STOP on

begin;
set local statement_timeout = '45s';
set local lock_timeout = '5s';
set local client_min_messages = 'warning';

do $verify$
declare
  v_actor uuid := gen_random_uuid();
  v_candidate uuid := gen_random_uuid();
  v_snapshot uuid := gen_random_uuid();
  v_session uuid := gen_random_uuid();
  v_row uuid := gen_random_uuid();
  v_timesheet uuid := gen_random_uuid();
  v_options jsonb;
  v_result jsonb;
  v_item jsonb;
  v_before text;
  v_after text;
  v_sort text;
  v_direction text;
  v_identity regprocedure := 'public.pay_workbench_session_get_action_required_page_v1(uuid,jsonb,uuid,text,text,text,integer,text,text)'::regprocedure;
  v_definition text;
begin
  select pg_get_functiondef(v_identity) into strict v_definition;
  if not (select p.prosecdef from pg_catalog.pg_proc p where p.oid=v_identity::oid)
     or not exists (select 1 from pg_catalog.pg_proc p where p.oid=v_identity::oid
       and p.proconfig @> array['search_path=""']::text[])
     or not has_function_privilege('service_role',v_identity,'EXECUTE')
     or has_function_privilege('anon',v_identity,'EXECUTE')
     or has_function_privilege('authenticated',v_identity,'EXECUTE')
     or has_function_privilege('public',v_identity,'EXECUTE') then
    raise exception 'BANKING_PAY_ACTION_PRESENTATION_VERIFY: function security boundary changed';
  end if;
  if lower(v_definition) like '%pg_catalog.coalesce(%'
     or lower(v_definition) like '%pg_catalog.nullif(%'
     or lower(v_definition) like '%pg_catalog.least(%'
     or lower(v_definition) like '%pg_catalog.greatest(%'
     or position('offset ' in lower(v_definition))>0 then
    raise exception 'BANKING_PAY_ACTION_PRESENTATION_VERIFY: unsafe function source';
  end if;

  insert into public.tms_users(id,email,password_hash,role,is_active)
  values(v_actor,'banking-action-presentation-'||v_actor::text||'@example.invalid','UNUSABLE_VERIFICATION_ONLY','admin',true);
  insert into public.candidates(id,display_name,tms_ref,pay_method)
  values(v_candidate,'Action presentation fixture','ACTION-VERIFY','PAYE');
  insert into public.banking_pay_snapshot_runs(id,pay_date,week_ending_cutoff,pay_week_start,eligibility_from_date,eligibility_to_date)
  values(v_snapshot,'2026-08-28','2026-08-30','2026-08-24','2026-08-01','2026-08-31');
  insert into public.banking_pay_workbench_sessions(
    id,actor_user_id,pay_date,week_ending_cutoff,session_signature,source_snapshot_run_id,
    version,progress_counter_version,progress_json,scope_seed_complete
  ) values(v_session,v_actor,'2026-08-28','2026-08-30','synthetic action presentation '||v_session::text,
    v_snapshot,1,4,'{"ready":true}',true);
  insert into public.banking_pay_workbench_session_scope(
    session_id,candidate_id,scope_ordinal,status,seeded,dirty,certified_preview_publication_attestation_json
  ) values(v_session,v_candidate,1,'READY',true,false,
    '{"attestation_version":"CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3","contract_version":"3","semantic_contract_version":"READY_TO_PAY_SEMANTIC_V2"}');
  insert into public.banking_pay_workbench_preview_rows(
    id,session_id,candidate_id,section,row_key,row_ordinal,row_json,key_type,key_value,
    selected,selection_state,status,session_version
  ) values(v_row,v_session,v_candidate,'cases_resolutions','action-presentation-fixture',1,
    jsonb_build_object(
      'candidate_name','Action presentation fixture','pay_channel','PAYE','line_type','TIMESHEET_PAYMENT',
      'timesheet_id',v_timesheet,'week_ending_date','2026-08-30','amount_display','123.45',
      'section_amount_display','123.45','amount_ex_vat','123.45','presentation_section','CASES_RESOLUTIONS',
      'readiness_state','CASES_RESOLUTIONS','case_key','finance:'||gen_random_uuid()::text,
      'finance_case_id',gen_random_uuid(),'resolution_family','NON_BUCKET','case_needs_resolution',true,
      'case_resolution_satisfied_now',false
    ),'SOURCE_REF','action-presentation-fixture',false,'NOT_SELECTABLE','READY',1);

  select md5(to_jsonb(r)::text) into strict v_before
  from public.banking_pay_workbench_preview_rows r where r.id=v_row;
  select * into strict v_result from public.pay_workbench_session_get_candidate_summary_page_v1(
    v_session,jsonb_build_object('expected_session_version',1,'expected_progress_counter_version',4,'pay_channel_scope','ALL'),v_actor
  );
  v_options := jsonb_build_object(
    'expected_session_version',1,'expected_progress_counter_version',4,
    'scope_hash',v_result->>'scope_hash','pay_channel_scope','ALL'
  );

  foreach v_sort in array array['CANDIDATES','PAYMENTS','TITLE','AMOUNT'] loop
    foreach v_direction in array array['ASC','DESC'] loop
      v_result := public.pay_workbench_session_get_action_required_page_v1(
        v_session,v_options,v_actor,v_sort,v_direction,null,100,'','ACTION_REQUIRED');
      if v_result->>'sort_key' is distinct from v_sort
         or v_result->>'sort_direction' is distinct from v_direction
         or v_result->>'total_count' is distinct from '1'
         or jsonb_array_length(v_result->'rows')<>1 then
        raise exception 'BANKING_PAY_ACTION_PRESENTATION_VERIFY: % % changed the task scope',v_sort,v_direction;
      end if;
      v_item := v_result#>'{rows,0}';
      if v_item->>'candidate_name' is distinct from 'Action presentation fixture'
         or v_item->>'candidate_reference' is distinct from 'ACTION-VERIFY'
         or v_item->>'payment_label' is distinct from 'Timesheet payment'
         or v_item->>'payment_date' is distinct from '2026-08-30'
         or v_item->>'affected_display_amount' is distinct from '123.45'
         or v_item->>'linked_timesheet_id' is distinct from v_timesheet::text
         or v_item->>'affected_candidate_count' is distinct from '1'
         or v_item->>'affected_payment_count' is distinct from '1' then
        raise exception 'BANKING_PAY_ACTION_PRESENTATION_VERIFY: server presentation facts changed for % %: %',
          v_sort,
          v_direction,
          jsonb_build_object(
            'candidate_name', v_item->>'candidate_name',
            'candidate_reference', v_item->>'candidate_reference',
            'payment_label', v_item->>'payment_label',
            'payment_date', v_item->>'payment_date',
            'affected_display_amount', v_item->>'affected_display_amount',
            'linked_timesheet_id_matches', (v_item->>'linked_timesheet_id' is not distinct from v_timesheet::text),
            'affected_candidate_count', v_item->>'affected_candidate_count',
            'affected_payment_count', v_item->>'affected_payment_count'
          );
      end if;
    end loop;
  end loop;

  begin
    perform public.pay_workbench_session_get_action_required_page_v1(
      v_session,v_options,v_actor,'GROSS','ASC',null,100,'','ACTION_REQUIRED');
    raise exception 'BANKING_PAY_ACTION_PRESENTATION_VERIFY: unapproved sort accepted';
  exception when invalid_parameter_value then
    if sqlerrm is distinct from 'BANKING_PAY_V2_INVALID_INPUT' then raise; end if;
  end;

  select md5(to_jsonb(r)::text) into strict v_after
  from public.banking_pay_workbench_preview_rows r where r.id=v_row;
  if v_after is distinct from v_before then
    raise exception 'BANKING_PAY_ACTION_PRESENTATION_VERIFY: read changed a payment row';
  end if;
end;
$verify$;

rollback;
