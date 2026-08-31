-- Mandatory NEW/UPGRADE proof that a zero recoverable amount never hides the
-- existing unresolved amount requiring an Action Required decision.
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
  v_finance_case uuid := gen_random_uuid();
  v_candidate_reference text;
  v_options jsonb;
  v_result jsonb;
  v_item jsonb;
  v_before text;
  v_after text;
  v_sort text;
  v_direction text;
  v_eligible_count integer;
  v_member_count integer;
  v_session_row public.banking_pay_workbench_sessions%rowtype;
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
    raise exception 'BANKING_PAY_ACTION_AMOUNT_VERIFY: function security boundary changed';
  end if;
  if lower(v_definition) like '%pg_catalog.coalesce(%'
     or lower(v_definition) like '%pg_catalog.nullif(%'
     or lower(v_definition) like '%pg_catalog.least(%'
     or lower(v_definition) like '%pg_catalog.greatest(%'
     or position('offset ' in lower(v_definition))>0 then
    raise exception 'BANKING_PAY_ACTION_AMOUNT_VERIFY: unsafe function source';
  end if;

  insert into public.tms_users(id,email,password_hash,role,is_active)
  values(v_actor,'banking-action-amount-'||v_actor::text||'@example.invalid','UNUSABLE_VERIFICATION_ONLY','admin',true);
  insert into public.candidates(id,display_name,tms_ref,pay_method)
  values(v_candidate,'Action amount fixture','ACTION-VERIFY','PAYE');
  select c.tms_ref into strict v_candidate_reference
  from public.candidates c where c.id=v_candidate;
  if nullif(btrim(v_candidate_reference),'') is null then
    raise exception 'BANKING_PAY_ACTION_AMOUNT_VERIFY: canonical candidate reference is missing';
  end if;
  -- The Workbench row carries the canonical Timesheet foreign key.  Keep the
  -- rollback fixture structurally real so the verifier exercises the same
  -- presentation path as an ordinary Timesheet-backed payment.
  insert into public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,
    job_title_norm,week_ending_date,status,is_current,version
  ) values(
    v_timesheet,'ACTION-AMOUNT-'||v_timesheet::text,
    lower(replace(v_candidate::text,'-','')),'VERIFY','VERIFY','VERIFY',
    '2026-08-30','RECEIVED',true,1
  );
  insert into public.banking_pay_snapshot_runs(id,pay_date,week_ending_cutoff,pay_week_start,eligibility_from_date,eligibility_to_date)
  values(v_snapshot,'2026-08-28','2026-08-30','2026-08-24','2026-08-01','2026-08-31');
  insert into public.banking_pay_workbench_sessions(
    id,actor_user_id,pay_date,week_ending_cutoff,session_signature,source_snapshot_run_id,
    version,progress_counter_version,progress_json,scope_seed_complete
  ) values(v_session,v_actor,'2026-08-28','2026-08-30','synthetic action amount '||v_session::text,
    v_snapshot,1,4,'{"ready":true}',true);
  insert into public.banking_pay_workbench_session_scope(
    session_id,candidate_id,scope_ordinal,status,seeded,dirty,certified_preview_publication_attestation_json
  ) values(v_session,v_candidate,1,'READY',true,false,
    '{"attestation_version":"CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3","contract_version":"3","semantic_contract_version":"READY_TO_PAY_SEMANTIC_V2"}');
  insert into public.pay_advances(
    id,candidate_id,reason,original_amount,outstanding_amount,schedule_json,status,advance_kind,case_type
  ) values(
    v_finance_case,v_candidate,'OVERPAYMENT',25,25,'[]','ACTIVE','OVERPAYMENT','OVERPAYMENT'
  );
  insert into public.banking_pay_workbench_preview_rows(
    id,session_id,candidate_id,timesheet_id,section,row_key,row_ordinal,row_json,key_type,key_value,
    selected,selection_state,status,session_version
  ) values(v_row,v_session,v_candidate,v_timesheet,'cases_resolutions','action-amount-fixture',1,
    jsonb_build_object(
      'candidate_name','Action amount fixture','pay_channel','UMBRELLA','line_type','OVERPAYMENT_RECOVERY',
      'timesheet_id',v_timesheet,'week_ending_date','2026-08-30','amount_display','0.00',
      'section_amount_display','0.00','amount_ex_vat','0.00','nominal_due_amount_ex_vat',25,
      'recoverable_this_pay_run_ex_vat','0.00',
      'presentation_section','CASES_RESOLUTIONS','readiness_state','CASES_RESOLUTIONS',
      'case_key','finance:'||v_finance_case::text,'finance_case_id',v_finance_case,
      'recovery_residual_contract_version',1,'recovery_source_outstanding_ex_vat',25,
      'recovery_active_reserved_ex_vat',0,'recovery_residual_outstanding_ex_vat',25,
      'resolution_family','TAXABLE_CHANNEL_RESTRUCTURE','case_needs_resolution',true,
      'case_resolution_satisfied_now',false
    ),'SOURCE_REF','action-amount-fixture',false,'NOT_SELECTABLE','READY',1);

  select md5(to_jsonb(r)::text) into strict v_before
  from public.banking_pay_workbench_preview_rows r where r.id=v_row;
  select * into strict v_result from public.pay_workbench_session_get_candidate_summary_page_v1(
    v_session,jsonb_build_object('expected_session_version',1,'expected_progress_counter_version',4,'pay_channel_scope','ALL'),v_actor
  );
  v_options := jsonb_build_object(
    'expected_session_version',1,'expected_progress_counter_version',4,
    'scope_hash',v_result->>'scope_hash','pay_channel_scope','ALL'
  );
  select * into strict v_session_row
  from public.banking_pay_workbench_sessions where id=v_session;
  select count(*) into strict v_eligible_count
  from private.pay_workbench_modal_eligible_rows_v2(v_session,1,'cases_resolutions');
  select count(*) into strict v_member_count
  from private.pay_workbench_modal_finance_task_members_v2(v_session_row,'ALL');
  if v_eligible_count<>1 or v_member_count<>1 then
    raise exception 'BANKING_PAY_ACTION_AMOUNT_VERIFY: fixture did not reach action authority eligible=% members=%',
      v_eligible_count,v_member_count;
  end if;

  foreach v_sort in array array['CANDIDATES','PAYMENTS','TITLE','AMOUNT'] loop
    foreach v_direction in array array['ASC','DESC'] loop
      v_result := public.pay_workbench_session_get_action_required_page_v1(
        v_session,v_options,v_actor,v_sort,v_direction,null,100,'','ACTION_REQUIRED');
      if v_result->>'sort_key' is distinct from v_sort
         or v_result->>'sort_direction' is distinct from v_direction
         or v_result->>'total_count' is distinct from '1'
         or jsonb_array_length(v_result->'rows')<>1 then
        raise exception 'BANKING_PAY_ACTION_AMOUNT_VERIFY: % % changed the task scope: %',v_sort,v_direction,v_result;
      end if;
      v_item := v_result#>'{rows,0}';
      if v_item->>'candidate_name' is distinct from 'Action amount fixture'
         or v_item->>'candidate_reference' is distinct from v_candidate_reference
         or v_item->>'payment_label' is distinct from 'Timesheet payment'
         or v_item->>'payment_date' is distinct from '2026-08-30'
         or v_item->>'affected_display_amount' is distinct from '25.00'
         or v_item->>'linked_timesheet_id' is distinct from v_timesheet::text
         or v_item->>'affected_candidate_count' is distinct from '1'
         or v_item->>'affected_payment_count' is distinct from '1' then
        raise exception 'BANKING_PAY_ACTION_AMOUNT_VERIFY: server presentation facts changed for % %: %',
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
    raise exception 'BANKING_PAY_ACTION_AMOUNT_VERIFY: unapproved sort accepted';
  exception when invalid_parameter_value then
    if sqlerrm is distinct from 'BANKING_PAY_V2_INVALID_INPUT' then raise; end if;
  end;

  select md5(to_jsonb(r)::text) into strict v_after
  from public.banking_pay_workbench_preview_rows r where r.id=v_row;
  if v_after is distinct from v_before then
    raise exception 'BANKING_PAY_ACTION_AMOUNT_VERIFY: read changed a payment row';
  end if;
end;
$verify$;

rollback;
