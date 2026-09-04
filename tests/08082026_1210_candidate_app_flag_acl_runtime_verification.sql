-- Candidate App dormant-flag and RPC ACL verification.
-- Disposable PostgreSQL only; all fixture writes are rolled back.

begin;

do $flag_off$
declare
  v_payload jsonb:=jsonb_build_object(
    'rows',jsonb_build_array(jsonb_build_object('timesheet_id','11111111-1111-1111-1111-111111111111')),
    'sentinel','unchanged'
  );
  v_result jsonb;
begin
  update public.settings_defaults set candidate_app_feature_flags_json=jsonb_build_object(
    'candidate_account_registration',false,'candidate_app_reads',false,
    'candidate_app_writes',false,'candidate_record_role_capabilities',false,
    'candidate_expense_atomic_placement',false,
    'candidate_expense_invoice_routing_v1',false,'candidate_manager_approval',false,
    'candidate_paper_qr',false,'candidate_notifications',false,
    'candidate_daily_finalisation',false,'candidate_settings',false
  ) where id=1;

  if private._candidate_office_context_overlay_v1(v_payload) is distinct from v_payload
     or private._candidate_dataset_overlay_v1(v_payload) is distinct from v_payload
     or private._candidate_signature_component_v1(null,null) is not null
     or private._candidate_draft_totals_guard_v1(null,'{}'::jsonb)<>'{}'::jsonb
     or private._candidate_weekly_final_state_guard_v1(null,null,null,'{}'::jsonb,null)<>'{}'::jsonb then
    raise exception 'Candidate App flag-off helper parity failed';
  end if;

  begin
    perform public.candidate_auth_challenge_transition_v1(
      'START','TEST','flag-off@example.test','ACTIVATE',null,
      decode(repeat('11',32),'hex'),'flag-off:challenge',now());
    raise exception 'Candidate auth unexpectedly ran while registration flag was false';
  exception when sqlstate '42501' then
    if sqlerrm<>'CANDIDATE_FEATURE_DISABLED' then raise; end if;
  end;

  begin
    perform public.candidate_app_timesheet_page_v1(
      '11111111-1111-1111-1111-111111111111','TEST','CURRENT',null,50,now());
    raise exception 'Candidate read unexpectedly ran while read flag was false';
  exception when sqlstate '42501' then
    if sqlerrm<>'CANDIDATE_FEATURE_DISABLED' then raise; end if;
  end;
end;
$flag_off$;

do $acl$
declare
  v_signature text;
  v_candidate_rpc text;
begin
  foreach v_candidate_rpc in array array[
    'candidate_auth_account_transition_v1(text,text,uuid,text,uuid,uuid,jsonb,text,timestamp with time zone)',
    'candidate_auth_challenge_transition_v1(text,text,text,text,uuid,bytea,text,timestamp with time zone,integer)',
    'candidate_app_bootstrap_v1(uuid,text,integer,timestamp with time zone)',
    'candidate_app_timesheet_page_v1(uuid,text,text,text,integer,timestamp with time zone)',
    'candidate_app_timesheet_detail_v1(uuid,text,uuid,uuid,uuid,timestamp with time zone)',
    'candidate_missing_week_options_v1(uuid,text,uuid,date,date,timestamp with time zone)',
    'candidate_contract_week_add_missing_atomic_v1(uuid,text,uuid,date,text,timestamp with time zone)',
    'expense_placement_resolve_v1(uuid,text,uuid,uuid,jsonb,timestamp with time zone)',
    'expense_carrier_resolve_or_create_atomic_v1(uuid,text,uuid,text,text,timestamp with time zone)',
    'timesheet_expense_apply_atomic_v1(uuid,text,uuid,uuid,integer,text,jsonb,uuid[],text,timestamp with time zone)',
    'candidate_workflow_transition_atomic_v1(uuid,text,uuid,text,integer,jsonb,text,timestamp with time zone)',
    'candidate_submission_finalize_atomic_v1(uuid,text,uuid,integer,text,text,timestamp with time zone,jsonb)',
    'candidate_submission_reject_atomic_v1(uuid,text,uuid,uuid,text,text,text,timestamp with time zone)',
    'candidate_no_work_atomic_v1(uuid,text,uuid,text,text,timestamp with time zone)'
  ] loop
    if has_function_privilege('anon',v_candidate_rpc,'EXECUTE')
       or has_function_privilege('authenticated',v_candidate_rpc,'EXECUTE')
       or not has_function_privilege('service_role',v_candidate_rpc,'EXECUTE') then
      raise exception 'Candidate RPC ACL mismatch: %',v_candidate_rpc;
    end if;
  end loop;

  if has_function_privilege('authenticated',
       'contract_week_manual_draft_upsert_atomic_v1(uuid,text,jsonb,jsonb,boolean,boolean,timestamp with time zone)',
       'EXECUTE')
     or has_function_privilege('anon',
       'contract_week_manual_draft_upsert_atomic_v1(uuid,text,jsonb,jsonb,boolean,boolean,timestamp with time zone)',
       'EXECUTE')
     or not has_function_privilege('service_role',
       'contract_week_manual_draft_upsert_atomic_v1(uuid,text,jsonb,jsonb,boolean,boolean,timestamp with time zone)',
       'EXECUTE') then
    raise exception 'draft upsert ACL no longer matches installed authority';
  end if;

  foreach v_signature in array array[
    'bulk_process_dataset_v1(jsonb)',
    'bulk_authorise_dataset_v1(jsonb)',
    'bulk_timesheet_row_patch_v1(jsonb)',
    'bulk_process_row_context_v1(jsonb)',
    'bulk_authorise_row_context_v1(jsonb)',
    'timesheet_qr_send_enqueue_v1(uuid,uuid,uuid,text,timestamp with time zone)',
    'timesheet_lifecycle_guard_signature_v1(uuid,uuid,boolean)'
  ] loop
    if not has_function_privilege('authenticated',v_signature,'EXECUTE')
       or not has_function_privilege('service_role',v_signature,'EXECUTE') then
      raise exception 'existing authenticated/service ACL mismatch: %',v_signature;
    end if;
  end loop;

  if has_function_privilege('authenticated',
       'timesheet_qr_refuse_and_reset(uuid,uuid,text,uuid)','EXECUTE')
     or has_function_privilege('anon',
       'timesheet_qr_refuse_and_reset(uuid,uuid,text,uuid)','EXECUTE')
     or not has_function_privilege('service_role',
       'timesheet_qr_refuse_and_reset(uuid,uuid,text,uuid)','EXECUTE') then
    raise exception 'QR refusal/reset authority is not service-only';
  end if;

  if has_function_privilege('anon',
       'contract_week_manual_upsert_atomic(uuid,uuid,jsonb,jsonb,jsonb,jsonb,jsonb,uuid,boolean,timestamp with time zone,text,jsonb)',
       'EXECUTE')
     or has_function_privilege('authenticated',
       'contract_week_manual_upsert_atomic(uuid,uuid,jsonb,jsonb,jsonb,jsonb,jsonb,uuid,boolean,timestamp with time zone,text,jsonb)',
       'EXECUTE')
     or not has_function_privilege('service_role',
       'contract_week_manual_upsert_atomic(uuid,uuid,jsonb,jsonb,jsonb,jsonb,jsonb,uuid,boolean,timestamp with time zone,text,jsonb)',
       'EXECUTE') then
    raise exception 'weekly finalisation owner ACL is not service-only';
  end if;

  if not has_function_privilege('anon',
       'timesheet_daily_manual_process_atomic(uuid,uuid,uuid,jsonb,jsonb,timestamp with time zone,text)',
       'EXECUTE')
     or not has_function_privilege('authenticated',
       'timesheet_daily_manual_process_atomic(uuid,uuid,uuid,jsonb,jsonb,timestamp with time zone,text)',
       'EXECUTE')
     or not has_function_privilege('service_role',
       'timesheet_daily_manual_process_atomic(uuid,uuid,uuid,jsonb,jsonb,timestamp with time zone,text)',
       'EXECUTE') then
    raise exception 'DAILY process PUBLIC/role ACL mismatch';
  end if;

  foreach v_signature in array array[
    'client_create_with_settings_v1(uuid,jsonb,uuid,jsonb,timestamp with time zone)',
    'client_update_with_settings_v1(uuid,bigint,timestamp with time zone,jsonb,jsonb,uuid,text)',
    'private._invoice_generation_resolve_command_groups(jsonb,uuid,timestamp with time zone)',
    'private._invoice_delivery_routes_batch(jsonb,date)',
    'private._invoice_issue_validate_batch(jsonb,date)',
    'private._invoice_generation_advance_core_v8(jsonb,timestamp with time zone)'
  ] loop
    if has_function_privilege('anon',v_signature,'EXECUTE')
       or has_function_privilege('authenticated',v_signature,'EXECUTE')
       or not has_function_privilege('service_role',v_signature,'EXECUTE') then
      raise exception 'existing service-only ACL mismatch: %',v_signature;
    end if;
  end loop;
end;
$acl$;

rollback;
