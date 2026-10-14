create or replace function public.timesheet_delete_with_pending_expense_apply_v1(
  p_environment text,
  p_delete_kind text,
  p_timesheet_id uuid,
  p_actor_user_id uuid,
  p_expected_timesheet_id uuid,
  p_expected_row_signature text,
  p_expected_timesheet_ids uuid[],
  p_expected_contract_week_ids uuid[],
  p_expected_nhsp_shift_ids uuid[],
  p_expected_preserved_source_timesheet_ids uuid[],
  p_expected_preserved_source_contract_week_ids uuid[],
  p_expected_pending_expense_context_sha256 text,
  p_delete_operation_id uuid,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_environment text:=private._candidate_assert_environment(p_environment);
  v_kind text:=upper(btrim(coalesce(p_delete_kind,'')));
  v_context jsonb;
  v_claim jsonb;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_cancel_result jsonb;
  v_cancel_results jsonb:='[]'::jsonb;
  v_notification_id uuid;
  v_notification_ids uuid[]:=array[]::uuid[];
  v_settings jsonb;
  v_template jsonb;
  v_terminal_mail jsonb;
  v_apply_result jsonb;
  v_delete_candidate_ids uuid[]:=array[]::uuid[];
  v_delete_candidate_id uuid;
  v_candidate_lock_key bigint;
  v_banking_dirty_result jsonb:='{}'::jsonb;
begin
  if p_actor_user_id is null or p_timesheet_id is null or p_expected_timesheet_id is null
     or p_delete_operation_id is null or nullif(btrim(coalesce(p_expected_row_signature,'')),'') is null
     or coalesce(p_expected_pending_expense_context_sha256,'') !~ '^[0-9a-f]{64}$'
     or v_kind not in (
       'STANDARD_DELETE','DAILY_ABANDONED_RECEIPT_DELETE',
       'WEEKLY_CHAIN_DELETE_PARENT','WEEKLY_MANUAL_ADJUSTMENT_DELETE'
     ) then
    raise exception 'PENDING_EXPENSE_DELETE_APPLY_INVALID' using errcode='22023';
  end if;

  perform 1
  from public.timesheets delete_timesheet
  where delete_timesheet.timesheet_id=any(p_expected_timesheet_ids)
  order by delete_timesheet.timesheet_id
  for update;

  v_context:=private._timesheet_pending_expense_delete_context_v1(
    v_environment,p_expected_timesheet_ids,true
  );
  if lower(v_context->>'context_sha256') is distinct from
       lower(p_expected_pending_expense_context_sha256) then
    raise exception 'PENDING_EXPENSE_DELETE_CONTEXT_CHANGED'
      using errcode='40001',detail=v_context::text;
  end if;
  if coalesce((v_context->>'delete_blocked')::boolean,false) then
    raise exception 'PENDING_EXPENSE_DELETE_BLOCKED'
      using errcode='55000',detail=v_context::text;
  end if;

  -- Every supported removal unit belongs to one Candidate. Capture and lock
  -- that owner while the Timesheets still exist so deletion cannot leave a
  -- stale Banking Pay view or try to validate a Timesheet after it is gone.
  select coalesce(array_agg(distinct owner.candidate_id order by owner.candidate_id),array[]::uuid[])
  into v_delete_candidate_ids
  from (
    select contract_row.candidate_id
    from public.timesheets timesheet_row
    join public.contracts contract_row on contract_row.id=timesheet_row.contract_id
    where timesheet_row.timesheet_id=any(p_expected_timesheet_ids)
    union all
    select financial_row.candidate_id
    from public.timesheets_financials financial_row
    where financial_row.timesheet_id=any(p_expected_timesheet_ids)
      and financial_row.is_current
    union all
    select request.candidate_id from private.timesheet_correction_requests request
    where request.replacement_timesheet_id=any(p_expected_timesheet_ids)
      and private.timesheet_correction_cancel_capability_v1(request.replacement_timesheet_id,p_actor_user_id) is not null
  ) owner
  where owner.candidate_id is not null;
  if cardinality(v_delete_candidate_ids)<>1 then
    raise exception 'TIMESHEET_DELETE_CANDIDATE_OWNERSHIP_INVALID'
      using errcode='23514',detail=jsonb_build_object(
        'candidate_count',cardinality(v_delete_candidate_ids)
      )::text;
  end if;
  v_delete_candidate_id:=v_delete_candidate_ids[1];
  v_candidate_lock_key:=pg_catalog.hashtextextended(
    public._pay_workbench_candidate_serial_key(v_delete_candidate_id),24062027
  );
  perform pg_catalog.pg_advisory_xact_lock(v_candidate_lock_key);

  if coalesce((v_context->>'cancellation_required')::boolean,false) then
    v_settings:=public.candidate_manager_email_settings_get_v1();
    v_template:=v_settings#>'{templates,EXPENSE_CLAIM,CANCELLATION}';
    v_terminal_mail:=jsonb_build_object(
      'subject',v_template->>'subject',
      'body_text',v_template->>'body_text',
      'body_html',v_template->>'body_html',
      'manager_template_version',v_settings->'version',
      'manager_template_sha256',v_settings->>'semantic_sha256_hex',
      'manager_submission_type','EXPENSE_CLAIM'
    );
  end if;

  for v_claim in
    select value from jsonb_array_elements(coalesce(
      v_context->'pending_expense_claims','[]'::jsonb
    )) items(value)
  loop
    select workflow_row.* into strict v_workflow
    from public.candidate_submission_workflows workflow_row
    where workflow_row.id=(v_claim->>'workflow_id')::uuid
      and workflow_row.generation=(v_claim->>'generation')::integer
      and workflow_row.environment=v_environment
      and workflow_row.workflow_kind='CONTRACT_EXPENSE'
      and workflow_row.target_timesheet_id is null
    for update;

    perform 1 from public.candidate_notifications notification
    where notification.workflow_id=v_workflow.id
       or notification.timesheet_id=any(p_expected_timesheet_ids)
    order by notification.id
    for update;

    perform private._candidate_office_service_context_open_v1(
      v_environment,p_actor_user_id,'delete_timesheet','CANCEL',p_now_utc
    );
    v_cancel_result:=public.candidate_workflow_cancel_atomic_v2(
      null,v_environment,v_workflow.id,v_workflow.generation,
      jsonb_build_object(
        'service_office_action',true,
        'actor_user_id',p_actor_user_id,
        'reason_note','Pending expense claim cancelled because its Timesheet was deleted.',
        'reason_code','TIMESHEET_DELETED',
        'manager_terminal_mail',v_terminal_mail
      ),
      'office-timesheet-delete:'||p_delete_operation_id::text||':'||v_workflow.id::text,
      p_now_utc
    );
    perform private._candidate_office_service_context_close_v1();

    update public.candidate_submission_components component
    set timesheet_id=null
    where component.workflow_id=v_workflow.id
      and component.timesheet_id=any(p_expected_timesheet_ids);

    update public.candidate_notifications notification
    set timesheet_id=null,
        deep_link_json=(coalesce(notification.deep_link_json,'{}'::jsonb)
          -'timesheet_id'-'contract_week_id')||jsonb_build_object(
            'type','workflow','workflow_id',v_workflow.id
          ),
        state='DISMISSED',
        dismissed_at_utc=coalesce(notification.dismissed_at_utc,p_now_utc),
        push_state=case
          when notification.push_state in ('PENDING','FAILED','CLAIMED') then 'SKIPPED'
          else notification.push_state
        end
    where notification.workflow_id=v_workflow.id;

    update public.candidate_submission_workflows workflow_row
    set contract_week_id=null,
        anchor_timesheet_id=null,
        target_timesheet_id=null,
        input_snapshot_json=coalesce(workflow_row.input_snapshot_json,'{}'::jsonb)
          ||jsonb_build_object(
            'office_permanent_delete_tombstone',jsonb_build_object(
              'delete_operation_id',p_delete_operation_id,
              'deleted_timesheet_ids',to_jsonb(p_expected_timesheet_ids),
              'deleted_contract_week_ids',to_jsonb(p_expected_contract_week_ids),
              'previous_contract_week_id',v_workflow.contract_week_id,
              'previous_anchor_timesheet_id',v_workflow.anchor_timesheet_id,
              'previous_target_timesheet_id',v_workflow.target_timesheet_id,
              'retired_at_utc',p_now_utc
            )
          ),
        issue_codes=(case
          when workflow_row.issue_codes @> '["OFFICE_TIMESHEET_DELETED_EXPENSE_CANCELLED"]'::jsonb
            then workflow_row.issue_codes
          else workflow_row.issue_codes||'["OFFICE_TIMESHEET_DELETED_EXPENSE_CANCELLED"]'::jsonb
        end)||(case
          when workflow_row.issue_codes @> '["OFFICE_PERMANENTLY_DELETED_TIMESHEET"]'::jsonb
            then '[]'::jsonb
          else '["OFFICE_PERMANENTLY_DELETED_TIMESHEET"]'::jsonb
        end),
        updated_at_utc=p_now_utc
    where workflow_row.id=v_workflow.id
      and workflow_row.state='CANCELLED'
      and workflow_row.target_timesheet_id is null;
    if not found then
      raise exception 'PENDING_EXPENSE_CANCELLATION_NOT_PROVEN' using errcode='40001';
    end if;

    insert into public.candidate_notifications(
      account_id,candidate_id,workflow_id,timesheet_id,event_type,preference_category,
      template_key,template_params,deep_link_json,state,push_state,dedupe_key,created_at_utc
    )
    select
      v_workflow.account_id,v_workflow.candidate_id,v_workflow.id,null,
      'EXPENSE_CLAIM_CANCELLED','timesheet_expense_attention',
      'candidate-expense-claim-cancelled-timesheet-delete-v1',
      jsonb_build_object(
        'workflow_id',v_workflow.id,
        'reason_code','TIMESHEET_DELETED'
      ),
      jsonb_build_object('type','workflow','workflow_id',v_workflow.id),
      'UNREAD',
      case
        when account.status='ACTIVE'
          and coalesce((account.notification_preferences_json->>'push')::boolean,true)
          and coalesce(
            (account.notification_preferences_json->>'timesheet_expense_attention')::boolean,
            (account.notification_preferences_json->>'office_rejection')::boolean,
            true
          )
          then 'PENDING'
        else 'SKIPPED'
      end,
      'CANDIDATE_EXPENSE_CLAIM_CANCELLED_TIMESHEET_DELETE_V1:'
        ||v_workflow.id::text||':'||v_workflow.generation::text,
      p_now_utc
    from public.candidate_app_accounts account
    where account.id=v_workflow.account_id
    on conflict(dedupe_key) do update set dedupe_key=excluded.dedupe_key
    returning id into v_notification_id;

    v_notification_ids:=array_append(v_notification_ids,v_notification_id);
    v_cancel_results:=v_cancel_results||jsonb_build_array(jsonb_build_object(
      'workflow_id',v_workflow.id,
      'previous_generation',v_workflow.generation,
      'state','CANCELLED',
      'notification_id',v_notification_id,
      'cancellation',v_cancel_result
    ));
  end loop;

  -- The established Timesheet delete removes financial rows before the
  -- Timesheet. Suppress only the duplicate row-level delete signals for this
  -- one locked Candidate, then issue one complete Candidate refresh after the
  -- authoritative delete has been proved. This uses the existing sealed
  -- Banking Pay suppression boundary and changes no financial truth.
  if pg_catalog.to_regclass('pg_temp._bpay_candidate_delete_context_v1') is not null then
    raise exception 'PAY_WORKBENCH_CANDIDATE_DELETE_CONTEXT_CONFLICT' using errcode='23514';
  end if;
  create temp table pg_temp._bpay_candidate_delete_context_v1(
    candidate_id uuid primary key,
    delete_operation_id uuid unique not null,
    candidate_lock_key bigint not null,
    backend_pid integer not null,
    transaction_id bigint not null,
    created_at_utc timestamptz not null,
    suppress boolean not null check(suppress)
  ) on commit drop;
  insert into pg_temp._bpay_candidate_delete_context_v1 values(
    v_delete_candidate_id,p_delete_operation_id,v_candidate_lock_key,
    pg_catalog.pg_backend_pid(),pg_catalog.txid_current(),p_now_utc,true
  );

  if v_kind='STANDARD_DELETE' then
    v_apply_result:=public.timesheet_standard_delete_apply_v1(
      p_timesheet_id,p_actor_user_id,p_expected_timesheet_id,p_expected_row_signature
    );
  elsif v_kind='DAILY_ABANDONED_RECEIPT_DELETE' then
    v_apply_result:=public.timesheet_daily_abandoned_receipt_delete_apply_v1(
      p_timesheet_id,p_actor_user_id,p_expected_timesheet_id,p_expected_row_signature
    );
  elsif v_kind='WEEKLY_CHAIN_DELETE_PARENT' then
    v_apply_result:=public.timesheet_weekly_chain_delete_apply(
      p_timesheet_id,p_actor_user_id,p_expected_timesheet_ids,
      p_expected_contract_week_ids,p_expected_nhsp_shift_ids,p_expected_row_signature
    );
  else
    v_apply_result:=public.timesheet_weekly_manual_adjustment_delete_apply(
      p_timesheet_id,p_actor_user_id,p_expected_timesheet_ids,
      p_expected_contract_week_ids,p_expected_preserved_source_timesheet_ids,
      p_expected_preserved_source_contract_week_ids,p_expected_row_signature
    );
  end if;

  if coalesce((v_apply_result->>'apply_performed')::boolean,false) is not true
     or upper(coalesce(v_apply_result->>'decision',''))<>'PERMANENT_DELETE' then
    raise exception 'TIMESHEET_DELETE_AFTER_EXPENSE_CANCELLATION_NOT_PROVEN'
      using errcode='40001',detail=coalesce(v_apply_result,'{}'::jsonb)::text;
  end if;

  drop table pg_temp._bpay_candidate_delete_context_v1;
  v_banking_dirty_result:=public.pay_workbench_dirty_event_enqueue(
    p_job_type=>'WORKBENCH_CANDIDATE_DIRTY_APPLY',
    p_scope_kind=>'CANDIDATE',
    p_scope_id=>v_delete_candidate_id::text,
    p_candidate_id=>v_delete_candidate_id,
    p_targeted_timesheet_ids=>array[]::uuid[],
    p_linked_timesheet_ids=>array[]::uuid[],
    p_payload_json=>jsonb_build_object(
      'trigger_table','timesheets',
      'trigger_op','DELETE',
      'trigger_operation','DELETE',
      'delete_operation_id',p_delete_operation_id,
      'deleted_timesheet_ids',coalesce(v_apply_result->'deleted_timesheet_ids','[]'::jsonb),
      'scope_resolution','CANDIDATE_FULL_AFTER_TIMESHEET_DELETE',
      'policy_x_authority_scope','PRE_DRAFT_LIVE_TRUTH',
      'economic_truth_mutation_allowed',false
    ),
    p_reason=>'DIRTY_TRIGGER:TIMESHEETS:DELETE',
    p_priority=>-1000,
    p_run_at_utc=>p_now_utc
  );
  if coalesce((v_banking_dirty_result->>'ok')::boolean,false) is not true then
    raise exception 'TIMESHEET_DELETE_BANKING_REFRESH_NOT_PROVEN'
      using errcode='40001',detail=coalesce(v_banking_dirty_result,'{}'::jsonb)::text;
  end if;

  return v_apply_result||jsonb_build_object(
    'pending_expense_context_sha256',v_context->>'context_sha256',
    'cancelled_pending_expense_claim_count',jsonb_array_length(v_cancel_results),
    'cancelled_pending_expense_claims',v_cancel_results,
    'candidate_notification_ids',to_jsonb(v_notification_ids),
    'banking_pay_candidate_refresh',v_banking_dirty_result
  );
exception when others then
  perform private._candidate_office_service_context_close_v1();
  raise;
end;
$function$;
