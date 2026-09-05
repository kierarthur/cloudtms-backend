-- Repeatable CloudTMS function authority: candidate_protected_additional_expense_action_v1
-- A finalised Candidate hours/combined claim may start a later expense-only
-- claim only after the current hours carrier is Office-protected.  Until then,
-- cancellation/resubmission is the sole correction route.

\set ON_ERROR_STOP on

begin;

create or replace function private._candidate_timesheet_primary_action_v1(
  p_candidate_status_code text,
  p_workflows jsonb,
  p_capabilities jsonb,
  p_timesheet_id uuid,
  p_contract_week_id uuid
)
returns jsonb
language plpgsql
stable
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_workflow jsonb;
  v_action text;
  v_draft_has_content boolean:=false;
  v_has_finalised_hours boolean:=false;
  v_hours_office_protected boolean:=false;
begin
  if upper(coalesce(p_candidate_status_code,'')) in ('PAID','AUTHORISED','INVOICED_NOT_PAID')
     and not coalesce((p_capabilities->>'can_edit_expenses')::boolean,false) then
    return null;
  end if;
  select item into v_workflow
  from jsonb_array_elements(coalesce(p_workflows,'[]'::jsonb)) item
  where item->>'state'='REJECTED'
    and coalesce((item->>'rejection_actionable')::boolean,false)
    and nullif(item->>'required_resubmission_action','') is not null
  order by coalesce((item->>'detail_action_owner')::boolean,false) desc,
    item->>'updated_at_utc' desc,item->>'workflow_id'
  limit 1;
  if v_workflow is not null then
    v_action:=v_workflow->>'required_resubmission_action';
    return jsonb_build_object(
      'code',v_action,
      'label',case v_action
        when 'RESUBMIT_EXPENSE_CLAIM' then 'Resubmit Expense Claim'
        when 'RESUBMIT_TIMESHEET_AND_EXPENSES' then 'Resubmit Timesheet and Expenses'
        else 'Resubmit Timesheet' end,
      'method','POST',
      'path','/candidate-app/v1/workflows/'||(v_workflow->>'workflow_id')||'/resubmit',
      'requires_confirmation',false,'requires_reason',false,
      'enabled',true,'disabled_reason',null,
      'workflow_id',v_workflow->>'workflow_id',
      'workflow_generation',nullif(v_workflow->>'generation','')::integer
    );
  end if;
  select item into v_workflow
  from jsonb_array_elements(coalesce(p_workflows,'[]'::jsonb)) item
  where item->>'state' in (
    'CREATED','WORKER_DRAFT','WORKER_SUBMITTED',
    'WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT','READY_FOR_MANAGER_APPROVAL',
    'AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED',
    'MANAGER_APPROVED_PENDING_FINAL_DOCUMENT','READY_TO_FINALISE',
    'AWAITING_PAPER_RETURN','RECEIVED','REFUSED','FINALISED'
  )
    and (
      item->>'state'<>'FINALISED'
      or (
        item->>'workflow_kind'='CONTRACT_EXPENSE'
        and (
          nullif(item->>'target_timesheet_id','') is null
          or not exists(
            select 1
            from public.timesheets_financials expense_financial
            where expense_financial.timesheet_id=nullif(item->>'target_timesheet_id','')::uuid
              and expense_financial.is_current=true
              and expense_financial.authorised_at_utc is not null
          )
        )
      )
    )
    and (
      item->>'state'<>'REFUSED'
      or not private._candidate_rejection_replaced_v1(
        nullif(item->>'workflow_id','')::uuid
      )
    )
  order by coalesce((item->>'detail_action_owner')::boolean,false) desc,
    item->>'updated_at_utc' desc,item->>'workflow_id'
  limit 1;
  if v_workflow is not null then
    if v_workflow->>'workflow_kind'='CONTRACT_EXPENSE'
       and v_workflow->>'state' in ('CREATED','WORKER_DRAFT') then
      select exists(
        select 1
        from public.candidate_submission_components component
        where component.workflow_id=nullif(v_workflow->>'workflow_id','')::uuid
          and component.workflow_generation=nullif(v_workflow->>'generation','')::integer
          and component.superseded_at_utc is null
          and component.component_kind in (
            'HOURS_TIMESHEET','CANDIDATE_SIGNATURE','MILEAGE_FORM','EXPENSE_EVIDENCE'
          )
      ) into v_draft_has_content;
    end if;
    if v_workflow->>'workflow_kind'='CONTRACT_EXPENSE'
       and v_workflow->>'state' in ('CREATED','WORKER_DRAFT')
       and not v_draft_has_content
       and coalesce((p_capabilities->>'can_edit_hours')::boolean,false) then
      v_workflow:=null;
    end if;
  end if;
  if v_workflow is not null then
    v_action:=case
      when v_workflow->>'state'='REFUSED' then 'REVIEW_AND_RESUBMIT'
      when v_workflow->>'workflow_kind'='CONTRACT_EXPENSE'
        and v_workflow->>'state' in ('CREATED','WORKER_DRAFT')
        and not v_draft_has_content then 'ADD_EXPENSES'
      when v_workflow->>'workflow_kind'='CONTRACT_EXPENSE' then 'CONTINUE_EXPENSE_CLAIM'
      else 'CONTINUE_TIMESHEET' end;
    return jsonb_build_object(
      'code',v_action,
      'label',case v_action
        when 'REVIEW_AND_RESUBMIT' then 'Review and Resubmit'
        when 'ADD_EXPENSES' then 'Add Expenses'
        when 'CONTINUE_EXPENSE_CLAIM' then 'Continue Expense Claim'
        else 'Continue Timesheet' end,
      'method',case when v_action='REVIEW_AND_RESUBMIT' then 'POST' else 'GET' end,
      'path',case when v_action='REVIEW_AND_RESUBMIT'
        then '/candidate-app/v1/workflows/'||(v_workflow->>'workflow_id')||'/actions/amend'
        else '/candidate-app/v1/workflows/'||(v_workflow->>'workflow_id')||'/timesheet-detail' end,
      'requires_confirmation',false,'enabled',true,'disabled_reason',null,
      'workflow_id',v_workflow->>'workflow_id',
      'workflow_generation',nullif(v_workflow->>'generation','')::integer
    );
  end if;

  select exists(
    select 1
    from jsonb_array_elements(coalesce(p_workflows,'[]'::jsonb)) item
    where item->>'state'='FINALISED'
      and item->>'workflow_kind' in ('CONTRACT_HOURS','CONTRACT_COMBINED','DAILY')
  ) into v_has_finalised_hours;
  if v_has_finalised_hours then
    v_hours_office_protected:=
      upper(coalesce(p_candidate_status_code,'')) in ('PAID','AUTHORISED','INVOICED_NOT_PAID')
      or exists(
        select 1
        from public.timesheets_financials hours_financial
        where hours_financial.timesheet_id=p_timesheet_id
          and hours_financial.is_current=true
          and (
            hours_financial.authorised_at_utc is not null
            or hours_financial.locked_by_invoice_id is not null
            or hours_financial.paid_at_utc is not null
          )
      );
    if v_hours_office_protected
       and coalesce((p_capabilities->>'can_edit_expenses')::boolean,false) then
      return jsonb_build_object(
        'code','ADD_EXPENSES','label','Add Expenses',
        'method','POST','path','/candidate-app/v1/workflows',
        'requires_confirmation',false,'requires_reason',false,
        'enabled',true,'disabled_reason',null,'timesheet_id',p_timesheet_id,
        'contract_week_id',p_contract_week_id
      );
    end if;
    return null;
  end if;

  if coalesce((p_capabilities->>'can_edit_hours')::boolean,false) then
    return jsonb_build_object(
      'code','ENTER_TIMESHEET','label','Enter Timesheet',
      'method','POST','path','/candidate-app/v1/workflows',
      'requires_confirmation',false,'requires_reason',false,
      'enabled',true,'disabled_reason',null,'timesheet_id',p_timesheet_id,
      'contract_week_id',p_contract_week_id
    );
  end if;
  if coalesce((p_capabilities->>'can_edit_expenses')::boolean,false) then
    return jsonb_build_object(
      'code','ADD_EXPENSES','label','Add Expenses',
      'method','POST','path','/candidate-app/v1/workflows',
      'requires_confirmation',false,'requires_reason',false,
      'enabled',true,'disabled_reason',null,'timesheet_id',p_timesheet_id,
      'contract_week_id',p_contract_week_id
    );
  end if;
  return null;
end;
$function$;

-- Keep the complete action collection aligned with the authoritative primary
-- action. A current Candidate workflow may not expose a parallel Add Expenses
-- choice merely because the underlying record permits later expenses.
create or replace function private._candidate_timesheet_action_contract_v1(
  p_candidate_status_code text,
  p_workflows jsonb,
  p_capabilities jsonb,
  p_timesheet_id uuid,
  p_contract_week_id uuid,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_primary jsonb;
  v_actions jsonb:='[]'::jsonb;
  v_workflow_json jsonb;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_approval public.candidate_approval_requests%rowtype;
  v_manager_approval jsonb;
  v_provider_first_accepted_at timestamptz;
  v_provider_accepted_at timestamptz;
  v_latest_delivery_state text;
  v_latest_provider_status text;
  v_pending_delivery_count integer:=0;
  v_next_reminder_at timestamptz;
  v_reminder_eligible boolean:=false;
  v_renewal_eligible boolean:=false;
  v_cancel_eligible boolean:=false;
  v_effective_request_state text;
  v_disabled_reason text;
  v_action jsonb;
  v_cancel_code text;
  v_cancel_authority jsonb;
  v_rejection jsonb;
  v_paper_pack jsonb;
begin
  v_primary:=private._candidate_timesheet_primary_action_v1(
    p_candidate_status_code,p_workflows,p_capabilities,p_timesheet_id,p_contract_week_id
  );
  if v_primary is not null then v_actions:=jsonb_build_array(v_primary); end if;

  for v_rejection in
    select item
    from jsonb_array_elements(coalesce(p_workflows,'[]'::jsonb)) item
    where item->>'state'='REJECTED'
      and coalesce((item->>'rejection_actionable')::boolean,false)
      and nullif(item->>'required_resubmission_action','') is not null
    order by coalesce((item->>'detail_action_owner')::boolean,false) desc,
      item->>'updated_at_utc' desc,item->>'workflow_id'
  loop
    if not exists(
      select 1 from jsonb_array_elements(v_actions) existing
      where existing->>'workflow_id'=v_rejection->>'workflow_id'
        and existing->>'code'=v_rejection->>'required_resubmission_action'
    ) then
      v_action:=jsonb_build_object(
        'code',v_rejection->>'required_resubmission_action',
        'label',case v_rejection->>'required_resubmission_action'
          when 'RESUBMIT_EXPENSE_CLAIM' then 'Resubmit Expense Claim'
          when 'RESUBMIT_TIMESHEET_AND_EXPENSES' then 'Resubmit Timesheet and Expenses'
          else 'Resubmit Timesheet' end,
        'method','POST','path','/candidate-app/v1/workflows/'
          ||(v_rejection->>'workflow_id')||'/resubmit',
        'requires_confirmation',false,'requires_reason',false,'enabled',true,
        'disabled_reason',null,'workflow_id',v_rejection->>'workflow_id',
        'workflow_generation',nullif(v_rejection->>'generation','')::integer,
        'claim_family',v_rejection->>'claim_family'
      );
      v_actions:=v_actions||jsonb_build_array(v_action);
    end if;
  end loop;

  select item into v_workflow_json
  from jsonb_array_elements(coalesce(p_workflows,'[]'::jsonb)) item
  where item->>'state' in (
    'CREATED','WORKER_DRAFT','WORKER_SUBMITTED',
    'WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT','READY_FOR_MANAGER_APPROVAL',
    'AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED',
    'MANAGER_APPROVED_PENDING_FINAL_DOCUMENT','READY_TO_FINALISE',
    'AWAITING_PAPER_RETURN','RECEIVED','REFUSED','FINALISED'
  )
  order by coalesce((item->>'detail_action_owner')::boolean,false) desc,
    item->>'updated_at_utc' desc,item->>'workflow_id'
  limit 1;

  if v_workflow_json is not null then
    select workflow_row.* into v_workflow
    from public.candidate_submission_workflows workflow_row
    where workflow_row.id=(v_workflow_json->>'workflow_id')::uuid;
  end if;

  if v_workflow.id is not null then
    v_cancel_authority:=private._candidate_workflow_cancel_authority_v1(v_workflow.id);
    v_cancel_eligible:=coalesce((v_cancel_authority->>'eligible')::boolean,false);
    v_cancel_code:=case when v_workflow.workflow_kind='CONTRACT_EXPENSE'
      then 'DISCARD_EXPENSE_CLAIM' else 'CANCEL_ENTIRE_CLAIM_AND_START_AGAIN' end;
    if v_cancel_eligible then
      v_actions:=v_actions||jsonb_build_array(jsonb_build_object(
        'code',v_cancel_code,
        'label',case when v_cancel_code='DISCARD_EXPENSE_CLAIM'
          then 'Discard Expense Claim' else 'Cancel Entire Claim and Start Again' end,
        'method','POST','path','/candidate-app/v1/workflows/'||v_workflow.id::text||'/actions/cancel',
        'workflow_id',v_workflow.id,'workflow_generation',v_workflow.generation,
        'approval_request_id',null,'requires_confirmation',true,'requires_reason',true,
        'enabled',true,'disabled_reason',null
      ));
    end if;

    if v_workflow.route='EMAIL' and v_workflow.state='AWAITING_MANAGER_APPROVAL' then
      select request_row.* into v_approval
      from public.candidate_approval_requests request_row
      where request_row.workflow_id=v_workflow.id
        and request_row.workflow_generation=v_workflow.generation
        and request_row.method='EMAIL'
      order by request_row.request_generation desc,request_row.created_at_utc desc
      limit 1;
      if found then
        select
          min(delivery.sent_at) filter (
            where delivery.status='SENT' and delivery.sent_at is not null
              and upper(coalesce(delivery.provider_status,'')) in ('ACCEPTED','SENT','SUCCESS','OK')
          ),
          max(delivery.sent_at) filter (
            where delivery.status='SENT' and delivery.sent_at is not null
              and upper(coalesce(delivery.provider_status,'')) in ('ACCEPTED','SENT','SUCCESS','OK')
          ),
          count(*) filter (
            where delivery.status='QUEUED' and delivery.sent_at is null
              and lower(coalesce(delivery.payment_scope_json->>'candidate_manager_mail_retired','false'))
                    in ('false','f','0','no')
          )::integer
        into v_provider_first_accepted_at,v_provider_accepted_at,v_pending_delivery_count
        from public.mail_outbox delivery
        where upper(coalesce(delivery.payment_scope_json->>'candidate_mail_authority',''))
                ='MANAGER_APPROVAL_V1'
          and delivery.payment_scope_json->>'candidate_manager_workflow_id'=v_workflow.id::text
          and delivery.payment_scope_json->>'candidate_manager_workflow_generation'=v_workflow.generation::text
          and delivery.payment_scope_json->>'candidate_approval_request_id'=v_approval.id::text
          and delivery.payment_scope_json->>'candidate_approval_request_generation'=v_approval.request_generation::text
          and upper(coalesce(delivery.payment_scope_json->>'candidate_manager_mail_kind',''))
                in ('INITIAL','REMINDER','RENEWAL');
        select delivery.status,delivery.provider_status
        into v_latest_delivery_state,v_latest_provider_status
        from public.mail_outbox delivery
        where upper(coalesce(delivery.payment_scope_json->>'candidate_mail_authority',''))
                ='MANAGER_APPROVAL_V1'
          and delivery.payment_scope_json->>'candidate_approval_request_id'=v_approval.id::text
          and upper(coalesce(delivery.payment_scope_json->>'candidate_manager_mail_kind',''))
                in ('INITIAL','REMINDER','RENEWAL')
        order by delivery.created_at_utc desc,delivery.id desc limit 1;

        v_effective_request_state:=case
          when v_approval.state='PENDING' and v_approval.expires_at_utc<=p_now_utc then 'EXPIRED'
          else v_approval.state end;
        v_next_reminder_at:=case when v_provider_accepted_at is null then null
          else v_provider_accepted_at+interval '24 hours' end;
        v_reminder_eligible:=v_effective_request_state='PENDING'
          and v_provider_accepted_at is not null
          and v_next_reminder_at<=p_now_utc
          and v_approval.resend_count<5
          and v_pending_delivery_count=0;
        v_renewal_eligible:=v_effective_request_state='EXPIRED'
          and v_approval.review_manifest_sha256 is not distinct from v_workflow.review_manifest_sha256;
        v_disabled_reason:=case
          when v_effective_request_state<>'PENDING' then 'MANAGER_APPROVAL_REQUEST_NOT_PENDING'
          when v_provider_accepted_at is null then 'MANAGER_EMAIL_NOT_PROVIDER_ACCEPTED'
          when v_pending_delivery_count>0 then 'MANAGER_EMAIL_DELIVERY_PENDING'
          when v_approval.resend_count>=5 then 'MANAGER_REMINDER_LIMIT_REACHED'
          when v_next_reminder_at>p_now_utc then 'MANAGER_REMINDER_WAIT_24_HOURS'
          else null end;
        v_manager_approval:=jsonb_build_object(
          'method','EMAIL','request_id',v_approval.id,
          'request_generation',v_approval.request_generation,
          'state',v_effective_request_state,
          'stored_state',v_approval.state,
          'provider_first_accepted_at_utc',v_provider_first_accepted_at,
          'provider_accepted_at_utc',v_provider_accepted_at,
          'delivery_state',v_latest_delivery_state,
          'provider_status',v_latest_provider_status,
          'delivery_pending',v_pending_delivery_count>0,
          'expires_at_utc',v_approval.expires_at_utc,
          'resend_count',v_approval.resend_count,
          'resends_remaining',greatest(5-v_approval.resend_count,0),
          'next_reminder_at_utc',v_next_reminder_at,
          'reminder_eligible',v_reminder_eligible,
          'renewal_eligible',v_renewal_eligible,
          'cancel_eligible',v_cancel_eligible
        );
        v_actions:=v_actions||jsonb_build_array(jsonb_build_object(
          'code','SEND_MANAGER_REMINDER','label','Send Manager Reminder',
          'method','POST','path','/candidate-app/v1/workflows/'||v_workflow.id::text||'/actions/remind',
          'workflow_id',v_workflow.id,'workflow_generation',v_workflow.generation,
          'approval_request_id',v_approval.id,
          'approval_request_generation',v_approval.request_generation,
          'requires_confirmation',false,'requires_reason',false,
          'enabled',v_reminder_eligible,'disabled_reason',v_disabled_reason
        ));
        v_actions:=v_actions||jsonb_build_array(jsonb_build_object(
          'code','REQUEST_APPROVAL_AGAIN','label','Request Approval Again',
          'method','POST','path','/candidate-app/v1/workflows/'||v_workflow.id::text||'/actions/renew',
          'workflow_id',v_workflow.id,'workflow_generation',v_workflow.generation,
          'approval_request_id',v_approval.id,
          'approval_request_generation',v_approval.request_generation,
          'requires_confirmation',true,'requires_reason',false,
          'enabled',v_renewal_eligible,
          'disabled_reason',case when v_renewal_eligible then null else 'MANAGER_APPROVAL_REQUEST_NOT_EXPIRED' end
        ));
      end if;
    end if;

    if v_workflow.route='PAPER' and v_workflow.state='AWAITING_PAPER_RETURN' then
      v_paper_pack:=private._candidate_paper_pack_readiness_v1(v_workflow.id,v_workflow.generation);
      v_actions:=v_actions||jsonb_build_array(
        jsonb_build_object(
          'code','DOWNLOAD_PAPER_DOCUMENTS','label','Download Documents','method','GET',
          'path','/candidate-app/v1/timesheets/'||coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id)::text||'/paper-pack',
          'workflow_id',v_workflow.id,'workflow_generation',v_workflow.generation,
          'approval_request_id',null,'requires_confirmation',false,'requires_reason',false,
          'enabled',coalesce((v_paper_pack->>'download_available')::boolean,false),
          'disabled_reason',case when coalesce((v_paper_pack->>'download_available')::boolean,false)
            then null else v_paper_pack->>'reason_code' end
        ),
        jsonb_build_object(
          'code','UPLOAD_SIGNED_RETURN','label','Upload Signed Return','method','POST',
          'path','/candidate-app/v1/workflows/'||v_workflow.id::text||'/actions/paper-return',
          'workflow_id',v_workflow.id,'workflow_generation',v_workflow.generation,
          'approval_request_id',null,'requires_confirmation',true,'requires_reason',false,
          'enabled',coalesce((v_paper_pack->>'upload_available')::boolean,false),
          'disabled_reason',case when coalesce((v_paper_pack->>'upload_available')::boolean,false)
            then null else v_paper_pack->>'reason_code' end
        )
      );
    elsif v_workflow.route='PAPER' and v_workflow.state='RECEIVED' then
      v_actions:=v_actions||jsonb_build_array(jsonb_build_object(
        'code','RETRY_FINALISATION','label','Retry Finalisation','method','POST',
        'path','/candidate-app/v1/workflows/'||v_workflow.id::text||'/actions/retry-finalisation',
        'workflow_id',v_workflow.id,'workflow_generation',v_workflow.generation,
        'approval_request_id',null,'requires_confirmation',false,'requires_reason',false,
        'enabled',true,'disabled_reason',null
      ));
    end if;
  end if;

  if coalesce((p_capabilities->>'candidate_no_work_allowed')::boolean,false) then
    v_actions:=v_actions||jsonb_build_array(jsonb_build_object(
      'code','NO_WORK_THIS_WEEK','label','I Did Not Work This Week','method','POST',
      'path','/candidate-app/v1/contract-weeks/'||p_contract_week_id::text||'/no-work',
      'timesheet_id',p_timesheet_id,'contract_week_id',p_contract_week_id,
      'requires_confirmation',true,'requires_reason',false,'enabled',true,'disabled_reason',null
    ));
  end if;
  if coalesce((p_capabilities->>'can_edit_expenses')::boolean,false)
     and coalesce(v_primary->>'code','')<>'ADD_EXPENSES'
     and (
       jsonb_array_length(coalesce(p_workflows,'[]'::jsonb))=0
       or coalesce(v_primary->>'code','')='ENTER_TIMESHEET'
     ) then
    v_actions:=v_actions||jsonb_build_array(jsonb_build_object(
      'code','ADD_EXPENSES','label','Add Expenses','method','POST',
      'path','/candidate-app/v1/workflows','timesheet_id',p_timesheet_id,
      'contract_week_id',p_contract_week_id,'requires_confirmation',false,
      'requires_reason',false,'enabled',true,'disabled_reason',null
    ));
  end if;
  select coalesce(jsonb_agg(private._candidate_action_invocation_v1(item)
    order by ordinal),'[]'::jsonb)
  into v_actions
  from jsonb_array_elements(v_actions) with ordinality action_item(item,ordinal);
  if v_primary is not null then
    v_primary:=private._candidate_action_invocation_v1(v_primary);
  end if;
  return jsonb_build_object(
    'primary_action',v_primary,
    'available_actions',v_actions,
    'manager_approval',v_manager_approval,
    'paper_pack',v_paper_pack
  );
end;
$function$;

alter function private._candidate_timesheet_action_contract_v1(text,jsonb,jsonb,uuid,uuid,timestamptz)
  owner to postgres;
revoke all on function private._candidate_timesheet_action_contract_v1(text,jsonb,jsonb,uuid,uuid,timestamptz)
  from public,anon,authenticated,service_role;

alter function private._candidate_timesheet_primary_action_v1(text,jsonb,jsonb,uuid,uuid)
  owner to postgres;
revoke all on function private._candidate_timesheet_primary_action_v1(text,jsonb,jsonb,uuid,uuid)
  from public,anon,authenticated,service_role;

commit;
