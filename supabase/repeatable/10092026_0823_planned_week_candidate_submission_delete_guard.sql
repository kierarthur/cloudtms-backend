-- Repeatable CloudTMS function/view authority: planned-week Candidate
-- submission reject-before-delete guard.

\set ON_ERROR_STOP on
begin;

-- Office can open a Weekly Candidate submission before manager approval has
-- materialised its Timesheet.  In that state the Contract Week is still a
-- planned/open row, but deleting it must follow the same reject-first rule as
-- deleting an Electronic/QR Timesheet.  This authority is deliberately scoped
-- to an exact Contract Week and does not alter Daily or manual Office records.
create or replace function private._contract_week_submission_delete_guard_v1(
  p_environment text,
  p_contract_week_id uuid,
  p_lock_workflows boolean default false
)
returns jsonb
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_environment text:=private._candidate_assert_environment(p_environment);
  v_week public.contract_weeks%rowtype;
  v_related_workflow_ids uuid[]:=array[]::uuid[];
  v_guarded_workflows jsonb:='[]'::jsonb;
  v_stage text;
  v_context jsonb;
  v_context_sha text;
begin
  if p_contract_week_id is null then
    raise exception 'CONTRACT_WEEK_SUBMISSION_DELETE_TARGET_INVALID'
      using errcode='22023';
  end if;

  select week_row.* into v_week
  from public.contract_weeks week_row
  where week_row.id=p_contract_week_id;
  if not found then
    raise exception 'CONTRACT_WEEK_NOT_FOUND' using errcode='P0002';
  end if;
  if v_week.timesheet_id is not null then
    raise exception 'CONTRACT_WEEK_NOT_PLANNED_ONLY' using errcode='55000';
  end if;

  select coalesce(array_agg(workflow_row.id order by workflow_row.id),array[]::uuid[])
  into v_related_workflow_ids
  from public.candidate_submission_workflows workflow_row
  where workflow_row.environment=v_environment
    and workflow_row.contract_week_id=v_week.id
    and workflow_row.workflow_kind in (
      'CONTRACT_HOURS','CONTRACT_EXPENSE','CONTRACT_COMBINED'
    );

  if cardinality(v_related_workflow_ids)>20 then
    raise exception 'CONTRACT_WEEK_SUBMISSION_DELETE_SCOPE_TOO_LARGE'
      using errcode='54000';
  end if;

  if p_lock_workflows and cardinality(v_related_workflow_ids)>0 then
    perform 1
    from public.candidate_submission_workflows workflow_row
    where workflow_row.id=any(v_related_workflow_ids)
    order by workflow_row.id
    for update;
    return private._contract_week_submission_delete_guard_v1(
      v_environment,v_week.id,false
    );
  end if;

  select coalesce(jsonb_agg(jsonb_build_object(
    'workflow_id',workflow_row.id,
    'workflow_generation',workflow_row.generation,
    'workflow_kind',workflow_row.workflow_kind,
    'route',workflow_row.route,
    'state',workflow_row.state,
    'candidate_submission_stage',case
      when workflow_row.state in (
        'MANAGER_APPROVED','MANAGER_APPROVED_PENDING_FINAL_DOCUMENT',
        'READY_TO_FINALISE','RECEIVED','FINALISED'
      ) then 'MANAGER_APPROVED'
      else 'CANDIDATE_SUBMITTED'
    end
  ) order by workflow_row.id),'[]'::jsonb)
  into v_guarded_workflows
  from public.candidate_submission_workflows workflow_row
  where workflow_row.id=any(v_related_workflow_ids)
    and workflow_row.state in (
      'WORKER_SUBMITTED','WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT',
      'READY_FOR_MANAGER_APPROVAL','AWAITING_MANAGER_APPROVAL',
      'MANAGER_APPROVED','MANAGER_APPROVED_PENDING_FINAL_DOCUMENT',
      'READY_TO_FINALISE','AWAITING_PAPER_RETURN','RECEIVED','FINALISED'
    );

  v_stage:=case
    when exists(
      select 1 from jsonb_array_elements(v_guarded_workflows) item
      where item->>'candidate_submission_stage'='MANAGER_APPROVED'
    ) then 'MANAGER_APPROVED'
    when jsonb_array_length(v_guarded_workflows)>0 then 'CANDIDATE_SUBMITTED'
    else null
  end;

  v_context:=jsonb_build_object(
    'contract_version','CONTRACT_WEEK_SUBMISSION_DELETE_GUARD_V1',
    'environment',v_environment,
    'contract_week_id',v_week.id,
    'contract_id',v_week.contract_id,
    'week_ending_date',v_week.week_ending_date,
    'contract_week_status',v_week.status,
    'submission_mode_snapshot',v_week.submission_mode_snapshot,
    'timesheet_id',v_week.timesheet_id,
    'candidate_submission_rejection_required',
      jsonb_array_length(v_guarded_workflows)>0,
    'candidate_submission_stage',v_stage,
    'guarded_workflows',v_guarded_workflows,
    'related_workflow_ids',to_jsonb(v_related_workflow_ids)
  );
  v_context_sha:=encode(
    extensions.digest(convert_to(v_context::text,'UTF8'),'sha256'),'hex'
  );

  return v_context||jsonb_build_object(
    'ok',true,
    'guarded_workflow_count',jsonb_array_length(v_guarded_workflows),
    'linked_pending_expense_claim_count',(
      select count(*)::integer
      from jsonb_array_elements(v_guarded_workflows) item
      where item->>'workflow_kind'='CONTRACT_EXPENSE'
    ),
    'context_sha256',v_context_sha
  );
end;
$function$;

create or replace function public.contract_week_submission_delete_guard_preview_v1(
  p_environment text,
  p_contract_week_id uuid
)
returns jsonb
language sql
volatile
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
  select private._contract_week_submission_delete_guard_v1(
    p_environment,p_contract_week_id,false
  )
$function$;

-- Reject the exact submitted Candidate workflow while the Contract Week still
-- has no Timesheet.  The Candidate sees the normal refused/restart state; no
-- Timesheet is invented merely so Office can reject the submission.
create or replace function public.contract_week_submission_reject_atomic_v1(
  p_actor_user_id uuid,
  p_environment text,
  p_contract_week_id uuid,
  p_expected_context_sha256 text,
  p_reason text,
  p_idempotency_key text,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_environment text:=private._candidate_assert_environment(p_environment);
  v_week public.contract_weeks%rowtype;
  v_guard jsonb;
  v_workflow_ids uuid[]:=array[]::uuid[];
  v_workflow public.candidate_submission_workflows%rowtype;
  v_expense_component public.candidate_expense_components%rowtype;
  v_expense_before jsonb;
  v_rejected_workflow_ids uuid[]:=array[]::uuid[];
  v_paper_workflow_ids uuid[]:=array[]::uuid[];
  v_paper_workflow_generations integer[]:=array[]::integer[];
  v_paper_retirement jsonb;
  v_request_sha text;
  v_receipt_before jsonb;
  v_response jsonb;
begin
  if p_actor_user_id is null or p_contract_week_id is null
     or nullif(btrim(coalesce(p_expected_context_sha256,'')),'') is null
     or p_expected_context_sha256!~'^[0-9a-fA-F]{64}$'
     or nullif(btrim(coalesce(p_reason,'')),'') is null
     or char_length(btrim(p_reason))>1000
     or nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then
    raise exception 'CONTRACT_WEEK_SUBMISSION_REJECT_PAYLOAD_INVALID'
      using errcode='22023';
  end if;

  perform pg_advisory_xact_lock(hashtextextended(
    'CONTRACT_WEEK_SUBMISSION_REJECTION:'||p_actor_user_id::text||':'
      ||btrim(p_idempotency_key),0
  ));

  v_request_sha:=encode(extensions.digest(convert_to(jsonb_build_object(
    'contract_version','CONTRACT_WEEK_SUBMISSION_REJECTION_REQUEST_V1',
    'environment',v_environment,
    'actor_user_id',p_actor_user_id,
    'contract_week_id',p_contract_week_id,
    'expected_context_sha256',lower(p_expected_context_sha256),
    'reason',btrim(p_reason)
  )::text,'UTF8'),'sha256'),'hex');

  select event.before_json,event.after_json
  into v_receipt_before,v_response
  from public.audit_events event
  where event.object_type='contract_week_submission_rejection_receipt'
    and event.actor_user_id=p_actor_user_id
    and event.correlation_id=p_idempotency_key
  order by event.ts_utc desc,event.id desc
  limit 1;
  if found then
    if v_receipt_before->>'request_sha256' is distinct from v_request_sha then
      raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT' using errcode='40001';
    end if;
    return coalesce(v_response,'{}'::jsonb)||jsonb_build_object(
      'idempotent_replay',true
    );
  end if;

  select week_row.* into v_week
  from public.contract_weeks week_row
  where week_row.id=p_contract_week_id;
  if not found then raise exception 'CONTRACT_WEEK_NOT_FOUND' using errcode='P0002'; end if;
  if v_week.timesheet_id is not null then
    raise exception 'CONTRACT_WEEK_NOT_PLANNED_ONLY' using errcode='55000';
  end if;
  perform pg_advisory_xact_lock(hashtextextended(
    'CANDIDATE_PAPER_FAMILY:'||v_environment||':'
      ||coalesce(v_week.contract_id::text,'-')||':'
      ||coalesce(v_week.week_ending_date::text,'-'),0
  ));

  v_guard:=private._contract_week_submission_delete_guard_v1(
    v_environment,p_contract_week_id,false
  );
  select coalesce(array_agg((item->>'workflow_id')::uuid
    order by (item->>'workflow_id')::uuid),array[]::uuid[])
  into v_workflow_ids
  from jsonb_array_elements(coalesce(v_guard->'guarded_workflows','[]'::jsonb)) item;
  if cardinality(v_workflow_ids)>0 then
    perform 1 from public.candidate_submission_workflows workflow_row
    where workflow_row.id=any(v_workflow_ids)
    order by workflow_row.id
    for update;
  end if;
  select week_row.* into v_week
  from public.contract_weeks week_row
  where week_row.id=p_contract_week_id
  for update;
  if not found or v_week.timesheet_id is not null then
    raise exception 'CONTRACT_WEEK_CHANGED' using errcode='40001';
  end if;
  v_guard:=private._contract_week_submission_delete_guard_v1(
    v_environment,p_contract_week_id,true
  );
  if lower(v_guard->>'context_sha256')
     is distinct from lower(p_expected_context_sha256) then
    raise exception 'CONTRACT_WEEK_SUBMISSION_CONTEXT_CHANGED'
      using errcode='40001',detail=v_guard::text;
  end if;
  if not coalesce(
    (v_guard->>'candidate_submission_rejection_required')::boolean,false
  ) then
    raise exception 'CANDIDATE_REJECTION_SCOPE_CONFLICT' using errcode='55000';
  end if;

  for v_workflow in
    select workflow_row.*
    from public.candidate_submission_workflows workflow_row
    where workflow_row.id=any(v_workflow_ids)
    order by workflow_row.id
    for update
  loop
    v_rejected_workflow_ids:=array_append(v_rejected_workflow_ids,v_workflow.id);
    if v_workflow.route='PAPER'
       and v_workflow.state in ('AWAITING_PAPER_RETURN','RECEIVED','FINALISED') then
      v_paper_workflow_ids:=array_append(v_paper_workflow_ids,v_workflow.id);
      v_paper_workflow_generations:=array_append(
        v_paper_workflow_generations,v_workflow.generation
      );
    end if;
  end loop;

  if cardinality(v_paper_workflow_ids)>0 then
    v_paper_retirement:=private._candidate_paper_delivery_retire_set_v1(
      v_paper_workflow_ids,v_paper_workflow_generations,
      'OFFICE_REJECTED',p_now_utc
    );
    if not coalesce((v_paper_retirement->>'retired')::boolean,false)
       or not coalesce(
         (v_paper_retirement->>'qr_invalidation_proven')::boolean,false
       ) then
      raise exception 'CANDIDATE_PAPER_QR_INVALIDATION_NOT_PROVEN'
        using errcode='40001',detail=v_paper_retirement::text;
    end if;
  end if;

  for v_workflow in
    select workflow_row.*
    from public.candidate_submission_workflows workflow_row
    where workflow_row.id=any(v_rejected_workflow_ids)
    order by workflow_row.id
    for update
  loop
    update public.candidate_approval_requests approval set
      state='SUPERSEDED',
      superseded_at_utc=coalesce(approval.superseded_at_utc,p_now_utc),
      updated_at_utc=p_now_utc
    where approval.workflow_id=v_workflow.id
      and approval.workflow_generation=v_workflow.generation
      and approval.state in ('PENDING','APPROVED');

    update public.candidate_submission_components component set
      state='REJECTED',
      superseded_at_utc=coalesce(component.superseded_at_utc,p_now_utc)
    where component.workflow_id=v_workflow.id
      and component.workflow_generation=v_workflow.generation
      and component.state not in ('REJECTED','SUPERSEDED','ABANDONED');

    for v_expense_component in
      select component.*
      from public.candidate_expense_components component
      where component.workflow_id=v_workflow.id
        and component.lifecycle_state not in (
          'MANAGER_REFUSED','OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED'
        )
      order by component.expense_component_id
      for update
    loop
      v_expense_before:=to_jsonb(v_expense_component);
      update public.candidate_expense_components component set
        component_generation=component.component_generation+1,
        lifecycle_state='OFFICE_REJECTED',
        refusal_kind='AGENCY_REJECTION',
        refusal_reason=btrim(p_reason),
        refused_at_utc=p_now_utc,
        removed_at_utc=p_now_utc,
        updated_at_utc=p_now_utc
      where component.expense_component_id=v_expense_component.expense_component_id
      returning component.* into v_expense_component;
      insert into public.candidate_expense_component_events(
        expense_component_id,workflow_id,component_generation,event_type,
        actor_kind,actor_id,before_state_json,after_state_json,
        idempotency_key,occurred_at_utc
      ) values (
        v_expense_component.expense_component_id,v_workflow.id,
        v_expense_component.component_generation,'OFFICE_REJECTED','OFFICE',
        p_actor_user_id,v_expense_before,to_jsonb(v_expense_component),
        'office-planned-week-reject:'||p_idempotency_key||':'
          ||v_expense_component.expense_component_id::text,p_now_utc
      ) on conflict(expense_component_id,idempotency_key) do nothing;
    end loop;

    update public.candidate_submission_workflows workflow_row set
      state='REJECTED',
      generation=workflow_row.generation+1,
      rejection_reason=btrim(p_reason),
      rejection_scope=case
        when workflow_row.workflow_kind='CONTRACT_EXPENSE'
          then 'COMPLETE_EXPENSE_CLAIM'
        else 'COMPLETE_TIMESHEET_CLAIM'
      end,
      updated_at_utc=p_now_utc
    where workflow_row.id=v_workflow.id
      and workflow_row.generation=v_workflow.generation
      and workflow_row.state=v_workflow.state;
    if not found then
      raise exception 'CANDIDATE_REJECT_WORKFLOW_CONFLICT' using errcode='40001';
    end if;

    perform private._candidate_notification_insert_v1(
      v_workflow.account_id,v_workflow.candidate_id,v_workflow.id,null,
      'OFFICE_REJECTED','office_rejection','candidate-office-rejected-v1',
      jsonb_build_object(
        'reason',btrim(p_reason),
        'reason_code','OFFICE_REJECTED',
        'workflow_id',v_workflow.id,
        'resubmission_scope',case
          when v_workflow.workflow_kind='CONTRACT_EXPENSE'
            then 'COMPLETE_EXPENSE_CLAIM'
          else 'COMPLETE_TIMESHEET_CLAIM'
        end
      ),
      jsonb_build_object('type','workflow','workflow_id',v_workflow.id),
      'CANDIDATE_OFFICE_REJECTED_PLANNED_WEEK_V1:'||v_workflow.id::text||':'
        ||(v_workflow.generation+1)::text,
      p_now_utc
    );
  end loop;

  update public.contract_weeks set
    status='OPEN',day_entries_json='[]'::jsonb,totals_json='{}'::jsonb,
    updated_at=p_now_utc
  where id=p_contract_week_id and timesheet_id is null;

  v_response:=jsonb_build_object(
    'ok',true,
    'contract_version','CONTRACT_WEEK_SUBMISSION_REJECTION_V1',
    'contract_week_id',p_contract_week_id,
    'contract_week_status','OPEN',
    'processing_status','UNPROCESSED',
    'rejected_workflow_ids',to_jsonb(v_rejected_workflow_ids),
    'candidate_submission_rejected',true,
    'candidate_must_start_new_claim',true,
    'paper_retirement_receipt',v_paper_retirement,
    'idempotency_key',p_idempotency_key,
    'idempotent_replay',false
  );
  insert into public.audit_events(
    actor_user_id,object_type,object_id_text,action,before_json,after_json,
    reason,correlation_id,ts_utc
  ) values (
    p_actor_user_id,'contract_week',p_contract_week_id::text,
    'CANDIDATE_SUBMISSION_REJECTED',v_guard,v_response,btrim(p_reason),
    p_idempotency_key,p_now_utc
  );
  insert into public.audit_events(
    actor_user_id,object_type,object_id_text,action,before_json,after_json,
    reason,correlation_id,ts_utc
  ) values (
    p_actor_user_id,'contract_week_submission_rejection_receipt',
    p_contract_week_id::text,'CANDIDATE_SUBMISSION_REJECTION_RECEIPT',
    jsonb_build_object(
      'request_sha256',v_request_sha,
      'contract_version','CONTRACT_WEEK_SUBMISSION_REJECTION_REQUEST_V1'
    ),v_response,btrim(p_reason),p_idempotency_key,p_now_utc
  );
  return v_response;
end;
$function$;

-- Final planned-week delete owner.  It proves that no submitted or approved
-- workflow remains, retires only safe draft/terminal workflow links, and then
-- delegates to the established planned-week/Contract-boundary delete owner.
create or replace function public.contract_week_delete_planned_guarded_v1(
  p_environment text,
  p_contract_week_id uuid,
  p_actor_user_id uuid,
  p_expected_context_sha256 text,
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
  v_week public.contract_weeks%rowtype;
  v_guard jsonb;
  v_related_workflow_ids uuid[]:=array[]::uuid[];
  v_workflow public.candidate_submission_workflows%rowtype;
  v_expense_component public.candidate_expense_components%rowtype;
  v_expense_before jsonb;
  v_retired_workflow_ids uuid[]:=array[]::uuid[];
  v_result record;
begin
  if p_actor_user_id is null or p_contract_week_id is null
     or p_delete_operation_id is null
     or nullif(btrim(coalesce(p_expected_context_sha256,'')),'') is null
     or p_expected_context_sha256!~'^[0-9a-fA-F]{64}$' then
    raise exception 'CONTRACT_WEEK_DELETE_GUARD_PAYLOAD_INVALID'
      using errcode='22023';
  end if;

  select week_row.* into v_week
  from public.contract_weeks week_row
  where week_row.id=p_contract_week_id;
  if not found then raise exception 'CONTRACT_WEEK_NOT_FOUND' using errcode='P0002'; end if;
  if v_week.timesheet_id is not null then
    raise exception 'CONTRACT_WEEK_NOT_PLANNED_ONLY' using errcode='55000';
  end if;
  perform pg_advisory_xact_lock(hashtextextended(
    'CANDIDATE_PAPER_FAMILY:'||v_environment||':'
      ||coalesce(v_week.contract_id::text,'-')||':'
      ||coalesce(v_week.week_ending_date::text,'-'),0
  ));

  v_guard:=private._contract_week_submission_delete_guard_v1(
    v_environment,p_contract_week_id,false
  );
  select coalesce(array_agg(value::uuid order by value::uuid),array[]::uuid[])
  into v_related_workflow_ids
  from jsonb_array_elements_text(coalesce(
    v_guard->'related_workflow_ids','[]'::jsonb
  )) ids(value);
  if cardinality(v_related_workflow_ids)>0 then
    perform 1 from public.candidate_submission_workflows workflow_row
    where workflow_row.id=any(v_related_workflow_ids)
    order by workflow_row.id
    for update;
  end if;
  select week_row.* into v_week
  from public.contract_weeks week_row
  where week_row.id=p_contract_week_id
  for update;
  if not found or v_week.timesheet_id is not null then
    raise exception 'CONTRACT_WEEK_CHANGED' using errcode='40001';
  end if;
  v_guard:=private._contract_week_submission_delete_guard_v1(
    v_environment,p_contract_week_id,true
  );
  if lower(v_guard->>'context_sha256')
     is distinct from lower(p_expected_context_sha256) then
    raise exception 'CONTRACT_WEEK_SUBMISSION_CONTEXT_CHANGED'
      using errcode='40001',detail=v_guard::text;
  end if;
  if coalesce(
    (v_guard->>'candidate_submission_rejection_required')::boolean,false
  ) then
    raise exception 'CANDIDATE_SUBMISSION_REJECTION_REQUIRED'
      using errcode='55000',detail=v_guard::text;
  end if;

  for v_workflow in
    select workflow_row.*
    from public.candidate_submission_workflows workflow_row
    where workflow_row.id=any(v_related_workflow_ids)
    order by workflow_row.id
    for update
  loop
    if v_workflow.state not in (
      'CREATED','WORKER_DRAFT','REFUSED','REJECTED','CANCELLED','EXPIRED','SUPERSEDED'
    ) then
      raise exception 'CANDIDATE_SUBMISSION_REJECTION_REQUIRED'
        using errcode='55000';
    end if;

    update public.candidate_approval_requests approval set
      state='CANCELLED',
      cancelled_at_utc=coalesce(approval.cancelled_at_utc,p_now_utc),
      updated_at_utc=p_now_utc
    where approval.workflow_id=v_workflow.id and approval.state='PENDING';

    for v_expense_component in
      select component.* from public.candidate_expense_components component
      where component.workflow_id=v_workflow.id
        and component.lifecycle_state not in (
          'MANAGER_REFUSED','OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED'
        )
      order by component.expense_component_id
      for update
    loop
      v_expense_before:=to_jsonb(v_expense_component);
      update public.candidate_expense_components component set
        component_generation=component.component_generation+1,
        owning_timesheet_id=null,
        lifecycle_state='CANCELLED',
        manager_approval_state=case
          when component.manager_approval_state in ('APPROVED','REFUSED')
            then component.manager_approval_state
          else 'NOT_REQUESTED'
        end,
        approval_request_id=case
          when component.manager_approval_state in ('APPROVED','REFUSED')
            then component.approval_request_id
          else null
        end,
        removed_at_utc=coalesce(component.removed_at_utc,p_now_utc),
        updated_at_utc=p_now_utc
      where component.expense_component_id=v_expense_component.expense_component_id
      returning component.* into v_expense_component;
      insert into public.candidate_expense_component_events(
        expense_component_id,workflow_id,component_generation,event_type,
        actor_kind,actor_id,before_state_json,after_state_json,
        idempotency_key,occurred_at_utc
      ) values (
        v_expense_component.expense_component_id,v_workflow.id,
        v_expense_component.component_generation,'CANCELLED','OFFICE',
        p_actor_user_id,v_expense_before,to_jsonb(v_expense_component),
        'office-planned-week-delete:'||p_delete_operation_id::text||':'
          ||v_expense_component.expense_component_id::text,p_now_utc
      ) on conflict(expense_component_id,idempotency_key) do nothing;
    end loop;

    update public.candidate_submission_components component set
      state=case
        when v_workflow.state in ('CREATED','WORKER_DRAFT')
          and component.state not in ('SUPERSEDED','REJECTED','ABANDONED')
          then 'ABANDONED'
        else component.state
      end,
      superseded_at_utc=case
        when v_workflow.state in ('CREATED','WORKER_DRAFT')
          then coalesce(component.superseded_at_utc,p_now_utc)
        else component.superseded_at_utc
      end
    where component.workflow_id=v_workflow.id;

    update public.candidate_notifications notification set
      timesheet_id=null,
      deep_link_json=(coalesce(notification.deep_link_json,'{}'::jsonb)
        -'timesheet_id'-'contract_week_id')||jsonb_build_object(
          'type','workflow','workflow_id',v_workflow.id
        )
    where notification.workflow_id=v_workflow.id;

    update public.candidate_submission_workflows workflow_row set
      state=case when workflow_row.state in ('CREATED','WORKER_DRAFT')
        then 'CANCELLED' else workflow_row.state end,
      contract_week_id=null,
      anchor_timesheet_id=null,
      target_timesheet_id=null,
      cancelled_at_utc=case when workflow_row.state in ('CREATED','WORKER_DRAFT')
        then coalesce(workflow_row.cancelled_at_utc,p_now_utc)
        else workflow_row.cancelled_at_utc end,
      input_snapshot_json=coalesce(workflow_row.input_snapshot_json,'{}'::jsonb)
        ||jsonb_build_object(
          'office_permanent_delete_tombstone',jsonb_build_object(
            'delete_operation_id',p_delete_operation_id,
            'deleted_timesheet_ids','[]'::jsonb,
            'deleted_contract_week_ids',jsonb_build_array(p_contract_week_id),
            'previous_contract_week_id',v_workflow.contract_week_id,
            'previous_anchor_timesheet_id',v_workflow.anchor_timesheet_id,
            'previous_target_timesheet_id',v_workflow.target_timesheet_id,
            'retired_at_utc',p_now_utc
          )
        ),
      issue_codes=case when workflow_row.issue_codes
        @> '["OFFICE_PERMANENTLY_DELETED_TIMESHEET"]'::jsonb
        then workflow_row.issue_codes
        else workflow_row.issue_codes
          ||'["OFFICE_PERMANENTLY_DELETED_TIMESHEET"]'::jsonb
      end,
      updated_at_utc=p_now_utc
    where workflow_row.id=v_workflow.id;

    insert into public.audit_events(
      actor_user_id,object_type,object_id_text,action,before_json,after_json,
      reason,correlation_id,ts_utc
    ) values (
      p_actor_user_id,'candidate_submission_workflows',v_workflow.id::text,
      'CANDIDATE_WORKFLOW_RETAINED_AFTER_PLANNED_WEEK_DELETE',
      jsonb_build_object(
        'state',v_workflow.state,
        'contract_week_id',v_workflow.contract_week_id
      ),
      jsonb_build_object(
        'state',case when v_workflow.state in ('CREATED','WORKER_DRAFT')
          then 'CANCELLED' else v_workflow.state end,
        'live_contract_week_link_released',true,
        'terminal_audit_retained',true
      ),
      'OFFICE_PERMANENTLY_DELETED_PLANNED_WEEK',p_delete_operation_id::text,
      p_now_utc
    );
    v_retired_workflow_ids:=array_append(v_retired_workflow_ids,v_workflow.id);
  end loop;

  select result.deleted,result.contract_week_id into v_result
  from public.contract_week_delete_planned(
    p_contract_week_id,p_actor_user_id
  ) result;
  if not coalesce(v_result.deleted,false) then
    raise exception 'CONTRACT_WEEK_DELETE_NOT_PERFORMED' using errcode='55000';
  end if;

  return jsonb_build_object(
    'ok',true,
    'contract_version','CONTRACT_WEEK_DELETE_PLANNED_GUARDED_V1',
    'deleted',v_result.deleted,
    'contract_week_id',v_result.contract_week_id,
    'delete_operation_id',p_delete_operation_id,
    'retired_workflow_ids',to_jsonb(v_retired_workflow_ids)
  );
end;
$function$;

alter function private._contract_week_submission_delete_guard_v1(text,uuid,boolean)
  owner to postgres;
alter function public.contract_week_submission_delete_guard_preview_v1(text,uuid)
  owner to postgres;
alter function public.contract_week_submission_reject_atomic_v1(
  uuid,text,uuid,text,text,text,timestamptz
) owner to postgres;
alter function public.contract_week_delete_planned_guarded_v1(
  text,uuid,uuid,text,uuid,timestamptz
) owner to postgres;

revoke all on function private._contract_week_submission_delete_guard_v1(
  text,uuid,boolean
) from public,anon,authenticated,service_role;
revoke all on function public.contract_week_submission_delete_guard_preview_v1(
  text,uuid
) from public,anon,authenticated;
revoke all on function public.contract_week_submission_reject_atomic_v1(
  uuid,text,uuid,text,text,text,timestamptz
) from public,anon,authenticated;
revoke all on function public.contract_week_delete_planned_guarded_v1(
  text,uuid,uuid,text,uuid,timestamptz
) from public,anon,authenticated;

grant execute on function public.contract_week_submission_delete_guard_preview_v1(
  text,uuid
) to service_role;
grant execute on function public.contract_week_submission_reject_atomic_v1(
  uuid,text,uuid,text,text,text,timestamptz
) to service_role;
grant execute on function public.contract_week_delete_planned_guarded_v1(
  text,uuid,uuid,text,uuid,timestamptz
) to service_role;

comment on function public.contract_week_submission_delete_guard_preview_v1(text,uuid)
is 'Reports whether a planned/open Contract Week has a Candidate-submitted or manager-approved workflow that Office must reject before deletion.';
comment on function public.contract_week_submission_reject_atomic_v1(
  uuid,text,uuid,text,text,text,timestamptz
) is 'Rejects the complete Candidate submission on an exact planned/open Contract Week without inventing a Timesheet.';
comment on function public.contract_week_delete_planned_guarded_v1(
  text,uuid,uuid,text,uuid,timestamptz
) is 'Race-safe Office planned-week delete owner that requires Candidate rejection first and retains detached terminal workflow audit history.';

notify pgrst, 'reload schema';

commit;
