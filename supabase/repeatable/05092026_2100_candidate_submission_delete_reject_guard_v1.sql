begin;

-- One authority identifies every Candidate workflow that belongs to an Office
-- rejection.  A target-less expense claim is still a workflow, not a
-- Timesheet.  It follows its worked anchor across Timesheet version rotation,
-- but only Electronic/QR worked records may acquire that linked rejection
-- behaviour.  A Candidate-created expense carrier with its own Timesheet is
-- identified by its direct CONTRACT_EXPENSE workflow, regardless of the
-- carrier's MANUAL storage shape.
create or replace function private._candidate_office_rejection_targets_v2(
  p_environment text,
  p_timesheet_id uuid
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_environment text:=private._candidate_assert_environment(p_environment);
  v_timesheet public.timesheets%rowtype;
  v_week public.contract_weeks%rowtype;
  v_route jsonb;
  v_route_identity jsonb;
  v_route_timesheet_id uuid;
  v_route_contract_week_id uuid;
  v_route_family text;
  v_workflow_ids uuid[]:=array[]::uuid[];
  v_targets jsonb:='[]'::jsonb;
  v_rejection_required boolean:=false;
  v_candidate_submission_stage text;
  v_linked_pending_expense_count integer:=0;
begin
  if p_timesheet_id is null then
    raise exception 'CANDIDATE_REJECT_TARGET_NOT_FOUND' using errcode='P0002';
  end if;

  select timesheet_row.* into v_timesheet
  from public.timesheets timesheet_row
  where timesheet_row.timesheet_id=p_timesheet_id;
  if not found then
    raise exception 'CANDIDATE_REJECT_TARGET_NOT_FOUND' using errcode='P0002';
  end if;

  select week_row.* into v_week
  from public.contract_weeks week_row
  where week_row.timesheet_id=v_timesheet.timesheet_id
  order by week_row.updated_at desc,week_row.id desc
  limit 1;

  if v_timesheet.sheet_scope='DAILY'::public.timesheet_scope_enum then
    -- Daily is Contract-free and PHONE-only.  Its established Office rejection
    -- must continue to work, but it is outside the Electronic/QR delete gate.
    v_route_family:='PHONE';
  else
    if v_week.id is null then
      -- A Weekly parent delete contains every Timesheet version in the stable
      -- booking chain.  Older versions legitimately no longer own the current
      -- Contract Week, so resolve them through the same closed identity rule
      -- used by the Office Candidate projection before classifying the route.
      v_route_identity:=private._candidate_office_projection_identity_v1(
        v_timesheet.timesheet_id,null
      );
      v_route_timesheet_id:=nullif(
        v_route_identity->>'current_timesheet_id',''
      )::uuid;
      v_route_contract_week_id:=nullif(
        v_route_identity->>'contract_week_id',''
      )::uuid;
    else
      v_route_timesheet_id:=v_timesheet.timesheet_id;
      v_route_contract_week_id:=v_week.id;
    end if;
    v_route:=private._candidate_route_family_v1(
      v_route_timesheet_id,v_route_contract_week_id
    );
    v_route_family:=upper(coalesce(v_route->>'route_family',''));
  end if;

  select coalesce(array_agg(workflow_row.id order by workflow_row.id),array[]::uuid[])
  into v_workflow_ids
  from public.candidate_submission_workflows workflow_row
  left join public.timesheets anchor_timesheet
    on anchor_timesheet.timesheet_id=workflow_row.anchor_timesheet_id
  where workflow_row.environment=v_environment
    and (
      (
        workflow_row.state not in (
          'FINALISED','REFUSED','REJECTED','CANCELLED','EXPIRED','SUPERSEDED'
        )
        and (
          workflow_row.target_timesheet_id=v_timesheet.timesheet_id
          or workflow_row.anchor_timesheet_id=v_timesheet.timesheet_id
          or (
            workflow_row.target_timesheet_id is null
            and v_route_family in ('ELECTRONIC','QR')
            and workflow_row.anchor_timesheet_id is not null
            and (
              workflow_row.anchor_timesheet_id=v_timesheet.timesheet_id
              or (
                anchor_timesheet.booking_id is not distinct from v_timesheet.booking_id
                and anchor_timesheet.occupant_key_norm
                  is not distinct from v_timesheet.occupant_key_norm
                and anchor_timesheet.contract_id is not distinct from v_timesheet.contract_id
                and anchor_timesheet.week_ending_date is not distinct from v_timesheet.week_ending_date
              )
            )
          )
        )
      )
      or (
        workflow_row.state='FINALISED'
        and workflow_row.target_timesheet_id=v_timesheet.timesheet_id
      )
    );

  if cardinality(v_workflow_ids)>20 then
    raise exception 'CANDIDATE_REJECTION_SCOPE_TOO_LARGE' using errcode='54000';
  end if;

  select coalesce(jsonb_agg(jsonb_build_object(
    'workflow_id',workflow_row.id,
    'workflow_generation',workflow_row.generation,
    'workflow_kind',workflow_row.workflow_kind,
    'route',workflow_row.route,
    'state',workflow_row.state,
    'scope',case
      when workflow_row.workflow_kind='CONTRACT_EXPENSE' then 'EXPENSE'
      when workflow_row.workflow_kind='CONTRACT_COMBINED' then 'COMBINED'
      else 'HOURS'
    end,
    'link_kind',case
      when workflow_row.target_timesheet_id=v_timesheet.timesheet_id then 'TARGET'
      when workflow_row.anchor_timesheet_id=v_timesheet.timesheet_id then 'ANCHOR'
      else 'ROTATED_ANCHOR'
    end,
    'linked_pending_expense',workflow_row.workflow_kind='CONTRACT_EXPENSE'
      and workflow_row.target_timesheet_id is null,
    'delete_guard_applicable',(
      workflow_row.workflow_kind='CONTRACT_EXPENSE'
        and workflow_row.target_timesheet_id=v_timesheet.timesheet_id
    ) or v_route_family in ('ELECTRONIC','QR'),
    'candidate_submission_stage',case
      when workflow_row.state in (
        'MANAGER_APPROVED','MANAGER_APPROVED_PENDING_FINAL_DOCUMENT',
        'READY_TO_FINALISE','RECEIVED','FINALISED'
      ) then 'MANAGER_APPROVED'
      when workflow_row.state in (
        'WORKER_SUBMITTED','WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT',
        'READY_FOR_MANAGER_APPROVAL','AWAITING_MANAGER_APPROVAL',
        'AWAITING_PAPER_RETURN'
      ) then 'CANDIDATE_SUBMITTED'
      else 'DRAFT'
    end
  ) order by workflow_row.id),'[]'::jsonb)
  into v_targets
  from public.candidate_submission_workflows workflow_row
  where workflow_row.id=any(v_workflow_ids);

  select
    coalesce(bool_or(target->>'candidate_submission_stage'<>'DRAFT'),false),
    case
      when coalesce(bool_or(target->>'candidate_submission_stage'='MANAGER_APPROVED'),false)
        then 'MANAGER_APPROVED'
      when coalesce(bool_or(target->>'candidate_submission_stage'='CANDIDATE_SUBMITTED'),false)
        then 'CANDIDATE_SUBMITTED'
      else null
    end,
    count(*) filter(where coalesce((target->>'linked_pending_expense')::boolean,false))::integer
  into v_rejection_required,v_candidate_submission_stage,v_linked_pending_expense_count
  from jsonb_array_elements(v_targets) target;

  return jsonb_build_object(
    'ok',true,
    'contract_version','CANDIDATE_OFFICE_REJECTION_TARGETS_V2',
    'environment',v_environment,
    'timesheet_id',v_timesheet.timesheet_id,
    'contract_week_id',v_week.id,
    'route_family',v_route_family,
    'target_workflows',v_targets,
    'target_workflow_count',jsonb_array_length(v_targets),
    'candidate_submission_rejection_required',v_rejection_required,
    'candidate_submission_stage',v_candidate_submission_stage,
    'linked_pending_expense_claim_count',v_linked_pending_expense_count
  );
end;
$function$;

create or replace function private._candidate_office_reject_preview_v1(
  p_environment text,
  p_timesheet_id uuid,
  p_actor_user_id uuid,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_environment text;
  v_timesheet public.timesheets%rowtype;
  v_week public.contract_weeks%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_capabilities jsonb;
  v_signature text;
  v_target_context jsonb;
  v_targets jsonb:='[]'::jsonb;
  v_context jsonb;
  v_context_sha text;
  v_permitted boolean:=false;
  v_disabled_code text;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  if p_actor_user_id is null or p_timesheet_id is null then
    raise exception 'CANDIDATE_OFFICE_PROJECTION_IDENTITY_INVALID' using errcode='22023';
  end if;
  select * into v_timesheet from public.timesheets where timesheet_id=p_timesheet_id;
  if not found then raise exception 'CANDIDATE_OFFICE_PROJECTION_NOT_FOUND' using errcode='P0002'; end if;
  if not v_timesheet.is_current or v_timesheet.archived_at_utc is not null then
    raise exception 'TIMESHEET_MOVED' using errcode='40001';
  end if;
  select * into v_week from public.contract_weeks where timesheet_id=v_timesheet.timesheet_id
  order by updated_at desc,id desc limit 1;
  if v_week.id is null and v_timesheet.sheet_scope<>'DAILY'::public.timesheet_scope_enum then
    raise exception 'CANDIDATE_CONTRACT_WEEK_NOT_FOUND' using errcode='P0002';
  end if;
  select * into v_fin from public.timesheets_financials
  where timesheet_id=v_timesheet.timesheet_id and is_current=true
  order by computed_at_utc desc nulls last,updated_at desc,id desc limit 1;
  v_signature:=coalesce(
    public.timesheet_lifecycle_guard_signature_v1(v_timesheet.timesheet_id,v_week.id,false)->>'row_signature',
    public.timesheet_lifecycle_guard_signature_v1(v_timesheet.timesheet_id,v_week.id,false)->>'backend_row_signature'
  );
  v_capabilities:=private._candidate_record_capabilities_v1(v_timesheet.timesheet_id,v_week.id,'{}'::jsonb);
  v_target_context:=private._candidate_office_rejection_targets_v2(
    v_environment,v_timesheet.timesheet_id
  );
  v_targets:=coalesce(v_target_context->'target_workflows','[]'::jsonb);
  v_permitted:=coalesce((v_capabilities->>'can_reject_candidate_submission')::boolean,false)
    and coalesce(v_fin.authorised_at_utc,v_timesheet.authorised_at_server) is null
    and v_fin.paid_at_utc is null and v_fin.locked_by_invoice_id is null
    and jsonb_array_length(v_targets)>0;
  v_disabled_code:=case
    when coalesce(v_fin.authorised_at_utc,v_timesheet.authorised_at_server) is not null then 'CANDIDATE_REQUIRES_UNAUTHORISE'
    when v_fin.paid_at_utc is not null or v_fin.locked_by_invoice_id is not null then 'CANDIDATE_PROTECTED_FINANCIAL_HISTORY'
    when not coalesce((v_capabilities->>'can_reject_candidate_submission')::boolean,false) then 'CANDIDATE_ACTION_NOT_ELIGIBLE'
    when jsonb_array_length(v_targets)=0 then 'CANDIDATE_REJECTION_SCOPE_CONFLICT'
    else null end;
  v_context:=jsonb_build_object(
    'contract_version','OFFICE_CANDIDATE_REJECTION_PREVIEW_V1',
    'environment',v_environment,
    'timesheet_id',v_timesheet.timesheet_id,
    'timesheet_version',v_timesheet.version,
    'contract_week_id',v_week.id,
    'row_signature',v_signature,
    'reject_scope',v_capabilities->>'reject_scope',
    'target_workflows',v_targets,
    'candidate_submission_stage',v_target_context->>'candidate_submission_stage',
    'linked_pending_expense_claim_count',coalesce(
      (v_target_context->>'linked_pending_expense_claim_count')::integer,0
    ),
    'permitted',v_permitted,
    'requires_unauthorise',coalesce(v_fin.authorised_at_utc,v_timesheet.authorised_at_server) is not null,
    'protected_financial_history',v_fin.paid_at_utc is not null or v_fin.locked_by_invoice_id is not null
  );
  v_context_sha:=encode(extensions.digest(convert_to(v_context::text,'UTF8'),'sha256'),'hex');
  return v_context||jsonb_build_object(
    'ok',true,
    'observed_at_utc',coalesce(p_now_utc,now()),
    'expected_timesheet_id',v_timesheet.timesheet_id,
    'expected_row_signature',v_signature,
    'scope',case v_capabilities->>'reject_scope'
      when 'COMPLETE_EXPENSE_CLAIM' then 'EXPENSE'
      else case when exists(select 1 from jsonb_array_elements(v_targets) x where x->>'workflow_kind'='CONTRACT_COMBINED') then 'COMBINED' else 'HOURS' end end,
    'target_workflow_id',case when jsonb_array_length(v_targets)=1 then (v_targets->0)->>'workflow_id' else null end,
    'target_workflow_generation',case when jsonb_array_length(v_targets)=1 then ((v_targets->0)->>'workflow_generation')::integer else null end,
    'requires_reason',true,
    'disabled_reason_code',v_disabled_code,
    'disabled_reason',case v_disabled_code
      when 'CANDIDATE_REQUIRES_UNAUTHORISE' then 'Unauthorise this timesheet before rejecting the Candidate submission.'
      when 'CANDIDATE_PROTECTED_FINANCIAL_HISTORY' then 'This submission can no longer be rejected because protected financial history exists.'
      when 'CANDIDATE_REJECTION_SCOPE_CONFLICT' then 'CloudTMS could not establish one safe Candidate rejection scope.'
      when 'CANDIDATE_ACTION_NOT_ELIGIBLE' then 'This Candidate submission is not currently eligible for rejection.'
      else null end,
    'context_sha256',v_context_sha,
    'affected_rows',jsonb_build_array(jsonb_build_object(
      'timesheet_id',v_timesheet.timesheet_id,'contract_week_id',v_week.id
    ))
  );
end;
$function$;

create or replace function private._timesheet_candidate_submission_delete_guard_v1(
  p_environment text,
  p_timesheet_ids uuid[],
  p_lock_workflows boolean default false
)
returns jsonb
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_environment text:=private._candidate_assert_environment(p_environment);
  v_timesheet_ids uuid[];
  v_timesheet_id uuid;
  v_target_context jsonb;
  v_target jsonb;
  v_guard_items jsonb:='[]'::jsonb;
  v_all_workflow_ids uuid[]:=array[]::uuid[];
  v_linked_pending_expense_workflow_ids uuid[]:=array[]::uuid[];
  v_stage text;
  v_linked_pending_expense_count integer:=0;
begin
  select coalesce(array_agg(distinct supplied_id order by supplied_id),array[]::uuid[])
  into v_timesheet_ids
  from unnest(coalesce(p_timesheet_ids,array[]::uuid[])) supplied(supplied_id)
  where supplied_id is not null;

  if cardinality(v_timesheet_ids)<1 or cardinality(v_timesheet_ids)>64 then
    raise exception 'CANDIDATE_SUBMISSION_DELETE_TARGET_SET_INVALID' using errcode='22023';
  end if;

  foreach v_timesheet_id in array v_timesheet_ids loop
    v_target_context:=private._candidate_office_rejection_targets_v2(
      v_environment,v_timesheet_id
    );
    for v_target in
      select value from jsonb_array_elements(coalesce(
        v_target_context->'target_workflows','[]'::jsonb
      )) target(value)
    loop
      if not ((v_target->>'workflow_id')::uuid=any(v_all_workflow_ids)) then
        v_all_workflow_ids:=array_append(
          v_all_workflow_ids,(v_target->>'workflow_id')::uuid
        );
      end if;
      if coalesce((v_target->>'delete_guard_applicable')::boolean,false)
         and coalesce((v_target->>'linked_pending_expense')::boolean,false)
         and not ((v_target->>'workflow_id')::uuid
           =any(v_linked_pending_expense_workflow_ids)) then
        v_linked_pending_expense_workflow_ids:=array_append(
          v_linked_pending_expense_workflow_ids,(v_target->>'workflow_id')::uuid
        );
      end if;
      if coalesce((v_target->>'delete_guard_applicable')::boolean,false)
         and v_target->>'candidate_submission_stage'<>'DRAFT'
         and not exists(
           select 1 from jsonb_array_elements(v_guard_items) existing(item)
           where existing.item->>'workflow_id'=v_target->>'workflow_id'
         ) then
        v_guard_items:=v_guard_items||jsonb_build_array(
          v_target||jsonb_build_object('rejection_timesheet_id',v_timesheet_id)
        );
      end if;
    end loop;
  end loop;

  if p_lock_workflows and cardinality(v_all_workflow_ids)>0 then
    perform 1
    from public.candidate_submission_workflows workflow_row
    where workflow_row.id=any(v_all_workflow_ids)
    order by workflow_row.id
    for update;
    return private._timesheet_candidate_submission_delete_guard_v1(
      v_environment,v_timesheet_ids,false
    );
  end if;

  v_stage:=case
    when exists(
      select 1 from jsonb_array_elements(v_guard_items) item
      where item->>'candidate_submission_stage'='MANAGER_APPROVED'
    ) then 'MANAGER_APPROVED'
    when jsonb_array_length(v_guard_items)>0 then 'CANDIDATE_SUBMITTED'
    else null
  end;
  v_linked_pending_expense_count:=cardinality(
    v_linked_pending_expense_workflow_ids
  );

  return jsonb_build_object(
    'ok',true,
    'contract_version','TIMESHEET_CANDIDATE_SUBMISSION_DELETE_GUARD_V1',
    'environment',v_environment,
    'timesheet_ids',to_jsonb(v_timesheet_ids),
    'candidate_submission_rejection_required',jsonb_array_length(v_guard_items)>0,
    'candidate_submission_stage',v_stage,
    'guarded_workflow_count',jsonb_array_length(v_guard_items),
    'guarded_workflows',v_guard_items,
    'related_workflow_ids',to_jsonb(v_all_workflow_ids),
    'linked_pending_expense_claim_count',v_linked_pending_expense_count
  );
end;
$function$;

create or replace function public.timesheet_candidate_submission_delete_guard_preview_v1(
  p_environment text,
  p_timesheet_ids uuid[]
)
returns jsonb
language sql
volatile
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
  select private._timesheet_candidate_submission_delete_guard_v1(
    p_environment,p_timesheet_ids,false
  )
$function$;

-- Once the rejection/delete guard has proved that no submitted or approved
-- Candidate workflow remains, release only terminal/draft weekly workflow
-- links that would otherwise retain the Timesheet or Contract Week. The
-- workflow, components and notifications remain as immutable audit history.
create or replace function private._candidate_timesheet_delete_retire_workflows_v1(
  p_environment text,
  p_timesheet_ids uuid[],
  p_contract_week_ids uuid[],
  p_excluded_workflow_ids uuid[],
  p_actor_user_id uuid,
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
  v_timesheet_ids uuid[];
  v_contract_week_ids uuid[];
  v_excluded_workflow_ids uuid[];
  v_workflow public.candidate_submission_workflows%rowtype;
  v_expense_component public.candidate_expense_components%rowtype;
  v_expense_before jsonb;
  v_retired_workflow_ids uuid[]:=array[]::uuid[];
  v_previous_state text;
begin
  select coalesce(array_agg(distinct supplied_id order by supplied_id),array[]::uuid[])
  into v_timesheet_ids
  from unnest(coalesce(p_timesheet_ids,array[]::uuid[])) supplied(supplied_id)
  where supplied_id is not null;
  select coalesce(array_agg(distinct supplied_id order by supplied_id),array[]::uuid[])
  into v_contract_week_ids
  from unnest(coalesce(p_contract_week_ids,array[]::uuid[])) supplied(supplied_id)
  where supplied_id is not null;
  select coalesce(array_agg(distinct supplied_id order by supplied_id),array[]::uuid[])
  into v_excluded_workflow_ids
  from unnest(coalesce(p_excluded_workflow_ids,array[]::uuid[])) supplied(supplied_id)
  where supplied_id is not null;

  if p_actor_user_id is null or p_delete_operation_id is null
     or cardinality(v_timesheet_ids)<1 or cardinality(v_timesheet_ids)>64
     or cardinality(v_contract_week_ids)>64
     or cardinality(v_excluded_workflow_ids)>20 then
    raise exception 'CANDIDATE_TIMESHEET_DELETE_RETIREMENT_INVALID'
      using errcode='22023';
  end if;

  for v_workflow in
    select workflow_row.*
    from public.candidate_submission_workflows workflow_row
    where workflow_row.environment=v_environment
      and workflow_row.workflow_kind in (
        'CONTRACT_HOURS','CONTRACT_EXPENSE','CONTRACT_COMBINED'
      )
      and not (workflow_row.id=any(v_excluded_workflow_ids))
      and (
        workflow_row.target_timesheet_id=any(v_timesheet_ids)
        or workflow_row.anchor_timesheet_id=any(v_timesheet_ids)
        or workflow_row.contract_week_id=any(v_contract_week_ids)
      )
    order by workflow_row.id
    for update
  loop
    if v_workflow.state not in (
      'CREATED','WORKER_DRAFT','REFUSED','REJECTED','CANCELLED','EXPIRED','SUPERSEDED'
    ) then
      raise exception 'CANDIDATE_SUBMISSION_REJECTION_REQUIRED'
        using errcode='55000',detail=jsonb_build_object(
          'workflow_id',v_workflow.id,
          'workflow_state',v_workflow.state
        )::text;
    end if;
    v_previous_state:=v_workflow.state;

    update public.candidate_approval_requests approval
    set state='CANCELLED',
        cancelled_at_utc=coalesce(approval.cancelled_at_utc,p_now_utc),
        updated_at_utc=p_now_utc
    where approval.workflow_id=v_workflow.id
      and approval.state='PENDING';

    -- The stable category ledger must be retired before the owning Timesheet
    -- FK is released.  Otherwise a draft component can survive the generic
    -- Office delete with no owner and still project a Candidate Remove action.
    -- Only components proved to belong to this deletion unit (or an unowned
    -- component on the exact deleted workflow anchor) are affected.
    for v_expense_component in
      select component.*
      from public.candidate_expense_components component
      where component.workflow_id=v_workflow.id
        and component.lifecycle_state not in (
          'MANAGER_REFUSED','OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED'
        )
        and (
          component.owning_timesheet_id=any(v_timesheet_ids)
          or (
            component.owning_timesheet_id is null
            and (
              v_workflow.target_timesheet_id=any(v_timesheet_ids)
              or v_workflow.anchor_timesheet_id=any(v_timesheet_ids)
            )
          )
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
          else 'NOT_REQUESTED' end,
        approval_request_id=case
          when component.manager_approval_state in ('APPROVED','REFUSED')
            then component.approval_request_id
          else null end,
        removed_at_utc=coalesce(component.removed_at_utc,p_now_utc),
        updated_at_utc=p_now_utc
      where component.expense_component_id=v_expense_component.expense_component_id
      returning component.* into v_expense_component;
      insert into public.candidate_expense_component_events(
        expense_component_id,workflow_id,component_generation,event_type,
        actor_kind,actor_id,before_state_json,after_state_json,idempotency_key,
        occurred_at_utc
      ) values (
        v_expense_component.expense_component_id,v_workflow.id,
        v_expense_component.component_generation,'CANCELLED','OFFICE',
        p_actor_user_id,v_expense_before,to_jsonb(v_expense_component),
        'office-timesheet-delete:'||p_delete_operation_id::text||':'
          ||v_expense_component.expense_component_id::text,p_now_utc
      ) on conflict(expense_component_id,idempotency_key) do nothing;
    end loop;

    update public.candidate_submission_components component
    set timesheet_id=null,
        state=case
          when v_previous_state in ('CREATED','WORKER_DRAFT')
            and component.state not in ('SUPERSEDED','REJECTED','ABANDONED')
            then 'ABANDONED'
          else component.state
        end,
        superseded_at_utc=case
          when v_previous_state in ('CREATED','WORKER_DRAFT')
            then coalesce(component.superseded_at_utc,p_now_utc)
          else component.superseded_at_utc
        end
    where component.workflow_id=v_workflow.id
      and component.timesheet_id=any(v_timesheet_ids);

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
          else notification.push_state end
    where notification.workflow_id=v_workflow.id;

    update public.candidate_submission_workflows workflow_row
    set state=case
          when workflow_row.state in ('CREATED','WORKER_DRAFT')
            then 'CANCELLED'
          else workflow_row.state
        end,
        contract_week_id=null,
        anchor_timesheet_id=null,
        target_timesheet_id=null,
        cancelled_at_utc=case
          when workflow_row.state in ('CREATED','WORKER_DRAFT')
            then coalesce(workflow_row.cancelled_at_utc,p_now_utc)
          else workflow_row.cancelled_at_utc
        end,
        input_snapshot_json=coalesce(workflow_row.input_snapshot_json,'{}'::jsonb)
          ||jsonb_build_object(
            'office_permanent_delete_tombstone',jsonb_build_object(
              'delete_operation_id',p_delete_operation_id,
              'deleted_timesheet_ids',to_jsonb(v_timesheet_ids),
              'deleted_contract_week_ids',to_jsonb(v_contract_week_ids),
              'previous_contract_week_id',v_workflow.contract_week_id,
              'previous_anchor_timesheet_id',v_workflow.anchor_timesheet_id,
              'previous_target_timesheet_id',v_workflow.target_timesheet_id,
              'retired_at_utc',p_now_utc
            )
          ),
        issue_codes=case
          when workflow_row.issue_codes
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
      'CANDIDATE_WORKFLOW_RETAINED_AFTER_TIMESHEET_DELETE',
      jsonb_build_object(
        'state',v_previous_state,
        'contract_week_id',v_workflow.contract_week_id,
        'anchor_timesheet_id',v_workflow.anchor_timesheet_id,
        'target_timesheet_id',v_workflow.target_timesheet_id
      ),
      jsonb_build_object(
        'state',case when v_previous_state in ('CREATED','WORKER_DRAFT')
          then 'CANCELLED' else v_previous_state end,
        'live_timesheet_links_released',true,
        'terminal_audit_retained',true
      ),
      'OFFICE_PERMANENTLY_DELETED_TIMESHEET',p_delete_operation_id,p_now_utc
    );
    v_retired_workflow_ids:=array_append(
      v_retired_workflow_ids,v_workflow.id
    );
  end loop;

  return jsonb_build_object(
    'ok',true,
    'contract_version','CANDIDATE_TIMESHEET_DELETE_WORKFLOW_RETIREMENT_V1',
    'retired_workflow_count',cardinality(v_retired_workflow_ids),
    'retired_workflow_ids',to_jsonb(v_retired_workflow_ids)
  );
end;
$function$;

-- This wrapper is the final race-safe delete gate.  It locks the removal unit
-- and its Contract Weeks, locks every related Candidate workflow, refuses a
-- submitted/manager-approved Candidate record, and only then enters the
-- established pending-expense-aware delete transaction.
create or replace function public.timesheet_delete_with_candidate_submission_guard_apply_v1(
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
  v_guard jsonb;
  v_family record;
  v_related_workflow_ids uuid[]:=array[]::uuid[];
  v_pending_context jsonb;
  v_pending_workflow_ids uuid[]:=array[]::uuid[];
  v_workflow_retirement jsonb;
  v_apply_result jsonb;
  v_signature_contract_week_id uuid;
  v_signature_result jsonb;
  v_pre_retirement_row_signature text;
  v_post_retirement_row_signature text;
begin
  -- Rejections and PAPER changes already use this family lock.  Take it before
  -- any row lock so delete and reject cannot acquire the same rows in reverse.
  for v_family in
    select distinct timesheet_row.contract_id,timesheet_row.week_ending_date
    from public.timesheets timesheet_row
    where timesheet_row.timesheet_id=any(
      coalesce(p_expected_timesheet_ids,array[]::uuid[])
    )
    order by timesheet_row.contract_id,timesheet_row.week_ending_date
  loop
    perform pg_advisory_xact_lock(hashtextextended(
      'CANDIDATE_PAPER_FAMILY:'||v_environment||':'
        ||coalesce(v_family.contract_id::text,'-')||':'
        ||coalesce(v_family.week_ending_date::text,'-'),0
    ));
  end loop;

  -- Existing Candidate transitions lock their workflow first.  Follow that
  -- order here, then re-read under all locks after the removal unit is locked.
  v_guard:=private._timesheet_candidate_submission_delete_guard_v1(
    v_environment,p_expected_timesheet_ids,false
  );
  select coalesce(
    array_agg(value::uuid order by value),array[]::uuid[]
  ) into v_related_workflow_ids
  from jsonb_array_elements_text(coalesce(
    v_guard->'related_workflow_ids','[]'::jsonb
  )) values_json(value);
  select coalesce(array_agg(distinct workflow_id order by workflow_id),array[]::uuid[])
  into v_related_workflow_ids
  from (
    select unnest(v_related_workflow_ids) as workflow_id
    union all
    select workflow_row.id
    from public.candidate_submission_workflows workflow_row
    where workflow_row.environment=v_environment
      and (
        workflow_row.target_timesheet_id=any(
          coalesce(p_expected_timesheet_ids,array[]::uuid[])
        )
        or workflow_row.anchor_timesheet_id=any(
          coalesce(p_expected_timesheet_ids,array[]::uuid[])
        )
        or workflow_row.contract_week_id=any(
          coalesce(p_expected_contract_week_ids,array[]::uuid[])
        )
      )
  ) related(workflow_id);
  if cardinality(v_related_workflow_ids)>0 then
    perform 1
    from public.candidate_submission_workflows workflow_row
    where workflow_row.id=any(v_related_workflow_ids)
    order by workflow_row.id
    for update;
  end if;

  perform 1
  from public.contract_weeks week_row
  where week_row.id=any(coalesce(p_expected_contract_week_ids,array[]::uuid[]))
  order by week_row.id
  for update;

  perform 1
  from public.timesheets timesheet_row
  where timesheet_row.timesheet_id=any(coalesce(p_expected_timesheet_ids,array[]::uuid[]))
  order by timesheet_row.timesheet_id
  for update;

  v_guard:=private._timesheet_candidate_submission_delete_guard_v1(
    v_environment,p_expected_timesheet_ids,true
  );
  if coalesce((v_guard->>'candidate_submission_rejection_required')::boolean,false) then
    raise exception 'CANDIDATE_SUBMISSION_REJECTION_REQUIRED'
      using errcode='55000',detail=v_guard::text;
  end if;

  -- Prove the caller's exact preview while all removal-unit and Candidate
  -- workflow locks are held. Releasing a retained workflow/component link can
  -- legitimately advance the server-owned summary revision, so the established
  -- delete must receive a fresh signature after that internal audit-only step.
  select week_row.id
  into v_signature_contract_week_id
  from public.contract_weeks week_row
  where week_row.timesheet_id=p_expected_timesheet_id
  order by week_row.updated_at desc,week_row.id desc
  limit 1;
  v_signature_result:=public.timesheet_lifecycle_guard_signature_v1(
    p_expected_timesheet_id,v_signature_contract_week_id,false
  );
  v_pre_retirement_row_signature:=coalesce(
    v_signature_result->>'backend_row_signature',
    v_signature_result->>'row_signature'
  );
  if v_pre_retirement_row_signature is null
     or lower(v_pre_retirement_row_signature)
       is distinct from lower(p_expected_row_signature) then
    raise exception 'ROW_SIGNATURE_MISMATCH'
      using errcode='40001',detail=jsonb_build_object(
        'expected_timesheet_id',p_expected_timesheet_id,
        'expected_row_signature',p_expected_row_signature,
        'current_row_signature',v_pre_retirement_row_signature
      )::text;
  end if;

  v_pending_context:=private._timesheet_pending_expense_delete_context_v1(
    v_environment,p_expected_timesheet_ids,false
  );
  select coalesce(array_agg(
    (item->>'workflow_id')::uuid order by (item->>'workflow_id')::uuid
  ),array[]::uuid[])
  into v_pending_workflow_ids
  from jsonb_array_elements(coalesce(
    v_pending_context->'pending_expense_claims','[]'::jsonb
  )) pending(item);

  v_workflow_retirement:=private._candidate_timesheet_delete_retire_workflows_v1(
    v_environment,p_expected_timesheet_ids,p_expected_contract_week_ids,
    v_pending_workflow_ids,p_actor_user_id,p_delete_operation_id,p_now_utc
  );

  v_signature_result:=public.timesheet_lifecycle_guard_signature_v1(
    p_expected_timesheet_id,v_signature_contract_week_id,false
  );
  v_post_retirement_row_signature:=coalesce(
    v_signature_result->>'backend_row_signature',
    v_signature_result->>'row_signature'
  );
  if v_post_retirement_row_signature is null then
    raise exception 'ROW_SIGNATURE_MISMATCH'
      using errcode='40001',detail=jsonb_build_object(
        'expected_timesheet_id',p_expected_timesheet_id,
        'expected_row_signature',p_expected_row_signature,
        'current_row_signature',v_post_retirement_row_signature
      )::text;
  end if;

  v_apply_result:=public.timesheet_delete_with_pending_expense_apply_v1(
    v_environment,p_delete_kind,p_timesheet_id,p_actor_user_id,
    p_expected_timesheet_id,v_post_retirement_row_signature,p_expected_timesheet_ids,
    p_expected_contract_week_ids,p_expected_nhsp_shift_ids,
    p_expected_preserved_source_timesheet_ids,
    p_expected_preserved_source_contract_week_ids,
    p_expected_pending_expense_context_sha256,p_delete_operation_id,p_now_utc
  );
  return v_apply_result||jsonb_build_object(
    'candidate_workflow_retirement',v_workflow_retirement
  );
end;
$function$;

create or replace function public.candidate_submission_reject_atomic_v1(
  p_actor_user_id uuid,
  p_environment text,
  p_timesheet_id uuid,
  p_expected_timesheet_id uuid,
  p_expected_row_signature text,
  p_reason text,
  p_idempotency_key text,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_environment text;
  v_timesheet public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_week public.contract_weeks%rowtype;
  v_signature jsonb;
  v_capabilities jsonb;
  v_qr_result record;
  v_new_timesheet_id uuid;
  v_qr_backed boolean:=false;
  v_reject_scope text;
  v_target_context jsonb;
  v_rejection_target_workflow_ids uuid[]:='{}'::uuid[];
  v_linked_pending_expense_count integer:=0;
  v_workflow record;
  v_rejected_workflow_ids uuid[]:='{}'::uuid[];
  v_paper_workflow_ids uuid[]:='{}'::uuid[];
  v_paper_workflow_generations integer[]:='{}'::integer[];
  v_paper_retirement_result jsonb;
  v_rejection_family_contract_id uuid;
  v_rejection_family_week_ending_date date;
  v_rejection_family_key text;
  v_rejection_request_hash text;
  v_rejection_receipt_before jsonb;
  v_response jsonb;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  if not private._candidate_office_service_context_valid_v1(
    v_environment,p_actor_user_id,'REJECT_CONFIRM'
  ) then
    perform private._candidate_require_feature_v1(v_environment,'candidate_app_writes');
  end if;
  if p_actor_user_id is null or p_timesheet_id is null or p_expected_timesheet_id is null
     or nullif(btrim(coalesce(p_expected_row_signature,'')),'') is null
     or nullif(btrim(coalesce(p_reason,'')),'') is null
     or nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then
    raise exception 'CANDIDATE_REJECT_PAYLOAD_INVALID' using errcode='22023';
  end if;
  perform pg_advisory_xact_lock(hashtextextended(
    'CANDIDATE_REJECTION_IDEMPOTENCY:'||p_actor_user_id::text||':'||p_idempotency_key,0
  ));
  -- Serialise every Candidate rejection for the same contract/week before
  -- locking either the hours row or a separate expense carrier. This prevents
  -- the inverse target/workflow/source lock order that can deadlock H1/E1.
  select target.contract_id,target.week_ending_date
  into v_rejection_family_contract_id,v_rejection_family_week_ending_date
  from public.timesheets target
  where target.timesheet_id=p_timesheet_id;
  if not found then
    raise exception 'CANDIDATE_REJECT_TARGET_NOT_FOUND' using errcode='P0002';
  end if;
  v_rejection_family_key:='CANDIDATE_PAPER_FAMILY:'||v_environment||':'
    ||coalesce(v_rejection_family_contract_id::text,'-')||':'
    ||coalesce(v_rejection_family_week_ending_date::text,'-');
  perform pg_advisory_xact_lock(hashtextextended(v_rejection_family_key,0));

  v_rejection_request_hash:=encode(extensions.digest(convert_to(jsonb_build_object(
    'contract_version','CANDIDATE_REJECTION_REQUEST_V2',
    'environment',v_environment,
    'actor_user_id',p_actor_user_id,
    'timesheet_id',p_timesheet_id,
    'expected_timesheet_id',p_expected_timesheet_id,
    'expected_row_signature',btrim(p_expected_row_signature),
    'reason',btrim(p_reason)
  )::text,'UTF8'),'sha256'),'hex');
  select ae.before_json,ae.after_json into v_rejection_receipt_before,v_response
  from public.audit_events ae
  where ae.object_type='candidate_submission_rejection_receipt'
    and ae.actor_user_id=p_actor_user_id
    and ae.correlation_id=p_idempotency_key
  order by ae.ts_utc desc,ae.id desc
  limit 1;
  if found then
    if v_rejection_receipt_before->>'request_sha256' is distinct from v_rejection_request_hash then
      raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT'
        using errcode='40001',detail=jsonb_build_object(
          'code','CANDIDATE_IDEMPOTENCY_CONFLICT','idempotency_key',p_idempotency_key
        )::text;
    end if;
    return coalesce(v_response,'{}'::jsonb)||jsonb_build_object('idempotent_replay',true);
  end if;

  select * into v_timesheet from public.timesheets where timesheet_id=p_timesheet_id for update;
  if not found then raise exception 'CANDIDATE_REJECT_TARGET_NOT_FOUND' using errcode='P0002'; end if;
  if not v_timesheet.is_current or v_timesheet.timesheet_id<>p_expected_timesheet_id then
    raise exception 'TIMESHEET_MOVED' using errcode='40001';
  end if;
  select * into v_week from public.contract_weeks where timesheet_id=v_timesheet.timesheet_id for update;
  if not found and v_timesheet.sheet_scope<>'DAILY'::public.timesheet_scope_enum then
    raise exception 'CANDIDATE_CONTRACT_WEEK_NOT_FOUND' using errcode='P0002';
  end if;
  select * into v_fin from public.timesheets_financials
  where timesheet_id=v_timesheet.timesheet_id and is_current=true for update;
  if v_fin.authorised_at_utc is not null or v_timesheet.authorised_at_server is not null then
    raise exception 'CANDIDATE_REJECT_REQUIRES_UNAUTHORISE' using errcode='55000';
  end if;
  if v_fin.paid_at_utc is not null or v_fin.locked_by_invoice_id is not null then
    raise exception 'CANDIDATE_REJECT_PROTECTED_HISTORY' using errcode='55000';
  end if;
  v_signature:=public.timesheet_lifecycle_guard_signature_v1(v_timesheet.timesheet_id,v_week.id,false);
  if coalesce(v_signature->>'row_signature',v_signature->>'backend_row_signature','')<>p_expected_row_signature then
    raise exception 'ROW_SIGNATURE_MISMATCH' using errcode='40001';
  end if;
  v_capabilities:=private._candidate_record_capabilities_v1(v_timesheet.timesheet_id,v_week.id,'{}'::jsonb);
  if coalesce((v_capabilities->>'can_reject_candidate_submission')::boolean,false)=false then
    raise exception 'CANDIDATE_REJECT_NOT_ALLOWED' using errcode='55000',detail=v_capabilities::text;
  end if;
  v_reject_scope:=v_capabilities->>'reject_scope';
  v_target_context:=private._candidate_office_rejection_targets_v2(
    v_environment,v_timesheet.timesheet_id
  );
  select
    coalesce(
      array_agg((target->>'workflow_id')::uuid order by target->>'workflow_id'),
      array[]::uuid[]
    ),
    count(*) filter(
      where coalesce((target->>'linked_pending_expense')::boolean,false)
    )::integer
  into v_rejection_target_workflow_ids,v_linked_pending_expense_count
  from jsonb_array_elements(
    coalesce(v_target_context->'target_workflows','[]'::jsonb)
  ) target;

  if cardinality(v_rejection_target_workflow_ids)=0 then
    raise exception 'CANDIDATE_REJECTION_SCOPE_CONFLICT' using errcode='55000';
  end if;

  v_qr_backed:=v_timesheet.qr_status is not null
    or v_timesheet.qr_token is not null
    or v_timesheet.qr_r2_key is not null
    or exists(
      select 1
      from public.candidate_submission_workflows workflow_row
      where workflow_row.id=any(v_rejection_target_workflow_ids)
        and workflow_row.route='PAPER'
    );

  for v_workflow in
    select w.id,w.generation,w.route,w.state
    from public.candidate_submission_workflows w
    where w.id=any(v_rejection_target_workflow_ids)
    order by w.id
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
    v_paper_retirement_result:=private._candidate_paper_delivery_retire_set_v1(
      v_paper_workflow_ids,v_paper_workflow_generations,
      'OFFICE_REJECTED',p_now_utc
    );
    if not coalesce((v_paper_retirement_result->>'retired')::boolean,false)
       or not coalesce(
         (v_paper_retirement_result->>'qr_invalidation_proven')::boolean,false
       ) then
      raise exception 'CANDIDATE_PAPER_QR_INVALIDATION_NOT_PROVEN'
        using errcode='40001',detail=jsonb_build_object(
          'code','CANDIDATE_PAPER_QR_INVALIDATION_NOT_PROVEN',
          'workflow_ids',to_jsonb(v_paper_workflow_ids),
          'retirement_receipt',v_paper_retirement_result
        )::text;
    end if;
  end if;

  if v_timesheet.sheet_scope='DAILY'::public.timesheet_scope_enum then
    if v_fin.id is null and v_timesheet.contract_id is null then
      v_new_timesheet_id:=nullif(
        private._candidate_daily_receipt_reset_v1(
          v_environment,(v_capabilities->>'candidate_id')::uuid,
          v_timesheet.timesheet_id,v_timesheet.timesheet_id,btrim(p_reason),
          p_actor_user_id,'OFFICE_REJECTED',p_now_utc
        )->>'current_timesheet_id',''
      )::uuid;
    else
      -- Existing financial/history reset owner is deliberately unchanged.
      v_new_timesheet_id:=nullif(
        private._candidate_daily_submission_reset_v1(
          v_timesheet.timesheet_id,v_timesheet.timesheet_id,btrim(p_reason),
          p_actor_user_id,'OFFICE_REJECTED',p_now_utc
        )->>'current_timesheet_id',''
      )::uuid;
    end if;
  elsif v_qr_backed then
    select * into v_qr_result from public.timesheet_qr_refuse_and_reset(
      v_timesheet.timesheet_id,v_timesheet.timesheet_id,btrim(p_reason),p_actor_user_id
    );
    v_new_timesheet_id:=v_qr_result.timesheet_id;
  else
    v_new_timesheet_id:=private._candidate_timesheet_reject_rotate_v1(
      v_timesheet.timesheet_id,v_timesheet.timesheet_id,btrim(p_reason),p_actor_user_id,p_now_utc);
  end if;
  if v_new_timesheet_id is null then raise exception 'CANDIDATE_REJECT_ROTATION_FAILED' using errcode='55000'; end if;

  update public.timesheets set
    authorised_at_server=null,worked_start_iso=null,worked_end_iso=null,break_start_iso=null,break_end_iso=null,
    break_minutes=null,actual_schedule_json=null,additional_units_week='{}'::jsonb,additional_units_per_day='{}'::jsonb,
    manual_pdf_r2_key=null,reference_number=null,day_references_json=null,
    qr_token=null,qr_status=case when v_qr_backed then 'PENDING'::public.timesheet_qr_status_enum else null end,
    qr_payload_json='{}'::jsonb,qr_generated_at=null,qr_scanned_at=null,qr_scan_info_json=null,qr_r2_key=null,
    qr_last_sent_hash=null,qr_last_sent_at_utc=null,qr_signed_hash=null,qr_signed_at_utc=null,
    updated_at=p_now_utc
  where timesheet_id=v_new_timesheet_id;

  update public.timesheets_financials set
    processing_status=case when v_timesheet.sheet_scope='DAILY'::public.timesheet_scope_enum
      then 'UNASSIGNED'::public.ts_fin_processing_status_enum
      else 'UNPROCESSED'::public.ts_fin_processing_status_enum end,
    worked_start_iso=null,worked_end_iso=null,break_start_iso=null,break_end_iso=null,
    break_minutes=null,hours_day=0,hours_night=0,hours_sat=0,hours_sun=0,hours_bh=0,total_hours=0,
    additional_units_json='{}'::jsonb,additional_pay_ex_vat=0,additional_charge_ex_vat=0,additional_margin_ex_vat=0,
    expenses_pay_ex_vat=0,expenses_charge_ex_vat=0,expenses_description=null,expenses_evidence_r2_key=null,
    expenses_evidence_manifest=null,mileage_units=0,mileage_pay_ex_vat=0,mileage_charge_ex_vat=0,
    mileage_evidence_r2_key=null,mileage_evidence_manifest=null,travel_pay_ex_vat=0,travel_charge_ex_vat=0,
    accommodation_pay_ex_vat=0,accommodation_charge_ex_vat=0,other_pay_ex_vat=0,other_charge_ex_vat=0,
    actual_schedule_json=null,actual_minutes_by_day_json=null,total_pay_ex_vat=0,total_charge_ex_vat=0,margin_ex_vat=0,
    authorised_at_utc=null,authorised_by_user_id=null,updated_at=p_now_utc
  where timesheet_id=v_new_timesheet_id and is_current=true;

  if v_week.id is not null then
    update public.contract_weeks set status='OPEN',day_entries_json='[]'::jsonb,
      totals_json='{}'::jsonb,updated_at=p_now_utc where id=v_week.id;
  end if;
  update public.timesheet_evidence set processing_state='SUPERSEDED'
  where timesheet_id=v_timesheet.timesheet_id and processing_state<>'SUPERSEDED';
  for v_workflow in
    select w.id,w.account_id,w.candidate_id,w.generation,w.state as captured_state,
      w.workflow_kind,
      coalesce((
        select (target->>'linked_pending_expense')::boolean
        from jsonb_array_elements(
          coalesce(v_target_context->'target_workflows','[]'::jsonb)
        ) target
        where target->>'workflow_id'=w.id::text
        limit 1
      ),false) as linked_pending_expense,
      case when w.state='FINALISED' then greatest(w.generation-1,1) else w.generation end as artifact_generation
    from public.candidate_submission_workflows w
    where w.id=any(v_rejected_workflow_ids)
    order by w.id
    for update
  loop
    update public.candidate_approval_requests set
      state='SUPERSEDED',superseded_at_utc=p_now_utc,updated_at_utc=p_now_utc
    where workflow_id=v_workflow.id
      and workflow_generation=v_workflow.artifact_generation
      and state in ('PENDING','APPROVED');
    update public.candidate_submission_components set
      state='REJECTED',superseded_at_utc=p_now_utc
    where workflow_id=v_workflow.id
      and workflow_generation=v_workflow.artifact_generation
      and state not in ('REJECTED','SUPERSEDED','ABANDONED');
    update public.candidate_submission_workflows set
      state='REJECTED',generation=v_workflow.generation+1,
      rejection_reason=btrim(p_reason),
      rejection_scope=case when v_workflow.workflow_kind='CONTRACT_EXPENSE'
        then 'COMPLETE_EXPENSE_CLAIM' else v_reject_scope end,
      updated_at_utc=p_now_utc
    where id=v_workflow.id and generation=v_workflow.generation
      and state=v_workflow.captured_state
      and state not in ('REFUSED','REJECTED','CANCELLED','EXPIRED','SUPERSEDED');
    if not found then
      raise exception 'CANDIDATE_REJECT_WORKFLOW_CONFLICT' using errcode='40001';
    end if;
    perform private._candidate_notification_insert_v1(
      v_workflow.account_id,v_workflow.candidate_id,v_workflow.id,
      case when v_workflow.linked_pending_expense
        then null else v_new_timesheet_id end,
      case when v_workflow.linked_pending_expense
        then 'EXPENSE_CLAIM_CANCELLED' else 'OFFICE_REJECTED' end,
      case when v_workflow.linked_pending_expense
        then 'timesheet_expense_attention' else 'office_rejection' end,
      case when v_workflow.linked_pending_expense
        then 'candidate-expense-claim-cancelled-timesheet-rejection-v1'
        else 'candidate-office-rejected-v1' end,
      jsonb_build_object(
        'reason',btrim(p_reason),
        'reason_code',case when v_workflow.linked_pending_expense
          then 'LINKED_TIMESHEET_REJECTED_FOR_DELETE'
          else 'OFFICE_REJECTED' end,
        'workflow_id',v_workflow.id,
        'resubmission_scope',case when v_workflow.workflow_kind='CONTRACT_EXPENSE'
          then 'COMPLETE_EXPENSE_CLAIM' else v_reject_scope end
      ),
      case when v_workflow.linked_pending_expense
        then jsonb_build_object('type','workflow','workflow_id',v_workflow.id)
        else jsonb_build_object('type','timesheet','timesheet_id',v_new_timesheet_id) end,
      case when v_workflow.linked_pending_expense
        then 'CANDIDATE_EXPENSE_CLAIM_CANCELLED_TIMESHEET_REJECTION_V1:'
        else 'CANDIDATE_OFFICE_REJECTED_V1:' end
        ||v_workflow.id::text||':'||(v_workflow.generation+1)::text,
      p_now_utc
    );
  end loop;
  v_response:=jsonb_build_object(
    'ok',true,'old_timesheet_id',v_timesheet.timesheet_id,'timesheet_id',v_new_timesheet_id,
    'scope',case when v_timesheet.sheet_scope='DAILY'::public.timesheet_scope_enum then 'DAILY' else 'WEEKLY' end,
    'contract_week_id',v_week.id,
    'contract_week_status',case when v_week.id is null then null else 'OPEN' end,
    'processing_status',case when v_week.id is null then 'UNASSIGNED' else 'UNPROCESSED' end,
    'rejection_scope',v_reject_scope,'qr_reissue_required',v_qr_backed,
    'paper_retirement_receipt',v_paper_retirement_result,
    'rejected_workflow_ids',to_jsonb(v_rejected_workflow_ids),
    'linked_pending_expense_claim_count',v_linked_pending_expense_count,
    'idempotency_key',p_idempotency_key
  );
  perform private._candidate_audit_v1('timesheet',v_new_timesheet_id::text,'CANDIDATE_SUBMISSION_REJECTED',
    jsonb_build_object('old_timesheet_id',v_timesheet.timesheet_id),v_response,btrim(p_reason),
    p_actor_user_id,p_idempotency_key,p_now_utc);
  insert into public.audit_events(
    actor_user_id,object_type,object_id_text,action,before_json,after_json,
    reason,correlation_id,ts_utc
  ) values (
    p_actor_user_id,'candidate_submission_rejection_receipt',p_timesheet_id::text,
    'CANDIDATE_SUBMISSION_REJECTION_RECEIPT',jsonb_build_object(
      'request_sha256',v_rejection_request_hash,'contract_version','CANDIDATE_REJECTION_REQUEST_V2'
    ),v_response,btrim(p_reason),p_idempotency_key,p_now_utc
  );
  return v_response;
end;
$function$;

alter function private._candidate_office_rejection_targets_v2(text,uuid) owner to postgres;
alter function private._candidate_office_reject_preview_v1(text,uuid,uuid,timestamptz) owner to postgres;
alter function private._timesheet_candidate_submission_delete_guard_v1(text,uuid[],boolean) owner to postgres;
alter function private._candidate_timesheet_delete_retire_workflows_v1(
  text,uuid[],uuid[],uuid[],uuid,uuid,timestamptz
) owner to postgres;
alter function public.timesheet_candidate_submission_delete_guard_preview_v1(text,uuid[]) owner to postgres;
alter function public.timesheet_delete_with_candidate_submission_guard_apply_v1(
  text,text,uuid,uuid,uuid,text,uuid[],uuid[],uuid[],uuid[],uuid[],text,uuid,timestamptz
) owner to postgres;
alter function public.candidate_submission_reject_atomic_v1(
  uuid,text,uuid,uuid,text,text,text,timestamptz
) owner to postgres;

revoke all on function private._candidate_office_rejection_targets_v2(text,uuid)
  from public,anon,authenticated,service_role;
revoke all on function private._candidate_office_reject_preview_v1(text,uuid,uuid,timestamptz)
  from public,anon,authenticated,service_role;
revoke all on function private._timesheet_candidate_submission_delete_guard_v1(text,uuid[],boolean)
  from public,anon,authenticated,service_role;
revoke all on function private._candidate_timesheet_delete_retire_workflows_v1(
  text,uuid[],uuid[],uuid[],uuid,uuid,timestamptz
) from public,anon,authenticated,service_role;
revoke all on function public.timesheet_candidate_submission_delete_guard_preview_v1(text,uuid[])
  from public,anon,authenticated;
revoke all on function public.timesheet_delete_with_candidate_submission_guard_apply_v1(
  text,text,uuid,uuid,uuid,text,uuid[],uuid[],uuid[],uuid[],uuid[],text,uuid,timestamptz
) from public,anon,authenticated;
revoke all on function public.candidate_submission_reject_atomic_v1(
  uuid,text,uuid,uuid,text,text,text,timestamptz
) from public,anon,authenticated;

grant execute on function public.timesheet_candidate_submission_delete_guard_preview_v1(text,uuid[])
  to service_role;
grant execute on function public.timesheet_delete_with_candidate_submission_guard_apply_v1(
  text,text,uuid,uuid,uuid,text,uuid[],uuid[],uuid[],uuid[],uuid[],text,uuid,timestamptz
) to service_role;
grant execute on function public.candidate_submission_reject_atomic_v1(
  uuid,text,uuid,uuid,text,text,text,timestamptz
) to service_role;

comment on function public.timesheet_candidate_submission_delete_guard_preview_v1(text,uuid[]) is
  'Refuses permanent deletion of Electronic/QR Candidate-submitted or manager-approved Timesheets, including Candidate expense carriers and linked target-less expense claims.';
comment on function public.timesheet_delete_with_candidate_submission_guard_apply_v1(
  text,text,uuid,uuid,uuid,text,uuid[],uuid[],uuid[],uuid[],uuid[],text,uuid,timestamptz
) is
  'Atomically requires Candidate rejection before permanent delete, preserves terminal Candidate history without live Timesheet links, then delegates to the established pending-expense-aware Timesheet delete transaction.';

notify pgrst, 'reload schema';

commit;
