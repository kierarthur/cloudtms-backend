-- Repeatable CloudTMS function/view authority: candidate_daily_detail_projection_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

create or replace function private._candidate_workflow_maps_to_card_v1(
  p_workflow_id uuid,
  p_timesheet_id uuid,
  p_contract_week_id uuid
)
returns boolean
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_workflow public.candidate_submission_workflows%rowtype;
  v_card_timesheet public.timesheets%rowtype;
  v_anchor public.timesheets%rowtype;
  v_target public.timesheets%rowtype;
  v_parent public.timesheets%rowtype;
begin
  if p_workflow_id is null then return false; end if;
  select * into v_workflow from public.candidate_submission_workflows where id=p_workflow_id;
  if not found then return false; end if;
  if p_timesheet_id is not null and v_workflow.workflow_kind='DAILY' then
    select * into v_card_timesheet from public.timesheets
      where timesheet_id=p_timesheet_id and sheet_scope='DAILY';
    if not found then return false; end if;
    return exists(select 1 from public.timesheets origin
      where origin.timesheet_id in (v_workflow.anchor_timesheet_id,v_workflow.target_timesheet_id)
        and origin.sheet_scope='DAILY' and origin.booking_id=v_card_timesheet.booking_id
        and nullif(btrim(origin.booking_id),'') is not null
        and origin.week_ending_date=v_card_timesheet.week_ending_date);
  end if;
  if p_contract_week_id is null then return false; end if;
  if v_workflow.contract_week_id=p_contract_week_id then return true; end if;
  if p_timesheet_id is null
     or not (v_workflow.workflow_kind='CONTRACT_EXPENSE'
       or v_workflow.rejection_scope='COMPLETE_EXPENSE_CLAIM') then return false; end if;
  select * into v_card_timesheet from public.timesheets where timesheet_id=p_timesheet_id;
  if not found then return false; end if;
  if v_workflow.anchor_timesheet_id is not null then
    select * into v_anchor from public.timesheets where timesheet_id=v_workflow.anchor_timesheet_id;
    if found and (
      v_anchor.timesheet_id=v_card_timesheet.timesheet_id
      or (nullif(btrim(coalesce(v_anchor.booking_id,'')),'') is not null
        and v_anchor.booking_id=v_card_timesheet.booking_id
        and v_anchor.contract_id is not distinct from v_card_timesheet.contract_id
        and v_anchor.week_ending_date is not distinct from v_card_timesheet.week_ending_date)
    ) then return true; end if;
  end if;
  if v_workflow.target_timesheet_id is not null then
    select * into v_target from public.timesheets where timesheet_id=v_workflow.target_timesheet_id;
    if found and v_target.parent_timesheet_id is not null then
      select * into v_parent from public.timesheets where timesheet_id=v_target.parent_timesheet_id;
      if found and (
        v_parent.timesheet_id=v_card_timesheet.timesheet_id
        or (nullif(btrim(coalesce(v_parent.booking_id,'')),'') is not null
          and v_parent.booking_id=v_card_timesheet.booking_id
          and v_parent.contract_id is not distinct from v_card_timesheet.contract_id
          and v_parent.week_ending_date is not distinct from v_card_timesheet.week_ending_date)
      ) then return true; end if;
    end if;
  end if;
  return false;
end;
$function$;

create or replace function private._candidate_action_invocation_v1(p_action jsonb)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_code text:=upper(coalesce(p_action->>'code',''));
  v_method text:=upper(coalesce(p_action->>'method',''));
  v_week public.contract_weeks%rowtype;
  v_daily public.timesheets%rowtype;
  v_route_family text;
  v_fixed jsonb:='{}'::jsonb;
  v_inputs jsonb:='[]'::jsonb;
  v_invocation jsonb;
begin
  if v_code in ('CONTINUE_TIMESHEET','CONTINUE_EXPENSE_CLAIM') then
    v_invocation:=jsonb_build_object('version',1,'kind','CLIENT_DESTINATION',
      'destination',case when v_code='CONTINUE_EXPENSE_CLAIM' then 'EXPENSE_CLAIM_EDITOR' else 'TIMESHEET_EDITOR' end,
      'context',jsonb_build_object('workflow_id',p_action->'workflow_id',
        'workflow_generation',p_action->'workflow_generation'));
    return (p_action-'method'-'path')||jsonb_build_object('method',null,'path',null,'invocation',v_invocation);
  end if;
  if v_code in ('ENTER_TIMESHEET','ADD_EXPENSES') then
    if v_code='ENTER_TIMESHEET' and nullif(p_action->>'contract_week_id','') is null then
      select * into v_daily from public.timesheets
        where timesheet_id=nullif(p_action->>'timesheet_id','')::uuid
          and sheet_scope='DAILY' and is_current and archived_at_utc is null;
      if found then
        return (p_action-'method'-'path')||jsonb_build_object('method',null,'path',null,
          'invocation',jsonb_build_object('version',1,'kind','CLIENT_DESTINATION',
            'destination','TIMESHEET_EDITOR','context',
            jsonb_build_object('timesheet_id',v_daily.timesheet_id)));
      end if;
    end if;
    select * into v_week from public.contract_weeks where id=nullif(p_action->>'contract_week_id','')::uuid;
    if not found then return p_action||jsonb_build_object('enabled',false,
      'disabled_reason','CANDIDATE_ACTION_CONTEXT_STALE'); end if;
    v_route_family:=private._candidate_route_family_v1(v_week.timesheet_id,v_week.id)->>'route_family';
    v_fixed:=jsonb_build_object('workflow',jsonb_strip_nulls(jsonb_build_object(
      'workflow_kind',case when v_code='ADD_EXPENSES' then 'CONTRACT_EXPENSE' else 'CONTRACT_HOURS' end,
      'scope','WEEKLY','route',case when v_route_family='QR' then 'PAPER' else 'ELECTRONIC' end,
      'contract_id',v_week.contract_id,'contract_week_id',v_week.id,
      'week_ending_date',v_week.week_ending_date,
      'timesheet_id',case when v_code='ADD_EXPENSES' then p_action->'timesheet_id' else null end
    )));
    v_inputs:='[{"name":"idempotency_key","type":"uuid","required":true}]'::jsonb;
  elsif v_code in ('RESUBMIT_TIMESHEET','RESUBMIT_TIMESHEET_AND_EXPENSES','RESUBMIT_EXPENSE_CLAIM','REVIEW_AND_RESUBMIT') then
    v_fixed:=jsonb_build_object('generation',p_action->'workflow_generation');
    v_inputs:='[{"name":"idempotency_key","type":"uuid","required":true}]'::jsonb;
  elsif v_method='POST' then
    if p_action ? 'workflow_generation' then
      v_fixed:=jsonb_build_object('generation',p_action->'workflow_generation');
    end if;
    if v_code in ('SEND_MANAGER_REMINDER','REQUEST_APPROVAL_AGAIN') then
      v_fixed:=v_fixed||jsonb_strip_nulls(jsonb_build_object(
        'approval_request_id',p_action->'approval_request_id',
        'approval_request_generation',p_action->'approval_request_generation'
      ));
    end if;
    v_inputs:=case v_code
      when 'DISCARD_EXPENSE_CLAIM' then '[{"name":"reason_note","type":"string","required":true},{"name":"idempotency_key","type":"uuid","required":true}]'::jsonb
      when 'CANCEL_ENTIRE_CLAIM_AND_START_AGAIN' then '[{"name":"reason_note","type":"string","required":true},{"name":"idempotency_key","type":"uuid","required":true}]'::jsonb
      when 'UPLOAD_SIGNED_RETURN' then '[{"name":"returned_pages","type":"array","required":true},{"name":"idempotency_key","type":"uuid","required":true}]'::jsonb
      when 'NO_WORK_THIS_WEEK' then '[{"name":"idempotency_key","type":"uuid","required":true}]'::jsonb
      else '[{"name":"idempotency_key","type":"uuid","required":true}]'::jsonb end;
  end if;
  v_invocation:=jsonb_build_object('version',1,'kind','HTTP','method',nullif(v_method,''),
    'path',p_action->>'path','fixed_body',v_fixed,'required_user_inputs',v_inputs,
    'idempotency',case when v_method='POST' then 'REQUIRED' else 'NOT_REQUIRED' end);
  return p_action||jsonb_build_object('invocation',v_invocation);
end;
$function$;

create or replace function public.candidate_app_timesheet_detail_v1(
  p_session_id uuid,
  p_environment text,
  p_timesheet_id uuid default null,
  p_contract_week_id uuid default null,
  p_workflow_id uuid default null,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_context jsonb;
  v_daily boolean:=false;
  v_daily_projection jsonb;
  v_candidate_id uuid;
  v_week public.contract_weeks%rowtype;
  v_contract public.contracts%rowtype;
  v_client public.clients%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_capabilities jsonb;
  v_evidence jsonb;
  v_components jsonb;
  v_claims jsonb;
  v_document_state jsonb:='{}'::jsonb;
  v_effective_week_ending_weekday integer;
  v_effective_current_week_ending_date date;
  v_tab_bucket text;
  v_candidate_status_code text;
  v_active_workflow_state text;
  v_rejected_workflow jsonb;
  v_primary_action jsonb;
  v_action_contract jsonb;
  v_detail_source_timesheet_id uuid;
begin
  perform private._candidate_require_feature_v1(p_environment,'candidate_app_reads');
  if num_nonnulls(p_timesheet_id,p_contract_week_id,p_workflow_id)<>1 then
    raise exception 'CANDIDATE_DETAIL_IDENTITY_INVALID' using errcode='22023';
  end if;
  v_context:=private._candidate_session_context_v1(p_session_id,p_environment,null,p_now_utc,false);
  v_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
  if v_candidate_id is null then raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000'; end if;

  if p_workflow_id is not null then
    select * into v_workflow from public.candidate_submission_workflows where id=p_workflow_id and candidate_id=v_candidate_id;
    if not found then raise exception 'CANDIDATE_DETAIL_NOT_FOUND' using errcode='P0002'; end if;
    v_detail_source_timesheet_id:=case
      when v_workflow.workflow_kind='CONTRACT_EXPENSE'
        or v_workflow.rejection_scope='COMPLETE_EXPENSE_CLAIM'
        then coalesce(v_workflow.anchor_timesheet_id,v_workflow.target_timesheet_id)
      else coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id) end;
    if v_detail_source_timesheet_id is not null then
      select current_version.timesheet_id into p_timesheet_id
      from public.timesheets source_version
      join public.timesheets current_version on current_version.is_current=true
        and current_version.archived_at_utc is null
        and (
          (nullif(btrim(coalesce(source_version.booking_id,'')),'') is not null
            and current_version.booking_id=source_version.booking_id
            and current_version.contract_id is not distinct from source_version.contract_id
            and current_version.week_ending_date is not distinct from source_version.week_ending_date)
          or (nullif(btrim(coalesce(source_version.booking_id,'')),'') is null
            and current_version.timesheet_id=source_version.timesheet_id)
        )
      where source_version.timesheet_id=v_detail_source_timesheet_id
        and upper(coalesce(current_version.line_type::text,'')) not in ('EXPENSES','MILEAGE')
      order by current_version.version desc,current_version.timesheet_id
      limit 1;
    end if;
    if p_timesheet_id is not null then
      select week_row.id into p_contract_week_id
      from public.contract_weeks week_row
      where week_row.timesheet_id=p_timesheet_id
        and week_row.contract_id=v_workflow.contract_id
        and week_row.week_ending_date=v_workflow.week_ending_date
      order by week_row.updated_at desc,week_row.id desc limit 1;
    end if;
    if p_contract_week_id is null then p_contract_week_id:=v_workflow.contract_week_id; end if;
  end if;
  if p_timesheet_id is not null then
    select * into v_timesheet from public.timesheets
      where timesheet_id=p_timesheet_id and is_current and archived_at_utc is null;
    v_daily:=found and v_timesheet.sheet_scope='DAILY';
  end if;
  if v_daily then
    if p_contract_week_id is not null then
      raise exception 'CANDIDATE_DETAIL_IDENTITY_INVALID' using errcode='22023';
    end if;
    v_daily_projection:=private._candidate_daily_read_projection_v1(
      p_environment,v_candidate_id,p_timesheet_id,p_now_utc);
    v_capabilities:=v_daily_projection->'capabilities';
    -- A local date value only: no Contract Week row is read, fabricated or written.
    v_week.week_ending_date:=v_timesheet.week_ending_date;
    v_effective_week_ending_weekday:=0;
    v_effective_current_week_ending_date:=(p_now_utc at time zone 'Europe/London')::date
      +mod(7-extract(dow from (p_now_utc at time zone 'Europe/London')::date)::integer,7);
    select * into v_fin from public.timesheets_financials
      where timesheet_id=p_timesheet_id and is_current
      order by computed_at_utc desc nulls last,updated_at desc,id desc limit 1;
  else
  if p_contract_week_id is not null then
    select * into v_week from public.contract_weeks where id=p_contract_week_id;
  else
    select * into v_week from public.contract_weeks where timesheet_id=p_timesheet_id order by updated_at desc,id desc limit 1;
  end if;
  if not found then raise exception 'CANDIDATE_DETAIL_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_contract from public.contracts where id=v_week.contract_id and candidate_id=v_candidate_id;
  if not found then raise exception 'CANDIDATE_DETAIL_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_client from public.clients where id=v_contract.client_id;
  select coalesce(
    v_contract.week_ending_weekday_snapshot,
    (
      select settings.week_ending_weekday
      from public.client_settings settings
      where settings.client_id=v_contract.client_id
        and settings.effective_from<=(p_now_utc at time zone 'Europe/London')::date
      order by settings.effective_from desc,settings.updated_at desc nulls last,settings.id desc
      limit 1
    ),0
  ) into v_effective_week_ending_weekday;
  v_effective_current_week_ending_date:=(
    (p_now_utc at time zone 'Europe/London')::date
    +mod(
      v_effective_week_ending_weekday
      -extract(dow from (p_now_utc at time zone 'Europe/London')::date)::integer+7,7
    )
  )::date;
  if p_timesheet_id is null then p_timesheet_id:=v_week.timesheet_id; end if;
  if p_timesheet_id is not null then
    select * into v_timesheet from public.timesheets where timesheet_id=p_timesheet_id and is_current=true and archived_at_utc is null;
    if not found then raise exception 'CANDIDATE_DETAIL_NOT_FOUND' using errcode='P0002'; end if;
    select * into v_fin from public.timesheets_financials where timesheet_id=p_timesheet_id and is_current=true
    order by computed_at_utc desc nulls last,updated_at desc,id desc limit 1;
  end if;
  v_capabilities:=private._candidate_record_capabilities_v1(p_timesheet_id,v_week.id,'{}'::jsonb);
  end if;

  select coalesce(jsonb_agg(jsonb_build_object(
    'id',e.id,'kind',e.kind,'document_role',e.document_role,'display_name',e.display_name,
    'processing_state',e.processing_state,'created_at',e.created_at
  ) order by e.created_at,e.id),'[]'::jsonb)
  into v_evidence from public.timesheet_evidence e
  where e.timesheet_id=p_timesheet_id and e.processing_state<>'SUPERSEDED';

  select coalesce(jsonb_agg(jsonb_build_object(
    'id',c.id,'workflow_id',c.workflow_id,'workflow_generation',c.workflow_generation,
    'component_kind',c.component_kind,
    'expense_category',c.expense_category,'document_role',c.document_role,'state',c.state,
    'media_type',c.media_type,'byte_size',c.byte_size,'created_at_utc',c.created_at_utc,
    'required',c.required,'review_ordinal',c.review_ordinal,
    'review_document_ready',c.review_render_state='READY',
    'review_document_generation',c.workflow_generation,
    'review_page_count',c.review_page_count,
    'review_content_sha256',case when c.review_content_sha256 is null then null else encode(c.review_content_sha256,'hex') end,
    'final_signed_document_ready',c.final_signed_render_state='READY',
    'final_signed_page_count',c.final_signed_page_count
  ) order by c.created_at_utc,c.component_no),'[]'::jsonb)
  into v_components from public.candidate_submission_components c
  join public.candidate_submission_workflows w on w.id=c.workflow_id
  where w.candidate_id=v_candidate_id
    and private._candidate_workflow_maps_to_card_v1(w.id,p_timesheet_id,v_week.id)
    and c.state<>'SUPERSEDED';

  select coalesce(jsonb_agg(jsonb_build_object(
    'workflow_id',w.id,'workflow_kind',w.workflow_kind,'state',w.state,'generation',w.generation,
    'detail_action_owner',w.id=p_workflow_id,
    'claim_family',rejection_policy.claim_family,
    'route',w.route,'target_timesheet_id',w.target_timesheet_id,
    'anchor_timesheet_id',w.anchor_timesheet_id,'issue_codes',w.issue_codes,
    'rejection_reason',w.rejection_reason,'rejection_scope',w.rejection_scope,
    'required_resubmission_action',case
      when w.state<>'REJECTED' or not rejection_policy.rejection_actionable then null
      when w.workflow_kind='CONTRACT_EXPENSE' or w.rejection_scope='COMPLETE_EXPENSE_CLAIM'
        then 'RESUBMIT_EXPENSE_CLAIM'
      when w.workflow_kind='CONTRACT_COMBINED' then 'RESUBMIT_TIMESHEET_AND_EXPENSES'
      else 'RESUBMIT_TIMESHEET' end,
    'rejection_actionable',rejection_policy.rejection_actionable,
    'review_document_ready',exists(select 1 from public.candidate_submission_components review_component
      where review_component.workflow_id=w.id and review_component.workflow_generation=case when w.state='FINALISED' then greatest(w.generation-1,1) else w.generation end
        and review_component.required=true and review_component.state<>'SUPERSEDED')
      and not exists(select 1 from public.candidate_submission_components review_component
      where review_component.workflow_id=w.id and review_component.workflow_generation=case when w.state='FINALISED' then greatest(w.generation-1,1) else w.generation end
        and review_component.required=true and review_component.state<>'SUPERSEDED'
        and review_component.review_render_state<>'READY'),
    'review_document_generation',case when w.state='FINALISED' then greatest(w.generation-1,1) else w.generation end,
    'review_page_count',(select sum(coalesce(review_component.review_page_count,0))
      from public.candidate_submission_components review_component
      where review_component.workflow_id=w.id and review_component.workflow_generation=case when w.state='FINALISED' then greatest(w.generation-1,1) else w.generation end
        and review_component.required=true and review_component.state<>'SUPERSEDED'),
    'manager_approval_state',coalesce((select approval.state
      from public.candidate_approval_requests approval where approval.workflow_id=w.id
        and approval.workflow_generation=case when w.state='FINALISED' then greatest(w.generation-1,1) else w.generation end
      order by approval.request_generation desc,approval.created_at_utc desc limit 1),w.state),
    'final_signed_document_ready',exists(select 1 from public.candidate_submission_components final_component
      where final_component.workflow_id=w.id and final_component.workflow_generation=case when w.state='FINALISED' then greatest(w.generation-1,1) else w.generation end
        and final_component.required=true and final_component.state<>'SUPERSEDED')
      and not exists(select 1 from public.candidate_submission_components final_component
      where final_component.workflow_id=w.id and final_component.workflow_generation=case when w.state='FINALISED' then greatest(w.generation-1,1) else w.generation end
        and final_component.required=true and final_component.state<>'SUPERSEDED'
        and final_component.final_signed_render_state<>'READY'),
    'updated_at_utc',w.updated_at_utc
  ) order by w.updated_at_utc desc,w.id desc),'[]'::jsonb)
  into v_claims from public.candidate_submission_workflows w
  cross join lateral (
    select
      case when w.workflow_kind='CONTRACT_EXPENSE'
          or w.rejection_scope='COMPLETE_EXPENSE_CLAIM'
        then 'EXPENSES' else 'HOURS' end as claim_family,
      case when w.state<>'REJECTED' then false
        else not private._candidate_rejection_replaced_v1(w.id)
      end as rejection_actionable
  ) rejection_policy
  where w.candidate_id=v_candidate_id and (v_daily or w.contract_id=v_contract.id)
    and (v_daily or w.week_ending_date=v_week.week_ending_date)
    and w.state not in ('CANCELLED','SUPERSEDED')
    and private._candidate_workflow_maps_to_card_v1(w.id,p_timesheet_id,v_week.id);

  select jsonb_build_object(
    'workflow_id',document_workflow.id,
    'workflow_generation',document_workflow.generation,
    'review_document_ready',coalesce(document_readiness.review_ready,false),
    'review_document_component_id',hours_component.id,
    'review_document_generation',hours_component.workflow_generation,
    'review_page_count',hours_component.review_page_count,
    'manager_approval_state',coalesce(latest_approval.state,document_workflow.state),
    'final_signed_document_ready',coalesce(document_readiness.final_ready,false)
  ) into v_document_state
  from public.candidate_submission_workflows document_workflow
  left join lateral (
    select component.* from public.candidate_submission_components component
    where component.workflow_id=document_workflow.id
      and component.workflow_generation=case when document_workflow.state='FINALISED' then greatest(document_workflow.generation-1,1) else document_workflow.generation end
      and component.component_kind='HOURS_TIMESHEET' and component.state<>'SUPERSEDED'
    order by component.review_ordinal,component.id limit 1
  ) hours_component on true
  left join lateral (
    select
      count(*)>0 and bool_and(component.review_render_state='READY') as review_ready,
      count(*)>0 and bool_and(component.final_signed_render_state='READY') as final_ready
    from public.candidate_submission_components component
    where component.workflow_id=document_workflow.id
      and component.workflow_generation=case when document_workflow.state='FINALISED' then greatest(document_workflow.generation-1,1) else document_workflow.generation end
      and component.required=true and component.state<>'SUPERSEDED'
  ) document_readiness on true
  left join lateral (
    select approval.* from public.candidate_approval_requests approval
    where approval.workflow_id=document_workflow.id
      and approval.workflow_generation=case when document_workflow.state='FINALISED' then greatest(document_workflow.generation-1,1) else document_workflow.generation end
    order by approval.request_generation desc,approval.created_at_utc desc limit 1
  ) latest_approval on true
  where document_workflow.candidate_id=v_candidate_id
    and private._candidate_workflow_maps_to_card_v1(
      document_workflow.id,p_timesheet_id,v_week.id
    )
    and document_workflow.state not in ('CANCELLED','SUPERSEDED')
  order by (document_workflow.id=p_workflow_id) desc,document_workflow.updated_at_utc desc
  limit 1;

  select workflow_item->>'state' into v_active_workflow_state
  from jsonb_array_elements(coalesce(v_claims,'[]'::jsonb)) workflow_item
  where workflow_item->>'state' in (
    'CREATED','WORKER_DRAFT','WORKER_SUBMITTED',
    'WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT','READY_FOR_MANAGER_APPROVAL',
    'AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED',
    'MANAGER_APPROVED_PENDING_FINAL_DOCUMENT','READY_TO_FINALISE',
    'AWAITING_PAPER_RETURN','RECEIVED','REFUSED'
  )
  order by coalesce((workflow_item->>'detail_action_owner')::boolean,false) desc,
    workflow_item->>'updated_at_utc' desc,workflow_item->>'workflow_id'
  limit 1;
  select workflow_item into v_rejected_workflow
  from jsonb_array_elements(coalesce(v_claims,'[]'::jsonb)) workflow_item
  where workflow_item->>'state'='REJECTED'
    and coalesce((workflow_item->>'rejection_actionable')::boolean,false)
  order by coalesce((workflow_item->>'detail_action_owner')::boolean,false) desc,
    workflow_item->>'updated_at_utc' desc,workflow_item->>'workflow_id'
  limit 1;
  v_candidate_status_code:=private._candidate_status_code_v1(
    v_fin.paid_at_utc is not null,
    v_fin.authorised_at_utc is not null or (v_daily and v_timesheet.authorised_at_server is not null),
    v_fin.locked_by_invoice_id is not null
      or upper(coalesce(v_timesheet.status::text,''))='INVOICED',
    v_active_workflow_state,v_rejected_workflow is not null,
    v_fin.processing_status::text,v_week.status::text
  );
  v_tab_bucket:=case
    when v_week.week_ending_date>v_effective_current_week_ending_date then 'EXCLUDED'
    when v_fin.paid_at_utc is null or v_fin.paid_at_utc>p_now_utc then 'CURRENT'
    when v_fin.paid_at_utc>=p_now_utc-interval '7 days' then 'CURRENT'
    when v_fin.paid_at_utc<p_now_utc-interval '7 days'
      and v_week.week_ending_date between v_effective_current_week_ending_date-105
        and v_effective_current_week_ending_date then 'HISTORY'
    else 'EXCLUDED' end;
  v_primary_action:=private._candidate_timesheet_primary_action_v1(
    v_candidate_status_code,v_claims,v_capabilities,p_timesheet_id,v_week.id
  );
  v_action_contract:=private._candidate_timesheet_action_contract_v1(
    v_candidate_status_code,v_claims,v_capabilities,p_timesheet_id,v_week.id,p_now_utc
  );
  v_primary_action:=v_action_contract->'primary_action';
  if v_daily and v_active_workflow_state='RECEIVED' then
    -- The complete receipt is waiting for Office, not another Candidate submit.
    -- Keep the separately guarded cancellation action until Office authorises.
    v_primary_action:=null;
    v_action_contract:=v_action_contract||jsonb_build_object('primary_action',null,
      'available_actions',(select coalesce(jsonb_agg(a),'[]'::jsonb)
        from jsonb_array_elements(v_action_contract->'available_actions') a
        where a->>'code'<>'CONTINUE_TIMESHEET'));
  end if;
  if v_active_workflow_state='AWAITING_PAPER_RETURN' then
    v_candidate_status_code:=case v_action_contract->'paper_pack'->>'state'
      when 'READY' then 'AWAITING_SIGNED_DOCUMENTS'
      when 'FAILED' then 'DOCUMENT_PREPARATION_FAILED'
      when 'RETIRED' then 'PAPER_DELIVERY_RETIRED'
      when 'STALE' then 'PAPER_DELIVERY_STALE'
      else 'PREPARING_DOCUMENTS' end;
  end if;

  return jsonb_build_object(
    'ok',true,
    'week_ending_label',private._candidate_week_ending_label_v1(v_week.week_ending_date),
    'candidate_status_code',v_candidate_status_code,
    'list_membership',jsonb_build_object(
      'tab_bucket',v_tab_bucket,
      'effective_current_week_ending_date',v_effective_current_week_ending_date,
      'paid_current_cutoff_utc',p_now_utc-interval '7 days'
    ),
    'primary_action',v_primary_action,
    'available_actions',coalesce(v_action_contract->'available_actions','[]'::jsonb),
    'manager_approval',v_action_contract->'manager_approval',
    'paper_pack',v_action_contract->'paper_pack',
    'daily_shift',case when v_daily then v_daily_projection->'daily_shift' else null end,
    'contract_week',case when v_daily then null else jsonb_build_object(
      'id',v_week.id,'contract_id',v_contract.id,'week_ending_date',v_week.week_ending_date,
      'week_ending_weekday',btrim(to_char(v_week.week_ending_date,'FMDay')),
      'client_name',v_client.name,'job_title',v_contract.role,'band',v_contract.band,
      'additional_seq',v_week.additional_seq,'status',v_week.status,'planned_schedule_json',v_week.planned_schedule_json
    ) end,
    'timesheet',case when v_timesheet.timesheet_id is null then null else jsonb_build_object(
      'id',v_timesheet.timesheet_id,'status',v_timesheet.status,'submission_mode',v_timesheet.submission_mode,
      'sheet_scope',v_timesheet.sheet_scope,'actual_schedule_json',v_timesheet.actual_schedule_json,
      'additional_units_week',v_timesheet.additional_units_week,'qr_status',v_timesheet.qr_status,
      'canonical_work_date',case when v_timesheet.sheet_scope='DAILY' then private._candidate_daily_work_date_v1(
        v_timesheet.worked_start_iso,v_timesheet.scheduled_start_iso,v_timesheet.week_ending_date) else null end
    ) end,
    'hours',jsonb_build_object('total_hours',case when v_daily then (v_daily_projection->>'hours')::numeric else coalesce(v_fin.total_hours,0) end,
      'actual_schedule_json',coalesce(v_fin.actual_schedule_json,v_timesheet.actual_schedule_json)),
    'expenses',jsonb_build_object(
      'expenses_pay_ex_vat',coalesce(v_fin.expenses_pay_ex_vat,0),'expenses_description',v_fin.expenses_description,
      'mileage_units',coalesce(v_fin.mileage_units,0),'mileage_pay_ex_vat',coalesce(v_fin.mileage_pay_ex_vat,0),
      'travel_pay_ex_vat',coalesce(v_fin.travel_pay_ex_vat,0),
      'accommodation_pay_ex_vat',coalesce(v_fin.accommodation_pay_ex_vat,0),
      'other_pay_ex_vat',coalesce(v_fin.other_pay_ex_vat,0)
    ),
    'lifecycle',jsonb_build_object(
      'processing_status',v_fin.processing_status,'authorised_at_utc',coalesce(v_fin.authorised_at_utc,case when v_daily then v_timesheet.authorised_at_server else null end),
      'paid_at_utc',v_fin.paid_at_utc,'invoice_locked',v_fin.locked_by_invoice_id is not null
    ),
    'capabilities',v_capabilities,
    'manager_review',coalesce(v_document_state,'{}'::jsonb),
    'evidence',v_evidence,
    'components',v_components,
    'workflows',v_claims,
    'rejections',(
      select coalesce(jsonb_agg(workflow_item order by
        workflow_item->>'updated_at_utc' desc,workflow_item->>'workflow_id'),'[]'::jsonb)
      from jsonb_array_elements(v_claims) workflow_item
      where workflow_item->>'state'='REJECTED'
        and coalesce((workflow_item->>'rejection_actionable')::boolean,false)
    )
  );
end;
$function$;

create or replace function public.candidate_app_timesheet_detail_v2(
  p_session_id uuid,
  p_environment text,
  p_timesheet_id uuid default null,
  p_contract_week_id uuid default null,
  p_workflow_id uuid default null,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_result jsonb;
  v_timesheet_id uuid;
  v_contract_week_id uuid;
  v_contract_id uuid;
  v_week_ending_date date;
  v_context jsonb;
  v_weekly_context jsonb;
  v_contract public.contracts%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_global public.settings_defaults%rowtype;
  v_additional_definitions jsonb:='[]'::jsonb;
  v_current_week_units jsonb:='[]'::jsonb;
  v_current_day_units jsonb:='[]'::jsonb;
  v_bh_dates jsonb:='[]'::jsonb;
  v_hours_entry_mode text;
begin
  v_result:=public.candidate_app_timesheet_detail_v1(
    p_session_id,p_environment,p_timesheet_id,p_contract_week_id,p_workflow_id,p_now_utc
  );
  v_timesheet_id:=nullif(v_result#>>'{timesheet,id}','')::uuid;
  v_contract_week_id:=nullif(v_result#>>'{contract_week,id}','')::uuid;
  v_contract_id:=nullif(v_result#>>'{capabilities,contract_id}','')::uuid;
  v_week_ending_date:=nullif(v_result#>>'{contract_week,week_ending_date}','')::date;

  if v_result#>>'{timesheet,sheet_scope}'='DAILY' then
    v_context:=private._candidate_daily_read_projection_v1(p_environment,
      (v_result#>>'{capabilities,candidate_id}')::uuid,v_timesheet_id,p_now_utc);
    return v_result||jsonb_build_object('break_entry',v_context->'break_entry',
      'weekly_entry',null);
  end if;
  v_context:=private._candidate_break_entry_context_core_v1(
    v_timesheet_id,v_contract_week_id,
    (p_now_utc at time zone 'Europe/London')::date,
    coalesce(v_result->'capabilities','{}'::jsonb)
  );

  if v_contract_id is null then
    raise exception 'CANDIDATE_CONTRACT_NOT_FOUND' using errcode='P0002';
  end if;
  select * into v_contract from public.contracts where id=v_contract_id;
  if not found then raise exception 'CANDIDATE_CONTRACT_NOT_FOUND' using errcode='P0002'; end if;

  if v_timesheet_id is not null then
    select * into v_timesheet from public.timesheets where timesheet_id=v_timesheet_id;
  end if;
  select * into v_global from public.settings_defaults where id=1;
  if not found then raise exception 'CANDIDATE_GLOBAL_SETTINGS_MISSING' using errcode='55000'; end if;

  if jsonb_typeof(v_contract.additional_rates_json)='array' then
    select coalesce(jsonb_agg(jsonb_build_object(
      'code',upper(btrim(rate->>'code')),
      'label',coalesce(nullif(btrim(rate->>'bucket_name'),''),upper(btrim(rate->>'code'))),
      'unit_name',coalesce(nullif(btrim(rate->>'unit_name'),''),'unit'),
      'frequency',case upper(coalesce(rate->>'frequency',''))
        when 'ONE_PER_DAY' then 'ONE_PER_DAY'
        when 'WEEKENDS_AND_BH_ONLY' then 'WEEKENDS_AND_BH_ONLY'
        when 'WEEKDAYS_EXCL_BH_ONLY' then 'WEEKDAYS_EXCL_BH_ONLY'
        else 'ONE_PER_WEEK'
      end
    ) order by ord),'[]'::jsonb)
    into v_additional_definitions
    from jsonb_array_elements(v_contract.additional_rates_json) with ordinality as configured(rate,ord)
    where jsonb_typeof(rate)='object'
      and upper(btrim(coalesce(rate->>'code',''))) ~ '^[A-Z0-9_]{1,40}$';
  end if;

  if jsonb_typeof(v_timesheet.additional_units_week)='object' then
    select coalesce(jsonb_agg(jsonb_build_object(
      'code',upper(btrim(code)),'value',quantity::numeric
    ) order by upper(btrim(code))),'[]'::jsonb)
    into v_current_week_units
    from jsonb_each(v_timesheet.additional_units_week) configured(code,raw_value)
    cross join lateral (select case
      when jsonb_typeof(raw_value)='number' then raw_value#>>'{}'
      when jsonb_typeof(raw_value)='object' then coalesce(
        raw_value->>'unit_count',raw_value->>'quantity',raw_value->>'units'
      )
      else null end as quantity) parsed
    where upper(btrim(code)) ~ '^[A-Z0-9_]{1,40}$'
      and quantity ~ '^\d+(\.\d+)?$'
      and quantity::numeric between 0 and 1000000;
  end if;

  if jsonb_typeof(v_timesheet.additional_units_per_day)='object' then
    select coalesce(jsonb_agg(jsonb_build_object(
      'date',work_date,'code',upper(btrim(code)),'value',quantity::numeric
    ) order by work_date,upper(btrim(code))),'[]'::jsonb)
    into v_current_day_units
    from jsonb_each(v_timesheet.additional_units_per_day) configured_days(work_date,day_values)
    cross join lateral jsonb_each(case when jsonb_typeof(day_values)='object' then day_values else '{}'::jsonb end)
      configured_units(code,raw_value)
    cross join lateral (select case
      when jsonb_typeof(raw_value)='number' then raw_value#>>'{}'
      when jsonb_typeof(raw_value)='object' then coalesce(
        raw_value->>'unit_count',raw_value->>'quantity',raw_value->>'units'
      )
      else null end as quantity) parsed
    where work_date ~ '^\d{4}-\d{2}-\d{2}$'
      and upper(btrim(code)) ~ '^[A-Z0-9_]{1,40}$'
      and quantity ~ '^\d+(\.\d+)?$'
      and quantity::numeric between 0 and 1000000;
  end if;

  if v_week_ending_date is not null and jsonb_typeof(v_global.bh_list)='array' then
    select coalesce(jsonb_agg(value order by value),'[]'::jsonb)
    into v_bh_dates
    from jsonb_array_elements_text(v_global.bh_list) as holiday(value)
    where value ~ '^\d{4}-\d{2}-\d{2}$'
      and value::date between v_week_ending_date-6 and v_week_ending_date;
  end if;

  v_hours_entry_mode:=case
    when coalesce((v_result#>>'{capabilities,import_authoritative}')::boolean,false)
      then 'VIEW_ONLY_IMPORT'
    when coalesce(v_contract.is_ad_hoc,false)
      then 'AD_HOC_BLANK'
    else 'PLANNED_PREFILL'
  end;

  v_weekly_context:=jsonb_build_object(
    'schema_version','CANDIDATE_WEEKLY_ENTRY_CONTEXT_V1',
    'hours_entry_mode',v_hours_entry_mode,
    'prepopulate_planned_schedule',v_hours_entry_mode='PLANNED_PREFILL',
    'additional_units_enabled',jsonb_array_length(v_additional_definitions)>0,
    'additional_unit_definitions',v_additional_definitions,
    'bank_holiday_dates',v_bh_dates,
    'current_additional_units_week',v_current_week_units,
    'current_additional_units_per_day',v_current_day_units
  );
  v_weekly_context:=v_weekly_context||jsonb_build_object(
    'context_sha256',encode(
      extensions.digest(convert_to(v_weekly_context::text,'UTF8'),'sha256'),'hex'
    )
  );

  return v_result||jsonb_build_object(
    'break_entry',v_context,
    'weekly_entry',v_weekly_context
  );
end
$function$;

create or replace function public.candidate_break_entry_context_get_v1(
  p_session_id uuid,
  p_environment text,
  p_workflow_id uuid,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_session jsonb;
  v_candidate_id uuid;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_capabilities jsonb;
begin
  v_session:=private._candidate_session_context_v1(
    p_session_id,p_environment,null,p_now_utc,false
  );
  v_candidate_id:=nullif(v_session->>'selected_candidate_id','')::uuid;
  select * into v_workflow from public.candidate_submission_workflows
  where id=p_workflow_id and candidate_id=v_candidate_id;
  if not found then raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002'; end if;
  if v_workflow.workflow_kind='DAILY' then
    v_capabilities:=private._candidate_daily_read_projection_v1(
      p_environment,v_candidate_id,v_workflow.target_timesheet_id,p_now_utc);
    return (v_capabilities->'break_entry')||jsonb_build_object(
      'workflow_id',v_workflow.id,'workflow_generation',v_workflow.generation);
  end if;
  v_capabilities:=private._candidate_record_capabilities_v1(
    v_workflow.target_timesheet_id,v_workflow.contract_week_id,'{}'::jsonb
  );
  return private._candidate_break_entry_context_core_v1(
    v_workflow.target_timesheet_id,v_workflow.contract_week_id,
    (p_now_utc at time zone 'Europe/London')::date,v_capabilities
  )||jsonb_build_object(
    'workflow_id',v_workflow.id,
    'workflow_generation',v_workflow.generation
  );
end
$function$;

alter function private._candidate_workflow_maps_to_card_v1(uuid,uuid,uuid) owner to postgres;
revoke all on function private._candidate_workflow_maps_to_card_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;

alter function private._candidate_action_invocation_v1(jsonb) owner to postgres;
revoke all on function private._candidate_action_invocation_v1(jsonb) from public,anon,authenticated,service_role;

alter function public.candidate_app_timesheet_detail_v1(uuid,text,uuid,uuid,uuid,timestamptz) owner to postgres;
revoke all on function public.candidate_app_timesheet_detail_v1(uuid,text,uuid,uuid,uuid,timestamptz) from public,anon,authenticated;
grant execute on function public.candidate_app_timesheet_detail_v1(uuid,text,uuid,uuid,uuid,timestamptz) to service_role;

alter function public.candidate_app_timesheet_detail_v2(uuid,text,uuid,uuid,uuid,timestamptz) owner to postgres;
revoke all on function public.candidate_app_timesheet_detail_v2(uuid,text,uuid,uuid,uuid,timestamptz) from public,anon,authenticated;
grant execute on function public.candidate_app_timesheet_detail_v2(uuid,text,uuid,uuid,uuid,timestamptz) to service_role;

alter function public.candidate_break_entry_context_get_v1(uuid,text,uuid,timestamptz) owner to postgres;
revoke all on function public.candidate_break_entry_context_get_v1(uuid,text,uuid,timestamptz) from public,anon,authenticated;
grant execute on function public.candidate_break_entry_context_get_v1(uuid,text,uuid,timestamptz) to service_role;

notify pgrst, 'reload schema';

commit;
