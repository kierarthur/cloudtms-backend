-- Repeatable CloudTMS function/view authority:
-- candidate_advanced_expense_component_policy_v1
--
-- Category identity and approval facts are separate from receipt pages.  All
-- mutations recheck the owning Timesheet protection state while locked.  This
-- file does not grant browser-table access and does not alter payment,
-- invoicing, settlement or Banking Pay authority.

\set ON_ERROR_STOP on

begin;

create or replace function private._candidate_expense_number_v1(
  p_value jsonb,
  p_key text
)
returns numeric
language sql
immutable
parallel safe
set search_path = pg_catalog, pg_temp
as $function$
  select case
    when pg_catalog.jsonb_typeof(coalesce(p_value,'{}'::jsonb)->p_key)='number'
      then greatest(((coalesce(p_value,'{}'::jsonb)->>p_key)::numeric),0)
    when coalesce(p_value,'{}'::jsonb)->>p_key ~ '^[+]?[0-9]+(?:[.][0-9]+)?$'
      then greatest(((coalesce(p_value,'{}'::jsonb)->>p_key)::numeric),0)
    else 0
  end;
$function$;

create or replace function private._candidate_expense_payload_without_category_v1(
  p_payload jsonb,
  p_expense_category text
)
returns jsonb
language plpgsql
immutable
parallel safe
set search_path = pg_catalog, private, pg_temp
as $function$
declare
  v_payload jsonb:=coalesce(p_payload,'{}'::jsonb);
  v_category text:=upper(btrim(coalesce(p_expense_category,'')));
  v_travel numeric;
  v_accommodation numeric;
  v_other numeric;
begin
  if pg_catalog.jsonb_typeof(v_payload)<>'object'
     or v_category not in ('MILEAGE','TRAVEL','ACCOMMODATION','OTHER') then
    raise exception 'CANDIDATE_EXPENSE_COMPONENT_ACTION_INVALID' using errcode='22023';
  end if;
  if v_category='MILEAGE' then
    v_payload:=v_payload||jsonb_build_object(
      'mileage_units',0,'mileage_pay_ex_vat',0,'mileage_charge_ex_vat',0,
      'total_mileage',0
    );
  elsif v_category='TRAVEL' then
    v_payload:=v_payload||jsonb_build_object(
      'travel_amount',0,'travel_pay_ex_vat',0,'travel_charge_ex_vat',0
    );
  elsif v_category='ACCOMMODATION' then
    v_payload:=v_payload||jsonb_build_object(
      'accommodation_amount',0,'accommodation_pay_ex_vat',0,'accommodation_charge_ex_vat',0
    );
  else
    v_payload:=v_payload||jsonb_build_object(
      'other_amount',0,'other_pay_ex_vat',0,'other_charge_ex_vat',0
    );
  end if;
  v_travel:=private._candidate_expense_number_v1(v_payload,'travel_pay_ex_vat');
  v_accommodation:=private._candidate_expense_number_v1(v_payload,'accommodation_pay_ex_vat');
  v_other:=private._candidate_expense_number_v1(v_payload,'other_pay_ex_vat');
  return v_payload||jsonb_build_object(
    'expenses_pay_ex_vat',v_travel+v_accommodation+v_other,
    'expenses_charge_ex_vat',
      private._candidate_expense_number_v1(v_payload,'travel_charge_ex_vat')
      +private._candidate_expense_number_v1(v_payload,'accommodation_charge_ex_vat')
      +private._candidate_expense_number_v1(v_payload,'other_charge_ex_vat')
  );
end;
$function$;

create or replace function private._candidate_expense_submission_without_category_v1(
  p_submission jsonb,
  p_expense_category text
)
returns jsonb
language plpgsql
immutable
parallel safe
set search_path = pg_catalog, private, pg_temp
as $function$
declare
  v_result jsonb:=coalesce(p_submission,'{}'::jsonb);
  v_key text;
  v_value jsonb;
  v_snapshot jsonb;
begin
  if pg_catalog.jsonb_typeof(v_result)<>'object' then
    raise exception 'CANDIDATE_IMMUTABLE_SUBMISSION_REQUIRED' using errcode='22023';
  end if;
  foreach v_key in array array['expense_submission','hours_submission','expense_claim'] loop
    if pg_catalog.jsonb_typeof(v_result->v_key)='object' then
      v_value:=private._candidate_expense_payload_without_category_v1(
        v_result->v_key,p_expense_category
      );
      if pg_catalog.jsonb_typeof(v_value->'canonical_tsfin_snapshot')='object' then
        v_snapshot:=private._candidate_expense_payload_without_category_v1(
          v_value->'canonical_tsfin_snapshot',p_expense_category
        );
        v_value:=pg_catalog.jsonb_set(v_value,'{canonical_tsfin_snapshot}',v_snapshot,true);
      end if;
      v_result:=pg_catalog.jsonb_set(v_result,array[v_key],v_value,true);
    end if;
  end loop;
  return private._candidate_expense_payload_without_category_v1(v_result,p_expense_category);
end;
$function$;

create or replace function private._candidate_expense_component_values_v1(
  p_workflow_id uuid
)
returns table(expense_category text,amount numeric,mileage_units numeric)
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_submission jsonb;
  v_claim jsonb;
begin
  select workflow.immutable_submission_json into v_submission
  from public.candidate_submission_workflows workflow where workflow.id=p_workflow_id;
  if not found then raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002'; end if;
  v_claim:=coalesce(
    v_submission#>'{expense_submission,canonical_tsfin_snapshot}',
    v_submission->'expense_submission',
    v_submission#>'{expense_claim,canonical_tsfin_snapshot}',
    v_submission->'expense_claim',
    '{}'::jsonb
  );
  return query
  select category.category_name,
    case category.category_name
      when 'MILEAGE' then private._candidate_expense_number_v1(v_claim,'mileage_pay_ex_vat')
      when 'TRAVEL' then private._candidate_expense_number_v1(v_claim,'travel_pay_ex_vat')
      when 'ACCOMMODATION' then private._candidate_expense_number_v1(v_claim,'accommodation_pay_ex_vat')
      else private._candidate_expense_number_v1(v_claim,'other_pay_ex_vat')
    end,
    case when category.category_name='MILEAGE'
      then private._candidate_expense_number_v1(v_claim,'mileage_units') else 0 end
  from (values ('MILEAGE'),('TRAVEL'),('ACCOMMODATION'),('OTHER')) category(category_name);
end;
$function$;

create or replace function private._candidate_expense_components_sync_v1(
  p_workflow_id uuid,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_workflow public.candidate_submission_workflows%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_approval public.candidate_approval_requests%rowtype;
  v_value record;
  v_component public.candidate_expense_components%rowtype;
  v_document_generation integer;
  v_lifecycle text;
  v_manager_state text;
  v_agency_state text;
  v_has_evidence boolean;
  v_changed boolean;
  v_count integer:=0;
  v_before jsonb;
begin
  select * into v_workflow from public.candidate_submission_workflows
  where id=p_workflow_id;
  if not found then raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002'; end if;
  if v_workflow.workflow_kind not in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
     or v_workflow.immutable_submission_json is null then
    return jsonb_build_object('ok',true,'workflow_id',v_workflow.id,'component_count',0);
  end if;
  v_document_generation:=case when v_workflow.state='FINALISED'
    then greatest(v_workflow.generation-1,1) else v_workflow.generation end;
  if v_workflow.target_timesheet_id is not null then
    select * into v_fin from public.timesheets_financials financial
    where financial.timesheet_id=v_workflow.target_timesheet_id and financial.is_current
    order by financial.computed_at_utc desc nulls last,financial.updated_at desc,financial.id desc limit 1;
    select * into v_timesheet from public.timesheets timesheet
    where timesheet.timesheet_id=v_workflow.target_timesheet_id and timesheet.is_current;
  end if;
  select * into v_approval from public.candidate_approval_requests request
  where request.workflow_id=v_workflow.id
    and request.workflow_generation=v_document_generation
  order by request.request_generation desc,request.updated_at_utc desc,request.id desc limit 1;
  v_lifecycle:=case
    when v_workflow.state='REFUSED' then 'MANAGER_REFUSED'
    when v_workflow.state='REJECTED' then 'OFFICE_REJECTED'
    when v_workflow.state='CANCELLED' then 'CANCELLED'
    when v_workflow.state in ('EXPIRED','SUPERSEDED') then 'SUPERSEDED'
    when v_workflow.state in ('MANAGER_APPROVED','MANAGER_APPROVED_PENDING_FINAL_DOCUMENT','READY_TO_FINALISE','FINALISED','RECEIVED') then 'MANAGER_APPROVED'
    when v_workflow.state in ('CREATED','WORKER_DRAFT') then 'DRAFT'
    else 'SUBMITTED' end;
  v_manager_state:=case
    when v_workflow.state='REFUSED' or v_approval.state='REFUSED' then 'REFUSED'
    when v_workflow.manager_approved_at_utc is not null or v_approval.state='APPROVED'
      or v_workflow.state in (
        'MANAGER_APPROVED','MANAGER_APPROVED_PENDING_FINAL_DOCUMENT',
        'READY_TO_FINALISE','FINALISED','RECEIVED'
      ) then 'APPROVED'
    when v_approval.id is not null and v_approval.state='PENDING' then 'PENDING'
    else 'NOT_REQUESTED' end;
  v_agency_state:=case
    when v_fin.paid_at_utc is not null or upper(coalesce(v_timesheet.status::text,''))='PAID' then 'PAID'
    when v_fin.locked_by_invoice_id is not null or upper(coalesce(v_timesheet.status::text,''))='INVOICED' then 'INVOICED'
    when v_fin.authorised_at_utc is not null
      or v_timesheet.authorised_at_server is not null
      or upper(coalesce(v_timesheet.status::text,'')) in ('AUTHORISED','AUTHORIZED')
      then 'AUTHORISED'
    else 'NOT_AUTHORISED' end;

  for v_value in select * from private._candidate_expense_component_values_v1(v_workflow.id) loop
    select exists(
      select 1 from public.candidate_submission_components evidence
      where evidence.workflow_id=v_workflow.id
        and evidence.workflow_generation=v_document_generation
        and evidence.expense_category=v_value.expense_category
        and evidence.component_kind in ('MILEAGE_FORM','EXPENSE_EVIDENCE')
        and evidence.state not in ('SUPERSEDED','REJECTED','ABANDONED')
    ) into v_has_evidence;
    if v_value.amount<=0 and v_value.mileage_units<=0 and not v_has_evidence then continue; end if;
    select * into v_component from public.candidate_expense_components component
    where component.workflow_id=v_workflow.id
      and component.expense_category=v_value.expense_category
    for update;
    if not found then
      v_before:='{}'::jsonb;
      insert into public.candidate_expense_components(
        workflow_id,workflow_generation,expense_category,owning_timesheet_id,amount,mileage_units,
        lifecycle_state,manager_approval_state,agency_authorisation_state,approval_request_id,
        submitted_at_utc,manager_approved_at_utc,refusal_kind,refusal_reason,refused_at_utc,
        removed_at_utc,created_at_utc,updated_at_utc
      ) values (
        v_workflow.id,v_document_generation,v_value.expense_category,v_workflow.target_timesheet_id,
        v_value.amount,v_value.mileage_units,v_lifecycle,v_manager_state,v_agency_state,v_approval.id,
        v_workflow.worker_submitted_at_utc,v_workflow.manager_approved_at_utc,
        case when v_lifecycle='MANAGER_REFUSED' then 'MANAGER_REFUSAL'
          when v_lifecycle='OFFICE_REJECTED' then 'AGENCY_REJECTION' else null end,
        case when v_lifecycle in ('MANAGER_REFUSED','OFFICE_REJECTED')
          then coalesce(v_approval.refusal_reason,v_workflow.rejection_reason,'No reason recorded') else null end,
        case when v_lifecycle in ('MANAGER_REFUSED','OFFICE_REJECTED')
          then coalesce(v_approval.refused_at_utc,v_workflow.updated_at_utc) else null end,
        case when v_lifecycle in ('OFFICE_REJECTED','CANCELLED','SUPERSEDED') then v_workflow.updated_at_utc else null end,
        p_now_utc,p_now_utc
      ) returning * into v_component;
      v_changed:=true;
    else
      -- A deliberate component withdrawal/cancellation is historical truth;
      -- a later generic workflow sync must never silently reactivate it.
       if v_component.lifecycle_state in (
         'MANAGER_REFUSED','OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED'
       ) then
        v_count:=v_count+1;
        continue;
      end if;
      v_changed:=v_component.workflow_generation is distinct from v_document_generation
        or v_component.owning_timesheet_id is distinct from v_workflow.target_timesheet_id
        or v_component.amount is distinct from v_value.amount
        or v_component.mileage_units is distinct from v_value.mileage_units
        or v_component.lifecycle_state is distinct from v_lifecycle
        or v_component.manager_approval_state is distinct from v_manager_state
        or v_component.agency_authorisation_state is distinct from v_agency_state
        or v_component.approval_request_id is distinct from v_approval.id;
      if v_changed then
        v_before:=to_jsonb(v_component);
        update public.candidate_expense_components set
          workflow_generation=v_document_generation,
          component_generation=component_generation+1,
          owning_timesheet_id=v_workflow.target_timesheet_id,
          amount=v_value.amount,mileage_units=v_value.mileage_units,
          lifecycle_state=v_lifecycle,manager_approval_state=v_manager_state,
          agency_authorisation_state=v_agency_state,approval_request_id=v_approval.id,
          submitted_at_utc=coalesce(submitted_at_utc,v_workflow.worker_submitted_at_utc),
          manager_approved_at_utc=v_workflow.manager_approved_at_utc,
          refusal_kind=case when v_lifecycle='MANAGER_REFUSED' then 'MANAGER_REFUSAL'
            when v_lifecycle='OFFICE_REJECTED' then 'AGENCY_REJECTION' else null end,
          refusal_reason=case when v_lifecycle in ('MANAGER_REFUSED','OFFICE_REJECTED')
            then coalesce(v_approval.refusal_reason,v_workflow.rejection_reason,'No reason recorded') else null end,
          refused_at_utc=case when v_lifecycle in ('MANAGER_REFUSED','OFFICE_REJECTED')
            then coalesce(v_approval.refused_at_utc,v_workflow.updated_at_utc) else null end,
          removed_at_utc=case when v_lifecycle in ('OFFICE_REJECTED','CANCELLED','SUPERSEDED')
            then v_workflow.updated_at_utc else null end,
          updated_at_utc=p_now_utc
        where expense_component_id=v_component.expense_component_id
        returning * into v_component;
      end if;
    end if;
    if v_changed then
      insert into public.candidate_expense_component_events(
        expense_component_id,workflow_id,component_generation,event_type,actor_kind,
        before_state_json,after_state_json,idempotency_key,occurred_at_utc
      ) values (
        v_component.expense_component_id,v_workflow.id,v_component.component_generation,
        case v_component.lifecycle_state
          when 'MANAGER_APPROVED' then 'MANAGER_APPROVED'
          when 'MANAGER_REFUSED' then 'MANAGER_REFUSED'
          when 'OFFICE_REJECTED' then 'OFFICE_REJECTED'
          when 'SUBMITTED' then 'SUBMITTED'
          when 'CANCELLED' then 'CANCELLED'
          when 'SUPERSEDED' then 'SUPERSEDED'
          else 'CREATED' end,
        'SYSTEM',coalesce(v_before,'{}'::jsonb),to_jsonb(v_component),
        'component-sync:'||v_component.expense_component_id::text||':'
          ||v_component.component_generation::text,p_now_utc
      ) on conflict(expense_component_id,idempotency_key) do nothing;
    end if;
    v_count:=v_count+1;
  end loop;
  return jsonb_build_object('ok',true,'workflow_id',v_workflow.id,'component_count',v_count);
end;
$function$;

create or replace function private._candidate_expense_workflow_sync_trigger_v1()
returns trigger
language plpgsql
security definer
set search_path = pg_catalog, private, pg_temp
as $function$
begin
  if new.workflow_kind in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
     and new.immutable_submission_json is not null
     and not exists(
       select 1 from public.candidate_pending_expense_updates update_row
       where update_row.workflow_id=new.id and update_row.state in ('EDITING','RENDERING')
     ) then
    perform private._candidate_expense_components_sync_v1(new.id,pg_catalog.transaction_timestamp());
  end if;
  return new;
end;
$function$;

drop trigger if exists candidate_expense_workflow_component_sync
  on public.candidate_submission_workflows;
create trigger candidate_expense_workflow_component_sync
after insert or update of state,generation,target_timesheet_id,immutable_submission_json,
  manager_approved_at_utc,rejection_reason
on public.candidate_submission_workflows
for each row execute function private._candidate_expense_workflow_sync_trigger_v1();

-- Financial recalculation is not a safe category-edit authority: several
-- independently approved workflows may legitimately resolve to one displayed
-- Timesheet.  This set-wise sync therefore updates protection state only.
-- Category amount/evidence edits use the explicit workflow/component RPC
-- further below, which cannot smear one Timesheet-wide total across claims.
create or replace function private._candidate_expense_components_sync_financial_v1(
  p_timesheet_id uuid,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_fin public.timesheets_financials%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_component public.candidate_expense_components%rowtype;
  v_before jsonb;
  v_agency text;
  v_count integer:=0;
begin
  select * into v_timesheet from public.timesheets row
  where row.timesheet_id=p_timesheet_id;
  select * into v_fin from public.timesheets_financials row
  where row.timesheet_id=p_timesheet_id and row.is_current
  order by row.computed_at_utc desc nulls last,row.updated_at desc,row.id desc limit 1;
  if not found or v_timesheet.timesheet_id is null then
    return jsonb_build_object('ok',true,'component_count',0);
  end if;
  v_agency:=case
    when v_fin.paid_at_utc is not null or upper(coalesce(v_timesheet.status::text,''))='PAID'
      then 'PAID'
    when v_fin.locked_by_invoice_id is not null
      or upper(coalesce(v_timesheet.status::text,''))='INVOICED' then 'INVOICED'
    when v_fin.authorised_at_utc is not null or v_timesheet.authorised_at_server is not null
      or upper(coalesce(v_timesheet.status::text,'')) in ('AUTHORISED','AUTHORIZED')
      then 'AUTHORISED'
    else 'NOT_AUTHORISED' end;
  for v_component in
    select component.*
    from public.candidate_expense_components component
    where component.lifecycle_state not in (
      'MANAGER_REFUSED','OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED'
    )
      and private._candidate_expense_owned_timesheet_id_v1(
        component.workflow_id,component.owning_timesheet_id
      )=p_timesheet_id
    order by component.expense_component_id
    for update
  loop
    if v_component.agency_authorisation_state is distinct from v_agency
       or v_component.owning_timesheet_id is distinct from p_timesheet_id then
      v_before:=to_jsonb(v_component);
      update public.candidate_expense_components set
        component_generation=component_generation+1,
        owning_timesheet_id=p_timesheet_id,
        agency_authorisation_state=v_agency,
        updated_at_utc=p_now_utc
      where expense_component_id=v_component.expense_component_id
      returning * into v_component;
      insert into public.candidate_expense_component_events(
        expense_component_id,workflow_id,component_generation,event_type,actor_kind,
        before_state_json,after_state_json,idempotency_key,occurred_at_utc
      ) values (
        v_component.expense_component_id,v_component.workflow_id,
        v_component.component_generation,
        case when v_before->>'agency_authorisation_state' is not distinct from v_agency
          then 'OWNER_ROTATED'
          else case v_agency when 'PAID' then 'PAID' when 'INVOICED' then 'INVOICED'
            when 'AUTHORISED' then 'AUTHORISED' else 'AGENCY_PROTECTION_CLEARED' end end,
        'SYSTEM',v_before,to_jsonb(v_component),
        'financial-protection-sync:'||v_fin.id::text||':'
          ||v_component.expense_component_id::text||':'||v_agency||':'
          ||p_timesheet_id::text,p_now_utc
      ) on conflict(expense_component_id,idempotency_key) do nothing;
      v_count:=v_count+1;
    end if;
  end loop;
  return jsonb_build_object('ok',true,'timesheet_id',p_timesheet_id,
    'component_count',v_count);
end;
$function$;

-- Evidence counts are part of the immutable unsigned Expense Summary input.
-- A same-value receipt replacement must therefore supersede the old summary
-- even when no financial column changes.  Summary evidence itself is excluded
-- to avoid a render-completion recursion.
create or replace function private._candidate_expense_evidence_summary_trigger_v1()
returns trigger
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_old_timesheet_id uuid;
  v_new_timesheet_id uuid;
  v_suppressed boolean:=false;
begin
  begin
    v_suppressed:=coalesce(current_setting(
      'cloudtms.candidate_expense_summary_suppressed',true
    ),'false')='true';
  exception when others then
    v_suppressed:=false;
  end;
  if v_suppressed then
    return coalesce(new,old);
  end if;
  if tg_op<>'INSERT'
     and old.document_role<>'EXPENSE_MILEAGE_APPROVAL_SUMMARY' then
    v_old_timesheet_id:=old.timesheet_id;
  end if;
  if tg_op<>'DELETE'
     and new.document_role<>'EXPENSE_MILEAGE_APPROVAL_SUMMARY' then
    v_new_timesheet_id:=new.timesheet_id;
  end if;
  if v_old_timesheet_id is not null then
    perform private._candidate_expense_summary_queue_v1(
      v_old_timesheet_id,pg_catalog.transaction_timestamp()
    );
  end if;
  if v_new_timesheet_id is not null
     and v_new_timesheet_id is distinct from v_old_timesheet_id then
    perform private._candidate_expense_summary_queue_v1(
      v_new_timesheet_id,pg_catalog.transaction_timestamp()
    );
  end if;
  return coalesce(new,old);
end;
$function$;

drop trigger if exists candidate_expense_evidence_summary_refresh
  on public.timesheet_evidence;
create trigger candidate_expense_evidence_summary_refresh
after insert or update of timesheet_id,kind,document_role,candidate_component_id,
  processing_state or delete
on public.timesheet_evidence
for each row execute function private._candidate_expense_evidence_summary_trigger_v1();

create or replace function private._candidate_expense_financial_sync_trigger_v1()
returns trigger
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
begin
  if new.is_current then
    if current_setting('cloudtms.candidate_expense_component_mutation',true) is null then
      perform private._candidate_expense_components_sync_financial_v1(
        new.timesheet_id,pg_catalog.transaction_timestamp()
      );
    end if;
    -- This trigger is already column-bounded. Queue on every invocation so a
    -- candidate/client identity move is as fresh as a monetary change; the
    -- queue digest makes an unchanged effective input a no-op.
    perform private._candidate_expense_summary_queue_v1(
      new.timesheet_id,pg_catalog.transaction_timestamp()
    );
  end if;
  return new;
end;
$function$;

drop trigger if exists candidate_expense_financial_component_sync
  on public.timesheets_financials;
create trigger candidate_expense_financial_component_sync
after insert or update of is_current,mileage_units,mileage_pay_ex_vat,travel_pay_ex_vat,
  accommodation_pay_ex_vat,other_pay_ex_vat,candidate_id,client_id,
  authorised_at_utc,locked_by_invoice_id,paid_at_utc
on public.timesheets_financials
for each row execute function private._candidate_expense_financial_sync_trigger_v1();

-- Agency protection can change on the Timesheet lifecycle row without a
-- financial-row write.  Keep the stable component authority in step with that
-- exact current Timesheet so list/detail projections never offer an action
-- after authorisation merely because the financial trigger did not fire.
create or replace function private._candidate_expense_timesheet_sync_trigger_v1()
returns trigger
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
begin
  if tg_op='UPDATE' and old.timesheet_id is distinct from new.timesheet_id then
    raise exception 'CANDIDATE_TIMESHEET_ID_MUTATION_NOT_SUPPORTED' using errcode='55000';
  end if;
  if new.is_current and new.archived_at_utc is null then
    perform private._candidate_expense_components_sync_financial_v1(
      new.timesheet_id,pg_catalog.transaction_timestamp()
    );
    perform private._candidate_expense_summary_queue_v1(
      new.timesheet_id,pg_catalog.transaction_timestamp()
    );
  end if;
  return new;
end;
$function$;

drop trigger if exists candidate_expense_timesheet_component_sync
  on public.timesheets;
create trigger candidate_expense_timesheet_component_sync
after insert or update of status,authorised_at_server,is_current,archived_at_utc,
  booking_id,contract_id,week_ending_date
on public.timesheets
for each row execute function private._candidate_expense_timesheet_sync_trigger_v1();

-- A READY summary retains the human-readable Candidate/client/contract
-- identity required by the document policy. Those source labels may be
-- corrected without touching a financial row, so queue every affected live
-- expense summary through one bounded identity trigger.
create or replace function private._candidate_expense_identity_summary_trigger_v1()
returns trigger
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_timesheet_id uuid;
begin
  for v_timesheet_id in
    select distinct financial.timesheet_id
    from public.timesheets_financials financial
    join public.timesheets timesheet on timesheet.timesheet_id=financial.timesheet_id
    where financial.is_current and timesheet.is_current
      and timesheet.archived_at_utc is null
      and case tg_table_name
        when 'candidates' then financial.candidate_id=new.id
        when 'clients' then financial.client_id=new.id
        when 'contracts' then timesheet.contract_id=new.id
        else false end
      and (
        coalesce(financial.mileage_units,0)<>0
        or coalesce(financial.mileage_pay_ex_vat,0)<>0
        or coalesce(financial.travel_pay_ex_vat,0)<>0
        or coalesce(financial.accommodation_pay_ex_vat,0)<>0
        or coalesce(financial.other_pay_ex_vat,0)<>0
        or exists(select 1 from public.candidate_expense_summary_refreshes refresh
          where refresh.timesheet_id=financial.timesheet_id)
      )
  loop
    perform private._candidate_expense_summary_queue_v1(
      v_timesheet_id,pg_catalog.transaction_timestamp()
    );
  end loop;
  return new;
end;
$function$;

drop trigger if exists candidate_expense_candidate_identity_summary_refresh
  on public.candidates;
create trigger candidate_expense_candidate_identity_summary_refresh
after update of display_name,first_name,last_name,tms_ref on public.candidates
for each row execute function private._candidate_expense_identity_summary_trigger_v1();
drop trigger if exists candidate_expense_client_identity_summary_refresh
  on public.clients;
create trigger candidate_expense_client_identity_summary_refresh
after update of name,cli_ref on public.clients
for each row execute function private._candidate_expense_identity_summary_trigger_v1();
drop trigger if exists candidate_expense_contract_identity_summary_refresh
  on public.contracts;
create trigger candidate_expense_contract_identity_summary_refresh
after update of role,display_site on public.contracts
for each row execute function private._candidate_expense_identity_summary_trigger_v1();

create or replace function private._candidate_expense_current_timesheet_id_v1(
  p_workflow_id uuid,
  p_stored_timesheet_id uuid
)
returns uuid
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_workflow public.candidate_submission_workflows%rowtype;
  v_stored public.timesheets%rowtype;
  v_result uuid;
  v_count integer;
begin
  select * into v_workflow from public.candidate_submission_workflows
  where id=p_workflow_id;
  if not found then return null; end if;
  if v_workflow.target_timesheet_id is not null and exists(
    select 1 from public.timesheets current_row
    where current_row.timesheet_id=v_workflow.target_timesheet_id
      and current_row.is_current and current_row.archived_at_utc is null
  ) then
    return v_workflow.target_timesheet_id;
  end if;
  if p_stored_timesheet_id is not null and exists(
    select 1 from public.timesheets current_row
    where current_row.timesheet_id=p_stored_timesheet_id
      and current_row.is_current and current_row.archived_at_utc is null
  ) then
    return p_stored_timesheet_id;
  end if;
  select * into v_stored from public.timesheets old_row
  where old_row.timesheet_id=coalesce(p_stored_timesheet_id,v_workflow.target_timesheet_id,
    v_workflow.anchor_timesheet_id);
  if found and nullif(btrim(coalesce(v_stored.booking_id,'')),'') is not null then
    select count(*)::integer,
      case when count(*)=1 then min(candidate.timesheet_id::text)::uuid end
    into v_count,v_result
    from public.timesheets candidate
    where candidate.booking_id=v_stored.booking_id
      and candidate.contract_id is not distinct from v_workflow.contract_id
      and candidate.week_ending_date is not distinct from v_workflow.week_ending_date
      and candidate.is_current and candidate.archived_at_utc is null;
    if v_count=1 then return v_result; end if;
  end if;
  return null;
end;
$function$;

-- Resolve only an expense component's own Timesheet identity.  Unlike the
-- presentation-anchor resolver above, this function never falls back to the
-- worked Timesheet merely because an expense workflow has no carrier yet.
create or replace function private._candidate_expense_owned_timesheet_id_v1(
  p_workflow_id uuid,
  p_stored_timesheet_id uuid
)
returns uuid
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_workflow public.candidate_submission_workflows%rowtype;
  v_stored public.timesheets%rowtype;
  v_result uuid;
  v_count integer;
begin
  if p_stored_timesheet_id is null then return null; end if;
  select * into v_workflow from public.candidate_submission_workflows
  where id=p_workflow_id;
  if not found then return null; end if;
  select * into v_stored from public.timesheets old_row
  where old_row.timesheet_id=p_stored_timesheet_id;
  if not found then return null; end if;
  if v_stored.is_current and v_stored.archived_at_utc is null then
    return v_stored.timesheet_id;
  end if;
  if nullif(btrim(coalesce(v_stored.booking_id,'')),'') is null then
    return null;
  end if;
  select count(*)::integer,
    case when count(*)=1 then min(candidate.timesheet_id::text)::uuid end
  into v_count,v_result
  from public.timesheets candidate
  where candidate.booking_id=v_stored.booking_id
    and candidate.contract_id is not distinct from v_workflow.contract_id
    and candidate.week_ending_date is not distinct from v_workflow.week_ending_date
    and candidate.is_current and candidate.archived_at_utc is null;
  if v_count=1 then return v_result; end if;
  return null;
end;
$function$;

-- Lock the complete mutable authority used by an expense-category
-- confirmation before its digest is re-evaluated.  The parent Timesheet lock
-- also fences new FK-backed financial/evidence/component rows, while the
-- ordered child locks make existing edits visible before the final context is
-- calculated.  This is mutation-only; read projections remain non-blocking.
create or replace function private._candidate_expense_owner_context_lock_v1(
  p_expense_component_id uuid
)
returns uuid
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_component public.candidate_expense_components%rowtype;
  v_timesheet_id uuid;
begin
  select component.* into v_component
  from public.candidate_expense_components component
  where component.expense_component_id=p_expense_component_id
  for update;
  if not found then
    raise exception 'CANDIDATE_EXPENSE_COMPONENT_NOT_FOUND' using errcode='P0002';
  end if;
  v_timesheet_id:=private._candidate_expense_owned_timesheet_id_v1(
    v_component.workflow_id,v_component.owning_timesheet_id
  );
  if v_timesheet_id is null then return null; end if;

  perform 1 from public.timesheets timesheet
  where timesheet.timesheet_id=v_timesheet_id for update;
  perform 1 from public.timesheets_financials financial
  where financial.timesheet_id=v_timesheet_id
  order by financial.id for update;
  perform 1 from public.timesheet_evidence evidence
  where evidence.timesheet_id=v_timesheet_id
  order by evidence.id for update;
  perform 1 from public.candidate_expense_components component
  where component.owning_timesheet_id=v_timesheet_id
  order by component.expense_component_id for update;
  perform 1 from public.candidate_submission_components evidence
  where evidence.workflow_id=v_component.workflow_id
  order by evidence.id for update;
  return v_timesheet_id;
end;
$function$;

create or replace function private._candidate_expense_component_status_v1(
  p_lifecycle_state text,
  p_manager_state text,
  p_agency_state text
)
returns text
language sql
immutable
parallel safe
set search_path = pg_catalog, pg_temp
as $function$
  select case
    when p_lifecycle_state='MANAGER_REFUSED' then 'MANAGER_REFUSED'
    when p_lifecycle_state='OFFICE_REJECTED' then 'AGENCY_REJECTED'
    when p_lifecycle_state='WITHDRAWN' then 'WITHDRAWN'
    when p_lifecycle_state='CANCELLED' then 'CANCELLED'
    when p_lifecycle_state='SUPERSEDED' then 'SUPERSEDED'
    when p_agency_state='PAID' then 'PAID'
    when p_agency_state='INVOICED' then 'INVOICED'
    when p_agency_state='AUTHORISED' then 'AGENCY_AUTHORISED'
    when p_lifecycle_state='MANAGER_APPROVED' or p_manager_state='APPROVED'
      then 'MANAGER_APPROVED'
    when p_manager_state='PENDING' then 'SUBMITTED'
    when p_lifecycle_state='SUBMITTED' then 'MANAGER_APPROVAL_REQUIRED'
    else 'DRAFT' end
$function$;

-- Manager decisions need a stable, non-displayed identity even when there is
-- no electronic approval-request row.  EMAIL/PHONE uses that exact request
-- UUID.  A completed PAPER/QR return has one deterministic workflow-generation
-- decision identity; it is derived only after the server has accepted that
-- returned pack as manager-approved.
create or replace function private._candidate_manager_decision_id_v1(
  p_workflow_id uuid,
  p_workflow_generation integer,
  p_approval_request_id uuid,
  p_manager_state text
)
returns uuid
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_workflow public.candidate_submission_workflows%rowtype;
  v_hex text;
begin
  -- A pending approval request is a request identity, not a completed manager
  -- decision.  Expose a decision identity only after an actual approve/refuse
  -- outcome; this also prevents a cancelled pending request from being
  -- misrepresented as a manager decision after a category is removed.
  if p_approval_request_id is not null
     and p_manager_state in ('APPROVED','REFUSED') then
    return p_approval_request_id;
  end if;
  if p_workflow_id is null or coalesce(p_workflow_generation,0)<1
     or p_manager_state<>'APPROVED' then return null; end if;
  select workflow.* into v_workflow
  from public.candidate_submission_workflows workflow
  where workflow.id=p_workflow_id;
  if not found or v_workflow.route<>'PAPER'
     or v_workflow.state not in ('RECEIVED','FINALISED') then return null; end if;
  v_hex:=encode(extensions.digest(pg_catalog.convert_to(
    'CANDIDATE_PAPER_MANAGER_DECISION_V1|'||p_workflow_id::text||'|'
      ||p_workflow_generation::text,'UTF8'
  ),'sha256'),'hex');
  -- Force the RFC 4122 version and variant bits so every deterministic PAPER
  -- decision identity is accepted by the same UUID validators as stored IDs.
  return (substring(v_hex from 1 for 8)||'-'||substring(v_hex from 9 for 4)
    ||'-5'||substring(v_hex from 14 for 3)||'-8'||substring(v_hex from 18 for 3)
    ||'-'||substring(v_hex from 21 for 12))::uuid;
end;
$function$;

create or replace function private._candidate_expense_unmaterialised_prior_v1(
  p_expense_component_id uuid
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_component public.candidate_expense_components%rowtype;
  v_update public.candidate_pending_expense_updates%rowtype;
  v_change jsonb;
  v_before jsonb;
begin
  select component.* into v_component
  from public.candidate_expense_components component
  where component.expense_component_id=p_expense_component_id;
  if not found then return jsonb_build_object('allowed',false); end if;
  select update_row.* into v_update
  from public.candidate_pending_expense_updates update_row
  where update_row.workflow_id=v_component.workflow_id
    and update_row.state='COMMITTED'
    and update_row.current_workflow_generation<=v_component.workflow_generation
    and update_row.completed_at_utc>=coalesce(
      v_component.submitted_at_utc,'-infinity'::timestamptz
    )
    and exists(
      select 1 from jsonb_array_elements(update_row.update_plan_json) change(value)
      where change.value->>'expense_category'=v_component.expense_category
        and change.value->>'update_kind' in ('ADD_CATEGORY','REPLACE_CATEGORY')
    )
  -- The earliest edit in the current submitted sequence owns the still-
  -- materialised baseline.  Later same-link replacements build on the stable
  -- component ledger while the Timesheet financial row intentionally remains
  -- at that first pre-edit value until manager finalisation.
  order by update_row.completed_at_utc asc nulls last,update_row.update_id asc
  limit 1;
  if not found then return jsonb_build_object('allowed',false); end if;
  select change.value into v_change
  from jsonb_array_elements(v_update.update_plan_json) change(value)
  where change.value->>'expense_category'=v_component.expense_category
    and change.value->>'update_kind' in ('ADD_CATEGORY','REPLACE_CATEGORY')
  limit 1;
  if v_change->>'update_kind'='ADD_CATEGORY' then
    return jsonb_build_object(
      'allowed',true,'update_id',v_update.update_id,'update_kind','ADD_CATEGORY',
      'amount',0,'mileage_units',0
    );
  end if;
  select event.before_state_json into v_before
  from public.candidate_expense_component_events event
  where event.expense_component_id=v_component.expense_component_id
    and event.event_type='REPLACED'
    and event.idempotency_key='expense-update-replace:'||v_update.update_id::text||':'
      ||v_component.expense_component_id::text
  order by event.occurred_at_utc desc,event.event_id desc limit 1;
  if v_before is null then return jsonb_build_object('allowed',false); end if;
  return jsonb_build_object(
    'allowed',true,'update_id',v_update.update_id,'update_kind','REPLACE_CATEGORY',
    'amount',coalesce((v_before->>'amount')::numeric,0),
    'mileage_units',coalesce((v_before->>'mileage_units')::numeric,0)
  );
exception when invalid_text_representation or numeric_value_out_of_range then
  return jsonb_build_object('allowed',false);
end;
$function$;

create or replace function private._candidate_expense_financial_remove_v1(
  p_expense_component_id uuid,
  p_expected_component_generation integer,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_component public.candidate_expense_components%rowtype;
  v_timesheet_id uuid;
  v_timesheet public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_mileage_units numeric;
  v_mileage_pay numeric;
  v_mileage_charge numeric;
  v_travel_pay numeric;
  v_travel_charge numeric;
  v_accommodation_pay numeric;
  v_accommodation_charge numeric;
  v_other_pay numeric;
  v_other_charge numeric;
  v_expenses_pay numeric;
  v_expenses_charge numeric;
  v_total_pay numeric;
  v_total_charge numeric;
  v_remaining_expense_value numeric;
  v_owner_category_count integer;
  v_financial_category_amount numeric;
  v_surviving_description text;
  v_unmaterialised_prior jsonb;
  v_prior_amount numeric;
  v_prior_units numeric;
begin
  select * into v_component from public.candidate_expense_components component
  where component.expense_component_id=p_expense_component_id for update;
  if not found then raise exception 'CANDIDATE_EXPENSE_COMPONENT_NOT_FOUND' using errcode='P0002'; end if;
  if v_component.component_generation<>p_expected_component_generation then
    raise exception 'CANDIDATE_EXPENSE_COMPONENT_CHANGED' using errcode='40001';
  end if;
  v_timesheet_id:=private._candidate_expense_owner_context_lock_v1(
    v_component.expense_component_id
  );
  if v_timesheet_id is null then
    if v_component.owning_timesheet_id is null then
      return jsonb_build_object('ok',true,'timesheet_id',null,
        'financial_changed',false,'zero_expense_carrier',false);
    end if;
    raise exception 'CANDIDATE_EXPENSE_OWNING_TIMESHEET_CHANGED' using errcode='40001';
  end if;
  select * into v_timesheet from public.timesheets row
  where row.timesheet_id=v_timesheet_id and row.is_current
    and row.archived_at_utc is null for update;
  if not found then
    raise exception 'CANDIDATE_EXPENSE_OWNING_TIMESHEET_CHANGED' using errcode='40001';
  end if;
  select * into v_fin from public.timesheets_financials row
  where row.timesheet_id=v_timesheet_id and row.is_current
  order by row.computed_at_utc desc nulls last,row.updated_at desc,row.id desc
  limit 1 for update;
  if not found then raise exception 'CANDIDATE_EXPENSE_FINANCIALS_NOT_FOUND' using errcode='P0002'; end if;
  select count(*)::integer into v_owner_category_count
  from public.candidate_expense_components other_component
  where other_component.expense_category=v_component.expense_category
    and other_component.lifecycle_state not in (
      'MANAGER_REFUSED','OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED'
    )
    and private._candidate_expense_owned_timesheet_id_v1(
      other_component.workflow_id,other_component.owning_timesheet_id
    )=v_timesheet_id;
  v_financial_category_amount:=case v_component.expense_category
    when 'MILEAGE' then coalesce(v_fin.mileage_pay_ex_vat,0)
    when 'TRAVEL' then coalesce(v_fin.travel_pay_ex_vat,0)
    when 'ACCOMMODATION' then coalesce(v_fin.accommodation_pay_ex_vat,0)
    else coalesce(v_fin.other_pay_ex_vat,0) end;
  v_unmaterialised_prior:=private._candidate_expense_unmaterialised_prior_v1(
    v_component.expense_component_id
  );
  v_prior_amount:=case when coalesce((v_unmaterialised_prior->>'allowed')::boolean,false)
    then (v_unmaterialised_prior->>'amount')::numeric end;
  v_prior_units:=case when coalesce((v_unmaterialised_prior->>'allowed')::boolean,false)
    then (v_unmaterialised_prior->>'mileage_units')::numeric end;
  if v_owner_category_count<>1
     or not (
       v_financial_category_amount is not distinct from v_component.amount
       or v_financial_category_amount is not distinct from v_prior_amount
     )
     or (v_component.expense_category='MILEAGE'
       and not (
         coalesce(v_fin.mileage_units,0) is not distinct from v_component.mileage_units
         or coalesce(v_fin.mileage_units,0) is not distinct from v_prior_units
       )) then
    raise exception 'CANDIDATE_EXPENSE_CATEGORY_OWNERSHIP_AMBIGUOUS'
      using errcode='55000',detail=jsonb_build_object(
        'expense_component_id',v_component.expense_component_id,
        'owning_timesheet_id',v_timesheet_id,
        'live_owner_category_count',v_owner_category_count
      )::text;
  end if;
  if v_timesheet.authorised_at_server is not null
     or upper(coalesce(v_timesheet.status::text,'')) in ('AUTHORISED','AUTHORIZED','INVOICED','PAID')
     or v_fin.authorised_at_utc is not null or v_fin.locked_by_invoice_id is not null
     or v_fin.paid_at_utc is not null then
    raise exception 'CANDIDATE_EXPENSE_COMPONENT_PROTECTED' using errcode='55000';
  end if;
  v_mileage_units:=case when v_component.expense_category='MILEAGE'
    then 0 else coalesce(v_fin.mileage_units,0) end;
  v_mileage_pay:=case when v_component.expense_category='MILEAGE'
    then 0 else coalesce(v_fin.mileage_pay_ex_vat,0) end;
  v_mileage_charge:=case when v_component.expense_category='MILEAGE'
    then 0 else coalesce(v_fin.mileage_charge_ex_vat,0) end;
  v_travel_pay:=case when v_component.expense_category='TRAVEL'
    then 0 else coalesce(v_fin.travel_pay_ex_vat,0) end;
  v_travel_charge:=case when v_component.expense_category='TRAVEL'
    then 0 else coalesce(v_fin.travel_charge_ex_vat,0) end;
  v_accommodation_pay:=case when v_component.expense_category='ACCOMMODATION'
    then 0 else coalesce(v_fin.accommodation_pay_ex_vat,0) end;
  v_accommodation_charge:=case when v_component.expense_category='ACCOMMODATION'
    then 0 else coalesce(v_fin.accommodation_charge_ex_vat,0) end;
  v_other_pay:=case when v_component.expense_category='OTHER'
    then 0 else coalesce(v_fin.other_pay_ex_vat,0) end;
  v_other_charge:=case when v_component.expense_category='OTHER'
    then 0 else coalesce(v_fin.other_charge_ex_vat,0) end;
  v_expenses_pay:=v_travel_pay+v_accommodation_pay+v_other_pay;
  v_expenses_charge:=v_travel_charge+v_accommodation_charge+v_other_charge;
  v_total_pay:=coalesce(v_fin.pay_day,0)+coalesce(v_fin.pay_night,0)
    +coalesce(v_fin.pay_sat,0)+coalesce(v_fin.pay_sun,0)+coalesce(v_fin.pay_bh,0)
    +coalesce(v_fin.additional_pay_ex_vat,0)+v_mileage_pay+v_expenses_pay;
  v_total_charge:=coalesce(v_fin.charge_day,0)+coalesce(v_fin.charge_night,0)
    +coalesce(v_fin.charge_sat,0)+coalesce(v_fin.charge_sun,0)+coalesce(v_fin.charge_bh,0)
    +coalesce(v_fin.additional_charge_ex_vat,0)+v_mileage_charge+v_expenses_charge;
  v_surviving_description:=nullif(concat_ws('; ',
    case when v_travel_pay<>0 then 'Travel £'||to_char(v_travel_pay,'FM999999999990.00') end,
    case when v_accommodation_pay<>0 then
      'Accommodation £'||to_char(v_accommodation_pay,'FM999999999990.00') end,
    case when v_other_pay<>0 then 'Other £'||to_char(v_other_pay,'FM999999999990.00') end
  ),'');
  perform set_config(
    'cloudtms.candidate_expense_component_mutation',
    v_component.expense_component_id::text,true
  );
  update public.timesheets_financials set
    mileage_units=v_mileage_units,mileage_pay_ex_vat=v_mileage_pay,
    mileage_charge_ex_vat=v_mileage_charge,
    mileage_evidence_r2_key=case when v_component.expense_category='MILEAGE'
      then null else mileage_evidence_r2_key end,
    mileage_evidence_manifest=case when v_component.expense_category='MILEAGE'
      then null else mileage_evidence_manifest end,
    travel_pay_ex_vat=v_travel_pay,travel_charge_ex_vat=v_travel_charge,
    accommodation_pay_ex_vat=v_accommodation_pay,
    accommodation_charge_ex_vat=v_accommodation_charge,
    other_pay_ex_vat=v_other_pay,other_charge_ex_vat=v_other_charge,
    expenses_pay_ex_vat=v_expenses_pay,expenses_charge_ex_vat=v_expenses_charge,
    -- These are legacy aggregate pointers and cannot identify which category
    -- a page belongs to.  Invalidate them after any non-mileage category
    -- change; category-exact current evidence remains in timesheet_evidence.
    expenses_evidence_r2_key=case when v_component.expense_category<>'MILEAGE'
      then null else expenses_evidence_r2_key end,
    expenses_evidence_manifest=case when v_component.expense_category<>'MILEAGE'
      then null else expenses_evidence_manifest end,
    -- The legacy description is one undifferentiated note. Rebuild it from
    -- the surviving category authority so removed wording cannot remain and
    -- a surviving category is never silently erased from legacy displays.
    expenses_description=v_surviving_description,
    total_pay_ex_vat=v_total_pay,total_charge_ex_vat=v_total_charge,
    margin_ex_vat=v_total_charge-v_total_pay,is_stale=true,
    stale_reason='CANDIDATE_EXPENSE_COMPONENT_REMOVED',updated_at=p_now_utc
  where id=v_fin.id and is_current;
  if not found then raise exception 'CANDIDATE_EXPENSE_FINANCIALS_CHANGED' using errcode='40001'; end if;
  update public.timesheet_evidence evidence set processing_state='SUPERSEDED'
  where evidence.timesheet_id=v_timesheet_id
    and evidence.processing_state<>'SUPERSEDED'
    and evidence.document_role<>'EXPENSE_MILEAGE_APPROVAL_SUMMARY'
    and (
      upper(coalesce(evidence.kind::text,''))=v_component.expense_category
      or evidence.candidate_component_id in (
        select source.id from public.candidate_submission_components source
        where source.workflow_id=v_component.workflow_id
          and source.expense_category=v_component.expense_category
      )
    );
  v_remaining_expense_value:=abs(v_mileage_units)+abs(v_mileage_pay)
    +abs(v_mileage_charge)+abs(v_travel_pay)+abs(v_travel_charge)
    +abs(v_accommodation_pay)+abs(v_accommodation_charge)
    +abs(v_other_pay)+abs(v_other_charge);
  return jsonb_build_object(
    'ok',true,'timesheet_id',v_timesheet_id,'financial_changed',true,
    'remaining_expense_value',v_remaining_expense_value,
    'remaining_total_hours',coalesce(v_fin.total_hours,0),
    'remaining_additional_value',abs(coalesce(v_fin.additional_pay_ex_vat,0))
      +abs(coalesce(v_fin.additional_charge_ex_vat,0)),
    'zero_expense_carrier',v_remaining_expense_value=0
      and coalesce(v_fin.total_hours,0)=0
      and abs(coalesce(v_fin.additional_pay_ex_vat,0))
        +abs(coalesce(v_fin.additional_charge_ex_vat,0))=0
      and coalesce(v_fin.hours_day,0)=0 and coalesce(v_fin.hours_night,0)=0
      and coalesce(v_fin.hours_sat,0)=0 and coalesce(v_fin.hours_sun,0)=0
      and coalesce(v_fin.hours_bh,0)=0
      and coalesce(v_fin.pay_day,0)=0 and coalesce(v_fin.pay_night,0)=0
      and coalesce(v_fin.pay_sat,0)=0 and coalesce(v_fin.pay_sun,0)=0
      and coalesce(v_fin.pay_bh,0)=0
      and coalesce(v_fin.charge_day,0)=0 and coalesce(v_fin.charge_night,0)=0
      and coalesce(v_fin.charge_sat,0)=0 and coalesce(v_fin.charge_sun,0)=0
      and coalesce(v_fin.charge_bh,0)=0
      and v_total_pay=0 and v_total_charge=0 and v_total_charge-v_total_pay=0
      and coalesce(v_fin.pay_vat_amount_snapshot,0)=0
      and coalesce(v_fin.pay_total_inc_vat_snapshot,0)=0
      and v_timesheet.authorised_at_server is null
      and upper(coalesce(v_timesheet.status::text,'')) not in (
        'AUTHORISED','AUTHORIZED','INVOICED','PAID'
      )
      and v_timesheet.sheet_scope::text='WEEKLY'
      and coalesce(v_timesheet.is_adjustment,false)
      and upper(coalesce(v_timesheet.adjustment_origin,'')) not in (
        'IMPORT_CORRECTION','IMPORT_CANCELLATION'
      )
  );
end;
$function$;

create or replace function private._candidate_expense_component_action_v1(
  p_component public.candidate_expense_components
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_workflow public.candidate_submission_workflows%rowtype;
  v_timesheet_id uuid;
  v_timesheet public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_code text;
  v_slug text;
  v_label text;
begin
  select * into v_workflow from public.candidate_submission_workflows
  where id=p_component.workflow_id;
  if not found then return null; end if;
  -- A refused/rejected category is immutable history.  Its only corrective
  -- action starts a blank category claim; it never reactivates or copies the
  -- rejected values/evidence, and it remains available after an otherwise
  -- empty expense carrier has been safely deleted.
  if p_component.lifecycle_state in ('MANAGER_REFUSED','OFFICE_REJECTED')
     and p_component.agency_authorisation_state='NOT_AUTHORISED'
     and not exists(
       select 1
       from public.candidate_expense_components active_component
       join public.candidate_submission_workflows active_workflow
         on active_workflow.id=active_component.workflow_id
       left join public.candidate_approval_requests active_request
         on active_request.workflow_id=active_workflow.id
        and active_request.workflow_generation=active_workflow.generation
        and active_request.state='PENDING'
       where active_component.expense_component_id<>p_component.expense_component_id
         and active_component.expense_category=p_component.expense_category
         and active_component.lifecycle_state not in (
           'MANAGER_REFUSED','OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED'
         )
         and active_workflow.environment=v_workflow.environment
         and active_workflow.candidate_id=v_workflow.candidate_id
         and active_workflow.contract_id=v_workflow.contract_id
         and active_workflow.week_ending_date=v_workflow.week_ending_date
         and (
           (active_workflow.route<>'PAPER'
             and active_workflow.state='AWAITING_MANAGER_APPROVAL'
             and active_request.id is not null)
           or (active_workflow.route='PAPER'
             and active_workflow.state='AWAITING_PAPER_RETURN')
         )
     ) then
    v_code:='RESUBMIT_EXPENSE_CATEGORY';
    v_slug:='resubmit-expense-category';
    v_label:='Submit this expense again';
    return jsonb_build_object(
      'code',v_code,'label',v_label,'method','POST',
      'path','/candidate-app/v1/workflows/'||p_component.workflow_id::text||'/actions/'||v_slug,
      'requires_confirmation',false,'requires_reason',false,'enabled',true,
      'disabled_reason',null,'workflow_id',p_component.workflow_id,
      'workflow_generation',v_workflow.generation,
      -- The corrective action never needs the old request identity.  In
      -- particular, an Office-rejected pending category must not expose its
      -- cancelled request as though it were a manager decision.
      'approval_request_id',null,
      'timesheet_id',private._candidate_expense_owned_timesheet_id_v1(
        p_component.workflow_id,p_component.owning_timesheet_id
      ),'contract_week_id',v_workflow.contract_week_id,
      'invocation',jsonb_build_object(
        'version',1,'kind','HTTP','method','POST',
        'path','/candidate-app/v1/workflows/'||p_component.workflow_id::text
          ||'/actions/'||v_slug,
        'fixed_body',jsonb_build_object(
          'generation',v_workflow.generation,
          'expense_component_id',p_component.expense_component_id,
          'component_generation',p_component.component_generation
        ),
        'required_user_inputs','[{"name":"idempotency_key","type":"uuid","required":true}]'::jsonb,
        'idempotency','REQUIRED'
      )
    );
  end if;
  v_timesheet_id:=private._candidate_expense_owned_timesheet_id_v1(
    p_component.workflow_id,p_component.owning_timesheet_id
  );
  if coalesce(p_component.agency_authorisation_state,'NOT_AUTHORISED')<>'NOT_AUTHORISED'
     or (p_component.owning_timesheet_id is not null and v_timesheet_id is null) then
    return null;
  end if;
  if v_timesheet_id is not null then
    select * into v_timesheet from public.timesheets where timesheet_id=v_timesheet_id;
    select * into v_fin from public.timesheets_financials
    where timesheet_id=v_timesheet_id and is_current
    order by computed_at_utc desc nulls last,updated_at desc,id desc limit 1;
    if v_timesheet.authorised_at_server is not null
       or upper(coalesce(v_timesheet.status::text,'')) in ('AUTHORISED','AUTHORIZED','INVOICED','PAID')
       or v_fin.authorised_at_utc is not null or v_fin.locked_by_invoice_id is not null
       or v_fin.paid_at_utc is not null then
      return null;
    end if;
  end if;
  if p_component.lifecycle_state='DRAFT'
     and v_workflow.state in ('CREATED','WORKER_DRAFT') then
    v_code:='REMOVE_EXPENSE'; v_slug:='remove-expense'; v_label:='Remove expense';
  elsif p_component.lifecycle_state='SUBMITTED'
        and (
          p_component.manager_approval_state='PENDING'
          or (v_workflow.route='PAPER' and v_workflow.state='AWAITING_PAPER_RETURN'
            and p_component.manager_approval_state='NOT_REQUESTED')
        ) then
    if v_workflow.route='PAPER' and v_workflow.state='AWAITING_PAPER_RETURN'
       and p_component.manager_approval_state='NOT_REQUESTED' then
      -- The category action returns a non-mutating, closed replacement plan.
      -- Candidate then invokes CREATE_UPDATED_DOCUMENTS with that exact
      -- server-provided REMOVE_CATEGORY plan, so the category target is never
      -- inferred client-side and the complete pack is replaced atomically.
    end if;
    v_code:='WITHDRAW_EXPENSE'; v_slug:='withdraw-expense'; v_label:='Withdraw expense';
  elsif p_component.lifecycle_state='MANAGER_APPROVED'
        and p_component.manager_approval_state='APPROVED'
        and v_workflow.state='FINALISED' then
    v_code:='CANCEL_EXPENSE'; v_slug:='cancel-expense'; v_label:='Cancel expense';
  else
    return null;
  end if;
  return jsonb_build_object(
    'code',v_code,'label',v_label,'method','POST',
    'path','/candidate-app/v1/workflows/'||p_component.workflow_id::text||'/actions/'||v_slug,
    'requires_confirmation',true,'requires_reason',false,'enabled',true,
    'disabled_reason',null,'workflow_id',p_component.workflow_id,
    'workflow_generation',v_workflow.generation,'approval_request_id',p_component.approval_request_id,
    'timesheet_id',v_timesheet_id,'contract_week_id',v_workflow.contract_week_id,
    'invocation',jsonb_build_object(
      'version',1,'kind','HTTP','method','POST',
      'path','/candidate-app/v1/workflows/'||p_component.workflow_id::text||'/actions/'||v_slug,
      'fixed_body',jsonb_build_object(
        'generation',v_workflow.generation,
        'expense_component_id',p_component.expense_component_id,
        'component_generation',p_component.component_generation
      ),
      'required_user_inputs','[{"name":"idempotency_key","type":"uuid","required":true}]'::jsonb,
      'idempotency','REQUIRED'
    )
  );
end;
$function$;

create or replace function private._candidate_expense_component_json_v1(
  p_component public.candidate_expense_components,
  p_actions_allowed boolean default true
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_timesheet_id uuid;
  v_supporting_count integer;
  v_status text;
  v_included boolean;
begin
  v_timesheet_id:=private._candidate_expense_owned_timesheet_id_v1(
    p_component.workflow_id,p_component.owning_timesheet_id
  );
  select
    (select count(distinct coalesce(evidence.source_component_id,evidence.id))::integer
     from public.candidate_submission_components evidence
     where evidence.workflow_id=p_component.workflow_id
       and evidence.workflow_generation=p_component.workflow_generation
       and evidence.expense_category=p_component.expense_category
       and evidence.component_kind in ('MILEAGE_FORM','EXPENSE_EVIDENCE')
       and evidence.state not in ('SUPERSEDED','REJECTED','ABANDONED'))
    +(select count(*)::integer
     from public.timesheet_evidence evidence
     where evidence.timesheet_id=v_timesheet_id
       and evidence.processing_state<>'SUPERSEDED'
       and upper(coalesce(evidence.kind::text,''))=p_component.expense_category
       and evidence.document_role<>'EXPENSE_MILEAGE_APPROVAL_SUMMARY'
       and (
         evidence.candidate_component_id is null
         or not exists(
           select 1
           from public.candidate_submission_components source_component
           left join public.candidate_submission_components materialised_source
             on materialised_source.id=evidence.candidate_component_id
           where source_component.workflow_id=p_component.workflow_id
             and source_component.workflow_generation=p_component.workflow_generation
             and source_component.expense_category=p_component.expense_category
             and source_component.component_kind in ('MILEAGE_FORM','EXPENSE_EVIDENCE')
             and source_component.state not in ('SUPERSEDED','REJECTED','ABANDONED')
             and coalesce(source_component.source_component_id,source_component.id)
               =coalesce(materialised_source.source_component_id,
                 evidence.candidate_component_id)
         )
       ))
  into v_supporting_count;
  v_status:=private._candidate_expense_component_status_v1(
    p_component.lifecycle_state,p_component.manager_approval_state,
    p_component.agency_authorisation_state
  );
  v_included:=p_component.lifecycle_state not in (
    'MANAGER_REFUSED','OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED'
  );
  return jsonb_build_object(
    'workflow_id',p_component.workflow_id,
    'expense_component_id',p_component.expense_component_id,
    'component_generation',p_component.component_generation,
    'expense_category',p_component.expense_category,
    'amount',p_component.amount,'mileage_units',p_component.mileage_units,
    'supporting_evidence_count',v_supporting_count,
    'state',p_component.lifecycle_state,'status_code',v_status,
    'manager_approval_state',p_component.manager_approval_state,
    'manager_decision_id',private._candidate_manager_decision_id_v1(
      p_component.workflow_id,p_component.workflow_generation,
      p_component.approval_request_id,p_component.manager_approval_state
    ),
    'agency_authorisation_state',p_component.agency_authorisation_state,
    'owning_timesheet_id',v_timesheet_id,
    'submitted_at_utc',p_component.submitted_at_utc,
    'manager_approved_at_utc',p_component.manager_approved_at_utc,
    'refusal',case when p_component.refusal_kind is null then null else jsonb_build_object(
      'kind',p_component.refusal_kind,'reason',p_component.refusal_reason,
      'at_utc',p_component.refused_at_utc,
      'decision_id',case when p_component.refusal_kind='MANAGER_REFUSAL'
        then p_component.approval_request_id else null end
    ) end,
    'protected',p_component.agency_authorisation_state<>'NOT_AUTHORISED'
      or (p_component.owning_timesheet_id is not null and v_timesheet_id is null),
    'included_in_total',v_included,
    'available_action',case when p_actions_allowed
      then private._candidate_expense_component_action_v1(p_component) else null end
  );
end;
$function$;

create or replace function private._candidate_expense_begin_update_action_v1(
  p_workflow_id uuid,
  p_conflicted boolean
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare v_workflow public.candidate_submission_workflows%rowtype;
begin
  select * into v_workflow from public.candidate_submission_workflows
  where id=p_workflow_id;
  if not found then return null; end if;
  if not p_conflicted
     and v_workflow.workflow_kind in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
     and v_workflow.route='PAPER'
     and v_workflow.state='AWAITING_PAPER_RETURN' then
    return jsonb_build_object(
      'code','CREATE_UPDATED_DOCUMENTS','label','Create updated documents',
      'method','POST','path','/candidate-app/v1/workflows/'||v_workflow.id::text
        ||'/actions/create-updated-documents',
      'requires_confirmation',true,'requires_reason',false,'enabled',true,
      'disabled_reason',null,'workflow_id',v_workflow.id,
      'workflow_generation',v_workflow.generation,'approval_request_id',null,
      'timesheet_id',v_workflow.target_timesheet_id,
      'contract_week_id',v_workflow.contract_week_id,
      'invocation',jsonb_build_object(
        'version',1,'kind','HTTP','method','POST',
        'path','/candidate-app/v1/workflows/'||v_workflow.id::text
          ||'/actions/create-updated-documents',
        'fixed_body',jsonb_build_object('generation',v_workflow.generation),
        'required_user_inputs','[{"name":"category_changes","type":"array","required":true},{"name":"idempotency_key","type":"uuid","required":true}]'::jsonb,
        'idempotency','REQUIRED'
      )
    );
  end if;
  return case when p_conflicted
      or v_workflow.workflow_kind not in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
      or v_workflow.route='PAPER'
      or v_workflow.state<>'AWAITING_MANAGER_APPROVAL'
      or not exists(
        select 1 from public.candidate_approval_requests request
        where request.workflow_id=v_workflow.id
          and request.workflow_generation=v_workflow.generation
          and request.state='PENDING'
      )
    then null else jsonb_build_object(
      'code','BEGIN_EXPENSE_UPDATE','label','Update expenses','method','POST',
      'path','/candidate-app/v1/workflows/'||v_workflow.id::text||'/actions/begin-expense-update',
      'requires_confirmation',false,'requires_reason',false,'enabled',true,
      'disabled_reason',null,'workflow_id',v_workflow.id,
      'workflow_generation',v_workflow.generation,'approval_request_id',null,
      'timesheet_id',v_workflow.target_timesheet_id,'contract_week_id',v_workflow.contract_week_id,
      'invocation',jsonb_build_object(
        'version',1,'kind','HTTP','method','POST',
        'path','/candidate-app/v1/workflows/'||v_workflow.id::text||'/actions/begin-expense-update',
        'fixed_body',jsonb_build_object('generation',v_workflow.generation),
        'required_user_inputs','[{"name":"category_changes","type":"array","required":true},{"name":"idempotency_key","type":"uuid","required":true}]'::jsonb,
        'idempotency','REQUIRED'
      )
    ) end;
end;
$function$;

create or replace function private._candidate_whole_claim_scope_v1(
  p_environment text,
  p_candidate_id uuid,
  p_contract_id uuid,
  p_week_ending date
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_scope jsonb;
begin
  if p_candidate_id is null or p_contract_id is null or p_week_ending is null then
    raise exception 'CANDIDATE_WHOLE_CLAIM_SCOPE_INVALID' using errcode='22023';
  end if;
  with scoped as (
    select workflow.*,
      coalesce((
        select jsonb_agg(jsonb_build_object(
          'expense_component_id',component.expense_component_id,
          'component_generation',component.component_generation,
          'workflow_generation',component.workflow_generation,
          'expense_category',component.expense_category,
          'lifecycle_state',component.lifecycle_state,
          'manager_approval_state',component.manager_approval_state,
          'agency_authorisation_state',component.agency_authorisation_state,
          'owning_timesheet_id',private._candidate_expense_owned_timesheet_id_v1(
            component.workflow_id,component.owning_timesheet_id
          ),
          'amount',component.amount,'mileage_units',component.mileage_units
        ) order by component.expense_component_id)
        from public.candidate_expense_components component
        where component.workflow_id=workflow.id
          and component.lifecycle_state not in (
            'MANAGER_REFUSED','OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED'
          )
      ),'[]'::jsonb) as components,
      (
        select jsonb_build_object(
          'approval_request_id',request.id,
          'request_generation',request.request_generation,
          'workflow_generation',request.workflow_generation,
          'state',request.state
        )
        from public.candidate_approval_requests request
        where request.workflow_id=workflow.id
          and request.workflow_generation=case when workflow.state='FINALISED'
            then greatest(workflow.generation-1,1) else workflow.generation end
          and request.state in ('PENDING','APPROVED')
        order by request.request_generation desc,request.id desc limit 1
      ) as approval
    from public.candidate_submission_workflows workflow
    where workflow.environment=private._candidate_assert_environment(p_environment)
      and workflow.candidate_id=p_candidate_id
      and workflow.contract_id=p_contract_id
      and workflow.week_ending_date=p_week_ending
      and workflow.state not in ('CANCELLED','EXPIRED','SUPERSEDED','REJECTED','REFUSED')
  )
  select coalesce(jsonb_agg(jsonb_build_object(
    'workflow_id',scoped.id,'generation',scoped.generation,
    'state',scoped.state,'workflow_kind',scoped.workflow_kind,'route',scoped.route,
    'target_timesheet_id',private._candidate_expense_owned_timesheet_id_v1(
      scoped.id,scoped.target_timesheet_id
    ),'approval',scoped.approval,'components',scoped.components
  ) order by scoped.id),'[]'::jsonb)
  into v_scope from scoped;
  return jsonb_build_object(
    'scope',v_scope,
    'scope_sha256',encode(private._candidate_sha256_jsonb_v1(v_scope),'hex')
  );
end;
$function$;

create or replace function private._candidate_whole_claim_action_v1(
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
  v_timesheet public.timesheets%rowtype;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_contract_id uuid;
  v_week_ending date;
  v_candidate_id uuid;
  v_pending_count integer;
  v_approved_count integer;
  v_protected_count integer;
  v_invalid_component_count integer;
  v_conflict_count integer;
  v_code text;
  v_slug text;
  v_scope jsonb;
begin
  select * into v_timesheet from public.timesheets where timesheet_id=p_timesheet_id;
  if not found then return null; end if;
  select financial.candidate_id into v_candidate_id
  from public.timesheets_financials financial
  where financial.timesheet_id=p_timesheet_id and financial.is_current
  order by financial.computed_at_utc desc nulls last,financial.updated_at desc,financial.id desc
  limit 1;
  if v_candidate_id is null then return null; end if;
  v_contract_id:=v_timesheet.contract_id;
  v_week_ending:=v_timesheet.week_ending_date;
  v_scope:=private._candidate_whole_claim_scope_v1(
    p_environment,v_candidate_id,v_contract_id,v_week_ending
  );
  select count(distinct workflow.id) filter(where
      request.state='PENDING'
      or (workflow.route='PAPER' and workflow.state='AWAITING_PAPER_RETURN')
    )::integer,
    count(distinct workflow.id) filter(where
      request.state='APPROVED'
      or (workflow.route='PAPER' and workflow.state in ('RECEIVED','FINALISED'))
    )::integer,
    count(distinct workflow.id) filter(where
      (request.state='PENDING'
        or (workflow.route='PAPER' and workflow.state='AWAITING_PAPER_RETURN'))
      and workflow.workflow_kind in ('CONTRACT_EXPENSE','CONTRACT_COMBINED'))::integer
  into v_pending_count,v_approved_count,v_conflict_count
  from public.candidate_submission_workflows workflow
  left join public.candidate_approval_requests request
    on request.workflow_id=workflow.id and request.workflow_generation=case
      when workflow.state='FINALISED' then greatest(workflow.generation-1,1) else workflow.generation end
    and request.state in ('PENDING','APPROVED')
  where workflow.environment=private._candidate_assert_environment(p_environment)
    and workflow.candidate_id=v_candidate_id
    and workflow.contract_id=v_contract_id
    and workflow.week_ending_date=v_week_ending
    and workflow.state not in ('CANCELLED','EXPIRED','SUPERSEDED','REJECTED','REFUSED');
  if v_conflict_count>1 then return null; end if;
  select count(*)::integer into v_protected_count
  from public.candidate_submission_workflows workflow
  join public.timesheets linked_timesheet
    on linked_timesheet.timesheet_id=private._candidate_expense_owned_timesheet_id_v1(
      workflow.id,workflow.target_timesheet_id
    )
  left join public.timesheets_financials financial
    on financial.timesheet_id=linked_timesheet.timesheet_id and financial.is_current
  where workflow.environment=private._candidate_assert_environment(p_environment)
    and workflow.candidate_id=v_candidate_id
    and workflow.contract_id=v_contract_id and workflow.week_ending_date=v_week_ending
    and workflow.state not in ('CANCELLED','EXPIRED','SUPERSEDED','REJECTED','REFUSED')
    and (linked_timesheet.authorised_at_server is not null
      or upper(linked_timesheet.status::text) in ('AUTHORISED','AUTHORIZED','INVOICED','PAID')
      or financial.authorised_at_utc is not null or financial.locked_by_invoice_id is not null
      or financial.paid_at_utc is not null);
  if v_protected_count>0 then return null; end if;
  -- Component protection is independent of whether an historical/current
  -- expense owner can still be resolved.  A target-less protected component
  -- must suppress the projection just as the locked mutation suppresses it;
  -- ownership/financial parity is checked separately below.
  if exists(
    select 1
    from public.candidate_expense_components component
    join public.candidate_submission_workflows workflow
      on workflow.id=component.workflow_id
    where workflow.environment=private._candidate_assert_environment(p_environment)
      and workflow.candidate_id=v_candidate_id
      and workflow.contract_id=v_contract_id
      and workflow.week_ending_date=v_week_ending
      and workflow.state not in ('CANCELLED','EXPIRED','SUPERSEDED','REJECTED','REFUSED')
      and component.lifecycle_state not in (
        'MANAGER_REFUSED','OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED'
      )
      and component.agency_authorisation_state in ('AUTHORISED','INVOICED','PAID')
  ) then
    return null;
  end if;
  -- Project the whole action only when every current expense component can
  -- pass the exact single-owner/amount preconditions enforced by the
  -- mutation.  The UI must never offer an action the server already knows
  -- will fail because legacy financial ownership is ambiguous.
  select count(*)::integer into v_invalid_component_count
  from public.candidate_expense_components component
  join public.candidate_submission_workflows workflow
    on workflow.id=component.workflow_id
  left join lateral (
    select private._candidate_expense_owned_timesheet_id_v1(
      component.workflow_id,component.owning_timesheet_id
    ) as timesheet_id
  ) owner on true
  left join lateral (
    select financial.*
    from public.timesheets_financials financial
    where financial.timesheet_id=owner.timesheet_id and financial.is_current
    order by financial.computed_at_utc desc nulls last,
      financial.updated_at desc,financial.id desc
    limit 1
  ) financial on true
  left join lateral (
    select private._candidate_expense_unmaterialised_prior_v1(
      component.expense_component_id
    ) as prior
  ) unmaterialised on true
  where workflow.environment=private._candidate_assert_environment(p_environment)
    and workflow.candidate_id=v_candidate_id
    and workflow.contract_id=v_contract_id
    and workflow.week_ending_date=v_week_ending
    and workflow.state not in ('CANCELLED','EXPIRED','SUPERSEDED','REJECTED','REFUSED')
    and component.lifecycle_state not in (
      'MANAGER_REFUSED','OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED'
    )
    and component.owning_timesheet_id is not null
    and (
      component.agency_authorisation_state<>'NOT_AUTHORISED'
      or owner.timesheet_id is null or financial.id is null
      or (select count(*)
          from public.candidate_expense_components same_category
          where same_category.expense_category=component.expense_category
            and same_category.lifecycle_state not in (
              'MANAGER_REFUSED','OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED'
            )
            and private._candidate_expense_owned_timesheet_id_v1(
              same_category.workflow_id,same_category.owning_timesheet_id
            )=owner.timesheet_id)<>1
      or ((case component.expense_category
        when 'MILEAGE' then coalesce(financial.mileage_pay_ex_vat,0)
        when 'TRAVEL' then coalesce(financial.travel_pay_ex_vat,0)
        when 'ACCOMMODATION' then coalesce(financial.accommodation_pay_ex_vat,0)
        else coalesce(financial.other_pay_ex_vat,0) end)
        is distinct from component.amount
        and (
          coalesce((unmaterialised.prior->>'allowed')::boolean,false) is not true
          or (case component.expense_category
            when 'MILEAGE' then coalesce(financial.mileage_pay_ex_vat,0)
            when 'TRAVEL' then coalesce(financial.travel_pay_ex_vat,0)
            when 'ACCOMMODATION' then coalesce(financial.accommodation_pay_ex_vat,0)
            else coalesce(financial.other_pay_ex_vat,0) end)
            is distinct from coalesce((unmaterialised.prior->>'amount')::numeric,0)
        ))
      or (component.expense_category='MILEAGE'
        and coalesce(financial.mileage_units,0) is distinct from component.mileage_units
        and (
          coalesce((unmaterialised.prior->>'allowed')::boolean,false) is not true
          or coalesce(financial.mileage_units,0) is distinct from
            coalesce((unmaterialised.prior->>'mileage_units')::numeric,0)
        ))
    );
  if v_invalid_component_count>0 then return null; end if;
  select workflow.* into v_workflow
  from public.candidate_submission_workflows workflow
  where workflow.environment=private._candidate_assert_environment(p_environment)
    and workflow.candidate_id=v_candidate_id
    and workflow.contract_id=v_contract_id and workflow.week_ending_date=v_week_ending
    and workflow.state not in ('CANCELLED','EXPIRED','SUPERSEDED','REJECTED','REFUSED')
  order by case workflow.workflow_kind when 'CONTRACT_COMBINED' then 0
    when 'CONTRACT_HOURS' then 1 else 2 end,workflow.created_at_utc,workflow.id limit 1;
  if not found then return null; end if;
  if coalesce(v_approved_count,0)>0 then
    v_code:='CANCEL_ENTIRE_CLAIM'; v_slug:='cancel-entire-claim';
  elsif coalesce(v_pending_count,0)>0 then
    v_code:='WITHDRAW_ENTIRE_CLAIM'; v_slug:='withdraw-entire-claim';
  else
    return null;
  end if;
  return jsonb_build_object(
    'code',v_code,'label',case when v_code='WITHDRAW_ENTIRE_CLAIM'
      then 'Withdraw entire claim' else 'Cancel entire claim' end,
    'method','POST','path','/candidate-app/v1/workflows/'||v_workflow.id::text||'/actions/'||v_slug,
    'requires_confirmation',true,'requires_reason',true,'enabled',true,'disabled_reason',null,
    'workflow_id',v_workflow.id,'workflow_generation',v_workflow.generation,
    'approval_request_id',null,'timesheet_id',p_timesheet_id,
    'contract_week_id',v_workflow.contract_week_id,
    'invocation',jsonb_build_object(
      'version',1,'kind','HTTP','method','POST',
      'path','/candidate-app/v1/workflows/'||v_workflow.id::text||'/actions/'||v_slug,
      'fixed_body',jsonb_build_object(
        'generation',v_workflow.generation,'timesheet_id',p_timesheet_id,
        'claim_scope_sha256',v_scope->>'scope_sha256'
      ),
      'required_user_inputs','[{"name":"reason_note","type":"string","required":true},{"name":"idempotency_key","type":"uuid","required":true}]'::jsonb,
      'idempotency','REQUIRED'
    )
  );
end;
$function$;

create or replace function public.candidate_expense_component_projection_v1(
  p_environment text,
  p_workflow_ids uuid[] default null,
  p_timesheet_ids uuid[] default null
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_environment text:=private._candidate_assert_environment(p_environment);
  v_claims jsonb;
  v_timesheets jsonb;
begin
  with selected_workflows as (
    select workflow.*,
      (select count(distinct other.id)
       from public.candidate_submission_workflows other
       left join public.candidate_approval_requests pending on pending.workflow_id=other.id
         and pending.workflow_generation=other.generation and pending.state='PENDING'
       where other.environment=workflow.environment
         and other.candidate_id=workflow.candidate_id
         and other.contract_id=workflow.contract_id
         and other.week_ending_date=workflow.week_ending_date
         and other.workflow_kind in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
         and (
           (other.state='AWAITING_MANAGER_APPROVAL' and pending.id is not null)
           or (other.route='PAPER' and other.state='AWAITING_PAPER_RETURN')
         )) as pending_workflow_count
    from public.candidate_submission_workflows workflow
    where workflow.environment=v_environment
      and workflow.workflow_kind in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
      and (p_workflow_ids is null or workflow.id=any(p_workflow_ids))
  ), category_rows as (
    select component.workflow_id,
      private._candidate_expense_component_json_v1(
        component,selected_workflows.pending_workflow_count<=1
      ) as category,
      private._candidate_expense_component_status_v1(
        component.lifecycle_state,component.manager_approval_state,
        component.agency_authorisation_state
      ) as status_code,
      component.manager_approval_state,component.agency_authorisation_state,
      component.lifecycle_state,
      component.expense_category,component.amount,component.mileage_units
    from selected_workflows
    join public.candidate_expense_components component
      on component.workflow_id=selected_workflows.id
  ), claim_rows as (
    select workflow.*,
      coalesce((select jsonb_agg(category_rows.category order by
        case category_rows.category->>'expense_category'
          when 'ACCOMMODATION' then 1 when 'TRAVEL' then 2
          when 'MILEAGE' then 3 else 4 end,
        category_rows.category->>'expense_component_id')
        from category_rows where category_rows.workflow_id=workflow.id),'[]'::jsonb) as categories,
      (select case when count(distinct status_code)=1 then min(status_code) else 'MIXED' end
        from category_rows where category_rows.workflow_id=workflow.id) as aggregate_status,
      (select case when count(distinct manager_approval_state)=1 then min(manager_approval_state)
        else 'MIXED' end from category_rows where category_rows.workflow_id=workflow.id) as aggregate_manager,
      (select case when count(distinct agency_authorisation_state)=1 then min(agency_authorisation_state)
        else 'MIXED' end from category_rows where category_rows.workflow_id=workflow.id) as aggregate_agency,
      (select coalesce(sum(amount) filter(where lifecycle_state not in (
        'MANAGER_REFUSED','OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED')),0)
        from category_rows where category_rows.workflow_id=workflow.id) as total_amount,
      (select coalesce(sum(mileage_units) filter(where lifecycle_state not in (
        'MANAGER_REFUSED','OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED')),0)
        from category_rows where category_rows.workflow_id=workflow.id) as total_mileage,
      (select coalesce(sum(amount) filter(where expense_category='MILEAGE'
          and lifecycle_state not in (
            'MANAGER_REFUSED','OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED'
          )),0)
        from category_rows where category_rows.workflow_id=workflow.id) as mileage_pay,
      (select coalesce(sum(amount) filter(where expense_category='TRAVEL'
          and lifecycle_state not in (
            'MANAGER_REFUSED','OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED'
          )),0)
        from category_rows where category_rows.workflow_id=workflow.id) as travel_pay,
      (select coalesce(sum(amount) filter(where expense_category='ACCOMMODATION'
          and lifecycle_state not in (
            'MANAGER_REFUSED','OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED'
          )),0)
        from category_rows where category_rows.workflow_id=workflow.id) as accommodation_pay,
      (select coalesce(sum(amount) filter(where expense_category='OTHER'
          and lifecycle_state not in (
            'MANAGER_REFUSED','OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED'
          )),0)
        from category_rows where category_rows.workflow_id=workflow.id) as other_pay
    from selected_workflows workflow
  )
  select coalesce(jsonb_agg(jsonb_build_object(
    'workflow_id',claim.id,'generation',claim.generation,
    'document_generation',case when claim.state='FINALISED' then greatest(claim.generation-1,1)
      else claim.generation end,
    'state',claim.state,'status_code',coalesce(claim.aggregate_status,case
      when claim.state='REFUSED' then 'MANAGER_REFUSED'
      when claim.state='REJECTED' then 'AGENCY_REJECTED'
      when claim.state='CANCELLED' then 'CANCELLED'
      when claim.state in ('EXPIRED','SUPERSEDED') then 'SUPERSEDED'
      when claim.state in ('CREATED','WORKER_DRAFT') then 'DRAFT' else 'SUBMITTED' end),
    'manager_approval_state',coalesce(claim.aggregate_manager,'NOT_REQUESTED'),
    'agency_authorisation_state',coalesce(claim.aggregate_agency,'NOT_AUTHORISED'),
    'target_timesheet_id',private._candidate_expense_owned_timesheet_id_v1(claim.id,claim.target_timesheet_id),
    'submitted_at_utc',claim.worker_submitted_at_utc,'updated_at_utc',claim.updated_at_utc,
    'protected',exists(
      select 1 from category_rows protected_category
      where protected_category.workflow_id=claim.id
        and protected_category.agency_authorisation_state<>'NOT_AUTHORISED'
        and protected_category.lifecycle_state not in (
          'MANAGER_REFUSED','OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED'
        )
    ),
    'can_withdraw',claim.aggregate_manager='PENDING' and claim.pending_workflow_count<=1,
    'totals',jsonb_build_object(
      'expenses_pay_ex_vat',claim.travel_pay+claim.accommodation_pay+claim.other_pay,
      'expenses_description',null,
      'mileage_units',claim.total_mileage,
      'mileage_pay_ex_vat',claim.mileage_pay,
      'travel_pay_ex_vat',claim.travel_pay,
      'accommodation_pay_ex_vat',claim.accommodation_pay,
      'other_pay_ex_vat',claim.other_pay,
      'amount',claim.total_amount
    ),
    'supporting_evidence_count',(select coalesce(sum((category->>'supporting_evidence_count')::integer),0)
      from jsonb_array_elements(claim.categories) category
      where coalesce((category->>'included_in_total')::boolean,false)),
    'supporting_evidence_categories',(select coalesce(jsonb_agg(category->>'expense_category'
      order by category->>'expense_category'),'[]'::jsonb) from jsonb_array_elements(claim.categories) category
      where coalesce((category->>'included_in_total')::boolean,false)
        and coalesce((category->>'supporting_evidence_count')::integer,0)>0),
    'categories',claim.categories,
    'begin_update_action',private._candidate_expense_begin_update_action_v1(
      claim.id,claim.pending_workflow_count>1
    ),
    'update_state',case when exists(select 1 from public.candidate_pending_expense_updates update_row
      where update_row.workflow_id=claim.id and update_row.state in ('EDITING','RENDERING'))
      then 'UPDATING' else 'NONE' end,
    'attention_code',case when claim.pending_workflow_count>1
      then 'MULTIPLE_PENDING_EXPENSE_CLAIMS' else null end
  ) order by claim.created_at_utc,claim.id),'[]'::jsonb)
  into v_claims from claim_rows claim;

  select coalesce(jsonb_agg(jsonb_build_object(
    'timesheet_id',requested.timesheet_id,
    'category_statuses',coalesce((
      select jsonb_agg(private._candidate_expense_component_json_v1(component,
        not exists(
          select 1 from public.candidate_submission_workflows conflict_workflow
          join public.candidate_approval_requests conflict_request
            on conflict_request.workflow_id=conflict_workflow.id
            and conflict_request.workflow_generation=conflict_workflow.generation
            and conflict_request.state='PENDING'
          where conflict_workflow.environment=v_environment
            and conflict_workflow.candidate_id=timesheet_financial.candidate_id
            and conflict_workflow.contract_id=timesheet.contract_id
            and conflict_workflow.week_ending_date=timesheet.week_ending_date
            and conflict_workflow.workflow_kind in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
          group by conflict_workflow.contract_id,conflict_workflow.week_ending_date
          having count(distinct conflict_workflow.id)>1
        )) order by component.created_at_utc,component.expense_component_id)
      from public.candidate_expense_components component
      join public.candidate_submission_workflows workflow on workflow.id=component.workflow_id
       where workflow.environment=v_environment and workflow.candidate_id=timesheet_financial.candidate_id
         and workflow.contract_id=timesheet.contract_id
        and workflow.week_ending_date=timesheet.week_ending_date
        and private._candidate_expense_owned_timesheet_id_v1(
          component.workflow_id,component.owning_timesheet_id
        )=requested.timesheet_id
    ),'[]'::jsonb),
    'expense_category_context',jsonb_build_object(
      'pending_categories',coalesce((select jsonb_agg(distinct component.expense_category
        order by component.expense_category)
        from public.candidate_expense_components component
        join public.candidate_submission_workflows workflow on workflow.id=component.workflow_id
        where workflow.environment=v_environment and workflow.candidate_id=timesheet_financial.candidate_id
          and workflow.contract_id=timesheet.contract_id
          and workflow.week_ending_date=timesheet.week_ending_date
          and component.manager_approval_state='PENDING'
          and component.lifecycle_state='SUBMITTED'),'[]'::jsonb),
      'accepted_categories',coalesce((select jsonb_agg(distinct component.expense_category
        order by component.expense_category)
        from public.candidate_expense_components component
        join public.candidate_submission_workflows workflow on workflow.id=component.workflow_id
        where workflow.environment=v_environment and workflow.candidate_id=timesheet_financial.candidate_id
          and workflow.contract_id=timesheet.contract_id
          and workflow.week_ending_date=timesheet.week_ending_date
          and component.manager_approval_state='APPROVED'
          and component.lifecycle_state='MANAGER_APPROVED'),'[]'::jsonb)
    ),
    'hours_component_status',private._candidate_hours_component_json_v1(
      v_environment,requested.timesheet_id
    ),
    'whole_claim_action',private._candidate_whole_claim_action_v1(v_environment,requested.timesheet_id)
  ) order by requested.ordinal),'[]'::jsonb) into v_timesheets
  from unnest(coalesce(p_timesheet_ids,array[]::uuid[])) with ordinality requested(timesheet_id,ordinal)
  left join public.timesheets timesheet on timesheet.timesheet_id=requested.timesheet_id
  left join lateral (
    select financial.candidate_id
    from public.timesheets_financials financial
    where financial.timesheet_id=requested.timesheet_id and financial.is_current
    order by financial.computed_at_utc desc nulls last,financial.updated_at desc,financial.id desc
    limit 1
  ) timesheet_financial on true;

  return jsonb_build_object('ok',true,'claims',v_claims,'timesheets',v_timesheets);
end;
$function$;

create or replace function public.candidate_expense_update_begin_atomic_v1(
  p_session_id uuid,
  p_environment text,
  p_workflow_id uuid,
  p_expected_generation integer,
  p_category_changes jsonb,
  p_idempotency_key text,
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
  v_context jsonb;
  v_office_context jsonb;
  v_actor_kind text:='CANDIDATE';
  v_actor_id uuid;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_approval public.candidate_approval_requests%rowtype;
  v_existing public.candidate_pending_expense_updates%rowtype;
  v_update public.candidate_pending_expense_updates%rowtype;
  v_component public.candidate_expense_components%rowtype;
  v_source public.candidate_submission_components%rowtype;
  v_signature_id uuid;
  v_change jsonb;
  v_category text;
  v_kind text;
  v_normalised jsonb:='[]'::jsonb;
  v_submission jsonb;
  v_input jsonb;
  v_next_generation integer;
  v_component_no integer:=0;
  v_pending_count integer;
  v_response jsonb;
  v_is_paper_replacement boolean:=false;
  v_paper_begin_context jsonb;
  v_paper_source public.timesheets%rowtype;
  v_paper_source_snapshot jsonb;
  v_begin_request_sha256 bytea;
begin
  if p_workflow_id is null or p_expected_generation is null
     or nullif(btrim(coalesce(p_idempotency_key,'')),'') is null
     or jsonb_typeof(p_category_changes) is distinct from 'array'
     or jsonb_array_length(p_category_changes) not between 1 and 4 then
    raise exception 'CANDIDATE_EXPENSE_UPDATE_PLAN_INVALID' using errcode='22023';
  end if;
  if p_session_id is null then
    begin
      v_office_context:=nullif(current_setting(
        'cloudtms.office_candidate_context',true
      ),'')::jsonb;
    exception when others then
      v_office_context:=null;
    end;
    if coalesce(v_office_context->>'environment','')<>v_environment
       or coalesce(v_office_context->>'permission','')<>'reject_submission'
       or coalesce(v_office_context->>'action','')<>'REJECT_EXPENSE_CATEGORY'
       or nullif(v_office_context->>'actor_user_id','') is null then
      raise exception 'CANDIDATE_OFFICE_SERVICE_CONTEXT_INVALID' using errcode='28000';
    end if;
    v_actor_kind:='OFFICE';
    v_actor_id:=(v_office_context->>'actor_user_id')::uuid;
    select * into v_workflow from public.candidate_submission_workflows workflow
    where workflow.id=p_workflow_id and workflow.environment=v_environment
    for update;
  else
    v_context:=private._candidate_session_context_v1(
      p_session_id,v_environment,null,p_now_utc,true
    );
    if nullif(v_context->>'selected_candidate_id','') is null then
      raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000';
    end if;
    v_actor_id:=(v_context->>'selected_candidate_id')::uuid;
    select * into v_workflow from public.candidate_submission_workflows workflow
    where workflow.id=p_workflow_id and workflow.environment=v_environment
      and workflow.account_id=(v_context->>'account_id')::uuid
      and workflow.candidate_id=(v_context->>'selected_candidate_id')::uuid
    for update;
  end if;
  if not found then raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002'; end if;
  v_begin_request_sha256:=private._candidate_sha256_jsonb_v1(jsonb_build_object(
    'contract_version','CANDIDATE_EXPENSE_UPDATE_BEGIN_REQUEST_V1',
    'workflow_id',p_workflow_id,'expected_generation',p_expected_generation,
    'category_changes',p_category_changes
  ));
  v_is_paper_replacement:=v_workflow.route='PAPER'
    and v_workflow.state='AWAITING_PAPER_RETURN';
  if v_is_paper_replacement then
    begin
      v_paper_begin_context:=nullif(current_setting(
        'cloudtms.candidate_paper_update_begin_context',true
      ),'')::jsonb;
    exception when others then
      v_paper_begin_context:=null;
    end;
    if coalesce(v_paper_begin_context->>'workflow_id','')<>v_workflow.id::text
       or coalesce(v_paper_begin_context->>'workflow_generation','')
         <>v_workflow.generation::text
       or coalesce(v_paper_begin_context->>'purpose','') not in (
         'CREATE_UPDATED_DOCUMENTS','RESUBMIT_EXPENSE_CATEGORY',
         'REJECT_EXPENSE_CATEGORY'
       ) then
      raise exception 'CANDIDATE_PAPER_DOCUMENT_UPDATE_REQUIRED'
        using errcode='55000';
    end if;
  end if;
  select * into v_existing from public.candidate_pending_expense_updates update_row
  where update_row.workflow_id=v_workflow.id
    and update_row.idempotency_key=btrim(p_idempotency_key)
  for update;
  if found then
    if v_existing.begin_request_sha256 is null
       or v_existing.begin_result_json is null then
      raise exception 'CANDIDATE_EXPENSE_UPDATE_RECEIPT_INVALID' using errcode='55000';
    end if;
    if v_existing.begin_request_sha256<>v_begin_request_sha256 then
      raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT' using errcode='40001';
    end if;
    -- BEGIN owns an immutable receipt.  Later SUBMIT, COMMIT or ABORT state
    -- must not change what an exact retry of the original BEGIN observes.
    return v_existing.begin_result_json||jsonb_build_object('idempotent_replay',true);
  end if;
  if v_workflow.generation<>p_expected_generation then
    raise exception 'WORKFLOW_GENERATION_CONFLICT' using errcode='40001';
  end if;
  if v_workflow.workflow_kind not in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
     or (not v_is_paper_replacement
       and (v_workflow.route='PAPER' or v_workflow.state<>'AWAITING_MANAGER_APPROVAL')) then
    raise exception 'CANDIDATE_EXPENSE_UPDATE_NOT_ALLOWED' using errcode='55000';
  end if;
  perform pg_advisory_xact_lock(hashtextextended(
    'candidate-expense-pending|'||v_environment||'|'||v_workflow.candidate_id::text||'|'
      ||v_workflow.contract_id::text||'|'||v_workflow.week_ending_date::text,0
  ));
  if not v_is_paper_replacement then
    select count(distinct workflow.id)::integer into v_pending_count
    from public.candidate_submission_workflows workflow
    join public.candidate_approval_requests request on request.workflow_id=workflow.id
      and request.workflow_generation=workflow.generation and request.state='PENDING'
    where workflow.environment=v_environment and workflow.candidate_id=v_workflow.candidate_id
      and workflow.contract_id=v_workflow.contract_id
      and workflow.week_ending_date=v_workflow.week_ending_date
      and workflow.workflow_kind in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
      and workflow.state='AWAITING_MANAGER_APPROVAL';
    if v_pending_count<>1 then
      raise exception 'CANDIDATE_EXPENSE_PENDING_WORKFLOW_CONFLICT' using errcode='55000';
    end if;
    select * into v_approval from public.candidate_approval_requests request
    where request.workflow_id=v_workflow.id
      and request.workflow_generation=v_workflow.generation
      and request.state='PENDING' and request.expires_at_utc>p_now_utc
    order by request.request_generation desc,request.id desc limit 1 for update;
    if not found then
      raise exception 'CANDIDATE_EXPENSE_UPDATE_APPROVAL_CHANGED' using errcode='40001';
    end if;
  else
    select source_row.* into v_paper_source
    from public.timesheets source_row
    where source_row.timesheet_id=private._candidate_expense_current_timesheet_id_v1(
      v_workflow.id,coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id)
    )
      and source_row.is_current and source_row.archived_at_utc is null
    for update;
    if not found then
      raise exception 'CANDIDATE_PAPER_TIMESHEET_NOT_READY' using errcode='55000';
    end if;
    v_paper_source_snapshot:=to_jsonb(v_paper_source);
  end if;
  for v_change in select value from jsonb_array_elements(p_category_changes) item(value) loop
    if jsonb_typeof(v_change)<>'object' then
      raise exception 'CANDIDATE_EXPENSE_UPDATE_PLAN_INVALID' using errcode='22023';
    end if;
    v_kind:=upper(btrim(coalesce(v_change->>'update_kind','')));
    v_category:=upper(btrim(coalesce(v_change->>'expense_category','')));
    -- REMOVE_CATEGORY is an internal-only plan used by the server-owned
    -- category action.  The public Candidate request validator exposes only
    -- ADD_CATEGORY and REPLACE_CATEGORY.
    if v_kind not in (
         'ADD_CATEGORY','REPLACE_CATEGORY','REMOVE_CATEGORY','OFFICE_REJECT_CATEGORY'
       )
       or v_category not in ('MILEAGE','TRAVEL','ACCOMMODATION','OTHER')
       or exists(select 1 from jsonb_array_elements(v_normalised) prior
         where prior->>'expense_category'=v_category) then
      raise exception 'CANDIDATE_EXPENSE_UPDATE_PLAN_INVALID' using errcode='22023';
    end if;
    select * into v_component from public.candidate_expense_components component
    where component.workflow_id=v_workflow.id and component.expense_category=v_category
    for update;
    if v_kind in ('REPLACE_CATEGORY','REMOVE_CATEGORY','OFFICE_REJECT_CATEGORY') then
      if not found
         or v_component.expense_component_id is distinct from nullif(v_change->>'expense_component_id','')::uuid
         or v_component.component_generation is distinct from nullif(v_change->>'component_generation','')::integer
         or v_component.lifecycle_state<>'SUBMITTED'
         or (not v_is_paper_replacement and (
           v_component.manager_approval_state<>'PENDING'
           or v_component.approval_request_id is distinct from v_approval.id
         ))
         or (v_is_paper_replacement and (
           v_component.manager_approval_state<>'NOT_REQUESTED'
           or v_component.approval_request_id is not null
         )) then
        raise exception 'CANDIDATE_EXPENSE_UPDATE_APPROVAL_CHANGED' using errcode='40001';
      end if;
      v_normalised:=v_normalised||jsonb_build_array(jsonb_build_object(
        'update_kind',v_kind,'expense_category',v_category,
        'expense_component_id',v_component.expense_component_id,
        'component_generation',v_component.component_generation,
        'reason_note',case when v_kind='OFFICE_REJECT_CATEGORY'
          then nullif(btrim(coalesce(v_change->>'reason_note','')),'') else null end
      ));
      if v_kind='OFFICE_REJECT_CATEGORY' and (
        nullif(btrim(coalesce(v_change->>'reason_note','')),'') is null
        or char_length(btrim(v_change->>'reason_note'))>1000
      ) then
        raise exception 'CANDIDATE_REASON_REQUIRED' using errcode='22023';
      end if;
    else
      if found and v_component.lifecycle_state not in (
        'MANAGER_REFUSED','OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED'
      ) then
        raise exception 'CANDIDATE_EXPENSE_CATEGORY_ALREADY_PENDING' using errcode='55000';
      end if;
      v_normalised:=v_normalised||jsonb_build_array(jsonb_build_object(
        'update_kind',v_kind,'expense_category',v_category
      ));
    end if;
  end loop;
  v_next_generation:=v_workflow.generation+1;
  v_submission:=v_workflow.immutable_submission_json;
  v_input:=v_workflow.input_snapshot_json;
  for v_change in select value from jsonb_array_elements(v_normalised) item(value)
    where value->>'update_kind' in (
      'ADD_CATEGORY','REPLACE_CATEGORY','REMOVE_CATEGORY','OFFICE_REJECT_CATEGORY'
    )
  loop
    v_submission:=private._candidate_expense_submission_without_category_v1(
      v_submission,v_change->>'expense_category'
    );
    v_input:=private._candidate_expense_submission_without_category_v1(
      v_input,v_change->>'expense_category'
    );
  end loop;
  insert into public.candidate_pending_expense_updates(
    workflow_id,approval_request_id,from_workflow_generation,current_workflow_generation,
    update_plan_json,state,update_mode,actor_kind,actor_id,
    terminal_lifecycle_state,reason_note,
    prior_workflow_state,prior_workflow_snapshot_json,
    prior_immutable_submission_json,prior_immutable_submission_sha256,
    prior_review_manifest_json,prior_review_manifest_sha256,idempotency_key,
    prior_paper_source_timesheet_id,prior_paper_source_timesheet_id_snapshot,
    prior_paper_source_snapshot_json,
    prior_paper_source_snapshot_sha256,
    started_at_utc,expires_at_utc,updated_at_utc
  ) values (
    v_workflow.id,v_approval.id,v_workflow.generation,v_next_generation,
    v_normalised,'EDITING',case when v_is_paper_replacement
      then 'PAPER_REPLACEMENT' else 'PENDING_MANAGER' end,v_actor_kind,v_actor_id,
    case when v_actor_kind='OFFICE' then 'OFFICE_REJECTED' end,
    case when v_actor_kind='OFFICE' then (
      select change->>'reason_note' from jsonb_array_elements(v_normalised) change
      where change->>'update_kind'='OFFICE_REJECT_CATEGORY' limit 1
    ) end,
    v_workflow.state,to_jsonb(v_workflow),
    v_workflow.immutable_submission_json,v_workflow.immutable_submission_sha256,
    coalesce(v_workflow.review_manifest_json,'{}'::jsonb),
    coalesce(v_workflow.review_manifest_sha256,
      private._candidate_sha256_jsonb_v1('{}'::jsonb)),
    btrim(p_idempotency_key),v_paper_source.timesheet_id,v_paper_source.timesheet_id,
    v_paper_source_snapshot,
    case when v_paper_source_snapshot is null then null
      else private._candidate_sha256_jsonb_v1(v_paper_source_snapshot) end,
    p_now_utc,case when v_is_paper_replacement then p_now_utc+interval '30 minutes'
      else least(p_now_utc+interval '30 minutes',v_approval.expires_at_utc) end,p_now_utc
  ) returning * into v_update;
  for v_source in
    select component.* from public.candidate_submission_components component
    where component.workflow_id=v_workflow.id
      and component.workflow_generation=v_workflow.generation
      and component.state='IMMUTABLE'
      and component.component_kind in (
        'HOURS_TIMESHEET','CANDIDATE_SIGNATURE','MILEAGE_FORM','EXPENSE_EVIDENCE'
      )
      and not exists(
        select 1 from jsonb_array_elements(v_normalised) change
        where change->>'update_kind' in (
          'ADD_CATEGORY','REPLACE_CATEGORY','REMOVE_CATEGORY','OFFICE_REJECT_CATEGORY'
        )
          and change->>'expense_category'=component.expense_category
      )
    order by component.component_no,component.id
  loop
    v_component_no:=v_component_no+1;
    insert into public.candidate_submission_components(
      workflow_id,workflow_generation,component_no,timesheet_id,component_kind,
      expense_category,document_role,state,source_component_id,storage_key,media_type,
      byte_size,source_content_sha256,immutable_at_utc,required,review_ordinal,
      review_render_state,final_signed_render_state,created_at_utc
    ) values (
      v_workflow.id,v_next_generation,v_component_no,v_source.timesheet_id,
      v_source.component_kind,v_source.expense_category,v_source.document_role,'IMMUTABLE',
      coalesce(v_source.source_component_id,v_source.id),v_source.storage_key,
      v_source.media_type,v_source.byte_size,v_source.source_content_sha256,p_now_utc,
      false,null,'NOT_REQUIRED','NOT_REQUIRED',p_now_utc
    ) returning id into v_signature_id;
    if v_source.component_kind<>'CANDIDATE_SIGNATURE' then v_signature_id:=null; end if;
  end loop;
  if v_workflow.workflow_kind<>'CONTRACT_EXPENSE' then
    select id into v_signature_id from public.candidate_submission_components
    where workflow_id=v_workflow.id and workflow_generation=v_next_generation
      and component_kind='CANDIDATE_SIGNATURE' and state='IMMUTABLE'
    order by component_no limit 1;
  else
    v_signature_id:=null;
  end if;
  update public.candidate_submission_workflows set
    state='WORKER_DRAFT',generation=v_next_generation,
    input_snapshot_json=v_input,immutable_submission_json=v_submission,
    immutable_submission_sha256=private._candidate_sha256_jsonb_v1(v_submission),
    candidate_signature_component_id=v_signature_id,
    candidate_signature_sha256=(select source_content_sha256
      from public.candidate_submission_components where id=v_signature_id),
    review_manifest_json=null,review_manifest_sha256=null,
    paper_return_manifest_json=null,paper_return_manifest_sha256=null,
    worker_submitted_at_utc=null,updated_at_utc=p_now_utc
  where id=v_workflow.id and generation=p_expected_generation;
  if not found then
    raise exception 'CANDIDATE_EXPENSE_UPDATE_APPROVAL_CHANGED' using errcode='40001';
  end if;
  v_response:=jsonb_build_object(
    'ok',true,'contract_version','CANDIDATE_EXPENSE_UPDATE_BEGIN_RESULT_V1',
    'action_code','BEGIN_EXPENSE_UPDATE',
    'workflow_id',v_workflow.id,'state','WORKER_DRAFT',
    'generation',v_next_generation,'update_state','UPDATING','update_id',v_update.update_id,
    'category_changes',v_normalised,'approval_request_id',v_approval.id,
    'approval_request_generation',v_approval.request_generation,
    'manager_link_preserved',not v_is_paper_replacement,
    'paper_pack_replacement',v_is_paper_replacement,
    'old_pack_recoverable',v_is_paper_replacement,
    'preserved_component_count',v_component_no,
    'candidate_signature_component_id',v_signature_id,'idempotent_replay',false
  );
  update public.candidate_pending_expense_updates update_row set
    begin_request_sha256=v_begin_request_sha256,begin_result_json=v_response,
    updated_at_utc=p_now_utc
  where update_row.update_id=v_update.update_id
    and update_row.begin_request_sha256 is null and update_row.begin_result_json is null;
  if not found then
    raise exception 'CANDIDATE_EXPENSE_UPDATE_RECEIPT_INVALID' using errcode='40001';
  end if;
  return v_response;
end;
$function$;

-- A pending edit must enter the ordinary submission/rendering authority, but
-- it may do so only while the exact original approval request is still
-- pending.  Keeping that comparison and the state transition in one database
-- transaction closes the Candidate-edit/manager-approval race.
create or replace function public.candidate_expense_update_submit_atomic_v1(
  p_session_id uuid,
  p_environment text,
  p_workflow_id uuid,
  p_expected_generation integer,
  p_update_id uuid,
  p_payload jsonb,
  p_idempotency_key text,
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
  v_context jsonb;
  v_office_context jsonb;
  v_update public.candidate_pending_expense_updates%rowtype;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_approval public.candidate_approval_requests%rowtype;
  v_response jsonb;
  v_new_submission jsonb;
  v_new_claim jsonb;
  v_prior_claim jsonb;
  v_category text;
  v_kind text;
  v_new_amount numeric;
  v_prior_amount numeric;
  v_new_charge numeric;
  v_prior_charge numeric;
  v_new_units numeric;
  v_prior_units numeric;
  v_submit_request_sha bytea;
  v_submit_context jsonb;
begin
  if p_workflow_id is null or p_expected_generation is null or p_update_id is null
     or jsonb_typeof(coalesce(p_payload,'{}'::jsonb))<>'object'
     or nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then
    raise exception 'CANDIDATE_EXPENSE_UPDATE_RECEIPT_INVALID' using errcode='22023';
  end if;
  if p_session_id is null then
    begin
      v_office_context:=nullif(current_setting(
        'cloudtms.office_candidate_context',true
      ),'')::jsonb;
    exception when others then
      v_office_context:=null;
    end;
    if coalesce(v_office_context->>'environment','')<>v_environment
       or coalesce(v_office_context->>'permission','')<>'reject_submission'
       or coalesce(v_office_context->>'action','')<>'REJECT_EXPENSE_CATEGORY' then
      raise exception 'CANDIDATE_OFFICE_SERVICE_CONTEXT_INVALID' using errcode='28000';
    end if;
    select workflow.* into v_workflow
    from public.candidate_submission_workflows workflow
    where workflow.id=p_workflow_id and workflow.environment=v_environment
    for update;
  else
    v_context:=private._candidate_session_context_v1(
      p_session_id,v_environment,null,p_now_utc,true
    );
    select workflow.* into v_workflow
    from public.candidate_submission_workflows workflow
    where workflow.id=p_workflow_id and workflow.environment=v_environment
      and workflow.account_id=(v_context->>'account_id')::uuid
      and workflow.candidate_id=(v_context->>'selected_candidate_id')::uuid
    for update;
  end if;
  if not found then raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002'; end if;
  select update_row.* into v_update
  from public.candidate_pending_expense_updates update_row
  where update_row.update_id=p_update_id and update_row.workflow_id=v_workflow.id
  for update;
  if not found then
    raise exception 'CANDIDATE_EXPENSE_UPDATE_NOT_FOUND' using errcode='P0002';
  end if;
  if p_session_id is null and (
    v_update.actor_kind<>'OFFICE'
    or v_update.actor_id is distinct from (v_office_context->>'actor_user_id')::uuid
  ) then
    raise exception 'CANDIDATE_OFFICE_SERVICE_CONTEXT_INVALID' using errcode='28000';
  end if;
  v_submit_request_sha:=private._candidate_sha256_jsonb_v1(jsonb_build_object(
    'contract_version','CANDIDATE_EXPENSE_UPDATE_SUBMIT_REQUEST_V1',
    'workflow_id',v_workflow.id,'expected_generation',p_expected_generation,
    'update_id',v_update.update_id,'payload',coalesce(p_payload,'{}'::jsonb)
  ));
  if v_update.submit_idempotency_key is not null then
    if v_update.submit_idempotency_key<>btrim(p_idempotency_key)
       or v_update.submit_request_sha256<>v_submit_request_sha then
      raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT' using errcode='23505';
    end if;
    if v_update.submit_result_json is not null
       and v_update.state in ('RENDERING','COMMITTED') then
      -- SUBMIT also owns an immutable receipt.  Rebind/commit state is read
      -- through the canonical detail projection, never by rewriting the
      -- result of a lost-response retry.
      return v_update.submit_result_json
        ||jsonb_build_object('idempotent_replay',true);
    end if;
  end if;
  if v_update.state<>'EDITING' or v_workflow.state<>'WORKER_DRAFT'
     or v_workflow.generation<>p_expected_generation
     or v_update.current_workflow_generation<>p_expected_generation then
    raise exception 'CANDIDATE_EXPENSE_UPDATE_APPROVAL_CHANGED' using errcode='40001';
  end if;
  if v_update.update_mode='PENDING_MANAGER' then
    select request.* into v_approval
    from public.candidate_approval_requests request
    where request.id=v_update.approval_request_id
      and request.workflow_id=v_workflow.id
      and request.workflow_generation=v_update.from_workflow_generation
    for update;
    if not found or v_approval.state<>'PENDING' then
      raise exception 'CANDIDATE_EXPENSE_UPDATE_APPROVAL_CHANGED' using errcode='40001';
    end if;
  elsif v_update.update_mode='PAPER_REPLACEMENT' then
    if v_update.approval_request_id is not null or v_workflow.route<>'PAPER' then
      raise exception 'CANDIDATE_EXPENSE_UPDATE_APPROVAL_CHANGED' using errcode='40001';
    end if;
  else
    raise exception 'CANDIDATE_EXPENSE_UPDATE_RECEIPT_INVALID' using errcode='22023';
  end if;
  v_new_submission:=coalesce(p_payload->'immutable_submission','{}'::jsonb);
  if jsonb_typeof(v_new_submission)<>'object' then
    raise exception 'CANDIDATE_IMMUTABLE_SUBMISSION_REQUIRED' using errcode='22023';
  end if;
  v_new_claim:=coalesce(
    v_new_submission#>'{expense_submission,canonical_tsfin_snapshot}',
    v_new_submission->'expense_submission',
    v_new_submission#>'{expense_claim,canonical_tsfin_snapshot}',
    v_new_submission->'expense_claim',v_new_submission,'{}'::jsonb
  );
  v_prior_claim:=coalesce(
    v_update.prior_immutable_submission_json#>'{expense_submission,canonical_tsfin_snapshot}',
    v_update.prior_immutable_submission_json->'expense_submission',
    v_update.prior_immutable_submission_json#>'{expense_claim,canonical_tsfin_snapshot}',
    v_update.prior_immutable_submission_json->'expense_claim','{}'::jsonb
  );
  foreach v_category in array array['MILEAGE','TRAVEL','ACCOMMODATION','OTHER'] loop
    select change->>'update_kind' into v_kind
    from jsonb_array_elements(v_update.update_plan_json) change
    where change->>'expense_category'=v_category limit 1;
    v_new_amount:=private._candidate_expense_number_v1(v_new_claim,case v_category
      when 'MILEAGE' then 'mileage_pay_ex_vat' when 'TRAVEL' then 'travel_pay_ex_vat'
      when 'ACCOMMODATION' then 'accommodation_pay_ex_vat' else 'other_pay_ex_vat' end);
    v_prior_amount:=private._candidate_expense_number_v1(v_prior_claim,case v_category
      when 'MILEAGE' then 'mileage_pay_ex_vat' when 'TRAVEL' then 'travel_pay_ex_vat'
      when 'ACCOMMODATION' then 'accommodation_pay_ex_vat' else 'other_pay_ex_vat' end);
    v_new_charge:=private._candidate_expense_number_v1(v_new_claim,case v_category
      when 'MILEAGE' then 'mileage_charge_ex_vat' when 'TRAVEL' then 'travel_charge_ex_vat'
      when 'ACCOMMODATION' then 'accommodation_charge_ex_vat' else 'other_charge_ex_vat' end);
    v_prior_charge:=private._candidate_expense_number_v1(v_prior_claim,case v_category
      when 'MILEAGE' then 'mileage_charge_ex_vat' when 'TRAVEL' then 'travel_charge_ex_vat'
      when 'ACCOMMODATION' then 'accommodation_charge_ex_vat' else 'other_charge_ex_vat' end);
    v_new_units:=case when v_category='MILEAGE'
      then private._candidate_expense_number_v1(v_new_claim,'mileage_units') else 0 end;
    v_prior_units:=case when v_category='MILEAGE'
      then private._candidate_expense_number_v1(v_prior_claim,'mileage_units') else 0 end;
    if v_kind in ('ADD_CATEGORY','REPLACE_CATEGORY') then
      if v_new_amount<=0 and v_new_charge<=0 and v_new_units<=0 then
        raise exception 'CANDIDATE_EXPENSE_UPDATE_EMPTY' using errcode='55000';
      end if;
    elsif v_kind in ('REMOVE_CATEGORY','OFFICE_REJECT_CATEGORY') then
      if v_new_amount<>0 or v_new_charge<>0 or v_new_units<>0 then
        raise exception 'CANDIDATE_EXPENSE_UPDATE_PLAN_MISMATCH' using errcode='40001';
      end if;
    elsif v_new_amount is distinct from v_prior_amount
       or v_new_charge is distinct from v_prior_charge
       or v_new_units is distinct from v_prior_units then
      raise exception 'CANDIDATE_EXPENSE_UPDATE_PLAN_MISMATCH' using errcode='40001';
    end if;
  end loop;
  -- The update plan is the complete amendment boundary. New evidence may
  -- exist only for an ADD/REPLACE category, and every untouched immutable
  -- source component must remain byte-for-byte present. This closes both a
  -- crash/resume mismatch and an attempt to alter an unplanned category by
  -- superseding or uploading a component after BEGIN.
  if exists(
    select 1
    from public.candidate_submission_components current_component
    where current_component.workflow_id=v_workflow.id
      and current_component.workflow_generation=v_workflow.generation
      and current_component.state='IMMUTABLE'
      and current_component.component_kind in ('MILEAGE_FORM','EXPENSE_EVIDENCE')
      and current_component.source_component_id is null
      and not exists(
        select 1 from jsonb_array_elements(v_update.update_plan_json) change
        where change->>'update_kind' in ('ADD_CATEGORY','REPLACE_CATEGORY')
          and change->>'expense_category'=current_component.expense_category
      )
  ) or exists(
    select 1 from jsonb_array_elements(v_update.update_plan_json) change
    where change->>'update_kind' in ('ADD_CATEGORY','REPLACE_CATEGORY')
      and not exists(
        select 1 from public.candidate_submission_components current_component
        where current_component.workflow_id=v_workflow.id
          and current_component.workflow_generation=v_workflow.generation
          and current_component.state='IMMUTABLE'
          and current_component.component_kind in ('MILEAGE_FORM','EXPENSE_EVIDENCE')
          and current_component.expense_category=change->>'expense_category'
          and current_component.source_component_id is null
          and current_component.source_content_sha256 is not null
      )
  ) or exists(
    select 1
    from public.candidate_submission_components prior_component
    where prior_component.workflow_id=v_workflow.id
      and prior_component.workflow_generation=v_update.from_workflow_generation
      and prior_component.state='IMMUTABLE'
      and prior_component.component_kind in (
        'HOURS_TIMESHEET','CANDIDATE_SIGNATURE','MILEAGE_FORM','EXPENSE_EVIDENCE'
      )
      and not exists(
        select 1 from jsonb_array_elements(v_update.update_plan_json) change
        where change->>'expense_category'=prior_component.expense_category
      )
      and not exists(
        select 1
        from public.candidate_submission_components current_component
        where current_component.workflow_id=prior_component.workflow_id
          and current_component.workflow_generation=v_workflow.generation
          and current_component.state='IMMUTABLE'
          and current_component.source_component_id=coalesce(
            prior_component.source_component_id,prior_component.id
          )
          and current_component.component_kind=prior_component.component_kind
          and current_component.expense_category is not distinct from
            prior_component.expense_category
          and current_component.source_content_sha256=prior_component.source_content_sha256
          and current_component.byte_size=prior_component.byte_size
          and current_component.media_type=prior_component.media_type
      )
  ) then
    raise exception 'CANDIDATE_EXPENSE_UPDATE_PLAN_MISMATCH' using errcode='40001';
  end if;
  if v_workflow.candidate_signature_component_id is not null and exists(
    select 1 from public.candidate_submission_components prior_hours
    where prior_hours.workflow_id=v_workflow.id
      and prior_hours.workflow_generation=v_update.from_workflow_generation
      and prior_hours.component_kind='HOURS_TIMESHEET'
      and prior_hours.state='IMMUTABLE'
  ) and not exists(
    select 1
    from public.candidate_submission_components current_hours
    join public.candidate_submission_components prior_hours
      on prior_hours.workflow_id=current_hours.workflow_id
      and prior_hours.workflow_generation=v_update.from_workflow_generation
      and prior_hours.component_kind='HOURS_TIMESHEET'
      and prior_hours.state='IMMUTABLE'
      and prior_hours.source_content_sha256=current_hours.source_content_sha256
    where current_hours.workflow_id=v_workflow.id
      and current_hours.workflow_generation=v_workflow.generation
      and current_hours.component_kind='HOURS_TIMESHEET'
      and current_hours.state='IMMUTABLE'
  ) then
    raise exception 'CANDIDATE_SIGNATURE_SCOPE_CHANGED' using errcode='40001';
  end if;
  v_submit_context:=jsonb_build_object(
    'contract_version','CANDIDATE_EXPENSE_UPDATE_SUBMIT_CONTEXT_V1',
    'workflow_id',v_workflow.id,
    'workflow_generation',v_workflow.generation,
    'update_id',v_update.update_id,
    'update_mode',v_update.update_mode,
    'actor_kind',v_update.actor_kind,
    'actor_id',v_update.actor_id,
    'idempotency_key',btrim(p_idempotency_key)
  );
  perform set_config(
    'cloudtms.candidate_expense_update_submit_context',v_submit_context::text,true
  );
  v_response:=public.candidate_workflow_transition_atomic_v1(
    p_session_id,v_environment,v_workflow.id,'WORKER_SUBMIT',
    p_expected_generation,
    coalesce(p_payload,'{}'::jsonb)||jsonb_build_object(
      'update_id',v_update.update_id,
      'candidate_signature_component_id',v_workflow.candidate_signature_component_id,
      'service_office_action',p_session_id is null,
      'actor_user_id',case when p_session_id is null then v_update.actor_id else null end
    ),
    p_idempotency_key,p_now_utc
  );
  perform set_config('cloudtms.candidate_expense_update_submit_context','{}',true);
  if v_update.update_mode='PENDING_MANAGER' then
    if coalesce(v_response->>'state','')<>'WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT'
       or jsonb_typeof(v_response->'render_contract') is distinct from 'object' then
      raise exception 'CANDIDATE_EXPENSE_UPDATE_RENDER_CONTRACT_MISSING'
        using errcode='55000';
    end if;
  elsif coalesce(v_response->>'state','')<>'WORKER_SUBMITTED' then
    raise exception 'CANDIDATE_EXPENSE_PAPER_REPLACEMENT_NOT_READY'
      using errcode='55000';
  end if;
  v_response:=v_response||jsonb_build_object(
    'update_id',v_update.update_id,'update_state','UPDATING',
    'approval_request_id',v_approval.id,
    'approval_request_generation',v_approval.request_generation,
    'manager_link_preserved',v_update.update_mode='PENDING_MANAGER',
    'paper_pack_replacement',v_update.update_mode='PAPER_REPLACEMENT',
    'paper_prepare_required',v_update.update_mode='PAPER_REPLACEMENT',
    'old_pack_recoverable',v_update.update_mode='PAPER_REPLACEMENT'
  );
  update public.candidate_pending_expense_updates set
    state='RENDERING',current_workflow_generation=(v_response->>'generation')::integer,
    submit_idempotency_key=btrim(p_idempotency_key),
    submit_request_sha256=v_submit_request_sha,
    submit_result_json=v_response,
    expires_at_utc=p_now_utc+case when v_update.update_mode='PAPER_REPLACEMENT'
      then interval '15 minutes' else interval '5 minutes' end,
    updated_at_utc=p_now_utc
  where update_id=v_update.update_id and state='EDITING';
  if not found then
    raise exception 'CANDIDATE_EXPENSE_UPDATE_APPROVAL_CHANGED' using errcode='40001';
  end if;
  return v_response;
end;
$function$;

create or replace function public.candidate_expense_update_rebind_atomic_v1(
  p_environment text,
  p_workflow_id uuid,
  p_update_id uuid,
  p_idempotency_key text,
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
  v_update public.candidate_pending_expense_updates%rowtype;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_approval public.candidate_approval_requests%rowtype;
  v_component_ids uuid[];
  v_change jsonb;
  v_component public.candidate_expense_components%rowtype;
  v_value record;
  v_before jsonb;
  v_manager_timesheet_component_id uuid;
  v_manager_timesheet_sha256 bytea;
  v_financial_result jsonb;
  v_response jsonb;
  v_operation public.candidate_expense_operations%rowtype;
  v_operation_result jsonb;
  v_paper_retirement jsonb;
  v_delete_result jsonb:='{}'::jsonb;
  v_previous_owner_timesheet_id uuid;
  v_current_owner_timesheet_id uuid;
  v_rebind_request_sha256 bytea;
  v_rejection_freshness jsonb;
  v_paper_mail_ids uuid[];
  v_paper_mail public.mail_outbox%rowtype;
  v_paper_notification_id uuid;
begin
  if p_workflow_id is null or p_update_id is null
     or nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then
    raise exception 'CANDIDATE_EXPENSE_UPDATE_RECEIPT_INVALID' using errcode='22023';
  end if;
  select * into v_workflow from public.candidate_submission_workflows workflow
  where workflow.id=p_workflow_id and workflow.environment=v_environment for update;
  if not found then raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_update from public.candidate_pending_expense_updates update_row
  where update_row.update_id=p_update_id and update_row.workflow_id=v_workflow.id
  for update;
  if not found then
    raise exception 'CANDIDATE_EXPENSE_UPDATE_NOT_FOUND' using errcode='P0002';
  end if;
  v_rebind_request_sha256:=private._candidate_sha256_jsonb_v1(jsonb_build_object(
    'contract_version','CANDIDATE_EXPENSE_UPDATE_REBIND_REQUEST_V1',
    'workflow_id',v_workflow.id,'update_id',v_update.update_id
  ));
  if v_update.rebind_idempotency_key is not null then
    if v_update.rebind_idempotency_key<>btrim(p_idempotency_key)
       or v_update.rebind_request_sha256<>v_rebind_request_sha256 then
      raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT' using errcode='40001';
    end if;
    if v_update.rebind_result_json is null then
      raise exception 'CANDIDATE_EXPENSE_UPDATE_RECEIPT_INVALID' using errcode='55000';
    end if;
    return v_update.rebind_result_json||jsonb_build_object('idempotent_replay',true);
  end if;
  if v_update.state='COMMITTED' then
    raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT' using errcode='40001';
  end if;
  if v_update.state not in ('EDITING','RENDERING')
     or (
       v_update.update_mode='PENDING_MANAGER'
       and (
         v_workflow.state<>'READY_FOR_MANAGER_APPROVAL'
         or v_workflow.review_manifest_json is null
         or v_workflow.review_manifest_sha256 is null
       )
     )
     or (
       v_update.update_mode='PAPER_REPLACEMENT'
       and (
         v_workflow.state<>'AWAITING_PAPER_RETURN'
         or v_workflow.paper_return_manifest_json is null
         or v_workflow.paper_return_manifest_sha256 is null
       )
     )
     or v_update.update_mode not in ('PENDING_MANAGER','PAPER_REPLACEMENT') then
    raise exception 'CANDIDATE_EXPENSE_UPDATE_NOT_READY' using errcode='55000';
  end if;
  if v_update.operation_id is not null then
    select operation.* into v_operation
    from public.candidate_expense_operations operation
    where operation.operation_id=v_update.operation_id
    for update;
    if not found or v_operation.workflow_id is distinct from v_update.workflow_id then
      raise exception 'CANDIDATE_EXPENSE_OPERATION_CHANGED' using errcode='40001';
    end if;
    if v_operation.actor_kind='OFFICE'
       and v_operation.action_code='REJECT_EXPENSE_CATEGORY' then
      perform private._candidate_expense_owner_context_lock_v1(
        v_operation.expense_component_id
      );
      v_rejection_freshness:=private._candidate_office_expense_rejection_context_v1(
        v_environment,v_operation.expense_component_id,p_now_utc
      );
      if coalesce(v_operation.progress_json->>'expected_rejection_freshness_sha256','')
           !~ '^[0-9a-f]{64}$'
         or v_operation.progress_json->>'expected_rejection_freshness_sha256'
           is distinct from v_rejection_freshness#>>'{basis,freshness_sha256}' then
        raise exception 'CANDIDATE_EXPENSE_CATEGORY_CONTEXT_CHANGED'
          using errcode='40001',detail=coalesce(v_rejection_freshness,'{}'::jsonb)::text;
      end if;
    end if;
  end if;
  if v_update.update_mode='PENDING_MANAGER' then
    select * into v_approval from public.candidate_approval_requests request
    where request.id=v_update.approval_request_id and request.workflow_id=v_workflow.id
      and request.workflow_generation=v_update.from_workflow_generation
      and request.state='PENDING' and request.expires_at_utc>p_now_utc for update;
    if not found then
      raise exception 'CANDIDATE_EXPENSE_UPDATE_APPROVAL_CHANGED' using errcode='40001';
    end if;
    select array_agg(component.id order by component.review_ordinal,component.id)
    into v_component_ids from public.candidate_submission_components component
    where component.workflow_id=v_workflow.id
      and component.workflow_generation=v_workflow.generation
      and component.required and component.state<>'SUPERSEDED';
    if coalesce(cardinality(v_component_ids),0)=0 then
      raise exception 'CANDIDATE_EXPENSE_UPDATE_EMPTY' using errcode='55000';
    end if;
    v_manager_timesheet_component_id:=nullif(
      v_workflow.review_manifest_json->>'manager_review_timesheet_component_id',''
    )::uuid;
    v_manager_timesheet_sha256:=case
      when coalesce(v_workflow.review_manifest_json->>'manager_review_timesheet_sha256','')
        ~ '^[0-9a-fA-F]{64}$'
      then decode(v_workflow.review_manifest_json->>'manager_review_timesheet_sha256','hex')
      else null end;
    update public.candidate_approval_requests set
      workflow_generation=v_workflow.generation,
      review_manifest_sha256=v_workflow.review_manifest_sha256,
      required_component_ids=v_component_ids,
      required_component_manifest_json=coalesce(
        v_workflow.review_manifest_json->'required_components','[]'::jsonb
      ),
      manager_review_timesheet_component_id=v_manager_timesheet_component_id,
      manager_review_timesheet_sha256=v_manager_timesheet_sha256,
      review_progress_json='{}'::jsonb,review_started_at_utc=null,
      review_completed_at_utc=null,updated_at_utc=p_now_utc
    where id=v_approval.id and state='PENDING'
      and workflow_generation=v_update.from_workflow_generation;
    if not found then
      raise exception 'CANDIDATE_EXPENSE_UPDATE_APPROVAL_CHANGED' using errcode='40001';
    end if;
    update public.candidate_submission_components set approval_request_id=v_approval.id
    where workflow_id=v_workflow.id and workflow_generation=v_workflow.generation
      and required and state<>'SUPERSEDED';
  end if;
  update public.candidate_pending_expense_updates set
    state='COMMITTED',current_workflow_generation=v_workflow.generation,
    completed_at_utc=p_now_utc,updated_at_utc=p_now_utc
  where update_id=v_update.update_id;
  if v_update.update_mode='PENDING_MANAGER' then
    update public.candidate_submission_workflows set
      state='AWAITING_MANAGER_APPROVAL',updated_at_utc=p_now_utc
    where id=v_workflow.id and generation=v_workflow.generation
      and state='READY_FOR_MANAGER_APPROVAL';
    if not found then
      raise exception 'CANDIDATE_EXPENSE_UPDATE_APPROVAL_CHANGED' using errcode='40001';
    end if;
  else
    v_paper_retirement:=private._candidate_paper_delivery_retire_v1(
      v_workflow.id,v_update.from_workflow_generation,
      'PAPER_PACK_REPLACED',p_now_utc
    );
    if not coalesce((v_paper_retirement->>'retired')::boolean,false)
       or not (
         coalesce((v_paper_retirement->>'qr_invalidated')::boolean,false)
         or coalesce((v_paper_retirement->>'qr_already_invalidated')::boolean,false)
       ) then
      raise exception 'CANDIDATE_PAPER_QR_INVALIDATION_NOT_PROVEN'
        using errcode='40001',detail=v_paper_retirement::text;
    end if;
    -- PAPER_PACK_RELEASE may finish rendering while this update is still
    -- RENDERING, but that replacement delivery is deliberately held at
    -- infinity. Make it claimable, and create its Candidate notification,
    -- only inside the same transaction that commits the update and retires
    -- the previous immutable pack.
    select coalesce(array_agg(mail_row.id order by mail_row.id),array[]::uuid[])
    into v_paper_mail_ids
    from public.mail_outbox mail_row
    where mail_row.type='TIMESHEET_QR'
      and mail_row.context_kind='timesheets'
      and mail_row.status='QUEUED' and mail_row.sent_at is null
      and mail_row.attempt_lease_token is null
      and mail_row.payment_scope_json->>'candidate_mail_authority'='CANDIDATE_PAPER_V1'
      and mail_row.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
      and mail_row.payment_scope_json->>'candidate_workflow_generation'=
        v_workflow.generation::text
      and lower(coalesce(mail_row.payment_scope_json->>'paper_return_manifest_sha256',''))=
        encode(v_workflow.paper_return_manifest_sha256,'hex')
      and lower(coalesce(mail_row.payment_scope_json->>'candidate_paper_pack_ready','false'))
        in ('true','t','1','yes')
      and lower(coalesce(mail_row.payment_scope_json->>'candidate_expense_update_hold','false'))
        in ('true','t','1','yes')
      and mail_row.payment_scope_json->>'mail_hold_reason'='CANDIDATE_EXPENSE_UPDATE_PENDING'
      and jsonb_typeof(mail_row.attachments)='array'
      and jsonb_array_length(mail_row.attachments)>0;
    if cardinality(v_paper_mail_ids)<>1 then
      raise exception 'CANDIDATE_EXPENSE_PAPER_REPLACEMENT_NOT_READY'
        using errcode='40001',detail=jsonb_build_object(
          'held_replacement_mail_count',cardinality(v_paper_mail_ids),
          'workflow_id',v_workflow.id,'generation',v_workflow.generation
        )::text;
    end if;
    update public.mail_outbox mail_row set
      scheduled_for_utc=p_now_utc,next_attempt_at_utc=p_now_utc,
      payment_scope_json=mail_row.payment_scope_json||jsonb_build_object(
        'mail_held_until_pdf_rendered',false,
        'mail_delayed_for_pdf_render',false,
        'mail_hold_reason',null,
        'candidate_expense_update_hold',false,
        'candidate_expense_update_committed_at_utc',p_now_utc
      )
    where mail_row.id=v_paper_mail_ids[1]
      and mail_row.status='QUEUED' and mail_row.sent_at is null
      and mail_row.attempt_lease_token is null
    returning mail_row.* into v_paper_mail;
    if not found then
      raise exception 'CANDIDATE_EXPENSE_PAPER_REPLACEMENT_NOT_READY'
        using errcode='40001';
    end if;
    insert into public.candidate_notifications(
      account_id,candidate_id,workflow_id,timesheet_id,event_type,preference_category,
      template_key,template_params,deep_link_json,state,push_state,dedupe_key,created_at_utc
    ) values (
      v_workflow.account_id,v_workflow.candidate_id,v_workflow.id,v_paper_mail.context_id,
      'PAPER_PACK_READY','resubmission_required','candidate-paper-pack-ready-v1',
      jsonb_build_object(
        'page_count',(v_paper_mail.payment_scope_json->>'candidate_complete_pack_page_count')::integer,
        'workflow_generation',v_workflow.generation
      ),
      jsonb_build_object('type','paper_pack','timesheet_id',v_paper_mail.context_id,
        'workflow_id',v_workflow.id,'workflow_generation',v_workflow.generation),
      'UNREAD','PENDING',
      'CANDIDATE_PAPER_PACK_READY_V1:'||v_workflow.id::text||':'
        ||v_workflow.generation::text||':'
        ||encode(v_workflow.paper_return_manifest_sha256,'hex'),p_now_utc
    ) on conflict(dedupe_key) do nothing
    returning id into v_paper_notification_id;
  end if;
  -- The old immutable generation remains untouched throughout editing and
  -- rendering.  Retire it only after the new manager manifest or complete
  -- PAPER pack is proved ready and the owning workflow has reached its new
  -- authoritative waiting state.
  update public.candidate_submission_components component set
    state='SUPERSEDED',superseded_at_utc=p_now_utc,
    review_render_state=case when component.review_render_state='NOT_REQUIRED'
      then component.review_render_state else 'SUPERSEDED' end,
    final_signed_render_state=case when component.final_signed_render_state='NOT_REQUIRED'
      then component.final_signed_render_state else 'SUPERSEDED' end
  where component.workflow_id=v_workflow.id
    and component.workflow_generation=v_update.from_workflow_generation
    and component.state<>'SUPERSEDED';
  perform private._candidate_expense_components_sync_v1(v_workflow.id,p_now_utc);
  -- Finalise the stable category ledger only after the fresh manifest and the
  -- original approval request have been atomically rebound.  Until this
  -- point, the previous category remains authoritative and ABORT can restore
  -- it without fabricating a transient withdrawal.
  for v_change in
    select value from jsonb_array_elements(v_update.update_plan_json) item(value)
  loop
    if v_change->>'update_kind' in ('REMOVE_CATEGORY','OFFICE_REJECT_CATEGORY') then
      select * into v_component
      from public.candidate_expense_components component
      where component.workflow_id=v_workflow.id
        and component.expense_component_id=(v_change->>'expense_component_id')::uuid
        and component.component_generation=(v_change->>'component_generation')::integer
      for update;
      if not found or v_component.lifecycle_state<>'SUBMITTED'
         or (v_update.update_mode='PENDING_MANAGER'
           and v_component.manager_approval_state<>'PENDING')
         or (v_update.update_mode='PAPER_REPLACEMENT'
           and v_component.manager_approval_state<>'NOT_REQUESTED') then
        raise exception 'CANDIDATE_EXPENSE_UPDATE_APPROVAL_CHANGED' using errcode='40001';
      end if;
      v_before:=to_jsonb(v_component);
      v_financial_result:=private._candidate_expense_financial_remove_v1(
        v_component.expense_component_id,v_component.component_generation,p_now_utc
      );
      update public.candidate_expense_components set
        workflow_generation=v_workflow.generation,
        component_generation=component_generation+1,
        lifecycle_state=case when v_change->>'update_kind'='OFFICE_REJECT_CATEGORY'
          then 'OFFICE_REJECTED' else 'WITHDRAWN' end,
        manager_approval_state='NOT_REQUESTED',
        approval_request_id=null,removed_at_utc=p_now_utc,updated_at_utc=p_now_utc
        ,refusal_kind=case when v_change->>'update_kind'='OFFICE_REJECT_CATEGORY'
          then 'AGENCY_REJECTION' end
        ,refusal_reason=case when v_change->>'update_kind'='OFFICE_REJECT_CATEGORY'
          then v_update.reason_note end
        ,refused_at_utc=case when v_change->>'update_kind'='OFFICE_REJECT_CATEGORY'
          then p_now_utc end
      where expense_component_id=v_component.expense_component_id
      returning * into v_component;
      insert into public.candidate_expense_component_events(
        expense_component_id,workflow_id,component_generation,event_type,actor_kind,
        actor_id,before_state_json,after_state_json,idempotency_key,occurred_at_utc
      ) values (
        v_component.expense_component_id,v_workflow.id,v_component.component_generation,
        case when v_change->>'update_kind'='OFFICE_REJECT_CATEGORY'
          then 'OFFICE_REJECTED' else 'WITHDRAWN' end,
        v_update.actor_kind,v_update.actor_id,v_before,to_jsonb(v_component),
        'expense-update-remove:'||v_update.update_id::text||':'
          ||v_component.expense_component_id::text,p_now_utc
      ) on conflict(expense_component_id,idempotency_key) do nothing;
      perform private._candidate_notification_insert_v1(
        v_workflow.account_id,v_workflow.candidate_id,v_workflow.id,
        private._candidate_expense_owned_timesheet_id_v1(
          v_workflow.id,v_component.owning_timesheet_id
        ),case when v_change->>'update_kind'='OFFICE_REJECT_CATEGORY'
          then 'OFFICE_REJECTED' else 'EXPENSE_WITHDRAWN' end,
        'timesheet_expense_attention',
        case when v_change->>'update_kind'='OFFICE_REJECT_CATEGORY'
          then 'candidate-expense-category-rejected-v1'
          else 'candidate-expense-category-withdrawn-v1' end,
        jsonb_build_object(
          'workflow_id',v_workflow.id,
          'expense_component_id',v_component.expense_component_id,
          'expense_category',v_component.expense_category,
          'reason',case when v_change->>'update_kind'='OFFICE_REJECT_CATEGORY'
            then v_update.reason_note end,
          'resubmission_scope',case when v_change->>'update_kind'='OFFICE_REJECT_CATEGORY'
            then 'EXPENSE_CATEGORY' end
        ),jsonb_build_object('type','workflow','workflow_id',v_workflow.id),
        case when v_change->>'update_kind'='OFFICE_REJECT_CATEGORY'
          then 'CANDIDATE_EXPENSE_CATEGORY_REJECTED_V1:'
          else 'CANDIDATE_EXPENSE_CATEGORY_WITHDRAWN_V1:' end
          ||v_update.update_id::text||':'
          ||v_component.expense_component_id::text,p_now_utc
      );
    elsif v_change->>'update_kind'='ADD_CATEGORY' then
      select * into v_component from public.candidate_expense_components component
      where component.workflow_id=v_workflow.id
        and component.expense_category=v_change->>'expense_category'
      for update;
      if found and v_component.lifecycle_state in (
        'MANAGER_REFUSED','OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED'
      ) then
        select * into v_value
        from private._candidate_expense_component_values_v1(v_workflow.id) value
        where value.expense_category=v_change->>'expense_category';
        if not found or (v_value.amount<=0 and v_value.mileage_units<=0) then
          raise exception 'CANDIDATE_EXPENSE_UPDATE_EMPTY' using errcode='55000';
        end if;
        v_before:=to_jsonb(v_component);
        update public.candidate_expense_components set
          workflow_generation=v_workflow.generation,
          component_generation=component_generation+1,
          owning_timesheet_id=v_workflow.target_timesheet_id,
          amount=v_value.amount,mileage_units=v_value.mileage_units,
          lifecycle_state='SUBMITTED',manager_approval_state=case
            when v_update.update_mode='PENDING_MANAGER' then 'PENDING'
            else 'NOT_REQUESTED' end,
          agency_authorisation_state='NOT_AUTHORISED',approval_request_id=case
            when v_update.update_mode='PENDING_MANAGER' then v_approval.id end,
          submitted_at_utc=p_now_utc,manager_approved_at_utc=null,
          refusal_kind=null,refusal_reason=null,refused_at_utc=null,
          removed_at_utc=null,updated_at_utc=p_now_utc
        where expense_component_id=v_component.expense_component_id
        returning * into v_component;
        insert into public.candidate_expense_component_events(
          expense_component_id,workflow_id,component_generation,event_type,actor_kind,
          before_state_json,after_state_json,idempotency_key,occurred_at_utc
        ) values (
          v_component.expense_component_id,v_workflow.id,v_component.component_generation,
          'SUBMITTED','CANDIDATE',v_before,to_jsonb(v_component),
          'expense-update-add:'||v_update.update_id::text||':'
            ||v_component.expense_component_id::text,p_now_utc
        ) on conflict(expense_component_id,idempotency_key) do nothing;
      elsif not found
         or v_component.lifecycle_state<>'SUBMITTED'
         or (v_update.update_mode='PENDING_MANAGER' and (
           v_component.manager_approval_state<>'PENDING'
           or v_component.approval_request_id is distinct from v_approval.id
         ))
         or (v_update.update_mode='PAPER_REPLACEMENT' and (
           v_component.manager_approval_state<>'NOT_REQUESTED'
           or v_component.approval_request_id is not null
         )) then
        raise exception 'CANDIDATE_EXPENSE_UPDATE_APPROVAL_CHANGED' using errcode='40001';
      end if;
    elsif v_change->>'update_kind'='REPLACE_CATEGORY' then
      select * into v_component from public.candidate_expense_components component
      where component.workflow_id=v_workflow.id
        and component.expense_component_id=(v_change->>'expense_component_id')::uuid
      for update;
      if not found then
        raise exception 'CANDIDATE_EXPENSE_UPDATE_APPROVAL_CHANGED' using errcode='40001';
      end if;
      if v_component.lifecycle_state<>'SUBMITTED'
         or (v_update.update_mode='PENDING_MANAGER' and (
           v_component.manager_approval_state<>'PENDING'
           or v_component.approval_request_id is distinct from v_approval.id
         ))
         or (v_update.update_mode='PAPER_REPLACEMENT' and (
           v_component.manager_approval_state<>'NOT_REQUESTED'
           or v_component.approval_request_id is not null
         )) then
        raise exception 'CANDIDATE_EXPENSE_UPDATE_APPROVAL_CHANGED' using errcode='40001';
      end if;
      select event.before_state_json into v_before
      from public.candidate_expense_component_events event
      where event.expense_component_id=v_component.expense_component_id
        and event.component_generation=v_component.component_generation
        and event.idempotency_key='component-sync:'
          ||v_component.expense_component_id::text||':'
          ||v_component.component_generation::text
      order by event.occurred_at_utc desc,event.event_id desc limit 1;
      if v_before is null then
        raise exception 'CANDIDATE_EXPENSE_UPDATE_COMPONENT_AUDIT_MISSING'
          using errcode='55000';
      end if;
      insert into public.candidate_expense_component_events(
        expense_component_id,workflow_id,component_generation,event_type,actor_kind,
        before_state_json,after_state_json,idempotency_key,occurred_at_utc
      ) values (
        v_component.expense_component_id,v_workflow.id,v_component.component_generation,
        'REPLACED',v_update.actor_kind,v_before,to_jsonb(v_component),
        'expense-update-replace:'||v_update.update_id::text||':'
          ||v_component.expense_component_id::text,p_now_utc
      ) on conflict(expense_component_id,idempotency_key) do nothing;
    end if;
  end loop;
  -- The Office result names the Timesheet that was current when the user
  -- confirmed the consequence.  Do not replace that historical identity
  -- with a later rotation target resolved during render/rebind.
  v_current_owner_timesheet_id:=nullif(v_financial_result->>'timesheet_id','')::uuid;
  v_previous_owner_timesheet_id:=case
    when v_operation.actor_kind='OFFICE' then coalesce(
      nullif(v_operation.progress_json->>'previous_owning_timesheet_id','')::uuid,
      v_operation.timesheet_id,
      v_current_owner_timesheet_id
    )
    else v_current_owner_timesheet_id
  end;
  if v_update.operation_id is not null then
    perform set_config(
      'cloudtms.candidate_expected_delete_scope_sha256',
      coalesce(v_operation.progress_json->>'expected_delete_target_scope_sha256',''),true
    );
  else
    perform set_config('cloudtms.candidate_expected_delete_scope_sha256','',true);
  end if;
  if coalesce((v_financial_result->>'zero_expense_carrier')::boolean,false)
     and v_current_owner_timesheet_id is not null then
    v_delete_result:=private._candidate_zero_expense_carrier_delete_v1(
      v_environment,v_current_owner_timesheet_id,
      coalesce(v_update.operation_id,v_update.update_id),p_now_utc
    );
  elsif v_current_owner_timesheet_id is not null then
    v_delete_result:=jsonb_build_object(
      'owning_timesheet_deleted',false,
      'empty_timesheet_consequence','NONE',
      'retained_timesheet_ids',jsonb_build_array(v_current_owner_timesheet_id),
      'affected_timesheet_ids',jsonb_build_array(v_current_owner_timesheet_id),
      'removed_from_current_timesheet_ids','[]'::jsonb
    );
  end if;
  -- Category evidence (including same-amount replacements) is summary input.
  -- Queue every surviving exact owner after the atomic rebind; the queue is
  -- content-addressed, so unchanged inputs are an idempotent no-op.
  perform private._candidate_expense_summary_queue_v1(
    owner.timesheet_id,p_now_utc
  )
  from (
    select distinct private._candidate_expense_owned_timesheet_id_v1(
      component.workflow_id,component.owning_timesheet_id
    ) as timesheet_id
    from public.candidate_expense_components component
    where component.workflow_id=v_workflow.id
  ) owner
  where owner.timesheet_id is not null
    and exists(select 1 from public.timesheets row
      where row.timesheet_id=owner.timesheet_id and row.is_current
        and row.archived_at_utc is null);
  v_response:=jsonb_build_object(
    'ok',true,'workflow_id',v_workflow.id,'state',case
      when v_update.update_mode='PENDING_MANAGER' then 'AWAITING_MANAGER_APPROVAL'
      else 'AWAITING_PAPER_RETURN' end,
    'generation',v_workflow.generation,'update_state','NONE','update_id',v_update.update_id,
    'category_changes',v_update.update_plan_json,'approval_request_id',v_approval.id,
    'approval_request_generation',v_approval.request_generation,
    'manager_link_preserved',v_update.update_mode='PENDING_MANAGER',
    'paper_pack_replacement',v_update.update_mode='PAPER_REPLACEMENT',
    'old_pack_retired',v_update.update_mode='PAPER_REPLACEMENT',
    'previous_owning_timesheet_id',v_previous_owner_timesheet_id,
    'empty_timesheet_consequence',coalesce(
      v_delete_result->>'empty_timesheet_consequence','NONE'
    ),
    'owning_timesheet_deleted',coalesce(
      (v_delete_result->>'owning_timesheet_deleted')::boolean,false
    ),
    'deleted_timesheet_ids',coalesce(v_delete_result->'deleted_timesheet_ids','[]'::jsonb),
    'retained_timesheet_ids',coalesce(v_delete_result->'retained_timesheet_ids','[]'::jsonb),
    'affected_timesheet_ids',coalesce(v_delete_result->'affected_timesheet_ids','[]'::jsonb),
    'removed_from_current_timesheet_ids',coalesce(
      v_delete_result->'removed_from_current_timesheet_ids','[]'::jsonb
    ),
    'idempotent_replay',false
  );
  if v_update.operation_id is not null then
    if v_operation.actor_kind='OFFICE'
       and v_operation.action_code='REJECT_EXPENSE_CATEGORY'
       and v_operation.state in ('PREPARING','RENDERING') then
      select component.* into v_component
      from public.candidate_expense_components component
      where component.expense_component_id=v_operation.expense_component_id
        and component.lifecycle_state='OFFICE_REJECTED';
      if not found then
        raise exception 'CANDIDATE_EXPENSE_CATEGORY_REJECTION_NOT_COMMITTED'
          using errcode='40001';
      end if;
      v_operation_result:=jsonb_build_object(
        'ok',true,
        'contract_version','OFFICE_EXPENSE_CATEGORY_REJECTION_RESULT_V2',
        'operation_id',v_operation.operation_id,
        'action_code','REJECT_EXPENSE_CATEGORY',
        'workflow_id',v_operation.workflow_id,
        'expense_component_id',v_component.expense_component_id,
        'component_generation',v_component.component_generation,
        'state','OFFICE_REJECTED',
        'refusal',jsonb_build_object(
          'kind','AGENCY_REJECTION','reason',v_component.refusal_reason,
          'at_utc',v_component.refused_at_utc
        ),
        'previous_owning_timesheet_id',v_previous_owner_timesheet_id,
        'empty_timesheet_consequence',coalesce(
          v_delete_result->>'empty_timesheet_consequence','NONE'
        ),
        'owning_timesheet_deleted',coalesce(
          (v_delete_result->>'owning_timesheet_deleted')::boolean,false
        ),
        'deleted_timesheet_ids',coalesce(
          v_delete_result->'deleted_timesheet_ids','[]'::jsonb
        ),
        'retained_timesheet_ids',coalesce(
          v_delete_result->'retained_timesheet_ids','[]'::jsonb
        ),
        'removed_from_current_timesheet_ids',coalesce(
          v_delete_result->'removed_from_current_timesheet_ids','[]'::jsonb
        ),
        'affected_timesheet_ids',coalesce(
          v_delete_result->'affected_timesheet_ids',
          case when v_previous_owner_timesheet_id is null then '[]'::jsonb
            else jsonb_build_array(v_previous_owner_timesheet_id) end
        ),
        'refresh_timesheet_ids',coalesce(
          v_delete_result->'affected_timesheet_ids',
          case when v_previous_owner_timesheet_id is null then '[]'::jsonb
            else jsonb_build_array(v_previous_owner_timesheet_id) end
        ),
        'refresh_hints',jsonb_build_object(
          'summary',true,'simple_timesheet',true,'bulk_process',true,
          'bulk_authorise',true,'refetch','AFFECTED_ROWS'
        ),
        'idempotent_replay',false
      );
      update public.candidate_expense_operations operation set
        state='COMMITTED',result_json=v_operation_result,
        completed_at_utc=p_now_utc,updated_at_utc=p_now_utc
      where operation.operation_id=v_operation.operation_id
        and operation.state in ('PREPARING','RENDERING');
      if not found then
        raise exception 'CANDIDATE_EXPENSE_OPERATION_CHANGED' using errcode='40001';
      end if;
      v_response:=v_operation_result;
    elsif v_operation.actor_kind='CANDIDATE'
       and v_operation.state in ('PREPARING','RENDERING') then
      v_operation_result:=v_response||jsonb_build_object(
        'contract_version','CANDIDATE_EXPENSE_CATEGORY_ACTION_RESULT_V1',
        'operation_id',v_operation.operation_id,
        'action_code',v_operation.action_code,
        'expense_component_id',v_operation.expense_component_id,
        'idempotent_replay',false
      );
      if coalesce(
          (v_operation.progress_json->>'automatic_resubmission_required')::boolean,false
         ) then
        v_operation_result:=v_operation_result||jsonb_build_object(
          'old_pack_recoverable',false,
          'preserved_component_count',coalesce(
            (v_operation.progress_json->>'preserved_component_count')::integer,0
          ),
          'candidate_signature_component_id',
            v_operation.progress_json->'candidate_signature_component_id',
          'automatic_resubmission_required',true
        );
      end if;
      update public.candidate_expense_operations operation set
        state='COMMITTED',
        result_json=case when operation.action_code='RESUBMIT_EXPENSE_CATEGORY'
          then operation.progress_json||jsonb_build_object('idempotent_replay',false)
          else v_operation_result end,
        completed_at_utc=p_now_utc,updated_at_utc=p_now_utc
      where operation.operation_id=v_operation.operation_id
        and operation.state in ('PREPARING','RENDERING')
        and (
          operation.action_code<>'RESUBMIT_EXPENSE_CATEGORY'
          or operation.progress_json->>'contract_version'
            ='CANDIDATE_EXPENSE_CATEGORY_RESUBMISSION_RESULT_V1'
        );
      if not found then
        raise exception 'CANDIDATE_EXPENSE_OPERATION_CHANGED' using errcode='40001';
      end if;
      v_response:=v_operation_result;
    end if;
  end if;
  update public.candidate_pending_expense_updates set
    rebind_idempotency_key=btrim(p_idempotency_key),
    rebind_request_sha256=v_rebind_request_sha256,
    rebind_result_json=v_response,
    updated_at_utc=p_now_utc
  where update_id=v_update.update_id and state='COMMITTED';
  if not found then
    raise exception 'CANDIDATE_EXPENSE_UPDATE_RECEIPT_INVALID' using errcode='55000';
  end if;
  return v_response;
end;
$function$;

create or replace function public.candidate_expense_update_abort_atomic_v1(
  p_session_id uuid,
  p_environment text,
  p_workflow_id uuid,
  p_update_id uuid,
  p_service_recovery boolean default false,
  p_failure_code text default null,
  p_idempotency_key text default null,
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
  v_context jsonb;
  v_update public.candidate_pending_expense_updates%rowtype;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_prior public.candidate_submission_workflows%rowtype;
  v_prior_paper_source public.timesheets%rowtype;
  v_current_paper_source public.timesheets%rowtype;
  v_new_paper_mail_count integer:=0;
  v_paper_retirement jsonb;
  v_abort_request jsonb;
  v_abort_request_sha256 bytea;
  v_abort_result jsonb;
  v_operation_terminal_result jsonb;
begin
  if p_workflow_id is null or p_update_id is null
     or nullif(btrim(coalesce(p_idempotency_key,'')),'') is null
     or (p_service_recovery and p_session_id is not null) then
    raise exception 'CANDIDATE_EXPENSE_UPDATE_RECEIPT_INVALID' using errcode='22023';
  end if;
  if not p_service_recovery then
    v_context:=private._candidate_session_context_v1(
      p_session_id,v_environment,null,p_now_utc,true
    );
  end if;
  select * into v_workflow from public.candidate_submission_workflows workflow
  where workflow.id=p_workflow_id and workflow.environment=v_environment for update;
  if not found then raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_update from public.candidate_pending_expense_updates update_row
  where update_row.update_id=p_update_id and update_row.workflow_id=p_workflow_id
  for update;
  if not found then raise exception 'CANDIDATE_EXPENSE_UPDATE_NOT_FOUND' using errcode='P0002'; end if;
  if not p_service_recovery and (
    v_workflow.account_id is distinct from (v_context->>'account_id')::uuid
    or v_workflow.candidate_id is distinct from (v_context->>'selected_candidate_id')::uuid
  ) then raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002'; end if;
  v_abort_request:=jsonb_build_object(
    'contract_version','CANDIDATE_EXPENSE_UPDATE_ABORT_REQUEST_V1',
    'workflow_id',p_workflow_id,'update_id',p_update_id,
    'service_recovery',p_service_recovery,
    'failure_code',case when p_service_recovery then coalesce(
      nullif(upper(btrim(coalesce(p_failure_code,''))),''),
      'CANDIDATE_EXPENSE_UPDATE_FAILED'
    ) else null end
  );
  v_abort_request_sha256:=private._candidate_sha256_jsonb_v1(v_abort_request);
  if v_update.state in ('ABORTED','FAILED') then
    if v_update.abort_idempotency_key is distinct from btrim(p_idempotency_key)
       or v_update.abort_request_sha256 is distinct from v_abort_request_sha256
       or v_update.abort_result_json is null then
      raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT' using errcode='23505';
    end if;
    return v_update.abort_result_json||jsonb_build_object('idempotent_replay',true);
  end if;
  if v_update.state='COMMITTED' then
    raise exception 'CANDIDATE_EXPENSE_UPDATE_ALREADY_COMMITTED' using errcode='55000';
  end if;
  select * into v_prior from jsonb_populate_record(
    null::public.candidate_submission_workflows,v_update.prior_workflow_snapshot_json
  );
  if v_prior.id is distinct from v_workflow.id
     or v_prior.generation is distinct from v_update.from_workflow_generation
     or v_prior.state is distinct from v_update.prior_workflow_state
     or v_update.prior_immutable_submission_sha256 is distinct from
        private._candidate_sha256_jsonb_v1(v_update.prior_immutable_submission_json)
     or v_update.prior_review_manifest_sha256 is distinct from
        private._candidate_sha256_jsonb_v1(v_update.prior_review_manifest_json)
     or v_prior.immutable_submission_json is distinct from
        v_update.prior_immutable_submission_json
     or v_prior.immutable_submission_sha256 is distinct from
        v_update.prior_immutable_submission_sha256
     or v_prior.review_manifest_json is distinct from
        v_update.prior_review_manifest_json
     or v_prior.review_manifest_sha256 is distinct from
        v_update.prior_review_manifest_sha256 then
    raise exception 'CANDIDATE_EXPENSE_UPDATE_RECOVERY_SNAPSHOT_INVALID'
      using errcode='55000';
  end if;
  if v_update.update_mode='PAPER_REPLACEMENT'
     and v_workflow.generation>v_update.from_workflow_generation then
    if v_update.prior_paper_source_timesheet_id is null
       or v_update.prior_paper_source_snapshot_json is null
       or v_update.prior_paper_source_snapshot_sha256 is null
       or v_update.prior_paper_source_snapshot_sha256 is distinct from
          private._candidate_sha256_jsonb_v1(v_update.prior_paper_source_snapshot_json) then
      raise exception 'CANDIDATE_EXPENSE_UPDATE_RECOVERY_SNAPSHOT_INVALID'
        using errcode='55000';
    end if;
    select * into v_prior_paper_source from jsonb_populate_record(
      null::public.timesheets,v_update.prior_paper_source_snapshot_json
    );
    if v_prior_paper_source.timesheet_id is distinct from
         v_update.prior_paper_source_timesheet_id then
      raise exception 'CANDIDATE_EXPENSE_UPDATE_RECOVERY_SNAPSHOT_INVALID'
        using errcode='55000';
    end if;
    select count(*)::integer into v_new_paper_mail_count
    from public.mail_outbox mail_row
    where mail_row.type='TIMESHEET_QR'
      and mail_row.context_kind='timesheets'
      and mail_row.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
      and mail_row.payment_scope_json->>'candidate_workflow_generation'=v_workflow.generation::text
      and lower(coalesce(
        mail_row.payment_scope_json->>'candidate_paper_generation_retired','false'
      )) in ('false','f','0','no');
    if v_new_paper_mail_count>0 then
      if v_workflow.state<>'AWAITING_PAPER_RETURN' then
        raise exception 'CANDIDATE_EXPENSE_UPDATE_RECOVERY_PAPER_CONFLICT'
          using errcode='40001';
      end if;
      v_paper_retirement:=private._candidate_paper_delivery_retire_v1(
        v_workflow.id,v_workflow.generation,
        'PAPER_REPLACEMENT_ABORTED',p_now_utc
      );
      if not coalesce((v_paper_retirement->>'retired')::boolean,false) then
        raise exception 'CANDIDATE_EXPENSE_UPDATE_RECOVERY_PAPER_CONFLICT'
          using errcode='40001';
      end if;
      select source_row.* into v_current_paper_source
      from public.timesheets source_row
      where source_row.timesheet_id=v_update.prior_paper_source_timesheet_id
        and source_row.is_current and source_row.archived_at_utc is null
      for update;
      if not found
         or v_current_paper_source.booking_id is distinct from v_prior_paper_source.booking_id
         or v_current_paper_source.contract_id is distinct from v_prior_paper_source.contract_id
         or v_current_paper_source.week_ending_date is distinct from
            v_prior_paper_source.week_ending_date
         or v_current_paper_source.document_revision is distinct from
            v_prior_paper_source.document_revision then
        raise exception 'CANDIDATE_EXPENSE_UPDATE_RECOVERY_PAPER_CONFLICT'
          using errcode='40001';
      end if;
      update public.timesheets source_row set
        qr_token=v_prior_paper_source.qr_token,
        qr_status=v_prior_paper_source.qr_status,
        qr_payload_json=v_prior_paper_source.qr_payload_json,
        qr_generated_at=v_prior_paper_source.qr_generated_at,
        qr_scanned_at=v_prior_paper_source.qr_scanned_at,
        qr_scan_info_json=v_prior_paper_source.qr_scan_info_json,
        qr_r2_key=v_prior_paper_source.qr_r2_key,
        qr_last_sent_hash=v_prior_paper_source.qr_last_sent_hash,
        qr_last_sent_at_utc=v_prior_paper_source.qr_last_sent_at_utc,
        qr_signed_hash=v_prior_paper_source.qr_signed_hash,
        qr_signed_at_utc=v_prior_paper_source.qr_signed_at_utc,
        current_document_version_id=v_prior_paper_source.current_document_version_id,
        active_document_operation_id=v_prior_paper_source.active_document_operation_id,
        document_state=v_prior_paper_source.document_state,
        last_document_error_json=v_prior_paper_source.last_document_error_json,
        updated_at=p_now_utc
      where source_row.timesheet_id=v_current_paper_source.timesheet_id
        and source_row.is_current and source_row.archived_at_utc is null;
      if not found then
        raise exception 'CANDIDATE_EXPENSE_UPDATE_RECOVERY_PAPER_CONFLICT'
          using errcode='40001';
      end if;
    end if;
  end if;
  update public.candidate_submission_components set
    state='SUPERSEDED',superseded_at_utc=p_now_utc,
    review_render_state=case when review_render_state='NOT_REQUIRED'
      then review_render_state else 'SUPERSEDED' end,
    final_signed_render_state=case when final_signed_render_state='NOT_REQUIRED'
      then final_signed_render_state else 'SUPERSEDED' end
  where workflow_id=v_workflow.id
    and workflow_generation>v_update.from_workflow_generation
    and state<>'SUPERSEDED';
  update public.candidate_pending_expense_updates set
    state=case when p_service_recovery then 'FAILED' else 'ABORTED' end,
    failure_code=case when p_service_recovery then coalesce(
      nullif(upper(btrim(coalesce(p_failure_code,''))),''),'CANDIDATE_EXPENSE_UPDATE_FAILED'
    ) else null end,
    completed_at_utc=p_now_utc,updated_at_utc=p_now_utc
  where update_id=v_update.update_id;
  if v_update.operation_id is not null then
    update public.candidate_expense_operations operation set
      state=case when p_service_recovery then 'FAILED' else 'ABORTED' end,
      failure_code=case when p_service_recovery then coalesce(
        nullif(upper(btrim(coalesce(p_failure_code,''))),''),
        'CANDIDATE_EXPENSE_UPDATE_FAILED'
      ) else null end,
      completed_at_utc=p_now_utc,updated_at_utc=p_now_utc
    where operation.operation_id=v_update.operation_id
      and operation.state in ('PREPARING','RENDERING');
  end if;
  update public.candidate_submission_workflows set
    workflow_kind=v_prior.workflow_kind,scope=v_prior.scope,
    state=v_prior.state,generation=v_prior.generation,route=v_prior.route,
    contract_id=v_prior.contract_id,contract_week_id=v_prior.contract_week_id,
    anchor_timesheet_id=v_prior.anchor_timesheet_id,
    target_timesheet_id=v_prior.target_timesheet_id,
    work_date=v_prior.work_date,week_ending_date=v_prior.week_ending_date,
    input_snapshot_json=v_prior.input_snapshot_json,
    immutable_submission_json=v_prior.immutable_submission_json,
    immutable_submission_sha256=v_prior.immutable_submission_sha256,
    policy_snapshot_json=v_prior.policy_snapshot_json,
    policy_snapshot_sha256=v_prior.policy_snapshot_sha256,
    expected_row_signature=v_prior.expected_row_signature,
    capability_hash=v_prior.capability_hash,
    rejection_reason=v_prior.rejection_reason,
    rejection_scope=v_prior.rejection_scope,
    idempotency_key=v_prior.idempotency_key,
    last_mutation_idempotency_key=v_prior.last_mutation_idempotency_key,
    last_mutation_response_json=v_prior.last_mutation_response_json,
    issue_codes=v_prior.issue_codes,
    candidate_signature_component_id=v_prior.candidate_signature_component_id,
    candidate_signature_sha256=v_prior.candidate_signature_sha256,
    candidate_signed_at_utc=v_prior.candidate_signed_at_utc,
    review_manifest_json=v_prior.review_manifest_json,
    review_manifest_sha256=v_prior.review_manifest_sha256,
    paper_return_manifest_json=v_prior.paper_return_manifest_json,
    paper_return_manifest_sha256=v_prior.paper_return_manifest_sha256,
    renderer_contract_version=v_prior.renderer_contract_version,
    manager_name=v_prior.manager_name,manager_position=v_prior.manager_position,
    manager_signature_component_id=v_prior.manager_signature_component_id,
    manager_signature_sha256=v_prior.manager_signature_sha256,
    manager_approved_at_utc=v_prior.manager_approved_at_utc,
    worker_submitted_at_utc=v_prior.worker_submitted_at_utc,
    finalised_at_utc=v_prior.finalised_at_utc,
    cancelled_at_utc=v_prior.cancelled_at_utc,
    daily_context_sha256=v_prior.daily_context_sha256,
    canonical_financial_sha256=v_prior.canonical_financial_sha256,
    canonical_save_input_sha256=v_prior.canonical_save_input_sha256,
    canonical_save_row_signature=v_prior.canonical_save_row_signature,
    canonical_save_financials_id=v_prior.canonical_save_financials_id,
    canonical_save_receipt_json=v_prior.canonical_save_receipt_json,
    canonical_saved_at_utc=v_prior.canonical_saved_at_utc,
    replacement_of_workflow_id=v_prior.replacement_of_workflow_id,
    creation_request_sha256=v_prior.creation_request_sha256,
    creation_identity_json=v_prior.creation_identity_json,
    updated_at_utc=p_now_utc
  where id=v_workflow.id;
  perform private._candidate_expense_components_sync_v1(v_workflow.id,p_now_utc);
  v_abort_result:=jsonb_build_object(
    'ok',true,'contract_version','CANDIDATE_EXPENSE_UPDATE_ABORT_RESULT_V1',
    'action_code','ABORT_EXPENSE_UPDATE',
    'workflow_id',v_workflow.id,'state',v_prior.state,
    'generation',v_prior.generation,'update_state','NONE','update_id',v_update.update_id,
    'manager_link_preserved',v_update.update_mode='PENDING_MANAGER',
    'paper_pack_restored',v_update.update_mode='PAPER_REPLACEMENT',
    'idempotent_replay',false
  );
  if v_update.operation_id is not null then
    select jsonb_build_object(
      'contract_version','CANDIDATE_EXPENSE_OPERATION_TERMINAL_RESULT_V1',
      'ok',false,'operation_id',operation.operation_id,
      'action_code',operation.action_code,
      'workflow_id',operation.workflow_id,
      'expense_component_id',operation.expense_component_id,
      'state',case when p_service_recovery then 'FAILED' else 'ABORTED' end,
      'error_code',case when p_service_recovery then coalesce(
        nullif(upper(btrim(coalesce(p_failure_code,''))),''),
        'CANDIDATE_EXPENSE_UPDATE_FAILED'
      ) else 'CANDIDATE_EXPENSE_OPERATION_ABORTED' end,
      'update_id',v_update.update_id,
      'paper_pack_restored',v_update.update_mode='PAPER_REPLACEMENT',
      'idempotent_replay',false
    ) into v_operation_terminal_result
    from public.candidate_expense_operations operation
    where operation.operation_id=v_update.operation_id;
    update public.candidate_expense_operations operation set
      result_json=v_operation_terminal_result,updated_at_utc=p_now_utc
    where operation.operation_id=v_update.operation_id
      and operation.state=case when p_service_recovery then 'FAILED' else 'ABORTED' end;
    if not found then
      raise exception 'CANDIDATE_EXPENSE_OPERATION_CHANGED' using errcode='40001';
    end if;
  end if;
  update public.candidate_pending_expense_updates update_row set
    abort_idempotency_key=btrim(p_idempotency_key),
    abort_request_sha256=v_abort_request_sha256,
    abort_result_json=v_abort_result,updated_at_utc=p_now_utc
  where update_row.update_id=v_update.update_id
    and update_row.state=case when p_service_recovery then 'FAILED' else 'ABORTED' end
    and update_row.abort_idempotency_key is null;
  if not found then
    raise exception 'CANDIDATE_EXPENSE_UPDATE_CHANGED' using errcode='40001';
  end if;
  return v_abort_result;
end;
$function$;

create or replace function public.candidate_expense_update_manager_hold_v1(
  p_environment text,
  p_workflow_id uuid,
  p_approval_token_hash_hex text,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_update public.candidate_pending_expense_updates%rowtype;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_approval public.candidate_approval_requests%rowtype;
  v_recovery jsonb;
begin
  if coalesce(p_approval_token_hash_hex,'') !~ '^[0-9a-fA-F]{64}$' then return null; end if;
  -- Match the BEGIN/REBIND/ABORT lock order. If a concurrent rebind committed
  -- first, the active update disappears and the same manager link proceeds
  -- normally instead of receiving a spurious recovery error.
  select workflow.* into v_workflow
  from public.candidate_submission_workflows workflow
  where workflow.id=p_workflow_id
    and workflow.environment=private._candidate_assert_environment(p_environment)
  for update;
  if not found then return null; end if;
  select update_row.* into v_update
  from public.candidate_pending_expense_updates update_row
  where update_row.workflow_id=p_workflow_id
    and update_row.state in ('EDITING','RENDERING')
  order by update_row.started_at_utc desc limit 1
  for update;
  if not found then return null; end if;
  select * into v_approval from public.candidate_approval_requests request
  where request.id=v_update.approval_request_id
    and request.workflow_id=p_workflow_id and request.state='PENDING'
    and request.token_hash=decode(lower(p_approval_token_hash_hex),'hex');
  if not found then return null; end if;
  if v_update.expires_at_utc<=p_now_utc or v_approval.expires_at_utc<=p_now_utc then
    v_recovery:=public.candidate_expense_update_abort_atomic_v1(
      null,p_environment,p_workflow_id,v_update.update_id,true,
      'CANDIDATE_EXPENSE_UPDATE_EXPIRED',
      'expense-update-expired:'||v_update.update_id::text,p_now_utc
    );
    if coalesce(v_recovery->>'update_state','')<>'NONE' then
      raise exception 'CANDIDATE_EXPENSE_UPDATE_RECOVERY_FAILED' using errcode='40001';
    end if;
    return null;
  end if;
  return jsonb_build_object(
    'ok',true,'state','UPDATING',
    'status_code','MANAGER_APPROVAL_REQUEST_UPDATING',
    'message','This claim is being updated. It will open automatically when ready.',
    'retry_after_seconds',2,'workflow_id',p_workflow_id,
    'approval_request_id',v_approval.id,
    'approval_request_generation',v_approval.request_generation
  );
end;
$function$;

-- A bounded service watchdog restores the last complete frozen claim after a
-- Candidate abandons editing or a renderer never returns.  It never commits a
-- half-built generation and therefore cannot leave the manager link held
-- indefinitely.
create or replace function public.candidate_expense_update_recover_expired_v1(
  p_environment text,
  p_limit integer default 20,
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
  v_target record;
  v_result jsonb;
  v_recovered jsonb:='[]'::jsonb;
  v_failed jsonb:='[]'::jsonb;
  v_terminal_state text;
begin
  if p_limit not between 1 and 100 then
    raise exception 'CANDIDATE_EXPENSE_UPDATE_RECOVERY_LIMIT_INVALID' using errcode='22023';
  end if;
  for v_target in
    select update_row.update_id,update_row.workflow_id
    from public.candidate_pending_expense_updates update_row
    join public.candidate_submission_workflows workflow
      on workflow.id=update_row.workflow_id
    where workflow.environment=v_environment
      and update_row.state in ('EDITING','RENDERING')
      and update_row.expires_at_utc<=p_now_utc
    order by update_row.expires_at_utc,update_row.update_id
    limit p_limit
  loop
    begin
      v_result:=public.candidate_expense_update_abort_atomic_v1(
        null,v_environment,v_target.workflow_id,v_target.update_id,true,
        'CANDIDATE_EXPENSE_UPDATE_EXPIRED',
        'expense-update-expired:'||v_target.update_id::text,p_now_utc
      );
      v_recovered:=v_recovered||jsonb_build_array(jsonb_build_object(
        'workflow_id',v_target.workflow_id,'update_id',v_target.update_id,
        'state',v_result->>'state'
      ));
    exception when sqlstate '40001' or sqlstate '55000' or sqlstate 'P0002' then
      select update_row.state into v_terminal_state
      from public.candidate_pending_expense_updates update_row
      where update_row.update_id=v_target.update_id;
      if v_terminal_state in ('COMMITTED','ABORTED','FAILED') then
        v_recovered:=v_recovered||jsonb_build_array(jsonb_build_object(
          'workflow_id',v_target.workflow_id,'update_id',v_target.update_id,
          'state',v_terminal_state,'concurrent_terminal',true
        ));
      else
        v_failed:=v_failed||jsonb_build_array(jsonb_build_object(
          'workflow_id',v_target.workflow_id,'update_id',v_target.update_id,
          'code',sqlerrm
        ));
      end if;
    end;
  end loop;
  return jsonb_build_object(
    'ok',jsonb_array_length(v_failed)=0,
    'recovered',v_recovered,'recovered_count',jsonb_array_length(v_recovered),
    'failed',v_failed,'failed_count',jsonb_array_length(v_failed)
  );
end;
$function$;

-- The hours decision is projected from the exact immutable HOURS_TIMESHEET
-- member of the submitted workflow.  An expense-only refusal can therefore
-- never be presented as a refusal of otherwise unaffected hours.
create or replace function private._candidate_hours_component_json_v1(
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
  v_timesheet public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_candidate_id uuid;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_approval public.candidate_approval_requests%rowtype;
  v_document_generation integer;
  v_lifecycle text;
  v_manager text;
  v_agency text;
begin
  select row.* into v_timesheet from public.timesheets row
  where row.timesheet_id=p_timesheet_id;
  if not found then return null; end if;
  select row.* into v_fin from public.timesheets_financials row
  where row.timesheet_id=p_timesheet_id and row.is_current
  order by row.computed_at_utc desc nulls last,row.updated_at desc,row.id desc limit 1;
  if not found or v_fin.candidate_id is null then return null; end if;
  v_candidate_id:=v_fin.candidate_id;
  select workflow.* into v_workflow
  from public.candidate_submission_workflows workflow
  where workflow.environment=private._candidate_assert_environment(p_environment)
    and workflow.workflow_kind in ('CONTRACT_HOURS','CONTRACT_COMBINED')
    and workflow.candidate_id=v_candidate_id
    and workflow.contract_id=v_timesheet.contract_id
    and workflow.week_ending_date=v_timesheet.week_ending_date
    and workflow.target_timesheet_id is not null
    and private._candidate_expense_current_timesheet_id_v1(
      workflow.id,workflow.target_timesheet_id
    )=p_timesheet_id
    and exists(
      select 1 from public.candidate_submission_components component
      where component.workflow_id=workflow.id
        and component.workflow_generation=case when workflow.state='FINALISED'
          then greatest(workflow.generation-1,1) else workflow.generation end
        and component.component_kind='HOURS_TIMESHEET'
        and component.state not in ('ABANDONED','REJECTED')
    )
  order by workflow.updated_at_utc desc,workflow.id desc
  limit 1;
  if not found then return null; end if;
  v_document_generation:=case when v_workflow.state='FINALISED'
    then greatest(v_workflow.generation-1,1) else v_workflow.generation end;
  select request.* into v_approval from public.candidate_approval_requests request
  where request.workflow_id=v_workflow.id
    and request.workflow_generation=v_document_generation
  order by request.request_generation desc,request.updated_at_utc desc,request.id desc
  limit 1;
  select row.* into v_fin from public.timesheets_financials row
  where row.timesheet_id=private._candidate_expense_current_timesheet_id_v1(
      v_workflow.id,v_workflow.target_timesheet_id
    ) and row.is_current
  order by row.computed_at_utc desc nulls last,row.updated_at desc,row.id desc limit 1;
  v_lifecycle:=case
    when v_workflow.state='REFUSED' then 'MANAGER_REFUSED'
    when v_workflow.state='REJECTED' then 'OFFICE_REJECTED'
    when v_workflow.state='CANCELLED' then 'CANCELLED'
    when v_workflow.state in ('EXPIRED','SUPERSEDED') then 'SUPERSEDED'
    when v_workflow.state in ('MANAGER_APPROVED','MANAGER_APPROVED_PENDING_FINAL_DOCUMENT',
      'READY_TO_FINALISE','FINALISED','RECEIVED') then 'MANAGER_APPROVED'
    when v_workflow.state in ('CREATED','WORKER_DRAFT') then 'DRAFT'
    else 'SUBMITTED' end;
  v_manager:=case
    when v_workflow.state='REFUSED' or v_approval.state='REFUSED' then 'REFUSED'
    when v_workflow.manager_approved_at_utc is not null or v_approval.state='APPROVED'
      or v_lifecycle='MANAGER_APPROVED' then 'APPROVED'
    when v_approval.state='PENDING' then 'PENDING'
    else 'NOT_REQUESTED' end;
  v_agency:=case
    when v_fin.paid_at_utc is not null or upper(coalesce(v_timesheet.status::text,''))='PAID'
      then 'PAID'
    when v_fin.locked_by_invoice_id is not null
      or upper(coalesce(v_timesheet.status::text,''))='INVOICED' then 'INVOICED'
    when v_fin.authorised_at_utc is not null or v_timesheet.authorised_at_server is not null
      or upper(coalesce(v_timesheet.status::text,'')) in ('AUTHORISED','AUTHORIZED')
      then 'AUTHORISED'
    else 'NOT_AUTHORISED' end;
  return jsonb_build_object(
    'component_kind','TIMESHEET_HOURS','workflow_id',v_workflow.id,
    'workflow_generation',v_document_generation,'state',v_lifecycle,
    'status_code',private._candidate_expense_component_status_v1(
      v_lifecycle,v_manager,v_agency
    ),'manager_approval_state',v_manager,'agency_authorisation_state',v_agency,
    'manager_decision_id',private._candidate_manager_decision_id_v1(
      v_workflow.id,v_document_generation,v_approval.id,v_manager
    ),
    'submitted_at_utc',v_workflow.worker_submitted_at_utc,
    'manager_approved_at_utc',v_workflow.manager_approved_at_utc,
    'refusal',case when v_lifecycle in ('MANAGER_REFUSED','OFFICE_REJECTED')
      then jsonb_build_object(
        'kind',case when v_lifecycle='MANAGER_REFUSED'
          then 'MANAGER_REFUSAL' else 'AGENCY_REJECTION' end,
        'reason',coalesce(v_approval.refusal_reason,v_workflow.rejection_reason,
          'No reason recorded'),
        'at_utc',coalesce(v_approval.refused_at_utc,v_workflow.updated_at_utc),
        'decision_id',case when v_lifecycle='MANAGER_REFUSED'
          then v_approval.id else null end
      ) else null end,
    'protected',v_agency<>'NOT_AUTHORISED'
  );
end;
$function$;

-- One category action is governed only by its owning Timesheet.  Pending
-- electronic removal enters the same-link update protocol; draft and
-- manager-approved removal change only the addressed category.
create or replace function private._candidate_empty_manager_request_cancel_v1(
  p_workflow_id uuid,
  p_expected_generation integer,
  p_reason_code text,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_workflow public.candidate_submission_workflows%rowtype;
  v_request public.candidate_approval_requests%rowtype;
  v_request_ids uuid[]:=array[]::uuid[];
  v_retirement jsonb:='{}'::jsonb;
  v_settings jsonb;
  v_template jsonb;
  v_terminal jsonb;
  v_submission_type text;
  v_cancelled_count integer:=0;
  v_mail_count integer:=0;
  v_request_id uuid;
begin
  select workflow.* into v_workflow
  from public.candidate_submission_workflows workflow
  where workflow.id=p_workflow_id and workflow.generation=p_expected_generation
    and workflow.state='AWAITING_MANAGER_APPROVAL'
  for update;
  if not found then
    raise exception 'CANDIDATE_EXPENSE_UPDATE_APPROVAL_CHANGED' using errcode='40001';
  end if;
  select coalesce(array_agg(locked.id order by locked.id),array[]::uuid[])
  into v_request_ids from (
    select request.id from public.candidate_approval_requests request
    where request.workflow_id=v_workflow.id
      and request.workflow_generation=v_workflow.generation
      and request.method='EMAIL' and request.state='PENDING'
    order by request.id for update
  ) locked;
  if cardinality(v_request_ids)>0 then
    v_retirement:=private._candidate_manager_mail_retire_v1(
      v_workflow.id,v_workflow.generation,v_request_ids,p_reason_code,p_now_utc
    );
    foreach v_request_id in array v_request_ids loop
      perform public.candidate_manager_email_route_receipt_retire_v1(
        v_workflow.id,v_request_id,p_reason_code,p_now_utc
      );
    end loop;
  end if;
  update public.candidate_approval_requests request set
    state='CANCELLED',cancelled_at_utc=p_now_utc,updated_at_utc=p_now_utc
  where request.workflow_id=v_workflow.id
    and request.workflow_generation=v_workflow.generation
    and request.state='PENDING';
  get diagnostics v_cancelled_count=row_count;
  if v_cancelled_count<>1 then
    raise exception 'CANDIDATE_EXPENSE_UPDATE_APPROVAL_CHANGED' using errcode='40001';
  end if;
  if coalesce((v_retirement->>'withdrawal_required')::boolean,false) then
    v_settings:=public.candidate_manager_email_settings_get_v1();
    v_submission_type:=case v_workflow.workflow_kind
      when 'CONTRACT_EXPENSE' then 'EXPENSE_CLAIM'
      when 'CONTRACT_COMBINED' then 'COMBINED' else 'TIMESHEET' end;
    v_template:=v_settings#>array['templates',v_submission_type,'CANCELLATION'];
    v_terminal:=jsonb_build_object(
      'subject',v_template->>'subject','body_text',v_template->>'body_text',
      'body_html',v_template->>'body_html','manager_template_version',v_settings->'version',
      'manager_template_sha256',v_settings->>'semantic_sha256_hex',
      'manager_submission_type',v_submission_type
    );
    for v_request in
      select request.* from public.candidate_approval_requests request
      where request.id=any(v_request_ids) and request.state='CANCELLED'
      order by request.id
    loop
      perform private._candidate_queue_mail_v1(
        private._candidate_manager_terminal_mail_payload_v1(
          v_terminal,'CANCELLATION'
        )||jsonb_build_object('payment_scope_json',jsonb_build_object(
          'candidate_mail_authority','MANAGER_APPROVAL_V1',
          'candidate_manager_mail_kind','CANCELLATION',
          'candidate_manager_workflow_id',v_workflow.id,
          'candidate_manager_workflow_generation',v_workflow.generation,
          'candidate_approval_request_id',v_request.id,
          'candidate_approval_request_generation',v_request.request_generation,
          'candidate_manager_template_version',(v_terminal->>'manager_template_version')::bigint,
          'candidate_manager_template_sha256',v_terminal->>'manager_template_sha256',
          'candidate_manager_submission_type',v_terminal->>'manager_submission_type',
          'candidate_manager_mail_retired',false
        )),v_request.manager_email_normalized,
        'CANDIDATE_MANAGER_CANCELLATION_V1:'||v_request.id::text||':'||v_workflow.generation::text,
        'candidate-manager-cancellation:'||v_request.id::text,v_workflow.id,p_now_utc
      );
      v_mail_count:=v_mail_count+1;
    end loop;
  end if;
  return jsonb_build_object(
    'cancelled_request_count',v_cancelled_count,
    'manager_mail_retirement',v_retirement,
    'manager_cancellation_mail_count',v_mail_count
  );
end;
$function$;

create or replace function public.candidate_expense_component_action_atomic_v1(
  p_session_id uuid,
  p_environment text,
  p_workflow_id uuid,
  p_expected_generation integer,
  p_expense_component_id uuid,
  p_expected_component_generation integer,
  p_action text,
  p_idempotency_key text,
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
  v_action text:=upper(btrim(coalesce(p_action,'')));
  v_context jsonb;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_component public.candidate_expense_components%rowtype;
  v_before jsonb;
  v_financial jsonb;
  v_begin jsonb;
  v_zero boolean:=false;
  v_delete_result jsonb:='{}'::jsonb;
  v_request jsonb;
  v_request_sha bytea;
  v_operation public.candidate_expense_operations%rowtype;
  v_result jsonb;
  v_removal_context jsonb;
  v_direct_empty_pending boolean:=false;
begin
  if p_workflow_id is null or p_expected_generation is null
     or p_expense_component_id is null or p_expected_component_generation is null
     or v_action not in ('REMOVE_EXPENSE','WITHDRAW_EXPENSE','CANCEL_EXPENSE')
     or nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then
    raise exception 'CANDIDATE_EXPENSE_COMPONENT_ACTION_INVALID' using errcode='22023';
  end if;
  v_context:=private._candidate_session_context_v1(
    p_session_id,v_environment,null,p_now_utc,true
  );
  -- Resolve an exact replay before consulting mutable workflow/component
  -- generations.  A successful category action necessarily advances those
  -- generations, so checking them first would make a lost 200 response
  -- impossible to reconcile safely.
  v_request:=jsonb_build_object(
    'contract_version','CANDIDATE_EXPENSE_CATEGORY_ACTION_REQUEST_V1',
    'workflow_id',p_workflow_id,'workflow_generation',p_expected_generation,
    'expense_component_id',p_expense_component_id,
    'component_generation',p_expected_component_generation,'action_code',v_action
  );
  v_request_sha:=private._candidate_sha256_jsonb_v1(v_request);
  perform pg_advisory_xact_lock(hashtextextended(
    'candidate-expense-operation|'||v_environment||'|'
      ||(v_context->>'selected_candidate_id')||'|'||btrim(p_idempotency_key),0
  ));
  select operation.* into v_operation
  from public.candidate_expense_operations operation
  where operation.environment=v_environment
    and operation.actor_kind='CANDIDATE'
    and operation.actor_id=(v_context->>'selected_candidate_id')::uuid
    and operation.idempotency_key=btrim(p_idempotency_key)
  for update;
  if found then
    if v_operation.action_code<>v_action or v_operation.request_sha256<>v_request_sha then
      raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT' using errcode='23505';
    end if;
    if v_operation.state='COMMITTED' then
      return v_operation.result_json||jsonb_build_object('idempotent_replay',true);
    end if;
    if v_operation.state in ('FAILED','ABORTED')
       and v_operation.result_json is not null then
      return v_operation.result_json||jsonb_build_object('idempotent_replay',true);
    end if;
    if v_operation.state='RENDERING' and v_operation.progress_json is not null then
      return v_operation.progress_json||jsonb_build_object('idempotent_replay',true);
    end if;
    raise exception 'CANDIDATE_EXPENSE_OPERATION_IN_PROGRESS' using errcode='55000';
  end if;
  select workflow.* into v_workflow from public.candidate_submission_workflows workflow
  where workflow.id=p_workflow_id and workflow.environment=v_environment
    and workflow.account_id=(v_context->>'account_id')::uuid
    and workflow.candidate_id=(v_context->>'selected_candidate_id')::uuid
  for update;
  if not found then raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002'; end if;
  select component.* into v_component from public.candidate_expense_components component
  where component.expense_component_id=p_expense_component_id
    and component.workflow_id=v_workflow.id
  for update;
  if not found then raise exception 'CANDIDATE_EXPENSE_COMPONENT_NOT_FOUND' using errcode='P0002'; end if;
  if v_workflow.generation<>p_expected_generation
     or v_component.component_generation<>p_expected_component_generation then
    raise exception 'CANDIDATE_EXPENSE_COMPONENT_CHANGED' using errcode='40001';
  end if;
  if v_component.agency_authorisation_state<>'NOT_AUTHORISED' then
    raise exception 'CANDIDATE_EXPENSE_COMPONENT_PROTECTED' using errcode='55000';
  end if;

  if v_action='WITHDRAW_EXPENSE' and v_workflow.route='PAPER'
     and v_workflow.state='AWAITING_PAPER_RETURN'
     and v_component.lifecycle_state='SUBMITTED'
     and v_component.manager_approval_state='NOT_REQUESTED' then
    return jsonb_build_object(
      'ok',true,'contract_version','CANDIDATE_EXPENSE_CATEGORY_ACTION_RESULT_V1',
      'operation_id',null,'workflow_id',v_workflow.id,'generation',v_workflow.generation,
      'expense_component_id',v_component.expense_component_id,
      'component_generation',v_component.component_generation,
      'state',v_component.lifecycle_state,'update_state','NONE',
      'action_code',v_action,'paper_replacement_required',true,
      'empty_timesheet_consequence','NONE',
      'removed_from_current_timesheet_ids','[]'::jsonb,
      'paper_replacement_action','CREATE_UPDATED_DOCUMENTS',
      'paper_replacement_category_changes',jsonb_build_array(jsonb_build_object(
        'update_kind','REMOVE_CATEGORY',
        'expense_category',v_component.expense_category,
        'expense_component_id',v_component.expense_component_id,
        'component_generation',v_component.component_generation
      )),
      'idempotent_replay',false
    );
  end if;

  if (v_action='REMOVE_EXPENSE' and v_component.lifecycle_state<>'DRAFT')
     or (v_action='WITHDRAW_EXPENSE' and not (
       v_component.lifecycle_state='SUBMITTED'
       and (
         (v_component.manager_approval_state='PENDING' and v_workflow.route<>'PAPER')
         or (v_component.manager_approval_state='NOT_REQUESTED'
           and v_workflow.route='PAPER'
           and v_workflow.state='AWAITING_PAPER_RETURN')
       )
     ))
     or (v_action='CANCEL_EXPENSE' and not (
       v_component.lifecycle_state='MANAGER_APPROVED'
       and v_component.manager_approval_state='APPROVED'
       and v_workflow.state='FINALISED'
     )) then
    raise exception 'CANDIDATE_EXPENSE_COMPONENT_ACTION_NOT_ALLOWED' using errcode='55000';
  end if;
  insert into public.candidate_expense_operations(
    environment,account_id,candidate_id,actor_kind,actor_id,action_code,
    workflow_id,timesheet_id,expense_component_id,request_sha256,idempotency_key,
    state,created_at_utc,updated_at_utc
  ) values (
    v_environment,v_workflow.account_id,v_workflow.candidate_id,'CANDIDATE',
    v_workflow.candidate_id,v_action,v_workflow.id,
    private._candidate_expense_owned_timesheet_id_v1(
      v_workflow.id,v_component.owning_timesheet_id
    ),v_component.expense_component_id,v_request_sha,btrim(p_idempotency_key),
    'PREPARING',p_now_utc,p_now_utc
  ) returning * into v_operation;

  if v_action='WITHDRAW_EXPENSE' and v_component.lifecycle_state='SUBMITTED'
     and v_component.manager_approval_state='PENDING'
     and v_workflow.route<>'PAPER' then
    v_removal_context:=private._candidate_office_expense_rejection_context_v1(
      v_environment,v_component.expense_component_id,p_now_utc
    );
    v_direct_empty_pending:=v_workflow.workflow_kind='CONTRACT_EXPENSE'
      and coalesce(v_removal_context#>>'{basis,empty_timesheet_consequence}','NONE')
        <>'NONE'
      and not exists(
        select 1 from public.candidate_expense_components other_component
        where other_component.workflow_id=v_workflow.id
          and other_component.expense_component_id<>v_component.expense_component_id
          and other_component.lifecycle_state not in (
            'MANAGER_REFUSED','OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED'
          )
    );
    if not v_direct_empty_pending then
      v_begin:=public.candidate_expense_update_begin_atomic_v1(
        p_session_id,v_environment,v_workflow.id,v_workflow.generation,
        jsonb_build_array(jsonb_build_object(
          'update_kind','REMOVE_CATEGORY',
          'expense_category',v_component.expense_category,
          'expense_component_id',v_component.expense_component_id,
          'component_generation',v_component.component_generation
        )),'category-withdraw-begin:'||btrim(p_idempotency_key),p_now_utc
      );
      update public.candidate_pending_expense_updates set
        operation_id=v_operation.operation_id,updated_at_utc=p_now_utc
      where update_id=(v_begin->>'update_id')::uuid and state='EDITING';
      if not found then
        raise exception 'CANDIDATE_EXPENSE_UPDATE_APPROVAL_CHANGED' using errcode='40001';
      end if;
      update public.candidate_expense_operations set state='RENDERING',
        progress_json=v_begin||jsonb_build_object(
          'operation_id',v_operation.operation_id,'action_code',v_action,
          'automatic_resubmission_required',true
        ),updated_at_utc=p_now_utc
      where operation_id=v_operation.operation_id and state='PREPARING';
      return v_begin||jsonb_build_object(
        'operation_id',v_operation.operation_id,'action_code',v_action,
        'automatic_resubmission_required',true,'idempotent_replay',false
      );
    end if;
    perform private._candidate_empty_manager_request_cancel_v1(
      v_workflow.id,v_workflow.generation,'EXPENSE_CATEGORY_WITHDRAWN',p_now_utc
    );
  end if;
  v_before:=to_jsonb(v_component);
  -- Always return the same closed financial receipt.  A target-less draft is a
  -- proved no-op (`financial_changed=false`), not an untyped empty object.
  v_financial:=private._candidate_expense_financial_remove_v1(
    v_component.expense_component_id,v_component.component_generation,p_now_utc
  );
  v_zero:=coalesce((v_financial->>'zero_expense_carrier')::boolean,false);
  if v_action='REMOVE_EXPENSE' then
    update public.candidate_submission_workflows set
      input_snapshot_json=private._candidate_expense_submission_without_category_v1(
        input_snapshot_json,v_component.expense_category
      ),
      immutable_submission_json=case when immutable_submission_json is null then null
        else private._candidate_expense_submission_without_category_v1(
          immutable_submission_json,v_component.expense_category
        ) end,
      immutable_submission_sha256=case when immutable_submission_json is null then null
        else private._candidate_sha256_jsonb_v1(
          private._candidate_expense_submission_without_category_v1(
            immutable_submission_json,v_component.expense_category
          )
        ) end,
      updated_at_utc=p_now_utc
    where id=v_workflow.id and generation=v_workflow.generation;
  end if;
  update public.candidate_submission_components set
    state='SUPERSEDED',superseded_at_utc=p_now_utc,
    review_render_state=case when review_render_state='NOT_REQUIRED'
      then review_render_state else 'SUPERSEDED' end,
    final_signed_render_state=case when final_signed_render_state='NOT_REQUIRED'
      then final_signed_render_state else 'SUPERSEDED' end
  where workflow_id=v_workflow.id
    and expense_category=v_component.expense_category
    and state<>'SUPERSEDED';
  update public.candidate_expense_components set
    component_generation=component_generation+1,
    lifecycle_state=case when v_action='REMOVE_EXPENSE' then 'SUPERSEDED'
      when v_action='WITHDRAW_EXPENSE' then 'WITHDRAWN' else 'CANCELLED' end,
    manager_approval_state=case when v_action='CANCEL_EXPENSE'
      then manager_approval_state else 'NOT_REQUESTED' end,
    approval_request_id=case when v_action='CANCEL_EXPENSE'
      then approval_request_id else null end,
    removed_at_utc=p_now_utc,updated_at_utc=p_now_utc
  where expense_component_id=v_component.expense_component_id
  returning * into v_component;
  insert into public.candidate_expense_component_events(
    expense_component_id,workflow_id,component_generation,event_type,actor_kind,
    actor_id,before_state_json,after_state_json,idempotency_key,occurred_at_utc
  ) values (
    v_component.expense_component_id,v_workflow.id,v_component.component_generation,
    case when v_action='REMOVE_EXPENSE' then 'SUPERSEDED'
      when v_action='WITHDRAW_EXPENSE' then 'WITHDRAWN' else 'CANCELLED' end,
    'CANDIDATE',v_workflow.candidate_id,v_before,to_jsonb(v_component),
    'candidate-category-action:'||btrim(p_idempotency_key),p_now_utc
  );
  -- Removing an unsubmitted draft is an immediate Candidate edit, not a
  -- cancellation event. Durable/in-app and push-outbox notices are reserved
  -- for a submitted withdrawal or an approved-category cancellation.
  if v_action in ('WITHDRAW_EXPENSE','CANCEL_EXPENSE') then
    perform private._candidate_notification_insert_v1(
      v_workflow.account_id,v_workflow.candidate_id,v_workflow.id,
      nullif(v_financial->>'timesheet_id','')::uuid,
      case when v_action='WITHDRAW_EXPENSE' then 'EXPENSE_WITHDRAWN'
        else 'EXPENSE_CANCELLED' end,'timesheet_expense_attention',
      case when v_action='WITHDRAW_EXPENSE'
        then 'candidate-expense-category-withdrawn-v1'
        else 'candidate-expense-category-cancelled-v1' end,jsonb_build_object(
        'workflow_id',v_workflow.id,
        'expense_component_id',v_component.expense_component_id,
        'expense_category',v_component.expense_category
      ),jsonb_build_object('type','workflow','workflow_id',v_workflow.id),
      case when v_action='WITHDRAW_EXPENSE'
        then 'CANDIDATE_EXPENSE_CATEGORY_WITHDRAWN_V1:'
        else 'CANDIDATE_EXPENSE_CATEGORY_CANCELLED_V1:' end
        ||v_component.expense_component_id::text
        ||':'||v_component.component_generation::text,p_now_utc
    );
  end if;
  if v_direct_empty_pending then
    update public.candidate_submission_workflows workflow set
      state='CANCELLED',generation=workflow.generation+1,
      cancelled_at_utc=p_now_utc,updated_at_utc=p_now_utc
    where workflow.id=v_workflow.id
      and workflow.generation=v_workflow.generation
      and workflow.state='AWAITING_MANAGER_APPROVAL'
    returning workflow.* into v_workflow;
    if not found then
      raise exception 'CANDIDATE_EXPENSE_UPDATE_APPROVAL_CHANGED' using errcode='40001';
    end if;
  end if;
  if v_zero and nullif(v_financial->>'timesheet_id','') is not null then
    v_delete_result:=private._candidate_zero_expense_carrier_delete_v1(
      v_environment,(v_financial->>'timesheet_id')::uuid,
      v_operation.operation_id,p_now_utc
    );
  elsif nullif(v_financial->>'timesheet_id','') is not null then
    v_delete_result:=jsonb_build_object(
      'owning_timesheet_deleted',false,
      'empty_timesheet_consequence','NONE',
      'deleted_timesheet_ids','[]'::jsonb,
      'retained_timesheet_ids',jsonb_build_array(v_financial->'timesheet_id'),
      'affected_timesheet_ids',jsonb_build_array(v_financial->'timesheet_id'),
      'removed_from_current_timesheet_ids','[]'::jsonb,
      'r2_cleanup_keys','[]'::jsonb
    );
  end if;
  v_result:=jsonb_build_object(
    'ok',true,'contract_version','CANDIDATE_EXPENSE_CATEGORY_ACTION_RESULT_V1',
    'operation_id',v_operation.operation_id,
    'workflow_id',v_workflow.id,'generation',v_workflow.generation,
    'expense_component_id',v_component.expense_component_id,
    'component_generation',v_component.component_generation,
    'state',v_component.lifecycle_state,'update_state','NONE',
    'action_code',v_action,'financial_result',coalesce(v_financial,'{}'::jsonb),
    'zero_expense_carrier',v_zero,
    'empty_timesheet_consequence',coalesce(
      v_delete_result->>'empty_timesheet_consequence','NONE'
    ),
    'owning_timesheet_deleted',coalesce(
      (v_delete_result->>'owning_timesheet_deleted')::boolean,false
    ),
    'deleted_timesheet_ids',coalesce(v_delete_result->'deleted_timesheet_ids','[]'::jsonb),
    'retained_timesheet_ids',coalesce(v_delete_result->'retained_timesheet_ids','[]'::jsonb),
    'affected_timesheet_ids',coalesce(v_delete_result->'affected_timesheet_ids','[]'::jsonb),
    'removed_from_current_timesheet_ids',coalesce(
      v_delete_result->'removed_from_current_timesheet_ids','[]'::jsonb
    ),
    'r2_cleanup_keys',coalesce(v_delete_result->'r2_cleanup_keys','[]'::jsonb),
    'idempotent_replay',false
  );
  update public.candidate_expense_operations operation set
    state='COMMITTED',result_json=v_result,completed_at_utc=p_now_utc,
    updated_at_utc=p_now_utc
  where operation.operation_id=v_operation.operation_id and operation.state='PREPARING';
  if not found then
    raise exception 'CANDIDATE_EXPENSE_OPERATION_CHANGED' using errcode='40001';
  end if;
  return v_result;
end;
$function$;

-- A whole-claim action is one transaction over every live workflow in the
-- linked Candidate presentation.  It checks every owning Timesheet before
-- changing the first row and therefore cannot partly withdraw a claim.
create or replace function public.candidate_whole_claim_action_atomic_v1(
  p_session_id uuid,
  p_environment text,
  p_workflow_id uuid,
  p_expected_generation integer,
  p_timesheet_id uuid,
  p_claim_scope_sha256 text,
  p_action text,
  p_reason_note text,
  p_idempotency_key text,
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
  v_action text:=upper(btrim(coalesce(p_action,'')));
  v_reason text:=nullif(btrim(coalesce(p_reason_note,'')),'');
  v_context jsonb;
  v_anchor public.timesheets%rowtype;
  v_requested_anchor public.timesheets%rowtype;
  v_anchor_fin public.timesheets_financials%rowtype;
  v_current_anchor_id uuid;
  v_owner public.candidate_submission_workflows%rowtype;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_component public.candidate_expense_components%rowtype;
  v_timesheet_id uuid;
  v_timesheet public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_pending_expense_count integer;
  v_approved_count integer;
  v_pending_count integer;
  v_settings jsonb;
  v_template jsonb;
  v_submission_type text;
  v_terminal_mail jsonb;
  v_result jsonb;
  v_results jsonb:='[]'::jsonb;
  v_cleanup uuid[]:=array[]::uuid[];
  v_delete_result jsonb;
  v_deleted_timesheet_ids uuid[]:=array[]::uuid[];
  v_retained_timesheet_ids uuid[]:=array[]::uuid[];
  v_removed_from_current_timesheet_ids uuid[]:=array[]::uuid[];
  v_empty_timesheet_results jsonb:='[]'::jsonb;
  v_r2_cleanup_keys text[]:=array[]::text[];
  v_request jsonb;
  v_request_sha bytea;
  v_operation public.candidate_expense_operations%rowtype;
  v_scope jsonb;
begin
  if p_workflow_id is null or p_expected_generation is null or p_timesheet_id is null
     or coalesce(p_claim_scope_sha256,'') !~ '^[0-9a-fA-F]{64}$'
     or v_action not in ('WITHDRAW_ENTIRE_CLAIM','CANCEL_ENTIRE_CLAIM')
     or v_reason is null or char_length(v_reason)>1000
     or nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then
    raise exception 'CANDIDATE_WHOLE_CLAIM_ACTION_INVALID' using errcode='22023';
  end if;
  v_context:=private._candidate_session_context_v1(
    p_session_id,v_environment,null,p_now_utc,true
  );
  v_request:=jsonb_build_object(
    'contract_version','CANDIDATE_WHOLE_CLAIM_ACTION_REQUEST_V1',
    'workflow_id',p_workflow_id,'workflow_generation',p_expected_generation,
    'timesheet_id',p_timesheet_id,
    'claim_scope_sha256',lower(p_claim_scope_sha256),
    'action_code',v_action,'reason_note',v_reason
  );
  v_request_sha:=private._candidate_sha256_jsonb_v1(v_request);
  perform pg_advisory_xact_lock(hashtextextended(
    'candidate-expense-operation|'||v_environment||'|'
      ||(v_context->>'selected_candidate_id')||'|'||btrim(p_idempotency_key),0
  ));
  select operation.* into v_operation
  from public.candidate_expense_operations operation
  where operation.environment=v_environment
    and operation.actor_kind='CANDIDATE'
    and operation.actor_id=(v_context->>'selected_candidate_id')::uuid
    and operation.idempotency_key=btrim(p_idempotency_key)
  for update;
  if found then
    if v_operation.action_code<>v_action or v_operation.request_sha256<>v_request_sha then
      raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT' using errcode='23505';
    end if;
    if v_operation.state='COMMITTED' then
      return v_operation.result_json||jsonb_build_object('idempotent_replay',true);
    end if;
    raise exception 'CANDIDATE_EXPENSE_OPERATION_IN_PROGRESS' using errcode='55000';
  end if;
  insert into public.candidate_expense_operations(
    environment,account_id,candidate_id,actor_kind,actor_id,action_code,
    workflow_id,timesheet_id,request_sha256,idempotency_key,state,
    created_at_utc,updated_at_utc
  ) values (
    v_environment,(v_context->>'account_id')::uuid,
    (v_context->>'selected_candidate_id')::uuid,'CANDIDATE',
    (v_context->>'selected_candidate_id')::uuid,v_action,p_workflow_id,
    p_timesheet_id,v_request_sha,btrim(p_idempotency_key),'PREPARING',
    p_now_utc,p_now_utc
  ) returning * into v_operation;
  select workflow.* into v_owner from public.candidate_submission_workflows workflow
  where workflow.id=p_workflow_id and workflow.environment=v_environment
    and workflow.candidate_id=(v_context->>'selected_candidate_id')::uuid
    and workflow.state not in ('CANCELLED','EXPIRED','SUPERSEDED','REJECTED','REFUSED')
  for update;
  if not found then raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002'; end if;
  if v_owner.generation<>p_expected_generation then
    raise exception 'WORKFLOW_GENERATION_CONFLICT' using errcode='40001';
  end if;
  -- A deleted row is never an action anchor.  A retained old version may
  -- resolve only through its exact booking chain to one current row owned by
  -- the same Candidate workflow.
  select row.* into v_requested_anchor from public.timesheets row
  where row.timesheet_id=p_timesheet_id;
  if not found then raise exception 'TIMESHEET_NOT_FOUND' using errcode='P0002'; end if;
  v_current_anchor_id:=private._candidate_expense_current_timesheet_id_v1(
    v_owner.id,p_timesheet_id
  );
  if v_current_anchor_id is null then
    raise exception 'CANDIDATE_EXPENSE_OWNING_TIMESHEET_CHANGED' using errcode='40001';
  end if;
  select row.* into v_anchor from public.timesheets row
  where row.timesheet_id=v_current_anchor_id and row.is_current
    and row.archived_at_utc is null for update;
  if not found
     or v_anchor.contract_id is distinct from v_owner.contract_id
     or v_anchor.week_ending_date is distinct from v_owner.week_ending_date
     or (p_timesheet_id<>v_current_anchor_id and (
       nullif(btrim(coalesce(v_requested_anchor.booking_id,'')),'') is null
       or v_requested_anchor.booking_id is distinct from v_anchor.booking_id
       or v_requested_anchor.contract_id is distinct from v_anchor.contract_id
       or v_requested_anchor.week_ending_date is distinct from v_anchor.week_ending_date
     )) then
    raise exception 'CANDIDATE_EXPENSE_OWNING_TIMESHEET_CHANGED' using errcode='40001';
  end if;
  select row.* into v_anchor_fin from public.timesheets_financials row
  where row.timesheet_id=v_current_anchor_id and row.is_current
  order by row.computed_at_utc desc nulls last,row.updated_at desc,row.id desc limit 1;
  if v_anchor_fin.candidate_id is distinct from
       (v_context->>'selected_candidate_id')::uuid then
    raise exception 'TIMESHEET_NOT_FOUND' using errcode='P0002';
  end if;
  update public.candidate_expense_operations operation
  set timesheet_id=v_current_anchor_id,updated_at_utc=p_now_utc
  where operation.operation_id=v_operation.operation_id
    and operation.state='PREPARING';
  perform pg_advisory_xact_lock(hashtextextended(
    'candidate-whole-claim|'||v_environment||'|'
      ||(v_context->>'selected_candidate_id')||'|'||coalesce(v_anchor.contract_id::text,'')
      ||'|'||v_anchor.week_ending_date::text,0
  ));
  perform 1 from public.candidate_submission_workflows workflow
  where workflow.environment=v_environment
    and workflow.candidate_id=(v_context->>'selected_candidate_id')::uuid
    and workflow.contract_id=v_anchor.contract_id
    and workflow.week_ending_date=v_anchor.week_ending_date
    and workflow.state not in ('CANCELLED','EXPIRED','SUPERSEDED','REJECTED','REFUSED')
  order by workflow.id for update;
  -- Component protection is independently durable. Lock it before the scope
  -- digest recheck and fail closed even when a legacy Timesheet/financial row
  -- has not yet caught up with AUTHORISED/INVOICED/PAID component history.
  perform 1
  from public.candidate_expense_components component
  join public.candidate_submission_workflows workflow
    on workflow.id=component.workflow_id
  where workflow.environment=v_environment
    and workflow.candidate_id=(v_context->>'selected_candidate_id')::uuid
    and workflow.contract_id=v_anchor.contract_id
    and workflow.week_ending_date=v_anchor.week_ending_date
    and workflow.state not in ('CANCELLED','EXPIRED','SUPERSEDED','REJECTED','REFUSED')
    and component.lifecycle_state not in (
      'MANAGER_REFUSED','OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED'
    )
  order by component.expense_component_id
  for update of component;
  v_scope:=private._candidate_whole_claim_scope_v1(
    v_environment,v_owner.candidate_id,v_owner.contract_id,v_owner.week_ending_date
  );
  if coalesce(v_scope->>'scope_sha256','')<>lower(p_claim_scope_sha256) then
    raise exception 'CANDIDATE_WHOLE_CLAIM_ACTION_CHANGED'
      using errcode='40001',detail=v_scope::text;
  end if;
  if exists(
    select 1
    from public.candidate_expense_components component
    join public.candidate_submission_workflows workflow
      on workflow.id=component.workflow_id
    where workflow.environment=v_environment
      and workflow.candidate_id=v_owner.candidate_id
      and workflow.contract_id=v_owner.contract_id
      and workflow.week_ending_date=v_owner.week_ending_date
      and workflow.state not in ('CANCELLED','EXPIRED','SUPERSEDED','REJECTED','REFUSED')
      and component.lifecycle_state not in (
        'MANAGER_REFUSED','OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED'
      )
      and component.agency_authorisation_state<>'NOT_AUTHORISED'
  ) then
    raise exception 'CANDIDATE_EXPENSE_COMPONENT_PROTECTED' using errcode='55000';
  end if;
  if v_owner.contract_id is distinct from v_anchor.contract_id
     or v_owner.week_ending_date is distinct from v_anchor.week_ending_date then
    raise exception 'CANDIDATE_EXPENSE_OWNING_TIMESHEET_CHANGED' using errcode='40001';
  end if;
  if exists(
    select 1 from public.candidate_pending_expense_updates update_row
    join public.candidate_submission_workflows workflow on workflow.id=update_row.workflow_id
    where workflow.environment=v_environment
      and workflow.candidate_id=v_owner.candidate_id
      and workflow.contract_id=v_owner.contract_id
      and workflow.week_ending_date=v_owner.week_ending_date
      and update_row.state in ('EDITING','RENDERING')
  ) then raise exception 'CANDIDATE_EXPENSE_UPDATE_IN_PROGRESS' using errcode='55000'; end if;
  select count(distinct workflow.id)::integer into v_pending_expense_count
  from public.candidate_submission_workflows workflow
  left join public.candidate_approval_requests request on request.workflow_id=workflow.id
    and request.workflow_generation=workflow.generation and request.state='PENDING'
  where workflow.environment=v_environment and workflow.candidate_id=v_owner.candidate_id
    and workflow.contract_id=v_owner.contract_id
    and workflow.week_ending_date=v_owner.week_ending_date
    and workflow.workflow_kind in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
    and (
      (workflow.state='AWAITING_MANAGER_APPROVAL' and request.id is not null)
      or (workflow.route='PAPER' and workflow.state='AWAITING_PAPER_RETURN')
    );
  if v_pending_expense_count>1 then
    raise exception 'CANDIDATE_EXPENSE_PENDING_WORKFLOW_CONFLICT' using errcode='55000';
  end if;
  select count(distinct workflow.id) filter(where
      request.state='APPROVED'
      or (workflow.route='PAPER' and workflow.state in ('RECEIVED','FINALISED'))
    )::integer,
    count(distinct workflow.id) filter(where
      request.state='PENDING'
      or (workflow.route='PAPER' and workflow.state='AWAITING_PAPER_RETURN')
    )::integer
  into v_approved_count,v_pending_count
  from public.candidate_submission_workflows workflow
  left join public.candidate_approval_requests request
    on request.workflow_id=workflow.id
    and request.workflow_generation=case when workflow.state='FINALISED'
      then greatest(workflow.generation-1,1) else workflow.generation end
    and request.state in ('PENDING','APPROVED')
  where workflow.environment=v_environment and workflow.candidate_id=v_owner.candidate_id
    and workflow.contract_id=v_owner.contract_id
    and workflow.week_ending_date=v_owner.week_ending_date
    and workflow.state not in ('CANCELLED','EXPIRED','SUPERSEDED','REJECTED','REFUSED');
  if (v_approved_count>0 and v_action<>'CANCEL_ENTIRE_CLAIM')
     or (v_approved_count=0 and v_pending_count>0
       and v_action<>'WITHDRAW_ENTIRE_CLAIM')
     or (v_approved_count=0 and v_pending_count=0) then
    raise exception 'CANDIDATE_WHOLE_CLAIM_ACTION_CHANGED' using errcode='40001';
  end if;
  for v_workflow in
    select workflow.* from public.candidate_submission_workflows workflow
    where workflow.environment=v_environment and workflow.candidate_id=v_owner.candidate_id
      and workflow.contract_id=v_owner.contract_id
      and workflow.week_ending_date=v_owner.week_ending_date
      and workflow.state not in ('CANCELLED','EXPIRED','SUPERSEDED','REJECTED','REFUSED')
    order by workflow.id
  loop
    v_timesheet_id:=private._candidate_expense_owned_timesheet_id_v1(
      v_workflow.id,v_workflow.target_timesheet_id
    );
    if v_workflow.target_timesheet_id is not null and v_timesheet_id is null then
      raise exception 'CANDIDATE_EXPENSE_OWNING_TIMESHEET_CHANGED' using errcode='40001';
    end if;
    if v_timesheet_id is not null then
      select row.* into v_timesheet from public.timesheets row
      where row.timesheet_id=v_timesheet_id for update;
      select row.* into v_fin from public.timesheets_financials row
      where row.timesheet_id=v_timesheet_id and row.is_current
      order by row.computed_at_utc desc nulls last,row.updated_at desc,row.id desc
      limit 1 for update;
      if v_timesheet.authorised_at_server is not null
         or upper(coalesce(v_timesheet.status::text,'')) in ('AUTHORISED','AUTHORIZED','INVOICED','PAID')
         or v_fin.authorised_at_utc is not null or v_fin.locked_by_invoice_id is not null
         or v_fin.paid_at_utc is not null then
        raise exception 'CANDIDATE_EXPENSE_COMPONENT_PROTECTED' using errcode='55000';
      end if;
    end if;
  end loop;
  v_settings:=public.candidate_manager_email_settings_get_v1();
  for v_workflow in
    select workflow.* from public.candidate_submission_workflows workflow
    where workflow.environment=v_environment and workflow.candidate_id=v_owner.candidate_id
      and workflow.contract_id=v_owner.contract_id
      and workflow.week_ending_date=v_owner.week_ending_date
      and workflow.state not in ('CANCELLED','EXPIRED','SUPERSEDED','REJECTED','REFUSED')
    order by workflow.id
  loop
    if v_workflow.workflow_kind in ('CONTRACT_EXPENSE','CONTRACT_COMBINED') then
      for v_component in select component.*
        from public.candidate_expense_components component
        where component.workflow_id=v_workflow.id
          and component.lifecycle_state not in (
            'MANAGER_REFUSED','OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED'
          )
        order by component.expense_component_id for update
      loop
        if v_component.owning_timesheet_id is not null then
          v_result:=private._candidate_expense_financial_remove_v1(
            v_component.expense_component_id,v_component.component_generation,p_now_utc
          );
          if coalesce((v_result->>'zero_expense_carrier')::boolean,false)
             and nullif(v_result->>'timesheet_id','') is not null then
            v_cleanup:=array_append(v_cleanup,(v_result->>'timesheet_id')::uuid);
          end if;
        end if;
      end loop;
    end if;
    v_submission_type:=case v_workflow.workflow_kind
      when 'CONTRACT_EXPENSE' then 'EXPENSE_CLAIM'
      when 'CONTRACT_COMBINED' then 'COMBINED' else 'TIMESHEET' end;
    v_template:=v_settings#>array['templates',v_submission_type,'CANCELLATION'];
    v_terminal_mail:=jsonb_build_object(
      'subject',v_template->>'subject','body_text',v_template->>'body_text',
      'body_html',v_template->>'body_html','manager_template_version',v_settings->'version',
      'manager_template_sha256',v_settings->>'semantic_sha256_hex',
      'manager_submission_type',v_submission_type
    );
    v_result:=public.candidate_workflow_cancel_atomic_v2(
      p_session_id,v_environment,v_workflow.id,v_workflow.generation,
      jsonb_build_object(
        'reason_note',v_reason,'reason_code',v_action,
        'manager_terminal_mail',v_terminal_mail
      ),'whole-claim:'||btrim(p_idempotency_key)||':'||v_workflow.id::text,p_now_utc
    );
    v_results:=v_results||jsonb_build_array(v_result);
  end loop;
  select coalesce(array_agg(distinct value order by value),array[]::uuid[])
  into v_cleanup from unnest(v_cleanup) value;
  foreach v_timesheet_id in array v_cleanup loop
    if exists(select 1 from public.timesheets row where row.timesheet_id=v_timesheet_id) then
      v_delete_result:=private._candidate_zero_expense_carrier_delete_v1(
        v_environment,v_timesheet_id,pg_catalog.gen_random_uuid(),p_now_utc
      );
      v_empty_timesheet_results:=v_empty_timesheet_results||jsonb_build_array(
        jsonb_build_object(
          'timesheet_id',v_timesheet_id,
          'empty_timesheet_consequence',coalesce(
            v_delete_result->>'empty_timesheet_consequence','NONE'
          ),
          'owning_timesheet_deleted',coalesce(
            (v_delete_result->>'owning_timesheet_deleted')::boolean,false
          ),
          'deleted_timesheet_ids',coalesce(
            v_delete_result->'deleted_timesheet_ids','[]'::jsonb
          ),
          'retained_timesheet_ids',coalesce(
            v_delete_result->'retained_timesheet_ids','[]'::jsonb
          ),
          'removed_from_current_timesheet_ids',coalesce(
            v_delete_result->'removed_from_current_timesheet_ids','[]'::jsonb
          )
        )
      );
      select coalesce(array_agg(distinct value::uuid order by value::uuid),array[]::uuid[])
      into v_deleted_timesheet_ids
      from jsonb_array_elements_text(
        to_jsonb(v_deleted_timesheet_ids)||coalesce(v_delete_result->'deleted_timesheet_ids','[]'::jsonb)
      ) ids(value);
      select coalesce(array_agg(distinct value::uuid order by value::uuid),array[]::uuid[])
      into v_retained_timesheet_ids
      from jsonb_array_elements_text(
        to_jsonb(v_retained_timesheet_ids)||coalesce(v_delete_result->'retained_timesheet_ids','[]'::jsonb)
      ) ids(value);
      select coalesce(array_agg(distinct value::uuid order by value::uuid),array[]::uuid[])
      into v_removed_from_current_timesheet_ids
      from jsonb_array_elements_text(
        to_jsonb(v_removed_from_current_timesheet_ids)
          ||coalesce(v_delete_result->'removed_from_current_timesheet_ids','[]'::jsonb)
      ) ids(value);
      select coalesce(array_agg(distinct value order by value),array[]::text[])
      into v_r2_cleanup_keys
      from jsonb_array_elements_text(
        to_jsonb(v_r2_cleanup_keys)||coalesce(v_delete_result->'r2_cleanup_keys','[]'::jsonb)
      ) keys(value);
    end if;
  end loop;
  perform private._candidate_notification_insert_v1(
    v_owner.account_id,v_owner.candidate_id,v_owner.id,
    case when v_current_anchor_id=any(v_removed_from_current_timesheet_ids)
      then null else v_current_anchor_id end,
    case when v_action='WITHDRAW_ENTIRE_CLAIM'
      then 'CLAIM_WITHDRAWN' else 'CLAIM_CANCELLED' end,
    'timesheet_expense_attention',case when v_action='WITHDRAW_ENTIRE_CLAIM'
      then 'candidate-entire-claim-withdrawn-v1' else 'candidate-entire-claim-cancelled-v1' end,
    jsonb_build_object('workflow_id',v_owner.id,'reason',v_reason,
      'affected_workflow_count',jsonb_array_length(v_results)),
    jsonb_build_object('type','workflow','workflow_id',v_owner.id),
    'CANDIDATE_WHOLE_CLAIM_ACTION_V1:'||btrim(p_idempotency_key),p_now_utc
  );
  v_result:=jsonb_build_object(
    'ok',true,'contract_version','CANDIDATE_WHOLE_CLAIM_ACTION_RESULT_V1',
    'operation_id',v_operation.operation_id,
    'action_code',v_action,'state','CANCELLED',
    'workflow_results',v_results,'zero_expense_carrier_timesheet_ids',to_jsonb(v_cleanup),
    'empty_timesheet_results',v_empty_timesheet_results,
    'deleted_timesheet_ids',to_jsonb(v_deleted_timesheet_ids),
    'retained_timesheet_ids',to_jsonb(v_retained_timesheet_ids),
    'removed_from_current_timesheet_ids',to_jsonb(v_removed_from_current_timesheet_ids),
    'r2_cleanup_keys',to_jsonb(v_r2_cleanup_keys),
    'idempotent_replay',false
  );
  update public.candidate_expense_operations operation set
    state='COMMITTED',result_json=v_result,completed_at_utc=p_now_utc,
    updated_at_utc=p_now_utc
  where operation.operation_id=v_operation.operation_id and operation.state='PREPARING';
  if not found then
    raise exception 'CANDIDATE_EXPENSE_OPERATION_CHANGED' using errcode='40001';
  end if;
  return v_result;
end;
$function$;

-- Office category rejection is projected from the same locked component and
-- owning-Timesheet facts that the mutation rechecks.  The confirmation digest
-- covers the category, its current evidence, route/protection, every remaining
-- amount, worked hours and the delete consequence.
create or replace function private._candidate_office_expense_rejection_context_v1(
  p_environment text,
  p_expense_component_id uuid,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_environment text:=private._candidate_assert_environment(p_environment);
  v_component public.candidate_expense_components%rowtype;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_timesheet_id uuid;
  v_timesheet public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_category_json jsonb;
  v_owner_count integer:=0;
  v_current_amount numeric:=0;
  v_current_charge numeric:=0;
  v_current_units numeric:=0;
  v_remaining_expense_total numeric:=0;
  v_remaining_expense_value numeric:=0;
  v_remaining_hours numeric:=0;
  v_will_delete boolean:=false;
  v_empty_timesheet_consequence text:='NONE';
  v_route_kind text;
  v_workflow_route_kind text;
  v_route_authority jsonb;
  v_unmaterialised_prior jsonb;
  v_eligible boolean:=false;
  v_disabled text;
  v_basis jsonb;
  v_digest text;
  v_confirmation jsonb;
  v_action jsonb;
  v_system_actor uuid;
  v_delete_preview jsonb;
  v_delete_signature jsonb;
  v_delete_row_signature text;
  v_delete_scope jsonb;
  v_delete_scope_sha256 text;
  v_delete_target_scope_sha256 text;
  v_freshness_basis jsonb;
  v_freshness_sha256 text;
begin
  select * into v_component
  from public.candidate_expense_components component
  where component.expense_component_id=p_expense_component_id;
  if found then
    select * into v_workflow
    from public.candidate_submission_workflows workflow
    where workflow.id=v_component.workflow_id
      and workflow.environment=v_environment;
  end if;
  if not found then return null; end if;
  v_timesheet_id:=private._candidate_expense_owned_timesheet_id_v1(
    v_component.workflow_id,v_component.owning_timesheet_id
  );
  if v_timesheet_id is not null then
    select * into v_timesheet from public.timesheets row
    where row.timesheet_id=v_timesheet_id and row.is_current
      and row.archived_at_utc is null;
    select * into v_fin from public.timesheets_financials row
    where row.timesheet_id=v_timesheet_id and row.is_current
    order by row.computed_at_utc desc nulls last,row.updated_at desc,row.id desc limit 1;
  end if;
  v_category_json:=private._candidate_expense_component_json_v1(v_component,false);
  v_unmaterialised_prior:=private._candidate_expense_unmaterialised_prior_v1(
    v_component.expense_component_id
  );
  select count(*)::integer into v_owner_count
  from public.candidate_expense_components other_component
  where other_component.expense_category=v_component.expense_category
    and other_component.lifecycle_state not in (
      'MANAGER_REFUSED','OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED'
    )
    and private._candidate_expense_owned_timesheet_id_v1(
      other_component.workflow_id,other_component.owning_timesheet_id
    )=v_timesheet_id;
  v_current_amount:=case v_component.expense_category
    when 'MILEAGE' then coalesce(v_fin.mileage_pay_ex_vat,0)
    when 'TRAVEL' then coalesce(v_fin.travel_pay_ex_vat,0)
    when 'ACCOMMODATION' then coalesce(v_fin.accommodation_pay_ex_vat,0)
    else coalesce(v_fin.other_pay_ex_vat,0) end;
  v_current_units:=case when v_component.expense_category='MILEAGE'
    then coalesce(v_fin.mileage_units,0) else 0 end;
  v_current_charge:=case v_component.expense_category
    when 'MILEAGE' then coalesce(v_fin.mileage_charge_ex_vat,0)
    when 'TRAVEL' then coalesce(v_fin.travel_charge_ex_vat,0)
    when 'ACCOMMODATION' then coalesce(v_fin.accommodation_charge_ex_vat,0)
    else coalesce(v_fin.other_charge_ex_vat,0) end;
  v_remaining_expense_total:=
    coalesce(v_fin.mileage_pay_ex_vat,0)+coalesce(v_fin.travel_pay_ex_vat,0)
    +coalesce(v_fin.accommodation_pay_ex_vat,0)+coalesce(v_fin.other_pay_ex_vat,0)
    -v_current_amount;
  v_remaining_expense_value:=
    case when v_component.expense_category='MILEAGE' then 0
      else abs(coalesce(v_fin.mileage_units,0))
        +abs(coalesce(v_fin.mileage_pay_ex_vat,0))
        +abs(coalesce(v_fin.mileage_charge_ex_vat,0)) end
    +case when v_component.expense_category='TRAVEL' then 0
      else abs(coalesce(v_fin.travel_pay_ex_vat,0))
        +abs(coalesce(v_fin.travel_charge_ex_vat,0)) end
    +case when v_component.expense_category='ACCOMMODATION' then 0
      else abs(coalesce(v_fin.accommodation_pay_ex_vat,0))
        +abs(coalesce(v_fin.accommodation_charge_ex_vat,0)) end
    +case when v_component.expense_category='OTHER' then 0
      else abs(coalesce(v_fin.other_pay_ex_vat,0))
        +abs(coalesce(v_fin.other_charge_ex_vat,0)) end;
  v_remaining_hours:=coalesce(v_fin.total_hours,0);
  v_will_delete:=v_remaining_expense_value=0 and v_remaining_hours=0
    and abs(coalesce(v_fin.additional_pay_ex_vat,0))
      +abs(coalesce(v_fin.additional_charge_ex_vat,0))=0
    and coalesce(v_fin.hours_day,0)=0 and coalesce(v_fin.hours_night,0)=0
    and coalesce(v_fin.hours_sat,0)=0 and coalesce(v_fin.hours_sun,0)=0
    and coalesce(v_fin.hours_bh,0)=0
    and coalesce(v_fin.pay_day,0)=0 and coalesce(v_fin.pay_night,0)=0
    and coalesce(v_fin.pay_sat,0)=0 and coalesce(v_fin.pay_sun,0)=0
    and coalesce(v_fin.pay_bh,0)=0
    and coalesce(v_fin.charge_day,0)=0 and coalesce(v_fin.charge_night,0)=0
    and coalesce(v_fin.charge_sat,0)=0 and coalesce(v_fin.charge_sun,0)=0
    and coalesce(v_fin.charge_bh,0)=0
    and coalesce(v_fin.total_pay_ex_vat,0)=v_current_amount
    and coalesce(v_fin.total_charge_ex_vat,0)=v_current_charge
    and coalesce(v_fin.margin_ex_vat,0)=v_current_charge-v_current_amount
    and coalesce(v_fin.expenses_pay_ex_vat,0)
      +coalesce(v_fin.mileage_pay_ex_vat,0)=v_current_amount
    and coalesce(v_fin.expenses_charge_ex_vat,0)
      +coalesce(v_fin.mileage_charge_ex_vat,0)=v_current_charge
    and coalesce(v_fin.additional_margin_ex_vat,0)=0
    and coalesce(v_fin.pay_vat_amount_snapshot,0)=0
    and coalesce(v_fin.pay_total_inc_vat_snapshot,0)=0
    and v_timesheet.sheet_scope::text='WEEKLY'
    and coalesce(v_timesheet.is_adjustment,false)
    and upper(coalesce(v_timesheet.adjustment_origin,'')) not in (
      'IMPORT_CORRECTION','IMPORT_CANCELLATION'
    );
  if v_will_delete then
    select defaults.candidate_app_system_actor_user_id into v_system_actor
    from public.settings_defaults defaults where defaults.id=1;
    begin
      v_delete_preview:=public.timesheet_weekly_manual_adjustment_delete_preview(
        v_timesheet_id,v_system_actor
      );
      v_empty_timesheet_consequence:=case
        when coalesce(v_delete_preview->>'decision','')='ARCHIVE_REQUIRED'
          then 'REMOVE_FROM_CURRENT_KEEP_HISTORY'
        when coalesce(v_delete_preview->>'decision','')='PERMANENT_DELETE'
          and coalesce(jsonb_array_length(
            v_delete_preview->'preserved_source_contract_week_ids'
          ),0)>0
          then 'PERMANENT_REMOVE'
        else 'NONE'
      end;
      v_will_delete:=v_empty_timesheet_consequence='PERMANENT_REMOVE';
      if v_empty_timesheet_consequence<>'NONE' then
        v_delete_signature:=public.timesheet_lifecycle_guard_signature_v1(
          v_timesheet_id,
          (select week.id from public.contract_weeks week
            where week.timesheet_id=v_timesheet_id order by week.id limit 1),false
        );
        v_delete_row_signature:=nullif(btrim(coalesce(
          v_delete_signature->>'backend_row_signature',
          v_delete_signature->>'row_signature',v_delete_signature->>'signature',''
        )), '');
        if v_delete_row_signature is null then
          v_empty_timesheet_consequence:='NONE';
          v_will_delete:=false;
        else
          v_delete_scope:=jsonb_build_object(
            'contract_version','CANDIDATE_EXPENSE_CARRIER_DELETE_SCOPE_V1',
            'requested_timesheet_id',v_timesheet_id,
            'current_timesheet_id',nullif(v_delete_preview->>'current_timesheet_id','')::uuid,
            'timesheet_ids',coalesce((select jsonb_agg(value::uuid order by value::uuid)
              from jsonb_array_elements_text(
                coalesce(v_delete_preview->'timesheet_ids','[]'::jsonb)
              ) item(value)),'[]'::jsonb),
            'contract_week_ids',coalesce((select jsonb_agg(value::uuid order by value::uuid)
              from jsonb_array_elements_text(
                coalesce(v_delete_preview->'contract_week_ids','[]'::jsonb)
              ) item(value)),'[]'::jsonb),
            'preserved_source_timesheet_ids',coalesce((select jsonb_agg(value::uuid order by value::uuid)
              from jsonb_array_elements_text(coalesce(
                v_delete_preview->'preserved_source_timesheet_ids','[]'::jsonb
              )) item(value)),'[]'::jsonb),
            'preserved_source_contract_week_ids',coalesce((select jsonb_agg(value::uuid order by value::uuid)
              from jsonb_array_elements_text(coalesce(
                v_delete_preview->'preserved_source_contract_week_ids','[]'::jsonb
              )) item(value)),'[]'::jsonb),
            'row_signature',v_delete_row_signature
          );
          v_delete_scope_sha256:=encode(
            private._candidate_sha256_jsonb_v1(v_delete_scope),'hex'
          );
          v_delete_target_scope_sha256:=encode(
            private._candidate_sha256_jsonb_v1(v_delete_scope-'row_signature'),'hex'
          );
        end if;
      end if;
    exception when others then
      v_empty_timesheet_consequence:='NONE';
      v_will_delete:=false;
      v_delete_scope:=null;
      v_delete_scope_sha256:=null;
      v_delete_target_scope_sha256:=null;
    end;
  end if;
  v_workflow_route_kind:=case when v_workflow.route='PAPER' then 'QR'
    when v_workflow.route in ('ELECTRONIC','PHONE','EMAIL') then 'ELECTRONIC' end;
  if v_timesheet_id is not null and v_timesheet.timesheet_id is not null then
    begin
      v_route_authority:=private._candidate_route_family_v1(
        v_timesheet_id,v_workflow.contract_week_id
      );
      v_route_kind:=v_route_authority->>'route_family';
    exception when others then
      v_route_authority:=null;
      v_route_kind:=null;
    end;
  end if;
  if v_timesheet_id is null or v_timesheet.timesheet_id is null or v_fin.id is null then
    v_disabled:='OWNING_TIMESHEET_CHANGED';
  elsif v_fin.nhsp_import_id is not null
     or v_route_kind not in ('ELECTRONIC','QR')
     or v_workflow_route_kind is distinct from v_route_kind then
    v_disabled:='ROUTE_NOT_ELIGIBLE';
  elsif v_component.agency_authorisation_state<>'NOT_AUTHORISED'
     or v_timesheet.authorised_at_server is not null
     or upper(coalesce(v_timesheet.status::text,'')) in ('AUTHORISED','AUTHORIZED','INVOICED','PAID')
     or v_fin.authorised_at_utc is not null or v_fin.locked_by_invoice_id is not null
     or v_fin.paid_at_utc is not null then
    v_disabled:='EXPENSE_PROTECTED';
  elsif v_owner_count<>1
     or (v_current_amount is distinct from v_component.amount and (
       coalesce((v_unmaterialised_prior->>'allowed')::boolean,false) is not true
       or v_current_amount is distinct from
         coalesce((v_unmaterialised_prior->>'amount')::numeric,0)
     ))
     or (v_current_units is distinct from v_component.mileage_units and (
       coalesce((v_unmaterialised_prior->>'allowed')::boolean,false) is not true
       or v_current_units is distinct from
         coalesce((v_unmaterialised_prior->>'mileage_units')::numeric,0)
     )) then
    v_disabled:='CATEGORY_OWNERSHIP_AMBIGUOUS';
  elsif not (
    (v_component.lifecycle_state='SUBMITTED' and (
      (v_component.manager_approval_state='PENDING'
        and v_workflow.route<>'PAPER')
      or (v_component.manager_approval_state='NOT_REQUESTED'
        and v_workflow.route='PAPER'
        and v_workflow.state='AWAITING_PAPER_RETURN')
    ))
    or (v_component.lifecycle_state='MANAGER_APPROVED'
      and v_component.manager_approval_state='APPROVED'
      and v_workflow.state='FINALISED')
  ) then
    v_disabled:='CATEGORY_STATE_NOT_ELIGIBLE';
  else
    v_eligible:=true;
  end if;
  v_freshness_basis:=jsonb_build_object(
    'contract_version','OFFICE_EXPENSE_CATEGORY_REJECTION_FRESHNESS_V1',
    'expense_component_id',v_component.expense_component_id,
    'component_generation',v_component.component_generation,
    'expense_category',v_component.expense_category,
    'amount',v_component.amount,'mileage_units',v_component.mileage_units,
    'supporting_evidence_count',(v_category_json->>'supporting_evidence_count')::integer,
    'owning_timesheet_id',v_timesheet_id,'route_family',v_route_kind,
    'timesheet_version',v_timesheet.version,'timesheet_status',v_timesheet.status,
    'financials_id',v_fin.id,'financials_updated_at',v_fin.updated_at,
    'remaining_hours',v_remaining_hours,
    'remaining_expense_total',v_remaining_expense_total,
    'remaining_expense_value',v_remaining_expense_value,
    'empty_timesheet_consequence',v_empty_timesheet_consequence,
    'will_delete_timesheet',v_will_delete,
    'delete_scope_sha256',case when v_empty_timesheet_consequence<>'NONE'
      then v_delete_scope_sha256
      else 'NO_DELETE' end
  );
  v_freshness_sha256:=encode(
    private._candidate_sha256_jsonb_v1(v_freshness_basis),'hex'
  );
  v_basis:=jsonb_build_object(
    'contract_version','OFFICE_EXPENSE_CATEGORY_REJECTION_CONTEXT_V1',
    'environment',v_environment,'workflow_id',v_workflow.id,
    'workflow_generation',v_workflow.generation,
    'expense_component_id',v_component.expense_component_id,
    'component_generation',v_component.component_generation,
    'expense_category',v_component.expense_category,
    'amount',v_component.amount,'mileage_units',v_component.mileage_units,
    'current_charge',v_current_charge,
    'supporting_evidence_count',(v_category_json->>'supporting_evidence_count')::integer,
    'owning_timesheet_id',v_timesheet_id,'route_family',v_route_kind,
    'timesheet_version',v_timesheet.version,'timesheet_status',v_timesheet.status,
    'financials_id',v_fin.id,'financials_updated_at',v_fin.updated_at,
    'remaining_hours',v_remaining_hours,
    'remaining_expense_total',v_remaining_expense_total,
    'remaining_expense_value',v_remaining_expense_value,
    'remaining_additional_value',abs(coalesce(v_fin.additional_pay_ex_vat,0))
      +abs(coalesce(v_fin.additional_charge_ex_vat,0)),
    'delete_scope',case when v_empty_timesheet_consequence<>'NONE' then v_delete_scope end,
    'delete_scope_sha256',case when v_empty_timesheet_consequence<>'NONE'
      then v_delete_scope_sha256 end,
    'delete_target_scope_sha256',case when v_empty_timesheet_consequence<>'NONE'
      then v_delete_target_scope_sha256 end,
    'freshness_sha256',v_freshness_sha256,
    'empty_timesheet_consequence',v_empty_timesheet_consequence,
    'will_delete_timesheet',v_will_delete,'eligible',v_eligible,
    'disabled_reason_code',v_disabled
  );
  v_digest:=encode(private._candidate_sha256_jsonb_v1(v_basis),'hex');
  v_confirmation:=jsonb_build_object(
    'contract_version','OFFICE_EXPENSE_CATEGORY_REJECTION_CONFIRMATION_V1',
    'confirmation_sha256',v_digest,
    'expense_category',v_component.expense_category,'amount',v_component.amount,
    'mileage_units',v_component.mileage_units,
    'supporting_evidence_count',(v_category_json->>'supporting_evidence_count')::integer,
    'owning_timesheet_id',v_timesheet_id,'route_family',v_route_kind,
    'empty_timesheet_consequence',v_empty_timesheet_consequence,
    'will_delete_timesheet',v_will_delete,
    'remaining_hours',v_remaining_hours,
    'remaining_expense_total',v_remaining_expense_total
  );
  if v_eligible then
    v_action:=private._candidate_office_action_v1(
      'REJECT_EXPENSE_CATEGORY','Reject expense','EXPENSES',true,null,null,
      true,true,'POST',
      '/api/candidate-app/workflows/'||v_workflow.id::text
        ||'/actions/reject-expense-category',
      jsonb_build_object(
        'generation',v_workflow.generation,
        'expense_component_id',v_component.expense_component_id,
        'component_generation',v_component.component_generation,
        'confirmation_sha256',v_digest
      ),
      '[{"name":"reason_note","type":"string","required":true,"max_length":1000}]'::jsonb,
      'REQUIRED',false
    );
  end if;
  return jsonb_build_object(
    'eligible',v_eligible,'disabled_reason_code',v_disabled,
    'confirmation_sha256',v_digest,'basis',v_basis,
    'confirmation',case when v_eligible then v_confirmation end,
    'action',v_action
  );
end;
$function$;

create or replace function public.candidate_office_expense_category_projection_v1(
  p_environment text,
  p_timesheet_id uuid,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_environment text:=private._candidate_assert_environment(p_environment);
  v_claims jsonb;
  v_route_family text;
begin
  if p_timesheet_id is null then
    raise exception 'TIMESHEET_NOT_FOUND' using errcode='22023';
  end if;
  select case when count(distinct context#>>'{basis,route_family}')=1
      then min(context#>>'{basis,route_family}') end
  into v_route_family
  from public.candidate_expense_components component
  join public.candidate_submission_workflows workflow
    on workflow.id=component.workflow_id
  cross join lateral private._candidate_office_expense_rejection_context_v1(
    v_environment,component.expense_component_id,p_now_utc
  ) context
  where component.owning_timesheet_id is not null
    and workflow.target_timesheet_id is not null
    and private._candidate_expense_owned_timesheet_id_v1(
      workflow.id,workflow.target_timesheet_id
    )=p_timesheet_id
    and private._candidate_expense_owned_timesheet_id_v1(
      component.workflow_id,component.owning_timesheet_id
    )=p_timesheet_id
    and context#>>'{basis,route_family}' in ('ELECTRONIC','QR');
  with owned as (
    select
      workflow.id as workflow_id,
      workflow.generation,
      case when workflow.state='FINALISED' then greatest(workflow.generation-1,1)
        else workflow.generation end as document_generation,
      workflow.route,
      workflow.state as workflow_state,
      workflow.target_timesheet_id,
      workflow.worker_submitted_at_utc,
      workflow.updated_at_utc as workflow_updated_at_utc,
      component.expense_component_id,
      component.component_generation,
      component.expense_category,
      component.amount,
      component.mileage_units,
      component.lifecycle_state,
      component.manager_approval_state,
      component.agency_authorisation_state,
      component.owning_timesheet_id,
      component.created_at_utc,
      private._candidate_expense_component_json_v1(component,false)
        -'available_action' as component_json,
      private._candidate_office_expense_rejection_context_v1(
        v_environment,component.expense_component_id,p_now_utc
      ) as rejection_context
    from public.candidate_expense_components component
    join public.candidate_submission_workflows workflow on workflow.id=component.workflow_id
    where workflow.environment=v_environment
      and component.owning_timesheet_id is not null
      and workflow.target_timesheet_id is not null
      and private._candidate_expense_owned_timesheet_id_v1(
        workflow.id,workflow.target_timesheet_id
      )=p_timesheet_id
      and private._candidate_expense_owned_timesheet_id_v1(
        component.workflow_id,component.owning_timesheet_id
      )=p_timesheet_id
  ), category_rows as (
    select owned.*,
      private._candidate_expense_component_status_v1(
        owned.lifecycle_state,owned.manager_approval_state,
        owned.agency_authorisation_state
      ) as status_code,
      owned.component_json||jsonb_build_object(
        'office_rejection_action',owned.rejection_context->'action',
        'office_rejection_confirmation',owned.rejection_context->'confirmation'
      ) as category_json,
      owned.lifecycle_state not in (
        'MANAGER_REFUSED','OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED'
      ) as included_in_total
    from owned
  ), claims as (
    select
      workflow_id,generation,document_generation,route,workflow_state,
      target_timesheet_id,worker_submitted_at_utc,workflow_updated_at_utc,
      case when count(distinct status_code)=1 then min(status_code) else 'MIXED' end
        as aggregate_status,
      case when count(distinct manager_approval_state)=1 then min(manager_approval_state)
        else 'MIXED' end as aggregate_manager,
      case when count(distinct agency_authorisation_state)=1 then min(agency_authorisation_state)
        else 'MIXED' end as aggregate_agency,
      bool_or(included_in_total and agency_authorisation_state<>'NOT_AUTHORISED')
        as protected,
      coalesce(sum(amount) filter(where included_in_total),0) as total_amount,
      coalesce(sum(amount) filter(where included_in_total and expense_category='MILEAGE'),0)
        as mileage_pay,
      coalesce(sum(amount) filter(where included_in_total and expense_category='TRAVEL'),0)
        as travel_pay,
      coalesce(sum(amount) filter(where included_in_total and expense_category='ACCOMMODATION'),0)
        as accommodation_pay,
      coalesce(sum(amount) filter(where included_in_total and expense_category='OTHER'),0)
        as other_pay,
      coalesce(sum(mileage_units) filter(where included_in_total),0) as mileage_units,
      coalesce(sum((category_json->>'supporting_evidence_count')::integer)
        filter(where included_in_total),0)::integer as supporting_evidence_count,
      coalesce(jsonb_agg(category_json order by created_at_utc,expense_component_id),'[]'::jsonb)
        as categories,
      coalesce(jsonb_agg(distinct expense_category order by expense_category)
        filter(where included_in_total
          and coalesce((category_json->>'supporting_evidence_count')::integer,0)>0),
        '[]'::jsonb) as supporting_evidence_categories
    from category_rows
    group by workflow_id,generation,document_generation,route,workflow_state,
      target_timesheet_id,worker_submitted_at_utc,workflow_updated_at_utc
  )
  select coalesce(jsonb_agg(jsonb_build_object(
    'workflow_id',claim.workflow_id,
    'generation',claim.generation,
    'document_generation',claim.document_generation,
    'route',claim.route,
    'state',claim.workflow_state,
    'status_code',claim.aggregate_status,
    'manager_approval_state',claim.aggregate_manager,
    'agency_authorisation_state',claim.aggregate_agency,
    'target_timesheet_id',p_timesheet_id,
    'submitted_at_utc',claim.worker_submitted_at_utc,
    'updated_at_utc',claim.workflow_updated_at_utc,
    'protected',coalesce(claim.protected,false),
    'can_withdraw',false,
    'totals',jsonb_build_object(
      'expenses_pay_ex_vat',claim.travel_pay+claim.accommodation_pay+claim.other_pay,
      'expenses_description',null,
      'mileage_units',claim.mileage_units,
      'mileage_pay_ex_vat',claim.mileage_pay,
      'travel_pay_ex_vat',claim.travel_pay,
      'accommodation_pay_ex_vat',claim.accommodation_pay,
      'other_pay_ex_vat',claim.other_pay,
      'amount',claim.total_amount
    ),
    'supporting_evidence_count',claim.supporting_evidence_count,
    'supporting_evidence_categories',claim.supporting_evidence_categories,
    'categories',claim.categories,
    'whole_claim_action',null,
    'begin_update_action',null,
    'update_state',case when exists(
      select 1 from public.candidate_pending_expense_updates update_row
      where update_row.workflow_id=claim.workflow_id
        and update_row.state in ('EDITING','RENDERING')
    ) then 'UPDATING' else 'NONE' end,
    'attention_code',null
  ) order by claim.worker_submitted_at_utc nulls first,claim.workflow_id),'[]'::jsonb)
  into v_claims
  from claims claim;
  return jsonb_build_object(
    'contract_version','OFFICE_EXPENSE_CATEGORY_PROJECTION_V2',
    'timesheet_id',p_timesheet_id,
    'route_family',v_route_family,
    'expense_claims',v_claims
  );
end;
$function$;

create or replace function public.candidate_office_expense_category_projection_batch_v1(
  p_environment text,
  p_timesheet_ids uuid[],
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_environment text:=private._candidate_assert_environment(p_environment);
  v_result jsonb;
begin
  if p_timesheet_ids is null or cardinality(p_timesheet_ids)>100
     or exists(select 1 from unnest(p_timesheet_ids) value where value is null) then
    raise exception 'CANDIDATE_OFFICE_PROJECTION_BATCH_INVALID' using errcode='22023';
  end if;
  select coalesce(jsonb_agg(jsonb_build_object(
    'timesheet_id',requested.timesheet_id,
    'route_family',projection->'route_family',
    'expense_claims',projection->'expense_claims'
  ) order by requested.ordinal),'[]'::jsonb)
  into v_result
  from unnest(p_timesheet_ids) with ordinality requested(timesheet_id,ordinal)
  cross join lateral public.candidate_office_expense_category_projection_v1(
    v_environment,requested.timesheet_id,p_now_utc
  ) projection;
  return jsonb_build_object(
    'contract_version','OFFICE_EXPENSE_CATEGORY_PROJECTION_BATCH_V1',
    'timesheets',v_result
  );
end;
$function$;

create or replace function public.candidate_office_expense_category_adapter_v1(
  p_actor_user_id uuid,
  p_environment text,
  p_payload jsonb,
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
  v_payload jsonb:=coalesce(p_payload,'{}'::jsonb);
  v_result jsonb;
begin
  if p_actor_user_id is null or jsonb_typeof(v_payload)<>'object' then
    raise exception 'OFFICE_AUTH_REQUIRED' using errcode='28000';
  end if;
  perform private._candidate_office_service_context_open_v1(
    v_environment,p_actor_user_id,'reject_submission','REJECT_EXPENSE_CATEGORY',p_now_utc
  );
  v_result:=public.candidate_office_expense_category_reject_atomic_v1(
    p_actor_user_id,v_environment,
    nullif(v_payload->>'workflow_id','')::uuid,
    nullif(v_payload->>'generation','')::integer,
    nullif(v_payload->>'expense_component_id','')::uuid,
    nullif(v_payload->>'component_generation','')::integer,
    v_payload->>'confirmation_sha256',v_payload->>'reason_note',
    v_payload->>'idempotency_key',p_now_utc
  );
  perform private._candidate_office_service_context_close_v1();
  return v_result||jsonb_build_object(
    'refresh_hints',jsonb_build_object(
      'summary',true,'simple_timesheet',true,'bulk_process',true,
      'bulk_authorise',true,'refetch','AFFECTED_ROWS'
    )
  );
exception when others then
  perform private._candidate_office_service_context_close_v1();
  raise;
end;
$function$;

create or replace function public.candidate_office_expense_category_reject_atomic_v1(
  p_actor_user_id uuid,
  p_environment text,
  p_workflow_id uuid,
  p_expected_generation integer,
  p_expense_component_id uuid,
  p_expected_component_generation integer,
  p_confirmation_sha256 text,
  p_reason_note text,
  p_idempotency_key text,
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
  v_reason text:=nullif(btrim(coalesce(p_reason_note,'')),'');
  v_context jsonb;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_component public.candidate_expense_components%rowtype;
  v_before jsonb;
  v_operation public.candidate_expense_operations%rowtype;
  v_request_sha bytea;
  v_financial jsonb;
  v_begin jsonb;
  v_submit jsonb;
  v_result jsonb;
  v_direct_empty_pending boolean:=false;
  v_is_paper_pending boolean:=false;
  v_paper_retirement jsonb;
begin
  if not private._candidate_office_service_context_valid_v1(
       v_environment,p_actor_user_id,'REJECT_EXPENSE_CATEGORY'
     ) then
    raise exception 'CANDIDATE_OFFICE_SERVICE_CONTEXT_INVALID' using errcode='28000';
  end if;
  if p_workflow_id is null or p_expected_generation is null
     or p_expense_component_id is null or p_expected_component_generation is null
     or coalesce(p_confirmation_sha256,'') !~ '^[0-9a-fA-F]{64}$'
     or v_reason is null or char_length(v_reason)>1000
     or nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then
    raise exception 'CANDIDATE_EXPENSE_CATEGORY_REJECTION_INVALID' using errcode='22023';
  end if;
  v_request_sha:=private._candidate_sha256_jsonb_v1(jsonb_build_object(
    'contract_version','OFFICE_EXPENSE_CATEGORY_REJECTION_REQUEST_V1',
    'actor_user_id',p_actor_user_id,'environment',v_environment,
    'workflow_id',p_workflow_id,'generation',p_expected_generation,
    'expense_component_id',p_expense_component_id,
    'component_generation',p_expected_component_generation,
    'confirmation_sha256',lower(p_confirmation_sha256),'reason_note',v_reason
  ));
  perform pg_advisory_xact_lock(hashtextextended(
    'office-expense-category-reject|'||p_actor_user_id::text||'|'
      ||btrim(p_idempotency_key),0
  ));
  select * into v_operation from public.candidate_expense_operations operation
  where operation.environment=v_environment and operation.actor_kind='OFFICE'
    and operation.actor_id=p_actor_user_id
    and operation.idempotency_key=btrim(p_idempotency_key) for update;
  if found then
    if v_operation.request_sha256<>v_request_sha then
      raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT' using errcode='40001';
    end if;
    if v_operation.state='COMMITTED' then
      return v_operation.result_json||jsonb_build_object('idempotent_replay',true);
    end if;
    if v_operation.state='RENDERING'
       and jsonb_typeof(v_operation.progress_json)='object'
       and nullif(v_operation.progress_json->>'update_id','') is not null
       and (
         jsonb_typeof(v_operation.progress_json->'render_contract')='object'
         or coalesce((v_operation.progress_json->>'paper_prepare_required')::boolean,false)
       ) then
      return v_operation.progress_json||jsonb_build_object(
        'ok',true,'contract_version','OFFICE_EXPENSE_CATEGORY_REJECTION_PENDING_V1',
        'operation_id',v_operation.operation_id,'action_code','REJECT_EXPENSE_CATEGORY',
        'state','RENDERING','idempotent_replay',true
      );
    end if;
    if v_operation.state in ('FAILED','ABORTED')
       and v_operation.result_json is not null then
      return v_operation.result_json||jsonb_build_object('idempotent_replay',true);
    elsif v_operation.state in ('FAILED','ABORTED') then
      raise exception 'CANDIDATE_EXPENSE_CATEGORY_REJECTION_NOT_COMMITTED'
        using errcode='40001';
    end if;
    return jsonb_build_object(
      'ok',true,'contract_version','OFFICE_EXPENSE_CATEGORY_REJECTION_PENDING_V1',
      'operation_id',v_operation.operation_id,'state',v_operation.state,
      'idempotent_replay',true
    );
  end if;
  select * into v_workflow from public.candidate_submission_workflows workflow
  where workflow.id=p_workflow_id and workflow.environment=v_environment for update;
  select * into v_component from public.candidate_expense_components component
  where component.expense_component_id=p_expense_component_id
    and component.workflow_id=p_workflow_id for update;
  if v_workflow.id is null or v_component.expense_component_id is null then
    raise exception 'CANDIDATE_EXPENSE_COMPONENT_NOT_FOUND' using errcode='P0002';
  end if;
  if v_workflow.generation<>p_expected_generation
     or v_component.component_generation<>p_expected_component_generation then
    raise exception 'CANDIDATE_EXPENSE_CATEGORY_CONTEXT_CHANGED' using errcode='40001';
  end if;
  perform private._candidate_expense_owner_context_lock_v1(
    v_component.expense_component_id
  );
  v_context:=private._candidate_office_expense_rejection_context_v1(
    v_environment,v_component.expense_component_id,p_now_utc
  );
  if not coalesce((v_context->>'eligible')::boolean,false)
     or lower(coalesce(v_context->>'confirmation_sha256',''))<>lower(p_confirmation_sha256) then
    raise exception 'CANDIDATE_EXPENSE_CATEGORY_CONTEXT_CHANGED'
      using errcode='40001',detail=coalesce(v_context,'{}'::jsonb)::text;
  end if;
  perform set_config(
    'cloudtms.candidate_expected_delete_scope_sha256',
    coalesce(v_context#>>'{basis,delete_target_scope_sha256}','NO_DELETE'),true
  );
  insert into public.candidate_expense_operations(
    environment,account_id,candidate_id,actor_kind,actor_id,action_code,
    workflow_id,timesheet_id,expense_component_id,request_sha256,idempotency_key,
    state,progress_json,created_at_utc,updated_at_utc
  ) values (
    v_environment,v_workflow.account_id,v_workflow.candidate_id,'OFFICE',p_actor_user_id,
    'REJECT_EXPENSE_CATEGORY',v_workflow.id,
    (v_context#>>'{basis,owning_timesheet_id}')::uuid,v_component.expense_component_id,
    v_request_sha,btrim(p_idempotency_key),'PREPARING',jsonb_build_object(
      'expected_rejection_freshness_sha256',v_context#>>'{basis,freshness_sha256}',
      'expected_delete_scope_sha256',coalesce(
        v_context#>>'{basis,delete_scope_sha256}','NO_DELETE'
      ),
      'expected_delete_target_scope_sha256',coalesce(
        v_context#>>'{basis,delete_target_scope_sha256}','NO_DELETE'
      )
    ),p_now_utc,p_now_utc
  ) returning * into v_operation;
  v_is_paper_pending:=v_component.lifecycle_state='SUBMITTED'
    and v_component.manager_approval_state='NOT_REQUESTED'
    and v_workflow.route='PAPER'
    and v_workflow.state='AWAITING_PAPER_RETURN';
  if v_component.lifecycle_state='SUBMITTED' and (
       (v_component.manager_approval_state='PENDING' and v_workflow.route<>'PAPER')
       or v_is_paper_pending
     ) then
    v_direct_empty_pending:=v_workflow.workflow_kind='CONTRACT_EXPENSE'
      and coalesce(v_context#>>'{basis,empty_timesheet_consequence}','NONE')
        <>'NONE'
      and not exists(
        select 1 from public.candidate_expense_components other_component
        where other_component.workflow_id=v_workflow.id
          and other_component.expense_component_id<>v_component.expense_component_id
          and other_component.lifecycle_state not in (
            'MANAGER_REFUSED','OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED'
          )
      );
    if not v_direct_empty_pending then
      if v_is_paper_pending then
        perform set_config(
          'cloudtms.candidate_paper_update_begin_context',
          jsonb_build_object(
            'purpose','REJECT_EXPENSE_CATEGORY',
            'workflow_id',v_workflow.id,
            'workflow_generation',v_workflow.generation
          )::text,true
        );
      end if;
      v_begin:=public.candidate_expense_update_begin_atomic_v1(
        null,v_environment,v_workflow.id,v_workflow.generation,
        jsonb_build_array(jsonb_build_object(
          'update_kind','OFFICE_REJECT_CATEGORY',
          'expense_category',v_component.expense_category,
          'expense_component_id',v_component.expense_component_id,
          'component_generation',v_component.component_generation,
          'reason_note',v_reason
        )),'office-category-reject-begin:'||v_operation.operation_id::text,p_now_utc
      );
      if v_is_paper_pending then
        perform set_config('cloudtms.candidate_paper_update_begin_context','',true);
      end if;
      update public.candidate_pending_expense_updates set
        operation_id=v_operation.operation_id
      where update_id=(v_begin->>'update_id')::uuid;
      select * into v_workflow from public.candidate_submission_workflows
      where id=v_workflow.id;
      v_submit:=public.candidate_expense_update_submit_atomic_v1(
        null,v_environment,v_workflow.id,v_workflow.generation,
        (v_begin->>'update_id')::uuid,
        jsonb_build_object('immutable_submission',v_workflow.immutable_submission_json),
        'office-category-reject-submit:'||v_operation.operation_id::text,p_now_utc
      );
      update public.candidate_expense_operations set
        state='RENDERING',
        progress_json=coalesce(progress_json,'{}'::jsonb)||v_submit
          ||jsonb_build_object(
            'previous_owning_timesheet_id',
            (v_context#>>'{basis,owning_timesheet_id}')::uuid,
            'expected_delete_scope_sha256',
            coalesce(v_context#>>'{basis,delete_scope_sha256}','NO_DELETE'),
            'expected_delete_target_scope_sha256',
            coalesce(v_context#>>'{basis,delete_target_scope_sha256}','NO_DELETE')
          ),
        updated_at_utc=p_now_utc
      where operation_id=v_operation.operation_id;
      return v_submit||jsonb_build_object(
        'ok',true,'contract_version','OFFICE_EXPENSE_CATEGORY_REJECTION_PENDING_V1',
        'operation_id',v_operation.operation_id,'action_code','REJECT_EXPENSE_CATEGORY',
        'state','RENDERING','idempotent_replay',false
      );
    end if;
    if v_is_paper_pending then
      v_paper_retirement:=private._candidate_paper_delivery_retire_v1(
        v_workflow.id,v_workflow.generation,
        'EXPENSE_CATEGORY_OFFICE_REJECTED',p_now_utc
      );
      if not coalesce((v_paper_retirement->>'retired')::boolean,false)
         or not (
           coalesce((v_paper_retirement->>'qr_invalidated')::boolean,false)
           or coalesce((v_paper_retirement->>'qr_already_invalidated')::boolean,false)
         ) then
        raise exception 'CANDIDATE_PAPER_QR_INVALIDATION_NOT_PROVEN'
          using errcode='40001',detail=coalesce(v_paper_retirement,'{}'::jsonb)::text;
      end if;
    else
      perform private._candidate_empty_manager_request_cancel_v1(
        v_workflow.id,v_workflow.generation,'EXPENSE_CATEGORY_OFFICE_REJECTED',p_now_utc
      );
    end if;
  end if;
  v_before:=to_jsonb(v_component);
  v_financial:=private._candidate_expense_financial_remove_v1(
    v_component.expense_component_id,v_component.component_generation,p_now_utc
  );
  update public.candidate_expense_components set
    component_generation=component_generation+1,lifecycle_state='OFFICE_REJECTED',
    manager_approval_state=case when manager_approval_state='APPROVED'
      then 'APPROVED' else 'NOT_REQUESTED' end,
    approval_request_id=case when manager_approval_state='APPROVED'
      then approval_request_id else null end,
    refusal_kind='AGENCY_REJECTION',refusal_reason=v_reason,
    refused_at_utc=p_now_utc,removed_at_utc=p_now_utc,updated_at_utc=p_now_utc
  where expense_component_id=v_component.expense_component_id
  returning * into v_component;
  insert into public.candidate_expense_component_events(
    expense_component_id,workflow_id,component_generation,event_type,actor_kind,
    actor_id,reason,before_state_json,after_state_json,idempotency_key,occurred_at_utc
  ) values (
    v_component.expense_component_id,v_component.workflow_id,
    v_component.component_generation,'OFFICE_REJECTED','OFFICE',p_actor_user_id,
    v_reason,v_before,to_jsonb(v_component),
    'office-category-reject:'||v_operation.operation_id::text,p_now_utc
  );
  perform private._candidate_notification_insert_v1(
    v_workflow.account_id,v_workflow.candidate_id,v_workflow.id,
    nullif(v_financial->>'timesheet_id','')::uuid,'OFFICE_REJECTED',
    'timesheet_expense_attention','candidate-expense-category-rejected-v1',
    jsonb_build_object('workflow_id',v_workflow.id,
      'expense_component_id',v_component.expense_component_id,
      'expense_category',v_component.expense_category,'reason',v_reason,
      'resubmission_scope','EXPENSE_CATEGORY'),
    jsonb_build_object('type','workflow','workflow_id',v_workflow.id),
    'CANDIDATE_EXPENSE_CATEGORY_REJECTED_V1:'||v_operation.operation_id::text,
    p_now_utc
  );
  if v_direct_empty_pending then
    update public.candidate_submission_workflows workflow set
      state='CANCELLED',generation=workflow.generation+1,
      cancelled_at_utc=p_now_utc,updated_at_utc=p_now_utc
    where workflow.id=v_workflow.id
      and workflow.generation=v_workflow.generation
      and workflow.state=case when v_is_paper_pending
        then 'AWAITING_PAPER_RETURN' else 'AWAITING_MANAGER_APPROVAL' end
    returning workflow.* into v_workflow;
    if not found then
      raise exception 'CANDIDATE_EXPENSE_CATEGORY_CONTEXT_CHANGED' using errcode='40001';
    end if;
  end if;
  if coalesce((v_financial->>'zero_expense_carrier')::boolean,false) then
    v_begin:=private._candidate_zero_expense_carrier_delete_v1(
      v_environment,(v_financial->>'timesheet_id')::uuid,
      v_operation.operation_id,p_now_utc
    );
  else
    v_begin:=jsonb_build_object(
      'owning_timesheet_deleted',false,
      'empty_timesheet_consequence','NONE',
      'retained_timesheet_ids',jsonb_build_array(v_financial->'timesheet_id'),
      'affected_timesheet_ids',jsonb_build_array(v_financial->'timesheet_id'),
      'removed_from_current_timesheet_ids','[]'::jsonb
    );
  end if;
  v_result:=jsonb_build_object(
    'ok',true,'contract_version','OFFICE_EXPENSE_CATEGORY_REJECTION_RESULT_V2',
    'operation_id',v_operation.operation_id,'action_code','REJECT_EXPENSE_CATEGORY',
    'workflow_id',v_workflow.id,'expense_component_id',v_component.expense_component_id,
    'component_generation',v_component.component_generation,
    'state','OFFICE_REJECTED','refusal',jsonb_build_object(
      'kind','AGENCY_REJECTION','reason',v_reason,'at_utc',p_now_utc
    ),'previous_owning_timesheet_id',v_financial->'timesheet_id',
    'empty_timesheet_consequence',coalesce(
      v_begin->>'empty_timesheet_consequence','NONE'
    ),
    'owning_timesheet_deleted',coalesce(
      (v_begin->>'owning_timesheet_deleted')::boolean,false
    ),
    'deleted_timesheet_ids',coalesce(v_begin->'deleted_timesheet_ids','[]'::jsonb),
    'retained_timesheet_ids',coalesce(v_begin->'retained_timesheet_ids','[]'::jsonb),
    'removed_from_current_timesheet_ids',coalesce(
      v_begin->'removed_from_current_timesheet_ids','[]'::jsonb
    ),
    'affected_timesheet_ids',coalesce(v_begin->'affected_timesheet_ids',
      jsonb_build_array(v_financial->'timesheet_id')),
    'refresh_timesheet_ids',coalesce(v_begin->'affected_timesheet_ids',
      jsonb_build_array(v_financial->'timesheet_id')),
    'refresh_hints',jsonb_build_object(
      'summary',true,'simple_timesheet',true,'bulk_process',true,
      'bulk_authorise',true,'refetch','AFFECTED_ROWS'
    ),
    'idempotent_replay',false
  );
  update public.candidate_expense_operations set state='COMMITTED',result_json=v_result,
    completed_at_utc=p_now_utc,updated_at_utc=p_now_utc
  where operation_id=v_operation.operation_id;
  return v_result;
end;
$function$;

create or replace function public.candidate_office_expense_category_reject_commit_v1(
  p_environment text,
  p_operation_id uuid,
  p_update_id uuid,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_operation public.candidate_expense_operations%rowtype;
begin
  if p_operation_id is null or p_update_id is null then
    raise exception 'CANDIDATE_EXPENSE_CATEGORY_REJECTION_NOT_COMMITTED'
      using errcode='22023';
  end if;
  select * into v_operation from public.candidate_expense_operations operation
  where operation.operation_id=p_operation_id
    and operation.environment=private._candidate_assert_environment(p_environment)
    and operation.action_code='REJECT_EXPENSE_CATEGORY' for update;
  if not found then raise exception 'CANDIDATE_EXPENSE_OPERATION_NOT_FOUND' using errcode='P0002'; end if;
  if v_operation.state<>'COMMITTED' or not exists(
    select 1 from public.candidate_pending_expense_updates update_row
    where update_row.update_id=p_update_id
      and update_row.operation_id=v_operation.operation_id
      and update_row.state='COMMITTED'
  ) then
    raise exception 'CANDIDATE_EXPENSE_CATEGORY_REJECTION_NOT_COMMITTED'
      using errcode='40001';
  end if;
  if coalesce(v_operation.result_json->>'contract_version','')
       <>'OFFICE_EXPENSE_CATEGORY_REJECTION_RESULT_V2' then
    raise exception 'CANDIDATE_EXPENSE_CATEGORY_REJECTION_RECEIPT_INVALID'
      using errcode='55000';
  end if;
  return v_operation.result_json||jsonb_build_object('idempotent_replay',true);
end;
$function$;

alter function private._candidate_expense_number_v1(jsonb,text) owner to postgres;
alter function private._candidate_expense_payload_without_category_v1(jsonb,text) owner to postgres;
alter function private._candidate_expense_submission_without_category_v1(jsonb,text) owner to postgres;
alter function private._candidate_expense_component_values_v1(uuid) owner to postgres;
alter function private._candidate_expense_components_sync_v1(uuid,timestamptz) owner to postgres;
alter function private._candidate_expense_workflow_sync_trigger_v1() owner to postgres;
alter function private._candidate_expense_components_sync_financial_v1(uuid,timestamptz) owner to postgres;
alter function private._candidate_expense_financial_sync_trigger_v1() owner to postgres;
alter function private._candidate_expense_evidence_summary_trigger_v1() owner to postgres;
alter function private._candidate_expense_timesheet_sync_trigger_v1() owner to postgres;
alter function private._candidate_expense_identity_summary_trigger_v1() owner to postgres;
alter function private._candidate_expense_current_timesheet_id_v1(uuid,uuid) owner to postgres;
alter function private._candidate_expense_owned_timesheet_id_v1(uuid,uuid) owner to postgres;
alter function private._candidate_expense_owner_context_lock_v1(uuid) owner to postgres;
alter function private._candidate_expense_component_status_v1(text,text,text) owner to postgres;
alter function private._candidate_manager_decision_id_v1(uuid,integer,uuid,text) owner to postgres;
alter function private._candidate_expense_unmaterialised_prior_v1(uuid) owner to postgres;
alter function private._candidate_expense_financial_remove_v1(uuid,integer,timestamptz) owner to postgres;
alter function private._candidate_expense_component_action_v1(public.candidate_expense_components) owner to postgres;
alter function private._candidate_expense_component_json_v1(public.candidate_expense_components,boolean) owner to postgres;
alter function private._candidate_expense_begin_update_action_v1(uuid,boolean) owner to postgres;
alter function private._candidate_whole_claim_scope_v1(text,uuid,uuid,date) owner to postgres;
alter function private._candidate_whole_claim_action_v1(text,uuid) owner to postgres;
alter function private._candidate_hours_component_json_v1(text,uuid) owner to postgres;
alter function private._candidate_empty_manager_request_cancel_v1(uuid,integer,text,timestamptz) owner to postgres;
alter function public.candidate_expense_component_projection_v1(text,uuid[],uuid[]) owner to postgres;
alter function public.candidate_expense_update_begin_atomic_v1(uuid,text,uuid,integer,jsonb,text,timestamptz) owner to postgres;
alter function public.candidate_expense_update_submit_atomic_v1(uuid,text,uuid,integer,uuid,jsonb,text,timestamptz) owner to postgres;
alter function public.candidate_expense_update_rebind_atomic_v1(text,uuid,uuid,text,timestamptz) owner to postgres;
alter function public.candidate_expense_update_abort_atomic_v1(uuid,text,uuid,uuid,boolean,text,text,timestamptz) owner to postgres;
alter function public.candidate_expense_update_manager_hold_v1(text,uuid,text,timestamptz) owner to postgres;
alter function public.candidate_expense_update_recover_expired_v1(text,integer,timestamptz) owner to postgres;
alter function public.candidate_expense_component_action_atomic_v1(uuid,text,uuid,integer,uuid,integer,text,text,timestamptz) owner to postgres;
alter function public.candidate_whole_claim_action_atomic_v1(uuid,text,uuid,integer,uuid,text,text,text,text,timestamptz) owner to postgres;
alter function private._candidate_office_expense_rejection_context_v1(text,uuid,timestamptz) owner to postgres;
alter function public.candidate_office_expense_category_projection_v1(text,uuid,timestamptz) owner to postgres;
alter function public.candidate_office_expense_category_projection_batch_v1(text,uuid[],timestamptz) owner to postgres;
alter function public.candidate_office_expense_category_reject_atomic_v1(uuid,text,uuid,integer,uuid,integer,text,text,text,timestamptz) owner to postgres;
alter function public.candidate_office_expense_category_reject_commit_v1(text,uuid,uuid,timestamptz) owner to postgres;
alter function public.candidate_office_expense_category_adapter_v1(uuid,text,jsonb,timestamptz) owner to postgres;

revoke all on function private._candidate_expense_number_v1(jsonb,text) from public,anon,authenticated,service_role;
revoke all on function private._candidate_expense_payload_without_category_v1(jsonb,text) from public,anon,authenticated,service_role;
revoke all on function private._candidate_expense_submission_without_category_v1(jsonb,text) from public,anon,authenticated,service_role;
revoke all on function private._candidate_expense_component_values_v1(uuid) from public,anon,authenticated,service_role;
revoke all on function private._candidate_expense_components_sync_v1(uuid,timestamptz) from public,anon,authenticated,service_role;
revoke all on function private._candidate_expense_workflow_sync_trigger_v1() from public,anon,authenticated,service_role;
revoke all on function private._candidate_expense_components_sync_financial_v1(uuid,timestamptz) from public,anon,authenticated,service_role;
revoke all on function private._candidate_expense_financial_sync_trigger_v1() from public,anon,authenticated,service_role;
revoke all on function private._candidate_expense_evidence_summary_trigger_v1() from public,anon,authenticated,service_role;
revoke all on function private._candidate_expense_timesheet_sync_trigger_v1() from public,anon,authenticated,service_role;
revoke all on function private._candidate_expense_identity_summary_trigger_v1() from public,anon,authenticated,service_role;
revoke all on function private._candidate_expense_current_timesheet_id_v1(uuid,uuid) from public,anon,authenticated,service_role;
revoke all on function private._candidate_expense_owned_timesheet_id_v1(uuid,uuid) from public,anon,authenticated,service_role;
revoke all on function private._candidate_expense_owner_context_lock_v1(uuid) from public,anon,authenticated,service_role;
revoke all on function private._candidate_expense_component_status_v1(text,text,text) from public,anon,authenticated,service_role;
revoke all on function private._candidate_manager_decision_id_v1(uuid,integer,uuid,text) from public,anon,authenticated,service_role;
revoke all on function private._candidate_expense_unmaterialised_prior_v1(uuid) from public,anon,authenticated,service_role;
revoke all on function private._candidate_expense_financial_remove_v1(uuid,integer,timestamptz) from public,anon,authenticated,service_role;
revoke all on function private._candidate_expense_component_action_v1(public.candidate_expense_components) from public,anon,authenticated,service_role;
revoke all on function private._candidate_expense_component_json_v1(public.candidate_expense_components,boolean) from public,anon,authenticated,service_role;
revoke all on function private._candidate_expense_begin_update_action_v1(uuid,boolean) from public,anon,authenticated,service_role;
revoke all on function private._candidate_whole_claim_scope_v1(text,uuid,uuid,date) from public,anon,authenticated,service_role;
revoke all on function private._candidate_whole_claim_action_v1(text,uuid) from public,anon,authenticated,service_role;
revoke all on function private._candidate_hours_component_json_v1(text,uuid) from public,anon,authenticated,service_role;
revoke all on function private._candidate_empty_manager_request_cancel_v1(uuid,integer,text,timestamptz) from public,anon,authenticated,service_role;

revoke all on function public.candidate_expense_component_projection_v1(text,uuid[],uuid[]) from public,anon,authenticated,service_role;
revoke all on function public.candidate_expense_update_begin_atomic_v1(uuid,text,uuid,integer,jsonb,text,timestamptz) from public,anon,authenticated,service_role;
revoke all on function public.candidate_expense_update_submit_atomic_v1(uuid,text,uuid,integer,uuid,jsonb,text,timestamptz) from public,anon,authenticated,service_role;
revoke all on function public.candidate_expense_update_rebind_atomic_v1(text,uuid,uuid,text,timestamptz) from public,anon,authenticated,service_role;
revoke all on function public.candidate_expense_update_abort_atomic_v1(uuid,text,uuid,uuid,boolean,text,text,timestamptz) from public,anon,authenticated,service_role;
revoke all on function public.candidate_expense_update_manager_hold_v1(text,uuid,text,timestamptz) from public,anon,authenticated,service_role;
revoke all on function public.candidate_expense_update_recover_expired_v1(text,integer,timestamptz) from public,anon,authenticated,service_role;
revoke all on function public.candidate_expense_component_action_atomic_v1(uuid,text,uuid,integer,uuid,integer,text,text,timestamptz) from public,anon,authenticated,service_role;
revoke all on function public.candidate_whole_claim_action_atomic_v1(uuid,text,uuid,integer,uuid,text,text,text,text,timestamptz) from public,anon,authenticated,service_role;
revoke all on function private._candidate_office_expense_rejection_context_v1(text,uuid,timestamptz) from public,anon,authenticated,service_role;
revoke all on function public.candidate_office_expense_category_projection_v1(text,uuid,timestamptz) from public,anon,authenticated,service_role;
revoke all on function public.candidate_office_expense_category_projection_batch_v1(text,uuid[],timestamptz) from public,anon,authenticated,service_role;
revoke all on function public.candidate_office_expense_category_reject_atomic_v1(uuid,text,uuid,integer,uuid,integer,text,text,text,timestamptz) from public,anon,authenticated,service_role;
revoke all on function public.candidate_office_expense_category_reject_commit_v1(text,uuid,uuid,timestamptz) from public,anon,authenticated,service_role;
revoke all on function public.candidate_office_expense_category_adapter_v1(uuid,text,jsonb,timestamptz) from public,anon,authenticated,service_role;

grant execute on function public.candidate_expense_component_projection_v1(text,uuid[],uuid[]) to service_role;
grant execute on function public.candidate_expense_update_begin_atomic_v1(uuid,text,uuid,integer,jsonb,text,timestamptz) to service_role;
grant execute on function public.candidate_expense_update_submit_atomic_v1(uuid,text,uuid,integer,uuid,jsonb,text,timestamptz) to service_role;
grant execute on function public.candidate_expense_update_rebind_atomic_v1(text,uuid,uuid,text,timestamptz) to service_role;
grant execute on function public.candidate_expense_update_abort_atomic_v1(uuid,text,uuid,uuid,boolean,text,text,timestamptz) to service_role;
grant execute on function public.candidate_expense_update_manager_hold_v1(text,uuid,text,timestamptz) to service_role;
grant execute on function public.candidate_expense_update_recover_expired_v1(text,integer,timestamptz) to service_role;
grant execute on function public.candidate_expense_component_action_atomic_v1(uuid,text,uuid,integer,uuid,integer,text,text,timestamptz) to service_role;
grant execute on function public.candidate_whole_claim_action_atomic_v1(uuid,text,uuid,integer,uuid,text,text,text,text,timestamptz) to service_role;
grant execute on function public.candidate_office_expense_category_projection_v1(text,uuid,timestamptz) to service_role;
grant execute on function public.candidate_office_expense_category_projection_batch_v1(text,uuid[],timestamptz) to service_role;
grant execute on function public.candidate_office_expense_category_reject_commit_v1(text,uuid,uuid,timestamptz) to service_role;
grant execute on function public.candidate_office_expense_category_adapter_v1(uuid,text,jsonb,timestamptz) to service_role;

comment on function public.candidate_expense_component_projection_v1(text,uuid[],uuid[]) is
  'Set-based Candidate and Office projection for stable expense-category decisions, totals, actions, duplicate context and the independently proven hours decision.';
comment on function public.candidate_expense_component_action_atomic_v1(uuid,text,uuid,integer,uuid,integer,text,text,timestamptz) is
  'Executes one server-authorised expense-category action, rechecking its canonical owning Timesheet and component generation under lock.';
comment on function public.candidate_whole_claim_action_atomic_v1(uuid,text,uuid,integer,uuid,text,text,text,text,timestamptz) is
  'Atomically withdraws or cancels every unprotected live workflow in one linked Candidate Timesheet presentation.';
comment on function public.candidate_office_expense_category_projection_v1(text,uuid,timestamptz) is
  'Projects complete category-level expense status and exact rejection authority for one Office Timesheet view.';
comment on function public.candidate_office_expense_category_projection_batch_v1(text,uuid[],timestamptz) is
  'Projects category-level expense status for up to 100 Office Timesheet views in one service call.';
comment on function public.candidate_office_expense_category_adapter_v1(uuid,text,jsonb,timestamptz) is
  'Service-only Office adapter that opens exact category-rejection authority and returns cross-surface refresh instructions.';

notify pgrst, 'reload schema';

commit;
