-- Candidate App expense placement, carrier allocation and atomic application.

create or replace function public.expense_placement_resolve_v1(
  p_candidate_id uuid,
  p_environment text,
  p_anchor_timesheet_id uuid,
  p_contract_week_id uuid default null,
  p_proposed_claim jsonb default '{}'::jsonb,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_environment text;
  v_anchor_week public.contract_weeks%rowtype;
  v_contract public.contracts%rowtype;
  v_anchor_capabilities jsonb;
  v_positive_work boolean:=false;
  v_candidate_count integer:=0;
  v_candidate_week_id uuid;
  v_candidate_timesheet_id uuid;
  v_candidate_role text;
  v_result text;
  v_reason text;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  perform private._candidate_require_feature_v1(v_environment,'candidate_expense_atomic_placement');
  if p_candidate_id is null or (p_anchor_timesheet_id is null and p_contract_week_id is null) then
    raise exception 'EXPENSE_PLACEMENT_IDENTITY_REQUIRED' using errcode='22023';
  end if;
  if p_contract_week_id is not null then
    select * into v_anchor_week from public.contract_weeks where id=p_contract_week_id;
  else
    select * into v_anchor_week from public.contract_weeks
    where timesheet_id=p_anchor_timesheet_id order by updated_at desc,id desc limit 1;
  end if;
  if not found then raise exception 'EXPENSE_PLACEMENT_ANCHOR_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_contract from public.contracts where id=v_anchor_week.contract_id and candidate_id=p_candidate_id;
  if not found then raise exception 'EXPENSE_PLACEMENT_CANDIDATE_MISMATCH' using errcode='28000'; end if;
  v_anchor_capabilities:=private._candidate_record_capabilities_v1(
    coalesce(p_anchor_timesheet_id,v_anchor_week.timesheet_id),v_anchor_week.id,coalesce(p_proposed_claim,'{}'::jsonb)
  );
  if not coalesce((v_anchor_capabilities->>'candidate_expenses_allowed')::boolean,false) then
    return jsonb_build_object(
      'ok',true,'placement','BLOCKED','reason_code','CANDIDATE_RECORD_VIEW_ONLY',
      'anchor_timesheet_id',p_anchor_timesheet_id,'anchor_contract_week_id',v_anchor_week.id,
      'capabilities',v_anchor_capabilities
    );
  end if;

  select exists(
    select 1
    from public.contract_weeks cw
    join public.timesheets t on t.timesheet_id=cw.timesheet_id and t.is_current=true and t.archived_at_utc is null
    join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current=true
    where cw.contract_id=v_contract.id and cw.week_ending_date=v_anchor_week.week_ending_date
      and (
        coalesce(tf.total_hours,0)>0
        or private._candidate_json_numeric_sum(coalesce(tf.additional_units_json,'{}'::jsonb))>0
        or private._candidate_json_numeric_sum(coalesce(t.additional_units_week,'{}'::jsonb))
          +private._candidate_json_numeric_sum(coalesce(t.additional_units_per_day,'{}'::jsonb))>0
      )
  ) into v_positive_work;
  if not v_positive_work then
    return jsonb_build_object(
      'ok',true,'placement','BLOCKED','reason_code','NO_POSITIVE_WORKED_TIME',
      'anchor_timesheet_id',p_anchor_timesheet_id,'anchor_contract_week_id',v_anchor_week.id,
      'capabilities',v_anchor_capabilities
    );
  end if;

  if coalesce((v_anchor_capabilities->>'effective_separation')::boolean,false)=false
     and v_anchor_capabilities->>'record_role' in ('COMBINED_ALLOWED','EXPENSE_ONLY','FLEXIBLE')
     and coalesce((v_anchor_capabilities->>'protected')::boolean,false)=false
     and coalesce((v_anchor_capabilities->>'candidate_mutation_locked')::boolean,false)=false
     and coalesce((v_anchor_capabilities->>'can_edit_expenses')::boolean,false) then
    return jsonb_build_object(
      'ok',true,'placement','SAME_RECORD','reason_code','COMBINED_ALLOWED',
      'anchor_timesheet_id',coalesce(p_anchor_timesheet_id,v_anchor_week.timesheet_id),
      'target_timesheet_id',coalesce(p_anchor_timesheet_id,v_anchor_week.timesheet_id),
      'target_contract_week_id',v_anchor_week.id,'capabilities',v_anchor_capabilities
    );
  end if;

  with candidate_carriers as (
    select cw.id as contract_week_id,cw.timesheet_id,
      private._candidate_record_capabilities_v1(cw.timesheet_id,cw.id,coalesce(p_proposed_claim,'{}'::jsonb)) as capabilities
    from public.contract_weeks cw
    left join public.timesheets t on t.timesheet_id=cw.timesheet_id
    where cw.contract_id=v_contract.id and cw.week_ending_date=v_anchor_week.week_ending_date
      and cw.additional_seq>0
      and (t.timesheet_id is null or (t.is_current=true and t.archived_at_utc is null))
      and not exists(
        select 1 from public.candidate_submission_workflows w
        where w.contract_week_id=cw.id and w.candidate_id<>p_candidate_id
          and w.state in ('CREATED','WORKER_SUBMITTED','AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED','AWAITING_PAPER_RETURN','RECEIVED')
      )
  ), safe as (
    select * from candidate_carriers
    where capabilities->>'record_role' in ('EXPENSE_ONLY','FLEXIBLE')
      and coalesce((capabilities->>'protected')::boolean,false)=false
      and coalesce((capabilities->>'candidate_mutation_locked')::boolean,false)=false
      and coalesce((capabilities->>'import_authoritative')::boolean,false)=false
  )
  select count(*)::integer into v_candidate_count from safe;

  if v_candidate_count=1 then
    with candidate_carriers as (
      select cw.id as contract_week_id,cw.timesheet_id,
        private._candidate_record_capabilities_v1(cw.timesheet_id,cw.id,coalesce(p_proposed_claim,'{}'::jsonb)) as capabilities
      from public.contract_weeks cw
      left join public.timesheets t on t.timesheet_id=cw.timesheet_id
      where cw.contract_id=v_contract.id and cw.week_ending_date=v_anchor_week.week_ending_date
        and cw.additional_seq>0
        and (t.timesheet_id is null or (t.is_current=true and t.archived_at_utc is null))
        and not exists(
          select 1 from public.candidate_submission_workflows w
          where w.contract_week_id=cw.id and w.candidate_id<>p_candidate_id
            and w.state in ('CREATED','WORKER_SUBMITTED','AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED','AWAITING_PAPER_RETURN','RECEIVED')
        )
    )
    select contract_week_id,timesheet_id,capabilities->>'record_role'
    into v_candidate_week_id,v_candidate_timesheet_id,v_candidate_role
    from candidate_carriers
    where capabilities->>'record_role' in ('EXPENSE_ONLY','FLEXIBLE')
      and coalesce((capabilities->>'protected')::boolean,false)=false
      and coalesce((capabilities->>'candidate_mutation_locked')::boolean,false)=false
      and coalesce((capabilities->>'import_authoritative')::boolean,false)=false
    order by contract_week_id
    limit 1;
  end if;

  if v_candidate_count>1 then v_result:='BLOCKED';v_reason:='EXPENSE_CARRIER_AMBIGUOUS';
  elsif v_candidate_count=1 then v_result:='REUSE_CARRIER';v_reason:='SAFE_EXISTING_CARRIER';
  else v_result:='CREATE_CARRIER';v_reason:='NO_SAFE_CARRIER'; end if;

  return jsonb_build_object(
    'ok',true,'placement',v_result,'reason_code',v_reason,
    'anchor_timesheet_id',coalesce(p_anchor_timesheet_id,v_anchor_week.timesheet_id),
    'anchor_contract_week_id',v_anchor_week.id,
    'target_timesheet_id',v_candidate_timesheet_id,
    'target_contract_week_id',v_candidate_week_id,
    'target_record_role',v_candidate_role,
    'capabilities',v_anchor_capabilities
  );
end;
$function$;

create or replace function public.expense_carrier_resolve_or_create_atomic_v1(
  p_candidate_id uuid,
  p_environment text,
  p_anchor_timesheet_id uuid,
  p_expected_row_signature text,
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
  v_anchor_week public.contract_weeks%rowtype;
  v_contract public.contracts%rowtype;
  v_placement jsonb;
  v_signature jsonb;
  v_new_week public.contract_weeks%rowtype;
  v_next_seq integer;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  perform private._candidate_require_feature_v1(v_environment,'candidate_expense_atomic_placement');
  if p_candidate_id is null or p_anchor_timesheet_id is null or nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then
    raise exception 'EXPENSE_CARRIER_PAYLOAD_INVALID' using errcode='22023';
  end if;
  select * into v_anchor_week from public.contract_weeks where timesheet_id=p_anchor_timesheet_id for update;
  if not found then raise exception 'EXPENSE_PLACEMENT_ANCHOR_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_contract from public.contracts where id=v_anchor_week.contract_id and candidate_id=p_candidate_id for update;
  if not found then raise exception 'EXPENSE_PLACEMENT_CANDIDATE_MISMATCH' using errcode='28000'; end if;
  if nullif(btrim(coalesce(p_expected_row_signature,'')),'') is not null then
    v_signature:=public.timesheet_lifecycle_guard_signature_v1(p_anchor_timesheet_id,v_anchor_week.id,false);
    if coalesce(v_signature->>'row_signature',v_signature->>'backend_row_signature','')<>p_expected_row_signature then
      raise exception 'ROW_SIGNATURE_MISMATCH'
        using errcode='40001',detail=jsonb_build_object('code','ROW_SIGNATURE_MISMATCH')::text;
    end if;
  end if;
  perform pg_advisory_xact_lock(hashtext(v_contract.id::text||'|'||v_anchor_week.week_ending_date::text||'|EXPENSE_CARRIER'));
  perform 1 from public.contract_weeks cw
  where cw.contract_id=v_contract.id and cw.week_ending_date=v_anchor_week.week_ending_date
  order by cw.additional_seq,cw.id for update;
  v_placement:=public.expense_placement_resolve_v1(p_candidate_id,v_environment,p_anchor_timesheet_id,v_anchor_week.id,'{}'::jsonb,p_now_utc);
  if v_placement->>'placement'='BLOCKED' then
    raise exception '%',v_placement->>'reason_code' using errcode='55000',detail=v_placement::text;
  elsif v_placement->>'placement' in ('SAME_RECORD','REUSE_CARRIER') then
    return v_placement||jsonb_build_object('idempotent_replay',true,'idempotency_key',p_idempotency_key);
  end if;
  select coalesce(max(additional_seq),0)+1 into v_next_seq from public.contract_weeks
  where contract_id=v_contract.id and week_ending_date=v_anchor_week.week_ending_date;
  insert into public.contract_weeks(
    contract_id,week_ending_date,additional_seq,status,submission_mode_snapshot,
    day_entries_json,totals_json,planned_schedule_json,is_adjustment,
    enforce_day_partition,allowed_days_mask,split_boundary_date,split_group_key,
    created_at,updated_at
  ) values (
    v_contract.id,v_anchor_week.week_ending_date,v_next_seq,'OPEN','MANUAL',
    '[]'::jsonb,
    jsonb_build_object(
      'hours',jsonb_build_object('day',0,'night',0,'sat',0,'sun',0,'bh',0),
      'additional_units_week','{}'::jsonb,
      'additional_units_per_day','{}'::jsonb,
      'expenses_draft',jsonb_build_object(
        'mileage_units',0,'travel_pay',0,'travel_charge',0,
        'accommodation_pay',0,'accommodation_charge',0,
        'other_pay',0,'other_charge',0,'note',''
      )
    ),
    '[]'::jsonb,true,
    v_anchor_week.enforce_day_partition,v_anchor_week.allowed_days_mask,
    v_anchor_week.split_boundary_date,v_anchor_week.split_group_key,
    p_now_utc,p_now_utc
  ) returning * into v_new_week;
  perform private._candidate_audit_v1('contract_week',v_new_week.id::text,'CANDIDATE_EXPENSE_CARRIER_CREATED',null,
    jsonb_build_object('contract_id',v_contract.id,'week_ending_date',v_new_week.week_ending_date,'additional_seq',v_new_week.additional_seq),
    null,null,p_idempotency_key,p_now_utc);
  return jsonb_build_object(
    'ok',true,'placement','CREATE_CARRIER','reason_code','CARRIER_CREATED',
    'anchor_timesheet_id',p_anchor_timesheet_id,'anchor_contract_week_id',v_anchor_week.id,
    'target_timesheet_id',null,'target_contract_week_id',v_new_week.id,
    'target_record_role','FLEXIBLE','idempotent_replay',false,'idempotency_key',p_idempotency_key
  );
exception when unique_violation then
  v_placement:=public.expense_placement_resolve_v1(p_candidate_id,v_environment,p_anchor_timesheet_id,v_anchor_week.id,'{}'::jsonb,p_now_utc);
  if v_placement->>'placement'='REUSE_CARRIER' then
    return v_placement||jsonb_build_object('idempotent_replay',true,'idempotency_key',p_idempotency_key);
  end if;
  raise;
end;
$function$;

create or replace function public.timesheet_expense_apply_atomic_v1(
  p_candidate_id uuid,
  p_environment text,
  p_target_timesheet_id uuid,
  p_workflow_id uuid,
  p_expected_workflow_generation integer,
  p_expected_row_signature text,
  p_claim_json jsonb,
  p_evidence_component_ids uuid[],
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
  v_workflow public.candidate_submission_workflows%rowtype;
  v_week public.contract_weeks%rowtype;
  v_contract public.contracts%rowtype;
  v_anchor_timesheet public.timesheets%rowtype;
  v_target_timesheet_id uuid:=p_target_timesheet_id;
  v_claim jsonb:=coalesce(p_claim_json,'{}'::jsonb);
  v_snapshot jsonb;
  v_result jsonb;
  v_capabilities jsonb;
  v_response jsonb;
  v_component public.candidate_submission_components%rowtype;
  v_candidate_signature public.candidate_submission_components%rowtype;
  v_manager_signature public.candidate_submission_components%rowtype;
  v_electronic_patch jsonb:='{}'::jsonb;
  v_is_separate_carrier boolean:=false;
  v_is_paper boolean:=false;
  v_component_count integer:=0;
  v_required_categories text[]:='{}'::text[];
  v_category text;
  v_kind text;
  v_document_role text;
  v_paper_page jsonb;
  v_materialised_storage_key text;
  v_system_actor uuid;
  v_constraint_name text;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  perform private._candidate_require_feature_v1(v_environment,'candidate_expense_atomic_placement');
  if p_candidate_id is null or p_workflow_id is null or p_expected_workflow_generation is null
     or nullif(btrim(coalesce(p_idempotency_key,'')),'') is null or jsonb_typeof(v_claim)<>'object' then
    raise exception 'CANDIDATE_EXPENSE_APPLY_PAYLOAD_INVALID' using errcode='22023';
  end if;
  select * into v_workflow from public.candidate_submission_workflows where id=p_workflow_id for update;
  if not found or v_workflow.environment<>v_environment or v_workflow.candidate_id<>p_candidate_id then
    raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';
  end if;
  if v_workflow.last_mutation_idempotency_key=p_idempotency_key and v_workflow.last_mutation_response_json is not null then
    return v_workflow.last_mutation_response_json||jsonb_build_object('idempotent_replay',true);
  end if;
  if v_workflow.generation<>p_expected_workflow_generation then
    raise exception 'WORKFLOW_VERSION_MISMATCH'
      using errcode='40001',detail=jsonb_build_object('code','WORKFLOW_VERSION_MISMATCH','current_generation',v_workflow.generation)::text;
  end if;
  v_is_paper:=v_workflow.route='PAPER';
  if ((not v_is_paper and (
          v_workflow.state<>'READY_TO_FINALISE'
          or v_workflow.manager_approved_at_utc is null
        ))
      or (v_is_paper and v_workflow.state<>'RECEIVED')
      or current_setting('cloudtms.candidate_finalize_workflow',true)
        is distinct from v_workflow.id::text||':'||v_workflow.generation::text) then
    raise exception 'CANDIDATE_EXPENSE_APPLY_FINALISE_ONLY' using errcode='55000';
  end if;
  if v_workflow.workflow_kind not in ('CONTRACT_EXPENSE','CONTRACT_COMBINED') then
    raise exception 'CANDIDATE_EXPENSE_WORKFLOW_REQUIRED' using errcode='22023';
  end if;
  v_is_separate_carrier:=v_workflow.workflow_kind='CONTRACT_EXPENSE'
    or v_workflow.target_timesheet_id is distinct from v_workflow.anchor_timesheet_id;
  if v_workflow.workflow_kind='CONTRACT_COMBINED' and not v_is_separate_carrier and not v_is_paper then
    select * into v_candidate_signature from public.candidate_submission_components
    where id=v_workflow.candidate_signature_component_id and workflow_id=v_workflow.id
      and workflow_generation=v_workflow.generation and state='IMMUTABLE';
    select * into v_manager_signature from public.candidate_submission_components
    where id=v_workflow.manager_signature_component_id and workflow_id=v_workflow.id
      and workflow_generation=v_workflow.generation and state='IMMUTABLE';
    if v_candidate_signature.id is null or v_manager_signature.id is null
       or v_candidate_signature.source_content_sha256 is distinct from v_workflow.candidate_signature_sha256
       or v_manager_signature.source_content_sha256 is distinct from v_workflow.manager_signature_sha256 then
      raise exception 'ELECTRONIC_SIGNATURE_PAIR_INCOMPLETE' using errcode='55000';
    end if;
    v_electronic_patch:=jsonb_build_object(
      'submission_mode','ELECTRONIC','auth_name',v_workflow.manager_name,
      'auth_job_title',v_workflow.manager_position,'r2_nurse_key',v_candidate_signature.storage_key,
      'r2_auth_key',v_manager_signature.storage_key,
      'img_sha256_nurse',encode(v_candidate_signature.source_content_sha256,'hex'),
      'img_sha256_auth',encode(v_manager_signature.source_content_sha256,'hex'),
      'candidate_workflow_id',v_workflow.id,
      'candidate_workflow_generation',v_workflow.generation,
      'candidate_manager_approved_at_utc',v_workflow.manager_approved_at_utc);
  elsif v_is_paper then
    v_electronic_patch:=jsonb_build_object(
      'submission_mode','MANUAL',
      'r2_nurse_key',null,
      'r2_auth_key',null,
      'candidate_workflow_id',v_workflow.id,
      'candidate_workflow_generation',v_workflow.generation,
      'candidate_manager_approved_at_utc',null
    );
  end if;
  if v_workflow.immutable_submission_json is null
     or v_workflow.immutable_submission_sha256 is null
     or (
       (v_workflow.workflow_kind='CONTRACT_EXPENSE'
         and (private._candidate_sha256_jsonb_v1(v_claim) is distinct from v_workflow.immutable_submission_sha256
           or v_claim is distinct from v_workflow.immutable_submission_json))
       or (v_workflow.workflow_kind='CONTRACT_COMBINED'
         and v_claim is distinct from v_workflow.immutable_submission_json
         and v_claim is distinct from v_workflow.immutable_submission_json->'expense_submission')
     ) then
    raise exception 'CANDIDATE_IMMUTABLE_SUBMISSION_MISMATCH' using errcode='40001';
  end if;
  select * into v_week from public.contract_weeks
  where id=v_workflow.contract_week_id for update;
  if not found then raise exception 'CANDIDATE_CONTRACT_WEEK_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_contract from public.contracts where id=v_week.contract_id and candidate_id=p_candidate_id for update;
  if not found then raise exception 'CANDIDATE_WORKFLOW_OWNERSHIP_MISMATCH' using errcode='28000'; end if;
  if v_workflow.anchor_timesheet_id is not null then
    select * into v_anchor_timesheet from public.timesheets
    where timesheet_id=v_workflow.anchor_timesheet_id and is_current=true;
  end if;
  select candidate_app_system_actor_user_id into v_system_actor from public.settings_defaults where id=1;
  if v_system_actor is null then
    raise exception 'CANDIDATE_SYSTEM_ACTOR_NOT_CONFIGURED' using errcode='55000';
  end if;
  if v_target_timesheet_id is not null and v_week.timesheet_id is distinct from v_target_timesheet_id then
    raise exception 'TIMESHEET_MOVED' using errcode='40001';
  end if;

  v_snapshot:=v_claim->'canonical_tsfin_snapshot';
  if v_snapshot is null or jsonb_typeof(v_snapshot)<>'object' then
    raise exception 'CANDIDATE_CANONICAL_TSFIN_SNAPSHOT_REQUIRED' using errcode='22023';
  end if;
  if nullif(v_snapshot->>'candidate_id','')::uuid is distinct from p_candidate_id
     or nullif(v_snapshot->>'client_id','')::uuid is distinct from v_contract.client_id then
    raise exception 'TSFIN_SNAPSHOT_MISMATCH' using errcode='22023';
  end if;
  if coalesce(nullif(v_snapshot->>'expenses_pay_ex_vat','')::numeric,0)<0
     or coalesce(nullif(v_snapshot->>'expenses_charge_ex_vat','')::numeric,0)<0
     or coalesce(nullif(v_snapshot->>'mileage_units','')::numeric,0)<0
     or coalesce(nullif(v_snapshot->>'mileage_pay_ex_vat','')::numeric,0)<0
     or coalesce(nullif(v_snapshot->>'mileage_charge_ex_vat','')::numeric,0)<0
     or coalesce(nullif(v_snapshot->>'travel_pay_ex_vat','')::numeric,0)<0
     or coalesce(nullif(v_snapshot->>'travel_charge_ex_vat','')::numeric,0)<0
     or coalesce(nullif(v_snapshot->>'accommodation_pay_ex_vat','')::numeric,0)<0
     or coalesce(nullif(v_snapshot->>'accommodation_charge_ex_vat','')::numeric,0)<0
     or coalesce(nullif(v_snapshot->>'other_pay_ex_vat','')::numeric,0)<0
     or coalesce(nullif(v_snapshot->>'other_charge_ex_vat','')::numeric,0)<0 then
    raise exception 'CANDIDATE_EXPENSE_VALUE_INVALID' using errcode='22023';
  end if;

  if coalesce(nullif(v_snapshot->>'travel_pay_ex_vat','')::numeric,0)>0
     or coalesce(nullif(v_snapshot->>'travel_charge_ex_vat','')::numeric,0)>0 then
    v_required_categories:=array_append(v_required_categories,'TRAVEL');
  end if;
  if coalesce(nullif(v_snapshot->>'accommodation_pay_ex_vat','')::numeric,0)>0
     or coalesce(nullif(v_snapshot->>'accommodation_charge_ex_vat','')::numeric,0)>0 then
    v_required_categories:=array_append(v_required_categories,'ACCOMMODATION');
  end if;
  if coalesce(nullif(v_snapshot->>'other_pay_ex_vat','')::numeric,0)>0
     or coalesce(nullif(v_snapshot->>'other_charge_ex_vat','')::numeric,0)>0
     or coalesce(nullif(v_snapshot->>'expenses_pay_ex_vat','')::numeric,0)>0
     or coalesce(nullif(v_snapshot->>'expenses_charge_ex_vat','')::numeric,0)>0 then
    v_required_categories:=array_append(v_required_categories,'OTHER');
  end if;
  if coalesce(nullif(v_snapshot->>'mileage_units','')::numeric,0)>0
     or coalesce(nullif(v_snapshot->>'mileage_pay_ex_vat','')::numeric,0)>0
     or coalesce(nullif(v_snapshot->>'mileage_charge_ex_vat','')::numeric,0)>0 then
    v_required_categories:=array_append(v_required_categories,'MILEAGE');
  end if;

  select count(*)::integer into v_component_count
  from public.candidate_submission_components component
  where component.id=any(coalesce(p_evidence_component_ids,'{}'::uuid[]))
    and component.workflow_id=v_workflow.id
    and component.workflow_generation=v_workflow.generation
    and component.state='IMMUTABLE'
    and component.immutable_at_utc is not null
    and (
      (not v_is_paper
        and component.required=true
        and component.component_kind<>'HOURS_TIMESHEET'
        and component.review_render_state='READY'
        and component.final_signed_render_state='READY'
        and component.final_signed_content_sha256 is not null)
      or (v_is_paper
        and component.component_kind='SIGNED_RETURN'
        and component.source_content_sha256 is not null
        and component.paper_return_page_key<>'HOURS_TIMESHEET')
    );
  if v_component_count<>coalesce(cardinality(p_evidence_component_ids),0) then
    raise exception 'CANDIDATE_EVIDENCE_COMPONENT_INVALID' using errcode='22023';
  end if;
  if not v_is_paper and exists(
    select 1 from public.candidate_submission_components component
    where component.workflow_id=v_workflow.id
      and component.workflow_generation=v_workflow.generation
      and component.required=true and component.state<>'SUPERSEDED'
      and component.component_kind<>'HOURS_TIMESHEET'
      and not (component.id=any(coalesce(p_evidence_component_ids,'{}'::uuid[])))
  ) then
    raise exception 'CANDIDATE_EVIDENCE_COMPONENT_SET_INCOMPLETE' using errcode='22023';
  end if;
  if v_is_paper and exists(
    select 1
    from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages') expected_page
    where expected_page->>'page_key'<>'HOURS_TIMESHEET'
      and not exists(
        select 1
        from public.candidate_submission_components component
        where component.id=any(coalesce(p_evidence_component_ids,'{}'::uuid[]))
          and component.workflow_id=v_workflow.id
          and component.workflow_generation=v_workflow.generation
          and component.component_kind='SIGNED_RETURN'
          and component.paper_return_page_key=expected_page->>'page_key'
          and component.state='IMMUTABLE'
      )
  ) then
    raise exception 'CANDIDATE_EVIDENCE_COMPONENT_SET_INCOMPLETE' using errcode='22023';
  end if;
  foreach v_category in array v_required_categories loop
    if not exists(
      select 1
      from public.candidate_submission_components component
      where component.id=any(coalesce(p_evidence_component_ids,'{}'::uuid[]))
        and component.workflow_id=v_workflow.id and component.workflow_generation=v_workflow.generation
        and component.state='IMMUTABLE'
        and (
          (not v_is_paper
            and component.expense_category=v_category
            and component.final_signed_render_state='READY'
            and component.document_role in ('SOURCE_EVIDENCE','MILEAGE_CLAIM_FORM'))
          or (v_is_paper and exists(
            select 1
            from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages') expected_page
            where expected_page->>'page_key'=component.paper_return_page_key
              and expected_page->>'expense_category'=v_category
          ))
        )
    ) then
      raise exception 'EXPENSE_EVIDENCE_REQUIRED'
        using errcode='22023',detail=jsonb_build_object('code','EXPENSE_EVIDENCE_REQUIRED','category',v_category)::text;
    end if;
  end loop;
  if 'MILEAGE'=any(v_required_categories) and not exists(
    select 1 from public.candidate_submission_components component
    where component.id=any(coalesce(p_evidence_component_ids,'{}'::uuid[]))
      and component.workflow_id=v_workflow.id
      and component.workflow_generation=v_workflow.generation
      and component.state='IMMUTABLE'
      and (
        (not v_is_paper
          and component.component_kind='MILEAGE_FORM'
          and component.expense_category='MILEAGE'
          and component.final_signed_render_state='READY')
        or (v_is_paper and exists(
          select 1
          from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages') expected_page
          where expected_page->>'page_key'=component.paper_return_page_key
            and expected_page->>'component_kind'='MILEAGE_FORM'
            and expected_page->>'expense_category'='MILEAGE'
        ))
      )
  ) then
    raise exception 'EXPENSE_EVIDENCE_REQUIRED'
      using errcode='22023',detail=jsonb_build_object(
        'code','EXPENSE_EVIDENCE_REQUIRED','category','MILEAGE','document_role','MILEAGE_CLAIM_FORM')::text;
  end if;

  v_result:=public.contract_week_manual_upsert_atomic(
    p_week_id=>v_week.id,
    p_expected_timesheet_id=>v_target_timesheet_id,
    p_timesheet_create_json=>case when v_target_timesheet_id is null then
      coalesce(v_claim->'timesheet_create_json','{}'::jsonb)
        ||case when v_is_separate_carrier then jsonb_build_object(
          'booking_id','CANDIDATE-EXPENSE-'||replace(v_week.id::text,'-',''),
          'contract_id',v_week.contract_id,
          'week_ending_date',v_week.week_ending_date,
          'status','SUBMITTED',
          'submission_mode','MANUAL',
          'sheet_scope','WEEKLY',
          'line_type','EXPENSES',
          'is_adjustment',true,
          'occupant_key_norm',v_anchor_timesheet.occupant_key_norm,
          'hospital_norm',v_anchor_timesheet.hospital_norm,
          'ward_norm',v_anchor_timesheet.ward_norm,
          'job_title_norm',v_anchor_timesheet.job_title_norm,
          'shift_label_norm','weekly-expenses',
          'band',v_anchor_timesheet.band,
          'candidate_hint_text',coalesce(v_anchor_timesheet.candidate_hint_text,'{}'::jsonb),
          'manual_pdf_r2_key',null
        ) else v_electronic_patch end
      else null end,
    p_timesheet_patch_json=>coalesce(v_claim->'timesheet_patch_json','{}'::jsonb)
      ||case when v_is_separate_carrier then '{}'::jsonb else v_electronic_patch end,
    p_contract_week_patch_json=>coalesce(v_claim->'contract_week_patch_json','{}'::jsonb),
    p_tsfin_snapshot_json=>v_snapshot,
    p_rotation_json=>null,
    p_actor_user_id=>v_system_actor,
    p_materialise_staged_evidence=>false,
    p_now_utc=>p_now_utc,
    p_expected_row_signature=>p_expected_row_signature,
    p_queue_timesheet_materialisation_json=>jsonb_build_object('suppress_timesheet_evidence_materialisation',true)
  );
  v_target_timesheet_id:=coalesce(nullif(v_result->>'timesheet_id','')::uuid,nullif(v_result#>>'{timesheet,timesheet_id}','')::uuid);
  if v_target_timesheet_id is null then raise exception 'CANDIDATE_EXPENSE_TARGET_NOT_CREATED' using errcode='55000'; end if;

  for v_component in
    select * from public.candidate_submission_components
    where id=any(coalesce(p_evidence_component_ids,'{}'::uuid[])) order by component_no,id for update
  loop
    update public.candidate_submission_components set timesheet_id=v_target_timesheet_id
    where id=v_component.id;
    if v_is_paper then
      select expected_page into v_paper_page
      from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages') expected_page
      where expected_page->>'page_key'=v_component.paper_return_page_key;
      v_category:=nullif(v_paper_page->>'expense_category','');
      v_document_role:=case v_paper_page->>'component_kind'
        when 'EXPENSE_SUMMARY' then 'EXPENSE_MILEAGE_APPROVAL_SUMMARY'
        when 'MILEAGE_FORM' then 'MILEAGE_CLAIM_FORM'
        else 'SOURCE_EVIDENCE' end;
      v_materialised_storage_key:=v_component.storage_key;
    else
      v_category:=v_component.expense_category;
      v_document_role:=v_component.document_role;
      v_materialised_storage_key:=v_component.final_signed_storage_key;
    end if;
    v_kind:=case
      when v_document_role='EXPENSE_MILEAGE_APPROVAL_SUMMARY' then 'OTHER'
      when v_document_role='MILEAGE_CLAIM_FORM' then 'MILEAGE'
      when v_document_role='SIGNED_TIMESHEET' then 'TIMESHEET'
      when v_category='MILEAGE' then 'MILEAGE'
      else v_category end;
    insert into public.timesheet_evidence(
      timesheet_id,kind,display_name,storage_key,created_at,document_role,candidate_component_id,processing_state
    ) values (
      v_target_timesheet_id,v_kind,coalesce(nullif(v_claim->>'evidence_display_name',''),'Candidate submission evidence'),
       v_materialised_storage_key,p_now_utc,v_document_role,v_component.id,'READY'
    ) on conflict (candidate_component_id) where candidate_component_id is not null do nothing;
  end loop;

  v_capabilities:=private._candidate_record_capabilities_v1(v_target_timesheet_id,v_week.id,'{}'::jsonb);
  if v_capabilities->>'record_role'='CONFLICT' then
    raise exception 'HOURS_AND_EXPENSES_REQUIRE_SEPARATE_TIMESHEETS'
      using errcode='55000',detail=v_capabilities::text;
  end if;
  v_response:=jsonb_build_object(
    'ok',true,'workflow_id',v_workflow.id,'generation',v_workflow.generation,
    'target_timesheet_id',v_target_timesheet_id,'target_contract_week_id',v_week.id,
    'canonical_result',v_result,'capabilities',v_capabilities,'idempotent_replay',false
  );
  update public.candidate_submission_workflows set
    target_timesheet_id=v_target_timesheet_id,contract_week_id=v_week.id,updated_at_utc=p_now_utc
  where id=v_workflow.id;
  perform private._candidate_audit_v1('candidate_submission_workflow',v_workflow.id::text,'CANDIDATE_EXPENSE_APPLIED',null,
    jsonb_build_object('timesheet_id',v_target_timesheet_id,'contract_week_id',v_week.id,'component_count',v_component_count),
    null,v_system_actor,p_idempotency_key,p_now_utc);
  return v_response;
exception when unique_violation then
  get stacked diagnostics v_constraint_name=constraint_name;
  if v_constraint_name='candidate_submission_components_source_sha256_uq' then
    raise exception 'CANDIDATE_EVIDENCE_BYTES_ALREADY_USED' using errcode='23505';
  elsif v_constraint_name='timesheet_evidence_one_active_timesheet_uq' then
    raise exception 'TIMESHEET_EVIDENCE_ALREADY_ATTACHED' using errcode='23505';
  end if;
  raise;
end;
$function$;

revoke all on function public.expense_placement_resolve_v1(uuid,text,uuid,uuid,jsonb,timestamptz) from public,anon,authenticated;
revoke all on function public.expense_carrier_resolve_or_create_atomic_v1(uuid,text,uuid,text,text,timestamptz) from public,anon,authenticated;
revoke all on function public.timesheet_expense_apply_atomic_v1(uuid,text,uuid,uuid,integer,text,jsonb,uuid[],text,timestamptz) from public,anon,authenticated;
grant execute on function public.expense_placement_resolve_v1(uuid,text,uuid,uuid,jsonb,timestamptz) to service_role;
grant execute on function public.expense_carrier_resolve_or_create_atomic_v1(uuid,text,uuid,text,text,timestamptz) to service_role;
grant execute on function public.timesheet_expense_apply_atomic_v1(uuid,text,uuid,uuid,integer,text,jsonb,uuid[],text,timestamptz) to service_role;
