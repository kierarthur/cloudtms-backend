-- Repeatable CloudTMS function/view authority: weekly_source_local_protected_decision_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- User ruling, 4 October 2026: retire external C1 as a protected Save gate
-- (WP-06 section 4.2 R1). Existing C1 streams remain immutable audit evidence.
-- This owns no Banking Pay rows, residuals, provider operations or invoices.
create or replace function private.weekly_source_local_preauthorisation_write_allowed_v1(
  p_root_timesheet_id uuid, p_snapshot jsonb
) returns boolean
language sql volatile security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
  select count(*)=1
  from private.weekly_source_local_protected_decision_receipts receipt
  join public.weekly_exceptional_pay_target_families family on family.id=receipt.family_id
  join public.weekly_exceptional_pay_generations generation on generation.id=receipt.generation_id
  join public.timesheets root on root.timesheet_id=receipt.root_timesheet_id
  where receipt.root_timesheet_id=p_root_timesheet_id
    and receipt.state='PREPARING'
    and receipt.preparing_transaction_id=pg_catalog.pg_current_xact_id()
    and family.root_timesheet_id=root.timesheet_id
    and family.ownership_state='TARGET_MANAGED'
    and generation.family_id=family.id and generation.lifecycle_state='PENDING_C1'
    and root.is_current and root.revoked_at is null and root.archived_at_utc is null
    and root.authorised_at_server is null
    and not exists(select 1 from public.weekly_source_root_authorisations authority
                   where authority.root_timesheet_id=root.timesheet_id and authority.withdrawn_at_utc is null)
    and (select count(*) from public.timesheets_financials financial
         where financial.timesheet_id=root.timesheet_id and financial.is_current)=1
    and exists(select 1 from public.timesheets_financials financial
      where financial.id=receipt.prior_financial_id and financial.is_current
        and financial.timesheet_id=root.timesheet_id and financial.authorised_at_utc is null
        and financial.processing_status='PENDING_AUTH'
        and financial.paid_at_utc is null and financial.locked_by_invoice_id is null)
    and (p_snapshot is null or p_snapshot=receipt.approved_snapshot_json);
$function$;

create or replace function public.weekly_exceptional_pay_complete_local_v1(p_request jsonb)
returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
set lock_timeout to '2500ms'
set statement_timeout to '30s'
as $function$
declare
  v_keys constant text[]:=array['schema_version','actor_user_id','publication_request_id',
    'expected_request_sha256','idempotency_key'];
  v_actor uuid;
  v_request_id uuid;
  v_hash bytea;
  v_key text;
  v_request public.weekly_exceptional_c1_publication_requests%rowtype;
  v_generation public.weekly_exceptional_pay_generations%rowtype;
  v_family public.weekly_exceptional_pay_target_families%rowtype;
  v_run public.weekly_exceptional_orchestration_runs%rowtype;
  v_approval public.weekly_exceptional_payment_approvals%rowtype;
  v_root public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_receipt private.weekly_source_local_protected_decision_receipts%rowtype;
  v_locks jsonb;
  v_snapshot jsonb;
  v_result jsonb;
  v_write jsonb;
  v_proposal jsonb;
  v_components jsonb;
  v_expenses jsonb;
  v_head_request jsonb;
  v_target_state text;
  v_source_hash bytea;
  v_result_hash bytea;
  v_live_authorisations integer;
  v_bundle_id uuid;
  v_local_context jsonb;
  v_local_origin jsonb;
  v_pending boolean:=false;
  v_manual_review_id uuid;
  v_rate_policy_hash bytea;
  v_scope_date date; v_scope_group uuid; v_scope_client uuid;
begin
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  if not private.weekly_exceptional_json_keys_exact_v1(p_request,v_keys)
     or not (p_request ?& v_keys)
     or p_request->>'schema_version'<>'WEEKLY_PROTECTED_LOCAL_COMPLETE_V1' then
    raise exception 'WEEKLY_PROTECTED_LOCAL_COMPLETE_INVALID' using errcode='22023';
  end if;
  v_actor:=(p_request->>'actor_user_id')::uuid;
  v_request_id:=(p_request->>'publication_request_id')::uuid;
  v_hash:=private.weekly_exceptional_hex_sha256_v1(p_request->>'expected_request_sha256');
  v_key:=pg_catalog.btrim(coalesce(p_request->>'idempotency_key',''));
  if v_actor is null or v_request_id is null or pg_catalog.char_length(v_key) not between 16 and 240
     or not exists(select 1 from public.tms_users actor where actor.id=v_actor and actor.is_active
                    and (actor.payment_authoriser or actor.payment_golden_key)) then
    raise exception 'WEEKLY_PROTECTED_LOCAL_COMPLETE_SCOPE_INVALID' using errcode='42501';
  end if;

  -- Exact committed receipt precedes freshness checks, locks and publication.
  select * into v_receipt from private.weekly_source_local_protected_decision_receipts
    where publication_request_id=v_request_id;
  -- The retained action's own report cutoff remains the Office authority,
  -- including exact replay. Work date and a later unrelated upload are not it.
  select c.finalisation_week_ending,c.source_group_id,a.client_id
    into strict v_scope_date,v_scope_group,v_scope_client
    from public.weekly_exceptional_c1_publication_requests r
    join public.weekly_exceptional_payment_approvals a
      on a.creation_orchestration_run_id=r.orchestration_run_id and a.pay_target_family_id=r.family_id
    join public.weekly_source_cycles c on c.id=a.source_cycle_id
    where r.id=v_request_id;
  perform private.weekly_source_office_authority_v1(v_actor,'APPROVE_PROTECTED_PAY',
    v_scope_group,v_scope_client,v_scope_date);
  -- SELECT INTO above changes FOUND; test the retained receipt identity itself.
  if v_receipt.publication_request_id is not null then
    if v_receipt.actor_user_id is distinct from v_actor or v_receipt.request_sha256 is distinct from v_hash
       or v_receipt.idempotency_key is distinct from v_key or v_receipt.state='PREPARING' then
      raise exception 'WEEKLY_PROTECTED_LOCAL_REPLAY_CONFLICT' using errcode='55000';
    end if;
    return v_receipt.result_json||jsonb_build_object('idempotent_replay',true);
  end if;
  perform private.weekly_source_pay_query_admit_v2();
  select * into strict v_request from public.weekly_exceptional_c1_publication_requests where id=v_request_id;
  -- I-1: Candidate serial gate, then all canonical family members, then rows.
  v_locks:=private.weekly_source_lock_and_resolve_families_v1(v_request.candidate_id,
    array[v_request.root_timesheet_id],'WORKBENCH_CANDIDATE_ENTITLEMENT_PUBLICATION',
    v_request.id,'WEEKLY_SOURCE_ENTITLEMENT_PUBLICATION');
  if coalesce((v_locks->>'ok')::boolean,false) is not true then
    return v_locks||jsonb_build_object('ok',false);
  end if;
  select * into v_receipt from private.weekly_source_local_protected_decision_receipts
    where publication_request_id=v_request_id;
  if found then
    if v_receipt.actor_user_id is distinct from v_actor or v_receipt.request_sha256 is distinct from v_hash
       or v_receipt.idempotency_key is distinct from v_key or v_receipt.state='PREPARING' then
      raise exception 'WEEKLY_PROTECTED_LOCAL_REPLAY_CONFLICT' using errcode='55000';
    end if;
    return v_receipt.result_json||jsonb_build_object('idempotent_replay',true);
  end if;
  select * into strict v_family from public.weekly_exceptional_pay_target_families
    where id=v_request.family_id for update;
  select * into strict v_request from public.weekly_exceptional_c1_publication_requests
    where id=v_request_id for update;
  select * into strict v_generation from public.weekly_exceptional_pay_generations
    where id=v_request.generation_id and family_id=v_family.id for update;
  select * into strict v_run from public.weekly_exceptional_orchestration_runs
    where id=v_request.orchestration_run_id and family_id=v_family.id for update;
  select * into strict v_approval from public.weekly_exceptional_payment_approvals
    where creation_orchestration_run_id=v_run.id and pay_target_family_id=v_family.id for share;
  select * into strict v_root from public.timesheets where timesheet_id=v_family.root_timesheet_id for update;
  select * into strict v_fin from public.timesheets_financials
    where timesheet_id=v_root.timesheet_id and is_current for update;
  if v_hash is distinct from v_request.request_sha256 or v_run.requested_by_user_id is distinct from v_actor
     or v_request.state<>'READY' or v_run.state<>'RUNNING' or v_generation.lifecycle_state<>'PENDING_C1'
     or v_request.c1_operation_id is not null or v_request.c1_publication_id is not null
     or v_request.c1_head_revision is not null or v_request.c1_receipt_sha256 is not null
     or exists(select 1 from public.weekly_exceptional_c1_publication_checkpoints c where c.publication_request_id=v_request.id)
     or exists(select 1 from public.weekly_exceptional_c1_unknown_outcomes u
                where u.publication_request_id=v_request.id and u.state='RECOVERY_REQUIRED')
     or v_family.current_generation_id is distinct from v_generation.prior_generation_id
     or v_family.current_generation_number+1 is distinct from v_generation.generation_number
     or v_fin.id is distinct from v_request.financial_row_id
     or not v_root.is_current or v_root.revoked_at is not null or v_root.archived_at_utc is not null
     or v_root.contract_id is distinct from v_family.contract_id
     or v_root.week_ending_date is distinct from v_family.week_ending_date
     or v_root.version is distinct from (v_generation.complete_next_vector_json#>>'{target_snapshot,tsfin_snapshot_json,timesheet_version}')::integer
     or v_approval.withdrawn_at_utc is not null
     or exists(select 1 from private.weekly_source_local_protected_decision_receipts r
               where r.family_id=v_family.id and r.state='PENDING_FREEZE') then
    raise exception 'WEEKLY_PROTECTED_LOCAL_STATE_CHANGED' using errcode='55000';
  end if;
  select target.resulting_lifecycle_state into strict v_target_state
    from public.weekly_exceptional_pay_target_events target
    where target.financial_generation_id=v_generation.id and target.family_id=v_family.id;
  select event.source_proposal_hash into strict v_source_hash
    from public.weekly_exceptional_pay_family_events event
    where event.evidence_approval_id=v_approval.id and event.family_id=v_family.id;
  select private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_RATE_POLICY_V1',
    jsonb_build_object('contract_id',v_family.contract_id,'rates_json',contract.rates_json,
      'policy',private._weekly_source_effective_policy_v1(v_approval.client_id,v_family.contract_id,
        v_approval.protected_work_date),'classification',event.rate_classification_json))
    into strict v_rate_policy_hash
    from public.weekly_exceptional_pay_family_events event
    join public.contracts contract on contract.id=v_family.contract_id
    where event.evidence_approval_id=v_approval.id and event.family_id=v_family.id;
  if v_rate_policy_hash is distinct from v_approval.contract_rate_policy_source_fingerprint then
    raise exception 'WEEKLY_PROTECTED_LOCAL_RATE_POLICY_CHANGED' using errcode='40001';
  end if;
  v_snapshot:=v_generation.complete_next_vector_json#>'{target_snapshot,tsfin_snapshot_json}';
  if jsonb_typeof(v_snapshot) is distinct from 'object'
     or private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_COMPLETE_VECTOR_V1',
        v_generation.complete_next_vector_json) is distinct from v_generation.complete_next_vector_hash
     or v_snapshot is distinct from v_approval.approved_target_pay_components_json->'tsfin_snapshot_json' then
    raise exception 'WEEKLY_PROTECTED_LOCAL_SNAPSHOT_INVALID' using errcode='55000';
  end if;
  v_snapshot:=v_snapshot||jsonb_build_object('timesheet_id',v_root.timesheet_id::text,
    'timesheet_version',v_root.version,'actual_schedule_json',
    v_generation.complete_next_vector_json#>'{target_snapshot,actual_schedule_json}');
  -- Reconfirm the staged identity proof before either mutable first-approval
  -- TSFIN or the common HEAD writer. Immutable old requests without this new
  -- metadata retain their existing route; they are not silently remapped.
  if v_generation.complete_next_vector_json ? 'protected_component_identity_basis'
     or exists(select 1 from jsonb_array_elements(v_snapshot#>'{invoice_breakdown_json,segments}') r
       where r ? 'weekly_protected_component_identity') then
    if v_generation.complete_next_vector_json#>>'{protected_component_identity_basis,schema_version}'
          is distinct from 'WEEKLY_PROTECTED_COMPONENT_MANIFEST_V1'
       or v_generation.complete_next_vector_json#>>'{protected_component_identity_basis,family_id}'
          is distinct from v_family.id::text
       or v_generation.complete_next_vector_json#>>'{protected_component_identity_basis,root_timesheet_id}'
          is distinct from v_root.timesheet_id::text
       or v_generation.complete_next_vector_json#>>'{protected_component_identity_basis,contract_id}'
          is distinct from v_family.contract_id::text
       or v_generation.complete_next_vector_json#>>'{protected_component_identity_basis,candidate_id}'
          is distinct from v_family.candidate_id::text
       or v_generation.complete_next_vector_json#>>'{protected_component_identity_basis,accepted_schedule_sha256}'
          is distinct from encode(private.weekly_source_sha256_jsonb_v1(
            'WEEKLY_PROTECTED_COMPONENT_ACCEPTED_SCHEDULE_V1',v_snapshot->'actual_schedule_json'),'hex')
       or v_generation.complete_next_vector_json#>>'{protected_component_identity_basis,calculated_segments_sha256}'
          is distinct from encode(private.weekly_source_sha256_jsonb_v1(
            'WEEKLY_PROTECTED_COMPONENT_CALCULATED_SEGMENTS_V1',v_snapshot#>'{invoice_breakdown_json,segments}'),'hex')
       or v_generation.complete_next_vector_json#>>'{protected_component_identity_basis,approved_components_sha256}'
          is distinct from encode(private.weekly_source_sha256_jsonb_v1(
            'WEEKLY_PROTECTED_COMPONENT_APPROVED_COMPONENTS_V1',
            case when v_generation.complete_next_vector_json ? 'prior_effective_inventory'
              then v_generation.complete_next_vector_json#>'{prior_effective_inventory,components}'
              else '[]'::jsonb end),'hex') then
      raise exception 'WEEKLY_PROTECTED_COMPONENT_MANIFEST_UNAVAILABLE' using errcode='55000'; end if;
  end if;
  select count(*) into v_live_authorisations from public.weekly_source_root_authorisations authority
    where authority.root_timesheet_id=v_root.timesheet_id and authority.withdrawn_at_utc is null;
  -- Exact accepted replay has already returned. Before admitting a NEW
  -- authorised decision, hold the existing owner share lock and require the
  -- same actual NEXT Local writer capabilities as the winning common core.
  -- Otherwise an old freeze could park a decision that cannot later publish.
  -- First Save remains the unauthorised TSFIN path and needs no Bank owner.
  if v_live_authorisations=1 and v_root.authorised_at_server is not null then
    if pg_catalog.to_regclass('private.bpay_next_module_control') is null
       or pg_catalog.to_regprocedure('private.bpay_next_legacy_callback_enabled_v1()') is null then
      raise exception 'WEEKLY_PROTECTED_LOCAL_PUBLICATION_OWNER_NOT_READY' using errcode='55000';
    end if;
    perform private.bpay_next_legacy_callback_enabled_v1();
    if (select active_owner from private.bpay_next_module_control where id=1) is distinct from 'NEXT'
       or pg_catalog.to_regprocedure(
         'private.weekly_source_local_publication_context_v2(uuid,uuid,uuid)') is null
       or pg_catalog.to_regprocedure(
         'private.weekly_source_entitlement_proposal_request_v2(uuid,jsonb,text,uuid,bigint,uuid,uuid,jsonb,text)') is null
       or pg_catalog.to_regprocedure(
         'private.bpay_next_capture_local_source_detail_v1(uuid,uuid,bigint,uuid,uuid,uuid,jsonb)') is null then
      raise exception 'WEEKLY_PROTECTED_LOCAL_PUBLICATION_OWNER_NOT_READY' using errcode='55000';
    end if;
  end if;
  insert into private.weekly_source_local_protected_decision_receipts(
    publication_request_id,generation_id,family_id,root_timesheet_id,actor_user_id,
    request_sha256,idempotency_key,preparing_transaction_id,prior_financial_id,approved_snapshot_json,state
  ) values(v_request.id,v_generation.id,v_family.id,v_root.timesheet_id,v_actor,
    v_hash,v_key,pg_current_xact_id(),v_fin.id,v_snapshot,'PREPARING');
  if v_live_authorisations=0 and v_root.authorised_at_server is null then
    -- 24 section 4.1: mutable unauthorised TSFIN, not a fabricated Authorise.
    if v_fin.authorised_at_utc is not null or v_fin.processing_status<>'PENDING_AUTH'
       or v_snapshot->>'processing_status' is distinct from 'PENDING_AUTH'
       or nullif(v_snapshot->>'authorised_at_utc','') is not null then
      raise exception 'WEEKLY_PROTECTED_LOCAL_AUTHORISATION_CONTRADICTION' using errcode='55000';
    end if;
    v_write:=public.tsfin_write_current_snapshot_single_bounded(v_root.timesheet_id,
      v_root.version,v_snapshot,v_actor,statement_timestamp());
    if coalesce((v_write->>'ok')::boolean,false) is not true then
      raise exception 'WEEKLY_PROTECTED_LOCAL_SNAPSHOT_WRITE_REFUSED' using errcode='55000';
    end if;
    -- V4 permits display of this confirmed decision before first Authorise,
    -- but never admission to Banking. Seal the actual written row, not the
    -- proposed input or an hours-only total. Old receipts are not backfilled.
    select * into strict v_fin from public.timesheets_financials financial
      where financial.id=(v_write->>'timesheet_financials_id')::uuid
        and financial.timesheet_id=v_root.timesheet_id and financial.is_current;
    if v_fin.authorised_at_utc is not null or v_fin.processing_status<>'PENDING_AUTH'
       or v_fin.paid_at_utc is not null or v_fin.locked_by_invoice_id is not null then
      raise exception 'WEEKLY_PROTECTED_LOCAL_SAVED_BASIS_INVALID' using errcode='55000';
    end if;
    update private.weekly_source_local_protected_decision_receipts
      set approved_snapshot_json=v_snapshot||jsonb_build_object('saved_unauthorised_basis',
        jsonb_build_object('schema_version','SAVED_UNAUTHORISED_LOCAL_V1',
          'financial_snapshot_id',v_fin.id,
          'financial_snapshot_sha256',encode(private.weekly_source_sha256_jsonb_v1(
            'WEEKLY_SOURCE_SAVED_UNAUTHORISED_FINANCIAL_V1',to_jsonb(v_fin)),'hex'),
          'detail_sha256',encode(private.weekly_source_sha256_jsonb_v1(
            'WEEKLY_SOURCE_SAVED_UNAUTHORISED_DETAIL_V1',jsonb_build_object(
              'segments',v_fin.invoice_breakdown_json->'segments',
              'actual_schedule',v_fin.actual_schedule_json,
              'additional_units',v_fin.additional_units_json)),'hex'),
          'inventory_sha256',encode(v_generation.complete_next_vector_hash,'hex'),
          'root_version',v_root.version,'family_booking_id',v_root.booking_id))
      where publication_request_id=v_request.id and state='PREPARING'
        and preparing_transaction_id=pg_current_xact_id();
    if not found then
      raise exception 'WEEKLY_PROTECTED_LOCAL_SAVED_BASIS_INVALID' using errcode='55000';
    end if;
    v_result:=jsonb_build_object('authority','UNAUTHORISED_TSFIN',
      'timesheet_financials_id',v_write->'timesheet_financials_id','requires_first_authorisation',true);
  elsif v_live_authorisations=1 and v_root.authorised_at_server is not null then
    -- Already authorised: the only later entitlement writer is the existing
    -- common-head publisher. Never rotate TSFIN, change Drafts or invoke C1.
    -- The real stage advances its own family bound version after sealing the
    -- old entitlement. The factual context proves that exact stage and the
    -- unchanged whole inventory, rather than treating the own increment as
    -- an unrelated intervening financial change.
    v_local_context:=private.weekly_source_local_publication_context_v2(v_request.id,v_generation.id,v_root.timesheet_id);
    if v_local_context is null then
      raise exception 'WEEKLY_PROTECTED_LOCAL_SOURCE_QUALIFICATION_UNAVAILABLE' using errcode='55000'; end if;
    select coalesce(jsonb_agg(jsonb_build_object('work_event_id',expense.work_event_id,
      'source_observation_kind',expense.source_observation_kind,
      'candidate_reimbursement_ex_vat',expense.candidate_reimbursement_ex_vat,
      'client_charge_ex_vat',expense.client_charge_ex_vat) order by expense.work_event_id),'[]'::jsonb)
      into v_expenses from (
        select authority.*,row_number() over(partition by authority.work_event_id
          order by cycle.finalisation_week_ending desc,revision.finalised_at_utc desc,
            authority.generation desc,authority.id desc) event_rank
        from public.weekly_expense_authority_generations authority
        join public.weekly_source_final_revisions revision on revision.id=authority.final_revision_id and revision.state='CURRENT'
        join public.weekly_source_cycles cycle on cycle.id=revision.source_cycle_id
        join public.weekly_source_cycles action_cycle on action_cycle.id=v_approval.source_cycle_id
        where authority.contract_id=v_family.contract_id and authority.state='CURRENT'
          and cycle.source_group_id=action_cycle.source_group_id
          and exists(select 1 from public.weekly_source_row_timesheet_lineages lineage
            where lineage.work_event_id=authority.work_event_id and lineage.contract_id=authority.contract_id
              and lineage.timesheet_id=any(private.weekly_source_invoice_family_timesheet_ids_v1(v_root.timesheet_id)))
      ) expense where expense.event_rank=1 and expense.source_expense_pence>0;
    v_components:=private.weekly_source_entitlement_components_v1(
      v_snapshot#>'{invoice_breakdown_json,segments}',v_expenses);
    -- Reuse the existing pure approved-additional mapper on the sealed
    -- calculator snapshot. It validates exact saved units/rates/totals and
    -- booking-family identity; it does not read or reprice live financial data.
    v_components:=private.weekly_source_initial_additional_components_v2(v_components,
      v_snapshot->'additional_units_json',
      (v_snapshot->>'additional_pay_ex_vat')::numeric,
      (v_snapshot->>'additional_charge_ex_vat')::numeric,v_root.booking_id);
    -- Source expenses must be the same approved observations and money, not
    -- a newer expense silently composed into an older protected decision.
    if (select coalesce(sum((item.value->>'pay_ex_vat')::numeric)
        filter(where not (item.value->>'exclude_from_pay')::boolean),0)
        from jsonb_array_elements(v_components) item(value)) is distinct from v_approval.approved_target_gross
       or (select coalesce(jsonb_agg(jsonb_build_object('code',upper('SOURCE_SUPPLIED:'||(item.value->>'work_event_id')),
          'pay',(item.value->>'candidate_reimbursement_ex_vat')::numeric,
          'charge',(item.value->>'client_charge_ex_vat')::numeric) order by item.value->>'work_event_id'),'[]'::jsonb)
          from jsonb_array_elements(v_expenses) item(value)) is distinct from
          (select coalesce(jsonb_agg(jsonb_build_object('code',upper(component.expense_code),
            'pay',component.pay_ex_vat,'charge',component.charge_ex_vat)
            order by component.expense_code),'[]'::jsonb)
            from public.weekly_exceptional_c1_component_records component
            where component.publication_request_id=v_request.id and component.component_kind='EXPENSE') then
      raise exception 'WEEKLY_PROTECTED_LOCAL_COMPLETE_ENTITLEMENT_CHANGED' using errcode='40001';
    end if;
    v_local_origin:=private.weekly_source_local_origin_canonical_v2(jsonb_build_object(
      'origin_kind','PROTECTED_LOCAL_DECISION_V1','publication_request_id',v_request.id,
      'generation_id',v_generation.id,'request_sha256',encode(v_request.request_sha256,'hex'),
      'source_qualification_digest',v_local_context->'source_qualification_digest',
      'policy_fingerprint',v_local_context->'policy_fingerprint','before_origin',v_local_context->'before_origin',
      'before_inventory_digest',v_local_context->'before_inventory_digest'));
    v_bundle_id:=pg_catalog.gen_random_uuid();
    update private.weekly_source_local_protected_decision_receipts set approved_snapshot_json=v_snapshot||jsonb_build_object(
      'common_decision_bundle_id',v_bundle_id,
      'local_source_qualification',v_local_context->'qualification','local_publication_origin',v_local_origin,
      'common_components_digest',encode(private.weekly_source_publication_request_digest_v1(
        (select coalesce(jsonb_agg(private.weekly_source_publication_component_canonical_v1(item.value,'protected.component')
          order by item.ordinality),'[]'::jsonb) from jsonb_array_elements(v_components) with ordinality item(value,ordinality))),'hex')),
      common_decision_bundle_id=v_bundle_id,common_bundle_revision=1,
      publication_origin_kind='PROTECTED_LOCAL_DECISION_V1',
      publication_origin_digest=private.weekly_source_publication_request_digest_v1(v_local_origin),
      source_qualification_digest=decode(v_local_context->>'source_qualification_digest','hex')
      where publication_request_id=v_request.id and state='PREPARING' and preparing_transaction_id=pg_current_xact_id();
    if not found then raise exception 'WEEKLY_PROTECTED_LOCAL_ORIGIN_SEAL_REFUSED' using errcode='55000'; end if;
    v_head_request:=private.weekly_source_entitlement_proposal_request_v2(v_root.timesheet_id,
      v_local_origin,'PROTECTED',v_bundle_id,1,pg_catalog.gen_random_uuid(),
      pg_catalog.gen_random_uuid(),v_components);
    v_proposal:=private.weekly_source_entitlement_proposal_record_v1(v_head_request,v_family.agency_id,
      v_family.contract_id,v_family.week_ending_date,v_actor);
    if coalesce((v_proposal->>'ok')::boolean,false) is not true then
      raise exception 'WEEKLY_PROTECTED_LOCAL_PROPOSAL_REFUSED' using errcode='55000';
    end if;
    v_write:=private.weekly_source_entitlement_publish_immediate_v1(v_head_request);
    if coalesce((v_write->>'ok')::boolean,false) is not true then
      raise exception '%',coalesce(v_write->>'code','WEEKLY_PROTECTED_LOCAL_HEAD_REFUSED') using errcode='55000';
    end if;
    v_pending:=coalesce((v_write->>'published')::boolean,false) is not true;
    if v_pending and v_write->>'code'<>'WEEKLY_SOURCE_PUBLICATION_DEFERRED_PENDING_FREEZE' then
      raise exception 'WEEKLY_PROTECTED_LOCAL_HEAD_RESULT_INVALID' using errcode='55000';
    end if;
    v_result:=jsonb_build_object('authority','COMMON_ENTITLEMENT_HEAD',
      'decision_bundle_id',v_bundle_id,'publication',v_write,'requires_first_authorisation',false);
  else
    raise exception 'WEEKLY_PROTECTED_LOCAL_AUTHORISATION_CONTRADICTION' using errcode='55000';
  end if;

  v_result:=v_result||jsonb_build_object('ok',true,'outcome',case when v_pending
    then 'SAVED_PENDING_FREEZE' else 'PUBLISHED' end,'idempotent_replay',false,
    'family_id',v_family.id,'generation_id',v_generation.id,'publication_request_id',v_request.id,
    'state',case when v_pending then 'PENDING' else 'LIVE' end,
    'family_bound_version',v_family.bound_version+1);
  v_result_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_LOCAL_RESULT_V1',v_result);
  if not v_pending then
    if v_family.current_generation_id is not null then
      update public.weekly_exceptional_pay_generations set lifecycle_state='SUPERSEDED',
        superseded_by_generation_id=v_generation.id,superseded_at_utc=statement_timestamp()
        where id=v_family.current_generation_id and lifecycle_state='PUBLISHED';
      if not found then raise exception 'WEEKLY_PROTECTED_LOCAL_PRIOR_GENERATION_INVALID' using errcode='55000'; end if;
    end if;
    update public.weekly_exceptional_pay_generations set lifecycle_state='PUBLISHED',
      published_at_utc=statement_timestamp(),result_hash=v_result_hash where id=v_generation.id;
    update public.weekly_exceptional_pay_target_families set current_generation_id=v_generation.id,
      current_generation_number=v_generation.generation_number,
      current_complete_target_vector_hash=v_generation.complete_next_vector_hash,
      current_source_proposal_hash=v_source_hash,current_lifecycle_state=v_target_state,
      current_component_count=v_request.expected_component_count,c1_publication_state='NONE',
      bound_version=bound_version+1 where id=v_family.id;
  else
    -- Previous effective family/head remains current while the existing frozen
    -- bundle owner holds the newly approved decision. No pretend live receipt.
    update public.weekly_exceptional_pay_target_families set c1_publication_state='NONE',
      bound_version=bound_version+1 where id=v_family.id;
  end if;
  update public.weekly_exceptional_c1_publication_requests set state='RETIRED',
    typed_result_json=v_result,completed_at_utc=statement_timestamp() where id=v_request.id;
  update public.weekly_exceptional_orchestration_runs set state='COMPLETE',
    after_state_fingerprint=v_result_hash,completed_at_utc=statement_timestamp() where id=v_run.id;
  update private.weekly_source_local_protected_decision_receipts set state=case when v_pending
    then 'PENDING_FREEZE' else 'COMPLETE' end,result_json=v_result,completed_at_utc=statement_timestamp()
    where publication_request_id=v_request.id;
  -- Finish the existing manual pay query atomically with its approved decision.
  -- The UI needs no second query command: only this accepted publication can
  -- clear the hold, using its exact retained approval and generation evidence.
  select review.id into v_manual_review_id from private.weekly_source_manual_reviews review
    join public.weekly_source_cycles cycle on cycle.id=v_approval.source_cycle_id
    where review.source_group_id=cycle.source_group_id
      and review.work_event_id=v_approval.work_event_id and review.state='OPEN'
      and review.contract_id=v_family.contract_id and review.candidate_id=v_family.candidate_id;
  insert into public.weekly_exceptional_payment_events(family_id,approval_id,event_kind,
    lifecycle_view,bounded_payload_json,idempotency_key) values(v_family.id,v_approval.id,
    'PROTECTED_STATE_OBSERVED',case when v_pending then 'PENDING' else 'LIVE' end,v_result,v_key||':payment-event');
  insert into public.weekly_exceptional_orchestration_steps(orchestration_run_id,sequence,
    step_kind,idempotency_key,allowlisted_owner_name,allowlisted_owner_signature,bounded_request_hash,
    before_state_fingerprint,bounded_owner_response_json,owner_response_hash,after_state_fingerprint,outcome,completed_at_utc)
    select v_run.id,coalesce(max(step.sequence),0)+1,'COMPLETE_LOCAL_PROTECTED_DECISION',v_key,
      'CloudTMS.weekly_exceptional_pay_complete_local_v1','jsonb',v_hash,v_run.before_state_fingerprint,
      v_result,v_result_hash,v_result_hash,case when v_pending then 'PENDING' else 'COMPLETE' end,statement_timestamp()
    from public.weekly_exceptional_orchestration_steps step where step.orchestration_run_id=v_run.id;
  -- Finish the actual completion evidence before any query resolution or
  -- accepted-status read, in this same outer transaction; a refusal rolls back
  -- the decision, publication, completion step and query together.
  if v_manual_review_id is not null and v_run.request_kind in ('APPROVE','AMEND') then
    perform public.weekly_source_manual_review_resolve_v1(jsonb_build_object('actor_user_id',v_actor,
      'review_id',v_manual_review_id,'resolution_kind','PROTECTED_PAY',
      'command_id',v_request.id,'accepted_approval_id',v_approval.id,
      'accepted_generation_id',v_generation.id));
  elsif v_manual_review_id is not null and v_run.request_kind in ('WITHDRAW','RECONCILE') then
    perform public.weekly_source_manual_review_resolve_v1(jsonb_build_object('actor_user_id',v_actor,
      'review_id',v_manual_review_id,'resolution_kind','OFFICE_ACCEPTED_SOURCE',
      'command_id',v_request.id,'accepted_approval_id',v_approval.id,
      'accepted_generation_id',v_generation.id));
  end if;
  -- Accepted pending approval is not publication. The existing release
  -- trigger retires its contacts only after the deferred receipt is proved.
  if not v_pending then
    perform private.weekly_source_protected_contact_retire_v1(v_family.id,v_approval.work_event_id);
  end if;
  return v_result;
exception when no_data_found or too_many_rows then
  raise exception 'WEEKLY_PROTECTED_LOCAL_COMPLETE_SCOPE_INVALID' using errcode='55000';
end;
$function$;

-- Bookkeeping hook on the EXISTING pending-bundle release. It neither releases
-- a bundle nor publishes a head itself. Only a positively proved common-head
-- receipt permits the approved generation to become current after a freeze.
create or replace function private.weekly_source_local_protected_pending_released_v1()
returns trigger language plpgsql security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_local private.weekly_source_local_protected_decision_receipts%rowtype;
  v_generation public.weekly_exceptional_pay_generations%rowtype;
  v_family public.weekly_exceptional_pay_target_families%rowtype;
  v_work_event_id uuid;
  v_target_state text;
  v_source_hash bytea;
  v_result jsonb;
  v_hash bytea;
begin
  if new.state<>'RELEASED' or old.state='RELEASED' then return new; end if;
  select * into v_local from private.weekly_source_local_protected_decision_receipts local_receipt
    where local_receipt.state='PENDING_FREEZE'
      and local_receipt.result_json->>'decision_bundle_id'=new.decision_bundle_id::text for update;
  if not found then return new; end if;
  if not exists(select 1 from private.weekly_source_entitlement_publication_receipts receipt
    where receipt.id=new.released_receipt_id and receipt.decision_bundle_id=new.decision_bundle_id
      and receipt.bundle_revision=new.bundle_revision
      and receipt.request_digest=new.released_receipt_digest
      and v_local.root_timesheet_id=any(receipt.member_root_ids)) then
    raise exception 'WEEKLY_PROTECTED_LOCAL_DEFERRED_RECEIPT_INVALID' using errcode='55000';
  end if;
  select * into strict v_family from public.weekly_exceptional_pay_target_families
    where id=v_local.family_id for update;
  select * into strict v_generation from public.weekly_exceptional_pay_generations
    where id=v_local.generation_id and family_id=v_family.id for update;
  if v_family.current_generation_id is distinct from v_generation.prior_generation_id
     or v_generation.lifecycle_state<>'PENDING_C1' then
    raise exception 'WEEKLY_PROTECTED_LOCAL_DEFERRED_GENERATION_CHANGED' using errcode='55000';
  end if;
  select target.resulting_lifecycle_state,event.source_proposal_hash,event.durable_work_event_id
    into strict v_target_state,v_source_hash,v_work_event_id
    from public.weekly_exceptional_pay_target_events target
    join public.weekly_exceptional_pay_family_events event on event.evidence_approval_id=target.approval_id
    where target.financial_generation_id=v_generation.id and target.family_id=v_family.id;
  v_result:=v_local.result_json||jsonb_build_object('outcome','PUBLISHED','state','LIVE',
    'deferred_release_receipt_id',new.released_receipt_id,
    'publication',jsonb_build_object('ok',true,'published',true,'replayed',false,
      'receipt',private.weekly_source_publication_receipt_json_v1(new.released_receipt_id)),
    'family_bound_version',v_family.bound_version+1);
  v_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_LOCAL_RESULT_V1',v_result);
  if v_family.current_generation_id is not null then
    update public.weekly_exceptional_pay_generations set lifecycle_state='SUPERSEDED',
      superseded_by_generation_id=v_generation.id,superseded_at_utc=statement_timestamp()
      where id=v_family.current_generation_id and lifecycle_state='PUBLISHED';
    if not found then raise exception 'WEEKLY_PROTECTED_LOCAL_PRIOR_GENERATION_INVALID' using errcode='55000'; end if;
  end if;
  update public.weekly_exceptional_pay_generations set lifecycle_state='PUBLISHED',
    published_at_utc=statement_timestamp(),result_hash=v_hash where id=v_generation.id;
  update public.weekly_exceptional_pay_target_families set current_generation_id=v_generation.id,
    current_generation_number=v_generation.generation_number,
    current_complete_target_vector_hash=v_generation.complete_next_vector_hash,
    current_source_proposal_hash=v_source_hash,current_lifecycle_state=v_target_state,
    current_component_count=(v_generation.complete_next_vector_json->>'component_count')::integer,
    c1_publication_state='NONE',bound_version=bound_version+1 where id=v_family.id;
  update private.weekly_source_local_protected_decision_receipts set state='COMPLETE',result_json=v_result
    where publication_request_id=v_local.publication_request_id;
  -- The accepted run remains the SAME completed Office request. Keep its
  -- result fingerprint correlated with the newly released result, without
  -- changing its actor/request/identity or first completion time. The original
  -- pending result remains in its existing completion step as audit evidence.
  -- Never backfill a missing/conflicting run or fabricate a successful release.
  update public.weekly_exceptional_orchestration_runs run
    set after_state_fingerprint=v_hash
    from public.weekly_exceptional_c1_publication_requests request
    where request.id=v_local.publication_request_id and request.state='RETIRED'
      and request.family_id=v_local.family_id and request.generation_id=v_generation.id
      and request.request_sha256=v_local.request_sha256
      and request.typed_result_json=v_local.result_json
      and run.id=request.orchestration_run_id and run.family_id=v_local.family_id
      and run.requested_by_user_id=v_local.actor_user_id and run.state='COMPLETE'
      and run.completed_at_utc is not null
      and run.after_state_fingerprint=private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_PROTECTED_LOCAL_RESULT_V1',v_local.result_json)
      and exists(select 1 from public.weekly_exceptional_orchestration_steps step
        where step.orchestration_run_id=run.id
          and step.step_kind='COMPLETE_LOCAL_PROTECTED_DECISION' and step.outcome='PENDING'
          and step.idempotency_key=v_local.idempotency_key
          and step.bounded_owner_response_json=v_local.result_json
          and step.owner_response_hash=run.after_state_fingerprint
          and step.after_state_fingerprint=run.after_state_fingerprint);
  if not found then
    raise exception 'WEEKLY_PROTECTED_LOCAL_DEFERRED_RUN_INVALID' using errcode='55000';
  end if;
  update public.weekly_exceptional_c1_publication_requests set typed_result_json=v_result
    where id=v_local.publication_request_id and state='RETIRED';
  perform private.weekly_source_protected_contact_retire_v1(v_family.id,v_work_event_id);
  return new;
end;
$function$;
alter function private.weekly_source_local_protected_pending_released_v1() owner to postgres;
revoke all on function private.weekly_source_local_protected_pending_released_v1()
  from public,anon,authenticated,service_role;
drop trigger if exists weekly_source_local_protected_pending_released on public.weekly_source_pending_entitlement_bundles;
create trigger weekly_source_local_protected_pending_released after update of state
  on public.weekly_source_pending_entitlement_bundles for each row
  execute function private.weekly_source_local_protected_pending_released_v1();

alter function private.weekly_source_local_preauthorisation_write_allowed_v1(uuid,jsonb) owner to postgres;
alter function public.weekly_exceptional_pay_complete_local_v1(jsonb) owner to postgres;
revoke all on function private.weekly_source_local_preauthorisation_write_allowed_v1(uuid,jsonb)
  from public,anon,authenticated,service_role;
-- E7 is SECURITY INVOKER. This helper reveals only a boolean, never grants a
-- write capability to callers or exposes its sealed snapshot/audit rows.
grant execute on function private.weekly_source_local_preauthorisation_write_allowed_v1(uuid,jsonb) to service_role;
revoke all on function public.weekly_exceptional_pay_complete_local_v1(jsonb)
  from public,anon,authenticated,service_role;
grant execute on function public.weekly_exceptional_pay_complete_local_v1(jsonb) to service_role;
notify pgrst, 'reload schema';

commit;
