-- Immutable Local Save status; distinct from the latest-WAIT query receipt.
-- This reader does not authorise, publish, price, close a query or stage Banking Pay.
\set ON_ERROR_STOP on
begin;

create or replace function private.weekly_source_local_saved_status_v1(
  p_family_id uuid,p_orchestration_run_id uuid,p_actor_user_id uuid
) returns jsonb
language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_scope jsonb;
  v_approval public.weekly_exceptional_payment_approvals%rowtype;
  v_generation public.weekly_exceptional_pay_generations%rowtype;
  v_local private.weekly_source_local_protected_decision_receipts%rowtype;
  v_request public.weekly_exceptional_c1_publication_requests%rowtype;
  v_run public.weekly_exceptional_orchestration_runs%rowtype;
  v_event public.weekly_exceptional_pay_family_events%rowtype;
  v_original_event public.weekly_exceptional_pay_family_events%rowtype;
  v_publication private.weekly_source_entitlement_publication_receipts%rowtype;
  v_pending public.weekly_source_pending_entitlement_bundles%rowtype;
  v_head public.weekly_source_entitlement_heads%rowtype;
  v_canonical jsonb;
  v_choice jsonb;
  v_entitlement jsonb;
  v_components jsonb;
  v_origin jsonb;
  v_basis jsonb;
  v_snapshot jsonb;
  v_publication_root public.timesheets%rowtype;
  v_publication_version integer;
  v_prior_inventory jsonb;
  v_before_origin jsonb;
  v_qualification jsonb;
  v_result_hash bytea;
  v_member integer;
  v_count integer;
  v_cycle public.weekly_source_cycles%rowtype;
  v_step public.weekly_exceptional_orchestration_steps%rowtype;
  p_approval_id uuid;
  p_generation_id uuid;
begin
  if p_family_id is null or p_orchestration_run_id is null or p_actor_user_id is null then
    raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_IDENTITY_REQUIRED' using errcode='22023';
  end if;
  -- NULL means no Local evidence at all, never a malformed or partial receipt.
  -- Do not inspect current WAIT, financial authorisation or generation pointers.
  select count(*) into v_count from public.weekly_exceptional_c1_publication_requests r
    where r.family_id=p_family_id and r.orchestration_run_id=p_orchestration_run_id;
  if v_count=0 then
    if exists(select 1 from private.weekly_source_local_protected_decision_receipts l
      join public.weekly_exceptional_c1_publication_requests r on r.id=l.publication_request_id
      where r.orchestration_run_id=p_orchestration_run_id)
    then raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000'; end if;
    return null;
  end if;
  if v_count<>1 then raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000'; end if;
  select r.* into strict v_request from public.weekly_exceptional_c1_publication_requests r
    where r.family_id=p_family_id and r.orchestration_run_id=p_orchestration_run_id;
  select l.* into v_local from private.weekly_source_local_protected_decision_receipts l
    where l.publication_request_id=v_request.id;
  if not found then
    if v_request.state='RETIRED'
       or v_request.typed_result_json->>'authority' in ('UNAUTHORISED_TSFIN','COMMON_ENTITLEMENT_HEAD')
    then raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000'; end if;
    return null;
  end if;
  if v_local.family_id is distinct from p_family_id
     or v_local.actor_user_id is distinct from p_actor_user_id
     or v_local.generation_id is distinct from v_request.generation_id
     or v_request.orchestration_run_id is distinct from p_orchestration_run_id then
    raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000';
  end if;
  select count(*) into v_count from public.weekly_exceptional_payment_approvals a
    where a.creation_orchestration_run_id=p_orchestration_run_id and a.pay_target_family_id=p_family_id;
  if v_count<>1 then raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000'; end if;
  select a.id into strict p_approval_id from public.weekly_exceptional_payment_approvals a
    where a.creation_orchestration_run_id=p_orchestration_run_id and a.pay_target_family_id=p_family_id;
  p_generation_id:=v_local.generation_id;
  select jsonb_build_object('root_timesheet_id',t.timesheet_id,'root_version',
      v_local.approved_snapshot_json->'timesheet_version','family_booking_id',t.booking_id,
      'target_family_id',p_family_id,'candidate_id',a.candidate_id,'client_id',a.client_id,
      'contract_id',a.contract_id,'week_ending_date',to_char(a.week_ending,'YYYY-MM-DD'),
      'family_bound_version',v_local.result_json->'family_bound_version')
    into strict v_scope
    from public.timesheets t
    join public.weekly_exceptional_payment_approvals a on a.id=p_approval_id
    join public.contracts c on c.id=t.contract_id
    join public.weekly_exceptional_pay_target_families family on family.id=p_family_id
    where t.timesheet_id=v_local.root_timesheet_id
      and c.candidate_id=a.candidate_id and c.client_id=a.client_id
      and family.candidate_id=a.candidate_id and family.contract_id=a.contract_id
      and family.week_ending_date=a.week_ending
      and t.contract_id=v_request.contract_id and t.week_ending_date=v_request.week_ending_date;
  select a.* into v_approval from public.weekly_exceptional_payment_approvals a
    where a.id=p_approval_id;
  if not found or v_approval.pay_target_family_id::text is distinct from v_scope->>'target_family_id'
     or v_approval.candidate_id::text is distinct from v_scope->>'candidate_id'
     or v_approval.client_id::text is distinct from v_scope->>'client_id'
     or v_approval.contract_id::text is distinct from v_scope->>'contract_id'
     or to_char(v_approval.week_ending,'YYYY-MM-DD') is distinct from v_scope->>'week_ending_date'
     or v_approval.protected_work_date not between
        (v_scope->>'week_ending_date')::date-6 and (v_scope->>'week_ending_date')::date then raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000'; end if;
  if not exists(select 1 from public.weekly_work_events w
    where w.id=v_approval.work_event_id and w.candidate_id=v_approval.candidate_id
      and w.client_id=v_approval.client_id and w.work_date=v_approval.protected_work_date) then raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000'; end if;
  -- Original report cutoff is the Office permission boundary, not today's
  -- work week or the report most recently imported for another shift.
  select c.* into strict v_cycle from public.weekly_source_cycles c
    where c.id=v_approval.source_cycle_id;
  perform private.weekly_source_office_authority_v1(p_actor_user_id,'APPROVE_PROTECTED_PAY',
    v_cycle.source_group_id,v_approval.client_id,v_cycle.finalisation_week_ending);
  select g.* into v_generation from public.weekly_exceptional_pay_generations g
    where g.id=p_generation_id and g.family_id=v_approval.pay_target_family_id;
  if not found or private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_COMPLETE_VECTOR_V1',
       v_generation.complete_next_vector_json) is distinct from v_generation.complete_next_vector_hash
     or not exists(select 1 from public.weekly_exceptional_pay_target_events t
       where t.family_id=v_approval.pay_target_family_id and t.approval_id=v_approval.id
         and t.financial_generation_id=v_generation.id
         and t.complete_next_family_vector_fingerprint=v_generation.complete_next_vector_hash) then raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000'; end if;
  select l.* into v_local from private.weekly_source_local_protected_decision_receipts l
    where l.generation_id=v_generation.id;
  if not found or v_local.family_id is distinct from v_approval.pay_target_family_id
     or v_local.actor_user_id is distinct from v_approval.approved_by_user_id
     or v_local.state not in ('COMPLETE','PENDING_FREEZE')
     or v_local.completed_at_utc is null or not isfinite(v_local.completed_at_utc) then raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000'; end if;
  -- Accepted evidence belongs to its original physical root/version. A lawful
  -- later rotation must not invalidate the same raw booking family's receipt.
  -- Point-read that pinned root; never choose an arbitrary historical winner.
  select t.* into v_publication_root from public.timesheets t
    where t.timesheet_id=v_local.root_timesheet_id;
  if not found or v_publication_root.booking_id is distinct from v_scope->>'family_booking_id'
     or v_publication_root.contract_id is distinct from v_approval.contract_id
     or v_publication_root.week_ending_date is distinct from v_approval.week_ending
     or v_publication_root.sheet_scope is distinct from 'WEEKLY'
     or v_publication_root.line_type is distinct from 'HOURS'
     or v_publication_root.is_adjustment is distinct from false
     or v_local.approved_snapshot_json->>'timesheet_id' is distinct from v_local.root_timesheet_id::text
     or jsonb_typeof(v_local.approved_snapshot_json->'timesheet_version') is distinct from 'number'
     or coalesce(v_local.approved_snapshot_json->>'timesheet_version','') !~ '^[1-9][0-9]*$'
     or (v_local.approved_snapshot_json->>'timesheet_version')::numeric>2147483647 then raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000'; end if;
  v_publication_version:=(v_local.approved_snapshot_json->>'timesheet_version')::integer;
  select r.* into v_request from public.weekly_exceptional_c1_publication_requests r
    where r.id=v_local.publication_request_id;
  if not found or v_request.state is distinct from 'RETIRED'
     or v_request.completed_at_utc is null or not isfinite(v_request.completed_at_utc)
     or v_request.family_id is distinct from v_local.family_id
     or v_request.generation_id is distinct from v_generation.id
     or v_request.orchestration_run_id is distinct from v_approval.creation_orchestration_run_id
     or v_request.root_timesheet_id is distinct from v_local.root_timesheet_id
     or v_request.candidate_id is distinct from v_approval.candidate_id
     or v_request.contract_id is distinct from v_approval.contract_id
     or v_request.week_ending_date is distinct from v_approval.week_ending
     or v_request.request_sha256 is distinct from v_local.request_sha256
     or v_request.typed_result_json is distinct from v_local.result_json
     or v_request.c1_operation_id is not null or v_request.c1_publication_id is not null
     or v_request.c1_head_revision is not null or v_request.c1_receipt_sha256 is not null then raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000'; end if;
  select r.* into v_run from public.weekly_exceptional_orchestration_runs r
    where r.id=v_approval.creation_orchestration_run_id;
  v_result_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_LOCAL_RESULT_V1',v_local.result_json);
  if not found or v_run.family_id is distinct from v_local.family_id
     or v_run.requested_by_user_id is distinct from v_local.actor_user_id
     or v_run.state is distinct from 'COMPLETE' or v_run.completed_at_utc is null
     or not isfinite(v_run.completed_at_utc)
     or v_run.after_state_fingerprint is distinct from v_result_hash
     or v_local.result_json->'ok' is distinct from 'true'::jsonb
     or v_local.result_json->'idempotent_replay' is distinct from 'false'::jsonb
     or v_local.result_json->>'family_id' is distinct from v_local.family_id::text
     or v_local.result_json->>'generation_id' is distinct from v_generation.id::text
     or v_local.result_json->>'publication_request_id' is distinct from v_request.id::text
     or coalesce(v_local.result_json->>'family_bound_version','') !~ '^[1-9][0-9]*$'
     or (v_local.result_json->>'family_bound_version')::numeric>
          (v_scope->>'family_bound_version')::numeric then raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000'; end if;
  v_snapshot:=v_generation.complete_next_vector_json#>'{target_snapshot,tsfin_snapshot_json}';
  if jsonb_typeof(v_snapshot) is distinct from 'object'
     or v_snapshot is distinct from v_approval.approved_target_pay_components_json->'tsfin_snapshot_json' then raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000'; end if;
  if v_local.state='COMPLETE' then
    -- A different sibling may have superseded this accepted generation. Its
    -- immutable published proof is still needed for this unchanged WAIT event.
    if v_generation.lifecycle_state not in ('PUBLISHED','SUPERSEDED')
       or v_generation.published_at_utc is null or not isfinite(v_generation.published_at_utc)
       or v_generation.result_hash is distinct from v_result_hash
       or v_local.result_json->>'outcome' is distinct from 'PUBLISHED'
       or v_local.result_json->>'state' is distinct from 'LIVE' then raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000'; end if;
  elsif v_generation.lifecycle_state not in ('PENDING_C1','SUPERSEDED')
     or v_generation.published_at_utc is not null or v_generation.result_hash is not null
     or v_local.result_json->>'outcome' is distinct from 'SAVED_PENDING_FREEZE'
     or v_local.result_json->>'state' is distinct from 'PENDING' then raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000';
  end if;
  if v_local.result_json->>'authority'='UNAUTHORISED_TSFIN' then
    if v_local.state<>'COMPLETE'
       or v_local.result_json->'requires_first_authorisation' is distinct from 'true'::jsonb then raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000'; end if;
    v_basis:=v_local.approved_snapshot_json->'saved_unauthorised_basis';
    if private.weekly_exceptional_json_keys_exact_v1(v_basis,array['schema_version',
       'financial_snapshot_id','financial_snapshot_sha256','detail_sha256','inventory_sha256',
       'root_version','family_booking_id']) is distinct from true
       or not (v_basis ?& array['schema_version','financial_snapshot_id','financial_snapshot_sha256',
          'detail_sha256','inventory_sha256','root_version','family_booking_id'])
       or v_basis->>'schema_version' is distinct from 'SAVED_UNAUTHORISED_LOCAL_V1'
       or v_basis->>'family_booking_id' is distinct from v_scope->>'family_booking_id'
       or v_basis->>'root_version' is distinct from v_publication_version::text
       or v_basis->>'financial_snapshot_id' is distinct from v_local.result_json->>'timesheet_financials_id'
       or coalesce(v_basis->>'financial_snapshot_sha256','') !~ '^[0-9a-f]{64}$'
       or coalesce(v_basis->>'detail_sha256','') !~ '^[0-9a-f]{64}$'
       or v_basis->>'inventory_sha256' is distinct from encode(v_generation.complete_next_vector_hash,'hex')
       or (v_local.approved_snapshot_json-'saved_unauthorised_basis') is distinct from
          (v_snapshot||jsonb_build_object('timesheet_id',v_local.root_timesheet_id::text,
           'timesheet_version',v_publication_version,'actual_schedule_json',
           v_generation.complete_next_vector_json#>'{target_snapshot,actual_schedule_json}'))
       or not exists(select 1 from public.timesheets_financials f
         where f.id::text=v_basis->>'financial_snapshot_id'
           and f.timesheet_id=v_local.root_timesheet_id
           and f.timesheet_version=v_publication_version
           and f.candidate_id=v_approval.candidate_id and f.client_id=v_approval.client_id) then raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000'; end if;
    -- This is accepted decision evidence, not current financial eligibility.
    -- First Authorise can legitimately change current TSFIN. Do not compare
    -- today's mutable finance row to the original sealed pre-authorise hash.
  elsif v_local.result_json->>'authority'='COMMON_ENTITLEMENT_HEAD' then
    if v_local.result_json->'requires_first_authorisation' is distinct from 'false'::jsonb
       or v_local.approved_snapshot_json->>'common_decision_bundle_id'
          is distinct from v_local.result_json->>'decision_bundle_id'
       or v_local.common_decision_bundle_id::text is distinct from v_local.result_json->>'decision_bundle_id'
       or v_local.common_bundle_revision is null
       or v_local.publication_origin_kind is distinct from 'PROTECTED_LOCAL_DECISION_V1'
       or v_local.publication_origin_digest is null
       or v_local.source_qualification_digest is null then raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000'; end if;
    if v_local.state='PENDING_FREEZE' then
      select p.* into v_pending from public.weekly_source_pending_entitlement_bundles p
        where p.id::text=v_local.result_json#>>'{publication,pending_bundle_verified}';
      if not found or v_pending.state not in ('PENDING','RELEASING','MANUAL_REVIEW','SUPERSEDED')
         or v_pending.decision_bundle_id::text is distinct from v_local.result_json->>'decision_bundle_id'
         or v_pending.bundle_revision is distinct from v_local.common_bundle_revision
         or v_pending.decided_by_user_id is distinct from v_local.actor_user_id
         or v_pending.candidate_id is distinct from v_approval.candidate_id
         or v_local.result_json#>'{publication,ok}' is distinct from 'true'::jsonb
         or v_local.result_json#>'{publication,published}' is distinct from 'false'::jsonb
         or v_local.result_json#>>'{publication,code}' is distinct from 'WEEKLY_SOURCE_PUBLICATION_DEFERRED_PENDING_FREEZE'
         or v_pending.request_digest is distinct from private.weekly_source_publication_request_digest_v1(
             private.weekly_source_publication_request_canonical_v1(v_pending.request_json,'DEFERRED',v_pending.id)) then raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000'; end if;
      v_member:=array_position(v_pending.member_root_ids,v_local.root_timesheet_id);
      if v_member is null or v_pending.member_family_booking_ids[v_member] is distinct from v_scope->>'family_booking_id'
         or v_pending.member_root_versions[v_member] is distinct from v_publication_version then raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000'; end if;
      v_canonical:=private.weekly_source_publication_request_canonical_v1(v_pending.request_json,'DEFERRED',v_pending.id);
      -- A self-consistent request digest does not prove that its identities
      -- agree with the separately stored pending row. Bind the complete member
      -- mapping before using the pending row's ordinal to select an entitlement.
      if v_canonical->>'decision_bundle_id' is distinct from v_pending.decision_bundle_id::text
         or (v_canonical->>'bundle_revision')::bigint is distinct from v_pending.bundle_revision
         or v_canonical->>'candidate_id' is distinct from v_pending.candidate_id::text
         or v_canonical->'member_root_ids' is distinct from to_jsonb(v_pending.member_root_ids)
         or v_canonical->'member_family_booking_ids' is distinct from to_jsonb(v_pending.member_family_booking_ids)
         or v_canonical->'member_root_versions' is distinct from to_jsonb(v_pending.member_root_versions)
         or v_canonical->'head_ids' is distinct from to_jsonb(v_pending.proposed_head_ids)
         or v_canonical->>'decision_id' is distinct from v_pending.decision_id::text
         or v_canonical->'member_root_ids'->>(v_member-1) is distinct from v_local.root_timesheet_id::text
         or v_canonical->'member_family_booking_ids'->>(v_member-1) is distinct from v_scope->>'family_booking_id'
         or (v_canonical->'member_root_versions'->>(v_member-1))::integer
            is distinct from v_publication_version then raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000'; end if;
      select c.value into strict v_choice from jsonb_array_elements(
        v_canonical#>'{financial_request,contract_choices}') c(value)
        where (c.value->>'root_ordinal')::integer=v_member;
      if v_choice->>'contract_id' is distinct from v_scope->>'contract_id'
         or v_choice->>'week_ending_date' is distinct from v_scope->>'week_ending_date' then raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000'; end if;
      select e.value into strict v_entitlement from jsonb_array_elements(
        v_canonical#>'{financial_request,member_entitlements}') e(value)
        where (e.value->>'root_ordinal')::integer=v_member;
      if v_entitlement->>'authority_kind' is distinct from 'PROTECTED'
         or (v_entitlement->>'component_count')::integer is distinct from jsonb_array_length(v_entitlement->'components') then raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000'; end if;
      v_components:=v_entitlement->'components';
      v_origin:=v_canonical#>'{financial_request,source_revision}';
    else
      select r.* into v_publication from private.weekly_source_entitlement_publication_receipts r
        where r.id::text=v_local.result_json#>>'{publication,receipt,id}';
      if not found or v_publication.decision_bundle_id::text is distinct from v_local.result_json->>'decision_bundle_id'
         or v_publication.bundle_revision is distinct from v_local.common_bundle_revision
         or v_publication.candidate_id is distinct from v_approval.candidate_id
         or v_publication.decided_by_user_id is distinct from v_local.actor_user_id
         or v_local.result_json#>'{publication,ok}' is distinct from 'true'::jsonb
         or v_local.result_json#>'{publication,published}' is distinct from 'true'::jsonb
         or v_local.result_json#>'{publication,receipt}' is distinct from
            private.weekly_source_publication_receipt_json_v1(v_publication.id) then raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000'; end if;
      v_member:=array_position(v_publication.member_root_ids,v_local.root_timesheet_id);
      if v_member is null or v_publication.member_family_booking_ids[v_member] is distinct from v_scope->>'family_booking_id'
         or v_publication.member_root_versions[v_member] is distinct from v_publication_version
         or not exists(select 1 from public.weekly_source_entitlement_heads h
           where h.id=v_publication.head_ids[v_member] and h.root_timesheet_id=v_local.root_timesheet_id
             and h.root_family_booking_id=v_scope->>'family_booking_id'
             and h.root_timesheet_version=v_publication_version
             and h.decision_bundle_id=v_publication.decision_bundle_id
             and h.bundle_revision=v_publication.bundle_revision
             and h.state in ('COMMITTED_CURRENT','SUPERSEDED')) then raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000'; end if;
      select h.* into strict v_head from public.weekly_source_entitlement_heads h
        where h.id=v_publication.head_ids[v_member];
      if v_head.authority_kind is distinct from 'PROTECTED'
         or v_head.candidate_id is distinct from v_approval.candidate_id
         or v_head.contract_id is distinct from v_approval.contract_id
         or v_head.week_ending_date is distinct from v_approval.week_ending then raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000'; end if;
      select coalesce(jsonb_agg(private.weekly_source_publication_component_canonical_v1(
        (to_jsonb(c)-array['id','head_id','decision_bundle_id','bundle_revision','component_sha256','created_at_utc'])
          ||jsonb_build_object('work_date',to_char(c.work_date,'YYYY-MM-DD'),
            'hours_day',c.hours_day::text,'hours_night',c.hours_night::text,
            'hours_sat',c.hours_sat::text,'hours_sun',c.hours_sun::text,
            'hours_bh',c.hours_bh::text,'unit_count',c.unit_count::text,
            'unit_pay_rate',c.unit_pay_rate::text,'unit_charge_rate',c.unit_charge_rate::text,
            'pay_ex_vat',c.pay_ex_vat::text,'charge_ex_vat',c.charge_ex_vat::text),
        'query.local.accepted_component') order by c.component_ordinal),'[]'::jsonb)
        into v_components from public.weekly_source_entitlement_head_components c where c.head_id=v_head.id;
      if jsonb_array_length(v_components) is distinct from v_head.component_count
         or exists(select 1 from public.weekly_source_entitlement_head_components c
           where c.head_id=v_head.id and c.component_sha256 is distinct from
             private.weekly_source_publication_request_digest_v1(private.weekly_source_publication_component_content_v1(
               private.weekly_source_publication_component_canonical_v1((to_jsonb(c)-array['id','head_id',
                 'decision_bundle_id','bundle_revision','component_sha256','created_at_utc'])
                   ||jsonb_build_object('work_date',to_char(c.work_date,'YYYY-MM-DD'),
                     'hours_day',c.hours_day::text,'hours_night',c.hours_night::text,
                     'hours_sat',c.hours_sat::text,'hours_sun',c.hours_sun::text,
                     'hours_bh',c.hours_bh::text,'unit_count',c.unit_count::text,
                     'unit_pay_rate',c.unit_pay_rate::text,'unit_charge_rate',c.unit_charge_rate::text,
                     'pay_ex_vat',c.pay_ex_vat::text,'charge_ex_vat',c.charge_ex_vat::text),
                 'query.local.accepted_component')))) then raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000'; end if;
      v_origin:=v_head.source_origin_json;
      if v_head.source_generation_digest is distinct from private.weekly_source_publication_request_digest_v1(v_origin) then raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000'; end if;
    end if;
    -- Compatible V4 local publisher must supply this real closed origin.
    -- The old current-Final constructor is deliberately NOT an accepted
    -- substitute, even if it published a genuine same-root/actor Source HEAD.
    if private.weekly_exceptional_json_keys_exact_v1(v_origin,array['origin_kind','publication_request_id',
       'generation_id','request_sha256','source_qualification_digest','policy_fingerprint',
       'before_origin','before_inventory_digest']) is distinct from true
       or not (v_origin ?& array['origin_kind','publication_request_id','generation_id','request_sha256',
          'source_qualification_digest','policy_fingerprint','before_origin','before_inventory_digest'])
       or v_origin->>'origin_kind' is distinct from 'PROTECTED_LOCAL_DECISION_V1'
       or v_origin->>'publication_request_id' is distinct from v_request.id::text
       or v_origin->>'generation_id' is distinct from v_generation.id::text
       or v_origin->>'request_sha256' is distinct from encode(v_request.request_sha256,'hex')
       or private.weekly_source_publication_request_digest_v1(v_origin)
          is distinct from v_local.publication_origin_digest
       or v_origin->>'source_qualification_digest'
          is distinct from encode(v_local.source_qualification_digest,'hex')
       or coalesce(v_origin->>'source_qualification_digest','') !~ '^[0-9a-f]{64}$'
       or coalesce(v_origin->>'policy_fingerprint','') !~ '^[0-9a-f]{64}$'
       or coalesce(v_origin->>'before_inventory_digest','') !~ '^[0-9a-f]{64}$'
       or jsonb_typeof(v_origin->'before_origin') is distinct from 'object'
       or v_local.approved_snapshot_json->'local_publication_origin' is distinct from v_origin
       or private.weekly_source_sha256_jsonb_v1('PROTECTED_LOCAL_SOURCE_QUALIFICATION_V2',
          v_local.approved_snapshot_json->'local_source_qualification')
          is distinct from v_local.source_qualification_digest
       or coalesce(v_local.approved_snapshot_json->>'common_components_digest','') !~ '^[0-9a-f]{64}$'
       or v_local.approved_snapshot_json->>'common_components_digest' is distinct from encode(
          private.weekly_source_publication_request_digest_v1(v_components),'hex') then raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000'; end if;
    -- The retained qualification must belong to this exact accepted action.
    -- A later WAIT can reuse this approval with a different source proposal:
    -- point-read the original immutable event, never compare it to latest WAIT.
    v_qualification:=v_local.approved_snapshot_json->'local_source_qualification';
    if private.weekly_exceptional_json_keys_exact_v1(v_qualification,array[
         'schema_version','publication_request_id','generation_id','scope','source_cycle_id',
         'work_event_id','approval_id','actor_user_id','family_event_sequence','source_proposal_hash',
         'complete_vector_hash','rate_policy_fingerprint','prepared_sources']) is distinct from true
       or not (v_qualification ?& array['schema_version','publication_request_id','generation_id',
         'scope','source_cycle_id','work_event_id','approval_id','actor_user_id','family_event_sequence',
         'source_proposal_hash','complete_vector_hash','rate_policy_fingerprint','prepared_sources'])
       or v_qualification->>'schema_version' is distinct from 'PROTECTED_LOCAL_SOURCE_QUALIFICATION_V2'
       or v_qualification->>'publication_request_id' is distinct from v_request.id::text
       or v_qualification->>'generation_id' is distinct from v_generation.id::text
       or v_qualification->>'source_cycle_id' is distinct from v_approval.source_cycle_id::text
       or v_qualification->>'work_event_id' is distinct from v_approval.work_event_id::text
       or v_qualification->>'approval_id' is distinct from v_approval.id::text
       or v_qualification->>'actor_user_id' is distinct from v_local.actor_user_id::text
       or v_qualification->>'complete_vector_hash' is distinct from encode(v_generation.complete_next_vector_hash,'hex')
       or v_qualification->>'rate_policy_fingerprint' is distinct from encode(v_approval.contract_rate_policy_source_fingerprint,'hex')
       or v_origin->>'policy_fingerprint' is distinct from v_qualification->>'rate_policy_fingerprint'
       or v_qualification#>>'{scope,root_timesheet_id}' is distinct from v_local.root_timesheet_id::text
       or v_qualification#>>'{scope,root_version}' is distinct from v_publication_version::text
       or v_qualification#>>'{scope,family_booking_id}' is distinct from v_scope->>'family_booking_id'
       or v_qualification#>>'{scope,target_family_id}' is distinct from v_local.family_id::text
       or v_qualification#>>'{scope,candidate_id}' is distinct from v_approval.candidate_id::text
       or v_qualification#>>'{scope,client_id}' is distinct from v_approval.client_id::text
       or v_qualification#>>'{scope,contract_id}' is distinct from v_approval.contract_id::text
       or v_qualification#>>'{scope,week_ending_date}' is distinct from to_char(v_approval.week_ending,'YYYY-MM-DD')
       or coalesce(v_qualification->>'family_event_sequence','') !~ '^[1-9][0-9]*$'
       or (v_qualification->>'family_event_sequence')::numeric>9223372036854775807
       or coalesce(v_qualification->>'source_proposal_hash','') !~ '^[0-9a-f]{64}$'
       or jsonb_typeof(v_qualification->'prepared_sources') is distinct from 'array' then raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000'; end if;
    select e.* into v_original_event from public.weekly_exceptional_pay_family_events e
      where e.family_id=v_local.family_id
        and e.event_sequence=(v_qualification->>'family_event_sequence')::bigint;
    if not found or v_original_event.durable_work_event_id is distinct from v_approval.work_event_id
       or v_original_event.evidence_approval_id is distinct from v_approval.id
       or v_original_event.office_actor_user_id is distinct from v_local.actor_user_id
       or v_original_event.work_date is distinct from v_approval.protected_work_date
       or v_original_event.source_proposal_hash is distinct from decode(v_qualification->>'source_proposal_hash','hex')
       or v_original_event.source_proposal_hash is distinct from private.weekly_source_sha256_jsonb_v1(
          'WEEKLY_PROTECTED_SOURCE_PROPOSAL_V1',v_original_event.source_proposal_snapshot_json) then raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000'; end if;
    -- Qualify the original sealed before-position, never today's HEAD/root.
    -- The complete vector was sealed by the actual stage owner. A later
    -- sibling amendment or physical rotation cannot replace this evidence.
    v_prior_inventory:=v_generation.complete_next_vector_json->'prior_effective_inventory';
    v_before_origin:=v_prior_inventory#>'{approval_basis,origin}';
    if v_prior_inventory->'ok' is distinct from 'true'::jsonb
       or v_prior_inventory#>'{approval_basis,coverage_complete}' is distinct from 'true'::jsonb
       or coalesce(v_before_origin->>'kind','') not in ('INITIAL_AUTHORISED_TSFIN_V1','COMMITTED_SOURCE_HEAD_V1')
       or v_prior_inventory#>>'{approval_basis,scope,root_timesheet_id}' is distinct from v_local.root_timesheet_id::text
       or v_prior_inventory#>>'{approval_basis,scope,root_version}' is distinct from v_publication_version::text
       or v_prior_inventory#>>'{approval_basis,scope,family_booking_id}' is distinct from v_scope->>'family_booking_id'
       or v_prior_inventory#>>'{approval_basis,scope,target_family_id}' is distinct from v_local.family_id::text
       or v_prior_inventory#>>'{approval_basis,scope,candidate_id}' is distinct from v_scope->>'candidate_id'
       or v_prior_inventory#>>'{approval_basis,scope,client_id}' is distinct from v_scope->>'client_id'
       or v_prior_inventory#>>'{approval_basis,scope,contract_id}' is distinct from v_scope->>'contract_id'
       or v_prior_inventory#>>'{approval_basis,scope,week_ending_date}' is distinct from v_scope->>'week_ending_date'
       or v_origin->>'before_inventory_digest' is distinct from v_prior_inventory->>'inventory_digest' then raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000'; end if;
    if v_before_origin->>'kind'='COMMITTED_SOURCE_HEAD_V1' then
      v_before_origin:=v_before_origin-'source_revision';
    end if;
    if v_origin->'before_origin' is distinct from v_before_origin
       or private.weekly_source_local_origin_canonical_v2(v_origin) is distinct from v_origin then raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000'; end if;
  else raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000';
  end if;
  -- The original completion step is immutable. Pending release updates the
  -- Local/run/request result together, but does not rewrite that original step.
  select count(*) into v_count from public.weekly_exceptional_orchestration_steps step
    where step.orchestration_run_id=v_run.id and step.step_kind='COMPLETE_LOCAL_PROTECTED_DECISION';
  if v_count<>1 then raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000'; end if;
  select step.* into strict v_step from public.weekly_exceptional_orchestration_steps step
    where step.orchestration_run_id=v_run.id and step.step_kind='COMPLETE_LOCAL_PROTECTED_DECISION';
  if v_step.idempotency_key is distinct from v_local.idempotency_key
     or v_step.allowlisted_owner_name is distinct from 'CloudTMS.weekly_exceptional_pay_complete_local_v1'
     or v_step.allowlisted_owner_signature is distinct from 'jsonb'
     or v_step.bounded_request_hash is distinct from v_local.request_sha256
     or v_step.before_state_fingerprint is distinct from v_run.before_state_fingerprint
     or v_step.completed_at_utc is null or not isfinite(v_step.completed_at_utc)
     or v_step.owner_response_hash is distinct from private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_PROTECTED_LOCAL_RESULT_V1',v_step.bounded_owner_response_json)
     or v_step.after_state_fingerprint is distinct from v_step.owner_response_hash then
    raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000';
  end if;
  if v_local.result_json ? 'deferred_release_receipt_id' then
    if v_local.state<>'COMPLETE' or v_step.outcome is distinct from 'PENDING'
       or v_step.bounded_owner_response_json->>'outcome' is distinct from 'SAVED_PENDING_FREEZE'
       or v_step.bounded_owner_response_json->>'state' is distinct from 'PENDING'
       or v_local.result_json is distinct from (v_step.bounded_owner_response_json||jsonb_build_object(
         'outcome','PUBLISHED','state','LIVE',
         'deferred_release_receipt_id',v_local.result_json->'deferred_release_receipt_id',
         'publication',v_local.result_json->'publication',
         'family_bound_version',v_local.result_json->'family_bound_version'))
       or not exists(select 1 from public.weekly_source_pending_entitlement_bundles p
         where p.id::text=v_step.bounded_owner_response_json#>>'{publication,pending_bundle_verified}'
           and p.state='RELEASED' and p.decision_bundle_id=v_local.common_decision_bundle_id
           and p.bundle_revision=v_local.common_bundle_revision
           and p.released_receipt_id::text=v_local.result_json->>'deferred_release_receipt_id'
           and p.released_receipt_id=v_publication.id
           and p.released_receipt_digest=v_publication.request_digest) then
      raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000';
    end if;
  elsif v_step.bounded_owner_response_json is distinct from v_local.result_json
     or v_step.outcome is distinct from (case when v_local.state='PENDING_FREEZE' then 'PENDING' else 'COMPLETE' end) then
    raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000';
  end if;
  return jsonb_build_object('family_id',v_local.family_id,'orchestration_run_id',v_run.id,
    'publication_request_id',v_request.id,'request_sha256',encode(v_local.request_sha256,'hex'),
    'generation_id',v_generation.id,'state',v_local.state,
    'requires_first_authorisation',v_local.result_json->'requires_first_authorisation',
    'idempotent_replay',true,'result',v_local.result_json||jsonb_build_object('idempotent_replay',true));
exception when no_data_found or too_many_rows or invalid_text_representation or numeric_value_out_of_range then
  raise exception 'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' using errcode='55000';
end;
$function$;
alter function private.weekly_source_local_saved_status_v1(uuid,uuid,uuid) owner to postgres;
revoke all on function private.weekly_source_local_saved_status_v1(uuid,uuid,uuid)
  from public,anon,authenticated,service_role;
commit;
