-- Repeatable CloudTMS function/view authority: weekly_source_local_publication_context_v2
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- Factual producer context, not a new calculator/financial/publication owner.
-- Qualifies only a real already-authorised local decision before publication,
-- or its unchanged pending before-position. A completed receipt's replay/read
-- uses its original sealed facts instead; this is NOT a historical chooser.
create or replace function private.weekly_source_local_publication_context_v2(
  p_publication_request_id uuid,p_generation_id uuid,p_root_timesheet_id uuid
) returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_local private.weekly_source_local_protected_decision_receipts%rowtype;
  v_request public.weekly_exceptional_c1_publication_requests%rowtype;
  v_generation public.weekly_exceptional_pay_generations%rowtype;
  v_family public.weekly_exceptional_pay_target_families%rowtype;
  v_run public.weekly_exceptional_orchestration_runs%rowtype;
  v_approval public.weekly_exceptional_payment_approvals%rowtype;
  v_event public.weekly_exceptional_pay_family_events%rowtype;
  v_stage public.weekly_exceptional_orchestration_steps%rowtype;
  v_complete public.weekly_exceptional_orchestration_steps%rowtype;
  v_scope jsonb;
  v_inventory jsonb;
  v_prior_inventory jsonb;
  v_basis jsonb;
  v_before_origin jsonb;
  v_snapshot jsonb;
  v_sources jsonb;
  v_source_count integer;
  v_context jsonb;
  v_qualification jsonb;
  v_policy_hash bytea;
  v_before_bound bigint;
  v_staged_bound bigint;
  v_stage_hash bytea;
begin
  if p_publication_request_id is null or p_generation_id is null or p_root_timesheet_id is null then
    raise exception 'WEEKLY_PROTECTED_LOCAL_PUBLICATION_IDENTITY_REQUIRED' using errcode='22023'; end if;
  select r.* into v_local from private.weekly_source_local_protected_decision_receipts r
    where r.publication_request_id=p_publication_request_id;
  if not found or v_local.generation_id is distinct from p_generation_id
     or v_local.root_timesheet_id is distinct from p_root_timesheet_id
     or (v_local.state='PREPARING' and v_local.preparing_transaction_id is distinct from pg_current_xact_id())
     or v_local.state not in ('PREPARING','PENDING_FREEZE') then return null; end if;
  v_scope:=private.weekly_source_pay_query_scope_v1(p_root_timesheet_id);
  if v_scope is null or v_scope->>'target_family_id' is distinct from v_local.family_id::text then return null; end if;
  select r.* into v_request from public.weekly_exceptional_c1_publication_requests r
    where r.id=v_local.publication_request_id;
  if not found or v_request.generation_id is distinct from v_local.generation_id
     or v_request.family_id is distinct from v_local.family_id
     or v_request.root_timesheet_id is distinct from p_root_timesheet_id
     or v_request.request_sha256 is distinct from v_local.request_sha256
     or v_request.financial_row_id is distinct from v_local.prior_financial_id
     or v_request.agency_id is distinct from (select f.agency_id from public.weekly_exceptional_pay_target_families f where f.id=v_local.family_id)
     or v_request.candidate_id::text is distinct from v_scope->>'candidate_id'
     or v_request.contract_id::text is distinct from v_scope->>'contract_id'
     or to_char(v_request.week_ending_date,'YYYY-MM-DD') is distinct from v_scope->>'week_ending_date'
     or v_request.c1_operation_id is not null or v_request.c1_publication_id is not null
     or v_request.c1_head_revision is not null or v_request.c1_receipt_sha256 is not null
     or (v_local.state='PREPARING' and v_request.state<>'READY')
     or (v_local.state='PENDING_FREEZE' and v_request.state<>'RETIRED') then return null; end if;
  select r.* into v_run from public.weekly_exceptional_orchestration_runs r
    where r.id=v_request.orchestration_run_id;
  if not found or v_run.family_id is distinct from v_local.family_id
     or v_run.requested_by_user_id is distinct from v_local.actor_user_id
     or (v_local.state='PREPARING' and v_run.state<>'RUNNING')
     or (v_local.state='PENDING_FREEZE' and v_run.state<>'COMPLETE') then return null; end if;
  if v_local.state='PENDING_FREEZE' and (v_run.completed_at_utc is null
     or v_request.completed_at_utc is null or v_local.completed_at_utc is null
     or v_request.typed_result_json is distinct from v_local.result_json
     or v_run.after_state_fingerprint is distinct from private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_PROTECTED_LOCAL_RESULT_V1',v_local.result_json)
     or v_local.result_json->>'outcome' is distinct from 'SAVED_PENDING_FREEZE'
     or v_local.result_json->>'family_bound_version' is distinct from v_scope->>'family_bound_version') then return null; end if;
  select f.* into v_family from public.weekly_exceptional_pay_target_families f
    where f.id=v_local.family_id;
  if not found or v_family.root_timesheet_id is distinct from p_root_timesheet_id
     or v_family.root_family_booking_id is distinct from v_scope->>'family_booking_id'
     or v_family.candidate_id is distinct from v_request.candidate_id
     or v_family.contract_id is distinct from v_request.contract_id
     or v_family.week_ending_date is distinct from v_request.week_ending_date then return null; end if;
  select g.* into v_generation from public.weekly_exceptional_pay_generations g
    where g.id=v_local.generation_id and g.family_id=v_local.family_id;
  if not found or v_generation.lifecycle_state<>'PENDING_C1'
     or private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_COMPLETE_VECTOR_V1',
        v_generation.complete_next_vector_json) is distinct from v_generation.complete_next_vector_hash
     or v_family.current_generation_id is distinct from v_generation.prior_generation_id then return null; end if;
  select a.* into strict v_approval from public.weekly_exceptional_payment_approvals a
    where a.creation_orchestration_run_id=v_run.id and a.pay_target_family_id=v_family.id;
  if v_approval.withdrawn_at_utc is not null or v_approval.approved_by_user_id is distinct from v_local.actor_user_id
     or v_approval.candidate_id is distinct from v_family.candidate_id
     or v_approval.client_id::text is distinct from v_scope->>'client_id'
     or v_approval.contract_id is distinct from v_family.contract_id
     or v_approval.week_ending is distinct from v_family.week_ending_date
     or v_approval.protected_work_date not between v_family.week_ending_date-6 and v_family.week_ending_date
     or not exists(select 1 from public.weekly_work_events e
       where e.id=v_approval.work_event_id and e.candidate_id=v_family.candidate_id
         and e.client_id=v_approval.client_id and e.work_date=v_approval.protected_work_date) then return null; end if;
  select e.* into v_event from public.weekly_exceptional_pay_family_events e
    where e.family_id=v_family.id and e.durable_work_event_id=v_approval.work_event_id
    order by e.event_sequence desc limit 1;
  if not found or v_event.evidence_approval_id is distinct from v_approval.id
     or v_event.office_actor_user_id is distinct from v_local.actor_user_id
     or v_event.work_date is distinct from v_approval.protected_work_date
     or private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_SOURCE_PROPOSAL_V1',
        v_event.source_proposal_snapshot_json) is distinct from v_event.source_proposal_hash
     or not exists(select 1 from public.weekly_exceptional_pay_target_events e
       where e.family_id=v_family.id and e.approval_id=v_approval.id
         and e.financial_generation_id=v_generation.id
         and e.complete_next_family_vector_fingerprint=v_generation.complete_next_vector_hash) then return null; end if;
  v_snapshot:=v_generation.complete_next_vector_json#>'{target_snapshot,tsfin_snapshot_json}';
  if jsonb_typeof(v_snapshot) is distinct from 'object'
     or v_snapshot is distinct from v_approval.approved_target_pay_components_json->'tsfin_snapshot_json'
     or (v_local.approved_snapshot_json-array['common_decision_bundle_id','common_components_digest',
           'local_source_qualification','local_publication_origin'])
        is distinct from (v_snapshot||jsonb_build_object('timesheet_id',p_root_timesheet_id::text,
          'timesheet_version',(v_scope->>'root_version')::integer,'actual_schedule_json',
          v_generation.complete_next_vector_json#>'{target_snapshot,actual_schedule_json}')) then return null; end if;
  v_inventory:=private.weekly_source_effective_inventory_v1(p_root_timesheet_id);
  v_prior_inventory:=v_generation.complete_next_vector_json->'prior_effective_inventory';
  -- The actual stage owner sealed the old entitlement at K, then advanced
  -- only its own family version to K+1. Prove that exact successful stage
  -- step before normalising this nonmonetary transition. No unrelated change
  -- to the complete before-position is accepted.
  if coalesce(v_prior_inventory#>>'{approval_basis,scope,family_bound_version}','') !~ '^(0|[1-9][0-9]*)$'
     or v_prior_inventory#>>'{approval_basis,scope,target_family_id}' is distinct from v_family.id::text then return null; end if;
  v_before_bound:=(v_prior_inventory#>>'{approval_basis,scope,family_bound_version}')::bigint;
  v_staged_bound:=v_before_bound+1;
  select s.* into strict v_stage from public.weekly_exceptional_orchestration_steps s
    where s.orchestration_run_id=v_run.id and s.step_kind='STAGE_C1_REQUEST';
  v_stage_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_C1_STAGED_STATE_V1',
    jsonb_build_object('family_id',v_family.id,'generation_id',v_generation.id,
      'publication_request_id',v_request.id,'request_sha256',encode(v_request.request_sha256,'hex'),
      'new_bound_version',v_staged_bound));
  if v_stage.outcome is distinct from 'COMPLETE' or v_stage.completed_at_utc is null
     or v_stage.allowlisted_owner_name is distinct from 'public.weekly_exceptional_pay_stage_c1_request_v1'
     or v_stage.allowlisted_owner_signature is distinct from 'jsonb->jsonb'
     or v_stage.before_state_fingerprint is distinct from v_run.before_state_fingerprint
     or v_stage.after_state_fingerprint is distinct from v_stage_hash
     or v_stage.owner_response_hash is distinct from v_stage_hash
     or v_stage.bounded_owner_response_json is distinct from jsonb_build_object(
       'publication_request_id',v_request.id,'generation_id',v_generation.id,
       'request_sha256',encode(v_request.request_sha256,'hex'))
     or (v_local.state='PREPARING' and v_family.bound_version is distinct from v_staged_bound)
     or (v_local.state='PENDING_FREEZE' and v_family.bound_version is distinct from v_staged_bound+1) then return null; end if;
  v_inventory:=jsonb_set(v_inventory,'{approval_basis,scope,family_bound_version}',
    v_prior_inventory#>'{approval_basis,scope,family_bound_version}',false);
  if v_local.state='PENDING_FREEZE' then
    select s.* into strict v_complete from public.weekly_exceptional_orchestration_steps s
      where s.orchestration_run_id=v_run.id and s.step_kind='COMPLETE_LOCAL_PROTECTED_DECISION'
        and s.idempotency_key=v_local.idempotency_key;
    if v_complete.sequence<=v_stage.sequence or v_complete.completed_at_utc is null
       or v_complete.outcome is distinct from 'PENDING'
       or v_complete.allowlisted_owner_name is distinct from 'CloudTMS.weekly_exceptional_pay_complete_local_v1'
       or v_complete.allowlisted_owner_signature is distinct from 'jsonb'
       or v_complete.bounded_request_hash is distinct from v_local.request_sha256
       or v_complete.before_state_fingerprint is distinct from v_run.before_state_fingerprint
       or v_complete.bounded_owner_response_json is distinct from v_local.result_json
       or v_complete.owner_response_hash is distinct from v_run.after_state_fingerprint
       or v_complete.after_state_fingerprint is distinct from v_run.after_state_fingerprint then return null; end if;
    -- Acceptance adds one further, independently proved own increment. Keep
    -- the qualified publication scope as it was at stage/Save, not today's
    -- accepted version; the original entitlement witness remains untouched.
    v_scope:=jsonb_set(v_scope,'{family_bound_version}',
      to_jsonb(v_staged_bound::text),false);
  end if;
  v_basis:=v_inventory->'approval_basis';
  if v_inventory->>'ok' is distinct from 'true'
     or v_basis->>'coverage_complete' is distinct from 'true'
     or coalesce(v_basis#>>'{origin,kind}','') not in ('INITIAL_AUTHORISED_TSFIN_V1','COMMITTED_SOURCE_HEAD_V1')
     or v_prior_inventory is distinct from v_inventory
     or v_basis#>>'{scope,root_timesheet_id}' is distinct from p_root_timesheet_id::text
     or v_basis#>>'{scope,family_booking_id}' is distinct from v_scope->>'family_booking_id'
     or v_basis#>>'{scope,root_version}' is distinct from v_scope->>'root_version'
     or v_basis#>>'{scope,candidate_id}' is distinct from v_scope->>'candidate_id'
     or v_basis#>>'{scope,client_id}' is distinct from v_scope->>'client_id'
     or v_basis#>>'{scope,contract_id}' is distinct from v_scope->>'contract_id'
     or v_basis#>>'{scope,week_ending_date}' is distinct from v_scope->>'week_ending_date' then return null; end if;
  -- Actual Source proposal owner, not a made-up current Final for a local save.
  v_context:=private.weekly_source_protected_final_source_context_v1(
    v_family.id,v_approval.source_cycle_id,v_approval.work_event_id);
  if v_context->'source_proposal' is distinct from v_event.source_proposal_snapshot_json then return null; end if;
  select private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_RATE_POLICY_V1',
    jsonb_build_object('contract_id',v_family.contract_id,'rates_json',c.rates_json,
      'policy',private._weekly_source_effective_policy_v1(v_approval.client_id,v_family.contract_id,
        v_approval.protected_work_date),'classification',v_event.rate_classification_json))
    into strict v_policy_hash from public.contracts c where c.id=v_family.contract_id;
  if v_policy_hash is distinct from v_approval.contract_rate_policy_source_fingerprint then return null; end if;
  select count(*)::integer,coalesce(jsonb_agg(jsonb_build_object(
    'source_ordinal',s.source_ordinal,'authority_kind',s.authority_kind,'source_system',s.source_system,
    'external_identity',s.external_identity,'external_revision',s.external_revision,
    'source_document_sha256',encode(s.source_document_sha256,'hex'),
    'payload_bytes',s.payload_bytes,'part_count',s.part_count,'work_date',s.work_date,
    'root_timesheet_id',s.root_timesheet_id,'candidate_id',s.candidate_id,'contract_id',s.contract_id,
    'source_row_sha256',encode(s.source_row_sha256,'hex')) order by s.source_ordinal),'[]'::jsonb)
    into v_source_count,v_sources from public.weekly_exceptional_c1_source_records s
    where s.publication_request_id=v_request.id;
  if v_source_count is distinct from v_request.expected_source_count
     or not exists(select 1 from public.weekly_exceptional_c1_source_records s
       where s.publication_request_id=v_request.id and s.authority_kind='CLIENT_SOURCE')
     or exists(select 1 from public.weekly_exceptional_c1_source_records s
       where s.publication_request_id=v_request.id and (s.root_timesheet_id is distinct from p_root_timesheet_id
         or s.candidate_id is distinct from v_family.candidate_id
         or s.contract_id is distinct from v_family.contract_id)) then return null; end if;
  v_qualification:=jsonb_build_object('schema_version','PROTECTED_LOCAL_SOURCE_QUALIFICATION_V2',
    'publication_request_id',v_request.id,'generation_id',v_generation.id,'scope',v_scope,
    'source_cycle_id',v_approval.source_cycle_id,'work_event_id',v_approval.work_event_id,
    'approval_id',v_approval.id,'actor_user_id',v_local.actor_user_id,
    'family_event_sequence',v_event.event_sequence::text,
    'source_proposal_hash',encode(v_event.source_proposal_hash,'hex'),
    'complete_vector_hash',encode(v_generation.complete_next_vector_hash,'hex'),
    'rate_policy_fingerprint',encode(v_approval.contract_rate_policy_source_fingerprint,'hex'),
    'prepared_sources',v_sources);
  -- Keep the complete live-versus-sealed I1 comparison above unchanged. Only
  -- this Local publication witness is flattened, after genuine qualification.
  -- Generic I1 retains the referenced HEAD's full Source/Final lineage.
  v_before_origin:=v_basis->'origin';
  if v_before_origin->>'kind'='COMMITTED_SOURCE_HEAD_V1' then
    v_before_origin:=v_before_origin-'source_revision';
  end if;
  return jsonb_build_object('scope',v_scope,'agency_id',v_family.agency_id,
    'actor_user_id',v_local.actor_user_id,'before_origin',v_before_origin,
    'before_inventory_digest',v_inventory->'inventory_digest',
    'source_qualification_digest',encode(private.weekly_source_sha256_jsonb_v1(
      'PROTECTED_LOCAL_SOURCE_QUALIFICATION_V2',v_qualification),'hex'),
    'policy_fingerprint',encode(v_approval.contract_rate_policy_source_fingerprint,'hex'),
    'qualification',v_qualification);
end;
$function$;
alter function private.weekly_source_local_publication_context_v2(uuid,uuid,uuid) owner to postgres;
revoke all on function private.weekly_source_local_publication_context_v2(uuid,uuid,uuid)
  from public,anon,authenticated,service_role;

commit;
