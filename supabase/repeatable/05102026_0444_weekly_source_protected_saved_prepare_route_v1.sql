-- Retained Local Save routing only. No current money, root or query eligibility
-- is reconstructed; the immutable completion owner returns the saved result.
\set ON_ERROR_STOP on
begin;

create or replace function private.weekly_source_protected_saved_prepare_route_v1(
  p_request jsonb,p_hash_domain text,p_expected_kind text
) returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_run public.weekly_exceptional_orchestration_runs%rowtype;
  v_request public.weekly_exceptional_c1_publication_requests%rowtype;
  v_local private.weekly_source_local_protected_decision_receipts%rowtype;
  v_generation public.weekly_exceptional_pay_generations%rowtype;
  v_approval public.weekly_exceptional_payment_approvals%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_actor uuid;
  v_hash bytea;
  v_count integer;
begin
  if coalesce(current_setting('request.jwt.claim.role',true),
       nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  if jsonb_typeof(p_request) is distinct from 'object'
     or p_hash_domain not in ('WEEKLY_PROTECTED_PREPARE_FAMILY_REQUEST_V1',
       'WEEKLY_PROTECTED_PREPARE_ACTION_REQUEST_V1')
     or p_expected_kind not in ('APPROVE','AMEND','WITHDRAW','WAIT','RECONCILE','RECORD_NOT_WORKED') then
    raise exception 'WEEKLY_PROTECTED_PREPARE_ROUTE_INVALID' using errcode='22023';
  end if;
  v_actor:=(p_request->>'actor_user_id')::uuid;
  v_hash:=private.weekly_source_sha256_jsonb_v1(p_hash_domain,p_request);
  select r.* into v_run from public.weekly_exceptional_orchestration_runs r
    where r.idempotency_key=btrim(p_request->>'idempotency_key');
  if not found then return null; end if;
  if v_run.requested_by_user_id is distinct from v_actor
     or v_run.request_fingerprint is distinct from v_hash
     or (p_hash_domain='WEEKLY_PROTECTED_PREPARE_FAMILY_REQUEST_V1'
       and (p_expected_kind<>'APPROVE' or v_run.request_kind not in ('APPROVE','AMEND')))
     or (p_hash_domain='WEEKLY_PROTECTED_PREPARE_ACTION_REQUEST_V1'
       and v_run.request_kind is distinct from p_expected_kind) then
    raise exception 'WEEKLY_PROTECTED_ACTION_IDEMPOTENCY_COLLISION' using errcode='23505';
  end if;
  if v_run.state is distinct from 'COMPLETE' or v_run.completed_at_utc is null
     or not isfinite(v_run.completed_at_utc) then return null; end if;
  select count(*) into v_count from public.weekly_exceptional_c1_publication_requests r
    where r.family_id=v_run.family_id and r.orchestration_run_id=v_run.id;
  if v_count<>1 then return null; end if;
  select r.* into strict v_request from public.weekly_exceptional_c1_publication_requests r
    where r.family_id=v_run.family_id and r.orchestration_run_id=v_run.id;
  select l.* into v_local from private.weekly_source_local_protected_decision_receipts l
    where l.publication_request_id=v_request.id;
  if not found or v_local.state not in ('COMPLETE','PENDING_FREEZE')
     or v_local.completed_at_utc is null or not isfinite(v_local.completed_at_utc)
     or v_local.family_id is distinct from v_run.family_id
     or v_local.actor_user_id is distinct from v_actor
     or v_local.generation_id is distinct from v_request.generation_id
     or v_local.root_timesheet_id is distinct from v_request.root_timesheet_id
     or v_local.request_sha256 is distinct from v_request.request_sha256
     or v_request.state is distinct from 'RETIRED'
     or v_request.completed_at_utc is null or not isfinite(v_request.completed_at_utc)
     or v_request.typed_result_json is distinct from v_local.result_json
     or v_request.c1_operation_id is not null or v_request.c1_publication_id is not null
     or v_request.c1_head_revision is not null or v_request.c1_receipt_sha256 is not null
     or v_run.after_state_fingerprint is distinct from private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_PROTECTED_LOCAL_RESULT_V1',v_local.result_json)
     or v_local.result_json->'ok' is distinct from 'true'::jsonb
     or v_local.result_json->>'family_id' is distinct from v_run.family_id::text
     or v_local.result_json->>'generation_id' is distinct from v_request.generation_id::text
     or v_local.result_json->>'publication_request_id' is distinct from v_request.id::text then return null; end if;
  select g.* into v_generation from public.weekly_exceptional_pay_generations g
    where g.id=v_local.generation_id and g.family_id=v_local.family_id;
  if not found or private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_COMPLETE_VECTOR_V1',
      v_generation.complete_next_vector_json) is distinct from v_generation.complete_next_vector_hash then return null; end if;
  if v_local.state='COMPLETE' then
    if v_generation.lifecycle_state not in ('PUBLISHED','SUPERSEDED')
       or v_generation.published_at_utc is null or not isfinite(v_generation.published_at_utc)
       or v_generation.result_hash is distinct from v_run.after_state_fingerprint
       or v_local.result_json->>'outcome' is distinct from 'PUBLISHED' then return null; end if;
  elsif v_generation.lifecycle_state is distinct from 'PENDING_C1'
     or v_generation.published_at_utc is not null or v_generation.result_hash is not null
     or v_local.result_json->>'outcome' is distinct from 'SAVED_PENDING_FREEZE' then return null;
  end if;
  select count(*) into v_count from public.weekly_exceptional_payment_approvals a
    where a.creation_orchestration_run_id=v_run.id and a.pay_target_family_id=v_run.family_id;
  if v_count<>1 then return null; end if;
  select a.* into strict v_approval from public.weekly_exceptional_payment_approvals a
    where a.creation_orchestration_run_id=v_run.id and a.pay_target_family_id=v_run.family_id;
  if v_approval.approved_by_user_id is distinct from v_actor
     or v_approval.candidate_id is distinct from v_request.candidate_id
     or v_approval.contract_id is distinct from v_request.contract_id
     or v_approval.week_ending is distinct from v_request.week_ending_date then return null; end if;
  select c.* into strict v_cycle from public.weekly_source_cycles c where c.id=v_approval.source_cycle_id;
  perform private.weekly_source_office_authority_v1(v_actor,'APPROVE_PROTECTED_PAY',
    v_cycle.source_group_id,v_approval.client_id,v_cycle.finalisation_week_ending);
  -- Only the proved immutable identifiers required by the saved orchestrator.
  -- Never fabricate the original full PREPARE/root/finance response.
  return jsonb_build_object('family_id',v_run.family_id,'orchestration_run_id',v_run.id,
    'request_kind',v_run.request_kind,'run_state','COMPLETE','idempotent_replay',true);
end;
$function$;
alter function private.weekly_source_protected_saved_prepare_route_v1(jsonb,text,text) owner to postgres;
revoke all on function private.weekly_source_protected_saved_prepare_route_v1(jsonb,text,text)
  from public,anon,authenticated,service_role;
commit;
