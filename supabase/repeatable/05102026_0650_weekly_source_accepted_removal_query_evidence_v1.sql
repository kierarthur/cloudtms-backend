-- Accepted removal/reconciliation evidence. No monetary or publication write.
\set ON_ERROR_STOP on
begin;
create or replace function private.weekly_source_accepted_removal_query_evidence_v1(
  p_review_id uuid,p_approval_id uuid,p_generation_id uuid,p_actor_user_id uuid,p_command_id uuid
) returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_review private.weekly_source_manual_reviews%rowtype;
  v_anchor private.weekly_source_manual_review_commands%rowtype;
  v_approval public.weekly_exceptional_payment_approvals%rowtype;
  v_generation public.weekly_exceptional_pay_generations%rowtype;
  v_run public.weekly_exceptional_orchestration_runs%rowtype;
  v_event public.weekly_exceptional_pay_family_events%rowtype;
  v_local private.weekly_source_local_protected_decision_receipts%rowtype;
  v_status jsonb; v_witness jsonb; v_scope jsonb; v_result jsonb;
begin
  if p_review_id is null or p_approval_id is null or p_generation_id is null
     or p_actor_user_id is null or p_command_id is null then
    raise exception 'WEEKLY_SOURCE_ACCEPTED_REMOVAL_IDENTITY_REQUIRED' using errcode='22023'; end if;
  select * into strict v_review from private.weekly_source_manual_reviews where id=p_review_id and state='OPEN';
  select * into strict v_approval from public.weekly_exceptional_payment_approvals where id=p_approval_id;
  select * into strict v_generation from public.weekly_exceptional_pay_generations where id=p_generation_id;
  select * into strict v_run from public.weekly_exceptional_orchestration_runs where id=v_approval.creation_orchestration_run_id;
  if v_run.request_kind not in ('WITHDRAW','RECONCILE')
     or v_generation.reason is distinct from v_run.request_kind
     or v_generation.family_id is distinct from v_approval.pay_target_family_id
     or v_run.family_id is distinct from v_generation.family_id
     or v_run.requested_by_user_id is distinct from p_actor_user_id
     or v_approval.approved_by_user_id is distinct from p_actor_user_id
     or v_approval.work_event_id is distinct from v_review.work_event_id
     or v_approval.candidate_id is distinct from v_review.candidate_id
     or v_approval.client_id is distinct from v_review.client_id
     or v_approval.contract_id is distinct from v_review.contract_id
     or v_approval.protected_work_date is distinct from v_review.work_date
  then raise exception 'WEEKLY_SOURCE_ACCEPTED_REMOVAL_SCOPE_INVALID' using errcode='55000'; end if;
  v_status:=private.weekly_source_local_saved_status_v1(v_run.family_id,v_run.id,p_actor_user_id);
  if v_status is null or v_status->>'publication_request_id' is distinct from p_command_id::text
     or v_status->>'generation_id' is distinct from p_generation_id::text then
    raise exception 'WEEKLY_SOURCE_ACCEPTED_REMOVAL_RECEIPT_REQUIRED' using errcode='55000'; end if;
  select * into strict v_local from private.weekly_source_local_protected_decision_receipts
    where publication_request_id=p_command_id;
  -- No competing C1/NEXT lane, including partial or contradictory evidence.
  if (select count(*) from public.weekly_exceptional_c1_publication_requests r
      where r.generation_id=v_generation.id or r.orchestration_run_id=v_run.id)<>1
     or exists(select 1 from private.bpay_next_protected_source_receipt r
       where r.generation_id=v_generation.id or r.approval_id=v_approval.id or r.orchestration_run_id=v_run.id)
  then raise exception 'WEEKLY_SOURCE_ACCEPTED_REMOVAL_RECEIPT_CONFLICT' using errcode='55000'; end if;
  select * into strict v_event from public.weekly_exceptional_pay_family_events e
    where e.family_id=v_run.family_id and e.durable_work_event_id=v_review.work_event_id
      and not exists(select 1 from public.weekly_exceptional_pay_family_events newer
        where newer.family_id=e.family_id and newer.durable_work_event_id=e.durable_work_event_id
          and newer.event_sequence>e.event_sequence);
  if v_event.state is distinct from 'ACCEPTED_SOURCE' or v_event.evidence_approval_id is distinct from v_approval.id
     or v_event.office_actor_user_id is distinct from p_actor_user_id
     or private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_SOURCE_PROPOSAL_V1',
       v_event.source_proposal_snapshot_json) is distinct from v_event.source_proposal_hash
     or not exists(select 1 from public.weekly_exceptional_pay_target_events t
       where t.family_id=v_run.family_id and t.approval_id=v_approval.id and t.financial_generation_id=v_generation.id
         and t.reason=v_run.request_kind and t.actor_user_id=p_actor_user_id
         and t.current_source_proposal_snapshot=v_event.source_proposal_snapshot_json
         and t.complete_next_family_vector_fingerprint=v_generation.complete_next_vector_hash)
  then raise exception 'WEEKLY_SOURCE_ACCEPTED_REMOVAL_EVENT_REQUIRED' using errcode='55000'; end if;
  select * into v_anchor from private.weekly_source_manual_review_commands c where c.review_id=v_review.id
    and (c.opening_final_revision_id is not null or c.opening_projection_publication_id is not null);
  if found then
    if (v_anchor.opening_target_family_id is not null and v_anchor.opening_target_family_id<>v_run.family_id)
       or v_event.event_sequence<=v_anchor.opening_event_sequence then
      raise exception 'WEEKLY_SOURCE_ACCEPTED_REMOVAL_PREOPENING_ACTION' using errcode='55000'; end if;
  elsif v_approval.approved_at_utc<v_review.opened_at_utc then
    raise exception 'WEEKLY_SOURCE_ACCEPTED_REMOVAL_PREOPENING_ACTION' using errcode='55000'; end if;
  v_scope:=private.weekly_source_pay_query_scope_v1(v_local.root_timesheet_id);
  if v_scope is null or v_scope->>'target_family_id' is distinct from v_run.family_id::text
     or v_scope->>'candidate_id' is distinct from v_review.candidate_id::text
     or v_scope->>'client_id' is distinct from v_review.client_id::text
     or v_scope->>'contract_id' is distinct from v_review.contract_id::text then
    raise exception 'WEEKLY_SOURCE_ACCEPTED_REMOVAL_SCOPE_INVALID' using errcode='55000'; end if;
  v_witness:=private.weekly_source_selected_source_witness_v1(v_run.family_id,v_approval.source_cycle_id,v_review.work_event_id);
  if v_witness is null or v_witness->'source_proposal' is distinct from v_event.source_proposal_snapshot_json
     or v_witness#>>'{scope,source_group_id}' is distinct from v_review.source_group_id::text then
    raise exception 'WEEKLY_SOURCE_ACCEPTED_REMOVAL_SOURCE_UNAVAILABLE' using errcode='55000'; end if;
  if v_local.result_json->>'authority'='COMMON_ENTITLEMENT_HEAD'
     and (v_local.approved_snapshot_json#>>'{local_source_qualification,schema_version}'
        is distinct from 'PROTECTED_LOCAL_SOURCE_QUALIFICATION_V2'
       or v_local.approved_snapshot_json#>>'{local_source_qualification,source_proposal_hash}'
        is distinct from encode(v_event.source_proposal_hash,'hex')) then
    raise exception 'WEEKLY_SOURCE_ACCEPTED_REMOVAL_SOURCE_UNAVAILABLE' using errcode='55000'; end if;
  if v_witness->>'kind'='CURRENT_ROW' then
    -- A provisional positive row is not the Final-only zero proposal accepted
    -- by removal. Never silently convert that mismatch into source acceptance.
    if v_witness#>>'{basis,source_cycle_id}' is distinct from v_review.source_cycle_id::text
       or v_witness#>>'{basis,final_revision_id}' is null
       or v_witness#>>'{basis,final_revision_id}' is distinct from v_event.source_proposal_snapshot_json->>'source_revision'
       or v_event.source_proposal_snapshot_json->'source_present' is distinct from 'true'::jsonb
    then raise exception 'WEEKLY_SOURCE_ACCEPTED_REMOVAL_SOURCE_MISMATCH' using errcode='55000'; end if;
  elsif v_witness->>'kind'='CERTIFIED_ABSENCE' then
    if v_event.source_proposal_snapshot_json->'source_present' is distinct from 'false'::jsonb
       or v_event.source_proposal_snapshot_json->>'source_minutes' is distinct from '0' then
      raise exception 'WEEKLY_SOURCE_ACCEPTED_REMOVAL_SOURCE_MISMATCH' using errcode='55000'; end if;
  else raise exception 'WEEKLY_SOURCE_ACCEPTED_REMOVAL_SOURCE_UNAVAILABLE' using errcode='55000'; end if;
  v_result:=jsonb_build_object('lane','SOURCE_LOCAL_ACCEPTED_SOURCE','scope',v_scope,
    'approval_id',v_approval.id,'generation_id',v_generation.id,'publication_request_id',p_command_id,
    'request_sha256',v_status->'request_sha256','result_sha256',encode(private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_PROTECTED_LOCAL_RESULT_V1',v_local.result_json),'hex'),'selected_source_witness',v_witness);
  return v_result;
end;
$function$;
alter function private.weekly_source_accepted_removal_query_evidence_v1(uuid,uuid,uuid,uuid,uuid) owner to postgres;
revoke all on function private.weekly_source_accepted_removal_query_evidence_v1(uuid,uuid,uuid,uuid,uuid)
  from public,anon,authenticated,service_role;
commit;
