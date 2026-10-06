-- Repeatable CloudTMS function/view authority: weekly_source_pay_query_c1_receipt_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- Retained accepted C1 proof only. No new C1 call, publication, query
-- resolution, approval, monetary calculation or Banking admission.
create or replace function private.weekly_source_pay_query_c1_receipt_v1(
  p_root_timesheet_id uuid,p_approval_id uuid,p_generation_id uuid
) returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_scope jsonb;
  v_approval public.weekly_exceptional_payment_approvals%rowtype;
  v_generation public.weekly_exceptional_pay_generations%rowtype;
  v_request public.weekly_exceptional_c1_publication_requests%rowtype;
  v_run public.weekly_exceptional_orchestration_runs%rowtype;
  v_event public.weekly_exceptional_pay_family_events%rowtype;
  v_checkpoint public.weekly_exceptional_c1_publication_checkpoints%rowtype;
  v_step public.weekly_exceptional_orchestration_steps%rowtype;
  v_publication_root public.timesheets%rowtype;
  v_publication_fin public.timesheets_financials%rowtype;
  v_after_hash bytea;
  v_count integer;
begin
  if p_root_timesheet_id is null or p_approval_id is null or p_generation_id is null then
    raise exception 'WEEKLY_SOURCE_PAY_QUERY_C1_RECEIPT_IDENTITY_REQUIRED' using errcode='22023'; end if;
  v_scope:=private.weekly_source_pay_query_scope_v1(p_root_timesheet_id);
  if v_scope is null or v_scope->>'target_family_id' is null then return null; end if;
  select a.* into v_approval from public.weekly_exceptional_payment_approvals a where a.id=p_approval_id;
  if not found or v_approval.withdrawn_at_utc is not null
     or v_approval.pay_target_family_id::text is distinct from v_scope->>'target_family_id'
     or v_approval.candidate_id::text is distinct from v_scope->>'candidate_id'
     or v_approval.client_id::text is distinct from v_scope->>'client_id'
     or v_approval.contract_id::text is distinct from v_scope->>'contract_id'
     or to_char(v_approval.week_ending,'YYYY-MM-DD') is distinct from v_scope->>'week_ending_date'
     or v_approval.protected_work_date not between
       (v_scope->>'week_ending_date')::date-6 and (v_scope->>'week_ending_date')::date
     or not exists(select 1 from public.weekly_work_events e where e.id=v_approval.work_event_id
       and e.candidate_id=v_approval.candidate_id and e.client_id=v_approval.client_id
       and e.work_date=v_approval.protected_work_date) then return null; end if;
  select e.* into v_event from public.weekly_exceptional_pay_family_events e
    where e.family_id=v_approval.pay_target_family_id and e.durable_work_event_id=v_approval.work_event_id
    order by e.event_sequence desc limit 1;
  if not found or v_event.state is distinct from 'WAIT'
     or v_event.evidence_approval_id is distinct from v_approval.id
     or v_event.work_date is distinct from v_approval.protected_work_date then return null; end if;
  select g.* into v_generation from public.weekly_exceptional_pay_generations g
    where g.id=p_generation_id and g.family_id=v_approval.pay_target_family_id;
  if not found or v_generation.lifecycle_state not in ('PUBLISHED','SUPERSEDED')
     or v_generation.published_at_utc is null or not isfinite(v_generation.published_at_utc)
     or private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_COMPLETE_VECTOR_V1',
       v_generation.complete_next_vector_json) is distinct from v_generation.complete_next_vector_hash
     or v_generation.complete_next_vector_json->'target_snapshot' is distinct from v_approval.approved_target_pay_components_json
     or not exists(select 1 from public.weekly_exceptional_pay_target_events e
       where e.family_id=v_approval.pay_target_family_id and e.approval_id=v_approval.id
         and e.financial_generation_id=v_generation.id
         and e.complete_next_family_vector_fingerprint=v_generation.complete_next_vector_hash)
     or exists(select 1 from private.weekly_source_local_protected_decision_receipts r
       where r.generation_id=v_generation.id) then return null; end if;
  select count(*) into v_count from public.weekly_exceptional_c1_publication_requests r
    where r.generation_id=v_generation.id;
  if v_count<>1 then return null; end if;
  select r.* into v_request from public.weekly_exceptional_c1_publication_requests r
    where r.generation_id=v_generation.id;
  if v_request.family_id is distinct from v_approval.pay_target_family_id
     or v_request.orchestration_run_id is distinct from v_approval.creation_orchestration_run_id
     or v_request.candidate_id is distinct from v_approval.candidate_id
     or v_request.contract_id is distinct from v_approval.contract_id
     or v_request.week_ending_date is distinct from v_approval.week_ending
     or v_request.agency_id is distinct from (select f.agency_id
       from public.weekly_exceptional_pay_target_families f where f.id=v_approval.pay_target_family_id)
     or v_request.state is distinct from 'PUBLISHED' or v_request.completed_at_utc is null
     or not isfinite(v_request.completed_at_utc)
     or v_request.c1_publication_id is null or v_request.c1_head_revision is null or v_request.c1_head_revision<1
     or v_request.c1_receipt_sha256 is null
     or v_generation.result_hash is distinct from v_request.c1_receipt_sha256 then return null; end if;
  -- The request retains its genuine publication-time physical root. A later
  -- lawful rotation does not invalidate that accepted same raw booking family.
  -- Point-read the pinned rows; never rank arbitrary historical alternatives.
  select t.* into v_publication_root from public.timesheets t
    where t.timesheet_id=v_request.root_timesheet_id;
  if not found or v_publication_root.booking_id is distinct from v_scope->>'family_booking_id'
     or v_publication_root.contract_id is distinct from v_approval.contract_id
     or v_publication_root.week_ending_date is distinct from v_approval.week_ending
     or v_publication_root.sheet_scope is distinct from 'WEEKLY'
     or v_publication_root.line_type is distinct from 'HOURS'
     or v_publication_root.is_adjustment is distinct from false
     or v_publication_root.version is null or v_publication_root.version<1 then return null; end if;
  select f.* into v_publication_fin from public.timesheets_financials f
    where f.id=v_request.financial_row_id;
  if not found or v_publication_fin.timesheet_id is distinct from v_publication_root.timesheet_id
     or private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_SOURCE_PROPOSAL_V1',
       v_event.source_proposal_snapshot_json) is distinct from v_event.source_proposal_hash
     -- Financial and physical Timesheet versions are separate domains. Prove
     -- the original stage owner's fixed-source seal, not an invented equality.
     or private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_FIXED_SOURCE_STATE_V1',
       jsonb_build_object('source_cycle_id',v_approval.source_cycle_id,
         'comparison_revision_id',v_approval.comparison_revision_id,
         'final_revision_id',v_approval.final_revision_id,
         'source_proposal_hash',encode(v_event.source_proposal_hash,'hex'),
         'financial_row_id',v_publication_fin.id,
         'financial_timesheet_version',v_publication_fin.timesheet_version))
       is distinct from v_generation.fixed_target_source_state_fingerprint then return null; end if;
  select r.* into v_run from public.weekly_exceptional_orchestration_runs r where r.id=v_request.orchestration_run_id;
  if not found or v_run.family_id is distinct from v_approval.pay_target_family_id
     or v_run.requested_by_user_id is distinct from v_approval.approved_by_user_id
     or v_run.state is distinct from 'COMPLETE' or v_run.completed_at_utc is null
     or not isfinite(v_run.completed_at_utc)
     or v_run.after_state_fingerprint is distinct from v_request.c1_receipt_sha256 then return null; end if;
  select c.* into v_checkpoint from public.weekly_exceptional_c1_publication_checkpoints c
    where c.publication_request_id=v_request.id order by c.checkpoint_sequence desc limit 1;
  if not found or v_checkpoint.phase is distinct from 'PUBLISH'
     or upper(v_checkpoint.result_json->>'status') is distinct from 'PUBLISHED'
     or v_checkpoint.result_json->'has_more' is distinct from 'false'::jsonb
     or v_checkpoint.result_json is distinct from v_request.typed_result_json
     or (v_checkpoint.result_json->>'publication_id')::uuid is distinct from v_request.c1_publication_id
     or (v_checkpoint.result_json->>'head_revision')::bigint is distinct from v_request.c1_head_revision
     or private.weekly_exceptional_hex_sha256_v1(v_checkpoint.result_json->>'request_sha256')
       is distinct from v_request.request_sha256
     or private.weekly_exceptional_hex_sha256_v1(v_checkpoint.result_json->>'receipt_sha256')
       is distinct from v_request.c1_receipt_sha256
     or (v_request.c1_operation_id is not null
       and (v_checkpoint.result_json->>'operation_id')::uuid is distinct from v_request.c1_operation_id) then return null; end if;
  v_after_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_C1_COMPLETE_AFTER_V1',
    jsonb_build_object('family_id',v_request.family_id,'generation_id',v_generation.id,
      'target_vector_hash',encode(v_generation.complete_next_vector_hash,'hex'),
      'c1_publication_id',v_request.c1_publication_id,'c1_head_revision',v_request.c1_head_revision::text,
      'c1_receipt_sha256',encode(v_request.c1_receipt_sha256,'hex')));
  select count(*) into v_count from public.weekly_exceptional_orchestration_steps s
    where s.orchestration_run_id=v_run.id and s.step_kind='COMPLETE_C1_PUBLICATION';
  if v_count<>1 then return null; end if;
  select s.* into v_step from public.weekly_exceptional_orchestration_steps s
    where s.orchestration_run_id=v_run.id and s.step_kind='COMPLETE_C1_PUBLICATION';
  if v_step.outcome is distinct from 'COMPLETE' or v_step.completed_at_utc is null
     or not isfinite(v_step.completed_at_utc)
     or v_step.allowlisted_owner_name is distinct from 'C1.weekly_source_publish_c1'
     or v_step.allowlisted_owner_signature is distinct from 'sealed C1 V1'
     or v_step.bounded_request_hash is distinct from v_request.request_sha256
     or v_step.bounded_owner_response_json is distinct from v_request.typed_result_json
     or v_step.owner_response_hash is distinct from v_request.c1_receipt_sha256
     or v_step.after_state_fingerprint is distinct from v_after_hash then return null; end if;
  return jsonb_build_object('lane','RETAINED_C1','publication_request_id',v_request.id,
    'orchestration_run_id',v_run.id,'generation_id',v_generation.id,
    'request_sha256',encode(v_request.request_sha256,'hex'),'c1_publication_id',v_request.c1_publication_id,
    'c1_head_revision',v_request.c1_head_revision::text,'c1_receipt_sha256',encode(v_request.c1_receipt_sha256,'hex'),
    'receipt_state','PUBLISHED');
end;
$function$;
alter function private.weekly_source_pay_query_c1_receipt_v1(uuid,uuid,uuid) owner to postgres;
revoke all on function private.weekly_source_pay_query_c1_receipt_v1(uuid,uuid,uuid)
  from public,anon,authenticated,service_role;

commit;
