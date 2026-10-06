-- Repeatable CloudTMS function/view authority: weekly_source_pay_query_next_receipt_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- Retained Direct NEXT acceptance only: read immutable publication authority,
-- never enrol a job or calculate, apply, settle or recover a payment.
create or replace function private.weekly_source_pay_query_next_receipt_v1(
  p_root_timesheet_id uuid,p_approval_id uuid,p_generation_id uuid
) returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_scope jsonb; v_receipts jsonb;
begin
  if p_root_timesheet_id is null or p_approval_id is null or p_generation_id is null then
    raise exception 'WEEKLY_SOURCE_PAY_QUERY_NEXT_RECEIPT_IDENTITY_REQUIRED' using errcode='22023'; end if;
  v_scope:=private.weekly_source_pay_query_scope_v1(p_root_timesheet_id);
  if v_scope is null or v_scope->>'target_family_id' is null then return null; end if;
  select jsonb_agg(jsonb_build_object(
    'lane','DIRECT_NEXT','orchestration_run_id',r.orchestration_run_id,
    'generation_id',r.generation_id,'work_id',r.work_id,'revision_id',r.revision_id,
    'command_id',r.command_id,'agency_sequence',r.agency_sequence::text,
    'accepted_family_bound_version',r.accepted_family_bound_version::text,
    'prepared_request_sha256',encode(r.prepared_request_sha256,'hex'),
    'publication_request_sha256',encode(r.publication_request_sha256,'hex'),
    'source_manifest_sha256',encode(r.source_manifest_sha256,'hex'),'receipt_state','COMPLETE'))
  into v_receipts
  from public.weekly_exceptional_payment_approvals a
  join private.bpay_next_protected_source_receipt r on r.approval_id=a.id and r.generation_id=p_generation_id
  join public.weekly_exceptional_pay_target_families f on f.id=r.family_id
  join public.weekly_exceptional_orchestration_runs o on o.id=r.orchestration_run_id
  join public.weekly_exceptional_pay_generations g on g.id=r.generation_id
  join private.bpay_next_work w on w.id=r.work_id
  join private.bpay_next_work_revision v on v.work_id=w.id and v.id=r.revision_id
  join private.bpay_next_publication p on p.command_id=r.command_id and p.work_id=w.id
    and p.revision_id=v.id and p.candidate_id=w.candidate_id
  join private.bpay_next_command c on c.id=p.command_id
  join private.bpay_next_command_member m on m.command_id=c.id and m.candidate_id=w.candidate_id
  join lateral (select e.* from public.weekly_exceptional_pay_family_events e
    where e.family_id=f.id and e.durable_work_event_id=a.work_event_id
    order by e.event_sequence desc limit 1) e on true
  where a.id=p_approval_id and f.id::text=v_scope->>'target_family_id'
    and a.pay_target_family_id=f.id and g.family_id=f.id and o.family_id=f.id
    and a.withdrawn_at_utc is null
    and a.candidate_id::text=v_scope->>'candidate_id' and a.client_id::text=v_scope->>'client_id'
    and a.contract_id::text=v_scope->>'contract_id'
    and a.week_ending=(v_scope->>'week_ending_date')::date
    and a.protected_work_date between a.week_ending-6 and a.week_ending
    and exists(select 1 from public.weekly_work_events d where d.id=a.work_event_id
      and d.candidate_id=a.candidate_id and d.client_id=a.client_id and d.work_date=a.protected_work_date)
    and e.state='WAIT' and e.evidence_approval_id=a.id and e.work_date=a.protected_work_date
    and e.office_actor_user_id=a.approved_by_user_id
    and e.start_at_local=a.protected_start_at_local and e.end_at_local=a.protected_end_at_local
    and e.break_minutes=a.protected_break_minutes
    and e.fixed_office_target_hash=a.signed_schedule_fact_hash
    and private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_SCHEDULE_V1',
      e.fixed_office_target_snapshot_json)=a.signed_schedule_fact_hash
    and a.creation_orchestration_run_id=o.id
    and o.requested_by_user_id=r.actor_user_id and a.approved_by_user_id=r.actor_user_id
    and o.request_fingerprint=r.prepared_request_sha256
    and o.state='COMPLETE' and o.completed_at_utc is not null and isfinite(o.completed_at_utc)
    and o.after_state_fingerprint=r.publication_request_sha256
    and g.lifecycle_state in ('PUBLISHED','SUPERSEDED')
    and g.published_at_utc is not null and isfinite(g.published_at_utc)
    and g.result_hash=r.publication_request_sha256
    and g.complete_next_vector_json=a.approved_target_pay_components_json
    and private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_COMPLETE_VECTOR_V1',
      g.complete_next_vector_json)=g.complete_next_vector_hash
    and g.fixed_target_source_state_fingerprint=r.source_manifest_sha256
    and e.source_proposal_hash=r.source_manifest_sha256
    and private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_SOURCE_PROPOSAL_V1',
      e.source_proposal_snapshot_json)=r.source_manifest_sha256
    and (select count(*) from public.weekly_exceptional_pay_target_events t
      where t.family_id=f.id and t.approval_id=a.id and t.financial_generation_id=g.id)=1
    and exists(select 1 from public.weekly_exceptional_pay_target_events t
      where t.family_id=f.id and t.approval_id=a.id and t.financial_generation_id=g.id
        and t.complete_next_family_vector_fingerprint=g.complete_next_vector_hash
        and t.fixed_target_component_snapshot=g.complete_next_vector_json
        and t.current_source_proposal_snapshot=e.source_proposal_snapshot_json)
    and r.accepted_family_bound_version>0 and r.accepted_family_bound_version<=f.bound_version
    and w.candidate_id=a.candidate_id and w.contract_id=a.contract_id
    and w.booking_id=v_scope->>'family_booking_id' and w.work_kind='SOURCE'
    and w.week_ending_date=a.week_ending and v.week_ending_date=w.week_ending_date
    and v.source_kind='PROTECTED' and v.source_event_id=a.id
    and v.source_head_id is null and v.financial_snapshot_id is null
    and v.approved_at_utc is not null and isfinite(v.approved_at_utc)
    and v.sealed_at_utc is not null and isfinite(v.sealed_at_utc)
    and v.approved_source_ex_vat=a.approved_target_gross
    -- Both exact retained physical identities must belong to the raw family;
    -- neither needs to remain current or equal today's canonical physical root.
    and exists(select 1 from public.timesheets t where t.timesheet_id=w.original_timesheet_id
      and t.booking_id=w.booking_id and t.contract_id=w.contract_id
      and t.week_ending_date=w.week_ending_date and t.sheet_scope='WEEKLY'
      and t.line_type='HOURS' and t.is_adjustment is false and t.version>=1)
    and exists(select 1 from public.timesheets t where t.timesheet_id=v.physical_timesheet_id
      and t.booking_id=w.booking_id and t.contract_id=w.contract_id
      and t.week_ending_date=w.week_ending_date and t.sheet_scope='WEEKLY'
      and t.line_type='HOURS' and t.is_adjustment is false and t.version=v.physical_timesheet_version)
    and p.revision_no=v.revision_no and c.agency_sequence=r.agency_sequence
    and c.command_kind='POSITION_APPLY' and c.expected_member_count=1 and m.member_no=1
    and c.sealed_at_utc is not null and isfinite(c.sealed_at_utc)
    and c.id=private.bpay_next_source_command_id_v1(a.id)
    -- A job is created by later enrolment, not by publication acceptance.
    and not exists(select 1 from private.bpay_next_job j where j.command_id=c.id
      and (j.candidate_id<>w.candidate_id or j.job_kind<>'POSITION_APPLY'
        or j.command_sequence<>c.agency_sequence or j.module_epoch<>c.module_epoch))
    and (select count(*) from public.weekly_exceptional_orchestration_steps s
      where s.orchestration_run_id=o.id and s.step_kind='PUBLISH_NEXT')=1
    and exists(select 1 from public.weekly_exceptional_orchestration_steps s
      where s.orchestration_run_id=o.id and s.step_kind='PUBLISH_NEXT'
        and s.outcome='COMPLETE' and s.completed_at_utc is not null and isfinite(s.completed_at_utc)
        and s.allowlisted_owner_name='public.weekly_exceptional_pay_publish_next_v1'
        and s.allowlisted_owner_signature='jsonb->jsonb'
        and s.bounded_request_hash=r.publication_request_sha256
        and s.owner_response_hash=r.publication_request_sha256
        and s.after_state_fingerprint=r.publication_request_sha256
        and s.before_state_fingerprint=o.before_state_fingerprint
        and s.bounded_owner_response_json=private.bpay_next_protected_receipt_json_v1(o.id,false));
  if jsonb_array_length(v_receipts) is distinct from 1 then return null; end if;
  return v_receipts->0;
end;
$function$;
alter function private.weekly_source_pay_query_next_receipt_v1(uuid,uuid,uuid) owner to postgres;
revoke all on function private.weekly_source_pay_query_next_receipt_v1(uuid,uuid,uuid)
  from public,anon,authenticated,service_role;

commit;
