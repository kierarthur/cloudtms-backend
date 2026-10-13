do $hr_auto_guard$ begin if current_database()<>'cloudtms_test_clone' or not exists(select 1 from private.cloudtms_database_identity where singleton and environment='TEST') or not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='LEGACY' and owner_epoch=1) or exists(select 1 from private.cloudtms_database_releases where status='APPLYING') then raise exception 'HR_AUTO_TEST_IDENTITY_REFUSED'; end if; if exists(select 1 from unnest(array['private.weekly_source_staged_source_documents_v1(uuid)','private.weekly_source_staged_publication_context_v1(uuid,uuid)','private.weekly_source_approved_detail_evidence_v1(uuid,jsonb)','private.weekly_source_pay_query_local_receipt_event_v1(uuid,uuid,uuid,uuid)','private.weekly_source_pay_query_c1_receipt_event_v1(uuid,uuid,uuid,uuid)','private.weekly_source_pay_query_next_receipt_event_v1(uuid,uuid,uuid,uuid)','private.weekly_source_staged_prior_wait_receipt_v1(uuid,uuid)','private.weekly_source_expense_authority_evidence_v1(uuid,jsonb)','private.weekly_source_approved_expense_evidence_v1(uuid,jsonb)','private.weekly_source_staged_entitlement_components_v1(uuid,uuid)','private.weekly_source_automatic_reconcile_delta_assert_v1(uuid,uuid)']) s where to_regprocedure(s) is not null) then raise exception 'HR_AUTO_NEW_HELPER_ALREADY_PRESENT'; end if; end; $hr_auto_guard$;
select current_database() physical_database, (select md5(string_agg(n.nspname||'.'||p.proname||'('||pg_get_function_identity_arguments(p.oid)||')'||coalesce(p.prosrc,'')||coalesce(p.probin,'')||p.proowner::text||coalesce(p.proacl::text,'')||coalesce(p.proconfig::text,'')||p.prolang::text||p.prosecdef::text||p.provolatile::text,'|' order by p.oid)) from pg_proc p join pg_namespace n on n.oid=p.pronamespace where n.nspname in('public','private')) functions_hash, exists(select 1 from private.cloudtms_database_identity where singleton and environment='TEST') identity_test, (select active_owner||':'||owner_epoch::text from private.bpay_next_module_control where id=1) module, (select count(*) from unnest(array['private.weekly_source_staged_source_documents_v1(uuid)','private.weekly_source_staged_publication_context_v1(uuid,uuid)','private.weekly_source_approved_detail_evidence_v1(uuid,jsonb)','private.weekly_source_pay_query_local_receipt_event_v1(uuid,uuid,uuid,uuid)','private.weekly_source_pay_query_c1_receipt_event_v1(uuid,uuid,uuid,uuid)','private.weekly_source_pay_query_next_receipt_event_v1(uuid,uuid,uuid,uuid)','private.weekly_source_staged_prior_wait_receipt_v1(uuid,uuid)','private.weekly_source_expense_authority_evidence_v1(uuid,jsonb)','private.weekly_source_approved_expense_evidence_v1(uuid,jsonb)','private.weekly_source_staged_entitlement_components_v1(uuid,uuid)','private.weekly_source_automatic_reconcile_delta_assert_v1(uuid,uuid)']) s where to_regprocedure(s) is not null) automatic_helper_count;
create or replace function private.weekly_source_bulk_source_cas_assert_v1(p_cycle_id uuid,p_client_ids uuid[])
returns jsonb language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_ready boolean; v_cas jsonb; v_clients uuid[];
begin
  if p_client_ids is null or array_position(p_client_ids,null::uuid) is not null then
    raise exception 'WEEKLY_SOURCE_BULK_SOURCE_CAS_INVALID' using errcode='22023'; end if;
  select coalesce(array_agg(distinct item order by item),'{}'::uuid[]) into v_clients from unnest(p_client_ids) item;
  if to_regprocedure('private.weekly_source_bulk_producer_ready_v1(uuid)') is null
    or to_regprocedure('private.weekly_source_bulk_source_cas_v1(uuid,uuid[])') is null then
    raise exception 'WEEKLY_SOURCE_BULK_PRODUCER_UNAVAILABLE' using errcode='55000'; end if;
  execute 'select private.weekly_source_bulk_producer_ready_v1($1)' into v_ready using p_cycle_id;
  if v_ready is distinct from true then raise exception 'WEEKLY_SOURCE_BULK_PRODUCER_UNAVAILABLE' using errcode='55000'; end if;
  execute 'select private.weekly_source_bulk_source_cas_v1($1,$2)' into v_cas using p_cycle_id,v_clients;
  if not private.weekly_exceptional_json_keys_exact_v1(v_cas,array['schema_version','source_cycle_id','client_ids','source_cas_sha256'])
    or not(v_cas ?& array['schema_version','source_cycle_id','client_ids','source_cas_sha256'])
    or v_cas->>'schema_version' is distinct from 'WEEKLY_SOURCE_BULK_SOURCE_CAS_V1'
    or v_cas->>'source_cycle_id' is distinct from p_cycle_id::text
    or v_cas->'client_ids' is distinct from to_jsonb(v_clients)
    or coalesce(v_cas->>'source_cas_sha256','') !~ '^[0-9a-f]{64}$' then
    raise exception 'WEEKLY_SOURCE_BULK_SOURCE_CAS_INVALID' using errcode='55000'; end if;
  return v_cas;
end;
$function$;
alter function private.weekly_source_bulk_source_cas_assert_v1(uuid,uuid[]) owner to CURRENT_USER;
revoke all on function private.weekly_source_bulk_source_cas_assert_v1(uuid,uuid[]) from public,anon,authenticated,service_role;
create or replace function private.weekly_source_automatic_reconcile_capability_assert_v1(p_cycle_id uuid)
returns void language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_ready boolean;
begin
  if to_regprocedure('private.weekly_source_automatic_reconcile_producer_ready_v1(uuid)') is null
    or to_regprocedure('private.weekly_source_automatic_reconcile_delta_assert_v1(uuid,uuid)') is null then
    raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_DELTA_UNAVAILABLE' using errcode='55000'; end if;
  execute 'select private.weekly_source_automatic_reconcile_producer_ready_v1($1)' into v_ready using p_cycle_id;
  if v_ready is distinct from true then
    raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_DELTA_UNAVAILABLE' using errcode='55000'; end if;
end;
$function$;
alter function private.weekly_source_automatic_reconcile_capability_assert_v1(uuid) owner to CURRENT_USER;
revoke all on function private.weekly_source_automatic_reconcile_capability_assert_v1(uuid) from public,anon,authenticated,service_role;
-- Source-only proposal. No receipt, store, calculator, readiness or exposure.
-- Decode only the actual immutable native C1 records and sealed parts. Their
-- transport UUIDs are provenance, never native I1 entitlement component IDs.
create or replace function private.weekly_source_staged_source_documents_v1(p_request_id uuid)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare r public.weekly_exceptional_c1_publication_requests%rowtype; s record;
  v_payload bytea; v_document jsonb; v_count integer; v_bytes bigint; v_parts integer;
  v_result jsonb:='[]'::jsonb; v_total_bytes bigint:=0; v_ordinal integer:=0;
begin
  select * into strict r from public.weekly_exceptional_c1_publication_requests where id=p_request_id;
  select count(*) into v_count from public.weekly_exceptional_c1_source_records where publication_request_id=r.id;
  if v_count is distinct from r.expected_source_count or v_count<1 then
    raise exception 'WEEKLY_SOURCE_STAGED_SOURCE_UNAVAILABLE' using errcode='55000'; end if;
  for s in select source.* from public.weekly_exceptional_c1_source_records source
    where source.publication_request_id=r.id order by source.source_ordinal loop
    v_ordinal:=v_ordinal+1;
    if s.source_ordinal is distinct from v_ordinal or s.root_timesheet_id is distinct from r.root_timesheet_id
      or s.candidate_id is distinct from r.candidate_id or s.contract_id is distinct from r.contract_id
      or octet_length(s.source_document_sha256) is distinct from 32 or octet_length(s.source_row_sha256) is distinct from 32
      or s.payload_bytes not between 1 and 16384 or s.part_count not between 1 and 6 then
      raise exception 'WEEKLY_SOURCE_STAGED_SOURCE_UNAVAILABLE' using errcode='55000'; end if;
    select count(*),coalesce(sum(octet_length(part.payload_utf8)),0),
      decode(coalesce(string_agg(encode(part.payload_utf8,'hex'),'' order by part.part_ordinal),''),'hex')
      into v_parts,v_bytes,v_payload from public.weekly_exceptional_c1_source_parts part
      where part.publication_request_id=r.id and part.source_record_id=s.id;
    if v_parts is distinct from s.part_count or v_bytes is distinct from s.payload_bytes::bigint
      or exists(select 1 from public.weekly_exceptional_c1_source_parts part
        where part.publication_request_id=r.id and part.source_record_id=s.id
          and (part.source_ordinal is distinct from s.source_ordinal or part.part_ordinal not between 1 and s.part_count
            or octet_length(part.payload_utf8) not between 1 and 3072
            or extensions.digest(part.payload_utf8,'sha256') is distinct from part.fragment_sha256))
      or exists(select 1 from public.weekly_exceptional_c1_source_parts part
        where part.publication_request_id=r.id and part.source_record_id=s.id group by part.part_ordinal having count(*)<>1) then
      raise exception 'WEEKLY_SOURCE_STAGED_SOURCE_UNAVAILABLE' using errcode='55000'; end if;
    v_document:=convert_from(v_payload,'UTF8')::jsonb;
    if jsonb_typeof(v_document) is distinct from 'object'
      or v_document->>'contract' is distinct from ('C1_'||s.authority_kind||'_V1')
      or v_document->>'source_id' is distinct from s.id::text
      or v_document->>'source_system' is distinct from s.source_system
      or v_document->>'external_identity' is distinct from s.external_identity
      or v_document->>'external_revision' is distinct from s.external_revision
      or v_document->>'root_timesheet_id' is distinct from r.root_timesheet_id::text
      or v_document->>'candidate_id' is distinct from r.candidate_id::text
      or v_document->>'contract_id' is distinct from r.contract_id::text
      or v_document->>'week_ending_date' is distinct from r.week_ending_date::text
      or v_document->>'document_sha256' is distinct from encode(s.source_document_sha256,'hex') then
      raise exception 'WEEKLY_SOURCE_STAGED_SOURCE_UNAVAILABLE' using errcode='55000'; end if;
    v_total_bytes:=v_total_bytes+v_bytes;
    v_result:=v_result||jsonb_build_array(jsonb_build_object('source_id',s.id,'source_ordinal',s.source_ordinal,
      'authority_kind',s.authority_kind,'source_system',s.source_system,'external_identity',s.external_identity,
      'external_revision',s.external_revision,'source_document_sha256',encode(s.source_document_sha256,'hex'),
      'source_row_sha256',encode(s.source_row_sha256,'hex'),'payload_bytes',s.payload_bytes,'part_count',s.part_count,
      'work_date',s.work_date,'document',v_document));
  end loop;
  if v_total_bytes is distinct from r.expected_payload_bytes::bigint
    or exists(select 1 from public.weekly_exceptional_c1_source_parts part where part.publication_request_id=r.id
      and not exists(select 1 from public.weekly_exceptional_c1_source_records source
        where source.id=part.source_record_id and source.publication_request_id=r.id)) then
    raise exception 'WEEKLY_SOURCE_STAGED_SOURCE_UNAVAILABLE' using errcode='55000'; end if;
  return v_result;
exception when no_data_found or too_many_rows or invalid_text_representation or character_not_in_repertoire then
  raise exception 'WEEKLY_SOURCE_STAGED_SOURCE_UNAVAILABLE' using errcode='55000';
end;
$function$;
alter function private.weekly_source_staged_source_documents_v1(uuid) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_staged_source_documents_v1(uuid) from public,anon,authenticated,service_role;

-- Common actual stage proof extracted from Local v2, usable before a real
-- PREPARING receipt exists. It does not create or pretend that receipt exists.
-- Normalise only the fully proved own K -> K+1 stage increment; every other
-- byte of the complete current-vs-sealed native I1 inventory must match.
create or replace function private.weekly_source_staged_publication_context_v1(p_request_id uuid,p_actor_id uuid)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare r public.weekly_exceptional_c1_publication_requests%rowtype;
  g public.weekly_exceptional_pay_generations%rowtype; prior public.weekly_exceptional_pay_generations%rowtype;
  family public.weekly_exceptional_pay_target_families%rowtype; run public.weekly_exceptional_orchestration_runs%rowtype;
  a public.weekly_exceptional_payment_approvals%rowtype; event public.weekly_exceptional_pay_family_events%rowtype;
  target public.weekly_exceptional_pay_target_events%rowtype; fin public.timesheets_financials%rowtype;
  stage public.weekly_exceptional_orchestration_steps%rowtype; local_receipt private.weekly_source_local_protected_decision_receipts%rowtype;
  v_scope jsonb; v_inventory jsonb; v_before jsonb; v_snapshot jsonb; v_context jsonb; v_documents jsonb;
  v_before_bound bigint; v_stage_hash bytea; v_policy_hash bytea; v_qualification jsonb; v_count integer;
begin
  if p_request_id is null or p_actor_id is null then
    raise exception 'WEEKLY_SOURCE_STAGED_OWNERSHIP_UNAVAILABLE' using errcode='22023'; end if;
  select * into strict r from public.weekly_exceptional_c1_publication_requests where id=p_request_id;
  select * into strict g from public.weekly_exceptional_pay_generations where id=r.generation_id and family_id=r.family_id;
  select * into strict family from public.weekly_exceptional_pay_target_families where id=r.family_id;
  select * into strict run from public.weekly_exceptional_orchestration_runs where id=r.orchestration_run_id and family_id=family.id;
  select * into strict a from public.weekly_exceptional_payment_approvals
    where creation_orchestration_run_id=run.id and pay_target_family_id=family.id;
  v_scope:=private.weekly_source_pay_query_scope_v1(r.root_timesheet_id);
  if v_scope is null or r.state is distinct from 'READY' or g.lifecycle_state is distinct from 'PENDING_C1'
    or run.state is distinct from 'RUNNING' or run.requested_by_user_id is distinct from p_actor_id
    or a.approved_by_user_id is distinct from p_actor_id or a.withdrawn_at_utc is not null
    or r.agency_id is distinct from family.agency_id or r.candidate_id is distinct from family.candidate_id
    or r.contract_id is distinct from family.contract_id or r.root_timesheet_id is distinct from family.root_timesheet_id
    or r.week_ending_date is distinct from family.week_ending_date or family.root_family_booking_id is distinct from v_scope->>'family_booking_id'
    or v_scope->>'target_family_id' is distinct from family.id::text or v_scope->>'candidate_id' is distinct from family.candidate_id::text
    or v_scope->>'contract_id' is distinct from family.contract_id::text or v_scope->>'client_id' is distinct from a.client_id::text
    or v_scope->>'week_ending_date' is distinct from family.week_ending_date::text
    or a.candidate_id is distinct from family.candidate_id or a.contract_id is distinct from family.contract_id
    or a.week_ending is distinct from family.week_ending_date
    or r.c1_operation_id is not null or r.c1_publication_id is not null or r.c1_head_revision is not null or r.c1_receipt_sha256 is not null
    or family.c1_publication_state is distinct from 'PENDING'
    or family.current_generation_id is distinct from g.prior_generation_id
    or family.current_generation_number+1 is distinct from g.generation_number or r.request_sequence is distinct from g.generation_number::bigint
    or private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_COMPLETE_VECTOR_V1',g.complete_next_vector_json)
      is distinct from g.complete_next_vector_hash
    or private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_COMPLETE_VECTOR_V1',g.complete_prior_vector_json)
      is distinct from g.complete_prior_vector_hash then
    raise exception 'WEEKLY_SOURCE_STAGED_OWNERSHIP_UNAVAILABLE' using errcode='55000'; end if;
  -- Automatic reconciliation has a real published predecessor. An empty or
  -- initial unapproved family cannot invent an I1 before-position here.
  select * into strict prior from public.weekly_exceptional_pay_generations where id=g.prior_generation_id and family_id=family.id;
  if prior.lifecycle_state is distinct from 'PUBLISHED' or prior.published_at_utc is null or not isfinite(prior.published_at_utc)
    or prior.result_hash is null or prior.generation_number is distinct from family.current_generation_number
    or prior.complete_next_vector_hash is distinct from family.current_complete_target_vector_hash
    or prior.complete_next_vector_json is distinct from g.complete_prior_vector_json
    or prior.complete_next_vector_hash is distinct from g.prior_generation_hash then
    raise exception 'WEEKLY_SOURCE_STAGED_OWNERSHIP_UNAVAILABLE' using errcode='55000'; end if;
  select * into strict event from public.weekly_exceptional_pay_family_events e
    where e.family_id=family.id and e.evidence_approval_id=a.id and e.durable_work_event_id=a.work_event_id
      and not exists(select 1 from public.weekly_exceptional_pay_family_events newer where newer.family_id=e.family_id
        and newer.durable_work_event_id=e.durable_work_event_id and newer.event_sequence>e.event_sequence);
  if event.office_actor_user_id is distinct from p_actor_id or event.work_date is distinct from a.protected_work_date
    or (event.start_at_local,event.end_at_local,event.break_minutes) is distinct from
      (a.protected_start_at_local,a.protected_end_at_local,a.protected_break_minutes)
    or private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_SOURCE_PROPOSAL_V1',event.source_proposal_snapshot_json)
      is distinct from event.source_proposal_hash
    or private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_SCHEDULE_V1',event.fixed_office_target_snapshot_json)
      is distinct from a.signed_schedule_fact_hash or event.fixed_office_target_hash is distinct from a.signed_schedule_fact_hash
    or not exists(select 1 from public.weekly_work_events w where w.id=a.work_event_id and w.candidate_id=family.candidate_id
      and w.client_id=a.client_id and w.work_date=a.protected_work_date) then
    raise exception 'WEEKLY_SOURCE_STAGED_OWNERSHIP_UNAVAILABLE' using errcode='55000'; end if;
  select count(*) into v_count from public.weekly_exceptional_pay_target_events t
    where t.family_id=family.id and t.approval_id=a.id and t.financial_generation_id=g.id
      and t.complete_prior_family_vector_fingerprint=g.complete_prior_vector_hash
      and t.complete_next_family_vector_fingerprint=g.complete_next_vector_hash
      and t.fixed_target_component_snapshot=a.approved_target_pay_components_json;
  if v_count<>1 then raise exception 'WEEKLY_SOURCE_STAGED_OWNERSHIP_UNAVAILABLE' using errcode='55000'; end if;
  select * into strict target from public.weekly_exceptional_pay_target_events
    where financial_generation_id=g.id and family_id=family.id;
  select * into strict fin from public.timesheets_financials where id=r.financial_row_id and timesheet_id=r.root_timesheet_id;
  if not exists(select 1 from public.weekly_exceptional_pay_family_events predecessor
      where predecessor.family_id=event.family_id and predecessor.event_sequence=event.event_sequence-1
        and predecessor.event_hash=event.prior_event_hash)
    or event.event_hash is distinct from private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_FAMILY_EVENT_V1',jsonb_build_object(
      'family_id',family.id,'event_sequence',event.event_sequence,'work_event_id',a.work_event_id,'approval_id',a.id,
      'schedule',event.fixed_office_target_snapshot_json,'source_proposal_hash',encode(event.source_proposal_hash,'hex'),
      'target_vector_hash',encode(g.complete_next_vector_hash,'hex'),'prior_event_hash',encode(event.prior_event_hash,'hex')))
    or target.event_hash is distinct from private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_TARGET_EVENT_V1',jsonb_build_object(
      'family_id',family.id,'event_sequence',target.event_sequence,'approval_id',a.id,'generation_id',g.id,
      'prior_vector_hash',encode(g.complete_prior_vector_hash,'hex'),'next_vector_hash',encode(g.complete_next_vector_hash,'hex'),
      'reason',g.reason,'actor_user_id',p_actor_id))
    or g.fixed_target_source_state_fingerprint is distinct from private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_FIXED_SOURCE_STATE_V1',
      jsonb_build_object('source_cycle_id',a.source_cycle_id,'comparison_revision_id',a.comparison_revision_id,
        'final_revision_id',a.final_revision_id,'source_proposal_hash',encode(event.source_proposal_hash,'hex'),
        'financial_row_id',fin.id,'financial_timesheet_version',fin.timesheet_version))
    or a.approval_hash is distinct from private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_APPROVAL_V1',jsonb_build_object(
      'family_id',family.id,'work_event_id',a.work_event_id,'candidate_id',family.candidate_id,'client_id',a.client_id,
      'contract_id',family.contract_id,'week_ending_date',family.week_ending_date,'schedule_hash',encode(a.signed_schedule_fact_hash,'hex'),
      'evidence_timesheet_id',a.evidence_timesheet_id,'signed_hash',encode(a.signed_submission_hash,'hex'),
      'issue_ids_hash',encode(a.contributing_issue_episode_ids_hash,'hex'),
      'rate_policy_hash',encode(a.contract_rate_policy_source_fingerprint,'hex'),'target_vector_hash',encode(g.complete_next_vector_hash,'hex'),
      'actor_user_id',p_actor_id,'reason',a.approval_reason)) then
    raise exception 'WEEKLY_SOURCE_STAGED_OWNERSHIP_UNAVAILABLE' using errcode='55000'; end if;
  v_snapshot:=g.complete_next_vector_json#>'{target_snapshot,tsfin_snapshot_json}';
  if jsonb_typeof(v_snapshot) is distinct from 'object' or v_snapshot is distinct from a.approved_target_pay_components_json->'tsfin_snapshot_json'
    or a.approved_target_pay_components_json is distinct from g.complete_next_vector_json->'target_snapshot'
    or v_snapshot->>'timesheet_id' is distinct from r.root_timesheet_id::text
    or v_snapshot->>'timesheet_version' is distinct from v_scope->>'root_version' then
    raise exception 'WEEKLY_SOURCE_STAGED_OWNERSHIP_UNAVAILABLE' using errcode='55000'; end if;
  v_before:=g.complete_next_vector_json->'prior_effective_inventory';
  if v_before->>'ok' is distinct from 'true' or v_before#>>'{approval_basis,coverage_complete}' is distinct from 'true'
    or coalesce(v_before#>>'{approval_basis,scope,family_bound_version}','')!~'^(0|[1-9][0-9]*)$'
    or v_before#>>'{approval_basis,scope,target_family_id}' is distinct from family.id::text then
    raise exception 'WEEKLY_SOURCE_STAGED_OWNERSHIP_UNAVAILABLE' using errcode='55000'; end if;
  v_before_bound:=(v_before#>>'{approval_basis,scope,family_bound_version}')::bigint;
  if g.complete_next_vector_json#>>'{protected_component_identity_basis,schema_version}' is distinct from 'WEEKLY_PROTECTED_COMPONENT_MANIFEST_V1'
    or g.complete_next_vector_json#>>'{protected_component_identity_basis,family_id}' is distinct from family.id::text
    or g.complete_next_vector_json#>>'{protected_component_identity_basis,root_timesheet_id}' is distinct from r.root_timesheet_id::text
    or g.complete_next_vector_json#>>'{protected_component_identity_basis,contract_id}' is distinct from family.contract_id::text
    or g.complete_next_vector_json#>>'{protected_component_identity_basis,candidate_id}' is distinct from family.candidate_id::text
    or g.complete_next_vector_json#>>'{protected_component_identity_basis,accepted_schedule_sha256}' is distinct from
      encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_COMPONENT_ACCEPTED_SCHEDULE_V1',
        g.complete_next_vector_json#>'{target_snapshot,actual_schedule_json}'),'hex')
    or g.complete_next_vector_json#>>'{protected_component_identity_basis,calculated_segments_sha256}' is distinct from
      encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_COMPONENT_CALCULATED_SEGMENTS_V1',v_snapshot#>'{invoice_breakdown_json,segments}'),'hex')
    or g.complete_next_vector_json#>>'{protected_component_identity_basis,approved_components_sha256}' is distinct from
      encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_COMPONENT_APPROVED_COMPONENTS_V1',v_before->'components'),'hex') then
    raise exception 'WEEKLY_SOURCE_STAGED_OWNERSHIP_UNAVAILABLE' using errcode='55000'; end if;
  select * into strict stage from public.weekly_exceptional_orchestration_steps
    where orchestration_run_id=run.id and step_kind='STAGE_C1_REQUEST';
  v_stage_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_C1_STAGED_STATE_V1',jsonb_build_object(
    'family_id',family.id,'generation_id',g.id,'publication_request_id',r.id,'request_sha256',encode(r.request_sha256,'hex'),
    'new_bound_version',v_before_bound+1));
  if stage.outcome is distinct from 'COMPLETE' or stage.completed_at_utc is null or not isfinite(stage.completed_at_utc)
    or stage.allowlisted_owner_name is distinct from 'public.weekly_exceptional_pay_stage_c1_request_v1'
    or stage.allowlisted_owner_signature is distinct from 'jsonb->jsonb'
    or stage.before_state_fingerprint is distinct from run.before_state_fingerprint
    or octet_length(stage.bounded_request_hash) is distinct from 32
    or stage.after_state_fingerprint is distinct from v_stage_hash or stage.owner_response_hash is distinct from v_stage_hash
    or stage.bounded_owner_response_json is distinct from jsonb_build_object('publication_request_id',r.id,'generation_id',g.id,
      'request_sha256',encode(r.request_sha256,'hex')) or family.bound_version is distinct from v_before_bound+1 then
    raise exception 'WEEKLY_SOURCE_STAGED_OWNERSHIP_UNAVAILABLE' using errcode='55000'; end if;
  v_inventory:=private.weekly_source_effective_inventory_v1(r.root_timesheet_id);
  v_inventory:=jsonb_set(v_inventory,'{approval_basis,scope,family_bound_version}',
    v_before#>'{approval_basis,scope,family_bound_version}',false);
  if v_inventory is distinct from v_before
    or v_inventory#>>'{approval_basis,scope,root_timesheet_id}' is distinct from r.root_timesheet_id::text
    or v_inventory#>>'{approval_basis,scope,family_booking_id}' is distinct from v_scope->>'family_booking_id'
    or v_inventory#>>'{approval_basis,scope,root_version}' is distinct from v_scope->>'root_version'
    or v_inventory#>>'{approval_basis,scope,candidate_id}' is distinct from v_scope->>'candidate_id'
    or v_inventory#>>'{approval_basis,scope,client_id}' is distinct from v_scope->>'client_id'
    or v_inventory#>>'{approval_basis,scope,contract_id}' is distinct from v_scope->>'contract_id'
    or v_inventory#>>'{approval_basis,scope,week_ending_date}' is distinct from v_scope->>'week_ending_date' then
    raise exception 'WEEKLY_SOURCE_STAGED_BEFORE_POSITION_CHANGED' using errcode='40001'; end if;
  v_context:=private.weekly_source_protected_final_source_context_v1(family.id,a.source_cycle_id,a.work_event_id);
  if v_context->'source_proposal' is distinct from event.source_proposal_snapshot_json then
    raise exception 'WEEKLY_SOURCE_STAGED_SOURCE_CHANGED' using errcode='40001'; end if;
  select private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_RATE_POLICY_V1',jsonb_build_object(
    'contract_id',family.contract_id,'rates_json',contract.rates_json,
    'policy',private._weekly_source_effective_policy_v1(a.client_id,family.contract_id,a.protected_work_date),
    'classification',event.rate_classification_json)) into strict v_policy_hash
    from public.contracts contract where contract.id=family.contract_id;
  if v_policy_hash is distinct from a.contract_rate_policy_source_fingerprint then
    raise exception 'WEEKLY_SOURCE_STAGED_RATE_POLICY_CHANGED' using errcode='40001'; end if;
  -- A receipt is optional here. If one exists, only this real transaction's
  -- matching PREPARING row can coexist with this unaccepted stage.
  select * into local_receipt from private.weekly_source_local_protected_decision_receipts where publication_request_id=r.id;
  if found and (local_receipt.state is distinct from 'PREPARING' or local_receipt.preparing_transaction_id is distinct from pg_current_xact_id()
    or local_receipt.actor_user_id is distinct from p_actor_id or local_receipt.generation_id is distinct from g.id
    or local_receipt.family_id is distinct from family.id or local_receipt.root_timesheet_id is distinct from r.root_timesheet_id
    or local_receipt.request_sha256 is distinct from r.request_sha256 or local_receipt.prior_financial_id is distinct from r.financial_row_id) then
    raise exception 'WEEKLY_SOURCE_STAGED_OWNERSHIP_UNAVAILABLE' using errcode='55000'; end if;
  if exists(select 1 from public.weekly_exceptional_c1_publication_checkpoints where publication_request_id=r.id)
    or exists(select 1 from public.weekly_exceptional_c1_unknown_outcomes where publication_request_id=r.id) then
    raise exception 'WEEKLY_SOURCE_STAGED_OWNERSHIP_UNAVAILABLE' using errcode='55000'; end if;
  v_documents:=private.weekly_source_staged_source_documents_v1(r.id);
  if (select count(*) from jsonb_array_elements(v_documents) source where source->>'authority_kind'='ROOT_FINANCIAL')<>1
    or (select count(*) from jsonb_array_elements(v_documents) source where source->>'authority_kind'='PROVIDER')<>1
    or (select count(*) from jsonb_array_elements(v_documents) source where source->>'authority_kind'='OFFICE_APPROVAL')<>1
    or not exists(select 1 from jsonb_array_elements(v_documents) source where source->>'authority_kind'='CLIENT_SOURCE'
      and source#>>'{document,source_complete}'='true')
    or exists(select 1 from jsonb_array_elements(v_documents) source where source->>'authority_kind'='ROOT_FINANCIAL'
      and source#>>'{document,financial_row_id}' is distinct from r.financial_row_id::text) then
    raise exception 'WEEKLY_SOURCE_STAGED_SOURCE_UNAVAILABLE' using errcode='55000'; end if;
  v_qualification:=jsonb_build_object('schema_version','WEEKLY_SOURCE_STAGED_PUBLICATION_CONTEXT_V1',
    'publication_request_id',r.id,'generation_id',g.id,'family_id',family.id,'actor_user_id',p_actor_id,'scope',v_scope,
    'source_cycle_id',a.source_cycle_id,'work_event_id',a.work_event_id,'approval_id',a.id,'family_event_id',event.id,
    'family_event_sequence',event.event_sequence::text,'request_sha256',encode(r.request_sha256,'hex'),
    'stage_step_id',stage.id,'stage_step_request_sha256',encode(stage.bounded_request_hash,'hex'),
    'stage_step_sha256',encode(v_stage_hash,'hex'),'before_bound_version',v_before_bound::text,
    'staged_bound_version',(v_before_bound+1)::text,'prior_inventory',v_before,'target_snapshot',g.complete_next_vector_json->'target_snapshot',
    'complete_next_vector_sha256',encode(g.complete_next_vector_hash,'hex'),'policy_fingerprint',encode(v_policy_hash,'hex'),
    'source_proposal',event.source_proposal_snapshot_json,'next_source_segments',v_context->'next_source_segments','source_documents',v_documents);
  return v_qualification||jsonb_build_object('qualification_sha256',encode(private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_STAGED_PUBLICATION_CONTEXT_V1',v_qualification),'hex'));
exception when no_data_found or too_many_rows or invalid_text_representation or numeric_value_out_of_range then
  raise exception 'WEEKLY_SOURCE_STAGED_OWNERSHIP_UNAVAILABLE' using errcode='55000';
end;
$function$;
alter function private.weekly_source_staged_publication_context_v1(uuid,uuid) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_staged_publication_context_v1(uuid,uuid) from public,anon,authenticated,service_role;

-- Read the actual evidence already qualified by approval_basis_v2. This is
-- not a current-TSFIN fallback: the I1 origin chooses one exact approved row
-- or exact committed HEAD detail. The native domains and identities stay intact.
create or replace function private.weekly_source_approved_detail_evidence_v1(p_root_id uuid,p_inventory jsonb)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_basis jsonb:=p_inventory->'approval_basis'; v_origin jsonb:=p_inventory#>'{approval_basis,origin}';
  f public.timesheets_financials%rowtype; h public.weekly_source_entitlement_heads%rowtype;
  v_details jsonb; v_policy jsonb; v_detail jsonb; v_component jsonb; v_count integer;
begin
  if p_inventory->>'ok' is distinct from 'true' or v_basis->>'coverage_complete' is distinct from 'true'
    or v_basis#>>'{scope,root_timesheet_id}' is distinct from p_root_id::text
    or private.weekly_source_inventory_approval_basis_v2(p_root_id,p_inventory) is distinct from v_basis then
    raise exception 'WEEKLY_SOURCE_APPROVED_DETAIL_UNAVAILABLE' using errcode='55000'; end if;
  if v_origin->>'kind'='INITIAL_AUTHORISED_TSFIN_V1' then
    select * into strict f from public.timesheets_financials where id=(v_origin->>'financial_snapshot_id')::uuid
      and timesheet_id=p_root_id and authorised_at_utc is not null and timesheet_version=(v_basis#>>'{scope,root_version}')::integer;
    v_details:=jsonb_build_object('segments',f.invoice_breakdown_json->'segments','additional_units',f.additional_units_json,
      'actual_schedule',f.actual_schedule_json,'rate_source_refs',f.rate_source_refs_json);
    v_policy:=f.policy_snapshot_json;
    if private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_APPROVED_FINANCIAL_SNAPSHOT_V2',jsonb_build_object(
      'policy',v_policy,'details',v_details,'components',p_inventory->'components'))
      is distinct from decode(v_origin->>'financial_snapshot_digest','hex')
      or private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_APPROVED_POLICY_V2',v_policy)
        is distinct from decode(v_basis->>'policy_fingerprint','hex') then
      raise exception 'WEEKLY_SOURCE_APPROVED_DETAIL_UNAVAILABLE' using errcode='55000'; end if;
  elsif v_origin->>'kind'='COMMITTED_SOURCE_HEAD_V1' then
    select * into strict h from public.weekly_source_entitlement_heads where id=(v_origin->>'head_id')::uuid and state='COMMITTED_CURRENT';
    v_policy:=h.source_origin_json;v_details:='[]'::jsonb;
    for v_component in select value from jsonb_array_elements(p_inventory->'components') loop
      if v_component->>'component_kind'='WORKED_TIME' then
        if to_regclass('private.bpay_next_source_chosen_detail') is null then
          raise exception 'WEEKLY_SOURCE_APPROVED_DETAIL_UNAVAILABLE' using errcode='55000'; end if;
        execute 'select count(*),jsonb_agg(to_jsonb(d))->0 from private.bpay_next_source_chosen_detail d
          where d.head_id=$1 and d.component_id=$2 and d.decision_bundle_id=$3 and d.bundle_revision=$4'
          into v_count,v_detail using h.id,(v_component->>'component_id')::uuid,h.decision_bundle_id,h.bundle_revision;
        if v_count<>1 or v_detail->>'component_sha256' is distinct from ('\x'||(v_component->>'component_sha256'))
          or v_detail->>'detail_sha256' is distinct from ('\x'||encode(sha256(convert_to((v_detail->'detail_json')::text,'UTF8')),'hex')) then
          raise exception 'WEEKLY_SOURCE_APPROVED_DETAIL_UNAVAILABLE' using errcode='55000'; end if;
        v_details:=v_details||jsonb_build_array(jsonb_build_object('component_id',v_component->'component_id','detail',v_detail->'detail_json'));
      else
        v_details:=v_details||jsonb_build_array(jsonb_build_object('component_id',v_component->'component_id','detail',v_component-'component_sha256'));
      end if;
    end loop;
  else raise exception 'WEEKLY_SOURCE_APPROVED_DETAIL_UNAVAILABLE' using errcode='55000'; end if;
  if private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_APPROVED_DETAIL_V2',v_details)
    is distinct from decode(v_basis->>'detail_digest','hex') then
    raise exception 'WEEKLY_SOURCE_APPROVED_DETAIL_UNAVAILABLE' using errcode='55000'; end if;
  return jsonb_build_object('approval_basis',v_basis,'details',v_details,'policy',v_policy,
    'financial_rate_evidence',case when v_origin->>'kind'='INITIAL_AUTHORISED_TSFIN_V1' then
      jsonb_build_object('pay',jsonb_build_object('day',f.pay_day,'night',f.pay_night,'sat',f.pay_sat,'sun',f.pay_sun,'bh',f.pay_bh),
        'charge',jsonb_build_object('day',f.charge_day,'night',f.charge_night,'sat',f.charge_sat,'sun',f.charge_sun,'bh',f.charge_bh)) else null end);
exception when no_data_found or too_many_rows or invalid_text_representation then
  raise exception 'WEEKLY_SOURCE_APPROVED_DETAIL_UNAVAILABLE' using errcode='55000';
end;
$function$;
alter function private.weekly_source_approved_detail_evidence_v1(uuid,jsonb) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_approved_detail_evidence_v1(uuid,jsonb) from public,anon,authenticated,service_role;

-- Complete native receipt successors. Ordinary calls retain the latest-event path.
-- Explicit historical events are private and must be qualified as the exact own-stage predecessor.

create or replace function private.weekly_source_pay_query_local_receipt_event_v1(
  p_root_timesheet_id uuid,p_approval_id uuid,p_generation_id uuid,p_family_event_id uuid
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
begin
  if p_root_timesheet_id is null or p_approval_id is null or p_generation_id is null then
    raise exception 'WEEKLY_SOURCE_PAY_QUERY_LOCAL_RECEIPT_IDENTITY_REQUIRED' using errcode='22023';
  end if;
  v_scope:=private.weekly_source_pay_query_scope_v1(p_root_timesheet_id);
  if v_scope is null or v_scope->>'target_family_id' is null then return null; end if;
  select a.* into v_approval from public.weekly_exceptional_payment_approvals a
    where a.id=p_approval_id;
  if not found or v_approval.withdrawn_at_utc is not null
     or v_approval.pay_target_family_id::text is distinct from v_scope->>'target_family_id'
     or v_approval.candidate_id::text is distinct from v_scope->>'candidate_id'
     or v_approval.client_id::text is distinct from v_scope->>'client_id'
     or v_approval.contract_id::text is distinct from v_scope->>'contract_id'
     or to_char(v_approval.week_ending,'YYYY-MM-DD') is distinct from v_scope->>'week_ending_date'
     or v_approval.protected_work_date not between
        (v_scope->>'week_ending_date')::date-6 and (v_scope->>'week_ending_date')::date then return null; end if;
  if not exists(select 1 from public.weekly_work_events w
    where w.id=v_approval.work_event_id and w.candidate_id=v_approval.candidate_id
      and w.client_id=v_approval.client_id and w.work_date=v_approval.protected_work_date) then return null; end if;
  -- Latest actual event sequence, not a timestamp or a search through JSON.
  select e.* into v_event from public.weekly_exceptional_pay_family_events e
    where e.family_id=v_approval.pay_target_family_id
      and e.durable_work_event_id=v_approval.work_event_id
      and (p_family_event_id is null or e.id=p_family_event_id)
    order by e.event_sequence desc limit 1;
  if not found or v_event.evidence_approval_id is distinct from v_approval.id
     or v_event.state is distinct from 'WAIT'
     or v_event.work_date is distinct from v_approval.protected_work_date then return null; end if;
  select g.* into v_generation from public.weekly_exceptional_pay_generations g
    where g.id=p_generation_id and g.family_id=v_approval.pay_target_family_id;
  if not found or private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_COMPLETE_VECTOR_V1',
       v_generation.complete_next_vector_json) is distinct from v_generation.complete_next_vector_hash
     or not exists(select 1 from public.weekly_exceptional_pay_target_events t
       where t.family_id=v_approval.pay_target_family_id and t.approval_id=v_approval.id
         and t.financial_generation_id=v_generation.id
         and t.complete_next_family_vector_fingerprint=v_generation.complete_next_vector_hash) then return null; end if;
  select l.* into v_local from private.weekly_source_local_protected_decision_receipts l
    where l.generation_id=v_generation.id;
  if not found or v_local.family_id is distinct from v_approval.pay_target_family_id
     or v_local.actor_user_id is distinct from v_approval.approved_by_user_id
     or v_local.state not in ('COMPLETE','PENDING_FREEZE')
     or v_local.completed_at_utc is null or not isfinite(v_local.completed_at_utc) then return null; end if;
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
     or (v_local.approved_snapshot_json->>'timesheet_version')::numeric>2147483647 then return null; end if;
  v_publication_version:=(v_local.approved_snapshot_json->>'timesheet_version')::integer;
  if v_publication_root.version is distinct from v_publication_version then return null; end if;
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
     or v_request.c1_head_revision is not null or v_request.c1_receipt_sha256 is not null then return null; end if;
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
          (v_scope->>'family_bound_version')::numeric then return null; end if;
  v_snapshot:=v_generation.complete_next_vector_json#>'{target_snapshot,tsfin_snapshot_json}';
  if jsonb_typeof(v_snapshot) is distinct from 'object'
     or v_snapshot is distinct from v_approval.approved_target_pay_components_json->'tsfin_snapshot_json' then return null; end if;
  if v_local.state='COMPLETE' then
    -- A different sibling may have superseded this accepted generation. Its
    -- immutable published proof is still needed for this unchanged WAIT event.
    if v_generation.lifecycle_state not in ('PUBLISHED','SUPERSEDED')
       or v_generation.published_at_utc is null or not isfinite(v_generation.published_at_utc)
       or v_generation.result_hash is distinct from v_result_hash
       or v_local.result_json->>'outcome' is distinct from 'PUBLISHED'
       or v_local.result_json->>'state' is distinct from 'LIVE' then return null; end if;
  elsif v_generation.lifecycle_state is distinct from 'PENDING_C1'
     or v_generation.published_at_utc is not null or v_generation.result_hash is not null
     or v_local.result_json->>'outcome' is distinct from 'SAVED_PENDING_FREEZE'
     or v_local.result_json->>'state' is distinct from 'PENDING' then return null;
  end if;
  if v_local.result_json->>'authority'='UNAUTHORISED_TSFIN' then
    if v_local.state<>'COMPLETE'
       or v_local.result_json->'requires_first_authorisation' is distinct from 'true'::jsonb then return null; end if;
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
           and f.candidate_id=v_approval.candidate_id and f.client_id=v_approval.client_id) then return null; end if;
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
       or v_local.source_qualification_digest is null then return null; end if;
    if v_local.state='PENDING_FREEZE' then
      select p.* into v_pending from public.weekly_source_pending_entitlement_bundles p
        where p.id::text=v_local.result_json#>>'{publication,pending_bundle_verified}';
      if not found or v_pending.state not in ('PENDING','RELEASING','MANUAL_REVIEW')
         or v_pending.decision_bundle_id::text is distinct from v_local.result_json->>'decision_bundle_id'
         or v_pending.bundle_revision is distinct from v_local.common_bundle_revision
         or v_pending.decided_by_user_id is distinct from v_local.actor_user_id
         or v_pending.candidate_id is distinct from v_approval.candidate_id
         or v_local.result_json#>'{publication,ok}' is distinct from 'true'::jsonb
         or v_local.result_json#>'{publication,published}' is distinct from 'false'::jsonb
         or v_local.result_json#>>'{publication,code}' is distinct from 'WEEKLY_SOURCE_PUBLICATION_DEFERRED_PENDING_FREEZE'
         or v_pending.request_digest is distinct from private.weekly_source_publication_request_digest_v1(
             private.weekly_source_publication_request_canonical_v1(v_pending.request_json,'DEFERRED',v_pending.id)) then return null; end if;
      v_member:=array_position(v_pending.member_root_ids,v_local.root_timesheet_id);
      if v_member is null or v_pending.member_family_booking_ids[v_member] is distinct from v_scope->>'family_booking_id'
         or v_pending.member_root_versions[v_member] is distinct from v_publication_version then return null; end if;
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
            is distinct from v_publication_version then return null; end if;
      select c.value into strict v_choice from jsonb_array_elements(
        v_canonical#>'{financial_request,contract_choices}') c(value)
        where (c.value->>'root_ordinal')::integer=v_member;
      if v_choice->>'contract_id' is distinct from v_scope->>'contract_id'
         or v_choice->>'week_ending_date' is distinct from v_scope->>'week_ending_date' then return null; end if;
      select e.value into strict v_entitlement from jsonb_array_elements(
        v_canonical#>'{financial_request,member_entitlements}') e(value)
        where (e.value->>'root_ordinal')::integer=v_member;
      if v_entitlement->>'authority_kind' is distinct from 'PROTECTED'
         or (v_entitlement->>'component_count')::integer is distinct from jsonb_array_length(v_entitlement->'components') then return null; end if;
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
            private.weekly_source_publication_receipt_json_v1(v_publication.id) then return null; end if;
      v_member:=array_position(v_publication.member_root_ids,v_local.root_timesheet_id);
      if v_member is null or v_publication.member_family_booking_ids[v_member] is distinct from v_scope->>'family_booking_id'
         or v_publication.member_root_versions[v_member] is distinct from v_publication_version
         or not exists(select 1 from public.weekly_source_entitlement_heads h
           where h.id=v_publication.head_ids[v_member] and h.root_timesheet_id=v_local.root_timesheet_id
             and h.root_family_booking_id=v_scope->>'family_booking_id'
             and h.root_timesheet_version=v_publication_version
             and h.decision_bundle_id=v_publication.decision_bundle_id
             and h.bundle_revision=v_publication.bundle_revision
             and h.state in ('COMMITTED_CURRENT','SUPERSEDED')) then return null; end if;
      select h.* into strict v_head from public.weekly_source_entitlement_heads h
        where h.id=v_publication.head_ids[v_member];
      if v_head.authority_kind is distinct from 'PROTECTED'
         or v_head.candidate_id is distinct from v_approval.candidate_id
         or v_head.contract_id is distinct from v_approval.contract_id
         or v_head.week_ending_date is distinct from v_approval.week_ending then return null; end if;
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
                 'query.local.accepted_component')))) then return null; end if;
      v_origin:=v_head.source_origin_json;
      if v_head.source_generation_digest is distinct from private.weekly_source_publication_request_digest_v1(v_origin) then return null; end if;
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
          private.weekly_source_publication_request_digest_v1(v_components),'hex') then return null; end if;
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
       or jsonb_typeof(v_qualification->'prepared_sources') is distinct from 'array' then return null; end if;
    select e.* into v_original_event from public.weekly_exceptional_pay_family_events e
      where e.family_id=v_local.family_id
        and e.event_sequence=(v_qualification->>'family_event_sequence')::bigint;
    if not found or v_original_event.durable_work_event_id is distinct from v_approval.work_event_id
       or v_original_event.evidence_approval_id is distinct from v_approval.id
       or v_original_event.office_actor_user_id is distinct from v_local.actor_user_id
       or v_original_event.work_date is distinct from v_approval.protected_work_date
       or v_original_event.source_proposal_hash is distinct from decode(v_qualification->>'source_proposal_hash','hex')
       or v_original_event.source_proposal_hash is distinct from private.weekly_source_sha256_jsonb_v1(
          'WEEKLY_PROTECTED_SOURCE_PROPOSAL_V1',v_original_event.source_proposal_snapshot_json) then return null; end if;
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
       or v_origin->>'before_inventory_digest' is distinct from v_prior_inventory->>'inventory_digest' then return null; end if;
    if v_before_origin->>'kind'='COMMITTED_SOURCE_HEAD_V1' then
      v_before_origin:=v_before_origin-'source_revision';
    end if;
    if v_origin->'before_origin' is distinct from v_before_origin
       or private.weekly_source_local_origin_canonical_v2(v_origin) is distinct from v_origin then return null; end if;
  else return null;
  end if;
  return jsonb_build_object('lane','SOURCE_LOCAL','publication_request_id',v_request.id,
    'orchestration_run_id',v_run.id,'generation_id',v_generation.id,
    'request_sha256',encode(v_local.request_sha256,'hex'),'receipt_state',v_local.state);
end;
$function$;
alter function private.weekly_source_pay_query_local_receipt_event_v1(uuid,uuid,uuid,uuid) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_pay_query_local_receipt_event_v1(uuid,uuid,uuid,uuid) from public,anon,authenticated,service_role;
create or replace function private.weekly_source_pay_query_local_receipt_v1(p_root_timesheet_id uuid,p_approval_id uuid,p_generation_id uuid)
returns jsonb language sql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$ select private.weekly_source_pay_query_local_receipt_event_v1(p_root_timesheet_id,p_approval_id,p_generation_id,null); $function$;
alter function private.weekly_source_pay_query_local_receipt_v1(uuid,uuid,uuid) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_pay_query_local_receipt_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;

create or replace function private.weekly_source_pay_query_c1_receipt_event_v1(
  p_root_timesheet_id uuid,p_approval_id uuid,p_generation_id uuid,p_family_event_id uuid
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
      and (p_family_event_id is null or e.id=p_family_event_id)
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
alter function private.weekly_source_pay_query_c1_receipt_event_v1(uuid,uuid,uuid,uuid) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_pay_query_c1_receipt_event_v1(uuid,uuid,uuid,uuid) from public,anon,authenticated,service_role;
create or replace function private.weekly_source_pay_query_c1_receipt_v1(p_root_timesheet_id uuid,p_approval_id uuid,p_generation_id uuid)
returns jsonb language sql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$ select private.weekly_source_pay_query_c1_receipt_event_v1(p_root_timesheet_id,p_approval_id,p_generation_id,null); $function$;
alter function private.weekly_source_pay_query_c1_receipt_v1(uuid,uuid,uuid) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_pay_query_c1_receipt_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;

create or replace function private.weekly_source_pay_query_next_receipt_event_v1(
  p_root_timesheet_id uuid,p_approval_id uuid,p_generation_id uuid,p_family_event_id uuid
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
      and (p_family_event_id is null or e.id=p_family_event_id)
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
alter function private.weekly_source_pay_query_next_receipt_event_v1(uuid,uuid,uuid,uuid) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_pay_query_next_receipt_event_v1(uuid,uuid,uuid,uuid) from public,anon,authenticated,service_role;
create or replace function private.weekly_source_pay_query_next_receipt_v1(p_root_timesheet_id uuid,p_approval_id uuid,p_generation_id uuid)
returns jsonb language sql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$ select private.weekly_source_pay_query_next_receipt_event_v1(p_root_timesheet_id,p_approval_id,p_generation_id,null); $function$;
alter function private.weekly_source_pay_query_next_receipt_v1(uuid,uuid,uuid) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_pay_query_next_receipt_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;

-- Complete native receipt count and transport qualification; exact own-stage predecessor only.
create or replace function private.weekly_source_staged_prior_wait_receipt_v1(
  p_request_id uuid,p_actor_id uuid
) returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_stage jsonb; v_wait jsonb; p_root_timesheet_id uuid; p_family_event_id uuid;
  v_scope jsonb; v_latest public.weekly_exceptional_pay_family_events%rowtype;
  v_generation uuid; v_count integer; v_approval_run uuid; v_invalid_wait_audit boolean;
  v_local_count integer; v_c1_count integer; v_next_count integer; v_audit_count integer;
  v_local jsonb; v_c1 jsonb; v_next jsonb; v_receipt jsonb; v_exception jsonb; v_unavailable boolean:=false;
begin
  v_stage:=private.weekly_source_staged_publication_context_v1(p_request_id,p_actor_id);
  select generation.complete_next_vector_json#>'{automatic_reconcile_proof,qualified_wait}' into strict v_wait
    from public.weekly_exceptional_c1_publication_requests request
    join public.weekly_exceptional_pay_generations generation on generation.id=request.generation_id and generation.family_id=request.family_id
    where request.id=p_request_id;
  p_root_timesheet_id:=(v_stage#>>'{scope,root_timesheet_id}')::uuid; p_family_event_id:=(v_wait->>'event_id')::uuid;
  if v_wait->>'state' is distinct from 'QUALIFIED_WAIT' or v_wait->>'family_id' is distinct from v_stage->>'family_id'
    or exists(select 1 from public.weekly_exceptional_pay_family_events e where e.family_id=(v_stage->>'family_id')::uuid
      and e.durable_work_event_id=(v_stage->>'work_event_id')::uuid and e.event_sequence>(v_wait->>'event_sequence')::bigint
      and e.id is distinct from (v_stage->>'family_event_id')::uuid) then
    raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_PRIOR_WAIT_CHANGED' using errcode='40001'; end if;
  v_scope:=private.weekly_source_pay_query_scope_v1(p_root_timesheet_id);
  if v_scope is null or v_scope->>'target_family_id' is null then return null; end if;
  select event.* into v_latest from public.weekly_exceptional_pay_family_events event
  where event.id=p_family_event_id and event.family_id=(v_scope->>'target_family_id')::uuid
    and event.state='WAIT'
    and event.durable_work_event_id=(v_stage->>'work_event_id')::uuid
    and event.evidence_approval_id=(v_wait->>'approval_id')::uuid
    and event.event_sequence=(v_wait->>'event_sequence')::bigint;
  if not found then return null; end if;
      -- Choose the actual approval's UNIQUE financial target-generation link.
      -- The Wait owner also appends generation-less, money-neutral audit rows
      -- for that same approval. They are not second payment publications, but
      -- must be genuine unchanged-vector WAIT evidence, never arbitrary nulls.
      -- A sibling publication may supersede the original financial generation;
      -- today's current_generation is not the immutable approval's pin.
      select count(*) filter (where t.financial_generation_id is not null),
        (array_agg(t.financial_generation_id) filter (where t.financial_generation_id is not null))[1],
        coalesce(bool_or(t.financial_generation_id is null and (
          t.reason is distinct from 'WAIT'
          or t.resulting_lifecycle_state is distinct from 'WAITING_SOURCE'
          or t.actor_user_id is null
          or t.complete_prior_family_vector_fingerprint is distinct from t.complete_next_family_vector_fingerprint
          or private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_COMPLETE_VECTOR_V1',
            t.fixed_target_component_snapshot) is distinct from t.complete_next_family_vector_fingerprint
          or jsonb_typeof(t.current_source_proposal_snapshot) is distinct from 'object'
          or t.event_hash is distinct from private.weekly_source_sha256_jsonb_v1(
            'WEEKLY_PROTECTED_TARGET_EVENT_V1',jsonb_build_object(
              'family_id',t.family_id,'event_sequence',t.event_sequence,
              'approval_id',t.approval_id,'generation_id',null,
              'prior_vector_hash',encode(t.complete_prior_family_vector_fingerprint,'hex'),
              'next_vector_hash',encode(t.complete_next_family_vector_fingerprint,'hex'),
              'reason','WAIT','actor_user_id',t.actor_user_id))
        )),false)
        into v_count,v_generation,v_invalid_wait_audit
        from public.weekly_exceptional_pay_target_events t
        where t.family_id=v_latest.family_id and t.approval_id=v_latest.evidence_approval_id;
      if v_latest.evidence_approval_id is null or v_count<>1 or v_generation is null or v_invalid_wait_audit then
        v_unavailable:=true;
      else
        v_local:=private.weekly_source_pay_query_local_receipt_event_v1(p_root_timesheet_id,
          v_latest.evidence_approval_id,v_generation,v_latest.id);
        v_c1:=private.weekly_source_pay_query_c1_receipt_event_v1(p_root_timesheet_id,
          v_latest.evidence_approval_id,v_generation,v_latest.id);
        v_next:=private.weekly_source_pay_query_next_receipt_event_v1(p_root_timesheet_id,
          v_latest.evidence_approval_id,v_generation,v_latest.id);
        v_count:=(v_local is not null)::integer+(v_c1 is not null)::integer+(v_next is not null)::integer;
        select a.creation_orchestration_run_id into v_approval_run
          from public.weekly_exceptional_payment_approvals a where a.id=v_latest.evidence_approval_id;
        -- Count partial evidence too. A local RETIRED request is its audit row,
        -- not a second transport lane; unrelated sibling/history stays excluded.
        select count(*) into v_local_count
          from private.weekly_source_local_protected_decision_receipts l
          join public.weekly_exceptional_c1_publication_requests r on r.id=l.publication_request_id
          where l.generation_id=v_generation or r.orchestration_run_id=v_approval_run;
        select count(*),count(*) filter (where
          (v_local is not null and r.id::text=v_local->>'publication_request_id' and r.state='RETIRED')
          or (v_c1 is not null and r.id::text=v_c1->>'publication_request_id' and r.state='PUBLISHED'))
          into v_c1_count,v_audit_count
          from public.weekly_exceptional_c1_publication_requests r
          where r.generation_id=v_generation or r.orchestration_run_id=v_approval_run;
        select count(*) into v_next_count from private.bpay_next_protected_source_receipt r
          where r.approval_id=v_latest.evidence_approval_id or r.generation_id=v_generation
            or r.orchestration_run_id=v_approval_run;
        if v_count<>1 or v_approval_run is null
           or (v_local is not null and (v_local_count<>1 or v_c1_count<>1 or v_audit_count<>1 or v_next_count<>0))
           or (v_c1 is not null and (v_local_count<>0 or v_c1_count<>1 or v_audit_count<>1 or v_next_count<>0))
           or (v_next is not null and (v_local_count<>0 or v_c1_count<>0 or v_next_count<>1)) then v_unavailable:=true;
        else
          v_receipt:=coalesce(v_local,v_c1,v_next);
          v_exception:=jsonb_build_object('family_id',v_latest.family_id,
            'bound_version',v_scope->>'family_bound_version','event_sequence',v_latest.event_sequence::text,
            'state','WAIT','evidence_approval_id',v_latest.evidence_approval_id,'accepted_receipt',v_receipt);
        end if;
      end if;

  if v_unavailable or v_exception is null or v_exception->'accepted_receipt' is distinct from v_wait->'accepted_receipt'
    or v_exception#>>'{accepted_receipt,generation_id}' is distinct from v_wait->>'accepted_generation_id'
    or coalesce(v_exception#>>'{accepted_receipt,receipt_state}','') not in ('COMPLETE','PUBLISHED') then
    raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_PRIOR_WAIT_UNAVAILABLE' using errcode='55000'; end if;
  return v_exception;
end;
$function$;
alter function private.weekly_source_staged_prior_wait_receipt_v1(uuid,uuid) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_staged_prior_wait_receipt_v1(uuid,uuid) from public,anon,authenticated,service_role;

-- Point-read immutable expense authorities selected by real approved evidence.
-- The existing complete native movement assertion owns lineage, policy hashes,
-- arithmetic and movement admission. This reader never reprices an expense.
create or replace function private.weekly_source_expense_authority_evidence_v1(p_root_id uuid,p_authority_ids jsonb)
returns jsonb language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_id jsonb; a public.weekly_expense_authority_generations%rowtype;
  p public.weekly_source_row_expense_policy_snapshots%rowtype; v_scope jsonb; m record;
  v_result jsonb:='[]'; v_policy jsonb; v_count integer; v_family uuid[];
begin
  v_scope:=private.weekly_source_pay_query_scope_v1(p_root_id);
  v_family:=private.weekly_source_invoice_family_timesheet_ids_v1(p_root_id);
  if v_scope is null or cardinality(v_family) is null or cardinality(v_family)=0
    or jsonb_typeof(p_authority_ids) is distinct from 'array'
    or exists(select 1 from jsonb_array_elements(p_authority_ids) x group by x having count(*)<>1) then
    raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_EXPENSE_UNAVAILABLE' using errcode='55000'; end if;
  for v_id in select value from jsonb_array_elements(p_authority_ids) loop
    select * into strict a from public.weekly_expense_authority_generations where id=(v_id#>>'{}')::uuid;
    select * into strict p from public.weekly_source_row_expense_policy_snapshots where id=a.row_expense_policy_snapshot_id;
    if a.contract_id::text is distinct from v_scope->>'contract_id' or a.source_expense_pence<=0
      or a.source_observation_kind is distinct from 'ROW_PRESENT' or a.work_event_id is distinct from p.work_event_id
      or p.candidate_id::text is distinct from v_scope->>'candidate_id' or p.client_id::text is distinct from v_scope->>'client_id'
      or p.contract_id is distinct from a.contract_id then
      raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_EXPENSE_UNAVAILABLE' using errcode='55000'; end if;
    select count(*) into v_count from public.weekly_source_billing_movements movement
      join public.weekly_source_manifest_movements membership on membership.billing_movement_id=movement.id
      where movement.expense_authority_generation_id=a.id and movement.final_revision_id=a.final_revision_id
        and movement.invoice_timesheet_id=any(v_family) and movement.movement_role in ('EXPENSE_POSITIVE','EXPENSE_REPLACEMENT')
        and movement.source_line_kind='SOURCE_FIXED_EXPENSE';
    if v_count<>1 then raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_EXPENSE_UNAVAILABLE' using errcode='55000'; end if;
    select movement.*,membership.client_manifest_id into strict m from public.weekly_source_billing_movements movement
      join public.weekly_source_manifest_movements membership on membership.billing_movement_id=movement.id
      where movement.expense_authority_generation_id=a.id and movement.final_revision_id=a.final_revision_id
        and movement.invoice_timesheet_id=any(v_family) and movement.movement_role in ('EXPENSE_POSITIVE','EXPENSE_REPLACEMENT')
        and movement.source_line_kind='SOURCE_FIXED_EXPENSE';
    if private.weekly_source_ordinary_projection_expense_movement_assert_v1(m.id,m.final_revision_id,
        m.finalisation_cycle_id,m.client_manifest_id,m.invoice_timesheet_id) is distinct from true then
      raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_EXPENSE_UNAVAILABLE' using errcode='55000'; end if;
    -- Remove only storage/version/transport identities. Preserve every frozen
    -- policy value and the economic owner scope. Raw authority and raw policy
    -- remain in the qualification digest as separate immutable provenance.
    v_policy:=to_jsonb(p)-array['id','row_resolution_id','upload_row_id','generation','snapshot_hash','created_at_utc'];
    v_result:=v_result||jsonb_build_array(jsonb_build_object('work_event_id',a.work_event_id,
      'authority_id',a.id,'authority_hash',encode(a.authority_hash,'hex'),'movement_id',m.id,
      'final_revision_id',a.final_revision_id,'policy_snapshot_id',p.id,'policy',v_policy,
      'authority',to_jsonb(a)-array['state','created_at_utc'],'policy_snapshot',to_jsonb(p),
      'source_observation_kind',a.source_observation_kind,'candidate_reimbursement_ex_vat',a.candidate_reimbursement_ex_vat,
      'client_charge_ex_vat',a.client_charge_ex_vat));
  end loop;
  if exists(select 1 from jsonb_array_elements(v_result) x group by x->>'work_event_id' having count(*)<>1) then
    raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_EXPENSE_UNAVAILABLE' using errcode='55000'; end if;
  return (select coalesce(jsonb_agg(x order by x->>'work_event_id'),'[]') from jsonb_array_elements(v_result) x);
exception when no_data_found or too_many_rows or invalid_text_representation then
  raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_EXPENSE_UNAVAILABLE' using errcode='55000';
end;
$function$;
alter function private.weekly_source_expense_authority_evidence_v1(uuid,jsonb) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_expense_authority_evidence_v1(uuid,jsonb) from public,anon,authenticated,service_role;

create or replace function private.weekly_source_approved_expense_evidence_v1(p_root_id uuid,p_inventory jsonb)
returns jsonb language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_origin jsonb:=p_inventory#>'{approval_basis,origin}'; v_ids jsonb; v_result jsonb;
  v_source jsonb; v_components jsonb:=p_inventory->'components';
begin
  -- The complete native approval basis is required even for an empty expense set.
  if p_inventory#>>'{approval_basis,coverage_complete}' is distinct from 'true'
    or private.weekly_source_inventory_approval_basis_v2(p_root_id,p_inventory) is distinct from p_inventory->'approval_basis' then
    raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_EXPENSE_UNAVAILABLE' using errcode='55000'; end if;
  if v_origin->>'kind'='INITIAL_AUTHORISED_TSFIN_V1' then
    select coalesce(jsonb_agg(m.expense_authority_generation_id order by m.expense_authority_generation_id),'[]') into v_ids
      from public.weekly_source_expense_pay_materialisations m where m.root_timesheet_id=p_root_id
        and m.candidate_timesheet_financial_id=(v_origin->>'financial_snapshot_id')::uuid;
  elsif v_origin->>'kind'='COMMITTED_SOURCE_HEAD_V1' then
    v_source:=v_origin->'source_revision';
    if v_source->>'origin_kind'='PROTECTED_LOCAL_DECISION_V1' then
      select coalesce(jsonb_agg(x#>'{document,source_expense_id}' order by x#>>'{document,source_expense_id}'),'[]') into v_ids
        from jsonb_array_elements(private.weekly_source_staged_source_documents_v1((v_source->>'publication_request_id')::uuid)) x
        where x->>'authority_kind'='SOURCE_EXPENSE';
    elsif v_source->>'final_revision_id' is not null and coalesce(v_source->>'origin_kind','CURRENT_FINAL_SOURCE_V1')='CURRENT_FINAL_SOURCE_V1' then
      select coalesce(jsonb_agg(x->'expense_authority_generation_id' order by x->>'expense_authority_generation_id'),'[]') into v_ids
        from jsonb_array_elements(private.weekly_source_ordinary_projection_current_expenses_v1(
          p_root_id,(v_source->>'final_revision_id')::uuid)) x;
    else raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_EXPENSE_UNAVAILABLE' using errcode='55000'; end if;
  else raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_EXPENSE_UNAVAILABLE' using errcode='55000'; end if;
  v_result:=private.weekly_source_expense_authority_evidence_v1(p_root_id,v_ids);
  if jsonb_array_length(v_result)<>(select count(*) from jsonb_array_elements(v_components) x where x->>'component_kind'='SOURCE_FIXED_EXPENSE')
    or exists(select 1 from jsonb_array_elements(v_components) c where c->>'component_kind'='SOURCE_FIXED_EXPENSE'
      and (select count(*) from jsonb_array_elements(v_result) e where e->>'work_event_id'=c->>'component_member_identity'
        and e->>'source_observation_kind'=c->>'expense_code'
        and (e->>'candidate_reimbursement_ex_vat')::numeric=(c->>'pay_ex_vat')::numeric
        and (e->>'client_charge_ex_vat')::numeric=(c->>'charge_ex_vat')::numeric)<>1) then
    raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_EXPENSE_UNAVAILABLE' using errcode='55000'; end if;
  return v_result;
end;
$function$;
alter function private.weekly_source_approved_expense_evidence_v1(uuid,jsonb) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_approved_expense_evidence_v1(uuid,jsonb) from public,anon,authenticated,service_role;

-- One producer for both the assertion and the actual authorised Local arm.
-- I1 components come only from native stable-ID composers. C1 UUIDs remain
-- transport identities, separately checked against the sealed native snapshot.
create or replace function private.weekly_source_staged_entitlement_components_v1(p_request_id uuid,p_actor_id uuid)
returns jsonb language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_context jsonb; v_snapshot jsonb; v_segments jsonb; v_expenses jsonb; v_ids jsonb; v_evidence jsonb;
  v_components jsonb; v_expected jsonb:='[]'; v_common jsonb; v_item jsonb; v_raw jsonb; v_row jsonb; v_doc jsonb; v_stored jsonb;
  v_entry record; t public.weekly_exceptional_c1_component_records%rowtype;
  r public.weekly_exceptional_c1_publication_requests%rowtype; a public.weekly_exceptional_payment_approvals%rowtype;
  v_code text; v_source_key text; v_count integer; v_key text; v_i integer:=0; v_ordinal integer:=0;
begin
  v_context:=private.weekly_source_staged_publication_context_v1(p_request_id,p_actor_id);
  select * into strict r from public.weekly_exceptional_c1_publication_requests where id=p_request_id;
  select * into strict a from public.weekly_exceptional_payment_approvals where id=(v_context->>'approval_id')::uuid;
  v_snapshot:=v_context#>'{target_snapshot,tsfin_snapshot_json}'; v_segments:=v_snapshot#>'{invoice_breakdown_json,segments}';
  if jsonb_typeof(v_segments) is distinct from 'array' or v_snapshot#>>'{invoice_breakdown_json,mode}' is distinct from 'SEGMENTS'
    or jsonb_array_length(v_segments)>100 or jsonb_typeof(v_snapshot->'additional_units_json') is distinct from 'object' then
    raise exception 'WEEKLY_SOURCE_STAGED_COMPONENTS_UNAVAILABLE' using errcode='55000'; end if;
  -- This is the exact current native Local expense selection, not an arbitrary
  -- later resolution. Each selected ID must also exist in the sealed C1 source.
  select coalesce(jsonb_agg(expense.id order by expense.work_event_id),'[]') into v_ids from (
    select authority.*,row_number() over(partition by authority.work_event_id order by cycle.finalisation_week_ending desc,
      revision.finalised_at_utc desc,authority.generation desc,authority.id desc) event_rank
    from public.weekly_expense_authority_generations authority
    join public.weekly_source_final_revisions revision on revision.id=authority.final_revision_id and revision.state='CURRENT'
    join public.weekly_source_cycles cycle on cycle.id=revision.source_cycle_id
    join public.weekly_source_cycles action_cycle on action_cycle.id=a.source_cycle_id
    where authority.contract_id=r.contract_id and authority.state='CURRENT' and cycle.source_group_id=action_cycle.source_group_id
      and exists(select 1 from public.weekly_source_row_timesheet_lineages lineage where lineage.work_event_id=authority.work_event_id
        and lineage.contract_id=authority.contract_id and lineage.timesheet_id=any(private.weekly_source_invoice_family_timesheet_ids_v1(r.root_timesheet_id)))
  ) expense where expense.event_rank=1 and expense.source_expense_pence>0;
  v_evidence:=private.weekly_source_expense_authority_evidence_v1(r.root_timesheet_id,v_ids);
  if jsonb_array_length(v_evidence)<>(select count(*) from jsonb_array_elements(v_context->'source_documents') x where x->>'authority_kind'='SOURCE_EXPENSE')
    or exists(select 1 from jsonb_array_elements(v_evidence) e where
      (select count(*) from jsonb_array_elements(v_context->'source_documents') d where d->>'authority_kind'='SOURCE_EXPENSE'
        and d#>>'{document,source_expense_id}'=e->>'authority_id' and d->>'external_identity'=e->>'authority_id'
        and d->>'source_document_sha256'=e->>'authority_hash'
        and d#>>'{document,expense_code}'=upper('SOURCE_SUPPLIED:'||(e->>'work_event_id'))
        and (d#>>'{document,pay_ex_vat}')::numeric=(e->>'candidate_reimbursement_ex_vat')::numeric
        and (d#>>'{document,charge_ex_vat}')::numeric=(e->>'client_charge_ex_vat')::numeric)<>1) then
    raise exception 'WEEKLY_SOURCE_STAGED_EXPENSE_CHANGED' using errcode='40001'; end if;
  select coalesce(jsonb_agg(jsonb_build_object('work_event_id',e->'work_event_id','source_observation_kind',e->'source_observation_kind',
    'candidate_reimbursement_ex_vat',e->'candidate_reimbursement_ex_vat','client_charge_ex_vat',e->'client_charge_ex_vat') order by e->>'work_event_id'),'[]')
    into v_expenses from jsonb_array_elements(v_evidence) e;
  v_components:=private.weekly_source_initial_additional_components_v2(
    private.weekly_source_entitlement_components_v1(v_segments,v_expenses),v_snapshot->'additional_units_json',
    (v_snapshot->>'additional_pay_ex_vat')::numeric,(v_snapshot->>'additional_charge_ex_vat')::numeric,
    v_context#>>'{scope,family_booking_id}');
  if exists(select 1 from jsonb_array_elements(v_components) c group by c->>'component_id' having count(*)<>1)
    or (select coalesce(sum((c->>'pay_ex_vat')::numeric) filter(where not (c->>'exclude_from_pay')::boolean),0)
      from jsonb_array_elements(v_components) c) is distinct from a.approved_target_gross then
    raise exception 'WEEKLY_SOURCE_STAGED_COMPONENTS_UNAVAILABLE' using errcode='55000'; end if;
  -- Exact existing C1 carrier projection. No total-only equivalence or reuse
  -- of transport IDs in the I1 component namespace.
  v_common:=jsonb_build_object('component_id',null,'component_kind',null,'economic_key_type',null,'economic_key_value',null,
    'component_member_identity',null,'segment_id',null,'segment_key',null,'segment_stable_key',null,'work_date',null,'reference_number',null,
    'hours_day',null,'hours_night',null,'hours_sat',null,'hours_sun',null,'hours_bh',null,'additional_code_raw',null,
    'unit_count',null,'unit_pay_rate',null,'unit_charge_rate',null,'expense_code',null,'pay_ex_vat','0.00','charge_ex_vat',null,
    'exclude_from_pay',false,'origin','WEEKLY_SOURCE_APPROVED_TARGET','movement_id',null,'movement_group_id',null);
  for v_raw in select value from jsonb_array_elements(v_segments) loop
    v_i:=v_i+1; v_source_key:='10:'||r.root_timesheet_id::text||':SEGMENT:'||lpad(v_i::text,12,'0');
    v_item:=v_common||jsonb_build_object('source_key',v_source_key,'component_kind','WORKED_TIME','economic_key_type','TS_DAY',
      'economic_key_value',v_raw->>'date','component_member_identity',coalesce(v_raw->>'segment_stable_key',v_raw->>'segment_id',v_raw->>'segment_key'),
      'segment_id',v_raw->>'segment_id','segment_key',coalesce(v_raw->>'segment_key',v_raw->>'segment_id'),
      'segment_stable_key',coalesce(v_raw->>'segment_stable_key',v_raw->>'segment_id',v_raw->>'segment_key'),
      'work_date',v_raw->>'date','reference_number',nullif(btrim(v_raw->>'ref_num'),''),
      'pay_ex_vat',case when v_raw->'exclude_from_pay'='true'::jsonb then '0.00' else v_raw->>'pay_amount' end,
      'charge_ex_vat',v_raw->>'charge_amount','exclude_from_pay',coalesce((v_raw->>'exclude_from_pay')::boolean,false));
    foreach v_key in array array['day','night','sat','sun','bh'] loop
      v_item:=v_item||jsonb_build_object('hours_'||v_key,coalesce(v_raw->>('hours_'||v_key),'0'));
    end loop;
    v_expected:=v_expected||jsonb_build_array(v_item);
  end loop;
  for v_entry in select key,value from jsonb_each(v_snapshot->'additional_units_json') loop
    v_raw:=v_entry.value; v_code:=btrim(v_entry.key);
    if (coalesce(v_raw->>'pay_ex_vat',v_raw->>'amount_ex_vat'))::numeric=0 then continue; end if;
    v_source_key:='10:'||r.root_timesheet_id::text||':ADDITIONAL:'||lpad(octet_length(convert_to(v_code,'UTF8'))::text,10,'0')||':'||encode(convert_to(v_code,'UTF8'),'hex');
    v_expected:=v_expected||jsonb_build_array(v_common||jsonb_build_object('source_key',v_source_key,'component_kind','ADDITIONAL_UNIT',
      'economic_key_type','ADDITIONAL_CODE','economic_key_value',upper(v_code),'component_member_identity','additional:'||upper(v_code),
      'additional_code_raw',v_code,'unit_count',coalesce(v_raw->>'unit_count',v_raw->>'units_week'),
      'unit_pay_rate',coalesce(v_raw->>'pay_rate',v_raw->>'rate'),'unit_charge_rate',v_raw->>'charge_rate',
      'pay_ex_vat',coalesce(v_raw->>'pay_ex_vat',v_raw->>'amount_ex_vat'),'charge_ex_vat',coalesce(v_raw->>'charge_ex_vat',v_raw->>'charge_amount_ex_vat')));
  end loop;
  for v_raw in select value from jsonb_array_elements(v_evidence) loop
    v_code:=upper('SOURCE_SUPPLIED:'||(v_raw->>'work_event_id'));
    v_expected:=v_expected||jsonb_build_array(v_common||jsonb_build_object('source_key','10:'||r.root_timesheet_id::text||':EXPENSE:'||v_code,
      'component_kind','EXPENSE','economic_key_type','EXPENSE_CODE','economic_key_value',v_code,'component_member_identity','expense:'||v_code,
      'expense_code',v_code,'pay_ex_vat',v_raw->>'candidate_reimbursement_ex_vat','charge_ex_vat',v_raw->>'client_charge_ex_vat'));
  end loop;
  -- Ordinary non-source expenses have no native I1 mapper in this publisher.
  -- Refuse an unmapped authority; never discard it to make a zero-delta claim.
  if exists(select 1 from jsonb_array_elements(v_context->'source_documents') x where x->>'authority_kind'='ORDINARY_EXPENSE') then
    raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_ORDINARY_EXPENSE_UNMAPPED' using errcode='55000'; end if;
  if (select count(*) from public.weekly_exceptional_c1_component_records where publication_request_id=r.id)
      is distinct from r.expected_component_count or jsonb_array_length(v_expected) is distinct from r.expected_component_count then
    raise exception 'WEEKLY_SOURCE_STAGED_TRANSPORT_COMPONENTS_CHANGED' using errcode='40001'; end if;
  for v_item in select value from jsonb_array_elements(v_expected) order by value->>'source_key' collate "C" loop
    v_ordinal:=v_ordinal+1;
    select * into strict t from public.weekly_exceptional_c1_component_records where publication_request_id=r.id and source_key=v_item->>'source_key';
    select x into strict v_row from public.weekly_exceptional_c1_publication_requests request
      join public.weekly_exceptional_pay_generations generation on generation.id=request.generation_id
      cross join lateral jsonb_array_elements(generation.complete_next_vector_json->'components') x
      where request.id=r.id and x->>'component_id'=t.component_id::text;
    v_doc:=null;
    select x into strict v_doc from jsonb_array_elements(v_context->'source_documents') x where (x->>'source_ordinal')::integer=t.source_ordinal;
    v_stored:=(to_jsonb(t)-array['id','publication_request_id','created_at_utc','component_sha256','source_ordinal','source_key'])
      ||jsonb_build_object('movement_id',null,'movement_group_id',null);
    -- The native closed component codec requires decimal STRINGS. SQL numeric
    -- columns are losslessly cast to text before invoking that existing codec.
    foreach v_key in array array['hours_day','hours_night','hours_sat','hours_sun','hours_bh',
      'unit_count','unit_pay_rate','unit_charge_rate','pay_ex_vat','charge_ex_vat'] loop
      v_stored:=v_stored||jsonb_build_object(v_key,to_jsonb(t)->>v_key);
    end loop;
    if t.component_ordinal is distinct from v_ordinal or t.component_sha256 is distinct from decode(v_row->>'component_sha256','hex')
      or v_row->>'source_key' is distinct from t.source_key or (v_row->>'source_ordinal')::integer is distinct from t.source_ordinal
      or private.weekly_source_publication_component_canonical_v1(v_stored,'staged.transport.row') is distinct from
        private.weekly_source_publication_component_canonical_v1(
          (v_row-array['source_ordinal','source_key','component_sha256','record_type'])
            ||jsonb_build_object('movement_id',null,'movement_group_id',null),'staged.transport.vector')
      or private.weekly_source_publication_component_canonical_v1(
        (v_item-'source_key')||jsonb_build_object('component_id',t.component_id,'component_ordinal',v_ordinal),'staged.transport.expected') is distinct from
        private.weekly_source_publication_component_canonical_v1(
          (v_row-array['source_ordinal','source_key','component_sha256','record_type'])
            ||jsonb_build_object('movement_id',null,'movement_group_id',null),'staged.transport.vector') then
      raise exception 'WEEKLY_SOURCE_STAGED_TRANSPORT_COMPONENTS_CHANGED' using errcode='40001'; end if;
    if t.component_kind in ('WORKED_TIME','ADDITIONAL_UNIT') then
      if v_doc->>'authority_kind' is distinct from 'APPROVED_COMPONENT' or v_doc->>'external_identity' is distinct from t.component_id::text
        or v_doc#>>'{document,source_key}' is distinct from t.source_key
        or (v_doc#>>'{document,source_ordinal}')::integer is distinct from t.source_ordinal
        or private.weekly_source_publication_component_canonical_v1(
          (v_doc->'document'-array['contract','source_id','source_system','external_identity','external_revision','root_timesheet_id','candidate_id','contract_id','week_ending_date','document_sha256','source_ordinal','source_key'])
            ||jsonb_build_object('movement_id',null,'movement_group_id',null),'staged.transport.document') is distinct from
          private.weekly_source_publication_component_canonical_v1(
            (v_row-array['source_ordinal','source_key','component_sha256','record_type'])
              ||jsonb_build_object('movement_id',null,'movement_group_id',null),'staged.transport.vector') then
        raise exception 'WEEKLY_SOURCE_STAGED_TRANSPORT_COMPONENTS_CHANGED' using errcode='40001'; end if;
    elsif t.component_kind='EXPENSE' then
      if v_doc->>'authority_kind' is distinct from 'SOURCE_EXPENSE' or v_doc#>>'{document,expense_code}' is distinct from t.expense_code
        or (v_doc#>>'{document,pay_ex_vat}')::numeric is distinct from t.pay_ex_vat
        or (v_doc#>>'{document,charge_ex_vat}')::numeric is distinct from t.charge_ex_vat then
        raise exception 'WEEKLY_SOURCE_STAGED_TRANSPORT_COMPONENTS_CHANGED' using errcode='40001'; end if;
    else raise exception 'WEEKLY_SOURCE_STAGED_TRANSPORT_COMPONENTS_CHANGED' using errcode='40001'; end if;
  end loop;
  if (select count(*) from jsonb_array_elements(v_context->'source_documents') x where x->>'authority_kind'='APPROVED_COMPONENT')<>
      (select count(*) from jsonb_array_elements(v_expected) x where x->>'component_kind' in ('WORKED_TIME','ADDITIONAL_UNIT')) then
    raise exception 'WEEKLY_SOURCE_STAGED_TRANSPORT_COMPONENTS_CHANGED' using errcode='40001'; end if;
  return jsonb_build_object('context',v_context,'components',v_components,'expenses',v_expenses,'expense_evidence',v_evidence,
    'transport_component_count',v_ordinal);
exception when no_data_found or too_many_rows or invalid_text_representation or numeric_value_out_of_range then
  raise exception 'WEEKLY_SOURCE_STAGED_COMPONENTS_UNAVAILABLE' using errcode='55000';
end;
$function$;
alter function private.weekly_source_staged_entitlement_components_v1(uuid,uuid) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_staged_entitlement_components_v1(uuid,uuid) from public,anon,authenticated,service_role;

-- Actual sealed native stage -> complete native I1 line/component proof.
-- Seven-field certificate stays compatible with Source27. The qualification
-- digest commits to every component, frozen detail, source and expense proof.
create or replace function private.weekly_source_automatic_reconcile_delta_assert_v1(p_request_id uuid,p_actor_id uuid)
returns jsonb language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare r public.weekly_exceptional_c1_publication_requests%rowtype; g public.weekly_exceptional_pay_generations%rowtype;
  run public.weekly_exceptional_orchestration_runs%rowtype; event public.weekly_exceptional_pay_family_events%rowtype;
  old_approval public.weekly_exceptional_payment_approvals%rowtype; approval public.weekly_exceptional_payment_approvals%rowtype;
  v_produced jsonb; v_context jsonb; v_proof jsonb; v_wait jsonb; v_prior_receipt jsonb; v_current jsonb; v_before jsonb;
  v_detail jsonb; v_expenses_before jsonb; v_snapshot jsonb; v_components jsonb; v_before_component jsonb; v_next_component jsonb;
  v_old_raw jsonb; v_raw jsonb; v_source jsonb; v_before_rate jsonb; v_before_charge_rate jsonb; v_key text;
  v_prior_work_count integer; v_count integer; v_component_proofs jsonb:='[]'; v_expense_proofs jsonb:='[]'; v_policy jsonb;
  v_rules jsonb; v_cas jsonb; v_facts jsonb; v_qualification jsonb; v_before_expense jsonb; v_next_expense jsonb;
begin
  if p_request_id is null or p_actor_id is null then
    raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_DELTA_UNAVAILABLE' using errcode='22023'; end if;
  select * into strict r from public.weekly_exceptional_c1_publication_requests where id=p_request_id;
  select * into strict g from public.weekly_exceptional_pay_generations where id=r.generation_id and family_id=r.family_id;
  select * into strict run from public.weekly_exceptional_orchestration_runs where id=r.orchestration_run_id and family_id=r.family_id;
  v_proof:=g.complete_next_vector_json->'automatic_reconcile_proof'; v_wait:=v_proof->'qualified_wait';
  -- Same actor/date/rules/release owner as native Source27, never a parallel capability classifier.
  perform private.weekly_source_automatic_reconcile_capability_assert_v1((v_proof->>'source_cycle_id')::uuid);
  v_produced:=private.weekly_source_staged_entitlement_components_v1(r.id,p_actor_id);
  v_context:=v_produced->'context'; v_before:=v_context->'prior_inventory'; v_components:=v_produced->'components';
  v_snapshot:=v_context#>'{target_snapshot,tsfin_snapshot_json}';
  select * into strict event from public.weekly_exceptional_pay_family_events where id=(v_context->>'family_event_id')::uuid;
  select * into strict approval from public.weekly_exceptional_payment_approvals where id=(v_context->>'approval_id')::uuid;
  select * into strict old_approval from public.weekly_exceptional_payment_approvals where id=(v_wait->>'approval_id')::uuid;
  if run.request_kind is distinct from 'RECONCILE' or event.state is distinct from 'ACCEPTED_SOURCE'
    or not private.weekly_exceptional_json_keys_exact_v1(v_proof,array['schema_version','eligible','actor_user_id','source_cycle_id',
      'work_event_id','qualified_wait','rule_set_version_id','rule_values_sha256','source_proposal','self_bill_facts','source_cas',
      'approved_inventory_basis','selection_sha256'])
    or not(v_proof ?& array['schema_version','eligible','actor_user_id','source_cycle_id','work_event_id','qualified_wait',
      'rule_set_version_id','rule_values_sha256','source_proposal','self_bill_facts','source_cas','approved_inventory_basis','selection_sha256'])
    or v_proof->>'schema_version' is distinct from 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_SELECTION_V1'
    or v_proof->>'eligible' is distinct from 'true' or v_proof->>'actor_user_id' is distinct from p_actor_id::text
    or v_proof->>'source_cycle_id' is distinct from v_context->>'source_cycle_id'
    or v_proof->>'work_event_id' is distinct from v_context->>'work_event_id'
    or v_proof->'source_proposal' is distinct from v_context->'source_proposal'
    or v_proof->'approved_inventory_basis' is distinct from v_before->'approval_basis'
    or private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_AUTOMATIC_RECONCILE_SELECTION_V1',v_proof-'selection_sha256')
      is distinct from private.weekly_exceptional_hex_sha256_v1(v_proof->>'selection_sha256')
    or v_wait->>'schema_version' is distinct from 'WEEKLY_SOURCE_PROTECTED_WAIT_WITNESS_V1' or v_wait->>'state' is distinct from 'QUALIFIED_WAIT'
    or private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_PROTECTED_WAIT_WITNESS_V1',v_wait-'qualification_sha256')
      is distinct from private.weekly_exceptional_hex_sha256_v1(v_wait->>'qualification_sha256')
    or v_wait->>'root_timesheet_id' is distinct from r.root_timesheet_id::text or v_wait->>'family_id' is distinct from r.family_id::text
    or v_wait->>'work_event_id' is distinct from event.durable_work_event_id::text
    or v_wait->>'candidate_id' is distinct from v_context#>>'{scope,candidate_id}'
    or v_wait->>'client_id' is distinct from v_context#>>'{scope,client_id}' or v_wait->>'contract_id' is distinct from r.contract_id::text
    or v_wait->>'family_bound_version' is distinct from v_context->>'before_bound_version'
    or v_wait->>'current_generation_id' is distinct from g.prior_generation_id::text
    or (v_wait->>'current_generation_number')::integer is distinct from g.generation_number-1
    or old_approval.withdrawn_at_utc is not null or old_approval.pay_target_family_id is distinct from r.family_id
    or old_approval.work_event_id is distinct from event.durable_work_event_id
    or old_approval.contract_rate_policy_source_fingerprint is distinct from approval.contract_rate_policy_source_fingerprint
    or (old_approval.protected_work_date,old_approval.protected_start_at_local,old_approval.protected_end_at_local,old_approval.protected_break_minutes)
      is distinct from (approval.protected_work_date,approval.protected_start_at_local,approval.protected_end_at_local,approval.protected_break_minutes) then
    raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_DELTA_UNAVAILABLE' using errcode='55000'; end if;
  v_prior_receipt:=private.weekly_source_staged_prior_wait_receipt_v1(r.id,p_actor_id);
  if v_prior_receipt is null or v_prior_receipt->'accepted_receipt' is distinct from v_wait->'accepted_receipt' then
    raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_PRIOR_WAIT_UNAVAILABLE' using errcode='55000'; end if;
  -- Source CAS deliberately excludes pending protected stage mutations. Do not
  -- normalize any Source/export/report/economic/expense/hold authority changes.
  v_cas:=private.weekly_source_bulk_source_cas_assert_v1((v_proof->>'source_cycle_id')::uuid,array[(v_context#>>'{scope,client_id}')::uuid]);
  v_rules:=private.weekly_source_effective_rule_set_v1((v_context#>>'{scope,client_id}')::uuid,(v_proof->>'source_cycle_id')::uuid);
  v_facts:=private.weekly_source_shift_on_self_bill_v1((v_proof->>'work_event_id')::uuid,(v_proof->>'source_cycle_id')::uuid);
  if v_cas is distinct from v_proof->'source_cas' or v_rules->'rule_set_version_id' is distinct from v_proof->'rule_set_version_id'
    or v_rules->'values_sha256' is distinct from v_proof->'rule_values_sha256' or v_facts is distinct from v_proof->'self_bill_facts' then
    raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_SOURCE_CHANGED' using errcode='40001'; end if;
  -- Read the real current approved position at K+1, qualified above against the
  -- sealed K position. Passing a hand-normalized inventory to approval_basis
  -- would be a false proof, so only staged_context performs the K adjustment.
  v_current:=private.weekly_source_effective_inventory_v1(r.root_timesheet_id);
  v_detail:=private.weekly_source_approved_detail_evidence_v1(r.root_timesheet_id,v_current);
  v_expenses_before:=private.weekly_source_approved_expense_evidence_v1(r.root_timesheet_id,v_current);
  if v_detail#>>'{approval_basis,detail_digest}' is distinct from v_before#>>'{approval_basis,detail_digest}'
    or jsonb_array_length(v_components) is distinct from jsonb_array_length(v_before->'components') then
    raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_COMPONENT_DELTA_NONZERO' using errcode='40001'; end if;
  select count(*) into v_prior_work_count from jsonb_array_elements(v_before->'components') c where c->>'component_kind'='WORKED_TIME';
  if v_current#>>'{approval_basis,origin,kind}'='INITIAL_AUTHORISED_TSFIN_V1'
    and (v_snapshot->'policy_snapshot_json' is distinct from v_detail->'policy'
      or v_snapshot->'rate_source_refs_json' is distinct from v_detail#>'{details,rate_source_refs}') then
    raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_FROZEN_POLICY_CHANGED' using errcode='40001'; end if;
  for v_before_component in select value from jsonb_array_elements(v_before->'components') loop
    select value into strict v_next_component from jsonb_array_elements(v_components)
      where value->>'component_id'=v_before_component->>'component_id';
    v_before_component:=private.weekly_source_publication_component_canonical_v1(v_before_component-'component_sha256','automatic.before');
    v_next_component:=private.weekly_source_publication_component_canonical_v1(v_next_component,'automatic.next');
    foreach v_key in array array['component_kind','economic_key_type','economic_key_value','component_member_identity','segment_id','segment_key','segment_stable_key',
      'work_date','reference_number','additional_code_raw','expense_code','exclude_from_pay','origin','movement_id','movement_group_id'] loop
      if v_before_component->v_key is distinct from v_next_component->v_key then
        raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_COMPONENT_IDENTITY_CHANGED' using errcode='40001'; end if;
    end loop;
    foreach v_key in array array['hours_day','hours_night','hours_sat','hours_sun','hours_bh','unit_count','unit_pay_rate','unit_charge_rate','pay_ex_vat','charge_ex_vat'] loop
      if (v_before_component->>v_key)::numeric is distinct from (v_next_component->>v_key)::numeric then
        raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_COMPONENT_DELTA_NONZERO' using errcode='40001',
          detail=jsonb_build_object('component_id',v_before_component->'component_id','field',v_key)::text; end if;
    end loop;
    v_old_raw:=null; v_source:=null;
    if v_before_component->>'component_kind'='WORKED_TIME' then
      select value into strict v_raw from jsonb_array_elements(v_snapshot#>'{invoice_breakdown_json,segments}')
        where value->>'segment_id'=v_next_component->>'segment_id';
      if v_current#>>'{approval_basis,origin,kind}'='INITIAL_AUTHORISED_TSFIN_V1' then
        select value into strict v_old_raw from jsonb_array_elements(v_detail#>'{details,segments}')
          where value->>'segment_id'=v_before_component->>'segment_id';
        foreach v_key in array array['date','start','end','overnight','break_mins','breaks','start_utc','end_utc'] loop
          if v_old_raw->v_key is distinct from v_raw->v_key then
            raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_APPROVED_DETAIL_CHANGED' using errcode='40001'; end if;
        end loop;
      else
        select value->'detail' into strict v_old_raw from jsonb_array_elements(v_detail->'details')
          where value->>'component_id'=v_before_component->>'component_id';
        if v_old_raw->>'work_date' is distinct from v_raw->>'date' or v_old_raw->>'start' is distinct from v_raw->>'start'
          or v_old_raw->>'end' is distinct from v_raw->>'end' or v_old_raw->'overnight' is distinct from v_raw->'overnight'
          or v_old_raw->'breaks' is distinct from v_raw->'breaks'
          or (jsonb_array_length(v_raw->'breaks')=0 and (v_old_raw->>'break_minutes')::integer is distinct from (v_raw->>'break_mins')::integer)
          or (jsonb_array_length(v_raw->'breaks')>0 and (v_raw->>'break_mins')::integer is distinct from 0)
          or v_old_raw->'start_utc' is distinct from v_raw->'start_utc' or v_old_raw->'end_utc' is distinct from v_raw->'end_utc' then
          raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_APPROVED_DETAIL_CHANGED' using errcode='40001'; end if;
      end if;
      -- Native Local capture uses the sealed snapshot's band rates; initial
      -- approved basis uses its own exact Source vectors/single frozen TSFIN.
      -- HEAD basis uses the real chosen-detail row qualified by approval_basis.
      foreach v_key in array array['day','night','sat','sun','bh'] loop
        if v_current#>>'{approval_basis,origin,kind}'='INITIAL_AUTHORISED_TSFIN_V1' then
          v_before_rate:=case when jsonb_typeof(v_old_raw->'weekly_source')='object' then v_old_raw#>array['weekly_source','pay_vector','rates',v_key]
            when v_prior_work_count=1 then v_detail#>array['financial_rate_evidence','pay',v_key] else null end;
          v_before_charge_rate:=case when jsonb_typeof(v_old_raw->'weekly_source')='object' then v_old_raw#>array['weekly_source','charge_vector','rates',v_key]
            when v_prior_work_count=1 then v_detail#>array['financial_rate_evidence','charge',v_key] else null end;
        else v_before_rate:=v_old_raw#>array['rates',v_key]; v_before_charge_rate:=v_old_raw#>array['charge_rates',v_key]; end if;
        if ((v_next_component->>('hours_'||v_key))::numeric<>0 and ((v_before_rate#>>'{}')::numeric is null
            and not (v_next_component->>'exclude_from_pay')::boolean or (v_before_charge_rate#>>'{}')::numeric is null))
          or (v_before_rate#>>'{}')::numeric is distinct from (v_snapshot->>('pay_'||v_key))::numeric
          or (v_before_charge_rate#>>'{}')::numeric is distinct from (v_snapshot->>('charge_'||v_key))::numeric then
          raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_APPROVED_RATE_CHANGED' using errcode='40001'; end if;
      end loop;
      -- This selected shift is now ACCEPTED_SOURCE. Its actual immutable
      -- Source economics must equal the saved producer output, not only clocks.
      if v_next_component->>'component_member_identity'=v_context->>'work_event_id' then
        select value into strict v_source from jsonb_array_elements(v_context->'next_source_segments')
          where value#>>'{weekly_source,work_event_id}'=v_context->>'work_event_id';
        if v_source->>'segment_id' is distinct from v_raw->>'segment_id' or v_source->>'date' is distinct from v_raw->>'date'
          or v_source->>'start' is distinct from v_raw->>'start' or v_source->>'end' is distinct from v_raw->>'end'
          or (v_source->>'break_mins')::integer is distinct from (v_raw->>'break_mins')::integer
          or (v_source->>'pay_amount')::numeric is distinct from (v_raw->>'pay_amount')::numeric
          or (v_source->>'charge_amount')::numeric is distinct from (v_raw->>'charge_amount')::numeric
          or v_source->'weekly_source' is distinct from v_raw->'weekly_source' then
          raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_SOURCE_VECTOR_CHANGED' using errcode='40001'; end if;
        foreach v_key in array array['day','night','sat','sun','bh'] loop
          if (v_source->>('hours_'||v_key))::numeric is distinct from (v_raw->>('hours_'||v_key))::numeric
            or (v_source#>>array['weekly_source','pay_vector','rates',v_key])::numeric is distinct from (v_snapshot->>('pay_'||v_key))::numeric
            or (v_source#>>array['weekly_source','charge_vector','rates',v_key])::numeric is distinct from (v_snapshot->>('charge_'||v_key))::numeric then
            raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_SOURCE_VECTOR_CHANGED' using errcode='40001'; end if;
        end loop;
      end if;
    elsif v_before_component->>'component_kind' not in ('ADDITIONAL_UNIT','SOURCE_FIXED_EXPENSE') then
      raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_COMPONENT_UNMAPPED' using errcode='55000';
    end if;
    v_component_proofs:=v_component_proofs||jsonb_build_array(jsonb_build_object('before',v_before_component,'next',v_next_component,
      'approved_detail',v_old_raw,'next_detail',case when v_before_component->>'component_kind'='WORKED_TIME' then v_raw else null end,
      'source_evidence',v_source));
  end loop;
  if jsonb_array_length(v_expenses_before) is distinct from jsonb_array_length(v_produced->'expense_evidence') then
    raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_EXPENSE_POLICY_CHANGED' using errcode='40001'; end if;
  for v_before_expense in select value from jsonb_array_elements(v_expenses_before) loop
    select value into strict v_next_expense from jsonb_array_elements(v_produced->'expense_evidence')
      where value->>'work_event_id'=v_before_expense->>'work_event_id';
    if v_before_expense->'policy' is distinct from v_next_expense->'policy'
      or v_before_expense->'source_observation_kind' is distinct from v_next_expense->'source_observation_kind' then
      raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_EXPENSE_POLICY_CHANGED' using errcode='40001'; end if;
    v_expense_proofs:=v_expense_proofs||jsonb_build_array(jsonb_build_object('before',v_before_expense,'next',v_next_expense));
  end loop;
  -- The outer certificate reports zero only after every actual native component
  -- passed its identity/quantity/rate/money/policy comparison independently.
  v_qualification:=jsonb_build_object('schema_version','WEEKLY_SOURCE_AUTOMATIC_RECONCILE_DELTA_QUALIFICATION_V1',
    'publication_request_id',r.id,'actor_user_id',p_actor_id,'stage_context',v_context,'automatic_selection',v_proof,
    'prior_wait_receipt',v_prior_receipt,'approved_basis',v_detail,'source_cas',v_cas,'component_proofs',v_component_proofs,
    'expense_proofs',v_expense_proofs,'native_next_components',v_components);
  return jsonb_build_object('schema_version','WEEKLY_SOURCE_AUTOMATIC_RECONCILE_DELTA_V1','publication_request_id',r.id,
    'actor_user_id',p_actor_id,'prior_inventory_digest',v_before->'inventory_digest',
    'complete_next_vector_sha256',encode(g.complete_next_vector_hash,'hex'),'delta_pay_ex_vat','0.00',
    'qualification_sha256',encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_AUTOMATIC_RECONCILE_DELTA_QUALIFICATION_V1',v_qualification),'hex'));
exception when no_data_found or too_many_rows or invalid_text_representation or numeric_value_out_of_range then
  raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_DELTA_UNAVAILABLE' using errcode='55000';
end;
$function$;
alter function private.weekly_source_automatic_reconcile_delta_assert_v1(uuid,uuid) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_automatic_reconcile_delta_assert_v1(uuid,uuid) from public,anon,authenticated,service_role;

-- Complete native owner successor; only sealed automatic requests use the common producer.
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
  v_automatic_components jsonb;
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
     or not private.weekly_source_pay_actor_allowed_v1(v_actor) then
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
  -- A manually invoked native Local owner cannot bypass the sealed automatic
  -- request's per-component zero-delta proof. This is before PREPARING insert.
  if v_generation.complete_next_vector_json ? 'automatic_reconcile_proof' then
    perform private.weekly_source_automatic_reconcile_delta_assert_v1(v_request.id,v_actor);
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
    if v_generation.complete_next_vector_json ? 'automatic_reconcile_proof' then
      -- Exactly the producer used by delta_assert; no separately guessed vector.
      v_automatic_components:=private.weekly_source_staged_entitlement_components_v1(v_request.id,v_actor);
      v_components:=v_automatic_components->'components';
      v_expenses:=v_automatic_components->'expenses';
    else
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
    end if;
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
alter function public.weekly_exceptional_pay_complete_local_v1(jsonb) OWNER TO CURRENT_USER;
revoke all on function public.weekly_exceptional_pay_complete_local_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_exceptional_pay_complete_local_v1(jsonb) to service_role;

alter function private.weekly_source_staged_source_documents_v1(uuid) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_staged_source_documents_v1(uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_staged_publication_context_v1(uuid,uuid) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_staged_publication_context_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_approved_detail_evidence_v1(uuid,jsonb) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_approved_detail_evidence_v1(uuid,jsonb) from public,anon,authenticated,service_role;
alter function private.weekly_source_pay_query_local_receipt_event_v1(uuid,uuid,uuid,uuid) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_pay_query_local_receipt_event_v1(uuid,uuid,uuid,uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_pay_query_c1_receipt_event_v1(uuid,uuid,uuid,uuid) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_pay_query_c1_receipt_event_v1(uuid,uuid,uuid,uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_pay_query_next_receipt_event_v1(uuid,uuid,uuid,uuid) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_pay_query_next_receipt_event_v1(uuid,uuid,uuid,uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_staged_prior_wait_receipt_v1(uuid,uuid) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_staged_prior_wait_receipt_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_expense_authority_evidence_v1(uuid,jsonb) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_expense_authority_evidence_v1(uuid,jsonb) from public,anon,authenticated,service_role;
alter function private.weekly_source_approved_expense_evidence_v1(uuid,jsonb) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_approved_expense_evidence_v1(uuid,jsonb) from public,anon,authenticated,service_role;
alter function private.weekly_source_staged_entitlement_components_v1(uuid,uuid) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_staged_entitlement_components_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_automatic_reconcile_delta_assert_v1(uuid,uuid) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_automatic_reconcile_delta_assert_v1(uuid,uuid) from public,anon,authenticated,service_role;

-- Data-free owner declarations, exact body hashes and early refusal only.
-- No positive component/CAS/transport/release qualification or activation.


do $automatic_declarations$
declare r record; p pg_catalog.pg_proc%rowtype;
begin
  for r in select * from (values
    ('private.weekly_source_staged_source_documents_v1(uuid)','5885da5f42fd5720abafc25e2db83ea0f31a72eda91853dc412ec635d999b426'),
    ('private.weekly_source_staged_publication_context_v1(uuid,uuid)','50abe98b4a7a89d5b0a8aec4577cbd5859b32e9b9232d2eecd85924645eaeea6'),
    ('private.weekly_source_approved_detail_evidence_v1(uuid,jsonb)','9c24f87d60218ff52700e5e31320b2a343c6b5edf51689780908c22705c51e43'),
    ('private.weekly_source_pay_query_local_receipt_event_v1(uuid,uuid,uuid,uuid)','2141c11ae0a472c7154b5a90a1bdaf9dc9fa8365c13d09b160bb68d95ad1e03d'),
    ('private.weekly_source_pay_query_c1_receipt_event_v1(uuid,uuid,uuid,uuid)','4b8a308150ecb3aeb0c5ddcfda81e03626aa6b03454441cb699f3a7c6aa5ef6f'),
    ('private.weekly_source_pay_query_next_receipt_event_v1(uuid,uuid,uuid,uuid)','e307b64f3c4a2be37eb3137ced47f0036bef187f2235c2687adbfd94b1bb5223'),
    ('private.weekly_source_staged_prior_wait_receipt_v1(uuid,uuid)','34b636779b3fa99785d15004fe4c47ce61bedef6b01cda8560040ad3d2858cab'),
    ('private.weekly_source_expense_authority_evidence_v1(uuid,jsonb)','a6e8f983d20fdd210a41b018b07309f5177a9fea3c2a130dccaad84f5dae4eec'),
    ('private.weekly_source_approved_expense_evidence_v1(uuid,jsonb)','61cb875c397d5fee968d805f3b489831af3ab2f0e5c04ef65c72eaf802130b8e'),
    ('private.weekly_source_staged_entitlement_components_v1(uuid,uuid)','227b134b376348d95260dd9f37537604310cffb2aaff4b3ad05b39674a539b84'),
    ('private.weekly_source_automatic_reconcile_delta_assert_v1(uuid,uuid)','f0e596041a2a0a225192a4b79ea887bd96806dd9045ec8635d0927756b85d9da')
  ) expected(signature,body_sha256) loop
    select * into p from pg_catalog.pg_proc where oid=pg_catalog.to_regprocedure(r.signature);
    if not found or p.proowner<>current_user::regrole::oid or not p.prosecdef or p.provolatile<>'s'
      or p.prokind<>'f' or p.pronargdefaults<>0 or p.prorettype<>'jsonb'::regtype::oid
      or p.proconfig is distinct from array['search_path=pg_catalog, pg_temp']::text[]
      or pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(p.prosrc,'UTF8')),'hex')<>r.body_sha256
      or pg_catalog.has_function_privilege('anon',p.oid,'EXECUTE')
      or pg_catalog.has_function_privilege('authenticated',p.oid,'EXECUTE')
      or pg_catalog.has_function_privilege('service_role',p.oid,'EXECUTE')
      or exists(select 1 from pg_catalog.aclexplode(coalesce(p.proacl,pg_catalog.acldefault('f',p.proowner))) a
        where a.grantee=0 and a.privilege_type='EXECUTE') then
      raise exception 'AUTOMATIC_NATIVE_PRIVATE_DECLARATION_FAILED' using errcode='55000',detail=r.signature;
    end if;
  end loop;
  -- Full ordinary receipt wrappers and Local retain their reviewed body bytes.
  for r in select * from (values
    ('public.weekly_exceptional_pay_complete_local_v1(jsonb)','b784c6b55a7255f34f2d46aaf10ed5ad98aceccd22ba400207343bf1c0c6f5cf'),
    ('private.weekly_source_pay_query_local_receipt_v1(uuid,uuid,uuid)','fc492a7e92c911ef618bf26808b9ec9fb483a2d5745e217d76def3f0672a70f7'),
    ('private.weekly_source_pay_query_c1_receipt_v1(uuid,uuid,uuid)','37cb2de5cf4038903079a965171540cb08be6277d3f8ba038bcf9643ca2d3923'),
    ('private.weekly_source_pay_query_next_receipt_v1(uuid,uuid,uuid)','037b250753c5230362737684061bef25e9f1e3673155326784b76240213be711')
  ) expected(signature,body_sha256) loop
    if not exists(select 1 from pg_catalog.pg_proc retained_proc where retained_proc.oid=pg_catalog.to_regprocedure(r.signature)
      and pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(retained_proc.prosrc,'UTF8')),'hex')=r.body_sha256) then
      raise exception 'AUTOMATIC_NATIVE_EXISTING_OWNER_BODY_CHANGED' using errcode='55000',detail=r.signature;
    end if;
  end loop;
end;
$automatic_declarations$;
do $automatic_early_refusals$
declare refused boolean; missing_id constant uuid:='13102026-2206-4000-8000-000000000001';
begin
  refused:=false;
  begin
    perform private.weekly_source_staged_publication_context_v1(null,null);
  exception when sqlstate '22023' then
    if sqlerrm<>'WEEKLY_SOURCE_STAGED_OWNERSHIP_UNAVAILABLE' then raise; end if; refused:=true;
  end;
  if not refused then raise exception 'AUTOMATIC_NATIVE_NULL_STAGE_NOT_REFUSED'; end if;
  refused:=false;
  begin
    perform private.weekly_source_automatic_reconcile_delta_assert_v1(null,null);
  exception when sqlstate '22023' then
    if sqlerrm<>'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_DELTA_UNAVAILABLE' then raise; end if; refused:=true;
  end;
  if not refused then raise exception 'AUTOMATIC_NATIVE_NULL_DELTA_NOT_REFUSED'; end if;
  -- Prove the missing selector before calling the complete owner; it maps
  -- SELECT STRICT no_data_found to its closed 55000 unavailable refusal.
  -- Never touch a real request or accept an arbitrary exception.
  if exists(select 1 from public.weekly_exceptional_c1_publication_requests where id=missing_id) then
    raise exception 'AUTOMATIC_NATIVE_VERIFIER_SENTINEL_COLLISION'; end if;
  refused:=false;
  begin
    perform private.weekly_source_staged_publication_context_v1(missing_id,missing_id);
  exception when sqlstate '55000' then
    if sqlerrm<>'WEEKLY_SOURCE_STAGED_OWNERSHIP_UNAVAILABLE' then raise; end if; refused:=true;
  end;
  if not refused then raise exception 'AUTOMATIC_NATIVE_MISSING_STAGE_NOT_REFUSED'; end if;
  refused:=false;
  begin
    perform private.weekly_source_automatic_reconcile_delta_assert_v1(missing_id,missing_id);
  exception when sqlstate '55000' then
    if sqlerrm<>'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_DELTA_UNAVAILABLE' then raise; end if; refused:=true;
  end;
  if not refused then raise exception 'AUTOMATIC_NATIVE_MISSING_DELTA_NOT_REFUSED'; end if;
  -- Presence is never readiness. Only exercise the proven missing-dependency arm.
  if pg_catalog.to_regprocedure('private.weekly_source_automatic_reconcile_producer_ready_v1(uuid)') is null then
    refused:=false;
    begin
      perform private.weekly_source_automatic_reconcile_capability_assert_v1(null);
    exception when sqlstate '55000' then
      if sqlerrm<>'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_DELTA_UNAVAILABLE' then raise; end if; refused:=true;
    end;
    if not refused then raise exception 'AUTOMATIC_NATIVE_MISSING_CAPABILITY_NOT_REFUSED'; end if;
  end if;
  if pg_catalog.to_regprocedure('private.weekly_source_bulk_producer_ready_v1(uuid)') is null
    or pg_catalog.to_regprocedure('private.weekly_source_bulk_source_cas_v1(uuid,uuid[])') is null then
    refused:=false;
    begin
      perform private.weekly_source_bulk_source_cas_assert_v1(null,'{}'::uuid[]);
    exception when sqlstate '55000' then
      if sqlerrm<>'WEEKLY_SOURCE_BULK_PRODUCER_UNAVAILABLE' then raise; end if; refused:=true;
    end;
    if not refused then raise exception 'AUTOMATIC_NATIVE_MISSING_BULK_NOT_REFUSED'; end if;
  end if;
end;
$automatic_early_refusals$;


