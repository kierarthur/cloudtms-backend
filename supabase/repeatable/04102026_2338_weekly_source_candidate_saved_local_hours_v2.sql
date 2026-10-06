-- Repeatable CloudTMS function/view authority: weekly_source_candidate_saved_local_hours_v2
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- Display-only V4 basis. A confirmed local Save is not first authorisation,
-- a Source HEAD or financial eligibility. Unconfirmed/stale TSFIN is never a
-- fallback. This helper exposes no money, rate or Banking facts.
create or replace function private.weekly_source_candidate_saved_local_hours_v2(
  p_root_timesheet_id uuid
) returns jsonb
language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_root public.timesheets%rowtype;
  v_contract public.contracts%rowtype;
  v_family public.weekly_exceptional_pay_target_families%rowtype;
  v_generation public.weekly_exceptional_pay_generations%rowtype;
  v_receipt private.weekly_source_local_protected_decision_receipts%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_identity jsonb;
  v_basis jsonb;
  v_snapshot jsonb;
  v_segment jsonb;
  v_clock jsonb;
  v_date date;
  v_start timestamp without time zone;
  v_end timestamp without time zone;
  v_break integer;
  v_hours numeric;
  v_total numeric:=0;
  v_rows jsonb:='[]'::jsonb;
  v_seen text[]:=array[]::text[];
  v_unavailable constant jsonb:=jsonb_build_object('state','UNAVAILABLE',
    'reason','SAVED_APPROVED_HOURS_NOT_DERIVABLE','authority',null,'head_id',null,
    'total_hours',null,'rows','[]'::jsonb);
  v_not_processed constant jsonb:=jsonb_build_object('state','NO_APPROVED_ENTITLEMENT',
    'reason','NO_CONFIRMED_SAVED_DECISION','authority',null,'head_id',null,
    'total_hours',null,'rows','[]'::jsonb);
begin
  if p_root_timesheet_id is null then return v_unavailable; end if;
  select * into v_root from public.timesheets t where t.timesheet_id=p_root_timesheet_id;
  if not found or not v_root.is_current or v_root.revoked_at is not null
     or v_root.archived_at_utc is not null or v_root.authorised_at_server is not null
     or v_root.sheet_scope is distinct from 'WEEKLY' or v_root.line_type is distinct from 'HOURS'
     or exists(select 1 from public.weekly_source_root_authorisations a
       where a.root_timesheet_id=p_root_timesheet_id)
     or exists(select 1 from public.weekly_source_entitlement_heads h
       where h.root_timesheet_id=p_root_timesheet_id) then return v_unavailable; end if;
  v_identity:=private.weekly_source_resolve_root_identity_v1(p_root_timesheet_id);
  if v_identity->>'ok' is distinct from 'true'
     or v_identity->>'canonical_timesheet_id' is distinct from p_root_timesheet_id::text
     or v_identity->>'family_is_current' is distinct from 'true'
     or exists(select 1 from public.weekly_source_root_authorisations a
       where a.root_timesheet_id in (select value::uuid from jsonb_array_elements_text(
         v_identity->'member_timesheet_ids')))
     or exists(select 1 from public.weekly_source_entitlement_heads h
       where h.root_timesheet_id in (select value::uuid from jsonb_array_elements_text(
         v_identity->'member_timesheet_ids'))) then
    return v_unavailable;
  end if;
  select * into v_contract from public.contracts contract where contract.id=v_root.contract_id;
  if not found then return v_unavailable; end if;
  select * into v_family from public.weekly_exceptional_pay_target_families family
    where family.root_timesheet_id=p_root_timesheet_id
      and family.root_family_booking_id=v_root.booking_id;
  if not found then return v_not_processed; end if;
  if (select count(*) from public.weekly_exceptional_pay_target_families family
        where family.root_family_booking_id=v_root.booking_id)<>1
     or v_family.ownership_state<>'TARGET_MANAGED'
     or v_family.candidate_id is distinct from v_contract.candidate_id
     or v_family.contract_id is distinct from v_root.contract_id
     or v_family.week_ending_date is distinct from v_root.week_ending_date then return v_unavailable; end if;
  select * into v_generation from public.weekly_exceptional_pay_generations generation
    where generation.id=v_family.current_generation_id and generation.family_id=v_family.id;
  if not found then return v_not_processed; end if;
  select * into v_receipt from private.weekly_source_local_protected_decision_receipts receipt
    where receipt.generation_id=v_generation.id and receipt.family_id=v_family.id
      and receipt.root_timesheet_id=p_root_timesheet_id;
  if not found then return v_not_processed; end if;
  v_basis:=v_receipt.approved_snapshot_json->'saved_unauthorised_basis';
  v_snapshot:=v_generation.complete_next_vector_json#>'{target_snapshot,tsfin_snapshot_json}';
  if v_receipt.state<>'COMPLETE' or v_receipt.completed_at_utc is null
     or v_receipt.result_json->>'authority' is distinct from 'UNAUTHORISED_TSFIN'
     or v_receipt.result_json->'requires_first_authorisation' is distinct from 'true'::jsonb
     or v_generation.lifecycle_state<>'PUBLISHED'
     or v_generation.generation_number is distinct from v_family.current_generation_number
     or v_generation.result_hash is distinct from private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_PROTECTED_LOCAL_RESULT_V1',v_receipt.result_json)
     or v_generation.complete_next_vector_hash is distinct from private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_PROTECTED_COMPLETE_VECTOR_V1',v_generation.complete_next_vector_json)
     or v_family.current_complete_target_vector_hash is distinct from v_generation.complete_next_vector_hash
     or jsonb_typeof(v_snapshot) is distinct from 'object'
     or not private.weekly_exceptional_json_keys_exact_v1(v_basis,array['schema_version',
        'financial_snapshot_id','financial_snapshot_sha256','detail_sha256','inventory_sha256',
        'root_version','family_booking_id'])
     or not (v_basis ?& array['schema_version','financial_snapshot_id',
        'financial_snapshot_sha256','detail_sha256','inventory_sha256','root_version','family_booking_id'])
     or v_basis->>'schema_version' is distinct from 'SAVED_UNAUTHORISED_LOCAL_V1'
     or v_basis->>'root_version' is distinct from v_root.version::text
     or v_basis->>'family_booking_id' is distinct from v_root.booking_id
     or v_basis->>'inventory_sha256' is distinct from encode(v_generation.complete_next_vector_hash,'hex')
     or (v_receipt.approved_snapshot_json-'saved_unauthorised_basis') is distinct from
       (v_snapshot||jsonb_build_object('timesheet_id',v_root.timesheet_id::text,
        'timesheet_version',v_root.version,'actual_schedule_json',
        v_generation.complete_next_vector_json#>'{target_snapshot,actual_schedule_json}'))
     or not exists(select 1 from public.weekly_exceptional_c1_publication_requests request
       join public.weekly_exceptional_orchestration_runs run on run.id=request.orchestration_run_id
       join public.weekly_exceptional_payment_approvals approval on approval.creation_orchestration_run_id=run.id
       where request.id=v_receipt.publication_request_id and request.generation_id=v_generation.id
         and request.request_sha256=v_receipt.request_sha256 and request.state='RETIRED'
         and request.typed_result_json=v_receipt.result_json and run.state='COMPLETE'
         and run.requested_by_user_id=v_receipt.actor_user_id and approval.withdrawn_at_utc is null
         and approval.pay_target_family_id=v_family.id
         and approval.approved_target_pay_components_json->'tsfin_snapshot_json'=v_snapshot) then
    return v_unavailable;
  end if;
  select * into v_fin from public.timesheets_financials financial
    where financial.timesheet_id=p_root_timesheet_id and financial.is_current;
  if not found or (select count(*) from public.timesheets_financials f
      where f.timesheet_id=p_root_timesheet_id and f.is_current)<>1
     or v_basis->>'financial_snapshot_id' is distinct from v_fin.id::text
     or v_receipt.result_json->>'timesheet_financials_id' is distinct from v_fin.id::text
     or v_fin.timesheet_version is distinct from v_root.version
     or v_fin.candidate_id is distinct from v_contract.candidate_id
     or v_fin.client_id is distinct from v_contract.client_id
     or v_fin.authorised_at_utc is not null or v_fin.processing_status<>'PENDING_AUTH'
     or v_fin.paid_at_utc is not null or v_fin.locked_by_invoice_id is not null
     or v_basis->>'financial_snapshot_sha256' is distinct from encode(private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_SAVED_UNAUTHORISED_FINANCIAL_V1',to_jsonb(v_fin)),'hex')
     or v_basis->>'detail_sha256' is distinct from encode(private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_SAVED_UNAUTHORISED_DETAIL_V1',jsonb_build_object(
          'segments',v_fin.invoice_breakdown_json->'segments',
          'actual_schedule',v_fin.actual_schedule_json,'additional_units',v_fin.additional_units_json)),'hex')
     or jsonb_typeof(v_fin.invoice_breakdown_json->'segments') is distinct from 'array'
     or jsonb_array_length(v_fin.invoice_breakdown_json->'segments')>1000 then return v_unavailable; end if;
  for v_segment in select value from jsonb_array_elements(v_fin.invoice_breakdown_json->'segments') loop
    if jsonb_typeof(v_segment) is distinct from 'object'
       or nullif(btrim(v_segment->>'segment_id'),'') is null
       or v_segment->>'segment_id'=any(v_seen)
       or jsonb_typeof(v_segment->'exclude_from_pay') is distinct from 'boolean' then return v_unavailable; end if;
    v_seen:=array_append(v_seen,v_segment->>'segment_id');
    if (v_segment->>'exclude_from_pay')::boolean then continue; end if;
    if coalesce(v_segment->>'date','') !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
       then return v_unavailable; end if;
    v_date:=(v_segment->>'date')::date;
    if v_date not between v_root.week_ending_date-6 and v_root.week_ending_date then return v_unavailable; end if;
    v_clock:=private.weekly_source_candidate_approved_clock_row_v2(
      v_segment,v_segment,'approved-'||(v_segment->>'segment_id'));
    if v_clock is null then return v_unavailable; end if;
    v_hours:=(v_clock->>'hours')::numeric;
    v_rows:=v_rows||jsonb_build_array(v_clock->'row');
    v_total:=v_total+v_hours;
  end loop;
  if v_total is distinct from v_fin.total_hours then return v_unavailable; end if;
  select coalesce(jsonb_agg(r.value order by r.value->>'date',
    r.value->>'start' collate "C",r.value->>'row_key' collate "C"),'[]'::jsonb)
    into v_rows from jsonb_array_elements(v_rows) r(value);
  return jsonb_build_object('state','AVAILABLE','reason',null,
    'authority','SAVED_UNAUTHORISED_LOCAL_V1','head_id',null,
    'total_hours',v_total,'rows',v_rows);
end;
$function$;

alter function private.weekly_source_candidate_saved_local_hours_v2(uuid) owner to postgres;
revoke all on function private.weekly_source_candidate_saved_local_hours_v2(uuid)
  from public,anon,authenticated,service_role;

commit;
