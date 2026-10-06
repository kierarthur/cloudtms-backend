-- Repeatable CloudTMS function/view authority: banking_pay_next_local_source_chosen_detail_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- This unpublished private candidate changed its internal signature. Never
-- leave an older authority overload installed or blindly drop unknown code.
do $local_capture_signature$
begin
  if pg_catalog.to_regprocedure(
      'private.bpay_next_capture_local_source_detail_v1(uuid,uuid,bigint,uuid,uuid,uuid)') is not null then
    raise exception using errcode='55000',message='BPAY_NEXT_LOCAL_DETAIL_OBSOLETE_SIGNATURE_PRESENT';
  end if;
end;
$local_capture_signature$;

-- Additive, owner-only Local chosen detail. The sole common publisher calls
-- after the genuine STAGED head/full inventory exists and before activation/I1.
-- It has already locked/validated the accepted request. No Final identity,
-- current financial fallback, calculation, public endpoint or new table.
create or replace function private.bpay_next_capture_local_source_detail_v1(
  p_head_id uuid,p_decision_bundle_id uuid,p_bundle_revision bigint,
  p_root_timesheet_id uuid,p_publication_request_id uuid,p_generation_id uuid,
  p_qualified_context jsonb
) returns integer
language plpgsql security definer
set search_path = pg_catalog, private, public
as $function$
declare
  v_local private.weekly_source_local_protected_decision_receipts%rowtype;
  v_generation public.weekly_exceptional_pay_generations%rowtype;
  v_bundle public.weekly_source_entitlement_decision_bundles%rowtype;
  v_head public.weekly_source_entitlement_heads%rowtype;
  v_root public.timesheets%rowtype;
  v_contract public.contracts%rowtype;
  v_approval public.weekly_exceptional_payment_approvals%rowtype;
  v_before_authorisation public.weekly_source_root_authorisations%rowtype;
  v_before_head public.weekly_source_entitlement_heads%rowtype;
  v_before_financial public.timesheets_financials%rowtype;
  v_before_origin jsonb;
  v_component public.weekly_source_entitlement_head_components%rowtype;
  v_existing private.bpay_next_source_chosen_detail%rowtype;
  v_origin jsonb;
  v_qualification jsonb;
  v_scope jsonb;
  v_context jsonb;
  v_snapshot jsonb;
  v_segments jsonb;
  v_schedule jsonb;
  v_components jsonb:='[]'::jsonb;
  v_worked jsonb;
  v_canonical jsonb;
  v_segment jsonb;
  v_raw jsonb;
  v_detail jsonb;
  v_component_sha bytea;
  v_detail_sha bytea;
  v_rates jsonb;
  v_charge_rates jsonb;
  v_rate numeric;
  v_charge_rate numeric;
  v_hours numeric;
  v_key text;
  v_ordinal integer;
  v_matches integer;
  v_component_count integer:=0;
  v_work_count integer:=0;
  v_count integer:=0;
  v_retained boolean;
  v_break jsonb;
  v_breaks jsonb;
  v_break_minutes integer;
  v_break_total integer;
  v_clock_minutes integer;
  v_start_utc timestamptz;
  v_end_utc timestamptz;
begin
  if p_head_id is null or p_decision_bundle_id is null or p_bundle_revision is null
     or p_bundle_revision<1 or p_root_timesheet_id is null
     or p_publication_request_id is null or p_generation_id is null then
    raise exception using errcode='22023',message='BPAY_NEXT_LOCAL_DETAIL_INPUT_INVALID';
  end if;
  perform 1 from private.bpay_next_module_control
    where id=1 and active_owner='NEXT' for share;
  if not found then return 0; end if;

  select * into v_local from private.weekly_source_local_protected_decision_receipts
    where publication_request_id=p_publication_request_id;
  if not found or v_local.generation_id is distinct from p_generation_id
     or v_local.root_timesheet_id is distinct from p_root_timesheet_id
     or v_local.common_decision_bundle_id is distinct from p_decision_bundle_id
     or v_local.common_bundle_revision is distinct from p_bundle_revision
     or v_local.publication_origin_kind is distinct from 'PROTECTED_LOCAL_DECISION_V1'
     or v_local.state not in ('PREPARING','PENDING_FREEZE','COMPLETE') then
    raise exception using errcode='23514',message='BPAY_NEXT_LOCAL_DETAIL_RECEIPT_NOT_EXACT';
  end if;
  v_snapshot:=v_local.approved_snapshot_json;
  v_origin:=private.weekly_source_local_origin_canonical_v2(v_snapshot->'local_publication_origin');
  v_qualification:=v_snapshot->'local_source_qualification';
  v_scope:=v_qualification->'scope';
  if v_origin->>'publication_request_id' is distinct from p_publication_request_id::text
     or v_origin->>'generation_id' is distinct from p_generation_id::text
     or v_origin->>'request_sha256' is distinct from pg_catalog.encode(v_local.request_sha256,'hex')
     or v_local.publication_origin_digest is distinct from
        private.weekly_source_publication_request_digest_v1(v_origin)
     or v_local.source_qualification_digest is distinct from
        private.weekly_source_sha256_jsonb_v1('PROTECTED_LOCAL_SOURCE_QUALIFICATION_V2',v_qualification)
     or v_origin->>'source_qualification_digest' is distinct from
        pg_catalog.encode(v_local.source_qualification_digest,'hex')
     or v_qualification->>'schema_version' is distinct from 'PROTECTED_LOCAL_SOURCE_QUALIFICATION_V2'
     or v_qualification->>'publication_request_id' is distinct from p_publication_request_id::text
     or v_qualification->>'generation_id' is distinct from p_generation_id::text
     or v_qualification->>'actor_user_id' is distinct from v_local.actor_user_id::text
     or v_qualification->>'rate_policy_fingerprint' is distinct from v_origin->>'policy_fingerprint'
     or v_scope->>'root_timesheet_id' is distinct from p_root_timesheet_id::text
     or v_scope->>'target_family_id' is distinct from v_local.family_id::text then
    raise exception using errcode='23514',message='BPAY_NEXT_LOCAL_DETAIL_SEAL_NOT_EXACT';
  end if;
  select * into v_bundle from public.weekly_source_entitlement_decision_bundles
    where decision_bundle_id=p_decision_bundle_id and bundle_revision=p_bundle_revision;
  if not found or v_bundle.bundle_kind is distinct from 'SINGLE_ROOT'
     or v_bundle.source_root_timesheet_id is distinct from p_root_timesheet_id
     or v_bundle.target_root_timesheet_id is not null
     or v_bundle.proposed_head_ids is distinct from array[p_head_id]
     or v_bundle.decided_by_user_id is distinct from v_local.actor_user_id
     or v_bundle.source_revision_digest is distinct from v_local.publication_origin_digest then
    raise exception using errcode='23514',message='BPAY_NEXT_LOCAL_DETAIL_BUNDLE_NOT_EXACT';
  end if;
  select * into v_head from public.weekly_source_entitlement_heads where id=p_head_id;
  if not found or v_head.authority_kind is distinct from 'PROTECTED'
     or v_head.decision_bundle_id is distinct from p_decision_bundle_id
     or v_head.bundle_revision is distinct from p_bundle_revision
     or v_head.root_timesheet_id is distinct from p_root_timesheet_id
     or v_head.root_family_booking_id is distinct from v_bundle.source_root_family_booking_id
     or v_head.root_timesheet_version::text is distinct from v_scope->>'root_version'
     or v_head.candidate_id is distinct from v_bundle.candidate_id
     or v_head.contract_id is distinct from v_bundle.source_contract_id
     or v_head.week_ending_date is distinct from v_bundle.week_ending_date
     or v_head.agency_id is distinct from v_bundle.agency_id
     or v_head.decision_id is distinct from v_bundle.decision_id
     or v_head.decided_by_user_id is distinct from v_local.actor_user_id
     or v_head.source_origin_json is distinct from v_origin
     or v_head.source_generation_digest is distinct from v_local.publication_origin_digest then
    raise exception using errcode='23514',message='BPAY_NEXT_LOCAL_DETAIL_HEAD_NOT_EXACT';
  end if;
  if v_scope->>'family_booking_id' is distinct from v_head.root_family_booking_id
     or v_scope->>'candidate_id' is distinct from v_head.candidate_id::text
     or coalesce(v_scope->>'client_id','') !~
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     or v_scope->>'contract_id' is distinct from v_head.contract_id::text
     or v_scope->>'week_ending_date' is distinct from pg_catalog.to_char(v_head.week_ending_date,'YYYY-MM-DD')
     or v_snapshot->>'timesheet_id' is distinct from p_root_timesheet_id::text
     or (v_snapshot->>'timesheet_version')::integer is distinct from v_head.root_timesheet_version
     or v_snapshot->>'candidate_id' is distinct from v_head.candidate_id::text
     or v_snapshot->>'client_id' is distinct from v_scope->>'client_id'
     or v_snapshot#>>'{rate_source_refs_json,mode}' is distinct from 'CONTRACT_RATES_JSON'
     or v_snapshot#>>'{rate_source_refs_json,contract_id}' is distinct from v_head.contract_id::text then
    raise exception using errcode='23514',message='BPAY_NEXT_LOCAL_DETAIL_SCOPE_NOT_EXACT';
  end if;
  select * into strict v_generation from public.weekly_exceptional_pay_generations where id=p_generation_id;
  select * into strict v_approval from public.weekly_exceptional_payment_approvals
    where id=(v_qualification->>'approval_id')::uuid;
  if v_generation.family_id is distinct from v_local.family_id
     or private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_COMPLETE_VECTOR_V1',
        v_generation.complete_next_vector_json) is distinct from v_generation.complete_next_vector_hash
     or v_qualification->>'complete_vector_hash' is distinct from pg_catalog.encode(v_generation.complete_next_vector_hash,'hex')
     or v_approval.pay_target_family_id is distinct from v_local.family_id
     or v_approval.approved_by_user_id is distinct from v_local.actor_user_id
     or v_approval.candidate_id is distinct from v_head.candidate_id
     or v_approval.client_id::text is distinct from v_scope->>'client_id'
     or v_approval.contract_id is distinct from v_head.contract_id
     or v_approval.week_ending is distinct from v_head.week_ending_date
     or v_qualification->>'work_event_id' is distinct from v_approval.work_event_id::text
     or v_qualification->>'source_cycle_id' is distinct from v_approval.source_cycle_id::text
     or v_snapshot-array['common_decision_bundle_id','common_components_digest',
          'local_source_qualification','local_publication_origin'] is distinct from
        (v_generation.complete_next_vector_json#>'{target_snapshot,tsfin_snapshot_json}'
          ||pg_catalog.jsonb_build_object('timesheet_id',p_root_timesheet_id::text,
             'timesheet_version',v_head.root_timesheet_version,'actual_schedule_json',
             v_generation.complete_next_vector_json#>'{target_snapshot,actual_schedule_json}'))
     or v_generation.complete_next_vector_json#>'{target_snapshot,tsfin_snapshot_json}' is distinct from
        v_approval.approved_target_pay_components_json->'tsfin_snapshot_json' then
    raise exception using errcode='23514',message='BPAY_NEXT_LOCAL_DETAIL_APPROVAL_NOT_EXACT';
  end if;

  -- Certify the COMPLETE vector, not only the worked subset, against the
  -- original receipt's canonical digest. DEC values are strings; DATE is ISO.
  for v_component in select c.* from public.weekly_source_entitlement_head_components c
    where c.head_id=p_head_id order by c.component_ordinal
  loop
    v_canonical:=private.weekly_source_publication_component_canonical_v1(
      (pg_catalog.to_jsonb(v_component)-array['id','head_id','component_sha256',
        'decision_bundle_id','bundle_revision','created_at_utc'])||pg_catalog.jsonb_build_object(
        'work_date',pg_catalog.to_char(v_component.work_date,'YYYY-MM-DD'),
        'hours_day',v_component.hours_day::text,'hours_night',v_component.hours_night::text,
        'hours_sat',v_component.hours_sat::text,'hours_sun',v_component.hours_sun::text,
        'hours_bh',v_component.hours_bh::text,'unit_count',v_component.unit_count::text,
        'unit_pay_rate',v_component.unit_pay_rate::text,'unit_charge_rate',v_component.unit_charge_rate::text,
        'pay_ex_vat',v_component.pay_ex_vat::text,'charge_ex_vat',v_component.charge_ex_vat::text),
      'bpay_next_local_detail.component');
    v_component_sha:=private.weekly_source_publication_request_digest_v1(
      private.weekly_source_publication_component_content_v1(v_canonical));
    v_component_count:=v_component_count+1;
    if v_component.component_ordinal is distinct from v_component_count
       or v_component.decision_bundle_id is distinct from p_decision_bundle_id
       or v_component.bundle_revision is distinct from p_bundle_revision
       or v_component.component_sha256 is distinct from v_component_sha then
      raise exception using errcode='23514',message='BPAY_NEXT_LOCAL_DETAIL_COMPONENT_NOT_EXACT';
    end if;
    v_components:=v_components||pg_catalog.jsonb_build_array(v_canonical);
    if v_component.component_kind='WORKED_TIME' then v_work_count:=v_work_count+1; end if;
  end loop;
  if v_component_count is distinct from v_head.component_count
     or v_head.certified_zero is distinct from (v_component_count=0)
     or v_component_count is distinct from (v_generation.complete_next_vector_json->>'component_count')::integer
     or v_snapshot->>'common_components_digest' is distinct from pg_catalog.encode(
        private.weekly_source_publication_request_digest_v1(v_components),'hex') then
    raise exception using errcode='23514',message='BPAY_NEXT_LOCAL_DETAIL_VECTOR_NOT_EXACT';
  end if;

  -- A retained COMPLETE replay reads only the sealed receipt, immutable
  -- generation/components/detail. It never requalifies a later live context.
  -- Missing detail at COMPLETE is NOT a historical backfill capability.
  v_retained:=v_local.state='COMPLETE';
  if v_retained then
    if v_head.state not in ('COMMITTED_CURRENT','SUPERSEDED')
       or v_head.publication_receipt_digest is null or v_head.committed_at_utc is null
       or v_bundle.state not in ('COMMITTED','SUPERSEDED')
       or (select pg_catalog.count(*) from private.bpay_next_source_chosen_detail d
           where d.head_id=p_head_id) is distinct from v_work_count::bigint then
      raise exception using errcode='23514',message='BPAY_NEXT_LOCAL_DETAIL_REPLAY_NOT_COMPLETE';
    end if;
  else
    if v_head.state is distinct from 'STAGED' or v_bundle.state is distinct from 'PROPOSED' then
      raise exception using errcode='23514',message='BPAY_NEXT_LOCAL_DETAIL_NOT_STAGED';
    end if;
    select * into strict v_root from public.timesheets where timesheet_id=p_root_timesheet_id;
    select * into strict v_contract from public.contracts where id=v_bundle.source_contract_id;
    if v_root.booking_id is distinct from v_head.root_family_booking_id
       or v_root.version is distinct from v_head.root_timesheet_version
       or v_root.contract_id is distinct from v_head.contract_id
       or v_root.week_ending_date is distinct from v_head.week_ending_date
       or v_root.is_adjustment is distinct from false
       or v_contract.candidate_id is distinct from v_head.candidate_id
       or v_contract.client_id::text is distinct from v_scope->>'client_id' then
      raise exception using errcode='23514',message='BPAY_NEXT_LOCAL_DETAIL_SCOPE_NOT_EXACT';
    end if;
    -- The sole publisher qualified the actual live whole before-position
    -- before inserting its own STAGED HEAD, while holding the family/root
    -- locks. Recalling generic I1 now would correctly refuse initial TSFIN
    -- because that HEAD exists. Carry that exact private transaction-local
    -- proof instead; never make I1 ignore history or accept a public context.
    v_context:=p_qualified_context;
    if pg_catalog.jsonb_typeof(v_context) is distinct from 'object'
       or pg_catalog.jsonb_typeof(v_context->'qualified_transaction_id') is distinct from 'string'
       or v_context->>'qualified_transaction_id' is distinct from pg_catalog.pg_current_xact_id()::text
       or (v_local.state='PREPARING' and v_local.preparing_transaction_id
          is distinct from pg_catalog.pg_current_xact_id())
       or v_context->'scope' is distinct from v_scope
       or v_context->'qualification' is distinct from v_qualification
       or v_context->>'actor_user_id' is distinct from v_local.actor_user_id::text
       or v_context->>'agency_id' is distinct from v_head.agency_id::text
       or v_context->>'source_qualification_digest' is distinct from v_origin->>'source_qualification_digest'
       or v_context->>'policy_fingerprint' is distinct from v_origin->>'policy_fingerprint'
       or v_context->'before_origin' is distinct from v_origin->'before_origin'
       or v_context->'before_inventory_digest' is distinct from v_origin->'before_inventory_digest' then
      raise exception using errcode='23514',message='BPAY_NEXT_LOCAL_DETAIL_CONTEXT_UNAVAILABLE';
    end if;
    -- Exact PK rebind, not another inventory/history chooser. The previously
    -- qualified authorisation/pointer must still be current before capture.
    v_before_origin:=v_context->'before_origin';
    select * into v_before_authorisation from public.weekly_source_root_authorisations
      where id=(v_before_origin->>'root_authorisation_id')::uuid;
    if not found or v_before_authorisation.withdrawn_at_utc is not null
       or v_before_authorisation.root_timesheet_id is distinct from p_root_timesheet_id
       or v_before_authorisation.family_booking_id is distinct from v_head.root_family_booking_id
       or v_before_authorisation.timesheet_version is distinct from v_head.root_timesheet_version
       or v_before_authorisation.authorisation_generation::text
          is distinct from v_before_origin->>'authorisation_generation' then
      raise exception using errcode='23514',message='BPAY_NEXT_LOCAL_DETAIL_BEFORE_AUTHORITY_CHANGED';
    end if;
    if v_before_origin->>'kind'='INITIAL_AUTHORISED_TSFIN_V1' then
      select * into v_before_financial from public.timesheets_financials
        where id=(v_before_origin->>'financial_snapshot_id')::uuid;
      if not found or v_before_authorisation.current_entitlement_head_id is not null
         or v_head.prior_head_id is not null
         or v_before_financial.timesheet_id is distinct from p_root_timesheet_id
         or v_before_financial.timesheet_version is distinct from v_head.root_timesheet_version
         or v_before_financial.is_current is distinct from true
         or v_before_financial.is_stale is distinct from false
         or v_before_financial.authorised_at_utc is null
         or v_before_authorisation.authorised_row_signature
            is distinct from v_before_origin->>'authorised_row_signature' then
        raise exception using errcode='23514',message='BPAY_NEXT_LOCAL_DETAIL_BEFORE_AUTHORITY_CHANGED';
      end if;
    elsif v_before_origin->>'kind'='COMMITTED_SOURCE_HEAD_V1' then
      select * into v_before_head from public.weekly_source_entitlement_heads
        where id=(v_before_origin->>'head_id')::uuid;
      if not found or v_before_head.state is distinct from 'COMMITTED_CURRENT'
         or v_before_authorisation.current_entitlement_head_id is distinct from v_before_head.id
         or v_head.prior_head_id is distinct from v_before_head.id
         or v_before_head.root_timesheet_id is distinct from p_root_timesheet_id
         or v_before_head.root_family_booking_id is distinct from v_head.root_family_booking_id
         or v_before_head.root_timesheet_version is distinct from v_head.root_timesheet_version
         or v_before_head.head_revision::text is distinct from v_before_origin->>'head_revision'
         or v_before_head.decision_bundle_id::text is distinct from v_before_origin->>'decision_bundle_id'
         or v_before_head.bundle_revision::text is distinct from v_before_origin->>'bundle_revision'
         or pg_catalog.encode(v_before_head.source_generation_digest,'hex')
            is distinct from v_before_origin->>'source_generation_digest'
         or pg_catalog.encode(v_before_head.inventory_digest,'hex')
            is distinct from v_before_origin->>'inventory_digest'
         or pg_catalog.encode(v_before_head.entitlement_digest,'hex')
            is distinct from v_before_origin->>'entitlement_digest' then
        raise exception using errcode='23514',message='BPAY_NEXT_LOCAL_DETAIL_BEFORE_AUTHORITY_CHANGED';
      end if;
    else
      raise exception using errcode='23514',message='BPAY_NEXT_LOCAL_DETAIL_BEFORE_AUTHORITY_CHANGED';
    end if;
  end if;

  v_segments:=v_snapshot#>'{invoice_breakdown_json,segments}';
  v_schedule:=v_generation.complete_next_vector_json#>'{target_snapshot,actual_schedule_json}';
  if pg_catalog.jsonb_typeof(v_segments) is distinct from 'array'
     or pg_catalog.jsonb_typeof(v_schedule) is distinct from 'array'
     or pg_catalog.jsonb_array_length(v_segments) is distinct from v_work_count
     or pg_catalog.jsonb_array_length(v_schedule) is distinct from v_work_count
     or v_snapshot->'actual_schedule_json' is distinct from v_schedule
     or exists(select 1 from pg_catalog.jsonb_array_elements(v_segments) s(value)
       group by s.value->>'segment_id' having pg_catalog.count(*)<>1)
     or exists(select 1 from pg_catalog.jsonb_array_elements(v_schedule) s(value)
       group by s.value->>'work_event_id' having pg_catalog.count(*)<>1) then
    raise exception using errcode='23514',message='BPAY_NEXT_LOCAL_DETAIL_ASSOCIATION_NOT_UNIQUE';
  end if;
  -- This is the existing component composer, not the calculator. Its unique
  -- segment ID binds the head to a particular sealed output; output ordinal
  -- then binds the same producer's accepted schedule input. No clock search.
  v_worked:=private.weekly_source_entitlement_components_v1(v_segments,'[]'::jsonb);
  for v_component in select c.* from public.weekly_source_entitlement_head_components c
    where c.head_id=p_head_id and c.component_kind='WORKED_TIME' order by c.component_ordinal
  loop
    select pg_catalog.count(*)::integer into v_matches
      from pg_catalog.jsonb_array_elements(v_segments) s(value)
      where s.value->>'segment_id'=v_component.segment_id;
    if v_matches<>1 or v_component.segment_id is null then
      raise exception using errcode='23514',message='BPAY_NEXT_LOCAL_DETAIL_SEGMENT_NOT_EXACT';
    end if;
    select s.value,s.ordinality::integer into strict v_segment,v_ordinal
      from pg_catalog.jsonb_array_elements(v_segments) with ordinality s(value,ordinality)
      where s.value->>'segment_id'=v_component.segment_id;
    v_raw:=v_schedule->(v_ordinal-1);
    select c.value into strict v_canonical from pg_catalog.jsonb_array_elements(v_components) c(value)
      where c.value->>'component_id'=v_component.component_id::text;
    if v_canonical is distinct from private.weekly_source_publication_component_canonical_v1(
        v_worked->(v_ordinal-1),'bpay_next_local_detail.sealed_segment')
       or v_component.component_ordinal is distinct from v_ordinal
       or pg_catalog.jsonb_typeof(v_raw) is distinct from 'object'
       or coalesce(v_raw->>'work_event_id','') !~
          '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       or v_segment->>'date' is distinct from v_raw->>'date'
       or v_segment->>'date' is distinct from pg_catalog.to_char(v_component.work_date,'YYYY-MM-DD')
       or v_segment->>'start' is distinct from v_raw->>'start'
       or v_segment->>'end' is distinct from v_raw->>'end'
       or (v_segment->>'pay_amount')::numeric is distinct from v_component.pay_ex_vat
       or (v_segment->>'charge_amount')::numeric is distinct from v_component.charge_ex_vat
       or (v_segment->>'exclude_from_pay')::boolean is distinct from v_component.exclude_from_pay
       or not exists(select 1 from public.weekly_work_events e
          where e.id=(v_raw->>'work_event_id')::uuid and e.candidate_id=v_head.candidate_id
            and e.client_id::text=v_scope->>'client_id' and e.work_date=v_component.work_date) then
      raise exception using errcode='23514',message='BPAY_NEXT_LOCAL_DETAIL_PRODUCER_NOT_EXACT';
    end if;
    if coalesce(v_segment->>'start','') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
       or coalesce(v_segment->>'end','') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
       or pg_catalog.jsonb_typeof(v_segment->'overnight') is distinct from 'boolean'
       or (v_segment->>'overnight')::boolean is distinct from
          ((v_segment->>'end')::time<=(v_segment->>'start')::time)
       or coalesce(v_segment->>'break_mins','') !~ '^(0|[1-9][0-9]*)$'
       or pg_catalog.jsonb_typeof(v_segment->'breaks') is distinct from 'array'
       or v_segment->'breaks' is distinct from coalesce(v_raw->'breaks','[]'::jsonb) then
      raise exception using errcode='23514',message='BPAY_NEXT_LOCAL_DETAIL_CLOCK_BREAK_NOT_EXACT';
    end if;
    if (pg_catalog.jsonb_array_length(v_segment->'breaks')=0
        and (v_segment->>'break_mins')::integer is distinct from coalesce((v_raw->>'break_mins')::integer,0))
       or (pg_catalog.jsonb_array_length(v_segment->'breaks')>0 and (v_segment->>'break_mins')::integer<>0) then
      raise exception using errcode='23514',message='BPAY_NEXT_LOCAL_DETAIL_CLOCK_BREAK_NOT_EXACT';
    end if;
    v_rates:='{}'::jsonb; v_charge_rates:='{}'::jsonb;
    foreach v_key in array array['day','night','sat','sun','bh'] loop
      v_hours:=(v_segment->>('hours_'||v_key))::numeric;
      if v_hours is distinct from (v_canonical->>('hours_'||v_key))::numeric then
        raise exception using errcode='23514',message='BPAY_NEXT_LOCAL_DETAIL_BUCKET_NOT_EXACT';
      end if;
      v_rate:=(v_snapshot->>('pay_'||v_key))::numeric;
      v_charge_rate:=(v_snapshot->>('charge_'||v_key))::numeric;
      if (v_rate is not null and (v_rate<>pg_catalog.round(v_rate,6) or v_rate::text in ('NaN','Infinity','-Infinity')))
         or (v_charge_rate is not null and (v_charge_rate<>pg_catalog.round(v_charge_rate,6)
           or v_charge_rate::text in ('NaN','Infinity','-Infinity')))
         or (coalesce(v_hours,0)<>0 and not v_component.exclude_from_pay and v_rate is null)
         or (coalesce(v_hours,0)<>0 and v_charge_rate is null) then
        raise exception using errcode='23514',message='BPAY_NEXT_LOCAL_DETAIL_USED_RATE_MISSING';
      end if;
      v_rates:=v_rates||pg_catalog.jsonb_build_object(v_key,v_rate);
      v_charge_rates:=v_charge_rates||pg_catalog.jsonb_build_object(v_key,v_charge_rate);
    end loop;
    v_breaks:=v_segment->'breaks'; v_break_total:=0;
    for v_break in select value from pg_catalog.jsonb_array_elements(v_breaks) loop
      if pg_catalog.jsonb_typeof(v_break) is distinct from 'object'
         or coalesce(v_break->>'start','') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
         or coalesce(v_break->>'end','') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$' then
        raise exception using errcode='23514',message='BPAY_NEXT_LOCAL_DETAIL_BREAK_INVALID';
      end if;
      v_break_minutes:=((pg_catalog.substring(v_break->>'end',1,2)::integer*60
        +pg_catalog.substring(v_break->>'end',4,2)::integer)
        -(pg_catalog.substring(v_break->>'start',1,2)::integer*60
        +pg_catalog.substring(v_break->>'start',4,2)::integer)+1440)%1440;
      if v_break_minutes=0 then
        raise exception using errcode='23514',message='BPAY_NEXT_LOCAL_DETAIL_BREAK_INVALID';
      end if;
      v_break_total:=v_break_total+v_break_minutes;
    end loop;
    if pg_catalog.jsonb_array_length(v_breaks)=0 then
      v_break_total:=(v_segment->>'break_mins')::integer;
    end if;
    v_clock_minutes:=((pg_catalog.substring(v_segment->>'end',1,2)::integer*60
      +pg_catalog.substring(v_segment->>'end',4,2)::integer)
      -(pg_catalog.substring(v_segment->>'start',1,2)::integer*60
      +pg_catalog.substring(v_segment->>'start',4,2)::integer)+1440)%1440;
    if v_clock_minutes=0 then v_clock_minutes:=1440; end if;
    if v_break_total>v_clock_minutes then
      raise exception using errcode='23514',message='BPAY_NEXT_LOCAL_DETAIL_BREAK_INVALID';
    end if;
    -- Exact break windows are copied verbatim. Their factual duration is the
    -- display total: the genuine normalizer sets break_mins=0 when windows exist.
    if pg_catalog.jsonb_typeof(v_segment->'start_utc') is distinct from 'string'
       or pg_catalog.jsonb_typeof(v_segment->'end_utc') is distinct from 'string'
       or coalesce(v_segment->>'start_utc','') !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,6})?(Z|[+-][0-9]{2}:[0-9]{2})$'
       or coalesce(v_segment->>'end_utc','') !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,6})?(Z|[+-][0-9]{2}:[0-9]{2})$' then
      raise exception using errcode='23514',message='BPAY_NEXT_LOCAL_DETAIL_UTC_INVALID';
    end if;
    begin
      v_start_utc:=(v_segment->>'start_utc')::timestamptz;
      v_end_utc:=(v_segment->>'end_utc')::timestamptz;
    exception when invalid_datetime_format or datetime_field_overflow or invalid_time_zone_displacement_value then
      raise exception using errcode='23514',message='BPAY_NEXT_LOCAL_DETAIL_UTC_INVALID';
    end;
    if not pg_catalog.isfinite(v_start_utc) or not pg_catalog.isfinite(v_end_utc)
       or v_end_utc<=v_start_utc
       or pg_catalog.to_char(v_start_utc at time zone 'Europe/London','YYYY-MM-DD') is distinct from v_segment->>'date'
       or pg_catalog.to_char(v_start_utc at time zone 'Europe/London','HH24:MI') is distinct from v_segment->>'start'
       or pg_catalog.to_char(v_end_utc at time zone 'Europe/London','HH24:MI') is distinct from v_segment->>'end'
       or (v_end_utc at time zone 'Europe/London')::date is distinct from
          v_component.work_date+(case when (v_segment->>'overnight')::boolean then 1 else 0 end) then
      raise exception using errcode='23514',message='BPAY_NEXT_LOCAL_DETAIL_UTC_INVALID';
    end if;
    v_detail:=pg_catalog.jsonb_build_object(
      'work_date',pg_catalog.to_char(v_component.work_date,'YYYY-MM-DD'),
      'start',v_segment->>'start','end',v_segment->>'end',
      'overnight',(v_segment->>'overnight')::boolean,'break_minutes',v_break_total,
      'breaks',v_breaks,'start_utc',v_segment->'start_utc','end_utc',v_segment->'end_utc',
      'work_event_id',(v_raw->>'work_event_id')::uuid,'rates',v_rates,'charge_rates',v_charge_rates,
      'local_publication_request_id',p_publication_request_id,'local_generation_id',p_generation_id,
      'local_approval_id',v_approval.id,'local_origin_sha256',pg_catalog.encode(v_local.publication_origin_digest,'hex'),
      'source_component_id',v_component.component_id,'source_component_sha256',pg_catalog.encode(v_component.component_sha256,'hex'),
      'sealed_segment_id',v_component.segment_id,'sealed_segment_ordinal',v_ordinal);
    if pg_catalog.octet_length(v_detail::text)>4096 then
      raise exception using errcode='23514',message='BPAY_NEXT_LOCAL_DETAIL_TOO_LARGE';
    end if;
    v_detail_sha:=pg_catalog.sha256(pg_catalog.convert_to(v_detail::text,'UTF8'));
    if not v_retained then
      insert into private.bpay_next_source_chosen_detail
        (head_id,component_id,decision_bundle_id,bundle_revision,component_sha256,detail_json,detail_sha256)
      values(p_head_id,v_component.component_id,p_decision_bundle_id,p_bundle_revision,
        v_component.component_sha256,v_detail,v_detail_sha)
      on conflict (head_id,component_id) do nothing;
    end if;
    select * into v_existing from private.bpay_next_source_chosen_detail
      where head_id=p_head_id and component_id=v_component.component_id;
    if not found or v_existing.decision_bundle_id is distinct from p_decision_bundle_id
       or v_existing.bundle_revision is distinct from p_bundle_revision
       or v_existing.component_sha256 is distinct from v_component.component_sha256
       or v_existing.detail_sha256 is distinct from v_detail_sha
       or v_existing.detail_json is distinct from v_detail then
      raise exception using errcode='23514',message='BPAY_NEXT_LOCAL_DETAIL_REPLAY_CONFLICT';
    end if;
    v_count:=v_count+1;
  end loop;
  if v_count<>v_work_count then
    raise exception using errcode='23514',message='BPAY_NEXT_LOCAL_DETAIL_COUNT_MISMATCH';
  end if;
  return v_count;
end
$function$;
alter function private.bpay_next_capture_local_source_detail_v1(uuid,uuid,bigint,uuid,uuid,uuid,jsonb) owner to postgres;
revoke all on function private.bpay_next_capture_local_source_detail_v1(uuid,uuid,bigint,uuid,uuid,uuid,jsonb)
  from public,anon,authenticated,service_role;

commit;
