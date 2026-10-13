create or replace function public.weekly_exceptional_pay_read_c1_request_v1(
  p_request jsonb
) returns jsonb
language plpgsql stable security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_keys constant text[]:=array[
    'schema_version','actor_user_id','publication_request_id',
    'expected_request_sha256'
  ];
  v_actor_user_id uuid;
  v_request_id uuid;
  v_expected_request_hash bytea;
  v_publication public.weekly_exceptional_c1_publication_requests%rowtype;
  v_run public.weekly_exceptional_orchestration_runs%rowtype;
  v_records jsonb;
  v_checkpoint jsonb;
  v_unknown jsonb;
begin
  if not private.weekly_exceptional_json_keys_exact_v1(p_request,v_keys)
     or not (p_request ?& v_keys)
     or p_request->>'schema_version'<>'WEEKLY_PROTECTED_C1_READ_V1' then
    raise exception 'WEEKLY_PROTECTED_C1_READ_INVALID' using errcode='22023';
  end if;
  begin
    v_actor_user_id:=(p_request->>'actor_user_id')::uuid;
    v_request_id:=(p_request->>'publication_request_id')::uuid;
    v_expected_request_hash:=private.weekly_exceptional_hex_sha256_v1(
      p_request->>'expected_request_sha256'
    );
  exception when others then
    raise exception 'WEEKLY_PROTECTED_C1_READ_INVALID' using errcode='22023';
  end;

  select request.* into strict v_publication
  from public.weekly_exceptional_c1_publication_requests request
  where request.id=v_request_id;
  select run.* into strict v_run
  from public.weekly_exceptional_orchestration_runs run
  where run.id=v_publication.orchestration_run_id;
  if v_publication.request_sha256 is distinct from v_expected_request_hash
     or v_run.requested_by_user_id is distinct from v_actor_user_id
     or not private.weekly_source_pay_actor_allowed_v1(v_actor_user_id) then
    raise exception 'WEEKLY_PROTECTED_C1_READ_SCOPE_INVALID' using errcode='55000';
  end if;

  with source_and_parts as (
    select source.source_ordinal::bigint as major_ordinal,0::bigint as minor_ordinal,
      pg_catalog.jsonb_build_object(
        'source_ordinal',source.source_ordinal::text,
        'source_id',source.id,
        'authority_kind',source.authority_kind,
        'source_system',source.source_system,
        'external_identity',source.external_identity,
        'external_revision',source.external_revision,
        'source_document_sha256',pg_catalog.encode(source.source_document_sha256,'hex'),
        'payload_bytes',source.payload_bytes::text,
        'part_count',source.part_count::text,
        'work_date',source.work_date,
        'root_timesheet_id',source.root_timesheet_id,
        'candidate_id',source.candidate_id,
        'contract_id',source.contract_id,
        'source_row_sha256',pg_catalog.encode(source.source_row_sha256,'hex'),
        'record_type','SOURCE'
      ) as record_json
    from public.weekly_exceptional_c1_source_records source
    where source.publication_request_id=v_request_id
    union all
    select part.source_ordinal::bigint,part.part_ordinal::bigint,
      pg_catalog.jsonb_build_object(
        'source_ordinal',part.source_ordinal::text,
        'part_ordinal',part.part_ordinal::text,
        'payload_utf8',pg_catalog.encode(part.payload_utf8,'hex'),
        'fragment_sha256',pg_catalog.encode(part.fragment_sha256,'hex'),
        'record_type','PART'
      )
    from public.weekly_exceptional_c1_source_parts part
    where part.publication_request_id=v_request_id
  ), ordered_records as (
    select 1::integer as kind_order,major_ordinal,minor_ordinal,record_json
    from source_and_parts
    union all
    select 2::integer,component.component_ordinal::bigint,0::bigint,
      pg_catalog.jsonb_build_object(
        'component_ordinal',component.component_ordinal::text,
        'component_id',component.component_id,
        'source_ordinal',component.source_ordinal::text,
        'source_key',component.source_key,
        'component_kind',component.component_kind,
        'economic_key_type',component.economic_key_type,
        'economic_key_value',component.economic_key_value,
        'component_member_identity',component.component_member_identity,
        'segment_id',component.segment_id,
        'segment_key',component.segment_key,
        'segment_stable_key',component.segment_stable_key,
        'work_date',component.work_date,
        'reference_number',component.reference_number,
        'hours_day',case when component.hours_day is null then null else pg_catalog.to_char(component.hours_day,'FM9999999999990.000000') end,
        'hours_night',case when component.hours_night is null then null else pg_catalog.to_char(component.hours_night,'FM9999999999990.000000') end,
        'hours_sat',case when component.hours_sat is null then null else pg_catalog.to_char(component.hours_sat,'FM9999999999990.000000') end,
        'hours_sun',case when component.hours_sun is null then null else pg_catalog.to_char(component.hours_sun,'FM9999999999990.000000') end,
        'hours_bh',case when component.hours_bh is null then null else pg_catalog.to_char(component.hours_bh,'FM9999999999990.000000') end,
        'additional_code_raw',component.additional_code_raw,
        'unit_count',case when component.unit_count is null then null else pg_catalog.to_char(component.unit_count,'FM9999999999990.000000') end,
        'unit_pay_rate',case when component.unit_pay_rate is null then null else pg_catalog.to_char(component.unit_pay_rate,'FM9999999999990.000000') end,
        'unit_charge_rate',case when component.unit_charge_rate is null then null else pg_catalog.to_char(component.unit_charge_rate,'FM9999999999990.000000') end,
        'expense_code',component.expense_code,
        'pay_ex_vat',pg_catalog.to_char(component.pay_ex_vat,'FM9999999999990.00'),
        'charge_ex_vat',case when component.charge_ex_vat is null then null else pg_catalog.to_char(component.charge_ex_vat,'FM9999999999990.00') end,
        'exclude_from_pay',component.exclude_from_pay,
        'origin',component.origin,
        'component_sha256',pg_catalog.encode(component.component_sha256,'hex'),
        'record_type','COMPONENT'
      )
    from public.weekly_exceptional_c1_component_records component
    where component.publication_request_id=v_request_id
  )
  select coalesce(pg_catalog.jsonb_agg(record_json order by kind_order,major_ordinal,minor_ordinal),'[]'::jsonb)
  into v_records from ordered_records;

  select pg_catalog.jsonb_build_object(
    'phase',checkpoint.phase,'result',checkpoint.result_json,
    'record_offset',checkpoint.record_offset,
    'next_record_offset',checkpoint.next_record_offset,
    'checkpoint_sha256',pg_catalog.encode(checkpoint.checkpoint_sha256,'hex')
  ) into v_checkpoint
  from public.weekly_exceptional_c1_publication_checkpoints checkpoint
  where checkpoint.publication_request_id=v_request_id
  order by checkpoint.checkpoint_sequence desc limit 1;

  select pg_catalog.jsonb_build_object(
    'unknown_outcome_id',unknown_outcome.id,
    'phase',unknown_outcome.phase,
    'record_offset',unknown_outcome.record_offset,
    'next_record_offset',unknown_outcome.next_record_offset,
    'records_submitted',unknown_outcome.records_submitted,
    'recovery_call',unknown_outcome.recovery_call_json,
    'recovery_call_sha256',pg_catalog.encode(unknown_outcome.recovery_call_sha256,'hex')
  ) into v_unknown
  from public.weekly_exceptional_c1_unknown_outcomes unknown_outcome
  where unknown_outcome.publication_request_id=v_request_id
    and unknown_outcome.state='RECOVERY_REQUIRED'
  order by unknown_outcome.unknown_sequence desc limit 1;

  return pg_catalog.jsonb_build_object(
    'ok',true,'contract','WEEKLY_PROTECTED_C1_READ_V1',
    'publication_request_id',v_publication.id,
    'state',v_publication.state,
    'publication',pg_catalog.jsonb_build_object(
      'request',pg_catalog.jsonb_build_object(
        'request_id',v_publication.id,
        'request_sequence',v_publication.request_sequence::text,
        'actor_user_id',v_actor_user_id,
        'candidate_id',v_publication.candidate_id,
        'contract_id',v_publication.contract_id,
        'root_timesheet_id',v_publication.root_timesheet_id,
        'week_ending_date',v_publication.week_ending_date,
        'source_mode',v_publication.source_mode,
        'expected_head_revision',v_publication.expected_head_revision::text,
        'expected_source_count',v_publication.expected_source_count::text,
        'expected_component_count',v_publication.expected_component_count::text,
        'expected_payload_bytes',v_publication.expected_payload_bytes::text,
        'source_manifest_sha256',pg_catalog.encode(v_publication.source_manifest_sha256,'hex'),
        'entitlement_sha256',pg_catalog.encode(v_publication.entitlement_sha256,'hex'),
        'approval_sha256',pg_catalog.encode(v_publication.approval_sha256,'hex'),
        'is_zero_entitlement',v_publication.is_zero_entitlement,
        'financial_row_id',v_publication.financial_row_id
      ),
      'request_sha256',pg_catalog.encode(v_publication.request_sha256,'hex'),
      'records',v_records
    ),
    'resume_checkpoint',v_checkpoint,
    'unknown_checkpoint',v_unknown
  );
exception
  when no_data_found or too_many_rows then
    raise exception 'WEEKLY_PROTECTED_C1_READ_SCOPE_INVALID' using errcode='55000';
end;
$function$;
