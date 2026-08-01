--
-- PostgreSQL database dump
--

\restrict G9axQQoMzC77z9luhCP1P9GP7cxcG1UjVG1pX8FZfqVwQ4yej12pQNch147Yfyy

-- Dumped from database version 18.1
-- Dumped by pg_dump version 18.1

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: extensions; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA extensions;


--
-- Name: pgcrypto; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA extensions;


--
-- Name: EXTENSION pgcrypto; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION pgcrypto IS 'cryptographic functions';


--
-- Name: _import_review_effective_invoice_balance_core_v1(uuid, jsonb, integer, integer, integer, integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public._import_review_effective_invoice_balance_core_v1(p_import_id uuid, p_source_items jsonb, p_max_sources integer DEFAULT 100, p_max_invoice_lines_per_source integer DEFAULT 512, p_max_audit_rows_per_source integer DEFAULT 256, p_max_operations_per_source integer DEFAULT 128) RETURNS TABLE(source_identity text, balance_json jsonb)
    LANGUAGE plpgsql
    SET search_path TO 'public', 'extensions', 'pg_temp'
    AS $_$
declare
  v_item jsonb;
  v_source_identity text;
  v_source_system text;
  v_external_row_key text;
  v_invoice_stream text;
  v_source_shift_id uuid;
  v_source_timesheet_id uuid;
  v_hr_row_id uuid;
  v_candidate_id uuid;
  v_client_id uuid;
  v_contract_id uuid;
  v_week_ending_date date;
  v_authoritative_import_id uuid;
  v_a_schedule jsonb;
  v_a_hours jsonb;
  v_a_fingerprint text;
  v_scope_fingerprint text;
  v_hist_ids uuid[]:=array[]::uuid[];
  v_audit_ids uuid[]:=array[]::uuid[];
  v_archived_ids uuid[]:=array[]::uuid[];
  v_active_ids uuid[]:=array[]::uuid[];
  v_missing_ids uuid[]:=array[]::uuid[];
  v_import_ids uuid[]:=array[]::uuid[];
  v_operation_ids uuid[]:=array[]::uuid[];
  v_effective_invoice_ids uuid[]:=array[]::uuid[];
  v_effective_line_ids uuid[]:=array[]::uuid[];
  v_credit_line_ids uuid[]:=array[]::uuid[];
  v_line_count integer:=0;
  v_audit_count integer:=0;
  v_operation_count integer:=0;
  v_operation_evidence jsonb:='[]'::jsonb;
  v_operation_member_ids uuid[]:=array[]::uuid[];
  v_member_supersession_map jsonb:='[]'::jsonb;
  v_operation_evidence_conflict boolean:=false;
  v_operation_in_progress boolean:=false;
  v_member_role_map jsonb:='[]'::jsonb;
  v_member_role_conflict boolean:=false;
  v_effective_component_count integer:=0;
  v_b_day numeric:=0;
  v_b_night numeric:=0;
  v_b_sat numeric:=0;
  v_b_sun numeric:=0;
  v_b_bh numeric:=0;
  v_b_pay numeric:=0;
  v_b_charge numeric:=0;
  v_b_margin numeric:=0;
  v_component_day numeric:=0;
  v_component_night numeric:=0;
  v_component_sat numeric:=0;
  v_component_sun numeric:=0;
  v_component_bh numeric:=0;
  v_component_pay numeric:=0;
  v_component_charge numeric:=0;
  v_component_margin numeric:=0;
  v_b_schedule jsonb:='[]'::jsonb;
  v_candidate_schedule jsonb:='[]'::jsonb;
  v_candidate_hours jsonb:='{}'::jsonb;
  v_b_policy_fingerprint text;
  v_effective_fingerprint text;
  v_line_evidence jsonb:='[]'::jsonb;
  v_ignored_nonhours_line_ids uuid[]:=array[]::uuid[];
  v_generation_role_evidence jsonb:='[]'::jsonb;
  v_fully_invoiced_generation_ids text[]:=array[]::text[];
  v_partial_generation_ids text[]:=array[]::text[];
  v_mutable_generation_ids text[]:=array[]::text[];
  v_archived_history_roles jsonb:='[]'::jsonb;
  v_role_evidence_conflicts jsonb:='[]'::jsonb;
  v_role_evidence_fingerprint text;
  v_repair_identity_mode text;
  v_reversal_repair_required boolean:=false;
  v_replacement_repair_required boolean:=false;
  v_line record;
  v_original_line public.invoice_lines%rowtype;
  v_tf public.timesheets_financials%rowtype;
  v_original_tf public.timesheets_financials%rowtype;
  v_seg jsonb;
  v_original_seg jsonb;
  v_line_type text;
  v_original_line_type text;
  v_is_weekly_hours boolean:=false;
  v_is_separable_nonhours boolean:=false;
  v_original_line_id uuid;
  v_seg_count integer:=0;
  v_matching_seg_count integer:=0;
  v_original_seg_count integer:=0;
  v_original_matching_seg_count integer:=0;
  v_single_source boolean:=false;
  v_line_scope_proven boolean:=false;
  v_operation_member_scope_proven boolean:=false;
  v_component_timesheet_id uuid;
  v_component_correction_id text;
  v_component_correction_kind text;
  v_scope_unprovable boolean:=false;
  v_credit_ambiguous boolean:=false;
  v_stream_conflict boolean:=false;
  v_archived_invoice_conflict boolean:=false;
  v_partial_invoice_state boolean:=false;
  v_active_invoice_activity boolean:=false;
  v_role_partial_invoice_state boolean:=false;
  v_role_active_invoice_activity boolean:=false;
  v_role_scope_unprovable boolean:=false;
  v_paid_mutable_state boolean:=false;
  v_mutable_correction_id text;
  v_mutable_member_ids uuid[]:=array[]::uuid[];
  v_mutable_missing_roles text[]:=array[]::text[];
  v_mutable_fingerprint text;
  v_mutable_parent_id uuid;
  v_m_day numeric:=0;
  v_m_night numeric:=0;
  v_m_sat numeric:=0;
  v_m_sun numeric:=0;
  v_m_bh numeric:=0;
  v_m_pay numeric:=0;
  v_m_charge numeric:=0;
  v_m_margin numeric:=0;
  v_m_financials_complete boolean:=true;
  v_b_standard_representable boolean:=false;
  v_b_hours_zero boolean:=false;
  v_b_money_zero boolean:=false;
  v_effective_zero boolean:=false;
  v_current_source_safe boolean:=false;
  v_current_source_safety_reason text;
  v_current_source_count integer:=0;
  v_current_source_invoice_lined boolean:=false;
  v_current_source_paid boolean:=false;
  v_current_source_unlocked boolean:=false;
  v_current_source_fresh boolean:=false;
  v_current_source_segment_unlocked boolean:=false;
  v_current_source_contract_week_safe boolean:=false;
  v_current_source_invoice_operation_clear boolean:=false;
  v_source_protection jsonb:='{}'::jsonb;
  v_blocking_code text;
  v_reconciliation_fingerprint text;
  v_uuid_re constant text:='^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$';
begin
  if p_import_id is null or jsonb_typeof(coalesce(p_source_items,'null'::jsonb))<>'array' then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_INPUT_INVALID' using errcode='22023';
  end if;
  if p_max_sources not between 1 and 100
     or p_max_invoice_lines_per_source not between 1 and 512
     or p_max_audit_rows_per_source not between 1 and 256
     or p_max_operations_per_source not between 1 and 128 then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_BOUND_INVALID' using errcode='22023';
  end if;
  if jsonb_array_length(p_source_items)>p_max_sources then
    raise exception 'IMPORT_REVIEW_SOURCE_LIMIT_EXCEEDED' using errcode='54000';
  end if;
  if exists (
    select 1 from jsonb_array_elements(p_source_items) s(value)
    group by nullif(btrim(s.value->>'source_identity'),'') having count(*)>1
  ) then
    raise exception 'IMPORT_REVIEW_SOURCE_IDENTITY_DUPLICATE' using errcode='22023';
  end if;

  for v_item in select s.value from jsonb_array_elements(p_source_items) s(value)
  loop
    if jsonb_typeof(v_item)<>'object' then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_SOURCE_INVALID' using errcode='22023';
    end if;
    v_source_identity:=nullif(btrim(v_item->>'source_identity'),'');
    v_source_system:=upper(btrim(coalesce(v_item->>'source_system','')));
    v_external_row_key:=nullif(btrim(v_item->>'external_row_key'),'');
    v_invoice_stream:=upper(btrim(coalesce(v_item->>'invoice_stream','')));
    if v_source_identity is null or v_source_system not in ('NHSP','HEALTHROSTER')
       or v_external_row_key is null or v_invoice_stream not in ('NORMAL','SELF_BILL')
       or coalesce(v_item->>'source_shift_id','')!~*v_uuid_re
       or coalesce(v_item->>'hr_row_id','')!~*v_uuid_re
       or coalesce(v_item->>'source_timesheet_id','')!~*v_uuid_re
       or coalesce(v_item->>'candidate_id','')!~*v_uuid_re
       or coalesce(v_item->>'client_id','')!~*v_uuid_re
       or coalesce(v_item->>'contract_id','')!~*v_uuid_re
       or coalesce(v_item->>'authoritative_import_id','')!~*v_uuid_re
       or coalesce(v_item->>'week_ending_date','')!~'^\d{4}-\d{2}-\d{2}$'
       or jsonb_typeof(v_item->'authoritative_schedule_json')<>'array'
       or jsonb_array_length(v_item->'authoritative_schedule_json')<>1
       or jsonb_typeof(v_item->'authoritative_hours')<>'object' then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_SOURCE_INVALID' using errcode='22023',detail=coalesce(v_source_identity,'missing source_identity');
    end if;
    v_source_shift_id:=(v_item->>'source_shift_id')::uuid;
    v_hr_row_id:=(v_item->>'hr_row_id')::uuid;
    v_source_timesheet_id:=(v_item->>'source_timesheet_id')::uuid;
    v_candidate_id:=(v_item->>'candidate_id')::uuid;
    v_client_id:=(v_item->>'client_id')::uuid;
    v_contract_id:=(v_item->>'contract_id')::uuid;
    v_authoritative_import_id:=(v_item->>'authoritative_import_id')::uuid;
    v_week_ending_date:=(v_item->>'week_ending_date')::date;
    v_a_schedule:=v_item->'authoritative_schedule_json';
    v_a_hours:=jsonb_build_object(
      'hours_day',coalesce((v_item#>>'{authoritative_hours,hours_day}')::numeric,0),
      'hours_night',coalesce((v_item#>>'{authoritative_hours,hours_night}')::numeric,0),
      'hours_sat',coalesce((v_item#>>'{authoritative_hours,hours_sat}')::numeric,0),
      'hours_sun',coalesce((v_item#>>'{authoritative_hours,hours_sun}')::numeric,0),
      'hours_bh',coalesce((v_item#>>'{authoritative_hours,hours_bh}')::numeric,0),
      'total_hours',coalesce((v_item#>>'{authoritative_hours,total_hours}')::numeric,0)
    );
    v_a_fingerprint:=encode(digest(convert_to(concat_ws('|','A-v1',v_source_identity,v_authoritative_import_id,v_a_schedule::text,v_a_hours::text),'UTF8'),'sha256'),'hex');
    v_scope_fingerprint:=encode(digest(convert_to(concat_ws('|','source-scope-v1',v_source_identity,v_source_system,v_source_shift_id,v_external_row_key,v_source_timesheet_id,v_candidate_id,v_client_id,v_contract_id,v_week_ending_date,v_invoice_stream),'UTF8'),'sha256'),'hex');

    perform 1 from public.hr_rows r
    where r.id=v_hr_row_id and r.import_id=p_import_id and r.external_row_key=v_external_row_key;
    if not found then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_SOURCE_INVALID' using errcode='22023',detail=v_source_identity;
    end if;
    perform 1 from public.nhsp_shifts s
    where s.id=v_source_shift_id and s.external_row_key=v_external_row_key
      and upper(s.source_system::text)=v_source_system
      and s.candidate_id=v_candidate_id and s.client_id=v_client_id
      and s.contract_id=v_contract_id and s.week_ending_date=v_week_ending_date;
    if not found then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_SOURCE_INVALID' using errcode='22023',detail=v_source_identity;
    end if;

    select count(*)::integer into v_audit_count
    from public.audit_events ae
    where ae.action in ('NHSP_IMPORT_CORRECTION_APPLIED','HR_IMPORT_CORRECTION_APPLIED')
      and (ae.after_json->>'shift_id'=v_source_shift_id::text
        or ae.after_json->>'external_row_key'=v_external_row_key);
    if v_audit_count>p_max_audit_rows_per_source then
      raise exception 'IMPORT_REVIEW_AUDIT_EVIDENCE_LIMIT_EXCEEDED' using errcode='54000',detail=v_source_identity;
    end if;

    select coalesce(array_agg(distinct candidate_id order by candidate_id),array[]::uuid[])
    into v_audit_ids
    from (
      select candidate_id
      from public.audit_events ae
      cross join lateral unnest(array[
        case when ae.object_type='timesheets' then ae.object_id_text end,
        ae.after_json->>'timesheet_id',
        ae.after_json->>'reversal_timesheet_id',
        ae.after_json->>'replacement_timesheet_id',
        ae.after_json->>'counterpart_timesheet_id'
      ]) raw(candidate_text)
      cross join lateral (select case when raw.candidate_text~*v_uuid_re then raw.candidate_text::uuid end candidate_id) parsed
      where ae.action in ('NHSP_IMPORT_CORRECTION_APPLIED','HR_IMPORT_CORRECTION_APPLIED')
        and (ae.after_json->>'shift_id'=v_source_shift_id::text
          or ae.after_json->>'external_row_key'=v_external_row_key)
        and parsed.candidate_id is not null
    ) candidates;

    select coalesce(array_agg(distinct import_id order by import_id),array[]::uuid[])
    into v_import_ids
    from (
      select p_import_id import_id
      union all select v_authoritative_import_id
      union all select s.latest_import_id from public.nhsp_shifts s where s.id=v_source_shift_id
      union all
      select case when raw.import_text~*v_uuid_re then raw.import_text::uuid end
      from public.audit_events ae
      cross join lateral unnest(array[
        ae.after_json->>'import_id',ae.after_json->>'trigger_import_id',ae.after_json->>'evidence_import_id'
      ]) raw(import_text)
      where ae.action in ('NHSP_IMPORT_CORRECTION_APPLIED','HR_IMPORT_CORRECTION_APPLIED')
        and (ae.after_json->>'shift_id'=v_source_shift_id::text
          or ae.after_json->>'external_row_key'=v_external_row_key)
    ) imports where import_id is not null;
    -- A later authoritative import replaces latest_import_id, so historical
    -- operation identity is discovered both through known imports and through
    -- the immutable decision/outcome link for this exact source shift.
    select coalesce(array_agg(distinct operation_id order by operation_id),array[]::uuid[])
    into v_operation_ids
    from (
      select op.id operation_id
      from public.import_apply_operations op
      where op.import_id=any(v_import_ids)
      union all
      select outcome.operation_id
      from public.import_review_decisions decision
      join public.import_review_action_outcomes outcome on outcome.action_id=decision.action_id
      where decision.shift_id=v_source_shift_id and outcome.shift_id=v_source_shift_id
        and decision.source_identity=v_source_identity and outcome.source_identity=v_source_identity
        and decision.candidate_id=v_candidate_id and outcome.candidate_id=v_candidate_id
        and decision.client_id=v_client_id and outcome.client_id=v_client_id
        and decision.contract_id is not distinct from v_contract_id
        and outcome.contract_id is not distinct from v_contract_id
        and decision.action_kind='APPLY_AMENDMENT' and outcome.action_kind='APPLY_AMENDMENT'
    ) operation_candidates
    where operation_id is not null;
    v_operation_count:=cardinality(v_operation_ids);
    if v_operation_count>p_max_operations_per_source then
      raise exception 'IMPORT_REVIEW_OPERATION_EVIDENCE_LIMIT_EXCEEDED' using errcode='54000',detail=v_source_identity;
    end if;

    -- A completed reconciliation operation is durable identity evidence, not
    -- economic evidence.  Validate every request/applied/policy triple before
    -- any invoice-line scope is built so physically deleted members remain
    -- discoverable without allowing an operation to contribute money twice.
    with matching_requests as (
      select op.id operation_id,op.state::text operation_state,op.committed_at_utc,op.finalised_at_utc,
        op.response_json,request_unit
      from public.import_apply_operations op
      cross join lateral jsonb_array_elements(coalesce(op.response_json#>'{request_envelope,reconciliation_units}','[]'::jsonb)) request_unit
      where op.id=any(v_operation_ids)
        and (request_unit->>'source_identity'=v_source_identity
          or request_unit->>'source_shift_id'=v_source_shift_id::text)
    ), triples as (
      select mr.*,
        (select count(*) from jsonb_array_elements(coalesce(mr.response_json#>'{request_envelope,reconciliation_units}','[]'::jsonb)) candidate
          where candidate->>'action_id'=mr.request_unit->>'action_id'
            and (candidate->>'source_identity'=v_source_identity or candidate->>'source_shift_id'=v_source_shift_id::text)) request_count,
        applied_match.applied_unit,applied_match.applied_count,
        policy_match.policy_unit,policy_match.policy_count,
        mr.response_json->'correction_operation_contract' operation_contract
      from matching_requests mr
      left join lateral (
        select min(applied::text)::jsonb applied_unit,count(*)::integer applied_count
        from jsonb_array_elements(coalesce(mr.response_json->'reconciliation_units','[]'::jsonb)) applied
        where applied->>'action_id'=mr.request_unit->>'action_id'
      ) applied_match on true
      left join lateral (
        select min(policy::text)::jsonb policy_unit,count(*)::integer policy_count
        from jsonb_array_elements(coalesce(mr.response_json#>'{correction_operation_contract,correction_units}','[]'::jsonb)) policy
        where policy->>'action_id'=mr.request_unit->>'action_id'
      ) policy_match on true
    ), evaluated as (
      select t.*,
        case when t.operation_state='COMPLETE'
          and t.committed_at_utc is not null and t.finalised_at_utc is not null
          and t.request_unit->>'route' in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT')
          and t.request_count=1 and t.applied_count=1 and t.policy_count=1
          and t.request_unit->>'action_id'=t.applied_unit->>'action_id'
          and t.request_unit->>'action_id'=t.policy_unit->>'action_id'
          and t.request_unit->>'source_identity'=v_source_identity
          and t.applied_unit->>'source_identity'=v_source_identity
          and t.policy_unit->>'source_row_key'=v_source_identity
          and t.request_unit->>'source_system'=v_source_system
          and t.applied_unit->>'source_system'=v_source_system
          and t.request_unit->>'source_shift_id'=v_source_shift_id::text
          and t.applied_unit->>'source_shift_id'=v_source_shift_id::text
          and t.policy_unit->>'source_shift_id'=v_source_shift_id::text
          and t.request_unit->>'source_timesheet_id'=v_source_timesheet_id::text
          and t.policy_unit->>'root_timesheet_id'=v_source_timesheet_id::text
          and t.request_unit->>'candidate_id'=v_candidate_id::text
          and t.request_unit->>'client_id'=v_client_id::text
          and t.request_unit->>'contract_id'=v_contract_id::text
          and t.request_unit->>'week_ending_date'=v_week_ending_date::text
          and nullif(t.request_unit->>'unit_fingerprint','') is not null
          and t.applied_unit->>'reviewed_unit_fingerprint'=t.request_unit->>'unit_fingerprint'
          and t.applied_unit->>'reconciliation_fingerprint'=t.request_unit->>'reconciliation_fingerprint'
          and coalesce(t.applied_unit->>'repair_identity_mode','')=coalesce(t.request_unit->>'repair_identity_mode','')
          and (coalesce(t.request_unit->>'repair_identity_mode','')<>'FRESH_CORRECTION_ID_ARCHIVED_ROLE_IGNORED'
            or (t.request_unit->>'route'='AMEND_EXISTING_REPLACEMENT'
              and nullif(t.request_unit->>'reviewed_existing_correction_id','') is not null
              and t.request_unit->>'reviewed_existing_correction_id'<>t.applied_unit->>'correction_id'
              and jsonb_typeof(t.request_unit->'reviewed_existing_member_ids')='array'
              and jsonb_array_length(t.request_unit->'reviewed_existing_member_ids') between 1 and 2
              and exists(select 1 from jsonb_array_elements_text(t.request_unit->'reviewed_existing_member_ids') reviewed(member_id)
                where reviewed.member_id in (t.applied_unit->>'reversal_timesheet_id',t.applied_unit->>'replacement_timesheet_id'))))
          and nullif(t.applied_unit->>'correction_id','') is not null
          and coalesce(t.applied_unit->>'reversal_timesheet_id','')~*v_uuid_re
          and coalesce(t.applied_unit->>'replacement_timesheet_id','')~*v_uuid_re
          and coalesce(t.applied_unit->>'parent_timesheet_id','')~*v_uuid_re
          and t.applied_unit->>'reversal_timesheet_id'<>t.applied_unit->>'replacement_timesheet_id'
          and jsonb_typeof(t.applied_unit->'applied_member_ids')='array'
          and jsonb_array_length(t.applied_unit->'applied_member_ids')=2
          and not exists(select 1 from jsonb_array_elements_text(t.applied_unit->'applied_member_ids') member(value)
            where member.value!~*v_uuid_re)
          and t.applied_unit->'applied_member_ids' @> jsonb_build_array(t.applied_unit->>'reversal_timesheet_id')
          and t.applied_unit->'applied_member_ids' @> jsonb_build_array(t.applied_unit->>'replacement_timesheet_id')
          and jsonb_typeof(t.policy_unit->'policy_envelope')='object'
          and nullif(t.policy_unit->>'policy_envelope_fingerprint','') is not null
          and t.policy_unit#>>'{policy_envelope,envelope_fingerprint}'=t.policy_unit->>'policy_envelope_fingerprint'
          and encode(digest(convert_to(((t.policy_unit->'policy_envelope')-'envelope_fingerprint'::text)::text,'UTF8'),'sha256'),'hex')
            =t.policy_unit->>'policy_envelope_fingerprint'
          and jsonb_typeof(t.operation_contract)='object'
          and nullif(t.operation_contract->>'operation_contract_fingerprint','') is not null
          and encode(digest(convert_to((t.operation_contract-'operation_contract_fingerprint'::text)::text,'UTF8'),'sha256'),'hex')
            =t.operation_contract->>'operation_contract_fingerprint'
          and case when coalesce(t.applied_unit->>'reversal_timesheet_id','')~*v_uuid_re
              and coalesce(t.applied_unit->>'replacement_timesheet_id','')~*v_uuid_re
              and coalesce(t.applied_unit->>'parent_timesheet_id','')~*v_uuid_re
            then t.applied_unit->>'applied_result_fingerprint'=encode(digest(convert_to(jsonb_build_object(
              'correction_id',t.applied_unit->>'correction_id',
              'reversal_timesheet_id',(t.applied_unit->>'reversal_timesheet_id')::uuid,
              'replacement_timesheet_id',(t.applied_unit->>'replacement_timesheet_id')::uuid,
              'M_active_member_ids',t.applied_unit->'applied_member_ids',
              'applied_member_ids',t.applied_unit->'applied_member_ids',
              'parent_timesheet_id',(t.applied_unit->>'parent_timesheet_id')::uuid,
              'repair_identity_mode',t.applied_unit->>'repair_identity_mode',
              'reviewed_unit_fingerprint',t.applied_unit->>'reviewed_unit_fingerprint',
              'reconciliation_fingerprint',t.applied_unit->>'reconciliation_fingerprint')::text,'UTF8'),'sha256'),'hex')
            else false end
          then true else false end valid_historical_authority
      from triples t
    )
    select
      coalesce(jsonb_agg(jsonb_build_object(
        'operation_id',e.operation_id,'action_id',e.request_unit->>'action_id',
        'evidence_at',e.finalised_at_utc,'source_identity',e.request_unit->>'source_identity',
        'source_shift_id',e.request_unit->>'source_shift_id','source_timesheet_id',e.request_unit->>'source_timesheet_id',
        'correction_id',e.applied_unit->>'correction_id',
        'reversal_timesheet_id',e.applied_unit->>'reversal_timesheet_id',
        'replacement_timesheet_id',e.applied_unit->>'replacement_timesheet_id',
        'applied_member_ids',e.applied_unit->'applied_member_ids',
        'parent_timesheet_id',e.applied_unit->>'parent_timesheet_id',
        'route',e.request_unit->>'route',
        'reviewed_existing_correction_id',e.request_unit->>'reviewed_existing_correction_id',
        'reviewed_existing_member_ids',coalesce(e.request_unit->'reviewed_existing_member_ids','[]'::jsonb),
        'repair_identity_mode',e.applied_unit->>'repair_identity_mode',
        'reviewed_unit_fingerprint',e.request_unit->>'unit_fingerprint',
        'reconciliation_fingerprint',e.request_unit->>'reconciliation_fingerprint',
        'B_standard_schedule_json',coalesce(e.request_unit->'B_standard_schedule_json','[]'::jsonb),
        'B_hours',coalesce(e.request_unit->'B_hours','{}'::jsonb),
        'A_schedule_json',coalesce(e.request_unit->'A_schedule_json','[]'::jsonb),
        'A_hours',coalesce(e.request_unit->'A_hours','{}'::jsonb)
      ) order by e.finalised_at_utc,e.operation_id,e.request_unit->>'action_id')
        filter(where e.valid_historical_authority),'[]'::jsonb),
      coalesce(bool_or(e.operation_state='COMPLETE'
        and e.request_unit->>'route' in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT')
        and not e.valid_historical_authority),false),
      coalesce(bool_or(e.operation_state in ('SOURCE_COMMITTED_TSFIN_PENDING','FINANCIALISED_PENDING_FINALISATION')
        and e.committed_at_utc is not null),false)
    into v_operation_evidence,v_operation_evidence_conflict,v_operation_in_progress
    from evaluated e;

    -- A valid archived-sibling repair deliberately re-keys the surviving
    -- physical member.  Preserve that exact old-to-new identity edge so a
    -- later import treats the old assignment as superseded audit history,
    -- rather than as contradictory ownership.
    select coalesce(jsonb_agg(jsonb_build_object(
      'operation_id',edge.operation_id,
      'member_timesheet_id',edge.member_timesheet_id,
      'correction_kind',edge.correction_kind,
      'superseded_correction_id',edge.superseded_correction_id,
      'canonical_correction_id',edge.canonical_correction_id
    ) order by edge.operation_id,edge.correction_kind,edge.member_timesheet_id),'[]'::jsonb)
    into v_member_supersession_map
    from (
      select distinct (unit->>'operation_id')::uuid operation_id,
        role.member_id::uuid member_timesheet_id,role.correction_kind,
        unit->>'reviewed_existing_correction_id' superseded_correction_id,
        unit->>'correction_id' canonical_correction_id
      from jsonb_array_elements(v_operation_evidence) unit
      cross join lateral (values
        ('CHANGED_HOURS_REVERSAL'::text,unit->>'reversal_timesheet_id'),
        ('CHANGED_HOURS_REPLACEMENT'::text,unit->>'replacement_timesheet_id')
      ) role(correction_kind,member_id)
      where unit->>'route'='AMEND_EXISTING_REPLACEMENT'
        and unit->>'repair_identity_mode'='FRESH_CORRECTION_ID_ARCHIVED_ROLE_IGNORED'
        and nullif(unit->>'reviewed_existing_correction_id','') is not null
        and unit->>'reviewed_existing_correction_id'<>unit->>'correction_id'
        and role.member_id~*v_uuid_re
        and coalesce(unit->'reviewed_existing_member_ids','[]'::jsonb) @> jsonb_build_array(role.member_id)
        and not exists(
          select 1
          from public.invoice_lines historical_line
          where (historical_line.timesheet_id=role.member_id::uuid
              or historical_line.meta_json->>'timesheet_id'=role.member_id)
            and historical_line.created_at<=coalesce((unit->>'evidence_at')::timestamptz,'infinity'::timestamptz))
    ) edge;

    select coalesce(array_agg(distinct member_id order by member_id),array[]::uuid[])
    into v_operation_member_ids
    from (
      select (unit->>'reversal_timesheet_id')::uuid member_id
      from jsonb_array_elements(v_operation_evidence) unit
      union all
      select (unit->>'replacement_timesheet_id')::uuid
      from jsonb_array_elements(v_operation_evidence) unit
      union all
      select member.value::uuid
      from jsonb_array_elements(v_operation_evidence) unit
      cross join lateral jsonb_array_elements_text(unit->'applied_member_ids') member(value)
    ) ids;

    select coalesce(array_agg(distinct timesheet_id order by timesheet_id),array[]::uuid[])
    into v_hist_ids
    from (
      select v_source_timesheet_id timesheet_id
      union all select s.timesheet_id from public.nhsp_shifts s where s.id=v_source_shift_id
      union all select unnest(v_audit_ids)
      union all select unnest(v_operation_member_ids)
      union all
      select t.timesheet_id
      from public.timesheets t
      join public.timesheets_financials tf_scope on tf_scope.timesheet_id=t.timesheet_id
      where tf_scope.candidate_id=v_candidate_id and t.contract_id=v_contract_id and t.week_ending_date=v_week_ending_date
        and (
          (jsonb_typeof(t.actual_schedule_json)='array' and t.actual_schedule_json @> jsonb_build_array(jsonb_build_object('shift_id',v_source_shift_id::text,'external_row_key',v_external_row_key)))
          or t.candidate_hint_text#>>'{correction_financials_policy_envelope,classification,source_shift_id}'=v_source_shift_id::text
          or t.candidate_hint_text#>>'{correction_financials_policy_envelope,classification,source_row_key}'=v_external_row_key
        )
    ) ids where timesheet_id is not null;
    select coalesce(array_agg(t.timesheet_id order by t.timesheet_id),array[]::uuid[])
    into v_archived_ids from public.timesheets t where t.timesheet_id=any(v_hist_ids) and t.archived_at_utc is not null;
    select coalesce(array_agg(t.timesheet_id order by t.timesheet_id),array[]::uuid[])
    into v_active_ids from public.timesheets t where t.timesheet_id=any(v_hist_ids) and t.is_current and t.archived_at_utc is null;
    select coalesce(array_agg(x order by x),array[]::uuid[]) into v_missing_ids
    from (select distinct unnest(v_audit_ids||v_operation_member_ids) x) missing
    where not exists(select 1 from public.timesheets t where t.timesheet_id=missing.x);

    -- Canonical source/role ownership is established once and then reused by
    -- both invoice balance and generation classification.  Higher-authority
    -- evidence may fill a missing identity but contradictory identities fail
    -- closed instead of being resolved by arbitrary precedence.
    with evidence as (
      select (unit->>'reversal_timesheet_id')::uuid timesheet_id,unit->>'correction_id' correction_id,
        'CHANGED_HOURS_REVERSAL'::text correction_kind,'COMPLETED_OPERATION'::text evidence_source,
        1 priority,true operation_proven
      from jsonb_array_elements(v_operation_evidence) unit
      union all
      select (unit->>'replacement_timesheet_id')::uuid,unit->>'correction_id',
        'CHANGED_HOURS_REPLACEMENT','COMPLETED_OPERATION',1,true
      from jsonb_array_elements(v_operation_evidence) unit
      union all
      select raw.member_id,ae.after_json->>'correction_id','CHANGED_HOURS_REVERSAL','CORRECTION_AUDIT',2,false
      from public.audit_events ae
      cross join lateral (select case when ae.after_json->>'reversal_timesheet_id'~*v_uuid_re
        then (ae.after_json->>'reversal_timesheet_id')::uuid end member_id) raw
      where ae.action in ('NHSP_IMPORT_CORRECTION_APPLIED','HR_IMPORT_CORRECTION_APPLIED')
        and (ae.after_json->>'shift_id'=v_source_shift_id::text or ae.after_json->>'external_row_key'=v_external_row_key)
        and nullif(ae.after_json->>'correction_id','') is not null and raw.member_id is not null
      union all
      select raw.member_id,ae.after_json->>'correction_id','CHANGED_HOURS_REPLACEMENT','CORRECTION_AUDIT',2,false
      from public.audit_events ae
      cross join lateral (select case when ae.after_json->>'replacement_timesheet_id'~*v_uuid_re
        then (ae.after_json->>'replacement_timesheet_id')::uuid end member_id) raw
      where ae.action in ('NHSP_IMPORT_CORRECTION_APPLIED','HR_IMPORT_CORRECTION_APPLIED')
        and (ae.after_json->>'shift_id'=v_source_shift_id::text or ae.after_json->>'external_row_key'=v_external_row_key)
        and nullif(ae.after_json->>'correction_id','') is not null and raw.member_id is not null
      union all
      select raw.member_id,ae.after_json->>'correction_id',ae.after_json->>'correction_kind','CORRECTION_AUDIT',2,false
      from public.audit_events ae
      cross join lateral (select case when ae.after_json->>'timesheet_id'~*v_uuid_re
        then (ae.after_json->>'timesheet_id')::uuid end member_id) raw
      where ae.action in ('NHSP_IMPORT_CORRECTION_APPLIED','HR_IMPORT_CORRECTION_APPLIED')
        and (ae.after_json->>'shift_id'=v_source_shift_id::text or ae.after_json->>'external_row_key'=v_external_row_key)
        and ae.after_json->>'correction_kind' in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
        and nullif(ae.after_json->>'correction_id','') is not null and raw.member_id is not null
      union all
      select t.timesheet_id,t.correction_id,t.correction_kind::text,'LIVE_ROW',4,false
      from public.timesheets t
      where t.timesheet_id=any(v_hist_ids) and nullif(t.correction_id,'') is not null
        and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
    ), conflicts as (
      select distinct left_evidence.timesheet_id
      from evidence left_evidence
      join evidence right_evidence on right_evidence.timesheet_id=left_evidence.timesheet_id
        and (right_evidence.correction_id,right_evidence.correction_kind)
          is distinct from (left_evidence.correction_id,left_evidence.correction_kind)
      where left_evidence.correction_kind<>right_evidence.correction_kind
        or (left_evidence.correction_id<>right_evidence.correction_id
          and not exists(
            select 1 from jsonb_array_elements(v_member_supersession_map) edge
            where edge->>'member_timesheet_id'=left_evidence.timesheet_id::text
              and edge->>'correction_kind'=left_evidence.correction_kind
              and ((edge->>'superseded_correction_id'=left_evidence.correction_id
                    and edge->>'canonical_correction_id'=right_evidence.correction_id)
                or (edge->>'superseded_correction_id'=right_evidence.correction_id
                    and edge->>'canonical_correction_id'=left_evidence.correction_id))))
    ), canonical as (
      select distinct on (e.timesheet_id) e.*
      from evidence e
      where not exists(select 1 from conflicts c where c.timesheet_id=e.timesheet_id)
        and not exists(
          select 1 from jsonb_array_elements(v_member_supersession_map) edge
          where edge->>'member_timesheet_id'=e.timesheet_id::text
            and edge->>'correction_kind'=e.correction_kind
            and edge->>'superseded_correction_id'=e.correction_id
            and edge->>'canonical_correction_id'<>e.correction_id)
      order by e.timesheet_id,e.priority,e.correction_id,e.correction_kind
    )
    select
      coalesce((select jsonb_agg(jsonb_build_object(
        'timesheet_id',c.timesheet_id,'correction_id',c.correction_id,'correction_kind',c.correction_kind,
        'evidence_source',c.evidence_source,'operation_proven',c.operation_proven,
        'source_system',v_source_system,'source_identity',v_source_identity,'source_shift_id',v_source_shift_id
      ) order by c.timesheet_id) from canonical c),'[]'::jsonb),
      exists(select 1 from conflicts)
    into v_member_role_map,v_member_role_conflict;

    v_effective_invoice_ids:=array[]::uuid[];
    v_effective_line_ids:=array[]::uuid[];
    v_credit_line_ids:=array[]::uuid[];
    v_effective_component_count:=0;
    v_b_day:=0; v_b_night:=0; v_b_sat:=0; v_b_sun:=0; v_b_bh:=0;
    v_b_pay:=0; v_b_charge:=0; v_b_margin:=0;
    v_b_schedule:='[]'::jsonb; v_candidate_schedule:='[]'::jsonb; v_candidate_hours:='{}'::jsonb;
    select encode(digest(convert_to(coalesce(tf.policy_snapshot_json,'{}'::jsonb)::text,'UTF8'),'sha256'),'hex')
    into v_b_policy_fingerprint
    from public.timesheets_financials tf
    where tf.timesheet_id=v_source_timesheet_id and tf.is_current
    order by tf.computed_at_utc desc nulls last,tf.id desc limit 1;
    v_line_evidence:='[]'::jsonb;
    v_ignored_nonhours_line_ids:=array[]::uuid[];
    v_generation_role_evidence:='[]'::jsonb;
    v_fully_invoiced_generation_ids:=array[]::text[];
    v_partial_generation_ids:=array[]::text[];
    v_mutable_generation_ids:=array[]::text[];
    v_archived_history_roles:='[]'::jsonb;
    v_role_evidence_conflicts:='[]'::jsonb;
    v_repair_identity_mode:=null;
    v_reversal_repair_required:=false;
    v_replacement_repair_required:=false;
    v_role_partial_invoice_state:=false;
    v_role_active_invoice_activity:=false;
    v_role_scope_unprovable:=false;
    v_scope_unprovable:=false; v_credit_ambiguous:=false; v_stream_conflict:=false;
    v_archived_invoice_conflict:=false; v_active_invoice_activity:=false;

    with directly_scoped as (
      select il.id
      from public.invoice_lines il
      where il.timesheet_id=any(v_hist_ids)
        or case when coalesce(il.meta_json->>'timesheet_id','')~*v_uuid_re
          then (il.meta_json->>'timesheet_id')::uuid=any(v_hist_ids) else false end
    ), scoped as (
      select il.id
      from public.invoice_lines il where il.id in(select id from directly_scoped)
      union
      select credit.id
      from public.invoice_lines credit
      where coalesce(credit.meta_json->>'original_invoice_line_id',credit.meta_json->>'credit_of_line_id','')~*v_uuid_re
        and coalesce(credit.meta_json->>'original_invoice_line_id',credit.meta_json->>'credit_of_line_id')::uuid in(select id from directly_scoped)
    )
    select count(*)::integer into v_line_count from scoped;
    if v_line_count>p_max_invoice_lines_per_source then
      raise exception 'IMPORT_REVIEW_INVOICE_EVIDENCE_LIMIT_EXCEEDED' using errcode='54000',detail=v_source_identity;
    end if;

    for v_line in
      with directly_scoped as (
        select il.id
        from public.invoice_lines il
        where il.timesheet_id=any(v_hist_ids)
          or case when coalesce(il.meta_json->>'timesheet_id','')~*v_uuid_re
            then (il.meta_json->>'timesheet_id')::uuid=any(v_hist_ids) else false end
      ), scoped as (
        select il.id from public.invoice_lines il where il.id in(select id from directly_scoped)
        union
        select credit.id from public.invoice_lines credit
        where coalesce(credit.meta_json->>'original_invoice_line_id',credit.meta_json->>'credit_of_line_id','')~*v_uuid_re
          and coalesce(credit.meta_json->>'original_invoice_line_id',credit.meta_json->>'credit_of_line_id')::uuid in(select id from directly_scoped)
      )
      select il.*,i.type::text invoice_type,i.status::text invoice_status,i.issued_at_utc,
        i.original_invoice_id,i.active_document_operation_id,i.active_issue_operation_id,i.issue_state
      from scoped s join public.invoice_lines il on il.id=s.id join public.invoices i on i.id=il.invoice_id
      order by i.issued_at_utc nulls last,il.id
    loop
      -- Archived rows are audit-only.  They cannot contribute to the current
      -- source balance or make an otherwise repairable generation block.
      if v_line.timesheet_id=any(v_archived_ids)
         or (coalesce(v_line.meta_json->>'timesheet_id','')~*v_uuid_re
           and (v_line.meta_json->>'timesheet_id')::uuid=any(v_archived_ids)) then
        continue;
      end if;

      v_tf:=null; v_seg:=null; v_seg_count:=0; v_matching_seg_count:=0;
      if coalesce(v_line.meta_json->>'tsfin_id','')~*v_uuid_re then
        select tf.* into v_tf from public.timesheets_financials tf where tf.id=(v_line.meta_json->>'tsfin_id')::uuid;
      elsif v_line.timesheet_id is not null then
        select tf.* into v_tf from public.timesheets_financials tf
        where tf.timesheet_id=v_line.timesheet_id
        order by case when tf.is_current then 0 else 1 end,tf.computed_at_utc desc nulls last,tf.id desc limit 1;
      end if;
      if v_tf.id is not null then
        select count(*)::integer,
          count(*) filter(where seg->>'nhsp_shift_id'=v_source_shift_id::text or seg->>'shift_id'=v_source_shift_id::text or seg->>'external_row_key'=v_external_row_key)::integer,
          (array_agg(seg order by case when seg->>'nhsp_shift_id'=v_source_shift_id::text or seg->>'shift_id'=v_source_shift_id::text then 0 else 1 end)
            filter(where seg->>'nhsp_shift_id'=v_source_shift_id::text or seg->>'shift_id'=v_source_shift_id::text or seg->>'external_row_key'=v_external_row_key))[1]
        into v_seg_count,v_matching_seg_count,v_seg
        from jsonb_array_elements(case when jsonb_typeof(v_tf.invoice_breakdown_json->'segments')='array' then v_tf.invoice_breakdown_json->'segments' else '[]'::jsonb end) seg;
      end if;

      v_original_line:=null;
      v_original_tf:=null;
      v_original_seg:=null;
      v_original_seg_count:=0;
      v_original_matching_seg_count:=0;
      v_original_line_id:=null;
      if v_line.invoice_type='CREDIT_NOTE' then
        if coalesce(v_line.meta_json->>'original_invoice_line_id',v_line.meta_json->>'credit_of_line_id','')~*v_uuid_re then
          v_original_line_id:=coalesce(v_line.meta_json->>'original_invoice_line_id',v_line.meta_json->>'credit_of_line_id')::uuid;
          select original.* into v_original_line from public.invoice_lines original where original.id=v_original_line_id;
        end if;
        if v_original_line.id is null then
          v_scope_unprovable:=true;
          continue;
        end if;
        if coalesce(v_original_line.meta_json->>'tsfin_id','')~*v_uuid_re then
          select tf.* into v_original_tf from public.timesheets_financials tf where tf.id=(v_original_line.meta_json->>'tsfin_id')::uuid;
        elsif v_original_line.timesheet_id is not null then
          select tf.* into v_original_tf from public.timesheets_financials tf
          where tf.timesheet_id=v_original_line.timesheet_id
          order by case when tf.is_current then 0 else 1 end,tf.computed_at_utc desc nulls last,tf.id desc limit 1;
        end if;
        if v_original_tf.id is not null then
          select count(*)::integer,
            count(*) filter(where seg->>'nhsp_shift_id'=v_source_shift_id::text
              or seg->>'shift_id'=v_source_shift_id::text
              or seg->>'external_row_key'=v_external_row_key)::integer,
            (array_agg(seg order by seg::text) filter(where seg->>'nhsp_shift_id'=v_source_shift_id::text
              or seg->>'shift_id'=v_source_shift_id::text
              or seg->>'external_row_key'=v_external_row_key))[1]
          into v_original_seg_count,v_original_matching_seg_count,v_original_seg
          from jsonb_array_elements(case when jsonb_typeof(v_original_tf.invoice_breakdown_json->'segments')='array'
            then v_original_tf.invoice_breakdown_json->'segments' else '[]'::jsonb end) seg;
        end if;
        -- The credit writer preserves the original hour buckets and writes one
        -- exact signed monetary mirror.  Reject partial or contradictory credit
        -- shapes before allocating any source component.
        if coalesce(v_line.hours_day,0)<>coalesce(v_original_line.hours_day,0)
          or coalesce(v_line.hours_night,0)<>coalesce(v_original_line.hours_night,0)
          or coalesce(v_line.hours_sat,0)<>coalesce(v_original_line.hours_sat,0)
          or coalesce(v_line.hours_sun,0)<>coalesce(v_original_line.hours_sun,0)
          or coalesce(v_line.hours_bh,0)<>coalesce(v_original_line.hours_bh,0)
          or round(coalesce(v_line.total_pay_ex_vat,0),2)<>-round(coalesce(v_original_line.total_pay_ex_vat,0),2)
          or round(coalesce(v_line.total_charge_ex_vat,0),2)<>-round(coalesce(v_original_line.total_charge_ex_vat,0),2)
          or round(coalesce(v_line.margin_ex_vat,v_line.total_charge_ex_vat-v_line.total_pay_ex_vat,0),2)
            <>-round(coalesce(v_original_line.margin_ex_vat,v_original_line.total_charge_ex_vat-v_original_line.total_pay_ex_vat,0),2)
          or (v_original_seg_count>0 and v_original_matching_seg_count<>1)
          or (v_original_seg_count>1 and (v_original_seg is null
            or nullif(v_original_seg->>'pay_amount','') is null
            or nullif(v_original_seg->>'charge_amount','') is null)) then
          v_scope_unprovable:=true;
          continue;
        end if;
      end if;

      v_line_type:=upper(nullif(btrim(coalesce(v_line.meta_json->>'line_type','')),''));
      v_original_line_type:=upper(nullif(btrim(coalesce(v_original_line.meta_json->>'line_type','')),''));
      v_is_separable_nonhours:=coalesce(case when v_line.invoice_type='CREDIT_NOTE' then v_original_line_type else v_line_type end,'')
        ~ '^(EXPENSE(_.*)?|MILEAGE|TRAVEL|ACCOMMODATION|REIMBURSEMENT|ADDITION)$';
      if v_is_separable_nonhours then
        v_ignored_nonhours_line_ids:=array_append(v_ignored_nonhours_line_ids,v_line.id);
        continue;
      end if;
      v_is_weekly_hours:=coalesce(case when v_line.invoice_type='CREDIT_NOTE' then v_original_line_type else v_line_type end,'')='HOURS_WEEKLY';
      if not v_is_weekly_hours then
        -- Legacy lines are acceptable only when a single frozen source segment
        -- proves the exact Weekly component for this shift.
        v_is_weekly_hours:=case when v_line.invoice_type='CREDIT_NOTE'
          then v_original_seg is not null
          else v_matching_seg_count=1 end;
      end if;
      if not v_is_weekly_hours then
        v_scope_unprovable:=true;
        continue;
      end if;

      v_component_timesheet_id:=case
        when v_line.invoice_type='CREDIT_NOTE' then coalesce(
          v_original_line.timesheet_id,
          case when coalesce(v_original_line.meta_json->>'timesheet_id','')~*v_uuid_re
            then (v_original_line.meta_json->>'timesheet_id')::uuid end)
        else coalesce(v_line.timesheet_id,
          case when coalesce(v_line.meta_json->>'timesheet_id','')~*v_uuid_re
            then (v_line.meta_json->>'timesheet_id')::uuid end)
        end;
      v_component_correction_id:=null;
      v_component_correction_kind:=null;
      v_operation_member_scope_proven:=false;
      select member->>'correction_id',member->>'correction_kind',
        coalesce((member->>'operation_proven')::boolean,false)
      into v_component_correction_id,v_component_correction_kind,v_operation_member_scope_proven
      from jsonb_array_elements(v_member_role_map) member
      where member->>'timesheet_id'=v_component_timesheet_id::text
      limit 1;

      if v_line.invoice_status='DRAFT' or v_line.issued_at_utc is null
         or v_line.active_document_operation_id is not null or v_line.active_issue_operation_id is not null
         or upper(coalesce(v_line.issue_state,'')) not in ('','IDLE','COMPLETE','COMPLETED','ISSUED') then
        v_active_invoice_activity:=true;
      end if;
      if v_line.invoice_type='CREDIT_NOTE'
         and v_line.invoice_status in ('ISSUED','PAID','ON_HOLD') and v_line.issued_at_utc is not null and (
        select count(*) from public.invoice_lines other_credit
        join public.invoices other_credit_invoice on other_credit_invoice.id=other_credit.invoice_id
        where coalesce(other_credit.meta_json->>'original_invoice_line_id',other_credit.meta_json->>'credit_of_line_id','')
          =coalesce(v_line.meta_json->>'original_invoice_line_id',v_line.meta_json->>'credit_of_line_id','')
          and other_credit_invoice.type='CREDIT_NOTE' and other_credit_invoice.status in ('ISSUED','PAID','ON_HOLD')
          and other_credit_invoice.issued_at_utc is not null
      )>1 then
        v_credit_ambiguous:=true;
      end if;
      v_single_source:=v_matching_seg_count=1 and v_seg_count=1;
      if not v_single_source and v_line.timesheet_id is not null then
        select jsonb_typeof(t.actual_schedule_json)='array' and jsonb_array_length(t.actual_schedule_json)=1
          and t.actual_schedule_json @> jsonb_build_array(jsonb_build_object('shift_id',v_source_shift_id::text,'external_row_key',v_external_row_key))
        into v_single_source from public.timesheets t where t.timesheet_id=v_line.timesheet_id;
        v_single_source:=coalesce(v_single_source,false);
      end if;
      v_line_scope_proven:=case when v_line.invoice_type='CREDIT_NOTE'
        then v_original_seg is not null
          or coalesce(v_original_line.timesheet_id=any(v_hist_ids),false)
          or (v_operation_member_scope_proven and v_original_line_type='HOURS_WEEKLY')
        else v_single_source or v_matching_seg_count=1
          or (v_operation_member_scope_proven and v_line_type='HOURS_WEEKLY') end;
      if not v_line_scope_proven or (v_line.invoice_type='CREDIT_NOTE' and v_original_seg is null
          and not coalesce(v_original_line.timesheet_id=any(v_hist_ids),false)
          and not (v_operation_member_scope_proven and v_original_line_type='HOURS_WEEKLY')) then
        v_scope_unprovable:=true;
        continue;
      end if;
      if (v_line.invoice_type<>'CREDIT_NOTE' and v_tf.id is not null
            and (case when upper(coalesce(v_tf.basis::text,'')) in ('NHSP','NHSP_ADJUSTMENT','HEALTHROSTER_SELF_BILL','HEALTHROSTER_ADJUSTMENT') then 'SELF_BILL' else 'NORMAL' end)<>v_invoice_stream)
         or (v_line.invoice_type='CREDIT_NOTE' and v_original_tf.id is not null
            and (case when upper(coalesce(v_original_tf.basis::text,'')) in ('NHSP','NHSP_ADJUSTMENT','HEALTHROSTER_SELF_BILL','HEALTHROSTER_ADJUSTMENT') then 'SELF_BILL' else 'NORMAL' end)<>v_invoice_stream) then
        v_stream_conflict:=true;
        continue;
      end if;

      if v_line.invoice_status='DRAFT' or v_line.issued_at_utc is null
         or v_line.active_document_operation_id is not null or v_line.active_issue_operation_id is not null
         or upper(coalesce(v_line.issue_state,'')) not in ('','IDLE','COMPLETE','COMPLETED','ISSUED') then
        v_line_evidence:=v_line_evidence||jsonb_build_array(jsonb_build_object(
          'invoice_id',v_line.invoice_id,'invoice_line_id',v_line.id,'invoice_type',v_line.invoice_type,
          'economic_state','PENDING','timesheet_id',v_component_timesheet_id,
          'correction_id',v_component_correction_id,'correction_kind',v_component_correction_kind));
        continue;
      end if;

      if v_line.invoice_type='CREDIT_NOTE' then
        -- Hours are the negative of the exact original frozen component.  A
        -- multi-source credit receives the matching source segment's money,
        -- never the whole aggregate line's money.
        if v_original_seg is not null then
          v_component_day:=-coalesce((v_original_seg->>'hours_day')::numeric,0);
          v_component_night:=-coalesce((v_original_seg->>'hours_night')::numeric,0);
          v_component_sat:=-coalesce((v_original_seg->>'hours_sat')::numeric,0);
          v_component_sun:=-coalesce((v_original_seg->>'hours_sun')::numeric,0);
          v_component_bh:=-coalesce((v_original_seg->>'hours_bh')::numeric,0);
        else
          v_component_day:=-coalesce(v_original_line.hours_day,0);
          v_component_night:=-coalesce(v_original_line.hours_night,0);
          v_component_sat:=-coalesce(v_original_line.hours_sat,0);
          v_component_sun:=-coalesce(v_original_line.hours_sun,0);
          v_component_bh:=-coalesce(v_original_line.hours_bh,0);
        end if;
        if v_original_seg_count>1 then
          v_component_pay:=-coalesce((v_original_seg->>'pay_amount')::numeric,0);
          v_component_charge:=-coalesce((v_original_seg->>'charge_amount')::numeric,0);
          v_component_margin:=v_component_charge-v_component_pay;
        else
          v_component_pay:=coalesce(v_line.total_pay_ex_vat,0);
          v_component_charge:=coalesce(v_line.total_charge_ex_vat,0);
          v_component_margin:=coalesce(v_line.margin_ex_vat,v_component_charge-v_component_pay);
        end if;
      elsif v_single_source or v_operation_member_scope_proven then
        v_component_day:=coalesce(v_line.hours_day,0); v_component_night:=coalesce(v_line.hours_night,0);
        v_component_sat:=coalesce(v_line.hours_sat,0); v_component_sun:=coalesce(v_line.hours_sun,0); v_component_bh:=coalesce(v_line.hours_bh,0);
        v_component_pay:=coalesce(v_line.total_pay_ex_vat,0); v_component_charge:=coalesce(v_line.total_charge_ex_vat,0); v_component_margin:=coalesce(v_line.margin_ex_vat,v_component_charge-v_component_pay);
      else
        v_component_day:=coalesce((v_seg->>'hours_day')::numeric,0); v_component_night:=coalesce((v_seg->>'hours_night')::numeric,0);
        v_component_sat:=coalesce((v_seg->>'hours_sat')::numeric,0); v_component_sun:=coalesce((v_seg->>'hours_sun')::numeric,0); v_component_bh:=coalesce((v_seg->>'hours_bh')::numeric,0);
        v_component_pay:=coalesce((v_seg->>'pay_amount')::numeric,0); v_component_charge:=coalesce((v_seg->>'charge_amount')::numeric,0); v_component_margin:=v_component_charge-v_component_pay;
      end if;
      v_b_day:=v_b_day+v_component_day; v_b_night:=v_b_night+v_component_night; v_b_sat:=v_b_sat+v_component_sat; v_b_sun:=v_b_sun+v_component_sun; v_b_bh:=v_b_bh+v_component_bh;
      v_b_pay:=v_b_pay+v_component_pay; v_b_charge:=v_b_charge+v_component_charge; v_b_margin:=v_b_margin+v_component_margin;
      v_effective_component_count:=v_effective_component_count+1;
      v_effective_invoice_ids:=array_append(v_effective_invoice_ids,v_line.invoice_id);
      v_effective_line_ids:=array_append(v_effective_line_ids,v_line.id);
      if v_line.invoice_type='CREDIT_NOTE' then v_credit_line_ids:=array_append(v_credit_line_ids,v_line.id); end if;
      v_line_evidence:=v_line_evidence||jsonb_build_array(jsonb_build_object(
        'invoice_id',v_line.invoice_id,'invoice_line_id',v_line.id,'invoice_type',v_line.invoice_type,
        'economic_state','EFFECTIVE','timesheet_id',v_component_timesheet_id,
        'correction_id',v_component_correction_id,'correction_kind',v_component_correction_kind,
        'hours',jsonb_build_object('hours_day',v_component_day,'hours_night',v_component_night,'hours_sat',v_component_sat,'hours_sun',v_component_sun,'hours_bh',v_component_bh),
        'pay_ex_vat',v_component_pay,'charge_ex_vat',v_component_charge,'margin_ex_vat',v_component_margin));
      if v_seg is not null and (v_component_day+v_component_night+v_component_sat+v_component_sun+v_component_bh)>0 then
        v_candidate_schedule:=jsonb_build_array(jsonb_strip_nulls(jsonb_build_object(
          'date',coalesce(v_seg->>'date',(v_a_schedule->0)->>'date'),
          'start_utc',v_seg->>'start_utc','end_utc',v_seg->>'end_utc',
          'break_mins',coalesce((v_seg->>'break_mins')::integer,0),
          'shift_id',v_source_shift_id::text,'external_row_key',v_external_row_key,
          'import_id',coalesce(v_seg->>'import_id',v_authoritative_import_id::text),
          'ref_num',coalesce(v_seg->>'ref_num',v_seg->>'reference_number',(v_a_schedule->0)->>'ref_num')
        )));
        v_candidate_hours:=jsonb_build_object('hours_day',v_component_day,'hours_night',v_component_night,'hours_sat',v_component_sat,'hours_sun',v_component_sun,'hours_bh',v_component_bh,'total_hours',v_component_day+v_component_night+v_component_sat+v_component_sun+v_component_bh);
        v_b_policy_fingerprint:=coalesce(v_tf.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint',v_tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}',encode(digest(convert_to(coalesce(v_tf.policy_snapshot_json,'{}'::jsonb)::text,'UTF8'),'sha256'),'hex'));
      end if;
    end loop;

    select coalesce(array_agg(distinct x order by x),array[]::uuid[]) into v_effective_invoice_ids from unnest(v_effective_invoice_ids) x;
    select coalesce(array_agg(distinct x order by x),array[]::uuid[]) into v_effective_line_ids from unnest(v_effective_line_ids) x;
    select coalesce(array_agg(distinct x order by x),array[]::uuid[]) into v_credit_line_ids from unnest(v_credit_line_ids) x;
    v_effective_fingerprint:=encode(digest(convert_to(concat_ws('|','effective-invoice-v1',v_source_identity,v_line_evidence::text,v_b_day,v_b_night,v_b_sat,v_b_sun,v_b_bh,v_b_pay,v_b_charge,v_b_margin),'UTF8'),'sha256'),'hex');

    -- Classify each generation from the same admitted, signed Weekly-hours
    -- component ledger used for B.  Raw invoice-line existence is never a
    -- second financial authority, and archived identities are audit-only.
    with correction_seed as (
      select correction_id,max(evidence_at) evidence_at
      from (
        select member->>'correction_id' correction_id,
          coalesce((select max(coalesce(t.updated_at,t.created_at)) from public.timesheets t
            where t.timesheet_id=(member->>'timesheet_id')::uuid),'-infinity'::timestamptz) evidence_at
        from jsonb_array_elements(v_member_role_map) member
        union all
        select unit->>'correction_id',coalesce((unit->>'evidence_at')::timestamptz,'-infinity'::timestamptz)
        from jsonb_array_elements(v_operation_evidence) unit
      ) seeded where nullif(correction_id,'') is not null group by correction_id
    ), roles as (
      select seed.correction_id,seed.evidence_at,role
      from correction_seed seed
      cross join lateral unnest(array['CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT']) role
    ), role_state as (
      select r.correction_id,r.evidence_at,r.role,
        coalesce((select array_agg(distinct (member->>'timesheet_id')::uuid order by (member->>'timesheet_id')::uuid)
          from jsonb_array_elements(v_member_role_map) member
          where member->>'correction_id'=r.correction_id and member->>'correction_kind'=r.role),array[]::uuid[]) member_ids,
        coalesce((select array_agg(distinct t.timesheet_id order by t.timesheet_id)
          from jsonb_array_elements(v_member_role_map) member
          join public.timesheets t on t.timesheet_id=(member->>'timesheet_id')::uuid
          where member->>'correction_id'=r.correction_id and member->>'correction_kind'=r.role
            and t.is_current and t.archived_at_utc is null),array[]::uuid[]) active_ids,
        coalesce((select array_agg(distinct t.timesheet_id order by t.timesheet_id)
          from jsonb_array_elements(v_member_role_map) member
          join public.timesheets t on t.timesheet_id=(member->>'timesheet_id')::uuid
          where member->>'correction_id'=r.correction_id and member->>'correction_kind'=r.role
            and t.archived_at_utc is not null),array[]::uuid[]) archived_ids,
        coalesce((select array_agg(distinct (member->>'timesheet_id')::uuid order by (member->>'timesheet_id')::uuid)
          from jsonb_array_elements(v_member_role_map) member
          where member->>'correction_id'=r.correction_id and member->>'correction_kind'=r.role
            and coalesce((member->>'operation_proven')::boolean,false)
            and not exists(select 1 from public.timesheets t where t.timesheet_id=(member->>'timesheet_id')::uuid)),array[]::uuid[]) operation_missing_ids,
        exists(select 1 from jsonb_array_elements(v_line_evidence) component
          where component->>'correction_id'=r.correction_id and component->>'correction_kind'=r.role
            and component->>'economic_state'='EFFECTIVE') has_effective_history,
        exists(select 1 from jsonb_array_elements(v_line_evidence) component
          where component->>'correction_id'=r.correction_id and component->>'correction_kind'=r.role
            and component->>'economic_state'='PENDING')
          or exists(select 1
            from jsonb_array_elements(v_member_role_map) member
            join public.timesheets t on t.timesheet_id=(member->>'timesheet_id')::uuid
            left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
            left join public.contract_weeks cw on cw.timesheet_id=t.timesheet_id
            where member->>'correction_id'=r.correction_id and member->>'correction_kind'=r.role
              and t.is_current and t.archived_at_utc is null
              and (tf.locked_by_invoice_id is not null or upper(coalesce(cw.status::text,''))='INVOICED'
                or exists(select 1 from jsonb_array_elements(case when jsonb_typeof(tf.invoice_breakdown_json->'segments')='array'
                  then tf.invoice_breakdown_json->'segments' else '[]'::jsonb end) seg
                  where nullif(seg->>'invoice_locked_invoice_id','') is not null))) pending_invoice,
        exists(select 1
          from jsonb_array_elements(v_member_role_map) member
          join public.timesheets t on t.timesheet_id=(member->>'timesheet_id')::uuid
          join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
          where member->>'correction_id'=r.correction_id and member->>'correction_kind'=r.role
            and t.is_current and t.archived_at_utc is null and tf.paid_at_utc is not null) paid,
        (select count(*) from jsonb_array_elements(v_member_role_map) member
          join public.timesheets t on t.timesheet_id=(member->>'timesheet_id')::uuid
          where member->>'correction_id'=r.correction_id and member->>'correction_kind'=r.role
            and t.is_current and t.archived_at_utc is null)>1 active_duplicate,
        (select count(distinct component->>'timesheet_id') from jsonb_array_elements(v_line_evidence) component
          where component->>'correction_id'=r.correction_id and component->>'correction_kind'=r.role
            and component->>'economic_state' in ('EFFECTIVE','PENDING'))>1 economic_member_duplicate,
        coalesce((select sum(coalesce((component#>>'{hours,hours_day}')::numeric,0))
          from jsonb_array_elements(v_line_evidence) component
          where component->>'correction_id'=r.correction_id and component->>'correction_kind'=r.role
            and component->>'economic_state'='EFFECTIVE'),0) net_day,
        coalesce((select sum(coalesce((component#>>'{hours,hours_night}')::numeric,0))
          from jsonb_array_elements(v_line_evidence) component
          where component->>'correction_id'=r.correction_id and component->>'correction_kind'=r.role
            and component->>'economic_state'='EFFECTIVE'),0) net_night,
        coalesce((select sum(coalesce((component#>>'{hours,hours_sat}')::numeric,0))
          from jsonb_array_elements(v_line_evidence) component
          where component->>'correction_id'=r.correction_id and component->>'correction_kind'=r.role
            and component->>'economic_state'='EFFECTIVE'),0) net_sat,
        coalesce((select sum(coalesce((component#>>'{hours,hours_sun}')::numeric,0))
          from jsonb_array_elements(v_line_evidence) component
          where component->>'correction_id'=r.correction_id and component->>'correction_kind'=r.role
            and component->>'economic_state'='EFFECTIVE'),0) net_sun,
        coalesce((select sum(coalesce((component#>>'{hours,hours_bh}')::numeric,0))
          from jsonb_array_elements(v_line_evidence) component
          where component->>'correction_id'=r.correction_id and component->>'correction_kind'=r.role
            and component->>'economic_state'='EFFECTIVE'),0) net_bh
      from roles r
    ), generation_state as (
      select correction_id,max(evidence_at) evidence_at,
        count(*) filter(where cardinality(member_ids)>0) proven_roles,
        count(*) filter(where has_effective_history) effective_roles,
        count(*) filter(where pending_invoice) pending_roles,
        count(*) filter(where cardinality(active_ids)>0) active_role_count,
        count(*) filter(where cardinality(operation_missing_ids)>0) missing_operation_role_count,
        count(*) filter(where cardinality(active_ids)=0 and cardinality(archived_ids)>0
          and cardinality(operation_missing_ids)=0) archived_only_role_count,
        bool_or(paid) paid,bool_or(active_duplicate) active_duplicate,
        bool_or(economic_member_duplicate) economic_member_duplicate,
        jsonb_agg(jsonb_build_object(
          'role',role,'member_ids',to_jsonb(member_ids),'active_member_ids',to_jsonb(active_ids),
          'archived_member_ids',to_jsonb(archived_ids),'operation_proven_missing_member_ids',to_jsonb(operation_missing_ids),
          'has_effective_history',has_effective_history,'effective_state',case
            when has_effective_history and net_day+net_night+net_sat+net_sun+net_bh=0 then 'SETTLED_ZERO_HISTORY'
            when has_effective_history then 'EFFECTIVE_HISTORY'
            when pending_invoice then 'PENDING_INVOICE'
            when cardinality(active_ids)>0 then 'ACTIVE_MUTABLE'
            when cardinality(operation_missing_ids)>0 then 'PHYSICALLY_MISSING_MUTABLE'
            when cardinality(archived_ids)>0 then 'ARCHIVED_AUDIT_ONLY'
            else 'UNPROVABLE' end,
          'signed_net_hours',jsonb_build_object('hours_day',net_day,'hours_night',net_night,
            'hours_sat',net_sat,'hours_sun',net_sun,'hours_bh',net_bh,
            'total_hours',net_day+net_night+net_sat+net_sun+net_bh),
          'pending_invoice',pending_invoice,'paid',paid,'economic_member_duplicate',economic_member_duplicate
        ) order by role) role_evidence
      from role_state group by correction_id
    )
    select
      coalesce((select jsonb_agg(jsonb_build_object('correction_id',g.correction_id,'state',case
          when g.active_duplicate or g.economic_member_duplicate then 'UNPROVABLE'
          when g.effective_roles=2 then 'FULLY_INVOICED'
          when g.effective_roles=1 and g.proven_roles=2 then 'PARTIALLY_INVOICED'
          when g.effective_roles=0 and g.pending_roles=0 and g.proven_roles=2 and not g.paid
            and (g.active_role_count>0 or g.missing_operation_role_count=2) then 'MUTABLE'
          when g.effective_roles=0 and g.pending_roles=0 and g.active_role_count=0
            and g.archived_only_role_count=2 then 'ARCHIVED_AUDIT_ONLY'
          else 'UNPROVABLE' end,
          'roles',g.role_evidence) order by g.evidence_at,g.correction_id) from generation_state g),'[]'::jsonb),
      coalesce((select array_agg(g.correction_id order by g.evidence_at,g.correction_id) from generation_state g
        where g.effective_roles=2 and not g.active_duplicate and not g.economic_member_duplicate),array[]::text[]),
      coalesce((select array_agg(g.correction_id order by g.evidence_at,g.correction_id) from generation_state g
        where g.effective_roles=1 and g.proven_roles=2 and not g.active_duplicate and not g.economic_member_duplicate),array[]::text[]),
      coalesce((select array_agg(g.correction_id order by g.evidence_at,g.correction_id) from generation_state g
        where g.effective_roles=0 and g.pending_roles=0 and g.proven_roles=2 and not g.paid
          and not g.active_duplicate and not g.economic_member_duplicate
          and (g.active_role_count>0 or g.missing_operation_role_count=2)),array[]::text[]),
      (select g.correction_id from generation_state g
        where g.effective_roles=0 and g.pending_roles=0 and g.proven_roles=2 and not g.paid
          and not g.active_duplicate and not g.economic_member_duplicate
          and (g.active_role_count>0 or g.missing_operation_role_count=2)
        order by g.evidence_at desc,g.correction_id desc limit 1),
      coalesce((select jsonb_agg(jsonb_build_object('correction_id',r.correction_id,'role',r.role,
        'timesheet_ids',to_jsonb(r.archived_ids)) order by r.correction_id,r.role)
        from role_state r where cardinality(r.archived_ids)>0),'[]'::jsonb),
      coalesce((select jsonb_agg(jsonb_build_object('correction_id',g.correction_id,'reason',case
          when g.active_duplicate then 'ACTIVE_ROLE_DUPLICATE'
          when g.economic_member_duplicate then 'DUPLICATE_EFFECTIVE_ROLE_WITHOUT_REPAIR_LINEAGE'
          else 'ROLE_IDENTITY_UNPROVABLE' end) order by g.evidence_at,g.correction_id)
        from generation_state g where g.active_duplicate or g.economic_member_duplicate
          or (g.proven_roles<2 and (g.effective_roles>0 or g.pending_roles>0))),'[]'::jsonb),
      exists(select 1 from generation_state g where g.effective_roles=1 and g.proven_roles=2
        and not g.active_duplicate and not g.economic_member_duplicate),
      exists(select 1 from generation_state g where g.pending_roles>0),
      exists(select 1 from generation_state g where g.active_duplicate or g.economic_member_duplicate
        or (g.proven_roles<2 and (g.effective_roles>0 or g.pending_roles>0)))
    into v_generation_role_evidence,v_fully_invoiced_generation_ids,v_partial_generation_ids,v_mutable_generation_ids,
      v_mutable_correction_id,v_archived_history_roles,v_role_evidence_conflicts,v_role_partial_invoice_state,
      v_role_active_invoice_activity,v_role_scope_unprovable;

    v_partial_invoice_state:=v_role_partial_invoice_state;
    v_active_invoice_activity:=v_active_invoice_activity or v_role_active_invoice_activity or v_operation_in_progress;
    v_scope_unprovable:=v_scope_unprovable or v_role_scope_unprovable
      or v_operation_evidence_conflict or v_member_role_conflict;

    if v_mutable_correction_id is not null then
      v_repair_identity_mode:=case when exists(select 1 from public.timesheets archived
        where archived.correction_id=v_mutable_correction_id and archived.archived_at_utc is not null
          and archived.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT'))
        then 'FRESH_CORRECTION_ID_ARCHIVED_ROLE_IGNORED' else 'RETAIN_EXISTING_CORRECTION_ID' end;
    end if;
    v_role_evidence_fingerprint:=encode(digest(convert_to(concat_ws('|','role-evidence-v3',
      v_operation_evidence::text,v_member_supersession_map::text,v_member_role_map::text,v_generation_role_evidence::text,
      v_archived_history_roles::text,v_role_evidence_conflicts::text),'UTF8'),'sha256'),'hex');
    v_effective_fingerprint:=encode(digest(convert_to(concat_ws('|','effective-invoice-v3',v_source_identity,
      v_line_evidence::text,v_ignored_nonhours_line_ids::text,v_role_evidence_fingerprint,
      v_b_day,v_b_night,v_b_sat,v_b_sun,v_b_bh,v_b_pay,v_b_charge,v_b_margin),'UTF8'),'sha256'),'hex');

    v_mutable_member_ids:=array[]::uuid[]; v_mutable_missing_roles:=array[]::text[];
    v_mutable_parent_id:=null; v_m_day:=0; v_m_night:=0; v_m_sat:=0; v_m_sun:=0; v_m_bh:=0;
    v_m_pay:=0; v_m_charge:=0; v_m_margin:=0; v_m_financials_complete:=true; v_paid_mutable_state:=false;
    if v_mutable_correction_id is not null then
      select coalesce(array_agg(t.timesheet_id order by t.correction_kind,t.timesheet_id),array[]::uuid[]),
        (array_agg(t.parent_timesheet_id order by t.created_at,t.timesheet_id))[1],
        coalesce(sum(tf.hours_day),0),coalesce(sum(tf.hours_night),0),coalesce(sum(tf.hours_sat),0),coalesce(sum(tf.hours_sun),0),coalesce(sum(tf.hours_bh),0),
        coalesce(sum(tf.total_pay_ex_vat),0),coalesce(sum(tf.total_charge_ex_vat),0),coalesce(sum(tf.margin_ex_vat),0),
        count(*)=count(tf.id) and bool_and(not coalesce(tf.is_stale,true) and not coalesce(tf.has_rate_issue,false) and not coalesce(tf.has_pay_channel_issue,false)),
        bool_or(tf.paid_at_utc is not null)
      into v_mutable_member_ids,v_mutable_parent_id,v_m_day,v_m_night,v_m_sat,v_m_sun,v_m_bh,v_m_pay,v_m_charge,v_m_margin,v_m_financials_complete,v_paid_mutable_state
      from public.timesheets t left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
      where t.correction_id=v_mutable_correction_id and t.is_current and t.archived_at_utc is null
        and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT');
      if not exists(select 1 from public.timesheets t where t.correction_id=v_mutable_correction_id and t.is_current and t.archived_at_utc is null and t.correction_kind='CHANGED_HOURS_REVERSAL') then
        v_mutable_missing_roles:=array_append(v_mutable_missing_roles,'CHANGED_HOURS_REVERSAL');
      end if;
      if not exists(select 1 from public.timesheets t where t.correction_id=v_mutable_correction_id and t.is_current and t.archived_at_utc is null and t.correction_kind='CHANGED_HOURS_REPLACEMENT') then
        v_mutable_missing_roles:=array_append(v_mutable_missing_roles,'CHANGED_HOURS_REPLACEMENT');
      end if;
    end if;
    v_mutable_fingerprint:=encode(digest(convert_to(concat_ws('|','mutable-v1',v_mutable_correction_id,v_mutable_member_ids::text,v_mutable_missing_roles::text,v_m_day,v_m_night,v_m_sat,v_m_sun,v_m_bh,v_m_pay,v_m_charge,v_m_margin),'UTF8'),'sha256'),'hex');

    -- If a historical member and its TSFIN were physically deleted, the
    -- latest fully invoiced generation's validated applied operation still
    -- carries the reviewed replacement schedule.  Use it only when every
    -- hours bucket exactly matches the signed frozen ledger.
    if jsonb_array_length(v_candidate_schedule)=0 and cardinality(v_fully_invoiced_generation_ids)>0 then
      select coalesce((select unit->'A_schedule_json'
        from jsonb_array_elements(v_operation_evidence) unit
        where unit->>'correction_id'=v_fully_invoiced_generation_ids[cardinality(v_fully_invoiced_generation_ids)]
          and jsonb_typeof(unit->'A_schedule_json')='array' and jsonb_array_length(unit->'A_schedule_json')=1
          and coalesce((unit#>>'{A_hours,hours_day}')::numeric,0)=v_b_day
          and coalesce((unit#>>'{A_hours,hours_night}')::numeric,0)=v_b_night
          and coalesce((unit#>>'{A_hours,hours_sat}')::numeric,0)=v_b_sat
          and coalesce((unit#>>'{A_hours,hours_sun}')::numeric,0)=v_b_sun
          and coalesce((unit#>>'{A_hours,hours_bh}')::numeric,0)=v_b_bh
        order by (unit->>'evidence_at')::timestamptz desc,unit->>'operation_id' desc limit 1),v_candidate_schedule),
        coalesce((select unit->'A_hours'
        from jsonb_array_elements(v_operation_evidence) unit
        where unit->>'correction_id'=v_fully_invoiced_generation_ids[cardinality(v_fully_invoiced_generation_ids)]
          and jsonb_typeof(unit->'A_schedule_json')='array' and jsonb_array_length(unit->'A_schedule_json')=1
          and coalesce((unit#>>'{A_hours,hours_day}')::numeric,0)=v_b_day
          and coalesce((unit#>>'{A_hours,hours_night}')::numeric,0)=v_b_night
          and coalesce((unit#>>'{A_hours,hours_sat}')::numeric,0)=v_b_sat
          and coalesce((unit#>>'{A_hours,hours_sun}')::numeric,0)=v_b_sun
          and coalesce((unit#>>'{A_hours,hours_bh}')::numeric,0)=v_b_bh
        order by (unit->>'evidence_at')::timestamptz desc,unit->>'operation_id' desc limit 1),v_candidate_hours)
      into v_candidate_schedule,v_candidate_hours;
    end if;

    v_b_hours_zero:=v_b_day=0 and v_b_night=0 and v_b_sat=0 and v_b_sun=0 and v_b_bh=0;
    v_b_money_zero:=round(v_b_pay,2)=0 and round(v_b_charge,2)=0 and round(v_b_margin,2)=0;
    v_b_standard_representable:=(v_b_hours_zero and v_b_money_zero)
      or (v_b_day>=0 and v_b_night>=0 and v_b_sat>=0 and v_b_sun>=0 and v_b_bh>=0
        and coalesce((v_candidate_hours->>'hours_day')::numeric,0)=v_b_day
        and coalesce((v_candidate_hours->>'hours_night')::numeric,0)=v_b_night
        and coalesce((v_candidate_hours->>'hours_sat')::numeric,0)=v_b_sat
        and coalesce((v_candidate_hours->>'hours_sun')::numeric,0)=v_b_sun
        and coalesce((v_candidate_hours->>'hours_bh')::numeric,0)=v_b_bh
        and jsonb_array_length(v_candidate_schedule)=1);
    if v_b_standard_representable and (v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh)>0 then v_b_schedule:=v_candidate_schedule; end if;

    if v_mutable_correction_id is not null then
      v_reversal_repair_required:=not exists(select 1 from public.timesheets t
        where t.correction_id=v_mutable_correction_id and t.correction_kind='CHANGED_HOURS_REVERSAL'
          and t.is_current and t.archived_at_utc is null
          and t.actual_schedule_json is not distinct from v_b_schedule);
      v_replacement_repair_required:=not exists(select 1 from public.timesheets t
        where t.correction_id=v_mutable_correction_id and t.correction_kind='CHANGED_HOURS_REPLACEMENT'
          and t.is_current and t.archived_at_utc is null
          and t.actual_schedule_json is not distinct from v_a_schedule);
    end if;

    v_effective_zero:=v_b_hours_zero and v_b_money_zero;
    v_source_protection:=public._import_review_timesheet_protection_core_v1(v_source_timesheet_id);
    select count(*)::integer
    into v_current_source_count
    from public.timesheets t
    where t.timesheet_id=v_source_timesheet_id and t.is_current and t.archived_at_utc is null
      and coalesce(t.correction_kind::text,'') not in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
      and t.contract_id=v_contract_id and t.week_ending_date=v_week_ending_date
      and jsonb_typeof(t.actual_schedule_json)='array'
      and t.actual_schedule_json @> jsonb_build_array(jsonb_build_object(
        'shift_id',v_source_shift_id::text,'external_row_key',v_external_row_key));
    v_current_source_invoice_lined:=exists(select 1 from public.invoice_lines il
      where il.timesheet_id=v_source_timesheet_id
        or il.meta_json->>'timesheet_id'=v_source_timesheet_id::text);
    select coalesce(bool_or(tf.paid_at_utc is not null),false),
      coalesce(bool_and(tf.locked_by_invoice_id is null),false),
      count(*)=1 and coalesce(bool_and(not coalesce(tf.is_stale,true)
        and not coalesce(tf.has_rate_issue,false) and not coalesce(tf.has_pay_channel_issue,false)),false),
      count(*)=1 and coalesce(bool_and(not exists(
        select 1 from jsonb_array_elements(case when jsonb_typeof(tf.invoice_breakdown_json->'segments')='array'
          then tf.invoice_breakdown_json->'segments' else '[]'::jsonb end) seg
        where nullif(seg->>'invoice_locked_invoice_id','') is not null)),false)
    into v_current_source_paid,v_current_source_unlocked,v_current_source_fresh,v_current_source_segment_unlocked
    from public.timesheets_financials tf
    where tf.timesheet_id=v_source_timesheet_id and tf.is_current
      and tf.candidate_id=v_candidate_id;
    select count(*)=1 and bool_and(upper(coalesce(cw.status::text,'')) not in ('INVOICED','CANCELLED'))
    into v_current_source_contract_week_safe
    from public.contract_weeks cw
    where cw.timesheet_id=v_source_timesheet_id and cw.contract_id=v_contract_id
      and cw.week_ending_date=v_week_ending_date;
    v_current_source_contract_week_safe:=coalesce(v_current_source_contract_week_safe,false);
    v_current_source_invoice_operation_clear:=not exists(
      select 1
      from public.invoice_lines il
      join public.invoices i on i.id=il.invoice_id
      where (il.timesheet_id=v_source_timesheet_id or il.meta_json->>'timesheet_id'=v_source_timesheet_id::text)
        and (i.active_document_operation_id is not null or i.active_issue_operation_id is not null
          or upper(coalesce(i.issue_state,'')) not in ('','IDLE','COMPLETE','COMPLETED','ISSUED'))
    );
    v_current_source_safe:=v_current_source_count=1
      and not v_current_source_invoice_lined
      and not v_current_source_paid
      and v_current_source_unlocked and v_current_source_fresh and v_current_source_segment_unlocked
      and v_current_source_contract_week_safe and v_current_source_invoice_operation_clear
      and not coalesce((v_source_protection->>'paid')::boolean,false)
      and not coalesce((v_source_protection->>'invoice_locked')::boolean,false)
      and not coalesce((v_source_protection->>'active_pay_draft')::boolean,false);
    v_current_source_safety_reason:=case
      when v_current_source_safe then 'SAFE_CURRENT_ORDINARY_SOURCE'
      when v_current_source_paid and v_current_source_invoice_lined then 'CURRENT_SOURCE_PAID_AND_INVOICE_LINED'
      when v_current_source_invoice_lined then 'CURRENT_SOURCE_INVOICE_LINED_AFTER_EFFECTIVE_ZERO'
      when v_current_source_paid then 'CURRENT_SOURCE_PAID_AFTER_EFFECTIVE_ZERO'
      when v_current_source_count<>1 then 'NO_EXACT_CURRENT_ORDINARY_SOURCE'
      when not v_current_source_unlocked then 'CURRENT_SOURCE_INVOICE_LOCKED'
      when not v_current_source_fresh then 'CURRENT_SOURCE_TSFIN_NOT_FRESH'
      when not v_current_source_segment_unlocked then 'CURRENT_SOURCE_SEGMENT_LOCKED'
      when not v_current_source_invoice_operation_clear then 'CURRENT_SOURCE_INVOICE_OPERATION_ACTIVE'
      when not v_current_source_contract_week_safe then 'CURRENT_SOURCE_CONTRACT_WEEK_UNSAFE'
      when coalesce((v_source_protection->>'active_pay_draft')::boolean,false) then 'CURRENT_SOURCE_ACTIVE_PAY_DRAFT'
      else 'CURRENT_SOURCE_LIFECYCLE_UNSAFE' end;

    v_blocking_code:=case
      when v_partial_invoice_state then 'IMPORT_REVIEW_CORRECTION_GENERATION_PARTIALLY_INVOICED'
      when v_active_invoice_activity then 'IMPORT_REVIEW_INVOICE_ACTIVITY_IN_PROGRESS'
      when v_credit_ambiguous then 'IMPORT_REVIEW_EFFECTIVE_CREDIT_AMBIGUOUS'
      when v_scope_unprovable or v_stream_conflict then 'IMPORT_REVIEW_INVOICE_COMPONENT_SCOPE_UNPROVABLE'
      when v_paid_mutable_state then 'IMPORT_REVIEW_PAID_MUTABLE_GENERATION_ROLLOVER_UNAVAILABLE'
      when v_b_hours_zero and not v_b_money_zero then 'IMPORT_REVIEW_EFFECTIVE_POSITION_NOT_STANDARD_REPRESENTABLE'
      when v_effective_zero and v_effective_component_count>0 and v_mutable_correction_id is null
        and coalesce((v_a_hours->>'total_hours')::numeric,0)>0 and not v_current_source_safe
        then 'IMPORT_REVIEW_EFFECTIVE_ZERO_NO_ACTIVE_SOURCE'
      when (v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh)<0 then 'IMPORT_REVIEW_INVOICE_STATE_UNSUPPORTED'
      when (v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh)>0 and not v_b_standard_representable then 'IMPORT_REVIEW_EFFECTIVE_POSITION_NOT_STANDARD_REPRESENTABLE'
      else null end;
    v_reconciliation_fingerprint:=encode(digest(convert_to(concat_ws('|','reconciliation-v3',v_scope_fingerprint,v_operation_ids::text,v_member_supersession_map::text,
      v_effective_fingerprint,v_mutable_fingerprint,v_a_fingerprint,v_blocking_code,v_b_policy_fingerprint,
      v_current_source_safe,v_current_source_safety_reason,v_current_source_invoice_lined,v_current_source_paid,
      v_current_source_unlocked,v_current_source_fresh,v_current_source_segment_unlocked,
      v_current_source_contract_week_safe,v_current_source_invoice_operation_clear,v_b_hours_zero,v_b_money_zero),'UTF8'),'sha256'),'hex');

    source_identity:=v_source_identity;
    balance_json:=jsonb_build_object(
      'schema_version','IMPORT_AUTHORITATIVE_RECONCILIATION_BALANCE_V1',
      'source_identity',v_source_identity,'source_system',v_source_system,'source_shift_id',v_source_shift_id,
      'external_row_key',v_external_row_key,'source_timesheet_id',v_source_timesheet_id,
      'candidate_id',v_candidate_id,'client_id',v_client_id,'contract_id',v_contract_id,
      'week_ending_date',v_week_ending_date,'invoice_stream',v_invoice_stream,
      'source_scope_fingerprint',v_scope_fingerprint,
      'archived_timesheet_ids',to_jsonb(v_archived_ids),'archived_history_timesheet_ids',to_jsonb(v_archived_ids),
      'archived_history_roles',v_archived_history_roles,'active_timesheet_ids',to_jsonb(v_active_ids),
      'historical_missing_timesheet_ids',to_jsonb(v_missing_ids),
      'effective_invoice_ids',to_jsonb(v_effective_invoice_ids),'effective_invoice_line_ids',to_jsonb(v_effective_line_ids),
      'effective_credit_line_ids',to_jsonb(v_credit_line_ids),'effective_invoice_component_count',v_effective_component_count,
      'effective_hours_component_count',v_effective_component_count,
      'ignored_nonhours_invoice_line_ids',to_jsonb(v_ignored_nonhours_line_ids)
    ) || jsonb_build_object(
      'generation_role_evidence',v_generation_role_evidence,
      'validated_completed_operation_evidence_count',jsonb_array_length(v_operation_evidence),
      'validated_completed_operation_evidence_fingerprint',encode(digest(convert_to(v_operation_evidence::text,'UTF8'),'sha256'),'hex'),
      'correction_member_supersession_lineage',v_member_supersession_map,
      'fully_invoiced_generation_ids',to_jsonb(v_fully_invoiced_generation_ids),
      'partial_generation_ids',to_jsonb(v_partial_generation_ids),
      'mutable_generation_ids',to_jsonb(v_mutable_generation_ids),
      'role_evidence_conflicts',v_role_evidence_conflicts,
      'role_evidence_fingerprint',v_role_evidence_fingerprint,
      'effective_invoice_fingerprint',v_effective_fingerprint,
      'B_hours',jsonb_build_object('hours_day',v_b_day,'hours_night',v_b_night,'hours_sat',v_b_sat,'hours_sun',v_b_sun,'hours_bh',v_b_bh,'total_hours',v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh),
      'effective_hours_net_is_zero',(v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh)=0,
      'effective_money_net_is_zero',v_b_money_zero,
      'effective_position_net_is_zero',v_effective_zero,
      'effective_hours_net_is_positive',(v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh)>0,
      'effective_hours_net_is_negative',(v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh)<0,
      'B_financials',jsonb_build_object('pay_ex_vat',v_b_pay,'charge_ex_vat',v_b_charge,'margin_ex_vat',v_b_margin),
      'B_standard_schedule_json',v_b_schedule,'B_policy_fingerprint',v_b_policy_fingerprint,'B_standard_representable',v_b_standard_representable,
      'active_mutable_generation',v_mutable_correction_id is not null,'active_mutable_member_ids',to_jsonb(v_mutable_member_ids),
      'active_mutable_missing_roles',to_jsonb(v_mutable_missing_roles),'active_mutable_correction_id',v_mutable_correction_id,
      'physically_missing_mutable_roles',to_jsonb(v_mutable_missing_roles),
      'reviewed_existing_correction_id',v_mutable_correction_id,'repair_identity_mode',v_repair_identity_mode,
      'reversal_repair_required',v_reversal_repair_required,'replacement_repair_required',v_replacement_repair_required
    ) || jsonb_build_object(
      'active_mutable_parent_timesheet_id',v_mutable_parent_id,'active_mutable_fingerprint',v_mutable_fingerprint,
      'M_hours',jsonb_build_object('hours_day',v_m_day,'hours_night',v_m_night,'hours_sat',v_m_sat,'hours_sun',v_m_sun,'hours_bh',v_m_bh,'total_hours',v_m_day+v_m_night+v_m_sat+v_m_sun+v_m_bh),
      'M_existing_financials',jsonb_build_object('pay_ex_vat',v_m_pay,'charge_ex_vat',v_m_charge,'margin_ex_vat',v_m_margin),'M_financials_complete',v_m_financials_complete,
      'A_schedule_json',v_a_schedule,'A_hours',v_a_hours,'A_evidence_fingerprint',v_a_fingerprint,
      'partial_invoice_state',v_partial_invoice_state,'active_invoice_activity',v_active_invoice_activity,
      'archived_active_conflict',false,'archived_invoice_conflict',false,
      'paid_mutable_state',v_paid_mutable_state,
      'current_source_safe_for_effective_zero_amendment',v_current_source_safe,
      'effective_zero_source_safety_reason',v_current_source_safety_reason,
      'current_source_invoice_lined',v_current_source_invoice_lined,'current_source_paid',v_current_source_paid,
      'recommended_route_inputs',jsonb_build_object('B_positive',(v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh)>0,
        'has_mutable_generation',v_mutable_correction_id is not null,'source_timesheet_active',v_source_timesheet_id=any(v_active_ids),
        'current_source_safe_for_effective_zero_amendment',v_current_source_safe),
      'blocking_code',v_blocking_code,'reconciliation_fingerprint',v_reconciliation_fingerprint
    );
    return next;
  end loop;
end
$_$;


--
-- Name: _import_review_timesheet_protection_core_v1(uuid); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public._import_review_timesheet_protection_core_v1(uuid) RETURNS jsonb
    LANGUAGE sql
    AS $$select '{}'::jsonb$$;


--
-- Name: fixture_add_completed_operation(text, text, uuid, uuid, uuid, uuid, uuid, date, uuid, uuid, text, text, uuid, uuid, text, text, text, jsonb, timestamp with time zone); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.fixture_add_completed_operation(p_source_identity text, p_source_system text, p_shift uuid, p_source_timesheet uuid, p_candidate uuid, p_client uuid, p_contract uuid, p_week date, p_import uuid, p_operation uuid, p_action text, p_correction text, p_reversal uuid, p_replacement uuid, p_route text, p_repair text, p_old_correction text DEFAULT NULL::text, p_reviewed_members jsonb DEFAULT '[]'::jsonb, p_finished timestamp with time zone DEFAULT now()) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'public', 'extensions'
    AS $$
declare
 v_unit_fp text:=repeat(substr(p_action,1,1),64);
 v_recon_fp text:=repeat(substr(p_action,2,1),64);
 v_request jsonb; v_env_base jsonb; v_env_fp text; v_policy jsonb;
 v_contract_base jsonb; v_contract jsonb; v_applied jsonb; v_applied_fp text;
begin
 v_request:=jsonb_build_object('action_id',p_action,'route',p_route,'source_identity',p_source_identity,
  'source_system',p_source_system,'source_shift_id',p_shift::text,'source_timesheet_id',p_source_timesheet::text,
  'candidate_id',p_candidate::text,'client_id',p_client::text,'contract_id',p_contract::text,
  'week_ending_date',p_week::text,'unit_fingerprint',v_unit_fp,'reconciliation_fingerprint',v_recon_fp,
  'repair_identity_mode',p_repair,'reviewed_existing_correction_id',p_old_correction,
  'reviewed_existing_member_ids',p_reviewed_members,'B_standard_schedule_json','[]'::jsonb,
  'B_hours','{}'::jsonb,'A_schedule_json',jsonb_build_array(jsonb_build_object('shift_id',p_shift::text,'external_row_key',split_part(p_source_identity,'|',2))),
  'A_hours',jsonb_build_object('hours_day',12,'hours_night',0,'hours_sat',0,'hours_sun',0,'hours_bh',0,'total_hours',12));
 v_env_base:=jsonb_build_object('source_shift_id',p_shift::text);
 v_env_fp:=encode(digest(convert_to(v_env_base::text,'UTF8'),'sha256'),'hex');
 v_policy:=jsonb_build_object('action_id',p_action,'source_row_key',p_source_identity,'source_shift_id',p_shift::text,
  'root_timesheet_id',p_source_timesheet::text,'policy_envelope',v_env_base||jsonb_build_object('envelope_fingerprint',v_env_fp),
  'policy_envelope_fingerprint',v_env_fp);
 v_contract_base:=jsonb_build_object('correction_units',jsonb_build_array(v_policy));
 v_contract:=v_contract_base||jsonb_build_object('operation_contract_fingerprint',encode(digest(convert_to(v_contract_base::text,'UTF8'),'sha256'),'hex'));
 v_applied:=jsonb_build_object('action_id',p_action,'source_identity',p_source_identity,'source_system',p_source_system,
  'source_shift_id',p_shift::text,'correction_id',p_correction,'reversal_timesheet_id',p_reversal::text,
  'replacement_timesheet_id',p_replacement::text,'applied_member_ids',jsonb_build_array(p_reversal::text,p_replacement::text),
  'parent_timesheet_id',p_source_timesheet::text,'repair_identity_mode',p_repair,
  'reviewed_unit_fingerprint',v_unit_fp,'reconciliation_fingerprint',v_recon_fp);
 v_applied_fp:=encode(digest(convert_to(jsonb_build_object('correction_id',p_correction,
  'reversal_timesheet_id',p_reversal,'replacement_timesheet_id',p_replacement,
  'M_active_member_ids',jsonb_build_array(p_reversal::text,p_replacement::text),
  'applied_member_ids',jsonb_build_array(p_reversal::text,p_replacement::text),
  'parent_timesheet_id',p_source_timesheet,'repair_identity_mode',p_repair,
  'reviewed_unit_fingerprint',v_unit_fp,'reconciliation_fingerprint',v_recon_fp)::text,'UTF8'),'sha256'),'hex');
 v_applied:=v_applied||jsonb_build_object('applied_result_fingerprint',v_applied_fp);
 insert into public.import_apply_operations values(p_operation,p_import,'COMPLETE',p_finished,p_finished,
  jsonb_build_object('request_envelope',jsonb_build_object('reconciliation_units',jsonb_build_array(v_request)),
   'reconciliation_units',jsonb_build_array(v_applied),'correction_operation_contract',v_contract));
 insert into public.import_review_decisions values(p_action,p_import,'APPLY_AMENDMENT',p_source_identity,p_shift,p_candidate,p_client,p_contract,p_source_timesheet);
 insert into public.import_review_action_outcomes values(p_action,p_import,p_operation,'APPLY_AMENDMENT',p_source_identity,p_shift,p_candidate,p_client,p_contract,p_source_timesheet);
end $$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: audit_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.audit_events (
    action text,
    after_json jsonb,
    object_type text,
    object_id_text text
);


--
-- Name: contract_weeks; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.contract_weeks (
    timesheet_id uuid,
    contract_id uuid,
    week_ending_date date,
    status text
);


--
-- Name: hr_rows; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.hr_rows (
    id uuid,
    import_id uuid,
    external_row_key text
);


--
-- Name: import_apply_operations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.import_apply_operations (
    id uuid,
    import_id uuid,
    state text,
    committed_at_utc timestamp with time zone,
    finalised_at_utc timestamp with time zone,
    response_json jsonb
);


--
-- Name: import_review_action_outcomes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.import_review_action_outcomes (
    action_id text,
    import_id uuid,
    operation_id uuid,
    action_kind text,
    source_identity text,
    shift_id uuid,
    candidate_id uuid,
    client_id uuid,
    contract_id uuid,
    timesheet_id uuid
);


--
-- Name: import_review_decisions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.import_review_decisions (
    action_id text,
    import_id uuid,
    action_kind text,
    source_identity text,
    shift_id uuid,
    candidate_id uuid,
    client_id uuid,
    contract_id uuid,
    timesheet_id uuid
);


--
-- Name: invoice_lines; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.invoice_lines (
    id uuid,
    invoice_id uuid,
    timesheet_id uuid,
    meta_json jsonb,
    hours_day numeric,
    hours_night numeric,
    hours_sat numeric,
    hours_sun numeric,
    hours_bh numeric,
    total_pay_ex_vat numeric,
    total_charge_ex_vat numeric,
    margin_ex_vat numeric,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: invoices; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.invoices (
    id uuid,
    type text,
    status text,
    issued_at_utc timestamp with time zone,
    original_invoice_id uuid,
    active_document_operation_id uuid,
    active_issue_operation_id uuid,
    issue_state text
);


--
-- Name: nhsp_shifts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nhsp_shifts (
    id uuid,
    external_row_key text,
    source_system text,
    candidate_id uuid,
    client_id uuid,
    contract_id uuid,
    week_ending_date date,
    timesheet_id uuid,
    latest_import_id uuid
);


--
-- Name: timesheets; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.timesheets (
    timesheet_id uuid,
    contract_id uuid,
    week_ending_date date,
    actual_schedule_json jsonb,
    candidate_hint_text jsonb,
    correction_id text,
    correction_kind text,
    parent_timesheet_id uuid,
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    is_current boolean,
    archived_at_utc timestamp with time zone
);


--
-- Name: timesheets_financials; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.timesheets_financials (
    id uuid,
    timesheet_id uuid,
    is_current boolean,
    computed_at_utc timestamp with time zone,
    invoice_breakdown_json jsonb,
    basis text,
    policy_snapshot_json jsonb,
    candidate_id uuid,
    locked_by_invoice_id uuid,
    is_stale boolean,
    has_rate_issue boolean,
    has_pay_channel_issue boolean,
    paid_at_utc timestamp with time zone,
    hours_day numeric,
    hours_night numeric,
    hours_sat numeric,
    hours_sun numeric,
    hours_bh numeric,
    total_pay_ex_vat numeric,
    total_charge_ex_vat numeric,
    margin_ex_vat numeric
);


--
-- PostgreSQL database dump complete
--

\unrestrict G9axQQoMzC77z9luhCP1P9GP7cxcG1UjVG1pX8FZfqVwQ4yej12pQNch147Yfyy
