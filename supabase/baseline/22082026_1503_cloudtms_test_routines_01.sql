-- Immutable CloudTMS TEST function snapshot, page 01.
-- Generated from pg_get_functiondef; definitions only, with function body checks deferred for forward references.
-- Do not edit an applied baseline page. Add or replace routine authority in supabase/repeatable.

\set ON_ERROR_STOP on
set check_function_bodies = off;
set search_path = pg_catalog, public, extensions;

-- _import_review_effective_authority_core_v1(text,uuid,uuid,date)
CREATE OR REPLACE FUNCTION public._import_review_effective_authority_core_v1(p_source_route text, p_contract_id uuid, p_client_id uuid, p_evidence_date date DEFAULT NULL::date)
 RETURNS TABLE(route_eligible boolean, validation_eligible boolean, import_authoritative boolean, authority_mode text, authority_basis text, effective_is_nhsp boolean, effective_autoprocess_hr boolean, effective_requires_hr boolean, effective_no_timesheet_required boolean, settings_as_of_date date, client_settings_id uuid, client_settings_effective_from date, client_settings_updated_at timestamp with time zone, contract_updated_at timestamp with time zone, authority_fingerprint text)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
  with context as (
    select upper(btrim(coalesce(p_source_route,''))) route,
      (statement_timestamp() at time zone 'Europe/London')::date today_london
  ), contract_row as (
    select c.* from public.contracts c where c.id=p_contract_id
  ), current_setting as (
    select cs.*
    from public.client_settings cs,context x
    where cs.client_id=coalesce((select c.client_id from contract_row c),p_client_id)
      and (cs.effective_from is null or cs.effective_from<=x.today_london)
    order by cs.effective_from desc nulls last,cs.updated_at desc nulls last,cs.id desc
    limit 1
  ), effective as (
    select x.route,x.today_london,c.id contract_id,c.updated_at contract_updated_at,
      cs.id client_settings_id,cs.effective_from client_settings_effective_from,
      cs.updated_at client_settings_updated_at,
      case when coalesce(c.overrideclientsettings,false) and c.is_nhsp is not null then c.is_nhsp
        else coalesce(cs.is_nhsp,false) end is_nhsp,
      case when coalesce(c.overrideclientsettings,false) and c.autoprocess_hr is not null then c.autoprocess_hr
        else coalesce(cs.autoprocess_hr,false) end autoprocess_hr,
      case when coalesce(c.overrideclientsettings,false) and c.requires_hr is not null then c.requires_hr
        else coalesce(cs.requires_hr,false) end requires_hr,
      case when coalesce(c.overrideclientsettings,false) and c.no_timesheet_required is not null then c.no_timesheet_required
        else coalesce(cs.no_timesheet_required,false) end no_timesheet_required,
      case when coalesce(c.overrideclientsettings,false) and (
        c.is_nhsp is not null or c.autoprocess_hr is not null or c.requires_hr is not null
        or c.no_timesheet_required is not null) then 'CONTRACT_OVERRIDE' else 'CLIENT_SETTINGS_CURRENT' end basis
    from context x
    left join contract_row c on true
    left join current_setting cs on true
  ), decision as (
    select e.*,
      case when e.route='NHSP' then e.is_nhsp
        when e.route in ('HR_WEEKLY','HEALTHROSTER','HEALTHROSTER_WEEKLY','HR_DAILY','HEALTHROSTER_DAILY')
          then e.autoprocess_hr else false end route_ok,
      case when e.route in ('HR_DAILY','HEALTHROSTER_DAILY') then e.autoprocess_hr
        when e.route in ('HR_WEEKLY','HEALTHROSTER','HEALTHROSTER_WEEKLY')
          then e.autoprocess_hr and not e.no_timesheet_required
        else false end validation_ok,
      case when e.route='NHSP' then e.is_nhsp
        when e.route in ('HR_WEEKLY','HEALTHROSTER','HEALTHROSTER_WEEKLY')
          then e.autoprocess_hr and e.no_timesheet_required
        else false end authoritative
    from effective e
  )
  select d.route_ok,d.validation_ok,d.authoritative,
    case when not d.route_ok then 'OUT_OF_SCOPE'
      when d.authoritative then 'AUTHORITATIVE'
      when d.validation_ok then 'VALIDATION_ONLY' else 'OUT_OF_SCOPE' end,
    d.basis,d.is_nhsp,d.autoprocess_hr,d.requires_hr,d.no_timesheet_required,
    d.today_london,d.client_settings_id,d.client_settings_effective_from,d.client_settings_updated_at,
    d.contract_updated_at,
    public._import_review_hash_v1(concat_ws('|','import-authority-v1',d.route,p_contract_id,
      coalesce((select c.client_id from contract_row c),p_client_id),p_evidence_date,d.today_london,
      d.client_settings_id,d.client_settings_effective_from,d.client_settings_updated_at,
      d.contract_updated_at,d.is_nhsp,d.autoprocess_hr,d.requires_hr,d.no_timesheet_required,
      d.route_ok,d.validation_ok,d.authoritative,d.basis))
  from decision d
$function$;

-- _import_review_effective_invoice_balance_core_v1(uuid,jsonb,integer,integer,integer,integer)
CREATE OR REPLACE FUNCTION public._import_review_effective_invoice_balance_core_v1(p_import_id uuid, p_source_items jsonb, p_max_sources integer DEFAULT 100, p_max_invoice_lines_per_source integer DEFAULT 512, p_max_audit_rows_per_source integer DEFAULT 256, p_max_operations_per_source integer DEFAULT 128)
 RETURNS TABLE(source_identity text, balance_json jsonb)
 LANGUAGE plpgsql
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
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
  v_issued_invoice_ids uuid[]:=array[]::uuid[];
  v_issued_line_ids uuid[]:=array[]::uuid[];
  v_pending_invoice_ids uuid[]:=array[]::uuid[];
  v_pending_line_ids uuid[]:=array[]::uuid[];
  v_credit_line_ids uuid[]:=array[]::uuid[];
  v_line_count integer:=0;
  v_audit_count integer:=0;
  v_operation_count integer:=0;
  v_operation_evidence jsonb:='[]'::jsonb;
  v_operation_member_ids uuid[]:=array[]::uuid[];
  v_member_supersession_map jsonb:='[]'::jsonb;
  v_member_supersession_conflict boolean:=false;
  v_operation_evidence_conflict boolean:=false;
  v_operation_in_progress boolean:=false;
  v_transition_operation_id uuid:=case
    when coalesce(current_setting('cloudtms.import_reconciliation_operation_id',true),'')
      ~*'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$'
      then current_setting('cloudtms.import_reconciliation_operation_id',true)::uuid
    else null end;
  v_member_role_map jsonb:='[]'::jsonb;
  v_member_role_conflict boolean:=false;
  v_effective_component_count integer:=0;
  v_issued_component_count integer:=0;
  v_pending_component_count integer:=0;
  v_b_day numeric:=0;
  v_b_night numeric:=0;
  v_b_sat numeric:=0;
  v_b_sun numeric:=0;
  v_b_bh numeric:=0;
  v_b_pay numeric:=0;
  v_b_charge numeric:=0;
  v_b_margin numeric:=0;
  v_issued_day numeric:=0;
  v_issued_night numeric:=0;
  v_issued_sat numeric:=0;
  v_issued_sun numeric:=0;
  v_issued_bh numeric:=0;
  v_issued_pay numeric:=0;
  v_issued_charge numeric:=0;
  v_issued_margin numeric:=0;
  v_pending_day numeric:=0;
  v_pending_night numeric:=0;
  v_pending_sat numeric:=0;
  v_pending_sun numeric:=0;
  v_pending_bh numeric:=0;
  v_pending_pay numeric:=0;
  v_pending_charge numeric:=0;
  v_pending_margin numeric:=0;
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
  v_schedule_candidates jsonb:='[]'::jsonb;
  v_candidate_policy_fingerprint text;
  v_b_policy_fingerprint text;
  v_terminal_generation_id text;
  v_terminal_positive_timesheet_id uuid;
  v_terminal_positive_member_count integer:=0;
  v_terminal_frozen_candidate jsonb;
  v_terminal_frozen_candidate_count integer:=0;
  v_terminal_frozen_schedule_variant_count integer:=0;
  v_terminal_frozen_policy_variant_count integer:=0;
  v_terminal_operation jsonb;
  v_terminal_operation_schedule jsonb:='[]'::jsonb;
  v_terminal_operation_hours jsonb:='{}'::jsonb;
  v_terminal_operation_policy_fingerprint text;
  v_terminal_operation_id uuid;
  v_terminal_policy_snapshot jsonb:='{}'::jsonb;
  v_terminal_policy_snapshot_fingerprint text;
  v_terminal_derived_day numeric:=0;
  v_terminal_derived_night numeric:=0;
  v_terminal_derived_sat numeric:=0;
  v_terminal_derived_sun numeric:=0;
  v_terminal_derived_bh numeric:=0;
  v_terminal_derived_total numeric:=0;
  v_terminal_frozen_matches_b boolean:=false;
  v_terminal_operation_matches_b boolean:=false;
  v_terminal_schedule_authority_conflict boolean:=false;
  v_terminal_policy_authority_conflict boolean:=false;
  v_b_standard_schedule_authority text:='NONE';
  v_b_standard_schedule_authority_timesheet_id uuid;
  v_b_standard_schedule_authority_correction_id text;
  v_b_standard_schedule_authority_operation_id uuid;
  v_b_standard_schedule_authority_policy_fingerprint text;
  v_b_standard_schedule_authority_fingerprint text;
  v_b_standard_schedule_authority_diagnostic text;
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
  v_original_invoice public.invoices%rowtype;
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
  v_component_economic_state text;
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
  v_financial_position_requires_amendment boolean:=false;
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
        outcome_match.action_outcome,outcome_match.outcome_count,
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
      left join lateral (
        select min(to_jsonb(outcome)::text)::jsonb action_outcome,count(*)::integer outcome_count
        from public.import_review_action_outcomes outcome
        where outcome.operation_id=mr.operation_id
          and outcome.action_id=mr.request_unit->>'action_id'
      ) outcome_match on true
    ), evaluated as (
      select t.*,
        case when t.operation_state='COMPLETE'
          and t.committed_at_utc is not null and t.finalised_at_utc is not null
          and t.request_unit->>'route' in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT')
          and t.request_count=1 and t.applied_count=1 and t.policy_count=1 and t.outcome_count=1
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
          and t.request_unit->>'invoice_stream'=v_invoice_stream
          and t.request_unit->>'source_scope_fingerprint'=v_scope_fingerprint
          and t.action_outcome->>'action_kind'='APPLY_AMENDMENT'
          and t.action_outcome->>'source_identity'=v_source_identity
          and t.action_outcome->>'shift_id'=v_source_shift_id::text
          and t.action_outcome->>'candidate_id'=v_candidate_id::text
          and t.action_outcome->>'client_id'=v_client_id::text
          and t.action_outcome->>'contract_id'=v_contract_id::text
          and nullif(t.action_outcome->>'evidence_fingerprint','') is not null
          and nullif(t.request_unit->>'unit_fingerprint','') is not null
          and t.request_unit->>'unit_fingerprint'=encode(digest(convert_to(concat_ws('|','unit-v2',
            t.request_unit->>'action_id',t.request_unit->>'source_identity',t.request_unit->>'source_shift_id',
            t.request_unit->>'route',t.request_unit->>'reconciliation_mode',
            t.request_unit->>'reconciliation_fingerprint',t.request_unit->>'review_policy_basis_kind',
            t.request_unit->>'review_policy_basis_fingerprint',t.action_outcome->>'evidence_fingerprint'),'UTF8'),'sha256'),'hex')
          and t.applied_unit->>'reviewed_unit_fingerprint'=t.request_unit->>'unit_fingerprint'
          and t.applied_unit->>'reconciliation_fingerprint'=t.request_unit->>'reconciliation_fingerprint'
          and case
            when t.request_unit->>'route'='CREATE_REVERSAL_REPLACEMENT' then
              coalesce(t.request_unit->>'repair_identity_mode','') in ('','CREATE_NEW_GENERATION')
              and t.applied_unit->>'repair_identity_mode'='CREATE_NEW_GENERATION'
              and nullif(t.request_unit->>'reviewed_existing_correction_id','') is null
            when t.request_unit->>'route'='AMEND_EXISTING_REPLACEMENT' then
              t.request_unit->>'repair_identity_mode' in ('RETAIN_EXISTING_CORRECTION_ID','FRESH_CORRECTION_ID_ARCHIVED_ROLE_IGNORED')
              and t.applied_unit->>'repair_identity_mode'=t.request_unit->>'repair_identity_mode'
              and nullif(t.request_unit->>'reviewed_existing_correction_id','') is not null
              and case
                when t.request_unit->>'repair_identity_mode'='RETAIN_EXISTING_CORRECTION_ID' then
                  t.applied_unit->>'correction_id'=t.request_unit->>'reviewed_existing_correction_id'
                else
                  t.request_unit->>'reviewed_existing_correction_id'<>t.applied_unit->>'correction_id'
                  and jsonb_typeof(t.request_unit->'reviewed_existing_member_ids')='array'
                  and jsonb_array_length(t.request_unit->'reviewed_existing_member_ids') between 1 and 2
                  and exists(select 1 from jsonb_array_elements_text(t.request_unit->'reviewed_existing_member_ids') reviewed(member_id)
                    where reviewed.member_id in (t.applied_unit->>'reversal_timesheet_id',t.applied_unit->>'replacement_timesheet_id'))
              end
            else false
          end
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
        'invoice_stream',e.request_unit->>'invoice_stream',
        'source_scope_fingerprint',e.request_unit->>'source_scope_fingerprint',
        'reviewed_existing_correction_id',e.request_unit->>'reviewed_existing_correction_id',
        'reviewed_existing_member_ids',coalesce(e.request_unit->'reviewed_existing_member_ids','[]'::jsonb),
        'request_repair_identity_mode',e.request_unit->>'repair_identity_mode',
        'repair_identity_mode',e.applied_unit->>'repair_identity_mode',
        'reviewed_unit_fingerprint',e.request_unit->>'unit_fingerprint',
        'action_evidence_fingerprint',e.action_outcome->>'evidence_fingerprint',
        'reconciliation_fingerprint',e.request_unit->>'reconciliation_fingerprint',
        'B_standard_schedule_json',coalesce(e.request_unit->'B_standard_schedule_json','[]'::jsonb),
        'B_hours',coalesce(e.request_unit->'B_hours','{}'::jsonb),
        'A_schedule_json',coalesce(e.request_unit->'A_schedule_json','[]'::jsonb),
        'A_hours',coalesce(e.request_unit->'A_hours','{}'::jsonb),
        'policy_envelope',e.policy_unit->'policy_envelope',
        'policy_envelope_fingerprint',e.policy_unit->>'policy_envelope_fingerprint'
      ) order by e.finalised_at_utc,e.operation_id,e.request_unit->>'action_id')
        filter(where e.valid_historical_authority),'[]'::jsonb),
      coalesce(bool_or(e.operation_state='COMPLETE'
        and e.request_unit->>'route' in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT')
        and not e.valid_historical_authority),false),
      coalesce(bool_or(e.operation_state in ('SOURCE_COMMITTED_TSFIN_PENDING','FINANCIALISED_PENDING_FINALISATION')
        and e.committed_at_utc is not null
        and e.operation_id is distinct from v_transition_operation_id),false)
    into v_operation_evidence,v_operation_evidence_conflict,v_operation_in_progress
    from evaluated e;

    -- A surviving member can be re-keyed more than once as successive archived
    -- siblings occupy prior correction identities.  Canonicalise the complete
    -- bounded chain (C1 -> C2 -> C3), retain every earlier assignment as audit
    -- history, and fail closed on cycles or branches.
    with recursive direct_edges as (
      select role.member_id::uuid member_timesheet_id,role.correction_kind,
        unit->>'reviewed_existing_correction_id' superseded_correction_id,
        unit->>'correction_id' canonical_correction_id,
        array_agg(distinct (unit->>'operation_id')::uuid order by (unit->>'operation_id')::uuid) operation_ids
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
          join public.invoices historical_invoice on historical_invoice.id=historical_line.invoice_id
          where (historical_line.timesheet_id=role.member_id::uuid
              or historical_line.meta_json->>'timesheet_id'=role.member_id)
            and historical_line.created_at<=coalesce((unit->>'evidence_at')::timestamptz,'infinity'::timestamptz)
            and upper(coalesce(historical_line.meta_json->>'line_type',''))
              !~ '^(EXPENSE(_.*)?|MILEAGE|TRAVEL|ACCOMMODATION|REIMBURSEMENT|ADDITION)$'
            and (
              historical_invoice.status::text='DRAFT'
              or historical_invoice.issued_at_utc is null
              or historical_invoice.active_document_operation_id is not null
              or historical_invoice.active_issue_operation_id is not null
              or upper(coalesce(historical_invoice.issue_state,'')) not in ('','IDLE','COMPLETE','COMPLETED','ISSUED')
              or (historical_invoice.status::text in ('ISSUED','PAID','ON_HOLD')
                and historical_invoice.issued_at_utc is not null)
            ))
      group by role.member_id,role.correction_kind,
        unit->>'reviewed_existing_correction_id',unit->>'correction_id'
    ), walk as (
      select edge.member_timesheet_id,edge.correction_kind,
        edge.superseded_correction_id root_correction_id,
        edge.canonical_correction_id reached_correction_id,
        array[edge.superseded_correction_id,edge.canonical_correction_id]::text[] path,
        edge.operation_ids,1 depth,
        edge.canonical_correction_id=edge.superseded_correction_id cycle
      from direct_edges edge
      union all
      select walk.member_timesheet_id,walk.correction_kind,walk.root_correction_id,
        edge.canonical_correction_id,walk.path||edge.canonical_correction_id,
        walk.operation_ids||edge.operation_ids,walk.depth+1,
        edge.canonical_correction_id=any(walk.path)
      from walk
      join direct_edges edge
        on edge.member_timesheet_id=walk.member_timesheet_id
       and edge.correction_kind=walk.correction_kind
       and edge.superseded_correction_id=walk.reached_correction_id
      where not walk.cycle and walk.depth<p_max_operations_per_source
    ), closure as (
      select distinct on (member_timesheet_id,correction_kind,root_correction_id,reached_correction_id)
        member_timesheet_id,correction_kind,root_correction_id,reached_correction_id,
        operation_ids,depth,path
      from walk
      where not cycle and root_correction_id<>reached_correction_id
      order by member_timesheet_id,correction_kind,root_correction_id,reached_correction_id,depth desc,operation_ids::text
    ), conflicts as (
      select exists(
        select 1 from direct_edges edge
        group by edge.member_timesheet_id,edge.correction_kind,edge.superseded_correction_id
        having count(distinct edge.canonical_correction_id)<>1
      )
      or exists(select 1 from walk where cycle)
      or exists(
        select 1 from walk
        where depth=p_max_operations_per_source
          and exists(select 1 from direct_edges edge
            where edge.member_timesheet_id=walk.member_timesheet_id
              and edge.correction_kind=walk.correction_kind
              and edge.superseded_correction_id=walk.reached_correction_id)
      )
      or exists(
        select 1
        from walk terminal
        where not terminal.cycle and not exists(select 1 from direct_edges edge
          where edge.member_timesheet_id=terminal.member_timesheet_id
            and edge.correction_kind=terminal.correction_kind
            and edge.superseded_correction_id=terminal.reached_correction_id)
        group by terminal.member_timesheet_id,terminal.correction_kind,terminal.root_correction_id
        having count(distinct terminal.reached_correction_id)<>1
      ) as has_conflict
    )
    select coalesce((select jsonb_agg(jsonb_build_object(
        'operation_id',entry.operation_ids[cardinality(entry.operation_ids)],
        'operation_ids',to_jsonb(entry.operation_ids),
        'member_timesheet_id',entry.member_timesheet_id,
        'correction_kind',entry.correction_kind,
        'superseded_correction_id',entry.root_correction_id,
        'canonical_correction_id',entry.reached_correction_id,
        'supersession_depth',entry.depth,
        'supersession_path',to_jsonb(entry.path)
      ) order by entry.member_timesheet_id,entry.correction_kind,
        entry.root_correction_id,entry.depth,entry.reached_correction_id)
      from closure entry),'[]'::jsonb),
      coalesce((select has_conflict from conflicts),false)
    into v_member_supersession_map,v_member_supersession_conflict;
    v_operation_evidence_conflict:=v_operation_evidence_conflict or v_member_supersession_conflict;

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
    v_issued_invoice_ids:=array[]::uuid[];
    v_issued_line_ids:=array[]::uuid[];
    v_pending_invoice_ids:=array[]::uuid[];
    v_pending_line_ids:=array[]::uuid[];
    v_credit_line_ids:=array[]::uuid[];
    v_effective_component_count:=0;
    v_issued_component_count:=0;
    v_pending_component_count:=0;
    v_b_day:=0; v_b_night:=0; v_b_sat:=0; v_b_sun:=0; v_b_bh:=0;
    v_b_pay:=0; v_b_charge:=0; v_b_margin:=0;
    v_issued_day:=0; v_issued_night:=0; v_issued_sat:=0; v_issued_sun:=0; v_issued_bh:=0;
    v_issued_pay:=0; v_issued_charge:=0; v_issued_margin:=0;
    v_pending_day:=0; v_pending_night:=0; v_pending_sat:=0; v_pending_sun:=0; v_pending_bh:=0;
    v_pending_pay:=0; v_pending_charge:=0; v_pending_margin:=0;
    v_b_schedule:='[]'::jsonb; v_candidate_schedule:='[]'::jsonb; v_candidate_hours:='{}'::jsonb;
    v_schedule_candidates:='[]'::jsonb; v_candidate_policy_fingerprint:=null; v_b_policy_fingerprint:=null;
    v_terminal_generation_id:=null; v_terminal_positive_timesheet_id:=null; v_terminal_positive_member_count:=0;
    v_terminal_frozen_candidate:=null; v_terminal_frozen_candidate_count:=0;
    v_terminal_frozen_schedule_variant_count:=0; v_terminal_frozen_policy_variant_count:=0;
    v_terminal_operation:=null; v_terminal_operation_schedule:='[]'::jsonb; v_terminal_operation_hours:='{}'::jsonb;
    v_terminal_operation_policy_fingerprint:=null; v_terminal_operation_id:=null;
    v_terminal_policy_snapshot:='{}'::jsonb; v_terminal_policy_snapshot_fingerprint:=null;
    v_terminal_derived_day:=0; v_terminal_derived_night:=0; v_terminal_derived_sat:=0;
    v_terminal_derived_sun:=0; v_terminal_derived_bh:=0; v_terminal_derived_total:=0;
    v_terminal_frozen_matches_b:=false; v_terminal_operation_matches_b:=false;
    v_terminal_schedule_authority_conflict:=false; v_terminal_policy_authority_conflict:=false;
    v_b_standard_schedule_authority:='NONE'; v_b_standard_schedule_authority_timesheet_id:=null;
    v_b_standard_schedule_authority_correction_id:=null; v_b_standard_schedule_authority_operation_id:=null;
    v_b_standard_schedule_authority_policy_fingerprint:=null; v_b_standard_schedule_authority_fingerprint:=null;
    v_b_standard_schedule_authority_diagnostic:=null;
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
    v_financial_position_requires_amendment:=false;

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
        i.client_id invoice_client_id,i.original_invoice_id,
        i.active_document_operation_id,i.active_issue_operation_id,i.issue_state
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
      v_original_invoice:=null;
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
        select original_invoice.* into v_original_invoice
        from public.invoices original_invoice
        where original_invoice.id=v_original_line.invoice_id;
        -- A credit is admissible only when the header, original physical line,
        -- client and exact source identity all prove one lineage.  The line's
        -- signed values are never trusted in isolation.
        if v_original_invoice.id is null
          or v_line.original_invoice_id is distinct from v_original_line.invoice_id
          or v_line.meta_json->>'original_invoice_line_id' is distinct from v_original_line.id::text
          or v_line.invoice_client_id is distinct from v_original_invoice.client_id
          or not (
            coalesce(v_line.timesheet_id::text,
              case when coalesce(v_line.meta_json->>'timesheet_id','')~*v_uuid_re
                then v_line.meta_json->>'timesheet_id' end)
              is not distinct from
            coalesce(v_original_line.timesheet_id::text,
              case when coalesce(v_original_line.meta_json->>'timesheet_id','')~*v_uuid_re
                then v_original_line.meta_json->>'timesheet_id' end)
            or (
              exists(select 1 from jsonb_array_elements(v_member_role_map) credit_member
                where credit_member->>'timesheet_id'=coalesce(v_line.timesheet_id::text,v_line.meta_json->>'timesheet_id')
                  and credit_member->>'source_identity'=v_source_identity)
              and exists(select 1 from jsonb_array_elements(v_member_role_map) original_member
                where original_member->>'timesheet_id'=coalesce(v_original_line.timesheet_id::text,v_original_line.meta_json->>'timesheet_id')
                  and original_member->>'source_identity'=v_source_identity)
            )
          ) then
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

      if v_line.active_document_operation_id is not null or v_line.active_issue_operation_id is not null
         or upper(coalesce(v_line.issue_state,'')) not in ('','IDLE','NOT_STARTED','COMPLETE','COMPLETED','ISSUED') then
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

      v_component_economic_state:=case
        when v_line.invoice_status='DRAFT' or v_line.issued_at_utc is null then 'PENDING'
        else 'EFFECTIVE' end;

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
      if v_component_economic_state='PENDING' then
        v_pending_day:=v_pending_day+v_component_day; v_pending_night:=v_pending_night+v_component_night;
        v_pending_sat:=v_pending_sat+v_component_sat; v_pending_sun:=v_pending_sun+v_component_sun; v_pending_bh:=v_pending_bh+v_component_bh;
        v_pending_pay:=v_pending_pay+v_component_pay; v_pending_charge:=v_pending_charge+v_component_charge; v_pending_margin:=v_pending_margin+v_component_margin;
        v_pending_component_count:=v_pending_component_count+1;
        v_pending_invoice_ids:=array_append(v_pending_invoice_ids,v_line.invoice_id);
        v_pending_line_ids:=array_append(v_pending_line_ids,v_line.id);
      else
        v_issued_day:=v_issued_day+v_component_day; v_issued_night:=v_issued_night+v_component_night;
        v_issued_sat:=v_issued_sat+v_component_sat; v_issued_sun:=v_issued_sun+v_component_sun; v_issued_bh:=v_issued_bh+v_component_bh;
        v_issued_pay:=v_issued_pay+v_component_pay; v_issued_charge:=v_issued_charge+v_component_charge; v_issued_margin:=v_issued_margin+v_component_margin;
        v_issued_component_count:=v_issued_component_count+1;
        v_issued_invoice_ids:=array_append(v_issued_invoice_ids,v_line.invoice_id);
        v_issued_line_ids:=array_append(v_issued_line_ids,v_line.id);
      end if;
      if v_line.invoice_type='CREDIT_NOTE' then v_credit_line_ids:=array_append(v_credit_line_ids,v_line.id); end if;
      v_line_evidence:=v_line_evidence||jsonb_build_array(jsonb_build_object(
        'invoice_id',v_line.invoice_id,'invoice_line_id',v_line.id,'invoice_type',v_line.invoice_type,
        'economic_state',v_component_economic_state,'timesheet_id',v_component_timesheet_id,
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
        v_candidate_policy_fingerprint:=case
          when v_component_correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT') then
            case when jsonb_typeof(v_tf.policy_snapshot_json->'correction_financials_policy_envelope')='object'
                and nullif(coalesce(v_tf.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint',
                  v_tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}'),'') is not null
                and v_tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}'
                  =coalesce(v_tf.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint',
                    v_tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}')
                and encode(digest(convert_to(((v_tf.policy_snapshot_json->'correction_financials_policy_envelope')
                    -'envelope_fingerprint'::text)::text,'UTF8'),'sha256'),'hex')
                  =coalesce(v_tf.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint',
                    v_tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}')
              then coalesce(v_tf.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint',
                v_tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}') end
          else coalesce(v_tf.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint',
            v_tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}',
            encode(digest(convert_to(coalesce(v_tf.policy_snapshot_json,'{}'::jsonb)::text,'UTF8'),'sha256'),'hex')) end;
        v_schedule_candidates:=v_schedule_candidates||jsonb_build_array(jsonb_build_object(
          'timesheet_id',v_component_timesheet_id,'correction_id',v_component_correction_id,
          'correction_kind',v_component_correction_kind,'invoice_line_id',v_line.id,
          'schedule_json',v_candidate_schedule,'hours',v_candidate_hours,
          'policy_fingerprint',v_candidate_policy_fingerprint));
      end if;
    end loop;

    select coalesce(array_agg(distinct x order by x),array[]::uuid[]) into v_effective_invoice_ids from unnest(v_effective_invoice_ids) x;
    select coalesce(array_agg(distinct x order by x),array[]::uuid[]) into v_effective_line_ids from unnest(v_effective_line_ids) x;
    select coalesce(array_agg(distinct x order by x),array[]::uuid[]) into v_issued_invoice_ids from unnest(v_issued_invoice_ids) x;
    select coalesce(array_agg(distinct x order by x),array[]::uuid[]) into v_issued_line_ids from unnest(v_issued_line_ids) x;
    select coalesce(array_agg(distinct x order by x),array[]::uuid[]) into v_pending_invoice_ids from unnest(v_pending_invoice_ids) x;
    select coalesce(array_agg(distinct x order by x),array[]::uuid[]) into v_pending_line_ids from unnest(v_pending_line_ids) x;
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
            and component->>'economic_state' in ('EFFECTIVE','PENDING')) has_effective_history,
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
            and component->>'economic_state' in ('EFFECTIVE','PENDING')),0) net_day,
        coalesce((select sum(coalesce((component#>>'{hours,hours_night}')::numeric,0))
          from jsonb_array_elements(v_line_evidence) component
          where component->>'correction_id'=r.correction_id and component->>'correction_kind'=r.role
            and component->>'economic_state' in ('EFFECTIVE','PENDING')),0) net_night,
        coalesce((select sum(coalesce((component#>>'{hours,hours_sat}')::numeric,0))
          from jsonb_array_elements(v_line_evidence) component
          where component->>'correction_id'=r.correction_id and component->>'correction_kind'=r.role
            and component->>'economic_state' in ('EFFECTIVE','PENDING')),0) net_sat,
        coalesce((select sum(coalesce((component#>>'{hours,hours_sun}')::numeric,0))
          from jsonb_array_elements(v_line_evidence) component
          where component->>'correction_id'=r.correction_id and component->>'correction_kind'=r.role
            and component->>'economic_state' in ('EFFECTIVE','PENDING')),0) net_sun,
        coalesce((select sum(coalesce((component#>>'{hours,hours_bh}')::numeric,0))
          from jsonb_array_elements(v_line_evidence) component
          where component->>'correction_id'=r.correction_id and component->>'correction_kind'=r.role
            and component->>'economic_state' in ('EFFECTIVE','PENDING')),0) net_bh
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
      -- A fully issued generation legitimately retains TSFIN/invoice locks.
      -- Those locks are immutable history, not active invoice activity.  Keep
      -- blocking only while at least one expected role is not yet effective.
      exists(select 1 from generation_state g where g.pending_roles>0 and g.effective_roles<2),
      exists(select 1 from generation_state g where g.active_duplicate or g.economic_member_duplicate
        or (g.proven_roles<2 and (g.effective_roles>0 or g.pending_roles>0)))
    into v_generation_role_evidence,v_fully_invoiced_generation_ids,v_partial_generation_ids,v_mutable_generation_ids,
      v_mutable_correction_id,v_archived_history_roles,v_role_evidence_conflicts,v_role_partial_invoice_state,
      v_role_active_invoice_activity,v_role_scope_unprovable;

    v_partial_invoice_state:=v_role_partial_invoice_state;
    -- Stable DRAFT lines are planned economics, not active invoice work.  Only
    -- a real document/issue operation or an import operation still in progress
    -- blocks review.  One-sided placement is reported separately below.
    v_active_invoice_activity:=v_active_invoice_activity or v_operation_in_progress;
    v_scope_unprovable:=v_scope_unprovable or v_role_scope_unprovable
      or v_operation_evidence_conflict or v_member_role_conflict;

    if v_mutable_correction_id is not null then
      v_repair_identity_mode:=case when exists(select 1 from public.timesheets archived
        where archived.correction_id=v_mutable_correction_id and archived.archived_at_utc is not null
          and archived.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT'))
        then 'FRESH_CORRECTION_ID_ARCHIVED_ROLE_IGNORED' else 'RETAIN_EXISTING_CORRECTION_ID' end;
    end if;
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

    -- A schedule is valid only when it belongs to the exact terminal positive
    -- member.  An older positive may have the same buckets while representing
    -- different work times, so mere candidate presence or bucket equality is
    -- never provenance authority.
    if cardinality(v_fully_invoiced_generation_ids)>0 then
      v_terminal_generation_id:=v_fully_invoiced_generation_ids[cardinality(v_fully_invoiced_generation_ids)];
      select count(distinct (member->>'timesheet_id')::uuid),
        (array_agg(distinct (member->>'timesheet_id')::uuid order by (member->>'timesheet_id')::uuid))[1]
      into v_terminal_positive_member_count,v_terminal_positive_timesheet_id
      from jsonb_array_elements(v_member_role_map) member
      where member->>'correction_id'=v_terminal_generation_id
        and member->>'correction_kind'='CHANGED_HOURS_REPLACEMENT';
      if v_terminal_positive_member_count<>1 then
        v_scope_unprovable:=true;
        v_b_standard_schedule_authority_diagnostic:='TERMINAL_POSITIVE_MEMBER_UNPROVABLE';
      end if;
    elsif (v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh)>0 then
      v_terminal_positive_timesheet_id:=v_source_timesheet_id;
      v_terminal_positive_member_count:=case when v_source_timesheet_id is null then 0 else 1 end;
    end if;

    if v_terminal_positive_timesheet_id is not null then
      select count(*)::integer,
        count(distinct candidate->'schedule_json'::text)::integer,
        count(distinct coalesce(candidate->>'policy_fingerprint','<NULL>'))::integer
      into v_terminal_frozen_candidate_count,v_terminal_frozen_schedule_variant_count,
        v_terminal_frozen_policy_variant_count
      from jsonb_array_elements(v_schedule_candidates) candidate
      where candidate->>'timesheet_id'=v_terminal_positive_timesheet_id::text
        and (v_terminal_generation_id is null or (
          candidate->>'correction_id'=v_terminal_generation_id
          and candidate->>'correction_kind'='CHANGED_HOURS_REPLACEMENT'));

      select candidate
      into v_terminal_frozen_candidate
      from jsonb_array_elements(v_schedule_candidates) candidate
      where candidate->>'timesheet_id'=v_terminal_positive_timesheet_id::text
        and (v_terminal_generation_id is null or (
          candidate->>'correction_id'=v_terminal_generation_id
          and candidate->>'correction_kind'='CHANGED_HOURS_REPLACEMENT'))
      order by candidate->>'invoice_line_id'
      limit 1;
    end if;

    if v_terminal_generation_id is not null and v_terminal_positive_timesheet_id is not null then
      select unit
      into v_terminal_operation
      from jsonb_array_elements(v_operation_evidence) unit
      where unit->>'correction_id'=v_terminal_generation_id
        and unit->>'replacement_timesheet_id'=v_terminal_positive_timesheet_id::text
      order by (unit->>'evidence_at')::timestamptz desc,unit->>'operation_id' desc
      limit 1;
    end if;

    v_candidate_schedule:=coalesce(v_terminal_frozen_candidate->'schedule_json','[]'::jsonb);
    v_candidate_hours:=coalesce(v_terminal_frozen_candidate->'hours','{}'::jsonb);
    v_candidate_policy_fingerprint:=nullif(v_terminal_frozen_candidate->>'policy_fingerprint','');
    v_terminal_operation_schedule:=coalesce(v_terminal_operation->'A_schedule_json','[]'::jsonb);
    v_terminal_operation_hours:=coalesce(v_terminal_operation->'A_hours','{}'::jsonb);
    v_terminal_operation_policy_fingerprint:=nullif(v_terminal_operation->>'policy_envelope_fingerprint','');
    v_terminal_operation_id:=case when coalesce(v_terminal_operation->>'operation_id','')~*v_uuid_re
      then (v_terminal_operation->>'operation_id')::uuid end;

    -- Historical request envelopes created before bucketed A-hours were
    -- populated can legitimately contain total_hours with zeroed buckets.
    -- Recover the buckets only from the exact terminal replacement member:
    -- its validated completed-operation schedule plus its matching frozen
    -- correction policy.  Never borrow a surviving older positive schedule.
    if v_terminal_positive_timesheet_id is not null and v_terminal_operation is not null then
      select tf.policy_snapshot_json,
        coalesce(tf.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint',
          tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}')
      into v_terminal_policy_snapshot,v_terminal_policy_snapshot_fingerprint
      from public.timesheets_financials tf
      where tf.timesheet_id=v_terminal_positive_timesheet_id
        and tf.is_current
        and jsonb_typeof(tf.policy_snapshot_json->'correction_financials_policy_envelope')='object'
        and coalesce(tf.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint',
          tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}')
          =v_terminal_operation_policy_fingerprint
        and tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}'
          =v_terminal_operation_policy_fingerprint
        -- jsonb considers numerically equivalent values (for example 1 and
        -- 1.0) equal even though their text encodings hash differently.  The
        -- completed operation envelope was already independently re-attested;
        -- require the terminal TSFIN copy to be semantically identical and to
        -- carry that exact validated fingerprint.
        and tf.policy_snapshot_json->'correction_financials_policy_envelope'
          =v_terminal_operation->'policy_envelope'
      order by tf.computed_at_utc desc nulls last,tf.id desc
      limit 1;

      if v_terminal_policy_snapshot_fingerprint=v_terminal_operation_policy_fingerprint
        and jsonb_typeof(v_terminal_operation_schedule)='array'
        and jsonb_array_length(v_terminal_operation_schedule)=1
        and nullif(coalesce((v_terminal_operation_schedule->0)->>'start_utc',
          (v_terminal_operation_schedule->0)->>'start'),'') is not null
        and nullif(coalesce((v_terminal_operation_schedule->0)->>'end_utc',
          (v_terminal_operation_schedule->0)->>'end'),'') is not null then
        begin
          select bucket.hours_day,bucket.hours_night,bucket.hours_sat,bucket.hours_sun,
            bucket.hours_bh,bucket.total_hours
          into v_terminal_derived_day,v_terminal_derived_night,v_terminal_derived_sat,
            v_terminal_derived_sun,v_terminal_derived_bh,v_terminal_derived_total
          from public._wkimp_bucket_hours_from_policy(
            v_terminal_policy_snapshot,
            coalesce((v_terminal_operation_schedule->0)->>'start_utc',
              (v_terminal_operation_schedule->0)->>'start')::timestamptz,
            coalesce((v_terminal_operation_schedule->0)->>'end_utc',
              (v_terminal_operation_schedule->0)->>'end')::timestamptz,
            coalesce(nullif(coalesce((v_terminal_operation_schedule->0)->>'break_mins',
              (v_terminal_operation_schedule->0)->>'break_minutes'),'')::integer,0)
          ) bucket;
          if v_terminal_derived_total=
            coalesce((v_terminal_operation_hours->>'total_hours')::numeric,v_terminal_derived_total) then
            v_terminal_operation_hours:=jsonb_build_object(
              'hours_day',v_terminal_derived_day,'hours_night',v_terminal_derived_night,
              'hours_sat',v_terminal_derived_sat,'hours_sun',v_terminal_derived_sun,
              'hours_bh',v_terminal_derived_bh,'total_hours',v_terminal_derived_total);
          end if;
        exception when others then
          v_b_standard_schedule_authority_diagnostic:='TERMINAL_OPERATION_SCHEDULE_BUCKET_DERIVATION_FAILED';
        end;
      end if;
    end if;

    v_terminal_frozen_matches_b:=v_terminal_frozen_candidate_count>0
      and jsonb_array_length(v_candidate_schedule)=1
      and coalesce((v_candidate_hours->>'hours_day')::numeric,0)=v_b_day
      and coalesce((v_candidate_hours->>'hours_night')::numeric,0)=v_b_night
      and coalesce((v_candidate_hours->>'hours_sat')::numeric,0)=v_b_sat
      and coalesce((v_candidate_hours->>'hours_sun')::numeric,0)=v_b_sun
      and coalesce((v_candidate_hours->>'hours_bh')::numeric,0)=v_b_bh;
    v_terminal_operation_matches_b:=v_terminal_operation is not null
      and jsonb_typeof(v_terminal_operation_schedule)='array'
      and jsonb_array_length(v_terminal_operation_schedule)=1
      and coalesce((v_terminal_operation_hours->>'hours_day')::numeric,0)=v_b_day
      and coalesce((v_terminal_operation_hours->>'hours_night')::numeric,0)=v_b_night
      and coalesce((v_terminal_operation_hours->>'hours_sat')::numeric,0)=v_b_sat
      and coalesce((v_terminal_operation_hours->>'hours_sun')::numeric,0)=v_b_sun
      and coalesce((v_terminal_operation_hours->>'hours_bh')::numeric,0)=v_b_bh;

    v_terminal_schedule_authority_conflict:=v_terminal_frozen_schedule_variant_count>1
      or (v_terminal_frozen_candidate_count>0 and v_terminal_operation is not null and (
        not v_terminal_frozen_matches_b or not v_terminal_operation_matches_b
        or jsonb_strip_nulls(jsonb_build_object(
          'date',coalesce((v_candidate_schedule->0)->>'date',(v_candidate_schedule->0)->>'work_date'),
          'start_utc',coalesce((v_candidate_schedule->0)->>'start_utc',(v_candidate_schedule->0)->>'start'),
          'end_utc',coalesce((v_candidate_schedule->0)->>'end_utc',(v_candidate_schedule->0)->>'end'),
          'break_mins',coalesce((v_candidate_schedule->0)->>'break_mins',(v_candidate_schedule->0)->>'break_minutes','0'),
          'shift_id',(v_candidate_schedule->0)->>'shift_id',
          'external_row_key',(v_candidate_schedule->0)->>'external_row_key'))
          is distinct from
        jsonb_strip_nulls(jsonb_build_object(
          'date',coalesce((v_terminal_operation_schedule->0)->>'date',(v_terminal_operation_schedule->0)->>'work_date'),
          'start_utc',coalesce((v_terminal_operation_schedule->0)->>'start_utc',(v_terminal_operation_schedule->0)->>'start'),
          'end_utc',coalesce((v_terminal_operation_schedule->0)->>'end_utc',(v_terminal_operation_schedule->0)->>'end'),
          'break_mins',coalesce((v_terminal_operation_schedule->0)->>'break_mins',(v_terminal_operation_schedule->0)->>'break_minutes','0'),
          'shift_id',(v_terminal_operation_schedule->0)->>'shift_id',
          'external_row_key',(v_terminal_operation_schedule->0)->>'external_row_key'))));
    v_terminal_policy_authority_conflict:=v_terminal_frozen_policy_variant_count>1
      or (v_terminal_frozen_candidate_count>0 and v_terminal_operation is not null
        and v_candidate_policy_fingerprint is distinct from v_terminal_operation_policy_fingerprint);

    if v_terminal_schedule_authority_conflict then
      v_scope_unprovable:=true;
      v_b_standard_schedule_authority_diagnostic:='TERMINAL_SCHEDULE_AUTHORITY_CONFLICT';
    elsif v_terminal_policy_authority_conflict then
      v_scope_unprovable:=true;
      v_b_standard_schedule_authority_diagnostic:='TERMINAL_POLICY_AUTHORITY_CONFLICT';
    elsif v_terminal_frozen_matches_b then
      if v_candidate_policy_fingerprint is null then
        v_scope_unprovable:=true;
        v_b_standard_schedule_authority_diagnostic:='TERMINAL_POLICY_AUTHORITY_MISSING';
      else
        v_b_schedule:=v_candidate_schedule;
        v_b_policy_fingerprint:=v_candidate_policy_fingerprint;
        v_b_standard_schedule_authority:=case when v_terminal_generation_id is null
          then 'ORIGINAL_SOURCE_FROZEN_SEGMENT' else 'TERMINAL_REPLACEMENT_FROZEN_SEGMENT' end;
        v_b_standard_schedule_authority_timesheet_id:=v_terminal_positive_timesheet_id;
        v_b_standard_schedule_authority_correction_id:=v_terminal_generation_id;
        v_b_standard_schedule_authority_operation_id:=v_terminal_operation_id;
        v_b_standard_schedule_authority_policy_fingerprint:=v_candidate_policy_fingerprint;
      end if;
    elsif v_terminal_operation_matches_b then
      if v_terminal_operation_policy_fingerprint is null then
        v_scope_unprovable:=true;
        v_b_standard_schedule_authority_diagnostic:='TERMINAL_POLICY_AUTHORITY_MISSING';
      else
        v_b_schedule:=v_terminal_operation_schedule;
        v_b_policy_fingerprint:=v_terminal_operation_policy_fingerprint;
        v_b_standard_schedule_authority:='TERMINAL_COMPLETED_OPERATION_A_SCHEDULE';
        v_b_standard_schedule_authority_timesheet_id:=v_terminal_positive_timesheet_id;
        v_b_standard_schedule_authority_correction_id:=v_terminal_generation_id;
        v_b_standard_schedule_authority_operation_id:=v_terminal_operation_id;
        v_b_standard_schedule_authority_policy_fingerprint:=v_terminal_operation_policy_fingerprint;
      end if;
    end if;

    v_b_hours_zero:=v_b_day=0 and v_b_night=0 and v_b_sat=0 and v_b_sun=0 and v_b_bh=0;
    v_b_money_zero:=round(v_b_pay,2)=0 and round(v_b_charge,2)=0 and round(v_b_margin,2)=0;
    if v_b_hours_zero and v_b_money_zero then
      v_b_schedule:='[]'::jsonb;
      v_b_policy_fingerprint:=null;
      v_b_standard_schedule_authority:='NONE';
      v_b_standard_schedule_authority_timesheet_id:=null;
      v_b_standard_schedule_authority_correction_id:=null;
      v_b_standard_schedule_authority_operation_id:=null;
      v_b_standard_schedule_authority_policy_fingerprint:=null;
    end if;
    v_b_standard_representable:=(v_b_hours_zero and v_b_money_zero)
      or (v_b_day>=0 and v_b_night>=0 and v_b_sat>=0 and v_b_sun>=0 and v_b_bh>=0
        and v_b_standard_schedule_authority<>'NONE' and jsonb_array_length(v_b_schedule)=1);
    v_b_standard_schedule_authority_fingerprint:=encode(digest(convert_to(jsonb_build_object(
      'contract','B-standard-schedule-authority-v1','source_scope_fingerprint',v_scope_fingerprint,
      'authority',v_b_standard_schedule_authority,'timesheet_id',v_b_standard_schedule_authority_timesheet_id,
      'correction_id',v_b_standard_schedule_authority_correction_id,
      'operation_id',v_b_standard_schedule_authority_operation_id,
      'schedule_json',v_b_schedule,
      'B_hours',jsonb_build_object('hours_day',v_b_day,'hours_night',v_b_night,'hours_sat',v_b_sat,
        'hours_sun',v_b_sun,'hours_bh',v_b_bh,'total_hours',v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh),
      'policy_fingerprint',v_b_standard_schedule_authority_policy_fingerprint)::text,'UTF8'),'sha256'),'hex');

    v_role_evidence_fingerprint:=encode(digest(convert_to(concat_ws('|','role-evidence-v4',
      v_operation_evidence::text,v_member_supersession_map::text,v_member_role_map::text,v_generation_role_evidence::text,
      v_archived_history_roles::text,v_role_evidence_conflicts::text,v_b_standard_schedule_authority_fingerprint),'UTF8'),'sha256'),'hex');
    v_effective_fingerprint:=encode(digest(convert_to(concat_ws('|','effective-invoice-v4',v_source_identity,
      v_line_evidence::text,v_ignored_nonhours_line_ids::text,v_role_evidence_fingerprint,
      v_b_day,v_b_night,v_b_sat,v_b_sun,v_b_bh,v_b_pay,v_b_charge,v_b_margin,
      v_issued_line_ids::text,v_pending_line_ids::text,
      v_issued_day,v_issued_night,v_issued_sat,v_issued_sun,v_issued_bh,v_issued_pay,v_issued_charge,v_issued_margin,
      v_pending_day,v_pending_night,v_pending_sat,v_pending_sun,v_pending_bh,v_pending_pay,v_pending_charge,v_pending_margin,
      v_b_standard_schedule_authority_fingerprint),'UTF8'),'sha256'),'hex');

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

    -- The authoritative decision is economic whenever an invoiced position or
    -- a mutable correction generation exists.  The live operational source row
    -- is not the financial authority in that case.  Compare every rate bucket,
    -- using B + M for an active mutable generation and B otherwise.
    v_financial_position_requires_amendment:=case
      when v_mutable_correction_id is not null then
        v_b_day+v_m_day is distinct from coalesce((v_a_hours->>'hours_day')::numeric,0)
        or v_b_night+v_m_night is distinct from coalesce((v_a_hours->>'hours_night')::numeric,0)
        or v_b_sat+v_m_sat is distinct from coalesce((v_a_hours->>'hours_sat')::numeric,0)
        or v_b_sun+v_m_sun is distinct from coalesce((v_a_hours->>'hours_sun')::numeric,0)
        or v_b_bh+v_m_bh is distinct from coalesce((v_a_hours->>'hours_bh')::numeric,0)
      when v_effective_component_count>0 and not v_effective_zero then
        v_b_day is distinct from coalesce((v_a_hours->>'hours_day')::numeric,0)
        or v_b_night is distinct from coalesce((v_a_hours->>'hours_night')::numeric,0)
        or v_b_sat is distinct from coalesce((v_a_hours->>'hours_sat')::numeric,0)
        or v_b_sun is distinct from coalesce((v_a_hours->>'hours_sun')::numeric,0)
        or v_b_bh is distinct from coalesce((v_a_hours->>'hours_bh')::numeric,0)
      else false
    end;

    v_blocking_code:=case
      when v_partial_invoice_state then 'IMPORT_REVIEW_CORRECTION_PAIR_PLACEMENT_INCOMPLETE'
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
    v_reconciliation_fingerprint:=encode(digest(convert_to(concat_ws('|','reconciliation-v4',v_scope_fingerprint,v_operation_ids::text,v_member_supersession_map::text,
      v_effective_fingerprint,v_mutable_fingerprint,v_a_fingerprint,v_blocking_code,v_b_policy_fingerprint,
      v_b_standard_schedule_authority,v_b_standard_schedule_authority_timesheet_id,
      v_b_standard_schedule_authority_correction_id,v_b_standard_schedule_authority_operation_id,
      v_b_standard_schedule_authority_policy_fingerprint,v_b_standard_schedule_authority_fingerprint,
      v_current_source_safe,v_current_source_safety_reason,v_current_source_invoice_lined,v_current_source_paid,
      v_current_source_unlocked,v_current_source_fresh,v_current_source_segment_unlocked,
      v_current_source_contract_week_safe,v_current_source_invoice_operation_clear,v_b_hours_zero,v_b_money_zero,
      v_financial_position_requires_amendment),'UTF8'),'sha256'),'hex');

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
      'issued_invoice_ids',to_jsonb(v_issued_invoice_ids),'issued_invoice_line_ids',to_jsonb(v_issued_line_ids),
      'issued_invoice_component_count',v_issued_component_count,
      'pending_invoice_ids',to_jsonb(v_pending_invoice_ids),'pending_invoice_line_ids',to_jsonb(v_pending_line_ids),
      'pending_invoice_component_count',v_pending_component_count,
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
      'B_issued_hours',jsonb_build_object('hours_day',v_issued_day,'hours_night',v_issued_night,'hours_sat',v_issued_sat,'hours_sun',v_issued_sun,'hours_bh',v_issued_bh,'total_hours',v_issued_day+v_issued_night+v_issued_sat+v_issued_sun+v_issued_bh),
      'P_pending_hours',jsonb_build_object('hours_day',v_pending_day,'hours_night',v_pending_night,'hours_sat',v_pending_sat,'hours_sun',v_pending_sun,'hours_bh',v_pending_bh,'total_hours',v_pending_day+v_pending_night+v_pending_sat+v_pending_sun+v_pending_bh),
      'planned_invoice_hours',jsonb_build_object('hours_day',v_b_day,'hours_night',v_b_night,'hours_sat',v_b_sat,'hours_sun',v_b_sun,'hours_bh',v_b_bh,'total_hours',v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh),
      'effective_hours_net_is_zero',(v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh)=0,
      'effective_money_net_is_zero',v_b_money_zero,
      'effective_position_net_is_zero',v_effective_zero,
      'effective_hours_net_is_positive',(v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh)>0,
      'effective_hours_net_is_negative',(v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh)<0,
      'B_financials',jsonb_build_object('pay_ex_vat',v_b_pay,'charge_ex_vat',v_b_charge,'margin_ex_vat',v_b_margin),
      'B_issued_financials',jsonb_build_object('pay_ex_vat',v_issued_pay,'charge_ex_vat',v_issued_charge,'margin_ex_vat',v_issued_margin),
      'P_pending_financials',jsonb_build_object('pay_ex_vat',v_pending_pay,'charge_ex_vat',v_pending_charge,'margin_ex_vat',v_pending_margin),
      'planned_invoice_financials',jsonb_build_object('pay_ex_vat',v_b_pay,'charge_ex_vat',v_b_charge,'margin_ex_vat',v_b_margin),
      'B_standard_schedule_json',v_b_schedule,'B_policy_fingerprint',v_b_policy_fingerprint,'B_standard_representable',v_b_standard_representable,
      'B_standard_schedule_authority',v_b_standard_schedule_authority,
      'B_standard_schedule_authority_timesheet_id',v_b_standard_schedule_authority_timesheet_id,
      'B_standard_schedule_authority_correction_id',v_b_standard_schedule_authority_correction_id,
      'B_standard_schedule_authority_operation_id',v_b_standard_schedule_authority_operation_id,
      'B_standard_schedule_authority_policy_fingerprint',v_b_standard_schedule_authority_policy_fingerprint,
      'B_standard_schedule_authority_fingerprint',v_b_standard_schedule_authority_fingerprint,
      'B_standard_schedule_authority_diagnostic',v_b_standard_schedule_authority_diagnostic,
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
      'financial_position_requires_amendment',v_financial_position_requires_amendment,
      'recommended_route_inputs',jsonb_build_object('B_positive',(v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh)>0,
        'has_mutable_generation',v_mutable_correction_id is not null,'source_timesheet_active',v_source_timesheet_id=any(v_active_ids),
        'current_source_safe_for_effective_zero_amendment',v_current_source_safe),
      'blocking_code',v_blocking_code,'reconciliation_fingerprint',v_reconciliation_fingerprint
    );
    return next;
  end loop;
end
$function$;

-- _import_review_events_immutable_guard_v1()
CREATE OR REPLACE FUNCTION public._import_review_events_immutable_guard_v1()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
begin raise exception 'IMPORT_REVIEW_EVENTS_ARE_APPEND_ONLY' using errcode='55000'; end
$function$;

-- _import_review_follow_up_reconcile_core_v1(uuid,uuid,uuid)
CREATE OR REPLACE FUNCTION public._import_review_follow_up_reconcile_core_v1(p_import_id uuid, p_operation_id uuid, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v public.import_review_states%rowtype; o public.import_apply_operations%rowtype;
  v_ts text; v_email text; v_aggregate text; v_error_code text; v_error_message text;
  v_ts_error_code text; v_ts_error_message text; v_email_error_code text; v_email_error_message text;
begin
  select * into v from public.import_review_states where import_id=p_import_id for update;
  select * into o from public.import_apply_operations where id=p_operation_id and import_id=p_import_id for update;
  if v.status not in ('IN_REVIEW','BLOCKED','READY','APPLIED')
    or v.last_operation_id is distinct from p_operation_id or o.id is null then
    raise exception 'IMPORT_REVIEW_FOLLOW_UP_RECONCILE_MISMATCH' using errcode='55000';
  end if;
  v_ts:=upper(coalesce(o.response_json->>'review_tsfin_follow_up_status','NOT_REQUIRED'));
  v_email:=upper(coalesce(o.response_json->>'review_email_follow_up_status','NOT_REQUIRED'));
  if v_ts not in ('PENDING','COMPLETE','FAILED_RETRYABLE','NOT_REQUIRED')
    or v_email not in ('PENDING','COMPLETE','FAILED_RETRYABLE','NOT_REQUIRED') then
    raise exception 'IMPORT_REVIEW_FOLLOW_UP_COMPONENT_STATE_INVALID' using errcode='23514';
  end if;
  v_ts_error_code:=nullif(o.response_json->>'review_tsfin_follow_up_error_code','');
  v_ts_error_message:=nullif(o.response_json->>'review_tsfin_follow_up_error_message','');
  v_email_error_code:=nullif(o.response_json->>'review_email_follow_up_error_code','');
  v_email_error_message:=nullif(o.response_json->>'review_email_follow_up_error_message','');
  v_aggregate:=case when v_ts='FAILED_RETRYABLE' or v_email='FAILED_RETRYABLE' then 'FAILED_RETRYABLE'
    when v_ts in ('COMPLETE','NOT_REQUIRED') and v_email in ('COMPLETE','NOT_REQUIRED')
      then case when v_ts='NOT_REQUIRED' and v_email='NOT_REQUIRED' then 'NOT_REQUIRED' else 'COMPLETE' end
    else 'PENDING' end;
  if v_aggregate='FAILED_RETRYABLE' then
    if v_email='FAILED_RETRYABLE' and v_ts='FAILED_RETRYABLE' then
      v_error_code:='MULTIPLE_FOLLOW_UP_COMPONENTS_FAILED';
      v_error_message:='Email and TSFIN follow-up require retry.';
    elsif v_email='FAILED_RETRYABLE' then
      v_error_code:=coalesce(v_email_error_code,'EMAIL_FOLLOW_UP_FAILED');
      v_error_message:=coalesce(v_email_error_message,'Query email follow-up failed and can be retried safely.');
    else
      v_error_code:=coalesce(v_ts_error_code,'TSFIN_FOLLOW_UP_FAILED');
      v_error_message:=coalesce(v_ts_error_message,'TSFIN follow-up failed and can be retried safely.');
    end if;
  end if;
  update public.import_review_states set follow_up_status=v_aggregate,
    follow_up_error_code=v_error_code,
    follow_up_error_message=v_error_message,
    state_version=state_version+case when follow_up_status is distinct from v_aggregate then 1 else 0 end,
    updated_at_utc=now(),updated_by_user_id=p_actor_user_id
  where import_id=p_import_id returning * into v;
  if v_aggregate in ('COMPLETE','NOT_REQUIRED') then
    update public.import_apply_operations set state='COMPLETE',finalised_at_utc=coalesce(finalised_at_utc,now()),updated_at_utc=now()
    where id=p_operation_id;
  end if;
  return jsonb_build_object('ok',true,'status',v.status,'follow_up_status',v.follow_up_status,
    'follow_up_error_code',v.follow_up_error_code,'follow_up_error_message',v.follow_up_error_message,
    'state_version',v.state_version,
    'tsfin_follow_up_status',v_ts,'tsfin_follow_up_error_code',v_ts_error_code,'tsfin_follow_up_error_message',v_ts_error_message,
    'email_follow_up_status',v_email,'email_follow_up_error_code',v_email_error_code,'email_follow_up_error_message',v_email_error_message);
end $function$;

-- _import_review_hash_v1(text)
CREATE OR REPLACE FUNCTION public._import_review_hash_v1(p_value text)
 RETURNS text
 LANGUAGE sql
 IMMUTABLE SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
  select encode(extensions.digest(convert_to(coalesce(p_value,''),'UTF8'),'sha256'::text),'hex')
$function$;

-- _import_review_html_escape_v1(text)
CREATE OR REPLACE FUNCTION public._import_review_html_escape_v1(p_value text)
 RETURNS text
 LANGUAGE sql
 IMMUTABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
  select replace(replace(replace(replace(replace(coalesce(p_value,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;')
$function$;

-- _import_review_immutable_guard_v1()
CREATE OR REPLACE FUNCTION public._import_review_immutable_guard_v1()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_locked timestamptz;
  v_import_id uuid;
  v_mode text;
  v_parent public.hr_imports%rowtype;
begin
  if tg_table_name='hr_imports' then
    if old.coverage_locked_at is not null and (
      new.coverage_mode is distinct from old.coverage_mode
      or new.coverage_start_date is distinct from old.coverage_start_date
      or new.coverage_end_date is distinct from old.coverage_end_date
      or new.coverage_fingerprint is distinct from old.coverage_fingerprint
      or new.coverage_locked_at is distinct from old.coverage_locked_at
      or new.coverage_operation_key is distinct from old.coverage_operation_key
      or new.coverage_request_hash is distinct from old.coverage_request_hash
      or new.source_file_sha256 is distinct from old.source_file_sha256
      or new.parser_version is distinct from old.parser_version
    ) then
      raise exception 'IMPORT_REVIEW_COVERAGE_IMMUTABLE' using errcode='55000';
    end if;
    if old.coverage_locked_at is not null and (
      new.revision_group_id is distinct from old.revision_group_id
      or new.revision_no is distinct from old.revision_no
      or new.supersedes_import_id is distinct from old.supersedes_import_id
    ) and (old.supersedes_import_id is not null or new.supersedes_import_id is null) then
      raise exception 'IMPORT_REVIEW_REVISION_IDENTITY_IMMUTABLE' using errcode='55000';
    end if;
    if new.supersedes_import_id is not null and (
      old.supersedes_import_id is distinct from new.supersedes_import_id
      or old.revision_group_id is distinct from new.revision_group_id
      or old.revision_no is distinct from new.revision_no
    ) then
      select p.* into v_parent from public.hr_imports p where p.id=new.supersedes_import_id;
      if not found or v_parent.id=new.id or v_parent.source_system is distinct from new.source_system
         or new.revision_group_id is distinct from coalesce(v_parent.revision_group_id,v_parent.id)
         or new.revision_no is null or new.revision_no<=coalesce(v_parent.revision_no,0) then
        raise exception 'IMPORT_REVIEW_SUPERSESSION_INCONSISTENT' using errcode='23514';
      end if;
    end if;
    if old.coverage_locked_at is null and new.coverage_locked_at is not null then
      if new.coverage_mode='COMPLETE_SELECTED_CANDIDATES' and not exists(
        select 1 from public.import_review_scope_candidates sc where sc.import_id=new.id) then
        raise exception 'IMPORT_REVIEW_SELECTED_CANDIDATE_SCOPE_REQUIRED' using errcode='23514';
      end if;
      if new.coverage_mode<>'COMPLETE_SELECTED_CANDIDATES' and exists(
        select 1 from public.import_review_scope_candidates sc where sc.import_id=new.id) then
        raise exception 'IMPORT_REVIEW_CANDIDATE_SCOPE_NOT_ALLOWED' using errcode='23514';
      end if;
    end if;
    return new;
  end if;
  if tg_op='DELETE' then v_import_id:=old.import_id; else v_import_id:=new.import_id; end if;
  if tg_table_name='import_review_scope_candidates' and tg_op<>'DELETE' then
    select hi.coverage_mode into v_mode from public.hr_imports hi where hi.id=new.import_id;
    if v_mode is distinct from 'COMPLETE_SELECTED_CANDIDATES' then
      raise exception 'IMPORT_REVIEW_CANDIDATE_SCOPE_NOT_ALLOWED' using errcode='23514';
    end if;
  end if;
  select hi.coverage_locked_at into v_locked from public.hr_imports hi where hi.id=v_import_id;
  if v_locked is null then if tg_op='DELETE' then return old; else return new; end if; end if;
  if tg_op in ('INSERT','DELETE') then raise exception 'IMPORT_REVIEW_SCOPE_IMMUTABLE' using errcode='55000'; end if;
  if tg_table_name='import_review_scope_clients' and (
    new.import_id is distinct from old.import_id or new.source_client_key is distinct from old.source_client_key
    or new.source_display_label is distinct from old.source_display_label) then
    raise exception 'IMPORT_REVIEW_SCOPE_IDENTITY_IMMUTABLE' using errcode='55000';
  end if;
  if tg_table_name='import_review_scope_candidates' and (
    new.import_id is distinct from old.import_id or new.source_candidate_key is distinct from old.source_candidate_key
    or new.source_display_label is distinct from old.source_display_label) then
    raise exception 'IMPORT_REVIEW_SCOPE_IDENTITY_IMMUTABLE' using errcode='55000';
  end if;
  return new;
end
$function$;

-- _import_review_overlap_preflight_core_v2(uuid,hr_source_enum,text,date,date,jsonb)
CREATE OR REPLACE FUNCTION public._import_review_overlap_preflight_core_v2(p_import_id uuid, p_source_system hr_source_enum, p_source_route text, p_coverage_start_date date, p_coverage_end_date date, p_scope_clients jsonb DEFAULT '[]'::jsonb)
 RETURNS jsonb
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
  with current_clients as (
    select nullif(client.value->>'client_id','')::uuid as client_id,
           nullif(btrim(client.value->>'source_client_key'),'') as source_client_key
    from jsonb_array_elements(case when jsonb_typeof(coalesce(p_scope_clients,'[]'::jsonb))='array'
      then coalesce(p_scope_clients,'[]'::jsonb) else '[]'::jsonb end) client(value)
  ), active_reviews as (
    select s.import_id,s.status,s.state_version,s.updated_at_utc,
           hi.filename,hi.coverage_start_date,hi.coverage_end_date,
           coalesce(hi.import_scope,hi.source_system::text) as source_route
    from public.import_review_states s
    join public.hr_imports hi on hi.id=s.import_id
    where s.import_id<>p_import_id
      and s.status in ('STAGED','IN_REVIEW','BLOCKED','READY','APPLYING')
      and hi.source_system=p_source_system
      and upper(coalesce(hi.import_scope,hi.source_system::text))=upper(coalesce(p_source_route,p_source_system::text))
      and daterange(hi.coverage_start_date,hi.coverage_end_date,'[]')
        && daterange(p_coverage_start_date,p_coverage_end_date,'[]')
  ), matched_healthroster as (
    select distinct on (active.import_id)
      active.*,
      case when current.client_id is not null and current.client_id=other_client.client_id
        then 'RESOLVED_CLIENT' else 'SOURCE_CLIENT_KEY' end as overlap_reason,
      coalesce(current.client_id,other_client.client_id) as client_id,
      coalesce(current.source_client_key,other_client.source_client_key) as source_client_key
    from active_reviews active
    join lateral (
      select sc.client_id,sc.source_client_key
      from public.import_review_scope_clients sc
      where sc.import_id=active.import_id
      union all
      select hi.client_id,'client:'||hi.client_id::text
      from public.hr_imports hi
      where hi.id=active.import_id and hi.client_id is not null
        and not exists(select 1 from public.import_review_scope_clients sc where sc.import_id=active.import_id)
    ) other_client on true
    join current_clients current on
      (current.client_id is not null and other_client.client_id=current.client_id)
      or (current.source_client_key is not null and other_client.source_client_key=current.source_client_key)
    where p_source_system<>'NHSP'::public.hr_source_enum
    order by active.import_id,
      case when current.client_id is not null and current.client_id=other_client.client_id then 0 else 1 end
  ), matched as (
    -- NHSP is a single cross-client feed, so any unfinished NHSP review for
    -- an overlapping period conflicts. HealthRoster feeds are client-owned
    -- and conflict only for the same client and the same Weekly/Daily route.
    select active.*,
      'NHSP_PERIOD'::text as overlap_reason,
      null::uuid as client_id,
      null::text as source_client_key
    from active_reviews active
    where p_source_system='NHSP'::public.hr_source_enum
    union all
    select * from matched_healthroster
  ), bounded as (
    select * from matched order by updated_at_utc desc,import_id desc limit 20
  )
  select coalesce(jsonb_agg(jsonb_build_object(
    'import_id',import_id,'status',status,'state_version',state_version,'filename',filename,
    'source_route',source_route,'coverage_start_date',coverage_start_date,'coverage_end_date',coverage_end_date,
    'overlap_reason',overlap_reason,'client_id',client_id,'source_client_key',source_client_key,
    'updated_at_utc',updated_at_utc
  ) order by updated_at_utc desc,import_id desc),'[]'::jsonb)
  from bounded
$function$;

-- _import_review_query_evidence_core_v1(uuid)
CREATE OR REPLACE FUNCTION public._import_review_query_evidence_core_v1(p_timesheet_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_ts public.timesheets%rowtype;
  v_doc public.invoice_document_versions%rowtype;
  v_asset public.invoice_document_assets%rowtype;
  v_protection jsonb;
  v_has_hours boolean:=false;
  v_source_complete boolean:=false;
  v_document_ready boolean:=false;
  v_is_signed_qr boolean:=false;
  v_reason_code text;
  v_fingerprint text;
begin
  if p_timesheet_id is null then
    return jsonb_build_object(
      'timesheet_id',null,'source_complete',false,'document_ready',false,
      'preparation_required',false,'reason_code','TIMESHEET_EVIDENCE_INCOMPLETE',
      'evidence_fingerprint',public._import_review_hash_v1('query-evidence-v1|null')
    );
  end if;

  select * into v_ts
  from public.timesheets t
  where t.timesheet_id=p_timesheet_id
  limit 1;

  if not found or not coalesce(v_ts.is_current,false) or v_ts.archived_at_utc is not null then
    return jsonb_build_object(
      'timesheet_id',p_timesheet_id,'source_complete',false,'document_ready',false,
      'preparation_required',false,'reason_code','TIMESHEET_EVIDENCE_INCOMPLETE',
      'evidence_fingerprint',public._import_review_hash_v1(concat_ws('|','query-evidence-v1',p_timesheet_id,'MISSING_CURRENT_SOURCE'))
    );
  end if;

  v_protection:=public._import_review_timesheet_protection_core_v1(p_timesheet_id);
  v_has_hours:=case
    when upper(coalesce(v_ts.sheet_scope::text,''))='WEEKLY' then
      jsonb_typeof(coalesce(v_ts.actual_schedule_json,'[]'::jsonb))='array'
      and jsonb_array_length(coalesce(v_ts.actual_schedule_json,'[]'::jsonb))>0
      and not exists (
        select 1 from jsonb_array_elements(coalesce(v_ts.actual_schedule_json,'[]'::jsonb)) day(value)
        where nullif(btrim(coalesce(day.value->>'start',day.value->>'start_time',day.value->>'worked_start','')),'') is null
           or nullif(btrim(coalesce(day.value->>'end',day.value->>'end_time',day.value->>'worked_end','')),'') is null
      )
    else v_ts.worked_start_iso is not null and v_ts.worked_end_iso is not null
  end;

  -- Any QR lifecycle marker makes this a QR timesheet.  It is complete only
  -- when the complete signed-QR contract below is satisfied; a pending or
  -- unsigned QR PDF must never fall through as an ordinary manual upload.
  v_is_signed_qr:=v_ts.qr_status is not null
    or v_ts.qr_scanned_at is not null
    or nullif(btrim(coalesce(v_ts.qr_signed_hash,'')),'') is not null
    or v_ts.qr_signed_at_utc is not null;

  if upper(coalesce(v_ts.submission_mode::text,''))='ELECTRONIC' then
    v_source_complete:=v_has_hours
      and nullif(btrim(coalesce(v_ts.r2_nurse_key,'')),'') is not null
      and nullif(btrim(coalesce(v_ts.img_sha256_nurse,'')),'') is not null
      and nullif(btrim(coalesce(v_ts.r2_auth_key,'')),'') is not null
      and nullif(btrim(coalesce(v_ts.img_sha256_auth,'')),'') is not null
      and nullif(btrim(coalesce(v_ts.auth_name,'')),'') is not null
      and v_ts.authorised_at_server is not null;
  elsif upper(coalesce(v_ts.submission_mode::text,''))='MANUAL' and v_is_signed_qr then
    v_source_complete:=v_has_hours
      and upper(coalesce(v_ts.qr_status::text,''))='USED'
      and v_ts.qr_scanned_at is not null
      and nullif(btrim(coalesce(v_ts.qr_signed_hash,'')),'') is not null
      and v_ts.qr_signed_at_utc is not null
      and nullif(btrim(coalesce(v_ts.manual_pdf_r2_key,'')),'') is not null;
  elsif upper(coalesce(v_ts.submission_mode::text,''))='MANUAL' then
    if v_ts.manual_document_asset_id is not null then
      select * into v_asset from public.invoice_document_assets a
      where a.id=v_ts.manual_document_asset_id limit 1;
    end if;
    v_source_complete:=v_has_hours and (
      nullif(btrim(coalesce(v_ts.manual_pdf_r2_key,'')),'') is not null
      or (
        v_asset.id is not null
        and upper(coalesce(v_asset.status,''))='READY'
        and nullif(btrim(coalesce(v_asset.normalised_r2_key,v_asset.original_r2_key,'')),'') is not null
      )
    );
  end if;

  if v_ts.current_document_version_id is not null then
    select * into v_doc from public.invoice_document_versions d
    where d.id=v_ts.current_document_version_id limit 1;
  end if;
  v_document_ready:=v_source_complete
    and v_doc.id is not null
    and v_doc.entity_type='TIMESHEET'
    and v_doc.entity_id=v_ts.timesheet_id
    and v_doc.purpose='TIMESHEET'
    and v_doc.source_revision=v_ts.document_revision::text
    and v_doc.status='READY'
    and v_doc.superseded_at_utc is null
    and nullif(btrim(coalesce(v_doc.r2_key,'')),'') is not null
    and coalesce(v_doc.sha256,'')~'^[0-9a-f]{64}$'
    and coalesce(v_doc.size_bytes,0)>0
    and coalesce(v_doc.page_count,0)>0
    and v_doc.ready_at_utc is not null
    and v_doc.verified_at_utc is not null;

  v_reason_code:=case
    when coalesce((v_protection->>'invoice_locked')::boolean,false) then 'TIMESHEET_PRESENT_BUT_INVOICED'
    when not v_source_complete then 'TIMESHEET_EVIDENCE_INCOMPLETE'
    when not v_document_ready then 'TIMESHEET_EVIDENCE_PREPARING'
  end;
  v_fingerprint:=public._import_review_hash_v1(concat_ws('|','query-evidence-v1',v_ts.timesheet_id,
    v_ts.document_revision,v_ts.document_state,v_ts.current_document_version_id,
    v_ts.submission_mode::text,v_ts.sheet_scope::text,v_ts.updated_at,
    v_source_complete,v_document_ready,v_reason_code,
    coalesce(v_doc.id::text,''),coalesce(v_doc.source_revision,''),coalesce(v_doc.sha256,''),
    coalesce(v_doc.size_bytes::text,''),coalesce(v_doc.page_count::text,''),
    coalesce((v_protection->>'invoice_locked')::boolean,false)));

  return jsonb_strip_nulls(jsonb_build_object(
    'timesheet_id',v_ts.timesheet_id,
    'source_complete',v_source_complete,
    'document_ready',v_document_ready,
    'preparation_required',v_source_complete and not v_document_ready
      and not coalesce((v_protection->>'invoice_locked')::boolean,false),
    'reason_code',v_reason_code,
    'evidence_fingerprint',v_fingerprint,
    'document_revision',v_ts.document_revision,
    'document_state',v_ts.document_state,
    'document_version_id',case when v_document_ready then v_doc.id end,
    'attachment_r2_key',case when v_document_ready then v_doc.r2_key end,
    'attachment_sha256',case when v_document_ready then v_doc.sha256 end,
    'attachment_size_bytes',case when v_document_ready then v_doc.size_bytes end,
    'attachment_page_count',case when v_document_ready then v_doc.page_count end,
    'attachment_filename',case when v_document_ready then 'timesheet-'||v_ts.timesheet_id::text||'.pdf' end,
    'submission_mode',v_ts.submission_mode::text,
    'sheet_scope',v_ts.sheet_scope::text,
    'signed_qr',v_is_signed_qr,
    'protection',v_protection
  ));
end
$function$;

-- _import_review_ready_action_ids_core_v1(uuid)
CREATE OR REPLACE FUNCTION public._import_review_ready_action_ids_core_v1(p_import_id uuid)
 RETURNS TABLE(action_id text)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
begin
  return query
  with selected_units as (
    select distinct d.candidate_id,d.client_id
    from public.import_review_decisions d
    where d.import_id=p_import_id and d.is_current and d.selectable and d.selected
      and not d.blocking and d.candidate_id is not null and d.client_id is not null
      and not exists (
        select 1 from public.import_review_action_outcomes o where o.action_id=d.action_id
      )
      and not (d.action_kind='NO_ACTION' and exists (
        select 1 from public.import_review_action_outcomes o
        where o.import_id=d.import_id and o.source_identity=d.source_identity
      ))
  ), eligible_units as (
    select u.candidate_id,u.client_id
    from selected_units u
    where not exists (
      select 1 from public.import_review_decisions b
      where b.import_id=p_import_id and b.is_current and b.blocking
        -- A blocker only owns a resolved candidate/client unit when both
        -- identities are known.  An unresolved source worker or source client
        -- must remain pending without freezing unrelated ready candidates in
        -- the same Trust/import.
        and b.candidate_id=u.candidate_id
        and b.client_id=u.client_id
    )
  )
  select d.action_id
  from public.import_review_decisions d
  join eligible_units u on u.candidate_id=d.candidate_id and u.client_id=d.client_id
  where d.import_id=p_import_id and d.is_current and d.selectable and d.selected and not d.blocking
    and not exists (
      select 1 from public.import_review_action_outcomes o where o.action_id=d.action_id
    )
    and not (d.action_kind='NO_ACTION' and exists (
      select 1 from public.import_review_action_outcomes o
      where o.import_id=d.import_id and o.source_identity=d.source_identity
    ))
  order by d.action_id;
end
$function$;

-- _import_review_refresh_core_v1(uuid,bigint,uuid,integer)
CREATE OR REPLACE FUNCTION public._import_review_refresh_core_v1(p_import_id uuid, p_expected_state_version bigint, p_actor_user_id uuid, p_max_actions integer DEFAULT 5000)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_state public.import_review_states%rowtype;
  v_generation integer;
  v_fingerprint text;
  v_changed integer:=0; v_retired integer:=0; v_inserted integer:=0;
  v_blockers integer; v_selected integer; v_status text; v_auto integer:=0;
begin
  perform public._import_review_assert_actor_v1(p_actor_user_id);
  select * into v_state from public.import_review_states where import_id=p_import_id for update;
  if not found then raise exception 'IMPORT_REVIEW_NOT_FOUND' using errcode='P0002'; end if;
  if v_state.status not in ('STAGED','IN_REVIEW','BLOCKED','READY') then
    raise exception 'IMPORT_REVIEW_REFRESH_NOT_ALLOWED' using errcode='55000',detail=v_state.status;
  end if;
  if p_expected_state_version is not null and v_state.state_version<>p_expected_state_version then
    raise exception 'IMPORT_REVIEW_VERSION_CONFLICT' using errcode='40001',detail=v_state.state_version::text;
  end if;
  v_generation:=v_state.preview_generation+1;

  create temporary table if not exists pg_temp.review_next_actions on commit drop as
    select * from public._import_review_action_catalog_core_v1(p_import_id,v_generation,p_max_actions) with no data;
  truncate pg_temp.review_next_actions;
  insert into pg_temp.review_next_actions
    select * from public._import_review_action_catalog_core_v1(p_import_id,v_generation,p_max_actions);

  delete from pg_temp.review_next_actions n
  using public.import_review_action_outcomes o
  where o.import_id=p_import_id and o.action_id=n.action_id;

  -- Persist database-unambiguous Daily associations in the same normalised table.
  -- This is evidence linkage only and never edits the selected timesheet.
  insert into public.import_review_daily_timesheet_resolutions(
    import_id,hr_row_id,resolved_timesheet_id,resolution_method,status,evidence_fingerprint,
    preview_generation,state_version,selected_by_user_id
  )
  select p_import_id,n.hr_row_id,n.timesheet_id,'AUTO_MATCHED','CURRENT',
    public._import_review_hash_v1(concat_ws('|','daily-resolution-evidence-v1',
      n.source_identity,n.hr_row_id,n.timesheet_id,n.candidate_id,n.client_id,n.contract_id,
      coalesce((n.summary_json->'imported_evidence')::text,''),
      coalesce((n.summary_json->'current_evidence')::text,''),
      coalesce((n.summary_json->'mapping_evidence')::text,''),
      coalesce(n.summary_json->>'authority_fingerprint',''))),
    v_generation,v_state.state_version,p_actor_user_id
  from pg_temp.review_next_actions n
  where n.action_kind='NO_ACTION' and n.hr_row_id is not null and n.timesheet_id is not null
    and coalesce((n.summary_json->>'is_daily')::boolean,false)
  on conflict(import_id,hr_row_id) do update set
    resolved_timesheet_id=excluded.resolved_timesheet_id,resolution_method='AUTO_MATCHED',status='CURRENT',
    evidence_fingerprint=excluded.evidence_fingerprint,preview_generation=excluded.preview_generation,
    state_version=excluded.state_version,selected_at_utc=now(),selected_by_user_id=excluded.selected_by_user_id,
    stale_at_utc=null,stale_reason_code=null,updated_at_utc=now()
  where public.import_review_daily_timesheet_resolutions.status<>'APPLIED'
    and (public.import_review_daily_timesheet_resolutions.resolved_timesheet_id is distinct from excluded.resolved_timesheet_id
      or public.import_review_daily_timesheet_resolutions.status<>'CURRENT'
      or public.import_review_daily_timesheet_resolutions.evidence_fingerprint is distinct from excluded.evidence_fingerprint);
  get diagnostics v_auto=row_count;
  if v_auto>0 then
    truncate pg_temp.review_next_actions;
    insert into pg_temp.review_next_actions
      select * from public._import_review_action_catalog_core_v1(p_import_id,v_generation,p_max_actions);
    delete from pg_temp.review_next_actions n
    using public.import_review_action_outcomes o
    where o.import_id=p_import_id and o.action_id=n.action_id;
  end if;
  select public._import_review_hash_v1(coalesce(string_agg(action_id||':'||evidence_fingerprint,'|' order by action_id),''))
  into v_fingerprint from pg_temp.review_next_actions;

  update public.import_review_decisions d set is_current=false,refreshed_at_utc=now()
  where d.import_id=p_import_id and d.is_current
    and not exists(select 1 from pg_temp.review_next_actions n where n.action_id=d.action_id);
  get diagnostics v_retired=row_count;

  update public.import_review_decisions d set
    action_kind=n.action_kind,action_category=n.action_category,target_key=n.target_key,source_identity=n.source_identity,
    hr_row_id=n.hr_row_id,timesheet_id=n.timesheet_id,shift_id=n.shift_id,client_id=n.client_id,
    candidate_id=n.candidate_id,contract_id=n.contract_id,issue_id=n.issue_id,
    preview_generation=v_generation,evidence_fingerprint=n.evidence_fingerprint,selectable=n.selectable,
    default_selected=n.default_selected,
    selected=case when d.evidence_fingerprint=n.evidence_fingerprint and n.selectable then d.selected else false end,
    blocking=n.blocking,requires_reconfirmation=(d.evidence_fingerprint<>n.evidence_fingerprint and d.selected),
    is_current=true,summary_json=n.summary_json,refreshed_at_utc=now(),
    selected_at_utc=case when d.evidence_fingerprint=n.evidence_fingerprint then d.selected_at_utc else null end,
    selected_by_user_id=case when d.evidence_fingerprint=n.evidence_fingerprint then d.selected_by_user_id else null end
  from pg_temp.review_next_actions n
  where d.action_id=n.action_id;
  get diagnostics v_changed=row_count;

  insert into public.import_review_decisions(action_id,import_id,action_kind,action_category,target_key,source_identity,
    hr_row_id,timesheet_id,shift_id,client_id,candidate_id,contract_id,issue_id,preview_generation,evidence_fingerprint,
    selectable,default_selected,selected,blocking,summary_json)
  select n.action_id,p_import_id,n.action_kind,n.action_category,n.target_key,n.source_identity,n.hr_row_id,n.timesheet_id,
    n.shift_id,n.client_id,n.candidate_id,n.contract_id,n.issue_id,v_generation,n.evidence_fingerprint,n.selectable,
    n.default_selected,n.default_selected,n.blocking,n.summary_json
  from pg_temp.review_next_actions n
  where not exists(select 1 from public.import_review_decisions d where d.action_id=n.action_id);
  get diagnostics v_inserted=row_count;

  -- A changed daily source/target evidence association is stale unless already applied.
  update public.import_review_daily_timesheet_resolutions r set status='STALE',stale_at_utc=now(),
    stale_reason_code='EVIDENCE_CHANGED',updated_at_utc=now()
  where r.import_id=p_import_id and r.status='CURRENT' and not exists (
    select 1 from pg_temp.review_next_actions n
    where n.hr_row_id=r.hr_row_id
      and public._import_review_hash_v1(concat_ws('|','daily-resolution-evidence-v1',
        n.source_identity,n.hr_row_id,n.timesheet_id,n.candidate_id,n.client_id,n.contract_id,
        coalesce((n.summary_json->'imported_evidence')::text,''),
        coalesce((n.summary_json->'current_evidence')::text,''),
        coalesce((n.summary_json->'mapping_evidence')::text,''),
        coalesce(n.summary_json->>'authority_fingerprint','')))=r.evidence_fingerprint);

  -- A Weekly candidate-did-not-work exception remains current only while the
  -- same server-proved HR row/timesheet evidence is still represented by the
  -- current catalogue.  The confirmed display action deliberately retains the
  -- same identity after the blocking advisory has been resolved.
  update public.import_review_weekly_validation_resolutions r
  set status='STALE',stale_at_utc=now(),stale_reason_code='EVIDENCE_CHANGED',updated_at_utc=now()
  where r.import_id=p_import_id and r.status='CURRENT' and not exists (
    select 1 from pg_temp.review_next_actions n
    where n.hr_row_id=r.hr_row_id
      and n.timesheet_id=r.timesheet_id
      and n.evidence_fingerprint=r.evidence_fingerprint
      and n.summary_json->>'resolution_kind'='WEEKLY_CANDIDATE_DID_NOT_WORK'
  );

  select count(*) filter(where blocking),count(*) filter(where selected)
  into v_blockers,v_selected from public.import_review_decisions
  where import_id=p_import_id and is_current;
  v_status:=case when v_blockers>0 then 'BLOCKED' else 'READY' end;
  update public.import_review_states set status=v_status,state_version=state_version+1,
    preview_generation=v_generation,preview_fingerprint=v_fingerprint,updated_at_utc=now(),updated_by_user_id=p_actor_user_id
  where import_id=p_import_id returning * into v_state;
  insert into public.import_review_events(import_id,state_version,event_code,actor_user_id,event_context_json)
  values(p_import_id,v_state.state_version,'PREVIEW_REFRESHED',p_actor_user_id,jsonb_build_object(
    'preview_generation',v_generation,'preview_fingerprint',v_fingerprint,'inserted',v_inserted,
    'reconciled',v_changed,'retired',v_retired,'blockers',v_blockers));
  return jsonb_build_object('ok',true,'schema_contract_version',v_state.schema_contract_version,
    'import_id',p_import_id,'status',v_state.status,
    'state_version',v_state.state_version,'preview_generation',v_generation,'preview_fingerprint',v_fingerprint,
    'action_count',(select count(*) from pg_temp.review_next_actions),'blocker_count',v_blockers,'selected_count',v_selected,
    'inserted_count',v_inserted,'reconciled_count',v_changed,'retired_count',v_retired);
end
$function$;

-- _import_review_state_guard_v1()
CREATE OR REPLACE FUNCTION public._import_review_state_guard_v1()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_transition_allowed boolean:=false;
  v_old_without_allowed jsonb;
  v_new_without_allowed jsonb;
begin
  if tg_op='DELETE' then raise exception 'IMPORT_REVIEW_STATE_DELETE_BLOCKED' using errcode='55000'; end if;
  if new.import_id is distinct from old.import_id or new.schema_contract_version is distinct from old.schema_contract_version then
    raise exception 'IMPORT_REVIEW_STATE_IDENTITY_IMMUTABLE' using errcode='55000';
  end if;
  if new.state_version<old.state_version then raise exception 'IMPORT_REVIEW_STATE_VERSION_REGRESSION' using errcode='23514'; end if;
  if old.status in ('APPLIED','ABANDONED','SUPERSEDED') then
    if old.status='APPLIED' then
      v_old_without_allowed:=to_jsonb(old)-array['state_version','follow_up_status','follow_up_error_code','follow_up_error_message','follow_up_retry_count','last_opened_at_utc','last_opened_by_user_id','updated_at_utc','updated_by_user_id'];
      v_new_without_allowed:=to_jsonb(new)-array['state_version','follow_up_status','follow_up_error_code','follow_up_error_message','follow_up_retry_count','last_opened_at_utc','last_opened_by_user_id','updated_at_utc','updated_by_user_id'];
    else
      v_old_without_allowed:=to_jsonb(old)-array['last_opened_at_utc','last_opened_by_user_id','updated_at_utc','updated_by_user_id'];
      v_new_without_allowed:=to_jsonb(new)-array['last_opened_at_utc','last_opened_by_user_id','updated_at_utc','updated_by_user_id'];
    end if;
    if v_old_without_allowed is distinct from v_new_without_allowed then raise exception 'IMPORT_REVIEW_TERMINAL_STATE_IMMUTABLE' using errcode='55000'; end if;
    return new;
  end if;
  v_transition_allowed:=case old.status
    when 'STAGED' then new.status in ('STAGED','IN_REVIEW','BLOCKED','READY','ABANDONED','SUPERSEDED')
    when 'IN_REVIEW' then new.status in ('IN_REVIEW','BLOCKED','READY','ABANDONED','SUPERSEDED')
    when 'BLOCKED' then new.status in ('IN_REVIEW','BLOCKED','READY','APPLYING','ABANDONED','SUPERSEDED')
    when 'READY' then new.status in ('IN_REVIEW','BLOCKED','READY','APPLYING','APPLIED','ABANDONED','SUPERSEDED')
    when 'APPLYING' then new.status in ('APPLYING','IN_REVIEW','BLOCKED','READY','APPLIED')
    else false end;
  if not v_transition_allowed then
    raise exception 'IMPORT_REVIEW_STATUS_TRANSITION_INVALID' using errcode='23514',detail=jsonb_build_object('old_status',old.status,'new_status',new.status)::text;
  end if;
  if new.status is distinct from old.status and new.state_version<=old.state_version then raise exception 'IMPORT_REVIEW_STATUS_TRANSITION_REQUIRES_VERSION' using errcode='23514'; end if;
  if new.status='APPLYING' and new.last_operation_id is null then raise exception 'IMPORT_REVIEW_APPLYING_OPERATION_REQUIRED' using errcode='23514'; end if;
  if new.status='APPLIED' and (new.applied_at_utc is null or new.applied_by_user_id is null or new.last_operation_id is null) then raise exception 'IMPORT_REVIEW_APPLIED_METADATA_REQUIRED' using errcode='23514'; end if;
  if new.status='ABANDONED' and (new.abandoned_at_utc is null or new.abandoned_by_user_id is null or nullif(btrim(new.abandoned_reason),'') is null) then raise exception 'IMPORT_REVIEW_ABANDONED_METADATA_REQUIRED' using errcode='23514'; end if;
  if new.status='SUPERSEDED' and (new.superseded_at_utc is null or new.superseded_by_user_id is null) then raise exception 'IMPORT_REVIEW_SUPERSEDED_METADATA_REQUIRED' using errcode='23514'; end if;
  return new;
end
$function$;

-- _import_review_timesheet_has_calculated_expenses_core_v1(uuid)
CREATE OR REPLACE FUNCTION public._import_review_timesheet_has_calculated_expenses_core_v1(p_timesheet_id uuid)
 RETURNS boolean
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
  select p_timesheet_id is not null and exists (
    select 1
    from public.timesheets_financials tf
    where tf.timesheet_id=p_timesheet_id
      and tf.is_current=true
      and (
        coalesce(tf.expenses_pay_ex_vat,0)<>0
        or coalesce(tf.expenses_charge_ex_vat,0)<>0
        or coalesce(tf.mileage_pay_ex_vat,0)<>0
        or coalesce(tf.mileage_charge_ex_vat,0)<>0
        or coalesce(tf.travel_pay_ex_vat,0)<>0
        or coalesce(tf.travel_charge_ex_vat,0)<>0
        or coalesce(tf.accommodation_pay_ex_vat,0)<>0
        or coalesce(tf.accommodation_charge_ex_vat,0)<>0
        or coalesce(tf.other_pay_ex_vat,0)<>0
        or coalesce(tf.other_charge_ex_vat,0)<>0
      )
  )
$function$;

-- _import_review_timesheet_protection_core_v1(uuid)
CREATE OR REPLACE FUNCTION public._import_review_timesheet_protection_core_v1(p_timesheet_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_active_draft boolean:=false;
  v_paid boolean:=false;
  v_invoice_locked boolean:=false;
  v_processing_status text;
  v_correction_root_id uuid;
  v_correction_family_ids uuid[];
begin
  if p_timesheet_id is null then
    return jsonb_build_object('active_pay_draft',false,'paid',false,'invoice_locked',false,'protected',false);
  end if;

  with recursive correction_ancestry as (
    select
      t.timesheet_id,
      t.parent_timesheet_id,
      array[t.timesheet_id]::uuid[] as visited_ids,
      0 as depth
    from public.timesheets t
    where t.timesheet_id=p_timesheet_id
    union all
    select
      parent_timesheet.timesheet_id,
      parent_timesheet.parent_timesheet_id,
      correction_ancestry.visited_ids||parent_timesheet.timesheet_id,
      correction_ancestry.depth+1
    from correction_ancestry
    join public.timesheets parent_timesheet
      on parent_timesheet.timesheet_id=correction_ancestry.parent_timesheet_id
    where correction_ancestry.depth<64
      and not parent_timesheet.timesheet_id=any(correction_ancestry.visited_ids)
  )
  select
    coalesce(array_agg(correction_ancestry.timesheet_id order by correction_ancestry.depth),array[p_timesheet_id]::uuid[]),
    (array_agg(correction_ancestry.timesheet_id order by correction_ancestry.depth desc))[1]
  into v_correction_family_ids,v_correction_root_id
  from correction_ancestry;
  v_correction_family_ids:=coalesce(v_correction_family_ids,array[p_timesheet_id]::uuid[]);
  v_correction_root_id:=coalesce(v_correction_root_id,p_timesheet_id);

  select coalesce(tf.paid_at_utc is not null,false),
         coalesce(tf.locked_by_invoice_id is not null,false),
         tf.processing_status::text
  into v_paid,v_invoice_locked,v_processing_status
  from public.timesheets_financials tf
  where tf.timesheet_id=p_timesheet_id and tf.is_current=true
  order by tf.updated_at desc nulls last
  limit 1;

  -- Banking Pay settlement is recorded in frozen batch artifacts and the
  -- canonical pay-state cache; it does not rewrite the legacy TSFIN paid marker.
  -- Treat a settled, non-voided batch item anywhere in the correction family
  -- as paid evidence so imports cannot amend a CSV/provider-settled replacement
  -- in place. Frozen delta items are intentionally rooted at the original
  -- timesheet, not at the replacement member that contributed the delta.
  v_paid := coalesce(v_paid,false) or exists (
    select 1
    from public.pay_batch_items settled_item
    join public.pay_batch_candidates settled_candidate
      on settled_candidate.id=settled_item.pay_batch_candidate_id
    where (
        settled_item.timesheet_id=any(v_correction_family_ids)
        or settled_item.frozen_component_snapshot_json->>'correction_root_id'=any(v_correction_family_ids::text[])
        or settled_item.frozen_resolution_payload_json->>'correction_root_id'=any(v_correction_family_ids::text[])
      )
      and coalesce(settled_item.is_voided,false)=false
      and (
        upper(btrim(coalesce(settled_candidate.settlement_status,'')))='SETTLED'
        or settled_candidate.settled_at_utc is not null
      )
  );

  -- Once any invoice line exists, the timesheet must not be amended in place.
  -- This applies equally to draft, unissued, issued and paid invoices: every
  -- subsequent import-authoritative change must use the reversal route.
  v_invoice_locked := coalesce(v_invoice_locked,false) or exists (
    select 1
    from public.invoice_lines il
    where il.timesheet_id=p_timesheet_id
  );

  v_active_draft := exists (
    select 1
    from (
      select pb.id
      from public.pay_batch_items pbi
      join public.pay_batch_candidates pbc on pbc.id=pbi.pay_batch_candidate_id
      join public.pay_batches pb on pb.id=pbc.pay_batch_id
      where pbi.timesheet_id=p_timesheet_id and not coalesce(pbi.is_voided,false)
        and public._pay_batch_status_is_active_reservation(pb.status)
      union all
      select pb.id
      from public.pay_batch_timesheet_snapshots pts
      join public.pay_batches pb on pb.id=pts.pay_batch_id
      where pts.timesheet_id=p_timesheet_id
        and public._pay_batch_status_is_active_reservation(pb.status)
      union all
      select pb.id
      from public.pay_payment_correction_items pci
      join public.pay_batches pb on pb.id=pci.pay_batch_id
      where pci.timesheet_id=p_timesheet_id and upper(btrim(coalesce(pci.status,'')))='APPLIED'
        and public._pay_batch_status_is_active_reservation(pb.status)
      union all
      select pb.id
      from public.timesheet_payment_overrides tpo
      join public.pay_batches pb on pb.id=tpo.consumed_by_pay_batch_id
      where tpo.timesheet_id=p_timesheet_id and tpo.cleared_at_utc is null
        and public._pay_batch_status_is_active_reservation(pb.status)
      union all
      select pb.id
      from public.pay_advances pa
      join public.pay_advance_reservations par on par.finance_case_id=pa.id
      join public.pay_batches pb on pb.id=par.pay_batch_id
      where pa.linked_timesheet_id=p_timesheet_id
        and upper(btrim(coalesce(par.status,''))) in ('RESERVED','COMMITTED')
        and par.released_at_utc is null and par.settled_at_utc is null
        and public._pay_batch_status_is_active_reservation(pb.status)
      union all
      select pb.id
      from public.pay_finance_case_components pfc
      join public.pay_advance_reservations par on par.finance_component_id=pfc.id
      join public.pay_batches pb on pb.id=par.pay_batch_id
      where pfc.linked_timesheet_id=p_timesheet_id
        and upper(btrim(coalesce(par.status,''))) in ('RESERVED','COMMITTED')
        and par.released_at_utc is null and par.settled_at_utc is null
        and public._pay_batch_status_is_active_reservation(pb.status)
      limit 1
    ) blockers
  );

  return jsonb_build_object(
    'active_pay_draft',v_active_draft,
    'paid',coalesce(v_paid,false),
    'invoice_locked',coalesce(v_invoice_locked,false),
    'processing_status',v_processing_status,
    'protected',v_active_draft or coalesce(v_paid,false) or coalesce(v_invoice_locked,false)
  );
end
$function$;

-- _import_review_validate_ui_state_v1(jsonb)
CREATE OR REPLACE FUNCTION public._import_review_validate_ui_state_v1(p_ui_state jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 IMMUTABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v jsonb:=coalesce(p_ui_state,'{}'::jsonb);
  v_key text;
begin
  if jsonb_typeof(v)<>'object' or pg_column_size(v)>65536 then
    raise exception 'IMPORT_REVIEW_UI_STATE_INVALID' using errcode='22023';
  end if;
  for v_key in select jsonb_object_keys(v) loop
    if v_key not in ('expanded_candidates','expanded_clients','expanded_weeks','expanded_shifts',
                     'active_section','scroll_anchor','show_no_action','show_automatic',
                     'page_number','page_size','sort_by','sort_direction') then
      raise exception 'IMPORT_REVIEW_UI_STATE_KEY_NOT_ALLOWED' using errcode='22023',detail=v_key;
    end if;
  end loop;
  -- Validate presentation state structurally.  Scanning the entire JSON text
  -- treated the legitimate active section EMAILS as authority and made a
  -- checkbox save fail.  Expansion tokens are now opaque, typed and bounded;
  -- no recipient, action, timesheet or financial identity can be persisted.
  if exists (
    select 1
    from jsonb_each(v) e
    where e.key in ('expanded_candidates','expanded_clients','expanded_weeks','expanded_shifts')
      and jsonb_typeof(e.value)<>'array'
  ) then
    raise exception 'IMPORT_REVIEW_UI_STATE_EXPANSION_INVALID' using errcode='22023';
  end if;
  if exists (
    select 1 from jsonb_each(v) e
    where jsonb_array_length(case when jsonb_typeof(e.value)='array' then e.value else '[]'::jsonb end)>500
  ) then raise exception 'IMPORT_REVIEW_UI_STATE_ARRAY_LIMIT_EXCEEDED' using errcode='22023'; end if;
  if exists (
    select 1
    from jsonb_each(v) e
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(e.value)='array' then e.value else '[]'::jsonb end
    ) a(value)
    where e.key in ('expanded_candidates','expanded_clients','expanded_weeks','expanded_shifts')
      and (
        jsonb_typeof(a.value)<>'string'
        or length(a.value#>>'{}')>96
        or (a.value#>>'{}') !~ '^(candidate|client|week|shift):u-[0-9a-f]{16}(-[0-9a-f]{16})?$'
      )
  ) then
    raise exception 'IMPORT_REVIEW_UI_STATE_CONTAINS_AUTHORITY' using errcode='22023';
  end if;
  if v?'active_section' and (
    jsonb_typeof(v->'active_section')<>'string'
    or upper(v->>'active_section') not in ('PENDING','READY','EMAIL','NO_ACTION')
  ) then raise exception 'IMPORT_REVIEW_UI_STATE_SECTION_INVALID' using errcode='22023'; end if;
  if v?'scroll_anchor' and jsonb_typeof(v->'scroll_anchor')<>'null' then
    raise exception 'IMPORT_REVIEW_UI_STATE_SCROLL_ANCHOR_INVALID' using errcode='22023'; end if;
  if v?'show_no_action' and jsonb_typeof(v->'show_no_action')<>'boolean' then
    raise exception 'IMPORT_REVIEW_UI_STATE_FLAG_INVALID' using errcode='22023'; end if;
  if v?'show_automatic' and jsonb_typeof(v->'show_automatic')<>'boolean' then
    raise exception 'IMPORT_REVIEW_UI_STATE_FLAG_INVALID' using errcode='22023'; end if;
  if v?'page_number' and (jsonb_typeof(v->'page_number')<>'number' or (v->>'page_number')!~'^\d+$'
      or (v->>'page_number')::integer not between 1 and 100) then
    raise exception 'IMPORT_REVIEW_UI_STATE_PAGE_INVALID' using errcode='22023'; end if;
  if v?'page_size' and (jsonb_typeof(v->'page_size')<>'number' or (v->>'page_size')!~'^\d+$'
      or (v->>'page_size')::integer not in (25,50,75,100)) then
    raise exception 'IMPORT_REVIEW_UI_STATE_PAGE_SIZE_INVALID' using errcode='22023'; end if;
  if v?'sort_by' and upper(v->>'sort_by') not in ('CANDIDATE','CLIENT','WEEK_ENDING','WORK_DATE','ACTION','STATUS') then
    raise exception 'IMPORT_REVIEW_UI_STATE_SORT_INVALID' using errcode='22023'; end if;
  if v?'sort_direction' and upper(v->>'sort_direction') not in ('ASC','DESC') then
    raise exception 'IMPORT_REVIEW_UI_STATE_SORT_DIRECTION_INVALID' using errcode='22023'; end if;
  return v;
end
$function$;

-- _inv_collect_weekly_manual_schedule_refs(text,text,jsonb)
CREATE OR REPLACE FUNCTION public._inv_collect_weekly_manual_schedule_refs(p_sheet_scope text, p_submission_mode text, p_actual_schedule_json jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 IMMUTABLE
AS $function$
declare
  scope text := upper(btrim(coalesce(p_sheet_scope,'')));
  mode  text := upper(btrim(coalesce(p_submission_mode,'')));
  seg jsonb;
  v_start text;
  v_end   text;
  v_ref   text;
  seen text[] := array[]::text[];
  out jsonb := '[]'::jsonb;
begin
  if not (scope = 'WEEKLY' and mode = 'MANUAL') then
    return '[]'::jsonb;
  end if;

  if p_actual_schedule_json is null or jsonb_typeof(p_actual_schedule_json) <> 'array' then
    return '[]'::jsonb;
  end if;

  for seg in
    select value from jsonb_array_elements(p_actual_schedule_json) value
  loop
    if seg is null or jsonb_typeof(seg) <> 'object' then
      continue;
    end if;

    v_start := btrim(coalesce(seg->>'start',''));
    v_end   := btrim(coalesce(seg->>'end',''));
    if not (v_start <> '' and v_end <> '') then
      continue;
    end if;

    v_ref := btrim(coalesce(seg->>'ref_num',''));
    if v_ref = '' then
      continue;
    end if;

    if v_ref = any(seen) then
      continue;
    end if;

    seen := array_append(seen, v_ref);
    out := out || jsonb_build_array(v_ref);
  end loop;

  return out;
end;
$function$;

-- _inv_day_refs_has_any(jsonb)
CREATE OR REPLACE FUNCTION public._inv_day_refs_has_any(day_refs jsonb)
 RETURNS boolean
 LANGUAGE sql
 IMMUTABLE
AS $function$
  select exists (
    select 1
    from jsonb_each_text(coalesce(day_refs, '{}'::jsonb)) e
    where btrim(coalesce(e.value,'')) <> ''
  );
$function$;

-- _inv_iso_utc(timestamp with time zone)
CREATE OR REPLACE FUNCTION public._inv_iso_utc(ts timestamp with time zone)
 RETURNS text
 LANGUAGE sql
 IMMUTABLE
AS $function$
  select to_char((ts at time zone 'UTC'), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"');
$function$;

-- _inv_lock_all_segments_json(jsonb,uuid)
CREATE OR REPLACE FUNCTION public._inv_lock_all_segments_json(p_ib jsonb, p_invoice_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 IMMUTABLE
AS $function$
declare
  seg jsonb;
  out_segs jsonb := '[]'::jsonb;
begin
  if p_ib is null or jsonb_typeof(p_ib) <> 'object' then
    return p_ib;
  end if;

  if coalesce(p_ib->>'mode','') <> 'SEGMENTS' then
    return p_ib;
  end if;

  if jsonb_typeof(p_ib->'segments') <> 'array' then
    return p_ib;
  end if;

  for seg in
    select value from jsonb_array_elements(p_ib->'segments') value
  loop
    if seg is null or jsonb_typeof(seg) <> 'object' then
      out_segs := out_segs || jsonb_build_array(seg);
    else
      if nullif(coalesce(seg->>'invoice_locked_invoice_id',''), '') is null then
        seg := jsonb_set(seg, '{invoice_locked_invoice_id}', to_jsonb(p_invoice_id::text), true);
      end if;
      out_segs := out_segs || jsonb_build_array(seg);
    end if;
  end loop;

  return jsonb_set(p_ib, '{segments}', out_segs, true);
end;
$function$;

-- _inv_lock_segments_for_invoice(uuid,jsonb)
CREATE OR REPLACE FUNCTION public._inv_lock_segments_for_invoice(p_invoice_id uuid, p_segment_refs jsonb)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now_iso text := public._inv_iso_utc(now());
  ref jsonb;
  v_tsfin_id uuid;

  -- per-tsfin
  v_lock_whole boolean;
  v_seg_ids text[];

  snap record;
  ib jsonb;
  basis text;
  is_selfbill_or_nhsp boolean;
  all_locked boolean;

  seg jsonb;
  segs_out jsonb;
  sid text;
  locked text;

  i int;

  -- =====================================================
  -- DEBUG (invoice_debug): single audit row per RPC call
  -- =====================================================
  v_invoice_debug boolean := false;
  v_dbg_started_at timestamptz := now();
  v_dbg_steps jsonb := '[]'::jsonb;
  v_dbg_sqlstate text := null;
  v_dbg_error text := null;
  v_dbg_stats jsonb := '{}'::jsonb;

  v_tsfins_seen int := 0;
  v_tsfins_updated int := 0;
  v_segments_newly_locked int := 0;
  v_summaries_set int := 0;
begin
  -- Load invoice_debug flag (safe even if column not yet present)
  begin
    select coalesce(sd.invoice_debug, false)
    into v_invoice_debug
    from public.settings_defaults sd
    where sd.id = 1
    limit 1;
  exception when undefined_column then
    v_invoice_debug := false;
  end;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','start',
        'at_utc', public._inv_iso_utc(v_dbg_started_at),
        'invoice_id', coalesce(p_invoice_id::text,'')
      )
    );
  end if;

  if p_segment_refs is null or jsonb_typeof(p_segment_refs) <> 'array' then
    return;
  end if;

  -- Build distinct tsfin_ids
  for ref in
    select value from jsonb_array_elements(p_segment_refs) value
  loop
    if jsonb_typeof(ref) <> 'object' then
      continue;
    end if;

    if nullif(coalesce(ref->>'tsfin_id',''), '') is null then
      continue;
    end if;
  end loop;

  -- Process each tsfin_id separately (mirror JS loop)
  for v_tsfin_id in
    select distinct (x->>'tsfin_id')::uuid
    from jsonb_array_elements(p_segment_refs) x
    where nullif(coalesce(x->>'tsfin_id',''), '') is not null
  loop
    v_tsfins_seen := v_tsfins_seen + 1;

    -- Gather ref set for this tsfin_id
    select
      bool_or(nullif(coalesce(x->>'segment_id',''), '') is null) as lock_whole,
      array_agg(distinct (x->>'segment_id')) filter (where nullif(coalesce(x->>'segment_id',''), '') is not null) as seg_ids
    into v_lock_whole, v_seg_ids
    from jsonb_array_elements(p_segment_refs) x
    where (x->>'tsfin_id')::uuid = v_tsfin_id;

    v_lock_whole := coalesce(v_lock_whole,false);
    v_seg_ids := coalesce(v_seg_ids, array[]::text[]);

    -- Load snapshot (✅ FIX: lock the tsfin row to prevent concurrent lost-updates)
    select tf.id, tf.basis, tf.locked_by_invoice_id, tf.invoice_breakdown_json
    into snap
    from public.timesheets_financials tf
    where tf.id = v_tsfin_id
    for update;

    if not found then
      continue;
    end if;

    ib := snap.invoice_breakdown_json;
    basis := upper(coalesce(snap.basis::text,''));

    is_selfbill_or_nhsp :=
      basis = 'NHSP' or
      basis = 'NHSP_ADJUSTMENT' or
      basis = 'HEALTHROSTER_SELF_BILL' or
      basis = 'HEALTHROSTER_ADJUSTMENT';

    all_locked := true;
    segs_out := null;

    if ib is not null
       and jsonb_typeof(ib) = 'object'
       and coalesce(ib->>'mode','') = 'SEGMENTS'
       and jsonb_typeof(ib->'segments') = 'array'
    then
      segs_out := '[]'::jsonb;

      for seg in
        select value from jsonb_array_elements(ib->'segments') value
      loop
        if seg is null or jsonb_typeof(seg) <> 'object' then
          segs_out := segs_out || jsonb_build_array(seg);

          -- ✅ FIX (defensive, does not affect valid data):
          -- If we are NOT explicitly locking whole, invalid segment elements must prevent
          -- whole-timesheet lock inference (avoids marking locked_by_invoice_id due to JSON nulls).
          if not v_lock_whole then
            all_locked := false;
          end if;

          continue;
        end if;

        sid := coalesce(seg->>'segment_id','');
        locked := nullif(coalesce(seg->>'invoice_locked_invoice_id',''), '');

        if v_lock_whole or (sid <> '' and sid = any(v_seg_ids)) then
          if locked is null then
            locked := p_invoice_id::text;
            v_segments_newly_locked := v_segments_newly_locked + 1;
          end if;
        end if;

        if locked is null then
          all_locked := false;
        end if;

    seg := jsonb_set(seg, '{invoice_locked_invoice_id}', coalesce(to_jsonb(locked), 'null'::jsonb), true);

        segs_out := segs_out || jsonb_build_array(seg);
      end loop;

      ib := jsonb_set(ib, '{segments}', segs_out, true);
    else
      -- No segments
      all_locked := (snap.locked_by_invoice_id is not null) or v_lock_whole;
    end if;

    if (not is_selfbill_or_nhsp) and all_locked then
      v_summaries_set := v_summaries_set + 1;
    end if;

    -- Patch
    if ib is not null then
      update public.timesheets_financials
      set
        updated_at = now(),
        invoice_breakdown_json = ib,
        locked_by_invoice_id = case
          when (not is_selfbill_or_nhsp) and all_locked then p_invoice_id
          else locked_by_invoice_id
        end,
        locked_at_utc = case
          when (not is_selfbill_or_nhsp) and all_locked then now()
          else locked_at_utc
        end
      where id = v_tsfin_id;
    else
      update public.timesheets_financials
      set
        updated_at = now(),
        locked_by_invoice_id = case
          when (not is_selfbill_or_nhsp) and all_locked then p_invoice_id
          else locked_by_invoice_id
        end,
        locked_at_utc = case
          when (not is_selfbill_or_nhsp) and all_locked then now()
          else locked_at_utc
        end
      where id = v_tsfin_id;
    end if;

    v_tsfins_updated := v_tsfins_updated + 1;
  end loop;

  if v_invoice_debug then
    begin
      v_dbg_stats := jsonb_build_object(
        'tsfins_seen', v_tsfins_seen,
        'tsfins_updated', v_tsfins_updated,
        'segments_newly_locked', v_segments_newly_locked,
        'summaries_set', v_summaries_set
      );

      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','finish',
          'at_utc', public._inv_iso_utc(now()),
          'stats', v_dbg_stats
        )
      );

      perform public._inv_write_audit(
        null,
        'INV_LOCK_SEGMENTS_DEBUG',
        jsonb_build_object(
          'invoice_id', coalesce(p_invoice_id::text,''),
          'stats', v_dbg_stats,
          'steps', v_dbg_steps
        ),
        'timesheets_financials',
        ('invoice:' || coalesce(p_invoice_id::text,'')),
        null,
        'INVOICE_DEBUG',
        null, null, null
      );
    exception when others then
      null;
    end;
  end if;

exception when others then
  v_dbg_sqlstate := SQLSTATE;
  v_dbg_error := SQLERRM;

  if v_invoice_debug then
    begin
      v_dbg_stats := jsonb_build_object(
        'tsfins_seen', v_tsfins_seen,
        'tsfins_updated', v_tsfins_updated,
        'segments_newly_locked', v_segments_newly_locked,
        'summaries_set', v_summaries_set
      );

      perform public._inv_write_audit(
        null,
        'INV_LOCK_SEGMENTS_ERROR',
        jsonb_build_object(
          'invoice_id', coalesce(p_invoice_id::text,''),
          'sqlstate', v_dbg_sqlstate,
          'error', v_dbg_error,
          'stats', v_dbg_stats,
          'steps', v_dbg_steps
        ),
        'timesheets_financials',
        ('invoice:' || coalesce(p_invoice_id::text,'')),
        null,
        'INVOICE_DEBUG',
        null, null, null
      );
    exception when others then
      null;
    end;
  end if;

  raise;
end;
$function$;

-- _inv_round2(numeric)
CREATE OR REPLACE FUNCTION public._inv_round2(n numeric)
 RETURNS numeric
 LANGUAGE sql
 IMMUTABLE
AS $function$
  select round(coalesce(n,0)::numeric, 2);
$function$;

-- _inv_segments_for_invoice(uuid,uuid,text,text,text)
CREATE OR REPLACE FUNCTION public._inv_segments_for_invoice(p_invoice_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid, p_ip text DEFAULT NULL::text, p_user_agent text DEFAULT NULL::text, p_correlation_id text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_invoice_debug boolean := false;
  v_steps jsonb := '[]'::jsonb;
  v_result jsonb := '{}'::jsonb;
  v_now timestamptz := now();
  v_reason text := 'INVOICE_DEBUG';
  v_action_ok text := 'INV_SEGMENTS_FOR_INVOICE_DEBUG';
  v_action_err text := 'INV_SEGMENTS_FOR_INVOICE_ERROR';
  v_sqlstate text;
  v_err text;
  v_ts_count int := 0;
  v_return_keys int := 0;
begin
  -- Load invoice_debug flag
  begin
    select coalesce(sd.invoice_debug, false)
      into v_invoice_debug
    from public.settings_defaults sd
    limit 1;
  exception when others then
    v_invoice_debug := false;
  end;

  if p_invoice_id is null then
    raise exception 'p_invoice_id is required';
  end if;

  select count(distinct l.timesheet_id)::int
    into v_ts_count
  from public.invoice_lines l
  where l.invoice_id = p_invoice_id
    and l.timesheet_id is not null;

  v_steps := v_steps || jsonb_build_array(
    jsonb_build_object(
      'step','start',
      'invoice_id', p_invoice_id::text,
      'timesheet_count_on_invoice', coalesce(v_ts_count,0),
      'at_utc', public._inv_iso_utc(v_now)
    )
  );

  select coalesce(
    jsonb_object_agg(
      t.timesheet_id::text,
      jsonb_build_object(
        'invoiced_segments', coalesce(t.invoiced_segments, '[]'::jsonb),
        'uninvoiced_segment_count', coalesce(t.uninvoiced_segment_count, 0),
        'locked_elsewhere_segment_count', coalesce(t.locked_elsewhere_segment_count, 0)
      )
    ),
    '{}'::jsonb
  )
  into v_result
  from (
    select
      ts.timesheet_id,
      coalesce(
        jsonb_agg(s.seg order by coalesce(s.seg->>'date',''), coalesce(s.seg->>'segment_id',''))
          filter (where s.locked_text = p_invoice_id::text),
        '[]'::jsonb
      ) as invoiced_segments,
      (count(*) filter (where s.locked_text is null))::int as uninvoiced_segment_count,
      (count(*) filter (where s.locked_text is not null and s.locked_text <> p_invoice_id::text))::int as locked_elsewhere_segment_count
    from (
      select distinct l.timesheet_id
      from public.invoice_lines l
      where l.invoice_id = p_invoice_id
        and l.timesheet_id is not null
    ) ts
    join public.timesheets_financials tf
      on tf.is_current = true
     and tf.timesheet_id = ts.timesheet_id
    left join lateral (
      select
        value as seg,
        nullif(btrim(coalesce(value->>'invoice_locked_invoice_id','')), '') as locked_text
      from jsonb_array_elements(
        case
          when jsonb_typeof(tf.invoice_breakdown_json) = 'object'
           and jsonb_typeof(tf.invoice_breakdown_json->'segments') = 'array'
          then tf.invoice_breakdown_json->'segments'
          else '[]'::jsonb
        end
      ) value
    ) s on true
    where upper(coalesce(tf.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
    group by ts.timesheet_id
  ) t;

  select count(*)::int
    into v_return_keys
  from jsonb_object_keys(v_result) k;

  v_steps := v_steps || jsonb_build_array(
    jsonb_build_object(
      'step','built',
      'return_timesheet_keys', coalesce(v_return_keys,0)
    )
  );

  if v_invoice_debug then
    perform public._inv_write_audit(
      p_actor_user_id,
      v_action_ok,
      jsonb_build_object(
        'invoice_id', p_invoice_id::text,
        'timesheet_count_on_invoice', coalesce(v_ts_count,0),
        'return_timesheet_keys', coalesce(v_return_keys,0),
        'steps', v_steps
      ),
      'invoices',
      p_invoice_id::text,
      null,
      v_reason,
      p_ip,
      p_user_agent,
      p_correlation_id
    );
  end if;

  return v_result;

exception when others then
  v_sqlstate := sqlstate;
  v_err := sqlerrm;

  if v_invoice_debug then
    perform public._inv_write_audit(
      p_actor_user_id,
      v_action_err,
      jsonb_build_object(
        'invoice_id', case when p_invoice_id is null then null else p_invoice_id::text end,
        'sqlstate', v_sqlstate,
        'error', v_err,
        'steps', v_steps
      ),
      'invoices',
      case when p_invoice_id is null then null else p_invoice_id::text end,
      null,
      v_reason,
      p_ip,
      p_user_agent,
      p_correlation_id
    );
  end if;

  raise;
end;
$function$;

-- _inv_timesheet_has_invoice_reference(text,text,text,jsonb,jsonb)
CREATE OR REPLACE FUNCTION public._inv_timesheet_has_invoice_reference(p_sheet_scope text, p_submission_mode text, p_reference_number text, p_day_references_json jsonb, p_actual_schedule_json jsonb)
 RETURNS boolean
 LANGUAGE plpgsql
 IMMUTABLE
AS $function$
declare
  scope text := upper(btrim(coalesce(p_sheet_scope,'')));
  mode  text := upper(btrim(coalesce(p_submission_mode,'')));
begin
  if scope = 'WEEKLY' and mode = 'MANUAL' then
    return public._inv_weekly_manual_schedule_has_complete_refs(p_actual_schedule_json);
  end if;

  if scope = 'WEEKLY' then
    return (btrim(coalesce(p_reference_number,'')) <> '') or public._inv_day_refs_has_any(p_day_references_json);
  end if;

  return (btrim(coalesce(p_reference_number,'')) <> '');
end;
$function$;

-- _inv_unlock_segment_refs_for_invoice(uuid,jsonb,uuid,text,text,text)
CREATE OR REPLACE FUNCTION public._inv_unlock_segment_refs_for_invoice(p_invoice_id uuid, p_segment_refs jsonb, p_actor_user_id uuid DEFAULT NULL::uuid, p_ip text DEFAULT NULL::text, p_user_agent text DEFAULT NULL::text, p_correlation_id text DEFAULT NULL::text)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_invoice_debug boolean := false;
  v_steps jsonb := '[]'::jsonb;
  v_tsfin_id uuid;
  v_seg_ids text[];
  v_ib jsonb;
  v_out_segs jsonb;
  v_seg jsonb;
  v_unlocked int;
  v_total int;
  v_has_unlocked boolean;
  v_locked_ids text[];
  v_only_locked_text text;
  v_locked_by uuid;
  v_locked_at timestamptz;
  v_now timestamptz := now();
  v_reason text := 'INVOICE_DEBUG';
  v_action_ok text := 'INV_UNLOCK_SEGREFS_DEBUG';
  v_action_err text := 'INV_UNLOCK_SEGREFS_ERROR';
  v_sqlstate text;
  v_err text;
begin
  -- Load invoice_debug flag
  begin
    select coalesce(sd.invoice_debug, false)
      into v_invoice_debug
    from public.settings_defaults sd
    limit 1;
  exception when others then
    v_invoice_debug := false;
  end;

  if p_invoice_id is null then
    raise exception 'p_invoice_id is required';
  end if;

  if p_segment_refs is null or jsonb_typeof(p_segment_refs) <> 'array' then
    raise exception 'p_segment_refs must be a json array';
  end if;

  v_steps := v_steps || jsonb_build_array(
    jsonb_build_object(
      'step','start',
      'invoice_id', p_invoice_id::text,
      'ref_count', jsonb_array_length(p_segment_refs),
      'at_utc', public._inv_iso_utc(v_now)
    )
  );

  -- Group segment refs by tsfin_id
  for v_tsfin_id, v_seg_ids in
    select
      (nullif(btrim(coalesce(x->>'tsfin_id','')),''))::uuid as tsfin_id,
      array_agg(nullif(btrim(coalesce(x->>'segment_id','')),''))
        filter (where nullif(btrim(coalesce(x->>'segment_id','')), '') is not null) as segment_ids
    from jsonb_array_elements(p_segment_refs) x
    group by (nullif(btrim(coalesce(x->>'tsfin_id','')),''))::uuid
  loop
    if v_tsfin_id is null then
      raise exception 'segment_refs contains null/invalid tsfin_id';
    end if;
    if v_seg_ids is null or array_length(v_seg_ids,1) is null then
      raise exception 'segment_refs contains no segment_id for tsfin_id %', v_tsfin_id;
    end if;

    -- ✅ FIX: Lock the TSFIN row at read time (prevents lost-update delay corruption)
    select tf.invoice_breakdown_json
      into v_ib
    from public.timesheets_financials tf
    where tf.id = v_tsfin_id
      and tf.is_current = true
    for update;

    if not found then
      raise exception 'timesheets_financials row not found or not current for tsfin_id %', v_tsfin_id;
    end if;

    if v_ib is null or jsonb_typeof(v_ib) <> 'object' then
      raise exception 'invoice_breakdown_json invalid for tsfin_id %', v_tsfin_id;
    end if;

    if upper(coalesce(v_ib->>'mode','')) <> 'SEGMENTS' then
      raise exception 'tsfin_id % is not SEGMENTS mode', v_tsfin_id;
    end if;

    if jsonb_typeof(v_ib->'segments') <> 'array' then
      raise exception 'tsfin_id % has no segments array', v_tsfin_id;
    end if;

    v_out_segs := '[]'::jsonb;
    v_unlocked := 0;

    for v_seg in
      select value
      from jsonb_array_elements(v_ib->'segments') value
    loop
      if v_seg is null or jsonb_typeof(v_seg) <> 'object' then
        v_out_segs := v_out_segs || jsonb_build_array(v_seg);
      else
        if (coalesce(v_seg->>'segment_id','') = any(v_seg_ids))
           and (nullif(btrim(coalesce(v_seg->>'invoice_locked_invoice_id','')), '') = p_invoice_id::text) then
          v_seg := jsonb_set(v_seg, '{invoice_locked_invoice_id}', 'null'::jsonb, true);
          v_seg := jsonb_set(v_seg, '{invoice_locked_at_utc}', 'null'::jsonb, true);
          v_unlocked := v_unlocked + 1;
        end if;
        v_out_segs := v_out_segs || jsonb_build_array(v_seg);
      end if;
    end loop;

    -- Recompute lock summary invariant
    -- ✅ FIX: ignore invalid/non-object elements (and elements missing segment_id) so JSON nulls
    -- cannot create "phantom unlocked" or affect whole-timesheet lock inference.
    select
      count(*)::int,
      bool_or(nullif(btrim(coalesce(e->>'invoice_locked_invoice_id','')), '') is null),
      array_agg(distinct nullif(btrim(coalesce(e->>'invoice_locked_invoice_id','')), ''))
        filter (where nullif(btrim(coalesce(e->>'invoice_locked_invoice_id','')), '') is not null)
      into v_total, v_has_unlocked, v_locked_ids
    from jsonb_array_elements(v_out_segs) e
    where jsonb_typeof(e) = 'object'
      and nullif(btrim(coalesce(e->>'segment_id','')), '') is not null;

    v_locked_by := null;
    v_locked_at := null;

    if coalesce(v_total,0) = 0 then
      v_locked_by := null;
      v_locked_at := null;
    else
      if (v_has_unlocked is false) and v_locked_ids is not null and array_length(v_locked_ids,1) = 1 then
        v_only_locked_text := v_locked_ids[1];

        if v_only_locked_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
          v_locked_by := v_only_locked_text::uuid;
        else
          v_locked_by := null;
        end if;

        begin
          select min((nullif(btrim(coalesce(e->>'invoice_locked_at_utc','')), ''))::timestamptz)
            into v_locked_at
          from jsonb_array_elements(v_out_segs) e
          where jsonb_typeof(e) = 'object'
            and nullif(btrim(coalesce(e->>'segment_id','')), '') is not null
            and nullif(btrim(coalesce(e->>'invoice_locked_invoice_id','')), '') = v_only_locked_text
            and nullif(btrim(coalesce(e->>'invoice_locked_at_utc','')), '') is not null;
        exception when others then
          v_locked_at := null;
        end;
      end if;
    end if;

    update public.timesheets_financials tf
    set
      invoice_breakdown_json = jsonb_set(tf.invoice_breakdown_json, '{segments}', v_out_segs, true),
      locked_by_invoice_id = v_locked_by,
      locked_at_utc = v_locked_at
    where tf.id = v_tsfin_id
      and tf.is_current = true;

    v_steps := v_steps || jsonb_build_array(
      jsonb_build_object(
        'step','tsfin_updated',
        'tsfin_id', v_tsfin_id::text,
        'segment_ids', to_jsonb(v_seg_ids),
        'segments_total', coalesce(v_total,0),
        'segments_unlocked_now', v_unlocked,
        'has_unlocked', coalesce(v_has_unlocked,false),
        'distinct_locked_invoice_ids', coalesce(array_length(v_locked_ids,1),0),
        'locked_by_invoice_id', case when v_locked_by is null then null else v_locked_by::text end,
        'locked_at_utc', case when v_locked_at is null then null else public._inv_iso_utc(v_locked_at) end
      )
    );

  end loop;

  if v_invoice_debug then
    perform public._inv_write_audit(
      p_actor_user_id,
      v_action_ok,
      jsonb_build_object(
        'invoice_id', p_invoice_id::text,
        'segment_refs', p_segment_refs,
        'steps', v_steps
      ),
      'invoices',
      p_invoice_id::text,
      null,
      v_reason,
      p_ip,
      p_user_agent,
      p_correlation_id
    );
  end if;

exception when others then
  v_sqlstate := sqlstate;
  v_err := sqlerrm;

  if v_invoice_debug then
    perform public._inv_write_audit(
      p_actor_user_id,
      v_action_err,
      jsonb_build_object(
        'invoice_id', case when p_invoice_id is null then null else p_invoice_id::text end,
        'segment_refs', p_segment_refs,
        'sqlstate', v_sqlstate,
        'error', v_err,
        'steps', v_steps
      ),
      'invoices',
      case when p_invoice_id is null then null else p_invoice_id::text end,
      null,
      v_reason,
      p_ip,
      p_user_agent,
      p_correlation_id
    );
  end if;

  raise;
end;
$function$;

-- _inv_unlock_segment_refs_for_invoice(uuid,jsonb)
CREATE OR REPLACE FUNCTION public._inv_unlock_segment_refs_for_invoice(p_invoice_id uuid, p_segment_refs jsonb)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();
  v_invoice_debug boolean := false;
  v_dbg_started_at timestamptz := now();
  v_dbg_steps jsonb := '[]'::jsonb;
  v_dbg_sqlstate text := null;
  v_dbg_error text := null;
  v_dbg_stats jsonb := '{}'::jsonb;

  v_tsfin_id uuid;
  v_unlock_whole boolean;
  v_seg_ids text[];

  r_tf record;
  v_ib jsonb;
  v_seg jsonb;
  v_out_segs jsonb;
  v_sid text;
  v_locked_text text;

  v_seg_count int;
  v_any_unlocked boolean;
  v_first_locked text;
  v_multi boolean;
  v_new_locked uuid;
  v_new_locked_at timestamptz;

  v_tsfins int := 0;
  v_tsfins_updated int := 0;
  v_segments_unlocked int := 0;
  v_summaries_set int := 0;
begin
  -- Load invoice_debug flag (safe even if column not yet present)
  begin
    select coalesce(sd.invoice_debug, false)
    into v_invoice_debug
    from public.settings_defaults sd
    where sd.id = 1
    limit 1;
  exception when undefined_column then
    v_invoice_debug := false;
  end;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object('step','start','at_utc',public._inv_iso_utc(v_dbg_started_at),'invoice_id',coalesce(p_invoice_id::text,''))
    );
  end if;

  if p_invoice_id is null then
    return;
  end if;

  if p_segment_refs is null or jsonb_typeof(p_segment_refs) <> 'array' then
    return;
  end if;

  for v_tsfin_id in
    select distinct (x->>'tsfin_id')::uuid
    from jsonb_array_elements(p_segment_refs) x
    where nullif(btrim(coalesce(x->>'tsfin_id','')), '') is not null
  loop
    v_tsfins := v_tsfins + 1;

    select
      bool_or(nullif(btrim(coalesce(x->>'segment_id','')), '') is null) as unlock_whole,
      array_agg(distinct (x->>'segment_id')) filter (where nullif(btrim(coalesce(x->>'segment_id','')), '') is not null) as seg_ids
    into v_unlock_whole, v_seg_ids
    from jsonb_array_elements(p_segment_refs) x
    where (x->>'tsfin_id')::uuid = v_tsfin_id;

    v_unlock_whole := coalesce(v_unlock_whole,false);
    v_seg_ids := coalesce(v_seg_ids, array[]::text[]);

    -- ✅ FIX: lock the TSFIN row at read time to prevent lost-update corruption
    select tf.*
    into r_tf
    from public.timesheets_financials tf
    where tf.id = v_tsfin_id
    limit 1
    for update;

    if not found then
      continue;
    end if;

    v_ib := r_tf.invoice_breakdown_json;

    v_seg_count := 0;
    v_any_unlocked := false;
    v_first_locked := null;
    v_multi := false;

    if v_ib is not null
       and jsonb_typeof(v_ib) = 'object'
       and coalesce(v_ib->>'mode','') = 'SEGMENTS'
       and jsonb_typeof(v_ib->'segments') = 'array'
    then
      v_out_segs := '[]'::jsonb;

      for v_seg in
        select value from jsonb_array_elements(v_ib->'segments') value
      loop
        v_seg_count := v_seg_count + 1;

        if v_seg is null or jsonb_typeof(v_seg) <> 'object' then
          v_out_segs := v_out_segs || jsonb_build_array(v_seg);
          v_any_unlocked := true;
          continue;
        end if;

        v_sid := coalesce(v_seg->>'segment_id','');
        v_locked_text := nullif(btrim(coalesce(v_seg->>'invoice_locked_invoice_id','')), '');

        if v_locked_text = p_invoice_id::text and (v_unlock_whole or (v_sid <> '' and v_sid = any(v_seg_ids))) then
          v_locked_text := null;
          v_segments_unlocked := v_segments_unlocked + 1;
        end if;

        if v_locked_text is null then
          v_any_unlocked := true;
        else
          if v_first_locked is null then
            v_first_locked := v_locked_text;
          elsif v_locked_text <> v_first_locked then
            v_multi := true;
          end if;
        end if;

  v_seg := jsonb_set(v_seg, '{invoice_locked_invoice_id}', coalesce(to_jsonb(v_locked_text), 'null'::jsonb), true);
v_out_segs := v_out_segs || jsonb_build_array(v_seg);

      end loop;

      v_ib := jsonb_set(v_ib, '{segments}', v_out_segs, true);

      v_new_locked := null;
      if v_seg_count > 0 and (not v_any_unlocked) and (not v_multi) and v_first_locked is not null then
        v_new_locked := v_first_locked::uuid;
      end if;

      if v_new_locked is not null then
        v_summaries_set := v_summaries_set + 1;
        if r_tf.locked_by_invoice_id is not null and r_tf.locked_by_invoice_id = v_new_locked and r_tf.locked_at_utc is not null then
          v_new_locked_at := r_tf.locked_at_utc;
        else
          v_new_locked_at := v_now;
        end if;
      else
        v_new_locked_at := null;
      end if;

      update public.timesheets_financials tfu
      set
        updated_at = v_now,
        invoice_breakdown_json = v_ib,
        locked_by_invoice_id = v_new_locked,
        locked_at_utc = v_new_locked_at
      where tfu.id = v_tsfin_id;

      v_tsfins_updated := v_tsfins_updated + 1;

    else
      if r_tf.locked_by_invoice_id = p_invoice_id then
        update public.timesheets_financials tfu
        set
          updated_at = v_now,
          locked_by_invoice_id = null,
          locked_at_utc = null
        where tfu.id = v_tsfin_id;

        v_tsfins_updated := v_tsfins_updated + 1;
      end if;
    end if;
  end loop;

  if v_invoice_debug then
    begin
      v_dbg_stats := jsonb_build_object(
        'tsfins_seen', v_tsfins,
        'tsfins_updated', v_tsfins_updated,
        'segments_unlocked', v_segments_unlocked,
        'summaries_set', v_summaries_set
      );
      v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object('step','finish','at_utc',public._inv_iso_utc(now()),'stats',v_dbg_stats));

      perform public._inv_write_audit(
        null,
        'INV_UNLOCK_SEGREFS_DEBUG',
        jsonb_build_object('invoice_id',p_invoice_id::text,'stats',v_dbg_stats,'steps',v_dbg_steps),
        'timesheets_financials',
        ('invoice:' || p_invoice_id::text),
        null,
        'INVOICE_DEBUG',
        null,null,null
      );
    exception when others then
      null;
    end;
  end if;

exception when others then
  v_dbg_sqlstate := SQLSTATE;
  v_dbg_error := SQLERRM;

  if v_invoice_debug then
    begin
      v_dbg_stats := jsonb_build_object(
        'tsfins_seen', v_tsfins,
        'tsfins_updated', v_tsfins_updated,
        'segments_unlocked', v_segments_unlocked,
        'summaries_set', v_summaries_set
      );
      perform public._inv_write_audit(
        null,
        'INV_UNLOCK_SEGREFS_ERROR',
        jsonb_build_object('invoice_id',coalesce(p_invoice_id::text,''),'sqlstate',v_dbg_sqlstate,'error',v_dbg_error,'stats',v_dbg_stats,'steps',v_dbg_steps),
        'timesheets_financials',
        ('invoice:' || coalesce(p_invoice_id::text,'')),
        null,
        'INVOICE_DEBUG',
        null,null,null
      );
    exception when others then
      null;
    end;
  end if;

  raise;
end;
$function$;

-- _inv_unlock_segments_for_invoice(uuid,uuid[])
CREATE OR REPLACE FUNCTION public._inv_unlock_segments_for_invoice(p_invoice_id uuid, p_timesheet_ids uuid[])
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();
  v_invoice_debug boolean := false;
  v_dbg_started_at timestamptz := now();
  v_dbg_steps jsonb := '[]'::jsonb;
  v_dbg_sqlstate text := null;
  v_dbg_error text := null;
  v_dbg_stats jsonb := '{}'::jsonb;

  v_ts_id uuid;
  r_tf record;
  v_ib jsonb;
  v_seg jsonb;
  v_out_segs jsonb;
  v_locked_text text;

  v_seg_count int;
  v_any_unlocked boolean;
  v_first_locked text;
  v_multi boolean;
  v_new_locked uuid;
  v_new_locked_at timestamptz;

  v_snapshots_found int := 0;
  v_snapshots_updated int := 0;
  v_segments_unlocked int := 0;
  v_summaries_set int := 0;
begin
  -- Load invoice_debug flag (safe even if column not yet present)
  begin
    select coalesce(sd.invoice_debug, false)
    into v_invoice_debug
    from public.settings_defaults sd
    where sd.id = 1
    limit 1;
  exception when undefined_column then
    v_invoice_debug := false;
  end;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object('step','start','at_utc',public._inv_iso_utc(v_dbg_started_at),'invoice_id',coalesce(p_invoice_id::text,''))
    );
  end if;

  if p_invoice_id is null then
    return;
  end if;

  if p_timesheet_ids is null or coalesce(array_length(p_timesheet_ids,1),0) = 0 then
    return;
  end if;

  foreach v_ts_id in array p_timesheet_ids loop
    if v_ts_id is null then
      continue;
    end if;

    select tf.*
    into r_tf
    from public.timesheets_financials tf
    where tf.is_current = true
      and tf.timesheet_id = v_ts_id
    order by tf.created_at desc
    limit 1;

    if not found then
      continue;
    end if;

    v_snapshots_found := v_snapshots_found + 1;
    v_ib := r_tf.invoice_breakdown_json;

    if v_ib is not null
       and jsonb_typeof(v_ib) = 'object'
       and coalesce(v_ib->>'mode','') = 'SEGMENTS'
       and jsonb_typeof(v_ib->'segments') = 'array'
    then
      v_out_segs := '[]'::jsonb;
      v_seg_count := 0;
      v_any_unlocked := false;
      v_first_locked := null;
      v_multi := false;

      for v_seg in
        select value from jsonb_array_elements(v_ib->'segments') value
      loop
        v_seg_count := v_seg_count + 1;

        if v_seg is null or jsonb_typeof(v_seg) <> 'object' then
          v_out_segs := v_out_segs || jsonb_build_array(v_seg);
          v_any_unlocked := true;
          continue;
        end if;

        v_locked_text := nullif(btrim(coalesce(v_seg->>'invoice_locked_invoice_id','')), '');

        if v_locked_text = p_invoice_id::text then
          v_locked_text := null;
          v_segments_unlocked := v_segments_unlocked + 1;
        end if;

        if v_locked_text is null then
          v_any_unlocked := true;
        else
          if v_first_locked is null then
            v_first_locked := v_locked_text;
          elsif v_locked_text <> v_first_locked then
            v_multi := true;
          end if;
        end if;

v_seg := jsonb_set(v_seg, '{invoice_locked_invoice_id}', coalesce(to_jsonb(v_locked_text), 'null'::jsonb), true);
v_out_segs := v_out_segs || jsonb_build_array(v_seg);

      end loop;

      v_ib := jsonb_set(v_ib, '{segments}', v_out_segs, true);

      v_new_locked := null;
      if v_seg_count > 0 and (not v_any_unlocked) and (not v_multi) and v_first_locked is not null then
        v_new_locked := v_first_locked::uuid;
      end if;

      if v_new_locked is not null then
        v_summaries_set := v_summaries_set + 1;
        if r_tf.locked_by_invoice_id is not null and r_tf.locked_by_invoice_id = v_new_locked and r_tf.locked_at_utc is not null then
          v_new_locked_at := r_tf.locked_at_utc;
        else
          v_new_locked_at := v_now;
        end if;
      else
        v_new_locked_at := null;
      end if;

      update public.timesheets_financials tfu
      set
        updated_at = v_now,
        invoice_breakdown_json = v_ib,
        locked_by_invoice_id = v_new_locked,
        locked_at_utc = v_new_locked_at
      where tfu.id = r_tf.id;

      v_snapshots_updated := v_snapshots_updated + 1;

    else
      -- Non-segments: unlock whole snapshot if it was locked to this invoice
      if r_tf.locked_by_invoice_id = p_invoice_id then
        update public.timesheets_financials tfu
        set
          updated_at = v_now,
          locked_by_invoice_id = null,
          locked_at_utc = null
        where tfu.id = r_tf.id;

        v_snapshots_updated := v_snapshots_updated + 1;
      end if;
    end if;
  end loop;

  if v_invoice_debug then
    begin
      v_dbg_stats := jsonb_build_object(
        'timesheet_ids_count', coalesce(array_length(p_timesheet_ids,1),0),
        'snapshots_found', v_snapshots_found,
        'snapshots_updated', v_snapshots_updated,
        'segments_unlocked', v_segments_unlocked,
        'summaries_set', v_summaries_set
      );
      v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object('step','finish','at_utc',public._inv_iso_utc(now()),'stats',v_dbg_stats));

      perform public._inv_write_audit(
        null,
        'INV_UNLOCK_SEGMENTS_DEBUG',
        jsonb_build_object('invoice_id',p_invoice_id::text,'stats',v_dbg_stats,'steps',v_dbg_steps),
        'timesheets_financials',
        ('invoice:' || p_invoice_id::text),
        null,
        'INVOICE_DEBUG',
        null,null,null
      );
    exception when others then
      null;
    end;
  end if;

exception when others then
  v_dbg_sqlstate := SQLSTATE;
  v_dbg_error := SQLERRM;

  if v_invoice_debug then
    begin
      v_dbg_stats := jsonb_build_object(
        'timesheet_ids_count', coalesce(array_length(p_timesheet_ids,1),0),
        'snapshots_found', v_snapshots_found,
        'snapshots_updated', v_snapshots_updated,
        'segments_unlocked', v_segments_unlocked,
        'summaries_set', v_summaries_set
      );
      perform public._inv_write_audit(
        null,
        'INV_UNLOCK_SEGMENTS_ERROR',
        jsonb_build_object('invoice_id',coalesce(p_invoice_id::text,''),'sqlstate',v_dbg_sqlstate,'error',v_dbg_error,'stats',v_dbg_stats,'steps',v_dbg_steps),
        'timesheets_financials',
        ('invoice:' || coalesce(p_invoice_id::text,'')),
        null,
        'INVOICE_DEBUG',
        null,null,null
      );
    exception when others then
      null;
    end;
  end if;

  raise;
end;
$function$;

-- _inv_weekly_manual_schedule_has_complete_refs(jsonb)
CREATE OR REPLACE FUNCTION public._inv_weekly_manual_schedule_has_complete_refs(schedule jsonb)
 RETURNS boolean
 LANGUAGE plpgsql
 IMMUTABLE
AS $function$
declare
  seg jsonb;
  v_start text;
  v_end   text;
  v_ref   text;
  n int;
begin
  if schedule is null or jsonb_typeof(schedule) <> 'array' then
    return false;
  end if;

  n := jsonb_array_length(schedule);
  if n <= 0 then
    return false;
  end if;

  for seg in
    select value from jsonb_array_elements(schedule) value
  loop
    if seg is null or jsonb_typeof(seg) <> 'object' then
      continue;
    end if;

    v_start := btrim(coalesce(seg->>'start',''));
    v_end   := btrim(coalesce(seg->>'end',''));

    if v_start <> '' and v_end <> '' then
      v_ref := btrim(coalesce(seg->>'ref_num',''));
      if v_ref = '' then
        return false;
      end if;
    end if;
  end loop;

  return true;
end;
$function$;

-- _inv_write_audit(uuid,text,jsonb,text,text,jsonb,text,text,text,text)
CREATE OR REPLACE FUNCTION public._inv_write_audit(p_actor_user_id uuid, p_action text, p_after_json jsonb, p_entity text, p_subject_id text, p_before_json jsonb DEFAULT NULL::jsonb, p_reason text DEFAULT NULL::text, p_ip text DEFAULT NULL::text, p_user_agent text DEFAULT NULL::text, p_correlation_id text DEFAULT NULL::text)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_actor_display text := null;
  v_actor_role text := 'system';
begin
  if p_actor_user_id is not null then
    select
      nullif(btrim(coalesce(u.display_name,'')), ''),
      nullif(btrim(coalesce(u.role,'')), '')
    into v_actor_display, v_actor_role
    from public.tms_users u
    where u.id = p_actor_user_id
    limit 1;

    if v_actor_display is null then
      select nullif(btrim(coalesce(u.email,'')), '')
      into v_actor_display
      from public.tms_users u
      where u.id = p_actor_user_id
      limit 1;
    end if;
  end if;

  if v_actor_display is null then
    v_actor_display := 'CloudTMS server';
  end if;

  if v_actor_role is null then
    v_actor_role := 'system';
  end if;

  insert into public.audit_events(
    object_type,
    object_id_text,
    action,
    before_json,
    after_json,
    reason,
    actor_user_id,
    actor_display,
    actor_role_at_time,
    ip,
    user_agent,
    correlation_id
  )
  values (
    coalesce(nullif(btrim(p_entity),''), 'generic'),
    nullif(btrim(p_subject_id), ''),
    coalesce(nullif(btrim(p_action),''), 'EVENT'),
    p_before_json,
    p_after_json,
    p_reason,
    p_actor_user_id,
    v_actor_display,
    v_actor_role,
    nullif(btrim(p_ip), ''),
    nullif(btrim(p_user_agent), ''),
    nullif(btrim(p_correlation_id), '')
  );
end;
$function$;

-- _pay_active_settled_components(uuid[])
CREATE OR REPLACE FUNCTION public._pay_active_settled_components(p_timesheet_ids uuid[])
 RETURNS TABLE(timesheet_id uuid, key_type text, key_value text, amount_ex_vat numeric, amount_inc_vat numeric)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
WITH input_timesheets AS (
  SELECT DISTINCT
    input_timesheet_values.timesheet_id_value AS timesheet_id
  FROM unnest(COALESCE(p_timesheet_ids, ARRAY[]::uuid[])) AS input_timesheet_values(timesheet_id_value)
  WHERE input_timesheet_values.timesheet_id_value IS NOT NULL
),
rotation_scope_rows AS (
  SELECT
    scope_rows.requested_timesheet_id,
    scope_rows.booking_id,
    scope_rows.canonical_timesheet_id,
    scope_rows.family_timesheet_id,
    scope_rows.family_is_current,
    scope_rows.family_version,
    scope_rows.requested_is_canonical
  FROM public._pay_timesheet_rotation_scope(
    (
      SELECT COALESCE(
        ARRAY_AGG(input_timesheets.timesheet_id ORDER BY input_timesheets.timesheet_id),
        ARRAY[]::uuid[]
      )
      FROM input_timesheets
    )
  ) AS scope_rows
),
rotation_scope_keyed AS (
  SELECT
    rotation_scope_rows.requested_timesheet_id,
    rotation_scope_rows.booking_id,
    rotation_scope_rows.canonical_timesheet_id,
    rotation_scope_rows.family_timesheet_id,
    rotation_scope_rows.family_is_current,
    rotation_scope_rows.family_version,
    rotation_scope_rows.requested_is_canonical,
    COALESCE(rotation_scope_rows.booking_id, rotation_scope_rows.requested_timesheet_id::text) AS scope_family_key
  FROM rotation_scope_rows
  WHERE rotation_scope_rows.requested_timesheet_id IS NOT NULL
),
projection_targets AS (
  SELECT
    rotation_scope_keyed.scope_family_key,
    COALESCE(
      (
        ARRAY_AGG(DISTINCT rotation_scope_keyed.canonical_timesheet_id ORDER BY rotation_scope_keyed.canonical_timesheet_id)
        FILTER (
          WHERE COALESCE(rotation_scope_keyed.requested_is_canonical, false) = true
            AND rotation_scope_keyed.canonical_timesheet_id IS NOT NULL
        )
      )[1],
      (
        ARRAY_AGG(DISTINCT rotation_scope_keyed.requested_timesheet_id ORDER BY rotation_scope_keyed.requested_timesheet_id)
        FILTER (WHERE rotation_scope_keyed.requested_timesheet_id IS NOT NULL)
      )[1],
      (
        ARRAY_AGG(DISTINCT rotation_scope_keyed.canonical_timesheet_id ORDER BY rotation_scope_keyed.canonical_timesheet_id)
        FILTER (WHERE rotation_scope_keyed.canonical_timesheet_id IS NOT NULL)
      )[1]
    ) AS projected_timesheet_id
  FROM rotation_scope_keyed
  GROUP BY rotation_scope_keyed.scope_family_key
),
family_to_projection AS (
  SELECT DISTINCT
    rotation_scope_keyed.family_timesheet_id,
    projection_targets.projected_timesheet_id
  FROM rotation_scope_keyed
  JOIN projection_targets
    ON projection_targets.scope_family_key = rotation_scope_keyed.scope_family_key
  WHERE rotation_scope_keyed.family_timesheet_id IS NOT NULL
    AND projection_targets.projected_timesheet_id IS NOT NULL
),
active_item_ids AS (
  SELECT DISTINCT
    public.pay_batch_items.id AS pay_batch_item_id,
    family_to_projection.projected_timesheet_id AS projected_timesheet_id
  FROM family_to_projection
  JOIN public.pay_batch_items
    ON public.pay_batch_items.timesheet_id = family_to_projection.family_timesheet_id
  JOIN public.pay_batch_candidates
    ON public.pay_batch_candidates.id = public.pay_batch_items.pay_batch_candidate_id
  LEFT JOIN public.pay_bank_transfers
    ON public.pay_bank_transfers.id = public.pay_batch_items.pay_bank_transfer_id
  WHERE COALESCE(public.pay_batch_items.is_voided, false) = false
    AND UPPER(BTRIM(COALESCE(public.pay_batch_items.item_type, ''))) IN (
      'SEGMENT_DELTA',
      'EXPENSE_DELTA',
      'ADJUSTMENT_DELTA',
      'MILEAGE_DELTA'
    )
    AND (
      UPPER(BTRIM(COALESCE(public.pay_batch_candidates.settlement_status, ''))) = 'SETTLED'
      OR public.pay_batch_candidates.settled_at_utc IS NOT NULL
      OR UPPER(BTRIM(COALESCE(public.pay_bank_transfers.status, ''))) = 'COMPLETED'
      OR public.pay_bank_transfers.completed_at_utc IS NOT NULL
      OR EXISTS (
        SELECT 1
        FROM public.timesheet_pay_state_history AS active_history
        WHERE active_history.pay_batch_id = public.pay_batch_candidates.pay_batch_id
          AND active_history.timesheet_id = public.pay_batch_items.timesheet_id
      )
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.pay_payment_correction_items AS applied_corrections
      WHERE applied_corrections.pay_batch_item_id = public.pay_batch_items.id
        AND applied_corrections.status = 'APPLIED'
        AND applied_corrections.correction_item_kind IN (
          'PRE_BANK_CANCEL',
          'NO_MONEY_UNWIND',
          'SETTLED_REVERSAL'
        )
    )
),
active_item_id_array AS (
  SELECT
    CASE
      WHEN COUNT(*) = 0 THEN ARRAY['00000000-0000-0000-0000-000000000000'::uuid]
      ELSE ARRAY_AGG(active_item_ids.pay_batch_item_id ORDER BY active_item_ids.pay_batch_item_id)
    END AS pay_batch_item_ids
  FROM active_item_ids
),
active_components AS (
  SELECT
    active_item_ids.pay_batch_item_id,
    active_item_ids.projected_timesheet_id AS component_timesheet_id,
    economic_components.key_type AS component_key_type,
    economic_components.key_value AS component_key_value,
    economic_components.source_amount_ex_vat AS component_amount_ex_vat,
    economic_components.source_amount_inc_vat AS component_amount_inc_vat
  FROM active_item_id_array
  JOIN LATERAL public._pay_batch_item_economic_components(
    p_pay_batch_id => NULL::uuid,
    p_pay_batch_item_ids => active_item_id_array.pay_batch_item_ids
  ) AS economic_components
    ON true
  JOIN active_item_ids
    ON active_item_ids.pay_batch_item_id = economic_components.pay_batch_item_id
  WHERE active_item_ids.projected_timesheet_id IS NOT NULL
    AND economic_components.timesheet_id IS NOT NULL
    AND economic_components.key_type IS NOT NULL
    AND BTRIM(COALESCE(economic_components.key_type, '')) <> ''
    AND economic_components.key_value IS NOT NULL
    AND BTRIM(COALESCE(economic_components.key_value, '')) <> ''
    AND economic_components.key_resolution_failure_reason IS NULL
    AND UPPER(BTRIM(COALESCE(economic_components.item_type, ''))) IN (
      'SEGMENT_DELTA',
      'EXPENSE_DELTA',
      'ADJUSTMENT_DELTA',
      'MILEAGE_DELTA'
    )
    AND UPPER(BTRIM(COALESCE(economic_components.key_type, ''))) IN (
      'TS_DAY',
      'TS_TOTAL',
      'ADDITIONAL_CODE',
      'ADJUSTMENT_CODE',
      'EXPENSE_CODE'
    )
    AND NOT (
      UPPER(BTRIM(COALESCE(economic_components.key_type, ''))) = 'TS_DAY'
      AND economic_components.key_value !~ '^\d{4}-\d{2}-\d{2}$'
    )
),
active_components_by_item_key AS (
  SELECT
    active_components.pay_batch_item_id,
    active_components.component_timesheet_id AS timesheet_id,
    UPPER(BTRIM(active_components.component_key_type)) AS key_type,
    active_components.component_key_value AS key_value,
    ROUND(COALESCE(SUM(COALESCE(active_components.component_amount_ex_vat, 0)), 0), 2)::numeric AS amount_ex_vat,
    ROUND(COALESCE(SUM(COALESCE(active_components.component_amount_inc_vat, 0)), 0), 2)::numeric AS amount_inc_vat
  FROM active_components
  GROUP BY
    active_components.pay_batch_item_id,
    active_components.component_timesheet_id,
    UPPER(BTRIM(active_components.component_key_type)),
    active_components.component_key_value
),
settled_finance_reservation_summary AS (
  SELECT
    finance_reservation.pay_batch_item_id,
    BOOL_OR(
      UPPER(BTRIM(COALESCE(finance_reservation.status, ''))) = 'SETTLED'
      OR finance_reservation.settled_at_utc IS NOT NULL
    ) AS has_settled_reservation,
    ROUND(COALESCE(SUM(
      ABS(COALESCE(
        finance_reservation.reserved_source_amount,
        finance_reservation.reserved_amount,
        0
      ))
    ) FILTER (
      WHERE UPPER(BTRIM(COALESCE(finance_reservation.status, ''))) = 'SETTLED'
         OR finance_reservation.settled_at_utc IS NOT NULL
    ), 0), 2)::numeric AS settled_source_amount_ex_vat
  FROM public.pay_advance_reservations AS finance_reservation
  WHERE finance_reservation.pay_batch_item_id IS NOT NULL
  GROUP BY finance_reservation.pay_batch_item_id
),
settled_finance_movement_components AS (
  SELECT
    finance_item.id AS pay_batch_item_id,
    family_to_projection.projected_timesheet_id AS timesheet_id,
    UPPER(BTRIM(finance_item.frozen_component_key_type)) AS key_type,
    BTRIM(finance_item.frozen_component_key_value) AS key_value,
    ROUND(
      CASE
        WHEN UPPER(BTRIM(finance_item.item_type)) = 'OVERPAYMENT_RECOVERY' THEN -1
        ELSE 1
      END
      * finance_source_amount.source_amount_ex_vat,
      2
    )::numeric AS amount_ex_vat,
    ROUND(
      CASE
        WHEN UPPER(BTRIM(finance_item.item_type)) = 'OVERPAYMENT_RECOVERY' THEN -1
        ELSE 1
      END
      * COALESCE(
          NULLIF(ABS(finance_item.amount_inc_vat), 0),
          finance_source_amount.source_amount_ex_vat
        ),
      2
    )::numeric AS amount_inc_vat
  FROM public.pay_batch_items AS finance_item
  JOIN public.pay_batch_candidates AS finance_candidate
    ON finance_candidate.id = finance_item.pay_batch_candidate_id
  LEFT JOIN public.pay_bank_transfers AS finance_transfer
    ON finance_transfer.id = finance_item.pay_bank_transfer_id
  LEFT JOIN settled_finance_reservation_summary AS finance_reservation
    ON finance_reservation.pay_batch_item_id = finance_item.id
  LEFT JOIN public.pay_finance_case_components AS finance_component
    ON finance_component.id = finance_item.finance_component_id
  JOIN family_to_projection
    ON family_to_projection.family_timesheet_id = CASE
      WHEN finance_item.timesheet_id IS NOT NULL THEN finance_item.timesheet_id
      WHEN NULLIF(BTRIM(COALESCE(finance_item.frozen_source_basis_json->>'timesheet_id', '')), '')
        ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        THEN (BTRIM(finance_item.frozen_source_basis_json->>'timesheet_id'))::uuid
      WHEN finance_component.linked_timesheet_id IS NOT NULL
        THEN finance_component.linked_timesheet_id
      ELSE NULL::uuid
    END
  CROSS JOIN LATERAL (
    SELECT ROUND(ABS(COALESCE(
      NULLIF(finance_item.frozen_source_amount, 0),
      NULLIF(finance_reservation.settled_source_amount_ex_vat, 0),
      NULLIF(finance_item.amount_ex_vat, 0),
      NULLIF(finance_item.amount_inc_vat, 0)
    )), 2)::numeric AS source_amount_ex_vat
  ) AS finance_source_amount
  WHERE COALESCE(finance_item.is_voided, false) = false
    AND UPPER(BTRIM(COALESCE(finance_item.item_type, ''))) IN (
      'OVERPAYMENT_RECOVERY',
      'UNDERPAYMENT_PAYMENT'
    )
    AND finance_item.frozen_component_key_type IS NOT NULL
    AND BTRIM(finance_item.frozen_component_key_type) <> ''
    AND finance_item.frozen_component_key_value IS NOT NULL
    AND BTRIM(finance_item.frozen_component_key_value) <> ''
    AND UPPER(BTRIM(finance_item.frozen_component_key_type)) IN (
      'TS_DAY',
      'TS_TOTAL',
      'ADDITIONAL_CODE',
      'ADJUSTMENT_CODE',
      'EXPENSE_CODE'
    )
    AND NOT (
      UPPER(BTRIM(finance_item.frozen_component_key_type)) = 'TS_DAY'
      AND finance_item.frozen_component_key_value !~ '^\d{4}-\d{2}-\d{2}$'
    )
    AND finance_source_amount.source_amount_ex_vat IS NOT NULL
    AND finance_source_amount.source_amount_ex_vat > 0
    AND (
      UPPER(BTRIM(COALESCE(finance_candidate.settlement_status, ''))) = 'SETTLED'
      OR finance_candidate.settled_at_utc IS NOT NULL
      OR UPPER(BTRIM(COALESCE(finance_transfer.status, ''))) = 'COMPLETED'
      OR finance_transfer.completed_at_utc IS NOT NULL
      OR COALESCE(finance_reservation.has_settled_reservation, false) = true
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.pay_payment_correction_items AS applied_finance_correction
      WHERE applied_finance_correction.pay_batch_item_id = finance_item.id
        AND applied_finance_correction.status = 'APPLIED'
        AND applied_finance_correction.correction_item_kind IN (
          'PRE_BANK_CANCEL',
          'NO_MONEY_UNWIND',
          'SETTLED_REVERSAL'
        )
    )
),
all_settled_components_by_item_key AS (
  SELECT
    active_components_by_item_key.pay_batch_item_id,
    active_components_by_item_key.timesheet_id,
    active_components_by_item_key.key_type,
    active_components_by_item_key.key_value,
    active_components_by_item_key.amount_ex_vat,
    active_components_by_item_key.amount_inc_vat
  FROM active_components_by_item_key

  UNION ALL

  SELECT
    settled_finance_movement_components.pay_batch_item_id,
    settled_finance_movement_components.timesheet_id,
    settled_finance_movement_components.key_type,
    settled_finance_movement_components.key_value,
    settled_finance_movement_components.amount_ex_vat,
    settled_finance_movement_components.amount_inc_vat
  FROM settled_finance_movement_components
),
active_component_totals AS (
  SELECT
    all_settled_components_by_item_key.timesheet_id,
    all_settled_components_by_item_key.key_type,
    all_settled_components_by_item_key.key_value,
    ROUND(COALESCE(SUM(COALESCE(all_settled_components_by_item_key.amount_ex_vat, 0)), 0), 2)::numeric AS amount_ex_vat,
    ROUND(COALESCE(SUM(COALESCE(all_settled_components_by_item_key.amount_inc_vat, 0)), 0), 2)::numeric AS amount_inc_vat
  FROM all_settled_components_by_item_key
  GROUP BY
    all_settled_components_by_item_key.timesheet_id,
    all_settled_components_by_item_key.key_type,
    all_settled_components_by_item_key.key_value
)
SELECT
  active_component_totals.timesheet_id,
  active_component_totals.key_type,
  active_component_totals.key_value,
  active_component_totals.amount_ex_vat,
  active_component_totals.amount_inc_vat
FROM active_component_totals
WHERE active_component_totals.timesheet_id IS NOT NULL
  AND active_component_totals.key_type IS NOT NULL
  AND active_component_totals.key_value IS NOT NULL
  AND (
    ROUND(COALESCE(active_component_totals.amount_ex_vat, 0), 2) <> 0
    OR ROUND(COALESCE(active_component_totals.amount_inc_vat, 0), 2) <> 0
  )
ORDER BY
  active_component_totals.timesheet_id,
  active_component_totals.key_type,
  active_component_totals.key_value;
$function$;

-- _pay_bank_transfer_provider_evidence_classify(uuid,uuid,jsonb,uuid)
CREATE OR REPLACE FUNCTION public._pay_bank_transfer_provider_evidence_classify(p_pay_batch_id uuid DEFAULT NULL::uuid, p_pay_bank_transfer_id uuid DEFAULT NULL::uuid, p_selection_json jsonb DEFAULT NULL::jsonb, p_operation_id uuid DEFAULT NULL::uuid)
 RETURNS TABLE(evidence_class text, provider_submitted boolean, provider_request_sent boolean, provider_response_present boolean, provider_event_present boolean, provider_external_id_present boolean, local_prepared_only boolean, cash_state text, blocker_code text, reason text, support_details_json jsonb)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_effective_pay_batch_id uuid := p_pay_batch_id;
  v_uuid_regex text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';
  v_scope_type text := NULL::text;
  v_direct_transfer_ids uuid[] := ARRAY[]::uuid[];
  v_selected_transfer_ids uuid[] := ARRAY[]::uuid[];
  v_transfer_ids uuid[] := ARRAY[]::uuid[];
  v_transfer_count integer := 0;
  v_scope_row_count integer := 0;
  v_provider_external_id_count integer := 0;
  v_provider_response_count integer := 0;
  v_provider_event_count integer := 0;
  v_provider_request_sent_count integer := 0;
  v_provider_response_event_count integer := 0;
  v_provider_poll_event_count integer := 0;
  v_verified_webhook_event_count integer := 0;
  v_failed_webhook_replay_event_count integer := 0;
  v_invalid_or_unverified_webhook_event_count integer := 0;
  v_canonical_provider_external_id_count integer := 0;
  v_canonical_final_paid_count integer := 0;
  v_canonical_terminal_no_money_count integer := 0;
  v_canonical_pending_non_final_count integer := 0;
  v_canonical_unknown_cash_state_count integer := 0;
  v_canonical_event_cash_state_counts jsonb := '{}'::jsonb;
  v_provider_outcome_unknown_count integer := 0;
  v_local_prepare_identity_count integer := 0;
  v_final_paid_count integer := 0;
  v_terminal_no_money_count integer := 0;
  v_pending_non_final_count integer := 0;
  v_unknown_cash_state_count integer := 0;
  v_operation_submit_attempt_count integer := 0;
  v_operation_submit_unknown_count integer := 0;
  v_operation_payload_evidence_count integer := 0;
  v_chunk_submit_attempt_count integer := 0;
BEGIN
  IF p_pay_bank_transfer_id IS NOT NULL AND v_effective_pay_batch_id IS NULL THEN
    SELECT transfer_row.pay_batch_id
    INTO v_effective_pay_batch_id
    FROM public.pay_bank_transfers AS transfer_row
    WHERE transfer_row.id = p_pay_bank_transfer_id;
  END IF;

  IF p_selection_json IS NOT NULL AND jsonb_typeof(p_selection_json) = 'object' THEN
    v_scope_type := upper(NULLIF(btrim(COALESCE(p_selection_json->>'scope_type', '')), ''));
  END IF;

  IF p_pay_bank_transfer_id IS NOT NULL THEN
    v_direct_transfer_ids := ARRAY[p_pay_bank_transfer_id];
  END IF;

  IF p_selection_json IS NOT NULL AND jsonb_typeof(p_selection_json) = 'object' THEN
    WITH raw_values AS (
      SELECT direct_transfer_values.raw_value
      FROM jsonb_array_elements_text(
        CASE
          WHEN jsonb_typeof(p_selection_json->'pay_bank_transfer_ids') = 'array' THEN p_selection_json->'pay_bank_transfer_ids'
          ELSE '[]'::jsonb
        END
      ) AS direct_transfer_values(raw_value)
      UNION ALL
      SELECT p_selection_json->>'pay_bank_transfer_id'
      WHERE p_selection_json ? 'pay_bank_transfer_id'
    ), clean_values AS (
      SELECT NULLIF(btrim(raw_values.raw_value), '') AS clean_value
      FROM raw_values
    )
    SELECT COALESCE(array_agg(clean_values.clean_value::uuid), ARRAY[]::uuid[])
    INTO v_selected_transfer_ids
    FROM clean_values
    WHERE clean_values.clean_value IS NOT NULL
      AND clean_values.clean_value ~ v_uuid_regex;
  END IF;

  v_transfer_ids := COALESCE(v_direct_transfer_ids, ARRAY[]::uuid[]) || COALESCE(v_selected_transfer_ids, ARRAY[]::uuid[]);

  IF v_effective_pay_batch_id IS NOT NULL
     AND p_selection_json IS NOT NULL
     AND jsonb_typeof(p_selection_json) = 'object'
     AND v_scope_type IS NOT NULL THEN
    BEGIN
      SELECT COALESCE(array_agg(DISTINCT selected_items.pay_bank_transfer_id) FILTER (WHERE selected_items.pay_bank_transfer_id IS NOT NULL), ARRAY[]::uuid[])
      INTO v_selected_transfer_ids
      FROM public._pay_payment_correction_selected_items(v_effective_pay_batch_id, p_selection_json, true) AS selected_items;

      v_transfer_ids := COALESCE(v_transfer_ids, ARRAY[]::uuid[]) || COALESCE(v_selected_transfer_ids, ARRAY[]::uuid[]);
    EXCEPTION
      WHEN OTHERS THEN
        v_transfer_ids := COALESCE(v_transfer_ids, ARRAY[]::uuid[]);
    END;
  END IF;

  SELECT COALESCE(array_agg(DISTINCT transfer_id_rows.transfer_id) FILTER (WHERE transfer_id_rows.transfer_id IS NOT NULL), ARRAY[]::uuid[])
  INTO v_transfer_ids
  FROM unnest(COALESCE(v_transfer_ids, ARRAY[]::uuid[])) AS transfer_id_rows(transfer_id);

  IF v_effective_pay_batch_id IS NOT NULL
     AND COALESCE(array_length(v_transfer_ids, 1), 0) = 0
     AND (p_selection_json IS NULL OR v_scope_type = 'BATCH') THEN
    SELECT COALESCE(array_agg(batch_transfer_rows.id), ARRAY[]::uuid[])
    INTO v_transfer_ids
    FROM public.pay_bank_transfers AS batch_transfer_rows
    WHERE batch_transfer_rows.pay_batch_id = v_effective_pay_batch_id;
  END IF;

  WITH target_transfers AS (
    SELECT transfer_row.*
    FROM public.pay_bank_transfers AS transfer_row
    WHERE (
        COALESCE(array_length(v_transfer_ids, 1), 0) > 0
        AND transfer_row.id = ANY(v_transfer_ids)
      )
      OR (
        COALESCE(array_length(v_transfer_ids, 1), 0) = 0
        AND v_effective_pay_batch_id IS NOT NULL
        AND transfer_row.pay_batch_id = v_effective_pay_batch_id
        AND (p_selection_json IS NULL OR v_scope_type = 'BATCH')
      )
  ), target_scopes AS (
    SELECT scope_row.*
    FROM public.banking_pay_operation_transfer_scope AS scope_row
    WHERE (v_effective_pay_batch_id IS NULL OR scope_row.pay_batch_id = v_effective_pay_batch_id)
      AND (p_operation_id IS NULL OR scope_row.operation_id = p_operation_id)
      AND (
        COALESCE(array_length(v_transfer_ids, 1), 0) = 0
        OR scope_row.pay_bank_transfer_id = ANY(v_transfer_ids)
        OR EXISTS (
          SELECT 1
          FROM target_transfers AS transfer_for_scope
          WHERE transfer_for_scope.pay_batch_id = scope_row.pay_batch_id
            AND transfer_for_scope.pay_channel = scope_row.pay_channel
            AND NULLIF(btrim(COALESCE(transfer_for_scope.transfer_group_key, '')), '') IS NOT NULL
            AND transfer_for_scope.transfer_group_key = scope_row.transfer_group_key
        )
      )
  ), evidence_rows AS (
    SELECT
      'TRANSFER'::text AS evidence_source,
      transfer_row.id AS pay_bank_transfer_id,
      transfer_row.pay_batch_id AS pay_batch_id,
      NULL::uuid AS operation_id,
      transfer_row.status AS transfer_status,
      transfer_row.rail_state AS rail_state,
      transfer_row.request_id AS request_id,
      transfer_row.payment_reference AS payment_reference,
      transfer_row.rail_tx_id AS rail_tx_id,
      COALESCE(transfer_row.rail_meta_json, '{}'::jsonb) AS meta_json,
      transfer_row.completed_at_utc AS completed_at_utc
    FROM target_transfers AS transfer_row
    UNION ALL
    SELECT
      'TRANSFER_SCOPE'::text AS evidence_source,
      scope_row.pay_bank_transfer_id AS pay_bank_transfer_id,
      scope_row.pay_batch_id AS pay_batch_id,
      scope_row.operation_id AS operation_id,
      scope_row.status AS transfer_status,
      NULL::text AS rail_state,
      scope_row.request_id AS request_id,
      scope_row.payment_reference AS payment_reference,
      NULL::text AS rail_tx_id,
      jsonb_build_object(
        'scope_id', scope_row.id::text,
        'operation_id', scope_row.operation_id::text,
        'scope_status', scope_row.status,
        'scope_request_id_present', NULLIF(btrim(COALESCE(scope_row.request_id, '')), '') IS NOT NULL,
        'scope_payment_reference_present', NULLIF(btrim(COALESCE(scope_row.payment_reference, '')), '') IS NOT NULL
      ) AS meta_json,
      NULL::timestamptz AS completed_at_utc
    FROM target_scopes AS scope_row
  ), classified_evidence_rows AS (
    SELECT
      evidence_rows.*,
      classification_rows.cash_state AS classified_cash_state,
      classification_rows.is_final_money_moved AS classified_is_final_money_moved,
      classification_rows.is_terminal_no_money AS classified_is_terminal_no_money,
      classification_rows.is_pending_non_final AS classified_is_pending_non_final,
      classification_rows.support_details_json AS classification_support_details_json,
      (
        NULLIF(btrim(COALESCE(evidence_rows.request_id, '')), '') IS NOT NULL
        OR NULLIF(btrim(COALESCE(evidence_rows.payment_reference, '')), '') IS NOT NULL
        OR evidence_rows.evidence_source = 'TRANSFER_SCOPE'
      ) AS has_local_prepare_identity,
      (
        (NULLIF(btrim(COALESCE(evidence_rows.rail_tx_id, '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(evidence_rows.rail_tx_id, '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'rail_tx_id', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'rail_tx_id', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'provider_transaction_id', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'provider_transaction_id', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'provider_payment_id', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'provider_payment_id', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'provider_reference', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'provider_reference', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(evidence_rows.meta_json #>> '{provider,transaction_id}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json #>> '{provider,transaction_id}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(evidence_rows.meta_json #>> '{provider,payment_id}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json #>> '{provider,payment_id}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
      ) AS has_provider_external_id,
      (
        (evidence_rows.meta_json ? 'provider_response' AND evidence_rows.meta_json->'provider_response' IS NOT NULL AND evidence_rows.meta_json->'provider_response' <> 'null'::jsonb AND ((jsonb_typeof(evidence_rows.meta_json->'provider_response') = 'object' AND evidence_rows.meta_json->'provider_response' <> '{}'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'provider_response') = 'array' AND evidence_rows.meta_json->'provider_response' <> '[]'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'provider_response') = 'string' AND NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'provider_response')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'provider_response')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(evidence_rows.meta_json->'provider_response') = 'number')))
        OR (evidence_rows.meta_json ? 'provider_response_json' AND evidence_rows.meta_json->'provider_response_json' IS NOT NULL AND evidence_rows.meta_json->'provider_response_json' <> 'null'::jsonb AND ((jsonb_typeof(evidence_rows.meta_json->'provider_response_json') = 'object' AND evidence_rows.meta_json->'provider_response_json' <> '{}'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'provider_response_json') = 'array' AND evidence_rows.meta_json->'provider_response_json' <> '[]'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'provider_response_json') = 'string' AND NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'provider_response_json')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'provider_response_json')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(evidence_rows.meta_json->'provider_response_json') = 'number')))
        OR (evidence_rows.meta_json ? 'submit_response' AND evidence_rows.meta_json->'submit_response' IS NOT NULL AND evidence_rows.meta_json->'submit_response' <> 'null'::jsonb AND ((jsonb_typeof(evidence_rows.meta_json->'submit_response') = 'object' AND evidence_rows.meta_json->'submit_response' <> '{}'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'submit_response') = 'array' AND evidence_rows.meta_json->'submit_response' <> '[]'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'submit_response') = 'string' AND NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'submit_response')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'submit_response')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(evidence_rows.meta_json->'submit_response') = 'number')))
        OR (evidence_rows.meta_json ? 'response_json' AND evidence_rows.meta_json->'response_json' IS NOT NULL AND evidence_rows.meta_json->'response_json' <> 'null'::jsonb AND ((jsonb_typeof(evidence_rows.meta_json->'response_json') = 'object' AND evidence_rows.meta_json->'response_json' <> '{}'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'response_json') = 'array' AND evidence_rows.meta_json->'response_json' <> '[]'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'response_json') = 'string' AND NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'response_json')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'response_json')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(evidence_rows.meta_json->'response_json') = 'number')))
        OR (evidence_rows.meta_json ? 'provider_result' AND evidence_rows.meta_json->'provider_result' IS NOT NULL AND evidence_rows.meta_json->'provider_result' <> 'null'::jsonb AND ((jsonb_typeof(evidence_rows.meta_json->'provider_result') = 'object' AND evidence_rows.meta_json->'provider_result' <> '{}'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'provider_result') = 'array' AND evidence_rows.meta_json->'provider_result' <> '[]'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'provider_result') = 'string' AND NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'provider_result')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'provider_result')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(evidence_rows.meta_json->'provider_result') = 'number')))
        OR (evidence_rows.meta_json ? 'provider_payload' AND evidence_rows.meta_json->'provider_payload' IS NOT NULL AND evidence_rows.meta_json->'provider_payload' <> 'null'::jsonb AND ((jsonb_typeof(evidence_rows.meta_json->'provider_payload') = 'object' AND evidence_rows.meta_json->'provider_payload' <> '{}'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'provider_payload') = 'array' AND evidence_rows.meta_json->'provider_payload' <> '[]'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'provider_payload') = 'string' AND NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'provider_payload')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'provider_payload')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(evidence_rows.meta_json->'provider_payload') = 'number')))
        OR (NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'http_status', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'http_status', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'provider_http_status', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'provider_http_status', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'response_status', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'response_status', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(evidence_rows.meta_json #>> '{provider,response}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json #>> '{provider,response}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
      ) AS has_provider_response,
      (
        (evidence_rows.meta_json ? 'provider_event' AND evidence_rows.meta_json->'provider_event' IS NOT NULL AND evidence_rows.meta_json->'provider_event' <> 'null'::jsonb AND ((jsonb_typeof(evidence_rows.meta_json->'provider_event') = 'object' AND evidence_rows.meta_json->'provider_event' <> '{}'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'provider_event') = 'array' AND evidence_rows.meta_json->'provider_event' <> '[]'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'provider_event') = 'string' AND NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'provider_event')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'provider_event')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(evidence_rows.meta_json->'provider_event') = 'number')))
        OR (evidence_rows.meta_json ? 'latest_provider_event' AND evidence_rows.meta_json->'latest_provider_event' IS NOT NULL AND evidence_rows.meta_json->'latest_provider_event' <> 'null'::jsonb AND ((jsonb_typeof(evidence_rows.meta_json->'latest_provider_event') = 'object' AND evidence_rows.meta_json->'latest_provider_event' <> '{}'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'latest_provider_event') = 'array' AND evidence_rows.meta_json->'latest_provider_event' <> '[]'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'latest_provider_event') = 'string' AND NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'latest_provider_event')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'latest_provider_event')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(evidence_rows.meta_json->'latest_provider_event') = 'number')))
        OR (evidence_rows.meta_json ? 'webhook' AND evidence_rows.meta_json->'webhook' IS NOT NULL AND evidence_rows.meta_json->'webhook' <> 'null'::jsonb AND ((jsonb_typeof(evidence_rows.meta_json->'webhook') = 'object' AND evidence_rows.meta_json->'webhook' <> '{}'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'webhook') = 'array' AND evidence_rows.meta_json->'webhook' <> '[]'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'webhook') = 'string' AND NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'webhook')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'webhook')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(evidence_rows.meta_json->'webhook') = 'number')))
        OR (evidence_rows.meta_json ? 'events' AND evidence_rows.meta_json->'events' IS NOT NULL AND evidence_rows.meta_json->'events' <> 'null'::jsonb AND ((jsonb_typeof(evidence_rows.meta_json->'events') = 'object' AND evidence_rows.meta_json->'events' <> '{}'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'events') = 'array' AND evidence_rows.meta_json->'events' <> '[]'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'events') = 'string' AND NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'events')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'events')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(evidence_rows.meta_json->'events') = 'number')))
        OR (evidence_rows.meta_json ? 'poll_result' AND evidence_rows.meta_json->'poll_result' IS NOT NULL AND evidence_rows.meta_json->'poll_result' <> 'null'::jsonb AND ((jsonb_typeof(evidence_rows.meta_json->'poll_result') = 'object' AND evidence_rows.meta_json->'poll_result' <> '{}'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'poll_result') = 'array' AND evidence_rows.meta_json->'poll_result' <> '[]'::jsonb) OR (jsonb_typeof(evidence_rows.meta_json->'poll_result') = 'string' AND NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'poll_result')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (evidence_rows.meta_json->'poll_result')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(evidence_rows.meta_json->'poll_result') = 'number')))
        OR (NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'poll_status', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'poll_status', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'event_id', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'event_id', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'provider_event_id', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'provider_event_id', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(evidence_rows.meta_json #>> '{provider,event_id}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json #>> '{provider,event_id}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
      ) AS has_provider_event,
      (
        lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'provider_request_sent', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'request_sent', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'provider_submit_attempted', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'request_dispatched', '')), '')) IN ('true','t','yes','y','1')
        OR (NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'request_sent_at_utc', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'request_sent_at_utc', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'provider_request_sent_at_utc', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'provider_request_sent_at_utc', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR upper(NULLIF(btrim(COALESCE(evidence_rows.transfer_status, '')), '')) IN ('REQUEST_SENT', 'PROVIDER_REQUEST_SENT', 'SUBMITTED', 'SENT', 'PROCESSING', 'ACCEPTED', 'PROVIDER_SUBMITTED', 'SUBMISSION_UNKNOWN', 'REQUEST_SENT_NO_RESPONSE')
      ) AS has_provider_request_sent,
      (
        lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'provider_outcome_unknown', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(evidence_rows.meta_json->>'request_sent_no_response', '')), '')) IN ('true','t','yes','y','1')
        OR upper(NULLIF(btrim(COALESCE(evidence_rows.transfer_status, '')), '')) IN ('UNKNOWN', 'SUBMISSION_UNKNOWN', 'PROVIDER_OUTCOME_UNKNOWN', 'REQUEST_SENT_NO_RESPONSE')
      ) AS has_provider_outcome_unknown
    FROM evidence_rows
    CROSS JOIN LATERAL public._pay_rail_state_money_movement_classify(
      evidence_rows.transfer_status,
      evidence_rows.rail_state,
      evidence_rows.meta_json,
      evidence_rows.meta_json
    ) AS classification_rows
  )
  SELECT
    (SELECT count(*)::integer FROM target_transfers),
    (SELECT count(*)::integer FROM target_scopes),
    COALESCE((count(*) FILTER (WHERE classified_evidence_rows.has_provider_external_id))::integer, 0),
    COALESCE((count(*) FILTER (WHERE classified_evidence_rows.has_provider_response))::integer, 0),
    COALESCE((count(*) FILTER (WHERE classified_evidence_rows.has_provider_event))::integer, 0),
    COALESCE((count(*) FILTER (WHERE classified_evidence_rows.has_provider_request_sent))::integer, 0),
    COALESCE((count(*) FILTER (WHERE classified_evidence_rows.has_provider_outcome_unknown))::integer, 0),
    COALESCE((count(*) FILTER (WHERE classified_evidence_rows.has_local_prepare_identity))::integer, 0),
    COALESCE((count(*) FILTER (WHERE classified_evidence_rows.classified_is_final_money_moved))::integer, 0),
    COALESCE((count(*) FILTER (WHERE classified_evidence_rows.classified_is_terminal_no_money))::integer, 0),
    COALESCE((count(*) FILTER (WHERE classified_evidence_rows.classified_is_pending_non_final))::integer, 0),
    COALESCE((count(*) FILTER (WHERE classified_evidence_rows.classified_cash_state = 'UNKNOWN'))::integer, 0)
  INTO
    v_transfer_count,
    v_scope_row_count,
    v_provider_external_id_count,
    v_provider_response_count,
    v_provider_event_count,
    v_provider_request_sent_count,
    v_provider_outcome_unknown_count,
    v_local_prepare_identity_count,
    v_final_paid_count,
    v_terminal_no_money_count,
    v_pending_non_final_count,
    v_unknown_cash_state_count
  FROM classified_evidence_rows;


  WITH canonical_event_rows AS (
    SELECT
      event_row.id AS event_id,
      event_row.pay_bank_transfer_id,
      event_row.pay_batch_id,
      event_row.provider_key,
      event_row.provider_event_transport,
      event_row.provider_signature_valid,
      event_row.provider_webhook_receipt_id,
      event_row.provider_transaction_id,
      event_row.provider_request_id,
      event_row.provider_event_key,
      event_row.rail_env,
      event_row.normalised_state,
      event_row.provider_state,
      COALESCE(event_row.raw_payload, '{}'::jsonb) AS raw_payload_json,
      receipt_row.status AS receipt_status,
      receipt_row.provider_key AS receipt_provider_key,
      receipt_row.rail_env AS receipt_rail_env,
      receipt_row.provider_event_key AS receipt_provider_event_key,
      receipt_row.signature_valid AS receipt_signature_valid
    FROM public.pay_bank_transfer_events AS event_row
    LEFT JOIN public.bank_provider_webhook_receipts AS receipt_row
      ON receipt_row.id = event_row.provider_webhook_receipt_id
    WHERE (v_effective_pay_batch_id IS NULL OR event_row.pay_batch_id = v_effective_pay_batch_id)
      AND (
        COALESCE(array_length(v_transfer_ids, 1), 0) = 0
        OR event_row.pay_bank_transfer_id = ANY(v_transfer_ids)
        OR event_row.pay_bank_transfer_id IS NULL
      )
  ), canonical_classified_events AS (
    SELECT
      canonical_event_rows.*,
      movement_rows.cash_state AS classified_cash_state,
      movement_rows.is_final_money_moved AS classified_is_final_money_moved,
      movement_rows.is_terminal_no_money AS classified_is_terminal_no_money,
      movement_rows.is_pending_non_final AS classified_is_pending_non_final,
      (
        canonical_event_rows.provider_event_transport = 'PROVIDER_WEBHOOK'
        AND canonical_event_rows.provider_signature_valid IS TRUE
        AND canonical_event_rows.provider_webhook_receipt_id IS NOT NULL
        AND canonical_event_rows.receipt_status IS NOT NULL
        AND upper(COALESCE(canonical_event_rows.receipt_status, '')) NOT IN ('SIGNATURE_INVALID', 'FAILED_FINAL', 'FAILED_RETRYABLE')
        AND canonical_event_rows.receipt_signature_valid IS TRUE
        AND canonical_event_rows.receipt_provider_key IS NOT DISTINCT FROM canonical_event_rows.provider_key
        AND canonical_event_rows.receipt_rail_env IS NOT DISTINCT FROM canonical_event_rows.rail_env
        AND NULLIF(btrim(COALESCE(canonical_event_rows.provider_event_key, '')), '') IS NOT NULL
        AND canonical_event_rows.receipt_provider_event_key IS NOT NULL
        AND canonical_event_rows.receipt_provider_event_key = canonical_event_rows.provider_event_key
      ) AS is_verified_webhook_event,
      (
        canonical_event_rows.provider_event_transport = 'FAILED_WEBHOOK_REPLAY'
        AND canonical_event_rows.provider_webhook_receipt_id IS NOT NULL
        AND canonical_event_rows.receipt_status IS NOT NULL
        AND upper(COALESCE(canonical_event_rows.receipt_status, '')) NOT IN ('SIGNATURE_INVALID', 'FAILED_FINAL')
        AND canonical_event_rows.receipt_provider_key IS NOT DISTINCT FROM canonical_event_rows.provider_key
        AND canonical_event_rows.receipt_rail_env IS NOT DISTINCT FROM canonical_event_rows.rail_env
        AND NULLIF(btrim(COALESCE(canonical_event_rows.provider_event_key, '')), '') IS NOT NULL
        AND canonical_event_rows.receipt_provider_event_key IS NOT NULL
        AND canonical_event_rows.receipt_provider_event_key = canonical_event_rows.provider_event_key
      ) AS is_valid_failed_webhook_replay_event,
      (
        canonical_event_rows.provider_event_transport = 'PROVIDER_WEBHOOK'
        AND NOT (
          canonical_event_rows.provider_signature_valid IS TRUE
          AND canonical_event_rows.provider_webhook_receipt_id IS NOT NULL
          AND canonical_event_rows.receipt_status IS NOT NULL
          AND upper(COALESCE(canonical_event_rows.receipt_status, '')) NOT IN ('SIGNATURE_INVALID', 'FAILED_FINAL', 'FAILED_RETRYABLE')
          AND canonical_event_rows.receipt_signature_valid IS TRUE
          AND canonical_event_rows.receipt_provider_key IS NOT DISTINCT FROM canonical_event_rows.provider_key
          AND canonical_event_rows.receipt_rail_env IS NOT DISTINCT FROM canonical_event_rows.rail_env
          AND NULLIF(btrim(COALESCE(canonical_event_rows.provider_event_key, '')), '') IS NOT NULL
          AND canonical_event_rows.receipt_provider_event_key IS NOT NULL
          AND canonical_event_rows.receipt_provider_event_key = canonical_event_rows.provider_event_key
        )
      ) AS is_invalid_or_unverified_webhook_event,
      (
        NULLIF(btrim(COALESCE(canonical_event_rows.provider_transaction_id, '')), '') IS NOT NULL
        OR NULLIF(btrim(COALESCE(canonical_event_rows.provider_event_key, '')), '') IS NOT NULL
      ) AS has_canonical_external_id
    FROM canonical_event_rows
    CROSS JOIN LATERAL public._pay_rail_state_money_movement_classify(
      canonical_event_rows.normalised_state,
      canonical_event_rows.provider_state,
      canonical_event_rows.raw_payload_json,
      jsonb_build_object(
        'provider_key', canonical_event_rows.provider_key,
        'provider_event_transport', canonical_event_rows.provider_event_transport,
        'provider_event_key', canonical_event_rows.provider_event_key,
        'provider_transaction_id', canonical_event_rows.provider_transaction_id,
        'provider_request_id', canonical_event_rows.provider_request_id,
        'rail_env', canonical_event_rows.rail_env
      )
    ) AS movement_rows
  )
  SELECT
    COALESCE((count(*) FILTER (WHERE canonical_classified_events.provider_event_transport = 'PROVIDER_RESPONSE'))::integer, 0),
    COALESCE((count(*) FILTER (WHERE canonical_classified_events.provider_event_transport = 'PROVIDER_POLL'))::integer, 0),
    COALESCE((count(*) FILTER (WHERE canonical_classified_events.is_verified_webhook_event))::integer, 0),
    COALESCE((count(*) FILTER (WHERE canonical_classified_events.is_valid_failed_webhook_replay_event))::integer, 0),
    COALESCE((count(*) FILTER (WHERE canonical_classified_events.is_invalid_or_unverified_webhook_event))::integer, 0),
    COALESCE((count(*) FILTER (WHERE canonical_classified_events.has_canonical_external_id))::integer, 0),
    COALESCE((count(*) FILTER (WHERE canonical_classified_events.classified_is_final_money_moved))::integer, 0),
    COALESCE((count(*) FILTER (WHERE canonical_classified_events.classified_is_terminal_no_money))::integer, 0),
    COALESCE((count(*) FILTER (WHERE canonical_classified_events.classified_is_pending_non_final))::integer, 0),
    COALESCE((count(*) FILTER (WHERE canonical_classified_events.classified_cash_state = 'UNKNOWN'))::integer, 0)
  INTO
    v_provider_response_event_count,
    v_provider_poll_event_count,
    v_verified_webhook_event_count,
    v_failed_webhook_replay_event_count,
    v_invalid_or_unverified_webhook_event_count,
    v_canonical_provider_external_id_count,
    v_canonical_final_paid_count,
    v_canonical_terminal_no_money_count,
    v_canonical_pending_non_final_count,
    v_canonical_unknown_cash_state_count
  FROM canonical_classified_events;

  WITH canonical_event_rows AS (
    SELECT
      event_row.normalised_state,
      event_row.provider_state,
      COALESCE(event_row.raw_payload, '{}'::jsonb) AS raw_payload_json,
      event_row.provider_key,
      event_row.provider_event_transport,
      event_row.provider_event_key,
      event_row.provider_transaction_id,
      event_row.provider_request_id,
      event_row.rail_env
    FROM public.pay_bank_transfer_events AS event_row
    WHERE (v_effective_pay_batch_id IS NULL OR event_row.pay_batch_id = v_effective_pay_batch_id)
      AND (
        COALESCE(array_length(v_transfer_ids, 1), 0) = 0
        OR event_row.pay_bank_transfer_id = ANY(v_transfer_ids)
        OR event_row.pay_bank_transfer_id IS NULL
      )
  ), canonical_cash_counts AS (
    SELECT movement_rows.cash_state, count(*)::integer AS state_count
    FROM canonical_event_rows
    CROSS JOIN LATERAL public._pay_rail_state_money_movement_classify(
      canonical_event_rows.normalised_state,
      canonical_event_rows.provider_state,
      canonical_event_rows.raw_payload_json,
      jsonb_build_object(
        'provider_key', canonical_event_rows.provider_key,
        'provider_event_transport', canonical_event_rows.provider_event_transport,
        'provider_event_key', canonical_event_rows.provider_event_key,
        'provider_transaction_id', canonical_event_rows.provider_transaction_id,
        'provider_request_id', canonical_event_rows.provider_request_id,
        'rail_env', canonical_event_rows.rail_env
      )
    ) AS movement_rows
    GROUP BY movement_rows.cash_state
  )
  SELECT COALESCE(jsonb_object_agg(canonical_cash_counts.cash_state, canonical_cash_counts.state_count), '{}'::jsonb)
  INTO v_canonical_event_cash_state_counts
  FROM canonical_cash_counts;

  WITH operation_payloads AS (
    SELECT
      'OPERATION'::text AS evidence_source,
      operation_rows.id AS operation_id,
      NULL::uuid AS chunk_id,
      payload_rows.payload_name,
      COALESCE(payload_rows.payload_json, '{}'::jsonb) AS payload_json
    FROM public.banking_pay_operations AS operation_rows
    CROSS JOIN LATERAL (VALUES
      ('progress_json'::text, operation_rows.progress_json),
      ('result_json'::text, operation_rows.result_json),
      ('error_json'::text, operation_rows.error_json)
    ) AS payload_rows(payload_name, payload_json)
    WHERE (p_operation_id IS NOT NULL AND operation_rows.id = p_operation_id)
       OR (
         p_operation_id IS NULL
         AND v_effective_pay_batch_id IS NOT NULL
         AND operation_rows.pay_batch_id = v_effective_pay_batch_id
         AND upper(COALESCE(operation_rows.operation_type, '')) LIKE '%PAY%'
       )
    UNION ALL
    SELECT
      'CHUNK'::text AS evidence_source,
      operation_rows.id AS operation_id,
      chunk_rows.id AS chunk_id,
      payload_rows.payload_name,
      COALESCE(payload_rows.payload_json, '{}'::jsonb) AS payload_json
    FROM public.banking_pay_operation_chunks AS chunk_rows
    JOIN public.banking_pay_operations AS operation_rows
      ON operation_rows.id = chunk_rows.operation_id
    CROSS JOIN LATERAL (VALUES
      ('payload_json'::text, chunk_rows.payload_json),
      ('result_json'::text, chunk_rows.result_json),
      ('error_json'::text, chunk_rows.error_json)
    ) AS payload_rows(payload_name, payload_json)
    WHERE (p_operation_id IS NOT NULL AND chunk_rows.operation_id = p_operation_id)
       OR (
         p_operation_id IS NULL
         AND v_effective_pay_batch_id IS NOT NULL
         AND operation_rows.pay_batch_id = v_effective_pay_batch_id
       )
  ), operation_payload_flags AS (
    SELECT
      operation_payloads.evidence_source,
      operation_payloads.operation_id,
      operation_payloads.chunk_id,
      operation_payloads.payload_name,
      operation_payloads.payload_json,
      (
        lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'provider_request_sent', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'providerRequestSent', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'provider_request_sent_confirmed', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'providerRequestSentConfirmed', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'request_sent', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'requestSent', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'request_dispatched', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'requestDispatched', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'provider_submit_attempted', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'providerSubmitAttempted', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider_submit_diagnostic,provider_request_sent}', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{providerSubmitDiagnostic,providerRequestSent}', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider_evidence,provider_request_sent}', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{providerEvidence,providerRequestSent}', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{diagnostic,provider_request_sent}', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{diagnostic,providerRequestSent}', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{outcome,provider_request_sent}', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{outcome,providerRequestSent}', '')), '')) IN ('true','t','yes','y','1')
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'request_sent_at_utc', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'request_sent_at_utc', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'provider_request_sent_at_utc', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'provider_request_sent_at_utc', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'requestSentAtUtc', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'requestSentAtUtc', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'providerRequestSentAtUtc', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'providerRequestSentAtUtc', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider_submit_diagnostic,request_sent_at_utc}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider_submit_diagnostic,request_sent_at_utc}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{providerSubmitDiagnostic,requestSentAtUtc}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{providerSubmitDiagnostic,requestSentAtUtc}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (
          NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'provider_request_sent_count', '')), '') ~ '^[0-9]+(\.[0-9]+)?$'
          AND (operation_payloads.payload_json->>'provider_request_sent_count')::numeric > 0
        )
        OR (
          NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'providerRequestSentCount', '')), '') ~ '^[0-9]+(\.[0-9]+)?$'
          AND (operation_payloads.payload_json->>'providerRequestSentCount')::numeric > 0
        )
        OR (
          NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'provider_call_sent_count', '')), '') ~ '^[0-9]+(\.[0-9]+)?$'
          AND (operation_payloads.payload_json->>'provider_call_sent_count')::numeric > 0
        )
        OR (
          NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'providerCallSentCount', '')), '') ~ '^[0-9]+(\.[0-9]+)?$'
          AND (operation_payloads.payload_json->>'providerCallSentCount')::numeric > 0
        )
        OR (
          NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider_submit_diagnostic,provider_request_sent_count}', '')), '') ~ '^[0-9]+(\.[0-9]+)?$'
          AND (operation_payloads.payload_json #>> '{provider_submit_diagnostic,provider_request_sent_count}')::numeric > 0
        )
        OR (
          NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{providerSubmitDiagnostic,providerRequestSentCount}', '')), '') ~ '^[0-9]+(\.[0-9]+)?$'
          AND (operation_payloads.payload_json #>> '{providerSubmitDiagnostic,providerRequestSentCount}')::numeric > 0
        )
      ) AS has_provider_request_sent,
      (
        lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'provider_outcome_unknown', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'providerOutcomeUnknown', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'request_sent_no_response', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'requestSentNoResponse', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider_submit_diagnostic,provider_outcome_unknown}', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{providerSubmitDiagnostic,providerOutcomeUnknown}', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{diagnostic,provider_outcome_unknown}', '')), '')) IN ('true','t','yes','y','1')
        OR lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{diagnostic,providerOutcomeUnknown}', '')), '')) IN ('true','t','yes','y','1')
        OR upper(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'code', '')), '')) IN ('REQUEST_SENT_NO_RESPONSE', 'PROVIDER_OUTCOME_UNKNOWN', 'SUBMISSION_UNKNOWN')
        OR upper(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'error_code', '')), '')) IN ('REQUEST_SENT_NO_RESPONSE', 'PROVIDER_OUTCOME_UNKNOWN', 'SUBMISSION_UNKNOWN')
        OR upper(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'status', '')), '')) IN ('REQUEST_SENT_NO_RESPONSE', 'PROVIDER_OUTCOME_UNKNOWN', 'SUBMISSION_UNKNOWN')
        OR upper(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'outcome', '')), '')) IN ('REQUEST_SENT_NO_RESPONSE', 'PROVIDER_OUTCOME_UNKNOWN', 'SUBMISSION_UNKNOWN')
        OR upper(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{error,code}', '')), '')) IN ('REQUEST_SENT_NO_RESPONSE', 'PROVIDER_OUTCOME_UNKNOWN', 'SUBMISSION_UNKNOWN')
        OR upper(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider_submit_diagnostic,code}', '')), '')) IN ('REQUEST_SENT_NO_RESPONSE', 'PROVIDER_OUTCOME_UNKNOWN', 'SUBMISSION_UNKNOWN')
        OR upper(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{providerSubmitDiagnostic,code}', '')), '')) IN ('REQUEST_SENT_NO_RESPONSE', 'PROVIDER_OUTCOME_UNKNOWN', 'SUBMISSION_UNKNOWN')
      ) AS has_provider_outcome_unknown,
      (
        (NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'rail_tx_id', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'rail_tx_id', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'railTxId', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'railTxId', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'provider_transaction_id', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'provider_transaction_id', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'providerTransactionId', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'providerTransactionId', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'provider_payment_id', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'provider_payment_id', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'providerPaymentId', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json->>'providerPaymentId', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider,transaction_id}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider,transaction_id}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider,transactionId}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider,transactionId}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider,payment_id}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider,payment_id}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider,paymentId}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider,paymentId}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider_submit_diagnostic,provider_transaction_id}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider_submit_diagnostic,provider_transaction_id}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider_submit_diagnostic,provider_payment_id}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{provider_submit_diagnostic,provider_payment_id}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{providerSubmitDiagnostic,providerTransactionId}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{providerSubmitDiagnostic,providerTransactionId}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{providerSubmitDiagnostic,providerPaymentId}', '')), '') IS NOT NULL AND lower(NULLIF(btrim(COALESCE(operation_payloads.payload_json #>> '{providerSubmitDiagnostic,providerPaymentId}', '')), '')) NOT IN ('null','false','true','none','n/a','na','local','local_only'))
        OR (operation_payloads.payload_json ? 'provider_response' AND operation_payloads.payload_json->'provider_response' IS NOT NULL AND operation_payloads.payload_json->'provider_response' <> 'null'::jsonb AND ((jsonb_typeof(operation_payloads.payload_json->'provider_response') = 'object' AND operation_payloads.payload_json->'provider_response' <> '{}'::jsonb) OR (jsonb_typeof(operation_payloads.payload_json->'provider_response') = 'array' AND operation_payloads.payload_json->'provider_response' <> '[]'::jsonb) OR (jsonb_typeof(operation_payloads.payload_json->'provider_response') = 'string' AND NULLIF(btrim(trim(both '"' from (operation_payloads.payload_json->'provider_response')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (operation_payloads.payload_json->'provider_response')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(operation_payloads.payload_json->'provider_response') = 'number')))
        OR (operation_payloads.payload_json ? 'provider_response_json' AND operation_payloads.payload_json->'provider_response_json' IS NOT NULL AND operation_payloads.payload_json->'provider_response_json' <> 'null'::jsonb AND ((jsonb_typeof(operation_payloads.payload_json->'provider_response_json') = 'object' AND operation_payloads.payload_json->'provider_response_json' <> '{}'::jsonb) OR (jsonb_typeof(operation_payloads.payload_json->'provider_response_json') = 'array' AND operation_payloads.payload_json->'provider_response_json' <> '[]'::jsonb) OR (jsonb_typeof(operation_payloads.payload_json->'provider_response_json') = 'string' AND NULLIF(btrim(trim(both '"' from (operation_payloads.payload_json->'provider_response_json')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (operation_payloads.payload_json->'provider_response_json')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(operation_payloads.payload_json->'provider_response_json') = 'number')))
        OR (operation_payloads.payload_json ? 'submit_response' AND operation_payloads.payload_json->'submit_response' IS NOT NULL AND operation_payloads.payload_json->'submit_response' <> 'null'::jsonb AND ((jsonb_typeof(operation_payloads.payload_json->'submit_response') = 'object' AND operation_payloads.payload_json->'submit_response' <> '{}'::jsonb) OR (jsonb_typeof(operation_payloads.payload_json->'submit_response') = 'array' AND operation_payloads.payload_json->'submit_response' <> '[]'::jsonb) OR (jsonb_typeof(operation_payloads.payload_json->'submit_response') = 'string' AND NULLIF(btrim(trim(both '"' from (operation_payloads.payload_json->'submit_response')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (operation_payloads.payload_json->'submit_response')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(operation_payloads.payload_json->'submit_response') = 'number')))
        OR (operation_payloads.payload_json ? 'response_json' AND operation_payloads.payload_json->'response_json' IS NOT NULL AND operation_payloads.payload_json->'response_json' <> 'null'::jsonb AND ((jsonb_typeof(operation_payloads.payload_json->'response_json') = 'object' AND operation_payloads.payload_json->'response_json' <> '{}'::jsonb) OR (jsonb_typeof(operation_payloads.payload_json->'response_json') = 'array' AND operation_payloads.payload_json->'response_json' <> '[]'::jsonb) OR (jsonb_typeof(operation_payloads.payload_json->'response_json') = 'string' AND NULLIF(btrim(trim(both '"' from (operation_payloads.payload_json->'response_json')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (operation_payloads.payload_json->'response_json')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(operation_payloads.payload_json->'response_json') = 'number')))
        OR (operation_payloads.payload_json ? 'provider_result' AND operation_payloads.payload_json->'provider_result' IS NOT NULL AND operation_payloads.payload_json->'provider_result' <> 'null'::jsonb AND ((jsonb_typeof(operation_payloads.payload_json->'provider_result') = 'object' AND operation_payloads.payload_json->'provider_result' <> '{}'::jsonb) OR (jsonb_typeof(operation_payloads.payload_json->'provider_result') = 'array' AND operation_payloads.payload_json->'provider_result' <> '[]'::jsonb) OR (jsonb_typeof(operation_payloads.payload_json->'provider_result') = 'string' AND NULLIF(btrim(trim(both '"' from (operation_payloads.payload_json->'provider_result')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (operation_payloads.payload_json->'provider_result')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(operation_payloads.payload_json->'provider_result') = 'number')))
        OR (operation_payloads.payload_json ? 'provider_payload' AND operation_payloads.payload_json->'provider_payload' IS NOT NULL AND operation_payloads.payload_json->'provider_payload' <> 'null'::jsonb AND ((jsonb_typeof(operation_payloads.payload_json->'provider_payload') = 'object' AND operation_payloads.payload_json->'provider_payload' <> '{}'::jsonb) OR (jsonb_typeof(operation_payloads.payload_json->'provider_payload') = 'array' AND operation_payloads.payload_json->'provider_payload' <> '[]'::jsonb) OR (jsonb_typeof(operation_payloads.payload_json->'provider_payload') = 'string' AND NULLIF(btrim(trim(both '"' from (operation_payloads.payload_json->'provider_payload')::text)), '') IS NOT NULL AND lower(NULLIF(btrim(trim(both '"' from (operation_payloads.payload_json->'provider_payload')::text)), '')) NOT IN ('null','false','true','none','n/a','na')) OR (jsonb_typeof(operation_payloads.payload_json->'provider_payload') = 'number')))
      ) AS has_provider_payload_evidence
    FROM operation_payloads
  )
  SELECT
    COALESCE((count(*) FILTER (WHERE operation_payload_flags.evidence_source = 'OPERATION' AND operation_payload_flags.has_provider_request_sent))::integer, 0),
    COALESCE((count(*) FILTER (WHERE operation_payload_flags.has_provider_outcome_unknown))::integer, 0),
    COALESCE((count(*) FILTER (WHERE operation_payload_flags.has_provider_payload_evidence))::integer, 0),
    COALESCE((count(*) FILTER (WHERE operation_payload_flags.evidence_source = 'CHUNK' AND operation_payload_flags.has_provider_request_sent))::integer, 0)
  INTO
    v_operation_submit_attempt_count,
    v_operation_submit_unknown_count,
    v_operation_payload_evidence_count,
    v_chunk_submit_attempt_count
  FROM operation_payload_flags;

  v_operation_submit_attempt_count := COALESCE(v_operation_submit_attempt_count, 0) + COALESCE(v_chunk_submit_attempt_count, 0);

  provider_external_id_present := (v_provider_external_id_count + v_canonical_provider_external_id_count) > 0;
  provider_response_present := v_provider_response_count > 0
    OR v_operation_payload_evidence_count > 0
    OR v_provider_response_event_count > 0;
  provider_event_present := v_provider_poll_event_count > 0
    OR v_verified_webhook_event_count > 0
    OR v_failed_webhook_replay_event_count > 0;
  provider_request_sent := provider_external_id_present
    OR provider_response_present
    OR provider_event_present
    OR v_operation_submit_attempt_count > 0;
  provider_submitted := provider_request_sent;
  local_prepared_only := (
    provider_submitted = false
    AND (
      v_local_prepare_identity_count > 0
      OR v_transfer_count > 0
      OR v_scope_row_count > 0
    )
  );

  v_final_paid_count := v_final_paid_count + v_canonical_final_paid_count;
  v_terminal_no_money_count := v_terminal_no_money_count + v_canonical_terminal_no_money_count;
  v_pending_non_final_count := v_pending_non_final_count + v_canonical_pending_non_final_count;
  v_unknown_cash_state_count := v_unknown_cash_state_count + v_canonical_unknown_cash_state_count;

  IF v_final_paid_count > 0 THEN
    cash_state := 'FINAL_PAID';
  ELSIF v_terminal_no_money_count > 0 THEN
    cash_state := 'TERMINAL_NO_MONEY';
  ELSIF v_pending_non_final_count > 0 THEN
    cash_state := 'PENDING_NON_FINAL';
  ELSIF v_unknown_cash_state_count > 0 OR provider_request_sent THEN
    cash_state := 'UNKNOWN';
  ELSE
    cash_state := 'NO_TRANSFER_EVIDENCE';
  END IF;

  IF provider_request_sent AND (v_provider_outcome_unknown_count > 0 OR v_operation_submit_unknown_count > 0 OR cash_state = 'UNKNOWN') THEN
    evidence_class := 'PROVIDER_OUTCOME_UNKNOWN';
  ELSIF provider_event_present THEN
    evidence_class := 'PROVIDER_EVENT_PRESENT';
  ELSIF provider_response_present THEN
    evidence_class := 'PROVIDER_RESPONSE_PRESENT';
  ELSIF provider_external_id_present THEN
    evidence_class := 'PROVIDER_EXTERNAL_ID_PRESENT';
  ELSIF provider_request_sent THEN
    evidence_class := 'PROVIDER_REQUEST_SENT';
  ELSIF local_prepared_only THEN
    evidence_class := 'LOCAL_PREPARED_ONLY';
  ELSE
    evidence_class := 'NO_PROVIDER_EVIDENCE';
  END IF;

  blocker_code := CASE
    WHEN evidence_class = 'PROVIDER_OUTCOME_UNKNOWN' OR (provider_submitted AND cash_state = 'UNKNOWN') THEN 'PAYMENT_OUTCOME_UNKNOWN_CHECK_PROVIDER'
    WHEN provider_submitted AND cash_state = 'PENDING_NON_FINAL' THEN 'PROVIDER_CANCELLATION_REQUIRED_BEFORE_UNWIND'
    ELSE NULL::text
  END;

  reason := CASE evidence_class
    WHEN 'PROVIDER_EVENT_PRESENT' THEN 'Provider event/webhook/poll evidence is present.'
    WHEN 'PROVIDER_RESPONSE_PRESENT' THEN 'Provider response evidence is present.'
    WHEN 'PROVIDER_EXTERNAL_ID_PRESENT' THEN 'Provider external transaction/payment ID evidence is present.'
    WHEN 'PROVIDER_OUTCOME_UNKNOWN' THEN 'Provider request may have been sent but final outcome is unknown; check/poll provider before retry or unwind.'
    WHEN 'PROVIDER_REQUEST_SENT' THEN 'Provider request-sent evidence is present.'
    WHEN 'LOCAL_PREPARED_ONLY' THEN 'Only local CloudTMS preparation artefacts are present; no provider submission evidence was found.'
    ELSE 'No provider submission evidence was found.'
  END;

  support_details_json := jsonb_build_object(
    'pay_batch_id', CASE WHEN v_effective_pay_batch_id IS NULL THEN NULL ELSE v_effective_pay_batch_id::text END,
    'pay_bank_transfer_id', CASE WHEN p_pay_bank_transfer_id IS NULL THEN NULL ELSE p_pay_bank_transfer_id::text END,
    'operation_id', CASE WHEN p_operation_id IS NULL THEN NULL ELSE p_operation_id::text END,
    'scope_type', v_scope_type,
    'transfer_ids', COALESCE((SELECT jsonb_agg(transfer_id_values.transfer_id::text ORDER BY transfer_id_values.transfer_id::text) FROM unnest(COALESCE(v_transfer_ids, ARRAY[]::uuid[])) AS transfer_id_values(transfer_id)), '[]'::jsonb),
    'transfer_count', v_transfer_count,
    'scope_row_count', v_scope_row_count,
    'provider_external_id_count', v_provider_external_id_count,
    'canonical_provider_external_id_count', v_canonical_provider_external_id_count,
    'provider_response_count', v_provider_response_count,
    'provider_response_event_count', v_provider_response_event_count,
    'provider_event_count', v_provider_event_count,
    'provider_poll_event_count', v_provider_poll_event_count,
    'verified_webhook_event_count', v_verified_webhook_event_count,
    'failed_webhook_replay_event_count', v_failed_webhook_replay_event_count,
    'invalid_or_unverified_webhook_event_count', v_invalid_or_unverified_webhook_event_count,
    'canonical_event_cash_state_counts', v_canonical_event_cash_state_counts,
    'provider_request_sent_count', v_provider_request_sent_count,
    'provider_outcome_unknown_count', v_provider_outcome_unknown_count,
    'local_prepare_identity_count', v_local_prepare_identity_count,
    'local_prepared_only', local_prepared_only,
    'local_prepared_only_basis', CASE
      WHEN local_prepared_only THEN jsonb_build_array(
        CASE WHEN v_local_prepare_identity_count > 0 THEN 'local_request_or_scope_identity' ELSE NULL END,
        CASE WHEN v_transfer_count > 0 THEN 'local_transfer_row' ELSE NULL END,
        CASE WHEN v_scope_row_count > 0 THEN 'local_transfer_scope_row' ELSE NULL END
      )
      ELSE '[]'::jsonb
    END,
    'final_paid_count', v_final_paid_count,
    'terminal_no_money_count', v_terminal_no_money_count,
    'pending_non_final_count', v_pending_non_final_count,
    'unknown_cash_state_count', v_unknown_cash_state_count,
    'operation_submit_attempt_count', v_operation_submit_attempt_count,
    'operation_submit_unknown_count', v_operation_submit_unknown_count,
    'operation_payload_evidence_count', v_operation_payload_evidence_count,
    'provider_request_sent_transfer_status_count_not_authoritative', v_provider_request_sent_count,
    'local_only_reference_count', v_local_prepare_identity_count,
    'provider_external_id_count_total', v_provider_external_id_count + v_canonical_provider_external_id_count,
    'local_artifacts_do_not_count_as_provider_submission', jsonb_build_array('request_id', 'payment_reference', 'bulk_reference', 'operation_id', 'scope_id', 'auth_request_id', 'local_idempotency_key', 'local_only_rail_meta_json', 'pending_local_transfer_rows')
  );

  RETURN NEXT;
END;
$function$;

-- _pay_bank_transfer_status_display_label(text,text,text)
CREATE OR REPLACE FUNCTION public._pay_bank_transfer_status_display_label(p_status text, p_rail_state text, p_cash_state text DEFAULT NULL::text)
 RETURNS text
 LANGUAGE plpgsql
 IMMUTABLE PARALLEL SAFE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_status text := NULLIF(REPLACE(REPLACE(UPPER(BTRIM(COALESCE(p_status, ''))), '-', '_'), ' ', '_'), '');
  v_rail_state text := NULLIF(REPLACE(REPLACE(UPPER(BTRIM(COALESCE(p_rail_state, ''))), '-', '_'), ' ', '_'), '');
  v_cash_state text := NULLIF(REPLACE(REPLACE(UPPER(BTRIM(COALESCE(p_cash_state, ''))), '-', '_'), ' ', '_'), '');
  v_primary_state text := COALESCE(v_status, v_rail_state);
BEGIN
  IF v_cash_state = 'FINAL_PAID' THEN
    RETURN 'Payment completed successfully';
  END IF;

  IF v_cash_state = 'TERMINAL_NO_MONEY' THEN
    IF v_primary_state IN ('CANCELLED', 'CANCELED', 'CANCELLED_BEFORE_RELEASE', 'CANCELED_BEFORE_RELEASE') THEN
      RETURN 'Payment cancelled';
    END IF;

    IF v_primary_state IN ('RETURNED', 'REVERTED', 'REVERSED') THEN
      RETURN 'Payment returned';
    END IF;

    IF v_primary_state IN ('REJECTED', 'DECLINED') THEN
      RETURN 'Payment rejected';
    END IF;

    RETURN 'Payment failed';
  END IF;

  IF v_cash_state = 'PENDING_NON_FINAL' THEN
    RETURN 'Submitted / pending bank confirmation';
  END IF;

  IF v_cash_state = 'UNKNOWN' THEN
    RETURN 'Check required';
  END IF;

  IF v_primary_state IS NULL THEN
    RETURN 'Check required';
  END IF;

  IF v_primary_state IN ('COMPLETED', 'COMPLETE', 'PAID', 'SETTLED', 'SUCCESS', 'SUCCESSFUL', 'SUCCEEDED', 'DONE') THEN
    RETURN 'Payment completed successfully';
  END IF;

  IF v_primary_state IN ('PENDING', 'SUBMITTED', 'SENT', 'PROCESSING', 'QUEUED', 'ACCEPTED', 'CREATED', 'SCHEDULED', 'IN_PROGRESS', 'WAITING', 'WAITING_BANK_CONFIRM', 'SUBMITTED_NOT_COMMITTED') THEN
    RETURN 'Submitted / pending bank confirmation';
  END IF;

  IF v_primary_state IN ('FAILED', 'FAILURE', 'ERROR') THEN
    RETURN 'Payment failed';
  END IF;

  IF v_primary_state IN ('REJECTED', 'DECLINED') THEN
    RETURN 'Payment rejected';
  END IF;

  IF v_primary_state IN ('CANCELLED', 'CANCELED', 'CANCELLED_BEFORE_RELEASE', 'CANCELED_BEFORE_RELEASE') THEN
    RETURN 'Payment cancelled';
  END IF;

  IF v_primary_state IN ('RETURNED', 'REVERTED', 'REVERSED') THEN
    RETURN 'Payment returned';
  END IF;

  IF v_primary_state IN ('BLOCKED_FUNDS', 'INSUFFICIENT_FUNDS', 'FUNDS_REQUIRED') THEN
    RETURN 'Payment blocked - funds required';
  END IF;

  IF v_primary_state IN ('UNMATCHED', 'UNMATCHED_REVIEW_REQUIRED', 'REVIEW_REQUIRED', 'CHECK_REQUIRED', 'UNKNOWN', 'UNCLEAR') THEN
    RETURN 'Check required';
  END IF;

  RETURN 'Check required';
END;
$function$;

-- _pay_bank_transfers_normalise_status_biu()
CREATE OR REPLACE FUNCTION public._pay_bank_transfers_normalise_status_biu()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
BEGIN
  IF NEW.status IS NOT NULL THEN
    NEW.status := upper(btrim(NEW.status));

    IF NEW.status = 'CANCELED' THEN
      NEW.status := 'CANCELLED';
    END IF;
  END IF;

  RETURN NEW;
END;
$function$;

-- _pay_batch_bank_payment_projection_rows(uuid,text)
CREATE OR REPLACE FUNCTION public._pay_batch_bank_payment_projection_rows(p_pay_batch_id uuid, p_scope text DEFAULT 'ALL'::text)
 RETURNS TABLE(pay_batch_id uuid, requested_scope text, payment_group_ordinal bigint, stable_order_key text, representative_pay_batch_item_id uuid, pay_batch_item_count bigint, pay_batch_candidate_id uuid, candidate_id uuid, pay_channel text, umbrella_id uuid, paye_net_required boolean, has_effective_paye_input boolean, effective_paye_net_input_id uuid, effective_paye_net_input_source text, effective_paye_net_input_amount numeric, final_frozen_bank_amount numeric, paye_net_classification text, is_paye_net_state_row boolean, is_positive_bank_payment boolean, transfer_id uuid, payment_reference text, payee_name text, sort_code text, account_number text, account_type text, amount numeric, currency text, rail_provider text, rail_env text, request_id text, transfer_group_key text, grouping_mode_used text, week_ending_bucket date, payee_entity_kind text, payee_entity_id uuid, bank_details_hash_snapshot text, payout_instruction_snapshot_json jsonb, paye_net_state_hash text, bank_payment_projection_hash text)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_scope text := UPPER(BTRIM(COALESCE(p_scope, 'ALL')));
  v_batch_kind_fixed text := NULL::text;
  v_rail_provider_snapshot text := NULL::text;
  v_rail_env_snapshot text := NULL::text;
BEGIN
  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'PAY_BATCH_BANK_PAYMENT_PROJECTION_BATCH_ID_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_BATCH_BANK_PAYMENT_PROJECTION_BATCH_ID_REQUIRED'
            )::text;
  END IF;

  IF v_scope NOT IN ('ALL', 'PAYE', 'UMBRELLA', 'LOANS') THEN
    RAISE EXCEPTION 'PAY_BATCH_BANK_PAYMENT_PROJECTION_SCOPE_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_BATCH_BANK_PAYMENT_PROJECTION_SCOPE_INVALID',
              'pay_batch_id', p_pay_batch_id::text,
              'scope', p_scope,
              'allowed_scopes', jsonb_build_array('ALL', 'PAYE', 'UMBRELLA', 'LOANS')
            )::text;
  END IF;

  SELECT
    UPPER(BTRIM(COALESCE(batch_row.batch_kind_fixed, ''))),
    batch_row.rail_provider_snapshot,
    batch_row.rail_env_snapshot
  INTO
    v_batch_kind_fixed,
    v_rail_provider_snapshot,
    v_rail_env_snapshot
  FROM public.pay_batches AS batch_row
  WHERE batch_row.id = p_pay_batch_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_BATCH_BANK_PAYMENT_PROJECTION_BATCH_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_BATCH_BANK_PAYMENT_PROJECTION_BATCH_NOT_FOUND',
              'pay_batch_id', p_pay_batch_id::text
            )::text;
  END IF;

  RETURN QUERY
  WITH source_items AS (
    SELECT
      batch_item.id AS pay_batch_item_id,
      batch_candidate.id AS pay_batch_candidate_id,
      batch_candidate.candidate_id,
      batch_item.pay_channel AS original_pay_channel,
      CASE
        WHEN v_batch_kind_fixed = 'LOANS' THEN 'PAYE'
        ELSE batch_item.pay_channel
      END AS effective_pay_channel,
      batch_item.item_type,
      ROUND(COALESCE(batch_item.amount_inc_vat, batch_item.amount_ex_vat, 0), 2)::numeric(14,2) AS source_item_amount,
      batch_candidate.net_bank_amount AS candidate_net_bank_amount,
      batch_item.payout_instruction_snapshot_json,
      UPPER(NULLIF(BTRIM(COALESCE(batch_item.payout_instruction_snapshot_json->>'payee_entity_kind', '')), '')) AS frozen_payee_entity_kind,
      COALESCE(batch_item.payout_instruction_snapshot_json->>'payee_entity_id', '') AS frozen_payee_entity_id_text_raw,
      CASE
        WHEN NULLIF(BTRIM(COALESCE(batch_item.payout_instruction_snapshot_json->>'payee_entity_id', '')), '')
             ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN NULLIF(BTRIM(COALESCE(batch_item.payout_instruction_snapshot_json->>'payee_entity_id', '')), '')::uuid
        ELSE NULL::uuid
      END AS frozen_payee_entity_id,
      NULLIF(BTRIM(COALESCE(batch_item.payout_instruction_snapshot_json->>'beneficiary_name', '')), '') AS frozen_payee_name,
      NULLIF(BTRIM(COALESCE(batch_item.payout_instruction_snapshot_json->>'sort_code', '')), '') AS frozen_sort_code,
      NULLIF(BTRIM(COALESCE(batch_item.payout_instruction_snapshot_json->>'account_number', '')), '') AS frozen_account_number,
      NULLIF(BTRIM(COALESCE(batch_item.payout_instruction_snapshot_json->>'account_type', '')), '') AS frozen_account_type,
      NULLIF(BTRIM(COALESCE(batch_item.payout_instruction_snapshot_json->>'bank_details_hash', '')), '') AS frozen_bank_details_hash,
      CASE
        WHEN COALESCE(batch_item.payout_instruction_snapshot_json->>'week_ending_bucket', '') ~ '^\d{4}-\d{2}-\d{2}$'
          THEN batch_item.payout_instruction_snapshot_json->>'week_ending_bucket'
        ELSE 'NO_WEEK'
      END AS frozen_week_ending_bucket_key,
      CASE
        WHEN COALESCE(batch_item.payout_instruction_snapshot_json->>'week_ending_bucket', '') ~ '^\d{4}-\d{2}-\d{2}$'
          THEN (batch_item.payout_instruction_snapshot_json->>'week_ending_bucket')::date
        ELSE NULL::date
      END AS frozen_week_ending_bucket,
      CASE
        WHEN (
          CASE
            WHEN v_batch_kind_fixed = 'LOANS' THEN 'PAYE'
            ELSE batch_item.pay_channel
          END
        ) = 'PAYE'
          THEN batch_candidate.candidate_id::text
               || '|PAYE|'
               || COALESCE(
                    NULLIF(BTRIM(COALESCE(batch_item.payout_instruction_snapshot_json->>'bank_details_hash', '')), ''),
                    (
                      SELECT NULLIF(
                               BTRIM(COALESCE(fallback_item.payout_instruction_snapshot_json->>'bank_details_hash', '')),
                               ''
                             )
                      FROM public.pay_batch_items AS fallback_item
                      WHERE fallback_item.pay_batch_candidate_id = batch_candidate.id
                        AND COALESCE(fallback_item.is_voided, false) = false
                        AND fallback_item.item_type <> 'DEBT_CREATED'
                        AND fallback_item.pay_channel = 'PAYE'
                        AND NULLIF(
                              BTRIM(COALESCE(fallback_item.payout_instruction_snapshot_json->>'bank_details_hash', '')),
                              ''
                            ) IS NOT NULL
                      ORDER BY
                        CASE
                          WHEN NULLIF(
                                 BTRIM(COALESCE(fallback_item.payout_instruction_snapshot_json->>'sort_code', '')),
                                 ''
                               ) IS NOT NULL
                           AND NULLIF(
                                 BTRIM(COALESCE(fallback_item.payout_instruction_snapshot_json->>'account_number', '')),
                                 ''
                               ) IS NOT NULL
                            THEN 0
                          ELSE 1
                        END,
                        fallback_item.id
                      LIMIT 1
                    ),
                    ''
                  )
        WHEN (
          CASE
            WHEN v_batch_kind_fixed = 'LOANS' THEN 'PAYE'
            ELSE batch_item.pay_channel
          END
        ) = 'UMBRELLA'
          THEN batch_candidate.candidate_id::text
               || '|UMBRELLA|'
               || COALESCE(
                    CASE
                      WHEN COALESCE(batch_item.payout_instruction_snapshot_json->>'week_ending_bucket', '') ~ '^\d{4}-\d{2}-\d{2}$'
                        THEN batch_item.payout_instruction_snapshot_json->>'week_ending_bucket'
                      ELSE 'NO_WEEK'
                    END,
                    'NO_WEEK'
                  )
               || '|'
               || COALESCE(batch_item.payout_instruction_snapshot_json->>'payee_entity_id', '')
               || '|'
               || COALESCE(
                    NULLIF(BTRIM(COALESCE(batch_item.payout_instruction_snapshot_json->>'bank_details_hash', '')), ''),
                    ''
                  )
        ELSE batch_candidate.candidate_id::text
             || '|OTHER|'
             || COALESCE(
                  NULLIF(BTRIM(COALESCE(batch_item.payout_instruction_snapshot_json->>'bank_details_hash', '')), ''),
                  ''
                )
      END AS projected_transfer_group_key
    FROM public.pay_batch_items AS batch_item
    JOIN public.pay_batch_candidates AS batch_candidate
      ON batch_candidate.id = batch_item.pay_batch_candidate_id
    WHERE batch_candidate.pay_batch_id = p_pay_batch_id
      AND COALESCE(batch_item.is_voided, false) = false
      AND batch_item.item_type <> 'DEBT_CREATED'
      AND (
        (v_scope = 'ALL' AND batch_item.pay_channel IN ('PAYE', 'UMBRELLA'))
        OR (v_scope IN ('PAYE', 'UMBRELLA') AND batch_item.pay_channel = v_scope)
        OR (v_scope = 'LOANS' AND batch_item.item_type = 'LOAN_PAYOUT')
        OR (v_batch_kind_fixed = 'LOANS' AND batch_item.item_type = 'LOAN_PAYOUT')
      )
  ),
  grouped_source_items AS (
    SELECT
      source_item_rows.*,
      ROW_NUMBER() OVER (
        PARTITION BY source_item_rows.effective_pay_channel, source_item_rows.projected_transfer_group_key
        ORDER BY
          CASE
            WHEN source_item_rows.frozen_bank_details_hash IS NOT NULL
             AND source_item_rows.frozen_sort_code IS NOT NULL
             AND source_item_rows.frozen_account_number IS NOT NULL
              THEN 0
            ELSE 1
          END,
          source_item_rows.pay_batch_item_id
      ) AS group_item_ordinal,
      COUNT(*) OVER (
        PARTITION BY source_item_rows.effective_pay_channel, source_item_rows.projected_transfer_group_key
      ) AS grouped_item_count,
      ROUND(
        SUM(source_item_rows.source_item_amount) OVER (
          PARTITION BY source_item_rows.effective_pay_channel, source_item_rows.projected_transfer_group_key
        ),
        2
      )::numeric(14,2) AS grouped_item_amount
    FROM source_items AS source_item_rows
  ),
  representative_groups AS (
    SELECT
      grouped_item_rows.pay_batch_item_id AS representative_pay_batch_item_id,
      grouped_item_rows.grouped_item_count AS pay_batch_item_count,
      grouped_item_rows.pay_batch_candidate_id,
      grouped_item_rows.candidate_id,
      grouped_item_rows.effective_pay_channel AS pay_channel,
      grouped_item_rows.candidate_net_bank_amount,
      grouped_item_rows.grouped_item_amount,
      grouped_item_rows.payout_instruction_snapshot_json,
      grouped_item_rows.frozen_payee_entity_kind AS payee_entity_kind,
      grouped_item_rows.frozen_payee_entity_id AS payee_entity_id,
      grouped_item_rows.frozen_payee_name AS payee_name,
      grouped_item_rows.frozen_sort_code AS raw_sort_code,
      grouped_item_rows.frozen_account_number AS raw_account_number,
      grouped_item_rows.frozen_account_type AS account_type,
      grouped_item_rows.frozen_bank_details_hash AS bank_details_hash_snapshot,
      grouped_item_rows.frozen_week_ending_bucket AS week_ending_bucket,
      grouped_item_rows.projected_transfer_group_key AS transfer_group_key
    FROM grouped_source_items AS grouped_item_rows
    WHERE grouped_item_rows.group_item_ordinal = 1
  ),
  groups_with_effective_input AS (
    SELECT
      representative_group_rows.*,
      effective_input_rows.id AS effective_paye_net_input_id,
      effective_input_rows.source AS effective_paye_net_input_source,
      effective_input_rows.net_amount AS effective_paye_net_input_amount
    FROM representative_groups AS representative_group_rows
    LEFT JOIN LATERAL (
      SELECT
        paye_input_row.id,
        paye_input_row.source,
        ROUND(paye_input_row.net_amount, 2)::numeric(14,2) AS net_amount
      FROM public.pay_batch_paye_net_inputs AS paye_input_row
      WHERE paye_input_row.pay_batch_candidate_id = representative_group_rows.pay_batch_candidate_id
      ORDER BY paye_input_row.imported_at_utc DESC, paye_input_row.id DESC
      LIMIT 1
    ) AS effective_input_rows
      ON true
  ),
  projected_groups AS (
    SELECT
      input_group_rows.*,
      (
        input_group_rows.pay_channel = 'PAYE'
        AND v_batch_kind_fixed <> 'LOANS'
      ) AS paye_net_required,
      CASE
        WHEN input_group_rows.pay_channel = 'PAYE'
         AND v_batch_kind_fixed <> 'LOANS'
          THEN input_group_rows.effective_paye_net_input_id IS NOT NULL
               AND input_group_rows.effective_paye_net_input_amount IS NOT NULL
        ELSE NULL::boolean
      END AS has_effective_paye_input,
      CASE
        WHEN input_group_rows.pay_channel = 'PAYE'
          THEN ROUND(input_group_rows.candidate_net_bank_amount, 2)::numeric(14,2)
        ELSE ROUND(input_group_rows.grouped_item_amount, 2)::numeric(14,2)
      END AS final_frozen_bank_amount,
      CASE
        WHEN input_group_rows.pay_channel = 'PAYE'
         AND v_batch_kind_fixed <> 'LOANS'
         AND (
           input_group_rows.effective_paye_net_input_id IS NULL
           OR input_group_rows.effective_paye_net_input_amount IS NULL
           OR input_group_rows.candidate_net_bank_amount IS NULL
         )
          THEN 'MISSING'
        WHEN input_group_rows.pay_channel = 'PAYE'
         AND v_batch_kind_fixed <> 'LOANS'
         AND ROUND(input_group_rows.candidate_net_bank_amount, 2) = 0
          THEN 'ZERO'
        WHEN input_group_rows.pay_channel = 'PAYE'
         AND v_batch_kind_fixed <> 'LOANS'
         AND ROUND(input_group_rows.candidate_net_bank_amount, 2) > 0
          THEN 'POSITIVE'
        ELSE NULL::text
      END AS paye_net_classification,
      CASE
        WHEN REGEXP_REPLACE(COALESCE(input_group_rows.raw_sort_code, ''), '[^0-9]', '', 'g') ~ '^[0-9]{6}$'
          THEN REGEXP_REPLACE(
                 REGEXP_REPLACE(COALESCE(input_group_rows.raw_sort_code, ''), '[^0-9]', '', 'g'),
                 '^([0-9]{2})([0-9]{2})([0-9]{2})$',
                 '\1-\2-\3'
               )
        ELSE NULL::text
      END AS projected_sort_code,
      NULLIF(
        REGEXP_REPLACE(COALESCE(input_group_rows.raw_account_number, ''), '[^0-9]', '', 'g'),
        ''
      ) AS projected_account_number,
      CASE
        WHEN input_group_rows.pay_channel = 'PAYE' THEN 'Pay'
        ELSE LEFT(COALESCE(input_group_rows.payee_name, input_group_rows.candidate_id::text), 18)
      END AS projected_payment_reference,
      CASE
        WHEN input_group_rows.pay_channel = 'PAYE' THEN 'CANDIDATE_DESTINATION'
        ELSE 'ROW_BACKED_DESTINATION'
      END AS projected_grouping_mode,
      CASE
        WHEN input_group_rows.pay_channel = 'UMBRELLA'
         AND input_group_rows.payee_entity_kind = 'UMBRELLA'
          THEN input_group_rows.payee_entity_id
        ELSE NULL::uuid
      END AS projected_umbrella_id
    FROM groups_with_effective_input AS input_group_rows
  ),
  classified_groups AS (
    SELECT
      projected_group_rows.*,
      CASE
        WHEN projected_group_rows.paye_net_required
          THEN COALESCE(projected_group_rows.paye_net_classification = 'POSITIVE', false)
        ELSE COALESCE(projected_group_rows.final_frozen_bank_amount, 0) > 0
      END AS is_positive_bank_payment
    FROM projected_groups AS projected_group_rows
  ),
  ordered_groups AS (
    SELECT
      classified_group_rows.*,
      ROW_NUMBER() OVER (
        ORDER BY
          classified_group_rows.pay_channel,
          classified_group_rows.transfer_group_key,
          classified_group_rows.representative_pay_batch_item_id
      ) AS payment_group_ordinal,
      classified_group_rows.pay_channel
        || '|'
        || classified_group_rows.transfer_group_key
        || '|'
        || classified_group_rows.representative_pay_batch_item_id::text AS stable_order_key,
      ROW_NUMBER() OVER (
        PARTITION BY classified_group_rows.pay_batch_candidate_id, classified_group_rows.pay_channel
        ORDER BY
          classified_group_rows.transfer_group_key,
          classified_group_rows.representative_pay_batch_item_id
      ) AS candidate_channel_group_ordinal
    FROM classified_groups AS classified_group_rows
  ),
  paye_state_payload AS (
    SELECT
      COALESCE(
        JSONB_AGG(
          JSONB_BUILD_OBJECT(
            'pay_batch_candidate_id', ordered_group_rows.pay_batch_candidate_id::text,
            'candidate_id', ordered_group_rows.candidate_id::text,
            'classification', ordered_group_rows.paye_net_classification,
            'has_effective_input', ordered_group_rows.has_effective_paye_input,
            'effective_input_amount', ordered_group_rows.effective_paye_net_input_amount,
            'final_frozen_bank_amount', ordered_group_rows.final_frozen_bank_amount
          )
          ORDER BY
            ordered_group_rows.pay_batch_candidate_id,
            ordered_group_rows.candidate_id
        ) FILTER (
          WHERE ordered_group_rows.paye_net_required
            AND ordered_group_rows.candidate_channel_group_ordinal = 1
        ),
        '[]'::jsonb
      ) AS state_rows_json
    FROM ordered_groups AS ordered_group_rows
  ),
  bank_projection_payload AS (
    SELECT
      COALESCE(
        JSONB_AGG(
          JSONB_BUILD_OBJECT(
            'payment_group_identity', ordered_group_rows.transfer_group_key,
            'pay_batch_candidate_id', ordered_group_rows.pay_batch_candidate_id::text,
            'candidate_id', ordered_group_rows.candidate_id::text,
            'pay_channel', ordered_group_rows.pay_channel,
            'paye_net_classification', ordered_group_rows.paye_net_classification,
            'final_amount', ordered_group_rows.final_frozen_bank_amount,
            'currency', 'GBP',
            'payment_reference', ordered_group_rows.projected_payment_reference,
            'payee_name', ordered_group_rows.payee_name,
            'sort_code', ordered_group_rows.projected_sort_code,
            'account_number', ordered_group_rows.projected_account_number,
            'account_type', ordered_group_rows.account_type,
            'bank_details_hash', ordered_group_rows.bank_details_hash_snapshot,
            'payee_entity_kind', ordered_group_rows.payee_entity_kind,
            'payee_entity_id', CASE
              WHEN ordered_group_rows.payee_entity_id IS NULL THEN NULL::text
              ELSE ordered_group_rows.payee_entity_id::text
            END,
            'umbrella_id', CASE
              WHEN ordered_group_rows.projected_umbrella_id IS NULL THEN NULL::text
              ELSE ordered_group_rows.projected_umbrella_id::text
            END,
            'grouping_mode', ordered_group_rows.projected_grouping_mode,
            'week_ending_bucket', CASE
              WHEN ordered_group_rows.week_ending_bucket IS NULL THEN NULL::text
              ELSE ordered_group_rows.week_ending_bucket::text
            END,
            'rail_provider', v_rail_provider_snapshot,
            'rail_env', v_rail_env_snapshot
          )
          ORDER BY
            ordered_group_rows.payment_group_ordinal,
            ordered_group_rows.stable_order_key
        ),
        '[]'::jsonb
      ) AS projection_rows_json
    FROM ordered_groups AS ordered_group_rows
  ),
  projection_hashes AS (
    SELECT
      MD5(
        JSONB_BUILD_OBJECT(
          'pay_batch_id', p_pay_batch_id::text,
          'scope', v_scope,
          'rows', paye_state_payload_rows.state_rows_json
        )::text
      ) AS paye_net_state_hash,
      MD5(
        JSONB_BUILD_OBJECT(
          'pay_batch_id', p_pay_batch_id::text,
          'scope', v_scope,
          'rows', bank_projection_payload_rows.projection_rows_json
        )::text
      ) AS bank_payment_projection_hash
    FROM paye_state_payload AS paye_state_payload_rows
    CROSS JOIN bank_projection_payload AS bank_projection_payload_rows
  )
  SELECT
    p_pay_batch_id AS pay_batch_id,
    v_scope AS requested_scope,
    ordered_group_rows.payment_group_ordinal,
    ordered_group_rows.stable_order_key,
    ordered_group_rows.representative_pay_batch_item_id,
    ordered_group_rows.pay_batch_item_count,
    ordered_group_rows.pay_batch_candidate_id,
    ordered_group_rows.candidate_id,
    ordered_group_rows.pay_channel,
    ordered_group_rows.projected_umbrella_id AS umbrella_id,
    ordered_group_rows.paye_net_required,
    ordered_group_rows.has_effective_paye_input,
    CASE
      WHEN ordered_group_rows.paye_net_required
        THEN ordered_group_rows.effective_paye_net_input_id
      ELSE NULL::uuid
    END AS effective_paye_net_input_id,
    CASE
      WHEN ordered_group_rows.paye_net_required
        THEN ordered_group_rows.effective_paye_net_input_source
      ELSE NULL::text
    END AS effective_paye_net_input_source,
    CASE
      WHEN ordered_group_rows.paye_net_required
        THEN ordered_group_rows.effective_paye_net_input_amount
      ELSE NULL::numeric
    END AS effective_paye_net_input_amount,
    ordered_group_rows.final_frozen_bank_amount,
    ordered_group_rows.paye_net_classification,
    (
      ordered_group_rows.paye_net_required
      AND ordered_group_rows.candidate_channel_group_ordinal = 1
    ) AS is_paye_net_state_row,
    ordered_group_rows.is_positive_bank_payment,
    NULL::uuid AS transfer_id,
    ordered_group_rows.projected_payment_reference AS payment_reference,
    ordered_group_rows.payee_name,
    ordered_group_rows.projected_sort_code AS sort_code,
    ordered_group_rows.projected_account_number AS account_number,
    ordered_group_rows.account_type,
    ordered_group_rows.final_frozen_bank_amount AS amount,
    'GBP'::text AS currency,
    v_rail_provider_snapshot AS rail_provider,
    v_rail_env_snapshot AS rail_env,
    NULL::text AS request_id,
    ordered_group_rows.transfer_group_key,
    ordered_group_rows.projected_grouping_mode AS grouping_mode_used,
    ordered_group_rows.week_ending_bucket,
    ordered_group_rows.payee_entity_kind,
    ordered_group_rows.payee_entity_id,
    ordered_group_rows.bank_details_hash_snapshot,
    ordered_group_rows.payout_instruction_snapshot_json,
    projection_hash_rows.paye_net_state_hash,
    projection_hash_rows.bank_payment_projection_hash
  FROM ordered_groups AS ordered_group_rows
  CROSS JOIN projection_hashes AS projection_hash_rows
  ORDER BY ordered_group_rows.payment_group_ordinal;
END;
$function$;

-- _pay_batch_item_breakdown_kind_guard_v1()
CREATE OR REPLACE FUNCTION public._pay_batch_item_breakdown_kind_guard_v1()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
DECLARE
  v_item_type text;
BEGIN
  SELECT upper(btrim(coalesce(batch_item.item_type, '')))
  INTO v_item_type
  FROM public.pay_batch_items AS batch_item
  WHERE batch_item.id = NEW.pay_batch_item_id;

  IF v_item_type IN (
    'OVERPAYMENT_RECOVERY',
    'LOAN_REPAYMENT',
    'MANUAL_DEBT_RECOVERY',
    'MANUAL_CREDIT_PAYOUT',
    'LOAN_PAYOUT',
    'UNDERPAYMENT_PAYMENT',
    'DEBT_CREATED'
  ) THEN
    NEW.line_kind := v_item_type;
  END IF;

  RETURN NEW;
END;
$function$;

-- _pay_batch_item_economic_components(uuid,uuid[])
CREATE OR REPLACE FUNCTION public._pay_batch_item_economic_components(p_pay_batch_id uuid DEFAULT NULL::uuid, p_pay_batch_item_ids uuid[] DEFAULT NULL::uuid[])
 RETURNS TABLE(pay_batch_id uuid, pay_batch_item_id uuid, timesheet_id uuid, item_type text, key_type text, key_value text, source_amount_ex_vat numeric, source_amount_inc_vat numeric, target_amount_ex_vat numeric, key_resolution_source text, key_resolution_failure_reason text)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  IF p_pay_batch_id IS NULL
     AND COALESCE(array_length(p_pay_batch_item_ids, 1), 0) = 0 THEN
    RAISE EXCEPTION '_pay_batch_item_economic_components requires p_pay_batch_id or p_pay_batch_item_ids';
  END IF;

  RETURN QUERY
  WITH input_item_ids AS (
    SELECT DISTINCT input_ids.item_id
    FROM unnest(COALESCE(p_pay_batch_item_ids, ARRAY[]::uuid[])) AS input_ids(item_id)
    WHERE input_ids.item_id IS NOT NULL
  ),
  base_items AS (
    SELECT
      pay_batch_candidate_row.pay_batch_id AS batch_id,
      pay_batch_item_row.id AS batch_item_id,
      pay_batch_item_row.timesheet_id AS item_timesheet_id,
      UPPER(NULLIF(BTRIM(COALESCE(pay_batch_item_row.item_type, '')), '')) AS item_type_norm,
      pay_batch_item_row.item_type AS item_type_raw,
      pay_batch_item_row.segment_key AS item_segment_key,
      pay_batch_item_row.source_ref AS item_source_ref,
      pay_batch_item_row.amount_ex_vat AS item_amount_ex_vat,
      pay_batch_item_row.amount_inc_vat AS item_amount_inc_vat,
      pay_batch_item_row.finance_case_id AS item_finance_case_id,
      pay_batch_item_row.finance_component_id AS item_finance_component_id,
      pay_batch_item_row.frozen_target_amount_ex_vat AS item_frozen_target_amount_ex_vat,
      pay_batch_item_row.frozen_source_amount AS item_frozen_source_amount,
      pay_batch_item_row.frozen_resolution_mode AS item_frozen_resolution_mode,
      pay_batch_item_row.frozen_resolution_payload_json AS item_frozen_resolution_payload_json,
      pay_batch_item_row.frozen_resolution_result_json AS item_frozen_resolution_result_json,
      pay_batch_item_row.frozen_component_key_type AS item_frozen_key_type,
      pay_batch_item_row.frozen_component_key_value AS item_frozen_key_value,
      CASE
        WHEN jsonb_typeof(COALESCE(pay_batch_item_row.frozen_component_snapshot_json, '{}'::jsonb)) = 'object'
          THEN COALESCE(pay_batch_item_row.frozen_component_snapshot_json, '{}'::jsonb)
        ELSE '{}'::jsonb
      END AS item_frozen_component_snapshot_json,
      CASE
        WHEN jsonb_typeof(COALESCE(pay_batch_item_row.frozen_source_basis_json, '{}'::jsonb)) = 'object'
          THEN COALESCE(pay_batch_item_row.frozen_source_basis_json, '{}'::jsonb)
        ELSE '{}'::jsonb
      END AS item_frozen_source_basis_json
    FROM public.pay_batch_items AS pay_batch_item_row
    JOIN public.pay_batch_candidates AS pay_batch_candidate_row
      ON pay_batch_candidate_row.id = pay_batch_item_row.pay_batch_candidate_id
    WHERE (p_pay_batch_id IS NULL OR pay_batch_candidate_row.pay_batch_id = p_pay_batch_id)
      AND (
        COALESCE(array_length(p_pay_batch_item_ids, 1), 0) = 0
        OR EXISTS (
          SELECT 1
          FROM input_item_ids AS input_filter
          WHERE input_filter.item_id = pay_batch_item_row.id
        )
      )
  ),
  single_breakdown_meta AS (
    SELECT
      breakdown_row.pay_batch_item_id AS batch_item_id,
      COUNT(*)::integer AS breakdown_count,
      CASE
        WHEN COUNT(*) = 1 THEN (ARRAY_AGG(breakdown_row.meta_json ORDER BY breakdown_row.id))[1]
        ELSE '{}'::jsonb
      END AS single_meta_json
    FROM public.pay_batch_item_breakdowns AS breakdown_row
    JOIN base_items AS base_for_breakdown
      ON base_for_breakdown.batch_item_id = breakdown_row.pay_batch_item_id
    GROUP BY breakdown_row.pay_batch_item_id
  ),
  snapshot_choice AS (
    SELECT
      distinct_item_pairs.batch_id,
      distinct_item_pairs.item_timesheet_id,
      (
        SELECT snapshot_row.target_snapshot_json
        FROM public.pay_batch_timesheet_snapshots AS snapshot_row
        WHERE snapshot_row.pay_batch_id = distinct_item_pairs.batch_id
          AND snapshot_row.timesheet_id = distinct_item_pairs.item_timesheet_id
        ORDER BY snapshot_row.created_at_utc DESC, snapshot_row.id DESC
        LIMIT 1
      ) AS target_snapshot_json
    FROM (
      SELECT DISTINCT
        base_item_rows.batch_id,
        base_item_rows.item_timesheet_id
      FROM base_items AS base_item_rows
      WHERE base_item_rows.item_timesheet_id IS NOT NULL
    ) AS distinct_item_pairs
  ),
  prepared_items AS (
    SELECT
      base_item_rows.batch_id,
      base_item_rows.batch_item_id,
      base_item_rows.item_timesheet_id,
      base_item_rows.item_type_norm,
      base_item_rows.item_type_raw,
      base_item_rows.item_segment_key,
      base_item_rows.item_source_ref,
      base_item_rows.item_amount_ex_vat,
      base_item_rows.item_amount_inc_vat,
      base_item_rows.item_finance_case_id,
      base_item_rows.item_finance_component_id,
      base_item_rows.item_frozen_target_amount_ex_vat,
      base_item_rows.item_frozen_source_amount,
      base_item_rows.item_frozen_resolution_mode,
      base_item_rows.item_frozen_resolution_payload_json,
      base_item_rows.item_frozen_resolution_result_json,
      base_item_rows.item_frozen_key_type,
      base_item_rows.item_frozen_key_value,
      base_item_rows.item_frozen_component_snapshot_json,
      base_item_rows.item_frozen_source_basis_json,
      CASE
        WHEN COALESCE(breakdown_meta.breakdown_count, 0) = 1
         AND jsonb_typeof(COALESCE(breakdown_meta.single_meta_json, '{}'::jsonb)) = 'object'
          THEN COALESCE(breakdown_meta.single_meta_json, '{}'::jsonb)
        ELSE '{}'::jsonb
      END AS item_single_breakdown_meta_json,
      CASE
        WHEN jsonb_typeof(COALESCE(snapshot_choice_rows.target_snapshot_json, '{}'::jsonb)) = 'object'
          THEN COALESCE(snapshot_choice_rows.target_snapshot_json, '{}'::jsonb)
        ELSE '{}'::jsonb
      END AS target_snapshot_json
    FROM base_items AS base_item_rows
    LEFT JOIN single_breakdown_meta AS breakdown_meta
      ON breakdown_meta.batch_item_id = base_item_rows.batch_item_id
    LEFT JOIN snapshot_choice AS snapshot_choice_rows
      ON snapshot_choice_rows.batch_id = base_item_rows.batch_id
     AND snapshot_choice_rows.item_timesheet_id = base_item_rows.item_timesheet_id
  ),
  resolved_items AS (
    SELECT
      prepared_item_rows.batch_id,
      prepared_item_rows.batch_item_id,
      prepared_item_rows.item_timesheet_id,
      prepared_item_rows.item_type_norm,
      prepared_item_rows.item_type_raw,
      prepared_item_rows.item_amount_ex_vat,
      prepared_item_rows.item_amount_inc_vat,
      prepared_item_rows.item_finance_case_id,
      prepared_item_rows.item_finance_component_id,
      prepared_item_rows.item_frozen_target_amount_ex_vat,
      prepared_item_rows.item_frozen_source_amount,
      prepared_item_rows.item_frozen_resolution_mode,
      prepared_item_rows.item_frozen_resolution_payload_json,
      prepared_item_rows.item_frozen_resolution_result_json,
      prepared_item_rows.item_frozen_component_snapshot_json,
      prepared_item_rows.item_frozen_source_basis_json,
      prepared_item_rows.item_single_breakdown_meta_json,
      prepared_item_rows.target_snapshot_json,
      resolved_key_rows.key_type AS resolved_key_type,
      resolved_key_rows.key_value AS resolved_key_value,
      resolved_key_rows.key_resolution_source AS resolved_key_source,
      resolved_key_rows.key_resolution_failure_reason AS resolved_key_failure_reason
    FROM prepared_items AS prepared_item_rows
    JOIN LATERAL public._pay_policy_x_resolve_post_draft_economic_key(
      p_pay_batch_item_id => prepared_item_rows.batch_item_id,
      p_pay_batch_id => prepared_item_rows.batch_id,
      p_timesheet_id => prepared_item_rows.item_timesheet_id,
      p_item_type => prepared_item_rows.item_type_norm,
      p_frozen_key_type => prepared_item_rows.item_frozen_key_type,
      p_frozen_key_value => prepared_item_rows.item_frozen_key_value,
      p_frozen_component_snapshot_json => prepared_item_rows.item_frozen_component_snapshot_json,
      p_frozen_source_basis_json => prepared_item_rows.item_frozen_source_basis_json,
      p_breakdown_meta_json => prepared_item_rows.item_single_breakdown_meta_json,
      p_target_snapshot_json => prepared_item_rows.target_snapshot_json
    ) AS resolved_key_rows
      ON true
  ),
  amount_candidates AS (
    SELECT
      resolved_item_rows.batch_id,
      resolved_item_rows.batch_item_id,
      resolved_item_rows.item_timesheet_id,
      resolved_item_rows.item_type_norm,
      resolved_item_rows.item_type_raw,
      resolved_item_rows.item_amount_ex_vat,
      resolved_item_rows.item_amount_inc_vat,
      resolved_item_rows.resolved_key_type,
      resolved_item_rows.resolved_key_value,
      resolved_item_rows.resolved_key_source,
      resolved_item_rows.resolved_key_failure_reason,
      (
           resolved_item_rows.item_finance_case_id IS NOT NULL
        OR resolved_item_rows.item_finance_component_id IS NOT NULL
        OR resolved_item_rows.item_frozen_target_amount_ex_vat IS NOT NULL
        OR resolved_item_rows.item_frozen_resolution_mode IS NOT NULL
        OR resolved_item_rows.item_frozen_resolution_payload_json IS NOT NULL
        OR resolved_item_rows.item_frozen_resolution_result_json IS NOT NULL
        OR resolved_item_rows.item_frozen_component_snapshot_json ? 'target_amount_ex_vat'
        OR resolved_item_rows.item_frozen_component_snapshot_json ? 'target_pay_ex_vat'
        OR resolved_item_rows.item_frozen_component_snapshot_json ? 'target_pay_amount_ex_vat'
        OR resolved_item_rows.item_frozen_component_snapshot_json ? 'frozen_target_amount_ex_vat'
        OR resolved_item_rows.item_frozen_component_snapshot_json ? 'target_rate'
        OR resolved_item_rows.item_frozen_component_snapshot_json ? 'target_units'
        OR resolved_item_rows.item_frozen_component_snapshot_json ? 'resolution_mode'
        OR resolved_item_rows.item_frozen_component_snapshot_json ? 'saved_resolution_mode'
        OR resolved_item_rows.item_frozen_component_snapshot_json ? 'saved_resolution_payload_json'
        OR resolved_item_rows.item_frozen_component_snapshot_json ? 'saved_resolution_result_json'
        OR resolved_item_rows.item_single_breakdown_meta_json ? 'target_amount_ex_vat'
        OR resolved_item_rows.item_single_breakdown_meta_json ? 'target_pay_ex_vat'
        OR resolved_item_rows.item_single_breakdown_meta_json ? 'target_rate'
        OR resolved_item_rows.item_single_breakdown_meta_json ? 'resolution_mode'
      ) AS item_has_source_target_split,
      public._pay_batch_item_source_reservation_amount_ex_vat(resolved_item_rows.batch_item_id) AS entitlement_source_amount_ex_vat,
      (
        SELECT target_candidates.target_text_value::numeric
        FROM (
          VALUES
            (resolved_item_rows.item_frozen_target_amount_ex_vat::text),
            (resolved_item_rows.item_amount_ex_vat::text),
            (resolved_item_rows.item_frozen_component_snapshot_json->>'frozen_target_amount_ex_vat'),
            (resolved_item_rows.item_frozen_component_snapshot_json->>'target_amount_ex_vat'),
            (resolved_item_rows.item_frozen_component_snapshot_json->>'target_pay_ex_vat'),
            (resolved_item_rows.item_frozen_component_snapshot_json->>'target_pay_amount_ex_vat'),
            (resolved_item_rows.item_frozen_component_snapshot_json->>'amount_ex_vat'),
            (resolved_item_rows.item_single_breakdown_meta_json->>'target_amount_ex_vat'),
            (resolved_item_rows.item_single_breakdown_meta_json->>'target_pay_ex_vat'),
            (resolved_item_rows.item_single_breakdown_meta_json->>'amount_ex_vat')
        ) AS target_candidates(target_text_value)
        WHERE target_candidates.target_text_value IS NOT NULL
          AND BTRIM(target_candidates.target_text_value) ~ '^-?[0-9]+(\.[0-9]+)?$'
        LIMIT 1
      ) AS raw_target_amount_ex_vat,
      (
        SELECT source_candidates.source_text_value::numeric
        FROM (
          VALUES
            (resolved_item_rows.item_frozen_source_amount::text),
            (resolved_item_rows.item_frozen_component_snapshot_json->>'source_reservation_amount_ex_vat'),
            (resolved_item_rows.item_frozen_component_snapshot_json->>'source_entitlement_amount_ex_vat'),
            (resolved_item_rows.item_frozen_component_snapshot_json->>'source_amount_ex_vat'),
            (resolved_item_rows.item_frozen_component_snapshot_json->>'source_pay_ex_vat'),
            (resolved_item_rows.item_frozen_component_snapshot_json->>'source_pay_amount_ex_vat'),
            (resolved_item_rows.item_frozen_component_snapshot_json->>'basis_source_amount_ex_vat'),
            (resolved_item_rows.item_frozen_component_snapshot_json->>'reserved_source_amount'),
            (resolved_item_rows.item_frozen_component_snapshot_json#>>'{source_basis_json,source_reservation_amount_ex_vat}'),
            (resolved_item_rows.item_frozen_component_snapshot_json#>>'{source_basis_json,source_entitlement_amount_ex_vat}'),
            (resolved_item_rows.item_frozen_component_snapshot_json#>>'{source_basis_json,source_amount_ex_vat}'),
            (resolved_item_rows.item_frozen_component_snapshot_json#>>'{source_basis_json,source_pay_ex_vat}'),
            (resolved_item_rows.item_frozen_source_basis_json->>'source_reservation_amount_ex_vat'),
            (resolved_item_rows.item_frozen_source_basis_json->>'source_entitlement_amount_ex_vat'),
            (resolved_item_rows.item_frozen_source_basis_json->>'source_amount_ex_vat'),
            (resolved_item_rows.item_frozen_source_basis_json->>'source_pay_ex_vat'),
            (resolved_item_rows.item_frozen_source_basis_json->>'pay_ex_vat'),
            (resolved_item_rows.item_frozen_source_basis_json->>'amount_ex_vat'),
            (resolved_item_rows.item_frozen_resolution_payload_json->>'source_reservation_amount_ex_vat'),
            (resolved_item_rows.item_frozen_resolution_payload_json->>'source_entitlement_amount_ex_vat'),
            (resolved_item_rows.item_frozen_resolution_payload_json->>'source_amount_ex_vat'),
            (resolved_item_rows.item_frozen_resolution_payload_json->>'source_pay_ex_vat'),
            (resolved_item_rows.item_frozen_resolution_result_json->>'source_reservation_amount_ex_vat'),
            (resolved_item_rows.item_frozen_resolution_result_json->>'source_entitlement_amount_ex_vat'),
            (resolved_item_rows.item_frozen_resolution_result_json->>'source_amount_ex_vat'),
            (resolved_item_rows.item_frozen_resolution_result_json->>'source_pay_ex_vat'),
            (resolved_item_rows.item_single_breakdown_meta_json->>'source_reservation_amount_ex_vat'),
            (resolved_item_rows.item_single_breakdown_meta_json->>'source_entitlement_amount_ex_vat'),
            (resolved_item_rows.item_single_breakdown_meta_json->>'source_amount_ex_vat'),
            (resolved_item_rows.item_single_breakdown_meta_json->>'source_pay_ex_vat')
        ) AS source_candidates(source_text_value)
        WHERE source_candidates.source_text_value IS NOT NULL
          AND BTRIM(source_candidates.source_text_value) ~ '^-?[0-9]+(\.[0-9]+)?$'
        LIMIT 1
      ) AS raw_artifact_source_amount_ex_vat,
      (
        SELECT source_inc_candidates.source_inc_text_value::numeric
        FROM (
          VALUES
            (resolved_item_rows.item_frozen_source_basis_json->>'source_amount_inc_vat'),
            (resolved_item_rows.item_frozen_source_basis_json->>'source_pay_inc_vat'),
            (resolved_item_rows.item_frozen_source_basis_json->>'source_entitlement_amount_inc_vat'),
            (resolved_item_rows.item_frozen_source_basis_json->>'source_reservation_amount_inc_vat'),
            (resolved_item_rows.item_frozen_source_basis_json->>'pay_inc_vat'),
            (resolved_item_rows.item_frozen_source_basis_json->>'amount_inc_vat'),
            (resolved_item_rows.item_frozen_component_snapshot_json->>'source_amount_inc_vat'),
            (resolved_item_rows.item_frozen_component_snapshot_json->>'source_pay_inc_vat'),
            (resolved_item_rows.item_frozen_component_snapshot_json->>'source_entitlement_amount_inc_vat'),
            (resolved_item_rows.item_frozen_component_snapshot_json->>'source_reservation_amount_inc_vat'),
            (resolved_item_rows.item_frozen_component_snapshot_json->>'basis_source_amount_inc_vat'),
            (resolved_item_rows.item_frozen_component_snapshot_json->>'reserved_source_amount_inc_vat'),
            (resolved_item_rows.item_frozen_component_snapshot_json#>>'{source_basis_json,source_amount_inc_vat}'),
            (resolved_item_rows.item_frozen_component_snapshot_json#>>'{source_basis_json,source_pay_inc_vat}'),
            (resolved_item_rows.item_frozen_component_snapshot_json#>>'{source_basis_json,source_entitlement_amount_inc_vat}'),
            (resolved_item_rows.item_frozen_component_snapshot_json#>>'{source_basis_json,source_reservation_amount_inc_vat}'),
            (resolved_item_rows.item_frozen_component_snapshot_json#>>'{source_basis_json,pay_inc_vat}'),
            (resolved_item_rows.item_frozen_component_snapshot_json#>>'{source_basis_json,amount_inc_vat}'),
            (resolved_item_rows.item_frozen_resolution_payload_json->>'source_amount_inc_vat'),
            (resolved_item_rows.item_frozen_resolution_payload_json->>'source_pay_inc_vat'),
            (resolved_item_rows.item_frozen_resolution_payload_json->>'source_entitlement_amount_inc_vat'),
            (resolved_item_rows.item_frozen_resolution_payload_json->>'source_reservation_amount_inc_vat'),
            (resolved_item_rows.item_frozen_resolution_result_json->>'source_amount_inc_vat'),
            (resolved_item_rows.item_frozen_resolution_result_json->>'source_pay_inc_vat'),
            (resolved_item_rows.item_frozen_resolution_result_json->>'source_entitlement_amount_inc_vat'),
            (resolved_item_rows.item_frozen_resolution_result_json->>'source_reservation_amount_inc_vat'),
            (resolved_item_rows.item_single_breakdown_meta_json->>'source_amount_inc_vat'),
            (resolved_item_rows.item_single_breakdown_meta_json->>'source_pay_inc_vat'),
            (resolved_item_rows.item_single_breakdown_meta_json->>'source_entitlement_amount_inc_vat'),
            (resolved_item_rows.item_single_breakdown_meta_json->>'source_reservation_amount_inc_vat')
        ) AS source_inc_candidates(source_inc_text_value)
        WHERE source_inc_candidates.source_inc_text_value IS NOT NULL
          AND BTRIM(source_inc_candidates.source_inc_text_value) ~ '^-?[0-9]+(\.[0-9]+)?$'
        LIMIT 1
      ) AS raw_artifact_source_amount_inc_vat
    FROM resolved_items AS resolved_item_rows
  ),
  final_rows AS (
    SELECT
      amount_candidate_rows.batch_id,
      amount_candidate_rows.batch_item_id,
      amount_candidate_rows.item_timesheet_id,
      amount_candidate_rows.item_type_raw,
      amount_candidate_rows.resolved_key_type,
      amount_candidate_rows.resolved_key_value,
      amount_candidate_rows.resolved_key_source,
      amount_candidate_rows.resolved_key_failure_reason,
      ROUND(ABS(
        CASE
          WHEN amount_candidate_rows.item_type_norm IN ('SEGMENT_DELTA', 'EXPENSE_DELTA', 'ADJUSTMENT_DELTA', 'MILEAGE_DELTA')
            THEN COALESCE(
              amount_candidate_rows.entitlement_source_amount_ex_vat,
              amount_candidate_rows.raw_artifact_source_amount_ex_vat,
              CASE
                WHEN amount_candidate_rows.item_has_source_target_split THEN NULL::numeric
                ELSE COALESCE(amount_candidate_rows.raw_target_amount_ex_vat, amount_candidate_rows.item_amount_ex_vat)
              END
            )
          ELSE COALESCE(amount_candidate_rows.raw_artifact_source_amount_ex_vat, amount_candidate_rows.raw_target_amount_ex_vat, amount_candidate_rows.item_amount_ex_vat)
        END
      ), 2)::numeric AS final_source_amount_ex_vat,
      ROUND(ABS(COALESCE(
        amount_candidate_rows.raw_artifact_source_amount_inc_vat,
        CASE
          WHEN amount_candidate_rows.item_has_source_target_split THEN NULL::numeric
          ELSE amount_candidate_rows.item_amount_inc_vat
        END,
        CASE
          WHEN amount_candidate_rows.item_type_norm IN ('SEGMENT_DELTA', 'EXPENSE_DELTA', 'ADJUSTMENT_DELTA', 'MILEAGE_DELTA')
            THEN COALESCE(
              amount_candidate_rows.entitlement_source_amount_ex_vat,
              amount_candidate_rows.raw_artifact_source_amount_ex_vat,
              CASE
                WHEN amount_candidate_rows.item_has_source_target_split THEN NULL::numeric
                ELSE COALESCE(amount_candidate_rows.raw_target_amount_ex_vat, amount_candidate_rows.item_amount_ex_vat)
              END
            )
          ELSE COALESCE(amount_candidate_rows.raw_artifact_source_amount_ex_vat, amount_candidate_rows.raw_target_amount_ex_vat, amount_candidate_rows.item_amount_ex_vat)
        END
      )), 2)::numeric AS final_source_amount_inc_vat,
      ROUND(ABS(COALESCE(
        amount_candidate_rows.raw_target_amount_ex_vat,
        amount_candidate_rows.item_amount_ex_vat,
        CASE
          WHEN amount_candidate_rows.item_type_norm IN ('SEGMENT_DELTA', 'EXPENSE_DELTA', 'ADJUSTMENT_DELTA', 'MILEAGE_DELTA')
            THEN COALESCE(amount_candidate_rows.entitlement_source_amount_ex_vat, amount_candidate_rows.raw_artifact_source_amount_ex_vat)
          ELSE amount_candidate_rows.raw_artifact_source_amount_ex_vat
        END
      )), 2)::numeric AS final_target_amount_ex_vat,
      CASE
        WHEN amount_candidate_rows.resolved_key_failure_reason IS NOT NULL
          THEN amount_candidate_rows.resolved_key_failure_reason
        WHEN amount_candidate_rows.resolved_key_type IS NULL OR BTRIM(COALESCE(amount_candidate_rows.resolved_key_type, '')) = ''
          THEN 'POST_DRAFT_KEY_RESOLUTION_FAILED'
        WHEN amount_candidate_rows.resolved_key_value IS NULL OR BTRIM(COALESCE(amount_candidate_rows.resolved_key_value, '')) = ''
          THEN 'POST_DRAFT_KEY_RESOLUTION_FAILED'
        WHEN amount_candidate_rows.resolved_key_type = 'TS_DAY'
         AND amount_candidate_rows.resolved_key_value !~ '^\d{4}-\d{2}-\d{2}$'
          THEN 'TS_DAY_KEY_VALUE_NOT_DATE'
        WHEN ROUND(ABS(
          CASE
            WHEN amount_candidate_rows.item_type_norm IN ('SEGMENT_DELTA', 'EXPENSE_DELTA', 'ADJUSTMENT_DELTA', 'MILEAGE_DELTA')
              THEN COALESCE(
                amount_candidate_rows.entitlement_source_amount_ex_vat,
                amount_candidate_rows.raw_artifact_source_amount_ex_vat,
                CASE
                  WHEN amount_candidate_rows.item_has_source_target_split THEN NULL::numeric
                  ELSE COALESCE(amount_candidate_rows.raw_target_amount_ex_vat, amount_candidate_rows.item_amount_ex_vat)
                END
              )
            ELSE COALESCE(amount_candidate_rows.raw_artifact_source_amount_ex_vat, amount_candidate_rows.raw_target_amount_ex_vat, amount_candidate_rows.item_amount_ex_vat)
          END
        ), 2) IS NULL
          THEN 'SOURCE_AMOUNT_NOT_RESOLVED'
        WHEN ROUND(ABS(COALESCE(
          amount_candidate_rows.raw_target_amount_ex_vat,
          amount_candidate_rows.item_amount_ex_vat,
          CASE
            WHEN amount_candidate_rows.item_type_norm IN ('SEGMENT_DELTA', 'EXPENSE_DELTA', 'ADJUSTMENT_DELTA', 'MILEAGE_DELTA')
              THEN COALESCE(amount_candidate_rows.entitlement_source_amount_ex_vat, amount_candidate_rows.raw_artifact_source_amount_ex_vat)
            ELSE amount_candidate_rows.raw_artifact_source_amount_ex_vat
          END
        )), 2) IS NULL
          THEN 'TARGET_AMOUNT_NOT_RESOLVED'
        ELSE NULL::text
      END AS final_failure_reason
    FROM amount_candidates AS amount_candidate_rows
  )
  SELECT
    final_rows.batch_id AS pay_batch_id,
    final_rows.batch_item_id AS pay_batch_item_id,
    final_rows.item_timesheet_id AS timesheet_id,
    final_rows.item_type_raw AS item_type,
    CASE WHEN final_rows.final_failure_reason IS NULL THEN final_rows.resolved_key_type ELSE NULL::text END AS key_type,
    CASE WHEN final_rows.final_failure_reason IS NULL THEN final_rows.resolved_key_value ELSE NULL::text END AS key_value,
    CASE WHEN final_rows.final_source_amount_ex_vat IS NULL THEN NULL::numeric ELSE ROUND(final_rows.final_source_amount_ex_vat, 2)::numeric END AS source_amount_ex_vat,
    CASE WHEN final_rows.final_source_amount_inc_vat IS NULL THEN NULL::numeric ELSE ROUND(final_rows.final_source_amount_inc_vat, 2)::numeric END AS source_amount_inc_vat,
    CASE WHEN final_rows.final_target_amount_ex_vat IS NULL THEN NULL::numeric ELSE ROUND(final_rows.final_target_amount_ex_vat, 2)::numeric END AS target_amount_ex_vat,
    CASE WHEN final_rows.final_failure_reason IS NULL THEN final_rows.resolved_key_source ELSE 'KEY_RESOLUTION_FAILED' END AS key_resolution_source,
    final_rows.final_failure_reason AS key_resolution_failure_reason
  FROM final_rows
  ORDER BY final_rows.batch_id, final_rows.batch_item_id;
END;
$function$;

-- _pay_batch_item_source_reservation_amount_ex_vat(uuid)
CREATE OR REPLACE FUNCTION public._pay_batch_item_source_reservation_amount_ex_vat(p_pay_batch_item_id uuid)
 RETURNS numeric
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
 SET "plpgsql_check.mode" TO 'disabled'
AS $function$
DECLARE
  v_pay_batch_id uuid;
  v_timesheet_id uuid;
  v_item_type text;
  v_finance_case_id uuid;
  v_finance_component_id uuid;
  v_amount_ex_vat numeric;
  v_frozen_source_amount numeric;
  v_frozen_target_amount_ex_vat numeric;
  v_frozen_resolution_mode public.pay_finance_component_resolution_mode_enum;
  v_frozen_resolution_payload_json jsonb;
  v_frozen_resolution_result_json jsonb;
  v_frozen_component_key_type text;
  v_frozen_component_key_value text;
  v_frozen_component_snapshot_json jsonb;
  v_frozen_source_basis_json jsonb;
  v_single_breakdown_meta_json jsonb := '{}'::jsonb;
  v_breakdown_count integer := 0;
  v_resolved_key_type text;
  v_resolved_key_value text;
  v_key_resolution_failure_reason text;
  v_source_text text;
  v_frozen_resolved_source_text text;
  v_has_source_target_split boolean;
BEGIN
  IF p_pay_batch_item_id IS NULL THEN
    RETURN NULL::numeric(12,2);
  END IF;

  SELECT
    pay_batch_candidate_row.pay_batch_id,
    pay_batch_item_row.timesheet_id,
    UPPER(NULLIF(BTRIM(COALESCE(pay_batch_item_row.item_type, '')), '')),
    pay_batch_item_row.finance_case_id,
    pay_batch_item_row.finance_component_id,
    pay_batch_item_row.amount_ex_vat,
    pay_batch_item_row.frozen_source_amount,
    pay_batch_item_row.frozen_target_amount_ex_vat,
    pay_batch_item_row.frozen_resolution_mode,
    pay_batch_item_row.frozen_resolution_payload_json,
    pay_batch_item_row.frozen_resolution_result_json,
    pay_batch_item_row.frozen_component_key_type,
    pay_batch_item_row.frozen_component_key_value,
    CASE
      WHEN jsonb_typeof(COALESCE(pay_batch_item_row.frozen_component_snapshot_json, '{}'::jsonb)) = 'object'
        THEN COALESCE(pay_batch_item_row.frozen_component_snapshot_json, '{}'::jsonb)
      ELSE '{}'::jsonb
    END,
    CASE
      WHEN jsonb_typeof(COALESCE(pay_batch_item_row.frozen_source_basis_json, '{}'::jsonb)) = 'object'
        THEN COALESCE(pay_batch_item_row.frozen_source_basis_json, '{}'::jsonb)
      ELSE '{}'::jsonb
    END
  INTO
    v_pay_batch_id,
    v_timesheet_id,
    v_item_type,
    v_finance_case_id,
    v_finance_component_id,
    v_amount_ex_vat,
    v_frozen_source_amount,
    v_frozen_target_amount_ex_vat,
    v_frozen_resolution_mode,
    v_frozen_resolution_payload_json,
    v_frozen_resolution_result_json,
    v_frozen_component_key_type,
    v_frozen_component_key_value,
    v_frozen_component_snapshot_json,
    v_frozen_source_basis_json
  FROM public.pay_batch_items AS pay_batch_item_row
  JOIN public.pay_batch_candidates AS pay_batch_candidate_row
    ON pay_batch_candidate_row.id = pay_batch_item_row.pay_batch_candidate_id
  WHERE pay_batch_item_row.id = p_pay_batch_item_id
  LIMIT 1;

  IF NOT FOUND THEN
    RETURN NULL::numeric(12,2);
  END IF;

  IF v_item_type NOT IN ('SEGMENT_DELTA', 'EXPENSE_DELTA', 'ADJUSTMENT_DELTA', 'MILEAGE_DELTA') THEN
    RETURN NULL::numeric(12,2);
  END IF;

  SELECT
    COUNT(*)::integer,
    CASE
      WHEN COUNT(*) = 1 THEN (ARRAY_AGG(breakdown_row.meta_json ORDER BY breakdown_row.id))[1]
      ELSE '{}'::jsonb
    END
  INTO
    v_breakdown_count,
    v_single_breakdown_meta_json
  FROM public.pay_batch_item_breakdowns AS breakdown_row
  WHERE breakdown_row.pay_batch_item_id = p_pay_batch_item_id;

  IF jsonb_typeof(COALESCE(v_single_breakdown_meta_json, '{}'::jsonb)) <> 'object' THEN
    v_single_breakdown_meta_json := '{}'::jsonb;
  END IF;

  SELECT
    resolved_key_row.key_type,
    resolved_key_row.key_value,
    resolved_key_row.key_resolution_failure_reason
  INTO
    v_resolved_key_type,
    v_resolved_key_value,
    v_key_resolution_failure_reason
  FROM public._pay_policy_x_resolve_post_draft_economic_key(
    p_pay_batch_item_id => p_pay_batch_item_id,
    p_pay_batch_id => v_pay_batch_id,
    p_timesheet_id => v_timesheet_id,
    p_item_type => v_item_type,
    p_frozen_key_type => v_frozen_component_key_type,
    p_frozen_key_value => v_frozen_component_key_value,
    p_frozen_component_snapshot_json => v_frozen_component_snapshot_json,
    p_frozen_source_basis_json => v_frozen_source_basis_json,
    p_breakdown_meta_json => v_single_breakdown_meta_json,
    p_target_snapshot_json => NULL::jsonb
  ) AS resolved_key_row
  LIMIT 1;

  IF v_key_resolution_failure_reason IS NOT NULL
     OR v_resolved_key_type IS NULL
     OR BTRIM(COALESCE(v_resolved_key_type, '')) = ''
     OR v_resolved_key_value IS NULL
     OR BTRIM(COALESCE(v_resolved_key_value, '')) = ''
     OR (v_resolved_key_type = 'TS_DAY' AND v_resolved_key_value !~ '^\d{4}-\d{2}-\d{2}$') THEN
    RETURN NULL::numeric(12,2);
  END IF;

  SELECT ROUND(
           SUM((frozen_resolution_component.component_json->>'source_pay_ex_vat')::numeric),
           2
         )::text
  INTO v_frozen_resolved_source_text
  FROM jsonb_array_elements(
    CASE
      WHEN jsonb_typeof(v_frozen_resolution_payload_json->'case_components') = 'array'
        THEN v_frozen_resolution_payload_json->'case_components'
      ELSE '[]'::jsonb
    END
  ) AS frozen_resolution_component(component_json)
  WHERE UPPER(BTRIM(COALESCE(frozen_resolution_component.component_json->>'component_key_type', ''))) = v_resolved_key_type
    AND BTRIM(COALESCE(frozen_resolution_component.component_json->>'component_key_value', '')) = v_resolved_key_value
    AND LOWER(BTRIM(COALESCE(frozen_resolution_component.component_json->>'is_resolution_stale', 'false'))) NOT IN ('true','t','1','yes','y','on')
    AND LOWER(BTRIM(COALESCE(frozen_resolution_component.component_json->>'is_stale_saved_resolution', 'false'))) NOT IN ('true','t','1','yes','y','on')
    AND LOWER(BTRIM(COALESCE(frozen_resolution_component.component_json->>'requires_resolution', 'true'))) IN ('false','f','0','no','n','off')
    AND NULLIF(BTRIM(COALESCE(frozen_resolution_component.component_json->>'resolved_rate_resolution_id', '')), '') IS NOT NULL
    AND COALESCE(frozen_resolution_component.component_json->>'source_pay_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$';

  IF v_frozen_resolved_source_text IS NOT NULL THEN
    RETURN ROUND(ABS(v_frozen_resolved_source_text::numeric), 2)::numeric(12,2);
  END IF;

  IF v_frozen_source_amount IS NOT NULL THEN
    RETURN ROUND(ABS(v_frozen_source_amount), 2)::numeric(12,2);
  END IF;

  SELECT source_candidates.source_text_value
  INTO v_source_text
  FROM (
    VALUES
      (v_frozen_component_snapshot_json->>'source_reservation_amount_ex_vat'),
      (v_frozen_component_snapshot_json->>'source_entitlement_amount_ex_vat'),
      (v_frozen_component_snapshot_json->>'source_amount_ex_vat'),
      (v_frozen_component_snapshot_json->>'source_pay_ex_vat'),
      (v_frozen_component_snapshot_json->>'source_pay_amount_ex_vat'),
      (v_frozen_component_snapshot_json->>'basis_source_amount_ex_vat'),
      (v_frozen_component_snapshot_json->>'reserved_source_amount'),
      (v_frozen_component_snapshot_json->>'frozen_source_amount'),
      (v_frozen_component_snapshot_json#>>'{source_basis_json,source_reservation_amount_ex_vat}'),
      (v_frozen_component_snapshot_json#>>'{source_basis_json,source_entitlement_amount_ex_vat}'),
      (v_frozen_component_snapshot_json#>>'{source_basis_json,source_amount_ex_vat}'),
      (v_frozen_component_snapshot_json#>>'{source_basis_json,source_pay_ex_vat}'),
      (v_frozen_component_snapshot_json#>>'{source_basis_json,source_pay_amount_ex_vat}'),
      (v_frozen_component_snapshot_json#>>'{source_basis_json,basis_source_amount_ex_vat}'),
      (v_frozen_component_snapshot_json#>>'{source_basis_json,pay_ex_vat}'),
      (v_frozen_component_snapshot_json#>>'{source_basis_json,pay_amount_ex_vat}'),
      (v_frozen_component_snapshot_json#>>'{source_basis_json,amount_ex_vat}'),
      (v_frozen_source_basis_json->>'source_reservation_amount_ex_vat'),
      (v_frozen_source_basis_json->>'source_entitlement_amount_ex_vat'),
      (v_frozen_source_basis_json->>'source_amount_ex_vat'),
      (v_frozen_source_basis_json->>'source_pay_ex_vat'),
      (v_frozen_source_basis_json->>'source_pay_amount_ex_vat'),
      (v_frozen_source_basis_json->>'basis_source_amount_ex_vat'),
      (v_frozen_source_basis_json->>'reserved_source_amount'),
      (v_frozen_source_basis_json->>'frozen_source_amount'),
      (v_frozen_source_basis_json->>'pay_ex_vat'),
      (v_frozen_source_basis_json->>'pay_amount_ex_vat'),
      (v_frozen_source_basis_json->>'amount_ex_vat'),
      (v_frozen_source_basis_json->>'pay_amount'),
      (v_frozen_resolution_payload_json->>'source_reservation_amount_ex_vat'),
      (v_frozen_resolution_payload_json->>'source_entitlement_amount_ex_vat'),
      (v_frozen_resolution_payload_json->>'source_amount_ex_vat'),
      (v_frozen_resolution_payload_json->>'source_pay_ex_vat'),
      (v_frozen_resolution_result_json->>'source_reservation_amount_ex_vat'),
      (v_frozen_resolution_result_json->>'source_entitlement_amount_ex_vat'),
      (v_frozen_resolution_result_json->>'source_amount_ex_vat'),
      (v_frozen_resolution_result_json->>'source_pay_ex_vat'),
      (v_single_breakdown_meta_json->>'source_reservation_amount_ex_vat'),
      (v_single_breakdown_meta_json->>'source_entitlement_amount_ex_vat'),
      (v_single_breakdown_meta_json->>'source_amount_ex_vat'),
      (v_single_breakdown_meta_json->>'source_pay_ex_vat')
  ) AS source_candidates(source_text_value)
  WHERE source_candidates.source_text_value IS NOT NULL
    AND BTRIM(source_candidates.source_text_value) ~ '^-?[0-9]+(\.[0-9]+)?$'
  LIMIT 1;

  IF v_source_text IS NOT NULL THEN
    RETURN ROUND(ABS(v_source_text::numeric), 2)::numeric(12,2);
  END IF;

  v_has_source_target_split :=
       v_finance_case_id IS NOT NULL
    OR v_finance_component_id IS NOT NULL
    OR v_frozen_target_amount_ex_vat IS NOT NULL
    OR v_frozen_resolution_mode IS NOT NULL
    OR v_frozen_resolution_payload_json IS NOT NULL
    OR v_frozen_resolution_result_json IS NOT NULL
    OR v_frozen_component_snapshot_json ? 'target_amount_ex_vat'
    OR v_frozen_component_snapshot_json ? 'target_pay_ex_vat'
    OR v_frozen_component_snapshot_json ? 'target_pay_amount_ex_vat'
    OR v_frozen_component_snapshot_json ? 'frozen_target_amount_ex_vat'
    OR v_frozen_component_snapshot_json ? 'target_rate'
    OR v_frozen_component_snapshot_json ? 'target_units'
    OR v_frozen_component_snapshot_json ? 'resolution_mode'
    OR v_frozen_component_snapshot_json ? 'saved_resolution_mode'
    OR v_frozen_component_snapshot_json ? 'saved_resolution_payload_json'
    OR v_frozen_component_snapshot_json ? 'saved_resolution_result_json';

  IF v_has_source_target_split THEN
    RETURN NULL::numeric(12,2);
  END IF;

  IF v_amount_ex_vat IS NOT NULL THEN
    RETURN ROUND(ABS(v_amount_ex_vat), 2)::numeric(12,2);
  END IF;

  RETURN NULL::numeric(12,2);
END;
$function$;

-- _pay_batch_provider_submit_preflight_recheck(uuid,jsonb,uuid,uuid)
CREATE OR REPLACE FUNCTION public._pay_batch_provider_submit_preflight_recheck(p_pay_batch_id uuid, p_scope_json jsonb DEFAULT '{}'::jsonb, p_operation_id uuid DEFAULT NULL::uuid, p_actor_user_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_uuid_regex text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';
  v_scope_json jsonb := COALESCE(p_scope_json, '{}'::jsonb);
  v_effective_scope_json jsonb := '{}'::jsonb;
  v_resolved_scope_json jsonb := '{}'::jsonb;
  v_pay_batch_item_ids uuid[] := ARRAY[]::uuid[];
  v_pay_bank_transfer_ids uuid[] := ARRAY[]::uuid[];
  v_blockers jsonb := '[]'::jsonb;
  v_provider_evidence_json jsonb := '{}'::jsonb;
  v_provider_submitted boolean := false;
  v_provider_evidence_class text := NULL::text;
  v_provider_cash_state text := NULL::text;
  v_transfer_provider_evidence_count integer := 0;
  v_operation_submit_attempt_evidence_count integer := 0;
  v_batch_status text := NULL::text;
  v_batch_cancelled_at_utc timestamptz := NULL::timestamptz;
  v_batch_execution_commit_state text := NULL::text;
  v_voided_item_count integer := 0;
  v_applied_pre_bank_cancel_count integer := 0;
  v_cancelled_transfer_count integer := 0;
  v_open_pre_bank_cancel_count integer := 0;
  v_other_operation_count integer := 0;
  v_other_chunk_count integer := 0;
BEGIN
  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'PAY_BATCH_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_BATCH_ID_REQUIRED')::text;
  END IF;

  IF v_scope_json IS NOT NULL AND jsonb_typeof(v_scope_json) <> 'object' THEN
    RAISE EXCEPTION 'SCOPE_JSON_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'SCOPE_JSON_MUST_BE_OBJECT', 'pay_batch_id', p_pay_batch_id)::text;
  END IF;

  IF v_scope_json IS NULL OR jsonb_typeof(v_scope_json) <> 'object' OR NULLIF(btrim(COALESCE(v_scope_json->>'scope_type', '')), '') IS NULL THEN
    v_effective_scope_json := COALESCE(v_scope_json, '{}'::jsonb) || jsonb_build_object('scope_type', 'BATCH');
  ELSE
    v_effective_scope_json := v_scope_json;
  END IF;

  SELECT
    batch_rows.status,
    batch_rows.cancelled_at_utc,
    batch_rows.execution_commit_state
  INTO
    v_batch_status,
    v_batch_cancelled_at_utc,
    v_batch_execution_commit_state
  FROM public.pay_batches AS batch_rows
  WHERE batch_rows.id = p_pay_batch_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_BATCH_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_BATCH_NOT_FOUND', 'pay_batch_id', p_pay_batch_id)::text;
  END IF;

  v_resolved_scope_json := public._pay_resolve_payment_scope_for_cancel_rewind(
    p_pay_batch_id,
    v_effective_scope_json,
    true,
    p_actor_user_id
  );

  WITH raw_values AS (
    SELECT item_values.raw_value
    FROM jsonb_array_elements_text(
      CASE
        WHEN jsonb_typeof(v_resolved_scope_json->'pay_batch_item_ids') = 'array' THEN v_resolved_scope_json->'pay_batch_item_ids'
        ELSE '[]'::jsonb
      END
    ) AS item_values(raw_value)
  ), clean_values AS (
    SELECT NULLIF(btrim(raw_values.raw_value), '') AS clean_value
    FROM raw_values
  )
  SELECT COALESCE(array_agg(clean_values.clean_value::uuid), ARRAY[]::uuid[])
  INTO v_pay_batch_item_ids
  FROM clean_values
  WHERE clean_values.clean_value IS NOT NULL
    AND clean_values.clean_value ~ v_uuid_regex;

  WITH raw_values AS (
    SELECT transfer_values.raw_value
    FROM jsonb_array_elements_text(
      CASE
        WHEN jsonb_typeof(v_resolved_scope_json->'pay_bank_transfer_ids') = 'array' THEN v_resolved_scope_json->'pay_bank_transfer_ids'
        ELSE '[]'::jsonb
      END
    ) AS transfer_values(raw_value)
  ), clean_values AS (
    SELECT NULLIF(btrim(raw_values.raw_value), '') AS clean_value
    FROM raw_values
  )
  SELECT COALESCE(array_agg(clean_values.clean_value::uuid), ARRAY[]::uuid[])
  INTO v_pay_bank_transfer_ids
  FROM clean_values
  WHERE clean_values.clean_value IS NOT NULL
    AND clean_values.clean_value ~ v_uuid_regex;

  IF upper(btrim(COALESCE(v_batch_status, ''))) = 'CANCELLED'
     OR v_batch_cancelled_at_utc IS NOT NULL THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'TRANSFER_CANCELLED_BEFORE_PROVIDER_SUBMIT',
      'reason', 'The pay batch is already cancelled before provider submission.',
      'pay_batch_id', p_pay_batch_id::text,
      'batch_status', v_batch_status,
      'cancelled_at_utc', v_batch_cancelled_at_utc
    ));
  END IF;

  SELECT count(*)::integer
  INTO v_voided_item_count
  FROM public.pay_batch_items AS item_rows
  WHERE item_rows.id = ANY(COALESCE(v_pay_batch_item_ids, ARRAY[]::uuid[]))
    AND COALESCE(item_rows.is_voided, false) = true;

  IF COALESCE(v_voided_item_count, 0) > 0 THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'PAYMENT_ITEMS_VOIDED_BEFORE_PROVIDER_SUBMIT',
      'reason', 'One or more payment items in the provider-submit scope have already been voided.',
      'voided_item_count', v_voided_item_count
    ));
  END IF;

  SELECT count(*)::integer
  INTO v_applied_pre_bank_cancel_count
  FROM public.pay_payment_correction_items AS correction_item_rows
  WHERE correction_item_rows.pay_batch_id = p_pay_batch_id
    AND correction_item_rows.correction_item_kind = 'PRE_BANK_CANCEL'
    AND upper(btrim(COALESCE(correction_item_rows.status, ''))) = 'APPLIED'
    AND (
      correction_item_rows.pay_batch_item_id = ANY(COALESCE(v_pay_batch_item_ids, ARRAY[]::uuid[]))
      OR correction_item_rows.pay_bank_transfer_id = ANY(COALESCE(v_pay_bank_transfer_ids, ARRAY[]::uuid[]))
    );

  IF COALESCE(v_applied_pre_bank_cancel_count, 0) > 0 THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'PAYMENT_ITEMS_VOIDED_BEFORE_PROVIDER_SUBMIT',
      'reason', 'One or more payment items in the provider-submit scope already have an applied PRE_BANK_CANCEL correction.',
      'applied_pre_bank_cancel_count', v_applied_pre_bank_cancel_count
    ));
  END IF;

  SELECT count(*)::integer
  INTO v_cancelled_transfer_count
  FROM public.pay_bank_transfers AS transfer_rows
  WHERE transfer_rows.id = ANY(COALESCE(v_pay_bank_transfer_ids, ARRAY[]::uuid[]))
    AND upper(btrim(COALESCE(transfer_rows.status, ''))) IN (
      'CANCELLED',
      'CANCELED',
      'VOIDED',
      'LOCALLY_CANCELLED',
      'LOCAL_CANCELLED',
      'CANCELLED_BEFORE_BANK_SUBMISSION',
      'CANCELED_BEFORE_BANK_SUBMISSION'
    );

  IF COALESCE(v_cancelled_transfer_count, 0) > 0 THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'TRANSFER_CANCELLED_BEFORE_PROVIDER_SUBMIT',
      'reason', 'One or more bank transfer rows in the provider-submit scope have already been locally cancelled.',
      'cancelled_transfer_count', v_cancelled_transfer_count
    ));
  END IF;

  SELECT count(*)::integer
  INTO v_open_pre_bank_cancel_count
  FROM public.pay_payment_correction_requests AS request_rows
  WHERE request_rows.pay_batch_id = p_pay_batch_id
    AND upper(btrim(COALESCE(request_rows.correction_kind, ''))) = 'PRE_BANK_CANCEL'
    AND upper(btrim(COALESCE(request_rows.status, ''))) IN (
      'REQUESTED',
      'PENDING',
      'AUTHORISED',
      'AUTHORIZED',
      'IN_PROGRESS',
      'PROCESSING',
      'EXPANDED'
    );

  IF COALESCE(v_open_pre_bank_cancel_count, 0) > 0 THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT',
      'reason', 'A pre-bank cancellation request exists for this batch/scope, so provider submission must not proceed.',
      'open_pre_bank_cancel_count', v_open_pre_bank_cancel_count
    ));
  END IF;

  SELECT jsonb_build_object(
      'evidence_class', evidence_rows.evidence_class,
      'provider_submitted', evidence_rows.provider_submitted,
      'provider_request_sent', evidence_rows.provider_request_sent,
      'provider_response_present', evidence_rows.provider_response_present,
      'provider_event_present', evidence_rows.provider_event_present,
      'provider_external_id_present', evidence_rows.provider_external_id_present,
      'local_prepared_only', evidence_rows.local_prepared_only,
      'cash_state', evidence_rows.cash_state,
      'blocker_code', evidence_rows.blocker_code,
      'reason', evidence_rows.reason,
      'support_details_json', evidence_rows.support_details_json
    ),
    evidence_rows.provider_submitted,
    evidence_rows.evidence_class,
    evidence_rows.cash_state
  INTO
    v_provider_evidence_json,
    v_provider_submitted,
    v_provider_evidence_class,
    v_provider_cash_state
  FROM public._pay_bank_transfer_provider_evidence_classify(
    p_pay_batch_id,
    NULL::uuid,
    v_effective_scope_json,
    p_operation_id
  ) AS evidence_rows
  LIMIT 1;

  v_transfer_provider_evidence_count :=
    COALESCE(NULLIF(v_provider_evidence_json #>> '{support_details_json,provider_external_id_count}', '')::integer, 0)
    + COALESCE(NULLIF(v_provider_evidence_json #>> '{support_details_json,provider_response_count}', '')::integer, 0)
    + COALESCE(NULLIF(v_provider_evidence_json #>> '{support_details_json,provider_event_count}', '')::integer, 0)
    + COALESCE(NULLIF(v_provider_evidence_json #>> '{support_details_json,provider_request_sent_count}', '')::integer, 0)
    + COALESCE(NULLIF(v_provider_evidence_json #>> '{support_details_json,provider_outcome_unknown_count}', '')::integer, 0);

  v_operation_submit_attempt_evidence_count := COALESCE(NULLIF(v_provider_evidence_json #>> '{support_details_json,operation_submit_attempt_count}', '')::integer, 0);

  IF COALESCE(v_provider_submitted, false)
     AND (
       COALESCE(v_transfer_provider_evidence_count, 0) > 0
       OR (p_operation_id IS NULL AND COALESCE(v_operation_submit_attempt_evidence_count, 0) > 0)
     ) THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'PROVIDER_SUBMISSION_ALREADY_CLAIMED',
      'reason', 'Provider submission evidence already exists for this payment scope.',
      'evidence_class', v_provider_evidence_class,
      'cash_state', v_provider_cash_state,
      'transfer_provider_evidence_count', v_transfer_provider_evidence_count,
      'operation_submit_attempt_evidence_count', v_operation_submit_attempt_evidence_count
    ));
  END IF;

  SELECT count(*)::integer
  INTO v_other_operation_count
  FROM public.banking_pay_operations AS operation_rows
  WHERE operation_rows.pay_batch_id = p_pay_batch_id
    AND (p_operation_id IS NULL OR operation_rows.id <> p_operation_id)
    AND upper(btrim(COALESCE(operation_rows.operation_type, ''))) IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS', 'PAYMENT_SETTLEMENT')
    AND upper(btrim(COALESCE(operation_rows.status, ''))) IN ('QUEUED', 'CLAIMED', 'LOCKED', 'RUNNING', 'PROCESSING', 'IN_PROGRESS', 'REVIEW_REQUIRED');

  SELECT count(*)::integer
  INTO v_other_chunk_count
  FROM public.banking_pay_operation_chunks AS chunk_rows
  JOIN public.banking_pay_operations AS operation_rows
    ON operation_rows.id = chunk_rows.operation_id
  WHERE operation_rows.pay_batch_id = p_pay_batch_id
    AND (p_operation_id IS NULL OR operation_rows.id <> p_operation_id)
    AND upper(btrim(COALESCE(operation_rows.operation_type, ''))) IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS', 'PAYMENT_SETTLEMENT')
    AND upper(btrim(COALESCE(chunk_rows.status, ''))) IN ('PENDING', 'CLAIMED', 'LOCKED', 'RUNNING', 'PROCESSING');

  IF COALESCE(v_other_operation_count, 0) + COALESCE(v_other_chunk_count, 0) > 0 THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'PROVIDER_SUBMISSION_IN_PROGRESS',
      'reason', 'Another payment provider operation/chunk is already in progress for this batch.',
      'other_operation_count', v_other_operation_count,
      'other_chunk_count', v_other_chunk_count
    ));
  END IF;

  RETURN jsonb_build_object(
    'ok', COALESCE(jsonb_array_length(v_blockers), 0) = 0,
    'pay_batch_id', p_pay_batch_id::text,
    'operation_id', CASE WHEN p_operation_id IS NULL THEN NULL ELSE p_operation_id::text END,
    'blockers', COALESCE(v_blockers, '[]'::jsonb),
    'resolved_scope_json', COALESCE(v_resolved_scope_json, '{}'::jsonb),
    'provider_evidence_json', COALESCE(v_provider_evidence_json, '{}'::jsonb),
    'support_details_json', jsonb_build_object(
      'batch_status', v_batch_status,
      'batch_cancelled_at_utc', v_batch_cancelled_at_utc,
      'batch_execution_commit_state', v_batch_execution_commit_state,
      'voided_item_count', COALESCE(v_voided_item_count, 0),
      'applied_pre_bank_cancel_count', COALESCE(v_applied_pre_bank_cancel_count, 0),
      'cancelled_transfer_count', COALESCE(v_cancelled_transfer_count, 0),
      'open_pre_bank_cancel_count', COALESCE(v_open_pre_bank_cancel_count, 0),
      'transfer_provider_evidence_count', COALESCE(v_transfer_provider_evidence_count, 0),
      'operation_submit_attempt_evidence_count', COALESCE(v_operation_submit_attempt_evidence_count, 0),
      'other_operation_count', COALESCE(v_other_operation_count, 0),
      'other_chunk_count', COALESCE(v_other_chunk_count, 0),
      'actor_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END
    )
  );
END;
$function$;

-- _pay_batch_status_display_label(text,integer,integer)
CREATE OR REPLACE FUNCTION public._pay_batch_status_display_label(p_status text, p_pending_count integer DEFAULT NULL::integer, p_unknown_count integer DEFAULT NULL::integer)
 RETURNS text
 LANGUAGE plpgsql
 IMMUTABLE PARALLEL SAFE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_status text := NULLIF(REPLACE(REPLACE(UPPER(BTRIM(COALESCE(p_status, ''))), '-', '_'), ' ', '_'), '');
  v_pending_count integer := GREATEST(COALESCE(p_pending_count, 0), 0);
  v_unknown_count integer := GREATEST(COALESCE(p_unknown_count, 0), 0);
BEGIN
  IF v_status IS NULL THEN
    RETURN 'Unknown';
  END IF;

  IF v_status = 'FAILED' THEN
    RETURN 'Completed with failed payments';
  END IF;

  IF v_status IN ('SETTLED', 'COMPLETED', 'COMPLETE', 'COMMITTED') THEN
    RETURN 'Completed';
  END IF;

  IF v_status = 'WAITING_BANK_CONFIRM' THEN
    RETURN 'Waiting for bank confirmation';
  END IF;

  IF v_status = 'PARTIAL' THEN
    RETURN 'Partially completed';
  END IF;

  IF v_status IN ('DRAFT', 'DRAFT_CREATED') THEN
    RETURN 'Draft';
  END IF;

  IF v_status = 'READY' THEN
    RETURN 'Ready';
  END IF;

  IF v_status = 'AWAITING_AUTHORISATION' THEN
    RETURN 'Awaiting authorisation';
  END IF;

  IF v_status = 'AUTHORISED_FOR_PAYMENT' THEN
    RETURN 'Authorised for payment';
  END IF;

  IF v_status = 'SCHEDULED' THEN
    RETURN 'Scheduled';
  END IF;

  IF v_status = 'EXECUTING' THEN
    RETURN 'Executing';
  END IF;

  IF v_status = 'BLOCKED_FUNDS' THEN
    RETURN 'Blocked funds';
  END IF;

  IF v_status = 'CANCELLED' THEN
    RETURN 'Cancelled';
  END IF;

  IF v_status = 'UNPAID' THEN
    RETURN 'Unpaid';
  END IF;

  IF v_status = 'PAID' THEN
    RETURN 'Paid';
  END IF;

  IF v_pending_count > 0 OR v_unknown_count > 0 THEN
    RETURN INITCAP(REPLACE(v_status, '_', ' '));
  END IF;

  RETURN INITCAP(REPLACE(v_status, '_', ' '));
END;
$function$;

-- _pay_batch_status_is_active_reservation(text)
CREATE OR REPLACE FUNCTION public._pay_batch_status_is_active_reservation(p_status text)
 RETURNS boolean
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT upper(btrim(coalesce(p_status, ''))) IN (
    'DRAFT',
    'DRAFT_CREATED',
    'READY',
    'WAITING_BANK_CONFIRM',
    'PARTIAL',
    'BLOCKED_FUNDS',
    'SCHEDULED',
    'EXECUTING',
    'AWAITING_AUTHORISATION',
    'AUTHORISED_FOR_PAYMENT'
  );
$function$;

-- _pay_batch_validate_freshness_base_v1(uuid,uuid,boolean)
CREATE OR REPLACE FUNCTION public._pay_batch_validate_freshness_base_v1(p_pay_batch_id uuid, p_actor_user_id uuid, p_allow_large_full_scan boolean DEFAULT false)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now_utc timestamptz := now();
  v_date_context jsonb := '{}'::jsonb;
  v_today_uk date := NULL::date;

  v_pay_date date;
  v_week_start date;
  v_batch_kind_fixed text;
  v_scope text;

  v_ts_ids uuid[] := array[]::uuid[];

  v_is_stale boolean := false;
  v_reasons text[] := array[]::text[];

  v_diffs jsonb := '[]'::jsonb;

  v_ts_changed_ct int := 0;
  v_key_diff_ct int := 0;
  v_finance_reservation_diff_ct int := 0;
  v_snooze_diff_ct int := 0;
  v_restructure_diff_ct int := 0;
  v_writeoff_diff_ct int := 0;
  v_timesheet_override_diff_ct int := 0;
  v_paye_guardrail_diff_ct int := 0;
  v_overpay_ded_diff_ct int := 0;
  v_manual_debt_ded_diff_ct int := 0;
  v_loan_ded_diff_ct int := 0;
  v_ded_diff_ct int := 0;
  v_paye_net_diff_ct int := 0;

  v_batch_created_at_utc timestamptz;
  v_batch_status text;
  v_batch_is_active_reservation boolean := false;
  v_finance_reservation_expected_status text := 'RESERVED';
  v_same_week_paye_override_used boolean := false;
  v_paye_guardrails jsonb := '{}'::jsonb;

  v_diff_limit int := 500;

  -- Scalable execution guard. Normal execute operation flow must use the
  -- chunked freshness RPCs. This legacy/full validator remains available for
  -- small/manual diagnostics, or explicit admin diagnostics when
  -- p_allow_large_full_scan = true. Counts are derived only from frozen
  -- batch/payment artifacts, never live TSFIN.
  v_allow_large_full_scan boolean := false;
  v_candidate_count integer := 0;
  v_item_count integer := 0;
  v_item_breakdown_count integer := 0;
  v_transfer_count integer := 0;
  v_large_batch boolean := false;
  v_large_batch_reasons jsonb := '[]'::jsonb;
  v_chunked_required_diff jsonb := '[]'::jsonb;
  v_carry_forward_freshness_result jsonb := '{}'::jsonb;
  v_carry_forward_blocker_count integer := 0;
begin
  PERFORM public.banking_pay_hot_path_budget_apply('DIAGNOSTIC');

  v_allow_large_full_scan := coalesce(p_allow_large_full_scan, false);

  if p_pay_batch_id is null then
    raise exception 'pay_batch_validate_freshness: pay_batch_id is required';
  end if;

  if p_actor_user_id is null then
    raise exception 'pay_batch_validate_freshness: actor_user_id is required';
  end if;

  select
    pb.pay_date,
    pb.batch_kind_fixed,
    pb.created_at_utc,
    upper(coalesce(pb.status, '')),
    coalesce(pb.same_week_paye_override_used, false)
  into
    v_pay_date,
    v_batch_kind_fixed,
    v_batch_created_at_utc,
    v_batch_status,
    v_same_week_paye_override_used
  from public.pay_batches pb
  where pb.id = p_pay_batch_id
  limit 1;

  if v_pay_date is null then
    raise exception 'pay_batch_validate_freshness: pay_batch_id not found (%).', p_pay_batch_id::text;
  end if;

  v_date_context := public.pay_banking_official_date_context_v1(NULL::timestamptz);
  v_today_uk := case
    when coalesce(v_date_context->>'london_current_date', '') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
      then (v_date_context->>'london_current_date')::date
    else NULL::date
  end;

  if v_today_uk is null then
    raise exception 'PAY_BATCH_FRESHNESS_LONDON_DATE_UNAVAILABLE'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'code', 'PAY_BATCH_FRESHNESS_LONDON_DATE_UNAVAILABLE',
              'pay_batch_id', p_pay_batch_id::text,
              'message', 'The current Europe/London business date could not be resolved for full batch freshness.'
            )::text;
  end if;

  select count(*)::integer
  into v_candidate_count
  from public.pay_batch_candidates as guard_candidate_count
  where guard_candidate_count.pay_batch_id = p_pay_batch_id;

  select count(*)::integer
  into v_item_count
  from public.pay_batch_items as guard_item_count
  join public.pay_batch_candidates as guard_item_candidate
    on guard_item_candidate.id = guard_item_count.pay_batch_candidate_id
  where guard_item_candidate.pay_batch_id = p_pay_batch_id;

  select count(*)::integer
  into v_item_breakdown_count
  from public.pay_batch_item_breakdowns as guard_breakdown_count
  join public.pay_batch_items as guard_breakdown_item
    on guard_breakdown_item.id = guard_breakdown_count.pay_batch_item_id
  join public.pay_batch_candidates as guard_breakdown_candidate
    on guard_breakdown_candidate.id = guard_breakdown_item.pay_batch_candidate_id
  where guard_breakdown_candidate.pay_batch_id = p_pay_batch_id;

  select count(*)::integer
  into v_transfer_count
  from public.pay_bank_transfers as guard_transfer_count
  where guard_transfer_count.pay_batch_id = p_pay_batch_id;

  v_large_batch := (
    coalesce(v_candidate_count, 0) > 100
    or coalesce(v_item_count, 0) > 250
    or coalesce(v_transfer_count, 0) > 100
    or coalesce(v_item_breakdown_count, 0) > 500
  );

  select coalesce(jsonb_agg(guard_reason.reason_json order by guard_reason.reason_key), '[]'::jsonb)
  into v_large_batch_reasons
  from (
    select
      'candidate_count'::text as reason_key,
      jsonb_build_object('metric', 'candidate_count', 'count', coalesce(v_candidate_count, 0), 'threshold', 100) as reason_json
    where coalesce(v_candidate_count, 0) > 100

    union all

    select
      'item_count'::text as reason_key,
      jsonb_build_object('metric', 'item_count', 'count', coalesce(v_item_count, 0), 'threshold', 250) as reason_json
    where coalesce(v_item_count, 0) > 250

    union all

    select
      'transfer_count'::text as reason_key,
      jsonb_build_object('metric', 'transfer_count', 'count', coalesce(v_transfer_count, 0), 'threshold', 100) as reason_json
    where coalesce(v_transfer_count, 0) > 100

    union all

    select
      'item_breakdown_count'::text as reason_key,
      jsonb_build_object('metric', 'item_breakdown_count', 'count', coalesce(v_item_breakdown_count, 0), 'threshold', 500) as reason_json
    where coalesce(v_item_breakdown_count, 0) > 500
  ) as guard_reason;

  if v_large_batch and v_allow_large_full_scan is not true then
    v_chunked_required_diff := jsonb_build_array(
      jsonb_build_object(
        'timesheet_id', null,
        'key_type', 'FRESHNESS_MODE',
        'key_value', 'FULL_BATCH_SCAN_GUARD',
        'expected', jsonb_build_object(
          'required_path', 'chunked_freshness',
          'seed_rpc', 'pay_batch_freshness_scope_seed',
          'chunk_rpc', 'pay_batch_validate_freshness_chunk',
          'aggregate_rpc', 'pay_batch_freshness_result_get'
        ),
        'actual', jsonb_build_object(
          'requested_path', 'pay_batch_validate_freshness_full_scan',
          'p_allow_large_full_scan', v_allow_large_full_scan,
          'candidate_count', coalesce(v_candidate_count, 0),
          'item_count', coalesce(v_item_count, 0),
          'transfer_count', coalesce(v_transfer_count, 0),
          'item_breakdown_count', coalesce(v_item_breakdown_count, 0),
          'large_batch_reasons', coalesce(v_large_batch_reasons, '[]'::jsonb)
        )
      )
    );

    return jsonb_build_object(
      'is_stale', true,
      'stale_reasons', jsonb_build_array('FULL_BATCH_FRESHNESS_REQUIRES_CHUNKED_VALIDATION'),
      'diff', v_chunked_required_diff,
      'requires_chunked_freshness', true,
      'code', 'FULL_BATCH_FRESHNESS_REQUIRES_CHUNKED_VALIDATION',
      'message', 'This batch is too large for the legacy full-batch freshness validator. Use the chunked freshness operation path.',
      'pay_batch_id', p_pay_batch_id::text,
      'counts', jsonb_build_object(
        'candidate_count', coalesce(v_candidate_count, 0),
        'item_count', coalesce(v_item_count, 0),
        'transfer_count', coalesce(v_transfer_count, 0),
        'item_breakdown_count', coalesce(v_item_breakdown_count, 0)
      ),
      'thresholds', jsonb_build_object(
        'candidate_count', 100,
        'item_count', 250,
        'transfer_count', 100,
        'item_breakdown_count', 500
      ),
      'large_batch_reasons', coalesce(v_large_batch_reasons, '[]'::jsonb)
    );
  end if;

  v_batch_is_active_reservation := public._pay_batch_status_is_active_reservation(v_batch_status);

  if v_batch_status in ('AUTHORISED_FOR_PAYMENT','SCHEDULED','EXECUTING') then
    v_finance_reservation_expected_status := 'COMMITTED';
  else
    v_finance_reservation_expected_status := 'RESERVED';
  end if;

  v_week_start := public._pay_week_start_monday(v_pay_date);

  select
    max(pbi.pay_channel)
  into
    v_scope
  from public.pay_batch_items pbi
  join public.pay_batch_candidates pbc
    on pbc.id = pbi.pay_batch_candidate_id
  where pbc.pay_batch_id = p_pay_batch_id
    and coalesce(pbi.is_voided, false) = false
      and not exists (
        select 1
        from public.pay_payment_correction_items as ppc_pbi_fresh_exclusion
        where ppc_pbi_fresh_exclusion.pay_batch_item_id = pbi.id
          and ppc_pbi_fresh_exclusion.status = 'APPLIED'
          and ppc_pbi_fresh_exclusion.correction_item_kind in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
      )
    and pbi.pay_channel in ('PAYE','UMBRELLA');

  v_scope := upper(btrim(coalesce(v_scope,'')));
  if v_scope not in ('PAYE','UMBRELLA') then
    v_scope := null;
  end if;

  select
    coalesce(
      (
        select array_agg(distinct t1.timesheet_id)
        from (
          select pbts.timesheet_id
          from public.pay_batch_timesheet_snapshots pbts
          where pbts.pay_batch_id = p_pay_batch_id
            and pbts.timesheet_id is not null

          union all

          select pbi2.timesheet_id
          from public.pay_batch_items pbi2
          join public.pay_batch_candidates pbc2
            on pbc2.id = pbi2.pay_batch_candidate_id
          where pbc2.pay_batch_id = p_pay_batch_id
            and coalesce(pbi2.is_voided, false) = false
            and not exists (
              select 1
              from public.pay_payment_correction_items as ppc_pbi2_fresh_exclusion
              where ppc_pbi2_fresh_exclusion.pay_batch_item_id = pbi2.id
                and ppc_pbi2_fresh_exclusion.status = 'APPLIED'
                and ppc_pbi2_fresh_exclusion.correction_item_kind in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
            )
            and pbi2.timesheet_id is not null
        ) t1
      ),
      array[]::uuid[]
    )
  into v_ts_ids;

  create temporary table if not exists pg_temp.tmp_fresh_ts_diffs (
    timesheet_id uuid null,
    key_type text not null,
    key_value text not null,
    expected_text text null,
    actual_text text null,
    ord int not null
  ) on commit drop;

  create temporary table if not exists pg_temp.tmp_fresh_key_diffs (
    timesheet_id uuid null,
    key_type text not null,
    key_value text not null,
    expected_ex numeric(12,2) null,
    actual_ex numeric(12,2) null,
    ord int not null
  ) on commit drop;

  create temporary table if not exists pg_temp.tmp_fresh_candidate_earnings (
    pay_batch_candidate_id uuid not null,
    candidate_id uuid not null,
    awaiting_net_amount boolean not null,
    earnings_before_loan_ex numeric(12,2) not null
  ) on commit drop;

  create temporary table if not exists pg_temp.tmp_fresh_expected_overpay_alloc (
    candidate_id uuid not null,
    advance_id uuid not null,
    take_ex numeric(12,2) not null,
    ord int not null
  ) on commit drop;

  create temporary table if not exists pg_temp.tmp_fresh_candidate_expected_overpay (
    candidate_id uuid not null,
    expected_overpayment_recovery_ex numeric(12,2) not null
  ) on commit drop;

  create temporary table if not exists pg_temp.tmp_fresh_expected_loan_alloc (
    candidate_id uuid not null,
    loan_id uuid not null,
    take_ex numeric(12,2) not null,
    ord int not null
  ) on commit drop;

  create temporary table if not exists pg_temp.tmp_fresh_deduction_diffs (
    key_type text not null,
    key_value text not null,
    expected_ex numeric(12,2) null,
    actual_ex numeric(12,2) null,
    ord int not null
  ) on commit drop;

  create temporary table if not exists pg_temp.tmp_fresh_paye_net_diffs (
    candidate_id uuid not null,
    key_type text not null,
    key_value text not null,
    expected_ex numeric(12,2) null,
    actual_ex numeric(12,2) null,
    ord int not null
  ) on commit drop;

  create temporary table if not exists pg_temp.tmp_fresh_expected_manual_debt_alloc (
    candidate_id uuid not null,
    finance_case_id uuid not null,
    take_ex numeric(12,2) not null,
    ord int not null
  ) on commit drop;

  create temporary table if not exists pg_temp.tmp_fresh_state_diffs (
    timesheet_id uuid null,
    candidate_id uuid null,
    key_type text not null,
    key_value text not null,
    expected_text text null,
    actual_text text null,
    ord int not null
  ) on commit drop;

  truncate table pg_temp.tmp_fresh_ts_diffs;
  truncate table pg_temp.tmp_fresh_key_diffs;
  truncate table pg_temp.tmp_fresh_candidate_earnings;
  truncate table pg_temp.tmp_fresh_expected_overpay_alloc;
  truncate table pg_temp.tmp_fresh_candidate_expected_overpay;
  truncate table pg_temp.tmp_fresh_expected_loan_alloc;
  truncate table pg_temp.tmp_fresh_expected_manual_debt_alloc;
  truncate table pg_temp.tmp_fresh_deduction_diffs;
  truncate table pg_temp.tmp_fresh_paye_net_diffs;
  truncate table pg_temp.tmp_fresh_state_diffs;

  ---------------------------------------------------------------------------
  -- TIMESHEET_CHANGED: Policy X keeps blocking freshness checks in the
  -- economic entitlement keyspace. The legacy whole-snapshot signature
  -- comparison is intentionally disabled as a blocking stale gate here.
  ---------------------------------------------------------------------------
  v_ts_changed_ct := 0;

  ---------------------------------------------------------------------------
  -- RESERVATION_CHANGED: compare this batch's frozen source entitlement
  -- against the current entitlement available before this batch.
  --
  -- Policy X (post-draft): the batch side is resolved only from frozen
  -- batch artifacts via _pay_batch_item_economic_components. Current
  -- live truth is comparison-only via _pay_current_timesheet_entitlement_components.
  -- This section must not call the broad outstanding helper, must not
  -- remap batch items from live TSFIN, and must not validate resolved rows
  -- against target payout amounts.
  ---------------------------------------------------------------------------
  if v_batch_is_active_reservation then
    insert into pg_temp.tmp_fresh_key_diffs (
      timesheet_id,
      key_type,
      key_value,
      expected_ex,
      actual_ex,
      ord
    )
    with
    this_components_raw as (
      select
        pbec.pay_batch_item_id,
        pbec.timesheet_id,
        upper(nullif(btrim(coalesce(pbec.item_type, '')), '')) as item_type,
        upper(nullif(btrim(coalesce(pbec.key_type, '')), '')) as key_type,
        nullif(btrim(coalesce(pbec.key_value, '')), '') as key_value,
        pbec.source_amount_ex_vat as source_amount_ex_vat_raw,
        pbec.target_amount_ex_vat as target_amount_ex_vat_raw,
        nullif(btrim(coalesce(pbec.key_resolution_failure_reason, '')), '') as key_resolution_failure_reason
      from public._pay_batch_item_economic_components(
        p_pay_batch_id => p_pay_batch_id,
        p_pay_batch_item_ids => null::uuid[]
      ) as pbec
      join public.pay_batch_items as pbi_this_fresh
        on pbi_this_fresh.id = pbec.pay_batch_item_id
      join public.pay_batch_candidates as pbc_this_fresh
        on pbc_this_fresh.id = pbi_this_fresh.pay_batch_candidate_id
      where pbc_this_fresh.pay_batch_id = p_pay_batch_id
        and coalesce(pbi_this_fresh.is_voided, false) = false
        and not exists (
          select 1
          from public.pay_payment_correction_items as ppc_this_fresh_exclusion
          where ppc_this_fresh_exclusion.pay_batch_item_id = pbi_this_fresh.id
            and ppc_this_fresh_exclusion.status = 'APPLIED'
            and ppc_this_fresh_exclusion.correction_item_kind in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
        )
        and upper(nullif(btrim(coalesce(pbec.item_type, '')), '')) in ('SEGMENT_DELTA','EXPENSE_DELTA','ADJUSTMENT_DELTA','MILEAGE_DELTA')
    ),
    this_component_resolution_failures as (
      select
        tcr.timesheet_id,
        case
          when tcr.key_resolution_failure_reason is not null then tcr.key_resolution_failure_reason
          when tcr.key_type is null or tcr.key_value is null then 'KEY_RESOLUTION_FAILURE'
          when tcr.key_type not in ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE') then 'KEY_RESOLUTION_FAILURE'
          when tcr.key_type = 'TS_DAY' and tcr.key_value !~ '^\d{4}-\d{2}-\d{2}$' then 'TS_DAY_KEY_VALUE_NOT_DATE'
          when tcr.source_amount_ex_vat_raw is null then 'SOURCE_RESOLUTION_FAILURE'
          else 'SOURCE_RESOLUTION_FAILURE'
        end as key_type,
        coalesce(tcr.pay_batch_item_id::text, 'UNKNOWN_PAY_BATCH_ITEM') as key_value,
        0::numeric(12,2) as expected_ex,
        1::numeric(12,2) as actual_ex,
        2 as ord
      from this_components_raw as tcr
      where tcr.key_resolution_failure_reason is not null
         or tcr.key_type is null
         or tcr.key_value is null
         or tcr.key_type not in ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE')
         or (tcr.key_type = 'TS_DAY' and tcr.key_value !~ '^\d{4}-\d{2}-\d{2}$')
         or tcr.source_amount_ex_vat_raw is null
    ),
    this_components as (
      select
        tcr.timesheet_id,
        tcr.key_type,
        tcr.key_value,
        round(sum(round(coalesce(tcr.source_amount_ex_vat_raw, 0), 2)), 2)::numeric(12,2) as current_source_ex_vat
      from this_components_raw as tcr
      where tcr.timesheet_id is not null
        and tcr.key_type is not null
        and tcr.key_value is not null
        and tcr.key_type in ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE')
        and not (tcr.key_type = 'TS_DAY' and tcr.key_value !~ '^\d{4}-\d{2}-\d{2}$')
        and tcr.key_resolution_failure_reason is null
        and tcr.source_amount_ex_vat_raw is not null
      group by
        tcr.timesheet_id,
        tcr.key_type,
        tcr.key_value
    ),
    current_truth_baseline as (
      select
        ctec.timesheet_id,
        upper(nullif(btrim(coalesce(ctec.key_type, '')), '')) as key_type,
        nullif(btrim(coalesce(ctec.key_value, '')), '') as key_value,
        round(sum(coalesce(ctec.truth_ex_vat, 0)), 2)::numeric(12,2) as truth_ex_vat,
        round(sum(coalesce(ctec.baseline_ex_vat, 0)), 2)::numeric(12,2) as baseline_ex_vat
      from public._pay_current_timesheet_entitlement_components(v_ts_ids) as ctec
      where ctec.timesheet_id is not null
        and ctec.key_type is not null
        and btrim(ctec.key_type) <> ''
        and ctec.key_value is not null
        and btrim(ctec.key_value) <> ''
        and upper(nullif(btrim(coalesce(ctec.key_type, '')), '')) in ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE')
        and not (
          upper(nullif(btrim(coalesce(ctec.key_type, '')), '')) = 'TS_DAY'
          and nullif(btrim(coalesce(ctec.key_value, '')), '') !~ '^\d{4}-\d{2}-\d{2}$'
        )
      group by
        ctec.timesheet_id,
        upper(nullif(btrim(coalesce(ctec.key_type, '')), '')),
        nullif(btrim(coalesce(ctec.key_value, '')), '')
    ),
    all_reserved as (
      select
        rc.timesheet_id,
        upper(nullif(btrim(coalesce(rc.key_type, '')), '')) as key_type,
        nullif(btrim(coalesce(rc.key_value, '')), '') as key_value,
        round(sum(coalesce(rc.amount_ex_vat, 0)), 2)::numeric(12,2) as all_reserved_ex_vat
      from public._pay_reserved_components(v_ts_ids) as rc
      where rc.timesheet_id is not null
        and rc.key_type is not null
        and btrim(rc.key_type) <> ''
        and rc.key_value is not null
        and btrim(rc.key_value) <> ''
        and upper(nullif(btrim(coalesce(rc.key_type, '')), '')) in ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE')
        and not (
          upper(nullif(btrim(coalesce(rc.key_type, '')), '')) = 'TS_DAY'
          and nullif(btrim(coalesce(rc.key_value, '')), '') !~ '^\d{4}-\d{2}-\d{2}$'
        )
      group by
        rc.timesheet_id,
        upper(nullif(btrim(coalesce(rc.key_type, '')), '')),
        nullif(btrim(coalesce(rc.key_value, '')), '')
    ),
    reservation_comparison as (
      select
        tc.timesheet_id,
        tc.key_type,
        tc.key_value,
        tc.current_source_ex_vat as expected_ex,
        round(
          coalesce(ctb.truth_ex_vat, 0)
          - coalesce(ctb.baseline_ex_vat, 0)
          - (
              coalesce(ar.all_reserved_ex_vat, 0)
              - coalesce(tc.current_source_ex_vat, 0)
            ),
          2
        )::numeric(12,2) as actual_ex,
        2 as ord
      from this_components as tc
      left join current_truth_baseline as ctb
        on ctb.timesheet_id = tc.timesheet_id
       and ctb.key_type = tc.key_type
       and ctb.key_value = tc.key_value
      left join all_reserved as ar
        on ar.timesheet_id = tc.timesheet_id
       and ar.key_type = tc.key_type
       and ar.key_value = tc.key_value
    ),
    reservation_mismatches as (
      select
        rc.timesheet_id,
        rc.key_type,
        rc.key_value,
        rc.expected_ex,
        rc.actual_ex,
        rc.ord
      from reservation_comparison as rc
      where abs(round(coalesce(rc.actual_ex, 0), 2) - round(coalesce(rc.expected_ex, 0), 2)) > 0.01
    )
    select
      tcrf.timesheet_id,
      tcrf.key_type,
      tcrf.key_value,
      tcrf.expected_ex,
      tcrf.actual_ex,
      tcrf.ord
    from this_component_resolution_failures as tcrf

    union all

    select
      rm.timesheet_id,
      rm.key_type,
      rm.key_value,
      rm.expected_ex,
      rm.actual_ex,
      rm.ord
    from reservation_mismatches as rm;
  end if;

  select count(*)::int
  into v_key_diff_ct
  from pg_temp.tmp_fresh_key_diffs;

  if v_key_diff_ct > 0 then
    v_is_stale := true;
    v_reasons := array_append(v_reasons, 'RESERVATION_CHANGED');
  end if;

  ---------------------------------------------------------------------------
  -- STATE_CHANGED: finance reservation rows, snoozes, restructure/write-off,
  -- timesheet-payment-override changes, and PAYE guardrail changes.
  ---------------------------------------------------------------------------
  insert into pg_temp.tmp_fresh_state_diffs (
    timesheet_id,
    candidate_id,
    key_type,
    key_value,
    expected_text,
    actual_text,
    ord
  )
  with batch_finance_items as (
    select
      pbi.timesheet_id,
      pbc.candidate_id,
      pbi.id as pay_batch_item_id,
      pbi.finance_case_id,
      pbi.reservation_id,
      pbi.item_type,
      pbi.repayment_week_start,
      round(abs(coalesce(pbi.frozen_source_amount, case when pbi.frozen_target_amount_ex_vat is null and pbi.frozen_resolution_mode is null and pbi.frozen_resolution_result_json is null then pbi.amount_ex_vat else null end, 0)), 2)::numeric(12,2) as reserved_amount_ex
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc
      on pbc.id = pbi.pay_batch_candidate_id
    where pbc.pay_batch_id = p_pay_batch_id
      and coalesce(pbi.is_voided, false) = false
      and not exists (
        select 1
        from public.pay_payment_correction_items as ppc_pbi_fresh_exclusion
        where ppc_pbi_fresh_exclusion.pay_batch_item_id = pbi.id
          and ppc_pbi_fresh_exclusion.status = 'APPLIED'
          and ppc_pbi_fresh_exclusion.correction_item_kind in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
      )
      and pbi.item_type in ('OVERPAYMENT_RECOVERY','LOAN_REPAYMENT','MANUAL_DEBT_RECOVERY')
      and pbi.finance_case_id is not null
  ),
  reservation_rows as (
    select
      par.id as reservation_id,
      par.finance_case_id,
      par.pay_batch_id,
      par.pay_batch_candidate_id,
      par.pay_batch_item_id,
      round(coalesce(par.reserved_source_amount, par.reserved_amount, 0), 2)::numeric(12,2) as reserved_amount_ex,
      par.repayment_week_start,
      upper(coalesce(par.status, '')) as reservation_status
    from public.pay_advance_reservations par
    where par.pay_batch_id = p_pay_batch_id
  )
  select
    bfi.timesheet_id,
    bfi.candidate_id,
    'FINANCE_RESERVATION' as key_type,
    bfi.pay_batch_item_id::text as key_value,
    jsonb_build_object(
      'finance_case_id', bfi.finance_case_id::text,
      'reservation_id', case when bfi.reservation_id is null then null else bfi.reservation_id::text end,
      'reserved_amount_ex', bfi.reserved_amount_ex,
      'repayment_week_start', case when bfi.repayment_week_start is null then null else bfi.repayment_week_start::text end,
      'status', v_finance_reservation_expected_status
    )::text as expected_text,
    case
      when rr.reservation_id is null then null
      else jsonb_build_object(
        'reservation_id', rr.reservation_id::text,
        'finance_case_id', rr.finance_case_id::text,
        'reserved_amount_ex', rr.reserved_amount_ex,
        'repayment_week_start', case when rr.repayment_week_start is null then null else rr.repayment_week_start::text end,
        'status', rr.reservation_status
      )::text
    end as actual_text,
    33 as ord
  from batch_finance_items bfi
  left join reservation_rows rr
    on rr.pay_batch_item_id = bfi.pay_batch_item_id
  where rr.reservation_id is null
     or rr.finance_case_id is distinct from bfi.finance_case_id
     or round(coalesce(rr.reserved_amount_ex, 0), 2) <> round(coalesce(bfi.reserved_amount_ex, 0), 2)
     or rr.repayment_week_start is distinct from bfi.repayment_week_start
     or rr.reservation_status <> v_finance_reservation_expected_status
     or (bfi.reservation_id is not null and rr.reservation_id is distinct from bfi.reservation_id);

  insert into pg_temp.tmp_fresh_state_diffs (
    timesheet_id,
    candidate_id,
    key_type,
    key_value,
    expected_text,
    actual_text,
    ord
  )
  with batch_items as (
    select
      pbi.timesheet_id,
      pbc.candidate_id,
      pbi.id as pay_batch_item_id,
      pbi.item_type,
      pbi.source_ref,
      pbi.finance_case_id,
      coalesce(
        case
          when pbi.finance_case_id is not null then 'advance:' || pbi.finance_case_id::text
          else null::text
        end,
        lower(nullif(btrim(coalesce(pbi.source_ref, '')), ''))
      ) as exact_source_ref,
      case
        when coalesce(
          case
            when pbi.finance_case_id is not null then 'advance:' || pbi.finance_case_id::text
            else null::text
          end,
          lower(nullif(btrim(coalesce(pbi.source_ref, '')), ''))
        ) ~* '^timesheet-expense:' then 'TIMESHEET_EXPENSE'
        when pbi.finance_case_id is not null
          or lower(nullif(btrim(coalesce(pbi.source_ref, '')), '')) ~* '^advance:' then 'FINANCE_CASE'
        when nullif(btrim(coalesce(pbi.source_ref, '')), '') is not null then 'EXACT_SOURCE'
        else 'TIMESHEET_OR_SEGMENT'
      end as source_identity_kind,
      nullif(btrim(coalesce(pbi.segment_key, '')), '') as segment_key,
      coalesce(
        nullif(btrim(pbi.frozen_source_basis_json #>> '{segment_stable_key}'), ''),
        nullif(btrim(pbi.frozen_source_basis_json #>> '{segment,stable_key}'), ''),
        nullif(btrim(pbi.frozen_component_snapshot_json #>> '{segment_stable_key}'), ''),
        nullif(btrim(pbi.frozen_component_snapshot_json #>> '{segment,stable_key}'), ''),
        nullif(btrim(pbi.payout_instruction_snapshot_json #>> '{segment_stable_key}'), ''),
        nullif(btrim(pbi.payout_instruction_snapshot_json #>> '{segment,stable_key}'), ''),
        nullif(btrim(coalesce(pbi.segment_key, '')), '')
      ) as segment_stable_key,
      coalesce(
        nullif(btrim(pbi.frozen_source_basis_json #>> '{segment_id}'), ''),
        nullif(btrim(pbi.frozen_source_basis_json #>> '{segment,id}'), ''),
        nullif(btrim(pbi.frozen_component_snapshot_json #>> '{segment_id}'), ''),
        nullif(btrim(pbi.frozen_component_snapshot_json #>> '{segment,id}'), ''),
        nullif(btrim(pbi.payout_instruction_snapshot_json #>> '{segment_id}'), ''),
        nullif(btrim(pbi.payout_instruction_snapshot_json #>> '{segment,id}'), '')
      ) as segment_id,
      coalesce(
        nullif(btrim(pbi.frozen_source_basis_json #>> '{booking_id}'), ''),
        nullif(btrim(pbi.frozen_source_basis_json #>> '{booking,booking_id}'), ''),
        nullif(btrim(pbi.frozen_component_snapshot_json #>> '{booking_id}'), ''),
        nullif(btrim(pbi.frozen_component_snapshot_json #>> '{booking,booking_id}'), ''),
        nullif(btrim(pbi.payout_instruction_snapshot_json #>> '{booking_id}'), ''),
        nullif(btrim(pbi.payout_instruction_snapshot_json #>> '{booking,booking_id}'), ''),
        (
          select coalesce(
            nullif(btrim(snapshot_row.target_snapshot_json #>> '{booking_id}'), ''),
            nullif(btrim(snapshot_row.target_snapshot_json #>> '{booking,booking_id}'), ''),
            nullif(btrim(snapshot_row.target_snapshot_json #>> '{timesheet,booking_id}'), ''),
            nullif(btrim(snapshot_row.target_snapshot_json #>> '{target,booking_id}'), '')
          )
          from public.pay_batch_timesheet_snapshots as snapshot_row
          where snapshot_row.pay_batch_id = p_pay_batch_id
            and snapshot_row.timesheet_id = pbi.timesheet_id
          order by snapshot_row.id
          limit 1
        )
      ) as booking_id
    from public.pay_batch_items as pbi
    join public.pay_batch_candidates as pbc
      on pbc.id = pbi.pay_batch_candidate_id
    where pbc.pay_batch_id = p_pay_batch_id
      and coalesce(pbi.is_voided, false) = false
      and not exists (
        select 1
        from public.pay_payment_correction_items as ppc_pbi_fresh_exclusion
        where ppc_pbi_fresh_exclusion.pay_batch_item_id = pbi.id
          and ppc_pbi_fresh_exclusion.status = 'APPLIED'
          and ppc_pbi_fresh_exclusion.correction_item_kind in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
      )
  ), matched_snoozes as (
    select
      bi.timesheet_id,
      bi.candidate_id,
      bi.pay_batch_item_id,
      bi.item_type,
      bi.exact_source_ref,
      bi.source_identity_kind,
      bi.segment_stable_key,
      bi.segment_id,
      bi.booking_id,
      active_snooze.snooze_id,
      active_snooze.snooze_kind,
      active_snooze.snooze_until_date,
      active_snooze.note,
      active_snooze.source_ref,
      active_snooze.segment_stable_key as snooze_segment_stable_key,
      active_snooze.segment_id as snooze_segment_id,
      active_snooze.booking_id as snooze_booking_id,
      active_snooze.match_scope
    from batch_items as bi
    join lateral (
      select
        pis.id as snooze_id,
        upper(coalesce(pis.snooze_kind, '')) as snooze_kind,
        pis.snooze_until_date,
        pis.note,
        lower(nullif(btrim(coalesce(pis.source_ref, '')), '')) as source_ref,
        pis.segment_stable_key,
        pis.segment_id,
        pis.booking_id,
        case
          when pis.source_ref is not null then 'EXACT_SOURCE_REF'
          else 'LEGACY_TIMESHEET_SEGMENT'
        end as match_scope
      from public.pay_item_snoozes as pis
      where pis.candidate_id = bi.candidate_id
        and pis.cleared_at_utc is null
        and pis.cancelled_at_utc is null
        and (
          pis.snooze_until_date is null
          or pis.snooze_until_date >= v_today_uk
        )
        and (
          (
            bi.exact_source_ref is not null
            and lower(nullif(btrim(coalesce(pis.source_ref, '')), '')) = bi.exact_source_ref
          )
          or (
            bi.exact_source_ref is null
            and pis.source_ref is null
            and (
              (
                bi.timesheet_id is not null
                and pis.timesheet_id = bi.timesheet_id
              )
              or (
                nullif(btrim(coalesce(pis.booking_id, '')), '') is not null
                and nullif(btrim(coalesce(pis.booking_id, '')), '') = bi.booking_id
              )
            )
            and (
              (
                nullif(btrim(coalesce(pis.segment_stable_key, '')), '') is null
                and nullif(btrim(coalesce(pis.segment_id, '')), '') is null
              )
              or (
                bi.segment_stable_key is not null
                and nullif(btrim(coalesce(pis.segment_stable_key, '')), '') = bi.segment_stable_key
              )
              or (
                bi.segment_id is not null
                and nullif(btrim(coalesce(pis.segment_id, '')), '') = bi.segment_id
              )
              or (
                bi.segment_key is not null
                and bi.segment_key in (
                  nullif(btrim(coalesce(pis.segment_stable_key, '')), ''),
                  nullif(btrim(coalesce(pis.segment_id, '')), '')
                )
              )
            )
          )
        )
      order by pis.updated_at_utc desc, pis.created_at_utc desc, pis.id
      limit 1
    ) as active_snooze on true
  )
  select
    ms.timesheet_id,
    ms.candidate_id,
    'SNOOZE' as key_type,
    ms.pay_batch_item_id::text as key_value,
    'NO_ACTIVE_SNOOZE' as expected_text,
    jsonb_build_object(
      'snooze_id', ms.snooze_id::text,
      'snooze_kind', ms.snooze_kind,
      'snooze_until_date', case when ms.snooze_until_date is null then null else ms.snooze_until_date::text end,
      'note', ms.note,
      'source_ref', ms.source_ref,
      'match_scope', ms.match_scope,
      'frozen_item_source_ref', ms.exact_source_ref,
      'frozen_source_identity_kind', ms.source_identity_kind,
      'frozen_segment_stable_key', ms.segment_stable_key,
      'frozen_segment_id', ms.segment_id,
      'frozen_booking_id', ms.booking_id,
      'active_as_of_london_date', v_today_uk::text
    )::text as actual_text,
    34 as ord
  from matched_snoozes as ms;

  insert into pg_temp.tmp_fresh_state_diffs (
    timesheet_id,
    candidate_id,
    key_type,
    key_value,
    expected_text,
    actual_text,
    ord
  )
  with referenced_finance_cases as (
    select distinct
      pbc.candidate_id,
      coalesce(
        pbi.finance_case_id,
        case
          when pbi.source_ref ~ '^advance:[0-9a-fA-F-]{36}$' then replace(pbi.source_ref, 'advance:', '')::uuid
          else null
        end
      ) as finance_case_id
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc
      on pbc.id = pbi.pay_batch_candidate_id
    where pbc.pay_batch_id = p_pay_batch_id
      and coalesce(pbi.is_voided, false) = false
      and not exists (
        select 1
        from public.pay_payment_correction_items as ppc_pbi_fresh_exclusion
        where ppc_pbi_fresh_exclusion.pay_batch_item_id = pbi.id
          and ppc_pbi_fresh_exclusion.status = 'APPLIED'
          and ppc_pbi_fresh_exclusion.correction_item_kind in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
      )
      and (
        pbi.finance_case_id is not null
        or pbi.source_ref ~ '^advance:[0-9a-fA-F-]{36}$'
      )
  )
  select
    null::uuid as timesheet_id,
    rfc.candidate_id,
    'RESTRUCTURE' as key_type,
    rfc.finance_case_id::text as key_value,
    'NO_RESTRUCTURE_AFTER_BATCH_CREATED' as expected_text,
    jsonb_build_object(
      'finance_case_id', rfc.finance_case_id::text,
      'event_type', pfce.event_type,
      'event_at_utc', pfce.event_at_utc
    )::text as actual_text,
    35 as ord
  from referenced_finance_cases rfc
  join public.pay_finance_case_events pfce
    on pfce.finance_case_id = rfc.finance_case_id
  where pfce.event_at_utc >= coalesce(v_batch_created_at_utc, v_now_utc)
    and upper(coalesce(pfce.event_type, '')) like '%RESTRUCTURE%';

  insert into pg_temp.tmp_fresh_state_diffs (
    timesheet_id,
    candidate_id,
    key_type,
    key_value,
    expected_text,
    actual_text,
    ord
  )
  with referenced_finance_cases as (
    select distinct
      pbc.candidate_id,
      coalesce(
        pbi.finance_case_id,
        case
          when pbi.source_ref ~ '^advance:[0-9a-fA-F-]{36}$' then replace(pbi.source_ref, 'advance:', '')::uuid
          else null
        end
      ) as finance_case_id
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc
      on pbc.id = pbi.pay_batch_candidate_id
    where pbc.pay_batch_id = p_pay_batch_id
      and coalesce(pbi.is_voided, false) = false
      and not exists (
        select 1
        from public.pay_payment_correction_items as ppc_pbi_fresh_exclusion
        where ppc_pbi_fresh_exclusion.pay_batch_item_id = pbi.id
          and ppc_pbi_fresh_exclusion.status = 'APPLIED'
          and ppc_pbi_fresh_exclusion.correction_item_kind in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
      )
      and (
        pbi.finance_case_id is not null
        or pbi.source_ref ~ '^advance:[0-9a-fA-F-]{36}$'
      )
  )
  select
    null::uuid as timesheet_id,
    rfc.candidate_id,
    'WRITE_OFF' as key_type,
    rfc.finance_case_id::text as key_value,
    'NOT_WRITTEN_OFF' as expected_text,
    jsonb_build_object(
      'finance_case_id', pa.id::text,
      'written_off_at_utc', pa.written_off_at_utc,
      'status', pa.status::text
    )::text as actual_text,
    36 as ord
  from referenced_finance_cases rfc
  join public.pay_advances pa
    on pa.id = rfc.finance_case_id
  where pa.written_off_at_utc is not null
    and pa.written_off_at_utc >= coalesce(v_batch_created_at_utc, v_now_utc);

  insert into pg_temp.tmp_fresh_state_diffs (
    timesheet_id,
    candidate_id,
    key_type,
    key_value,
    expected_text,
    actual_text,
    ord
  )
  with batch_ts as (
    select distinct
      pbi.timesheet_id,
      pbc.candidate_id
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc
      on pbc.id = pbi.pay_batch_candidate_id
    where pbc.pay_batch_id = p_pay_batch_id
      and coalesce(pbi.is_voided, false) = false
      and not exists (
        select 1
        from public.pay_payment_correction_items as ppc_pbi_fresh_exclusion
        where ppc_pbi_fresh_exclusion.pay_batch_item_id = pbi.id
          and ppc_pbi_fresh_exclusion.status = 'APPLIED'
          and ppc_pbi_fresh_exclusion.correction_item_kind in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
      )
      and pbi.timesheet_id is not null
  ),
  batch_entitlement_ts as (
    select distinct
      pbi.timesheet_id
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc
      on pbc.id = pbi.pay_batch_candidate_id
    where pbc.pay_batch_id = p_pay_batch_id
      and coalesce(pbi.is_voided, false) = false
      and pbi.item_type in ('SEGMENT_DELTA','EXPENSE_DELTA','ADJUSTMENT_DELTA','MILEAGE_DELTA')
      and pbi.timesheet_id is not null
      and not exists (
        select 1
        from public.pay_payment_correction_items as ppc_pbi_entitlement_fresh_exclusion
        where ppc_pbi_entitlement_fresh_exclusion.pay_batch_item_id = pbi.id
          and ppc_pbi_entitlement_fresh_exclusion.status = 'APPLIED'
          and ppc_pbi_entitlement_fresh_exclusion.correction_item_kind in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
      )
  ),
  override_changes as (
    select
      bt.timesheet_id,
      bt.candidate_id,
      tpo.id as override_id,
      tpo.override_type,
      tpo.created_at_utc,
      tpo.consumed_at_utc,
      tpo.consumed_by_pay_batch_id,
      tpo.cleared_at_utc,
      consumed_batch.status as consumed_batch_status,
      coalesce(tpo.created_at_utc, '-infinity'::timestamptz) >= coalesce(v_batch_created_at_utc, v_now_utc) as created_after_batch,
      coalesce(tpo.cleared_at_utc, '-infinity'::timestamptz) >= coalesce(v_batch_created_at_utc, v_now_utc) as cleared_after_batch,
      coalesce(tpo.consumed_at_utc, '-infinity'::timestamptz) >= coalesce(v_batch_created_at_utc, v_now_utc) as consumed_after_batch,
      (
        upper(coalesce(tpo.override_type, '')) = 'ADVANCE_THIS_PAYMENT'
        and tpo.consumed_by_pay_batch_id = p_pay_batch_id
        and tpo.consumed_at_utc is not null
        and tpo.cleared_at_utc is null
        and coalesce(tpo.created_at_utc, '-infinity'::timestamptz) < coalesce(v_batch_created_at_utc, v_now_utc)
        and exists (
          select 1
          from batch_entitlement_ts bet
          where bet.timesheet_id = bt.timesheet_id
        )
      ) as expected_same_batch_advance_consumption
    from batch_ts bt
    join public.timesheet_payment_overrides tpo
      on tpo.timesheet_id = bt.timesheet_id
    left join public.pay_batches consumed_batch
      on consumed_batch.id = tpo.consumed_by_pay_batch_id
    where upper(coalesce(tpo.override_type, '')) = 'ADVANCE_THIS_PAYMENT'
  )
  select
    oc.timesheet_id,
    oc.candidate_id,
    'TIMESHEET_ADVANCE_OVERRIDE' as key_type,
    oc.timesheet_id::text as key_value,
    'NO_OVERRIDE_CHANGE_AFTER_BATCH_CREATED' as expected_text,
    jsonb_build_object(
      'override_id', oc.override_id::text,
      'override_type', oc.override_type,
      'created_at_utc', oc.created_at_utc,
      'consumed_at_utc', oc.consumed_at_utc,
      'consumed_by_pay_batch_id', case when oc.consumed_by_pay_batch_id is null then null else oc.consumed_by_pay_batch_id::text end,
      'consumed_batch_status', oc.consumed_batch_status,
      'cleared_at_utc', oc.cleared_at_utc
    )::text as actual_text,
    37 as ord
  from override_changes oc
  where oc.created_after_batch
     or oc.cleared_after_batch
     or (
          oc.consumed_after_batch
          and oc.expected_same_batch_advance_consumption is not true
          and (
            oc.consumed_by_pay_batch_id is null
            or upper(coalesce(oc.consumed_batch_status, '')) <> 'CANCELLED'
          )
        );

  select count(*)::int
  into v_finance_reservation_diff_ct
  from pg_temp.tmp_fresh_state_diffs tsd
  where tsd.key_type = 'FINANCE_RESERVATION';

  if v_finance_reservation_diff_ct > 0 then
    v_is_stale := true;
    v_reasons := array_append(v_reasons, 'FINANCE_RESERVATION_CHANGED');
  end if;

  select count(*)::int
  into v_snooze_diff_ct
  from pg_temp.tmp_fresh_state_diffs tsd
  where tsd.key_type = 'SNOOZE';

  if v_snooze_diff_ct > 0 then
    v_is_stale := true;
    v_reasons := array_append(v_reasons, 'SNOOZE_CHANGED');
  end if;

  select count(*)::int
  into v_restructure_diff_ct
  from pg_temp.tmp_fresh_state_diffs tsd
  where tsd.key_type = 'RESTRUCTURE';

  if v_restructure_diff_ct > 0 then
    v_is_stale := true;
    v_reasons := array_append(v_reasons, 'RESTRUCTURE_CHANGED');
  end if;

  select count(*)::int
  into v_writeoff_diff_ct
  from pg_temp.tmp_fresh_state_diffs tsd
  where tsd.key_type = 'WRITE_OFF';

  if v_writeoff_diff_ct > 0 then
    v_is_stale := true;
    v_reasons := array_append(v_reasons, 'WRITE_OFF_CHANGED');
  end if;

  select count(*)::int
  into v_timesheet_override_diff_ct
  from pg_temp.tmp_fresh_state_diffs tsd
  where tsd.key_type = 'TIMESHEET_ADVANCE_OVERRIDE';

  if v_timesheet_override_diff_ct > 0 then
    v_is_stale := true;
    v_reasons := array_append(v_reasons, 'TIMESHEET_PAYMENT_OVERRIDE_CHANGED');
  end if;

  if v_scope = 'PAYE' and coalesce(v_batch_kind_fixed, '') <> 'LOANS' then
    v_paye_guardrails := public.pay_paye_guardrails(
      p_pay_date => v_pay_date,
      p_ignore_pay_batch_id => p_pay_batch_id,
      p_actor_user_id => p_actor_user_id
    );

    if coalesce((v_paye_guardrails->>'create_paye_blocked')::boolean, false) = true
       or (
            coalesce((v_paye_guardrails->>'override_required')::boolean, false) = true
            and coalesce(v_same_week_paye_override_used, false) = false
          )
    then
      insert into pg_temp.tmp_fresh_state_diffs (
        timesheet_id,
        candidate_id,
        key_type,
        key_value,
        expected_text,
        actual_text,
        ord
      )
      values (
        null,
        null,
        'PAYE_GUARDRAILS',
        p_pay_batch_id::text,
        jsonb_build_object(
          'same_week_paye_override_used', coalesce(v_same_week_paye_override_used, false)
        )::text,
        v_paye_guardrails::text,
        38
      );

      select count(*)::int
      into v_paye_guardrail_diff_ct
      from pg_temp.tmp_fresh_state_diffs tsd
      where tsd.key_type = 'PAYE_GUARDRAILS';

      if v_paye_guardrail_diff_ct > 0 then
        v_is_stale := true;
        v_reasons := array_append(v_reasons, 'PAYE_GUARDRAILS_CHANGED');
      end if;
    end if;
  end if;


  ---------------------------------------------------------------------------
  -- DEDUCTION_CHANGED: recompute expected OVERPAYMENT_RECOVERY,
  -- MANUAL_DEBT_RECOVERY, and LOAN_REPAYMENT from frozen batch-local
  -- deduction templates only. No live finance register / pay_advances
  -- authority is permitted in this post-draft branch.
  ---------------------------------------------------------------------------
  if v_scope in ('PAYE','UMBRELLA') then
    create temporary table if not exists pg_temp.tmp_fresh_recovery_templates (
      item_type text not null,
      pay_batch_candidate_id uuid not null,
      candidate_id uuid not null,
      finance_case_id uuid null,
      source_ref text null,
      pay_channel text null,
      umbrella_id uuid null,
      is_mismatch boolean null,
      paye_treatment text null,
      finance_component_id uuid null,
      frozen_component_snapshot_json jsonb null,
      frozen_component_key_type text null,
      frozen_component_key_value text null,
      frozen_component_classification public.pay_finance_component_classification_enum null,
      frozen_source_basis_json jsonb null,
      frozen_source_pay_method text null,
      frozen_target_pay_method text null,
      frozen_resolution_mode public.pay_finance_component_resolution_mode_enum null,
      frozen_resolution_payload_json jsonb null,
      frozen_resolution_result_json jsonb null,
      frozen_source_amount numeric(12,2) null,
      frozen_target_amount_ex_vat numeric(12,2) null,
      frozen_target_amount_vat numeric(12,2) null,
      frozen_target_amount_inc_vat numeric(12,2) null,
      payout_instruction_snapshot_json jsonb null,
      template_sort_at timestamptz null,
      sort_order integer not null,
      frozen_case_type text null,
      frozen_payout_status text null,
      frozen_remaining_source_amount numeric(12,2) not null,
      frozen_weekly_due_amount numeric(12,2) not null,
      frozen_next_due_week_start date null,
      minimum_earnings_threshold numeric(12,2) null,
      take_home_floor_override numeric(12,2) null,
      default_take_home_floor numeric(12,2) null
    ) on commit drop;

    truncate table pg_temp.tmp_fresh_recovery_templates;

    insert into pg_temp.tmp_fresh_recovery_templates (
      item_type,
      pay_batch_candidate_id,
      candidate_id,
      finance_case_id,
      source_ref,
      pay_channel,
      umbrella_id,
      is_mismatch,
      paye_treatment,
      finance_component_id,
      frozen_component_snapshot_json,
      frozen_component_key_type,
      frozen_component_key_value,
      frozen_component_classification,
      frozen_source_basis_json,
      frozen_source_pay_method,
      frozen_target_pay_method,
      frozen_resolution_mode,
      frozen_resolution_payload_json,
      frozen_resolution_result_json,
      frozen_source_amount,
      frozen_target_amount_ex_vat,
      frozen_target_amount_vat,
      frozen_target_amount_inc_vat,
      payout_instruction_snapshot_json,
      template_sort_at,
      sort_order,
      frozen_case_type,
      frozen_payout_status,
      frozen_remaining_source_amount,
      frozen_weekly_due_amount,
      frozen_next_due_week_start,
      minimum_earnings_threshold,
      take_home_floor_override,
      default_take_home_floor
    )
    select distinct on (
      pbi_rt.item_type,
      pbi_rt.pay_batch_candidate_id,
      coalesce(
        coalesce(
          pbi_rt.finance_case_id,
          case
            when pbi_rt.source_ref ~ '^advance:[0-9a-fA-F-]{36}$' then replace(pbi_rt.source_ref, 'advance:', '')::uuid
            else null::uuid
          end
        )::text,
        ''
      ),
      coalesce(pbi_rt.finance_component_id::text, ''),
      coalesce(pbi_rt.source_ref, '')
    )
      pbi_rt.item_type,
      pbi_rt.pay_batch_candidate_id,
      pbc_rt.candidate_id,
      coalesce(
        pbi_rt.finance_case_id,
        case
          when pbi_rt.source_ref ~ '^advance:[0-9a-fA-F-]{36}$' then replace(pbi_rt.source_ref, 'advance:', '')::uuid
          else null::uuid
        end
      ) as finance_case_id,
      pbi_rt.source_ref,
      pbi_rt.pay_channel,
      pbi_rt.umbrella_id,
      pbi_rt.is_mismatch,
      pbi_rt.paye_treatment,
      pbi_rt.finance_component_id,
      pbi_rt.frozen_component_snapshot_json,
      pbi_rt.frozen_component_key_type,
      pbi_rt.frozen_component_key_value,
      pbi_rt.frozen_component_classification,
      pbi_rt.frozen_source_basis_json,
      pbi_rt.frozen_source_pay_method,
      pbi_rt.frozen_target_pay_method,
      pbi_rt.frozen_resolution_mode,
      pbi_rt.frozen_resolution_payload_json,
      pbi_rt.frozen_resolution_result_json,
      round(coalesce(pbi_rt.frozen_source_amount, 0), 2)::numeric(12,2) as frozen_source_amount,
      round(coalesce(pbi_rt.frozen_target_amount_ex_vat, 0), 2)::numeric(12,2) as frozen_target_amount_ex_vat,
      round(coalesce(pbi_rt.frozen_target_amount_vat, 0), 2)::numeric(12,2) as frozen_target_amount_vat,
      round(coalesce(pbi_rt.frozen_target_amount_inc_vat, 0), 2)::numeric(12,2) as frozen_target_amount_inc_vat,
      pbi_rt.payout_instruction_snapshot_json,
      coalesce(pbi_rt.created_at, pbi_rt.updated_at, now()) as template_sort_at,
      case
        when nullif(btrim(coalesce(pbi_rt.frozen_source_basis_json->>'allocation_priority_order', pbi_rt.frozen_source_basis_json->>'sort_order', pbi_rt.frozen_resolution_payload_json->>'sort_order')), '') ~ '^-?[0-9]+$'
          then (coalesce(pbi_rt.frozen_source_basis_json->>'allocation_priority_order', pbi_rt.frozen_source_basis_json->>'sort_order', pbi_rt.frozen_resolution_payload_json->>'sort_order'))::integer
        else 2147483647
      end as sort_order,
      upper(coalesce(
        nullif(btrim(pbi_rt.frozen_source_basis_json->>'case_type'), ''),
        case pbi_rt.item_type
          when 'OVERPAYMENT_RECOVERY' then 'OVERPAYMENT'
          when 'LOAN_REPAYMENT' then 'PAYMENT_ADVANCE'
          when 'MANUAL_DEBT_RECOVERY' then 'MANUAL_DEBT_ADJUSTMENT'
          else null
        end
      )) as frozen_case_type,
      upper(nullif(btrim(pbi_rt.frozen_source_basis_json->>'payout_status'), '')) as frozen_payout_status,
      round(greatest(
        coalesce(
          case
            when pbi_rt.frozen_source_amount is not null then abs(pbi_rt.frozen_source_amount)
            when nullif(btrim(pbi_rt.frozen_component_snapshot_json->>'remaining_source_amount'), '') is not null then (pbi_rt.frozen_component_snapshot_json->>'remaining_source_amount')::numeric
            when nullif(btrim(pbi_rt.frozen_source_basis_json->>'remaining_source_amount'), '') is not null then (pbi_rt.frozen_source_basis_json->>'remaining_source_amount')::numeric
            when nullif(btrim(pbi_rt.frozen_source_basis_json->>'outstanding_amount'), '') is not null then (pbi_rt.frozen_source_basis_json->>'outstanding_amount')::numeric
            when nullif(btrim(pbi_rt.frozen_source_basis_json->>'amount'), '') is not null then abs((pbi_rt.frozen_source_basis_json->>'amount')::numeric)
            else 0::numeric
          end,
          0::numeric
        ),
        0::numeric
      ), 2)::numeric(12,2) as frozen_remaining_source_amount,
      case
        when nullif(btrim(pbi_rt.frozen_resolution_result_json->>'case_source_weekly_due'), '') is not null
          then round(greatest(abs((pbi_rt.frozen_resolution_result_json->>'case_source_weekly_due')::numeric), 0), 2)::numeric(12,2)
        when nullif(btrim(pbi_rt.frozen_source_basis_json->>'weekly_due'), '') is not null
          then round(greatest(abs((pbi_rt.frozen_source_basis_json->>'weekly_due')::numeric), 0), 2)::numeric(12,2)
        else 0::numeric(12,2)
      end as frozen_weekly_due_amount,
      case
        when nullif(btrim(pbi_rt.frozen_source_basis_json->>'next_due_week_start'), '') is null then null::date
        else (pbi_rt.frozen_source_basis_json->>'next_due_week_start')::date
      end as frozen_next_due_week_start,
      case
        when nullif(btrim(pbi_rt.frozen_source_basis_json->>'minimum_earnings_threshold'), '') is null then null::numeric(12,2)
        else round(greatest((pbi_rt.frozen_source_basis_json->>'minimum_earnings_threshold')::numeric, 0), 2)::numeric(12,2)
      end as minimum_earnings_threshold,
      case
        when nullif(btrim(pbi_rt.frozen_source_basis_json->>'take_home_floor_override'), '') is null then null::numeric(12,2)
        else round(greatest((pbi_rt.frozen_source_basis_json->>'take_home_floor_override')::numeric, 0), 2)::numeric(12,2)
      end as take_home_floor_override,
      case
        when nullif(btrim(pbi_rt.frozen_source_basis_json->>'default_take_home_floor'), '') is null then null::numeric(12,2)
        else round(greatest((pbi_rt.frozen_source_basis_json->>'default_take_home_floor')::numeric, 0), 2)::numeric(12,2)
      end as default_take_home_floor
    from public.pay_batch_items pbi_rt
    join public.pay_batch_candidates pbc_rt
      on pbc_rt.id = pbi_rt.pay_batch_candidate_id
    where pbc_rt.pay_batch_id = p_pay_batch_id
      and coalesce(pbi_rt.is_voided, false) = false
      and not exists (
        select 1
        from public.pay_payment_correction_items as ppc_pbi_rt_fresh_exclusion
        where ppc_pbi_rt_fresh_exclusion.pay_batch_item_id = pbi_rt.id
          and ppc_pbi_rt_fresh_exclusion.status = 'APPLIED'
          and ppc_pbi_rt_fresh_exclusion.correction_item_kind in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
      )
      and pbi_rt.item_type in ('OVERPAYMENT_RECOVERY','LOAN_REPAYMENT','MANUAL_DEBT_RECOVERY')
    order by
      pbi_rt.item_type,
      pbi_rt.pay_batch_candidate_id,
      coalesce(
        coalesce(
          pbi_rt.finance_case_id,
          case
            when pbi_rt.source_ref ~ '^advance:[0-9a-fA-F-]{36}$' then replace(pbi_rt.source_ref, 'advance:', '')::uuid
            else null::uuid
          end
        )::text,
        ''
      ),
      coalesce(pbi_rt.finance_component_id::text, ''),
      coalesce(pbi_rt.source_ref, ''),
      pbi_rt.is_voided desc,
      coalesce(pbi_rt.updated_at, pbi_rt.created_at) desc,
      pbi_rt.id desc;

    insert into pg_temp.tmp_fresh_state_diffs (
      timesheet_id,
      candidate_id,
      key_type,
      key_value,
      expected_text,
      actual_text,
      ord
    )
    select
      null::uuid as timesheet_id,
      pbc_bad.candidate_id,
      'DEDUCTION_TEMPLATE' as key_type,
      pbi_bad.id::text as key_value,
      'FROZEN_TEMPLATE_WITH_FINANCE_CASE_ID' as expected_text,
      jsonb_build_object(
        'pay_batch_item_id', pbi_bad.id::text,
        'item_type', pbi_bad.item_type,
        'source_ref', pbi_bad.source_ref,
        'finance_case_id', pbi_bad.finance_case_id::text
      )::text as actual_text,
      30 as ord
    from public.pay_batch_items pbi_bad
    join public.pay_batch_candidates pbc_bad
      on pbc_bad.id = pbi_bad.pay_batch_candidate_id
    where pbc_bad.pay_batch_id = p_pay_batch_id
      and coalesce(pbi_bad.is_voided, false) = false
      and not exists (
        select 1
        from public.pay_payment_correction_items as ppc_pbi_bad_fresh_exclusion
        where ppc_pbi_bad_fresh_exclusion.pay_batch_item_id = pbi_bad.id
          and ppc_pbi_bad_fresh_exclusion.status = 'APPLIED'
          and ppc_pbi_bad_fresh_exclusion.correction_item_kind in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
      )
      and pbi_bad.item_type in ('OVERPAYMENT_RECOVERY','LOAN_REPAYMENT','MANUAL_DEBT_RECOVERY')
      and pbi_bad.finance_case_id is null;

    insert into pg_temp.tmp_fresh_expected_overpay_alloc (
      candidate_id,
      advance_id,
      take_ex,
      ord
    )
    with candidate_basis as (
      select
        pbc.id as pay_batch_candidate_id,
        pbc.candidate_id,
        coalesce(pbc.awaiting_net_amount, false) as awaiting_net_amount,
        greatest(
          coalesce(
            case
              when v_scope = 'PAYE' then pni.net_amount
              else (
                select sum(pbi_pos.amount_ex_vat)
                from public.pay_batch_items pbi_pos
                where pbi_pos.pay_batch_candidate_id = pbc.id
                  and coalesce(pbi_pos.is_voided, false) = false
      and not exists (
        select 1
        from public.pay_payment_correction_items as ppc_pbi_pos_fresh_exclusion
        where ppc_pbi_pos_fresh_exclusion.pay_batch_item_id = pbi_pos.id
          and ppc_pbi_pos_fresh_exclusion.status = 'APPLIED'
          and ppc_pbi_pos_fresh_exclusion.correction_item_kind in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
      )
                  and pbi_pos.amount_ex_vat > 0
                  and pbi_pos.item_type in ('SEGMENT_DELTA','EXPENSE_DELTA','ADJUSTMENT_DELTA','MILEAGE_DELTA')
              )
            end,
            0
          ),
          0
        )::numeric(12,2) as earnings_pool_ex
      from public.pay_batch_candidates pbc
      left join lateral (
        select pni_inner.net_amount
        from public.pay_batch_paye_net_inputs pni_inner
        where pni_inner.pay_batch_candidate_id = pbc.id
        order by pni_inner.imported_at_utc desc
        limit 1
      ) pni on true
      where pbc.pay_batch_id = p_pay_batch_id
    ),
    case_templates as (
      select distinct on (rt.pay_batch_candidate_id, rt.finance_case_id, coalesce(rt.finance_component_id::text, ''), coalesce(rt.source_ref, ''))
        rt.*
      from pg_temp.tmp_fresh_recovery_templates rt
      where rt.item_type = 'OVERPAYMENT_RECOVERY'
        and rt.finance_case_id is not null
        /*
         * PAYE gross-side deductions have already been incorporated into the
         * payroll result before the bank-net figure is entered. Only net-side
         * deductions may be reallocated from that later net authority.
         */
        and (
          v_scope <> 'PAYE'
          or upper(coalesce(rt.paye_treatment, 'NET_DEDUCT')) not in ('GROSS_DEDUCT', 'GROSS_DEDUCTION', 'DEDUCT_GROSS')
        )
      order by
        rt.pay_batch_candidate_id,
        rt.finance_case_id,
        coalesce(rt.finance_component_id::text, ''),
        coalesce(rt.source_ref, ''),
        rt.sort_order,
        rt.template_sort_at,
        rt.finance_component_id nulls last,
        rt.source_ref nulls last
    ),
    alloc_base as (
      select
        cb.candidate_id,
        ct.finance_case_id,
        ct.frozen_remaining_source_amount,
        ct.sort_order,
        ct.template_sort_at,
        cb.earnings_pool_ex,
        sum(ct.frozen_remaining_source_amount) over (
          partition by cb.candidate_id
          order by ct.sort_order, ct.template_sort_at, ct.finance_case_id, ct.finance_component_id, coalesce(ct.source_ref, '')
          rows between unbounded preceding and 1 preceding
        )::numeric(12,2) as cum_before_ex
      from candidate_basis cb
      join case_templates ct
        on ct.pay_batch_candidate_id = cb.pay_batch_candidate_id
      where (v_scope <> 'PAYE' or cb.awaiting_net_amount = false)
        and ct.frozen_remaining_source_amount > 0
        and upper(coalesce(ct.frozen_case_type, 'OVERPAYMENT')) = 'OVERPAYMENT'
    )
    select
      ab.candidate_id,
      ab.finance_case_id as advance_id,
      round(
        least(
          ab.frozen_remaining_source_amount,
          greatest(ab.earnings_pool_ex - coalesce(ab.cum_before_ex, 0), 0)
        ),
        2
      )::numeric(12,2) as take_ex,
      31 as ord
    from alloc_base ab
    where round(
      least(
        ab.frozen_remaining_source_amount,
        greatest(ab.earnings_pool_ex - coalesce(ab.cum_before_ex, 0), 0)
      ),
      2
    ) > 0;

    insert into pg_temp.tmp_fresh_candidate_expected_overpay (
      candidate_id,
      expected_overpayment_recovery_ex
    )
    select
      teo.candidate_id,
      round(sum(teo.take_ex), 2)::numeric(12,2) as expected_overpayment_recovery_ex
    from pg_temp.tmp_fresh_expected_overpay_alloc teo
    group by teo.candidate_id;

    insert into pg_temp.tmp_fresh_deduction_diffs (
      key_type,
      key_value,
      expected_ex,
      actual_ex,
      ord
    )
    with batch_expected_overpay as (
      select
        coalesce(
          pbi.finance_case_id,
          case
            when pbi.source_ref ~ '^advance:[0-9a-fA-F-]{36}$' then replace(pbi.source_ref, 'advance:', '')::uuid
            else null
          end
        ) as finance_case_id,
        round(sum(coalesce(abs(pbi.frozen_source_amount), -pbi.amount_ex_vat, -pbi.amount_inc_vat, 0)), 2)::numeric(12,2) as expected_ex
      from public.pay_batch_items pbi
      join public.pay_batch_candidates pbc
        on pbc.id = pbi.pay_batch_candidate_id
      where pbc.pay_batch_id = p_pay_batch_id
        /*
         * A PAYE candidate awaiting their first net entry has no net-owned
         * deduction pool yet. Comparing the frozen draft deduction with an
         * empty pre-entry allocation would make the batch stale solely
         * because PAYE net has not been entered. pay_set_paye_net_manual
         * validates freshness before it saves that first net value, then
         * rebuilds the deductions from the entered net in the same
         * transaction. Exclude only those awaiting-net candidates here;
         * all other freshness domains remain active and the deduction is
         * compared normally as soon as the net input exists.
         */
        and (
          v_scope <> 'PAYE'
          or coalesce(pbc.awaiting_net_amount, false) = false
        )
        and coalesce(pbi.is_voided, false) = false
      and not exists (
        select 1
        from public.pay_payment_correction_items as ppc_pbi_fresh_exclusion
        where ppc_pbi_fresh_exclusion.pay_batch_item_id = pbi.id
          and ppc_pbi_fresh_exclusion.status = 'APPLIED'
          and ppc_pbi_fresh_exclusion.correction_item_kind in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
      )
        and pbi.item_type = 'OVERPAYMENT_RECOVERY'
        and (
          v_scope <> 'PAYE'
          or upper(coalesce(pbi.paye_treatment, 'NET_DEDUCT')) not in ('GROSS_DEDUCT', 'GROSS_DEDUCTION', 'DEDUCT_GROSS')
        )
        and pbi.repayment_week_start = v_week_start
      group by 1
    ),
    actual_overpay as (
      select
        teo.advance_id as finance_case_id,
        round(sum(teo.take_ex), 2)::numeric(12,2) as actual_ex
      from pg_temp.tmp_fresh_expected_overpay_alloc teo
      group by teo.advance_id
    ),
    union_keys as (
      select beo.finance_case_id from batch_expected_overpay beo
      union
      select ao.finance_case_id from actual_overpay ao
    )
    select
      'OVERPAYMENT_RECOVERY' as key_type,
      uk.finance_case_id::text as key_value,
      round(coalesce(beo.expected_ex, 0), 2)::numeric(12,2) as expected_ex,
      round(coalesce(ao.actual_ex, 0), 2)::numeric(12,2) as actual_ex,
      31 as ord
    from union_keys uk
    left join batch_expected_overpay beo
      on beo.finance_case_id = uk.finance_case_id
    left join actual_overpay ao
      on ao.finance_case_id = uk.finance_case_id
    where uk.finance_case_id is not null
      and round(coalesce(beo.expected_ex, 0), 2) <> round(coalesce(ao.actual_ex, 0), 2);

    insert into pg_temp.tmp_fresh_expected_loan_alloc (
      candidate_id,
      loan_id,
      take_ex,
      ord
    )
    with batch_expected_overpay_by_candidate as (
      select
        teo.candidate_id,
        round(sum(teo.take_ex), 2)::numeric(12,2) as expected_overpay_ex
      from pg_temp.tmp_fresh_expected_overpay_alloc teo
      group by teo.candidate_id
    ),
    candidate_basis as (
      select
        pbc.id as pay_batch_candidate_id,
        pbc.candidate_id,
        coalesce(pbc.awaiting_net_amount, false) as awaiting_net_amount,
        greatest(coalesce(pni.net_amount, 0), 0)::numeric(12,2) as paye_net_amount
      from public.pay_batch_candidates pbc
      left join lateral (
        select pni_inner.net_amount
        from public.pay_batch_paye_net_inputs pni_inner
        where pni_inner.pay_batch_candidate_id = pbc.id
        order by pni_inner.imported_at_utc desc
        limit 1
      ) pni on true
      where pbc.pay_batch_id = p_pay_batch_id
    ),
    case_templates as (
      select distinct on (rt.pay_batch_candidate_id, rt.finance_case_id, coalesce(rt.finance_component_id::text, ''), coalesce(rt.source_ref, ''))
        rt.*
      from pg_temp.tmp_fresh_recovery_templates rt
      where rt.item_type = 'LOAN_REPAYMENT'
        and rt.finance_case_id is not null
        and (
          v_scope <> 'PAYE'
          or upper(coalesce(rt.paye_treatment, 'NET_DEDUCT')) not in ('GROSS_DEDUCT', 'GROSS_DEDUCTION', 'DEDUCT_GROSS')
        )
      order by
        rt.pay_batch_candidate_id,
        rt.finance_case_id,
        coalesce(rt.finance_component_id::text, ''),
        coalesce(rt.source_ref, ''),
        rt.sort_order,
        rt.template_sort_at,
        rt.finance_component_id nulls last,
        rt.source_ref nulls last
    ),
    loan_seed as (
      select
        cb.pay_batch_candidate_id,
        cb.candidate_id,
        ct.finance_case_id,
        ct.template_sort_at,
        ct.sort_order,
        round(greatest(least(coalesce(ct.frozen_weekly_due_amount, ct.frozen_remaining_source_amount, 0::numeric(12,2)), ct.frozen_remaining_source_amount), 0), 2)::numeric(12,2) as nominal_due_amount,
        ct.minimum_earnings_threshold,
        ct.take_home_floor_override,
        round(greatest(cb.paye_net_amount - coalesce(beo.expected_overpay_ex, 0), 0), 2)::numeric(12,2) as run_earnings_headroom_ex,
        round(greatest(cb.paye_net_amount - coalesce(beo.expected_overpay_ex, 0), 0), 2)::numeric(12,2) as run_take_home_before_ex
      from candidate_basis cb
      join case_templates ct
        on ct.pay_batch_candidate_id = cb.pay_batch_candidate_id
      left join batch_expected_overpay_by_candidate beo
        on beo.candidate_id = cb.candidate_id
      where cb.awaiting_net_amount = false
        and ct.frozen_remaining_source_amount > 0
        and upper(coalesce(ct.frozen_case_type, 'PAYMENT_ADVANCE')) = 'PAYMENT_ADVANCE'
        and upper(coalesce(ct.frozen_payout_status, 'PAID')) = 'PAID'
        and (ct.frozen_next_due_week_start is null or ct.frozen_next_due_week_start <= v_week_start)
        and round(greatest(least(coalesce(ct.frozen_weekly_due_amount, ct.frozen_remaining_source_amount, 0::numeric(12,2)), ct.frozen_remaining_source_amount), 0), 2) > 0
    ),
    loan_inputs as (
      select
        ls.candidate_id,
        coalesce(
          jsonb_agg(
            jsonb_build_object(
              'sort_order', ls.sort_order,
              'finance_case_id', ls.finance_case_id::text,
              'case_type', 'PAYMENT_ADVANCE',
              'payout_status', 'PAID',
              'nominal_due_amount', ls.nominal_due_amount,
              'minimum_earnings_threshold', ls.minimum_earnings_threshold,
              'take_home_floor_override', ls.take_home_floor_override
            )
            order by ls.sort_order, ls.template_sort_at, ls.finance_case_id
          ),
          '[]'::jsonb
        ) as recovery_rows_json,
        max(ls.run_earnings_headroom_ex)::numeric(12,2) as run_earnings_headroom_ex,
        max(ls.run_take_home_before_ex)::numeric(12,2) as run_take_home_before_ex
      from loan_seed ls
      group by ls.candidate_id
    ),
    loan_alloc as (
      select
        li.candidate_id,
        lra.finance_case_id,
        round(coalesce(lra.protected_recoverable_amount, 0), 2)::numeric(12,2) as take_ex
      from loan_inputs li
      cross join lateral public._pay_finance_protected_recovery_allocate(
        li.recovery_rows_json,
        li.run_earnings_headroom_ex,
        li.run_take_home_before_ex,
        null::numeric
      ) lra
    )
    select
      la.candidate_id,
      la.finance_case_id as loan_id,
      la.take_ex,
      33 as ord
    from loan_alloc la
    where round(coalesce(la.take_ex, 0), 2) > 0;

    insert into pg_temp.tmp_fresh_expected_manual_debt_alloc (
      candidate_id,
      finance_case_id,
      take_ex,
      ord
    )
    with batch_expected_overpay_by_candidate as (
      select
        teo.candidate_id,
        round(sum(teo.take_ex), 2)::numeric(12,2) as expected_overpay_ex
      from pg_temp.tmp_fresh_expected_overpay_alloc teo
      group by teo.candidate_id
    ),
    batch_expected_loan_by_candidate as (
      select
        tel.candidate_id,
        round(sum(tel.take_ex), 2)::numeric(12,2) as expected_loan_ex
      from pg_temp.tmp_fresh_expected_loan_alloc tel
      group by tel.candidate_id
    ),
    candidate_basis as (
      select
        pbc.id as pay_batch_candidate_id,
        pbc.candidate_id,
        coalesce(pbc.awaiting_net_amount, false) as awaiting_net_amount,
        round(greatest(coalesce(pbc.gross_preview, 0), 0), 2)::numeric(12,2) as gross_preview_ex,
        greatest(coalesce(pni.net_amount, 0), 0)::numeric(12,2) as paye_net_amount,
        case
          when v_scope = 'PAYE' then round(greatest(coalesce(pni.net_amount, 0), 0), 2)::numeric(12,2)
          else null::numeric(12,2)
        end as run_take_home_base_ex
      from public.pay_batch_candidates pbc
      left join lateral (
        select pni_inner.net_amount
        from public.pay_batch_paye_net_inputs pni_inner
        where pni_inner.pay_batch_candidate_id = pbc.id
        order by pni_inner.imported_at_utc desc
        limit 1
      ) pni on true
      where pbc.pay_batch_id = p_pay_batch_id
    ),
    case_templates as (
      select distinct on (rt.pay_batch_candidate_id, rt.finance_case_id, coalesce(rt.finance_component_id::text, ''), coalesce(rt.source_ref, ''))
        rt.*
      from pg_temp.tmp_fresh_recovery_templates rt
      where rt.item_type = 'MANUAL_DEBT_RECOVERY'
        and rt.finance_case_id is not null
        and (
          v_scope <> 'PAYE'
          or upper(coalesce(rt.paye_treatment, 'NET_DEDUCT')) not in ('GROSS_DEDUCT', 'GROSS_DEDUCTION', 'DEDUCT_GROSS')
        )
      order by
        rt.pay_batch_candidate_id,
        rt.finance_case_id,
        coalesce(rt.finance_component_id::text, ''),
        coalesce(rt.source_ref, ''),
        rt.sort_order,
        rt.template_sort_at,
        rt.finance_component_id nulls last,
        rt.source_ref nulls last
    ),
    manual_template_values as (
      select
        cb.pay_batch_candidate_id,
        cb.candidate_id,
        ct.finance_case_id,
        ct.template_sort_at,
        ct.sort_order,
        ct.frozen_case_type,
        ct.frozen_next_due_week_start,
        ct.minimum_earnings_threshold,
        ct.take_home_floor_override,
        ct.frozen_weekly_due_amount,
        ct.frozen_remaining_source_amount,
        nullif(ct.default_take_home_floor, 0)::numeric(12,2) as default_take_home_floor,
        round(greatest(cb.gross_preview_ex, 0), 2)::numeric(12,2) as run_earnings_headroom_ex,
        case
          when v_scope = 'PAYE' then round(greatest(coalesce(cb.run_take_home_base_ex, 0) - coalesce(beo.expected_overpay_ex, 0) - coalesce(bl.expected_loan_ex, 0), 0), 2)::numeric(12,2)
          else null::numeric(12,2)
        end as run_take_home_before_ex,
        sched_md.scheduled_due_amount
      from candidate_basis cb
      join case_templates ct
        on ct.pay_batch_candidate_id = cb.pay_batch_candidate_id
      left join batch_expected_overpay_by_candidate beo
        on beo.candidate_id = cb.candidate_id
      left join batch_expected_loan_by_candidate bl
        on bl.candidate_id = cb.candidate_id
      left join lateral (
        select
          round(greatest(abs((sched_item.value->>'amount')::numeric), 0), 2)::numeric(12,2) as scheduled_due_amount
        from jsonb_array_elements(
          case
            when jsonb_typeof(coalesce(ct.frozen_source_basis_json->'schedule_json', '[]'::jsonb)) = 'array'
              then coalesce(ct.frozen_source_basis_json->'schedule_json', '[]'::jsonb)
            else '[]'::jsonb
          end
        ) as sched_item(value)
        where nullif(btrim(sched_item.value->>'week_start'), '') is not null
          and (sched_item.value->>'week_start')::date = v_week_start
        limit 1
      ) sched_md on true
      where (v_scope <> 'PAYE' or cb.awaiting_net_amount = false)
    ),
    manual_seed as (
      select
        mtv.candidate_id,
        mtv.finance_case_id,
        mtv.template_sort_at,
        mtv.sort_order,
        round(greatest(least(coalesce(mtv.scheduled_due_amount, mtv.frozen_weekly_due_amount, 0::numeric(12,2)), mtv.frozen_remaining_source_amount), 0), 2)::numeric(12,2) as nominal_due_amount,
        mtv.minimum_earnings_threshold,
        mtv.take_home_floor_override,
        mtv.run_earnings_headroom_ex,
        mtv.run_take_home_before_ex,
        mtv.default_take_home_floor
      from manual_template_values mtv
      where upper(coalesce(mtv.frozen_case_type, 'MANUAL_DEBT_ADJUSTMENT')) = 'MANUAL_DEBT_ADJUSTMENT'
        and mtv.frozen_remaining_source_amount > 0
        and (mtv.frozen_next_due_week_start is null or mtv.frozen_next_due_week_start <= v_week_start)
        and round(greatest(least(coalesce(mtv.scheduled_due_amount, mtv.frozen_weekly_due_amount, 0::numeric(12,2)), mtv.frozen_remaining_source_amount), 0), 2) > 0
    ),
    manual_inputs as (
      select
        ms.candidate_id,
        coalesce(
          jsonb_agg(
            jsonb_build_object(
              'sort_order', ms.sort_order,
              'finance_case_id', ms.finance_case_id::text,
              'case_type', 'MANUAL_DEBT_ADJUSTMENT',
              'payout_status', null,
              'nominal_due_amount', ms.nominal_due_amount,
              'minimum_earnings_threshold', ms.minimum_earnings_threshold,
              'take_home_floor_override', ms.take_home_floor_override
            )
            order by ms.sort_order, ms.template_sort_at, ms.finance_case_id
          ),
          '[]'::jsonb
        ) as recovery_rows_json,
        max(ms.run_earnings_headroom_ex)::numeric(12,2) as run_earnings_headroom_ex,
        max(ms.run_take_home_before_ex)::numeric(12,2) as run_take_home_before_ex,
        nullif(max(ms.default_take_home_floor), 0)::numeric(12,2) as default_take_home_floor
      from manual_seed ms
      group by ms.candidate_id
    ),
    manual_alloc as (
      select
        mi.candidate_id,
        mdra.finance_case_id,
        round(coalesce(mdra.protected_recoverable_amount, 0), 2)::numeric(12,2) as take_ex
      from manual_inputs mi
      cross join lateral public._pay_finance_protected_recovery_allocate(
        mi.recovery_rows_json,
        mi.run_earnings_headroom_ex,
        mi.run_take_home_before_ex,
        mi.default_take_home_floor
      ) mdra
    )
    select
      ma.candidate_id,
      ma.finance_case_id,
      ma.take_ex,
      32 as ord
    from manual_alloc ma
    where round(coalesce(ma.take_ex, 0), 2) > 0;

    insert into pg_temp.tmp_fresh_deduction_diffs (
      key_type,
      key_value,
      expected_ex,
      actual_ex,
      ord
    )
    with batch_expected_manual as (
      select
        coalesce(
          pbi_md.finance_case_id,
          case
            when pbi_md.source_ref ~ '^advance:[0-9a-fA-F-]{36}$' then replace(pbi_md.source_ref, 'advance:', '')::uuid
            else null
          end
        ) as finance_case_id,
        round(sum(coalesce(abs(pbi_md.frozen_source_amount), -pbi_md.amount_ex_vat, -pbi_md.amount_inc_vat, 0)), 2)::numeric(12,2) as expected_ex
      from public.pay_batch_items pbi_md
      join public.pay_batch_candidates pbc_md
        on pbc_md.id = pbi_md.pay_batch_candidate_id
      where pbc_md.pay_batch_id = p_pay_batch_id
        and (
          v_scope <> 'PAYE'
          or coalesce(pbc_md.awaiting_net_amount, false) = false
        )
        and coalesce(pbi_md.is_voided, false) = false
      and not exists (
        select 1
        from public.pay_payment_correction_items as ppc_pbi_md_fresh_exclusion
        where ppc_pbi_md_fresh_exclusion.pay_batch_item_id = pbi_md.id
          and ppc_pbi_md_fresh_exclusion.status = 'APPLIED'
          and ppc_pbi_md_fresh_exclusion.correction_item_kind in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
      )
        and pbi_md.item_type = 'MANUAL_DEBT_RECOVERY'
        and (
          v_scope <> 'PAYE'
          or upper(coalesce(pbi_md.paye_treatment, 'NET_DEDUCT')) not in ('GROSS_DEDUCT', 'GROSS_DEDUCTION', 'DEDUCT_GROSS')
        )
        and pbi_md.repayment_week_start = v_week_start
      group by 1
    ),
    actual_manual as (
      select
        temd.finance_case_id,
        round(sum(temd.take_ex), 2)::numeric(12,2) as actual_ex
      from pg_temp.tmp_fresh_expected_manual_debt_alloc temd
      group by temd.finance_case_id
    ),
    union_keys as (
      select bem.finance_case_id from batch_expected_manual bem
      union
      select am.finance_case_id from actual_manual am
    )
    select
      'MANUAL_DEBT_RECOVERY' as key_type,
      uk.finance_case_id::text as key_value,
      round(coalesce(bem.expected_ex, 0), 2)::numeric(12,2) as expected_ex,
      round(coalesce(am.actual_ex, 0), 2)::numeric(12,2) as actual_ex,
      32 as ord
    from union_keys uk
    left join batch_expected_manual bem
      on bem.finance_case_id = uk.finance_case_id
    left join actual_manual am
      on am.finance_case_id = uk.finance_case_id
    where uk.finance_case_id is not null
      and round(coalesce(bem.expected_ex, 0), 2) <> round(coalesce(am.actual_ex, 0), 2);

    insert into pg_temp.tmp_fresh_deduction_diffs (
      key_type,
      key_value,
      expected_ex,
      actual_ex,
      ord
    )
    with batch_expected_loans as (
      select
        coalesce(
          pbi_ln.finance_case_id,
          case
            when pbi_ln.source_ref ~ '^advance:[0-9a-fA-F-]{36}$' then replace(pbi_ln.source_ref, 'advance:', '')::uuid
            else null
          end
        ) as finance_case_id,
        round(sum(coalesce(abs(pbi_ln.frozen_source_amount), -pbi_ln.amount_ex_vat, -pbi_ln.amount_inc_vat, 0)), 2)::numeric(12,2) as expected_ex
      from public.pay_batch_items pbi_ln
      join public.pay_batch_candidates pbc_ln
        on pbc_ln.id = pbi_ln.pay_batch_candidate_id
      where pbc_ln.pay_batch_id = p_pay_batch_id
        and (
          v_scope <> 'PAYE'
          or coalesce(pbc_ln.awaiting_net_amount, false) = false
        )
        and coalesce(pbi_ln.is_voided, false) = false
      and not exists (
        select 1
        from public.pay_payment_correction_items as ppc_pbi_ln_fresh_exclusion
        where ppc_pbi_ln_fresh_exclusion.pay_batch_item_id = pbi_ln.id
          and ppc_pbi_ln_fresh_exclusion.status = 'APPLIED'
          and ppc_pbi_ln_fresh_exclusion.correction_item_kind in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
      )
        and pbi_ln.item_type = 'LOAN_REPAYMENT'
        and (
          v_scope <> 'PAYE'
          or upper(coalesce(pbi_ln.paye_treatment, 'NET_DEDUCT')) not in ('GROSS_DEDUCT', 'GROSS_DEDUCTION', 'DEDUCT_GROSS')
        )
        and pbi_ln.repayment_week_start = v_week_start
      group by 1
    ),
    actual_loans as (
      select
        tel.loan_id as finance_case_id,
        round(sum(tel.take_ex), 2)::numeric(12,2) as actual_ex
      from pg_temp.tmp_fresh_expected_loan_alloc tel
      group by tel.loan_id
    ),
    union_keys as (
      select bel.finance_case_id from batch_expected_loans bel
      union
      select al.finance_case_id from actual_loans al
    )
    select
      'LOAN_REPAYMENT' as key_type,
      uk.finance_case_id::text as key_value,
      round(coalesce(bel.expected_ex, 0), 2)::numeric(12,2) as expected_ex,
      round(coalesce(al.actual_ex, 0), 2)::numeric(12,2) as actual_ex,
      33 as ord
    from union_keys uk
    left join batch_expected_loans bel
      on bel.finance_case_id = uk.finance_case_id
    left join actual_loans al
      on al.finance_case_id = uk.finance_case_id
    where uk.finance_case_id is not null
      and round(coalesce(bel.expected_ex, 0), 2) <> round(coalesce(al.actual_ex, 0), 2);

    select count(*)::int
    into v_overpay_ded_diff_ct
    from pg_temp.tmp_fresh_deduction_diffs tdd
    where tdd.key_type = 'OVERPAYMENT_RECOVERY';

    select count(*)::int
    into v_manual_debt_ded_diff_ct
    from pg_temp.tmp_fresh_deduction_diffs tdd
    where tdd.key_type = 'MANUAL_DEBT_RECOVERY';

    select count(*)::int
    into v_loan_ded_diff_ct
    from pg_temp.tmp_fresh_deduction_diffs tdd
    where tdd.key_type = 'LOAN_REPAYMENT';

    v_ded_diff_ct := coalesce(v_overpay_ded_diff_ct, 0)
                     + coalesce(v_manual_debt_ded_diff_ct, 0)
                     + coalesce(v_loan_ded_diff_ct, 0);

    if exists (
      select 1
      from pg_temp.tmp_fresh_state_diffs tsd
      where tsd.key_type = 'DEDUCTION_TEMPLATE'
    ) then
      v_is_stale := true;
      v_reasons := array_append(v_reasons, 'DEDUCTION_TEMPLATE_MISSING');
    end if;

    if v_ded_diff_ct > 0 then
      v_is_stale := true;
      v_reasons := array_append(v_reasons, 'DEDUCTION_CHANGED');
    end if;
  end if;
  ---------------------------------------------------------------------------
  -- PAYE_NET_CHANGED:
  -- PAYE net entry and PAYE net edits are draft-owned mutable batch data,
  -- not live-truth freshness drift. Users must be able to enter, save,
  -- revisit, and amend PAYE net values on an existing draft without making
  -- the batch stale.
  --
  -- Real live-truth drift for the batch remains covered by the economic-key,
  -- reservation, deduction, snooze, override, restructuring, writeoff, and
  -- guardrail checks above. PAYE_NET is therefore intentionally excluded from
  -- stale detection here.
  ---------------------------------------------------------------------------
  if v_scope = 'PAYE' and coalesce(v_batch_kind_fixed,'') <> 'LOANS' then
    v_paye_net_diff_ct := 0;
  end if;

  ---------------------------------------------------------------------------
  -- Build final diff array from the temp diff tables.
  ---------------------------------------------------------------------------
  with diff_rows as (
    select
      t.ord,
      coalesce(t.timesheet_id::text, '') as sort1,
      t.key_type as sort2,
      t.key_value as sort3,
      jsonb_build_object(
        'timesheet_id', t.timesheet_id::text,
        'key_type', t.key_type,
        'key_value', t.key_value,
        'expected', t.expected_text,
        'actual', t.actual_text
      ) as diff_json
    from (
      select *
      from pg_temp.tmp_fresh_ts_diffs t0
      order by coalesce(t0.timesheet_id::text, ''), t0.key_type, t0.key_value
      limit v_diff_limit
    ) t

    union all

    select
      k.ord,
      coalesce(k.timesheet_id::text, '') as sort1,
      k.key_type as sort2,
      k.key_value as sort3,
      jsonb_build_object(
        'timesheet_id', k.timesheet_id::text,
        'key_type', k.key_type,
        'key_value', k.key_value,
        'expected', k.expected_ex,
        'actual', k.actual_ex
      ) as diff_json
    from (
      select *
      from pg_temp.tmp_fresh_key_diffs k0
      order by coalesce(k0.timesheet_id::text, ''), k0.key_type, k0.key_value
      limit v_diff_limit
    ) k

    union all

    select
      d.ord,
      '' as sort1,
      d.key_type as sort2,
      d.key_value as sort3,
      jsonb_build_object(
        'timesheet_id', null,
        'key_type', d.key_type,
        'key_value', d.key_value,
        'expected', d.expected_ex,
        'actual', d.actual_ex
      ) as diff_json
    from (
      select *
      from pg_temp.tmp_fresh_deduction_diffs d0
      order by d0.ord, d0.key_type, d0.key_value
      limit v_diff_limit
    ) d

    union all

    select
      p.ord,
      p.candidate_id::text as sort1,
      p.key_type as sort2,
      p.key_value as sort3,
      jsonb_build_object(
        'timesheet_id', null,
        'key_type', p.key_type,
        'key_value', p.key_value,
        'expected', p.expected_ex,
        'actual', p.actual_ex
      ) as diff_json
    from (
      select *
      from pg_temp.tmp_fresh_paye_net_diffs p0
      order by p0.candidate_id::text, p0.key_type, p0.key_value
      limit v_diff_limit
    ) p

    union all

    select
      s.ord,
      coalesce(s.candidate_id::text, coalesce(s.timesheet_id::text, '')) as sort1,
      s.key_type as sort2,
      s.key_value as sort3,
      jsonb_build_object(
        'timesheet_id', s.timesheet_id::text,
        'key_type', s.key_type,
        'key_value', s.key_value,
        'expected', s.expected_text,
        'actual', s.actual_text
      ) as diff_json
    from (
      select *
      from pg_temp.tmp_fresh_state_diffs s0
      order by coalesce(s0.candidate_id::text, coalesce(s0.timesheet_id::text, '')), s0.key_type, s0.key_value
      limit v_diff_limit
    ) s

    union all

    select
      99 as ord,
      '' as sort1,
      'INFO' as sort2,
      'COUNTS' as sort3,
      jsonb_build_object(
        'timesheet_id', null,
        'key_type', 'INFO',
        'key_value', 'COUNTS',
        'expected', jsonb_build_object(
          'ts_changed', v_ts_changed_ct,
          'key_diffs', v_key_diff_ct,
          'finance_reservation_diffs', v_finance_reservation_diff_ct,
          'snooze_diffs', v_snooze_diff_ct,
          'restructure_diffs', v_restructure_diff_ct,
          'writeoff_diffs', v_writeoff_diff_ct,
          'timesheet_override_diffs', v_timesheet_override_diff_ct,
          'paye_guardrail_diffs', v_paye_guardrail_diff_ct,
          'ded_diffs', v_ded_diff_ct,
          'overpayment_ded_diffs', v_overpay_ded_diff_ct,
          'manual_debt_ded_diffs', v_manual_debt_ded_diff_ct,
          'loan_ded_diffs', v_loan_ded_diff_ct,
          'paye_net_diffs', v_paye_net_diff_ct
        ),
        'actual', null
      ) as diff_json
  )
  select
    coalesce(
      jsonb_agg(dr.diff_json order by dr.ord, dr.sort1, dr.sort2, dr.sort3),
      '[]'::jsonb
    )
  into v_diffs
  from diff_rows dr;

  v_carry_forward_freshness_result := public._pay_manual_adjustment_carry_forward_freshness_check(
    p_pay_batch_id,
    NULL::uuid[],
    NULL::uuid[],
    jsonb_build_object('pay_batch_id', p_pay_batch_id::text),
    p_actor_user_id
  );

  v_carry_forward_blocker_count := jsonb_array_length(COALESCE(v_carry_forward_freshness_result->'blockers', '[]'::jsonb));

  IF COALESCE(v_carry_forward_blocker_count, 0) > 0 THEN
    v_is_stale := true;

    IF EXISTS (
      SELECT 1
      FROM jsonb_array_elements(COALESCE(v_carry_forward_freshness_result->'blockers', '[]'::jsonb)) AS carry_forward_blockers(blocker_value)
      WHERE COALESCE(carry_forward_blockers.blocker_value->>'code', '') IN (
        'MANUAL_ADJUSTMENT_CARRY_FORWARD_CHANGED',
        'MANUAL_ADJUSTMENT_CARRY_FORWARD_AMOUNT_CHANGED',
        'MANUAL_ADJUSTMENT_CARRY_FORWARD_SOURCE_AMOUNT_CHANGED',
        'MANUAL_ADJUSTMENT_CARRY_FORWARD_RESERVED_ELSEWHERE',
        'RESERVED_CARRY_FORWARD_MISSING_FROM_TARGET_BATCH',
        'RESERVED_CARRY_FORWARD_TARGET_ITEM_VOIDED',
        'PENDING_CARRY_FORWARD_NOT_INCLUDED_IN_TARGET_BATCH'
      )
    ) THEN
      v_reasons := array_append(v_reasons, 'Manual adjustment carry-forward changed');
    END IF;

    IF EXISTS (
      SELECT 1
      FROM jsonb_array_elements(COALESCE(v_carry_forward_freshness_result->'blockers', '[]'::jsonb)) AS carry_forward_blockers(blocker_value)
      WHERE COALESCE(carry_forward_blockers.blocker_value->>'code', '') = 'MANUAL_ADJUSTMENT_CARRY_FORWARD_CONSUMED_ELSEWHERE'
    ) THEN
      v_reasons := array_append(v_reasons, 'Manual adjustment carry-forward was consumed elsewhere');
    END IF;

    IF EXISTS (
      SELECT 1
      FROM jsonb_array_elements(COALESCE(v_carry_forward_freshness_result->'blockers', '[]'::jsonb)) AS carry_forward_blockers(blocker_value)
      WHERE COALESCE(carry_forward_blockers.blocker_value->>'code', '') = 'SOURCE_PAYMENT_SCOPE_BECAME_PAID_OR_SETTLED'
    ) THEN
      v_reasons := array_append(v_reasons, 'Source payment scope changed');
    END IF;

    SELECT COALESCE(
      jsonb_agg(
        jsonb_build_object(
          'timesheet_id', NULL,
          'key_type', 'MANUAL_CARRY_FORWARD',
          'key_value', COALESCE(carry_forward_blockers.blocker_value->>'carry_forward_id', carry_forward_blockers.blocker_value->>'code'),
          'expected', jsonb_build_object(
            'status', 'fresh',
            'message', 'Manual adjustment carry-forward remains reserved for this batch with unchanged signed amounts.'
          ),
          'actual', carry_forward_blockers.blocker_value
        )
        ORDER BY COALESCE(carry_forward_blockers.blocker_value->>'carry_forward_id', ''), COALESCE(carry_forward_blockers.blocker_value->>'code', '')
      ),
      '[]'::jsonb
    )
    INTO v_carry_forward_freshness_result
    FROM jsonb_array_elements(COALESCE(v_carry_forward_freshness_result->'blockers', '[]'::jsonb)) AS carry_forward_blockers(blocker_value);

    v_diffs := COALESCE(v_diffs, '[]'::jsonb) || COALESCE(v_carry_forward_freshness_result, '[]'::jsonb);
  END IF;

  IF EXISTS (
    SELECT 1
    FROM public.pay_payment_correction_items AS correction_items
    WHERE correction_items.pay_batch_id = p_pay_batch_id
      AND correction_items.status = 'APPLIED'
      AND correction_items.correction_item_kind = 'PRE_BANK_CANCEL'
  ) THEN
    v_is_stale := true;
    v_reasons := array_append(v_reasons, 'Payment was cancelled/recalculated');
    v_diffs := COALESCE(v_diffs, '[]'::jsonb) || jsonb_build_array(jsonb_build_object(
      'timesheet_id', NULL,
      'key_type', 'PAYMENT_CORRECTION',
      'key_value', 'PRE_BANK_CANCEL',
      'expected', jsonb_build_object('payment_scope', 'active'),
      'actual', jsonb_build_object('payment_scope', 'cancelled/recalculated')
    ));
  END IF;

  IF EXISTS (
    SELECT 1
    FROM public.pay_payment_correction_items AS correction_items
    WHERE correction_items.pay_batch_id = p_pay_batch_id
      AND correction_items.status = 'APPLIED'
      AND correction_items.correction_item_kind = 'NO_MONEY_UNWIND'
  ) THEN
    v_is_stale := true;
    v_reasons := array_append(v_reasons, 'Financials were rewound');
    v_diffs := COALESCE(v_diffs, '[]'::jsonb) || jsonb_build_array(jsonb_build_object(
      'timesheet_id', NULL,
      'key_type', 'PAYMENT_CORRECTION',
      'key_value', 'NO_MONEY_UNWIND',
      'expected', jsonb_build_object('financials', 'reserved/active'),
      'actual', jsonb_build_object('financials', 'rewound')
    ));
  END IF;

  if array_length(v_reasons,1) is not null then
    select coalesce(array_agg(distinct r order by r), array[]::text[])
    into v_reasons
    from unnest(v_reasons) r;
  end if;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_BATCH_VALIDATE_FRESHNESS',
      jsonb_build_object(
        'pay_batch_id', p_pay_batch_id::text,
        'pay_date', v_pay_date::text,
        'week_start', v_week_start::text,
        'batch_kind_fixed', coalesce(v_batch_kind_fixed, null),
        'scope', coalesce(v_scope, null),
        'is_stale', v_is_stale,
        'stale_reasons', coalesce(to_jsonb(v_reasons), '[]'::jsonb),
        'counts', jsonb_build_object(
          'timesheet_changed', v_ts_changed_ct,
          'stable_key_diffs', v_key_diff_ct,
          'finance_reservation_diffs', v_finance_reservation_diff_ct,
          'snooze_diffs', v_snooze_diff_ct,
          'restructure_diffs', v_restructure_diff_ct,
          'writeoff_diffs', v_writeoff_diff_ct,
          'timesheet_override_diffs', v_timesheet_override_diff_ct,
          'paye_guardrail_diffs', v_paye_guardrail_diff_ct,
          'deduction_diffs', v_ded_diff_ct,
          'overpayment_deduction_diffs', v_overpay_ded_diff_ct,
          'manual_debt_deduction_diffs', v_manual_debt_ded_diff_ct,
          'loan_deduction_diffs', v_loan_ded_diff_ct,
          'paye_net_diffs', v_paye_net_diff_ct
        )
      ),
      'pay_batches',
      p_pay_batch_id::text
    );
  exception when others then
    null;
  end;

  return jsonb_build_object(
    'is_stale', v_is_stale,
    'stale_reasons', coalesce(to_jsonb(v_reasons), '[]'::jsonb),
    'diff', coalesce(v_diffs, '[]'::jsonb)
  );
end;
$function$;

-- _pay_candidate_arranged_pay_wtd_before(uuid,date,date,timestamp with time zone,uuid)
CREATE OR REPLACE FUNCTION public._pay_candidate_arranged_pay_wtd_before(p_candidate_id uuid, p_week_start date, p_pay_date date, p_before_created_at_utc timestamp with time zone DEFAULT NULL::timestamp with time zone, p_before_pay_batch_id uuid DEFAULT NULL::uuid)
 RETURNS numeric
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_result numeric(12,2);
BEGIN
  IF p_candidate_id IS NULL THEN
    RAISE EXCEPTION 'public._pay_candidate_arranged_pay_wtd_before: p_candidate_id is required';
  END IF;

  IF p_week_start IS NULL THEN
    RAISE EXCEPTION 'public._pay_candidate_arranged_pay_wtd_before: p_week_start is required';
  END IF;

  IF p_pay_date IS NULL THEN
    RAISE EXCEPTION 'public._pay_candidate_arranged_pay_wtd_before: p_pay_date is required';
  END IF;

  SELECT
    round(
      coalesce(
        sum(
          CASE
            WHEN public.pay_batch_candidates.net_bank_amount IS NOT NULL THEN public.pay_batch_candidates.net_bank_amount
            WHEN public.pay_batch_candidates.gross_preview IS NOT NULL THEN public.pay_batch_candidates.gross_preview
            ELSE 0::numeric
          END
        ),
        0::numeric
      ),
      2
    )::numeric(12,2)
  INTO v_result
  FROM public.pay_batch_candidates
  JOIN public.pay_batches
    ON public.pay_batches.id = public.pay_batch_candidates.pay_batch_id
  WHERE public.pay_batch_candidates.candidate_id = p_candidate_id
    AND public.pay_batches.pay_date >= p_week_start
    AND public.pay_batches.pay_date < (p_week_start + 7)
    AND public.pay_batches.cancelled_at_utc IS NULL
    AND upper(coalesce(public.pay_batches.status::text, '')) <> 'CANCELLED'
    AND upper(coalesce(public.pay_batches.batch_kind_fixed::text, 'PAYROLL')) <> 'LOANS'
    AND (p_before_pay_batch_id IS NULL OR public.pay_batches.id <> p_before_pay_batch_id)
    AND (
      public.pay_batches.pay_date < p_pay_date
      OR (
        public.pay_batches.pay_date = p_pay_date
        AND (
          p_before_created_at_utc IS NULL
          OR public.pay_batches.created_at_utc < p_before_created_at_utc
          OR (
            public.pay_batches.created_at_utc = p_before_created_at_utc
            AND p_before_pay_batch_id IS NOT NULL
            AND public.pay_batches.id::text < p_before_pay_batch_id::text
          )
        )
      )
    );

  RETURN coalesce(v_result, 0::numeric(12,2));
END;
$function$;

-- _pay_candidate_week_totals(uuid[],date)
CREATE OR REPLACE FUNCTION public._pay_candidate_week_totals(p_candidate_ids uuid[], p_week_start date)
 RETURNS TABLE(candidate_id uuid, week_start date, paid_wtd numeric, loan_repaid_wtd numeric, overpay_recovered_wtd numeric)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  PERFORM public._imp_debug_audit(
    NULL::uuid,
    'PAY_CANDIDATE_WEEK_TOTALS_START',
    jsonb_build_object(
      'candidate_count', COALESCE(array_length(p_candidate_ids, 1), 0),
      'week_start', p_week_start
    ),
    'pay_candidate_week_totals',
    COALESCE(p_week_start::text, 'NO_WEEK_START'),
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  RETURN QUERY
  WITH
  inp AS (
    SELECT
      COALESCE(
        (
          SELECT array_agg(DISTINCT input_candidate_ids.candidate_id_value)
          FROM unnest(COALESCE(p_candidate_ids, ARRAY[]::uuid[])) AS input_candidate_ids(candidate_id_value)
          WHERE input_candidate_ids.candidate_id_value IS NOT NULL
        ),
        ARRAY[]::uuid[]
      ) AS cand_ids,
      p_week_start AS week_start,
      (p_week_start + INTERVAL '6 days')::date AS week_end
  ),
  eligible_batches AS (
    SELECT
      public.pay_batches.id AS pay_batch_id
    FROM inp
    JOIN public.pay_batches
      ON public.pay_batches.pay_date >= inp.week_start
     AND public.pay_batches.pay_date <= inp.week_end
    WHERE public.pay_batches.cancelled_at_utc IS NULL
      AND upper(coalesce(public.pay_batches.batch_kind_fixed,'')) <> 'LOANS'
  ),
  settled_candidate_rows AS (
    SELECT
      public.pay_batch_candidates.id AS pay_batch_candidate_id,
      public.pay_batch_candidates.candidate_id,
      public.pay_batch_candidates.pay_batch_id,
      COALESCE(public.pay_batch_candidates.net_bank_amount,0)::numeric AS net_bank_amount
    FROM inp
    JOIN public.pay_batch_candidates
      ON public.pay_batch_candidates.candidate_id = ANY(inp.cand_ids)
    JOIN eligible_batches
      ON eligible_batches.pay_batch_id = public.pay_batch_candidates.pay_batch_id
    WHERE public.pay_batch_candidates.settled_at_utc IS NOT NULL
      AND upper(coalesce(public.pay_batch_candidates.settlement_status,'')) = 'SETTLED'
  ),
  corrected_candidate_item_amounts AS (
    SELECT
      settled_candidate_rows.pay_batch_candidate_id,
      round(
        COALESCE(
          sum(COALESCE(public.pay_batch_items.amount_inc_vat, public.pay_batch_items.amount_ex_vat, 0)),
          0
        ),
        2
      ) AS corrected_amount_inc_vat
    FROM settled_candidate_rows
    JOIN public.pay_batch_items
      ON public.pay_batch_items.pay_batch_candidate_id = settled_candidate_rows.pay_batch_candidate_id
    JOIN public.pay_payment_correction_items
      ON public.pay_payment_correction_items.pay_batch_item_id = public.pay_batch_items.id
     AND public.pay_payment_correction_items.status = 'APPLIED'
     AND public.pay_payment_correction_items.correction_item_kind IN ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
    GROUP BY settled_candidate_rows.pay_batch_candidate_id
  ),
  paid AS (
    SELECT
      settled_candidate_rows.candidate_id,
      round(
        COALESCE(
          sum(
            COALESCE(settled_candidate_rows.net_bank_amount,0)
            - COALESCE(corrected_candidate_item_amounts.corrected_amount_inc_vat,0)
          ),
          0
        ),
        2
      ) AS paid_wtd
    FROM settled_candidate_rows
    LEFT JOIN corrected_candidate_item_amounts
      ON corrected_candidate_item_amounts.pay_batch_candidate_id = settled_candidate_rows.pay_batch_candidate_id
    GROUP BY settled_candidate_rows.candidate_id
  ),
  loan_rep AS (
    SELECT
      settled_candidate_rows.candidate_id,
      round(sum(abs(coalesce(public.pay_batch_items.amount_ex_vat, public.pay_batch_items.amount_inc_vat, 0))),2) AS loan_repaid_wtd
    FROM settled_candidate_rows
    JOIN public.pay_batch_items
      ON public.pay_batch_items.pay_batch_candidate_id = settled_candidate_rows.pay_batch_candidate_id
    WHERE COALESCE(public.pay_batch_items.is_voided, false) = false
      AND public.pay_batch_items.item_type = 'LOAN_REPAYMENT'
      AND NOT EXISTS (
        SELECT 1
        FROM public.pay_payment_correction_items AS applied_corrections
        WHERE applied_corrections.pay_batch_item_id = public.pay_batch_items.id
          AND applied_corrections.status = 'APPLIED'
          AND applied_corrections.correction_item_kind IN ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
      )
    GROUP BY settled_candidate_rows.candidate_id
  ),
  overpay_rec AS (
    SELECT
      settled_candidate_rows.candidate_id,
      round(sum(abs(coalesce(public.pay_batch_items.amount_ex_vat, public.pay_batch_items.amount_inc_vat, 0))),2) AS overpay_recovered_wtd
    FROM settled_candidate_rows
    JOIN public.pay_batch_items
      ON public.pay_batch_items.pay_batch_candidate_id = settled_candidate_rows.pay_batch_candidate_id
    WHERE COALESCE(public.pay_batch_items.is_voided, false) = false
      AND public.pay_batch_items.item_type = 'OVERPAYMENT_RECOVERY'
      AND NOT EXISTS (
        SELECT 1
        FROM public.pay_payment_correction_items AS applied_corrections
        WHERE applied_corrections.pay_batch_item_id = public.pay_batch_items.id
          AND applied_corrections.status = 'APPLIED'
          AND applied_corrections.correction_item_kind IN ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
      )
    GROUP BY settled_candidate_rows.candidate_id
  )
  SELECT
    public.candidates.id AS candidate_id,
    (SELECT inp.week_start FROM inp) AS week_start,
    COALESCE(paid.paid_wtd,0) AS paid_wtd,
    COALESCE(loan_rep.loan_repaid_wtd,0) AS loan_repaid_wtd,
    COALESCE(overpay_rec.overpay_recovered_wtd,0) AS overpay_recovered_wtd
  FROM inp
  JOIN public.candidates
    ON public.candidates.id = ANY(inp.cand_ids)
  LEFT JOIN paid
    ON paid.candidate_id = public.candidates.id
  LEFT JOIN loan_rep
    ON loan_rep.candidate_id = public.candidates.id
  LEFT JOIN overpay_rec
    ON overpay_rec.candidate_id = public.candidates.id;

  PERFORM public._imp_debug_audit(
    NULL::uuid,
    'PAY_CANDIDATE_WEEK_TOTALS_RESULT',
    jsonb_build_object(
      'candidate_count', COALESCE(array_length(p_candidate_ids, 1), 0),
      'week_start', p_week_start
    ),
    'pay_candidate_week_totals',
    COALESCE(p_week_start::text, 'NO_WEEK_START'),
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  RETURN;

EXCEPTION
  WHEN OTHERS THEN
    PERFORM public._imp_debug_audit(
      NULL::uuid,
      'PAY_CANDIDATE_WEEK_TOTALS_ERROR',
      jsonb_build_object(
        'candidate_count', COALESCE(array_length(p_candidate_ids, 1), 0),
        'week_start', p_week_start,
        'sqlstate', SQLSTATE,
        'error_message', SQLERRM
      ),
      'pay_candidate_week_totals',
      COALESCE(p_week_start::text, 'NO_WEEK_START'),
      NULL::jsonb,
      NULL::text,
      NULL::text,
      NULL::text
    );
    RAISE;
END;
$function$;

-- _pay_convert_paye_to_umbrella(numeric,numeric,numeric,boolean)
CREATE OR REPLACE FUNCTION public._pay_convert_paye_to_umbrella(p_paye_ex numeric, p_erni_pct numeric, p_vat_rate_pct numeric, p_vat_chargeable boolean)
 RETURNS jsonb
 LANGUAGE plpgsql
 IMMUTABLE
AS $function$
declare
  v_erni_mult numeric := public._pay_pct_to_mult(p_erni_pct);
  v_ex numeric := round(coalesce(p_paye_ex,0) * v_erni_mult, 2);
begin
  return public._pay_umbrella_vat_calc(v_ex, p_vat_rate_pct, p_vat_chargeable);
end;
$function$;

-- _pay_convert_umbrella_to_paye_ex(numeric,numeric)
CREATE OR REPLACE FUNCTION public._pay_convert_umbrella_to_paye_ex(p_umbrella_ex numeric, p_erni_pct numeric)
 RETURNS numeric
 LANGUAGE plpgsql
 IMMUTABLE
AS $function$
declare
  v_erni_mult numeric := public._pay_pct_to_mult(p_erni_pct);
  v_ex numeric := coalesce(p_umbrella_ex,0);
begin
  if v_erni_mult <= 0 then v_erni_mult := 1; end if;
  return round(v_ex / v_erni_mult, 2);
end;
$function$;

-- _pay_csv_parse_line(text)
CREATE OR REPLACE FUNCTION public._pay_csv_parse_line(p_line text)
 RETURNS text[]
 LANGUAGE plpgsql
 IMMUTABLE
AS $function$
declare
  v text := coalesce(p_line,'');
  i int := 1;
  ch text;
  in_quotes boolean := false;
  cur text := '';
  out_arr text[] := array[]::text[];
begin
  while i <= char_length(v) loop
    ch := substr(v, i, 1);

    if ch = '"' then
      -- Double quote escape inside quoted field
      if in_quotes and i < char_length(v) and substr(v, i+1, 1) = '"' then
        cur := cur || '"';
        i := i + 1;
      else
        in_quotes := not in_quotes;
      end if;

    elsif ch = ',' and not in_quotes then
      out_arr := out_arr || array[cur];
      cur := '';

    else
      cur := cur || ch;
    end if;

    i := i + 1;
  end loop;

  out_arr := out_arr || array[cur];
  return out_arr;
end;
$function$;

-- _pay_csv_trim_field(text)
CREATE OR REPLACE FUNCTION public._pay_csv_trim_field(p_field text)
 RETURNS text
 LANGUAGE plpgsql
 IMMUTABLE
AS $function$
begin
  return nullif(btrim(coalesce(p_field,'')), '');
end;
$function$;

-- _pay_current_timesheet_entitlement_components(uuid[])
CREATE OR REPLACE FUNCTION public._pay_current_timesheet_entitlement_components(p_timesheet_ids uuid[])
 RETURNS TABLE(timesheet_id uuid, key_type text, key_value text, truth_ex_vat numeric, baseline_ex_vat numeric, truth_inc_vat numeric, baseline_inc_vat numeric)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
with
inp as (
  select coalesce(
    (
      select array_agg(distinct t_input.x order by t_input.x)
      from unnest(coalesce(p_timesheet_ids, array[]::uuid[])) as t_input(x)
      where t_input.x is not null
    ),
    array[]::uuid[]
  ) as ts_ids
),
rotation_scope_rows as (
  select
    scope_rows.requested_timesheet_id,
    scope_rows.booking_id,
    scope_rows.canonical_timesheet_id,
    scope_rows.family_timesheet_id,
    scope_rows.family_is_current,
    scope_rows.family_version,
    scope_rows.requested_is_canonical
  from inp as input_scope
  join public._pay_timesheet_rotation_scope(input_scope.ts_ids) as scope_rows
    on true
),
rotation_scope_keyed as (
  select
    rotation_scope_rows.requested_timesheet_id,
    rotation_scope_rows.booking_id,
    rotation_scope_rows.canonical_timesheet_id,
    rotation_scope_rows.family_timesheet_id,
    rotation_scope_rows.family_is_current,
    rotation_scope_rows.family_version,
    rotation_scope_rows.requested_is_canonical,
    coalesce(rotation_scope_rows.booking_id, rotation_scope_rows.requested_timesheet_id::text) as scope_family_key
  from rotation_scope_rows
  where rotation_scope_rows.requested_timesheet_id is not null
),
projection_targets as (
  select
    rotation_scope_keyed.scope_family_key,
    coalesce(
      (
        array_agg(distinct rotation_scope_keyed.canonical_timesheet_id order by rotation_scope_keyed.canonical_timesheet_id)
        filter (
          where coalesce(rotation_scope_keyed.requested_is_canonical, false) = true
            and rotation_scope_keyed.canonical_timesheet_id is not null
        )
      )[1],
      (
        array_agg(distinct rotation_scope_keyed.requested_timesheet_id order by rotation_scope_keyed.requested_timesheet_id)
        filter (where rotation_scope_keyed.requested_timesheet_id is not null)
      )[1],
      (
        array_agg(distinct rotation_scope_keyed.canonical_timesheet_id order by rotation_scope_keyed.canonical_timesheet_id)
        filter (where rotation_scope_keyed.canonical_timesheet_id is not null)
      )[1]
    ) as projected_timesheet_id,
    (
      array_agg(distinct rotation_scope_keyed.canonical_timesheet_id order by rotation_scope_keyed.canonical_timesheet_id)
      filter (where rotation_scope_keyed.canonical_timesheet_id is not null)
    )[1] as canonical_timesheet_id
  from rotation_scope_keyed
  group by rotation_scope_keyed.scope_family_key
),
family_to_projection as (
  select distinct
    rotation_scope_keyed.family_timesheet_id,
    projection_targets.projected_timesheet_id,
    projection_targets.canonical_timesheet_id
  from rotation_scope_keyed
  join projection_targets
    on projection_targets.scope_family_key = rotation_scope_keyed.scope_family_key
  where rotation_scope_keyed.family_timesheet_id is not null
    and projection_targets.projected_timesheet_id is not null
),
canonical_projection as (
  select distinct
    family_to_projection.canonical_timesheet_id,
    family_to_projection.projected_timesheet_id
  from family_to_projection
  where family_to_projection.canonical_timesheet_id is not null
    and family_to_projection.projected_timesheet_id is not null
),
tf as (
  select
    canonical_projection.projected_timesheet_id as timesheet_id,
    canonical_projection.canonical_timesheet_id as canonical_timesheet_id,
    tfin.total_pay_ex_vat,
    tfin.invoice_breakdown_json,
    tfin.additional_units_json,
    tfin.expenses_pay_ex_vat,
    tfin.travel_pay_ex_vat,
    tfin.accommodation_pay_ex_vat,
    tfin.other_pay_ex_vat,
    tfin.mileage_pay_ex_vat,
    timesheet_rows.booking_id,
    upper(coalesce(timesheet_rows.sheet_scope::text,'')) as sheet_scope,
    timesheet_rows.reference_number,
    timesheet_rows.worked_start_iso as ts_worked_start_iso,
    timesheet_rows.worked_end_iso as ts_worked_end_iso,
    timesheet_rows.break_start_iso as ts_break_start_iso,
    timesheet_rows.break_end_iso as ts_break_end_iso,
    timesheet_rows.break_minutes as ts_break_minutes,
    timesheet_rows.actual_schedule_json as ts_actual_schedule_json,
    tfin.worked_start_iso as tf_worked_start_iso,
    tfin.worked_end_iso as tf_worked_end_iso,
    tfin.break_start_iso as tf_break_start_iso,
    tfin.break_end_iso as tf_break_end_iso,
    tfin.break_minutes as tf_break_minutes,
    tfin.actual_schedule_json as tf_actual_schedule_json
  from canonical_projection
  join public.timesheets_financials as tfin
    on tfin.is_current = true
   and tfin.timesheet_id = canonical_projection.canonical_timesheet_id
  join public.timesheets as timesheet_rows
    on timesheet_rows.timesheet_id = canonical_projection.canonical_timesheet_id
   and timesheet_rows.is_current = true
   and timesheet_rows.revoked_at is null
   and timesheet_rows.archived_at_utc is null
   and timesheet_rows.authorised_at_server is not null
),
truth_enriched as (
  select
    tf0.timesheet_id,
    tf0.canonical_timesheet_id,
    tf0.total_pay_ex_vat,
    tf0.invoice_breakdown_json,
    tf0.additional_units_json,
    tf0.expenses_pay_ex_vat,
    tf0.travel_pay_ex_vat,
    tf0.accommodation_pay_ex_vat,
    tf0.other_pay_ex_vat,
    tf0.mileage_pay_ex_vat,
    tf0.booking_id,
    tf0.sheet_scope,
    tf0.reference_number,
    coalesce(tf0.tf_worked_start_iso, tf0.ts_worked_start_iso) as effective_worked_start_iso,
    coalesce(tf0.tf_worked_end_iso, tf0.ts_worked_end_iso) as effective_worked_end_iso,
    coalesce(tf0.tf_break_start_iso, tf0.ts_break_start_iso) as effective_break_start_iso,
    coalesce(tf0.tf_break_end_iso, tf0.ts_break_end_iso) as effective_break_end_iso,
    coalesce(tf0.tf_break_minutes, tf0.ts_break_minutes) as effective_break_minutes,
    case
      when jsonb_typeof(tf0.tf_actual_schedule_json) = 'object' then tf0.tf_actual_schedule_json
      when jsonb_typeof(tf0.tf_actual_schedule_json) = 'array' then (
        select tf_sched_item.value
        from jsonb_array_elements(tf0.tf_actual_schedule_json) as tf_sched_item(value)
        where tf_sched_item.value is not null
          and jsonb_typeof(tf_sched_item.value) = 'object'
        limit 1
      )
      when jsonb_typeof(tf0.ts_actual_schedule_json) = 'object' then tf0.ts_actual_schedule_json
      when jsonb_typeof(tf0.ts_actual_schedule_json) = 'array' then (
        select ts_sched_item.value
        from jsonb_array_elements(tf0.ts_actual_schedule_json) as ts_sched_item(value)
        where ts_sched_item.value is not null
          and jsonb_typeof(ts_sched_item.value) = 'object'
        limit 1
      )
      else null::jsonb
    end as effective_daily_schedule_json
  from tf tf0
),
truth_segments as (
  select
    te.timesheet_id,
    te.canonical_timesheet_id,
    case
      when te.invoice_breakdown_json is not null
       and jsonb_typeof(te.invoice_breakdown_json) = 'object'
       and upper(coalesce(te.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
       and jsonb_typeof(te.invoice_breakdown_json->'segments') = 'array'
      then (
        select coalesce(
          jsonb_agg(seg.value),
          '[]'::jsonb
        )
        from jsonb_array_elements(te.invoice_breakdown_json->'segments') as seg(value)
        where seg.value is not null
          and jsonb_typeof(seg.value) = 'object'
      )
      when te.sheet_scope = 'DAILY'
      then jsonb_build_array(
        jsonb_build_object(
          'segment_id', ('ts:' || te.timesheet_id::text),
          'pay_amount', round(coalesce(te.total_pay_ex_vat,0),2),
          'exclude_from_pay', false,
          'date', case
            when te.effective_worked_start_iso is not null then ((te.effective_worked_start_iso at time zone 'Europe/London')::date)::text
            else coalesce(
              nullif(btrim(coalesce(te.effective_daily_schedule_json->>'date','')), ''),
              nullif(btrim(coalesce(te.effective_daily_schedule_json->>'work_date','')), ''),
              nullif(btrim(coalesce(te.effective_daily_schedule_json->>'ymd','')), ''),
              nullif(btrim(coalesce(te.effective_daily_schedule_json->>'date_ymd','')), '')
            )
          end,
          'segment_key', ('ts:' || te.timesheet_id::text),
          'segment_stable_key', ('timesheet:' || coalesce(te.booking_id, te.timesheet_id::text)),
          'ref_num', nullif(btrim(coalesce(te.reference_number,'')), ''),
          'start_utc', case
            when te.effective_worked_start_iso is not null then te.effective_worked_start_iso::text
            else null
          end,
          'end_utc', case
            when te.effective_worked_end_iso is not null then te.effective_worked_end_iso::text
            else null
          end,
          'start', case
            when te.effective_worked_start_iso is not null then to_char((te.effective_worked_start_iso at time zone 'Europe/London'), 'HH24:MI')
            else coalesce(
              nullif(btrim(coalesce(te.effective_daily_schedule_json->>'start','')), ''),
              nullif(btrim(coalesce(te.effective_daily_schedule_json->>'worked_start','')), '')
            )
          end,
          'end', case
            when te.effective_worked_end_iso is not null then to_char((te.effective_worked_end_iso at time zone 'Europe/London'), 'HH24:MI')
            else coalesce(
              nullif(btrim(coalesce(te.effective_daily_schedule_json->>'end','')), ''),
              nullif(btrim(coalesce(te.effective_daily_schedule_json->>'worked_end','')), '')
            )
          end,
          'break_start', case
            when te.effective_break_start_iso is not null then to_char((te.effective_break_start_iso at time zone 'Europe/London'), 'HH24:MI')
            else nullif(btrim(coalesce(te.effective_daily_schedule_json->>'break_start','')), '')
          end,
          'break_end', case
            when te.effective_break_end_iso is not null then to_char((te.effective_break_end_iso at time zone 'Europe/London'), 'HH24:MI')
            else nullif(btrim(coalesce(te.effective_daily_schedule_json->>'break_end','')), '')
          end,
          'break_mins', coalesce(
            te.effective_break_minutes::numeric,
            nullif(btrim(coalesce(te.effective_daily_schedule_json->>'break_minutes','')), '')::numeric,
            nullif(btrim(coalesce(te.effective_daily_schedule_json->>'break_mins','')), '')::numeric,
            nullif(btrim(coalesce(te.effective_daily_schedule_json->>'breakMinutes','')), '')::numeric,
            nullif(btrim(coalesce(te.effective_daily_schedule_json->>'breakMin','')), '')::numeric,
            case
              when te.effective_break_start_iso is not null and te.effective_break_end_iso is not null
                then greatest(
                  0::numeric,
                  round((extract(epoch from (te.effective_break_end_iso - te.effective_break_start_iso)) / 60.0)::numeric, 0)
                )
              else null::numeric
            end
          ),
          'breaks', case
            when jsonb_typeof(te.effective_daily_schedule_json->'breaks') = 'array' then te.effective_daily_schedule_json->'breaks'
            when te.effective_break_start_iso is not null and te.effective_break_end_iso is not null
              then jsonb_build_array(
                jsonb_build_object(
                  'start', to_char((te.effective_break_start_iso at time zone 'Europe/London'), 'HH24:MI'),
                  'end', to_char((te.effective_break_end_iso at time zone 'Europe/London'), 'HH24:MI'),
                  'break_mins', coalesce(
                    te.effective_break_minutes::numeric,
                    case
                      when te.effective_break_start_iso is not null and te.effective_break_end_iso is not null
                        then greatest(
                          0::numeric,
                          round((extract(epoch from (te.effective_break_end_iso - te.effective_break_start_iso)) / 60.0)::numeric, 0)
                        )
                      else null::numeric
                    end
                  )
                )
              )
            when nullif(btrim(coalesce(te.effective_daily_schedule_json->>'break_start','')), '') is not null
             and nullif(btrim(coalesce(te.effective_daily_schedule_json->>'break_end','')), '') is not null
              then jsonb_build_array(
                jsonb_build_object(
                  'start', nullif(btrim(coalesce(te.effective_daily_schedule_json->>'break_start','')), ''),
                  'end', nullif(btrim(coalesce(te.effective_daily_schedule_json->>'break_end','')), ''),
                  'break_mins', coalesce(
                    te.effective_break_minutes::numeric,
                    nullif(btrim(coalesce(te.effective_daily_schedule_json->>'break_minutes','')), '')::numeric,
                    nullif(btrim(coalesce(te.effective_daily_schedule_json->>'break_mins','')), '')::numeric,
                    nullif(btrim(coalesce(te.effective_daily_schedule_json->>'breakMinutes','')), '')::numeric,
                    nullif(btrim(coalesce(te.effective_daily_schedule_json->>'breakMin','')), '')::numeric
                  )
                )
              )
            else '[]'::jsonb
          end
        )
      )
      else jsonb_build_array(
        jsonb_build_object(
          'segment_id', ('ts:' || te.timesheet_id::text),
          'pay_amount', round(coalesce(te.total_pay_ex_vat,0),2),
          'exclude_from_pay', false
        )
      )
    end as segments_json
  from truth_enriched te
),
adjustment_truth_rows as (
  select distinct on (
    family_to_projection.projected_timesheet_id,
    adjustment_rows.id
  )
    family_to_projection.projected_timesheet_id as timesheet_id,
    adjustment_rows.id as adjustment_id,
    adjustment_rows.created_at as adjustment_created_at,
    round(coalesce(adjustment_rows.delta_pay_ex_vat, 0), 2) as delta_pay_ex_vat
  from family_to_projection
  join public.ts_pay_adjustments as adjustment_rows
    on adjustment_rows.timesheet_id = family_to_projection.family_timesheet_id
  where adjustment_rows.as_advance = false
    and adjustment_rows.timesheet_id is not null
    and adjustment_rows.id is not null
    and family_to_projection.projected_timesheet_id is not null
  order by
    family_to_projection.projected_timesheet_id,
    adjustment_rows.id,
    adjustment_rows.created_at nulls last,
    family_to_projection.family_timesheet_id
),
truth_snapshot_like as (
  select
    truth_segment_rows.timesheet_id,
    jsonb_build_object(
      'segments', truth_segment_rows.segments_json,
      'additional_units_json', coalesce(tf1.additional_units_json,'{}'::jsonb),
      'additional_pay_ex_vat', 0,
      'expenses', jsonb_build_object(
        'expenses_pay_ex_vat', round(coalesce(tf1.expenses_pay_ex_vat,0),2),
        'travel_pay_ex_vat', round(coalesce(tf1.travel_pay_ex_vat,0),2),
        'accommodation_pay_ex_vat', round(coalesce(tf1.accommodation_pay_ex_vat,0),2),
        'other_pay_ex_vat', round(coalesce(tf1.other_pay_ex_vat,0),2),
        'mileage_pay_ex_vat', round(coalesce(tf1.mileage_pay_ex_vat,0),2)
      ),
      'adjustments', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'id', adjustment_truth_rows.adjustment_id::text,
            'delta_pay_ex_vat', adjustment_truth_rows.delta_pay_ex_vat
          )
          order by adjustment_truth_rows.adjustment_created_at nulls last, adjustment_truth_rows.adjustment_id
        )
        from adjustment_truth_rows
        where adjustment_truth_rows.timesheet_id = truth_segment_rows.timesheet_id
      ), '[]'::jsonb)
    ) as snap_json
  from truth_segments as truth_segment_rows
  join tf as tf1
    on tf1.timesheet_id = truth_segment_rows.timesheet_id
),
truth_components_raw as (
  select
    truth_snapshot_rows.timesheet_id,
    upper(nullif(btrim(coalesce(timesheet_component_rows.key_type, '')), '')) as raw_key_type,
    nullif(btrim(coalesce(timesheet_component_rows.key_value, '')), '') as raw_key_value,
    timesheet_component_rows.amount_ex_vat,
    timesheet_component_rows.amount_inc_vat,
    case
      when upper(nullif(btrim(coalesce(timesheet_component_rows.key_type, '')), '')) in ('TS_DAY','TS_TOTAL')
        then 'SEGMENT_DELTA'
      when upper(nullif(btrim(coalesce(timesheet_component_rows.key_type, '')), '')) = 'ADJUSTMENT_CODE'
        then 'ADJUSTMENT_DELTA'
      when upper(nullif(btrim(coalesce(timesheet_component_rows.key_type, '')), '')) = 'EXPENSE_CODE'
       and upper(nullif(btrim(coalesce(timesheet_component_rows.key_value, '')), '')) = 'MILEAGE'
        then 'MILEAGE_DELTA'
      when upper(nullif(btrim(coalesce(timesheet_component_rows.key_type, '')), '')) in ('ADDITIONAL_CODE','EXPENSE_CODE')
        then 'EXPENSE_DELTA'
      else null::text
    end as resolver_item_type
  from truth_snapshot_like as truth_snapshot_rows
  join lateral public._pay_timesheet_components(truth_snapshot_rows.snap_json) as timesheet_component_rows
    on true
),
truth_components as (
  select
    truth_component_rows.timesheet_id,
    resolved_truth_keys.key_type,
    resolved_truth_keys.key_value,
    truth_component_rows.amount_ex_vat,
    truth_component_rows.amount_inc_vat
  from truth_components_raw as truth_component_rows
  join lateral public._pay_policy_x_resolve_pre_draft_economic_key(
    p_timesheet_id => truth_component_rows.timesheet_id,
    p_live_source_json => jsonb_build_object(
      'timesheet_id', truth_component_rows.timesheet_id::text,
      'item_type', truth_component_rows.resolver_item_type,
      'component_key_type', truth_component_rows.raw_key_type,
      'component_key_value', truth_component_rows.raw_key_value,
      'work_date', case when truth_component_rows.raw_key_type = 'TS_DAY' then truth_component_rows.raw_key_value else null::text end
    ),
    p_item_type => truth_component_rows.resolver_item_type,
    p_key_type_hint => truth_component_rows.raw_key_type,
    p_key_value_hint => truth_component_rows.raw_key_value,
    p_work_date => case
      when truth_component_rows.raw_key_type = 'TS_DAY'
       and truth_component_rows.raw_key_value ~ '^\d{4}-\d{2}-\d{2}$'
        then truth_component_rows.raw_key_value::date
      else null::date
    end
  ) as resolved_truth_keys
    on true
  where resolved_truth_keys.key_resolution_failure_reason is null
),
active_settled_components as (
  select
    active_rows.timesheet_id,
    active_rows.key_type,
    active_rows.key_value,
    active_rows.amount_ex_vat,
    active_rows.amount_inc_vat
  from public._pay_active_settled_components((select inp.ts_ids from inp)) as active_rows
),
legacy_baseline_timesheets as (
  select distinct
    timesheet_pay_state_rows.timesheet_id as source_timesheet_id,
    family_to_projection.projected_timesheet_id as projected_timesheet_id
  from family_to_projection
  join public.timesheet_pay_state as timesheet_pay_state_rows
    on timesheet_pay_state_rows.timesheet_id = family_to_projection.family_timesheet_id
  where not exists (
    select 1
    from active_settled_components as active_check
    where active_check.timesheet_id = family_to_projection.projected_timesheet_id
  )
),
legacy_baseline_components_raw as (
  select
    legacy_baseline_timesheet_rows.projected_timesheet_id as timesheet_id,
    upper(nullif(btrim(coalesce(baseline_component_rows.key_type, '')), '')) as raw_key_type,
    nullif(btrim(coalesce(baseline_component_rows.key_value, '')), '') as raw_key_value,
    baseline_component_rows.amount_ex_vat,
    baseline_component_rows.amount_inc_vat,
    case
      when upper(nullif(btrim(coalesce(baseline_component_rows.key_type, '')), '')) in ('TS_DAY','TS_TOTAL')
        then 'SEGMENT_DELTA'
      when upper(nullif(btrim(coalesce(baseline_component_rows.key_type, '')), '')) = 'ADJUSTMENT_CODE'
        then 'ADJUSTMENT_DELTA'
      when upper(nullif(btrim(coalesce(baseline_component_rows.key_type, '')), '')) = 'EXPENSE_CODE'
       and upper(nullif(btrim(coalesce(baseline_component_rows.key_value, '')), '')) = 'MILEAGE'
        then 'MILEAGE_DELTA'
      when upper(nullif(btrim(coalesce(baseline_component_rows.key_type, '')), '')) in ('ADDITIONAL_CODE','EXPENSE_CODE')
        then 'EXPENSE_DELTA'
      else null::text
    end as resolver_item_type
  from legacy_baseline_timesheets as legacy_baseline_timesheet_rows
  join public.timesheet_pay_state as timesheet_pay_state_rows
    on timesheet_pay_state_rows.timesheet_id = legacy_baseline_timesheet_rows.source_timesheet_id
  join lateral public._pay_timesheet_components(coalesce(timesheet_pay_state_rows.last_settled_snapshot_json,'{}'::jsonb)) as baseline_component_rows
    on true
),
legacy_baseline_components as (
  select
    legacy_baseline_rows.timesheet_id,
    resolved_baseline_keys.key_type,
    resolved_baseline_keys.key_value,
    legacy_baseline_rows.amount_ex_vat,
    legacy_baseline_rows.amount_inc_vat
  from legacy_baseline_components_raw as legacy_baseline_rows
  join lateral public._pay_policy_x_resolve_pre_draft_economic_key(
    p_timesheet_id => legacy_baseline_rows.timesheet_id,
    p_live_source_json => jsonb_build_object(
      'timesheet_id', legacy_baseline_rows.timesheet_id::text,
      'item_type', legacy_baseline_rows.resolver_item_type,
      'component_key_type', legacy_baseline_rows.raw_key_type,
      'component_key_value', legacy_baseline_rows.raw_key_value,
      'work_date', case when legacy_baseline_rows.raw_key_type = 'TS_DAY' then legacy_baseline_rows.raw_key_value else null::text end
    ),
    p_item_type => legacy_baseline_rows.resolver_item_type,
    p_key_type_hint => legacy_baseline_rows.raw_key_type,
    p_key_value_hint => legacy_baseline_rows.raw_key_value,
    p_work_date => case
      when legacy_baseline_rows.raw_key_type = 'TS_DAY'
       and legacy_baseline_rows.raw_key_value ~ '^\d{4}-\d{2}-\d{2}$'
        then legacy_baseline_rows.raw_key_value::date
      else null::date
    end
  ) as resolved_baseline_keys
    on true
  where resolved_baseline_keys.key_resolution_failure_reason is null
),
baseline_components as (
  select
    active_settled_components.timesheet_id,
    active_settled_components.key_type,
    active_settled_components.key_value,
    active_settled_components.amount_ex_vat,
    active_settled_components.amount_inc_vat
  from active_settled_components
  union all
  select
    legacy_baseline_components.timesheet_id,
    legacy_baseline_components.key_type,
    legacy_baseline_components.key_value,
    legacy_baseline_components.amount_ex_vat,
    legacy_baseline_components.amount_inc_vat
  from legacy_baseline_components
),
truth_grouped as (
  select
    truth_components.timesheet_id,
    upper(nullif(btrim(coalesce(truth_components.key_type, '')), '')) as key_type,
    nullif(btrim(coalesce(truth_components.key_value, '')), '') as key_value,
    round(sum(coalesce(truth_components.amount_ex_vat, 0)), 2) as truth_ex_vat,
    round(sum(coalesce(truth_components.amount_inc_vat, 0)), 2) as truth_inc_vat
  from truth_components
  where truth_components.timesheet_id is not null
    and truth_components.key_type is not null
    and btrim(truth_components.key_type) <> ''
    and truth_components.key_value is not null
    and btrim(truth_components.key_value) <> ''
    and truth_components.key_type in ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE')
    and not (truth_components.key_type = 'TS_DAY' and truth_components.key_value !~ '^\d{4}-\d{2}-\d{2}$')
  group by
    truth_components.timesheet_id,
    upper(nullif(btrim(coalesce(truth_components.key_type, '')), '')),
    nullif(btrim(coalesce(truth_components.key_value, '')), '')
),
baseline_grouped as (
  select
    baseline_components.timesheet_id,
    upper(nullif(btrim(coalesce(baseline_components.key_type, '')), '')) as key_type,
    nullif(btrim(coalesce(baseline_components.key_value, '')), '') as key_value,
    round(sum(coalesce(baseline_components.amount_ex_vat, 0)), 2) as baseline_ex_vat,
    round(sum(coalesce(baseline_components.amount_inc_vat, 0)), 2) as baseline_inc_vat
  from baseline_components
  where baseline_components.timesheet_id is not null
    and baseline_components.key_type is not null
    and btrim(baseline_components.key_type) <> ''
    and baseline_components.key_value is not null
    and btrim(baseline_components.key_value) <> ''
    and baseline_components.key_type in ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE')
    and not (baseline_components.key_type = 'TS_DAY' and baseline_components.key_value !~ '^\d{4}-\d{2}-\d{2}$')
  group by
    baseline_components.timesheet_id,
    upper(nullif(btrim(coalesce(baseline_components.key_type, '')), '')),
    nullif(btrim(coalesce(baseline_components.key_value, '')), '')
),
all_keys as (
  select
    truth_grouped.timesheet_id,
    truth_grouped.key_type,
    truth_grouped.key_value
  from truth_grouped

  union

  select
    baseline_grouped.timesheet_id,
    baseline_grouped.key_type,
    baseline_grouped.key_value
  from baseline_grouped
)
select
  all_keys.timesheet_id,
  all_keys.key_type,
  all_keys.key_value,
  round(coalesce(truth_grouped.truth_ex_vat, 0), 2) as truth_ex_vat,
  round(coalesce(baseline_grouped.baseline_ex_vat, 0), 2) as baseline_ex_vat,
  round(coalesce(truth_grouped.truth_inc_vat, 0), 2) as truth_inc_vat,
  round(coalesce(baseline_grouped.baseline_inc_vat, 0), 2) as baseline_inc_vat
from all_keys
left join truth_grouped
  on truth_grouped.timesheet_id = all_keys.timesheet_id
 and truth_grouped.key_type = all_keys.key_type
 and truth_grouped.key_value = all_keys.key_value
left join baseline_grouped
  on baseline_grouped.timesheet_id = all_keys.timesheet_id
 and baseline_grouped.key_type = all_keys.key_type
 and baseline_grouped.key_value = all_keys.key_value
where all_keys.timesheet_id is not null
  and all_keys.key_type in ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE')
  and all_keys.key_value is not null
  and btrim(all_keys.key_value) <> ''
  and not (all_keys.key_type = 'TS_DAY' and all_keys.key_value !~ '^\d{4}-\d{2}-\d{2}$')
order by
  all_keys.timesheet_id,
  all_keys.key_type,
  all_keys.key_value;
$function$;

-- _pay_detect_manual_adjustments_for_carry_forward(uuid,jsonb,uuid)
CREATE OR REPLACE FUNCTION public._pay_detect_manual_adjustments_for_carry_forward(p_pay_batch_id uuid, p_scope_json jsonb, p_actor_user_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_scope_json jsonb := COALESCE(p_scope_json, '{}'::jsonb);
  v_uuid_regex text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';
  v_pay_batch_item_ids uuid[] := ARRAY[]::uuid[];
  v_manual_adjustments_to_carry_forward jsonb := '[]'::jsonb;
  v_carry_forward_blockers jsonb := '[]'::jsonb;
  v_manual_adjustment_support_details_json jsonb := '{}'::jsonb;
  v_finance_backed_count integer := 0;
  v_source_backed_count integer := 0;
  v_source_less_safe_count integer := 0;
  v_source_less_ambiguous_count integer := 0;
  v_manual_count integer := 0;
BEGIN
  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'PAY_BATCH_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_BATCH_ID_REQUIRED')::text;
  END IF;

  IF v_scope_json IS NULL OR jsonb_typeof(v_scope_json) <> 'object' THEN
    RAISE EXCEPTION 'PAY_SCOPE_JSON_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_SCOPE_JSON_MUST_BE_OBJECT', 'pay_batch_id', p_pay_batch_id)::text;
  END IF;

  WITH raw_values AS (
    SELECT item_values.raw_value
    FROM jsonb_array_elements_text(
      CASE
        WHEN jsonb_typeof(v_scope_json->'pay_batch_item_ids') = 'array' THEN v_scope_json->'pay_batch_item_ids'
        WHEN jsonb_typeof(v_scope_json #> '{resolved_full_payment_scope_json,pay_batch_item_ids}') = 'array' THEN v_scope_json #> '{resolved_full_payment_scope_json,pay_batch_item_ids}'
        ELSE '[]'::jsonb
      END
    ) AS item_values(raw_value)
  ), clean_values AS (
    SELECT NULLIF(btrim(raw_values.raw_value), '') AS clean_value
    FROM raw_values
  )
  SELECT COALESCE(array_agg(clean_values.clean_value::uuid), ARRAY[]::uuid[])
  INTO v_pay_batch_item_ids
  FROM clean_values
  WHERE clean_values.clean_value IS NOT NULL
    AND clean_values.clean_value ~ v_uuid_regex;

  IF COALESCE(array_length(v_pay_batch_item_ids, 1), 0) = 0 THEN
    RETURN jsonb_build_object(
      'manual_adjustment_carry_forward_required', false,
      'manual_adjustments_to_carry_forward', '[]'::jsonb,
      'can_carry_forward_automatically', true,
      'carry_forward_blockers', '[]'::jsonb,
      'manual_adjustment_support_details_json', jsonb_build_object('manual_item_count', 0, 'reason', 'No pay_batch_item_ids were supplied in the resolved scope.'),
      'finance_backed_count', 0,
      'source_backed_count', 0,
      'source_less_safe_count', 0,
      'source_less_ambiguous_count', 0
    );
  END IF;

  WITH scoped_items AS (
    SELECT
      item_rows.id AS pay_batch_item_id,
      item_rows.item_type,
      item_rows.timesheet_id,
      item_rows.source_ref,
      item_rows.description,
      item_rows.amount_ex_vat,
      item_rows.amount_vat,
      item_rows.amount_inc_vat,
      item_rows.pay_channel,
      item_rows.umbrella_id,
      item_rows.finance_case_id,
      item_rows.finance_component_id,
      item_rows.reservation_id,
      item_rows.paye_treatment,
      item_rows.frozen_component_snapshot_json,
      item_rows.frozen_component_key_type,
      item_rows.frozen_component_key_value,
      item_rows.frozen_component_classification::text AS frozen_component_classification,
      item_rows.frozen_source_basis_json,
      item_rows.frozen_source_pay_method,
      item_rows.frozen_target_pay_method,
      item_rows.frozen_resolution_mode::text AS frozen_resolution_mode,
      item_rows.frozen_resolution_payload_json,
      item_rows.frozen_resolution_result_json,
      item_rows.operation_source_key,
      item_rows.pay_bank_transfer_id,
      batch_candidate_rows.pay_batch_id,
      batch_candidate_rows.id AS pay_batch_candidate_id,
      batch_candidate_rows.candidate_id,
      COALESCE(
        item_rows.umbrella_id,
        transfer_rows.umbrella_id,
        CASE
          WHEN upper(btrim(COALESCE(item_rows.pay_channel, ''))) = 'UMBRELLA' THEN candidate_record_rows.umbrella_id
          ELSE NULL::uuid
        END,
        CASE
          WHEN upper(COALESCE(transfer_rows.payee_entity_kind, '')) IN ('UMBRELLA', 'UMBRELLA_COMPANY') THEN transfer_rows.payee_entity_id
          ELSE NULL::uuid
        END
      ) AS effective_umbrella_id,
      transfer_rows.transfer_group_key,
      transfer_rows.payee_entity_kind,
      transfer_rows.payee_entity_id,
      existing_target_carry_forward_rows.id AS existing_target_carry_forward_id,
      existing_source_carry_forward_rows.id AS existing_source_carry_forward_id,
      (
        upper(COALESCE(item_rows.item_type, '')) LIKE '%MANUAL%'
        OR upper(COALESCE(item_rows.item_type, '')) LIKE '%ADJUSTMENT%'
        OR upper(COALESCE(item_rows.item_type, '')) LIKE '%DEBT%'
        OR upper(COALESCE(item_rows.item_type, '')) LIKE '%CREDIT%'
        OR upper(COALESCE(item_rows.item_type, '')) IN ('ADJUSTMENT_DELTA', 'MANUAL_CREDIT_ADJUSTMENT_PAYMENT', 'MANUAL_CREDIT_PAYOUT', 'MANUAL_DEBT_RECOVERY', 'FINANCE_ADJUSTMENT')
        OR upper(COALESCE(item_rows.source_ref, '')) LIKE 'MANUAL%'
        OR upper(COALESCE(item_rows.operation_source_key, '')) LIKE 'MANUAL%'
      ) AS is_manual_like
    FROM public.pay_batch_items AS item_rows
    JOIN public.pay_batch_candidates AS batch_candidate_rows
      ON batch_candidate_rows.id = item_rows.pay_batch_candidate_id
    JOIN public.candidates AS candidate_record_rows
      ON candidate_record_rows.id = batch_candidate_rows.candidate_id
    LEFT JOIN public.pay_bank_transfers AS transfer_rows
      ON transfer_rows.id = item_rows.pay_bank_transfer_id
    LEFT JOIN public.pay_manual_adjustment_carry_forwards AS existing_target_carry_forward_rows
      ON existing_target_carry_forward_rows.target_pay_batch_item_id = item_rows.id
    LEFT JOIN public.pay_manual_adjustment_carry_forwards AS existing_source_carry_forward_rows
      ON existing_source_carry_forward_rows.source_pay_batch_item_id = item_rows.id
    WHERE batch_candidate_rows.pay_batch_id = p_pay_batch_id
      AND item_rows.id = ANY(COALESCE(v_pay_batch_item_ids, ARRAY[]::uuid[]))
  ), classified_items AS (
    SELECT
      scoped_items.*,
      CASE
        WHEN scoped_items.is_manual_like = false THEN 'NOT_MANUAL'
        WHEN scoped_items.existing_target_carry_forward_id IS NOT NULL
          OR upper(COALESCE(scoped_items.source_ref, '')) LIKE 'CARRY_FORWARD:%'
          OR upper(COALESCE(scoped_items.operation_source_key, '')) LIKE 'CARRY_FORWARD:%'
          THEN 'SOURCE_BACKED'
        WHEN scoped_items.finance_case_id IS NOT NULL
          OR scoped_items.finance_component_id IS NOT NULL
          OR scoped_items.reservation_id IS NOT NULL
          THEN 'FINANCE_BACKED'
        WHEN scoped_items.timesheet_id IS NOT NULL
          OR upper(COALESCE(scoped_items.operation_source_key, '')) LIKE 'TIMESHEET:%'
          OR upper(COALESCE(scoped_items.operation_source_key, '')) LIKE 'TS:%'
          OR upper(COALESCE(scoped_items.operation_source_key, '')) LIKE 'SEG:%'
          OR upper(COALESCE(scoped_items.operation_source_key, '')) LIKE 'ADJ:%'
          OR upper(COALESCE(scoped_items.operation_source_key, '')) LIKE 'EXPENSE:%'
          OR upper(COALESCE(scoped_items.operation_source_key, '')) LIKE 'MILEAGE:%'
          OR upper(COALESCE(scoped_items.operation_source_key, '')) LIKE 'ADDITIONAL%'
          OR upper(COALESCE(scoped_items.operation_source_key, '')) LIKE 'FINANCE:%'
          OR upper(COALESCE(scoped_items.operation_source_key, '')) LIKE 'FINANCE_CASE:%'
          OR upper(COALESCE(scoped_items.operation_source_key, '')) LIKE 'RESERVATION:%'
          OR upper(COALESCE(scoped_items.operation_source_key, '')) LIKE 'ADVANCE:%'
          OR upper(COALESCE(scoped_items.source_ref, '')) LIKE 'TS:%'
          OR upper(COALESCE(scoped_items.source_ref, '')) LIKE 'SEG:%'
          OR upper(COALESCE(scoped_items.source_ref, '')) LIKE 'ADJ:%'
          OR upper(COALESCE(scoped_items.source_ref, '')) LIKE 'EXPENSE:%'
          OR upper(COALESCE(scoped_items.source_ref, '')) LIKE 'MILEAGE:%'
          OR upper(COALESCE(scoped_items.source_ref, '')) LIKE 'ADDITIONAL%'
          THEN 'SOURCE_BACKED'
        WHEN scoped_items.amount_inc_vat IS NOT NULL
          AND round(scoped_items.amount_inc_vat, 2) <> 0
          AND scoped_items.amount_ex_vat IS NOT NULL
          AND scoped_items.amount_vat IS NOT NULL
          AND NULLIF(btrim(COALESCE(scoped_items.description, '')), '') IS NOT NULL
          AND scoped_items.candidate_id IS NOT NULL
          AND upper(btrim(COALESCE(scoped_items.pay_channel, ''))) IN ('PAYE', 'UMBRELLA')
          AND (
            upper(btrim(COALESCE(scoped_items.pay_channel, ''))) = 'PAYE'
            OR scoped_items.effective_umbrella_id IS NOT NULL
          )
          THEN 'SOURCE_LESS_CARRY_FORWARD_SAFE'
        ELSE 'SOURCE_LESS_AMBIGUOUS'
      END AS classification,
      CASE
        WHEN scoped_items.amount_inc_vat IS NULL THEN 'MISSING_AMOUNT_INC_VAT'
        WHEN round(scoped_items.amount_inc_vat, 2) = 0 THEN 'ZERO_AMOUNT'
        WHEN scoped_items.amount_ex_vat IS NULL THEN 'MISSING_AMOUNT_EX_VAT'
        WHEN scoped_items.amount_vat IS NULL THEN 'MISSING_AMOUNT_VAT'
        WHEN NULLIF(btrim(COALESCE(scoped_items.description, '')), '') IS NULL THEN 'MISSING_DESCRIPTION'
        WHEN scoped_items.candidate_id IS NULL THEN 'MISSING_CANDIDATE_CONTEXT'
        WHEN upper(btrim(COALESCE(scoped_items.pay_channel, ''))) NOT IN ('PAYE', 'UMBRELLA') THEN 'UNSUPPORTED_OR_MISSING_PAY_CHANNEL'
        WHEN upper(btrim(COALESCE(scoped_items.pay_channel, ''))) = 'UMBRELLA' AND scoped_items.effective_umbrella_id IS NULL THEN 'MISSING_UMBRELLA_PAYEE_CONTEXT'
        ELSE NULL::text
      END AS ambiguity_reason
    FROM scoped_items
  )
  SELECT
    COALESCE((count(*) FILTER (WHERE classified_items.is_manual_like))::integer, 0),
    COALESCE((count(*) FILTER (WHERE classified_items.classification = 'FINANCE_BACKED'))::integer, 0),
    COALESCE((count(*) FILTER (WHERE classified_items.classification = 'SOURCE_BACKED'))::integer, 0),
    COALESCE((count(*) FILTER (WHERE classified_items.classification = 'SOURCE_LESS_CARRY_FORWARD_SAFE'))::integer, 0),
    COALESCE((count(*) FILTER (WHERE classified_items.classification = 'SOURCE_LESS_AMBIGUOUS'))::integer, 0),
    COALESCE(jsonb_agg(jsonb_build_object(
      'pay_batch_item_id', classified_items.pay_batch_item_id::text,
      'pay_batch_id', classified_items.pay_batch_id::text,
      'pay_batch_candidate_id', classified_items.pay_batch_candidate_id::text,
      'candidate_id', classified_items.candidate_id::text,
      'umbrella_id', CASE WHEN classified_items.effective_umbrella_id IS NULL THEN NULL ELSE classified_items.effective_umbrella_id::text END,
      'pay_bank_transfer_id', CASE WHEN classified_items.pay_bank_transfer_id IS NULL THEN NULL ELSE classified_items.pay_bank_transfer_id::text END,
      'item_type', classified_items.item_type,
      'description', classified_items.description,
      'pay_channel', classified_items.pay_channel,
      'amount_ex_vat', classified_items.amount_ex_vat,
      'amount_vat', classified_items.amount_vat,
      'amount_inc_vat', classified_items.amount_inc_vat,
      'adjustment_direction', CASE WHEN classified_items.amount_inc_vat > 0 THEN 'CREDIT' ELSE 'DEBIT' END,
      'signed_amount_convention', 'SIGNED_AMOUNTS',
      'paye_treatment', classified_items.paye_treatment,
      'source_ref', classified_items.source_ref,
      'operation_source_key', classified_items.operation_source_key,
      'classification', classified_items.classification
    ) ORDER BY classified_items.pay_batch_item_id::text) FILTER (WHERE classified_items.classification = 'SOURCE_LESS_CARRY_FORWARD_SAFE'), '[]'::jsonb),
    COALESCE(jsonb_agg(jsonb_build_object(
      'code', 'SOURCE_LESS_MANUAL_ADJUSTMENT_AMBIGUOUS',
      'pay_batch_item_id', classified_items.pay_batch_item_id::text,
      'item_type', classified_items.item_type,
      'description', classified_items.description,
      'amount_inc_vat', classified_items.amount_inc_vat,
      'pay_channel', classified_items.pay_channel,
      'reason', classified_items.ambiguity_reason
    ) ORDER BY classified_items.pay_batch_item_id::text) FILTER (WHERE classified_items.classification = 'SOURCE_LESS_AMBIGUOUS'), '[]'::jsonb),
    COALESCE(jsonb_agg(jsonb_build_object(
      'pay_batch_item_id', classified_items.pay_batch_item_id::text,
      'item_type', classified_items.item_type,
      'classification', classified_items.classification,
      'is_manual_like', classified_items.is_manual_like,
      'effective_umbrella_id', CASE WHEN classified_items.effective_umbrella_id IS NULL THEN NULL ELSE classified_items.effective_umbrella_id::text END,
      'payee_entity_kind', classified_items.payee_entity_kind,
      'payee_entity_id', CASE WHEN classified_items.payee_entity_id IS NULL THEN NULL ELSE classified_items.payee_entity_id::text END,
      'finance_case_id', CASE WHEN classified_items.finance_case_id IS NULL THEN NULL ELSE classified_items.finance_case_id::text END,
      'finance_component_id', CASE WHEN classified_items.finance_component_id IS NULL THEN NULL ELSE classified_items.finance_component_id::text END,
      'reservation_id', CASE WHEN classified_items.reservation_id IS NULL THEN NULL ELSE classified_items.reservation_id::text END,
      'timesheet_id', CASE WHEN classified_items.timesheet_id IS NULL THEN NULL ELSE classified_items.timesheet_id::text END,
      'source_ref', classified_items.source_ref,
      'operation_source_key', classified_items.operation_source_key,
      'existing_target_carry_forward_id', CASE WHEN classified_items.existing_target_carry_forward_id IS NULL THEN NULL ELSE classified_items.existing_target_carry_forward_id::text END,
      'existing_source_carry_forward_id', CASE WHEN classified_items.existing_source_carry_forward_id IS NULL THEN NULL ELSE classified_items.existing_source_carry_forward_id::text END,
      'ambiguity_reason', classified_items.ambiguity_reason
    ) ORDER BY classified_items.pay_batch_item_id::text) FILTER (WHERE classified_items.is_manual_like), '[]'::jsonb)
  INTO
    v_manual_count,
    v_finance_backed_count,
    v_source_backed_count,
    v_source_less_safe_count,
    v_source_less_ambiguous_count,
    v_manual_adjustments_to_carry_forward,
    v_carry_forward_blockers,
    v_manual_adjustment_support_details_json
  FROM classified_items;

  RETURN jsonb_build_object(
    'manual_adjustment_carry_forward_required', v_source_less_safe_count > 0,
    'manual_adjustments_to_carry_forward', COALESCE(v_manual_adjustments_to_carry_forward, '[]'::jsonb),
    'can_carry_forward_automatically', v_source_less_ambiguous_count = 0,
    'carry_forward_blockers', COALESCE(v_carry_forward_blockers, '[]'::jsonb),
    'manual_adjustment_support_details_json', jsonb_build_object(
      'manual_item_count', v_manual_count,
      'classified_items', COALESCE(v_manual_adjustment_support_details_json, '[]'::jsonb),
      'signed_amount_convention', 'SIGNED_AMOUNTS',
      'adjustment_direction_is_display_only', true,
      'actor_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END
    ),
    'finance_backed_count', v_finance_backed_count,
    'source_backed_count', v_source_backed_count,
    'source_less_safe_count', v_source_less_safe_count,
    'source_less_ambiguous_count', v_source_less_ambiguous_count
  );
END;
$function$;

-- _pay_execute_operation_cleanup_failed_local_artifacts_base(uuid,uuid,text,jsonb,boolean)
CREATE OR REPLACE FUNCTION public._pay_execute_operation_cleanup_failed_local_artifacts_base(p_operation_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid, p_failure_phase text DEFAULT NULL::text, p_failure_error_json jsonb DEFAULT '{}'::jsonb, p_dry_run boolean DEFAULT false)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_operation_row public.banking_pay_operations%ROWTYPE;
  v_batch_row public.pay_batches%ROWTYPE;
  v_operation_type_upper text := NULL::text;
  v_failure_phase text := NULL::text;
  v_failure_error_json jsonb := '{}'::jsonb;
  v_effective_actor_user_id uuid := NULL::uuid;
  v_batch_execution_boundary_crossed boolean := false;
  v_chunk_id uuid := NULL::uuid;
  v_provider_state_risk_count integer := 0;
  v_provider_chunk_risk_count integer := 0;
  v_transfer_event_risk_count integer := 0;
  v_scope_rows_considered integer := 0;
  v_transfer_rows_considered integer := 0;
  v_scope_rows_deleted integer := 0;
  v_transfer_rows_deleted integer := 0;
  v_item_links_cleared integer := 0;
  v_bank_references_cleared integer := 0;
  v_chunks_marked_failed integer := 0;
  v_chunks_marked_skipped integer := 0;
  v_locks_released integer := 0;
  v_review_required boolean := false;
  v_safe_to_retry boolean := false;
  v_retry_blocked boolean := true;
  v_retry_blocked_reason text := NULL::text;
  v_cleanup_mode text := 'REVIEW_REQUIRED';
  v_deleted_scope_ids jsonb := '[]'::jsonb;
  v_deleted_transfer_ids jsonb := '[]'::jsonb;
  v_safe_scope_ids jsonb := '[]'::jsonb;
  v_safe_transfer_ids jsonb := '[]'::jsonb;
  v_unsafe_reasons jsonb := '[]'::jsonb;
  v_active_auth_request_count integer := 0;
  v_operation_active_auth_request_count integer := 0;
  v_auth_requests_cancelled integer := 0;
  v_auth_tokens_voided integer := 0;
  v_batch_execution_intent_cleared integer := 0;
  v_result jsonb := '{}'::jsonb;
BEGIN
  PERFORM set_config('lock_timeout', '5s', true);
  PERFORM public.banking_pay_hot_path_budget_apply('WORKER_CHUNK');

  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_EXECUTE_OPERATION_CLEANUP_FAILED_LOCAL_ARTIFACTS',
      'code', 'OPERATION_ID_REQUIRED',
      'message', 'pay_execute_operation_cleanup_failed_local_artifacts: operation_id is required'
    )::text USING ERRCODE = 'P0001';
  END IF;

  IF p_failure_error_json IS NOT NULL AND jsonb_typeof(p_failure_error_json) = 'object' THEN
    v_failure_error_json := p_failure_error_json;
  ELSE
    v_failure_error_json := '{}'::jsonb;
  END IF;

  v_failure_phase := NULLIF(BTRIM(COALESCE(p_failure_phase, v_failure_error_json->>'phase', v_failure_error_json->>'failure_phase', '')), '');

  IF COALESCE(v_failure_error_json->>'chunk_id', v_failure_error_json->>'provider_chunk_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_chunk_id := COALESCE(v_failure_error_json->>'chunk_id', v_failure_error_json->>'provider_chunk_id')::uuid;
  END IF;

  SELECT operation_row.*
  INTO v_operation_row
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF v_operation_row.id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_EXECUTE_OPERATION_CLEANUP_FAILED_LOCAL_ARTIFACTS',
      'code', 'OPERATION_NOT_FOUND',
      'message', 'pay_execute_operation_cleanup_failed_local_artifacts: operation not found',
      'operation_id', p_operation_id::text
    )::text USING ERRCODE = 'P0001';
  END IF;

  v_effective_actor_user_id := COALESCE(p_actor_user_id, v_operation_row.actor_user_id);
  IF v_failure_phase IS NULL THEN
    v_failure_phase := NULLIF(BTRIM(COALESCE(v_operation_row.phase, '')), '');
  END IF;

  v_operation_type_upper := upper(BTRIM(COALESCE(v_operation_row.operation_type, '')));
  IF v_operation_type_upper NOT IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS') THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_EXECUTE_OPERATION_CLEANUP_FAILED_LOCAL_ARTIFACTS',
      'code', 'OPERATION_TYPE_NOT_SUPPORTED',
      'message', 'Only PAYMENT_EXECUTE and PAYMENT_RETRY_BLOCKED_FUNDS operations are supported.',
      'operation_id', p_operation_id::text,
      'operation_type', v_operation_row.operation_type
    )::text USING ERRCODE = 'P0001';
  END IF;

  IF upper(BTRIM(COALESCE(v_operation_row.status, ''))) = 'COMPLETE' THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_EXECUTE_OPERATION_CLEANUP_FAILED_LOCAL_ARTIFACTS',
      'code', 'COMPLETE_OPERATION_CANNOT_BE_CLEANED',
      'operation_id', p_operation_id::text
    )::text USING ERRCODE = 'P0001';
  END IF;

  IF v_operation_row.pay_batch_id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_EXECUTE_OPERATION_CLEANUP_FAILED_LOCAL_ARTIFACTS',
      'code', 'PAY_BATCH_ID_REQUIRED',
      'operation_id', p_operation_id::text
    )::text USING ERRCODE = 'P0001';
  END IF;

  SELECT batch_row.*
  INTO v_batch_row
  FROM public.pay_batches AS batch_row
  WHERE batch_row.id = v_operation_row.pay_batch_id
  FOR UPDATE;

  IF v_batch_row.id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_EXECUTE_OPERATION_CLEANUP_FAILED_LOCAL_ARTIFACTS',
      'code', 'PAY_BATCH_NOT_FOUND',
      'operation_id', p_operation_id::text,
      'pay_batch_id', v_operation_row.pay_batch_id::text
    )::text USING ERRCODE = 'P0001';
  END IF;

  v_batch_execution_boundary_crossed := (
    upper(BTRIM(COALESCE(v_batch_row.execution_commit_state, 'NOT_SUBMITTED'))) <> 'NOT_SUBMITTED'
    OR NULLIF(BTRIM(COALESCE(v_batch_row.execution_commit_ref, '')), '') IS NOT NULL
    OR v_batch_row.execution_committed_at_utc IS NOT NULL
  );

  SELECT COUNT(*)::integer
  INTO v_scope_rows_considered
  FROM public.banking_pay_operation_transfer_scope AS scope_row
  WHERE scope_row.operation_id = p_operation_id
    AND scope_row.pay_batch_id = v_operation_row.pay_batch_id
    AND (v_chunk_id IS NULL OR scope_row.provider_submit_chunk_id = v_chunk_id);

  SELECT COUNT(DISTINCT scope_row.pay_bank_transfer_id)::integer
  INTO v_transfer_rows_considered
  FROM public.banking_pay_operation_transfer_scope AS scope_row
  WHERE scope_row.operation_id = p_operation_id
    AND scope_row.pay_batch_id = v_operation_row.pay_batch_id
    AND scope_row.pay_bank_transfer_id IS NOT NULL
    AND (v_chunk_id IS NULL OR scope_row.provider_submit_chunk_id = v_chunk_id);

  SELECT COUNT(*)::integer
  INTO v_provider_state_risk_count
  FROM public.banking_pay_operation_transfer_scope AS scope_row
  WHERE scope_row.operation_id = p_operation_id
    AND scope_row.pay_batch_id = v_operation_row.pay_batch_id
    AND (v_chunk_id IS NULL OR scope_row.provider_submit_chunk_id = v_chunk_id)
    AND (
      upper(BTRIM(COALESCE(scope_row.provider_submit_state, ''))) IN (
        'REQUEST_SENDING',
        'REQUEST_SENT_LOCAL',
        'PROVIDER_ACCEPTED',
        'PROVIDER_REJECTED',
        'PROVIDER_UNKNOWN',
        'CHUNK_FINALISED'
      )
      OR (
        upper(BTRIM(COALESCE(scope_row.provider_submit_state, ''))) = 'REVIEW_REQUIRED'
        AND NOT (
          upper(BTRIM(COALESCE(scope_row.provider_unsafe_reason, ''))) IN (
            'TRANSFER_GROUP_PAYOUT_INSTRUCTION_INVALID',
            'PAYE_NET_REQUIRED_FOR_EXECUTION',
            'TRANSFER_SCOPE_NON_POSITIVE_AMOUNT',
            'TRANSFER_SCOPE_ITEM_SOURCE_EMPTY',
            'TRANSFER_SCOPE_ITEM_SEED_ERROR',
            'TRANSFER_SCOPE_ITEM_SEED_INCOMPLETE',
            'TRANSFER_SCOPE_ITEM_ROLLUP_PENDING'
          )
          AND scope_row.pay_bank_transfer_id IS NULL
          AND scope_row.provider_submit_chunk_id IS NULL
          AND COALESCE(scope_row.provider_submit_attempt_count, 0) = 0
          AND NULLIF(BTRIM(COALESCE(scope_row.provider_idempotency_key, '')), '') IS NULL
          AND NULLIF(BTRIM(COALESCE(scope_row.provider_request_id, '')), '') IS NULL
          AND NULLIF(BTRIM(COALESCE(scope_row.provider_transaction_id, '')), '') IS NULL
          AND scope_row.provider_request_prepared_at_utc IS NULL
          AND scope_row.provider_request_sending_at_utc IS NULL
          AND scope_row.provider_request_sent_at_utc IS NULL
          AND scope_row.provider_response_at_utc IS NULL
          AND NULLIF(BTRIM(COALESCE(scope_row.provider_submission_status, '')), '') IS NULL
          AND NOT EXISTS (
            SELECT 1
            FROM public.banking_pay_operation_provider_attempts AS provider_attempt
            WHERE provider_attempt.operation_id = scope_row.operation_id
              AND provider_attempt.transfer_scope_id = scope_row.id
          )
        )
      )
    );

  SELECT COUNT(*)::integer
  INTO v_provider_chunk_risk_count
  FROM public.banking_pay_operation_chunks AS chunk_row
  WHERE chunk_row.operation_id = p_operation_id
    AND (v_chunk_id IS NULL OR chunk_row.id = v_chunk_id)
    AND (chunk_row.phase = 'SUBMIT_PROVIDER_TRANSFERS' OR chunk_row.chunk_type = 'TRANSFER_SUBMIT')
    AND (
      upper(BTRIM(COALESCE(chunk_row.status, ''))) IN ('RUNNING', 'COMPLETE', 'FAILED')
      OR chunk_row.started_at_utc IS NOT NULL
      OR COALESCE(chunk_row.result_json, '{}'::jsonb) <> '{}'::jsonb
      OR COALESCE(chunk_row.error_json, '{}'::jsonb) <> '{}'::jsonb
    );

  SELECT COUNT(*)::integer
  INTO v_transfer_event_risk_count
  FROM public.pay_bank_transfer_events AS event_row
  WHERE event_row.pay_batch_id = v_operation_row.pay_batch_id
    AND EXISTS (
      SELECT 1
      FROM public.banking_pay_operation_transfer_scope AS scope_row
      WHERE scope_row.operation_id = p_operation_id
        AND scope_row.pay_batch_id = v_operation_row.pay_batch_id
        AND (v_chunk_id IS NULL OR scope_row.provider_submit_chunk_id = v_chunk_id)
        AND scope_row.pay_bank_transfer_id = event_row.pay_bank_transfer_id
    );

  SELECT COUNT(*)::integer,
         COUNT(*) FILTER (WHERE auth_request.execution_intent_json->>'operation_id' = p_operation_id::text)::integer
  INTO v_active_auth_request_count,
       v_operation_active_auth_request_count
  FROM public.pay_batch_auth_requests AS auth_request
  WHERE auth_request.pay_batch_id = v_operation_row.pay_batch_id
    AND auth_request.state IN ('AWAITING', 'PENDING_AUTHORISATION', 'AUTHORISED');

  v_review_required := v_batch_execution_boundary_crossed
    OR COALESCE(v_provider_state_risk_count, 0) > 0
    OR COALESCE(v_provider_chunk_risk_count, 0) > 0
    OR COALESCE(v_transfer_event_risk_count, 0) > 0
    OR COALESCE(v_active_auth_request_count, 0) > COALESCE(v_operation_active_auth_request_count, 0);

  IF v_review_required THEN
    v_retry_blocked := true;
    v_safe_to_retry := false;
    v_retry_blocked_reason := CASE
      WHEN v_batch_execution_boundary_crossed THEN 'BATCH_EXECUTION_BOUNDARY_CROSSED'
      WHEN COALESCE(v_provider_state_risk_count, 0) > 0 THEN 'PROVIDER_STATE_REQUIRES_RECONCILIATION'
      WHEN COALESCE(v_provider_chunk_risk_count, 0) > 0 THEN 'PROVIDER_SUBMIT_CHUNK_REQUIRES_RECONCILIATION'
      WHEN COALESCE(v_transfer_event_risk_count, 0) > 0 THEN 'TRANSFER_EVENT_PRESENT'
      ELSE 'ACTIVE_AUTH_REQUEST_PRESENT'
    END;
    v_cleanup_mode := 'REVIEW_REQUIRED_' || v_retry_blocked_reason;
    v_unsafe_reasons := jsonb_build_array(jsonb_strip_nulls(jsonb_build_object(
      'reason', v_retry_blocked_reason,
      'provider_state_risk_count', COALESCE(v_provider_state_risk_count, 0),
      'provider_chunk_risk_count', COALESCE(v_provider_chunk_risk_count, 0),
      'transfer_event_risk_count', COALESCE(v_transfer_event_risk_count, 0),
      'active_auth_request_count', COALESCE(v_active_auth_request_count, 0)
    )));
  ELSE
    v_retry_blocked := false;
    v_safe_to_retry := true;
    v_cleanup_mode := CASE WHEN COALESCE(p_dry_run, false) THEN 'DRY_RUN_BOUNDED_LOCAL_CLEANUP' ELSE 'CLEANED_BOUNDED_LOCAL_ARTIFACTS' END;
  END IF;

  DROP TABLE IF EXISTS pg_temp.tmp_cleanup_scope_ids;
  CREATE TEMPORARY TABLE pg_temp.tmp_cleanup_scope_ids AS
  SELECT scope_row.id AS scope_id,
         scope_row.pay_bank_transfer_id
  FROM public.banking_pay_operation_transfer_scope AS scope_row
  WHERE scope_row.operation_id = p_operation_id
    AND scope_row.pay_batch_id = v_operation_row.pay_batch_id
    AND (v_chunk_id IS NULL OR scope_row.provider_submit_chunk_id = v_chunk_id)
    AND COALESCE(v_review_required, false) IS FALSE
  ORDER BY scope_row.id
  LIMIT 100;

  SELECT COALESCE(jsonb_agg(to_jsonb(cleanup_scope.scope_id::text) ORDER BY cleanup_scope.scope_id), '[]'::jsonb)
  INTO v_safe_scope_ids
  FROM pg_temp.tmp_cleanup_scope_ids AS cleanup_scope;

  SELECT COALESCE(jsonb_agg(DISTINCT to_jsonb(cleanup_scope.pay_bank_transfer_id::text)), '[]'::jsonb)
  INTO v_safe_transfer_ids
  FROM pg_temp.tmp_cleanup_scope_ids AS cleanup_scope
  WHERE cleanup_scope.pay_bank_transfer_id IS NOT NULL;

  IF COALESCE(p_dry_run, false) IS FALSE AND COALESCE(v_review_required, false) IS FALSE THEN
    WITH same_operation_auth_requests AS (
      SELECT auth_request.id
      FROM public.pay_batch_auth_requests AS auth_request
      WHERE auth_request.pay_batch_id = v_operation_row.pay_batch_id
        AND auth_request.state IN ('AWAITING', 'AUTHORISED')
        AND auth_request.execution_intent_json->>'operation_id' = p_operation_id::text
    ), cancelled_auth_requests AS (
      UPDATE public.pay_batch_auth_requests AS auth_request_update
      SET state = 'CANCELLED',
          finalised_at_utc = COALESCE(auth_request_update.finalised_at_utc, v_now),
          finalised_by_user_id = COALESCE(auth_request_update.finalised_by_user_id, v_effective_actor_user_id),
          execution_intent_json = jsonb_strip_nulls(COALESCE(auth_request_update.execution_intent_json, '{}'::jsonb) || jsonb_build_object(
            'cancelled_by_bounded_execution_cleanup', true,
            'cleanup_operation_id', p_operation_id::text,
            'cleanup_at_utc', v_now::text,
            'failure_phase', v_failure_phase
          ))
      FROM same_operation_auth_requests
      WHERE auth_request_update.id = same_operation_auth_requests.id
      RETURNING auth_request_update.id
    )
    SELECT COUNT(*)::integer
    INTO v_auth_requests_cancelled
    FROM cancelled_auth_requests;

    WITH voided_tokens AS (
      UPDATE public.pay_batch_auth_tokens AS auth_token_update
      SET used_at_utc = COALESCE(auth_token_update.used_at_utc, v_now),
          expires_at_utc = CASE WHEN auth_token_update.expires_at_utc > v_now THEN v_now ELSE auth_token_update.expires_at_utc END
      WHERE auth_token_update.auth_request_id IN (
        SELECT auth_request.id
        FROM public.pay_batch_auth_requests AS auth_request
        WHERE auth_request.pay_batch_id = v_operation_row.pay_batch_id
          AND auth_request.execution_intent_json->>'operation_id' = p_operation_id::text
          AND auth_request.state = 'CANCELLED'
      )
        AND (auth_token_update.used_at_utc IS NULL OR auth_token_update.expires_at_utc > v_now)
      RETURNING auth_token_update.token
    )
    SELECT COUNT(*)::integer
    INTO v_auth_tokens_voided
    FROM voided_tokens;

    WITH item_rows_to_clear AS (
      SELECT item_row.id AS pay_batch_item_id,
             item_row.bank_reference AS previous_bank_reference,
             transfer_row.payment_reference AS transfer_payment_reference
      FROM public.pay_batch_items AS item_row
      JOIN public.pay_batch_candidates AS batch_candidate
        ON batch_candidate.id = item_row.pay_batch_candidate_id
       AND batch_candidate.pay_batch_id = v_operation_row.pay_batch_id
      JOIN pg_temp.tmp_cleanup_scope_ids AS cleanup_scope
        ON cleanup_scope.pay_bank_transfer_id = item_row.pay_bank_transfer_id
      JOIN public.pay_bank_transfers AS transfer_row
        ON transfer_row.id = cleanup_scope.pay_bank_transfer_id
    ), cleared_item_links AS (
      UPDATE public.pay_batch_items AS item_update
      SET pay_bank_transfer_id = NULL,
          bank_reference = CASE WHEN item_update.bank_reference = item_rows_to_clear.transfer_payment_reference THEN NULL ELSE item_update.bank_reference END,
          updated_at = v_now
      FROM item_rows_to_clear
      WHERE item_update.id = item_rows_to_clear.pay_batch_item_id
      RETURNING item_update.id,
                item_rows_to_clear.previous_bank_reference,
                item_rows_to_clear.transfer_payment_reference
    )
    SELECT COUNT(*)::integer,
           COALESCE((COUNT(*) FILTER (
             WHERE cleared_item_links.previous_bank_reference = cleared_item_links.transfer_payment_reference
               AND NULLIF(BTRIM(COALESCE(cleared_item_links.previous_bank_reference, '')), '') IS NOT NULL
           )), 0)::integer
    INTO v_item_links_cleared,
         v_bank_references_cleared
    FROM cleared_item_links;

    WITH deleted_scope_rows AS (
      DELETE FROM public.banking_pay_operation_transfer_scope AS scope_delete
      USING pg_temp.tmp_cleanup_scope_ids AS cleanup_scope
      WHERE scope_delete.id = cleanup_scope.scope_id
        AND scope_delete.operation_id = p_operation_id
        AND scope_delete.pay_batch_id = v_operation_row.pay_batch_id
      RETURNING scope_delete.id
    )
    SELECT COUNT(*)::integer,
           COALESCE(jsonb_agg(to_jsonb(deleted_scope_rows.id::text) ORDER BY deleted_scope_rows.id), '[]'::jsonb)
    INTO v_scope_rows_deleted,
         v_deleted_scope_ids
    FROM deleted_scope_rows;

    WITH deleted_transfer_rows AS (
      DELETE FROM public.pay_bank_transfers AS transfer_delete
      WHERE transfer_delete.pay_batch_id = v_operation_row.pay_batch_id
        AND transfer_delete.id IN (SELECT cleanup_scope.pay_bank_transfer_id FROM pg_temp.tmp_cleanup_scope_ids AS cleanup_scope WHERE cleanup_scope.pay_bank_transfer_id IS NOT NULL)
        AND transfer_delete.status = 'PENDING'
        AND transfer_delete.rail_tx_id IS NULL
        AND transfer_delete.completed_at_utc IS NULL
        AND transfer_delete.failed_reason IS NULL
        AND NOT EXISTS (SELECT 1 FROM public.banking_pay_operation_transfer_scope AS remaining_scope WHERE remaining_scope.pay_bank_transfer_id = transfer_delete.id)
        AND NOT EXISTS (SELECT 1 FROM public.pay_batch_items AS remaining_item WHERE remaining_item.pay_bank_transfer_id = transfer_delete.id)
        AND NOT EXISTS (SELECT 1 FROM public.pay_bank_transfer_events AS transfer_event WHERE transfer_event.pay_bank_transfer_id = transfer_delete.id)
      RETURNING transfer_delete.id
    )
    SELECT COUNT(*)::integer,
           COALESCE(jsonb_agg(to_jsonb(deleted_transfer_rows.id::text) ORDER BY deleted_transfer_rows.id), '[]'::jsonb)
    INTO v_transfer_rows_deleted,
         v_deleted_transfer_ids
    FROM deleted_transfer_rows;

    WITH chunks_to_mutate AS (
      SELECT operation_chunk.id AS operation_chunk_id,
             operation_chunk.status AS previous_status,
             (operation_chunk.locked_by IS NOT NULL OR operation_chunk.lock_expires_at_utc IS NOT NULL) AS had_lock
      FROM public.banking_pay_operation_chunks AS operation_chunk
      WHERE operation_chunk.operation_id = p_operation_id
        AND operation_chunk.status IN ('PENDING', 'RUNNING')
        AND NOT (operation_chunk.phase = 'SUBMIT_PROVIDER_TRANSFERS' OR operation_chunk.chunk_type = 'TRANSFER_SUBMIT')
      ORDER BY operation_chunk.sequence_no, operation_chunk.id
      LIMIT 100
    ), mutated_chunks AS (
      UPDATE public.banking_pay_operation_chunks AS operation_chunk_update
      SET status = CASE WHEN chunks_to_mutate.previous_status = 'RUNNING' THEN 'FAILED' ELSE 'SKIPPED' END,
          error_json = CASE WHEN chunks_to_mutate.previous_status = 'RUNNING' THEN jsonb_build_object('code', 'PAYMENT_EXECUTE_BOUNDED_LOCAL_CLEANUP', 'operation_id', p_operation_id::text, 'failure_phase', v_failure_phase, 'cleanup_at_utc', v_now::text) ELSE operation_chunk_update.error_json END,
          locked_by = NULL::text,
          lock_expires_at_utc = NULL::timestamptz,
          completed_at_utc = COALESCE(operation_chunk_update.completed_at_utc, v_now),
          updated_at_utc = v_now
      FROM chunks_to_mutate
      WHERE operation_chunk_update.id = chunks_to_mutate.operation_chunk_id
      RETURNING chunks_to_mutate.previous_status,
                chunks_to_mutate.had_lock
    )
    SELECT COALESCE((COUNT(*) FILTER (WHERE mutated_chunks.previous_status = 'RUNNING')), 0)::integer,
           COALESCE((COUNT(*) FILTER (WHERE mutated_chunks.previous_status = 'PENDING')), 0)::integer,
           COALESCE((COUNT(*) FILTER (WHERE mutated_chunks.had_lock)), 0)::integer
    INTO v_chunks_marked_failed,
         v_chunks_marked_skipped,
         v_locks_released
    FROM mutated_chunks;

    UPDATE public.pay_batches AS batch_update
    SET execution_intent_json = CASE
          WHEN NULLIF(BTRIM(COALESCE(batch_update.execution_intent_json->>'operation_id', '')), '') = p_operation_id::text THEN NULL::jsonb
          ELSE batch_update.execution_intent_json
        END
    WHERE batch_update.id = v_operation_row.pay_batch_id
      AND v_batch_execution_boundary_crossed IS FALSE;
    GET DIAGNOSTICS v_batch_execution_intent_cleared = ROW_COUNT;
  END IF;

  IF COALESCE(p_dry_run, false) IS FALSE THEN
    UPDATE public.banking_pay_operations AS operation_update
    SET status = CASE WHEN v_review_required THEN 'REVIEW_REQUIRED' ELSE 'FAILED' END,
        runner_state = CASE WHEN v_review_required THEN 'WAITING_USER_REVIEW' ELSE 'FAILED' END,
        run_after_utc = NULL::timestamptz,
        requires_user_action = v_review_required,
        resume_reason = CASE WHEN v_review_required THEN COALESCE(v_retry_blocked_reason, 'PAYMENT_EXECUTION_CLEANUP_REVIEW_REQUIRED') ELSE 'PAYMENT_EXECUTION_LOCAL_ARTIFACTS_CLEANED' END,
        error_json = jsonb_strip_nulls(COALESCE(operation_update.error_json, '{}'::jsonb) || jsonb_build_object(
          'cleanup_at_utc', v_now::text,
          'cleanup_mode', v_cleanup_mode,
          'retry_blocked_reason', v_retry_blocked_reason,
          'failure_phase', v_failure_phase
        )),
        locked_by = NULL::text,
        lock_expires_at_utc = NULL::timestamptz,
        updated_at_utc = v_now
    WHERE operation_update.id = p_operation_id;

    BEGIN
      PERFORM public.pay_batch_display_summary_touch(v_operation_row.pay_batch_id);
    EXCEPTION WHEN OTHERS THEN
      NULL;
    END;

    BEGIN
      PERFORM public.banking_pay_batch_signal_touch(
        v_operation_row.pay_batch_id,
        CASE WHEN v_review_required THEN 'PAYMENT_EXECUTION_CLEANUP_REVIEW_REQUIRED' ELSE 'PAYMENT_EXECUTION_LOCAL_ARTIFACTS_CLEANED' END,
        'pay_execute_operation_cleanup_failed_local_artifacts',
        jsonb_strip_nulls(jsonb_build_object(
          'operation_id', p_operation_id::text,
          'cleanup_mode', v_cleanup_mode,
          'retry_blocked_reason', v_retry_blocked_reason
        )),
        true,
        false,
        v_review_required,
        true
      );
    EXCEPTION WHEN OTHERS THEN
      NULL;
    END;
  END IF;

  v_result := jsonb_strip_nulls(jsonb_build_object(
    'ok', true,
    'dry_run', COALESCE(p_dry_run, false),
    'operation_id', p_operation_id::text,
    'pay_batch_id', v_operation_row.pay_batch_id::text,
    'chunk_id', CASE WHEN v_chunk_id IS NULL THEN NULL ELSE v_chunk_id::text END,
    'failure_phase', v_failure_phase,
    'cleanup_mode', v_cleanup_mode,
    'retry_blocked_reason', v_retry_blocked_reason,
    'safe_to_retry', v_safe_to_retry,
    'retry_blocked', v_retry_blocked,
    'review_required', v_review_required,
    'scope_rows_considered', COALESCE(v_scope_rows_considered, 0),
    'transfer_rows_considered', COALESCE(v_transfer_rows_considered, 0),
    'provider_state_risk_count', COALESCE(v_provider_state_risk_count, 0),
    'provider_chunk_risk_count', COALESCE(v_provider_chunk_risk_count, 0),
    'transfer_event_risk_count', COALESCE(v_transfer_event_risk_count, 0),
    'scope_rows_deleted', COALESCE(v_scope_rows_deleted, 0),
    'transfer_rows_deleted', COALESCE(v_transfer_rows_deleted, 0),
    'item_links_cleared', COALESCE(v_item_links_cleared, 0),
    'bank_references_cleared', COALESCE(v_bank_references_cleared, 0),
    'chunks_marked_failed', COALESCE(v_chunks_marked_failed, 0),
    'chunks_marked_skipped', COALESCE(v_chunks_marked_skipped, 0),
    'locks_released', COALESCE(v_locks_released, 0),
    'safe_scope_ids', CASE WHEN COALESCE(p_dry_run, false) THEN COALESCE(v_safe_scope_ids, '[]'::jsonb) ELSE COALESCE(v_deleted_scope_ids, '[]'::jsonb) END,
    'safe_transfer_ids', CASE WHEN COALESCE(p_dry_run, false) THEN COALESCE(v_safe_transfer_ids, '[]'::jsonb) ELSE COALESCE(v_deleted_transfer_ids, '[]'::jsonb) END,
    'unsafe_reasons', COALESCE(v_unsafe_reasons, '[]'::jsonb),
    'active_auth_request_count', COALESCE(v_active_auth_request_count, 0),
    'operation_active_auth_request_count', COALESCE(v_operation_active_auth_request_count, 0),
    'auth_requests_cancelled', COALESCE(v_auth_requests_cancelled, 0),
    'auth_tokens_voided', COALESCE(v_auth_tokens_voided, 0),
    'batch_execution_intent_cleared', COALESCE(v_batch_execution_intent_cleared, 0),
    'batch_execution_boundary_crossed', v_batch_execution_boundary_crossed,
    'execution_commit_state', upper(BTRIM(COALESCE(v_batch_row.execution_commit_state, 'NOT_SUBMITTED'))),
    'execution_commit_ref_present', NULLIF(BTRIM(COALESCE(v_batch_row.execution_commit_ref, '')), '') IS NOT NULL,
    'execution_committed_at_utc_present', v_batch_row.execution_committed_at_utc IS NOT NULL
  ));

  RETURN v_result;
END;
$function$;

-- _pay_finance_case_effective_payout_instruction(uuid)
CREATE OR REPLACE FUNCTION public._pay_finance_case_effective_payout_instruction(p_finance_case_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_case public.pay_advances%rowtype;
  v_candidate public.candidates%rowtype;
  v_umbrella public.umbrellas%rowtype;
  v_oneoff public.pay_finance_case_oneoff_payout_bank_details%rowtype;

  v_component_classification public.pay_finance_component_classification_enum := null;
  v_taxability public.pay_finance_taxability_enum := null;
  v_routing_kind public.pay_finance_routing_kind_enum := null;

  v_pay_method text := null;
  v_pay_channel text := null;
  v_destination_label text := null;
  v_payee_entity_kind text := null;
  v_payee_entity_id uuid := null;

  v_bank_details_hash text := null;
  v_beneficiary_name text := null;
  v_masked_bank_account text := null;

  v_oneoff_bank_details_present boolean := false;
  v_oneoff_bank_details_required boolean := false;
  v_oneoff_bank_details_editable boolean := false;
  v_is_candidate_directed_oneoff_payout boolean := false;
  v_appears_on_umbrella_remittance boolean := false;
  v_generates_candidate_payment_advice boolean := false;

  v_has_noncancelled_batch_item boolean := false;
BEGIN
  IF p_finance_case_id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_FINANCE_CASE_EFFECTIVE_PAYOUT_INSTRUCTION',
      'code', 'FINANCE_CASE_ID_REQUIRED',
      'message', '_pay_finance_case_effective_payout_instruction: finance_case_id is required'
    )::text;
  END IF;

  SELECT pa.*
  INTO v_case
  FROM public.pay_advances AS pa
  WHERE pa.id = p_finance_case_id
  LIMIT 1;

  IF v_case.id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_FINANCE_CASE_EFFECTIVE_PAYOUT_INSTRUCTION',
      'code', 'FINANCE_CASE_NOT_FOUND',
      'message', '_pay_finance_case_effective_payout_instruction: finance case not found',
      'finance_case_id', p_finance_case_id::text
    )::text;
  END IF;

  SELECT c.*
  INTO v_candidate
  FROM public.candidates AS c
  WHERE c.id = v_case.candidate_id
  LIMIT 1;

  IF v_candidate.id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_FINANCE_CASE_EFFECTIVE_PAYOUT_INSTRUCTION',
      'code', 'CANDIDATE_NOT_FOUND',
      'message', '_pay_finance_case_effective_payout_instruction: candidate not found for finance case',
      'finance_case_id', p_finance_case_id::text,
      'candidate_id', v_case.candidate_id::text
    )::text;
  END IF;

  v_pay_method := upper(coalesce(v_candidate.pay_method, ''));
  IF v_pay_method NOT IN ('PAYE', 'UMBRELLA') THEN
    v_pay_method := null;
  END IF;

  IF v_candidate.umbrella_id IS NOT NULL THEN
    SELECT u.*
    INTO v_umbrella
    FROM public.umbrellas AS u
    WHERE u.id = v_candidate.umbrella_id
    LIMIT 1;
  END IF;

  SELECT pfc.classification
  INTO v_component_classification
  FROM public.pay_finance_case_components AS pfc
  WHERE pfc.finance_case_id = p_finance_case_id
    AND pfc.component_key_type = 'CASE_TOTAL'
    AND pfc.component_key_value = 'TOTAL'
    AND pfc.closed_at_utc IS NULL
  ORDER BY pfc.updated_at_utc DESC, pfc.created_at_utc DESC, pfc.id DESC
  LIMIT 1;

  v_taxability := v_case.taxability;
  IF v_taxability IS NULL THEN
    IF v_case.case_type = 'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum THEN
      v_taxability := 'NON_TAXABLE'::public.pay_finance_taxability_enum;
    ELSIF v_component_classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum THEN
      v_taxability := 'TAXABLE'::public.pay_finance_taxability_enum;
    ELSIF v_component_classification IN (
      'REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum,
      'NET_PAY_FIXED_RECOVERY'::public.pay_finance_component_classification_enum
    ) THEN
      v_taxability := 'NON_TAXABLE'::public.pay_finance_taxability_enum;
    END IF;
  END IF;

  v_routing_kind := v_case.routing_kind;
  IF v_routing_kind IS NULL THEN
    IF v_case.case_type = 'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum THEN
      IF v_pay_method = 'PAYE' THEN
        v_routing_kind := 'NORMAL_PAY_ROUTE'::public.pay_finance_routing_kind_enum;
      ELSIF v_pay_method = 'UMBRELLA' THEN
        v_routing_kind := 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'::public.pay_finance_routing_kind_enum;
      END IF;
    ELSIF v_case.case_type = 'MANUAL_CREDIT_ADJUSTMENT'::public.pay_finance_case_type_enum THEN
      IF v_pay_method = 'PAYE' THEN
        v_routing_kind := 'NORMAL_PAY_ROUTE'::public.pay_finance_routing_kind_enum;
      ELSIF v_pay_method = 'UMBRELLA' AND v_taxability = 'TAXABLE'::public.pay_finance_taxability_enum THEN
        v_routing_kind := 'UMBRELLA_COMPANY'::public.pay_finance_routing_kind_enum;
      ELSIF v_pay_method = 'UMBRELLA' AND v_taxability = 'NON_TAXABLE'::public.pay_finance_taxability_enum THEN
        v_routing_kind := 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'::public.pay_finance_routing_kind_enum;
      END IF;
    ELSIF v_case.case_type = 'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum THEN
      IF v_pay_method = 'PAYE' THEN
        v_routing_kind := 'NORMAL_PAY_ROUTE'::public.pay_finance_routing_kind_enum;
      ELSIF v_pay_method = 'UMBRELLA' THEN
        v_routing_kind := 'UMBRELLA_COMPANY'::public.pay_finance_routing_kind_enum;
      END IF;
    END IF;
  END IF;

  v_oneoff_bank_details_required := coalesce(v_case.oneoff_bank_details_required, false);
  IF v_oneoff_bank_details_required = false THEN
    v_oneoff_bank_details_required := (
      v_routing_kind = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'::public.pay_finance_routing_kind_enum
      AND v_case.case_type IN (
        'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum,
        'MANUAL_CREDIT_ADJUSTMENT'::public.pay_finance_case_type_enum
      )
    );
  END IF;

  SELECT d.*
  INTO v_oneoff
  FROM public.pay_finance_case_oneoff_payout_bank_details AS d
  WHERE d.finance_case_id = p_finance_case_id
  LIMIT 1;

  v_oneoff_bank_details_present := (v_oneoff.finance_case_id IS NOT NULL);

  SELECT exists (
    SELECT 1
    FROM public.pay_batch_items AS pbi
    JOIN public.pay_batch_candidates AS pbc
      ON pbc.id = pbi.pay_batch_candidate_id
    JOIN public.pay_batches AS pb
      ON pb.id = pbc.pay_batch_id
    WHERE (
        pbi.finance_case_id = p_finance_case_id
        OR pbi.source_ref = ('advance:'::text || p_finance_case_id::text)
      )
      AND coalesce(pbi.is_voided, false) = false
      AND pb.cancelled_at_utc IS NULL
      AND upper(coalesce(pb.status, '')) <> 'CANCELLED'
  )
  INTO v_has_noncancelled_batch_item;

  v_oneoff_bank_details_editable := (
    v_oneoff_bank_details_required = true
    AND v_case.case_type IN (
      'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum,
      'MANUAL_CREDIT_ADJUSTMENT'::public.pay_finance_case_type_enum
    )
    AND v_case.payout_pay_batch_id IS NULL
    AND v_has_noncancelled_batch_item = false
  );

  IF v_routing_kind = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'::public.pay_finance_routing_kind_enum THEN
    v_destination_label := 'one-off specified bank account';
    v_payee_entity_kind := 'CANDIDATE';
    v_payee_entity_id := v_candidate.id;
    v_bank_details_hash := v_oneoff.bank_details_hash;
    v_beneficiary_name := v_oneoff.beneficiary_name;
    IF nullif(coalesce(v_oneoff.account_number, ''), '') IS NOT NULL THEN
      v_masked_bank_account := lpad(right(v_oneoff.account_number, 4), length(v_oneoff.account_number), '*');
    END IF;
  ELSIF v_routing_kind = 'UMBRELLA_COMPANY'::public.pay_finance_routing_kind_enum THEN
    v_destination_label := 'umbrella company';
    v_payee_entity_kind := 'UMBRELLA';
    v_payee_entity_id := v_candidate.umbrella_id;
    v_bank_details_hash := v_umbrella.bank_details_hash;
    v_beneficiary_name := v_umbrella.name;
    IF nullif(coalesce(v_umbrella.account_number, ''), '') IS NOT NULL THEN
      v_masked_bank_account := lpad(right(v_umbrella.account_number, 4), length(v_umbrella.account_number), '*');
    END IF;
  ELSE
    v_destination_label := 'normal PAYE route';
    v_payee_entity_kind := 'CANDIDATE';
    v_payee_entity_id := v_candidate.id;
    v_bank_details_hash := v_candidate.bank_details_hash;
    v_beneficiary_name := nullif(btrim(coalesce(v_candidate.account_holder, v_candidate.display_name, concat_ws(' ', v_candidate.first_name, v_candidate.last_name))), '');
    IF nullif(coalesce(v_candidate.account_number, ''), '') IS NOT NULL THEN
      v_masked_bank_account := lpad(right(v_candidate.account_number, 4), length(v_candidate.account_number), '*');
    END IF;
  END IF;

  v_pay_channel := v_pay_method;

  v_is_candidate_directed_oneoff_payout := (
    v_routing_kind = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'::public.pay_finance_routing_kind_enum
    AND v_case.case_type IN (
      'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum,
      'MANUAL_CREDIT_ADJUSTMENT'::public.pay_finance_case_type_enum
    )
    AND (
      v_case.case_type <> 'MANUAL_CREDIT_ADJUSTMENT'::public.pay_finance_case_type_enum
      OR v_taxability = 'NON_TAXABLE'::public.pay_finance_taxability_enum
    )
  );

  v_appears_on_umbrella_remittance := (
    v_routing_kind = 'UMBRELLA_COMPANY'::public.pay_finance_routing_kind_enum
    AND v_case.case_type IN (
      'MANUAL_CREDIT_ADJUSTMENT'::public.pay_finance_case_type_enum,
      'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum
    )
  );

  v_generates_candidate_payment_advice := v_is_candidate_directed_oneoff_payout;

  RETURN jsonb_build_object(
    'finance_case_id', p_finance_case_id::text,
    'case_type', v_case.case_type::text,
    'candidate_id', v_candidate.id::text,
    'client_id', CASE WHEN v_case.client_id IS NULL THEN NULL ELSE v_case.client_id::text END,
    'pay_method', v_pay_method,
    'pay_channel', v_pay_channel,
    'taxability', CASE WHEN v_taxability IS NULL THEN NULL ELSE v_taxability::text END,
    'routing_kind', CASE WHEN v_routing_kind IS NULL THEN NULL ELSE v_routing_kind::text END,
    'destination_label', v_destination_label,
    'payee_entity_kind', v_payee_entity_kind,
    'payee_entity_id', CASE WHEN v_payee_entity_id IS NULL THEN NULL ELSE v_payee_entity_id::text END,
    'beneficiary_name', v_beneficiary_name,
    'bank_details_hash', v_bank_details_hash,
    'masked_bank_account', v_masked_bank_account,
    'oneoff_bank_details_required', v_oneoff_bank_details_required,
    'oneoff_bank_details_present', v_oneoff_bank_details_present,
    'oneoff_bank_details_editable', v_oneoff_bank_details_editable,
    'is_candidate_directed_oneoff_payout', v_is_candidate_directed_oneoff_payout,
    'appears_on_umbrella_remittance', v_appears_on_umbrella_remittance,
    'generates_candidate_payment_advice', v_generates_candidate_payment_advice
  );
END;
$function$;

