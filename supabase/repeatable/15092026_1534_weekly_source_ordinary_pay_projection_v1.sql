-- Repeatable CloudTMS authority: weekly_source_ordinary_pay_projection_v1
--
-- Plan 6.2 Gate 2.  This owner has exactly two outcomes for a root.
--
--   * A root that has NEVER been authorised is PREPARED: the ordinary public
--     Weekly Timesheet and current TSFIN are written through the established
--     mutable, unauthorised writers (24 section 4.1), and the owner then STOPS.
--     It does not authorise.  The first authorisation is the Office's ordinary
--     Authorise action, used exactly once (XSG-001 / G2-0; 27 section 3).
--
--   * A root that HAS been authorised gets a complete PROPOSED entitlement and
--     nothing else.  It is never unauthorised, its submitted schedule evidence
--     is never overwritten, its current TSFIN is never rewritten and it is
--     never reauthorised (25 section 1 "Removed"; 24 section 4.2; 27 section 4).
--     The Office then decides `Approve updated hours` or `Keep currently
--     approved hours` and the decision publishes one complete current
--     entitlement head through the Gate 5 coordinator.
--
-- There is no REFUSED_LOCKED: a paid, invoiced or Draft-frozen root is no
-- longer an obstacle, because the later path mutates nothing.  Freezing is
-- decided inside the coordinator by the read-only complete-family census.
--
-- This owner still creates no second candidate-pay route and writes no invoice,
-- Draft, Workbench or Banking Pay state.

\set ON_ERROR_STOP on

begin;

create or replace function private.weekly_source_ordinary_projection_hex32_v1(
  p_value text,
  p_error_code text
) returns bytea
language plpgsql immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_value text:=pg_catalog.lower(pg_catalog.btrim(coalesce(p_value,'')));
begin
  if v_value!~'^[0-9a-f]{64}$' then
    raise exception '%',coalesce(nullif(p_error_code,''),'WEEKLY_SOURCE_HASH_INVALID')
      using errcode='22023';
  end if;
  return pg_catalog.decode(v_value,'hex');
end;
$function$;

create or replace function private.weekly_source_ordinary_projection_receipt_json_v1(
  p_receipt public.weekly_source_ordinary_pay_projection_receipts
) returns jsonb
language sql stable
set search_path to 'public','pg_catalog','pg_temp'
as $function$
  select pg_catalog.jsonb_build_object(
    -- Every outcome this owner can now reach is a successful outcome.  There is
    -- no refusal state left: Gate 2 deleted REFUSED_LOCKED rather than routing
    -- it anywhere.
    'ok',true,
    'outcome',p_receipt.outcome,
    'error_code',null::text,
    'receipt_id',p_receipt.id,
    'receipt_hash',pg_catalog.encode(p_receipt.receipt_hash,'hex'),
    'final_revision_id',p_receipt.final_revision_id,
    'root_timesheet_id',p_receipt.root_timesheet_id,
    'timesheet_financials_id',p_receipt.published_timesheet_financial_id,
    'source_unit_count',p_receipt.source_unit_count,
    'source_expense_authority_count',
      pg_catalog.jsonb_array_length(p_receipt.source_expense_authorities_json),
    'root_after_hash',pg_catalog.encode(p_receipt.root_after_hash,'hex'),
    'created_at_utc',p_receipt.created_at_utc
  );
$function$;

create or replace function private.weekly_source_ordinary_projection_expense_movement_assert_v1(
  p_billing_movement_id uuid,
  p_final_revision_id uuid,
  p_finalisation_cycle_id uuid,
  p_client_manifest_id uuid,
  p_root_timesheet_id uuid
) returns boolean
language plpgsql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_movement public.weekly_source_billing_movements%rowtype;
  v_authority public.weekly_expense_authority_generations%rowtype;
  v_prior_authority public.weekly_expense_authority_generations%rowtype;
  v_policy public.weekly_source_row_expense_policy_snapshots%rowtype;
  v_resolution public.weekly_source_row_resolutions%rowtype;
  v_source_row public.weekly_source_upload_rows%rowtype;
  v_prior_movement public.weekly_source_billing_movements%rowtype;
  v_event public.weekly_work_events%rowtype;
  v_contract public.contracts%rowtype;
  v_old_pence bigint:=0;
  v_new_pence bigint;
  v_expected_facts jsonb;
  v_expected_pay_vector jsonb;
  v_expected_charge_vector jsonb;
  v_expected_mapping_fingerprint bytea;
  v_expected_movement_hash bytea;
  v_expected_authority_hash bytea;
  v_expected_policy_snapshot_hash bytea;
  v_expected_vat_rate numeric:=0;
  v_expected_vat numeric:=0;
  v_expected_total_inc_vat numeric:=0;
  v_expected_role text;
  -- WP-30 (WP-27 sweep finding N6), standing rule 3.  The lineage assert below
  -- keyed the source-row binding on the physical root id, so a legitimate
  -- rotated root raised WEEKLY_SOURCE_EXPENSE_POLICY_LINEAGE_INVALID instead of
  -- projecting.  It fails CLOSED, so it is a correctness defect rather than a
  -- money risk, and it moves to the family with the rest of the class.  One
  -- resolution, through the one installed adapter.
  v_family uuid[];
begin
  if p_billing_movement_id is null or p_final_revision_id is null
     or p_finalisation_cycle_id is null or p_client_manifest_id is null
     or p_root_timesheet_id is null then
    raise exception 'WEEKLY_SOURCE_EXPENSE_MOVEMENT_ASSERT_INPUT_INVALID'
      using errcode='22023';
  end if;
  v_family:=private.weekly_source_invoice_family_timesheet_ids_v1(p_root_timesheet_id);
  -- Standing rule 3's fail-closed branch: an explicit cardinality test, never a
  -- `limit`.  This assert already refuses on every other unprovable fact.
  if v_family is null or pg_catalog.cardinality(v_family)=0 then
    raise exception 'WEEKLY_SOURCE_EXPENSE_POLICY_LINEAGE_FAMILY_UNRESOLVED'
      using errcode='55000';
  end if;
  select * into strict v_movement
  from public.weekly_source_billing_movements movement
  where movement.id=p_billing_movement_id;
  if v_movement.final_revision_id is distinct from p_final_revision_id
     or v_movement.finalisation_cycle_id is distinct from p_finalisation_cycle_id
     or v_movement.invoice_timesheet_id is distinct from p_root_timesheet_id
     or v_movement.transition_id is not null
     or v_movement.nhsp_upload_row_id is not null
     or v_movement.expense_authority_generation_id is null
     or v_movement.source_profile_kind<>'GENERIC_COMPLETE_SNAPSHOT'
     or v_movement.source_line_kind<>'SOURCE_FIXED_EXPENSE'
     or v_movement.movement_role not in (
       'EXPENSE_POSITIVE','EXPENSE_REVERSAL','EXPENSE_REPLACEMENT'
     )
     or v_movement.price_check_result<>'NOT_APPLICABLE'
     or v_movement.price_check_fingerprint is not null
     or v_movement.original_cycle_key is distinct from p_finalisation_cycle_id::text
     or not exists(
       select 1 from public.weekly_source_manifest_movements manifest_movement
       where manifest_movement.client_manifest_id=p_client_manifest_id
         and manifest_movement.billing_movement_id=v_movement.id
         and manifest_movement.movement_hash=v_movement.movement_economic_hash
     ) then
    raise exception 'WEEKLY_SOURCE_EXPENSE_MOVEMENT_INTEGRITY_FAILED'
      using errcode='55000';
  end if;

  select * into strict v_authority
  from public.weekly_expense_authority_generations authority
  where authority.id=v_movement.expense_authority_generation_id;
  select * into strict v_event
  from public.weekly_work_events event_row where event_row.id=v_authority.work_event_id;
  select * into strict v_contract
  from public.contracts contract_row where contract_row.id=v_authority.contract_id;
  v_new_pence:=v_authority.source_expense_pence;
  if v_authority.final_revision_id is distinct from p_final_revision_id
     or not (
       (v_authority.state='CURRENT' and not exists(
         select 1 from public.weekly_expense_authority_generations later_authority
         where later_authority.prior_expense_authority_generation_id=v_authority.id
       ))
       or
       (v_authority.state='SUPERSEDED' and exists(
         select 1 from public.weekly_expense_authority_generations later_authority
         where later_authority.prior_expense_authority_generation_id=v_authority.id
           and later_authority.work_event_id=v_authority.work_event_id
           and later_authority.generation=v_authority.generation+1
       ))
     )
     or v_authority.work_event_id is distinct from v_movement.work_event_id
     or v_authority.candidate_reimbursement_ex_vat
          is distinct from v_new_pence::numeric/100
     or v_authority.client_charge_ex_vat
          is distinct from v_new_pence::numeric/100
     or v_contract.candidate_id is distinct from v_event.candidate_id
     or v_contract.client_id is distinct from v_event.client_id then
    raise exception 'WEEKLY_SOURCE_EXPENSE_AUTHORITY_INTEGRITY_FAILED'
      using errcode='55000';
  end if;
  if v_authority.prior_expense_authority_generation_id is not null then
    select * into strict v_prior_authority
    from public.weekly_expense_authority_generations authority
    where authority.id=v_authority.prior_expense_authority_generation_id
      and authority.work_event_id=v_authority.work_event_id
      and authority.generation=v_authority.generation-1
      and authority.state='SUPERSEDED';
    v_old_pence:=v_prior_authority.source_expense_pence;
    if v_prior_authority.candidate_reimbursement_ex_vat
          is distinct from v_old_pence::numeric/100
       or v_prior_authority.client_charge_ex_vat
          is distinct from v_old_pence::numeric/100 then
      raise exception 'WEEKLY_SOURCE_PRIOR_EXPENSE_AUTHORITY_INTEGRITY_FAILED'
        using errcode='55000';
    end if;
  elsif v_authority.generation<>1 then
    raise exception 'WEEKLY_SOURCE_PRIOR_EXPENSE_AUTHORITY_MISSING'
      using errcode='55000';
  end if;
  v_expected_authority_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_EXPENSE_AUTHORITY_GENERATION_V1',
    pg_catalog.jsonb_build_object(
      'final_revision_id',v_authority.final_revision_id,
      'work_event_id',v_authority.work_event_id,
      'contract_id',v_authority.contract_id,
      'row_expense_policy_snapshot_id',v_authority.row_expense_policy_snapshot_id,
      'prior_expense_authority_generation_id',
        v_authority.prior_expense_authority_generation_id,
      'generation',v_authority.generation,
      'source_observation_kind',v_authority.source_observation_kind,
      'correction_presentation',v_authority.correction_presentation,
      'source_expense_pence',v_authority.source_expense_pence,
      'source_expense_vat_enabled',v_authority.source_expense_vat_enabled
    )
  );
  if v_authority.authority_hash is distinct from v_expected_authority_hash then
    raise exception 'WEEKLY_SOURCE_EXPENSE_AUTHORITY_HASH_MISMATCH'
      using errcode='55000';
  end if;

  if v_authority.source_observation_kind='ROW_PRESENT' then
    select * into strict v_policy
    from public.weekly_source_row_expense_policy_snapshots policy
    where policy.id=v_authority.row_expense_policy_snapshot_id;
    select resolution.* into strict v_resolution
    from public.weekly_source_row_resolutions resolution
    where resolution.id=v_policy.row_resolution_id;
    select source_row.* into strict v_source_row
    from public.weekly_source_upload_rows source_row
    join public.weekly_source_uploads upload_row
      on upload_row.id=source_row.upload_id
     and upload_row.source_cycle_id=p_finalisation_cycle_id
    where source_row.id=v_resolution.upload_row_id;
    v_expected_policy_snapshot_hash:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_ROW_EXPENSE_POLICY_SNAPSHOT_V1',
      pg_catalog.jsonb_build_object(
        'row_resolution_id',v_resolution.id,
        'upload_row_id',v_source_row.id,
        'generation',v_resolution.generation,
        'work_event_id',v_resolution.work_event_id,
        'candidate_id',v_resolution.candidate_id,
        'client_id',v_resolution.client_id,
        'contract_id',v_resolution.contract_id,
        'row_finalisation_state',v_source_row.row_finalisation_state,
        'source_expense_pence',v_source_row.source_expense_pence,
        'source_expense_parse_state',v_source_row.source_expense_parse_state,
        'source_expense_vat_enabled',v_policy.source_expense_vat_enabled,
        'invoice_vat_chargeable',v_policy.invoice_vat_chargeable,
        'invoice_vat_rate_pct',v_policy.invoice_vat_rate_pct,
        'correction_presentation',v_policy.correction_presentation,
        'effective_policy_fingerprint',
          pg_catalog.encode(v_policy.effective_policy_fingerprint,'hex'),
        'invoice_vat_policy_fingerprint',
          pg_catalog.encode(v_policy.invoice_vat_policy_fingerprint,'hex')
      )
    );
    -- The final revision freezes the policy used at finalisation.  Recompute
    -- that immutable snapshot; do not re-resolve today's settings, because a
    -- later-dated policy row must not retroactively invalidate a frozen source
    -- cycle before it reaches the ordinary Timesheet projection.
    if v_resolution.mapping_state<>'RESOLVED'
       or v_policy.upload_row_id is distinct from v_source_row.id
       or v_policy.generation is distinct from v_resolution.generation
       or v_policy.work_event_id is distinct from v_authority.work_event_id
       or v_policy.contract_id is distinct from v_authority.contract_id
       or v_policy.candidate_id is distinct from v_event.candidate_id
       or v_policy.client_id is distinct from v_event.client_id
       or v_resolution.work_event_id is distinct from v_policy.work_event_id
       or v_resolution.candidate_id is distinct from v_policy.candidate_id
       or v_resolution.client_id is distinct from v_policy.client_id
       or v_resolution.contract_id is distinct from v_policy.contract_id
       or v_resolution.effective_policy_fingerprint
            is distinct from v_policy.effective_policy_fingerprint
       or v_source_row.row_finalisation_state not in (
            'NOT_APPLICABLE','SOURCE_WORKED','SOURCE_ABSENT_ZERO'
          )
       or v_policy.source_expense_pence is distinct from v_new_pence
       or v_policy.source_expense_pence is distinct from v_source_row.source_expense_pence
       or v_policy.source_expense_parse_state
            is distinct from v_source_row.source_expense_parse_state
       or v_policy.source_expense_vat_enabled
            is distinct from v_authority.source_expense_vat_enabled
       or v_policy.correction_presentation
            is distinct from v_authority.correction_presentation
       or (
         v_source_row.row_finalisation_state='SOURCE_ABSENT_ZERO'
         and exists(
           select 1
           from public.weekly_source_row_economic_snapshots economic
           where economic.row_resolution_id=v_resolution.id
         )
       )
       or (
         v_policy.source_expense_pence>0
         and (
           v_source_row.start_at_local is null
           or v_source_row.end_at_local is null
           or v_source_row.end_at_local<=v_source_row.start_at_local
           or v_source_row.break_minutes is null
         )
       )
       or v_policy.snapshot_hash is distinct from v_expected_policy_snapshot_hash
       or (
         v_new_pence>0
         and not exists(
           select 1
           from public.weekly_source_row_timesheet_lineages lineage
           where lineage.row_resolution_id=v_policy.row_resolution_id
             and lineage.work_event_id=v_policy.work_event_id
             and lineage.contract_id=v_policy.contract_id
             and lineage.timesheet_id=any(v_family)
         )
       ) then
      raise exception 'WEEKLY_SOURCE_EXPENSE_POLICY_LINEAGE_INVALID'
        using errcode='55000';
    end if;
  elsif v_authority.source_observation_kind='OMITTED_IN_COMPLETE_COVERAGE' then
    if v_authority.row_expense_policy_snapshot_id is not null
       or v_authority.prior_expense_authority_generation_id is null
       or v_new_pence<>0 then
      raise exception 'WEEKLY_SOURCE_EXPENSE_OMISSION_INVALID'
        using errcode='55000';
    end if;
  else
    raise exception 'WEEKLY_SOURCE_EXPENSE_OBSERVATION_KIND_INVALID'
      using errcode='55000';
  end if;

  if v_old_pence>0 then
    select * into strict v_prior_movement
    from public.weekly_source_billing_movements movement
    where movement.expense_authority_generation_id=v_prior_authority.id
      and movement.source_line_kind='SOURCE_FIXED_EXPENSE'
      and movement.movement_role in ('EXPENSE_POSITIVE','EXPENSE_REPLACEMENT')
      and movement.invoice_presentation_charge_pence=v_old_pence
    order by movement.created_at_utc desc,movement.id desc
    limit 1;
  end if;
  v_expected_facts:=pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_SOURCE_FIXED_EXPENSE_FACTS_V1',
    'correction_presentation',v_authority.correction_presentation,
    'old_expense_pence',v_old_pence,'new_expense_pence',v_new_pence,
    'authority_generation_id',v_authority.id,
    'prior_authority_generation_id',v_authority.prior_expense_authority_generation_id,
    'row_expense_policy_snapshot_id',v_authority.row_expense_policy_snapshot_id,
    'source_observation_kind',v_authority.source_observation_kind
  );
  if v_movement.source_facts_json is distinct from v_expected_facts then
    raise exception 'WEEKLY_SOURCE_EXPENSE_MOVEMENT_FACTS_MISMATCH'
      using errcode='55000';
  end if;

  if v_movement.movement_role='EXPENSE_REVERSAL' then
    if v_old_pence<=0 or v_movement.prior_movement_id is distinct from v_prior_movement.id
       or v_movement.actual_client_id is distinct from v_prior_movement.actual_client_id
       or v_movement.candidate_id is distinct from v_prior_movement.candidate_id
       or v_movement.contract_id is distinct from v_prior_movement.contract_id
       or v_movement.work_event_id is distinct from v_prior_movement.work_event_id then
      raise exception 'WEEKLY_SOURCE_EXPENSE_REVERSAL_LINEAGE_INVALID'
        using errcode='55000';
    end if;
    v_expected_pay_vector:=pg_catalog.jsonb_build_object(
      'schema_version','WEEKLY_SOURCE_FIXED_EXPENSE_VECTOR_V1',
      'kind','PAY_AND_CHARGE','source_expense_pence',-v_old_pence,
      'source_expense_vat_enabled',v_prior_authority.source_expense_vat_enabled
    );
    v_expected_charge_vector:=v_expected_pay_vector;
    v_expected_vat_rate:=v_prior_movement.vat_rate_pct;
    v_expected_vat:=-v_prior_movement.vat_amount;
    v_expected_total_inc_vat:=-v_prior_movement.total_inc_vat;
    v_expected_mapping_fingerprint:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_FIXED_EXPENSE_MAPPING_V1',
      pg_catalog.jsonb_build_object(
        'current_expense_authority_generation_id',v_authority.id,
        'prior_expense_authority_generation_id',v_prior_authority.id,
        'prior_movement_id',v_prior_movement.id,'role','EXPENSE_REVERSAL'
      )
    );
    v_expected_movement_hash:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_BILLING_MOVEMENT_V1',
      pg_catalog.jsonb_build_object(
        'expense_authority_generation_id',v_authority.id,
        'final_revision_id',p_final_revision_id,
        'finalisation_cycle_id',p_finalisation_cycle_id,
        'actual_client_id',v_movement.actual_client_id,
        'candidate_id',v_movement.candidate_id,
        'contract_id',v_movement.contract_id,
        'work_event_id',v_movement.work_event_id,
        'movement_role','EXPENSE_REVERSAL',
        'correction_unit_id',v_movement.correction_unit_id,
        'prior_movement_id',v_movement.prior_movement_id,
        'source_profile_kind',v_movement.source_profile_kind,
        'source_line_kind','SOURCE_FIXED_EXPENSE',
        'source_facts',v_movement.source_facts_json,
        'vector',v_movement.canonical_pay_vector_json,
        'invoice_presentation_charge_pence',
          v_movement.invoice_presentation_charge_pence,
        'vat_rate_pct',v_movement.vat_rate_pct,
        'vat_amount',v_movement.vat_amount,
        'total_inc_vat',v_movement.total_inc_vat,
        'mapping_rate_policy_fingerprint',
          pg_catalog.encode(v_movement.mapping_rate_policy_fingerprint,'hex'),
        'invoice_timesheet_id',v_movement.invoice_timesheet_id
      )
    );
    if v_movement.total_pay_ex_vat is distinct from -v_old_pence::numeric/100
       or v_movement.calculated_comparison_charge_pence is distinct from -v_old_pence
       or v_movement.source_validation_charge_pence is distinct from -v_old_pence
       or v_movement.invoice_presentation_charge_pence is distinct from -v_old_pence then
      raise exception 'WEEKLY_SOURCE_EXPENSE_REVERSAL_ECONOMICS_INVALID'
        using errcode='55000';
    end if;
  else
    v_expected_role:=case when v_old_pence=0
      then 'EXPENSE_POSITIVE' else 'EXPENSE_REPLACEMENT' end;
    if v_new_pence<=0 or v_movement.movement_role<>v_expected_role
       or v_movement.actual_client_id is distinct from v_policy.client_id
       or v_movement.candidate_id is distinct from v_policy.candidate_id
       or v_movement.contract_id is distinct from v_policy.contract_id
       or v_movement.work_event_id is distinct from v_policy.work_event_id
       or v_movement.prior_movement_id is distinct from
            (case when v_old_pence>0 then v_prior_movement.id end) then
      raise exception 'WEEKLY_SOURCE_EXPENSE_POSITIVE_LINEAGE_INVALID'
        using errcode='55000';
    end if;
    v_expected_pay_vector:=pg_catalog.jsonb_build_object(
      'schema_version','WEEKLY_SOURCE_FIXED_EXPENSE_VECTOR_V1','kind','PAY',
      'source_expense_pence',v_new_pence,
      'source_expense_vat_enabled',v_authority.source_expense_vat_enabled
    );
    v_expected_charge_vector:=v_expected_pay_vector
      ||pg_catalog.jsonb_build_object('kind','CHARGE');
    v_expected_vat_rate:=case when v_authority.source_expense_vat_enabled
      then v_policy.invoice_vat_rate_pct else 0 end;
    v_expected_vat:=pg_catalog.round(
      (v_new_pence::numeric/100)*(v_expected_vat_rate/100),2
    );
    v_expected_total_inc_vat:=v_new_pence::numeric/100+v_expected_vat;
    v_expected_mapping_fingerprint:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_FIXED_EXPENSE_MAPPING_V1',
      pg_catalog.jsonb_build_object(
        'current_expense_authority_generation_id',v_authority.id,
        'prior_expense_authority_generation_id',
          v_authority.prior_expense_authority_generation_id,
        'expense_policy_snapshot_id',v_policy.id,
        'invoice_vat_policy_fingerprint',
          pg_catalog.encode(v_policy.invoice_vat_policy_fingerprint,'hex'),
        'role',v_expected_role
      )
    );
    v_expected_movement_hash:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_BILLING_MOVEMENT_V1',
      pg_catalog.jsonb_build_object(
        'expense_authority_generation_id',v_authority.id,
        'final_revision_id',p_final_revision_id,
        'finalisation_cycle_id',p_finalisation_cycle_id,
        'actual_client_id',v_movement.actual_client_id,
        'candidate_id',v_movement.candidate_id,
        'contract_id',v_movement.contract_id,
        'work_event_id',v_movement.work_event_id,
        'movement_role',v_movement.movement_role,
        'correction_unit_id',v_movement.correction_unit_id,
        'prior_movement_id',v_movement.prior_movement_id,
        'source_profile_kind',v_movement.source_profile_kind,
        'source_line_kind','SOURCE_FIXED_EXPENSE',
        'source_facts',v_movement.source_facts_json,
        'pay_vector',v_movement.canonical_pay_vector_json,
        'charge_vector',v_movement.canonical_charge_vector_json,
        'invoice_presentation_charge_pence',
          v_movement.invoice_presentation_charge_pence,
        'vat_rate_pct',v_movement.vat_rate_pct,
        'vat_amount',v_movement.vat_amount,
        'total_inc_vat',v_movement.total_inc_vat,
        'mapping_rate_policy_fingerprint',
          pg_catalog.encode(v_movement.mapping_rate_policy_fingerprint,'hex'),
        'invoice_timesheet_id',v_movement.invoice_timesheet_id
      )
    );
    if v_movement.total_pay_ex_vat is distinct from v_new_pence::numeric/100
       or v_movement.calculated_comparison_charge_pence is distinct from v_new_pence
       or v_movement.source_validation_charge_pence is distinct from v_new_pence
       or v_movement.invoice_presentation_charge_pence is distinct from v_new_pence then
      raise exception 'WEEKLY_SOURCE_EXPENSE_POSITIVE_ECONOMICS_INVALID'
        using errcode='55000';
    end if;
  end if;

  if v_movement.canonical_pay_vector_json is distinct from v_expected_pay_vector
     or v_movement.canonical_charge_vector_json is distinct from v_expected_charge_vector
     or v_movement.vat_rate_pct is distinct from v_expected_vat_rate
     or v_movement.vat_amount is distinct from v_expected_vat
     or v_movement.total_inc_vat is distinct from v_expected_total_inc_vat
     or v_movement.mapping_rate_policy_fingerprint
          is distinct from v_expected_mapping_fingerprint
     or v_movement.movement_economic_hash is distinct from v_expected_movement_hash then
    raise exception 'WEEKLY_SOURCE_EXPENSE_MOVEMENT_ECONOMIC_HASH_MISMATCH'
      using errcode='55000',detail=pg_catalog.jsonb_build_object(
        'movement_id',v_movement.id,
        'pay_vector_match',v_movement.canonical_pay_vector_json=v_expected_pay_vector,
        'charge_vector_match',v_movement.canonical_charge_vector_json=v_expected_charge_vector,
        'vat_rate_match',v_movement.vat_rate_pct=v_expected_vat_rate,
        'vat_match',v_movement.vat_amount=v_expected_vat,
        'total_inc_vat_match',v_movement.total_inc_vat=v_expected_total_inc_vat,
        'mapping_fingerprint_match',v_movement.mapping_rate_policy_fingerprint=
          v_expected_mapping_fingerprint,
        'movement_hash_match',v_movement.movement_economic_hash=v_expected_movement_hash,
        'actual_movement_hash',pg_catalog.encode(v_movement.movement_economic_hash,'hex'),
        'expected_movement_hash',pg_catalog.encode(v_expected_movement_hash,'hex')
      )::text;
  end if;
  return true;
exception
  when no_data_found or too_many_rows then
    raise exception 'WEEKLY_SOURCE_EXPENSE_MOVEMENT_SCOPE_INVALID'
      using errcode='55000';
end;
$function$;

-- ---------------------------------------------------------------------------
-- Gate 2 proposal composer.
--
-- 24 section 4.5 step 2 requires "the immutable identity of every moved
-- component", so component_id is DERIVED from the work identity and from
-- nothing else.  It deliberately contains no root id, no Contract id, no
-- revision id and no head id: a component that moves from Contract A to
-- Contract B must carry the SAME component_id into B's head or the
-- coordinator's H2-024 set proofs (A_before = A_after union moved,
-- moved subset of B_after) cannot be stated at all.  This is a SHA-256-derived
-- UUID, not a v4 UUID.
-- WP-06c review finding F3.  This helper used to `coalesce(p_domain,'')` and
-- `coalesce(p_key,'')`, so an ABSENT key produced a perfectly ordinary-looking
-- identifier - and the SAME one every time.  A caller whose bundle identity was
-- null therefore derived ONE movement_id for every component of the request
-- (executed: `d5688827-1e02-adde-ea86-4e55dc39816b` for two different
-- components).  Nothing installed could persist that, because the canonicaliser
-- refuses the null identities first, but a derivation helper that maps an absent
-- key to a fixed identity is a collision class waiting for a caller that does.
-- A derived money identity with no input is not an identity, so it raises.
create or replace function private.weekly_source_entitlement_derived_uuid_v1(
  p_domain text,
  p_key text
) returns uuid
language plpgsql immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_hex text;
begin
  if p_domain is null or p_key is null then
    raise exception 'WEEKLY_SOURCE_ENTITLEMENT_DERIVED_IDENTITY_KEY_INVALID'
      using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_ENTITLEMENT_DERIVED_IDENTITY_KEY_INVALID',
              'reason','NULL_NOT_ALLOWED',
              'domain_is_null',p_domain is null,
              'key_is_null',p_key is null)::text;
  end if;
  v_hex:=pg_catalog.encode(pg_catalog.sha256(
           pg_catalog.convert_to(p_domain||'|'||p_key,'UTF8')),'hex');
  return (
    pg_catalog.substr(v_hex,1,8)||'-'||pg_catalog.substr(v_hex,9,4)||'-'||
    pg_catalog.substr(v_hex,13,4)||'-'||pg_catalog.substr(v_hex,17,4)||'-'||
    pg_catalog.substr(v_hex,21,12)
  )::uuid;
end;
$function$;

create or replace function private.weekly_source_entitlement_component_id_v1(
  p_component_kind text,
  p_economic_key_type text,
  p_economic_key_value text,
  p_component_member_identity text
) returns uuid
language sql immutable
set search_path to 'private','pg_catalog','pg_temp'
as $function$
  select private.weekly_source_entitlement_derived_uuid_v1(
    'WEEKLY_SOURCE_ENTITLEMENT_COMPONENT_V1',
    coalesce(p_component_kind,'')||'|'||coalesce(p_economic_key_type,'')||'|'||
    coalesce(p_economic_key_value,'')||'|'||coalesce(p_component_member_identity,'')
  );
$function$;

-- One complete proposed entitlement vector, in the exact shape
-- IMPL\interfaces\PUBLICATION_REQUEST_SHAPE.md section 4.1 fixes: every key of
-- the head-component allowlist present, null where absent, money and hours as
-- fixed-scale decimal STRINGS, and no adjustment_id anywhere (WB-007, WB-013,
-- 24 section 5: an ordinary non-advance ts_pay_adjustments occurrence stays
-- independently owned and is composed once by the Workbench, outside the head).
create or replace function private.weekly_source_entitlement_components_v1(
  p_segments jsonb,
  p_expenses jsonb
) returns jsonb
language sql immutable
set search_path to 'private','pg_catalog','pg_temp'
as $function$
  with worked as (
    select
      segment.ordinality as within_kind_ordinal,
      1 as kind_order,
      'WORKED_TIME'::text as component_kind,
      'SEGMENT'::text as economic_key_type,
      segment.value->>'segment_id' as economic_key_value,
      -- A segment read back from an ordinary (non-Weekly-Source) financial
      -- snapshot carries no `weekly_source` block, so the stable identity falls
      -- back to the segment id itself.  It must never be null, or two unrelated
      -- segments would derive the same component id.
      coalesce(segment.value#>>'{weekly_source,work_event_id}',
               segment.value->>'segment_id') as component_member_identity,
      segment.value->>'segment_id' as segment_id,
      coalesce(segment.value#>>'{weekly_source,work_event_id}',
               segment.value->>'segment_id') as segment_key,
      segment.value#>>'{weekly_source,calculation_fingerprint}' as segment_stable_key,
      segment.value->>'date' as work_date,
      segment.value->>'ref_num' as reference_number,
      (segment.value->>'hours_day')::numeric as hours_day,
      (segment.value->>'hours_night')::numeric as hours_night,
      (segment.value->>'hours_sat')::numeric as hours_sat,
      (segment.value->>'hours_sun')::numeric as hours_sun,
      (segment.value->>'hours_bh')::numeric as hours_bh,
      null::text as expense_code,
      (segment.value->>'pay_amount')::numeric as pay_ex_vat,
      (segment.value->>'charge_amount')::numeric as charge_ex_vat,
      coalesce((segment.value->>'exclude_from_pay')::boolean,false) as exclude_from_pay,
      'WEEKLY_SOURCE'::text as origin
    from pg_catalog.jsonb_array_elements(coalesce(p_segments,'[]'::jsonb))
      with ordinality as segment(value,ordinality)
  ), expensed as (
    select
      expense.ordinality as within_kind_ordinal,
      2 as kind_order,
      'SOURCE_FIXED_EXPENSE'::text,
      'EXPENSE_AUTHORITY'::text,
      'weekly-source-expense:'||(expense.value->>'work_event_id'),
      expense.value->>'work_event_id',
      null::text,null::text,null::text,null::text,null::text,
      null::numeric,null::numeric,null::numeric,null::numeric,null::numeric,
      expense.value->>'source_observation_kind',
      (expense.value->>'candidate_reimbursement_ex_vat')::numeric,
      (expense.value->>'client_charge_ex_vat')::numeric,
      false,
      'WEEKLY_SOURCE_EXPENSE'::text
    from pg_catalog.jsonb_array_elements(coalesce(p_expenses,'[]'::jsonb))
      with ordinality as expense(value,ordinality)
  ), combined as (
    select * from worked
    union all
    select * from expensed
  ), ordered as (
    select combined.*,
      pg_catalog.row_number() over (
        order by combined.kind_order,combined.within_kind_ordinal
      ) as component_ordinal
    from combined
  )
  select coalesce(pg_catalog.jsonb_agg(
    pg_catalog.jsonb_build_object(
      'component_ordinal',ordered.component_ordinal::integer,
      'component_id',private.weekly_source_entitlement_component_id_v1(
        ordered.component_kind,ordered.economic_key_type,
        ordered.economic_key_value,ordered.component_member_identity),
      'component_kind',ordered.component_kind,
      'economic_key_type',ordered.economic_key_type,
      'economic_key_value',ordered.economic_key_value,
      'component_member_identity',ordered.component_member_identity,
      'segment_id',ordered.segment_id,
      'segment_key',ordered.segment_key,
      'segment_stable_key',ordered.segment_stable_key,
      'work_date',ordered.work_date,
      'reference_number',ordered.reference_number,
      'hours_day',case when ordered.hours_day is null then null
        else pg_catalog.to_char(ordered.hours_day,'FM9999999999990.000000') end,
      'hours_night',case when ordered.hours_night is null then null
        else pg_catalog.to_char(ordered.hours_night,'FM9999999999990.000000') end,
      'hours_sat',case when ordered.hours_sat is null then null
        else pg_catalog.to_char(ordered.hours_sat,'FM9999999999990.000000') end,
      'hours_sun',case when ordered.hours_sun is null then null
        else pg_catalog.to_char(ordered.hours_sun,'FM9999999999990.000000') end,
      'hours_bh',case when ordered.hours_bh is null then null
        else pg_catalog.to_char(ordered.hours_bh,'FM9999999999990.000000') end,
      'additional_code_raw',null::text,
      'unit_count',null::text,
      'unit_pay_rate',null::text,
      'unit_charge_rate',null::text,
      'expense_code',ordered.expense_code,
      'pay_ex_vat',pg_catalog.to_char(coalesce(ordered.pay_ex_vat,0),'FM9999999999990.00'),
      'charge_ex_vat',case when ordered.charge_ex_vat is null then null
        else pg_catalog.to_char(ordered.charge_ex_vat,'FM9999999999990.00') end,
      'exclude_from_pay',ordered.exclude_from_pay,
      'origin',ordered.origin,
      'movement_id',null::uuid,
      'movement_group_id',null::uuid
    ) order by ordered.component_ordinal
  ),'[]'::jsonb)
  from ordered;
$function$;

-- ---------------------------------------------------------------------------
-- INTERFACE I-7 (decision D9).  The server-side, read-only statement of a
-- root's currently effective complete entitlement, read from the SINGLE
-- committed effective authority under the caller's locks:
--
--   * the committed current head when one exists (authority = 'HEAD');
--   * otherwise the root's current authorised financial snapshot, mapped to
--     components by EXACTLY the same composer this file uses (authority =
--     'TSFIN'), so the composer and the coordinator can never disagree about a
--     before-position.
--
-- H2-024: "each before-position is read from the single committed effective
-- authority under lock."  A before-position is therefore never taken from the
-- caller.  Ordinary non-advance `ts_pay_adjustments` occurrences are never
-- included: they stay independently owned outside every head (27 section 5.1;
-- WB-007, WB-013), and this function reads no adjustment table at all.
--
-- Component ids and hashes are deterministic: the ids come from the immutable
-- work identity (see weekly_source_entitlement_component_id_v1) and the hashes
-- are produced by the coordinator's OWN component canonicaliser and encoder,
-- so a snapshot-derived component and the head component it later becomes are
-- byte-identical by construction, and "the moved component" means one thing.
--
-- Reads are bounded: one root Timesheet, one current financial row, that row's
-- own segment array, and that row's own source-fixed expense authorities.
-- STABLE, SECURITY DEFINER, no write, owner-only.
create or replace function private.weekly_source_effective_inventory_v1(
  p_root_timesheet_id uuid
) returns jsonb
language plpgsql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_identity jsonb;
  v_head public.weekly_source_entitlement_heads%rowtype;
  v_financial public.timesheets_financials%rowtype;
  v_components jsonb:='[]'::jsonb;
  v_hashed jsonb:='[]'::jsonb;
  v_pairs jsonb:='[]'::jsonb;
  v_segments jsonb:='[]'::jsonb;
  v_expenses jsonb:='[]'::jsonb;
  v_authority text;
  v_head_id uuid;
  v_head_count integer:=0;
  v_inventory_digest text;
begin
  v_identity:=private.weekly_source_resolve_root_identity_v1(p_root_timesheet_id);
  if coalesce((v_identity->>'ok')::boolean,false) is not true
     or (v_identity->>'canonical_timesheet_id')::uuid is distinct from p_root_timesheet_id then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
      'authority',null,'head_id',null,'components','[]'::jsonb,
      'inventory_digest',null,'component_count',0,
      'detail',coalesce(v_identity,'{}'::jsonb)
    );
  end if;

  -- The root identity is the Timesheet FAMILY, not one physical id: that is what
  -- S8 and WP-01a's head keying establish, and it is how the coordinator
  -- resolves a committed head.  Resolving by the physical id would answer "this
  -- root has no head, the effective entitlement is the ordinary TSFIN" for a
  -- family that rotated after its head was committed -- an UNDERSTATED effective
  -- entitlement, which is exactly the input shape that turns a residual into an
  -- overpayment.  Where the family's head names a different physical root the
  -- answer is a refusal, never a quiet TSFIN.
  -- WP-06c review finding F5.  This read used to be a bare `select ... into`
  -- whose safety rested entirely on the partial unique index
  -- `weekly_source_entitlement_heads_committed_current_uq`.  Part 1 review rule
  -- 5 forbids exactly that: "never express safety through `limit`, `order by` or
  -- 'the unique index makes this impossible'".  Executed with the index dropped
  -- to stand for the later change that removes it, two committed current rows
  -- for one family made this function return an ARBITRARY one with `ok:true`,
  -- and this answer decides a pay outcome.  The cardinality is therefore checked
  -- explicitly and anything unexpected goes to the fail-closed branch, exactly
  -- as `weekly_source_target_family_for_root_v1` below already does.
  select pg_catalog.count(*)::integer into v_head_count
  from public.weekly_source_entitlement_heads head
  where pg_catalog.btrim(head.root_family_booking_id)
        =pg_catalog.btrim(v_identity->>'family_booking_id')
    and head.state='COMMITTED_CURRENT';
  if v_head_count>1 then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
      'authority',null,'head_id',null,'components','[]'::jsonb,
      'inventory_digest',null,'component_count',0,
      'detail',pg_catalog.jsonb_build_object(
        'reason','MULTIPLE_COMMITTED_CURRENT_HEADS_FOR_THE_FAMILY',
        'timesheet_id',p_root_timesheet_id,
        'family_booking_id',v_identity->>'family_booking_id',
        'committed_current_head_count',v_head_count));
  end if;
  if v_head_count=1 then
    select head.* into strict v_head
    from public.weekly_source_entitlement_heads head
    where pg_catalog.btrim(head.root_family_booking_id)
          =pg_catalog.btrim(v_identity->>'family_booking_id')
      and head.state='COMMITTED_CURRENT';
  end if;

  if v_head_count=1 and v_head.root_timesheet_id is distinct from p_root_timesheet_id then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
      'authority',null,'head_id',null,'components','[]'::jsonb,
      'inventory_digest',null,'component_count',0,
      'detail',pg_catalog.jsonb_build_object(
        'reason','COMMITTED_HEAD_BELONGS_TO_ANOTHER_PHYSICAL_ROOT',
        'timesheet_id',p_root_timesheet_id,
        'family_booking_id',v_identity->>'family_booking_id',
        'current_head_id',v_head.id,
        'head_root_timesheet_id',v_head.root_timesheet_id));
  end if;

  -- `found` is not consulted here: the cardinality check above is the fact, and a
  -- `count(*)` query sets `found` true whatever the count is.
  if v_head_count=1 then
    v_authority:='HEAD';
    v_head_id:=v_head.id;
    -- The head already stores every component and its frozen component_sha256.
    select coalesce(pg_catalog.jsonb_agg(
             pg_catalog.jsonb_build_object(
               'component_ordinal',component.component_ordinal,
               'component_id',component.component_id,
               'component_sha256',pg_catalog.encode(component.component_sha256,'hex'),
               'component_kind',component.component_kind,
               'economic_key_type',component.economic_key_type,
               'economic_key_value',component.economic_key_value,
               'component_member_identity',component.component_member_identity,
               'segment_id',component.segment_id,
               'segment_key',component.segment_key,
               'segment_stable_key',component.segment_stable_key,
               'work_date',component.work_date,
               'reference_number',component.reference_number,
               'hours_day',case when component.hours_day is null then null
                 else pg_catalog.to_char(component.hours_day,'FM9999999999990.000000') end,
               'hours_night',case when component.hours_night is null then null
                 else pg_catalog.to_char(component.hours_night,'FM9999999999990.000000') end,
               'hours_sat',case when component.hours_sat is null then null
                 else pg_catalog.to_char(component.hours_sat,'FM9999999999990.000000') end,
               'hours_sun',case when component.hours_sun is null then null
                 else pg_catalog.to_char(component.hours_sun,'FM9999999999990.000000') end,
               'hours_bh',case when component.hours_bh is null then null
                 else pg_catalog.to_char(component.hours_bh,'FM9999999999990.000000') end,
               'additional_code_raw',component.additional_code_raw,
               'unit_count',case when component.unit_count is null then null
                 else pg_catalog.to_char(component.unit_count,'FM9999999999990.000000') end,
               'unit_pay_rate',case when component.unit_pay_rate is null then null
                 else pg_catalog.to_char(component.unit_pay_rate,'FM9999999999990.000000') end,
               'unit_charge_rate',case when component.unit_charge_rate is null then null
                 else pg_catalog.to_char(component.unit_charge_rate,'FM9999999999990.000000') end,
               'expense_code',component.expense_code,
               'pay_ex_vat',pg_catalog.to_char(component.pay_ex_vat,'FM9999999999990.00'),
               'charge_ex_vat',case when component.charge_ex_vat is null then null
                 else pg_catalog.to_char(component.charge_ex_vat,'FM9999999999990.00') end,
               'exclude_from_pay',component.exclude_from_pay,
               'origin',component.origin,
               'movement_id',component.movement_id,
               'movement_group_id',component.movement_group_id
             ) order by component.component_ordinal),'[]'::jsonb)
      into v_hashed
    from public.weekly_source_entitlement_head_components component
    where component.head_id=v_head.id;
    return pg_catalog.jsonb_build_object(
      'ok',true,'code',null,
      'authority',v_authority,'head_id',v_head_id,
      'components',v_hashed,
      'inventory_digest',pg_catalog.encode(v_head.inventory_digest,'hex'),
      'component_count',v_head.component_count
    );
  end if;

  -- No head: the effective entitlement is the root's current authorised
  -- financial snapshot (24 section 4.1 makes the first entitlement the ordinary
  -- TSFIN), mapped through the same composer.
  v_authority:='TSFIN';
  select financial.* into v_financial
  from public.timesheets_financials financial
  where financial.timesheet_id=p_root_timesheet_id and financial.is_current=true;

  if found then
    v_segments:=case
      when pg_catalog.jsonb_typeof(v_financial.invoice_breakdown_json->'segments')='array'
        then v_financial.invoice_breakdown_json->'segments'
      else '[]'::jsonb end;
    select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'work_event_id',expense.work_event_id,
      'source_observation_kind',expense.source_observation_kind,
      'candidate_reimbursement_ex_vat',expense.candidate_reimbursement_ex_vat::text,
      'client_charge_ex_vat',expense.client_charge_ex_vat::text
    ) order by expense.work_event_id),'[]'::jsonb) into v_expenses
    from public.weekly_source_expense_pay_materialisations materialisation
    join public.weekly_expense_authority_generations expense
      on expense.id=materialisation.expense_authority_generation_id
    where materialisation.root_timesheet_id=p_root_timesheet_id
      and materialisation.candidate_timesheet_financial_id=v_financial.id;
  end if;

  v_components:=private.weekly_source_entitlement_components_v1(v_segments,v_expenses);

  -- Hash every component with the COORDINATOR'S own canonicaliser, CONTENT
  -- projection and encoder, so a TSFIN-derived component and the head component
  -- it becomes are equal byte for byte rather than merely similar.
  --
  -- `component_sha256` is a CONTENT identity and deliberately excludes
  -- `component_ordinal`, `movement_id` and `movement_group_id`
  -- (interfaces\PUBLICATION_REQUEST_SHAPE.md revision note and section 5.2):
  -- the ordinal is a component's position inside a head, and a component that
  -- is retained while another moves out from in front of it legitimately
  -- changes position.  That is why the coordinator writes the head row's own
  -- component_sha256 as digest(component_content_v1(canonical)) and compares a
  -- retained component the same way, and it is why the HEAD branch above --
  -- which returns the head row's stored hash -- is content-based.
  --
  -- This branch previously hashed the CANONICAL component, ordinal and all, so
  -- the two branches of this one function disagreed about the same component
  -- and every first publication over a head-less root with a non-empty TSFIN
  -- before-position was refused
  -- WEEKLY_SOURCE_PUBLICATION_RETAINED_COMPONENT_CHANGED.  It also made this
  -- function's own guarantee 2 false.
  select coalesce(pg_catalog.jsonb_agg(
           component_element.value||pg_catalog.jsonb_build_object(
             'component_sha256',pg_catalog.encode(
               private.weekly_source_publication_request_digest_v1(
                 private.weekly_source_publication_component_content_v1(
                   private.weekly_source_publication_component_canonical_v1(
                     component_element.value,'effective_inventory.component'))),'hex'))
           order by (component_element.value->>'component_ordinal')::integer),'[]'::jsonb),
         coalesce(pg_catalog.jsonb_agg(
           pg_catalog.jsonb_build_object(
             'component_ordinal',component_element.value->'component_ordinal',
             'component_id',component_element.value->'component_id')
           order by (component_element.value->>'component_ordinal')::integer),'[]'::jsonb)
    into v_hashed,v_pairs
  from pg_catalog.jsonb_array_elements(v_components) as component_element(value);

  -- The same inventory digest the coordinator writes on a head, so the two
  -- values are directly comparable.
  v_inventory_digest:=pg_catalog.encode(
    private.weekly_source_publication_request_digest_v1(
      pg_catalog.jsonb_build_object('components',v_pairs)),'hex');

  return pg_catalog.jsonb_build_object(
    'ok',true,'code',null,
    'authority',v_authority,'head_id',null,
    'components',v_hashed,
    'inventory_digest',v_inventory_digest,
    'component_count',pg_catalog.jsonb_array_length(v_hashed)
  );
end;
$function$;

-- S8.  The old unique (root_timesheet_id) made a bare `select ... into` keyed
-- on the physical root safe by accident.  With the family key in its place an
-- unqualified singleton read would silently return an ARBITRARY row, and this
-- read decides a pay outcome, so it is replaced by a deterministic fail-closed
-- reader that resolves the family first (proof/34 section 7) and raises rather
-- than choosing.
create or replace function private.weekly_source_target_family_for_root_v1(
  p_root_timesheet_id uuid
) returns public.weekly_exceptional_pay_target_families
language plpgsql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_identity jsonb;
  v_family public.weekly_exceptional_pay_target_families%rowtype;
  v_count integer;
begin
  v_identity:=private.weekly_source_resolve_root_identity_v1(p_root_timesheet_id);
  if coalesce((v_identity->>'ok')::boolean,false) is not true then
    raise exception 'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE'
      using errcode='55000',detail=coalesce(v_identity,'{}'::jsonb)::text;
  end if;

  select pg_catalog.count(*)::integer into v_count
  from public.weekly_exceptional_pay_target_families family
  where pg_catalog.btrim(family.root_family_booking_id)
        =pg_catalog.btrim(v_identity->>'family_booking_id');
  if v_count>1 then
    raise exception 'WEEKLY_SOURCE_TARGET_FAMILY_AMBIGUOUS'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'family_booking_id',v_identity->>'family_booking_id',
              'family_row_count',v_count)::text;
  end if;
  if v_count=0 then
    return null;
  end if;

  select family.* into v_family
  from public.weekly_exceptional_pay_target_families family
  where pg_catalog.btrim(family.root_family_booking_id)
        =pg_catalog.btrim(v_identity->>'family_booking_id');
  return v_family;
end;
$function$;

-- Build the complete I-3 publication request for ONE root.  Everything in it
-- comes from the server: the browser supplies no money, no hours and no head
-- id.  publication_mode and pending_bundle_id are written as the interface
-- says the caller should write them and are overwritten by the coordinator.
create or replace function private.weekly_source_entitlement_proposal_request_v1(
  p_root_timesheet_id uuid,
  p_final_revision_id uuid,
  p_authority_kind text,
  p_decision_bundle_id uuid,
  p_bundle_revision bigint,
  p_head_id uuid,
  p_decision_id uuid,
  p_components jsonb,
  p_selection_method text default 'UNCHANGED'
) returns jsonb
language plpgsql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_identity jsonb;
  v_timesheet public.timesheets%rowtype;
  v_contract public.contracts%rowtype;
  v_revision public.weekly_source_final_revisions%rowtype;
  v_expected_head_id uuid;
  v_components jsonb:=coalesce(p_components,'[]'::jsonb);
  v_count integer:=pg_catalog.jsonb_array_length(v_components);
  v_before_ids jsonb:='[]'::jsonb;
  v_before_digest text;
  v_inventory jsonb;
begin
  if p_authority_kind not in ('PROTECTED','LOCKED_FINAL_SOURCE') then
    raise exception 'WEEKLY_SOURCE_PROPOSAL_AUTHORITY_KIND_INVALID' using errcode='22023';
  end if;
  v_identity:=private.weekly_source_resolve_root_identity_v1(p_root_timesheet_id);
  if coalesce((v_identity->>'ok')::boolean,false) is not true
     or (v_identity->>'canonical_timesheet_id')::uuid is distinct from p_root_timesheet_id then
    raise exception 'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE'
      using errcode='55000',detail=coalesce(v_identity,'{}'::jsonb)::text;
  end if;
  select * into strict v_timesheet from public.timesheets
  where timesheet_id=p_root_timesheet_id;
  -- public.timesheets carries no candidate_id: the Candidate reaches a Timesheet
  -- through its Contract.  contracts.candidate_id is NULLABLE, and a null would
  -- be carried into the I-3 request, cast by the coordinator and handed to the
  -- Candidate serial gate and the I-1 lock set -- the very mechanism that stops
  -- two concurrent publications for one Candidate.  So it fails closed here.
  select * into strict v_contract from public.contracts
  where id=v_timesheet.contract_id;
  if v_contract.candidate_id is null then
    raise exception 'WEEKLY_SOURCE_ROOT_CANDIDATE_MISSING'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'root_timesheet_id',p_root_timesheet_id,
              'contract_id',v_timesheet.contract_id)::text;
  end if;
  select * into strict v_revision from public.weekly_source_final_revisions
  where id=p_final_revision_id;

  -- Decision D9 / interface I-7.  The before-position is NEVER invented here
  -- and never taken from a caller: it is read from the single committed
  -- effective authority through the one named owner, which is the same function
  -- the coordinator uses, so the two can never disagree.
  v_inventory:=private.weekly_source_effective_inventory_v1(p_root_timesheet_id);
  if coalesce((v_inventory->>'ok')::boolean,false) is not true then
    raise exception 'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE'
      using errcode='55000',detail=coalesce(v_inventory,'{}'::jsonb)::text;
  end if;
  select coalesce(pg_catalog.jsonb_agg(component_element.value->'component_id'
           order by component_element.value->>'component_id'),'[]'::jsonb)
    into v_before_ids
  from pg_catalog.jsonb_array_elements(v_inventory->'components') as component_element(value);
  v_before_digest:=v_inventory->>'inventory_digest';
  v_expected_head_id:=(v_inventory->>'head_id')::uuid;

  return pg_catalog.jsonb_build_object(
    'decision_bundle_id',p_decision_bundle_id,
    'pending_bundle_id',null,
    'bundle_revision',p_bundle_revision,
    'candidate_id',v_contract.candidate_id,
    'member_root_ids',pg_catalog.jsonb_build_array(p_root_timesheet_id),
    'member_family_booking_ids',pg_catalog.jsonb_build_array(v_timesheet.booking_id),
    'member_root_versions',pg_catalog.jsonb_build_array(v_timesheet.version),
    'head_ids',pg_catalog.jsonb_build_array(p_head_id),
    'decision_id',p_decision_id,
    'publication_mode','IMMEDIATE',
    'financial_request',pg_catalog.jsonb_build_object(
      'source_revision',pg_catalog.jsonb_build_object(
        'final_revision_id',v_revision.id,
        'source_cycle_id',v_revision.source_cycle_id,
        'revision_number',v_revision.revision_number,
        'manifest_hash',pg_catalog.encode(v_revision.manifest_hash,'hex'),
        'policy_fingerprint',pg_catalog.encode(v_revision.policy_fingerprint,'hex')
      ),
      'contract_choices',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object(
          'root_ordinal',1,
          'contract_id',v_timesheet.contract_id,
          'week_ending_date',v_timesheet.week_ending_date,
          'selection_method',p_selection_method
        )
      ),
      'member_entitlements',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object(
          'root_ordinal',1,
          'authority_kind',p_authority_kind,
          'certified_zero',v_count=0,
          'component_count',v_count,
          'components',v_components
        )
      )
    ),
    'control',pg_catalog.jsonb_build_object(
      'bundle_kind','SINGLE_ROOT',
      'reason','WEEKLY_SOURCE_ENTITLEMENT_PUBLICATION',
      'expected_current_head_ids',pg_catalog.jsonb_build_array(v_expected_head_id),
      'before_positions',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object(
          'root_ordinal',1,
          'component_ids',v_before_ids,
          'inventory_digest',v_before_digest
        )
      ),
      'moved_component_ids','[]'::jsonb,
      'target_root_authorisation',null,
      'whole_root_office_review',null
    )
  );
end;
$function$;

-- ---------------------------------------------------------------------------
-- The TWO-ROOT, cross-Contract A-to-B builder (24 section 4.5; handoff N6).
--
-- 24 section 4.5 makes an A-to-B correction ONE atomic decision over TWO roots,
-- and its step 3 defines both post-decision heads in terms of the PREVIOUSLY
-- EFFECTIVE entitlement and nothing else:
--
--   "A-after is A's previously effective complete entitlement minus only the
--    exact moved component(s).  B-after is B's previously effective complete
--    entitlement plus only those component(s).  A is certified zero only when
--    no component remains.  An already-authorised B root keeps every existing
--    component."
--
-- So this builder RE-PRICES NOTHING and invents no figure.  Both members are
-- assembled from the two interface I-7 before-positions, and I-7 is itself
-- produced by the ONE component composer above
-- (private.weekly_source_entitlement_components_v1) on its TSFIN branch and by
-- the committed head that composer produced on its HEAD branch.  The moved
-- component objects are carried across VERBATIM from A's before-position, so a
-- component that moves from Contract A to Contract B keeps the SAME
-- component_id, the same hours and the same money on both sides.  That is what
-- makes it a move rather than a delete and an add, and it is what lets the
-- coordinator state H2-024's set proofs at all.
--
-- There is therefore exactly ONE path to an entitlement figure.  This is an
-- EXTENSION of the existing composer, never a second one: the single-root
-- builder above is untouched and still produces a byte-identical request.
--
-- Two things are DERIVED here rather than generated, because WP-11b's proposal
-- view rebuilds the request and compares its digest with the accepted
-- decision's before it will display anything: a `gen_random_uuid()` movement
-- identity would make the request irreproducible and the proposal permanently
-- undisplayable.  24 section 4.5 step 3 requires "one immutable identifier per
-- moved component with a non-unique movement_group_id for components that move
-- together"; both are SHA-256-derived from the bundle revision and, for
-- movement_id, the component's own immutable identity.
--
-- Everything the caller could otherwise assert is read from the server instead:
-- the two before-positions and the two expected current heads come from I-7
-- (decision D9), the Candidate comes from each root's Contract, the week and
-- the Contract come from each root Timesheet, and whether B must be authorised
-- is decided from public.weekly_source_root_authorisations, not from a flag.
-- The only economic input is `p_moved_component_ids`, which is Office's
-- decision about WHICH identified components move; it is validated against both
-- before-positions and refused by name when it does not fit.
--
-- STABLE, SECURITY DEFINER, no write.  It is called under the caller's locks,
-- exactly as the single-root builder and I-7 are.
create or replace function private.weekly_source_entitlement_proposal_cross_contract_request_v1(
  p_source_root_timesheet_id uuid,
  p_target_root_timesheet_id uuid,
  p_final_revision_id uuid,
  p_decision_bundle_id uuid,
  p_bundle_revision bigint,
  p_source_head_id uuid,
  p_target_head_id uuid,
  p_decision_id uuid,
  p_moved_component_ids uuid[],
  p_source_authority_kind text default 'LOCKED_FINAL_SOURCE',
  p_target_authority_kind text default 'LOCKED_FINAL_SOURCE',
  p_source_selection_method text default 'UNCHANGED',
  p_target_selection_method text default 'OFFICE_SELECTED',
  p_target_authorisation_actor_user_id uuid default null,
  p_whole_root_reviewed_by_user_id uuid default null,
  p_whole_root_reviewed_at_utc timestamptz default null
) returns jsonb
language plpgsql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_source_timesheet public.timesheets%rowtype;
  v_target_timesheet public.timesheets%rowtype;
  v_source_contract public.contracts%rowtype;
  v_target_contract public.contracts%rowtype;
  v_revision public.weekly_source_final_revisions%rowtype;
  v_identity jsonb;
  v_source_inventory jsonb;
  v_target_inventory jsonb;
  v_source_before uuid[];
  v_target_before uuid[];
  v_moved uuid[];
  v_source_components jsonb;
  v_target_components jsonb;
  v_source_count integer;
  v_target_count integer;
  v_source_before_ids jsonb;
  v_target_before_ids jsonb;
  v_movement_group_id uuid;
  v_target_live_generations integer;
  v_source_live_generations integer;
  v_target_authorisation jsonb:=null;
  v_whole_root_review jsonb:=null;
  v_blank jsonb;
  v_contract_week_rows integer;
  v_contract_week_id uuid;
  v_signature_json jsonb;
  v_signature text;
  v_offending uuid;
begin
  -- ---- 1. the caller's own declarations, before any read ------------------
  -- WP-06c review finding F3.  The bundle identities were never validated, so a
  -- request composed with a null `decision_bundle_id`, `decision_id`,
  -- `final_revision_id` or `bundle_revision`, or with revision 0, was composed
  -- successfully; and because the movement key concatenates them, a null
  -- anywhere made the whole key null and every component of the request derived
  -- the SAME movement identity.  Nothing reached the money - the canonicaliser
  -- refuses the null identities by name and the bundle CHECK refuses revision 0
  -- as a raw 23514 - but this package's own principle is that a request is
  -- refused WHEN IT IS COMPOSED, not when it is published.  This is the first
  -- test in the first step, before any read and before any derivation.
  if p_decision_bundle_id is null
     or p_decision_id is null
     or p_final_revision_id is null
     or p_bundle_revision is null
     or p_bundle_revision<1 then
    raise exception 'WEEKLY_SOURCE_PROPOSAL_IDENTITY_INVALID'
      using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_PROPOSAL_IDENTITY_INVALID',
              'reason','EVERY_BUNDLE_IDENTITY_IS_REQUIRED_AND_THE_REVISION_IS_AT_LEAST_ONE',
              'decision_bundle_id',p_decision_bundle_id,
              'bundle_revision',p_bundle_revision,
              'decision_id',p_decision_id,
              'final_revision_id',p_final_revision_id)::text;
  end if;
  if p_source_authority_kind not in ('PROTECTED','LOCKED_FINAL_SOURCE')
     or p_target_authority_kind not in ('PROTECTED','LOCKED_FINAL_SOURCE') then
    raise exception 'WEEKLY_SOURCE_PROPOSAL_AUTHORITY_KIND_INVALID'
      using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'source_authority_kind',p_source_authority_kind,
              'target_authority_kind',p_target_authority_kind)::text;
  end if;
  if p_source_selection_method not in
       ('AUTO_UNIQUE','OFFICE_SELECTED','DURABLE_LINEAGE','UNCHANGED')
     or p_target_selection_method not in
       ('AUTO_UNIQUE','OFFICE_SELECTED','DURABLE_LINEAGE','UNCHANGED') then
    raise exception 'WEEKLY_SOURCE_PROPOSAL_SELECTION_METHOD_INVALID'
      using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'source_selection_method',p_source_selection_method,
              'target_selection_method',p_target_selection_method)::text;
  end if;
  -- I-3 section 2: member index 1 is A and member index 2 is B, positionally.
  -- Two members means two DIFFERENT roots and two DIFFERENT pre-allocated head
  -- ids; the receipt relation's own constraints say the same thing, and a
  -- duplicate head id is a published money identity reused.
  if p_source_root_timesheet_id is null
     or p_target_root_timesheet_id is null
     or p_source_root_timesheet_id=p_target_root_timesheet_id then
    raise exception 'WEEKLY_SOURCE_PROPOSAL_CROSS_CONTRACT_ROOTS_NOT_DISTINCT'
      using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'source_root_timesheet_id',p_source_root_timesheet_id,
              'target_root_timesheet_id',p_target_root_timesheet_id)::text;
  end if;
  if p_source_head_id is null or p_target_head_id is null
     or p_source_head_id=p_target_head_id then
    raise exception 'WEEKLY_SOURCE_PROPOSAL_HEAD_IDS_INVALID'
      using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'source_head_id',p_source_head_id,
              'target_head_id',p_target_head_id)::text;
  end if;
  -- An absent, empty or repeating move set is refused rather than guessed at.
  -- "The whole entitlement moves" is expressed by naming every component id,
  -- never by omitting the argument.
  if p_moved_component_ids is null
     or pg_catalog.cardinality(p_moved_component_ids)=0
     or pg_catalog.array_position(p_moved_component_ids,null::uuid) is not null
     or private.weekly_source_uuid_array_is_distinct_v1(p_moved_component_ids) is not true then
    raise exception 'WEEKLY_SOURCE_PROPOSAL_CROSS_CONTRACT_MOVE_SET_INVALID'
      using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'reason','MOVE_SET_MUST_BE_A_NON_EMPTY_SET_OF_DISTINCT_IDS',
              'moved_component_ids',pg_catalog.to_jsonb(p_moved_component_ids))::text;
  end if;
  v_moved:=p_moved_component_ids;

  -- ---- 2. both roots, resolved as the single-root builder resolves one -----
  v_identity:=private.weekly_source_resolve_root_identity_v1(p_source_root_timesheet_id);
  if coalesce((v_identity->>'ok')::boolean,false) is not true
     or (v_identity->>'canonical_timesheet_id')::uuid is distinct from p_source_root_timesheet_id then
    raise exception 'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'root_ordinal',1,'identity',coalesce(v_identity,'{}'::jsonb))::text;
  end if;
  v_identity:=private.weekly_source_resolve_root_identity_v1(p_target_root_timesheet_id);
  if coalesce((v_identity->>'ok')::boolean,false) is not true
     or (v_identity->>'canonical_timesheet_id')::uuid is distinct from p_target_root_timesheet_id then
    raise exception 'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'root_ordinal',2,'identity',coalesce(v_identity,'{}'::jsonb))::text;
  end if;

  select * into strict v_source_timesheet from public.timesheets
  where timesheet_id=p_source_root_timesheet_id;
  select * into strict v_target_timesheet from public.timesheets
  where timesheet_id=p_target_root_timesheet_id;
  -- public.timesheets carries no candidate_id: the Candidate reaches a
  -- Timesheet through its Contract, and contracts.candidate_id is NULLABLE.  A
  -- null would be carried into the I-3 request and handed to the Candidate
  -- serial gate and the I-1 lock set, so it fails closed here, exactly as the
  -- single-root builder does.
  select * into strict v_source_contract from public.contracts
  where id=v_source_timesheet.contract_id;
  select * into strict v_target_contract from public.contracts
  where id=v_target_timesheet.contract_id;
  if v_source_contract.candidate_id is null or v_target_contract.candidate_id is null then
    raise exception 'WEEKLY_SOURCE_ROOT_CANDIDATE_MISSING'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'source_contract_id',v_source_timesheet.contract_id,
              'target_contract_id',v_target_timesheet.contract_id)::text;
  end if;
  -- One bundle is one Candidate: candidate_id is a single digest field, and it
  -- is the key of the serial gate that stops two concurrent publications.
  if v_source_contract.candidate_id is distinct from v_target_contract.candidate_id then
    raise exception 'WEEKLY_SOURCE_PROPOSAL_CROSS_CONTRACT_CANDIDATE_MISMATCH'
      using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'source_candidate_id',v_source_contract.candidate_id,
              'target_candidate_id',v_target_contract.candidate_id)::text;
  end if;
  -- "old-Contract/new-Contract": A and B must differ in Contract, which the
  -- decision-bundle relation also asserts
  -- (check target_contract_id is distinct from source_contract_id).
  if v_source_timesheet.contract_id is not distinct from v_target_timesheet.contract_id then
    raise exception 'WEEKLY_SOURCE_PROPOSAL_CROSS_CONTRACT_SAME_CONTRACT'
      using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'contract_id',v_source_timesheet.contract_id)::text;
  end if;
  -- One bundle is one week: the decision-bundle row carries a single
  -- week_ending_date and the coordinator requires both Contract choices and
  -- both root Timesheets to agree with it.
  if v_source_timesheet.week_ending_date is distinct from v_target_timesheet.week_ending_date then
    raise exception 'WEEKLY_SOURCE_PROPOSAL_CROSS_CONTRACT_WEEK_MISMATCH'
      using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'source_week_ending_date',v_source_timesheet.week_ending_date,
              'target_week_ending_date',v_target_timesheet.week_ending_date)::text;
  end if;

  select * into strict v_revision from public.weekly_source_final_revisions
  where id=p_final_revision_id;

  -- ---- 3. both before-positions, from interface I-7 and nowhere else ------
  -- Decision D9 and H2-024: "each before-position is read from the single
  -- committed effective authority under lock".  Two roots are two I-7 reads;
  -- I-7 is per root by contract, so this introduces no second path to a figure.
  v_source_inventory:=private.weekly_source_effective_inventory_v1(p_source_root_timesheet_id);
  if coalesce((v_source_inventory->>'ok')::boolean,false) is not true then
    raise exception 'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'root_ordinal',1,'reason','INTERFACE_I7_REFUSED_THE_ROOT',
              'effective_inventory',coalesce(v_source_inventory,'{}'::jsonb))::text;
  end if;
  v_target_inventory:=private.weekly_source_effective_inventory_v1(p_target_root_timesheet_id);
  if coalesce((v_target_inventory->>'ok')::boolean,false) is not true then
    raise exception 'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'root_ordinal',2,'reason','INTERFACE_I7_REFUSED_THE_ROOT',
              'effective_inventory',coalesce(v_target_inventory,'{}'::jsonb))::text;
  end if;

  select coalesce(pg_catalog.array_agg((component_element.value->>'component_id')::uuid
           order by (component_element.value->>'component_ordinal')::integer),array[]::uuid[])
    into v_source_before
  from pg_catalog.jsonb_array_elements(
         coalesce(v_source_inventory->'components','[]'::jsonb)) as component_element(value);
  select coalesce(pg_catalog.array_agg((component_element.value->>'component_id')::uuid
           order by (component_element.value->>'component_ordinal')::integer),array[]::uuid[])
    into v_target_before
  from pg_catalog.jsonb_array_elements(
         coalesce(v_target_inventory->'components','[]'::jsonb)) as component_element(value);

  -- ---- 4. the move set, checked against both before-positions -------------
  -- These are the same set facts H2-024 makes the coordinator prove again in
  -- the publishing transaction.  Proving them here as well is not duplication
  -- of a money rule: it is the difference between a request that is refused
  -- when it is composed and one that is refused only when it is published.
  select moved_element.value into v_offending
  from pg_catalog.unnest(v_moved) as moved_element(value)
  where not (moved_element.value=any(v_source_before))
  limit 1;
  if v_offending is not null then
    raise exception 'WEEKLY_SOURCE_PROPOSAL_CROSS_CONTRACT_MOVED_NOT_IN_SOURCE'
      using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'component_id',v_offending,
              'source_before',pg_catalog.to_jsonb(v_source_before))::text;
  end if;
  select moved_element.value into v_offending
  from pg_catalog.unnest(v_moved) as moved_element(value)
  where moved_element.value=any(v_target_before)
  limit 1;
  if v_offending is not null then
    raise exception 'WEEKLY_SOURCE_PROPOSAL_CROSS_CONTRACT_MOVED_ALREADY_IN_TARGET'
      using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'component_id',v_offending,
              'target_before',pg_catalog.to_jsonb(v_target_before))::text;
  end if;

  -- Round-5 ruling, Part E, "Partial Contract-to-Contract move": "Not in scope
  -- for this release.  The supported operation is the whole-entitlement move.  A
  -- REQUESTED partial move must take the named fail-closed path and explain that
  -- partial movement is unsupported; it must not approximate the move."
  --
  -- The request is refused HERE, where it is requested, as well as at the
  -- coordinator, which is the gate that decides whether money moves.  Refusing
  -- only at publication would let a partial proposal be composed and RECORDED as
  -- an accepted decision that can never be published - a stuck `PROPOSED` bundle
  -- revision that the operator was told had been recorded, which is the shape of
  -- the defect WP-06c review finding F2 was about.
  --
  -- WHOLE means the source root retains nothing after the move.  Every component
  -- of the source before-position is checked to be in the move set, which - the
  -- two tests above having already established that the move set is a set of
  -- distinct ids drawn from the source before-position and absent from the
  -- target - is the same statement as "A-after is empty" and as "member 1 is
  -- certified zero".
  if pg_catalog.cardinality(v_moved)<>pg_catalog.cardinality(v_source_before) then
    raise exception 'WEEKLY_SOURCE_PROPOSAL_PARTIAL_MOVE_UNSUPPORTED'
      using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_PROPOSAL_PARTIAL_MOVE_UNSUPPORTED',
              'reason','ONLY_A_WHOLE_ENTITLEMENT_MOVE_IS_SUPPORTED_IN_THIS_RELEASE',
              'message','This proposal moves only part of the entitlement from one '
                      ||'Contract to the other and leaves the rest behind. Moving part '
                      ||'of an entitlement is not supported in this release: a '
                      ||'Contract-to-Contract amendment must move the whole entitlement, '
                      ||'so that the old Contract is left holding nothing. Nothing has '
                      ||'been proposed. Either move every component of the old '
                      ||'Contract''s entitlement, or leave the entitlement where it is.',
              'definition','A move is WHOLE when the source root retains nothing after it.',
              'source_before',pg_catalog.to_jsonb(v_source_before),
              'source_before_component_count',pg_catalog.cardinality(v_source_before),
              'moved',pg_catalog.to_jsonb(v_moved),
              'moved_component_count',pg_catalog.cardinality(v_moved),
              'would_remain_on_the_source',pg_catalog.cardinality(v_source_before)
                                          -pg_catalog.cardinality(v_moved))::text;
  end if;

  -- ---- 5. both complete post-decision entitlement vectors ----------------
  -- A-after: A's before-position minus only the moved components, carried
  -- across verbatim, re-ordinalised 1..n with no gap (I-3 section 6.2) and with
  -- every movement identity cleared - no component in A-after may carry one
  -- (H2-024; WEEKLY_SOURCE_PUBLICATION_MOVEMENT_IDENTITY_INVALID).
  -- `component_sha256` is I-7's own field and is not part of the I-3 component
  -- allowlist, so it is removed; leaving it would be
  -- WEEKLY_SOURCE_PUBLICATION_REQUEST_UNKNOWN_FIELD.
  select coalesce(pg_catalog.jsonb_agg(
           (ordered.value-'component_sha256')
           ||pg_catalog.jsonb_build_object(
               'component_ordinal',ordered.new_ordinal::integer,
               'movement_id',null::uuid,
               'movement_group_id',null::uuid)
           order by ordered.new_ordinal),'[]'::jsonb)
    into v_source_components
  from (
    select component_element.value,
           pg_catalog.row_number() over (
             order by (component_element.value->>'component_ordinal')::integer) as new_ordinal
      from pg_catalog.jsonb_array_elements(
             coalesce(v_source_inventory->'components','[]'::jsonb)) as component_element(value)
     where not ((component_element.value->>'component_id')::uuid=any(v_moved))
  ) as ordered;

  -- B-after: B's before-position first, in its own order, then the moved
  -- components in A's order.  An already-authorised B keeps every existing
  -- component (24 section 4.5 step 3), and nothing else appears.
  v_movement_group_id:=private.weekly_source_entitlement_derived_uuid_v1(
    'WEEKLY_SOURCE_ENTITLEMENT_MOVEMENT_GROUP_V1',
    p_decision_bundle_id::text||'|'||p_bundle_revision::text);
  select coalesce(pg_catalog.jsonb_agg(
           (ordered.value-'component_sha256')
           ||pg_catalog.jsonb_build_object(
               'component_ordinal',ordered.new_ordinal::integer,
               'movement_id',ordered.movement_id,
               'movement_group_id',ordered.movement_group_id)
           order by ordered.new_ordinal),'[]'::jsonb)
    into v_target_components
  from (
    select side.value,
           pg_catalog.row_number() over (
             order by side.side_order,(side.value->>'component_ordinal')::integer) as new_ordinal,
           case when side.side_order=2
                then private.weekly_source_entitlement_derived_uuid_v1(
                       'WEEKLY_SOURCE_ENTITLEMENT_MOVEMENT_V1',
                       p_decision_bundle_id::text||'|'||p_bundle_revision::text||'|'
                       ||(side.value->>'component_id'))
                else null::uuid end as movement_id,
           case when side.side_order=2 then v_movement_group_id
                else null::uuid end as movement_group_id
      from (
        select 1 as side_order,component_element.value
          from pg_catalog.jsonb_array_elements(
                 coalesce(v_target_inventory->'components','[]'::jsonb)) as component_element(value)
        union all
        select 2,component_element.value
          from pg_catalog.jsonb_array_elements(
                 coalesce(v_source_inventory->'components','[]'::jsonb)) as component_element(value)
         where (component_element.value->>'component_id')::uuid=any(v_moved)
      ) as side
  ) as ordered;

  v_source_count:=pg_catalog.jsonb_array_length(v_source_components);
  v_target_count:=pg_catalog.jsonb_array_length(v_target_components);
  -- The two set identities 24 section 4.5 step 3 names, restated over what was
  -- actually built rather than over what was intended.
  if v_source_count<>pg_catalog.cardinality(v_source_before)-pg_catalog.cardinality(v_moved)
     or v_target_count<>pg_catalog.cardinality(v_target_before)+pg_catalog.cardinality(v_moved) then
    raise exception 'WEEKLY_SOURCE_PROPOSAL_CROSS_CONTRACT_MOVE_SET_INVALID'
      using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'reason','POST_DECISION_CARDINALITY_DOES_NOT_MATCH_THE_MOVE',
              'source_before',pg_catalog.cardinality(v_source_before),
              'source_after',v_source_count,
              'target_before',pg_catalog.cardinality(v_target_before),
              'target_after',v_target_count,
              'moved',pg_catalog.cardinality(v_moved))::text;
  end if;

  select coalesce(pg_catalog.jsonb_agg(component_element.value->'component_id'
           order by component_element.value->>'component_id'),'[]'::jsonb)
    into v_source_before_ids
  from pg_catalog.jsonb_array_elements(
         coalesce(v_source_inventory->'components','[]'::jsonb)) as component_element(value);
  select coalesce(pg_catalog.jsonb_agg(component_element.value->'component_id'
           order by component_element.value->>'component_id'),'[]'::jsonb)
    into v_target_before_ids
  from pg_catalog.jsonb_array_elements(
         coalesce(v_target_inventory->'components','[]'::jsonb)) as component_element(value);

  -- ---- 6. the target root's authorisation state, decided from the database -
  -- I-3 section 5.4: the coordinator decides for itself whether B is authorised
  -- and refuses a request that disagrees, so the builder reads the same fact
  -- rather than taking an instruction.  Decision D8 puts that fact in
  -- public.weekly_source_root_authorisations, and the test is over the whole
  -- FAMILY because since schema change S8 nothing keyed on the physical root id
  -- alone is unique.
  select pg_catalog.count(*)::integer into v_source_live_generations
  from public.weekly_source_root_authorisations as authorisation_row
  where authorisation_row.withdrawn_at_utc is null
    and (authorisation_row.root_timesheet_id=p_source_root_timesheet_id
         or pg_catalog.btrim(authorisation_row.family_booking_id)
            =pg_catalog.btrim(v_source_timesheet.booking_id));
  if v_source_live_generations=0 then
    -- A head over a root Weekly Source has not authorised would never reach
    -- payroll; the coordinator refuses it as
    -- WEEKLY_SOURCE_PUBLICATION_TARGET_NOT_AUTHORISED, so it is not composed.
    raise exception 'WEEKLY_SOURCE_PROPOSAL_SOURCE_ROOT_NOT_AUTHORISED'
      using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'root_ordinal',1,'root_timesheet_id',p_source_root_timesheet_id,
              'family_booking_id',v_source_timesheet.booking_id)::text;
  end if;

  select pg_catalog.count(*)::integer into v_target_live_generations
  from public.weekly_source_root_authorisations as authorisation_row
  where authorisation_row.withdrawn_at_utc is null
    and (authorisation_row.root_timesheet_id=p_target_root_timesheet_id
         or pg_catalog.btrim(authorisation_row.family_booking_id)
            =pg_catalog.btrim(v_target_timesheet.booking_id));

  if v_target_live_generations=0 then
    -- B must be authorised by this bundle, through interface I-6, so the
    -- request carries the instruction I-3 section 5.4 defines.
    if p_target_authorisation_actor_user_id is null then
      raise exception 'WEEKLY_SOURCE_PROPOSAL_TARGET_AUTHORISATION_ACTOR_REQUIRED'
        using errcode='22023',
              detail=pg_catalog.jsonb_build_object(
                'root_timesheet_id',p_target_root_timesheet_id,
                'family_booking_id',v_target_timesheet.booking_id)::text;
    end if;
    -- The row signature at authorisation, recomputed exactly as the installed
    -- first-authorisation owner recomputes it.  Never a `limit 1`: two
    -- contract_week rows for one Timesheet would otherwise hand the ordinary
    -- Authorise owner an arbitrary signature.
    select pg_catalog.count(*)::integer into v_contract_week_rows
    from public.contract_weeks as contract_week_row
    where contract_week_row.timesheet_id=p_target_root_timesheet_id;
    if v_contract_week_rows>1 then
      raise exception 'WEEKLY_SOURCE_PROPOSAL_TARGET_CONTRACT_WEEK_AMBIGUOUS'
        using errcode='55000',
              detail=pg_catalog.jsonb_build_object(
                'root_timesheet_id',p_target_root_timesheet_id,
                'contract_week_rows',v_contract_week_rows)::text;
    end if;
    if v_contract_week_rows=1 then
      select contract_week_row.id into v_contract_week_id
      from public.contract_weeks as contract_week_row
      where contract_week_row.timesheet_id=p_target_root_timesheet_id;
    end if;
    v_signature_json:=public.timesheet_lifecycle_guard_signature_v1(
      p_target_root_timesheet_id,v_contract_week_id,false);
    v_signature:=nullif(pg_catalog.btrim(coalesce(
      v_signature_json->>'backend_row_signature',
      v_signature_json->>'row_signature','')),'');
    if v_signature is null or pg_catalog.char_length(v_signature)>512 then
      raise exception 'WEEKLY_SOURCE_PROPOSAL_TARGET_ROOT_SIGNATURE_UNAVAILABLE'
        using errcode='55000',
              detail=pg_catalog.jsonb_build_object(
                'root_timesheet_id',p_target_root_timesheet_id)::text;
    end if;
    v_target_authorisation:=pg_catalog.jsonb_build_object(
      'timesheet_id',p_target_root_timesheet_id,
      'expected_row_signature',v_signature,
      'actor_user_id',p_target_authorisation_actor_user_id);

    -- 24 section 4.5 step 4: "If B already exists unauthorised and contains
    -- unrelated content, the bundle must either receive explicit whole-root
    -- Office review of that content or block; it must never silently authorise
    -- B."  The blank test is the coordinator's own owner, so the two cannot
    -- disagree, and a missing review is refused HERE rather than composed into
    -- a request that the coordinator would refuse later.
    v_blank:=private.weekly_source_publication_target_root_blank_v1(
      p_target_root_timesheet_id,v_target_timesheet.booking_id);
    if coalesce((v_blank->>'blank')::boolean,false) is not true then
      if p_whole_root_reviewed_by_user_id is null
         or p_whole_root_reviewed_at_utc is null then
        raise exception 'WEEKLY_SOURCE_PROPOSAL_TARGET_ROOT_REVIEW_REQUIRED'
          using errcode='22023',
                detail=pg_catalog.jsonb_build_object(
                  'reason','TARGET_ROOT_IS_NOT_PROVABLY_BLANK',
                  'blank_check',v_blank)::text;
      end if;
      v_whole_root_review:=pg_catalog.jsonb_build_object(
        'reviewed',true,
        'reviewed_by_user_id',p_whole_root_reviewed_by_user_id,
        'reviewed_at_utc',pg_catalog.to_char(
          p_whole_root_reviewed_at_utc at time zone 'UTC',
          'YYYY-MM-DD"T"HH24:MI:SS.US')||'Z',
        'decision_id',p_decision_id);
    end if;
  end if;

  -- A review only ever answers for a B root this bundle is about to authorise.
  -- Recording one anywhere else would be a control the coordinator never reads,
  -- which is worse than no control at all.
  if v_whole_root_review is null
     and (p_whole_root_reviewed_by_user_id is not null
          or p_whole_root_reviewed_at_utc is not null) then
    raise exception 'WEEKLY_SOURCE_PROPOSAL_WHOLE_ROOT_REVIEW_NOT_APPLICABLE'
      using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'target_live_generations',v_target_live_generations,
              'target_root_is_blank',coalesce(v_blank->'blank','null'::jsonb))::text;
  end if;

  -- ---- 7. the request, in I-3's exact shape ------------------------------
  return pg_catalog.jsonb_build_object(
    'decision_bundle_id',p_decision_bundle_id,
    'pending_bundle_id',null,
    'bundle_revision',p_bundle_revision,
    'candidate_id',v_source_contract.candidate_id,
    'member_root_ids',pg_catalog.jsonb_build_array(
      p_source_root_timesheet_id,p_target_root_timesheet_id),
    'member_family_booking_ids',pg_catalog.jsonb_build_array(
      v_source_timesheet.booking_id,v_target_timesheet.booking_id),
    'member_root_versions',pg_catalog.jsonb_build_array(
      v_source_timesheet.version,v_target_timesheet.version),
    'head_ids',pg_catalog.jsonb_build_array(p_source_head_id,p_target_head_id),
    'decision_id',p_decision_id,
    'publication_mode','IMMEDIATE',
    'financial_request',pg_catalog.jsonb_build_object(
      'source_revision',pg_catalog.jsonb_build_object(
        'final_revision_id',v_revision.id,
        'source_cycle_id',v_revision.source_cycle_id,
        'revision_number',v_revision.revision_number,
        'manifest_hash',pg_catalog.encode(v_revision.manifest_hash,'hex'),
        'policy_fingerprint',pg_catalog.encode(v_revision.policy_fingerprint,'hex')
      ),
      'contract_choices',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object(
          'root_ordinal',1,
          'contract_id',v_source_timesheet.contract_id,
          'week_ending_date',v_source_timesheet.week_ending_date,
          'selection_method',p_source_selection_method
        ),
        pg_catalog.jsonb_build_object(
          'root_ordinal',2,
          'contract_id',v_target_timesheet.contract_id,
          'week_ending_date',v_target_timesheet.week_ending_date,
          'selection_method',p_target_selection_method
        )
      ),
      'member_entitlements',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object(
          'root_ordinal',1,
          'authority_kind',p_source_authority_kind,
          'certified_zero',v_source_count=0,
          'component_count',v_source_count,
          'components',v_source_components
        ),
        pg_catalog.jsonb_build_object(
          'root_ordinal',2,
          'authority_kind',p_target_authority_kind,
          'certified_zero',v_target_count=0,
          'component_count',v_target_count,
          'components',v_target_components
        )
      )
    ),
    'control',pg_catalog.jsonb_build_object(
      'bundle_kind','CROSS_CONTRACT_A_B',
      'reason','WEEKLY_SOURCE_ENTITLEMENT_PUBLICATION',
      'expected_current_head_ids',pg_catalog.jsonb_build_array(
        (v_source_inventory->>'head_id')::uuid,
        (v_target_inventory->>'head_id')::uuid
      ),
      'before_positions',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object(
          'root_ordinal',1,
          'component_ids',v_source_before_ids,
          'inventory_digest',v_source_inventory->>'inventory_digest'
        ),
        pg_catalog.jsonb_build_object(
          'root_ordinal',2,
          'component_ids',v_target_before_ids,
          'inventory_digest',v_target_inventory->>'inventory_digest'
        )
      ),
      'moved_component_ids',(
        select coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(moved_element.value::text)
                 order by moved_element.value::text),'[]'::jsonb)
        from pg_catalog.unnest(v_moved) as moved_element(value)),
      'target_root_authorisation',v_target_authorisation,
      'whole_root_office_review',v_whole_root_review
    )
  );
end;
$function$;

-- Record exactly one PROPOSED decision bundle revision for a request.  This is
-- the row IMPL\interfaces\PUBLICATION_REQUEST_SHAPE.md section 5.0 requires to
-- exist before the coordinator will publish, and proposed_head_ids is where the
-- pre-allocated head ids live (section 3).  The digest is computed through the
-- ONE canonical encoder, never a second implementation (H2-032).
--
-- It records a SINGLE_ROOT bundle and a CROSS_CONTRACT_A_B bundle through the
-- same path, because the bundle row is what BINDS the publication: I-3
-- section 5.0 and the coordinator's own check 2b require the request to equal
-- the accepted decision in every identity the row carries, which for two
-- members includes target_root_family_booking_id, target_root_timesheet_id and
-- target_contract_id, and section 5.5 requires the whole-root Office review to
-- be PERSISTED on the accepted decision rather than asserted in the request.
--
-- All four approval digests are taken with the ONE canonical encoder, in the
-- exact forms I-3 section 5.1a fixes and the coordinator recomputes:
--
--   request_digest          digest(canonical(request,'IMMEDIATE',null))   -- the acceptance digest
--   source_revision_digest  digest(canonical financial_request.source_revision)
--   contract_choice_digest  digest(canonical financial_request.contract_choices)
--   before_inventory_digest digest(before_inventory(control, member_count))
--
-- The last three previously used private.weekly_source_sha256_jsonb_v1 and a
-- raw copy of before_positions[0].inventory_digest.  Those are different values
-- from the ones the coordinator recomputes, so every publication was refused
-- with WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID /
-- APPROVAL_DIGESTS_DISAGREE_WITH_ACCEPTED_DECISION before any write.  The
-- divergence is a parallel-edit one: the coordinator's four-digest binding
-- arrived with its review finding U1 after this recorder was written.
create or replace function private.weekly_source_entitlement_proposal_record_v1(
  p_request jsonb,
  p_agency_id uuid,
  p_contract_id uuid,
  p_week_ending_date date,
  p_actor_user_id uuid
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_canonical jsonb;
  v_digest bytea;
  v_row public.weekly_source_entitlement_decision_bundles%rowtype;
  v_financial jsonb;
  v_head_ids uuid[];
  v_member_count integer;
  v_bundle_kind text;
  v_declared_kind text;
  v_target_family_booking_id text;
  v_target_root_timesheet_id uuid;
  v_target_contract_id uuid;
  v_source_contract_id uuid;
  v_choice_rows integer;
  v_review jsonb;
  v_review_required boolean:=false;
  v_reviewed_by_user_id uuid;
  v_reviewed_at_utc timestamptz;
  v_week_mismatch integer;
  v_revision_chain_rows integer;
  v_revision_agency_id uuid;
  v_i integer;
begin
  v_canonical:=private.weekly_source_publication_request_canonical_v1(
    p_request,'IMMEDIATE',null
  );
  v_digest:=private.weekly_source_publication_request_digest_v1(v_canonical);
  -- Every identity below is taken from the CANONICAL request, so the row and
  -- the digest can never describe two different decisions.
  v_financial:=v_canonical->'financial_request';

  -- WP-06c review finding F4 (pre-existing from WP-06).  `p_agency_id` was
  -- written straight into the bundle row with no check at all: a null produced a
  -- raw 23502 rather than a named refusal, and a WRONG non-null agency was not
  -- detectable by anything installed (`public.timesheets` carries no agency).
  -- The coordinator copies `bundle.agency_id` into every head it writes, so this
  -- is the agency a published entitlement is filed under.  The request already
  -- names the authority the agency can be derived from - the source revision -
  -- so the declared value is checked against the revision's own chain,
  -- `weekly_source_final_revisions` -> `weekly_source_cycles` ->
  -- `weekly_source_groups.agency_id`, and a disagreement is refused by name.
  if p_agency_id is null then
    raise exception 'WEEKLY_SOURCE_PROPOSAL_AGENCY_INVALID'
      using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_PROPOSAL_AGENCY_INVALID',
              'field','p_agency_id','reason','NULL_NOT_ALLOWED')::text;
  end if;
  -- Never a `limit 1`: the chain is three primary keys, and a count that is not
  -- exactly one means the source revision this bundle names cannot be anchored
  -- to one agency, which is a refusal rather than a choice.
  select pg_catalog.count(*)::integer into v_revision_chain_rows
  from public.weekly_source_final_revisions revision_row
  join public.weekly_source_cycles cycle_row
    on cycle_row.id=revision_row.source_cycle_id
  join public.weekly_source_groups group_row
    on group_row.id=cycle_row.source_group_id
  where revision_row.id=(v_financial->'source_revision'->>'final_revision_id')::uuid;
  if v_revision_chain_rows<>1 then
    raise exception 'WEEKLY_SOURCE_PROPOSAL_SOURCE_REVISION_AGENCY_UNRESOLVABLE'
      using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_PROPOSAL_SOURCE_REVISION_AGENCY_UNRESOLVABLE',
              'final_revision_id',v_financial->'source_revision'->'final_revision_id',
              'chain_rows',v_revision_chain_rows)::text;
  end if;
  select group_row.agency_id into strict v_revision_agency_id
  from public.weekly_source_final_revisions revision_row
  join public.weekly_source_cycles cycle_row
    on cycle_row.id=revision_row.source_cycle_id
  join public.weekly_source_groups group_row
    on group_row.id=cycle_row.source_group_id
  where revision_row.id=(v_financial->'source_revision'->>'final_revision_id')::uuid;
  if p_agency_id is distinct from v_revision_agency_id then
    raise exception 'WEEKLY_SOURCE_PROPOSAL_AGENCY_DISAGREES_WITH_SOURCE_REVISION'
      using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_PROPOSAL_AGENCY_DISAGREES_WITH_SOURCE_REVISION',
              'declared_agency_id',p_agency_id,
              'source_revision_agency_id',v_revision_agency_id,
              'final_revision_id',v_financial->'source_revision'->'final_revision_id')::text;
  end if;

  -- 24 section 4.5: the approval is the BOUNDED A/B amendment, so one or two
  -- members and nothing else.  The canonicaliser has already refused three, and
  -- this is the same rule stated over what is about to be WRITTEN.  It is
  -- derived from the CANONICAL request alone - no caller argument - so it is
  -- taken here, before the idempotence lookup, where the whole-root review block
  -- below needs the bundle kind.
  v_member_count:=pg_catalog.jsonb_array_length(v_canonical->'member_root_ids');
  if v_member_count not in (1,2) then
    raise exception 'WEEKLY_SOURCE_PROPOSAL_BUNDLE_UNBOUNDED'
      using errcode='22023',
            detail=pg_catalog.jsonb_build_object('member_count',v_member_count)::text;
  end if;
  v_bundle_kind:=case when v_member_count=2 then 'CROSS_CONTRACT_A_B' else 'SINGLE_ROOT' end;
  -- control.bundle_kind is a caller assertion; the member set is the fact.  A
  -- disagreement is refused rather than reconciled, because the coordinator
  -- cross-checks the two as well.
  v_declared_kind:=p_request#>>'{control,bundle_kind}';
  if v_declared_kind is distinct from v_bundle_kind then
    raise exception 'WEEKLY_SOURCE_PROPOSAL_BUNDLE_KIND_DISAGREES_WITH_MEMBERS'
      using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'declared_bundle_kind',v_declared_kind,
              'member_count',v_member_count,
              'bundle_kind_from_members',v_bundle_kind)::text;
  end if;

  -- Round-5 ruling, Part E: a PARTIAL Contract-to-Contract move is not in scope
  -- for this release.  The builder refuses to compose one and the coordinator
  -- refuses to publish one; this is the third point, and it is the one that stops
  -- a hand-built partial request being RECORDED as an accepted decision that can
  -- never be published - a stuck `PROPOSED` revision the operator was told had
  -- been recorded.  The insert below is the proposal's first write, so this is
  -- the point at which the request is validated.
  --
  -- Statement (iii) of the definition, which is the one the bundle row can be
  -- held to: the source member is certified zero, i.e. it retains nothing.  The
  -- coordinator proves the same thing from the component sets it has already
  -- reconciled, and separately re-checks that `certified_zero` really does equal
  -- `component_count = 0`, so the two readings cannot diverge.
  if v_bundle_kind='CROSS_CONTRACT_A_B' then
    if pg_catalog.jsonb_typeof(
         coalesce(v_financial#>'{member_entitlements,0,certified_zero}','null'::jsonb))<>'boolean'
       or (v_financial#>'{member_entitlements,0,certified_zero}')::boolean is not true
       or coalesce((v_financial#>>'{member_entitlements,0,component_count}')::integer,-1)<>0 then
      raise exception 'WEEKLY_SOURCE_PROPOSAL_PARTIAL_MOVE_UNSUPPORTED'
        using errcode='22023',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_PROPOSAL_PARTIAL_MOVE_UNSUPPORTED',
                'reason','ONLY_A_WHOLE_ENTITLEMENT_MOVE_IS_SUPPORTED_IN_THIS_RELEASE',
                'message','This decision moves only part of the entitlement from one '
                        ||'Contract to the other and leaves the rest behind. Moving part '
                        ||'of an entitlement is not supported in this release: a '
                        ||'Contract-to-Contract amendment must move the whole '
                        ||'entitlement, so that the old Contract is left holding nothing. '
                        ||'Nothing has been recorded. Either move every component of the '
                        ||'old Contract''s entitlement, or leave the entitlement where it is.',
                'definition','A move is WHOLE when the source root retains nothing after it.',
                'source_member_certified_zero',
                  coalesce(v_financial#>'{member_entitlements,0,certified_zero}','null'::jsonb),
                'source_member_component_count',
                  coalesce(v_financial#>'{member_entitlements,0,component_count}','null'::jsonb))::text;
    end if;
  end if;

  -- I-3 section 5.5: the whole-root Office review is part of what Office
  -- ACCEPTED, so it is written when the bundle revision is written and is
  -- immutable afterwards.  A boolean read from JSON is three-valued: an absent
  -- key and a JSON null both make `not value` evaluate to NULL, so absent,
  -- null and non-boolean are all treated as "no review".
  --
  -- The whole block is read and validated BEFORE the idempotence lookup because
  -- the idempotent branch has to compare it (finding F2 below).
  v_review:=case when pg_catalog.jsonb_typeof(
                        coalesce(p_request#>'{control,whole_root_office_review}','null'::jsonb))='object'
                 then p_request#>'{control,whole_root_office_review}' end;
  if v_review is not null then
    if pg_catalog.jsonb_typeof(v_review->'reviewed')<>'boolean'
       or (v_review->'reviewed')::boolean is not true
       or (v_review->>'reviewed_by_user_id') is null
       or (v_review->>'reviewed_at_utc') is null
       or (v_review->>'decision_id') is null
       or (v_review->>'decision_id')::uuid is distinct from (v_canonical->>'decision_id')::uuid then
      raise exception 'WEEKLY_SOURCE_PROPOSAL_WHOLE_ROOT_REVIEW_MALFORMED'
        using errcode='22023',
              detail=pg_catalog.jsonb_build_object('review',v_review)::text;
    end if;
    -- Only a cross-Contract bundle can carry a whole-root review, because the
    -- review is about the TARGET root.  The bundle relation's own
    -- `check (not whole_root_review_required or bundle_kind='CROSS_CONTRACT_A_B')`
    -- is the backstop.
    if v_bundle_kind<>'CROSS_CONTRACT_A_B' then
      raise exception 'WEEKLY_SOURCE_PROPOSAL_WHOLE_ROOT_REVIEW_NOT_APPLICABLE'
        using errcode='22023',
              detail=pg_catalog.jsonb_build_object('bundle_kind',v_bundle_kind)::text;
    end if;
    v_review_required:=true;
    v_reviewed_by_user_id:=(v_review->>'reviewed_by_user_id')::uuid;
    v_reviewed_at_utc:=(v_review->>'reviewed_at_utc')::timestamptz;
  end if;

  select * into v_row
  from public.weekly_source_entitlement_decision_bundles bundle_row
  where bundle_row.request_digest=v_digest;
  if found then
    -- WP-06c review finding F2 (MEDIUM).  Idempotence is keyed on
    -- `request_digest`, and `control.whole_root_office_review` is CONTROL scope,
    -- so it is deliberately not in the digest (I-3 section 0).  A request first
    -- recorded WITHOUT a review and re-recorded WITH one therefore reached this
    -- branch and returned `{ok:true, created:false}` while the bundle row kept
    -- `whole_root_review_required=false`: the Office act that the whole
    -- unauthorised-B path depends on was accepted and then did not exist.  The
    -- system still failed closed - the coordinator later refuses
    -- `THE_ACCEPTED_DECISION_CARRIES_NO_WHOLE_ROOT_OFFICE_REVIEW` - but the
    -- operator was told the proposal was recorded and nobody was told the review
    -- had been dropped.  The mirror case (recorded WITH a review, re-recorded
    -- WITHOUT one) reported success just as silently.
    --
    -- I-3 section 5.5 already rules what to do: "a review taken after a bundle
    -- was proposed is a NEW bundle revision, exactly as a changed request digest
    -- is".  So a disagreement is refused BY NAME, and `created:false` is returned
    -- only when the review facts are equal too.
    if v_review_required is distinct from v_row.whole_root_review_required
       or v_reviewed_by_user_id is distinct from v_row.whole_root_reviewed_by_user_id
       or v_reviewed_at_utc is distinct from v_row.whole_root_reviewed_at_utc then
      raise exception 'WEEKLY_SOURCE_PROPOSAL_REVIEW_DISAGREES_WITH_ACCEPTED_DECISION'
        using errcode='22023',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_PROPOSAL_REVIEW_DISAGREES_WITH_ACCEPTED_DECISION',
                'decision_bundle_id',v_row.decision_bundle_id,
                'bundle_revision',v_row.bundle_revision,
                'state',v_row.state,
                'request_review',pg_catalog.jsonb_build_object(
                  'whole_root_review_required',v_review_required,
                  'reviewed_by_user_id',v_reviewed_by_user_id,
                  'reviewed_at_utc',v_reviewed_at_utc),
                'accepted_decision_review',pg_catalog.jsonb_build_object(
                  'whole_root_review_required',v_row.whole_root_review_required,
                  'reviewed_by_user_id',v_row.whole_root_reviewed_by_user_id,
                  'reviewed_at_utc',v_row.whole_root_reviewed_at_utc),
                'remedy','I-3 section 5.5: a review taken after a bundle was proposed is a NEW bundle revision')::text;
    end if;
    -- Re-composing the same proposal is idempotent, not an error.
    return pg_catalog.jsonb_build_object(
      'ok',true,'created',false,
      'decision_bundle_id',v_row.decision_bundle_id,
      'bundle_revision',v_row.bundle_revision,
      'state',v_row.state,
      'bundle_kind',v_row.bundle_kind,
      'proposed_head_ids',pg_catalog.to_jsonb(v_row.proposed_head_ids),
      'request_digest',pg_catalog.encode(v_row.request_digest,'hex')
    );
  end if;

  -- The Contract choices, by ordinal.  Never a `limit 1`: an ordinal that
  -- appeared twice would otherwise pick an arbitrary Contract for a money row.
  for v_i in 1..v_member_count loop
    select pg_catalog.count(*)::integer into v_choice_rows
    from pg_catalog.jsonb_array_elements(v_financial->'contract_choices') as choice_element(value)
    where (choice_element.value->>'root_ordinal')::integer=v_i;
    if v_choice_rows<>1 then
      raise exception 'WEEKLY_SOURCE_PROPOSAL_CONTRACT_CHOICE_NOT_UNIQUE'
        using errcode='22023',
              detail=pg_catalog.jsonb_build_object(
                'root_ordinal',v_i,'rows',v_choice_rows)::text;
    end if;
  end loop;
  select (choice_element.value->>'contract_id')::uuid into v_source_contract_id
  from pg_catalog.jsonb_array_elements(v_financial->'contract_choices') as choice_element(value)
  where (choice_element.value->>'root_ordinal')::integer=1;
  -- The caller's declared Contract is the SOURCE root's, and it must be the one
  -- the request itself carries, or the bundle row and the request would name
  -- two different Contracts for member 1.
  if p_contract_id is distinct from v_source_contract_id then
    raise exception 'WEEKLY_SOURCE_PROPOSAL_SOURCE_CONTRACT_DISAGREES_WITH_REQUEST'
      using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'declared_contract_id',p_contract_id,
              'request_contract_id',v_source_contract_id)::text;
  end if;
  -- One bundle row carries ONE week, and the coordinator requires every
  -- Contract choice to agree with it.
  select pg_catalog.count(*)::integer into v_week_mismatch
  from pg_catalog.jsonb_array_elements(v_financial->'contract_choices') as choice_element(value)
  where (choice_element.value->>'week_ending_date')::date is distinct from p_week_ending_date;
  if v_week_mismatch>0 then
    raise exception 'WEEKLY_SOURCE_PROPOSAL_WEEK_DISAGREES_WITH_REQUEST'
      using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'declared_week_ending_date',p_week_ending_date,
              'disagreeing_contract_choices',v_week_mismatch)::text;
  end if;

  if v_member_count=2 then
    v_target_family_booking_id:=v_canonical->'member_family_booking_ids'->>1;
    v_target_root_timesheet_id:=(v_canonical->'member_root_ids'->>1)::uuid;
    select (choice_element.value->>'contract_id')::uuid into v_target_contract_id
    from pg_catalog.jsonb_array_elements(v_financial->'contract_choices') as choice_element(value)
    where (choice_element.value->>'root_ordinal')::integer=2;
  end if;

  select coalesce(pg_catalog.array_agg(head_element.value::uuid
           order by head_element.ordinality),array[]::uuid[])
    into v_head_ids
  from pg_catalog.jsonb_array_elements_text(v_canonical->'head_ids')
       with ordinality as head_element(value,ordinality);

  insert into public.weekly_source_entitlement_decision_bundles(
    decision_bundle_id,bundle_revision,agency_id,candidate_id,week_ending_date,
    bundle_kind,source_root_family_booking_id,source_root_timesheet_id,
    source_contract_id,target_root_family_booking_id,target_root_timesheet_id,
    target_contract_id,decision_id,decided_by_user_id,publication_mode,
    request_digest,source_revision_digest,contract_choice_digest,
    before_inventory_digest,proposed_head_ids,
    whole_root_review_required,whole_root_reviewed_by_user_id,
    whole_root_reviewed_at_utc,state
  ) values (
    (v_canonical->>'decision_bundle_id')::uuid,
    (v_canonical->>'bundle_revision')::bigint,
    p_agency_id,
    (v_canonical->>'candidate_id')::uuid,
    p_week_ending_date,
    v_bundle_kind,
    v_canonical->'member_family_booking_ids'->>0,
    (v_canonical->'member_root_ids'->>0)::uuid,
    p_contract_id,
    v_target_family_booking_id,
    v_target_root_timesheet_id,
    v_target_contract_id,
    (v_canonical->>'decision_id')::uuid,
    p_actor_user_id,
    'IMMEDIATE',
    v_digest,
    private.weekly_source_publication_request_digest_v1(v_financial->'source_revision'),
    private.weekly_source_publication_request_digest_v1(v_financial->'contract_choices'),
    private.weekly_source_publication_request_digest_v1(
      private.weekly_source_publication_before_inventory_v1(
        p_request->'control',v_member_count)),
    v_head_ids,
    v_review_required,
    v_reviewed_by_user_id,
    v_reviewed_at_utc,
    'PROPOSED'
  ) returning * into v_row;

  return pg_catalog.jsonb_build_object(
    'ok',true,'created',true,
    'decision_bundle_id',v_row.decision_bundle_id,
    'bundle_revision',v_row.bundle_revision,
    'state',v_row.state,
    'bundle_kind',v_row.bundle_kind,
    'proposed_head_ids',pg_catalog.to_jsonb(v_row.proposed_head_ids),
    'request_digest',pg_catalog.encode(v_row.request_digest,'hex')
  );
end;
$function$;

create or replace function public.weekly_source_ordinary_pay_projection_apply_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_keys text[]:=array[
    'actor_user_id','final_revision_id','idempotency_key','root_timesheet_id',
    'schema_version','service_snapshot'
  ];
  v_actual_keys text[];
  v_actor uuid;
  v_final_revision_id uuid;
  v_root_timesheet_id uuid;
  v_idempotency_key text;
  v_service_snapshot jsonb;
  v_request_hash bytea;
  v_service_snapshot_hash bytea;
  v_source_unit_manifest_hash bytea;
  v_source_expense_manifest_hash bytea;
  v_active_segment_manifest_hash bytea;
  v_server_calculation_fingerprint bytea;
  v_root_before_hash bytea;
  v_root_after_hash bytea;
  v_revision public.weekly_source_final_revisions%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_scope public.weekly_source_report_scopes%rowtype;
  v_upload public.weekly_source_uploads%rowtype;
  v_profile public.weekly_source_format_profiles%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_manifest public.weekly_source_client_manifests%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_contract_week public.contract_weeks%rowtype;
  v_current_financial public.timesheets_financials%rowtype;
  v_receipt public.weekly_source_ordinary_pay_projection_receipts%rowtype;
  v_correction_impact public.weekly_final_source_correction_root_impacts%rowtype;
  v_source_units jsonb;
  v_expected_source_expenses jsonb;
  v_expected_segments jsonb;
  v_expected_actual_schedule jsonb;
  v_expected_rate_source_refs jsonb;
  v_canonical_tsfin jsonb;
  v_source_mode text;
  v_source_profile_kind text;
  v_client_id uuid;
  v_client_count integer;
  v_profile_count integer;
  v_raw_unit_count integer;
  v_scope_movement_count integer;
  v_active_movement_count integer;
  v_ambiguous_event_count integer;
  v_first_negative_only boolean;
  v_target_state text;
  v_target_family public.weekly_exceptional_pay_target_families%rowtype;
  v_live_generation_count integer;
  v_outcome text;
  v_components jsonb;
  v_decision_bundle_id uuid;
  v_decision_id uuid;
  v_head_id uuid;
  v_proposal_request jsonb;
  v_proposal jsonb;
  v_preflight jsonb;
  v_required_path text;
  v_lifecycle_result jsonb;
  v_write_result jsonb;
  v_published_financial_id uuid;
  v_unit record;
  v_expense_unit record;
begin
  if coalesce(
       pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role',''
     )<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  if pg_catalog.jsonb_typeof(p_request)<>'object' then
    raise exception 'WEEKLY_SOURCE_PROJECTION_REQUEST_INVALID' using errcode='22023';
  end if;
  select pg_catalog.array_agg(key order by key) into v_actual_keys
  from pg_catalog.jsonb_object_keys(p_request) key;
  select pg_catalog.array_agg(key order by key) into v_keys
  from pg_catalog.unnest(v_keys) key;
  if v_actual_keys is distinct from v_keys
     or p_request->>'schema_version'<>'WEEKLY_SOURCE_ORDINARY_PAY_PROJECTION_REQUEST_V1'
     or pg_catalog.jsonb_typeof(p_request->'service_snapshot')<>'object' then
    raise exception 'WEEKLY_SOURCE_PROJECTION_REQUEST_CONTRACT_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_final_revision_id:=(p_request->>'final_revision_id')::uuid;
    v_root_timesheet_id:=(p_request->>'root_timesheet_id')::uuid;
  exception when invalid_text_representation then
    raise exception 'WEEKLY_SOURCE_PROJECTION_ID_INVALID' using errcode='22023';
  end;
  v_idempotency_key:=pg_catalog.btrim(coalesce(p_request->>'idempotency_key',''));
  v_service_snapshot:=p_request->'service_snapshot';
  if v_actor is null or v_final_revision_id is null or v_root_timesheet_id is null
     or pg_catalog.char_length(v_idempotency_key) not between 1 and 200 then
    raise exception 'WEEKLY_SOURCE_PROJECTION_REQUIRED_INPUT_MISSING' using errcode='22023';
  end if;
  perform 1 from public.tms_users actor
  where actor.id=v_actor and coalesce(actor.is_active,false);
  if not found then
    raise exception 'WEEKLY_SOURCE_PROJECTION_ACTOR_INVALID' using errcode='42501';
  end if;

  v_request_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_ORDINARY_PAY_PROJECTION_REQUEST_V1',
    p_request-'idempotency_key'
  );
  select * into v_receipt
  from public.weekly_source_ordinary_pay_projection_receipts receipt
  where receipt.idempotency_key=v_idempotency_key
     or receipt.request_hash=v_request_hash
  order by case when receipt.idempotency_key=v_idempotency_key then 0 else 1 end,
           receipt.created_at_utc,receipt.id
  limit 1;
  if found then
    if v_receipt.request_hash is distinct from v_request_hash then
      raise exception 'WEEKLY_SOURCE_PROJECTION_IDEMPOTENCY_COLLISION' using errcode='22023';
    end if;
    return private.weekly_source_ordinary_projection_receipt_json_v1(v_receipt)
      ||pg_catalog.jsonb_build_object('idempotent_replay',true);
  end if;

  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    'WEEKLY_SOURCE_ORDINARY_PROJECTION|'||v_final_revision_id::text||'|'||
    v_root_timesheet_id::text,0
  ));
  select * into v_receipt
  from public.weekly_source_ordinary_pay_projection_receipts receipt
  where receipt.idempotency_key=v_idempotency_key
     or receipt.request_hash=v_request_hash
  limit 1;
  if found then
    if v_receipt.request_hash is distinct from v_request_hash then
      raise exception 'WEEKLY_SOURCE_PROJECTION_IDEMPOTENCY_COLLISION' using errcode='22023';
    end if;
    return private.weekly_source_ordinary_projection_receipt_json_v1(v_receipt)
      ||pg_catalog.jsonb_build_object('idempotent_replay',true);
  end if;

  select * into v_revision from public.weekly_source_final_revisions
  where id=v_final_revision_id for share;
  if not found or v_revision.state<>'CURRENT' then
    raise exception 'WEEKLY_SOURCE_FINAL_REVISION_NOT_CURRENT' using errcode='55000';
  end if;
  select * into strict v_cycle from public.weekly_source_cycles
  where id=v_revision.source_cycle_id for share;
  select * into strict v_upload from public.weekly_source_uploads
  where id=v_revision.upload_id;
  select * into strict v_profile from public.weekly_source_format_profiles
  where id=v_upload.source_format_profile_id;
  select * into strict v_group from public.weekly_source_groups
  where id=v_cycle.source_group_id;
  if v_revision.authority_scope_kind='CYCLE' then
    if v_cycle.current_final_revision_id is distinct from v_revision.id
       or v_upload.report_scope_id is not null then
      raise exception 'WEEKLY_SOURCE_FINAL_REVISION_POINTER_STALE' using errcode='40001';
    end if;
  else
    select * into v_scope from public.weekly_source_report_scopes
    where id=v_revision.report_scope_id for share;
    if not found or v_scope.current_final_revision_id is distinct from v_revision.id
       or v_scope.source_cycle_id is distinct from v_cycle.id
       or v_upload.report_scope_id is distinct from v_scope.id then
      raise exception 'WEEKLY_SOURCE_FINAL_REVISION_POINTER_STALE' using errcode='40001';
    end if;
  end if;
  v_source_profile_kind:=v_profile.final_authority_kind;
  v_source_mode:=case when v_source_profile_kind='NHSP_TRUST_BACKING_REPORT'
    then 'NHSP_WEEKLY' else 'HEALTHROSTER_WEEKLY' end;
  if v_source_profile_kind not in (
       'GENERIC_COMPLETE_SNAPSHOT','NHSP_TRUST_BACKING_REPORT','HEALTHROSTER_ACTUAL_ROWS'
     ) or (v_group.source_family='NHSP') is distinct from (v_source_mode='NHSP_WEEKLY') then
    raise exception 'WEEKLY_SOURCE_FINAL_PROFILE_INVALID' using errcode='55000';
  end if;

  select pg_catalog.count(distinct movement.actual_client_id),
         pg_catalog.count(distinct movement.source_profile_kind),
         pg_catalog.count(*) filter (
           where movement.expense_authority_generation_id is null
             and movement.source_line_kind<>'SOURCE_FIXED_EXPENSE'
         ),
         pg_catalog.count(*),
         (pg_catalog.array_agg(movement.actual_client_id order by movement.id))[1]
  into v_client_count,v_profile_count,v_raw_unit_count,v_scope_movement_count,v_client_id
  from public.weekly_source_billing_movements movement
  where movement.final_revision_id=v_revision.id
    and movement.invoice_timesheet_id=v_root_timesheet_id;
  if v_scope_movement_count=0 then
    select * into v_correction_impact
    from public.weekly_final_source_correction_root_impacts impact
    where impact.replacement_final_revision_id=v_revision.id
      and impact.root_timesheet_id=v_root_timesheet_id;
    if not found or v_revision.reason<>'CORRECT_FINAL_SOURCE'
       or v_correction_impact.source_profile_kind is distinct from v_source_profile_kind
       or v_correction_impact.source_mode is distinct from v_source_mode then
      raise exception 'WEEKLY_SOURCE_PROJECTION_UNIT_SCOPE_INVALID' using errcode='55000';
    end if;
    v_client_id:=v_correction_impact.client_id;
  elsif v_client_count<>1 or v_profile_count<>1
     or exists(
       select 1 from public.weekly_source_billing_movements movement
       where movement.final_revision_id=v_revision.id
         and movement.invoice_timesheet_id=v_root_timesheet_id
         and movement.source_profile_kind is distinct from v_source_profile_kind
     ) then
    raise exception 'WEEKLY_SOURCE_PROJECTION_UNIT_SCOPE_INVALID' using errcode='55000';
  end if;
  select * into v_manifest from public.weekly_source_client_manifests manifest
  where manifest.final_revision_id=v_revision.id and manifest.client_id=v_client_id;
  if not found then
    raise exception 'WEEKLY_SOURCE_CLIENT_MANIFEST_MISSING' using errcode='55000';
  end if;
  if v_scope_movement_count=0 and (
       v_manifest.client_id is distinct from v_correction_impact.client_id
       or v_correction_impact.prior_final_revision_id
            is distinct from v_revision.predecessor_revision_id
       or not exists(
         select 1
         from public.weekly_source_ordinary_pay_projection_receipts prior_receipt
         where prior_receipt.id=v_correction_impact.prior_projection_receipt_id
           and prior_receipt.final_revision_id=v_correction_impact.prior_final_revision_id
           and prior_receipt.root_timesheet_id=v_root_timesheet_id
           and prior_receipt.client_id=v_client_id
           and prior_receipt.outcome in ('PREPARED_FOR_AUTHORISATION','PROPOSED')
       )
     ) then
    raise exception 'WEEKLY_SOURCE_CORRECTION_IMPACT_INVALID' using errcode='55000';
  end if;
  if exists(
    select 1
    from public.weekly_source_billing_movements predecessor_movement
    join public.weekly_source_final_revisions predecessor_revision
      on predecessor_revision.id=predecessor_movement.final_revision_id
     and predecessor_revision.state='CURRENT'
    join public.weekly_source_cycles predecessor_cycle
      on predecessor_cycle.id=predecessor_movement.finalisation_cycle_id
    where predecessor_movement.invoice_timesheet_id=v_root_timesheet_id
      and (
        predecessor_cycle.finalisation_week_ending<v_cycle.finalisation_week_ending
        or (
          predecessor_cycle.finalisation_week_ending=v_cycle.finalisation_week_ending
          and predecessor_revision.finalised_at_utc<v_revision.finalised_at_utc
        )
        or (
          predecessor_cycle.finalisation_week_ending=v_cycle.finalisation_week_ending
          and predecessor_revision.finalised_at_utc=v_revision.finalised_at_utc
          and predecessor_revision.revision_number<v_revision.revision_number
        )
        or (
          predecessor_cycle.finalisation_week_ending=v_cycle.finalisation_week_ending
          and predecessor_revision.finalised_at_utc=v_revision.finalised_at_utc
          and predecessor_revision.revision_number=v_revision.revision_number
          and predecessor_revision.id::text<v_revision.id::text
        )
      )
      and not exists(
        select 1
        from public.weekly_source_ordinary_pay_projection_receipts predecessor_receipt
        where predecessor_receipt.final_revision_id=predecessor_revision.id
          and predecessor_receipt.root_timesheet_id=v_root_timesheet_id
          and predecessor_receipt.outcome in (
            'PREPARED_FOR_AUTHORISATION','PROPOSED','NO_OP_FIRST_NEGATIVE',
            'TARGET_MANAGED_SUPPRESSED'
          )
      )
  ) then
    raise exception 'WEEKLY_SOURCE_PROJECTION_PREDECESSOR_REQUIRED'
      using errcode='55000';
  end if;
  if exists(
    select 1
    from public.weekly_source_ordinary_pay_projection_receipts later_receipt
    join public.weekly_source_final_revisions later_revision
      on later_revision.id=later_receipt.final_revision_id
    join public.weekly_source_cycles later_cycle
      on later_cycle.id=later_receipt.source_cycle_id
    where later_receipt.root_timesheet_id=v_root_timesheet_id
      and later_receipt.outcome in (
        'PREPARED_FOR_AUTHORISATION','PROPOSED','NO_OP_FIRST_NEGATIVE',
        'TARGET_MANAGED_SUPPRESSED'
      )
      and (
        later_cycle.finalisation_week_ending>v_cycle.finalisation_week_ending
        or (
          later_cycle.finalisation_week_ending=v_cycle.finalisation_week_ending
          and later_revision.finalised_at_utc>v_revision.finalised_at_utc
        )
        or (
          later_cycle.finalisation_week_ending=v_cycle.finalisation_week_ending
          and later_revision.finalised_at_utc=v_revision.finalised_at_utc
          and later_revision.revision_number>v_revision.revision_number
        )
        or (
          later_cycle.finalisation_week_ending=v_cycle.finalisation_week_ending
          and later_revision.finalised_at_utc=v_revision.finalised_at_utc
          and later_revision.revision_number=v_revision.revision_number
          and later_revision.id::text>v_revision.id::text
        )
      )
  ) then
    raise exception 'WEEKLY_SOURCE_PROJECTION_LEDGER_ORDER_INVALID'
      using errcode='55000';
  end if;
  v_source_units:=private.weekly_source_ordinary_projection_source_units_v1(
    v_revision.id,v_root_timesheet_id
  );
  if pg_catalog.jsonb_array_length(v_source_units)<>v_raw_unit_count then
    raise exception 'WEEKLY_SOURCE_PROJECTION_MANIFEST_INCOMPLETE' using errcode='55000';
  end if;

  select * into v_timesheet from public.timesheets
  where timesheet_id=v_root_timesheet_id for update;
  if not found or not v_timesheet.is_current
     or v_timesheet.contract_id is null
     or v_timesheet.sheet_scope<>'WEEKLY'::public.timesheet_scope_enum
     or v_timesheet.line_type<>'HOURS'::public.timesheet_line_type_enum
     or v_timesheet.is_adjustment or v_timesheet.revoked_at is not null
     or v_timesheet.archived_at_utc is not null then
    raise exception 'WEEKLY_SOURCE_ROOT_TIMESHEET_INVALID' using errcode='55000';
  end if;
  select * into v_contract_week from public.contract_weeks
  where timesheet_id=v_root_timesheet_id
  order by additional_seq,updated_at desc,id desc limit 1 for update;
  if not found or v_contract_week.additional_seq<>0 or v_contract_week.is_adjustment
     or v_contract_week.contract_id is distinct from v_timesheet.contract_id
     or v_contract_week.week_ending_date is distinct from v_timesheet.week_ending_date
     or v_contract_week.status='CANCELLED'::public.contract_week_status_enum then
    raise exception 'WEEKLY_SOURCE_ROOT_CONTRACT_WEEK_INVALID' using errcode='55000';
  end if;
  if exists(
    select 1 from public.weekly_source_billing_movements movement
    where movement.final_revision_id=v_revision.id
      and movement.invoice_timesheet_id=v_root_timesheet_id
      and (
        movement.actual_client_id is distinct from v_client_id
        or movement.contract_id is distinct from v_timesheet.contract_id
      )
  ) then
    raise exception 'WEEKLY_SOURCE_PROJECTION_ROOT_LINEAGE_MISMATCH' using errcode='55000';
  end if;

  for v_unit in
    select movement.*
    from public.weekly_source_billing_movements movement
    where movement.final_revision_id=v_revision.id
      and movement.invoice_timesheet_id=v_root_timesheet_id
      and movement.expense_authority_generation_id is null
      and movement.source_line_kind<>'SOURCE_FIXED_EXPENSE'
    order by movement.created_at_utc,movement.id
  loop
    if v_unit.source_profile_kind is distinct from v_source_profile_kind
       or v_unit.finalisation_cycle_id is distinct from v_cycle.id
       or pg_catalog.jsonb_typeof(v_unit.canonical_pay_vector_json)<>'object'
       or pg_catalog.jsonb_typeof(v_unit.canonical_charge_vector_json)<>'object'
       or v_unit.canonical_pay_vector_json->>'source_mode' is distinct from v_source_mode
       or v_unit.canonical_charge_vector_json->>'source_mode' is distinct from v_source_mode
       or v_unit.canonical_pay_vector_json->>'kind'<>'PAY'
       or v_unit.canonical_charge_vector_json->>'kind'<>'CHARGE'
       or v_unit.canonical_pay_vector_json->>'calculation_fingerprint'
            is distinct from v_unit.canonical_charge_vector_json->>'calculation_fingerprint'
       or not exists(
         select 1 from public.weekly_source_manifest_movements manifest_movement
         where manifest_movement.client_manifest_id=v_manifest.id
           and manifest_movement.billing_movement_id=v_unit.id
           and manifest_movement.movement_hash=v_unit.movement_economic_hash
       ) then
      raise exception 'WEEKLY_SOURCE_PROJECTION_UNIT_INTEGRITY_FAILED' using errcode='55000';
    end if;
    if v_unit.source_line_kind='GENERATED_HISTORICAL_REVERSAL' then
      if v_unit.prior_movement_id is null or not exists(
        select 1 from public.weekly_source_billing_movements prior
        where prior.id=v_unit.prior_movement_id
          and prior.invoice_timesheet_id=v_root_timesheet_id
          and private.weekly_source_finalisation_negate_vector_v1(
                prior.canonical_pay_vector_json
              )=v_unit.canonical_pay_vector_json
          and private.weekly_source_finalisation_negate_vector_v1(
                prior.canonical_charge_vector_json
              )=v_unit.canonical_charge_vector_json
      ) then
        raise exception 'WEEKLY_SOURCE_PROJECTION_REVERSAL_INTEGRITY_FAILED' using errcode='55000';
      end if;
    elsif coalesce(v_unit.source_facts_json->>'row_resolution_id','')
          !~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       or not exists(
         select 1
         from public.weekly_source_row_resolutions resolution
         join public.weekly_source_row_economic_snapshots economic
           on economic.row_resolution_id=resolution.id
         join public.weekly_source_row_timesheet_lineages lineage
           on lineage.row_resolution_id=resolution.id
         where resolution.id=(v_unit.source_facts_json->>'row_resolution_id')::uuid
           and resolution.work_event_id=v_unit.work_event_id
           and resolution.contract_id=v_unit.contract_id
           and resolution.client_id=v_unit.actual_client_id
           and lineage.timesheet_id=v_root_timesheet_id
           and pg_catalog.encode(economic.calculation_fingerprint,'hex')=
                 v_unit.canonical_pay_vector_json->>'calculation_fingerprint'
           and private.weekly_source_finalisation_vector_v1(
                 economic.id,'PAY',1::smallint
               )=
                 v_unit.canonical_pay_vector_json
           and private.weekly_source_finalisation_vector_v1(
                 economic.id,'CHARGE',1::smallint
               )=
                 v_unit.canonical_charge_vector_json
       ) then
      raise exception 'WEEKLY_SOURCE_PROJECTION_SOURCE_FACT_INTEGRITY_FAILED' using errcode='55000';
    end if;
  end loop;

  -- Source-fixed expenses are a third invoice origin but remain ordinary
  -- candidate pay on this same Weekly root.  Validate every immutable expense
  -- movement independently; an expense-only zero-hours row deliberately has
  -- no worked movement and no segment.
  for v_expense_unit in
    select movement.*
    from public.weekly_source_billing_movements movement
    where movement.final_revision_id=v_revision.id
      and movement.invoice_timesheet_id=v_root_timesheet_id
      and movement.expense_authority_generation_id is not null
      and movement.source_line_kind='SOURCE_FIXED_EXPENSE'
    order by movement.created_at_utc,movement.id
  loop
    if not private.weekly_source_ordinary_projection_expense_movement_assert_v1(
      v_expense_unit.id,v_revision.id,v_cycle.id,v_manifest.id,v_root_timesheet_id
    ) then
      raise exception 'WEEKLY_SOURCE_EXPENSE_MOVEMENT_INTEGRITY_FAILED'
        using errcode='55000';
    end if;
  end loop;

  v_service_snapshot_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_ORDINARY_SERVICE_SNAPSHOT_V1',v_service_snapshot
  );
  v_source_unit_manifest_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_ORDINARY_UNIT_MANIFEST_V1',v_source_units
  );
  v_expected_segments:=private.weekly_source_ordinary_projection_current_segments_v1(
    v_root_timesheet_id,v_revision.id
  );
  v_expected_source_expenses:=
    private.weekly_source_ordinary_projection_current_expenses_v1(
      v_root_timesheet_id,v_revision.id
    );
  v_source_expense_manifest_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_ORDINARY_SOURCE_EXPENSE_MANIFEST_V1',
    v_expected_source_expenses
  );
  -- The completeness check reads the SAME owner the segment composer reads,
  -- so the count and the composed manifest can never be derived from two
  -- different rules.  The block that used to stand here was a second copy of
  -- the row-order ranking and had to be kept in step with it by hand.
  select pg_catalog.count(*) into v_active_movement_count
  from private.weekly_source_ordinary_projection_active_movements_v1(
    v_root_timesheet_id,v_revision.id
  );
  if v_active_movement_count<>pg_catalog.jsonb_array_length(v_expected_segments) then
    raise exception 'WEEKLY_SOURCE_ACTIVE_SEGMENT_MANIFEST_INCOMPLETE'
      using errcode='55000';
  end if;

  -- One work event is one shift, so it has at most one current entitlement
  -- head (14 s6.2.1 to s6.2.3).  Two live source positions of DIFFERENT
  -- economics on one work event are two contradictory statements about the
  -- same shift - the source shape 14 s4.2.8 requires to block at finalisation.
  -- There is no order-free way to choose between them and no rule that says
  -- which is right, so the money fails closed here rather than being settled
  -- by whichever row the Trust happened to list last (standing rule 5).
  select pg_catalog.count(*) into v_ambiguous_event_count
  from (
    select active.work_event_id
    from private.weekly_source_ordinary_projection_active_movements_v1(
      v_root_timesheet_id,v_revision.id
    ) active
    group by active.work_event_id
    having pg_catalog.count(*)>1
  ) ambiguous;
  if v_ambiguous_event_count>0 then
    raise exception 'WEEKLY_SOURCE_ORDINARY_AMBIGUOUS_SOURCE_HEAD'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'root_timesheet_id',v_root_timesheet_id,
              'final_revision_id',v_revision.id,
              'ambiguous_work_event_count',v_ambiguous_event_count
            )::text;
  end if;
  v_expected_actual_schedule:=
    private.weekly_source_ordinary_projection_actual_schedule_v1(v_expected_segments);
  v_active_segment_manifest_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_ORDINARY_ACTIVE_SEGMENTS_V1',v_expected_segments
  );
  v_expected_rate_source_refs:=pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_SOURCE_FINAL_AUTHORITY_RATE_SOURCE_V1',
    'source_mode',v_source_mode,'root_timesheet_id',v_root_timesheet_id,
    'final_revision_id',v_revision.id,
    'final_manifest_hash',pg_catalog.encode(v_revision.manifest_hash,'hex'),
    'final_policy_fingerprint',pg_catalog.encode(v_revision.policy_fingerprint,'hex'),
    'client_manifest_hash',pg_catalog.encode(v_manifest.manifest_hash,'hex'),
    'source_unit_manifest_hash',pg_catalog.encode(v_source_unit_manifest_hash,'hex'),
    'source_expense_manifest_hash',
      pg_catalog.encode(v_source_expense_manifest_hash,'hex'),
    'active_segment_manifest_hash',pg_catalog.encode(v_active_segment_manifest_hash,'hex')
  );
  v_server_calculation_fingerprint:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_ORDINARY_SERVER_CALCULATION_V1',
    pg_catalog.jsonb_build_object(
      'final_revision_id',v_revision.id,'root_timesheet_id',v_root_timesheet_id,
      'source_mode',v_source_mode,'source_units',v_source_units,
      'source_expense_authorities',v_expected_source_expenses,
      'expected_segments',v_expected_segments,
      'expected_actual_schedule',v_expected_actual_schedule,
      'expected_rate_source_refs',v_expected_rate_source_refs,
      'service_snapshot_hash',pg_catalog.encode(v_service_snapshot_hash,'hex')
    )
  );
  v_root_before_hash:=
    private.weekly_source_ordinary_projection_root_hash_v1(v_root_timesheet_id);

  -- S8.  This read decides TARGET_MANAGED_SUPPRESSED on a pay receipt, so it
  -- must never be allowed to pick an arbitrary row once the family is keyed on
  -- the Timesheet family rather than on one physical id (WP-03_NEEDS.md N1.2).
  v_target_family:=private.weekly_source_target_family_for_root_v1(v_root_timesheet_id);
  v_target_state:=v_target_family.ownership_state;
  if v_target_family.id is not null and v_target_state='TARGET_MANAGED' then
    v_receipt:=private.weekly_source_ordinary_projection_receipt_insert_v1(
      v_revision.id,v_cycle.id,v_client_id,v_root_timesheet_id,null,
      v_source_profile_kind,v_source_mode,'TARGET_MANAGED_SUPPRESSED',v_source_units,
      v_source_unit_manifest_hash,v_expected_source_expenses,
      v_source_expense_manifest_hash,v_revision.manifest_hash,v_revision.policy_fingerprint,
      v_service_snapshot_hash,v_server_calculation_fingerprint,v_root_before_hash,
      v_root_before_hash,v_idempotency_key,v_request_hash,v_actor
    );
    perform pg_catalog.set_config(
      'cloudtms.weekly_source_projection_owner',
      'weekly_source_ordinary_pay_projection_apply_atomic_v1',true
    );
    update public.weekly_source_state_transitions transition_row
    set ordinary_source_entitlement_projection_state='PUBLISHED'
    where transition_row.final_revision_id=v_revision.id
      and transition_row.ordinary_source_entitlement_projection_state='PENDING'
      and exists(
        select 1 from public.weekly_source_billing_movements movement
        where movement.transition_id=transition_row.id
          and movement.invoice_timesheet_id=v_root_timesheet_id
      );
    return private.weekly_source_ordinary_projection_receipt_json_v1(v_receipt)
      ||pg_catalog.jsonb_build_object('idempotent_replay',false);
  end if;
  if v_target_family.id is not null and v_target_state<>'ORDINARY_SOURCE' then
    raise exception 'WEEKLY_SOURCE_TARGET_FAMILY_STATE_INVALID' using errcode='55000';
  end if;

  select coalesce(pg_catalog.bool_and(
    coalesce((unit.value->>'first_plan6_negative')::boolean,false)
  ),false) into v_first_negative_only
  from pg_catalog.jsonb_array_elements(v_source_units) unit(value);
  if v_first_negative_only then
    v_receipt:=private.weekly_source_ordinary_projection_receipt_insert_v1(
      v_revision.id,v_cycle.id,v_client_id,v_root_timesheet_id,null,
      v_source_profile_kind,v_source_mode,'NO_OP_FIRST_NEGATIVE',v_source_units,
      v_source_unit_manifest_hash,v_expected_source_expenses,
      v_source_expense_manifest_hash,v_revision.manifest_hash,v_revision.policy_fingerprint,
      v_service_snapshot_hash,v_server_calculation_fingerprint,v_root_before_hash,
      v_root_before_hash,v_idempotency_key,v_request_hash,v_actor
    );
    return private.weekly_source_ordinary_projection_receipt_json_v1(v_receipt)
      ||pg_catalog.jsonb_build_object('idempotent_replay',false);
  end if;

  v_preflight:=public.import_timesheet_financial_preflight_v1(
    array[v_root_timesheet_id],'WEEKLY_SOURCE_ORDINARY_PROJECTION',v_actor,
    '{}'::jsonb,true,1
  );
  v_required_path:=v_preflight->>'required_path';

  v_canonical_tsfin:=private.weekly_source_ordinary_projection_snapshot_assert_v1(
    v_root_timesheet_id,v_source_mode,v_expected_segments,
    v_expected_actual_schedule,v_expected_rate_source_refs,
    v_expected_source_expenses,v_service_snapshot
  );

  -- Has this root ever been authorised?  Two independent signals, and the root
  -- counts as authorised if EITHER says so, because the consequence of getting
  -- this wrong in the permissive direction is the forbidden behaviour itself.
  --   1. a live (non-withdrawn) Weekly Source lineage generation, which is what
  --      proof/34 section 4 records at first authorisation;
  --   2. the installed preflight owner's own classification: it asks for
  --      UNAUTHORISE_AMEND_RECALCULATE_REAUTHORISE precisely when the root is
  --      authorised, and it refuses outright when the root is paid, invoiced or
  --      inside an active Draft.
  -- Decision D8: the authorisation record is per ROOT, in
  -- public.weekly_source_root_authorisations, NOT on the per-source-row lineage
  -- table (which is written before any authorisation exists).
  select pg_catalog.count(*)::integer into v_live_generation_count
  from public.weekly_source_root_authorisations root_authorisation
  where pg_catalog.btrim(root_authorisation.family_booking_id)=pg_catalog.btrim(v_timesheet.booking_id)
    and root_authorisation.withdrawn_at_utc is null;

  if v_live_generation_count=0
     and coalesce((v_preflight->>'allowed')::boolean,false) is true
     and v_required_path='DIRECT_AMEND_RECALCULATE' then

    -- =====================================================================
    -- Initial branch: a genuinely never-authorised root (24 section 4.1;
    -- 27 section 3).  Prepared through the established mutable, unauthorised
    -- Timesheet/TSFIN writers, and NOT authorised here (XSG-001 / G2-0).
    -- =====================================================================

    -- 24 section 4.1: "The Candidate's signed/submitted hours must already be
    -- retained as separate immutable evidence and must remain readable after
    -- preparation.  If the current schema cannot prove that separation,
    -- preparation must fail closed rather than overwrite the only copy of the
    -- Candidate submission."  actual_schedule_json IS the submitted-hours copy
    -- the Office read projection uses, so it may only be overwritten when the
    -- Candidate submission is provably retained elsewhere.
    if v_timesheet.candidate_workflow_id is not null
       and not exists(
         select 1 from public.candidate_submission_workflows workflow
         where workflow.id=v_timesheet.candidate_workflow_id
           and workflow.immutable_submission_json is not null
           and workflow.immutable_submission_sha256 is not null
       ) then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_SUBMISSION_NOT_SEPARATELY_RETAINED'
        using errcode='55000',
              detail=pg_catalog.jsonb_build_object(
                'root_timesheet_id',v_root_timesheet_id,
                'candidate_workflow_id',v_timesheet.candidate_workflow_id)::text;
    end if;

    perform pg_catalog.set_config(
      'cloudtms.lifecycle_mutation_context','manual_timesheet_save',true
    );
    update public.timesheets timesheet_row
    set actual_schedule_json=v_expected_actual_schedule,
        updated_at=pg_catalog.transaction_timestamp()
    where timesheet_row.timesheet_id=v_root_timesheet_id
      and timesheet_row.is_current=true;
    if not found then
      raise exception 'WEEKLY_SOURCE_ROOT_TIMESHEET_CHANGED' using errcode='40001';
    end if;

    v_write_result:=public.tsfin_write_current_snapshot_single_bounded(
      v_root_timesheet_id,v_timesheet.version,v_canonical_tsfin,v_actor,
      pg_catalog.transaction_timestamp()
    );
    if coalesce((v_write_result->>'ok')::boolean,false) is not true
       or coalesce(v_write_result->>'timesheet_financials_id','')
            !~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
      raise exception 'WEEKLY_SOURCE_ORDINARY_TSFIN_WRITE_FAILED'
        using errcode='55000',detail=coalesce(v_write_result,'{}'::jsonb)::text;
    end if;
    v_published_financial_id:=(v_write_result->>'timesheet_financials_id')::uuid;
    perform private.weekly_source_ordinary_projection_expenses_materialise_v1(
      v_root_timesheet_id,v_published_financial_id,v_expected_source_expenses
    );

    -- Deliberately absent: public.timesheet_authorise_generic_atomic.
    -- Finalisation never authorises (XSG-001, G2-0; 24 section 4.1).
    v_outcome:='PREPARED_FOR_AUTHORISATION';
  else

    -- =====================================================================
    -- Later branch: the root is already authorised (24 section 4.2).  Compose
    -- one complete proposed entitlement and record it.  NOTHING on the root is
    -- touched: no unauthorise, no schedule overwrite, no TSFIN rotation, no
    -- reauthorise.  A frozen, paid or invoiced root takes exactly this path;
    -- there is no REFUSED_LOCKED any more.
    -- =====================================================================
    v_components:=private.weekly_source_entitlement_components_v1(
      v_expected_segments,v_expected_source_expenses
    );
    v_decision_bundle_id:=private.weekly_source_entitlement_derived_uuid_v1(
      'WEEKLY_SOURCE_DECISION_BUNDLE_V1',
      pg_catalog.btrim(v_timesheet.booking_id)||'|'||v_revision.id::text
    );
    v_decision_id:=private.weekly_source_entitlement_derived_uuid_v1(
      'WEEKLY_SOURCE_DECISION_V1',v_decision_bundle_id::text||'|1'
    );
    v_head_id:=private.weekly_source_entitlement_derived_uuid_v1(
      'WEEKLY_SOURCE_PROPOSED_HEAD_V1',
      v_decision_bundle_id::text||'|1|'||v_root_timesheet_id::text
    );
    v_proposal_request:=private.weekly_source_entitlement_proposal_request_v1(
      v_root_timesheet_id,v_revision.id,'LOCKED_FINAL_SOURCE',
      v_decision_bundle_id,1::bigint,v_head_id,v_decision_id,v_components
    );
    v_proposal:=private.weekly_source_entitlement_proposal_record_v1(
      v_proposal_request,v_group.agency_id,v_timesheet.contract_id,
      v_timesheet.week_ending_date,v_actor
    );
    v_outcome:='PROPOSED';
  end if;

  perform pg_catalog.set_config(
    'cloudtms.weekly_source_projection_owner',
    'weekly_source_ordinary_pay_projection_apply_atomic_v1',true
  );
  update public.weekly_source_state_transitions transition_row
  set ordinary_source_entitlement_projection_state='PUBLISHED'
  where transition_row.final_revision_id=v_revision.id
    and transition_row.ordinary_source_entitlement_projection_state='PENDING'
    and exists(
      select 1 from public.weekly_source_billing_movements movement
      where movement.transition_id=transition_row.id
        and movement.invoice_timesheet_id=v_root_timesheet_id
    );

  v_root_after_hash:=
    private.weekly_source_ordinary_projection_root_hash_v1(v_root_timesheet_id);
  v_receipt:=private.weekly_source_ordinary_projection_receipt_insert_v1(
    v_revision.id,v_cycle.id,v_client_id,v_root_timesheet_id,v_published_financial_id,
    v_source_profile_kind,v_source_mode,v_outcome,v_source_units,
    v_source_unit_manifest_hash,v_expected_source_expenses,
    v_source_expense_manifest_hash,v_revision.manifest_hash,v_revision.policy_fingerprint,
    v_service_snapshot_hash,v_server_calculation_fingerprint,v_root_before_hash,
    v_root_after_hash,v_idempotency_key,v_request_hash,v_actor
  );
  return private.weekly_source_ordinary_projection_receipt_json_v1(v_receipt)
    ||pg_catalog.jsonb_build_object(
      'idempotent_replay',false,
      'required_path',v_required_path,
      'preflight_fingerprint',v_preflight->>'preflight_fingerprint',
      'proposal',coalesce(v_proposal,'null'::jsonb)
    );
exception
  when no_data_found or too_many_rows then
    raise exception 'WEEKLY_SOURCE_ORDINARY_PROJECTION_SCOPE_INVALID' using errcode='55000';
end;
$function$;

drop function if exists private.weekly_source_ordinary_projection_snapshot_assert_v1(
  uuid,text,jsonb,jsonb,jsonb,jsonb
);

create or replace function private.weekly_source_ordinary_projection_snapshot_assert_v1(
  p_root_timesheet_id uuid,
  p_source_mode text,
  p_expected_segments jsonb,
  p_expected_actual_schedule jsonb,
  p_expected_rate_source_refs jsonb,
  p_expected_source_expenses jsonb,
  p_service_snapshot jsonb
) returns jsonb
language plpgsql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_timesheet public.timesheets%rowtype;
  v_contract public.contracts%rowtype;
  v_current public.timesheets_financials%rowtype;
  v_authority jsonb;
  v_policy jsonb;
  v_tsfin jsonb;
  v_actual jsonb;
  v_expected_basis text;
  v_outer_keys text[]:=array[
    'calculator_owner','schema_version','source_actual_schedule_json','tsfin_snapshot_json'
  ];
  v_tsfin_keys text[]:=array[
    'additional_charge_ex_vat','additional_margin_ex_vat','additional_pay_ex_vat',
    'additional_units_json','band','basis','candidate_assignment','candidate_id',
    'charge_bh','charge_day','charge_night','charge_sat','charge_sun','client_id',
    'expenses_charge_ex_vat','expenses_description','expenses_evidence_manifest',
    'expenses_evidence_r2_key','expenses_pay_ex_vat','hours_bh','hours_day',
    'hours_night','hours_sat','hours_sun','invoice_breakdown_json','margin_ex_vat',
    'mileage_charge_ex_vat','mileage_charge_rate','mileage_evidence_manifest',
    'mileage_evidence_r2_key','mileage_pay_ex_vat','mileage_pay_rate','mileage_units',
    'pay_bh','pay_day','pay_method','pay_night','pay_sat','pay_sun',
    'policy_snapshot_json','processing_status','rate_source_refs_json','role',
    'timesheet_id','timesheet_version','total_charge_ex_vat','total_hours',
    'total_pay_ex_vat'
  ];
  v_actual_outer_keys text[];
  v_actual_tsfin_keys text[];
  v_core_pay numeric:=0;
  v_core_charge numeric:=0;
  v_hours_day numeric:=0;
  v_hours_night numeric:=0;
  v_hours_sat numeric:=0;
  v_hours_sun numeric:=0;
  v_hours_bh numeric:=0;
  v_additional_pay numeric:=0;
  v_additional_charge numeric:=0;
  v_additional_margin numeric:=0;
  v_expenses_pay numeric:=0;
  v_expenses_charge numeric:=0;
  v_expected_expenses_description text;
  v_expected_expenses_evidence_r2_key text;
  v_expected_expenses_evidence_manifest jsonb;
  v_mileage_pay numeric:=0;
  v_mileage_charge numeric:=0;
  v_expected_total_pay numeric:=0;
  v_expected_total_charge numeric:=0;
  v_expected_margin numeric:=0;
  v_wage_pay numeric:=0;
  v_reimbursement_pay numeric:=0;
  v_erni_pct numeric:=0;
  v_erni_multiplier numeric:=1;
  v_first_segment jsonb;
  v_invoice_breakdown jsonb;
begin
  if p_source_mode not in ('NHSP_WEEKLY','HEALTHROSTER_WEEKLY')
     or pg_catalog.jsonb_typeof(p_expected_segments)<>'array'
     or pg_catalog.jsonb_typeof(p_expected_actual_schedule)<>'array'
     or pg_catalog.jsonb_typeof(p_expected_rate_source_refs)<>'object'
     or pg_catalog.jsonb_typeof(p_expected_source_expenses)<>'array'
     or pg_catalog.jsonb_typeof(p_service_snapshot)<>'object' then
    raise exception 'WEEKLY_SOURCE_SERVICE_SNAPSHOT_INVALID' using errcode='22023';
  end if;

  select pg_catalog.array_agg(key order by key) into v_actual_outer_keys
  from pg_catalog.jsonb_object_keys(p_service_snapshot) key;
  select pg_catalog.array_agg(key order by key) into v_outer_keys
  from pg_catalog.unnest(v_outer_keys) key;
  if v_actual_outer_keys is distinct from v_outer_keys
     or p_service_snapshot->>'schema_version'
          <>'WEEKLY_SOURCE_ORDINARY_TSFIN_SERVICE_SNAPSHOT_V1'
     or p_service_snapshot->>'calculator_owner'
          <>'buildWeeklyScheduleSegmentsSnapshot'
     or pg_catalog.jsonb_typeof(p_service_snapshot->'source_actual_schedule_json')<>'array'
     or pg_catalog.jsonb_typeof(p_service_snapshot->'tsfin_snapshot_json')<>'object' then
    raise exception 'WEEKLY_SOURCE_SERVICE_SNAPSHOT_CONTRACT_INVALID' using errcode='22023';
  end if;
  v_actual:=p_service_snapshot->'source_actual_schedule_json';
  v_tsfin:=p_service_snapshot->'tsfin_snapshot_json';

  select pg_catalog.array_agg(key order by key) into v_actual_tsfin_keys
  from pg_catalog.jsonb_object_keys(v_tsfin) key;
  select pg_catalog.array_agg(key order by key) into v_tsfin_keys
  from pg_catalog.unnest(v_tsfin_keys) key;
  if v_actual_tsfin_keys is distinct from v_tsfin_keys then
    raise exception 'WEEKLY_SOURCE_TSFIN_SNAPSHOT_KEYS_INVALID' using errcode='22023';
  end if;

  select * into v_timesheet
  from public.timesheets where timesheet_id=p_root_timesheet_id;
  if not found or not v_timesheet.is_current
     or v_timesheet.sheet_scope<>'WEEKLY'::public.timesheet_scope_enum
     or v_timesheet.line_type<>'HOURS'::public.timesheet_line_type_enum
     or v_timesheet.is_adjustment or v_timesheet.revoked_at is not null
     or v_timesheet.archived_at_utc is not null then
    raise exception 'WEEKLY_SOURCE_ROOT_TIMESHEET_INVALID' using errcode='55000';
  end if;
  select * into strict v_contract
  from public.contracts where id=v_timesheet.contract_id;
  select * into v_current
  from public.timesheets_financials financial
  where financial.timesheet_id=p_root_timesheet_id and financial.is_current=true
  order by financial.computed_at_utc desc nulls last,
           financial.updated_at desc nulls last,financial.id desc
  limit 1;

  v_authority:=private._timesheet_settings_authority_frozen_v1(p_root_timesheet_id);
  -- The established settings owner includes the read timestamp in its envelope.
  -- It is audit metadata, not an economic input, so remove it from the exact
  -- service/server contract while retaining every source identifier and the
  -- stable authority fingerprint.
  v_policy:=(v_authority->'values')-'resolved_at_utc';
  if pg_catalog.jsonb_typeof(v_policy)<>'object' then
    raise exception 'WEEKLY_SOURCE_ROOT_SETTINGS_AUTHORITY_INVALID' using errcode='55000';
  end if;
  v_expected_basis:=case when p_source_mode='NHSP_WEEKLY'
    then 'NHSP' else 'HEALTHROSTER_SELF_BILL' end;

  if v_actual is distinct from p_expected_actual_schedule
     or v_tsfin->'rate_source_refs_json' is distinct from p_expected_rate_source_refs
     or v_tsfin->'policy_snapshot_json' is distinct from v_policy
     or coalesce(v_tsfin->>'timesheet_id','') is distinct from p_root_timesheet_id::text
     or coalesce(v_tsfin->>'timesheet_version','')!~'^[1-9][0-9]*$'
     or (v_tsfin->>'timesheet_version')::integer is distinct from v_timesheet.version
     or v_tsfin->>'basis' is distinct from v_expected_basis
     or coalesce(v_tsfin->>'candidate_id','') is distinct from v_contract.candidate_id::text
     or coalesce(v_tsfin->>'client_id','') is distinct from v_contract.client_id::text
     or v_tsfin->>'role' is distinct from v_contract.role
     or v_tsfin->>'band' is distinct from v_contract.band
     or pg_catalog.upper(coalesce(v_tsfin->>'pay_method',''))
          is distinct from pg_catalog.upper(coalesce(v_contract.pay_method_snapshot,''))
     or v_tsfin->>'candidate_assignment'<>'ASSIGNED'
     or v_tsfin->>'processing_status'<>'PENDING_AUTH' then
    raise exception 'WEEKLY_SOURCE_TSFIN_IDENTITY_OR_POLICY_MISMATCH' using errcode='22023';
  end if;

  v_invoice_breakdown:=v_tsfin->'invoice_breakdown_json';
  if pg_catalog.jsonb_typeof(v_invoice_breakdown)<>'object'
     or v_invoice_breakdown->>'mode'<>'SEGMENTS'
     or v_invoice_breakdown->'segments' is distinct from p_expected_segments
     or pg_catalog.jsonb_typeof(v_invoice_breakdown->'additional')<>'object'
     or pg_catalog.jsonb_typeof(v_invoice_breakdown->'totals')<>'object' then
    raise exception 'WEEKLY_SOURCE_TSFIN_SEGMENT_MANIFEST_MISMATCH' using errcode='22023';
  end if;

  select
    coalesce(pg_catalog.sum((segment.value->>'pay_amount')::numeric),0),
    coalesce(pg_catalog.sum((segment.value->>'charge_amount')::numeric),0),
    coalesce(pg_catalog.sum((segment.value->>'hours_day')::numeric),0),
    coalesce(pg_catalog.sum((segment.value->>'hours_night')::numeric),0),
    coalesce(pg_catalog.sum((segment.value->>'hours_sat')::numeric),0),
    coalesce(pg_catalog.sum((segment.value->>'hours_sun')::numeric),0),
    coalesce(pg_catalog.sum((segment.value->>'hours_bh')::numeric),0)
  into v_core_pay,v_core_charge,v_hours_day,v_hours_night,
       v_hours_sat,v_hours_sun,v_hours_bh
  from pg_catalog.jsonb_array_elements(p_expected_segments) segment(value);

  v_additional_pay:=coalesce(v_current.additional_pay_ex_vat,0);
  v_additional_charge:=coalesce(v_current.additional_charge_ex_vat,0);
  v_additional_margin:=coalesce(v_current.additional_margin_ex_vat,0);
  if pg_catalog.jsonb_array_length(p_expected_source_expenses)>0 then
    if exists(
      select 1
      from pg_catalog.jsonb_array_elements(p_expected_source_expenses) expense(value)
      where pg_catalog.jsonb_typeof(expense.value)<>'object'
        or coalesce(expense.value->>'source_expense_pence','')!~'^[0-9]+$'
        or expense.value->>'candidate_reimbursement_ex_vat'
             is distinct from expense.value->>'client_charge_ex_vat'
        or (expense.value->>'candidate_reimbursement_ex_vat')::numeric
             is distinct from (expense.value->>'source_expense_pence')::numeric/100
    ) then
      raise exception 'WEEKLY_SOURCE_EXPENSE_AUTHORITY_MANIFEST_INVALID'
        using errcode='55000';
    end if;
    select coalesce(pg_catalog.sum((expense.value->>'source_expense_pence')::numeric),0)/100
      into v_expenses_pay
    from pg_catalog.jsonb_array_elements(p_expected_source_expenses) expense(value);
    v_expenses_charge:=v_expenses_pay;
    v_expected_expenses_description:='Source-approved expenses';
    v_expected_expenses_evidence_r2_key:=null;
    v_expected_expenses_evidence_manifest:=pg_catalog.jsonb_build_object(
      'schema_version','WEEKLY_SOURCE_FIXED_EXPENSE_TSFIN_LINEAGE_V1',
      'authorities',p_expected_source_expenses,
      'manifest_hash',pg_catalog.encode(private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_ORDINARY_SOURCE_EXPENSE_MANIFEST_V1',
        p_expected_source_expenses
      ),'hex')
    );
  elsif coalesce(v_current.expenses_evidence_manifest->>'schema_version','')=
        'WEEKLY_SOURCE_FIXED_EXPENSE_TSFIN_LINEAGE_V1' then
    -- A source-fixed expense belongs to the superseded final authority.  When
    -- the prepared replacement contains no source expense, remove that prior
    -- source amount; it must never be reclassified as an ordinary receipt
    -- expense.  Receipt/mileage/additional economics remain unchanged below.
    v_expenses_pay:=0;
    v_expenses_charge:=0;
    v_expected_expenses_description:=null;
    v_expected_expenses_evidence_r2_key:=null;
    v_expected_expenses_evidence_manifest:='null'::jsonb;
  else
    v_expenses_pay:=coalesce(v_current.expenses_pay_ex_vat,0);
    v_expenses_charge:=coalesce(v_current.expenses_charge_ex_vat,0);
    v_expected_expenses_description:=v_current.expenses_description;
    v_expected_expenses_evidence_r2_key:=v_current.expenses_evidence_r2_key;
    v_expected_expenses_evidence_manifest:=coalesce(
      pg_catalog.to_jsonb(v_current.expenses_evidence_manifest),'null'::jsonb
    );
  end if;
  v_mileage_pay:=coalesce(v_current.mileage_pay_ex_vat,0);
  v_mileage_charge:=coalesce(v_current.mileage_charge_ex_vat,0);
  v_expected_total_pay:=pg_catalog.round(
    v_core_pay+v_additional_pay+v_expenses_pay+v_mileage_pay,2
  );
  v_expected_total_charge:=pg_catalog.round(
    v_core_charge+v_additional_charge+v_expenses_charge+v_mileage_charge,2
  );

  v_wage_pay:=pg_catalog.round(v_core_pay+v_additional_pay,2);
  v_reimbursement_pay:=pg_catalog.round(v_expenses_pay+v_mileage_pay,2);
  begin
    v_erni_pct:=coalesce((v_policy->>'erni_pct')::numeric,0);
  exception when invalid_text_representation or numeric_value_out_of_range then
    raise exception 'WEEKLY_SOURCE_TSFIN_POLICY_NUMBER_INVALID' using errcode='22023';
  end;
  if v_erni_pct>0 then
    v_erni_multiplier:=1+case when v_erni_pct>1 then v_erni_pct/100 else v_erni_pct end;
  end if;
  v_expected_margin:=pg_catalog.round(v_expected_total_charge-(
    case when pg_catalog.upper(coalesce(v_contract.pay_method_snapshot,''))='PAYE'
           and pg_catalog.upper(coalesce(v_policy->>'apply_erni_to','PAYE_ONLY'))
             in ('ALL','PAYE_ONLY')
      then pg_catalog.round(v_wage_pay*v_erni_multiplier,2)
      else v_wage_pay end
    +v_reimbursement_pay
  ),2);

  if coalesce((v_tsfin->>'hours_day')::numeric,0) is distinct from pg_catalog.round(v_hours_day,2)
     or coalesce((v_tsfin->>'hours_night')::numeric,0) is distinct from pg_catalog.round(v_hours_night,2)
     or coalesce((v_tsfin->>'hours_sat')::numeric,0) is distinct from pg_catalog.round(v_hours_sat,2)
     or coalesce((v_tsfin->>'hours_sun')::numeric,0) is distinct from pg_catalog.round(v_hours_sun,2)
     or coalesce((v_tsfin->>'hours_bh')::numeric,0) is distinct from pg_catalog.round(v_hours_bh,2)
     or coalesce((v_tsfin->>'total_hours')::numeric,0) is distinct from
          pg_catalog.round(v_hours_day+v_hours_night+v_hours_sat+v_hours_sun+v_hours_bh,2)
     or coalesce((v_tsfin->>'total_pay_ex_vat')::numeric,0)
          is distinct from v_expected_total_pay
     or coalesce((v_tsfin->>'total_charge_ex_vat')::numeric,0)
          is distinct from v_expected_total_charge
     or coalesce((v_tsfin->>'margin_ex_vat')::numeric,0)
          is distinct from v_expected_margin
     or coalesce((v_invoice_breakdown#>>'{totals,total_pay_ex_vat}')::numeric,0)
          is distinct from v_expected_total_pay
     or coalesce((v_invoice_breakdown#>>'{totals,total_charge_ex_vat}')::numeric,0)
          is distinct from v_expected_total_charge
     or coalesce((v_invoice_breakdown#>>'{totals,margin_ex_vat}')::numeric,0)
          is distinct from v_expected_margin then
    raise exception 'WEEKLY_SOURCE_TSFIN_TOTALS_MISMATCH' using errcode='22023';
  end if;

  if v_tsfin->'additional_units_json'
        is distinct from coalesce(v_current.additional_units_json,'{}'::jsonb)
     or coalesce((v_tsfin->>'additional_pay_ex_vat')::numeric,0)
        is distinct from v_additional_pay
     or coalesce((v_tsfin->>'additional_charge_ex_vat')::numeric,0)
        is distinct from v_additional_charge
     or coalesce((v_tsfin->>'additional_margin_ex_vat')::numeric,0)
        is distinct from v_additional_margin
     or v_invoice_breakdown#>'{additional,units}'
        is distinct from coalesce(v_current.additional_units_json,'{}'::jsonb)
     or coalesce((v_invoice_breakdown#>>'{additional,pay_ex_vat}')::numeric,0)
        is distinct from v_additional_pay
     or coalesce((v_invoice_breakdown#>>'{additional,charge_ex_vat}')::numeric,0)
        is distinct from v_additional_charge
     or coalesce((v_invoice_breakdown#>>'{additional,margin_ex_vat}')::numeric,0)
        is distinct from v_additional_margin
     or coalesce((v_tsfin->>'expenses_pay_ex_vat')::numeric,0)
        is distinct from v_expenses_pay
     or coalesce((v_tsfin->>'expenses_charge_ex_vat')::numeric,0)
        is distinct from v_expenses_charge
     or v_tsfin->>'expenses_description' is distinct from v_expected_expenses_description
     or v_tsfin->>'expenses_evidence_r2_key'
        is distinct from v_expected_expenses_evidence_r2_key
     or v_tsfin->'expenses_evidence_manifest'
        is distinct from v_expected_expenses_evidence_manifest
     or coalesce((v_tsfin->>'mileage_units')::numeric,0)
        is distinct from coalesce(v_current.mileage_units,0)
     or coalesce((v_tsfin->>'mileage_pay_ex_vat')::numeric,0)
        is distinct from v_mileage_pay
     or coalesce((v_tsfin->>'mileage_charge_ex_vat')::numeric,0)
        is distinct from v_mileage_charge
     or v_tsfin->>'mileage_evidence_r2_key' is distinct from v_current.mileage_evidence_r2_key
     or v_tsfin->'mileage_evidence_manifest'
        is distinct from coalesce(
          pg_catalog.to_jsonb(v_current.mileage_evidence_manifest),'null'::jsonb
        )
     or nullif(v_tsfin->>'mileage_pay_rate','')::numeric
        is distinct from v_current.mileage_pay_rate
     or nullif(v_tsfin->>'mileage_charge_rate','')::numeric
        is distinct from v_current.mileage_charge_rate then
    raise exception 'WEEKLY_SOURCE_NON_SOURCE_ECONOMICS_CHANGED' using errcode='22023';
  end if;

  v_first_segment:=p_expected_segments->0;
  if v_first_segment is not null and (
       nullif(v_tsfin->>'pay_day','')::numeric is distinct from
         (v_first_segment#>>'{weekly_source,pay_vector,rates,day}')::numeric
       or nullif(v_tsfin->>'pay_night','')::numeric is distinct from
         (v_first_segment#>>'{weekly_source,pay_vector,rates,night}')::numeric
       or nullif(v_tsfin->>'pay_sat','')::numeric is distinct from
         (v_first_segment#>>'{weekly_source,pay_vector,rates,sat}')::numeric
       or nullif(v_tsfin->>'pay_sun','')::numeric is distinct from
         (v_first_segment#>>'{weekly_source,pay_vector,rates,sun}')::numeric
       or nullif(v_tsfin->>'pay_bh','')::numeric is distinct from
         (v_first_segment#>>'{weekly_source,pay_vector,rates,bh}')::numeric
       or nullif(v_tsfin->>'charge_day','')::numeric is distinct from
         (v_first_segment#>>'{weekly_source,charge_vector,rates,day}')::numeric
       or nullif(v_tsfin->>'charge_night','')::numeric is distinct from
         (v_first_segment#>>'{weekly_source,charge_vector,rates,night}')::numeric
       or nullif(v_tsfin->>'charge_sat','')::numeric is distinct from
         (v_first_segment#>>'{weekly_source,charge_vector,rates,sat}')::numeric
       or nullif(v_tsfin->>'charge_sun','')::numeric is distinct from
         (v_first_segment#>>'{weekly_source,charge_vector,rates,sun}')::numeric
       or nullif(v_tsfin->>'charge_bh','')::numeric is distinct from
         (v_first_segment#>>'{weekly_source,charge_vector,rates,bh}')::numeric
     ) then
    raise exception 'WEEKLY_SOURCE_TSFIN_DISPLAY_RATES_MISMATCH' using errcode='22023';
  end if;

  return v_tsfin||pg_catalog.jsonb_build_object(
    'policy_snapshot_json',v_policy,
    'rate_source_refs_json',p_expected_rate_source_refs,
    'invoice_breakdown_json',v_invoice_breakdown,
    'actual_schedule_json',p_expected_actual_schedule,
    'external_source_rows_json',pg_catalog.jsonb_build_object(
      'schema_version','WEEKLY_SOURCE_ORDINARY_EXTERNAL_ROWS_V1',
      'source_mode',p_source_mode,
      'segments',coalesce((select pg_catalog.jsonb_agg(segment.value->'weekly_source'
        order by segment.ordinality)
        from pg_catalog.jsonb_array_elements(p_expected_segments)
          with ordinality segment(value,ordinality)),'[]'::jsonb)
    )
  );
exception
  when invalid_text_representation or numeric_value_out_of_range then
    raise exception 'WEEKLY_SOURCE_SERVICE_SNAPSHOT_NUMBER_INVALID' using errcode='22023';
end;
$function$;

create or replace function private.weekly_source_ordinary_projection_root_hash_v1(
  p_timesheet_id uuid
) returns bytea
language plpgsql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_payload jsonb;
begin
  select pg_catalog.jsonb_build_object(
    'timesheet',pg_catalog.to_jsonb(timesheet_row),
    'contract_week',case when contract_week.id is null then null
      else pg_catalog.to_jsonb(contract_week) end,
    'current_financial',case when current_financial.id is null then null
      else pg_catalog.to_jsonb(current_financial) end
  ) into v_payload
  from public.timesheets timesheet_row
  left join lateral (
    select contract_week_inner.*
    from public.contract_weeks contract_week_inner
    where contract_week_inner.timesheet_id=timesheet_row.timesheet_id
    order by contract_week_inner.additional_seq,contract_week_inner.updated_at desc,
             contract_week_inner.id desc
    limit 1
  ) contract_week on true
  left join lateral (
    select financial.*
    from public.timesheets_financials financial
    where financial.timesheet_id=timesheet_row.timesheet_id
      and financial.is_current=true
    order by financial.computed_at_utc desc nulls last,
             financial.updated_at desc nulls last,financial.id desc
    limit 1
  ) current_financial on true
  where timesheet_row.timesheet_id=p_timesheet_id;

  if v_payload is null then
    raise exception 'WEEKLY_SOURCE_ROOT_TIMESHEET_NOT_FOUND' using errcode='P0002';
  end if;
  return private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_ORDINARY_ROOT_STATE_V1',v_payload
  );
end;
$function$;

create or replace function private.weekly_source_ordinary_projection_source_units_v1(
  p_final_revision_id uuid,
  p_root_timesheet_id uuid
) returns jsonb
language sql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
  select coalesce(pg_catalog.jsonb_agg(
    pg_catalog.jsonb_build_object(
      'manifest_ordinal',manifest_movement.manifest_ordinal,
      'movement_id',movement.id,
      'work_event_id',movement.work_event_id,
      'movement_role',movement.movement_role,
      'source_profile_kind',movement.source_profile_kind,
      'source_line_kind',movement.source_line_kind,
      'movement_economic_hash',pg_catalog.encode(movement.movement_economic_hash,'hex'),
      'mapping_rate_policy_fingerprint',
        pg_catalog.encode(movement.mapping_rate_policy_fingerprint,'hex'),
      'source_facts_hash',pg_catalog.encode(private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_ORDINARY_UNIT_FACTS_V1',movement.source_facts_json
      ),'hex'),
      'first_plan6_negative',(
        movement.source_profile_kind='NHSP_TRUST_BACKING_REPORT'
        and movement.movement_role='REVERSAL'
        and not exists(
          select 1
          from public.weekly_source_billing_movements prior_movement
          join public.weekly_source_final_revisions prior_revision
            on prior_revision.id=prior_movement.final_revision_id
          join public.weekly_source_cycles prior_cycle
            on prior_cycle.id=prior_movement.finalisation_cycle_id
          -- WP-62 (executed on a rotated family): the outstanding positive may
          -- sit on ANOTHER physical member of the Timesheet family, because a
          -- movement keeps the member that was current when its report was
          -- finalised.  Keyed on the physical root alone, a reversal landing
          -- after a rotation was flagged as a first-activation negative, the
          -- projection returned NO_OP_FIRST_NEGATIVE and the Candidate stayed
          -- paid for a shift the Trust had withdrawn.  The search is therefore
          -- keyed on the FAMILY through the established adapter (standing
          -- rule 3); for an unrotated family the adapter returns exactly
          -- {invoice_timesheet_id}, so nothing changes there.
          where prior_movement.invoice_timesheet_id=any(
            private.weekly_source_invoice_family_timesheet_ids_v1(
              movement.invoice_timesheet_id))
            and prior_movement.work_event_id=movement.work_event_id
            and prior_movement.source_profile_kind='NHSP_TRUST_BACKING_REPORT'
            and prior_movement.movement_role in ('POSITIVE','REPLACEMENT')
            and (
              revision.state<>'PREPARED'
              or prior_revision.id is distinct from revision.predecessor_revision_id
            )
            -- The positive must not stand LATER than this negative on the
            -- revision ladder.  A positive in the SAME revision counts,
            -- whatever physical row it occupied: 14 s4.2.5 says row order has
            -- no financial meaning, so this limb must not read
            -- `source_row_ordinal`, which is what it used to compare.
            and (
              prior_cycle.finalisation_week_ending<source_cycle.finalisation_week_ending
              or (
                prior_cycle.finalisation_week_ending=source_cycle.finalisation_week_ending
                and prior_revision.finalised_at_utc<=revision.finalised_at_utc
              )
            )
        )
      )
    ) order by manifest_movement.manifest_ordinal
  ),'[]'::jsonb)
  from public.weekly_source_billing_movements movement
  join public.weekly_source_manifest_movements manifest_movement
    on manifest_movement.billing_movement_id=movement.id
  join public.weekly_source_client_manifests client_manifest
    on client_manifest.id=manifest_movement.client_manifest_id
   and client_manifest.final_revision_id=movement.final_revision_id
   and client_manifest.client_id=movement.actual_client_id
  join public.weekly_source_final_revisions revision
    on revision.id=movement.final_revision_id
  join public.weekly_source_cycles source_cycle
    on source_cycle.id=movement.finalisation_cycle_id
  where movement.final_revision_id=p_final_revision_id
    and movement.invoice_timesheet_id=p_root_timesheet_id
    and movement.expense_authority_generation_id is null
    and movement.source_line_kind<>'SOURCE_FIXED_EXPENSE';
$function$;

-- ---------------------------------------------------------------------------
-- The live source position of every work event on one ordinary Weekly root,
-- read through one final revision.  This is the single owner of "which source
-- movements are the Candidate's current entitlement"; the segment composer and
-- the projection owner's own completeness check both read it, so the two can
-- never disagree.
--
-- WHY THIS IS NOT A RANKING.  The superseded rule kept the rank-1 movement per
-- work event ordered by `source_row_ordinal desc`.  That was written for the
-- HealthRoster paired `AMEND`, where the generated REVERSAL and REPLACEMENT
-- share one transition and the role priority settles them.  NHSP supplies its
-- reversal and its re-issue as INDEPENDENT physical rows (14 s4.2.4) which,
-- since durable work identity became the compatible schedule rather than the
-- Reference Number (24 s9), land on the SAME work event.  The rank rule then
-- let the physical order of the Trust's spreadsheet decide the Candidate's
-- pay: reversal listed second, or arriving in a later report, made the event
-- rank-1 a REVERSAL and paid the Candidate nothing for a shift the Client was
-- still invoiced for.  14 s4.2.5 is explicit: "Row order has no financial
-- meaning. A negative and a later positive may appear in different reports and
-- cycles."  Standing rule 5 forbids carrying a safety property in an
-- `order by`.
--
-- WHAT REPLACES IT.  A positive is live unless something supersedes it:
--
--   1. EXPLICIT LINEAGE.  A generated REVERSAL, and the REPLACEMENT that
--      accompanies it, name the movement they supersede in prior_movement_id
--      (private.weekly_source_finalisation_insert_reversal_v1).  The
--      HealthRoster and generic paired shapes therefore need no inference at
--      all and are decided exactly as before.
--
--   2. NHSP PHYSICAL FULL NEGATIVE.  It carries no prior_movement_id, so the
--      binding is derived: 14 s5.5.4 and s4.3.8 say a valid full reversal
--      "updates entitlement only for the lineage it reverses" and only when it
--      "binds uniquely to post-activation source/protected lineage".  The
--      lineage it reverses is the outstanding positive on the SAME work event
--      whose CloudTMS-calculated economics it mirrors - the worked interval and
--      the absolute pay and charge vectors, never the source pence, which is
--      invoice-only (14 s5.5.5).  Matching is by COUNT within that economic
--      magnitude, and a reversal can only bind to a positive whose final
--      revision is not LATER than its own, so a negative that arrives before
--      any matching positive stays a historical reversal and leaves pay
--      untouched (14 s4.3.8).  No ordinal, no arrival order, no `limit 1`.
--
-- Several live positives of the SAME economic magnitude are one position
-- re-reported, so exactly one of them is returned and the money is identical
-- whichever it is.  Live positives of DIFFERENT magnitudes on one work event
-- are two contradictory positions for one shift; they are all returned, and
-- the projection owner refuses rather than choosing between them.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_ordinary_projection_active_movements_v1(
  p_root_timesheet_id uuid,
  p_through_final_revision_id uuid
) returns setof public.weekly_source_billing_movements
language sql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
  with family as materialized (
    -- WP-62 (WP-57 handoff N1, executed): a Timesheet family can carry more
    -- than one physical member after a rotation, and a movement keeps the
    -- member that was current when its report was finalised.  Pairing a full
    -- negative to its positive must therefore run over the WHOLE family, or a
    -- reversal landing after a rotation never meets the positive it reverses
    -- and the Candidate stays paid for a shift the Trust withdrew (executed:
    -- 8.00 h / GBP 80.00 paid against a GBP 0.00 invoice).  The family is
    -- resolved ONCE through the established adapter - the same one the
    -- protected proposal owner and the invoice owners use - never by a second
    -- resolver.  For an unrotated family it returns exactly
    -- {p_root_timesheet_id}, so every decision below is unchanged there, and
    -- the answer is the same whichever member of a family is asked.
    select private.weekly_source_invoice_family_timesheet_ids_v1(
      p_root_timesheet_id
    ) as timesheet_ids
  ), target as (
    select target_revision.finalised_at_utc,target_revision.revision_number,
           target_cycle.finalisation_week_ending,target_revision.id,
           target_revision.state,target_revision.predecessor_revision_id
    from public.weekly_source_final_revisions target_revision
    join public.weekly_source_cycles target_cycle
      on target_cycle.id=target_revision.source_cycle_id
    where target_revision.id=p_through_final_revision_id
  ), scoped as (
    select
      movement.id,
      movement.work_event_id,
      movement.movement_role,
      movement.source_line_kind,
      movement.prior_movement_id,
      movement.created_at_utc,
      revision.finalised_at_utc,
      revision.revision_number,
      source_cycle.finalisation_week_ending,
      pg_catalog.jsonb_build_object(
        'work_date',movement.source_facts_json->>'work_date',
        'start_at_local',movement.source_facts_json->>'start_at_local',
        'end_at_local',movement.source_facts_json->>'end_at_local',
        'break_minutes',movement.source_facts_json->>'break_minutes',
        'hours_day',pg_catalog.round(pg_catalog.abs(
          (movement.canonical_pay_vector_json#>>'{hours,day}')::numeric),2)::text,
        'hours_night',pg_catalog.round(pg_catalog.abs(
          (movement.canonical_pay_vector_json#>>'{hours,night}')::numeric),2)::text,
        'hours_sat',pg_catalog.round(pg_catalog.abs(
          (movement.canonical_pay_vector_json#>>'{hours,sat}')::numeric),2)::text,
        'hours_sun',pg_catalog.round(pg_catalog.abs(
          (movement.canonical_pay_vector_json#>>'{hours,sun}')::numeric),2)::text,
        'hours_bh',pg_catalog.round(pg_catalog.abs(
          (movement.canonical_pay_vector_json#>>'{hours,bh}')::numeric),2)::text,
        'pay_pence',pg_catalog.abs(
          (movement.canonical_pay_vector_json->>'total_pence')::bigint)::text,
        'charge_pence',pg_catalog.abs(
          (movement.canonical_charge_vector_json->>'total_pence')::bigint)::text
      ) as economic_magnitude
    from public.weekly_source_billing_movements movement
    join public.weekly_source_final_revisions revision
      on revision.id=movement.final_revision_id
    join public.weekly_source_cycles source_cycle
      on source_cycle.id=movement.finalisation_cycle_id
    cross join target
    cross join family
    where movement.invoice_timesheet_id=any(family.timesheet_ids)
      and movement.expense_authority_generation_id is null
      and movement.source_line_kind<>'SOURCE_FIXED_EXPENSE'
      and (revision.state='CURRENT' or revision.id=target.id)
      and (
        target.state<>'PREPARED'
        or revision.id is distinct from target.predecessor_revision_id
      )
      and (
        source_cycle.finalisation_week_ending<target.finalisation_week_ending
        or (
          source_cycle.finalisation_week_ending=target.finalisation_week_ending
          and revision.finalised_at_utc<target.finalised_at_utc
        )
        or (
          source_cycle.finalisation_week_ending=target.finalisation_week_ending
          and revision.finalised_at_utc=target.finalised_at_utc
          and revision.revision_number<target.revision_number
        )
        or (
          source_cycle.finalisation_week_ending=target.finalisation_week_ending
          and revision.finalised_at_utc=target.finalised_at_utc
          and revision.revision_number=target.revision_number
          and revision.id::text<=target.id::text
        )
      )
  ), explicitly_superseded as (
    select distinct scoped.prior_movement_id as movement_id
    from scoped
    where scoped.prior_movement_id is not null
      and scoped.movement_role in ('REVERSAL','REPLACEMENT')
  ), nhsp_physical as (
    select scoped.*
    from scoped
    where scoped.source_line_kind in (
        'NHSP_PHYSICAL_POSITIVE','NHSP_PHYSICAL_FULL_NEGATIVE'
      )
      and scoped.prior_movement_id is null
  ), nhsp_cancelled as (
    -- A positive is bound by a full negative when, inside its own work event
    -- and economic magnitude, the number of reversals standing at or after it
    -- is at least the number of positives standing at or after it.  That is a
    -- pure counting statement over the revision ladder: it is unchanged by any
    -- permutation of the physical rows inside a report, and it is unchanged by
    -- which report a row arrived in as long as the ladder order is the same.
    select positive.id as movement_id
    from nhsp_physical positive
    where positive.movement_role in ('POSITIVE','REPLACEMENT')
      and (
        select pg_catalog.count(*)
        from nhsp_physical reversal
        where reversal.work_event_id=positive.work_event_id
          and reversal.economic_magnitude=positive.economic_magnitude
          and reversal.movement_role='REVERSAL'
          and (reversal.finalisation_week_ending,reversal.finalised_at_utc,
               reversal.revision_number)
              >=(positive.finalisation_week_ending,positive.finalised_at_utc,
                 positive.revision_number)
      )>=(
        select pg_catalog.count(*)
        from nhsp_physical peer
        where peer.work_event_id=positive.work_event_id
          and peer.economic_magnitude=positive.economic_magnitude
          and peer.movement_role in ('POSITIVE','REPLACEMENT')
          and (peer.finalisation_week_ending,peer.finalised_at_utc,
               peer.revision_number)
              >=(positive.finalisation_week_ending,positive.finalised_at_utc,
                 positive.revision_number)
      )
  ), live as (
    select scoped.*
    from scoped
    where scoped.movement_role in ('POSITIVE','REPLACEMENT')
      and not exists(
        select 1 from explicitly_superseded
        where explicitly_superseded.movement_id=scoped.id
      )
      and not exists(
        select 1 from nhsp_cancelled
        where nhsp_cancelled.movement_id=scoped.id
      )
  ), current_head as (
    -- 14 s6.2.2: where no protected version exists, "the latest final source
    -- position supplies the current entitlement".  Latest means latest on the
    -- REVISION ladder - which report, which revision - and nothing else.  A
    -- surviving position from an earlier report is history the later report
    -- has restated; it is not a second shift.  Stated as "no live peer on this
    -- work event stands later", so no `order by` decides it.
    select live.*
    from live
    where not exists(
      select 1
      from live peer
      where peer.work_event_id=live.work_event_id
        and (peer.finalisation_week_ending,peer.finalised_at_utc,
             peer.revision_number)
            >(live.finalisation_week_ending,live.finalised_at_utc,
              live.revision_number)
    )
  ), one_per_position as (
    -- Inside that one latest revision, several live rows of the SAME economic
    -- magnitude are one position stated more than once, so exactly one of them
    -- represents it and the money is identical whichever it is.  The
    -- comparison is a strict total order because id::text is unique, so
    -- exactly one member of each group satisfies "no peer in this group stands
    -- after me".  Rows of DIFFERENT magnitudes survive this step together and
    -- the projection owner refuses them: inside one revision there is no
    -- later-than relation to appeal to, and the only thing that could separate
    -- them is the physical row order, which carries no financial meaning.
    select current_head.id
    from current_head
    where not exists(
      select 1
      from current_head peer
      where peer.work_event_id=current_head.work_event_id
        and peer.economic_magnitude=current_head.economic_magnitude
        and (peer.created_at_utc,peer.id::text)
            >(current_head.created_at_utc,current_head.id::text)
    )
  )
  select movement.*
  from public.weekly_source_billing_movements movement
  join one_per_position on one_per_position.id=movement.id;
$function$;

drop function if exists private.weekly_source_ordinary_projection_current_segments_v1(uuid);

create or replace function private.weekly_source_ordinary_projection_current_segments_v1(
  p_root_timesheet_id uuid,
  p_through_final_revision_id uuid
) returns jsonb
language sql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
  with family as materialized (
    -- WP-62: the live-position owner answers over the whole Timesheet family,
    -- so the lineage a segment is composed from must be found over the same
    -- family.  Keyed on the physical root alone, the composer dropped a live
    -- position bound to an earlier member and the projection owner's
    -- completeness check refused every rotated family that kept working
    -- (executed: WEEKLY_SOURCE_ACTIVE_SEGMENT_MANIFEST_INCOMPLETE).  Same
    -- adapter, resolved once; unrotated families are unchanged.
    select private.weekly_source_invoice_family_timesheet_ids_v1(
      p_root_timesheet_id
    ) as timesheet_ids
  ), active as (
    select *
    from private.weekly_source_ordinary_projection_active_movements_v1(
      p_root_timesheet_id,p_through_final_revision_id
    )
  ), facts as (
    select
      active.*,
      (active.source_facts_json->>'row_resolution_id')::uuid as row_resolution_id
    from active
    where coalesce(active.source_facts_json->>'row_resolution_id','')
      ~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ), shaped as (
    select
      source_row.work_date,
      source_row.start_at_local,
      source_row.end_at_local,
      source_row.break_minutes,
      source_row.external_source_key,
      facts.work_event_id,
      facts.id as movement_id,
      facts.final_revision_id,
      facts.source_profile_kind,
      facts.source_line_kind,
      facts.movement_economic_hash,
      facts.mapping_rate_policy_fingerprint,
      facts.canonical_pay_vector_json,
      facts.canonical_charge_vector_json,
      facts.row_resolution_id,
      economic.id as economic_snapshot_id,
      economic.calculation_fingerprint,
      lineage.lineage_fingerprint
    from facts
    cross join family
    join public.weekly_source_row_resolutions resolution
      on resolution.id=facts.row_resolution_id
     and resolution.work_event_id=facts.work_event_id
     and resolution.mapping_state='RESOLVED'
    join public.weekly_source_upload_rows source_row
      on source_row.id=resolution.upload_row_id
    join public.weekly_source_row_economic_snapshots economic
      on economic.row_resolution_id=resolution.id
     and economic.work_event_id=facts.work_event_id
    join public.weekly_source_row_timesheet_lineages lineage
      on lineage.row_resolution_id=resolution.id
     and lineage.timesheet_id=any(family.timesheet_ids)
    where (facts.canonical_pay_vector_json->>'row_sign')::integer=1
      and (facts.canonical_charge_vector_json->>'row_sign')::integer=1
  )
  select coalesce(pg_catalog.jsonb_agg(
    pg_catalog.jsonb_build_object(
      'segment_id','weekly-source-event:'||shaped.work_event_id::text,
      'date',shaped.work_date,
      'start',pg_catalog.to_char(shaped.start_at_local,'HH24:MI'),
      'end',pg_catalog.to_char(shaped.end_at_local,'HH24:MI'),
      'overnight',shaped.end_at_local::date>shaped.start_at_local::date,
      'break_mins',shaped.break_minutes,
      'ref_num',nullif(pg_catalog.btrim(coalesce(shaped.external_source_key,'')),''),
      'breaks','[]'::jsonb,
      'hours_day',(shaped.canonical_pay_vector_json#>>'{hours,day}')::numeric,
      'hours_night',(shaped.canonical_pay_vector_json#>>'{hours,night}')::numeric,
      'hours_sat',(shaped.canonical_pay_vector_json#>>'{hours,sat}')::numeric,
      'hours_sun',(shaped.canonical_pay_vector_json#>>'{hours,sun}')::numeric,
      'hours_bh',(shaped.canonical_pay_vector_json#>>'{hours,bh}')::numeric,
      'pay_amount',(shaped.canonical_pay_vector_json->>'total_pence')::numeric/100,
      'charge_amount',(shaped.canonical_charge_vector_json->>'total_pence')::numeric/100,
      'is_reversal',false,
      'exclude_from_pay',false,
      'weekly_source',pg_catalog.jsonb_build_object(
        'schema_version','WEEKLY_SOURCE_SEGMENT_LINEAGE_V1',
        'work_event_id',shaped.work_event_id,
        'movement_id',shaped.movement_id,
        'final_revision_id',shaped.final_revision_id,
        'source_profile_kind',shaped.source_profile_kind,
        'source_line_kind',shaped.source_line_kind,
        'row_resolution_id',shaped.row_resolution_id,
        'economic_snapshot_id',shaped.economic_snapshot_id,
        'pay_vector',shaped.canonical_pay_vector_json,
        'charge_vector',shaped.canonical_charge_vector_json,
        'movement_economic_hash',pg_catalog.encode(shaped.movement_economic_hash,'hex'),
        'mapping_rate_policy_fingerprint',
          pg_catalog.encode(shaped.mapping_rate_policy_fingerprint,'hex'),
        'calculation_fingerprint',pg_catalog.encode(shaped.calculation_fingerprint,'hex'),
        'timesheet_lineage_fingerprint',pg_catalog.encode(shaped.lineage_fingerprint,'hex')
      )
    ) order by shaped.work_date,shaped.start_at_local,shaped.end_at_local,
               shaped.work_event_id
  ),'[]'::jsonb)
  from shaped;
$function$;

create or replace function private.weekly_source_ordinary_projection_current_expenses_v1(
  p_root_timesheet_id uuid,
  p_through_final_revision_id uuid
) returns jsonb
language sql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
  with target as (
    select revision.finalised_at_utc,revision.revision_number,
           source_cycle.finalisation_week_ending,revision.id,revision.state,
           revision.predecessor_revision_id
    from public.weekly_source_final_revisions revision
    join public.weekly_source_cycles source_cycle
      on source_cycle.id=revision.source_cycle_id
    where revision.id=p_through_final_revision_id
  ), root_events as (
    select distinct movement.work_event_id,movement.contract_id
    from public.weekly_source_billing_movements movement
    where movement.invoice_timesheet_id=p_root_timesheet_id
  ), ranked as (
    select expense.*,source_cycle.finalisation_week_ending,
      revision.finalised_at_utc,revision.revision_number,
      pg_catalog.row_number() over (
        partition by expense.work_event_id
        order by source_cycle.finalisation_week_ending desc,
                 revision.finalised_at_utc desc,revision.revision_number desc,
                 expense.generation desc,expense.id desc
      ) as event_rank
    from public.weekly_expense_authority_generations expense
    join root_events root_event
      on root_event.work_event_id=expense.work_event_id
     and root_event.contract_id=expense.contract_id
    join public.weekly_source_final_revisions revision
      on revision.id=expense.final_revision_id
    join public.weekly_source_cycles source_cycle
      on source_cycle.id=revision.source_cycle_id
    cross join target
    where (revision.state='CURRENT' or revision.id=target.id)
      and (
        target.state<>'PREPARED'
        or revision.id is distinct from target.predecessor_revision_id
      )
      and (source_cycle.finalisation_week_ending<target.finalisation_week_ending
       or (
         source_cycle.finalisation_week_ending=target.finalisation_week_ending
         and revision.finalised_at_utc<target.finalised_at_utc
       )
       or (
         source_cycle.finalisation_week_ending=target.finalisation_week_ending
         and revision.finalised_at_utc=target.finalised_at_utc
         and revision.revision_number<target.revision_number
       )
       or (
         source_cycle.finalisation_week_ending=target.finalisation_week_ending
         and revision.finalised_at_utc=target.finalised_at_utc
         and revision.revision_number=target.revision_number
         and revision.id::text<=target.id::text
       )
      )
  )
  select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
    'expense_authority_generation_id',ranked.id,
    'final_revision_id',ranked.final_revision_id,
    'work_event_id',ranked.work_event_id,
    'contract_id',ranked.contract_id,
    'row_expense_policy_snapshot_id',ranked.row_expense_policy_snapshot_id,
    'prior_expense_authority_generation_id',ranked.prior_expense_authority_generation_id,
    'generation',ranked.generation,
    'source_observation_kind',ranked.source_observation_kind,
    'correction_presentation',ranked.correction_presentation,
    'source_expense_pence',ranked.source_expense_pence::text,
    'source_expense_vat_enabled',ranked.source_expense_vat_enabled,
    'candidate_reimbursement_ex_vat',ranked.candidate_reimbursement_ex_vat::text,
    'client_charge_ex_vat',ranked.client_charge_ex_vat::text,
    'authority_hash',pg_catalog.encode(ranked.authority_hash,'hex')
  ) order by ranked.work_event_id),'[]'::jsonb)
  from ranked
  -- Keep a zero authority in the ranking so it remains the newest immutable
  -- observation and the superseded positive authority cannot reappear.  A
  -- zero tombstone is not, however, a payable source expense and must not
  -- produce Timesheet expense description or evidence.
  where ranked.event_rank=1
    and ranked.source_expense_pence>0;
$function$;

create or replace function private.weekly_source_ordinary_projection_actual_schedule_v1(
  p_segments jsonb
) returns jsonb
language sql immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select coalesce(pg_catalog.jsonb_agg(
    pg_catalog.jsonb_strip_nulls(pg_catalog.jsonb_build_object(
      'segment_id',segment.value->>'segment_id',
      'date',segment.value->>'date',
      'start',segment.value->>'start',
      'end',segment.value->>'end',
      'overnight',(segment.value->>'overnight')::boolean,
      'break_mins',(segment.value->>'break_mins')::integer,
      'ref_num',segment.value->>'ref_num',
      'breaks',coalesce(segment.value->'breaks','[]'::jsonb),
      'weekly_source_work_event_id',segment.value#>>'{weekly_source,work_event_id}',
      'weekly_source_movement_id',segment.value#>>'{weekly_source,movement_id}',
      'weekly_source_final_revision_id',segment.value#>>'{weekly_source,final_revision_id}'
    )) order by segment.ordinality
  ),'[]'::jsonb)
  from pg_catalog.jsonb_array_elements(coalesce(p_segments,'[]'::jsonb))
    with ordinality as segment(value,ordinality);
$function$;

drop function if exists private.weekly_source_ordinary_projection_receipt_insert_v1(
  uuid,uuid,uuid,uuid,uuid,text,text,text,jsonb,bytea,bytea,bytea,bytea,bytea,
  bytea,bytea,text,bytea,uuid
);

create or replace function private.weekly_source_ordinary_projection_receipt_insert_v1(
  p_final_revision_id uuid,
  p_source_cycle_id uuid,
  p_client_id uuid,
  p_root_timesheet_id uuid,
  p_published_timesheet_financial_id uuid,
  p_source_profile_kind text,
  p_source_mode text,
  p_outcome text,
  p_source_units jsonb,
  p_source_unit_manifest_hash bytea,
  p_source_expenses jsonb,
  p_source_expense_manifest_hash bytea,
  p_final_manifest_hash bytea,
  p_final_policy_fingerprint bytea,
  p_service_snapshot_hash bytea,
  p_server_calculation_fingerprint bytea,
  p_root_before_hash bytea,
  p_root_after_hash bytea,
  p_idempotency_key text,
  p_request_hash bytea,
  p_actor_user_id uuid
) returns public.weekly_source_ordinary_pay_projection_receipts
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_receipt public.weekly_source_ordinary_pay_projection_receipts%rowtype;
  v_receipt_hash bytea;
begin
  v_receipt_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_ORDINARY_PAY_PROJECTION_RECEIPT_V1',
    pg_catalog.jsonb_build_object(
      'final_revision_id',p_final_revision_id,'source_cycle_id',p_source_cycle_id,
      'client_id',p_client_id,'root_timesheet_id',p_root_timesheet_id,
      'published_timesheet_financial_id',p_published_timesheet_financial_id,
      'source_profile_kind',p_source_profile_kind,'source_mode',p_source_mode,
      'outcome',p_outcome,'source_units',p_source_units,
      'source_unit_manifest_hash',pg_catalog.encode(p_source_unit_manifest_hash,'hex'),
      'source_expense_authorities',p_source_expenses,
      'source_expense_manifest_hash',
        pg_catalog.encode(p_source_expense_manifest_hash,'hex'),
      'final_manifest_hash',pg_catalog.encode(p_final_manifest_hash,'hex'),
      'final_policy_fingerprint',pg_catalog.encode(p_final_policy_fingerprint,'hex'),
      'service_snapshot_hash',pg_catalog.encode(p_service_snapshot_hash,'hex'),
      'server_calculation_fingerprint',
        pg_catalog.encode(p_server_calculation_fingerprint,'hex'),
      'root_before_hash',pg_catalog.encode(p_root_before_hash,'hex'),
      'root_after_hash',pg_catalog.encode(p_root_after_hash,'hex'),
      'request_hash',pg_catalog.encode(p_request_hash,'hex'),'actor_user_id',p_actor_user_id
    )
  );
  insert into public.weekly_source_ordinary_pay_projection_receipts(
    final_revision_id,source_cycle_id,client_id,root_timesheet_id,
    published_timesheet_financial_id,source_profile_kind,source_mode,outcome,
    source_unit_count,source_unit_outcomes_json,source_unit_manifest_hash,
    source_expense_authorities_json,source_expense_manifest_hash,
    final_manifest_hash,final_policy_fingerprint,service_snapshot_hash,
    server_calculation_fingerprint,root_before_hash,root_after_hash,
    idempotency_key,request_hash,receipt_hash,actor_user_id
  ) values (
    p_final_revision_id,p_source_cycle_id,p_client_id,p_root_timesheet_id,
    p_published_timesheet_financial_id,p_source_profile_kind,p_source_mode,p_outcome,
    pg_catalog.jsonb_array_length(p_source_units),p_source_units,p_source_unit_manifest_hash,
    p_source_expenses,p_source_expense_manifest_hash,
    p_final_manifest_hash,p_final_policy_fingerprint,p_service_snapshot_hash,
    p_server_calculation_fingerprint,p_root_before_hash,p_root_after_hash,
    p_idempotency_key,p_request_hash,v_receipt_hash,p_actor_user_id
  ) returning * into v_receipt;
  return v_receipt;
end;
$function$;

create or replace function private.weekly_source_ordinary_projection_expenses_materialise_v1(
  p_root_timesheet_id uuid,
  p_timesheet_financial_id uuid,
  p_expected_source_expenses jsonb
) returns integer
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_expense jsonb;
  v_authority public.weekly_expense_authority_generations%rowtype;
  v_movement public.weekly_source_billing_movements%rowtype;
  v_existing public.weekly_source_expense_pay_materialisations%rowtype;
  v_lineage_hash bytea;
  v_changed integer:=0;
begin
  if p_root_timesheet_id is null or p_timesheet_financial_id is null
     or pg_catalog.jsonb_typeof(p_expected_source_expenses)<>'array'
     or not exists(
       select 1 from public.timesheets_financials financial
       where financial.id=p_timesheet_financial_id
         and financial.timesheet_id=p_root_timesheet_id
         and financial.is_current
     ) then
    raise exception 'WEEKLY_SOURCE_EXPENSE_MATERIALISATION_INPUT_INVALID'
      using errcode='22023';
  end if;
  if pg_catalog.jsonb_array_length(p_expected_source_expenses)=0 then
    return 0;
  end if;

  perform pg_catalog.set_config(
    'cloudtms.weekly_source_projection_owner',
    'weekly_source_ordinary_projection_expenses_materialise_v1',true
  );
  for v_expense in
    select expense.value
    from pg_catalog.jsonb_array_elements(p_expected_source_expenses) expense(value)
    where coalesce(expense.value->>'source_expense_pence','')~'^[1-9][0-9]*$'
    order by expense.value->>'work_event_id'
  loop
    if coalesce(v_expense->>'expense_authority_generation_id','')
         !~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       or coalesce(v_expense->>'authority_hash','')!~'^[0-9a-f]{64}$' then
      raise exception 'WEEKLY_SOURCE_EXPENSE_AUTHORITY_MANIFEST_INVALID'
        using errcode='55000';
    end if;
    select authority.* into strict v_authority
    from public.weekly_expense_authority_generations authority
    where authority.id=(v_expense->>'expense_authority_generation_id')::uuid
      and authority.work_event_id=(v_expense->>'work_event_id')::uuid
      and authority.contract_id=(v_expense->>'contract_id')::uuid
      and authority.source_expense_pence=(v_expense->>'source_expense_pence')::bigint
      and pg_catalog.encode(authority.authority_hash,'hex')=v_expense->>'authority_hash';
    select movement.* into strict v_movement
    from public.weekly_source_billing_movements movement
    where movement.expense_authority_generation_id=v_authority.id
      and movement.invoice_timesheet_id=p_root_timesheet_id
      and movement.source_line_kind='SOURCE_FIXED_EXPENSE'
      and movement.movement_role in ('EXPENSE_POSITIVE','EXPENSE_REPLACEMENT')
      and movement.total_pay_ex_vat=v_authority.candidate_reimbursement_ex_vat
    order by movement.created_at_utc desc,movement.id desc
    limit 1;

    select materialisation.* into v_existing
    from public.weekly_source_expense_pay_materialisations materialisation
    where materialisation.expense_authority_generation_id=v_authority.id
      and materialisation.candidate_timesheet_financial_id=p_timesheet_financial_id;
    if found then
      if v_existing.billing_movement_id is distinct from v_movement.id
         or v_existing.root_timesheet_id is distinct from p_root_timesheet_id then
        raise exception 'WEEKLY_SOURCE_EXPENSE_MATERIALISATION_CONFLICT'
          using errcode='55000';
      end if;
      continue;
    end if;
    v_lineage_hash:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_EXPENSE_TSFIN_MATERIALISATION_V1',
      pg_catalog.jsonb_build_object(
        'expense_authority_generation_id',v_authority.id,
        'authority_hash',pg_catalog.encode(v_authority.authority_hash,'hex'),
        'billing_movement_id',v_movement.id,
        'movement_economic_hash',pg_catalog.encode(v_movement.movement_economic_hash,'hex'),
        'root_timesheet_id',p_root_timesheet_id,
        'candidate_timesheet_financial_id',p_timesheet_financial_id
      )
    );
    insert into public.weekly_source_expense_pay_materialisations(
      expense_authority_generation_id,billing_movement_id,
      root_timesheet_id,candidate_timesheet_financial_id,pay_lineage_hash
    ) values (
      v_authority.id,v_movement.id,p_root_timesheet_id,
      p_timesheet_financial_id,v_lineage_hash
    );
    v_changed:=v_changed+1;
  end loop;
  return v_changed;
exception
  when no_data_found or too_many_rows then
    raise exception 'WEEKLY_SOURCE_EXPENSE_MATERIALISATION_SCOPE_INVALID'
      using errcode='55000';
end;
$function$;

create or replace function public.weekly_source_target_managed_root_prepare_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_expected_keys text[]:=array[
    'actor_user_id','idempotency_key','root_timesheet_id','schema_version',
    'service_snapshot','source_cycle_id','target_family_id'
  ];
  v_actual_keys text[];
  v_actor_user_id uuid;
  v_family_id uuid;
  v_root_timesheet_id uuid;
  v_source_cycle_id uuid;
  v_idempotency_key text;
  v_service_snapshot jsonb;
  v_request_hash bytea;
  v_request_hash_hex text;
  v_service_snapshot_hash bytea;
  v_server_calculation_fingerprint bytea;
  v_root_before_hash bytea;
  v_root_after_hash bytea;
  v_receipt_hash bytea;
  v_family public.weekly_exceptional_pay_target_families%rowtype;
  v_family_members uuid[];
  v_family_members_after_lock uuid[];
  v_timesheet public.timesheets%rowtype;
  v_contract public.contracts%rowtype;
  v_contract_week public.contract_weeks%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_policy jsonb;
  v_source_mode text;
  v_source_profile_domain text;
  v_expected_rate_source_refs jsonb;
  v_canonical_tsfin jsonb;
  v_preflight jsonb;
  v_required_path text;
  v_lifecycle_result jsonb;
  v_write_result jsonb;
  v_published_financial_id uuid;
  v_live_generation_count integer;
  v_prepared_kind text;
  v_decision_bundle_id uuid;
  v_decision_id uuid;
  v_head_id uuid;
  v_proposal_request jsonb;
  v_proposal jsonb;
  v_audit public.audit_events%rowtype;
  v_result jsonb;
begin
  if coalesce(
       pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role',''
     )<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  if pg_catalog.jsonb_typeof(p_request)<>'object'
     or pg_catalog.octet_length(p_request::text)>1048576 then
    raise exception 'WEEKLY_SOURCE_TARGET_ROOT_PREPARE_REQUEST_INVALID'
      using errcode='22023';
  end if;
  select pg_catalog.array_agg(key order by key) into v_actual_keys
  from pg_catalog.jsonb_object_keys(p_request) key;
  select pg_catalog.array_agg(key order by key) into v_expected_keys
  from pg_catalog.unnest(v_expected_keys) key;
  if v_actual_keys is distinct from v_expected_keys
     or p_request->>'schema_version'
          <>'WEEKLY_SOURCE_TARGET_MANAGED_ROOT_PREPARE_REQUEST_V1'
     or pg_catalog.jsonb_typeof(p_request->'service_snapshot')<>'object' then
    raise exception 'WEEKLY_SOURCE_TARGET_ROOT_PREPARE_CONTRACT_INVALID'
      using errcode='22023';
  end if;
  begin
    v_actor_user_id:=(p_request->>'actor_user_id')::uuid;
    v_family_id:=(p_request->>'target_family_id')::uuid;
    v_root_timesheet_id:=(p_request->>'root_timesheet_id')::uuid;
    v_source_cycle_id:=(p_request->>'source_cycle_id')::uuid;
  exception when invalid_text_representation then
    raise exception 'WEEKLY_SOURCE_TARGET_ROOT_PREPARE_ID_INVALID'
      using errcode='22023';
  end;
  v_idempotency_key:=pg_catalog.btrim(coalesce(p_request->>'idempotency_key',''));
  v_service_snapshot:=p_request->'service_snapshot';
  if v_actor_user_id is null or v_family_id is null or v_root_timesheet_id is null
     or v_source_cycle_id is null
     or pg_catalog.char_length(v_idempotency_key) not between 1 and 200 then
    raise exception 'WEEKLY_SOURCE_TARGET_ROOT_PREPARE_REQUIRED_INPUT_MISSING'
      using errcode='22023';
  end if;
  perform 1 from public.tms_users actor
  where actor.id=v_actor_user_id and coalesce(actor.is_active,false);
  if not found then
    raise exception 'WEEKLY_SOURCE_TARGET_ROOT_PREPARE_ACTOR_INVALID'
      using errcode='42501';
  end if;

  v_request_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_TARGET_MANAGED_ROOT_PREPARE_REQUEST_V1',
    p_request-'idempotency_key'
  );
  v_request_hash_hex:=pg_catalog.encode(v_request_hash,'hex');
  select audit_row.* into v_audit
  from public.audit_events audit_row
  where audit_row.action='WEEKLY_SOURCE_TARGET_MANAGED_ROOT_PREPARED'
    and (
      audit_row.after_json->>'idempotency_key'=v_idempotency_key
      or audit_row.after_json->>'request_hash'=v_request_hash_hex
    )
  order by audit_row.ts_utc,audit_row.id
  limit 1;
  if found then
    if v_audit.after_json->>'request_hash' is distinct from v_request_hash_hex then
      raise exception 'WEEKLY_SOURCE_TARGET_ROOT_PREPARE_IDEMPOTENCY_COLLISION'
        using errcode='22023';
    end if;
    if pg_catalog.jsonb_typeof(v_audit.after_json->'result')<>'object'
       or v_audit.after_json#>>'{result,root_timesheet_id}'
            is distinct from v_root_timesheet_id::text
       or v_audit.after_json#>>'{result,target_family_id}'
            is distinct from v_family_id::text then
      raise exception 'WEEKLY_SOURCE_TARGET_ROOT_PREPARE_RECEIPT_INVALID'
        using errcode='55000';
    end if;
    return (v_audit.after_json->'result')
      ||pg_catalog.jsonb_build_object('idempotent_replay',true);
  end if;

  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    'WEEKLY_SOURCE_TARGET_ROOT_PREPARE_KEY|'||v_idempotency_key,0
  ));
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    'WEEKLY_SOURCE_TARGET_ROOT_PREPARE_ROOT|'||v_root_timesheet_id::text,0
  ));
  select audit_row.* into v_audit
  from public.audit_events audit_row
  where audit_row.action='WEEKLY_SOURCE_TARGET_MANAGED_ROOT_PREPARED'
    and (
      audit_row.after_json->>'idempotency_key'=v_idempotency_key
      or audit_row.after_json->>'request_hash'=v_request_hash_hex
    )
  order by audit_row.ts_utc,audit_row.id
  limit 1;
  if found then
    if v_audit.after_json->>'request_hash' is distinct from v_request_hash_hex then
      raise exception 'WEEKLY_SOURCE_TARGET_ROOT_PREPARE_IDEMPOTENCY_COLLISION'
        using errcode='22023';
    end if;
    if pg_catalog.jsonb_typeof(v_audit.after_json->'result')<>'object'
       or v_audit.after_json#>>'{result,root_timesheet_id}'
            is distinct from v_root_timesheet_id::text
       or v_audit.after_json#>>'{result,target_family_id}'
            is distinct from v_family_id::text then
      raise exception 'WEEKLY_SOURCE_TARGET_ROOT_PREPARE_RECEIPT_INVALID'
        using errcode='55000';
    end if;
    return (v_audit.after_json->'result')
      ||pg_catalog.jsonb_build_object('idempotent_replay',true);
  end if;

  select family.* into v_family
  from public.weekly_exceptional_pay_target_families family
  where family.id=v_family_id for update;
  if not found or v_family.root_timesheet_id is distinct from v_root_timesheet_id
     or v_family.target_domain<>'WEEKLY_PROTECTED_PAY'
     or v_family.target_domain_version<>'C1_V1'
     or v_family.ownership_state<>'TARGET_MANAGED' then
    raise exception 'WEEKLY_SOURCE_TARGET_MANAGED_FAMILY_INVALID'
      using errcode='55000';
  end if;
  -- WP-30 (WP-27 sweep finding N1), standing rule 3.  The invoice-isolation
  -- guard below is read over the Timesheet FAMILY, so the family's rows are
  -- locked BEFORE the guarded state is read, and the family is then re-resolved
  -- and required to be unchanged.
  --
  -- Lock shape, placement and why this one is safe, measured rather than
  -- assumed.  The installed path already takes `public.timesheets … for update`
  -- for the root (immediately below) and, through
  -- public.import_timesheet_financial_preflight_v1 a few statements later, for
  -- every member of the CORRECTION CHAIN `order by timesheet_id for update`.
  -- Executed on a rotated family: the correction chain of the current root is
  -- the root ALONE, so the rotation sibling is not locked anywhere on this path
  -- today.  Taking the family lock AFTER the root's own row lock would
  -- therefore be a new cycle (hold root, want sibling) against every owner that
  -- locks a family in timesheet_id order, so it is taken FIRST, in
  -- timesheet_id order, which is the order private.weekly_source_correct_final_
  -- preconditions_v1, interface I-1 and the preflight all use.  The root's own
  -- `for update` below then re-locks a row this transaction already holds.
  v_family_members:=private.weekly_source_invoice_family_timesheet_ids_v1(
    v_root_timesheet_id
  );
  -- Standing rule 3's fail-closed branch: an explicit cardinality test, never a
  -- `limit` and never "the unique index makes this impossible".
  if v_family_members is null or pg_catalog.cardinality(v_family_members)=0 then
    raise exception 'WEEKLY_SOURCE_TARGET_MANAGED_FAMILY_UNRESOLVED'
      using errcode='55000';
  end if;
  perform 1 from public.timesheets lock_row
  where lock_row.timesheet_id=any(v_family_members)
  order by lock_row.timesheet_id for update;
  v_family_members_after_lock:=private.weekly_source_invoice_family_timesheet_ids_v1(
    v_root_timesheet_id
  );
  -- A membership change across the lock fails closed and is retryable, exactly
  -- as the lock-and-resolve helper (I-1 step 3) reports one.
  if v_family_members_after_lock is null
     or pg_catalog.cardinality(v_family_members_after_lock)=0
     or not (v_family_members_after_lock @> v_family_members
             and v_family_members @> v_family_members_after_lock) then
    raise exception 'WEEKLY_SOURCE_TARGET_MANAGED_FAMILY_UNRESOLVED'
      using errcode='40001';
  end if;
  select timesheet_row.* into v_timesheet
  from public.timesheets timesheet_row
  where timesheet_row.timesheet_id=v_root_timesheet_id for update;
  if not found or not v_timesheet.is_current
     or v_timesheet.contract_id is distinct from v_family.contract_id
     or v_timesheet.week_ending_date is distinct from v_family.week_ending_date
     or v_timesheet.sheet_scope<>'WEEKLY'::public.timesheet_scope_enum
     or v_timesheet.line_type<>'HOURS'::public.timesheet_line_type_enum
     or v_timesheet.is_adjustment or v_timesheet.revoked_at is not null
     or v_timesheet.archived_at_utc is not null then
    raise exception 'WEEKLY_SOURCE_TARGET_MANAGED_ROOT_INVALID'
      using errcode='55000';
  end if;
  select contract.* into strict v_contract
  from public.contracts contract where contract.id=v_family.contract_id;
  if v_contract.candidate_id is distinct from v_family.candidate_id
     or v_contract.client_id is null
     or v_family.week_start_date<>v_family.week_ending_date-6 then
    raise exception 'WEEKLY_SOURCE_TARGET_MANAGED_CONTRACT_INVALID'
      using errcode='55000';
  end if;
  select contract_week.* into strict v_contract_week
  from public.contract_weeks contract_week
  where contract_week.contract_id=v_family.contract_id
    and contract_week.week_ending_date=v_family.week_ending_date
    and contract_week.additional_seq=0
  for update;
  if v_contract_week.timesheet_id is distinct from v_root_timesheet_id
     or v_contract_week.is_adjustment
     or v_contract_week.status='CANCELLED'::public.contract_week_status_enum then
    raise exception 'WEEKLY_SOURCE_TARGET_MANAGED_CONTRACT_WEEK_INVALID'
      using errcode='55000';
  end if;
  select cycle.* into strict v_cycle
  from public.weekly_source_cycles cycle where cycle.id=v_source_cycle_id for share;
  select source_group.* into strict v_group
  from public.weekly_source_groups source_group
  where source_group.id=v_cycle.source_group_id and source_group.active for share;
  if v_group.agency_id is distinct from v_family.agency_id then
    raise exception 'WEEKLY_SOURCE_TARGET_MANAGED_GROUP_INVALID'
      using errcode='55000';
  end if;
  v_policy:=private._weekly_source_effective_policy_v1(
    v_contract.client_id,v_contract.id,v_family.week_ending_date
  );
  v_source_mode:=v_policy->>'c1_source_mode';
  if v_policy->>'source_group_id' is distinct from v_group.id::text
     or v_policy->>'authority_mode'<>'SOURCE_AUTHORITY'
     or coalesce((v_policy->>'self_bill_enabled')::boolean,false) is not true
     or v_source_mode not in ('NHSP_WEEKLY','HEALTHROSTER_WEEKLY')
     or (v_group.source_family='NHSP') is distinct from (v_source_mode='NHSP_WEEKLY')
     or coalesce(v_policy->>'policy_sha256','')!~'^[0-9a-f]{64}$' then
    raise exception 'WEEKLY_SOURCE_TARGET_MANAGED_POLICY_INVALID'
      using errcode='55000';
  end if;
  v_source_profile_domain:=case when v_source_mode='NHSP_WEEKLY'
    then 'NHSP_TRUST_BACKING_REPORT' else 'ROSTER_FINAL_AUTHORITY' end;

  -- WP-30 (WP-27 sweep finding N1), standing rule 3.  Three limbs of one
  -- isolation guard: WP-27 moved the first onto the family and left these two
  -- on the physical root id, which is more dangerous than all three being
  -- wrong, because the guard appears to work.  EXECUTED on the real installed
  -- owner: a week that was invoiced, then rotated, then protected was PREPARED
  -- — the guard named "invoice isolation" let a family that already carries
  -- billing movements through — while the same call on an unrotated family
  -- with the same movement on its own physical root was refused.  All three
  -- limbs now read the ONE family resolved and locked above.
  if private.weekly_source_invoice_movement_only_integrity_v1(v_root_timesheet_id)
       is not true
     or exists(
       select 1 from public.weekly_source_billing_movements movement
       where movement.invoice_timesheet_id=any(v_family_members)
     )
     or exists(
       select 1 from public.invoice_lines invoice_line
       where invoice_line.timesheet_id=any(v_family_members)
     ) then
    raise exception 'WEEKLY_SOURCE_TARGET_MANAGED_INVOICE_ISOLATION_FAILED'
      using errcode='55000';
  end if;

  v_preflight:=public.import_timesheet_financial_preflight_v1(
    array[v_root_timesheet_id],'WEEKLY_SOURCE_TARGET_ROOT_PREPARE',v_actor_user_id,
    '{}'::jsonb,true,1
  );
  v_required_path:=v_preflight->>'required_path';
  -- G2-2.  There is no REFUSED_LOCKED return any more.  A locked, paid or
  -- Draft-frozen root is no longer refused here, because this owner no longer
  -- mutates an authorised root at all: it prepares only a never-authorised one
  -- and otherwise records a certified-zero PROPOSAL.

  v_service_snapshot_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_TARGET_MANAGED_ZERO_SERVICE_SNAPSHOT_V1',v_service_snapshot
  );
  v_expected_rate_source_refs:=pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_SOURCE_TARGET_MANAGED_ZERO_RATE_SOURCE_V1',
    'source_cycle_id',v_cycle.id,'source_group_id',v_group.id,
    'source_family',v_group.source_family,'source_mode',v_source_mode,
    'source_profile_domain',v_source_profile_domain,
    'target_family_id',v_family.id,'root_timesheet_id',v_root_timesheet_id,
    'effective_policy_sha256',v_policy->>'policy_sha256'
  );
  v_server_calculation_fingerprint:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_TARGET_MANAGED_ZERO_SERVER_CALCULATION_V1',
    pg_catalog.jsonb_build_object(
      'target_family_id',v_family.id,'root_timesheet_id',v_root_timesheet_id,
      'source_cycle_id',v_cycle.id,'source_mode',v_source_mode,
      'source_profile_domain',v_source_profile_domain,
      'expected_segments','[]'::jsonb,'expected_actual_schedule','[]'::jsonb,
      'expected_rate_source_refs',v_expected_rate_source_refs,
      'service_snapshot_hash',pg_catalog.encode(v_service_snapshot_hash,'hex')
    )
  );
  v_root_before_hash:=
    private.weekly_source_ordinary_projection_root_hash_v1(v_root_timesheet_id);
  v_canonical_tsfin:=private.weekly_source_ordinary_projection_snapshot_assert_v1(
    v_root_timesheet_id,v_source_mode,'[]'::jsonb,'[]'::jsonb,
    v_expected_rate_source_refs,'[]'::jsonb,v_service_snapshot
  );

  -- =======================================================================
  -- G2-2.  Certified zero is an explicit complete entitlement head, never a
  -- zeroed public schedule (25 section 1 Added; 24 section 4.5 step 3; WB-009).
  -- The three forbidden acts are gone from this owner:
  --   * public.timesheet_unauthorise_atomic          - deleted
  --   * update public.timesheets set actual_schedule_json='[]'  - deleted
  --     (that destroyed the Candidate's submitted evidence, 24 section 2)
  --   * public.timesheet_authorise_generic_atomic    - deleted
  -- =======================================================================
  -- Decision D8: the authorisation record is per ROOT, in
  -- public.weekly_source_root_authorisations, NOT on the per-source-row lineage
  -- table (which is written before any authorisation exists).
  select pg_catalog.count(*)::integer into v_live_generation_count
  from public.weekly_source_root_authorisations root_authorisation
  where pg_catalog.btrim(root_authorisation.family_booking_id)=pg_catalog.btrim(v_timesheet.booking_id)
    and root_authorisation.withdrawn_at_utc is null;

  if v_live_generation_count=0
     and coalesce((v_preflight->>'allowed')::boolean,false) is true
     and v_required_path='DIRECT_AMEND_RECALCULATE' then
    -- Never-authorised root: the zero source TSFIN may be prepared through the
    -- established mutable, unauthorised writer (24 section 4.1).  The public
    -- schedule is NOT touched and the root is NOT authorised.
    v_write_result:=public.tsfin_write_current_snapshot_single_bounded(
      v_root_timesheet_id,v_timesheet.version,v_canonical_tsfin,v_actor_user_id,
      pg_catalog.transaction_timestamp()
    );
    if coalesce((v_write_result->>'ok')::boolean,false) is not true
       or coalesce(v_write_result->>'timesheet_financials_id','')
            !~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
      raise exception 'WEEKLY_SOURCE_TARGET_ROOT_TSFIN_WRITE_FAILED'
        using errcode='55000',detail=coalesce(v_write_result,'{}'::jsonb)::text;
    end if;
    v_published_financial_id:=(v_write_result->>'timesheet_financials_id')::uuid;
    v_prepared_kind:='ZERO_TSFIN_AWAITING_FIRST_AUTHORISATION';
  else
    -- Already authorised: write nothing at all.  The certified zero becomes a
    -- complete PROTECTED head with component_count = 0, published by the Office
    -- decision owner through the Gate 5 coordinator.
    v_prepared_kind:='CERTIFIED_ZERO_PROPOSAL';
  end if;

  -- Record the certified-zero proposal whenever the cycle has a current final
  -- revision to anchor it to.  When it has none there is no conforming
  -- publication request to build, so the proposal is deliberately deferred and
  -- the reason is returned rather than guessed at.
  if v_cycle.current_final_revision_id is not null then
    v_decision_bundle_id:=private.weekly_source_entitlement_derived_uuid_v1(
      'WEEKLY_SOURCE_DECISION_BUNDLE_V1',
      pg_catalog.btrim(v_timesheet.booking_id)||'|'||
      v_cycle.current_final_revision_id::text||'|CERTIFIED_ZERO'
    );
    v_decision_id:=private.weekly_source_entitlement_derived_uuid_v1(
      'WEEKLY_SOURCE_DECISION_V1',v_decision_bundle_id::text||'|1'
    );
    v_head_id:=private.weekly_source_entitlement_derived_uuid_v1(
      'WEEKLY_SOURCE_PROPOSED_HEAD_V1',
      v_decision_bundle_id::text||'|1|'||v_root_timesheet_id::text
    );
    v_proposal_request:=private.weekly_source_entitlement_proposal_request_v1(
      v_root_timesheet_id,v_cycle.current_final_revision_id,'PROTECTED',
      v_decision_bundle_id,1::bigint,v_head_id,v_decision_id,'[]'::jsonb
    );
    v_proposal:=private.weekly_source_entitlement_proposal_record_v1(
      v_proposal_request,v_family.agency_id,v_family.contract_id,
      v_family.week_ending_date,v_actor_user_id
    );
  end if;

  v_root_after_hash:=
    private.weekly_source_ordinary_projection_root_hash_v1(v_root_timesheet_id);
  v_receipt_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_TARGET_MANAGED_ROOT_PREPARE_RECEIPT_V1',
    pg_catalog.jsonb_build_object(
      'request_hash',v_request_hash_hex,'target_family_id',v_family.id,
      'root_timesheet_id',v_root_timesheet_id,'source_cycle_id',v_cycle.id,
      'source_mode',v_source_mode,'source_profile_domain',v_source_profile_domain,
      'published_timesheet_financial_id',v_published_financial_id,
      'service_snapshot_hash',pg_catalog.encode(v_service_snapshot_hash,'hex'),
      'server_calculation_fingerprint',
        pg_catalog.encode(v_server_calculation_fingerprint,'hex'),
      'root_before_hash',pg_catalog.encode(v_root_before_hash,'hex'),
      'root_after_hash',pg_catalog.encode(v_root_after_hash,'hex'),
      'actor_user_id',v_actor_user_id
    )
  );
  v_result:=pg_catalog.jsonb_build_object(
    'ok',true,'outcome','PREPARED','target_family_id',v_family.id,
    'root_timesheet_id',v_root_timesheet_id,'source_cycle_id',v_cycle.id,
    'source_mode',v_source_mode,'source_profile_domain',v_source_profile_domain,
    'timesheet_financials_id',v_published_financial_id,
    'prepared_kind',v_prepared_kind,
    'proposal',coalesce(v_proposal,'null'::jsonb),
    'proposal_deferred_reason',case when v_proposal is null
      then 'WEEKLY_SOURCE_CYCLE_HAS_NO_CURRENT_FINAL_REVISION' else null end,
    'required_path',v_required_path,
    'server_calculation_fingerprint',
      pg_catalog.encode(v_server_calculation_fingerprint,'hex'),
    'root_after_hash',pg_catalog.encode(v_root_after_hash,'hex'),
    'receipt_hash',pg_catalog.encode(v_receipt_hash,'hex')
  );
  perform public._audit_insert(
    'weekly_exceptional_pay_target_families',v_family.id::text,
    'WEEKLY_SOURCE_TARGET_MANAGED_ROOT_PREPARED',null,
    pg_catalog.jsonb_build_object(
      'idempotency_key',v_idempotency_key,'request_hash',v_request_hash_hex,
      'service_snapshot_hash',pg_catalog.encode(v_service_snapshot_hash,'hex'),
      'server_calculation_fingerprint',
        pg_catalog.encode(v_server_calculation_fingerprint,'hex'),
      'result',v_result
    ),'WEEKLY_SOURCE_TARGET_MANAGED_ZERO_ROOT_PREPARE',v_actor_user_id
  );
  return v_result||pg_catalog.jsonb_build_object('idempotent_replay',false);
exception
  when no_data_found or too_many_rows then
    raise exception 'WEEKLY_SOURCE_TARGET_ROOT_PREPARE_SCOPE_INVALID'
      using errcode='55000';
end;
$function$;

alter function private.weekly_source_entitlement_derived_uuid_v1(text,text)
  owner to postgres;
alter function private.weekly_source_entitlement_component_id_v1(text,text,text,text)
  owner to postgres;
alter function private.weekly_source_entitlement_components_v1(jsonb,jsonb)
  owner to postgres;
alter function private.weekly_source_effective_inventory_v1(uuid)
  owner to postgres;
alter function private.weekly_source_target_family_for_root_v1(uuid)
  owner to postgres;
alter function private.weekly_source_entitlement_proposal_request_v1(
  uuid,uuid,text,uuid,bigint,uuid,uuid,jsonb,text
) owner to postgres;
alter function private.weekly_source_entitlement_proposal_cross_contract_request_v1(
  uuid,uuid,uuid,uuid,bigint,uuid,uuid,uuid,uuid[],text,text,text,text,uuid,uuid,timestamptz
) owner to postgres;
alter function private.weekly_source_entitlement_proposal_record_v1(
  jsonb,uuid,uuid,date,uuid
) owner to postgres;
revoke all on function private.weekly_source_entitlement_derived_uuid_v1(text,text)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_entitlement_component_id_v1(text,text,text,text)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_entitlement_components_v1(jsonb,jsonb)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_effective_inventory_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_target_family_for_root_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_entitlement_proposal_request_v1(
  uuid,uuid,text,uuid,bigint,uuid,uuid,jsonb,text
) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_entitlement_proposal_cross_contract_request_v1(
  uuid,uuid,uuid,uuid,bigint,uuid,uuid,uuid,uuid[],text,text,text,text,uuid,uuid,timestamptz
) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_entitlement_proposal_record_v1(
  jsonb,uuid,uuid,date,uuid
) from public,anon,authenticated,service_role;

alter function private.weekly_source_ordinary_projection_hex32_v1(text,text)
  owner to postgres;
alter function private.weekly_source_ordinary_projection_receipt_json_v1(
  public.weekly_source_ordinary_pay_projection_receipts
) owner to postgres;
alter function private.weekly_source_ordinary_projection_expense_movement_assert_v1(
  uuid,uuid,uuid,uuid,uuid
) owner to postgres;
alter function private.weekly_source_ordinary_projection_snapshot_assert_v1(
  uuid,text,jsonb,jsonb,jsonb,jsonb,jsonb
) owner to postgres;
alter function private.weekly_source_ordinary_projection_root_hash_v1(uuid)
  owner to postgres;
alter function private.weekly_source_ordinary_projection_source_units_v1(uuid,uuid)
  owner to postgres;
alter function private.weekly_source_ordinary_projection_active_movements_v1(uuid,uuid)
  owner to postgres;
alter function private.weekly_source_ordinary_projection_current_segments_v1(uuid,uuid)
  owner to postgres;
alter function private.weekly_source_ordinary_projection_current_expenses_v1(uuid,uuid)
  owner to postgres;
alter function private.weekly_source_ordinary_projection_actual_schedule_v1(jsonb)
  owner to postgres;
alter function private.weekly_source_ordinary_projection_receipt_insert_v1(
  uuid,uuid,uuid,uuid,uuid,text,text,text,jsonb,bytea,jsonb,bytea,bytea,bytea,
  bytea,bytea,bytea,bytea,text,bytea,uuid
) owner to postgres;
alter function private.weekly_source_ordinary_projection_expenses_materialise_v1(
  uuid,uuid,jsonb
) owner to postgres;
alter function public.weekly_source_ordinary_pay_projection_apply_atomic_v1(jsonb)
  owner to postgres;
alter function public.weekly_source_target_managed_root_prepare_atomic_v1(jsonb)
  owner to postgres;

revoke all on function private.weekly_source_ordinary_projection_hex32_v1(text,text)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_ordinary_projection_receipt_json_v1(
  public.weekly_source_ordinary_pay_projection_receipts
) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_ordinary_projection_expense_movement_assert_v1(
  uuid,uuid,uuid,uuid,uuid
) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_ordinary_projection_snapshot_assert_v1(
  uuid,text,jsonb,jsonb,jsonb,jsonb,jsonb
) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_ordinary_projection_root_hash_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_ordinary_projection_source_units_v1(uuid,uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_ordinary_projection_active_movements_v1(uuid,uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_ordinary_projection_current_segments_v1(uuid,uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_ordinary_projection_current_expenses_v1(uuid,uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_ordinary_projection_actual_schedule_v1(jsonb)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_ordinary_projection_receipt_insert_v1(
  uuid,uuid,uuid,uuid,uuid,text,text,text,jsonb,bytea,jsonb,bytea,bytea,bytea,
  bytea,bytea,bytea,bytea,text,bytea,uuid
) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_ordinary_projection_expenses_materialise_v1(
  uuid,uuid,jsonb
) from public,anon,authenticated,service_role;
revoke all on function public.weekly_source_ordinary_pay_projection_apply_atomic_v1(jsonb)
  from public,anon,authenticated,service_role;
revoke all on function public.weekly_source_target_managed_root_prepare_atomic_v1(jsonb)
  from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_ordinary_pay_projection_apply_atomic_v1(jsonb)
  to service_role;
grant execute on function public.weekly_source_target_managed_root_prepare_atomic_v1(jsonb)
  to service_role;

comment on function public.weekly_source_ordinary_pay_projection_apply_atomic_v1(jsonb)
is 'Gate 2. Prepares a never-authorised ordinary Weekly root through the established unauthorised Timesheet/TSFIN writers WITHOUT authorising it, or, for an already-authorised root, composes one complete proposed entitlement and records a PROPOSED decision bundle. It never unauthorises, never overwrites submitted schedule evidence, never rotates a current TSFIN and never reauthorises.';
comment on function public.weekly_source_target_managed_root_prepare_atomic_v1(jsonb)
is 'Gate 2. Prepares an existing TARGET_MANAGED ordinary Weekly root for a certified-zero entitlement. It writes a zero source TSFIN only while the root is still unauthorised, never zeroes the public schedule, never authorises or unauthorises, and records the certified zero as a complete PROTECTED proposal for the Office decision owner to publish as an explicit head.';

notify pgrst, 'reload schema';

commit;
