-- Repeatable CloudTMS authority: weekly_source_projection_build_v1
-- Appends one complete, immutable mapping/qualification generation to a
-- BUILDING source publication. It creates no Timesheet, invoice or payment.

\set ON_ERROR_STOP on

begin;

create or replace function private.weekly_source_projection_hex32_v1(
  p_value text,
  p_code text
) returns bytea
language plpgsql immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
begin
  if p_value is null or p_value!~'^[0-9a-f]{64}$' then
    raise exception '%',coalesce(p_code,'WEEKLY_SOURCE_HASH_INVALID') using errcode='22023';
  end if;
  return pg_catalog.decode(p_value,'hex');
end;
$function$;

-- G6-13 / XSG-010, corrected by WP-37 against `24 §9` and `24 §6.2`.
--
-- `24 §9` keys durable work identity, in order, on "an existing explicit source
-- alias or Office-confirmed relationship; exact Candidate, actual Client,
-- worked date and **compatible** schedule/Contract facts; unique prior lineage;
-- Office confirmation when more than one plausible relationship remains.  It
-- never guesses."  It does NOT make the exact Actual start and end the
-- identity, and it says in terms that the evidence values "may change during
-- correction".  `24 §6.2` ("Paid eight hours becomes nine") and `24 §6.3`
-- ("…becomes seven") are later source changes on ONE root.
--
-- Two source rows on the same worked date therefore describe the same work
-- record when their worked intervals are compatible, which for a schedule is
-- that they overlap: 09:00-17:00 corrected to 09:00-18:00 is one shift, while
-- an early and a night shift on the same date are two.  A row whose interval is
-- not known cannot establish compatibility and never merges with an existing
-- root: that is the safe direction, because a wrong merge silently nets two
-- shifts into one while a wrong split is visible as an extra root.
create or replace function private.weekly_source_work_event_schedule_compatible_v1(
  p_work_event_id uuid,
  p_start_at_local timestamp without time zone,
  p_end_at_local timestamp without time zone
) returns boolean
language sql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
  select p_work_event_id is not null
    and p_start_at_local is not null
    and p_end_at_local is not null
    and exists(
      select 1
      from public.weekly_work_event_source_links link
      join public.weekly_source_upload_rows prior_row on prior_row.id=link.upload_row_id
      where link.work_event_id=p_work_event_id
        and prior_row.start_at_local is not null
        and prior_row.end_at_local is not null
        and prior_row.start_at_local<p_end_at_local
        and p_start_at_local<prior_row.end_at_local
    );
$function$;

create or replace function public.weekly_source_projection_rows_apply_atomic_v1(
  p_actor_user_id uuid,
  p_publication_id uuid,
  p_rows jsonb
) returns jsonb
language plpgsql
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_publication public.weekly_source_projection_publications%rowtype;
  v_upload public.weekly_source_uploads%rowtype;
  v_profile public.weekly_source_format_profiles%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_scope public.weekly_source_report_scopes%rowtype;
  v_correction public.weekly_final_source_correction_sessions%rowtype;
  v_row_json jsonb;
  v_source_row public.weekly_source_upload_rows%rowtype;
  v_contract public.contracts%rowtype;
  v_work_event public.weekly_work_events%rowtype;
  v_resolution_id uuid;
  v_work_event_id uuid;
  v_mapping_state text;
  v_selection_method text;
  v_candidate_id uuid;
  v_client_id uuid;
  v_contract_id uuid;
  v_prior_work_event_id uuid;
  v_identity_kind text;
  v_link_kind text;
  v_work_event_match_kind text;
  v_work_event_match_fingerprint bytea;
  v_work_event_was_present boolean;
  v_profile_external_key text;
  v_durable_identity_hash bytea;
  -- WP-37: the `24 §9` step-2 candidate set, counted explicitly.  Safety is
  -- never expressed through `limit`/`order by` here: anything other than
  -- exactly one compatible root takes a named branch.
  v_compatible_count integer;
  v_compatible_event_id uuid;
  v_eligible_ids jsonb;
  v_eligible_hash bytea;
  v_generation integer;
  v_applied integer:=0;
  v_expected integer;
  v_existing integer;
  v_qualification jsonb;
  v_charge jsonb;
  v_economic jsonb;
  v_effective_policy jsonb;
  v_contract_settings_authority jsonb;
  v_canonical_economic jsonb;
  v_contract_rate_fingerprint bytea;
  v_effective_policy_fingerprint bytea;
  v_invoice_vat_policy_fingerprint bytea;
  v_calculation_fingerprint bytea;
  v_expense_policy_snapshot_hash bytea;
  v_source_mode text;
  v_row_sign smallint;
  v_invoice_vat_chargeable boolean;
  v_invoice_vat_rate_pct numeric;
  v_source_expense_vat_enabled boolean;
  v_source_authority boolean;
  v_has_worked_tuple boolean;
  v_expected_pay_pence bigint;
  v_expected_charge_pence bigint;
  v_result text;
  v_passed boolean;
  v_charge_source_pence bigint;
  v_charge_calculated_pence bigint;
  v_charge_difference_pence bigint;
  v_charge_row_sign_kind text;
  v_charge_claimed_result text;
  v_charge_expected_result text;
  v_charge_expected_sign_kind text;
  v_mode_a_policy jsonb;
begin
  if coalesce(current_setting('request.jwt.claim.role',true),
       nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  if p_actor_user_id is null or p_publication_id is null
     or pg_catalog.jsonb_typeof(p_rows)<>'array' then
    raise exception 'WEEKLY_SOURCE_PROJECTION_INPUT_INVALID' using errcode='22023';
  end if;

  -- Read immutable identity first, then enter the same scope lock order as
  -- begin/publish before taking any row lock. Exact retries can therefore race
  -- safely without forming a publication-to-scope lock cycle.
  select * into v_publication
  from public.weekly_source_projection_publications
  where id=p_publication_id;
  if not found then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_NOT_FOUND' using errcode='22023';
  end if;
  select * into strict v_upload
  from public.weekly_source_uploads
  where id=v_publication.upload_id;
  select * into strict v_cycle
  from public.weekly_source_cycles
  where id=v_publication.source_cycle_id;
  select * into strict v_group
  from public.weekly_source_groups
  where id=v_cycle.source_group_id;
  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended(
      pg_catalog.encode(v_upload.declared_scope_fingerprint,'hex'),73241837
    )
  );

  if v_publication.authority_scope_kind='NHSP_REPORT_SCOPE' then
    select * into strict v_cycle
    from public.weekly_source_cycles
    where id=v_publication.source_cycle_id;
    select * into strict v_scope
    from public.weekly_source_report_scopes
    where id=v_publication.report_scope_id
    for update;
  else
    select * into strict v_cycle
    from public.weekly_source_cycles
    where id=v_publication.source_cycle_id
    for update;
  end if;
  select * into strict v_publication
  from public.weekly_source_projection_publications
  where id=p_publication_id
  for update;
  if v_publication.state not in ('BUILDING','CORRECTION_READY')
     or (v_publication.state='CORRECTION_READY'
         and v_publication.correction_session_id is null) then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_NOT_BUILDING' using errcode='55000';
  end if;
  select * into strict v_upload
  from public.weekly_source_uploads
  where id=v_publication.upload_id
  for share;
  select * into strict v_profile
  from public.weekly_source_format_profiles profile
  where profile.id=v_upload.source_format_profile_id and profile.active;

  if v_publication.authority_scope_kind='NHSP_REPORT_SCOPE' then
    if v_scope.version is distinct from v_publication.authority_scope_version
       or (
         v_publication.correction_session_id is null
         and (v_scope.current_complete_upload_id is distinct from v_upload.id
              or v_scope.projection_state<>'REBUILDING')
       ) then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_STALE' using errcode='40001';
    end if;
  elsif v_cycle.version is distinct from v_publication.authority_scope_version
        or (
          v_publication.correction_session_id is null
          and (v_cycle.current_complete_upload_id is distinct from v_upload.id
               or v_cycle.projection_state<>'REBUILDING')
        ) then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_STALE' using errcode='40001';
  end if;

  if v_publication.correction_session_id is not null then
    select * into v_correction
    from public.weekly_final_source_correction_sessions
    where id=v_publication.correction_session_id for update;
    if not found
       or v_correction.source_cycle_id is distinct from v_cycle.id
       or v_correction.authority_scope_kind is distinct from v_publication.authority_scope_kind
       or v_correction.report_scope_id is distinct from v_publication.report_scope_id
       or v_correction.replacement_correction_upload_id is distinct from v_upload.id
       or v_correction.actor_user_id is distinct from p_actor_user_id
       or v_upload.purpose<>'FINAL_SOURCE_CORRECTION'
       or v_upload.state<>'CORRECTION_READY'
       or (v_publication.state='BUILDING'
         and v_publication.rebuild_idempotency_key is null and (
         v_correction.state<>'READY'
         or v_correction.replacement_projection_publication_id is not null
       ))
       or (v_publication.state='BUILDING'
         and v_publication.rebuild_idempotency_key is not null and (
         v_publication.projection_generation is null
         or v_publication.ready_session_version is null
         or v_correction.state<>'REVIEWED'
         or v_correction.version<>v_publication.ready_session_version
         or v_correction.replacement_projection_publication_id is null
         or v_correction.replacement_projection_publication_id=v_publication.id
       ))
       or (v_publication.state='CORRECTION_READY'
         and v_correction.replacement_projection_publication_id
               is distinct from v_publication.id)
       or coalesce(v_scope.current_final_revision_id,v_cycle.current_final_revision_id)
            is distinct from v_correction.expected_current_final_revision_id then
      raise exception 'WEEKLY_SOURCE_CORRECTION_PUBLICATION_STALE' using errcode='40001';
    end if;
  elsif v_upload.purpose<>'ORDINARY' or v_upload.state<>'CURRENT' then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_STALE' using errcode='40001';
  end if;

  perform private.weekly_source_office_authority_v1(
    p_actor_user_id,'RECHECK_SOURCE',v_group.id,
    case when v_publication.authority_scope_kind='NHSP_REPORT_SCOPE' then v_scope.client_id else null end,
    v_cycle.finalisation_week_ending
  );

  if v_publication.authority_scope_version>2147483647
     or coalesce(v_publication.projection_generation,0)>2147483647 then
    raise exception 'WEEKLY_SOURCE_PROJECTION_GENERATION_OVERFLOW' using errcode='22003';
  end if;
  v_generation:=coalesce(
    v_publication.projection_generation,
    v_publication.authority_scope_version::integer
  );

  select pg_catalog.count(*)::integer into v_expected
  from public.weekly_source_upload_rows source_row
  where source_row.upload_id=v_upload.id;
  if pg_catalog.jsonb_array_length(p_rows)<>v_expected then
    raise exception 'WEEKLY_SOURCE_PROJECTION_ROW_CENSUS_MISMATCH'
      using errcode='22023',detail=pg_catalog.jsonb_build_object(
        'expected',v_expected,'received',pg_catalog.jsonb_array_length(p_rows)
      )::text;
  end if;

  select pg_catalog.count(*)::integer into v_existing
  from public.weekly_source_row_resolutions resolution
  join public.weekly_source_upload_rows source_row
    on source_row.id=resolution.upload_row_id
  where source_row.upload_id=v_upload.id
    and resolution.generation=v_generation;
  if v_publication.state='CORRECTION_READY' or v_existing>0 then
    if v_existing<>v_expected or exists(
      select 1
      from public.weekly_source_upload_rows source_row
      where source_row.upload_id=v_upload.id
        and not exists(
          select 1
          from public.weekly_source_row_resolutions resolution
          where resolution.upload_row_id=source_row.id
            and resolution.generation=v_generation
            and resolution.source_row_fingerprint=source_row.normalised_row_hash
        )
    ) then
      raise exception 'WEEKLY_SOURCE_PROJECTION_RESOLUTION_CENSUS_INCOMPLETE'
        using errcode='55000';
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',true,'publication_id',v_publication.id,'upload_id',v_upload.id,
      'authority_scope_version',v_publication.authority_scope_version,
      'generation',v_generation,'applied_row_count',v_existing,'idempotent',true
    );
  end if;

  for v_row_json in select value from pg_catalog.jsonb_array_elements(p_rows)
  loop
    if pg_catalog.jsonb_typeof(v_row_json)<>'object' then
      raise exception 'WEEKLY_SOURCE_PROJECTION_ROW_INVALID' using errcode='22023';
    end if;
    select * into strict v_source_row
    from public.weekly_source_upload_rows source_row
    where source_row.id=(v_row_json->>'upload_row_id')::uuid
      and source_row.upload_id=v_upload.id
    for share;

    if exists(
      select 1 from public.weekly_source_row_resolutions resolution
      where resolution.upload_row_id=v_source_row.id and resolution.generation=v_generation
    ) then
      raise exception 'WEEKLY_SOURCE_PROJECTION_ROW_ALREADY_APPLIED' using errcode='23505';
    end if;

    v_mapping_state:=pg_catalog.upper(pg_catalog.btrim(coalesce(v_row_json->>'mapping_state','')));
    if v_mapping_state not in (
      'RESOLVED','CANDIDATE_NOT_FOUND','CLIENT_NOT_FOUND','NO_ELIGIBLE_CONTRACT',
      'AMBIGUOUS_CONTRACT','CONTRACT_SELECTION_REQUIRED','SOURCE_ROW_BLOCKED'
    ) then
      raise exception 'WEEKLY_SOURCE_MAPPING_STATE_INVALID' using errcode='22023';
    end if;
    v_candidate_id:=nullif(v_row_json->>'candidate_id','')::uuid;
    v_client_id:=nullif(v_row_json->>'client_id','')::uuid;
    v_contract_id:=nullif(v_row_json->>'contract_id','')::uuid;
    v_prior_work_event_id:=nullif(v_row_json->>'prior_work_event_id','')::uuid;
    v_selection_method:=nullif(pg_catalog.upper(pg_catalog.btrim(coalesce(v_row_json->>'contract_selection_method',''))),'');
    v_work_event_id:=null;
    v_link_kind:=null;
    v_work_event_match_kind:=null;
    v_work_event_match_fingerprint:=null;
    v_work_event_was_present:=false;
    v_compatible_count:=0;
    v_compatible_event_id:=null;
    v_economic:=v_row_json->'economic_snapshot';
    v_effective_policy:=null;
    v_contract_settings_authority:=null;
    v_canonical_economic:=null;
    v_contract_rate_fingerprint:=null;
    v_effective_policy_fingerprint:=null;
    v_invoice_vat_policy_fingerprint:=null;
    v_calculation_fingerprint:=null;
    v_expense_policy_snapshot_hash:=null;
    v_source_mode:=null;
    v_row_sign:=null;
    v_invoice_vat_chargeable:=null;
    v_invoice_vat_rate_pct:=null;
    v_source_expense_vat_enabled:=null;
    v_source_authority:=false;
    v_has_worked_tuple:=false;

    v_eligible_ids:=coalesce((
      select pg_catalog.jsonb_agg(contract_id order by contract_id)
      from (
        select distinct value as contract_id
        from pg_catalog.jsonb_array_elements_text(coalesce(v_row_json->'qualifying_contract_ids','[]'::jsonb))
      ) ordered
    ),'[]'::jsonb);
    v_eligible_hash:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_QUALIFYING_CONTRACT_SET_V1',v_eligible_ids
    );

    if v_mapping_state='RESOLVED' then
      if v_candidate_id is null or v_client_id is null or v_contract_id is null
         or v_selection_method not in ('AUTO_UNIQUE','OFFICE_SELECTED','DURABLE_LINEAGE') then
        raise exception 'WEEKLY_SOURCE_RESOLVED_MAPPING_INCOMPLETE' using errcode='22023';
      end if;
      select * into strict v_contract
      from public.contracts contract
      where contract.id=v_contract_id
        and contract.candidate_id=v_candidate_id
        and contract.client_id=v_client_id
        and v_source_row.work_date between contract.start_date and coalesce(contract.end_date,'infinity'::date);
      if not (v_eligible_ids ? v_contract_id::text) then
        raise exception 'WEEKLY_SOURCE_SELECTED_CONTRACT_NOT_QUALIFIED' using errcode='22023';
      end if;
      -- G6-4: the selection method must be provable from server-derived
      -- facts, not asserted by the caller. Authority: 24 §8 ("If one Contract
      -- remains, the system selects it… The system must never offer a chooser
      -- when only one Contract is eligible"), 24 §9 (unique prior lineage),
      -- 25 §7, XSG-011, XSG-012.
      if v_selection_method='AUTO_UNIQUE'
         and pg_catalog.jsonb_array_length(v_eligible_ids)<>1 then
        raise exception 'WEEKLY_SOURCE_AUTO_UNIQUE_NOT_UNIQUE' using errcode='22023',
          detail=pg_catalog.jsonb_build_object(
            'source_row_ordinal',v_source_row.source_row_ordinal,
            'qualifying_contract_count',pg_catalog.jsonb_array_length(v_eligible_ids)
          )::text;
      end if;
      if v_selection_method='OFFICE_SELECTED'
         and pg_catalog.jsonb_array_length(v_eligible_ids)<2 then
        raise exception 'WEEKLY_SOURCE_OFFICE_CHOICE_NOT_WARRANTED' using errcode='22023',
          detail=pg_catalog.jsonb_build_object(
            'source_row_ordinal',v_source_row.source_row_ordinal,
            'qualifying_contract_count',pg_catalog.jsonb_array_length(v_eligible_ids)
          )::text;
      end if;
      if v_selection_method='DURABLE_LINEAGE' then
        if v_prior_work_event_id is null then
          raise exception 'WEEKLY_SOURCE_DURABLE_LINEAGE_UNPROVEN' using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'source_row_ordinal',v_source_row.source_row_ordinal,
              'reason','PRIOR_WORK_EVENT_ABSENT'
            )::text;
        end if;
        perform 1
        from public.weekly_work_events prior_event
        join public.weekly_source_row_resolutions prior_resolution
          on prior_resolution.work_event_id=prior_event.id
        where prior_event.id=v_prior_work_event_id
          and prior_event.candidate_id=v_candidate_id
          and prior_event.client_id=v_client_id
          and prior_event.first_source_group_id=v_group.id
          and prior_resolution.mapping_state='RESOLVED'
          and prior_resolution.contract_id=v_contract_id
          and prior_resolution.upload_row_id<>v_source_row.id;
        if not found then
          raise exception 'WEEKLY_SOURCE_DURABLE_LINEAGE_UNPROVEN' using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'source_row_ordinal',v_source_row.source_row_ordinal,
              'reason','NO_PRIOR_RESOLVED_CONTRACT_FOR_WORK_EVENT'
            )::text;
        end if;
      end if;

      v_effective_policy:=private._weekly_source_effective_policy_v1(
        v_client_id,v_contract_id,v_source_row.work_date
      );
      v_effective_policy_fingerprint:=private.weekly_source_projection_hex32_v1(
        v_effective_policy->>'policy_sha256','WEEKLY_SOURCE_POLICY_FINGERPRINT_INVALID'
      );
      if nullif(v_row_json->>'effective_policy_fingerprint','') is not null
         and private.weekly_source_projection_hex32_v1(
           v_row_json->>'effective_policy_fingerprint','WEEKLY_SOURCE_POLICY_FINGERPRINT_INVALID'
         ) is distinct from v_effective_policy_fingerprint then
        raise exception 'WEEKLY_SOURCE_POLICY_STALE' using errcode='40001';
      end if;
      v_source_authority:=coalesce(v_effective_policy->>'authority_mode','')='SOURCE_AUTHORITY';
      v_source_mode:=nullif(v_effective_policy->>'c1_source_mode','');
      if v_source_authority and (
           coalesce((v_effective_policy->>'self_bill_enabled')::boolean,false) is not true
           or v_source_mode not in ('NHSP_WEEKLY','HEALTHROSTER_WEEKLY')
         ) then
        raise exception 'WEEKLY_SOURCE_C1_MODE_UNRESOLVED' using errcode='55000';
      end if;
      if not v_source_authority and v_source_mode is not null then
        raise exception 'WEEKLY_SOURCE_TIMESHEET_AUTHORITY_C1_MODE_FORBIDDEN' using errcode='55000';
      end if;
      if (v_profile.profile_code in ('NHSP_PREFINAL_RELEASED_V1','NHSP_FINAL_BACKING_V1'))
           is distinct from (v_source_mode='NHSP_WEEKLY')
         and v_source_authority then
        raise exception 'WEEKLY_SOURCE_PROFILE_MODE_MISMATCH' using errcode='55000';
      end if;

      v_contract_settings_authority:=private._contract_settings_effective_core_v1(
        v_client_id,v_contract_id,v_source_row.work_date,'IMPORT',null
      );
      v_invoice_vat_chargeable:=coalesce(
        (v_contract_settings_authority#>>'{values,client_vat_chargeable}')::boolean,true
      );
      v_invoice_vat_rate_pct:=case when v_invoice_vat_chargeable then
        coalesce((v_contract_settings_authority#>>'{values,vat_rate_pct}')::numeric,20)
        else 0::numeric end;
      if v_invoice_vat_rate_pct<0 or v_invoice_vat_rate_pct>100 then
        raise exception 'WEEKLY_SOURCE_INVOICE_VAT_RATE_INVALID' using errcode='22023';
      end if;
      v_source_expense_vat_enabled:=coalesce(
        (v_effective_policy->>'source_expense_vat_enabled')::boolean,false
      );
      v_invoice_vat_policy_fingerprint:=private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_INVOICE_VAT_POLICY_V1',
        pg_catalog.jsonb_build_object(
          'client_id',v_client_id,'contract_id',v_contract_id,'work_date',v_source_row.work_date,
          'client_settings_id',v_contract_settings_authority->>'client_settings_id',
          'client_settings_effective_from',v_contract_settings_authority->>'client_settings_effective_from',
          'finance_settings_id',v_contract_settings_authority->>'finance_settings_id',
          'finance_settings_date_from',v_contract_settings_authority->>'finance_settings_date_from',
          'client_vat_chargeable',v_invoice_vat_chargeable,
          'vat_rate_pct',v_invoice_vat_rate_pct,
          'source_expense_vat_enabled',v_source_expense_vat_enabled,
          'settings_authority_fingerprint',v_contract_settings_authority->>'authority_fingerprint'
        )
      );

      -- G8-1/G8-2 (XSG-009).  `24 §14` and `25 §9`: a signed-Timesheet-authority
      -- row belongs to the established validation-only Mode A journey, never to
      -- source finalisation, and "merely labelling the row as Timesheet evidence
      -- is insufficient".  The unified upload owner therefore records the
      -- resolved Mode A authority as production data on the
      -- `weekly_timesheet_authority_resolutions` relation, which until now had
      -- no writer outside a verification fixture.
      -- `Reference required before pay` and `require_reference_to_invoice` come
      -- from the installed contract-settings authority and default to false;
      -- the auto-authorisation decision is taken from the established resolver
      -- `public.import_auto_authorise_policy_resolve_v2` in validation context,
      -- exactly as `public._import_review_auto_authorise_targets_core_v1` reads
      -- it, so no second policy engine is introduced.
      if not v_source_authority then
        if coalesce(v_effective_policy->>'document_mode','')<>'INVOICE_EVIDENCE_REQUIRED'
           or coalesce((v_effective_policy->>'self_bill_enabled')::boolean,false) then
          raise exception 'WEEKLY_SOURCE_TIMESHEET_AUTHORITY_POLICY_INVALID' using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'source_row_ordinal',v_source_row.source_row_ordinal,
              'document_mode',v_effective_policy->>'document_mode',
              'self_bill_enabled',v_effective_policy->>'self_bill_enabled'
            )::text;
        end if;
        v_mode_a_policy:=public.import_auto_authorise_policy_resolve_v2(
          case when v_profile.profile_code in ('NHSP_PREFINAL_RELEASED_V1','NHSP_FINAL_BACKING_V1')
            then 'NHSP'::public.hr_source_enum
            else 'HEALTHROSTER'::public.hr_source_enum end,
          v_client_id,v_contract_id,null,v_source_row.work_date,true
        );
        insert into public.weekly_timesheet_authority_resolutions(
          source_cycle_id,client_id,contract_id,work_date,authority_mode,document_mode,
          require_reference_to_pay,require_reference_to_invoice,auto_authorise_enabled,
          effective_policy_fingerprint
        ) values (
          v_cycle.id,v_client_id,v_contract_id,v_source_row.work_date,
          'TIMESHEET_AUTHORITY','INVOICE_EVIDENCE_REQUIRED',
          coalesce((v_contract_settings_authority#>>'{values,require_reference_to_pay}')::boolean,false),
          coalesce((v_contract_settings_authority#>>'{values,require_reference_to_invoice}')::boolean,false),
          coalesce((v_mode_a_policy->>'effective_value')::boolean,false),
          v_effective_policy_fingerprint
        ) on conflict (source_cycle_id,contract_id,work_date,effective_policy_fingerprint) do nothing;
      end if;

      v_has_worked_tuple:=v_source_row.start_at_local is not null
        and v_source_row.end_at_local is not null
        and v_source_row.break_minutes is not null
        and coalesce(v_source_row.actual_net_minutes,0)>0;

      v_identity_kind:=pg_catalog.upper(pg_catalog.btrim(coalesce(v_row_json->>'identity_kind','')));
      if v_identity_kind not in ('PROFILE_EXTERNAL_KEY','SCHEDULE_TUPLE') then
        raise exception 'WEEKLY_SOURCE_WORK_EVENT_IDENTITY_INVALID' using errcode='22023';
      end if;
      if v_profile.profile_code in (
        'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1',
        'HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1',
        'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1'
      ) and v_identity_kind<>'PROFILE_EXTERNAL_KEY' then
        raise exception 'WEEKLY_SOURCE_PROFILE_EXTERNAL_EVENT_KEY_REQUIRED' using errcode='22023';
      end if;
      -- WP-58 / WP-50 finding F7, the mirror image of the guard above.
      -- Pack 24 section 1 supersedes "the NHSP Reference Number is the durable
      -- identity of the real shift", and 24 section 9 states the Reference
      -- Number is "not sole durable work identity": it, the ward and the
      -- location may all change during a correction. An NHSP shift is the same
      -- shift as another by the schedule tuple
      -- (private.weekly_source_work_event_schedule_compatible_v1), never by the
      -- reference. Reference-keyed identity on an NHSP row makes a reversal and
      -- its re-issue three separate work events for one corrected shift, which
      -- is what WP-50 executed. Until this guard existed, the only thing
      -- sending SCHEDULE_TUPLE for NHSP was one ternary in the broker
      -- (broker/src/weekly-source/upload-publication-owner.mjs), so the rule
      -- held on exactly one route. It is enforced here, where no caller can go
      -- round it.
      if v_profile.profile_code in (
        'NHSP_PREFINAL_RELEASED_V1',
        'NHSP_FINAL_BACKING_V1'
      ) and v_identity_kind<>'SCHEDULE_TUPLE' then
        raise exception 'WEEKLY_SOURCE_SCHEDULE_TUPLE_EVENT_KEY_REQUIRED' using errcode='22023';
      end if;
      v_profile_external_key:=case when v_identity_kind='PROFILE_EXTERNAL_KEY'
        then nullif(pg_catalog.btrim(coalesce(v_row_json->>'profile_external_key','')),'') end;
      if v_identity_kind='PROFILE_EXTERNAL_KEY'
         and (v_profile_external_key is null
              or v_profile_external_key is distinct from nullif(pg_catalog.btrim(v_source_row.external_source_key),'')) then
        raise exception 'WEEKLY_SOURCE_EXTERNAL_EVENT_KEY_REQUIRED' using errcode='22023';
      end if;

      if v_prior_work_event_id is not null then
        select * into strict v_work_event
        from public.weekly_work_events work_event
        where work_event.id=v_prior_work_event_id
          and work_event.candidate_id=v_candidate_id
          and work_event.client_id=v_client_id
          and work_event.first_source_group_id=v_group.id;
        if v_identity_kind='PROFILE_EXTERNAL_KEY'
           and exists(
             select 1
             from public.weekly_work_events other_event
             where other_event.first_source_group_id=v_group.id
               and other_event.source_format_profile_id=v_upload.source_format_profile_id
               and other_event.identity_kind='PROFILE_EXTERNAL_KEY'
               and other_event.profile_external_key=v_profile_external_key
               and other_event.id<>v_work_event.id
           ) then
          raise exception 'WEEKLY_SOURCE_PROFILE_EVENT_KEY_CONFLICT' using errcode='55000';
        end if;
        v_work_event_id:=v_work_event.id;
        v_work_event_match_kind:='EXACT_DURABLE_LINEAGE';
      else
        if v_identity_kind='PROFILE_EXTERNAL_KEY' then
          v_durable_identity_hash:=private.weekly_source_sha256_jsonb_v1(
            'WEEKLY_WORK_EVENT_PROFILE_KEY_V1',
            pg_catalog.jsonb_build_object(
              'source_group_id',v_group.id,
              'source_format_profile_id',v_upload.source_format_profile_id,
              'profile_external_key',v_profile_external_key
            )
          );
        else
          -- WP-37 / `24 §9` step 2 and step 4.  The guarded set is read only
          -- after the family lock is held, so a concurrent publication cannot
          -- create a second root for the same work between this read and the
          -- insert below.
          perform pg_catalog.pg_advisory_xact_lock(
            pg_catalog.hashtextextended(
              v_group.id::text||'|'||v_upload.source_format_profile_id::text||'|'
                ||v_candidate_id::text||'|'||v_client_id::text||'|'
                ||v_source_row.work_date::text,
              17092026
            )
          );
          select pg_catalog.count(*)::integer,
                 pg_catalog.min(work_event.id::text)::uuid
            into v_compatible_count,v_compatible_event_id
          from public.weekly_work_events work_event
          where work_event.first_source_group_id=v_group.id
            and work_event.source_format_profile_id=v_upload.source_format_profile_id
            and work_event.identity_kind='SCHEDULE_TUPLE'
            and work_event.candidate_id=v_candidate_id
            and work_event.client_id=v_client_id
            and work_event.work_date=v_source_row.work_date
            and private.weekly_source_work_event_schedule_compatible_v1(
                  work_event.id,v_source_row.start_at_local,v_source_row.end_at_local
                );
          if v_compatible_count>1 then
            -- `24 §9`: "Office confirmation when more than one plausible
            -- relationship remains.  It never guesses."  No root is chosen.
            raise exception 'WEEKLY_SOURCE_WORK_EVENT_IDENTITY_AMBIGUOUS' using errcode='55000',
              detail=pg_catalog.jsonb_build_object(
                'source_row_ordinal',v_source_row.source_row_ordinal,
                'candidate_id',v_candidate_id,'client_id',v_client_id,
                'work_date',v_source_row.work_date,
                'compatible_work_event_count',v_compatible_count
              )::text;
          end if;
          -- The hash still carries this row's own schedule, so a genuinely
          -- different, non-overlapping shift on the same date gets its own
          -- durable identity.  It is a uniqueness token for a NEW root, not
          -- the identity rule: the rule is the compatible-schedule set above.
          v_durable_identity_hash:=private.weekly_source_sha256_jsonb_v1(
            'WEEKLY_WORK_EVENT_SCHEDULE_TUPLE_V1',
            pg_catalog.jsonb_build_object(
              'source_group_id',v_group.id,'source_format_profile_id',v_upload.source_format_profile_id,
              'candidate_id',v_candidate_id,'client_id',v_client_id,'work_date',v_source_row.work_date,
              'start_at_local',v_source_row.start_at_local,'end_at_local',v_source_row.end_at_local
            )
          );
        end if;
        if v_identity_kind='SCHEDULE_TUPLE' and v_compatible_count=1 then
          select * into strict v_work_event
          from public.weekly_work_events work_event
          where work_event.id=v_compatible_event_id;
          v_work_event_was_present:=true;
        else
          select exists(
            select 1 from public.weekly_work_events work_event
            where work_event.durable_identity_hash=v_durable_identity_hash
          ) into v_work_event_was_present;
          insert into public.weekly_work_events(
            candidate_id,client_id,work_date,identity_kind,profile_external_key,
            durable_identity_hash,first_source_group_id,source_format_profile_id
          ) values (
            v_candidate_id,v_client_id,v_source_row.work_date,v_identity_kind,
            v_profile_external_key,v_durable_identity_hash,v_group.id,v_upload.source_format_profile_id
          ) on conflict (durable_identity_hash) do nothing;
          select * into strict v_work_event
          from public.weekly_work_events work_event
          where work_event.durable_identity_hash=v_durable_identity_hash;
        end if;
        if v_work_event.candidate_id is distinct from v_candidate_id
           or v_work_event.client_id is distinct from v_client_id
           or v_work_event.identity_kind is distinct from v_identity_kind
           or v_work_event.profile_external_key is distinct from v_profile_external_key
           or v_work_event.first_source_group_id is distinct from v_group.id
           or v_work_event.source_format_profile_id is distinct from v_upload.source_format_profile_id
           or (v_identity_kind='SCHEDULE_TUPLE' and v_work_event.work_date is distinct from v_source_row.work_date) then
          raise exception 'WEEKLY_SOURCE_WORK_EVENT_IDENTITY_COLLISION' using errcode='55000';
        end if;
        v_work_event_id:=v_work_event.id;
        v_work_event_match_kind:=case
          when v_identity_kind='PROFILE_EXTERNAL_KEY' and v_work_event_was_present then 'REUSED_PROFILE_KEY'
          when v_identity_kind='PROFILE_EXTERNAL_KEY' then 'NEW_PROFILE_KEY'
          when v_work_event_was_present then 'EXACT_DURABLE_LINEAGE'
          else 'NEW_SCHEDULE_TUPLE'
        end;
      end if;
      v_work_event_match_fingerprint:=private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_WORK_EVENT_MATCH_V1',
        pg_catalog.jsonb_build_object(
          'upload_row_id',v_source_row.id,'work_event_id',v_work_event_id,
          'match_kind',v_work_event_match_kind,'prior_work_event_id',v_prior_work_event_id,
          'source_row_fingerprint',pg_catalog.encode(v_source_row.normalised_row_hash,'hex')
        )
      );
    end if;

    -- Only a resolved, source-authoritative worked row owns a C1 economic
    -- snapshot.  Timesheet-authoritative validation rows, unfinished rows and
    -- explicit source-zero positions must never cross the pay boundary.
    if v_mapping_state='RESOLVED'
       and v_source_authority
       and v_source_row.row_finalisation_state in ('NOT_APPLICABLE','SOURCE_WORKED') then
      if not v_has_worked_tuple then
        raise exception 'WEEKLY_SOURCE_WORKED_TUPLE_REQUIRED' using errcode='22023';
      end if;
      if pg_catalog.jsonb_typeof(v_economic)<>'object'
         or v_economic->>'schema_version' is distinct from 'WEEKLY_SOURCE_CANONICAL_ECONOMIC_SNAPSHOT_V1'
         or v_economic->>'calculator_version' is distinct from 'WEEKLY_SHIFT_FINANCIAL_SEGMENT_V1'
         or pg_catalog.upper(coalesce(v_economic->>'source_mode','')) is distinct from v_source_mode
         or pg_catalog.upper(coalesce(v_economic->>'rate_method',''))
              is distinct from v_effective_policy->>'weekly_rate_classification_method'
         or pg_catalog.jsonb_typeof(v_economic->'bucket_minutes')<>'object'
         or pg_catalog.jsonb_typeof(v_economic->'hours')<>'object'
         or pg_catalog.jsonb_typeof(v_economic->'pay_rates')<>'object'
         or pg_catalog.jsonb_typeof(v_economic->'charge_rates')<>'object' then
        raise exception 'WEEKLY_SOURCE_ECONOMIC_SNAPSHOT_INVALID' using errcode='22023';
      end if;

      -- A structurally valid NHSP £0 line is still a worked row.  Source money
      -- decides whether a physical row is a reversal only when it is negative;
      -- zero therefore keeps the positive worked-time/pay sign while the
      -- invoice movement retains the exact £0 source value.
      v_row_sign:=case when v_source_mode='NHSP_WEEKLY' then
        case when coalesce(v_source_row.source_shift_charge_pence,0)<0 then -1 else 1 end
        else 1 end;
      if v_row_sign not in (-1,1)
         or coalesce(v_economic->>'sign','')!~'^[-]?[0-9]+$'
         or (v_economic->>'sign')::smallint is distinct from v_row_sign
         or coalesce(v_economic->>'paid_minutes','')!~'^[0-9]+$'
         or (v_economic->>'paid_minutes')::integer is distinct from v_source_row.actual_net_minutes
         or coalesce(v_economic->>'break_minutes','')!~'^[0-9]+$'
         or (v_economic->>'break_minutes')::integer is distinct from v_source_row.break_minutes then
        raise exception 'WEEKLY_SOURCE_ECONOMIC_SOURCE_TUPLE_MISMATCH' using errcode='22023';
      end if;

      if exists(
        select 1
        from (values ('day'),('night'),('sat'),('sun'),('bh')) bucket(name)
        where coalesce(v_economic#>>array['bucket_minutes',bucket.name],'')!~'^[0-9]+$'
           or coalesce(v_economic#>>array['hours',bucket.name],'')!~'^[+-]?[0-9]+([.][0-9]+)?$'
           or coalesce(v_economic#>>array['pay_rates',bucket.name],'')!~'^[+]?[0-9]+([.][0-9]+)?$'
           or coalesce(v_economic#>>array['charge_rates',bucket.name],'')!~'^[+]?[0-9]+([.][0-9]+)?$'
           or (v_economic#>>array['pay_rates',bucket.name])::numeric<=0
           or (v_economic#>>array['charge_rates',bucket.name])::numeric<=0
      ) then
        raise exception 'WEEKLY_SOURCE_ECONOMIC_BUCKET_INVALID' using errcode='22023';
      end if;
      if (
        (v_economic#>>'{bucket_minutes,day}')::integer+
        (v_economic#>>'{bucket_minutes,night}')::integer+
        (v_economic#>>'{bucket_minutes,sat}')::integer+
        (v_economic#>>'{bucket_minutes,sun}')::integer+
        (v_economic#>>'{bucket_minutes,bh}')::integer
      ) is distinct from v_source_row.actual_net_minutes then
        raise exception 'WEEKLY_SOURCE_ECONOMIC_MINUTES_MISMATCH' using errcode='22023';
      end if;
      if exists(
        select 1
        from (values ('day'),('night'),('sat'),('sun'),('bh')) bucket(name)
        where (v_economic#>>array['hours',bucket.name])::numeric is distinct from
          pg_catalog.round(
            ((v_economic#>>array['bucket_minutes',bucket.name])::numeric/60)*v_row_sign,2
          )
      ) then
        raise exception 'WEEKLY_SOURCE_ECONOMIC_HOURS_MISMATCH' using errcode='22023';
      end if;
      if coalesce(v_economic->>'total_pay_pence','')!~'^[-]?[0-9]+$'
         or coalesce(v_economic->>'calculated_charge_pence','')!~'^[-]?[0-9]+$' then
        raise exception 'WEEKLY_SOURCE_ECONOMIC_TOTAL_INVALID' using errcode='22023';
      end if;

      v_expected_pay_pence:=(pg_catalog.round(
        pg_catalog.round(
          pg_catalog.abs((v_economic#>>'{hours,day}')::numeric)*(v_economic#>>'{pay_rates,day}')::numeric+
          pg_catalog.abs((v_economic#>>'{hours,night}')::numeric)*(v_economic#>>'{pay_rates,night}')::numeric+
          pg_catalog.abs((v_economic#>>'{hours,sat}')::numeric)*(v_economic#>>'{pay_rates,sat}')::numeric+
          pg_catalog.abs((v_economic#>>'{hours,sun}')::numeric)*(v_economic#>>'{pay_rates,sun}')::numeric+
          pg_catalog.abs((v_economic#>>'{hours,bh}')::numeric)*(v_economic#>>'{pay_rates,bh}')::numeric,2
        )*100,0
      )::bigint)*v_row_sign;
      v_expected_charge_pence:=(pg_catalog.round(
        pg_catalog.round(
          pg_catalog.abs((v_economic#>>'{hours,day}')::numeric)*(v_economic#>>'{charge_rates,day}')::numeric+
          pg_catalog.abs((v_economic#>>'{hours,night}')::numeric)*(v_economic#>>'{charge_rates,night}')::numeric+
          pg_catalog.abs((v_economic#>>'{hours,sat}')::numeric)*(v_economic#>>'{charge_rates,sat}')::numeric+
          pg_catalog.abs((v_economic#>>'{hours,sun}')::numeric)*(v_economic#>>'{charge_rates,sun}')::numeric+
          pg_catalog.abs((v_economic#>>'{hours,bh}')::numeric)*(v_economic#>>'{charge_rates,bh}')::numeric,2
        )*100,0
      )::bigint)*v_row_sign;
      if (v_economic->>'total_pay_pence')::bigint is distinct from v_expected_pay_pence
         or (v_economic->>'calculated_charge_pence')::bigint is distinct from v_expected_charge_pence then
        raise exception 'WEEKLY_SOURCE_ECONOMIC_TOTAL_MISMATCH' using errcode='22023';
      end if;

      v_contract_rate_fingerprint:=private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_CONTRACT_RATE_AUTHORITY_V1',
        pg_catalog.jsonb_build_object(
          'contract_id',v_contract.id,'contract_updated_at',v_contract.updated_at,
          'rates_json',v_contract.rates_json,'additional_rates_json',v_contract.additional_rates_json,
          'rate_method',v_effective_policy->>'weekly_rate_classification_method',
          'duration_break_tie_rule',v_effective_policy->>'duration_break_tie_rule',
          'canonical_pay_rates',v_economic->'pay_rates',
          'canonical_charge_rates',v_economic->'charge_rates',
          'settings_authority_fingerprint',v_contract_settings_authority->>'authority_fingerprint'
        )
      );
      v_canonical_economic:=pg_catalog.jsonb_build_object(
        'schema_version','WEEKLY_SOURCE_CANONICAL_ECONOMIC_SNAPSHOT_V1',
        'calculator_version','WEEKLY_SHIFT_FINANCIAL_SEGMENT_V1',
        'source_mode',v_source_mode,
        'rate_method',v_effective_policy->>'weekly_rate_classification_method',
        'sign',v_row_sign,
        'paid_minutes',v_source_row.actual_net_minutes,
        'break_minutes',v_source_row.break_minutes,
        'bucket_minutes',v_economic->'bucket_minutes',
        'hours',v_economic->'hours',
        'pay_rates',v_economic->'pay_rates',
        'charge_rates',v_economic->'charge_rates',
        'total_pay_pence',v_expected_pay_pence::text,
        'calculated_charge_pence',v_expected_charge_pence::text,
        'invoice_vat_chargeable',v_invoice_vat_chargeable,
        'invoice_vat_rate_pct',v_invoice_vat_rate_pct,
        'source_expense_vat_enabled',v_source_expense_vat_enabled,
        'contract_and_rate_fingerprint',pg_catalog.encode(v_contract_rate_fingerprint,'hex'),
        'effective_policy_fingerprint',pg_catalog.encode(v_effective_policy_fingerprint,'hex'),
        'invoice_vat_policy_fingerprint',pg_catalog.encode(v_invoice_vat_policy_fingerprint,'hex'),
        'break_allocation',v_economic->'break_allocation'
      );
      v_calculation_fingerprint:=private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_CANONICAL_ECONOMIC_SNAPSHOT_V1',v_canonical_economic
      );
    else
      if pg_catalog.jsonb_typeof(v_economic)='object' then
        raise exception 'WEEKLY_SOURCE_ECONOMIC_SNAPSHOT_NOT_APPLICABLE' using errcode='22023';
      end if;
      if v_mapping_state='RESOLVED' and v_source_authority
         and v_source_row.row_finalisation_state='SOURCE_ABSENT_ZERO'
         and coalesce(v_source_row.actual_net_minutes,-1)<>0 then
        raise exception 'WEEKLY_SOURCE_SOURCE_ZERO_INVALID' using errcode='22023';
      end if;
    end if;

    if v_mapping_state='RESOLVED' then
      v_link_kind:=case
        when not v_source_authority then 'TIMESHEET_EVIDENCE'
        when v_profile.profile_code='NHSP_PREFINAL_RELEASED_V1'
          or v_source_row.row_finalisation_state='SOURCE_UNFINALISED' then 'PROVISIONAL_SOURCE'
        when v_source_row.row_finalisation_state='SOURCE_ABSENT_ZERO' then 'ZERO_SOURCE'
        when v_source_mode='NHSP_WEEKLY' and v_row_sign=-1 then 'FULL_NEGATIVE_SOURCE'
        else 'POSITIVE_SOURCE'
      end;
      if nullif(pg_catalog.upper(pg_catalog.btrim(coalesce(v_row_json->>'link_kind',''))),'')
           is distinct from v_link_kind then
        raise exception 'WEEKLY_SOURCE_LINK_KIND_MISMATCH' using errcode='22023';
      end if;
    elsif nullif(pg_catalog.btrim(coalesce(v_row_json->>'link_kind','')),'') is not null then
      raise exception 'WEEKLY_SOURCE_LINK_NOT_APPLICABLE' using errcode='22023';
    end if;

    insert into public.weekly_source_row_resolutions(
      upload_row_id,generation,candidate_id,client_id,contract_id,work_event_id,
      paid_minutes,rate_classifications_json,mapping_state,blocker_code,
      contract_selection_method,work_event_match_kind,work_event_match_fingerprint,
      qualification_profile_fingerprint,
      qualifying_contract_count,qualifying_contract_set_hash,source_row_fingerprint,
      contract_and_rate_fingerprint,effective_policy_fingerprint
    ) values (
      v_source_row.id,v_generation,v_candidate_id,v_client_id,v_contract_id,v_work_event_id,
      v_source_row.actual_net_minutes,v_canonical_economic,
      v_mapping_state,nullif(v_row_json->>'blocker_code',''),v_selection_method,
      v_work_event_match_kind,v_work_event_match_fingerprint,
      private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_QUALIFICATION_PROFILE_V1',
        pg_catalog.jsonb_build_object(
          'upload_row_id',v_source_row.id,'generation',v_generation,
          'qualifying_contract_ids',v_eligible_ids,'mapping_state',v_mapping_state
        )
      ),
      pg_catalog.jsonb_array_length(v_eligible_ids),v_eligible_hash,
      v_source_row.normalised_row_hash,
      case when v_mapping_state='RESOLVED' then coalesce(
        v_contract_rate_fingerprint,
        private.weekly_source_sha256_jsonb_v1(
          'WEEKLY_SOURCE_NON_ECONOMIC_CONTRACT_AUTHORITY_V1',
          pg_catalog.jsonb_build_object(
            'contract_id',v_contract.id,'contract_updated_at',v_contract.updated_at,
            'rates_json',v_contract.rates_json,'additional_rates_json',v_contract.additional_rates_json,
            'source_row_state',v_source_row.row_finalisation_state
          )
        )
      ) end,
      case when v_mapping_state='RESOLVED' then v_effective_policy_fingerprint end
    ) returning id into v_resolution_id;

    if v_canonical_economic is not null then
      insert into public.weekly_source_row_economic_snapshots(
        row_resolution_id,upload_row_id,generation,work_event_id,
        candidate_id,client_id,contract_id,calculator_version,source_mode,rate_method,row_sign,
        paid_minutes,break_minutes,minutes_day,minutes_night,minutes_sat,minutes_sun,minutes_bh,
        hours_day,hours_night,hours_sat,hours_sun,hours_bh,
        pay_day,pay_night,pay_sat,pay_sun,pay_bh,
        charge_day,charge_night,charge_sat,charge_sun,charge_bh,
        total_pay_pence,calculated_charge_pence,
        invoice_vat_chargeable,invoice_vat_rate_pct,source_expense_vat_enabled,
        canonical_result_json,contract_and_rate_fingerprint,effective_policy_fingerprint,
        invoice_vat_policy_fingerprint,calculation_fingerprint
      ) values (
        v_resolution_id,v_source_row.id,v_generation,v_work_event_id,
        v_candidate_id,v_client_id,v_contract_id,'WEEKLY_SHIFT_FINANCIAL_SEGMENT_V1',
        v_source_mode,v_effective_policy->>'weekly_rate_classification_method',v_row_sign,
        v_source_row.actual_net_minutes,v_source_row.break_minutes,
        (v_economic#>>'{bucket_minutes,day}')::integer,
        (v_economic#>>'{bucket_minutes,night}')::integer,
        (v_economic#>>'{bucket_minutes,sat}')::integer,
        (v_economic#>>'{bucket_minutes,sun}')::integer,
        (v_economic#>>'{bucket_minutes,bh}')::integer,
        (v_economic#>>'{hours,day}')::numeric,
        (v_economic#>>'{hours,night}')::numeric,
        (v_economic#>>'{hours,sat}')::numeric,
        (v_economic#>>'{hours,sun}')::numeric,
        (v_economic#>>'{hours,bh}')::numeric,
        (v_economic#>>'{pay_rates,day}')::numeric,
        (v_economic#>>'{pay_rates,night}')::numeric,
        (v_economic#>>'{pay_rates,sat}')::numeric,
        (v_economic#>>'{pay_rates,sun}')::numeric,
        (v_economic#>>'{pay_rates,bh}')::numeric,
        (v_economic#>>'{charge_rates,day}')::numeric,
        (v_economic#>>'{charge_rates,night}')::numeric,
        (v_economic#>>'{charge_rates,sat}')::numeric,
        (v_economic#>>'{charge_rates,sun}')::numeric,
        (v_economic#>>'{charge_rates,bh}')::numeric,
        v_expected_pay_pence,v_expected_charge_pence,
        v_invoice_vat_chargeable,v_invoice_vat_rate_pct,v_source_expense_vat_enabled,
        v_canonical_economic,v_contract_rate_fingerprint,v_effective_policy_fingerprint,
        v_invoice_vat_policy_fingerprint,v_calculation_fingerprint
      );
    end if;

    if v_mapping_state='RESOLVED'
       and v_source_authority
       and v_profile.profile_code='ROSTER_WEEKLY_SUMMARY_ACTUAL_V1'
       and coalesce((v_effective_policy->>'source_fixed_expenses_enabled')::boolean,false) then
      if v_source_row.source_expense_pence is null
         or v_source_row.source_expense_parse_state not in ('VALID','OMITTED_ZERO')
         or (v_source_row.source_expense_parse_state='OMITTED_ZERO'
             and v_source_row.source_expense_pence<>0) then
        raise exception 'WEEKLY_SOURCE_EXPENSE_POLICY_SNAPSHOT_INPUT_INVALID'
          using errcode='22023';
      end if;
      if v_source_row.source_expense_pence>0 and (
           v_source_row.start_at_local is null or v_source_row.end_at_local is null
           or v_source_row.end_at_local<=v_source_row.start_at_local
           or v_source_row.break_minutes is null
         ) then
        raise exception 'WEEKLY_SOURCE_EXPENSE_DISPLAY_TUPLE_REQUIRED'
          using errcode='22023';
      end if;
      v_expense_policy_snapshot_hash:=private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_ROW_EXPENSE_POLICY_SNAPSHOT_V1',
        pg_catalog.jsonb_build_object(
          'row_resolution_id',v_resolution_id,'upload_row_id',v_source_row.id,
          'generation',v_generation,'work_event_id',v_work_event_id,
          'candidate_id',v_candidate_id,'client_id',v_client_id,
          'contract_id',v_contract_id,'row_finalisation_state',v_source_row.row_finalisation_state,
          'source_expense_pence',v_source_row.source_expense_pence,
          'source_expense_parse_state',v_source_row.source_expense_parse_state,
          'source_expense_vat_enabled',v_source_expense_vat_enabled,
          'invoice_vat_chargeable',v_invoice_vat_chargeable,
          'invoice_vat_rate_pct',v_invoice_vat_rate_pct,
          'correction_presentation',v_effective_policy->>'self_bill_correction_presentation',
          'effective_policy_fingerprint',pg_catalog.encode(v_effective_policy_fingerprint,'hex'),
          'invoice_vat_policy_fingerprint',pg_catalog.encode(v_invoice_vat_policy_fingerprint,'hex')
        )
      );
      insert into public.weekly_source_row_expense_policy_snapshots(
        row_resolution_id,upload_row_id,generation,work_event_id,
        candidate_id,client_id,contract_id,source_expense_pence,
        source_expense_parse_state,source_expense_vat_enabled,
        invoice_vat_chargeable,invoice_vat_rate_pct,correction_presentation,effective_policy_fingerprint,
        invoice_vat_policy_fingerprint,snapshot_hash
      ) values (
        v_resolution_id,v_source_row.id,v_generation,v_work_event_id,
        v_candidate_id,v_client_id,v_contract_id,v_source_row.source_expense_pence,
        v_source_row.source_expense_parse_state,v_source_expense_vat_enabled,
        v_invoice_vat_chargeable,v_invoice_vat_rate_pct,
        v_effective_policy->>'self_bill_correction_presentation',v_effective_policy_fingerprint,
        v_invoice_vat_policy_fingerprint,v_expense_policy_snapshot_hash
      );
    end if;

    if v_mapping_state='RESOLVED' then
      insert into public.weekly_work_event_source_links(
        work_event_id,upload_row_id,row_resolution_id,link_kind,link_hash
      ) values (
        v_work_event_id,v_source_row.id,v_resolution_id,
        v_link_kind,
        private.weekly_source_sha256_jsonb_v1(
          'WEEKLY_SOURCE_WORK_EVENT_LINK_V1',
          pg_catalog.jsonb_build_object(
            'work_event_id',v_work_event_id,'upload_row_id',v_source_row.id,
            'resolution_id',v_resolution_id,'link_kind',v_link_kind
          )
        )
      );
    end if;

    for v_qualification in
      select value from pg_catalog.jsonb_array_elements(coalesce(v_row_json->'qualification_observations','[]'::jsonb))
    loop
      if v_profile.profile_code not in ('NHSP_PREFINAL_RELEASED_V1','NHSP_FINAL_BACKING_V1') then
        raise exception 'WEEKLY_SOURCE_QUALIFICATION_PROFILE_NOT_APPLICABLE' using errcode='22023';
      end if;
      v_result:=pg_catalog.upper(coalesce(v_qualification->>'comparison_result',''));
      -- G6-2/G6-4: a per-Contract qualification verdict is re-derived here
      -- from the immutable source pence and the caller's canonical pence for
      -- that Contract, using the same symmetric NHSP_TWO_COMPONENT_PENCE_V1
      -- rule (25 §6; 24 §8, §13; 14 §4.3 items 1 and 5; NHSP-BR-013). The
      -- eligible set that the selection method is proved against is therefore
      -- the set the server itself would admit. UNVERIFIABLE stays available
      -- as the fail-closed verdict and is never upgraded.
      v_charge_source_pence:=nullif(v_qualification->>'source_shift_charge_pence','')::bigint;
      v_charge_calculated_pence:=nullif(v_qualification->>'canonical_calculated_pence','')::bigint;
      if v_charge_source_pence is distinct from v_source_row.source_shift_charge_pence then
        raise exception 'WEEKLY_SOURCE_QUALIFICATION_SOURCE_PENCE_MISMATCH' using errcode='22023',
          detail=pg_catalog.jsonb_build_object(
            'source_row_ordinal',v_source_row.source_row_ordinal,
            'contract_id',v_qualification->>'contract_id'
          )::text;
      end if;
      v_charge_difference_pence:=case
        when v_charge_source_pence is null or v_charge_calculated_pence is null then null
        else v_charge_source_pence-v_charge_calculated_pence end;
      v_charge_expected_result:=case
        when v_charge_source_pence is null or v_charge_calculated_pence is null then 'UNVERIFIABLE'
        when v_charge_source_pence=0 and v_charge_calculated_pence<>0 then 'ZERO_SOURCE_CHARGE'
        when v_charge_calculated_pence=0 then 'MISMATCH'
        when v_charge_difference_pence=0 then 'EXACT'
        when (v_charge_difference_pence=1 or v_charge_difference_pence=-1)
          and ((v_charge_source_pence>0 and v_charge_calculated_pence>0)
            or (v_charge_source_pence<0 and v_charge_calculated_pence<0))
          then 'SOURCE_ROUNDING_EQUIVALENT'
        else 'MISMATCH' end;
      if v_result<>'UNVERIFIABLE' and v_result is distinct from v_charge_expected_result then
        raise exception 'WEEKLY_SOURCE_QUALIFICATION_RESULT_NOT_REDERIVED' using errcode='22023',
          detail=pg_catalog.jsonb_build_object(
            'source_row_ordinal',v_source_row.source_row_ordinal,
            'contract_id',v_qualification->>'contract_id',
            'claimed_result',v_result,'rederived_result',v_charge_expected_result,
            'signed_difference_pence',v_charge_difference_pence
          )::text;
      end if;
      -- Price does not decide Contract identity. Every safe canonical
      -- observation remains in the reliable base set; warning admission is a
      -- separate Office decision after Contract resolution.
      v_passed:=v_result in ('EXACT','SOURCE_ROUNDING_EQUIVALENT','MISMATCH','ZERO_SOURCE_CHARGE');
      perform 1
      from public.contracts contract
      where contract.id=(v_qualification->>'contract_id')::uuid
        and contract.candidate_id=v_candidate_id
        and contract.client_id=v_client_id
        and v_source_row.work_date between contract.start_date and coalesce(contract.end_date,'infinity'::date)
        and v_eligible_ids ? contract.id::text;
      if not found then
        raise exception 'WEEKLY_SOURCE_QUALIFICATION_CONTRACT_OUT_OF_SCOPE' using errcode='22023';
      end if;
      insert into public.weekly_source_contract_qualification_observations(
        upload_row_id,generation,candidate_id,client_id,contract_id,
        source_format_profile_id,qualification_profile_version,
        qualification_profile_fingerprint,contract_revision_fingerprint,
        source_shift_charge_pence,canonical_calculated_pence,
        source_charge_difference_pence,comparison_result,qualification_passed,
        ordered_reason_codes,evidence_fingerprint
      ) values (
        v_source_row.id,v_generation,v_candidate_id,v_client_id,
        (v_qualification->>'contract_id')::uuid,v_upload.source_format_profile_id,
        'NHSP_TWO_COMPONENT_PENCE_V1',
        private.weekly_source_sha256_jsonb_v1('NHSP_QUALIFICATION_PROFILE_V1',v_qualification),
        private.weekly_source_projection_hex32_v1(
          v_qualification->>'contract_revision_fingerprint','WEEKLY_SOURCE_CONTRACT_REVISION_FINGERPRINT_INVALID'
        ),
        v_charge_source_pence,
        v_charge_calculated_pence,
        v_charge_difference_pence,
        v_result,v_passed,
        coalesce(array(select pg_catalog.jsonb_array_elements_text(v_qualification->'reason_codes')),'{}'::text[]),
        private.weekly_source_sha256_jsonb_v1('NHSP_CONTRACT_QUALIFICATION_EVIDENCE_V1',v_qualification)
      );
    end loop;

    v_charge:=v_row_json->'charge_check';
    if v_charge is not null and pg_catalog.jsonb_typeof(v_charge)='object' then
      if v_profile.profile_code not in ('NHSP_PREFINAL_RELEASED_V1','NHSP_FINAL_BACKING_V1')
         or v_mapping_state<>'RESOLVED' then
        raise exception 'WEEKLY_SOURCE_CHARGE_CHECK_NOT_APPLICABLE' using errcode='22023';
      end if;
      if v_calculation_fingerprint is null then
        raise exception 'WEEKLY_SOURCE_CHARGE_FINGERPRINT_MISSING' using errcode='55000';
      end if;
      if nullif(v_charge->>'charge_calculation_fingerprint','') is not null
         and private.weekly_source_projection_hex32_v1(
               v_charge->>'charge_calculation_fingerprint',
               'WEEKLY_SOURCE_CHARGE_FINGERPRINT_INVALID'
             ) is distinct from v_calculation_fingerprint then
        raise exception 'WEEKLY_SOURCE_CHARGE_FINGERPRINT_MISMATCH' using errcode='22023';
      end if;
      if nullif(v_charge->>'source_commission_pence','')::bigint
            is distinct from v_source_row.source_commission_pence
         or nullif(v_charge->>'source_total_cost_pence','')::bigint
            is distinct from v_source_row.source_total_cost_pence
         or nullif(v_charge->>'source_shift_charge_pence','')::bigint
            is distinct from v_source_row.source_shift_charge_pence
         or nullif(v_charge->>'calculated_segment_charge_pence','')::bigint
            is distinct from v_expected_charge_pence then
        raise exception 'WEEKLY_SOURCE_CHARGE_CHECK_ECONOMIC_MISMATCH' using errcode='22023';
      end if;
      -- G6-2: server-side re-derivation of the NHSP_TWO_COMPONENT_PENCE_V1
      -- verdict. Authority: 25 §6; 24 §13; 14 §4.3 items 1 and 5;
      -- NHSP-BR-013; PRC-006, PRC-007, PRC-020, NHSBR-019, NHSBR-020.
      -- The four inputs are already anchored above to the immutable upload
      -- row and to this server's own canonical calculation, so the verdict
      -- is recomputed here from those facts. A caller may never upgrade a
      -- verdict: anything other than the fail-closed UNVERIFIABLE must equal
      -- the re-derived result, and the sign kind must equal the re-derived
      -- sign kind. EXACT is a zero difference. SOURCE_ROUNDING_EQUIVALENT is
      -- the same non-zero sign on the source and calculated pence with an
      -- absolute difference of exactly one penny in either arithmetic
      -- direction; no directional constraint is permitted.
      v_charge_source_pence:=v_source_row.source_shift_charge_pence;
      v_charge_calculated_pence:=v_expected_charge_pence;
      v_charge_difference_pence:=case
        when v_charge_source_pence is null or v_charge_calculated_pence is null then null
        else v_charge_source_pence-v_charge_calculated_pence end;
      v_charge_expected_sign_kind:=case
        when v_charge_source_pence is null then null
        when v_charge_source_pence>0 then 'POSITIVE'
        when v_charge_source_pence<0 then 'FULL_NEGATIVE'
        when v_charge_calculated_pence>0 then 'POSITIVE'
        when v_charge_calculated_pence<0 then 'FULL_NEGATIVE' end;
      v_charge_expected_result:=case
        when v_source_row.source_commission_pence is null
          or v_source_row.source_total_cost_pence is null
          or v_charge_source_pence is null
          or v_charge_calculated_pence is null then 'UNVERIFIABLE'
        when v_charge_source_pence=0 and v_charge_calculated_pence<>0 then 'ZERO_SOURCE_CHARGE'
        when v_charge_calculated_pence=0 then 'MISMATCH'
        when v_charge_difference_pence=0 then 'EXACT'
        when (v_charge_difference_pence=1 or v_charge_difference_pence=-1)
          and ((v_charge_source_pence>0 and v_charge_calculated_pence>0)
            or (v_charge_source_pence<0 and v_charge_calculated_pence<0))
          then 'SOURCE_ROUNDING_EQUIVALENT'
        else 'MISMATCH' end;
      v_charge_claimed_result:=pg_catalog.upper(pg_catalog.btrim(
        coalesce(v_charge->>'comparison_result','')));
      v_charge_row_sign_kind:=pg_catalog.upper(pg_catalog.btrim(
        coalesce(v_charge->>'row_sign_kind','')));
      if v_charge_claimed_result<>'UNVERIFIABLE'
         and v_charge_claimed_result is distinct from v_charge_expected_result then
        raise exception 'WEEKLY_SOURCE_CHARGE_CHECK_RESULT_NOT_REDERIVED'
          using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'source_row_ordinal',v_source_row.source_row_ordinal,
              'claimed_result',v_charge_claimed_result,
              'rederived_result',v_charge_expected_result,
              'signed_difference_pence',v_charge_difference_pence
            )::text;
      end if;
      if v_charge_claimed_result<>'UNVERIFIABLE'
         and v_charge_row_sign_kind is distinct from v_charge_expected_sign_kind then
        raise exception 'WEEKLY_SOURCE_CHARGE_CHECK_SIGN_NOT_REDERIVED'
          using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'source_row_ordinal',v_source_row.source_row_ordinal,
              'claimed_row_sign_kind',v_charge_row_sign_kind,
              'rederived_row_sign_kind',v_charge_expected_sign_kind
            )::text;
      end if;
      insert into public.weekly_source_charge_checks(
        upload_row_id,row_resolution_id,generation,row_sign_kind,
        source_commission_pence,source_total_cost_pence,source_shift_charge_pence,
        calculated_segment_charge_pence,source_charge_difference_pence,
        comparison_profile_version,comparison_result,comparison_reason_code,
        phase_severity,blocker_code,charge_calculation_fingerprint
      ) values (
        v_source_row.id,v_resolution_id,v_generation,
        coalesce(v_charge_expected_sign_kind,v_charge_row_sign_kind),
        v_source_row.source_commission_pence,
        v_source_row.source_total_cost_pence,
        v_charge_source_pence,
        v_charge_calculated_pence,
        v_charge_difference_pence,
        'NHSP_TWO_COMPONENT_PENCE_V1',v_charge_claimed_result,
        coalesce(v_charge->>'comparison_reason_code',''),pg_catalog.upper(v_charge->>'phase_severity'),
        nullif(v_charge->>'blocker_code',''),
        v_calculation_fingerprint
      );
    end if;

    v_applied:=v_applied+1;
  end loop;

  if v_applied<>v_expected or exists(
    select 1 from public.weekly_source_upload_rows source_row
    where source_row.upload_id=v_upload.id
      and not exists(
        select 1 from public.weekly_source_row_resolutions resolution
        where resolution.upload_row_id=source_row.id and resolution.generation=v_generation
      )
  ) then
    raise exception 'WEEKLY_SOURCE_PROJECTION_RESOLUTION_CENSUS_INCOMPLETE' using errcode='55000';
  end if;

  return pg_catalog.jsonb_build_object(
    'ok',true,'publication_id',v_publication.id,'upload_id',v_upload.id,
    'authority_scope_version',v_publication.authority_scope_version,
    'generation',v_generation,'applied_row_count',v_applied
  );
exception
  when invalid_text_representation or numeric_value_out_of_range then
    raise exception 'WEEKLY_SOURCE_PROJECTION_VALUE_INVALID' using errcode='22023';
end;
$function$;

alter function private.weekly_source_projection_hex32_v1(text,text) owner to postgres;
alter function private.weekly_source_work_event_schedule_compatible_v1(
  uuid,timestamp without time zone,timestamp without time zone) owner to postgres;
alter function public.weekly_source_projection_rows_apply_atomic_v1(uuid,uuid,jsonb) owner to postgres;
revoke all on function private.weekly_source_projection_hex32_v1(text,text)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_work_event_schedule_compatible_v1(
  uuid,timestamp without time zone,timestamp without time zone)
  from public,anon,authenticated,service_role;
revoke all on function public.weekly_source_projection_rows_apply_atomic_v1(uuid,uuid,jsonb)
  from public,anon,authenticated;
grant execute on function public.weekly_source_projection_rows_apply_atomic_v1(uuid,uuid,jsonb)
  to service_role;

comment on function public.weekly_source_projection_rows_apply_atomic_v1(uuid,uuid,jsonb) is
  'Appends one complete immutable mapping/qualification generation to a current BUILDING Weekly Source publication. Creates no Timesheet, invoice or payment.';

notify pgrst, 'reload schema';

commit;
