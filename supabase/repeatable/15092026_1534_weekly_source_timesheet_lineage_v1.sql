-- Repeatable CloudTMS authority: weekly_source_timesheet_lineage_v1
--
-- Binds each current, resolved source row to the ordinary WEEKLY Timesheet and
-- Contract Week identity that existing invoice locks/readers already
-- understand.  This owner deliberately writes no worked/source schedule, financial,
-- invoice, Workbench, Draft or Banking Pay state.

\set ON_ERROR_STOP on

begin;

create or replace function private.weekly_source_contract_week_settings_authority_assert_v1(
  p_contract_week_id uuid
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_snapshot jsonb;
  v_version text;
  v_fingerprint text;
  v_resolved_at timestamptz;
  v_expected text;
begin
  select
    contract_week.settings_authority_json,
    contract_week.settings_authority_version,
    contract_week.settings_authority_fingerprint,
    contract_week.settings_authority_resolved_at
  into v_snapshot,v_version,v_fingerprint,v_resolved_at
  from public.contract_weeks contract_week
  where contract_week.id=p_contract_week_id;
  if not found then
    raise exception 'WEEKLY_SOURCE_CONTRACT_WEEK_NOT_FOUND' using errcode='P0002';
  end if;

  if coalesce(v_snapshot,'{}'::jsonb)='{}'::jsonb
     or v_version is distinct from 'CONTRACT_SETTINGS_AUTHORITY_V1'
     or coalesce(v_fingerprint,'') !~ '^[0-9a-f]{64}$'
     or v_resolved_at is null
     or nullif(v_snapshot->>'resolved_at_utc','') is null then
    raise exception 'WEEKLY_SOURCE_CONTRACT_WEEK_AUTHORITY_NOT_FROZEN' using errcode='55000';
  end if;

  v_expected:=pg_catalog.encode(extensions.digest(pg_catalog.convert_to(
    (v_snapshot-'authority_fingerprint'-'resolved_at_utc')::text,'UTF8'
  ),'sha256'),'hex');
  if v_expected is distinct from v_fingerprint
     or v_snapshot->>'authority_fingerprint' is distinct from v_fingerprint
     or (v_snapshot->>'resolved_at_utc')::timestamptz is distinct from v_resolved_at then
    raise exception 'WEEKLY_SOURCE_CONTRACT_WEEK_AUTHORITY_INVALID' using errcode='22023';
  end if;

  return v_snapshot;
exception
  when invalid_text_representation or datetime_field_overflow then
    raise exception 'WEEKLY_SOURCE_CONTRACT_WEEK_AUTHORITY_INVALID' using errcode='22023';
end;
$function$;

create or replace function private.weekly_source_timesheet_lineage_assert_v1(
  p_row_resolution_id uuid
) returns public.weekly_source_row_timesheet_lineages
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_lineage public.weekly_source_row_timesheet_lineages%rowtype;
  v_resolution public.weekly_source_row_resolutions%rowtype;
  v_contract_week public.contract_weeks%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_contract_week_settings jsonb;
  v_timesheet_settings jsonb;
  v_expected_fingerprint bytea;
  v_root_identity jsonb;
begin
  -- Decision D8: this relation is the per-source-row BINDING and carries no
  -- authorisation generation, so there is exactly one row per resolution.  The
  -- authorisation record lives in public.weekly_source_root_authorisations.
  select * into v_lineage
  from public.weekly_source_row_timesheet_lineages
  where row_resolution_id=p_row_resolution_id;
  if not found then
    raise exception 'WEEKLY_SOURCE_TIMESHEET_LINEAGE_MISSING' using errcode='55000';
  end if;

  select * into strict v_resolution
  from public.weekly_source_row_resolutions
  where id=v_lineage.row_resolution_id;
  select * into strict v_contract_week
  from public.contract_weeks
  where id=v_lineage.contract_week_id;
  select * into strict v_timesheet
  from public.timesheets
  where timesheet_id=v_lineage.timesheet_id;

  -- G6-8 / proof/34 section 5 step 4 and section 6.  The stored physical id is
  -- never trusted on its own: it is resolved through the installed Workbench
  -- rotation authority and compared with the retained family identity and
  -- version.  A rotation observed after first authorisation is
  -- WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE, never a stale rebuild.
  v_root_identity:=private.weekly_source_root_integrity_assert_v1(
    v_lineage.timesheet_id,v_lineage.family_booking_id,v_lineage.timesheet_version
  );

  v_contract_week_settings:=
    private.weekly_source_contract_week_settings_authority_assert_v1(v_contract_week.id);
  v_timesheet_settings:=
    private._timesheet_settings_authority_frozen_v1(v_timesheet.timesheet_id);

  v_expected_fingerprint:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_ROW_TIMESHEET_LINEAGE_V1',
    pg_catalog.jsonb_build_object(
      'row_resolution_id',v_lineage.row_resolution_id,
      'source_cycle_id',v_lineage.source_cycle_id,
      'work_event_id',v_lineage.work_event_id,
      'candidate_id',v_lineage.candidate_id,
      'client_id',v_lineage.client_id,
      'contract_id',v_lineage.contract_id,
      'contract_week_id',v_lineage.contract_week_id,
      'timesheet_id',v_lineage.timesheet_id,
      'week_ending_date',v_lineage.week_ending_date
    )
  );

  if v_resolution.mapping_state<>'RESOLVED'
     or v_resolution.work_event_id is distinct from v_lineage.work_event_id
     or v_resolution.candidate_id is distinct from v_lineage.candidate_id
     or v_resolution.client_id is distinct from v_lineage.client_id
     or v_resolution.contract_id is distinct from v_lineage.contract_id
     or v_contract_week.contract_id is distinct from v_lineage.contract_id
     or v_contract_week.week_ending_date is distinct from v_lineage.week_ending_date
     or v_contract_week.additional_seq<>0
     or v_contract_week.is_adjustment
     or v_contract_week.status='CANCELLED'::public.contract_week_status_enum
     or v_contract_week.timesheet_id is distinct from v_lineage.timesheet_id
     or v_timesheet.contract_id is distinct from v_lineage.contract_id
     or v_timesheet.week_ending_date is distinct from v_lineage.week_ending_date
     or v_timesheet.sheet_scope<>'WEEKLY'::public.timesheet_scope_enum
     or v_timesheet.line_type<>'HOURS'::public.timesheet_line_type_enum
     or v_timesheet.is_adjustment
     or not v_timesheet.is_current
     or v_timesheet.revoked_at is not null
     or v_timesheet.archived_at_utc is not null
     or v_lineage.family_booking_id is distinct from v_timesheet.booking_id
     or v_lineage.timesheet_version is distinct from v_timesheet.version
     or v_lineage.family_booking_id is distinct from
        (v_root_identity->>'family_booking_id')
     or v_lineage.timesheet_version is distinct from
        (v_root_identity->>'canonical_version')::integer
     or coalesce((v_root_identity->>'requested_is_canonical')::boolean,false)
        is not true
     or v_timesheet.settings_authority_resolved_at is null
     or (v_timesheet_settings->>'resolved_at_utc')::timestamptz is distinct from
        v_timesheet.settings_authority_resolved_at
     or v_timesheet.settings_authority_fingerprint is distinct from
        v_contract_week.settings_authority_fingerprint
     or v_timesheet_settings->>'authority_fingerprint' is distinct from
        v_contract_week_settings->>'authority_fingerprint'
     or v_lineage.lineage_fingerprint is distinct from v_expected_fingerprint then
    raise exception 'WEEKLY_SOURCE_TIMESHEET_LINEAGE_INVALID' using errcode='55000';
  end if;

  return v_lineage;
exception
  when no_data_found or too_many_rows then
    raise exception 'WEEKLY_SOURCE_TIMESHEET_LINEAGE_INVALID' using errcode='55000';
end;
$function$;

create or replace function public.weekly_source_timesheet_lineage_ensure_atomic_v1(
  p_row_resolution_id uuid,
  p_actor_user_id uuid
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_resolution public.weekly_source_row_resolutions%rowtype;
  v_source_row public.weekly_source_upload_rows%rowtype;
  v_upload public.weekly_source_uploads%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_scope public.weekly_source_report_scopes%rowtype;
  v_publication public.weekly_source_projection_publications%rowtype;
  v_correction public.weekly_final_source_correction_sessions%rowtype;
  v_profile public.weekly_source_format_profiles%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_event public.weekly_work_events%rowtype;
  v_contract public.contracts%rowtype;
  v_candidate public.candidates%rowtype;
  v_client public.clients%rowtype;
  v_expense_policy public.weekly_source_row_expense_policy_snapshots%rowtype;
  v_policy jsonb;
  v_contract_week public.contract_weeks%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_lineage public.weekly_source_row_timesheet_lineages%rowtype;
  v_guard jsonb;
  v_contract_week_settings jsonb;
  v_timesheet_settings jsonb;
  v_week_ending_date date;
  v_weekday integer;
  v_booking_id text;
  v_booking_material text;
  v_occupant_norm text;
  v_hospital_norm text;
  v_ward_norm text;
  v_role_norm text;
  v_lineage_fingerprint bytea;
  v_expected_expense_policy_hash bytea;
  v_created_timesheet boolean:=false;
  v_existing_lineage boolean:=false;
  v_is_correction boolean:=false;
  v_lock_result jsonb;
  v_family jsonb;
  v_known_root_timesheet_id uuid;
  v_root_authorisation jsonb;
  v_bound_timesheet_id uuid;
  v_finalisation_root_set text;
begin
  if coalesce(
       pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role',
       ''
     )<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  if p_row_resolution_id is null or p_actor_user_id is null then
    raise exception 'WEEKLY_SOURCE_TIMESHEET_LINEAGE_INPUT_REQUIRED' using errcode='22023';
  end if;

  select * into v_resolution
  from public.weekly_source_row_resolutions
  where id=p_row_resolution_id;
  if not found or v_resolution.mapping_state<>'RESOLVED' then
    raise exception 'WEEKLY_SOURCE_RESOLUTION_NOT_CURRENT' using errcode='55000';
  end if;

  select * into strict v_source_row
  from public.weekly_source_upload_rows
  where id=v_resolution.upload_row_id;
  select * into strict v_upload
  from public.weekly_source_uploads
  where id=v_source_row.upload_id;

  -- Lock in the same outer-to-inner order as finalisation.  The initial reads
  -- above discover only the keys; every authoritative fact is re-read after
  -- its owner has been locked.
  select * into v_cycle
  from public.weekly_source_cycles
  where id=v_upload.source_cycle_id
  for update;
  if not found then
    raise exception 'WEEKLY_SOURCE_CYCLE_NOT_FOUND' using errcode='55000';
  end if;

  if v_upload.report_scope_id is not null then
    select * into v_scope
    from public.weekly_source_report_scopes
    where id=v_upload.report_scope_id
    for update;
    if not found or v_scope.source_cycle_id is distinct from v_cycle.id then
      raise exception 'WEEKLY_SOURCE_REPORT_SCOPE_NOT_CURRENT' using errcode='55000';
    end if;
  end if;

  select * into v_upload
  from public.weekly_source_uploads
  where id=v_source_row.upload_id
    and source_cycle_id=v_cycle.id
  for share;
  if not found then
    raise exception 'WEEKLY_SOURCE_UPLOAD_NOT_CURRENT' using errcode='55000';
  end if;

  select * into v_publication
  from public.weekly_source_projection_publications publication
  where publication.source_cycle_id=v_cycle.id
    and publication.upload_id=v_upload.id
    and publication.authority_scope_version=v_resolution.generation
    and (
      (
        publication.state='CURRENT'
        and v_upload.purpose='ORDINARY'
        and v_upload.correction_session_id is null
        and (
          (v_upload.report_scope_id is null
           and publication.authority_scope_kind='CYCLE'
           and publication.report_scope_id is null
           and v_cycle.current_projection_publication_id=publication.id
           and v_cycle.current_complete_upload_id=v_upload.id)
          or
          (v_upload.report_scope_id is not null
           and publication.authority_scope_kind='NHSP_REPORT_SCOPE'
           and publication.report_scope_id=v_upload.report_scope_id
           and v_scope.current_projection_publication_id=publication.id
           and v_scope.current_complete_upload_id=v_upload.id)
        )
      )
      or (
        publication.state='CORRECTION_READY'
        and v_upload.state='CORRECTION_READY'
        and v_upload.purpose='FINAL_SOURCE_CORRECTION'
        and v_upload.correction_session_id is not null
        and publication.correction_session_id=v_upload.correction_session_id
        and (
          (v_upload.report_scope_id is null
           and publication.authority_scope_kind='CYCLE'
           and publication.report_scope_id is null)
          or
          (v_upload.report_scope_id is not null
           and publication.authority_scope_kind='NHSP_REPORT_SCOPE'
           and publication.report_scope_id=v_upload.report_scope_id)
        )
      )
    )
  for share;
  if not found then
    raise exception 'WEEKLY_SOURCE_PROJECTION_NOT_CURRENT' using errcode='55000';
  end if;

  v_is_correction:=v_publication.state='CORRECTION_READY';
  if v_is_correction then
    select * into v_correction
    from public.weekly_final_source_correction_sessions correction
    where correction.id=v_upload.correction_session_id
    for share;
    if not found or v_correction.state<>'PREPARING'
       or v_correction.source_cycle_id is distinct from v_cycle.id
       or v_correction.authority_scope_kind is distinct from
          (case when v_upload.report_scope_id is null then 'CYCLE'
                else 'NHSP_REPORT_SCOPE' end)
       or v_correction.report_scope_id is distinct from v_upload.report_scope_id
       or v_correction.replacement_correction_upload_id is distinct from v_upload.id
       or v_correction.replacement_projection_publication_id is distinct from v_publication.id
       or coalesce(v_scope.current_final_revision_id,v_cycle.current_final_revision_id)
            is distinct from v_correction.expected_current_final_revision_id then
      raise exception 'WEEKLY_SOURCE_CORRECTION_SESSION_STALE' using errcode='40001';
    end if;
  else
    v_guard:=private.weekly_source_current_publication_guard_v1(
      v_cycle.id,
      case when v_upload.report_scope_id is null then 'CYCLE' else 'NHSP_REPORT_SCOPE' end,
      v_upload.report_scope_id,
      v_upload.id,
      v_publication.id,
      v_publication.authority_scope_version
    );
    if coalesce((v_guard->>'ok')::boolean,false) is not true
       or v_upload.purpose is distinct from 'ORDINARY'
       or v_upload.correction_session_id is not null then
      raise exception 'WEEKLY_SOURCE_PROJECTION_NOT_CURRENT' using errcode='55000';
    end if;
  end if;

  select * into v_resolution
  from public.weekly_source_row_resolutions
  where id=p_row_resolution_id
    and upload_row_id=v_source_row.id
    and generation=v_publication.authority_scope_version
  for update;
  if not found or v_resolution.mapping_state<>'RESOLVED' then
    raise exception 'WEEKLY_SOURCE_RESOLUTION_NOT_CURRENT' using errcode='55000';
  end if;
  select * into strict v_source_row
  from public.weekly_source_upload_rows
  where id=v_resolution.upload_row_id and upload_id=v_upload.id;

  select * into strict v_profile
  from public.weekly_source_format_profiles
  where id=v_upload.source_format_profile_id;
  select * into strict v_group
  from public.weekly_source_groups
  where id=v_cycle.source_group_id;
  select * into strict v_event
  from public.weekly_work_events
  where id=v_resolution.work_event_id;
  select * into v_contract
  from public.contracts
  where id=v_resolution.contract_id;
  if not found then
    raise exception 'WEEKLY_SOURCE_CONTRACT_NOT_FOUND' using errcode='55000';
  end if;

  if v_upload.report_scope_id is not null and not v_is_correction then
    if v_scope.source_cycle_id is distinct from v_cycle.id
       or v_scope.client_id is distinct from v_resolution.client_id
       or v_scope.current_projection_publication_id is distinct from v_publication.id
       or v_scope.current_complete_upload_id is distinct from v_upload.id then
      raise exception 'WEEKLY_SOURCE_REPORT_SCOPE_NOT_CURRENT' using errcode='55000';
    end if;
  end if;

  if not v_profile.active
     or v_profile.profile_code not in (
       'NHSP_FINAL_BACKING_V1',
       'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1',
       'HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1',
       'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1'
     )
     or v_source_row.row_finalisation_state not in (
       'NOT_APPLICABLE','SOURCE_WORKED','SOURCE_ABSENT_ZERO'
     )
     or (v_source_row.row_finalisation_state='SOURCE_ABSENT_ZERO'
         and v_profile.profile_code<>'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1')
     or v_resolution.work_event_id is distinct from v_event.id
     or v_resolution.candidate_id is distinct from v_event.candidate_id
     or v_resolution.client_id is distinct from v_event.client_id
     or v_source_row.work_date is distinct from v_event.work_date
     or v_resolution.client_id is distinct from v_contract.client_id
     or v_resolution.candidate_id is distinct from v_contract.candidate_id
     or v_source_row.work_date not between v_contract.start_date and v_contract.end_date then
    raise exception 'WEEKLY_SOURCE_TIMESHEET_LINEAGE_SCOPE_INVALID' using errcode='55000';
  end if;

  v_policy:=private._weekly_source_effective_policy_v1(
    v_resolution.client_id,v_resolution.contract_id,v_source_row.work_date
  );
  if v_policy->>'authority_mode'<>'SOURCE_AUTHORITY'
     or coalesce((v_policy->>'self_bill_enabled')::boolean,false) is not true
     or v_policy->>'c1_source_mode' not in ('NHSP_WEEKLY','HEALTHROSTER_WEEKLY')
     or private.weekly_source_projection_hex32_v1(
          v_policy->>'policy_sha256','WEEKLY_SOURCE_POLICY_FINGERPRINT_INVALID'
        ) is distinct from v_resolution.effective_policy_fingerprint
     or (v_group.source_family='NHSP') is distinct from
        (v_policy->>'c1_source_mode'='NHSP_WEEKLY')
     or (v_profile.final_authority_kind='NHSP_TRUST_BACKING_REPORT') is distinct from
        (v_group.source_family='NHSP') then
    raise exception 'WEEKLY_SOURCE_TIMESHEET_LINEAGE_POLICY_INVALID' using errcode='55000';
  end if;

  if v_source_row.row_finalisation_state='SOURCE_ABSENT_ZERO' then
    if coalesce((v_policy->>'source_fixed_expenses_enabled')::boolean,false) is not true
       or v_source_row.source_expense_parse_state<>'VALID'
       or coalesce(v_source_row.source_expense_pence,0)<=0
       or v_source_row.actual_net_minutes<>0
       or v_source_row.start_at_local is null
       or v_source_row.end_at_local is null
       or v_source_row.end_at_local<=v_source_row.start_at_local
       or v_source_row.break_minutes is null
       or exists(
         select 1 from public.weekly_source_row_economic_snapshots economic
         where economic.row_resolution_id=v_resolution.id
       ) then
      raise exception 'WEEKLY_SOURCE_ZERO_HOUR_EXPENSE_LINEAGE_INVALID' using errcode='55000';
    end if;

    select * into v_expense_policy
    from public.weekly_source_row_expense_policy_snapshots expense_policy
    where expense_policy.row_resolution_id=v_resolution.id;
    if not found or (
      select pg_catalog.count(*)
      from public.weekly_source_row_expense_policy_snapshots expense_policy
      where expense_policy.row_resolution_id=v_resolution.id
    )<>1 then
      raise exception 'WEEKLY_SOURCE_EXPENSE_POLICY_SNAPSHOT_CARDINALITY' using errcode='55000';
    end if;
    v_expected_expense_policy_hash:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_ROW_EXPENSE_POLICY_SNAPSHOT_V1',
      pg_catalog.jsonb_build_object(
        'row_resolution_id',v_resolution.id,'upload_row_id',v_source_row.id,
        'generation',v_resolution.generation,'work_event_id',v_resolution.work_event_id,
        'candidate_id',v_resolution.candidate_id,'client_id',v_resolution.client_id,
        'contract_id',v_resolution.contract_id,
        'row_finalisation_state',v_source_row.row_finalisation_state,
        'source_expense_pence',v_source_row.source_expense_pence,
        'source_expense_parse_state',v_source_row.source_expense_parse_state,
        'source_expense_vat_enabled',v_expense_policy.source_expense_vat_enabled,
        'invoice_vat_chargeable',v_expense_policy.invoice_vat_chargeable,
        'invoice_vat_rate_pct',v_expense_policy.invoice_vat_rate_pct,
        'correction_presentation',v_expense_policy.correction_presentation,
        'effective_policy_fingerprint',
          pg_catalog.encode(v_expense_policy.effective_policy_fingerprint,'hex'),
        'invoice_vat_policy_fingerprint',
          pg_catalog.encode(v_expense_policy.invoice_vat_policy_fingerprint,'hex')
      )
    );
    if v_expense_policy.upload_row_id is distinct from v_source_row.id
       or v_expense_policy.generation is distinct from v_resolution.generation
       or v_expense_policy.work_event_id is distinct from v_resolution.work_event_id
       or v_expense_policy.candidate_id is distinct from v_resolution.candidate_id
       or v_expense_policy.client_id is distinct from v_resolution.client_id
       or v_expense_policy.contract_id is distinct from v_resolution.contract_id
       or v_expense_policy.source_expense_pence is distinct from v_source_row.source_expense_pence
       or v_expense_policy.source_expense_parse_state is distinct from v_source_row.source_expense_parse_state
       or v_expense_policy.effective_policy_fingerprint is distinct from
          v_resolution.effective_policy_fingerprint
       or v_expense_policy.source_expense_vat_enabled is distinct from
          coalesce((v_policy->>'source_expense_vat_enabled')::boolean,false)
       or v_expense_policy.correction_presentation is distinct from
          pg_catalog.upper(pg_catalog.btrim(coalesce(
            v_policy->>'self_bill_correction_presentation',''
          )))
       or v_expense_policy.snapshot_hash is distinct from v_expected_expense_policy_hash then
      raise exception 'WEEKLY_SOURCE_EXPENSE_POLICY_SNAPSHOT_INVALID' using errcode='55000';
    end if;
  elsif (select pg_catalog.count(*)
         from public.weekly_source_row_economic_snapshots economic
         where economic.row_resolution_id=v_resolution.id)<>1 then
    raise exception 'WEEKLY_SOURCE_ECONOMIC_SNAPSHOT_CARDINALITY' using errcode='55000';
  end if;

  perform private.weekly_source_office_authority_v1(
    p_actor_user_id,'FINALISE_WEEK',v_group.id,v_resolution.client_id,
    v_cycle.finalisation_week_ending
  );

  v_weekday:=v_contract.week_ending_weekday_snapshot;
  if v_weekday not between 0 and 6 then
    raise exception 'WEEKLY_SOURCE_WEEK_ENDING_POLICY_INVALID' using errcode='55000';
  end if;
  v_week_ending_date:=v_source_row.work_date+
    ((v_weekday-extract(dow from v_source_row.work_date)::integer+7)%7);

  -- Note on the Candidate serial gate (proof/32 section 6 step 1): this owner
  -- binds a resolved source row to its ordinary base Weekly Timesheet.  It is
  -- not one of the four pinned Weekly Source job types (first authorisation,
  -- first-authorisation withdrawal, entitlement publication, pending release),
  -- so it does not take the gate; the first-authorisation owner does, through
  -- interface I-1.  It does take the full rotation lock set below.

  -- Review F1 and F4.  Every installed rotation owner takes the family advisory
  -- key and then the family rows, and only then contract_weeks (core
  -- 08082026_2035_...:126-131 then :190-193; legacy 16122025_...:844-849;
  -- confirmed route :2374-2376 then :2384-2385).  This owner previously locked
  -- the Contract Week, the Contract and the root row FIRST and asked for the
  -- family advisory keys afterwards, which is the reverse order and a proven
  -- deadlock (reproductions D1 and D2).  The root is therefore discovered with a
  -- PLAIN read, the I-1 lock set is taken FIRST, and every other lock follows.
  select contract_week.timesheet_id into v_known_root_timesheet_id
  from public.contract_weeks contract_week
  where contract_week.contract_id=v_resolution.contract_id
    and contract_week.week_ending_date=v_week_ending_date
    and contract_week.additional_seq=0;

  -- Review G2, the finalisation side of the same rule.  When a finalisation has
  -- already taken the complete sorted family lock set for this cycle, it
  -- publishes that set here.  A root this owner discovers that the set does not
  -- contain would be locked out of that sorted order, so refuse retryably
  -- instead.  Outside a finalisation the setting is absent and nothing changes.
  v_finalisation_root_set:=nullif(pg_catalog.btrim(coalesce(
    pg_catalog.current_setting('cloudtms.weekly_source_finalisation_root_set',true),''
  )),'');
  if v_finalisation_root_set is not null
     and v_known_root_timesheet_id is not null
     and not (v_known_root_timesheet_id::text
              =any(pg_catalog.string_to_array(v_finalisation_root_set,','))) then
    raise exception 'WEEKLY_SOURCE_FINALISATION_ROOT_SET_CHANGED_DURING_LOCK'
      using errcode='40001',
        detail=pg_catalog.jsonb_build_object(
          'code','WEEKLY_SOURCE_FINALISATION_ROOT_SET_CHANGED_DURING_LOCK',
          'observed_root_timesheet_id',v_known_root_timesheet_id
        )::text;
  end if;

  if v_known_root_timesheet_id is not null then
    v_lock_result:=private.weekly_source_lock_family_rows_v1(
      array[v_known_root_timesheet_id]::uuid[],v_resolution.candidate_id
    );
    if coalesce((v_lock_result->>'ok')::boolean,false) is not true then
      raise exception '%',v_lock_result->>'code'
        using errcode='55000',detail=v_lock_result::text;
    end if;
    v_family:=v_lock_result->'families'->0;
  end if;

  -- A missing base Contract Week has no row to lock, so use the same scoped
  -- advisory lock before selecting/inserting it.  This is not a business key
  -- or an externally visible lock, and it is now taken after the family keys.
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    'WEEKLY_SOURCE_BASE_WEEK|'||v_resolution.contract_id::text||'|'||v_week_ending_date::text,
    0
  ));

  insert into public.contract_weeks(
    contract_id,week_ending_date,additional_seq,status,
    submission_mode_snapshot,timesheet_id,is_adjustment,created_at,updated_at
  ) values (
    v_resolution.contract_id,v_week_ending_date,0,
    'SUBMITTED'::public.contract_week_status_enum,
    'MANUAL'::public.submission_mode_enum,null,false,
    pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp()
  ) on conflict (contract_id,week_ending_date,additional_seq) do nothing;

  select * into strict v_contract_week
  from public.contract_weeks
  where contract_id=v_resolution.contract_id
    and week_ending_date=v_week_ending_date
    and additional_seq=0
  for update;

  -- Existing manual week owners lock the base week before the Contract.  Keep
  -- that order, then revalidate every mutable Contract fact and recompute the
  -- week end while both rows are protected.
  select * into v_contract
  from public.contracts
  where id=v_resolution.contract_id
  for update;
  if not found
     or v_resolution.client_id is distinct from v_contract.client_id
     or v_resolution.candidate_id is distinct from v_contract.candidate_id
     or v_source_row.work_date not between v_contract.start_date and v_contract.end_date
     or v_contract.week_ending_weekday_snapshot not between 0 and 6
     or v_week_ending_date is distinct from v_source_row.work_date+
        ((v_contract.week_ending_weekday_snapshot-
          extract(dow from v_source_row.work_date)::integer+7)%7) then
    raise exception 'WEEKLY_SOURCE_CONTRACT_CHANGED_DURING_LINEAGE' using errcode='55000';
  end if;

  if v_contract_week.contract_id is distinct from v_resolution.contract_id
     or v_contract_week.week_ending_date is distinct from v_week_ending_date
     or v_contract_week.additional_seq<>0
     or v_contract_week.is_adjustment
     or v_contract_week.status='CANCELLED'::public.contract_week_status_enum then
    raise exception 'WEEKLY_SOURCE_BASE_CONTRACT_WEEK_INVALID' using errcode='55000';
  end if;

  -- Under the locks, the Contract Week must still name the root whose family
  -- this transaction locked.  If it now names a different root, or gained one
  -- after the plain read found none, the family locks would be the wrong ones:
  -- refuse retryably rather than take a second family lock set out of order.
  if v_contract_week.timesheet_id is distinct from v_known_root_timesheet_id then
    raise exception 'WEEKLY_SOURCE_BASE_WEEK_ROOT_CHANGED_DURING_LOCK'
      using errcode='40001',
        detail=pg_catalog.jsonb_build_object(
          'expected_root_timesheet_id',v_known_root_timesheet_id,
          'observed_root_timesheet_id',v_contract_week.timesheet_id
        )::text;
  end if;

  if v_contract_week.timesheet_id is not null then
    -- The family rows are already locked by the I-1 lock set above, so this is
    -- a re-entrant read of a row this transaction holds, not a new lock order.
    select * into v_timesheet
    from public.timesheets
    where timesheet_id=v_contract_week.timesheet_id
    for update;
    -- proof/34 rule 6 and section 5 step 3: a stored contract-week Timesheet id
    -- that is no longer the current version is a ROTATION, not an invalid
    -- Timesheet.  It is classified below, after the rotation lock set has been
    -- taken, so it carries the exact
    -- WEEKLY_SOURCE_TIMESHEET_ROTATED_BEFORE_AUTHORISATION refusal (ROT-001).
    if not found
       or v_timesheet.contract_id is distinct from v_resolution.contract_id
       or v_timesheet.week_ending_date is distinct from v_week_ending_date
       or v_timesheet.sheet_scope<>'WEEKLY'::public.timesheet_scope_enum
       or v_timesheet.line_type<>'HOURS'::public.timesheet_line_type_enum
       or v_timesheet.is_adjustment
       or v_timesheet.revoked_at is not null
       or v_timesheet.archived_at_utc is not null then
      raise exception 'WEEKLY_SOURCE_EXISTING_TIMESHEET_INVALID' using errcode='55000';
    end if;
  else
    if v_contract_week.status in (
      'AUTHORISED'::public.contract_week_status_enum,
      'INVOICED'::public.contract_week_status_enum,
      'CANCELLED'::public.contract_week_status_enum
    ) then
      raise exception 'WEEKLY_SOURCE_FINAL_WEEK_TIMESHEET_MISSING' using errcode='55000';
    end if;

    select * into strict v_candidate
    from public.candidates where id=v_resolution.candidate_id;
    select * into strict v_client
    from public.clients where id=v_resolution.client_id;

    v_occupant_norm:=pg_catalog.lower(coalesce(
      nullif(pg_catalog.btrim(v_candidate.tms_ref),''),
      nullif(pg_catalog.btrim(v_candidate.display_name),''),
      v_candidate.id::text
    ));
    v_hospital_norm:=pg_catalog.lower(coalesce(
      nullif(pg_catalog.btrim(v_contract.display_site),''),
      nullif(pg_catalog.btrim(v_client.name),''),
      v_client.id::text
    ));
    v_ward_norm:=pg_catalog.lower(coalesce(
      nullif(pg_catalog.btrim(v_contract.ward_hint),''),'contract'
    ));
    v_role_norm:=pg_catalog.lower(coalesce(
      nullif(pg_catalog.btrim(v_contract.role),''),'weekly'
    ));
    v_booking_material:='weekly-source|'||v_resolution.contract_id::text||'|'||
      v_week_ending_date::text;
    v_booking_id:='bk_'||pg_catalog.substr(pg_catalog.encode(
      extensions.digest(pg_catalog.convert_to(v_booking_material,'UTF8'),'sha256'),'hex'
    ),1,24);

    if exists(
      select 1
      from public.timesheets existing
      where existing.booking_id=v_booking_id
        and existing.is_current
    ) then
      raise exception 'WEEKLY_SOURCE_BOOKING_ID_COLLISION' using errcode='55000';
    end if;

    insert into public.timesheets(
      booking_id,version,is_current,status,sheet_scope,submission_mode,line_type,
      occupant_key_norm,hospital_norm,ward_norm,job_title_norm,shift_label_norm,
      week_ending_date,contract_id,actual_schedule_json,qr_payload_json,
      is_adjustment,created_at,updated_at
    ) values (
      v_booking_id,1,true,'RECEIVED'::public.timesheet_status_enum,
      'WEEKLY'::public.timesheet_scope_enum,'MANUAL'::public.submission_mode_enum,
      'HOURS'::public.timesheet_line_type_enum,v_occupant_norm,v_hospital_norm,
      v_ward_norm,v_role_norm,'weekly-0',v_week_ending_date,
      v_resolution.contract_id,'[]'::jsonb,'{}'::jsonb,false,
      pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp()
    ) returning * into v_timesheet;
    v_created_timesheet:=true;

    update public.contract_weeks
    set timesheet_id=v_timesheet.timesheet_id,
        status=case when status='AUTHORISED'::public.contract_week_status_enum
          then status else 'SUBMITTED'::public.contract_week_status_enum end,
        updated_at=pg_catalog.statement_timestamp()
    where id=v_contract_week.id
    returning * into v_contract_week;

    insert into public.audit_events(
      ts_utc,actor_user_id,actor_display,actor_role_at_time,
      object_type,object_id_text,action,before_json,after_json,reason
    )
    select
      pg_catalog.statement_timestamp(),p_actor_user_id,actor.display_name,actor.role,
      'timesheets',v_timesheet.timesheet_id::text,
      'WEEKLY_SOURCE_BASE_TIMESHEET_CREATED',null,
      pg_catalog.jsonb_build_object(
        'source_cycle_id',v_cycle.id,
        'row_resolution_id',v_resolution.id,
        'work_event_id',v_resolution.work_event_id,
        'contract_id',v_resolution.contract_id,
        'contract_week_id',v_contract_week.id,
        'candidate_id',v_resolution.candidate_id,
        'client_id',v_resolution.client_id,
        'week_ending_date',v_week_ending_date
      ),'FINAL_SOURCE_LINEAGE'
    from public.tms_users actor
    where actor.id=p_actor_user_id;

    if not found then
      raise exception 'WEEKLY_SOURCE_ACTOR_NOT_FOUND' using errcode='55000';
    end if;
  end if;

  -- proof/34 section 5 step 2: the deadlock-free rotation lock set and a
  -- re-resolution of the canonical current version through the installed
  -- Workbench authority.  A stored or newly minted physical id is only a lookup
  -- key; it is never trusted on its own (rule 6).
  --
  -- For an existing root the lock set was already taken, before every other
  -- lock, so this call is re-entrant and re-resolves under the locks.  For a
  -- root this transaction has just minted, no other session can know the
  -- booking id yet, so taking the family keys here cannot join a cycle.
  v_lock_result:=private.weekly_source_lock_family_rows_v1(
    array[v_timesheet.timesheet_id]::uuid[],v_resolution.candidate_id
  );
  if coalesce((v_lock_result->>'ok')::boolean,false) is not true then
    raise exception '%',v_lock_result->>'code'
      using errcode='55000',detail=v_lock_result::text;
  end if;
  v_family:=v_lock_result->'families'->0;

  -- Review F5 / proof/34 rule 7: an approved decision is never silently
  -- transferred from one physical Timesheet id to another.  If the family
  -- already carries a live root authorisation on a member that is NOT the
  -- canonical row, the authorised root has rotated: refuse rather than bind a
  -- new source row to the new physical id and report success.
  v_root_authorisation:=private.weekly_source_root_authorisation_state_v1(
    (v_family->>'canonical_timesheet_id')::uuid,
    (select pg_catalog.array_agg(member.value::uuid)
     from pg_catalog.jsonb_array_elements_text(
       v_family->'member_timesheet_ids'
     ) member(value))
  );
  -- Review G8: the guard treats a missing authorisation relation as "cannot
  -- tell" and fails closed; this owner must point the same way, not bind.
  if coalesce((v_root_authorisation->>'relation_present')::boolean,false) is not true then
    raise exception 'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE'
      using errcode='55000',
        detail=pg_catalog.jsonb_build_object(
          'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
          'reason',coalesce(v_root_authorisation->>'reason',
                            'ROOT_AUTHORISATION_RELATION_MISSING')
        )::text;
  end if;
  if coalesce((v_root_authorisation->>'live_on_other_member_count')::integer,0)>0 then
    raise exception 'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE'
      using errcode='55000',
        detail=pg_catalog.jsonb_build_object(
          'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
          'reason','AUTHORISED_ROOT_NOT_CANONICAL',
          'canonical_timesheet_id',v_family->>'canonical_timesheet_id',
          'authorised_timesheet_id',
            v_root_authorisation->>'live_on_other_member_timesheet_id'
        )::text;
  end if;

  -- proof/34 section 5 step 3: a changed current id before first authorisation
  -- makes the request stale.  Write nothing and never authorise the older id.
  if coalesce((v_family->>'requested_is_canonical')::boolean,false) is not true
     or coalesce((v_family->>'family_is_current')::boolean,false) is not true then
    raise exception 'WEEKLY_SOURCE_TIMESHEET_ROTATED_BEFORE_AUTHORISATION'
      using errcode='55000',detail=v_family::text;
  end if;

  -- The family rows are locked now, so re-read the root under the lock before
  -- any fact derived from it is frozen into the lineage record.
  select * into strict v_timesheet
  from public.timesheets
  where timesheet_id=v_timesheet.timesheet_id;
  if v_timesheet.booking_id is distinct from (v_family->>'family_booking_id')
     or v_timesheet.version is distinct from (v_family->>'canonical_version')::integer
     or not v_timesheet.is_current then
    raise exception 'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE'
      using errcode='55000',detail=v_family::text;
  end if;

  -- Decision D8: this owner writes a BINDING record, never an authorisation.
  -- The row signature at authorisation, the authorisation generation and the
  -- entitlement head pointer all belong to
  -- public.weekly_source_root_authorisations, written only by the
  -- first-authorisation owner (interface I-6) after the ordinary Authorise
  -- succeeds, and marked withdrawn only by the withdrawal owner.  This owner
  -- therefore computes and stores no signature at all.

  v_contract_week_settings:=
    private.weekly_source_contract_week_settings_authority_assert_v1(v_contract_week.id);
  v_timesheet_settings:=
    private._timesheet_settings_authority_frozen_v1(v_timesheet.timesheet_id);
  if v_timesheet.settings_authority_resolved_at is null
     or (v_timesheet_settings->>'resolved_at_utc')::timestamptz is distinct from
        v_timesheet.settings_authority_resolved_at
     or v_timesheet.settings_authority_fingerprint is distinct from
        v_contract_week.settings_authority_fingerprint
     or v_timesheet_settings->>'authority_fingerprint' is distinct from
        v_contract_week_settings->>'authority_fingerprint' then
    raise exception 'WEEKLY_SOURCE_SETTINGS_AUTHORITY_NOT_FROZEN' using errcode='55000';
  end if;

  v_lineage_fingerprint:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_ROW_TIMESHEET_LINEAGE_V1',
    pg_catalog.jsonb_build_object(
      'row_resolution_id',v_resolution.id,
      'source_cycle_id',v_cycle.id,
      'work_event_id',v_resolution.work_event_id,
      'candidate_id',v_resolution.candidate_id,
      'client_id',v_resolution.client_id,
      'contract_id',v_resolution.contract_id,
      'contract_week_id',v_contract_week.id,
      'timesheet_id',v_timesheet.timesheet_id,
      'week_ending_date',v_week_ending_date
    )
  );

  -- Decision D8: one binding row per resolution.  A replay is a no-op; this
  -- owner never writes an authorisation generation, and re-authorisation after
  -- a withdrawal appends a row to public.weekly_source_root_authorisations,
  -- written only by the first-authorisation owner (interface I-6).
  select exists(
    select 1 from public.weekly_source_row_timesheet_lineages lineage
    where lineage.row_resolution_id=v_resolution.id
  ) into v_existing_lineage;

  -- Decision D8: the binding record.  It carries the binding-time family
  -- identity and physical version, which stale detection needs, and no
  -- authorisation fact of any kind.
  insert into public.weekly_source_row_timesheet_lineages(
    row_resolution_id,source_cycle_id,work_event_id,candidate_id,client_id,
    contract_id,contract_week_id,timesheet_id,family_booking_id,timesheet_version,
    week_ending_date,lineage_fingerprint
  ) values (
    v_resolution.id,v_cycle.id,v_resolution.work_event_id,v_resolution.candidate_id,
    v_resolution.client_id,v_resolution.contract_id,v_contract_week.id,
    v_timesheet.timesheet_id,v_timesheet.booking_id,v_timesheet.version,
    v_week_ending_date,v_lineage_fingerprint
  ) on conflict (row_resolution_id) do nothing;

  -- Review G3 / proof/34 section 5 step 3 and section 6.  A binding that already
  -- exists on a DIFFERENT physical id means the base Timesheet rotated since it
  -- was bound.  Before first authorisation that is permitted and rotation WINS
  -- (rule 2): the request is STALE, so refuse with
  -- WEEKLY_SOURCE_TIMESHEET_ROTATED_BEFORE_AUTHORISATION and say that the
  -- resolution must be rebuilt at a new generation.  Only when the family
  -- carries a live authorisation record is it the section 6 integrity failure,
  -- which the assert below raises.  Without this the immutable binding would
  -- make that source row impossible to finalise ever again.
  select lineage.timesheet_id into v_bound_timesheet_id
  from public.weekly_source_row_timesheet_lineages lineage
  where lineage.row_resolution_id=v_resolution.id;
  if v_bound_timesheet_id is distinct from v_timesheet.timesheet_id then
    if coalesce((v_root_authorisation->>'live_on_canonical')::boolean,false) is not true
       and coalesce((v_root_authorisation->>'live_on_other_member_count')::integer,0)=0 then
      raise exception 'WEEKLY_SOURCE_TIMESHEET_ROTATED_BEFORE_AUTHORISATION'
        using errcode='55000',
          detail=pg_catalog.jsonb_build_object(
            'code','WEEKLY_SOURCE_TIMESHEET_ROTATED_BEFORE_AUTHORISATION',
            'reason','BOUND_ROOT_ROTATED_BEFORE_AUTHORISATION',
            'bound_timesheet_id',v_bound_timesheet_id,
            'canonical_timesheet_id',v_timesheet.timesheet_id,
            'rebuild','REBUILD_RESOLUTION_AT_A_NEW_GENERATION'
          )::text;
    end if;
    raise exception 'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE'
      using errcode='55000',
        detail=pg_catalog.jsonb_build_object(
          'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
          'reason','AUTHORISED_ROOT_ROTATED_AFTER_AUTHORISATION',
          'bound_timesheet_id',v_bound_timesheet_id,
          'canonical_timesheet_id',v_timesheet.timesheet_id
        )::text;
  end if;

  v_lineage:=private.weekly_source_timesheet_lineage_assert_v1(v_resolution.id);
  if v_lineage.lineage_fingerprint is distinct from v_lineage_fingerprint then
    raise exception 'WEEKLY_SOURCE_TIMESHEET_LINEAGE_CONFLICT' using errcode='55000';
  end if;

  if not v_existing_lineage then
    insert into public.audit_events(
      ts_utc,actor_user_id,actor_display,actor_role_at_time,
      object_type,object_id_text,action,before_json,after_json,reason
    )
    select
      pg_catalog.statement_timestamp(),p_actor_user_id,actor.display_name,actor.role,
      'weekly_source_row_timesheet_lineages',v_lineage.id::text,
      'WEEKLY_SOURCE_TIMESHEET_LINEAGE_CREATED',null,
      pg_catalog.jsonb_build_object(
        'source_cycle_id',v_lineage.source_cycle_id,
        'row_resolution_id',v_lineage.row_resolution_id,
        'work_event_id',v_lineage.work_event_id,
        'contract_id',v_lineage.contract_id,
        'contract_week_id',v_lineage.contract_week_id,
        'timesheet_id',v_lineage.timesheet_id,
        'candidate_id',v_lineage.candidate_id,
        'client_id',v_lineage.client_id,
        'week_ending_date',v_lineage.week_ending_date,
        'created_timesheet',v_created_timesheet
      ),'FINAL_SOURCE_LINEAGE'
    from public.tms_users actor
    where actor.id=p_actor_user_id;

    if not found then
      raise exception 'WEEKLY_SOURCE_ACTOR_NOT_FOUND' using errcode='55000';
    end if;
  end if;

  return pg_catalog.jsonb_build_object(
    'row_resolution_id',v_resolution.id,
    'source_cycle_id',v_cycle.id,
    'contract_week_id',v_lineage.contract_week_id,
    'timesheet_id',v_lineage.timesheet_id,
    'week_ending_date',v_lineage.week_ending_date,
    'created_timesheet',v_created_timesheet,
    'idempotent_replay',v_existing_lineage
  );
exception
  when no_data_found or too_many_rows then
    raise exception 'WEEKLY_SOURCE_TIMESHEET_LINEAGE_SCOPE_INVALID' using errcode='55000';
end;
$function$;

alter function private.weekly_source_contract_week_settings_authority_assert_v1(uuid)
  owner to postgres;
alter function private.weekly_source_timesheet_lineage_assert_v1(uuid) owner to postgres;
alter function public.weekly_source_timesheet_lineage_ensure_atomic_v1(uuid,uuid) owner to postgres;

revoke all on function private.weekly_source_contract_week_settings_authority_assert_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_timesheet_lineage_assert_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function public.weekly_source_timesheet_lineage_ensure_atomic_v1(uuid,uuid)
  from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_timesheet_lineage_ensure_atomic_v1(uuid,uuid)
  to service_role;

comment on function public.weekly_source_timesheet_lineage_ensure_atomic_v1(uuid,uuid) is
  'Service-only idempotent binding from one current resolved worked row, or one validated positive source-expense zero-hours row, to its ordinary base Weekly HOURS Timesheet. Writes no hours, economics, invoice or payment state.';

notify pgrst, 'reload schema';

commit;
