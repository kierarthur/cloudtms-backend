-- Repeatable CloudTMS authority: weekly_source_mode_a_dispatch_v1
--
-- Plan 6.2 Gate 8 (contract item G8-1, G8-2, G8-3; gap row XSG-009).
--
-- Pack `24 §14`: "The unified import acceptance route must dispatch this policy
-- to the existing validation-only Mode A owner. Merely labelling the row as
-- Timesheet evidence is insufficient."
-- Pack `25 §9`: "The unified upload owner must dispatch signed-Timesheet-authority
-- files to the established Mode A comparison/reference route, not to source
-- finalisation."
--
-- This file contains **no comparison logic of its own**. It is a bridge, built
-- on the precedent of the installed Daily compatibility bridge
-- `22082026_1606_daily_validation_compatibility_v1.sql:189-207`, which inserts
-- `public.hr_imports` and then calls `public._import_review_create_core_v2`.
-- Every import-review owner is call-only here:
--
--   * `public._import_review_assert_actor_v1`            actor gate
--   * `public._import_review_effective_authority_core_v1` route eligibility
--   * `public._import_review_hash_v1`                     fingerprints
--   * `public._import_review_create_core_v2`              import review creation
--   * `public.hr_weekly_validation_preview`               THE comparison engine
--   * `public._import_review_apply_envelope_core_v1`      server apply envelope
--   * `public.hr_weekly_apply_transactional`              reference write,
--                                                          manager correction email
--                                                          actions and the
--                                                          established
--                                                          auto-authorisation
--
-- None of them is edited, wrapped or re-created. The hours comparison, the
-- equal-break-duration rule, the reference decision, the mismatch-to-manager
-- email route and the auto-authorisation gate are all theirs.
--
-- What this file owns is only the Weekly Source side: which rows are
-- signed-Timesheet-authority, staging them into import-review staging exactly
-- as the installed HealthRoster parser does, and recording the established
-- owner's results on the Weekly Source relations
-- `weekly_timesheet_source_comparisons`,
-- `weekly_timesheet_reference_apply_operations` and `_items`, which until now
-- had no production writer.
--
-- Not done here, deliberately: no rotation guard is added at
-- `hr_weekly_apply_transactional` — that entry point belongs to the Gate 6
-- guard work package.

\set ON_ERROR_STOP on

begin;

-- Reproduces `inferRoleTypeFromText` from the installed HealthRoster weekly
-- parser (`broker/src/index.js:113675-113687`) so a bridged row carries the
-- same staging value the file route would have produced. `public.hr_rows`
-- requires it; the weekly validation preview never reads it.
create or replace function private.weekly_source_mode_a_role_type_v1(
  p_primary text,
  p_secondary text
) returns public.role_type_enum
language sql immutable
set search_path to 'public','pg_catalog','pg_temp'
as $function$
  select case when pg_catalog.lower(
      pg_catalog.concat_ws(' ',coalesce(p_primary,''),coalesce(p_secondary,''))
    ) similar to '%(hca|healthcare assistant|support worker|csw|hcsw)%'
    then 'HCA'::public.role_type_enum else 'RMN'::public.role_type_enum end;
$function$;

-- One deterministic import-review coverage operation key per Weekly Source
-- projection publication and Client, so the bridge is idempotent and the
-- reference-apply owner can find the import the dispatcher created without a
-- new column on any Weekly Source relation.
create or replace function private.weekly_source_mode_a_operation_key_v1(
  p_publication_id uuid,
  p_client_id uuid
) returns text
language sql stable
set search_path to 'public','pg_catalog','pg_temp'
as $function$
  select 'weekly-source-mode-a:'||public._import_review_hash_v1(
    pg_catalog.jsonb_build_object(
      'schema','WEEKLY_SOURCE_MODE_A_DISPATCH_V1',
      'projection_publication_id',p_publication_id,
      'client_id',p_client_id
    )::text
  );
$function$;

create or replace function public.weekly_source_mode_a_dispatch_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_publication_id uuid;
  v_publication public.weekly_source_projection_publications%rowtype;
  v_upload public.weekly_source_uploads%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_client_id uuid;
  v_client public.clients%rowtype;
  v_authority record;
  v_operation_key text;
  v_import_id uuid;
  v_existing public.hr_imports%rowtype;
  v_min_date date;
  v_max_date date;
  v_staged integer;
  v_preview jsonb;
  v_group_row jsonb;
  v_comparison jsonb;
  v_timesheet public.timesheets%rowtype;
  v_work_event_id uuid;
  v_upload_row public.weekly_source_upload_rows%rowtype;
  v_resolution public.weekly_source_row_resolutions%rowtype;
  v_state text;
  v_reference text;
  v_break_match boolean;
  v_fingerprint bytea;
  v_written integer:=0;
  v_unrepresentable jsonb:='[]'::jsonb;
  v_client_results jsonb:='[]'::jsonb;
  v_source_row_total integer:=0;
  v_dispatched boolean:=false;
begin
  if coalesce(current_setting('request.jwt.claim.role',true),
       nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(
       select 1 from pg_catalog.jsonb_object_keys(p_request) key
       where key not in ('actor_user_id','publication_id')
     ) then
    raise exception 'WEEKLY_SOURCE_MODE_A_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_publication_id:=(p_request->>'publication_id')::uuid;
  exception when invalid_text_representation then
    raise exception 'WEEKLY_SOURCE_MODE_A_REQUEST_INVALID' using errcode='22023';
  end;
  if v_actor is null or v_publication_id is null then
    raise exception 'WEEKLY_SOURCE_MODE_A_REQUEST_INVALID' using errcode='22023';
  end if;

  select * into v_publication
  from public.weekly_source_projection_publications where id=v_publication_id for update;
  if not found then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_NOT_FOUND' using errcode='22023';
  end if;
  if v_publication.state not in ('CURRENT','CORRECTION_READY') then
    raise exception 'WEEKLY_SOURCE_MODE_A_PUBLICATION_NOT_PUBLISHED' using errcode='55000',
      detail=v_publication.state;
  end if;
  select * into strict v_upload from public.weekly_source_uploads where id=v_publication.upload_id;
  select * into strict v_cycle from public.weekly_source_cycles where id=v_publication.source_cycle_id;
  select * into strict v_group from public.weekly_source_groups where id=v_cycle.source_group_id;

  perform private.weekly_source_office_authority_v1(
    v_actor,'RECHECK_SOURCE',v_group.id,null,v_cycle.finalisation_week_ending
  );
  perform public._import_review_assert_actor_v1(v_actor);

  -- The signed-Timesheet-authority subset of this publication.  Mode A
  -- membership is the resolved authority the projection owner already wrote
  -- to public.weekly_timesheet_authority_resolutions; it is never recomputed
  -- here and never taken from a caller.
  create temporary table if not exists pg_temp.weekly_source_mode_a_rows(
    resolution_id uuid primary key,
    upload_row_id uuid not null,
    work_event_id uuid not null,
    candidate_id uuid not null,
    client_id uuid not null,
    contract_id uuid not null,
    work_date date not null,
    start_at_local timestamp without time zone not null,
    end_at_local timestamp without time zone not null,
    break_minutes integer not null,
    source_candidate_identity text not null,
    role_band_source text,
    external_source_key text
  ) on commit drop;
  delete from pg_temp.weekly_source_mode_a_rows;

  insert into pg_temp.weekly_source_mode_a_rows
  select resolution.id,source_row.id,resolution.work_event_id,resolution.candidate_id,
    resolution.client_id,resolution.contract_id,source_row.work_date,
    source_row.start_at_local,source_row.end_at_local,coalesce(source_row.break_minutes,0),
    source_row.source_candidate_identity,
    coalesce(nullif(pg_catalog.btrim(source_row.role_band_source),''),
      nullif(pg_catalog.btrim(contract.band),''),nullif(pg_catalog.btrim(contract.role),'')),
    source_row.external_source_key
  from public.weekly_source_row_resolutions resolution
  join public.weekly_source_upload_rows source_row on source_row.id=resolution.upload_row_id
  join public.contracts contract on contract.id=resolution.contract_id
  where source_row.upload_id=v_upload.id
    and resolution.generation=coalesce(
      v_publication.projection_generation,v_publication.authority_scope_version::integer)
    and resolution.mapping_state='RESOLVED'
    and resolution.work_event_id is not null
    and resolution.contract_id is not null
    and resolution.candidate_id is not null
    and resolution.client_id is not null
    and source_row.start_at_local is not null
    and source_row.end_at_local is not null
    and exists(
      select 1 from public.weekly_timesheet_authority_resolutions authority
      where authority.source_cycle_id=v_cycle.id
        and authority.contract_id=resolution.contract_id
        and authority.work_date=source_row.work_date
    );

  select count(*)::integer into v_source_row_total from pg_temp.weekly_source_mode_a_rows;
  if v_source_row_total=0 then
    return pg_catalog.jsonb_build_object(
      'ok',true,'dispatched',false,'reason_code','NO_TIMESHEET_AUTHORITY_ROWS',
      'publication_id',v_publication_id,'clients','[]'::jsonb
    );
  end if;

  for v_client_id in
    select distinct mode_a.client_id from pg_temp.weekly_source_mode_a_rows mode_a order by 1
  loop
    select * into strict v_client from public.clients where id=v_client_id;

    -- Route eligibility is the import-review authority's own answer, per
    -- Contract and worked date.  A Contract that the installed authority does
    -- not consider validation eligible is never bridged.
    if exists(
      select 1 from pg_temp.weekly_source_mode_a_rows mode_a
      cross join lateral public._import_review_effective_authority_core_v1(
        'HR_WEEKLY',mode_a.contract_id,mode_a.client_id,mode_a.work_date
      ) authority
      where mode_a.client_id=v_client_id
        and (coalesce(authority.route_eligible,false) is not true
          or coalesce(authority.validation_eligible,false) is not true)
    ) then
      raise exception 'WEEKLY_SOURCE_MODE_A_ROUTE_NOT_ELIGIBLE' using errcode='55000',
        detail=pg_catalog.jsonb_build_object('client_id',v_client_id)::text;
    end if;

    v_operation_key:=private.weekly_source_mode_a_operation_key_v1(v_publication_id,v_client_id);
    perform pg_catalog.pg_advisory_xact_lock(
      pg_catalog.hashtextextended(v_operation_key,17092026)
    );

    select * into v_existing from public.hr_imports
    where coverage_operation_key=v_operation_key for update;
    if found then
      v_import_id:=v_existing.id;
      v_staged:=(select count(*)::integer from public.hr_rows where import_id=v_import_id);
    else
      -- WP-37 (WP-31 finding F4).  The bridged import's coverage is the
      -- OFFICE-CONFIRMED coverage of the upload, not the min/max of the rows
      -- the file happened to contain.  Deriving it from the rows meant a signed
      -- day the source omitted was never compared at all: no UNMATCHED, no
      -- `SOURCE_SHIFT_MISSING`, and `24 §14`'s "a mismatch uses the existing
      -- manager correction email journey" was bypassed for exactly the case it
      -- exists for.  Confirmed coverage is nullable on the relation, so an
      -- unconfirmed upload fails CLOSED here rather than silently falling back
      -- to the rows.
      v_min_date:=v_upload.confirmed_coverage_start_local_date;
      v_max_date:=v_upload.confirmed_coverage_end_local_date;
      if v_min_date is null or v_max_date is null then
        raise exception 'WEEKLY_SOURCE_MODE_A_COVERAGE_UNCONFIRMED' using errcode='55000',
          detail=pg_catalog.jsonb_build_object(
            'upload_id',v_upload.id,'client_id',v_client_id,
            'coverage_state',v_upload.coverage_state
          )::text;
      end if;
      if exists(
        select 1 from pg_temp.weekly_source_mode_a_rows mode_a
        where mode_a.client_id=v_client_id
          and mode_a.work_date not between v_min_date and v_max_date
      ) then
        raise exception 'WEEKLY_SOURCE_MODE_A_ROW_OUTSIDE_CONFIRMED_COVERAGE' using errcode='55000',
          detail=pg_catalog.jsonb_build_object(
            'upload_id',v_upload.id,'client_id',v_client_id,
            'confirmed_coverage_start',v_min_date,'confirmed_coverage_end',v_max_date
          )::text;
      end if;

      v_import_id:=pg_catalog.gen_random_uuid();
      insert into public.hr_imports(
        id,filename,uploaded_by,uploaded_at_utc,tz_assumption,parse_summary_json,
        source_system,file_r2_key,client_id,import_scope,source_file_sha256,parser_version
      ) values (
        v_import_id,
        'Weekly Source signed-Timesheet authority · '||v_upload.original_filename,
        v_actor,pg_catalog.now(),'Europe/London',
        pg_catalog.jsonb_build_object(
          'status','PARSED','input_format','WEEKLY_SOURCE_MODE_A_BRIDGE',
          'notes','Bridged from a Weekly Source upload for the established validation-only Mode A route',
          'weekly_source_upload_id',v_upload.id,
          'weekly_source_projection_publication_id',v_publication_id,
          'header_rows','[]'::jsonb,'header_columns','[]'::jsonb
        ),
        'HEALTHROSTER'::public.hr_source_enum,null,v_client_id,'HR_WEEKLY',
        pg_catalog.encode(v_upload.content_sha256,'hex'),
        'WEEKLY_SOURCE_MODE_A_BRIDGE_V1'
      );

      -- Staged exactly as the installed HealthRoster weekly parser stages a
      -- file (`broker/src/index.js:114280-114322`): the same payload keys the
      -- validation preview reads, and the raw Candidate identity the source
      -- file carried, so the preview resolves the Candidate through the same
      -- installed alias and mapping authorities.
      insert into public.hr_rows(
        import_id,hr_request_id,date_local,start_time_local,end_time_local,
        staff_norm,role_type,unit_raw,unit_hint,agency_raw,external_row_key,
        payload_json,staff_raw,assignment_grade_norm,hours_worked
      )
      select v_import_id,
        nullif(pg_catalog.btrim(coalesce(mode_a.external_source_key,'')),''),
        mode_a.work_date,
        mode_a.start_at_local::time,
        mode_a.end_at_local::time,
        pg_catalog.lower(pg_catalog.btrim(mode_a.source_candidate_identity)),
        private.weekly_source_mode_a_role_type_v1(
          mode_a.role_band_source,mode_a.source_candidate_identity
        ),
        nullif(pg_catalog.btrim(coalesce(mode_a.role_band_source,'')),''),
        null,'WEEKLY_SOURCE',
        'ws:'||mode_a.upload_row_id::text,
        pg_catalog.jsonb_build_object(
          'type','WEEKLY_SOURCE_MODE_A',
          'staff_name',mode_a.source_candidate_identity,
          'work_date',mode_a.work_date::text,
          'request_id',nullif(pg_catalog.btrim(coalesce(mode_a.external_source_key,'')),''),
          'grade_raw',nullif(pg_catalog.btrim(coalesce(mode_a.role_band_source,'')),''),
          'start_local',pg_catalog.to_char(mode_a.start_at_local,'HH24:MI'),
          'end_local',pg_catalog.to_char(mode_a.end_at_local,'HH24:MI'),
          'break_mins',mode_a.break_minutes,
          'actual_break_mins',mode_a.break_minutes,
          'break_evidence_supplied',true,
          'break_inferred_from_worked_hours',false,
          'start_utc',(mode_a.start_at_local at time zone 'Europe/London'),
          'end_utc',(mode_a.end_at_local at time zone 'Europe/London'),
          'weekly_source_upload_row_id',mode_a.upload_row_id,
          'weekly_source_work_event_id',mode_a.work_event_id
        ),
        mode_a.source_candidate_identity,
        nullif(pg_catalog.btrim(coalesce(mode_a.role_band_source,'')),''),
        pg_catalog.round(
          (pg_catalog.date_part('epoch',mode_a.end_at_local-mode_a.start_at_local)/60
            -mode_a.break_minutes)::numeric/60,2
        )
      from pg_temp.weekly_source_mode_a_rows mode_a
      where mode_a.client_id=v_client_id;
      get diagnostics v_staged=row_count;

      update public.hr_imports
      set parse_summary_json=parse_summary_json||pg_catalog.jsonb_build_object(
        'rows_total',v_staged,'rows_parsed',v_staged,'rows_skipped',0
      )
      where id=v_import_id;

      perform public._import_review_create_core_v2(
        v_import_id,'COMPLETE_ALL',v_min_date,v_max_date,
        pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
          'source_client_key','client:'||v_client_id::text,
          'source_display_label',v_client.name,
          'client_id',v_client_id
        )),
        '[]'::jsonb,
        pg_catalog.encode(v_upload.content_sha256,'hex'),
        'WEEKLY_SOURCE_MODE_A_BRIDGE_V1',
        v_actor,v_operation_key,null,null
      );
    end if;

    -- THE established validation-only comparison engine.  Nothing below
    -- recomputes hours, breaks or references; it only records what this owner
    -- decided.
    v_preview:=public.hr_weekly_validation_preview(v_import_id);

    for v_group_row in
      select value from pg_catalog.jsonb_array_elements(coalesce(v_preview->'rows','[]'::jsonb))
    loop
      if nullif(v_group_row->>'timesheet_id','') is null then
        v_unrepresentable:=v_unrepresentable||pg_catalog.jsonb_build_array(
          pg_catalog.jsonb_build_object(
            'reason','NO_TIMESHEET','candidate_id',v_group_row->>'candidate_id',
            'week_ending_date',v_group_row->>'week_ending_date'
          )
        );
        continue;
      end if;
      select * into v_timesheet from public.timesheets
      where timesheet_id=(v_group_row->>'timesheet_id')::uuid;
      if not found then continue; end if;

      for v_comparison in
        select value from pg_catalog.jsonb_array_elements(
          coalesce(v_group_row->'comparisons','[]'::jsonb)
        )
      loop
        -- Only a comparison that the established owner paired to exactly one
        -- staged source row can be represented: the relation requires both a
        -- Weekly Source work event and the signed Candidate times.
        if nullif(v_comparison->>'hr_row_id','') is null
           or nullif(v_comparison->>'timesheet_start','') is null
           or nullif(v_comparison->>'timesheet_end','') is null then
          v_unrepresentable:=v_unrepresentable||pg_catalog.jsonb_build_array(
            pg_catalog.jsonb_build_object(
              'reason',coalesce(v_comparison->>'match_status','UNKNOWN'),
              'timesheet_id',v_group_row->>'timesheet_id',
              'work_date',v_comparison->>'work_date'
            )
          );
          continue;
        end if;

        select * into v_upload_row from public.weekly_source_upload_rows
        where id=(
          select (row_data.payload_json->>'weekly_source_upload_row_id')::uuid
          from public.hr_rows row_data where row_data.id=(v_comparison->>'hr_row_id')::uuid
        );
        if not found then continue; end if;
        select * into v_resolution from public.weekly_source_row_resolutions resolution
        where resolution.upload_row_id=v_upload_row.id
          and resolution.generation=coalesce(
            v_publication.projection_generation,v_publication.authority_scope_version::integer);
        if not found or v_resolution.work_event_id is null then continue; end if;
        v_work_event_id:=v_resolution.work_event_id;

        v_reference:=nullif(pg_catalog.btrim(coalesce(v_comparison->>'ref_after','')),'');
        v_break_match:=coalesce(
          (v_comparison->>'timesheet_break_mins')::integer,0
        )=coalesce((v_comparison->>'healthroster_break_mins')::integer,0);
        v_state:=case
          when pg_catalog.upper(coalesce(v_comparison->>'match_status',''))='MATCH'
            and v_reference is not null and v_break_match then 'EXACT_MATCH'
          when pg_catalog.upper(coalesce(v_comparison->>'match_status',''))='MATCH'
            then 'REFERENCE_MISSING'
          when pg_catalog.upper(coalesce(v_comparison->>'match_status',''))='AMBIGUOUS'
            then 'AMBIGUOUS_SOURCE_ROW'
          else 'HOURS_MISMATCH' end;

        v_fingerprint:=private.weekly_source_sha256_jsonb_v1(
          'WEEKLY_TIMESHEET_SOURCE_COMPARISON_V1',
          pg_catalog.jsonb_build_object(
            'projection_publication_id',v_publication_id,
            'timesheet_id',v_timesheet.timesheet_id,
            'timesheet_revision',v_timesheet.version,
            'work_event_id',v_work_event_id,
            'comparison',v_comparison
          )
        );

        insert into public.weekly_timesheet_source_comparisons(
          source_cycle_id,upload_id,projection_publication_id,upload_row_id,
          timesheet_id,timesheet_revision,work_event_id,contract_id,work_date,
          comparison_state,candidate_start_at_local,candidate_end_at_local,
          candidate_break_minutes,source_start_at_local,source_end_at_local,
          source_break_minutes,total_break_minutes_match,source_reference_number,
          comparison_fingerprint
        ) values (
          v_cycle.id,v_upload.id,v_publication_id,v_upload_row.id,
          v_timesheet.timesheet_id,v_timesheet.version,v_work_event_id,
          v_resolution.contract_id,(v_comparison->>'work_date')::date,
          v_state,
          ((v_comparison->>'work_date')||' '||(v_comparison->>'timesheet_start'))::timestamp,
          ((v_comparison->>'work_date')||' '||(v_comparison->>'timesheet_end'))::timestamp
            +case when (v_comparison->>'timesheet_end')::time<=(v_comparison->>'timesheet_start')::time
                  then interval '1 day' else interval '0' end,
          coalesce((v_comparison->>'timesheet_break_mins')::integer,0),
          v_upload_row.start_at_local,v_upload_row.end_at_local,
          v_upload_row.break_minutes,v_break_match,v_reference,
          v_fingerprint
        ) on conflict (projection_publication_id,timesheet_id,timesheet_revision,work_event_id)
          do nothing;
        if found then v_written:=v_written+1; end if;
      end loop;
    end loop;

    v_dispatched:=true;
    v_client_results:=v_client_results||pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'client_id',v_client_id,'import_id',v_import_id,
        'staged_source_rows',v_staged,
        'unmapped_candidate_rows',v_preview->'unmapped_candidate_rows',
        'unmatched_timesheet_triples',v_preview->'unmatched_timesheet_triples'
      )
    );
  end loop;

  return pg_catalog.jsonb_build_object(
    'ok',true,'dispatched',v_dispatched,'publication_id',v_publication_id,
    'timesheet_authority_rows',v_source_row_total,
    'comparisons_written',v_written,
    'comparisons_not_representable',v_unrepresentable,
    'clients',v_client_results
  );
end;
$function$;

-- WP-37 (WP-31 finding F3).  Weekly Source's own evidence that it declined to
-- ask the established owner for a reference on this physical Timesheet id, and
-- why.  It reads the comparison rows only — never `public.timesheets` and never
-- the settings resolver — so a superseded head can be recorded without going
-- anywhere near the `P0002` the resolver raises for a non-current id.
create or replace function private.weekly_source_mode_a_refuse_operation_v1(
  p_source_cycle_id uuid,
  p_upload_id uuid,
  p_publication_id uuid,
  p_timesheet_id uuid,
  p_actor_user_id uuid,
  p_result_code text
) returns void
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_items jsonb;
  v_count integer;
  v_revision integer;
begin
  select coalesce(pg_catalog.jsonb_agg(
           pg_catalog.jsonb_build_object(
             'comparison_id',comparison.id,'upload_row_id',comparison.upload_row_id,
             'work_event_id',comparison.work_event_id,'work_date',comparison.work_date,
             'reference_number',comparison.source_reference_number
           ) order by comparison.work_date,comparison.id
         ),'[]'::jsonb),
         pg_catalog.count(*)::integer,
         pg_catalog.max(comparison.timesheet_revision)
    into v_items,v_count,v_revision
  from public.weekly_timesheet_source_comparisons comparison
  where comparison.projection_publication_id=p_publication_id
    and comparison.timesheet_id=p_timesheet_id
    and comparison.comparison_state='EXACT_MATCH';
  if coalesce(v_count,0)=0 or v_revision is null then
    return;
  end if;
  insert into public.weekly_timesheet_reference_apply_operations(
    source_cycle_id,upload_id,projection_publication_id,timesheet_id,
    expected_timesheet_revision,expected_item_count,expected_item_manifest_hash,
    auto_authorise_requested,auto_authorise_applied,state,result_code,
    completed_at_utc,actor_user_id,idempotency_key,operation_hash
  ) values (
    p_source_cycle_id,p_upload_id,p_publication_id,p_timesheet_id,
    v_revision,v_count,
    private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_TIMESHEET_REFERENCE_APPLY_MANIFEST_V1',v_items
    ),
    false,false,'REFUSED',p_result_code,pg_catalog.transaction_timestamp(),p_actor_user_id,
    'weekly-source-mode-a-apply:'||p_publication_id::text||':'||p_timesheet_id::text
      ||':'||v_revision::text,
    private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_TIMESHEET_REFERENCE_APPLY_OPERATION_V1',
      pg_catalog.jsonb_build_object(
        'projection_publication_id',p_publication_id,'timesheet_id',p_timesheet_id,
        'timesheet_revision',v_revision,'manifest',v_items
      )
    )
  )
  on conflict (idempotency_key) do nothing;
end;
$function$;

create or replace function public.weekly_source_mode_a_reference_apply_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_publication_id uuid;
  v_operation_id uuid;
  v_publication public.weekly_source_projection_publications%rowtype;
  v_upload public.weekly_source_uploads%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_client_id uuid;
  v_operation_key text;
  v_import public.hr_imports%rowtype;
  v_state public.import_review_states%rowtype;
  v_envelope jsonb;
  v_request_hash text;
  v_apply jsonb;
  v_auto_requested boolean;
  v_timesheet_id uuid;
  v_timesheet public.timesheets%rowtype;
  v_items jsonb;
  v_item_count integer;
  v_manifest_hash bytea;
  v_operation_row_id uuid;
  v_comparison public.weekly_timesheet_source_comparisons%rowtype;
  v_ordinal integer;
  v_applied_ids uuid[];
  -- WP-37: the established family resolver's answer for one physical id.
  v_root_identity jsonb;
  v_root_reason text;
  v_refused integer:=0;
  v_results jsonb:='[]'::jsonb;
  v_client_results jsonb:='[]'::jsonb;
begin
  if coalesce(current_setting('request.jwt.claim.role',true),
       nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(
       select 1 from pg_catalog.jsonb_object_keys(p_request) key
       where key not in ('actor_user_id','publication_id','operation_id')
     ) then
    raise exception 'WEEKLY_SOURCE_MODE_A_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_publication_id:=(p_request->>'publication_id')::uuid;
    v_operation_id:=coalesce(nullif(p_request->>'operation_id','')::uuid,pg_catalog.gen_random_uuid());
  exception when invalid_text_representation then
    raise exception 'WEEKLY_SOURCE_MODE_A_REQUEST_INVALID' using errcode='22023';
  end;
  if v_actor is null or v_publication_id is null then
    raise exception 'WEEKLY_SOURCE_MODE_A_REQUEST_INVALID' using errcode='22023';
  end if;

  select * into v_publication
  from public.weekly_source_projection_publications where id=v_publication_id for update;
  if not found then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_NOT_FOUND' using errcode='22023';
  end if;
  select * into strict v_upload from public.weekly_source_uploads where id=v_publication.upload_id;
  select * into strict v_cycle from public.weekly_source_cycles where id=v_publication.source_cycle_id;
  select * into strict v_group from public.weekly_source_groups where id=v_cycle.source_group_id;
  perform private.weekly_source_office_authority_v1(
    v_actor,'RECHECK_SOURCE',v_group.id,null,v_cycle.finalisation_week_ending
  );
  perform public._import_review_assert_actor_v1(v_actor);

  for v_client_id in
    select distinct contract.client_id
    from public.weekly_timesheet_source_comparisons comparison
    join public.contracts contract on contract.id=comparison.contract_id
    where comparison.projection_publication_id=v_publication_id
    order by 1
  loop
    v_operation_key:=private.weekly_source_mode_a_operation_key_v1(v_publication_id,v_client_id);
    select * into v_import from public.hr_imports where coverage_operation_key=v_operation_key;
    if not found then
      raise exception 'WEEKLY_SOURCE_MODE_A_NOT_DISPATCHED' using errcode='55000',
        detail=pg_catalog.jsonb_build_object('client_id',v_client_id)::text;
    end if;
    select * into strict v_state from public.import_review_states where import_id=v_import.id;

    -- Every Weekly Source reference-apply operation is recorded before the
    -- established owner runs, one row per signed Timesheet with at least one
    -- exact match, so the Weekly Source side carries its own evidence of what
    -- it asked for.
    --
    -- WP-37, closing WP-31's finding F3.  A CloudTMS Timesheet id ROTATES when
    -- the Timesheet is replaced by a newer version (`24 §9A`), and the
    -- comparisons written against the superseded head remain.  Iterating those
    -- physical ids used to hand a non-current id to the established settings
    -- resolver, which raised `P0002 CONTRACT_SETTINGS_CURRENT_TIMESHEET_NOT_FOUND`
    -- and aborted the whole call — so one superseded head blocked the CURRENT
    -- head's reference and its auto-authorisation decision.  Each physical id
    -- is therefore resolved through the established family resolver
    -- `private.weekly_source_resolve_root_identity_v1` (itself the installed
    -- `public._pay_timesheet_rotation_scope`; no second resolver is written
    -- here), and only the family's canonical current head proceeds.  A
    -- superseded member is recorded as REFUSED with its reason and skipped —
    -- `24 §9A`: "the older id is never authorised" — and a family whose
    -- identity cannot be established fails closed the same way.
    for v_timesheet_id in
      select distinct comparison.timesheet_id
      from public.weekly_timesheet_source_comparisons comparison
      join public.contracts contract on contract.id=comparison.contract_id
      where comparison.projection_publication_id=v_publication_id
        and contract.client_id=v_client_id
        and comparison.comparison_state='EXACT_MATCH'
      order by 1
    loop
      v_root_identity:=private.weekly_source_resolve_root_identity_v1(v_timesheet_id);
      -- A boolean read from JSON is three-valued: an absent key, a JSON null
      -- and a non-boolean value must all be the UNSAFE answer, so every read
      -- below is gated on `jsonb_typeof` before it is trusted.
      v_root_reason:=case
        when pg_catalog.jsonb_typeof(v_root_identity->'ok')<>'boolean'
          or (v_root_identity->>'ok')::boolean is not true
          then coalesce(nullif(v_root_identity->>'reason',''),'ROOT_IDENTITY_UNRESOLVED')
        when pg_catalog.jsonb_typeof(v_root_identity->'requested_is_canonical')<>'boolean'
          or (v_root_identity->>'requested_is_canonical')::boolean is not true
          then 'TIMESHEET_SUPERSEDED'
        when pg_catalog.jsonb_typeof(v_root_identity->'family_is_current')<>'boolean'
          or (v_root_identity->>'family_is_current')::boolean is not true
          then 'TIMESHEET_NOT_CURRENT'
        else null
      end;
      if v_root_reason is not null then
        perform private.weekly_source_mode_a_refuse_operation_v1(
          v_cycle.id,v_upload.id,v_publication_id,v_timesheet_id,v_actor,
          'WEEKLY_SOURCE_MODE_A_'||v_root_reason
        );
        v_refused:=v_refused+1;
        continue;
      end if;
      select * into strict v_timesheet from public.timesheets where timesheet_id=v_timesheet_id;
      select coalesce(pg_catalog.jsonb_agg(
               pg_catalog.jsonb_build_object(
                 'comparison_id',comparison.id,'upload_row_id',comparison.upload_row_id,
                 'work_event_id',comparison.work_event_id,'work_date',comparison.work_date,
                 'reference_number',comparison.source_reference_number
               ) order by comparison.work_date,comparison.id
             ),'[]'::jsonb),
             count(*)::integer
      into v_items,v_item_count
      from public.weekly_timesheet_source_comparisons comparison
      where comparison.projection_publication_id=v_publication_id
        and comparison.timesheet_id=v_timesheet_id
        and comparison.comparison_state='EXACT_MATCH';
      if v_item_count=0 then continue; end if;

      v_manifest_hash:=private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_TIMESHEET_REFERENCE_APPLY_MANIFEST_V1',v_items
      );
      -- The auto-authorisation decision is the established resolver's, taken
      -- in validation context exactly as
      -- public._import_review_auto_authorise_targets_core_v1 reads it.
      v_auto_requested:=coalesce((
        public.import_auto_authorise_policy_resolve_v2(
          'HEALTHROSTER'::public.hr_source_enum,v_client_id,v_timesheet.contract_id,
          v_timesheet_id,v_timesheet.week_ending_date,true
        )->>'effective_value')::boolean,false);

      insert into public.weekly_timesheet_reference_apply_operations(
        source_cycle_id,upload_id,projection_publication_id,timesheet_id,
        expected_timesheet_revision,expected_item_count,expected_item_manifest_hash,
        auto_authorise_requested,auto_authorise_applied,state,actor_user_id,
        idempotency_key,operation_hash
      ) values (
        v_cycle.id,v_upload.id,v_publication_id,v_timesheet_id,
        v_timesheet.version,v_item_count,v_manifest_hash,
        v_auto_requested,false,'READY',v_actor,
        'weekly-source-mode-a-apply:'||v_publication_id::text||':'||v_timesheet_id::text
          ||':'||v_timesheet.version::text,
        private.weekly_source_sha256_jsonb_v1(
          'WEEKLY_TIMESHEET_REFERENCE_APPLY_OPERATION_V1',
          pg_catalog.jsonb_build_object(
            'projection_publication_id',v_publication_id,'timesheet_id',v_timesheet_id,
            'timesheet_revision',v_timesheet.version,'manifest',v_items
          )
        )
      )
      on conflict (idempotency_key) do nothing
      returning id into v_operation_row_id;
      if v_operation_row_id is null then
        select id into v_operation_row_id
        from public.weekly_timesheet_reference_apply_operations
        where idempotency_key='weekly-source-mode-a-apply:'||v_publication_id::text||':'
          ||v_timesheet_id::text||':'||v_timesheet.version::text;
        continue;
      end if;

      v_ordinal:=0;
      for v_comparison in
        select * from public.weekly_timesheet_source_comparisons comparison
        where comparison.projection_publication_id=v_publication_id
          and comparison.timesheet_id=v_timesheet_id
          and comparison.comparison_state='EXACT_MATCH'
        order by comparison.work_date,comparison.id
      loop
        v_ordinal:=v_ordinal+1;
        insert into public.weekly_timesheet_reference_apply_items(
          operation_id,item_ordinal,comparison_id,upload_row_id,work_event_id,
          work_date,reference_number,reference_fact_hash
        ) values (
          v_operation_row_id,v_ordinal,v_comparison.id,v_comparison.upload_row_id,
          v_comparison.work_event_id,v_comparison.work_date,
          v_comparison.source_reference_number,
          private.weekly_source_sha256_jsonb_v1(
            'WEEKLY_TIMESHEET_REFERENCE_FACT_V1',
            pg_catalog.jsonb_build_object(
              'comparison_id',v_comparison.id,'work_event_id',v_comparison.work_event_id,
              'work_date',v_comparison.work_date,
              'reference_number',v_comparison.source_reference_number
            )
          )
        );
      end loop;
      v_results:=v_results||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'operation_id',v_operation_row_id,'timesheet_id',v_timesheet_id,
        'item_count',v_item_count,'auto_authorise_requested',v_auto_requested
      ));
    end loop;

    -- The established reference and apply owner.  The apply contract is the
    -- server's own envelope, hashed by the import-review owner, never composed
    -- from caller facts.
    v_envelope:=public._import_review_apply_envelope_core_v1(v_import.id);
    v_request_hash:=public._import_review_hash_v1(v_envelope::text);
    if pg_catalog.jsonb_array_length(coalesce(v_envelope->'selected_action_ids','[]'::jsonb))=0 then
      update public.weekly_timesheet_reference_apply_operations
      set state='REFUSED',result_code='IMPORT_REVIEW_NO_READY_SELECTED_ACTIONS',
        completed_at_utc=pg_catalog.transaction_timestamp()
      where projection_publication_id=v_publication_id and state='READY'
        and timesheet_id in (
          select comparison.timesheet_id from public.weekly_timesheet_source_comparisons comparison
          join public.contracts contract on contract.id=comparison.contract_id
          where comparison.projection_publication_id=v_publication_id and contract.client_id=v_client_id
        );
      v_client_results:=v_client_results||pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object('client_id',v_client_id,'import_id',v_import.id,
          'applied',false,'reason_code','IMPORT_REVIEW_NO_READY_SELECTED_ACTIONS')
      );
      continue;
    end if;

    v_apply:=public.hr_weekly_apply_transactional(
      v_import.id,
      pg_catalog.jsonb_build_object(
        'review_contract',pg_catalog.jsonb_build_object(
          'schema_version','IMPORT_REVIEW_APPLY_CONTRACT_V1',
          'operation_id',v_operation_id,
          'state_version',v_state.state_version,
          'coverage_fingerprint',v_import.coverage_fingerprint,
          'preview_fingerprint',v_state.preview_fingerprint,
          'request_hash',v_request_hash
        ),
        'review_selected_action_ids',coalesce(v_envelope->'selected_action_ids','[]'::jsonb),
        'invalidation_action_ids',coalesce(v_envelope->'reference_invalidation_action_ids','[]'::jsonb)
      ),
      v_actor
    );

    -- The established owner names the Timesheets it auto-authorised.  This
    -- owner only records that answer; it never authorises anything itself.
    select coalesce(array_agg(distinct value::uuid),array[]::uuid[]) into v_applied_ids
    from pg_catalog.jsonb_array_elements_text(
      coalesce(v_apply->'auto_authorise_timesheet_ids','[]'::jsonb)
    ) value;

    update public.weekly_timesheet_reference_apply_operations operation
    set state='APPLIED',result_code='IMPORT_REVIEW_APPLIED',
      auto_authorise_applied=operation.timesheet_id=any(v_applied_ids),
      completed_at_utc=pg_catalog.transaction_timestamp()
    where operation.projection_publication_id=v_publication_id and operation.state='READY'
      and operation.timesheet_id in (
        select comparison.timesheet_id from public.weekly_timesheet_source_comparisons comparison
        join public.contracts contract on contract.id=comparison.contract_id
        where comparison.projection_publication_id=v_publication_id and contract.client_id=v_client_id
      );
    update public.weekly_timesheet_reference_apply_items item
    set applied_at_utc=pg_catalog.transaction_timestamp()
    where item.operation_id in (
      select operation.id from public.weekly_timesheet_reference_apply_operations operation
      where operation.projection_publication_id=v_publication_id and operation.state='APPLIED'
    ) and item.applied_at_utc is null;

    v_client_results:=v_client_results||pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object('client_id',v_client_id,'import_id',v_import.id,
        'applied',true,'apply_result',v_apply,
        -- WP-37.  `hr_weekly_apply_transactional` does not authorise: it records
        -- the committed operation and NAMES the post-commit targets in
        -- `auto_authorise_timesheet_ids`, which the established import-review
        -- follow-up owner then acts on.  These three values are that owner's
        -- own handle (`public.import_review_apply_status_get_v1`), so the
        -- acceptance route can run the SAME follow-up the ordinary Imports
        -- route runs instead of inventing a second authorisation path
        -- (`25 §9` "Complete coverage may auto-authorise…").
        'operation_id',v_operation_id,
        'request_hash',v_request_hash,
        'auto_authorise_timesheet_ids',
          coalesce(v_apply->'auto_authorise_timesheet_ids','[]'::jsonb))
    );
  end loop;

  return pg_catalog.jsonb_build_object(
    'ok',true,'publication_id',v_publication_id,
    'operations',v_results,'clients',v_client_results,
    -- WP-37: physical Timesheet ids skipped because the family's canonical
    -- current head is a different row (`24 §9A`), recorded as REFUSED.
    'superseded_heads_refused',v_refused
  );
end;
$function$;

alter function private.weekly_source_mode_a_role_type_v1(text,text) owner to postgres;
alter function private.weekly_source_mode_a_operation_key_v1(uuid,uuid) owner to postgres;
alter function private.weekly_source_mode_a_refuse_operation_v1(uuid,uuid,uuid,uuid,uuid,text)
  owner to postgres;
alter function public.weekly_source_mode_a_dispatch_atomic_v1(jsonb) owner to postgres;
alter function public.weekly_source_mode_a_reference_apply_atomic_v1(jsonb) owner to postgres;
revoke all on function private.weekly_source_mode_a_role_type_v1(text,text)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_mode_a_operation_key_v1(uuid,uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_mode_a_refuse_operation_v1(uuid,uuid,uuid,uuid,uuid,text)
  from public,anon,authenticated,service_role;
revoke all on function public.weekly_source_mode_a_dispatch_atomic_v1(jsonb)
  from public,anon,authenticated;
revoke all on function public.weekly_source_mode_a_reference_apply_atomic_v1(jsonb)
  from public,anon,authenticated;
grant execute on function public.weekly_source_mode_a_dispatch_atomic_v1(jsonb) to service_role;
grant execute on function public.weekly_source_mode_a_reference_apply_atomic_v1(jsonb) to service_role;

comment on function public.weekly_source_mode_a_dispatch_atomic_v1(jsonb) is
  'Dispatches the signed-Timesheet-authority rows of one Weekly Source projection publication to the established validation-only Mode A route and records that owner''s comparisons. Creates no source manifest, self-bill, cutoff or protected-pay work.';
comment on function public.weekly_source_mode_a_reference_apply_atomic_v1(jsonb) is
  'Runs the established import-review weekly apply owner for a dispatched Weekly Source Mode A import and records the reference-apply operation and items. Writes no reference itself.';

notify pgrst, 'reload schema';

commit;
