-- Repeatable CloudTMS authority: weekly_source_upload_publication_v1
-- Owns only immutable Weekly source upload evidence, complete-upload
-- supersession and compare-and-swap preview publication. It creates no
-- Timesheet, financial, invoice, Workbench or Banking Pay record.

\set ON_ERROR_STOP on

begin;

create or replace function private.weekly_source_hex32_v1(
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

create or replace function private.weekly_source_canonical_report_number_v1(
  p_value text
) returns text
language sql immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select nullif(
    pg_catalog.upper(
      pg_catalog.regexp_replace(pg_catalog.btrim(coalesce(p_value,'')),'[[:space:]]+',' ','g')
    ),
    ''
  );
$function$;

create or replace function public.weekly_source_upload_stage_begin_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_allowed_keys constant text[]:=array[
    'actor_user_id','environment','agency_id','source_group_id','source_cycle_id',
    'report_scope_id','client_id','original_filename','content_sha256','byte_count',
    'profile_code','profile_version','parser_version','normaliser_version',
    'workbook_part_and_sheet_fingerprint','header_coordinate_map_json',
    'header_coordinate_map_hash','money_lexical_authority_version','purpose',
    'correction_session_id','expected_correction_session_version','declared_scope_fingerprint',
    'suggested_coverage_start_local_date','suggested_coverage_end_local_date',
    'confirmed_coverage_start_local_date','confirmed_coverage_end_local_date',
    'coverage_timezone','coverage_confirmation_version','coverage_confirmed_at_utc',
    'coverage_shrink_acknowledged','coverage_state','coverage_proof_kind',
    'physical_row_count','header_count','trailer_count','continuation_count',
    'accepted_count','blocking_economic_duplicate_count','malformed_count',
    'blocked_count','file_metadata_json','parser_summary_json'
  ]::text[];
  v_actor uuid;
  v_environment text;
  v_agency uuid;
  v_group_id uuid;
  v_cycle_id uuid;
  v_report_scope_id uuid;
  v_client_id uuid;
  v_group public.weekly_source_groups%rowtype;
  v_correction public.weekly_final_source_correction_sessions%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_scope public.weekly_source_report_scopes%rowtype;
  v_profile public.weekly_source_format_profiles%rowtype;
  v_purpose text;
  v_content bytea;
  v_workbook_fingerprint bytea;
  v_header_map jsonb;
  v_header_hash bytea;
  v_declared bytea;
  v_computed_scope bytea;
  v_metadata jsonb;
  v_summary jsonb;
  v_report_number text;
  v_existing public.weekly_source_uploads%rowtype;
  v_upload_id uuid;
  v_attempt_id uuid;
  v_unknown_key text;
  v_profile_version integer;
  v_physical integer;
  v_header integer;
  v_trailer integer;
  v_continuation integer;
  v_accepted integer;
  v_duplicate integer;
  v_malformed integer;
  v_blocked integer;
  v_byte_count bigint;
  v_suggested_start date;
  v_suggested_end date;
  v_confirmed_start date;
  v_confirmed_end date;
  v_coverage_timezone text;
  v_confirmation_version text;
  v_coverage_state text;
  v_proof_kind text;
  v_profile_identity text;
  v_expected_correction_version bigint;
begin
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object' then
    raise exception 'WEEKLY_SOURCE_UPLOAD_REQUEST_INVALID' using errcode='22023';
  end if;

  select key into v_unknown_key
  from pg_catalog.jsonb_object_keys(p_request) key
  where not (key=any(v_allowed_keys))
  order by key
  limit 1;
  if v_unknown_key is not null then
    raise exception 'WEEKLY_SOURCE_UPLOAD_REQUEST_UNKNOWN_FIELD'
      using errcode='22023',detail=v_unknown_key;
  end if;

  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_environment:=pg_catalog.upper(pg_catalog.btrim(coalesce(p_request->>'environment','')));
    v_agency:=(p_request->>'agency_id')::uuid;
    v_group_id:=(p_request->>'source_group_id')::uuid;
    v_cycle_id:=(p_request->>'source_cycle_id')::uuid;
    v_report_scope_id:=nullif(p_request->>'report_scope_id','')::uuid;
    v_client_id:=nullif(p_request->>'client_id','')::uuid;
    v_profile_version:=(p_request->>'profile_version')::integer;
    v_byte_count:=(p_request->>'byte_count')::bigint;
    v_physical:=(p_request->>'physical_row_count')::integer;
    v_header:=coalesce((p_request->>'header_count')::integer,0);
    v_trailer:=coalesce((p_request->>'trailer_count')::integer,0);
    v_continuation:=coalesce((p_request->>'continuation_count')::integer,0);
    v_accepted:=coalesce((p_request->>'accepted_count')::integer,0);
    v_duplicate:=coalesce((p_request->>'blocking_economic_duplicate_count')::integer,0);
    v_malformed:=coalesce((p_request->>'malformed_count')::integer,0);
    v_blocked:=coalesce((p_request->>'blocked_count')::integer,0);
    v_suggested_start:=nullif(p_request->>'suggested_coverage_start_local_date','')::date;
    v_suggested_end:=nullif(p_request->>'suggested_coverage_end_local_date','')::date;
    v_confirmed_start:=nullif(p_request->>'confirmed_coverage_start_local_date','')::date;
    v_confirmed_end:=nullif(p_request->>'confirmed_coverage_end_local_date','')::date;
    v_expected_correction_version:=nullif(p_request->>'expected_correction_session_version','')::bigint;
  exception when invalid_text_representation or numeric_value_out_of_range or datetime_field_overflow then
    raise exception 'WEEKLY_SOURCE_UPLOAD_REQUEST_TYPE_INVALID' using errcode='22023';
  end;

  if v_environment not in ('TEST','LIVE') or v_agency is null
     or v_group_id is null or v_cycle_id is null or v_actor is null then
    raise exception 'WEEKLY_SOURCE_UPLOAD_SCOPE_REQUIRED' using errcode='22023';
  end if;
  if nullif(pg_catalog.btrim(coalesce(p_request->>'original_filename','')),'') is null
     or pg_catalog.char_length(p_request->>'original_filename')>255 then
    raise exception 'WEEKLY_SOURCE_FILENAME_INVALID' using errcode='22023';
  end if;
  if v_byte_count is null or v_byte_count<=0
     or v_physical is null or least(v_physical,v_header,v_trailer,v_continuation,
       v_accepted,v_duplicate,v_malformed,v_blocked)<0 then
    raise exception 'WEEKLY_SOURCE_DECLARED_COUNTS_INVALID' using errcode='22023';
  end if;

  v_content:=private.weekly_source_hex32_v1(
    p_request->>'content_sha256','WEEKLY_SOURCE_CONTENT_HASH_INVALID'
  );
  v_header_map:=coalesce(p_request->'header_coordinate_map_json','{}'::jsonb);
  v_metadata:=coalesce(p_request->'file_metadata_json','{}'::jsonb);
  v_summary:=coalesce(p_request->'parser_summary_json','{}'::jsonb);
  if pg_catalog.jsonb_typeof(v_header_map)<>'object'
     or pg_catalog.jsonb_typeof(v_metadata)<>'object'
     or pg_catalog.jsonb_typeof(v_summary)<>'object' then
    raise exception 'WEEKLY_SOURCE_UPLOAD_OBJECT_INVALID' using errcode='22023';
  end if;
  v_header_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_HEADER_COORDINATE_MAP_V1',v_header_map
  );
  if nullif(p_request->>'header_coordinate_map_hash','') is not null
     and private.weekly_source_hex32_v1(
       p_request->>'header_coordinate_map_hash','WEEKLY_SOURCE_HEADER_MAP_HASH_INVALID'
     ) is distinct from v_header_hash then
    raise exception 'WEEKLY_SOURCE_HEADER_MAP_HASH_MISMATCH' using errcode='22023';
  end if;
  v_workbook_fingerprint:=case
    when nullif(p_request->>'workbook_part_and_sheet_fingerprint','') is null then null
    else private.weekly_source_hex32_v1(
      p_request->>'workbook_part_and_sheet_fingerprint',
      'WEEKLY_SOURCE_WORKBOOK_FINGERPRINT_INVALID'
    ) end;

  select * into v_group from public.weekly_source_groups where id=v_group_id;
  if not found or not v_group.active
     or v_group.environment is distinct from v_environment
     or v_group.agency_id is distinct from v_agency then
    raise exception 'WEEKLY_SOURCE_GROUP_SCOPE_MISMATCH' using errcode='22023';
  end if;
  select * into v_cycle from public.weekly_source_cycles where id=v_cycle_id;
  if not found or v_cycle.source_group_id is distinct from v_group_id then
    raise exception 'WEEKLY_SOURCE_CYCLE_SCOPE_MISMATCH' using errcode='22023';
  end if;
  select * into v_profile
  from public.weekly_source_format_profiles
  where profile_code=pg_catalog.btrim(coalesce(p_request->>'profile_code',''))
    and version=v_profile_version;
  if not found or not v_profile.active or v_profile.scheduled_hours_fallback then
    raise exception 'WEEKLY_SOURCE_PROFILE_NOT_RELEASED' using errcode='22023';
  end if;
  if v_profile.profile_code not in (
    'NHSP_PREFINAL_RELEASED_V1','NHSP_FINAL_BACKING_V1',
    'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1',
    'HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1',
    'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1'
  ) or v_profile.version<>1 then
    raise exception 'WEEKLY_SOURCE_PROFILE_NOT_ADMITTED' using errcode='22023';
  end if;
  -- An approved workbook profile may also arrive as one self-contained
  -- data-bearing HTML table (including NHSP's Excel-exported .xls). Only
  -- the server parser identifies that container; HTML has no OOXML part to
  -- fingerprint, while an actual XLSX must retain its selected-part proof.
  if v_profile.container_kind='XLSX' then
    if pg_catalog.upper(pg_catalog.btrim(coalesce(v_summary->>'source_kind','XLSX')))
       not in ('XLSX','HTML')
       or (pg_catalog.upper(pg_catalog.btrim(coalesce(v_summary->>'source_kind','XLSX')))='XLSX')
          is distinct from (v_workbook_fingerprint is not null) then
      raise exception 'WEEKLY_SOURCE_WORKBOOK_FINGERPRINT_REQUIRED' using errcode='22023';
    end if;
  elsif v_workbook_fingerprint is not null then
    raise exception 'WEEKLY_SOURCE_WORKBOOK_FINGERPRINT_REQUIRED' using errcode='22023';
  end if;
  if v_profile.profile_code in ('NHSP_PREFINAL_RELEASED_V1','NHSP_FINAL_BACKING_V1') then
    if nullif(p_request->>'money_lexical_authority_version','') is distinct from
       'XLSX_BINARY64_SAME_VALUE_PENCE_V1' then
      raise exception 'WEEKLY_SOURCE_NHSP_MONEY_AUTHORITY_INVALID' using errcode='22023';
    end if;
  elsif nullif(p_request->>'money_lexical_authority_version','') is not null then
    raise exception 'WEEKLY_SOURCE_MONEY_AUTHORITY_NOT_ALLOWED' using errcode='22023';
  end if;

  v_purpose:=pg_catalog.upper(pg_catalog.btrim(coalesce(p_request->>'purpose','ORDINARY')));
  if v_purpose not in ('ORDINARY','FINAL_SOURCE_CORRECTION') then
    raise exception 'WEEKLY_SOURCE_UPLOAD_PURPOSE_INVALID' using errcode='22023';
  end if;
  if nullif(pg_catalog.btrim(coalesce(p_request->>'parser_version','')),'') is null
     or nullif(pg_catalog.btrim(coalesce(p_request->>'normaliser_version','')),'') is null then
    raise exception 'WEEKLY_SOURCE_PARSER_VERSION_REQUIRED' using errcode='22023';
  end if;
  v_coverage_timezone:=nullif(pg_catalog.btrim(coalesce(p_request->>'coverage_timezone','')),'');
  v_confirmation_version:=nullif(pg_catalog.btrim(coalesce(p_request->>'coverage_confirmation_version','')),'');
  v_coverage_state:=pg_catalog.upper(pg_catalog.btrim(coalesce(p_request->>'coverage_state','')));
  v_proof_kind:=pg_catalog.upper(pg_catalog.btrim(coalesce(p_request->>'coverage_proof_kind','')));

  if v_profile.profile_code='NHSP_FINAL_BACKING_V1' then
    if v_group.source_family<>'NHSP' or v_report_scope_id is null
       or v_profile.final_authority_kind<>'NHSP_TRUST_BACKING_REPORT'
       or v_profile.omission_meaning<>'NO_INFERENCE'
       or not v_profile.report_number_required
       or not v_profile.fmc_must_equal_zero then
      raise exception 'WEEKLY_SOURCE_NHSP_FINAL_PROFILE_INVALID' using errcode='22023';
    end if;
    select * into v_scope
    from public.weekly_source_report_scopes
    where id=v_report_scope_id;
    if not found or v_scope.source_cycle_id is distinct from v_cycle_id
       or v_scope.environment is distinct from v_environment
       or v_scope.agency_id is distinct from v_agency
       or v_scope.source_group_id is distinct from v_group_id
       or v_scope.cutoff_at_utc is distinct from v_cycle.cutoff_at_utc then
      raise exception 'WEEKLY_SOURCE_NHSP_REPORT_SCOPE_MISMATCH' using errcode='22023';
    end if;
    v_client_id:=v_scope.client_id;
    if nullif(p_request->>'client_id','') is not null
       and (p_request->>'client_id')::uuid is distinct from v_client_id then
      raise exception 'WEEKLY_SOURCE_NHSP_CLIENT_MISMATCH' using errcode='22023';
    end if;
    v_report_number:=private.weekly_source_canonical_report_number_v1(
      v_metadata->>'nhsp_report_number'
    );
    if v_report_number is null
       or nullif(pg_catalog.btrim(coalesce(v_metadata->>'nhsp_report_heading_name','')),'')
          is distinct from pg_catalog.btrim(v_group.nhsp_report_heading_name)
       or v_proof_kind<>'NHSP_TRUST_REPORT_SCOPE'
       or v_confirmed_start is not null or v_confirmed_end is not null then
      raise exception 'WEEKLY_SOURCE_NHSP_REPORT_IDENTITY_INVALID' using errcode='22023';
    end if;
    v_metadata:=v_metadata||pg_catalog.jsonb_build_object(
      'nhsp_report_number',v_report_number,'client_id',v_client_id
    );
  elsif v_profile.profile_code='NHSP_PREFINAL_RELEASED_V1' then
    if v_group.source_family<>'NHSP' or v_report_scope_id is not null
       or v_profile.profile_json->>'purpose'<>'PREFINAL_CHECKING'
       or v_profile.row_finalisation_capability<>'CHECKING_ONLY' then
      raise exception 'WEEKLY_SOURCE_NHSP_PREFINAL_SCOPE_INVALID' using errcode='22023';
    end if;
  else
    if v_group.source_family<>'ROSTER' or v_report_scope_id is not null then
      raise exception 'WEEKLY_SOURCE_ROSTER_SCOPE_INVALID' using errcode='22023';
    end if;
    if v_profile.single_client_required and v_client_id is null then
      raise exception 'WEEKLY_SOURCE_CLIENT_SCOPE_REQUIRED' using errcode='22023';
    end if;
    if v_client_id is not null then
      if not exists(
        select 1 from public.weekly_source_group_clients membership
        where membership.source_group_id=v_group_id
          and membership.client_id=v_client_id
          and v_cycle.finalisation_week_ending between membership.valid_from
            and coalesce(membership.valid_to,'infinity'::date)
      ) then
        raise exception 'WEEKLY_SOURCE_CLIENT_SCOPE_MISMATCH' using errcode='22023';
      end if;
      v_metadata:=v_metadata||pg_catalog.jsonb_build_object('client_id',v_client_id);
    end if;
    if v_profile.final_authority_kind='HEALTHROSTER_ACTUAL_ROWS'
       and (pg_catalog.jsonb_typeof(v_metadata->'saved_finalisation_profile_map') is distinct from 'object'
            or v_metadata->'saved_finalisation_profile_map'='{}'::jsonb) then
      raise exception 'WEEKLY_SOURCE_HEALTHROSTER_FINALISATION_MAP_REQUIRED' using errcode='22023';
    end if;
  end if;

  if v_profile.profile_code<>'NHSP_FINAL_BACKING_V1' then
    if v_coverage_state<>'COMPLETE'
       or v_confirmed_start is null or v_confirmed_end is null
       or v_confirmed_start>v_confirmed_end
       or v_coverage_timezone<>'Europe/London'
       or v_confirmation_version is null
       or v_proof_kind not in (
         'FORMAT_MANIFEST','OFFICE_COMPLETE_EXPORT_ATTESTATION',
         'EXPLICIT_EMPTY_CONFIRMATION','HEALTHROSTER_COMPLETE_EXPORT_ATTESTATION'
       ) then
      raise exception 'WEEKLY_SOURCE_COMPLETE_COVERAGE_REQUIRED' using errcode='22023';
    end if;
  end if;
  if (v_suggested_start is null)<>(v_suggested_end is null)
     or (v_confirmed_start is null)<>(v_confirmed_end is null)
     or (v_suggested_start is not null and v_suggested_start>v_suggested_end) then
    raise exception 'WEEKLY_SOURCE_COVERAGE_RANGE_INVALID' using errcode='22023';
  end if;
  -- 14 section 5.4.3.  The service SUGGESTS inclusive coverage from the file's
  -- own earliest and latest valid work Date; Office then CONFIRMS the range for
  -- which the export is complete.  Confirmation is therefore NOT the same fact
  -- as the evidence and must not be forced to equal it: 5.4.5 bullet 4 makes a
  -- Request Id that is "absent inside confirmed coverage" a full cancellation
  -- reversal, so a shift the Trust withdraws at the edge of the week can only
  -- ever be noticed by a confirmed range WIDER than the rows that arrived.  The
  -- same section already contemplates a confirmation with no rows at all ("a
  -- genuinely empty complete export requires explicit start/end dates and
  -- attestation"), which is the same freedom at its limit.
  --
  -- A confirmed range NARROWER than the evidence is refused here: every
  -- accepted row must stay inside the final scope, and the finaliser admits
  -- nothing outside confirmed coverage
  -- (WEEKLY_SOURCE_RESOLUTION_OUTSIDE_FINAL_SCOPE), so a narrower confirmation
  -- could only produce an upload that can never be finalised.  The suggestion
  -- itself is still evidence and is re-checked against the real rows at the
  -- seal.
  if v_profile.profile_code<>'NHSP_FINAL_BACKING_V1' then
    if v_accepted>0 and v_suggested_start is null then
      raise exception 'WEEKLY_SOURCE_COVERAGE_CONFIRMATION_MISMATCH' using errcode='22023';
    end if;
    if v_accepted>0 and (
         v_confirmed_start>v_suggested_start
         or v_confirmed_end<v_suggested_end
       ) then
      raise exception 'WEEKLY_SOURCE_COVERAGE_CONFIRMATION_NARROWER_THAN_EVIDENCE'
        using errcode='22023';
    end if;
    if v_accepted=0 and (v_suggested_start is not null or v_suggested_end is not null) then
      raise exception 'WEEKLY_SOURCE_EMPTY_COVERAGE_SUGGESTION_INVALID' using errcode='22023';
    end if;
  end if;

  v_computed_scope:=private.weekly_source_scope_fingerprint_v1(
    v_environment,v_agency,v_group_id,v_cycle_id,v_report_scope_id,v_client_id
  );
  if nullif(p_request->>'declared_scope_fingerprint','') is not null then
    v_declared:=private.weekly_source_hex32_v1(
      p_request->>'declared_scope_fingerprint','WEEKLY_SOURCE_DECLARED_SCOPE_HASH_INVALID'
    );
    if v_declared is distinct from v_computed_scope then
      raise exception 'WEEKLY_SOURCE_DECLARED_SCOPE_MISMATCH' using errcode='22023';
    end if;
  end if;
  v_declared:=v_computed_scope;

  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended(pg_catalog.encode(v_declared,'hex'),73241837)
  );
  if v_report_scope_id is null then
    select * into strict v_cycle
    from public.weekly_source_cycles where id=v_cycle_id for update;
  else
    select * into strict v_cycle
    from public.weekly_source_cycles where id=v_cycle_id;
  end if;
  if v_cycle.source_group_id is distinct from v_group_id then
    raise exception 'WEEKLY_SOURCE_CYCLE_SCOPE_MISMATCH' using errcode='22023';
  end if;
  if v_report_scope_id is not null then
    select * into strict v_scope
    from public.weekly_source_report_scopes where id=v_report_scope_id for update;
    if v_scope.source_cycle_id is distinct from v_cycle_id
       or v_scope.environment is distinct from v_environment
       or v_scope.agency_id is distinct from v_agency
       or v_scope.source_group_id is distinct from v_group_id
       or v_scope.client_id is distinct from v_client_id
       or v_scope.cutoff_at_utc is distinct from v_cycle.cutoff_at_utc then
      raise exception 'WEEKLY_SOURCE_REPORT_SCOPE_CHANGED' using errcode='55000';
    end if;
  end if;
  if private.weekly_source_scope_fingerprint_v1(
       v_environment,v_agency,v_group_id,v_cycle_id,v_report_scope_id,v_client_id
     ) is distinct from v_declared then
    raise exception 'WEEKLY_SOURCE_SCOPE_CHANGED' using errcode='55000';
  end if;
  perform private.weekly_source_office_authority_v1(
    v_actor,'UPLOAD_SOURCE',v_group_id,v_client_id,v_cycle.finalisation_week_ending
  );

  if v_purpose='FINAL_SOURCE_CORRECTION' then
    if nullif(p_request->>'correction_session_id','') is null then
      raise exception 'WEEKLY_SOURCE_CORRECTION_SESSION_REQUIRED' using errcode='22023';
    end if;
    select * into v_correction
    from public.weekly_final_source_correction_sessions
    where id=(p_request->>'correction_session_id')::uuid for update;
    if not found or v_correction.source_cycle_id is distinct from v_cycle_id
       or v_correction.authority_scope_kind is distinct from
          (case when v_report_scope_id is null then 'CYCLE' else 'NHSP_REPORT_SCOPE' end)
       or v_correction.report_scope_id is distinct from v_report_scope_id
       or v_correction.actor_user_id is distinct from v_actor
       or (v_expected_correction_version is not null
           and v_correction.version is distinct from v_expected_correction_version)
       or v_correction.state not in ('DRAFT','STAGING','READY')
       or (v_correction.state='DRAFT' and v_correction.replacement_correction_upload_id is not null)
       or (v_correction.state in ('STAGING','READY') and v_correction.replacement_correction_upload_id is null) then
      raise exception 'WEEKLY_SOURCE_CORRECTION_SESSION_INVALID' using errcode='22023';
    end if;
    if v_correction.replacement_correction_upload_id is not null then
      select * into strict v_existing
      from public.weekly_source_uploads
      where id=v_correction.replacement_correction_upload_id;
      if v_existing.content_sha256=v_content
         and v_existing.byte_count=v_byte_count
         and v_existing.source_format_profile_id=v_profile.id
         and (v_profile.profile_code='NHSP_FINAL_BACKING_V1'
              or (v_existing.parser_version=p_request->>'parser_version'
                  and v_existing.normaliser_version=p_request->>'normaliser_version'
                  and v_existing.workbook_part_and_sheet_fingerprint is not distinct from v_workbook_fingerprint
                  and v_existing.header_coordinate_map_hash=v_header_hash
                  and v_existing.confirmed_coverage_start_local_date is not distinct from v_confirmed_start
                  and v_existing.confirmed_coverage_end_local_date is not distinct from v_confirmed_end
                  and v_existing.coverage_confirmation_version is not distinct from v_confirmation_version)) then
        v_attempt_id:=private.weekly_source_attempt_append_without_upload_v1(
          v_environment,v_agency,v_group_id,v_cycle_id,v_report_scope_id,v_purpose,
          v_declared,v_correction.id,v_actor,p_request->>'original_filename',v_byte_count,
          v_content,p_request->>'parser_version',
          v_profile.profile_code||':'||v_profile.version::text,
          p_request->>'normaliser_version','DUPLICATE',
          'CORRECTION_UPLOAD_EXACT_REPLAY',v_existing.id
        );
        return pg_catalog.jsonb_build_object(
          'ok',true,'status','DUPLICATE','logical_upload_id',v_existing.id,
          'attempt_id',v_attempt_id,'current_pointer_moved',false
        );
      end if;
      v_attempt_id:=private.weekly_source_attempt_append_without_upload_v1(
        v_environment,v_agency,v_group_id,v_cycle_id,v_report_scope_id,v_purpose,
        v_declared,v_correction.id,v_actor,p_request->>'original_filename',v_byte_count,
        v_content,p_request->>'parser_version',
        v_profile.profile_code||':'||v_profile.version::text,
        p_request->>'normaliser_version','CONFLICT',
        'CORRECTION_SESSION_REPLACEMENT_CONFLICT',v_existing.id
      );
      return pg_catalog.jsonb_build_object(
        'ok',false,'status','CONFLICT',
        'reason_code','CORRECTION_SESSION_REPLACEMENT_CONFLICT',
        'logical_upload_id',v_existing.id,'attempt_id',v_attempt_id,
        'current_pointer_moved',false
      );
    end if;
  elsif nullif(p_request->>'correction_session_id','') is not null
        or v_expected_correction_version is not null then
    raise exception 'WEEKLY_SOURCE_CORRECTION_SESSION_NOT_ALLOWED' using errcode='22023';
  end if;

  if v_profile.profile_code='NHSP_FINAL_BACKING_V1' and v_purpose='ORDINARY' then
    select upload.* into v_existing
    from public.weekly_source_uploads upload
    join public.weekly_source_format_profiles profile
      on profile.id=upload.source_format_profile_id
    where upload.source_cycle_id=v_cycle_id
      and upload.report_scope_id=v_report_scope_id
      and profile.final_authority_kind='NHSP_TRUST_BACKING_REPORT'
      and private.weekly_source_canonical_report_number_v1(
        upload.file_metadata_json->>'nhsp_report_number'
      )=v_report_number
      and upload.content_sha256=v_content
      and upload.byte_count=v_byte_count
      and upload.state<>'REJECTED'
    order by (upload.state='CURRENT') desc,upload.uploaded_at_utc desc,upload.id desc
    limit 1;
    if found then
      v_attempt_id:=private.weekly_source_attempt_append_without_upload_v1(
        v_environment,v_agency,v_group_id,v_cycle_id,v_report_scope_id,v_purpose,
        v_declared,null,v_actor,p_request->>'original_filename',v_byte_count,v_content,
        p_request->>'parser_version',v_profile.profile_code||':'||v_profile.version::text,
        p_request->>'normaliser_version','DUPLICATE','NHSP_REPORT_IDENTICAL_REPLAY',
        v_existing.id
      );
      return pg_catalog.jsonb_build_object(
        'ok',true,'status','DUPLICATE','logical_upload_id',v_existing.id,
        'attempt_id',v_attempt_id,'current_pointer_moved',false
      );
    end if;

    select upload.* into v_existing
    from public.weekly_source_uploads upload
    join public.weekly_source_format_profiles profile
      on profile.id=upload.source_format_profile_id
    where upload.source_cycle_id=v_cycle_id
      and upload.report_scope_id=v_report_scope_id
      and profile.final_authority_kind='NHSP_TRUST_BACKING_REPORT'
      and private.weekly_source_canonical_report_number_v1(
        upload.file_metadata_json->>'nhsp_report_number'
      )=v_report_number
      and upload.state<>'REJECTED'
    order by upload.uploaded_at_utc,upload.id
    limit 1;

    if found then
      v_attempt_id:=private.weekly_source_attempt_append_without_upload_v1(
        v_environment,v_agency,v_group_id,v_cycle_id,v_report_scope_id,v_purpose,
        v_declared,null,v_actor,p_request->>'original_filename',v_byte_count,v_content,
        p_request->>'parser_version',v_profile.profile_code||':'||v_profile.version::text,
        p_request->>'normaliser_version','CONFLICT','NHSP_REPORT_IDENTITY_BYTES_CONFLICT',
        v_existing.id
      );
      return pg_catalog.jsonb_build_object(
        'ok',false,'status','CONFLICT','reason_code','NHSP_REPORT_IDENTITY_BYTES_CONFLICT',
        'logical_upload_id',v_existing.id,'attempt_id',v_attempt_id,
        'current_pointer_moved',false
      );
    end if;
  elsif v_profile.profile_code<>'NHSP_FINAL_BACKING_V1' then
    v_profile_identity:=v_profile.profile_code||':'||v_profile.version::text;
    select upload.* into v_existing
    from public.weekly_source_uploads upload
    where upload.source_cycle_id=v_cycle_id
      and upload.report_scope_id is not distinct from v_report_scope_id
      and upload.purpose=v_purpose
      and upload.content_sha256=v_content
      and upload.byte_count=v_byte_count
      and upload.source_format_profile_id=v_profile.id
      and upload.parser_version=coalesce(nullif(p_request->>'parser_version',''),'')
      and upload.normaliser_version=coalesce(nullif(p_request->>'normaliser_version',''),'')
      and upload.workbook_part_and_sheet_fingerprint is not distinct from v_workbook_fingerprint
      and upload.header_coordinate_map_hash=v_header_hash
      and upload.money_lexical_authority_version is not distinct from
          nullif(p_request->>'money_lexical_authority_version','')
      and upload.correction_session_id is not distinct from
          nullif(p_request->>'correction_session_id','')::uuid
      and upload.declared_scope_fingerprint=v_declared
      and upload.confirmed_coverage_start_local_date is not distinct from v_confirmed_start
      and upload.confirmed_coverage_end_local_date is not distinct from v_confirmed_end
      and upload.coverage_confirmation_version is not distinct from v_confirmation_version
      and coalesce(upload.file_metadata_json->'saved_finalisation_profile_map','null'::jsonb)
          =coalesce(v_metadata->'saved_finalisation_profile_map','null'::jsonb)
      and upload.state<>'REJECTED'
    order by upload.uploaded_at_utc,upload.id
    limit 1;
    if found then
      v_attempt_id:=private.weekly_source_attempt_append_without_upload_v1(
        v_environment,v_agency,v_group_id,v_cycle_id,v_report_scope_id,v_purpose,
        v_declared,nullif(p_request->>'correction_session_id','')::uuid,v_actor,
        p_request->>'original_filename',v_byte_count,v_content,p_request->>'parser_version',
        v_profile_identity,p_request->>'normaliser_version','DUPLICATE',
        'WEEKLY_SOURCE_EXACT_DUPLICATE',v_existing.id
      );
      return pg_catalog.jsonb_build_object(
        'ok',true,'status','DUPLICATE','logical_upload_id',v_existing.id,
        'attempt_id',v_attempt_id,'current_pointer_moved',false
      );
    end if;
  end if;

  insert into public.weekly_source_uploads(
    source_cycle_id,report_scope_id,original_filename,content_sha256,byte_count,
    source_format_profile_id,parser_version,normaliser_version,
    workbook_part_and_sheet_fingerprint,header_coordinate_map_json,
    header_coordinate_map_hash,money_lexical_authority_version,purpose,
    correction_session_id,declared_scope_fingerprint,
    suggested_coverage_start_local_date,suggested_coverage_end_local_date,
    confirmed_coverage_start_local_date,confirmed_coverage_end_local_date,
    coverage_timezone,coverage_confirmation_version,coverage_confirmed_by_user_id,
    coverage_confirmed_at_utc,coverage_shrink_acknowledged,coverage_state,
    coverage_proof_kind,physical_row_count,header_count,trailer_count,
    continuation_count,accepted_count,blocking_economic_duplicate_count,
    malformed_count,blocked_count,state,uploaded_by_user_id,file_metadata_json,
    parser_summary_json
  ) values (
    v_cycle_id,v_report_scope_id,p_request->>'original_filename',v_content,v_byte_count,
    v_profile.id,p_request->>'parser_version',p_request->>'normaliser_version',
    v_workbook_fingerprint,v_header_map,v_header_hash,
    nullif(p_request->>'money_lexical_authority_version',''),v_purpose,
    nullif(p_request->>'correction_session_id','')::uuid,v_declared,
    v_suggested_start,v_suggested_end,v_confirmed_start,v_confirmed_end,
    case when v_confirmed_start is null then null else v_coverage_timezone end,
    case when v_confirmed_start is null then null else v_confirmation_version end,
    case when v_confirmed_start is null then null else v_actor end,
    case when v_confirmed_start is null then null
      else coalesce(nullif(p_request->>'coverage_confirmed_at_utc','')::timestamptz,
        pg_catalog.transaction_timestamp()) end,
    case when p_request?'coverage_shrink_acknowledged'
      then (p_request->>'coverage_shrink_acknowledged')::boolean else null end,
    nullif(v_coverage_state,''),v_proof_kind,v_physical,v_header,v_trailer,
    v_continuation,v_accepted,v_duplicate,v_malformed,v_blocked,'STAGING',v_actor,
    v_metadata,v_summary
  ) returning id into v_upload_id;

  if v_purpose='FINAL_SOURCE_CORRECTION' then
    update public.weekly_final_source_correction_sessions
    set state='STAGING',replacement_correction_upload_id=v_upload_id,
        version=version+1,updated_at_utc=pg_catalog.transaction_timestamp()
    where id=v_correction.id;
  end if;

  return pg_catalog.jsonb_build_object(
    'ok',true,'status','STAGING','logical_upload_id',v_upload_id,
    'declared_scope_fingerprint',pg_catalog.encode(v_declared,'hex'),
    'header_coordinate_map_hash',pg_catalog.encode(v_header_hash,'hex'),
    'current_pointer_moved',false
  );
end;
$function$;

create or replace function private.weekly_source_upload_attempt_append_v1(
  p_upload_id uuid,
  p_actor_user_id uuid,
  p_result text,
  p_reason_code text
) returns uuid
language plpgsql volatile security definer
set search_path to 'public','pg_catalog','pg_temp'
as $function$
declare
  v_upload public.weekly_source_uploads%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_attempt_id uuid;
begin
  select * into v_upload
  from public.weekly_source_uploads
  where id=p_upload_id;
  if not found then
    raise exception 'WEEKLY_SOURCE_UPLOAD_NOT_FOUND' using errcode='22023';
  end if;

  select source_group.* into v_group
  from public.weekly_source_cycles cycle
  join public.weekly_source_groups source_group on source_group.id=cycle.source_group_id
  where cycle.id=v_upload.source_cycle_id;
  if not found then
    raise exception 'WEEKLY_SOURCE_UPLOAD_SCOPE_NOT_FOUND' using errcode='22023';
  end if;

  insert into public.weekly_source_upload_attempts(
    environment,agency_id,source_group_id,source_cycle_id,report_scope_id,
    purpose,declared_scope_fingerprint,correction_session_id,actor_user_id,
    original_filename,byte_count,content_sha256,parser_version,profile_version,
    normaliser_version,result,reason_code,logical_upload_id
  )
  select
    v_group.environment,v_group.agency_id,v_group.id,v_upload.source_cycle_id,
    v_upload.report_scope_id,v_upload.purpose,v_upload.declared_scope_fingerprint,
    v_upload.correction_session_id,p_actor_user_id,v_upload.original_filename,
    v_upload.byte_count,v_upload.content_sha256,v_upload.parser_version,
    profile.profile_code||':'||profile.version::text,v_upload.normaliser_version,
    p_result,p_reason_code,v_upload.id
  from public.weekly_source_format_profiles profile
  where profile.id=v_upload.source_format_profile_id
  returning id into v_attempt_id;

  return v_attempt_id;
end;
$function$;

create or replace function private.weekly_source_attempt_append_without_upload_v1(
  p_environment text,
  p_agency_id uuid,
  p_source_group_id uuid,
  p_source_cycle_id uuid,
  p_report_scope_id uuid,
  p_purpose text,
  p_declared_scope_fingerprint bytea,
  p_correction_session_id uuid,
  p_actor_user_id uuid,
  p_original_filename text,
  p_byte_count bigint,
  p_content_sha256 bytea,
  p_parser_version text,
  p_profile_version text,
  p_normaliser_version text,
  p_result text,
  p_reason_code text,
  p_logical_upload_id uuid default null
) returns uuid
language sql volatile security definer
set search_path to 'public','pg_catalog','pg_temp'
as $function$
  insert into public.weekly_source_upload_attempts(
    environment,agency_id,source_group_id,source_cycle_id,report_scope_id,
    purpose,declared_scope_fingerprint,correction_session_id,actor_user_id,
    original_filename,byte_count,content_sha256,parser_version,profile_version,
    normaliser_version,result,reason_code,logical_upload_id
  ) values (
    p_environment,p_agency_id,p_source_group_id,p_source_cycle_id,p_report_scope_id,
    p_purpose,p_declared_scope_fingerprint,p_correction_session_id,p_actor_user_id,
    p_original_filename,p_byte_count,p_content_sha256,p_parser_version,p_profile_version,
    p_normaliser_version,p_result,p_reason_code,p_logical_upload_id
  ) returning id;
$function$;

create or replace function private.weekly_source_upload_manifest_v1(
  p_upload_id uuid
) returns jsonb
language sql stable security definer
set search_path to 'public','pg_catalog','pg_temp'
as $function$
  select pg_catalog.jsonb_build_object(
    'manifest_version','WEEKLY_SOURCE_ROW_MANIFEST_V1',
    'rows',coalesce((
      select pg_catalog.jsonb_agg(
        pg_catalog.jsonb_build_object(
          'ordinal',physical.source_row_ordinal,
          'classification',physical.classification,
          'physical_sha256',pg_catalog.encode(physical.row_sha256,'hex'),
          'normalised_sha256',case when normalised.id is null then null
            else pg_catalog.encode(normalised.normalised_row_hash,'hex') end,
          'money_token_sha256',coalesce((
            select pg_catalog.jsonb_agg(
              pg_catalog.jsonb_build_object(
                'kind',money.money_field_kind,
                'sha256',pg_catalog.encode(money.token_sha256,'hex')
              ) order by money.money_field_kind
            )
            from public.weekly_source_money_cell_evidence money
            where money.upload_id=physical.upload_id
              and money.source_row_ordinal=physical.source_row_ordinal
          ),'[]'::jsonb),
          'expense_token_sha256',(
            select pg_catalog.encode(expense.token_sha256,'hex')
            from public.weekly_source_expense_cell_evidence expense
            where expense.upload_id=physical.upload_id
              and expense.source_row_ordinal=physical.source_row_ordinal
          )
        ) order by physical.source_row_ordinal
      )
      from public.weekly_source_physical_rows physical
      left join public.weekly_source_upload_rows normalised
        on normalised.upload_id=physical.upload_id
       and normalised.source_row_ordinal=physical.source_row_ordinal
      where physical.upload_id=p_upload_id
    ),'[]'::jsonb)
  );
$function$;

create or replace function private.weekly_source_upload_manifest_hash_v1(
  p_upload_id uuid
) returns bytea
language sql stable security definer
set search_path to 'private','pg_catalog','pg_temp'
as $function$
  select private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_ROW_MANIFEST_V1',
    private.weekly_source_upload_manifest_v1(p_upload_id)
  );
$function$;

create or replace function private.weekly_source_projection_comparison_manifest_v1(
  p_publication_id uuid
) returns jsonb
language sql stable security definer
set search_path to 'public','pg_catalog','pg_temp'
as $function$
  select pg_catalog.jsonb_build_object(
    'manifest_version','WEEKLY_SOURCE_COMPARISON_MANIFEST_V1',
    'source_authority',coalesce((
      select pg_catalog.jsonb_agg(
        pg_catalog.encode(comparison.material_comparison_fingerprint,'hex')
        order by pg_catalog.encode(comparison.material_comparison_fingerprint,'hex')
      )
      from public.weekly_issue_comparison_revisions comparison
      where comparison.projection_publication_id=p_publication_id
    ),'[]'::jsonb),
    'timesheet_authority',coalesce((
      select pg_catalog.jsonb_agg(
        pg_catalog.encode(comparison.comparison_fingerprint,'hex')
        order by pg_catalog.encode(comparison.comparison_fingerprint,'hex')
      )
      from public.weekly_timesheet_source_comparisons comparison
      where comparison.projection_publication_id=p_publication_id
    ),'[]'::jsonb)
  );
$function$;

create or replace function private.weekly_source_projection_comparison_manifest_hash_v1(
  p_publication_id uuid
) returns bytea
language sql stable security definer
set search_path to 'private','pg_catalog','pg_temp'
as $function$
  select private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_COMPARISON_MANIFEST_V1',
    private.weekly_source_projection_comparison_manifest_v1(p_publication_id)
  );
$function$;

create or replace function private.weekly_source_projection_issue_set_v1(
  p_publication_id uuid
) returns jsonb
language sql stable security definer
set search_path to 'public','pg_catalog','pg_temp'
as $function$
  select pg_catalog.jsonb_build_object(
    'issue_set_version','WEEKLY_SOURCE_ISSUE_SET_V1',
    'issues',coalesce((
      select pg_catalog.jsonb_agg(
        pg_catalog.jsonb_build_object(
          'incident_id',comparison.incident_id,
          'issue_family',comparison.issue_family,
          'source_presence',comparison.source_presence,
          'fingerprint',pg_catalog.encode(comparison.material_comparison_fingerprint,'hex')
        ) order by comparison.incident_id,comparison.revision_number
      )
      from public.weekly_issue_comparison_revisions comparison
      where comparison.projection_publication_id=p_publication_id
    ),'[]'::jsonb)
  );
$function$;

create or replace function private.weekly_source_projection_issue_set_hash_v1(
  p_publication_id uuid
) returns bytea
language sql stable security definer
set search_path to 'private','pg_catalog','pg_temp'
as $function$
  select private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_ISSUE_SET_V1',
    private.weekly_source_projection_issue_set_v1(p_publication_id)
  );
$function$;

create or replace function public.weekly_source_upload_attempt_record_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_environment text;
  v_agency uuid;
  v_group uuid;
  v_cycle uuid;
  v_scope uuid;
  v_purpose text;
  v_declared bytea;
  v_correction uuid;
  v_content bytea;
  v_attempt uuid;
  v_result text;
  v_reason text;
  v_client uuid;
  v_logical_upload_id uuid;
  v_group_row public.weekly_source_groups%rowtype;
  v_cycle_row public.weekly_source_cycles%rowtype;
  v_scope_row public.weekly_source_report_scopes%rowtype;
  v_correction_row public.weekly_final_source_correction_sessions%rowtype;
  v_logical_upload public.weekly_source_uploads%rowtype;
begin
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(
       select 1 from pg_catalog.jsonb_object_keys(p_request) key
       where key not in (
         'actor_user_id','environment','agency_id','source_group_id','source_cycle_id',
         'report_scope_id','client_id','purpose','declared_scope_fingerprint',
         'correction_session_id','original_filename','byte_count','content_sha256',
         'parser_version','profile_version','normaliser_version','result','reason_code',
         'logical_upload_id'
       )
     ) then
    raise exception 'WEEKLY_SOURCE_ATTEMPT_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_environment:=pg_catalog.upper(pg_catalog.btrim(coalesce(p_request->>'environment','')));
    v_agency:=(p_request->>'agency_id')::uuid;
    v_group:=(p_request->>'source_group_id')::uuid;
    v_cycle:=nullif(p_request->>'source_cycle_id','')::uuid;
    v_scope:=nullif(p_request->>'report_scope_id','')::uuid;
    v_client:=nullif(p_request->>'client_id','')::uuid;
    v_correction:=nullif(p_request->>'correction_session_id','')::uuid;
    v_logical_upload_id:=nullif(p_request->>'logical_upload_id','')::uuid;
  exception when invalid_text_representation then
    raise exception 'WEEKLY_SOURCE_ATTEMPT_REQUEST_INVALID' using errcode='22023';
  end;
  v_purpose:=pg_catalog.upper(pg_catalog.btrim(coalesce(p_request->>'purpose','ORDINARY')));
  v_declared:=case when nullif(p_request->>'declared_scope_fingerprint','') is null then null
    else private.weekly_source_hex32_v1(p_request->>'declared_scope_fingerprint','WEEKLY_SOURCE_DECLARED_SCOPE_HASH_INVALID') end;
  v_content:=case when nullif(p_request->>'content_sha256','') is null then null
    else private.weekly_source_hex32_v1(p_request->>'content_sha256','WEEKLY_SOURCE_CONTENT_HASH_INVALID') end;
  v_result:=pg_catalog.upper(pg_catalog.btrim(coalesce(p_request->>'result','')));
  v_reason:=pg_catalog.upper(pg_catalog.btrim(coalesce(p_request->>'reason_code','')));

  if v_result not in ('REJECTED','PARTIAL','CORRUPT','FAILED','CONFLICT')
     or v_reason!~'^[A-Z][A-Z0-9_]{2,99}$' then
    raise exception 'WEEKLY_SOURCE_ATTEMPT_OUTCOME_INVALID' using errcode='22023';
  end if;
  if v_environment not in ('TEST','LIVE') or v_agency is null or v_group is null
     or v_purpose not in ('ORDINARY','FINAL_SOURCE_CORRECTION') then
    raise exception 'WEEKLY_SOURCE_ATTEMPT_SCOPE_INVALID' using errcode='22023';
  end if;
  select * into v_group_row from public.weekly_source_groups where id=v_group;
  if not found or not v_group_row.active
     or v_group_row.environment is distinct from v_environment
     or v_group_row.agency_id is distinct from v_agency then
    raise exception 'WEEKLY_SOURCE_ATTEMPT_SCOPE_INVALID' using errcode='22023';
  end if;
  if v_cycle is not null then
    select * into v_cycle_row from public.weekly_source_cycles where id=v_cycle;
    if not found or v_cycle_row.source_group_id is distinct from v_group then
      raise exception 'WEEKLY_SOURCE_ATTEMPT_SCOPE_INVALID' using errcode='22023';
    end if;
  end if;
  if v_scope is not null then
    select * into v_scope_row from public.weekly_source_report_scopes where id=v_scope;
    if not found or v_cycle is null
       or v_scope_row.source_cycle_id is distinct from v_cycle
       or v_scope_row.source_group_id is distinct from v_group
       or v_scope_row.environment is distinct from v_environment
       or v_scope_row.agency_id is distinct from v_agency
       or (v_client is not null and v_scope_row.client_id is distinct from v_client) then
      raise exception 'WEEKLY_SOURCE_ATTEMPT_SCOPE_INVALID' using errcode='22023';
    end if;
    v_client:=v_scope_row.client_id;
  end if;
  if v_declared is not null and v_cycle is not null
     and v_declared is distinct from private.weekly_source_scope_fingerprint_v1(
       v_environment,v_agency,v_group,v_cycle,v_scope,v_client
     ) then
    raise exception 'WEEKLY_SOURCE_ATTEMPT_SCOPE_FINGERPRINT_MISMATCH' using errcode='22023';
  end if;
  if v_logical_upload_id is not null then
    select * into v_logical_upload
    from public.weekly_source_uploads where id=v_logical_upload_id;
    if not found or v_cycle is null
       or v_logical_upload.source_cycle_id is distinct from v_cycle
       or v_logical_upload.report_scope_id is distinct from v_scope then
      raise exception 'WEEKLY_SOURCE_ATTEMPT_LOGICAL_UPLOAD_MISMATCH' using errcode='22023';
    end if;
  end if;
  if (v_purpose='FINAL_SOURCE_CORRECTION') is distinct from (v_correction is not null) then
    raise exception 'WEEKLY_SOURCE_ATTEMPT_CORRECTION_SCOPE_INVALID' using errcode='22023';
  end if;
  if v_correction is not null then
    select * into v_correction_row
    from public.weekly_final_source_correction_sessions
    where id=v_correction;
    if not found or v_cycle is null
       or v_correction_row.source_cycle_id is distinct from v_cycle
       or v_correction_row.authority_scope_kind is distinct from
          (case when v_scope is null then 'CYCLE' else 'NHSP_REPORT_SCOPE' end)
       or v_correction_row.report_scope_id is distinct from v_scope
       or v_correction_row.actor_user_id is distinct from v_actor then
      raise exception 'WEEKLY_SOURCE_ATTEMPT_CORRECTION_SCOPE_INVALID' using errcode='22023';
    end if;
  end if;
  if v_logical_upload_id is not null
     and (v_logical_upload.purpose is distinct from v_purpose
          or v_logical_upload.correction_session_id is distinct from v_correction) then
    raise exception 'WEEKLY_SOURCE_ATTEMPT_LOGICAL_UPLOAD_MISMATCH' using errcode='22023';
  end if;
  perform private.weekly_source_office_authority_v1(
    v_actor,'UPLOAD_SOURCE',v_group,v_client,
    coalesce(v_cycle_row.finalisation_week_ending,current_date)
  );
  if p_request->>'original_filename' is not null
     and pg_catalog.char_length(p_request->>'original_filename')>255 then
    raise exception 'WEEKLY_SOURCE_FILENAME_INVALID' using errcode='22023';
  end if;

  v_attempt:=private.weekly_source_attempt_append_without_upload_v1(
    v_environment,v_agency,v_group,v_cycle,v_scope,v_purpose,v_declared,v_correction,
    v_actor,p_request->>'original_filename',nullif(p_request->>'byte_count','')::bigint,
    v_content,p_request->>'parser_version',p_request->>'profile_version',
    p_request->>'normaliser_version',v_result,v_reason,
    v_logical_upload_id
  );
  return pg_catalog.jsonb_build_object(
    'ok',true,'status',v_result,'reason_code',v_reason,'attempt_id',v_attempt
  );
end;
$function$;

create or replace function public.weekly_source_upload_stage_rows_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_upload_id uuid;
  v_upload public.weekly_source_uploads%rowtype;
  v_group_id uuid;
  v_cycle_date date;
  v_client_id uuid;
  v_item jsonb;
  v_key text;
  v_ordinal integer;
  v_hash bytea;
  v_existing_physical public.weekly_source_physical_rows%rowtype;
  v_existing_row public.weekly_source_upload_rows%rowtype;
  v_existing_money public.weekly_source_money_cell_evidence%rowtype;
  v_existing_expense public.weekly_source_expense_cell_evidence%rowtype;
  v_row public.weekly_source_upload_rows%rowtype;
  v_money public.weekly_source_money_cell_evidence%rowtype;
  v_expense public.weekly_source_expense_cell_evidence%rowtype;
  v_payload jsonb;
  v_physical_added integer:=0;
  v_rows_added integer:=0;
  v_money_added integer:=0;
  v_expenses_added integer:=0;
begin
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(
       select 1 from pg_catalog.jsonb_object_keys(p_request) key
       where key not in ('actor_user_id','upload_id','physical_rows','normalised_rows',
         'money_evidence','expense_evidence')
     ) then
    raise exception 'WEEKLY_SOURCE_STAGE_ROWS_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_upload_id:=(p_request->>'upload_id')::uuid;
  exception when invalid_text_representation then
    raise exception 'WEEKLY_SOURCE_STAGE_ROWS_REQUEST_INVALID' using errcode='22023';
  end;
  if v_actor is null or v_upload_id is null
     or pg_catalog.jsonb_typeof(coalesce(p_request->'physical_rows','[]'::jsonb))<>'array'
     or pg_catalog.jsonb_typeof(coalesce(p_request->'normalised_rows','[]'::jsonb))<>'array'
     or pg_catalog.jsonb_typeof(coalesce(p_request->'money_evidence','[]'::jsonb))<>'array'
     or pg_catalog.jsonb_typeof(coalesce(p_request->'expense_evidence','[]'::jsonb))<>'array' then
    raise exception 'WEEKLY_SOURCE_STAGE_ROWS_REQUEST_INVALID' using errcode='22023';
  end if;
  if pg_catalog.jsonb_array_length(coalesce(p_request->'physical_rows','[]'::jsonb))>10000
     or pg_catalog.jsonb_array_length(coalesce(p_request->'normalised_rows','[]'::jsonb))>10000
     or pg_catalog.jsonb_array_length(coalesce(p_request->'money_evidence','[]'::jsonb))>40001
     or pg_catalog.jsonb_array_length(coalesce(p_request->'expense_evidence','[]'::jsonb))>10000 then
    raise exception 'WEEKLY_SOURCE_STAGE_ROWS_BATCH_TOO_LARGE' using errcode='22023';
  end if;

  select * into v_upload
  from public.weekly_source_uploads where id=v_upload_id for update;
  if not found or v_upload.state<>'STAGING' then
    raise exception 'WEEKLY_SOURCE_UPLOAD_NOT_STAGING' using errcode='55000';
  end if;
  if v_upload.uploaded_by_user_id is distinct from v_actor then
    raise exception 'WEEKLY_SOURCE_UPLOAD_ACTOR_MISMATCH' using errcode='42501';
  end if;
  select cycle.source_group_id,cycle.finalisation_week_ending
    into strict v_group_id,v_cycle_date
  from public.weekly_source_cycles cycle where cycle.id=v_upload.source_cycle_id;
  if v_upload.report_scope_id is null then
    begin
      v_client_id:=nullif(v_upload.file_metadata_json->>'client_id','')::uuid;
    exception when invalid_text_representation then
      raise exception 'WEEKLY_SOURCE_CLIENT_SCOPE_INVALID' using errcode='55000';
    end;
  else
    select scope.client_id into strict v_client_id
    from public.weekly_source_report_scopes scope
    where scope.id=v_upload.report_scope_id
      and scope.source_cycle_id=v_upload.source_cycle_id;
  end if;
  perform private.weekly_source_office_authority_v1(
    v_actor,'UPLOAD_SOURCE',v_group_id,v_client_id,v_cycle_date
  );

  for v_item in
    select value from pg_catalog.jsonb_array_elements(
      coalesce(p_request->'physical_rows','[]'::jsonb)
    )
  loop
    if pg_catalog.jsonb_typeof(v_item)<>'object' then
      raise exception 'WEEKLY_SOURCE_PHYSICAL_ROW_INVALID' using errcode='22023';
    end if;
    select key into v_key from pg_catalog.jsonb_object_keys(v_item) key
    where key not in ('source_row_ordinal','bounded_raw_cells_json','classification','row_sha256')
    order by key limit 1;
    if v_key is not null then
      raise exception 'WEEKLY_SOURCE_PHYSICAL_ROW_UNKNOWN_FIELD'
        using errcode='22023',detail=v_key;
    end if;
    begin
      v_ordinal:=(v_item->>'source_row_ordinal')::integer;
    exception when invalid_text_representation or numeric_value_out_of_range then
      raise exception 'WEEKLY_SOURCE_PHYSICAL_ROW_ORDINAL_INVALID' using errcode='22023';
    end;
    if v_ordinal is null or v_ordinal<1 or v_ordinal>v_upload.physical_row_count
       or pg_catalog.jsonb_typeof(v_item->'bounded_raw_cells_json')<>'object'
       or coalesce(v_item->>'classification','') not in (
         'HEADER','TRAILER','PROFILE_PROVED_NON_ECONOMIC_CONTINUATION',
         'ACCEPTED_SHIFT','BLOCKING_ECONOMIC_DUPLICATE','BLOCKING_MALFORMED'
       ) then
      raise exception 'WEEKLY_SOURCE_PHYSICAL_ROW_INVALID' using errcode='22023';
    end if;
    v_payload:=pg_catalog.jsonb_build_object(
      'source_row_ordinal',v_ordinal,
      'classification',v_item->>'classification',
      'bounded_raw_cells_json',v_item->'bounded_raw_cells_json'
    );
    v_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_PHYSICAL_ROW_V1',v_payload);
    if nullif(v_item->>'row_sha256','') is not null
       and private.weekly_source_hex32_v1(
         v_item->>'row_sha256','WEEKLY_SOURCE_PHYSICAL_ROW_HASH_INVALID'
       ) is distinct from v_hash then
      raise exception 'WEEKLY_SOURCE_PHYSICAL_ROW_HASH_MISMATCH' using errcode='22023';
    end if;
    select * into v_existing_physical
    from public.weekly_source_physical_rows
    where upload_id=v_upload_id and source_row_ordinal=v_ordinal;
    if found then
      if v_existing_physical.classification is distinct from v_item->>'classification'
         or v_existing_physical.bounded_raw_cells_json is distinct from v_item->'bounded_raw_cells_json'
         or v_existing_physical.row_sha256 is distinct from v_hash then
        raise exception 'WEEKLY_SOURCE_PHYSICAL_ROW_REPLAY_CONFLICT' using errcode='55000';
      end if;
    else
      insert into public.weekly_source_physical_rows(
        upload_id,source_row_ordinal,bounded_raw_cells_json,row_sha256,classification
      ) values (
        v_upload_id,v_ordinal,v_item->'bounded_raw_cells_json',v_hash,v_item->>'classification'
      );
      v_physical_added:=v_physical_added+1;
    end if;
  end loop;

  for v_item in
    select value from pg_catalog.jsonb_array_elements(
      coalesce(p_request->'normalised_rows','[]'::jsonb)
    )
  loop
    if pg_catalog.jsonb_typeof(v_item)<>'object' then
      raise exception 'WEEKLY_SOURCE_NORMALISED_ROW_INVALID' using errcode='22023';
    end if;
    select key into v_key from pg_catalog.jsonb_object_keys(v_item) key
    where key not in (
      'source_row_ordinal','external_source_key','source_candidate_identity',
      'source_client_identity','work_date','start_at_local','end_at_local',
      'break_minutes','actual_net_minutes','row_finalisation_state','finalised_by',
      'role_band_source','source_commission_pence','source_total_cost_pence',
      'source_shift_charge_pence','source_money_parse_state',
      'source_qualification_profile_version','source_expense_pence',
      'source_expense_parse_state','bounded_raw_columns_json','normalised_row_hash'
    ) order by key limit 1;
    if v_key is not null then
      raise exception 'WEEKLY_SOURCE_NORMALISED_ROW_UNKNOWN_FIELD'
        using errcode='22023',detail=v_key;
    end if;
    begin
      v_row:=pg_catalog.jsonb_populate_record(null::public.weekly_source_upload_rows,v_item);
    exception when others then
      raise exception 'WEEKLY_SOURCE_NORMALISED_ROW_TYPE_INVALID' using errcode='22023';
    end;
    v_row.row_finalisation_state:=coalesce(v_row.row_finalisation_state,'NOT_APPLICABLE');
    v_row.source_money_parse_state:=coalesce(v_row.source_money_parse_state,'NOT_APPLICABLE');
    v_row.source_expense_parse_state:=coalesce(v_row.source_expense_parse_state,'NOT_APPLICABLE');
    v_row.bounded_raw_columns_json:=coalesce(v_row.bounded_raw_columns_json,'{}'::jsonb);
    if v_row.source_row_ordinal is null or v_row.source_row_ordinal<1
       or v_row.source_row_ordinal>v_upload.physical_row_count
       or nullif(pg_catalog.btrim(coalesce(v_row.source_candidate_identity,'')),'') is null
       or nullif(pg_catalog.btrim(coalesce(v_row.source_client_identity,'')),'') is null
       or v_row.work_date is null
       or pg_catalog.jsonb_typeof(v_row.bounded_raw_columns_json)<>'object'
       or not exists(
         select 1 from public.weekly_source_physical_rows physical
         where physical.upload_id=v_upload_id
           and physical.source_row_ordinal=v_row.source_row_ordinal
           and physical.classification='ACCEPTED_SHIFT'
       ) then
      raise exception 'WEEKLY_SOURCE_NORMALISED_ROW_INVALID' using errcode='22023';
    end if;
    v_payload:=pg_catalog.jsonb_build_object(
      'source_row_ordinal',v_row.source_row_ordinal,
      'external_source_key',v_row.external_source_key,
      'source_candidate_identity',v_row.source_candidate_identity,
      'source_client_identity',v_row.source_client_identity,
      'work_date',v_row.work_date,
      'start_at_local',v_row.start_at_local,
      'end_at_local',v_row.end_at_local,
      'break_minutes',v_row.break_minutes,
      'actual_net_minutes',v_row.actual_net_minutes,
      'row_finalisation_state',v_row.row_finalisation_state,
      'finalised_by',v_row.finalised_by,
      'role_band_source',v_row.role_band_source,
      'source_commission_pence',v_row.source_commission_pence,
      'source_total_cost_pence',v_row.source_total_cost_pence,
      'source_shift_charge_pence',v_row.source_shift_charge_pence,
      'source_money_parse_state',v_row.source_money_parse_state,
      'source_qualification_profile_version',v_row.source_qualification_profile_version,
      'source_expense_pence',v_row.source_expense_pence,
      'source_expense_parse_state',v_row.source_expense_parse_state,
      'bounded_raw_columns_json',v_row.bounded_raw_columns_json
    );
    v_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_NORMALISED_ROW_V1',v_payload);
    if nullif(v_item->>'normalised_row_hash','') is not null
       and private.weekly_source_hex32_v1(
         v_item->>'normalised_row_hash','WEEKLY_SOURCE_NORMALISED_ROW_HASH_INVALID'
       ) is distinct from v_hash then
      raise exception 'WEEKLY_SOURCE_NORMALISED_ROW_HASH_MISMATCH' using errcode='22023';
    end if;
    select * into v_existing_row
    from public.weekly_source_upload_rows
    where upload_id=v_upload_id and source_row_ordinal=v_row.source_row_ordinal;
    if found then
      if v_existing_row.normalised_row_hash is distinct from v_hash then
        raise exception 'WEEKLY_SOURCE_NORMALISED_ROW_REPLAY_CONFLICT' using errcode='55000';
      end if;
    else
      -- Two accepted economic rows of one upload may not share an external
      -- source key.  For HealthRoster the key is the `Request Id`, which the
      -- profile uses as durable work identity, so a repeat is a duplicate
      -- economic row (14 s4.2.8).  For NHSP the broker stages the Reference
      -- Number, which 24 s9 says is evidence and never identity; a report that
      -- repeats one is a source shape this build cannot account for, and 14
      -- s4.2.8 blocks it.
      --
      -- Either way the refusal must be STATED.  Until this guard existed the
      -- only thing that stopped it was the unique index
      -- weekly_source_upload_rows_external_key_uq, which reached the caller as
      -- a bare 23505 from a constraint name - an unhandled error, not a
      -- refusal, and the exact shape a positive and its same-Reference full
      -- negative produce.  A unique index must never be the statement of a
      -- rule (standing rule 5); it stays as the backstop behind this one.
      if v_row.external_source_key is not null
         and exists(
           select 1
           from public.weekly_source_upload_rows existing_key
           where existing_key.upload_id=v_upload_id
             and existing_key.external_source_key=v_row.external_source_key
             and existing_key.source_row_ordinal is distinct from v_row.source_row_ordinal
         ) then
        raise exception 'WEEKLY_SOURCE_UPLOAD_DUPLICATE_EXTERNAL_KEY'
          using errcode='55000',
                detail=pg_catalog.jsonb_build_object(
                  'upload_id',v_upload_id,
                  'source_row_ordinal',v_row.source_row_ordinal,
                  'external_source_key',v_row.external_source_key
                )::text;
      end if;
      insert into public.weekly_source_upload_rows(
        upload_id,source_row_ordinal,external_source_key,source_candidate_identity,
        source_client_identity,work_date,start_at_local,end_at_local,break_minutes,
        actual_net_minutes,row_finalisation_state,finalised_by,role_band_source,
        source_commission_pence,source_total_cost_pence,source_shift_charge_pence,
        source_money_parse_state,source_qualification_profile_version,
        source_expense_pence,source_expense_parse_state,normalised_row_hash,
        bounded_raw_columns_json
      ) values (
        v_upload_id,v_row.source_row_ordinal,v_row.external_source_key,
        v_row.source_candidate_identity,v_row.source_client_identity,v_row.work_date,
        v_row.start_at_local,v_row.end_at_local,v_row.break_minutes,v_row.actual_net_minutes,
        v_row.row_finalisation_state,v_row.finalised_by,v_row.role_band_source,
        v_row.source_commission_pence,v_row.source_total_cost_pence,
        v_row.source_shift_charge_pence,v_row.source_money_parse_state,
        v_row.source_qualification_profile_version,v_row.source_expense_pence,
        v_row.source_expense_parse_state,v_hash,v_row.bounded_raw_columns_json
      );
      v_rows_added:=v_rows_added+1;
    end if;
  end loop;

  for v_item in
    select value from pg_catalog.jsonb_array_elements(
      coalesce(p_request->'money_evidence','[]'::jsonb)
    )
  loop
    if pg_catalog.jsonb_typeof(v_item)<>'object' then
      raise exception 'WEEKLY_SOURCE_MONEY_EVIDENCE_INVALID' using errcode='22023';
    end if;
    select key into v_key from pg_catalog.jsonb_object_keys(v_item) key
    where key not in (
      'source_row_ordinal','money_field_kind','source_column_index','cell_coordinate',
      'source_kind','original_token','decoded_token','cell_type_marker',
      'formula_present','parse_state','parsed_pence','source_file_sha256','token_sha256'
    ) order by key limit 1;
    if v_key is not null then
      raise exception 'WEEKLY_SOURCE_MONEY_EVIDENCE_UNKNOWN_FIELD'
        using errcode='22023',detail=v_key;
    end if;
    begin
      v_money:=pg_catalog.jsonb_populate_record(null::public.weekly_source_money_cell_evidence,v_item);
    exception when others then
      raise exception 'WEEKLY_SOURCE_MONEY_EVIDENCE_TYPE_INVALID' using errcode='22023';
    end;
    v_money.formula_present:=coalesce(v_money.formula_present,false);
    if v_money.source_row_ordinal is null or v_money.source_row_ordinal<1
       or v_money.source_row_ordinal>v_upload.physical_row_count
       or v_money.money_field_kind not in ('COMMISSION','TOTAL_COST','FMC','BOTTOM_TOTAL_COST')
       or v_money.source_column_index is null or v_money.source_column_index<0
       or v_money.source_kind not in (
         'XLSX_NUMERIC_TOKEN','XLSX_STRING_TOKEN','HTML_DECODED_TEXT','CSV_DECODED_TEXT'
       )
       or v_money.original_token is null or v_money.decoded_token is null
       or v_money.parse_state not in (
         'VALID','MISSING','INVALID','EXCESS_PRECISION','FORMULA',
         'UNSUPPORTED_CELL_TYPE','OVERFLOW'
       ) then
      raise exception 'WEEKLY_SOURCE_MONEY_EVIDENCE_INVALID' using errcode='22023';
    end if;
    if nullif(v_item->>'source_file_sha256','') is not null
       and private.weekly_source_hex32_v1(
         v_item->>'source_file_sha256','WEEKLY_SOURCE_EVIDENCE_FILE_HASH_INVALID'
       ) is distinct from v_upload.content_sha256 then
      raise exception 'WEEKLY_SOURCE_EVIDENCE_FILE_HASH_MISMATCH' using errcode='22023';
    end if;
    v_payload:=pg_catalog.jsonb_build_object(
      'source_row_ordinal',v_money.source_row_ordinal,
      'money_field_kind',v_money.money_field_kind,
      'source_column_index',v_money.source_column_index,
      'cell_coordinate',v_money.cell_coordinate,
      'source_kind',v_money.source_kind,
      'original_token',v_money.original_token,
      'decoded_token',v_money.decoded_token,
      'cell_type_marker',v_money.cell_type_marker,
      'formula_present',v_money.formula_present,
      'parse_state',v_money.parse_state,
      'parsed_pence',v_money.parsed_pence
    );
    v_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_MONEY_TOKEN_V1',v_payload);
    if nullif(v_item->>'token_sha256','') is not null
       and private.weekly_source_hex32_v1(
         v_item->>'token_sha256','WEEKLY_SOURCE_MONEY_TOKEN_HASH_INVALID'
       ) is distinct from v_hash then
      raise exception 'WEEKLY_SOURCE_MONEY_TOKEN_HASH_MISMATCH' using errcode='22023';
    end if;
    select * into v_existing_money
    from public.weekly_source_money_cell_evidence
    where upload_id=v_upload_id and source_row_ordinal=v_money.source_row_ordinal
      and money_field_kind=v_money.money_field_kind;
    if found then
      if v_existing_money.token_sha256 is distinct from v_hash
         or v_existing_money.source_file_sha256 is distinct from v_upload.content_sha256 then
        raise exception 'WEEKLY_SOURCE_MONEY_EVIDENCE_REPLAY_CONFLICT' using errcode='55000';
      end if;
    else
      insert into public.weekly_source_money_cell_evidence(
        upload_id,source_row_ordinal,money_field_kind,source_column_index,
        cell_coordinate,source_kind,original_token,decoded_token,cell_type_marker,
        formula_present,parse_state,parsed_pence,source_file_sha256,token_sha256
      ) values (
        v_upload_id,v_money.source_row_ordinal,v_money.money_field_kind,
        v_money.source_column_index,v_money.cell_coordinate,v_money.source_kind,
        v_money.original_token,v_money.decoded_token,v_money.cell_type_marker,
        v_money.formula_present,v_money.parse_state,v_money.parsed_pence,
        v_upload.content_sha256,v_hash
      );
      v_money_added:=v_money_added+1;
    end if;
  end loop;

  for v_item in
    select value from pg_catalog.jsonb_array_elements(
      coalesce(p_request->'expense_evidence','[]'::jsonb)
    )
  loop
    if pg_catalog.jsonb_typeof(v_item)<>'object' then
      raise exception 'WEEKLY_SOURCE_EXPENSE_EVIDENCE_INVALID' using errcode='22023';
    end if;
    select key into v_key from pg_catalog.jsonb_object_keys(v_item) key
    where key not in (
      'source_row_ordinal','source_column_index','cell_coordinate','source_kind',
      'original_token','decoded_token','cell_type_marker','formula_present',
      'lexical_profile_version','parse_state','parsed_pence',
      'source_file_sha256','token_sha256'
    ) order by key limit 1;
    if v_key is not null then
      raise exception 'WEEKLY_SOURCE_EXPENSE_EVIDENCE_UNKNOWN_FIELD'
        using errcode='22023',detail=v_key;
    end if;
    begin
      v_expense:=pg_catalog.jsonb_populate_record(null::public.weekly_source_expense_cell_evidence,v_item);
    exception when others then
      raise exception 'WEEKLY_SOURCE_EXPENSE_EVIDENCE_TYPE_INVALID' using errcode='22023';
    end;
    v_expense.formula_present:=coalesce(v_expense.formula_present,false);
    v_expense.lexical_profile_version:=coalesce(
      v_expense.lexical_profile_version,'SOURCE_FIXED_EXPENSE_PENCE_V1'
    );
    if v_expense.source_row_ordinal is null or v_expense.source_row_ordinal<1
       or v_expense.source_row_ordinal>v_upload.physical_row_count
       or v_expense.source_column_index is null or v_expense.source_column_index<0
       or v_expense.source_kind not in (
         'XLSX_NUMERIC_TOKEN','XLSX_STRING_TOKEN','HTML_DECODED_TEXT','CSV_DECODED_TEXT'
       )
       or v_expense.original_token is null or v_expense.decoded_token is null
       or v_expense.lexical_profile_version<>'SOURCE_FIXED_EXPENSE_PENCE_V1'
       or v_expense.parse_state not in (
         'VALID','OMITTED_ZERO','INVALID','EXCESS_PRECISION','FORMULA',
         'UNSUPPORTED_CELL_TYPE','OVERFLOW'
       ) then
      raise exception 'WEEKLY_SOURCE_EXPENSE_EVIDENCE_INVALID' using errcode='22023';
    end if;
    if nullif(v_item->>'source_file_sha256','') is not null
       and private.weekly_source_hex32_v1(
         v_item->>'source_file_sha256','WEEKLY_SOURCE_EVIDENCE_FILE_HASH_INVALID'
       ) is distinct from v_upload.content_sha256 then
      raise exception 'WEEKLY_SOURCE_EVIDENCE_FILE_HASH_MISMATCH' using errcode='22023';
    end if;
    v_payload:=pg_catalog.jsonb_build_object(
      'source_row_ordinal',v_expense.source_row_ordinal,
      'source_column_index',v_expense.source_column_index,
      'cell_coordinate',v_expense.cell_coordinate,
      'source_kind',v_expense.source_kind,
      'original_token',v_expense.original_token,
      'decoded_token',v_expense.decoded_token,
      'cell_type_marker',v_expense.cell_type_marker,
      'formula_present',v_expense.formula_present,
      'lexical_profile_version',v_expense.lexical_profile_version,
      'parse_state',v_expense.parse_state,
      'parsed_pence',v_expense.parsed_pence
    );
    v_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_EXPENSE_TOKEN_V1',v_payload);
    if nullif(v_item->>'token_sha256','') is not null
       and private.weekly_source_hex32_v1(
         v_item->>'token_sha256','WEEKLY_SOURCE_EXPENSE_TOKEN_HASH_INVALID'
       ) is distinct from v_hash then
      raise exception 'WEEKLY_SOURCE_EXPENSE_TOKEN_HASH_MISMATCH' using errcode='22023';
    end if;
    select * into v_existing_expense
    from public.weekly_source_expense_cell_evidence
    where upload_id=v_upload_id and source_row_ordinal=v_expense.source_row_ordinal;
    if found then
      if v_existing_expense.token_sha256 is distinct from v_hash
         or v_existing_expense.source_file_sha256 is distinct from v_upload.content_sha256 then
        raise exception 'WEEKLY_SOURCE_EXPENSE_EVIDENCE_REPLAY_CONFLICT' using errcode='55000';
      end if;
    else
      insert into public.weekly_source_expense_cell_evidence(
        upload_id,source_row_ordinal,source_column_index,cell_coordinate,source_kind,
        original_token,decoded_token,cell_type_marker,formula_present,
        lexical_profile_version,parse_state,parsed_pence,source_file_sha256,token_sha256
      ) values (
        v_upload_id,v_expense.source_row_ordinal,v_expense.source_column_index,
        v_expense.cell_coordinate,v_expense.source_kind,v_expense.original_token,
        v_expense.decoded_token,v_expense.cell_type_marker,v_expense.formula_present,
        v_expense.lexical_profile_version,v_expense.parse_state,v_expense.parsed_pence,
        v_upload.content_sha256,v_hash
      );
      v_expenses_added:=v_expenses_added+1;
    end if;
  end loop;

  return pg_catalog.jsonb_build_object(
    'ok',true,'status','STAGING','upload_id',v_upload_id,
    'inserted',pg_catalog.jsonb_build_object(
      'physical_rows',v_physical_added,'normalised_rows',v_rows_added,
      'money_evidence',v_money_added,'expense_evidence',v_expenses_added
    ),
    'stored',pg_catalog.jsonb_build_object(
      'physical_rows',(select count(*) from public.weekly_source_physical_rows where upload_id=v_upload_id),
      'normalised_rows',(select count(*) from public.weekly_source_upload_rows where upload_id=v_upload_id),
      'money_evidence',(select count(*) from public.weekly_source_money_cell_evidence where upload_id=v_upload_id),
      'expense_evidence',(select count(*) from public.weekly_source_expense_cell_evidence where upload_id=v_upload_id)
    )
  );
end;
$function$;

create or replace function private.weekly_source_upload_seal_core_v1(
  p_upload_id uuid,
  p_actor_user_id uuid
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_upload public.weekly_source_uploads%rowtype;
  v_profile public.weekly_source_format_profiles%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_scope public.weekly_source_report_scopes%rowtype;
  v_correction public.weekly_final_source_correction_sessions%rowtype;
  v_prior_upload public.weekly_source_uploads%rowtype;
  v_client_id uuid;
  v_expected_scope bytea;
  v_manifest_hash bytea;
  v_version_before bigint;
  v_version_after bigint;
  v_attempt_id uuid;
  v_actual_physical integer;
  v_actual_header integer;
  v_actual_trailer integer;
  v_actual_continuation integer;
  v_actual_accepted integer;
  v_actual_duplicate integer;
  v_actual_malformed integer;
  v_actual_blocked integer;
  v_normalised_count integer;
  v_money_count integer;
  v_expense_count integer;
  v_actual_coverage_start date;
  v_actual_coverage_end date;
begin
  if p_upload_id is null or p_actor_user_id is null then
    raise exception 'WEEKLY_SOURCE_SEAL_INPUT_INVALID' using errcode='22023';
  end if;
  select * into v_upload
  from public.weekly_source_uploads where id=p_upload_id;
  if not found then
    raise exception 'WEEKLY_SOURCE_UPLOAD_NOT_FOUND' using errcode='22023';
  end if;
  if v_upload.uploaded_by_user_id is distinct from p_actor_user_id then
    raise exception 'WEEKLY_SOURCE_UPLOAD_ACTOR_MISMATCH' using errcode='42501';
  end if;
  if v_upload.state in ('CURRENT','SUPERSEDED','CORRECTION_READY') then
    return pg_catalog.jsonb_build_object(
      'ok',true,'status',v_upload.state,'logical_upload_id',v_upload.id,
      'row_manifest_hash',pg_catalog.encode(v_upload.row_manifest_hash,'hex'),
      'current_pointer_moved',false,'idempotent',true
    );
  end if;
  if v_upload.state<>'STAGING' then
    raise exception 'WEEKLY_SOURCE_UPLOAD_NOT_STAGING' using errcode='55000';
  end if;

  select * into strict v_cycle
  from public.weekly_source_cycles where id=v_upload.source_cycle_id;
  select * into strict v_group
  from public.weekly_source_groups where id=v_cycle.source_group_id;
  select * into strict v_profile
  from public.weekly_source_format_profiles where id=v_upload.source_format_profile_id;
  if not v_group.active or not v_profile.active
     or v_profile.profile_code not in (
       'NHSP_PREFINAL_RELEASED_V1','NHSP_FINAL_BACKING_V1',
       'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1',
       'HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1',
       'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1'
     ) or v_profile.version<>1 then
    raise exception 'WEEKLY_SOURCE_RELEASE_PROFILE_STALE' using errcode='55000';
  end if;
  if v_upload.header_coordinate_map_hash is distinct from
     private.weekly_source_sha256_jsonb_v1(
       'WEEKLY_SOURCE_HEADER_COORDINATE_MAP_V1',v_upload.header_coordinate_map_json
     ) then
    raise exception 'WEEKLY_SOURCE_HEADER_MAP_HASH_MISMATCH' using errcode='55000';
  end if;

  if v_upload.report_scope_id is not null then
    if v_profile.profile_code<>'NHSP_FINAL_BACKING_V1' then
      raise exception 'WEEKLY_SOURCE_REPORT_SCOPE_PROFILE_INVALID' using errcode='55000';
    end if;
    select * into strict v_scope
    from public.weekly_source_report_scopes
    where id=v_upload.report_scope_id;
    if v_scope.source_cycle_id is distinct from v_cycle.id
       or v_scope.source_group_id is distinct from v_group.id
       or v_scope.environment is distinct from v_group.environment
       or v_scope.agency_id is distinct from v_group.agency_id
       or v_scope.cutoff_at_utc is distinct from v_cycle.cutoff_at_utc then
      raise exception 'WEEKLY_SOURCE_REPORT_SCOPE_MISMATCH' using errcode='55000';
    end if;
    v_client_id:=v_scope.client_id;
  else
    begin
      v_client_id:=nullif(v_upload.file_metadata_json->>'client_id','')::uuid;
    exception when invalid_text_representation then
      raise exception 'WEEKLY_SOURCE_CLIENT_SCOPE_INVALID' using errcode='55000';
    end;
  end if;
  v_expected_scope:=private.weekly_source_scope_fingerprint_v1(
    v_group.environment,v_group.agency_id,v_group.id,v_cycle.id,
    v_upload.report_scope_id,v_client_id
  );
  if v_upload.declared_scope_fingerprint is distinct from v_expected_scope then
    raise exception 'WEEKLY_SOURCE_DECLARED_SCOPE_STALE' using errcode='55000';
  end if;
  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended(pg_catalog.encode(v_expected_scope,'hex'),73241837)
  );
  select * into strict v_upload
  from public.weekly_source_uploads where id=p_upload_id for update;
  if v_upload.uploaded_by_user_id is distinct from p_actor_user_id then
    raise exception 'WEEKLY_SOURCE_UPLOAD_ACTOR_MISMATCH' using errcode='42501';
  end if;
  if v_upload.state in ('CURRENT','SUPERSEDED','CORRECTION_READY') then
    return pg_catalog.jsonb_build_object(
      'ok',true,'status',v_upload.state,'logical_upload_id',v_upload.id,
      'row_manifest_hash',pg_catalog.encode(v_upload.row_manifest_hash,'hex'),
      'current_pointer_moved',false,'idempotent',true
    );
  end if;
  if v_upload.state<>'STAGING'
     or v_upload.declared_scope_fingerprint is distinct from v_expected_scope then
    raise exception 'WEEKLY_SOURCE_UPLOAD_NOT_STAGING' using errcode='55000';
  end if;
  if v_upload.report_scope_id is null then
    select * into strict v_cycle
    from public.weekly_source_cycles where id=v_upload.source_cycle_id for update;
  else
    -- NHSP authority is per Trust.  Do not serialize independent Trusts by
    -- taking the shared cycle row lock; the exact report-scope row is its head.
    select * into strict v_cycle
    from public.weekly_source_cycles where id=v_upload.source_cycle_id;
    select * into strict v_scope
    from public.weekly_source_report_scopes where id=v_upload.report_scope_id for update;
    if v_scope.source_cycle_id is distinct from v_cycle.id
       or v_scope.source_group_id is distinct from v_group.id
       or v_scope.environment is distinct from v_group.environment
       or v_scope.agency_id is distinct from v_group.agency_id
       or v_scope.client_id is distinct from v_client_id
       or v_scope.cutoff_at_utc is distinct from v_cycle.cutoff_at_utc then
      raise exception 'WEEKLY_SOURCE_REPORT_SCOPE_CHANGED' using errcode='55000';
    end if;
  end if;
  if private.weekly_source_scope_fingerprint_v1(
       v_group.environment,v_group.agency_id,v_group.id,v_cycle.id,
       v_upload.report_scope_id,v_client_id
     ) is distinct from v_expected_scope then
    raise exception 'WEEKLY_SOURCE_SCOPE_CHANGED' using errcode='55000';
  end if;
  perform private.weekly_source_office_authority_v1(
    p_actor_user_id,'SEAL_SOURCE',v_group.id,v_client_id,v_cycle.finalisation_week_ending
  );

  select
    count(*)::integer,
    count(*) filter(where classification='HEADER')::integer,
    count(*) filter(where classification='TRAILER')::integer,
    count(*) filter(where classification='PROFILE_PROVED_NON_ECONOMIC_CONTINUATION')::integer,
    count(*) filter(where classification='ACCEPTED_SHIFT')::integer,
    count(*) filter(where classification='BLOCKING_ECONOMIC_DUPLICATE')::integer,
    count(*) filter(where classification='BLOCKING_MALFORMED')::integer
  into v_actual_physical,v_actual_header,v_actual_trailer,v_actual_continuation,
       v_actual_accepted,v_actual_duplicate,v_actual_malformed
  from public.weekly_source_physical_rows where upload_id=v_upload.id;
  select count(*)::integer,
         count(*) filter(where row_finalisation_state in (
           'BLOCK_FINALISATION_DISAGREEMENT','BLOCK_ACTUAL_TUPLE'
         ))::integer
    into v_normalised_count,v_actual_blocked
  from public.weekly_source_upload_rows where upload_id=v_upload.id;
  v_actual_blocked:=v_actual_blocked+v_actual_duplicate+v_actual_malformed;

  if v_actual_physical<>v_upload.physical_row_count
     or v_actual_header<>v_upload.header_count
     or v_actual_trailer<>v_upload.trailer_count
     or v_actual_continuation<>v_upload.continuation_count
     or v_actual_accepted<>v_upload.accepted_count
     or v_actual_duplicate<>v_upload.blocking_economic_duplicate_count
     or v_actual_malformed<>v_upload.malformed_count
     or v_actual_blocked<>v_upload.blocked_count
     or v_actual_physical<>v_actual_header+v_actual_trailer+v_actual_continuation+
       v_actual_accepted+v_actual_duplicate+v_actual_malformed
     or v_normalised_count<>v_actual_accepted then
    raise exception 'WEEKLY_SOURCE_ROW_COUNTS_MISMATCH' using errcode='55000';
  end if;
  if exists(
    select 1 from public.weekly_source_upload_rows normalised
    left join public.weekly_source_physical_rows physical
      on physical.upload_id=normalised.upload_id
     and physical.source_row_ordinal=normalised.source_row_ordinal
    where normalised.upload_id=v_upload.id
      and (physical.id is null or physical.classification<>'ACCEPTED_SHIFT')
  ) or exists(
    select 1 from public.weekly_source_physical_rows physical
    left join public.weekly_source_upload_rows normalised
      on normalised.upload_id=physical.upload_id
     and normalised.source_row_ordinal=physical.source_row_ordinal
    where physical.upload_id=v_upload.id
      and physical.classification='ACCEPTED_SHIFT'
      and normalised.id is null
  ) then
    raise exception 'WEEKLY_SOURCE_ACCEPTED_ROW_EVIDENCE_MISMATCH' using errcode='55000';
  end if;
  if v_actual_duplicate<>0 or v_actual_malformed<>0 or v_actual_blocked<>0 then
    raise exception 'WEEKLY_SOURCE_UPLOAD_HAS_BLOCKERS' using errcode='55000';
  end if;

  -- The sealed row state is profile authority, not a caller-selected label.
  -- A source-zero row is retained as evidence only by the one released
  -- no-finalisation-column profile which proves that zero means source absence.
  if exists(
    select 1
    from public.weekly_source_upload_rows source_row
    where source_row.upload_id=v_upload.id
      and (
        (v_profile.profile_code in ('NHSP_PREFINAL_RELEASED_V1','NHSP_FINAL_BACKING_V1')
          and source_row.row_finalisation_state<>'SOURCE_WORKED')
        or (v_profile.profile_code in (
              'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1',
              'HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1'
            ) and source_row.row_finalisation_state not in ('SOURCE_WORKED','SOURCE_UNFINALISED'))
        or (v_profile.profile_code='ROSTER_WEEKLY_SUMMARY_ACTUAL_V1'
            and source_row.row_finalisation_state not in ('SOURCE_WORKED','SOURCE_ABSENT_ZERO'))
        or (source_row.row_finalisation_state='SOURCE_WORKED' and (
              source_row.start_at_local is null
              or source_row.end_at_local is null
              or source_row.break_minutes is null
              or source_row.actual_net_minutes is null
              or source_row.actual_net_minutes<=0
              or pg_catalog.mod(
                   extract(epoch from (source_row.end_at_local-source_row.start_at_local))::numeric,
                   60::numeric
                 )<>0
              or source_row.actual_net_minutes<>
                 (extract(epoch from (source_row.end_at_local-source_row.start_at_local))/60)::integer
                 - source_row.break_minutes
            ))
        or (source_row.row_finalisation_state='SOURCE_ABSENT_ZERO'
            and source_row.actual_net_minutes is distinct from 0)
      )
  ) then
    raise exception 'WEEKLY_SOURCE_ROW_FINALISATION_STATE_INVALID' using errcode='55000';
  end if;

  -- WP-54.  Two source-row admission rules the pack states and that nothing
  -- enforced.  They are placed HERE, after the state validation immediately
  -- above, because that validation is what guarantees every SOURCE_WORKED row
  -- has a complete, internally consistent Actual tuple; a guard that reads
  -- start and end before that check could silently skip a row whose times were
  -- absent.  Both raise, and public.weekly_source_upload_seal_atomic_v1 turns
  -- the raise into a typed REJECTED attempt with the code as its reason, so a
  -- refused report is refused as a whole -- pack 14 section 4.1.7, "Finalisation
  -- is all or nothing.  A blocker cannot be removed, unticked or ignored".
  --
  -- NHSP-BR-006 (pack 14 section 4.1.3): the confirmed cutoff occurrence may
  -- not predate any row's real Actual finish, overnight finishes included.  The
  -- cutoff handed over is v_cycle.cutoff_at_utc, which for a report scope this
  -- function has already proved equal to v_scope.cutoff_at_utc under the scope
  -- row lock above.
  perform private.weekly_source_cutoff_admission_assert_v1(
    v_upload.id,v_profile.profile_code,v_cycle.cutoff_at_utc
  );
  -- Pack 03 section 7 with pack 14 section 4.2.8: one person cannot work two
  -- intersecting intervals, and two overlapping records are not made valid by
  -- differing references.
  perform private.weekly_source_overlap_admission_assert_v1(v_upload.id);

  if v_actual_accepted=0
     and v_upload.coverage_proof_kind<>'EXPLICIT_EMPTY_CONFIRMATION'
     and coalesce((v_upload.file_metadata_json->>'explicit_empty_attestation')::boolean,false)=false then
    raise exception 'WEEKLY_SOURCE_EMPTY_ATTESTATION_REQUIRED' using errcode='55000';
  end if;

  if v_profile.profile_code='NHSP_FINAL_BACKING_V1' then
    if v_upload.coverage_proof_kind<>'NHSP_TRUST_REPORT_SCOPE'
       or v_upload.confirmed_coverage_start_local_date is not null
       or v_upload.confirmed_coverage_end_local_date is not null
       or private.weekly_source_canonical_report_number_v1(
         v_upload.file_metadata_json->>'nhsp_report_number'
       ) is null
       or v_upload.money_lexical_authority_version<>
          'XLSX_BINARY64_SAME_VALUE_PENCE_V1' then
      raise exception 'WEEKLY_SOURCE_NHSP_FINAL_EVIDENCE_INVALID' using errcode='55000';
    end if;
    select count(*)::integer into v_money_count
    from public.weekly_source_money_cell_evidence where upload_id=v_upload.id;
    if v_money_count not in (v_actual_accepted*3,v_actual_accepted*3+1)
       or (v_money_count=v_actual_accepted*3+1 and not exists(
         select 1 from public.weekly_source_money_cell_evidence money
         join public.weekly_source_physical_rows physical
           on physical.upload_id=money.upload_id
          and physical.source_row_ordinal=money.source_row_ordinal
         where money.upload_id=v_upload.id and money.money_field_kind='BOTTOM_TOTAL_COST'
           and physical.classification='TRAILER'
       ))
       or exists(
         select 1
         from public.weekly_source_upload_rows source_row
         where source_row.upload_id=v_upload.id
           and (
             source_row.source_money_parse_state<>'VALID'
             or source_row.source_commission_pence is null
             or source_row.source_total_cost_pence is null
             or source_row.source_shift_charge_pence is null
             or source_row.source_shift_charge_pence<>
                source_row.source_commission_pence+source_row.source_total_cost_pence
             or sign(source_row.source_commission_pence) not in (0,sign(source_row.source_shift_charge_pence))
             or sign(source_row.source_total_cost_pence) not in (0,sign(source_row.source_shift_charge_pence))
             or (select count(*) from public.weekly_source_money_cell_evidence money
                 where money.upload_id=source_row.upload_id
                   and money.source_row_ordinal=source_row.source_row_ordinal
                    and money.money_field_kind in ('COMMISSION','TOTAL_COST','FMC')
                    and money.parse_state='VALID'
                    and not money.formula_present
                    and money.source_kind in ('XLSX_NUMERIC_TOKEN','XLSX_STRING_TOKEN')
                    and nullif(money.cell_coordinate,'') is not null)<>3
             or (select money.parsed_pence from public.weekly_source_money_cell_evidence money
                 where money.upload_id=source_row.upload_id
                   and money.source_row_ordinal=source_row.source_row_ordinal
                   and money.money_field_kind='COMMISSION')
                is distinct from source_row.source_commission_pence
             or (select money.parsed_pence from public.weekly_source_money_cell_evidence money
                 where money.upload_id=source_row.upload_id
                   and money.source_row_ordinal=source_row.source_row_ordinal
                   and money.money_field_kind='TOTAL_COST')
                is distinct from source_row.source_total_cost_pence
             or (select money.parsed_pence from public.weekly_source_money_cell_evidence money
                 where money.upload_id=source_row.upload_id
                   and money.source_row_ordinal=source_row.source_row_ordinal
                   and money.money_field_kind='FMC') is distinct from 0
           )
       )
       or exists(
          select 1 from public.weekly_source_money_cell_evidence money
          where money.upload_id=v_upload.id
            and (
              money.source_file_sha256 is distinct from v_upload.content_sha256
              or money.source_kind not in ('XLSX_NUMERIC_TOKEN','XLSX_STRING_TOKEN')
              or nullif(money.cell_coordinate,'') is null
              or (money.formula_present is distinct from (money.parse_state='FORMULA'))
              or (money.money_field_kind='BOTTOM_TOTAL_COST'
                  and (money.parse_state<>'VALID' or money.formula_present))
            )
        ) then
      raise exception 'WEEKLY_SOURCE_NHSP_MONEY_EVIDENCE_INVALID' using errcode='55000';
    end if;
  elsif v_profile.profile_code='NHSP_PREFINAL_RELEASED_V1' then
    -- Pre-final NHSP money is checking evidence only.  It must be retained
    -- exactly, but an unparseable component remains a non-blocking pricing
    -- issue for the comparison owner rather than rejecting the hours upload.
    if v_upload.money_lexical_authority_version<>
         'XLSX_BINARY64_SAME_VALUE_PENCE_V1' then
      raise exception 'WEEKLY_SOURCE_NHSP_PREFINAL_EVIDENCE_INVALID' using errcode='55000';
    end if;
    select count(*)::integer into v_money_count
    from public.weekly_source_money_cell_evidence where upload_id=v_upload.id;
    if v_money_count<>v_actual_accepted*2
       or exists(
         select 1
         from public.weekly_source_upload_rows source_row
         where source_row.upload_id=v_upload.id
           and (
             (select count(*) from public.weekly_source_money_cell_evidence money
              where money.upload_id=source_row.upload_id
                and money.source_row_ordinal=source_row.source_row_ordinal
                and money.money_field_kind in ('COMMISSION','TOTAL_COST'))<>2
             or (
               source_row.source_money_parse_state='VALID'
               and (
                 source_row.source_commission_pence is null
                 or source_row.source_total_cost_pence is null
                 or source_row.source_shift_charge_pence is null
                 or source_row.source_shift_charge_pence<>
                    source_row.source_commission_pence+source_row.source_total_cost_pence
                 or sign(source_row.source_commission_pence) not in
                    (0,sign(source_row.source_shift_charge_pence))
                 or sign(source_row.source_total_cost_pence) not in
                    (0,sign(source_row.source_shift_charge_pence))
                 or (select count(*) from public.weekly_source_money_cell_evidence money
                     where money.upload_id=source_row.upload_id
                       and money.source_row_ordinal=source_row.source_row_ordinal
                       and money.money_field_kind in ('COMMISSION','TOTAL_COST')
                       and money.parse_state='VALID'
                       and not money.formula_present)<>2
                 or (select money.parsed_pence
                     from public.weekly_source_money_cell_evidence money
                     where money.upload_id=source_row.upload_id
                       and money.source_row_ordinal=source_row.source_row_ordinal
                       and money.money_field_kind='COMMISSION')
                    is distinct from source_row.source_commission_pence
                 or (select money.parsed_pence
                     from public.weekly_source_money_cell_evidence money
                     where money.upload_id=source_row.upload_id
                       and money.source_row_ordinal=source_row.source_row_ordinal
                       and money.money_field_kind='TOTAL_COST')
                    is distinct from source_row.source_total_cost_pence
               )
             )
             or (
               source_row.source_money_parse_state<>'VALID'
               and (
                 source_row.source_money_parse_state not in (
                   'MISSING','INVALID','EXCESS_PRECISION','FORMULA',
                   'UNSUPPORTED_CELL_TYPE','OVERFLOW'
                 )
                 or source_row.source_commission_pence is not null
                 or source_row.source_total_cost_pence is not null
                 or source_row.source_shift_charge_pence is not null
                 or not exists(
                   select 1 from public.weekly_source_money_cell_evidence money
                   where money.upload_id=source_row.upload_id
                     and money.source_row_ordinal=source_row.source_row_ordinal
                     and money.parse_state<>'VALID'
                 )
               )
             )
           )
       )
       or exists(
         select 1 from public.weekly_source_money_cell_evidence money
         where money.upload_id=v_upload.id
           and (
             money.money_field_kind not in ('COMMISSION','TOTAL_COST')
             or money.source_file_sha256 is distinct from v_upload.content_sha256
             or money.source_kind not in ('XLSX_NUMERIC_TOKEN','XLSX_STRING_TOKEN')
             or nullif(money.cell_coordinate,'') is null
             or (money.formula_present is distinct from (money.parse_state='FORMULA'))
           )
       ) then
      raise exception 'WEEKLY_SOURCE_NHSP_PREFINAL_MONEY_EVIDENCE_INVALID'
        using errcode='55000';
    end if;
  else
    if exists(select 1 from public.weekly_source_money_cell_evidence where upload_id=v_upload.id)
       or exists(
         select 1 from public.weekly_source_upload_rows source_row
         where source_row.upload_id=v_upload.id
           and (source_row.source_money_parse_state<>'NOT_APPLICABLE'
                or source_row.source_commission_pence is not null
                or source_row.source_total_cost_pence is not null
                or source_row.source_shift_charge_pence is not null)
       ) then
      raise exception 'WEEKLY_SOURCE_MONEY_EVIDENCE_NOT_ALLOWED' using errcode='55000';
    end if;
  end if;

  select count(*)::integer into v_expense_count
  from public.weekly_source_expense_cell_evidence where upload_id=v_upload.id;
  if v_profile.profile_code='ROSTER_WEEKLY_SUMMARY_ACTUAL_V1' then
    if v_expense_count<>v_actual_accepted
       or exists(
         select 1 from public.weekly_source_upload_rows source_row
         left join public.weekly_source_expense_cell_evidence expense
           on expense.upload_id=source_row.upload_id
          and expense.source_row_ordinal=source_row.source_row_ordinal
         where source_row.upload_id=v_upload.id
           and (
             expense.id is null
             or expense.source_file_sha256 is distinct from v_upload.content_sha256
             or source_row.source_expense_parse_state not in ('VALID','OMITTED_ZERO')
             or expense.parse_state is distinct from source_row.source_expense_parse_state
             or expense.parsed_pence is distinct from source_row.source_expense_pence
           )
       ) then
      raise exception 'WEEKLY_SOURCE_EXPENSE_EVIDENCE_INVALID' using errcode='55000';
    end if;
  elsif v_expense_count<>0 or exists(
    select 1 from public.weekly_source_upload_rows source_row
    where source_row.upload_id=v_upload.id
      and (source_row.source_expense_parse_state<>'NOT_APPLICABLE'
           or source_row.source_expense_pence is not null)
  ) then
    raise exception 'WEEKLY_SOURCE_EXPENSE_EVIDENCE_NOT_ALLOWED' using errcode='55000';
  end if;

  if v_profile.profile_code<>'NHSP_FINAL_BACKING_V1' then
    if v_upload.coverage_state<>'COMPLETE'
       or v_upload.confirmed_coverage_start_local_date is null
       or v_upload.confirmed_coverage_end_local_date is null
       or v_upload.confirmed_coverage_start_local_date>v_upload.confirmed_coverage_end_local_date
       or v_upload.coverage_timezone<>'Europe/London'
       or nullif(v_upload.coverage_confirmation_version,'') is null
       or v_upload.coverage_confirmed_by_user_id is null
       or v_upload.coverage_confirmed_at_utc is null then
      raise exception 'WEEKLY_SOURCE_COMPLETE_COVERAGE_INVALID' using errcode='55000';
    end if;
    if v_actual_accepted>0 then
      select pg_catalog.min(source_row.work_date),pg_catalog.max(source_row.work_date)
        into v_actual_coverage_start,v_actual_coverage_end
      from public.weekly_source_upload_rows source_row
      where source_row.upload_id=v_upload.id;
      -- 14 section 5.4.3, first sentence: the SUGGESTION is the file's own
      -- earliest and latest valid work Date, so it is evidence and is still
      -- checked exactly against the rows that were actually staged.  A caller
      -- may not widen the suggestion to manufacture coverage.
      if v_upload.suggested_coverage_start_local_date is distinct from v_actual_coverage_start
         or v_upload.suggested_coverage_end_local_date is distinct from v_actual_coverage_end then
        raise exception 'WEEKLY_SOURCE_COVERAGE_EVIDENCE_MISMATCH' using errcode='55000';
      end if;
      -- 14 section 5.4.3, second sentence: "Office must confirm that the export
      -- is complete for that date range."  The CONFIRMATION is Office's own
      -- attestation and is deliberately independent of the rows present, so
      -- that 5.4.5 bullet 4 -- "absent inside confirmed coverage: create one
      -- full cancellation reversal" -- can reach a shift the source has
      -- stopped sending.  Coverage derived from what arrived can never notice
      -- what stopped arriving.  It must still CONTAIN the evidence, because the
      -- finaliser admits no resolution outside confirmed coverage.
      if v_upload.confirmed_coverage_start_local_date>v_actual_coverage_start
         or v_upload.confirmed_coverage_end_local_date<v_actual_coverage_end then
        raise exception 'WEEKLY_SOURCE_COVERAGE_CONFIRMATION_NARROWER_THAN_EVIDENCE'
          using errcode='55000';
      end if;
    elsif v_upload.suggested_coverage_start_local_date is not null
       or v_upload.suggested_coverage_end_local_date is not null then
      raise exception 'WEEKLY_SOURCE_EMPTY_COVERAGE_SUGGESTION_INVALID' using errcode='55000';
    end if;
  end if;

  v_manifest_hash:=private.weekly_source_upload_manifest_hash_v1(v_upload.id);
  if v_upload.purpose='FINAL_SOURCE_CORRECTION' then
    select * into v_correction
    from public.weekly_final_source_correction_sessions
    where id=v_upload.correction_session_id for update;
    if not found or v_correction.source_cycle_id is distinct from v_cycle.id
       or v_correction.report_scope_id is distinct from v_upload.report_scope_id
       or v_correction.replacement_correction_upload_id is distinct from v_upload.id
       or v_correction.state<>'STAGING' then
      raise exception 'WEEKLY_SOURCE_CORRECTION_SESSION_STALE' using errcode='55000';
    end if;
    update public.weekly_source_uploads
    set row_manifest_hash=v_manifest_hash,state='CORRECTION_READY'
    where id=v_upload.id;
    update public.weekly_final_source_correction_sessions
    set state='READY',version=version+1,updated_at_utc=pg_catalog.transaction_timestamp()
    where id=v_correction.id;
    v_attempt_id:=private.weekly_source_upload_attempt_append_v1(
      v_upload.id,p_actor_user_id,'ACCEPTED','CORRECTION_UPLOAD_READY'
    );
    return pg_catalog.jsonb_build_object(
      'ok',true,'status','CORRECTION_READY','logical_upload_id',v_upload.id,
      'row_manifest_hash',pg_catalog.encode(v_manifest_hash,'hex'),
      'attempt_id',v_attempt_id,'current_pointer_moved',false
    );
  end if;

  if v_upload.report_scope_id is null then
    select * into strict v_cycle
    from public.weekly_source_cycles where id=v_upload.source_cycle_id for update;
    if v_cycle.state not in ('OPEN','FINALISABLE') then
      raise exception 'WEEKLY_SOURCE_CYCLE_NOT_OPEN' using errcode='55000';
    end if;
    v_version_before:=v_cycle.version;
    if v_cycle.current_complete_upload_id is not null then
      select * into strict v_prior_upload
      from public.weekly_source_uploads
      where id=v_cycle.current_complete_upload_id for update;
      if v_prior_upload.source_cycle_id is distinct from v_cycle.id
         or v_prior_upload.report_scope_id is not null
         or v_prior_upload.state<>'CURRENT' then
        raise exception 'WEEKLY_SOURCE_CURRENT_POINTER_CORRUPT' using errcode='55000';
      end if;
      if (v_upload.confirmed_coverage_start_local_date>
            v_prior_upload.confirmed_coverage_start_local_date
          or v_upload.confirmed_coverage_end_local_date<
            v_prior_upload.confirmed_coverage_end_local_date)
         and coalesce(v_upload.coverage_shrink_acknowledged,false)=false then
        raise exception 'WEEKLY_SOURCE_COVERAGE_SHRINK_ACKNOWLEDGEMENT_REQUIRED'
          using errcode='55000';
      end if;
      update public.weekly_source_uploads set state='SUPERSEDED'
      where id=v_prior_upload.id;
    end if;
    update public.weekly_source_projection_publications
    set state='STALE',failure_code='SUPERSEDED_BY_COMPLETE_UPLOAD'
    where source_cycle_id=v_cycle.id and authority_scope_kind='CYCLE'
      and report_scope_id is null and state in ('BUILDING','CURRENT');
    v_version_after:=v_version_before+1;
    update public.weekly_source_uploads
    set row_manifest_hash=v_manifest_hash,state='CURRENT' where id=v_upload.id;
    update public.weekly_source_cycles
    set current_complete_upload_id=v_upload.id,version=v_version_after,
        current_projection_publication_id=null,projection_state='REBUILDING'
    where id=v_cycle.id;
    insert into public.weekly_source_upload_supersessions(
      source_cycle_id,authority_scope_kind,report_scope_id,superseding_upload_id,
      superseded_upload_id,cycle_version_before,cycle_version_after
    ) values (
      v_cycle.id,'CYCLE',null,v_upload.id,v_prior_upload.id,
      v_version_before,v_version_after
    );
  else
    select * into strict v_scope
    from public.weekly_source_report_scopes where id=v_upload.report_scope_id for update;
    if v_scope.state not in ('OPEN','FINALISABLE') then
      raise exception 'WEEKLY_SOURCE_REPORT_SCOPE_NOT_OPEN' using errcode='55000';
    end if;
    v_version_before:=v_scope.version;
    if v_scope.current_complete_upload_id is not null then
      select * into strict v_prior_upload
      from public.weekly_source_uploads
      where id=v_scope.current_complete_upload_id for update;
      if v_prior_upload.source_cycle_id is distinct from v_cycle.id
         or v_prior_upload.report_scope_id is distinct from v_scope.id
         or v_prior_upload.state<>'CURRENT' then
        raise exception 'WEEKLY_SOURCE_CURRENT_POINTER_CORRUPT' using errcode='55000';
      end if;
      update public.weekly_source_uploads set state='SUPERSEDED'
      where id=v_prior_upload.id;
    end if;
    update public.weekly_source_projection_publications
    set state='STALE',failure_code='SUPERSEDED_BY_COMPLETE_UPLOAD'
    where source_cycle_id=v_cycle.id and authority_scope_kind='NHSP_REPORT_SCOPE'
      and report_scope_id=v_scope.id and state in ('BUILDING','CURRENT');
    v_version_after:=v_version_before+1;
    update public.weekly_source_uploads
    set row_manifest_hash=v_manifest_hash,state='CURRENT' where id=v_upload.id;
    update public.weekly_source_report_scopes
    set current_complete_upload_id=v_upload.id,version=v_version_after,
        current_projection_publication_id=null,projection_state='REBUILDING',
        updated_at_utc=pg_catalog.transaction_timestamp()
    where id=v_scope.id;
    insert into public.weekly_source_upload_supersessions(
      source_cycle_id,authority_scope_kind,report_scope_id,superseding_upload_id,
      superseded_upload_id,cycle_version_before,cycle_version_after
    ) values (
      v_cycle.id,'NHSP_REPORT_SCOPE',v_scope.id,v_upload.id,v_prior_upload.id,
      v_version_before,v_version_after
    );
  end if;

  v_attempt_id:=private.weekly_source_upload_attempt_append_v1(
    v_upload.id,p_actor_user_id,'ACCEPTED','NEW_COMPLETE_UPLOAD'
  );
  return pg_catalog.jsonb_build_object(
    'ok',true,'status','CURRENT','logical_upload_id',v_upload.id,
    'superseded_upload_id',v_prior_upload.id,
    'authority_scope_kind',case when v_upload.report_scope_id is null
      then 'CYCLE' else 'NHSP_REPORT_SCOPE' end,
    'authority_scope_version',v_version_after,
    'row_manifest_hash',pg_catalog.encode(v_manifest_hash,'hex'),
    'attempt_id',v_attempt_id,'current_pointer_moved',true,
    'projection_state','REBUILDING'
  );
end;
$function$;

create or replace function public.weekly_source_upload_seal_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_upload_id uuid;
  v_actor uuid;
  v_upload public.weekly_source_uploads%rowtype;
  v_group_id uuid;
  v_cycle_date date;
  v_client_id uuid;
  v_message text;
  v_state text;
  v_reason text;
  v_attempt_id uuid;
begin
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(
       select 1 from pg_catalog.jsonb_object_keys(p_request) key
       where key not in ('upload_id','actor_user_id')
     ) then
    raise exception 'WEEKLY_SOURCE_SEAL_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_upload_id:=(p_request->>'upload_id')::uuid;
    v_actor:=(p_request->>'actor_user_id')::uuid;
  exception when invalid_text_representation then
    raise exception 'WEEKLY_SOURCE_SEAL_REQUEST_INVALID' using errcode='22023';
  end;
  select * into v_upload
  from public.weekly_source_uploads where id=v_upload_id;
  if not found then
    raise exception 'WEEKLY_SOURCE_UPLOAD_NOT_FOUND' using errcode='22023';
  end if;
  if v_upload.uploaded_by_user_id is distinct from v_actor then
    raise exception 'WEEKLY_SOURCE_UPLOAD_ACTOR_MISMATCH' using errcode='42501';
  end if;
  select cycle.source_group_id,cycle.finalisation_week_ending
    into strict v_group_id,v_cycle_date
  from public.weekly_source_cycles cycle where cycle.id=v_upload.source_cycle_id;
  if v_upload.report_scope_id is null then
    begin
      v_client_id:=nullif(v_upload.file_metadata_json->>'client_id','')::uuid;
    exception when invalid_text_representation then
      raise exception 'WEEKLY_SOURCE_CLIENT_SCOPE_INVALID' using errcode='55000';
    end;
  else
    select scope.client_id into strict v_client_id
    from public.weekly_source_report_scopes scope
    where scope.id=v_upload.report_scope_id
      and scope.source_cycle_id=v_upload.source_cycle_id;
  end if;
  perform private.weekly_source_office_authority_v1(
    v_actor,'SEAL_SOURCE',v_group_id,v_client_id,v_cycle_date
  );

  begin
    return private.weekly_source_upload_seal_core_v1(v_upload_id,v_actor);
  exception when others then
    get stacked diagnostics v_message=message_text,v_state=returned_sqlstate;
    v_reason:=case
      when v_message~'^[A-Z][A-Z0-9_]{2,99}$' then v_message
      else 'WEEKLY_SOURCE_SEAL_FAILED'
    end;
    update public.weekly_source_uploads
    set state='REJECTED'
    where id=v_upload_id and state='STAGING';
    if found then
      if v_upload.purpose='FINAL_SOURCE_CORRECTION' then
        update public.weekly_final_source_correction_sessions
        set state='DRAFT',replacement_correction_upload_id=null,
            version=version+1,updated_at_utc=pg_catalog.transaction_timestamp()
        where id=v_upload.correction_session_id
          and state='STAGING'
          and replacement_correction_upload_id=v_upload_id;
        if not found then
          raise exception 'WEEKLY_SOURCE_CORRECTION_SESSION_RESET_FAILED' using errcode='55000';
        end if;
      end if;
      v_attempt_id:=private.weekly_source_upload_attempt_append_v1(
        v_upload_id,v_actor,'REJECTED',v_reason
      );
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',false,'status','REJECTED','reason_code',v_reason,
      'sqlstate',v_state,'logical_upload_id',v_upload_id,
      'attempt_id',v_attempt_id,'current_pointer_moved',false
    );
  end;
end;
$function$;

create or replace function public.weekly_source_upload_abort_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_upload_id uuid;
  v_upload public.weekly_source_uploads%rowtype;
  v_group_id uuid;
  v_cycle_date date;
  v_client_id uuid;
  v_attempt_id uuid;
begin
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(
       select 1 from pg_catalog.jsonb_object_keys(p_request) key
       where key not in ('actor_user_id','upload_id')
     ) then
    raise exception 'WEEKLY_SOURCE_ABORT_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_upload_id:=(p_request->>'upload_id')::uuid;
  exception when invalid_text_representation then
    raise exception 'WEEKLY_SOURCE_ABORT_REQUEST_INVALID' using errcode='22023';
  end;
  select * into v_upload
  from public.weekly_source_uploads where id=v_upload_id for update;
  if not found or v_upload.uploaded_by_user_id is distinct from v_actor then
    raise exception 'WEEKLY_SOURCE_UPLOAD_NOT_AVAILABLE' using errcode='42501';
  end if;
  select cycle.source_group_id,cycle.finalisation_week_ending
    into strict v_group_id,v_cycle_date
  from public.weekly_source_cycles cycle where cycle.id=v_upload.source_cycle_id;
  if v_upload.report_scope_id is null then
    begin
      v_client_id:=nullif(v_upload.file_metadata_json->>'client_id','')::uuid;
    exception when invalid_text_representation then
      raise exception 'WEEKLY_SOURCE_CLIENT_SCOPE_INVALID' using errcode='55000';
    end;
  else
    select scope.client_id into strict v_client_id
    from public.weekly_source_report_scopes scope
    where scope.id=v_upload.report_scope_id
      and scope.source_cycle_id=v_upload.source_cycle_id;
  end if;
  perform private.weekly_source_office_authority_v1(
    v_actor,'UPLOAD_SOURCE',v_group_id,v_client_id,v_cycle_date
  );
  if v_upload.state='REJECTED' then
    return pg_catalog.jsonb_build_object(
      'ok',true,'status','REJECTED','logical_upload_id',v_upload.id,'idempotent',true
    );
  end if;
  if v_upload.state<>'STAGING' then
    raise exception 'WEEKLY_SOURCE_UPLOAD_ABORT_NOT_ALLOWED' using errcode='55000';
  end if;
  update public.weekly_source_uploads set state='REJECTED' where id=v_upload.id;
  if v_upload.purpose='FINAL_SOURCE_CORRECTION' then
    update public.weekly_final_source_correction_sessions
    set state='DRAFT',replacement_correction_upload_id=null,
        version=version+1,updated_at_utc=pg_catalog.transaction_timestamp()
    where id=v_upload.correction_session_id
      and state='STAGING'
      and replacement_correction_upload_id=v_upload.id;
    if not found then
      raise exception 'WEEKLY_SOURCE_CORRECTION_SESSION_RESET_FAILED' using errcode='55000';
    end if;
  end if;
  v_attempt_id:=private.weekly_source_upload_attempt_append_v1(
    v_upload.id,v_actor,'REJECTED','UPLOAD_ABORTED_BY_OFFICE'
  );
  return pg_catalog.jsonb_build_object(
    'ok',true,'status','REJECTED','logical_upload_id',v_upload.id,
    'attempt_id',v_attempt_id,'current_pointer_moved',false
  );
end;
$function$;

create or replace function public.weekly_source_projection_begin_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_allowed_keys constant text[]:=array[
    'actor_user_id','upload_id','expected_authority_scope_version',
    'correction_session_id','expected_correction_session_version',
    'expected_projection_publication_id','expected_row_manifest_hash',
    'expected_comparison_manifest_hash','expected_issue_set_hash',
    'rebuild_idempotency_key'
  ]::text[];
  v_rebuild_keys constant text[]:=array[
    'correction_session_id','expected_correction_session_version',
    'expected_projection_publication_id','expected_row_manifest_hash',
    'expected_comparison_manifest_hash','expected_issue_set_hash',
    'rebuild_idempotency_key'
  ]::text[];
  v_unknown_key text;
  v_rebuild boolean;
  v_actor uuid;
  v_upload_id uuid;
  v_expected_version bigint;
  v_correction_id uuid;
  v_expected_session_version bigint;
  v_expected_publication_id uuid;
  v_expected_row_hash bytea;
  v_expected_comparison_hash bytea;
  v_expected_issue_hash bytea;
  v_rebuild_idempotency_key text;
  v_rebuild_request_hash bytea;
  v_upload public.weekly_source_uploads%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_scope public.weekly_source_report_scopes%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_publication public.weekly_source_projection_publications%rowtype;
  v_prior_publication public.weekly_source_projection_publications%rowtype;
  v_correction public.weekly_final_source_correction_sessions%rowtype;
  v_scope_kind text;
  v_scope_version bigint;
  v_scope_current_upload uuid;
  v_client_id uuid;
  v_empty_comparison_hash bytea;
  v_empty_issue_hash bytea;
  v_projection_generation integer;
  v_resolution_count integer;
  v_rows_applied boolean:=false;
  v_rebuild_replay boolean:=false;
  v_correction_found boolean:=false;
begin
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object' then
    raise exception 'WEEKLY_SOURCE_PROJECTION_BEGIN_REQUEST_INVALID' using errcode='22023';
  end if;
  select key into v_unknown_key
  from pg_catalog.jsonb_object_keys(p_request) key
  where not key=any(v_allowed_keys)
  order by key limit 1;
  if v_unknown_key is not null then
    raise exception 'WEEKLY_SOURCE_PROJECTION_BEGIN_REQUEST_INVALID'
      using errcode='22023',detail=v_unknown_key;
  end if;
  v_rebuild:=p_request ?| v_rebuild_keys;
  if v_rebuild and exists(
    select 1 from pg_catalog.unnest(v_rebuild_keys) key
    where not (p_request ? key)
  ) then
    raise exception 'WEEKLY_SOURCE_PROJECTION_REBUILD_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_upload_id:=(p_request->>'upload_id')::uuid;
    v_expected_version:=(p_request->>'expected_authority_scope_version')::bigint;
    if v_rebuild then
      v_correction_id:=(p_request->>'correction_session_id')::uuid;
      v_expected_session_version:=(p_request->>'expected_correction_session_version')::bigint;
      v_expected_publication_id:=(p_request->>'expected_projection_publication_id')::uuid;
      v_expected_row_hash:=private.weekly_source_hex32_v1(
        p_request->>'expected_row_manifest_hash','WEEKLY_SOURCE_ROW_MANIFEST_HASH_INVALID'
      );
      v_expected_comparison_hash:=private.weekly_source_hex32_v1(
        p_request->>'expected_comparison_manifest_hash',
        'WEEKLY_SOURCE_COMPARISON_MANIFEST_HASH_INVALID'
      );
      v_expected_issue_hash:=private.weekly_source_hex32_v1(
        p_request->>'expected_issue_set_hash','WEEKLY_SOURCE_ISSUE_SET_HASH_INVALID'
      );
      v_rebuild_idempotency_key:=pg_catalog.btrim(
        coalesce(p_request->>'rebuild_idempotency_key','')
      );
    end if;
  exception when invalid_text_representation or numeric_value_out_of_range then
    raise exception 'WEEKLY_SOURCE_PROJECTION_BEGIN_REQUEST_INVALID' using errcode='22023';
  end;
  if v_actor is null or v_upload_id is null or v_expected_version is null
     or v_expected_version<1
     or (v_rebuild and (
       v_correction_id is null or v_expected_session_version is null
       or v_expected_session_version<1 or v_expected_publication_id is null
       or pg_catalog.octet_length(v_expected_row_hash)<>32
       or pg_catalog.octet_length(v_expected_comparison_hash)<>32
       or pg_catalog.octet_length(v_expected_issue_hash)<>32
       or pg_catalog.char_length(v_rebuild_idempotency_key) not between 1 and 200
     )) then
    raise exception 'WEEKLY_SOURCE_PROJECTION_BEGIN_REQUEST_INVALID' using errcode='22023';
  end if;
  if v_rebuild then
    v_rebuild_request_hash:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_PROJECTION_REBUILD_V1',p_request-'rebuild_idempotency_key'
    );
  end if;
  select * into v_upload
  from public.weekly_source_uploads where id=v_upload_id;
  if not found or v_upload.row_manifest_hash is null
     or (v_upload.purpose='ORDINARY' and v_upload.state<>'CURRENT')
     or (v_upload.purpose='FINAL_SOURCE_CORRECTION' and v_upload.state<>'CORRECTION_READY') then
    return pg_catalog.jsonb_build_object(
      'ok',false,'status','STALE','reason_code','WEEKLY_SOURCE_UPLOAD_NOT_CURRENT'
    );
  end if;
  select * into strict v_cycle
  from public.weekly_source_cycles where id=v_upload.source_cycle_id;
  select * into strict v_group
  from public.weekly_source_groups where id=v_cycle.source_group_id;
  if v_upload.purpose<>'FINAL_SOURCE_CORRECTION' and v_upload.correction_session_id is not null then
    raise exception 'WEEKLY_SOURCE_PROJECTION_SCOPE_INVALID' using errcode='55000';
  end if;

  if v_upload.report_scope_id is null then
    v_scope_kind:='CYCLE';
    v_scope_version:=v_cycle.version;
    v_scope_current_upload:=v_cycle.current_complete_upload_id;
    begin
      v_client_id:=nullif(v_upload.file_metadata_json->>'client_id','')::uuid;
    exception when invalid_text_representation then
      raise exception 'WEEKLY_SOURCE_PROJECTION_SCOPE_INVALID' using errcode='55000';
    end;
  else
    v_scope_kind:='NHSP_REPORT_SCOPE';
    select * into strict v_scope
    from public.weekly_source_report_scopes where id=v_upload.report_scope_id;
    if v_scope.source_cycle_id is distinct from v_cycle.id
       or v_scope.source_group_id is distinct from v_group.id then
      raise exception 'WEEKLY_SOURCE_PROJECTION_SCOPE_INVALID' using errcode='55000';
    end if;
    v_scope_version:=v_scope.version;
    v_scope_current_upload:=v_scope.current_complete_upload_id;
    v_client_id:=v_scope.client_id;
  end if;
  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended(pg_catalog.encode(v_upload.declared_scope_fingerprint,'hex'),73241837)
  );
  if v_scope_kind='CYCLE' then
    select * into strict v_cycle
    from public.weekly_source_cycles where id=v_upload.source_cycle_id for update;
    v_scope_version:=v_cycle.version;
    v_scope_current_upload:=v_cycle.current_complete_upload_id;
  else
    -- Each NHSP Trust publishes independently under its report-scope head.
    select * into strict v_cycle
    from public.weekly_source_cycles where id=v_upload.source_cycle_id;
    select * into strict v_scope
    from public.weekly_source_report_scopes where id=v_upload.report_scope_id for update;
    if v_scope.source_cycle_id is distinct from v_cycle.id
       or v_scope.source_group_id is distinct from v_group.id
       or v_scope.environment is distinct from v_group.environment
       or v_scope.agency_id is distinct from v_group.agency_id
       or v_scope.client_id is distinct from v_client_id
       or v_scope.cutoff_at_utc is distinct from v_cycle.cutoff_at_utc then
      raise exception 'WEEKLY_SOURCE_PROJECTION_SCOPE_CHANGED' using errcode='55000';
    end if;
    v_scope_version:=v_scope.version;
    v_scope_current_upload:=v_scope.current_complete_upload_id;
  end if;
  if v_upload.purpose='FINAL_SOURCE_CORRECTION' then
    select * into v_correction
    from public.weekly_final_source_correction_sessions
    where id=v_upload.correction_session_id for update;
    v_correction_found:=found;
    if v_correction_found and v_rebuild then
      select * into v_publication
      from public.weekly_source_projection_publications publication
      where publication.correction_session_id=v_correction.id
        and publication.rebuild_idempotency_key=v_rebuild_idempotency_key
      for update;
      if found then
        if v_publication.rebuild_request_hash is distinct from v_rebuild_request_hash then
          raise exception 'WEEKLY_SOURCE_PROJECTION_REBUILD_IDEMPOTENCY_COLLISION'
            using errcode='22023';
        end if;
        if v_publication.state='BUILDING' then
          if v_correction.state<>'REVIEWED'
             or v_correction.version<>v_expected_session_version
             or v_correction.replacement_projection_publication_id
                  is distinct from v_expected_publication_id then
            raise exception 'WEEKLY_SOURCE_CORRECTION_REBUILD_STALE' using errcode='40001';
          end if;
        elsif v_publication.state='CORRECTION_READY' then
          if v_correction.replacement_projection_publication_id
               is distinct from v_publication.id
             or v_publication.ready_session_version is null then
            raise exception 'WEEKLY_SOURCE_CORRECTION_REBUILD_STALE' using errcode='40001';
          end if;
        else
          raise exception 'WEEKLY_SOURCE_CORRECTION_REBUILD_STALE' using errcode='40001';
        end if;
        v_rebuild_replay:=true;
      end if;
    end if;
    if not v_correction_found
       or v_correction.source_cycle_id is distinct from v_cycle.id
       or v_correction.authority_scope_kind is distinct from v_scope_kind
       or v_correction.report_scope_id is distinct from v_upload.report_scope_id
       or v_correction.replacement_correction_upload_id is distinct from v_upload.id
       or v_correction.actor_user_id is distinct from v_actor
       or (v_rebuild and not v_rebuild_replay and (
         v_correction.id is distinct from v_correction_id
         or v_correction.state<>'REVIEWED'
         or v_correction.version<>v_expected_session_version
         or v_correction.replacement_projection_publication_id
              is distinct from v_expected_publication_id
       ))
       or (not v_rebuild and v_correction.state<>'READY') then
      raise exception 'WEEKLY_SOURCE_CORRECTION_SESSION_STALE' using errcode='40001';
    end if;
  elsif v_rebuild then
    raise exception 'WEEKLY_SOURCE_PROJECTION_REBUILD_REQUEST_INVALID' using errcode='22023';
  end if;
  if private.weekly_source_scope_fingerprint_v1(
       v_group.environment,v_group.agency_id,v_group.id,v_cycle.id,
       v_upload.report_scope_id,v_client_id
     ) is distinct from v_upload.declared_scope_fingerprint then
    raise exception 'WEEKLY_SOURCE_PROJECTION_SCOPE_CHANGED' using errcode='55000';
  end if;
  perform private.weekly_source_office_authority_v1(
    v_actor,'RECHECK_SOURCE',v_group.id,v_client_id,v_cycle.finalisation_week_ending
  );
  if v_scope_version<>v_expected_version
     or (v_upload.purpose='ORDINARY' and v_scope_current_upload is distinct from v_upload.id)
     or (v_upload.purpose='FINAL_SOURCE_CORRECTION' and
       coalesce(v_scope.current_final_revision_id,v_cycle.current_final_revision_id)
         is distinct from v_correction.expected_current_final_revision_id) then
    return pg_catalog.jsonb_build_object(
      'ok',false,'status','STALE','reason_code','WEEKLY_SOURCE_SCOPE_VERSION_STALE',
      'current_upload_id',v_scope_current_upload,'current_authority_scope_version',v_scope_version
    );
  end if;

  if v_rebuild then
    select * into v_publication
    from public.weekly_source_projection_publications publication
    where publication.correction_session_id=v_correction.id
      and publication.rebuild_idempotency_key=v_rebuild_idempotency_key
    for update;
    if found then
      if v_publication.state not in ('BUILDING','CORRECTION_READY')
         or v_publication.upload_id is distinct from v_upload.id
         or v_publication.authority_scope_version<>v_scope_version then
        return pg_catalog.jsonb_build_object(
          'ok',false,'status','STALE',
          'reason_code','WEEKLY_SOURCE_CORRECTION_REBUILD_STALE',
          'publication_id',v_publication.id
        );
      end if;
      if v_publication.state='CORRECTION_READY'
         and v_correction.replacement_projection_publication_id
               is distinct from v_publication.id then
        return pg_catalog.jsonb_build_object(
          'ok',false,'status','STALE',
          'reason_code','WEEKLY_SOURCE_CORRECTION_REBUILD_POINTER_STALE',
          'publication_id',v_publication.id
        );
      end if;
      select pg_catalog.count(*)::integer into v_resolution_count
      from public.weekly_source_row_resolutions resolution
      join public.weekly_source_upload_rows source_row
        on source_row.id=resolution.upload_row_id
      where source_row.upload_id=v_upload.id
        and resolution.generation=coalesce(
          v_publication.projection_generation,
          v_publication.authority_scope_version::integer
        );
      v_rows_applied:=v_resolution_count=v_upload.accepted_count;
      return pg_catalog.jsonb_build_object(
        'ok',true,'status',v_publication.state,
        'publication_id',v_publication.id,'upload_id',v_upload.id,
        'authority_scope_version',v_scope_version,
        'projection_generation',v_publication.projection_generation,
        'ready_session_version',v_publication.ready_session_version,
        'comparison_manifest_hash',pg_catalog.encode(v_publication.comparison_manifest_hash,'hex'),
        'issue_set_hash',pg_catalog.encode(v_publication.issue_set_hash,'hex'),
        'rows_applied',v_rows_applied,'idempotent',true
      );
    end if;
    select * into v_prior_publication
    from public.weekly_source_projection_publications publication
    where publication.id=v_expected_publication_id for share;
    if not found or v_upload.row_manifest_hash is distinct from v_expected_row_hash
       or v_prior_publication.state<>'CORRECTION_READY'
       or v_prior_publication.correction_session_id is distinct from v_correction.id
       or v_prior_publication.upload_id is distinct from v_upload.id
       or v_prior_publication.source_cycle_id is distinct from v_cycle.id
       or v_prior_publication.authority_scope_kind is distinct from v_scope_kind
       or v_prior_publication.report_scope_id is distinct from v_upload.report_scope_id
       or v_prior_publication.authority_scope_version<>v_scope_version
       or v_prior_publication.comparison_manifest_hash is distinct from v_expected_comparison_hash
       or v_prior_publication.issue_set_hash is distinct from v_expected_issue_hash then
      raise exception 'WEEKLY_SOURCE_CORRECTION_REBUILD_STALE' using errcode='40001';
    end if;
  end if;

  select * into v_publication
  from public.weekly_source_projection_publications
  where source_cycle_id=v_cycle.id
    and authority_scope_kind=v_scope_kind
    and report_scope_id is not distinct from v_upload.report_scope_id
    and authority_scope_version=v_scope_version
    and upload_id=v_upload.id
    and projection_generation is null
    and rebuild_idempotency_key is null
  for update;
  if found and not v_rebuild then
    if v_publication.state in ('BUILDING','CURRENT','CORRECTION_READY') then
      return pg_catalog.jsonb_build_object(
        'ok',true,'status',v_publication.state,'publication_id',v_publication.id,
        'upload_id',v_upload.id,'authority_scope_version',v_scope_version,
        'idempotent',true
      );
    elsif v_publication.state='FAILED' then
      update public.weekly_source_projection_publications
      set state='BUILDING',failure_code=null,published_at_utc=null
      where id=v_publication.id;
      return pg_catalog.jsonb_build_object(
        'ok',true,'status','BUILDING','publication_id',v_publication.id,
        'upload_id',v_upload.id,'authority_scope_version',v_scope_version,
        'retry',true
      );
    else
      return pg_catalog.jsonb_build_object(
        'ok',false,'status','STALE','reason_code','WEEKLY_SOURCE_PUBLICATION_STALE',
        'publication_id',v_publication.id
      );
    end if;
  end if;

  v_empty_comparison_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_COMPARISON_MANIFEST_V1',
    pg_catalog.jsonb_build_object(
      'manifest_version','WEEKLY_SOURCE_COMPARISON_MANIFEST_V1',
      'source_authority','[]'::jsonb,'timesheet_authority','[]'::jsonb
    )
  );
  v_empty_issue_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_ISSUE_SET_V1',
    pg_catalog.jsonb_build_object('issue_set_version','WEEKLY_SOURCE_ISSUE_SET_V1','issues','[]'::jsonb)
  );
  if v_rebuild then
    if v_scope_version>2147483647 then
      raise exception 'WEEKLY_SOURCE_PROJECTION_GENERATION_OVERFLOW' using errcode='22003';
    end if;
    select pg_catalog.max(coalesce(
      publication.projection_generation,
      publication.authority_scope_version::integer
    )) into v_projection_generation
    from public.weekly_source_projection_publications publication
    where publication.upload_id=v_upload.id;
    if coalesce(v_projection_generation,0)>=2147483647 then
      raise exception 'WEEKLY_SOURCE_PROJECTION_GENERATION_OVERFLOW' using errcode='22003';
    end if;
    v_projection_generation:=coalesce(v_projection_generation,0)+1;
  else
    v_projection_generation:=null;
  end if;
  insert into public.weekly_source_projection_publications(
    source_cycle_id,authority_scope_kind,report_scope_id,upload_id,correction_session_id,
    authority_scope_version,projection_generation,rebuild_idempotency_key,
    rebuild_request_hash,ready_session_version,
    comparison_manifest_hash,issue_set_hash,state
  ) values (
    v_cycle.id,v_scope_kind,v_upload.report_scope_id,v_upload.id,v_upload.correction_session_id,
    v_scope_version,v_projection_generation,v_rebuild_idempotency_key,
    v_rebuild_request_hash,case when v_rebuild then v_expected_session_version end,
    v_empty_comparison_hash,v_empty_issue_hash,'BUILDING'
  ) returning * into v_publication;

  return pg_catalog.jsonb_build_object(
    'ok',true,'status','BUILDING','publication_id',v_publication.id,
    'upload_id',v_upload.id,'authority_scope_kind',v_scope_kind,
    'authority_scope_version',v_scope_version,
    'projection_generation',v_publication.projection_generation,
    'ready_session_version',v_publication.ready_session_version,
    'rows_applied',false,'idempotent',false
  );
end;
$function$;

create or replace function public.weekly_source_projection_publish_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_publication_id uuid;
  v_expected_comparison bytea;
  v_expected_issue bytea;
  v_publication public.weekly_source_projection_publications%rowtype;
  v_upload public.weekly_source_uploads%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_scope public.weekly_source_report_scopes%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_correction public.weekly_final_source_correction_sessions%rowtype;
  v_scope_version bigint;
  v_scope_upload uuid;
  v_client_id uuid;
  v_comparison_hash bytea;
  v_issue_hash bytea;
  v_fingerprint bytea;
  v_resolution_count integer;
  v_generation integer;
  v_ready_session_version bigint;
begin
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(
       select 1 from pg_catalog.jsonb_object_keys(p_request) key
       where key not in ('actor_user_id','publication_id',
         'expected_comparison_manifest_hash','expected_issue_set_hash')
     ) then
    raise exception 'WEEKLY_SOURCE_PROJECTION_PUBLISH_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_publication_id:=(p_request->>'publication_id')::uuid;
  exception when invalid_text_representation then
    raise exception 'WEEKLY_SOURCE_PROJECTION_PUBLISH_REQUEST_INVALID' using errcode='22023';
  end;
  if v_actor is null or v_publication_id is null then
    raise exception 'WEEKLY_SOURCE_PROJECTION_PUBLISH_REQUEST_INVALID' using errcode='22023';
  end if;
  v_expected_comparison:=case
    when nullif(p_request->>'expected_comparison_manifest_hash','') is null then null
    else private.weekly_source_hex32_v1(
      p_request->>'expected_comparison_manifest_hash',
      'WEEKLY_SOURCE_COMPARISON_MANIFEST_HASH_INVALID'
    ) end;
  v_expected_issue:=case
    when nullif(p_request->>'expected_issue_set_hash','') is null then null
    else private.weekly_source_hex32_v1(
      p_request->>'expected_issue_set_hash','WEEKLY_SOURCE_ISSUE_SET_HASH_INVALID'
    ) end;

  select * into v_publication
  from public.weekly_source_projection_publications
  where id=v_publication_id;
  if not found then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_NOT_FOUND' using errcode='22023';
  end if;
  select * into strict v_upload
  from public.weekly_source_uploads where id=v_publication.upload_id;
  select * into strict v_cycle
  from public.weekly_source_cycles where id=v_publication.source_cycle_id;
  select * into strict v_group
  from public.weekly_source_groups where id=v_cycle.source_group_id;
  if v_publication.authority_scope_kind='CYCLE' then
    v_scope_version:=v_cycle.version;
    v_scope_upload:=v_cycle.current_complete_upload_id;
    begin
      v_client_id:=nullif(v_upload.file_metadata_json->>'client_id','')::uuid;
    exception when invalid_text_representation then
      raise exception 'WEEKLY_SOURCE_PROJECTION_SCOPE_INVALID' using errcode='55000';
    end;
  else
    select * into strict v_scope
    from public.weekly_source_report_scopes
    where id=v_publication.report_scope_id;
    v_scope_version:=v_scope.version;
    v_scope_upload:=v_scope.current_complete_upload_id;
    v_client_id:=v_scope.client_id;
  end if;
  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended(pg_catalog.encode(v_upload.declared_scope_fingerprint,'hex'),73241837)
  );
  if v_publication.authority_scope_kind='CYCLE' then
    select * into strict v_cycle
    from public.weekly_source_cycles where id=v_publication.source_cycle_id for update;
    v_scope_version:=v_cycle.version;
    v_scope_upload:=v_cycle.current_complete_upload_id;
  else
    -- Each NHSP Trust publishes independently under its report-scope head.
    select * into strict v_cycle
    from public.weekly_source_cycles where id=v_publication.source_cycle_id;
    select * into strict v_scope
    from public.weekly_source_report_scopes
    where id=v_publication.report_scope_id for update;
    if v_scope.source_cycle_id is distinct from v_cycle.id
       or v_scope.source_group_id is distinct from v_group.id
       or v_scope.environment is distinct from v_group.environment
       or v_scope.agency_id is distinct from v_group.agency_id
       or v_scope.cutoff_at_utc is distinct from v_cycle.cutoff_at_utc then
      raise exception 'WEEKLY_SOURCE_PROJECTION_SCOPE_CHANGED' using errcode='55000';
    end if;
    v_scope_version:=v_scope.version;
    v_scope_upload:=v_scope.current_complete_upload_id;
    v_client_id:=v_scope.client_id;
  end if;
  select * into strict v_publication
  from public.weekly_source_projection_publications
  where id=v_publication_id for update;
  select * into strict v_upload
  from public.weekly_source_uploads where id=v_publication.upload_id;
  if private.weekly_source_scope_fingerprint_v1(
       v_group.environment,v_group.agency_id,v_group.id,v_cycle.id,
       v_publication.report_scope_id,v_client_id
     ) is distinct from v_upload.declared_scope_fingerprint then
    raise exception 'WEEKLY_SOURCE_PROJECTION_SCOPE_CHANGED' using errcode='55000';
  end if;
  perform private.weekly_source_office_authority_v1(
    v_actor,'RECHECK_SOURCE',v_group.id,v_client_id,v_cycle.finalisation_week_ending
  );
  if v_publication.correction_session_id is not null then
    select * into v_correction
    from public.weekly_final_source_correction_sessions
    where id=v_publication.correction_session_id for update;
    if not found
       or v_correction.source_cycle_id is distinct from v_cycle.id
       or v_correction.authority_scope_kind is distinct from v_publication.authority_scope_kind
       or v_correction.report_scope_id is distinct from v_publication.report_scope_id
       or v_correction.replacement_correction_upload_id is distinct from v_upload.id
       or v_correction.actor_user_id is distinct from v_actor
       or v_upload.purpose<>'FINAL_SOURCE_CORRECTION'
       or v_upload.state<>'CORRECTION_READY'
       or v_scope_version<>v_publication.authority_scope_version
       or coalesce(v_scope.current_final_revision_id,v_cycle.current_final_revision_id)
            is distinct from v_correction.expected_current_final_revision_id then
      raise exception 'WEEKLY_SOURCE_CORRECTION_PUBLICATION_STALE' using errcode='40001';
    end if;
    if v_publication.state='CORRECTION_READY' then
      if v_correction.replacement_projection_publication_id is distinct from v_publication.id
         or (v_publication.rebuild_idempotency_key is not null
             and v_publication.ready_session_version is null) then
        raise exception 'WEEKLY_SOURCE_CORRECTION_PUBLICATION_STALE' using errcode='40001';
      end if;
      if (v_expected_comparison is not null
            and v_expected_comparison<>v_publication.comparison_manifest_hash)
         or (v_expected_issue is not null
            and v_expected_issue<>v_publication.issue_set_hash) then
        raise exception 'WEEKLY_SOURCE_PUBLICATION_HASH_MISMATCH' using errcode='40001';
      end if;
      v_fingerprint:=private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_PUBLICATION_FINGERPRINT_V1',
        pg_catalog.jsonb_build_object(
          'publication_id',v_publication.id,'upload_id',v_publication.upload_id,
          'authority_scope_version',v_publication.authority_scope_version,
          'comparison_manifest_hash',pg_catalog.encode(v_publication.comparison_manifest_hash,'hex'),
          'issue_set_hash',pg_catalog.encode(v_publication.issue_set_hash,'hex')
        )
      );
      return pg_catalog.jsonb_build_object(
        'ok',true,'status','CORRECTION_READY','publication_id',v_publication.id,
        'upload_id',v_publication.upload_id,
        'authority_scope_version',v_publication.authority_scope_version,
        'projection_generation',v_publication.projection_generation,
        'ready_session_version',v_publication.ready_session_version,
        'comparison_manifest_hash',pg_catalog.encode(v_publication.comparison_manifest_hash,'hex'),
        'issue_set_hash',pg_catalog.encode(v_publication.issue_set_hash,'hex'),
        'publication_fingerprint',pg_catalog.encode(v_fingerprint,'hex'),'idempotent',true
      );
    end if;
    if (v_publication.rebuild_idempotency_key is null and (
         v_correction.state<>'READY'
         or v_correction.replacement_projection_publication_id is not null
       ))
       or (v_publication.rebuild_idempotency_key is not null and (
         v_publication.projection_generation is null
         or v_publication.ready_session_version is null
         or v_correction.state<>'REVIEWED'
         or v_correction.version<>v_publication.ready_session_version
         or v_correction.replacement_projection_publication_id is null
         or v_correction.replacement_projection_publication_id=v_publication.id
       )) then
      raise exception 'WEEKLY_SOURCE_CORRECTION_PUBLICATION_STALE' using errcode='40001';
    end if;
  end if;
  if v_publication.state='CURRENT'
      and v_publication.correction_session_id is null
      and v_scope_upload=v_publication.upload_id
      and v_scope_version=v_publication.authority_scope_version then
    if (v_expected_comparison is not null
          and v_expected_comparison<>v_publication.comparison_manifest_hash)
       or (v_expected_issue is not null
          and v_expected_issue<>v_publication.issue_set_hash) then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_HASH_MISMATCH' using errcode='40001';
    end if;
    v_fingerprint:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_PUBLICATION_FINGERPRINT_V1',
      pg_catalog.jsonb_build_object(
        'publication_id',v_publication.id,'upload_id',v_publication.upload_id,
        'authority_scope_version',v_publication.authority_scope_version,
        'comparison_manifest_hash',pg_catalog.encode(v_publication.comparison_manifest_hash,'hex'),
        'issue_set_hash',pg_catalog.encode(v_publication.issue_set_hash,'hex')
      )
    );
    return pg_catalog.jsonb_build_object(
      'ok',true,'status',v_publication.state,'publication_id',v_publication.id,
      'upload_id',v_publication.upload_id,
      'authority_scope_version',v_publication.authority_scope_version,
      'projection_generation',v_publication.projection_generation,
      'comparison_manifest_hash',pg_catalog.encode(v_publication.comparison_manifest_hash,'hex'),
      'issue_set_hash',pg_catalog.encode(v_publication.issue_set_hash,'hex'),
      'publication_fingerprint',pg_catalog.encode(v_fingerprint,'hex'),'idempotent',true
    );
  end if;
  if v_publication.state not in ('BUILDING','FAILED')
     or v_scope_version<>v_publication.authority_scope_version
     or (v_publication.correction_session_id is null and (
       v_scope_upload is distinct from v_publication.upload_id
       or v_upload.state<>'CURRENT'
     )) then
    if v_publication.state in ('BUILDING','FAILED') then
      update public.weekly_source_projection_publications
      set state='STALE',failure_code='AUTHORITY_SCOPE_CHANGED'
      where id=v_publication.id;
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',false,'status','STALE','reason_code','WEEKLY_SOURCE_PUBLICATION_CAS_STALE',
      'publication_id',v_publication.id,'current_upload_id',v_scope_upload,
      'current_authority_scope_version',v_scope_version
    );
  end if;

  if v_publication.authority_scope_version>2147483647
     or coalesce(v_publication.projection_generation,0)>2147483647 then
    raise exception 'WEEKLY_SOURCE_PROJECTION_GENERATION_OVERFLOW' using errcode='22003';
  end if;
  v_generation:=coalesce(
    v_publication.projection_generation,
    v_publication.authority_scope_version::integer
  );
  select pg_catalog.count(*)::integer into v_resolution_count
  from public.weekly_source_row_resolutions resolution
  join public.weekly_source_upload_rows source_row
    on source_row.id=resolution.upload_row_id
  where source_row.upload_id=v_upload.id
    and resolution.generation=v_generation;
  if v_resolution_count<>v_upload.accepted_count
     or exists(
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
     )
     or (v_publication.projection_generation is null and exists(
        select 1
        from public.weekly_source_row_resolutions resolution
       join public.weekly_source_upload_rows source_row
         on source_row.id=resolution.upload_row_id
        where source_row.upload_id=v_upload.id
          and resolution.generation<>v_generation
     )) then
    raise exception 'WEEKLY_SOURCE_PROJECTION_RESOLUTION_CENSUS_INCOMPLETE'
      using errcode='55000';
  end if;

  v_comparison_hash:=private.weekly_source_projection_comparison_manifest_hash_v1(
    v_publication.id
  );
  v_issue_hash:=private.weekly_source_projection_issue_set_hash_v1(v_publication.id);
  if (v_expected_comparison is not null and v_expected_comparison<>v_comparison_hash)
     or (v_expected_issue is not null and v_expected_issue<>v_issue_hash) then
    update public.weekly_source_projection_publications
    set state='FAILED',failure_code='PUBLICATION_HASH_MISMATCH'
    where id=v_publication.id;
    if v_publication.correction_session_id is not null then
      update public.weekly_final_source_correction_sessions
      set updated_at_utc=pg_catalog.transaction_timestamp()
      where id=v_publication.correction_session_id;
    elsif v_publication.authority_scope_kind='CYCLE' then
      update public.weekly_source_cycles
      set projection_state='FAILED',current_projection_publication_id=null
      where id=v_cycle.id and current_complete_upload_id=v_publication.upload_id
        and version=v_publication.authority_scope_version;
    else
      update public.weekly_source_report_scopes
      set projection_state='FAILED',current_projection_publication_id=null,
          updated_at_utc=pg_catalog.transaction_timestamp()
      where id=v_scope.id and current_complete_upload_id=v_publication.upload_id
        and version=v_publication.authority_scope_version;
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',false,'status','FAILED','reason_code','WEEKLY_SOURCE_PUBLICATION_HASH_MISMATCH',
      'publication_id',v_publication.id
    );
  end if;

  v_ready_session_version:=case when v_publication.correction_session_id is null then null
    else v_correction.version+1 end;
  update public.weekly_source_projection_publications
  set comparison_manifest_hash=v_comparison_hash,issue_set_hash=v_issue_hash,
      state=case when v_publication.correction_session_id is null
        then 'CURRENT' else 'CORRECTION_READY' end,
      ready_session_version=v_ready_session_version,
      failure_code=null,published_at_utc=pg_catalog.transaction_timestamp()
  where id=v_publication.id;
  if v_publication.correction_session_id is not null then
    if v_publication.rebuild_idempotency_key is null then
      update public.weekly_final_source_correction_sessions
      set replacement_projection_publication_id=v_publication.id,
          version=version+1,updated_at_utc=pg_catalog.transaction_timestamp()
      where id=v_publication.correction_session_id and state='READY'
        and version=v_correction.version
        and replacement_correction_upload_id=v_publication.upload_id
        and replacement_projection_publication_id is null;
    else
      update public.weekly_final_source_correction_sessions
      set replacement_projection_publication_id=v_publication.id,
          state='READY',review_idempotency_key=null,review_request_hash=null,
          review_result_json=null,review_result_hash=null,
          version=version+1,updated_at_utc=pg_catalog.transaction_timestamp()
      where id=v_publication.correction_session_id and state='REVIEWED'
        and version=v_publication.ready_session_version
        and replacement_correction_upload_id=v_publication.upload_id
        and replacement_projection_publication_id is not null
        and replacement_projection_publication_id<>v_publication.id;
    end if;
    if not found then
      raise exception 'WEEKLY_SOURCE_CORRECTION_PUBLICATION_CAS_LOST' using errcode='40001';
    end if;
  elsif v_publication.authority_scope_kind='CYCLE' then
    update public.weekly_source_cycles
    set projection_state='CURRENT',current_projection_publication_id=v_publication.id
    where id=v_cycle.id and current_complete_upload_id=v_publication.upload_id
      and version=v_publication.authority_scope_version;
  else
    update public.weekly_source_report_scopes
    set projection_state='CURRENT',current_projection_publication_id=v_publication.id,
        updated_at_utc=pg_catalog.transaction_timestamp()
    where id=v_scope.id and current_complete_upload_id=v_publication.upload_id
      and version=v_publication.authority_scope_version;
  end if;
  if not found then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_CAS_LOST' using errcode='40001';
  end if;
  v_fingerprint:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_PUBLICATION_FINGERPRINT_V1',
    pg_catalog.jsonb_build_object(
      'publication_id',v_publication.id,'upload_id',v_publication.upload_id,
      'authority_scope_version',v_publication.authority_scope_version,
      'comparison_manifest_hash',pg_catalog.encode(v_comparison_hash,'hex'),
      'issue_set_hash',pg_catalog.encode(v_issue_hash,'hex')
    )
  );
  return pg_catalog.jsonb_build_object(
    'ok',true,'status',case when v_publication.correction_session_id is null
      then 'CURRENT' else 'CORRECTION_READY' end,
    'publication_id',v_publication.id,
    'upload_id',v_publication.upload_id,
    'authority_scope_version',v_publication.authority_scope_version,
    'projection_generation',v_publication.projection_generation,
    'ready_session_version',v_ready_session_version,
    'comparison_manifest_hash',pg_catalog.encode(v_comparison_hash,'hex'),
    'issue_set_hash',pg_catalog.encode(v_issue_hash,'hex'),
    'publication_fingerprint',pg_catalog.encode(v_fingerprint,'hex'),
    'idempotent',false
  );
end;
$function$;

create or replace function private.weekly_source_current_publication_guard_v1(
  p_source_cycle_id uuid,
  p_authority_scope_kind text,
  p_report_scope_id uuid,
  p_upload_id uuid,
  p_publication_id uuid,
  p_authority_scope_version bigint
) returns jsonb
language plpgsql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_cycle public.weekly_source_cycles%rowtype;
  v_scope public.weekly_source_report_scopes%rowtype;
  v_upload public.weekly_source_uploads%rowtype;
  v_publication public.weekly_source_projection_publications%rowtype;
  v_profile public.weekly_source_format_profiles%rowtype;
  v_fingerprint bytea;
begin
  if p_source_cycle_id is null or p_upload_id is null or p_publication_id is null
     or p_authority_scope_version is null or p_authority_scope_version<1
     or p_authority_scope_kind not in ('CYCLE','NHSP_REPORT_SCOPE')
     or (p_authority_scope_kind='CYCLE' and p_report_scope_id is not null)
     or (p_authority_scope_kind='NHSP_REPORT_SCOPE' and p_report_scope_id is null) then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_GUARD_INPUT_INVALID' using errcode='22023';
  end if;
  select * into v_cycle
  from public.weekly_source_cycles where id=p_source_cycle_id;
  if not found then
    raise exception 'WEEKLY_SOURCE_CYCLE_NOT_FOUND' using errcode='22023';
  end if;
  if p_authority_scope_kind='CYCLE' then
    if v_cycle.current_complete_upload_id is distinct from p_upload_id
       or v_cycle.current_projection_publication_id is distinct from p_publication_id
       or v_cycle.version<>p_authority_scope_version
       or v_cycle.projection_state<>'CURRENT' then
      raise exception 'SOURCE_CHECK_IN_PROGRESS' using errcode='55000';
    end if;
  else
    select * into v_scope
    from public.weekly_source_report_scopes
    where id=p_report_scope_id;
    if not found or v_scope.source_cycle_id is distinct from p_source_cycle_id
       or v_scope.current_complete_upload_id is distinct from p_upload_id
       or v_scope.current_projection_publication_id is distinct from p_publication_id
       or v_scope.version<>p_authority_scope_version
       or v_scope.projection_state<>'CURRENT' then
      raise exception 'SOURCE_CHECK_IN_PROGRESS' using errcode='55000';
    end if;
  end if;
  select * into v_upload
  from public.weekly_source_uploads where id=p_upload_id;
  select * into v_publication
  from public.weekly_source_projection_publications where id=p_publication_id;
  if not found or v_upload.state<>'CURRENT' or v_upload.row_manifest_hash is null
     or v_upload.source_cycle_id is distinct from p_source_cycle_id
     or v_upload.report_scope_id is distinct from p_report_scope_id
     or v_publication.state<>'CURRENT'
     or v_publication.source_cycle_id is distinct from p_source_cycle_id
     or v_publication.authority_scope_kind is distinct from p_authority_scope_kind
     or v_publication.report_scope_id is distinct from p_report_scope_id
     or v_publication.upload_id is distinct from p_upload_id
     or v_publication.authority_scope_version<>p_authority_scope_version then
    raise exception 'WEEKLY_SOURCE_PREVIEW_STALE' using errcode='55000';
  end if;
  select * into strict v_profile
  from public.weekly_source_format_profiles where id=v_upload.source_format_profile_id;
  if v_profile.profile_code='NHSP_PREFINAL_RELEASED_V1'
     or v_profile.profile_json->>'purpose'='PREFINAL_CHECKING'
     or v_profile.row_finalisation_capability='CHECKING_ONLY' then
    raise exception 'WEEKLY_SOURCE_PREFINAL_NOT_FINAL_AUTHORITY' using errcode='55000';
  end if;
  v_fingerprint:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_CURRENT_PUBLICATION_GUARD_V1',
    pg_catalog.jsonb_build_object(
      'source_cycle_id',p_source_cycle_id,'authority_scope_kind',p_authority_scope_kind,
      'report_scope_id',p_report_scope_id,'upload_id',p_upload_id,
      'publication_id',p_publication_id,'authority_scope_version',p_authority_scope_version,
      'row_manifest_hash',pg_catalog.encode(v_upload.row_manifest_hash,'hex'),
      'comparison_manifest_hash',pg_catalog.encode(v_publication.comparison_manifest_hash,'hex'),
      'issue_set_hash',pg_catalog.encode(v_publication.issue_set_hash,'hex')
    )
  );
  return pg_catalog.jsonb_build_object(
    'ok',true,'source_cycle_id',p_source_cycle_id,
    'authority_scope_kind',p_authority_scope_kind,'report_scope_id',p_report_scope_id,
    'upload_id',p_upload_id,'publication_id',p_publication_id,
    'authority_scope_version',p_authority_scope_version,
    'profile_code',v_profile.profile_code,'profile_version',v_profile.version,
    'row_manifest_hash',pg_catalog.encode(v_upload.row_manifest_hash,'hex'),
    'comparison_manifest_hash',pg_catalog.encode(v_publication.comparison_manifest_hash,'hex'),
    'issue_set_hash',pg_catalog.encode(v_publication.issue_set_hash,'hex'),
    'guard_fingerprint',pg_catalog.encode(v_fingerprint,'hex')
  );
end;
$function$;

alter function private.weekly_source_hex32_v1(text,text) owner to postgres;
alter function private.weekly_source_canonical_report_number_v1(text) owner to postgres;
alter function private.weekly_source_upload_attempt_append_v1(uuid,uuid,text,text) owner to postgres;
alter function private.weekly_source_attempt_append_without_upload_v1(
  text,uuid,uuid,uuid,uuid,text,bytea,uuid,uuid,text,bigint,bytea,text,text,text,text,text,uuid
) owner to postgres;
alter function private.weekly_source_upload_manifest_v1(uuid) owner to postgres;
alter function private.weekly_source_upload_manifest_hash_v1(uuid) owner to postgres;
alter function private.weekly_source_projection_comparison_manifest_v1(uuid) owner to postgres;
alter function private.weekly_source_projection_comparison_manifest_hash_v1(uuid) owner to postgres;
alter function private.weekly_source_projection_issue_set_v1(uuid) owner to postgres;
alter function private.weekly_source_projection_issue_set_hash_v1(uuid) owner to postgres;
alter function private.weekly_source_upload_seal_core_v1(uuid,uuid) owner to postgres;
alter function private.weekly_source_current_publication_guard_v1(
  uuid,text,uuid,uuid,uuid,bigint
) owner to postgres;

alter function public.weekly_source_upload_attempt_record_atomic_v1(jsonb) owner to postgres;
alter function public.weekly_source_upload_stage_begin_atomic_v1(jsonb) owner to postgres;
alter function public.weekly_source_upload_stage_rows_atomic_v1(jsonb) owner to postgres;
alter function public.weekly_source_upload_seal_atomic_v1(jsonb) owner to postgres;
alter function public.weekly_source_upload_abort_atomic_v1(jsonb) owner to postgres;
alter function public.weekly_source_projection_begin_atomic_v1(jsonb) owner to postgres;
alter function public.weekly_source_projection_publish_atomic_v1(jsonb) owner to postgres;

revoke all on function private.weekly_source_hex32_v1(text,text)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_canonical_report_number_v1(text)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_upload_attempt_append_v1(uuid,uuid,text,text)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_attempt_append_without_upload_v1(
  text,uuid,uuid,uuid,uuid,text,bytea,uuid,uuid,text,bigint,bytea,text,text,text,text,text,uuid
) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_upload_manifest_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_upload_manifest_hash_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_projection_comparison_manifest_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_projection_comparison_manifest_hash_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_projection_issue_set_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_projection_issue_set_hash_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_upload_seal_core_v1(uuid,uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_current_publication_guard_v1(
  uuid,text,uuid,uuid,uuid,bigint
) from public,anon,authenticated,service_role;

revoke all on function public.weekly_source_upload_attempt_record_atomic_v1(jsonb)
  from public,anon,authenticated;
revoke all on function public.weekly_source_upload_stage_begin_atomic_v1(jsonb)
  from public,anon,authenticated;
revoke all on function public.weekly_source_upload_stage_rows_atomic_v1(jsonb)
  from public,anon,authenticated;
revoke all on function public.weekly_source_upload_seal_atomic_v1(jsonb)
  from public,anon,authenticated;
revoke all on function public.weekly_source_upload_abort_atomic_v1(jsonb)
  from public,anon,authenticated;
revoke all on function public.weekly_source_projection_begin_atomic_v1(jsonb)
  from public,anon,authenticated;
revoke all on function public.weekly_source_projection_publish_atomic_v1(jsonb)
  from public,anon,authenticated;

grant execute on function public.weekly_source_upload_attempt_record_atomic_v1(jsonb) to service_role;
grant execute on function public.weekly_source_upload_stage_begin_atomic_v1(jsonb) to service_role;
grant execute on function public.weekly_source_upload_stage_rows_atomic_v1(jsonb) to service_role;
grant execute on function public.weekly_source_upload_seal_atomic_v1(jsonb) to service_role;
grant execute on function public.weekly_source_upload_abort_atomic_v1(jsonb) to service_role;
grant execute on function public.weekly_source_projection_begin_atomic_v1(jsonb) to service_role;
grant execute on function public.weekly_source_projection_publish_atomic_v1(jsonb) to service_role;

select pg_catalog.pg_notify('pgrst','reload schema');

commit;
