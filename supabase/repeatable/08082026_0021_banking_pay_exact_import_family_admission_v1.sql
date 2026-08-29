-- Exact NHSP/HealthRoster Weekly import-family admission for targeted Banking Pay delta.
-- This helper proves family and occurrence identity only; it performs no pay calculation or mutation.

CREATE OR REPLACE FUNCTION private.pay_workbench_exact_import_family_admission_v1(
  p_session_id uuid,
  p_candidate_id uuid,
  p_seed_timesheet_ids uuid[],
  p_source_change_seq bigint
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
  v_settings public.settings_defaults%ROWTYPE;
  v_seeds uuid[]:=ARRAY[]::uuid[];
  v_family uuid[]:=ARRAY[]::uuid[];
  v_closure jsonb:='{}'::jsonb;
  v_route_count integer:=0;
  v_route text:=NULL::text;
  v_member_count integer:=0;
  v_bad_count integer:=0;
  v_evidence_count integer:=0;
  v_occurrence_count integer:=0;
  v_tsfin_digest text:=NULL::text;
  v_occurrence_digest text:=NULL::text;
BEGIN
  IF p_session_id IS NULL OR p_candidate_id IS NULL OR p_source_change_seq IS NULL THEN
    RETURN pg_catalog.jsonb_build_object('ok',true,'admitted',false,'reason','EXACT_IMPORT_ARGUMENT_REQUIRED');
  END IF;

  SELECT settings_row.* INTO v_settings
  FROM public.settings_defaults AS settings_row WHERE settings_row.id=1;
  IF coalesce(v_settings.banking_pay_workbench_delta_enable_exact_import_family,false) IS NOT TRUE THEN
    RETURN pg_catalog.jsonb_build_object('ok',true,'admitted',false,'reason','EXACT_IMPORT_FAMILY_DISABLED');
  END IF;

  SELECT coalesce(pg_catalog.array_agg(DISTINCT seed ORDER BY seed),ARRAY[]::uuid[])
  INTO v_seeds
  FROM pg_catalog.unnest(coalesce(p_seed_timesheet_ids,ARRAY[]::uuid[])) AS seed
  WHERE seed IS NOT NULL;

  IF coalesce(pg_catalog.array_length(v_seeds,1),0)=0
     OR pg_catalog.array_length(v_seeds,1)>250 THEN
    RETURN pg_catalog.jsonb_build_object('ok',true,'admitted',false,'reason','EXACT_IMPORT_SEED_INVALID');
  END IF;

  v_closure:=public._pay_workbench_refresh_dependency_closure_v1(
    p_candidate_id,v_seeds,ARRAY[]::uuid[],ARRAY[]::uuid[],250,100
  );
  IF coalesce((v_closure->>'coverage_complete')::boolean,false) IS NOT TRUE
     OR coalesce((v_closure->>'requires_full_candidate')::boolean,true) THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok',true,'admitted',false,
      'reason',coalesce(NULLIF(v_closure->>'fallback_reason',''),'EXACT_IMPORT_FAMILY_INCOMPLETE')
    );
  END IF;

  SELECT coalesce(pg_catalog.array_agg(DISTINCT value::uuid ORDER BY value::uuid),ARRAY[]::uuid[])
  INTO v_family
  FROM pg_catalog.jsonb_array_elements_text(
    coalesce(v_closure->'effective_targeted_timesheet_ids','[]'::jsonb)
  ) AS family_value(value)
  WHERE value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

  IF coalesce(pg_catalog.array_length(v_family,1),0)=0
     OR pg_catalog.array_length(v_family,1)>250 THEN
    RETURN pg_catalog.jsonb_build_object('ok',true,'admitted',false,'reason','EXACT_IMPORT_FAMILY_INVALID');
  END IF;

  SELECT pg_catalog.count(*)::integer,
         pg_catalog.count(DISTINCT pg_catalog.upper(contract_row.weekly_timesheet_source::text))::integer,
         pg_catalog.min(pg_catalog.upper(contract_row.weekly_timesheet_source::text))
  INTO v_member_count,v_route_count,v_route
  FROM public.timesheets AS timesheet_row
  JOIN public.contracts AS contract_row ON contract_row.id=timesheet_row.contract_id
  WHERE timesheet_row.timesheet_id=ANY(v_family)
    AND timesheet_row.is_current IS TRUE
    AND contract_row.candidate_id=p_candidate_id
    AND pg_catalog.upper(contract_row.weekly_timesheet_source::text) IN ('NHSP','HEALTHROSTER');

  IF v_member_count<>pg_catalog.array_length(v_family,1) OR v_route_count<>1 THEN
    RETURN pg_catalog.jsonb_build_object('ok',true,'admitted',false,'reason','EXACT_IMPORT_ROUTE_OR_OWNERSHIP_MISMATCH');
  END IF;

  SELECT pg_catalog.count(*)::integer
  INTO v_bad_count
  FROM public.timesheets AS timesheet_row
  JOIN public.timesheets_financials AS financial
    ON financial.timesheet_id=timesheet_row.timesheet_id
   AND financial.is_current IS TRUE
  WHERE timesheet_row.timesheet_id=ANY(v_family)
    AND (
      timesheet_row.correction_id IS NOT NULL
      OR timesheet_row.parent_timesheet_id IS NOT NULL
      OR coalesce(timesheet_row.is_adjustment,false)
      OR coalesce((public._ctms_import_correction_classify_v1(timesheet_row.timesheet_id)
                    ->>'is_import_authoritative_correction')::boolean,false)
      OR (v_route='NHSP' AND (
            pg_catalog.upper(financial.basis::text)<>'NHSP'
            OR financial.nhsp_import_id IS NULL
          ))
      OR (v_route='HEALTHROSTER' AND (
            pg_catalog.upper(financial.basis::text) NOT IN ('HR_VALIDATED','HEALTHROSTER_SELF_BILL')
            OR pg_catalog.jsonb_typeof(coalesce(financial.external_source_rows_json,'[]'::jsonb))<>'array'
          ))
    );

  IF v_bad_count>0 OR (
    SELECT pg_catalog.count(*) FROM public.timesheets_financials AS financial
    WHERE financial.timesheet_id=ANY(v_family) AND financial.is_current IS TRUE
  )<>pg_catalog.array_length(v_family,1) THEN
    RETURN pg_catalog.jsonb_build_object('ok',true,'admitted',false,'reason','EXACT_IMPORT_MEMBER_UNSUPPORTED');
  END IF;

  SELECT pg_catalog.count(*)::integer,
         pg_catalog.encode(extensions.digest(pg_catalog.convert_to(
           coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
             'timesheet_id',financial.timesheet_id,
             'timesheet_version',financial.timesheet_version,
             'basis',financial.basis,
             'nhsp_import_id',financial.nhsp_import_id,
             'external_source_rows_json',financial.external_source_rows_json,
             'actual_schedule_json',financial.actual_schedule_json
           ) ORDER BY financial.timesheet_id)::text,'[]'),'UTF8'),'sha256'),'hex'),
         pg_catalog.encode(extensions.digest(pg_catalog.convert_to(
           coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
             'timesheet_id',financial.timesheet_id,
             'actual_schedule_json',financial.actual_schedule_json
           ) ORDER BY financial.timesheet_id)::text,'[]'),'UTF8'),'sha256'),'hex')
  INTO v_member_count,v_tsfin_digest,v_occurrence_digest
  FROM public.timesheets_financials AS financial
  WHERE financial.timesheet_id=ANY(v_family) AND financial.is_current IS TRUE;

  SELECT pg_catalog.count(DISTINCT family_member.timesheet_id)::integer
  INTO v_evidence_count
  FROM pg_catalog.unnest(v_family) AS family_member(timesheet_id)
  CROSS JOIN LATERAL public.timesheet_import_rows_for_timesheet_current(
    family_member.timesheet_id,false,NULL::uuid,NULL::uuid
  ) AS import_evidence
  WHERE pg_catalog.upper(coalesce(import_evidence.source_system,''))=v_route
    AND pg_catalog.jsonb_typeof(coalesce(import_evidence.rows,'[]'::jsonb))='array'
    AND pg_catalog.jsonb_array_length(coalesce(import_evidence.rows,'[]'::jsonb))>0;

  SELECT coalesce(pg_catalog.sum(
    CASE
      WHEN pg_catalog.jsonb_typeof(coalesce(financial.actual_schedule_json,'[]'::jsonb))='array'
        THEN pg_catalog.jsonb_array_length(coalesce(financial.actual_schedule_json,'[]'::jsonb))
      ELSE 0
    END
  ),0)::integer
  INTO v_occurrence_count
  FROM public.timesheets_financials AS financial
  WHERE financial.timesheet_id=ANY(v_family) AND financial.is_current IS TRUE;

  IF v_evidence_count<pg_catalog.array_length(v_family,1) OR v_occurrence_count<=0 THEN
    RETURN pg_catalog.jsonb_build_object('ok',true,'admitted',false,'reason','EXACT_IMPORT_OCCURRENCE_EVIDENCE_INCOMPLETE');
  END IF;

  RETURN pg_catalog.jsonb_build_object(
    'ok',true,'admitted',true,'reason','EXACT_IMPORT_FAMILY_ADMITTED',
    'family_kind',CASE WHEN v_route='NHSP' THEN 'NHSP_WEEKLY' ELSE 'HEALTHROSTER_WEEKLY' END,
    'family_timesheet_ids',pg_catalog.to_jsonb(v_family),
    'family_count',pg_catalog.array_length(v_family,1),
    'tsfin_revision_digest',v_tsfin_digest,
    'occurrence_count',v_occurrence_count,
    'occurrence_digest',v_occurrence_digest,
    'import_evidence_count',v_evidence_count,
    'source_change_seq',p_source_change_seq
  );
END;
$function$;

ALTER FUNCTION private.pay_workbench_exact_import_family_admission_v1(uuid,uuid,uuid[],bigint) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_exact_import_family_admission_v1(uuid,uuid,uuid[],bigint) FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_exact_import_family_admission_v1(uuid,uuid,uuid[],bigint) TO postgres;
