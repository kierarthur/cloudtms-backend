-- Banking Pay bounded-scope V1.2.10: one exact private source-build stage per
-- durable RPC-1 attempt, with sealed physical-source completeness. No ordinary
-- candidate-lifetime seed or 100-row scope cap.

CREATE OR REPLACE FUNCTION public.pay_workbench_candidate_source_build_chunk(
  p_session_id uuid,
  p_candidate_id uuid,
  p_cursor_json jsonb DEFAULT NULL::jsonb,
  p_payload_json jsonb DEFAULT '{}'::jsonb,
  p_limit integer DEFAULT 100
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
PARALLEL UNSAFE
SECURITY DEFINER
SET search_path = ''
SET plpgsql_check.mode TO 'disabled'
AS $function$
DECLARE
  v_cursor jsonb:=COALESCE(p_cursor_json,'{}'::jsonb);
  v_payload jsonb:=CASE WHEN jsonb_typeof(COALESCE(p_payload_json,'{}'::jsonb))='object'
    THEN COALESCE(p_payload_json,'{}'::jsonb) ELSE '{}'::jsonb END;
  v_build_id uuid:=NULLIF(v_payload->>'economic_build_id','')::uuid;
  v_attempt_id uuid:=NULLIF(v_payload->>'attempt_id','')::uuid;
  v_attempt_nonce uuid:=NULLIF(v_payload->>'attempt_nonce','')::uuid;
  v_stage text:=upper(COALESCE(NULLIF(v_payload->>'private_stage',''),NULLIF(v_cursor->>'cursor_kind','')));
  v_build private.banking_pay_workbench_economic_builds%ROWTYPE;
  v_registry private.banking_pay_workbench_candidate_scope_registry%ROWTYPE;
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_limit integer:=LEAST(GREATEST(COALESCE(p_limit,25),1),500);
  -- Fact collection is intentionally capped independently of callers.  The
  -- complete candidate remains unbounded through durable continuation pages.
  v_fact_limit integer:=LEAST(GREATEST(COALESCE(p_limit,25),1),25);
  v_result jsonb;
  v_next jsonb;
  v_unit_key text:=NULLIF(v_cursor->>'dependency_unit_key','');
  v_fact_family text:=NULLIF(v_cursor->>'fact_family','');
  v_last_source_key text:=NULLIF(v_cursor->>'last_source_key','');
  v_page_number integer:=GREATEST(COALESCE((v_cursor->>'page_number')::integer,1),1);
  v_page_count integer:=0;
  v_page_digest text;
  v_cumulative_fact_count integer:=GREATEST(COALESCE((v_cursor->>'cumulative_fact_count')::integer,0),0);
  v_next_cumulative_fact_count integer:=0;
  v_cumulative_digest text:=COALESCE(NULLIF(v_cursor->>'cumulative_digest',''),md5('BPAY_FACT_STREAM_V2'));
  v_raw_physical_source_count bigint:=GREATEST(COALESCE((v_cursor->>'raw_physical_source_count')::bigint,0),0);
  v_resolved_physical_source_count bigint:=GREATEST(COALESCE((v_cursor->>'resolved_physical_source_count')::bigint,0),0);
  v_failed_physical_source_count bigint:=GREATEST(COALESCE((v_cursor->>'failed_physical_source_count')::bigint,0),0);
  v_page_physical_source_count bigint:=0;
  v_page_resolved_source_count bigint:=0;
  v_page_failed_source_count bigint:=0;
  v_next_raw_physical_source_count bigint:=0;
  v_next_resolved_physical_source_count bigint:=0;
  v_next_failed_physical_source_count bigint:=0;
  v_raw_physical_amount_ex_vat numeric:=COALESCE((v_cursor->>'raw_physical_amount_ex_vat')::numeric,0);
  v_resolved_physical_amount_ex_vat numeric:=COALESCE((v_cursor->>'resolved_physical_amount_ex_vat')::numeric,0);
  v_page_physical_amount_ex_vat numeric:=0;
  v_page_resolved_amount_ex_vat numeric:=0;
  v_next_raw_physical_amount_ex_vat numeric:=0;
  v_next_resolved_physical_amount_ex_vat numeric:=0;
  v_expected_source_count integer;
  v_raw_source_has_more boolean:=false;
  v_has_more boolean:=false;
  v_is_final boolean:=false;
  v_last_page_key text;
  v_last_raw_physical_source_key text:=NULLIF(v_cursor->>'last_raw_physical_source_key','');
  v_next_last_raw_physical_source_key text;
  v_cursor_start_hash text;
  v_cursor_end_hash text;
  v_existing_page private.banking_pay_workbench_economic_build_fact_pages%ROWTYPE;
  v_previous_page private.banking_pay_workbench_economic_build_fact_pages%ROWTYPE;
  v_fact_page_timestamp timestamptz;
  v_replay_cursor jsonb;
  v_unit_ids uuid[]:=ARRAY[]::uuid[];
  v_family_ordinal integer;
  v_unit_families text[]:=ARRAY[
    'LIVE_ENTITLEMENT_INPUT','FROZEN_SETTLED_COMPONENT','PAY_STATE_FALLBACK',
    'ENTITLEMENT_COMPONENT','PAYEE_BASELINE_INPUT','CANONICAL_INPUT'
  ];
  v_global_families text[]:=ARRAY[
    'FINANCE_ITEM_AUTHORITY','RESERVATION_COMPONENT','FINANCE_CASE_IDENTITY','FINANCE_COMPONENT_IDENTITY',
    'PROTECTION_EVIDENCE','ALLOCATION_INPUT'
  ];
  v_scope_inserted integer:=0;
  v_derived_fact_count integer:=0;
  v_derived_settled_count integer:=0;
  v_derived_fallback_count integer:=0;
  v_live_page_ids uuid[]:=ARRAY[]::uuid[];
  v_input_phase text:=upper(COALESCE(NULLIF(v_cursor->>'input_phase',''),'PROJECTION'));
  v_input_projection_id uuid:=NULLIF(v_cursor->>'input_projection_id','')::uuid;
  v_next_projection_id uuid;
  v_projection_has_more boolean:=false;
  v_projection_phase_transition boolean:=false;
  v_projection_family_transition boolean:=false;
  v_occurrence_bundle jsonb:='{}'::jsonb;
  v_resolution_failure text;
  v_vector jsonb;
  v_envelope_version integer;
  v_envelope jsonb;
  v_envelope_evidence jsonb;
  v_vector_item record;
  v_scale_blocked boolean:=false;
  v_sync_result jsonb;
  v_effect_plan jsonb:='[]'::jsonb;
  v_effect_plan_digest text:=md5('');
  v_effect_plan_count integer:=0;
  v_publish_now boolean:=false;
  v_published_count integer:=0;
  v_published_digest text;
  v_candidate_pay_method text;
  v_actor_user_id uuid;
  v_expected_count integer;
  v_cleanup_kind text;
  v_cleanup_count integer:=0;
  v_bootstrap_stream text;
  v_bootstrap_next_stream text;
  v_bootstrap_last_key text;
  v_bootstrap_page_count integer:=0;
  v_bootstrap_registered integer:=0;
  v_bootstrap_has_more boolean:=false;
  v_bootstrap_has_dirty boolean:=false;
  v_bootstrap_classification_phase text;
  v_bootstrap_unit_key text;
  v_bootstrap_last_unit_key text;
  v_bootstrap_last_ordinal bigint:=0;
  v_bootstrap_unit_relevant boolean:=false;
  v_bootstrap_page_relevant boolean:=false;
  v_bootstrap_raw_current_count bigint:=0;
  v_bootstrap_resolved_current_count bigint:=0;
  v_bootstrap_raw_baseline_count bigint:=0;
  v_bootstrap_resolved_baseline_count bigint:=0;
  v_bootstrap_page_raw_current_count bigint:=0;
  v_bootstrap_page_resolved_current_count bigint:=0;
  v_bootstrap_page_raw_baseline_count bigint:=0;
  v_bootstrap_page_resolved_baseline_count bigint:=0;
  v_bootstrap_evidence_complete boolean:=false;
  v_bootstrap_reset_count integer:=0;
  v_bootstrap_id uuid;
BEGIN
  IF v_build_id IS NULL OR v_attempt_id IS NULL OR v_attempt_nonce IS NULL
     OR jsonb_typeof(v_cursor)<>'object' OR p_session_id IS NULL OR p_candidate_id IS NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_BUILD_CURSOR_INVALID' USING ERRCODE='22023';
  END IF;
  PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    public._pay_workbench_candidate_serial_key(p_candidate_id),24062027));
  SELECT * INTO v_registry FROM private.banking_pay_workbench_candidate_scope_registry
  WHERE candidate_id=p_candidate_id FOR UPDATE;
  SELECT * INTO v_build FROM private.banking_pay_workbench_economic_builds
  WHERE id=v_build_id FOR UPDATE;
  SELECT * INTO v_session FROM public.banking_pay_workbench_sessions
  WHERE id=p_session_id FOR UPDATE;
  IF v_registry.current_build_id IS DISTINCT FROM v_build_id
     OR v_build.candidate_id IS DISTINCT FROM p_candidate_id
     OR v_build.session_id IS DISTINCT FROM p_session_id
     OR v_build.session_version IS DISTINCT FROM v_session.version
     OR v_registry.dirty_generation IS DISTINCT FROM v_build.captured_candidate_generation
     OR v_registry.current_source_change_seq IS DISTINCT FROM v_build.source_change_seq
     OR v_build.private_stage IS DISTINCT FROM v_stage THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_BUILD_NOT_CURRENT' USING ERRCODE='40001';
  END IF;
  IF NOT EXISTS(
    SELECT 1 FROM private.banking_pay_workbench_stage_attempts attempt
    WHERE attempt.id=v_attempt_id AND attempt.build_id=v_build_id
      AND attempt.attempt_nonce=v_attempt_nonce AND attempt.private_stage=v_stage
      AND attempt.attempt_status='STARTED' AND clock_timestamp()<attempt.lease_expires_at_utc
  ) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_RECONCILIATION_ATTEMPT_STALE' USING ERRCODE='40001';
  END IF;

  IF v_stage='PREPARE_SCOPE' THEN
    v_result:=private.pay_workbench_candidate_bounded_scope_v1(
      v_build_id,p_candidate_id,v_cursor,LEAST(v_limit,500),
      ARRAY(SELECT value::uuid FROM jsonb_array_elements_text(COALESCE(v_payload->'targeted_timesheet_ids','[]'::jsonb)) value
        WHERE value ~* '^[0-9a-f-]{36}$' ORDER BY value::uuid),
      ARRAY(SELECT value::uuid FROM jsonb_array_elements_text(COALESCE(v_payload->'force_include_timesheet_ids','[]'::jsonb)) value
        WHERE value ~* '^[0-9a-f-]{36}$' ORDER BY value::uuid),
      ARRAY(SELECT value::uuid FROM jsonb_array_elements_text(COALESCE(v_payload->'exclude_timesheet_ids','[]'::jsonb)) value
        WHERE value ~* '^[0-9a-f-]{36}$' ORDER BY value::uuid),
      NULLIF(v_payload->>'client_filter_single','')::uuid
    );
    RETURN v_result||jsonb_build_object(
      'private_stage',(SELECT private_stage FROM private.banking_pay_workbench_economic_builds WHERE id=v_build_id),
      'stage_status','PREPARING','continuation_enqueued',false,
      'next_action',(SELECT private_stage FROM private.banking_pay_workbench_economic_builds WHERE id=v_build_id)
    );
  END IF;

  IF v_stage='DEPENDENCY_CLOSURE' THEN
    v_result:=private.pay_workbench_timesheet_dependency_closure_v2(v_build_id,v_cursor,LEAST(v_limit,25));
    RETURN v_result||jsonb_build_object(
      'private_stage',(SELECT private_stage FROM private.banking_pay_workbench_economic_builds WHERE id=v_build_id),
      'stage_status','PREPARING','continuation_enqueued',false,
      'next_action',(SELECT private_stage FROM private.banking_pay_workbench_economic_builds WHERE id=v_build_id)
    );
  END IF;

  IF v_stage='WORKSPACE_FACT' THEN
    -- Version 1.2.9: validate but never reconstruct a fact cursor.  The exact
    -- committed page-end JSON is the next invocation's page-start authority.
    v_cursor:=private.pay_workbench_fact_cursor_preserve_v2(v_cursor);
    IF NULLIF(v_cursor->>'build_id','')::uuid IS DISTINCT FROM v_build_id
       OR NULLIF(v_cursor->>'candidate_id','')::uuid IS DISTINCT FROM p_candidate_id
       OR COALESCE((v_cursor->>'captured_candidate_generation')::bigint,-1)
          IS DISTINCT FROM v_build.captured_candidate_generation
       OR COALESCE((v_cursor->>'captured_source_change_seq')::bigint,-1)
          IS DISTINCT FROM v_build.source_change_seq
       OR NULLIF(v_cursor->>'dependency_unit_key','') IS NULL
       OR NULLIF(v_cursor->>'fact_family','') IS NULL THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_BUILD_CURSOR_INVALID' USING ERRCODE='22023';
    END IF;
    IF v_build.dependency_closure_sealed_at_utc IS NULL OR EXISTS(
      SELECT 1 FROM private.banking_pay_workbench_economic_build_scope
      WHERE build_id=v_build_id AND closure_status<>'SEALED' LIMIT 1
    ) THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_RECONCILIATION_FACTS_INCOMPLETE' USING ERRCODE='23514';
    END IF;

    IF v_unit_key IS NULL THEN
      IF NOT EXISTS(SELECT 1
        FROM private.banking_pay_workbench_economic_build_fact_pages page
        WHERE page.build_id=v_build_id AND page.dependency_unit_key='GLOBAL'
          AND page.fact_family='FINANCE_ITEM_AUTHORITY'
          AND page.status='COMPLETED' AND page.is_family_final LIMIT 1) THEN
        v_unit_key:='GLOBAL';
        v_fact_family:='FINANCE_ITEM_AUTHORITY';
      ELSE
        SELECT dependency_unit_key INTO v_unit_key
        FROM private.banking_pay_workbench_economic_build_scope
        WHERE build_id=v_build_id
          AND NOT required_fact_families <@ completed_fact_families
        ORDER BY stable_ordinal LIMIT 1;
        IF v_unit_key IS NULL THEN v_unit_key:='GLOBAL'; END IF;
      END IF;
    END IF;
    IF v_fact_family IS NULL THEN
      v_fact_family:=CASE WHEN v_unit_key='GLOBAL' THEN v_global_families[1] ELSE v_unit_families[1] END;
    END IF;
    IF v_unit_key='GLOBAL' THEN
      v_family_ordinal:=array_position(v_global_families,v_fact_family);
    ELSE
      v_family_ordinal:=array_position(v_unit_families,v_fact_family);
      SELECT array_agg(timesheet_id ORDER BY stable_ordinal) INTO v_unit_ids
      FROM private.banking_pay_workbench_economic_build_scope
      WHERE build_id=v_build_id AND dependency_unit_key=v_unit_key;
    END IF;
    IF v_family_ordinal IS NULL THEN RAISE EXCEPTION 'PAY_WORKBENCH_BUILD_CURSOR_INVALID' USING ERRCODE='22023'; END IF;
    IF (v_unit_key='GLOBAL' AND v_input_phase IS DISTINCT FROM 'PHYSICAL_SOURCE')
       OR (v_unit_key<>'GLOBAL' AND v_fact_family='LIVE_ENTITLEMENT_INPUT'
         AND v_input_phase NOT IN ('PROJECTION','COMPONENTS'))
       OR (v_unit_key<>'GLOBAL' AND v_fact_family<>'LIVE_ENTITLEMENT_INPUT'
         AND v_input_phase IS DISTINCT FROM 'COMPONENTS') THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_BUILD_CURSOR_INVALID' USING ERRCODE='22023';
    END IF;
    IF v_fact_family IN ('FROZEN_SETTLED_COMPONENT','PAY_STATE_FALLBACK')
       OR (v_fact_family='LIVE_ENTITLEMENT_INPUT' AND v_input_phase='COMPONENTS') THEN
      IF v_input_phase<>'COMPONENTS' THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_BUILD_CURSOR_INVALID' USING ERRCODE='22023';
      END IF;
      IF v_input_projection_id IS NULL THEN
        SELECT projection.timesheet_id INTO v_input_projection_id
        FROM private.banking_pay_workbench_economic_build_facts projection
        WHERE projection.build_id=v_build_id
          AND projection.fact_family='LIVE_ENTITLEMENT_INPUT'
          AND projection.dependency_unit_key=v_unit_key
          AND projection.source_relation='UNIT_PROJECTION'
        ORDER BY projection.timesheet_id,projection.natural_key LIMIT 1;
      END IF;
      IF v_input_projection_id IS NULL THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_UNIT_PROJECTION_INCOMPLETE' USING ERRCODE='23514';
      END IF;
    END IF;
    CREATE TEMP TABLE IF NOT EXISTS pg_temp._bpay_wb_fact_page_v1(
      source_key text PRIMARY KEY,natural_key text NOT NULL,timesheet_id uuid NULL,
      subject_timesheet_ids uuid[] NOT NULL,source_relation text NOT NULL,source_id uuid NULL,
      economic_key_type text NULL,economic_key_value text NULL,
      amount_ex_vat numeric NULL,amount_inc_vat numeric NULL,
      truth_ex_vat numeric NULL,truth_inc_vat numeric NULL,
      baseline_ex_vat numeric NULL,baseline_inc_vat numeric NULL,
      reserved_source_amount numeric NULL,finance_case_id uuid NULL,
      finance_component_id uuid NULL,reservation_id uuid NULL,
      source_payload_json jsonb NOT NULL,financial_digest text NOT NULL
    ) ON COMMIT DROP;
    TRUNCATE pg_temp._bpay_wb_fact_page_v1;

    IF v_fact_family='FINANCE_ITEM_AUTHORITY' THEN
      v_occurrence_bundle:=private.pay_workbench_finance_item_authority_page_bundle_v1(
        v_build_id,
        CASE WHEN COALESCE(v_last_source_key,'') ~
          '^05:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN split_part(v_last_source_key,':',2)::uuid END,
        CASE WHEN COALESCE(v_last_source_key,'') ~
          '^05:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN split_part(v_last_source_key,':',3)::uuid END,
        v_fact_limit);
      INSERT INTO pg_temp._bpay_wb_fact_page_v1
      SELECT authority.source_key,md5('FINANCE_ITEM_AUTHORITY:'||authority.source_key),
        authority.resolved_timesheet_id,
        CASE WHEN authority.resolved_timesheet_id IS NULL THEN ARRAY[]::uuid[]
          ELSE ARRAY[authority.resolved_timesheet_id] END,
        'pay_batch_items',authority.pay_batch_item_id,
        authority.economic_key_type,authority.economic_key_value,
        authority.source_amount_ex_vat,authority.source_amount_inc_vat,
        NULL,NULL,NULL,NULL,NULL,
        authority.resolved_finance_case_id,authority.finance_component_id,NULL,
        authority.source_payload_json||jsonb_build_object(
          'authoritative_in_scope',authority.is_authoritative,
          'resolution_failure',authority.resolution_failure),
        authority.financial_digest
      FROM jsonb_to_recordset(COALESCE(v_occurrence_bundle->'rows','[]'::jsonb)) authority(
        source_key text,pay_batch_candidate_id uuid,pay_batch_item_id uuid,
        resolved_timesheet_id uuid,resolved_candidate_id uuid,resolved_finance_case_id uuid,
        finance_component_id uuid,economic_key_type text,economic_key_value text,
        source_amount_ex_vat numeric,source_amount_inc_vat numeric,is_authoritative boolean,
        source_payload_json jsonb,financial_digest text,resolution_failure text);
    ELSIF v_fact_family='LIVE_ENTITLEMENT_INPUT' AND v_input_phase='PROJECTION' THEN
      INSERT INTO pg_temp._bpay_wb_fact_page_v1
      SELECT projection.source_key,md5('UNIT_PROJECTION:'||projection.source_key),
        COALESCE(projection.projected_timesheet_id,projection.family_timesheet_id),
        ARRAY[projection.family_timesheet_id],
        'UNIT_PROJECTION',projection.family_timesheet_id,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
        NULL,NULL,NULL,NULL,
        jsonb_build_object('role','UNIT_PROJECTION','scope_family_key',projection.scope_family_key,
          'family_timesheet_id',projection.family_timesheet_id,
          'canonical_timesheet_id',projection.canonical_timesheet_id,
          'projected_timesheet_id',projection.projected_timesheet_id,
          'stable_ordinal',projection.stable_ordinal,
          'resolution_failure',CASE WHEN projection.projected_timesheet_id IS NULL
            THEN 'UNIT_PROJECTION_MISSING_CURRENT' END),
        md5(jsonb_build_object('family_timesheet_id',projection.family_timesheet_id,
          'scope_family_key',projection.scope_family_key,
          'canonical_timesheet_id',projection.canonical_timesheet_id,
          'projected_timesheet_id',projection.projected_timesheet_id,
          'stable_ordinal',projection.stable_ordinal)::text)
      FROM (
        WITH scope_page AS MATERIALIZED (
          SELECT scope_row.timesheet_id,scope_row.stable_ordinal
          FROM private.banking_pay_workbench_economic_build_scope scope_row
          WHERE scope_row.build_id=v_build_id
            AND scope_row.dependency_unit_key=v_unit_key
            AND (v_last_source_key IS NULL OR
              '00:'||LPAD(scope_row.stable_ordinal::text,20,'0')||':'||scope_row.timesheet_id::text>v_last_source_key)
          ORDER BY scope_row.stable_ordinal,scope_row.timesheet_id
          LIMIT v_fact_limit+1
        )
        SELECT scope_page.stable_ordinal,scope_page.timesheet_id AS family_timesheet_id,
          COALESCE(NULLIF(BTRIM(member.booking_id),''),scope_page.timesheet_id::text) AS scope_family_key,
          canonical.timesheet_id AS canonical_timesheet_id,
          canonical.timesheet_id AS projected_timesheet_id,
          '00:'||LPAD(scope_page.stable_ordinal::text,20,'0')||':'||scope_page.timesheet_id::text AS source_key
        FROM scope_page
        JOIN public.timesheets member ON member.timesheet_id=scope_page.timesheet_id
        LEFT JOIN LATERAL (
          SELECT current_member.timesheet_id
          FROM public.timesheets current_member
          JOIN private.banking_pay_workbench_economic_build_scope current_scope
            ON current_scope.build_id=v_build_id
           AND current_scope.dependency_unit_key=v_unit_key
           AND current_scope.timesheet_id=current_member.timesheet_id
          WHERE current_member.is_current IS TRUE
            AND current_member.revoked_at IS NULL
            AND current_member.archived_at_utc IS NULL
            AND (CASE WHEN NULLIF(BTRIM(member.booking_id),'') IS NULL
                  THEN current_member.timesheet_id=member.timesheet_id
                  ELSE current_member.booking_id=member.booking_id END)
          ORDER BY current_member.version DESC NULLS LAST,
            current_member.updated_at DESC NULLS LAST,current_member.created_at DESC NULLS LAST,
            current_member.timesheet_id
          LIMIT 1
        ) canonical ON true
        ORDER BY scope_page.stable_ordinal,scope_page.timesheet_id
      ) projection;
    ELSIF v_fact_family IN ('LIVE_ENTITLEMENT_INPUT','FROZEN_SETTLED_COMPONENT','PAY_STATE_FALLBACK') THEN
      v_occurrence_bundle:=private.pay_workbench_unit_economic_occurrence_bundle_v1(
        v_build_id,v_unit_key,v_fact_family,v_input_projection_id,
        v_last_source_key,v_fact_limit);
      IF jsonb_typeof(v_occurrence_bundle)<>'object'
         OR COALESCE((v_occurrence_bundle->>'bundle_version')::integer,0)<>1
         OR jsonb_typeof(v_occurrence_bundle->'rows')<>'array' THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_PHYSICAL_SOURCE_EVIDENCE_INVALID'
          USING ERRCODE='23514';
      END IF;
      INSERT INTO pg_temp._bpay_wb_fact_page_v1
      SELECT occurrence.source_key,occurrence.natural_key,occurrence.timesheet_id,
        occurrence.subject_timesheet_ids,occurrence.source_relation,occurrence.source_id,
        occurrence.economic_key_type,occurrence.economic_key_value,
        occurrence.amount_ex_vat,occurrence.amount_inc_vat,
        occurrence.truth_ex_vat,occurrence.truth_inc_vat,
        occurrence.baseline_ex_vat,occurrence.baseline_inc_vat,
        NULL,
        CASE WHEN COALESCE(occurrence.source_payload_json->>'finance_case_id','')
          ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN (occurrence.source_payload_json->>'finance_case_id')::uuid END,
        CASE WHEN COALESCE(occurrence.source_payload_json->>'finance_component_id','')
          ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN (occurrence.source_payload_json->>'finance_component_id')::uuid END,
        CASE WHEN COALESCE(occurrence.source_payload_json->>'reservation_id','')
          ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN (occurrence.source_payload_json->>'reservation_id')::uuid END,
        occurrence.source_payload_json,occurrence.financial_digest
      FROM jsonb_to_recordset(v_occurrence_bundle->'rows') occurrence(
        source_key text,natural_key text,timesheet_id uuid,subject_timesheet_ids uuid[],
        source_relation text,source_id uuid,economic_key_type text,economic_key_value text,
        amount_ex_vat numeric,amount_inc_vat numeric,truth_ex_vat numeric,truth_inc_vat numeric,
        baseline_ex_vat numeric,baseline_inc_vat numeric,source_payload_json jsonb,
        financial_digest text,resolution_failure text
      );
    ELSIF v_fact_family IN ('ENTITLEMENT_COMPONENT','PAYEE_BASELINE_INPUT','CANONICAL_INPUT') THEN
      INSERT INTO pg_temp._bpay_wb_fact_page_v1
      SELECT source.source_key,md5(v_fact_family||':'||source.source_key),source.timesheet_id,
        ARRAY[source.timesheet_id],'economic_build_facts',source.source_id,
        source.key_type,source.key_value,
        CASE WHEN v_fact_family='CANONICAL_INPUT' THEN source.truth_ex_vat END,
        CASE WHEN v_fact_family='CANONICAL_INPUT' THEN source.truth_inc_vat END,
        source.truth_ex_vat,source.truth_inc_vat,source.baseline_ex_vat,source.baseline_inc_vat,
        NULL,NULL,NULL,NULL,
        jsonb_build_object('fact_role',v_fact_family,'source_fact_family',source.source_family,
          'source_natural_key',source.source_natural_key),
        md5(jsonb_build_object('timesheet_id',source.timesheet_id,'source_family',source.source_family,
          'source_natural_key',source.source_natural_key,'key_type',source.key_type,
          'key_value',source.key_value,'truth_ex_vat',source.truth_ex_vat,
          'truth_inc_vat',source.truth_inc_vat,'baseline_ex_vat',source.baseline_ex_vat,
          'baseline_inc_vat',source.baseline_inc_vat)::text)
      FROM (
        SELECT 'T:'||fact.natural_key AS source_key,fact.timesheet_id,fact.source_id,
          fact.economic_key_type AS key_type,fact.economic_key_value AS key_value,
          COALESCE(fact.truth_ex_vat,0) AS truth_ex_vat,
          COALESCE(fact.truth_inc_vat,fact.truth_ex_vat,0) AS truth_inc_vat,
          0::numeric AS baseline_ex_vat,0::numeric AS baseline_inc_vat,
          fact.fact_family AS source_family,fact.natural_key AS source_natural_key
        FROM private.banking_pay_workbench_economic_build_facts fact
        WHERE fact.build_id=v_build_id AND fact.fact_family='LIVE_ENTITLEMENT_INPUT'
          AND fact.dependency_unit_key=v_unit_key AND fact.source_relation<>'UNIT_PROJECTION'
          AND fact.economic_key_type IS NOT NULL
          AND COALESCE(fact.source_payload_json->>'evidence_only','false')<>'true'
        UNION ALL
        SELECT 'B:'||fact.fact_family||':'||fact.natural_key,fact.timesheet_id,fact.source_id,
          fact.economic_key_type,fact.economic_key_value,0::numeric,0::numeric,
          COALESCE(fact.amount_ex_vat,0),COALESCE(fact.amount_inc_vat,fact.amount_ex_vat,0),
          fact.fact_family,fact.natural_key
        FROM private.banking_pay_workbench_economic_build_facts fact
        WHERE fact.build_id=v_build_id
          AND fact.fact_family IN ('FROZEN_SETTLED_COMPONENT','PAY_STATE_FALLBACK')
          AND fact.dependency_unit_key=v_unit_key AND fact.economic_key_type IS NOT NULL
          AND COALESCE(fact.source_payload_json->>'evidence_only','false')<>'true'
          AND (fact.fact_family<>'PAY_STATE_FALLBACK' OR NOT EXISTS(
            SELECT 1 FROM private.banking_pay_workbench_economic_build_facts active
            WHERE active.build_id=v_build_id AND active.fact_family='FROZEN_SETTLED_COMPONENT'
              AND active.dependency_unit_key=v_unit_key AND active.timesheet_id=fact.timesheet_id))
      ) source
      WHERE v_last_source_key IS NULL OR source.source_key>v_last_source_key
      ORDER BY source.source_key LIMIT v_fact_limit+1;
    ELSIF v_fact_family='RESERVATION_COMPONENT' THEN
      INSERT INTO pg_temp._bpay_wb_fact_page_v1
      SELECT reservation.id::text,md5('RESERVATION:'||reservation.id::text),
        authority.resolved_timesheet_id,authority.owner_ids,'pay_advance_reservations',reservation.id,
        authority.resolved_component_key_type,authority.resolved_component_key_value,
        NULL,NULL,NULL,NULL,NULL,NULL,COALESCE(reservation.reserved_source_amount,reservation.reserved_amount),
        authority.resolved_finance_case_id,authority.resolved_finance_component_id,
        reservation.id,to_jsonb(reservation)||jsonb_build_object('resolution_failure',CASE
          WHEN authority.resolution_failure IS NOT NULL THEN authority.resolution_failure
          WHEN cardinality(authority.component_key_pairs)<>1 THEN 'RESERVATION_ECONOMIC_KEY_CONFLICT'
          WHEN authority.resolved_component_key_type NOT IN
            ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE','MANUAL_CARRY_FORWARD')
            OR NULLIF(BTRIM(authority.resolved_component_key_value),'') IS NULL
            THEN 'RESERVATION_ECONOMIC_KEY_INVALID'
          WHEN authority.resolved_component_key_type='TS_DAY' AND NOT (
            authority.resolved_component_key_value ~ '^\d{4}-\d{2}-\d{2}$'
            AND pg_input_is_valid(authority.resolved_component_key_value,'date')
            AND CASE WHEN pg_input_is_valid(authority.resolved_component_key_value,'date')
              THEN (authority.resolved_component_key_value::date)::text=
                authority.resolved_component_key_value ELSE false END)
            THEN 'RESERVATION_TS_DAY_INVALID'
          END,'authority_evidence',authority.evidence_json),
        md5(to_jsonb(reservation)::text)
      FROM public.pay_advance_reservations reservation
      LEFT JOIN public.pay_batch_items item ON item.id=reservation.pay_batch_item_id
      LEFT JOIN public.pay_finance_case_components component ON component.id=reservation.finance_component_id
      LEFT JOIN public.pay_advances finance_case
        ON finance_case.id=COALESCE(reservation.finance_case_id,component.finance_case_id)
      LEFT JOIN public.pay_batch_candidates batch_candidate ON batch_candidate.id=reservation.pay_batch_candidate_id
      LEFT JOIN public.pay_batch_candidates item_batch_candidate ON item_batch_candidate.id=item.pay_batch_candidate_id
      CROSS JOIN LATERAL (
        SELECT resolved.owner_ids,resolved.candidate_ids,resolved.finance_case_ids,
          resolved.resolved_timesheet_id,resolved.resolved_finance_case_id,
          resolved.resolved_finance_component_id,resolved.resolved_component_key_type,
          resolved.resolved_component_key_value,resolved.resolution_failure,
          resolved.evidence_json,resolved.component_key_pairs
        FROM private.pay_workbench_financial_source_authority_v3(
          p_candidate_id,
          ARRAY(SELECT DISTINCT owner_id FROM (VALUES
            (item.timesheet_id),(component.linked_timesheet_id),(finance_case.linked_timesheet_id)
          ) owner(owner_id) WHERE owner_id IS NOT NULL ORDER BY owner_id),
          ARRAY(SELECT DISTINCT candidate_id FROM (VALUES
            (batch_candidate.candidate_id),(item_batch_candidate.candidate_id),
            (component.candidate_id),(finance_case.candidate_id)
          ) owner_candidate(candidate_id) WHERE candidate_id IS NOT NULL ORDER BY candidate_id),
          ARRAY(SELECT DISTINCT finance_case_id FROM (VALUES
            (reservation.finance_case_id),(item.finance_case_id),(component.finance_case_id)
          ) owner_case(finance_case_id) WHERE finance_case_id IS NOT NULL ORDER BY finance_case_id),
          ARRAY(SELECT DISTINCT finance_component_id FROM (VALUES
            (reservation.finance_component_id),(item.finance_component_id),(component.id)
          ) owner_component(finance_component_id)
            WHERE finance_component_id IS NOT NULL ORDER BY finance_component_id),
          jsonb_build_array(
            jsonb_build_object('source','RESERVATION_FROZEN_COLUMNS',
              'key_type',reservation.frozen_component_key_type,
              'key_value',reservation.frozen_component_key_value),
            jsonb_build_object('source','PAY_BATCH_ITEM_FROZEN_COLUMNS',
              'key_type',item.frozen_component_key_type,
              'key_value',item.frozen_component_key_value),
            jsonb_build_object('source','FINANCE_COMPONENT',
              'key_type',component.component_key_type,
              'key_value',component.component_key_value)),
          jsonb_build_array(
            jsonb_build_object('source','RESERVATION',
              'document',COALESCE(reservation.frozen_source_basis_json,'{}'::jsonb)),
            jsonb_build_object('source','PAY_BATCH_ITEM',
              'document',COALESCE(item.frozen_source_basis_json,'{}'::jsonb))),
          jsonb_build_array(
            jsonb_build_object('source','RESERVATION',
              'document',COALESCE(reservation.frozen_component_snapshot_json,'{}'::jsonb)),
            jsonb_build_object('source','PAY_BATCH_ITEM',
              'document',COALESCE(item.frozen_component_snapshot_json,'{}'::jsonb))),
          'RESERVATION'
        ) resolved
      ) authority
      WHERE (p_candidate_id=ANY(authority.candidate_ids)
          OR authority.owner_ids && ARRAY(
            SELECT timesheet_id FROM private.banking_pay_workbench_economic_build_scope
            WHERE build_id=v_build_id))
        AND upper(btrim(COALESCE(reservation.status,''))) IN ('RESERVED','COMMITTED')
        AND reservation.released_at_utc IS NULL
        AND (item.id IS NULL OR COALESCE(item.is_voided,false) IS NOT TRUE)
        AND (authority.resolution_failure IS NOT NULL OR authority.owner_ids && ARRAY(
          SELECT timesheet_id FROM private.banking_pay_workbench_economic_build_scope WHERE build_id=v_build_id))
        AND (v_last_source_key IS NULL OR reservation.id::text>v_last_source_key)
      ORDER BY reservation.id LIMIT v_fact_limit+1;
    ELSIF v_fact_family='FINANCE_CASE_IDENTITY' THEN
      INSERT INTO pg_temp._bpay_wb_fact_page_v1
      SELECT finance_case.id::text,md5('CASE:'||finance_case.id::text),finance_case.linked_timesheet_id,
        CASE WHEN finance_case.linked_timesheet_id IS NULL THEN ARRAY[]::uuid[] ELSE ARRAY[finance_case.linked_timesheet_id] END,
        'pay_advances',finance_case.id,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,finance_case.id,NULL,NULL,
        to_jsonb(finance_case),md5(to_jsonb(finance_case)::text)
      FROM public.pay_advances finance_case WHERE finance_case.candidate_id=p_candidate_id
        AND ((finance_case.cleared_at_utc IS NULL AND finance_case.written_off_at_utc IS NULL)
          OR finance_case.linked_timesheet_id IN (
            SELECT timesheet_id FROM private.banking_pay_workbench_economic_build_scope
            WHERE build_id=v_build_id
          ))
        AND (v_last_source_key IS NULL OR finance_case.id::text>v_last_source_key)
      ORDER BY finance_case.id LIMIT v_fact_limit+1;
    ELSIF v_fact_family='FINANCE_COMPONENT_IDENTITY' THEN
      INSERT INTO pg_temp._bpay_wb_fact_page_v1
      SELECT component.id::text,md5('COMPONENT:'||component.id::text),component.linked_timesheet_id,
        CASE WHEN component.linked_timesheet_id IS NULL THEN ARRAY[]::uuid[] ELSE ARRAY[component.linked_timesheet_id] END,
        'pay_finance_case_components',component.id,
        CASE WHEN component.component_key_type IN ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE','MANUAL_CARRY_FORWARD')
          THEN component.component_key_type ELSE NULL END,
        CASE WHEN component.component_key_type IN ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE','MANUAL_CARRY_FORWARD')
          THEN component.component_key_value ELSE NULL END,
        component.source_amount,NULL,NULL,NULL,NULL,NULL,component.remaining_source_amount,
        component.finance_case_id,component.id,NULL,to_jsonb(component),md5(to_jsonb(component)::text)
      FROM public.pay_finance_case_components component WHERE component.candidate_id=p_candidate_id
        AND component.closed_at_utc IS NULL AND (v_last_source_key IS NULL OR component.id::text>v_last_source_key)
      ORDER BY component.id LIMIT v_fact_limit+1;
    ELSIF v_fact_family='PROTECTION_EVIDENCE' THEN
      INSERT INTO pg_temp._bpay_wb_fact_page_v1
      SELECT 'EVENT:'||event.id::text,md5('EVENT:'||event.id::text),component.linked_timesheet_id,
        CASE WHEN component.linked_timesheet_id IS NULL THEN ARRAY[]::uuid[] ELSE ARRAY[component.linked_timesheet_id] END,
        'pay_finance_case_events',event.id,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
        event.finance_case_id,event.finance_component_id,event.reservation_id,to_jsonb(event),md5(to_jsonb(event)::text)
      FROM public.pay_finance_case_events event
      JOIN private.banking_pay_workbench_economic_build_facts case_fact
        ON case_fact.build_id=v_build_id AND case_fact.fact_family='FINANCE_CASE_IDENTITY'
       AND case_fact.finance_case_id=event.finance_case_id
      LEFT JOIN public.pay_finance_case_components component ON component.id=event.finance_component_id
      WHERE case_fact.candidate_id=p_candidate_id
        AND (
          upper(btrim(COALESCE(event.event_type,''))) LIKE '%WRITE%OFF%'
          OR upper(btrim(COALESCE(event.event_type,''))) IN (
            'MANUAL_ECONOMIC_OVERRIDE','MANUAL_WRITE_OFF','CASE_MANUAL_ECONOMIC_OVERRIDE',
            'COMPONENT_MANUAL_ECONOMIC_OVERRIDE'
          )
          OR upper(btrim(COALESCE(event.reason,''))) IN (
            'MANUAL_ECONOMIC_OVERRIDE','WRITE_OFF','COMPONENT_MANUAL_ECONOMIC_OVERRIDE'
          )
          OR (
            upper(btrim(COALESCE(event.event_type,'')))='CASE_CLEARED'
            AND lower(btrim(COALESCE(event.reason,'')))='rail_settlement'
          )
          OR (
            upper(btrim(COALESCE(event.event_type,'')))='CLEARED'
            AND upper(btrim(COALESCE(event.reason,'')))='PREVIEW_FINANCE_SYNC'
          )
        )
        AND event.id=(
          SELECT event_latest.id
          FROM public.pay_finance_case_events event_latest
          WHERE event_latest.finance_case_id=event.finance_case_id
            AND (
              upper(btrim(COALESCE(event_latest.event_type,''))) LIKE '%WRITE%OFF%'
              OR upper(btrim(COALESCE(event_latest.event_type,''))) IN (
                'MANUAL_ECONOMIC_OVERRIDE','MANUAL_WRITE_OFF','CASE_MANUAL_ECONOMIC_OVERRIDE',
                'COMPONENT_MANUAL_ECONOMIC_OVERRIDE')
              OR upper(btrim(COALESCE(event_latest.reason,''))) IN (
                'MANUAL_ECONOMIC_OVERRIDE','WRITE_OFF','COMPONENT_MANUAL_ECONOMIC_OVERRIDE')
              OR (upper(btrim(COALESCE(event_latest.event_type,'')))='CASE_CLEARED'
                AND lower(btrim(COALESCE(event_latest.reason,'')))='rail_settlement')
              OR (upper(btrim(COALESCE(event_latest.event_type,'')))='CLEARED'
                AND upper(btrim(COALESCE(event_latest.reason,'')))='PREVIEW_FINANCE_SYNC')
            )
            AND (CASE WHEN (
              upper(btrim(COALESCE(event_latest.event_type,''))) LIKE '%WRITE%OFF%'
              OR upper(btrim(COALESCE(event_latest.event_type,''))) IN (
                'MANUAL_ECONOMIC_OVERRIDE','MANUAL_WRITE_OFF','CASE_MANUAL_ECONOMIC_OVERRIDE',
                'COMPONENT_MANUAL_ECONOMIC_OVERRIDE')
              OR upper(btrim(COALESCE(event_latest.reason,''))) IN (
                'MANUAL_ECONOMIC_OVERRIDE','WRITE_OFF','COMPONENT_MANUAL_ECONOMIC_OVERRIDE'))
              THEN 'MANUAL' ELSE 'AUTOMATIC_CLEAR' END)
              =(CASE WHEN (
                upper(btrim(COALESCE(event.event_type,''))) LIKE '%WRITE%OFF%'
                OR upper(btrim(COALESCE(event.event_type,''))) IN (
                  'MANUAL_ECONOMIC_OVERRIDE','MANUAL_WRITE_OFF','CASE_MANUAL_ECONOMIC_OVERRIDE',
                  'COMPONENT_MANUAL_ECONOMIC_OVERRIDE')
                OR upper(btrim(COALESCE(event.reason,''))) IN (
                  'MANUAL_ECONOMIC_OVERRIDE','WRITE_OFF','COMPONENT_MANUAL_ECONOMIC_OVERRIDE'))
                THEN 'MANUAL' ELSE 'AUTOMATIC_CLEAR' END)
          ORDER BY event_latest.event_at_utc DESC NULLS LAST,event_latest.id DESC
          LIMIT 1
        )
        AND (v_last_source_key IS NULL OR 'EVENT:'||event.id::text>v_last_source_key)
      ORDER BY event.id LIMIT v_fact_limit+1;
      INSERT INTO pg_temp._bpay_wb_fact_page_v1
      SELECT 'BATCH_ITEM:'||item.id::text,md5('BATCH_ITEM_PROTECTION:'||item.id::text),
        item.timesheet_id,CASE WHEN item.timesheet_id IS NULL THEN ARRAY[]::uuid[]
          ELSE ARRAY[item.timesheet_id] END,'pay_batch_items',item.id,
        NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,item.finance_case_id,
        item.finance_component_id,item.reservation_id,
        to_jsonb(item)||jsonb_build_object('protection_kind','ACTIVE_CORRECTION_CHAIN_BATCH_ITEM'),
        md5(to_jsonb(item)::text)
      FROM public.pay_batch_items item
      JOIN public.pay_batch_candidates batch_candidate ON batch_candidate.id=item.pay_batch_candidate_id
      JOIN public.pay_batches batch_row ON batch_row.id=batch_candidate.pay_batch_id
      WHERE batch_candidate.candidate_id=p_candidate_id
        AND COALESCE(item.is_voided,false) IS NOT TRUE
        AND batch_row.cancelled_at_utc IS NULL
        AND upper(btrim(COALESCE(batch_row.status,''))) IN (
          'DRAFT','DRAFT_CREATED','READY','WAITING_BANK_CONFIRM','PARTIAL','FAILED',
          'BLOCKED_FUNDS','SCHEDULED','EXECUTING','AWAITING_AUTHORISATION','AUTHORISED_FOR_PAYMENT')
        AND COALESCE(item.frozen_component_snapshot_json->>'correction_root_id',
          item.frozen_resolution_payload_json->>'correction_root_id','') IN (
            SELECT scope_row.root_timesheet_id::text
            FROM private.banking_pay_workbench_economic_build_scope scope_row
            WHERE scope_row.build_id=v_build_id AND scope_row.root_timesheet_id IS NOT NULL)
        AND (v_last_source_key IS NULL OR 'BATCH_ITEM:'||item.id::text>v_last_source_key)
      ORDER BY item.id LIMIT v_fact_limit+1
      ON CONFLICT(source_key) DO NOTHING;
    ELSIF v_fact_family='ALLOCATION_INPUT' THEN
      INSERT INTO pg_temp._bpay_wb_fact_page_v1
      SELECT fact.fact_family||':'||fact.natural_key,md5('ALLOCATION:'||fact.fact_family||':'||fact.natural_key),
        fact.timesheet_id,fact.subject_timesheet_ids,'economic_build_facts',fact.source_id,
        fact.economic_key_type,fact.economic_key_value,
        COALESCE(fact.amount_ex_vat,fact.truth_ex_vat),COALESCE(fact.amount_inc_vat,fact.truth_inc_vat),
        fact.truth_ex_vat,fact.truth_inc_vat,fact.baseline_ex_vat,fact.baseline_inc_vat,
        fact.reserved_source_amount,fact.finance_case_id,fact.finance_component_id,fact.reservation_id,
        jsonb_build_object('source_fact_family',fact.fact_family,'source_natural_key',fact.natural_key),fact.financial_digest
      FROM private.banking_pay_workbench_economic_build_facts fact
      WHERE fact.build_id=v_build_id AND fact.fact_family IN ('ENTITLEMENT_COMPONENT','RESERVATION_COMPONENT')
        AND fact.economic_key_type IS NOT NULL
        AND (v_last_source_key IS NULL OR fact.fact_family||':'||fact.natural_key>v_last_source_key)
      ORDER BY fact.fact_family,fact.natural_key LIMIT v_fact_limit+1;
    ELSE
      RAISE EXCEPTION 'PAY_WORKBENCH_BUILD_CURSOR_INVALID' USING ERRCODE='22023';
    END IF;

    SELECT NULLIF(BTRIM(page.source_payload_json->>'resolution_failure'),'')
    INTO v_resolution_failure
    FROM pg_temp._bpay_wb_fact_page_v1 page
    WHERE NULLIF(BTRIM(page.source_payload_json->>'resolution_failure'),'') IS NOT NULL
    ORDER BY page.source_key LIMIT 1;
    IF v_resolution_failure IS NOT NULL THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_ECONOMIC_KEY_RESOLUTION_INCOMPLETE'
        USING ERRCODE='23514',DETAIL=v_resolution_failure;
    END IF;
    IF v_fact_family='FINANCE_ITEM_AUTHORITY' THEN
      v_raw_source_has_more:=COALESCE(
        (v_occurrence_bundle->>'raw_source_has_more')::boolean,false);
      v_has_more:=v_raw_source_has_more;
    ELSIF v_fact_family IN ('LIVE_ENTITLEMENT_INPUT','FROZEN_SETTLED_COMPONENT','PAY_STATE_FALLBACK')
       AND v_input_phase<>'PROJECTION' THEN
      v_raw_source_has_more:=COALESCE(
        (v_occurrence_bundle->>'raw_source_has_more')::boolean,false);
      v_has_more:=v_raw_source_has_more;
    ELSE
      v_raw_source_has_more:=false;
      SELECT count(*)>v_fact_limit INTO v_has_more FROM pg_temp._bpay_wb_fact_page_v1;
    END IF;
    SELECT count(*)::integer,
      md5(COALESCE(string_agg(source_key||':'||financial_digest,'' ORDER BY source_key),'')),
      max(source_key)
    INTO v_page_count,v_page_digest,v_last_page_key
    FROM (SELECT * FROM pg_temp._bpay_wb_fact_page_v1 ORDER BY source_key LIMIT v_fact_limit) page_rows;

    IF NOT v_has_more AND v_fact_family='LIVE_ENTITLEMENT_INPUT' AND v_input_phase='PROJECTION' THEN
      SELECT projection_ids.projected_timesheet_id INTO v_next_projection_id
      FROM (
        SELECT fact.timesheet_id AS projected_timesheet_id
        FROM private.banking_pay_workbench_economic_build_facts fact
        WHERE fact.build_id=v_build_id AND fact.fact_family='LIVE_ENTITLEMENT_INPUT'
          AND fact.dependency_unit_key=v_unit_key AND fact.source_relation='UNIT_PROJECTION'
        UNION ALL
        SELECT page.timesheet_id
        FROM (SELECT * FROM pg_temp._bpay_wb_fact_page_v1 ORDER BY source_key LIMIT v_fact_limit) page
        WHERE page.source_relation='UNIT_PROJECTION'
      ) projection_ids
      ORDER BY projection_ids.projected_timesheet_id
      LIMIT 1;
      IF v_next_projection_id IS NULL THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_UNIT_PROJECTION_INCOMPLETE' USING ERRCODE='23514';
      END IF;
      v_projection_phase_transition:=true;
      v_has_more:=true;
    ELSIF NOT v_has_more
      AND v_fact_family IN ('LIVE_ENTITLEMENT_INPUT','FROZEN_SETTLED_COMPONENT','PAY_STATE_FALLBACK') THEN
      SELECT projection.timesheet_id INTO v_next_projection_id
      FROM private.banking_pay_workbench_economic_build_facts projection
      WHERE projection.build_id=v_build_id
        AND projection.fact_family='LIVE_ENTITLEMENT_INPUT'
        AND projection.dependency_unit_key=v_unit_key
        AND projection.source_relation='UNIT_PROJECTION'
        AND projection.timesheet_id>v_input_projection_id
      ORDER BY projection.timesheet_id
      LIMIT 1;
      IF v_next_projection_id IS NOT NULL THEN
        v_projection_family_transition:=true;
        v_has_more:=true;
      END IF;
    END IF;

    IF v_fact_family='FINANCE_ITEM_AUTHORITY' OR (
         v_fact_family IN ('LIVE_ENTITLEMENT_INPUT','FROZEN_SETTLED_COMPONENT','PAY_STATE_FALLBACK')
         AND v_input_phase<>'PROJECTION') THEN
      v_has_more:=private.pay_workbench_physical_source_continuation_v1(
        v_raw_source_has_more,
        COALESCE((v_occurrence_bundle->>'raw_source_exhausted')::boolean,false),
        v_projection_family_transition);
    END IF;

    IF v_fact_family='FINANCE_ITEM_AUTHORITY' OR (
         v_fact_family IN ('LIVE_ENTITLEMENT_INPUT','FROZEN_SETTLED_COMPONENT','PAY_STATE_FALLBACK')
         AND v_input_phase<>'PROJECTION') THEN
      v_page_physical_source_count:=COALESCE((v_occurrence_bundle->>'raw_page_count')::bigint,0);
      v_page_resolved_source_count:=COALESCE((v_occurrence_bundle->>'resolved_page_count')::bigint,0);
      v_page_failed_source_count:=COALESCE((v_occurrence_bundle->>'failed_page_count')::bigint,0);
      v_page_physical_amount_ex_vat:=COALESCE((v_occurrence_bundle->>'raw_amount_ex_vat')::numeric,0);
      v_page_resolved_amount_ex_vat:=COALESCE((v_occurrence_bundle->>'resolved_amount_ex_vat')::numeric,0);
      IF (v_page_physical_source_count>0
           AND NULLIF(v_occurrence_bundle->>'last_raw_source_key','') IS NULL)
         OR NULLIF(v_occurrence_bundle->>'evidence_digest','') IS NULL THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_PHYSICAL_SOURCE_EVIDENCE_INVALID'
          USING ERRCODE='23514';
      END IF;
    ELSE
      SELECT count(*)::bigint,
        count(*) FILTER(WHERE NULLIF(page.source_payload_json->>'resolution_failure','') IS NULL)::bigint,
        count(*) FILTER(WHERE NULLIF(page.source_payload_json->>'resolution_failure','') IS NOT NULL)::bigint,
        ROUND(COALESCE(sum(COALESCE(page.truth_ex_vat,page.amount_ex_vat,
          page.reserved_source_amount,0)),0),2),
        ROUND(COALESCE(sum(COALESCE(page.truth_ex_vat,page.amount_ex_vat,
          page.reserved_source_amount,0)) FILTER(
            WHERE NULLIF(page.source_payload_json->>'resolution_failure','') IS NULL),0),2)
      INTO v_page_physical_source_count,v_page_resolved_source_count,
        v_page_failed_source_count,v_page_physical_amount_ex_vat,
        v_page_resolved_amount_ex_vat
      FROM (SELECT * FROM pg_temp._bpay_wb_fact_page_v1 ORDER BY source_key LIMIT v_fact_limit) page
      WHERE v_fact_family='RESERVATION_COMPONENT'
        AND page.source_relation<>'UNIT_PROJECTION';
      IF v_fact_family='RESERVATION_COMPONENT' THEN
        SELECT jsonb_build_object(
          'evidence_digest',md5(COALESCE(string_agg(page.source_key||':'||COALESCE(
            NULLIF(page.source_payload_json->>'raw_physical_digest',''),page.financial_digest),''
            ORDER BY page.source_key),'')),
          'last_raw_source_key',max(page.source_key))
        INTO v_occurrence_bundle
        FROM (SELECT * FROM pg_temp._bpay_wb_fact_page_v1
          ORDER BY source_key LIMIT v_fact_limit) page;
      END IF;
    END IF;
    v_next_raw_physical_source_count:=v_raw_physical_source_count+
      COALESCE(v_page_physical_source_count,0);
    v_next_resolved_physical_source_count:=v_resolved_physical_source_count+
      COALESCE(v_page_resolved_source_count,0);
    v_next_failed_physical_source_count:=v_failed_physical_source_count+
      COALESCE(v_page_failed_source_count,0);
    v_next_raw_physical_amount_ex_vat:=ROUND(v_raw_physical_amount_ex_vat+
      COALESCE(v_page_physical_amount_ex_vat,0),2);
    v_next_resolved_physical_amount_ex_vat:=ROUND(v_resolved_physical_amount_ex_vat+
      COALESCE(v_page_resolved_amount_ex_vat,0),2);
    v_next_last_raw_physical_source_key:=COALESCE(
      NULLIF(v_occurrence_bundle->>'last_raw_source_key',''),
      CASE WHEN v_page_physical_source_count>0 THEN v_last_page_key END,
      v_last_raw_physical_source_key);
    v_cursor_start_hash:=md5(v_cursor::text);
    v_next_cumulative_fact_count:=v_cumulative_fact_count+v_page_count;
    v_cumulative_digest:=md5(v_cumulative_digest||v_page_digest);
    v_is_final:=NOT v_has_more;
    v_next:=jsonb_build_object('cursor_kind','WORKSPACE_FACT','cursor_version',2,
      'build_id',v_build_id,'candidate_id',p_candidate_id,
      'captured_candidate_generation',v_build.captured_candidate_generation,
      'captured_source_change_seq',v_build.source_change_seq,
      'dependency_unit_key',v_unit_key,'fact_family',v_fact_family,
      'page_number',v_page_number+1,'last_source_key',CASE
        WHEN v_projection_phase_transition OR v_projection_family_transition THEN NULL
        WHEN v_has_more THEN v_last_page_key ELSE NULL END,
      'previous_page_digest',v_page_digest,'cumulative_fact_count',v_next_cumulative_fact_count,
      'cumulative_digest',v_cumulative_digest,
      'terminal',v_is_final,
      'raw_physical_source_count',v_next_raw_physical_source_count,
      'resolved_physical_source_count',v_next_resolved_physical_source_count,
      'failed_physical_source_count',v_next_failed_physical_source_count,
      'raw_physical_amount_ex_vat',v_next_raw_physical_amount_ex_vat,
      'resolved_physical_amount_ex_vat',v_next_resolved_physical_amount_ex_vat,
      'last_raw_physical_source_key',v_next_last_raw_physical_source_key,
      'source_exhausted',v_is_final,
      'raw_terminal_source_key',CASE WHEN v_is_final AND v_page_physical_source_count>0
        THEN v_next_last_raw_physical_source_key
        WHEN v_is_final AND v_next_raw_physical_source_count>0
        THEN v_next_last_raw_physical_source_key END,
      'raw_page_evidence_digest',v_occurrence_bundle->>'evidence_digest',
      'input_phase',CASE WHEN v_projection_phase_transition THEN 'COMPONENTS'
        ELSE v_input_phase END,
      'input_projection_id',CASE WHEN v_projection_phase_transition OR v_projection_family_transition
        THEN v_next_projection_id ELSE v_input_projection_id END);
    v_cursor_end_hash:=md5(v_next::text);
    v_expected_source_count:=CASE
      WHEN v_is_final AND v_fact_family IN ('FINANCE_ITEM_AUTHORITY','LIVE_ENTITLEMENT_INPUT','FROZEN_SETTLED_COMPONENT',
        'PAY_STATE_FALLBACK','RESERVATION_COMPONENT')
        THEN v_next_raw_physical_source_count::integer
      ELSE NULL END;

    IF v_page_number=1 THEN
      IF v_cumulative_fact_count<>0 OR v_cursor->>'previous_page_digest' IS NOT NULL
         OR v_cursor->>'cumulative_digest' IS DISTINCT FROM md5('BPAY_FACT_STREAM_V2')
         OR EXISTS(SELECT 1 FROM private.banking_pay_workbench_economic_build_fact_pages
           WHERE build_id=v_build_id AND dependency_unit_key=v_unit_key
             AND fact_family=v_fact_family LIMIT 1) THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_FACT_PAGE_CHAIN_INCOMPLETE' USING ERRCODE='23514';
      END IF;
    ELSE
      SELECT * INTO v_previous_page
      FROM private.banking_pay_workbench_economic_build_fact_pages
      WHERE build_id=v_build_id AND dependency_unit_key=v_unit_key
        AND fact_family=v_fact_family AND page_number=v_page_number-1;
      IF v_previous_page.id IS NULL OR v_previous_page.status<>'COMPLETED'
         OR v_previous_page.is_family_final
         OR v_previous_page.cursor_end_hash<>v_cursor_start_hash
         OR v_previous_page.cursor_end_json IS DISTINCT FROM v_cursor
         OR v_previous_page.cumulative_fact_count<>v_cumulative_fact_count
         OR v_previous_page.cumulative_digest<>COALESCE(v_cursor->>'cumulative_digest','')
         OR v_previous_page.page_digest<>COALESCE(v_cursor->>'previous_page_digest','') THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_FACT_PAGE_CHAIN_INCOMPLETE' USING ERRCODE='23514';
      END IF;
    END IF;

    SELECT * INTO v_existing_page FROM private.banking_pay_workbench_economic_build_fact_pages
    WHERE build_id=v_build_id AND dependency_unit_key=v_unit_key
      AND fact_family=v_fact_family AND page_number=v_page_number;
    IF FOUND THEN
      IF v_existing_page.attempt_id IS DISTINCT FROM v_attempt_id
         OR v_existing_page.status<>'COMPLETED'
         OR v_existing_page.cursor_start_json IS DISTINCT FROM v_cursor
         OR v_existing_page.cursor_start_hash<>v_cursor_start_hash
         OR v_existing_page.cursor_end_json IS DISTINCT FROM v_next
         OR v_existing_page.cursor_end_hash<>v_cursor_end_hash
         OR v_existing_page.expected_source_count IS DISTINCT FROM v_expected_source_count
         OR v_existing_page.actual_fact_count<>v_page_count
         OR v_existing_page.cumulative_fact_count<>v_next_cumulative_fact_count
         OR v_existing_page.page_digest<>v_page_digest
         OR v_existing_page.cumulative_digest<>v_cumulative_digest
         OR v_existing_page.is_family_final IS DISTINCT FROM v_is_final THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_FACT_PAGE_REPLAY_CONFLICT' USING ERRCODE='23514';
      END IF;
      SELECT fact_cursor_json INTO v_replay_cursor
      FROM private.banking_pay_workbench_economic_builds WHERE id=v_build_id;
      RETURN jsonb_build_object('ok',true,'build_id',v_build_id,
        'private_stage',v_build.private_stage,'stage_status','PAGE_REPLAYED',
        'has_more',true,'continuation_enqueued',false,
        'next_cursor_json',COALESCE(v_replay_cursor,v_next),'next_action',v_build.private_stage,
        'page_fact_count',v_page_count,'cumulative_fact_count',v_next_cumulative_fact_count,
        'page_digest',v_page_digest,'cumulative_digest',v_cumulative_digest);
    END IF;

    IF EXISTS(
      SELECT 1 FROM pg_temp._bpay_wb_fact_page_v1 page_row
      JOIN private.banking_pay_workbench_economic_build_facts existing
        ON existing.build_id=v_build_id AND existing.fact_family=v_fact_family
       AND existing.natural_key=page_row.natural_key
      WHERE existing.financial_digest<>page_row.financial_digest LIMIT 1
    ) THEN RAISE EXCEPTION 'PAY_WORKBENCH_FACT_PAGE_REPLAY_CONFLICT' USING ERRCODE='23514'; END IF;

    IF v_fact_family IS NOT NULL THEN
      INSERT INTO private.banking_pay_workbench_economic_build_facts(
      build_id,fact_family,natural_key,candidate_id,timesheet_id,subject_timesheet_ids,
      dependency_unit_key,source_relation,source_id,economic_key_type,economic_key_value,
      amount_ex_vat,amount_inc_vat,truth_ex_vat,truth_inc_vat,baseline_ex_vat,baseline_inc_vat,
      reserved_source_amount,finance_case_id,finance_component_id,reservation_id,
      source_payload_json,financial_digest
    )
    SELECT v_build_id,v_fact_family,page.natural_key,p_candidate_id,page.timesheet_id,
      page.subject_timesheet_ids,v_unit_key,page.source_relation,page.source_id,
      page.economic_key_type,page.economic_key_value,page.amount_ex_vat,page.amount_inc_vat,
      page.truth_ex_vat,page.truth_inc_vat,page.baseline_ex_vat,page.baseline_inc_vat,
      page.reserved_source_amount,page.finance_case_id,page.finance_component_id,page.reservation_id,
      page.source_payload_json,page.financial_digest
    FROM (SELECT * FROM pg_temp._bpay_wb_fact_page_v1 ORDER BY source_key LIMIT v_fact_limit) page;
      GET DIAGNOSTICS v_scope_inserted=ROW_COUNT;
      IF v_scope_inserted<>v_page_count THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_FACT_PAGE_COUNT_MISMATCH' USING ERRCODE='23514';
      END IF;
    ELSE
      v_scope_inserted:=0;
    END IF;

    -- Physical occurrences are already typed and immutable. Derived families
    -- page those stored occurrences; no page-local live helper is invoked.
    v_derived_fact_count:=0;
    v_derived_settled_count:=0;
    v_derived_fallback_count:=0;

    -- The table's physical column order evaluates completed_at_utc before
    -- created_at_utc.  Supplying one database-owned timestamp prevents two
    -- independent clock_timestamp() defaults from intermittently violating
    -- completed_at_utc >= created_at_utc under fast concurrent lanes.
    v_fact_page_timestamp:=clock_timestamp();
    INSERT INTO private.banking_pay_workbench_economic_build_fact_pages(
      build_id,attempt_id,dependency_unit_key,fact_family,page_number,
      cursor_start_json,cursor_start_hash,cursor_end_json,cursor_end_hash,
      expected_source_count,actual_fact_count,cumulative_fact_count,page_digest,cumulative_digest,is_family_final,
      completed_at_utc,created_at_utc
    ) VALUES(v_build_id,v_attempt_id,v_unit_key,v_fact_family,v_page_number,
      v_cursor,v_cursor_start_hash,v_next,v_cursor_end_hash,v_expected_source_count,v_page_count,
      v_next_cumulative_fact_count,v_page_digest,v_cumulative_digest,v_is_final,
      v_fact_page_timestamp,v_fact_page_timestamp);

    IF v_unit_key<>'GLOBAL' THEN
      UPDATE private.banking_pay_workbench_economic_build_scope scope_row
      SET fact_row_count=scope_row.fact_row_count+page_count.page_count,
          fact_digest=md5(COALESCE(scope_row.fact_digest,md5('BPAY_SCOPE_FACT_V1'))||v_page_digest),
          updated_at_utc=clock_timestamp()
      FROM (
        SELECT page.timesheet_id,count(*)::integer page_count
        FROM (SELECT * FROM pg_temp._bpay_wb_fact_page_v1 ORDER BY source_key LIMIT v_fact_limit) page
        WHERE page.timesheet_id IS NOT NULL GROUP BY page.timesheet_id
      ) page_count
      WHERE scope_row.build_id=v_build_id AND scope_row.timesheet_id=page_count.timesheet_id;
    END IF;

    UPDATE private.banking_pay_workbench_economic_builds build_row
    SET fact_count=build_row.fact_count+v_scope_inserted+v_derived_fact_count+v_derived_settled_count+v_derived_fallback_count,
        complexity_vector_json=COALESCE(build_row.complexity_vector_json,'{}'::jsonb)
          ||jsonb_build_object(
            'settled_source_row_count',COALESCE((build_row.complexity_vector_json->>'settled_source_row_count')::bigint,0)+page_stats.settled_count,
            'settled_component_count',COALESCE((build_row.complexity_vector_json->>'settled_component_count')::bigint,0)+page_stats.settled_count,
            'entitlement_component_count',COALESCE((build_row.complexity_vector_json->>'entitlement_component_count')::bigint,0)+page_stats.entitlement_count,
            'reservation_component_count',COALESCE((build_row.complexity_vector_json->>'reservation_component_count')::bigint,0)+page_stats.reservation_count,
            'finance_case_count',COALESCE((build_row.complexity_vector_json->>'finance_case_count')::bigint,0)+page_stats.case_count,
            'finance_component_count',COALESCE((build_row.complexity_vector_json->>'finance_component_count')::bigint,0)+page_stats.component_count,
            'protection_evidence_count',COALESCE((build_row.complexity_vector_json->>'protection_evidence_count')::bigint,0)+page_stats.protection_count,
            'expected_case_insert_count',COALESCE((build_row.complexity_vector_json->>'expected_case_insert_count')::bigint,0)+page_stats.negative_count,
            'expected_component_insert_count',COALESCE((build_row.complexity_vector_json->>'expected_component_insert_count')::bigint,0)+page_stats.negative_count,
            'canonical_source_row_count',COALESCE((build_row.complexity_vector_json->>'canonical_source_row_count')::bigint,0)+page_stats.canonical_count,
            'staging_bytes',COALESCE((build_row.complexity_vector_json->>'staging_bytes')::bigint,0)+page_stats.staging_bytes),
        updated_at_utc=clock_timestamp()
    FROM (SELECT
      count(*) FILTER(WHERE v_fact_family='FROZEN_SETTLED_COMPONENT')::bigint settled_count,
      count(*) FILTER(WHERE v_fact_family='ENTITLEMENT_COMPONENT')::bigint entitlement_count,
      count(*) FILTER(WHERE v_fact_family='RESERVATION_COMPONENT')::bigint reservation_count,
      count(*) FILTER(WHERE v_fact_family='FINANCE_CASE_IDENTITY')::bigint case_count,
      count(*) FILTER(WHERE v_fact_family='FINANCE_COMPONENT_IDENTITY')::bigint component_count,
      count(*) FILTER(WHERE v_fact_family='PROTECTION_EVIDENCE')::bigint protection_count,
      count(*) FILTER(WHERE v_fact_family='ENTITLEMENT_COMPONENT' AND truth_ex_vat<baseline_ex_vat)::bigint negative_count,
      count(*) FILTER(WHERE v_fact_family='CANONICAL_INPUT')::bigint canonical_count,
      COALESCE(sum(pg_column_size(source_payload_json)) FILTER(WHERE v_fact_family='CANONICAL_INPUT'),0)::bigint staging_bytes
      FROM (SELECT * FROM pg_temp._bpay_wb_fact_page_v1 ORDER BY source_key LIMIT v_fact_limit) accepted) page_stats
    WHERE build_row.id=v_build_id;

    IF v_is_final THEN
      IF v_unit_key<>'GLOBAL' THEN
        UPDATE private.banking_pay_workbench_economic_build_scope
        SET completed_fact_families=(SELECT array_agg(DISTINCT family ORDER BY family)
          FROM unnest(completed_fact_families||ARRAY[v_fact_family]) family),updated_at_utc=clock_timestamp()
        WHERE build_id=v_build_id AND dependency_unit_key=v_unit_key;
      END IF;
      IF v_unit_key='GLOBAL' AND v_fact_family='FINANCE_ITEM_AUTHORITY' THEN
        SELECT dependency_unit_key INTO v_unit_key
        FROM private.banking_pay_workbench_economic_build_scope
        WHERE build_id=v_build_id AND NOT required_fact_families <@ completed_fact_families
        ORDER BY stable_ordinal LIMIT 1;
        IF v_unit_key IS NULL THEN
          v_unit_key:='GLOBAL';
          v_fact_family:=v_global_families[2];
        ELSE
          v_fact_family:=v_unit_families[1];
        END IF;
        v_next:=private.pay_workbench_fact_cursor_transition_v3(
          v_next,v_unit_key,v_fact_family,
          CASE WHEN v_unit_key<>'GLOBAL' THEN 'PROJECTION'
            ELSE 'PHYSICAL_SOURCE' END,NULL);
      ELSIF v_unit_key<>'GLOBAL' AND v_family_ordinal<cardinality(v_unit_families) THEN
        v_next:=private.pay_workbench_fact_cursor_transition_v3(
          v_next,v_unit_key,v_unit_families[v_family_ordinal+1],
          CASE WHEN v_unit_families[v_family_ordinal+1]='LIVE_ENTITLEMENT_INPUT'
            THEN 'PROJECTION' ELSE 'COMPONENTS' END,NULL);
      ELSIF v_unit_key<>'GLOBAL' THEN
        SELECT dependency_unit_key INTO v_unit_key
        FROM private.banking_pay_workbench_economic_build_scope
        WHERE build_id=v_build_id AND NOT required_fact_families <@ completed_fact_families
        ORDER BY stable_ordinal LIMIT 1;
        IF v_unit_key IS NULL THEN v_unit_key:='GLOBAL';v_fact_family:=v_global_families[2];
        ELSE v_fact_family:=v_unit_families[1]; END IF;
        v_next:=private.pay_workbench_fact_cursor_transition_v3(
          v_next,v_unit_key,v_fact_family,
          CASE WHEN v_unit_key<>'GLOBAL' THEN 'PROJECTION'
            ELSE 'PHYSICAL_SOURCE' END,NULL);
      ELSIF v_family_ordinal<cardinality(v_global_families) THEN
        v_next:=private.pay_workbench_fact_cursor_transition_v3(
          v_next,'GLOBAL',v_global_families[v_family_ordinal+1],
          'PHYSICAL_SOURCE',NULL);
      ELSE
        IF EXISTS(SELECT 1 FROM private.banking_pay_workbench_economic_build_scope
          WHERE build_id=v_build_id AND NOT required_fact_families<@completed_fact_families LIMIT 1) THEN
          RAISE EXCEPTION 'PAY_WORKBENCH_FACT_PAGE_CHAIN_INCOMPLETE' USING ERRCODE='23514';
        END IF;
        IF EXISTS(
          WITH required_stream AS (
            SELECT DISTINCT scope_row.dependency_unit_key,required.family AS fact_family
            FROM private.banking_pay_workbench_economic_build_scope scope_row
            CROSS JOIN LATERAL unnest(ARRAY['LIVE_ENTITLEMENT_INPUT',
              'FROZEN_SETTLED_COMPONENT','PAY_STATE_FALLBACK']) required(family)
            WHERE scope_row.build_id=v_build_id
            UNION ALL SELECT 'GLOBAL'::text,'FINANCE_ITEM_AUTHORITY'::text
            UNION ALL SELECT 'GLOBAL'::text,'RESERVATION_COMPONENT'::text
          ), terminal_stream AS (
            SELECT page.dependency_unit_key,page.fact_family,count(*) AS terminal_count,
              min(page.expected_source_count) AS expected_source_count,
              min((page.cursor_end_json->>'raw_physical_source_count')::bigint) AS raw_count,
              min((page.cursor_end_json->>'resolved_physical_source_count')::bigint) AS resolved_count,
              min((page.cursor_end_json->>'failed_physical_source_count')::bigint) AS failed_count,
              min((page.cursor_end_json->>'raw_physical_amount_ex_vat')::numeric) AS raw_amount,
              min((page.cursor_end_json->>'resolved_physical_amount_ex_vat')::numeric) AS resolved_amount,
              min(NULLIF(page.cursor_end_json->>'raw_terminal_source_key','')) AS terminal_source_key,
              bool_and(COALESCE((page.cursor_end_json->>'source_exhausted')::boolean,false)) AS exhausted
            FROM private.banking_pay_workbench_economic_build_fact_pages page
            WHERE page.build_id=v_build_id AND page.status='COMPLETED' AND page.is_family_final
            GROUP BY page.dependency_unit_key,page.fact_family
          )
          SELECT 1 FROM required_stream required
          LEFT JOIN terminal_stream terminal USING(dependency_unit_key,fact_family)
          WHERE terminal.terminal_count IS DISTINCT FROM 1
             OR terminal.expected_source_count IS DISTINCT FROM terminal.raw_count::integer
             OR terminal.raw_count IS DISTINCT FROM terminal.resolved_count
             OR terminal.failed_count IS DISTINCT FROM 0
             OR ROUND(terminal.raw_amount,2) IS DISTINCT FROM ROUND(terminal.resolved_amount,2)
             OR (terminal.raw_count>0 AND terminal.terminal_source_key IS NULL)
             OR terminal.exhausted IS DISTINCT FROM true
          LIMIT 1
        ) THEN
          RAISE EXCEPTION 'PAY_WORKBENCH_PHYSICAL_SOURCE_SEAL_INCOMPLETE' USING ERRCODE='23514';
        END IF;
        IF v_registry.initialisation_status IN ('DISCOVERING','CLASSIFYING')
           AND v_registry.bootstrap_id IS NOT NULL THEN
          v_next:=jsonb_build_object('cursor_kind','BOOTSTRAP_DISCOVERY','cursor_version',1,
            'bootstrap_id',v_registry.bootstrap_id,'bootstrap_stream','CLASSIFY_UNITS',
            'classification_phase','EVIDENCE','last_dependency_unit_key',NULL,
            'dependency_unit_key',NULL,'last_stable_ordinal',0,
            'unit_financially_relevant',false,'build_id',v_build_id,
            'candidate_id',p_candidate_id,
            'captured_candidate_generation',v_build.captured_candidate_generation,
            'captured_source_change_seq',v_build.source_change_seq);
          UPDATE private.banking_pay_workbench_economic_builds
          SET status='COLLECTING',private_stage='BOOTSTRAP_DISCOVERY',
            fact_cursor_json=jsonb_build_object('cursor_kind','WORKSPACE_FACT','cursor_version',2,
              'terminal',true,'build_id',v_build_id),
            updated_at_utc=clock_timestamp()
          WHERE id=v_build_id;
          UPDATE private.banking_pay_workbench_candidate_scope_registry
          SET initialisation_status='CLASSIFYING',bootstrap_stream='CLASSIFY_UNITS',
            bootstrap_cursor_json=v_next,updated_at_utc=clock_timestamp()
          WHERE candidate_id=p_candidate_id AND current_build_id=v_build_id;
          RETURN jsonb_build_object('ok',true,'build_id',v_build_id,
            'private_stage','BOOTSTRAP_DISCOVERY','stage_status','CLASSIFYING',
            'has_more',true,'continuation_enqueued',false,'next_cursor_json',v_next,
            'next_action','BOOTSTRAP_DISCOVERY');
        END IF;
        SELECT COALESCE(complexity_vector_json,'{}'::jsonb)||jsonb_build_object(
          'relevant_timesheet_count',scope_count,'dependency_node_count',dependency_node_count,
          'dependency_edge_count',dependency_edge_count,
          'expected_case_update_count',COALESCE((complexity_vector_json->>'finance_case_count')::bigint,0),
          'expected_case_clear_count',0,
          'expected_component_update_count',COALESCE((complexity_vector_json->>'finance_component_count')::bigint,0),
          'expected_component_close_count',0)
        INTO v_vector FROM private.banking_pay_workbench_economic_builds WHERE id=v_build_id;
        SELECT banking_pay_workbench_reconciliation_envelope_version,
               banking_pay_workbench_reconciliation_envelope_json,
               banking_pay_workbench_reconciliation_envelope_evidence_json
        INTO v_envelope_version,v_envelope,v_envelope_evidence
        FROM public.settings_defaults WHERE id=1;
        IF v_envelope_version IS NULL OR jsonb_typeof(v_envelope)<>'object' OR v_envelope='{}'::jsonb THEN
          v_scale_blocked:=true;
        ELSE
          FOR v_vector_item IN SELECT key,value FROM jsonb_each_text(v_vector) LOOP
            IF COALESCE(v_envelope->>v_vector_item.key,'') !~ '^[0-9]+$'
               OR v_vector_item.value::numeric>(v_envelope->>v_vector_item.key)::numeric THEN
              v_scale_blocked:=true; EXIT;
            END IF;
          END LOOP;
        END IF;
        IF v_scale_blocked THEN
          UPDATE private.banking_pay_workbench_economic_builds SET
            status='BLOCKED_UNVALIDATED_RECONCILIATION_SCALE',complexity_vector_json=v_vector,
            envelope_version=v_envelope_version,envelope_snapshot_json=COALESCE(v_envelope,'{}'::jsonb),
            envelope_evidence_json=COALESCE(v_envelope_evidence,'{}'::jsonb),
            fact_cursor_json=v_next||jsonb_build_object('terminal',true),updated_at_utc=clock_timestamp()
          WHERE id=v_build_id;
          RETURN jsonb_build_object('ok',true,'build_id',v_build_id,'private_stage','WORKSPACE_FACT',
            'stage_status','BLOCKED_UNVALIDATED_RECONCILIATION_SCALE','has_more',false,
            'continuation_enqueued',false,'next_action','SCALE_EVIDENCE_REQUIRED','complexity_vector',v_vector);
        END IF;
        v_next:=jsonb_build_object('cursor_kind','RECONCILE_EXECUTE','cursor_version',1,
          'build_id',v_build_id,'candidate_id',p_candidate_id,
          'captured_candidate_generation',v_build.captured_candidate_generation,
          'captured_source_change_seq',v_build.source_change_seq);
        UPDATE private.banking_pay_workbench_economic_builds SET status='READY_FOR_RECONCILIATION',
          private_stage='RECONCILE_EXECUTE',fact_cursor_json=jsonb_build_object(
            'cursor_kind','WORKSPACE_FACT','cursor_version',2,'terminal',true,'build_id',v_build_id),
          complexity_vector_json=v_vector,envelope_version=v_envelope_version,
          envelope_snapshot_json=v_envelope,envelope_evidence_json=v_envelope_evidence,
          ready_at_utc=clock_timestamp(),updated_at_utc=clock_timestamp() WHERE id=v_build_id;
      END IF;
    END IF;
    IF (SELECT private_stage FROM private.banking_pay_workbench_economic_builds WHERE id=v_build_id)='WORKSPACE_FACT' THEN
      UPDATE private.banking_pay_workbench_economic_builds SET fact_cursor_json=v_next,
        updated_at_utc=clock_timestamp() WHERE id=v_build_id;
    END IF;
    RETURN jsonb_build_object('ok',true,'build_id',v_build_id,'private_stage',
      (SELECT private_stage FROM private.banking_pay_workbench_economic_builds WHERE id=v_build_id),
      'stage_status','PREPARING','has_more',true,'continuation_enqueued',false,
      'next_cursor_json',v_next,'next_action',
      (SELECT private_stage FROM private.banking_pay_workbench_economic_builds WHERE id=v_build_id),
      'page_fact_count',v_page_count,'page_digest',v_page_digest);
  END IF;

  IF v_stage='RECONCILE_EXECUTE' THEN
    UPDATE private.banking_pay_workbench_economic_builds SET status='RECONCILING',updated_at_utc=clock_timestamp()
    WHERE id=v_build_id AND status='READY_FOR_RECONCILIATION' AND private_stage='RECONCILE_EXECUTE';
    IF NOT FOUND THEN RAISE EXCEPTION 'PAY_WORKBENCH_RECONCILIATION_ATTEMPT_STALE' USING ERRCODE='40001'; END IF;
    SELECT upper(NULLIF(btrim(to_jsonb(candidate_row)->>'pay_method'),'')) INTO v_candidate_pay_method
    FROM public.candidates candidate_row WHERE candidate_row.id=p_candidate_id;
    v_actor_user_id:=COALESCE(NULLIF(v_payload->>'actor_user_id','')::uuid,v_session.actor_user_id);
    IF COALESCE((v_build.attestation_json->>'effect_plan_sealed')::boolean,false) IS NOT TRUE THEN
      BEGIN
        PERFORM set_config('cloudtms.pay_workbench_effect_capture_mode','capture',true);
        v_sync_result:=public.pay_sync_overpayments_from_preview(
          v_session.pay_date,v_session.week_ending_cutoff,v_actor_user_id,v_candidate_pay_method,
          ARRAY[p_candidate_id],COALESCE(v_payload->'mismatch_choices','{}'::jsonb),
          NULLIF(v_payload->>'client_filter_single','')::uuid,NULL,NULL
        );
        v_effect_plan:=COALESCE(v_sync_result->'captured_effects','[]'::jsonb);
        IF jsonb_typeof(v_effect_plan)<>'array' THEN
          RAISE EXCEPTION 'PAY_WORKBENCH_EXPECTED_EFFECT_CAPTURE_INVALID' USING ERRCODE='23514';
        END IF;
        RAISE EXCEPTION 'PAY_WORKBENCH_EFFECT_PLAN_CAPTURE_ROLLBACK' USING ERRCODE='PZ001';
      EXCEPTION WHEN SQLSTATE 'PZ001' THEN
        NULL;
      END;
      -- Generated row identities and audit timestamps are not economic authority.
      -- INSERT effects are normalised, deterministically ordered, and assigned a
      -- build-owned plan identity before the durable plan is sealed.
      WITH ordered AS (
        SELECT raw_effect.value AS raw_effect,raw_effect.ordinality,
          row_number() OVER(ORDER BY raw_effect.value::text,raw_effect.ordinality) AS plan_ordinal
        FROM jsonb_array_elements(COALESCE(v_effect_plan,'[]'::jsonb))
          WITH ORDINALITY raw_effect(value,ordinality)
      ), identified AS (
        SELECT ordered.*,
          CASE WHEN ordered.raw_effect->>'operation'='INSERT'
            THEN (md5(v_build_id::text||':EXPECTED_FINANCE_EFFECT:'||ordered.plan_ordinal::text))::uuid
            ELSE NULLIF(ordered.raw_effect->>'source_id','')::uuid END AS logical_source_id
        FROM ordered
      ), normalised AS (
        SELECT identified.plan_ordinal,
          identified.raw_effect||jsonb_build_object(
            'source_id',identified.logical_source_id,
            'finance_case_id',CASE
              WHEN identified.raw_effect->>'operation'='INSERT'
               AND identified.raw_effect->>'relation_name'='pay_advances'
                THEN identified.logical_source_id
              WHEN NULLIF(identified.raw_effect->>'finance_case_id','') IS NULL THEN NULL
              ELSE COALESCE((SELECT parent.logical_source_id FROM identified parent
                WHERE parent.raw_effect->>'operation'='INSERT'
                  AND parent.raw_effect->>'relation_name'='pay_advances'
                  AND NULLIF(parent.raw_effect->>'source_id','')::uuid=
                    NULLIF(identified.raw_effect->>'finance_case_id','')::uuid),
                NULLIF(identified.raw_effect->>'finance_case_id','')::uuid) END,
            'finance_component_id',CASE
              WHEN identified.raw_effect->>'operation'='INSERT'
               AND identified.raw_effect->>'relation_name'='pay_finance_case_components'
                THEN identified.logical_source_id
              WHEN NULLIF(identified.raw_effect->>'finance_component_id','') IS NULL THEN NULL
              ELSE COALESCE((SELECT parent.logical_source_id FROM identified parent
                WHERE parent.raw_effect->>'operation'='INSERT'
                  AND parent.raw_effect->>'relation_name'='pay_finance_case_components'
                  AND NULLIF(parent.raw_effect->>'source_id','')::uuid=
                    NULLIF(identified.raw_effect->>'finance_component_id','')::uuid),
                NULLIF(identified.raw_effect->>'finance_component_id','')::uuid) END
          ) AS effect
        FROM identified
      )
      SELECT COALESCE(jsonb_agg(normalised.effect ORDER BY normalised.plan_ordinal),'[]'::jsonb)
      INTO v_effect_plan FROM normalised;
      v_effect_plan_count:=jsonb_array_length(COALESCE(v_effect_plan,'[]'::jsonb));
      v_effect_plan_digest:=md5(COALESCE(v_effect_plan::text,'[]'));
      DELETE FROM private.banking_pay_workbench_economic_build_facts
      WHERE build_id=v_build_id AND fact_family='EXPECTED_FINANCE_EFFECT';
      INSERT INTO private.banking_pay_workbench_economic_build_facts(build_id,fact_family,natural_key,
        candidate_id,timesheet_id,subject_timesheet_ids,dependency_unit_key,source_relation,source_id,
        finance_case_id,finance_component_id,economic_key_type,economic_key_value,
        source_payload_json,financial_digest,source_ordinal)
      SELECT v_build_id,'EXPECTED_FINANCE_EFFECT',md5('EXPECTED_FINANCE_EFFECT:'||effect.ordinality::text||':'||effect.value::text),
        (effect.value->>'candidate_id')::uuid,NULLIF(effect.value->>'timesheet_id','')::uuid,
        CASE WHEN NULLIF(effect.value->>'timesheet_id','') IS NULL THEN ARRAY[]::uuid[]
          ELSE ARRAY[(effect.value->>'timesheet_id')::uuid] END,'GLOBAL',effect.value->>'relation_name',
        (effect.value->>'source_id')::uuid,
        NULLIF(effect.value->>'finance_case_id','')::uuid,
        NULLIF(effect.value->>'finance_component_id','')::uuid,
        NULLIF(effect.value->>'economic_key_type',''),
        NULLIF(effect.value->>'economic_key_value',''),jsonb_build_object(
          'operation',effect.value->>'operation',
          'expected_before_digest',effect.value->>'expected_before_digest',
          'expected_after_digest',effect.value->>'expected_after_digest'),md5(effect.value::text),effect.ordinality
      FROM jsonb_array_elements(COALESCE(v_effect_plan,'[]'::jsonb)) WITH ORDINALITY effect(value,ordinality);
      UPDATE private.banking_pay_workbench_economic_builds SET status='READY_FOR_RECONCILIATION',
        attestation_json=COALESCE(attestation_json,'{}'::jsonb)||jsonb_build_object(
          'effect_plan_sealed',true,'effect_plan_count',v_effect_plan_count,
          'effect_plan_digest',v_effect_plan_digest,'effect_plan_created_at_utc',clock_timestamp()),
        updated_at_utc=clock_timestamp() WHERE id=v_build_id AND status='RECONCILING';
      v_next:=jsonb_build_object('cursor_kind','RECONCILE_EXECUTE','cursor_version',1,
        'build_id',v_build_id,'candidate_id',p_candidate_id,'reconcile_phase','EXECUTE',
        'captured_candidate_generation',v_build.captured_candidate_generation,
        'captured_source_change_seq',v_build.source_change_seq,'effect_plan_digest',v_effect_plan_digest);
      RETURN jsonb_build_object('ok',true,'build_id',v_build_id,'private_stage','RECONCILE_EXECUTE',
        'stage_status','EFFECT_PLAN_READY','has_more',true,'continuation_enqueued',false,
        'next_cursor_json',v_next,'next_action','RECONCILE_EXECUTE',
        'effect_plan_count',v_effect_plan_count,'effect_plan_digest',v_effect_plan_digest);
    END IF;
    SELECT count(*)::integer,md5(COALESCE(jsonb_agg(jsonb_build_object(
      'candidate_id',fact.candidate_id,'timesheet_id',fact.timesheet_id,
      'relation_name',fact.source_relation,'operation',fact.source_payload_json->>'operation',
      'source_id',fact.source_id,'finance_case_id',fact.finance_case_id,
      'finance_component_id',fact.finance_component_id,
      'economic_key_type',fact.economic_key_type,'economic_key_value',fact.economic_key_value,
      'expected_before_digest',NULLIF(fact.source_payload_json->>'expected_before_digest',''),
      'expected_after_digest',NULLIF(fact.source_payload_json->>'expected_after_digest','')
      ) ORDER BY fact.source_ordinal),'[]'::jsonb)::text)
    INTO v_effect_plan_count,v_effect_plan_digest
    FROM private.banking_pay_workbench_economic_build_facts fact
    WHERE fact.build_id=v_build_id AND fact.fact_family='EXPECTED_FINANCE_EFFECT';
    IF v_effect_plan_count IS DISTINCT FROM
         COALESCE((v_build.attestation_json->>'effect_plan_count')::integer,0)
       OR v_effect_plan_digest IS DISTINCT FROM v_build.attestation_json->>'effect_plan_digest'
       OR NULLIF(v_cursor->>'effect_plan_digest','') IS DISTINCT FROM
          v_build.attestation_json->>'effect_plan_digest' THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_EXPECTED_EFFECT_PLAN_MISMATCH'
        USING ERRCODE='23514';
    END IF;
    v_sync_result:=public.pay_sync_overpayments_from_preview(
      v_session.pay_date,v_session.week_ending_cutoff,v_actor_user_id,v_candidate_pay_method,
      ARRAY[p_candidate_id],COALESCE(v_payload->'mismatch_choices','{}'::jsonb),
      NULLIF(v_payload->>'client_filter_single','')::uuid,NULL,NULL
    );
    SELECT canonical_count<=25 INTO v_publish_now FROM private.banking_pay_workbench_economic_builds WHERE id=v_build_id;
    IF NOT v_publish_now THEN
      RETURN jsonb_build_object('ok',true,'build_id',v_build_id,'private_stage','SOURCE_PUBLISH',
        'stage_status','RECONCILED','has_more',true,'continuation_enqueued',false,
        'next_cursor_json',(SELECT publication_cursor_json FROM private.banking_pay_workbench_economic_builds WHERE id=v_build_id),
        'next_action','SOURCE_PUBLISH','sync_result',v_sync_result);
    END IF;
    v_stage:='SOURCE_PUBLISH';
  END IF;

  IF v_stage='SOURCE_PUBLISH' THEN
    SELECT * INTO v_build FROM private.banking_pay_workbench_economic_builds WHERE id=v_build_id FOR UPDATE;
    IF v_build.status NOT IN ('RECONCILED','PUBLISHING') OR v_build.private_stage<>'SOURCE_PUBLISH'
       OR EXISTS(SELECT 1 FROM private.banking_pay_workbench_canonical_stage_lines
         WHERE build_id=v_build_id AND stage_status<>'VERIFIED' LIMIT 1) THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_CANONICAL_PUBLICATION_NOT_READY' USING ERRCODE='23514';
    END IF;
    UPDATE private.banking_pay_workbench_economic_builds SET status='PUBLISHING',updated_at_utc=clock_timestamp()
    WHERE id=v_build_id;
    PERFORM 1 FROM public.banking_pay_workbench_candidate_source_lines
    WHERE session_id=p_session_id AND candidate_id=p_candidate_id AND status='CURRENT'
    ORDER BY source_ordinal FOR UPDATE;
    UPDATE public.banking_pay_workbench_candidate_source_lines
    SET status='SUPERSEDED',updated_at_utc=clock_timestamp()
    WHERE session_id=p_session_id AND candidate_id=p_candidate_id AND status='CURRENT';
    INSERT INTO public.banking_pay_workbench_candidate_source_lines(
      session_id,candidate_id,session_version,source_change_seq,source_build_run_id,
      source_ordinal,line_key,parent_line_key,split_suffix,timesheet_id,section,
      source_row_json,economic_key_json,contract_json,pay_channel_scope,refresh_scope_kind,status
    ) SELECT session_id,candidate_id,session_version,source_change_seq,source_build_run_id,
      source_ordinal,line_key,parent_line_key,split_suffix,timesheet_id,section,
      source_row_json,economic_key_json,contract_json,pay_channel_scope,refresh_scope_kind,'CURRENT'
    FROM private.banking_pay_workbench_canonical_stage_lines WHERE build_id=v_build_id
    ORDER BY source_ordinal;
    GET DIAGNOSTICS v_published_count=ROW_COUNT;
    SELECT md5(COALESCE(string_agg(md5(source_row_json::text),'' ORDER BY source_ordinal),''))
    INTO v_published_digest FROM public.banking_pay_workbench_candidate_source_lines
    WHERE session_id=p_session_id AND candidate_id=p_candidate_id AND status='CURRENT'
      AND source_build_run_id=v_build.source_build_run_id;
    IF v_published_count<>v_build.canonical_count OR v_published_digest IS DISTINCT FROM v_build.canonical_digest THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_CANONICAL_PUBLICATION_DIGEST_MISMATCH' USING ERRCODE='23514';
    END IF;
    UPDATE public.banking_pay_workbench_session_candidate_state SET status='READY',
      source_change_seq=v_build.source_change_seq,session_version=v_build.session_version,
      pending_job_id=NULL,last_error_json='{}'::jsonb,last_recomputed_at_utc=clock_timestamp(),
      updated_at_utc=clock_timestamp()
    WHERE session_id=p_session_id AND candidate_id=p_candidate_id;
    UPDATE private.banking_pay_workbench_timesheet_scope_state state_row
    SET economic_state=CASE WHEN EXISTS(
        SELECT 1
        FROM (
          SELECT fact.economic_key_type,fact.economic_key_value,
            ROUND(SUM(COALESCE(fact.truth_ex_vat,0)),2) AS truth_ex_vat,
            ROUND(SUM(COALESCE(fact.baseline_ex_vat,0)),2) AS baseline_ex_vat,
            ROUND(SUM(COALESCE(fact.reserved_source_amount,0)),2) AS reserved_source_amount,
            bool_or(fact.finance_case_id IS NOT NULL) AS has_finance_case
          FROM private.banking_pay_workbench_economic_build_facts fact
          WHERE fact.build_id=v_build_id
            AND state_row.timesheet_id=ANY(fact.subject_timesheet_ids)
            AND fact.fact_family IN ('ENTITLEMENT_COMPONENT','RESERVATION_COMPONENT','FINANCE_CASE_IDENTITY','FINANCE_COMPONENT_IDENTITY')
          GROUP BY fact.economic_key_type,fact.economic_key_value
        ) authority
        WHERE authority.truth_ex_vat IS DISTINCT FROM authority.baseline_ex_vat
          OR authority.reserved_source_amount<>0 OR authority.has_finance_case
      ) THEN 'LIVE' ELSE 'CLOSED' END,
      evaluated_generation=v_build.captured_candidate_generation,
      current_input_fingerprint=scope_row.captured_input_fingerprint,
      evaluated_input_fingerprint=scope_row.captured_input_fingerprint,
      current_build_id=NULL,last_evaluated_at_utc=clock_timestamp(),
      closed_at_utc=CASE WHEN EXISTS(
        SELECT 1
        FROM (
          SELECT fact.economic_key_type,fact.economic_key_value,
            ROUND(SUM(COALESCE(fact.truth_ex_vat,0)),2) AS truth_ex_vat,
            ROUND(SUM(COALESCE(fact.baseline_ex_vat,0)),2) AS baseline_ex_vat,
            ROUND(SUM(COALESCE(fact.reserved_source_amount,0)),2) AS reserved_source_amount,
            bool_or(fact.finance_case_id IS NOT NULL) AS has_finance_case
          FROM private.banking_pay_workbench_economic_build_facts fact
          WHERE fact.build_id=v_build_id
            AND state_row.timesheet_id=ANY(fact.subject_timesheet_ids)
            AND fact.fact_family IN ('ENTITLEMENT_COMPONENT','RESERVATION_COMPONENT','FINANCE_CASE_IDENTITY','FINANCE_COMPONENT_IDENTITY')
          GROUP BY fact.economic_key_type,fact.economic_key_value
        ) authority
        WHERE authority.truth_ex_vat IS DISTINCT FROM authority.baseline_ex_vat
          OR authority.reserved_source_amount<>0 OR authority.has_finance_case
      ) THEN NULL ELSE clock_timestamp() END,updated_at_utc=clock_timestamp()
    FROM private.banking_pay_workbench_economic_build_scope scope_row
    WHERE scope_row.build_id=v_build_id AND state_row.timesheet_id=scope_row.timesheet_id;
    UPDATE private.banking_pay_workbench_candidate_scope_registry SET initialisation_status='READY',
      evaluated_generation=dirty_generation,current_build_id=NULL,last_evaluated_at_utc=clock_timestamp(),
      initialised_at_utc=COALESCE(initialised_at_utc,clock_timestamp()),failure_json='{}'::jsonb,
      updated_at_utc=clock_timestamp() WHERE candidate_id=p_candidate_id;
    UPDATE private.banking_pay_workbench_economic_builds SET status='COMPLETE',private_stage='COMPLETE',
      publication_cursor_json=jsonb_build_object('cursor_kind','SOURCE_PUBLISH','cursor_version',1,
        'terminal',true,'published_count',v_published_count,'published_digest',v_published_digest),
      completed_at_utc=clock_timestamp(),cleanup_not_before_utc=clock_timestamp()+interval '7 days',
      updated_at_utc=clock_timestamp() WHERE id=v_build_id;
    RETURN jsonb_build_object('ok',true,'build_id',v_build_id,'private_stage','COMPLETE',
      'stage_status','COMPLETE','has_more',false,'continuation_enqueued',false,
      'next_action','COMPLETE','published_count',v_published_count,'published_digest',v_published_digest);
  END IF;

  IF v_stage='BUILD_CLEANUP' THEN
    v_cleanup_kind:=COALESCE(NULLIF(v_cursor->>'cleanup_kind',''),'CANONICAL_STAGE');
    IF v_build.status<>'CLEANING' OR v_build.cleanup_not_before_utc IS NULL
       OR clock_timestamp()<v_build.cleanup_not_before_utc
       OR v_registry.current_build_id IS NOT DISTINCT FROM v_build_id
       OR EXISTS(SELECT 1 FROM public.banking_pay_workbench_jobs cleanup_job
         WHERE cleanup_job.economic_build_id=v_build_id
           AND cleanup_job.id<>(SELECT job_id FROM private.banking_pay_workbench_stage_attempts WHERE id=v_attempt_id)
           AND cleanup_job.status IN ('QUEUED','RUNNING') LIMIT 1)
       OR EXISTS(SELECT 1 FROM private.banking_pay_workbench_stage_attempts cleanup_attempt
         WHERE cleanup_attempt.build_id=v_build_id AND cleanup_attempt.id<>v_attempt_id
           AND cleanup_attempt.attempt_status='STARTED' LIMIT 1) THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_CLEANUP_BUILD_PROTECTED' USING ERRCODE='55006';
    END IF;
    IF v_cleanup_kind='CANONICAL_STAGE' THEN
      WITH doomed AS (SELECT ctid FROM private.banking_pay_workbench_canonical_stage_lines
        WHERE build_id=v_build_id ORDER BY source_ordinal LIMIT LEAST(v_limit,500))
      DELETE FROM private.banking_pay_workbench_canonical_stage_lines row USING doomed WHERE row.ctid=doomed.ctid;
      GET DIAGNOSTICS v_cleanup_count=ROW_COUNT;
      IF v_cleanup_count<LEAST(v_limit,500) THEN v_cleanup_kind:='FACT_PAGES'; END IF;
    ELSIF v_cleanup_kind='FACT_PAGES' THEN
      WITH doomed AS (SELECT ctid FROM private.banking_pay_workbench_economic_build_fact_pages
        WHERE build_id=v_build_id ORDER BY id LIMIT LEAST(v_limit,500))
      DELETE FROM private.banking_pay_workbench_economic_build_fact_pages row USING doomed WHERE row.ctid=doomed.ctid;
      GET DIAGNOSTICS v_cleanup_count=ROW_COUNT;
      IF v_cleanup_count<LEAST(v_limit,500) THEN v_cleanup_kind:='FACTS'; END IF;
    ELSIF v_cleanup_kind='FACTS' THEN
      WITH doomed AS (SELECT ctid FROM private.banking_pay_workbench_economic_build_facts
        WHERE build_id=v_build_id ORDER BY fact_family,natural_key LIMIT LEAST(v_limit,500))
      DELETE FROM private.banking_pay_workbench_economic_build_facts row USING doomed WHERE row.ctid=doomed.ctid;
      GET DIAGNOSTICS v_cleanup_count=ROW_COUNT;
      IF v_cleanup_count<LEAST(v_limit,500) THEN v_cleanup_kind:='SCOPE'; END IF;
    ELSIF v_cleanup_kind='SCOPE' THEN
      WITH doomed AS (SELECT ctid FROM private.banking_pay_workbench_economic_build_scope
        WHERE build_id=v_build_id ORDER BY timesheet_id LIMIT LEAST(v_limit,500))
      DELETE FROM private.banking_pay_workbench_economic_build_scope row USING doomed WHERE row.ctid=doomed.ctid;
      GET DIAGNOSTICS v_cleanup_count=ROW_COUNT;
      IF v_cleanup_count<LEAST(v_limit,500) THEN v_cleanup_kind:='ATTEMPTS'; END IF;
    ELSIF v_cleanup_kind='ATTEMPTS' THEN
      WITH doomed AS (SELECT ctid FROM private.banking_pay_workbench_stage_attempts
        WHERE build_id=v_build_id AND id<>v_attempt_id ORDER BY id LIMIT LEAST(v_limit,500))
      DELETE FROM private.banking_pay_workbench_stage_attempts row USING doomed WHERE row.ctid=doomed.ctid;
      GET DIAGNOSTICS v_cleanup_count=ROW_COUNT;
      IF v_cleanup_count<LEAST(v_limit,500) THEN v_cleanup_kind:='HEADER_FINALISE'; END IF;
    ELSIF v_cleanup_kind='HEADER_FINALISE' THEN
      IF EXISTS(SELECT 1 FROM private.banking_pay_workbench_canonical_stage_lines WHERE build_id=v_build_id LIMIT 1)
         OR EXISTS(SELECT 1 FROM private.banking_pay_workbench_economic_build_fact_pages WHERE build_id=v_build_id LIMIT 1)
         OR EXISTS(SELECT 1 FROM private.banking_pay_workbench_economic_build_facts WHERE build_id=v_build_id LIMIT 1)
         OR EXISTS(SELECT 1 FROM private.banking_pay_workbench_economic_build_scope WHERE build_id=v_build_id LIMIT 1)
         OR EXISTS(SELECT 1 FROM private.banking_pay_workbench_stage_attempts
           WHERE build_id=v_build_id AND id<>v_attempt_id LIMIT 1) THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_CLEANUP_CHILDREN_REMAIN' USING ERRCODE='23514';
      END IF;
      v_cleanup_kind:='COMPLETE';
      v_cleanup_count:=0;
    ELSIF v_cleanup_kind<>'COMPLETE' THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_BUILD_CURSOR_INVALID' USING ERRCODE='22023';
    END IF;
    v_next:=jsonb_build_object('cursor_kind','BUILD_CLEANUP','cursor_version',1,
      'build_id',v_build_id,'candidate_id',p_candidate_id,'cleanup_kind',v_cleanup_kind,
      'deleted_count',v_cleanup_count,'terminal',v_cleanup_kind='COMPLETE');
    UPDATE private.banking_pay_workbench_economic_builds SET cleanup_cursor_json=v_next,
      status=CASE WHEN v_cleanup_kind='COMPLETE' THEN 'COMPLETE' ELSE status END,
      private_stage=CASE WHEN v_cleanup_kind='COMPLETE' THEN 'COMPLETE' ELSE private_stage END,
      updated_at_utc=clock_timestamp() WHERE id=v_build_id;
    RETURN jsonb_build_object('ok',true,'build_id',v_build_id,'private_stage','BUILD_CLEANUP',
      'stage_status',CASE WHEN v_cleanup_kind='COMPLETE' THEN 'COMPLETE' ELSE 'CLEANING' END,
      'has_more',v_cleanup_kind<>'COMPLETE','continuation_enqueued',false,
      'next_cursor_json',v_next,'next_action',CASE WHEN v_cleanup_kind='COMPLETE' THEN 'COMPLETE' ELSE 'BUILD_CLEANUP' END,
      'deleted_count',v_cleanup_count);
  END IF;

  IF v_stage='BOOTSTRAP_DISCOVERY' THEN
    v_bootstrap_id:=NULLIF(v_cursor->>'bootstrap_id','')::uuid;
    v_bootstrap_stream:=upper(COALESCE(NULLIF(v_cursor->>'bootstrap_stream',''),v_registry.bootstrap_stream));
    v_bootstrap_last_key:=NULLIF(v_cursor->>'last_source_key','');
    IF v_bootstrap_id IS NULL OR v_bootstrap_id IS DISTINCT FROM v_registry.bootstrap_id
       OR v_registry.initialisation_status NOT IN ('DISCOVERING','CLASSIFYING') THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_BOOTSTRAP_CURSOR_INVALID' USING ERRCODE='22023';
    END IF;

    CREATE TEMP TABLE IF NOT EXISTS pg_temp._bpay_wb_bootstrap_page_v1(
      source_key text PRIMARY KEY,
      timesheet_id uuid NULL
    ) ON COMMIT DROP;
    TRUNCATE pg_temp._bpay_wb_bootstrap_page_v1;

    IF v_bootstrap_stream='TIMESHEETS_TSFIN' THEN
      INSERT INTO pg_temp._bpay_wb_bootstrap_page_v1
      SELECT row_key,timesheet_id FROM (
        SELECT 'TIMESHEET:'||timesheet_row.timesheet_id::text row_key,timesheet_row.timesheet_id
        FROM public.timesheets timesheet_row
        JOIN public.contracts contract_row ON contract_row.id=timesheet_row.contract_id
        WHERE contract_row.candidate_id=p_candidate_id
        UNION ALL
        SELECT 'TSFIN:'||financial.id::text row_key,financial.timesheet_id
        FROM public.timesheets_financials financial
        WHERE financial.candidate_id=p_candidate_id AND financial.is_current
      ) rows WHERE v_bootstrap_last_key IS NULL OR row_key>v_bootstrap_last_key
      ORDER BY row_key LIMIT 251;
      v_bootstrap_next_stream:='BATCH_ARTIFACTS';
    ELSIF v_bootstrap_stream='BATCH_ARTIFACTS' THEN
      INSERT INTO pg_temp._bpay_wb_bootstrap_page_v1
      SELECT row_key,timesheet_id FROM (
        SELECT 'ITEM:'||item.id::text row_key,item.timesheet_id
        FROM public.pay_batch_items item
        JOIN public.pay_batch_candidates batch_candidate ON batch_candidate.id=item.pay_batch_candidate_id
        WHERE batch_candidate.candidate_id=p_candidate_id AND item.timesheet_id IS NOT NULL
        UNION ALL
        SELECT 'SNAPSHOT:'||snapshot.id::text,snapshot.timesheet_id
        FROM public.pay_batch_timesheet_snapshots snapshot
        WHERE snapshot.candidate_id=p_candidate_id
      ) rows WHERE v_bootstrap_last_key IS NULL OR row_key>v_bootstrap_last_key
      ORDER BY row_key LIMIT 251;
      v_bootstrap_next_stream:='CORRECTIONS';
    ELSIF v_bootstrap_stream='CORRECTIONS' THEN
      INSERT INTO pg_temp._bpay_wb_bootstrap_page_v1
      SELECT 'CORRECTION:'||correction.id::text,correction.timesheet_id
      FROM public.pay_payment_correction_items correction
      WHERE correction.candidate_id=p_candidate_id AND correction.timesheet_id IS NOT NULL
        AND (v_bootstrap_last_key IS NULL OR 'CORRECTION:'||correction.id::text>v_bootstrap_last_key)
      ORDER BY correction.id LIMIT 251;
      v_bootstrap_next_stream:='FINANCE';
    ELSIF v_bootstrap_stream='FINANCE' THEN
      INSERT INTO pg_temp._bpay_wb_bootstrap_page_v1
      SELECT row_key,timesheet_id FROM (
        SELECT 'ADVANCE:'||finance_case.id::text row_key,finance_case.linked_timesheet_id timesheet_id
        FROM public.pay_advances finance_case WHERE finance_case.candidate_id=p_candidate_id
        UNION ALL
        SELECT 'COMPONENT:'||component.id::text,component.linked_timesheet_id
        FROM public.pay_finance_case_components component WHERE component.candidate_id=p_candidate_id
        UNION ALL
        SELECT 'EVENT:'||event.id::text,component.linked_timesheet_id
        FROM public.pay_finance_case_events event
        JOIN public.pay_advances finance_case ON finance_case.id=event.finance_case_id
        LEFT JOIN public.pay_finance_case_components component ON component.id=event.finance_component_id
        WHERE finance_case.candidate_id=p_candidate_id
      ) rows WHERE v_bootstrap_last_key IS NULL OR row_key>v_bootstrap_last_key
      ORDER BY row_key LIMIT 251;
      v_bootstrap_next_stream:='RESERVATIONS';
    ELSIF v_bootstrap_stream='RESERVATIONS' THEN
      INSERT INTO pg_temp._bpay_wb_bootstrap_page_v1
      SELECT 'RESERVATION:'||reservation.id::text,COALESCE(item.timesheet_id,component.linked_timesheet_id)
      FROM public.pay_advance_reservations reservation
      LEFT JOIN public.pay_batch_candidates batch_candidate ON batch_candidate.id=reservation.pay_batch_candidate_id
      LEFT JOIN public.pay_batch_items item ON item.id=reservation.pay_batch_item_id
      LEFT JOIN public.pay_finance_case_components component ON component.id=reservation.finance_component_id
      WHERE COALESCE(batch_candidate.candidate_id,component.candidate_id)=p_candidate_id
        AND (v_bootstrap_last_key IS NULL OR 'RESERVATION:'||reservation.id::text>v_bootstrap_last_key)
      ORDER BY reservation.id LIMIT 251;
      v_bootstrap_next_stream:='OVERRIDES_ADJUSTMENTS';
    ELSIF v_bootstrap_stream='OVERRIDES_ADJUSTMENTS' THEN
      INSERT INTO pg_temp._bpay_wb_bootstrap_page_v1
      SELECT row_key,timesheet_id FROM (
        SELECT 'OVERRIDE:'||override_row.id::text row_key,override_row.timesheet_id
        FROM public.timesheet_payment_overrides override_row WHERE override_row.candidate_id=p_candidate_id
        UNION ALL
        SELECT 'ADJUSTMENT:'||adjustment.id::text,adjustment.timesheet_id
        FROM public.ts_pay_adjustments adjustment WHERE adjustment.candidate_id=p_candidate_id
      ) rows WHERE v_bootstrap_last_key IS NULL OR row_key>v_bootstrap_last_key
      ORDER BY row_key LIMIT 251;
      v_bootstrap_next_stream:='SNOOZES';
    ELSIF v_bootstrap_stream='SNOOZES' THEN
      INSERT INTO pg_temp._bpay_wb_bootstrap_page_v1
      SELECT 'SNOOZE:'||snooze.id::text,snooze.timesheet_id
      FROM public.pay_item_snoozes snooze
      WHERE snooze.candidate_id=p_candidate_id AND snooze.timesheet_id IS NOT NULL
        AND (v_bootstrap_last_key IS NULL OR 'SNOOZE:'||snooze.id::text>v_bootstrap_last_key)
      ORDER BY snooze.id LIMIT 251;
      v_bootstrap_next_stream:='CURRENT_SOURCE';
    ELSIF v_bootstrap_stream='CURRENT_SOURCE' THEN
      INSERT INTO pg_temp._bpay_wb_bootstrap_page_v1
      SELECT 'SOURCE:'||source_row.id::text,source_row.timesheet_id
      FROM public.banking_pay_workbench_candidate_source_lines source_row
      WHERE source_row.candidate_id=p_candidate_id AND source_row.status IN ('CURRENT','DIRTY')
        AND source_row.timesheet_id IS NOT NULL
        AND (v_bootstrap_last_key IS NULL OR 'SOURCE:'||source_row.id::text>v_bootstrap_last_key)
      ORDER BY source_row.id LIMIT 251;
      v_bootstrap_next_stream:='SCOPE_CLOSURE';
    ELSIF v_bootstrap_stream='CLASSIFY_UNITS' THEN
      v_bootstrap_classification_phase:=upper(COALESCE(NULLIF(v_cursor->>'classification_phase',''),'EVIDENCE'));
      v_bootstrap_unit_key:=NULLIF(v_cursor->>'dependency_unit_key','');
      v_bootstrap_last_unit_key:=NULLIF(v_cursor->>'last_dependency_unit_key','');
      v_bootstrap_last_ordinal:=GREATEST(COALESCE((v_cursor->>'last_stable_ordinal')::bigint,0),0);
      v_bootstrap_unit_relevant:=COALESCE((v_cursor->>'unit_financially_relevant')::boolean,false);
      v_bootstrap_raw_current_count:=GREATEST(COALESCE((v_cursor->>'raw_current_occurrence_count')::bigint,0),0);
      v_bootstrap_resolved_current_count:=GREATEST(COALESCE((v_cursor->>'resolved_current_occurrence_count')::bigint,0),0);
      v_bootstrap_raw_baseline_count:=GREATEST(COALESCE((v_cursor->>'raw_baseline_occurrence_count')::bigint,0),0);
      v_bootstrap_resolved_baseline_count:=GREATEST(COALESCE((v_cursor->>'resolved_baseline_occurrence_count')::bigint,0),0);

      IF v_bootstrap_classification_phase='EVIDENCE' THEN
        IF v_bootstrap_unit_key IS NULL THEN
          SELECT scope_row.dependency_unit_key INTO v_bootstrap_unit_key
          FROM private.banking_pay_workbench_economic_build_scope scope_row
          WHERE scope_row.build_id=v_build_id AND scope_row.closure_status='SEALED'
            AND (v_bootstrap_last_unit_key IS NULL
              OR scope_row.dependency_unit_key>v_bootstrap_last_unit_key)
          ORDER BY scope_row.dependency_unit_key LIMIT 1;
        END IF;
        IF v_bootstrap_unit_key IS NULL THEN
          v_next:=jsonb_build_object('cursor_kind','BOOTSTRAP_DISCOVERY','cursor_version',1,
            'bootstrap_id',v_bootstrap_id,'bootstrap_stream','RESET_FACTS',
            'last_source_key',NULL,'build_id',v_build_id,'candidate_id',p_candidate_id,
            'captured_candidate_generation',v_build.captured_candidate_generation,
            'captured_source_change_seq',v_build.source_change_seq);
          UPDATE private.banking_pay_workbench_candidate_scope_registry
          SET bootstrap_stream='RESET_FACTS',bootstrap_cursor_json=v_next,
              updated_at_utc=clock_timestamp()
          WHERE candidate_id=p_candidate_id AND current_build_id=v_build_id;
          -- The sealed seed cursor remains immutable. Bootstrap continuation is
          -- owned by the registry's dedicated bootstrap_cursor_json.
          UPDATE private.banking_pay_workbench_economic_builds
          SET updated_at_utc=clock_timestamp() WHERE id=v_build_id;
          RETURN jsonb_build_object('ok',true,'build_id',v_build_id,
            'private_stage','BOOTSTRAP_DISCOVERY','stage_status','CLASSIFYING',
            'has_more',true,'continuation_enqueued',false,'next_cursor_json',v_next,
            'next_action','BOOTSTRAP_DISCOVERY','classification_phase','RESET_FACTS');
        END IF;

        INSERT INTO pg_temp._bpay_wb_bootstrap_page_v1(source_key,timesheet_id)
        SELECT lpad(scope_row.stable_ordinal::text,20,'0'),scope_row.timesheet_id
        FROM private.banking_pay_workbench_economic_build_scope scope_row
        WHERE scope_row.build_id=v_build_id
          AND scope_row.dependency_unit_key=v_bootstrap_unit_key
          AND scope_row.stable_ordinal>v_bootstrap_last_ordinal
        ORDER BY scope_row.stable_ordinal LIMIT 26;
        SELECT count(*)>25 INTO v_bootstrap_has_more FROM pg_temp._bpay_wb_bootstrap_page_v1;
        SELECT count(*)::integer,COALESCE(max(source_key)::bigint,v_bootstrap_last_ordinal)
        INTO v_bootstrap_page_count,v_bootstrap_last_ordinal
        FROM (SELECT source_key FROM pg_temp._bpay_wb_bootstrap_page_v1
          ORDER BY source_key LIMIT 25) page;
        SELECT EXISTS(
          SELECT 1
          FROM (SELECT timesheet_id FROM pg_temp._bpay_wb_bootstrap_page_v1
            ORDER BY source_key LIMIT 25) page
          WHERE EXISTS(SELECT 1 FROM public.timesheets_financials financial
              WHERE financial.timesheet_id=page.timesheet_id
                AND financial.candidate_id=p_candidate_id AND financial.is_current
                AND financial.paid_at_utc IS NULL)
            OR EXISTS(SELECT 1 FROM public.banking_pay_workbench_candidate_source_lines source_row
              WHERE source_row.candidate_id=p_candidate_id
                AND source_row.timesheet_id=page.timesheet_id
                AND source_row.status IN ('CURRENT','DIRTY'))
            OR EXISTS(SELECT 1 FROM public.pay_advances finance_case
              WHERE finance_case.candidate_id=p_candidate_id
                AND finance_case.linked_timesheet_id=page.timesheet_id
                AND finance_case.cleared_at_utc IS NULL
                AND finance_case.written_off_at_utc IS NULL)
            OR EXISTS(SELECT 1 FROM public.pay_finance_case_components component
              WHERE component.candidate_id=p_candidate_id
                AND component.linked_timesheet_id=page.timesheet_id
                AND component.closed_at_utc IS NULL)
            OR EXISTS(SELECT 1 FROM public.timesheet_payment_overrides override_row
              WHERE override_row.candidate_id=p_candidate_id
                AND override_row.timesheet_id=page.timesheet_id
                AND override_row.cleared_at_utc IS NULL
                AND override_row.consumed_at_utc IS NULL
                AND override_row.consumed_by_pay_batch_id IS NULL)
            OR EXISTS(SELECT 1 FROM public.ts_pay_adjustments adjustment
              WHERE adjustment.candidate_id=p_candidate_id
                AND adjustment.timesheet_id=page.timesheet_id
                AND adjustment.paid_at_utc IS NULL)
            OR EXISTS(SELECT 1 FROM public.pay_item_snoozes snooze
              WHERE snooze.candidate_id=p_candidate_id
                AND snooze.timesheet_id=page.timesheet_id
                AND snooze.cleared_at_utc IS NULL)
            OR EXISTS(SELECT 1 FROM public.pay_advance_reservations reservation
              LEFT JOIN public.pay_batch_items item ON item.id=reservation.pay_batch_item_id
              LEFT JOIN public.pay_finance_case_components component
                ON component.id=reservation.finance_component_id
              WHERE COALESCE(item.timesheet_id,component.linked_timesheet_id)=page.timesheet_id
                AND reservation.status NOT IN ('RELEASED','SETTLED'))
            OR EXISTS(
              SELECT 1
              FROM private.pay_current_timesheet_entitlement_components_from_build_v1(
                v_build_id,v_bootstrap_unit_key) component
              WHERE component.timesheet_id=page.timesheet_id
                AND (ROUND(COALESCE(component.truth_ex_vat,0),2)
                      IS DISTINCT FROM ROUND(COALESCE(component.baseline_ex_vat,0),2)
                  OR ROUND(COALESCE(component.truth_inc_vat,0),2)
                      IS DISTINCT FROM ROUND(COALESCE(component.baseline_inc_vat,0),2))
            )
            OR (
              NOT EXISTS(
                SELECT 1
                FROM private.pay_current_timesheet_entitlement_components_from_build_v1(
                  v_build_id,v_bootstrap_unit_key) component
                WHERE component.timesheet_id=page.timesheet_id
              )
              AND (
                EXISTS(SELECT 1
                  FROM private.banking_pay_workbench_economic_build_facts fact
                  WHERE fact.build_id=v_build_id AND fact.timesheet_id=page.timesheet_id
                    AND fact.fact_family IN ('LIVE_ENTITLEMENT_INPUT',
                      'FROZEN_SETTLED_COMPONENT','PAY_STATE_FALLBACK')
                    AND fact.source_relation<>'UNIT_PROJECTION')
                OR EXISTS(SELECT 1 FROM public.timesheets_financials financial
                  WHERE financial.timesheet_id=page.timesheet_id
                    AND financial.candidate_id=p_candidate_id AND financial.is_current
                    AND ABS(COALESCE(financial.total_pay_ex_vat,0))>0.005)
                OR EXISTS(SELECT 1 FROM public.timesheet_pay_state state
                  WHERE state.timesheet_id=page.timesheet_id
                    AND COALESCE(state.last_settled_snapshot_json,'{}'::jsonb)<>'{}'::jsonb)
              )
            )
          LIMIT 1
        ) INTO v_bootstrap_page_relevant;
        SELECT
          COALESCE(SUM((page.cursor_end_json->>'raw_physical_source_count')::bigint)
            FILTER(WHERE page.fact_family='LIVE_ENTITLEMENT_INPUT'),0),
          COALESCE(SUM((page.cursor_end_json->>'resolved_physical_source_count')::bigint)
            FILTER(WHERE page.fact_family='LIVE_ENTITLEMENT_INPUT'),0),
          COALESCE(SUM((page.cursor_end_json->>'raw_physical_source_count')::bigint)
            FILTER(WHERE page.fact_family IN ('FROZEN_SETTLED_COMPONENT','PAY_STATE_FALLBACK')),0),
          COALESCE(SUM((page.cursor_end_json->>'resolved_physical_source_count')::bigint)
            FILTER(WHERE page.fact_family IN ('FROZEN_SETTLED_COMPONENT','PAY_STATE_FALLBACK')),0),
          COUNT(*)=3
            AND BOOL_AND(page.expected_source_count IS NOT NULL
              AND page.expected_source_count=(page.cursor_end_json->>'raw_physical_source_count')::integer
              AND (page.cursor_end_json->>'raw_physical_source_count')::bigint=
                (page.cursor_end_json->>'resolved_physical_source_count')::bigint
              AND (page.cursor_end_json->>'failed_physical_source_count')::bigint=0
              AND ROUND((page.cursor_end_json->>'raw_physical_amount_ex_vat')::numeric,2)=
                ROUND((page.cursor_end_json->>'resolved_physical_amount_ex_vat')::numeric,2)
              AND COALESCE((page.cursor_end_json->>'source_exhausted')::boolean,false))
        INTO v_bootstrap_raw_current_count,v_bootstrap_resolved_current_count,
          v_bootstrap_raw_baseline_count,v_bootstrap_resolved_baseline_count,
          v_bootstrap_evidence_complete
        FROM private.banking_pay_workbench_economic_build_fact_pages page
        WHERE page.build_id=v_build_id AND page.dependency_unit_key=v_bootstrap_unit_key
          AND page.fact_family IN ('LIVE_ENTITLEMENT_INPUT','FROZEN_SETTLED_COMPONENT','PAY_STATE_FALLBACK')
          AND page.status='COMPLETED' AND page.is_family_final;
        v_bootstrap_unit_relevant:=v_bootstrap_unit_relevant OR v_bootstrap_page_relevant;
        v_bootstrap_evidence_complete:=COALESCE(v_bootstrap_evidence_complete,false)
          AND v_bootstrap_raw_current_count=v_bootstrap_resolved_current_count
          AND v_bootstrap_raw_baseline_count=v_bootstrap_resolved_baseline_count;
        IF v_bootstrap_has_more THEN
          v_next:=jsonb_build_object('cursor_kind','BOOTSTRAP_DISCOVERY','cursor_version',1,
            'bootstrap_id',v_bootstrap_id,'bootstrap_stream','CLASSIFY_UNITS',
            'classification_phase','EVIDENCE','last_dependency_unit_key',v_bootstrap_last_unit_key,
            'dependency_unit_key',v_bootstrap_unit_key,'last_stable_ordinal',v_bootstrap_last_ordinal,
            'unit_financially_relevant',v_bootstrap_unit_relevant,
            'raw_current_occurrence_count',v_bootstrap_raw_current_count,
            'resolved_current_occurrence_count',v_bootstrap_resolved_current_count,
            'raw_baseline_occurrence_count',v_bootstrap_raw_baseline_count,
            'resolved_baseline_occurrence_count',v_bootstrap_resolved_baseline_count,
            'evidence_complete',v_bootstrap_evidence_complete,'build_id',v_build_id,
            'candidate_id',p_candidate_id,
            'captured_candidate_generation',v_build.captured_candidate_generation,
            'captured_source_change_seq',v_build.source_change_seq);
        ELSE
          v_next:=jsonb_build_object('cursor_kind','BOOTSTRAP_DISCOVERY','cursor_version',1,
            'bootstrap_id',v_bootstrap_id,'bootstrap_stream','CLASSIFY_UNITS',
            'classification_phase','APPLY','last_dependency_unit_key',v_bootstrap_last_unit_key,
            'dependency_unit_key',v_bootstrap_unit_key,'last_stable_ordinal',0,
            'unit_financially_relevant',v_bootstrap_unit_relevant,
            'raw_current_occurrence_count',v_bootstrap_raw_current_count,
            'resolved_current_occurrence_count',v_bootstrap_resolved_current_count,
            'raw_baseline_occurrence_count',v_bootstrap_raw_baseline_count,
            'resolved_baseline_occurrence_count',v_bootstrap_resolved_baseline_count,
            'evidence_complete',v_bootstrap_evidence_complete,'build_id',v_build_id,
            'candidate_id',p_candidate_id,
            'captured_candidate_generation',v_build.captured_candidate_generation,
            'captured_source_change_seq',v_build.source_change_seq);
        END IF;
      ELSIF v_bootstrap_classification_phase='APPLY' THEN
        IF v_bootstrap_unit_key IS NULL THEN
          RAISE EXCEPTION 'PAY_WORKBENCH_BOOTSTRAP_CURSOR_INVALID' USING ERRCODE='22023';
        END IF;
        SELECT COUNT(*)=3 AND BOOL_AND(
            page.expected_source_count IS NOT NULL
            AND page.expected_source_count=(page.cursor_end_json->>'raw_physical_source_count')::integer
            AND (page.cursor_end_json->>'raw_physical_source_count')::bigint=
              (page.cursor_end_json->>'resolved_physical_source_count')::bigint
            AND (page.cursor_end_json->>'failed_physical_source_count')::bigint=0
            AND ROUND((page.cursor_end_json->>'raw_physical_amount_ex_vat')::numeric,2)=
              ROUND((page.cursor_end_json->>'resolved_physical_amount_ex_vat')::numeric,2)
            AND COALESCE((page.cursor_end_json->>'source_exhausted')::boolean,false))
        INTO v_bootstrap_evidence_complete
        FROM private.banking_pay_workbench_economic_build_fact_pages page
        WHERE page.build_id=v_build_id AND page.dependency_unit_key=v_bootstrap_unit_key
          AND page.fact_family IN ('LIVE_ENTITLEMENT_INPUT','FROZEN_SETTLED_COMPONENT','PAY_STATE_FALLBACK')
          AND page.status='COMPLETED' AND page.is_family_final;
        v_bootstrap_evidence_complete:=COALESCE(v_bootstrap_evidence_complete,false)
          AND COALESCE((v_cursor->>'evidence_complete')::boolean,false)
          AND v_bootstrap_raw_current_count=v_bootstrap_resolved_current_count
          AND v_bootstrap_raw_baseline_count=v_bootstrap_resolved_baseline_count
          AND NOT EXISTS(
            SELECT 1 FROM private.banking_pay_workbench_economic_build_scope scope_row
            WHERE scope_row.build_id=v_build_id
              AND scope_row.dependency_unit_key=v_bootstrap_unit_key
              AND (scope_row.closure_status<>'SEALED'
                OR NOT scope_row.required_fact_families <@ scope_row.completed_fact_families))
          AND NOT EXISTS(
            SELECT 1 FROM private.banking_pay_workbench_economic_build_facts fact
            WHERE fact.build_id=v_build_id AND fact.dependency_unit_key=v_bootstrap_unit_key
              AND NULLIF(BTRIM(fact.source_payload_json->>'resolution_failure'),'') IS NOT NULL);
        IF NOT v_bootstrap_evidence_complete THEN
          RAISE EXCEPTION 'PAY_WORKBENCH_BOOTSTRAP_CLOSED_AUTHORITY_UNPROVED'
            USING ERRCODE='23514';
        END IF;
        INSERT INTO pg_temp._bpay_wb_bootstrap_page_v1(source_key,timesheet_id)
        SELECT lpad(scope_row.stable_ordinal::text,20,'0'),scope_row.timesheet_id
        FROM private.banking_pay_workbench_economic_build_scope scope_row
        WHERE scope_row.build_id=v_build_id
          AND scope_row.dependency_unit_key=v_bootstrap_unit_key
          AND scope_row.stable_ordinal>v_bootstrap_last_ordinal
        ORDER BY scope_row.stable_ordinal LIMIT 26;
        SELECT count(*)>25 INTO v_bootstrap_has_more FROM pg_temp._bpay_wb_bootstrap_page_v1;
        SELECT count(*)::integer,COALESCE(max(source_key)::bigint,v_bootstrap_last_ordinal)
        INTO v_bootstrap_page_count,v_bootstrap_last_ordinal
        FROM (SELECT source_key FROM pg_temp._bpay_wb_bootstrap_page_v1
          ORDER BY source_key LIMIT 25) page;
        UPDATE private.banking_pay_workbench_timesheet_scope_state state_row SET
          economic_state=CASE WHEN v_bootstrap_unit_relevant THEN 'DIRTY' ELSE 'CLOSED' END,
          evaluated_generation=CASE WHEN v_bootstrap_unit_relevant
            THEN state_row.evaluated_generation ELSE state_row.dirty_generation END,
          current_input_fingerprint=CASE WHEN v_bootstrap_unit_relevant
            THEN state_row.current_input_fingerprint ELSE scope_row.captured_input_fingerprint END,
          evaluated_input_fingerprint=CASE WHEN v_bootstrap_unit_relevant
            THEN state_row.evaluated_input_fingerprint ELSE scope_row.captured_input_fingerprint END,
          last_dirty_reason=CASE WHEN v_bootstrap_unit_relevant
            THEN 'LEGACY_ACTIVE_UNIT_CLASSIFIED' ELSE 'LEGACY_CLOSED_UNIT_CLASSIFIED' END,
          last_evaluated_at_utc=CASE WHEN v_bootstrap_unit_relevant
            THEN state_row.last_evaluated_at_utc ELSE clock_timestamp() END,
          closed_at_utc=CASE WHEN v_bootstrap_unit_relevant THEN NULL ELSE clock_timestamp() END,
          current_build_id=CASE WHEN v_bootstrap_unit_relevant THEN v_build_id ELSE NULL END,
          updated_at_utc=clock_timestamp()
        FROM private.banking_pay_workbench_economic_build_scope scope_row
        JOIN (SELECT timesheet_id FROM pg_temp._bpay_wb_bootstrap_page_v1
          ORDER BY source_key LIMIT 25) page ON page.timesheet_id=scope_row.timesheet_id
        WHERE scope_row.build_id=v_build_id
          AND state_row.timesheet_id=scope_row.timesheet_id
          AND state_row.dirty_generation<=v_build.captured_candidate_generation;
        IF v_bootstrap_has_more THEN
          v_next:=jsonb_build_object('cursor_kind','BOOTSTRAP_DISCOVERY','cursor_version',1,
            'bootstrap_id',v_bootstrap_id,'bootstrap_stream','CLASSIFY_UNITS',
            'classification_phase','APPLY','last_dependency_unit_key',v_bootstrap_last_unit_key,
            'dependency_unit_key',v_bootstrap_unit_key,'last_stable_ordinal',v_bootstrap_last_ordinal,
            'unit_financially_relevant',v_bootstrap_unit_relevant,
            'raw_current_occurrence_count',v_bootstrap_raw_current_count,
            'resolved_current_occurrence_count',v_bootstrap_resolved_current_count,
            'raw_baseline_occurrence_count',v_bootstrap_raw_baseline_count,
            'resolved_baseline_occurrence_count',v_bootstrap_resolved_baseline_count,
            'evidence_complete',v_bootstrap_evidence_complete,'build_id',v_build_id,
            'candidate_id',p_candidate_id,
            'captured_candidate_generation',v_build.captured_candidate_generation,
            'captured_source_change_seq',v_build.source_change_seq);
        ELSE
          v_next:=jsonb_build_object('cursor_kind','BOOTSTRAP_DISCOVERY','cursor_version',1,
            'bootstrap_id',v_bootstrap_id,'bootstrap_stream','CLASSIFY_UNITS',
            'classification_phase','EVIDENCE','last_dependency_unit_key',v_bootstrap_unit_key,
            'dependency_unit_key',NULL,'last_stable_ordinal',0,
            'unit_financially_relevant',false,'build_id',v_build_id,
            'candidate_id',p_candidate_id,
            'captured_candidate_generation',v_build.captured_candidate_generation,
            'captured_source_change_seq',v_build.source_change_seq);
        END IF;
      ELSE
        RAISE EXCEPTION 'PAY_WORKBENCH_BOOTSTRAP_CURSOR_INVALID' USING ERRCODE='22023';
      END IF;
      UPDATE private.banking_pay_workbench_candidate_scope_registry
      SET initialisation_status='CLASSIFYING',bootstrap_stream='CLASSIFY_UNITS',
          bootstrap_cursor_json=v_next,
          bootstrap_rows_seen=bootstrap_rows_seen+v_bootstrap_page_count,
          updated_at_utc=clock_timestamp()
      WHERE candidate_id=p_candidate_id AND current_build_id=v_build_id;
      -- Do not replace the terminal seed-scope cursor during classification.
      UPDATE private.banking_pay_workbench_economic_builds
      SET updated_at_utc=clock_timestamp() WHERE id=v_build_id;
      RETURN jsonb_build_object('ok',true,'build_id',v_build_id,
        'private_stage','BOOTSTRAP_DISCOVERY','stage_status','CLASSIFYING',
        'has_more',true,'continuation_enqueued',false,'next_cursor_json',v_next,
        'next_action','BOOTSTRAP_DISCOVERY','classification_phase',
        v_next->>'classification_phase','page_source_count',v_bootstrap_page_count);
    ELSIF v_bootstrap_stream='RESET_FACTS' THEN
      WITH doomed AS (
        SELECT fact.ctid
        FROM private.banking_pay_workbench_economic_build_facts fact
        WHERE fact.build_id=v_build_id AND fact.fact_family='DEPENDENCY_EDGE'
        ORDER BY fact.natural_key LIMIT 250
      ) DELETE FROM private.banking_pay_workbench_economic_build_facts fact
        USING doomed WHERE fact.ctid=doomed.ctid;
      GET DIAGNOSTICS v_bootstrap_reset_count=ROW_COUNT;
      v_next:=jsonb_build_object('cursor_kind','BOOTSTRAP_DISCOVERY','cursor_version',1,
        'bootstrap_id',v_bootstrap_id,'bootstrap_stream',
        CASE WHEN v_bootstrap_reset_count<250 THEN 'RESET_SCOPE' ELSE 'RESET_FACTS' END,
        'build_id',v_build_id,'candidate_id',p_candidate_id,
        'captured_candidate_generation',v_build.captured_candidate_generation,
        'captured_source_change_seq',v_build.source_change_seq);
      UPDATE private.banking_pay_workbench_candidate_scope_registry
      SET bootstrap_stream=v_next->>'bootstrap_stream',bootstrap_cursor_json=v_next,
          updated_at_utc=clock_timestamp()
      WHERE candidate_id=p_candidate_id AND current_build_id=v_build_id;
      -- Reset progress belongs to bootstrap_cursor_json until the atomic
      -- seed-seal reset below clears both the digest and sealed timestamp.
      UPDATE private.banking_pay_workbench_economic_builds
      SET updated_at_utc=clock_timestamp() WHERE id=v_build_id;
      RETURN jsonb_build_object('ok',true,'build_id',v_build_id,
        'private_stage','BOOTSTRAP_DISCOVERY','stage_status','CLASSIFYING',
        'has_more',true,'continuation_enqueued',false,'next_cursor_json',v_next,
        'next_action','BOOTSTRAP_DISCOVERY','deleted_count',v_bootstrap_reset_count);
    ELSIF v_bootstrap_stream='RESET_SCOPE' THEN
      WITH doomed AS (
        SELECT scope_row.ctid
        FROM private.banking_pay_workbench_economic_build_scope scope_row
        WHERE scope_row.build_id=v_build_id ORDER BY scope_row.stable_ordinal LIMIT 250
      ) DELETE FROM private.banking_pay_workbench_economic_build_scope scope_row
        USING doomed WHERE scope_row.ctid=doomed.ctid;
      GET DIAGNOSTICS v_bootstrap_reset_count=ROW_COUNT;
      IF v_bootstrap_reset_count>=250 THEN
        v_next:=v_cursor;
        UPDATE private.banking_pay_workbench_candidate_scope_registry
        SET bootstrap_cursor_json=v_next,updated_at_utc=clock_timestamp()
        WHERE candidate_id=p_candidate_id AND current_build_id=v_build_id;
        RETURN jsonb_build_object('ok',true,'build_id',v_build_id,
          'private_stage','BOOTSTRAP_DISCOVERY','stage_status','CLASSIFYING',
          'has_more',true,'continuation_enqueued',false,'next_cursor_json',v_next,
          'next_action','BOOTSTRAP_DISCOVERY','deleted_count',v_bootstrap_reset_count);
      END IF;
      SELECT EXISTS(
        SELECT 1 FROM private.banking_pay_workbench_timesheet_scope_state
        WHERE candidate_id=p_candidate_id AND economic_state='DIRTY' LIMIT 1
      ) INTO v_bootstrap_has_dirty;
      IF NOT v_bootstrap_has_dirty
         AND v_registry.dirty_generation=v_build.captured_candidate_generation
         AND v_registry.current_source_change_seq=v_build.source_change_seq THEN
        UPDATE private.banking_pay_workbench_candidate_scope_registry SET
          initialisation_status='READY',evaluated_generation=dirty_generation,current_build_id=NULL,
          last_evaluated_at_utc=clock_timestamp(),initialised_at_utc=clock_timestamp(),
          bootstrap_stream='COMPLETE',bootstrap_cursor_json=jsonb_build_object(
            'cursor_kind','BOOTSTRAP_DISCOVERY','cursor_version',1,'terminal',true,
            'bootstrap_id',v_bootstrap_id),failure_json='{}'::jsonb,updated_at_utc=clock_timestamp()
        WHERE candidate_id=p_candidate_id;
        UPDATE private.banking_pay_workbench_economic_builds SET
          status='COMPLETE',private_stage='COMPLETE',scope_count=0,dependency_node_count=0,
          dependency_edge_count=0,tagged_edge_count=0,unit_count=0,row_seal_count=0,
          last_stable_ordinal=0,scope_digest=md5(''),dependency_digest=md5(''),
          pre_sync_digest=md5(''),post_sync_digest=md5(''),canonical_digest=md5(''),
          attestation_json=jsonb_build_object('bootstrap_id',v_bootstrap_id,
            'no_active_scope',true,'classification_dependency_closed',true),
          reconciled_at_utc=clock_timestamp(),completed_at_utc=clock_timestamp(),
          cleanup_not_before_utc=clock_timestamp()+interval '7 days',
          updated_at_utc=clock_timestamp() WHERE id=v_build_id;
        RETURN jsonb_build_object('ok',true,'build_id',v_build_id,'private_stage','COMPLETE',
          'stage_status','COMPLETE','has_more',false,'continuation_enqueued',false,
          'next_action','COMPLETE','classification_dependency_closed',true);
      END IF;
      v_next:=jsonb_build_object('cursor_kind','SCOPE_SELECT','cursor_version',1,
        'seed_family','ACTIVE_STATE','last_source_key',NULL,'build_id',v_build_id,
        'candidate_id',p_candidate_id,'captured_candidate_generation',v_registry.dirty_generation,
        'captured_source_change_seq',v_registry.current_source_change_seq,'processed_source_rows',0);
      UPDATE private.banking_pay_workbench_economic_builds SET
        captured_candidate_generation=v_registry.dirty_generation,
        source_change_seq=v_registry.current_source_change_seq,status='COLLECTING',
        private_stage='PREPARE_SCOPE',scope_cursor_json=v_next,closure_cursor_json='{}'::jsonb,
        fact_cursor_json='{}'::jsonb,seed_scope_count=0,scope_count=0,
        dependency_node_count=0,dependency_edge_count=0,tagged_edge_count=0,
        unit_count=0,row_seal_count=0,last_stable_ordinal=0,fact_count=0,
        seed_scope_digest=NULL,dependency_edge_stream_digest=NULL,
        dependency_edge_stream_complete=false,dependency_edge_stream_terminal_key_json='{}'::jsonb,
        edge_tag_digest=NULL,edge_tag_stream_complete=false,
        edge_tag_stream_terminal_key_json='{}'::jsonb,unit_digest=NULL,
        sealed_fingerprint_digest=NULL,scope_digest=NULL,dependency_digest=NULL,
        seed_scope_sealed_at_utc=NULL,dependency_closure_sealed_at_utc=NULL,
        updated_at_utc=clock_timestamp() WHERE id=v_build_id;
      UPDATE private.banking_pay_workbench_candidate_scope_registry SET
        initialisation_status='CLASSIFYING',bootstrap_stream='ACTIVE_RECONCILIATION',
        bootstrap_cursor_json=jsonb_build_object('cursor_kind','BOOTSTRAP_DISCOVERY',
          'cursor_version',1,'terminal',true,'bootstrap_id',v_bootstrap_id),
        updated_at_utc=clock_timestamp()
      WHERE candidate_id=p_candidate_id AND current_build_id=v_build_id;
      RETURN jsonb_build_object('ok',true,'build_id',v_build_id,
        'private_stage','PREPARE_SCOPE','stage_status','PREPARING','has_more',true,
        'continuation_enqueued',false,'next_cursor_json',v_next,'next_action','PREPARE_SCOPE');
    ELSE
      RAISE EXCEPTION 'PAY_WORKBENCH_BOOTSTRAP_CURSOR_INVALID' USING ERRCODE='22023';
    END IF;

    SELECT count(*)>250 INTO v_bootstrap_has_more FROM pg_temp._bpay_wb_bootstrap_page_v1;
    SELECT count(*)::integer,max(source_key) INTO v_bootstrap_page_count,v_bootstrap_last_key
    FROM (SELECT source_key FROM pg_temp._bpay_wb_bootstrap_page_v1 ORDER BY source_key LIMIT 250) page;

    WITH discovered AS (
      SELECT DISTINCT page.timesheet_id
      FROM (SELECT * FROM pg_temp._bpay_wb_bootstrap_page_v1
        ORDER BY source_key LIMIT 250) page
      JOIN public.timesheets timesheet_row ON timesheet_row.timesheet_id=page.timesheet_id
      WHERE page.timesheet_id IS NOT NULL
    ), inserted AS (
      INSERT INTO private.banking_pay_workbench_timesheet_scope_state(
        timesheet_id,candidate_id,economic_state,dirty_generation,last_dirty_reason,
        current_build_id,registered_at_utc,last_dirtied_at_utc,updated_at_utc
      ) SELECT timesheet_id,p_candidate_id,'DIRTY',v_build.captured_candidate_generation,
        'LEGACY_BOOTSTRAP_DISCOVERY',v_build_id,clock_timestamp(),clock_timestamp(),clock_timestamp()
      FROM discovered
      ON CONFLICT(timesheet_id) DO UPDATE SET
        candidate_id=EXCLUDED.candidate_id,economic_state='DIRTY',
        dirty_generation=GREATEST(private.banking_pay_workbench_timesheet_scope_state.dirty_generation,
          EXCLUDED.dirty_generation),
        last_dirty_reason='LEGACY_BOOTSTRAP_DISCOVERY',current_build_id=v_build_id,
        last_dirtied_at_utc=clock_timestamp(),updated_at_utc=clock_timestamp()
      RETURNING (xmax=0) newly_inserted
    ) SELECT count(*) FILTER(WHERE newly_inserted)::integer
      INTO v_bootstrap_registered FROM inserted;

    IF v_bootstrap_has_more THEN
      v_next:=jsonb_build_object('cursor_kind','BOOTSTRAP_DISCOVERY','cursor_version',1,
        'bootstrap_id',v_bootstrap_id,'bootstrap_stream',v_bootstrap_stream,
        'last_source_key',v_bootstrap_last_key,'build_id',v_build_id,
        'candidate_id',p_candidate_id,'captured_candidate_generation',v_build.captured_candidate_generation,
        'captured_source_change_seq',v_build.source_change_seq);
    ELSIF v_bootstrap_next_stream='SCOPE_CLOSURE' THEN
      v_next:=jsonb_build_object('cursor_kind','SCOPE_SELECT','cursor_version',1,
        'seed_family','ACTIVE_STATE','last_source_key',NULL,'build_id',v_build_id,
        'candidate_id',p_candidate_id,
        'captured_candidate_generation',v_build.captured_candidate_generation,
        'captured_source_change_seq',v_build.source_change_seq,'processed_source_rows',0);
      UPDATE private.banking_pay_workbench_economic_builds SET
        private_stage='PREPARE_SCOPE',scope_cursor_json=v_next,updated_at_utc=clock_timestamp()
      WHERE id=v_build_id AND private_stage='BOOTSTRAP_DISCOVERY';
      UPDATE private.banking_pay_workbench_candidate_scope_registry SET
        initialisation_status='CLASSIFYING',bootstrap_stream='DEPENDENCY_CLOSURE',
        bootstrap_cursor_json=jsonb_build_object('cursor_kind','BOOTSTRAP_DISCOVERY',
          'cursor_version',1,'terminal',true,'bootstrap_id',v_bootstrap_id,
          'next_stage','PREPARE_SCOPE'),
        bootstrap_rows_seen=bootstrap_rows_seen+v_bootstrap_page_count,
        bootstrap_timesheets_registered=bootstrap_timesheets_registered+v_bootstrap_registered,
        updated_at_utc=clock_timestamp()
      WHERE candidate_id=p_candidate_id AND current_build_id=v_build_id;
      RETURN jsonb_build_object('ok',true,'build_id',v_build_id,'private_stage','PREPARE_SCOPE',
        'stage_status','CLASSIFYING','has_more',true,'continuation_enqueued',false,
        'next_cursor_json',v_next,'next_action','PREPARE_SCOPE',
        'page_source_count',v_bootstrap_page_count,
        'timesheets_registered',v_bootstrap_registered);
    ELSE
      v_next:=jsonb_build_object('cursor_kind','BOOTSTRAP_DISCOVERY','cursor_version',1,
        'bootstrap_id',v_bootstrap_id,'bootstrap_stream',v_bootstrap_next_stream,
        'last_source_key',NULL,'build_id',v_build_id,'candidate_id',p_candidate_id,
        'captured_candidate_generation',v_build.captured_candidate_generation,
        'captured_source_change_seq',v_build.source_change_seq);
    END IF;

    UPDATE private.banking_pay_workbench_candidate_scope_registry SET
      initialisation_status='DISCOVERING',
      bootstrap_stream=COALESCE(v_next->>'bootstrap_stream',bootstrap_stream),
      bootstrap_cursor_json=v_next,
      bootstrap_rows_seen=bootstrap_rows_seen+v_bootstrap_page_count,
      bootstrap_timesheets_registered=bootstrap_timesheets_registered+v_bootstrap_registered,
      updated_at_utc=clock_timestamp()
    WHERE candidate_id=p_candidate_id;
    UPDATE private.banking_pay_workbench_economic_builds SET scope_cursor_json=v_next,
      updated_at_utc=clock_timestamp() WHERE id=v_build_id;
    RETURN jsonb_build_object('ok',true,'build_id',v_build_id,'private_stage','BOOTSTRAP_DISCOVERY',
      'stage_status','DISCOVERING',
      'has_more',true,'continuation_enqueued',false,'next_cursor_json',v_next,
      'next_action','BOOTSTRAP_DISCOVERY','page_source_count',v_bootstrap_page_count,
      'timesheets_registered',v_bootstrap_registered);
  END IF;
  RAISE EXCEPTION 'PAY_WORKBENCH_BUILD_CURSOR_INVALID' USING ERRCODE='22023';
END;
$function$;

ALTER FUNCTION public.pay_workbench_candidate_source_build_chunk(uuid,uuid,jsonb,jsonb,integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_candidate_source_build_chunk(uuid,uuid,jsonb,jsonb,integer) FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_candidate_source_build_chunk(uuid,uuid,jsonb,jsonb,integer) TO postgres;
