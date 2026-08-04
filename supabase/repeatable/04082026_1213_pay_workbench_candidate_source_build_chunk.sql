-- Banking Pay bounded-scope V1.2.4: one exact private source-build stage per
-- durable RPC-1 attempt. No ordinary candidate-lifetime seed or 100-row scope cap.

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
  v_cumulative_digest text:=COALESCE(NULLIF(v_cursor->>'cumulative_digest',''),md5('BPAY_FACT_STREAM_V1'));
  v_has_more boolean:=false;
  v_is_final boolean:=false;
  v_last_page_key text;
  v_cursor_start_hash text;
  v_cursor_end_hash text;
  v_existing_page private.banking_pay_workbench_economic_build_fact_pages%ROWTYPE;
  v_unit_ids uuid[]:=ARRAY[]::uuid[];
  v_family_ordinal integer;
  v_unit_families text[]:=ARRAY[
    'FROZEN_SETTLED_COMPONENT','LIVE_ENTITLEMENT_INPUT','ENTITLEMENT_COMPONENT',
    'PAY_STATE_FALLBACK','PAYEE_BASELINE_INPUT','CANONICAL_INPUT'
  ];
  v_global_families text[]:=ARRAY[
    'RESERVATION_COMPONENT','FINANCE_CASE_IDENTITY','FINANCE_COMPONENT_IDENTITY',
    'PROTECTION_EVIDENCE','ALLOCATION_INPUT'
  ];
  v_scope_inserted integer:=0;
  v_vector jsonb;
  v_envelope_version integer;
  v_envelope jsonb;
  v_envelope_evidence jsonb;
  v_vector_item record;
  v_scale_blocked boolean:=false;
  v_sync_result jsonb;
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
    IF v_build.dependency_closure_sealed_at_utc IS NULL OR EXISTS(
      SELECT 1 FROM private.banking_pay_workbench_economic_build_scope
      WHERE build_id=v_build_id AND closure_status<>'SEALED' LIMIT 1
    ) THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_RECONCILIATION_FACTS_INCOMPLETE' USING ERRCODE='23514';
    END IF;

    IF v_unit_key IS NULL THEN
      SELECT dependency_unit_key INTO v_unit_key
      FROM private.banking_pay_workbench_economic_build_scope
      WHERE build_id=v_build_id
        AND NOT required_fact_families <@ completed_fact_families
      ORDER BY stable_ordinal LIMIT 1;
      IF v_unit_key IS NULL THEN v_unit_key:='GLOBAL'; END IF;
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

    IF v_fact_family='FROZEN_SETTLED_COMPONENT' THEN
      INSERT INTO pg_temp._bpay_wb_fact_page_v1
      SELECT key_row.source_key,md5(key_row.source_key),key_row.timesheet_id,ARRAY[key_row.timesheet_id],
        '_pay_active_settled_components',key_row.timesheet_id,key_row.key_type,key_row.key_value,
        key_row.amount_ex_vat,key_row.amount_inc_vat,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
        jsonb_build_object('key_type',key_row.key_type,'key_value',key_row.key_value),
        md5(jsonb_build_object('timesheet_id',key_row.timesheet_id,'key_type',key_row.key_type,
          'key_value',key_row.key_value,'amount_ex_vat',key_row.amount_ex_vat,'amount_inc_vat',key_row.amount_inc_vat)::text)
      FROM (SELECT settled.*,settled.timesheet_id::text||':'||settled.key_type||':'||settled.key_value source_key
        FROM public._pay_active_settled_components(v_unit_ids) settled) key_row
      WHERE (v_last_source_key IS NULL OR key_row.source_key>v_last_source_key)
      ORDER BY key_row.source_key LIMIT v_fact_limit+1;
    ELSIF v_fact_family='LIVE_ENTITLEMENT_INPUT' THEN
      INSERT INTO pg_temp._bpay_wb_fact_page_v1
      SELECT ts.timesheet_id::text,md5('LIVE:'||ts.timesheet_id::text),ts.timesheet_id,ARRAY[ts.timesheet_id],
        'timesheets_financials',tf.id,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
        jsonb_build_object('timesheet',to_jsonb(ts),'current_financial',to_jsonb(tf)),
        md5(jsonb_build_object('timesheet',to_jsonb(ts),'current_financial',to_jsonb(tf))::text)
      FROM public.timesheets ts LEFT JOIN public.timesheets_financials tf
        ON tf.timesheet_id=ts.timesheet_id AND tf.is_current AND tf.candidate_id=p_candidate_id
      WHERE ts.timesheet_id=ANY(v_unit_ids) AND (v_last_source_key IS NULL OR ts.timesheet_id::text>v_last_source_key)
      ORDER BY ts.timesheet_id LIMIT v_fact_limit+1;
    ELSIF v_fact_family IN ('ENTITLEMENT_COMPONENT','PAYEE_BASELINE_INPUT','CANONICAL_INPUT') THEN
      INSERT INTO pg_temp._bpay_wb_fact_page_v1
      SELECT key_row.source_key,md5(v_fact_family||':'||key_row.source_key),key_row.timesheet_id,ARRAY[key_row.timesheet_id],
        'pay_current_timesheet_entitlement_components_from_build_v1',key_row.timesheet_id,
        key_row.key_type,key_row.key_value,
        CASE WHEN v_fact_family='CANONICAL_INPUT' THEN key_row.truth_ex_vat ELSE NULL END,NULL,
        key_row.truth_ex_vat,key_row.truth_inc_vat,key_row.baseline_ex_vat,key_row.baseline_inc_vat,
        NULL,NULL,NULL,NULL,jsonb_build_object('fact_role',v_fact_family),
        md5(jsonb_build_object('timesheet_id',key_row.timesheet_id,'key_type',key_row.key_type,
          'key_value',key_row.key_value,'truth_ex_vat',key_row.truth_ex_vat,
          'baseline_ex_vat',key_row.baseline_ex_vat,'truth_inc_vat',key_row.truth_inc_vat,
          'baseline_inc_vat',key_row.baseline_inc_vat)::text)
      FROM (SELECT entitlement.*,entitlement.timesheet_id::text||':'||entitlement.key_type||':'||entitlement.key_value source_key
        FROM private.pay_current_timesheet_entitlement_components_from_build_v1(v_build_id,v_unit_key) entitlement) key_row
      WHERE (v_last_source_key IS NULL OR key_row.source_key>v_last_source_key)
      ORDER BY key_row.source_key LIMIT v_fact_limit+1;
    ELSIF v_fact_family='PAY_STATE_FALLBACK' THEN
      INSERT INTO pg_temp._bpay_wb_fact_page_v1
      SELECT state.timesheet_id::text,md5('PAY_STATE:'||state.timesheet_id::text),state.timesheet_id,
        ARRAY[state.timesheet_id],'timesheet_pay_state',state.timesheet_id,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
        NULL,NULL,NULL,NULL,to_jsonb(state),md5(to_jsonb(state)::text)
      FROM public.timesheet_pay_state state
      WHERE state.timesheet_id=ANY(v_unit_ids)
        AND (v_last_source_key IS NULL OR state.timesheet_id::text>v_last_source_key)
      ORDER BY state.timesheet_id LIMIT v_fact_limit+1;
    ELSIF v_fact_family='RESERVATION_COMPONENT' THEN
      INSERT INTO pg_temp._bpay_wb_fact_page_v1
      SELECT reservation.id::text,md5('RESERVATION:'||reservation.id::text),
        COALESCE(item.timesheet_id,component.linked_timesheet_id),
        ARRAY[COALESCE(item.timesheet_id,component.linked_timesheet_id)],'pay_advance_reservations',reservation.id,
        COALESCE(reservation.frozen_component_key_type,component.component_key_type),
        COALESCE(reservation.frozen_component_key_value,component.component_key_value),
        NULL,NULL,NULL,NULL,NULL,NULL,COALESCE(reservation.reserved_source_amount,reservation.reserved_amount),
        reservation.finance_case_id,reservation.finance_component_id,reservation.id,to_jsonb(reservation),
        md5(to_jsonb(reservation)::text)
      FROM public.pay_advance_reservations reservation
      LEFT JOIN public.pay_batch_items item ON item.id=reservation.pay_batch_item_id
      LEFT JOIN public.pay_finance_case_components component ON component.id=reservation.finance_component_id
      LEFT JOIN public.pay_batch_candidates batch_candidate ON batch_candidate.id=reservation.pay_batch_candidate_id
      WHERE COALESCE(batch_candidate.candidate_id,component.candidate_id)=p_candidate_id
        AND COALESCE(item.timesheet_id,component.linked_timesheet_id) IN (
          SELECT timesheet_id FROM private.banking_pay_workbench_economic_build_scope WHERE build_id=v_build_id)
        AND COALESCE(reservation.frozen_component_key_type,component.component_key_type) IN
          ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE','MANUAL_CARRY_FORWARD')
        AND NULLIF(btrim(COALESCE(reservation.frozen_component_key_value,component.component_key_value)), '') IS NOT NULL
        AND (v_last_source_key IS NULL OR reservation.id::text>v_last_source_key)
      ORDER BY reservation.id LIMIT v_fact_limit+1;
    ELSIF v_fact_family='FINANCE_CASE_IDENTITY' THEN
      INSERT INTO pg_temp._bpay_wb_fact_page_v1
      SELECT finance_case.id::text,md5('CASE:'||finance_case.id::text),finance_case.linked_timesheet_id,
        CASE WHEN finance_case.linked_timesheet_id IS NULL THEN ARRAY[]::uuid[] ELSE ARRAY[finance_case.linked_timesheet_id] END,
        'pay_advances',finance_case.id,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,finance_case.id,NULL,NULL,
        to_jsonb(finance_case),md5(to_jsonb(finance_case)::text)
      FROM public.pay_advances finance_case WHERE finance_case.candidate_id=p_candidate_id
        AND finance_case.cleared_at_utc IS NULL AND finance_case.written_off_at_utc IS NULL
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
      SELECT event.id::text,md5('EVENT:'||event.id::text),component.linked_timesheet_id,
        CASE WHEN component.linked_timesheet_id IS NULL THEN ARRAY[]::uuid[] ELSE ARRAY[component.linked_timesheet_id] END,
        'pay_finance_case_events',event.id,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
        event.finance_case_id,event.finance_component_id,event.reservation_id,to_jsonb(event),md5(to_jsonb(event)::text)
      FROM public.pay_finance_case_events event
      JOIN public.pay_advances finance_case ON finance_case.id=event.finance_case_id
      LEFT JOIN public.pay_finance_case_components component ON component.id=event.finance_component_id
      WHERE finance_case.candidate_id=p_candidate_id AND (v_last_source_key IS NULL OR event.id::text>v_last_source_key)
      ORDER BY event.id LIMIT v_fact_limit+1;
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

    SELECT count(*)>v_fact_limit INTO v_has_more FROM pg_temp._bpay_wb_fact_page_v1;
    SELECT count(*)::integer,
      md5(COALESCE(string_agg(source_key||':'||financial_digest,'' ORDER BY source_key),'')),
      max(source_key)
    INTO v_page_count,v_page_digest,v_last_page_key
    FROM (SELECT * FROM pg_temp._bpay_wb_fact_page_v1 ORDER BY source_key LIMIT v_fact_limit) page_rows;

    IF EXISTS(
      SELECT 1 FROM pg_temp._bpay_wb_fact_page_v1 page_row
      JOIN private.banking_pay_workbench_economic_build_facts existing
        ON existing.build_id=v_build_id AND existing.fact_family=v_fact_family
       AND existing.natural_key=page_row.natural_key
      WHERE existing.financial_digest<>page_row.financial_digest LIMIT 1
    ) THEN RAISE EXCEPTION 'PAY_WORKBENCH_FACT_PAGE_REPLAY_CONFLICT' USING ERRCODE='23514'; END IF;

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
    FROM (SELECT * FROM pg_temp._bpay_wb_fact_page_v1 ORDER BY source_key LIMIT v_fact_limit) page
    ON CONFLICT(build_id,fact_family,natural_key) DO NOTHING;
    GET DIAGNOSTICS v_scope_inserted=ROW_COUNT;

    v_cursor_start_hash:=md5(v_cursor::text);
    v_cumulative_digest:=md5(v_cumulative_digest||v_page_digest);
    v_is_final:=NOT v_has_more;
    v_next:=jsonb_build_object('cursor_kind','WORKSPACE_FACT','cursor_version',1,
      'build_id',v_build_id,'candidate_id',p_candidate_id,
      'captured_candidate_generation',v_build.captured_candidate_generation,
      'captured_source_change_seq',v_build.source_change_seq,
      'dependency_unit_key',v_unit_key,'fact_family',v_fact_family,
      'page_number',v_page_number+1,'last_source_key',CASE WHEN v_has_more THEN v_last_page_key ELSE NULL END,
      'previous_page_digest',v_page_digest,'cumulative_digest',v_cumulative_digest,
      'terminal',v_is_final);
    v_cursor_end_hash:=md5(v_next::text);

    SELECT * INTO v_existing_page FROM private.banking_pay_workbench_economic_build_fact_pages
    WHERE build_id=v_build_id AND dependency_unit_key=v_unit_key
      AND fact_family=v_fact_family AND page_number=v_page_number;
    IF FOUND THEN
      IF v_existing_page.cursor_start_hash<>v_cursor_start_hash OR v_existing_page.cursor_end_hash<>v_cursor_end_hash
         OR v_existing_page.page_digest<>v_page_digest OR v_existing_page.actual_fact_count<>v_page_count THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_FACT_PAGE_REPLAY_CONFLICT' USING ERRCODE='23514';
      END IF;
    ELSE
      INSERT INTO private.banking_pay_workbench_economic_build_fact_pages(
        build_id,attempt_id,dependency_unit_key,fact_family,page_number,
        cursor_start_json,cursor_start_hash,cursor_end_json,cursor_end_hash,
        actual_fact_count,cumulative_fact_count,page_digest,cumulative_digest,is_family_final
      ) VALUES(v_build_id,v_attempt_id,v_unit_key,v_fact_family,v_page_number,
        v_cursor,v_cursor_start_hash,v_next,v_cursor_end_hash,v_page_count,
        COALESCE((v_cursor->>'cumulative_fact_count')::integer,0)+v_page_count,
        v_page_digest,v_cumulative_digest,v_is_final);
    END IF;

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

    IF v_is_final THEN
      IF v_unit_key<>'GLOBAL' THEN
        UPDATE private.banking_pay_workbench_economic_build_scope
        SET completed_fact_families=(SELECT array_agg(DISTINCT family ORDER BY family)
          FROM unnest(completed_fact_families||ARRAY[v_fact_family]) family),updated_at_utc=clock_timestamp()
        WHERE build_id=v_build_id AND dependency_unit_key=v_unit_key;
      END IF;
      IF v_unit_key<>'GLOBAL' AND v_family_ordinal<cardinality(v_unit_families) THEN
        v_next:=v_next||jsonb_build_object('fact_family',v_unit_families[v_family_ordinal+1],
          'page_number',1,'last_source_key',NULL,'terminal',false,'cumulative_digest',md5('BPAY_FACT_STREAM_V1'));
      ELSIF v_unit_key<>'GLOBAL' THEN
        SELECT dependency_unit_key INTO v_unit_key
        FROM private.banking_pay_workbench_economic_build_scope
        WHERE build_id=v_build_id AND NOT required_fact_families <@ completed_fact_families
        ORDER BY stable_ordinal LIMIT 1;
        IF v_unit_key IS NULL THEN v_unit_key:='GLOBAL';v_fact_family:=v_global_families[1];
        ELSE v_fact_family:=v_unit_families[1]; END IF;
        v_next:=v_next||jsonb_build_object('dependency_unit_key',v_unit_key,'fact_family',v_fact_family,
          'page_number',1,'last_source_key',NULL,'terminal',false,'cumulative_digest',md5('BPAY_FACT_STREAM_V1'));
      ELSIF v_family_ordinal<cardinality(v_global_families) THEN
        v_next:=v_next||jsonb_build_object('fact_family',v_global_families[v_family_ordinal+1],
          'page_number',1,'last_source_key',NULL,'terminal',false,'cumulative_digest',md5('BPAY_FACT_STREAM_V1'));
      ELSE
        SELECT jsonb_build_object(
          'relevant_timesheet_count',v_build.scope_count,
          'dependency_node_count',v_build.dependency_node_count,
          'dependency_edge_count',v_build.dependency_edge_count,
          'settled_source_row_count',count(*) FILTER(WHERE fact_family='FROZEN_SETTLED_COMPONENT'),
          'settled_component_count',count(*) FILTER(WHERE fact_family='FROZEN_SETTLED_COMPONENT'),
          'entitlement_component_count',count(*) FILTER(WHERE fact_family='ENTITLEMENT_COMPONENT'),
          'reservation_component_count',count(*) FILTER(WHERE fact_family='RESERVATION_COMPONENT'),
          'finance_case_count',count(*) FILTER(WHERE fact_family='FINANCE_CASE_IDENTITY'),
          'finance_component_count',count(*) FILTER(WHERE fact_family='FINANCE_COMPONENT_IDENTITY'),
          'protection_evidence_count',count(*) FILTER(WHERE fact_family='PROTECTION_EVIDENCE'),
          'expected_case_insert_count',count(*) FILTER(WHERE fact_family='ENTITLEMENT_COMPONENT' AND truth_ex_vat<baseline_ex_vat),
          'expected_case_update_count',count(*) FILTER(WHERE fact_family='FINANCE_CASE_IDENTITY'),
          'expected_case_clear_count',0,
          'expected_component_insert_count',count(*) FILTER(WHERE fact_family='ENTITLEMENT_COMPONENT' AND truth_ex_vat<baseline_ex_vat),
          'expected_component_update_count',count(*) FILTER(WHERE fact_family='FINANCE_COMPONENT_IDENTITY'),
          'expected_component_close_count',0,
          'canonical_source_row_count',count(*) FILTER(WHERE fact_family='CANONICAL_INPUT'),
          'staging_bytes',COALESCE(sum(pg_column_size(source_payload_json)) FILTER(WHERE fact_family='CANONICAL_INPUT'),0)
        ) INTO v_vector FROM private.banking_pay_workbench_economic_build_facts WHERE build_id=v_build_id;
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
            'cursor_kind','WORKSPACE_FACT','cursor_version',1,'terminal',true,'build_id',v_build_id),
          complexity_vector_json=v_vector,envelope_version=v_envelope_version,
          envelope_snapshot_json=v_envelope,envelope_evidence_json=v_envelope_evidence,
          ready_at_utc=clock_timestamp(),updated_at_utc=clock_timestamp() WHERE id=v_build_id;
      END IF;
    END IF;
    IF (SELECT private_stage FROM private.banking_pay_workbench_economic_builds WHERE id=v_build_id)='WORKSPACE_FACT' THEN
      UPDATE private.banking_pay_workbench_economic_builds SET fact_cursor_json=v_next,
        fact_count=fact_count+v_scope_inserted,updated_at_utc=clock_timestamp() WHERE id=v_build_id;
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
        SELECT 1 FROM private.banking_pay_workbench_economic_build_facts fact
        WHERE fact.build_id=v_build_id AND state_row.timesheet_id=ANY(fact.subject_timesheet_ids)
          AND fact.fact_family IN ('ENTITLEMENT_COMPONENT','RESERVATION_COMPONENT','FINANCE_CASE_IDENTITY','FINANCE_COMPONENT_IDENTITY')
          AND (COALESCE(fact.truth_ex_vat,0)<>COALESCE(fact.baseline_ex_vat,0)
            OR COALESCE(fact.reserved_source_amount,0)<>0 OR fact.finance_case_id IS NOT NULL)
      ) THEN 'LIVE' ELSE 'CLOSED' END,
      evaluated_generation=v_build.captured_candidate_generation,
      current_input_fingerprint=scope_row.captured_input_fingerprint,
      evaluated_input_fingerprint=scope_row.captured_input_fingerprint,
      current_build_id=NULL,last_evaluated_at_utc=clock_timestamp(),
      closed_at_utc=CASE WHEN EXISTS(
        SELECT 1 FROM private.banking_pay_workbench_economic_build_facts fact
        WHERE fact.build_id=v_build_id AND state_row.timesheet_id=ANY(fact.subject_timesheet_ids)
          AND fact.fact_family IN ('ENTITLEMENT_COMPONENT','RESERVATION_COMPONENT','FINANCE_CASE_IDENTITY','FINANCE_COMPONENT_IDENTITY')
          AND (COALESCE(fact.truth_ex_vat,0)<>COALESCE(fact.baseline_ex_vat,0)
            OR COALESCE(fact.reserved_source_amount,0)<>0 OR fact.finance_case_id IS NOT NULL)
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
    IF v_cleanup_kind='CANONICAL_STAGE' THEN
      WITH doomed AS (SELECT ctid FROM private.banking_pay_workbench_canonical_stage_lines
        WHERE build_id=v_build_id ORDER BY source_ordinal LIMIT LEAST(v_limit,500))
      DELETE FROM private.banking_pay_workbench_canonical_stage_lines row USING doomed WHERE row.ctid=doomed.ctid;
      GET DIAGNOSTICS v_cleanup_count=ROW_COUNT;
      IF v_cleanup_count<v_limit THEN v_cleanup_kind:='FACT_PAGES'; END IF;
    ELSIF v_cleanup_kind='FACT_PAGES' THEN
      WITH doomed AS (SELECT ctid FROM private.banking_pay_workbench_economic_build_fact_pages
        WHERE build_id=v_build_id ORDER BY id LIMIT LEAST(v_limit,500))
      DELETE FROM private.banking_pay_workbench_economic_build_fact_pages row USING doomed WHERE row.ctid=doomed.ctid;
      GET DIAGNOSTICS v_cleanup_count=ROW_COUNT;
      IF v_cleanup_count<v_limit THEN v_cleanup_kind:='FACTS'; END IF;
    ELSIF v_cleanup_kind='FACTS' THEN
      WITH doomed AS (SELECT ctid FROM private.banking_pay_workbench_economic_build_facts
        WHERE build_id=v_build_id ORDER BY fact_family,natural_key LIMIT LEAST(v_limit,500))
      DELETE FROM private.banking_pay_workbench_economic_build_facts row USING doomed WHERE row.ctid=doomed.ctid;
      GET DIAGNOSTICS v_cleanup_count=ROW_COUNT;
      IF v_cleanup_count<v_limit THEN v_cleanup_kind:='SCOPE'; END IF;
    ELSIF v_cleanup_kind='SCOPE' THEN
      WITH doomed AS (SELECT ctid FROM private.banking_pay_workbench_economic_build_scope
        WHERE build_id=v_build_id ORDER BY timesheet_id LIMIT LEAST(v_limit,500))
      DELETE FROM private.banking_pay_workbench_economic_build_scope row USING doomed WHERE row.ctid=doomed.ctid;
      GET DIAGNOSTICS v_cleanup_count=ROW_COUNT;
      IF v_cleanup_count<v_limit THEN v_cleanup_kind:='ATTEMPTS'; END IF;
    ELSE
      WITH doomed AS (SELECT ctid FROM private.banking_pay_workbench_stage_attempts
        WHERE build_id=v_build_id AND id<>v_attempt_id ORDER BY id LIMIT LEAST(v_limit,500))
      DELETE FROM private.banking_pay_workbench_stage_attempts row USING doomed WHERE row.ctid=doomed.ctid;
      GET DIAGNOSTICS v_cleanup_count=ROW_COUNT;
    END IF;
    v_next:=jsonb_build_object('cursor_kind','BUILD_CLEANUP','cursor_version',1,
      'build_id',v_build_id,'cleanup_kind',v_cleanup_kind,'deleted_count',v_cleanup_count);
    UPDATE private.banking_pay_workbench_economic_builds SET cleanup_cursor_json=v_next,
      updated_at_utc=clock_timestamp() WHERE id=v_build_id;
    RETURN jsonb_build_object('ok',true,'build_id',v_build_id,'private_stage','BUILD_CLEANUP',
      'stage_status','CLEANING','has_more',true,'continuation_enqueued',false,
      'next_cursor_json',v_next,'next_action','BUILD_CLEANUP','deleted_count',v_cleanup_count);
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
          UPDATE private.banking_pay_workbench_economic_builds
          SET scope_cursor_json=v_next,updated_at_utc=clock_timestamp() WHERE id=v_build_id;
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
        ORDER BY scope_row.stable_ordinal LIMIT 251;
        SELECT count(*)>250 INTO v_bootstrap_has_more FROM pg_temp._bpay_wb_bootstrap_page_v1;
        SELECT count(*)::integer,COALESCE(max(source_key)::bigint,v_bootstrap_last_ordinal)
        INTO v_bootstrap_page_count,v_bootstrap_last_ordinal
        FROM (SELECT source_key FROM pg_temp._bpay_wb_bootstrap_page_v1
          ORDER BY source_key LIMIT 250) page;
        SELECT EXISTS(
          SELECT 1
          FROM (SELECT timesheet_id FROM pg_temp._bpay_wb_bootstrap_page_v1
            ORDER BY source_key LIMIT 250) page
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
          LIMIT 1
        ) INTO v_bootstrap_page_relevant;
        v_bootstrap_unit_relevant:=v_bootstrap_unit_relevant OR v_bootstrap_page_relevant;
        IF v_bootstrap_has_more THEN
          v_next:=jsonb_build_object('cursor_kind','BOOTSTRAP_DISCOVERY','cursor_version',1,
            'bootstrap_id',v_bootstrap_id,'bootstrap_stream','CLASSIFY_UNITS',
            'classification_phase','EVIDENCE','last_dependency_unit_key',v_bootstrap_last_unit_key,
            'dependency_unit_key',v_bootstrap_unit_key,'last_stable_ordinal',v_bootstrap_last_ordinal,
            'unit_financially_relevant',v_bootstrap_unit_relevant,'build_id',v_build_id,
            'candidate_id',p_candidate_id,
            'captured_candidate_generation',v_build.captured_candidate_generation,
            'captured_source_change_seq',v_build.source_change_seq);
        ELSE
          v_next:=jsonb_build_object('cursor_kind','BOOTSTRAP_DISCOVERY','cursor_version',1,
            'bootstrap_id',v_bootstrap_id,'bootstrap_stream','CLASSIFY_UNITS',
            'classification_phase','APPLY','last_dependency_unit_key',v_bootstrap_last_unit_key,
            'dependency_unit_key',v_bootstrap_unit_key,'last_stable_ordinal',0,
            'unit_financially_relevant',v_bootstrap_unit_relevant,'build_id',v_build_id,
            'candidate_id',p_candidate_id,
            'captured_candidate_generation',v_build.captured_candidate_generation,
            'captured_source_change_seq',v_build.source_change_seq);
        END IF;
      ELSIF v_bootstrap_classification_phase='APPLY' THEN
        IF v_bootstrap_unit_key IS NULL THEN
          RAISE EXCEPTION 'PAY_WORKBENCH_BOOTSTRAP_CURSOR_INVALID' USING ERRCODE='22023';
        END IF;
        INSERT INTO pg_temp._bpay_wb_bootstrap_page_v1(source_key,timesheet_id)
        SELECT lpad(scope_row.stable_ordinal::text,20,'0'),scope_row.timesheet_id
        FROM private.banking_pay_workbench_economic_build_scope scope_row
        WHERE scope_row.build_id=v_build_id
          AND scope_row.dependency_unit_key=v_bootstrap_unit_key
          AND scope_row.stable_ordinal>v_bootstrap_last_ordinal
        ORDER BY scope_row.stable_ordinal LIMIT 251;
        SELECT count(*)>250 INTO v_bootstrap_has_more FROM pg_temp._bpay_wb_bootstrap_page_v1;
        SELECT count(*)::integer,COALESCE(max(source_key)::bigint,v_bootstrap_last_ordinal)
        INTO v_bootstrap_page_count,v_bootstrap_last_ordinal
        FROM (SELECT source_key FROM pg_temp._bpay_wb_bootstrap_page_v1
          ORDER BY source_key LIMIT 250) page;
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
          ORDER BY source_key LIMIT 250) page ON page.timesheet_id=scope_row.timesheet_id
        WHERE scope_row.build_id=v_build_id
          AND state_row.timesheet_id=scope_row.timesheet_id
          AND state_row.dirty_generation<=v_build.captured_candidate_generation;
        IF v_bootstrap_has_more THEN
          v_next:=jsonb_build_object('cursor_kind','BOOTSTRAP_DISCOVERY','cursor_version',1,
            'bootstrap_id',v_bootstrap_id,'bootstrap_stream','CLASSIFY_UNITS',
            'classification_phase','APPLY','last_dependency_unit_key',v_bootstrap_last_unit_key,
            'dependency_unit_key',v_bootstrap_unit_key,'last_stable_ordinal',v_bootstrap_last_ordinal,
            'unit_financially_relevant',v_bootstrap_unit_relevant,'build_id',v_build_id,
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
      UPDATE private.banking_pay_workbench_economic_builds
      SET scope_cursor_json=v_next,updated_at_utc=clock_timestamp() WHERE id=v_build_id;
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
      UPDATE private.banking_pay_workbench_economic_builds
      SET scope_cursor_json=v_next,updated_at_utc=clock_timestamp() WHERE id=v_build_id;
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
