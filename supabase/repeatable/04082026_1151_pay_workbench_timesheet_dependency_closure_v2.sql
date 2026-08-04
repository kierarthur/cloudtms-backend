-- Banking Pay bounded-scope V1.2.4: resumable, uncapped dependency closure
-- and bounded six-phase final sealing.

CREATE OR REPLACE FUNCTION private.pay_workbench_timesheet_dependency_closure_v2(
  p_build_id uuid,
  p_cursor_json jsonb DEFAULT '{}'::jsonb,
  p_limit integer DEFAULT 25
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
PARALLEL UNSAFE
SECURITY INVOKER
SET search_path = ''
AS $function$
DECLARE
  v_build private.banking_pay_workbench_economic_builds%ROWTYPE;
  v_registry private.banking_pay_workbench_candidate_scope_registry%ROWTYPE;
  v_cursor jsonb:=COALESCE(p_cursor_json,'{}'::jsonb);
  v_limit integer:=LEAST(GREATEST(COALESCE(p_limit,25),2),25);
  v_mode text:=COALESCE(NULLIF(v_cursor->>'cursor_kind',''),'DEPENDENCY_CLOSURE');
  v_frontier uuid;
  v_family integer:=COALESCE((v_cursor->>'dependency_family_ordinal')::integer,1);
  v_last_key text:=v_cursor->>'last_edge_key';
  v_processed integer:=0;
  v_used integer:=0;
  v_new_scope integer:=0;
  v_new_edges integer:=0;
  v_edge_count bigint:=COALESCE((v_cursor->>'processed_edge_count')::bigint,0);
  v_emission_count bigint:=COALESCE((v_cursor->>'processed_emission_count')::bigint,0);
  v_page integer:=COALESCE((v_cursor->>'page_number')::integer,1);
  v_phase text:=COALESCE(NULLIF(v_cursor->>'seal_phase',''),'ANCHOR_INITIALISE');
  v_pass integer:=COALESCE((v_cursor->>'pass_number')::integer,1);
  v_changed bigint:=COALESCE((v_cursor->>'changed_rows_in_pass')::bigint,0);
  v_tagged bigint:=COALESCE((v_cursor->>'tagged_edge_count')::bigint,0);
  v_row_seal_count bigint:=COALESCE((v_cursor->>'row_seal_count')::bigint,0);
  v_last_ordinal bigint:=COALESCE((v_cursor->>'last_stable_ordinal')::bigint,0);
  v_scope_digest text:=COALESCE(NULLIF(v_cursor->>'rolling_scope_digest',''),md5('BPAY_SCOPE_V1'));
  v_fingerprint_digest text:=COALESCE(NULLIF(v_cursor->>'rolling_fingerprint_digest',''),md5('BPAY_FINGERPRINT_V1'));
  v_edge_tag_digest text:=COALESCE(NULLIF(v_cursor->>'rolling_edge_tag_digest',''),md5('BPAY_EDGE_TAG_V1'));
  v_edge_stream_digest text:=COALESCE(NULLIF(v_cursor->>'rolling_dependency_edge_digest',''),md5('BPAY_DEPENDENCY_EDGE_STREAM_V1'));
  v_unit_roll text:=COALESCE(NULLIF(v_cursor->>'current_unit_digest',''),md5('BPAY_UNIT_V1'));
  v_all_unit_digest text:=COALESCE(NULLIF(v_cursor->>'rolling_all_unit_digest',''),md5('BPAY_ALL_UNITS_V1'));
  v_completed_units integer:=COALESCE((v_cursor->>'completed_unit_count')::integer,0);
  v_unit_subphase text:=COALESCE(NULLIF(v_cursor->>'unit_subphase',''),'TAGGED_EDGE_STREAM');
  v_current_unit text:=NULLIF(v_cursor->>'current_unit_key','');
  v_last_timesheet uuid:=NULLIF(v_cursor->>'last_timesheet_id','')::uuid;
  v_last_anchor uuid:=NULLIF(v_cursor->>'last_unit_anchor_timesheet_id','')::uuid;
  v_candidate_id uuid;
  v_source_key text;
  v_member uuid;
  v_edge_kind text;
  v_edge_source uuid;
  v_natural_key text;
  v_financial_digest text;
  v_scope_exists boolean;
  v_edge_exists boolean;
  v_existing_digest text;
  v_anchor_from uuid;
  v_anchor_to uuid;
  v_anchor uuid;
  v_unit_key text;
  v_unit_digest text;
  v_input_fingerprint text;
  v_row_digest text;
  v_next jsonb;
  v_has_more boolean:=true;
  v_row record;
  v_family_names text[]:=ARRAY[
    'CORRECTION_FORWARD','CORRECTION_REVERSE','CORRECTION_OPERATION_UNIT',
    'IMPORT_AUTHORITATIVE_UNIT','BOOKING_FAMILY','ROTATION_FAMILY',
    'CANONICAL_ROTATION_PROJECTION','FINANCE_CASE_LINK','FINANCE_COMPONENT_LINK',
    'RESERVATION_LINK','COMPLETE'
  ];
BEGIN
  IF jsonb_typeof(v_cursor)<>'object' THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_BUILD_CURSOR_INVALID' USING ERRCODE='22023';
  END IF;
  SELECT candidate_id INTO v_candidate_id
  FROM private.banking_pay_workbench_economic_builds WHERE id=p_build_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'PAY_WORKBENCH_BUILD_NOT_CURRENT' USING ERRCODE='40001'; END IF;
  PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    public._pay_workbench_candidate_serial_key(v_candidate_id),24062027));
  SELECT * INTO v_registry FROM private.banking_pay_workbench_candidate_scope_registry
  WHERE candidate_id=v_candidate_id FOR UPDATE;
  SELECT * INTO v_build FROM private.banking_pay_workbench_economic_builds
  WHERE id=p_build_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'PAY_WORKBENCH_BUILD_NOT_CURRENT' USING ERRCODE='40001'; END IF;
  IF v_registry.current_build_id IS DISTINCT FROM p_build_id
     OR v_registry.dirty_generation<>v_build.captured_candidate_generation
     OR v_registry.current_source_change_seq<>v_build.source_change_seq
     OR v_build.status<>'COLLECTING' OR v_build.private_stage<>'DEPENDENCY_CLOSURE' THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_BUILD_NOT_CURRENT' USING ERRCODE='40001';
  END IF;

  IF v_mode='DEPENDENCY_CLOSURE' THEN
    v_frontier:=NULLIF(v_cursor->>'frontier_timesheet_id','')::uuid;
    IF v_frontier IS NULL THEN
      SELECT timesheet_id INTO v_frontier
      FROM private.banking_pay_workbench_economic_build_scope
      WHERE build_id=p_build_id AND closure_status='PENDING'
      ORDER BY timesheet_id LIMIT 1 FOR UPDATE;
      v_family:=1; v_last_key:=NULL;
    END IF;

    IF v_frontier IS NULL THEN
      UPDATE private.banking_pay_workbench_economic_builds
      SET dependency_edge_stream_complete=true,
          dependency_edge_stream_terminal_key_json=jsonb_build_object('terminal',true,'last_edge_key',v_cursor->>'last_edge_key'),
          dependency_edge_stream_digest=v_edge_stream_digest,
          dependency_node_count=scope_count,
          closure_cursor_json=jsonb_build_object(
            'cursor_kind','DEPENDENCY_SCOPE_SEAL','cursor_version',1,'build_id',p_build_id,
            'candidate_id',v_candidate_id,'captured_candidate_generation',v_build.captured_candidate_generation,
            'captured_source_change_seq',v_build.source_change_seq,'seal_phase','ANCHOR_INITIALISE',
            'pass_number',1,'last_timesheet_id',NULL,'last_edge_key',NULL,
            'changed_rows_in_pass',0,'tagged_edge_count',0,'row_seal_count',0,
            'last_stable_ordinal',0,'rolling_scope_digest',md5('BPAY_SCOPE_V1'),
            'rolling_fingerprint_digest',md5('BPAY_FINGERPRINT_V1'),
            'rolling_edge_tag_digest',md5('BPAY_EDGE_TAG_V1'),
            'rolling_dependency_edge_digest',v_edge_stream_digest,
            'rolling_all_unit_digest',md5('BPAY_ALL_UNITS_V1'),
            'completed_unit_count',0,'unit_subphase','TAGGED_EDGE_STREAM','page_number',1
          ),updated_at_utc=clock_timestamp()
      WHERE id=p_build_id;
      v_next:=(SELECT closure_cursor_json FROM private.banking_pay_workbench_economic_builds WHERE id=p_build_id);
      RETURN jsonb_build_object('ok',true,'build_id',p_build_id,'processed_frontier_count',0,
        'processed_emission_count',0,'inserted_scope_count',0,'inserted_edge_count',0,
        'dependency_node_count',v_build.scope_count,'dependency_edge_count',v_build.dependency_edge_count,
        'has_more',true,'next_cursor_json',v_next,'scope_digest',NULL,'dependency_digest',NULL);
    END IF;

    CREATE TEMP TABLE IF NOT EXISTS pg_temp._bpay_wb_closure_edge_page_v2(
      source_key text PRIMARY KEY,member_timesheet_id uuid NOT NULL,
      edge_kind text NOT NULL,edge_source_id uuid NOT NULL
    ) ON COMMIT DROP;
    TRUNCATE pg_temp._bpay_wb_closure_edge_page_v2;

    IF v_family=1 THEN
      INSERT INTO pg_temp._bpay_wb_closure_edge_page_v2
      SELECT 'PARENT:'||parent_timesheet_id::text,parent_timesheet_id,'CORRECTION_FORWARD',timesheet_id
      FROM public.timesheets WHERE timesheet_id=v_frontier AND parent_timesheet_id IS NOT NULL
        AND (v_last_key IS NULL OR 'PARENT:'||parent_timesheet_id::text>v_last_key)
      ORDER BY 1 LIMIT v_limit+1;
    ELSIF v_family=2 THEN
      INSERT INTO pg_temp._bpay_wb_closure_edge_page_v2
      SELECT 'CHILD:'||timesheet_id::text,timesheet_id,'CORRECTION_REVERSE',timesheet_id
      FROM public.timesheets WHERE parent_timesheet_id=v_frontier
        AND (v_last_key IS NULL OR 'CHILD:'||timesheet_id::text>v_last_key)
      ORDER BY 1 LIMIT v_limit+1;
    ELSIF v_family IN (3,4) THEN
      INSERT INTO pg_temp._bpay_wb_closure_edge_page_v2
      SELECT (CASE WHEN v_family=3 THEN 'CORRECTION:' ELSE 'IMPORT:' END)||related.timesheet_id::text,
             related.timesheet_id,
             CASE WHEN v_family=3 THEN 'CORRECTION_OPERATION_UNIT' ELSE 'IMPORT_AUTHORITATIVE_UNIT' END,
             related.timesheet_id
      FROM public.timesheets AS frontier
      JOIN public.timesheets AS related ON related.correction_id=frontier.correction_id
      WHERE frontier.timesheet_id=v_frontier AND frontier.correction_id IS NOT NULL
        AND related.timesheet_id<>v_frontier
        AND (v_last_key IS NULL OR (CASE WHEN v_family=3 THEN 'CORRECTION:' ELSE 'IMPORT:' END)||related.timesheet_id::text>v_last_key)
      ORDER BY 1 LIMIT v_limit+1;
    ELSIF v_family=5 THEN
      INSERT INTO pg_temp._bpay_wb_closure_edge_page_v2
      SELECT 'BOOKING:'||related.timesheet_id::text,related.timesheet_id,'BOOKING_FAMILY',related.timesheet_id
      FROM public.timesheets AS frontier
      JOIN public.timesheets AS related ON related.booking_id=frontier.booking_id
      WHERE frontier.timesheet_id=v_frontier AND frontier.booking_id IS NOT NULL
        AND related.timesheet_id<>v_frontier
        AND (v_last_key IS NULL OR 'BOOKING:'||related.timesheet_id::text>v_last_key)
      ORDER BY 1 LIMIT v_limit+1;
    ELSIF v_family IN (6,7) THEN
      INSERT INTO pg_temp._bpay_wb_closure_edge_page_v2
      SELECT (CASE WHEN v_family=6 THEN 'ROTATION:' ELSE 'CANONICAL_ROTATION:' END)||rotation.family_timesheet_id::text,
             rotation.family_timesheet_id,
             CASE WHEN v_family=6 THEN 'ROTATION_FAMILY' ELSE 'CANONICAL_ROTATION_PROJECTION' END,
             rotation.family_timesheet_id
      FROM public._pay_timesheet_rotation_scope(ARRAY[v_frontier]) AS rotation
      WHERE rotation.family_timesheet_id<>v_frontier
        AND (v_last_key IS NULL OR (CASE WHEN v_family=6 THEN 'ROTATION:' ELSE 'CANONICAL_ROTATION:' END)||rotation.family_timesheet_id::text>v_last_key)
      ORDER BY 1 LIMIT v_limit+1;
    ELSIF v_family IN (8,9) THEN
      INSERT INTO pg_temp._bpay_wb_closure_edge_page_v2
      SELECT (CASE WHEN v_family=8 THEN 'CASE:' ELSE 'COMPONENT:' END)||component_row.id::text,
             component_row.linked_timesheet_id,
             CASE WHEN v_family=8 THEN 'FINANCE_CASE_LINK' ELSE 'FINANCE_COMPONENT_LINK' END,
             component_row.id
      FROM public.pay_finance_case_components AS frontier_component
      JOIN public.pay_finance_case_components AS component_row
        ON component_row.finance_case_id=frontier_component.finance_case_id
      WHERE frontier_component.linked_timesheet_id=v_frontier
        AND component_row.linked_timesheet_id IS NOT NULL
        AND component_row.linked_timesheet_id<>v_frontier
        AND (v_last_key IS NULL OR (CASE WHEN v_family=8 THEN 'CASE:' ELSE 'COMPONENT:' END)||component_row.id::text>v_last_key)
      ORDER BY 1 LIMIT v_limit+1;
    ELSIF v_family=10 THEN
      INSERT INTO pg_temp._bpay_wb_closure_edge_page_v2
      SELECT 'RESERVATION:'||reservation_row.id::text||':'||linked_component.linked_timesheet_id::text,
             linked_component.linked_timesheet_id,'RESERVATION_LINK',reservation_row.id
      FROM public.pay_finance_case_components AS frontier_component
      JOIN public.pay_advance_reservations AS reservation_row
        ON reservation_row.finance_case_id=frontier_component.finance_case_id
        OR reservation_row.finance_component_id=frontier_component.id
      JOIN public.pay_finance_case_components AS linked_component
        ON linked_component.id=reservation_row.finance_component_id
      WHERE frontier_component.linked_timesheet_id=v_frontier
        AND linked_component.linked_timesheet_id IS NOT NULL
        AND linked_component.linked_timesheet_id<>v_frontier
        AND (v_last_key IS NULL OR 'RESERVATION:'||reservation_row.id::text||':'||linked_component.linked_timesheet_id::text>v_last_key)
      ORDER BY 1 LIMIT v_limit+1;
    END IF;

    FOR v_row IN SELECT * FROM pg_temp._bpay_wb_closure_edge_page_v2 ORDER BY source_key
    LOOP
      SELECT EXISTS(SELECT 1 FROM private.banking_pay_workbench_economic_build_scope
        WHERE build_id=p_build_id AND timesheet_id=v_row.member_timesheet_id) INTO v_scope_exists;
      IF v_used+1+(CASE WHEN v_scope_exists THEN 0 ELSE 1 END)>v_limit THEN EXIT; END IF;
      IF NOT v_scope_exists AND NOT EXISTS(
        SELECT 1 FROM public.timesheets_financials AS owner_row
        WHERE owner_row.timesheet_id=v_row.member_timesheet_id AND owner_row.candidate_id=v_candidate_id
      ) THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_DEPENDENCY_IDENTITY_CONFLICT' USING ERRCODE='23514';
      END IF;
      v_source_key:=v_row.source_key; v_member:=v_row.member_timesheet_id;
      v_edge_kind:=v_row.edge_kind; v_edge_source:=v_row.edge_source_id;
      v_natural_key:=lower(md5(v_edge_kind||':'||v_frontier::text||':'||v_member::text||':'||v_edge_source::text));
      v_financial_digest:=md5(jsonb_build_object('edge_kind',v_edge_kind,
        'from',v_frontier,'to',v_member,'source',v_edge_source)::text);
      v_edge_exists:=false; v_existing_digest:=NULL;
      SELECT true,financial_digest INTO v_edge_exists,v_existing_digest
      FROM private.banking_pay_workbench_economic_build_facts
      WHERE build_id=p_build_id AND fact_family='DEPENDENCY_EDGE' AND natural_key=v_natural_key;
      IF COALESCE(v_edge_exists,false) THEN
        IF v_existing_digest IS DISTINCT FROM v_financial_digest THEN
          RAISE EXCEPTION 'PAY_WORKBENCH_DEPENDENCY_EDGE_REPLAY_CONFLICT' USING ERRCODE='23514';
        END IF;
      ELSE
        INSERT INTO private.banking_pay_workbench_economic_build_facts(
          build_id,fact_family,natural_key,candidate_id,subject_timesheet_ids,
          source_relation,source_id,financial_digest,edge_kind,
          edge_from_timesheet_id,edge_to_timesheet_id,edge_source_id
        ) VALUES (p_build_id,'DEPENDENCY_EDGE',v_natural_key,v_candidate_id,
          ARRAY[v_frontier,v_member],CASE WHEN v_edge_kind LIKE 'FINANCE_%' THEN 'pay_finance_case_components'
            WHEN v_edge_kind='RESERVATION_LINK' THEN 'pay_advance_reservations' ELSE 'timesheets' END,
          v_edge_source,v_financial_digest,v_edge_kind,v_frontier,v_member,v_edge_source);
        v_new_edges:=v_new_edges+1;
      END IF;
      v_edge_stream_digest:=md5(v_edge_stream_digest||v_natural_key||v_financial_digest);
      IF NOT v_scope_exists THEN
        INSERT INTO private.banking_pay_workbench_economic_build_scope(
          build_id,timesheet_id,candidate_id,seed_reasons,dependency_reasons,
          captured_dirty_generation,required_fact_families
        ) VALUES (p_build_id,v_member,v_candidate_id,ARRAY[]::text[],ARRAY[v_edge_kind],
          v_build.captured_candidate_generation,
          ARRAY['FROZEN_SETTLED_COMPONENT','LIVE_ENTITLEMENT_INPUT','ENTITLEMENT_COMPONENT',
            'PAY_STATE_FALLBACK','PAYEE_BASELINE_INPUT','CANONICAL_INPUT']::text[])
        ON CONFLICT(build_id,timesheet_id) DO NOTHING;
        v_new_scope:=v_new_scope+1;
      ELSE
        UPDATE private.banking_pay_workbench_economic_build_scope AS scope_row
        SET dependency_reasons=(SELECT array_agg(DISTINCT reason ORDER BY reason)
          FROM unnest(scope_row.dependency_reasons||ARRAY[v_edge_kind]) AS reason),
            updated_at_utc=clock_timestamp()
        WHERE build_id=p_build_id AND timesheet_id=v_member;
      END IF;
      v_used:=v_used+1+(CASE WHEN v_scope_exists THEN 0 ELSE 1 END);
      v_processed:=v_processed+1; v_edge_count:=v_edge_count+1;
      v_emission_count:=v_emission_count+1; v_last_key:=v_source_key;
    END LOOP;

    IF v_processed=0 AND EXISTS(SELECT 1 FROM pg_temp._bpay_wb_closure_edge_page_v2) THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_DEPENDENCY_PAGE_UNIT_TOO_SMALL' USING ERRCODE='54000';
    END IF;
    IF NOT EXISTS(
      SELECT 1 FROM pg_temp._bpay_wb_closure_edge_page_v2
      WHERE v_last_key IS NULL OR source_key>v_last_key
    ) THEN
      v_family:=v_family+1; v_last_key:=NULL;
    END IF;
    IF v_family>10 THEN
      UPDATE private.banking_pay_workbench_economic_build_scope
      SET closure_status='EXPANDED',closure_family_ordinal=11,updated_at_utc=clock_timestamp()
      WHERE build_id=p_build_id AND timesheet_id=v_frontier AND closure_status='PENDING';
      v_frontier:=NULL; v_family:=1;
    ELSE
      UPDATE private.banking_pay_workbench_economic_build_scope
      SET closure_family_ordinal=v_family,closure_last_edge_key=v_last_key,
          closure_processed_edge_count=closure_processed_edge_count+v_processed,
          closure_processed_emission_count=closure_processed_emission_count+v_processed,
          updated_at_utc=clock_timestamp()
      WHERE build_id=p_build_id AND timesheet_id=v_frontier;
    END IF;
    UPDATE private.banking_pay_workbench_economic_builds
    SET scope_count=scope_count+v_new_scope,dependency_node_count=dependency_node_count+v_new_scope,
        dependency_edge_count=dependency_edge_count+v_new_edges,
        dependency_edge_stream_digest=v_edge_stream_digest,
        closure_cursor_json=jsonb_build_object(
          'cursor_kind','DEPENDENCY_CLOSURE','cursor_version',1,'build_id',p_build_id,
          'candidate_id',v_candidate_id,'captured_candidate_generation',v_build.captured_candidate_generation,
          'captured_source_change_seq',v_build.source_change_seq,'frontier_timesheet_id',v_frontier,
          'dependency_family',v_family_names[v_family],'dependency_family_ordinal',v_family,
          'last_edge_key',v_last_key,'processed_edge_count',v_edge_count,
          'processed_emission_count',v_emission_count,'page_number',v_page+1,
          'rolling_dependency_edge_digest',v_edge_stream_digest,
          'previous_page_digest',md5(v_cursor::text||COALESCE(v_last_key,'')||v_edge_stream_digest)
        ),updated_at_utc=clock_timestamp()
    WHERE id=p_build_id;
    v_next:=(SELECT closure_cursor_json FROM private.banking_pay_workbench_economic_builds WHERE id=p_build_id);
    RETURN jsonb_build_object('ok',true,'build_id',p_build_id,'processed_frontier_count',1,
      'processed_emission_count',v_processed,'inserted_scope_count',v_new_scope,
      'inserted_edge_count',v_new_edges,'dependency_node_count',v_build.dependency_node_count+v_new_scope,
      'dependency_edge_count',v_build.dependency_edge_count+v_new_edges,'has_more',true,
      'next_cursor_json',v_next,'scope_digest',NULL,'dependency_digest',NULL);
  END IF;

  IF v_mode<>'DEPENDENCY_SCOPE_SEAL' THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_BUILD_CURSOR_INVALID' USING ERRCODE='22023';
  END IF;

  -- Phase 1: bounded anchor initialisation.
  IF v_phase='ANCHOR_INITIALISE' THEN
    FOR v_row IN SELECT timesheet_id FROM private.banking_pay_workbench_economic_build_scope
      WHERE build_id=p_build_id AND closure_status='EXPANDED'
        AND (v_last_timesheet IS NULL OR timesheet_id>v_last_timesheet)
      ORDER BY timesheet_id LIMIT v_limit FOR UPDATE
    LOOP
      UPDATE private.banking_pay_workbench_economic_build_scope
      SET dependency_unit_anchor_timesheet_id=v_row.timesheet_id,closure_status='SEALING',updated_at_utc=clock_timestamp()
      WHERE build_id=p_build_id AND timesheet_id=v_row.timesheet_id;
      v_last_timesheet:=v_row.timesheet_id; v_processed:=v_processed+1;
    END LOOP;
    IF v_processed<v_limit THEN v_phase:='ANCHOR_PROPAGATE';v_last_timesheet:=NULL;v_last_key:=NULL;v_pass:=1;v_changed:=0; END IF;

  ELSIF v_phase='ANCHOR_PROPAGATE' THEN
    FOR v_row IN SELECT natural_key,edge_from_timesheet_id,edge_to_timesheet_id
      FROM private.banking_pay_workbench_economic_build_facts
      WHERE build_id=p_build_id AND fact_family='DEPENDENCY_EDGE'
        AND (v_last_key IS NULL OR natural_key>v_last_key)
      ORDER BY natural_key LIMIT v_limit
    LOOP
      SELECT dependency_unit_anchor_timesheet_id INTO v_anchor_from
      FROM private.banking_pay_workbench_economic_build_scope
      WHERE build_id=p_build_id AND timesheet_id=v_row.edge_from_timesheet_id FOR UPDATE;
      SELECT dependency_unit_anchor_timesheet_id INTO v_anchor_to
      FROM private.banking_pay_workbench_economic_build_scope
      WHERE build_id=p_build_id AND timesheet_id=v_row.edge_to_timesheet_id FOR UPDATE;
      v_anchor:=LEAST(v_anchor_from,v_anchor_to);
      UPDATE private.banking_pay_workbench_economic_build_scope
      SET dependency_unit_anchor_timesheet_id=v_anchor,updated_at_utc=clock_timestamp()
      WHERE build_id=p_build_id AND timesheet_id IN (v_row.edge_from_timesheet_id,v_row.edge_to_timesheet_id)
        AND dependency_unit_anchor_timesheet_id<>v_anchor;
      GET DIAGNOSTICS v_used=ROW_COUNT; v_changed:=v_changed+v_used;
      v_last_key:=v_row.natural_key;v_processed:=v_processed+1;
    END LOOP;
    IF v_processed<v_limit THEN
      IF v_changed>0 THEN v_pass:=v_pass+1;v_last_key:=NULL;v_changed:=0;
      ELSE v_phase:='EDGE_UNIT_TAG';v_last_key:=NULL; END IF;
    END IF;

  ELSIF v_phase='EDGE_UNIT_TAG' THEN
    FOR v_row IN SELECT natural_key,edge_from_timesheet_id,edge_to_timesheet_id,financial_digest
      FROM private.banking_pay_workbench_economic_build_facts
      WHERE build_id=p_build_id AND fact_family='DEPENDENCY_EDGE'
        AND (v_last_key IS NULL OR natural_key>v_last_key)
      ORDER BY natural_key LIMIT v_limit FOR UPDATE
    LOOP
      SELECT dependency_unit_anchor_timesheet_id INTO v_anchor_from FROM private.banking_pay_workbench_economic_build_scope
      WHERE build_id=p_build_id AND timesheet_id=v_row.edge_from_timesheet_id;
      SELECT dependency_unit_anchor_timesheet_id INTO v_anchor_to FROM private.banking_pay_workbench_economic_build_scope
      WHERE build_id=p_build_id AND timesheet_id=v_row.edge_to_timesheet_id;
      IF v_anchor_from IS NULL OR v_anchor_from IS DISTINCT FROM v_anchor_to THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_DEPENDENCY_IDENTITY_CONFLICT' USING ERRCODE='23514';
      END IF;
      v_unit_key:='UNIT:'||lower(v_anchor_from::text);
      UPDATE private.banking_pay_workbench_economic_build_facts SET dependency_unit_key=v_unit_key
      WHERE build_id=p_build_id AND fact_family='DEPENDENCY_EDGE' AND natural_key=v_row.natural_key;
      v_edge_tag_digest:=md5(v_edge_tag_digest||md5(v_row.natural_key||':'||v_unit_key||':'||v_row.financial_digest));
      v_tagged:=v_tagged+1;v_last_key:=v_row.natural_key;v_processed:=v_processed+1;
    END LOOP;
    IF v_processed<v_limit THEN
      IF v_tagged<>v_build.dependency_edge_count THEN RAISE EXCEPTION 'PAY_WORKBENCH_DEPENDENCY_EDGE_COUNT_MISMATCH' USING ERRCODE='23514'; END IF;
      UPDATE private.banking_pay_workbench_economic_builds SET tagged_edge_count=v_tagged,
        edge_tag_digest=v_edge_tag_digest,edge_tag_stream_complete=true,
        edge_tag_stream_terminal_key_json=jsonb_build_object('terminal',true,'last_edge_key',v_last_key),updated_at_utc=clock_timestamp()
      WHERE id=p_build_id;
      v_phase:='UNIT_DIGEST';v_last_key:=NULL;v_current_unit:=NULL;
      v_unit_subphase:='TAGGED_EDGE_STREAM';v_unit_roll:=md5('BPAY_UNIT_V1');
    END IF;

  ELSIF v_phase='UNIT_DIGEST' THEN
    IF v_unit_subphase='TAGGED_EDGE_STREAM' THEN
      FOR v_row IN
        SELECT dependency_unit_key,natural_key,financial_digest
        FROM private.banking_pay_workbench_economic_build_facts
        WHERE build_id=p_build_id AND fact_family='DEPENDENCY_EDGE'
          AND dependency_unit_key IS NOT NULL
          AND (
            v_current_unit IS NULL
            OR dependency_unit_key>v_current_unit
            OR (dependency_unit_key=v_current_unit AND (v_last_key IS NULL OR natural_key>v_last_key))
          )
        ORDER BY dependency_unit_key,natural_key LIMIT v_limit
      LOOP
        IF v_current_unit IS NULL THEN
          v_current_unit:=v_row.dependency_unit_key;
          v_unit_roll:=md5('BPAY_UNIT_V1');
        ELSIF v_row.dependency_unit_key<>v_current_unit THEN
          v_anchor:=substring(v_current_unit from 6)::uuid;
          UPDATE private.banking_pay_workbench_economic_build_scope
          SET dependency_unit_digest=v_unit_roll,updated_at_utc=clock_timestamp()
          WHERE build_id=p_build_id AND timesheet_id=v_anchor
            AND dependency_unit_anchor_timesheet_id=v_anchor AND dependency_unit_digest IS NULL;
          IF NOT FOUND THEN RAISE EXCEPTION 'PAY_WORKBENCH_DEPENDENCY_UNIT_DIGEST_CONFLICT' USING ERRCODE='23514'; END IF;
          v_all_unit_digest:=md5(v_all_unit_digest||v_current_unit||v_unit_roll);
          v_completed_units:=v_completed_units+1;
          v_current_unit:=v_row.dependency_unit_key;
          v_unit_roll:=md5('BPAY_UNIT_V1');
        END IF;
        v_unit_roll:=md5(v_unit_roll||v_row.natural_key||v_row.financial_digest);
        v_last_key:=v_row.natural_key;v_processed:=v_processed+1;
      END LOOP;
      IF v_processed<v_limit THEN
        IF v_current_unit IS NOT NULL THEN
          v_anchor:=substring(v_current_unit from 6)::uuid;
          UPDATE private.banking_pay_workbench_economic_build_scope
          SET dependency_unit_digest=v_unit_roll,updated_at_utc=clock_timestamp()
          WHERE build_id=p_build_id AND timesheet_id=v_anchor
            AND dependency_unit_anchor_timesheet_id=v_anchor AND dependency_unit_digest IS NULL;
          IF NOT FOUND THEN RAISE EXCEPTION 'PAY_WORKBENCH_DEPENDENCY_UNIT_DIGEST_CONFLICT' USING ERRCODE='23514'; END IF;
          v_all_unit_digest:=md5(v_all_unit_digest||v_current_unit||v_unit_roll);
          v_completed_units:=v_completed_units+1;
        END IF;
        v_unit_subphase:='ISOLATED_UNITS';v_current_unit:=NULL;v_last_key:=NULL;v_last_timesheet:=NULL;
      END IF;
    ELSE
      FOR v_row IN
        SELECT timesheet_id,dependency_unit_anchor_timesheet_id
        FROM private.banking_pay_workbench_economic_build_scope
        WHERE build_id=p_build_id AND timesheet_id=dependency_unit_anchor_timesheet_id
          AND dependency_unit_digest IS NULL
          AND (v_last_timesheet IS NULL OR timesheet_id>v_last_timesheet)
        ORDER BY timesheet_id LIMIT v_limit FOR UPDATE
      LOOP
        v_unit_key:='UNIT:'||lower(v_row.timesheet_id::text);
        v_unit_roll:=md5('BPAY_UNIT_V1:'||v_row.timesheet_id::text);
        UPDATE private.banking_pay_workbench_economic_build_scope
        SET dependency_unit_digest=v_unit_roll,updated_at_utc=clock_timestamp()
        WHERE build_id=p_build_id AND timesheet_id=v_row.timesheet_id AND dependency_unit_digest IS NULL;
        v_all_unit_digest:=md5(v_all_unit_digest||v_unit_key||v_unit_roll);
        v_completed_units:=v_completed_units+1;v_last_timesheet:=v_row.timesheet_id;v_processed:=v_processed+1;
      END LOOP;
      IF v_processed<v_limit THEN
        v_phase:='ROW_SEAL';v_last_timesheet:=NULL;v_current_unit:=NULL;v_last_key:=NULL;
      END IF;
    END IF;

  ELSIF v_phase='ROW_SEAL' THEN
    FOR v_row IN SELECT scope_row.timesheet_id,scope_row.dependency_unit_anchor_timesheet_id,
      anchor_row.dependency_unit_digest
      FROM private.banking_pay_workbench_economic_build_scope AS scope_row
      JOIN private.banking_pay_workbench_economic_build_scope AS anchor_row
        ON anchor_row.build_id=scope_row.build_id
       AND anchor_row.timesheet_id=scope_row.dependency_unit_anchor_timesheet_id
      WHERE scope_row.build_id=p_build_id AND scope_row.closure_status='SEALING'
        AND (
          v_last_anchor IS NULL
          OR (scope_row.dependency_unit_anchor_timesheet_id,scope_row.timesheet_id)>(v_last_anchor,v_last_timesheet)
        )
      ORDER BY scope_row.dependency_unit_anchor_timesheet_id,scope_row.timesheet_id
      LIMIT v_limit FOR UPDATE OF scope_row
    LOOP
      SELECT fingerprint.input_fingerprint INTO v_input_fingerprint
      FROM private.pay_workbench_timesheet_input_fingerprint_v1(p_build_id,v_candidate_id,ARRAY[v_row.timesheet_id]) AS fingerprint;
      IF v_input_fingerprint IS NULL THEN RAISE EXCEPTION 'PAY_WORKBENCH_FINGERPRINT_INCOMPLETE' USING ERRCODE='23514'; END IF;
      v_last_ordinal:=v_last_ordinal+1;
      v_unit_key:='UNIT:'||lower(v_row.dependency_unit_anchor_timesheet_id::text);
      UPDATE private.banking_pay_workbench_economic_build_scope SET
        dependency_unit_key=v_unit_key,dependency_unit_digest=v_row.dependency_unit_digest,
        stable_ordinal=v_last_ordinal,captured_input_fingerprint=v_input_fingerprint,
        closure_status='SEALED',seal_prepared_at_utc=clock_timestamp(),updated_at_utc=clock_timestamp()
      WHERE build_id=p_build_id AND timesheet_id=v_row.timesheet_id;
      v_row_digest:=md5(jsonb_build_object('timesheet_id',v_row.timesheet_id,'unit',v_unit_key,
        'unit_digest',v_row.dependency_unit_digest,'ordinal',v_last_ordinal,'fingerprint',v_input_fingerprint)::text);
      v_scope_digest:=md5(v_scope_digest||v_row_digest);
      v_fingerprint_digest:=md5(v_fingerprint_digest||v_input_fingerprint);
      v_row_seal_count:=v_row_seal_count+1;v_last_anchor:=v_row.dependency_unit_anchor_timesheet_id;
      v_last_timesheet:=v_row.timesheet_id;v_processed:=v_processed+1;
    END LOOP;
    IF v_processed<v_limit THEN v_phase:='COMPLETE'; END IF;

  ELSIF v_phase='COMPLETE' THEN
    -- Metadata-only terminal proof: scalar cursor/header checks plus one partial-index probe.
    IF NOT v_build.dependency_edge_stream_complete
       OR NOT v_build.edge_tag_stream_complete
       OR v_build.tagged_edge_count<>v_build.dependency_edge_count
       OR v_build.unit_count<>v_completed_units OR v_build.unit_digest IS NULL
       OR v_row_seal_count<>v_build.scope_count OR v_last_ordinal<>v_row_seal_count
       OR EXISTS(SELECT 1 FROM private.banking_pay_workbench_economic_build_scope
         WHERE build_id=p_build_id AND closure_status<>'SEALED' LIMIT 1) THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_DEPENDENCY_SEAL_INCOMPLETE' USING ERRCODE='23514';
    END IF;
    v_next:=CASE
      WHEN v_registry.initialisation_status IN ('DISCOVERING','CLASSIFYING')
       AND v_registry.bootstrap_id IS NOT NULL
      THEN jsonb_build_object(
        'cursor_kind','BOOTSTRAP_DISCOVERY','cursor_version',1,
        'bootstrap_id',v_registry.bootstrap_id,
        'bootstrap_stream','CLASSIFY_UNITS','classification_phase','EVIDENCE',
        'last_dependency_unit_key',NULL,'dependency_unit_key',NULL,
        'last_stable_ordinal',0,'unit_financially_relevant',false,
        'build_id',p_build_id,'candidate_id',v_candidate_id,
        'captured_candidate_generation',v_build.captured_candidate_generation,
        'captured_source_change_seq',v_build.source_change_seq
      )
      ELSE jsonb_build_object(
        'cursor_kind','WORKSPACE_FACT','cursor_version',1,
        'build_id',p_build_id,'candidate_id',v_candidate_id,
        'dependency_unit_key',NULL,'fact_family','FROZEN_SETTLED_COMPONENT',
        'page_number',1,'last_source_key',NULL
      )
    END;
    UPDATE private.banking_pay_workbench_economic_builds SET
      row_seal_count=v_row_seal_count,last_stable_ordinal=v_last_ordinal,
      scope_digest=v_scope_digest,sealed_fingerprint_digest=v_fingerprint_digest,
      dependency_digest=md5(COALESCE(dependency_edge_stream_digest,'')||COALESCE(edge_tag_digest,'')),
      unit_digest=v_all_unit_digest,unit_count=v_completed_units,
      dependency_closure_sealed_at_utc=clock_timestamp(),
      private_stage=CASE
        WHEN v_registry.initialisation_status IN ('DISCOVERING','CLASSIFYING')
         AND v_registry.bootstrap_id IS NOT NULL THEN 'BOOTSTRAP_DISCOVERY'
        ELSE 'WORKSPACE_FACT' END,
      closure_cursor_json=jsonb_build_object('cursor_kind','DEPENDENCY_SCOPE_SEAL','cursor_version',1,
        'seal_phase','COMPLETE','terminal',true,'build_id',p_build_id,'candidate_id',v_candidate_id,
        'tagged_edge_count',tagged_edge_count,'row_seal_count',v_row_seal_count,
        'completed_unit_count',v_completed_units,'rolling_all_unit_digest',v_all_unit_digest,
        'last_stable_ordinal',v_last_ordinal,'rolling_scope_digest',v_scope_digest,
        'rolling_fingerprint_digest',v_fingerprint_digest,'rolling_edge_tag_digest',edge_tag_digest),
      fact_cursor_json=CASE
        WHEN v_registry.initialisation_status IN ('DISCOVERING','CLASSIFYING')
         AND v_registry.bootstrap_id IS NOT NULL THEN '{}'::jsonb
        ELSE v_next END,
      scope_cursor_json=CASE
        WHEN v_registry.initialisation_status IN ('DISCOVERING','CLASSIFYING')
         AND v_registry.bootstrap_id IS NOT NULL THEN v_next
        ELSE scope_cursor_json END,
      updated_at_utc=clock_timestamp()
    WHERE id=p_build_id;
    IF v_registry.initialisation_status IN ('DISCOVERING','CLASSIFYING')
       AND v_registry.bootstrap_id IS NOT NULL THEN
      UPDATE private.banking_pay_workbench_candidate_scope_registry
      SET initialisation_status='CLASSIFYING',bootstrap_stream='CLASSIFY_UNITS',
          bootstrap_cursor_json=v_next,updated_at_utc=clock_timestamp()
      WHERE candidate_id=v_candidate_id AND current_build_id=p_build_id;
    END IF;
    RETURN jsonb_build_object('ok',true,'build_id',p_build_id,'processed_frontier_count',0,
      'processed_emission_count',0,'inserted_scope_count',0,'inserted_edge_count',0,
      'dependency_node_count',v_build.scope_count,'dependency_edge_count',v_build.dependency_edge_count,
      'has_more',true,'next_cursor_json',v_next,'scope_digest',v_scope_digest,
      'dependency_digest',md5(COALESCE(v_build.dependency_edge_stream_digest,'')||COALESCE(v_build.edge_tag_digest,'')));
  ELSE
    RAISE EXCEPTION 'PAY_WORKBENCH_BUILD_CURSOR_INVALID' USING ERRCODE='22023';
  END IF;

  v_next:=jsonb_build_object(
    'cursor_kind','DEPENDENCY_SCOPE_SEAL','cursor_version',1,'build_id',p_build_id,
    'candidate_id',v_candidate_id,'captured_candidate_generation',v_build.captured_candidate_generation,
    'captured_source_change_seq',v_build.source_change_seq,'seal_phase',v_phase,
    'pass_number',v_pass,'last_timesheet_id',v_last_timesheet,
    'last_unit_anchor_timesheet_id',v_last_anchor,'last_edge_key',v_last_key,
    'changed_rows_in_pass',v_changed,'current_unit_key',v_current_unit,
    'current_unit_digest',v_unit_roll,'unit_subphase',v_unit_subphase,
    'completed_unit_count',v_completed_units,'rolling_all_unit_digest',v_all_unit_digest,
    'rolling_dependency_edge_digest',v_edge_stream_digest,'tagged_edge_count',v_tagged,
    'row_seal_count',v_row_seal_count,'last_stable_ordinal',v_last_ordinal,
    'rolling_scope_digest',v_scope_digest,'rolling_fingerprint_digest',v_fingerprint_digest,
    'rolling_edge_tag_digest',v_edge_tag_digest,'page_number',v_page+1
  );
  UPDATE private.banking_pay_workbench_economic_builds
  SET closure_cursor_json=v_next,row_seal_count=v_row_seal_count,last_stable_ordinal=v_last_ordinal,
      unit_count=v_completed_units,unit_digest=v_all_unit_digest,
      dependency_edge_stream_digest=v_edge_stream_digest,
      scope_digest=CASE WHEN v_row_seal_count>0 THEN v_scope_digest ELSE scope_digest END,
      sealed_fingerprint_digest=CASE WHEN v_row_seal_count>0 THEN v_fingerprint_digest ELSE sealed_fingerprint_digest END,
      updated_at_utc=clock_timestamp()
  WHERE id=p_build_id;
  RETURN jsonb_build_object('ok',true,'build_id',p_build_id,'processed_frontier_count',v_processed,
    'processed_emission_count',v_processed,'inserted_scope_count',0,'inserted_edge_count',0,
    'dependency_node_count',v_build.scope_count,'dependency_edge_count',v_build.dependency_edge_count,
    'has_more',v_has_more,'next_cursor_json',v_next,'scope_digest',NULL,'dependency_digest',NULL);
END;
$function$;

ALTER FUNCTION private.pay_workbench_timesheet_dependency_closure_v2(uuid,jsonb,integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_timesheet_dependency_closure_v2(uuid,jsonb,integer) FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_timesheet_dependency_closure_v2(uuid,jsonb,integer) TO postgres;
