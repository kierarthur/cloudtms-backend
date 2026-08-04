-- Banking Pay bounded-scope V1.2.4: indexed active-state seed selector.

CREATE OR REPLACE FUNCTION private.pay_workbench_candidate_bounded_scope_v1(
  p_build_id uuid,
  p_candidate_id uuid,
  p_cursor_json jsonb DEFAULT '{}'::jsonb,
  p_limit integer DEFAULT 500,
  p_targeted_timesheet_ids uuid[] DEFAULT ARRAY[]::uuid[],
  p_force_include_timesheet_ids uuid[] DEFAULT ARRAY[]::uuid[],
  p_exclude_timesheet_ids uuid[] DEFAULT ARRAY[]::uuid[],
  p_client_filter_single uuid DEFAULT NULL::uuid
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
  v_cursor jsonb := COALESCE(p_cursor_json,'{}'::jsonb);
  v_limit integer := LEAST(GREATEST(COALESCE(p_limit,500),1),500);
  v_cursor_kind text := COALESCE(NULLIF(v_cursor->>'cursor_kind',''),'PREPARE_SCOPE');
  v_family_ordinal integer := COALESCE((v_cursor->>'seed_family_ordinal')::integer,1);
  v_family text := COALESCE(NULLIF(v_cursor->>'seed_family',''),'ACTIVE_STATE');
  v_last_key text := v_cursor->>'last_source_key';
  v_selected_count integer := 0;
  v_inserted_count integer := 0;
  v_merged_count integer := 0;
  v_accumulated_count bigint := COALESCE((v_cursor->>'accumulated_row_count')::bigint,0);
  v_processed_count bigint := COALESCE((v_cursor->>'processed_source_rows')::bigint,0);
  v_rolling_digest text := COALESCE(NULLIF(v_cursor->>'rolling_digest',''),md5('BPAY_SEED_SCOPE_V1'));
  v_row_digest text;
  v_has_more boolean := true;
  v_initial_sealed boolean := false;
  v_next_cursor jsonb;
  v_row record;
  v_scope_exists boolean;
  v_last_timesheet_id uuid;
  v_next_family text;
BEGIN
  IF jsonb_typeof(v_cursor)<>'object' THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_BUILD_CURSOR_INVALID' USING ERRCODE='22023';
  END IF;

  SELECT * INTO v_registry
  FROM private.banking_pay_workbench_candidate_scope_registry AS registry
  WHERE registry.candidate_id=p_candidate_id FOR UPDATE;
  SELECT * INTO v_build
  FROM private.banking_pay_workbench_economic_builds AS build_row
  WHERE build_row.id=p_build_id FOR UPDATE;

  IF v_registry.candidate_id IS NULL OR v_build.id IS NULL
     OR v_build.candidate_id<>p_candidate_id
     OR v_registry.current_build_id IS DISTINCT FROM p_build_id
     OR v_registry.dirty_generation<>v_build.captured_candidate_generation
     OR v_registry.current_source_change_seq<>v_build.source_change_seq
     OR v_build.status<>'COLLECTING' OR v_build.private_stage<>'PREPARE_SCOPE' THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_BUILD_NOT_CURRENT'
      USING ERRCODE='40001',DETAIL=jsonb_build_object(
        'code','PAY_WORKBENCH_BUILD_NOT_CURRENT','build_id',p_build_id,'candidate_id',p_candidate_id
      )::text;
  END IF;

  IF COALESCE(v_cursor->>'build_id',p_build_id::text)<>p_build_id::text
     OR COALESCE(v_cursor->>'candidate_id',p_candidate_id::text)<>p_candidate_id::text
     OR COALESCE((v_cursor->>'captured_candidate_generation')::bigint,v_build.captured_candidate_generation)
        <>v_build.captured_candidate_generation
     OR COALESCE((v_cursor->>'captured_source_change_seq')::bigint,v_build.source_change_seq)
        <>v_build.source_change_seq THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_BUILD_CURSOR_INVALID' USING ERRCODE='22023';
  END IF;

  IF v_cursor_kind='SEED_SCOPE_SEAL' THEN
    v_last_timesheet_id := NULLIF(v_cursor->>'last_timesheet_id','')::uuid;
    FOR v_row IN
      SELECT scope_row.timesheet_id,scope_row.candidate_id,scope_row.seed_reasons,
             scope_row.captured_dirty_generation
      FROM private.banking_pay_workbench_economic_build_scope AS scope_row
      WHERE scope_row.build_id=p_build_id
        AND (v_last_timesheet_id IS NULL OR scope_row.timesheet_id>v_last_timesheet_id)
      ORDER BY scope_row.timesheet_id
      LIMIT v_limit
      FOR UPDATE
    LOOP
      v_selected_count:=v_selected_count+1;
      v_last_timesheet_id:=v_row.timesheet_id;
      v_row_digest:=md5(jsonb_build_object(
        'candidate_id',v_row.candidate_id,'timesheet_id',v_row.timesheet_id,
        'seed_reasons',(SELECT jsonb_agg(reason ORDER BY reason)
          FROM unnest(v_row.seed_reasons) AS reason),
        'captured_dirty_generation',v_row.captured_dirty_generation
      )::text);
      v_rolling_digest:=md5(v_rolling_digest||v_row_digest);
      v_accumulated_count:=v_accumulated_count+1;
    END LOOP;

    IF v_selected_count<v_limit THEN
      UPDATE private.banking_pay_workbench_economic_builds
      SET seed_scope_count=v_accumulated_count::integer,
          seed_scope_digest=v_rolling_digest,
          seed_scope_sealed_at_utc=clock_timestamp(),
          scope_count=v_accumulated_count::integer,
          dependency_node_count=v_accumulated_count::integer,
          private_stage='DEPENDENCY_CLOSURE',
          scope_cursor_json=jsonb_build_object(
            'cursor_kind','SEED_SCOPE_SEAL','cursor_version',1,'terminal',true,
            'build_id',p_build_id,'candidate_id',p_candidate_id,
            'processed_row_count',v_accumulated_count,'rolling_digest',v_rolling_digest
          ),
          closure_cursor_json=jsonb_build_object(
            'cursor_kind','DEPENDENCY_CLOSURE','cursor_version',1,
            'build_id',p_build_id,'candidate_id',p_candidate_id,
            'captured_candidate_generation',v_build.captured_candidate_generation,
            'captured_source_change_seq',v_build.source_change_seq,
            'frontier_timesheet_id',NULL,'dependency_family','CORRECTION_FORWARD',
            'dependency_family_ordinal',1,'last_edge_key',NULL,
            'processed_edge_count',0,'processed_emission_count',0,'page_number',1
          ),
          updated_at_utc=clock_timestamp()
      WHERE id=p_build_id AND private_stage='PREPARE_SCOPE';
      v_initial_sealed:=true;
      v_has_more:=true;
      v_next_cursor:=(SELECT closure_cursor_json
        FROM private.banking_pay_workbench_economic_builds WHERE id=p_build_id);
    ELSE
      v_next_cursor:=jsonb_build_object(
        'cursor_kind','SEED_SCOPE_SEAL','cursor_version',1,'build_id',p_build_id,
        'candidate_id',p_candidate_id,
        'captured_candidate_generation',v_build.captured_candidate_generation,
        'captured_source_change_seq',v_build.source_change_seq,
        'page_number',COALESCE((v_cursor->>'page_number')::integer,1)+1,
        'last_timesheet_id',v_last_timesheet_id,'processed_row_count',v_accumulated_count,
        'accumulated_row_count',v_accumulated_count,'rolling_digest',v_rolling_digest,
        'previous_page_digest',md5(v_cursor::text||v_rolling_digest)
      );
      UPDATE private.banking_pay_workbench_economic_builds
      SET scope_cursor_json=v_next_cursor,updated_at_utc=clock_timestamp()
      WHERE id=p_build_id;
    END IF;

    RETURN jsonb_build_object(
      'ok',true,'build_id',p_build_id,'candidate_id',p_candidate_id,
      'cursor_kind',v_next_cursor->>'cursor_kind','cursor_version',1,
      'seed_family','COMPLETE','processed_source_count',v_selected_count,
      'new_scope_row_count',0,'merged_scope_row_count',0,
      'initial_scope_count',v_accumulated_count,'initial_scope_digest',v_rolling_digest,
      'initial_scope_sealed',v_initial_sealed,'has_more',v_has_more,
      'next_cursor_json',v_next_cursor
    );
  END IF;

  IF v_cursor_kind NOT IN ('PREPARE_SCOPE','SCOPE_SELECT') OR v_family_ordinal NOT BETWEEN 1 AND 10 THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_BUILD_CURSOR_INVALID' USING ERRCODE='22023';
  END IF;

  CREATE TEMP TABLE IF NOT EXISTS pg_temp._bpay_wb_seed_page_v1(
    source_key text PRIMARY KEY,timesheet_id uuid NOT NULL,reason text NOT NULL
  ) ON COMMIT DROP;
  TRUNCATE pg_temp._bpay_wb_seed_page_v1;

  INSERT INTO pg_temp._bpay_wb_seed_page_v1(source_key,timesheet_id,reason)
  WITH emissions AS (
    SELECT scope_state.timesheet_id::text AS source_key,scope_state.timesheet_id,'ACTIVE_STATE'::text AS reason
    FROM private.banking_pay_workbench_timesheet_scope_state AS scope_state
    WHERE v_family_ordinal=1 AND scope_state.candidate_id=p_candidate_id
      AND scope_state.economic_state IN ('DIRTY','LIVE')
    UNION ALL
    SELECT source_line.timesheet_id::text||':'||source_line.id::text,source_line.timesheet_id,'CURRENT_SOURCE_OWNER'
    FROM public.banking_pay_workbench_candidate_source_lines AS source_line
    WHERE v_family_ordinal=2 AND source_line.candidate_id=p_candidate_id
      AND source_line.status='CURRENT' AND source_line.timesheet_id IS NOT NULL
    UNION ALL
    SELECT advance_row.linked_timesheet_id::text||':'||advance_row.id::text,
           advance_row.linked_timesheet_id,'OPEN_ADVANCE'
    FROM public.pay_advances AS advance_row
    WHERE v_family_ordinal=3 AND advance_row.candidate_id=p_candidate_id
      AND advance_row.status='ACTIVE' AND advance_row.linked_timesheet_id IS NOT NULL
      AND advance_row.cleared_at_utc IS NULL AND advance_row.written_off_at_utc IS NULL
    UNION ALL
    SELECT component_row.linked_timesheet_id::text||':'||component_row.id::text,
           component_row.linked_timesheet_id,'OPEN_FINANCE_COMPONENT'
    FROM public.pay_finance_case_components AS component_row
    WHERE v_family_ordinal=4 AND component_row.candidate_id=p_candidate_id
      AND component_row.linked_timesheet_id IS NOT NULL AND component_row.closed_at_utc IS NULL
    UNION ALL
    SELECT reservation_row.id::text||':'||COALESCE(component_row.linked_timesheet_id,item_row.timesheet_id)::text,
           COALESCE(component_row.linked_timesheet_id,item_row.timesheet_id),'ACTIVE_RESERVATION'
    FROM public.pay_advance_reservations AS reservation_row
    LEFT JOIN public.pay_finance_case_components AS component_row
      ON component_row.id=reservation_row.finance_component_id
    LEFT JOIN public.pay_batch_items AS item_row ON item_row.id=reservation_row.pay_batch_item_id
    LEFT JOIN public.pay_batch_candidates AS batch_candidate
      ON batch_candidate.id=COALESCE(reservation_row.pay_batch_candidate_id,item_row.pay_batch_candidate_id)
    WHERE v_family_ordinal=5 AND reservation_row.released_at_utc IS NULL
      AND COALESCE(component_row.candidate_id,batch_candidate.candidate_id)=p_candidate_id
      AND COALESCE(component_row.linked_timesheet_id,item_row.timesheet_id) IS NOT NULL
    UNION ALL
    SELECT override_row.timesheet_id::text||':'||override_row.id::text,
           override_row.timesheet_id,'ACTIVE_OVERRIDE'
    FROM public.timesheet_payment_overrides AS override_row
    WHERE v_family_ordinal=6 AND override_row.candidate_id=p_candidate_id
      AND override_row.timesheet_id IS NOT NULL AND override_row.cleared_at_utc IS NULL
      AND override_row.consumed_at_utc IS NULL AND override_row.consumed_by_pay_batch_id IS NULL
    UNION ALL
    SELECT adjustment_row.timesheet_id::text||':'||adjustment_row.id::text,
           adjustment_row.timesheet_id,'UNPAID_ADJUSTMENT'
    FROM public.ts_pay_adjustments AS adjustment_row
    WHERE v_family_ordinal=7 AND adjustment_row.candidate_id=p_candidate_id
      AND adjustment_row.timesheet_id IS NOT NULL AND adjustment_row.paid_at_utc IS NULL
    UNION ALL
    SELECT snooze_row.timesheet_id::text||':'||snooze_row.id::text,
           snooze_row.timesheet_id,'ACTIVE_SNOOZE'
    FROM public.pay_item_snoozes AS snooze_row
    WHERE v_family_ordinal=8 AND snooze_row.candidate_id=p_candidate_id
      AND snooze_row.timesheet_id IS NOT NULL AND snooze_row.cleared_at_utc IS NULL
      AND snooze_row.cancelled_at_utc IS NULL
    UNION ALL
    SELECT target_id::text,target_id,'TARGETED_ID'
    FROM unnest(COALESCE(p_targeted_timesheet_ids,ARRAY[]::uuid[])) AS target_id
    WHERE v_family_ordinal=9 AND target_id IS NOT NULL
    UNION ALL
    SELECT force_id::text,force_id,'FORCED_ID'
    FROM unnest(COALESCE(p_force_include_timesheet_ids,ARRAY[]::uuid[])) AS force_id
    WHERE v_family_ordinal=10 AND force_id IS NOT NULL
  ), valid_emissions AS (
    SELECT DISTINCT ON (emission.source_key) emission.source_key,emission.timesheet_id,emission.reason
    FROM emissions AS emission
    JOIN public.timesheets AS timesheet_row ON timesheet_row.timesheet_id=emission.timesheet_id
    JOIN LATERAL (
      SELECT financial_row.candidate_id,financial_row.client_id
      FROM public.timesheets_financials AS financial_row
      WHERE financial_row.timesheet_id=emission.timesheet_id AND financial_row.is_current
      ORDER BY financial_row.id DESC LIMIT 1
    ) AS ownership_financial ON ownership_financial.candidate_id=p_candidate_id
    WHERE (p_client_filter_single IS NULL OR ownership_financial.client_id=p_client_filter_single)
      AND NOT emission.timesheet_id=ANY(COALESCE(p_exclude_timesheet_ids,ARRAY[]::uuid[]))
      AND (v_last_key IS NULL OR emission.source_key>v_last_key)
    ORDER BY emission.source_key,emission.timesheet_id
  )
  SELECT source_key,timesheet_id,reason FROM valid_emissions
  ORDER BY source_key LIMIT v_limit;

  FOR v_row IN SELECT * FROM pg_temp._bpay_wb_seed_page_v1 ORDER BY source_key
  LOOP
    v_selected_count:=v_selected_count+1;
    v_processed_count:=v_processed_count+1;
    v_last_key:=v_row.source_key;
    SELECT EXISTS(
      SELECT 1 FROM private.banking_pay_workbench_economic_build_scope AS scope_row
      WHERE scope_row.build_id=p_build_id AND scope_row.timesheet_id=v_row.timesheet_id
    ) INTO v_scope_exists;
    INSERT INTO private.banking_pay_workbench_economic_build_scope AS scope_row(
      build_id,timesheet_id,candidate_id,seed_reasons,dependency_reasons,
      captured_dirty_generation,required_fact_families
    ) VALUES (
      p_build_id,v_row.timesheet_id,p_candidate_id,ARRAY[v_row.reason],ARRAY[]::text[],
      v_build.captured_candidate_generation,
      ARRAY['FROZEN_SETTLED_COMPONENT','LIVE_ENTITLEMENT_INPUT','ENTITLEMENT_COMPONENT',
        'PAY_STATE_FALLBACK','PAYEE_BASELINE_INPUT','CANONICAL_INPUT']::text[]
    )
    ON CONFLICT(build_id,timesheet_id) DO UPDATE
    SET seed_reasons=(SELECT array_agg(DISTINCT reason ORDER BY reason)
      FROM unnest(scope_row.seed_reasons||EXCLUDED.seed_reasons) AS reason),
        updated_at_utc=clock_timestamp()
    WHERE scope_row.candidate_id=EXCLUDED.candidate_id
      AND scope_row.captured_dirty_generation=EXCLUDED.captured_dirty_generation;
    IF v_scope_exists THEN v_merged_count:=v_merged_count+1;
    ELSE v_inserted_count:=v_inserted_count+1; END IF;
  END LOOP;

  IF v_selected_count<v_limit THEN
    v_family_ordinal:=v_family_ordinal+1;
    v_last_key:=NULL;
  END IF;

  IF v_family_ordinal>10 THEN
    v_next_cursor:=jsonb_build_object(
      'cursor_kind','SEED_SCOPE_SEAL','cursor_version',1,'build_id',p_build_id,
      'candidate_id',p_candidate_id,
      'captured_candidate_generation',v_build.captured_candidate_generation,
      'captured_source_change_seq',v_build.source_change_seq,
      'page_number',1,'last_timesheet_id',NULL,'processed_row_count',0,
      'accumulated_row_count',0,'rolling_digest',md5('BPAY_SEED_SCOPE_V1'),
      'previous_page_digest',NULL
    );
  ELSE
    v_next_family:=(ARRAY['ACTIVE_STATE','CURRENT_SOURCE_OWNER','OPEN_ADVANCE',
      'OPEN_FINANCE_COMPONENT','ACTIVE_RESERVATION','ACTIVE_OVERRIDE','UNPAID_ADJUSTMENT',
      'ACTIVE_SNOOZE','TARGETED_ID','FORCED_ID'])[v_family_ordinal];
    v_next_cursor:=jsonb_build_object(
      'cursor_kind','PREPARE_SCOPE','cursor_version',1,'build_id',p_build_id,
      'candidate_id',p_candidate_id,'seed_family',v_next_family,
      'seed_family_ordinal',v_family_ordinal,'last_source_key',v_last_key,
      'captured_candidate_generation',v_build.captured_candidate_generation,
      'captured_source_change_seq',v_build.source_change_seq,
      'processed_source_rows',v_processed_count
    );
  END IF;

  UPDATE private.banking_pay_workbench_economic_builds
  SET scope_cursor_json=v_next_cursor,scope_count=scope_count+v_inserted_count,
      updated_at_utc=clock_timestamp()
  WHERE id=p_build_id AND private_stage='PREPARE_SCOPE';

  RETURN jsonb_build_object(
    'ok',true,'build_id',p_build_id,'candidate_id',p_candidate_id,
    'cursor_kind',v_next_cursor->>'cursor_kind','cursor_version',1,
    'seed_family',COALESCE(v_next_cursor->>'seed_family','COMPLETE'),
    'processed_source_count',v_selected_count,'new_scope_row_count',v_inserted_count,
    'merged_scope_row_count',v_merged_count,'initial_scope_count',NULL,
    'initial_scope_digest',NULL,'initial_scope_sealed',false,'has_more',true,
    'next_cursor_json',v_next_cursor
  );
END;
$function$;

ALTER FUNCTION private.pay_workbench_candidate_bounded_scope_v1(uuid,uuid,jsonb,integer,uuid[],uuid[],uuid[],uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_candidate_bounded_scope_v1(uuid,uuid,jsonb,integer,uuid[],uuid[],uuid[],uuid) FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_candidate_bounded_scope_v1(uuid,uuid,jsonb,integer,uuid[],uuid[],uuid[],uuid) TO postgres;
