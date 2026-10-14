CREATE OR REPLACE FUNCTION public.timesheet_standard_delete_preview_v1(p_timesheet_id uuid, p_actor_user_id uuid, p_expected_timesheet_id uuid DEFAULT NULL::uuid, p_expected_row_signature text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_requested public.timesheets%ROWTYPE;
  v_current public.timesheets%ROWTYPE;
  v_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_nhsp_shift_ids uuid[] := ARRAY[]::uuid[];
  v_preserved_source_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_preserved_source_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_primary_contract_week_id uuid := NULL;
  v_history jsonb := '{}'::jsonb;
  v_blockers jsonb := '[]'::jsonb;
  v_decision text := 'BLOCKED';
  v_signature_payload jsonb := '{}'::jsonb;
  v_current_signature text := NULL;
  v_class jsonb := '{}'::jsonb;
  v_chain jsonb := '{}'::jsonb;
  v_unit jsonb := '{}'::jsonb;
  v_is_correction_pair boolean := false;
  v_pair_fingerprint text := NULL;
  v_pair_members jsonb := '[]'::jsonb;
  v_pair_net_hours numeric := 0;
BEGIN
  IF p_timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_ID_REQUIRED';
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'ACTOR_USER_ID_REQUIRED';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM public.tms_users AS actor
    WHERE actor.id = p_actor_user_id
      AND actor.is_active = true
  ) THEN
    RAISE EXCEPTION USING MESSAGE = 'ACTOR_NOT_FOUND_OR_INACTIVE';
  END IF;

  SELECT t.*
    INTO v_requested
  FROM public.timesheets AS t
  WHERE t.timesheet_id = p_timesheet_id;

  IF NOT FOUND THEN
    RETURN jsonb_build_object(
      'ok', true,
      'kind', 'STANDARD_DELETE',
      'decision', 'BLOCKED',
      'eligible', false,
      'blockers', jsonb_build_array(jsonb_build_object('code', 'TARGET_NOT_FOUND')),
      'blocked_reasons', jsonb_build_array(jsonb_build_object('code', 'TARGET_NOT_FOUND')),
      'timesheet_ids', '[]'::jsonb,
      'contract_week_ids', '[]'::jsonb,
      'nhsp_shift_ids', '[]'::jsonb,
      'preserved_source_timesheet_ids', '[]'::jsonb,
      'preserved_source_contract_week_ids', '[]'::jsonb,
      'retention_reasons', '[]'::jsonb,
      'advance', jsonb_build_object(
        'active', false,
        'clearable', false,
        'consumed', false,
        'historical', false
      )
    );
  END IF;

  SELECT t.*
    INTO v_current
  FROM public.timesheets AS t
  WHERE t.booking_id = v_requested.booking_id
    AND t.is_current = true
  ORDER BY t.version DESC NULLS LAST,
           t.updated_at DESC NULLS LAST,
           t.created_at DESC NULLS LAST,
           t.timesheet_id DESC
  LIMIT 1;

  IF NOT FOUND THEN
    RETURN jsonb_build_object(
      'ok', true,
      'kind', 'STANDARD_DELETE',
      'decision', 'BLOCKED',
      'eligible', false,
      'requested_timesheet_id', p_timesheet_id,
      'blockers', jsonb_build_array(jsonb_build_object('code', 'CURRENT_TIMESHEET_NOT_FOUND')),
      'blocked_reasons', jsonb_build_array(jsonb_build_object('code', 'CURRENT_TIMESHEET_NOT_FOUND')),
      'timesheet_ids', '[]'::jsonb,
      'contract_week_ids', '[]'::jsonb,
      'nhsp_shift_ids', '[]'::jsonb,
      'preserved_source_timesheet_ids', '[]'::jsonb,
      'preserved_source_contract_week_ids', '[]'::jsonb,
      'retention_reasons', '[]'::jsonb,
      'advance', jsonb_build_object(
        'active', false,
        'clearable', false,
        'consumed', false,
        'historical', false
      )
    );
  END IF;

  -- Import-authoritative changed-hours members are one exact two-row removal
  -- unit.  Resolve the requested correction generation, never the wider Weekly
  -- parent chain or a neighbouring generation.
  v_class:=public._ctms_import_correction_classify_v1(v_current.timesheet_id);
  IF COALESCE((v_class->>'is_import_authoritative_correction')::boolean,false) THEN
    v_chain:=public.timesheet_correction_chain_scope_v1(v_current.timesheet_id,false,32,100);
    v_unit:=v_chain->'requested_correction_unit';
    IF COALESCE((v_chain->>'valid')::boolean,false) IS TRUE
       AND COALESCE((v_unit->>'valid')::boolean,false) IS TRUE
       AND v_unit->>'correction_shape'='REVERSAL_REPLACEMENT'
       AND COALESCE((v_unit->>'expected_member_count')::integer,0)=2 THEN
      SELECT COALESCE(array_agg(value::uuid ORDER BY value::uuid),ARRAY[]::uuid[])
      INTO v_timesheet_ids
      FROM jsonb_array_elements_text(v_unit->'member_ids') member(value);
      v_is_correction_pair:=cardinality(v_timesheet_ids)=2;
      v_pair_fingerprint:=public._import_review_hash_v1(concat_ws('|',
        'correction-pair-removal-v1',v_chain->>'chain_fingerprint',v_unit->>'correction_id',
        array_to_string(v_timesheet_ids,',')));
    ELSE
      -- Archived pairs are intentionally excluded from some active-chain
      -- projections.  Their exact durable correction identity remains enough
      -- to support an atomic Unarchive preview, but never a guessed role.
      SELECT COALESCE(array_agg(t.timesheet_id ORDER BY t.timesheet_id),ARRAY[]::uuid[])
      INTO v_timesheet_ids
      FROM public.timesheets t
      WHERE t.correction_id=v_current.correction_id AND t.is_current
        AND t.correction_kind IN ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT');
      IF cardinality(v_timesheet_ids)=2
         AND (SELECT count(*) FROM public.timesheets t WHERE t.timesheet_id=ANY(v_timesheet_ids)
                AND t.correction_kind='CHANGED_HOURS_REVERSAL')=1
         AND (SELECT count(*) FROM public.timesheets t WHERE t.timesheet_id=ANY(v_timesheet_ids)
                AND t.correction_kind='CHANGED_HOURS_REPLACEMENT')=1 THEN
        v_is_correction_pair:=true;
        v_unit:=jsonb_build_object('valid',true,'correction_id',v_current.correction_id,
          'correction_shape','REVERSAL_REPLACEMENT','expected_member_count',2,
          'member_ids',to_jsonb(v_timesheet_ids));
        v_pair_fingerprint:=public._import_review_hash_v1(concat_ws('|',
          'correction-pair-removal-v1',v_current.correction_id,array_to_string(v_timesheet_ids,',')));
      ELSE
        v_blockers:=v_blockers||jsonb_build_array(jsonb_build_object(
          'code','CORRECTION_PAIR_MALFORMED','detail',v_chain));
      END IF;
    END IF;
  END IF;
  IF NOT v_is_correction_pair THEN
    -- Ordinary and non-authoritative paths keep the installed single-row unit.
    v_timesheet_ids:=ARRAY[v_current.timesheet_id]::uuid[];
  END IF;

  SELECT COALESCE(array_agg(DISTINCT cw.id ORDER BY cw.id), ARRAY[]::uuid[])
    INTO v_contract_week_ids
  FROM public.contract_weeks AS cw
  WHERE cw.timesheet_id = ANY(v_timesheet_ids);

  SELECT cw.id INTO v_primary_contract_week_id
  FROM public.contract_weeks cw
  WHERE cw.timesheet_id=v_current.timesheet_id
  ORDER BY cw.id LIMIT 1;

  SELECT COALESCE(array_agg(DISTINCT nhsp_shift.id ORDER BY nhsp_shift.id), ARRAY[]::uuid[])
    INTO v_nhsp_shift_ids
  FROM public.nhsp_shifts AS nhsp_shift
  WHERE nhsp_shift.timesheet_id = ANY(v_timesheet_ids);

  SELECT COALESCE(array_agg(DISTINCT source_contract_week.id ORDER BY source_contract_week.id), ARRAY[]::uuid[])
    INTO v_preserved_source_contract_week_ids
  FROM public.contract_weeks AS target_contract_week
  JOIN public.contract_weeks AS source_contract_week
    ON source_contract_week.contract_id = target_contract_week.contract_id
   AND source_contract_week.week_ending_date = target_contract_week.week_ending_date
   AND COALESCE(source_contract_week.additional_seq, 0) = 0
   AND COALESCE(source_contract_week.is_adjustment, false) = false
  WHERE target_contract_week.id = ANY(v_contract_week_ids)
    AND source_contract_week.id <> ALL(v_contract_week_ids);

  SELECT COALESCE(array_agg(DISTINCT preserved_row.preserved_timesheet_id ORDER BY preserved_row.preserved_timesheet_id), ARRAY[]::uuid[])
    INTO v_preserved_source_timesheet_ids
  FROM (
    SELECT source_contract_week.timesheet_id AS preserved_timesheet_id
    FROM public.contract_weeks AS source_contract_week
    WHERE source_contract_week.id = ANY(v_preserved_source_contract_week_ids)
      AND source_contract_week.timesheet_id IS NOT NULL

    UNION

    SELECT target_timesheet.parent_timesheet_id AS preserved_timesheet_id
    FROM public.timesheets AS target_timesheet
    WHERE target_timesheet.timesheet_id = ANY(v_timesheet_ids)
      AND target_timesheet.parent_timesheet_id IS NOT NULL
  ) AS preserved_row
  WHERE preserved_row.preserved_timesheet_id IS NOT NULL
    AND preserved_row.preserved_timesheet_id <> ALL(v_timesheet_ids);

  IF p_expected_timesheet_id IS NULL THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'EXPECTED_TIMESHEET_ID_REQUIRED',
      'current_timesheet_id', v_current.timesheet_id,
      'message', 'The current Timesheet identity is required. Refresh before continuing.'
    ));
  ELSIF p_expected_timesheet_id IS DISTINCT FROM v_current.timesheet_id THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'EXPECTED_TIMESHEET_MISMATCH',
      'expected_timesheet_id', p_expected_timesheet_id,
      'current_timesheet_id', v_current.timesheet_id
    ));
  END IF;

  v_signature_payload := public.timesheet_lifecycle_guard_signature_v1(
    v_current.timesheet_id,
    v_primary_contract_week_id,
    false
  );
  v_current_signature := NULLIF(BTRIM(COALESCE(
    v_signature_payload ->> 'backend_row_signature',
    v_signature_payload ->> 'row_signature',
    v_signature_payload ->> 'signature',
    ''
  )), '');

  IF NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), '') IS NULL THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'EXPECTED_ROW_SIGNATURE_REQUIRED',
      'current_row_signature', v_current_signature,
      'message', 'The current Timesheet lifecycle signature is required. Refresh before continuing.'
    ));
  ELSIF v_current_signature IS DISTINCT FROM BTRIM(p_expected_row_signature) THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'ROW_SIGNATURE_MISMATCH',
      'expected_row_signature', BTRIM(p_expected_row_signature),
      'current_row_signature', v_current_signature
    ));
  END IF;

  IF NOT v_is_correction_pair AND v_current.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum THEN
    IF COALESCE(v_current.is_adjustment, false)
       OR EXISTS (
         SELECT 1
         FROM public.contract_weeks AS cw
         WHERE cw.timesheet_id = v_current.timesheet_id
           AND (COALESCE(cw.is_adjustment, false) OR COALESCE(cw.additional_seq, 0) > 0)
       ) THEN
      v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
        'code', 'WEEKLY_MANUAL_ADJUSTMENT_PREVIEW_REQUIRED'
      ));
    ELSE
      v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
        'code', 'WEEKLY_CHAIN_PREVIEW_REQUIRED'
      ));
    END IF;
  END IF;

  IF NOT v_is_correction_pair AND private.timesheet_correction_cancel_capability_v1(v_current.timesheet_id,p_actor_user_id) is null AND (UPPER(COALESCE(v_current.adjustment_origin, '')) IN ('IMPORT_CORRECTION', 'IMPORT_CANCELLATION')
     OR NULLIF(BTRIM(COALESCE(v_current.correction_kind, '')), '') IS NOT NULL
     OR v_current.correction_id IS NOT NULL) THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'IMPORT_DERIVED_CHILD',
      'message', 'Import-derived children must be handled through the parent-chain removal path.'
    ));
  END IF;

  IF v_is_correction_pair THEN
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
      'timesheet_id',t.timesheet_id,'correction_kind',t.correction_kind,
      'signed_hours',COALESCE(tf.hours_day,0)+COALESCE(tf.hours_night,0)+COALESCE(tf.hours_sat,0)+COALESCE(tf.hours_sun,0)+COALESCE(tf.hours_bh,0),
      'authorised',t.authorised_at_server IS NOT NULL OR tf.authorised_at_utc IS NOT NULL,
      'invoice_linked',EXISTS(SELECT 1 FROM public.invoice_lines il WHERE il.timesheet_id=t.timesheet_id),
      'paid',tf.paid_at_utc IS NOT NULL,'archived',t.archived_at_utc IS NOT NULL)
      ORDER BY CASE t.correction_kind WHEN 'CHANGED_HOURS_REVERSAL' THEN 1 ELSE 2 END),'[]'::jsonb),
      COALESCE(sum(COALESCE(tf.hours_day,0)+COALESCE(tf.hours_night,0)+COALESCE(tf.hours_sat,0)+COALESCE(tf.hours_sun,0)+COALESCE(tf.hours_bh,0)),0)
    INTO v_pair_members,v_pair_net_hours
    FROM public.timesheets t
    LEFT JOIN public.timesheets_financials tf ON tf.timesheet_id=t.timesheet_id AND tf.is_current=true
    WHERE t.timesheet_id=ANY(v_timesheet_ids);

    IF (SELECT count(*) FROM public.timesheets t WHERE t.timesheet_id=ANY(v_timesheet_ids)
          AND t.archived_at_utc IS NOT NULL)=1 THEN
      v_blockers:=v_blockers||jsonb_build_array(jsonb_build_object('code','CORRECTION_PAIR_PARTIAL_ARCHIVE'));
    END IF;
    IF EXISTS(SELECT 1 FROM public.timesheets t LEFT JOIN public.timesheets_financials tf
        ON tf.timesheet_id=t.timesheet_id AND tf.is_current=true
      WHERE t.timesheet_id=ANY(v_timesheet_ids)
        AND (t.authorised_at_server IS NOT NULL OR tf.authorised_at_utc IS NOT NULL)) THEN
      v_blockers:=v_blockers||jsonb_build_array(jsonb_build_object('code','CORRECTION_PAIR_MUST_BE_UNAUTHORISED'));
    END IF;
    IF EXISTS(
      SELECT 1 FROM public.timesheets later
      WHERE later.is_current AND later.archived_at_utc IS NULL
        AND later.adjustment_origin='IMPORT_CORRECTION'
        AND later.parent_timesheet_id=(
          SELECT replacement.timesheet_id FROM public.timesheets replacement
          WHERE replacement.timesheet_id=ANY(v_timesheet_ids)
            AND replacement.correction_kind='CHANGED_HOURS_REPLACEMENT' LIMIT 1)
        AND later.timesheet_id<>ALL(v_timesheet_ids)
    ) THEN
      v_blockers:=v_blockers||jsonb_build_array(jsonb_build_object('code','CORRECTION_PAIR_HAS_LATER_GENERATION'));
    END IF;
  END IF;

  v_history := public.timesheet_removal_financial_history_v1(
    v_timesheet_ids,
    ARRAY[v_current.booking_id]::text[],
    v_contract_week_ids
  );

  v_blockers := v_blockers || COALESCE(v_history -> 'blockers', '[]'::jsonb);

  IF jsonb_array_length(v_blockers) > 0 THEN
    v_decision := 'BLOCKED';
  ELSIF COALESCE((v_history ->> 'archive_required')::boolean, false) THEN
    v_decision := 'ARCHIVE_REQUIRED';
  ELSE
    v_decision := 'PERMANENT_DELETE';
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'kind', 'STANDARD_DELETE',
    'decision', v_decision,
    'eligible', v_decision = 'PERMANENT_DELETE',
    'requested_timesheet_id', p_timesheet_id,
    'current_timesheet_id', v_current.timesheet_id,
    'was_stale', v_current.timesheet_id IS DISTINCT FROM p_timesheet_id,
    'booking_id', v_current.booking_id,
    'timesheet_ids', to_jsonb(v_timesheet_ids),
    'contract_week_ids', to_jsonb(v_contract_week_ids),
    'nhsp_shift_ids', to_jsonb(v_nhsp_shift_ids),
    'preserved_source_timesheet_ids', to_jsonb(v_preserved_source_timesheet_ids),
    'preserved_source_contract_week_ids', to_jsonb(v_preserved_source_contract_week_ids),
    'blockers', v_blockers,
    'blocked_reasons', v_blockers,
    'retention_reasons', COALESCE(v_history -> 'retention_reasons', '[]'::jsonb),
    'advance', COALESCE(v_history -> 'advance', '{}'::jsonb),
    'current_row_signature', v_current_signature,
    'correction_pair',v_is_correction_pair,
    'source_system',CASE WHEN v_is_correction_pair THEN CASE
      WHEN upper(coalesce(
        v_current.candidate_hint_text#>>'{correction_financials_policy_envelope,classification,source_system}',
        v_current.adjustment_origin,'')) like 'NHSP%' THEN 'NHSP'
      ELSE 'HEALTHROSTER' END END,
    'correction_id',CASE WHEN v_is_correction_pair THEN v_unit->>'correction_id' END,
    'pair_timesheet_ids',CASE WHEN v_is_correction_pair THEN to_jsonb(v_timesheet_ids) ELSE '[]'::jsonb END,
    'pair_fingerprint',v_pair_fingerprint,
    'members',CASE WHEN v_is_correction_pair THEN v_pair_members ELSE '[]'::jsonb END,
    'pair_net_position',CASE WHEN v_is_correction_pair THEN jsonb_build_object('hours',v_pair_net_hours) ELSE NULL END,
    'resulting_financial_position',CASE WHEN v_is_correction_pair THEN jsonb_build_object('pair_removed_hours',v_pair_net_hours) ELSE NULL END
  );
END;
$function$;
