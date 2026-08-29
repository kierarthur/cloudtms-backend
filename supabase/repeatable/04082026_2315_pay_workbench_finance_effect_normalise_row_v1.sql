-- Banking Pay bounded-scope V1.2.5: one semantic finance-effect row normaliser
-- shared by capture, execution preflight and AFTER-statement observation.

CREATE OR REPLACE FUNCTION private.pay_workbench_finance_effect_normalise_row_v1(
  p_relation_name text,
  p_operation text,
  p_row_json jsonb,
  p_before_row_json jsonb DEFAULT NULL::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
  v_relation text:=LOWER(NULLIF(BTRIM(COALESCE(p_relation_name,'')),''));
  v_operation text:=UPPER(NULLIF(BTRIM(COALESCE(p_operation,'')),''));
  v_row jsonb:=CASE WHEN jsonb_typeof(COALESCE(p_row_json,'{}'::jsonb))='object'
    THEN COALESCE(p_row_json,'{}'::jsonb) ELSE '{}'::jsonb END;
  v_before jsonb:=CASE WHEN jsonb_typeof(COALESCE(p_before_row_json,'{}'::jsonb))='object'
    THEN COALESCE(p_before_row_json,'{}'::jsonb) ELSE '{}'::jsonb END;
  v_field text;
  v_before_value jsonb;
  v_after_value jsonb;
  v_nested_before jsonb := '{}'::jsonb;
  v_nested_after jsonb := '{}'::jsonb;
BEGIN
  IF v_relation NOT IN ('pay_advances','pay_finance_case_components','pay_finance_case_events')
     OR v_operation NOT IN ('INSERT','UPDATE','DELETE') THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_EFFECT_RELATION_INVALID' USING ERRCODE='22023';
  END IF;

  v_row:=v_row-'created_at'-'created_at_utc'-'updated_at'-'updated_at_utc'-'event_at_utc';

  -- Component sync refreshes this one nested audit timestamp whenever an
  -- existing amount-only resolution remains reusable. Capture and execute run
  -- in separate transactions, so their generated instants must be attested as
  -- the same non-economic transition. Keep every sibling amount, ratio,
  -- fingerprint and resolution field exact.
  IF v_relation='pay_finance_case_components'
     AND v_operation='UPDATE'
     AND jsonb_typeof(v_row->'saved_resolution_result_json')='object'
     AND LOWER(COALESCE(
       v_row#>>'{saved_resolution_result_json,amount_only_resolution_reused}',
       'false'
     )) IN ('true','t','1','yes','y','on')
     AND NULLIF(BTRIM(COALESCE(
       v_row#>>'{saved_resolution_result_json,amount_only_resolution_reused_at_utc}',
       ''
     )), '') IS NOT NULL THEN
    v_row:=jsonb_set(
      v_row,
      ARRAY['saved_resolution_result_json','amount_only_resolution_reused_at_utc'],
      to_jsonb('__GENERATED_NON_NULL__'::text),
      false
    );
  END IF;

  IF v_operation='INSERT' THEN
    v_row:=v_row-'id'-'finance_case_id'-'finance_component_id';

    -- Event rows duplicate their stable parent identities inside the captured
    -- before/after snapshots.  A newly inserted case/component receives a new
    -- physical UUID in capture and execute, while its logical parent identity
    -- is already attested independently by the expected-effect identity map.
    -- Remove only those duplicated generated identities from the event digest;
    -- all economic, classification and lifecycle fields remain exact.
    IF v_relation='pay_finance_case_events' THEN
      IF jsonb_typeof(v_row->'before_json')='object' THEN
        v_row:=jsonb_set(v_row,ARRAY['before_json'],
          (v_row->'before_json')-'id'-'finance_case_id'-'finance_component_id',false);
      END IF;
      IF jsonb_typeof(v_row->'after_json')='object' THEN
        v_row:=jsonb_set(v_row,ARRAY['after_json'],
          (v_row->'after_json')-'id'-'finance_case_id'-'finance_component_id',false);
      END IF;

      -- Closure timestamps are repeated in the immutable event snapshot.  The
      -- capture and execute transactions generate different instants for the
      -- same NULL -> non-NULL lifecycle transition, so attest that transition
      -- semantically.  Existing/manual timestamps, removals, and every other
      -- nested field remain exact and therefore still fail on any difference.
      v_nested_before:=CASE WHEN jsonb_typeof(v_row->'before_json')='object'
        THEN v_row->'before_json' ELSE '{}'::jsonb END;
      v_nested_after:=CASE WHEN jsonb_typeof(v_row->'after_json')='object'
        THEN v_row->'after_json' ELSE '{}'::jsonb END;
      FOR v_field IN
        SELECT field_name FROM unnest(ARRAY[
          'closed_at_utc','cleared_at_utc','written_off_at_utc'
        ]::text[]) field_name
      LOOP
        v_before_value:=v_nested_before->v_field;
        v_after_value:=v_nested_after->v_field;
        IF (v_before_value IS NULL OR v_before_value='null'::jsonb)
           AND v_after_value IS NOT NULL
           AND v_after_value<>'null'::jsonb THEN
          v_nested_after:=jsonb_set(v_nested_after,ARRAY[v_field],
            to_jsonb('__GENERATED_NON_NULL__'::text),true);
        END IF;
      END LOOP;
      IF jsonb_typeof(v_row->'after_json')='object' THEN
        v_row:=jsonb_set(v_row,ARRAY['after_json'],v_nested_after,false);
      END IF;
    END IF;
  END IF;

  FOR v_field IN
    SELECT field_name FROM unnest(CASE v_relation
      WHEN 'pay_advances' THEN ARRAY['cleared_at_utc','written_off_at_utc']::text[]
      WHEN 'pay_finance_case_components' THEN ARRAY['closed_at_utc']::text[]
      ELSE ARRAY[]::text[] END) field_name
  LOOP
    v_before_value:=v_before->v_field;
    v_after_value:=v_row->v_field;
    IF v_before_value IS NOT NULL AND v_before_value<>'null'::jsonb THEN
      IF v_after_value IS NOT NULL AND v_after_value<>'null'::jsonb
         AND v_after_value IS DISTINCT FROM v_before_value THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_EXPECTED_EFFECT_LIFECYCLE_TIMESTAMP_MISMATCH'
          USING ERRCODE='23514';
      END IF;
      -- An existing manual/lifecycle timestamp remains exact authority.
    ELSIF v_after_value IS NOT NULL AND v_after_value<>'null'::jsonb THEN
      v_row:=jsonb_set(v_row,ARRAY[v_field],to_jsonb('__GENERATED_NON_NULL__'::text),true);
    END IF;
  END LOOP;
  RETURN v_row;
END;
$function$;

ALTER FUNCTION private.pay_workbench_finance_effect_normalise_row_v1(text,text,jsonb,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_finance_effect_normalise_row_v1(text,text,jsonb,jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION private.pay_workbench_finance_effect_normalise_row_v1(text,text,jsonb,jsonb) FROM anon;
REVOKE ALL ON FUNCTION private.pay_workbench_finance_effect_normalise_row_v1(text,text,jsonb,jsonb) FROM authenticated;
REVOKE ALL ON FUNCTION private.pay_workbench_finance_effect_normalise_row_v1(text,text,jsonb,jsonb) FROM service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_finance_effect_normalise_row_v1(text,text,jsonb,jsonb) TO postgres;
