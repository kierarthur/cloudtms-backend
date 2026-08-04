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
BEGIN
  IF v_relation NOT IN ('pay_advances','pay_finance_case_components','pay_finance_case_events')
     OR v_operation NOT IN ('INSERT','UPDATE','DELETE') THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_EFFECT_RELATION_INVALID' USING ERRCODE='22023';
  END IF;

  v_row:=v_row-'created_at'-'created_at_utc'-'updated_at'-'updated_at_utc'-'event_at_utc';
  IF v_operation='INSERT' THEN
    v_row:=v_row-'id'-'finance_case_id'-'finance_component_id';
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
