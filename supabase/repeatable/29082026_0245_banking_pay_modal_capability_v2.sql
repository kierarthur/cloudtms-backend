-- Repeatable CloudTMS authority: Banking Pay Modal Structure v2 capability.
-- This extends the existing Workbench contract additively. It does not change
-- payment economics, Draft input, provider work, or any mutation owner.

\set ON_ERROR_STOP on

begin;

CREATE OR REPLACE FUNCTION public.pay_workbench_contract_version_get_v1()
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path TO 'public', 'pg_catalog'
AS $function$
DECLARE
  v_projection_contract jsonb :=
    public._pay_workbench_candidate_projection_contract();
  v_selection_carry_table_oid oid :=
    to_regclass(
      'public.banking_pay_workbench_selection_carry_registrations'
    );
  v_canonical_contract_version text;
  v_targeted_family_materialisation_version text;
  v_banking_pay_workbench_v2_available boolean;
BEGIN
  v_canonical_contract_version := CASE
    WHEN v_selection_carry_table_oid IS NOT NULL
      AND to_regprocedure(
        'public.pay_workbench_session_carry_forward_preview_selections_v1(uuid,uuid,jsonb)'
      ) IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM pg_catalog.pg_trigger AS trigger_row
        WHERE trigger_row.tgrelid =
          'public.banking_pay_workbench_preview_rows'::regclass
          AND trigger_row.tgname =
            'trg_banking_pay_preview_selection_carry_apply'
          AND trigger_row.tgenabled <> 'D'
          AND trigger_row.tgisinternal IS FALSE
      )
      THEN v_projection_contract ->> 'canonical_correction_carrier_version'
    ELSE 'BANKING_PAY_CANONICAL_CORRECTION_CARRIER_INCOMPLETE'
  END;

  v_projection_contract := jsonb_set(
    v_projection_contract,
    '{canonical_correction_carrier_version}',
    to_jsonb(v_canonical_contract_version),
    true
  );

  v_targeted_family_materialisation_version := CASE
    WHEN to_regprocedure(
      'public._pay_workbench_refresh_dependency_closure_v1(uuid,uuid[],uuid[],uuid[],integer,integer)'
    ) IS NOT NULL
      THEN v_projection_contract ->> 'targeted_family_materialisation_version'
    ELSE 'BANKING_PAY_TARGETED_FAMILY_MATERIALISATION_INCOMPLETE'
  END;

  v_projection_contract := jsonb_set(
    v_projection_contract,
    '{targeted_family_materialisation_version}',
    to_jsonb(v_targeted_family_materialisation_version),
    true
  );

  v_banking_pay_workbench_v2_available :=
    to_regprocedure('public.pay_workbench_session_get_candidate_summary_page_v1(uuid,jsonb,uuid,text,text,text,integer)') IS NOT NULL
    AND to_regprocedure('public.pay_workbench_session_get_candidate_ready_page_v1(uuid,uuid,jsonb,uuid,text,integer)') IS NOT NULL
    AND to_regprocedure('public.pay_workbench_session_get_action_required_page_v1(uuid,jsonb,uuid,text,text,text,integer,text,text)') IS NOT NULL
    AND to_regprocedure('public.pay_workbench_session_get_action_required_detail_v1(uuid,jsonb,uuid,text,text,integer)') IS NOT NULL
    AND to_regprocedure('public.pay_workbench_session_get_blocked_page_v1(uuid,jsonb,uuid,text,text,text,integer,text)') IS NOT NULL
    AND to_regprocedure('public.pay_workbench_session_get_blocked_detail_v1(uuid,jsonb,uuid,text,text,integer)') IS NOT NULL
    AND to_regprocedure('public.pay_workbench_session_get_selected_ready_timesheets_v1(uuid,uuid,jsonb,uuid,text)') IS NOT NULL
    AND to_regprocedure('public.pay_workbench_session_set_filtered_ready_selection_v1(uuid,jsonb,uuid,text,uuid,text)') IS NOT NULL
    AND to_regprocedure('public.pay_workbench_session_set_candidate_ready_selection_v1(uuid,uuid,jsonb,uuid,text,uuid,text,jsonb)') IS NOT NULL
    AND to_regprocedure('public.pay_workbench_session_set_ready_rows_v1(uuid,uuid,jsonb,uuid,jsonb,boolean,uuid,text,jsonb)') IS NOT NULL
    AND to_regprocedure('public.pay_workbench_session_set_ready_group_v1(uuid,uuid,jsonb,uuid,text,text,boolean,uuid,text,jsonb)') IS NOT NULL;

  RETURN jsonb_build_object(
    'ok', true,
    'contract_version', 'BANKING_PAY_WORKBENCH_DB_V1',
    'canonical_correction_carrier_version',
      v_canonical_contract_version,
    'targeted_family_materialisation_version',
      v_targeted_family_materialisation_version,
    'candidate_projection_contract', v_projection_contract,
    'banking_pay_workbench_v2', jsonb_build_object(
      'available', v_banking_pay_workbench_v2_available,
      'contract_version', 1,
      'surface_contract', 'BANKING_PAY_MODAL_STRUCTURE_V2'
    )
  );
END;
$function$;

ALTER FUNCTION public.pay_workbench_contract_version_get_v1()
  OWNER TO postgres;

REVOKE ALL
ON FUNCTION public.pay_workbench_contract_version_get_v1()
FROM PUBLIC, anon, authenticated;

GRANT EXECUTE
ON FUNCTION public.pay_workbench_contract_version_get_v1()
TO service_role;

commit;
