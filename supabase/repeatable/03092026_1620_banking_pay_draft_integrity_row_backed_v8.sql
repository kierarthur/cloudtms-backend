-- Row-backed transport source for the established Draft integrity owner.
-- Runtime authority is Miget TEST. The `supabase` directory name is historical.
--
-- The public integrity function below remains the financial and policy owner.
-- These two private helpers only expose the already-frozen selected lines from
-- their V8 normalized rows. Legacy operations retain their exact saved arrays.

CREATE OR REPLACE FUNCTION private.pay_workbench_operation_selected_lines_v8(
  p_operation_id uuid,
  p_candidate_scope_id uuid
)
RETURNS TABLE(value jsonb)
LANGUAGE plpgsql
SECURITY INVOKER
STABLE
PARALLEL UNSAFE
SET search_path TO ''
AS $function$
DECLARE
  v_is_v8_operation boolean := false;
BEGIN
  IF p_operation_id IS NULL OR p_candidate_scope_id IS NULL THEN
    RETURN;
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM private.banking_pay_draft_frozen_certificate_scopes_v8 AS certificate_scope
    WHERE certificate_scope.operation_id = p_operation_id
      AND certificate_scope.freeze_state = 'FROZEN'
  )
  INTO v_is_v8_operation;

  IF v_is_v8_operation THEN
    RETURN QUERY
    SELECT payload.payload_json
    FROM public.banking_pay_operation_candidate_scope AS public_scope
    JOIN private.banking_pay_draft_frozen_candidate_scopes_v8 AS frozen_scope
      ON frozen_scope.operation_id = public_scope.operation_id
     AND frozen_scope.candidate_id = public_scope.candidate_id
     AND frozen_scope.resolved_pay_channel = public_scope.pay_channel
     AND frozen_scope.scope_digest_sha256 = public_scope.scope_hash
     AND frozen_scope.scope_state IN ('FROZEN', 'BATCH_LINKED', 'COMPLETE')
    JOIN private.banking_pay_draft_frozen_candidate_scope_members_v8 AS member
      ON member.operation_id = frozen_scope.operation_id
     AND member.candidate_scope_ordinal = frozen_scope.candidate_scope_ordinal
    JOIN private.banking_pay_draft_frozen_constituent_payloads_v8 AS payload
      ON payload.operation_id = member.operation_id
     AND payload.constituent_ordinal = member.constituent_ordinal
     AND payload.candidate_id = frozen_scope.candidate_id
     AND payload.resolved_pay_channel = frozen_scope.resolved_pay_channel
    WHERE public_scope.operation_id = p_operation_id
      AND public_scope.id = p_candidate_scope_id
    ORDER BY member.constituent_ordinal;
    RETURN;
  END IF;

  RETURN QUERY
  SELECT selected_line.value
  FROM public.banking_pay_operation_candidate_scope AS legacy_scope
  CROSS JOIN LATERAL pg_catalog.jsonb_array_elements(
    CASE
      WHEN pg_catalog.jsonb_typeof(legacy_scope.selected_canonical_preview_lines_json) = 'array'
        THEN legacy_scope.selected_canonical_preview_lines_json
      ELSE '[]'::jsonb
    END
  ) AS selected_line(value)
  WHERE legacy_scope.operation_id = p_operation_id
    AND legacy_scope.id = p_candidate_scope_id;
END;
$function$;

ALTER FUNCTION private.pay_workbench_operation_selected_lines_v8(uuid,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_operation_selected_lines_v8(uuid,uuid)
  FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION private.pay_workbench_operation_selected_line_count_v8(
  p_operation_id uuid,
  p_candidate_scope_id uuid
)
RETURNS integer
LANGUAGE sql
SECURITY INVOKER
STABLE
PARALLEL UNSAFE
SET search_path TO ''
AS $function$
  SELECT pg_catalog.count(*)::integer
  FROM private.pay_workbench_operation_selected_lines_v8(
    p_operation_id,
    p_candidate_scope_id
  ) AS selected_line;
$function$;

ALTER FUNCTION private.pay_workbench_operation_selected_line_count_v8(uuid,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_operation_selected_line_count_v8(uuid,uuid)
  FROM PUBLIC, anon, authenticated, service_role;

-- The exact current public.pay_batch_assert_integrity definition follows.
-- Its only intended source change is replacing reads of the legacy selected
-- JSON array with the adaptive row source above. All validation, equations,
-- result fields, metadata and failure outcomes remain owned by this function.
CREATE OR REPLACE FUNCTION public.pay_batch_assert_integrity(p_pay_batch_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid, p_operation_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_batch_id uuid := p_pay_batch_id;
  v_stage text := NULL;
  v_breakdown_missing_ct integer := 0;
  v_breakdown_bad_ct integer := 0;
  v_breakdown_bad jsonb := '[]'::jsonb;
  v_blocked_candidates jsonb := '[]'::jsonb;
  v_rows_upd_timesheet_overrides_consumed integer := 0;
  v_consumed_timesheet_payment_overrides jsonb := '[]'::jsonb;
  v_rail_provider_default text := NULL;
  v_rail_env_default text := NULL;
  v_need_name_check boolean := false;
  v_requires_payee_map boolean := false;
  v_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_now_utc timestamptz := now();
  v_override_consume_rec record;
  v_operation_mismatch_details jsonb := '{}'::jsonb;
  v_operation_affected_candidate_ids jsonb := '[]'::jsonb;
  v_operation_mismatch_count integer := 0;
  v_operation_active_item_count integer := 0;
  v_operation_scope_count integer := 0;
  v_operation_reservation_unavailable_scope_count integer := 0;
  v_consumed_timesheet_payment_override_ids jsonb := '[]'::jsonb;
  v_expected_advance_override_count integer := 0;
  v_expected_advance_override_ids jsonb := '[]'::jsonb;
  v_advance_override_consumption_mismatch_count integer := 0;
  v_advance_override_consumption_mismatches jsonb := '[]'::jsonb;
  v_integrity_operation public.banking_pay_operations%ROWTYPE;
  v_integrity_batch public.pay_batches%ROWTYPE;
  v_generation_provenance_mismatch_count integer := 0;
  v_generation_provenance_json jsonb := '{}'::jsonb;
BEGIN
  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'pay_batch_id is required';
  END IF;

  IF p_operation_id IS NOT NULL THEN
    SELECT operation_row.*
    INTO v_integrity_operation
    FROM public.banking_pay_operations AS operation_row
    WHERE operation_row.id = p_operation_id;

    SELECT batch_row.*
    INTO v_integrity_batch
    FROM public.pay_batches AS batch_row
    WHERE batch_row.id = v_batch_id;

    IF v_integrity_operation.id IS NULL OR v_integrity_batch.id IS NULL THEN
      RETURN jsonb_build_object(
        'ok', false,
        'pay_batch_id', v_batch_id::text,
        'operation_id', p_operation_id::text,
        'pass', false,
        'code', 'DRAFT_CREATE_OPERATION_BATCH_PROVENANCE_MISMATCH',
        'mismatch_details', jsonb_build_array(jsonb_build_object('check_code', 'OPERATION_OR_BATCH_NOT_FOUND'))
      );
    END IF;

    IF UPPER(BTRIM(COALESCE(v_integrity_operation.operation_type, ''))) = 'DRAFT_CREATE' THEN
      SELECT COUNT(*)::integer
      INTO v_generation_provenance_mismatch_count
      FROM public.banking_pay_operation_candidate_scope AS scope_row
      WHERE scope_row.operation_id = p_operation_id
        AND scope_row.pay_batch_id = v_batch_id
        AND (
          scope_row.workbench_session_id IS DISTINCT FROM v_integrity_operation.workbench_session_id
          OR scope_row.source_session_version IS DISTINCT FROM v_integrity_operation.frozen_source_session_version
          OR scope_row.source_snapshot_run_id IS DISTINCT FROM v_integrity_operation.frozen_source_snapshot_run_id
        );

      v_generation_provenance_json := jsonb_build_object(
        'scope_freeze_status', v_integrity_operation.scope_freeze_status,
        'operation_scope_change_generation', v_integrity_operation.frozen_scope_change_generation,
        'batch_scope_change_generation', v_integrity_batch.source_scope_change_generation,
        'operation_workbench_session_id', v_integrity_operation.workbench_session_id,
        'batch_workbench_session_id', v_integrity_batch.source_workbench_session_id,
        'operation_source_session_version', v_integrity_operation.frozen_source_session_version,
        'batch_source_session_version', v_integrity_batch.source_session_version,
        'operation_source_snapshot_run_id', v_integrity_operation.frozen_source_snapshot_run_id,
        'batch_source_snapshot_run_id', v_integrity_batch.source_snapshot_run_id,
        'policy_x_authority', 'FROZEN_OPERATION_SCOPE'
      );

      IF UPPER(BTRIM(COALESCE(v_integrity_operation.scope_freeze_status, ''))) <> 'FROZEN'
         OR NOT COALESCE(v_integrity_operation.source_scope_seed_complete, false)
         OR v_integrity_operation.frozen_scope_change_generation IS NULL
         OR v_integrity_batch.source_scope_change_generation IS DISTINCT FROM v_integrity_operation.frozen_scope_change_generation
         OR v_integrity_batch.source_workbench_session_id IS DISTINCT FROM v_integrity_operation.workbench_session_id
         OR v_integrity_batch.source_session_version IS DISTINCT FROM v_integrity_operation.frozen_source_session_version
         OR v_integrity_batch.source_snapshot_run_id IS DISTINCT FROM v_integrity_operation.frozen_source_snapshot_run_id
         OR v_generation_provenance_mismatch_count > 0
         OR NOT EXISTS (
           SELECT 1
           FROM public.banking_pay_operation_candidate_scope AS linked_scope
           WHERE linked_scope.operation_id = p_operation_id
             AND linked_scope.pay_batch_id = v_batch_id
         ) THEN
        RETURN jsonb_build_object(
          'ok', false,
          'pay_batch_id', v_batch_id::text,
          'operation_id', p_operation_id::text,
          'pass', false,
          'error', 'DRAFT_INTEGRITY_FAILED',
          'code', 'DRAFT_CREATE_OPERATION_BATCH_PROVENANCE_MISMATCH',
          'mismatch_details', jsonb_build_array(
            jsonb_build_object(
              'check_code', 'DRAFT_CREATE_OPERATION_BATCH_PROVENANCE_MISMATCH',
              'generation_provenance', v_generation_provenance_json,
              'candidate_scope_provenance_mismatch_count', v_generation_provenance_mismatch_count
            )
          ),
          'friendly_error_message', 'Draft integrity failed because frozen operation and batch provenance do not match.'
        );
      END IF;
    END IF;
  END IF;

  IF to_regclass('pg_temp.tmp_pay_build_settings_context') IS NOT NULL THEN
    SELECT ctx.rail_provider_default, ctx.rail_env_default, ctx.need_name_check, ctx.requires_payee_map
    INTO v_rail_provider_default, v_rail_env_default, v_need_name_check, v_requires_payee_map
    FROM pg_temp.tmp_pay_build_settings_context AS ctx
    LIMIT 1;
  END IF;

  IF (v_rail_provider_default IS NULL OR v_rail_env_default IS NULL) AND p_operation_id IS NOT NULL THEN
    SELECT batch_row.rail_provider_snapshot,
           batch_row.rail_env_snapshot,
           (coalesce(settings_row.rail_supports_name_check, false) = true and upper(btrim(coalesce(batch_row.rail_provider_snapshot, ''))) <> 'CSV') AS need_name_check,
           (upper(btrim(coalesce(batch_row.rail_provider_snapshot, ''))) <> 'CSV') AS requires_payee_map
    INTO v_rail_provider_default, v_rail_env_default, v_need_name_check, v_requires_payee_map
    FROM public.pay_batches AS batch_row
    CROSS JOIN LATERAL (
      SELECT settings_defaults_row.rail_supports_name_check
      FROM public.settings_defaults AS settings_defaults_row
      ORDER BY settings_defaults_row.id ASC
      LIMIT 1
    ) AS settings_row
    WHERE batch_row.id = v_batch_id;
  END IF;

  IF v_rail_provider_default IS NULL OR v_rail_env_default IS NULL THEN
    RAISE EXCEPTION 'tmp_pay_build_settings_context temp table is required';
  END IF;

  IF to_regclass('pg_temp.tmp_pay_build_selected_preview_rows') IS NOT NULL THEN
    SELECT COALESCE(array_agg(distinct spr.candidate_id), ARRAY[]::uuid[])
    INTO v_candidate_ids
    FROM pg_temp.tmp_pay_build_selected_preview_rows AS spr;
  ELSIF p_operation_id IS NOT NULL THEN
    SELECT COALESCE(array_agg(distinct scope_row.candidate_id), ARRAY[]::uuid[])
    INTO v_candidate_ids
    FROM public.banking_pay_operation_candidate_scope AS scope_row
    WHERE scope_row.operation_id = p_operation_id
      AND scope_row.pay_batch_id = v_batch_id;
  END IF;

  IF p_operation_id IS NOT NULL THEN
    CREATE TEMPORARY TABLE IF NOT EXISTS pg_temp.tmp_pay_build_candidates_ctx (
      id uuid PRIMARY KEY,
      tms_ref text,
      display_name text,
      pay_method text,
      umbrella_id uuid,
      first_name text,
      last_name text,
      account_holder text,
      sort_code text,
      account_number text,
      bank_details_hash text,
      payee_id text,
      payee_account_id text,
      umbrella_vat_chargeable boolean
    ) ON COMMIT DROP;

    INSERT INTO pg_temp.tmp_pay_build_candidates_ctx (
      id,
      tms_ref,
      display_name,
      pay_method,
      umbrella_id,
      first_name,
      last_name,
      account_holder,
      sort_code,
      account_number,
      bank_details_hash,
      payee_id,
      payee_account_id,
      umbrella_vat_chargeable
    )
    SELECT
      candidate_row.id,
      candidate_row.tms_ref,
      candidate_row.display_name,
      upper(coalesce(candidate_row.pay_method, '')),
      candidate_row.umbrella_id,
      candidate_row.first_name,
      candidate_row.last_name,
      candidate_row.account_holder,
      regexp_replace(coalesce(candidate_row.sort_code, ''), '[^0-9]', '', 'g'),
      nullif(regexp_replace(coalesce(candidate_row.account_number, ''), '[^0-9]', '', 'g'), ''),
      candidate_row.bank_details_hash,
      bank_payee_candidate.payee_id,
      bank_payee_candidate.payee_account_id,
      coalesce(umbrella_row.vat_chargeable, false)
    FROM public.candidates AS candidate_row
    LEFT JOIN public.umbrellas AS umbrella_row
      ON umbrella_row.id = candidate_row.umbrella_id
    LEFT JOIN public.bank_payee_map AS bank_payee_candidate
      ON upper(coalesce(bank_payee_candidate.rail_provider, '')) = upper(btrim(coalesce(v_rail_provider_default, '')))
     AND upper(coalesce(bank_payee_candidate.rail_env, '')) = upper(btrim(coalesce(v_rail_env_default, '')))
     AND upper(coalesce(bank_payee_candidate.entity_kind, '')) = 'CANDIDATE'
     AND bank_payee_candidate.entity_id = candidate_row.id
     AND bank_payee_candidate.bank_details_hash = candidate_row.bank_details_hash
    WHERE candidate_row.id = ANY(v_candidate_ids)
    ON CONFLICT (id) DO NOTHING;

    CREATE TEMPORARY TABLE IF NOT EXISTS pg_temp.tmp_pay_build_umbrellas_ctx (
      id uuid PRIMARY KEY,
      name text,
      vat_chargeable boolean,
      sort_code text,
      account_number text,
      bank_details_hash text,
      payee_id text,
      payee_account_id text
    ) ON COMMIT DROP;

    INSERT INTO pg_temp.tmp_pay_build_umbrellas_ctx (
      id,
      name,
      vat_chargeable,
      sort_code,
      account_number,
      bank_details_hash,
      payee_id,
      payee_account_id
    )
    SELECT
      umbrella_row.id,
      umbrella_row.name,
      coalesce(umbrella_row.vat_chargeable, false),
      regexp_replace(coalesce(umbrella_row.sort_code, ''), '[^0-9]', '', 'g'),
      nullif(regexp_replace(coalesce(umbrella_row.account_number, ''), '[^0-9]', '', 'g'), ''),
      umbrella_row.bank_details_hash,
      bank_payee_umbrella.payee_id,
      bank_payee_umbrella.payee_account_id
    FROM public.umbrellas AS umbrella_row
    JOIN (
      SELECT DISTINCT candidate_ctx.umbrella_id
      FROM pg_temp.tmp_pay_build_candidates_ctx AS candidate_ctx
      WHERE candidate_ctx.umbrella_id IS NOT NULL
    ) AS required_umbrella
      ON required_umbrella.umbrella_id = umbrella_row.id
    LEFT JOIN public.bank_payee_map AS bank_payee_umbrella
      ON upper(coalesce(bank_payee_umbrella.rail_provider, '')) = upper(btrim(coalesce(v_rail_provider_default, '')))
     AND upper(coalesce(bank_payee_umbrella.rail_env, '')) = upper(btrim(coalesce(v_rail_env_default, '')))
     AND upper(coalesce(bank_payee_umbrella.entity_kind, '')) = 'UMBRELLA'
     AND bank_payee_umbrella.entity_id = umbrella_row.id
     AND bank_payee_umbrella.bank_details_hash = umbrella_row.bank_details_hash
    ON CONFLICT (id) DO NOTHING;
  END IF;


v_stage := 'STAGE_21_BREAKDOWN_INTEGRITY_MISSING';

  -- ✅ Integrity checks: every active (non-voided) item must have ≥1 breakdown row; sums must match exactly
  select count(*)
  into v_breakdown_missing_ct
  from public.pay_batch_items pbi_m
  join public.pay_batch_candidates pbc_m
    on pbc_m.id = pbi_m.pay_batch_candidate_id
  where pbc_m.pay_batch_id = v_batch_id
    and (p_operation_id is null or pbc_m.candidate_id = any(v_candidate_ids))
    and coalesce(pbi_m.is_voided, false) = false
    and not exists (
      select 1
      from public.pay_batch_item_breakdowns pbib_m
      where pbib_m.pay_batch_item_id = pbi_m.id
      limit 1
    );

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:STAGE_21_BREAKDOWN_MISSING_COUNT',
      jsonb_build_object(
        'stage', v_stage,
        'pay_batch_id', v_batch_id::text,
        'breakdown_missing_ct', coalesce(v_breakdown_missing_ct,0)
      ),
      'pay_batches',
      v_batch_id::text,
      null, null, null, null, null
    );
  exception when others then null; end;

  if coalesce(v_breakdown_missing_ct,0) > 0 then
    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_CREATE_DRAFT_BATCH:ERROR_BREAKDOWN_MISSING',
        jsonb_build_object(
          'stage', v_stage,
          'pay_batch_id', v_batch_id::text,
          'breakdown_missing_ct', coalesce(v_breakdown_missing_ct,0),
          'error', 'PAY_BATCH_BREAKDOWN_MISSING'
        ),
        'pay_batches',
        v_batch_id::text,
        null, null, null, null, null
      );
    exception when others then null; end;

    IF p_operation_id IS NOT NULL THEN
      RETURN jsonb_build_object(
        'ok', false,
        'pay_batch_id', v_batch_id::text,
        'operation_id', p_operation_id::text,
        'pass', false,
        'mismatch_details', jsonb_build_array(jsonb_build_object(
          'check_code', 'PAY_BATCH_BREAKDOWN_MISSING',
          'breakdown_missing_ct', coalesce(v_breakdown_missing_ct, 0)
        )),
        'affected_candidate_ids', coalesce(to_jsonb(v_candidate_ids), '[]'::jsonb),
        'friendly_error_message', 'Draft integrity failed because one or more active batch items have no breakdown rows.',
        'breakdown_missing_ct', coalesce(v_breakdown_missing_ct, 0),
        'breakdown_bad_ct', coalesce(v_breakdown_bad_ct, 0),
        'blocked_count', jsonb_array_length(coalesce(v_blocked_candidates, '[]'::jsonb)),
        'consumed_timesheet_override_count', coalesce(v_rows_upd_timesheet_overrides_consumed, 0),
        'consumed_timesheet_override_ids', coalesce(v_consumed_timesheet_payment_override_ids, '[]'::jsonb)
      );
    END IF;

    raise exception 'PAY_BATCH_BREAKDOWN_MISSING: % items have no breakdown rows', v_breakdown_missing_ct;
  end if;

  v_stage := 'STAGE_22_BREAKDOWN_INTEGRITY_SUM_MATCH';

  with sums as (
    select
      pbi_s.id as pay_batch_item_id,
      round(pbi_s.amount_ex_vat,2) as item_ex,
      round(pbi_s.amount_vat,2) as item_vat,
      round(pbi_s.amount_inc_vat,2) as item_inc,
      round(coalesce(sum(pbib_s.amount_ex_vat),0),2) as sum_ex,
      round(coalesce(sum(pbib_s.amount_vat),0),2) as sum_vat,
      round(coalesce(sum(pbib_s.amount_inc_vat),0),2) as sum_inc
    from public.pay_batch_items pbi_s
    join public.pay_batch_candidates pbc_s
      on pbc_s.id = pbi_s.pay_batch_candidate_id
    left join public.pay_batch_item_breakdowns pbib_s
      on pbib_s.pay_batch_item_id = pbi_s.id
    where pbc_s.pay_batch_id = v_batch_id
      and (p_operation_id is null or pbc_s.candidate_id = any(v_candidate_ids))
      and coalesce(pbi_s.is_voided, false) = false
    group by pbi_s.id, pbi_s.amount_ex_vat, pbi_s.amount_vat, pbi_s.amount_inc_vat
  ),
  bad as (
    select
      s.pay_batch_item_id,
      s.item_ex, s.sum_ex,
      s.item_vat, s.sum_vat,
      s.item_inc, s.sum_inc
    from sums s
    where s.item_ex <> s.sum_ex
       or s.item_vat <> s.sum_vat
       or s.item_inc <> s.sum_inc
  )
  select
    count(*),
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'pay_batch_item_id', b.pay_batch_item_id::text,
          'item_ex', b.item_ex, 'sum_ex', b.sum_ex,
          'item_vat', b.item_vat, 'sum_vat', b.sum_vat,
          'item_inc', b.item_inc, 'sum_inc', b.sum_inc
        )
        order by b.pay_batch_item_id::text
      ),
      '[]'::jsonb
    )
  into v_breakdown_bad_ct, v_breakdown_bad
  from bad b;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:STAGE_22_BREAKDOWN_SUMS_RESULT',
      jsonb_build_object(
        'stage', v_stage,
        'pay_batch_id', v_batch_id::text,
        'breakdown_bad_ct', coalesce(v_breakdown_bad_ct,0),
        'breakdown_bad_sample', (
          case
            when jsonb_typeof(v_breakdown_bad) = 'array' then (
              select coalesce(jsonb_agg(x order by (x->>'pay_batch_item_id')), '[]'::jsonb)
              from (
                select e as x
                from jsonb_array_elements(v_breakdown_bad) e
                limit 50
              ) s
            )
            else v_breakdown_bad
          end
        )
      ),
      'pay_batches',
      v_batch_id::text,
      null, null, null, null, null
    );
  exception when others then null; end;

  if coalesce(v_breakdown_bad_ct,0) > 0 then
    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_CREATE_DRAFT_BATCH:ERROR_BREAKDOWN_MISMATCH',
        jsonb_build_object(
          'stage', v_stage,
          'pay_batch_id', v_batch_id::text,
          'breakdown_bad_ct', coalesce(v_breakdown_bad_ct,0),
          'breakdown_bad', v_breakdown_bad,
          'error', 'PAY_BATCH_BREAKDOWN_MISMATCH'
        ),
        'pay_batches',
        v_batch_id::text,
        null, null, null, null, null
      );
    exception when others then null; end;

    IF p_operation_id IS NOT NULL THEN
      RETURN jsonb_build_object(
        'ok', false,
        'pay_batch_id', v_batch_id::text,
        'operation_id', p_operation_id::text,
        'pass', false,
        'mismatch_details', coalesce(v_breakdown_bad, '[]'::jsonb),
        'affected_candidate_ids', coalesce(to_jsonb(v_candidate_ids), '[]'::jsonb),
        'friendly_error_message', 'Draft integrity failed because one or more item breakdown totals do not match their batch item totals.',
        'breakdown_missing_ct', coalesce(v_breakdown_missing_ct, 0),
        'breakdown_bad_ct', coalesce(v_breakdown_bad_ct, 0),
        'blocked_count', jsonb_array_length(coalesce(v_blocked_candidates, '[]'::jsonb)),
        'consumed_timesheet_override_count', coalesce(v_rows_upd_timesheet_overrides_consumed, 0),
        'consumed_timesheet_override_ids', coalesce(v_consumed_timesheet_payment_override_ids, '[]'::jsonb)
      );
    END IF;

    raise exception 'PAY_BATCH_BREAKDOWN_MISMATCH %', v_breakdown_bad::text;
  end if;

  v_stage := 'STAGE_23_FINAL_SAFETY_ASSERT_BLOCKERS';

  with destination_items as (
    select
      pbc.candidate_id,
      pbi.id as pay_batch_item_id,
      pbi.item_type,
      upper(coalesce(pbi.pay_channel::text,'')) as pay_channel,
      upper(coalesce(nullif(btrim(coalesce(pbi.payout_instruction_snapshot_json->>'routing_kind','')), ''), '')) as routing_kind,
      pbi.payout_instruction_snapshot_json,
      pbi.umbrella_id as item_umbrella_id,
      c.umbrella_id as candidate_umbrella_id,
      c.bank_details_hash as candidate_bank_hash,
      u.bank_details_hash as umbrella_bank_hash,
      nullif(btrim(coalesce(pbi.payout_instruction_snapshot_json->>'bank_details_hash','')), '') as snapshot_bank_hash
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc
      on pbc.id = pbi.pay_batch_candidate_id
    join pg_temp.tmp_pay_build_candidates_ctx c
      on c.id = pbc.candidate_id
    left join pg_temp.tmp_pay_build_umbrellas_ctx u
      on u.id = coalesce(pbi.umbrella_id, c.umbrella_id)
    where pbc.pay_batch_id = v_batch_id
      and coalesce(pbi.is_voided, false) = false
      and pbi.item_type <> 'DEBT_CREATED'
      and pbc.candidate_id = any(v_candidate_ids)
  ),
  destinations as (
    select distinct
      di.candidate_id,
      case
        when di.routing_kind = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT' then 'CANDIDATE'
        when di.pay_channel = 'PAYE' then 'CANDIDATE'
        when di.routing_kind = 'UMBRELLA_COMPANY' then 'UMBRELLA'
        when di.pay_channel = 'UMBRELLA' then 'UMBRELLA'
        else 'CANDIDATE'
      end as entity_kind,
      case
        when di.routing_kind = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT' then di.candidate_id
        when di.pay_channel = 'PAYE' then di.candidate_id
        when di.routing_kind = 'UMBRELLA_COMPANY' then coalesce(di.item_umbrella_id, di.candidate_umbrella_id)
        when di.pay_channel = 'UMBRELLA' then coalesce(di.item_umbrella_id, di.candidate_umbrella_id)
        else di.candidate_id
      end as entity_id,
      case
        when di.routing_kind = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT' then di.snapshot_bank_hash
        when di.pay_channel = 'PAYE' then di.candidate_bank_hash
        when di.routing_kind = 'UMBRELLA_COMPANY' then di.umbrella_bank_hash
        when di.pay_channel = 'UMBRELLA' then di.umbrella_bank_hash
        else di.candidate_bank_hash
      end as bank_hash
    from destination_items di
  ),
  destination_blockers as (

    select
      d.candidate_id,
      d.entity_kind,
      d.entity_id,
      d.bank_hash,
      (
        (case
           when d.entity_id is null or d.bank_hash is null or btrim(d.bank_hash) = '' then jsonb_build_array('BLOCKED_BANK_DETAILS')
           else '[]'::jsonb
         end)
        ||
        (case
           when d.entity_id is not null
            and d.bank_hash is not null and btrim(d.bank_hash) <> ''
            and v_need_name_check = true
            and not exists (
              select 1
              from public.bank_name_checks bnc
              where upper(coalesce(bnc.rail_provider,'')) = upper(btrim(coalesce(v_rail_provider_default,'')))
                and upper(coalesce(bnc.rail_env,'')) = upper(btrim(coalesce(v_rail_env_default,'')))
                and upper(coalesce(bnc.entity_kind,'')) = d.entity_kind
                and bnc.entity_id = d.entity_id
                and bnc.bank_details_hash = d.bank_hash
                and (
                  upper(coalesce(bnc.status,'')) = 'PASS'
                  or (
                    bnc.override_reason is not null
                    and bnc.override_hash is not null
                    and bnc.override_hash = d.bank_hash
                  )
                )
              limit 1
            )
           then jsonb_build_array('BLOCKED_NAME_CHECK')
           else '[]'::jsonb
         end)
        ||
        (case
           when d.entity_id is not null
            and d.bank_hash is not null and btrim(d.bank_hash) <> ''
            and v_requires_payee_map = true
            and not exists (
              select 1
              from public.bank_payee_map bpm
              where upper(coalesce(bpm.rail_provider,'')) = upper(btrim(coalesce(v_rail_provider_default,'')))
                and upper(coalesce(bpm.rail_env,'')) = upper(btrim(coalesce(v_rail_env_default,'')))
                and upper(coalesce(bpm.entity_kind,'')) = d.entity_kind
                and bpm.entity_id = d.entity_id
                and bpm.bank_details_hash = d.bank_hash
              limit 1
            )
           then jsonb_build_array('BLOCKED_NO_PAYEE_MAP')
           else '[]'::jsonb
         end)
      ) as blockers
    from destinations d
  ),
  bad as (
    select
      db.candidate_id,
      db.blockers
    from destination_blockers db
    where jsonb_array_length(db.blockers) > 0
  )
  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'candidate_id', b.candidate_id::text,
        'blockers', b.blockers
      )
      order by b.candidate_id::text
    ),
    '[]'::jsonb
  )
  into v_blocked_candidates
  from bad b;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:STAGE_23_BLOCKER_ASSERT_RESULT',
      jsonb_build_object(
        'stage', v_stage,
        'pay_batch_id', v_batch_id::text,
        'blocked_candidates', (
          case
            when jsonb_typeof(v_blocked_candidates) = 'array' then (
              select coalesce(jsonb_agg(x order by (x->>'candidate_id')), '[]'::jsonb)
              from (
                select e as x
                from jsonb_array_elements(v_blocked_candidates) e
                limit 50
              ) s
            )
            else v_blocked_candidates
          end
        ),
        'blocked_count', jsonb_array_length(v_blocked_candidates)
      ),
      'pay_batches',
      v_batch_id::text,
      null, null, null, null, null
    );
  exception when others then null; end;

  if jsonb_array_length(v_blocked_candidates) > 0 then
    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_CREATE_DRAFT_BATCH:ERROR_DRAFT_CONTAINS_BLOCKED_ITEMS',
        jsonb_build_object(
          'stage', v_stage,
          'pay_batch_id', v_batch_id::text,
          'blocked_candidates', v_blocked_candidates,
          'error', 'DRAFT_CONTAINS_BLOCKED_ITEMS'
        ),
        'pay_batches',
        v_batch_id::text,
        null, null, null, null, null
      );
    exception when others then null; end;

    IF p_operation_id IS NOT NULL THEN
      RETURN jsonb_build_object(
        'ok', false,
        'pay_batch_id', v_batch_id::text,
        'operation_id', p_operation_id::text,
        'pass', false,
        'mismatch_details', coalesce(v_blocked_candidates, '[]'::jsonb),
        'affected_candidate_ids', coalesce((
          SELECT jsonb_agg(DISTINCT blocked_candidate.value->>'candidate_id')
          FROM jsonb_array_elements(coalesce(v_blocked_candidates, '[]'::jsonb)) AS blocked_candidate(value)
          WHERE NULLIF(blocked_candidate.value->>'candidate_id', '') IS NOT NULL
        ), '[]'::jsonb),
        'friendly_error_message', 'Draft integrity failed because the draft contains blocked payment destinations.',
        'breakdown_missing_ct', coalesce(v_breakdown_missing_ct, 0),
        'breakdown_bad_ct', coalesce(v_breakdown_bad_ct, 0),
        'blocked_count', jsonb_array_length(coalesce(v_blocked_candidates, '[]'::jsonb)),
        'consumed_timesheet_override_count', coalesce(v_rows_upd_timesheet_overrides_consumed, 0),
        'consumed_timesheet_override_ids', coalesce(v_consumed_timesheet_payment_override_ids, '[]'::jsonb)
      );
    END IF;

    raise exception 'DRAFT_CONTAINS_BLOCKED_ITEMS %', v_blocked_candidates::text;
  end if;

  v_stage := 'STAGE_23B_CONSUME_TIMESHEET_PAYMENT_OVERRIDES';

  with consumed as (
    update public.timesheet_payment_overrides tpo
    set
      consumed_by_pay_batch_id = v_batch_id,
      consumed_at_utc = v_now_utc
    where tpo.cleared_at_utc is null
      and tpo.consumed_by_pay_batch_id is null
      and tpo.consumed_at_utc is null
      and upper(coalesce(tpo.override_type,'')) = 'ADVANCE_THIS_PAYMENT'
      and tpo.id in (
        select tpo_pick.id
        from (
          select distinct on (tpo2.timesheet_id)
            tpo2.id,
            tpo2.timesheet_id
          from public.timesheet_payment_overrides tpo2
          where tpo2.cleared_at_utc is null
            and tpo2.consumed_by_pay_batch_id is null
            and tpo2.consumed_at_utc is null
            and upper(coalesce(tpo2.override_type,'')) = 'ADVANCE_THIS_PAYMENT'
            and exists (
              select 1
              from public.pay_batch_items pbi
              join public.pay_batch_candidates pbc
                on pbc.id = pbi.pay_batch_candidate_id
              where pbc.pay_batch_id = v_batch_id
                and (p_operation_id is null or pbc.candidate_id = any(v_candidate_ids))
                and pbi.timesheet_id = tpo2.timesheet_id
                and coalesce(pbi.is_voided, false) = false
                and pbi.item_type in ('SEGMENT_DELTA','EXPENSE_DELTA','ADJUSTMENT_DELTA','MILEAGE_DELTA')
            )
          order by tpo2.timesheet_id, tpo2.created_at_utc desc, tpo2.id desc
        ) tpo_pick
      )
    returning
      tpo.id,
      tpo.timesheet_id,
      tpo.candidate_id,
      tpo.override_type,
      tpo.reason,
      tpo.created_at_utc,
      tpo.created_by_user_id,
      tpo.consumed_by_pay_batch_id,
      tpo.consumed_at_utc
  )
  select
    count(*)::int,
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'override_id', consumed.id::text,
          'timesheet_id', consumed.timesheet_id::text,
          'candidate_id', case when consumed.candidate_id is null then null else consumed.candidate_id::text end,
          'override_type', consumed.override_type,
          'reason', consumed.reason,
          'created_at_utc', consumed.created_at_utc,
          'created_by_user_id', case when consumed.created_by_user_id is null then null else consumed.created_by_user_id::text end,
          'consumed_by_pay_batch_id', case when consumed.consumed_by_pay_batch_id is null then null else consumed.consumed_by_pay_batch_id::text end,
          'consumed_at_utc', consumed.consumed_at_utc
        )
        order by consumed.timesheet_id::text, consumed.id::text
      ),
      '[]'::jsonb
    ),
    coalesce(
      jsonb_agg(to_jsonb(consumed.id::text) order by consumed.timesheet_id::text, consumed.id::text),
      '[]'::jsonb
    )
  into
    v_rows_upd_timesheet_overrides_consumed,
    v_consumed_timesheet_payment_overrides,
    v_consumed_timesheet_payment_override_ids
  from consumed;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:STAGE_23B_TIMESHEET_OVERRIDES_CONSUMED',
      jsonb_build_object(
        'stage', v_stage,
        'pay_batch_id', v_batch_id::text,
        'consumed_timesheet_payment_override_count', coalesce(v_rows_upd_timesheet_overrides_consumed, 0),
        'consumed_timesheet_payment_override_ids', coalesce(v_consumed_timesheet_payment_override_ids, '[]'::jsonb),
        'consumed_timesheet_payment_overrides', v_consumed_timesheet_payment_overrides
      ),
      'pay_batches',
      v_batch_id::text,
      null, null, null, null, null
    );
  exception when others then null; end;

  if coalesce(v_rows_upd_timesheet_overrides_consumed, 0) > 0 then
    for v_override_consume_rec in
      select
        x.override_id::uuid as override_id,
        x.timesheet_id::uuid as timesheet_id,
        case when x.candidate_id is null or btrim(x.candidate_id) = '' then null else x.candidate_id::uuid end as candidate_id,
        x.override_type,
        x.reason,
        case when x.created_by_user_id is null or btrim(x.created_by_user_id) = '' then null else x.created_by_user_id::uuid end as created_by_user_id,
        x.created_at_utc::timestamptz as created_at_utc,
        case when x.consumed_by_pay_batch_id is null or btrim(x.consumed_by_pay_batch_id) = '' then null else x.consumed_by_pay_batch_id::uuid end as consumed_by_pay_batch_id,
        x.consumed_at_utc::timestamptz as consumed_at_utc
      from jsonb_to_recordset(v_consumed_timesheet_payment_overrides) as x(
        override_id text,
        timesheet_id text,
        candidate_id text,
        override_type text,
        reason text,
        created_at_utc text,
        created_by_user_id text,
        consumed_by_pay_batch_id text,
        consumed_at_utc text
      )
    loop
      begin
        perform public._audit_insert(
          'timesheets',
          v_override_consume_rec.timesheet_id::text,
          'TIMESHEET_PAYMENT_OVERRIDE_CONSUMED',
          jsonb_build_object(
            'override_id', v_override_consume_rec.override_id::text,
            'timesheet_id', v_override_consume_rec.timesheet_id::text,
            'candidate_id', case when v_override_consume_rec.candidate_id is null then null else v_override_consume_rec.candidate_id::text end,
            'override_type', v_override_consume_rec.override_type,
            'reason', v_override_consume_rec.reason,
            'created_at_utc', v_override_consume_rec.created_at_utc,
            'created_by_user_id', case when v_override_consume_rec.created_by_user_id is null then null else v_override_consume_rec.created_by_user_id::text end
          ),
          jsonb_build_object(
            'override_id', v_override_consume_rec.override_id::text,
            'timesheet_id', v_override_consume_rec.timesheet_id::text,
            'candidate_id', case when v_override_consume_rec.candidate_id is null then null else v_override_consume_rec.candidate_id::text end,
            'consumed_by_pay_batch_id', case when v_override_consume_rec.consumed_by_pay_batch_id is null then null else v_override_consume_rec.consumed_by_pay_batch_id::text end,
            'consumed_at_utc', v_override_consume_rec.consumed_at_utc,
            'pay_batch_id', v_batch_id::text
          ),
          'ADVANCE_THIS_PAYMENT',
          p_actor_user_id
        );
      exception when others then
        null;
      end;
    end loop;
  end if;

  


  v_stage := 'STAGE_23C_ASSERT_ADVANCE_OVERRIDE_CONSUMPTION';

  IF p_operation_id IS NOT NULL THEN
    WITH selected_advanced_lines AS (
      SELECT
        scope_row.id AS candidate_scope_id,
        scope_row.candidate_id,
        scope_row.pay_channel,
        COALESCE(
          NULLIF(BTRIM(COALESCE(line_element.value->>'preview_row_id', '')), ''),
          NULLIF(BTRIM(COALESCE(line_element.value->>'line_id', '')), ''),
          NULLIF(BTRIM(COALESCE(line_element.value->>'row_id', '')), ''),
          NULLIF(BTRIM(COALESCE(line_element.value->>'id', '')), '')
        ) AS preview_row_id,
        CASE
          WHEN COALESCE(line_element.value->>'timesheet_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            THEN (line_element.value->>'timesheet_id')::uuid
          ELSE NULL::uuid
        END AS timesheet_id,
        CASE
          WHEN COALESCE(line_element.value->>'advanced_override_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            THEN (line_element.value->>'advanced_override_id')::uuid
          ELSE NULL::uuid
        END AS advanced_override_id,
        NULLIF(BTRIM(COALESCE(line_element.value->>'advanced_override_id', '')), '') AS raw_advanced_override_id,
        lower(coalesce(line_element.value->>'is_advanced', 'false')) IN ('true','t','1','yes','y') AS is_advanced
      FROM public.banking_pay_operation_candidate_scope AS scope_row
      CROSS JOIN LATERAL private.pay_workbench_operation_selected_lines_v8(p_operation_id, scope_row.id) AS line_element(value)
      WHERE scope_row.operation_id = p_operation_id
        AND scope_row.pay_batch_id = v_batch_id
        AND jsonb_typeof(line_element.value) = 'object'
        AND (
          lower(coalesce(line_element.value->>'is_advanced', 'false')) IN ('true','t','1','yes','y')
          OR NULLIF(BTRIM(COALESCE(line_element.value->>'advanced_override_id', '')), '') IS NOT NULL
        )
    ), expected_advanced_overrides AS (
      SELECT DISTINCT
        selected_advanced_lines.candidate_scope_id,
        selected_advanced_lines.candidate_id,
        selected_advanced_lines.pay_channel,
        selected_advanced_lines.preview_row_id,
        selected_advanced_lines.timesheet_id,
        selected_advanced_lines.advanced_override_id,
        selected_advanced_lines.raw_advanced_override_id
      FROM selected_advanced_lines
      WHERE selected_advanced_lines.timesheet_id IS NOT NULL
        AND EXISTS (
          SELECT 1
          FROM public.pay_batch_items AS entitlement_item
          JOIN public.pay_batch_candidates AS entitlement_candidate
            ON entitlement_candidate.id = entitlement_item.pay_batch_candidate_id
          WHERE entitlement_candidate.pay_batch_id = v_batch_id
            AND entitlement_candidate.candidate_id = selected_advanced_lines.candidate_id
            AND entitlement_item.timesheet_id = selected_advanced_lines.timesheet_id
            AND coalesce(entitlement_item.is_voided, false) = false
            AND entitlement_item.item_type IN ('SEGMENT_DELTA','EXPENSE_DELTA','ADJUSTMENT_DELTA','MILEAGE_DELTA')
            AND (
              entitlement_item.operation_source_key IS NULL
              OR entitlement_item.operation_source_key LIKE (p_operation_id::text || ':%')
              OR EXISTS (
                SELECT 1
                FROM public.banking_pay_operation_candidate_allocation_rows AS allocation_row
                WHERE allocation_row.operation_id = p_operation_id
                  AND allocation_row.pay_batch_id = v_batch_id
                  AND allocation_row.pay_batch_item_id = entitlement_item.id
              )
            )
          LIMIT 1
        )
    ), matched_advanced_overrides AS (
      SELECT
        expected_advanced_overrides.candidate_scope_id,
        expected_advanced_overrides.candidate_id,
        expected_advanced_overrides.pay_channel,
        expected_advanced_overrides.preview_row_id,
        expected_advanced_overrides.timesheet_id,
        expected_advanced_overrides.advanced_override_id,
        expected_advanced_overrides.raw_advanced_override_id,
        COUNT(matching_override.id)::integer AS matching_consumed_count
      FROM expected_advanced_overrides
      LEFT JOIN public.timesheet_payment_overrides AS matching_override
        ON matching_override.id = expected_advanced_overrides.advanced_override_id
       AND matching_override.timesheet_id = expected_advanced_overrides.timesheet_id
       AND matching_override.candidate_id = expected_advanced_overrides.candidate_id
       AND upper(coalesce(matching_override.override_type, '')) = 'ADVANCE_THIS_PAYMENT'
       AND matching_override.cleared_at_utc IS NULL
       AND matching_override.consumed_by_pay_batch_id = v_batch_id
       AND matching_override.consumed_at_utc IS NOT NULL
      GROUP BY
        expected_advanced_overrides.candidate_scope_id,
        expected_advanced_overrides.candidate_id,
        expected_advanced_overrides.pay_channel,
        expected_advanced_overrides.preview_row_id,
        expected_advanced_overrides.timesheet_id,
        expected_advanced_overrides.advanced_override_id,
        expected_advanced_overrides.raw_advanced_override_id
    ), mismatch_rows AS (
      SELECT
        matched_advanced_overrides.*,
        CASE
          WHEN matched_advanced_overrides.advanced_override_id IS NULL THEN 'ADVANCED_OVERRIDE_ID_MISSING_OR_INVALID'
          ELSE 'PAY_DRAFT_ADVANCE_OVERRIDE_NOT_CONSUMED'
        END AS reason_code
      FROM matched_advanced_overrides
      WHERE matched_advanced_overrides.advanced_override_id IS NULL
         OR matched_advanced_overrides.matching_consumed_count <> 1
    )
    SELECT
      (SELECT COUNT(*)::integer FROM expected_advanced_overrides),
      COALESCE((
        SELECT jsonb_agg(to_jsonb(expected_ids.advanced_override_id::text) ORDER BY expected_ids.advanced_override_id::text)
        FROM (
          SELECT DISTINCT expected_advanced_overrides.advanced_override_id
          FROM expected_advanced_overrides
          WHERE expected_advanced_overrides.advanced_override_id IS NOT NULL
        ) AS expected_ids
      ), '[]'::jsonb),
      (SELECT COUNT(*)::integer FROM mismatch_rows),
      COALESCE((
        SELECT jsonb_agg(
          jsonb_build_object(
            'check_code', 'PAY_DRAFT_ADVANCE_OVERRIDE_NOT_CONSUMED',
            'reason_code', mismatch_rows.reason_code,
            'candidate_scope_id', mismatch_rows.candidate_scope_id::text,
            'candidate_id', mismatch_rows.candidate_id::text,
            'pay_channel', mismatch_rows.pay_channel,
            'preview_row_id', mismatch_rows.preview_row_id,
            'timesheet_id', mismatch_rows.timesheet_id::text,
            'advanced_override_id', CASE WHEN mismatch_rows.advanced_override_id IS NULL THEN NULL ELSE mismatch_rows.advanced_override_id::text END,
            'raw_advanced_override_id', mismatch_rows.raw_advanced_override_id,
            'matching_consumed_count', mismatch_rows.matching_consumed_count
          )
          ORDER BY mismatch_rows.candidate_id::text, mismatch_rows.timesheet_id::text, mismatch_rows.preview_row_id
        )
        FROM mismatch_rows
      ), '[]'::jsonb)
    INTO
      v_expected_advance_override_count,
      v_expected_advance_override_ids,
      v_advance_override_consumption_mismatch_count,
      v_advance_override_consumption_mismatches;

    BEGIN
      PERFORM public._imp_debug_audit(
        p_actor_user_id,
        'PAY_CREATE_DRAFT_BATCH:STAGE_23C_ADVANCE_OVERRIDE_CONSUMPTION_ASSERT_RESULT',
        jsonb_build_object(
          'stage', v_stage,
          'pay_batch_id', v_batch_id::text,
          'operation_id', p_operation_id::text,
          'expected_advance_override_count', coalesce(v_expected_advance_override_count, 0),
          'expected_advance_override_ids', coalesce(v_expected_advance_override_ids, '[]'::jsonb),
          'consumed_timesheet_payment_override_count', coalesce(v_rows_upd_timesheet_overrides_consumed, 0),
          'consumed_timesheet_payment_override_ids', coalesce(v_consumed_timesheet_payment_override_ids, '[]'::jsonb),
          'advance_override_consumption_mismatch_count', coalesce(v_advance_override_consumption_mismatch_count, 0),
          'advance_override_consumption_mismatches', coalesce(v_advance_override_consumption_mismatches, '[]'::jsonb)
        ),
        'pay_batches',
        v_batch_id::text,
        NULL, NULL, NULL, NULL, NULL
      );
    EXCEPTION WHEN OTHERS THEN
      NULL;
    END;

    IF coalesce(v_advance_override_consumption_mismatch_count, 0) > 0 THEN
      RETURN jsonb_build_object(
        'ok', false,
        'pay_batch_id', v_batch_id::text,
        'operation_id', p_operation_id::text,
        'pass', false,
        'error', 'PAY_DRAFT_ADVANCE_OVERRIDE_NOT_CONSUMED',
        'code', 'PAY_DRAFT_ADVANCE_OVERRIDE_NOT_CONSUMED',
        'message', 'Draft integrity failed because one or more selected Advance Pay overrides were not consumed by the draft batch.',
        'friendly_error_message', 'Draft integrity failed because one or more selected Advance Pay overrides were not consumed by the draft batch.',
        'mismatch_details', coalesce(v_advance_override_consumption_mismatches, '[]'::jsonb),
        'affected_candidate_ids', coalesce((
          SELECT jsonb_agg(DISTINCT mismatch_value.value->>'candidate_id')
          FROM jsonb_array_elements(coalesce(v_advance_override_consumption_mismatches, '[]'::jsonb)) AS mismatch_value(value)
          WHERE NULLIF(mismatch_value.value->>'candidate_id', '') IS NOT NULL
        ), '[]'::jsonb),
        'breakdown_missing_ct', coalesce(v_breakdown_missing_ct, 0),
        'breakdown_bad_ct', coalesce(v_breakdown_bad_ct, 0),
        'blocked_count', jsonb_array_length(coalesce(v_blocked_candidates, '[]'::jsonb)),
        'consumed_timesheet_override_count', coalesce(v_rows_upd_timesheet_overrides_consumed, 0),
        'consumed_timesheet_override_ids', coalesce(v_consumed_timesheet_payment_override_ids, '[]'::jsonb),
        'expected_advance_override_count', coalesce(v_expected_advance_override_count, 0),
        'expected_advance_override_ids', coalesce(v_expected_advance_override_ids, '[]'::jsonb)
      );
    END IF;
  END IF;

  IF p_operation_id IS NOT NULL THEN
    SELECT count(*)::integer
    INTO v_operation_active_item_count
    FROM public.pay_batch_items AS item_row
    JOIN public.pay_batch_candidates AS batch_candidate
      ON batch_candidate.id = item_row.pay_batch_candidate_id
    WHERE batch_candidate.pay_batch_id = v_batch_id
      AND coalesce(item_row.is_voided, false) = false
      AND (
        (
          item_row.operation_source_key IS NOT NULL
          AND item_row.operation_source_key LIKE (p_operation_id::text || ':%')
        )
        OR EXISTS (
          SELECT 1
          FROM public.banking_pay_operation_candidate_allocation_rows AS linked_allocation_row
          WHERE linked_allocation_row.operation_id = p_operation_id
            AND linked_allocation_row.pay_batch_id = v_batch_id
            AND linked_allocation_row.pay_batch_item_id = item_row.id
        )
      );

    SELECT count(*)::integer,
           count(*) FILTER (
             WHERE lower(coalesce(scope_row.allocation_basis_json #>> '{reservation_availability,all_selected_rows_unavailable}', 'false')) IN ('true','t','1','yes','y')
           )::integer
    INTO v_operation_scope_count, v_operation_reservation_unavailable_scope_count
    FROM public.banking_pay_operation_candidate_scope AS scope_row
    WHERE scope_row.operation_id = p_operation_id
      AND scope_row.pay_batch_id = v_batch_id;

    IF coalesce(v_operation_active_item_count, 0) = 0 THEN
      RETURN jsonb_build_object(
        'ok', false,
        'pay_batch_id', v_batch_id::text,
        'operation_id', p_operation_id::text,
        'pass', false,
        'error', CASE
          WHEN coalesce(v_operation_scope_count, 0) > 0
           AND coalesce(v_operation_reservation_unavailable_scope_count, 0) = coalesce(v_operation_scope_count, 0)
            THEN 'PAY_DRAFT_ALL_SELECTED_PAYMENTS_ALREADY_RESERVED'
          ELSE 'PAY_DRAFT_NO_PAYABLE_ITEMS_REMAIN'
        END,
        'code', CASE
          WHEN coalesce(v_operation_scope_count, 0) > 0
           AND coalesce(v_operation_reservation_unavailable_scope_count, 0) = coalesce(v_operation_scope_count, 0)
            THEN 'PAY_DRAFT_ALL_SELECTED_PAYMENTS_ALREADY_RESERVED'
          ELSE 'PAY_DRAFT_NO_PAYABLE_ITEMS_REMAIN'
        END,
        'message', CASE
          WHEN coalesce(v_operation_scope_count, 0) > 0
           AND coalesce(v_operation_reservation_unavailable_scope_count, 0) = coalesce(v_operation_scope_count, 0)
            THEN 'No draft was created because all selected payments are already reserved in another active draft batch.'
          ELSE 'No draft was created because no payable items remained after eligibility checks.'
        END,
        'friendly_error_message', CASE
          WHEN coalesce(v_operation_scope_count, 0) > 0
           AND coalesce(v_operation_reservation_unavailable_scope_count, 0) = coalesce(v_operation_scope_count, 0)
            THEN 'No draft was created because all selected payments are already reserved in another active draft batch.'
          ELSE 'No draft was created because no payable items remained after eligibility checks.'
        END,
        'mismatch_details', jsonb_build_array(jsonb_build_object(
          'check_code', CASE
            WHEN coalesce(v_operation_scope_count, 0) > 0
             AND coalesce(v_operation_reservation_unavailable_scope_count, 0) = coalesce(v_operation_scope_count, 0)
              THEN 'PAY_DRAFT_ALL_SELECTED_PAYMENTS_ALREADY_RESERVED'
            ELSE 'PAY_DRAFT_NO_PAYABLE_ITEMS_REMAIN'
          END,
          'active_item_count', coalesce(v_operation_active_item_count, 0),
          'operation_scope_count', coalesce(v_operation_scope_count, 0),
          'reservation_unavailable_scope_count', coalesce(v_operation_reservation_unavailable_scope_count, 0)
        )),
        'affected_candidate_ids', coalesce(to_jsonb(v_candidate_ids), '[]'::jsonb),
        'breakdown_missing_ct', coalesce(v_breakdown_missing_ct, 0),
        'breakdown_bad_ct', coalesce(v_breakdown_bad_ct, 0),
        'blocked_count', jsonb_array_length(coalesce(v_blocked_candidates, '[]'::jsonb)),
        'consumed_timesheet_override_count', coalesce(v_rows_upd_timesheet_overrides_consumed, 0),
        'consumed_timesheet_override_ids', coalesce(v_consumed_timesheet_payment_override_ids, '[]'::jsonb)
      );
    END IF;

    WITH operation_checks AS (
      SELECT 'MISSING_BATCH_CANDIDATE'::text AS check_code,
             scope_row.candidate_id,
             jsonb_build_object(
               'candidate_scope_id', scope_row.id::text,
               'candidate_id', scope_row.candidate_id::text,
               'pay_channel', scope_row.pay_channel
             ) AS detail_json
      FROM public.banking_pay_operation_candidate_scope AS scope_row
      WHERE scope_row.operation_id = p_operation_id
        AND scope_row.pay_batch_id = v_batch_id
        AND lower(coalesce(scope_row.allocation_basis_json #>> '{reservation_availability,all_selected_rows_unavailable}', 'false')) NOT IN ('true','t','1','yes','y')
        AND NOT EXISTS (
          SELECT 1
          FROM public.pay_batch_candidates AS batch_candidate
          WHERE batch_candidate.pay_batch_id = v_batch_id
            AND batch_candidate.candidate_id = scope_row.candidate_id
        )

      UNION ALL

      SELECT 'UNLINKED_ALLOCATION_ROW'::text AS check_code,
             allocation_row.candidate_id,
             jsonb_build_object(
               'allocation_row_id', allocation_row.id::text,
               'candidate_scope_id', allocation_row.candidate_scope_id::text,
               'candidate_id', allocation_row.candidate_id::text,
               'pay_channel', allocation_row.pay_channel,
               'operation_source_key', allocation_row.operation_source_key
             ) AS detail_json
      FROM public.banking_pay_operation_candidate_allocation_rows AS allocation_row
      WHERE allocation_row.operation_id = p_operation_id
        AND allocation_row.pay_batch_id = v_batch_id
        AND (UPPER(BTRIM(COALESCE(allocation_row.status, ''))) NOT IN ('ITEM_CREATED', 'ITEM_INSERTED') OR allocation_row.pay_batch_item_id IS NULL)

      UNION ALL

      SELECT 'DUPLICATE_BATCH_CANDIDATE'::text AS check_code,
             duplicate_candidate.candidate_id,
             jsonb_build_object(
               'candidate_id', duplicate_candidate.candidate_id::text,
               'duplicate_count', duplicate_candidate.duplicate_count
             ) AS detail_json
      FROM (
        SELECT batch_candidate.candidate_id, count(*)::integer AS duplicate_count
        FROM public.pay_batch_candidates AS batch_candidate
        WHERE batch_candidate.pay_batch_id = v_batch_id
          AND batch_candidate.candidate_id = ANY(v_candidate_ids)
        GROUP BY batch_candidate.candidate_id
        HAVING count(*) > 1
      ) AS duplicate_candidate

      UNION ALL

      SELECT 'DUPLICATE_OPERATION_ITEM_KEY'::text AS check_code,
             batch_candidate.candidate_id,
             jsonb_build_object(
               'candidate_id', batch_candidate.candidate_id::text,
               'operation_source_key', duplicate_item.operation_source_key,
               'duplicate_count', duplicate_item.duplicate_count
             ) AS detail_json
      FROM (
        SELECT item_row.pay_batch_candidate_id, item_row.operation_source_key, count(*)::integer AS duplicate_count
        FROM public.pay_batch_items AS item_row
        WHERE item_row.operation_source_key IS NOT NULL
          AND item_row.operation_source_key LIKE (p_operation_id::text || ':%')
        GROUP BY item_row.pay_batch_candidate_id, item_row.operation_source_key
        HAVING count(*) > 1
      ) AS duplicate_item
      JOIN public.pay_batch_candidates AS batch_candidate
        ON batch_candidate.id = duplicate_item.pay_batch_candidate_id
       AND batch_candidate.pay_batch_id = v_batch_id

      UNION ALL

      SELECT 'DUPLICATE_OPERATION_BREAKDOWN_KEY'::text AS check_code,
             batch_candidate.candidate_id,
             jsonb_build_object(
               'candidate_id', batch_candidate.candidate_id::text,
               'operation_source_key', duplicate_breakdown.operation_source_key,
               'duplicate_count', duplicate_breakdown.duplicate_count
             ) AS detail_json
      FROM (
        SELECT breakdown_row.pay_batch_item_id, breakdown_row.operation_source_key, count(*)::integer AS duplicate_count
        FROM public.pay_batch_item_breakdowns AS breakdown_row
        WHERE breakdown_row.operation_source_key IS NOT NULL
          AND breakdown_row.operation_source_key LIKE (p_operation_id::text || ':%')
        GROUP BY breakdown_row.pay_batch_item_id, breakdown_row.operation_source_key
        HAVING count(*) > 1
      ) AS duplicate_breakdown
      JOIN public.pay_batch_items AS item_row
        ON item_row.id = duplicate_breakdown.pay_batch_item_id
      JOIN public.pay_batch_candidates AS batch_candidate
        ON batch_candidate.id = item_row.pay_batch_candidate_id
       AND batch_candidate.pay_batch_id = v_batch_id

      UNION ALL

      SELECT 'DUPLICATE_TIMESHEET_SNAPSHOT'::text AS check_code,
             snapshot_duplicate.candidate_id,
             jsonb_build_object(
               'timesheet_id', snapshot_duplicate.timesheet_id::text,
               'candidate_id', snapshot_duplicate.candidate_id::text,
               'pay_channel', snapshot_duplicate.pay_channel,
               'duplicate_count', snapshot_duplicate.duplicate_count
             ) AS detail_json
      FROM (
        SELECT snapshot_row.timesheet_id, snapshot_row.candidate_id, snapshot_row.pay_channel, count(*)::integer AS duplicate_count
        FROM public.pay_batch_timesheet_snapshots AS snapshot_row
        WHERE snapshot_row.pay_batch_id = v_batch_id
          AND snapshot_row.candidate_id = ANY(v_candidate_ids)
        GROUP BY snapshot_row.timesheet_id, snapshot_row.candidate_id, snapshot_row.pay_channel
        HAVING count(*) > 1
      ) AS snapshot_duplicate

      UNION ALL

      SELECT 'MISSING_TIMESHEET_SNAPSHOT'::text AS check_code,
             batch_candidate.candidate_id,
             jsonb_build_object(
               'pay_batch_item_id', item_row.id::text,
               'candidate_id', batch_candidate.candidate_id::text,
               'timesheet_id', item_row.timesheet_id::text,
               'pay_channel', item_row.pay_channel,
               'operation_source_key', item_row.operation_source_key
             ) AS detail_json
      FROM public.pay_batch_items AS item_row
      JOIN public.pay_batch_candidates AS batch_candidate
        ON batch_candidate.id = item_row.pay_batch_candidate_id
      WHERE batch_candidate.pay_batch_id = v_batch_id
        AND coalesce(item_row.is_voided, false) = false
        AND item_row.operation_source_key IS NOT NULL
        AND item_row.operation_source_key LIKE (p_operation_id::text || ':%')
        AND item_row.timesheet_id IS NOT NULL
        AND UPPER(BTRIM(COALESCE(item_row.item_type::text, ''))) IN ('SEGMENT_DELTA', 'EXPENSE_DELTA', 'ADJUSTMENT_DELTA', 'MILEAGE_DELTA')
        AND NOT EXISTS (
          SELECT 1
          FROM public.pay_batch_timesheet_snapshots AS snapshot_row
          WHERE snapshot_row.pay_batch_id = v_batch_id
            AND snapshot_row.timesheet_id = item_row.timesheet_id
            AND snapshot_row.candidate_id = batch_candidate.candidate_id
            AND UPPER(BTRIM(COALESCE(snapshot_row.pay_channel, ''))) = UPPER(BTRIM(COALESCE(item_row.pay_channel, '')))
        )

      UNION ALL

      SELECT 'DRAFT_SCOPE_SELECTED_LINES_MISSING'::text AS check_code,
             scope_row.candidate_id,
             jsonb_build_object(
               'candidate_scope_id', scope_row.id::text,
               'candidate_id', scope_row.candidate_id::text,
               'pay_channel', scope_row.pay_channel,
               'selected_row_count_seeded_in_page', CASE
                 WHEN COALESCE(scope_row.candidate_totals_json->>'selected_row_count_seeded_in_page', '') ~ '^[0-9]+$'
                   THEN (scope_row.candidate_totals_json->>'selected_row_count_seeded_in_page')::integer
                 WHEN COALESCE(scope_row.candidate_totals_json->>'selected_preview_row_count', '') ~ '^[0-9]+$'
                   THEN (scope_row.candidate_totals_json->>'selected_preview_row_count')::integer
                 WHEN COALESCE(scope_row.candidate_totals_json->>'selected_row_count', '') ~ '^[0-9]+$'
                   THEN (scope_row.candidate_totals_json->>'selected_row_count')::integer
                 ELSE 0
               END,
               'selected_canonical_preview_lines_count', private.pay_workbench_operation_selected_line_count_v8(p_operation_id, scope_row.id)
             ) AS detail_json
      FROM public.banking_pay_operation_candidate_scope AS scope_row
      WHERE scope_row.operation_id = p_operation_id
        AND scope_row.pay_batch_id = v_batch_id
        AND lower(coalesce(scope_row.allocation_basis_json #>> '{reservation_availability,all_selected_rows_unavailable}', 'false')) NOT IN ('true','t','1','yes','y')
        AND (
          CASE
            WHEN COALESCE(scope_row.candidate_totals_json->>'selected_row_count_seeded_in_page', '') ~ '^[0-9]+$'
              THEN (scope_row.candidate_totals_json->>'selected_row_count_seeded_in_page')::integer
            WHEN COALESCE(scope_row.candidate_totals_json->>'selected_preview_row_count', '') ~ '^[0-9]+$'
              THEN (scope_row.candidate_totals_json->>'selected_preview_row_count')::integer
            WHEN COALESCE(scope_row.candidate_totals_json->>'selected_row_count', '') ~ '^[0-9]+$'
              THEN (scope_row.candidate_totals_json->>'selected_row_count')::integer
            ELSE 0
          END
        ) > 0
        AND private.pay_workbench_operation_selected_line_count_v8(p_operation_id, scope_row.id) = 0

      UNION ALL

      SELECT 'MISSING_SELECTED_PREVIEW_ROW_ITEM'::text AS check_code,
             selected_line.candidate_id,
             jsonb_build_object(
               'candidate_scope_id', selected_line.candidate_scope_id::text,
               'candidate_id', selected_line.candidate_id::text,
               'pay_channel', selected_line.pay_channel,
               'preview_row_id', selected_line.preview_row_id,
               'finance_case_id', CASE WHEN selected_line.finance_case_id IS NULL THEN NULL ELSE selected_line.finance_case_id::text END,
               'timesheet_id', CASE WHEN selected_line.timesheet_id IS NULL THEN NULL ELSE selected_line.timesheet_id::text END
             ) AS detail_json
      FROM (
        SELECT
          scope_row.id AS candidate_scope_id,
          scope_row.candidate_id,
          scope_row.pay_channel,
          coalesce(
            nullif(btrim(coalesce(line_element.value->>'preview_row_id', '')), ''),
            nullif(btrim(coalesce(line_element.value->>'line_id', '')), ''),
            nullif(btrim(coalesce(line_element.value->>'row_id', '')), ''),
            nullif(btrim(coalesce(line_element.value->>'id', '')), '')
          ) AS preview_row_id,
          CASE WHEN coalesce(line_element.value->>'finance_case_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN (line_element.value->>'finance_case_id')::uuid ELSE NULL::uuid END AS finance_case_id,
          CASE WHEN coalesce(line_element.value->>'timesheet_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN (line_element.value->>'timesheet_id')::uuid ELSE NULL::uuid END AS timesheet_id,
          upper(btrim(coalesce(line_element.value->>'line_type', line_element.value->>'item_type', line_element.value->>'case_type', ''))) AS line_type,
          round(coalesce(
            CASE WHEN coalesce(reservation_metadata.reservation_json->>'effective_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (reservation_metadata.reservation_json->>'effective_amount_ex_vat')::numeric ELSE NULL::numeric END,
            CASE WHEN coalesce(line_element.value->>'amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (line_element.value->>'amount_ex_vat')::numeric ELSE NULL::numeric END,
            CASE WHEN coalesce(line_element.value->>'preview_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (line_element.value->>'preview_amount_ex_vat')::numeric ELSE NULL::numeric END,
            0::numeric
          ), 2) AS expected_amount_ex_vat,
          coalesce(
            lower(coalesce(reservation_metadata.reservation_json->>'skipped_due_to_active_reservation', 'false')) IN ('true','t','1','yes','y'),
            false
          ) AS skipped_due_to_active_reservation
        FROM public.banking_pay_operation_candidate_scope AS scope_row
        CROSS JOIN LATERAL private.pay_workbench_operation_selected_lines_v8(p_operation_id, scope_row.id) AS line_element(value)
        LEFT JOIN LATERAL (
          SELECT reservation_row.value AS reservation_json
          FROM jsonb_array_elements(
            CASE
              WHEN jsonb_typeof(scope_row.allocation_basis_json #> '{reservation_availability,preview_rows}') = 'array'
                THEN scope_row.allocation_basis_json #> '{reservation_availability,preview_rows}'
              ELSE '[]'::jsonb
            END
          ) AS reservation_row(value)
          WHERE reservation_row.value->>'preview_row_id' = coalesce(
            nullif(btrim(coalesce(line_element.value->>'preview_row_id', '')), ''),
            nullif(btrim(coalesce(line_element.value->>'line_id', '')), ''),
            nullif(btrim(coalesce(line_element.value->>'row_id', '')), ''),
            nullif(btrim(coalesce(line_element.value->>'id', '')), '')
          )
          LIMIT 1
        ) AS reservation_metadata ON true
        WHERE scope_row.operation_id = p_operation_id
          AND scope_row.pay_batch_id = v_batch_id
          AND jsonb_typeof(line_element.value) = 'object'
          AND coalesce(CASE WHEN lower(coalesce(line_element.value->>'draftable', 'true')) IN ('true','false') THEN (line_element.value->>'draftable')::boolean ELSE true END, true) = true
      ) AS selected_line
      WHERE selected_line.preview_row_id IS NOT NULL
        AND coalesce(selected_line.skipped_due_to_active_reservation, false) = false
        AND NOT EXISTS (
          SELECT 1
          FROM public.pay_batch_candidates AS batch_candidate
          JOIN public.pay_batch_items AS item_row
            ON item_row.pay_batch_candidate_id = batch_candidate.id
          WHERE batch_candidate.pay_batch_id = v_batch_id
            AND batch_candidate.candidate_id = selected_line.candidate_id
            AND coalesce(item_row.is_voided, false) = false
            AND (
              item_row.operation_source_key IS NOT NULL
              AND item_row.operation_source_key LIKE (p_operation_id::text || ':%')
              AND item_row.operation_source_key LIKE ('%' || selected_line.preview_row_id || '%')
            )
          LIMIT 1
        )

      UNION ALL

      SELECT 'CANDIDATE_SCOPE_TOTAL_MISMATCH'::text AS check_code,
             scope_totals.candidate_id,
             jsonb_build_object(
               'candidate_scope_id', scope_totals.candidate_scope_id::text,
               'candidate_id', scope_totals.candidate_id::text,
               'pay_channel', scope_totals.pay_channel,
               'expected_amount_ex_vat', scope_totals.expected_amount_ex_vat,
               'actual_amount_ex_vat', scope_totals.actual_amount_ex_vat
             ) AS detail_json
      FROM (
        SELECT
          scope_row.id AS candidate_scope_id,
          scope_row.candidate_id,
          scope_row.pay_channel,
          round(coalesce((
            SELECT sum(
              round(coalesce(
                CASE WHEN coalesce(reservation_metadata.reservation_json->>'effective_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (reservation_metadata.reservation_json->>'effective_amount_ex_vat')::numeric ELSE NULL::numeric END,
                CASE WHEN coalesce(selected_line_element.value->>'amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (selected_line_element.value->>'amount_ex_vat')::numeric ELSE NULL::numeric END,
                CASE WHEN coalesce(selected_line_element.value->>'preview_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (selected_line_element.value->>'preview_amount_ex_vat')::numeric ELSE NULL::numeric END,
                0::numeric
              ), 2)
            )
            FROM private.pay_workbench_operation_selected_lines_v8(p_operation_id, scope_row.id) AS selected_line_element(value)
            LEFT JOIN LATERAL (
              SELECT reservation_row.value AS reservation_json
              FROM jsonb_array_elements(
                CASE
                  WHEN jsonb_typeof(scope_row.allocation_basis_json #> '{reservation_availability,preview_rows}') = 'array'
                    THEN scope_row.allocation_basis_json #> '{reservation_availability,preview_rows}'
                  ELSE '[]'::jsonb
                END
              ) AS reservation_row(value)
              WHERE reservation_row.value->>'preview_row_id' = coalesce(
                nullif(btrim(coalesce(selected_line_element.value->>'preview_row_id', '')), ''),
                nullif(btrim(coalesce(selected_line_element.value->>'line_id', '')), ''),
                nullif(btrim(coalesce(selected_line_element.value->>'row_id', '')), ''),
                nullif(btrim(coalesce(selected_line_element.value->>'id', '')), '')
              )
              LIMIT 1
            ) AS reservation_metadata ON true
            WHERE jsonb_typeof(selected_line_element.value) = 'object'
              AND coalesce(CASE WHEN lower(coalesce(selected_line_element.value->>'draftable', 'true')) IN ('true','false') THEN (selected_line_element.value->>'draftable')::boolean ELSE true END, true) = true
          ), 0::numeric), 2) AS expected_amount_ex_vat,
          round(coalesce((
            SELECT sum(coalesce(item_row.amount_ex_vat, 0))
            FROM public.pay_batch_candidates AS batch_candidate
            JOIN public.pay_batch_items AS item_row
              ON item_row.pay_batch_candidate_id = batch_candidate.id
            WHERE batch_candidate.pay_batch_id = v_batch_id
              AND batch_candidate.candidate_id = scope_row.candidate_id
              AND upper(coalesce(item_row.pay_channel, '')) = upper(coalesce(scope_row.pay_channel, ''))
              AND coalesce(item_row.is_voided, false) = false
          ), 0), 2) AS actual_amount_ex_vat
        FROM public.banking_pay_operation_candidate_scope AS scope_row
        WHERE scope_row.operation_id = p_operation_id
          AND scope_row.pay_batch_id = v_batch_id
      ) AS scope_totals
      WHERE scope_totals.expected_amount_ex_vat IS NOT NULL
        AND scope_totals.expected_amount_ex_vat <> scope_totals.actual_amount_ex_vat

      UNION ALL

      SELECT 'ALLOCATION_ITEM_AMOUNT_MISMATCH'::text AS check_code,
             allocation_row.candidate_id,
             jsonb_build_object(
               'allocation_row_id', allocation_row.id::text,
               'pay_batch_item_id', CASE WHEN item_row.id IS NULL THEN NULL ELSE item_row.id::text END,
               'candidate_id', allocation_row.candidate_id::text,
               'pay_channel', allocation_row.pay_channel,
               'operation_source_key', allocation_row.operation_source_key,
               'expected_allocated_amount', round(coalesce(allocation_row.allocated_amount, 0), 2),
               'actual_item_amount_ex_vat', CASE WHEN item_row.id IS NULL THEN NULL ELSE round(coalesce(item_row.amount_ex_vat, 0), 2) END
             ) AS detail_json
      FROM public.banking_pay_operation_candidate_allocation_rows AS allocation_row
      LEFT JOIN public.pay_batch_items AS item_row
        ON item_row.id = allocation_row.pay_batch_item_id
       AND coalesce(item_row.is_voided, false) = false
      WHERE allocation_row.operation_id = p_operation_id
        AND allocation_row.pay_batch_id = v_batch_id
        AND UPPER(BTRIM(COALESCE(allocation_row.status, ''))) IN ('ITEM_CREATED', 'ITEM_INSERTED')
        AND (
          item_row.id IS NULL
          OR round(coalesce(item_row.amount_ex_vat, 0), 2) <> round(coalesce(allocation_row.allocated_amount, 0), 2)
        )

      UNION ALL

      SELECT 'PAY_CHANNEL_SCOPE_TOTAL_MISMATCH'::text AS check_code,
             NULL::uuid AS candidate_id,
             jsonb_build_object(
               'pay_channel', channel_totals.pay_channel,
               'expected_amount_ex_vat', channel_totals.expected_amount_ex_vat,
               'actual_amount_ex_vat', channel_totals.actual_amount_ex_vat
             ) AS detail_json
      FROM (
        SELECT
          expected_channel.pay_channel,
          round(sum(expected_channel.expected_amount_ex_vat), 2) AS expected_amount_ex_vat,
          round(coalesce((
            SELECT sum(coalesce(item_row.amount_ex_vat, 0))
            FROM public.pay_batch_items AS item_row
            JOIN public.pay_batch_candidates AS batch_candidate
              ON batch_candidate.id = item_row.pay_batch_candidate_id
            WHERE batch_candidate.pay_batch_id = v_batch_id
              AND upper(coalesce(item_row.pay_channel, '')) = expected_channel.pay_channel
              AND coalesce(item_row.is_voided, false) = false
          ), 0), 2) AS actual_amount_ex_vat
        FROM (
          SELECT
            upper(btrim(coalesce(scope_row.pay_channel, ''))) AS pay_channel,
            round(coalesce(
              CASE WHEN coalesce(reservation_metadata.reservation_json->>'effective_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (reservation_metadata.reservation_json->>'effective_amount_ex_vat')::numeric ELSE NULL::numeric END,
              CASE WHEN coalesce(line_element.value->>'amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (line_element.value->>'amount_ex_vat')::numeric ELSE NULL::numeric END,
              CASE WHEN coalesce(line_element.value->>'preview_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (line_element.value->>'preview_amount_ex_vat')::numeric ELSE NULL::numeric END,
              0::numeric
            ), 2) AS expected_amount_ex_vat
          FROM public.banking_pay_operation_candidate_scope AS scope_row
          CROSS JOIN LATERAL private.pay_workbench_operation_selected_lines_v8(p_operation_id, scope_row.id) AS line_element(value)
          LEFT JOIN LATERAL (
            SELECT reservation_row.value AS reservation_json
            FROM jsonb_array_elements(
              CASE
                WHEN jsonb_typeof(scope_row.allocation_basis_json #> '{reservation_availability,preview_rows}') = 'array'
                  THEN scope_row.allocation_basis_json #> '{reservation_availability,preview_rows}'
                ELSE '[]'::jsonb
              END
            ) AS reservation_row(value)
            WHERE reservation_row.value->>'preview_row_id' = coalesce(
              nullif(btrim(coalesce(line_element.value->>'preview_row_id', '')), ''),
              nullif(btrim(coalesce(line_element.value->>'line_id', '')), ''),
              nullif(btrim(coalesce(line_element.value->>'row_id', '')), ''),
              nullif(btrim(coalesce(line_element.value->>'id', '')), '')
            )
            LIMIT 1
          ) AS reservation_metadata ON true
          WHERE scope_row.operation_id = p_operation_id
            AND scope_row.pay_batch_id = v_batch_id
            AND jsonb_typeof(line_element.value) = 'object'
            AND coalesce(CASE WHEN lower(coalesce(line_element.value->>'draftable', 'true')) IN ('true','false') THEN (line_element.value->>'draftable')::boolean ELSE true END, true) = true
        ) AS expected_channel
        WHERE expected_channel.pay_channel IN ('PAYE','UMBRELLA')
        GROUP BY expected_channel.pay_channel
      ) AS channel_totals
      WHERE channel_totals.expected_amount_ex_vat <> channel_totals.actual_amount_ex_vat
    )
    SELECT count(*)::integer,
           coalesce(jsonb_agg(operation_checks.detail_json || jsonb_build_object('check_code', operation_checks.check_code) order by operation_checks.check_code, operation_checks.candidate_id::text), '[]'::jsonb),
           coalesce(jsonb_agg(distinct to_jsonb(operation_checks.candidate_id::text)) filter (where operation_checks.candidate_id is not null), '[]'::jsonb)
    INTO v_operation_mismatch_count, v_operation_mismatch_details, v_operation_affected_candidate_ids
    FROM operation_checks;

    IF coalesce(v_operation_mismatch_count, 0) > 0 THEN
      RETURN jsonb_build_object(
        'ok', false,
        'pay_batch_id', v_batch_id::text,
        'operation_id', p_operation_id::text,
        'pass', false,
        'error', 'DRAFT_INTEGRITY_FAILED',
        'code', 'DRAFT_INTEGRITY_FAILED',
        'mismatch_details', coalesce(v_operation_mismatch_details, '[]'::jsonb),
        'affected_candidate_ids', coalesce(v_operation_affected_candidate_ids, '[]'::jsonb),
        'friendly_error_message', 'Operation draft integrity checks failed. The chunked draft does not exactly match the snapshotted operation scope.',
        'breakdown_missing_ct', coalesce(v_breakdown_missing_ct, 0),
        'breakdown_bad_ct', coalesce(v_breakdown_bad_ct, 0),
        'blocked_count', jsonb_array_length(coalesce(v_blocked_candidates, '[]'::jsonb)),
        'consumed_timesheet_override_count', coalesce(v_rows_upd_timesheet_overrides_consumed, 0),
        'consumed_timesheet_override_ids', coalesce(v_consumed_timesheet_payment_override_ids, '[]'::jsonb)
      );
    END IF;
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'pay_batch_id', v_batch_id::text,
    'operation_id', CASE WHEN p_operation_id IS NULL THEN NULL ELSE p_operation_id::text END,
    'pass', true,
    'mismatch_details', coalesce(v_operation_mismatch_details, '{}'::jsonb),
    'affected_candidate_ids', coalesce(v_operation_affected_candidate_ids, '[]'::jsonb),
    'generation_provenance', CASE
      WHEN p_operation_id IS NULL THEN NULL::jsonb
      ELSE v_generation_provenance_json
    END,
    'friendly_error_message', null::text,
    'breakdown_missing_ct', coalesce(v_breakdown_missing_ct, 0),
    'breakdown_bad_ct', coalesce(v_breakdown_bad_ct, 0),
    'blocked_count', jsonb_array_length(coalesce(v_blocked_candidates, '[]'::jsonb)),
    'consumed_timesheet_override_count', coalesce(v_rows_upd_timesheet_overrides_consumed, 0),
        'consumed_timesheet_override_ids', coalesce(v_consumed_timesheet_payment_override_ids, '[]'::jsonb)
  );
END;
$function$;
