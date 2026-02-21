CREATE OR REPLACE FUNCTION public.candidate_pay_method_change_apply(
  p_candidate_id uuid,
  p_new_method text,
  p_plan jsonb,
  p_actor_user_id uuid,
  p_reason text DEFAULT null
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_old_method text;
  v_umbrella_id uuid;

  v_item jsonb;
  v_contract_id uuid;
  v_successor_start date;

  v_expected_earliest_we date;
  v_expected_successor_start date;

  v_old_contract_end date;
  v_old_contract_start date;

  v_new_contract_id uuid;

  v_moved_timesheet_ids uuid[] := ARRAY[]::uuid[];

  v_keep_min_we date;
  v_keep_max_we date;
  v_new_old_end date;

  v_old_timesheets_remaining integer := 0;

  v_contract_before jsonb;
  v_contract_after jsonb;

  v_result jsonb := jsonb_build_object(
    'candidate_id', coalesce(p_candidate_id::text,''),
    'old_method', null,
    'new_method', coalesce(p_new_method,''),
    'migrations', jsonb_build_array()
  );
BEGIN
  -- Validate inputs
  IF p_candidate_id IS NULL THEN
    RAISE EXCEPTION 'candidate_id is required';
  END IF;

  IF p_new_method IS NULL OR (p_new_method <> 'PAYE' AND p_new_method <> 'UMBRELLA') THEN
    RAISE EXCEPTION 'new_method must be PAYE or UMBRELLA';
  END IF;

  IF p_plan IS NULL OR jsonb_typeof(p_plan) <> 'array' THEN
    RAISE EXCEPTION 'plan must be a JSON array';
  END IF;

  -- Lock candidate row to prevent concurrent migrations
  SELECT c.pay_method, c.umbrella_id
    INTO v_old_method, v_umbrella_id
  FROM public.candidates c
  WHERE c.id = p_candidate_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Candidate not found';
  END IF;

  v_result := jsonb_set(v_result, '{old_method}', to_jsonb(v_old_method), true);

  IF v_old_method = p_new_method THEN
    RAISE EXCEPTION 'Candidate already has pay_method %', p_new_method;
  END IF;

  IF p_new_method = 'UMBRELLA' AND v_umbrella_id IS NULL THEN
    RAISE EXCEPTION 'Cannot change candidate to UMBRELLA without umbrella_id';
  END IF;

  -- Perform migrations per plan item
  FOR v_item IN SELECT value FROM jsonb_array_elements(p_plan)
  LOOP
    v_contract_id := (v_item->>'contract_id')::uuid;
    v_successor_start := (v_item->>'successor_start')::date;

    IF v_contract_id IS NULL THEN
      RAISE EXCEPTION 'plan item missing contract_id';
    END IF;

    IF v_successor_start IS NULL THEN
      RAISE EXCEPTION 'plan item missing successor_start for contract %', v_contract_id;
    END IF;

    IF (v_item ? 'new_rates_json') IS NOT TRUE THEN
      RAISE EXCEPTION 'plan item missing new_rates_json for contract %', v_contract_id;
    END IF;

    -- Lock and validate contract belongs to candidate and matches old method
    SELECT ct.start_date, ct.end_date
      INTO v_old_contract_start, v_old_contract_end
    FROM public.contracts ct
    WHERE ct.id = v_contract_id
      AND ct.candidate_id = p_candidate_id
      AND ct.pay_method_snapshot = v_old_method
    FOR UPDATE;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'Contract % not found for candidate or pay_method mismatch', v_contract_id;
    END IF;

    -- Compute earliest outstanding week-ending date (authoritative)
    SELECT MIN(cw.week_ending_date)
      INTO v_expected_earliest_we
    FROM public.contract_weeks cw
    LEFT JOIN public.timesheets_financials tsf
      ON tsf.timesheet_id = cw.timesheet_id
     AND tsf.is_current = true
    WHERE cw.contract_id = v_contract_id
      AND (
        cw.timesheet_id IS NULL
        OR (
          tsf.locked_by_invoice_id IS NULL
          AND tsf.paid_at_utc IS NULL
        )
      );

    IF v_expected_earliest_we IS NULL THEN
      RAISE EXCEPTION 'Contract % has no outstanding weeks to migrate', v_contract_id;
    END IF;

    -- ✅ FIX: expected successor_start is the later of:
    --   (earliest_outstanding_we - 6) AND old_contract.start_date
    v_expected_successor_start := (v_expected_earliest_we - 6);
    IF v_expected_successor_start < v_old_contract_start THEN
      v_expected_successor_start := v_old_contract_start;
    END IF;

    IF v_expected_successor_start <> v_successor_start THEN
      RAISE EXCEPTION
        'Plan successor_start % does not match expected % for contract %',
        v_successor_start, v_expected_successor_start, v_contract_id;
    END IF;

    -- Capture before snapshot (minimal) for audit
    v_contract_before := jsonb_build_object(
      'contract_id', v_contract_id::text,
      'start_date', v_old_contract_start::text,
      'end_date', v_old_contract_end::text,
      'pay_method_snapshot', v_old_method,
      'successor_start', v_successor_start::text
    );

    -- Create successor contract: copy ALL columns except overrides
    INSERT INTO public.contracts (
      candidate_id,
      client_id,
      role,
      band,
      display_site,
      ward_hint,
      start_date,
      end_date,
      pay_method_snapshot,
      rates_json,
      std_hours_json,
      default_submission_mode,
      week_ending_weekday_snapshot,
      auto_invoice,
      require_reference_to_pay,
      require_reference_to_invoice,
      created_at,
      updated_at,
      bucket_labels_json,
      std_schedule_json,
      mileage_pay_rate,
      mileage_charge_rate,
      additional_rates_json,
      self_bill,
      weekly_timesheet_source,
      no_timesheet_required,
      daily_calc_of_invoices,
      group_nightsat_sunbh,
      is_nhsp,
      autoprocess_hr,
      requires_hr,
      hr_attach_to_invoice,
      ts_attach_to_invoice,
      overrideclientsettings,
      reference_number_required_to_issue_invoice,
      send_manual_invoices_to_different_email,
      manual_invoices_alt_email_address
    )
    SELECT
      ct.candidate_id,
      ct.client_id,
      ct.role,
      ct.band,
      ct.display_site,
      ct.ward_hint,
      v_successor_start,
      ct.end_date,
      p_new_method,
      (v_item->'new_rates_json'),
      ct.std_hours_json,
      ct.default_submission_mode,
      ct.week_ending_weekday_snapshot,
      ct.auto_invoice,
      ct.require_reference_to_pay,
      ct.require_reference_to_invoice,
      now(),
      now(),
      ct.bucket_labels_json,
      ct.std_schedule_json,
      ct.mileage_pay_rate,
      ct.mileage_charge_rate,
      COALESCE(v_item->'new_additional_rates_json', ct.additional_rates_json),
      ct.self_bill,
      ct.weekly_timesheet_source,
      ct.no_timesheet_required,
      ct.daily_calc_of_invoices,
      ct.group_nightsat_sunbh,
      ct.is_nhsp,
      ct.autoprocess_hr,
      ct.requires_hr,
      ct.hr_attach_to_invoice,
      ct.ts_attach_to_invoice,
      ct.overrideclientsettings,
      ct.reference_number_required_to_issue_invoice,
      ct.send_manual_invoices_to_different_email,
      ct.manual_invoices_alt_email_address
    FROM public.contracts ct
    WHERE ct.id = v_contract_id
    RETURNING id INTO v_new_contract_id;

    -- Move outstanding contract_weeks to successor (and update updated_at)
    UPDATE public.contract_weeks cw
    SET contract_id = v_new_contract_id,
        updated_at = now()
    WHERE cw.contract_id = v_contract_id
      AND (
        cw.timesheet_id IS NULL
        OR EXISTS (
          SELECT 1
          FROM public.timesheets_financials tsf2
          WHERE tsf2.timesheet_id = cw.timesheet_id
            AND tsf2.is_current = true
            AND tsf2.locked_by_invoice_id IS NULL
            AND tsf2.paid_at_utc IS NULL
        )
      );

    -- Move current timesheets for those moved weeks
    WITH moved_ts AS (
      SELECT DISTINCT cw2.timesheet_id
      FROM public.contract_weeks cw2
      JOIN public.timesheets_financials tsf3
        ON tsf3.timesheet_id = cw2.timesheet_id
       AND tsf3.is_current = true
      WHERE cw2.contract_id = v_new_contract_id
        AND cw2.timesheet_id IS NOT NULL
        AND tsf3.locked_by_invoice_id IS NULL
        AND tsf3.paid_at_utc IS NULL
    )
    UPDATE public.timesheets t
    SET contract_id = v_new_contract_id
    WHERE t.timesheet_id IN (SELECT timesheet_id FROM moved_ts)
      AND t.is_current = true;

    -- Collect moved timesheet ids (for return + outbox enqueue)
    SELECT COALESCE(array_agg(mt.timesheet_id), ARRAY[]::uuid[])
      INTO STRICT v_moved_timesheet_ids
    FROM (
      SELECT DISTINCT cw2.timesheet_id
      FROM public.contract_weeks cw2
      JOIN public.timesheets_financials tsf3
        ON tsf3.timesheet_id = cw2.timesheet_id
       AND tsf3.is_current = true
      WHERE cw2.contract_id = v_new_contract_id
        AND cw2.timesheet_id IS NOT NULL
        AND tsf3.locked_by_invoice_id IS NULL
        AND tsf3.paid_at_utc IS NULL
    ) mt;

    -- Enqueue TSFIN recompute for moved timesheets
    IF array_length(v_moved_timesheet_ids, 1) IS NOT NULL THEN
      INSERT INTO public.ts_financials_outbox (timesheet_id, reason, attempt_count, created_at)
      SELECT tid, 'RATE_CHANGED'::public.ts_fin_reason_enum, 0, now()
      FROM unnest(v_moved_timesheet_ids) AS tid;
    END IF;

    -- Adjust old contract end_date:
    -- Keep range that covers any invoice-locked or paid weeks still attached to old contract.
    SELECT MIN(cw3.week_ending_date), MAX(cw3.week_ending_date)
      INTO v_keep_min_we, v_keep_max_we
    FROM public.contract_weeks cw3
    JOIN public.timesheets_financials tsf4
      ON tsf4.timesheet_id = cw3.timesheet_id
     AND tsf4.is_current = true
    WHERE cw3.contract_id = v_contract_id
      AND cw3.timesheet_id IS NOT NULL
      AND (
        tsf4.locked_by_invoice_id IS NOT NULL
        OR tsf4.paid_at_utc IS NOT NULL
      );

    IF v_keep_max_we IS NULL THEN
      v_new_old_end := (v_successor_start - 1);
    ELSE
      v_new_old_end := v_keep_max_we;
    END IF;

    -- ✅ FIX: never allow old end_date to precede old start_date
    IF v_new_old_end < v_old_contract_start THEN
      v_new_old_end := v_old_contract_start;
    END IF;

    UPDATE public.contracts ct_old
    SET end_date = v_new_old_end,
        updated_at = now()
    WHERE ct_old.id = v_contract_id;

    -- Delete future empty weeks on old contract (timesheet_id IS NULL) beyond old end
    DELETE FROM public.contract_weeks cw_del
    WHERE cw_del.contract_id = v_contract_id
      AND cw_del.timesheet_id IS NULL
      AND cw_del.week_ending_date > v_new_old_end;

    -- Remaining timesheets on OLD contract after migration (for UI delete-old prompt)
    SELECT COUNT(1)
      INTO v_old_timesheets_remaining
    FROM public.timesheets t_rem
    WHERE t_rem.contract_id = v_contract_id
      AND t_rem.is_current = true;

    -- Audit contract migration
    v_contract_after := jsonb_build_object(
      'old_contract_id', v_contract_id::text,
      'new_contract_id', v_new_contract_id::text,
      'successor_start', v_successor_start::text,
      'moved_timesheet_ids', to_jsonb(COALESCE(v_moved_timesheet_ids, ARRAY[]::uuid[])),
      'old_contract_new_end_date', v_new_old_end::text,
      'old_contract_timesheets_remaining', v_old_timesheets_remaining
    );

    PERFORM public._audit_insert(
      'contracts',
      v_contract_id::text,
      'PAY_METHOD_CONTRACT_MIGRATED',
      v_contract_before,
      v_contract_after,
      COALESCE(p_reason, 'PAY_METHOD_CHANGE'),
      p_actor_user_id
    );

    -- Add to result
    v_result := jsonb_set(
      v_result,
      '{migrations}',
      (v_result->'migrations') || jsonb_build_object(
        'old_contract_id', v_contract_id::text,
        'new_contract_id', v_new_contract_id::text,
        'successor_start', v_successor_start::text,
        'moved_timesheet_ids', to_jsonb(COALESCE(v_moved_timesheet_ids, ARRAY[]::uuid[])),
        'old_contract_new_end_date', v_new_old_end::text,
        'old_contract_timesheets_remaining', v_old_timesheets_remaining,
        'old_contract_can_delete', (v_old_timesheets_remaining = 0)
      ),
      true
    );

  END LOOP;

  -- Update candidate pay_method last (still in txn)
  PERFORM public._audit_insert(
    'candidates',
    p_candidate_id::text,
    'PAY_METHOD_CHANGE_APPLY',
    jsonb_build_object('pay_method', v_old_method, 'umbrella_id', CASE WHEN v_umbrella_id IS NULL THEN null ELSE v_umbrella_id::text END),
    jsonb_build_object('pay_method', p_new_method, 'umbrella_id', CASE WHEN p_new_method = 'PAYE' THEN null ELSE (CASE WHEN v_umbrella_id IS NULL THEN null ELSE v_umbrella_id::text END) END),
    COALESCE(p_reason, 'PAY_METHOD_CHANGE'),
    p_actor_user_id
  );

  UPDATE public.candidates c
  SET pay_method = p_new_method,
      umbrella_id = CASE WHEN p_new_method = 'PAYE' THEN NULL ELSE c.umbrella_id END,
      updated_at = now()
  WHERE c.id = p_candidate_id;

  -- Bump change counters for UI delta sync
  PERFORM public._change_bump('candidates');
  PERFORM public._change_bump('contracts');
  PERFORM public._change_bump('timesheets');

  RETURN v_result;
END;
$function$;



-- =========================================================
-- 3) NEW RPCs: Candidate delete eligibility + apply (tombstones + audit + change bump)
--    Verified:
--      candidates_tombstones(id, deleted_rev, deleted_at)
--      app_change_counters(entity_key, seq, updated_at)
-- =========================================================

CREATE OR REPLACE FUNCTION public.candidate_delete_eligibility(p_candidate_id uuid)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_contracts int;
  v_contract_weeks int;
  v_timesheets int;
  v_invoice_lines int;
  v_pay_batches int;
  v_reason text := null;
  v_can boolean := true;
BEGIN
  IF p_candidate_id IS NULL THEN
    RAISE EXCEPTION 'candidate_id is required';
  END IF;

  SELECT COUNT(*) INTO v_contracts
  FROM public.contracts ct
  WHERE ct.candidate_id = p_candidate_id;

  SELECT COUNT(*) INTO v_contract_weeks
  FROM public.contract_weeks cw
  JOIN public.contracts ct ON ct.id = cw.contract_id
  WHERE ct.candidate_id = p_candidate_id;

  SELECT COUNT(*) INTO v_timesheets
  FROM public.timesheets t
  JOIN public.contracts ct ON ct.id = t.contract_id
  WHERE ct.candidate_id = p_candidate_id;

  SELECT COUNT(*) INTO v_invoice_lines
  FROM public.invoice_lines il
  JOIN public.timesheets t ON t.timesheet_id = il.timesheet_id
  JOIN public.contracts ct ON ct.id = t.contract_id
  WHERE ct.candidate_id = p_candidate_id;

  SELECT COUNT(*) INTO v_pay_batches
  FROM public.pay_batch_candidates pbc
  WHERE pbc.candidate_id = p_candidate_id;

  IF v_contracts > 0 OR v_contract_weeks > 0 OR v_timesheets > 0 OR v_invoice_lines > 0 OR v_pay_batches > 0 THEN
    v_can := false;
    v_reason := 'Candidate cannot be deleted because related records exist.';
  END IF;

  RETURN jsonb_build_object(
    'candidate_id', p_candidate_id::text,
    'can_delete', v_can,
    'reason', v_reason,
    'blockers', jsonb_build_object(
      'contracts', v_contracts,
      'contract_weeks', v_contract_weeks,
      'timesheets', v_timesheets,
      'invoice_lines', v_invoice_lines,
      'pay_batch_candidates', v_pay_batches
    )
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.candidate_delete_apply(p_candidate_id uuid, p_actor_user_id uuid, p_reason text DEFAULT null)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_elig jsonb;
  v_can boolean;
  v_deleted_rev bigint;
  v_seq bigint;
  v_before jsonb;
BEGIN
  v_elig := public.candidate_delete_eligibility(p_candidate_id);
  v_can := COALESCE((v_elig->>'can_delete')::boolean, false);

  IF v_can IS NOT TRUE THEN
    RAISE EXCEPTION '%', COALESCE(v_elig->>'reason', 'Candidate cannot be deleted');
  END IF;

  -- Lock candidate to ensure consistent delete
  SELECT jsonb_build_object(
           'id', c.id::text,
           'tms_ref', c.tms_ref,
           'display_name', c.display_name,
           'active', c.active
         )
    INTO v_before
  FROM public.candidates c
  WHERE c.id = p_candidate_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Candidate not found';
  END IF;

  -- Non-blocking dependents (verified tables)
  DELETE FROM public.candidate_job_titles cjt WHERE cjt.candidate_id = p_candidate_id;
  DELETE FROM public.rates_candidate_overrides rco WHERE rco.candidate_id = p_candidate_id;
  DELETE FROM public.legacy_eclipse_candidate_map lem WHERE lem.candidate_id = p_candidate_id;

  -- Delete candidate
  DELETE FROM public.candidates c WHERE c.id = p_candidate_id;

  -- Bump change counter and record deleted_rev
  PERFORM public._change_bump('candidates');
  SELECT acc.seq INTO v_seq
  FROM public.app_change_counters acc
  WHERE acc.entity_key = 'candidates'
  LIMIT 1;

  v_deleted_rev := COALESCE(v_seq, 0);

  INSERT INTO public.candidates_tombstones (id, deleted_rev, deleted_at)
  VALUES (p_candidate_id, v_deleted_rev, now())
  ON CONFLICT (id) DO UPDATE
    SET deleted_rev = EXCLUDED.deleted_rev,
        deleted_at  = EXCLUDED.deleted_at;

  PERFORM public._audit_insert(
    'candidates',
    p_candidate_id::text,
    'DELETE',
    v_before,
    jsonb_build_object('deleted_rev', v_deleted_rev),
    COALESCE(p_reason, 'DELETE'),
    p_actor_user_id
  );

  RETURN jsonb_build_object('deleted', true, 'candidate_id', p_candidate_id::text, 'deleted_rev', v_deleted_rev);
END;
$function$;


-- =========================================================
-- 4) NEW RPCs: Client delete eligibility + apply (tombstones + audit + change bump)
-- =========================================================

CREATE OR REPLACE FUNCTION public.client_delete_eligibility(p_client_id uuid)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_contracts int;
  v_contract_weeks int;
  v_timesheets int;
  v_invoices int;
  v_reason text := null;
  v_can boolean := true;
BEGIN
  IF p_client_id IS NULL THEN
    RAISE EXCEPTION 'client_id is required';
  END IF;

  SELECT COUNT(*) INTO v_contracts
  FROM public.contracts ct
  WHERE ct.client_id = p_client_id;

  SELECT COUNT(*) INTO v_contract_weeks
  FROM public.contract_weeks cw
  JOIN public.contracts ct ON ct.id = cw.contract_id
  WHERE ct.client_id = p_client_id;

  SELECT COUNT(*) INTO v_timesheets
  FROM public.timesheets t
  JOIN public.contracts ct ON ct.id = t.contract_id
  WHERE ct.client_id = p_client_id;

  SELECT COUNT(*) INTO v_invoices
  FROM public.invoices inv
  WHERE inv.client_id = p_client_id;

  IF v_contracts > 0 OR v_contract_weeks > 0 OR v_timesheets > 0 OR v_invoices > 0 THEN
    v_can := false;
    v_reason := 'Client cannot be deleted because related records exist.';
  END IF;

  RETURN jsonb_build_object(
    'client_id', p_client_id::text,
    'can_delete', v_can,
    'reason', v_reason,
    'blockers', jsonb_build_object(
      'contracts', v_contracts,
      'contract_weeks', v_contract_weeks,
      'timesheets', v_timesheets,
      'invoices', v_invoices
    )
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.client_delete_apply(p_client_id uuid, p_actor_user_id uuid, p_reason text DEFAULT null)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_elig jsonb;
  v_can boolean;
  v_deleted_rev bigint;
  v_seq bigint;
  v_before jsonb;
BEGIN
  v_elig := public.client_delete_eligibility(p_client_id);
  v_can := COALESCE((v_elig->>'can_delete')::boolean, false);

  IF v_can IS NOT TRUE THEN
    RAISE EXCEPTION '%', COALESCE(v_elig->>'reason', 'Client cannot be deleted');
  END IF;

  -- Lock client row
  SELECT jsonb_build_object(
           'id', c.id::text,
           'name', c.name,
           'cli_ref', c.cli_ref
         )
    INTO v_before
  FROM public.clients c
  WHERE c.id = p_client_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Client not found';
  END IF;

  -- Non-blocking dependents (verified tables)
  DELETE FROM public.client_hospitals ch WHERE ch.client_id = p_client_id;
  DELETE FROM public.client_settings cs WHERE cs.client_id = p_client_id;
  DELETE FROM public.rates_client_defaults rcd WHERE rcd.client_id = p_client_id;
  DELETE FROM public.legacy_eclipse_client_map lec WHERE lec.client_id = p_client_id;

  -- Delete client
  DELETE FROM public.clients c WHERE c.id = p_client_id;

  -- Bump change counter and record deleted_rev
  PERFORM public._change_bump('clients');
  SELECT acc.seq INTO v_seq
  FROM public.app_change_counters acc
  WHERE acc.entity_key = 'clients'
  LIMIT 1;

  v_deleted_rev := COALESCE(v_seq, 0);

  INSERT INTO public.clients_tombstones (id, deleted_rev, deleted_at)
  VALUES (p_client_id, v_deleted_rev, now())
  ON CONFLICT (id) DO UPDATE
    SET deleted_rev = EXCLUDED.deleted_rev,
        deleted_at  = EXCLUDED.deleted_at;

  PERFORM public._audit_insert(
    'clients',
    p_client_id::text,
    'DELETE',
    v_before,
    jsonb_build_object('deleted_rev', v_deleted_rev),
    COALESCE(p_reason, 'DELETE'),
    p_actor_user_id
  );

  RETURN jsonb_build_object('deleted', true, 'client_id', p_client_id::text, 'deleted_rev', v_deleted_rev);
END;
$function$;


-- =========================================================
-- 5) NEW RPC: Job Titles delete apply (category deletes descendants if safe)
--    Verified tables:
--      default_job_titles(id,label,parent_id,depth,is_role,active,created_at,updated_at,...)
--      candidate_job_titles(candidate_id,job_title_id,is_primary,...)
-- =========================================================

CREATE OR REPLACE FUNCTION public.job_titles_delete_apply(p_job_title_id uuid, p_actor_user_id uuid, p_reason text DEFAULT null)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_is_role boolean;
  v_in_use int;
  v_before jsonb;
BEGIN
  IF p_job_title_id IS NULL THEN
    RAISE EXCEPTION 'job_title_id is required';
  END IF;

  SELECT jsonb_build_object('id', djt.id::text, 'label', djt.label, 'parent_id', CASE WHEN djt.parent_id IS NULL THEN null ELSE djt.parent_id::text END, 'is_role', djt.is_role),
         djt.is_role
    INTO v_before, v_is_role
  FROM public.default_job_titles djt
  WHERE djt.id = p_job_title_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Job title not found';
  END IF;

  IF v_is_role THEN
    SELECT COUNT(*) INTO v_in_use
    FROM public.candidate_job_titles cjt
    WHERE cjt.job_title_id = p_job_title_id;

    IF v_in_use > 0 THEN
      RAISE EXCEPTION 'Candidates have been assigned to this role. You need to reassign the candidates before you can delete this role';
    END IF;

    DELETE FROM public.default_job_titles djt WHERE djt.id = p_job_title_id;

    PERFORM public._audit_insert(
      'job_titles',
      p_job_title_id::text,
      'DELETE_ROLE',
      v_before,
      jsonb_build_object('deleted', true),
      COALESCE(p_reason, 'DELETE_JOB_TITLE'),
      p_actor_user_id
    );

    RETURN jsonb_build_object('deleted', true, 'kind', 'role', 'id', p_job_title_id::text);
  END IF;

  -- Category: gather all descendants
  WITH RECURSIVE tree AS (
    SELECT d1.id, d1.parent_id, d1.is_role
    FROM public.default_job_titles d1
    WHERE d1.id = p_job_title_id
    UNION ALL
    SELECT d2.id, d2.parent_id, d2.is_role
    FROM public.default_job_titles d2
    JOIN tree t ON d2.parent_id = t.id
  ),
  descendants AS (
    SELECT id, is_role
    FROM tree
    WHERE id <> p_job_title_id
  ),
  role_desc AS (
    SELECT id
    FROM descendants
    WHERE is_role = true
  )
  SELECT COUNT(*)
    INTO v_in_use
  FROM public.candidate_job_titles cjt
  WHERE cjt.job_title_id IN (SELECT id FROM role_desc);

  IF v_in_use > 0 THEN
    RAISE EXCEPTION 'Candidates have been assigned to roles in this category. You need to reassign the candidates before you can delete this category';
  END IF;

  -- Delete descendants first, then category
  WITH RECURSIVE tree AS (
    SELECT d1.id, d1.parent_id
    FROM public.default_job_titles d1
    WHERE d1.id = p_job_title_id
    UNION ALL
    SELECT d2.id, d2.parent_id
    FROM public.default_job_titles d2
    JOIN tree t ON d2.parent_id = t.id
  )
  DELETE FROM public.default_job_titles djt
  WHERE djt.id IN (SELECT id FROM tree)
    AND djt.id <> p_job_title_id;

  DELETE FROM public.default_job_titles djt WHERE djt.id = p_job_title_id;

  PERFORM public._audit_insert(
    'job_titles',
    p_job_title_id::text,
    'DELETE_CATEGORY',
    v_before,
    jsonb_build_object('deleted', true),
    COALESCE(p_reason, 'DELETE_JOB_TITLE'),
    p_actor_user_id
  );

  RETURN jsonb_build_object('deleted', true, 'kind', 'category', 'id', p_job_title_id::text);
END;
$function$;

create or replace function public.invoice_quicksearch_ids(
  p_q text,
  p_limit integer default 20000
)
returns table(invoice_id uuid)
language sql
stable
security definer
set search_path = public
as $$
  select s.invoice_id
  from (
    select distinct on (i.id)
      i.id as invoice_id,
      coalesce(i.issued_at_utc, i.created_at) as sort_ts,
      i.invoice_no as sort_no
    from public.invoices as i
    left join public.clients as c
      on c.id = i.client_id
    left join public.invoice_lines as il
      on il.invoice_id = i.id
    left join public.v_timesheets_summary as vts
      on vts.timesheet_id = il.timesheet_id
    where
      p_q is not null
      and btrim(p_q) <> ''
      and (
        i.invoice_no ilike ('%' || p_q || '%')
        or c.name ilike ('%' || p_q || '%')
        or vts.candidate_name ilike ('%' || p_q || '%')
      )
    -- DISTINCT ON requires i.id first in ORDER BY; after that we pick the “best” row per invoice
    order by
      i.id,
      coalesce(i.issued_at_utc, i.created_at) desc nulls last,
      i.invoice_no desc
  ) as s
  order by
    s.sort_ts desc nulls last,
    s.sort_no desc
  limit greatest(1, least(coalesce(p_limit, 20000), 20000));
$$;
