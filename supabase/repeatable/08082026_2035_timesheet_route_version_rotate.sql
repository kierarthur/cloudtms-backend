-- Canonical QR/version lifecycle authority shared by office and Candidate routes.
-- Preserve the exact installed five-argument owner for the dormant flag-off path.
do $migration$
begin
  if to_regprocedure('private._timesheet_route_version_legacy_v1(uuid,uuid,text,uuid,boolean)') is null
     and to_regprocedure('public.timesheet_route_version_rotate(uuid,uuid,text,uuid,boolean)') is not null then
    alter function public.timesheet_route_version_rotate(uuid,uuid,text,uuid,boolean) set schema private;
    alter function private.timesheet_route_version_rotate(uuid,uuid,text,uuid,boolean)
      rename to _timesheet_route_version_legacy_v1;
  end if;
end;
$migration$;

CREATE OR REPLACE FUNCTION private._timesheet_route_version_core_v1(
  p_current_timesheet_id uuid,
  p_expected_timesheet_id uuid,
  p_target_action text,
  p_actor_user_id uuid,
  p_allow_manual_only boolean DEFAULT false
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_action text := upper(btrim(coalesce(p_target_action, '')));
  v_now timestamptz := now();

  v_requested_row public.timesheets%ROWTYPE;
  v_current_row public.timesheets%ROWTYPE;
  v_new_row public.timesheets%ROWTYPE;
  v_electronic_row public.timesheets%ROWTYPE;
  v_contract_week_row public.contract_weeks%ROWTYPE;
  v_tsfin_row public.timesheets_financials%ROWTYPE;
  v_revert_workflow public.candidate_submission_workflows%ROWTYPE;
  v_revert_approval_request public.candidate_approval_requests%ROWTYPE;
  v_revert_hours_component public.candidate_submission_components%ROWTYPE;
  v_revert_candidate_signature public.candidate_submission_components%ROWTYPE;
  v_revert_manager_signature public.candidate_submission_components%ROWTYPE;
  v_revert_evidence public.timesheet_evidence%ROWTYPE;
  v_revert_required_component_ids uuid[];
  v_current_financial_sha256 bytea;

  v_booking_id text;
  v_requested_timesheet_id uuid;
  v_current_timesheet_id uuid;
  v_new_timesheet_id uuid;
  v_contract_week_id uuid;
  v_contract_id uuid;
  v_client_id uuid;

  v_next_version integer;
  v_old_version integer;
  v_new_version integer;
  v_electronic_version integer;

  v_scope text;
  v_submission_mode text;
  v_qr_status text;
  v_basis text;

  v_has_qr_token boolean := false;
  v_has_qr_generated boolean := false;
  v_has_qr_scanned boolean := false;
  v_is_manual_only boolean := false;
  v_hard_locked boolean := false;
  v_has_segment_invoice_lock boolean := false;
  v_supports_electronic boolean := false;
  v_effective_mode public.submission_mode_enum;
  v_effective_policy jsonb := '{}'::jsonb;
  v_has_prior_qr_lineage boolean := false;
  v_was_stale boolean := false;

  v_is_import_authoritative boolean := false;
  v_import_authority jsonb := '{}'::jsonb;

  v_reverted boolean := false;
  v_switched boolean := false;
  v_converted boolean := false;

BEGIN
  IF p_current_timesheet_id IS NULL THEN
    RAISE EXCEPTION 'timesheet_id is required';
  END IF;

  IF p_expected_timesheet_id IS NULL THEN
    RAISE EXCEPTION 'expected_timesheet_id is required';
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'actor_user_id is required';
  END IF;

  IF v_action NOT IN (
    'CONVERT_QR_TO_MANUAL',
    'ALLOW_ELECTRONIC_AGAIN',
    'ALLOW_QR_AGAIN',
    'INVALIDATE_QR',
    'REISSUE_QR',
    'DISABLE_QR',
    'SWITCH_TO_MANUAL',
    'SWITCH_DAILY_TO_MANUAL',
    'REVERT_TO_ELECTRONIC'
  ) THEN
    RAISE EXCEPTION 'Unsupported target action: %', p_target_action;
  END IF;

  SELECT t_req.*
  INTO v_requested_row
  FROM public.timesheets AS t_req
  WHERE t_req.timesheet_id = p_current_timesheet_id
  LIMIT 1;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Timesheet not found';
  END IF;

  v_requested_timesheet_id := v_requested_row.timesheet_id;
  v_booking_id := v_requested_row.booking_id;

  IF v_booking_id IS NULL OR btrim(v_booking_id) = '' THEN
    RAISE EXCEPTION 'Timesheet booking_id is missing; cannot version';
  END IF;

  PERFORM pg_advisory_xact_lock(hashtext(v_booking_id));

  PERFORM 1
  FROM public.timesheets AS t_lock
  WHERE t_lock.booking_id = v_booking_id
  FOR UPDATE;

  SELECT t_cur.*
  INTO v_current_row
  FROM public.timesheets AS t_cur
  WHERE t_cur.booking_id = v_booking_id
    AND t_cur.is_current = true
  ORDER BY t_cur.version DESC, t_cur.updated_at DESC, t_cur.created_at DESC
  LIMIT 1;

  IF NOT FOUND THEN
    IF coalesce(v_requested_row.is_current, false) = true THEN
      v_current_row := v_requested_row;
    ELSE
      RAISE EXCEPTION 'Timesheet not found';
    END IF;
  END IF;

  v_current_timesheet_id := v_current_row.timesheet_id;
  v_was_stale := (v_requested_timesheet_id IS DISTINCT FROM v_current_timesheet_id);

  IF v_current_timesheet_id IS DISTINCT FROM p_expected_timesheet_id THEN
    RAISE EXCEPTION 'TIMESHEET_MOVED'
      USING DETAIL = jsonb_build_object(
        'current_timesheet_id', v_current_timesheet_id::text
      )::text;
  END IF;

  v_old_version := coalesce(v_current_row.version, 1);
  v_scope := upper(coalesce(v_current_row.sheet_scope::text, ''));
  v_submission_mode := upper(coalesce(v_current_row.submission_mode::text, ''));
  v_qr_status := upper(coalesce(v_current_row.qr_status::text, ''));

  -- The coordinated Candidate route authority never creates or replaces a
  -- DAILY paper route.  The dormant flag-off wrapper still delegates to the
  -- exact legacy owner, preserving existing office behaviour until cutover.
  IF v_scope = 'DAILY'
     AND v_action IN ('ALLOW_QR_AGAIN', 'INVALIDATE_QR', 'REISSUE_QR') THEN
    RAISE EXCEPTION 'DAILY_PAPER_ROUTE_NOT_ALLOWED'
      USING ERRCODE='55000', DETAIL=jsonb_build_object(
        'code','DAILY_PAPER_ROUTE_NOT_ALLOWED',
        'sheet_scope',v_scope,
        'action',v_action
      )::text;
  END IF;

  IF v_current_row.archived_at_utc IS NOT NULL THEN
    RAISE EXCEPTION 'TIMESHEET_ARCHIVED_EDIT_BLOCKED: Archived timesheets cannot rotate submission route';
  END IF;

  SELECT cw_cur.*
  INTO v_contract_week_row
  FROM public.contract_weeks AS cw_cur
  WHERE cw_cur.timesheet_id = v_current_timesheet_id
  ORDER BY cw_cur.updated_at DESC, cw_cur.created_at DESC
  LIMIT 1;

  IF FOUND THEN
    v_contract_week_id := v_contract_week_row.id;
    PERFORM 1
    FROM public.contract_weeks AS cw_lock
    WHERE cw_lock.id = v_contract_week_id
    FOR UPDATE;
  ELSE
    v_contract_week_id := NULL;
  END IF;

  SELECT tf_cur.*
  INTO v_tsfin_row
  FROM public.timesheets_financials AS tf_cur
  WHERE tf_cur.timesheet_id = v_current_timesheet_id
    AND tf_cur.is_current = true
  ORDER BY tf_cur.updated_at DESC, tf_cur.created_at DESC
  LIMIT 1;

  IF v_tsfin_row.id IS NOT NULL THEN
    PERFORM 1
    FROM public.timesheets_financials AS tf_lock
    WHERE tf_lock.id = v_tsfin_row.id
    FOR UPDATE;
  END IF;

  v_contract_id := coalesce(v_contract_week_row.contract_id, v_current_row.contract_id);
  v_client_id := v_tsfin_row.client_id;

  IF v_contract_id IS NOT NULL THEN
    SELECT coalesce(c.client_id, v_client_id)
    INTO v_client_id
    FROM public.contracts AS c
    WHERE c.id = v_contract_id
    LIMIT 1;
  END IF;

  IF v_client_id IS NOT NULL THEN
    v_import_authority := private._candidate_import_authoritative_v1(
      v_client_id,
      v_contract_id,
      v_current_timesheet_id,
      CASE WHEN v_tsfin_row.id IS NULL THEN NULL ELSE to_jsonb(v_tsfin_row) END,
      v_current_row.week_ending_date
    );
    v_is_import_authoritative := coalesce(
      (v_import_authority ->> 'is_import_authoritative')::boolean,
      false
    );
  END IF;

  IF v_is_import_authoritative THEN
    RAISE EXCEPTION 'Import-authoritative timesheets (NHSP / HealthRoster weekly no-timesheets) cannot change submission route';
  END IF;

  -- Route/version rotation invalidates the evidence and signature lineage that
  -- supported the current lifecycle.  It is never an alternative
  -- unauthorisation implementation: callers must compose the existing
  -- CloudTMS unauthorise authority first.
  IF v_current_row.authorised_at_server IS NOT NULL
     OR v_tsfin_row.authorised_at_utc IS NOT NULL THEN
    RAISE EXCEPTION 'TIMESHEET_AUTHORISED_EDIT_BLOCKED: This timesheet is authorised. Unauthorise it before changing its submission route.';
  END IF;

  IF v_action IN ('CONVERT_QR_TO_MANUAL', 'DISABLE_QR') THEN
    IF (
      v_current_row.authorised_at_server IS NOT NULL
      AND btrim(v_current_row.authorised_at_server::text) <> ''
      AND (v_current_row.revoked_at IS NULL OR btrim(v_current_row.revoked_at::text) = '')
    ) THEN
      RAISE EXCEPTION 'TIMESHEET_AUTHORISED_EDIT_BLOCKED: This timesheet is authorised. Unauthorise it before converting QR to manual.';
    END IF;

    IF v_scope <> '' AND v_scope NOT IN ('WEEKLY', 'DAILY') THEN
      RAISE EXCEPTION 'Unsupported sheet_scope for QR conversion';
    END IF;

    IF v_submission_mode <> 'MANUAL' THEN
      RAISE EXCEPTION 'Timesheet is not in MANUAL mode; QR conversion not applicable';
    END IF;

    IF v_qr_status = '' THEN
      RAISE EXCEPTION 'Timesheet does not have QR metadata; nothing to convert';
    END IF;

    IF v_tsfin_row.id IS NULL THEN
      RAISE EXCEPTION 'No financial snapshot for timesheet';
    END IF;

    v_hard_locked := (
      coalesce(v_tsfin_row.paid_at_utc IS NOT NULL, false)
      OR coalesce(v_tsfin_row.locked_by_invoice_id IS NOT NULL, false)
                      );

    IF v_hard_locked THEN
      RAISE EXCEPTION 'Cannot convert QR: timesheet already invoiced or paid';
    END IF;
  ELSIF v_action = 'ALLOW_ELECTRONIC_AGAIN' THEN
    IF v_current_row.is_adjustment = true OR coalesce(v_contract_week_row.is_adjustment, false) = true THEN
      RAISE EXCEPTION 'Adjustment timesheets cannot be converted to electronic submission';
    END IF;

    v_hard_locked := (
      coalesce(v_tsfin_row.locked_by_invoice_id IS NOT NULL, false)
      OR coalesce(v_tsfin_row.paid_at_utc IS NOT NULL, false)
    );
    IF v_hard_locked THEN
      RAISE EXCEPTION 'Cannot allow electronic again: timesheet is invoiced/locked or paid';
    END IF;

    v_has_qr_token := (btrim(coalesce(v_current_row.qr_token, '')) <> '');
    v_has_qr_generated := (v_current_row.qr_generated_at IS NOT NULL);
    v_has_qr_scanned := (v_current_row.qr_scanned_at IS NOT NULL);
    v_is_manual_only := (
      v_submission_mode = 'MANUAL'
      AND v_qr_status = ''
      AND v_has_qr_token = false
      AND v_has_qr_generated = false
      AND v_has_qr_scanned = false
    );

    IF v_is_manual_only = false THEN
      RAISE EXCEPTION 'Allow electronic again is only valid for manual-only timesheets';
    END IF;

    IF v_client_id IS NULL AND v_contract_id IS NOT NULL THEN
      BEGIN
        SELECT c_client.client_id
        INTO v_client_id
        FROM public.contracts AS c_client
        WHERE c_client.id = v_contract_id
        LIMIT 1;
      EXCEPTION WHEN OTHERS THEN
        v_client_id := NULL;
      END;
    END IF;

    IF v_client_id IS NULL THEN
      RAISE EXCEPTION 'Cannot resolve client_id for electronic eligibility check';
    END IF;

    v_effective_mode := private._candidate_submission_mode_v1(
      v_client_id,
      v_contract_id,
      coalesce(v_current_row.week_ending_date,
        (current_timestamp at time zone 'Europe/London')::date)
    );
    v_supports_electronic := v_effective_mode='ELECTRONIC'::public.submission_mode_enum;

    IF coalesce(v_supports_electronic, false) = false THEN
      RAISE EXCEPTION 'Client does not support electronic submission';
    END IF;
  ELSIF v_action IN ('ALLOW_QR_AGAIN', 'INVALIDATE_QR', 'REISSUE_QR') THEN
    IF v_current_row.is_adjustment = true THEN
      RAISE EXCEPTION 'Adjustment timesheets cannot be converted to QR submission';
    END IF;

    v_hard_locked := (
      coalesce(v_tsfin_row.locked_by_invoice_id IS NOT NULL, false)
      OR coalesce(v_tsfin_row.paid_at_utc IS NOT NULL, false)
    );
    IF v_hard_locked THEN
      RAISE EXCEPTION 'Cannot allow QR again: timesheet is invoiced/locked or paid';
    END IF;

    v_has_qr_token := (btrim(coalesce(v_current_row.qr_token, '')) <> '');
    v_has_qr_generated := (v_current_row.qr_generated_at IS NOT NULL);
    v_has_qr_scanned := (v_current_row.qr_scanned_at IS NOT NULL);
    v_is_manual_only := (
      v_submission_mode = 'MANUAL'
      AND v_qr_status = ''
      AND v_has_qr_token = false
      AND v_has_qr_generated = false
      AND v_has_qr_scanned = false
    );

    IF v_action = 'ALLOW_QR_AGAIN' AND v_is_manual_only = false THEN
      RAISE EXCEPTION 'Allow QR again is only valid for manual-only timesheets';
    END IF;
    IF v_action = 'ALLOW_QR_AGAIN' THEN
      SELECT EXISTS (
        SELECT 1
        FROM public.timesheets qr_history
        WHERE qr_history.booking_id=v_booking_id
          AND (
            qr_history.qr_status IS NOT NULL
            OR nullif(btrim(coalesce(qr_history.qr_token,'')),'') IS NOT NULL
            OR qr_history.qr_generated_at IS NOT NULL
            OR qr_history.qr_scanned_at IS NOT NULL
            OR nullif(btrim(coalesce(qr_history.qr_r2_key,'')),'') IS NOT NULL
            OR qr_history.qr_signed_at_utc IS NOT NULL
          )
      ) INTO v_has_prior_qr_lineage;
      IF v_client_id IS NULL OR v_contract_id IS NULL THEN
        RAISE EXCEPTION 'Cannot resolve effective paper submission policy';
      END IF;
      v_effective_policy:=private._candidate_policy_resolve_v1(
        v_client_id,v_contract_id,
        coalesce(v_current_row.week_ending_date,
          (current_timestamp at time zone 'Europe/London')::date)
      );
      IF NOT v_has_prior_qr_lineage
         AND NOT coalesce((v_effective_policy->>'paper_submission_enabled')::boolean,false) THEN
        RAISE EXCEPTION 'ALLOW_QR_AGAIN_REQUIRES_PRIOR_LINEAGE_OR_PAPER_PERMISSION'
          USING ERRCODE='55000';
      END IF;
    END IF;
    IF v_action IN ('INVALIDATE_QR', 'REISSUE_QR')
       AND v_qr_status = '' AND v_has_qr_token = false
       AND v_has_qr_generated = false AND v_has_qr_scanned = false THEN
      RAISE EXCEPTION 'QR invalidation/reissue requires an actual QR-backed current timesheet';
    END IF;
  ELSIF v_action = 'SWITCH_TO_MANUAL' THEN
    IF (
      v_current_row.authorised_at_server IS NOT NULL
      AND btrim(v_current_row.authorised_at_server::text) <> ''
      AND (v_current_row.revoked_at IS NULL OR btrim(v_current_row.revoked_at::text) = '')
    ) THEN
      RAISE EXCEPTION 'TIMESHEET_AUTHORISED_EDIT_BLOCKED: This timesheet is authorised. Unauthorise it before switching to manual.';
    END IF;

    IF v_scope <> 'WEEKLY' THEN
      RAISE EXCEPTION 'Only WEEKLY timesheets can be switched to manual';
    END IF;

    IF NOT (
      v_submission_mode = 'ELECTRONIC'
      OR (
        v_submission_mode = 'MANUAL'
        AND upper(coalesce(v_contract_week_row.submission_mode_snapshot::text,''))='ELECTRONIC'
      )
    ) THEN
      RAISE EXCEPTION 'Timesheet is not an electronic weekly timesheet';
    END IF;

    IF v_contract_week_row.id IS NULL THEN
      RAISE EXCEPTION 'Timesheet not linked to a contract week';
    END IF;

    IF v_tsfin_row.id IS NULL THEN
      RAISE EXCEPTION 'No financial snapshot to switch';
    END IF;

    v_hard_locked := (
      coalesce(v_tsfin_row.paid_at_utc IS NOT NULL, false)
      OR coalesce(v_tsfin_row.locked_by_invoice_id IS NOT NULL, false)
                      );
    IF v_hard_locked THEN
      RAISE EXCEPTION 'Cannot switch: timesheet already invoiced or paid';
    END IF;

    v_basis := upper(coalesce(v_tsfin_row.basis::text, ''));
    IF v_basis <> 'CONTRACT_WEEKLY' THEN
      RAISE EXCEPTION 'Only CONTRACT_WEEKLY timesheets can be switched to manual';
    END IF;
  ELSIF v_action = 'SWITCH_DAILY_TO_MANUAL' THEN
    IF (
      v_current_row.authorised_at_server IS NOT NULL
      AND btrim(v_current_row.authorised_at_server::text) <> ''
      AND (v_current_row.revoked_at IS NULL OR btrim(v_current_row.revoked_at::text) = '')
    ) THEN
      RAISE EXCEPTION 'TIMESHEET_AUTHORISED_EDIT_BLOCKED: This timesheet is authorised. Unauthorise it before switching to manual.';
    END IF;

    IF v_booking_id IS NULL
      OR btrim(v_booking_id) = ''
      OR btrim(v_booking_id) = '{}'
      OR lower(btrim(v_booking_id)) = 'null'
      OR lower(btrim(v_booking_id)) = 'undefined'
    THEN
      RAISE EXCEPTION 'Timesheet booking_id is invalid; cannot rotate versions. Please repair booking_id.';
    END IF;

    IF v_scope <> 'DAILY' THEN
      RAISE EXCEPTION 'Only DAILY timesheets can be switched to manual via this endpoint';
    END IF;

    IF NOT (
      v_submission_mode = 'ELECTRONIC'
      OR (
        v_submission_mode = 'MANUAL'
        AND v_current_row.candidate_submission_route_intent='ELECTRONIC'
      )
    ) THEN
      RAISE EXCEPTION 'Timesheet is not an electronic daily timesheet';
    END IF;

    IF v_tsfin_row.id IS NULL THEN
      RAISE EXCEPTION 'No financial snapshot to switch';
    END IF;

    v_hard_locked := (
      coalesce(v_tsfin_row.paid_at_utc IS NOT NULL, false)
      OR coalesce(v_tsfin_row.locked_by_invoice_id IS NOT NULL, false)
                      );
    IF v_hard_locked THEN
      RAISE EXCEPTION 'Cannot switch: timesheet already invoiced or paid';
    END IF;

    v_basis := upper(coalesce(v_tsfin_row.basis::text, ''));
    IF v_basis IN ('NHSP', 'NHSP_ADJUSTMENT', 'HEALTHROSTER_SELF_BILL', 'HEALTHROSTER_ADJUSTMENT') THEN
      RAISE EXCEPTION 'Cannot switch NHSP/HR-based daily timesheets to manual';
    END IF;
  ELSIF v_action = 'REVERT_TO_ELECTRONIC' THEN
    IF v_current_row.is_adjustment = true THEN
      RAISE EXCEPTION 'Adjustment timesheets cannot be reverted/converted to electronic submission';
    END IF;

    SELECT t_elec.*
    INTO v_electronic_row
    FROM public.timesheets AS t_elec
    WHERE t_elec.booking_id = v_booking_id
      AND upper(coalesce(t_elec.submission_mode::text, '')) = 'ELECTRONIC'
    ORDER BY t_elec.version DESC, t_elec.updated_at DESC, t_elec.created_at DESC
    LIMIT 1
    FOR UPDATE;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'No electronic version exists for this timesheet';
    END IF;

    IF v_tsfin_row.id IS NOT NULL THEN
      v_has_segment_invoice_lock := EXISTS (
        SELECT 1
        FROM jsonb_array_elements(
          CASE
            WHEN v_tsfin_row.invoice_breakdown_json IS NOT NULL
             AND jsonb_typeof(v_tsfin_row.invoice_breakdown_json) = 'object'
             AND jsonb_typeof(v_tsfin_row.invoice_breakdown_json -> 'segments') = 'array'
            THEN v_tsfin_row.invoice_breakdown_json -> 'segments'
            ELSE '[]'::jsonb
          END
        ) AS s(seg)
        WHERE nullif(btrim(coalesce(s.seg ->> 'invoice_locked_invoice_id', '')), '') IS NOT NULL
      );

      v_hard_locked := (
        coalesce(v_tsfin_row.paid_at_utc IS NOT NULL, false)
        OR coalesce(v_tsfin_row.locked_by_invoice_id IS NOT NULL, false)
                                OR v_has_segment_invoice_lock
      );

      IF v_hard_locked THEN
        RAISE EXCEPTION 'Cannot revert: current timesheet is invoiced or paid';
      END IF;
    END IF;

    v_has_qr_token := (btrim(coalesce(v_current_row.qr_token, '')) <> '');
    v_has_qr_generated := (v_current_row.qr_generated_at IS NOT NULL);
    v_has_qr_scanned := (v_current_row.qr_scanned_at IS NOT NULL);
    v_is_manual_only := (
      v_submission_mode = 'MANUAL'
      AND v_qr_status = ''
      AND v_has_qr_token = false
      AND v_has_qr_generated = false
      AND v_has_qr_scanned = false
    );

    IF v_is_manual_only = true AND coalesce(p_allow_manual_only, false) = false THEN
      RAISE EXCEPTION 'Cannot revert: current is manual-only (set allow_manual_only=true to override)';
    END IF;

    IF v_current_timesheet_id = v_electronic_row.timesheet_id AND coalesce(v_current_row.is_current, false) = true THEN
      RETURN jsonb_build_object(
        'ok', true,
        'action', v_action,
        'reverted', false,
        'reason', 'Electronic version is already current',
        'booking_id', v_booking_id,
        'current_timesheet_id', v_current_timesheet_id::text,
        'current_version', coalesce(v_electronic_row.version, 1),
        'requested_timesheet_id', v_requested_timesheet_id::text,
        'was_stale', v_was_stale
      );
    END IF;
  END IF;

  SELECT coalesce(max(t_ver.version), 0) + 1
  INTO v_next_version
  FROM public.timesheets AS t_ver
  WHERE t_ver.booking_id = v_booking_id;

  IF v_action <> 'REVERT_TO_ELECTRONIC' THEN
    UPDATE public.timesheets AS t_demote
    SET
      is_current = false,
      status = 'REVOKED'::public.timesheet_status_enum,
      revoked_at = v_now,
      revoked_reason = CASE
        WHEN v_action IN ('CONVERT_QR_TO_MANUAL', 'DISABLE_QR') THEN 'QR_CONVERTED_TO_MANUAL'
        WHEN v_action = 'ALLOW_ELECTRONIC_AGAIN' THEN 'ALLOW_ELECTRONIC_AGAIN'
        WHEN v_action = 'ALLOW_QR_AGAIN' THEN 'ALLOW_QR_AGAIN'
        WHEN v_action IN ('INVALIDATE_QR', 'REISSUE_QR') THEN 'QR_INVALIDATED_AFTER_CONTENT_CHANGE'
        WHEN v_action IN ('SWITCH_TO_MANUAL', 'SWITCH_DAILY_TO_MANUAL') THEN 'SWITCHED_TO_MANUAL'
        ELSE t_demote.revoked_reason
      END,
      revoked_by = CASE
        WHEN v_action IN ('CONVERT_QR_TO_MANUAL', 'DISABLE_QR', 'ALLOW_ELECTRONIC_AGAIN', 'ALLOW_QR_AGAIN', 'INVALIDATE_QR', 'REISSUE_QR', 'SWITCH_TO_MANUAL', 'SWITCH_DAILY_TO_MANUAL')
        THEN p_actor_user_id::text
        ELSE t_demote.revoked_by
      END,
      updated_at = v_now
    WHERE t_demote.booking_id = v_booking_id
      AND t_demote.is_current = true;

    -- Explicit route-version field matrix. Business facts are copied; every
    -- signature, QR token, generated/manual document and authorisation fact is
    -- deliberately cleared so a new route can never inherit stale evidence.
    INSERT INTO public.timesheets (
      timesheet_id, booking_id, occupant_key_norm, hospital_norm, ward_norm,
      job_title_norm, shift_label_norm, scheduled_start_iso, scheduled_end_iso,
      worked_start_iso, worked_end_iso, break_start_iso, break_end_iso,
      break_minutes, worked_minutes, week_ending_date, auth_name,
      auth_job_title, authorised_at_server, r2_nurse_key, r2_auth_key,
      img_sha256_nurse, img_sha256_auth, reference_number, reference_set_at,
      status, idempotency_key, client_hash, client_ua, created_at, updated_at,
      version, is_current, revoked_at, revoked_reason, revoked_by, contract_id,
      submission_mode, manual_pdf_r2_key, line_type, sheet_scope,
      actual_schedule_json, additional_units_week, additional_units_per_day,
      qr_token, qr_status, qr_payload_json, qr_generated_at, qr_scanned_at,
      qr_scan_info_json, qr_r2_key, day_references_json,
      manual_pdf_rotation_degrees, qr_last_sent_hash, qr_last_sent_at_utc,
      qr_signed_hash, qr_signed_at_utc, candidate_hint_text, band,
      generated_pdf_at_utc, is_adjustment, parent_timesheet_id,
      generated_pdf_refs_sig, generated_pdf_refs_snapshot_json,
      generated_pdf_refs_captured_at_utc, qr_sent_refs_sig,
      qr_sent_refs_snapshot_json, qr_sent_refs_captured_at_utc, correction_id,
      correction_kind, adjustment_origin, archived_at_utc,
      archived_by_user_id, archived_reason_code, document_revision,
      document_state, current_document_version_id,
      active_document_operation_id, manual_document_asset_id,
      last_document_error_json, candidate_workflow_id,
      candidate_workflow_generation, candidate_manager_approved_at_utc,
      candidate_submission_route_intent
    )
    SELECT
      gen_random_uuid(), v_booking_id, v_current_row.occupant_key_norm,
      v_current_row.hospital_norm, v_current_row.ward_norm,
      v_current_row.job_title_norm, v_current_row.shift_label_norm,
      v_current_row.scheduled_start_iso, v_current_row.scheduled_end_iso,
      v_current_row.worked_start_iso, v_current_row.worked_end_iso,
      v_current_row.break_start_iso, v_current_row.break_end_iso,
      v_current_row.break_minutes, v_current_row.worked_minutes,
      v_current_row.week_ending_date, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
      v_current_row.reference_number, v_current_row.reference_set_at,
      'RECEIVED'::public.timesheet_status_enum, NULL, NULL, NULL, v_now, v_now,
      v_next_version, true, NULL, NULL, NULL, v_current_row.contract_id,
      -- A canonical ELECTRONIC row is forbidden until both signatures exist.
      -- Fresh electronic resubmission is represented by a clean unsigned row
      -- plus the authoritative ELECTRONIC contract-week route snapshot.  The
      -- existing WEEKLY finalisation authority sets ELECTRONIC atomically with
      -- the complete candidate/manager signature pair.
      'MANUAL'::public.submission_mode_enum,
      NULL, v_current_row.line_type, v_current_row.sheet_scope,
      v_current_row.actual_schedule_json,
      coalesce(v_current_row.additional_units_week, '{}'::jsonb),
      coalesce(v_current_row.additional_units_per_day, '{}'::jsonb),
      NULL,
      CASE WHEN v_action IN ('ALLOW_QR_AGAIN', 'INVALIDATE_QR', 'REISSUE_QR')
        THEN 'PENDING'::public.timesheet_qr_status_enum ELSE NULL END,
      '{}'::jsonb, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL, NULL,
      v_current_row.candidate_hint_text, v_current_row.band, NULL,
      v_current_row.is_adjustment, v_current_row.parent_timesheet_id,
      NULL, NULL, NULL, NULL, NULL, NULL, v_current_row.correction_id,
      v_current_row.correction_kind, v_current_row.adjustment_origin,
      NULL, NULL, NULL, 1, 'NOT_REQUESTED', NULL, NULL, NULL, NULL,
      NULL, NULL, NULL,
      case
        when v_action='ALLOW_ELECTRONIC_AGAIN' and v_scope='DAILY' then 'ELECTRONIC'
        when v_action in ('ALLOW_QR_AGAIN','INVALIDATE_QR','REISSUE_QR')
          and v_scope='WEEKLY' then 'PAPER'
        else null
      end
    RETURNING * INTO v_new_row;

    v_new_timesheet_id := v_new_row.timesheet_id;
    v_new_version := v_new_row.version;

    IF v_new_timesheet_id IS NULL THEN
      RAISE EXCEPTION 'Insert succeeded but no timesheet_id returned';
    END IF;

    IF v_action = 'SWITCH_TO_MANUAL' THEN
      IF v_contract_week_row.id IS NULL THEN
        RAISE EXCEPTION 'Timesheet not linked to a contract week';
      END IF;

      UPDATE public.contract_weeks AS cw_upd
      SET
        timesheet_id = v_new_timesheet_id,
        submission_mode_snapshot = 'MANUAL'::public.submission_mode_enum,
        updated_at = v_now
      WHERE cw_upd.id = v_contract_week_row.id;
    ELSIF v_action IN ('CONVERT_QR_TO_MANUAL', 'DISABLE_QR', 'ALLOW_ELECTRONIC_AGAIN', 'ALLOW_QR_AGAIN', 'INVALIDATE_QR', 'REISSUE_QR') THEN
      IF v_contract_week_row.id IS NOT NULL THEN
        UPDATE public.contract_weeks AS cw_move
        SET
          timesheet_id = v_new_timesheet_id,
          submission_mode_snapshot = CASE
            WHEN v_action = 'ALLOW_ELECTRONIC_AGAIN'
              THEN 'ELECTRONIC'::public.submission_mode_enum
            ELSE 'MANUAL'::public.submission_mode_enum
          END,
          updated_at = v_now
        WHERE cw_move.id = v_contract_week_row.id;
      END IF;
    END IF;

    IF v_action IN ('CONVERT_QR_TO_MANUAL', 'DISABLE_QR', 'SWITCH_TO_MANUAL', 'SWITCH_DAILY_TO_MANUAL') THEN
      IF v_tsfin_row.id IS NULL THEN
        RAISE EXCEPTION 'No financial snapshot to switch';
      END IF;

      UPDATE public.timesheets_financials AS tf_upd
      SET
        timesheet_id = v_new_timesheet_id,
        timesheet_version = coalesce(v_new_row.version, v_next_version),
        processing_status = CASE
          WHEN v_action IN ('CONVERT_QR_TO_MANUAL', 'DISABLE_QR') THEN
            CASE WHEN tf_upd.processing_status = 'UNPROCESSED'::public.ts_fin_processing_status_enum
              THEN 'UNPROCESSED'::public.ts_fin_processing_status_enum
              ELSE 'AWAITING_MANUAL_SIGNATURE'::public.ts_fin_processing_status_enum END
          ELSE tf_upd.processing_status
        END,
        authorised_by_user_id = NULL,
        authorised_at_utc = NULL,
        updated_at = v_now
      WHERE tf_upd.id = v_tsfin_row.id;
    ELSIF v_action IN ('ALLOW_ELECTRONIC_AGAIN', 'ALLOW_QR_AGAIN', 'INVALIDATE_QR', 'REISSUE_QR') THEN
      IF v_tsfin_row.id IS NOT NULL THEN
        UPDATE public.timesheets_financials AS tf_upd
        SET
          timesheet_id = v_new_timesheet_id,
          timesheet_version = coalesce(v_new_row.version, v_next_version),
          processing_status = CASE
            WHEN v_action = 'ALLOW_ELECTRONIC_AGAIN' THEN 'UNASSIGNED'::public.ts_fin_processing_status_enum
            WHEN v_action IN ('ALLOW_QR_AGAIN', 'INVALIDATE_QR', 'REISSUE_QR') THEN
              CASE WHEN tf_upd.processing_status = 'UNPROCESSED'::public.ts_fin_processing_status_enum
                THEN 'UNPROCESSED'::public.ts_fin_processing_status_enum
                ELSE 'AWAITING_MANUAL_SIGNATURE'::public.ts_fin_processing_status_enum END
            ELSE tf_upd.processing_status
          END,
          -- Defensive only: the central precondition above proves these are
          -- already null.  Keeping the new generation explicit prevents stale
          -- lifecycle values reappearing if this branch changes later.
          authorised_by_user_id = NULL,
          authorised_at_utc = NULL,
          updated_at = v_now
        WHERE tf_upd.id = v_tsfin_row.id;
      END IF;
    END IF;

    IF v_action IN ('CONVERT_QR_TO_MANUAL', 'DISABLE_QR') THEN
      v_converted := true;
    ELSE
      v_switched := true;
    END IF;

    RETURN jsonb_build_object(
      'ok', true,
      'action', v_action,
      'converted', v_converted,
      'switched', v_switched,
      'reverted', false,
      'booking_id', v_booking_id,
      'contract_week_id', CASE WHEN v_contract_week_row.id IS NULL THEN NULL ELSE v_contract_week_row.id::text END,
      'old_timesheet_id', v_current_timesheet_id::text,
      'new_timesheet_id', v_new_timesheet_id::text,
      'current_timesheet_id', v_new_timesheet_id::text,
      'old_version', v_old_version,
      'new_version', v_new_version,
      'new_submission_mode', CASE
        WHEN v_action = 'ALLOW_ELECTRONIC_AGAIN' THEN 'ELECTRONIC'
        ELSE 'MANUAL'
      END,
      'has_electronic_original', CASE WHEN v_action IN ('SWITCH_TO_MANUAL', 'SWITCH_DAILY_TO_MANUAL') THEN true ELSE NULL END,
      'electronic_version', CASE WHEN v_action IN ('SWITCH_TO_MANUAL', 'SWITCH_DAILY_TO_MANUAL') THEN v_old_version ELSE NULL END,
      'requested_timesheet_id', v_requested_timesheet_id::text,
      'was_stale', v_was_stale
    );
  ELSE
    IF ROW(
      v_current_row.contract_id, v_current_row.sheet_scope, v_current_row.line_type,
      v_current_row.occupant_key_norm, v_current_row.hospital_norm,
      v_current_row.ward_norm, v_current_row.job_title_norm, v_current_row.band,
      v_current_row.scheduled_start_iso, v_current_row.scheduled_end_iso,
      v_current_row.worked_start_iso, v_current_row.worked_end_iso,
      v_current_row.break_start_iso, v_current_row.break_end_iso,
      v_current_row.break_minutes, v_current_row.worked_minutes,
      v_current_row.week_ending_date, v_current_row.actual_schedule_json,
      v_current_row.additional_units_week, v_current_row.additional_units_per_day,
      v_current_row.day_references_json,
      v_current_row.reference_number, v_current_row.is_adjustment,
      v_current_row.parent_timesheet_id, v_current_row.correction_id,
      v_current_row.correction_kind, v_current_row.adjustment_origin
    ) IS DISTINCT FROM ROW(
      v_electronic_row.contract_id, v_electronic_row.sheet_scope,
      v_electronic_row.line_type, v_electronic_row.occupant_key_norm,
      v_electronic_row.hospital_norm, v_electronic_row.ward_norm,
      v_electronic_row.job_title_norm, v_electronic_row.band,
      v_electronic_row.scheduled_start_iso, v_electronic_row.scheduled_end_iso,
      v_electronic_row.worked_start_iso, v_electronic_row.worked_end_iso,
      v_electronic_row.break_start_iso, v_electronic_row.break_end_iso,
      v_electronic_row.break_minutes, v_electronic_row.worked_minutes,
      v_electronic_row.week_ending_date, v_electronic_row.actual_schedule_json,
      v_electronic_row.additional_units_week,
      v_electronic_row.additional_units_per_day,
      v_electronic_row.day_references_json,
      v_electronic_row.reference_number, v_electronic_row.is_adjustment,
      v_electronic_row.parent_timesheet_id, v_electronic_row.correction_id,
      v_electronic_row.correction_kind, v_electronic_row.adjustment_origin
    ) THEN
      RAISE EXCEPTION 'TIMESHEET_REVERT_CONTENT_MISMATCH: A changed route must create a new electronic approval generation';
    END IF;

    IF v_electronic_row.candidate_workflow_id IS NULL
       OR v_electronic_row.candidate_workflow_generation IS NULL
       OR nullif(btrim(coalesce(v_electronic_row.r2_nurse_key, '')), '') IS NULL
       OR nullif(btrim(coalesce(v_electronic_row.r2_auth_key, '')), '') IS NULL
       OR nullif(btrim(coalesce(v_electronic_row.img_sha256_nurse, '')), '') IS NULL
       OR nullif(btrim(coalesce(v_electronic_row.img_sha256_auth, '')), '') IS NULL THEN
      RAISE EXCEPTION 'TIMESHEET_REVERT_PROOF_INCOMPLETE: A new electronic approval generation is required';
    END IF;

    SELECT workflow.* INTO v_revert_workflow
    FROM public.candidate_submission_workflows workflow
    WHERE workflow.id=v_electronic_row.candidate_workflow_id
    FOR UPDATE;
    IF NOT FOUND OR v_revert_workflow.state<>'FINALISED'
       OR v_revert_workflow.canonical_financial_sha256 IS NULL
       OR v_revert_workflow.candidate_signature_component_id IS NULL
       OR v_revert_workflow.manager_signature_component_id IS NULL THEN
      RAISE EXCEPTION 'TIMESHEET_REVERT_PROOF_INCOMPLETE: A new electronic approval generation is required';
    END IF;

    SELECT request_row.* INTO v_revert_approval_request
    FROM public.candidate_approval_requests request_row
    WHERE request_row.workflow_id=v_revert_workflow.id
      AND request_row.workflow_generation=v_electronic_row.candidate_workflow_generation
      AND request_row.state IN ('APPROVED','SUPERSEDED')
      AND request_row.approved_at_utc IS NOT NULL
    ORDER BY request_row.approved_at_utc DESC,
      request_row.request_generation DESC,request_row.created_at_utc DESC
    LIMIT 1
    FOR UPDATE;
    IF NOT FOUND
       OR v_revert_approval_request.review_manifest_sha256 IS NULL
       OR v_revert_approval_request.review_manifest_sha256
            IS DISTINCT FROM v_revert_workflow.review_manifest_sha256
       OR cardinality(v_revert_approval_request.required_component_ids)=0
       OR jsonb_array_length(v_revert_approval_request.required_component_manifest_json)
            <> cardinality(v_revert_approval_request.required_component_ids)
       OR cardinality(v_revert_approval_request.required_component_ids)
            <> (SELECT count(DISTINCT expected_id)
                FROM unnest(v_revert_approval_request.required_component_ids) expected_id)
       OR EXISTS(
         SELECT 1
         FROM unnest(v_revert_approval_request.required_component_ids) expected_id
         WHERE NOT EXISTS(
           SELECT 1
           FROM jsonb_array_elements(
             v_revert_approval_request.required_component_manifest_json
           ) manifest_item
           WHERE manifest_item->>'component_id'=expected_id::text
         )
       )
       OR EXISTS(
         SELECT 1
         FROM jsonb_array_elements(
           v_revert_approval_request.required_component_manifest_json
         ) manifest_item
         WHERE NOT (manifest_item->>'component_id'=ANY(
           SELECT expected_id::text
           FROM unnest(v_revert_approval_request.required_component_ids) expected_id
         ))
       ) THEN
      RAISE EXCEPTION 'TIMESHEET_REVERT_APPROVED_MANIFEST_INCOMPLETE: A new electronic approval generation is required';
    END IF;
    v_revert_required_component_ids:=v_revert_approval_request.required_component_ids;

    SELECT component.* INTO v_revert_hours_component
    FROM public.candidate_submission_components component
    WHERE component.id=v_revert_approval_request.manager_review_timesheet_component_id
      AND component.id=ANY(v_revert_required_component_ids)
      AND component.workflow_id=v_revert_workflow.id
      AND component.workflow_generation=v_electronic_row.candidate_workflow_generation
      AND component.component_kind='HOURS_TIMESHEET'
      AND component.required=true
      AND component.state='IMMUTABLE'
      AND component.final_signed_render_state='READY'
      AND component.final_signed_storage_key IS NOT NULL
      AND component.final_signed_content_sha256 IS NOT NULL
      AND component.final_signed_render_input_sha256 IS NOT NULL
      AND component.review_render_input_sha256=component.final_signed_render_input_sha256
    ORDER BY component.review_ordinal,component.id
    LIMIT 1
    FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'TIMESHEET_REVERT_DOCUMENT_LINEAGE_INCOMPLETE: A new electronic approval generation is required';
    END IF;

    SELECT component.* INTO v_revert_candidate_signature
    FROM public.candidate_submission_components component
    WHERE component.id=v_revert_workflow.candidate_signature_component_id
      AND component.workflow_id=v_revert_workflow.id
      AND component.workflow_generation=v_electronic_row.candidate_workflow_generation
      AND component.component_kind='CANDIDATE_SIGNATURE'
      AND component.state='IMMUTABLE'
      AND component.immutable_at_utc IS NOT NULL
      AND component.source_content_sha256=v_revert_workflow.candidate_signature_sha256
      AND component.storage_key=v_electronic_row.r2_nurse_key
    FOR UPDATE;
    IF NOT FOUND OR lower(btrim(v_electronic_row.img_sha256_nurse))
         IS DISTINCT FROM encode(v_revert_candidate_signature.source_content_sha256,'hex') THEN
      RAISE EXCEPTION 'TIMESHEET_REVERT_SIGNATURE_LINEAGE_INCOMPLETE: A new electronic approval generation is required';
    END IF;

    SELECT component.* INTO v_revert_manager_signature
    FROM public.candidate_submission_components component
    WHERE component.id=v_revert_workflow.manager_signature_component_id
      AND component.workflow_id=v_revert_workflow.id
      AND component.workflow_generation=v_electronic_row.candidate_workflow_generation
      AND component.component_kind='MANAGER_SIGNATURE'
      AND component.state='IMMUTABLE'
      AND component.immutable_at_utc IS NOT NULL
      AND component.approval_request_id=v_revert_approval_request.id
      AND component.source_content_sha256=v_revert_workflow.manager_signature_sha256
      AND component.storage_key=v_electronic_row.r2_auth_key
    FOR UPDATE;
    IF NOT FOUND OR lower(btrim(v_electronic_row.img_sha256_auth))
         IS DISTINCT FROM encode(v_revert_manager_signature.source_content_sha256,'hex') THEN
      RAISE EXCEPTION 'TIMESHEET_REVERT_SIGNATURE_LINEAGE_INCOMPLETE: A new electronic approval generation is required';
    END IF;

    SELECT evidence.* INTO v_revert_evidence
    FROM public.timesheet_evidence evidence
    WHERE evidence.timesheet_id=v_electronic_row.timesheet_id
      AND upper(btrim(evidence.kind))='TIMESHEET'
      AND evidence.processing_state<>'SUPERSEDED'
      AND evidence.candidate_component_id=v_revert_hours_component.id
      AND evidence.storage_key=v_revert_hours_component.final_signed_storage_key
    LIMIT 1
    FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'TIMESHEET_REVERT_DOCUMENT_LINEAGE_INCOMPLETE: A new electronic approval generation is required';
    END IF;

    -- Re-prove the immutable page IDs frozen into the request that the manager
    -- actually approved.  A superseded/missing component cannot disappear
    -- from this proof, and a replacement component with the same role cannot
    -- stand in for the approved identity.
    IF EXISTS(
      SELECT 1
      FROM unnest(v_revert_required_component_ids) expected_id
      LEFT JOIN public.candidate_submission_components component
        ON component.id=expected_id
       AND component.workflow_id=v_revert_workflow.id
       AND component.workflow_generation=v_electronic_row.candidate_workflow_generation
      WHERE component.id IS NULL
        OR component.required IS DISTINCT FROM true
        OR component.review_ordinal IS NULL
        OR component.state<>'IMMUTABLE'
        OR component.final_signed_render_state<>'READY'
        OR component.final_signed_storage_key IS NULL
        OR component.final_signed_content_sha256 IS NULL
        OR component.final_signed_render_input_sha256 IS NULL
        OR component.review_render_input_sha256
             IS DISTINCT FROM component.final_signed_render_input_sha256
        OR NOT EXISTS(
          SELECT 1
          FROM public.timesheet_evidence evidence
          WHERE evidence.candidate_component_id=expected_id
            AND evidence.processing_state<>'SUPERSEDED'
            AND evidence.storage_key=component.final_signed_storage_key
            AND evidence.document_role=CASE component.component_kind
              WHEN 'HOURS_TIMESHEET' THEN 'SIGNED_TIMESHEET'
              WHEN 'EXPENSE_SUMMARY' THEN 'EXPENSE_MILEAGE_APPROVAL_SUMMARY'
              WHEN 'MILEAGE_FORM' THEN 'MILEAGE_CLAIM_FORM'
              WHEN 'EXPENSE_EVIDENCE' THEN 'SOURCE_EVIDENCE'
              ELSE component.document_role
            END
        )
    ) OR EXISTS(
      SELECT 1
      FROM public.candidate_submission_components component
      WHERE component.workflow_id=v_revert_workflow.id
        AND component.workflow_generation=v_electronic_row.candidate_workflow_generation
        AND component.required=true
        AND component.review_ordinal IS NOT NULL
        AND component.state='IMMUTABLE'
        AND NOT (component.id=ANY(v_revert_required_component_ids))
    ) THEN
      RAISE EXCEPTION 'TIMESHEET_REVERT_REQUIRED_FINAL_PAGE_PROOF_INCOMPLETE: A new electronic approval generation is required';
    END IF;

    IF v_tsfin_row.id IS NULL THEN
      RAISE EXCEPTION 'TIMESHEET_REVERT_FINANCIAL_PROOF_INCOMPLETE: A new electronic approval generation is required';
    END IF;
    v_current_financial_sha256:=private._candidate_financial_content_sha256_v1(v_tsfin_row.id);
    IF v_current_financial_sha256 IS DISTINCT FROM v_revert_workflow.canonical_financial_sha256 THEN
      RAISE EXCEPTION 'TIMESHEET_REVERT_FINANCIAL_CONTENT_MISMATCH: A new electronic approval generation is required';
    END IF;

    IF v_current_row.authorised_at_server IS NOT NULL
       OR v_tsfin_row.authorised_at_utc IS NOT NULL THEN
      RAISE EXCEPTION 'TIMESHEET_AUTHORISED_EDIT_BLOCKED: Unauthorise before reverting route';
    END IF;

    UPDATE public.timesheets AS t_demote
    SET
      is_current = false,
      status = 'REVOKED'::public.timesheet_status_enum,
      revoked_at = v_now,
      revoked_reason = 'REVERTED_TO_EXACT_ELECTRONIC_VERSION',
      revoked_by = p_actor_user_id::text,
      updated_at = v_now
    WHERE t_demote.booking_id = v_booking_id
      AND t_demote.is_current = true;

    UPDATE public.timesheets AS t_promote
    SET
      is_current = true,
      status = 'RECEIVED'::public.timesheet_status_enum,
      revoked_at = NULL,
      revoked_reason = NULL,
      revoked_by = NULL,
      qr_status = NULL,
      qr_token = NULL,
      qr_generated_at = NULL,
      qr_scanned_at = NULL,
      qr_scan_info_json = NULL,
      qr_r2_key = NULL,
      qr_payload_json = '{}'::jsonb,
      qr_last_sent_hash = NULL,
      qr_last_sent_at_utc = NULL,
      qr_signed_hash = NULL,
      qr_signed_at_utc = NULL,
      updated_at = v_now
    WHERE t_promote.timesheet_id = v_electronic_row.timesheet_id;

    IF v_contract_week_row.id IS NOT NULL THEN
      UPDATE public.contract_weeks AS cw_move
      SET
        timesheet_id = v_electronic_row.timesheet_id,
        submission_mode_snapshot = 'ELECTRONIC'::public.submission_mode_enum,
        updated_at = v_now
      WHERE cw_move.id = v_contract_week_row.id;
    END IF;

    IF v_tsfin_row.id IS NOT NULL THEN
      UPDATE public.timesheets_financials AS tf_move
      SET
        timesheet_id = v_electronic_row.timesheet_id,
        timesheet_version = coalesce(v_electronic_row.version, 1),
        authorised_by_user_id = NULL,
        authorised_at_utc = NULL,
        updated_at = v_now
      WHERE tf_move.id = v_tsfin_row.id;
    END IF;

    v_reverted := true;
    v_electronic_version := coalesce(v_electronic_row.version, 1);

    RETURN jsonb_build_object(
      'ok', true,
      'action', v_action,
      'converted', false,
      'switched', false,
      'reverted', v_reverted,
      'booking_id', v_booking_id,
      'contract_week_id', CASE WHEN v_contract_week_row.id IS NULL THEN NULL ELSE v_contract_week_row.id::text END,
      'previous_current_timesheet_id', v_current_timesheet_id::text,
      'old_timesheet_id', v_current_timesheet_id::text,
      'new_timesheet_id', v_electronic_row.timesheet_id::text,
      'current_timesheet_id', v_electronic_row.timesheet_id::text,
      'electronic_version', v_electronic_version,
      'new_submission_mode', 'ELECTRONIC',
      'requested_timesheet_id', v_requested_timesheet_id::text,
      'was_stale', v_was_stale
    );
  END IF;
END;
$function$;

-- Exact compatibility boundary.  With the feature disabled this calls the
-- installed legacy body byte-for-byte; the new core is reachable only from
-- the locked confirmed-transition transaction marker.
create or replace function public.timesheet_route_version_rotate(
  p_current_timesheet_id uuid,
  p_expected_timesheet_id uuid,
  p_target_action text,
  p_actor_user_id uuid,
  p_allow_manual_only boolean default false
)
returns jsonb
language plpgsql
security definer
set search_path to 'public'
as $function$
begin
  if not private._candidate_feature_enabled_current_v1('candidate_route_confirmation') then
    return private._timesheet_route_version_legacy_v1(
      p_current_timesheet_id,p_expected_timesheet_id,p_target_action,
      p_actor_user_id,p_allow_manual_only
    );
  end if;
  if coalesce(current_setting('cloudtms.route_transition_confirmed',true),'') <> 'on' then
    raise exception 'ROUTE_CHANGE_CONFIRMATION_REQUIRED'
      using errcode='55000',detail=jsonb_build_object(
        'code','ROUTE_CHANGE_CONFIRMATION_REQUIRED',
        'preview_rpc','timesheet_route_version_preview_v1',
        'confirm_rpc','timesheet_route_version_confirmed_v1'
      )::text;
  end if;
  return private._timesheet_route_version_core_v1(
    p_current_timesheet_id,p_expected_timesheet_id,p_target_action,
    p_actor_user_id,p_allow_manual_only
  );
end;
$function$;

-- Read-only proof used by W13.  The mutating core repeats every proof under
-- lock, so the preview may truthfully say that exact restoration is proven.
create or replace function private._timesheet_exact_electronic_restore_proof_v1(
  p_current_timesheet_id uuid
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_current public.timesheets%rowtype;
  v_electronic public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_approval_request public.candidate_approval_requests%rowtype;
  v_hours public.candidate_submission_components%rowtype;
  v_candidate_signature public.candidate_submission_components%rowtype;
  v_manager_signature public.candidate_submission_components%rowtype;
  v_required_component_ids uuid[];
  v_financial_sha256 bytea;
  v_reason text;
begin
  select * into v_current from public.timesheets where timesheet_id=p_current_timesheet_id;
  if not found then return jsonb_build_object('proven',false,'reason','TIMESHEET_NOT_FOUND'); end if;
  select * into v_electronic from public.timesheets
  where booking_id=v_current.booking_id
    and upper(coalesce(submission_mode::text,''))='ELECTRONIC'
  order by version desc,updated_at desc,created_at desc limit 1;
  if not found then return jsonb_build_object('proven',false,'reason','NO_ELECTRONIC_VERSION'); end if;

  if row(
    v_current.contract_id,v_current.sheet_scope,v_current.line_type,
    v_current.occupant_key_norm,v_current.hospital_norm,v_current.ward_norm,
    v_current.job_title_norm,v_current.band,v_current.scheduled_start_iso,
    v_current.scheduled_end_iso,v_current.worked_start_iso,v_current.worked_end_iso,
    v_current.break_start_iso,v_current.break_end_iso,v_current.break_minutes,
    v_current.worked_minutes,v_current.week_ending_date,v_current.actual_schedule_json,
    v_current.additional_units_week,v_current.additional_units_per_day,
    v_current.day_references_json,v_current.reference_number,v_current.is_adjustment,
    v_current.parent_timesheet_id,v_current.correction_id,v_current.correction_kind,
    v_current.adjustment_origin
  ) is distinct from row(
    v_electronic.contract_id,v_electronic.sheet_scope,v_electronic.line_type,
    v_electronic.occupant_key_norm,v_electronic.hospital_norm,v_electronic.ward_norm,
    v_electronic.job_title_norm,v_electronic.band,v_electronic.scheduled_start_iso,
    v_electronic.scheduled_end_iso,v_electronic.worked_start_iso,v_electronic.worked_end_iso,
    v_electronic.break_start_iso,v_electronic.break_end_iso,v_electronic.break_minutes,
    v_electronic.worked_minutes,v_electronic.week_ending_date,v_electronic.actual_schedule_json,
    v_electronic.additional_units_week,v_electronic.additional_units_per_day,
    v_electronic.day_references_json,v_electronic.reference_number,v_electronic.is_adjustment,
    v_electronic.parent_timesheet_id,v_electronic.correction_id,v_electronic.correction_kind,
    v_electronic.adjustment_origin
  ) then return jsonb_build_object('proven',false,'reason','CONTENT_MISMATCH'); end if;

  if v_electronic.candidate_workflow_id is null
     or v_electronic.candidate_workflow_generation is null
     or nullif(btrim(coalesce(v_electronic.r2_nurse_key,'')),'') is null
     or nullif(btrim(coalesce(v_electronic.r2_auth_key,'')),'') is null
     or nullif(btrim(coalesce(v_electronic.img_sha256_nurse,'')),'') is null
     or nullif(btrim(coalesce(v_electronic.img_sha256_auth,'')),'') is null then
    return jsonb_build_object('proven',false,'reason','SIGNATURE_PROOF_INCOMPLETE');
  end if;
  select * into v_workflow from public.candidate_submission_workflows
  where id=v_electronic.candidate_workflow_id;
  if not found or v_workflow.state<>'FINALISED'
     or v_workflow.canonical_financial_sha256 is null
     or v_workflow.candidate_signature_component_id is null
     or v_workflow.manager_signature_component_id is null then
    return jsonb_build_object('proven',false,'reason','WORKFLOW_PROOF_INCOMPLETE');
  end if;
  select * into v_approval_request
  from public.candidate_approval_requests request_row
  where request_row.workflow_id=v_workflow.id
    and request_row.workflow_generation=v_electronic.candidate_workflow_generation
    and request_row.state in ('APPROVED','SUPERSEDED')
    and request_row.approved_at_utc is not null
  order by request_row.approved_at_utc desc,
    request_row.request_generation desc,request_row.created_at_utc desc
  limit 1;
  if not found
     or v_approval_request.review_manifest_sha256 is null
     or v_approval_request.review_manifest_sha256
          is distinct from v_workflow.review_manifest_sha256
     or cardinality(v_approval_request.required_component_ids)=0
     or jsonb_array_length(v_approval_request.required_component_manifest_json)
          <> cardinality(v_approval_request.required_component_ids)
     or cardinality(v_approval_request.required_component_ids)
          <> (select count(distinct expected_id)
              from unnest(v_approval_request.required_component_ids) expected_id)
     or exists(
       select 1
       from unnest(v_approval_request.required_component_ids) expected_id
       where not exists(
         select 1
         from jsonb_array_elements(
           v_approval_request.required_component_manifest_json
         ) manifest_item
         where manifest_item->>'component_id'=expected_id::text
       )
     )
     or exists(
       select 1
       from jsonb_array_elements(
         v_approval_request.required_component_manifest_json
       ) manifest_item
       where not (manifest_item->>'component_id'=any(
         select expected_id::text
         from unnest(v_approval_request.required_component_ids) expected_id
       ))
     ) then
    return jsonb_build_object('proven',false,'reason','APPROVED_MANIFEST_PROOF_INCOMPLETE');
  end if;
  v_required_component_ids:=v_approval_request.required_component_ids;
  select * into v_hours from public.candidate_submission_components
  where id=v_approval_request.manager_review_timesheet_component_id
    and id=any(v_required_component_ids)
    and workflow_id=v_workflow.id
    and workflow_generation=v_electronic.candidate_workflow_generation
    and component_kind='HOURS_TIMESHEET' and required=true
    and state='IMMUTABLE' and final_signed_render_state='READY'
    and final_signed_storage_key is not null
    and final_signed_content_sha256 is not null
    and final_signed_render_input_sha256 is not null
    and review_render_input_sha256=final_signed_render_input_sha256
  order by review_ordinal,id limit 1;
  if not found then return jsonb_build_object('proven',false,'reason','DOCUMENT_PROOF_INCOMPLETE'); end if;
  -- W13 is a high-assurance historical restore.  Its authority is the exact
  -- immutable ID set frozen into the manager-approved request, not whichever
  -- required components happen to remain active today.
  if exists(
    select 1
    from unnest(v_required_component_ids) expected_id
    left join public.candidate_submission_components component
      on component.id=expected_id
     and component.workflow_id=v_workflow.id
     and component.workflow_generation=v_electronic.candidate_workflow_generation
    where component.id is null
      or component.required is distinct from true
      or component.review_ordinal is null
      or component.state<>'IMMUTABLE'
      or component.final_signed_render_state<>'READY'
      or component.final_signed_storage_key is null
      or component.final_signed_content_sha256 is null
      or component.final_signed_render_input_sha256 is null
      or component.review_render_input_sha256
           is distinct from component.final_signed_render_input_sha256
      or not exists(
        select 1
        from public.timesheet_evidence evidence
        where evidence.candidate_component_id=expected_id
          and evidence.processing_state<>'SUPERSEDED'
          and evidence.storage_key=component.final_signed_storage_key
          and evidence.document_role=case component.component_kind
            when 'HOURS_TIMESHEET' then 'SIGNED_TIMESHEET'
            when 'EXPENSE_SUMMARY' then 'EXPENSE_MILEAGE_APPROVAL_SUMMARY'
            when 'MILEAGE_FORM' then 'MILEAGE_CLAIM_FORM'
            when 'EXPENSE_EVIDENCE' then 'SOURCE_EVIDENCE'
            else component.document_role
          end
      )
  ) or exists(
    select 1
    from public.candidate_submission_components component
    where component.workflow_id=v_workflow.id
      and component.workflow_generation=v_electronic.candidate_workflow_generation
      and component.required=true
      and component.review_ordinal is not null
      and component.state='IMMUTABLE'
      and not (component.id=any(v_required_component_ids))
  ) then
    return jsonb_build_object(
      'proven',false,
      'reason','REQUIRED_FINAL_PAGE_PROOF_INCOMPLETE'
    );
  end if;
  select * into v_candidate_signature from public.candidate_submission_components
  where id=v_workflow.candidate_signature_component_id
    and workflow_id=v_workflow.id
    and workflow_generation=v_electronic.candidate_workflow_generation
    and component_kind='CANDIDATE_SIGNATURE'
    and state='IMMUTABLE'
    and immutable_at_utc is not null
    and source_content_sha256=v_workflow.candidate_signature_sha256
    and storage_key=v_electronic.r2_nurse_key;
  if not found or lower(btrim(v_electronic.img_sha256_nurse))
       is distinct from encode(v_candidate_signature.source_content_sha256,'hex') then
    return jsonb_build_object('proven',false,'reason','CANDIDATE_SIGNATURE_PROOF_INCOMPLETE');
  end if;
  select * into v_manager_signature from public.candidate_submission_components
  where id=v_workflow.manager_signature_component_id
    and workflow_id=v_workflow.id
    and workflow_generation=v_electronic.candidate_workflow_generation
    and component_kind='MANAGER_SIGNATURE'
    and state='IMMUTABLE'
    and immutable_at_utc is not null
    and approval_request_id=v_approval_request.id
    and source_content_sha256=v_workflow.manager_signature_sha256
    and storage_key=v_electronic.r2_auth_key;
  if not found or lower(btrim(v_electronic.img_sha256_auth))
       is distinct from encode(v_manager_signature.source_content_sha256,'hex') then
    return jsonb_build_object('proven',false,'reason','MANAGER_SIGNATURE_PROOF_INCOMPLETE');
  end if;
  if not exists(
    select 1 from public.timesheet_evidence evidence
    where evidence.timesheet_id=v_electronic.timesheet_id
      and upper(btrim(evidence.kind))='TIMESHEET'
      and evidence.processing_state<>'SUPERSEDED'
      and evidence.candidate_component_id=v_hours.id
      and evidence.storage_key=v_hours.final_signed_storage_key
  ) then return jsonb_build_object('proven',false,'reason','EVIDENCE_PROOF_INCOMPLETE'); end if;
  select * into v_fin from public.timesheets_financials
  where timesheet_id=v_current.timesheet_id and is_current=true
  order by computed_at_utc desc nulls last,updated_at desc,id desc limit 1;
  if not found then return jsonb_build_object('proven',false,'reason','FINANCIAL_PROOF_INCOMPLETE'); end if;
  v_financial_sha256:=private._candidate_financial_content_sha256_v1(v_fin.id);
  if v_financial_sha256 is distinct from v_workflow.canonical_financial_sha256 then
    return jsonb_build_object('proven',false,'reason','FINANCIAL_CONTENT_MISMATCH');
  end if;
  return jsonb_build_object('proven',true,'electronic_timesheet_id',v_electronic.timesheet_id,
    'electronic_version',v_electronic.version);
end;
$function$;

-- One read-only context authority feeds every future Simple Timesheet, Bulk
-- Process and Bulk Authorise route warning.  The mutation recomputes the same
-- digest under the booking/timesheet/TSFIN/workflow/request locks.
create or replace function private._timesheet_route_change_context_v1(
  p_current_timesheet_id uuid,
  p_target_action text
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_action text:=upper(btrim(coalesce(p_target_action,'')));
  v_requested public.timesheets%rowtype;
  v_current public.timesheets%rowtype;
  v_week public.contract_weeks%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_contract public.contracts%rowtype;
  v_linked_workflow public.candidate_submission_workflows%rowtype;
  v_active_workflow public.candidate_submission_workflows%rowtype;
  v_request public.candidate_approval_requests%rowtype;
  v_signature jsonb:='{}'::jsonb;
  v_policy jsonb:='{}'::jsonb;
  v_import jsonb:='{}'::jsonb;
  v_restore_proof jsonb:='{}'::jsonb;
  v_history jsonb:='{}'::jsonb;
  v_payload jsonb;
  v_context_hash text;
  v_route_family text;
  v_warning_code text;
  v_block_reason text;
  v_row_signature text;
  v_effective_mode text;
  v_booking_id text;
  v_contract_id uuid;
  v_client_id uuid;
  v_candidate_signed boolean:=false;
  v_manager_pending boolean:=false;
  v_manager_approved boolean:=false;
  v_final_signed_ready boolean:=false;
  v_qr_route_active boolean:=false;
  v_qr_code_generated boolean:=false;
  v_qr_pack_ready boolean:=false;
  v_qr_pack_issued_or_sent boolean:=false;
  v_qr_issued boolean:=false;
  v_qr_signed boolean:=false;
  v_prior_qr boolean:=false;
  v_import_authoritative boolean:=false;
  v_office_authorised boolean:=false;
  v_invoiced boolean:=false;
  v_paid boolean:=false;
  v_paper_allowed boolean:=false;
  v_permitted boolean:=false;
  v_reason_required boolean:=false;
  v_confirmation_required boolean:=false;
  v_active_workflow_count integer:=0;
  v_contract_week_authorised boolean:=false;
  v_protected_history boolean:=false;
  v_scope text;
  v_basis text;
  v_actual_qr_backing boolean:=false;
begin
  if p_current_timesheet_id is null then
    raise exception 'ROUTE_CHANGE_TIMESHEET_REQUIRED' using errcode='22023';
  end if;
  if v_action not in (
    'CONVERT_QR_TO_MANUAL','ALLOW_ELECTRONIC_AGAIN','ALLOW_QR_AGAIN',
    'INVALIDATE_QR','REISSUE_QR','DISABLE_QR','SWITCH_TO_MANUAL',
    'SWITCH_DAILY_TO_MANUAL','REVERT_TO_ELECTRONIC'
  ) then
    raise exception 'ROUTE_CHANGE_ACTION_INVALID' using errcode='22023';
  end if;

  select * into v_requested from public.timesheets
  where timesheet_id=p_current_timesheet_id;
  if not found then raise exception 'TIMESHEET_NOT_FOUND' using errcode='P0002'; end if;
  v_booking_id:=nullif(btrim(coalesce(v_requested.booking_id,'')),'');
  if v_booking_id is null then
    raise exception 'TIMESHEET_BOOKING_ID_REQUIRED' using errcode='55000';
  end if;
  select * into v_current from public.timesheets
  where booking_id=v_booking_id and is_current=true
  order by version desc,updated_at desc,created_at desc limit 1;
  if not found then raise exception 'TIMESHEET_CURRENT_VERSION_NOT_FOUND' using errcode='P0002'; end if;

  select * into v_week from public.contract_weeks
  where timesheet_id=v_current.timesheet_id
  order by updated_at desc,created_at desc,id desc limit 1;
  select * into v_fin from public.timesheets_financials
  where timesheet_id=v_current.timesheet_id and is_current=true
  order by computed_at_utc desc nulls last,updated_at desc,id desc limit 1;
  v_contract_id:=coalesce(v_week.contract_id,v_current.contract_id);
  if v_contract_id is not null then
    select * into v_contract from public.contracts where id=v_contract_id;
    v_client_id:=v_contract.client_id;
  end if;
  v_client_id:=coalesce(v_client_id,v_fin.client_id);

  if v_client_id is not null then
    v_import:=private._candidate_import_authoritative_v1(
      v_client_id,v_contract_id,v_current.timesheet_id,
      case when v_fin.id is null then null else to_jsonb(v_fin) end,
      v_current.week_ending_date
    );
    v_import_authoritative:=coalesce((v_import->>'is_import_authoritative')::boolean,false);
  end if;
  if v_client_id is not null and v_contract_id is not null then
    v_policy:=private._candidate_policy_resolve_v1(
      v_client_id,v_contract_id,
      coalesce(v_current.week_ending_date,(current_timestamp at time zone 'Europe/London')::date)
    );
    v_effective_mode:=private._candidate_submission_mode_v1(
      v_client_id,v_contract_id,
      coalesce(v_current.week_ending_date,(current_timestamp at time zone 'Europe/London')::date)
    )::text;
    v_paper_allowed:=coalesce((v_policy->>'paper_submission_enabled')::boolean,false);
  else
    v_effective_mode:=coalesce(v_current.submission_mode::text,'MANUAL');
  end if;

  select exists(
    select 1 from public.timesheets history
    where history.booking_id=v_booking_id and (
      history.qr_status is not null
      or nullif(btrim(coalesce(history.qr_token,'')),'') is not null
      or history.qr_generated_at is not null
      or history.qr_scanned_at is not null
      or nullif(btrim(coalesce(history.qr_r2_key,'')),'') is not null
      or history.qr_signed_at_utc is not null
    )
  ) into v_prior_qr;
  v_qr_route_active:=coalesce(v_current.qr_status is not null
    or nullif(btrim(coalesce(v_current.qr_token,'')),'') is not null
    or v_current.qr_generated_at is not null
    or nullif(btrim(coalesce(v_current.qr_r2_key,'')),'') is not null
    or v_current.qr_scanned_at is not null
    or v_current.qr_signed_at_utc is not null
    or nullif(btrim(coalesce(v_current.qr_signed_hash,'')),'') is not null
    or v_current.candidate_submission_route_intent='PAPER',false);
  -- The QR code is created before the printable PDF.  Keep code generation,
  -- durable pack readiness, actual issue/send and signed return as separate
  -- facts so the approved W08/W09 wording is never asserted prematurely.
  v_qr_code_generated:=v_qr_route_active
    and nullif(btrim(coalesce(v_current.qr_token,'')),'') is not null
    and v_current.qr_generated_at is not null;
  v_qr_pack_issued_or_sent:=v_qr_route_active and (
    v_current.qr_last_sent_at_utc is not null
    or nullif(btrim(coalesce(v_current.qr_last_sent_hash,'')),'') is not null
  );
  v_qr_pack_ready:=v_qr_route_active and (
      -- The unchanged QR backend writes this key only after successful PDF
      -- rendering.  It is proof of an unsigned generated pack, never proof of
      -- a returned signature.
      nullif(btrim(coalesce(v_current.manual_pdf_r2_key,'')),'') is not null
      or (
      upper(coalesce(v_current.document_state::text,''))='READY'
      and v_current.current_document_version_id is not null
      and exists(
        select 1 from public.invoice_document_versions document_version
        where document_version.id=v_current.current_document_version_id
          and document_version.entity_type='TIMESHEET'
          and document_version.entity_id=v_current.timesheet_id
          and upper(coalesce(document_version.status,''))='READY'
      )
    )
    or v_qr_pack_issued_or_sent
  );
  v_qr_signed:=v_qr_route_active and (
    v_current.qr_signed_at_utc is not null
    or nullif(btrim(coalesce(v_current.qr_signed_hash,'')),'') is not null
    or v_current.qr_scanned_at is not null
    or exists(
      select 1 from public.timesheet_evidence evidence
      where evidence.timesheet_id=v_current.timesheet_id
        and upper(btrim(evidence.kind))='TIMESHEET'
        and evidence.document_role='SIGNED_TIMESHEET'
        and evidence.processing_state<>'SUPERSEDED'
        and nullif(btrim(coalesce(evidence.storage_key,'')),'') is not null
    )
  );
  v_scope:=upper(coalesce(v_current.sheet_scope::text,''));
  v_basis:=upper(coalesce(v_fin.basis::text,''));
  v_actual_qr_backing:=v_qr_route_active;
  v_route_family:=case
    when v_import_authoritative then 'IMPORT_AUTHORITATIVE'
    when v_actual_qr_backing then 'QR'
    when upper(coalesce(v_current.submission_mode::text,''))='ELECTRONIC'
      or upper(coalesce(v_week.submission_mode_snapshot::text,''))='ELECTRONIC'
      or v_current.candidate_submission_route_intent='ELECTRONIC'
      then 'ELECTRONIC'
    else 'MANUAL_NON_QR' end;

  -- Existing signed ELECTRONIC rows pre-date Candidate workflows.  Canonical
  -- CloudTMS signature/document evidence remains authoritative for warnings.
  v_candidate_signed:=nullif(btrim(coalesce(v_current.r2_nurse_key,'')),'') is not null
    or nullif(btrim(coalesce(v_current.img_sha256_nurse,'')),'') is not null;
  v_manager_approved:=nullif(btrim(coalesce(v_current.r2_auth_key,'')),'') is not null
    or nullif(btrim(coalesce(v_current.img_sha256_auth,'')),'') is not null
    or v_current.candidate_manager_approved_at_utc is not null;
  v_final_signed_ready:=v_manager_approved and exists(
    select 1 from public.timesheet_evidence evidence
    where evidence.timesheet_id=v_current.timesheet_id
      and upper(btrim(evidence.kind))='TIMESHEET'
      and evidence.processing_state<>'SUPERSEDED'
      and nullif(btrim(coalesce(evidence.storage_key,'')),'') is not null
  );

  select workflow.* into v_linked_workflow
  from public.candidate_submission_workflows workflow
  where workflow.id=v_current.candidate_workflow_id
     or workflow.target_timesheet_id=v_current.timesheet_id
     or workflow.anchor_timesheet_id=v_current.timesheet_id
  order by (workflow.id=v_current.candidate_workflow_id) desc,
    workflow.updated_at_utc desc,workflow.created_at_utc desc
  limit 1;
  select workflow.* into v_active_workflow
  from public.candidate_submission_workflows workflow
  where (workflow.id=v_current.candidate_workflow_id
      or workflow.target_timesheet_id=v_current.timesheet_id
      or workflow.anchor_timesheet_id=v_current.timesheet_id)
    and workflow.state not in ('FINALISED','REFUSED','REJECTED','CANCELLED','EXPIRED','SUPERSEDED')
  order by (workflow.id=v_current.candidate_workflow_id) desc,
    workflow.updated_at_utc desc,workflow.created_at_utc desc
  limit 1;
  select count(*) into v_active_workflow_count
  from public.candidate_submission_workflows workflow
  where (workflow.id=v_current.candidate_workflow_id
      or workflow.target_timesheet_id=v_current.timesheet_id
      or workflow.anchor_timesheet_id=v_current.timesheet_id)
    and workflow.state not in ('FINALISED','REFUSED','REJECTED','CANCELLED','EXPIRED','SUPERSEDED');

  if v_linked_workflow.id is not null then
    v_candidate_signed:=v_candidate_signed
      or v_linked_workflow.candidate_signature_component_id is not null
      or v_linked_workflow.candidate_signed_at_utc is not null;
    v_manager_approved:=v_manager_approved
      or v_linked_workflow.state in ('MANAGER_APPROVED','RECEIVED','FINALISED')
      or v_linked_workflow.manager_signature_component_id is not null
      or v_linked_workflow.manager_approved_at_utc is not null;
    v_final_signed_ready:=v_final_signed_ready or exists(
      select 1 from public.candidate_submission_components component
      where component.workflow_id=v_linked_workflow.id
        and component.workflow_generation=v_linked_workflow.generation
        and component.required=true
        and component.state<>'SUPERSEDED'
        and component.final_signed_render_state='READY'
    );
    v_qr_signed:=v_qr_signed or (v_qr_route_active and exists(
      select 1 from public.candidate_submission_components component
      where component.workflow_id=v_linked_workflow.id
        and component.workflow_generation=v_linked_workflow.generation
        and component.component_kind='SIGNED_RETURN'
        and component.document_role='SIGNED_RETURN'
        and component.state='IMMUTABLE'
        and component.immutable_at_utc is not null
        and nullif(btrim(coalesce(component.storage_key,'')),'') is not null
    ));
  end if;
  -- A signed return necessarily followed issue even where a legacy send stamp
  -- is incomplete.  A merely generated or READY-but-unsent pack does not.
  v_qr_issued:=v_qr_pack_issued_or_sent or v_qr_signed;
  v_qr_pack_ready:=v_qr_pack_ready or v_qr_issued;
  if v_active_workflow.id is not null then
    select request_row.* into v_request
    from public.candidate_approval_requests request_row
    where request_row.workflow_id=v_active_workflow.id
      and request_row.workflow_generation=v_active_workflow.generation
      and (request_row.state='APPROVED' or (
        request_row.state='PENDING'
        and (request_row.expires_at_utc is null or request_row.expires_at_utc>now())
      ))
    order by (request_row.state='PENDING'
      and (request_row.expires_at_utc is null or request_row.expires_at_utc>now())) desc,
      request_row.request_generation desc,request_row.created_at_utc desc
    limit 1;
    v_manager_pending:=coalesce(v_request.state='PENDING',false);
    v_manager_approved:=v_manager_approved or coalesce(v_request.state='APPROVED',false);
  end if;

  v_contract_week_authorised:=upper(coalesce(v_week.status::text,''))='AUTHORISED';
  v_office_authorised:=v_current.authorised_at_server is not null
    or v_fin.authorised_at_utc is not null or v_contract_week_authorised;
  v_paid:=v_fin.paid_at_utc is not null;
  v_invoiced:=v_fin.locked_by_invoice_id is not null or exists(
    select 1 from jsonb_array_elements(
      case when jsonb_typeof(v_fin.invoice_breakdown_json->'segments')='array'
        then v_fin.invoice_breakdown_json->'segments' else '[]'::jsonb end
    ) segment
    where nullif(btrim(coalesce(segment->>'invoice_locked_invoice_id','')),'') is not null
  );
  select public.timesheet_removal_financial_history_v1(
    array_agg(history.timesheet_id order by history.timesheet_id),
    array[v_booking_id],
    case when v_week.id is null then array[]::uuid[] else array[v_week.id] end
  ) into v_history
  from public.timesheets history where history.booking_id=v_booking_id;
  v_protected_history:=coalesce((v_history->>'archive_required')::boolean,false)
    or exists(select 1 from jsonb_array_elements(coalesce(v_history->'blockers','[]'::jsonb)) blocker
      where blocker->>'code' not in ('TIMESHEET_STILL_AUTHORISED'));
  v_signature:=public.timesheet_lifecycle_guard_signature_v1(
    v_current.timesheet_id,
    case when v_week.id is null then null else v_week.id end,
    false
  );
  v_row_signature:=nullif(btrim(coalesce(
    v_signature->>'backend_row_signature',v_signature->>'row_signature',v_signature->>'signature',''
  )), '');
  if v_row_signature is null then
    raise exception 'ROUTE_CHANGE_ROW_SIGNATURE_UNAVAILABLE' using errcode='55000';
  end if;
  if v_action='REVERT_TO_ELECTRONIC' then
    v_restore_proof:=private._timesheet_exact_electronic_restore_proof_v1(v_current.timesheet_id);
  end if;

  if v_import_authoritative then
    v_warning_code:='ROUTE_CHANGE_IMPORT_AUTHORITATIVE_BLOCK';
    v_block_reason:='IMPORT_AUTHORITATIVE';
  elsif v_paid or v_invoiced or v_protected_history then
    v_warning_code:='ROUTE_CHANGE_FINANCIAL_HISTORY_BLOCK';
    v_block_reason:='FINANCIAL_HISTORY';
  elsif v_office_authorised then
    v_warning_code:='ROUTE_CHANGE_REQUIRES_UNAUTHORISE';
    v_block_reason:='AUTHORISED';
  elsif v_active_workflow_count>1 then
    v_warning_code:='ROUTE_CHANGE_WORKFLOW_CONFLICT';
    v_block_reason:='MULTIPLE_ACTIVE_WORKFLOWS';
  elsif v_scope='DAILY'
        and v_action in ('ALLOW_QR_AGAIN','INVALIDATE_QR','REISSUE_QR') then
    v_warning_code:='DAILY_PAPER_ROUTE_NOT_ALLOWED';
    v_block_reason:='DAILY_PAPER_ROUTE_NOT_ALLOWED';
  elsif v_action='SWITCH_TO_MANUAL' and v_route_family='ELECTRONIC'
        and v_scope='WEEKLY' and v_week.id is not null and v_fin.id is not null
        and v_basis='CONTRACT_WEEKLY' and not coalesce(v_current.is_adjustment,false) then
    v_permitted:=true;
    v_reason_required:=true;
    v_warning_code:=case
      when v_manager_approved or v_final_signed_ready then 'MANAGER_APPROVED_TO_MANUAL'
      when v_candidate_signed or v_manager_pending then 'CANDIDATE_SIGNED_MANAGER_PENDING_TO_MANUAL'
      else 'ELECTRONIC_UNSIGNED_TO_MANUAL' end;
  elsif v_action='SWITCH_DAILY_TO_MANUAL' and v_route_family='ELECTRONIC'
        and v_scope='DAILY' and v_fin.id is not null
        and v_basis not in ('NHSP','NHSP_ADJUSTMENT','HEALTHROSTER_SELF_BILL','HEALTHROSTER_ADJUSTMENT')
        and not coalesce(v_current.is_adjustment,false) then
    v_permitted:=true;
    v_reason_required:=true;
    v_warning_code:=case
      when v_manager_approved or v_final_signed_ready then 'MANAGER_APPROVED_TO_MANUAL'
      when v_candidate_signed or v_manager_pending then 'CANDIDATE_SIGNED_MANAGER_PENDING_TO_MANUAL'
      else 'ELECTRONIC_UNSIGNED_TO_MANUAL' end;
  elsif v_action in ('CONVERT_QR_TO_MANUAL','DISABLE_QR') and v_route_family='QR'
        and v_actual_qr_backing and v_fin.id is not null
        and not coalesce(v_current.is_adjustment,false) then
    if v_qr_signed then
      v_permitted:=true;
      v_reason_required:=true;
      v_warning_code:='QR_SIGNED_TO_MANUAL';
    elsif v_qr_pack_issued_or_sent then
      v_permitted:=true;
      v_reason_required:=true;
      v_warning_code:='QR_ISSUED_TO_MANUAL';
    elsif v_qr_pack_ready then
      v_warning_code:='ROUTE_CHANGE_NOT_PERMITTED';
      v_block_reason:='QR_PACK_READY_NOT_ISSUED';
    else
      v_warning_code:='ROUTE_CHANGE_NOT_PERMITTED';
      v_block_reason:='QR_PACK_PREPARATION_IN_PROGRESS';
    end if;
  elsif v_action='ALLOW_ELECTRONIC_AGAIN' and v_route_family='MANUAL_NON_QR'
        and upper(coalesce(v_effective_mode,''))='ELECTRONIC'
        and v_fin.id is not null and not coalesce(v_current.is_adjustment,false) then
    v_permitted:=true;
    v_warning_code:='FRESH_ELECTRONIC_RESUBMISSION_REQUIRED';
  elsif v_action='ALLOW_QR_AGAIN' and v_route_family='MANUAL_NON_QR'
        and v_scope='WEEKLY'
        and (v_prior_qr or v_paper_allowed) and v_fin.id is not null
        and not coalesce(v_current.is_adjustment,false) then
    v_permitted:=true;
    v_warning_code:='FRESH_PAPER_RESUBMISSION_REQUIRED';
  elsif v_action in ('INVALIDATE_QR','REISSUE_QR') and v_route_family='QR'
        and v_scope='WEEKLY'
        and v_actual_qr_backing and v_qr_issued and v_fin.id is not null
        and not coalesce(v_current.is_adjustment,false) then
    v_permitted:=true;
    v_warning_code:='QR_REPLACEMENT_PACK_REQUIRED';
  elsif v_action='REVERT_TO_ELECTRONIC' and v_route_family='MANUAL_NON_QR'
        and coalesce((v_restore_proof->>'proven')::boolean,false) then
    v_permitted:=true;
    v_warning_code:='EXACT_ELECTRONIC_RESTORE_PROVEN';
  else
    v_warning_code:='ROUTE_CHANGE_NOT_PERMITTED';
    v_block_reason:='ROUTE_OR_POLICY_MISMATCH';
  end if;
  v_confirmation_required:=v_permitted;

  v_payload:=jsonb_build_object(
    'contract_version','TIMESHEET_ROUTE_CHANGE_CONTEXT_V1',
    'target_action',v_action,
    'booking_id',v_booking_id,
    'requested_timesheet_id',v_requested.timesheet_id,
    'current_timesheet_id',v_current.timesheet_id,
    'current_version',v_current.version,
    'current_updated_at_utc',v_current.updated_at,
    'current_status',v_current.status,
    'current_row_signature',v_row_signature,
    'row_signature',v_row_signature,
    'sheet_scope',v_scope,
    'tsfin_basis',v_basis,
    'pending_route_intent',v_current.candidate_submission_route_intent,
    'route_family',v_route_family,
    'effective_submission_mode',v_effective_mode,
    'effective_paper_submission_allowed',v_paper_allowed,
    'prior_qr_lineage',v_prior_qr,
    'import_authoritative',v_import_authoritative,
    'office_authorised',v_office_authorised,
    'contract_week_authorised',v_contract_week_authorised,
    'invoiced',v_invoiced,
    'paid',v_paid,
    'protected_financial_history',v_protected_history,
    'candidate_signed',v_candidate_signed,
    'manager_approval_pending',v_manager_pending,
    'manager_approved',v_manager_approved,
    'final_signed_document_ready',v_final_signed_ready,
    'qr_route_active',v_qr_route_active,
    'qr_code_generated',v_qr_code_generated,
    'qr_pack_ready',v_qr_pack_ready,
    'qr_pack_issued_or_sent',v_qr_pack_issued_or_sent,
    'qr_pack_issued',v_qr_issued,
    'qr_signed_returned',v_qr_signed
  )||jsonb_build_object(
    'linked_workflow_id',v_linked_workflow.id,
    'linked_workflow_generation',v_linked_workflow.generation,
    'linked_workflow_state',v_linked_workflow.state,
    'linked_workflow_updated_at_utc',v_linked_workflow.updated_at_utc,
    'active_workflow_id',v_active_workflow.id,
    'active_workflow_count',v_active_workflow_count,
    'active_workflow_generation',v_active_workflow.generation,
    'active_approval_request_id',v_request.id,
    'active_approval_request_state',v_request.state,
    'active_approval_request_updated_at_utc',v_request.updated_at_utc,
    'exact_electronic_restore_proof',v_restore_proof,
    'tsfin_id',v_fin.id,
    'tsfin_updated_at_utc',v_fin.updated_at,
    'permitted_action',v_permitted,
    'warning_code',v_warning_code,
    'confirmation_required',v_confirmation_required,
    'reason_required',v_reason_required,
    'block_reason',v_block_reason,
    'fresh_submission_required',v_action in ('ALLOW_ELECTRONIC_AGAIN','ALLOW_QR_AGAIN','INVALIDATE_QR','REISSUE_QR'),
    'notification_required',v_action in ('ALLOW_ELECTRONIC_AGAIN','ALLOW_QR_AGAIN','INVALIDATE_QR','REISSUE_QR')
  );
  v_context_hash:=encode(digest(convert_to(v_payload::text,'UTF8'),'sha256'),'hex');
  return v_payload||jsonb_build_object('context_sha256',v_context_hash);
end;
$function$;

create or replace function private._timesheet_route_supersede_candidate_v1(
  p_workflow_id uuid,
  p_action text,
  p_reason_code text,
  p_reason_note text,
  p_actor_user_id uuid,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_workflow public.candidate_submission_workflows%rowtype;
  v_request public.candidate_approval_requests%rowtype;
  v_cancelled integer:=0;
  v_superseded integer:=0;
  v_mail_ids jsonb:='[]'::jsonb;
  v_mail_id uuid;
  v_reason text:=btrim(coalesce(p_reason_code,''))
    ||case when nullif(btrim(coalesce(p_reason_note,'')),'') is not null
      then ': '||btrim(p_reason_note) else '' end;
begin
  if p_workflow_id is null then
    return jsonb_build_object('workflow_changed',false,'manager_request_cancelled',false,
      'manager_cancellation_email_queued',false,'manager_cancellation_mail_ids','[]'::jsonb);
  end if;
  select * into v_workflow from public.candidate_submission_workflows
  where id=p_workflow_id for update;
  if not found or v_workflow.state in ('FINALISED','REFUSED','REJECTED','CANCELLED','EXPIRED','SUPERSEDED') then
    return jsonb_build_object('workflow_changed',false,'workflow_id',p_workflow_id,
      'manager_request_cancelled',false,'manager_cancellation_email_queued',false,
      'manager_cancellation_mail_ids','[]'::jsonb);
  end if;

  if v_workflow.route='PAPER' and v_workflow.state='AWAITING_PAPER_RETURN' then
    perform private._candidate_paper_delivery_retire_v1(
      v_workflow.id,v_workflow.generation,
      'ROUTE_INTERVENTION_'||upper(btrim(coalesce(p_action,'UNKNOWN'))),p_now_utc
    );
  end if;

  for v_request in
    select * from public.candidate_approval_requests
    where workflow_id=v_workflow.id and state in ('PENDING','APPROVED')
    order by created_at_utc,id for update
  loop
    if v_request.state='PENDING' then
      if v_request.expires_at_utc is not null and v_request.expires_at_utc<=p_now_utc then
        update public.candidate_approval_requests set
          state='EXPIRED',updated_at_utc=p_now_utc
        where id=v_request.id;
        continue;
      end if;
      if v_request.method='EMAIL'
         and v_request.initial_sent_at_utc is not null
         and v_request.manager_email_normalized is not null then
        v_mail_id:=private._candidate_queue_mail_v1(
          jsonb_build_object(
            'subject','Timesheet approval request withdrawn',
            'body_text','The approval request for this timesheet has been withdrawn by CloudTMS. No further action is required.',
            'email_type','CANDIDATE_MANAGER_APPROVAL_WITHDRAWN'
          ),
          v_request.manager_email_normalized,
          'CANDIDATE_MANAGER_APPROVAL_WITHDRAWN_V1:'||v_request.id::text,
          'candidate-manager-approval-withdrawn:'||v_request.id::text,
          v_workflow.id,p_now_utc
        );
        v_mail_ids:=v_mail_ids||jsonb_build_array(v_mail_id);
      end if;
      update public.candidate_approval_requests set
        state='CANCELLED',cancelled_at_utc=p_now_utc,updated_at_utc=p_now_utc
      where id=v_request.id;
      v_cancelled:=v_cancelled+1;
    else
      update public.candidate_approval_requests set
        state='SUPERSEDED',superseded_at_utc=p_now_utc,updated_at_utc=p_now_utc
      where id=v_request.id;
      v_superseded:=v_superseded+1;
    end if;
  end loop;

  update public.candidate_submission_components set
    state='SUPERSEDED',superseded_at_utc=p_now_utc,
    review_render_state=case when review_render_state='NOT_REQUIRED'
      then review_render_state else 'SUPERSEDED' end,
    final_signed_render_state=case when final_signed_render_state='NOT_REQUIRED'
      then final_signed_render_state else 'SUPERSEDED' end
  where workflow_id=v_workflow.id and state not in ('SUPERSEDED','REJECTED');

  update public.candidate_submission_workflows set
    state='SUPERSEDED',cancelled_at_utc=coalesce(cancelled_at_utc,p_now_utc),
    last_mutation_idempotency_key='ROUTE_INTERVENTION:'||upper(btrim(p_action))||':'||v_workflow.generation::text,
    last_mutation_response_json=jsonb_build_object(
      'ok',true,'state','SUPERSEDED','reason_code',p_reason_code,
      'reason_note',nullif(btrim(coalesce(p_reason_note,'')),'')
    ),updated_at_utc=p_now_utc
  where id=v_workflow.id;

  perform private._candidate_audit_v1(
    'candidate_submission_workflow',v_workflow.id::text,'ROUTE_INTERVENTION_SUPERSEDE',
    jsonb_build_object('state',v_workflow.state,'generation',v_workflow.generation),
    jsonb_build_object('state','SUPERSEDED','action',upper(btrim(p_action))),
    v_reason,p_actor_user_id,'route-intervention:'||v_workflow.id::text,p_now_utc
  );
  return jsonb_build_object(
    'workflow_changed',true,'workflow_id',v_workflow.id,
    'manager_request_cancelled',v_cancelled>0,
    'cancelled_request_count',v_cancelled,'superseded_request_count',v_superseded,
    'manager_cancellation_email_queued',jsonb_array_length(v_mail_ids)>0,
    'manager_cancellation_mail_ids',v_mail_ids
  );
end;
$function$;

create or replace function private._timesheet_route_resubmission_notifications_v1(
  p_old_timesheet_id uuid,
  p_new_timesheet_id uuid,
  p_new_version integer,
  p_target_action text,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_old public.timesheets%rowtype;
  v_new public.timesheets%rowtype;
  v_candidate public.candidates%rowtype;
  v_account public.candidate_app_accounts%rowtype;
  v_environment text;
  v_action text:=upper(btrim(coalesce(p_target_action,'')));
  v_key text;
  v_template text;
  v_created integer:=0;
  v_recipients integer:=0;
  v_existing boolean;
  v_push_enabled boolean;
  v_keys jsonb:='[]'::jsonb;
begin
  select * into v_old from public.timesheets where timesheet_id=p_old_timesheet_id;
  select * into v_new from public.timesheets where timesheet_id=p_new_timesheet_id;
  -- CONTRACT ownership is authoritative and does not depend on a GCK.  DAILY
  -- may fall through to current TSFIN/workflow/occupant identity.
  select candidate_row.* into v_candidate
  from public.contracts contract_row
  join public.candidates candidate_row on candidate_row.id=contract_row.candidate_id
  where contract_row.id=coalesce(v_new.contract_id,v_old.contract_id)
  limit 1;
  if not found then
    select candidate_row.* into v_candidate
    from public.timesheets_financials financial_row
    join public.candidates candidate_row on candidate_row.id=financial_row.candidate_id
    where financial_row.timesheet_id in (p_new_timesheet_id,p_old_timesheet_id)
      and financial_row.is_current=true
    order by (financial_row.timesheet_id=p_new_timesheet_id) desc,
      financial_row.updated_at desc,financial_row.id desc limit 1;
  end if;
  if not found then
    select workflow_candidate.* into v_candidate
  from public.candidate_submission_workflows workflow
  join public.candidates workflow_candidate on workflow_candidate.id=workflow.candidate_id
  where workflow.id=v_old.candidate_workflow_id
     or workflow.target_timesheet_id=p_old_timesheet_id
     or workflow.anchor_timesheet_id=p_old_timesheet_id
  order by (workflow.id=v_old.candidate_workflow_id) desc,workflow.updated_at_utc desc
  limit 1;
  end if;
  if not found then
    select * into v_candidate from public.candidates candidate_row
    where candidate_row.active=true
      and nullif(btrim(coalesce(candidate_row.key_norm,'')),'') is not null
      and btrim(candidate_row.key_norm)=btrim(coalesce(v_old.occupant_key_norm,''))
    order by candidate_row.id limit 1;
  end if;
  select candidate_app_environment into v_environment
  from public.settings_defaults where id=1;
  if v_candidate.id is null or v_environment is null then
    return jsonb_build_object('notification_required',true,'notification_created',false,
      'notification_created_count',0,'notification_recipient_count',0,
      'notification_recipient_unavailable',true,'notification_dedupe_keys','[]'::jsonb);
  end if;
  v_template:=case when v_action='ALLOW_QR_AGAIN'
    then 'candidate-paper-resubmission-required-v1'
    else 'candidate-electronic-resubmission-required-v1' end;

  for v_account in
    select distinct account_row.*
    from public.candidate_app_accounts account_row
    where account_row.environment=v_environment and account_row.status='ACTIVE'
      and (
        account_row.email_normalized=lower(btrim(coalesce(v_candidate.email,'')))
        or exists(select 1 from public.candidate_app_sessions session_row
          where session_row.account_id=account_row.id
            and session_row.selected_candidate_id=v_candidate.id)
        or exists(select 1 from public.candidate_submission_workflows workflow_row
          where workflow_row.account_id=account_row.id
            and workflow_row.candidate_id=v_candidate.id)
      )
    order by account_row.id
  loop
    v_recipients:=v_recipients+1;
    v_key:='candidate-resubmission-required:'||v_account.id::text||':'
      ||p_new_timesheet_id::text||':'||coalesce(p_new_version,1)::text;
    select exists(select 1 from public.candidate_notifications where dedupe_key=v_key)
      into v_existing;
    v_push_enabled:=coalesce(
      (v_account.notification_preferences_json->>'resubmission_required')::boolean,true
    );
    insert into public.candidate_notifications(
      account_id,candidate_id,workflow_id,timesheet_id,event_type,preference_category,
      template_key,template_params,deep_link_json,state,push_state,dedupe_key,created_at_utc
    ) values (
      v_account.id,v_candidate.id,null,p_new_timesheet_id,'RESUBMISSION_REQUIRED',
      'resubmission_required',v_template,
      jsonb_build_object('route',case when v_action='ALLOW_QR_AGAIN' then 'PAPER' else 'ELECTRONIC' end),
      jsonb_build_object('type','timesheet','timesheet_id',p_new_timesheet_id,
        'submission_route',case when v_action='ALLOW_QR_AGAIN' then 'PAPER' else 'ELECTRONIC' end),
      'UNREAD',case when v_push_enabled then 'PENDING' else 'SKIPPED' end,
      v_key,p_now_utc
    ) on conflict (dedupe_key) do update set dedupe_key=excluded.dedupe_key;
    if not v_existing then v_created:=v_created+1; end if;
    v_keys:=v_keys||jsonb_build_array(v_key);
  end loop;
  return jsonb_build_object(
    'notification_required',true,'notification_created',v_created>0,
    'notification_created_count',v_created,'notification_recipient_count',v_recipients,
    'notification_recipient_unavailable',v_recipients=0,
    'notification_dedupe_keys',v_keys
  );
end;
$function$;

-- Release the QR/paper resubmission push only in the transaction that makes
-- the canonical pack durable and READY.  The route transition itself merely
-- records PAPER intent and therefore cannot claim documents are ready early.
create or replace function private._candidate_qr_pack_ready_notification_v1()
returns trigger
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_notification jsonb;
begin
  if not private._candidate_feature_enabled_current_v1('candidate_route_confirmation')
     or new.candidate_submission_route_intent is distinct from 'PAPER'
     or new.sheet_scope is distinct from 'WEEKLY'::public.timesheet_scope_enum
     or upper(coalesce(new.document_state::text,''))<>'READY'
     or upper(coalesce(new.qr_status::text,'')) not in ('PENDING','SENT','READY') then
    return new;
  end if;
  if new.current_document_version_id is null or not exists(
    select 1 from public.invoice_document_versions document_version
    where document_version.id=new.current_document_version_id
      and document_version.entity_type='TIMESHEET'
      and document_version.entity_id=new.timesheet_id
      and upper(document_version.status)='READY'
  ) then
    return new;
  end if;
  if exists(
    select 1
    from public.candidate_submission_workflows workflow
    where workflow.route='PAPER'
      and workflow.state='AWAITING_PAPER_RETURN'
      and (workflow.target_timesheet_id=new.timesheet_id
        or workflow.anchor_timesheet_id=new.timesheet_id)
  ) then
    return new;
  end if;
  v_notification:=private._timesheet_route_resubmission_notifications_v1(
    new.timesheet_id,new.timesheet_id,new.version,'ALLOW_QR_AGAIN',now()
  );
  new.candidate_submission_route_intent:=null;
  return new;
end;
$function$;

drop trigger if exists timesheets_candidate_qr_pack_ready_notification_trg
  on public.timesheets;
create trigger timesheets_candidate_qr_pack_ready_notification_trg
before update of document_state,current_document_version_id on public.timesheets
for each row execute function private._candidate_qr_pack_ready_notification_v1();

create or replace function public.timesheet_route_version_preview_v1(
  p_current_timesheet_id uuid,
  p_target_action text
)
returns jsonb
language sql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
  select private._timesheet_route_change_context_v1(p_current_timesheet_id,p_target_action);
$function$;

create or replace function public.timesheet_route_version_confirmed_v1(
  p_current_timesheet_id uuid,
  p_expected_timesheet_id uuid,
  p_expected_row_signature text,
  p_expected_context_sha256 text,
  p_target_action text,
  p_actor_user_id uuid,
  p_reason_code text default null,
  p_reason_note text default null,
  p_idempotency_key text default null,
  p_allow_manual_only boolean default false,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_action text:=upper(btrim(coalesce(p_target_action,'')));
  v_reason_code text:=upper(btrim(coalesce(p_reason_code,'')));
  v_reason_note text:=nullif(btrim(coalesce(p_reason_note,'')),'');
  v_requested public.timesheets%rowtype;
  v_current public.timesheets%rowtype;
  v_context jsonb;
  v_result jsonb;
  v_workflow_result jsonb:='{}'::jsonb;
  v_notification_result jsonb:=jsonb_build_object(
    'notification_required',false,'notification_created',false,
    'notification_recipient_unavailable',false,'notification_dedupe_keys','[]'::jsonb
  );
  v_current_signature text;
  v_new_timesheet_id uuid;
  v_new_version integer;
  v_reason_required boolean;
  v_retire_workflow boolean:=false;
  v_retirement_reason text;
  v_intervention_reasons constant text[]:=array[
    'CANDIDATE_SUPPLIED_MANUAL_TIMESHEET',
    'CANDIDATE_REPORTED_HOURS_INCORRECT',
    'HIRING_MANAGER_REPORTED_HOURS_INCORRECT',
    'ELECTRONIC_SUBMISSION_TECHNICAL_FAILURE',
    'OTHER_EXCEPTIONAL_OFFICE_INTERVENTION'
  ];
begin
  if not private._candidate_feature_enabled_current_v1('candidate_route_confirmation') then
    raise exception 'CANDIDATE_ROUTE_CONFIRMATION_DISABLED' using errcode='42501';
  end if;
  if p_actor_user_id is null or p_expected_timesheet_id is null
     or nullif(btrim(coalesce(p_expected_row_signature,'')),'') is null
     or coalesce(p_expected_context_sha256,'') !~ '^[0-9a-fA-F]{64}$' then
    raise exception 'ROUTE_CHANGE_CONFIRMATION_INPUT_INVALID' using errcode='22023';
  end if;
  select * into v_requested from public.timesheets
  where timesheet_id=p_current_timesheet_id;
  if not found then raise exception 'TIMESHEET_NOT_FOUND' using errcode='P0002'; end if;
  perform pg_advisory_xact_lock(hashtext(btrim(v_requested.booking_id)));
  perform 1 from public.timesheets
  where booking_id=v_requested.booking_id for update;
  select * into v_current from public.timesheets
  where booking_id=v_requested.booking_id and is_current=true
  order by version desc,updated_at desc,created_at desc limit 1 for update;
  if v_current.timesheet_id is distinct from p_expected_timesheet_id then
    raise exception 'TIMESHEET_MOVED' using errcode='40001',detail=jsonb_build_object(
      'current_timesheet_id',v_current.timesheet_id)::text;
  end if;
  perform 1 from public.contract_weeks
  where timesheet_id=v_current.timesheet_id for update;
  perform 1 from public.timesheets_financials
  where timesheet_id=v_current.timesheet_id and is_current=true for update;
  perform 1 from public.candidate_submission_workflows workflow
  where workflow.id=v_current.candidate_workflow_id
     or workflow.target_timesheet_id=v_current.timesheet_id
     or workflow.anchor_timesheet_id=v_current.timesheet_id for update;
  perform 1 from public.candidate_approval_requests request_row
  where request_row.workflow_id in (
    select workflow.id from public.candidate_submission_workflows workflow
    where workflow.id=v_current.candidate_workflow_id
       or workflow.target_timesheet_id=v_current.timesheet_id
       or workflow.anchor_timesheet_id=v_current.timesheet_id
  ) for update;

  v_context:=private._timesheet_route_change_context_v1(v_current.timesheet_id,v_action);
  v_current_signature:=nullif(btrim(coalesce(v_context->>'current_row_signature','')),'');
  if v_current_signature is distinct from btrim(p_expected_row_signature) then
    raise exception 'ROW_SIGNATURE_MISMATCH' using errcode='40001',detail=jsonb_build_object(
      'expected_row_signature',p_expected_row_signature,
      'current_row_signature',v_current_signature)::text;
  end if;
  if lower(v_context->>'context_sha256') is distinct from lower(p_expected_context_sha256) then
    raise exception 'ROUTE_CHANGE_CONTEXT_CHANGED' using errcode='40001',detail=jsonb_build_object(
      'warning_code',v_context->>'warning_code',
      'current_context_sha256',v_context->>'context_sha256')::text;
  end if;
  if not coalesce((v_context->>'permitted_action')::boolean,false) then
    raise exception 'ROUTE_CHANGE_NOT_PERMITTED' using errcode='55000',detail=v_context::text;
  end if;
  v_reason_required:=coalesce((v_context->>'reason_required')::boolean,false);
  if v_reason_required and not (v_reason_code=any(v_intervention_reasons)) then
    raise exception 'ROUTE_INTERVENTION_REASON_REQUIRED' using errcode='22023';
  end if;
  if v_reason_code='OTHER_EXCEPTIONAL_OFFICE_INTERVENTION' and v_reason_note is null then
    raise exception 'ROUTE_INTERVENTION_REASON_NOTE_REQUIRED' using errcode='22023';
  end if;

  v_retire_workflow:=v_action in (
    'CONVERT_QR_TO_MANUAL','DISABLE_QR','INVALIDATE_QR','REISSUE_QR',
    'SWITCH_TO_MANUAL','SWITCH_DAILY_TO_MANUAL'
  );
  v_retirement_reason:=case when v_action in ('INVALIDATE_QR','REISSUE_QR')
    then 'QR_REPLACED_BY_OFFICE' else v_reason_code end;
  if v_retire_workflow then
    v_workflow_result:=private._timesheet_route_supersede_candidate_v1(
      nullif(v_context->>'active_workflow_id','')::uuid,v_action,
      v_retirement_reason,v_reason_note,p_actor_user_id,p_now_utc
    );
  end if;
  perform set_config('cloudtms.route_transition_confirmed','on',true);
  v_result:=public.timesheet_route_version_rotate(
    v_current.timesheet_id,p_expected_timesheet_id,v_action,p_actor_user_id,
    case when v_action='REVERT_TO_ELECTRONIC' then true else p_allow_manual_only end
  );
  v_new_timesheet_id:=nullif(v_result->>'new_timesheet_id','')::uuid;
  v_new_version:=nullif(v_result->>'new_version','')::integer;

  if v_reason_required then
    update public.timesheets set
      revoked_reason=left(coalesce(revoked_reason,v_action)||':'||v_reason_code,500),
      updated_at=p_now_utc
    where timesheet_id=v_current.timesheet_id and is_current=false;
  end if;
  perform private._candidate_audit_v1(
    'timesheet_route',v_current.timesheet_id::text,'ROUTE_VERSION_CONFIRMED',
    v_context-'context_sha256',
    jsonb_build_object('result',v_result,'reason_code',nullif(v_reason_code,''),
      'reason_note',v_reason_note,'idempotency_key',nullif(btrim(coalesce(p_idempotency_key,'')),'')),
    nullif(v_reason_code||case when v_reason_note is not null then ': '||v_reason_note else '' end,''),
    p_actor_user_id,nullif(btrim(coalesce(p_idempotency_key,'')),''),p_now_utc
  );

  if v_action='ALLOW_ELECTRONIC_AGAIN'
     and v_new_timesheet_id is not null then
    v_notification_result:=private._timesheet_route_resubmission_notifications_v1(
      v_current.timesheet_id,v_new_timesheet_id,v_new_version,v_action,p_now_utc
    );
  end if;
  return v_result||jsonb_build_object(
    'warning_code',v_context->>'warning_code',
    'confirmed_context_sha256',v_context->>'context_sha256',
    'intervention_reason_code',nullif(v_reason_code,''),
    'intervention_reason_note',v_reason_note,
    'workflow_retirement',v_workflow_result,
    'fresh_submission_required',v_action in ('ALLOW_ELECTRONIC_AGAIN','ALLOW_QR_AGAIN','INVALIDATE_QR','REISSUE_QR'),
    'notification_deferred_until_pack_ready',v_action in ('ALLOW_QR_AGAIN','INVALIDATE_QR','REISSUE_QR'),
    'retain_historical_evidence',true
  )||v_notification_result;
end;
$function$;

-- Preserve exact legacy behaviour while the coordinated route-confirmation
-- feature is off, but make historical QR restoration unreachable once the new
-- UI/API route is enabled.  Fresh ALLOW_QR_AGAIN/REISSUE_QR generations are the
-- only ordinary product routes after cutover.
do $migration$
begin
  if to_regprocedure('private._timesheet_qr_restore_legacy_v1(uuid,uuid,text,uuid)') is null
     and to_regprocedure('public.timesheet_qr_restore_version(uuid,uuid,text,uuid)') is not null then
    alter function public.timesheet_qr_restore_version(uuid,uuid,text,uuid) set schema private;
    alter function private.timesheet_qr_restore_version(uuid,uuid,text,uuid)
      rename to _timesheet_qr_restore_legacy_v1;
  end if;
end;
$migration$;

create or replace function public.timesheet_qr_restore_version(
  p_timesheet_id uuid,
  p_expected_timesheet_id uuid,
  p_restore_kind text,
  p_actor_user_id uuid
)
returns table (
  timesheet_id uuid,
  restored_version integer,
  sheet_scope text,
  submission_mode text,
  qr_status text,
  qr_token text,
  restored_has_signed_pdf boolean
)
language plpgsql
security definer
set search_path to 'public'
as $function$
begin
  if private._candidate_feature_enabled_current_v1('candidate_route_confirmation') then
    raise exception 'QR_RESTORE_RETIRED_USE_FRESH_GENERATION'
      using errcode='55000',detail=jsonb_build_object(
        'code','QR_RESTORE_RETIRED_USE_FRESH_GENERATION',
        'allowed_actions',jsonb_build_array('ALLOW_QR_AGAIN','REISSUE_QR')
      )::text;
  end if;
  return query select * from private._timesheet_qr_restore_legacy_v1(
    p_timesheet_id,p_expected_timesheet_id,p_restore_kind,p_actor_user_id
  );
end;
$function$;

revoke all on function private._timesheet_route_change_context_v1(uuid,text)
  from public,anon,authenticated,service_role;
revoke all on function private._timesheet_exact_electronic_restore_proof_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private._timesheet_route_version_core_v1(uuid,uuid,text,uuid,boolean)
  from public,anon,authenticated,service_role;
revoke all on function private._candidate_qr_pack_ready_notification_v1()
  from public,anon,authenticated,service_role;
revoke all on function private._timesheet_route_supersede_candidate_v1(uuid,text,text,text,uuid,timestamptz)
  from public,anon,authenticated,service_role;
revoke all on function private._timesheet_route_resubmission_notifications_v1(uuid,uuid,integer,text,timestamptz)
  from public,anon,authenticated,service_role;
do $migration$
begin
  if to_regprocedure('private._timesheet_qr_restore_legacy_v1(uuid,uuid,text,uuid)') is not null then
    execute 'revoke all on function private._timesheet_qr_restore_legacy_v1(uuid,uuid,text,uuid) from public,anon,authenticated,service_role';
  end if;
end;
$migration$;
do $migration$
begin
  if to_regprocedure('private._timesheet_route_version_legacy_v1(uuid,uuid,text,uuid,boolean)') is not null then
    execute 'revoke all on function private._timesheet_route_version_legacy_v1(uuid,uuid,text,uuid,boolean) from public,anon,authenticated,service_role';
  end if;
end;
$migration$;
revoke all on function public.timesheet_route_version_rotate(uuid,uuid,text,uuid,boolean)
  from public,anon;
grant execute on function public.timesheet_route_version_rotate(uuid,uuid,text,uuid,boolean)
  to authenticated,service_role;
revoke all on function public.timesheet_qr_restore_version(uuid,uuid,text,uuid)
  from public,anon;
grant execute on function public.timesheet_qr_restore_version(uuid,uuid,text,uuid)
  to authenticated,service_role;
revoke all on function public.timesheet_route_version_preview_v1(uuid,text)
  from public,anon,authenticated;
revoke all on function public.timesheet_route_version_confirmed_v1(uuid,uuid,text,text,text,uuid,text,text,text,boolean,timestamptz)
  from public,anon,authenticated;
grant execute on function public.timesheet_route_version_preview_v1(uuid,text) to service_role;
grant execute on function public.timesheet_route_version_confirmed_v1(uuid,uuid,text,text,text,uuid,text,text,text,boolean,timestamptz)
  to service_role;

comment on function public.timesheet_route_version_preview_v1(uuid,text) is
  'Service-only signed-state route-change preflight. Returns warning code, permissions and a stale-safe context digest; performs no mutation.';
comment on function public.timesheet_route_version_confirmed_v1(uuid,uuid,text,text,text,uuid,text,text,text,boolean,timestamptz) is
  'Sole confirmed route-transition adapter: rechecks context under lock, records intervention reason, retires live Candidate approval, preserves immutable history and creates idempotent resubmission notifications.';
