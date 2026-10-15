async function handleTimesheetDailyManualUpsert(env, req, timesheetId) {
  const enc = encodeURIComponent;

  // 1) Auth
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  if (!timesheetId) {
    return withCORS(env, req, badRequest('timesheet_id is required'));
  }

  // 2) Parse body
  let body;
  try {
    body = await parseJSONBody(req);
  } catch {
    return withCORS(env, req, badRequest('Invalid JSON'));
  }

  const requestContext = String(body?.context || body?.mode || '').trim().toLowerCase();
  const bulkAuthoriseMode = !!(
    body?.bulk_authorise === true ||
    body?.bulkAuthorise === true ||
    body?.bulk_authorise_mode === true ||
    body?.bulkAuthoriseMode === true ||
    requestContext === 'bulk_authorise'
  );
  const bulkProcessMode = !bulkAuthoriseMode && !!(
    body?.bulk_process === true ||
    body?.bulkProcess === true ||
    body?.bulk_process_mode === true ||
    body?.bulkProcessMode === true ||
    requestContext === 'bulk_process'
  );
  const bulkPatchResponseRequested = !!(
    bulkAuthoriseMode ||
    bulkProcessMode ||
    body?.return_bulk_patch === true ||
    body?.returnBulkPatch === true
  );
  const minimalLifecycleMutation = !!(
    body?.minimal_lifecycle_mutation === true ||
    body?.minimalLifecycleMutation === true ||
    body?.requires_affected_row_refresh === true ||
    body?.requiresAffectedRowRefresh === true ||
    String(body?.response_context || body?.responseContext || '').trim().toLowerCase() === 'minimal_lifecycle_mutation'
  );
  const fullBulkPatchResponseRequested = bulkPatchResponseRequested && !minimalLifecycleMutation;
  const bulkDatasetMode = bulkAuthoriseMode ? 'authorise' : 'process';
  const bulkResponseContext = bulkAuthoriseMode ? 'bulk_authorise' : (bulkProcessMode ? 'bulk_process' : 'standard');
  const expectedRowSignature = String(
    body?.expected_row_signature ||
    body?.expectedRowSignature ||
    body?.row_signature ||
    body?.rowSignature ||
    ''
  ).trim();

  const processNowIntent = !!(
    body?.process_now === true ||
    body?.processNow === true ||
    body?.process === true ||
    String(body?.action || '').trim().toUpperCase() === 'PROCESS' ||
    String(body?.operation || '').trim().toUpperCase() === 'PROCESS'
  );

  let dailyManualSaveDiagCurrentTimesheetId = null;
  const dailyManualSaveDiagText = (v) => {
    const s = String(v == null ? '' : v).trim();
    return s || null;
  };
  const dailyManualSaveDiagNum = (v) => {
    const n = Number(v);
    return Number.isFinite(n) ? n : 0;
  };
  const dailyManualSaveDiagRound2 = (v) => Math.round(dailyManualSaveDiagNum(v) * 100) / 100;
  const dailyManualSaveDiagKeys = (obj) => (obj && typeof obj === 'object' && !Array.isArray(obj)) ? Object.keys(obj).sort() : [];
  const dailyManualSaveDiagTimesheet = (row) => {
    if (!row || typeof row !== 'object') return null;
    return {
      timesheet_id: row.timesheet_id || null,
      booking_id: row.booking_id || null,
      version: row.version ?? null,
      is_current: row.is_current === true,
      status: row.status || null,
      sheet_scope: row.sheet_scope || null,
      submission_mode: row.submission_mode || null,
      authorised: row.authorised_at_server != null && String(row.authorised_at_server).trim() !== '',
      authorised_at_server: row.authorised_at_server || null,
      revoked_at: row.revoked_at || null,
      qr_status: row.qr_status || null,
      worked_start_iso: row.worked_start_iso || null,
      worked_end_iso: row.worked_end_iso || null,
      break_start_iso: row.break_start_iso || null,
      break_end_iso: row.break_end_iso || null,
      break_minutes: row.break_minutes ?? null,
      worked_minutes: row.worked_minutes ?? null,
      reference_number_present: dailyManualSaveDiagText(row.reference_number) ? true : false,
      updated_at: row.updated_at || null
    };
  };
  const dailyManualSaveDiagTsfin = (row) => {
    if (!row || typeof row !== 'object') return null;
    return {
      tsfin_id: row.id || null,
      timesheet_id: row.timesheet_id || null,
      is_current: row.is_current === true,
      processing_status: row.processing_status || null,
      updated_at: row.updated_at || null,
      total_hours: dailyManualSaveDiagRound2(row.total_hours),
      hours_day: dailyManualSaveDiagRound2(row.hours_day),
      hours_night: dailyManualSaveDiagRound2(row.hours_night),
      hours_sat: dailyManualSaveDiagRound2(row.hours_sat),
      hours_sun: dailyManualSaveDiagRound2(row.hours_sun),
      hours_bh: dailyManualSaveDiagRound2(row.hours_bh),
      total_pay_ex_vat: dailyManualSaveDiagRound2(row.total_pay_ex_vat),
      total_charge_ex_vat: dailyManualSaveDiagRound2(row.total_charge_ex_vat),
      margin_ex_vat: dailyManualSaveDiagRound2(row.margin_ex_vat),
      expenses_pay_ex_vat: dailyManualSaveDiagRound2(row.expenses_pay_ex_vat),
      expenses_charge_ex_vat: dailyManualSaveDiagRound2(row.expenses_charge_ex_vat),
      mileage_units: dailyManualSaveDiagRound2(row.mileage_units),
      mileage_pay_ex_vat: dailyManualSaveDiagRound2(row.mileage_pay_ex_vat),
      mileage_charge_ex_vat: dailyManualSaveDiagRound2(row.mileage_charge_ex_vat),
      paid: !!row.paid_at_utc,
      locked_by_invoice: !!row.locked_by_invoice_id
    };
  };
  const dailyManualSaveDiagResponseContains = (payload) => ({
    timesheet: !!(payload?.timesheet && typeof payload.timesheet === 'object'),
    tsfin: !!(payload?.tsfin && typeof payload.tsfin === 'object'),
    row_signature: !!dailyManualSaveDiagText(payload?.row_signature),
    backend_row_signature: !!dailyManualSaveDiagText(payload?.backend_row_signature),
    mutation_row_signature: !!dailyManualSaveDiagText(payload?.mutation_row_signature),
    expected_row_signature: !!dailyManualSaveDiagText(payload?.expected_row_signature),
    affected_rows: Array.isArray(payload?.affected_rows),
    affected_rows_count: Array.isArray(payload?.affected_rows) ? payload.affected_rows.length : null,
    row_patch: !!(payload?.row_patch && typeof payload.row_patch === 'object'),
    row_patches: Array.isArray(payload?.row_patches),
    data_row: !!(payload?.data_row && typeof payload.data_row === 'object'),
    cache_invalidation_hints: !!(payload?.cache_invalidation_hints && typeof payload.cache_invalidation_hints === 'object'),
    refresh_required: payload?.refresh_required === true,
    requires_affected_row_refresh: payload?.requires_affected_row_refresh === true,
    summary_row_hint: !!(payload?.summary_row_hint && typeof payload.summary_row_hint === 'object')
  });
  const dailyManualSaveSignatureLog = (step, extra = {}) => {
    try {
      let routePath = null;
      try { routePath = new URL(req.url).pathname; } catch {}
      console.log('[TIMESHEET_SAVE_SIGNATURE_DIAG]', JSON.stringify({
        tag: 'TIMESHEET_SAVE_SIGNATURE_DIAG',
        handler: 'handleTimesheetDailyManualUpsert',
        route_family: 'DAILY_MANUAL_UPSERT',
        step,
        route_path: routePath,
        requested_timesheet_id: timesheetId || null,
        current_timesheet_id: dailyManualSaveDiagCurrentTimesheetId || null,
        expected_row_signature_present: !!expectedRowSignature,
        expected_row_signature: expectedRowSignature || null,
        response_context: bulkResponseContext,
        bulk_process_response_requested: bulkProcessMode,
        bulk_authorise_response_requested: bulkAuthoriseMode,
        minimal_lifecycle_mutation: minimalLifecycleMutation,
        server_utc: new Date().toISOString(),
        ...(extra && typeof extra === 'object' && !Array.isArray(extra) ? extra : {})
      }));
    } catch {}
  };

  dailyManualSaveSignatureLog('received', {
    body_keys: dailyManualSaveDiagKeys(body),
    expected_timesheet_id: body?.expected_timesheet_id || null,
    schedule_json_present: Object.prototype.hasOwnProperty.call(body || {}, 'schedule_json'),
    reference_number_present: Object.prototype.hasOwnProperty.call(body || {}, 'reference_number'),
    response_context: bulkResponseContext
  });

  if (bulkProcessMode && processNowIntent) {
    return withCORS(
      env,
      req,
      new Response(
        JSON.stringify({
          error_code: 'USE_DAILY_MANUAL_PROCESS_ATOMIC',
          message: 'Bulk Process must use the daily manual process endpoint so save and process happen through timesheet_daily_manual_process_atomic.',
          timesheet_id: timesheetId
        }),
        { status: 409, headers: { 'Content-Type': 'application/json' } }
      )
    );
  }

  // NEW: guarded write (optimistic concurrency)
  const expectedTimesheetId = body?.expected_timesheet_id || null;
  const guard = await guardCurrentTimesheetWrite(env, req, timesheetId, expectedTimesheetId);
  if (!guard.ok) return guard.res;

  // From here on, always act on the CURRENT timesheet row.
  let currentTimesheetId = guard.resolved.current_timesheet_id;
  dailyManualSaveDiagCurrentTimesheetId = currentTimesheetId || null;
  dailyManualSaveSignatureLog('guard_resolved_current', {
    requested_timesheet_id: guard.resolved.requested_timesheet_id || timesheetId || null,
    current_timesheet_id: currentTimesheetId || null,
    booking_id: guard.resolved.booking_id || null,
    current_version: guard.resolved.current_version ?? null,
    was_stale: !!guard.resolved.was_stale
  });

  const has = (k) => Object.prototype.hasOwnProperty.call(body || {}, k);

  // Optional helper input (do NOT patch to DB unless you actually have this column)
  let worked_date_ymd = body?.worked_date_ymd ? String(body.worked_date_ymd) : null;

  // Worked/break inputs (only patch if provided)
  const worked_start_iso_in = has('worked_start_iso') ? body.worked_start_iso : undefined;
  const worked_end_iso_in   = has('worked_end_iso')   ? body.worked_end_iso   : undefined;
  const break_start_iso_in  = has('break_start_iso')  ? body.break_start_iso  : undefined;
  const break_end_iso_in    = has('break_end_iso')    ? body.break_end_iso    : undefined;
  const break_minutes_in    = has('break_minutes')    ? body.break_minutes    : undefined;

  // NEW: schedule_json payload (preferred FE shape for daily upsert)
  const schedule_json_in    = has('schedule_json')    ? body.schedule_json    : undefined;
  const reference_number_in = has('reference_number') ? body.reference_number : undefined;

  // QR action hints from FE decision modal (legacy booleans still supported)
  const rawIssueQr        = body?.issue_qr === true;
  const rawDisableQr      = body?.disable_qr === true;
  const rawRevokeToManual = body?.revoke_to_manual === true;
  const rawRevokeQr       = body?.revoke_qr === true;   // legacy
  const rawReissueQr      = body?.reissue_qr === true;  // legacy

  // ✅ NEW: preferred enum (from After-save decision modal)
  const qrActionEnumRaw = String(body?.qr_action || body?.qrAction || '').trim().toUpperCase();
  const enumOk = (x) => (
    x === 'INVALIDATE' ||
    x === 'REVOKE_TO_MANUAL' ||
    x === 'REISSUE' ||
    x === 'ISSUE' ||
    x === 'DISABLE' ||
    x === 'SAVE_WITHOUT_ISSUE'
  );

  const now = nowIso();

  // ✅ FIX: daily-manual-upsert no longer issues/resends QR.
  // These legacy variables must exist so the function does not throw ReferenceError.
  const qrIssued   = false;
  const qrReissued = false;
  const qrToken    = null;
  const qr_r2_key  = null;
  const pdfKey     = null;

  // Helpers
  const trimStr = (v) => String(v == null ? '' : v).trim();

  const parseMaybeJsonObj = (v) => {
    if (v == null) return null;
    if (typeof v === 'object') return v;
    if (typeof v === 'string') {
      try {
        const parsed = JSON.parse(v);
        return (parsed && typeof parsed === 'object') ? parsed : null;
      } catch {
        return null;
      }
    }
    return null;
  };

  const loadDailyManualBulkPatchRow = async ({ timesheetId: patchTimesheetId, datasetMode = 'process', changedDomains = [] } = {}) => {
    const tsId = trimStr(patchTimesheetId);
    if (!tsId) return null;

    const domainList = Array.isArray(changedDomains)
      ? changedDomains.map(v => trimStr(v)).filter(Boolean)
      : [];

    const filters = {
      dataset_mode: datasetMode === 'authorise' ? 'authorise' : 'process',
      projection: 'dataset_row',
      profile: 'list',
      timesheet_id: tsId,
      actor_user_id: user?.id || null
    };

    if (domainList.length) filters.changed_domains = domainList;

    const patchRes = await sbRpc(env, 'bulk_timesheet_row_patch_v1', { p_filters: filters }, { timeoutMs: 45000 });
    const first = Array.isArray(patchRes) ? (patchRes[0] || null) : patchRes;
    const rowJson = parseMaybeJsonObj(first?.row_json) || parseMaybeJsonObj(first?.bulk_timesheet_row_patch_v1) || parseMaybeJsonObj(first);
    return (rowJson && typeof rowJson === 'object' && !Array.isArray(rowJson)) ? rowJson : null;
  };

  const normIso = (v) => {
    if (v === undefined) return undefined;
    if (v === null) return null;
    const s = trimStr(v);
    return s ? s : null; // treat '' as null (clear)
  };

  const normTextOrNull = (v) => {
    if (v === undefined) return undefined;
    const s = trimStr(v);
    return s ? s : null;
  };

  const parseOptionalBreakMinutesInput = (v) => {
    if (v === undefined) return undefined;
    if (v === null) return null;
    const s = trimStr(v);
    if (!s) return null;
    const n = Number(s);
    return Number.isFinite(n) ? n : NaN;
  };

  const hasIso = (v) => (v != null && trimStr(v) !== '');

  const isoToMs = (iso) => {
    if (!hasIso(iso)) return null;
    const t = new Date(String(iso)).getTime();
    return Number.isFinite(t) ? t : null;
  };

  const bad = (msg) => withCORS(env, req, badRequest(msg));

    // 3) Load DAILY timesheet (current version) — use currentTimesheetId
  let ts = await sbGetOne(
    env,
    `${env.SUPABASE_URL}/rest/v1/timesheets` +
      `?timesheet_id=eq.${enc(currentTimesheetId)}` +
      `&is_current=eq.true` +
      `&select=*`
  );
  if (!ts) return withCORS(env, req, notFound('Timesheet not found'));

  // ✅ Hard rule: reject any attempt to alter hours/schedule while authorised
  if (ts.authorised_at_server) {
    return withCORS(
      env,
      req,
      new Response(
        JSON.stringify({
          error_code: 'TIMESHEET_AUTHORISED_EDIT_BLOCKED',
          message: 'This timesheet is authorised. Unauthorise it before changing hours.'
        }),
        { status: 409, headers: { 'Content-Type': 'application/json' } }
      )
    );
  }

  const sheetScope = String(ts.sheet_scope || '').toUpperCase();
  if (sheetScope !== 'DAILY') {
    return bad('Timesheet is not DAILY; daily-manual-upsert only applies to DAILY sheets');
  }

  const subMode = String(ts.submission_mode || '').toUpperCase();
  if (subMode !== 'MANUAL') {
    return bad('Timesheet must be MANUAL before editing hours. Use switch-daily-to-manual first.');
  }

  // 4) Load TSFIN – invoice lock still blocks direct editing, but paid status alone
  // does not block hours/schedule amendment.
  const tsfin = await sbGetOne(
    env,
    `${env.SUPABASE_URL}/rest/v1/timesheets_financials` +
      `?timesheet_id=eq.${enc(currentTimesheetId)}` +
      `&is_current=eq.true` +
      `&select=*`
  );
  if (!tsfin) return withCORS(env, req, serverError('No TSFIN snapshot found'));

  if (tsfin.locked_by_invoice_id) {
    dailyManualSaveSignatureLog('blocked_invoice_locked', {
      status: 400,
      timesheet: dailyManualSaveDiagTimesheet(ts),
      tsfin: dailyManualSaveDiagTsfin(tsfin)
    });
    return bad('Timesheet already invoiced; cannot edit');
  }

  dailyManualSaveSignatureLog('before_write', {
    timesheet: dailyManualSaveDiagTimesheet(ts),
    tsfin: dailyManualSaveDiagTsfin(tsfin)
  });

  const isDailyManualCurrentlyUnprocessed = (value) =>
    String(value || '').trim().toUpperCase() === 'UNPROCESSED';

  const wasUnprocessedBefore = isDailyManualCurrentlyUnprocessed(tsfin.processing_status);

  const writeDailyManualSaveAudit = async () => {
    try {
      await writeAudit(
        env,
        user,
        'TIMESHEET_DRAFT_SAVED',
        {
          timesheet_id: currentTimesheetId,
          sheet_scope: 'DAILY',
          submission_mode: 'MANUAL',
          source: 'timesheet_daily_manual_upsert',
          qr_action: qrAction,
          processing_status: finAfter?.processing_status ?? null,
          bucket_hours: finAfter ? {
            day:   Number(finAfter.hours_day   ?? 0),
            night: Number(finAfter.hours_night ?? 0),
            sat:   Number(finAfter.hours_sat   ?? 0),
            sun:   Number(finAfter.hours_sun   ?? 0),
            bh:    Number(finAfter.hours_bh    ?? 0),
            total: Number(finAfter.total_hours ?? 0)
          } : null
        },
        { entity: 'timesheets', subject_id: currentTimesheetId, req }
      );
    } catch {}
  };
  // ─────────────────────────────────────────────────────────────
  // NEW: schedule_json -> ISO mapping (runs BEFORE validation)
  // ─────────────────────────────────────────────────────────────
  const scheduleProvided = (schedule_json_in !== undefined);
  let mappedWorkedStartIso = worked_start_iso_in;
  let mappedWorkedEndIso   = worked_end_iso_in;
  let mappedBreakStartIso  = break_start_iso_in;
  let mappedBreakEndIso    = break_end_iso_in;
  let mappedBreakMinutesIn = break_minutes_in;
  let mappedScheduleYmd    = null;

  if (scheduleProvided) {
    if (schedule_json_in == null) return bad('schedule_json must not be null.');
    const mapped = mapCanonicalDailyScheduleToIso(schedule_json_in, { ukLocalToUtcISO });
    if (mapped && mapped.error) return bad(mapped.error);

    mappedScheduleYmd = mapped.ymd || null;
    worked_date_ymd = mapped.ymd || worked_date_ymd;

    mappedWorkedStartIso = mapped.worked_start_iso;
    mappedWorkedEndIso   = mapped.worked_end_iso;
    mappedBreakStartIso  = mapped.break_start_iso;
    mappedBreakEndIso    = mapped.break_end_iso;
    mappedBreakMinutesIn = (mapped.break_minutes != null) ? mapped.break_minutes : null;
  }

  // ─────────────────────────────────────────────────────────────
  // ✅ Pre-validate worked/break ordering + partials BEFORE rotate/patch/rebuild
  // ─────────────────────────────────────────────────────────────
  const shouldPatchWorked = scheduleProvided || worked_start_iso_in !== undefined || worked_end_iso_in !== undefined;
  const shouldPatchBreakTimes = scheduleProvided || break_start_iso_in !== undefined || break_end_iso_in !== undefined;
  const shouldPatchBreakMinutes = scheduleProvided || break_minutes_in !== undefined;
  const shouldPatchActualSchedule = shouldPatchWorked || shouldPatchBreakTimes || shouldPatchBreakMinutes;
  const referenceProvided = (reference_number_in !== undefined);

  let nextWorkedStartIso = (mappedWorkedStartIso !== undefined) ? normIso(mappedWorkedStartIso) : normIso(ts.worked_start_iso);
  let nextWorkedEndIso   = (mappedWorkedEndIso   !== undefined) ? normIso(mappedWorkedEndIso)   : normIso(ts.worked_end_iso);

  let nextBreakStartIso  = (mappedBreakStartIso  !== undefined) ? normIso(mappedBreakStartIso)  : normIso(ts.break_start_iso);
  let nextBreakEndIso    = (mappedBreakEndIso    !== undefined) ? normIso(mappedBreakEndIso)    : normIso(ts.break_end_iso);

  let nextBreakMinutesNum =
    (mappedBreakMinutesIn !== undefined)
      ? parseOptionalBreakMinutesInput(mappedBreakMinutesIn)
      : (ts.break_minutes != null ? Number(ts.break_minutes) : null);

  let hasShiftStart = hasIso(nextWorkedStartIso);
  let hasShiftEnd   = hasIso(nextWorkedEndIso);

  if (hasShiftStart !== hasShiftEnd) {
    return bad('worked_start_iso and worked_end_iso must both be provided (or both cleared).');
  }

  let shiftStartMs = null;
  let shiftEndMs   = null;
  let shiftMinutes = null;

  if (hasShiftStart && hasShiftEnd) {
    shiftStartMs = isoToMs(nextWorkedStartIso);
    shiftEndMs   = isoToMs(nextWorkedEndIso);

    if (shiftStartMs == null || shiftEndMs == null) {
      return bad('worked_start_iso/worked_end_iso must be valid ISO timestamps.');
    }

    const diffMs = shiftEndMs - shiftStartMs;
    if (diffMs <= 0) {
      return bad('worked_end_iso must be after worked_start_iso (overnight shifts must use next-day end ISO).');
    }

    const diffMin = diffMs / 60000;
    if (!(diffMin > 0)) {
      return bad('Worked shift duration must be > 0 minutes.');
    }

    if (diffMs === 0) {
      return bad('worked_start_iso cannot equal worked_end_iso.');
    }

    shiftMinutes = Math.round(diffMin);
    if (shiftMinutes <= 0) {
      return bad('Worked shift duration must be at least 1 minute.');
    }
  }

  const shiftClearedExplicitly = shouldPatchWorked && !hasShiftStart && !hasShiftEnd;
  if (shiftClearedExplicitly) {
    nextBreakStartIso = null;
    nextBreakEndIso = null;
    nextBreakMinutesNum = null;
  }

  const hasBrStart = hasIso(nextBreakStartIso);
  const hasBrEnd   = hasIso(nextBreakEndIso);

  if (hasBrStart !== hasBrEnd) {
    return bad('break_start_iso and break_end_iso must both be provided (or both cleared).');
  }

  const anyBreakInfo =
    (hasBrStart && hasBrEnd) ||
    (mappedBreakMinutesIn !== undefined && nextBreakMinutesNum != null) ||
    (!shiftClearedExplicitly && mappedBreakMinutesIn === undefined && Number.isFinite(nextBreakMinutesNum));

  if (anyBreakInfo && !(hasShiftStart && hasShiftEnd)) {
    return bad('You cannot set break times/minutes without worked_start_iso and worked_end_iso.');
  }

  let computedBreakMinutes = null;

  if (hasBrStart && hasBrEnd) {
    const brStartMs = isoToMs(nextBreakStartIso);
    const brEndMs   = isoToMs(nextBreakEndIso);

    if (brStartMs == null || brEndMs == null) {
      return bad('break_start_iso/break_end_iso must be valid ISO timestamps.');
    }

    const brDiffMs = brEndMs - brStartMs;
    if (brDiffMs <= 0) {
      return bad('break_end_iso must be after break_start_iso (overnight breaks must use next-day end ISO).');
    }

    if (brDiffMs === 0) {
      return bad('break_start_iso cannot equal break_end_iso.');
    }

    if (brStartMs < shiftStartMs || brEndMs > shiftEndMs) {
      return bad('Break window must be fully within the worked shift span.');
    }

    computedBreakMinutes = Math.round(brDiffMs / 60000);
    if (!(computedBreakMinutes > 0)) {
      return bad('Break duration must be > 0 minutes.');
    }

    if (shiftMinutes != null && computedBreakMinutes > shiftMinutes) {
      return bad('Break duration cannot exceed the worked shift duration.');
    }

    nextBreakMinutesNum = computedBreakMinutes;
  } else {
    if (mappedBreakMinutesIn !== undefined) {
      if (nextBreakMinutesNum != null) {
        if (!Number.isFinite(nextBreakMinutesNum) || nextBreakMinutesNum < 0) {
          return bad('break_minutes must be a non-negative number.');
        }
        nextBreakMinutesNum = Math.round(nextBreakMinutesNum);
        if (shiftMinutes != null && nextBreakMinutesNum > shiftMinutes) {
          return bad('break_minutes cannot exceed the worked shift duration.');
        }
      }
    } else if (Number.isFinite(nextBreakMinutesNum) && shiftMinutes != null && nextBreakMinutesNum > shiftMinutes) {
      return bad('Existing break_minutes exceeds the worked shift duration. Please reduce break_minutes or set break_start_iso/break_end_iso.');
    }
  }

  const nextWorkedMinutes = (hasShiftStart && hasShiftEnd && Number.isFinite(shiftMinutes)) ? shiftMinutes : null;

  const currentReferenceNumber = normTextOrNull(ts.reference_number);
  const nextReferenceNumber = referenceProvided ? normTextOrNull(reference_number_in) : currentReferenceNumber;
  const nextReferenceSetAt = referenceProvided
    ? (nextReferenceNumber ? ((nextReferenceNumber !== currentReferenceNumber) ? now : (ts.reference_set_at || now)) : null)
    : (ts.reference_set_at ?? null);

  const nextActualScheduleJson = shouldPatchActualSchedule
    ? buildCanonicalDailyScheduleFromState({
        workedStartIso: nextWorkedStartIso,
        workedEndIso: nextWorkedEndIso,
        breakStartIso: nextBreakStartIso,
        breakEndIso: nextBreakEndIso,
        breakMinutes: nextBreakMinutesNum,
        ymdHint: mappedScheduleYmd || worked_date_ymd || null,
        toLocalParts
      })
    : (ts.actual_schedule_json != null ? ts.actual_schedule_json : null);

  const shouldPatchBreakTimesResolved = shouldPatchBreakTimes || shiftClearedExplicitly;
  const shouldPatchBreakMinutesResolved = shouldPatchBreakMinutes || shiftClearedExplicitly || (hasBrStart && hasBrEnd);

  // ─────────────────────────────────────────────────────────────
  // MINIMAL TIMESHEET AUDIT: capture BEFORE for diffing
  // ─────────────────────────────────────────────────────────────
  const tsBefore = { ...ts };
  const finBefore = { ...tsfin };

  let preBulkPatchRow = null;
  if (fullBulkPatchResponseRequested && expectedRowSignature) {
    try {
      preBulkPatchRow = await loadDailyManualBulkPatchRow({
        timesheetId: currentTimesheetId,
        datasetMode: bulkDatasetMode,
        changedDomains: ['manual', 'save']
      });
    } catch (e) {
      return withCORS(env, req, serverError(`Failed to load daily manual bulk row signature: ${e?.message || e}`));
    }

    const currentRowSignature = String(preBulkPatchRow?.row_signature || '').trim();
    if (!currentRowSignature || currentRowSignature !== expectedRowSignature) {
      return withCORS(
        env,
        req,
        new Response(
          JSON.stringify({
            error: 'ROW_SIGNATURE_MISMATCH',
            error_code: 'ROW_SIGNATURE_MISMATCH',
            message: 'Timesheet changed after it was loaded. Refresh the row and try again.',
            expected_row_signature: expectedRowSignature,
            current_row_signature: currentRowSignature || null,
            current_timesheet_id: currentTimesheetId,
            requested_timesheet_id: guard.resolved.requested_timesheet_id || timesheetId,
            was_stale: !!guard.resolved.was_stale
          }),
          { status: 409, headers: { 'Content-Type': 'application/json' } }
        )
      );
    }
  }

  const hadQrBefore = !!(
    ts.qr_status ||
    ts.qr_token ||
    ts.qr_generated_at ||
    ts.qr_scanned_at
  );

  // ✅ Decide requested QR action (enum preferred)
  let qrAction = null; // 'INVALIDATE' | 'REVOKE_TO_MANUAL' | 'REISSUE' | 'ISSUE' | 'DISABLE' | null

  if (enumOk(qrActionEnumRaw)) {
    // SAVE_WITHOUT_ISSUE means "no QR side-effects here"
    qrAction = (qrActionEnumRaw === 'SAVE_WITHOUT_ISSUE') ? null : qrActionEnumRaw;
  } else if (hadQrBefore) {
    // Legacy fallback (only meaningful if the sheet already had QR activity)
    if (rawReissueQr) qrAction = 'REISSUE';
    else if (rawRevokeToManual || rawRevokeQr) qrAction = 'REVOKE_TO_MANUAL';
    else if (rawDisableQr) qrAction = 'DISABLE';
    else if (rawIssueQr) qrAction = 'ISSUE';
  }

  const preRotateVersion = Number(ts.version || 1);
  let rotated = false;
  let rotateReason = null;

  // Rotate for invalidate/revoke/disable/reissue so restore is possible and old evidence remains in history.
  if (hadQrBefore && (qrAction === 'INVALIDATE' || qrAction === 'REISSUE' || qrAction === 'REVOKE_TO_MANUAL' || qrAction === 'DISABLE')) {
    const why =
      (qrAction === 'INVALIDATE' || qrAction === 'REISSUE') ? 'QR_INVALIDATED_AFTER_CONTENT_CHANGE' :
      (qrAction === 'REVOKE_TO_MANUAL') ? 'QR_REVOKED_TO_MANUAL' :
      'QR_DISABLED_TO_MANUAL';

    rotateReason = why;
    const canonicalQrAction = (
      qrAction === 'INVALIDATE' ? 'INVALIDATE_QR' :
      qrAction === 'REISSUE' ? 'REISSUE_QR' :
      'DISABLE_QR'
    );
    const rotateResult = await sbRpcRecordingGuardRefusal(env, 'timesheet_route_version_rotate', {
      p_current_timesheet_id: currentTimesheetId,
      p_expected_timesheet_id: currentTimesheetId,
      p_target_action: canonicalQrAction,
      p_actor_user_id: user?.id || null,
      p_allow_manual_only: false
    });
    const rotatedTimesheetId =
      rotateResult?.current_timesheet_id ||
      rotateResult?.new_timesheet_id ||
      null;
    if (!rotatedTimesheetId) {
      throw new Error('Canonical QR/version rotation did not return a current timesheet');
    }
    ts = await sbGetOne(
      env,
      `${env.SUPABASE_URL}/rest/v1/timesheets` +
        `?timesheet_id=eq.${enc(rotatedTimesheetId)}&is_current=eq.true&select=*&limit=1`
    );
    if (!ts) throw new Error('Canonical QR/version rotation result was not found');
    rotated = true;
    currentTimesheetId = ts.timesheet_id;

    try {
      if (ts.booking_id) await purgeSupersededTimesheetArtifactsForBooking(env, ts.booking_id);
    } catch (e) {
      try {
        console.warn('[DAILY_MANUAL_UPSERT][ROTATE] purge failed (non-fatal)', {
          booking_id: ts.booking_id || null,
          err: e?.message || String(e)
        });
      } catch {}
    }
  }

  // 5) Patch worked/break/reference fields (on CURRENT timesheet id)
  const patchBody = { updated_at: now };

  if (shouldPatchWorked) {
    patchBody.worked_start_iso = normIso(nextWorkedStartIso);
    patchBody.worked_end_iso   = normIso(nextWorkedEndIso);
    patchBody.worked_minutes   = nextWorkedMinutes;
  }

  if (shouldPatchBreakTimesResolved) {
    patchBody.break_start_iso = normIso(nextBreakStartIso);
    patchBody.break_end_iso   = normIso(nextBreakEndIso);
  }

  if (shouldPatchBreakMinutesResolved) {
    patchBody.break_minutes = (Number.isFinite(nextBreakMinutesNum) ? Math.round(nextBreakMinutesNum) : null);
  }

  if (shouldPatchActualSchedule) {
    patchBody.actual_schedule_json = nextActualScheduleJson;
  }

  if (referenceProvided) {
    patchBody.reference_number = nextReferenceNumber;
    patchBody.reference_set_at = nextReferenceSetAt;
  }

  dailyManualSaveSignatureLog('timesheet_patch_request', {
    current_timesheet_id: currentTimesheetId || null,
    changed_fields: Object.keys(patchBody || {}).sort(),
    patch_contains: {
      worked_start_iso: Object.prototype.hasOwnProperty.call(patchBody, 'worked_start_iso'),
      worked_end_iso: Object.prototype.hasOwnProperty.call(patchBody, 'worked_end_iso'),
      worked_minutes: Object.prototype.hasOwnProperty.call(patchBody, 'worked_minutes'),
      break_start_iso: Object.prototype.hasOwnProperty.call(patchBody, 'break_start_iso'),
      break_end_iso: Object.prototype.hasOwnProperty.call(patchBody, 'break_end_iso'),
      break_minutes: Object.prototype.hasOwnProperty.call(patchBody, 'break_minutes'),
      actual_schedule_json: Object.prototype.hasOwnProperty.call(patchBody, 'actual_schedule_json'),
      reference_number: Object.prototype.hasOwnProperty.call(patchBody, 'reference_number')
    },
    rotated: rotated === true,
    rotate_reason: rotateReason || null
  });

  let canonicalDailySave;
  try {
    canonicalDailySave = await saveAndRecalculateCanonicalDaily(env, {
      timesheetId: currentTimesheetId,
      timesheetPatch: patchBody,
      forceUnprocessed: wasUnprocessedBefore
    });
  } catch (e) {
    return withCORS(env, req, serverError(`Failed to save/recalculate DAILY timesheet: ${e?.message || e}`));
  }

  const updatedTs = canonicalDailySave.timesheet;
  let finAfter = canonicalDailySave.financials;

  dailyManualSaveSignatureLog('after_timesheet_patch', {
    current_timesheet_id: currentTimesheetId || null,
    timesheet_before: dailyManualSaveDiagTimesheet(tsBefore),
    timesheet_after: dailyManualSaveDiagTimesheet(updatedTs)
  });

  dailyManualSaveSignatureLog('after_tsfin_rebuild', {
    current_timesheet_id: currentTimesheetId || null,
    tsfin_before: dailyManualSaveDiagTsfin(finBefore),
    tsfin_after: dailyManualSaveDiagTsfin(finAfter),
    was_unprocessed_before: wasUnprocessedBefore
  });

  // ─────────────────────────────────────────────────────────────
  // MINIMAL TIMESHEET AUDIT: log HOURS_CHANGED only when meaningful
  // ─────────────────────────────────────────────────────────────
  const changedFields = [];
  const same = (a, b) => String(a ?? '') === String(b ?? '');

  if ((scheduleProvided || worked_start_iso_in !== undefined) && !same(tsBefore.worked_start_iso, updatedTs?.worked_start_iso)) changedFields.push('worked_start_iso');
  if ((scheduleProvided || worked_end_iso_in   !== undefined) && !same(tsBefore.worked_end_iso,   updatedTs?.worked_end_iso))   changedFields.push('worked_end_iso');
  if ((scheduleProvided || break_start_iso_in  !== undefined) && !same(tsBefore.break_start_iso,  updatedTs?.break_start_iso))  changedFields.push('break_start_iso');
  if ((scheduleProvided || break_end_iso_in    !== undefined) && !same(tsBefore.break_end_iso,    updatedTs?.break_end_iso))    changedFields.push('break_end_iso');
  if ((scheduleProvided || break_minutes_in !== undefined || (hasBrStart && hasBrEnd)) && !same(tsBefore.break_minutes, updatedTs?.break_minutes)) changedFields.push('break_minutes');

  const hoursChangedBuckets = !!(finAfter && (
    Number(finBefore.hours_day   ?? 0) !== Number(finAfter.hours_day   ?? 0) ||
    Number(finBefore.hours_night ?? 0) !== Number(finAfter.hours_night ?? 0) ||
    Number(finBefore.hours_sat   ?? 0) !== Number(finAfter.hours_sat   ?? 0) ||
    Number(finBefore.hours_sun   ?? 0) !== Number(finAfter.hours_sun   ?? 0) ||
    Number(finBefore.hours_bh    ?? 0) !== Number(finAfter.hours_bh    ?? 0) ||
    Number(finBefore.total_hours ?? 0) !== Number(finAfter.total_hours ?? 0)
  ));

  if (changedFields.length || hoursChangedBuckets) {
    try {
      await writeAudit(
        env,
        user,
        'TIMESHEET_HOURS_CHANGED',
        {
          timesheet_id: currentTimesheetId,
          sheet_scope: 'DAILY',
          submission_mode: 'MANUAL',
          changed_fields: changedFields,
          rotated_for_qr: rotated,
          rotate_reason: rotateReason,
          before: {
            worked_start_iso: tsBefore.worked_start_iso ?? null,
            worked_end_iso:   tsBefore.worked_end_iso   ?? null,
            break_start_iso:  tsBefore.break_start_iso  ?? null,
            break_end_iso:    tsBefore.break_end_iso    ?? null,
            break_minutes:    tsBefore.break_minutes    ?? null,
            bucket_hours: {
              day:   Number(finBefore.hours_day   ?? 0),
              night: Number(finBefore.hours_night ?? 0),
              sat:   Number(finBefore.hours_sat   ?? 0),
              sun:   Number(finBefore.hours_sun   ?? 0),
              bh:    Number(finBefore.hours_bh    ?? 0),
              total: Number(finBefore.total_hours ?? 0)
            }
          },
          after: {
            worked_start_iso: updatedTs?.worked_start_iso ?? null,
            worked_end_iso:   updatedTs?.worked_end_iso   ?? null,
            break_start_iso:  updatedTs?.break_start_iso  ?? null,
            break_end_iso:    updatedTs?.break_end_iso    ?? null,
            break_minutes:    updatedTs?.break_minutes    ?? null,
            bucket_hours: finAfter ? {
              day:   Number(finAfter.hours_day   ?? 0),
              night: Number(finAfter.hours_night ?? 0),
              sat:   Number(finAfter.hours_sat   ?? 0),
              sun:   Number(finAfter.hours_sun   ?? 0),
              bh:    Number(finAfter.hours_bh    ?? 0),
              total: Number(finAfter.total_hours ?? 0)
            } : null
          }
        },
        { entity: 'timesheets', subject_id: currentTimesheetId, req }
      );
    } catch {}
  }

  // NOTE: We do NOT issue/send a QR from daily-manual-upsert anymore.
  // Sending happens via /api/timesheets/:id/qr-resend after save when the FE chooses “Send new now”.

  // ─────────────────────────────────────────────────────────────
  // MINIMAL TIMESHEET AUDIT: QR events (only meaningful ones)
  // ─────────────────────────────────────────────────────────────
  try {
    if (qrAction === 'DISABLE' && rotated) {
      await writeAudit(
        env,
        user,
        'TIMESHEET_QR_DISABLED',
        {
          timesheet_id: currentTimesheetId,
          sheet_scope: 'DAILY',
          rotated: true,
          from_version: preRotateVersion,
          to_version: Number(ts?.version || preRotateVersion),
          reason: rotateReason
        },
        { entity: 'timesheets', subject_id: currentTimesheetId, req }
      );
    }

    if (qrAction === 'REVOKE_TO_MANUAL' && rotated) {
      await writeAudit(
        env,
        user,
        'TIMESHEET_QR_REVOKED_TO_MANUAL',
        {
          timesheet_id: currentTimesheetId,
          sheet_scope: 'DAILY',
          rotated: true,
          from_version: preRotateVersion,
          to_version: Number(ts?.version || preRotateVersion),
          reason: rotateReason
        },
        { entity: 'timesheets', subject_id: currentTimesheetId, req }
      );
    }

    // ✅ FIX: Remove issuance/reissue audit blocks — daily-manual-upsert does not issue QR now.
    // if (qrIssued && qrToken) { ... }
    // if (qrReissued && qrToken) { ... }

  } catch {}

  // ─────────────────────────────────────────────────────────────
  // MINIMAL TIMESHEET AUDIT: PROCESSED (always after rebuild)
  // ─────────────────────────────────────────────────────────────
  if (wasUnprocessedBefore) {
    if (changedFields.length || hoursChangedBuckets || qrAction) {
      await writeDailyManualSaveAudit();
    }
  } else {
    try {
      await writeAudit(
        env,
        user,
        'TIMESHEET_PROCESSED',
        {
          timesheet_id: currentTimesheetId,
          sheet_scope: 'DAILY',
          submission_mode: 'MANUAL',
          source: 'timesheet_daily_manual_upsert',
          qr_action: qrAction,
          processing_status: finAfter?.processing_status ?? null,
          bucket_hours: finAfter ? {
            day:   Number(finAfter.hours_day   ?? 0),
            night: Number(finAfter.hours_night ?? 0),
            sat:   Number(finAfter.hours_sat   ?? 0),
            sun:   Number(finAfter.hours_sun   ?? 0),
            bh:    Number(finAfter.hours_bh    ?? 0),
            total: Number(finAfter.total_hours ?? 0)
          } : null
        },
        { entity: 'timesheets', subject_id: currentTimesheetId, req }
      );
    } catch {}
  }



  const finalTs = await sbGetOne(
    env,
    `${env.SUPABASE_URL}/rest/v1/timesheets` +
      `?timesheet_id=eq.${enc(currentTimesheetId)}` +
      `&is_current=eq.true` +
      `&select=*`
  ).catch(() => updatedTs || ts || null);

  dailyManualSaveSignatureLog('final_loaded', {
    current_timesheet_id: currentTimesheetId || null,
    timesheet: dailyManualSaveDiagTimesheet(finalTs),
    tsfin: dailyManualSaveDiagTsfin(finAfter)
  });

  // The canonical Daily save/recalculation and existing audits have succeeded.
  // Prepare only native Checklist metadata using the real current identity.
  const checklistCycleReadiness = await prepareSubmittedDailyCycleAfterSave({
    rpc: (name, args) => sbRpc(env, name, args, { timeoutMs: 45000 }),
    actorUserId: user.id,
    timesheetId: currentTimesheetId
  });
  if (checklistCycleReadiness.ok !== true) {
    return withCORS(env, req, new Response(JSON.stringify(checklistCycleReadiness), {
      status: 502, headers: { 'Content-Type': 'application/json' }
    }));
  }

  if (bulkPatchResponseRequested) {
    const manualChanged = !!(
      shouldPatchActualSchedule ||
      referenceProvided ||
      rotated ||
      qrAction ||
      changedFields.length ||
      hoursChangedBuckets
    );

    const minimalRowKey = `timesheet:${currentTimesheetId}`;
    const minimalCacheHints = {
      row_keys: [minimalRowKey].filter(Boolean),
      timesheet_ids: [currentTimesheetId].filter(Boolean),
      storage_keys: [],
      datasets: ['bulk_process', 'bulk_authorise'],
      row_signature: null,
      manual_changed: manualChanged,
      status_only: !manualChanged,
      identity_changed: rotated === true,
      invalidate_context: false,
      invalidate_row_context: false,
      invalidate_editor_context: manualChanged,
      invalidate_preview: false,
      invalidate_evidence: false
    };

    if (minimalLifecycleMutation) {
      dailyManualSaveSignatureLog('response_shape', {
        response_variant: 'minimal_lifecycle_mutation',
        status: 200,
        current_timesheet_id: currentTimesheetId || null,
        response_contains: {
          timesheet: false,
          tsfin: false,
          row_signature: false,
          backend_row_signature: false,
          mutation_row_signature: false,
          expected_row_signature: false,
          affected_rows: true,
          affected_rows_count: 1,
          row_patch: false,
          row_patches: false,
          data_row: false,
          cache_invalidation_hints: true,
          refresh_required: true,
          requires_affected_row_refresh: true,
          summary_row_hint: true
        }
      });
      return withCORS(env, req, ok({
        ok: true,
        success: true,
        response_context: 'minimal_lifecycle_mutation',
        minimal_lifecycle_mutation: true,
        timesheet_id: currentTimesheetId,
        booking_id: guard.resolved.booking_id || null,
        requested_timesheet_id: guard.resolved.requested_timesheet_id || timesheetId,
        current_timesheet_id: currentTimesheetId,
        expected_timesheet_id: currentTimesheetId,
        current_version: finalTs?.version ?? ts?.version ?? guard.resolved.current_version ?? null,
        was_stale: !!guard.resolved.was_stale,
        affected_rows: [{
          row_key: minimalRowKey,
          timesheet_id: currentTimesheetId,
          current_timesheet_id: currentTimesheetId,
          expected_timesheet_id: currentTimesheetId,
          booking_id: guard.resolved.booking_id || null,
          contract_week_id: finalTs?.contract_week_id ?? ts?.contract_week_id ?? null,
          current_version: finalTs?.version ?? ts?.version ?? guard.resolved.current_version ?? null,
          processing_status: finAfter?.processing_status ?? null,
          reference_number: finalTs?.reference_number ?? null
        }],
        requires_affected_row_refresh: true,
        refresh_required: true,
        row_key: minimalRowKey,
        new_row_key: minimalRowKey,
        row_signature: null,
        backend_row_signature: null,
        row_backend_signature: null,
        count_deltas: {},
        cache_invalidation_hints: minimalCacheHints,
        cache_invalidation: {
          row_keys: minimalCacheHints.row_keys,
          timesheet_ids: minimalCacheHints.timesheet_ids,
          storage_keys: minimalCacheHints.storage_keys,
          datasets: minimalCacheHints.datasets,
          row_signature: minimalCacheHints.row_signature,
          manual_changed: minimalCacheHints.manual_changed,
          invalidate_context: minimalCacheHints.invalidate_context,
          invalidate_preview: minimalCacheHints.invalidate_preview
        },
        bulk_authorise: bulkDatasetMode === 'authorise',
        bulk_process: bulkDatasetMode === 'process',
        summary_row_hint: {
          timesheet_id: currentTimesheetId,
          booking_id: guard.resolved.booking_id || null,
          processing_status: finAfter?.processing_status ?? null,
          reference_number: finalTs?.reference_number ?? null,
          worked_start_iso: finalTs?.worked_start_iso ?? null,
          worked_end_iso: finalTs?.worked_end_iso ?? null,
          break_start_iso: finalTs?.break_start_iso ?? null,
          break_end_iso: finalTs?.break_end_iso ?? null,
          break_minutes: finalTs?.break_minutes ?? null,
          worked_minutes: finalTs?.worked_minutes ?? null,
          actual_schedule_json: finalTs?.actual_schedule_json ?? null,
          total_hours: finAfter?.total_hours ?? null
        },
        qr_action: qrAction,
        qr_issued: qrIssued,
        qr_reissued: qrReissued,
        qr_pdf_key: pdfKey,
        qr_r2_key,
        qr_token: qrToken || null
      }));
    }

    let bulkRow = null;
    try {
      bulkRow = await loadDailyManualBulkPatchRow({
        timesheetId: currentTimesheetId,
        datasetMode: bulkDatasetMode,
        changedDomains: manualChanged ? ['manual', 'schedule', 'save'] : ['save']
      });
    } catch (e) {
      return withCORS(env, req, serverError(`Failed to build daily manual bulk patch row: ${e?.message || e}`));
    }

    if (!bulkRow || typeof bulkRow !== 'object' || Array.isArray(bulkRow)) {
      return withCORS(env, req, serverError('bulk_timesheet_row_patch_v1 did not return a daily manual row patch.'));
    }

    const rowPatch = (bulkRow.row_patch && typeof bulkRow.row_patch === 'object' && !Array.isArray(bulkRow.row_patch))
      ? bulkRow.row_patch
      : {};
    const rowKey = String(bulkRow.row_key || rowPatch.row_key || `timesheet:${currentTimesheetId}`).trim();
    const rowSignature = String(bulkRow.row_signature || rowPatch.row_signature || '').trim() || null;
    const cacheHints = {
      ...((bulkRow.cache_invalidation_hints && typeof bulkRow.cache_invalidation_hints === 'object' && !Array.isArray(bulkRow.cache_invalidation_hints)) ? bulkRow.cache_invalidation_hints : {}),
      row_keys: [rowKey].filter(Boolean),
      timesheet_ids: [currentTimesheetId].filter(Boolean),
      storage_keys: [],
      datasets: ['bulk_process', 'bulk_authorise'],
      row_signature: rowSignature,
      manual_changed: manualChanged,
      status_only: !manualChanged,
      identity_changed: rotated === true,
      invalidate_context: false,
      invalidate_row_context: false,
      invalidate_editor_context: manualChanged,
      invalidate_preview: false,
      invalidate_evidence: false
    };

    dailyManualSaveSignatureLog('response_shape', {
      response_variant: 'bulk_patch',
      status: 200,
      current_timesheet_id: currentTimesheetId || null,
      row_signature: rowSignature || null,
      backend_row_signature: rowSignature || null,
      timesheet: dailyManualSaveDiagTimesheet(finalTs),
      tsfin: dailyManualSaveDiagTsfin(finAfter),
      response_contains: dailyManualSaveDiagResponseContains({
        timesheet: finalTs || null,
        tsfin: finAfter || null,
        row_patch: rowPatch,
        row_patches: [rowPatch],
        data_row: bulkRow,
        row_signature: rowSignature,
        backend_row_signature: rowSignature,
        affected_rows: [{ row_key: rowKey }],
        cache_invalidation_hints: cacheHints,
        refresh_required: true,
        requires_affected_row_refresh: true,
        summary_row_hint: {}
      })
    });

    return withCORS(env, req, ok({
      timesheet_id: currentTimesheetId,
      booking_id: guard.resolved.booking_id || null,
      requested_timesheet_id: guard.resolved.requested_timesheet_id || timesheetId,
      current_timesheet_id: currentTimesheetId,
      expected_timesheet_id: currentTimesheetId,
      current_version: finalTs?.version ?? ts?.version ?? guard.resolved.current_version ?? null,
      was_stale: !!guard.resolved.was_stale,

      worked_start_iso: finalTs?.worked_start_iso ?? null,
      worked_end_iso: finalTs?.worked_end_iso ?? null,
      break_start_iso: finalTs?.break_start_iso ?? null,
      break_end_iso: finalTs?.break_end_iso ?? null,
      break_minutes: finalTs?.break_minutes ?? null,
      worked_minutes: finalTs?.worked_minutes ?? null,
      actual_schedule_json: finalTs?.actual_schedule_json ?? null,
      reference_number: finalTs?.reference_number ?? null,
      reference_set_at: finalTs?.reference_set_at ?? null,
      processing_status: finAfter?.processing_status ?? null,

      timesheet: finalTs || null,
      tsfin: finAfter || null,
      row_patch: rowPatch,
      row_patches: [rowPatch],
      data_row: bulkRow,
      row: bulkRow,
      row_key: rowKey,
      new_row_key: rowKey,
      row_signature: rowSignature,
      backend_row_signature: rowSignature,
      row_backend_signature: rowSignature,
      count_deltas: (bulkRow.count_deltas && typeof bulkRow.count_deltas === 'object' && !Array.isArray(bulkRow.count_deltas)) ? bulkRow.count_deltas : {},
      cache_invalidation_hints: cacheHints,
      cache_invalidation: {
        row_keys: cacheHints.row_keys,
        timesheet_ids: cacheHints.timesheet_ids,
        storage_keys: cacheHints.storage_keys,
        datasets: cacheHints.datasets,
        row_signature: cacheHints.row_signature,
        manual_changed: cacheHints.manual_changed,
        invalidate_context: cacheHints.invalidate_context,
        invalidate_preview: cacheHints.invalidate_preview
      },
      response_context: bulkResponseContext,
      bulk_authorise: bulkDatasetMode === 'authorise',
      bulk_process: bulkDatasetMode === 'process',
      summary_row_hint: {
        timesheet_id: currentTimesheetId,
        booking_id: guard.resolved.booking_id || null,
        processing_status: finAfter?.processing_status ?? null,
        reference_number: finalTs?.reference_number ?? null,
        worked_start_iso: finalTs?.worked_start_iso ?? null,
        worked_end_iso: finalTs?.worked_end_iso ?? null,
        break_start_iso: finalTs?.break_start_iso ?? null,
        break_end_iso: finalTs?.break_end_iso ?? null,
        break_minutes: finalTs?.break_minutes ?? null,
        worked_minutes: finalTs?.worked_minutes ?? null,
        actual_schedule_json: finalTs?.actual_schedule_json ?? null,
        total_hours: finAfter?.total_hours ?? null
      },

      qr_action: qrAction,
      qr_issued: qrIssued,
      qr_reissued: qrReissued,
      qr_pdf_key: pdfKey,
      qr_r2_key,
      qr_token: qrToken || null
    }));
  }


  dailyManualSaveSignatureLog('response_shape', {
    response_variant: 'standard',
    status: 200,
    current_timesheet_id: currentTimesheetId || null,
    timesheet: dailyManualSaveDiagTimesheet(finalTs),
    tsfin: dailyManualSaveDiagTsfin(finAfter),
    response_contains: dailyManualSaveDiagResponseContains({
      timesheet: finalTs || null,
      tsfin: finAfter || null,
      summary_row_hint: {},
      refresh_required: false,
      requires_affected_row_refresh: false
    })
  });

  return withCORS(env, req, ok({
    timesheet_id: currentTimesheetId,
    booking_id: guard.resolved.booking_id || null,
    requested_timesheet_id: guard.resolved.requested_timesheet_id || timesheetId,
    current_timesheet_id: currentTimesheetId,
    current_version: finalTs?.version ?? ts?.version ?? guard.resolved.current_version ?? null,
    was_stale: !!guard.resolved.was_stale,

    worked_start_iso: finalTs?.worked_start_iso ?? null,
    worked_end_iso: finalTs?.worked_end_iso ?? null,
    break_start_iso: finalTs?.break_start_iso ?? null,
    break_end_iso: finalTs?.break_end_iso ?? null,
    break_minutes: finalTs?.break_minutes ?? null,
    worked_minutes: finalTs?.worked_minutes ?? null,
    actual_schedule_json: finalTs?.actual_schedule_json ?? null,
    reference_number: finalTs?.reference_number ?? null,
    reference_set_at: finalTs?.reference_set_at ?? null,
    processing_status: finAfter?.processing_status ?? null,

    timesheet: finalTs || null,
    tsfin: finAfter || null,
    summary_row_hint: {
      timesheet_id: currentTimesheetId,
      booking_id: guard.resolved.booking_id || null,
      processing_status: finAfter?.processing_status ?? null,
      reference_number: finalTs?.reference_number ?? null,
      worked_start_iso: finalTs?.worked_start_iso ?? null,
      worked_end_iso: finalTs?.worked_end_iso ?? null,
      break_start_iso: finalTs?.break_start_iso ?? null,
      break_end_iso: finalTs?.break_end_iso ?? null,
      break_minutes: finalTs?.break_minutes ?? null,
      worked_minutes: finalTs?.worked_minutes ?? null,
      actual_schedule_json: finalTs?.actual_schedule_json ?? null,
      total_hours: finAfter?.total_hours ?? null
    },

    qr_action: qrAction,
    qr_issued: qrIssued,
    qr_reissued: qrReissued,
    qr_pdf_key: pdfKey,
    qr_r2_key,
    qr_token: qrToken || null
  }));
}
