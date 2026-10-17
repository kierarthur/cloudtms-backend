async function handleTimesheetAuthoriseGeneric(env, req, timesheetId, ctx = null, internalOptions = null) {
  const enc = encodeURIComponent;
  const user = await requireUser(env, req, []);
  if (!user || (user.role !== 'admin' && !isActivePaymentActor(user))) return withCORS(env, req, unauthorized());

  const jsonHeaders = (typeof JSON_HEADERS !== 'undefined') ? JSON_HEADERS : { 'Content-Type': 'application/json' };
  const trimStr = (value) => String(value == null ? '' : value).trim();
  const firstString = function firstString() {
    for (let index = 0; index < arguments.length; index += 1) {
      const value = trimStr(arguments[index]);
      if (value) return value;
    }
    return '';
  };
  const parseMaybeJsonObj = (value) => {
    if (value == null) return null;
    if (typeof value === 'object' && !Array.isArray(value)) return value;
    const text = trimStr(value);
    if (!text) return null;
    try {
      const parsed = JSON.parse(text);
      return (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) ? parsed : null;
    } catch {
      return null;
    }
  };
  const unwrapRpcPayload = (payload, fnName) => {
    let out = payload;
    if (Array.isArray(out) && out.length === 1) out = out[0];
    if (out && typeof out === 'object' && !Array.isArray(out) && Object.prototype.hasOwnProperty.call(out, fnName)) {
      out = out[fnName];
    }
    if (typeof out === 'string') {
      const parsed = parseMaybeJsonObj(out);
      if (parsed) out = parsed;
    }
    return (out && typeof out === 'object' && !Array.isArray(out)) ? out : null;
  };
  const parseRpcFailure = (err) => {
    let payload = null;
    if (err && err.json && typeof err.json === 'object') {
      payload = err.json;
    } else if (typeof err?.body === 'string' && err.body) {
      try {
        payload = JSON.parse(err.body);
      } catch {
        payload = null;
      }
    }
    const details = payload?.details ?? payload?.detail ?? err?.details ?? err?.detail ?? null;
    const detailJson = parseMaybeJsonObj(details);
    const message = firstString(payload?.message, payload?.error, err?.message, 'AUTHORISE_FAILED');
    return {
      status: Number(err?.status || payload?.status || 0) || 0,
      message,
      code: firstString(payload?.error_code, payload?.code, detailJson?.error_code, message),
      details,
      detailJson,
      payload
    };
  };
  const withJson = (status, payload) => withCORS(env, req, new Response(JSON.stringify(payload || {}), { status, headers: jsonHeaders }));
  const boolish = (value) => {
    if (value === true) return true;
    if (value === false) return false;
    if (value == null) return false;
    const text = String(value).trim().toLowerCase();
    return text === 'true' || text === '1' || text === 'yes' || text === 'y' || text === 'on';
  };
  const affectedRowsFor = (payload, fallbackTimesheetId) => {
    if (Array.isArray(payload?.affected_rows) && payload.affected_rows.length) return payload.affected_rows;
    const cleanTimesheetId = firstString(payload?.current_timesheet_id, payload?.timesheet_id, fallbackTimesheetId);
    const cleanContractWeekId = firstString(payload?.contract_week_id);
    return [{
      timesheet_id: cleanTimesheetId || null,
      contract_week_id: cleanContractWeekId || null,
      booking_id: payload?.booking_id || null,
      row_key: cleanTimesheetId ? `timesheet:${cleanTimesheetId}` : null,
      context: 'timesheet_modal'
    }];
  };
  const lifecycleSignatureDiagText = (value) => {
    const text = String(value == null ? '' : value).trim();
    return text || null;
  };
  const lifecycleSignatureDiagObjectKeys = (obj) => {
    if (!obj || typeof obj !== 'object' || Array.isArray(obj)) return [];
    return Object.keys(obj).sort();
  };
  const lifecycleSignatureDiagSignature = (obj) => {
    if (!obj || typeof obj !== 'object' || Array.isArray(obj)) {
      const signatureText = lifecycleSignatureDiagText(obj);
      return {
        present: !!signatureText,
        backend_row_signature: null,
        mutation_row_signature: null,
        row_signature: null,
        expected_row_signature: null,
        current_row_signature: null,
        signature: signatureText
      };
    }

    const backend = lifecycleSignatureDiagText(obj.backend_row_signature || obj.backendRowSignature);
    const mutation = lifecycleSignatureDiagText(obj.mutation_row_signature || obj.mutationRowSignature);
    const row = lifecycleSignatureDiagText(obj.row_signature || obj.rowSignature);
    const expected = lifecycleSignatureDiagText(obj.expected_row_signature || obj.expectedRowSignature);
    const current = lifecycleSignatureDiagText(obj.current_row_signature || obj.currentRowSignature);
    const generic = lifecycleSignatureDiagText(obj.signature || obj.render_signature || obj.renderSignature);

    return {
      present: !!(backend || mutation || row || expected || current || generic),
      backend_row_signature: backend,
      mutation_row_signature: mutation,
      row_signature: row,
      expected_row_signature: expected,
      current_row_signature: current,
      signature: backend || mutation || row || expected || current || generic || null
    };
  };
  const lifecycleSignatureDiagResponseContains = (payload) => ({
    ok: Object.prototype.hasOwnProperty.call(payload || {}, 'ok'),
    success: Object.prototype.hasOwnProperty.call(payload || {}, 'success'),
    authorised: Object.prototype.hasOwnProperty.call(payload || {}, 'authorised'),
    unauthorised: Object.prototype.hasOwnProperty.call(payload || {}, 'unauthorised'),
    backend_row_signature: !!lifecycleSignatureDiagText(payload?.backend_row_signature),
    mutation_row_signature: !!lifecycleSignatureDiagText(payload?.mutation_row_signature),
    row_signature: !!lifecycleSignatureDiagText(payload?.row_signature),
    expected_row_signature: !!lifecycleSignatureDiagText(payload?.expected_row_signature),
    current_row_signature: !!lifecycleSignatureDiagText(payload?.current_row_signature),
    decision: !!(payload?.decision && typeof payload.decision === 'object'),
    lifecycle_patch: !!(payload?.lifecycle_patch && typeof payload.lifecycle_patch === 'object'),
    affected_rows: Array.isArray(payload?.affected_rows),
    affected_rows_count: Array.isArray(payload?.affected_rows) ? payload.affected_rows.length : null,
    refresh_hints: !!(payload?.refresh_hints && typeof payload.refresh_hints === 'object'),
    cache_invalidation_hints: !!(payload?.cache_invalidation_hints && typeof payload.cache_invalidation_hints === 'object'),
    requires_affected_row_refresh: payload?.requires_affected_row_refresh === true,
    requires_full_details_refresh: payload?.requires_full_details_refresh === true,
    refresh_required: payload?.refresh_required === true,
    permission_state_patch_complete: payload?.permission_state_patch_complete === true,
    priority_badges_patch_complete: payload?.priority_badges_patch_complete === true,
    immediate_lifecycle_patch_available: payload?.immediate_lifecycle_patch_available === true
  });
  const lifecycleSignatureLog = (step, extra = {}) => {
    try {
      let routePath = null;
      try {
        routePath = new URL(req?.url || '').pathname;
      } catch {}
      console.log('[TIMESHEET_LIFECYCLE_SIGNATURE_DIAG]', JSON.stringify({
        tag: 'TIMESHEET_LIFECYCLE_SIGNATURE_DIAG',
        handler: 'handleTimesheetAuthoriseGeneric',
        route_family: 'TIMESHEET_AUTHORISE',
        lifecycle_action: 'AUTHORISE',
        step,
        route_path: routePath,
        requested_timesheet_id: trimStr(timesheetId) || null,
        actor_user_id: user?.id || null,
        server_utc: new Date().toISOString(),
        ...(extra && typeof extra === 'object' && !Array.isArray(extra) ? extra : {})
      }));
    } catch {}
  };

  const isUnknownLifecycleOutcomeError = (err, parsed = null) => {
    const status = Number(parsed?.status || err?.status || 0) || 0;
    const name = firstString(err?.name, parsed?.payload?.name);
    const payload = (parsed?.payload && typeof parsed.payload === 'object' && !Array.isArray(parsed.payload)) ? parsed.payload : {};
    const detailJson = (parsed?.detailJson && typeof parsed.detailJson === 'object' && !Array.isArray(parsed.detailJson)) ? parsed.detailJson : {};
    const failureClassification = (payload.failure_classification && typeof payload.failure_classification === 'object' && !Array.isArray(payload.failure_classification))
      ? payload.failure_classification
      : ((detailJson.failure_classification && typeof detailJson.failure_classification === 'object' && !Array.isArray(detailJson.failure_classification)) ? detailJson.failure_classification : {});
    const classifierBool = (value) => value === true || ['true', 't', '1', 'yes', 'y', 'on'].includes(String(value == null ? '' : value).trim().toLowerCase());
    const transientCode = firstString(parsed?.code, payload.error_code, payload.code, detailJson.error_code).toUpperCase();
    const classificationRetryDecision = firstString(failureClassification.retry_decision_reason).toUpperCase();
    const classificationHttpStatus = Number(failureClassification.upstream_http_status || 0) || 0;
    const controlledTransientUnknownOutcome = transientCode === 'TRANSIENT_LIFECYCLE_CONFLICT' && (
      classifierBool(failureClassification.is_local_timeout) ||
      classifierBool(failureClassification.is_upstream_408) ||
      classifierBool(failureClassification.is_postgrest_timeout) ||
      classifierBool(failureClassification.is_fetch_abort) ||
      classifierBool(failureClassification.is_network_error) ||
      [408, 504, 524].includes(classificationHttpStatus) ||
      (classifierBool(failureClassification.is_retryable) && ['RETRY_BUDGET_EXHAUSTED', 'MAX_ATTEMPTS_REACHED', 'RETRYABLE_TRANSIENT_OR_TIMEOUT'].includes(classificationRetryDecision))
    );
    const message = [
      parsed?.message,
      parsed?.code,
      parsed?.details,
      err?.message,
      err?.body,
      err?.name
    ].map((value) => String(value == null ? '' : value)).join(' ').toLowerCase();
    return controlledTransientUnknownOutcome || [408, 504, 524].includes(status) || String(name || '').toLowerCase() === 'aborterror' || /timeout after|rpc timesheet_authorise_generic_atomic failed 408|rpc timesheet_authorise_generic_atomic timed out|fetch timeout|network timeout|gateway timeout|worker timeout|aborted|request timed out|request timeout/.test(message);
  };
  const lifecycleOutcomeMessage = (code) => {
    const normalized = firstString(code).toUpperCase();
    if (normalized === 'STALE_LIFECYCLE_STATE') {
      return 'This timesheet changed while the update was being processed. The latest timesheet details are now shown.';
    }
    if (normalized === 'UNKNOWN_LIFECYCLE_OUTCOME') {
      return 'We could not confirm whether this timesheet update completed. The latest timesheet details are now shown. Close this message to continue.';
    }
    return 'This timesheet could not be authorised. The latest timesheet details are now shown. Close this message to continue.';
  };
  const lifecycleOutcomeResponse = (reconciliation, parsed = {}, options = {}) => {
    const rec = (reconciliation && typeof reconciliation === 'object') ? reconciliation : {};
    let code = firstString(options.code, rec.recommended_response_code).toUpperCase();
    if (rec.stale_lifecycle_state === true || code === 'ROW_SIGNATURE_MISMATCH') code = 'STALE_LIFECYCLE_STATE';
    if (!code || code === 'LIFECYCLE_TIMEOUT_SAFE_RETRY_ALLOWED') code = options.retryExhausted ? 'LIFECYCLE_TIMEOUT_RETRY_EXHAUSTED' : 'UNKNOWN_LIFECYCLE_OUTCOME';
    if (code === 'LIFECYCLE_TIMEOUT_RECONCILED_SUCCESS') code = 'UNKNOWN_LIFECYCLE_OUTCOME';
    const finalTimesheetId = firstString(rec.current_timesheet_id, rec.timesheet_id, currentTimesheetId, expectedTimesheetId, requestedTimesheetId);
    const sig = firstString(rec.backend_row_signature, rec.row_signature, rec.current_row_signature);
    const message = lifecycleOutcomeMessage(code);
    const patchSource = firstString(rec.patch_source).toUpperCase();
    const unsafeTargetPatch = rec.target_state_reached !== true && patchSource === 'TARGET_STATE';
    const safeLifecyclePatch = unsafeTargetPatch ? null : ((rec.lifecycle_patch && typeof rec.lifecycle_patch === 'object') ? rec.lifecycle_patch : null);
    const safeAffectedRows = unsafeTargetPatch ? affectedRowsFor(rec, finalTimesheetId) : (Array.isArray(rec.affected_rows) && rec.affected_rows.length ? rec.affected_rows : affectedRowsFor(rec, finalTimesheetId));
    return withJson(409, {
      ok: false,
      success: false,
      error: code,
      error_code: code,
      message,
      user_message: message,
      refresh_required: true,
      stale_lifecycle_state: code === 'STALE_LIFECYCLE_STATE',
      unknown_lifecycle_outcome: code === 'UNKNOWN_LIFECYCLE_OUTCOME' || code === 'LIFECYCLE_TIMEOUT_RETRY_EXHAUSTED',
      retry_exhausted: code === 'LIFECYCLE_TIMEOUT_RETRY_EXHAUSTED',
      reconciled: rec.reconciled === true,
      target_state_reached: false,
      current_timesheet_id: finalTimesheetId || null,
      timesheet_id: finalTimesheetId || null,
      requested_timesheet_id: requestedTimesheetId || null,
      expected_timesheet_id: expectedTimesheetId || null,
      contract_week_id: firstString(rec.contract_week_id) || null,
      booking_id: firstString(rec.booking_id, guard.resolved?.booking_id) || null,
      candidate_id: firstString(rec.candidate_id) || null,
      current_version: rec.current_version ?? guard.resolved?.current_version ?? null,
      processing_status: firstString(rec.processing_status) || null,
      previous_processing_status: firstString(rec.previous_processing_status) || null,
      authorised_at_server: rec.authorised_at_server ?? null,
      authorised_at_utc: rec.authorised_at_utc ?? null,
      revoked_at: rec.revoked_at ?? null,
      backend_row_signature: sig || null,
      mutation_row_signature: sig || null,
      row_signature: sig || null,
      expected_row_signature: sig || expectedRowSignature || null,
      lifecycle_patch: safeLifecyclePatch,
      affected_rows: safeAffectedRows,
      refresh_hints: (rec.refresh_hints && typeof rec.refresh_hints === 'object') ? rec.refresh_hints : {},
      cache_invalidation_hints: (rec.cache_invalidation_hints && typeof rec.cache_invalidation_hints === 'object') ? rec.cache_invalidation_hints : {},
      permission_state_patch_complete: rec.permission_state_patch_complete === true,
      priority_badges_patch_complete: rec.priority_badges_patch_complete === true,
      immediate_lifecycle_patch_available: rec.immediate_lifecycle_patch_available === true,
      requires_affected_row_refresh: rec.requires_affected_row_refresh !== false,
      requires_full_details_refresh: rec.requires_full_details_refresh !== false,
      reconciliation_target_state_reached: rec.reconciliation_target_state_reached === true,
      reconciliation_current_processing_status: firstString(rec.reconciliation_current_processing_status, rec.processing_status) || null,
      reconciliation_current_authorised_at_server_present: rec.reconciliation_current_authorised_at_server_present === true,
      reconciliation_current_revoked_at_present: rec.reconciliation_current_revoked_at_present === true,
      patch_source: firstString(rec.patch_source) || (safeLifecyclePatch ? 'CURRENT_STATE' : 'OMITTED'),
      target_patch_suppressed: rec.target_patch_suppressed === true || unsafeTargetPatch,
      suppression_reason: firstString(rec.suppression_reason) || (unsafeTargetPatch ? 'TARGET_STATE_NOT_REACHED' : null),
      affected_rows_source: firstString(rec.affected_rows_source) || (safeAffectedRows.length ? 'CURRENT_STATE' : 'OMITTED'),
      source: 'lifecycle_timeout_reconciliation'
    });
  };
  const rpcPayloadFromReconciliation = (reconciliation, operation = 'timesheet_authorise') => {
    const rec = (reconciliation && typeof reconciliation === 'object') ? reconciliation : {};
    const finalTimesheetId = firstString(rec.current_timesheet_id, rec.timesheet_id, currentTimesheetId, expectedTimesheetId, requestedTimesheetId);
    const sig = firstString(rec.backend_row_signature, rec.row_signature, rec.current_row_signature);
    return {
      ok: true,
      success: true,
      authorised: true,
      operation,
      requested_timesheet_id: requestedTimesheetId,
      current_timesheet_id: finalTimesheetId || null,
      timesheet_id: finalTimesheetId || null,
      contract_week_id: firstString(rec.contract_week_id) || null,
      booking_id: firstString(rec.booking_id, guard.resolved?.booking_id) || null,
      candidate_id: firstString(rec.candidate_id) || null,
      current_version: rec.current_version ?? guard.resolved?.current_version ?? null,
      processing_status: firstString(rec.processing_status) || null,
      new_processing_status: firstString(rec.processing_status) || null,
      previous_processing_status: firstString(rec.previous_processing_status) || null,
      authorised_at_server: rec.authorised_at_server || now,
      authorised_at_utc: rec.authorised_at_utc || rec.authorised_at_server || now,
      revoked_at: rec.revoked_at ?? null,
      ready_to_pay: rec.ready_to_pay,
      backend_row_signature: sig || null,
      row_signature: sig || null,
      affected_rows: Array.isArray(rec.affected_rows) && rec.affected_rows.length ? rec.affected_rows : affectedRowsFor(rec, finalTimesheetId),
      cache_invalidation_hints: (rec.cache_invalidation_hints && typeof rec.cache_invalidation_hints === 'object') ? rec.cache_invalidation_hints : {},
      was_stale: !!(requestedTimesheetId && finalTimesheetId && requestedTimesheetId !== finalTimesheetId),
      reconciled_after_unknown_outcome: true,
      recommended_response_code: 'LIFECYCLE_TIMEOUT_RECONCILED_SUCCESS'
    };
  };

  const mutationErrorResponse = (rawCode, detailObj, options = {}) => {
    const code = firstString(detailObj?.error_code, detailObj?.error, detailObj?.code, rawCode, 'AUTHORISE_FAILED').toUpperCase();
    const currentTimesheetId = firstString(detailObj?.current_timesheet_id, detailObj?.timesheet_id, options.currentTimesheetId);
    const reason = firstString(detailObj?.reason).toUpperCase();
    const missingRate = bpayNextAuthoriseKnownRefusal(rawCode, detailObj, options.message);
    if (missingRate) return withJson(409, { ...missingRate,
      current_timesheet_id: currentTimesheetId || null,
      affected_rows: affectedRowsFor(detailObj || {}, currentTimesheetId) });
    const sourceRefusal = weeklySourceAuthoriseKnownRefusal(rawCode, detailObj);
    if (sourceRefusal) return withJson(sourceRefusal.status, {
      error: sourceRefusal.error_code, error_code: sourceRefusal.error_code,
      retryable: sourceRefusal.retryable, message: sourceRefusal.message,
      detail: detailObj || null, current_timesheet_id: currentTimesheetId || null,
      refresh_required: true,
      affected_rows: affectedRowsFor(detailObj || {}, currentTimesheetId)
    });
    if (code === 'TIMESHEET_WORK_INTERVAL_OVERLAP') {
      return withJson(409, {
        error: 'TIMESHEET_WORK_INTERVAL_OVERLAP',
        error_code: 'TIMESHEET_WORK_INTERVAL_OVERLAP',
        message: 'You have already submitted a timesheet containing these hours. This timesheet cannot be accepted.',
        current_timesheet_id: currentTimesheetId || null,
        refresh_required: false,
        affected_rows: affectedRowsFor(detailObj || {}, currentTimesheetId)
      });
    }
    if (code === 'DUPLICATE_EXPENSE_REVIEW_REQUIRED' || code === 'DUPLICATE_EXPENSE_REVIEW_CONFIRMATION_REQUIRED') {
      return withJson(409, {
        error: 'DUPLICATE_EXPENSE_REVIEW_REQUIRED',
        error_code: 'DUPLICATE_EXPENSE_REVIEW_REQUIRED',
        message: 'An expense category was already submitted for this Candidate, Client and week ending. Review the duplicate warning before authorising this claim.',
        categories: Array.isArray(detailObj?.categories) ? detailObj.categories : [],
        duplicate_expense_confirmation_required: true,
        current_timesheet_id: currentTimesheetId || null,
        refresh_required: false,
        affected_rows: affectedRowsFor(detailObj || {}, currentTimesheetId)
      });
    }
    if (code === 'TRANSIENT_LIFECYCLE_CONFLICT') {
      return withJson(409, {
        error: 'TRANSIENT_LIFECYCLE_CONFLICT',
        error_code: 'TRANSIENT_LIFECYCLE_CONFLICT',
        message: 'This timesheet could not be authorised because it was being updated at the same time. The latest timesheet details are now shown. Close this message to continue.',
        transient: true,
        retry_exhausted: true,
        current_timesheet_id: currentTimesheetId || null,
        expected_timesheet_id: firstString(detailObj?.expected_timesheet_id, options.expectedTimesheetId) || null,
        refresh_required: true,
        affected_rows: affectedRowsFor(detailObj || {}, currentTimesheetId)
      });
    }
    if ((code === 'TARGET_NOT_FOUND' || code === 'TIMESHEET_NOT_FOUND') && reason === 'NO_TSFIN') {
      return withJson(400, {
        error: 'NO_TSFIN',
        error_code: 'NO_TSFIN',
        message: 'Cannot authorise: no financial snapshot exists for this timesheet',
        current_timesheet_id: currentTimesheetId || null,
        refresh_required: true,
        affected_rows: affectedRowsFor(detailObj || {}, currentTimesheetId)
      });
    }
    if ((code === 'TARGET_NOT_FOUND' || code === 'TIMESHEET_NOT_FOUND') && reason === 'CONTRACT_WEEK_NOT_FOUND_FOR_WEEKLY_TIMESHEET') {
      return withJson(409, {
        error: 'CONTRACT_WEEK_NOT_FOUND_FOR_WEEKLY_TIMESHEET',
        error_code: 'CONTRACT_WEEK_NOT_FOUND_FOR_WEEKLY_TIMESHEET',
        message: 'Cannot authorise: the linked contract week could not be resolved. Refresh the timesheet and try again.',
        current_timesheet_id: currentTimesheetId || null,
        contract_week_id: firstString(detailObj?.contract_week_id) || null,
        refresh_required: true,
        affected_rows: affectedRowsFor(detailObj || {}, currentTimesheetId)
      });
    }
    if (code === 'TARGET_NOT_FOUND' || code === 'TIMESHEET_NOT_FOUND') {
      return withCORS(env, req, notFound('Timesheet not found'));
    }
    if (code === 'INVALID_PAYLOAD') {
      return withJson(400, {
        error: 'INVALID_PAYLOAD',
        error_code: 'INVALID_PAYLOAD',
        message: 'Invalid authorise request',
        detail: detailObj || null,
        current_timesheet_id: currentTimesheetId || null
      });
    }
    if (code === 'EXPECTED_TIMESHEET_MISMATCH' || code === 'TIMESHEET_MOVED') {
      return withJson(409, {
        error: 'TIMESHEET_MOVED',
        error_code: 'TIMESHEET_MOVED',
        current_timesheet_id: currentTimesheetId || null,
        expected_timesheet_id: firstString(detailObj?.expected_timesheet_id, options.expectedTimesheetId) || null,
        refresh_required: true,
        affected_rows: affectedRowsFor(detailObj || {}, currentTimesheetId)
      });
    }
    if (code === 'ROW_SIGNATURE_MISMATCH') {
      lifecycleSignatureLog('row_signature_mismatch', {
        source: 'mutation_error_response',
        raw_code: code,
        expected_row_signature: firstString(detailObj?.expected_row_signature, options.expectedRowSignature) || null,
        current_row_signature: firstString(detailObj?.current_row_signature) || null,
        current_timesheet_id: currentTimesheetId || null,
        contract_week_id: firstString(detailObj?.contract_week_id) || null
      });
      return withJson(409, {
        error: 'STALE_LIFECYCLE_STATE',
        error_code: 'STALE_LIFECYCLE_STATE',
        message: 'This timesheet changed while the update was being processed. The latest timesheet details are now shown.',
        expected_row_signature: firstString(detailObj?.expected_row_signature, options.expectedRowSignature) || null,
        current_row_signature: firstString(detailObj?.current_row_signature) || null,
        current_timesheet_id: currentTimesheetId || null,
        contract_week_id: firstString(detailObj?.contract_week_id) || null,
        refresh_required: true,
        affected_rows: affectedRowsFor(detailObj || {}, currentTimesheetId)
      });
    }
    if (code === 'AWAITING_SIGNED_QR') {
      return withJson(409, {
        error: 'AWAITING_SIGNED_QR',
        error_code: 'AWAITING_SIGNED_QR',
        message: 'Cannot authorise: awaiting signed QR timesheet',
        current_timesheet_id: currentTimesheetId || null,
        refresh_required: true,
        affected_rows: affectedRowsFor(detailObj || {}, currentTimesheetId)
      });
    }
    if (code === 'TIMESHEET_LOCKED_OR_PAID' || code === 'TIMESHEET_LOCKED_BY_INVOICE' || code === 'ALREADY_AUTHORISED' || code === 'AUTHORISE_NOT_ALLOWED' || code === 'CONTRACT_WEEK_NOT_AUTHORISABLE' || code === 'LOCK_TIMEOUT') {
      return withJson(409, {
        error: code,
        error_code: code,
        message: code === 'LOCK_TIMEOUT' ? 'This timesheet is currently being updated. The latest timesheet details are now required before trying again.' : 'Timesheet could not be authorised in its current state.',
        detail: detailObj || null,
        current_timesheet_id: currentTimesheetId || null,
        contract_week_id: firstString(detailObj?.contract_week_id) || null,
        refresh_required: true,
        affected_rows: affectedRowsFor(detailObj || {}, currentTimesheetId)
      });
    }
    return withJson(409, {
      error: 'UNKNOWN_LIFECYCLE_OUTCOME',
      error_code: 'UNKNOWN_LIFECYCLE_OUTCOME',
      message: 'This timesheet could not be authorised. The latest timesheet details are now shown. Close this message to continue.',
      current_timesheet_id: currentTimesheetId || null,
      refresh_required: true,
      affected_rows: affectedRowsFor(detailObj || {}, currentTimesheetId)
    });
  };

  let body = {};
  try {
    body = await parseJSONBody(req);
  } catch {
    return withCORS(env, req, badRequest('Invalid JSON'));
  }
  if (!body || typeof body !== 'object' || Array.isArray(body)) body = {};
  const duplicateExpenseConfirmed = body.duplicate_expense_confirmation === true
    || body.duplicateExpenseConfirmation === true;
  const authoriseRpcFunctionName = duplicateExpenseConfirmed
    ? 'timesheet_authorise_reviewed_atomic'
    : 'timesheet_authorise_generic_atomic';

  const requestedTimesheetId = trimStr(timesheetId);
  if (!requestedTimesheetId) return withCORS(env, req, badRequest('timesheet_id is required'));

  const expectedTimesheetId = firstString(
    body.expected_timesheet_id,
    body.expectedTimesheetId,
    body.current_timesheet_id,
    body.currentTimesheetId,
    requestedTimesheetId
  );
  if (!expectedTimesheetId) return withCORS(env, req, badRequest('expected_timesheet_id is required'));

  const expectedRowSignature = firstString(
    body.expected_row_signature,
    body.expectedRowSignature,
    body.backend_row_signature,
    body.backendRowSignature,
    body.mutation_row_signature,
    body.mutationRowSignature,
    body.row_signature,
    body.rowSignature
  );

  lifecycleSignatureLog('request_received', {
    expected_timesheet_id: expectedTimesheetId || null,
    expected_row_signature_present: !!expectedRowSignature,
    request_signature: lifecycleSignatureDiagSignature({
      expected_row_signature: firstString(body.expected_row_signature, body.expectedRowSignature) || null,
      backend_row_signature: firstString(body.backend_row_signature, body.backendRowSignature) || null,
      mutation_row_signature: firstString(body.mutation_row_signature, body.mutationRowSignature) || null,
      row_signature: firstString(body.row_signature, body.rowSignature) || null
    }),
    request_signature_fields: {
      expected_row_signature: Object.prototype.hasOwnProperty.call(body, 'expected_row_signature'),
      expectedRowSignature: Object.prototype.hasOwnProperty.call(body, 'expectedRowSignature'),
      backend_row_signature: Object.prototype.hasOwnProperty.call(body, 'backend_row_signature'),
      backendRowSignature: Object.prototype.hasOwnProperty.call(body, 'backendRowSignature'),
      mutation_row_signature: Object.prototype.hasOwnProperty.call(body, 'mutation_row_signature'),
      mutationRowSignature: Object.prototype.hasOwnProperty.call(body, 'mutationRowSignature'),
      row_signature: Object.prototype.hasOwnProperty.call(body, 'row_signature'),
      rowSignature: Object.prototype.hasOwnProperty.call(body, 'rowSignature')
    }
  });

  const guard = await guardCurrentTimesheetWrite(env, req, requestedTimesheetId, expectedTimesheetId);
  if (!guard.ok) {
    lifecycleSignatureLog('guard_failed', {
      expected_timesheet_id: expectedTimesheetId || null,
      expected_row_signature: expectedRowSignature || null
    });
    return guard.res;
  }

  const currentTimesheetId = firstString(guard.resolved?.current_timesheet_id, expectedTimesheetId);
  const sourcePayPermission = await weeklySourceScopedLifecycleActor(env, req, currentTimesheetId, user);
  if (!sourcePayPermission.ok) return sourcePayPermission.response;
  try {
    const pairFlow = await runCorrectionPairLifecycleIfApplicable(
      env, 'AUTHORISE', currentTimesheetId, expectedTimesheetId, expectedRowSignature, body, user, req
    );
    if (pairFlow?.is_pair) {
      if (pairFlow.preview_only) return withJson(200, { ok: true, pair_lifecycle: pairFlow.preview });
      if (pairFlow.confirmation_required) return withJson(409, {
        ok: false, error: 'CORRECTION_PAIR_CONFIRMATION_REQUIRED',
        error_code: 'CORRECTION_PAIR_CONFIRMATION_REQUIRED',
        message: 'This is a linked correction pair. Both timesheets will be authorised together.',
        pair_lifecycle: pairFlow.preview
      });
      if (pairFlow.blocked || pairFlow.mutation?.ok === false || pairFlow.mutation?.all_success === false) {
        return withJson(409, {
          ok: false, error: pairFlow.mutation?.error_code || 'CORRECTION_UNIT_LIFECYCLE_TRANSITION_BLOCKED',
          error_code: pairFlow.mutation?.error_code || 'CORRECTION_UNIT_LIFECYCLE_TRANSITION_BLOCKED',
          message: 'The linked correction pair could not be authorised safely. Refresh and try again.',
          pair_lifecycle: pairFlow.preview, mutation: pairFlow.mutation || null, refresh_required: true
        });
      }
      const results = Array.isArray(pairFlow.mutation?.results) ? pairFlow.mutation.results : [];
      if (!timesheetLifecycleWorkbenchWakeSuppressed(internalOptions)) {
        requestTimesheetLifecycleWorkbenchWake(env, ctx, {
          action: 'AUTHORISE',
          sourceRoute: 'TIMESHEET_AUTHORISE_CORRECTION_PAIR',
          lifecycleChangedCount: timesheetLifecycleMutationChangedCount(pairFlow.mutation),
          actorUserId: user?.id || null,
          timesheetId: currentTimesheetId || requestedTimesheetId || null
        });
      }
      return withJson(200, {
        ok: true, success: true, authorised: true,
        operation: 'correction_pair_authorise', pair_lifecycle: pairFlow.preview,
        mutation: pairFlow.mutation,
        affected_rows: results.flatMap((row) => Array.isArray(row?.affected_rows) ? row.affected_rows : []),
        affected_timesheet_ids: pairFlow.mutation?.affected_timesheet_ids || [],
        refresh_required: true
      });
    }
  } catch (error) {
    if (['CORRECTION_SOURCE_SCOPE_NOT_ALLOWED', 'WEEKLY_SOURCE_AUTHORISE_ROUTING_UNAVAILABLE', 'SOURCE_CHECK_IN_PROGRESS'].includes(error?.code))
      return withJson(error.status, { ok: false, error: error.code, error_code: error.code, refresh_required: true, retryable: error.status === 503 });
    const pairRateRefusal = bpayNextAuthoriseKnownRefusal(error?.code, error, error?.message);
    if (pairRateRefusal) return withJson(409, pairRateRefusal);
    return withJson(409, {
      ok: false, error: 'CORRECTION_UNIT_LIFECYCLE_TRANSITION_BLOCKED',
      error_code: 'CORRECTION_UNIT_LIFECYCLE_TRANSITION_BLOCKED',
      message: 'The linked correction pair could not be checked safely. Refresh and try again.',
      refresh_required: true
    });
  }
  lifecycleSignatureLog('guard_resolved', {
    requested_timesheet_id: requestedTimesheetId || null,
    expected_timesheet_id: expectedTimesheetId || null,
    current_timesheet_id: currentTimesheetId || null,
    current_version: guard.resolved?.current_version ?? null,
    booking_id: guard.resolved?.booking_id || null,
    was_stale: !!guard.resolved?.was_stale,
    expected_row_signature: expectedRowSignature || null
  });
  const now = nowIso();
  const warnings = [];

  const tsBefore = await sbGetOne(
    env,
    `${env.SUPABASE_URL}/rest/v1/timesheets` +
      `?timesheet_id=eq.${enc(currentTimesheetId)}` +
      `&is_current=eq.true` +
      `&select=timesheet_id,sheet_scope,week_ending_date,worked_start_iso,worked_end_iso,break_start_iso,break_end_iso,break_minutes,reference_number,day_references_json,actual_schedule_json,additional_units_week,additional_units_per_day,qr_status,qr_token,qr_generated_at,qr_last_sent_hash,qr_signed_hash,qr_scanned_at,authorised_at_server`
  ).catch(() => null);

  if (!tsBefore?.timesheet_id) return withCORS(env, req, notFound('Timesheet not found'));

  lifecycleSignatureLog('before_authorise_precheck', {
    current_timesheet_id: currentTimesheetId || null,
    sheet_scope: tsBefore.sheet_scope || null,
    authorised_at_server_present: !!tsBefore.authorised_at_server,
    qr_status: tsBefore.qr_status || null,
    qr_token_present: !!trimStr(tsBefore.qr_token),
    qr_generated_at_present: !!tsBefore.qr_generated_at,
    qr_scanned_at_present: !!tsBefore.qr_scanned_at,
    qr_signed_hash_present: !!trimStr(tsBefore.qr_signed_hash)
  });

  const qrStatus = String(tsBefore.qr_status || '').trim().toUpperCase();
  const qrToken = trimStr(tsBefore.qr_token);
  const hasQrIssuedProof = !!((qrToken && tsBefore.qr_generated_at) || trimStr(tsBefore.qr_last_sent_hash));
  const qrAwaitingSignatureUpload = !!(qrStatus === 'PENDING' && hasQrIssuedProof && !tsBefore.qr_scanned_at);
  if (qrAwaitingSignatureUpload) {
    return withJson(409, {
      error: 'AWAITING_SIGNED_QR',
      error_code: 'AWAITING_SIGNED_QR',
      message: 'Cannot authorise: awaiting signed QR timesheet',
      current_timesheet_id: currentTimesheetId,
      refresh_required: true,
      affected_rows: affectedRowsFor({}, currentTimesheetId)
    });
  }

  const signedHash = trimStr(tsBefore.qr_signed_hash);
  if (signedHash && typeof sha256Hex === 'function') {
    const stableClone = (value) => {
      if (Array.isArray(value)) return value.map(stableClone);
      if (value && typeof value === 'object') {
        const out = {};
        const keys = Object.keys(value).sort();
        for (const key of keys) out[key] = stableClone(value[key]);
        return out;
      }
      return value;
    };
    const scope = String(tsBefore.sheet_scope || '').trim().toUpperCase();
    const currentObject = scope === 'WEEKLY'
      ? {
          sheet_scope: 'WEEKLY',
          week_ending_date: tsBefore.week_ending_date || null,
          actual_schedule_json: tsBefore.actual_schedule_json || null,
          reference_number: tsBefore.reference_number || null,
          day_references_json: tsBefore.day_references_json || null,
          additional_units_week: tsBefore.additional_units_week || {},
          additional_units_per_day: tsBefore.additional_units_per_day || {}
        }
      : {
          sheet_scope: 'DAILY',
          worked_start_iso: tsBefore.worked_start_iso || null,
          worked_end_iso: tsBefore.worked_end_iso || null,
          break_start_iso: tsBefore.break_start_iso || null,
          break_end_iso: tsBefore.break_end_iso || null,
          break_minutes: tsBefore.break_minutes != null ? tsBefore.break_minutes : null,
          reference_number: tsBefore.reference_number || null,
          day_references_json: tsBefore.day_references_json || null
        };
    const currentHash = await sha256Hex(JSON.stringify(stableClone(currentObject)));
    if (String(currentHash) !== String(signedHash)) {
      return withJson(409, {
        error: 'SIGNED_QR_MISMATCH',
        error_code: 'SIGNED_QR_MISMATCH',
        message: 'Cannot authorise: signed QR timesheet no longer matches current hours. Revoke and request resubmission.',
        current_timesheet_id: currentTimesheetId,
        refresh_required: true,
        affected_rows: affectedRowsFor({}, currentTimesheetId)
      });
    }
  }

  try {
    const { rows: precheckRows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/v_ts_invoice_precheck?timesheet_id=eq.${enc(currentTimesheetId)}&select=precheck_status&limit=1`
    );
    const precheckStatus = trimStr(precheckRows?.[0]?.precheck_status).toUpperCase();
    if (precheckStatus === 'BLOCK_NO_PDF') warnings.push('NO_TIMESHEET_EVIDENCE_INVOICE_BLOCKED');
  } catch {
  }

  let rpcPayload = null;

  // WP-23 / Gate 13 hostile finance review F2.  This is the Office Authorise
  // route, and Bulk Authorise reaches it too through
  // `callGoldTimesheetLifecycleActionForBulkItem`.  A Weekly Source week must go
  // to the first-authorisation wrapper, never straight to the ordinary owner:
  // the ordinary owner writes no root-authorisation generation, so the week
  // silently stays unmanaged and every later entitlement publication then raises
  // at the pointer update.  See `weeklySourceAuthoriseRouting` above.
  const weeklySourceRouting = sourcePayPermission.routing ?? await weeklySourceAuthoriseRouting(
    (functionName, args, options) => sbRpc(env, functionName, args, options),
    currentTimesheetId, user?.id || null
  );
  if (weeklySourceRouting.refusal) {
    lifecycleSignatureLog('weekly_source_routing_refused', {
      rpc_function_name: WEEKLY_SOURCE_AUTHORISE_PROBE_RPC,
      current_timesheet_id: currentTimesheetId || null,
      error_code: weeklySourceRouting.refusal.error_code
    });
    return withJson(weeklySourceRouting.refusal.status, {
      error: weeklySourceRouting.refusal.error_code,
      error_code: weeklySourceRouting.refusal.error_code,
      message: weeklySourceRouting.refusal.message,
      current_timesheet_id: currentTimesheetId || null,
      refresh_required: true,
      affected_rows: affectedRowsFor({}, currentTimesheetId)
    });
  }
  if (weeklySourceRouting.bound === true) {
    lifecycleSignatureLog('weekly_source_first_authorise', {
      rpc_function_name: WEEKLY_SOURCE_FIRST_AUTHORISE_RPC,
      requested_timesheet_id: requestedTimesheetId || null,
      expected_timesheet_id: expectedTimesheetId || null,
      current_timesheet_id: currentTimesheetId || null,
      expected_row_signature: expectedRowSignature || null
    });
    let firstAuthorise = null;
    try {
      firstAuthorise = weeklySourceUnwrapRpc(
        // WP-07's signature, unchanged.  The wrapper interposes the serial gate,
        // the rotation lock set and the stale-rotation refusal, then calls the
        // UNCHANGED ordinary Authorise owner itself, exactly once.
        await sbRpc(env, WEEKLY_SOURCE_FIRST_AUTHORISE_RPC, {
          p_timesheet_id: currentTimesheetId,
          p_expected_timesheet_id: expectedTimesheetId || currentTimesheetId,
          p_expected_row_signature: expectedRowSignature || null,
          p_actor_user_id: user?.id || null
        }, { timeoutMs: 20000 }),
        WEEKLY_SOURCE_FIRST_AUTHORISE_RPC
      );
    } catch (err) {
      const rpcErr = parseRpcFailure(err);
      const detailObj = rpcErr.detailJson || parseMaybeJsonObj(rpcErr.details) || {};
      return mutationErrorResponse(rpcErr.message || rpcErr.code, detailObj, {
        currentTimesheetId,
        expectedTimesheetId,
        expectedRowSignature,
        message: rpcErr.message
      });
    }
    if (!firstAuthorise || firstAuthorise.ok !== true) {
      return mutationErrorResponse(
        firstString(firstAuthorise?.code, firstAuthorise?.error_code, 'WEEKLY_SOURCE_FIRST_AUTHORISE_REFUSED'),
        firstAuthorise || {},
        {
          currentTimesheetId,
          expectedTimesheetId,
          expectedRowSignature,
          message: firstAuthorise?.refusal_message || firstAuthorise?.reason
        }
      );
    }
    // The wrapper returns the ordinary owner's own result under
    // `authorise_result`, so the response shape the Office and the bulk
    // orchestrator read is rebuilt from it and neither surface has to know which
    // owner ran.
    rpcPayload = {
      ok: true,
      success: true,
      operation: firstAuthorise?.authorise_result?.operation || 'AUTHORISE',
      timesheet_id: firstString(firstAuthorise?.authorise_result?.timesheet_id, firstAuthorise?.timesheet_id, currentTimesheetId),
      current_timesheet_id: firstString(firstAuthorise?.authorise_result?.timesheet_id, firstAuthorise?.timesheet_id, currentTimesheetId),
      contract_week_id: firstAuthorise?.authorise_result?.contract_week_id || null,
      processing_status: firstAuthorise?.authorise_result?.processing_status || null,
      new_processing_status: firstAuthorise?.authorise_result?.processing_status || null,
      backend_row_signature: firstAuthorise?.authorised_row_signature || null,
      row_signature: firstAuthorise?.authorised_row_signature || null,
      weekly_source_first_authorisation: {
        root_authorisation_id: firstAuthorise?.root_authorisation_id || null,
        authorisation_generation: firstAuthorise?.authorisation_generation ?? null,
        family_booking_id: firstAuthorise?.family_booking_id || null,
        gate: firstAuthorise?.gate || null
      }
    };
  }

  if (!rpcPayload) {
  try {
    lifecycleSignatureLog('before_rpc', {
      rpc_function_name: authoriseRpcFunctionName,
      requested_timesheet_id: requestedTimesheetId || null,
      expected_timesheet_id: expectedTimesheetId || null,
      current_timesheet_id: currentTimesheetId || null,
      expected_row_signature: expectedRowSignature || null
    });
    const authoriseRpcArgs = {
      p_timesheet_id: requestedTimesheetId,
      p_expected_timesheet_id: expectedTimesheetId,
      p_actor_user_id: user?.id || null,
      p_now_utc: now,
      p_expected_row_signature: expectedRowSignature || null
    };
    if (duplicateExpenseConfirmed) authoriseRpcArgs.p_duplicate_expense_confirmed = true;
    const rpcRes = await callTimesheetLifecycleRpcWithTransientRetry(env, authoriseRpcFunctionName, authoriseRpcArgs, {
      timeoutMs: 12000,
      lifecycleAction: 'AUTHORISE',
      routeFamily: 'TIMESHEET_AUTHORISE',
      requestedTimesheetId,
      expectedTimesheetId,
      currentTimesheetId,
      expectedRowSignature,
      actorUserId: user?.id || null,
      maxAttempts: 4,
      retryBudgetMs: 1800,
      retryDelaysMs: [0, 125, 375, 850],
      jitterMs: 75
    });
    rpcPayload = unwrapRpcPayload(rpcRes, authoriseRpcFunctionName);
    lifecycleSignatureLog('rpc_completed', {
      rpc_function_name: authoriseRpcFunctionName,
      requested_timesheet_id: requestedTimesheetId || null,
      expected_timesheet_id: expectedTimesheetId || null,
      current_timesheet_id: currentTimesheetId || null,
      payload_signature: lifecycleSignatureDiagSignature(rpcPayload),
      payload_keys: lifecycleSignatureDiagObjectKeys(rpcPayload)
    });
  } catch (err) {
    const rpcErr = parseRpcFailure(err);
    const detailObj = rpcErr.detailJson || parseMaybeJsonObj(rpcErr.details) || {};
    lifecycleSignatureLog('rpc_failed', {
      rpc_function_name: authoriseRpcFunctionName,
      status: rpcErr.status || null,
      message: rpcErr.message || null,
      code: rpcErr.code || null,
      expected_row_signature: expectedRowSignature || detailObj?.expected_row_signature || null,
      current_row_signature: detailObj?.current_row_signature || null,
      current_timesheet_id: detailObj?.current_timesheet_id || currentTimesheetId || null,
      contract_week_id: detailObj?.contract_week_id || null
    });
    if (bpayNextAuthoriseKnownRefusal(rpcErr.code, detailObj, rpcErr.message)) {
      return mutationErrorResponse(rpcErr.code, detailObj, { currentTimesheetId, message: rpcErr.message });
    }
    if (isUnknownLifecycleOutcomeError(err, rpcErr)) {
      let reconciliation = null;
      if (typeof reconcileTimesheetLifecycleAfterUnknownOutcome === 'function') {
        try {
          reconciliation = await reconcileTimesheetLifecycleAfterUnknownOutcome(env, {
            action: 'AUTHORISE',
            requestedTimesheetId,
            expectedTimesheetId,
            expectedRowSignature,
            actorUserId: user?.id || null,
            routeFamily: 'TIMESHEET_AUTHORISE',
            rpcFunctionName: authoriseRpcFunctionName,
            originalError: err,
            startedAtUtc: now,
            guardResult: guard,
            resolved: guard.resolved || null
          });
        } catch (reconcileErr) {
          lifecycleSignatureLog('timeout_reconciliation_failed', {
            rpc_function_name: authoriseRpcFunctionName,
            status: Number(reconcileErr?.status || 0) || null,
            message: String(reconcileErr?.message || reconcileErr || '') || null,
            current_timesheet_id: currentTimesheetId || null,
            expected_row_signature: expectedRowSignature || null
          });
          reconciliation = null;
        }
      }

      lifecycleSignatureLog('timeout_reconciliation_decision', {
        rpc_function_name: authoriseRpcFunctionName,
        current_timesheet_id: reconciliation?.current_timesheet_id || currentTimesheetId || null,
        target_state_reached: reconciliation?.target_state_reached === true,
        safe_retry_allowed: reconciliation?.safe_retry_allowed === true,
        stale_lifecycle_state: reconciliation?.stale_lifecycle_state === true,
        recommended_response_code: reconciliation?.recommended_response_code || null,
        backend_row_signature_present: !!firstString(reconciliation?.backend_row_signature, reconciliation?.row_signature)
      });

      if (reconciliation?.target_state_reached === true) {
        rpcPayload = rpcPayloadFromReconciliation(reconciliation, 'timesheet_authorise');
      } else if (reconciliation?.safe_retry_allowed === true) {
        try {
          lifecycleSignatureLog('timeout_safe_retry_before_rpc', {
            rpc_function_name: authoriseRpcFunctionName,
            requested_timesheet_id: requestedTimesheetId || null,
            expected_timesheet_id: expectedTimesheetId || null,
            current_timesheet_id: reconciliation.current_timesheet_id || currentTimesheetId || null,
            expected_row_signature: expectedRowSignature || null
          });
          const retryRpcArgs = {
            p_timesheet_id: requestedTimesheetId,
            p_expected_timesheet_id: expectedTimesheetId,
            p_actor_user_id: user?.id || null,
            p_now_utc: now,
            p_expected_row_signature: expectedRowSignature || null
          };
          if (duplicateExpenseConfirmed) retryRpcArgs.p_duplicate_expense_confirmed = true;
          const retryRes = await callTimesheetLifecycleRpcWithTransientRetry(env, authoriseRpcFunctionName, retryRpcArgs, {
            timeoutMs: 9000,
            lifecycleAction: 'AUTHORISE',
            routeFamily: 'TIMESHEET_AUTHORISE',
            requestedTimesheetId,
            expectedTimesheetId,
            currentTimesheetId,
            expectedRowSignature,
            actorUserId: user?.id || null,
            maxAttempts: 1,
            retryBudgetMs: 0,
            retryDelaysMs: [0],
            jitterMs: 0
          });
          rpcPayload = unwrapRpcPayload(retryRes, authoriseRpcFunctionName);
          lifecycleSignatureLog('timeout_safe_retry_completed', {
            rpc_function_name: authoriseRpcFunctionName,
            payload_signature: lifecycleSignatureDiagSignature(rpcPayload),
            payload_keys: lifecycleSignatureDiagObjectKeys(rpcPayload)
          });
        } catch (retryErr) {
          const retryRpcErr = parseRpcFailure(retryErr);
          let retryReconciliation = null;
          if (typeof reconcileTimesheetLifecycleAfterUnknownOutcome === 'function') {
            try {
              retryReconciliation = await reconcileTimesheetLifecycleAfterUnknownOutcome(env, {
                action: 'AUTHORISE',
                requestedTimesheetId,
                expectedTimesheetId,
                expectedRowSignature,
                actorUserId: user?.id || null,
                routeFamily: 'TIMESHEET_AUTHORISE',
                rpcFunctionName: authoriseRpcFunctionName,
                originalError: retryErr,
                retryError: retryErr,
                startedAtUtc: now,
                guardResult: guard,
                resolved: guard.resolved || null
              });
            } catch {}
          }
          if (retryReconciliation?.target_state_reached === true) {
            rpcPayload = rpcPayloadFromReconciliation(retryReconciliation, 'timesheet_authorise');
          } else {
            const retryCode = String(retryRpcErr.code || '').toUpperCase() === 'ROW_SIGNATURE_MISMATCH'
              ? 'STALE_LIFECYCLE_STATE'
              : 'LIFECYCLE_TIMEOUT_RETRY_EXHAUSTED';
            return lifecycleOutcomeResponse(retryReconciliation || reconciliation, retryRpcErr, { retryExhausted: true, code: retryCode });
          }
        }
      } else {
        return lifecycleOutcomeResponse(reconciliation, rpcErr, { code: reconciliation?.recommended_response_code || 'UNKNOWN_LIFECYCLE_OUTCOME' });
      }
    } else {
      return mutationErrorResponse(rpcErr.code || rpcErr.message, detailObj, {
        currentTimesheetId,
        expectedTimesheetId,
        expectedRowSignature,
        message: rpcErr.message
      });
    }
  }

  }

  if (!rpcPayload) {
    lifecycleSignatureLog('rpc_payload_missing', {
      rpc_function_name: authoriseRpcFunctionName,
      requested_timesheet_id: requestedTimesheetId || null,
      expected_timesheet_id: expectedTimesheetId || null,
      current_timesheet_id: currentTimesheetId || null,
      expected_row_signature: expectedRowSignature || null
    });
    return lifecycleOutcomeResponse(null, { code: 'UNKNOWN_LIFECYCLE_OUTCOME' }, { code: 'UNKNOWN_LIFECYCLE_OUTCOME', payloadMissing: true });
  }
  if (rpcPayload.ok === false || rpcPayload.success === false) {
    lifecycleSignatureLog('rpc_payload_not_ok', {
      rpc_function_name: authoriseRpcFunctionName,
      current_timesheet_id: firstString(rpcPayload.current_timesheet_id, rpcPayload.timesheet_id, currentTimesheetId) || null,
      expected_row_signature: expectedRowSignature || rpcPayload.expected_row_signature || null,
      current_row_signature: rpcPayload.current_row_signature || null,
      error_code: firstString(rpcPayload.error_code, rpcPayload.error, rpcPayload.message) || null,
      payload_signature: lifecycleSignatureDiagSignature(rpcPayload),
      payload_keys: lifecycleSignatureDiagObjectKeys(rpcPayload)
    });
    return mutationErrorResponse(firstString(rpcPayload.error_code, rpcPayload.error, rpcPayload.message), rpcPayload, {
      currentTimesheetId,
      expectedTimesheetId,
      expectedRowSignature,
      message: rpcPayload.message
    });
  }

  const finalTimesheetId = firstString(rpcPayload.current_timesheet_id, rpcPayload.timesheet_id, currentTimesheetId);
  const backendRowSignature = firstString(rpcPayload.backend_row_signature, rpcPayload.row_signature);
  const finalWarnings = warnings.slice();
  if (Array.isArray(rpcPayload.warnings)) {
    for (const warning of rpcPayload.warnings) {
      const text = trimStr(warning);
      if (text && finalWarnings.indexOf(text) === -1) finalWarnings.push(text);
    }
  }


  if (!timesheetLifecycleWorkbenchWakeSuppressed(internalOptions)) {
    requestTimesheetLifecycleWorkbenchWake(env, ctx, {
      action: 'AUTHORISE',
      sourceRoute: 'TIMESHEET_AUTHORISE',
      lifecycleChangedCount: 1,
      actorUserId: user?.id || null,
      timesheetId: finalTimesheetId || requestedTimesheetId || null
    });
  }

  const decision = {
    ok: true,
    blocked: false,
    action: 'AUTHORISE',
    scope: 'TIMESHEET',
    timesheet_id: finalTimesheetId,
    contract_week_id: rpcPayload.contract_week_id || null,
    booking_id: rpcPayload.booking_id || guard.resolved?.booking_id || null,
    requested_timesheet_id: rpcPayload.requested_timesheet_id || guard.resolved?.requested_timesheet_id || requestedTimesheetId,
    current_timesheet_id: finalTimesheetId,
    current_version: rpcPayload.current_version ?? guard.resolved?.current_version ?? null,
    was_stale: rpcPayload.was_stale != null ? !!rpcPayload.was_stale : !!guard.resolved?.was_stale,
    processing_status_before: rpcPayload.previous_processing_status || null,
    processing_status_after: rpcPayload.new_processing_status || rpcPayload.processing_status || null,
    warnings: finalWarnings,
    validation_status: rpcPayload.validation_status || null,
    validation_pre_validated: boolish(rpcPayload.validation_pre_validated),
    hr_validation_required_for_invoice: boolish(rpcPayload.hr_validation_required_for_invoice)
  };

  const lifecyclePatchBundle = buildTimesheetLifecyclePatchFromMutation({
    action: 'AUTHORISE',
    weeklySourcePaymentScope: weeklySourceRouting.bound === true,
    weeklySourcePaymentActor: user,
    requestedTimesheetId,
    currentTimesheetId: finalTimesheetId,
    currentVersion: decision.current_version,
    contractWeekId: rpcPayload.contract_week_id || null,
    bookingId: decision.booking_id || null,
    candidateId: rpcPayload.candidate_id || null,
    atomicRpcPayload: rpcPayload,
    guardResult: guard,
    previousProcessingStatus: rpcPayload.previous_processing_status || null,
    newProcessingStatus: rpcPayload.new_processing_status || rpcPayload.processing_status || null,
    backendRowSignature,
    rowSignature: backendRowSignature,
    expectedRowSignature: backendRowSignature || expectedRowSignature || null,
    authorised: true,
    authorisedAtUtc: rpcPayload.authorised_at_utc || rpcPayload.authorised_at || now,
    authorisedAtServer: rpcPayload.authorised_at_server || now,
    readyToPay: rpcPayload.ready_to_pay,
    existingAffectedRows: affectedRowsFor(rpcPayload, finalTimesheetId),
    staleMarkers: {
      was_stale: decision.was_stale,
      timesheet_moved: !!(requestedTimesheetId && finalTimesheetId && requestedTimesheetId !== finalTimesheetId)
    }
  });

  lifecycleSignatureLog('response_shape', {
    status: 200,
    rpc_function_name: 'timesheet_authorise_generic_atomic',
    requested_timesheet_id: requestedTimesheetId || null,
    expected_timesheet_id: expectedTimesheetId || null,
    current_timesheet_id: finalTimesheetId || null,
    contract_week_id: rpcPayload.contract_week_id || null,
    booking_id: decision.booking_id || null,
    current_version: decision.current_version ?? null,
    was_stale: decision.was_stale,
    processing_status_before: rpcPayload.previous_processing_status || null,
    processing_status_after: rpcPayload.new_processing_status || rpcPayload.processing_status || null,
    expected_row_signature: expectedRowSignature || null,
    backend_row_signature: backendRowSignature || null,
    response_signature: lifecycleSignatureDiagSignature({
      backend_row_signature: backendRowSignature || null,
      mutation_row_signature: backendRowSignature || null,
      row_signature: backendRowSignature || null,
      expected_row_signature: backendRowSignature || null
    }),
    response_contains: lifecycleSignatureDiagResponseContains({
      ok: true,
      success: true,
      authorised: true,
      backend_row_signature: backendRowSignature || null,
      mutation_row_signature: backendRowSignature || null,
      row_signature: backendRowSignature || null,
      expected_row_signature: backendRowSignature || null,
      decision,
      lifecycle_patch: lifecyclePatchBundle.lifecycle_patch,
      affected_rows: lifecyclePatchBundle.affected_rows,
      refresh_hints: lifecyclePatchBundle.refresh_hints,
      permission_state_patch_complete: lifecyclePatchBundle.permission_state_patch_complete,
      priority_badges_patch_complete: lifecyclePatchBundle.priority_badges_patch_complete,
      immediate_lifecycle_patch_available: lifecyclePatchBundle.immediate_lifecycle_patch_available,
      requires_affected_row_refresh: lifecyclePatchBundle.requires_affected_row_refresh,
      requires_full_details_refresh: lifecyclePatchBundle.requires_full_details_refresh,
      refresh_required: lifecyclePatchBundle.refresh_required,
      cache_invalidation_hints: (rpcPayload.cache_invalidation_hints && typeof rpcPayload.cache_invalidation_hints === 'object') ? rpcPayload.cache_invalidation_hints : {}
    })
  });

  return withCORS(env, req, ok({
    ok: true,
    success: true,
    authorised: true,
    tsfin_updated: true,
    operation: rpcPayload.operation || 'timesheet_authorise',
    processing_status: rpcPayload.processing_status || rpcPayload.new_processing_status || null,
    previous_processing_status: rpcPayload.previous_processing_status || null,
    new_processing_status: rpcPayload.new_processing_status || rpcPayload.processing_status || null,
    warnings: finalWarnings,
    decision,
    booking_id: decision.booking_id,
    requested_timesheet_id: decision.requested_timesheet_id,
    current_timesheet_id: finalTimesheetId,
    timesheet_id: finalTimesheetId,
    contract_week_id: rpcPayload.contract_week_id || null,
    current_version: decision.current_version,
    was_stale: decision.was_stale,
    backend_row_signature: backendRowSignature || null,
    mutation_row_signature: backendRowSignature || null,
    row_signature: backendRowSignature || null,
    expected_row_signature: backendRowSignature || null,
    lifecycle_patch: lifecyclePatchBundle.lifecycle_patch,
    affected_rows: lifecyclePatchBundle.affected_rows,
    refresh_hints: lifecyclePatchBundle.refresh_hints,
    permission_state_patch_complete: lifecyclePatchBundle.permission_state_patch_complete,
    priority_badges_patch_complete: lifecyclePatchBundle.priority_badges_patch_complete,
    immediate_lifecycle_patch_available: lifecyclePatchBundle.immediate_lifecycle_patch_available,
    requires_affected_row_refresh: lifecyclePatchBundle.requires_affected_row_refresh,
    requires_full_details_refresh: lifecyclePatchBundle.requires_full_details_refresh,
    refresh_required: lifecyclePatchBundle.refresh_required,
    cache_invalidation_hints: (rpcPayload.cache_invalidation_hints && typeof rpcPayload.cache_invalidation_hints === 'object') ? rpcPayload.cache_invalidation_hints : {}
  }));
}