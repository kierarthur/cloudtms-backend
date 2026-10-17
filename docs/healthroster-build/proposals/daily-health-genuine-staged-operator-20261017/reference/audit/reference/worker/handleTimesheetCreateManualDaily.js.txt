async function handleTimesheetCreateManualDaily(env, req) {
  const enc = encodeURIComponent;

  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  let body;
  try {
    body = await parseJSONBody(req);
  } catch {
    return withCORS(env, req, badRequest('Invalid JSON'));
  }

  const candidateId = String(body?.candidate_id || '').trim();
  const clientId = String(body?.client_id || '').trim();

  if (!candidateId) return withCORS(env, req, badRequest('candidate_id is required'));
  if (!clientId) return withCORS(env, req, badRequest('client_id is required'));

  const scheduleInput =
    body && typeof body.schedule_json === 'object' && body.schedule_json && !Array.isArray(body.schedule_json)
      ? body.schedule_json
      : null;

  if (!scheduleInput) {
    return withCORS(env, req, badRequest('schedule_json is required and must be an object'));
  }

  const now = nowIso();

  const trimStr = (v) => String(v == null ? '' : v).trim();

  const lowerOrNull = (v) => {
    const s = trimStr(v).toLowerCase();
    return s || null;
  };

  const normRole = (v) => {
    const s = trimStr(v).replace(/\s+/g, ' ').trim();
    return s ? s.toUpperCase() : null;
  };

  const normBand = (v) => {
    const s = trimStr(v);
    if (!s) return null;

    let m = s.match(/^\s*band\s*([0-9]+)\s*$/i);
    if (!m) m = s.match(/^\s*b\s*([0-9]+)\s*$/i);
    if (!m) m = s.match(/^\s*([0-9]+)\s*$/);

    if (m && m[1]) return `Band ${parseInt(m[1], 10)}`;
    return s;
  };

  const candidate = await sbGetOne(
    env,
    `${env.SUPABASE_URL}/rest/v1/candidates` +
      `?id=eq.${enc(candidateId)}` +
      `&select=id,display_name,first_name,last_name,key_norm,pay_method,umbrella_id,roles,mileage_pay_rate` +
      `&limit=1`
  );
  if (!candidate) return withCORS(env, req, notFound('Candidate not found'));

  const client = await sbGetOne(
    env,
    `${env.SUPABASE_URL}/rest/v1/clients` +
      `?id=eq.${enc(clientId)}` +
      `&select=id,name,mileage_charge_rate` +
      `&limit=1`
  );
  if (!client) return withCORS(env, req, notFound('Client not found'));

  const workedDateFallback = trimStr(body?.worked_date_ymd || '');
  const mappedSchedule = mapCanonicalDailyScheduleToIso(scheduleInput, {
    workedDateFallback,
    ukLocalToUtcISO
  });
  if (mappedSchedule && mappedSchedule.error) {
    return withCORS(env, req, badRequest(mappedSchedule.error));
  }

  const dateYmd = mappedSchedule.ymd;

  const inputRole = normRole(body?.role);
  const inputBand = normBand(body?.band);
  const gradeRaw = trimStr(body?.gradeRaw || '') || null;

  let resolvedRole = inputRole;
  let resolvedBand = inputBand;

  if (!resolvedRole && gradeRaw) {
    const mappedRole = await mapRequestGradeToRole(env, {
      client_id: clientId,
      candidate_id: candidateId,
      gradeRaw,
      dateYmd,
      candidate_roles: null
    });

    if (mappedRole?.role) resolvedRole = normRole(mappedRole.role);
    if (!resolvedBand && mappedRole?.band) resolvedBand = normBand(mappedRole.band);
  }

  const generatedId =
    (globalThis.crypto?.randomUUID?.() || `${Date.now()}-${Math.random().toString(16).slice(2)}`).toString();

  const hospitalRaw = trimStr(body?.hospital || '') || trimStr(client?.name || '');
  const wardRaw = trimStr(body?.ward || '');
  const shiftLabelRaw = trimStr(body?.shift_label || '') || `daily-manual-${generatedId}`;

  const occupantKeyNorm = lowerOrNull(
    candidate?.key_norm ||
    candidate?.display_name ||
    [candidate?.first_name, candidate?.last_name].filter(Boolean).join(' ') ||
    candidateId
  );

  const hospitalNorm = lowerOrNull(hospitalRaw);
  const wardNorm = lowerOrNull(wardRaw);
  const jobTitleNorm = lowerOrNull(resolvedRole);
  const shiftLabelNorm = lowerOrNull(shiftLabelRaw);

  const bookingId = await makeBookingId(
    candidateId,
    dateYmd,
    hospitalNorm,
    wardNorm,
    jobTitleNorm,
    shiftLabelNorm || ''
  );

  if (!bookingId || bookingId === '{}' || bookingId === 'null' || bookingId === 'undefined') {
    return withCORS(env, req, badRequest(`Invalid booking_id produced: "${bookingId}"`));
  }

  const weekEndingDate = weekEndingSunday(dateYmd);

  const candidateHintText = {
    candidate_id: candidateId,
    display_name: candidate?.display_name || null,
    first_name: candidate?.first_name || null,
    last_name: candidate?.last_name || null
  };

  const tsInsert = [{
    booking_id: bookingId,
    version: 1,
    is_current: true,
    status: 'RECEIVED',

    occupant_key_norm: occupantKeyNorm,
    hospital_norm: hospitalNorm,
    ward_norm: wardNorm,
    job_title_norm: jobTitleNorm,
    shift_label_norm: shiftLabelNorm,

    scheduled_start_iso: mappedSchedule.worked_start_iso,
    scheduled_end_iso: mappedSchedule.worked_end_iso,
    worked_start_iso: mappedSchedule.worked_start_iso,
    worked_end_iso: mappedSchedule.worked_end_iso,
    break_start_iso: mappedSchedule.break_start_iso,
    break_end_iso: mappedSchedule.break_end_iso,
    break_minutes: mappedSchedule.break_minutes,
    worked_minutes: mappedSchedule.worked_minutes,

    week_ending_date: weekEndingDate,

    auth_name: null,
    auth_job_title: null,
    authorised_at_server: null,

    r2_nurse_key: null,
    r2_auth_key: null,
    img_sha256_nurse: null,
    img_sha256_auth: null,

    reference_number: null,
    reference_set_at: null,

    idempotency_key: generatedId,
    client_hash: null,
    client_ua: req.headers.get('user-agent') || '',

    created_at: now,
    updated_at: now,

    contract_id: null,
    submission_mode: 'MANUAL',
    manual_pdf_r2_key: null,
    line_type: 'HOURS',
    sheet_scope: 'DAILY',

    actual_schedule_json: mappedSchedule.normalized_schedule_json,
    additional_units_week: {},
    additional_units_per_day: {},

    qr_token: null,
    qr_status: null,
    qr_payload_json: {},
    qr_generated_at: null,
    qr_scanned_at: null,
    qr_scan_info_json: null,
    qr_r2_key: null,

    day_references_json: null,
    manual_pdf_rotation_degrees: 0,
    qr_last_sent_hash: null,
    qr_last_sent_at_utc: null,
    qr_signed_hash: null,
    qr_signed_at_utc: null,

    candidate_hint_text: candidateHintText,
    band: resolvedBand,
    generated_pdf_at_utc: null,

    is_adjustment: false,
    parent_timesheet_id: null,

    generated_pdf_refs_sig: null,
    generated_pdf_refs_snapshot_json: null,
    generated_pdf_refs_captured_at_utc: null,
    qr_sent_refs_sig: null,
    qr_sent_refs_snapshot_json: null,
    qr_sent_refs_captured_at_utc: null,

    correction_id: null,
    correction_kind: null,
    adjustment_origin: null
  }];

  const ins = await fetch(`${env.SUPABASE_URL}/rest/v1/timesheets`, {
    method: 'POST',
    headers: { ...sbHeaders(env), Prefer: 'return=representation' },
    body: JSON.stringify(tsInsert)
  });

  if (!ins.ok) {
    return withCORS(env, req, serverError(await ins.text()));
  }

  const createdTs = (await ins.json().catch(() => []))[0] || null;
  if (!createdTs?.timesheet_id) {
    return withCORS(env, req, serverError('Failed to create manual daily timesheet'));
  }

  // One canonical DAILY authority owns all time bucketing, rates and TSFIN.
  // The create route only creates the row, queues it and invokes that owner.
  try {
    await saveAndRecalculateCanonicalDaily(env, {
      timesheetId: createdTs.timesheet_id,
      forceUnprocessed: true
    });
  } catch (e) {
    await fetch(
      `${env.SUPABASE_URL}/rest/v1/timesheets?timesheet_id=eq.${enc(createdTs.timesheet_id)}&is_current=eq.true`,
      {
        method: 'DELETE',
        headers: { ...sbHeaders(env), Prefer: 'return=minimal' }
      }
    ).catch(() => {});

    return withCORS(
      env,
      req,
      serverError(String(e?.message || e || 'Failed to create canonical DAILY financial snapshot'))
    );
  }

  try {
    await writeAudit(
      env,
      user,
      'TIMESHEET_CREATED',
      {
        timesheet_id: createdTs.timesheet_id,
        candidate_id: candidateId,
        client_id: clientId,
        sheet_scope: 'DAILY',
        submission_mode: 'MANUAL',
        source: 'timesheet_create_manual_daily',
        work_date: dateYmd,
        created_unprocessed: true,
        role: resolvedRole || null,
        band: resolvedBand || null
      },
      { entity: 'timesheets', subject_id: createdTs.timesheet_id, req }
    );
  } catch {}

  return withCORS(env, req, ok({
    timesheet_id: createdTs.timesheet_id,
    current_timesheet_id: createdTs.timesheet_id,
    created_unprocessed: true
  }));
}