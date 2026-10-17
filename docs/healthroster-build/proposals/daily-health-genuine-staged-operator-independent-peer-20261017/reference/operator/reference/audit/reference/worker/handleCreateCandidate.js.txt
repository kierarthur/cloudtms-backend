async function handleCreateCandidate(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  const dataRaw = await parseJSONBody(req);
  if (!dataRaw) return withCORS(env, req, badRequest("Invalid JSON"));

  // Strip any accidental CCR fields (immutable/minted by DB)
  const { tms_ref, ccr_num, job_titles, ...data } = dataRaw;

  const enc = encodeURIComponent;

  // ✅ NEW: normalise remittance override booleans on create (accept 'on'/'true'/1, empty => false)
  const normalizeBoolish = (v) => {
    if (typeof v === 'boolean') return v;
    if (v === null || v === undefined) return false;
    if (typeof v === 'number') return v === 1;
    const s = String(v).trim().toLowerCase();
    if (!s) return false;
    if (['true','1','yes','y','on'].includes(s)) return true;
    if (['false','0','no','n','off'].includes(s)) return false;
    return false;
  };
  const normalizeBoolField = (obj, key) => {
    if (!Object.prototype.hasOwnProperty.call(obj, key)) return;
    obj[key] = normalizeBoolish(obj[key]);
  };
  normalizeBoolField(data, 'remittance_overrides_enabled');
  normalizeBoolField(data, 'remittance_receive_enabled');
  normalizeBoolField(data, 'remittances_detailed_breakdown');
  normalizeBoolField(data, 'remittance_receive_when_umbrella_paid');

  // Normalise NHSP/HR name aliases (JSONB) if provided
  if (Object.prototype.hasOwnProperty.call(data, 'nhsp_hr_name_aliases')) {
    const rawAliases = data.nhsp_hr_name_aliases;
    let aliases = [];

    if (Array.isArray(rawAliases)) {
      aliases = rawAliases
        .map((x) => String(x || '').trim().toLowerCase())
        .filter(Boolean);
    } else if (typeof rawAliases === 'string') {
      const s = rawAliases.trim();
      if (s) {
        if (s.startsWith('[')) {
          try {
            const arr = JSON.parse(s);
            if (Array.isArray(arr)) {
              aliases = arr
                .map((x) => String(x || '').trim().toLowerCase())
                .filter(Boolean);
            }
          } catch {
            aliases = s
              .split(',')
              .map((x) => x.trim().toLowerCase())
              .filter(Boolean);
          }
        } else {
          aliases = s
            .split(',')
            .map((x) => x.trim().toLowerCase())
            .filter(Boolean);
        }
      }
    } else if (rawAliases != null) {
      const s = String(rawAliases).trim();
      if (s) aliases = [s.toLowerCase()];
    }

    data.nhsp_hr_name_aliases = Array.from(new Set(aliases));
  }

  // ✅ Normalise key_norm (GCK) and allow empty -> null
  if (Object.prototype.hasOwnProperty.call(data, 'key_norm')) {
    const v = String(data.key_norm ?? '').trim();
    data.key_norm = v ? v.toLowerCase() : null;
  }

  // ✅ Enforce key_norm uniqueness if provided
  if (data.key_norm) {
    const other = await sbGetOne(
      env,
      `${env.SUPABASE_URL}/rest/v1/candidates` +
        `?key_norm=eq.${enc(data.key_norm)}` +
        `&select=id` +
        `&limit=1`
    ).catch(() => null);

    if (other?.id) {
      return withCORS(env, req, badRequest('Global Candidate Key (key_norm) is already assigned to another candidate.'));
    }
  }

  // Normalise pay_method: only PAYE/UMBRELLA; Unknown/other -> "use DB default" (do not send)
  if (Object.prototype.hasOwnProperty.call(data, 'pay_method')) {
    let pm = (data.pay_method == null ? '' : String(data.pay_method)).trim().toUpperCase();
    if (pm === 'UNKNOWN' || pm === '') {
      delete data.pay_method;
    } else if (pm === 'PAYE' || pm === 'UMBRELLA') {
      data.pay_method = pm;
    } else {
      delete data.pay_method;
    }
  }

  // Normalise job_titles array -> list of UUID strings
  const jtArray = Array.isArray(job_titles)
    ? job_titles.map((x) => String(x)).filter(Boolean)
    : [];

  const primaryJobTitleId = jtArray[0] || null;

  const enqueueTsfinOutbox = async (items) => {
    if (!Array.isArray(items) || !items.length) return { enqueued: 0 };
    try {
      await fetch(
        `${env.SUPABASE_URL}/rest/v1/ts_financials_outbox?on_conflict=timesheet_id,reason`,
        {
          method: "POST",
          headers: { ...sbHeaders(env), "Prefer": "resolution=ignore-duplicates" },
          body: JSON.stringify(items)
        }
      );
      return { enqueued: items.length };
    } catch (e) {
      console.warn('[TSFIN] enqueue outbox failed', e?.message || e);
      return { enqueued: 0 };
    }
  };

  const runTsfinImmediate = async (limitHint) => {
    try {
      if (typeof runTsfinWorkerOnce !== 'function') {
        return { ran: false, picked: 0, ok: 0, fail: 0 };
      }
      const limit = Math.min(Math.max(parseInt(limitHint || '50', 10) || 50, 1), 200);
      const res = await runTsfinWorkerOnce(env, { limit });
      return { ran: true, ...(res || {}) };
    } catch (e) {
      console.warn('[TSFIN] immediate run failed', e?.message || e);
      return { ran: false, picked: 0, ok: 0, fail: 0 };
    }
  };

 const assignTimesheetsByOccKey = async (occKeyLower, candId) => {
  // candId is kept as a guard so callers don’t accidentally call with null,
  // but we do NOT write candidate_id into timesheets (column does not exist).
  if (!occKeyLower || !candId) return [];
  try {
    const { rows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/timesheets` +
        `?is_current=eq.true` +
        `&occupant_key_norm=eq.${enc(occKeyLower)}` +
        `&select=timesheet_id` +
        `&limit=500`
    );

    const ids = (rows || []).map(r => r?.timesheet_id).filter(Boolean);

    // ✅ Do NOT PATCH timesheets here (timesheets.candidate_id does not exist).
    // TSFIN rebuild will populate candidate_id/client_id in timesheets_financials / v_timesheets_summary.
    return ids;
  } catch (e) {
    console.warn('[CAND] assignTimesheetsByOccKey failed', e?.message || e);
    return [];
  }
};

  try {
    // Insert candidate; set job_title_id to primary (or null)
    const res = await fetch(`${env.SUPABASE_URL}/rest/v1/candidates`, {
      method: "POST",
      headers: { ...sbHeaders(env), "Prefer": "return=representation" },
      body: JSON.stringify({
        ...data,
        job_title_id: primaryJobTitleId,
        created_at: new Date().toISOString()
      })
    });
    if (!res.ok) {
      const err = await res.text();
      return withCORS(env, req, badRequest(`Candidate creation failed: ${err}`));
    }

    const json = await res.json().catch(() => ({}));
    const candidate = Array.isArray(json) ? json[0] : json;
    const candidateId = candidate?.id;

    // Insert candidate_job_titles join rows if provided
    if (candidateId && jtArray.length) {
      const nowIso = new Date().toISOString();
      const rows = jtArray.map((jobTitleId, idx) => ({
        candidate_id: candidateId,
        job_title_id: jobTitleId,
        is_primary: idx === 0,
        created_at: nowIso,
        updated_at: nowIso
      }));

      const jtRes = await fetch(`${env.SUPABASE_URL}/rest/v1/candidate_job_titles`, {
        method: "POST",
        headers: { ...sbHeaders(env), "Prefer": "resolution=ignore-duplicates" },
        body: JSON.stringify(rows)
      });

      if (!jtRes.ok) {
        const err = await jtRes.text().catch(() => 'insert candidate_job_titles failed');
        console.error('candidate_job_titles insert failed', err);
      }
    }

    // ✅ NEW: on candidate create, try to resolve any unassigned timesheets by key_norm
    let autoAssignedTimesheets = [];
    if (candidateId && candidate?.key_norm) {
      const k = String(candidate.key_norm || '').trim().toLowerCase();
      if (k) {
        autoAssignedTimesheets = await assignTimesheetsByOccKey(k, candidateId);
      }
    }

// ✅ Priority enqueue + targeted drain for any auto-assigned timesheets
const tsfinRun = await tsfinTargetedDrainNow(env, {
  timesheetIds: autoAssignedTimesheets,
  reason: 'CONTEXT_CHANGED',
  chunkSize: 50
});

return withCORS(env, req, ok({
  candidate,
  tsfin: {
    enqueued: tsfinRun.enqueued || 0,
    ran_now: tsfinRun.ran || 0,
    picked: tsfinRun.picked || 0,
    ok: tsfinRun.ok || 0,
    fail: tsfinRun.fail || 0,
    auto_assigned_timesheets: autoAssignedTimesheets.length
  }
}));

  } catch (e) {
    console.error('handleCreateCandidate failed', e);
    return withCORS(env, req, serverError("Failed to create candidate"));
  }
}