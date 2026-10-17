async function runTsfinWorkerOnce(env, { limit = 50, onlyTimesheetIds = null, forceUnprocessedTimesheetIds = null } = {}) {
  // Local helpers used by both branches
  const round2 = (n) => Math.round((Number(n) || 0) * 100) / 100;
  const asNumberLocal = (v) => (v == null ? 0 : Number(v) || 0);
  const enc = encodeURIComponent;

  const chunk = (arr, n) => {
    const out = [];
    for (let i = 0; i < (arr?.length || 0); i += n) out.push(arr.slice(i, i + n));
    return out;
  };

  // Helper: derive Mon–Sun week start from a week-ending date (ymd)
  const computeWeekStartFromWeekEnding = (weYmd) => {
    if (!weYmd) return null;
    const d = new Date(`${weYmd}T00:00:00Z`);
    if (Number.isNaN(d.getTime())) return weYmd;
    d.setUTCDate(d.getUTCDate() - 6);
    const yyyy = d.getUTCFullYear();
    const mm   = String(d.getUTCMonth() + 1).padStart(2, '0');
    const dd   = String(d.getUTCDate()).padStart(2, '0');
    return `${yyyy}-${mm}-${dd}`;
  };

  // ─────────────────────────────────────────────────────────────
  // ✅ NEW: No-op TSFIN write guard (1A)
  // If the computed snapshot is identical to the current TSFIN row (for all snapshot keys),
  // DO NOT write a new timesheets_financials row; just mark outbox success.
  // ─────────────────────────────────────────────────────────────
  const _isPlainObj = (v) => !!(v && typeof v === 'object' && !Array.isArray(v) && !(v instanceof Date));

  const _stableCanonical = (v) => {
    if (v === undefined) return null;
    if (v === null) return null;

    if (v instanceof Date) return v.toISOString();

    if (Array.isArray(v)) return v.map(_stableCanonical);

    if (_isPlainObj(v)) {
      const out = {};
      const keys = Object.keys(v).sort();
      for (const k of keys) out[k] = _stableCanonical(v[k]);
      return out;
    }

    // keep primitives as-is
    return v;
  };

  const _stableStringify = (v) => {
    try { return JSON.stringify(_stableCanonical(v)); } catch { return String(v); }
  };

  const _tryParseJson = (s) => {
    if (typeof s !== 'string') return null;
    const t = s.trim();
    if (!t) return null;
    if (!(t.startsWith('{') || t.startsWith('['))) return null;
    try { return JSON.parse(t); } catch { return null; }
  };

  const _coerceLike = (template, actual) => {
    if (template === undefined) return null;

    if (template === null) return (actual == null ? null : actual);

    // Objects/arrays: try to parse JSON strings from DB
    if (Array.isArray(template) || _isPlainObj(template)) {
      if (typeof actual === 'string') {
        const parsed = _tryParseJson(actual);
        return parsed != null ? parsed : actual;
      }
      return actual == null ? null : actual;
    }

    // Numbers: accept numeric strings
    if (typeof template === 'number') {
      if (actual == null) return null;
      const n = (typeof actual === 'number') ? actual : Number(actual);
      return Number.isFinite(n) ? n : null;
    }

    // Booleans: accept string/boolish forms
    if (typeof template === 'boolean') {
      if (actual === true || actual === false) return actual;
      if (actual == null) return false;
      const s = String(actual).trim().toLowerCase();
      return (s === 'true' || s === '1' || s === 'yes' || s === 'y' || s === 'on');
    }

    // Strings: stringify
    if (typeof template === 'string') {
      return (actual == null) ? '' : String(actual);
    }

    // Fallback
    return actual == null ? null : actual;
  };

  const _isSnapshotNoop = (snapshot, curFin) => {
    if (!snapshot || typeof snapshot !== 'object') return false;
    if (!curFin || typeof curFin !== 'object') return false;

    const keys = Object.keys(snapshot);
    if (!keys.length) return false;

    const a = {};
    const b = {};

    for (const k of keys) {
      a[k] = snapshot[k];
      b[k] = _coerceLike(snapshot[k], curFin[k]);
    }

    return _stableStringify(a) === _stableStringify(b);
  };

  // ─────────────────────────────────────────────────────────────
  // Weekly helpers (contract-driven)
  // ─────────────────────────────────────────────────────────────

  // ✅ Updated: accepts policyOverride so it never triggers REST in resolveBucketsFromSchedule
  async function computeWeeklyHours(env, ts, contract, policyOverride = null) {
    const zeroHours = { day: 0, night: 0, sat: 0, sun: 0, bh: 0 };

    if (Array.isArray(ts.actual_schedule_json) && ts.actual_schedule_json.length) {
      const minsByBucket = await resolveBucketsFromSchedule(env, contract, ts.actual_schedule_json, policyOverride);
      const hours = {
        day: +(asNumberLocal(minsByBucket.day) / 60).toFixed(2),
        night: +(asNumberLocal(minsByBucket.night) / 60).toFixed(2),
        sat: +(asNumberLocal(minsByBucket.sat) / 60).toFixed(2),
        sun: +(asNumberLocal(minsByBucket.sun) / 60).toFixed(2),
        bh: +(asNumberLocal(minsByBucket.bh) / 60).toFixed(2),
      };
      return hours;
    }

    const scope = String(ts.sheet_scope || "").toUpperCase();
    if (scope === "WEEKLY") return zeroHours;

    const n = (v) => (v == null ? 0 : Number(v) || 0);
    return {
      day: n(ts.hours_day),
      night: n(ts.hours_night),
      sat: n(ts.hours_sat),
      sun: n(ts.hours_sun),
      bh: n(ts.hours_bh),
    };
  }

  // ─────────────────────────────────────────────────────────────
  // DAILY helpers
  // ─────────────────────────────────────────────────────────────
  const boolish = (v) => {
    if (v === true) return true;
    if (v === false) return false;
    if (v == null) return false;
    const s = String(v).trim().toLowerCase();
    return (s === 'true' || s === '1' || s === 'yes' || s === 'y' || s === 'on');
  };

  const norm = (s) => String(s || "").trim();
  const normRole = (s) => norm(s).replace(/\s+/g, " ").trim().toUpperCase();
  const normBand = (v) => {
    const s = norm(v);
    if (!s) return null;
    let m = s.match(/^\s*band\s*([0-9]+)\s*$/i);
    if (!m) m = s.match(/^\s*b\s*([0-9]+)\s*$/i);
    if (!m) m = s.match(/^\s*([0-9]+)\s*$/);
    if (m && m[1]) return `Band ${parseInt(m[1], 10)}`;
    return s;
  };

  const parseCandidateRoleCodes = (rolesVal) => {
    try {
      let roles = rolesVal;
      if (typeof roles === "string") roles = JSON.parse(roles);
      if (!Array.isArray(roles)) return [];
      return roles
        .map((r) => (r && typeof r === "object" ? r.code : r))
        .map((c) => normRole(c))
        .filter(Boolean);
    } catch {
      return [];
    }
  };

  const anyMissingRatesLocal = (usedBuckets, pay, charge) => {
    const u = usedBuckets || {};
    const p = pay || {};
    const c = charge || {};
    const used = (x) => (Number(x || 0) > 0);
    return (
      (used(u.day)   && (p.day   == null || c.day   == null)) ||
      (used(u.night) && (p.night == null || c.night == null)) ||
      (used(u.sat)   && (p.sat   == null || c.sat   == null)) ||
      (used(u.sun)   && (p.sun   == null || c.sun   == null)) ||
      (used(u.bh)    && (p.bh    == null || c.bh    == null))
    );
  };

  // ─────────────────────────────────────────────────────────────
  // Worker core (SQL-driven dequeue + context + rate batch + batch write)
  // ─────────────────────────────────────────────────────────────

  // 1) Lease outbox rows
  let lease = [];
  if (Array.isArray(onlyTimesheetIds) && onlyTimesheetIds.length) {
    const rows = await rpcTsfinDequeueSpecific(env, { timesheetIds: onlyTimesheetIds, limit });
    lease = (rows || []).map(r => ({
      outbox_id: r.id,
      timesheet_id: r.timesheet_id,
      reason: r.reason
    }));
  } else {
    lease = await rpcTsfinDequeueBatchIds(env, { limit });
  }

  if (!Array.isArray(lease) || !lease.length) return { picked: 0, ok: 0, fail: 0, completed_timesheet_ids: [] };

  // ✅ NEW (logging only): map outbox_id -> reason for invoice_debug gating (no functional impact)
  const reasonByOutboxId = new Map();
  try {
    for (const it of (lease || [])) {
      const ob = it?.outbox_id != null ? String(it.outbox_id) : '';
      if (!ob) continue;
      const rs = it?.reason != null ? String(it.reason) : '';
      if (rs) reasonByOutboxId.set(ob, rs);
    }
  } catch {}

  // 2) Map original timesheet_id -> effective/current timesheet_id
  const origIds = [...new Set(lease.map(x => String(x.timesheet_id)).filter(Boolean))];

  let origToCurrent = new Map();
  try {
    const inIds = origIds.map(enc).join(',');
    const { rows: tsRows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/timesheets` +
        `?timesheet_id=in.(${inIds})` +
        `&select=timesheet_id,booking_id,is_current,version,updated_at,created_at`
    );

    const bookingIds = [...new Set((tsRows || []).map(r => r.booking_id).filter(Boolean))];
    const bookingToBest = new Map();

    if (bookingIds.length) {
      const inBookings = bookingIds.map(enc).join(',');
      const { rows: allRows } = await sbFetch(
        env,
        `${env.SUPABASE_URL}/rest/v1/timesheets` +
          `?booking_id=in.(${inBookings})` +
          `&select=booking_id,timesheet_id,is_current,version,updated_at,created_at`
      );

      const byBooking = new Map();
      for (const r of (allRows || [])) {
        const bid = r?.booking_id ? String(r.booking_id) : '';
        const tid = r?.timesheet_id ? String(r.timesheet_id) : '';
        if (!bid || !tid) continue;
        if (!byBooking.has(bid)) byBooking.set(bid, []);
        byBooking.get(bid).push(r);
      }

      for (const [bid, arr] of byBooking.entries()) {
        const rows = Array.isArray(arr) ? arr.slice() : [];
        const currents = rows.filter(x => x && x.is_current === true);
        const pool = currents.length ? currents : rows;

        pool.sort((a, b) => {
          const va = Number(a?.version || 0) || 0;
          const vb = Number(b?.version || 0) || 0;
          if (vb !== va) return vb - va;

          const ua = a?.updated_at ? new Date(a.updated_at).getTime() : 0;
          const ub = b?.updated_at ? new Date(b.updated_at).getTime() : 0;
          if (ub !== ua) return ub - ua;

          const ca = a?.created_at ? new Date(a.created_at).getTime() : 0;
          const cb = b?.created_at ? new Date(b.created_at).getTime() : 0;
          return cb - ca;
        });

        const best = pool[0];
        if (best?.timesheet_id) bookingToBest.set(String(bid), String(best.timesheet_id));
      }
    }

    for (const r of (tsRows || [])) {
      const tid = String(r?.timesheet_id || '');
      if (!tid) continue;
      const bid = r?.booking_id ? String(r.booking_id) : '';
      const best = (bid && bookingToBest.get(bid)) ? bookingToBest.get(bid) : tid;
      origToCurrent.set(tid, best);
    }
  } catch {
    for (const id of origIds) origToCurrent.set(String(id), String(id));
  }

  // 3) Group outbox rows by effective timesheet_id (dedupe)
  const effToOutboxIds = new Map();
  const effToPrimary = new Map();
  const effToOriginalIds = new Map();
  const effIds = [];

  for (const it of lease) {
    const orig = String(it.timesheet_id || '');
    const eff = origToCurrent.get(orig) || orig;
    if (!eff) continue;

    if (!effToOutboxIds.has(eff)) effToOutboxIds.set(eff, []);
    effToOutboxIds.get(eff).push(it.outbox_id);
    if (!effToOriginalIds.has(eff)) effToOriginalIds.set(eff, new Set());
    effToOriginalIds.get(eff).add(orig);

    if (!effToPrimary.has(eff)) {
      effToPrimary.set(eff, it.outbox_id);
      effIds.push(eff);
    }
  }

  // 4) Load batch context for effective IDs (one RPC)
  const ctxRows2 = await rpcTsfinLoadContextBatch(env, { timesheetIds: effIds });
  const ctxByEff = new Map();
  for (const r of (ctxRows2 || [])) {
    const eid = String(r?.effective_timesheet_id || '');
    if (!eid) continue;
    ctxByEff.set(eid, r);
  }

  let ok = 0, fail = 0;
  const completedTimesheetIds = new Set();
  const completedPrimaryOutboxSet = new Set();
  const markTimesheetComplete = (effectiveId) => {
    const eff = String(effectiveId || '').trim().toLowerCase();
    if (!eff) return;
    const primary = effToPrimary.get(effectiveId) || effToPrimary.get(String(effectiveId));
    if (primary) completedPrimaryOutboxSet.add(String(primary).toLowerCase());
    completedTimesheetIds.add(eff);
    for (const originalId of (effToOriginalIds.get(effectiveId) || effToOriginalIds.get(String(effectiveId)) || [])) {
      const original = String(originalId || '').trim().toLowerCase();
      if (original) completedTimesheetIds.add(original);
    }
  };

  const preFailPrimaryOutboxSet = new Set();
  const weeklyWork = [];
  const dailyItemsForRates = [];
  const dailyWork = [];

  // 5) First pass: collect WEEKLY + DAILY work
  for (const effId of effIds) {
    const outboxIds = effToOutboxIds.get(effId) || [];
    const primaryOutboxId = effToPrimary.get(effId) || outboxIds[0] || null;

    const ctx = ctxByEff.get(effId) || null;
    if (!ctx) {
      let contextCleanupFailed = false;
      for (const ob of outboxIds) {
        try {
          await sbRpcRecordingGuardRefusal(env, "tsfin_mark_revoked", { p_timesheet_id: effId });
          await sbRpc(env, "tsfin_work_success", { p_id: ob });
          ok++;
        } catch (e) {
          contextCleanupFailed = true;
          await sbRpc(env, "tsfin_work_fail", { p_id: ob, p_error: String(e?.message || e) });
          fail++;
        }
      }
      if (!contextCleanupFailed) markTimesheetComplete(effId);
      continue;
    }

    const ts = ctx.out_timesheet || null;
    const curFin = ctx.out_cur_fin || null;
    const effFlags = ctx.out_effective_flags || null;

    const isLocked = !!(curFin?.locked_by_invoice_id || curFin?.paid_at_utc);
    if (isLocked) {
      let lockedCleanupFailed = false;
      for (const ob of outboxIds) {
        try { await sbRpc(env, "tsfin_work_success", { p_id: ob }); ok++; } catch { lockedCleanupFailed = true; fail++; }
      }
      if (!lockedCleanupFailed) markTimesheetComplete(effId);
      continue;
    }

    const scope = String(ts?.sheet_scope || '').toUpperCase();

    if (scope === 'WEEKLY') {
      try {
        const basis = String(curFin?.basis || '').toUpperCase();
        const routeType = String(effFlags?.route_type || '').toUpperCase();

        const isNhspRoute =
          (routeType === 'WEEKLY_NHSP' || routeType === 'WEEKLY_NHSP_ADJUSTMENT' || boolish(effFlags?.client_is_nhsp));

        const isHrRoute =
          (routeType === 'WEEKLY_HEALTHROSTER' || boolish(effFlags?.client_autoprocess_hr));

        const isNhspBasis = isNhspRoute || (basis === 'NHSP' || basis === 'NHSP_ADJUSTMENT');
        const isHrBasis = isHrRoute || (
          basis === 'HEALTHROSTER_SELF_BILL' ||
          basis === 'HEALTHROSTER_SELF_BILL_ADJUSTMENT' ||
          basis === 'HEALTHROSTER_ADJUSTMENT' ||
          basis === 'HEALTHROSTER_WEEKLY' ||
          basis === 'HEALTHROSTER_WEEKLY_ADJUSTMENT'
        );

        weeklyWork.push({
          effId: String(effId),
          outbox_id: primaryOutboxId,
          extra_outbox_ids: outboxIds.filter(x => x !== primaryOutboxId),
          ts,
          curFin,
          effFlags,
          policySql: (ctx.out_policy || null),
          routeType,
          basis,
          isNhspRoute,
          isHrRoute,
          isNhspBasis,
          isHrBasis,
          cw: null,
          contract: null,
        });
      } catch (e) {
        for (const ob of outboxIds) {
          await sbRpc(env, "tsfin_work_fail", { p_id: ob, p_error: String(e?.message || e) });
          fail++;
        }
      }
      continue;
    }

    // DAILY / ROTA batch path. Identity/rate-request derivation is owned by
    // the same canonical DAILY module used by office and Candidate callers.
    try {
      const dailyContext = deriveCanonicalDailyFinancialContext({
        effectiveTimesheetId: effId,
        context: ctx,
        toLocalParts,
        parseCandidateRoleCodes,
        normaliseRole: normRole,
        normaliseBand: normBand
      });

      dailyItemsForRates.push(dailyContext.rateRequest);

      dailyWork.push({
        effId: String(effId),
        outbox_id: primaryOutboxId,
        extra_outbox_ids: outboxIds.filter(x => x !== primaryOutboxId),
        ts,
        curFin, // ✅ allow no-op compare for daily too
        ctx,
        policy: dailyContext.policy,
        candidate: dailyContext.candidate,
        candidate_id: dailyContext.candidateId,
        candidate_assignment: dailyContext.candidateAssignment,
        client_id: dailyContext.clientId,
        workedDateYmd: dailyContext.workedDateYmd,
        roleForRates: dailyContext.roleForRates,
        bandForRates: dailyContext.bandForRates,
        rate_type: dailyContext.rateType,
        isAuthorised: !!ts?.authorised_at_server,
        isCorrectionTs: dailyContext.isCorrectionTimesheet,
      });
    } catch (e) {
      if (primaryOutboxId) preFailPrimaryOutboxSet.add(String(primaryOutboxId));
      for (const ob of outboxIds) {
        await sbRpc(env, "tsfin_work_fail", { p_id: ob, p_error: String(e?.message || e) });
        fail++;
      }
    }
  }

  // WEEKLY: batch-load contract_week + contract in ONE RPC
  try {
    const wkIds = [...new Set(weeklyWork.map(w => String(w?.ts?.timesheet_id || '')).filter(Boolean))];

    if (wkIds.length) {
      const wkRows = await sbRpc(env, 'tsfin_load_weekly_context_batch', { p_timesheet_ids: wkIds });

      const wkByTsId = new Map();
      for (const r of (wkRows || [])) {
        const tid = r?.timesheet_id ? String(r.timesheet_id) : '';
        if (!tid) continue;
        wkByTsId.set(tid, r);
      }

      for (const w of weeklyWork) {
        const tid = String(w?.ts?.timesheet_id || '');
        const row = tid ? (wkByTsId.get(tid) || null) : null;
        w.cw = row?.out_cw || null;
        w.contract = row?.out_contract || null;
      }
    }
  } catch (e) {
    try { console.warn('[TSFIN][WEEKLY_CTX_BATCH] failed', { err: e?.message || String(e) }); } catch {}
  }

  // WEEKLY: batch-load nhsp_shifts ONCE for all weekly timesheets
  let shiftsByTsId = new Map();
  try {
    const weeklyTsIds = [...new Set(
      weeklyWork.map(w => w?.ts?.timesheet_id).filter(Boolean).map(x => String(x))
    )];

    if (weeklyTsIds.length) {
      const rows = await sbRpc(env, 'tsfin_load_nhsp_shifts_batch', { p_timesheet_ids: weeklyTsIds });
      for (const r of (rows || [])) {
        const tid = r?.timesheet_id ? String(r.timesheet_id) : null;
        if (!tid) continue;
        let arr = r?.shifts;
        if (typeof arr === 'string') {
          try { arr = JSON.parse(arr); } catch { arr = []; }
        }
        if (!Array.isArray(arr)) arr = [];
        shiftsByTsId.set(tid, arr);
      }
    }
  } catch {
    shiftsByTsId = new Map();
  }

  // ✅ NEW: WEEKLY batch-load timesheet_validations so we only apply import refs once HR validation is OK
  let weeklyValidationByTsId = new Map();
  try {
    const weeklyTsIds = [...new Set(
      weeklyWork.map(w => w?.ts?.timesheet_id).filter(Boolean).map(x => String(x))
    )];

    if (weeklyTsIds.length) {
      const inTs = weeklyTsIds.map(enc).join(',');
      const { rows: vRows } = await sbFetch(
        env,
        `${env.SUPABASE_URL}/rest/v1/timesheet_validations` +
          `?timesheet_id=in.(${inTs})` +
          `&select=timesheet_id,status,reason_code,validated_at_utc,updated_at`
      );

      for (const vr of (vRows || [])) {
        const tid = vr?.timesheet_id ? String(vr.timesheet_id) : '';
        if (!tid) continue;
        weeklyValidationByTsId.set(tid, {
          status: vr?.status != null ? String(vr.status) : null,
          reason_code: vr?.reason_code != null ? String(vr.reason_code) : null,
          validated_at_utc: vr?.validated_at_utc ?? null,
          updated_at: vr?.updated_at ?? null
        });
      }
    }
  } catch {
    weeklyValidationByTsId = new Map();
  }

  // 6) Resolve rates in one RPC for all DAILY items
  const ratesRows = await rpcTsfinResolveRatesBatch(env, { items: dailyItemsForRates });
  const ratesByK = new Map();
  for (const r of (ratesRows || [])) {
    const k = String(r?.k || '');
    if (!k) continue;
    ratesByK.set(k, r);
  }

  // 7) Build WEEKLY + DAILY snapshots, then batch-write ONCE
  const rowsToWriteAll = [];

  // --- WEEKLY snapshots ---
  for (const w of weeklyWork) {
    const {
      outbox_id,
      extra_outbox_ids,
      ts,
      curFin,
      effFlags,
      cw,
      contract,
      routeType,
      basis,
      isNhspRoute,
      isHrRoute,
      isNhspBasis,
      isHrBasis,
      policySql
    } = w;

    if (!outbox_id) {
      for (const ob of [outbox_id, ...(extra_outbox_ids || [])].filter(Boolean)) {
        await sbRpc(env, "tsfin_work_fail", { p_id: ob, p_error: "NO_OUTBOX_ID" });
        fail++;
      }
      continue;
    }

    if (!cw || !contract) {
      const reason = 'WEEKLY_CTX_MISSING_CW_OR_CONTRACT';
      for (const ob of [outbox_id, ...(extra_outbox_ids || [])].filter(Boolean)) {
        await sbRpc(env, "tsfin_work_fail", { p_id: ob, p_error: reason });
        fail++;
      }
      continue;
    }

    try {
      const tsid = ts?.timesheet_id ? String(ts.timesheet_id) : null;
      const preloadedShifts = tsid ? (shiftsByTsId.get(tsid) || []) : [];
      const hasImportedShifts = Array.isArray(preloadedShifts) && preloadedShifts.length > 0;

      const val = tsid ? (weeklyValidationByTsId.get(tsid) || null) : null;

      const weeklyOptions = {
        policy_override: policySql || null,
        hr_eff_flags: effFlags || null,
        hr_preloaded_shifts: preloadedShifts,

        // ✅ NEW: validation state (so ref_num can be overridden ONLY after validation OK)
        hr_validation_status: val?.status || null,
        hr_validation_reason_code: val?.reason_code || null,

        outbox_id,
        write_now: false
      };

      // ✅ FIX (correction model):
      const corrKindU = String(ts?.correction_kind || '').trim().toUpperCase();
      const adjOriginU = String(ts?.adjustment_origin || '').trim().toUpperCase();
      const isCorrectionTs =
        !!corrKindU ||
        adjOriginU === 'IMPORT_CORRECTION' ||
        adjOriginU === 'IMPORT_CANCELLATION';

      const curIbForRouting = curFin?.invoice_breakdown_json || null;
      const curIbModeForRouting = String(curIbForRouting?.mode || '').trim().toUpperCase();
      const curIbSegmentsForRouting = Array.isArray(curIbForRouting?.segments) ? curIbForRouting.segments : [];
      const hasExistingInvoiceSegmentsForRouting = (curIbModeForRouting === 'SEGMENTS' && curIbSegmentsForRouting.length > 0);

      const tsActualScheduleForRouting = Array.isArray(ts?.actual_schedule_json) ? ts.actual_schedule_json : [];
      const curActualScheduleForRouting = Array.isArray(curFin?.actual_schedule_json) ? curFin.actual_schedule_json : [];
      const hasActualScheduleForRouting = (tsActualScheduleForRouting.length > 0 || curActualScheduleForRouting.length > 0);

      const routeTypeUForRouting = String(routeType || '').trim().toUpperCase();
      const basisUForRouting = String(basis || '').trim().toUpperCase();
      const submissionModeUForRouting = String(ts?.submission_mode || cw?.submission_mode_snapshot || '').trim().toUpperCase();

      const isManualSubmissionForRouting = (submissionModeUForRouting === 'MANUAL');
      const isAdjustmentForRouting = (
        boolish(ts?.is_adjustment) ||
        boolish(cw?.is_adjustment) ||
        routeTypeUForRouting === 'WEEKLY_NHSP_ADJUSTMENT' ||
        basisUForRouting === 'NHSP_ADJUSTMENT'
      );

      const expensePayForRoutingRaw = asNumberLocal(curFin?.expenses_pay_ex_vat);
      const expenseChargeForRoutingRaw = asNumberLocal(curFin?.expenses_charge_ex_vat);
      const categoryPayForRoutingRaw =
        asNumberLocal(curFin?.travel_pay_ex_vat) +
        asNumberLocal(curFin?.accommodation_pay_ex_vat) +
        asNumberLocal(curFin?.other_pay_ex_vat);
      const categoryChargeForRoutingRaw =
        asNumberLocal(curFin?.travel_charge_ex_vat) +
        asNumberLocal(curFin?.accommodation_charge_ex_vat) +
        asNumberLocal(curFin?.other_charge_ex_vat);

      const expensePayForRouting = Math.abs(expensePayForRoutingRaw) > 0.009 ? expensePayForRoutingRaw : categoryPayForRoutingRaw;
      const expenseChargeForRouting = Math.abs(expenseChargeForRoutingRaw) > 0.009 ? expenseChargeForRoutingRaw : categoryChargeForRoutingRaw;

      const mileagePayForRouting = asNumberLocal(curFin?.mileage_pay_ex_vat);
      const mileageChargeForRouting = asNumberLocal(curFin?.mileage_charge_ex_vat);
      const additionalPayForRouting = asNumberLocal(curFin?.additional_pay_ex_vat);
      const additionalChargeForRouting = asNumberLocal(curFin?.additional_charge_ex_vat);

      const nonSegmentPayForRouting = round2(expensePayForRouting + mileagePayForRouting + additionalPayForRouting);
      const nonSegmentChargeForRouting = round2(expenseChargeForRouting + mileageChargeForRouting + additionalChargeForRouting);
      const hasNonSegmentFinancialsForRouting = (
        Math.abs(nonSegmentPayForRouting) > 0.009 ||
        Math.abs(nonSegmentChargeForRouting) > 0.009
      );

      const totalPayForRouting = asNumberLocal(curFin?.total_pay_ex_vat);
      const totalChargeForRouting = asNumberLocal(curFin?.total_charge_ex_vat);
      const workedHoursForRouting = round2(
        asNumberLocal(curFin?.total_hours) +
        asNumberLocal(curFin?.hours_day) +
        asNumberLocal(curFin?.hours_night) +
        asNumberLocal(curFin?.hours_sat) +
        asNumberLocal(curFin?.hours_sun) +
        asNumberLocal(curFin?.hours_bh)
      );

      const residualSegmentPayForRouting = round2(totalPayForRouting - nonSegmentPayForRouting);
      const residualSegmentChargeForRouting = round2(totalChargeForRouting - nonSegmentChargeForRouting);
      const hasWorkedTimeFinancialsForRouting = (
        Math.abs(workedHoursForRouting) > 0.009 ||
        Math.abs(residualSegmentPayForRouting) > 0.009 ||
        Math.abs(residualSegmentChargeForRouting) > 0.009
      );

      const isManualNhspFinancialOnlyZeroSegmentRoute = (
        !isCorrectionTs &&
        isNhspBasis &&
        !isHrBasis &&
        !hasImportedShifts &&
        isManualSubmissionForRouting &&
        isAdjustmentForRouting &&
        !hasExistingInvoiceSegmentsForRouting &&
        !hasActualScheduleForRouting &&
        !hasWorkedTimeFinancialsForRouting &&
        hasNonSegmentFinancialsForRouting
      );

      const preserveCurrentNonSegmentFinancialsForManualZeroSegmentRoute = (snapshot) => {
        if (!snapshot || typeof snapshot !== 'object' || !curFin || typeof curFin !== 'object') return snapshot;

        const expPay0 = round2(expensePayForRouting);
        const expChg0 = round2(expenseChargeForRouting);
        const milPay0 = round2(mileagePayForRouting);
        const milChg0 = round2(mileageChargeForRouting);
        const addPay0 = round2(additionalPayForRouting);
        const addChg0 = round2(additionalChargeForRouting);
        const addMar0 = round2(asNumberLocal(curFin?.additional_margin_ex_vat));
        const totalPay0 = round2(addPay0 + expPay0 + milPay0);
        const totalCharge0 = round2(addChg0 + expChg0 + milChg0);
        const margin0 = round2(totalCharge0 - totalPay0);

        snapshot.additional_units_json = (curFin.additional_units_json && typeof curFin.additional_units_json === 'object' && !Array.isArray(curFin.additional_units_json))
          ? curFin.additional_units_json
          : {};
        snapshot.additional_pay_ex_vat = addPay0;
        snapshot.additional_charge_ex_vat = addChg0;
        snapshot.additional_margin_ex_vat = addMar0;

        snapshot.expenses_pay_ex_vat = expPay0;
        snapshot.expenses_charge_ex_vat = expChg0;
        snapshot.expenses_description = curFin?.expenses_description ?? null;
        snapshot.expenses_evidence_r2_key = curFin?.expenses_evidence_r2_key ?? null;
        snapshot.expenses_evidence_manifest = curFin?.expenses_evidence_manifest ?? null;

        snapshot.travel_pay_ex_vat = round2(asNumberLocal(curFin?.travel_pay_ex_vat));
        snapshot.travel_charge_ex_vat = round2(asNumberLocal(curFin?.travel_charge_ex_vat));
        snapshot.accommodation_pay_ex_vat = round2(asNumberLocal(curFin?.accommodation_pay_ex_vat));
        snapshot.accommodation_charge_ex_vat = round2(asNumberLocal(curFin?.accommodation_charge_ex_vat));
        snapshot.other_pay_ex_vat = round2(asNumberLocal(curFin?.other_pay_ex_vat));
        snapshot.other_charge_ex_vat = round2(asNumberLocal(curFin?.other_charge_ex_vat));

        snapshot.mileage_units = round2(asNumberLocal(curFin?.mileage_units));
        snapshot.mileage_pay_ex_vat = milPay0;
        snapshot.mileage_charge_ex_vat = milChg0;
        snapshot.mileage_evidence_r2_key = curFin?.mileage_evidence_r2_key ?? null;
        snapshot.mileage_evidence_manifest = curFin?.mileage_evidence_manifest ?? null;
        snapshot.mileage_pay_rate = curFin?.mileage_pay_rate ?? null;
        snapshot.mileage_charge_rate = curFin?.mileage_charge_rate ?? null;

        snapshot.total_pay_ex_vat = totalPay0;
        snapshot.total_charge_ex_vat = totalCharge0;
        snapshot.margin_ex_vat = margin0;

        if (!snapshot.invoice_breakdown_json || typeof snapshot.invoice_breakdown_json !== 'object' || Array.isArray(snapshot.invoice_breakdown_json)) {
          snapshot.invoice_breakdown_json = { mode: 'SEGMENTS', segments: [] };
        }
        if (!Array.isArray(snapshot.invoice_breakdown_json.segments)) snapshot.invoice_breakdown_json.segments = [];
        snapshot.invoice_breakdown_json.mode = 'SEGMENTS';
        snapshot.invoice_breakdown_json.additional = {
          units: snapshot.additional_units_json || {},
          pay_ex_vat: addPay0,
          charge_ex_vat: addChg0,
          margin_ex_vat: addMar0
        };
        snapshot.invoice_breakdown_json.totals = {
          total_pay_ex_vat: totalPay0,
          total_charge_ex_vat: totalCharge0,
          margin_ex_vat: margin0
        };

        return snapshot;
      };

      let buildRes = null;

      if (isCorrectionTs) {
        buildRes = await buildWeeklyScheduleSegmentsSnapshot(
          env,
          ts,
          cw,
          contract,
          curFin,
          weeklyOptions
        );
      } else if (hasImportedShifts && (isNhspRoute || isHrRoute || isNhspBasis || isHrBasis)) {
        const routeU = String(routeType || '').toUpperCase();
        const basisU = String(basis || '').toUpperCase();

        const basisForBuild =
          (isHrRoute || isHrBasis)
            ? ((basisU && basisU.startsWith('HEALTHROSTER')) ? basisU : 'HEALTHROSTER_SELF_BILL')
            : ((routeU === 'WEEKLY_NHSP_ADJUSTMENT' || basisU === 'NHSP_ADJUSTMENT') ? 'NHSP_ADJUSTMENT' : 'NHSP');

        const getPolicyFn =
          (policySql && typeof policySql === 'object')
            ? (_clientId, _date) => policySql
            : (typeof getPolicyCached === 'function' ? getPolicyCached : async () => null);

        buildRes = await buildNhspWeeklySnapshotCached(
          env,
          ts,
          contract,
          preloadedShifts,
          curFin?.nhsp_import_id || null,
          basisForBuild,
          getPolicyFn,
          curFin,
          weeklyOptions
        );
      } else if (isManualNhspFinancialOnlyZeroSegmentRoute) {
        buildRes = await buildWeeklyScheduleSegmentsSnapshot(
          env,
          ts,
          cw,
          contract,
          curFin,
          weeklyOptions
        );
        if (buildRes && buildRes.ok === true && buildRes.snapshot) {
          buildRes.snapshot = preserveCurrentNonSegmentFinancialsForManualZeroSegmentRoute(buildRes.snapshot);
        }
      } else if (isNhspBasis) {
        buildRes = await rebuildFromExistingSegmentsEvidence(
          env,
          ts,
          cw,
          contract,
          curFin,
          weeklyOptions
        );
      } else if (isHrBasis) {
        buildRes = await rebuildFromExistingSegmentsEvidence(
          env,
          ts,
          cw,
          contract,
          curFin,
          weeklyOptions
        );
      } else {
        buildRes = await buildWeeklyScheduleSegmentsSnapshot(
          env,
          ts,
          cw,
          contract,
          curFin,
          weeklyOptions
        );
      }

      if (!buildRes || buildRes.ok !== true || !buildRes.snapshot) {
        const reason = buildRes?.reason ? String(buildRes.reason) : 'WEEKLY_BUILD_FAILED';
        for (const ob of [outbox_id, ...(extra_outbox_ids || [])].filter(Boolean)) {
          await sbRpc(env, "tsfin_work_fail", { p_id: ob, p_error: reason });
          fail++;
        }
        continue;
      }

      const snapshot = buildRes.snapshot;

      if (snapshot && snapshot.occupant_key_norm == null && ts?.occupant_key_norm != null) {
        snapshot.occupant_key_norm = ts.occupant_key_norm;
      }

      // ✅ NEW (logging only): invoice_debug-gated audit for SEGMENTS recompute attempts (no functional impact)
      try {
        const ibBefore = curFin?.invoice_breakdown_json || null;
        const modeBefore = String(ibBefore?.mode || '').toUpperCase();
        const segsBefore = Array.isArray(ibBefore?.segments) ? ibBefore.segments : null;

        const ibAfter = snapshot?.invoice_breakdown_json || null;
        const modeAfter = String(ibAfter?.mode || '').toUpperCase();
        const segsAfter = Array.isArray(ibAfter?.segments) ? ibAfter.segments : null;

        if (modeAfter === 'SEGMENTS' && Array.isArray(segsAfter)) {
          let lockedBefore = 0, unlockedBefore = 0;
          let lockedAfter = 0, unlockedAfter = 0;

          const oldUnlockedShiftIds = new Set();
          const newUnlockedShiftIds = new Set();

          if (modeBefore === 'SEGMENTS' && Array.isArray(segsBefore)) {
            for (const s of segsBefore) {
              const lockId = String(s?.invoice_locked_invoice_id || '').trim();
              const isLockedSeg = !!lockId;
              if (isLockedSeg) lockedBefore++; else unlockedBefore++;
              if (!isLockedSeg) {
                const sid = String(s?.nhsp_shift_id || '').trim();
                if (sid) oldUnlockedShiftIds.add(sid);
              }
            }
          }

          for (const s of segsAfter) {
            const lockId = String(s?.invoice_locked_invoice_id || '').trim();
            const isLockedSeg = !!lockId;
            if (isLockedSeg) lockedAfter++; else unlockedAfter++;
            if (!isLockedSeg) {
              const sid = String(s?.nhsp_shift_id || '').trim();
              if (sid) newUnlockedShiftIds.add(sid);
            }
          }

          const removed = [];
          for (const sid of oldUnlockedShiftIds) {
            if (!newUnlockedShiftIds.has(sid)) {
              removed.push(sid);
              if (removed.length >= 20) break;
            }
          }

          const staleU = String(curFin?.stale_reason || '').trim().toUpperCase();
          const reasonU = String(reasonByOutboxId.get(String(outbox_id)) || '').trim().toUpperCase();

          const shouldLog =
            (lockedBefore > 0 || lockedAfter > 0) ||
            (staleU === 'IMPORT_CANCEL_DETACH') ||
            (reasonU.includes('IMPORT_'));

          if (shouldLog) {
            await sbRpc(env, "_imp_debug_audit", {
              p_actor_user_id: null,
              p_action: "TSFIN_SEGMENTS_RECOMPUTE_DEBUG",
              p_after_json: {
                timesheet_id: String(ts?.timesheet_id || ''),
                outbox_id: String(outbox_id || ''),
                locked_before_count: lockedBefore,
                locked_after_count: lockedAfter,
                unlocked_before_count: unlockedBefore,
                unlocked_after_count: unlockedAfter,
                removed_unlocked_nhsp_shift_ids: removed,
                used_locked_merge: (lockedBefore > 0 || lockedAfter > 0)
              },
              p_entity: "timesheets_financials",
              p_subject_id: String(ts?.timesheet_id || ''),
              p_before_json: null,
              p_ip: null,
              p_user_agent: null,
              p_correlation_id: outbox_id ? `tsfin_recompute:${String(outbox_id)}` : null
            });
          }
        }
      } catch {}

      // Enrich HR cross-check BEFORE write (mutates snapshot)
      try {
        await enrichTsfinWithHrCrosscheck(env, ts, snapshot, effFlags || null, preloadedShifts);
      } catch {}

      // Self-heal locked segments (unchanged)
      try {
        const curIb0 = curFin?.invoice_breakdown_json || null;
        const curMode0 = String(curIb0?.mode || '').toUpperCase();
        const curSegs0 = Array.isArray(curIb0?.segments) ? curIb0.segments : null;

        let invalidSegCount0 = 0;
        let lockedSegCount0 = 0;

        if (curMode0 === 'SEGMENTS' && Array.isArray(curSegs0)) {
          for (const s0 of curSegs0) {
            const isObj0 = !!(s0 && typeof s0 === 'object' && !Array.isArray(s0));
            const segId0 = isObj0 ? String(s0.segment_id || '').trim() : '';
            if (!isObj0 || !segId0) {
              invalidSegCount0++;
              continue;
            }
            const lockId0 = String(s0.invoice_locked_invoice_id || '').trim();
            if (lockId0) lockedSegCount0++;
          }
        }

        const needsHeal0 = (invalidSegCount0 > 0 && lockedSegCount0 > 0);

        if (needsHeal0) {
          const newSegs0 = snapshot?.invoice_breakdown_json?.segments;
          const healRes0 = await rpcTsfinRepairMergeSegmentsLocked(env, {
            timesheetId: ts.timesheet_id,
            newSegments: Array.isArray(newSegs0) ? newSegs0 : [],
            actorUserId: null,
            correlationId: outbox_id ? `tsfin_selfheal:${String(outbox_id)}` : null
          });

          const okHeal0 =
            !!(healRes0 && typeof healRes0 === 'object' && healRes0.ok === true && Number(healRes0.new_invalid_count || 0) === 0);

          if (okHeal0) {
            await sbRpc(env, "tsfin_work_success", { p_id: outbox_id });
            ok++;
            markTimesheetComplete(effId);
            continue;
          } else {
            const errMsg0 =
              (healRes0 && typeof healRes0 === 'object' && healRes0.error) ? String(healRes0.error) :
              'TSFIN_SELFHEAL_MERGE_FAILED';

            for (const ob0 of [outbox_id, ...(extra_outbox_ids || [])].filter(Boolean)) {
              await sbRpc(env, "tsfin_work_fail", { p_id: ob0, p_error: errMsg0 });
              fail++;
            }
            preFailPrimaryOutboxSet.add(String(outbox_id));
            continue;
          }
        }
      } catch (e0) {
        const errMsg0 = `TSFIN_SELFHEAL_ERROR:${String(e0?.message || e0)}`;
        for (const ob0 of [outbox_id, ...(extra_outbox_ids || [])].filter(Boolean)) {
          await sbRpc(env, "tsfin_work_fail", { p_id: ob0, p_error: errMsg0 });
          fail++;
        }
        preFailPrimaryOutboxSet.add(String(outbox_id));
        continue;
      }

      // ✅ NEW: no-op write optimisation (skip DB write if identical)
      if (_isSnapshotNoop(snapshot, curFin) && !isCorrectionTs) {
        try {
          await sbRpc(env, "tsfin_work_success", { p_id: outbox_id });
          ok++;
          markTimesheetComplete(effId);
        } catch (e) {
          await sbRpc(env, "tsfin_work_fail", { p_id: outbox_id, p_error: String(e?.message || e) });
          fail++;
        }
        continue;
      }

      rowsToWriteAll.push({
        outbox_id,
        timesheet_id: ts.timesheet_id,
        snapshot,
        is_correction_ts: isCorrectionTs
      });
    } catch (e) {
      for (const ob of [outbox_id, ...(extra_outbox_ids || [])].filter(Boolean)) {
        await sbRpc(env, "tsfin_work_fail", { p_id: ob, p_error: String(e?.message || e) });
        fail++;
      }
    }
  }

  // --- DAILY snapshots: one canonical calculation body ---
  const forceUnprocessedDailyIds = new Set(
    (Array.isArray(forceUnprocessedTimesheetIds) ? forceUnprocessedTimesheetIds : [])
      .map((value) => String(value || '').trim().toLowerCase())
      .filter(Boolean)
  );

  for (const w of dailyWork) {
    const { effId, outbox_id, ts, policy, candidate, candidate_id, client_id, curFin, isCorrectionTs } = w;

    try {
      const ratesRow = ratesByK.get(String(effId)) || null;
      const preserveUnprocessed =
        forceUnprocessedDailyIds.has(String(effId || '').trim().toLowerCase()) ||
        String(curFin?.processing_status || '').trim().toUpperCase() === 'UNPROCESSED';

      const snapshot = buildCanonicalDailyFinancialSnapshot({
        env,
        timesheet: ts,
        currentFinancials: curFin,
        policy,
        candidate,
        umbrella: w.ctx?.out_umbrella || null,
        candidateId: candidate_id,
        candidateAssignment: w.candidate_assignment,
        clientId: client_id,
        role: w.roleForRates,
        band: w.bandForRates,
        rateType: w.rate_type,
        ratesRow,
        effectiveFlags: w.ctx?.out_effective_flags || null,
        preserveUnprocessed,
        subtractBreak,
        classifyMinutes,
        resolveEffectivePayChannel
      });

      if (_isSnapshotNoop(snapshot, curFin) && !isCorrectionTs) {
        try {
          await sbRpc(env, "tsfin_work_success", { p_id: outbox_id });
          ok++;
          markTimesheetComplete(effId);
        } catch (e) {
          preFailPrimaryOutboxSet.add(String(outbox_id));
          await sbRpc(env, "tsfin_work_fail", { p_id: outbox_id, p_error: String(e?.message || e) });
          fail++;
        }
        continue;
      }

      rowsToWriteAll.push({
        outbox_id,
        timesheet_id: ts.timesheet_id,
        snapshot,
        is_correction_ts: isCorrectionTs
      });
    } catch (e) {
      preFailPrimaryOutboxSet.add(String(w.outbox_id));
      const allObs = [w.outbox_id, ...(w.extra_outbox_ids || [])].filter(Boolean);
      for (const ob of allObs) {
        await sbRpc(env, "tsfin_work_fail", { p_id: ob, p_error: String(e?.message || e) });
        fail++;
      }
    }
  }
  // 8) One batch write for WEEKLY + DAILY
  let wr = null;
  let failOutboxSet = new Set();

  if (rowsToWriteAll.length) {
    wr = await rpcTsfinWriteSnapshotsAndComplete(env, { rows: rowsToWriteAll });

    ok += Number(wr?.ok_count || 0) || 0;
    fail += Number(wr?.fail_count || 0) || 0;

    // The existing helper validates native counts and exact failed input tuples.
    failOutboxSet = new Set(wr.errors.map(error => error.outbox_id.toLowerCase()));
    for (const row of rowsToWriteAll) {
      const outboxId = String(row?.outbox_id || '').toLowerCase();
      if (outboxId && !failOutboxSet.has(outboxId) && !preFailPrimaryOutboxSet.has(outboxId)) markTimesheetComplete(row.timesheet_id);
    }
  }

  // Bulk-clear duplicate outbox rows ONLY if primary succeeded
  const extrasToClear = [];

  for (const w of weeklyWork) {
    const p = String(w?.outbox_id || '');
    if (!p) continue;
    if (failOutboxSet.has(p)) continue;
    if (preFailPrimaryOutboxSet.has(p)) continue;
    if (!completedPrimaryOutboxSet.has(p.toLowerCase())) continue;
    for (const ob of (w.extra_outbox_ids || [])) {
      if (ob) extrasToClear.push(ob);
    }
  }

  for (const w of dailyWork) {
    const p = String(w?.outbox_id || '');
    if (!p) continue;
    if (failOutboxSet.has(p)) continue;
    if (preFailPrimaryOutboxSet.has(p)) continue;
    if (!completedPrimaryOutboxSet.has(p.toLowerCase())) continue;
    for (const ob of (w.extra_outbox_ids || [])) {
      if (ob) extrasToClear.push(ob);
    }
  }

  if (extrasToClear.length) {
    for (const idsChunk of chunk(extrasToClear, 200)) {
      try {
        const cleared = await sbRpc(env, 'tsfin_work_success_bulk', { p_ids: idsChunk });
        ok += Number(cleared || 0) || 0;
      } catch {
        // best-effort only
      }
    }
  }

  return { picked: lease.length, ok, fail, write: wr, completed_timesheet_ids: [...completedTimesheetIds].slice(0, 10000) };
}