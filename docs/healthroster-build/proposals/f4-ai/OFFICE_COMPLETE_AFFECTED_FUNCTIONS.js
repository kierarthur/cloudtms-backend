function invoiceModalComputePreviewTotals(modalCtx) {
  const invData = invoiceModalGetInvoiceData(modalCtx);
  const { invoice, header_snapshot_json, items } = invData;

  const vatRatePct = invoiceModalGetVatRatePct(invoice, header_snapshot_json);

  const removed = modalCtx?.invoiceEdits?.remove_invoice_line_ids || new Set();
  const stagedAddTs = modalCtx?.invoiceEdits?.add_timesheet_ids || new Set();
  const stagedAdjs = Array.isArray(modalCtx?.invoiceEdits?.add_adjustments) ? modalCtx.invoiceEdits.add_adjustments : [];

  const stagedRemoveSeg = Array.isArray(modalCtx?.invoiceEdits?.remove_segment_refs) ? modalCtx.invoiceEdits.remove_segment_refs : [];
  const stagedAddSeg    = Array.isArray(modalCtx?.invoiceEdits?.add_segment_refs) ? modalCtx.invoiceEdits.add_segment_refs : [];

  let subtotal_ex_vat = 0;
  let vat_amount = 0;
  let total_inc_vat = 0;

  // Base: sum all existing invoice lines that are not staged for removal
  for (const it of (items || [])) {
    const lineId = it?.invoice_line_id;
    if (lineId && removed.has(String(lineId))) continue;

    subtotal_ex_vat += Number(it?.total_charge_ex_vat || 0);
    vat_amount += Number(it?.vat_amount || 0);
    total_inc_vat += Number(it?.total_inc_vat || 0);
  }

  // Add staged adjustments (preview VAT at invoice rate)
  for (const a of stagedAdjs) {
    const ex = Number(a?.amount_ex_vat || 0);
    const source = invData.weekly_source_invoice;
    const helper = window.CloudTmsSourceInvoiceAdjustments;
    const vat = source?.is_weekly_source_invoice === true
      ? (Object.prototype.hasOwnProperty.call(a,'vat_amount') ? Number(a.vat_amount)
        : Number(helper.defaultVat(a.amount_ex_vat,helper.context(source).default_vat_rate_pct)))
      : ex * (vatRatePct / 100);
    subtotal_ex_vat += ex;
    vat_amount += vat;
    total_inc_vat += (ex + vat);
  }

  // Add staged timesheets using eligible-timesheets cache (segment-aware totals as provided by RPC)
  if (stagedAddTs.size > 0) {
    const cache = modalCtx?.eligibleTimesheetsCache;
    const arr = Array.isArray(cache?.timesheets) ? cache.timesheets : [];
    const byId = new Map(arr.map(t => [String(t?.timesheet_id || ''), t]));

    for (const tsId of stagedAddTs) {
      const t = byId.get(String(tsId));
      if (!t) continue;

      const ex = Number(t?.invoiceable_charge_ex_vat || 0);
      const vat = ex * (vatRatePct / 100);

      subtotal_ex_vat += ex;
      vat_amount += vat;
      total_inc_vat += (ex + vat);
    }
  }

  // ─────────────────────────────────────────────────────────────
  // Segment staging adjustments (remove/add segments)
  // ─────────────────────────────────────────────────────────────
  const key = (tsfinId, segId) => `${String(tsfinId || '')}::${String(segId || '')}`;

  // Build lookup for segments currently on this invoice (manifest)
  const segMap =
    (invData?.segments_on_invoice_by_timesheet && typeof invData.segments_on_invoice_by_timesheet === 'object')
      ? invData.segments_on_invoice_by_timesheet
      : ((invData?.segments_by_timesheet && typeof invData.segments_by_timesheet === 'object')
          ? invData.segments_by_timesheet
          : {});

  const tsfinMap =
    (invData?.tsfin_id_by_timesheet_id && typeof invData.tsfin_id_by_timesheet_id === 'object')
      ? invData.tsfin_id_by_timesheet_id
      : {};

  const invoicedSegByKey = new Map();
  try {
    for (const tsId of Object.keys(segMap || {})) {
      const info = segMap[tsId];
      if (!info || typeof info !== 'object') continue;

      const tsfinId = (info.tsfin_id != null && String(info.tsfin_id).trim())
        ? String(info.tsfin_id)
        : (tsfinMap[tsId] != null ? String(tsfinMap[tsId]) : '');

      const segs = Array.isArray(info.invoiced_segments) ? info.invoiced_segments : [];
      if (!tsfinId || !segs.length) continue;

      for (const s of segs) {
        if (!s || typeof s !== 'object') continue;
        const segId = (s.segment_id != null && String(s.segment_id).trim()) ? String(s.segment_id) : '';
        if (!segId) continue;

        const chg = Number(
          (s.charge_amount != null ? s.charge_amount : (s['charge_amount'] != null ? s['charge_amount'] : 0))
        ) || 0;

        invoicedSegByKey.set(key(tsfinId, segId), { charge_amount: chg });
      }
    }
  } catch {}

  // Build lookup for eligible segments (eligible-timesheets cache)
  const eligibleSegByKey = new Map();
  try {
    const cache = modalCtx?.eligibleTimesheetsCache;
    const arr = Array.isArray(cache?.timesheets) ? cache.timesheets : [];
    for (const t of arr) {
      if (!t || typeof t !== 'object') continue;
      const tsfinId = (t.tsfin_id != null && String(t.tsfin_id).trim()) ? String(t.tsfin_id) : '';
      if (!tsfinId) continue;

      const segs = Array.isArray(t.eligible_segments) ? t.eligible_segments : [];
      for (const s of segs) {
        if (!s || typeof s !== 'object') continue;
        const segId = (s.segment_id != null && String(s.segment_id).trim()) ? String(s.segment_id) : '';
        if (!segId) continue;

        const chg = Number(s.charge_amount || 0) || 0;
        eligibleSegByKey.set(key(tsfinId, segId), { charge_amount: chg });
      }
    }
  } catch {}

  const applyDelta = (chargeEx, sign) => {
    const ex = Number(chargeEx || 0);
    if (!Number.isFinite(ex) || ex === 0) return;

    const vat = ex * (vatRatePct / 100);
    subtotal_ex_vat += sign * ex;
    vat_amount += sign * vat;
    total_inc_vat += sign * (ex + vat);
  };

  // Remove segments staged for removal (subtract their charge)
  for (const r of stagedRemoveSeg) {
    if (!r || typeof r !== 'object') continue;
    const k = key(r.tsfin_id, r.segment_id);
    const seg = invoicedSegByKey.get(k);
    if (!seg) continue;
    applyDelta(seg.charge_amount, -1);
  }

  // Add segments staged for add (add their charge)
  for (const r of stagedAddSeg) {
    if (!r || typeof r !== 'object') continue;
    const k = key(r.tsfin_id, r.segment_id);
    const seg = eligibleSegByKey.get(k);
    if (!seg) continue;
    applyDelta(seg.charge_amount, +1);
  }

  return {
    subtotal_ex_vat: invoiceModalRound2(subtotal_ex_vat),
    vat_amount: invoiceModalRound2(vat_amount),
    total_inc_vat: invoiceModalRound2(total_inc_vat)
  };
}


async function invoiceModalSaveEdits(modalCtx, { rerender, reload }) {
  // ✅ Single Save pipeline:
  //   1) hold/unhold (if staged)
  //   2) issue/unissue (if staged)
  //   3) paid/unpaid (if staged)
  //   4) apply edits (/save-edits) (lines + segments + references)
  //   5) reload + clear staging
  //   6) refresh the summary sheet behind the modal (so totals/columns update immediately)

  const invoiceId = modalCtx?.invoiceId ? String(modalCtx.invoiceId) : '';
  if (!invoiceId) throw new Error('Invoice id missing.');

  // Prefer the loaded invoice payload (openInvoiceModal stores it on dataLoaded)
  const inv =
    modalCtx?.dataLoaded?.invoice ||
    modalCtx?.dataLoaded?.invoice_row ||
    modalCtx?.data?.invoice ||
    null;

  if (!inv || typeof inv !== 'object') {
    throw new Error('Invoice not loaded.');
  }

  const type = String(inv.type || '').toUpperCase();
  if (type === 'CREDIT_NOTE') throw new Error('Credit notes cannot be edited.');

  // Current state
  const curStatus = String(inv.status || '').toUpperCase();
  const curIsHold = (curStatus === 'ON_HOLD');

  // Treat issued as either status ISSUED or issued_at_utc present (defensive)
  const curIsIssued = (curStatus === 'ISSUED') || !!inv.issued_at_utc;
  const curIsPaid = !!inv.paid_at_utc;

  // Staged status toggles
  const stagedStatus = (modalCtx?.invoiceEdits && typeof modalCtx.invoiceEdits === 'object' && modalCtx.invoiceEdits.staged_status && typeof modalCtx.invoiceEdits.staged_status === 'object')
    ? modalCtx.invoiceEdits.staged_status
    : {};

  // Optional legacy reason payload (existing codepath)
  const stagedReason = (modalCtx?.invoiceEdits && typeof modalCtx.invoiceEdits === 'object' && modalCtx.invoiceEdits.staged_reason && typeof modalCtx.invoiceEdits.staged_reason === 'object')
    ? modalCtx.invoiceEdits.staged_reason
    : {};

  const wantHold   = (Object.prototype.hasOwnProperty.call(stagedStatus, 'on_hold') && stagedStatus.on_hold != null) ? !!stagedStatus.on_hold : null;
  const wantIssued = (Object.prototype.hasOwnProperty.call(stagedStatus, 'issued')  && stagedStatus.issued  != null) ? !!stagedStatus.issued  : null;
  const wantPaid   = (Object.prototype.hasOwnProperty.call(stagedStatus, 'paid')    && stagedStatus.paid    != null) ? !!stagedStatus.paid    : null;

  const toast = (msg) => {
    try {
      if (typeof window.toast === 'function') return window.toast(msg);
      if (typeof window.showToast === 'function') return window.showToast(msg);
      if (typeof window.notify === 'function') return window.notify(msg);
      if (typeof window.__toast === 'function') return window.__toast(msg);
      console.log('[INV][TOAST]', msg);
    } catch {}
  };
  // Styled modal prompt for post-issue email delivery.
  // Returns true if user clicks "Yes, send now", otherwise false.
  const promptSendInvoiceEmailNow = async ({ toEmail } = {}) => {
    const to = (typeof toEmail === 'string' && toEmail.trim()) ? toEmail.trim() : '';

    const title = 'Send invoice by email now?';

    const renderTab = () => {
      const recipientLine = to ? `<div class="mini" style="margin-top:6px;"><b>Recipient:</b> ${escapeHtml(to)}</div>` : '';
      return `
        <div style="padding: 6px 2px;">
          <div style="font-size:14px; font-weight:700; margin-bottom:6px;">
            Do you want to send the newly issued invoice to the client by email now?
          </div>
          ${recipientLine}
          <div class="mini" style="margin-top:10px; color:#666;">
            If you choose “Yes”, the email will be queued immediately. The PDF will be generated via the invoice PDF queue if needed.
          </div>

          <div style="display:flex; gap:10px; margin-top:14px;">
            <button id="btnInvSendEmailYes" type="button" class="btn btn-primary">Yes, send now</button>
            <button id="btnInvSendEmailNo"  type="button" class="btn btn-outline">No, not now</button>
          </div>
        </div>
      `;
    };

    return await new Promise((resolve) => {
      let done = false;
      const finish = (v) => {
        if (done) return;
        done = true;
        resolve(!!v);
      };

      // Utility modal: no Save footer, Close works as "No"
      showModal(
        title,
        [{ key: 'confirm', label: 'Confirm' }],
        () => renderTab(),
        null,
        true,
        null,
        {
          kind: 'invoice-send-email-confirm',
          noParentGate: true,
          showSave: false,
          showApply: false,
          onDismiss: () => finish(false)
        }
      );

      const wire = () => {
        const yes = document.getElementById('btnInvSendEmailYes');
        const no  = document.getElementById('btnInvSendEmailNo');

        if (yes && !yes.__wired) {
          yes.__wired = true;
          yes.onclick = () => {
            finish(true);
            try { document.getElementById('btnCloseModal')?.click(); } catch {}
          };
        }

        if (no && !no.__wired) {
          no.__wired = true;
          no.onclick = () => {
            finish(false);
            try { document.getElementById('btnCloseModal')?.click(); } catch {}
          };
        }
      };

      try {
        requestAnimationFrame(() => requestAnimationFrame(wire));
      } catch {
        setTimeout(wire, 0);
      }
    });
  };

  const nonEmpty = (v) => {
    if (!v) return false;
    if (Array.isArray(v)) return v.length > 0;
    if (v instanceof Set || v instanceof Map) return v.size > 0;
    if (typeof v === 'object') return Object.keys(v).length > 0;
    return false;
  };

  // ✅ Refresh the summary sheet behind the modal (best-effort)
  // IMPORTANT: never call renderSummary() with no rows (it expects an array).
  const refreshSummaryBehindModal = async () => {
    try {
      // Prefer captured summary context (set at modal-open in showModal/openInvoiceModal). Fallback to capture now.
      let ctx = (modalCtx && modalCtx.__summaryCtx) ? modalCtx.__summaryCtx : null;
      if (!ctx && typeof captureSummaryContextForModalOpen === 'function') {
        try { ctx = captureSummaryContextForModalOpen(); } catch { ctx = null; }
      }

      // If we have the summary patch pipeline available, use it
      if (typeof summaryFetchCanonicalRow === 'function' && typeof summaryApplySavedRecordToActiveSummary === 'function') {
        let canonical = null;
        try {
          canonical = await summaryFetchCanonicalRow('invoices', invoiceId, ctx);
        } catch {
          canonical = null;
        }

        if (canonical && typeof normalizeSavedRecordForSummary === 'function') {
          try { canonical = normalizeSavedRecordForSummary('invoices', canonical) || canonical; } catch {}
        }

        if (canonical && typeof canonical === 'object') {
          const r = await summaryApplySavedRecordToActiveSummary('invoices', canonical, ctx);
          if (r && r.ok) return;
        }
      }

      // Fallback: only use renderAll (safe full pipeline) — do NOT call renderSummary() with no args
      if (typeof window.renderAll === 'function') {
        await window.renderAll();
        return;
      }

      // Last resort: section-specific refresh helper, if you have one
      if (typeof window.refreshCurrentSummary === 'function') {
        await window.refreshCurrentSummary();
        return;
      }

    } catch (e) {
      try { console.warn('[INV][SAVE] summary refresh failed (non-fatal)', e); } catch {}
    }
  };

  // Build payload including only non-empty keys
  const e = (modalCtx?.invoiceEdits && typeof modalCtx.invoiceEdits === 'object') ? modalCtx.invoiceEdits : {};

  const remove_invoice_line_ids = Array.from(e.remove_invoice_line_ids || []);
  const add_timesheet_ids = Array.from(e.add_timesheet_ids || []);

  const add_adjustments = (Array.isArray(e.add_adjustments) ? e.add_adjustments : []).map(a => ({
    client_token: a.client_token,
    description: a.description,
    amount_ex_vat: a.amount_ex_vat,
    ...(invoiceModalGetInvoiceData(modalCtx).weekly_source_invoice?.is_weekly_source_invoice === true
      && Object.prototype.hasOwnProperty.call(a,'vat_amount') ? {vat_amount:a.vat_amount} : {})
  })).filter(a => (a && (a.amount_ex_vat != null || (a.description && String(a.description).trim()))));

  const normSeg = (r) => {
    if (!r || typeof r !== 'object') return null;
    const tsfin_id = (r.tsfin_id != null) ? String(r.tsfin_id) : '';
    const segment_id = (r.segment_id != null) ? String(r.segment_id) : '';
    if (!tsfin_id || !segment_id) return null;
    return { tsfin_id, segment_id };
  };

  const remove_segment_refs = (Array.isArray(e.remove_segment_refs) ? e.remove_segment_refs : [])
    .map(normSeg)
    .filter(Boolean);

  const add_segment_refs = (Array.isArray(e.add_segment_refs) ? e.add_segment_refs : [])
    .map(normSeg)
    .filter(Boolean);

  // ✅ References: support new keyed staging model (object map) + Map + legacy array
  let reference_updates = [];
  if (nonEmpty(e.reference_updates_by_key)) {
    const keys = Object.keys(e.reference_updates_by_key || {}).sort(); // stable
    reference_updates = keys.map(k => e.reference_updates_by_key[k]).filter(x => x && typeof x === 'object');
  } else if (e.reference_updates_map instanceof Map && e.reference_updates_map.size > 0) {
    reference_updates = Array.from(e.reference_updates_map.values()).filter(x => x && typeof x === 'object');
  } else if (Array.isArray(e.reference_updates)) {
    reference_updates = e.reference_updates.filter(x => x && typeof x === 'object');
  }

  // Require timesheet_id for any ref update
  reference_updates = reference_updates.filter(u => {
    const tsId = (u && u.timesheet_id != null) ? String(u.timesheet_id).trim() : '';
    return !!tsId;
  });

  // ─────────────────────────────────────────────────────────────
  // ✅ Compile row-level ref edits into DB-applied updates
  // invoice_apply_edits only applies:
  //   reference_number, day_references_json, actual_schedule_json
  // (keyed by timesheet_id). Do NOT send row-level edits to DB.
  // ─────────────────────────────────────────────────────────────
  const compileReferenceUpdatesForDb = (rowLevelUpdates) => {
    if (!Array.isArray(rowLevelUpdates) || rowLevelUpdates.length === 0) return [];

    // Prefer the pre-hydrated maps from invoice manifest/GET (zero extra calls).
    // NOTE: payloads may store these at:
    //  - modalCtx.timesheet_reference_sources_by_id
    //  - modalCtx.dataLoaded.timesheet_reference_sources_by_id
    //  - modalCtx.dataLoaded.manifest.timesheet_reference_sources_by_id
    const sourcesById =
      (modalCtx?.timesheet_reference_sources_by_id && typeof modalCtx.timesheet_reference_sources_by_id === 'object')
        ? modalCtx.timesheet_reference_sources_by_id
        : ((modalCtx?.dataLoaded?.timesheet_reference_sources_by_id && typeof modalCtx.dataLoaded.timesheet_reference_sources_by_id === 'object')
            ? modalCtx.dataLoaded.timesheet_reference_sources_by_id
            : ((modalCtx?.dataLoaded?.manifest?.timesheet_reference_sources_by_id && typeof modalCtx.dataLoaded.manifest.timesheet_reference_sources_by_id === 'object')
                ? modalCtx.dataLoaded.manifest.timesheet_reference_sources_by_id
                : null));

    const rowsByKey =
      (modalCtx?.reference_rows_by_key && typeof modalCtx.reference_rows_by_key === 'object')
        ? modalCtx.reference_rows_by_key
        : ((modalCtx?.dataLoaded?.reference_rows_by_key && typeof modalCtx.dataLoaded.reference_rows_by_key === 'object')
            ? modalCtx.dataLoaded.reference_rows_by_key
            : ((modalCtx?.dataLoaded?.manifest?.reference_rows_by_key && typeof modalCtx.dataLoaded.manifest.reference_rows_by_key === 'object')
                ? modalCtx.dataLoaded.manifest.reference_rows_by_key
                : null));

    if (!sourcesById || typeof sourcesById !== 'object') {
      throw new Error('Reference sources are missing on this invoice payload. Reload the invoice and try again.');
    }

    const normStr = (v) => (v == null ? '' : String(v).trim());
    const upperStr = (v) => normStr(v).toUpperCase();

    const deepClone = (v) => {
      if (v == null) return v;
      try { return JSON.parse(JSON.stringify(v)); } catch { return v; }
    };

    const toRefVal = (v) => {
      const s = normStr(v);
      return s ? s : null;
    };

    const getMeta = (u) => {
      const k = normStr(u?.row_key);
      if (k && rowsByKey && rowsByKey[k] && typeof rowsByKey[k] === 'object') return rowsByKey[k];
      return (u && typeof u === 'object') ? u : {};
    };

    const getTarget = (meta) => {
      return upperStr(meta?.ref_target || meta?.kind || meta?.target);
    };

    const getDay = (meta) => {
      return normStr(meta?.day_ymd || meta?.day_date || meta?.date);
    };

    const getSegId = (meta) => {
      return normStr(meta?.segment_id);
    };

    const getStart = (meta) => {
      return normStr(meta?.start_utc || meta?.start);
    };

    const getEnd = (meta) => {
      return normStr(meta?.end_utc || meta?.end);
    };

    // Group updates per timesheet_id
    const byTs = new Map();
    for (const u of rowLevelUpdates) {
      const tsid = normStr(u?.timesheet_id);
      if (!tsid) continue;
      if (!byTs.has(tsid)) byTs.set(tsid, []);
      byTs.get(tsid).push(u);
    }

    const tsIds = Array.from(byTs.keys()).sort();
    const out = [];

    for (const tsid of tsIds) {
      const base = (sourcesById[tsid] && typeof sourcesById[tsid] === 'object') ? sourcesById[tsid] : {};
      const baseRefNum = toRefVal(base.reference_number);

      const baseDayRefs =
        (base.day_references_json && typeof base.day_references_json === 'object' && !Array.isArray(base.day_references_json))
          ? base.day_references_json
          : null;

      const baseSched =
        Array.isArray(base.actual_schedule_json)
          ? base.actual_schedule_json
          : null;

      let newRefNum = baseRefNum;
      let newDayRefs = baseDayRefs ? deepClone(baseDayRefs) : null;
      let newSched = baseSched ? deepClone(baseSched) : null;

      let changedRefNum = false;
      let changedDayRefs = false;
      let changedSched = false;

      const updates = byTs.get(tsid) || [];

      for (const u of updates) {
        const meta = getMeta(u);
        const target = getTarget(meta);
        const val = toRefVal(u?.current_reference);

        if (target === 'TIMESHEET') {
          const prev = toRefVal(newRefNum);
          const next = toRefVal(val);
          if (prev !== next) {
            newRefNum = next;
            changedRefNum = true;
          }
          continue;
        }

        if (target === 'FREEFORM') {
          const day = getDay(meta);
          if (!day) continue;

          if (!newDayRefs || typeof newDayRefs !== 'object' || Array.isArray(newDayRefs)) {
            newDayRefs = {};
          }

          const prev = toRefVal(newDayRefs[day]);
          const next = toRefVal(val);

          if (next == null) {
            if (Object.prototype.hasOwnProperty.call(newDayRefs, day)) {
              delete newDayRefs[day];
              changedDayRefs = true;
            }
          } else {
            if (prev !== next) {
              newDayRefs[day] = next;
              changedDayRefs = true;
            }
          }

          // collapse empty map to null
          if (newDayRefs && typeof newDayRefs === 'object' && !Array.isArray(newDayRefs)) {
            if (Object.keys(newDayRefs).length === 0) newDayRefs = null;
          }

          continue;
        }

        if (target === 'SEGMENT') {
          // We require a schedule-like array (manual schedule OR TSFIN-derived SEGMENTS schedule from manifest).
          if (!Array.isArray(newSched) || newSched.length === 0) {
            throw new Error(`Cannot update segment reference(s): schedule is missing for timesheet ${tsid}. Reload the invoice and try again.`);
          }

          const segId = getSegId(meta);
          const start = getStart(meta);
          const end = getEnd(meta);

          // 1) Manual weekly index form: segment_id = "ts:<timesheet_id>:<idx>"
          if (segId && segId.startsWith(`ts:${tsid}:`)) {
            const parts = segId.split(':');
            const idxStr = parts[parts.length - 1];
            const idx = Number.parseInt(idxStr, 10);

            if (Number.isFinite(idx) && idx >= 0 && idx < newSched.length) {
              const seg = (newSched[idx] && typeof newSched[idx] === 'object') ? newSched[idx] : {};
              const prev = toRefVal(seg.ref_num);
              const next = toRefVal(val);

              if (prev !== next) {
                seg.ref_num = next;
                newSched[idx] = seg;
                changedSched = true;
              }
              continue;
            }

            throw new Error(`Cannot update segment reference: invalid segment index "${idxStr}" for timesheet ${tsid}.`);
          }

          // 2) Segment-id match (preferred for NHSP/HR/import segments; supports multi-shift/day)
          if (segId) {
            let foundById = -1;
            for (let i = 0; i < newSched.length; i++) {
              const seg = newSched[i];
              if (!seg || typeof seg !== 'object') continue;
              const sid = normStr(seg.segment_id);
              if (sid && sid === segId) { foundById = i; break; }
            }

            if (foundById >= 0) {
              const seg = (newSched[foundById] && typeof newSched[foundById] === 'object') ? newSched[foundById] : {};
              const prev = toRefVal(seg.ref_num);
              const next = toRefVal(val);

              if (prev !== next) {
                seg.ref_num = next;
                newSched[foundById] = seg;
                changedSched = true;
              }
              continue;
            }
          }

          // 3) Match by start/end window (fallback)
          if (!start || !end) {
            throw new Error(`Cannot update segment reference: missing start/end identifiers for timesheet ${tsid}.`);
          }

          let found = -1;
          for (let i = 0; i < newSched.length; i++) {
            const seg = newSched[i];
            if (!seg || typeof seg !== 'object') continue;

            const s1 = normStr(seg.start_utc || seg.start);
            const e1 = normStr(seg.end_utc || seg.end);

            if (s1 === start && e1 === end) {
              found = i;
              break;
            }
          }

          if (found < 0) {
            const sidTxt = segId ? ` seg_id=${segId}` : '';
            throw new Error(`Cannot update segment reference: no matching schedule entry for timesheet ${tsid}${sidTxt} (${start} → ${end}).`);
          }

          const seg = (newSched[found] && typeof newSched[found] === 'object') ? newSched[found] : {};
          const prev = toRefVal(seg.ref_num);
          const next = toRefVal(val);

          if (prev !== next) {
            seg.ref_num = next;
            newSched[found] = seg;
            changedSched = true;
          }

          continue;
        }

        // Unknown target: ignore (defensive)
      }

      if (!changedRefNum && !changedDayRefs && !changedSched) continue;

      const expectedRevision = String(base.document_revision ?? '').trim();
      if (!/^[1-9][0-9]*$/.test(expectedRevision)) {
        throw new Error(`The current document revision is missing for timesheet ${tsid}. Reload the invoice and try again.`);
      }
      const obj = {
        timesheet_id: tsid,
        expected_document_revision: Number(expectedRevision)
      };

      if (changedRefNum) obj.reference_number = newRefNum;

      if (changedDayRefs) {
        // If newDayRefs is null we send null (DB converts json null -> SQL null)
        obj.day_references_json = (newDayRefs && typeof newDayRefs === 'object' && !Array.isArray(newDayRefs)) ? newDayRefs : null;
      }

      if (changedSched) {
        // Never null-out schedule here; we only edit ref_num within existing entries
        obj.actual_schedule_json = Array.isArray(newSched) ? newSched : null;
      }

      out.push(obj);
    }

    return out;
  };

  // Compile now (but do NOT mutate staged row-level updates)
  const reference_updates_compiled = compileReferenceUpdatesForDb(reference_updates);
  const canonicalLocation = (value) => String(value == null ? '' : value)
    .trim().replace(/\s+/g, ' ').toLowerCase();
  const compileLocationUpdatesForDb = () => {
    const staged =
      (e.timesheet_location_updates_by_id
        && typeof e.timesheet_location_updates_by_id === 'object'
        && !Array.isArray(e.timesheet_location_updates_by_id))
        ? e.timesheet_location_updates_by_id
        : {};
    if (!Object.keys(staged).length) return [];
    const sourcesById =
      (modalCtx?.timesheet_reference_sources_by_id
        && typeof modalCtx.timesheet_reference_sources_by_id === 'object')
        ? modalCtx.timesheet_reference_sources_by_id
        : modalCtx?.dataLoaded?.timesheet_reference_sources_by_id;
    if (!sourcesById || typeof sourcesById !== 'object') {
      throw new Error('Timesheet hospital and ward sources are missing. Reload the invoice and try again.');
    }
    return Object.keys(staged).sort().flatMap(timesheetId => {
      const update = staged[timesheetId] || {};
      const source = sourcesById[timesheetId];
      if (!source || typeof source !== 'object') {
        throw new Error('A timesheet on this invoice has changed. Reload the invoice and try again.');
      }
      const hospital = canonicalLocation(update.hospital_norm);
      const ward = canonicalLocation(update.ward_norm);
      if (
        hospital === canonicalLocation(source.hospital_norm)
        && ward === canonicalLocation(source.ward_norm)
      ) return [];
      return [{
        timesheet_id: timesheetId,
        expected_document_revision: (() => {
          const revision = String(source.document_revision ?? '').trim();
          if (!/^[1-9][0-9]*$/.test(revision)) {
            throw new Error('A timesheet document revision is missing. Reload the invoice and try again.');
          }
          return Number(revision);
        })(),
        hospital_norm: hospital || null,
        ward_norm: ward || null
      }];
    });
  };
  const timesheet_location_updates = compileLocationUpdatesForDb();
  const hasTimesheetSourceEdits =
    reference_updates_compiled.length > 0 || timesheet_location_updates.length > 0;
  const invoiceHadGeneratedOrActivePreview = (
    modalCtx?.invoiceDetail?.source_edit_will_replace_preview === true
    || modalCtx?.dataLoaded?.source_edit_will_replace_preview === true
  );

  const payload = {};
  if (remove_invoice_line_ids.length) payload.remove_invoice_line_ids = remove_invoice_line_ids;
  if (add_timesheet_ids.length) payload.add_timesheet_ids = add_timesheet_ids;
  if (add_adjustments.length) payload.add_adjustments = add_adjustments;
  if (remove_segment_refs.length) payload.remove_segment_refs = remove_segment_refs;
  if (add_segment_refs.length) payload.add_segment_refs = add_segment_refs;

  // ✅ Only send DB-applied compiled updates (never row-level edits)
  if (reference_updates_compiled.length) payload.reference_updates = reference_updates_compiled;
  if (timesheet_location_updates.length) {
    payload.timesheet_location_updates = timesheet_location_updates;
  }
  const hasApplyEdits = Object.keys(payload).length > 0;
  const asyncInvoiceUiEnabled =
    typeof window.isInvoiceAsyncUiEnabled === 'function'
    && window.isInvoiceAsyncUiEnabled();
  const asyncIssueAfterEdits =
    asyncInvoiceUiEnabled
    && hasApplyEdits
    && wantIssued === true
    && curIsIssued !== true;

  const submitAsyncIssueFromCurrentDetail = async () => {
    const canDeliver = modalCtx?.invoiceDetail?.can_issue_and_deliver === true
      || modalCtx?.dataLoaded?.can_issue_and_deliver === true;
    const sendNow = canDeliver ? await promptSendInvoiceEmailNow({
      toEmail: modalCtx?.invoiceDetail?.email_summary?.to
        || modalCtx?.dataLoaded?.email_summary?.to
        || null
    }) : false;
    const issueCommandToken = String(
      modalCtx?.invoiceAsync?.issue_command_token || crypto.randomUUID()
    );
    modalCtx.invoiceAsync = (modalCtx.invoiceAsync && typeof modalCtx.invoiceAsync === 'object')
      ? modalCtx.invoiceAsync : {};
    modalCtx.invoiceAsync.issue_command_token = issueCommandToken;
    const deliveryRequestToken = sendNow
      ? String(modalCtx.invoiceAsync.delivery_request_token || crypto.randomUUID())
      : null;
    if (sendNow && deliveryRequestToken === issueCommandToken) {
      throw new Error('Delivery request identity must be distinct from the Issue command identity.');
    }
    modalCtx.invoiceAsync.delivery_request_token = deliveryRequestToken;
    const expectedIssueRevision = String(
      modalCtx?.invoiceDetail?.invoice?.document_revision
      ?? modalCtx?.dataLoaded?.invoice?.document_revision
      ?? modalCtx?.data?.document_revision
      ?? ''
    ).trim();
    if (!/^[1-9][0-9]*$/.test(expectedIssueRevision)) {
      throw new Error('The invoice revision is missing. Reload the invoice before issuing it.');
    }
    const issueRes = await invoiceModalFetchJson(`/api/invoices/${encodeURIComponent(invoiceId)}/issue`, {
      method: 'POST',
      headers: { 'Idempotency-Key': issueCommandToken },
      body: JSON.stringify({
        command_token: issueCommandToken,
        expected_revision: expectedIssueRevision,
        deliver: sendNow,
        delivery_request_token: deliveryRequestToken || undefined
      })
    });
    const operations = window.registerInvoiceOperationsFromResponse?.(issueRes, {
      entity_type: 'INVOICE',
      entity_id: invoiceId,
      operation_type: 'ISSUE_INVOICES',
      purpose: sendNow ? 'ISSUE_AND_DELIVER' : 'ISSUE',
      command_token: issueCommandToken,
      modal_identity: `invoice:${invoiceId}`,
      explicit_operation_ids: true
    }) || [];
    modalCtx.invoiceAsync.issue_operation = operations[0] || null;
    toast(sendNow
      ? 'Invoice issue and delivery preparation started. You can close this window.'
      : 'Invoice issue preparation started. You can close this window.');
    return issueRes;
  };

  // ✅ Confirm dialogs (status changes only)
  // (Do this BEFORE we mark the modal busy)
  if (hasTimesheetSourceEdits) {
    const message = invoiceHadGeneratedOrActivePreview
      ? 'These changes will force a new timesheet image to be generated and will queue an invoice to be regenerated. Are you sure you want to proceed?'
      : 'These changes will force a new timesheet image to be generated. Are you sure you want to proceed?';
    const confirmation = await openUiConfirmModal({
      title: 'Regenerate document images?',
      message,
      confirm_label: 'Proceed',
      cancel_label: 'Cancel',
      kind: 'invoice-source-edit-confirm',
      suppress_parent_persist: true
    });
    if (confirmation?.confirmed !== true) return;
  }
  if (wantHold !== null && wantHold !== curIsHold) {
    const ok = await invoiceModalUiConfirm({
      title: wantHold ? 'Put invoice on hold?' : 'Remove invoice hold?',
      message: wantHold ? 'Put this invoice ON HOLD?' : 'Remove ON HOLD from this invoice?',
      confirmLabel: wantHold ? 'Put on hold' : 'Remove hold',
      kind: 'invoice-hold-save-confirm'
    });
    if (!ok) return;
  }
  if (wantIssued !== null && wantIssued !== curIsIssued) {
    const ok = await invoiceModalUiConfirm({
      title: wantIssued ? 'Issue invoice?' : 'Unissue invoice?',
      message: wantIssued ? 'Issue this invoice now?' : 'Unissue this invoice? This will invalidate the PDF.',
      confirmLabel: wantIssued ? 'Issue invoice' : 'Unissue invoice',
      kind: 'invoice-issued-save-confirm'
    });
    if (!ok) return;
  }
  if (wantPaid !== null && wantPaid !== curIsPaid) {
    const ok = await invoiceModalUiConfirm({
      title: wantPaid ? 'Mark invoice paid?' : 'Mark invoice unpaid?',
      message: wantPaid ? 'Mark this invoice as PAID?' : 'Mark this invoice as UNPAID?',
      confirmLabel: wantPaid ? 'Mark paid' : 'Mark unpaid',
      kind: 'invoice-paid-save-confirm'
    });
    if (!ok) return;
  }

  // ✅ Hard-block issuing while ON_HOLD (unless unhold is staged in this same save)
  if (wantIssued === true) {
    const effHold = (wantHold === null) ? curIsHold : !!wantHold;
    if (effHold) {
      throw new Error('Invoice is ON HOLD. Unhold first.');
    }
  }

  // If there are apply-edits staged, we must ensure invoice will NOT be ISSUED/PAID at apply time.
  if (hasApplyEdits) {
    const willStillBeIssued = (wantIssued == null) ? curIsIssued : wantIssued;
    if (willStillBeIssued && !asyncIssueAfterEdits) {
      throw new Error('Cannot edit invoice lines/segments/refs while invoice is ISSUED. Unissue first (staged) and Save again.');
    }

    const willStillBePaid = (wantPaid == null) ? curIsPaid : wantPaid;
    if (willStillBePaid) {
      throw new Error('Cannot edit invoice lines/segments/refs while invoice is PAID. Mark unpaid first (staged) and Save again.');
    }
  }

  modalCtx.isBusy = true;
  modalCtx.error = null;
  rerender();

  try {
    // 1) HOLD / UNHOLD
    if (wantHold !== null && wantHold !== curIsHold) {
      if (wantHold) {
        const reason = (typeof stagedReason.on_hold_reason === 'string' && stagedReason.on_hold_reason.trim())
          ? stagedReason.on_hold_reason.trim()
          : null;

        await invoiceModalFetchJson(`/api/invoices/${encodeURIComponent(invoiceId)}/hold`, {
          method: 'POST',
          body: JSON.stringify({ reason })
        });
      } else {
        await invoiceModalFetchJson(`/api/invoices/${encodeURIComponent(invoiceId)}/unhold`, {
          method: 'POST',
          body: JSON.stringify({})
        });
      }
    }

    // 2) ISSUE / UNISSUE
    if (wantIssued !== null && wantIssued !== curIsIssued) {
      if (wantIssued === true && !asyncInvoiceUiEnabled) {
        throw new Error('Invoice processing is temporarily unavailable while the new invoice system is being updated.');
      }
      if (wantIssued) {
        if (asyncInvoiceUiEnabled) {
          if (!asyncIssueAfterEdits) await submitAsyncIssueFromCurrentDetail();
        } else {
        const canDeliver = modalCtx?.invoiceDetail?.can_issue_and_deliver === true
          || modalCtx?.dataLoaded?.can_issue_and_deliver === true;
        const sendNow = canDeliver ? await promptSendInvoiceEmailNow({
          toEmail: modalCtx?.invoiceDetail?.email_summary?.to
            || modalCtx?.dataLoaded?.email_summary?.to
            || null
        }) : false;
        const issueCommandToken = String(
          modalCtx?.invoiceAsync?.issue_command_token || crypto.randomUUID()
        );
        modalCtx.invoiceAsync = (modalCtx.invoiceAsync && typeof modalCtx.invoiceAsync === 'object')
          ? modalCtx.invoiceAsync : {};
        modalCtx.invoiceAsync.issue_command_token = issueCommandToken;

        const issueRes = await invoiceModalFetchJson(`/api/invoices/${encodeURIComponent(invoiceId)}/issue`, {
          method: 'POST',
          body: JSON.stringify({
            command_token: issueCommandToken,
            deliver: sendNow,
            delivery_request_token: sendNow ? issueCommandToken : undefined
          })
        });

        const asyncAccepted = issueRes?.accepted === true
          || Number(issueRes?.accepted_count || 0) > 0
          || Array.isArray(issueRes?.operation_ids) && issueRes.operation_ids.length > 0;
        if (asyncAccepted) {
          try {
            window.registerInvoiceOperationsFromResponse?.(issueRes, {
              entity_type: 'INVOICE',
              entity_id: invoiceId,
              purpose: sendNow ? 'ISSUE_AND_DELIVER' : 'ISSUE',
              command_token: issueCommandToken,
              modal_identity: `invoice:${invoiceId}`
            });
          } catch {}
          toast(sendNow
            ? 'Invoice issue and delivery preparation started. You can close this window.'
            : 'Invoice issue preparation started. You can close this window.');
        } else {
          const st2 = String(issueRes?.status || '').toUpperCase();
          if (st2 === 'ON_HOLD') {
            const reasons = Array.isArray(issueRes?.reasons) ? issueRes.reasons : [];
            const msg = reasons.length
              ? `Invoice is ON HOLD and cannot be issued: ${reasons.join(', ')}`
              : 'Invoice is ON HOLD and cannot be issued.';
            throw new Error(msg);
          }
          if (st2 !== 'ISSUED') throw new Error(issueRes?.error || 'Failed to issue invoice.');

          if (sendNow && issueRes?.email_eligible === true) {
            try {
              await invoiceModalFetchJson(`/api/invoices/${encodeURIComponent(invoiceId)}/email`, {
                method: 'POST',
                body: JSON.stringify({ command_token: issueCommandToken })
              });
              toast('Invoice email queued.');
            } catch (err) {
              toast(String(err?.message || err || 'Failed to queue invoice email.'));
            }
          }
        }
        }
      } else {
        // ✅ clear_pdf MUST be true on unissue (invalidate bundle)
        await invoiceModalFetchJson(`/api/invoices/${encodeURIComponent(invoiceId)}/unissue`, {
          method: 'POST',
          body: JSON.stringify({ clear_pdf: true })
        });
      }
    }

    // 3) PAID / UNPAID
    if (wantPaid !== null && wantPaid !== curIsPaid) {
      if (wantPaid) {
        const paid_date = (typeof stagedReason.paid_date === 'string' && stagedReason.paid_date.trim())
          ? stagedReason.paid_date.trim()
          : null;

        await invoiceModalFetchJson(`/api/invoices/${encodeURIComponent(invoiceId)}/mark-paid`, {
          method: 'POST',
          body: JSON.stringify(paid_date ? { paid_date } : {})
        });
      } else {
        await invoiceModalFetchJson(`/api/invoices/${encodeURIComponent(invoiceId)}/mark-unpaid`, {
          method: 'POST',
          body: JSON.stringify({})
        });
      }
    }

    // 4) APPLY EDITS (only if there are edits staged)
    if (hasApplyEdits) {
      try {
        const res = await invoiceModalFetchJson(`/api/invoices/${encodeURIComponent(invoiceId)}/save-edits`, {
          method: 'POST',
          body: JSON.stringify(payload)
        });

        const out = (res && typeof res === 'object') ? res : {};
        if (!out.ok) {
          throw new Error(out.error || 'Failed to save edits');
        }
        if (Array.isArray(out.accepted_operations) && out.accepted_operations.length) {
          try {
            window.registerInvoiceOperationsFromResponse?.(out, {
              modal_identity: `invoice:${invoiceId}`,
              explicit_operation_ids: true
            });
          } catch {}
        }
        if (out.operation_submission_error?.message) {
          toast('The changes were saved and the replacement documents were queued, but immediate processing could not be confirmed.');
        }

        // invoice content changed → eligibility cache must be treated as stale
        try {
          modalCtx.eligibleTimesheetsCache = null;
          modalCtx.eligibleTimesheetsCacheRev = null;
        } catch {}
      } catch (err) {
        const msg = String(err?.message || err || '');

        if (/changed after the invoice was opened|stale revision/i.test(msg)) {
          try { await reload(); } catch {}
          modalCtx.error =
            'This timesheet changed after the invoice was opened. Review the refreshed values before saving again.';
          toast(modalCtx.error);
          rerender();
          throw err;
        }

        if (/Segment already invoiced/i.test(msg) || /already invoiced/i.test(msg) || /no longer available/i.test(msg)) {
          modalCtx.error =
            'This amended invoice cannot be saved because the timesheets you have selected are no longer available to invoice.';
          toast(modalCtx.error);
          rerender();
          throw err;
        }

        if (/Segments cannot be moved when additional rates exist/i.test(msg)) {
          modalCtx.error = 'Cannot move segments because this invoice contains additional rates. Remove additional rates or remove the whole timesheet instead.';
          toast(modalCtx.error);
          rerender();
          throw err;
        }

        if (/Segments cannot be moved when expenses or mileage exist/i.test(msg)) {
          modalCtx.error = 'Cannot move segments because this invoice contains expenses or mileage. Remove expenses/mileage or remove the whole timesheet instead.';
          toast(modalCtx.error);
          rerender();
          throw err;
        }

        modalCtx.error = msg || 'Failed to save invoice edits.';
        toast(modalCtx.error);
        rerender();
        throw err;
      }
    }

    // Async issue must use the revision returned after edits are committed.
    // Rendering and delivery remain background operations; this only submits
    // the legal issue command against the freshly reloaded revision.
    if (asyncIssueAfterEdits) {
      await reload();
      await submitAsyncIssueFromCurrentDetail();
    }

    // 5) Clear staged edits + exit edit mode
    invoiceModalResetEdits(modalCtx);
    modalCtx.isEditing = false;

    // Final reload (single source of truth)
    await reload();

    // ✅ Refresh the summary sheet behind the modal so the list updates immediately
    await refreshSummaryBehindModal();

  } finally {
    modalCtx.isBusy = false;
    rerender();
  }
}




async function openInvoiceAddAdjustmentModal(modalCtx, { rerender }) {
  const parentCtx = modalCtx; // ✅ stable reference
  const sourceInvoiceData = invoiceModalGetInvoiceData(parentCtx);
  const { invoice } = sourceInvoiceData;
  if (sourceInvoiceData.weekly_source_invoice?.is_weekly_source_invoice === true) {
    return window.CloudTmsSourceInvoiceAdjustments?.open(parentCtx,{
      invoiceData:sourceInvoiceData,canEdit:invoiceModalIsEditable(invoice,parentCtx,'lines'),
      showModal,document,newToken:invoiceModalNewClientToken,
      close:()=>document.getElementById('btnCloseModal')?.click(),rerender
    });
  }

  // Ensure showModal uses the invoice modal ctx (shared context across modal stack)
  try { window.modalCtx = parentCtx; } catch {}

  // Determine whether user is allowed to edit lines right now (staging-aware)
  const canEditLines = invoiceModalIsEditable(invoice, parentCtx, 'lines');

  const status = String(invoice?.status || '').toUpperCase();
  const baseIssued = !!invoice?.issued_at_utc || status === 'ISSUED';
  const basePaid = !!invoice?.paid_at_utc || status === 'PAID';

  const st = (parentCtx?.invoiceEdits?.staged_status && typeof parentCtx.invoiceEdits.staged_status === 'object')
    ? parentCtx.invoiceEdits.staged_status
    : { issued: null, paid: null, on_hold: null };

  const effIssued = (st.issued === null || st.issued === undefined) ? baseIssued : !!st.issued;
  const effPaid = (st.paid === null || st.paid === undefined) ? basePaid : !!st.paid;

  let bannerHtml = '';
  if (!canEditLines) {
    if (effPaid) {
      bannerHtml = `
        <div class="alert alert-warning py-2 mb-3">
          <b>Unpay invoice first</b> (then unissue invoice).
        </div>
      `;
    } else if (effIssued) {
      bannerHtml = `
        <div class="alert alert-warning py-2 mb-3">
          <b>Unissue invoice first</b>.
        </div>
      `;
    } else {
      bannerHtml = `
        <div class="alert alert-warning py-2 mb-3">
          <b>Invoice is not editable</b>.
        </div>
      `;
    }
  }

  const disabledAttr = canEditLines ? '' : 'disabled';
  const disabledOpacity = canEditLines ? '' : 'opacity-50';

  const html = `
    <div class="p-3">
      <div class="h6 mb-3">Invoice Adjustment</div>

      ${bannerHtml}

      <div class="mb-2 ${disabledOpacity}">
        <label class="form-label small text-muted">Description</label>
        <input id="invAdjDesc" class="form-control" type="text" placeholder="e.g. Manual correction" ${disabledAttr} />
      </div>

      <div class="mb-2 ${disabledOpacity}">
        <label class="form-label small text-muted">Amount (ex VAT)</label>
        <input id="invAdjAmt" class="form-control" type="number" step="0.01" placeholder="0.00" ${disabledAttr} />
        <div class="text-muted small mt-1">Use a negative number for a credit/discount.</div>
      </div>

      <div class="d-flex justify-content-end gap-2 mt-3">
        <button type="button" class="btn btn-sm btn-secondary" id="invAdjCancel">Cancel</button>
        <button type="button" class="btn btn-sm btn-primary" id="invAdjAdd" ${canEditLines ? '' : 'disabled'}>Add adjustment</button>
      </div>
    </div>
  `;

  showModal(
    'Invoice Adjustment',
    [{ key: 'main', label: 'Adjustment' }],
    () => html,
    null,
    true,
    null,
    { kind: 'invoice-batch-add-adjustment', noParentGate: true, showSave: false, showApply: true }
  );

  const closeTop = () => {
    // Close ONLY the top frame (this child) using the stack-safe close button
    try { document.getElementById('btnCloseModal')?.click(); } catch {}
  };

  const btnCancel = document.getElementById('invAdjCancel');
  const btnAdd = document.getElementById('invAdjAdd');

  if (btnCancel && !btnCancel.__invWired) {
    btnCancel.__invWired = true;
    btnCancel.onclick = (e) => {
      e.preventDefault();
      closeTop();
    };
  }

  if (btnAdd && !btnAdd.__invWired) {
    btnAdd.__invWired = true;
    btnAdd.onclick = async (e) => {
      e.preventDefault();

      // If selection is disabled (must unpay/unissue first), do nothing.
      if (!canEditLines) return;

      const desc = String(document.getElementById('invAdjDesc')?.value || '').trim();
      const amtRaw = document.getElementById('invAdjAmt')?.value;
      const amt = Number(amtRaw);

      if (!desc) {
        await invoiceModalUiNotice('Adjustment description is required.');
        return;
      }
      if (!Number.isFinite(amt)) {
        await invoiceModalUiNotice('Adjustment amount must be a valid number.');
        return;
      }

      // Stage ONLY on confirm
      parentCtx.invoiceEdits = parentCtx.invoiceEdits || {};
      parentCtx.invoiceEdits.add_adjustments = Array.isArray(parentCtx.invoiceEdits.add_adjustments)
        ? parentCtx.invoiceEdits.add_adjustments
        : [];

      parentCtx.invoiceEdits.add_adjustments.push({
        client_token: invoiceModalNewClientToken(),
        description: desc,
        amount_ex_vat: invoiceModalRound2(amt)
      });

      closeTop();

      // Ensure parent invoice modal reflects the staged change immediately
      try { setTimeout(() => { rerender && rerender(); }, 0); } catch {}
    };
  }
}


function renderInvoiceModalContent(modalCtx, invoiceData) {
  const fmtMoneyLocal = (n) => {
    const x = invoiceModalRound2(Number(n || 0));
    const s = x.toFixed(2);
    return `£${s}`;
  };

  const invData = (invoiceData && typeof invoiceData === 'object')
    ? invoiceData
    : invoiceModalGetInvoiceData(modalCtx);

  const {
    invoice,
    header_snapshot_json,
    items,
    weekly_source_invoice,
    email_summary,
    reference_rows,
    timesheet_reference_sources_by_id
  } = invData || {};

  if (!invoice) return `<div class="p-3 text-muted">Invoice not found.</div>`;

  const baseStatus = String(invoice.status || '').toUpperCase();
  const baseIsIssued = !!invoice.issued_at_utc;
  const baseIsPaid   = !!invoice.paid_at_utc;
  const baseIsOnHold = (baseStatus === 'ON_HOLD');

  const isEditing = !!modalCtx.isEditing;

  const canToggleStatus = invoiceModalIsEditable(invoice, modalCtx, 'status');
  const canEditLines    = invoiceModalIsEditable(invoice, modalCtx, 'lines');

  const staged = (modalCtx && modalCtx.invoiceEdits && typeof modalCtx.invoiceEdits === 'object') ? modalCtx.invoiceEdits : {};
  const st = (staged.staged_status && typeof staged.staged_status === 'object') ? staged.staged_status : { issued: null, paid: null, on_hold: null };
  const sd = (staged.staged_dates && typeof staged.staged_dates === 'object') ? staged.staged_dates : { issued_at_utc: null, paid_at_utc: null, status_date_utc: null };

  const effIssued = (st.issued === null || st.issued === undefined) ? baseIsIssued : !!st.issued;
  const effPaid   = (st.paid   === null || st.paid   === undefined) ? baseIsPaid   : !!st.paid;
  const effHold   = (st.on_hold=== null || st.on_hold=== undefined) ? baseIsOnHold : !!st.on_hold;

  const dueAt = invoice.due_at_utc ? new Date(invoice.due_at_utc) : null;
  const overdue =
    !!effIssued &&
    !effPaid &&
    dueAt &&
    Number.isFinite(dueAt.getTime()) &&
    (dueAt.getTime() < Date.now());

  const fmtLondonDateOnly = (iso) => {
    const s = String(fmtLondonTs(iso) || '').trim();
    const m = s.match(/^(\d{2}\/\d{2}\/\d{4})/);
    return m ? m[1] : s;
  };

  const displayIssuedAt = (() => {
    if (effIssued) {
      if (invoice.issued_at_utc) return fmtLondonTs(invoice.issued_at_utc);
      if (isEditing && st.issued === true && sd.issued_at_utc) return `${fmtLondonTs(sd.issued_at_utc)} (staged)`;
      return '—';
    }
    if (isEditing && st.issued === false) return 'Will clear';
    return '—';
  })();

  const displayPaidAt = (() => {
    if (effPaid) {
      if (invoice.paid_at_utc) return fmtLondonTs(invoice.paid_at_utc);
      if (isEditing && st.paid === true && sd.paid_at_utc) return `${fmtLondonTs(sd.paid_at_utc)} (staged)`;
      return '—';
    }
    if (isEditing && st.paid === false) return 'Will clear';
    return '—';
  })();

  const displayHoldAt = (() => {
    if (effHold) {
      if (invoice.status_date_utc) return fmtLondonTs(invoice.status_date_utc);
      if (isEditing && st.on_hold === true && sd.status_date_utc) return `${fmtLondonTs(sd.status_date_utc)} (staged)`;
      return '—';
    }
    if (isEditing && st.on_hold === false) return 'Will clear';
    return '—';
  })();

  const clientName =
    header_snapshot_json?.client_name ||
    invoice?.client?.name ||
    (invoiceData && invoiceData.client && invoiceData.client.name) ||
    '';

  const invoiceNo = (invoice.invoice_no != null && String(invoice.invoice_no).trim())
    ? `Invoice #${escapeHtml(String(invoice.invoice_no))}`
    : `Invoice ${escapeHtml(String(invoice.id || ''))}`;

  const invoiceDate = (effIssued && invoice.issued_at_utc) ? fmtLondonDateOnly(invoice.issued_at_utc) : '—';
  const createdDate = invoice.created_at ? fmtLondonTs(invoice.created_at) : '—';

  const hasLineEdits = (() => {
    const e = modalCtx?.invoiceEdits;
    if (!e) return false;
    return (
      (e.remove_invoice_line_ids && e.remove_invoice_line_ids.size > 0) ||
      (e.add_timesheet_ids && e.add_timesheet_ids.size > 0) ||
      (Array.isArray(e.add_adjustments) && e.add_adjustments.length > 0) ||
      (Array.isArray(e.remove_segment_refs) && e.remove_segment_refs.length > 0) ||
      (Array.isArray(e.add_segment_refs) && e.add_segment_refs.length > 0)
    );
  })();

  const hasStatusEdits =
    (st.issued !== null && st.issued !== undefined) ||
    (st.paid !== null && st.paid !== undefined) ||
    (st.on_hold !== null && st.on_hold !== undefined);

const hasRefEdits = (() => {
  const e = modalCtx?.invoiceEdits;
  if (!e) return false;

  if (e.reference_updates_by_key && typeof e.reference_updates_by_key === 'object' && !Array.isArray(e.reference_updates_by_key)) {
    if (Object.keys(e.reference_updates_by_key).length > 0) return true;
  }

  if (e.timesheet_location_updates_by_id && typeof e.timesheet_location_updates_by_id === 'object' && !Array.isArray(e.timesheet_location_updates_by_id)) {
    if (Object.keys(e.timesheet_location_updates_by_id).length > 0) return true;
  }

  return (Array.isArray(e.reference_updates) && e.reference_updates.length > 0);
})();


  const hasPendingEdits = hasLineEdits || hasStatusEdits || hasRefEdits;

  const currentTotals = {
    subtotal_ex_vat: invoiceModalRound2(invoice.subtotal_ex_vat || 0),
    vat_amount:      invoiceModalRound2(invoice.vat_amount || 0),
    total_inc_vat:   invoiceModalRound2(invoice.total_inc_vat || 0)
  };

  const previewTotals = hasLineEdits ? invoiceModalComputePreviewTotals(modalCtx) : currentTotals;
  const totalsToShow = hasLineEdits ? previewTotals : currentTotals;

  const pillSpan = (text, cls) => `<span class="pill ${cls}">${escapeHtml(text)}</span>`;

  // ✅ Support disabling the Issued pill when invoice is ON_HOLD (must unhold first).
  const pillBtn = (action, text, cls, opts = {}) => {
    const disabled = !!opts.disabled;
    const title = (opts.title != null && String(opts.title).trim()) ? String(opts.title).trim() : '';
    const disAttr = disabled ? 'disabled data-disabled="1"' : '';
    const disCls = disabled ? ' disabled' : '';
    const titleAttr = title ? ` title="${escapeHtml(title)}"` : '';
    return `
      <button type="button" class="pill ${cls}${disCls}" data-action="${action}" ${disAttr}${titleAttr}>
        ${escapeHtml(text)}
      </button>
    `;
  };

  // ✅ Hard-block issuing while ON_HOLD (UI-level): disable Issued toggle when ON_HOLD and currently unissued.
  const blockIssueWhileHold = !!(effHold && !effIssued);
  const issuedPill = canToggleStatus
    ? pillBtn(
        'inv-toggle-issued',
        effIssued ? 'Issued' : 'Unissued',
        effIssued ? 'pill-ok' : 'pill-bad',
        blockIssueWhileHold ? { disabled: true, title: 'Unhold first' } : {}
      )
    : pillSpan(effIssued ? 'Issued' : 'Unissued', effIssued ? 'pill-ok' : 'pill-bad');

  const paidPill = canToggleStatus
    ? pillBtn('inv-toggle-paid', effPaid ? 'Paid' : 'Unpaid', effPaid ? 'pill-ok' : 'pill-bad')
    : pillSpan(effPaid ? 'Paid' : 'Unpaid', effPaid ? 'pill-ok' : 'pill-bad');

  const holdPill = canToggleStatus
    ? pillBtn('inv-toggle-hold', effHold ? 'On hold' : 'Active', effHold ? 'pill-warn' : '')
    : pillSpan(effHold ? 'On hold' : 'Active', effHold ? 'pill-warn' : '');

  const overduePill = overdue ? pillSpan('Overdue', 'pill-bad') : '';

  const emailedOnce = !!email_summary?.emailed_once;
  const emailLabel = emailedOnce ? 'Re-email' : 'Email';
  const emailPolicy = invoiceModalEmailPolicyState(invData);
  const emailDisabledAttr = emailPolicy.disabled
    ? 'disabled data-disabled="1" aria-disabled="true" style="pointer-events:none;"'
    : 'aria-disabled="false"';
  const emailButtonHtml = `
    <button type="button" class="btn btn-sm btn-secondary" data-action="inv-email" ${emailDisabledAttr}>
      ${escapeHtml(emailLabel)}
    </button>
  `;
  const emailActionHtml = emailPolicy.disabled ? `
    <span class="invoice-email-policy-hint" data-invoice-email-policy="${escapeHtml(emailPolicy.reason_code || 'BLOCKED')}"
      title="${escapeHtml(emailPolicy.hint)}" aria-label="${escapeHtml(`${emailLabel} unavailable. ${emailPolicy.hint}`)}"
      tabindex="0" style="display:inline-flex;">
      ${emailButtonHtml}
    </span>
  ` : emailButtonHtml;

  const hasRefOrLocationRows =
    (Array.isArray(reference_rows) && reference_rows.length > 0)
    || (
      timesheet_reference_sources_by_id
      && typeof timesheet_reference_sources_by_id === 'object'
      && Object.keys(timesheet_reference_sources_by_id).length > 0
    );
  const sourceInvoice = weekly_source_invoice?.is_weekly_source_invoice === true;
  const refDisabledAttr = hasRefOrLocationRows && !sourceInvoice ? '' : 'disabled';
  const refTitle = sourceInvoice
    ? 'title="This ordinary reference editor does not apply to a finalised-source invoice"'
    : (hasRefOrLocationRows ? '' : 'title="No timesheet reference or ward rows on this invoice"');

  const addDisabledAttr = canEditLines ? '' : 'disabled';
  const addDisabledTitle = canEditLines ? '' : 'title="Unissue and unpay the invoice first (staged), then you can edit lines."';

  const invoiceAsyncState = (modalCtx?.invoiceAsync && typeof modalCtx.invoiceAsync === 'object')
    ? modalCtx.invoiceAsync
    : {};
  const documentOperation = (invoiceAsyncState.document_operation && typeof invoiceAsyncState.document_operation === 'object')
    ? invoiceAsyncState.document_operation
    : {};
  const documentOperationStatus = String(
    documentOperation.status
    || documentOperation.operation_status
    || ''
  ).trim().toUpperCase();
  const documentOperationId = String(
    documentOperation.operation_id
    || invoice.active_document_operation_id
    || ''
  ).trim().toLowerCase();
  const activeDocumentStatuses = new Set([
    'PENDING', 'QUEUED', 'CLAIMED', 'RUNNING', 'PROCESSING',
    'RETRY_WAIT', 'WAITING', 'BUILDING', 'RENDERING'
  ]);
  const documentGenerating = activeDocumentStatuses.has(documentOperationStatus)
    || (!!documentOperationId && !['COMPLETE', 'READY', 'FAILED', 'BLOCKED', 'CANCELLED', 'SUPERSEDED'].includes(documentOperationStatus));
  const issuedDocumentAuthority = !!(
    effIssued
    || invoice.issued_at_utc
    || ['ISSUED', 'PAID', 'PART_PAID', 'PARTIALLY_PAID'].includes(String(invoice.status || '').trim().toUpperCase())
  );
  const exactDocumentVersionId = String(
    invoiceAsyncState.document_version_id
    || (issuedDocumentAuthority
      ? invoice.issued_document_version_id
      : invoice.preview_document_version_id)
    || ''
  ).trim().toLowerCase();
  const invoiceUuidRe = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
  const exactDocumentReady = String(invoice.document_state || '').trim().toUpperCase() === 'READY'
    && invoiceUuidRe.test(exactDocumentVersionId)
    && !documentGenerating;
  const documentFailed = ['FAILED', 'BLOCKED', 'CANCELLED', 'SUPERSEDED'].includes(documentOperationStatus);
  const invoicePdfLabel = exactDocumentReady
    ? 'Open invoice PDF'
    : documentGenerating
      ? 'Generate invoice PDF now'
      : documentFailed
        ? 'Retry invoice PDF'
        : 'Generate invoice PDF';
  const invoicePdfDisabled = '';
  const invoicePdfBusy = documentGenerating ? 'true' : 'false';
  const invoicePdfOperationAttr = documentOperationId
    ? ` data-invoice-async-operation-id="${escapeHtml(documentOperationId)}"`
    : '';
  const invoicePdfVersionAttr = exactDocumentReady
    ? ` data-document-version-id="${escapeHtml(exactDocumentVersionId)}"`
    : '';

  const holdReason = (invoice.on_hold_reason != null && String(invoice.on_hold_reason).trim())
    ? String(invoice.on_hold_reason)
    : '';

  // ✅ Additional user-facing message in edit mode when hold blocks issuing.
  const holdIssueBlockMsg = (isEditing && blockIssueWhileHold) ? `
    <div class="alert alert-warning py-2 px-2 small mb-3">
      <b>On hold:</b> Unhold this invoice before issuing.
    </div>
  ` : '';

  return `
    <div class="p-2">

      ${modalCtx.error ? `<div class="alert alert-danger mb-3">${escapeHtml(modalCtx.error)}</div>` : ''}

      <div class="mb-2 d-flex align-items-start justify-content-between gap-3" style="flex-wrap:wrap;">
        <div>
          <div class="h5 fw-bold mb-1">${invoiceNo}</div>
          <div class="fw-bold" style="font-size:1.05rem;">${escapeHtml(String(clientName || ''))}</div>
        </div>

        <div class="d-flex align-items-center gap-2" style="margin-left:auto; flex-wrap:wrap; justify-content:flex-end;">
          ${issuedPill}
          ${paidPill}
          ${holdPill}
          ${overdue ? overduePill : ''}
        </div>
      </div>

      ${holdIssueBlockMsg}

      ${effHold && holdReason ? `
        <div class="alert alert-warning py-2 px-2 small mb-3">
          <b>On hold:</b> ${escapeHtml(holdReason)}
        </div>
      ` : ''}

      <div class="mb-3">
        <table class="grid mini" style="width:100%;">
          <tbody>
            <tr><td style="width:180px;">Invoice date</td><td class="fw-semibold">${escapeHtml(String(invoiceDate))}</td></tr>
            ${weekly_source_invoice?.is_weekly_source_invoice === true ? `<tr><td>Backing report</td><td class="fw-semibold">${escapeHtml((Array.isArray(weekly_source_invoice.report_numbers) && weekly_source_invoice.report_numbers.length) ? weekly_source_invoice.report_numbers.join(', ') : String(weekly_source_invoice.report_number || '—'))}</td></tr>` : ''}
            <tr><td>Created</td><td class="fw-semibold">${escapeHtml(String(createdDate))}</td></tr>
            <tr><td>Amount (ex VAT)</td><td class="fw-semibold">${fmtMoneyLocal(totalsToShow.subtotal_ex_vat)}</td></tr>
            <tr><td>Amount (inc VAT)</td><td class="fw-semibold">${fmtMoneyLocal(totalsToShow.total_inc_vat)}</td></tr>
            <tr><td>Issue Date</td><td class="fw-semibold">${escapeHtml(String(displayIssuedAt))}</td></tr>
            <tr><td>Paid On</td><td class="fw-semibold">${escapeHtml(String(displayPaidAt))}</td></tr>
            <tr><td>On Hold Since</td><td class="fw-semibold">${escapeHtml(String(displayHoldAt))}</td></tr>
          </tbody>
        </table>
      </div>

      <div class="d-flex flex-wrap gap-2 align-items-center mb-3">
        <button type="button" class="btn btn-sm btn-primary" data-action="inv-open-pdf"
          data-invoice-async-family="DOCUMENT"${invoicePdfOperationAttr}${invoicePdfVersionAttr}
          aria-busy="${invoicePdfBusy}" aria-disabled="false" ${invoicePdfDisabled}>
          ${escapeHtml(invoicePdfLabel)}
        </button>

        ${emailActionHtml}

        <button type="button" class="btn btn-sm btn-outline-secondary" data-action="inv-open-reference-numbers" ${refDisabledAttr} ${refTitle}>
          Refs and Ward
        </button>

        ${isEditing && weekly_source_invoice?.is_weekly_source_invoice !== true ? `
          <button type="button" class="btn btn-sm btn-outline-secondary" data-action="inv-add-timesheets" ${addDisabledAttr} ${addDisabledTitle}>
            + Add timesheet
          </button>
          <button type="button" class="btn btn-sm btn-outline-secondary" data-action="inv-add-adjustment" ${addDisabledAttr} ${addDisabledTitle}>
            + Add adjustment
          </button>
        ` : ''}
        ${isEditing && weekly_source_invoice?.is_weekly_source_invoice === true
          && window.CloudTmsSourceInvoiceAdjustments?.context(weekly_source_invoice) ? `
          <button type="button" class="btn btn-sm btn-outline-secondary" data-action="inv-add-adjustment" ${addDisabledAttr}>+ Add adjustment</button>
        ` : ''}
      </div>

      ${isEditing && hasPendingEdits ? `
        <div class="alert alert-warning py-1 px-2 small mb-3">
          You have <b>unsaved changes</b>. Nothing is committed until you press <b>Save</b>.
        </div>
      ` : ''}

      ${renderInvoiceLinesTable(modalCtx, invoiceData, items)}
    </div>
  `;
}




function renderInvoiceLinesTable(modalCtx, invoiceData, items) {
  // Source presentation times are timestamp-without-time-zone values.  Their
  // JSON representation begins with the date, so taking the first five
  // characters would display "2026-" rather than the worked time.
  const fmtSourceLocalTime = (value) => {
    const match = String(value || '').match(/(?:^|[T\s])(\d{2}:\d{2})(?::\d{2})?(?:$|[.Z+-])/);
    return match ? match[1] : '—';
  };
  const fmtMoneyLocal = (n) => {
    const x = invoiceModalRound2(Number(n || 0));
    return `£${x.toFixed(2)}`;
  };

  const invData = (invoiceData && typeof invoiceData === 'object')
    ? invoiceData
    : invoiceModalGetInvoiceData(modalCtx);

  const { invoice, segments_by_timesheet, segments_on_invoice_by_timesheet, tsfin_id_by_timesheet_id } = invData || {};

  const isEditing = !!modalCtx.isEditing;
  const canEditLines = invoiceModalIsEditable(invoice, modalCtx, 'lines');
  const isUnissued = !(invoice && invoice.issued_at_utc);

  const removedSet = modalCtx?.invoiceEdits?.remove_invoice_line_ids || new Set();

  const stagedSegRemoveArr = (modalCtx?.invoiceEdits && Array.isArray(modalCtx.invoiceEdits.remove_segment_refs))
    ? modalCtx.invoiceEdits.remove_segment_refs
    : [];

  const stagedSegAddArr = (modalCtx?.invoiceEdits && Array.isArray(modalCtx.invoiceEdits.add_segment_refs))
    ? modalCtx.invoiceEdits.add_segment_refs
    : [];

  const segKey = (tsfinId, segId) => `${String(tsfinId || '')}::${String(segId || '')}`;

  const stagedSegRemoveSet = (() => {
    const s = new Set();
    for (const r of stagedSegRemoveArr) {
      if (!r || typeof r !== 'object') continue;
      s.add(segKey(r.tsfin_id, r.segment_id));
    }
    return s;
  })();

  const stagedSegAddSet = (() => {
    const s = new Set();
    for (const r of stagedSegAddArr) {
      if (!r || typeof r !== 'object') continue;
      s.add(segKey(r.tsfin_id, r.segment_id));
    }
    return s;
  })();

  modalCtx.invoiceUi = (modalCtx.invoiceUi && typeof modalCtx.invoiceUi === 'object') ? modalCtx.invoiceUi : {};
  modalCtx.invoiceUi.expanded_timesheets = (modalCtx.invoiceUi.expanded_timesheets && typeof modalCtx.invoiceUi.expanded_timesheets === 'object')
    ? modalCtx.invoiceUi.expanded_timesheets
    : {};

  const esc = (typeof escapeHtml === 'function') ? escapeHtml : (s) => String(s == null ? '' : s);

  const fmtDowDmy = (ymd) => {
    const s = String(ymd || '').slice(0, 10);
    const m = s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (!m) return s || '—';
    const [, y, mo, d] = m;
    let dow = '';
    try {
      const dt = new Date(`${s}T00:00:00Z`);
      dow = dt.toLocaleDateString('en-GB', { weekday: 'short', timeZone: 'Europe/London' });
    } catch {}
    return `${dow || ''} ${d}/${mo}/${y}`.trim();
  };

  const weeklySource = invData?.weekly_source_invoice;
  if (weeklySource?.is_weekly_source_invoice === true) {
    // Gate 7 item G7-2.  `shift_groups` no longer exists: the edit-context owner
    // returns `movable_lines`, one element per immutable source PRESENTATION
    // line, each carrying the presentation-line id and the expected
    // presentation hash the move owner demands
    // (backend\supabase\repeatable\15092026_1534_weekly_source_invoice_batch_integration_v1.sql:588).
    const sourceRows = Array.isArray(weeklySource.movable_lines) ? weeklySource.movable_lines : [];
    const timesheetByInvoiceLine = new Map((Array.isArray(items) ? items : [])
      .map(item => [String(item?.invoice_line_id || '').trim(), String(item?.timesheet_id || '').trim()])
      .filter(([lineId, timesheetId]) => lineId && timesheetId));
    const destinations = Array.isArray(weeklySource.compatible_destinations)
      ? weeklySource.compatible_destinations : [];
    const canMoveSource = isEditing && weeklySource.editable === true && destinations.length > 0;
    const sourceBody = sourceRows.map((row) => {
      const presentationLineId = String(row?.presentation_line_id || '').trim();
      // The server certifies a single physical root for this presentation.
      // An invoice-line shortcut is unsafe for a multi-movement net line.
      const timesheetId = String(row?.timesheet_id || '').trim()
        || (Number(row?.bound_movement_count) === 1
          ? timesheetByInvoiceLine.get(String(row?.invoice_line_id || '').trim()) || '' : '');
      const presentationHash = String(row?.presentation_hash || '').trim().toLowerCase();
      const start = fmtSourceLocalTime(row?.start_at_local);
      const end = fmtSourceLocalTime(row?.end_at_local);
      const breakMinutes = Math.max(0, Number(row?.break_minutes || 0));
      const selectorId = `weeklySourceMove_${presentationLineId.replace(/[^a-z0-9_-]/gi, '')}`;
      // A source-fixed expense follows its declared companion and is never
      // selected on its own; a non-NHSP net presentation binds more than one
      // movement and moves as one.  Both facts are the server's, not derived.
      const followsCompanion = String(row?.follows_companion_presentation_line_id || '').trim();
      const independentlyMovable = row?.independently_movable === true && !followsCompanion;
      const indivisible = row?.indivisible_presentation === true;
      const movable = canMoveSource && independentlyMovable
        && !!presentationLineId && /^[0-9a-f]{64}$/.test(presentationHash);
      const options = destinations.map(destination => {
        const label = [
          destination.invoice_number || 'Draft invoice',
          destination.finalisation_week_ending ? `week ending ${destination.finalisation_week_ending}` : '',
          Array.isArray(destination.report_numbers) && destination.report_numbers.length
            ? `report ${destination.report_numbers.join(', ')}` : ''
        ].filter(Boolean).join(' · ');
        return `<option value="${esc(String(destination.invoice_id || ''))}" data-document-revision="${esc(String(destination.document_revision || ''))}">${esc(label)}</option>`;
      }).join('');
      const move = movable
        ? `<div class="d-flex gap-2 justify-content-end"><select class="input" id="${esc(selectorId)}" aria-label="Destination invoice"><option value="">Choose invoice</option>${options}</select><button type="button" class="btn btn-sm btn-outline-secondary" data-action="inv-move-weekly-source-shift" data-presentation-line-id="${esc(presentationLineId)}" data-presentation-hash="${esc(presentationHash)}" data-destination-select-id="${esc(selectorId)}">Move</button></div>`
        : (isEditing && weeklySource.editable === true
          ? (followsCompanion
            ? '<span class="text-muted small">Moves with the line it belongs to</span>'
            : (destinations.length === 0
              ? '<span class="text-muted small">No compatible unissued invoice</span>'
              : '<span class="text-muted small">Cannot be moved on its own</span>'))
          : '');
      const isSourceExpense = String(row?.line_kind || '').toUpperCase() === 'SOURCE_FIXED_EXPENSE';
      const hoursCell = isSourceExpense
        ? '<span class="text-muted small">Source expense</span>'
        : (row?.work_date || row?.start_at_local)
        ? esc(`${start}–${end}`)
        : '<span class="text-muted small">Client-provided expense</span>';
      const sourceDetail = [
        row?.source_reference ? `Ref ${row.source_reference}` : '',
        row?.source_trust ? `Trust ${row.source_trust}` : '',
        row?.source_hospital ? `Hospital ${row.source_hospital}` : '',
        row?.source_ward ? `Ward ${row.source_ward}` : '',
        row?.source_band ? `Band ${row.source_band}` : ''
      ].filter(Boolean).map(value => `<span>${esc(value)}</span>`).join('');
      return `<tr data-presentation-line-id="${esc(presentationLineId)}">
        <td data-label="Date">${row?.work_date ? esc(fmtDowDmy(row?.work_date)) : '—'}</td>
        <td data-label="Candidate">${esc(String(row?.candidate || '—'))}</td>
        <td data-label="Source details"><div class="ws-invoice-source-details">${sourceDetail || '—'}</div></td>
        <td data-label="Hours">${hoursCell}</td>
        <td data-label="Break">${!isSourceExpense && row?.work_date ? `${breakMinutes.toLocaleString('en-GB')} min` : '—'}</td>
        <td data-label="Status">${row?.released_after_dispute === true ? '<span class="pill pill-warn">Released after dispute</span>' : '<span class="pill pill-ok">Final source</span>'}${indivisible ? '<span class="pill pill-info">Moves as one line</span>' : ''}${timesheetId ? `<button type="button" class="btn btn-sm btn-outline-secondary ms-2" data-action="inv-open-weekly-source-timesheet" data-timesheet-id="${esc(timesheetId)}" aria-label="Open related Timesheet for ${esc(String(row?.candidate || 'candidate'))} on ${esc(fmtDowDmy(row?.work_date))}">View Timesheet</button>` : ''}</td>
        ${isEditing ? `<td data-label="Move" class="text-end">${move}</td>` : ''}
      </tr>`;
    }).join('');
    const reportNumbers = Array.isArray(weeklySource.report_numbers) && weeklySource.report_numbers.length
      ? weeklySource.report_numbers.join(', ')
      : String(weeklySource.report_number || '');
    return `<div>
      <div class="d-flex align-items-center justify-content-between flex-wrap gap-2 mb-2">
        <div><span class="fw-semibold">Finalised source lines</span>${reportNumbers ? `<span class="text-muted small"> · Backing report ${esc(reportNumbers)}</span>` : ''}</div>
      </div>
      <table class="grid ws-source-invoice-lines" style="width:100%">
        <thead><tr><th>Date</th><th>Candidate</th><th>Source details</th><th>Hours</th><th>Break</th><th>Status</th>${isEditing ? '<th class="text-end">Move to another unissued invoice</th>' : ''}</tr></thead>
        <tbody>${sourceBody || `<tr><td colspan="${isEditing ? 7 : 6}" class="text-muted">No source lines remain on this invoice.</td></tr>`}</tbody>
      </table>
      ${window.CloudTmsSourceInvoiceAdjustments?.rows(items,modalCtx.invoiceEdits?.add_adjustments,
        removedSet,weeklySource,isEditing,esc,fmtMoneyLocal) || ''}
    </div>`;
  }

  const isoToHHMM = (iso) => {
    try {
      const d = new Date(String(iso || ''));
      if (Number.isNaN(d.getTime())) return '';
      const t = d.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', hour12: false, timeZone: 'Europe/London' });
      return String(t || '').replace(':', '');
    } catch { return ''; }
  };

  const isoToDowDmy = (iso) => {
    try {
      const d = new Date(String(iso || ''));
      if (Number.isNaN(d.getTime())) return '';
      const parts = new Intl.DateTimeFormat('en-GB', { timeZone: 'Europe/London', year: 'numeric', month: '2-digit', day: '2-digit', weekday: 'short' }).formatToParts(d);
      const get = (t) => (parts.find(p => p.type === t)?.value || '');
      const dd = get('day'), mm = get('month'), yy = get('year'), wd = get('weekday');
      return `${wd} ${dd}/${mm}/${yy}`.trim();
    } catch { return ''; }
  };

  const asLineType = (it) => {
    const mj = (it && it.meta_json && typeof it.meta_json === 'object') ? it.meta_json : {};
    return String(it?.line_type_norm || mj?.line_type || '').trim().toUpperCase();
  };

  const isExpenseLineType = (lt) => {
    const t = String(lt || '').toUpperCase();
    return (t === 'EXPENSE' || t.startsWith('EXPENSE_') || t === 'TRAVEL' || t === 'ACCOMMODATION' || t === 'OTHER');
  };

  const isMileageLineType = (lt) => String(lt || '').toUpperCase() === 'MILEAGE';

  const isAdditionalRateType = (lt) => {
    const t = String(lt || '').toUpperCase();
    return (t === 'ADDITIONAL_RATE' || t === 'ADDITIONAL' || t === 'ADDITIONAL_RATES' || t.includes('_ADDITIONAL_') || (t.startsWith('TS_') && t.includes('ADDITIONAL')));
  };

  const segmentBlock = (() => {
    let hasAdditional = false;
    let hasExpense = false;
    let hasMileage = false;

    for (const l of (items || [])) {
      const id = String(l?.invoice_line_id || '');
      if (id && removedSet && typeof removedSet.has === 'function' && removedSet.has(id)) continue;

      const lt = asLineType(l);
      if (!hasAdditional && isAdditionalRateType(lt)) hasAdditional = true;
      if (!hasMileage && isMileageLineType(lt)) hasMileage = true;
      if (!hasExpense && isExpenseLineType(lt)) hasExpense = true;

      if (hasAdditional && (hasExpense || hasMileage)) break;
    }

    return { hasAdditional, hasExpense, hasMileage, blocked: (hasAdditional || hasExpense || hasMileage) };
  })();

  const getWeekEndingRaw = (g) => {
    const first = g.lines[0] || {};
    const mj = (first.meta_json && typeof first.meta_json === 'object') ? first.meta_json : {};
    return (mj.week_ending_date || mj.week_ending_date_local || mj.week_ending || mj.week_end_date || '');
  };

  const getWeekEndingLabel = (g) => {
    const raw = String(getWeekEndingRaw(g) || '').slice(0, 10);
    if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return fmtDowDmy(raw);
    return raw || '—';
  };

  const getCandidate = (g) => {
    const first = g.lines[0] || {};
    const mj = (first.meta_json && typeof first.meta_json === 'object') ? first.meta_json : {};
    return (mj.candidate_display || mj.candidate_name || mj.worker_name || '');
  };

  const sumBucketQty = (it) => {
    const q = it?.qty || {};
    return (Number(q.day || 0) + Number(q.night || 0) + Number(q.sat || 0) + Number(q.sun || 0) + Number(q.bh || 0));
  };

  const sumSegHours = (seg) => (
    Number(seg?.hours_day || 0) +
    Number(seg?.hours_night || 0) +
    Number(seg?.hours_sat || 0) +
    Number(seg?.hours_sun || 0) +
    Number(seg?.hours_bh || 0)
  );

  const groupTotals = (g) => {
    let hrs = 0;
    let ex = 0;
    let inc = 0;
    let margin = 0;
    let expensesEx = 0;
    let additionalHours = 0;

    for (const l of g.lines) {
      const lt = asLineType(l);
      const id = String(l?.invoice_line_id || '');
      const isRemoved = (id && removedSet && typeof removedSet.has === 'function' && removedSet.has(id));
      if (isRemoved) continue;

      const qtyHrs = sumBucketQty(l);
      if (qtyHrs > 0) hrs += qtyHrs;

      ex += Number(l?.total_charge_ex_vat || 0);
      inc += Number(l?.total_inc_vat || 0);
      margin += Number(l?.margin_ex_vat || 0);

      if (isExpenseLineType(lt) && !isMileageLineType(lt)) expensesEx += Number(l?.total_charge_ex_vat || 0);

      if (isAdditionalRateType(lt)) {
        const mj = (l.meta_json && typeof l.meta_json === 'object') ? l.meta_json : {};
        const u = (mj.units && typeof mj.units === 'object') ? mj.units : {};
        const unitName = String(u.unit_name || '').toLowerCase();
        const unitCount = Number(u.unit_count || 0);
        if (unitCount > 0 && unitName && unitName.includes('hour')) additionalHours += unitCount;
      }
    }

    // subtract staged removed segments (keeps preview coherent)
    try {
      const tsId = String(g.timesheet_id || '');
      const segMap = (segments_on_invoice_by_timesheet && typeof segments_on_invoice_by_timesheet === 'object')
        ? segments_on_invoice_by_timesheet
        : ((segments_by_timesheet && typeof segments_by_timesheet === 'object') ? segments_by_timesheet : {});
      const info = (tsId && segMap && typeof segMap === 'object') ? segMap[tsId] : null;

      const invoicedSegs = (info && typeof info === 'object' && Array.isArray(info.invoiced_segments)) ? info.invoiced_segments : null;

      if (invoicedSegs && invoicedSegs.length) {
        const tsfinId = (info && info.tsfin_id) ? String(info.tsfin_id) : (tsfin_id_by_timesheet_id && tsfin_id_by_timesheet_id[tsId] ? String(tsfin_id_by_timesheet_id[tsId]) : '');
        if (tsfinId) {
          let deltaEx = 0;
          let deltaPay = 0;
          let deltaHours = 0;

          for (const seg of invoicedSegs) {
            if (!seg || typeof seg !== 'object') continue;
            const segId = (seg.segment_id != null) ? String(seg.segment_id) : '';
            if (!segId) continue;
            if (stagedSegRemoveSet.has(segKey(tsfinId, segId))) {
              deltaEx += Number(seg.charge_amount || 0);
              deltaPay += Number(seg.pay_amount || 0);
              deltaHours += sumSegHours(seg);
            }
          }

          const vatFactor = (ex && Number(ex) !== 0) ? (Number(inc || 0) / Number(ex || 1)) : 1;
          const deltaInc = deltaEx * vatFactor;
          const deltaMargin = (deltaEx - deltaPay);

          hrs -= deltaHours;
          ex -= deltaEx;
          inc -= deltaInc;
          margin -= deltaMargin;
        }
      }
    } catch {}

    return {
      week: getWeekEndingLabel(g),
      candidate: getCandidate(g),
      totalHours: invoiceModalRound2(hrs),
      additionalHours: invoiceModalRound2(additionalHours),
      expensesEx: invoiceModalRound2(expensesEx),
      ex: invoiceModalRound2(ex),
      inc: invoiceModalRound2(inc),
      margin: invoiceModalRound2(margin)
    };
  };

  const isGroupRemoved = (g) => {
    const ids = g.lines.map(l => String(l?.invoice_line_id || '')).filter(Boolean);
    if (!ids.length) return false;
    return ids.every(id => removedSet.has(id));
  };

  // Group invoice lines by timesheet_id; adjustments separate
  const tsGroups = new Map();
  const adjLines = [];

  for (const it of (items || [])) {
    const tsId = it?.timesheet_id ? String(it.timesheet_id) : null;
    const lt = asLineType(it);

    if (tsId) {
      if (!tsGroups.has(tsId)) tsGroups.set(tsId, { key: tsId, timesheet_id: tsId, lines: [] });
      tsGroups.get(tsId).lines.push(it);
    } else {
      const lid = String(it?.invoice_line_id || '');
      adjLines.push({ key: `adj:${lid}`, invoice_line_id: lid, line: it, line_type: lt, staged: false });
    }
  }

  const tsGroupArr = Array.from(tsGroups.values());

  const segMapForUi = (() => {
    if (segments_on_invoice_by_timesheet && typeof segments_on_invoice_by_timesheet === 'object') return segments_on_invoice_by_timesheet;
    if (segments_by_timesheet && typeof segments_by_timesheet === 'object') return segments_by_timesheet;
    return {};
  })();

  const hasAnySegmentTimesheets = (() => {
    try {
      for (const g of tsGroupArr) {
        const info = segMapForUi[String(g.timesheet_id || '')];
        const segs = (info && typeof info === 'object' && Array.isArray(info.invoiced_segments)) ? info.invoiced_segments : null;
        if (segs && segs.length) return true;
      }
    } catch {}
    return false;
  })();

  // Staged adjustments: render inside Adjustments table (green), not a separate "Pending" block.
  const stagedAdjs = (modalCtx?.invoiceEdits && Array.isArray(modalCtx.invoiceEdits.add_adjustments))
    ? modalCtx.invoiceEdits.add_adjustments
    : [];

  const stagedAdjRows = stagedAdjs.map((a) => {
    const tok = String(a?.client_token || '').trim();
    return {
      key: `staged-adj:${tok}`,
      client_token: tok,
      description: String(a?.description || '').trim() || 'Adjustment',
      ex: invoiceModalRound2(Number(a?.amount_ex_vat || 0)),
      staged: true
    };
  });

  const allocByWeights = (total, weights) => {
    const out = {};
    const keys = Object.keys(weights || {}).filter(k => Number(weights[k] || 0) > 0);
    if (!keys.length) return out;

    const sumW = keys.reduce((s, k) => s + Number(weights[k] || 0), 0);
    if (!(sumW > 0)) return out;

    let used = 0;
    keys.forEach((k) => {
      const w = Number(weights[k] || 0);
      const raw = (total * w) / sumW;
      const val = invoiceModalRound2(raw);
      out[k] = val;
      used += val;
    });

    const remainder = invoiceModalRound2(total - used);
    const lastKey = keys[keys.length - 1];
    out[lastKey] = invoiceModalRound2((out[lastKey] || 0) + remainder);
    return out;
  };

const buildHourBucketRows = (l) => {
  const q = l?.qty || {};
  const pr = l?.pay_rate || {};
  const cr = l?.charge_rate || {};

  const buckets = [['day','Day'],['night','Night'],['sat','Sat'],['sun','Sun'],['bh','BH']];

  const qtyBy = {};
  const wChg = {};
  const wPay = {};

  for (const [k] of buckets) {
    const qty = Number(q?.[k] || 0);
    if (qty > 0) {
      qtyBy[k] = qty;
      const chgR = Number(cr?.[k]);
      const payR = Number(pr?.[k]);
      wChg[k] = (Number.isFinite(chgR) ? (qty * chgR) : qty);
      wPay[k] = (Number.isFinite(payR) ? (qty * payR) : qty);
    }
  }

  const totalPay = invoiceModalRound2(Number(l?.total_pay_ex_vat || 0));
  const totalChg = invoiceModalRound2(Number(l?.total_charge_ex_vat || 0));

  const allocPay = allocByWeights(totalPay, wPay);
  const allocChg = allocByWeights(totalChg, wChg);

  const rows = [];
  for (const [k, label] of buckets) {
    const qty = Number(qtyBy[k] || 0);
    if (!(qty > 0)) continue;

    const payEx = invoiceModalRound2(Number(allocPay[k] || 0));
    const chgEx = invoiceModalRound2(Number(allocChg[k] || 0));

    rows.push({
      unit: label,
      qty: qty,
      pay_ex: payEx,
      chg_ex: chgEx
    });
  }

  return rows;
};

const buildNonHourRow = (l) => {
  const lt = asLineType(l);
  const mj = (l.meta_json && typeof l.meta_json === 'object') ? l.meta_json : {};
  const u = (mj.units && typeof mj.units === 'object') ? mj.units : {};
  const bucket = (mj.bucket && typeof mj.bucket === 'object') ? mj.bucket : {};

  const unitName =
    bucket.name ||
    u.unit_name ||
    ((isExpenseLineType(lt) || isMileageLineType(lt)) ? lt : (lt || (l.description || '')));

  const qty =
    (Number.isFinite(Number(u.unit_count)) && Number(u.unit_count) !== 0)
      ? Number(u.unit_count)
      : null;

  const payEx = invoiceModalRound2(Number(l?.total_pay_ex_vat || 0));
  const chgEx = invoiceModalRound2(Number(l?.total_charge_ex_vat || 0));

  const shouldShow =
    ((isExpenseLineType(lt) || isMileageLineType(lt)) ? (payEx !== 0 || chgEx !== 0) : ((qty && qty !== 0) || payEx !== 0 || chgEx !== 0));

  if (!shouldShow) return null;

  return {
    unit: String(unitName || ''),
    qty: qty,
    pay_ex: payEx,
    chg_ex: chgEx
  };
};


  const lookupEligibleSeg = (tsfinId, segId) => {
    try {
      const obj = modalCtx?.eligibleTimesheetsCache;
      const map = obj?._bySegKey instanceof Map ? obj._bySegKey : null;
      if (!map) return null;
      return map.get(`${String(tsfinId || '')}::${String(segId || '')}`) || null;
    } catch { return null; }
  };

  const renderSegmentsOnInvoiceTable = (g) => {
    const tsId = String(g.timesheet_id || '');
    const info = (segMapForUi && typeof segMapForUi === 'object') ? segMapForUi[tsId] : null;
    const invoicedSegs = (info && typeof info === 'object' && Array.isArray(info.invoiced_segments)) ? info.invoiced_segments : [];

    const tsfinId =
      (info && typeof info === 'object' && info.tsfin_id) ? String(info.tsfin_id)
      : (tsfin_id_by_timesheet_id && tsfin_id_by_timesheet_id[tsId] ? String(tsfin_id_by_timesheet_id[tsId]) : '');

    // staged added segments for this timesheet
    const stagedAddsForTs = (() => {
      const out = [];
      for (const r of stagedSegAddArr) {
        if (!r || typeof r !== 'object') continue;
        const tId = String(r.timesheet_id || '').trim();
        if (tId && tId !== tsId) continue;
        if (!tId) {
          // fallback: infer from segment_id pattern "...:<timesheet_id>:..."
          const sid = String(r.segment_id || '');
          if (sid && tsId && sid.includes(`:${tsId}:`) === false && sid.includes(`:${tsId}`) === false) continue;
        }
        out.push(r);
      }
      return out;
    })();

    const stagedAddSegs = stagedAddsForTs.map(r => {
      const s = lookupEligibleSeg(r.tsfin_id, r.segment_id) || {};
      return {
        __staged_add: true,
        segment_id: String(r.segment_id || ''),
        date: String(s.date || ''),
        start_utc: String(s.start_utc || s.start || ''),
        end_utc: String(s.end_utc || s.end || ''),
        ref_num: String(s.ref_num || ''),
        charge_amount: Number(s.charge_amount || 0),
        pay_amount: Number(s.pay_amount || 0),
        hours_day: Number(s.hours_day || 0),
        hours_night: Number(s.hours_night || 0),
        hours_sat: Number(s.hours_sat || 0),
        hours_sun: Number(s.hours_sun || 0),
        hours_bh: Number(s.hours_bh || 0)
      };
    });

    if (!invoicedSegs.length && !stagedAddSegs.length) return '';

    const uninvoicedCount = (info && typeof info === 'object' && Number.isFinite(Number(info.uninvoiced_segment_count)))
      ? Number(info.uninvoiced_segment_count)
      : 0;

    const lockedElsewhereCount = (info && typeof info === 'object' && Number.isFinite(Number(info.locked_elsewhere_segment_count)))
      ? Number(info.locked_elsewhere_segment_count)
      : 0;

    const canStageSegments =
      !!isEditing &&
      !!canEditLines &&
      !!isUnissued &&
      !segmentBlock.blocked;

    const disabledReason = (() => {
      if (!isEditing) return 'Not in edit mode';
      if (!canEditLines) return 'Invoice lines are not editable';
      if (!isUnissued) return 'Segments can only be changed before issuing';
      if (segmentBlock.hasAdditional) return 'Segments cannot be moved when additional rates exist';
      if (segmentBlock.hasMileage || segmentBlock.hasExpense) return 'Segments cannot be moved when expenses or mileage exist';
      return '';
    })();

    const banner = (!canStageSegments && isEditing && (segmentBlock.blocked || !isUnissued)) ? `
      <div class="alert alert-warning py-2 px-2 small mb-2">
        <b>Segment edits disabled:</b> ${esc(disabledReason || 'Not allowed for this invoice.')}
      </div>
    ` : '';

    const combined = [
      ...invoicedSegs.map(s => ({ ...(s || {}), __staged_add: false })),
      ...stagedAddSegs
    ];

    // sort by start time then date
    combined.sort((a, b) => {
      const ta = new Date(String(a?.start_utc || '')).getTime();
      const tb = new Date(String(b?.start_utc || '')).getTime();
      if (Number.isFinite(ta) && Number.isFinite(tb) && ta !== tb) return ta - tb;

      const da = String(a?.date || '').slice(0,10);
      const db = String(b?.date || '').slice(0,10);
      if (da && db && da !== db) return da < db ? -1 : 1;
      return 0;
    });

    const rows = combined.map(seg => {
      const segId = (seg && typeof seg === 'object' && seg.segment_id != null) ? String(seg.segment_id) : '';
      const startUtc = (seg && typeof seg === 'object' && seg.start_utc != null) ? String(seg.start_utc) : '';
      const endUtc = (seg && typeof seg === 'object' && seg.end_utc != null) ? String(seg.end_utc) : '';

      const dateLabel = (() => {
        const raw = String(seg?.date || '').slice(0, 10);
        if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return fmtDowDmy(raw);
        const fromIso = isoToDowDmy(startUtc);
        return fromIso || '—';
      })();

      const startHH = isoToHHMM(startUtc);
      const endHH = isoToHHMM(endUtc);

      const refNum = (seg && typeof seg === 'object' && seg.ref_num != null) ? String(seg.ref_num) : '';
      const hrs = invoiceModalRound2(sumSegHours(seg));
      const chg = invoiceModalRound2(Number(seg?.charge_amount || 0));
      const pay = invoiceModalRound2(Number(seg?.pay_amount || 0));

      const k = segKey(tsfinId, segId);
      const stagedRemove = (!!tsfinId && !!segId && stagedSegRemoveSet.has(k));
      const stagedAdd = !!seg.__staged_add;

      const checkbox = stagedAdd
        ? `
          <input type="checkbox"
            class="form-check-input"
            data-action="inv-toggle-add-segment"
            data-timesheet-id="${esc(tsId)}"
            data-tsfin-id="${esc(String(tsfinId || ''))}"
            data-segment-id="${esc(String(segId || ''))}"
            checked
            ${canStageSegments ? '' : 'disabled'}
          />
        `
        : `
          <input type="checkbox"
            class="form-check-input"
            data-action="inv-toggle-remove-segment"
            data-timesheet-id="${esc(tsId)}"
            data-tsfin-id="${esc(String(tsfinId || ''))}"
            data-segment-id="${esc(String(segId || ''))}"
            ${stagedRemove ? 'checked' : ''}
            ${canStageSegments ? '' : 'disabled'}
          />
        `;

      const chip = stagedAdd
        ? `<span class="pill pill-ok">Staged add</span>`
        : (stagedRemove ? `<span class="pill pill-warn">Pending removal</span>` : '');

      const rowStyle = stagedAdd
        ? ` style="background: rgba(46, 204, 113, 0.18);"`
        : (stagedRemove ? ` class="opacity-50"` : '');

      return `
        <tr${rowStyle}>
          <td style="width:32px;">${checkbox}</td>
          <td>${esc(dateLabel)}</td>
          <td>${esc(startHH || '—')}</td>
          <td>${esc(endHH || '—')}</td>
          <td>${esc(refNum || '—')}</td>
          <td class="text-end">${esc(invoiceModalFmtHours(hrs))}</td>
          <td class="text-end">${fmtMoneyLocal(chg)}</td>
          <td class="text-end">${fmtMoneyLocal(pay)}</td>
          <td class="text-end">${chip}</td>
        </tr>
      `;
    }).join('');

     return `
      <div class="mt-3">
        <div class="d-flex align-items-center justify-content-between flex-wrap gap-2 mb-2">
          <div class="fw-semibold">
            Segments
            <span class="text-muted small">
              (${invoicedSegs.length} on invoice, ${uninvoicedCount} uninvoiced, ${lockedElsewhereCount} locked elsewhere)
            </span>
          </div>
        </div>

        ${banner}

        <table class="grid" style="width:100%">
          <thead>
            <tr>
              <th style="width:32px;" class="text-center">
                <input type="checkbox"
                  class="form-check-input"
                  title="Select / deselect all"
                  ${canStageSegments ? '' : 'disabled'}
                  onchange="(function(cb){ try{ var tbl=cb.closest('table'); if(!tbl) return; var qs=tbl.querySelectorAll('tbody input[type=&quot;checkbox&quot;].form-check-input[data-action]'); for (var i=0;i<qs.length;i++){ var x=qs[i]; if(!x || x.disabled) continue; x.checked = cb.checked; try{ x.dispatchEvent(new Event('change',{bubbles:true})); }catch(e){} } }catch(e){} })(this)"
                />
              </th>
              <th>Date</th>
              <th>Start</th>
              <th>End</th>
              <th>Ref</th>
              <th class="text-end">Hours</th>
              <th class="text-end">Charge (ex VAT)</th>
              <th class="text-end">Pay (ex VAT)</th>
              <th class="text-end"></th>
            </tr>
          </thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
    `;

  };

  const renderExpandedTable = (g) => {
    const rows = [];

    for (const l of g.lines) {
      const id = String(l?.invoice_line_id || '');
      if (id && removedSet && typeof removedSet.has === 'function' && removedSet.has(id)) continue;

      const qtyHrs = sumBucketQty(l);

      if (qtyHrs > 0) {
        const parts = buildHourBucketRows(l);
        parts.forEach(r => rows.push(r));
        continue;
      }

      const r = buildNonHourRow(l);
      if (r) rows.push(r);
    }

 const breakdownHtml = (!rows.length)
  ? `<div class="text-muted small">No billable lines.</div>`
  : `
    <div class="mt-2">
      <table class="grid" style="width:100%">
        <thead>
          <tr>
            <th>Unit</th>
            <th class="text-end">Qty</th>
            <th class="text-end">Pay (ex VAT)</th>
            <th class="text-end">Charge (ex VAT)</th>
          </tr>
        </thead>
        <tbody>
          ${rows.map(r => {
            const qtyTxt = (r.qty == null) ? '—' : invoiceModalFmtHours(Number(r.qty || 0));
            return `
              <tr>
                <td>${esc(String(r.unit || ''))}</td>
                <td class="text-end">${esc(String(qtyTxt))}</td>
                <td class="text-end">${fmtMoneyLocal(r.pay_ex)}</td>
                <td class="text-end">${fmtMoneyLocal(r.chg_ex)}</td>
              </tr>
            `;
          }).join('')}
        </tbody>
      </table>
    </div>
  `;


    const segmentsHtml = renderSegmentsOnInvoiceTable(g);

    return `${breakdownHtml}${segmentsHtml}`;
  };

  const renderRemoveBtnForTimesheet = (g, removed) => {
    if (!isEditing) return '';

    if (removed) {
      return `
        <button type="button"
          class="btn btn-sm btn-outline-secondary"
          data-action="inv-toggle-remove-timesheet"
          data-timesheet-id="${esc(g.timesheet_id)}">
          Undo
        </button>
      `;
    }

    if (!canEditLines) return '';

    return `
      <button type="button"
        class="btn btn-sm btn-outline-danger"
        data-action="inv-toggle-remove-timesheet"
        data-timesheet-id="${esc(g.timesheet_id)}">
        Remove
      </button>
    `;
  };

  const stagedBlocks = [];

  if (isEditing && hasAnySegmentTimesheets && (segmentBlock.blocked || !isUnissued)) {
    const reason = segmentBlock.hasAdditional
      ? 'Segments cannot be moved when additional rates exist.'
      : ((segmentBlock.hasMileage || segmentBlock.hasExpense)
          ? 'Segments cannot be moved when expenses or mileage exist.'
          : (!isUnissued ? 'Segments can only be changed before issuing.' : ''));
    stagedBlocks.push(`
      <div class="alert alert-warning py-2 px-2 small mb-3">
        <b>Segment edits disabled:</b> ${esc(reason)}
      </div>
    `);
  }

  const head = `
    <thead>
      <tr>
        <th>Week ending</th>
        <th>Candidate</th>
        <th class="text-end">Total hours</th>
        <th class="text-end">Additional hours</th>
        <th class="text-end">Expenses</th>
        <th class="text-end">Cost (ex VAT)</th>
        <th class="text-end">Cost (inc VAT)</th>
        <th class="text-end">Margin</th>
        ${isEditing ? '<th class="text-end">Actions</th>' : ''}
      </tr>
    </thead>
  `;

  const bodyRows = tsGroupArr.map(g => {
    const totals = groupTotals(g);
    const removed = isGroupRemoved(g);
    const expanded = !!modalCtx.invoiceUi.expanded_timesheets[String(g.timesheet_id)];

    const trMain = `
      <tr data-action="inv-toggle-expand-timesheet" data-timesheet-id="${esc(g.timesheet_id)}" style="cursor:pointer;">
        <td>${esc(String(totals.week || ''))}</td>
        <td>${esc(String(totals.candidate || ''))}</td>
        <td class="text-end">${esc(invoiceModalFmtHours(totals.totalHours))}</td>
        <td class="text-end">${esc(invoiceModalFmtHours(totals.additionalHours))}</td>
        <td class="text-end">${fmtMoneyLocal(totals.expensesEx)}</td>
        <td class="text-end">${fmtMoneyLocal(totals.ex)}</td>
        <td class="text-end">${fmtMoneyLocal(totals.inc)}</td>
        <td class="text-end">${fmtMoneyLocal(totals.margin)}</td>
        ${isEditing ? `<td class="text-end">${renderRemoveBtnForTimesheet(g, removed)}</td>` : ''}
      </tr>
    `;

    const trExpanded = expanded ? `
      <tr>
        <td colspan="${isEditing ? 9 : 8}">
          ${renderExpandedTable(g)}
        </td>
      </tr>
    ` : '';

    return trMain + trExpanded;
  }).join('');

  const adjTable = (() => {
    const allAdj = [...stagedAdjRows, ...adjLines];

    if (!allAdj.length) return '';

    const rows = allAdj.map(a => {
      const isStaged = !!a.staged;

      if (isStaged) {
        const desc = String(a.description || 'Adjustment');
        const ex = invoiceModalRound2(Number(a.ex || 0));

        const btn = (!isEditing)
          ? ''
          : `
            <button type="button" class="btn btn-sm btn-outline-danger"
              data-action="inv-remove-staged-adjustment"
              data-client-token="${esc(String(a.client_token || ''))}">
              Remove
            </button>
          `;

        return `
          <tr style="background: rgba(46, 204, 113, 0.18);">
            <td>${esc(desc)} <span class="pill pill-ok" style="margin-left:6px;">Staged</span></td>
            <td class="text-end">${fmtMoneyLocal(ex)}</td>
            <td class="text-end">—</td>
            <td class="text-end">—</td>
            ${isEditing ? `<td class="text-end">${btn}</td>` : ''}
          </tr>
        `;
      }

      const l = a.line || {};
      const lid = String(l?.invoice_line_id || a.invoice_line_id || '');
      const desc = String(l?.description || 'Adjustment');
      const ex = invoiceModalRound2(Number(l?.total_charge_ex_vat || 0));
      const inc = invoiceModalRound2(Number(l?.total_inc_vat || 0));
      const margin = invoiceModalRound2(Number(l?.margin_ex_vat || 0));

      const removed = removedSet.has(lid);

      const btn = (!isEditing)
        ? ''
        : (removed
          ? `
            <button type="button"
              class="btn btn-sm btn-outline-secondary"
              data-action="inv-toggle-remove-line"
              data-invoice-line-id="${esc(lid)}">
              Undo
            </button>
          `
          : (canEditLines ? `
            <button type="button"
              class="btn btn-sm btn-outline-danger"
              data-action="inv-toggle-remove-line"
              data-invoice-line-id="${esc(lid)}">
              Remove
            </button>
          ` : '')
        );

      return `
        <tr class="${removed ? 'opacity-50' : ''}">
          <td>${esc(desc)}</td>
          <td class="text-end">${fmtMoneyLocal(ex)}</td>
          <td class="text-end">${fmtMoneyLocal(inc)}</td>
          <td class="text-end">${fmtMoneyLocal(margin)}</td>
          ${isEditing ? `<td class="text-end">${btn}</td>` : ''}
        </tr>
      `;
    }).join('');

    return `
      <div class="mt-3">
        <div class="fw-semibold mb-2">Adjustments</div>
        <table class="grid" style="width:100%">
          <thead>
            <tr>
              <th>Description</th>
              <th class="text-end">Cost (ex VAT)</th>
              <th class="text-end">Cost (inc VAT)</th>
              <th class="text-end">Margin</th>
              ${isEditing ? '<th class="text-end">Actions</th>' : ''}
            </tr>
          </thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
    `;
  })();

  if (!tsGroupArr.length && !adjLines.length && stagedAdjRows.length === 0 && stagedBlocks.length === 0) {
    return `<div class="text-muted">No invoice lines.</div>`;
  }

  return `
    <div>
      ${stagedBlocks.join('')}
      <div class="fw-semibold mb-2">Timesheets</div>
      <table class="grid" style="width:100%">
        ${head}
        <tbody>${bodyRows}</tbody>
      </table>
      ${adjTable}
    </div>
  `;
}


