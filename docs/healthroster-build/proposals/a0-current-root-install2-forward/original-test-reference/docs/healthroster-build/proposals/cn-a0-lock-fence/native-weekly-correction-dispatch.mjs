// Bounded ordinary-worker read-only route extraction; calculators stay with their native owner.
  const boolish = (v) => {
    if (v === true) return true;
    if (v === false) return false;
    if (v == null) return false;
    const s = String(v).trim().toLowerCase();
    return (s === 'true' || s === '1' || s === 'yes' || s === 'y' || s === 'on');
  };


export function deriveNativeWeeklyCorrectionRoute(ts,curFin,effFlags) {
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


  return {routeType,basis,isNhspRoute,isHrRoute,isNhspBasis,isHrBasis};
}
export async function buildNativeWeeklyCorrectionSnapshot(input,owners) {
  const {env,ts,curFin,effFlags,cw,contract,policySql,routeType,basis,isNhspRoute,isHrRoute,isNhspBasis,isHrBasis}=input;
  const {buildWeeklyScheduleSegmentsSnapshot,buildNhspWeeklySnapshotCached,rebuildFromExistingSegmentsEvidence,enrichTsfinWithHrCrosscheck}=owners;
  const shiftsByTsId=new Map([[String(ts.timesheet_id),input.shifts]]);
  const weeklyValidationByTsId=new Map([[String(ts.timesheet_id),input.validation]]);
  const outbox_id=null;
  const round2 = (n) => Math.round((Number(n) || 0) * 100) / 100;
  const asNumberLocal = (v) => (v == null ? 0 : Number(v) || 0);
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


  if (!buildRes || buildRes.ok !== true || !buildRes.snapshot) throw new Error(buildRes?.reason || 'CORRECTION_NATIVE_WEEKLY_BUILD_FAILED');
  const snapshot=buildRes.snapshot;
  if (snapshot.occupant_key_norm == null && ts.occupant_key_norm != null) snapshot.occupant_key_norm=ts.occupant_key_norm;
  await enrichTsfinWithHrCrosscheck(env,ts,snapshot,effFlags || null,input.shifts);
  return snapshot;
}
