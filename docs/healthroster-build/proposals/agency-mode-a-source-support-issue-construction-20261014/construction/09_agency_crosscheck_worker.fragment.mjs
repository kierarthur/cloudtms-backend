  // Preset crosscheck belongs to the native Source comparison and the sole
  // typed completeness writer. It runs before the legacy unsigned/HR flags
  // branch, which would otherwise clear real preset support.
  const agencySupport = await sbRpc(env, 'weekly_source_agency_crosscheck_context_v1', { p_timesheet_id: tsId });
  if (!agencySupport || typeof agencySupport !== 'object' || Array.isArray(agencySupport)
    || agencySupport.contract !== 'WEEKLY_SOURCE_AGENCY_CROSSCHECK_CONTEXT_V1'
    || agencySupport.timesheet_id !== tsId || !Number.isInteger(agencySupport.timesheet_version)
    || agencySupport.timesheet_version < 1 || agencySupport.timesheet_version !== ts.version
    || typeof agencySupport.is_agency_invoice !== 'boolean') {
    throw new Error('AGENCY_INVOICE_SUPPORT_NATIVE_CONTEXT_INVALID');
  }
  if (!agencySupport.is_agency_invoice && (Object.keys(agencySupport).length !== 4
    || Object.keys(agencySupport).some(k => !['contract','is_agency_invoice','timesheet_id','timesheet_version'].includes(k)))) {
    throw new Error('AGENCY_INVOICE_SUPPORT_NATIVE_CONTEXT_INVALID');
  }
  if (agencySupport.is_agency_invoice) {
    const uuid = /^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
    const allowed = new Set(['contract','is_agency_invoice','timesheet_id','timesheet_version','source_cycle_id',
      'projection_publication_id','upload_id','hr_import_id','facts_sha256','week_complete',
      'validation_source_kind','validation_status','validation_reason','crosscheck_status','crosscheck_issues','rows']);
    if (Object.keys(agencySupport).some(k => !allowed.has(k)) || Object.keys(agencySupport).length !== allowed.size
      || ['source_cycle_id','projection_publication_id','upload_id','hr_import_id'].some(k =>
        typeof agencySupport[k] !== 'string' || !uuid.test(agencySupport[k]))
      || typeof agencySupport.facts_sha256 !== 'string' || !/^[0-9a-f]{64}$/.test(agencySupport.facts_sha256)
      || typeof agencySupport.week_complete !== 'boolean' || agencySupport.validation_source_kind !== 'AGENCY_INVOICE_WEEK'
      || !['VALIDATION_OK','VALIDATION_ERROR'].includes(agencySupport.validation_status)
      || agencySupport.week_complete !== (agencySupport.validation_status === 'VALIDATION_OK')
      || (agencySupport.validation_reason !== null && (typeof agencySupport.validation_reason !== 'string'
        || agencySupport.validation_reason.length < 1 || agencySupport.validation_reason.length > 256))
      || !['OK','HOURS_MISMATCH_HR'].includes(agencySupport.crosscheck_status)
      || (agencySupport.crosscheck_status === 'OK' ? agencySupport.crosscheck_issues !== null
        : !Array.isArray(agencySupport.crosscheck_issues) || agencySupport.crosscheck_issues.length !== 1
          || agencySupport.crosscheck_issues[0] !== 'HOURS_MISMATCH_HR')
      || !Array.isArray(agencySupport.rows) || agencySupport.rows.length < 1 || agencySupport.rows.length > 100) {
      throw new Error('AGENCY_INVOICE_SUPPORT_NATIVE_CONTEXT_INVALID');
    }
    const rowKeys = ['date','source_system','hr_request_id','nhsp_shift_id','external_row_key','ward',
      'assignment_code','ref_num','start','end','break_mins','paid_minutes'];
    for (const row of agencySupport.rows) {
      if (!row || typeof row !== 'object' || Array.isArray(row) || Object.keys(row).length !== rowKeys.length
        || Object.keys(row).some(k => !rowKeys.includes(k)) || row.source_system !== 'HEALTHROSTER'
        || row.nhsp_shift_id !== null || typeof row.date !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(row.date)
        || ['start','end'].some(k => typeof row[k] !== 'string' || !/^([01]\d|2[0-3]):[0-5]\d$/.test(row[k]))
        || !Number.isInteger(row.break_mins) || row.break_mins < 0 || !Number.isInteger(row.paid_minutes) || row.paid_minutes <= 0
        || ['hr_request_id','external_row_key','ward','assignment_code','ref_num'].some(k =>
          row[k] !== null && (typeof row[k] !== 'string' || row[k].length > 4096))) {
        throw new Error('AGENCY_INVOICE_SUPPORT_NATIVE_ROW_INVALID');
      }
    }
    // Copy the actual existing HR_WEEKLY row shape and genuine bridge import.
    // No comparison, paid-break arithmetic, amount or payer is recomputed here.
    const ext = tsfinRow.external_source_rows_json && typeof tsfinRow.external_source_rows_json === 'object'
      ? structuredClone(tsfinRow.external_source_rows_json) : {};
    ext.HR_WEEKLY = structuredClone(agencySupport.rows);
    tsfinRow.external_source_rows_json = ext;
    tsfinRow.nhsp_import_id = agencySupport.hr_import_id;
    tsfinRow.hr_crosscheck_status = agencySupport.crosscheck_status;
    tsfinRow.hr_crosscheck_issues = structuredClone(agencySupport.crosscheck_issues);
    return tsfinRow;
  }
