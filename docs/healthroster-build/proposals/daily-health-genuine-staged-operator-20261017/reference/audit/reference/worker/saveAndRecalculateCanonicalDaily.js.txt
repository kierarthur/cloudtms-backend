async function saveAndRecalculateCanonicalDaily(env, {
  timesheetId,
  timesheetPatch = null,
  forceUnprocessed = false,
  reason = 'CONTEXT_CHANGED'
} = {}) {
  const id = String(timesheetId || '').trim();
  if (!id) throw new Error('DAILY_CANONICAL_TIMESHEET_REQUIRED');

  if (timesheetPatch && typeof timesheetPatch === 'object' && !Array.isArray(timesheetPatch)) {
    const patchResponse = await fetch(
      `${env.SUPABASE_URL}/rest/v1/timesheets?timesheet_id=eq.${encodeURIComponent(id)}&is_current=eq.true`,
      {
        method: 'PATCH',
        headers: { ...sbHeaders(env), Prefer: 'return=minimal' },
        body: JSON.stringify(timesheetPatch)
      }
    );
    if (!patchResponse.ok) {
      throw new Error(`DAILY_CANONICAL_SAVE_FAILED: ${await patchResponse.text().catch(() => '')}`);
    }
  }

  await sbRpc(env, 'enqueue_ts_financials_priority', {
    _timesheet_ids: [id],
    _reason: reason
  });
  const workerResult = await runTsfinWorkerOnce(env, {
    limit: 1,
    onlyTimesheetIds: [id],
    forceUnprocessedTimesheetIds: forceUnprocessed ? [id] : []
  });
  const timesheet = await sbGetOne(
    env,
    `${env.SUPABASE_URL}/rest/v1/timesheets?timesheet_id=eq.${encodeURIComponent(id)}&is_current=eq.true&select=*&limit=1`
  );
  const financials = await sbGetOne(
    env,
    `${env.SUPABASE_URL}/rest/v1/timesheets_financials?timesheet_id=eq.${encodeURIComponent(id)}&is_current=eq.true&select=*&limit=1`
  );
  const completedIds = new Set(
    (Array.isArray(workerResult?.completed_timesheet_ids) ? workerResult.completed_timesheet_ids : [])
      .map((value) => String(value || '').trim().toLowerCase())
      .filter(Boolean)
  );
  if (!timesheet || !financials || Number(workerResult?.fail || 0) > 0 || !completedIds.has(id.toLowerCase())) {
    throw new Error('DAILY_CANONICAL_RECALCULATION_FAILED');
  }
  return { timesheet, financials, worker: workerResult };
}