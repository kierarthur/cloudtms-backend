const labels = Object.freeze({
  CREATED: 'Expense claim not submitted', WORKER_DRAFT: 'Expense claim not submitted',
  WORKER_SUBMITTED: 'Expense claim submitted',
  WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT: 'Preparing expense claim',
  READY_FOR_MANAGER_APPROVAL: 'Expense claim submitted',
  AWAITING_MANAGER_APPROVAL: 'Expenses awaiting manager approval',
  MANAGER_APPROVED: 'Expenses approved by manager',
  MANAGER_APPROVED_PENDING_FINAL_DOCUMENT: 'Finalising expense claim',
  READY_TO_FINALISE: 'Finalising expense claim',
  AWAITING_PAPER_RETURN: 'Expenses awaiting signed return',
  RECEIVED: 'Finalising expense claim', FINALISED: 'Expense claim submitted',
  CANCELLED: 'Expense claim cancelled', EXPIRED: 'Expense claim expired',
  SUPERSEDED: 'Expense claim replaced', REJECTED: 'Expense claim rejected',
  REFUSED: 'Expense claim refused', HISTORY: 'Expense claim history',
  AMBIGUOUS: 'Expense claim needs checking'
});

// This is a read-only supplement for exact provisional Contract Weeks, not a
// replacement for canonical physical-Timesheet lifecycle/action projections.
export async function attachExpenseReservationSummaries(env, actorUserId, rows, rpc, unwrap) {
  const candidates = rows.filter(row => !row.timesheet_id && row.contract_week_id
    && Number(row.additional_seq) > 0);
  const ids = [...new Set(candidates.map(row => String(row.contract_week_id)))];
  for (let offset = 0; offset < ids.length; offset += 100) {
    const batch = ids.slice(offset, offset + 100);
    const selected = new Set(batch);
    try {
      const raw = await rpc(env, 'candidate_office_expense_reservations_v1', {
        p_environment: String(env?.CANDIDATE_APP_ENVIRONMENT || '').trim().toUpperCase(),
        p_actor_user_id: actorUserId, p_contract_week_ids: batch
      });
      const result = unwrap(raw, 'candidate_office_expense_reservations_v1') || raw;
      if (result?.ok !== true || !Array.isArray(result.rows) || result.rows.length > batch.length) {
        throw new Error('EXPENSE_RESERVATION_PROJECTION_INVALID');
      }
      const byId = new Map();
      for (const item of result.rows) {
        if (!selected.has(item?.contract_week_id) || byId.has(item.contract_week_id)
          || !Object.hasOwn(labels, item.state)
          || !Number.isInteger(item.workflow_count) || item.workflow_count < 0) {
          throw new Error('EXPENSE_RESERVATION_PROJECTION_INVALID');
        }
        byId.set(item.contract_week_id, item);
      }
      for (const row of candidates) {
        const item = byId.get(String(row.contract_week_id));
        if (!item) continue;
        row.candidate_expense_reservation = { state: item.state, label: labels[item.state] };
        row.candidate_office_summary_status_label = labels[item.state];
        row.display_route_label = 'Expense claim';
      }
    } catch {
      for (const row of candidates) {
        if (selected.has(String(row.contract_week_id))) {
          row.candidate_expense_reservation_error = 'EXPENSE_RESERVATION_STATUS_UNAVAILABLE';
        }
      }
    }
  }
}
