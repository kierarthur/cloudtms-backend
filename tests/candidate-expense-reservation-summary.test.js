import test from 'node:test';
import assert from 'node:assert/strict';
import { attachExpenseReservationSummaries } from '../broker/src/candidate-expense-reservation-summary.mjs';

const env = { CANDIDATE_APP_ENVIRONMENT: 'TEST' };
const unwrap = value => value;

test('only provisional additional rows are queried; no payment/source fields change', async () => {
  const rows = [
    { contract_week_id: 'root', additional_seq: 0 },
    { contract_week_id: 'physical', timesheet_id: 'timesheet', additional_seq: 1 },
    { contract_week_id: 'pending', additional_seq: 1, total_hours: 0, route_type: 'WEEKLY_NHSP_ADJUSTMENT' }
  ];
  await attachExpenseReservationSummaries(env, 'actor', rows, async (_env, name, args) => {
    assert.equal(name, 'candidate_office_expense_reservations_v1');
    assert.deepEqual(args.p_contract_week_ids, ['pending']);
    assert.equal(args.p_actor_user_id, 'actor');
    return { ok: true, rows: [{ contract_week_id: 'pending', workflow_count: 1, state: 'AWAITING_MANAGER_APPROVAL' }] };
  }, unwrap);
  assert.equal(rows[0].candidate_expense_reservation, undefined);
  assert.equal(rows[1].candidate_expense_reservation, undefined);
  assert.equal(rows[2].candidate_expense_reservation.label, 'Expenses awaiting manager approval');
  assert.equal(rows[2].display_route_label, 'Expense claim');
  assert.equal(rows[2].route_type, 'WEEKLY_NHSP_ADJUSTMENT');
  assert.equal(rows[2].total_hours, 0);
});

test('unknown or foreign response identities are rejected without attaching false claim status', async () => {
  for (const item of [
    { contract_week_id: 'foreign', workflow_count: 1, state: 'READY_TO_FINALISE' },
    { contract_week_id: 'pending', workflow_count: 1, state: 'INVENTED' }
  ]) {
    const rows = [{ contract_week_id: 'pending', additional_seq: 1 }];
    await attachExpenseReservationSummaries(env, 'actor', rows,
      async () => ({ ok: true, rows: [item] }), unwrap);
    assert.equal(rows[0].candidate_expense_reservation, undefined);
    assert.equal(rows[0].candidate_expense_reservation_error, 'EXPENSE_RESERVATION_STATUS_UNAVAILABLE');
  }
});

test('an ordinary additional row stays unchanged when not proved an expense reservation', async () => {
  const rows = [{ contract_week_id: 'ordinary', additional_seq: 1 }];
  await attachExpenseReservationSummaries(env, 'actor', rows,
    async () => ({ ok: true, rows: [] }), unwrap);
  assert.deepEqual(rows, [{ contract_week_id: 'ordinary', additional_seq: 1 }]);
});

test('large lists remain bounded and all requested reservations are reached', async () => {
  const rows = Array.from({ length: 205 }, (_, i) => ({ contract_week_id: `week-${i}`, additional_seq: 1 }));
  const sizes = [];
  await attachExpenseReservationSummaries(env, 'actor', rows, async (_env, _name, args) => {
    sizes.push(args.p_contract_week_ids.length);
    return { ok: true, rows: args.p_contract_week_ids.map(id => ({ contract_week_id: id, workflow_count: 1, state: 'READY_TO_FINALISE' })) };
  }, unwrap);
  assert.deepEqual(sizes, [100, 100, 5]);
  assert.ok(rows.every(row => row.candidate_expense_reservation.label === 'Finalising expense claim'));
});
