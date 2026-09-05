import assert from 'node:assert/strict';
import test from 'node:test';

import {
  createTimesheetRouteComparator,
  displayedTimesheetRouteLabel
} from '../broker/src/timesheet-route-sort.js';

test('displayed Route label takes precedence over the internal route type', () => {
  const expense = {
    route_type: 'WEEKLY_NHSP_ADJUSTMENT',
    route_display: 'Electronic Expense',
    display_route_label: 'Electronic Expense'
  };
  assert.equal(displayedTimesheetRouteLabel(expense), 'Electronic Expense');
  assert.equal(displayedTimesheetRouteLabel({ route_type: 'WEEKLY_NHSP_ADJUSTMENT' }), 'Weekly NHSP Adjustment');
});

test('Route sorting follows exactly the labels shown to the user', () => {
  const rows = [
    { id: '3', route_type: 'WEEKLY_NHSP_ADJUSTMENT', route_display: 'Weekly NHSP Adjustment', candidate_name: 'C' },
    { id: '2', route_type: 'WEEKLY_NHSP_ADJUSTMENT', display_route_label: 'Electronic Expense', candidate_name: 'B' },
    { id: '1', route_type: 'DAILY_ELECTRONIC', route_display: 'Daily Electronic', candidate_name: 'A' }
  ];

  assert.deepEqual(
    rows.slice().sort(createTimesheetRouteComparator('asc')).map(displayedTimesheetRouteLabel),
    ['Daily Electronic', 'Electronic Expense', 'Weekly NHSP Adjustment']
  );
  assert.deepEqual(
    rows.slice().sort(createTimesheetRouteComparator('desc')).map(displayedTimesheetRouteLabel),
    ['Weekly NHSP Adjustment', 'Electronic Expense', 'Daily Electronic']
  );
});

test('blank Route labels remain last in both directions', () => {
  const rows = [
    { id: 'blank' },
    { id: 'expense', route_display: 'Electronic Expense' }
  ];
  assert.equal(rows.slice().sort(createTimesheetRouteComparator('asc')).at(-1).id, 'blank');
  assert.equal(rows.slice().sort(createTimesheetRouteComparator('desc')).at(-1).id, 'blank');
});
