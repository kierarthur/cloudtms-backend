import assert from 'node:assert/strict';
import test from 'node:test';

import { evaluateCreateDraftRecoveryHeadroom } from '../broker/src/banking-pay-create-draft-headroom.js';

const candidateId = '11111111-1111-4111-8111-111111111111';

test('database-certified current pre-draft recovery does not require a selectable context row', () => {
  const result = evaluateCreateDraftRecoveryHeadroom([{
    candidate_id: candidateId,
    pay_channel: 'PAYE',
    line_type: 'OVERPAYMENT_RECOVERY',
    amount_ex_vat: -147.82,
    recoverable_this_pay_run_ex_vat: 147.82,
    policy_x_authority_scope: 'PRE_DRAFT_LIVE_TRUTH'
  }]);

  assert.equal(result.ok, true);
  assert.deepEqual(result.invalid_groups, []);
});

test('unattested recovery without selected positive headroom remains blocked', () => {
  const result = evaluateCreateDraftRecoveryHeadroom([{
    candidate_id: candidateId,
    pay_channel: 'PAYE',
    line_type: 'OVERPAYMENT_RECOVERY',
    amount_ex_vat: -147.82,
    recoverable_this_pay_run_ex_vat: null,
    policy_x_authority_scope: null
  }]);

  assert.equal(result.ok, false);
  assert.equal(result.invalid_groups[0].uncertified_finance_recovery_ex_vat, 147.82);
});

test('legacy recovery remains valid when the submitted same-channel scope has positive headroom', () => {
  const result = evaluateCreateDraftRecoveryHeadroom([
    {
      candidate_id: candidateId,
      pay_channel: 'PAYE',
      line_type: 'TIMESHEET_PAYMENT',
      amount_ex_vat: 200
    },
    {
      candidate_id: candidateId,
      pay_channel: 'PAYE',
      line_type: 'OVERPAYMENT_RECOVERY',
      amount_ex_vat: -147.82
    }
  ]);

  assert.equal(result.ok, true);
});

test('certification must cover the full selected recovery amount and be current pre-draft authority', () => {
  for (const contract of [
    { recoverable_this_pay_run_ex_vat: 147.81, policy_x_authority_scope: 'PRE_DRAFT_LIVE_TRUTH' },
    { recoverable_this_pay_run_ex_vat: 147.82, policy_x_authority_scope: 'POST_DRAFT_FROZEN_TRUTH' },
    { recoverable_this_pay_run_ex_vat: 'not-a-number', policy_x_authority_scope: 'PRE_DRAFT_LIVE_TRUTH' }
  ]) {
    const result = evaluateCreateDraftRecoveryHeadroom([{
      candidate_id: candidateId,
      pay_channel: 'PAYE',
      line_type: 'OVERPAYMENT_RECOVERY',
      amount_ex_vat: -147.82,
      ...contract
    }]);
    assert.equal(result.ok, false);
  }
});

test('PAYE and Umbrella remain independently bounded', () => {
  const result = evaluateCreateDraftRecoveryHeadroom([
    {
      candidate_id: candidateId,
      pay_channel: 'PAYE',
      line_type: 'TIMESHEET_PAYMENT',
      amount_ex_vat: 200
    },
    {
      candidate_id: candidateId,
      pay_channel: 'UMBRELLA',
      line_type: 'OVERPAYMENT_RECOVERY',
      amount_ex_vat: -50
    }
  ]);

  assert.equal(result.ok, false);
  assert.equal(result.invalid_groups[0].pay_channel, 'UMBRELLA');
});

test('mixed certified and legacy recovery cannot spend selected positive headroom twice', () => {
  const result = evaluateCreateDraftRecoveryHeadroom([
    {
      candidate_id: candidateId,
      pay_channel: 'PAYE',
      line_type: 'TIMESHEET_PAYMENT',
      amount_ex_vat: 100
    },
    {
      candidate_id: candidateId,
      pay_channel: 'PAYE',
      line_type: 'OVERPAYMENT_RECOVERY',
      amount_ex_vat: -80,
      recoverable_this_pay_run_ex_vat: 80,
      policy_x_authority_scope: 'PRE_DRAFT_LIVE_TRUTH'
    },
    {
      candidate_id: candidateId,
      pay_channel: 'PAYE',
      line_type: 'LOAN_REPAYMENT',
      amount_ex_vat: -30
    }
  ]);

  assert.equal(result.ok, false);
  assert.equal(result.invalid_groups[0].selected_positive_requirement_ex_vat, 110);
});
