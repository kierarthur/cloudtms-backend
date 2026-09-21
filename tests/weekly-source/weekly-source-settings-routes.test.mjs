import test from 'node:test';
import assert from 'node:assert/strict';

import { dispatchWeeklySourceRequest } from '../../broker/src/weekly-source/routes.js';

const ACTOR = '11111111-1111-4111-8111-111111111111';
const AGENCY = '22222222-2222-4222-8222-222222222222';
const CLIENT = '33333333-3333-4333-8333-333333333333';
const CONTRACT = '44444444-4444-4444-8444-444444444444';
const GROUP = '55555555-5555-4555-8555-555555555555';
const env = Object.freeze({
  MYTMS_OFFICE_AGENCY_ID: AGENCY,
  CANDIDATE_APP_ENVIRONMENT: 'TEST',
});

function dependencies(calls) {
  return {
    requireUser: async () => ({ id: ACTOR, role: 'admin' }),
    rpc: async (name, args) => {
      calls.push({ name, args });
      return { ok: true, name, request: args.p_request };
    },
  };
}

test('all Client, Contract, global and source-group settings routes reach their exact RPC owners', async () => {
  const cases = [
    ['GET', '/api/weekly-source/v1/settings/global', null, 'weekly_source_global_settings_get_v1'],
    ['PUT', '/api/weekly-source/v1/settings/global', { expected_settings_version: 1, settings: {} }, 'weekly_source_global_settings_save_atomic_v1'],
    ['GET', '/api/weekly-source/v1/settings/source-groups', null, 'weekly_source_source_groups_get_v1'],
    ['PUT', `/api/weekly-source/v1/settings/clients/${CLIENT}`, { expected_settings_version: 'client-v1', settings: {} }, 'weekly_source_client_settings_save_atomic_v1'],
    ['GET', `/api/weekly-source/v1/settings/contracts/${CONTRACT}`, null, 'weekly_source_contract_settings_get_v1'],
  ];

  for (const [method, path, body, rpcName] of cases) {
    const calls = [];
    const response = await dispatchWeeklySourceRequest(
      new Request(`https://test.invalid${path}`, {
        method,
        ...(body ? { headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) } : {}),
      }),
      env,
      {},
      dependencies(calls),
    );
    assert.equal(response.status, 200, `${method} ${path}`);
    assert.equal(calls.length, 1, `${method} ${path}`);
    assert.equal(calls[0].name, rpcName, `${method} ${path}`);
    assert.equal(calls[0].args.p_request.actor_user_id, ACTOR);
    assert.equal(calls[0].args.p_request.agency_id, AGENCY);
    assert.equal(calls[0].args.p_request.environment, 'TEST');
  }
});

test('Client settings GET injects actor, Agency and environment on the server', async () => {
  const calls = [];
  const response = await dispatchWeeklySourceRequest(
    new Request(`https://test.invalid/api/weekly-source/v1/settings/clients/${CLIENT}?effective_date=2026-09-16`),
    env,
    {},
    dependencies(calls),
  );
  assert.equal(response.status, 200);
  assert.deepEqual(calls, [{
    name: 'weekly_source_client_settings_get_v1',
    args: { p_request: {
      actor_user_id: ACTOR,
      agency_id: AGENCY,
      environment: 'TEST',
      effective_date: '2026-09-16',
      client_id: CLIENT,
    } },
  }]);
});

test('Contract settings PUT keeps the strict payload and injects protected scope', async () => {
  const calls = [];
  const response = await dispatchWeeklySourceRequest(
    new Request(`https://test.invalid/api/weekly-source/v1/settings/contracts/${CONTRACT}`, {
      method: 'PUT',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        expected_settings_version: 'abc',
        settings: {
          effective_from: '2026-09-16',
          weekly_rate_classification_method_override: null,
          duration_break_tie_rule_override: null,
          source_fixed_expenses_enabled_override: null,
          source_expense_vat_enabled_override: null,
          candidate_queries_enabled_override: null,
          manager_queries_enabled_override: null,
          manager_query_recipient_override: null,
          completed_pack_copy_enabled_override: null,
          completed_pack_recipient_override: null,
        },
      }),
    }),
    env,
    {},
    dependencies(calls),
  );
  assert.equal(response.status, 200);
  assert.equal(calls[0].name, 'weekly_source_contract_settings_save_atomic_v1');
  assert.equal(calls[0].args.p_request.actor_user_id, ACTOR);
  assert.equal(calls[0].args.p_request.agency_id, AGENCY);
  assert.equal(calls[0].args.p_request.environment, 'TEST');
  assert.equal(calls[0].args.p_request.contract_id, CONTRACT);
});

test('Source-group PUT accepts the policy-owned nested source_group shape', async () => {
  const calls = [];
  const response = await dispatchWeeklySourceRequest(
    new Request('https://test.invalid/api/weekly-source/v1/settings/source-groups', {
      method: 'PUT',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ source_group: {
        id: GROUP,
        expected_version: 2,
        code: 'NHSP',
        display_name: 'NHSP',
        source_family: 'NHSP',
        cutoff_weekday: 3,
        cutoff_local_time: '15:00',
        nhsp_report_heading_name: 'Arthur Rai Medical Servic',
        active: true,
      } }),
    }),
    env,
    {},
    dependencies(calls),
  );
  assert.equal(response.status, 200);
  assert.equal(calls[0].name, 'weekly_source_source_group_save_atomic_v1');
  assert.equal(calls[0].args.p_request.source_group.id, GROUP);
});

test('Browser cannot select an Agency, environment or actor for settings', async () => {
  for (const forbidden of ['agency_id', 'environment', 'actor_user_id']) {
    const calls = [];
    const response = await dispatchWeeklySourceRequest(
      new Request('https://test.invalid/api/weekly-source/v1/settings/global', {
        method: 'PUT',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          [forbidden]: forbidden === 'agency_id' ? AGENCY : forbidden === 'environment' ? 'LIVE' : ACTOR,
          expected_settings_version: 1,
          settings: {},
        }),
      }),
      env,
      {},
      dependencies(calls),
    );
    assert.equal(response.status, 400);
    assert.equal((await response.json()).error_code, 'WEEKLY_SOURCE_SETTINGS_SCOPE_FORBIDDEN');
    assert.equal(calls.length, 0);
  }
});

test('Unknown settings query keys and invalid deployment context fail closed', async () => {
  const calls = [];
  const badQuery = await dispatchWeeklySourceRequest(
    new Request('https://test.invalid/api/weekly-source/v1/settings/global?agency_id=spoofed'),
    env,
    {},
    dependencies(calls),
  );
  assert.equal(badQuery.status, 400);
  assert.equal(calls.length, 0);

  const badDeployment = await dispatchWeeklySourceRequest(
    new Request('https://test.invalid/api/weekly-source/v1/settings/global'),
    { MYTMS_OFFICE_AGENCY_ID: AGENCY, CANDIDATE_APP_ENVIRONMENT: 'STAGING' },
    {},
    dependencies(calls),
  );
  assert.equal(badDeployment.status, 503);
  assert.equal(calls.length, 0);
});
