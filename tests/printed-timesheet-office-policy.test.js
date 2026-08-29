import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

const sourceUrl = new URL('../broker/src/index.js', import.meta.url);
const source = await readFile(sourceUrl, 'utf8');

const section = (startMarker, endMarker) => {
  const start = source.indexOf(startMarker);
  assert.notEqual(start, -1, `missing ${startMarker}`);
  const end = source.indexOf(endMarker, start + startMarker.length);
  assert.notEqual(end, -1, `missing ${endMarker}`);
  return source.slice(start, end);
};

test('existing Client and Contract policy writes have narrow authenticated routes', () => {
  const client = section('async function handleClientPrintedTimesheetPolicyUpdate', 'async function handleContractPrintedTimesheetPolicyUpdate');
  assert.match(client, /requireUser\(env, req, \['admin'\]\)/);
  assert.match(client, /allowedKeys = new Set\(\['enabled', 'expected_settings_updated_at', 'request_key'\]\)/);
  assert.match(client, /candidate_paper_submission_enabled: body\.enabled/);
  assert.match(client, /updated_at=eq\./);

  const contract = section('async function handleContractPrintedTimesheetPolicyUpdate', 'async function handleUpdateClient');
  assert.match(contract, /requireUser\(env, req, \['admin'\]\)/);
  assert.match(contract, /allowedKeys = new Set\(\['override', 'expected_contract_updated_at', 'request_key'\]\)/);
  assert.match(contract, /candidate_paper_submission_enabled_override: body\.override/);
  assert.match(contract, /updated_at=eq\./);

  assert.match(source, /\/api\/clients\/:id\/printed-timesheet-policy/);
  assert.match(source, /\/api\/contracts\/:id\/printed-timesheet-policy/);
});

test('generic existing-record saves cannot silently mutate the independent policy', () => {
  const updateClientStart = source.indexOf('async function handleUpdateClient(env, req, clientId)');
  assert.notEqual(updateClientStart, -1);
  const updateClient = source.slice(updateClientStart, updateClientStart + 18_000);
  assert.match(updateClient, /delete csInput\.candidate_paper_submission_enabled/);

  const updateContract = section('async function handleContractsUpdate', 'async function handleContractsReplace');
  assert.doesNotMatch(updateContract, /candidate_paper_submission_enabled_override\s*:/);
});

test('Contract creation and duplication preserve the independent policy without a QR submission enum', () => {
  const create = section('async function handleContractsCreate', 'async function handleContractsList');
  assert.match(create, /candidate_paper_submission_enabled_override/);
  assert.doesNotMatch(create, /default_submission_mode[^\n]*(?:QR|PAPER)/);

  const duplicate = section('async function handleContractsDuplicate', 'async function handleContractsCloneAndExtend');
  assert.match(duplicate, /candidate_paper_submission_enabled_override: copyBoolOrNull/);
});
