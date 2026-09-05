import { spawnSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  buildCertifiedDraftTerminalResultV8,
  validateCertifiedDraftTerminalContextV8
} from '../broker/src/banking-pay-draft-certified-v8.js';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const args = new Map();
for (let index = 2; index < process.argv.length; index += 2) {
  const key = process.argv[index];
  const value = process.argv[index + 1];
  if (!key?.startsWith('--') || !value) throw new Error(`Invalid argument at position ${index}`);
  args.set(key.slice(2), value);
}

const container = String(args.get('container') || '');
const database = String(args.get('database') || '');
const operationId = String(args.get('operation-id') || '').toLowerCase();
if (!/^h12-v8-restart-pg(?:17|18)$/.test(container)) throw new Error('Exact task-owned --container is required');
if (database !== 'banking_modal_v2_test') throw new Error('Exact task-owned --database is required');
if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(operationId)) {
  throw new Error('Valid --operation-id is required');
}

function psql(sql) {
  const result = spawnSync(
    'docker',
    ['exec', '-i', '-e', 'PGOPTIONS=-c jit=off', container, 'psql', '-X', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', database, '-At'],
    { cwd: root, input: sql, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 }
  );
  if (result.status !== 0) throw new Error(`Disposable psql failed (${result.status}):\n${result.stderr || result.stdout}`);
  return String(result.stdout || '').trim();
}

const prepared = JSON.parse(psql(`
SELECT public.banking_pay_draft_advance_bounded_v8(
  '${operationId}'::uuid,
  'H2_V8_DISPOSABLE_TERMINAL',
  (
    SELECT receipt_digest_sha256
    FROM private.banking_pay_draft_frozen_stage_receipts_v8
    WHERE operation_id='${operationId}'::uuid
      AND stage_kind='CERTIFICATE_PARTITION_REFS'
      AND stage_status='TERMINAL'
      AND has_more=false
    ORDER BY page_sequence DESC
    LIMIT 1
  )
);
`));
if (prepared.work_kind !== 'READY_FOR_TERMINAL_FINISH') throw new Error(`Operation is not ready to finish: ${JSON.stringify(prepared)}`);

const terminalContext = validateCertifiedDraftTerminalContextV8(prepared.terminal_prepare);
if (terminalContext.ok !== true) throw new Error(terminalContext.code || 'BANKING_PAY_DRAFT_TERMINAL_CONTEXT_INVALID');
if (terminalContext.replacement_session_required) {
  throw new Error('Disposable mixed-channel oracle unexpectedly requires a replacement Workbench session');
}
const terminalResult = buildCertifiedDraftTerminalResultV8(
  terminalContext,
  null,
  { ok: true, scheduled: false, skipped: true, reason: 'TASK_OWNED_DISPOSABLE_NO_BACKGROUND_WORKER' }
);
const encodedResult = JSON.stringify(terminalResult);
if (encodedResult.includes('$h2_terminal$')) throw new Error('Unexpected SQL delimiter collision');

const finish = JSON.parse(psql(`
SELECT pg_catalog.row_to_json(finish_row)::text
FROM public.banking_pay_draft_operation_finish_v8(
  '${operationId}'::uuid,
  'COMPLETE',
  $h2_terminal$${encodedResult}$h2_terminal$::jsonb,
  NULL::jsonb
) AS finish_row;
`));
if (
  finish.status !== 'COMPLETE'
  || !['COMPLETE', 'POST_CREATE_REFRESH'].includes(String(finish.phase || '').toUpperCase())
  || finish.finished !== true
) {
  throw new Error(`Terminal finish did not complete: ${JSON.stringify(finish)}`);
}

process.stdout.write(`${JSON.stringify({
  contract: 'BANKING_PAY_DRAFT_V8_DISPOSABLE_TERMINAL_RESULT_V1',
  operation_id: operationId,
  status: finish.status,
  phase: finish.phase,
  terminal_status_is_authoritative: true,
  created_pay_batch_ids: terminalResult.created_pay_batch_ids,
  paye_pay_batch_id: terminalResult.paye_pay_batch_id,
  umbrella_pay_batch_id: terminalResult.umbrella_pay_batch_id,
  replacement_session_required: false,
  external_background_worker_scheduled: false
}, null, 2)}\n`);
