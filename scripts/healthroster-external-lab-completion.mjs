import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {abortableDelay} from './healthroster-turn-control.mjs';

// This is only a bounded completion channel for a server-owned rollback lab.
// It neither opens a database connection nor takes or releases the shared turn.
export function assertExternalLabCompletion(result, expected) {
  const refuse = code => { throw Object.assign(new Error(code), {code}); };
  if (!/^hr_alias_lab_[a-f0-9]{32}$/.test(expected?.marker ?? '')
      || !/^[a-f0-9]{64}$/.test(expected.sqlSha256 ?? '')
      || expected.statementCount !== 52
      || typeof expected.postmasterStart !== 'string') refuse('LAB_COMPLETION_EXPECTED_IDENTITY_REFUSED');
  if (result?.accepted !== true || result.database !== 'cloudtms_test_clone'
      || result.sqlSha256 !== expected.sqlSha256 || result.statementCount !== expected.statementCount
      || !Array.isArray(result.statements) || result.statements.length !== expected.statementCount
      || result.statements.some((entry, index) => entry?.statement !== index + 1
        || Object.hasOwn(entry, 'error') || Object.hasOwn(entry, 'errorCode')
        || entry.rowsTruncated !== false)
      || result.rolledBack !== true) refuse('LAB_COMPLETION_NATIVE_RESULT_REFUSED');
  const last = result.statements.at(-1).rows?.[0]?.jsonb_build_object;
  if (last?.status !== 'ALIAS_FIXTURE_ALL_ASSERTIONS_REACHED'
      || last.database !== 'cloudtms_test_clone' || last.marker !== expected.marker)
    refuse('LAB_COMPLETION_FINAL_ASSERTION_REFUSED');
  const verification = result.rollbackVerification;
  if (verification?.connectedFresh !== true || verification.database !== 'cloudtms_test_clone'
      || !Array.isArray(verification.statements) || verification.statements.length !== 1
      || verification.statements[0].rowsTruncated !== false
      || Object.hasOwn(verification.statements[0], 'error')) refuse('LAB_COMPLETION_FRESH_CONNECTION_REFUSED');
  const fresh = verification.statements[0].rows?.[0]?.jsonb_build_object;
  const relations = ['public.candidates','public.clients','public.contracts','public.tms_users',
    'public.weekly_source_cycles','public.weekly_source_group_clients','public.weekly_source_groups',
    'public.weekly_source_row_resolutions','public.weekly_source_upload_rows','public.weekly_source_uploads',
    'public.weekly_work_event_source_links','public.weekly_work_events'];
  if (fresh?.database !== 'cloudtms_test_clone' || fresh.testIdentity !== true
      || fresh.legacyOne !== true || fresh.helperAbsent !== true || fresh.markerAbsent !== true
      || fresh.allFixtureIdentifiersAbsent !== true || fresh.postmasterStart !== expected.postmasterStart
      || !Array.isArray(fresh.fixtureCounts) || fresh.fixtureCounts.length !== relations.length
      || fresh.fixtureCounts.some((row, index) => row.relation !== relations[index] || row.fixture_matches !== 0))
    refuse('LAB_COMPLETION_FRESH_RESTORATION_REFUSED');
  return {status:'EXTERNAL_LAB_RESULT_VALIDATED',marker:expected.marker,sqlSha256:expected.sqlSha256,
    statementCount:expected.statementCount,physicalDatabase:result.database,serverRolledBack:true,
    connectedFresh:true,finiteFixtureRelations:relations.length,
    scope:'Native assertions and fresh finite fixture absence only; caller must independently prove shared turn and fresh core fingerprints.'};
}

export async function awaitExternalLabCompletion({receiptDir,expected,signal,assertHeld,timeoutMs=180000,pollMs=200}) {
  if (!path.isAbsolute(receiptDir) || typeof assertHeld !== 'function' || !signal
      || !Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 180000
      || !Number.isSafeInteger(pollMs) || pollMs < 1 || pollMs > 1000)
    throw new Error('LAB_COMPLETION_CHANNEL_ARGUMENTS_REFUSED');
  assertExternalLabExpected(expected);
  const file = path.join(receiptDir, 'NATIVE_LAB_RESULT.json');
  if (fs.existsSync(file)) throw new Error('LAB_COMPLETION_PREEXISTING_RECEIPT_REFUSED');
  // The owning turn's existing monitor feeds this abort signal. Polling files
  // does not add repeated database probes; assert again before returning.
  await assertHeld('before-external-lab-completion');
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (signal.aborted) throw signal.reason;
    if (fs.existsSync(file)) {
      // The producer publishes by atomic rename after writing the entire JSON.
      const stat=fs.lstatSync(file);
      if (!stat.isFile() || stat.isSymbolicLink() || stat.size > 1000000) throw new Error('LAB_COMPLETION_FILE_REFUSED');
      const result=JSON.parse(fs.readFileSync(file,'utf8'));
      const validation=assertExternalLabCompletion(result,expected);
      await assertHeld('external-lab-completion-validated');
      return {result,validation};
    }
    await abortableDelay(Math.min(pollMs,Math.max(1,deadline-Date.now())),signal);
  }
  throw new Error('LAB_COMPLETION_DEADLINE_NO_RESULT');
}

export function publishExternalLabCompletion({receiptDir,result,expected}) {
  if (!path.isAbsolute(receiptDir)) throw new Error('LAB_COMPLETION_CHANNEL_ARGUMENTS_REFUSED');
  const validation=assertExternalLabCompletion(result,expected);
  const file=path.join(receiptDir,'NATIVE_LAB_RESULT.json');
  if(fs.existsSync(file))throw new Error('LAB_COMPLETION_PREEXISTING_RECEIPT_REFUSED');
  const temporary=path.join(receiptDir,'NATIVE_LAB_RESULT.'+crypto.randomBytes(12).toString('hex')+'.tmp');
  fs.writeFileSync(temporary,JSON.stringify(result,null,2)+'\n',{flag:'wx'});
  // Publish a complete file with an atomic hard link. Unlike overwrite rename,
  // EEXIST preserves any concurrent producer's first completed receipt.
  try {fs.linkSync(temporary,file);} finally {fs.unlinkSync(temporary);}
  return validation;
}

function assertExternalLabExpected(expected) {
  if (!/^hr_alias_lab_[a-f0-9]{32}$/.test(expected?.marker ?? '')
      || !/^[a-f0-9]{64}$/.test(expected.sqlSha256 ?? '')
      || expected.statementCount !== 52 || typeof expected.postmasterStart !== 'string')
    throw new Error('LAB_COMPLETION_EXPECTED_IDENTITY_REFUSED');
}
