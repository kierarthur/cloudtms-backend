import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {collectInstall2ReviewedInstalledSource} from '../scripts/healthroster-install2-installed-source.mjs';
import {validateInstall2Predecessor} from '../scripts/healthroster-install2-native-gate.mjs';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
test('exact installed Git source retains all nested SQL roots and uppercase migration authority',()=>{
 const value=collectInstall2ReviewedInstalledSource({repositoryRoot:root,installedSourceCommit:'f743b81fe537830d81b1de93ec4f9cb0331532a7'});
 assert.equal(value.source,'EXACT_COMMITTED_GIT_OBJECTS');assert.equal(value.nativeDatabaseProbed,false);
 assert.equal(value.inventory.migrations.length,389);assert.equal(value.inventory.repeatables.length,1019);
 assert.equal(value.contractHash,'7e3c00b70366c2c036cfea419adf32c77df5b65070771591e75168b68f3336d6');
 assert.ok(value.inventory.migrations.some(row=>row.path==='supabase/migrations/26052026_1525_INDEX_OUTBOX.SQL'));
 assert.ok(value.inventory.repeatables.some(row=>row.path==='supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_invoice_apply_edits.sql'));
 // This deliberately omits any native observation: Git authority alone must
 // never mint an installed/predecessor qualification.
 assert.throws(()=>validateInstall2Predecessor({reviewedSource:value}),/FRESH_PROBE_REQUIRED/);
});
test('installed-source collector refuses another relative repository, short ref or unavailable exact object',()=>{
 for(const args of [{repositoryRoot:'.',installedSourceCommit:'f'.repeat(40)},
  {repositoryRoot:root,installedSourceCommit:'f743b81'},{repositoryRoot:root,installedSourceCommit:'0'.repeat(40)}]){
  assert.throws(()=>collectInstall2ReviewedInstalledSource(args));
 }
});
