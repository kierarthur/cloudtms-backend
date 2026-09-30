import assert from 'node:assert/strict';
import test from 'node:test';
import { chooseTestDatabaseRoute, classifyApplicationPaths, publicationStages, validateConnectionProof, selectDispatchedRun } from '../scripts/automatic-test-release-policy.mjs';
const base = { environment:'TEST',database:'cloudtms_test_clone',identityVerified:true,ledgerVerified:true,
  latestStatus:'VERIFIED',pendingMigrations:[],pendingRepeatables:[],contractMatches:true,verificationAuthorityUnchanged:true };
test('build connection evidence must be fresh, commit-bound and include the active commit',()=>{
  const sha='a'.repeat(40), now=Date.now(), targets=[['backend','worker','release-branch']];
  const proof={environment:'TEST',backendCommit:sha,checkedAt:new Date(now).toISOString(),workers:[{worker:'worker',branch:'release-branch',repository:'kierarthur/cloudtms-backend',verified:true,activeCommit:sha}]};
  assert.doesNotThrow(()=>validateConnectionProof(proof,sha,targets,now));
  for(const patch of [{checkedAt:'invalid'},{checkedAt:new Date(now-900001).toISOString()},{environment:'LIVE'},{backendCommit:'b'.repeat(40)},{workers:[{...proof.workers[0],activeCommit:undefined}]}])
    assert.throws(()=>validateConnectionProof({...proof,...patch},sha,targets,now));
});
test('workflow selection pins one exact dispatch and refuses ambiguous concurrent runs',()=>{
  const run={id:1,head_sha:'a',event:'workflow_dispatch',created_at:'2026-10-01T12:00:00Z'};
  assert.equal(selectDispatchedRun([run],'a',run.created_at).id,1);
  assert.equal(selectDispatchedRun([run],'b',run.created_at),undefined);
  assert.throws(()=>selectDispatchedRun([run,{...run,id:2}],'a',run.created_at));
  assert.equal(selectDispatchedRun([run,{...run,id:2}],'a',run.created_at,1).id,1);
});
test('application-only release skips SQL only with all fresh authority proofs', () => {
  assert.equal(chooseTestDatabaseRoute(base).route,'NO_DATABASE_CHANGE');
  for(const patch of [{latestStatus:'FAILED'},{latestStatus:'APPLYING'},{latestStatus:null},{contractMatches:false},{verificationAuthorityUnchanged:false}])
    assert.equal(chooseTestDatabaseRoute({...base,...patch}).route,'FULL_UPGRADE');
});
test('wrong database, LIVE and unproved ledgers are rejected', () => {
  for(const patch of [{environment:'LIVE'},{database:'other'},{identityVerified:false},{ledgerVerified:false}])
    assert.throws(()=>chooseTestDatabaseRoute({...base,...patch}));
});
test('pending schema or unclassified definitions require full verification', () => {
  assert.equal(chooseTestDatabaseRoute({...base,pendingMigrations:['migration.sql']}).route,'FULL_UPGRADE');
  assert.equal(chooseTestDatabaseRoute({...base,pendingRepeatables:['routine.sql']}).route,'FULL_UPGRADE');
});
test('only one exact proved component can be selected', () => {
  const state={...base,pendingRepeatables:['a'],components:[{id:'one',paths:['a'],sourceVerified:true,baseVerified:true}]};
  assert.equal(chooseTestDatabaseRoute(state).route,'APPROVED_COMPONENT');
  for(const patch of [{latestStatus:'FAILED'},{pendingMigrations:['m']},{pendingRepeatables:['a','b']},
    {components:[...state.components,...state.components]},{verificationAuthorityUnchanged:false},
    {components:[{...state.components[0],baseVerified:false}]}])
    assert.equal(chooseTestDatabaseRoute({...state,...patch}).route,'FULL_UPGRADE');
});
test('private authorities precede public broker and Office', () => {
  assert.deepEqual(publicationStages(classifyApplicationPaths(['broker/src/index.js','office:app.js'])),
    ['database','backend','candidate-private','candidate-synthetic','candidate-broker','office']);
  assert.deepEqual(publicationStages(classifyApplicationPaths(['office:app.js'])),['database','office']);
  assert.throws(()=>publicationStages(classifyApplicationPaths(['unregistered-service/code.js'])));
});
