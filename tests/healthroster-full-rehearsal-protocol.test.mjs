import test from 'node:test';
import assert from 'node:assert/strict';
import {digest} from '../scripts/cloudtms-managed-release-sql.mjs';
import {renderFullRehearsalUnits,closedRehearsalDiagnostic,neutraliseReviewedVerifierEnvelope} from '../scripts/healthroster-full-rehearsal-protocol.mjs';
const unit=(sql,kind='verifier')=>({file:'supabase/verification/13102026_example.sql',kind,sql,sqlHash:digest(sql)});

test('a reviewed read-only envelope is removed while every native assertion and inner rollback remains exact',()=>{
  const body="do $native$ begin if false then raise exception 'NATIVE_ASSERTION'; end if; end; $native$;\nsavepoint native_case;\nselect 1;\nrollback to native_case;\nrelease native_case;";
  const original=unit('\\set ON_ERROR_STOP on\nbegin read only;\n'+body+'\nrollback;');
  const reviewed={sourceSqlHash:original.sqlHash,readOnlyBodyReviewed:true,
    removedCommands:['\\set ON_ERROR_STOP on','BEGIN READ ONLY','ROLLBACK']};
  const neutral=neutraliseReviewedVerifierEnvelope(original,reviewed);
  assert.ok(neutral.sql.includes(body));assert.equal(neutral.sourceSqlHash,original.sqlHash);
  assert.deepEqual(neutral.envelope.map(x=>x.command),reviewed.removedCommands);
  assert.equal(renderFullRehearsalUnits([neutral]).units[0].reviewedEnvelope.length,3);
  for (const changed of [{sourceSqlHash:'0'.repeat(64)},{readOnlyBodyReviewed:false},
    {removedCommands:['BEGIN READ ONLY','ROLLBACK']}]) {
    assert.throws(()=>neutraliseReviewedVerifierEnvelope(original,{...reviewed,...changed}),/REFUSED/);
  }
});
test('envelope review cannot permit prepared transactions, alternate modes or error rollback overrides',()=>{
  for(const sql of ['begin read write;','start transaction;','prepare transaction \'other\';',
    '\\set ON_ERROR_ROLLBACK off']) {
    const original=unit(sql);
    assert.throws(()=>neutraliseReviewedVerifierEnvelope(original,
      {sourceSqlHash:original.sqlHash,removedCommands:[],readOnlyBodyReviewed:true}),/REFUSED/);
  }
});

test('verifier client variable is cleared before and after each complete savepoint, while its native body is retained',()=>{
  const first=unit('\\set weekly_source_verification_outer_transaction on\nselect 1;');
  const second=unit('\\if :{?weekly_source_verification_outer_transaction}\nselect 2;\n\\else\nselect 3;\n\\endif');
  const out=renderFullRehearsalUnits([first,second]);
  assert.equal(out.sql.split('\\unset weekly_source_verification_outer_transaction').length-1,4);
  assert.ok(out.sql.indexOf(first.sql)>out.sql.indexOf('savepoint hr_verifier_0;'));
  assert.ok(out.sql.indexOf('rollback to savepoint hr_verifier_0;')<out.sql.indexOf(second.sql));
  assert.ok(out.sql.includes(second.sql));
  assert.equal(out.sha256,digest(out.sql));
});
test('source definitions remain in the outer transaction and are not isolated like verifier fixtures',()=>{
  const out=renderFullRehearsalUnits([unit('select 1;','repeatable')]);
  assert.ok(!out.sql.includes('savepoint'));assert.equal(out.units[0].clientVariableIsolation,false);
});
test('reviewed verifier paths under tests and native inner rollback-to behavior are retained',()=>{
  const u={...unit('savepoint native_case;\nselect 1;\nrollback to savepoint native_case;\nrelease savepoint native_case;'),file:'tests/13102026_native_case.sql'};
  assert.ok(renderFullRehearsalUnits([u]).sql.includes(u.sql));
  assert.throws(()=>renderFullRehearsalUnits([{...u,kind:'migration'}]),/IDENTITY_REFUSED/);
});
test('transaction or connection escape and error-control override are refused before any script is returned',()=>{
  for(const sql of ['commit;','rollback;','begin;','prepare transaction \'other\';','\\connect other','\\quit','\\! echo unsafe','\\ir other.sql','\\gexec','\\g private-output.txt','\\watch 1','\\setenv OTHER unsafe','\\echo `unsafe-command`','\\pset pager on','\\gset `unsafe-command`','\\set ON_ERROR_STOP on','\\set ON_ERROR_ROLLBACK off']){
    assert.throws(()=>renderFullRehearsalUnits([unit(sql)]),/REFUSED/);
  }
});
test('source identity/hash/path/kind drift refuses rather than using current dirty SQL',()=>{
  for(const changed of [{sqlHash:'0'.repeat(64)},{file:'supabase/verification/../other.sql'},{kind:'unreviewed'},{sql:'select 2;'}]){
    assert.throws(()=>renderFullRehearsalUnits([{...unit('select 1;'),...changed}]),/IDENTITY_REFUSED/);
  }
});
test('only closed SQLSTATE/code and sealed source census fields survive diagnostic sanitization',()=>{
  const map=[{startLine:10,endLine:20,file:'supabase/verification/example.sql',kind:'verifier',statement:2,object:'private.safe_source_owner'}];
  const error=closedRehearsalDiagnostic('psql:<stdin>:15: ERROR: 23505: duplicate key contains confidential fixture payload',map);
  assert.equal(error.error,'DATABASE_DIAGNOSTIC_WITHHELD');assert.equal(error.sqlstate,'23505');assert.equal(error.object,'private.safe_source_owner');
  assert.equal(closedRehearsalDiagnostic('DETAIL: confidential',map),null);
  assert.equal(closedRehearsalDiagnostic('CONTEXT: query text',map),null);
  assert.equal(closedRehearsalDiagnostic('psql:<stdin>:15: ERROR: 55000: SECRET_UPPERCASE_PAYLOAD',map).error,'DATABASE_DIAGNOSTIC_WITHHELD');
  assert.equal(closedRehearsalDiagnostic('psql:<stdin>:15: ERROR: 55000: WEEKLY_SOURCE_HELD',map,new Set(['WEEKLY_SOURCE_HELD'])).error,'WEEKLY_SOURCE_HELD');
});
