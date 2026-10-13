import test from 'node:test';
import assert from 'node:assert/strict';
import {digest} from '../scripts/cloudtms-managed-release-sql.mjs';
import {renderFullRehearsalUnits,closedRehearsalDiagnostic} from '../scripts/healthroster-full-rehearsal-protocol.mjs';
const unit=(sql,kind='verifier')=>({file:'supabase/verification/13102026_example.sql',kind,sql,sqlHash:digest(sql)});

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
test('transaction or connection escape and error-control override are refused before any script is returned',()=>{
  for(const sql of ['commit;','rollback;','begin;','prepare transaction \'other\';','\\connect other','\\quit','\\! echo unsafe','\\ir other.sql','\\gexec','\\set ON_ERROR_STOP on','\\set ON_ERROR_ROLLBACK off']){
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
