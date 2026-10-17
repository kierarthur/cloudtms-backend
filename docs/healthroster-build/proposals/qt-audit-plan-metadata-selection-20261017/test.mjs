import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {createHash} from 'node:crypto';
import {buildInput,auditQuery,settingNames} from './selection.mjs';
const identity={physicalDatabase:'cloudtms_test_clone',databaseOid:1234,backendPid:4321,
 turnId:'hr_turn_'+'1'.repeat(32),key:[20261013,1],takenAt:'2026-10-17T13:00:00.000Z',task:'SYNTHETIC SOURCE TEST ONLY'};
const copy=v=>JSON.parse(JSON.stringify(v)),sha=v=>createHash('sha256').update(v).digest('hex');
test('exact unchanged current full audit query and its two original occurrences',()=>{
 const q=JSON.parse(fs.readFileSync(new URL('./QUERY.json',import.meta.url)));
 const original=fs.readFileSync(new URL('./reference/qt-assertions.sql',import.meta.url));
 assert.equal(sha(original),q.sha256);assert.equal(auditQuery,q.unchangedFullAuditQuery);
 assert.equal(sha(auditQuery),q.querySha256);assert.equal(original.toString().split(auditQuery).length-1,2);
});
test('fixed packet contains only readonly metadata and non-ANALYZE original query plan',()=>{
 const v=buildInput(identity);
 assert.equal(v.input.database,'agency_test');assert.equal(v.receipt.mainStatements,8);assert.equal(v.receipt.freshStatements,1);
 assert.deepEqual([v.input.continue_on_error,v.input.max_rows_per_statement,v.input.statement_timeout_ms,v.input.lock_timeout_ms],[false,1,30000,2000]);
 assert(v.input.sql.includes("set local jit=off;\nset local pg_show_plans.is_enabled=off;\nset local work_mem='4MB';\nset local maintenance_work_mem='32MB';\nset local search_path=pg_catalog,public;"));
 assert(v.input.sql.endsWith('explain (format json,verbose true,settings true) '+auditQuery+';'));
 assert.equal(/\banalyze\b/i.test(v.input.sql),false);
 assert.equal(/pg_stat_activity|pg_stat_statements|sourcefile|sourceline|password|storage-state|PRIVATE_BINDING/.test(v.input.sql),false);
 assert(v.input.sql.startsWith('set local transaction_read_only=on;\n'));
 assert.equal(v.receipt.sqlSha256,sha(v.input.sql));assert.equal(v.receipt.verificationSqlSha256,sha(v.input.verification_sql));
 assert.equal(v.receipt.effectQualified,false);assert.equal(v.receipt.restorationClaimed,false);
});
test('all fixed settings remain literal, bounded and present-or-missing rather than fabricated',()=>{
 assert.equal(new Set(settingNames).size,settingNames.length);assert.equal(settingNames.length,26);
 const s=buildInput(identity).input.sql;
 for(const name of settingNames)assert(s.includes("'"+name+"'"));
 assert(s.includes('left join pg_catalog.pg_settings'));
 assert(s.includes("'present',s.name is not null"));
});
test('actual turn is copied/frozen and caller strings never become SQL',()=>{
 const original=copy(identity),a=buildInput(original);original.backendPid=9999;original.key[0]=0;
 assert.equal(a.receipt.identity.backendPid,4321);assert.equal(a.receipt.identity.key[0],20261013);
 assert.throws(()=>{a.receipt.identity.key[0]=0;},TypeError);
 const text=buildInput({...identity,task:"public source task '; SELECT secret; --"});
 assert.equal(text.input.sql,a.input.sql);assert.equal(text.input.verification_sql,a.input.verification_sql);
 assert.equal(Object.keys(a.receipt.identity).length,7);
});
for(const [name,change] of [['wrong DB',v=>v.physicalDatabase='other'],['missing task',v=>delete v.task],
 ['extra field',v=>v.actor_user_id='not accepted'],['SQL PID',v=>v.backendPid='4321;select 1'],
 ['unsafe OID',v=>v.databaseOid=9007199254740992],['wrong key',v=>v.key=[20261013,2]],
 ['malformed turn',v=>v.turnId='hr_turn_bad'],['impossible timestamp',v=>v.takenAt='2026-02-31T10:00:00.000Z']])
 test('refuse '+name,()=>{const v=copy(identity);change(v);assert.throws(()=>buildInput(v),/QT_PLAN_ORIGINAL_TURN_REQUIRED/);});
