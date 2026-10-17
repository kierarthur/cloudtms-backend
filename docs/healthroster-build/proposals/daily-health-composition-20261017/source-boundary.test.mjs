// Adversarial offline source-boundary tests. These do not execute SQL/native money.
import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';
import {validateHealthSource} from './verify.mjs';
const source=fs.readFileSync(new URL('./17102026_0120_daily_health_composition.sql',import.meta.url),'utf8');
test('complete Health support implements actual bounded metadata with no public success or business writes',()=>{
 assert.deepEqual(validateHealthSource(source),{functions:5,publicComposers:0,businessWrites:0});
});
test('source guard rejects removing the post-wait actor recheck',()=>{
 assert.throws(()=>validateHealthSource(source.replace('perform private.daily_rate_correction_health_actor_require_v1(actor,p_lock);','')),/actor rechecked/);
});
test('source guard rejects treating a mutex as refreshing an old snapshot',()=>{
 assert.throws(()=>validateHealthSource(source.replace("is distinct from 'read committed'","is distinct from 'serializable'")),/old snapshot refusal/);
});
test('source guard rejects acquiring shared metadata before native Invoice locks',()=>{
 const line='select x.* into strict locked_o from private.timesheet_contract_move_operations x where x.id=o.id for update;';
 const moved=source.replace(line,'').replace('daily:=private.daily_rate_correction_health_basis_v1(preview_args,true);',line+'\n daily:=private.daily_rate_correction_health_basis_v1(preview_args,true);');
 assert.throws(()=>validateHealthSource(moved),/native then Invoice then metadata/);
});
test('source guard rejects a fake public finish or early financial write',()=>{
 assert.throws(()=>validateHealthSource(source+'\ncreate or replace function public.timesheet_daily_rate_correction_preview_v1('),/no public completion/);
 assert.throws(()=>validateHealthSource(source+'\ninsert into public.timesheets_financials select 1;'),/no business mutation/);
});
test('source guard rejects a partial sealed-review substitute',()=>{
 const unsafe=source.replace('insert into private.timesheet_contract_move_members(','insert into private.timesheet_contract_move_reviews(');
 assert.throws(()=>validateHealthSource(unsafe));
});
