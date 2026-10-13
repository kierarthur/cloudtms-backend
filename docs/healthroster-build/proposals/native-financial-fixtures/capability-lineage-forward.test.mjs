import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';
import {financialReceiptContractOracle} from './capability-receipt-vectors.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url)),read=x=>fs.readFileSync(path.join(dir,x),'utf8');
const id=n=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const receipt={published_timesheet_financial_id:id(1),root_timesheet_id:id(2),client_id:id(3),final_revision_id:id(4),source_unit_outcomes_json:[{movement_id:id(10),movement_economic_hash:'a'.repeat(64)},{movement_id:id(11),movement_economic_hash:'b'.repeat(64)}]};
const financial={id:id(1),timesheet_id:id(2),client_id:id(3),candidate_id:id(5)};
const movement=(n,hash)=>({id:id(n),movement_economic_hash:hash.repeat(64),final_revision_id:id(4),invoice_timesheet_id:id(2),actual_client_id:id(3),candidate_id:id(5),contract_id:id(6),source_profile_kind:'HEALTHROSTER_ACTUAL_ROWS',source_line_kind:'SOURCE_ORDINARY'});
const movements=[movement(10,'a'),movement(11,'b')];
const oracle=(r=receipt,m=movements)=>financialReceiptContractOracle({receipt:r,financial,movements:m});
test('all immutable receipt units map to one exact Contract/Candidate, independent literal census',()=>assert.equal(oracle(),id(6)));
test('zero mappings and one missing/foreign/hash-changed unit refuse rather than selecting matching subset',()=>{
 for(const m of [[],[movements[0]],[movements[0],{...movements[1],candidate_id:id(99)}],[movements[0],{...movements[1],actual_client_id:id(99)}],[movements[0],{...movements[1],final_revision_id:id(99)}],[movements[0],{...movements[1],movement_economic_hash:'c'.repeat(64)}]])assert.throws(()=>oracle(receipt,m),/FINANCIAL_IDENTITY_INVALID/);
});
test('multiple Contracts, invoice-only masquerade and malformed/empty/nonarray units fail closed',()=>{
 assert.throws(()=>oracle(receipt,[movements[0],{...movements[1],contract_id:id(7)}]));
 assert.throws(()=>oracle(receipt,[movements[0],{...movements[1],source_line_kind:'INVOICE_VAT_ROUNDING'}]));
 for(const units of [null,{},'x',[],[null],[{}],[{movement_id:'bad',movement_economic_hash:'a'.repeat(64)}]])assert.throws(()=>oracle({...receipt,source_unit_outcomes_json:units}));
});
test('complete SQL forward keeps every-unit Candidate/array/single-Contract/native identity guards',()=>{
 const s=read('CAPABILITY_SOURCE_CAS_SCHEMA_FORWARD.sqlinc');
 assert.doesNotMatch(s,/financial\.contract_id|root\.candidate_id|root\.family_booking_id/);
 assert.match(s,/jsonb_typeof\(receipt->'source_unit_outcomes_json'\) is distinct from 'array'/);
 assert.match(s,/financial->>'candidate_id'=m\.candidate_id::text/);
 assert.match(s,/having count\(distinct m\.contract_id\)=1/);
 assert.match(s,/min\(m\.contract_id::text\)::uuid/);assert.doesNotMatch(s,/min\(m\.contract_id\)/);
 assert.doesNotMatch(s,/financial\.is_current|financial\.is_stale|root\.is_current|root\.version/);
 assert.match(s,/root->>'client_id' is null or root->>'client_id'=financial->>'client_id'/);
});
test('forward Source owner retains full original NEXT registration bytes and native CAS/growth/immutable guards',()=>{
 const s=read('LINEAGE_OWN_NEW_ROOT_FORWARD.sqlinc'),manifest=JSON.parse(read('LINEAGE_OWN_NEW_ROOT_FORWARD.json'));
 const root=path.resolve(process.env.NFX_REVIEW_SOURCE_ROOT??'../healthroster-v2-backend-13102026');
 const baseline=fs.readFileSync(path.join(root,manifest.input.file),'utf8');
 const native=baseline.match(/create or replace function public\.weekly_source_timesheet_lineage_ensure_atomic_v1\([\s\S]*?\$function\$;/)[0];
 const hook=x=>x.match(/    if v_finalisation_lock_operation\.id is not null then[\s\S]*?    end if;/)[0];
 assert.equal(hook(s),hook(native));assert.equal(manifest.native_next_hook_equal,true);
 assert.match(s,/if v_created_timesheet and v_finalisation_lock_operation\.id is null/);
 assert.match(s,/v_legacy_root_set_now is distinct from v_finalisation_root_set/);
 assert.match(s,/v_known_root_timesheet_id is not null/);
 assert.match(s,/if v_legacy_root_set_now='EMPTY_LOCKED_SET' then[\s\S]*?array\[\]::uuid\[\]/);
 assert.match(s,/v_contract_week\.timesheet_id is distinct from v_known_root_timesheet_id/);
 assert.match(s,/v_lineage_observed is distinct from v_lineage_discovery/);
 assert.match(s,/WEEKLY_SOURCE_TIMESHEET_ROTATED_BEFORE_AUTHORISATION/);
 assert.match(s,/weekly_source_timesheet_lineage_assert_v1\(v_resolution.id\)/);
 assert.ok(s.indexOf('if v_created_timesheet and')>s.indexOf('if coalesce((v_lock_result'));
});
test('frozen capability and predecessor function hash remain unchanged',()=>{
 const m=JSON.parse(read('CAPABILITY_SCHEMA_FORWARD.json'));
 const source=fs.readFileSync(path.resolve(m.input.file));const sha=x=>crypto.createHash('sha256').update(x).digest('hex');
 assert.equal(sha(source),m.input.sha256);
 const review=JSON.parse(fs.readFileSync(path.resolve('docs/healthroster-build/proposals/protected-source-capability/REVIEW_MANIFEST.json')));
 for(const f of review.source_artifacts)assert.equal(sha(fs.readFileSync(path.resolve(f.file))),f.sha256,f.file);
});
