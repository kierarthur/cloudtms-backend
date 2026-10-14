import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {fileURLToPath,pathToFileURL} from 'node:url';
const own=path.dirname(fileURLToPath(import.meta.url));
if(fs.existsSync(path.join(own,'REVIEW_MANIFEST.json')))throw new Error('FROZEN_SOURCE_NO_WRITE');
const repo=path.resolve(own,'../../../..');
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const read=rel=>fs.readFileSync(path.join(own,rel));const checks=[];
const check=(name,fn)=>{checks.push({name,pass:true,...fn()});};
const awaitParser=await import('./source-tools/cloudtms-managed-release-sql.mjs');
const r=JSON.parse(read('lineage/FULL_NATIVE_AND_WORKER_REVERSE.json'));
check('three full current-owner forwards and unique reverses',()=>{
 for(const row of r.rows){assert.equal(sha(row.before),row.before_sha256);assert.equal(sha(row.after),row.after_sha256);assert.equal(sha(read('lineage/'+row.forward_path)),row.forward_file_sha256);let s=row.after;for(const h of [...row.hunks].reverse()){assert.equal(s.split(h.after).length,2);s=s.replace(h.after,()=>h.before);}assert.equal(s,row.before);}
 const old=r.rows[0].before.slice(r.rows[0].body_start,r.rows[0].body_end).trimEnd().replace(/;$/,'');assert.ok(r.rows[0].after.includes(old));return {owners:r.rows.length,hunks:r.rows.reduce((n,x)=>n+x.hunks.length,0),original_source_query_raw_identical:true};
});
check('six complete closest stores with raw evidence pins',()=>{
 const inputs=JSON.parse(read('CLOSEST_STORE_AND_CURRENT_SOURCE_INPUTS.json'));assert.equal(inputs.closest_stores.length,6);for(const row of inputs.closest_stores){assert.equal(sha(read(row.evidence)),row.definition_sha256);assert.match(read(row.evidence).toString(),/create\s+table\s+(?:if\s+not\s+exists\s+)?/i);}return {stores:6};
});
check('owner-private planned store contains no pay/physical authority or grants',()=>{
 const s=read('native/07_check_only_shift_protection_store.sqlinc').toString();const definition=s.slice(s.indexOf('create table '),s.indexOf('\n);')+4);
 assert.ok(!/\b(?:timesheet_id|segment_id|booking_id|invoice_id|paid_at_utc|pay_amount|rate|payment_reference)\s/.test(definition));assert.match(s,/on private.weekly_source_check_only_shift_protections\(candidate_id,work_event_id\) where state='ACTIVE'/);assert.match(s,/check\(coalesce\(\(state='ACTIVE'/);assert.match(s,/old.state<>'ACTIVE'/);assert.match(s,/new.version<>old.version\+1/);assert.match(s,/revoke all on private.weekly_source_check_only_shift_protections from public,anon,authenticated,service_role/);assert.ok(!/grant\s+\w+.*to\s+(?:public|anon|authenticated|service_role)/i.test(s));return {native_state_or_intent_actions_invoked:false};
});
check('exact current effect-free relative import closure',()=>{
 const graph=JSON.parse(read('lineage/TEST_IMPORT_CLOSURE.json'));for(const row of graph.entries){assert.equal(sha(read('lineage/test-mount/'+row.path)),row.mounted_sha256);for(const edge of row.edges)assert.ok(fs.existsSync(path.join(own,'lineage/test-mount',edge.target)));}return {objects:graph.entries.length,edges:graph.entries.reduce((n,x)=>n+x.edges.length,0)};
});
check('actual mapper preserved Source and new Agency lineage refusals',()=>{
 const text=execFileSync(process.execPath,['--test',path.join(own,'lineage/protected-lineage.test.mjs')],{encoding:'utf8'});assert.match(text,/tests 15/);assert.match(text,/pass 15/);assert.match(text,/fail 0/);return {unit_only_tests:15};
});
check('transaction-neutral source SQL envelopes only',()=>{
 const {scanManagedSql,exactAtomicBody}=awaitParser;const files=['native/07_check_only_shift_protection_store.sqlinc','lineage/native/08_complete_native_protected_match.sqlinc','lineage/native/09_complete_native_projection_rows.sqlinc'];const rows=files.map(rel=>{const b=read(rel);exactAtomicBody(b.toString());return {path:rel,raw_sha256:sha(b),statements:scanManagedSql(b.toString()).statements.length};});return {files:3,rows,postgres_compilation:false};
});

check('pure source JavaScript syntax',()=>{const graph=JSON.parse(read('lineage/TEST_IMPORT_CLOSURE.json'));const files=[...graph.entries.map(r=>'lineage/test-mount/'+r.path),'lineage/protected-lineage.test.mjs','check-source.mjs','prepare-protection-lineage-review.mjs','source-tools/cloudtms-managed-release-sql.mjs'];for(const rel of files)execFileSync(process.execPath,['--check',path.join(own,rel)]);return {files:files.length};});
check('high confidence credential scan',()=>{const files=[];function walk(d){for(const e of fs.readdirSync(d,{withFileTypes:true})){const f=path.join(d,e.name);if(e.isDirectory())walk(f);else files.push(f);}}walk(own);const rules=[/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/,/eyJ[A-Za-z0-9_-]{30,}\.[A-Za-z0-9_-]{30,}\.[A-Za-z0-9_-]{20,}/,/gh[pousr]_[A-Za-z0-9]{30,}/,/AKIA[0-9A-Z]{16}/];for(const f of files)assert.equal(rules.some(r=>r.test(fs.readFileSync(f,'utf8'))),false,'CREDENTIAL_MATERIAL:'+path.relative(own,f));return {files:files.length,patterns:4,findings:0};});
fs.writeFileSync(path.join(own,'SOURCE_CHECKS.json'),JSON.stringify({schema:'AGENCY_CHECK_ONLY_PROTECTION_LINEAGE_SOURCE_CHECKS_V1',source_only:true,implementation_complete:false,native_compiled:false,native_qualified:false,private_protection_actions_present:false,native_nx_ai_6_present:false,paid_writer_purpose_present:false,public_checklist_owner_present:false,compiled_contract_sha256:null,checks},null,2)+'\n');
console.log(JSON.stringify({checks:checks.length,pass:true,native_qualified:false}));
