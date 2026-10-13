import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),root=process.argv[2];assert.ok(root);
const sha=b=>crypto.createHash('sha256').update(b).digest('hex'),inputs=[];
function read(p){const b=fs.readFileSync(path.join(root,p));inputs.push({path:p,bytes:b.length,sha256:sha(b)});return b.toString('utf8');}
const c=JSON.parse(read('supabase/release/current-contract.json'));
const migration=read('supabase/migrations/13102026_1127_healthroster_timesheet_correction_requests_v1.sql');
const census=JSON.parse(fs.readFileSync(path.join(here,'FIRST_CREATOR_CENSUS.json')));
const bodies=fs.readFileSync(path.join(here,'NATIVE_CREATOR_INPUT_BODIES.sqlinc'),'utf8');
const fields=new Map(c.relations.map(r=>[r.schema+'.'+r.name,new Set(r.columns.map(v=>v.name))]));
fields.set('private.timesheet_correction_requests',new Set([...migration.matchAll(/^\s{2}([a-z_]\w*)\s+(?:uuid|text|bigint|integer|date|jsonb|timestamptz)\b/gm)].map(m=>m[1])));
const explicit={
 'public.timesheets':['timesheet_id','booking_id','contract_id','sheet_scope','submission_mode','occupant_key_norm','is_current','version'],
 'public.timesheets_financials':['timesheet_id'],
 'public.contracts':['id','candidate_id','client_id'],
 'public.candidates':['id','key_norm','nhsp_hr_name_aliases','updated_at','created_at'],
 'public.timesheet_validations':['timesheet_id'],
 'public.invoice_lines':['timesheet_id','booking_id'],
 'private.timesheet_correction_requests':['id','state','state_revision','target_revision','origin','reversed_financial_id','frozen_original_json',
   'root_timesheet_id','reversed_timesheet_id','replacement_timesheet_id','original_booking_id','replacement_booking_id','candidate_id','client_id','contract_id']
};
const schemaChecks=Object.entries(explicit).map(([relation,columns])=>({relation,columns,missing:columns.filter(n=>!fields.get(relation)?.has(n))}));
assert.deepEqual(schemaChecks.filter(x=>x.missing.length),[]);
assert.equal(fields.get('public.timesheets').has('candidate_id'),false);assert.equal(fields.get('public.timesheets').has('family_booking_id'),false);
const protectedFields={candidates:['id','key_norm','nhsp_hr_name_aliases','created_at'],timesheets:['timesheet_id','booking_id','contract_id'],invoice_lines:['timesheet_id','booking_id']};
const triggerReview=[];
for(const t of census.triggers.filter(x=>/\bBEFORE\b/i.test(x.definition))){
 const match=t.definition.match(/EXECUTE FUNCTION ([\w.]+)\(/i),name=match[1].includes('.')?match[1]:'public.'+match[1];
 const owner=census.nativeOwners.find(o=>o.name===name);assert.ok(owner,name);
 const start=bodies.indexOf('-- INPUT ONLY '+owner.file+' / '+name+'\n');assert.ok(start>=0);
 const end=bodies.indexOf('\n-- INPUT ONLY ',start+10),raw=bodies.slice(start,end<0?undefined:end);
 const assignments=[...raw.matchAll(/\bnew\.([\w]+)\s*(?::=|=(?!=))/gi)].map(m=>m[1].toLowerCase());
 const protectedAssignments=assignments.filter(n=>(protectedFields[t.table]||[]).includes(n));assert.deepEqual(protectedAssignments,[],t.name);
 triggerReview.push({...t,name,sourceDefinitionSha256:owner.definitionSha256,sourceFile:owner.file,directNewAssignments:[...new Set(assignments)],protectedAssignments,
  conclusion:'Reviewed full saved owner; no direct protected identity assignment. Nested/dynamic effects and installed trigger order remain native gates.'});
}
const acl=c.relations.filter(r=>r.schema==='public'&&['timesheets','invoice_lines','timesheet_validations','candidates'].includes(r.name)).map(r=>({relation:'public.'+r.name,
 mutations:r.acl.filter(a=>['INSERT','UPDATE','DELETE'].includes(a.privilege)),foreignKeys:r.constraints.filter(k=>k.type==='f')}));
const heldKinds=['VALIDATION_FIRST_INSERT','INVOICE_FIRST_INSERT','CANDIDATE_ALIAS_RANK','RAW_BOOKING_SIBLING','NATIVE_FIN_LATE_HOOKS'];
const holds=heldKinds.map(kind=>({kind,status:'HELD_NATIVE_PROOF_REQUIRED',sourceDisposition:{
 VALIDATION_FIRST_INSERT:'New exact signed-precursor parent intent and visible-active native Candidate/I-1 admission; no Timesheet FK invented.',
 INVOICE_FIRST_INSERT:'Retain native archived guard KEY SHARE including booking fallback. New visible-active guard only precedes it; absent exact family still native refusal.',
 CANDIDATE_ALIAS_RANK:'ALL Candidate mutations use exact key/JSON-alias matching. SEND checks normal canonical context against real Contract. No Candidate resolver fork.',
 RAW_BOOKING_SIBLING:'Earliest BEFORE new scoped admission; null/foreign/trimmed collision under active request refused. SEND checks complete native family before allocation.',
 NATIVE_FIN_LATE_HOOKS:'Unchanged call-only closure. Existing creator-trigger locks do not certify the complete late Banking/document/expense graph.'
 }[kind]}));
const invocationEvidence={
 validationCreators:census.firstCreators.filter(d=>d.firstCreatorTables.includes('timesheet_validations')).map(d=>({name:d.name,file:d.file,sourceDefinitionSha256:d.definitionSha256})),
 validationHttp:'All five actual index.js /rest/v1/timesheet_validations sites are reads; proximity method matches are only search candidates.',
 hrSql:'HR Weekly/Daily upserts are derived from persisted selected import-review decisions. The actual left join to Timesheets is not an existence requirement; no missing-parent guarantee claimed.',
 candidateNativeWriters:census.controllingWriterDefinitions.filter(d=>d.writerTables.includes('candidates')).map(d=>({name:d.name,file:d.file,sourceDefinitionSha256:d.definitionSha256,callees:d.callees})),
 candidateHttp:'Existing Candidate service POST/PATCH/DELETE and ordinary SQL mutations all traverse table row triggers when enabled; privileged trigger-disable/replication bypass is not certified.',
 gateEffects:'The unchanged native Candidate try-gate writes existing audit_events. Refusal rolls the containing transaction/subtransaction back; it is not literally read-only.',
 beforeParent:'New validation/raw-booking/Candidate parent intents apply only to native signed WEEKLY/no-ANY-FIN precursor records; native invoice parent intents are retained, never duplicated for ordinary records.',
 ordering:'Active writes: all persisted request Candidate try-gates in sorted UUID order, then one native I-1 whole existing set, then NOWAIT request/parent rows. Existing outer row locks can precede triggers; 1ms/NOWAIT is deliberate truthful contention refusal, not a global deadlock-free certificate.'
};
const result={contract:'HEALTHROSTER_A0_CREATOR_SCHEMA_AND_NATIVE_SOURCE_AUDIT_V1',status:'REVIEW_SOURCE_ONLY_NATIVE_HELD',inputs,schemaChecks,triggerReview,acl,invocationEvidence,holds,
 isolation:{savedSourceEnforcesRc:false,qualifiedRuntimeIsolation:null,
  newPurposeNonRcRefusals:['missing-A0 signed SEND','exact missing-A0 atomic fence','visible active missing-A0 creator'],
  hiddenRequestStatus:'HELD: an earlier RR snapshot can hide newly committed P0/WAITING. Parent row locking alone neither changes the row version nor refreshes transaction visibility.',
  ordinaryRr:'Preserved. No global transaction mode change, parent timestamp/revision bump, fake FK, new mutex/key or alternate authority.'},
 rootIntegration:'Root owns adjacent includes, registry/ACL/contract generation and installation. No standalone enumerated repeatable, root/index mount or DB action is supplied.'};
fs.writeFileSync(path.join(here,'CREATOR_SCHEMA_AND_SOURCE_AUDIT.json'),JSON.stringify(result,null,2)+'\n');
console.log(JSON.stringify({relations:schemaChecks.length,fields:schemaChecks.reduce((n,r)=>n+r.columns.length,0),beforeTriggers:triggerReview.length,nativeHolds:holds.length}));
