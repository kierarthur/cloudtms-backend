import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';import {fileURLToPath} from 'node:url';
import {parseTriggerIdentity,triggerOrderChecks,sqlTokens} from './sql-function-source-index.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),root=process.argv[2];assert.ok(root);
const sha=b=>crypto.createHash('sha256').update(b).digest('hex'),inputs=[];
function read(p){const b=fs.readFileSync(path.join(root,p));inputs.push({path:p,bytes:b.length,sha256:sha(b)});return b.toString('utf8');}
const c=JSON.parse(read('supabase/release/current-contract.json')),migration=read('supabase/migrations/13102026_1127_healthroster_timesheet_correction_requests_v1.sql');
const census=JSON.parse(fs.readFileSync(path.join(here,'FIRST_CREATOR_CENSUS.json'))),bodies=fs.readFileSync(path.join(here,'NATIVE_CREATOR_INPUT_BODIES.sqlinc'),'utf8');
const fields=new Map(c.relations.map(r=>[r.schema+'.'+r.name,new Set(r.columns.map(v=>v.name))]));
fields.set('private.timesheet_correction_requests',new Set([...migration.matchAll(/^\s{2}([a-z_]\w*)\s+(?:uuid|text|bigint|integer|date|jsonb|timestamptz)\b/gm)].map(m=>m[1])));
const explicit={'public.timesheets':['timesheet_id','booking_id','contract_id','sheet_scope','submission_mode','occupant_key_norm','is_current','version'],
 'public.timesheets_financials':['timesheet_id'],'public.contracts':['id','candidate_id','client_id'],'public.candidates':['id','key_norm','nhsp_hr_name_aliases','updated_at','created_at'],
 'public.timesheet_validations':['timesheet_id'],'public.invoice_lines':['timesheet_id','booking_id'],
 'private.timesheet_correction_requests':['id','state','state_revision','target_revision','origin','reversed_financial_id','frozen_original_json','root_timesheet_id','reversed_timesheet_id',
  'replacement_timesheet_id','original_booking_id','replacement_booking_id','candidate_id','client_id','contract_id']};
const schemaChecks=Object.entries(explicit).map(([relation,columns])=>({relation,columns,missing:columns.filter(n=>!fields.get(relation)?.has(n))}));assert.deepEqual(schemaChecks.flatMap(x=>x.missing),[]);
assert.equal(fields.get('public.timesheets').has('candidate_id'),false);assert.equal(fields.get('public.timesheets').has('family_booking_id'),false);
const protectedFields={candidates:['id','key_norm','nhsp_hr_name_aliases','created_at'],timesheets:['timesheet_id','booking_id','contract_id'],invoice_lines:['timesheet_id','booking_id']},triggerReview=[];
for(const t of census.triggers.filter(x=>/\bBEFORE\b/i.test(x.definition))){
 const {triggerName,routineName}=parseTriggerIdentity(t),owner=census.nativeOwners.find(o=>o.name===routineName);assert.ok(owner,routineName);
 const start=bodies.indexOf('-- INPUT ONLY '+owner.file+' / '+routineName+'\n');assert.ok(start>=0);const end=bodies.indexOf('\n-- INPUT ONLY ',start+10),raw=bodies.slice(start,end<0?undefined:end);
 const tokens=sqlTokens(raw),assignments=[];for(let i=0;i<tokens.length-3;i++)if(tokens[i].kind==='word'&&tokens[i].value==='new'&&tokens[i+1].value==='.'
  &&(['word','identifier'].includes(tokens[i+2].kind))&&(tokens[i+3].value==='='||(tokens[i+3].value===':'&&tokens[i+4]?.value==='=')))assignments.push(tokens[i+2].value);
 const protectedAssignments=assignments.filter(n=>(protectedFields[t.table]||[]).includes(n));assert.deepEqual(protectedAssignments,[],triggerName);
 triggerReview.push({...t,triggerName,routineName,sourceDefinitionSha256:owner.definitionSha256,sourceFile:owner.file,directNewAssignments:[...new Set(assignments)],protectedAssignments,
  conclusion:'Saved actual trigger identity/definition and complete source owner are distinct. Direct identity assignment absent; nested/installed/native effects held.'});
}
const orderChecks=triggerOrderChecks(census.triggers);assert.ok(orderChecks.length);assert.ok(orderChecks.every(x=>x.pass),'A proposed guard does not sort correctly by actual trigger name');
const original=triggerReview.find(t=>t.table==='timesheets'&&t.routineName==='private.bpay_source_timesheet_family_gate_v1');assert.ok(original);
const counterexample={...original,name:'000_preexisting_family_gate',definition:original.definition.replace(/CREATE TRIGGER \S+/,'CREATE TRIGGER "000_preexisting_family_gate"')};
const failures=triggerOrderChecks([counterexample]).filter(c=>!c.pass);assert.ok(failures.length);assert.ok(failures.every(c=>c.routineName===original.routineName));
const holds=['VALIDATION_FIRST_INSERT','INVOICE_FIRST_INSERT','CANDIDATE_ALIAS_RANK','RAW_BOOKING_SIBLING','NATIVE_FIN_LATE_HOOKS'].map(kind=>({kind,status:'HELD_NATIVE_PROOF_REQUIRED'}));
const result={contract:'HEALTHROSTER_A0_CREATOR_SCHEMA_AND_NATIVE_SOURCE_AUDIT_V2',status:'CORRECTED_EVIDENCE_SOURCE_ONLY_NATIVE_HELD',inputs,schemaChecks,triggerReview,triggerOrderChecks:orderChecks,
 observedOrderCounterexample:{triggerName:counterexample.name,routineName:original.routineName,failedChecks:failures,sourceOrDatabaseRenamed:false},
 acl:c.relations.filter(r=>r.schema==='public'&&['timesheets','invoice_lines','timesheet_validations','candidates'].includes(r.name)).map(r=>({relation:'public.'+r.name,
  mutations:r.acl.filter(a=>['INSERT','UPDATE','DELETE'].includes(a.privilege)),foreignKeys:r.constraints.filter(k=>k.type==='f')})),
 invocationEvidence:{validationCreators:census.firstCreators.filter(d=>d.firstCreatorTables.includes('timesheet_validations')).map(d=>({id:d.id,file:d.file,sourceDefinitionSha256:d.definitionSha256})),
  candidateNativeWriters:census.controllingWriterDefinitions.filter(d=>d.writerTables.includes('candidates')).map(d=>({id:d.id,file:d.file,sourceDefinitionSha256:d.definitionSha256,possibleCallees:d.possibleCallees,provenInvokedEdges:[]})),
  note:'Possible source callees/dynamic candidates are never a claim of invocation. Existing five validation HTTP occurrences remain reads; current native SQL admission/body/role/trigger qualification held.'},
 holds,isolation:{savedSourceEnforcesRc:false,qualifiedRuntimeIsolation:null,hiddenRequestStatus:'HELD: earlier RR can hide later P0/WAITING; row intent is not a row-version change or visibility refresh.',ordinaryRrPreserved:true},
 scope:'Evidence tools only. Six SQL helpers/four triggers/two complete successors stay byte-identical in the prior frozen20 packet. Root alone owns SQL/registry/ACL/contract/runtime composition and native qualification.'};
fs.writeFileSync(path.join(here,'CREATOR_SCHEMA_AND_SOURCE_AUDIT.json'),JSON.stringify(result,null,2)+'\n');
console.log(JSON.stringify({relations:schemaChecks.length,fields:schemaChecks.reduce((n,r)=>n+r.columns.length,0),beforeTriggers:triggerReview.length,actualNameOrderChecks:orderChecks.length,counterexampleFailures:failures.length,nativeHolds:holds.length}));
