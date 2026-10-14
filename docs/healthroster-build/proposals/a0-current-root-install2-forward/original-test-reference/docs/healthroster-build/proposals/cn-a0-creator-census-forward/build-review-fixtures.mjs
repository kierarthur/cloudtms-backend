import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';import {fileURLToPath} from 'node:url';
import {functionEvents,sqlTokens} from './sql-function-source-index.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),root=process.argv[2],critic=process.argv[3];assert.ok(root&&critic);
const sha=b=>crypto.createHash('sha256').update(b).digest('hex'),inputs=[];
function read(p){const b=fs.readFileSync(path.join(root,p));inputs.push({path:p,bytes:b.length,sha256:sha(b)});return b.toString('utf8');}
const qrFile='supabase/repeatable/16122025_timesheet_qr_refuse_restore_planned_delete_audit_rpc.sql',qr=read(qrFile);
const declaration=functionEvents(qr).events.find(e=>e.kind==='CREATE'&&e.name==='public.timesheet_qr_restore_version');assert.ok(declaration);
const asToken=sqlTokens(declaration.definition).find(t=>t.kind==='word'&&t.value==='as');assert.ok(asToken);
const originalHeader=declaration.definition.slice(0,asToken.start);assert.match(originalHeader,/--[^\r\n]*'pending' or 'signed'/i);
const drops=[];
for(const [source,id] of [['supabase/repeatable/06092026_2355_candidate_advanced_expense_completion_v1.sql','public.timesheet_weekly_manual_adjustment_delete_apply(uuid,uuid)'],
 ['supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql','public.timesheet_weekly_chain_delete_apply(uuid,uuid)']]){
 const text=read(source),matches=functionEvents(text).events.filter(e=>e.kind==='DROP'&&e.id===id);assert.equal(matches.length,1);
 const e=matches[0];drops.push({source,line:text.slice(0,e.offset).split('\n').length,id:e.id,kind:e.kind});
}
const census=JSON.parse(fs.readFileSync(path.join(here,'FIRST_CREATOR_CENSUS.json')));
const old=JSON.parse(fs.readFileSync(path.resolve(here,'../cn-a0-phantom-writers/FIRST_CREATOR_CENSUS.json'))),nativeEquality=census.nativeOwners.map(n=>{
 const prior=old.nativeOwners.find(p=>p.name===n.name);assert.ok(prior,n.name);assert.equal(n.definitionSha256,prior.definitionSha256,n.name);return {id:n.id,definitionSha256:n.definitionSha256,priorDefinitionSha256:prior.definitionSha256,byteEqual:true};});
const creatorEquality=census.firstCreators.map(n=>{const prior=old.firstCreators.find(p=>p.name===n.name&&p.inputTypes===n.inputTypes);assert.ok(prior,n.id);assert.equal(n.definitionSha256,prior.definitionSha256,n.id);return {id:n.id,definitionSha256:n.definitionSha256,byteEqual:true};});
const criticBytes=fs.readFileSync(critic);assert.equal(sha(criticBytes),'f2887142f109c35c0861eaed4e43a3b46429dcae5ec3eebba6a87e0812e81b9f');fs.writeFileSync(path.join(here,'A0_CREATOR_SOURCE_CRITIC.md'),criticBytes);
fs.writeFileSync(path.join(here,'OBSERVED_REGRESSION_FIXTURES.json'),JSON.stringify({contract:'HEALTHROSTER_A0_OBSERVED_CENSUS_DEFECT_FIXTURES_V1',inputs,
 originalCommentedDeclaration:{source:qrFile,line:qr.slice(0,declaration.offset).split('\n').length,rawHeader:originalHeader,expectedInputTypes:'uuid,uuid,text,uuid'},
 actualDroppedOverloads:drops,
 nativeOwnerPreservation:nativeEquality,firstCreatorBodyPreservation:creatorEquality,critic:{copiedByteExact:true,sha256:sha(criticBytes)},
 nativeProof:'NONE. Fixtures are exact saved declarations and offline identity/order cases; they do not run SQL or certify native acceptance.'},null,2)+'\n');
console.log(JSON.stringify({observedDeclaration:1,exactDropOverloads:drops.length,nativeOwnersByteEqual:nativeEquality.length,firstCreatorsByteEqual:creatorEquality.length,criticByteEqual:true}));
