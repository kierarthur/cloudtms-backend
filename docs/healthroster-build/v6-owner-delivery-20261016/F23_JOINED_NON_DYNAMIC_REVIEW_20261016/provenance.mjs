import fs from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath,pathToFileURL} from 'node:url';
const root=process.argv[2],here=path.dirname(fileURLToPath(import.meta.url));
if(!root)throw Error('ROOT_REQUIRED');
const commit='c1ebb691feb6a11324060e0c28210e2bd735624a',sha=x=>createHash('sha256').update(x).digest('hex');
const frozen=(file,ref=commit)=>execFileSync('git',['show',`${ref}:${file}`],{cwd:root,maxBuffer:30_000_000});
const json=file=>JSON.parse(frozen(file));
const {scanManagedSql}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')));
const {routineDefinitions}=await import(pathToFileURL(path.join(root,'scripts/bpay-next-final-definers.mjs')));
const norm=s=>s.replaceAll('timestamptz','timestamp with time zone');
const D=JSON.parse(fs.readFileSync(path.join(here,'DISCOVERY.json')));
const invoicePath='docs/healthroster-build/v6-owner-delivery-20261016/INVOICE_SOURCE_ASSEMBLY_20261016/ASSEMBLY_MANIFEST.json';
const candidatePath='docs/healthroster-build/v6-owner-delivery-20261016/CANDIDATE_SOURCE_COMPOSITION_20261016/JOINED_SQL_MAP.json';
const im=json(invoicePath),cm=json(candidatePath);
const oldPaths={rowtype:'docs/healthroster-build/v6-owner-delivery-20261016/ROWTYPE_TUPLE_TYPE_REVIEW.json',aggregate:'docs/healthroster-build/v6-owner-delivery-20261016/F23_AGGREGATE_SOURCE_REVIEW_20261016/AGGREGATE_REVIEW.json',uuid:'docs/healthroster-build/v6-owner-delivery-20261016/F23_UUID_CARDINALITY_REVIEW.json'};
const op=Object.fromEntries(Object.entries(oldPaths).map(([k,p])=>[k,json(p)]));
const prior=[...op.rowtype.files.flatMap(f=>f.rows.map(r=>({...r,file:f.path,sourceSha256:f.sha256,review:oldPaths.rowtype}))),...op.aggregate.rows.map(r=>({...r,review:oldPaths.aggregate})),...op.uuid.rows.flatMap(r=>r.violations.map(v=>({...v,signature:r.signature??JSON.parse(v.key)[0],hazard:'UUID_CARDINALITY_UNCLASSIFIED',file:r.file,sourceSha256:r.frozen_file_sha256,review:oldPaths.uuid})))];
const cache=new Map();
function functions(file,ref=commit){const key=ref+':'+file;if(cache.has(key))return cache.get(key);const bytes=frozen(file,ref),source=bytes.toString('utf8').replace(/\r\n/g,'\n'),defs=new Map();for(const s of scanManagedSql(source).statements){const text=source.slice(s.start,s.end).trim();for(const sig of routineDefinitions(text,file))defs.set(norm(sig),{text,sha256:sha(text)});}const data={bytes,source,defs};cache.set(key,data);return data;}
const reviewed=[];
for(const f of D.functions){
 let p;
 if(f.file.includes('invoice_native_authority')){
  const fn=im.functions.filter(x=>norm(x.signature)===f.signature);if(fn.length!==1||fn[0].statementSha256!==f.routineSha256)throw Error('INVOICE_FUNCTION_PIN '+f.signature);
  const payload=im.payloads.filter(x=>x.path===fn[0].source);if(payload.length!==1)throw Error('INVOICE_PAYLOAD '+f.signature);
  p={kind:'INVOICE_COMPLETE_FUNCTION',commit:payload[0].sourceCommit,path:fn[0].source,fileSha256:payload[0].rawSha256,record:fn[0]};
 }else{
  const fn=cm.functions.filter(x=>norm(x.signature)===f.signature);if(fn.length!==1)throw Error('CANDIDATE_FUNCTION_PIN '+f.signature);p={kind:'CANDIDATE_COMPLETE_FUNCTION',...fn[0]};
 }
 const origin=functions(p.path,p.commit),original=origin.defs.get(f.signature);
 if(sha(origin.bytes)!==p.fileSha256||!original||original.sha256!==f.routineSha256)throw Error('ORIGIN_NOT_EXACT '+f.signature);
 const counterparts=[];
 for(const file of [...new Set(prior.filter(x=>norm(x.signature)===f.signature).map(x=>x.file))]){
  const reviewedRows=prior.filter(x=>x.file===file&&norm(x.signature)===f.signature),prev=functions(file),old=prev.defs.get(f.signature);
  if(!old)throw Error('OLD_FUNCTION_NOT_FOUND '+f.signature+' '+file);
  for(const row of reviewedRows)if(row.sourceSha256&&row.sourceSha256!==sha(prev.bytes))throw Error('OLD_REVIEW_FILE_DRIFT '+file);
  const currentRows=D.rows.filter(r=>r.signature===f.signature);
  counterparts.push({file,sourceSha256:sha(prev.bytes),completeFunctionSha256:old.sha256,completeFunctionEqual:old.sha256===f.routineSha256,priorReviewPaths:[...new Set(reviewedRows.map(r=>r.review))],priorReviewedKeys:[...new Set(reviewedRows.map(r=>r.keySha256??r.key_sha256))],exactCurrentKeyMatches:currentRows.filter(r=>reviewedRows.some(x=>(x.keySha256??x.key_sha256)===r.keySha256)).map(r=>r.id),approvalLimit:'Exact statement/key equality is recorded separately. A changed complete function receives no inherited whole-function approval.'});
 }
 reviewed.push({...f,producer:{kind:p.kind,commit:p.commit,path:p.path,fileSha256:p.fileSha256,completeFunctionSha256:original.sha256,completeFunctionByteEqual:true},oldReviewedCounterparts:counterparts,priorCounterpartStatus:counterparts.length?'COMPARED_NOT_TRANSFERRED':'NO_PRIOR_REVIEWED_SAME_SIGNATURE_COUNTERPART',currentReviewIds:D.rows.filter(r=>r.signature===f.signature).map(r=>r.id)});
}
const dependencies=[['private._invoice_generation_vat_policy_batch(jsonb)','numeric VAT-policy output'],['private._invoice_reference_rows_batch(uuid[])','text row_key/ref_target/day_ymd reference output'],['private.timesheet_contract_current_placement_v1(uuid)','single JSONB H103 actual-current placement output']].map(([signature,role])=>{
 const file=signature.includes('current_placement')?D.filePins.find(f=>f.file.includes('candidate_native_authority')).file:D.filePins.find(f=>f.file.includes('invoice_native_authority')).file;
 const d=functions(file),f=d.defs.get(signature);if(!f)throw Error('TYPE_DEPENDENCY_MISSING '+signature);
 const delimiter=f.text.match(/\bas\s+(\$[a-z_]*\$)/i);if(!delimiter)throw Error('TYPE_DECLARATION_MISSING');
 return {signature,role,file,sourceSha256:sha(d.bytes),completeFunctionSha256:f.sha256,declaration:f.text.slice(0,delimiter.index),qualification:'Exact declaration/body pin supporting only the current finite binding/cardinality proof; no new whole-function approval'};
});
const result={version:'F23_JOINED_NON_DYNAMIC_COMPLETE_PROVENANCE_V1',sourceCommit:commit,qualification:'EXACT_SOURCE_PROVENANCE_AND_COMPARISON_NOT_BEHAVIORAL_APPROVAL',manifestPins:[invoicePath,candidatePath,...Object.values(oldPaths)].map(p=>({path:p,sha256:sha(frozen(p))})),counts:{completeFunctions:reviewed.length,exactProducerWholeFunctions:reviewed.filter(r=>r.producer.completeFunctionByteEqual).length,functionsWithOldCounterparts:reviewed.filter(r=>r.oldReviewedCounterparts.length).length,exactOldWholeFunctions:reviewed.flatMap(r=>r.oldReviewedCounterparts).filter(r=>r.completeFunctionEqual).length,diagnosticsWithExactPriorKey:D.rows.filter(r=>r.priorExactKeyMatches.length).length},functions:reviewed,nativeBindingDependencies:dependencies,native:'NOT_RUN'};
fs.writeFileSync(path.join(here,'PROVENANCE.json'),JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify({counts:result.counts,sha256:sha(fs.readFileSync(path.join(here,'PROVENANCE.json')))},null,2));
