// Health READ/classification relocation only. This proves source ancestry,
// never native financial qualification or public READ_RESULT availability.
import fs from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';

export const reviewPath='docs/healthroster-build/proposals/source-current-read-peer-f23-20261017/CLASSIFICATION.json';
export const reviewSha256='7aa875bf450b81be3d945182c67398c4134f3cb325ab88ff2aa089ac99f2a399';
const sha=value=>createHash('sha256').update(value).digest('hex');
const canonical=value=>value.toString().replaceAll('\r\n','\n');
const fail=code=>{throw Error('HEALTH_SOURCE_READ_TYPE_'+code);};
const scoped=file=>file.startsWith('supabase/repeatable/17102026_0722_weekly_source_current_read_health')
  ||file.startsWith('supabase/repeatable/17102026_0741_weekly_source_complete_classification')
  ||file==='supabase/verification/15092026_1534_weekly_source_acl_contract_v1.sql'
  ||file==='supabase/verification/17102026_0722_source_current_read_security.sql'
  ||file==='supabase/verification/17102026_0741_source_complete_security.sql'
  ||file==='supabase/migrations/17102026_0749_source_current_read_private_admission.sql';

function reverse(source,hunks){
  for(const hunk of [...hunks].reverse()){
    const after=canonical(hunk.after),before=canonical(hunk.before);
    if(!after||source.split(after).length!==2)fail('WHOLE_OWNER_REVERSE_ANCHOR');
    source=source.replace(after,()=>before);
  }
  return source;
}
function census(points){
  const result=new Map();
  for(const point of points){
    const prior=result.get(point.key);
    if(prior&&prior.hazard!==point.hazard)fail('CENSUS_COLLISION');
    result.set(point.key,{hazard:point.hazard,count:(prior?.count??0)+1});
  }
  return result;
}

export function createReviewedSourceReadClassifier(root,{classifyPrior,hazardCensus,readFile}={}){
  const raw=file=>readFile?readFile(file):fs.readFileSync(path.join(root,file));
  const read=file=>canonical(raw(file));
  function load(){
    const bytes=raw(reviewPath);
    if(sha(bytes)!==reviewSha256)fail('REVIEW_CHANGED');
    const proof=JSON.parse(bytes);
    if(proof.version!=='HEALTH_SOURCE_READ_FINITE_STATEMENT_TRANSFER_V1'
      ||proof.native!=='NOT_RUN'||proof.qualification!=='UNCHANGED_ACTUAL_OWNER_STATEMENTS_ONLY'
      ||proof.variants.length!==17||new Set(proof.variants.map(v=>v.file)).size!==16
      ||proof.reverses.length!==4||proof.transfers.length!==38
      ||JSON.stringify(proof.stages)!=='[{"files":11,"keys":19},{"files":5,"keys":19}]')fail('SCOPE_CHANGED');
    if(typeof classifyPrior!=='function'||typeof hazardCensus!=='function')fail('PRIOR_PROOF_REQUIRED');
    for(const dependency of proof.dependencies)if(sha(raw(dependency.path))!==dependency.sha256)fail('PRIOR_HELPER_CHANGED');
    for(const parent of proof.actualParents)if(sha(read(parent.source??parent.path))!==parent.sha256)fail('ACTUAL_PARENT_CHANGED');
    for(const variant of proof.variants)if(sha(read(variant.source))!==variant.sha256)fail('EVIDENCE_SOURCE_CHANGED');
    for(const owner of proof.reverses){
      const source=read(owner.output),parent=read(owner.parentSource??owner.parent);
      if(sha(source)!==owner.outputSha256||sha(parent)!==owner.inputSha256
        ||reverse(source,owner.hunks)!==parent)fail('FULL_OWNER_REVERSE_CHANGED');
    }
    return proof;
  }
  return function reviewedSourceReadHazards(source,file,baseline,violations){
    if(!scoped(file))return baseline;
    const proof=load(),normalized=canonical(source);
    const variants=proof.variants.filter(v=>v.file===file&&sha(normalized)===v.sha256);
    if(variants.length!==1)fail('CURRENT_SOURCE_CHANGED');
    const entry=variants[0],actual=census(hazardCensus(normalized,file));
    if(actual.size!==entry.points.length)fail('POINT_CENSUS_CHANGED');
    if(violations){
      const supplied=census(violations);
      if(supplied.size!==actual.size||[...actual].some(([key,value])=>supplied.get(key)?.count!==value.count
        ||supplied.get(key)?.hazard!==value.hazard))fail('CALLER_CENSUS_CHANGED');
    }
    const result=new Map(baseline),priorResults=new Map();
    for(const point of entry.points){
      const found=actual.get(point.key);
      if(!found||found.hazard!==point.hazard||found.count!==point.count)fail('POINT_CHANGED');
      const transfers=proof.transfers.filter(t=>t.file===file&&t.key===point.key);
      // The mandatory verifier has the same one statement in both approved
      // stages; duplicate provenance is acceptable only when identical.
      if(!transfers.length||transfers.some(t=>JSON.stringify(t)!==JSON.stringify(transfers[0])))fail('TRANSFER_CHANGED');
      const transfer=transfers[0],currentKey=JSON.parse(point.key),priorKey=JSON.parse(transfer.priorKey);
      if(currentKey.length!==3||priorKey.length!==3||currentKey[1]!==priorKey[1]||currentKey[2]!==priorKey[2]
        ||currentKey[0]!=='outer:'+file||priorKey[0]!=='outer:'+transfer.priorFile)fail('WHOLE_STATEMENT_CHANGED');
      const parent=proof.actualParents.find(p=>p.path===transfer.priorFile);
      if(!parent)fail('PRIOR_OWNER_MISSING');
      const priorSource=read(parent.source??parent.path);
      if(!priorResults.has(transfer.priorFile)){
        const points=census(hazardCensus(priorSource,transfer.priorFile));
        const qualified=classifyPrior(priorSource,transfer.priorFile,baseline);
        priorResults.set(transfer.priorFile,{points,qualified});
      }
      const prior=priorResults.get(transfer.priorFile);
      if(prior.points.get(transfer.priorKey)?.count!==point.count
        ||(prior.qualified.get(transfer.priorKey)??0)<point.count)fail('PRIOR_STATEMENT_UNPROVED:'+file+':'+point.hazard);
      result.set(point.key,Math.max(result.get(point.key)??0,point.count));
    }
    return result;
  };
}
