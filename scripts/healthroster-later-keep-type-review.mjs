// Finite Later/Keep source construction review. No SQL or permission waiver.
import fs from'node:fs';import path from'node:path';import{fileURLToPath}from'node:url';import{createHash}from'node:crypto';
import{scanManagedSql}from'./cloudtms-managed-release-sql.mjs';
const repoRoot=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
export const reviewPath='docs/healthroster-build/proposals/later-keep-f23-classification-20261017/CLASSIFICATION.json';
export const reviewSha256='326fded2189c8076c0c6a3ed8d1c5bbae6350f3815a142de128bf5a39e606140';
const hash=x=>createHash('sha256').update(x).digest('hex'),canon=x=>x.toString().replaceAll('\r\n','\n'),fail=code=>{throw Error('HEALTH_LATER_KEEP_TYPE_'+code);};
const statements=source=>{const p=scanManagedSql(source);return p.statements.map(x=>p.source.slice(x.start,x.end).trim());};
// Literal callbacks preserve SQL dollar anchors and all JS replacement tokens.
export function reverseLiteralHunks(source,hunks){for(const h of [...hunks].reverse()){const after=canon(h.after),before=canon(h.before);if(source.split(after).length!==2)fail('REVERSE_ANCHOR');source=source.replace(after,()=>before);}return source;}
const column=(relations,table,name,type)=>relations[table]?.columns.some(x=>x.name===name&&x.type===type)===true;
export function provesMissingReplay(body,relations){
 const replay="return v_saved.after_json->'result'||jsonb_build_object('idempotent_replay',true,'historical_proposal',true);";
 return column(relations,'public.audit_events','after_json','jsonb')&&body.includes('v_saved public.audit_events%rowtype;')
  &&body.split(replay).length===3&&body.split('if v_saved_count>1 then raise exception').length===3
  &&body.split('if v_saved_count=1 then').length===3&&body.split('select * into strict v_saved from public.audit_events audit').length===3
  &&body.split("v_saved.actor_user_id is distinct from v_actor or v_saved.after_json->>'request_hash' is distinct from encode(v_request_hash,'hex')").length===3
  &&body.split("or v_saved.after_json->'prepare_request' is distinct from p_request").length===3
  &&body.includes("v_result:=jsonb_build_object('ok',true,'published',false,'decision_bundle_id',v_bundle,'bundle_revision','1',")
  &&body.includes("'idempotent_replay',false);")
  &&body.includes("v_proof:=jsonb_build_object('schema_version','WEEKLY_SOURCE_MISSING_ACCEPT_PROPOSAL_PROOF_V1',")
  &&body.includes("'owner_backend_pid',pg_backend_pid(),'result',v_result);")
  &&body.includes("perform public._audit_insert('weekly_source_entitlement_decision_bundles',v_bundle::text,\n    'WEEKLY_SOURCE_MISSING_ACCEPT_PROPOSED',v_inputs->'current_inventory',v_proof,");
}
export function createReviewedLaterKeepClassifier(root=repoRoot,{tupleProof,classifyPrior,hazardCensus,readFile}={}){
 const read=file=>canon(readFile?readFile(file):fs.readFileSync(path.join(root,file)));
 function load(){
  const raw=readFile?readFile(reviewPath):fs.readFileSync(path.join(root,reviewPath));if(hash(raw)!==reviewSha256)fail('REVIEW_CHANGED');const data=JSON.parse(raw);
  if(data.version!=='HEALTH_LATER_KEEP_FINITE_TYPE_PROPOSAL_V1'||data.native!=='NOT_RUN'||data.qualification!=='SOURCE_TYPES_AND_CONSTRUCTION_ONLY'||data.counts.files!==44||data.counts.rawOccurrences!==233||data.counts.uniquePoints!==223||data.ownerReverses.length!==14)fail('SCOPE_CHANGED');
  for(const pin of data.dependencies)if(hash(read(pin.path))!==pin.sha256)fail('DEPENDENCY_CHANGED');
  const schema=JSON.parse(read(data.schema.path));for(const[relation,proof]of Object.entries(data.schema.relations)){
   if(proof.ddl){if(!statements(read(proof.ddlPath)).includes(proof.ddl))fail('NATIVE_DDL_CHANGED');for(const c of proof.columns)if(!new RegExp('\\b'+c.name+' '+c.type+'\\b').test(proof.ddl))fail('NATIVE_COLUMN_CHANGED');}
   else{const actual=schema.relations.find(x=>x.schema+'.'+x.name===relation);if(!actual||actual.kind!==proof.kind||JSON.stringify(actual.columns)!==JSON.stringify(proof.columns))fail('CATALOGUE_TYPE_CHANGED');}
  }
  for(const recipe of data.ownerReverses){const whole=statements(read(recipe.file)).filter(s=>s.startsWith('create or replace function '+recipe.signature.split('(')[0]+'('));if(whole.length!==1||hash(whole[0])!==recipe.successorStatementSha256)fail('COMPLETE_OWNER_CHANGED');const prior=read(recipe.priorFile).trimEnd();if(hash(prior)!==recipe.priorStatementSha256||reverseLiteralHunks(whole[0],recipe.hunks)!==prior)fail('FULL_OWNER_REVERSE_CHANGED');if(recipe.currentFile&&!statements(read(recipe.currentFile)).includes(prior))fail('ACTUAL_PARENT_CHANGED');}
  const c=data.classifier;if(reverseLiteralHunks(read(c.keep),c.keepHunks)!==read(c.later)||reverseLiteralHunks(reverseLiteralHunks(read(c.later),c.laterHunks),c.missingHunks)!==read(c.entry)||reverseLiteralHunks(read(c.entry),c.entryHunks)!==read(c.kept)||reverseLiteralHunks(read(c.kept),c.keptHunks)!==read(c.prior))fail('CLASSIFIER_REVERSE_CHANGED');
  return data;
 }
 return function reviewedLaterKeepHazards(source,file,baseline,violations,parts){
  if(!/^supabase\/(?:migrations\/17102026_(?:0126_weekly_source_missing_|0141_weekly_source_later_accept_)|repeatable\/17102026_(?:0141_weekly_source_later_accept_health(?:\/|_v1\.sql$)|0209_weekly_source_keep_current_public_read(?:\/|_v1\.sql$)))/.test(file))return baseline;
  const data=load(),entry=data.files.find(x=>x.file===file);if(!entry)return baseline;if(hash(canon(source))!==entry.sha256)fail('CURRENT_SOURCE_CHANGED');
  const observed=new Map();for(const row of violations){const old=observed.get(row.key);observed.set(row.key,{...row,count:(old?.count??0)+1});}if(observed.size!==entry.points.length)fail('POINT_CENSUS_CHANGED');
  const result=new Map(baseline),classifier=[data.classifier.later,data.classifier.keep].includes(file);let originalClassifier;
  for(const point of entry.points){const actual=observed.get(point.key);if(!actual||actual.hazard!==point.hazard||actual.count!==point.count)fail('POINT_CHANGED');const[sig,hazard,sql]=JSON.parse(point.key);let proved=false;
   if(classifier){if(!classifyPrior)fail('PRIOR_REVIEW_REQUIRED');originalClassifier??=classifyPrior(read(data.classifier.prior),data.classifier.prior);const oldKey=JSON.stringify([sig.startsWith('outer:')?'outer:'+data.classifier.prior:sig,hazard,sql]);proved=(originalClassifier.get(oldKey)??0)>=point.count;}
   else if(hazard.startsWith('ROWTYPE_SELECT:')){const relation=hazard.split(':')[2].replaceAll(' ','').toLowerCase(),variable=hazard.split(':')[1],body=parts.filter(p=>p.signature===sig&&p.text.includes(sql));proved=typeof tupleProof==='function'&&tupleProof(actual)&&!!data.schema.relations[relation]&&body.length===1&&!body[0].unclassified&&new RegExp('\\b'+variable+'\\s+'+relation.replaceAll('.','\\.')+'\\s*%\\s*rowtype\\b','i').test(body[0].text);}
   else if(hazard==='JSON_CONCAT'&&sig==='private.weekly_source_missing_accept_prepare_v1(jsonb)'){const body=parts.filter(p=>p.signature===sig);proved=body.length===1&&sql==="return v_saved.after_json->'result'||jsonb_build_object('idempotent_replay',true,'historical_proposal',true);"&&point.count===2&&provesMissingReplay(body[0].text,data.schema.relations);}
   else{const recipe=data.ownerReverses.find(r=>r.file===file&&r.signature===sig);if(recipe?.currentFile&&hazardCensus&&classifyPrior){const prior=read(recipe.currentFile),count=hazardCensus(prior,recipe.currentFile).filter(h=>h.key===point.key).length,old=classifyPrior(prior,recipe.currentFile,baseline);proved=count>=point.count&&(old.get(point.key)??0)>=point.count;}}
   if(!proved)fail('UNPROVED_'+hazard+':'+file+':'+sig);result.set(point.key,Math.max(result.get(point.key)??0,point.count));
  }
  return result;
 };
}
