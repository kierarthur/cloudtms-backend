// Finite source tuple/cardinality/JSON/DDL review; no native or financial authority.
import fs from'node:fs';import path from'node:path';import{fileURLToPath}from'node:url';import{createHash}from'node:crypto';
import{scanManagedSql}from'./cloudtms-managed-release-sql.mjs';
const repoRoot=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
export const reviewPath='docs/healthroster-build/proposals/kept-entry-f23-classification-20261017/CLASSIFICATION.json';
export const reviewSha256='6309ea0cadd0d2cd3e98537c145cd8bb49aef984d74bbfe0f11075f54982a874';
const hash=x=>createHash('sha256').update(x).digest('hex'),canon=x=>x.toString().replaceAll('\r\n','\n'),fail=code=>{throw Error('HEALTH_KEPT_ENTRY_TYPE_'+code);};
const signature=sql=>{const m=/^create\s+or\s+replace\s+function\s+([\w.]+)\s*\(([^)]*)\)/i.exec(sql);return m?m[1]+'('+m[2].split(',').map(p=>p.trim()).filter(Boolean).map(p=>p.split(/\s+/)[1]).join(',')+')':null;};
const statements=source=>{const p=scanManagedSql(source);return p.statements.map(x=>p.source.slice(x.start,x.end).trim());};
function reverse(source,hunks){for(const h of [...hunks].reverse()){const after=canon(h.after),before=canon(h.before);if(source.split(after).length!==2)fail('REVERSE_ANCHOR');source=source.replace(after,before);}return source;}
const column=(relations,table,name,type)=>relations[table]?.columns.some(x=>x.name===name&&x.type===type)===true;

// COUNT and MIN consume the very same native UUID rows. The UUID cannot escape
// on a multirow result; this is identity/cardinality, never economic arithmetic.
export function provesRekeyCardinality(body,statement,relations){
 if(!column(relations,'public.weekly_source_group_clients','client_id','uuid')||!column(relations,'public.weekly_work_events','id','uuid')||!/\bv_count bigint; v_prior uuid;/.test(body))return false;
 if(statement.includes('min(gc.client_id::text)::uuid'))return /select count\(\*\),min\(gc\.client_id::text\)::uuid into v_count,v_client\s+from public\.weekly_source_group_clients gc join public\.clients c on c\.id=gc\.client_id\s+where[^;]+;\s+if v_count<>1 then v_client:=null; end if;/.test(body)
   &&body.indexOf('if v_count<>1 then v_client:=null; end if;')<body.indexOf('if v_choice.client_id is not null then v_client:=v_choice.client_id; end if;')
   &&body.includes("if v_client is null or (v_cycle.scope_client_id is not null and v_cycle.scope_client_id is distinct from v_client)");
 if(statement.includes('min(e.id::text)::uuid'))return /select count\(\*\),min\(e\.id::text\)::uuid into v_count,v_prior\s+from public\.weekly_work_events e\s+join lateral\(select r\.\* from public\.weekly_source_row_resolutions r/.test(body)
   &&/if v_count>1 then\s+raise exception 'WEEKLY_SOURCE_IDENTICAL_REKEY_AMBIGUOUS'[\s\S]*?end if;\s+if v_count=0 then return null; end if;\s+return v_prior;/.test(body);
 return false;
}

// The complete typed writer constructs result as an object; only the replay
// boolean is overlaid on that native stored object, at both replay points.
export function provesKeptReplay(body,relations){
 const replay="return v_audit.after_json->'result'||jsonb_build_object('idempotent_replay',true);";
 return column(relations,'public.audit_events','after_json','jsonb')&&body.includes('v_audit public.audit_events%rowtype;')
  &&body.split(replay).length===3&&body.split('select audit.* into v_audit from public.audit_events audit').length===3
  &&body.split("v_audit.after_json->'request' is distinct from p_request").length===3
  &&body.split("v_audit.after_json->>'request_sha256' is distinct from encode(v_request_hash,'hex')").length===3
  &&body.split('v_audit.actor_user_id is distinct from v_actor').length===3
  &&body.includes("v_result:=jsonb_build_object('ok',true,'decision','KEEP_AS_PAID','decision_id',v_decision.id,")
  &&body.includes("'acknowledgement_sequence',v_sequence::text,'idempotent_replay',false,'pay_changed',false,'invoice_changed',false);")
  &&body.includes("'request_sha256',encode(v_request_hash,'hex'),'request',p_request,'result',v_result,");
}
export function provesKeptOwnerPolicy(statement){return statement==="begin\n  execute pg_catalog.format('create policy cloudtms_miget_service_owner_all on public.weekly_source_kept_as_paid_decisions for all to %I,service_role using (true) with check (true)',current_user);";}

// The read override is an offline drift-test boundary. It cannot supply another
// review hash or scope. tupleProof is the existing same-table tuple checker;
// classifyPrior runs the existing aggregate/dynamic review on its original file.
export function createReviewedKeptEntryClassifier(root=repoRoot,{tupleProof,classifyPrior,hazardCensus,readFile}={}){
 const read=file=>canon(readFile?readFile(file):fs.readFileSync(path.join(root,file)));
 function load(){
  const raw=readFile?readFile(reviewPath):fs.readFileSync(path.join(root,reviewPath));if(hash(raw)!==reviewSha256)fail('REVIEW_CHANGED');const data=JSON.parse(raw);
  if(data.version!=='HEALTH_KEPT_ENTRY_FINITE_TYPE_PROPOSAL_V1'||data.native!=='NOT_RUN'||data.qualification!=='SOURCE_TYPES_AND_CONSTRUCTION_ONLY'||data.counts.files!==19||data.counts.rawOccurrences!==94||data.counts.uniquePoints!==93)fail('SCOPE_CHANGED');
  for(const pin of data.inputs)if(hash(read(pin.path))!==pin.sha256)fail('DEPENDENCY_CHANGED');
  const schema=JSON.parse(read(data.schema.path));for(const[relation,proof]of Object.entries(data.schema.relations)){
   if(proof.ddl){if(!statements(read(proof.ddlPath)).includes(proof.ddl))fail('NATIVE_DDL_CHANGED');for(const c of proof.columns)if(!new RegExp('\\b'+c.name+' '+c.type+'\\b').test(proof.ddl))fail('NATIVE_COLUMN_CHANGED');}
   else{const actual=schema.relations.find(x=>x.schema+'.'+x.name===relation);if(!actual||actual.kind!==proof.kind||JSON.stringify(actual.columns)!==JSON.stringify(proof.columns))fail('CATALOGUE_TYPE_CHANGED');}
  }
  for(const recipe of data.ownerReverses){const whole=statements(read(recipe.file)).filter(s=>signature(s)===recipe.signature);if(whole.length!==1||hash(whole[0])!==recipe.successorStatementSha256)fail('COMPLETE_OWNER_CHANGED');const current=read(recipe.currentFile).trimEnd();if(hash(current)!==recipe.currentStatementSha256||reverse(whole[0],recipe.hunks)!==current||!read(recipe.priorFile).includes(current))fail('FULL_OWNER_REVERSE_CHANGED');}
  const c=data.classifier;if(reverse(read(c.kept),c.keptHunks)!==read(c.prior)||reverse(read(c.entry),c.entryHunks)!==read(c.kept))fail('CLASSIFIER_REVERSE_CHANGED');
  return data;
 }
 return function reviewedKeptEntryHazards(source,file,baseline,violations,parts){
  // This list is scope, not approval: all full-source, statement and semantic
  // checks below still run before any new key can enter the allowance map.
  if(!/^supabase\/(?:migrations\/17102026_(?:0113_weekly_source_kept_|0135_correction_entry_admission)|repeatable\/17102026_(?:0113_weekly_source_kept_pay_rekey\/|0135_correction_entry_reader\/))/.test(file))return baseline;
  const data=load(),entry=data.files.find(x=>x.file===file);if(!entry)return baseline;if(hash(canon(source))!==entry.sha256)fail('CURRENT_SOURCE_CHANGED');
  const observed=new Map();for(const row of violations){const old=observed.get(row.key);observed.set(row.key,{...row,count:(old?.count??0)+1});}if(observed.size!==entry.points.length)fail('POINT_CENSUS_CHANGED');
  const result=new Map(baseline),classifier=[data.classifier.kept,data.classifier.entry].includes(file);let prior;
  for(const point of entry.points){const actual=observed.get(point.key);if(!actual||actual.hazard!==point.hazard||actual.count!==point.count)fail('POINT_CHANGED');const[sig,hazard,sql]=JSON.parse(point.key);let proved=false;
   if(classifier){if(!classifyPrior)fail('PRIOR_REVIEW_REQUIRED');prior??=classifyPrior(read(data.classifier.prior),data.classifier.prior);const oldKey=JSON.stringify([sig.startsWith('outer:')?'outer:'+data.classifier.prior:sig,hazard,sql]);proved=(prior.get(oldKey)??0)>=point.count;}
   else if(hazard.startsWith('ROWTYPE_SELECT:')){const relation=hazard.split(':')[2].replaceAll(' ','').toLowerCase(),variable=hazard.split(':')[1],body=parts.filter(p=>p.signature===sig&&p.text.includes(sql));proved=typeof tupleProof==='function'&&tupleProof(actual)&&!!data.schema.relations[relation]&&body.length===1&&!body[0].unclassified&&new RegExp('\\b'+variable+'\\s+'+relation.replaceAll('.','\\.')+'\\s*%\\s*rowtype\\b','i').test(body[0].text);}
   else if(hazard==='UUID_CARDINALITY_UNCLASSIFIED'&&sig==='private.weekly_source_identical_rekey_prior_v1(uuid)'){const body=parts.filter(p=>p.signature===sig);proved=body.length===1&&provesRekeyCardinality(body[0].text,sql,data.schema.relations);}
   else if(hazard==='JSON_CONCAT'&&sig==='public.weekly_source_keep_as_paid_decide_atomic_v1(jsonb)'){const body=parts.filter(p=>p.signature===sig);proved=body.length===1&&sql==="return v_audit.after_json->'result'||jsonb_build_object('idempotent_replay',true);"&&point.count===2&&provesKeptReplay(body[0].text,data.schema.relations);}
   else if(hazard==='DYNAMIC_SQL_UNCLASSIFIED'&&file.endsWith('10_decision_schema.sql')){
    if(sql.includes('$immutable_expected$')){const literal=sql.match(/\$immutable_expected\$([\s\S]*?)\$immutable_expected\$/)?.[1],guard=parts.filter(p=>p.signature==='private._weekly_source_immutable_record_guard_v1()');proved=guard.length===1&&literal?.trim()===guard[0].text.trim()&&sql.includes("or not p.prosecdef or p.prokind<>'f' or pg_get_userbyid(p.proowner) not in(current_user,'postgres')");}
    else proved=provesKeptOwnerPolicy(sql);
   }
   // Non-tuple statements copied from a complete prior owner retain its exact
   // signature/literal key and original classification, after the full reverse.
   // A reverse alone never creates an allowance for a previously unproved key.
   else{const recipe=data.ownerReverses.find(r=>r.file===file&&r.signature===sig);if(recipe&&hazardCensus){const original=read(recipe.priorFile),observedOld=hazardCensus(original,recipe.priorFile).filter(h=>h.key===point.key).length,old=classifyPrior?classifyPrior(original,recipe.priorFile,baseline):baseline;proved=observedOld>=point.count&&(old.get(point.key)??0)>=point.count;}}
   if(!proved)fail('UNPROVED_'+hazard+':'+file+':'+sig);result.set(point.key,Math.max(result.get(point.key)??0,point.count));
  }return result;
 };
}
