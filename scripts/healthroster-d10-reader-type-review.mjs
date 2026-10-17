// Finite complete-source and native type-construction proof. No SQL invocation.
import fs from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';
const reviewBase='docs/healthroster-build/proposals/d10-reader-current-source-20261017/';
const expected={
  "GRAMMAR_TYPE_RESULTS.json": "faad64059371bfc5be35a255a24064f98b86198174a807ca44b1107ebd72957d",
  "RAW_F23_CENSUS.json": "f12047227a06db9a48cdee4ea785222a50723da6f5db109c4641634f8e31fae3",
  "parents/supabase/release/current-contract.json": "da0e0870442c151534c04d108757a0d76a812879aafae71f01486c7077ce674a",
  "SOURCE_PROVENANCE.json": "198e08c49480015e655c68cf4a8ac389834fc27eb06037384d9d66eb94efb7fd",
  "SOURCE_OWNER_REVERSES.json": "94016cf755db58eabebb258a72d68e70627e67f2feca72fbf0ffc0289fc13fc6",
  "FULL_PARENT_FORWARD.json": "953a86c10fe108dc7431b858f060215fc40b2501aea3cb82b09b09c73f93421a"
};
const hash=b=>createHash('sha256').update(b).digest('hex');
const fail=code=>{throw Error('HEALTH_D10_FINITE_TYPE_'+code);};
export function createReviewedD10ReaderClassifier(root,{tupleProof,hazardCensus,readFile}={}){
 const read=p=>readFile?readFile(p):fs.readFileSync(path.join(root,p));
 const checked=p=>{const b=read(reviewBase+p);if(hash(b)!==expected[p])fail('PROOF_INPUT_CHANGED');return b;};
 return function reviewedD10ReaderHazards(source,file,baseline=new Map()){
  if(!/^supabase\/repeatable\/17102026_1900_health_d10_query_reader\//.test(file)
   &&file!=='supabase/verification/15092026_1534_weekly_source_acl_contract_v1.sql')return baseline;
  const proof=JSON.parse(checked('GRAMMAR_TYPE_RESULTS.json'));
  const census=JSON.parse(checked('RAW_F23_CENSUS.json'));
  const schema=JSON.parse(checked('parents/supabase/release/current-contract.json'));
  const provenance=JSON.parse(checked('SOURCE_PROVENANCE.json'));
  const inverses=JSON.parse(checked('SOURCE_OWNER_REVERSES.json'));
  const forwards=JSON.parse(checked('FULL_PARENT_FORWARD.json'));
  const entry=proof.grammar.find(x=>x.target===file);
  if(!entry)return baseline;
  if(hash(source.replaceAll('\r\n','\n'))!==entry.sha256)fail('CURRENT_SOURCE_CHANGED');
  const points=proof.f23SourceProof.filter(x=>x.target===file),raw=census.find(x=>x.target===file)?.hazards??[];
  if(points.length!==raw.length)fail('PROOF_CENSUS_CHANGED');
  const actual=hazardCensus(source,file);
  if(actual.length!==raw.length||actual.some((x,i)=>x.key!==raw[i].key||x.hazard!==raw[i].hazard))fail('ACTUAL_CENSUS_CHANGED');
  const out=new Map(baseline);
  for(const point of points){
   const [signature,hazard,sql]=JSON.parse(point.key),e=point.evidence;
   const owner=provenance.owners.find(x=>x.id===signature);
   if(owner){
    if(entry.sha256!==owner.output_sha256)fail('AUTHORED_OWNER_CHANGED');
    const old=inverses.rows.find(x=>x.signature===signature);
    if(old&&(hash(old.after)!==entry.sha256||hash(old.before)!==old.beforeSha256))fail('OWNER_INVERSE_CHANGED');
   }
   let proved=false;
   if(hazard.startsWith('ROWTYPE_SELECT:')){
    const variable=hazard.split(':')[1],relation=hazard.split(':',2)[1]&&hazard.split(':')[2].replaceAll(' ','').toLowerCase();
    const native=schema.relations.find(x=>x.schema+'.'+x.name===relation);
    const columns=Object.fromEntries((native?.columns??[]).map(x=>[x.name,x.type]));
    proved=e.classification==='EXACT_SINGLE_RELATION_ROWTYPE'&&e.relation===relation&&typeof tupleProof==='function'&&tupleProof(actual.find(x=>x.key===point.key))
     &&JSON.stringify(columns)===JSON.stringify(e.schemaColumns)
     &&new RegExp('\\b'+variable+'\\s+'+relation.replaceAll('.','\\.')+'%rowtype\\b','i').test(source);
   }else if(e.classification==='UNCHANGED_CURRENT_MANDATORY_STATEMENT_FULL_PARENT_TRANSFER'){
    const forward=forwards.rows.find(x=>x.target===file);
    const fresh=provenance.owners.filter(x=>!inverses.rows.some(r=>r.signature===x.id)).map(x=>x.id).sort();
    let prior=source;for(const s of fresh){const added="    '"+s+"',\n";if(prior.split(added).length!==2)fail('VERIFIER_LITERAL_DELTA_CHANGED');prior=prior.replace(added,()=>'');}
    proved=!!forward&&prior===forward.before&&hash(prior)===e.completeParentSha256&&prior.includes(sql)&&forward.after===source;
   }else if(e.classification==='EXACT_TYPED_CTE_OUTPUTS'){
    proved=signature==='private.weekly_source_check_only_query_rows_v1(uuid,uuid,jsonb)'
     &&source.includes('v_revision text; v_rows jsonb; v_more boolean;')
     &&source.includes('select * from rows union all select * from source_information')
     &&source.includes("coalesce(jsonb_agg(value order by worker_sort,actual_work_date,item_sort),'[]'::jsonb)")
     &&source.includes('count(*)>v_limit')&&hash(sql)===e.fullStatementSha256;
   }else if(e.classification==='MAX_NATIVE_TIMESTAMPTZ_TO_SAME_TYPE'){
    const native=schema.relations.find(x=>x.schema+'.'+x.name==='public.candidate_notifications');
    proved=native?.columns.some(x=>x.name==='created_at_utc'&&x.type==='timestamp with time zone')
     &&signature==='private.weekly_source_check_only_candidate_reminder_context_v1(uuid,jsonb,text)'
     &&sql.startsWith('select max(n.created_at_utc) into last_reminder from public.candidate_notifications n')
     &&source.includes('last_reminder timestamptz');
   }else if(e.classification==='JSONB_ARRAY_APPEND'){
    proved=signature==='private.weekly_source_check_only_query_groups_v1(uuid,uuid,jsonb)'&&(
     sql==="rows:=rows||page->'rows';"&&source.includes("rows jsonb:='[]'::jsonb")&&source.includes("page->'ok' is distinct from 'true'::jsonb")
     ||sql==="if reminder_action is not null then group_row:=jsonb_set(group_row,'{actions}',group_row->'actions'||jsonb_build_array(reminder_action),false);"
      &&source.includes("'first_seen_at_utc',first_seen,'children',children,'actions',jsonb_build_array(review_action)"));
   }
   if(!proved)fail('SEMANTIC_TYPE_NOT_PROVED');
   out.set(point.key,Math.max(out.get(point.key)??0,raw.filter(x=>x.key===point.key).length));
  }
  return out;
 };
}


// The retained READ proof owns a complete pre-D10 verifier image. Reconstruct
// only that exact image for that proof; the outer D10 proof sees the real image.
export function createReviewedD10SourceReadClassifier(root,{sourceReadFactory,...options}={}){
 if(typeof sourceReadFactory!=='function'||typeof options.hazardCensus!=='function')fail('SOURCE_READ_FACTORY_REQUIRED');
 const retained=sourceReadFactory(root,options);
 if(typeof retained!=='function')fail('SOURCE_READ_CLASSIFIER_REQUIRED');
 const read=p=>options.readFile?options.readFile(p):fs.readFileSync(path.join(root,p));
 const checked=p=>{const b=read(reviewBase+p);if(hash(b)!==expected[p])fail('PROOF_INPUT_CHANGED');return JSON.parse(b);};
 const fileScope='supabase/verification/15092026_1534_weekly_source_acl_contract_v1.sql';
 return function reviewedD10SourceReadHazards(source,file,baseline,violations){
  if(file!==fileScope)return retained(source,file,baseline,violations);
  const forwards=checked('FULL_PARENT_FORWARD.json'),provenance=checked('SOURCE_PROVENANCE.json');
  const inverses=checked('SOURCE_OWNER_REVERSES.json'),raw=checked('RAW_F23_CENSUS.json');
  const rows=forwards.rows.filter(x=>x.target===fileScope);
  if(rows.length!==1)fail('SOURCE_READ_PARENT_REQUIRED');
  const forward=rows[0];
  if(hash(forward.before)!==forward.beforeSha256||hash(forward.after)!==forward.afterSha256
   ||Buffer.byteLength(forward.before)!==forward.beforeBytes||Buffer.byteLength(forward.after)!==forward.afterBytes)fail('SOURCE_READ_PARENT_CHANGED');
  // Preserve the original factory on its exact original image as well.
  if(source===forward.before)return retained(source,file,baseline,violations);
  if(source!==forward.after)fail('SOURCE_READ_CURRENT_SOURCE_CHANGED');
  const fresh=provenance.owners.filter(x=>!inverses.rows.some(r=>r.signature===x.id)).map(x=>x.id).sort();
  const expectedFresh=[
   'private.weekly_source_check_only_candidate_reminder_context_v1(uuid,jsonb,text)',
   'private.weekly_source_check_only_candidate_reminder_subject_v1(jsonb,text,uuid)',
   'private.weekly_source_check_only_issue_filter_keys_v1(text,jsonb)',
   'private.weekly_source_check_only_query_groups_v1(uuid,uuid,jsonb)'];
  if(JSON.stringify(fresh)!==JSON.stringify(expectedFresh))fail('SOURCE_READ_LITERAL_SCOPE_CHANGED');
  let prior=source;
  for(const signature of fresh){
   const line="    '"+signature+"',\n";
   if(prior.split(line).length!==2)fail('SOURCE_READ_LITERAL_DELTA_CHANGED');
   prior=prior.replace(line,()=>'');
  }
  if(prior!==forward.before||hash(prior)!==forward.beforeSha256)fail('SOURCE_READ_WHOLE_PARENT_CHANGED');
  const actual=options.hazardCensus(source,file),old=options.hazardCensus(prior,file);
  const points=raw.find(x=>x.target===file)?.hazards;
  const census=xs=>{
   const m=new Map();for(const x of xs){const p=m.get(x.key);if(p&&p.hazard!==x.hazard)fail('SOURCE_READ_CENSUS_COLLISION');m.set(x.key,{hazard:x.hazard,count:(p?.count??0)+1});}
   return [...m].sort(([a],[b])=>a<b?-1:a>b?1:0);
  };
  const exact=JSON.stringify(census(actual));
  if(!points||exact!==JSON.stringify(census(points))||exact!==JSON.stringify(census(old))
   ||(violations&&exact!==JSON.stringify(census(violations))))fail('SOURCE_READ_CENSUS_CHANGED');
  return retained(prior,file,baseline,old);
 };
}
