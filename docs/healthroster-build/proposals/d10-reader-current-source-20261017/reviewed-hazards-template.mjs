// Finite complete-source and native type-construction proof. No SQL invocation.
import fs from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';
const reviewBase='docs/healthroster-build/proposals/d10-reader-current-source-20261017/';
const expected=__PROOF_PINS__;
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
