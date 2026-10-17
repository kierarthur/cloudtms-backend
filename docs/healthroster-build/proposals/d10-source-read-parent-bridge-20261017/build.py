from pathlib import Path
import json,hashlib,subprocess,os
HERE=Path(__file__).resolve().parent
ROOT=Path('D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026')
OLD=HERE.parent/'d10-reader-current-source-20261017'
def lp(p):return Path('\\\\?\\'+str(Path(p).resolve()))
def read(p):return lp(p).read_bytes()
def put(p,b):
 p=lp(HERE/p);p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b if isinstance(b,bytes) else b.encode())
def doc(p,o):put(p,json.dumps(o,indent=2,ensure_ascii=False)+'\n')
def sha(b):return hashlib.sha256(b).hexdigest()
def git(*a):return subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false','-c','core.longpaths=true',*a],cwd=ROOT,env={**os.environ,'GIT_OPTIONAL_LOCKS':'0','GIT_NO_LAZY_FETCH':'1'})
head=git('rev-parse','HEAD').decode().strip()
assert head=='bb9786de3c4efa4327cbf01cc8da58f285657604'
targets=['scripts/healthroster-d10-reader-type-review.mjs','scripts/bpay-next-source-gates.mjs']
selection=json.loads(read(OLD/'PAYLOAD_SELECTION.json'))
parents=[];forwards=[];outputs=[]
addition=r'''

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
'''
for target in targets:
 row=next(r for r in selection['targets'] if r['target']==target)
 before=read(OLD/row['source'])
 assert sha(before)==row['sha256']
 assert before==read(ROOT/target)==git('show',':'+target),target
 historical=git('show','HEAD:'+target) if target!=targets[0] else None
 if historical is not None:assert historical==read(OLD/'parents'/target)
 put('parents/'+target,before)
 if target==targets[0]:after=before+addition.encode();hunks=[{'before':'','after':addition,'kind':'APPEND'}]
 else:
  changes=[("import {createReviewedD10ReaderClassifier} from './healthroster-d10-reader-type-review.mjs';", "import {createReviewedD10ReaderClassifier,createReviewedD10SourceReadClassifier} from './healthroster-d10-reader-type-review.mjs';"),
   ('const reviewedSourceReadHazards=createReviewedSourceReadClassifier(repoRoot,{','const reviewedSourceReadHazards=createReviewedD10SourceReadClassifier(repoRoot,{\n  sourceReadFactory:createReviewedSourceReadClassifier,')]
  after=before;hunks=[]
  for b,a in changes:
   assert after.count(b.encode())==1
   after=after.replace(b.encode(),a.encode());hunks.append({'before':b,'after':a})
 put('payload/'+target,after)
 forwards.append({'target':target,'before':before.decode(),'after':after.decode(),'beforeSha256':sha(before),'afterSha256':sha(after),'beforeBytes':len(before),'afterBytes':len(after),'hunks':hunks})
 outputs.append({'target':target,'source':'payload/'+target,'sha256':sha(after),'bytes':len(after),'beforeSha256':sha(before),'beforeBytes':len(before)})
 parents.append({'target':target,'capture':'parents/'+target,'savedAndIndexSha256':sha(before),'bytes':len(before),'HEAD':head,'headStatus':'ABSENT_NEW_D10_MODULE' if historical is None else 'PRE_D10_HISTORICAL_PARENT','historicalSha256':None if historical is None else sha(historical)})
oldModule='scripts/healthroster-source-read-type-review.mjs'
b=read(ROOT/oldModule);assert b==git('show',':'+oldModule)==git('show','HEAD:'+oldModule)
put('reference/'+oldModule,b)
doc('PARENTS.json',{'root':str(ROOT),'HEAD':head,'source':'ACTUAL_STAGED_D10_POSTIMAGES_EQUAL_FROZEN86','rows':parents,'unchangedSourceRead':{'target':oldModule,'source':'reference/'+oldModule,'sha256':sha(b),'bytes':len(b)}})
doc('FULL_PARENT_FORWARD.json',{'version':'HEALTH_D10_SOURCE_READ_EAGER_PARENT_BRIDGE_V1','rows':forwards})
doc('PAYLOAD_SELECTION.json',{'version':'HEALTH_D10_SOURCE_READ_EAGER_PARENT_BRIDGE_V1','parent':head,'targets':outputs,'SQLChanges':0,'oldSourceReadModuleChanged':False,'original86':'PRESERVED_HISTORICAL_JOINED_GATE_HOLD'})
proofs=[]
for p in ['GRAMMAR_TYPE_RESULTS.json','RAW_F23_CENSUS.json','SOURCE_PROVENANCE.json','SOURCE_OWNER_REVERSES.json','FULL_PARENT_FORWARD.json','parents/supabase/release/current-contract.json']:
 b=read(OLD/p);put('original-proof/'+p,b);proofs.append({'file':p,'sha256':sha(b),'bytes':len(b)})
doc('ORIGINAL_PROOF_INPUTS.json',{'originalManifestSha256':sha(read(OLD/'MANIFEST.json')),'rows':proofs})
frozen='a6f7423eb71e6e7cd173e705c2290e5408a1d077'
verifier='supabase/verification/15092026_1534_weekly_source_acl_contract_v1.sql'
b=git('show',frozen+':'+verifier);put('reference/frozen-mandatory-verifier.sql',b)
doc('FROZEN_BASELINE.json',{'commit':frozen,'target':verifier,'source':'reference/frozen-mandatory-verifier.sql','bytes':len(b),'sha256':sha(b),'scope':'Exact relevant original F23 baseline owner; counts calculated by actual specialConstructViolations, never copied allowances'})
print(json.dumps({'targets':outputs,'parent':head},indent=2))
