// Tests the actual proposed helper against complete c1ebb sources. No SQL runs.
import fs from'node:fs';import path from'node:path';import os from'node:os';import assert from'node:assert/strict';import{execFileSync}from'node:child_process';import{createHash}from'node:crypto';import{fileURLToPath,pathToFileURL}from'node:url';
const integrator=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..'),dir=path.join(integrator,'docs/healthroster-build/v6-owner-delivery-20261016/F23_DYNAMIC_SOURCE_REVIEW_20261016');
const hash=x=>createHash('sha256').update(x).digest('hex'),commit='c1ebb691feb6a11324060e0c28210e2bd735624a';
const {createReviewedHealthDynamicClassifier,reviewPath}=await import('../scripts/healthroster-dynamic-source-review.mjs');
const {specialConstructViolations,executableParts}=await import(pathToFileURL(path.join(integrator,'scripts/bpay-next-source-gates.mjs')));
const approval=JSON.parse(fs.readFileSync(path.join(dir,'CLASSIFICATION.json'))),review=JSON.parse(fs.readFileSync(path.join(dir,'REVIEW.json')));
const fixtureParent=path.join(integrator,'.codex-tmp/f23-finite-classifier-tests-20261016');fs.mkdirSync(fixtureParent,{recursive:true});const fixture=fs.mkdtempSync(path.join(fixtureParent,'dynamic-'));
const save=(file,source)=>{fs.mkdirSync(path.dirname(path.join(fixture,file)),{recursive:true});fs.writeFileSync(path.join(fixture,file),source);};
const sources=new Map(),parsed=new Map(),tests=[];
function run(name,fn){fn();tests.push({name,status:'PASS'});}
function refuses(fn){assert.throws(fn,/HEALTHROSTER_DYNAMIC_SOURCE_REVIEW_CHANGED/);}
try{
 for(const input of [...approval.dependencies,...review.files.map(f=>({path:f.file,sha256:f.sha256}))]){
  const source=fs.readFileSync(path.join(integrator,input.path),'utf8').replaceAll('\r\n','\n');if(input.path==='supabase/repeatable/16102026_0955_nhsp_v6_invoice_native_authority/01_complete_invoice_authorities.sqlinc')assert.equal(hash(source),'e03acb39db8cc2441cde46125ad9b38e8594b3790c772b247cea76a052cb2044');else assert.equal(hash(source),input.sha256);save(input.path,source);sources.set(input.path,source);
 }
 save('docs/healthroster-build/v6-owner-delivery-20261016/F23_DYNAMIC_SOURCE_REVIEW_20261016/INVOICE_GROUPING_SUCCESSOR.json',fs.readFileSync(path.join(dir,'INVOICE_GROUPING_SUCCESSOR.json')));
 save(reviewPath,fs.readFileSync(path.join(dir,'CLASSIFICATION.json')));
 for(const [ref,file]of[[approval.review,'REVIEW.json'],[approval.priorReview,'PRIOR_SOURCE_REVIEW.json'],[approval.ast,'AST.json']])save(ref.path,fs.readFileSync(path.join(dir,file)));
 const classify=createReviewedHealthDynamicClassifier(fixture),baseline=new Map([['unrelated-known-baseline-key',2]]);
 for(const f of review.files){const s=sources.get(f.file);parsed.set(f.file,{violations:specialConstructViolations(s,f.file),parts:executableParts(s,f.file)});}
 const invoke=(file,source=sources.get(file),changes={})=>{const p=parsed.get(file);return classify(source,file,baseline,changes.violations??p.violations,changes.parts??p.parts);};
 run('all95 finite points across complete files, preserved unrelated baseline',()=>{
  let added=0;for(const f of approval.files){const out=invoke(f.path);assert.equal(out.get('unrelated-known-baseline-key'),2);added+=out.size-baseline.size;for(const p of f.points)assert.equal([...out.keys()].filter(k=>hash(k)===p.keySha256).length,1);}assert.equal(added,95);
 });
 const daily=approval.files.find(f=>f.path.includes('0858_')).path,invoice=approval.files.find(f=>f.path.includes('0955_')).path,candidate=approval.files.find(f=>f.path.includes('1003_')).path;
 const change=(file,before,after)=>{const s=sources.get(file);assert(s.includes(before),before);return s.replace(before,after);};
 run('unknown file and same signature cannot borrow approval',()=>{assert.equal(classify(sources.get(daily),'supabase/repeatable/unknown.sql',baseline,parsed.get(daily).violations,parsed.get(daily).parts),baseline);});
 run('caller-controlled identifier substituted for literal registry is refused',()=>refuses(()=>invoke(daily,change(daily,'declare signature text;','declare signature text:=current_setting(\'app.caller_function\');'))));
 run('unquoted caller target concatenated into real Candidate effect is refused',()=>refuses(()=>invoke(candidate,change(candidate,"'select public.candidate_targetless_expense_paper_pack_enqueue_v1($1,$2,$3,$4,$5)'","'select '||(v_payload->>'callee')||'($1,$2,$3,$4,$5)'"))));
 run('changed financial write target is refused',()=>refuses(()=>invoke(invoice,change(invoice,'insert into public.invoices(id,type,client_id,status,status_date_utc','insert into public.audit_events(id,type,client_id,status,status_date_utc'))));
 run('changed fixed effect callee is refused',()=>refuses(()=>invoke(candidate,change(candidate,'select public.candidate_targetless_expense_paper_pack_enqueue_v1($1,$2,$3,$4,$5)','select public.unreviewed_payment_provider_action($1,$2,$3,$4,$5)'))));
 run('removed current-source claim admission is refused',()=>refuses(()=>invoke(invoice,change(invoice,'perform private.invoice_source_original_claim_enter_v1(p_claims,v_now);','perform 1;'))));
 run('Candidate auth removal outside EXECUTE is refused',()=>refuses(()=>invoke(candidate,change(candidate,'v_context:=private._candidate_session_context_v1(p_session_id,v_environment,null,p_now_utc,true);',"v_context:=v_payload->'context';"))));
 run('extra effect appended to an otherwise exact file is refused',()=>refuses(()=>invoke(daily,sources.get(daily)+'\nselect public.unreviewed_effect();\n')));
 const p=parsed.get(daily),dynamic=p.violations.filter(x=>x.hazard==='DYNAMIC_SQL_UNCLASSIFIED');
 run('unknown dynamic key in exact file stays refused',()=>refuses(()=>invoke(daily,undefined,{violations:[...p.violations,{hazard:'DYNAMIC_SQL_UNCLASSIFIED',key:'["unknown()","DYNAMIC_SQL_UNCLASSIFIED","execute p_sql;"]'}]})));
 run('duplicate occurrence is refused',()=>refuses(()=>invoke(daily,undefined,{violations:[...p.violations,dynamic[0]]})));
 run('edited masked body with original source and key is refused',()=>refuses(()=>invoke(daily,undefined,{parts:p.parts.map(x=>({...x,text:x.text+'\nperform 1;'}))})));
 run('ambiguous enclosing body is refused',()=>refuses(()=>invoke(daily,undefined,{parts:[...p.parts,...p.parts]})));
 run('unclassified enclosing producer is refused',()=>refuses(()=>invoke(daily,undefined,{parts:p.parts.map(x=>({...x,unclassified:true}))})));
 run('all three1430 points remain outside proposal allowance',()=>{const f=review.files.find(f=>f.file.includes('13102026_1430_')).file;assert.equal(invoke(f),baseline);});
 run('capability0400 native census gap remains outside allowance',()=>{const f=review.files.find(f=>f.file.includes('14102026_0400_')).file;assert.equal(invoke(f),baseline);});
 run('three inherited baseline keys gain no new approval',()=>{for(const p of review.inheritedFrozenPoints){const out=invoke(p.file);assert(![...out.keys()].some(k=>hash(k)===p.keySha256));}});
 const dep=approval.dependencies.find(d=>d.path.includes('30082026_1232_'));
 run('callee dependency edit is refused in reused process, exact restore recovers',()=>{save(dep.path,sources.get(dep.path)+'\n-- changed callee\n');try{refuses(()=>invoke(candidate));}finally{save(dep.path,sources.get(dep.path));}assert(invoke(candidate).size>baseline.size);});
 run('approval tampering is refused',()=>{save(reviewPath,JSON.stringify({...approval,native:'PASS'}));try{refuses(()=>invoke(daily));}finally{save(reviewPath,fs.readFileSync(path.join(dir,'CLASSIFICATION.json')));}});
 run('evidence tampering is refused',()=>{save(approval.review.path,'{}');try{refuses(()=>invoke(daily));}finally{save(approval.review.path,fs.readFileSync(path.join(dir,'REVIEW.json')));}});
 run('offline AST tampering is refused',()=>{save(approval.ast.path,'{}');try{refuses(()=>invoke(daily));}finally{save(approval.ast.path,fs.readFileSync(path.join(dir,'AST.json')));}});
 run('rollback fixture arbitrary-text executor cannot move to runtime path',()=>{const f=approval.files.find(f=>f.path.includes('/verification/13102026_1206_')).path;assert.equal(classify(sources.get(f),'supabase/repeatable/unreviewed_refusal_executor.sql',baseline,parsed.get(f).violations,parsed.get(f).parts),baseline);});

 const {createReviewedJoinedNonDynamicClassifier,reviewFolder}=await import('../scripts/healthroster-joined-nondynamic-review.mjs');
 const ndDir=path.join(integrator,reviewFolder),nd=JSON.parse(fs.readFileSync(path.join(ndDir,'EXTENSION_MAP.json'))),ndReview=JSON.parse(fs.readFileSync(path.join(ndDir,'REVIEW.json')));
 for(const artifact of ['EXTENSION_MAP.json',...nd.artifacts.map(x=>x.path)])save(reviewFolder+'/'+artifact,fs.readFileSync(path.join(ndDir,artifact)));
 for(const input of [ndReview.schema,ndReview.schema.additiveMigration,{path:ndReview.uuidNativeSchemaEvidence.aliases.path}])save(input.path,fs.readFileSync(path.join(integrator,input.path)));
 const joined=createReviewedJoinedNonDynamicClassifier(fixture),ndInvoke=(file,source=sources.get(file),changes={})=>{const pp=parsed.get(file);return joined(source,file,baseline,changes.violations??pp.violations,changes.parts??pp.parts);};
 const ndRefuses=fn=>assert.throws(fn,/HEALTHROSTER_JOINED_NON_DYNAMIC_REVIEW_CHANGED/),nf=nd.filePins[0].path;
 run('all37 exact tuple/scalar/cardinality points and original baseline retained',()=>{let total=0;for(const pin of nd.filePins){const out=ndInvoke(pin.path);total+=out.size-baseline.size;assert.equal(out.get('unrelated-known-baseline-key'),2);}assert.equal(total,37);});
 run('unknown joined-path cannot borrow identical known tuple source',()=>assert.equal(joined(sources.get(nf),'supabase/repeatable/unreviewed_joined.sql',baseline,parsed.get(nf).violations,parsed.get(nf).parts),baseline));
 run('whole-row SELECT changed to mismatched relation refuses',()=>ndRefuses(()=>ndInvoke(nf,sources.get(nf).replace('from public.weekly_source_row_resolutions','from public.audit_events'))));
 run('UUID ambiguity guard cannot be removed',()=>{const original=sources.get(nf),changed=original.replace('case when count(*)=1 then min(ranked.shift_id::text)::uuid end shift_id','min(ranked.shift_id::text)::uuid shift_id');assert.notEqual(changed,original);ndRefuses(()=>ndInvoke(nf,changed));});
 run('scalar aggregate input cannot become UUID',()=>{const original=sources.get(nf),changed=original.replace('min(ranked.shift_id::text)','min(ranked.shift_id)');assert.notEqual(changed,original);ndRefuses(()=>ndInvoke(nf,changed));});
 run('removed actor authority outside reviewed tuple refuses',()=>{const original=sources.get(candidate),changed=original.replace('v_context:=private._candidate_session_context_v1(p_session_id,v_environment,null,p_now_utc,true);',"v_context:=v_payload->'context';");assert.notEqual(changed,original);ndRefuses(()=>ndInvoke(candidate,changed));});
 const ndKey=nd.points.find(x=>x.file===nf),ndp=parsed.get(nf),ndViolation=ndp.violations.find(x=>hash(x.key)===ndKey.keySha256);
 run('missing exact reviewed statement refuses',()=>ndRefuses(()=>ndInvoke(nf,undefined,{violations:ndp.violations.filter(x=>x!==ndViolation)})));
 run('duplicate reviewed tuple occurrence refuses',()=>ndRefuses(()=>ndInvoke(nf,undefined,{violations:[...ndp.violations,ndViolation]})));
 run('ambiguous complete containing body refuses',()=>ndRefuses(()=>ndInvoke(nf,undefined,{parts:[...ndp.parts,...ndp.parts]})));
 run('edited containing body refuses',()=>ndRefuses(()=>ndInvoke(nf,undefined,{parts:ndp.parts.map(x=>({...x,text:x.text+'\nperform 1;'}))})));
 run('arbitrary new UUID and JSON hazards receive no finite allowance',()=>{const unknown=[{hazard:'UUID_CARDINALITY_UNCLASSIFIED',key:'["unknown()","UUID_CARDINALITY_UNCLASSIFIED","select min(id::text)::uuid from public.audit_events;"]'},{hazard:'JSON_CONCAT_UNCLASSIFIED',key:'["unknown()","JSON_CONCAT_UNCLASSIFIED","select p->\'a\'||p;"]'}];const out=ndInvoke(nf,undefined,{violations:[...ndp.violations,...unknown]});for(const point of unknown)assert(!out.has(point.key));});
 for(const input of [ndReview.schema,ndReview.schema.additiveMigration,{path:ndReview.uuidNativeSchemaEvidence.aliases.path}])run('same-process native binding source drift refuses: '+input.path,()=>{const original=fs.readFileSync(path.join(fixture,input.path));save(input.path,Buffer.concat([original,Buffer.from('\n-- changed native binding\n')]));try{ndRefuses(()=>ndInvoke(nf));}finally{save(input.path,original);}assert(ndInvoke(nf).size>baseline.size);});
 run('finite source-review tamper refuses in reused process',()=>{const name=reviewFolder+'/REVIEW.json',original=fs.readFileSync(path.join(fixture,name));save(name,'{}');try{ndRefuses(()=>ndInvoke(nf));}finally{save(name,original);}});
 run('unreviewed source grouping successor refuses',()=>{const original=sources.get(nf),changed=original.replace("refs:=(snap->'rate_source_refs_json')||","refs:=(snap->'other_refs')||");assert.notEqual(changed,original);ndRefuses(()=>ndInvoke(nf,changed));refuses(()=>invoke(nf,changed));});
 run('dynamic successor metadata tamper refuses',()=>{const name='docs/healthroster-build/v6-owner-delivery-20261016/F23_DYNAMIC_SOURCE_REVIEW_20261016/INVOICE_GROUPING_SUCCESSOR.json',original=fs.readFileSync(path.join(fixture,name));save(name,'{}');try{refuses(()=>invoke(candidate));}finally{save(name,original);}});

 const result={version:'HEALTHROSTER_JOINED_FINITE_CLASSIFIER_INTEGRATION_TEST_RESULTS_V1',status:'PASS_SOURCE_ONLY',sourceCommit:commit,helperSha256:hash(fs.readFileSync(path.join(integrator,'scripts/healthroster-dynamic-source-review.mjs'))),classificationSha256:hash(fs.readFileSync(path.join(dir,'CLASSIFICATION.json'))),tests,passed:tests.length,databaseCalls:0,native:'NOT_RUN'};
 fs.writeFileSync(path.join(integrator,'docs/healthroster-build/v6-owner-delivery-20261016/F23_CLASSIFIER_INTEGRATION_TEST_RESULTS.json'),JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify({passed:tests.length,status:result.status,native:result.native}));
}finally{
 // Only this mkdtemp-created fixture under the verified proposal-local parent.
 const actual=fs.realpathSync(fixture),parent=fs.realpathSync(fixtureParent);assert.equal(path.dirname(actual),parent);assert(path.basename(actual).startsWith('dynamic-'));fs.rmSync(actual,{recursive:true,force:false});
 if(fs.readdirSync(fixtureParent).length===0)fs.rmdirSync(fixtureParent);
}
