// Peer-only controlled values. Never actual auth, source admission, materialization or Git.
import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';
import path from 'node:path';import vm from 'node:vm';import {pathToFileURL} from 'node:url';
import {createHash} from 'node:crypto';
const packet='D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/.codex-tmp/daily-auth-r11-b2-source-recipe-20261017';
const recipe=await import(pathToFileURL(packet+'/recipe.mjs'));
const auth=await import(pathToFileURL(packet+'/auth-supersession.mjs'));
const {assertActualMount,readPublicBinding}=await import(pathToFileURL(packet+'/materialize.mjs'));
const read=f=>fs.readFileSync(path.join(packet,f),'utf8'),clone=x=>structuredClone(x);
const base=JSON.parse(read('B2_BASELINE.json')),parentRows=JSON.parse(read('PARENTS.json')).rows;
const fake={recipeCommit:'1'.repeat(40),receiptCommit:'2'.repeat(40),operatorCommit:'3'.repeat(40),holderCommit:'4'.repeat(40),coordinatorCommit:'5'.repeat(40),continuationCommit:'6'.repeat(40)};
function binding(){
 const r=JSON.parse(read('reference/R10_PUBLIC_RECEIPT.json'));
 r.version='TEST_AUTH_REFRESH_R11_ACTUAL_SUCCESS_V1';
 r.privateStatePath=recipe.ROOT+'/.codex-tmp/test-auth-refresh-20261017-'+'e'.repeat(32)+'/storage-state.json';
 const publicReceipt=JSON.stringify(r,null,2)+'\n';
 return {observation:{file:recipe.receiptFile,commit:fake.receiptCommit,sha256:recipe.hash(publicReceipt)},publicReceipt};
}
const s=()=>({...fake,receiptSha256:binding().observation.sha256});
test('controlled R11 receipt preserves actual validator: every auth/privacy refusal and current-path reuse',()=>{
 const b=binding();assert.equal(recipe.validateBinding(b).observation.file,recipe.receiptFile);
 const mutations=[r=>r.version='TEST_AUTH_REFRESH_R10_ACTUAL_SUCCESS_V1',r=>r.actualClosedResult.status='HOLD',
  r=>r.actualClosedResult.code='HOLD',r=>r.actualClosedResult.browserClosed=false,r=>r.actualClosedResult.loginRequests=2,
  r=>r.actualClosedResult.refreshRequests=-1,r=>r.actualClosedResult.refreshRequests=2,r=>r.actualClosedResult.refreshRequests='0',
  r=>r.actualClosedResult.forbiddenWriteRequests=1,r=>r.actualClosedResult.refusedRequests=1,
  r=>r.actualClosedResult.suppressedStartupRequests=-1,r=>r.actualClosedResult.suppressedStartupRequests=2,
  r=>r.actualClosedResult.diagnostic.admission.meStatus=401,r=>r.actualClosedResult.diagnostic.admission.actorUuid=false,
  r=>r.actualClosedResult.diagnostic.admission.admin=false,r=>r.actualClosedResult.diagnostic.admission.workspaceStatus=401,
  r=>r.actualClosedResult.diagnostic.admission.contract=false,r=>r.actualClosedResult.diagnostic.admission.queries=false,
  r=>r.privateContentsReadOrHashedByParent=true,r=>r.sharedFrontendStateUntouched=false,r=>r.browserAndChildClosed=false,
  r=>r.processExitCode=1,r=>r.nativeIntervalActive=true,r=>r.startupPingForwarded=true,r=>r.startupResponseFabricated=true,
  r=>r.businessFixturesCreated=true,r=>r.qualification='CURRENTNESS_PROVED',
  r=>r.privateStatePath=JSON.parse(read('reference/R10_PUBLIC_RECEIPT.json')).privateStatePath,
  r=>r.privateStatePath=r.privateStatePath.replace('D:/Development','C:/retained')];
 for(const mutate of mutations){const r=JSON.parse(b.publicReceipt);mutate(r);const text=JSON.stringify(r)+'\n';
  assert.throws(()=>recipe.validateBinding({observation:{...b.observation,sha256:recipe.hash(text)},publicReceipt:text}));}
 for(const patch of [{publicReceipt:b.publicReceipt+' '},{extra:true},{observation:{...b.observation,extra:true}},
  {observation:{...b.observation,commit:'HEAD'}},{observation:{...b.observation,file:'other.json'}}])
  assert.throws(()=>recipe.validateBinding({...b,...patch}));
});
test('all nine complete auth images derive from frozen current parents with exact pin/manifest counts',()=>{
 const phases=auth.derivedAuth(s(),binding()),all=phases.flatMap(p=>p.targets);
 assert.equal(all.length,9);assert.equal(new Set(all.map(t=>t.target)).size,9);
 assert.equal(phases[0].proof.manifest.retainedRows,132);
 assert.equal(phases[1].proof.pins.retainedRows,6);
 assert.equal(phases[2].proof.pins.retainedRows,148);
 assert.equal(phases[3].proof.pins.retainedRows,14);
 const contract=all.find(t=>t.file==='contract.mjs');
 const old=read('reference/operator/contract.mjs'),oldPath=JSON.parse(read('reference/R10_PUBLIC_RECEIPT.json')).privateStatePath.slice(recipe.ROOT.length),
  newPath=JSON.parse(binding().publicReceipt).privateStatePath.slice(recipe.ROOT.length);
 assert.equal(contract.text.split(newPath).join(oldPath),old);
 const pin=JSON.parse(all.find(t=>t.target===recipe.folders.coordinator+'SOURCE_PINS.json').text);
 assert.equal(pin.length,153);assert(pin.some(r=>r.file.endsWith('/current-read.mjs')));assert(pin.some(r=>r.file.endsWith('/current-ipc.mjs')));
 const bk=parentRows.find(r=>r.file.endsWith('daily-genuine-stage-root-bookkeeper-20261017/PARENTS.json'));
 const original=JSON.parse(read(bk.captured));assert.equal(original.fixedRuntime.length,12);assert.equal(original.rows.length,15);
});
function readerFixture(){
 const phases=auth.derivedAuth(s(),binding()),overlay=phases.flatMap(p=>p.targets),heads={root:'7'.repeat(40),report:'6'.repeat(40)};
 const historical=new Map();for(const r of base.rows){const p=parentRows.find(x=>x.file===r.file&&x.root===(r.owner==='root'?recipe.ROOT:recipe.report));
  assert(p);historical.set(r.owner+'|'+r.file,Buffer.from(read(p.captured)));}
 const current=new Map(historical);for(const t of overlay)current.set('report|'+t.target,Buffer.from(t.text));
 const images={saved:new Map(current),index:new Map(current),head:new Map(current)};
 const reader={baseHead:o=>base[o+'Commit'],head:o=>heads[o],
  saved:(o,f)=>images.saved.get(o+'|'+f),index:(o,f)=>images.index.get(o+'|'+f),
  blob:(o,ref,f)=>(ref===base[o+'Commit']?historical:images.head).get(o+'|'+f)};
 return {reader,heads,overlay,historical,images};
}
test('actual baseline predicate refuses every one of nine image substitutions and all unchanged row substitutions',()=>{
 const clean=readerFixture();auth.assertBaselineCurrent(base.rows,clean.overlay,clean.reader,clean.heads);
 let rejected=0;
 for(const r of base.rows)for(const kind of ['saved','index','head']){
  const f=readerFixture();f.images[kind].set(r.owner+'|'+r.file,Buffer.from('SUBSTITUTED_PUBLIC_SOURCE'));
  assert.throws(()=>auth.assertBaselineCurrent(base.rows,f.overlay,f.reader,f.heads));rejected++;
 }
 assert.equal(rejected,base.rows.length*3);assert.equal(base.rows.length,62);
 for(const r of base.rows){const f=readerFixture();f.historical.set(r.owner+'|'+r.file,Buffer.from('REBASED_OLD_PARENT'));
  assert.throws(()=>auth.assertBaselineCurrent(base.rows,f.overlay,f.reader,f.heads));}
});
test('duplicate/missing/unknown auth overlay or moved current heads refuse',()=>{
 for(const alter of [a=>a.slice(1),a=>[...a,a[0]],a=>a.map((t,i)=>i? t:{...t,target:'docs/healthroster-build/unknown.json'})]){
  const f=readerFixture();assert.throws(()=>auth.assertBaselineCurrent(base.rows,alter(f.overlay),f.reader,f.heads));
 }
 const f=readerFixture();f.reader.head=()=> 'f'.repeat(40);assert.throws(()=>auth.assertBaselineCurrent(base.rows,f.overlay,f.reader,f.heads));
 for(const patch of [{extra:true},{receiptSha256:'bad'},{receiptCommit:'HEAD'},{continuationCommit:'HEAD'}])assert.throws(()=>auth.exactSupersession({...s(),...patch}));
});
test('actual committed reader is exercised only through explicit memory stand-ins, missing index/current/ref always refuses',()=>{
 const b=binding(),bytes=Buffer.from(b.publicReceipt),head='7'.repeat(40);
 function transport(defect){let headReads=0;return {read:()=>defect==='saved'?Buffer.from('bad'):bytes,run:(...args)=>{
  if(args[0]==='rev-parse'){headReads++;return Buffer.from(defect==='moved'&&headReads>1?'8'.repeat(40):head);}
  if(args[0]==='merge-base'){if(defect==='ancestry')throw Error('CONTROLLED_ANCESTRY_REFUSAL');return Buffer.alloc(0);}
  if(args[0]==='show'){if(defect==='index'&&args[1].startsWith(':'))return Buffer.from('bad');
   if(defect==='head'&&args[1].startsWith(head+':'))return Buffer.from('bad');
   if(defect==='commit'&&args[1].startsWith(fake.receiptCommit+':'))return Buffer.from('bad');return bytes;}
  throw Error('UNEXPECTED_CONTROLLED_GIT');}};}
 assert.equal(readPublicBinding({receiptCommit:fake.receiptCommit,receiptSha256:b.observation.sha256},transport()).publicReceipt,b.publicReceipt);
 for(const d of ['saved','index','head','commit','ancestry','moved'])assert.throws(()=>readPublicBinding({receiptCommit:fake.receiptCommit,receiptSha256:b.observation.sha256},transport(d)));
 const target={target:recipe.receiptFile,text:b.publicReceipt,afterSha256:b.observation.sha256};
 assert.equal(assertActualMount(fake.receiptCommit,[target],transport()),head);
 for(const d of ['saved','index','head','commit','ancestry','moved'])assert.throws(()=>assertActualMount(fake.receiptCommit,[target],transport(d)));
});
test('actual new loadConfig couples real-source-shaped config/supersession/review and all five entry images under controlled readers',()=>{
 const file='payload/docs/healthroster-build/proposals/daily-b2-r11-s0-publish-stageplans-20261017.mjs';
 const text=read(file),code=text.slice(0,text.indexOf('\nif(process.argv[1]')).replace(/^import[^\n]*\n/gm,'').replace(/^export /gm,'')+'\n;({loadConfig});';
 const B='docs/healthroster-build/v6-owner-delivery-20261016/',P='docs/healthroster-build/proposals/';
 const configFile=B+'DAILY_GENUINE_B2_R11_S0_ROOT_CONFIG_20261017.json',reviewFile=B+'DAILY_GENUINE_B2_R11_S0_SOURCE_ADMISSION_20261017.json';
 const originalConfig={version:'DAILY_GENUINE_B2_R11_S0_CONFIG_V1',operatorCommit:fake.continuationCommit,metadataCommits:base.config.metadataCommits,
  entrypointCommit:'9'.repeat(40),recipeCommit:base.config.recipeCommit,authReviewCommit:'a'.repeat(40),sourceReviewCommit:'b'.repeat(40),authSupersession:s()};
 for(const defect of [null,'source','configSHA','operatorEnv','receiptMismatch','metadataMismatch','entryMismatch','authReviewMismatch','metadataProof','publicProof','entrySaved','entryIndex','reviewIndex','authMissing']){
  const config=clone(originalConfig),review={version:'DAILY_GENUINE_B2_R11_SOURCE_ADMISSION_V1',...clone(config),
   metadataProof:{status:'ACTUAL_B2_AUTH_SUPERSESSION_IMAGES_PASS',metadataRows:Array.from({length:14},()=>({}))},
   publicProof:{status:'ACTUAL_PUBLIC_SOURCE_ONLY_PASS',reportHead:fake.continuationCommit}};
  review.version='DAILY_GENUINE_B2_R11_SOURCE_ADMISSION_V1';
  if(defect==='source')config.operatorCommit='f'.repeat(40);
  if(defect==='receiptMismatch')review.authSupersession.receiptSha256='f'.repeat(64);
  if(defect==='metadataMismatch')review.metadataCommits.rootMetadata='f'.repeat(40);
  if(defect==='entryMismatch')review.entrypointCommit='f'.repeat(40);
  if(defect==='authReviewMismatch')review.authReviewCommit='f'.repeat(40);
  if(defect==='metadataProof')review.metadataProof.metadataRows.pop();
  if(defect==='publicProof')review.publicProof.reportHead='f'.repeat(40);
  const data=new Map([[configFile,Buffer.from(JSON.stringify(config))],[reviewFile,Buffer.from(JSON.stringify(review))],
   [B+'DAILY_AUTH_R11_SOURCE_RECIPE_ROOT_REVIEW_20261017.json',Buffer.from('{}')]]);
  for(const name of ['daily-genuine-root-s0-b2-r11-bookkeeper-20261017.mjs','daily-genuine-s0-b2-r11-tools-orchestration-20261017.js.txt',
   'daily-b2-r11-s0-publish-stageplans-20261017.mjs','daily-five-stage-b2-r11-source-plan-20261017/compose.mjs','daily-five-stage-b2-r11-source-plan-20261017/MANIFEST.json'])
   data.set(P+name,Buffer.from(read('payload/'+P+name)));
  const firstEntry=P+'daily-genuine-root-s0-b2-r11-bookkeeper-20261017.mjs';
  const local=f=>f.slice(recipe.ROOT.length+1);
  const fakeFs={realpathSync:{native:f=>f},lstatSync:f=>{const key=local(f);if(defect==='authMissing'&&key.endsWith('ROOT_REVIEW_20261017.json'))throw Error('CONTROLLED_MISSING');return{isFile:()=>true,isSymbolicLink:()=>false,size:data.get(key)?.length??1};},
   readFileSync:f=>defect==='entrySaved'&&local(f)===firstEntry?Buffer.from('BAD'):data.get(local(f))};
  const exec=(cmd,args)=>{const owner=args[args.indexOf('-C')+1],tail=args.slice(args.indexOf('-C')+2),h=owner===recipe.ROOT?'7'.repeat(40):fake.continuationCommit;
   if(tail[0]==='merge-base')return Buffer.alloc(0);
   if(tail[0]==='rev-parse')return Buffer.from(tail[1]==='--show-toplevel'?owner:tail[1]==='--absolute-git-dir'?'D:/Development/GitHub/cloudtms-backend/.git/worktrees/'+path.basename(owner):tail[1]==='--git-common-dir'?'D:/Development/GitHub/cloudtms-backend/.git':h);
   if(tail[0]==='show'){const i=tail[1].indexOf(':'),ref=tail[1].slice(0,i),f=tail[1].slice(i+1);
    if(ref===''&&(defect==='entryIndex'&&f===firstEntry||defect==='reviewIndex'&&f===reviewFile))return Buffer.from('BAD');return data.get(f);}
   throw Error('UNEXPECTED_CONTROLLED_GIT');};
  const module=new vm.Script(code).runInNewContext({fs:fakeFs,path,cp:{execFileSync:exec},createHash,Buffer,
   process:{env:{}},fileURLToPath:()=>'',pathToFileURL:()=>''});
  const env=defect==='configSHA'?{HR_B2_CONFIG_SHA:'f'.repeat(64)}:defect==='operatorEnv'?{HR_OPERATOR_COMMIT:'f'.repeat(40)}:{};
  if(defect)assert.throws(()=>module.loadConfig(env),defect);
  else assert.equal(module.loadConfig(env).config.operatorCommit,fake.continuationCommit);
 }
});
