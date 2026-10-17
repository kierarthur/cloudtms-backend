// ROOT REVIEW ONLY after actual reseal. Reads public source/Git, never run()/writer,
// out directories, auth/private files, native/API/browser/control or SQL.
import fs from 'node:fs';import path from 'node:path';import vm from 'node:vm';import crypto from 'node:crypto';import {execFileSync} from 'node:child_process';import {pathToFileURL,fileURLToPath} from 'node:url';
const P='docs/healthroster-build/proposals/',ROOT='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026',REPORT='D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026';
const sha=b=>crypto.createHash('sha256').update(b).digest('hex'),must=(v,c)=>{if(!v)throw Error('B2_PUBLIC_SOURCE_'+c);};
export const packetOwners=Object.freeze([P+'daily-s1-after-clock-adapter-20261017/',P+'daily-s1-actual-effect-recorder-20261017/',P+'daily-original-stages-root-effect-adapter-20261017/',P+'daily-original-stages-after-response-clock-forward-20261017/']);
export function exactPacketPredicate(source,dependencies){
 // Execute the literal complete existing packet() predicate, not a reimplementation.
 // The prefix has only imports, constants and function declarations. No run()/CLI tail.
 const marker='export async function run(env=process.env){';must(source.split(marker).length===2,'EXACT_ADAPTER_RUN_ANCHOR_REQUIRED');
 const prefix=source.slice(0,source.indexOf(marker));must(prefix.includes('function packet(relative,commit,head){'),'ORIGINAL_PACKET_REQUIRED');
 const text=prefix.replace(/^import [^\n]*\n/gm,'').replace(/^export /gm,'')+'\nglobalThis.originalPacket=packet;';
 const context=vm.createContext({...dependencies,Buffer});new vm.Script(text,{filename:'EXACT_EXISTING_PUBLIC_PACKET_PREFIX_ONLY'}).runInContext(context,{timeout:1000});return context.originalPacket;
}
export async function assertPublicSource(a){
 const keys=['rootHead','operatorCommit','helperCommit','s1AdapterCommit','s1RecorderCommit','stageAdapterCommit','stageRecorderCommit','composerCommit'];
 must(a&&Object.keys(a).sort().join('|')===[...keys,'aclComposed'].sort().join('|')&&keys.every(k=>/^[a-f0-9]{40}$/.test(a[k]??''))&&typeof a.aclComposed==='boolean','EXACT_REAL_CONFIG_COMMITS_REQUIRED');
 const physical=p=>fs.realpathSync.native(p).replaceAll('\\','/'),git=(r,...x)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-c','core.autocrlf=false','-c','core.longpaths=true','-C',r,...x],{windowsHide:true,maxBuffer:32e6,stdio:['ignore','pipe','ignore'],env:{...process.env,GIT_OPTIONAL_LOCKS:'0',GIT_NO_LAZY_FETCH:'1'}});
 must(physical(process.cwd()).startsWith('D:/Development/GitHub/'),'ACTUAL_D_CWD_REQUIRED');
 for(const r of[ROOT,REPORT]){
  must(physical(r)===r&&physical(git(r,'rev-parse','--show-toplevel').toString().trim())===r,'PHYSICAL_D_REPOSITORY_REQUIRED');
  must(physical(git(r,'rev-parse','--absolute-git-dir').toString().trim())==='D:/Development/GitHub/cloudtms-backend/.git/worktrees/'+path.basename(r)&&physical(path.resolve(r,git(r,'rev-parse','--git-common-dir').toString().trim()))==='D:/Development/GitHub/cloudtms-backend/.git','PHYSICAL_D_GIT_REQUIRED');
 }
 const rh=git(ROOT,'rev-parse','HEAD').toString().trim(),sh=git(REPORT,'rev-parse','HEAD').toString().trim();must(sh===a.operatorCommit&&rh===a.rootHead,'ACTUAL_METADATA_REVIEW_HEADS_REQUIRED');
 must(git(ROOT,'symbolic-ref','--short','HEAD').toString().trim()==='codex/healthroster-install2-14102026','ACTUAL_ROOT_FEATURE_BRANCH_REQUIRED');
 must(git(ROOT,'diff','--cached','--name-only','-z').length===0&&git(REPORT,'diff','--cached','--name-only','-z').length===0,'CLEAN_INDEX_REQUIRED');
 const small=(root,file,n=2e6)=>{must(file.startsWith(P)&&!file.split('/').includes('..')&&!file.includes('\\'),'PUBLIC_PROPOSAL_SOURCE_REQUIRED');const p=root+'/'+file,s=fs.lstatSync(p);must(s.isFile()&&!s.isSymbolicLink()&&s.size>0&&s.size<=n&&physical(p)===p,'BOUNDED_PHYSICAL_SOURCE_REQUIRED');return fs.readFileSync(p);};
 const image=(root,ref,file)=>{const b=small(root,file);git(root,'merge-base','--is-ancestor',ref,root===ROOT?rh:sh);must([ref,root===ROOT?rh:sh,''].every(r=>b.equals(git(root,'show',r+':'+file))),'COMPLETE_REAL_SOURCE_IMAGE_REQUIRED');return b;};
 const reviewed=JSON.parse(fs.readFileSync(path.join(path.dirname(fileURLToPath(import.meta.url)),'REVIEWED_POSTIMAGES.json')));
 must(reviewed.version==='B2_AND_COMPOSER_RAW_REVIEWED_SOURCE_V1'&&reviewed.files.length===19,'REVIEWED_FUNCTIONAL_SELECTION_REQUIRED');
 for(const r of reviewed.files){const root=r.owner==='report'?REPORT:ROOT,b=image(root,r.owner==='report'?sh:a.helperCommit,r.file),acl=a.aclComposed&&r.owner==='report'&&r.file===reviewed.optionalAcl.file;
  must(b.length===(acl?reviewed.optionalAcl.combinedBytes:r.bytes)&&sha(b)===(acl?reviewed.optionalAcl.combinedSha256:r.sha256),'EXACT_REVIEWED_FUNCTIONAL_SOURCE_REQUIRED');}
 const run1=packetOwners[0]+'run.mjs',run4=packetOwners[2]+'run.mjs';
 const deps={fs,path,crypto,execFileSync,process,fileURLToPath,pathToFileURL};
 const p1=exactPacketPredicate(image(ROOT,a.s1AdapterCommit,run1).toString(),deps),p4=exactPacketPredicate(image(ROOT,a.stageAdapterCommit,run4).toString(),deps);
 p1(packetOwners[0],a.s1AdapterCommit,rh);p1(packetOwners[1],a.s1RecorderCommit,rh);p4(packetOwners[2],a.stageAdapterCommit,rh);p4(packetOwners[3],a.stageRecorderCommit,rh);
 const op=P+'daily-health-genuine-staged-operator-20261017/',bk=P+'daily-genuine-stage-root-bookkeeper-20261017/';
 const original=await import(pathToFileURL(REPORT+'/'+op+'source.mjs').href),om=JSON.parse(small(REPORT,op+'MANIFEST.json')),bm=JSON.parse(small(REPORT,bk+'MANIFEST.json'));
 original.assertReviewedImage(REPORT,sh,[op+'MANIFEST.json',...om.files.map(r=>op+r.file),bk+'MANIFEST.json',...bm.files.map(r=>bk+r.file)]);
 const coordinator=await import(pathToFileURL(REPORT+'/'+P+'daily-health-s0-stage-coordinator-20261017/source.mjs').href),one=await import(pathToFileURL(REPORT+'/'+P+'daily-health-genuine-single-stage-coordinator-20261017/source.mjs').href);
 must(coordinator.source(sh)===sh&&one.source(sh)===sh,'ACTUAL_COMPLETE_REPORT_SOURCE_CHAIN_REQUIRED');
 const pins=JSON.parse(small(REPORT,bk+'PARENTS.json'));must(pins.fixedRuntime.length===12,'TWELVE_RUNTIME_ROWS_REQUIRED');
 for(const r of pins.fixedRuntime){const b=image(REPORT,sh,r.file);must(b.length===r.bytes&&sha(b)===r.sha256&&b.equals(git(REPORT,'show',r.commit+':'+r.file)),'ALL_RUNTIME_HISTORY_ROWS_REQUIRED');}
 for(const r of pins.rows){must([ROOT,REPORT].includes(r.root),'ORIGINAL_HISTORY_ROOT_REQUIRED');const b=image(REPORT,sh,bk+r.captured);must(b.length===r.bytes&&sha(b)===r.sha256&&b.equals(git(r.root,'show',r.commit+':'+r.file)),'ORIGINAL_CAPTURED_HISTORY_ROW_REQUIRED');}
 const ef=P+'daily-b2-current-effect-20261017/',selection=JSON.parse(image(ROOT,a.helperCommit,ef+'SOURCE_SELECTION.json'));
 must(selection.version==='DAILY_B2_EXACT_EFFECT_SOURCE_V1'&&selection.files.length===7,'EXACT_SEVEN_EFFECT_IMAGES_REQUIRED');
 for(const r of selection.files){const b=image(ROOT,a.helperCommit,r.file);must(b.length===r.bytes&&sha(b)===r.sha256,'CURRENT_EFFECT_SOURCE_IMAGE_REQUIRED');}
 image(ROOT,a.helperCommit,P+'daily-genuine-root-stage-bookkeeper-20261017.mjs');
 const cp=P+'daily-five-stage-producer-hash-forward-20261017/',cm=JSON.parse(image(ROOT,a.composerCommit,cp+'MANIFEST.json'));
 for(const r of cm.files){const b=image(ROOT,a.composerCommit,cp+r.file);must(b.length===r.bytes&&sha(b)===r.sha256,'COMPLETE_COMPOSER_PACKET_REQUIRED');}
 must(git(ROOT,'rev-parse','HEAD').toString().trim()===rh&&git(REPORT,'rev-parse','HEAD').toString().trim()===sh,'HEAD_MOVED_DURING_CHECK');
 return{status:'ACTUAL_PUBLIC_SOURCE_ONLY_PASS',rootHead:rh,reportHead:sh,completeAdapterRecorderPackets:4,effectImages:7,runtimeRows:12,historicalCaptureRows:pins.rows.length,native:'NOT_RUN',privateStateRead:false};
}
// No CLI invocation, environment loader, writer or automatic admission call.
