// Whole actual adapter/bookkeeper run bodies with in-memory FS/Git/import doubles only.
// Synthetic committed images are NOT evidence that this unmounted proposal passed real admission.
import fs from 'node:fs';import path from 'node:path';import {createHash} from 'node:crypto';import {fileURLToPath,pathToFileURL} from 'node:url';
import {here,P,OP,BK,c,n,b2,st,core,effect,s1Clock,other,otherValidator,moduleVM,source,NOW} from './test-support.mjs';
export function rootMachine(f){
 const ROOT=c.ROOT,REPORT='D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026',out=ROOT+'/.codex-tmp/daily-genuine-stages-20261017-'+'9'.repeat(32),files=new Map(),events=[];
 const norm=p=>p.replaceAll('\\','/');let head='a'.repeat(40),staged=[],serial=0;const put=(p,b)=>files.set(norm(p),Buffer.from(b)),save=(name,value)=>put(out+'/'+name,Buffer.isBuffer(value)?value:b2.jsonBytes(value));
 for(const [file,b] of f.publicFiles)put(ROOT+'/'+file,b);
 if(f.plan.previousEvidence)put(ROOT+'/'+f.plan.previousEvidence,Buffer.from(JSON.stringify({...f.prior,originalCoordinatorSummary:{ownedDirectory:out}},null,2)+'\n'));
 save(f.stage+'_BEFORE_INPUT.json',f.beforeInputBytes);save(f.stage+'_BEFORE_NATIVE.json',f.beforeBytes);save(f.stage+'_AFTER_INPUT.json',f.afterBuilt);save(f.stage+'_AFTER_NATIVE.json',f.afterBytes);save(f.stage+'_RESPONSE.json',f.args.responseBytes);save(f.stage+'_ENTRY_BINDING.json',{entry:f.entry,effectFile:f.effectFile});save(f.stage+'_CURRENT_INPUT.json',f.pending.built);save(f.stage+'_CURRENT_BINDING.json',f.pending.binding);save(f.stage+'_CURRENT_NATIVE.json',Buffer.from(f.record.nativeResultText));save(f.stage+'_CURRENT_TIMING.json',f.record.timing);save(f.stage+'_CURRENT_CONSUMPTION.json',f.consumptionBytes);
 for(const phase of ['Before','After'])if(f.args['aux'+phase+'Built']){save(f.stage+'_AUX_'+phase.toUpperCase()+'_INPUT.json',f.args['aux'+phase+'Built']);save(f.stage+'_AUX_'+phase.toUpperCase()+'_NATIVE.json',f.args['aux'+phase+'Bytes']);}
 const manifest=entries=>b2.jsonBytes({files:entries.map(([file,b])=>({file,bytes:b.length,sha256:c.sha(b)}))});
 const addPacket=(root,relative,selected)=>{const entries=selected.map(name=>{const b=fs.readFileSync(here+'/payload/'+(root===ROOT?'root':'report')+'/'+relative+name);put(root+'/'+relative+name,b);return [name,b];});put(root+'/'+relative+'MANIFEST.json',manifest(entries));};
 addPacket(ROOT,P+'daily-s1-after-clock-adapter-20261017/',['run.mjs','clock-forward.mjs']);addPacket(ROOT,P+'daily-s1-actual-effect-recorder-20261017/',['recorder.mjs']);addPacket(ROOT,P+'daily-original-stages-root-effect-adapter-20261017/',['run.mjs','validate.mjs']);addPacket(ROOT,P+'daily-original-stages-after-response-clock-forward-20261017/',['recorder.mjs']);
 addPacket(REPORT,OP,['worker.mjs','stages.mjs','current-read.mjs','current-ipc.mjs','native-read.mjs','contract.mjs']);addPacket(REPORT,BK,['core.mjs','bookkeeper.mjs']);
 const parents=JSON.parse(fs.readFileSync(here+'/parents/report/'+BK+'PARENTS.json'));
 for(const r of parents.fixedRuntime){const b=fs.readFileSync(here+'/payload/report/'+r.file);put(REPORT+'/'+r.file,b);Object.assign(r,{commit:source,bytes:b.length,sha256:c.sha(b)});}
 put(REPORT+'/'+BK+'PARENTS.json',b2.jsonBytes(parents));
 const effectRel=P+'daily-b2-current-effect-20261017/';for(const name of ['current-effect.mjs','SOURCE_SELECTION.json'])put(ROOT+'/'+effectRel+name,fs.readFileSync(here+'/payload/root/'+effectRel+name));
 const helper=P+'daily-genuine-root-stage-bookkeeper-20261017.mjs';put(ROOT+'/'+helper,fs.readFileSync(here+'/payload/root/'+helper));
 const get=p=>{p=norm(p);if(!files.has(p))throw Error('SYNTHETIC_MISSING_'+p);return Buffer.from(files.get(p));};
 const memoryFS={realpathSync:{native:norm},existsSync:p=>files.has(norm(p)),lstatSync:p=>({isFile:()=>files.has(norm(p)),isSymbolicLink:()=>false,size:files.get(norm(p))?.length??0}),readFileSync:(p,e)=>e?get(p).toString():get(p),writeFileSync:(p,b,o)=>{if(o?.flag==='wx'&&files.has(norm(p)))throw Error('SYNTHETIC_EEXIST');put(p,b);events.push(['write',p]);}};
 const git=(_binary,args)=>{
  const i=args.indexOf('-C'),root=args[i+1],a=args.slice(i+2);events.push(['git',root,...a]);
  if(a[0]==='rev-parse'){const v=a[1];return Buffer.from(v==='HEAD'?(root===ROOT?head:source):v==='--show-toplevel'?root:v==='--absolute-git-dir'?'D:/Development/GitHub/cloudtms-backend/.git/worktrees/'+path.basename(root):'D:/Development/GitHub/cloudtms-backend/.git');}
  if(a[0]==='symbolic-ref')return Buffer.from('codex/healthroster-install2-14102026');
  if(a[0]==='merge-base')return Buffer.alloc(0);
  if(a[0]==='show'){const file=a[1].slice(a[1].indexOf(':')+1);return get(root+'/'+file);}
  if(a[0]==='diff')return Buffer.from(a.includes('--name-only')?staged.join('\0')+(staged.length?'\0':''):'');
  if(a[0]==='add'){staged=a.slice(a.indexOf('--')+1);return Buffer.alloc(0);}
  if(a[0]==='commit'){head=(++serial).toString(16).padStart(40,'c');staged=[];return Buffer.alloc(0);}
  if(a[0]==='push')return Buffer.alloc(0);
  if(a[0]==='ls-remote')return Buffer.from(head+'\trefs/heads/codex/healthroster-install2-14102026\n');
  throw Error('SYNTHETIC_UNSUPPORTED_GIT_'+a.join(' '));
 };
 const imports=async url=>{
  if(url.endsWith('/'+OP+'git-image-batch.mjs'))return {reviewedImage:(_r,_c,list)=>{events.push(['reviewedImages',...list]);},blobImages:(r,requests)=>{if(!requests.length)throw Error('SYNTHETIC_EMPTY_BATCH');for(const q of requests)get(r+'/'+q.file);return {get:(_ref,file)=>get(r+'/'+file)};}};
  if(url.endsWith('/'+OP+'source.mjs'))return {assertReviewedImage:(_root,_commit,list)=>{if(!Array.isArray(list)||list.length<2)throw Error('SYNTHETIC_IMAGES_REQUIRED');events.push(['reviewedImages',...list]);}};
  if(url.endsWith('/'+BK+'core.mjs'))return core;
  if(url.endsWith('/'+OP+'contract.mjs'))return c;if(url.endsWith('/'+OP+'native-read.mjs'))return n;if(url.endsWith('/'+OP+'stages.mjs'))return st;if(url.endsWith('/'+OP+'current-read.mjs'))return b2;
  throw Error('SYNTHETIC_UNSUPPORTED_IMPORT_'+url);
 };
 const common={fs:memoryFS,path,fileURLToPath,pathToFileURL,process:{argv:[],cwd:()=>ROOT,env:{}},execFileSync:git,cp:{execFileSync:git},crypto:{createHash},__import:imports};
 return {out,files,events,get,put,save,memoryFS,git,imports,common,ROOT,REPORT,helper,get head(){return head;}};
}
export async function runAdapter(f,m){
 const one=f.stage==='S1',relative=P+(one?'daily-s1-after-clock-adapter-20261017/':'daily-original-stages-root-effect-adapter-20261017/');
 const bindings={...m.common,...one?s1Clock:{...other,...otherValidator}},mod=moduleVM('root',relative+'run.mjs',bindings,['run']);
 return mod.run({HR_ROOT_STAGE:f.stage,HR_OPERATOR_COMMIT:source,HR_ROOT_OUT:m.out,HR_ROOT_PLAN:f.planFile,HR_S1_ADAPTER_COMMIT:'a'.repeat(40),HR_S1_RECORDER_COMMIT:'a'.repeat(40),HR_STAGE_ADAPTER_COMMIT:'a'.repeat(40),HR_STAGE_RECORDER_COMMIT:'a'.repeat(40)});
}
export async function runBookkeeper(f,m,phase){
 const mod=moduleVM('root',m.helper,{...m.common,...core},['run']);
 return mod.run({HR_ROOT_STAGE:f.stage,HR_ROOT_PHASE:phase,HR_OPERATOR_COMMIT:source,HR_ROOT_OUT:m.out,HR_ROOT_PLAN:f.planFile,HR_ROOT_HELPER_COMMIT:'a'.repeat(40),HR_ROOT_QUALIFY_RESULT:'CLOSED_qualify_'+f.stage+'_'+'e'.repeat(16)+'.json'});
}
