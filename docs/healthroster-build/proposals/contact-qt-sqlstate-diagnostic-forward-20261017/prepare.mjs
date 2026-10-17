// Isolated source-only complete postimage. No child/native/DB transport is invoked.
import fs from'node:fs';import path from'node:path';import{fileURLToPath}from'node:url';import{execFileSync}from'node:child_process';import{createHash}from'node:crypto';
const here=path.dirname(fileURLToPath(import.meta.url)),root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026',commit='2b0fcee4eeecb9ce3adb61ef3c25d02c0e5a948c',prefix='docs/healthroster-build/proposals/contact-qt-positive-native-proposal-20261017';
const sha=b=>createHash('sha256').update(b).digest('hex'),check=(v,e)=>{if(!v)throw Error(e);};
check(fs.realpathSync.native(here).replaceAll('\\','/').startsWith('D:/Development/GitHub/.codex-worktrees/healthroster-checklist-native-14102026/'),'OWN_PHYSICAL_D_ONLY');check(!fs.existsSync(here+'/MANIFEST.json'),'FROZEN_HISTORY');
const git=(...a)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,...a],{maxBuffer:32e6});
const originalManifest=git('show',commit+':'+prefix+'/MANIFEST.json'),manifest=JSON.parse(originalManifest),parents=new Map();check(manifest.files.length===44,'MOUNTED_45_PARENT_REQUIRED');
for(const r of [...manifest.files,{file:'MANIFEST.json',sha256:sha(originalManifest),bytes:originalManifest.length}]){const b=git('show',commit+':'+prefix+'/'+r.file);check(b.length===r.bytes&&sha(b)===r.sha256&&b.equals(fs.readFileSync(root+'/'+prefix+'/'+r.file)),'ROOT_SAVED_COMMITTED_PARENT_REQUIRED:'+r.file);parents.set(r.file,b);if(r.file==='MANIFEST.json')continue;fs.mkdirSync(path.dirname(here+'/current/'+r.file),{recursive:true});fs.writeFileSync(here+'/current/'+r.file,b);}
const changes=[];const edit=(file,before,after)=>{const newline=parents.get(file).includes(Buffer.from('\r\n'))?'\r\n':'\n';before=before.replace(/\r?\n/g,()=>newline);after=after.replace(/\r?\n/g,()=>newline);const old=fs.readFileSync(here+'/current/'+file,'utf8');check(old.split(before).length===2,'ONE_LITERAL_HUNK_REQUIRED:'+file);fs.writeFileSync(here+'/current/'+file,old.replace(before,()=>after));changes.push({file,before,after});};
edit('fixed-metadata-reader.mjs',"import {NATIVE_BOUNDARY_SQL} from './native-boundary.mjs';","import {NATIVE_BOUNDARY_SQL} from './native-boundary.mjs';\nimport {SqlstateStderrCapture,diagnosticContext,diagnosticError,preserveMetadataDiagnostic} from './metadata-diagnostic.mjs';");
edit('fixed-metadata-reader.mjs','export class BoundedMemoryCapture {','export class BoundedMemoryCapture {\n #stderrState=new SqlstateStderrCapture();');
edit('fixed-metadata-reader.mjs',`  if(this.failed)return;
  this.stderrBytes+=Buffer.byteLength(chunk);
  this.failed=this.stderrBytes>LIMITS.stderrBytes?'LOCAL_METADATA_STDERR_BOUND':'LOCAL_METADATA_DIAGNOSTIC_REFUSED';
  this.chunks=[];
 }`,`  this.stderrBytes=Math.min(LIMITS.stderrBytes+1,this.stderrBytes+Buffer.byteLength(chunk));
  this.#stderrState.write(chunk);
  if(!this.failed||this.stderrBytes>LIMITS.stderrBytes)this.failed=this.stderrBytes>LIMITS.stderrBytes?'LOCAL_METADATA_STDERR_BOUND':'LOCAL_METADATA_DIAGNOSTIC_REFUSED';
  this.chunks=[];
 }
 diagnosticSqlstate(closed){return this.#stderrState.finish({closed});}`);
edit('fixed-metadata-reader.mjs',`async function runFixedChild({input,env,turn,phase}){
 try{await turn.assertHeld('before-fixed-read-only-metadata');}catch{throw Error('LOCAL_METADATA_TURN_LOST');}`,`async function runFixedChild({input,env,turn,phase}){
 const diagnostic=diagnosticContext(phase);
 try{await turn.assertHeld('before-fixed-read-only-metadata');}catch{throw diagnosticError('LOCAL_METADATA_TURN_LOST',diagnostic);}`);
edit('fixed-metadata-reader.mjs',`  {windowsHide:true,env,stdio:['pipe','pipe','pipe']});}catch{throw Error('LOCAL_METADATA_START_FAILED');}`,`  {windowsHide:true,env,stdio:['pipe','pipe','pipe']});diagnostic.spawnReturnedAt=new Date().toISOString();}catch{throw diagnosticError('LOCAL_METADATA_START_FAILED',diagnostic);}`);
edit('fixed-metadata-reader.mjs',`const finishError=code=>{if(settled)return;settled=true;clean();reject(Error(code));};`,`const finishError=code=>{if(settled)return;settled=true;clean();reject(diagnosticError(code,diagnostic,capture.diagnosticSqlstate(closed)));};`);
edit('fixed-metadata-reader.mjs',`child.stderr.on('data',chunk=>{if(failure)return;capture.stderr(chunk);if(capture.failed)stop(capture.failed);});`,`child.stderr.on('data',chunk=>{diagnostic.firstStderrAt??=new Date().toISOString();const alreadyFailed=!!failure;capture.stderr(chunk);if(!alreadyFailed&&capture.failed)stop(capture.failed);});`);
edit('fixed-metadata-reader.mjs',`}catch(error){phase='FAILED';throw Error(/^LOCAL_METADATA_[A-Z_]+$/.test(error?.message??'')?error.message:'LOCAL_METADATA_READ_REFUSED');}`,`}catch(error){phase='FAILED';throw preserveMetadataDiagnostic(error,Error(/^LOCAL_METADATA_[A-Z_]+$/.test(error?.message??'')?error.message:'LOCAL_METADATA_READ_REFUSED'));}`);
edit('hold-case.mjs',"import {BUDGET,FiniteControls,assertElapsed,assertNoMaps}from'./control.mjs';","import {BUDGET,FiniteControls,assertElapsed,assertNoMaps}from'./control.mjs';\nimport {metadataDiagnosticFor}from'./metadata-diagnostic.mjs';");
edit('hold-case.mjs',`saveSmall(out,'HOLD.json',{status:'HOLD',caseId,code,observedErrorCode:closed(error),deadlineObserved,localTiming,`,`saveSmall(out,'HOLD.json',{status:'HOLD',caseId,code,observedErrorCode:closed(error),deadlineObserved,localTiming,...metadataDiagnosticFor(error)?{metadataDiagnostic:metadataDiagnosticFor(error)}:{},`);
const reverse=JSON.parse(fs.readFileSync(here+'/current/READER_PARENT_REVERSE.json'));
reverse.hunks.push(...changes.filter(r=>r.file==='fixed-metadata-reader.mjs').map(({before,after})=>({before,after})));
if('after'in reverse)reverse.after=fs.readFileSync(here+'/current/fixed-metadata-reader.mjs','utf8');
if('afterSha256'in reverse)reverse.afterSha256=sha(fs.readFileSync(here+'/current/fixed-metadata-reader.mjs'));
fs.writeFileSync(here+'/current/READER_PARENT_REVERSE.json',JSON.stringify(reverse,null,2)+'\n');
edit('README.md','The fixed C reader is a seven-hunk literal successor to the reviewed actor reader, with a complete raw parent inverse.','The fixed C reader retains the seven-hunk actor-reader successor plus the separately reversed closed SQLSTATE diagnostic forward. Only one complete anchored ERROR/FATAL SQLSTATE frame may yield its five uppercase/digit characters; unsupported stderr and NOTICE remain generic refusals. Actual phase/start/spawn-return/first-stderr/finish times accompany any branded fixed-child failure. No raw diagnostic, SQL, query/body, map, server message or guessed cause is saved. The complete raw parent inverse retains both reader stages.');
fs.copyFileSync(here+'/metadata-diagnostic.mjs',here+'/current/metadata-diagnostic.mjs');
const rows=[...manifest.files.map(r=>{const b=fs.readFileSync(here+'/current/'+r.file);return{file:r.file,bytes:b.length,sha256:sha(b)};}),{file:'metadata-diagnostic.mjs',bytes:fs.statSync(here+'/current/metadata-diagnostic.mjs').size,sha256:sha(fs.readFileSync(here+'/current/metadata-diagnostic.mjs'))}];
fs.writeFileSync(here+'/current/MANIFEST.json',JSON.stringify({...manifest,version:'CONTACT_QT_FIXED_CHILD_SQLSTATE_DIAGNOSTIC_FORWARD_V1',parentManifestSha256:sha(originalManifest),files:rows},null,2)+'\n');
const reverses=[];for(const [file,before]of parents){const after=fs.readFileSync(here+'/current/'+file);if(before.equals(after))continue;const selectedHunks=changes.filter(r=>r.file===file);reverses.push({file,beforeRaw:before.toString(),afterRaw:after.toString(),beforeSha256:sha(before),afterSha256:sha(after),wholeRawParentReverse:true,hunks:selectedHunks.length?selectedHunks:[{before:before.toString(),after:after.toString()}]});}
fs.writeFileSync(here+'/PARENT_REVERSES.json',JSON.stringify({version:'CONTACT_QT_DIAGNOSTIC_FULL_MOUNTED_PARENT_REVERSES_V1',parentCommit:commit,parentPrefix:prefix,parentManifestSha256:sha(originalManifest),changed:reverses,original45Preserved:true,newFile:'metadata-diagnostic.mjs'},null,2)+'\n');
const selected=[...rows,{file:'MANIFEST.json',bytes:fs.statSync(here+'/current/MANIFEST.json').size,sha256:sha(fs.readFileSync(here+'/current/MANIFEST.json'))}].map(r=>({target:prefix+'/'+r.file,payload:'current/'+r.file,bytes:r.bytes,sha256:r.sha256,parentSha256:parents.has(r.file)?sha(parents.get(r.file)):null}));
fs.writeFileSync(here+'/PAYLOAD_SELECTION.json',JSON.stringify({version:'CONTACT_QT_DIAGNOSTIC_EXACT_ADAPTER_POSTIMAGE_SELECTION_V1',parentCommit:commit,files:selected,native:'NOT_RUN',runtimeTargets:[]},null,2)+'\n');
console.log(JSON.stringify({selected:selected.length,changedExisting:reverses.length,added:1,parentManifestSha256:sha(originalManifest),native:'NOT_RUN'}));
