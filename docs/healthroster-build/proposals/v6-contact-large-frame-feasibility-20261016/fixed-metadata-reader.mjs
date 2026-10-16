// SOURCE PROPOSAL ONLY. Import only from the root's live withSharedRehearsalTurn
// callback. There is deliberately no CLI, arbitrary SQL, query callback or file sink.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {spawn} from 'node:child_process';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {source} from '../v6-contact-six-lock-mcp-successor-20261016/input-builder.mjs';

export const LIMITS=Object.freeze({stdoutBytes:64*1024*1024,stderrBytes:16*1024,
 connectSeconds:10,statementMilliseconds:45000,lockMilliseconds:2000,
 childMilliseconds:70000,terminateGraceMilliseconds:2000,closeGraceMilliseconds:3000});
const psql='C:/Program Files/PostgreSQL/18/bin/psql.exe';
const check=(ok,code)=>{if(!ok)throw Error(code);};
const sha=v=>crypto.createHash('sha256').update(v).digest('hex');
const literal=v=>`'${String(v).replaceAll("'","''")}'`;
const expectedPins=JSON.parse(fs.readFileSync(new URL('./SOURCE_PINS.json',import.meta.url),'utf8'));
const protectedFiles=expectedPins.rootInputs;

// Pure transport state used by the offline tests. Once failed, no further bytes
// are retained. A cap bounds captured bytes, not all JSON objects/V8 process RAM.
export class BoundedMemoryCapture {
 constructor(){this.bytes=0;this.stderrBytes=0;this.chunks=[];this.failed=null;}
 stdout(chunk){
  if(this.failed)return;
  if(!Buffer.isBuffer(chunk))chunk=Buffer.from(chunk);
  if(this.bytes+chunk.length>LIMITS.stdoutBytes){this.failed='LOCAL_METADATA_STDOUT_BOUND';this.chunks=[];return;}
  this.bytes+=chunk.length;this.chunks.push(chunk);
 }
 stderr(chunk){
  if(this.failed)return;
  this.stderrBytes+=Buffer.byteLength(chunk);
  this.failed=this.stderrBytes>LIMITS.stderrBytes?'LOCAL_METADATA_STDERR_BOUND':'LOCAL_METADATA_DIAGNOSTIC_REFUSED';
  this.chunks=[];
 }
 finish(){
  check(!this.failed,this.failed);
  const bytes=Buffer.concat(this.chunks,this.bytes);this.chunks=[];
  let text;try{text=new TextDecoder('utf-8',{fatal:true}).decode(bytes);}catch{throw Error('LOCAL_METADATA_UTF8_REFUSED');}
  const lines=text.replaceAll('\r\n','\n').split('\n');
  if(lines.at(-1)==='')lines.pop();
  check(lines.length===2&&lines.every(v=>v.startsWith('{')&&v.endsWith('}')),'LOCAL_METADATA_FRAME_COUNT_REFUSED');
  let peer,snapshot;try{peer=JSON.parse(lines[0]);snapshot=JSON.parse(lines[1]);}catch{throw Error('LOCAL_METADATA_JSON_REFUSED');}
  check(peer&&typeof peer==='object'&&!Array.isArray(peer)&&snapshot&&typeof snapshot==='object'&&!Array.isArray(snapshot),'LOCAL_METADATA_OBJECT_REQUIRED');
  return {peer,snapshot,transport:{stdoutBytes:this.bytes,snapshotUtf8Bytes:Buffer.byteLength(lines[1]),snapshotSha256:sha(lines[1])}};
 }
}

function assertPins(root){
 for(const row of protectedFiles)check(sha(fs.readFileSync(path.join(root,row.file)))===row.sha256,'LOCAL_METADATA_REVIEWED_SOURCE_CHANGED');
}
async function runFixedChild({input,env,turn}){
 try{await turn.assertHeld('before-fixed-read-only-metadata');}catch{throw Error('LOCAL_METADATA_TURN_LOST');}
 check(!turn.signal.aborted,'LOCAL_METADATA_TURN_LOST');
 let child;try{child=spawn(psql,['-X','-q','-A','-t','-v','ON_ERROR_STOP=on','-v','VERBOSITY=sqlstate'],
  {windowsHide:true,env,stdio:['pipe','pipe','pipe']});}catch{throw Error('LOCAL_METADATA_START_FAILED');}
 return new Promise((resolve,reject)=>{
  const capture=new BoundedMemoryCapture();let failure=null,closed=false,settled=false,termTimer,closeTimer;
  const clean=()=>{clearTimeout(deadline);clearTimeout(termTimer);clearTimeout(closeTimer);turn.signal.removeEventListener('abort',lost);};
  const finishError=code=>{if(settled)return;settled=true;clean();reject(Error(code));};
  const stop=code=>{
   failure??=code;if(closed)return;
   child.kill('SIGTERM');
   termTimer??=setTimeout(()=>{if(!closed)child.kill('SIGKILL');},LIMITS.terminateGraceMilliseconds);
   closeTimer??=setTimeout(()=>{if(!closed)finishError('LOCAL_METADATA_CHILD_CLOSE_UNCONFIRMED');},LIMITS.terminateGraceMilliseconds+LIMITS.closeGraceMilliseconds);
  };
  const lost=()=>stop('LOCAL_METADATA_TURN_LOST');
  const deadline=setTimeout(()=>stop('LOCAL_METADATA_CHILD_DEADLINE'),LIMITS.childMilliseconds);
  turn.signal.addEventListener('abort',lost,{once:true});
  child.stdout.on('data',chunk=>{if(failure)return;capture.stdout(chunk);if(capture.failed)stop(capture.failed);});
  child.stderr.on('data',chunk=>{if(failure)return;capture.stderr(chunk);if(capture.failed)stop(capture.failed);});
  child.stdin.on('error',()=>stop('LOCAL_METADATA_INPUT_FAILED'));
  child.once('error',()=>stop('LOCAL_METADATA_START_FAILED'));
  child.once('close',async(code,signal)=>{
   closed=true;if(settled)return;clean();
   if(failure){finishError(failure);return;}
   if(code!==0||signal){finishError('LOCAL_METADATA_CHILD_FAILED');return;}
   try{await turn.assertHeld('after-fixed-read-only-metadata');const result=capture.finish();settled=true;resolve({...result,childClosed:true});}
   catch(error){finishError(/^LOCAL_METADATA_[A-Z_]+$/.test(error?.message??'')?error.message:'LOCAL_METADATA_READ_REFUSED');}
  });
  child.stdin.end(input);if(turn.signal.aborted)lost();
 });
}

export async function createFixedMetadataReader({root,turn,caseId}){
 check(turn&&typeof turn.assertHeld==='function'&&turn.signal&&typeof turn.runOwnedChild==='function','LOCAL_METADATA_ROOT_CALLBACK_REQUIRED');
 assertPins(root);
 const {plan,rehearsal,live}=await source(root);
 check(plan.cases.some(v=>v.id===caseId),'LOCAL_METADATA_EXACT_CASE_REQUIRED');
 const helpers=await import(pathToFileURL(path.join(root,'scripts/healthroster-rehearsal-turn.mjs')).href);
 const controls=await import(pathToFileURL(path.join(root,'scripts/healthroster-turn-control.mjs')).href);
 controls.assertTurnIdentity(turn.identity);
 // Exact existing finite Contact scope: the six witnesses plus both complete
 // background maps. No filter, digest substitute or narrower quiet dialect.
 const relations=[...plan.relationNames,...live.backgroundRelations].map(name=>({schema:name.split('.')[0],name:name.split('.')[1],kind:'r'}));
 const canonicalSql=rehearsal.restorationSnapshotSql({identity:turn.identity,marker:plan.marker,relations,
  exportContractSql:fs.readFileSync(path.join(root,'supabase/release/export_contract.sql'),'utf8'),observeLiveBusiness:true});
 const guard=`do $hr_metadata_guard$ begin if current_database()<>'cloudtms_test_clone'
  or current_setting('transaction_read_only')<>'on' or current_setting('transaction_isolation')<>'repeatable read'
  or not exists(select 1 from private.cloudtms_database_identity where singleton and environment='TEST' and customer_key is null)
  or not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='LEGACY' and owner_epoch=1)
  or exists(select 1 from private.cloudtms_database_releases where status='APPLYING')
  or to_regclass(${literal('private.'+plan.marker)}) is not null
  or (select oid from pg_catalog.pg_database where datname=current_database())<>${turn.identity.databaseOid}
  or not exists(select 1 from pg_catalog.pg_locks where locktype='advisory' and database=${turn.identity.databaseOid}
   and pid=${turn.identity.backendPid} and classid=20261013 and objid=1 and objsubid=2 and mode='ExclusiveLock' and granted)
  then raise exception 'LOCAL_METADATA_IDENTITY_REFUSED' using errcode='55000'; end if; end; $hr_metadata_guard$;`;
 const peerSql=`select jsonb_build_object('database',current_database(),'databaseOid',(select oid::bigint from pg_catalog.pg_database where datname=current_database()),
  'readerPid',pg_backend_pid(),'holderPid',${turn.identity.backendPid},'readOnly',current_setting('transaction_read_only')::boolean,
  'isolation',current_setting('transaction_isolation'),'noRoleSwitch',current_user=session_user)::text;`;
 const input='begin isolation level repeatable read read only;\n'+guard+'\n'+peerSql+'\n'+canonicalSql+'\nrollback;\n';
 // The exact canonical query including its terminal delimiter occurs once.
 check(input.split(canonicalSql).length===2,'LOCAL_METADATA_CANONICAL_EMBED_REFUSED');
 let phase='BEFORE',beforePeer;
 const read=async(wanted,nativeBackendPid)=>{
  check(phase===wanted,'LOCAL_METADATA_PHASE_REFUSED');phase='RUNNING';assertPins(root);
  if(wanted==='AFTER')check(Number.isSafeInteger(nativeBackendPid)&&nativeBackendPid>0&&nativeBackendPid!==turn.identity.backendPid,'LOCAL_METADATA_NATIVE_B_PID_REQUIRED');
  try{
   const env=helpers.agencyChildEnvironment('healthroster_contact_fixed_metadata');
   env.PGCLIENTENCODING='UTF8';env.PGCONNECT_TIMEOUT=String(LIMITS.connectSeconds);
   env.PGOPTIONS+=' -c default_transaction_read_only=on -c statement_timeout='+LIMITS.statementMilliseconds+' -c lock_timeout='+LIMITS.lockMilliseconds;
   const result=await runFixedChild({input,env,turn}),p=result.peer,s=result.snapshot;
   check(p.database==='cloudtms_test_clone'&&p.databaseOid===turn.identity.databaseOid&&Number.isSafeInteger(p.readerPid)&&p.readerPid>0
    &&p.readerPid!==turn.identity.backendPid&&p.holderPid===turn.identity.backendPid&&p.readOnly===true&&p.isolation==='repeatable read'&&p.noRoleSwitch===true,'LOCAL_METADATA_NATIVE_PEER_REFUSED');
   if(wanted==='AFTER')check(p.readerPid!==nativeBackendPid&&p.readerPid!==beforePeer.readerPid,'LOCAL_METADATA_AFTER_NOT_FRESH');
   check(s.physicalDatabase==='cloudtms_test_clone'&&s.identityTest===true&&s.customerKeyNull===true&&s.module==='LEGACY:1'
    &&s.applyingCount===0&&s.markerAbsent===true&&s.backgroundRows&&s.backgroundWriterRoutines&&Array.isArray(s.businessRelations),'LOCAL_METADATA_CANONICAL_IDENTITY_REFUSED');
   assertPins(root);phase=wanted==='BEFORE'?'AFTER':'DONE';if(wanted==='BEFORE')beforePeer=p;
   return {...result,snapshot:{...s,marker:plan.marker,...wanted==='AFTER'?{connectedFresh:true}:{}},
    provenance:{phase:wanted,caseId,canonicalSqlSha256:sha(canonicalSql),inputSha256:sha(input),sourcePinsSha256:sha(JSON.stringify(expectedPins)),
     childClosed:true,route:'OWNER_SEND5B_FIXED_READ_ONLY_METADATA_NOT_PROPOSED_B_SQL',observeLiveBusiness:true,nativeQualification:'NOT_PROVED_BY_READER'}};
  }catch(error){phase='FAILED';throw Error(/^LOCAL_METADATA_[A-Z_]+$/.test(error?.message??'')?error.message:'LOCAL_METADATA_READ_REFUSED');}
 };
 return Object.freeze({before:()=>read('BEFORE'),after:({nativeBackendPid})=>read('AFTER',nativeBackendPid),
  sourceDescriptor:Object.freeze({canonicalSqlSha256:sha(canonicalSql),inputSha256:sha(input),caseId,limits:LIMITS}),
  // Pure source proof; no authority and no connection is created by inspection.
  inspectSource:()=>({input,canonicalSql,relations})});
}

if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url))throw Error('SOURCE_ONLY_ROOT_METADATA_CALLBACK_REQUIRED');
