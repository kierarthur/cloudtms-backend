import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {spawn,execFileSync} from 'node:child_process';
import {closedTurnError,runOwnedTurnChild,serveTurnControl,ownTurnActivityExclusionSql} from './healthroster-turn-control.mjs';

export const TURN_KEY = Object.freeze([20261013,1]);
const workspace='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub';
const psql='C:/Program Files/PostgreSQL/18/bin/psql.exe';

// Credentials exist only in this process and the owned child's environment.
// Neither the connection URI nor credentials enter command arguments or receipts.
export function agencyChildEnvironment(applicationName){
 const secret=JSON.parse(fs.readFileSync(path.join(workspace,'cloudtms-backend/.codex-tmp/miget-postgres-service.secret.json'),'utf8'));
 const url=new URL(secret.connection_details.external.external_connection_url);
 if(!['postgres:','postgresql:'].includes(url.protocol))throw Error('AGENCY_CONNECTION_SCHEME_REFUSED');
 url.pathname='/cloudtms_test_clone';
 const inherited=Object.fromEntries(Object.entries(process.env).filter(([key])=>!/^PG/i.test(key)));
 const env={...inherited,PGHOST:url.hostname,PGPORT:url.port||'5432',PGDATABASE:'cloudtms_test_clone',PGUSER:decodeURIComponent(url.username),PGPASSWORD:decodeURIComponent(url.password),PGCONNECT_TIMEOUT:'10'};
 const options={sslmode:'PGSSLMODE',sslcert:'PGSSLCERT',sslkey:'PGSSLKEY',sslrootcert:'PGSSLROOTCERT',sslcrl:'PGSSLCRL',sslcrldir:'PGSSLCRLDIR',sslpassword:'PGSSLPASSWORD',gssencmode:'PGGSSENCMODE',channel_binding:'PGCHANNELBINDING',target_session_attrs:'PGTARGETSESSIONATTRS',hostaddr:'PGHOSTADDR',client_encoding:'PGCLIENTENCODING',application_name:'PGAPPNAME',connect_timeout:'PGCONNECT_TIMEOUT',options:'PGOPTIONS'};
 for(const [key,value] of url.searchParams){if(!Object.hasOwn(options,key))throw Error('UNREVIEWED_LIBPQ_OPTION');env[options[key]]=value;}
 env.PGAPPNAME=applicationName;
 env.PGOPTIONS=(env.PGOPTIONS??'')+' -c jit=off -c pg_show_plans.is_enabled=off -c statement_timeout=5s';
 return env;
}

export class OwnedTurnConnection {
 constructor(env,{probeMs=10000}={}){
  this.child=spawn(psql,['-X','-q','-A','-t','-v','ON_ERROR_STOP=on','-v','VERBOSITY=sqlstate'],{windowsHide:true,env,stdio:['pipe','pipe','pipe']});
  this.pending=null;this.buffer='';this.closed=false;this.probeMs=probeMs;this.lossController=new AbortController();this.signal=this.lossController.signal;this.closing=false;
  this.child.stdout.on('data',chunk=>{this.buffer+=chunk.toString();let end;while((end=this.buffer.indexOf('\n'))>=0){const line=this.buffer.slice(0,end).replace(/\r$/,'');this.buffer=this.buffer.slice(end+1);const p=this.pending;if(!p)continue;if(line===p.start)p.reading=true;else if(line===p.end){this.pending=null;clearTimeout(p.timer);p.resolve(p.lines);}else if(p.reading){if(line.length>1000000||p.lines.length>20){this.fail('TURN_RESULT_ENVELOPE_REFUSED');this.child.kill();}else p.lines.push(line);}}});
  // Keep only a closed SQLSTATE. Raw server diagnostics are never emitted/saved.
  this.child.stderr.on('data',chunk=>{const state=chunk.toString().match(/(?:ERROR|FATAL):\s*([A-Z0-9]{5})/)?.[1];if(state)this.fail('TURN_SQLSTATE_'+state);});
  this.child.on('error',()=>this.fail('TURN_CONNECTION_FAILED'));
  this.child.on('close',()=>{this.closed=true;this.fail('TURN_CONNECTION_CLOSED');});
  this.child.stdin.on('error',()=>this.fail('TURN_INPUT_CLOSED'));
 }
 fail(code){if(!this.closing&&!this.signal.aborted)this.lossController.abort(closedTurnError(code));if(this.pending){const p=this.pending;this.pending=null;clearTimeout(p.timer);p.reject(closedTurnError(code));}}
 query(sql){if(this.closed||this.pending||this.signal.aborted)throw closedTurnError('TURN_QUERY_STATE_REFUSED');const id=crypto.randomBytes(12).toString('hex');return new Promise((resolve,reject)=>{const timer=setTimeout(()=>{this.fail('TURN_LIVENESS_PROBE_TIMEOUT');this.child.kill();},this.probeMs);this.pending={start:'HR_TURN_BEGIN_'+id,end:'HR_TURN_END_'+id,reading:false,lines:[],resolve,reject,timer};this.child.stdin.write('\\echo HR_TURN_BEGIN_'+id+'\n'+sql+'\n\\echo HR_TURN_END_'+id+'\n');});}
 async close(){if(this.closed)return;this.closing=true;const done=new Promise(resolve=>this.child.once('close',resolve));const timer=setTimeout(()=>this.child.kill(),this.probeMs);this.child.stdin.end('\\quit\n');try{await done;}finally{clearTimeout(timer);}}
}

export const TURN_OWNERSHIP_SQL=`select json_build_object(
 'physicalDatabase',current_database(),'databaseOid',(select oid::bigint from pg_catalog.pg_database where datname=current_database()),
 'backendPid',pg_backend_pid(),'testIdentity',exists(select 1 from private.cloudtms_database_identity where singleton and environment='TEST'),
 'legacyControl',exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='LEGACY' and owner_epoch=1),
 'held',exists(select 1 from pg_catalog.pg_locks where locktype='advisory' and pid=pg_backend_pid()
 and database=(select oid from pg_catalog.pg_database where datname=current_database())
 and classid=20261013 and objid=1 and objsubid=1 and mode='ExclusiveLock' and granted));`;

function readOwnership(lines,expected){
 let row;try{if(lines.length!==1)throw Error();row=JSON.parse(lines[0]);}catch{throw closedTurnError('TURN_OWNERSHIP_RESULT_REFUSED');}
 if(row.physicalDatabase!=='cloudtms_test_clone'||!Number.isSafeInteger(row.databaseOid)||row.databaseOid<1
  ||!Number.isSafeInteger(row.backendPid)||row.backendPid<1||row.testIdentity!==true||row.legacyControl!==true||row.held!==true
  ||expected&&(row.databaseOid!==expected.databaseOid||row.backendPid!==expected.backendPid))throw closedTurnError('TURN_OWNERSHIP_LOST');
 return row;
}

export function assertNoProtectedRelease(){
 const runs=JSON.parse(execFileSync('C:/Program Files/GitHub CLI/gh.exe',['run','list','--repo','kierarthur/cloudtms-backend','--workflow','340102476','--limit','50','--json','databaseId,status'],{encoding:'utf8',windowsHide:true}));
 if(runs.some(x=>x.status!=='completed'))throw Error('PROTECTED_RELEASE_RUNNING_HOLD');
}

export async function withSharedRehearsalTurn({task,progressPath,work,monitorMs=5000,beforePhase},deps={}){
 const now=deps.now??(()=>Date.now());
 const lastBusy=deps.readLastBusy??(()=>{
  if(!progressPath||!fs.existsSync(progressPath))return null;
  let latest=null;
  for(const line of fs.readFileSync(progressPath,'utf8').split(/\r?\n/)){
   if(!line.startsWith('SEND_7 turn: '))continue;
   const record=JSON.parse(line.slice('SEND_7 turn: '.length));
   if(record.chat==='HealthRoster'&&record.status==='BUSY_NO_TRIAL'){
    const time=Date.parse(record.checkedAt);if(!Number.isFinite(time))throw Error('TURN_BUSY_TIMESTAMP_INVALID');
    latest=Math.max(latest??time,time);
   }
  }
  return latest;
 });
 const busyAt=lastBusy();
 if(busyAt!==null){
  if(!Number.isFinite(busyAt))throw Error('TURN_BUSY_TIMESTAMP_INVALID');
  if(now()<busyAt+300000)return {status:'COOLDOWN_NO_CONNECTION',nextEligibleAt:new Date(busyAt+300000).toISOString()};
 }
 const check=deps.checkProtectedRelease??assertNoProtectedRelease;
 const open=deps.openTurn??(()=>new OwnedTurnConnection(agencyChildEnvironment('healthroster_rehearsal_turn')));
 const log=deps.log??(record=>{fs.appendFileSync(progressPath,'\nSEND_7 turn: '+JSON.stringify(record)+'\n');});
 await check();const connection=open();let taken=false,takenAt=null,releaseConfirmed=false,primaryError,result,monitor,probe,control;
 const controller=new AbortController();
 const lose=error=>{if(!controller.signal.aborted){controller.abort(error);log({chat:'HealthRoster',task,status:'LOST_INCOMPLETE',at:new Date().toISOString(),code:error.code??'SHARED_TURN_LOST',key:TURN_KEY});}};
 const connectionLost=()=>lose(connection.signal.reason??closedTurnError('TURN_CONNECTION_CLOSED'));
 connection.signal?.addEventListener('abort',connectionLost,{once:true});
 try {
  const answer=await connection.query('select pg_try_advisory_lock(20261013, 1);');
  if(answer.length!==1||!['t','f'].includes(answer[0]))throw Error('TURN_LOCK_RESULT_REFUSED');
  if(answer[0]==='f'){log({chat:'HealthRoster',task,status:'BUSY_NO_TRIAL',checkedAt:new Date().toISOString(),nextAttemptAfterMinutes:5});return {status:'BUSY_NO_TRIAL'};}
  taken=true;takenAt=new Date().toISOString();log({chat:'HealthRoster',task,status:'TAKEN',takenAt,key:TURN_KEY});
  const identity=await connection.query("select current_database()='cloudtms_test_clone' and exists(select 1 from private.cloudtms_database_identity where singleton and environment='TEST') and exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='LEGACY' and owner_epoch=1) and not exists(select 1 from private.cloudtms_database_releases where status='APPLYING');");
  if(identity.length!==1||identity[0]!=='t')throw Error('TURN_TEST_IDENTITY_OR_RELEASE_REFUSED');
  const owned=readOwnership(await connection.query(TURN_OWNERSHIP_SQL));
  const trusted=Object.freeze({physicalDatabase:owned.physicalDatabase,databaseOid:owned.databaseOid,backendPid:owned.backendPid,
   turnId:'hr_turn_'+crypto.randomBytes(16).toString('hex'),key:TURN_KEY,takenAt,task});
  const assertHeld=async phase=>{
   if(controller.signal.aborted)throw controller.signal.reason;
   // Interval and phase checks share one small probe; no concurrent psql input.
   if(!probe){probe=(async()=>{try{readOwnership(await connection.query(TURN_OWNERSHIP_SQL),trusted);}catch(error){lose(error);throw error;}finally{probe=null;}})();}
   await probe;if(controller.signal.aborted)throw controller.signal.reason;
   // Optional coordinator gate can re-read Banking's completed install/turn
   // record and exact shared source immediately before protected dispatch.
   if(phase&&beforePhase)await beforePhase(phase,trusted);
   if(controller.signal.aborted)throw controller.signal.reason;
  };
  if(!Number.isFinite(monitorMs)||monitorMs<1)throw Error('TURN_MONITOR_INTERVAL_REFUSED');
  monitor=setInterval(()=>{void assertHeld().catch(()=>{});},monitorMs);
  const context={turnConnection:connection,takenAt,identity:trusted,signal:controller.signal,assertHeld,
   activityExclusionSql:ownTurnActivityExclusionSql(trusted),
   runOwnedChild:options=>runOwnedTurnChild({...options,signal:controller.signal,assertHeld}),
   async managedReleaseEnvironment(){await assertHeld('managed-release-control');control??=await serveTurnControl({identity:trusted,signal:controller.signal,assertHeld});return control.environment;},
   async runManagedRelease({npmCli,connectionsProof,officePath,cwd=process.cwd(),env=process.env,node=process.execPath}){
    if(!path.isAbsolute(npmCli)||!path.isAbsolute(connectionsProof))throw Error('MANAGED_RELEASE_ABSOLUTE_INPUTS_REQUIRED');
    const shared=await context.managedReleaseEnvironment();
    return context.runOwnedChild({file:node,args:[npmCli,'run','release:test','--','--shared-turn',`--connections-proof=${connectionsProof}`,...officePath?[`--office-path=${officePath}`]:[]],
     options:{cwd,env:{...env,...shared},stdio:['ignore','pipe','pipe']},cancelOnLoss:false});
   }};
  // The same retained session spans before/full SQL/rollback/fresh proof AND
  // managed database wait, ordered runtime publication and Office publication.
  await assertHeld('before-work');result=await work(context);await assertHeld('callback-complete');
 } catch(error){primaryError=error;
 } finally {
  clearInterval(monitor);try{await probe;}catch(error){primaryError??=error;}
  try{await control?.close();}catch(error){primaryError??=error;}
  try{if(taken){const unlock=await connection.query('select pg_advisory_unlock(20261013, 1);');releaseConfirmed=unlock.length===1&&unlock[0]==='t';}}catch(error){primaryError??=error;}
  let connectionClosed=false;try{await connection.close();connectionClosed=true;}catch(error){primaryError??=error;}
  connection.signal?.removeEventListener('abort',connectionLost);
  if(taken)log({chat:'HealthRoster',task,status:'RELEASED',takenAt,releasedAt:new Date().toISOString(),explicitUnlockConfirmed:releaseConfirmed,connectionClosed,
   proved:!primaryError&&!controller.signal.aborted&&releaseConfirmed&&connectionClosed,key:TURN_KEY});
  // Preserve the original source/child error; cleanup failure never hides it.
  if(taken&&!releaseConfirmed){if(primaryError)primaryError.turnUnlockConfirmed=false;else primaryError=closedTurnError('TURN_UNLOCK_CONFIRMATION_FAILED');}
  if(controller.signal.aborted)primaryError??=controller.signal.reason;
 }
 if(primaryError)throw primaryError;return result;
}
