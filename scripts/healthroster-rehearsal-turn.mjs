import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {spawn,execFileSync} from 'node:child_process';

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
 constructor(env){
  this.child=spawn(psql,['-X','-q','-A','-t','-v','ON_ERROR_STOP=on','-v','VERBOSITY=sqlstate'],{windowsHide:true,env,stdio:['pipe','pipe','pipe']});
  this.pending=null;this.buffer='';this.closed=false;
  this.child.stdout.on('data',chunk=>{this.buffer+=chunk.toString();let end;while((end=this.buffer.indexOf('\n'))>=0){const line=this.buffer.slice(0,end).replace(/\r$/,'');this.buffer=this.buffer.slice(end+1);const p=this.pending;if(!p)continue;if(line===p.start)p.reading=true;else if(line===p.end){this.pending=null;p.resolve(p.lines);}else if(p.reading){if(line.length>1000000||p.lines.length>20){p.reject(Error('TURN_RESULT_ENVELOPE_REFUSED'));this.close();}else p.lines.push(line);}}});
  // Keep only a closed SQLSTATE. Raw server diagnostics are never emitted/saved.
  this.child.stderr.on('data',chunk=>{const state=chunk.toString().match(/(?:ERROR|FATAL):\s*([A-Z0-9]{5})/)?.[1];if(state&&this.pending){const p=this.pending;this.pending=null;p.reject(Error('TURN_SQLSTATE_'+state));}});
  this.child.on('error',()=>this.fail('TURN_CONNECTION_FAILED'));
  this.child.on('close',()=>{this.closed=true;this.fail('TURN_CONNECTION_CLOSED');});
  this.child.stdin.on('error',()=>this.fail('TURN_INPUT_CLOSED'));
 }
 fail(code){if(this.pending){const p=this.pending;this.pending=null;p.reject(Error(code));}}
 query(sql){if(this.closed||this.pending)throw Error('TURN_QUERY_STATE_REFUSED');const id=crypto.randomBytes(12).toString('hex');return new Promise((resolve,reject)=>{this.pending={start:'HR_TURN_BEGIN_'+id,end:'HR_TURN_END_'+id,reading:false,lines:[],resolve,reject};this.child.stdin.write('\\echo HR_TURN_BEGIN_'+id+'\n'+sql+'\n\\echo HR_TURN_END_'+id+'\n');});}
 async close(){if(this.closed)return;const done=new Promise(resolve=>this.child.once('close',resolve));this.child.stdin.end('\\quit\n');await done;}
}

export function assertNoProtectedRelease(){
 const runs=JSON.parse(execFileSync('C:/Program Files/GitHub CLI/gh.exe',['run','list','--repo','kierarthur/cloudtms-backend','--workflow','340102476','--limit','50','--json','databaseId,status'],{encoding:'utf8',windowsHide:true}));
 if(runs.some(x=>x.status!=='completed'))throw Error('PROTECTED_RELEASE_RUNNING_HOLD');
}

export async function withSharedRehearsalTurn({task,progressPath,work},deps={}){
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
 check();const connection=open();let taken=false,takenAt=null,releaseConfirmed=false;
 try {
  const answer=await connection.query('select pg_try_advisory_lock(20261013, 1);');
  if(answer.length!==1||!['t','f'].includes(answer[0]))throw Error('TURN_LOCK_RESULT_REFUSED');
  if(answer[0]==='f'){log({chat:'HealthRoster',task,status:'BUSY_NO_TRIAL',checkedAt:new Date().toISOString(),nextAttemptAfterMinutes:5});return {status:'BUSY_NO_TRIAL'};}
  taken=true;takenAt=new Date().toISOString();log({chat:'HealthRoster',task,status:'TAKEN',takenAt,key:TURN_KEY});
  const identity=await connection.query("select current_database()='cloudtms_test_clone' and exists(select 1 from private.cloudtms_database_identity where singleton and environment='TEST') and exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='LEGACY' and owner_epoch=1) and not exists(select 1 from private.cloudtms_database_releases where status='APPLYING');");
  if(identity.length!==1||identity[0]!=='t')throw Error('TURN_TEST_IDENTITY_OR_RELEASE_REFUSED');
  // Work owns before, one short trial, rollback, then fresh after proof. The
  // separate turn connection stays alive until ALL of that work settles.
  return await work({turnConnection:connection,takenAt});
 } finally {
  try{if(taken){const result=await connection.query('select pg_advisory_unlock(20261013, 1);');releaseConfirmed=result.length===1&&result[0]==='t';}}
  finally{
   await connection.close();
   if(taken)log({chat:'HealthRoster',task,status:'RELEASED',takenAt,releasedAt:new Date().toISOString(),explicitUnlockConfirmed:releaseConfirmed,connectionClosed:true,key:TURN_KEY});
   // A successful callback cannot become a proved turn when lock ownership or
   // explicit unlock disappeared. Retain the false receipt and fail closed.
   if(taken&&!releaseConfirmed)throw Error('TURN_UNLOCK_CONFIRMATION_FAILED');
  }
 }
}
