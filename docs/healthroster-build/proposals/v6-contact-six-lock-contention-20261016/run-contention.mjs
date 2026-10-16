// ROOT-ONLY NATIVE ENTRY. Importing this file neither reads credentials nor
// opens a connection. No migration body, DDL fixture or business DML is sent.
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {assertSourcePins,buildPlan,sha} from './prepare.mjs';
const here=path.dirname(fileURLToPath(import.meta.url));
const requireThat=(ok,code)=>{if(!ok)throw Object.assign(Error(code),{code});};
const literal=value=>`'${String(value).replaceAll("'","''")}'`;
export const lockMode=mode=>({'share':'ShareLock','access exclusive':'AccessExclusiveLock','access share':'AccessShareLock','row exclusive':'RowExclusiveLock'})[mode];
const one=lines=>{if(typeof lines==='string')lines=lines.split(/\r?\n/).filter(Boolean);requireThat(lines.length===1,'CONTACT_CLOSED_FRAME_REFUSED');return JSON.parse(lines[0]);};
export const relationShapeSql=names=>`select jsonb_build_object('physicalDatabase',current_database(),'valid',
 (select count(*)=${names.length} from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace
 where n.nspname||'.'||c.relname in (${names.map(literal).join(',')}) and c.relkind='r' and c.relpersistence='p' and not c.relispartition
 and c.relowner=(select oid from pg_catalog.pg_roles where rolname=current_user)
 and not exists(select 1 from pg_catalog.pg_inherits i where i.inhrelid=c.oid or i.inhparent=c.oid)));
`;
export function holderProofSql(testCase,relationNames){
 return `select jsonb_build_object('backendPid',pg_backend_pid(),'physicalDatabase',current_database(),
 'readOnly',current_setting('transaction_read_only')::boolean,'targetGranted',exists(select 1 from pg_catalog.pg_locks
 where pid=pg_backend_pid() and database=(select oid from pg_catalog.pg_database where datname=current_database())
 and locktype='relation' and relation=${literal(testCase.targetTable)}::regclass and mode=${literal(lockMode(testCase.holderMode))} and granted),
 'onlyOneWitnessLock',(select count(*)=1 from pg_catalog.pg_locks where pid=pg_backend_pid() and locktype='relation'
 and relation in (${relationNames.map(name=>literal(name)+'::regclass').join(',')})));
`;
}
const acquiredSql=(testCase,pid,count)=>`select jsonb_build_object('writerPid',pg_backend_pid(),'holderAlive',exists(
 select 1 from pg_catalog.pg_stat_activity where pid=${pid} and datid=(select oid from pg_catalog.pg_database where datname=current_database())),
 'holderGranted',exists(select 1 from pg_catalog.pg_locks where pid=${pid} and locktype='relation'
 and database=(select oid from pg_catalog.pg_database where datname=current_database()) and relation=${literal(testCase.targetTable)}::regclass
 and mode=${literal(lockMode(testCase.holderMode))} and granted),
 'precedingGranted',${count?testCase.preceding.slice(0,count).map(row=>`exists(select 1 from pg_catalog.pg_locks where pid=pg_backend_pid() and locktype='relation' and relation=${literal(row.table)}::regclass and mode=${literal(lockMode(row.mode))} and granted)`).join(' and '):'true'});`;
const writerIdentitySql=`select jsonb_build_object('backendPid',pg_backend_pid(),'physicalDatabase',current_database(),
 'serverMajor',current_setting('server_version_num')::integer/10000,
 'testIdentity',exists(select 1 from private.cloudtms_database_identity where singleton and environment='TEST' and customer_key is null),
 'legacy',exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='LEGACY' and owner_epoch=1),
 'noApplying',not exists(select 1 from private.cloudtms_database_releases where status='APPLYING'));`;

// Dependencies are explicit so offline tests exercise failure/cleanup paths
// without importing a connection helper or reading a credential.
export async function qualifyCase({plan,testCase,turn,openWriter,snapshot,assertRestoration,acquireWriterSql,record=()=>{}}){
 await turn.assertHeld(testCase.id+'-before');
 const before=await snapshot(),shape=one(await turn.turnConnection.query(relationShapeSql(plan.relationNames)));
 requireThat(shape.physicalDatabase==='cloudtms_test_clone'&&shape.valid===true,'CONTACT_EXISTING_WITNESS_SHAPE_REFUSED');
 requireThat(before.physicalDatabase==='cloudtms_test_clone'&&before.identityTest===true&&before.customerKeyNull===true
  &&before.module==='LEGACY:1'&&before.markerAbsent===true&&before.applyingCount===0&&before.otherActivityCount===0,'CONTACT_BEFORE_REFUSED');
 let holderStarted=false,writer,writerPid,failure,expected=false,cleanupFailure,after,restoration;
 const receipt={case:testCase.id,targetTable:testCase.targetTable,targetOrdinal:testCase.canonicalTargetOrdinal,
  targetMode:testCase.targetMode,holderMode:testCase.holderMode,prefixSha256:testCase.prefixSha256,
  canonicalPacketSha256:testCase.canonicalPacketSha256,precedingSuccessful:0,expectedSqlState:'55P03',native:'INCOMPLETE',
  originalAssertionsBodyLedgerSent:false,fixtures:0,markerCreated:false,before};
 try{
  await turn.turnConnection.query(`begin ${testCase.holderReadOnly?'read only':'read write'}; set local lock_timeout='2s';\n${testCase.holderLockSql}`);
  holderStarted=true;
  const holder=one(await turn.turnConnection.query(holderProofSql(testCase,plan.relationNames)));
  requireThat(holder.backendPid===turn.identity.backendPid&&holder.physicalDatabase==='cloudtms_test_clone'
   &&holder.readOnly===testCase.holderReadOnly&&holder.targetGranted===true&&holder.onlyOneWitnessLock===true,'CONTACT_HOLDER_PROOF_REFUSED');
  receipt.holder=holder;
  await turn.assertHeld(testCase.id+'-holder');writer=await openWriter();
  const identity=one(await writer.execute(writerIdentitySql));writerPid=identity.backendPid;
  requireThat(Number.isSafeInteger(writerPid)&&writerPid!==holder.backendPid&&identity.physicalDatabase==='cloudtms_test_clone'
   &&identity.serverMajor===17&&identity.testIdentity===true&&identity.legacy===true&&identity.noApplying===true,'CONTACT_WRITER_IDENTITY_REFUSED');
  requireThat((await writer.execute(acquireWriterSql)).trim()==='t','CONTACT_MANAGED_WRITER_BUSY');
  await writer.execute("set lock_timeout='2s'; set statement_timeout='5s'; set default_transaction_isolation='read committed';");
  // Each command is an exact canonical statement. The only interposed queries
  // inspect our two PIDs/locks; no original assertion or body is transmitted.
  await writer.execute(testCase.commands[0]); // original canonical BEGIN
  await writer.execute(testCase.commands[1]); // actual assertWriterSql
  for(const [index,command]of testCase.commands.slice(2,-1).entries()){
   await writer.execute(command);receipt.precedingSuccessful++;
   const proof=one(await writer.execute(acquiredSql(testCase,holder.backendPid,index+1)));
   requireThat(proof.writerPid===writerPid&&proof.holderAlive===true&&proof.holderGranted===true&&proof.precedingGranted===true,'CONTACT_PRECEDING_LOCK_PROOF_REFUSED');
  }
  const ready=one(await writer.execute(acquiredSql(testCase,holder.backendPid,testCase.preceding.length)));
  requireThat(ready.writerPid===writerPid&&ready.holderAlive===true&&ready.holderGranted===true&&ready.precedingGranted===true,'CONTACT_TARGET_PRECONDITION_REFUSED');
  receipt.targetDispatched=true;
  try{await writer.execute(testCase.commands.at(-1));}
  catch(error){receipt.actualSqlState=error.sqlState??null;requireThat(error.sqlState==='55P03','CONTACT_UNEXPECTED_TARGET_SQLSTATE');expected=true;}
  requireThat(expected,'CONTACT_EXPECTED_CONTENTION_MISSING');
  requireThat(receipt.precedingSuccessful===testCase.preceding.length,'CONTACT_PRECEDING_LOCK_COUNT_REFUSED');
  // NOWAIT is part of the exact dispatched target. The safe closed driver
  // reports SQLSTATE only; timing is deliberately not a substitute for it.
  receipt.native='EXPECTED_NOWAIT_REFUSAL_RESTORATION_PENDING';
 }catch(error){failure=error;}
 finally{
  if(writer){
   // On expected ON_ERROR_STOP failure the owned child already closes/rolls
   // back. A missed refusal or earlier local failure rolls back explicitly.
   if(!writer.failure&&!writer.finished)try{await writer.execute('rollback;');}catch(error){cleanupFailure??=error;}
   try{await writer.close();}catch(error){cleanupFailure??=error;}
  }
  if(holderStarted)try{
   const holderStill=one(await turn.turnConnection.query(holderProofSql(testCase,plan.relationNames)));
   requireThat(holderStill.targetGranted===true&&holderStill.backendPid===turn.identity.backendPid,'CONTACT_HOLDER_DISAPPEARED_DURING_TRIAL');
  }catch(error){cleanupFailure??=error;}
  // Also roll back when holder setup partially failed. This touches only our
  // own retained turn session, whose session advisory mutex survives ROLLBACK.
  try{await turn.turnConnection.query('rollback;');holderStarted=false;receipt.holderRolledBack=true;}catch(error){cleanupFailure??=error;}
  try{
   if(writerPid){const gone=one(await turn.turnConnection.query(`select jsonb_build_object('writerGone',not exists(select 1 from pg_catalog.pg_stat_activity where pid=${writerPid} and datid=${turn.identity.databaseOid}), 'writerLocksGone',not exists(select 1 from pg_catalog.pg_locks where pid=${writerPid} and database=${turn.identity.databaseOid}));`));
    requireThat(gone.writerGone===true&&gone.writerLocksGone===true,'CONTACT_WRITER_SHUTDOWN_UNPROVED');receipt.writerShutdown=gone;}
   const holderReleased=one(await turn.turnConnection.query(`select jsonb_build_object('witnessLocksGone',not exists(select 1 from pg_catalog.pg_locks where pid=pg_backend_pid() and locktype='relation' and relation in (${plan.relationNames.map(name=>literal(name)+'::regclass').join(',')})));`));
   requireThat(holderReleased.witnessLocksGone===true,'CONTACT_HOLDER_SHUTDOWN_UNPROVED');receipt.holderWitnessLocksGone=true;
   await turn.assertHeld(testCase.id+'-fresh-restoration');after=await snapshot();receipt.after=after;
   requireThat(after.connectedFresh===true&&after.observerPid!==writerPid&&after.observerPid!==turn.identity.backendPid,'CONTACT_FRESH_CONNECTION_UNPROVED');
   restoration=assertRestoration(before,after,plan.marker);
   receipt.restoration=restoration;receipt.markerAbsent=after.markerAbsent;receipt.legacy=after.module;
  }catch(error){cleanupFailure??=error;}
 }
 receipt.status=failure||cleanupFailure?'HOLD_INCOMPLETE_OR_RESTORATION_FAILED':'PASS_EXPECTED_LOCK_REFUSAL_FRESH_RESTORATION';
 receipt.errorCode=failure?.code??(failure?'CONTACT_TRIAL_FAILED':null);receipt.cleanupCode=cleanupFailure?.code??(cleanupFailure?'CONTACT_CLEANUP_FAILED':null);
 receipt.native=receipt.status;await record(receipt);
 if(failure||cleanupFailure)throw failure??cleanupFailure;
 return receipt;
}

function serializeConnection(connection){
 const query=connection.query.bind(connection);let queue=Promise.resolve();
 connection.query=sql=>{const next=queue.then(()=>query(sql));queue=next.catch(()=>{});return next;};return connection;
}
export async function runNative({root,output,caseId,authorisedRootRun=false}){
 requireThat(authorisedRootRun===true,'ROOT_NATIVE_AUTHORIZATION_REQUIRED');
 const plan=JSON.parse(fs.readFileSync(path.join(here,'PLAN.json'),'utf8'));assertSourcePins(plan,root);
 const reconstructed=await buildPlan(root);
 requireThat(JSON.stringify(reconstructed.cases)===JSON.stringify(plan.cases)&&JSON.stringify(reconstructed.packets)===JSON.stringify(plan.packets)
  &&JSON.stringify(reconstructed.positive)===JSON.stringify(plan.positive),
  'EXACT_CANONICAL_QUALIFICATION_RECONSTRUCTION_CHANGED');
 const selected=plan.cases.filter(row=>row.id===caseId);requireThat(selected.length===1,'ONE_EXACT_CASE_REQUIRED');
 // A fresh task folder is mandatory. No existing native receipt is replaced.
 requireThat(path.dirname(path.resolve(output))===path.join(path.resolve(root),'.codex-tmp')&&/^contact-six-locks-20261016-[a-z0-9-]+$/.test(path.basename(output))&&!fs.existsSync(output),'ROOT_FRESH_OUTPUT_DIRECTORY_REQUIRED');
 fs.mkdirSync(output,{recursive:true});
 const load=file=>import(pathToFileURL(path.join(root,file)).href);
 const native=await load('scripts/healthroster-rehearsal-turn.mjs'),driver=await load('scripts/cloudtms-managed-psql.mjs'),
  rehearsal=await load('scripts/healthroster-install1-full-rehearsal.mjs'),compiler=await load('scripts/cloudtms-managed-release.mjs');
 const bin='C:/Program Files/PostgreSQL/18/bin/psql.exe';
 const openWriter=async()=>new driver.ManagedPsqlSession({bin,args:['-X','-q','-A','-t','-v','ON_ERROR_STOP=on','-v','VERBOSITY=sqlstate'],
  cwd:root,env:native.agencyChildEnvironment('healthroster_contact_six_lock_owned'),timeoutMs:10000,closeTimeoutMs:10000});
 const record=value=>fs.writeFileSync(path.join(output,value.case+'.json'),JSON.stringify(value,null,2)+'\n');
 const result=await native.withSharedRehearsalTurn({task:'HEALTH_CONTACT_SIX_EXACT_NOWAIT_LOCKS',progressPath:path.join(root,'docs/healthroster-build/PROGRESS.md'),
  async work(turn){
   const snapshot=async()=>{
    const observer=await openWriter();
    try{
     await observer.execute("begin read only; set local lock_timeout='2s';");
     const identity=one(await observer.execute(writerIdentitySql));
     requireThat(identity.physicalDatabase==='cloudtms_test_clone'&&identity.serverMajor===17&&identity.testIdentity&&identity.legacy&&identity.noApplying,'CONTACT_OBSERVER_IDENTITY_REFUSED');
     const sql=rehearsal.restorationSnapshotSql({identity:turn.identity,marker:plan.marker,
      relations:plan.relationNames.map(name=>({schema:'public',name:name.slice(7),kind:'r'})),
      exportContractSql:fs.readFileSync(path.join(root,'supabase/release/export_contract.sql'),'utf8'),observeLiveBusiness:false});
     const value=one(await observer.execute(sql));await observer.execute('rollback;');
     return {...value,marker:plan.marker,observerPid:identity.backendPid,connectedFresh:true};
    }finally{await observer.close();}
   };
   const receipts=[];
   for(const testCase of selected){assertSourcePins(plan,root);receipts.push(await qualifyCase({plan,testCase,turn,openWriter,snapshot,assertRestoration:rehearsal.assertFreshRestoration,acquireWriterSql:compiler.acquireWriterSql,record}));}
   return {status:'PASS_ONE_BOUNDED_LOCK_REFUSAL_AND_FRESH_RESTORATION',caseId,cases:receipts.length,planSha256:sha(fs.readFileSync(path.join(here,'PLAN.json'))),
    originalMigrationBodiesRun:false,fixtures:0,markerCreated:false,maxConcurrentDatabaseSessions:2,
    outerRepeatableReadSnapshotCurrentness:'NOT_QUALIFIED',positiveFullMigrations:'NOT_RUN',fullRehearsal:'NOT_RUN',PartB:'NOT_RUN'};
  }},{openTurn:()=>serializeConnection(new native.OwnedTurnConnection(native.agencyChildEnvironment('healthroster_contact_six_lock_turn')))});
 fs.writeFileSync(path.join(output,'SUMMARY.json'),JSON.stringify(result,null,2)+'\n');return result;
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 try{const [root,output,flag,caseId]=process.argv.slice(2);requireThat(flag==='--authorised-root-native-run','ROOT_NATIVE_AUTHORIZATION_REQUIRED');console.log(JSON.stringify(await runNative({root,output,caseId,authorisedRootRun:true})));}
 catch(error){console.error(JSON.stringify({status:'HOLD',code:/^[A-Z0-9_]+$/.test(error.code??'')?error.code:'CONTACT_CLOSED_NATIVE_FAILURE'}));process.exitCode=1;}
}
