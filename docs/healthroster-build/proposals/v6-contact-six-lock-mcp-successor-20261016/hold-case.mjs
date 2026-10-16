// ROOT-ONLY holder A. Never opens a second direct database session, executes
// B's proposed SQL, invokes MCP, reads another account credential or pauses jobs.
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {source,buildInput,saveInput,holderProofSql,requireThat,literal} from './input-builder.mjs';
import {validatePair} from './result-validator.mjs';
const one=lines=>{requireThat(lines.length===1,'HOLDER_ONE_FRAME_REQUIRED');return JSON.parse(lines[0]);};
function serialized(connection){const query=connection.query.bind(connection);let queue=Promise.resolve();connection.query=sql=>{const next=queue.then(()=>query(sql));queue=next.catch(()=>{});return next;};return connection;}
export async function holdCase({root,out,caseId,authorisedRootHolder=false}){
 requireThat(authorisedRootHolder===true,'ROOT_HOLDER_AUTHORIZATION_REQUIRED');
 const {plan}=await source(root),row=plan.cases.find(r=>r.id===caseId);requireThat(row,'EXACT_CASE_REQUIRED');
 requireThat(path.dirname(path.resolve(out))===path.join(path.resolve(root),'.codex-tmp')&&/^contact-six-mcp-20261016-[a-z0-9-]+$/.test(path.basename(out))&&!fs.existsSync(out),'FRESH_ROOT_OUTPUT_DIRECTORY_REQUIRED');
 fs.mkdirSync(out,{recursive:true});
 const native=await import(pathToFileURL(path.join(root,'scripts/healthroster-rehearsal-turn.mjs')).href);
 const result=await native.withSharedRehearsalTurn({task:'HEALTH_CONTACT_MCP_'+caseId.toUpperCase(),progressPath:path.join(root,'docs/healthroster-build/PROGRESS.md'),
  async work(turn){let completion='HOLD_ABORTED_WITHOUT_NATIVE_QUALIFICATION';
   try{
    const shape=one(await turn.turnConnection.query(`select jsonb_build_object('valid',(select count(*)=6 from pg_class c join pg_namespace n on n.oid=c.relnamespace
     where n.nspname||'.'||c.relname in (${plan.relationNames.map(literal).join(',')}) and c.relkind='r' and c.relpersistence='p' and not c.relispartition
     and c.relowner=(select oid from pg_roles where rolname=current_user) and not exists(select 1 from pg_inherits i where i.inhrelid=c.oid or i.inhparent=c.oid)));`));
    requireThat(shape.valid===true,'EXACT_NATIVE_WITNESS_SHAPE_REQUIRED');
    await turn.turnConnection.query(`begin ${row.holderReadOnly?'read only':'read write'}; set local lock_timeout='2s'; ${row.holderLockSql}`);
    const proof=one(await turn.turnConnection.query(holderProofSql(row,plan.relationNames)));
    requireThat(proof.backendPid===turn.identity.backendPid&&proof.databaseOid===turn.identity.databaseOid&&proof.physicalDatabase==='cloudtms_test_clone'
     &&proof.readOnly===row.holderReadOnly&&proof.targetGranted===true&&proof.onlyTargetWitness===true,'EXACT_OWNED_HOLDER_REQUIRED');
    fs.writeFileSync(path.join(out,'TURN_IDENTITY.json'),JSON.stringify(turn.identity,null,2)+'\n');
    fs.writeFileSync(path.join(out,'HOLDER_PROOF.json'),JSON.stringify(proof,null,2)+'\n');
    saveInput(out,await buildInput({root,caseId,identity:turn.identity}));
    console.log(JSON.stringify({status:'OWNED_HOLDER_READY_ROOT_MCP_CALL_REQUIRED',caseId,out,backendPid:turn.identity.backendPid,route:'mcp__cloudtms_miget_operations_kier_codex__miget_db_transaction_rollback'}));
    const action=await new Promise((resolve,reject)=>{
     let buffer='',done=false;const finish=action=>{if(done)return;done=true;clearTimeout(timer);turn.signal.removeEventListener('abort',lost);process.stdin.removeListener('data',data);process.stdin.removeListener('end',eof);process.removeListener('SIGINT',abort);process.removeListener('SIGTERM',abort);process.stdin.pause();resolve(action);};
     const lost=()=>{finish('ABORT');reject(turn.signal.reason);},abort=()=>finish('ABORT'),eof=()=>finish('ABORT');
     const data=chunk=>{buffer+=chunk;let end;while((end=buffer.indexOf('\n'))>=0){const line=buffer.slice(0,end).trim();buffer=buffer.slice(end+1);if(['RELEASE','ABORT'].includes(line))finish(line);}};
     const timer=setTimeout(abort,180000);turn.signal.addEventListener('abort',lost,{once:true});process.stdin.setEncoding('utf8');process.stdin.on('data',data);process.stdin.once('end',eof);process.once('SIGINT',abort);process.once('SIGTERM',abort);process.stdin.resume();if(turn.signal.aborted)lost();
    });
    if(action==='RELEASE'){
     const read=name=>JSON.parse(fs.readFileSync(path.join(out,name),'utf8'));
     const summary=await validatePair({root,result:read('NATIVE_RESULT.json'),receipt:read('INPUT_RECEIPT.json'),qualificationInput:read('QUALIFICATION_INPUT.json'),qualificationResult:read('QUALIFICATION_RESULT.json')});
     fs.writeFileSync(path.join(out,'SUMMARY.json'),JSON.stringify(summary,null,2)+'\n');completion=summary.status;
    }
    return {status:completion,caseId,holderOnlyDirectConnection:true,McpRouteRequiredForB:true,backgroundPaused:false};
   }finally{
    await turn.turnConnection.query('rollback;');
    const proof=one(await turn.turnConnection.query(`select jsonb_build_object('witnessLocksGone',not exists(select 1 from pg_locks where pid=pg_backend_pid() and locktype='relation' and relation in (${plan.relationNames.map(n=>literal(n)+'::regclass').join(',')})));`));
    requireThat(proof.witnessLocksGone===true,'OWNED_HOLDER_LOCK_RELEASE_UNPROVED');
    fs.writeFileSync(path.join(out,'HOLDER_ROLLBACK.json'),JSON.stringify({caseId,...proof,nativeSummaryStatus:completion},null,2)+'\n');
   }
  }},{openTurn:()=>serialized(new native.OwnedTurnConnection(native.agencyChildEnvironment('healthroster_contact_mcp_holder_only')))});
 fs.writeFileSync(path.join(out,'HOLDER_COMPLETION.json'),JSON.stringify({...result,canonicalSharedTurnHelperCompleted:true},null,2)+'\n');return result;
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 try{const [root,out,caseId,flag]=process.argv.slice(2);requireThat(flag==='--authorised-root-holder','ROOT_HOLDER_AUTHORIZATION_REQUIRED');console.log(JSON.stringify(await holdCase({root,out,caseId,authorisedRootHolder:true})));}
 catch(error){console.error(JSON.stringify({status:'HOLD',code:/^[A-Z0-9_]+$/.test(error.message)?error.message:'HOLDER_CLOSED_FAILURE'}));process.exitCode=1;}
}
