import path from 'node:path';import {fileURLToPath} from 'node:url';
import {withSharedRehearsalTurn} from './healthroster-rehearsal-turn.mjs';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
if(process.argv[2]!=='hold'||!process.argv[3])throw Error('EXPLICIT_SHORT_TRIAL_LABEL_REQUIRED');
const result=await withSharedRehearsalTurn({task:process.argv[3],progressPath:path.join(root,'docs/healthroster-build/PROGRESS.md'),async work({identity,signal}){
 console.log(JSON.stringify({status:'TURN_HELD_FOR_ONE_SHORT_TRIAL',releaseCommand:'RELEASE',pid:process.pid,turnBackendPid:identity.backendPid,databaseOid:identity.databaseOid,physicalDatabase:identity.physicalDatabase}));
 await new Promise((resolve,reject)=>{let buffer='';const lost=()=>{process.stdin.pause();reject(signal.reason);};signal.addEventListener('abort',lost,{once:true});
  const done=()=>{signal.removeEventListener('abort',lost);process.stdin.pause();resolve();};
  process.stdin.setEncoding('utf8');process.stdin.on('data',chunk=>{buffer+=chunk;if(buffer.split(/\r?\n/).includes('RELEASE'))done();});process.stdin.once('end',done);process.once('SIGINT',done);process.once('SIGTERM',done);
  if(signal.aborted)lost();});
 return {status:'SHORT_TRIAL_TURN_RELEASED'};
}});
console.log(JSON.stringify(result));
