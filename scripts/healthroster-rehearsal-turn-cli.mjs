import path from 'node:path';import {fileURLToPath} from 'node:url';
import {withSharedRehearsalTurn} from './healthroster-rehearsal-turn.mjs';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
if(process.argv[2]!=='hold'||!process.argv[3])throw Error('EXPLICIT_SHORT_TRIAL_LABEL_REQUIRED');
const result=await withSharedRehearsalTurn({task:process.argv[3],progressPath:path.join(root,'docs/healthroster-build/PROGRESS.md'),async work({turnConnection}){
 const ownPid=await turnConnection.query('select pg_backend_pid();');
 if(ownPid.length!==1||! /^[1-9][0-9]*$/.test(ownPid[0]))throw Error('OWN_TURN_BACKEND_PID_INVALID');
 console.log(JSON.stringify({status:'TURN_HELD_FOR_ONE_SHORT_TRIAL',releaseCommand:'RELEASE',pid:process.pid,turnBackendPid:Number(ownPid[0]),physicalDatabase:'cloudtms_test_clone'}));
 await new Promise(resolve=>{let buffer='';process.stdin.setEncoding('utf8');process.stdin.on('data',chunk=>{buffer+=chunk;if(buffer.split(/\r?\n/).includes('RELEASE')){process.stdin.pause();resolve();}});process.stdin.once('end',resolve);process.once('SIGINT',resolve);process.once('SIGTERM',resolve);});
 return {status:'SHORT_TRIAL_TURN_RELEASED'};
}});
console.log(JSON.stringify(result));
