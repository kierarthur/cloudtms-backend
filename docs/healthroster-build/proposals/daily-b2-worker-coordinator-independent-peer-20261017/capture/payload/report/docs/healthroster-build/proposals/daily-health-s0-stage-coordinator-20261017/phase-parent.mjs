// Root-owned executable proposal. Importing does not start a browser or perform IO to a network.
import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {spawn,execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
import {ROOT,check,sha} from '../daily-health-genuine-staged-operator-20261017/contract.mjs';
import {here,sourcePreflight,ownedDirectory,physical,git} from '../daily-health-genuine-staged-operator-20261017/source.mjs';
import {childEnvironment,aclChildEnvironment} from '../test-auth-refresh-suppressed-startup-forward-20261017/run-auth-refresh.mjs';
import {ownedWorker} from './owned-worker.mjs';
export function argumentsFor(flags){
 check(flags[0]==='--authorised-root-staged-fixture'&&flags[1]==='--outside-other-native-interval','FIXTURE_ROOT_AUTHORITY_REQUIRED');
 const options={};for(let i=2;i<flags.length;i+=2){check(['--phase','--stage','--owned','--effect-file'].includes(flags[i])&&typeof flags[i+1]==='string'&&!options[flags[i]],'FIXTURE_EXACT_ARGUMENTS_REQUIRED');options[flags[i]]=flags[i+1];}
 check(['bind','prepare','entry','submit','qualify','reconcile'].includes(options['--phase'])&&/^(?:S[0-4]|S1a)$/.test(options['--stage']??''),'FIXTURE_PHASE_REQUIRED');
 check((options['--phase']==='bind')===(options['--owned']===undefined),'FIXTURE_EXISTING_SESSION_REQUIRED');
 check(options['--phase']!=='bind'||options['--stage']==='S0','FIXTURE_BIND_S0_ONLY');return options;
}
export async function runStaged(flags,{controlEnvironment,signal,assertHeld,runOwnedTurnChild,treeFactory=ownedWorker,currentNative=null}){
 const options=argumentsFor(flags);sourcePreflight();let out;
 // Only the existing original turn coordinator supplies this authenticated pipe/capability pair.
 for(const key of ['CLOUDTMS_SHARED_TURN_PIPE','CLOUDTMS_SHARED_TURN_CAPABILITY'])check(typeof controlEnvironment[key]==='string'&&controlEnvironment[key].length>0,'FIXTURE_LIVE_ORIGINAL_TURN_CONTROL_REQUIRED');
 const system=path.join(process.env.SystemRoot,'System32'),aclShell=path.join(system,'WindowsPowerShell/v1.0/powershell.exe');
 if(options['--phase']==='bind'){
  out=ownedDirectory(ROOT+'/.codex-tmp/daily-genuine-stages-20261017-'+crypto.randomBytes(16).toString('hex'));
  check(!fs.existsSync(out),'FIXTURE_FRESH_PRIVATE_DIRECTORY_REQUIRED');fs.mkdirSync(out,{mode:0o700});
  try{execFileSync(aclShell,['-NoProfile','-NonInteractive','-File',path.join(here,'secure-owned.ps1'),'-OwnedPath',out],{windowsHide:true,stdio:'ignore',timeout:15000,env:aclChildEnvironment(aclShell)});}
  catch{return {status:'HOLD',code:'FIXTURE_PRIVATE_ACL_SETUP_HELD',ownedDirectory:out,childClosed:true,browserStarted:false};}
  git(ROOT,'check-ignore','--quiet','--',path.relative(ROOT,out+'/PRIVATE_BINDING.json'));
  fs.mkdirSync(out+'/tmp',{mode:0o700});
 }else{out=ownedDirectory(options['--owned']);check(physical(out)===out,'FIXTURE_PRIVATE_PHYSICAL_SESSION_REQUIRED');}
 // ACL validation is repeated on every invocation; it never mutates an existing folder/state.
 execFileSync(aclShell,['-NoProfile','-NonInteractive','-File',path.join(here,'verify-owned.ps1'),'-OwnedPath',out],{windowsHide:true,stdio:'ignore',timeout:15000,env:aclChildEnvironment(aclShell)});
 execFileSync(aclShell,['-NoProfile','-NonInteractive','-File',path.join(here,'verify-owned.ps1'),'-OwnedPath',path.dirname((await import('../daily-health-genuine-staged-operator-20261017/contract.mjs')).PRIVATE_STATE)],{windowsHide:true,stdio:'ignore',timeout:15000,env:aclChildEnvironment(aclShell)});
 const resultName='CLOSED_'+options['--phase']+'_'+options['--stage']+'_'+crypto.randomBytes(8).toString('hex')+'.json';
 const env=childEnvironment(out);delete env.HEALTH_TEST_AUTH_OWNED_OUT;
 for(const key of ['CLOUDTMS_SHARED_TURN_PIPE','CLOUDTMS_SHARED_TURN_CAPABILITY'])env[key]=controlEnvironment[key];
 env.HEALTH_GENUINE_OWNED_OUT=out;
 const tree=treeFactory();let execution;const spawnOwned=(...args)=>{const child=tree.spawn(...args);currentNative?.attach(child);return child;};
 try{execution=await tree.wait(runOwnedTurnChild({file:process.execPath,args:[here+'/worker.mjs',JSON.stringify({...options,resultName})],options:{cwd:out,env,stdio:['ignore','ignore','ignore','ipc']},signal,assertHeld,spawnChild:spawnOwned,resourceGuard:tree}),signal);}
 finally{await tree.confirm();currentNative?.close();}
 check(execution.signal===null,'S0_STAGE_CHILD_SIGNAL_HELD');const exit=execution.code;
 const file=out+'/'+resultName,s=fs.lstatSync(file);check(s.isFile()&&!s.isSymbolicLink()&&s.size<=2048,'FIXTURE_CLOSED_RESULT_REQUIRED');
 const r=JSON.parse(fs.readFileSync(file,'utf8'));
 check(Object.keys(r).sort().join(',')==='browserClosed,code,phase,stage,status'&&['PASS','HOLD'].includes(r.status)&&/^FIXTURE_[A-Z0-9_]+$/.test(r.code)
  &&r.phase===options['--phase']&&r.stage===options['--stage']&&typeof r.browserClosed==='boolean','FIXTURE_CLOSED_RESULT_REQUIRED');
 check(r.status!=='PASS'||exit===0&&r.browserClosed===true,'FIXTURE_PASS_REQUIRES_CONFIRMED_CLOSE_AND_EXIT');
 if(r.status==='PASS')currentNative?.assertCompleted();
  return {...r,ownedDirectory:out,childClosed:true,qualification:'SOURCE_OPERATOR_PHASE_ONLY_NOT_PUBLIC_COMPOSER_OR_FINANCIAL_SUCCESS'};
}
// No standalone CLI: root S0 coordinator supplies the authenticated context.
