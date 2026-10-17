// Read-only whole-parent reproduction unless the author explicitly requests --freeze.
import fs from 'node:fs';import crypto from 'node:crypto';
const here=new URL('./',import.meta.url),hash=b=>crypto.createHash('sha256').update(b).digest('hex'),read=f=>fs.readFileSync(new URL(f,here));
const original=read('reference/run-staged-parent.mjs');let derived=original.toString();const edits=[];
function edit(before,after){if(derived.split(before).length!==2)throw Error('S0_STAGE_FINITE_PARENT_EDIT_REQUIRED');edits.push({before,after});derived=derived.replace(before,after);}
const parentPath="'./",adaptedPath="'../daily-health-genuine-staged-operator-20261017/";
for(const name of ['contract.mjs','source.mjs'])edit('from '+parentPath+name+"'",'from '+adaptedPath+name+"'");
edit("import('./contract.mjs')","import('../daily-health-genuine-staged-operator-20261017/contract.mjs')");
edit("import {childEnvironment,aclChildEnvironment} from '../test-auth-refresh-suppressed-startup-forward-20261017/run-auth-refresh.mjs';","import {childEnvironment,aclChildEnvironment} from '../test-auth-refresh-suppressed-startup-forward-20261017/run-auth-refresh.mjs';\nimport {ownedWorker} from './owned-worker.mjs';");
edit('export async function runStaged(flags){','export async function runStaged(flags,{controlEnvironment,signal,assertHeld,runOwnedTurnChild,treeFactory=ownedWorker}){');
const capCheck="for(const key of ['CLOUDTMS_SHARED_TURN_PIPE','CLOUDTMS_SHARED_TURN_CAPABILITY'])check(typeof process.env[key]==='string'&&process.env[key].length>0,'FIXTURE_LIVE_ORIGINAL_TURN_CONTROL_REQUIRED');";
edit(capCheck,capCheck.replaceAll('process.env[key]','controlEnvironment[key]'));
const capCopy="for(const key of ['CLOUDTMS_SHARED_TURN_PIPE','CLOUDTMS_SHARED_TURN_CAPABILITY'])env[key]=process.env[key];";
edit(capCopy,capCopy.replace('process.env[key]','controlEnvironment[key]'));
const begin=derived.indexOf(' const child=spawn('),end=derived.indexOf(' const file=out+',begin);if(begin<0||end<0)throw Error('S0_STAGE_CHILD_PARENT_BOUNDARY_REQUIRED');
edit(derived.slice(begin,end),` const tree=treeFactory();let execution;
 try{execution=await tree.wait(runOwnedTurnChild({file:process.execPath,args:[here+'/worker.mjs',JSON.stringify({...options,resultName})],options:{cwd:out,env,stdio:['ignore','ignore','ignore','ipc']},signal,assertHeld,spawnChild:tree.spawn,resourceGuard:tree}),signal);}
 finally{await tree.confirm();}
 check(execution.signal===null,'S0_STAGE_CHILD_SIGNAL_HELD');const exit=execution.code;
`);
const cli=derived.indexOf('\nif(process.argv[1]');if(cli<0)throw Error('S0_STAGE_PARENT_CLI_BOUNDARY_REQUIRED');edit(derived.slice(cli),'\n// No standalone CLI: root S0 coordinator supplies the authenticated context.\n');
let reverse=derived;for(const r of [...edits].reverse()){if(reverse.split(r.after).length!==2)throw Error('S0_STAGE_INVERSE_NOT_UNIQUE');reverse=reverse.replace(r.after,r.before);}if(reverse!==original.toString())throw Error('S0_STAGE_WHOLE_PARENT_INVERSE_FAILED');
const forward={version:'DAILY_S0_STAGE_LAUNCHER_WHOLE_PARENT_FORWARD_V1',parent:'reference/run-staged-parent.mjs',parentSha256:hash(original),parentBytes:original.length,derived:'phase-parent.mjs',derivedSha256:hash(derived),derivedBytes:Buffer.byteLength(derived),edits,wholeRawInverse:true};
if(process.argv.includes('--freeze')){fs.writeFileSync(new URL('phase-parent.mjs',here),derived);fs.writeFileSync(new URL('LAUNCHER_FULL_PARENT_FORWARD.json',here),JSON.stringify(forward,null,2)+'\n');}
else{if(!read('phase-parent.mjs').equals(Buffer.from(derived))||JSON.stringify(JSON.parse(read('LAUNCHER_FULL_PARENT_FORWARD.json')))!==JSON.stringify(forward))throw Error('S0_STAGE_FROZEN_PARENT_FORWARD_CHANGED');console.log(JSON.stringify({status:'PASS',edits:edits.length,parentBytes:original.length,wholeRawInverse:true}));}
