// Effect-free actual mounted source checks. No SQL/native/release calls.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
import {execFileSync,spawnSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
import {digest,canonical,readManagedClosure,exactAtomicBody,scanManagedSql} from '../../../scripts/cloudtms-managed-release-sql.mjs';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'../../..'),base='docs/healthroster-build/install2-composition/';
const receipt=JSON.parse(fs.readFileSync(path.join(root,base+'NO_SHIFTS_UNDO_MOUNT_14102026.json')));
const pins=[...receipt.archivePins,...receipt.owners];
for(const p of pins){const raw=fs.readFileSync(path.join(root,p.file));assert.equal(digest(raw),p.sha256);assert.deepEqual(raw,execFileSync('git',['show',':'+p.file],{cwd:root,maxBuffer:16000000}));}
const native=base+'reviewed-no-shifts-undo-14102026/native',state=base+'reviewed-no-shifts-undo-14102026/state';
const original=fs.readFileSync(path.join(root,native,'undo-current-transport.test.mjs'),'utf8');
const substitutions=[
 ["from './test-mount/broker/src/weekly-source/routes.js'","from '../../../broker/src/weekly-source/routes.js'",1],
 ["const dir=path.dirname(fileURLToPath(import.meta.url));",`const actualRoot=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'../../..');\nconst dir=path.join(actualRoot,${JSON.stringify(native)});`,1],
 ["path.resolve(dir,'../../../../../healthroster-install2-14102026')","actualRoot",1],
 ["path.join(dir,'forward',pin.new_native_owners[0].path)",`path.join(actualRoot,${JSON.stringify(receipt.owners[2].file)})`,2],
 ["path.join(dir,'forward',pin.new_native_owners[1].path)",`path.join(actualRoot,${JSON.stringify(receipt.owners[1].file)})`,1],
 ['14102026_0900_healthroster_no_shifts_undo','14102026_0953_healthroster_no_shifts_undo',1]
];
let testSource=canonical(original);for(const [before,after,count]of substitutions){assert.equal(testSource.split(before).length-1,count,'REBINDING_COUNT '+before);testSource=testSource.replaceAll(before,after);}
const testFile=base+'no-shifts-undo-mounted-transport.test.mjs';fs.writeFileSync(path.join(root,testFile),testSource);
const leaf=fs.readFileSync(path.join(root,receipt.owners[2].file),'utf8'),reverse=JSON.parse(fs.readFileSync(path.join(root,state,'CURRENT_STATE_OWNER_REVERSE_AND_PINS.json'))).changed_leaf;
assert.equal(digest(leaf),reverse.after_sha256);assert.equal(digest(leaf.slice(0,reverse.reverse_insert_offset)),reverse.reverse_prefix_sha256);
assert.equal(digest(leaf.slice(reverse.reverse_insert_offset)),reverse.reverse_suffix_sha256);
assert.equal(digest(leaf.slice(0,reverse.reverse_insert_offset)+reverse.hunk.before+leaf.slice(reverse.reverse_insert_offset)),reverse.before_sha256);
const closure=readManagedClosure(receipt.owners[1].file,root),body=exactAtomicBody(closure.expanded);assert.equal(closure.files.size,2);assert.equal(scanManagedSql(body).statements.length,5);
const allowed=new Set(['PATH','SYSTEMROOT','WINDIR','TEMP','TMP','PATHEXT','COMSPEC','USERPROFILE','APPDATA','LOCALAPPDATA']);
const env=Object.fromEntries(Object.entries(process.env).filter(([k])=>allowed.has(k.toUpperCase())));
const jobs=[['mounted-dispatcher38',process.execPath,['--test','--test-reporter=tap',testFile]],
 ['route-syntax',process.execPath,['--check','broker/src/weekly-source/routes.js']],
 ['recipe-syntax',process.execPath,['--check',base+'mount-no-shifts-undo-reviewed.mjs']],
 ['check-syntax',process.execPath,['--check',base+'check-no-shifts-undo-mounted.mjs']],
 ['database-source-integrity',process.execPath,['scripts/cloudtms-db-release.mjs','check']],
 ['tracked-credential-scan',process.execPath,['scripts/verify-source-secret-safety.mjs']]];
const steps=[];for(const [name,command,args]of jobs){const r=spawnSync(command,args,{cwd:root,env,windowsHide:true,encoding:'utf8',maxBuffer:8000000});const out=(r.stdout??'')+(r.stderr??'');
 const step={name,exitCode:r.status,outputSha256:digest(out),tests:Number(out.match(/# tests (\d+)/)?.[1]??0),pass:Number(out.match(/# pass (\d+)/)?.[1]??0),fail:Number(out.match(/# fail (\d+)/)?.[1]??0)};steps.push(step);console.log(JSON.stringify(step));assert.equal(r.status,0,name);}
const result={status:'MOUNTED_SOURCE_PASS_NATIVE_AND_FINAL_COUPLING_HELD',sourcePins:pins.length,archiveFiles:receipt.archivePins.length,
 closureFiles:closure.files.size,closureSha256:closure.closureHash,atomicStatements:scanManagedSql(body).statements.length,
 fullRouteReverseHunks:2,fullStateReverseHunks:1,reboundTestSha256:digest(testSource),rebindings:substitutions.map(([before,after,count])=>({before,after,count})),steps,
 contractSeal:null,native:false,installation:false,initialReaderHold:'Manifest artifact count includes its own manifest; first count assertion held before writes, then exact entries plus manifest were checked.'};
fs.writeFileSync(path.join(root,base+'NO_SHIFTS_UNDO_SOURCE_CHECKS_14102026.json'),JSON.stringify(result,null,2)+'\n');
console.log(JSON.stringify({status:result.status,pins:pins.length,tests:steps.reduce((n,s)=>n+s.tests,0)}));
