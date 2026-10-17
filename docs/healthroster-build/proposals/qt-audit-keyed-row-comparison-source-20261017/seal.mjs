// Run once by source author after tests. Writes only this ignored packet's final public metadata.
import fs from 'node:fs';import cp from 'node:child_process';import path from 'node:path';import {fileURLToPath} from 'node:url';import {createHash} from 'node:crypto';
const here=path.dirname(fileURLToPath(import.meta.url)),sha=b=>createHash('sha256').update(b).digest('hex'),f=JSON.parse(fs.readFileSync(here+'/FORWARD.json'));
if(fs.existsSync(here+'/MANIFEST.json'))throw Error('FROZEN_PACKET');
const git=(...a)=>cp.execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',f.root,...a],{windowsHide:true,maxBuffer:4e6,stdio:['ignore','pipe','ignore']}),head=git('rev-parse','HEAD').toString().trim();git('merge-base','--is-ancestor',f.parentCommit,head);
for(const r of f.all52ParentImages){const b=fs.readFileSync(f.root+'/'+r.target);if(b.length!==r.bytes||sha(b)!==r.sha256||![head,f.parentCommit,''].every(ref=>b.equals(git('show',ref+':'+r.target))))throw Error('ALL52_PARENT_CHANGED');}
const tests=cp.spawnSync(process.execPath,['--test',here+'/test.mjs'],{encoding:'utf8',windowsHide:true});if(tests.status!==0)throw Error(tests.stdout+tests.stderr);fs.writeFileSync(here+'/TEST_RESULTS.txt',tests.stdout,{flag:'wx'});
const pg=cp.spawnSync('python',[here+'/pg17-proof.py'],{encoding:'utf8',windowsHide:true});if(pg.status!==0)throw Error(pg.stdout+pg.stderr);fs.writeFileSync(here+'/PG17_AST_PROOF.json',JSON.stringify(JSON.parse(pg.stdout),null,2)+'\n',{flag:'wx'});
if(git('rev-parse','HEAD').toString().trim()!==head)throw Error('ROOT_HEAD_MOVED');
fs.writeFileSync(here+'/RESULTS.json',JSON.stringify({version:'QT_KEYED_AUDIT_SOURCE_RESULTS_V1',status:'PASS_BOUNDED_SOURCE_PREPARATION_ONLY',tests:32,pg17Ast:true,fullCodeInverses:8,fullTargetImages:6,allParentImages:52,unselectedUnchanged:46,originalParent:f.parentCommit,finalSourceVerificationHead:head,sqlExecuted:false,native:'NOT_RUN',ctasProviderAbi:'UNOBSERVED',performance:'NOT_PROVED',wholeWorkBudgetFit:'NOT_PROVED',noActiveSourceWrites:true},null,2)+'\n',{flag:'wx'});
const list=(dir,relative='')=>fs.readdirSync(dir,{withFileTypes:true}).flatMap(d=>d.isDirectory()?list(dir+'/'+d.name,relative+d.name+'/'):[relative+d.name]);
const files=list(here).filter(x=>x!=='MANIFEST.json').sort().map(file=>{const b=fs.readFileSync(here+'/'+file);return{file,bytes:b.length,sha256:sha(b)};});
fs.writeFileSync(here+'/MANIFEST.json',JSON.stringify({version:'QT_KEYED_FULL_AUDIT_BOUND_SOURCE_PACKET_V1',files},null,2)+'\n',{flag:'wx'});
console.log(JSON.stringify({packet:here,files:files.length+1,manifestSha256:sha(fs.readFileSync(here+'/MANIFEST.json')),selectionSha256:sha(fs.readFileSync(here+'/FORWARD.json')),currentRoot:head}));
