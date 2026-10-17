import fs from'node:fs';import path from'node:path';import assert from'node:assert/strict';import{spawnSync}from'node:child_process';import{pathToFileURL}from'node:url';import{here,repo,sha,write}from'./capture.mjs';
const results=[];
for(const mode of['recipe','context']){
 const out=path.join(here,'replay',mode);fs.mkdirSync(out,{recursive:true});
 const args=['--import',pathToFileURL(path.join(here,'replay-hooks.mjs')).href,'--test','--test-reporter=tap',path.join(here,'reference',mode,'test.mjs')];
 const r=spawnSync(process.execPath,args,{cwd:repo,env:{...process.env,DAILY_PEER_MODE:mode},encoding:'utf8',maxBuffer:3e6,windowsHide:true});
 write('replay/'+mode+'/tests.tap',r.stdout);write('replay/'+mode+'/tests.stderr.txt',r.stderr);assert.equal(r.status,0,r.stderr+'\n'+r.stdout);
 const expected=mode==='recipe'?6:9;assert.ok(r.stdout.includes('# pass '+expected));results.push({mode,tests:expected,pass:expected,fail:0});
}
const loader=spawnSync(process.execPath,['--import',pathToFileURL(path.join(here,'replay-hooks.mjs')).href,path.join(here,'reference/context/loader-check.mjs')],{cwd:repo,env:{...process.env,DAILY_PEER_MODE:'context'},encoding:'utf8',maxBuffer:4e6,windowsHide:true});
write('replay/context/loader.stdout.txt',loader.stdout);write('replay/context/loader.stderr.txt',loader.stderr);assert.equal(loader.status,0,loader.stderr+'\n'+loader.stdout);
const grammar=fs.readFileSync(path.join(here,'reference/context/grammar.py'),'utf8');
const extended=p=>'//?/'+p.replaceAll('\\','/');
const before='HERE=Path(__file__).resolve().parent',after='HERE=Path('+JSON.stringify(extended(path.join(here,'reference/context')))+')';assert.equal(grammar.split(before).length,2);
const outputBefore="(HERE/'GRAMMAR_RESULTS.json').write_text",outputAfter='(Path('+JSON.stringify(extended(path.join(here,'replay/context/GRAMMAR_RESULTS.json')))+')).write_text';assert.equal(grammar.split(outputBefore).length,2);
const adapted=grammar.replace(before,()=>after).replace(outputBefore,()=>outputAfter);write('replay/context/grammar.py',adapted);
const pg=spawnSync('python',[path.join(here,'replay/context/grammar.py')],{cwd:repo,encoding:'utf8',maxBuffer:2e6,windowsHide:true});assert.equal(pg.status,0,pg.stderr);write('replay/context/grammar.stdout.txt',pg.stdout);
const capture=JSON.parse(fs.readFileSync(path.join(here,'CAPTURE.json')));for(const s of capture.selections)for(const f of s.files)assert.equal(sha(fs.readFileSync(path.join(here,'reference',s.name,f.file))),f.sha256);
write('REPLAY_RESULTS.json',{tests:results,actualLoader:true,grammar:true,harness:{GitPinnedReadInputs:true,moduleHooksFiniteBrokerAndScripts:true,referenceWrites:0,rootWrites:0,grammarOperationalEdits:[{before,after},{before:outputBefore,after:outputAfter}]},native:'NOT_RUN'});console.log(JSON.stringify({tests:15,pass:15,loader:'PASS',grammar:'PASS',referenceFiles:33,native:'NOT_RUN'}));
