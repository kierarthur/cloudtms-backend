import fs from 'node:fs';import path from 'node:path';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';
const here=path.dirname(fileURLToPath(import.meta.url)),root='D:/Development/GitHub/.codex-worktrees/healthroster-checklist-native-14102026',packet='docs/healthroster-build/proposals/daily-health-genuine-hospital-mapping-stage-20261017',commit='bb2acf3eb32f80b5b73f462fbf39e41272acadc4';
const sha=b=>createHash('sha256').update(b).digest('hex'),git=(...a)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,...a],{maxBuffer:32e6});
const raw=fs.readFileSync(root+'/'+packet+'/MANIFEST.json');assert.equal(sha(raw),'1d6bd4efe842421893fede8b8234e68c8cc74c6862315c1caa09edf2a7c348e5');
const manifest=JSON.parse(raw),files=[...manifest.files,{file:'MANIFEST.json',bytes:raw.length,sha256:sha(raw)}];
for(const r of files){const b=fs.readFileSync(root+'/'+packet+'/'+r.file);assert.equal(b.length,r.bytes);assert.equal(sha(b),r.sha256);assert(b.equals(git('show',commit+':'+packet+'/'+r.file)));const file=here+'/reference/mapping/'+r.file;fs.mkdirSync(path.dirname(file),{recursive:true});fs.writeFileSync(file,b,{flag:'wx'});}
fs.writeFileSync(here+'/MAPPING_CAPTURE.json',JSON.stringify({commit,packet,manifestSha256:sha(raw),files,native:'NOT_RUN'},null,2)+'\n',{flag:'wx'});
console.log(JSON.stringify({exactMappingSourceFiles:files.length,native:'NOT_RUN'}));
