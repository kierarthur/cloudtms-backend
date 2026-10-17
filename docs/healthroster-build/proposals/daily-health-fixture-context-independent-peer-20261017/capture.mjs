import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {execFileSync}from'node:child_process';import{createHash}from'node:crypto';import{fileURLToPath}from'node:url';
export const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..'),sha=x=>createHash('sha256').update(x).digest('hex');
export const get=(commit,file)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','show',commit+':'+file],{cwd:repo,maxBuffer:24e6,windowsHide:true});
export const write=(file,value)=>{const p=path.join(here,file);fs.mkdirSync(path.dirname(p),{recursive:true});fs.writeFileSync(p,Buffer.isBuffer(value)||typeof value==='string'?value:JSON.stringify(value,null,2)+'\n');};
export const selections=[{name:'recipe',commit:'ad02698589fa28b1a73af26390fad7cf4c0466f9',folder:'daily-health-genuine-fixture-recipe-20261017'},{name:'context',commit:'1d00a57a1cf292f2a0c6cf8c17898d1e13204d31',folder:'daily-health-role-identity-forward-20261017'}];
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 const out=[];for(const s of selections){const prefix='docs/healthroster-build/proposals/'+s.folder+'/',raw=get(s.commit,prefix+'MANIFEST.json'),m=JSON.parse(raw),rows=[];
 for(const r of [...m.files,{file:'MANIFEST.json',sha256:sha(raw),bytes:raw.length}]){const b=get(s.commit,prefix+r.file);assert.equal(sha(b),r.sha256);assert.equal(b.length,r.bytes);write('reference/'+s.name+'/'+r.file,b);rows.push({file:r.file,sha256:sha(b),bytes:b.length});}
 out.push({...s,manifestSha256:sha(raw),files:rows});}
 write('CAPTURE.json',{selections:out,sourceOnly:true,dbConnections:0,native:'NOT_RUN'});console.log(JSON.stringify(out.map(s=>({name:s.name,files:s.files.length,manifestSha256:s.manifestSha256}))));
}
