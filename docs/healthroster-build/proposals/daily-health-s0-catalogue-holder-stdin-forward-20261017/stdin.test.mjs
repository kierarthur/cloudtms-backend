// Explicitly authorised real child/pipe regression. No EOF, operator, browser, IPC or native calls.
import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import {spawn} from 'node:child_process';import {PassThrough} from 'node:stream';
const read=f=>fs.readFileSync(new URL(f,import.meta.url),'utf8');
function controlClass(file){const code=read(file),start=code.indexOf('export class Controls{'),end=code.indexOf('\nconst stable=',start);assert(start>=0&&end>start);return code.slice(start,end).replace(/^export /,'');}
async function actualPipe(file,command,{expectLingering=false}={}){
 const script=controlClass(file)+`\nconst abort=new AbortController(),controls=new Controls(process.stdin,abort.signal);try{const ready=controls.wait('RELEASE');console.log('READY');await ready;}catch{process.exitCode=3;}finally{controls.dispose();console.log('DISPOSED');}\n`;
 const env={};for(const key of ['SystemRoot','WINDIR'])if(process.env[key])env[key]=process.env[key];
 const child=spawn(process.execPath,['--input-type=module','-e',script],{cwd:process.cwd(),env,windowsHide:true,stdio:['pipe','pipe','ignore']});
 let closed=false,code,signal,kills=0,ended=false,buffer='';const lines=[],waiters=new Map();
 child.stdin.on('error',()=>{});const exit=new Promise((resolve,reject)=>{child.once('error',reject);child.once('close',(c,s)=>{closed=true;code=c;signal=s;resolve();});});
 child.stdout.on('data',b=>{buffer+=b.toString();let i;while((i=buffer.indexOf('\n'))>=0){const line=buffer.slice(0,i).trim();buffer=buffer.slice(i+1);lines.push(line);waiters.get(line)?.();}});
 const line=w=>lines.includes(w)?Promise.resolve():new Promise(r=>waiters.set(w,r));
 const timeout=async(p,ms)=>{let t;try{return await Promise.race([p,new Promise((_,reject)=>t=setTimeout(()=>reject(Error('REAL_CHILD_BOUND')),ms))]);}finally{clearTimeout(t);}};
 try{
  await timeout(line('READY'),3000);child.stdin.write(command+'\n');await timeout(line('DISPOSED'),3000);
  assert.equal(child.stdin.writableEnded,false); // The parent never sends EOF, including after disposal.
  if(expectLingering){await new Promise(r=>setTimeout(r,350));assert.equal(closed,false,'original must reproduce a live input handle after disposal');}
  else{await timeout(exit,3000);assert.equal(signal,null);assert.equal(code,command==='RELEASE'?0:3);assert.equal(kills,0);}
  return {closed,code,signal,kills,parentSentEOF:ended,disposed:true};
 }finally{if(!closed){kills++;child.kill('SIGTERM');await timeout(exit,3000);}assert.equal(ended,false);}
}
test('complete protocol and manifest reverse exactly, retaining all16 other manifest rows',()=>{const forward=JSON.parse(read('./FULL_PARENT_FORWARD.json')),a=read('./reference/protocol.mjs'),b=read('./payload/protocol.mjs');assert.equal(b.split(forward.protocol.after).length,2);assert.equal(b.replace(forward.protocol.after,forward.protocol.before),a);const old=JSON.parse(read('./reference/MANIFEST.json')),next=JSON.parse(read('./payload/MANIFEST.json'));assert.equal(next.files.length,old.files.length);assert.equal(old.files.length,17);next.files=next.files.map(r=>r.file==='protocol.mjs'?forward.manifest.beforeRow:r);assert.equal(JSON.stringify(next,null,2)+'\n',read('./reference/MANIFEST.json'));});
test('real original class reproduces lingering child with pipe deliberately kept open',async()=>{const r=await actualPipe('./reference/protocol.mjs','RELEASE',{expectLingering:true});assert.equal(r.closed,false);assert.equal(r.parentSentEOF,false);});
test('real corrected class exits normally without EOF or forced kill after RELEASE',async()=>{const r=await actualPipe('./payload/protocol.mjs','RELEASE');assert.equal(r.closed,true);assert.equal(r.kills,0);assert.equal(r.parentSentEOF,false);});
test('real corrected failure disposal exits normally with closed nonzero status, without EOF or forced kill',async()=>{const r=await actualPipe('./payload/protocol.mjs','ABORT');assert.equal(r.closed,true);assert.equal(r.code,3);assert.equal(r.kills,0);});
test('corrected disposal pauses only its own input and removes the same listeners',async()=>{const Controls=Function(controlClass('./payload/protocol.mjs')+';return Controls;')(),a=new AbortController(),input=new PassThrough(),other=new PassThrough();other.resume();const c=new Controls(input,a.signal);assert.equal(input.readableFlowing,true);c.dispose();assert.equal(input.readableFlowing,false);assert.equal(input.listenerCount('data'),0);assert.equal(other.readableFlowing,true);other.pause();});
