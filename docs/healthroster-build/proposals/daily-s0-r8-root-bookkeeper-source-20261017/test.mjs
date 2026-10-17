import fs from 'node:fs';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import test from 'node:test';
const base=new URL('./',import.meta.url),read=f=>fs.readFileSync(new URL(f,base)),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const body=read('bookkeeper.mjs').toString(),parent=read('reference/r7-bookkeeper.mjs'),proof=JSON.parse(read('FULL_PARENT_FORWARD.json'));
const gate=body.slice(body.indexOf('// Genuine public R10'),body.indexOf('const files=[]'));
const writeBlock=body.slice(body.indexOf('const files=[]'),body.indexOf("if(phase==='SETUP')"));
const prefix='docs/healthroster-build/v6-owner-delivery-20261016/',s='docs/healthroster-build/proposals/daily-health-genuine-staged-operator-20261017/',i='ROOT',r='REPORT';
const rootHead='1'.repeat(40),reviewCommit='2'.repeat(40),receiptCommit='3'.repeat(40),source='4'.repeat(40);
const review=prefix+'DAILY_AUTH_R10_SOURCE_RECIPE_ROOT_REVIEW_20261017.json',plan=prefix+'DAILY_GENUINE_S0_R8_ROOT_EXECUTION_PLAN_20261017.json',receipt=prefix+'TEST_AUTH_REFRESH_R10_SUCCESS_20261017.json',pinFile=s+'SOURCE_PINS.json';
function fixture(){
 const rows=new Map([[i+'/'+review,Buffer.from('review')],[i+'/'+plan,Buffer.from('plan')],[i+'/'+receipt,Buffer.from('public actual receipt')]]),images=new Map(),calls=[];
 const obs={file:receipt,commit:receiptCommit,sha256:sha(rows.get(i+'/'+receipt))};
 const setPins=()=>rows.set(r+'/'+pinFile,Buffer.from(JSON.stringify({authObservation:obs})));
 setPins();
 for(const[file,hist]of [[review,reviewCommit],[plan,rootHead],[receipt,receiptCommit]])for(const ref of[rootHead,'',hist])images.set(i+'|'+ref+':'+file,rows.get(i+'/'+file));
 for(const ref of[source,''])images.set(r+'|'+ref+':'+pinFile,rows.get(r+'/'+pinFile));
 const env={HR_R10_REVIEW_COMMIT:reviewCommit},flags={ancestor:true};
 const g=(repo,...a)=>{calls.push([repo,...a]);if(a.join('|')==='rev-parse|HEAD')return Buffer.from(rootHead);if(a[0]==='merge-base'){if(!flags.ancestor)throw Error('NOT_ANCESTOR');return Buffer.alloc(0);}if(a[0]==='show'){const b=images.get(repo+'|'+a[1]);if(!b)throw Error('MISSING_GIT_IMAGE');return b;}throw Error('UNKNOWN_GIT');};
 const run=()=>new Function('fs','g','i','r','p','s','source','sha','process',gate+'return observed;')({readFileSync:f=>{assert(rows.has(f));return rows.get(f);}},g,i,r,prefix,s,source,sha,{env});
 return{rows,images,calls,obs,setPins,env,flags,run};
}
function writers(){
 const before=read('reference/r7-effect-before.json'),after=read('reference/r7-effect-after.json'),rows=new Map([[i+'/'+proof.protectedBefore.file,before],[i+'/'+proof.protectedAfter.file,after]]),images=new Map(),writes=[];
 for(const q of[proof.protectedBefore,proof.protectedAfter])for(const ref of['HEAD','',q.commit])images.set(ref+':'+q.file,rows.get(i+'/'+q.file));
 const fakefs={readFileSync:f=>{assert(rows.has(f));return rows.get(f);},writeFileSync:(f,b,opt)=>{if(opt?.flag==='wx'&&rows.has(f))throw Error('EEXIST');rows.set(f,Buffer.from(b));writes.push({file:f,option:opt});}};
 const g=(_,...a)=>{assert.equal(a[0],'show');const b=images.get(a[1]);assert(b);return b;};
 const api=new Function('fs','g','i','p','sha',writeBlock+'return{write,files};')(fakefs,g,i,prefix,sha);
 return{...api,rows,images,writes};
}
test('all seven edits reconstruct exact complete R7/R8 parents',()=>{let b=parent.toString();for(const c of proof.changes){assert.equal(b.split(c.before).length-1,c.occurrences);b=b.split(c.before).join(c.after);}assert.equal(b,body);for(const c of [...proof.changes].reverse())b=b.split(c.after).join(c.before);assert.equal(b,parent.toString());assert.equal(sha(parent),proof.parentSha256);assert.equal(sha(Buffer.from(body)),proof.afterSha256);});
test('original phase, actor/native scope/freshness, index, append and publish bodies retained',()=>{for(const mark of ["if(phase==='SETUP'){","const {readEnvelope}=","g(i,'add','--',...files)"]){const a=body.slice(body.indexOf(mark)),b=parent.toString().slice(parent.toString().indexOf(mark));let x=a;for(const c of [...proof.changes].reverse())x=x.split(c.after).join(c.before);assert.equal(x,b);}assert(body.includes("if(phase!=='BEFORE'){g(i,'push'"));assert(body.includes("readEnvelope(JSON.parse(beforeBytes),b.receipt,{maxAgeMs:180000})"));});
test('actual public binding accepts saved/index/current and historical equal images',()=>{const f=fixture();assert.deepEqual(f.run(),f.obs);assert(f.calls.some(x=>x[2]===':'+review));assert(f.calls.some(x=>x[2]===':'+receipt));});
test('missing actual R10 review commit refused',()=>{const f=fixture();delete f.env.HR_R10_REVIEW_COMMIT;assert.throws(f.run,/ACTUAL_R10_REVIEW_COMMIT_REQUIRED/);});
test('non-ancestor actual review refused',()=>{const f=fixture();f.flags.ancestor=false;assert.throws(f.run,/NOT_ANCESTOR/);});
for(const[file,ref]of [[review,rootHead],[review,''],[review,reviewCommit],[plan,rootHead],[plan,''],[receipt,rootHead],[receipt,''],[receipt,receiptCommit]])test('public evidence image substitution refused '+file.split('/').at(-1)+' '+(ref||'INDEX'),()=>{const f=fixture();f.images.set(i+'|'+ref+':'+file,Buffer.from('changed'));assert.throws(f.run,/ACTUAL_COMMITTED_R10_PUBLIC_EVIDENCE_REQUIRED/);});
test('saved public evidence drift refused',()=>{const f=fixture();f.rows.set(i+'/'+review,Buffer.from('changed'));assert.throws(f.run,/ACTUAL_COMMITTED_R10_PUBLIC_EVIDENCE_REQUIRED/);});
for(const ref of[source,''])test('operator source-pin image drift refused '+(ref||'INDEX'),()=>{const f=fixture();f.images.set(r+'|'+ref+':'+pinFile,Buffer.from('changed'));assert.throws(f.run,/CURRENT_R10_SOURCE_PINS_REQUIRED/);});
for(const [key,value]of [['file',prefix+'TEST_AUTH_REFRESH_R9_SUCCESS_20261017.json'],['commit','x'],['sha256','x']])test('R10 observation '+key+' must be genuine fixed shape',()=>{const f=fixture();f.obs[key]=value;f.setPins();for(const ref of[source,''])f.images.set(r+'|'+ref+':'+pinFile,f.rows.get(r+'/'+pinFile));assert.throws(f.run,/ACTUAL_R10_SOURCE_OBSERVATION_REQUIRED/);});
test('actual receipt hash mismatch refused',()=>{const f=fixture();f.obs.sha256='0'.repeat(64);f.setPins();for(const ref of[source,''])f.images.set(r+'|'+ref+':'+pinFile,f.rows.get(r+'/'+pinFile));assert.throws(f.run,/ACTUAL_R10_PUBLIC_RECEIPT_HASH_REQUIRED/);});
for(const q of[proof.protectedBefore,proof.protectedAfter]){
 test('exact R7 central descriptor can advance '+q.file.split('/').at(-1),()=>{const w=writers();w.write(q.file,{actual:true});assert.equal(w.writes.length,1);assert.equal(w.writes[0].option,undefined);assert.deepEqual(w.files,[q.file]);});
 for(const ref of['HEAD','',q.commit])test('central descriptor parent drift refused '+q.file.split('/').at(-1)+' '+(ref||'INDEX'),()=>{const w=writers();w.images.set(ref+':'+q.file,Buffer.from('changed'));assert.throws(()=>w.write(q.file,{}),/EXACT_R7_/);assert.equal(w.writes.length,0);});
 test('central saved hash drift refused '+q.file.split('/').at(-1),()=>{const w=writers();w.rows.set(i+'/'+q.file,Buffer.from('changed'));assert.throws(()=>w.write(q.file,{}),/EXACT_R7_/);assert.equal(w.writes.length,0);});
}
test('new R8 proof records remain exclusive wx and refuse overwrite',()=>{const w=writers(),f=prefix+'DAILY_GENUINE_S0_R8_BEFORE_NATIVE_QUALIFICATION_20261017.json';w.write(f,{});assert.equal(w.writes[0].option.flag,'wx');assert.throws(()=>w.write(f,{}),/EEXIST/);});
test('harness never imports helper or calls sourcePreflight/private data/child/native',()=>{assert(!gate.includes('PRIVATE_BINDING'));assert(!gate.includes('storage-state'));assert(!gate.includes('execFile'));assert(!writeBlock.includes('imp('));});

