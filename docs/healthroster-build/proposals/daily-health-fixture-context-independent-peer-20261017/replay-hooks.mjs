// Frozen-input replay only; protect all captured evidence and shared worktrees.
import fs from'node:fs';import path from'node:path';import{registerHooks,syncBuiltinESMExports}from'node:module';import{fileURLToPath}from'node:url';import{execFileSync}from'node:child_process';import{here,repo,get,sha}from'./capture.mjs';
const mode=process.env.DAILY_PEER_MODE;if(!['recipe','context','current'].includes(mode))throw Error('FINITE_REPLAY_MODE_REQUIRED');
const commit=mode==='recipe'?'fc302f15103102a87903bada1b61ad68dfd0cc05':mode==='current'?'3cbc4585be761479de4854ae1caa621a0186ad8e':'dd338cecd355a077192a236770db26e15559180a';
const root=path.resolve(repo,'../healthroster-install2-14102026'),reference=path.join(here,'reference',mode==='current'?'context':mode),out=path.join(here,'replay',mode),read=fs.readFileSync,write=fs.writeFileSync;
fs.mkdirSync(out,{recursive:true});
const cache=new Map();
// The actual loader hashes the whole SQL inventory. Batch exact Git objects;
// these bodies remain opaque input bytes, not a Banking semantic review.
const tracked=execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','ls-tree','-r','-z','--name-only',commit,'--','supabase'],{cwd:repo,maxBuffer:8e6,windowsHide:true}).toString().split('\0').filter(p=>/[.](?:sql(?:inc)?|inc)$/i.test(p));
const batch=execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','cat-file','--batch'],{cwd:repo,input:tracked.map(p=>commit+':'+p+'\n').join(''),maxBuffer:256e6,windowsHide:true});
let offset=0;for(const p of tracked){const end=batch.indexOf(10,offset),m=batch.subarray(offset,end).toString().match(/^[0-9a-f]+ blob (\d+)$/);if(!m)throw Error('EXACT_BATCH_HEADER_REQUIRED');const bytes=Number(m[1]);cache.set(p,Buffer.from(batch.subarray(end+1,end+1+bytes)));offset=end+1+bytes+1;}if(offset!==batch.length)throw Error('EXACT_BATCH_LENGTH_REQUIRED');
const blob=p=>{if(!cache.has(p))cache.set(p,get(commit,p));return cache.get(p);};
const asText=(b,o)=>typeof o==='string'||o?.encoding?b.toString(typeof o==='string'?o:o.encoding):Buffer.from(b);
const relRoot=p=>{const r=path.relative(root,path.resolve(String(p))).replaceAll('\\','/');return !r.startsWith('../')&&!path.isAbsolute(r)?r:null;};
const modulePins=[];registerHooks({load(url,context,next){if(url.startsWith('file:')){
 const file=fileURLToPath(url),r=relRoot(file);if(r&&(r==='broker/src/daily-canonical-authority.js'||r.startsWith('scripts/'))){const b=blob(r);modulePins.push({path:r,sha256:sha(b),commit});return{format:'module',source:b.toString(),shortCircuit:true};}
 }return next(url,context);}});
fs.readFileSync=function(file,options){const fileAbsolute=path.resolve(String(file));if(mode==='current')for(const p of['docs/bpay-next/final-definer-closures.json','supabase/release/current-release.json'])if(fileAbsolute===path.join(reference,'payload',p))return read(path.join(here,'current-metadata-review/candidate',p),options);
 const r=relRoot(file);if(r){if(cache.has(r))return asText(cache.get(r),options);try{return asText(blob(r),options);}catch(error){
 // Actual lib.inventory() discovers current extra roots before its explicit
 // tracked-DD338 filter. These bytes are hashed only, never selected/qualified.
 if(['context','current'].includes(mode)&&/^supabase\/(repeatable|migrations)\//.test(r))return read(file,options);
 throw error;
 }}return read(file,options);};
fs.writeFileSync=function(file,value,...args){const p=path.resolve(String(file)),r=path.relative(reference,p).replaceAll('\\','/');
 if(!r.startsWith('../')&&!path.isAbsolute(r)){
  if(!['LOADER_RESULTS.json'].includes(r))throw Error('NO_FROZEN_REFERENCE_WRITE '+r);
  return write(path.join(out,r),value,...args);
 }
 if(!p.startsWith(here+path.sep))throw Error('NO_SHARED_WORKTREE_WRITE');return write(file,value,...args);
};syncBuiltinESMExports();
process.on('exit',()=>write(path.join(out,'MODULE_PINS.json'),JSON.stringify({mode,commit,modules:modulePins,native:'NOT_RUN',rootWrites:0,referenceWrites:0},null,2)+'\n'));
