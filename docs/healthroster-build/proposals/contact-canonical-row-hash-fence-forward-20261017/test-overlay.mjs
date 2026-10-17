// OFFLINE ONLY: two exact selected module bodies at their genuine module URLs.
// This does not prove source() admission of the proposed, unmounted postimage.
import fs from'node:fs';import path from'node:path';import{registerHooks,syncBuiltinESMExports}from'node:module';import{fileURLToPath,pathToFileURL}from'node:url';import cp from'node:child_process';import net from'node:net';import tls from'node:tls';import assert from'node:assert/strict';
const here=path.dirname(fileURLToPath(import.meta.url)),root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026',names=['scripts/healthroster-install1-full-rehearsal.mjs','scripts/healthroster-live-test-restoration.mjs'],selected=new Map(names.map(n=>[pathToFileURL(root+'/'+n).href,here+'/candidate/'+n])),loaded=new Set();
registerHooks({load(url,context,next){if(selected.has(url)){loaded.add(url);return{format:'module',source:fs.readFileSync(selected.get(url),'utf8'),shortCircuit:true};}return next(url,context);}});
const refuse=()=>{throw Error('SOURCE_ONLY_EXTERNAL_EXECUTION_REFUSED');},spawnSync=cp.spawnSync,execFileSync=cp.execFileSync;
for(const k of ['spawn','spawnSync','exec','execSync','execFile','execFileSync','fork'])cp[k]=refuse;
// The two original helper tests need only real tracked inventory Git reads.
cp.spawnSync=(file,args,options)=>{assert.equal(file,'git');assert.equal(args.length,4);assert.deepEqual(args.slice(0,3),['ls-files','-z','--']);assert(['supabase/migrations','supabase/repeatable'].includes(args[3]));assert.equal(path.resolve(options.cwd),path.resolve(root));return spawnSync(file,['-c','gc.auto=0','-c','maintenance.auto=false',...args],{...options,env:{...process.env,GIT_OPTIONAL_LOCKS:'0',GIT_NO_LAZY_FETCH:'1'}});};
// The unchanged Contact protocol test reads only real, finite source Git images.
cp.execFileSync=(file,args,options)=>{assert.equal(file,'git');assert.deepEqual(args.slice(0,6),['-c','gc.auto=0','-c','maintenance.auto=false','-C',root]);const op=args.slice(6);assert(['rev-parse','show','diff','merge-base'].includes(op[0]));if(op[0]==='diff')assert(op.includes('--quiet'));if(op[0]==='merge-base')assert.equal(op[1],'--is-ancestor');return execFileSync(file,args,options);};
net.connect=refuse;net.createConnection=refuse;tls.connect=refuse;syncBuiltinESMExports();
process.on('beforeExit',()=>assert.equal(loaded.size,2,'BOTH_EXACT_SHARED_MODULE_IMAGES_REQUIRED'));
