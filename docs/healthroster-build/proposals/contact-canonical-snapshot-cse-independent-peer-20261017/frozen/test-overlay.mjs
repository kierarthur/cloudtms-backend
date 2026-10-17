// Offline test-only exact two-module source overlay. Original URLs and all other
// actual repository reads remain genuine. This is not native source admission.
import fs from 'node:fs';import path from 'node:path';import {registerHooks,syncBuiltinESMExports} from 'node:module';import {fileURLToPath,pathToFileURL} from 'node:url';import cp from 'node:child_process';import net from 'node:net';import tls from 'node:tls';import assert from 'node:assert/strict';
const here=path.dirname(fileURLToPath(import.meta.url)),root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
const selected=new Map(['healthroster-live-test-restoration.mjs','healthroster-install1-full-rehearsal.mjs'].map(n=>[pathToFileURL(root+'/scripts/'+n).href,here+'/candidate/scripts/'+n])),loaded=new Set();
registerHooks({load(url,context,next){if(selected.has(url)){loaded.add(url);return{format:'module',source:fs.readFileSync(selected.get(url),'utf8'),shortCircuit:true};}return next(url,context);}});
const refuse=()=>{throw Error('SOURCE_ONLY_EXTERNAL_EXECUTION_REFUSED');},spawnSync=cp.spawnSync;for(const k of ['spawn','spawnSync','exec','execSync','execFile','execFileSync','fork'])cp[k]=refuse;
// Actual release inventory uses this one read-only command; no fake inventory.
cp.spawnSync=(file,args,options)=>{assert.equal(file,'git');assert.deepEqual(args.slice(0,4),['ls-files','-z','--',args[3]]);assert.equal(args.length,4);assert(['supabase/migrations','supabase/repeatable'].includes(args[3]));assert.equal(path.resolve(options.cwd),path.resolve(root));return spawnSync(file,args,{...options,env:{...process.env,GIT_OPTIONAL_LOCKS:'0',GIT_NO_LAZY_FETCH:'1'}});};
net.connect=refuse;net.createConnection=refuse;tls.connect=refuse;syncBuiltinESMExports();
process.on('beforeExit',()=>assert.equal(loaded.size,2,'BOTH_ACTUAL_POSTIMAGES_MUST_BE_TESTED'));
