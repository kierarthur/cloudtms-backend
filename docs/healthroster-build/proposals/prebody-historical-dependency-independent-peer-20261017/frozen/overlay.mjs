// Offline unmounted-postimage import selection. No source-admission or executor.
import fs from'node:fs';import{registerHooks}from'node:module';import{pathToFileURL,fileURLToPath}from'node:url';import path from'node:path';
const here=path.dirname(fileURLToPath(import.meta.url)),ROOT='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026',names=['healthroster-prebody-dynamic-review.mjs','healthroster-same-separate-type-review.mjs','healthroster-source-read-type-review.mjs'];
const modules=new Map(names.map(f=>[pathToFileURL(ROOT+'/scripts/'+f).href,here+'/payload/scripts/'+f]));
registerHooks({resolve(s,c,next){let u;try{u=new URL(s,c.parentURL).href;}catch{}if(modules.has(u))return{url:u,shortCircuit:true};return next(s,c);},load(u,c,next){if(modules.has(u))return{format:'module',source:fs.readFileSync(modules.get(u),'utf8'),shortCircuit:true};return next(u,c);}});
