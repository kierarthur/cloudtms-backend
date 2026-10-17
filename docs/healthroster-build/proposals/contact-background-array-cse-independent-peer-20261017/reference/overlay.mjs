// OFFLINE ONLY. No source-admission bypass or native child is exported.
import fs from'node:fs';import{registerHooks}from'node:module';import{pathToFileURL}from'node:url';import{here,ROOT,ADAPTER}from'./prepare.mjs';
const selected=new Map(['fixed-metadata-reader.mjs','batch-source.mjs'].map(n=>[pathToFileURL(ROOT+'/'+ADAPTER+'/'+n).href,here+'/candidate/'+ADAPTER+'/'+n]));
registerHooks({resolve(s,c,next){let u;try{u=new URL(s,c.parentURL).href;}catch{}if(selected.has(u))return{url:u,shortCircuit:true};return next(s,c);},load(u,c,next){if(selected.has(u))return{format:'module',source:fs.readFileSync(selected.get(u),'utf8'),shortCircuit:true};return next(u,c);}});
