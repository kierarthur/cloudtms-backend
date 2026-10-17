// Offline source postimage only; actual root fs/index/HEAD are never replaced.
import fs from'node:fs';import{registerHooks}from'node:module';import{pathToFileURL}from'node:url';import{here,ROOT,ADAPTER}from'./prepare.mjs';
const selected=pathToFileURL(ROOT+'/'+ADAPTER+'/source.mjs').href;
registerHooks({resolve(specifier,context,next){let u;try{u=new URL(specifier,context.parentURL).href;}catch{}if(u===selected)return{url:u,shortCircuit:true};return next(specifier,context);},load(url,context,next){if(url===selected)return{format:'module',source:fs.readFileSync(here+'/candidate/'+ADAPTER+'/source.mjs','utf8'),shortCircuit:true};return next(url,context);}});
