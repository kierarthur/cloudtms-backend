// OFFLINE ONLY module postimages at genuine root module URLs. Real source/Git
// admission stays unchanged and is not claimed for this unmounted proposal.
import fs from'node:fs';import{registerHooks}from'node:module';import{pathToFileURL}from'node:url';import{here,ROOT,ADAPTER}from'./prepare.mjs';
const selected=new Map(['fixed-metadata-reader.mjs','batch-source.mjs','hold-case.mjs'].map(n=>[pathToFileURL(ROOT+'/'+ADAPTER+'/'+n).href,here+'/candidate/'+ADAPTER+'/'+n]));
registerHooks({resolve(specifier,context,next){let url;try{url=new URL(specifier,context.parentURL).href;}catch{}if(selected.has(url))return{url,shortCircuit:true};return next(specifier,context);},load(url,context,next){if(selected.has(url))return{format:'module',source:fs.readFileSync(selected.get(url),'utf8'),shortCircuit:true};return next(url,context);}});
