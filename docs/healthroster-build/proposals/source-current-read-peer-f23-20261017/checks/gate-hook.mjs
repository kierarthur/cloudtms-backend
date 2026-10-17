import fs from 'node:fs';
import path from 'node:path';
import {registerHooks} from 'node:module';
import {fileURLToPath,pathToFileURL} from 'node:url';
const here=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..'),repo=path.resolve(here,'../../../..');
const root=path.resolve(repo,'../healthroster-install2-14102026');
const gate=pathToFileURL(path.join(root,'scripts/bpay-next-source-gates.mjs')).href+'?source-read-peer';
const helper=pathToFileURL(path.join(root,'scripts/healthroster-source-read-type-review.mjs')).href;
registerHooks({resolve(specifier,context,next){
 const resolved=specifier.startsWith('.')?new URL(specifier,context.parentURL).href:specifier;
 if(resolved===helper)return{url:resolved,shortCircuit:true};return next(specifier,context);
},load(url,context,next){
 if(url===helper)return{format:'module',source:fs.readFileSync(path.join(here,'payload/scripts/healthroster-source-read-type-review.mjs'),'utf8'),shortCircuit:true};
 if(url===gate){
  const source=fs.readFileSync(path.join(here,'payload/scripts/bpay-next-source-gates.mjs'),'utf8');
  const line=source.split('\n').find(s=>s.includes('} else assertSpecialConstructs(source,file,reviewedSourceReadHazards('));
  const expression=line.slice(line.indexOf('assertSpecialConstructs('),line.indexOf(';assertFrozenAuthority('));
  return{format:'module',source:source+`\nexport function peerF23(source,file,baselineHazards){
   const projectionFile='',earlyFenceFile='',dr15OldFile='';
   const projectionHazards=baselineHazards,earlyFenceHazards=baselineHazards,dr15OldHazards=baselineHazards;
   ${expression};}\n`,shortCircuit:true};
 }
 return next(url,context);
}});
