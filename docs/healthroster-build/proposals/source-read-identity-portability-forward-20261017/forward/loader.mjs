// Exact Git-backed Health test modules. No mutable I2 source, DB or network.
import {registerHooks} from 'node:module';import fs from 'node:fs';import path from 'node:path';import {fileURLToPath,pathToFileURL} from 'node:url';import {execFileSync} from 'node:child_process';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..'),i2=path.resolve(repo,'../healthroster-install2-14102026'),parent='af933e3e611a0b83d361a185bfda269cb6543bc8',cache=new Map();
const sameCommit='a7e4522cb87ae6961cb40efcb83e4e69b12fbd6c',samePrefix='docs/healthroster-build/proposals/same-separate-counter-forward-20261017/',same=JSON.parse(execFileSync('git',['show',sameCommit+':'+samePrefix+'COMPOSED_SELECTION.json'],{cwd:repo,maxBuffer:1000000}).toString()),sameFiles=new Map(same.files.map(f=>[f.target,f]));
export const loaded=[];
const denied=p=>/^(?:broker\/src\/bpay-next|broker\/src\/banking\/)/.test(p);
function source(p){if(p==='broker/src/bpay-next-legacy-fence.js')return "export function normalizeBpayNextOwnerStatus(){throw new Error('UNRELATED_BANKING_FENCE_NOT_IN_SOURCE_READ_SCOPE');}";
 if(denied(p))throw Error('BANKING_BODY_EXCLUDED:'+p);if(cache.has(p))return cache.get(p);
 const candidate=path.join(here,'forward',p),prior=sameFiles.get(p);let raw;if(fs.existsSync(candidate))raw=fs.readFileSync(candidate,'utf8');else raw=execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','show',prior?(prior.sourceCommit??sameCommit)+':'+(prior.sourceCommit?prior.sourcePath:samePrefix+prior.sourcePath):parent+':'+p],{cwd:repo,maxBuffer:40000000,windowsHide:true}).toString();
 cache.set(p,raw);loaded.push(p);return raw;}
registerHooks({resolve(specifier,context,next){
 // Exact self-module exemption: Node --test can re-import this --import loader
 // inside I2. It is evidence infrastructure, not a Git-backed product module.
 if(specifier===import.meta.url)return next(specifier,context);
 if(specifier.startsWith('health-source:'))return{url:specifier,shortCircuit:true};
 if(context.parentURL?.startsWith('health-source:')&&specifier.startsWith('.'))return{url:new URL(specifier,context.parentURL).href,shortCircuit:true};
 if(context.parentURL?.startsWith('health-source:'))return next(specifier,{...context,parentURL:pathToFileURL(path.join(i2,new URL(context.parentURL).pathname.slice(1))).href});
 if(specifier.startsWith('file:')){const p=fileURLToPath(specifier);if(p.startsWith(i2+path.sep))return{url:'health-source:///'+path.relative(i2,p).replaceAll('\\','/'),shortCircuit:true};}
 return next(specifier,context);
},load(url,context,next){if(url.startsWith('health-source:'))return{format:'module',source:source(decodeURIComponent(new URL(url).pathname.slice(1))),shortCircuit:true};return next(url,context);}});
