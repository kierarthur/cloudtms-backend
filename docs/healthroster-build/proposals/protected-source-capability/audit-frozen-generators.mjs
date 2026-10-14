import fs from 'node:fs';
import crypto from 'node:crypto';
import path from 'node:path';
import vm from 'node:vm';
const dir='docs/healthroster-build/proposals/protected-source-capability';
const generators=['f5-window-rounding/generate-owner-deltas.mjs','m1-whole-shift/generate-complete-owners.mjs','f5-window-receipt-complete/generate-complete-successors.mjs'];
const report=[];
for(const relative of generators){
 const file='docs/healthroster-build/proposals/'+relative, virtual=new Map(), defects=[];
 const mock={...fs,mkdirSync(){},writeFileSync(name,data){virtual.set(name,Buffer.from(data));},copyFileSync(from,to){virtual.set(to,fs.readFileSync(from));},
  readFileSync(name,options){if(!virtual.has(name))return fs.readFileSync(name,options);const b=virtual.get(name);return typeof options==='string'?b.toString(options):b;}};
 const context=vm.createContext({fs:mock,crypto,path,process:{stdout:{write(){}}},Buffer,defects});
 vm.runInContext(`const originalReplaceAll=String.prototype.replaceAll;
 String.prototype.replaceAll=function(search,replacement){
  if(typeof replacement==='string' && /\\$[&'\x60]|\\$\\d|\\$</.test(replacement)){
   const unsafe=originalReplaceAll.call(this,search,replacement),safe=originalReplaceAll.call(this,search,()=>replacement);
   if(unsafe!==safe)defects.push({anchor:String(search).slice(0,100),replacement_excerpt:replacement.match(/.{0,35}\\$[&'\x60].{0,35}/g),stack:new Error().stack,unsafe_length:unsafe.length,safe_length:safe.length});
  }
  return originalReplaceAll.call(this,search,replacement);
 };`,context);
 let failure=null;
 try{new vm.Script(fs.readFileSync(file,'utf8').replace(/^import .*;\r?\n/gm,''),{filename:file}).runInContext(context);}catch(e){failure=String(e);}
 report.push({generator:file,defects,failure,virtual_artifacts:[...virtual.keys()],disk_writes:0});
}
fs.writeFileSync(dir+'/FROZEN_GENERATOR_AUDIT.json',JSON.stringify({schema_version:'FROZEN_GENERATOR_REPLACEMENT_AUDIT_V1',report},null,2)+'\n');
process.stdout.write(JSON.stringify(report.map(x=>({generator:x.generator,defects:x.defects,failure:x.failure})))+'\n');
