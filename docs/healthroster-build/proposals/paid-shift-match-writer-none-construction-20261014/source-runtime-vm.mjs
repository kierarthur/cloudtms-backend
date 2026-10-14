// Test-only whole-module VM. Exact current pure financial/qualification modules
// are read from local Git objects. Unused transport/parser/Report entry exports
// throw if called; no transport, filesystem fixture or native proof is supplied.
import fs from 'node:fs';import path from 'node:path';import vm from 'node:vm';
import assert from 'node:assert/strict';import {execFileSync} from 'node:child_process';import * as crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..');
const commit='fa66183f01913e86651e82816f5e88f97e07123c',prefix='broker/src/weekly-source/';
export function loadProjectionOwner(side='forward'){
 const cache=new Map(),pins=[];
 const inert=names=>Object.fromEntries(names.map(name=>[name,()=>{throw Error('UNUSED_TRANSPORT_CALLED:'+name);}]));
 const stubs={
  'report-upload-provenance.mjs':{REPORT_PROFILE:'SELF_BILL_REPORT_V1',...inert(['assertReportUploadRequest','assertReportRuleContext','reportScopeRequest','assertReportScopeReceipt','assertReportPublicationContext','assertReportStageRowsReceipt','assertReportProjectionRowsReceipt'])},
  'upload-staging-adapter.mjs':inert(['adaptWeeklySourceParserOutput']),
  'coverage-warnings.mjs':inert(['weeklySourceCoverageWarnings'])
 };
 function load(file){
  if(cache.has(file))return cache.get(file);
  if(file==='node:crypto')return crypto;
  assert.ok(file.startsWith(prefix),file);
  const short=file.slice(prefix.length);if(stubs[short])return stubs[short];
  const raw=file===prefix+'upload-publication-owner.mjs'
    ?fs.readFileSync(path.join(dir,side,file))
    :execFileSync('git',['show',commit+':'+file],{cwd:root,maxBuffer:2e6});
  pins.push({file,sha256:crypto.createHash('sha256').update(raw).digest('hex')});
  let source=raw.toString(),bindings={},exports=[];
  source=source.replace(/^import\s+\{([\s\S]*?)\}\s+from\s+['"]([^'"]+)['"];?/gm,(_m,names,ref)=>{
   const target=ref.startsWith('node:')?ref:path.posix.normalize(path.posix.join(path.posix.dirname(file),ref));
   const values=load(target);for(const spec of names.split(',').map(s=>s.trim()).filter(Boolean)){
    const [name,alias]=spec.split(/\s+as\s+/);assert.ok(name in values,name+' in '+target);bindings[alias??name]=values[name];
   }return '';
  });
  source=source.replace(/^export\s+(?=(?:async\s+)?(?:function|class|const|let|var)\b)/gm,'');
  for(const m of raw.toString().matchAll(/^export\s+(?:(?:async\s+)?function|class|const|let|var)\s+(\w+)/gm))exports.push(m[1]);
  assert.ok(!/^import\s|^export\s/m.test(source),'unsupported module form:'+file);
  const context=vm.createContext({...bindings,TextEncoder,TextDecoder,Buffer,Intl,structuredClone,__exports:{}});
  vm.runInContext(source+'\n'+exports.map(name=>`__exports.${name}=${name};`).join('\n'),context,{filename:file,timeout:3000});
  const result=context.__exports;cache.set(file,result);return result;
 }
 return {module:load(prefix+'upload-publication-owner.mjs'),pins,source_only:true,native_proof:false};
}
