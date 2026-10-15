// Only literal module/VM source loaders are redirected. All frozen assertions
// and actual caller bodies are retained. This does not execute any SQL.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import crypto from 'node:crypto';import {execFileSync} from 'node:child_process';import {pathToFileURL,fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..'),root=path.resolve(repo,'../healthroster-install2-14102026'),pins=[];
const sha=b=>crypto.createHash('sha256').update(b).digest('hex'),get=(c,p)=>execFileSync('git',['cat-file','blob',c+':'+p],{cwd:repo,maxBuffer:4e6});
for(const file of ['tests/healthroster-checklist-source.test.mjs','tests/healthroster-checklist-read-transport.test.mjs','tests/healthroster-checklist-source-query-money.test.mjs','tests/healthroster-checklist-source-invoice-block.test.mjs','tests/healthroster-checklist-invoice-decision-transport.test.mjs','tests/healthroster-checklist-ordinary-next-hold.test.mjs']){
 const commit='b0086dd9d6b5b6c1f20ef9bc57c2b73c7e669fe4',original=get(commit,file),url=pathToFileURL(path.join(repo,file));assert.ok(original.equals(fs.readFileSync(path.join(repo,file))),file);
 let source=original.toString();const meta=(source.match(/import\.meta\.url/g)||[]).length;
 // Full inspection pins exactly these seven real outer URL expressions; none
 // occur in a string/embedded script. Do not replace nested script metadata.
 const expected={'healthroster-checklist-source-query-money.test.mjs':3,'healthroster-checklist-source-invoice-block.test.mjs':2,'healthroster-checklist-invoice-decision-transport.test.mjs':1,'healthroster-checklist-ordinary-next-hold.test.mjs':1};
 assert.equal(meta,expected[path.basename(file)]||0);source=source.replaceAll('import.meta.url',JSON.stringify(url.href));
 source=source.replace(/from\s+(['"])(\.[^'"]+)\1/g,(all,q,r)=>{let target=new URL(r,url);
  if(r.endsWith('/healthroster-checklist-source.mjs'))target=pathToFileURL(path.join(here,'forward/healthroster-checklist-source.mjs'));
  if(r.endsWith('/checklist-read.mjs'))target=pathToFileURL(path.join(here,'forward/checklist-read.mjs'));return 'from '+q+target.href+q;});
 pins.push({commit,file,original_sha256:sha(original),adapted_sha256:sha(source),loader_changes_only:true,original_assertions_unchanged:true,actual_outer_meta_expressions:meta});
 await import('data:text/javascript;base64,'+Buffer.from(source).toString('base64'));
}
{
 const commit='de90ab2134aca782a0f4b56154585fad65a81f12',file='tests/healthroster-office-paid-native-caller-union.test.mjs',original=get(commit,file),url=pathToFileURL(path.join(root,file));assert.ok(original.equals(fs.readFileSync(path.join(root,file))));
 let source=original.toString();assert.equal((source.match(/import\.meta\.url/g)||[]).length,1);source=source.replace('import.meta.url',JSON.stringify(url.href));
 source=source.replace(/from\s+(['"])(\.[^'"]+)\1/g,(all,q,r)=>'from '+q+new URL(r,url).href+q);
 const needle="vm.runInContext(read(original?'reference/workspace-actions.paid-native-before-guard.jsinc':'forward/workspace-actions.jsinc'),ctx,{timeout:3000});";
 assert.equal(source.split(needle).length,2);source=source.replace(needle,()=>"vm.runInContext(original?read('reference/workspace-actions.paid-native-before-guard.jsinc'):fs.readFileSync("+JSON.stringify(path.join(here,'forward/workspace-actions.jsinc'))+",'utf8'),ctx,{timeout:3000});");
 pins.push({commit,file,original_sha256:sha(original),adapted_sha256:sha(source),loader_changes_only:true,original_assertions_unchanged:true,whole_paid_caller_VM_uses_new_complete_successor:true,actual_outer_meta_expressions:1});
 await import('data:text/javascript;base64,'+Buffer.from(source).toString('base64'));
}
fs.writeFileSync(path.join(here,'WHOLE_PREDECESSOR_REGRESSION_PINS.json'),JSON.stringify({source_only:true,native:false,SQL_executed:false,pins},null,2)+'\n');
