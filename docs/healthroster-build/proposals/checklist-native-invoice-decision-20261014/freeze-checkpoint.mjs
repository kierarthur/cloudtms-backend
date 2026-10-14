import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(here,'../../../..');
const walk=dir=>fs.readdirSync(dir,{withFileTypes:true}).flatMap(e=>e.isDirectory()?walk(path.join(dir,e.name)):[path.join(dir,e.name)]);
const files=walk(here).filter(p=>path.basename(p)!=='CHECKPOINT_MANIFEST.json').concat(path.join(root,'tests/healthroster-checklist-source-invoice-release-kernel.test.mjs')).sort();
const manifest={status:'UNMOUNTED_PRIVATE_DECISION_OWNER_CONSTRUCTION_PUBLIC_INVOICE_FREEZE_UNCHANGED',source_checks:5,
  native:false,SQL_executed:false,public_producer_consumes_decision:false,true_mode_parity:false,false_mode_purity:false,
  public_invoice_freeze:'aea33728bb76c4d2c57a9e82abffa570e743a0f0',repeated_scope_census_performance_unqualified:true,
  artifacts:files.map(p=>{const b=fs.readFileSync(p);return {file:path.relative(root,p).replaceAll('\\','/'),bytes:b.length,sha256:crypto.createHash('sha256').update(b).digest('hex')};})};
fs.writeFileSync(path.join(here,'CHECKPOINT_MANIFEST.json'),JSON.stringify(manifest,null,2)+'\n');
console.log(JSON.stringify({...manifest,artifacts:manifest.artifacts.length}));
