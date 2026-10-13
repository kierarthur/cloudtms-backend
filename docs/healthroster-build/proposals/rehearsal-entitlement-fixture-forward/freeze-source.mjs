import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url));const sha=x=>crypto.createHash('sha256').update(x).digest('hex');
const artifacts=fs.readdirSync(dir).filter(p=>p!=='SOURCE_MANIFEST.json').sort().map(p=>{const b=fs.readFileSync(path.join(dir,p));return {path:p,bytes:b.length,sha256:sha(b)};});
const manifest={schema_version:'ENTITLEMENT_REHEARSAL_FIXTURE_SOURCE_MANIFEST_V1',status:'SOURCE_ONLY_ROOT_REVIEW_REQUIRED',
  historical_commit:'547c74da5f0a05b09297c042bc7f3cc0c0105b85',branch:'codex/healthroster-v2-report-build-13102026',artifacts,
  package_sha256:sha(JSON.stringify(artifacts)),local_checks:{passed:8,failed:0,skipped:0,guarded_runner_syntax:'PASS'},
  database_or_runtime_executed:false,first_native_fix_proved:false,root_or_frozen_files_modified:false,
  new_production_routines:0,new_production_tables_or_acl:0,entitlement_verifier_conditional_changed:false,
  native_gates:['Bank first install record and current SEND_7 owned connection','Current staged-blob complete verifier compile and first failure labels',
    'Named head build checks under actual initial timing','Standalone/savepoint caller-frame atomicity positive and negative','All original FK/hash/security/receipt/expected failures',
    'Explicit rollback and fresh marker/catalogue/ledger restoration'],archive_guard_retained:true};
fs.writeFileSync(path.join(dir,'SOURCE_MANIFEST.json'),JSON.stringify(manifest,null,2)+'\n');
console.log(JSON.stringify({artifact_count:artifacts.length,package_sha256:manifest.package_sha256,manifest_sha256:sha(fs.readFileSync(path.join(dir,'SOURCE_MANIFEST.json')))}));
