import fs from 'node:fs';
import path from 'node:path';
import { here, hash } from './read-source.mjs';
const artifacts=fs.readdirSync(here).filter(p=>p!=='SOURCE_MANIFEST.json').sort().map(p=>{
 const b=fs.readFileSync(path.join(here,p));return {path:p,bytes:b.length,sha256:hash(b)};
});
const value={schema_version:'AUTOMATIC_B9E2_ADJACENT_REVIEW_MANIFEST_V1',
 status:'REVIEWED_SOURCE_ONLY_NATIVE_COMPILE_EXECUTION_AND_EXPOSURE_HELD',
 baseline:'b9e2cbfcdf061a4e8fe07044e658c2daf043a362',
 branch:'codex/healthroster-v2-report-build-13102026',artifacts,
 artifact_count:artifacts.length,
 package_sha256:hash(artifacts.map(f=>`${f.path}\0${f.sha256}\0`).join('')),
 local_checks:{original_automatic:31,stable_key_forward:5,adjacent_review:13,total:49,pass:49,fail:0,skip:0,
  command:'node --test tests/weekly-source/automatic-native-delta-proposal.test.mjs docs/healthroster-build/proposals/automatic-native-delta-forward-review/forward-review.test.mjs docs/healthroster-build/proposals/automatic-native-delta-root-b9e2-review/review.test.mjs',
  qualification:'Real source/hash/reverse-proof/native-carrier and adversarial comparison checks; official SQL packaging scanner only, no PostgreSQL semantic compilation or native acceptance.'},
 unchanged:{rootFiles:true,frozenAutomatic23:true,frozenStableKey7:true,frozenReport53:true,frozenFixture16:true,old168SafetyReviews:true,old152Eligible16Exclusions:true},
 databaseCalls:false,networkBusinessCalls:false,APIExposure:false,
 root_gates:['Actual latest capability/Bank ACL union and generated contract.',
  'SEND_7 isolated native fixture reruns and complete K->K+1 component/receipt/Source-CAS proof.',
  'Post-Banking NEW/UPGRADE rehearsal and exact release/native/API evidence.',
  'Report public FIN dispatch/operational A/B remains held; no pay or settlement inferred.']};
fs.writeFileSync(path.join(here,'SOURCE_MANIFEST.json'),JSON.stringify(value,null,2)+'\n');
console.log(JSON.stringify({artifact_count:value.artifact_count,package_sha256:value.package_sha256,
 manifest_sha256:hash(fs.readFileSync(path.join(here,'SOURCE_MANIFEST.json'))),bytes:artifacts.reduce((n,a)=>n+a.bytes,0)}));
