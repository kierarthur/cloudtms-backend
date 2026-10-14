// Data-free source freeze only. This performs no Git/database/runtime action.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),lane=path.resolve(here,'../../../..');
const hash=value=>crypto.createHash('sha256').update(value).digest('hex');
function files(dir){return fs.readdirSync(dir,{withFileTypes:true}).flatMap(entry=>entry.isDirectory()?files(path.join(dir,entry.name)):[path.join(dir,entry.name)]);}
for(const [folder,schema,checks] of [
  ['report-native-pay-fin-delegate','REPORT_NATIVE_FIN_WORKER_SOURCE_MANIFEST_V1',57],
  ['automatic-native-delta-forward-review','AUTOMATIC_NATIVE_DELTA_FORWARD_SOURCE_MANIFEST_V1',5],
]){
  const dir=path.resolve(here,'..',folder);
  const artifacts=files(dir).filter(file=>path.basename(file)!=='SOURCE_MANIFEST.json').sort().map(file=>{
    const bytes=fs.readFileSync(file);return {path:path.relative(lane,file).replaceAll('\\','/'),bytes:bytes.length,sha256:hash(bytes)};
  });
  const manifest={schema_version:schema,status:'FROZEN_SOURCE_ONLY_FOR_ROOT_INDEPENDENT_REVIEW',
    authoritative_lane:'healthroster-v2-report-build-13102026',branch:'codex/healthroster-v2-report-build-13102026',
    artifact_count:artifacts.length,artifacts,package_sha256:hash(JSON.stringify(artifacts)),
    local_checks:{passed:checks,failed:0,skipped:0,combined_passed:62,node_syntax:'PASS_THREE_CHANGED_WORKER_MODULES',
      evidence:'Offline actual-parser/source/hunk/closure/protocol/adversarial checks. Transport stubs are not native acceptance.'},
    database_compile:'NOT_RUN_LANE_FORBIDDEN',runtime_native_acceptance:'HELD_ROOT_SEND_7_AFTER_BANK_FIRST_INSTALL',
    exposure:'UNMOUNTED',root_or_frozen_edits:0,
    root_gates:folder==='report-native-pay-fin-delegate'?[
      'Exact PostgreSQL 17 staged-blob compile and canonical +3 routine contract/final ACL fingerprint',
      'Banking first install recorded and root SEND_7 lock for serial controlled rehearsal',
      'Actual S12 permitted Report upload date and native Office/rule/source authority',
      'Actual native Worker Report stage/seal/current publication/binding and preserved ordinary/NHSP/M1 journeys',
      'Real ordinary/protected pay-run/task/receipt/approved inventory/current publication positive and negative journeys',
      'Internal separate A/B native FIN/admission/render/zero-pay qualification then release-bound deterministic operational variant',
      'Protected reconcile-only current publisher has no proved latest WAIT producer',
      'Correct-final-source pay-run provenance must be supplied by controlling native orchestration',
      'Prepared saved amount unavailable without exact retained normalized writer output',
      'Actual NEXT active/withheld caller-owned amount join remains unproved',
      'Native schema cache/API/runtime proof before mounting existing route'
    ]:[
      'Complete forward same-signature owner compile',
      'Native sealed stage K to K+1 and independent stable-key-only negative vector',
      'Actual complete Source CAS equality before/after own stage with no real Source authority drift waiver',
      'Changed additional raw detail/policy/rates and SourceFixedExpense provenance negative vectors'
    ]};
  fs.writeFileSync(path.join(dir,'SOURCE_MANIFEST.json'),JSON.stringify(manifest,null,2)+'\n');
  console.log(JSON.stringify({folder,artifact_count:artifacts.length,package_sha256:manifest.package_sha256,manifest_sha256:hash(fs.readFileSync(path.join(dir,'SOURCE_MANIFEST.json')))}));
}
