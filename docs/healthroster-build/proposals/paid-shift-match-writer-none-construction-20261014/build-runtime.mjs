import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..'),sha=b=>createHash('sha256').update(b).digest('hex');
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')));
const commit='fa66183f01913e86651e82816f5e88f97e07123c',file='broker/src/weekly-source/upload-publication-owner.mjs';
const raw=execFileSync('git',['show',commit+':'+file],{cwd:root,maxBuffer:2e6}),before=raw.toString(),eol=before.includes('\r\n')?'\r\n':'\n';
const hunks=[
 {name:'literal real paid choice and native paid overlap blocker, preserving existing protected picker',before:'  if (sourceAuthority) {\n    const matches = (row.protected_matches ?? []).filter(item => item.contract_id === qualified.selectedContractId);',after:`  const paidChoice = row.paid_overlap_choice;
  if (sourceAuthority && paidChoice != null) {
    if (paidChoice.schema_version !== 'WEEKLY_SOURCE_PAID_OVERLAP_CURRENT_CHOICE_V1'
      || !['NORMAL', 'NONE'].includes(paidChoice.pay_effect)
      || paidChoice.upload_row_id !== row.upload_row_id
      || paidChoice.candidate_id !== row.candidate_id || paidChoice.client_id !== row.client_id
      || paidChoice.contract_id !== qualified.selectedContractId
      || typeof paidChoice.paid_overlap_target_work_event_id !== 'string'
      || typeof paidChoice.choice_proof_sha256 !== 'string' || !/^[0-9a-f]{64}$/.test(paidChoice.choice_proof_sha256)) {
      return { ...base, contract_id: qualified.selectedContractId, mapping_state: 'SOURCE_ROW_BLOCKED',
        blocker_code: 'PAID_SHIFT_MATCH_CHOICE_STALE', qualifying_contract_ids: qualified.eligibleContractIds };
    }
    base.prior_work_event_id = paidChoice.pay_effect === 'NONE' ? paidChoice.paid_overlap_target_work_event_id : null;
    // The native constructor re-reads the actual immutable choice and derives
    // pay_effect itself. No pay/evidence assertion enters p_rows from Worker.
  } else if (sourceAuthority && ROSTER_EXTERNAL.has(row.profile_id) && row.row_finalisation_state === 'SOURCE_WORKED'
    && !base.prior_work_event_id && (row.paid_overlap_inputs?.overlaps ?? []).some(item => item.native_payment_facts?.paid !== false)) {
    return { ...base, contract_id: qualified.selectedContractId, mapping_state: 'SOURCE_ROW_BLOCKED',
      blocker_code: 'PAID_SHIFT_OVERLAP_CONFIRMATION_REQUIRED', qualifying_contract_ids: qualified.eligibleContractIds };
  } else if (sourceAuthority) {
    const matches = (row.protected_matches ?? []).filter(item => item.contract_id === qualified.selectedContractId);`}
].map(h=>({...h,before:h.before.replaceAll('\n',eol),after:h.after.replaceAll('\n',eol)}));
let after=before;for(const h of hunks){assert.equal(after.split(h.before).length,2);after=after.replace(h.before,h.after);}let reverse=after;
for(const h of [...hunks].reverse()){assert.equal(reverse.split(h.after).length,2);reverse=reverse.replace(h.after,h.before);}assert.equal(reverse,before);
fs.mkdirSync(path.join(dir,'forward/broker/src/weekly-source'),{recursive:true});fs.mkdirSync(path.join(dir,'before/broker/src/weekly-source'),{recursive:true});
fs.writeFileSync(path.join(dir,'forward',file),after);fs.writeFileSync(path.join(dir,'before',file),raw);
fs.writeFileSync(path.join(dir,'RUNTIME_REVERSE.json'),JSON.stringify({source_only:true,commit,file,input_bytes:raw.length,input_sha256:sha(raw),after_sha256:sha(after),hunks,whole_reverse_verified:true,all_current_imports_and_callers_retained:true,native_private_payload_unchanged:true,route_and_office_consumers_implemented:false,canonical_mounted:false,native_proof:false,pipeline_complete:false},null,2)+'\n');
console.log(JSON.stringify({file,hunks:hunks.length,after_sha256:sha(after),pipeline_complete:false}));
