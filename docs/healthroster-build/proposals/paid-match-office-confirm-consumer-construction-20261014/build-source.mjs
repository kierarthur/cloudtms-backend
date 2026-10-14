import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),sha=b=>createHash('sha256').update(b).digest('hex');
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')));
const records=[];
function compose(file,hunks,options={}){
 const raw=fs.readFileSync(path.join(dir,'before',file)),before=raw.toString(),eol=before.includes('\r\n')?'\r\n':'\n';
 hunks=hunks.map(h=>({...h,before:h.before.replaceAll('\n',eol),after:h.after.replaceAll('\n',eol)}));
 let after=before;for(const h of hunks){assert.equal(after.split(h.before).length,2,h.name);after=after.replace(h.before,h.after);}
 let reversed=after;for(const h of [...hunks].reverse()){assert.equal(reversed.split(h.after).length,2,h.name);reversed=reversed.replace(h.after,h.before);}assert.equal(reversed,before);
 fs.mkdirSync(path.dirname(path.join(dir,'forward',file)),{recursive:true});fs.writeFileSync(path.join(dir,'forward',file),after);
 records.push({file,...options,before_bytes:raw.length,before_sha256:sha(raw),after_bytes:Buffer.byteLength(after),after_sha256:sha(after),hunks,whole_reverse_verified:true});
}
compose('02_complete_office_workspace.sqlinc',[
 {name:'native question status in existing blocked row',before:"        'text',case\n          when resolution.mapping_state is distinct from 'RESOLVED' then 'Needs correction'",after:"        'text',case\n          when resolution.blocker_code='PAID_SHIFT_OVERLAP_CONFIRMATION_REQUIRED' then 'Paid shift needs matching'\n          when resolution.mapping_state is distinct from 'RESOLVED' then 'Needs correction'"},
 {name:'paid matching plain problem',before:"      'problem',case\n        when resolution.blocker_code='PROTECTED_SHIFT_MATCH_REQUIRED'",after:"      'problem',case\n        when resolution.blocker_code='PAID_SHIFT_OVERLAP_CONFIRMATION_REQUIRED' then case\n          when paid_question.value->>'state'='READY' then 'Confirm whether this is the paid shift or separate work.'\n          else 'The paid-shift comparison is unavailable. Recheck the current payment and source authority.' end\n        when resolution.blocker_code='PROTECTED_SHIFT_MATCH_REQUIRED'"},
 {name:'finite distinct question action',before:"        'label',case when resolution.blocker_code='PROTECTED_SHIFT_MATCH_REQUIRED' then 'Confirm shift match'",after:"        'label',case when resolution.blocker_code='PAID_SHIFT_OVERLAP_CONFIRMATION_REQUIRED' then case\n            when paid_question.value->>'state'='READY' then 'Confirm paid shift match' else 'View details' end\n          when resolution.blocker_code='PROTECTED_SHIFT_MATCH_REQUIRED' then 'Confirm shift match'"},
 {name:'bounded native DTO without raw paid facts',before:"          'match_choices',private.weekly_source_protected_match_candidates_v1(source_row.id,resolution.candidate_id,resolution.client_id),",after:"          'paid_match_question',paid_question.value,\n          'match_choices',private.weekly_source_protected_match_candidates_v1(source_row.id,resolution.candidate_id,resolution.client_id),"},
 {name:'paid unavailable detail has no implied financial permission',before:"            'problem',case\n              when resolution.blocker_code='PROTECTED_SHIFT_MATCH_REQUIRED'",after:"            'problem',case\n              when resolution.blocker_code='PAID_SHIFT_OVERLAP_CONFIRMATION_REQUIRED' then case\n                when paid_question.value->>'state'='READY' then 'Choose Same only for the work already paid. Earlier pay is retained; the imported row is invoiced. Choose Separate only for different work, then enter a reason. The saved comparison is rechecked before it can be used.'\n                else 'The native paid proof, current source or payment authority is unavailable. No Same or Separate choice can be saved from this screen. Recheck the workspace.' end\n              when resolution.blocker_code='PROTECTED_SHIFT_MATCH_REQUIRED'"},
 {name:'question derives once from actual current publication and current resolution',before:"    left join lateral (select latest.* from public.weekly_source_row_resolutions latest\n      where latest.upload_row_id=source_row.id order by latest.generation desc,latest.id desc limit 1) resolution on true\n    left join public.candidates candidate on candidate.id=resolution.candidate_id\n    left join lateral (select latest.* from public.weekly_source_charge_checks latest",after:"    left join lateral (select latest.* from public.weekly_source_row_resolutions latest\n      where latest.upload_row_id=source_row.id order by latest.generation desc,latest.id desc limit 1) resolution on true\n    left join lateral (select case when resolution.blocker_code='PAID_SHIFT_OVERLAP_CONFIRMATION_REQUIRED'\n      then private.weekly_source_paid_match_question_v1(v_publication.id,source_row.id,v_actor) else null end value) paid_question on true\n    left join public.candidates candidate on candidate.id=resolution.candidate_id\n    left join lateral (select latest.* from public.weekly_source_charge_checks latest"},
],{repository:'backend',commit:'fa66183f01913e86651e82816f5e88f97e07123c',signature:'public.weekly_source_office_workspace_v1(jsonb)',complete_definition:true});
// Existing owner/service-only ACL is made literal in the selected wrapper,
// separate from the exact complete-definition reversal above.
fs.writeFileSync(path.join(dir,'forward/03_office_workspace_acl.sqlinc'),`alter function public.weekly_source_office_workspace_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_office_workspace_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_source_office_workspace_v1(jsonb) to service_role;
notify pgrst, 'reload schema';
`);
const addition=fs.readFileSync(path.join(dir,'paid-match-frontend-functions.jsinc'),'utf8');
compose('frontend/js/weekly-source/workspace-actions.js',[
 {name:'closed question request builder and actor-frame-bound actual modal',before:'  function openProtectedMatch(payloadValue) {',after:addition+'  function openProtectedMatch(payloadValue) {'},
 {name:'literal dispatch leaves protected match unchanged',before:"    if (label === 'Confirm shift match') return openProtectedMatch(detail.payload);",after:"    if (label === 'Confirm paid shift match') return openPaidMatch(detail.payload);\n    if (label === 'Confirm shift match') return openProtectedMatch(detail.payload);"},
 {name:'pure closed DTO constructors available for inspected tests',before:'    CONTRACT, normalisePreview, renderPreview, buildUploadAcceptancePayload,',after:'    CONTRACT, normalisePreview, renderPreview, buildUploadAcceptancePayload, normalisePaidMatch, buildPaidMatchRequest,'}
],{repository:'frontend',commit:'2d15d121ac888a2e2a076dc7a2c4be85f9958556',complete_file:true});
fs.mkdirSync(path.join(dir,'before/frontend/js/weekly-source'),{recursive:true});
fs.writeFileSync(path.join(dir,'before/frontend/js/weekly-source/import-workspace.js'),fs.readFileSync(path.join(dir,'input/current_import_workspace.js')));
compose('frontend/js/weekly-source/import-workspace.js',[
 {name:'existing Office checks allowlist receives one exact paid-match action',before:"    finalise: new Set(['Confirm shift match',",after:"    finalise: new Set(['Confirm paid shift match', 'Confirm shift match',"},
 {name:'stale current workspace disables the new matching mutation too',before:"'No shifts to import', 'Confirm shift match',",after:"'No shifts to import', 'Confirm paid shift match', 'Confirm shift match',"}
],{repository:'frontend',commit:'2d15d121ac888a2e2a076dc7a2c4be85f9958556',complete_file:true});
fs.writeFileSync(path.join(dir,'COMPLETE_CURRENT_REVERSES.json'),JSON.stringify({source_only:true,records,
 native_money_classification_copied:false,existing_Office_recheck_route_and_saved_replay_unchanged:true,
 frozen_writer58_runtime_required:true,canonical_mounted:false,native_proof:false,full_primary_complete:false},null,2)+'\n');
console.log(JSON.stringify(records.map(r=>({file:r.file,hunks:r.hunks.length,after_sha256:r.after_sha256}))));
