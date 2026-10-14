// Finite TEST OFF command plumbing only; no fixture/certificate is created.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),base=path.resolve(dir,'../missing-accept-off-admission-construction-20261014');
const sha=b=>createHash('sha256').update(b).digest('hex'),records=[];
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')));
function compose(input,output,hunks){const raw=fs.readFileSync(path.join(base,input)),before=raw.toString(),eol=before.includes('\r\n')?'\r\n':'\n';
 hunks=hunks.map(h=>({...h,before:h.before.replaceAll('\n',eol),after:h.after.replaceAll('\n',eol)}));let after=before;
 for(const h of hunks){assert.equal(after.split(h.before).length,2,h.name);after=after.replace(h.before,h.after);}
 let reverse=after;for(const h of [...hunks].reverse()){assert.equal(reverse.split(h.after).length,2,h.name);reverse=reverse.replace(h.after,h.before);}assert.equal(reverse,before);
 for(const side of ['before','forward'])fs.mkdirSync(path.dirname(path.join(dir,side,output)),{recursive:true});
 fs.writeFileSync(path.join(dir,'before',output),raw);fs.writeFileSync(path.join(dir,'forward',output),after);
 records.push({input:'missing-accept-off-admission-construction-20261014/'+input,output,before_sha256:sha(raw),after_sha256:sha(after),hunks,whole_reverse_verified:true});
}
compose('forward/01_complete_off_command.sqlinc','01_complete_off_command.sqlinc',[
 {name:'existing native Office recheck in finite SOURCE_FIXTURE_SETUP purpose',before:"if op in ('weekly_source_upload_context_v1','weekly_source_upload_stage_begin_atomic_v1'",after:"if op in ('weekly_source_office_recheck_begin_v1','weekly_source_upload_context_v1','weekly_source_upload_stage_begin_atomic_v1'"},
 {name:'same actual public native owner, unchanged request and frame',before:"    when 'weekly_source_upload_context_v1' then result:=public.weekly_source_upload_context_v1(req);",after:"    when 'weekly_source_office_recheck_begin_v1' then result:=public.weekly_source_office_recheck_begin_v1(req);\n    when 'weekly_source_upload_context_v1' then result:=public.weekly_source_upload_context_v1(req);"}
]);
compose('forward/02_complete_off_transport.sqlinc','02_complete_off_transport.sqlinc',[
 {name:'same finite Office operation discovery purpose',before:"if op in ('weekly_source_upload_context_v1','weekly_source_upload_stage_begin_atomic_v1'",after:"if op in ('weekly_source_office_recheck_begin_v1','weekly_source_upload_context_v1','weekly_source_upload_stage_begin_atomic_v1'"}
]);
compose('forward/03_complete_off_setup_shape.sqlinc','03_complete_off_setup_shape.sqlinc',[
 {name:'closed SOURCE setup allowed-operation list, not an AUTOMATIC or general SQL route',before:"    'weekly_source_upload_context_v1','weekly_source_upload_stage_begin_atomic_v1'",after:"    'weekly_source_office_recheck_begin_v1','weekly_source_upload_context_v1','weekly_source_upload_stage_begin_atomic_v1'"}
]);
compose('forward/04_complete_off_parent_scope.sqlinc','04_complete_off_parent_scope.sqlinc',[
 {name:'discover actual new paid target parent before command audit/execution',before:"'family_id','work_event_id','prior_work_event_id',",after:"'family_id','work_event_id','prior_work_event_id','paid_overlap_target_work_event_id',"},
 {name:'actual target event inherits same real group/client/Candidate and sibling/fixture census refusal',before:"when 'work_event_id','prior_work_event_id' then",after:"when 'work_event_id','prior_work_event_id','paid_overlap_target_work_event_id' then"}
]);
compose('forward/broker/src/weekly-source/off-rule-native-proxy.mjs','broker/src/weekly-source/off-rule-native-proxy.mjs',[
 {name:'closed native operation entry in existing server dependency proxy',before:"sourceOperations=Object.freeze(['weekly_source_upload_context_v1'",after:"sourceOperations=Object.freeze(['weekly_source_office_recheck_begin_v1','weekly_source_upload_context_v1'"}
]);
fs.writeFileSync(path.join(dir,'COMPLETE_CONSUMER_REVERSES.json'),JSON.stringify({source_only:true,records,
 preserved_off_scope_gate_and_native_I1_order:true,command_parent_admission_before_frame:true,ordinary_transport_and_closed_requests_unchanged:true,
 new_operation:'weekly_source_office_recheck_begin_v1',purpose:'SOURCE_FIXTURE_SETUP',native_payload_ABI_unchanged:true,
 native_parent_field:'paid_overlap_target_work_event_id',existing_actor_and_group_and_fixture_identity_guards_preserved:true,
 qualified_multi_period_fixture_constructed:false,settled_Original_fixture_or_qualification_minted:false,new_public_signature:false,
 canonical_mounted:false,SQL_compiled:false,native_proof:false,full_primary_complete:false},null,2)+'\n');
console.log(JSON.stringify(records.map(r=>({output:r.output,hunks:r.hunks.length,sha256:r.after_sha256}))));
