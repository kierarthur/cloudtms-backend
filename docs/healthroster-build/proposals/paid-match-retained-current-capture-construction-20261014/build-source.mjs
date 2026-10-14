import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..'),sha=b=>createHash('sha256').update(b).digest('hex');assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')));
const records=[];
function compose(packet,input,out,hunks){const source=`docs/healthroster-build/proposals/${packet}/${input}`,raw=fs.readFileSync(path.join(root,source));
 assert.deepEqual(raw,execFileSync('git',['show','76a5dbd0721ae2c41edb049312aa81d5df5a06be:'+source],{cwd:root,maxBuffer:2e6}));
 const before=raw.toString(),eol=before.includes('\r\n')?'\r\n':'\n';hunks=hunks.map(h=>({...h,before:h.before.replaceAll('\n',eol),after:h.after.replaceAll('\n',eol)}));
 let after=before;for(const h of hunks){assert.equal(after.split(h.before).length,2,h.name);after=after.replace(h.before,h.after);}let back=after;
 for(const h of [...hunks].reverse()){assert.equal(back.split(h.after).length,2,h.name);back=back.replace(h.after,h.before);}assert.equal(back,before);
 fs.mkdirSync(path.join(dir,'before'),{recursive:true});fs.writeFileSync(path.join(dir,'before',out),raw);fs.writeFileSync(path.join(dir,'forward',out),after);
 records.push({source,containing_commit:'76a5dbd0721ae2c41edb049312aa81d5df5a06be',file:out,before_bytes:raw.length,before_sha256:sha(raw),after_bytes:Buffer.byteLength(after),after_sha256:sha(after),hunks,whole_reverse_verified:true});}
compose('paid-shift-overlap-native-facts-construction-20261014','forward/03_paid_overlap_source_inputs.sqlinc','02_complete_overlap_inputs.sqlinc',[
 {name:'actual maintained current snapshot observation',before:'  v_payment jsonb;v_items jsonb',after:'  v_current_snapshot jsonb;\n  v_payment jsonb;v_items jsonb'},
 {name:'genuine current native snapshot rather than original movement transition only',before:`    select snapshot.* into strict v_snapshot from public.weekly_source_final_snapshot_lines snapshot
      join public.weekly_source_state_transitions transition on transition.new_snapshot_line_id=snapshot.id
      where transition.id=v_movement.transition_id and transition.final_revision_id=v_movement.final_revision_id
        and snapshot.final_revision_id=v_movement.final_revision_id and snapshot.work_event_id=v_event.id
        and snapshot.candidate_id=p_candidate_id and snapshot.client_id=p_client_id
        and snapshot.contract_id=v_movement.contract_id and snapshot.work_date=v_row.work_date;`,after:`    v_current_snapshot:=private.weekly_source_paid_overlap_current_snapshot_v1(v_event.id,v_movement.id,
      p_candidate_id,p_client_id,v_movement.contract_id,v_cycle.source_group_id,
      array(select value::uuid from jsonb_array_elements_text(v_payment->'root_timesheet_ids') value));
    if v_current_snapshot->>'state'='CURRENT' then
      select snapshot.* into strict v_snapshot from public.weekly_source_final_snapshot_lines snapshot
        where snapshot.id=(v_current_snapshot->>'snapshot_line_id')::uuid and snapshot.work_event_id=v_event.id
          and snapshot.candidate_id=p_candidate_id and snapshot.client_id=p_client_id
          and snapshot.contract_id=v_movement.contract_id and snapshot.work_date=v_row.work_date;
    else
      -- Retain a factual unknown/legacy overlap rather than erasing it. Its
      -- original tuple never authorises Same/Separate without CURRENT proof.
      select snapshot.* into strict v_snapshot from public.weekly_source_final_snapshot_lines snapshot
        join public.weekly_source_state_transitions transition on transition.new_snapshot_line_id=snapshot.id
        where transition.id=v_movement.transition_id and transition.final_revision_id=v_movement.final_revision_id
          and snapshot.final_revision_id=v_movement.final_revision_id and snapshot.work_event_id=v_event.id
          and snapshot.candidate_id=p_candidate_id and snapshot.client_id=p_client_id
          and snapshot.contract_id=v_movement.contract_id and snapshot.work_date=v_row.work_date;
    end if;`},
 {name:'current selection provenance is hashed in actual full native input',before:"      'native_payment_facts',v_payment));",after:"      'current_pay_snapshot',v_current_snapshot,'native_payment_facts',v_payment));"}
]);
compose('paid-shift-match-writer-none-construction-20261014','forward/01_paid_overlap_choice_admit.sqlinc','03_complete_choice_admit.sqlinc',[
 {name:'locked choice requires genuine complete current monetary snapshot lineage',before:"  if v_native_paid->'paid' is distinct from 'true'::jsonb",after:"  if v_match#>>'{current_pay_snapshot,state}' is distinct from 'CURRENT'\n    or v_match#>>'{current_pay_snapshot,snapshot_line_id}' is distinct from v_match->>'snapshot_line_id'\n    or v_match#>>'{current_pay_snapshot,movement_id}' is distinct from v_match->>'current_pay_head_movement_id'\n    or v_native_paid->'paid' is distinct from 'true'::jsonb"}
]);
compose('paid-shift-match-writer-none-construction-20261014','forward/03_paid_overlap_choice_current.sqlinc','04_complete_choice_current.sqlinc',[
 {name:'fresh current choice cannot treat an unavailable original tuple as current',before:"  if v_match is null or v_match#>'{native_payment_facts,paid}'",after:"  if v_match is null or v_match#>>'{current_pay_snapshot,state}' is distinct from 'CURRENT'\n    or v_match#>>'{current_pay_snapshot,snapshot_line_id}' is distinct from v_match->>'snapshot_line_id'\n    or v_match#>>'{current_pay_snapshot,movement_id}' is distinct from v_match->>'current_pay_head_movement_id'\n    or v_match#>'{native_payment_facts,paid}'"}
]);
compose('paid-match-office-confirm-consumer-construction-20261014','forward/01_paid_match_question.sqlinc','05_complete_office_question.sqlinc',[
 {name:'only a certified current monetary snapshot becomes a UI choice',before:"    if v_item#>'{native_payment_facts,paid}'",after:"    if v_item#>>'{current_pay_snapshot,state}' is distinct from 'CURRENT'\n      or v_item#>>'{current_pay_snapshot,snapshot_line_id}' is distinct from v_item->>'snapshot_line_id'\n      or v_item#>>'{current_pay_snapshot,movement_id}' is distinct from v_item->>'current_pay_head_movement_id'\n      or v_item#>'{native_payment_facts,paid}'"}
]);
compose('missing-retained-snapshot-mapping-successor-20261014','forward/09_complete_shared_current_capture_proposal.sqlinc','06_complete_shared_capture_proposal.sqlinc',[
 {name:'separate actual retained NONE provenance frame',before:'  v_origin_resolution uuid;v_missing_basis jsonb;',after:'  v_origin_resolution uuid;v_missing_basis jsonb;v_paid_basis jsonb;'},
 {name:'retained actual B and original pay snapshot assert before unchanged NO_CHANGE map',before:"    select s.movement_id into v_movement from private.bpay_next_source_current_snapshots s\n      where s.snapshot_line_id=v_t.previous_snapshot_line_id and s.scope_id=v_scope and s.work_event_id=v_t.work_event_id;",after:"    if v_t.pay_basis_kind='RETAINED_PAID_MATCH' then\n      v_paid_basis:=private.weekly_source_paid_match_retained_transition_assert_v1(v_t.id);\n    end if;\n    select s.movement_id into v_movement from private.bpay_next_source_current_snapshots s\n      where s.snapshot_line_id=v_t.previous_snapshot_line_id and s.scope_id=v_scope and s.work_event_id=v_t.work_event_id;\n    if v_paid_basis is not null and (v_paid_basis->>'snapshot_line_id' is distinct from v_t.previous_snapshot_line_id::text\n      or v_paid_basis->>'movement_id' is distinct from v_movement::text\n      or v_paid_basis->>'row_resolution_id' is distinct from v_origin_resolution::text\n      or v_paid_basis->>'lineage_id' is distinct from v_lineage::text\n      or v_paid_basis->>'economic_snapshot_id' is distinct from v_economic::text) then\n      raise exception 'BPAY_NEXT_SOURCE_RETAINED_PAID_BASIS_UNBOUND' using errcode='55000';end if;"}
]);
const commit='fa66183f01913e86651e82816f5e88f97e07123c',inputFiles=['supabase/migrations/04102026_0470_banking_pay_next_source_current_selector.sql','supabase/repeatable/04102026_0475_banking_pay_next_source_current_selector_v1.sql'];
fs.mkdirSync(path.join(dir,'input'),{recursive:true});const pins=[];for(const file of inputFiles){const b=execFileSync('git',['show',commit+':'+file],{cwd:root,maxBuffer:2e6}),saved='input/'+path.basename(file);fs.writeFileSync(path.join(dir,saved),b);pins.push({commit,file,saved,bytes:b.length,sha256:sha(b)});}
fs.writeFileSync(path.join(dir,'COMPLETE_CURRENT_REVERSES.json'),JSON.stringify({source_only:true,records,current_native_inputs:pins,shared_Banking_capture_proposal_not_canonical:true,SQL_compiled:false,native_proof:false},null,2)+'\n');
console.log(JSON.stringify({whole_owners:records.length,hunks:records.reduce((n,r)=>n+r.hunks.length,0),records:records.map(r=>({file:r.file,sha256:r.after_sha256}))}));
