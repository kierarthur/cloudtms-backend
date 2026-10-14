import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url));
const prior=path.resolve(dir,'../missing-default-current-native-proposal-20261014');
const sha=b=>createHash('sha256').update(b).digest('hex');
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')),'Frozen successor must not be replaced');
const oldManifest=fs.readFileSync(path.join(prior,'FROZEN_ARTIFACT_MANIFEST.json'));
assert.equal(sha(oldManifest),'4b80a17dab44b0e3f17357b3b2dde57478dfdba09f871c0ef3743222f711ad73');
const before=fs.readFileSync(path.join(prior,'forward/09_shared_current_capture_proposal.sqlinc'));
assert.equal(sha(before),'4a8e8d7fb15392a897b65a0045bd93d4a835aa2cb32041b64ec2b71d83da53b4');
const eol=before.includes(Buffer.from('\r\n'))?'\r\n':'\n';
const marker='  if v_t.new_snapshot_line_id is not null then';
const insertion=[
"  if v_t.outcome='CANCEL' and v_t.exclude_cancel then",
'    -- Record the genuine retained previous snapshot, including pre-NEXT',
'    -- history. This is the existing native provenance map, not a new/current',
'    -- snapshot or export-presence claim. The same original movement has just',
'    -- passed the immutable Source basis, exact scope and subject checks.',
'    insert into private.bpay_next_source_current_snapshots(snapshot_line_id,scope_id,work_event_id,movement_id)',
'      values(v_t.previous_snapshot_line_id,v_scope,v_t.work_event_id,v_movement) on conflict do nothing;',
'    if not exists(select 1 from private.bpay_next_source_current_snapshots s',
'      where s.snapshot_line_id=v_t.previous_snapshot_line_id and s.scope_id=v_scope',
'        and s.work_event_id=v_t.work_event_id and s.movement_id=v_movement) then',
"      raise exception 'BPAY_NEXT_SOURCE_SNAPSHOT_MAPPING_CHANGED' using errcode='55000';",
'    end if;',
'  end if;',
marker].join(eol);
const source=before.toString('utf8');assert.equal(source.split(marker).length,2);
const after=Buffer.from(source.replace(marker,insertion));
assert.equal(after.toString('utf8').split(insertion).length,2);
assert.deepEqual(Buffer.from(after.toString('utf8').replace(insertion,marker)),before);
fs.mkdirSync(path.join(dir,'before'),{recursive:true});fs.mkdirSync(path.join(dir,'forward'),{recursive:true});
fs.writeFileSync(path.join(dir,'before/09_shared_current_capture_proposal.sqlinc'),before);
fs.writeFileSync(path.join(dir,'forward/09_complete_shared_current_capture_proposal.sqlinc'),after);
const oldReverse=JSON.parse(fs.readFileSync(path.join(prior,'SHARED_CAPTURE_PROPOSED_COMPLETE_REVERSE.json')));
let restored=after.toString('utf8').replace(insertion,marker);
for(const h of [...oldReverse.hunks].reverse()) {
 const a=h.after.replaceAll('\n',eol),b=h.before.replaceAll('\n',eol);
 assert.equal(restored.split(a).length,2,h.name);restored=restored.replace(a,b);
}
assert.deepEqual(Buffer.from(restored),fs.readFileSync(path.join(prior,oldReverse.before_file)));
const inputs=['FROZEN_ARTIFACT_MANIFEST.json','SOURCE_CHECK_RESULT.json','SHARED_CAPTURE_PROPOSED_COMPLETE_REVERSE.json',oldReverse.before_file,'forward/02_missing_snapshot_movement.sqlinc','forward/07_missing_cancel_transition_assert.sqlinc'];
fs.writeFileSync(path.join(dir,'INPUT_PINS.json'),JSON.stringify({source_only:true,current_input_commit:'33927fff2f7b90b90241d746d9e35aa21c57bd07',native_input_file:oldReverse.input_file,prior_packet:path.basename(prior),inputs:inputs.map(file=>{const b=fs.readFileSync(path.join(prior,file));return {file,bytes:b.length,sha256:sha(b)};})},null,2)+'\n');
fs.writeFileSync(path.join(dir,'COMPLETE_REVERSE.json'),JSON.stringify({source_only:true,shared_banking_owner_change_proposed:true,shared_owner_approval:false,owner:oldReverse.owner,before_file:'before/09_shared_current_capture_proposal.sqlinc',before_bytes:before.length,before_sha256:sha(before),forward_file:'forward/09_complete_shared_current_capture_proposal.sqlinc',forward_bytes:after.length,forward_sha256:sha(after),hunks:[{name:'admitted genuine retained previous snapshot provenance',before:marker,after:insertion}],unique_hunks:1,whole_reverse_verified:true,chained_native_before_sha256:oldReverse.before_sha256,chained_native_before_bytes:oldReverse.before_bytes,chained_native_unique_hunks:5,chained_native_whole_reverse_verified:true,no_new_signature:true,no_schema_change:true,sql_compiled:false,native_proof:false},null,2)+'\n');
console.log(JSON.stringify({before_sha256:sha(before),after_sha256:sha(after),whole_reverse_verified:true,chained_native_whole_reverse_verified:true}));
