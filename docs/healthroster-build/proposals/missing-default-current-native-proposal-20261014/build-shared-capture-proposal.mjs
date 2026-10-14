import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),sha=b=>createHash('sha256').update(b).digest('hex');assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')));
const b='before/private.bpay_next_source_current_capture_transition_v1_uuid_.sqlinc',f='forward/09_shared_current_capture_proposal.sqlinc';
const bytes=fs.readFileSync(path.join(dir,b));assert.equal(sha(bytes),'2fa0c952b3031e409a56800a78f44040b3cc966c4c4f57b4ed7dc7870f5127fc');
// The selected-input writer retains the exact native declaration and adds
// one final LF as a support-leaf delimiter. Pin both, never equate their SHA.
assert.equal(sha(bytes.subarray(0,-1)),'cd7b376b618793bba2bc38e89a401f1eaef423c4f61689b56921d16ee2ca15bf');
const old=bytes.toString('utf8'),nl=old.includes('\r\n')?'\r\n':'\n',norm=s=>s.replaceAll('\r\n','\n').replaceAll('\n',nl);let source=old;const hunks=[];
function replace(name,before,after){before=norm(before);after=norm(after);assert.equal(source.split(before).length,2,name);assert.notEqual(before,after);source=source.replace(before,after);assert.equal(source.split(after).length,2,name+' after');hunks.push({name,before,after});}
replace('native origin resolution local','  v_lineage uuid;v_economic uuid;v_inserted boolean;v_reversal public.weekly_source_billing_movements%rowtype;',
 `  v_lineage uuid;v_economic uuid;v_inserted boolean;v_reversal public.weekly_source_billing_movements%rowtype;
  v_origin_resolution uuid;v_missing_basis jsonb;`);
replace('preserve original normal resolution','  if v_t.outcome in (\'ADD\',\'AMEND\') then',
 `  v_origin_resolution:=v_line.row_resolution_id;
  if v_t.outcome in ('ADD','AMEND') then`);
replace('exact excluded absent CANCEL native capture',`    if v_movement is null then raise exception 'BPAY_NEXT_SOURCE_PREVIOUS_SNAPSHOT_UNCAPTURED' using errcode='55000';end if;
  end if;
  if v_t.outcome in ('AMEND','CANCEL') then`,
 `    if v_movement is null then raise exception 'BPAY_NEXT_SOURCE_PREVIOUS_SNAPSHOT_UNCAPTURED' using errcode='55000';end if;
  elsif v_t.outcome='CANCEL' and v_t.exclude_cancel then
    -- Shared Banking owner proposal, not approved or mounted. The real
    -- Source transition stays absent: new_present=false and new snapshot=NULL.
    -- Financial selection retains exactly its immutable original basis.
    v_missing_basis:=private.weekly_source_missing_cancel_transition_assert_v1(v_t.id);
    v_movement:=(v_missing_basis#>>'{original_basis,movement_id}')::uuid;
    v_origin_resolution:=(v_missing_basis#>>'{original_basis,row_resolution_id}')::uuid;
    v_lineage:=(v_missing_basis#>>'{original_basis,lineage_id}')::uuid;
    v_economic:=(v_missing_basis#>>'{original_basis,economic_snapshot_id}')::uuid;
    if private.bpay_next_source_current_scope_v1(v_origin_resolution,v_revision.source_group_id,v_t.work_event_id,v_line.contract_id)
      is distinct from v_scope then
      raise exception 'BPAY_NEXT_SOURCE_MISSING_CANCEL_SCOPE_CHANGED' using errcode='55000';
    end if;
  end if;
  if v_t.outcome in ('AMEND','CANCEL') and not v_t.exclude_cancel then`);
replace('actual old resolution in retained financial origin','      v_t.transition_fingerprint,v_line.row_resolution_id,v_lineage,v_economic,v_movement,v_t.new_snapshot_line_id,v_t.created_at_utc)',
 '      v_t.transition_fingerprint,v_origin_resolution,v_lineage,v_economic,v_movement,v_t.new_snapshot_line_id,v_t.created_at_utc)');
let reversed=source;for(const h of [...hunks].reverse()){assert.equal(reversed.split(h.after).length,2,h.name+' reverse');reversed=reversed.replace(h.after,h.before);}assert.equal(reversed,old);
fs.writeFileSync(path.join(dir,f),source);
fs.writeFileSync(path.join(dir,'SHARED_CAPTURE_PROPOSED_COMPLETE_REVERSE.json'),JSON.stringify({source_only:true,proposal_only:true,shared_banking_owner_change_proposed:true,shared_owner_approval:false,canonical_modified:false,input_current_commit:'33927fff2f7b90b90241d746d9e35aa21c57bd07',input_original_commit:'5db7abee7925fffbeaf7c4e885cce809980fa079',input_file:'supabase/repeatable/04102026_0475_banking_pay_next_source_current_selector_v1.sql',owner:'private.bpay_next_source_current_capture_transition_v1(uuid)',before_file:b,before_bytes:bytes.length,before_sha256:sha(bytes),forward_file:f,forward_bytes:Buffer.byteLength(source),forward_sha256:sha(source),unique_hunks:hunks.length,hunks,whole_reverse_verified:true,unchanged:['native refresh financial source_present semantics','native event/page ABI and original monetary queries','ordinary ADD/AMEND/NO_CHANGE/unexcluded CANCEL','original absence new_present=false/new_snapshot=NULL','original invoice/settlement/calculator authorities'],sql_compiled:false,native_proof:false},null,2)+'\n');
console.log(JSON.stringify({proposal_only:true,shared_banking_owner_approval:false,whole_reverse:true,hunks:hunks.length}));
