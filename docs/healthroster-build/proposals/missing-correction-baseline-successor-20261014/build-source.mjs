import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {spawnSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),prior=path.resolve(dir,'../missing-default-current-native-proposal-20261014'),root=path.resolve(dir,'../../../..'),sha=b=>createHash('sha256').update(b).digest('hex');
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')),'Never replace frozen successor');
assert.equal(sha(fs.readFileSync(path.join(prior,'FROZEN_ARTIFACT_MANIFEST.json'))),'4b80a17dab44b0e3f17357b3b2dde57478dfdba09f871c0ef3743222f711ad73');
const proofs=[];
function compose(file,hunks){const before=fs.readFileSync(path.join(prior,'forward',file)),eol=before.includes(Buffer.from('\r\n'))?'\r\n':'\n';let text=before.toString('utf8');const actual=hunks.map(h=>({...h,before:h.before.replaceAll('\n',eol),after:h.after.replaceAll('\n',eol)}));for(const h of actual){assert.equal(text.split(h.before).length,2,h.name);text=text.replace(h.before,h.after);}const after=Buffer.from(text);for(const h of [...actual].reverse()){assert.equal(text.split(h.after).length,2,h.name);text=text.replace(h.after,h.before);}assert.deepEqual(Buffer.from(text),before);fs.mkdirSync(path.join(dir,'before'),{recursive:true});fs.writeFileSync(path.join(dir,'before',file),before);fs.writeFileSync(path.join(dir,'forward',file),after);proofs.push({file,before_bytes:before.length,before_sha256:sha(before),after_bytes:after.length,after_sha256:sha(after),hunks:actual,unique_hunks:actual.length,whole_reverse_verified:true});}
compose('03_missing_cancel_basis.sqlinc',[
 {name:'correction proof local',before:'  v_context jsonb; v_original jsonb; v_result jsonb;',after:'  v_context jsonb; v_original jsonb; v_result jsonb; v_correction_baseline jsonb;'},
 {name:'retain strict ordinary/protected position while admitting actual unpaid correction baseline',before:`  v_original:=private.weekly_source_missing_snapshot_movement_v1(v_snapshot.id);
  if v_original->>'movement_id' is distinct from v_context->>'current_pay_head_movement_id' then
    raise exception 'WEEKLY_SOURCE_MISSING_CANCEL_NATIVE_POSITION_CHANGED' using errcode='40001';
  end if;
  v_disposition:=case`,after:`  v_original:=private.weekly_source_missing_snapshot_movement_v1(v_snapshot.id);
  v_disposition:=case`},
 {name:'native correction proof before existing basis',before:`  v_result:=jsonb_build_object('schema_version','WEEKLY_SOURCE_MISSING_CANCEL_BASIS_V1',`,after:`  if v_original->>'movement_id' is distinct from v_context->>'current_pay_head_movement_id' then
    if v_disposition is distinct from 'CANCEL_NOT_PAID_OR_INVOICED' then
      raise exception 'WEEKLY_SOURCE_MISSING_CANCEL_NATIVE_POSITION_CHANGED' using errcode='40001';
    end if;
    v_correction_baseline:=private.weekly_source_missing_correction_baseline_v1(
      v_publication.id,v_snapshot.id,p_actor_user_id);
  end if;
  v_result:=jsonb_build_object('schema_version','WEEKLY_SOURCE_MISSING_CANCEL_BASIS_V1',`},
 {name:'freeze genuine proof only for correction mismatch',before:`  v_result:=v_result||jsonb_build_object('basis_sha256',encode(private.weekly_source_sha256_jsonb_v1(`,after:`  if v_correction_baseline is not null then
    v_result:=v_result||jsonb_build_object('correction_baseline',v_correction_baseline);
  end if;
  v_result:=v_result||jsonb_build_object('basis_sha256',encode(private.weekly_source_sha256_jsonb_v1(`}
]);
compose('14_missing_prepared_census_recheck.sqlinc',[
 {name:'reprove whole correction baseline/current-parent before existing prepared disposition',before:`    if v_transition.exclude_cancel then`,after:`    if v_basis ? 'correction_baseline' then
      if v_transition.exclude_cancel
        or v_entry.value->>'disposition' is distinct from 'CANCEL_NOT_PAID_OR_INVOICED'
        or private.weekly_source_missing_correction_baseline_v1(
          (v_basis->>'projection_publication_id')::uuid,v_transition.previous_snapshot_line_id,p_actor_user_id)
          is distinct from v_basis->'correction_baseline' then
        raise exception 'WEEKLY_SOURCE_MISSING_PREPARED_CORRECTION_BASELINE_CHANGED' using errcode='40001';
      end if;
    end if;
    if v_transition.exclude_cancel then`}
]);
fs.writeFileSync(path.join(dir,'COMPLETE_REVERSES.json'),JSON.stringify({source_only:true,prior_packet:path.basename(prior),complete_reverses:2,unique_hunks:5,proofs,new_private_signature:'private.weekly_source_missing_correction_baseline_v1(uuid,uuid,uuid)',new_public_signatures:0,banking_owner_changed:false,sql_compiled:false,native_proof:false},null,2)+'\n');
const commit='33927fff2f7b90b90241d746d9e35aa21c57bd07';const files=['supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/10c_complete_root_report_consumer_union.sqlinc','supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/09_complete_pay_consumers.sqlinc','supabase/repeatable/21072026_1235_06_import_timesheet_financial_preflight_v1.sql','supabase/repeatable/14102026_0821_healthroster_fin4_rebill_record_fields/01_complete_fin4.sqlinc'];
const pins=files.map(file=>{const r=spawnSync('git',['show',commit+':'+file],{cwd:root,maxBuffer:4e6});assert.equal(r.status,0,r.stderr.toString());return {commit,file,bytes:r.stdout.length,sha256:sha(r.stdout)};});
const oldFiles=['FROZEN_ARTIFACT_MANIFEST.json','SOURCE_CHECK_RESULT.json','forward/03_missing_cancel_basis.sqlinc','forward/14_missing_prepared_census_recheck.sqlinc','forward/06_complete_fin4_default_draft.sqlinc','forward/01_target_publication_context.sqlinc','before/private.weekly_source_ordinary_projection_active_movements_v1_uuid_uuid_.sqlinc'];
fs.writeFileSync(path.join(dir,'INPUT_PINS.json'),JSON.stringify({source_only:true,current_commit:commit,native_inputs:pins,prior_packet:path.basename(prior),prior_inputs:oldFiles.map(file=>{const b=fs.readFileSync(path.join(prior,file));return {file,bytes:b.length,sha256:sha(b)};})},null,2)+'\n');
console.log(JSON.stringify({proofs:proofs.map(p=>({file:p.file,after_sha256:p.after_sha256})),complete_reverses:2,unique_hunks:5,native_inputs:pins.length}));
