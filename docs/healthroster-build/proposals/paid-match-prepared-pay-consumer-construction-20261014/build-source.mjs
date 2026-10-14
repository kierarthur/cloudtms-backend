import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';import {fileURLToPath} from 'node:url';
import {functionEvents} from '../a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..'),base=path.resolve(dir,'../missing-default-current-native-proposal-20261014');
const commit='973625f17cd51a49803872a819638e10f13bbbff',sha=b=>createHash('sha256').update(b).digest('hex');
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')));fs.mkdirSync(path.join(dir,'before'),{recursive:true});fs.mkdirSync(path.join(dir,'forward'),{recursive:true});
const controlling=JSON.parse(fs.readFileSync(path.join(base,'CURRENT_COUPLED_OWNER_COMPLETE_REVERSES.json'))),records=[];
function compose(input,output,hunks){
 const prior=controlling.proofs.find(p=>p.forward_file===input);assert.ok(prior,input);
 const raw=execFileSync('git',['show',commit+':'+prior.input_file],{cwd:root,maxBuffer:3e6});
 const event=functionEvents(raw.toString()).events.find(e=>e.kind==='CREATE'&&e.id===prior.owner);assert.ok(event);
 assert.equal(sha(event.definition),prior.native_definition_sha256,'actual current native owner changed');
 const before=fs.readFileSync(path.join(base,input)).toString();assert.equal(sha(before),prior.forward_sha256);
 const eol=before.includes('\r\n')?'\r\n':'\n';hunks=hunks.map(h=>({...h,before:h.before.replaceAll('\n',eol),after:h.after.replaceAll('\n',eol)}));
 let after=before;for(const h of hunks){assert.equal(after.split(h.before).length,2,h.name);after=after.replace(h.before,h.after);}
 let reverse=after;for(const h of [...hunks].reverse()){assert.equal(reverse.split(h.after).length,2,h.name);reverse=reverse.replace(h.after,h.before);}assert.equal(reverse,before);
 const acl=`\nALTER FUNCTION ${prior.owner} OWNER TO postgres;\nREVOKE ALL ON FUNCTION ${prior.owner} FROM public,anon,authenticated${prior.owner.startsWith('private.')?',service_role':''};\n${prior.owner.startsWith('public.')?`GRANT EXECUTE ON FUNCTION ${prior.owner} TO service_role;\n`:''}`.replaceAll('\n',eol);
 fs.writeFileSync(path.join(dir,'before',output),before);fs.writeFileSync(path.join(dir,'forward',output),after+acl);
 records.push({commit,file:prior.input_file,input_bytes:raw.length,input_sha256:sha(raw),native_definition_sha256:sha(event.definition),signature:prior.owner,
   selected_input:'missing-default-current-native-proposal-20261014/'+input,output,before_sha256:sha(before),after_definition_sha256:sha(after),after_leaf_sha256:sha(after+acl),hunks,whole_reverse_verified:true});
}
compose('forward/15_complete_prepared_context_missing_census.sqlinc','02_complete_prepared_context.sqlinc',[
 {name:'native payable replacement root discovery excludes NONE components',before:"      where movement.source_profile_kind<>'SELF_BILL_REPORT' and movement.source_line_kind not in ('SELF_BILL_REPORT_LINE','INVOICE_VAT_ROUNDING','SOURCE_REBILL_LINE','SOURCE_INVOICE_CORRECTION')",after:"      where movement.source_profile_kind<>'SELF_BILL_REPORT' and movement.source_line_kind not in ('SELF_BILL_REPORT_LINE','INVOICE_VAT_ROUNDING','SOURCE_REBILL_LINE','SOURCE_INVOICE_CORRECTION') and movement.pay_effect='NORMAL'"},
 {name:'prepared context contains actual rederived immutable choice census',before:"    'missing_cancel_basis_census',private.weekly_source_missing_prepared_census_recheck_v1(v_revision.id,v_session.actor_user_id)) end;",after:"    'missing_cancel_basis_census',private.weekly_source_missing_prepared_census_recheck_v1(v_revision.id,v_session.actor_user_id)) end\n    ||case when v_revision.paid_overlap_choice_census='{}'::jsonb then '{}'::jsonb else jsonb_build_object(\n      'paid_overlap_choice_census',private.weekly_source_paid_match_prepared_census_recheck_v1(v_revision.id,v_session.actor_user_id)) end;"}
]);
compose('forward/16_complete_prepare_result_missing_census.sqlinc','03_complete_prepare_result.sqlinc',[
 {name:'native full prepare result hash binds its real census',before:"    'missing_cancel_basis_census',v_context->'missing_cancel_basis_census') else '{}'::jsonb end;",after:"    'missing_cancel_basis_census',v_context->'missing_cancel_basis_census') else '{}'::jsonb end\n    ||case when v_context ? 'paid_overlap_choice_census' then jsonb_build_object(\n      'paid_overlap_choice_census',v_context->'paid_overlap_choice_census') else '{}'::jsonb end;"}
]);
compose('forward/17_complete_apply_missing_census_recheck.sqlinc','04_complete_apply_census_recheck.sqlinc',[
 {name:'whole fresh prepared census must equal the native immutable prepare result',before:"      is distinct from coalesce(v_session.prepare_result_json->'missing_cancel_basis_census','{}'::jsonb) then",after:"      is distinct from coalesce(v_session.prepare_result_json->'missing_cancel_basis_census','{}'::jsonb)\n    or coalesce(v_prepared_context->'paid_overlap_choice_census','{}'::jsonb)\n      is distinct from coalesce(v_session.prepare_result_json->'paid_overlap_choice_census','{}'::jsonb) then"},
 {name:'real facts reproof after native complete root locks and before first own business writes',before:"  update public.weekly_final_source_correction_sessions\n  set state='COMMITTING',apply_idempotency_key=v_idempotency_key,",after:"  if v_prepared_revision.paid_overlap_choice_census<>'{}'::jsonb then\n    perform private.weekly_source_paid_match_prepared_census_recheck_v1(v_prepared_revision.id,v_actor);\n  end if;\n  update public.weekly_final_source_correction_sessions\n  set state='COMMITTING',apply_idempotency_key=v_idempotency_key,"}
]);
const expenseFile='supabase/repeatable/15092026_1534_weekly_source_ordinary_pay_projection_v1.sql';
const raw=execFileSync('git',['show',commit+':'+expenseFile],{cwd:root,maxBuffer:3e6});
const id='private.weekly_source_ordinary_projection_current_expenses_v1(uuid,uuid)',event=functionEvents(raw.toString()).events.find(e=>e.kind==='CREATE'&&e.id===id);assert.ok(event);
const before=event.definition,eol=before.includes('\r\n')?'\r\n':'\n';
const h={name:'PAY expense roots come actual payable native movements only',before:'    where movement.invoice_timesheet_id=p_root_timesheet_id',after:"    where movement.invoice_timesheet_id=p_root_timesheet_id\n      and movement.source_profile_kind<>'SELF_BILL_REPORT'\n      and movement.source_line_kind not in ('SELF_BILL_REPORT_LINE','INVOICE_VAT_ROUNDING','SOURCE_REBILL_LINE','SOURCE_INVOICE_CORRECTION')\n      and movement.pay_effect='NORMAL'".replaceAll('\n',eol)};
assert.equal(before.split(h.before).length,2);const after=before.replace(h.before,h.after);assert.equal(after.replace(h.after,h.before),before);
const acl=`\nALTER FUNCTION ${id} OWNER TO postgres;\nREVOKE ALL ON FUNCTION ${id} FROM public,anon,authenticated,service_role;\n`.replaceAll('\n',eol);
fs.writeFileSync(path.join(dir,'before/05_complete_current_pay_expenses.sqlinc'),before);fs.writeFileSync(path.join(dir,'forward/05_complete_current_pay_expenses.sqlinc'),after+acl);
records.push({commit,file:expenseFile,input_bytes:raw.length,input_sha256:sha(raw),signature:id,output:'05_complete_current_pay_expenses.sqlinc',before_sha256:sha(before),after_definition_sha256:sha(after),after_leaf_sha256:sha(after+acl),hunks:[h],whole_reverse_verified:true});
fs.writeFileSync(path.join(dir,'COMPLETE_CURRENT_REVERSES.json'),JSON.stringify({source_only:true,records,actual_current_native_prepared_owners_match:true,
 original_native_replay_actor_root_cash_guards_retained:true,financial_calculators_changed:false,new_public_signature:false,
 complete_pay_consumer_census:false,CorrectFinal_paid_choice_admission_complete:false,canonical_mounted:false,SQL_compiled:false,native_proof:false,full_primary_complete:false},null,2)+'\n');
console.log(JSON.stringify(records.map(r=>({signature:r.signature,hunks:r.hunks.length,sha256:r.after_leaf_sha256}))));
