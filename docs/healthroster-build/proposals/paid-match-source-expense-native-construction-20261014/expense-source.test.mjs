import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import test from 'node:test';
import {createHash} from 'node:crypto';import {fileURLToPath} from 'node:url';import {execFileSync} from 'node:child_process';
import {scanManagedSql} from '../strict-qualification-install1-forward/PINNED_MANAGED_SQL_SCANNER.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..'),read=f=>fs.readFileSync(path.join(dir,f),'utf8'),sha=b=>createHash('sha256').update(b).digest('hex');
test('single exact native expense VAT/vector block is extracted and full writer reverses',()=>{
 const r=JSON.parse(read('NATIVE_VALUES_REVERSE.json'));let a=read('forward/02_complete_native_expense_materialiser.sqlinc');assert.equal(sha(a),r.after_leaf_sha256);
 a=a.slice(0,a.indexOf('\nALTER FUNCTION'));assert.equal(sha(a),r.after_definition_sha256);
 for(const h of [...r.hunks].reverse()){assert.equal(a.split(h.after).length,2);a=a.replace(h.after,h.before);}
 assert.equal(a,read('before/02_complete_native_expense_materialiser.sqlinc'));assert.equal(sha(a),r.before_sha256);
 const block=read('input/exact_native_expense_values.sqlinc');assert.equal(sha(block),r.extracted_native_block_sha256);
 const kernel=block.replaceAll('v_current.source_expense_vat_enabled','v_expense_policy.source_expense_vat_enabled').replaceAll('\r\n','\n');
 assert.ok(read('forward/01_native_expense_invoice_values.sqlinc').includes(kernel));
 assert.ok(read('forward/02_complete_native_expense_materialiser.sqlinc').indexOf('v_expense_policy.source_expense_pence is distinct from v_current.source_expense_pence')<read('forward/02_complete_native_expense_materialiser.sqlinc').indexOf('v_values:='));
});
test('five complete successor owners/constraints raw reverse without unrelated edits',()=>{
 const d=JSON.parse(read('EXPENSE_WRITER_REVERSES.json'));assert.equal(d.records.length,5);
 for(const r of d.records){let a=read(r.output);assert.equal(sha(a),r.after_sha256);for(const h of [...r.hunks].reverse()){assert.equal(a.split(h.after).length,2);a=a.replace(h.after,h.before);}const b=read('before/'+path.basename(r.output));assert.equal(a,b);assert.equal(sha(b),r.before_sha256);}
});
test('true NONE-only presence retains payable expense history; no new omitted-zero generation',()=>{
 const s=read('forward/07_complete_fin4_expense.sqlinc');assert.doesNotMatch(s,/WEEKLY_SOURCE_PAID_MATCH_EXPENSE_COMPOSITION_REQUIRED/);
 const i=s.indexOf('-- Explicit real Same rows');const end=s.indexOf('v_expense_policy_id:=',i);assert.ok(i>=0&&end>i);
 const arm=s.slice(i,end);assert.match(arm,/companion\.pay_effect='NONE'/);assert.match(arm,/payable\.pay_effect='NORMAL'/);assert.match(arm,/continue;/);assert.doesNotMatch(arm,/update |insert |ROW_OMITTED/);
 assert.match(s,/resolution\.pay_effect='NORMAL'\s*\), prior_authority/);
});
test('ordinary payable expense authority and native replacement/reversal remain full old owner',()=>{
 const s=read('forward/02_complete_native_expense_materialiser.sqlinc');for(const code of ['WEEKLY_SOURCE_PREPARED_EXPENSE_HISTORY_INVALID','WEEKLY_SOURCE_PRIOR_EXPENSE_MOVEMENT_MISSING','EXPENSE_REVERSAL','EXPENSE_REPLACEMENT','v_reversal_vector','-v_prior_movement.vat_amount'])assert.ok(s.includes(code),code);
 assert.equal((s.match(/weekly_source_fixed_expense_invoice_values_v1\(/g)??[]).length,1);
});
test('positive companion uses native snapshot values and genuinely zero PAY; no payable generation',()=>{
 const s=read('forward/03_expense_invoice_companion.sqlinc');assert.match(s,/weekly_source_fixed_expense_invoice_values_v1\(p\.id\)/);assert.match(s,/jsonb_build_object\('source_expense_pence',0\)/);
 assert.match(s,/if p\.source_expense_pence=0 then return null/);assert.match(s,/weekly_source_finalisation_expense_policy_assert_v1/);
 assert.doesNotMatch(s,/insert into public\.weekly_expense_authority_generations|update public\.weekly_expense_authority_generations|set paid_at|(?:insert into|update) public\.(?:cash|settlement|pay_batch)/i);
});
test('two genuine raw components replay separately without ambiguous snapshot-only lookup',()=>{
 const h=read('forward/05_complete_hours_companion.sqlinc'),e=read('forward/03_expense_invoice_companion.sqlinc');
 assert.match(h,/m\.invoice_only_snapshot_line_id=s\.id and m\.source_line_kind='SOURCE_REBILL_LINE'/);
 assert.match(e,/invoice_only_snapshot_line_id=s\.id and source_line_kind='SOURCE_FIXED_EXPENSE'/);
 assert.match(read('one-time/08_complete_snapshot_origin_constraints.sqlinc'),/invoice_only_snapshot_line_id,source_line_kind/);
});
test('historical expense origin binds real HOURS proof and complete immutable policy tuple',()=>{
 const s=read('forward/04_expense_invoice_origin_assert.sqlinc');for(const marker of ['weekly_source_paid_match_invoice_origin_assert_v1(h.id)','p.row_resolution_id','p.upload_row_id','p.work_event_id','p.candidate_id','p.client_id','p.contract_id','s.source_expense_pence','m.movement_economic_hash','m.invoice_timesheet_id','m.source_facts_json->\'invoice_values\''])assert.ok(s.includes(marker),marker);
 assert.doesNotMatch(s,/paid.*:=|paid_at|UPDATE\s|INSERT\s/i);
});
test('whole origin dispatch is literal and expense-to-hours delegation cannot recurse by component',()=>{
 const d=read('forward/06_complete_origin_dispatch.sqlinc'),e=read('forward/04_expense_invoice_origin_assert.sqlinc');
 assert.match(d,/m\.invoice_only_snapshot_line_id is not null and m\.source_line_kind='SOURCE_FIXED_EXPENSE'/);
 assert.match(e,/h\.source_line_kind<>'SOURCE_REBILL_LINE'/);
});
test('existing native invoice binding fields derive the actual same-snapshot buddy',()=>{
 const s=read('forward/09_complete_invoice_admission.sqlinc');assert.match(s,/hours_binding\.presentation_line_id=shift_line\.id/);assert.match(s,/hours_binding\.state='CURRENT' and hours_binding\.invoice_id=v_invoice_id/);
 assert.match(s,/hours_movement\.invoice_only_snapshot_line_id=v_movement\.invoice_only_snapshot_line_id/);assert.doesNotMatch(s,/weekly_source_invoice_manifest_members/);
 const actual=execFileSync('git',['show','973625f17cd51a49803872a819638e10f13bbbff:supabase/repeatable/14102026_0541_healthroster_report_current_native_composition/15_complete_native_report_admission.sqlinc'],{cwd:root,maxBuffer:2e6}).toString();
 for(const column of ['billing_movement_id','presentation_line_id','invoice_id','state'])assert.ok(actual.includes(column),column);
});
test('new Source expense functions retain owner-private ACLs and closed native definitions',()=>{
 for(const file of ['01_native_expense_invoice_values.sqlinc','03_expense_invoice_companion.sqlinc','04_expense_invoice_origin_assert.sqlinc']){const s=read('forward/'+file);assert.match(s,/set search_path to 'pg_catalog','pg_temp'/);assert.match(s,/from public,anon,authenticated,service_role/);assert.doesNotMatch(s,/grant execute|create table/i);}
});
test('schema preserves the six genuine origins and distinguishes zero PAY vector grains',()=>{
 const s=read('one-time/08_complete_snapshot_origin_constraints.sqlinc');assert.match(s,/\+\(invoice_only_snapshot_line_id is not null\)::integer=1/);assert.match(s,/WEEKLY_SOURCE_FIXED_EXPENSE_VECTOR_V1/);assert.match(s,/canonical_pay_vector_json->>'total_pence'='0'/);assert.match(s,/source_line_kind='SOURCE_FIXED_EXPENSE' and movement_role='EXPENSE_POSITIVE'/);
});
test('all constructed source SQL leaves are finite and contain no external effects',()=>{
 for(const side of ['forward','one-time'])for(const f of fs.readdirSync(path.join(dir,side)).filter(f=>f.endsWith('.sqlinc'))){const s=read(side+'/'+f),p=scanManagedSql(s);assert.ok(p.statements.length);assert.equal(p.meta.length,0);assert.doesNotMatch(s,/\bCOPY\b|\bdblink\b|\bhttp_post\b|pg_read_file|ALTER ROLE|ALTER SYSTEM/i);}
});
