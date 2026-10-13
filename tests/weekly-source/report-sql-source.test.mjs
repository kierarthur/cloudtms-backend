import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';
import {closureFor,sqlFiles} from '../../scripts/cloudtms-db-release-lib.mjs';
const root='supabase/repeatable/13102026_1544_weekly_source_self_bill_report_scope_v1.sql';
const closure=closureFor(root);const source=closure.paths.map(p=>fs.readFileSync(p,'utf8')).join('\n');
const file=n=>fs.readFileSync('supabase/repeatable/13102026_1344_weekly_source_self_bill_report/13102026_1344_'+n+'.sqlinc','utf8');
test('one report repeatable root owns every included page without independent SQL inventory roots',()=>{
  assert(closure.paths.length>=21);assert(closure.paths.slice(1).every(p=>p.endsWith('.sqlinc')));
  assert(!sqlFiles('supabase/repeatable').some(p=>p.includes('13102026_1344_weekly_source_self_bill_report/')));
  assert(!fs.existsSync('supabase/repeatable/13102026_1344_weekly_source_self_bill_report_scope_v1.sql'));
});
test('report metadata remains immutable and has no final revision FK; bindings freeze exact export identity',()=>{
  const s=fs.readFileSync('supabase/migrations/13102026_1344_weekly_source_self_bill_report_schema.sql','utf8');
  assert.equal((s.match(/create table public\./g)||[]).length,2);
  assert(!s.slice(s.indexOf('create table public.weekly_source_self_bill_reports'),s.indexOf('create table public.weekly_source_self_bill_report_line_bindings')).includes('final_revision_id'));
  assert(s.includes('bound_row_resolution_id uuid not null references public.weekly_source_row_resolutions'));
  assert(s.includes('export_projection_publication_id uuid not null references public.weekly_source_projection_publications'));
  assert(file('guards').includes("to_jsonb(old)-array['state','superseded_at_utc']"));
});
test('NHSP conflict inference and resolver are separated by NULL report identity',()=>{
  assert(file('nhsp_scope').includes('where self_bill_report_id is null\n    do nothing'));
  assert(file('nhsp_scope').includes('and report_scope.self_bill_report_id is null;'));
});
test('same closed literal DTO and hash are used at registration, stage and persisted seal',()=>{
  assert(file('report_scope').includes("self_bill_report_manifest_v1(p_request->'rows')"));
  assert(file('upload_rows').includes('self_bill_report_row_hash_v1(v_item-array['));
  assert(file('seal_assert').includes('self_bill_report_manifest_v1(private.weekly_source_self_bill_report_stored_rows_v1'));
  assert(file('row_authority').includes("'external_source_key',v_row.external_source_key"));
  assert(file('row_authority').includes("'source_vat_pence',v_row.source_vat_pence"));
});
test('seal keeps native physical/evidence census and exempts only report from hours/coverage checks',()=>{
  const s=file('upload_seal');for(const token of ['WEEKLY_SOURCE_ROW_COUNTS_MISMATCH','WEEKLY_SOURCE_ACCEPTED_ROW_EVIDENCE_MISMATCH','WEEKLY_SOURCE_UPLOAD_HAS_BLOCKERS','weekly_source_self_bill_report_seal_assert_v1'])assert(s.includes(token));
  assert(s.includes("if v_profile.profile_code<>'ACACIUM_SELF_BILL_REPORT_V1' then"));
  assert(file('seal_assert').includes("r.break_minutes is not null or r.actual_net_minutes is not null"));
});
test('accepted report means exact CURRENT publication and binding never reads an arbitrary later resolution',()=>{
  const s=file('binding');assert(s.includes("rp.id=s.current_projection_publication_id"));assert(s.includes("rp.authority_scope_version=s.version"));
  assert(s.includes('rr.generation=coalesce(ep.projection_generation,ep.authority_scope_version)'));
  assert(file('pay_witness').includes('fs.row_resolution_id=rr.id'));assert(file('finalise').includes('where id=binding.bound_row_resolution_id'));
  assert(s.includes('Omission preserves any earlier current binding'));
  assert(!s.includes('order by rr.generation desc'));
});
test('four separate facts use qualified WAIT and actual pay publication; no protection => invoice readiness shortcut',()=>{
  const s=file('binding');assert(s.includes('weekly_source_protected_wait_witness_v1(e.id,c.id)'));
  assert(s.includes("'invoice_ready',v_bound and v_ready"));assert(s.includes("'s25_pay_selected',v_protected or (v_bound and v_pay_export_worked)"));
  assert(file('pay_witness').includes("receipt.outcome='PREPARED_FOR_AUTHORISATION'"));
  assert(!s.includes("f.c1_publication_state='LIVE'"));assert(!s.includes('entered_date'));
});
test('report finalisation preserves literal gross, full zero pay and bound origin key',()=>{
  const s=file('finalise');assert(s.includes('WEEKLY_SOURCE_REPORT_LITERAL_GROSS_MISMATCH'));
  assert(s.includes("weekly_source_invoice_only_vector_v1('PAY',0)"));assert(s.includes("'bound_row_resolution_id',binding.bound_row_resolution_id"));
  assert(!s.includes("'row_resolution_id',"));assert(s.includes("'snapshot_count',0,'transition_count',0,'expense_generation_count',0"));
  for(const forbidden of ['weekly_source_ordinary_pay_projection_apply_atomic_v1','weekly_source_finalisation_vector_v1','tsfin_write','pay_orchestrator'])assert(!s.includes(forbidden));
});
test('G1, F8 timing/floor and mandatory internal A/B are retained',()=>{
  const s=file('finalise');assert(s.indexOf('from public.weekly_source_groups')<s.indexOf('into strict v_cycle'));
  assert(s.includes('weekly_source_final_upload_assert_v1(v_upload.id)'));assert(file('upload_begin').includes('weekly_source_upload_coverage_floor_assert_v1'));
  assert(s.includes("p_vat_rounding_variant not in ('A','B')"));assert(!s.includes('p_vat_rounding_variant text default'));
});
test('every Correct Final arm refuses report scope before native mutation',()=>{
  const s=file('correct_final_refusals');assert.equal((s.match(/WEEKLY_SOURCE_SELF_BILL_REPORT_CORRECT_FINAL_UNSUPPORTED/g)||[]).length,7);
  for(const action of ['open','review','prepare','apply','cancel'])assert(s.includes('weekly_source_correct_final_'+action+'_atomic_v1'));
});
test('prior-linked negatives use immutable prior binding without a CURRENT export; duplicate invoice hold has one raw owner',()=>{
  const prior=file('prior_binding');assert(prior.includes("'resolution_id',b.bound_row_resolution_id"));
  assert(prior.includes("b.id=(m.source_facts_json->>'report_line_binding_id')::uuid"));
  assert(!prior.includes("export_publication.state='CURRENT'"));
  assert(file('binding').includes('v_prior:=private.weekly_source_self_bill_report_prior_binding_v1(line.id)'));
  const dupe=file('duplicate_hold');assert(dupe.includes("m.placement_state='PLACED'"));
  assert(dupe.includes("private.weekly_source_self_bill_report_duplicate_prior_v1(p_report_row_id)"));
  assert(dupe.includes("receipt.action='BILL_AS_FILE'"));
});
test('line-sum audit controls and frozen-rule invoice target controls are compared at distinct grains',()=>{
  const s=file('finalise');assert(s.indexOf('WEEKLY_SOURCE_REPORT_MOVEMENT_TOTALS_MISMATCH')<s.indexOf('weekly_source_vat_rounding_movement_insert_v1'));
  assert(s.includes('sum(vat_amount*100)=v_report.vat_pence::numeric+v_rounding_pence'));
  assert(s.includes('sum(total_inc_vat*100)=v_report.net_pence::numeric+v_report.vat_pence::numeric+v_rounding_pence'));
  assert(!s.includes('set vat_pence='));
});
test('PostgreSQL source parentheses and JSON object call argument limits',()=>{
  // Strip SQL literals/comments within each function body, then verify call
  // balance and PG's 100-argument bound. This does not claim SQL compilation.
  for(const m of source.matchAll(/\bas\s+(\$[\w]*\$)([^]*?)\1\s*;/gi)){
    const body=m[2].replace(/--[^\n]*/g,'').replace(/\/\*[^]*?\*\//g,'').replace(/'(?:''|[^'])*'/g,"''");
    const stack=[];let token='';
    for(const char of body){if(char==='('){stack.push({token:token.trim().match(/[\w.]+$/)?.[0],commas:0});token='';}
      else if(char===')'){assert(stack.length>0,'extra closing parenthesis');const call=stack.pop();if(call.token==='jsonb_build_object'||call.token==='pg_catalog.jsonb_build_object')assert(call.commas<100,'PG JSON object argument limit');token='';}
      else if(char===','){if(stack.length)stack.at(-1).commas++;token='';}else token+=char;}
    assert.equal(stack.length,0,'unclosed parentheses');
  }
});
