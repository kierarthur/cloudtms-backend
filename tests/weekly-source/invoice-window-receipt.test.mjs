import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {createHash} from 'node:crypto';
import {validateFrozenInvoicePresentationModel,validateZeroRoundingSuppressions} from '../../broker/src/invoice-presentation-contract.js';
import {buildProfessionalInvoiceHtml,renderInvoiceLineTable} from '../../broker/src/invoice-document-templates.js';
import {validateLegacyInvoiceManifestSuppressions} from '../../broker/src/weekly-source/invoice-render-qualification.js';
import {invoiceQueueRuntimeInternals} from '../../broker/src/invoice-queue-runtime.js';
const base='docs/healthroster-build/proposals/f5-window-receipt-complete/generated/';
const sql=name=>{
  if(name==='01_schema.sqlinc') return fs.readFileSync('supabase/migrations/13102026_1680_weekly_source_invoice_window_receipt_schema.sql','utf8');
  if(name==='13b_complete_acl_helper_service_union.sqlinc') return fs.readFileSync('supabase/repeatable/13102026_1436_weekly_source_invoice_money_acl_inventory_v1.sql','utf8');
  if(name.endsWith('.sqlinc')) return fs.readFileSync('supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/'+name,'utf8');
  return fs.readFileSync(base+name,'utf8');
};
const id=n=>`f5530000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const hash=n=>String(n).padStart(64,'0');
const witness=()=>({contract:'WEEKLY_SOURCE_ZERO_ROUNDING_SUPPRESSION_V1',invoice_id:id(1),invoice_line_id:id(99),
  presentation_line_id:id(90),presentation_hash:hash(1),source_hash:hash(2),rounding_authority_sha256:hash(3),total_pay_pence:0,net_pence:0,vat_pence:0,gross_pence:0});
const model=()=>({schema_version:'INVOICE_RENDER_MODEL_V1',purpose:'FINAL_ISSUE',document_type:'SELF_BILL_INVOICE',invoice_id:id(1),
  invoice_number:'SB-WINDOW-1',issue_date:'2026-10-29',tax_point:'2026-10-29',due_date:'2026-11-28',currency:'GBP',
  supplier:{legal_name:'Fixture agency',registered_address:['Fixture address'],vat_registration_number:'GB123456789'},
  customer:{legal_name:'Fixture client',billing_address:['Fixture client address']},references:{},
  lines:[{row_key:'source-1',source_invoice_line_id:id(2),source_key:'source-1',description:'Genuine source shift',worker:'Fixture worker',
    week_ending_date:'2026-10-25',reference:'LEXICAL-001',unit:'HOUR',quantity:'1',unit_price:'2',net_amount:'2.00',vat_rate:'20',vat_amount:'0.40',gross_amount:'2.40',display_order:1}],
  totals:{net:'2.00',vat:'0.40',gross:'2.40'},vat_breakdown:[{rate:'20',net_amount:'2.00',vat_amount:'0.40',gross_amount:'2.40'}],
  payment:{terms_text:'30 days from invoice date'},credit_note:{is_credit_note:false},self_bill:{is_self_bill:true,legal_wording:'Self-bill invoice'},legal_wording:[],template_version:'invoice-professional-v2'});

test('complete native model and HTML accept qualified omitted zero authority with unchanged totals',()=>{
  const m=model();m.source_rounding_suppressions=[witness()];
  assert.equal(validateFrozenInvoicePresentationModel(m),m);
  const html=buildProfessionalInvoiceHtml(m);
  assert.match(html,/Genuine source shift/);assert.doesNotMatch(html,/VAT rounding/);
  assert.match(html,/£2\.40/);assert.equal(m.lines.length,1);
  assert.equal(m.source_rounding_suppressions[0].invoice_line_id,id(99));
});
test('ordinary zero line stays visible and D0/no-hold model shape stays exactly the original shape',()=>{
  const m=model();const original=structuredClone(m);validateZeroRoundingSuppressions(m);assert.deepEqual(m,original);
  m.lines.push({...m.lines[0],row_key:'ordinary-zero',source_invoice_line_id:id(3),source_key:'ordinary-zero',
    description:'Ordinary zero line',quantity:'0',unit_price:'0',net_amount:'0',vat_amount:'0',gross_amount:'0',display_order:2});
  assert.match(buildProfessionalInvoiceHtml(m),/Ordinary zero line/);
  assert.equal(m.lines.length,2);
});
test('nonzero positive or negative ROUND authority remains a visible native line with exact totals',()=>{
  for(const delta of ['0.10','-0.10']){
    const m=model(),vat=(0.4+Number(delta)).toFixed(2),gross=(2.4+Number(delta)).toFixed(2);
    m.lines.push({...m.lines[0],row_key:'round',source_invoice_line_id:id(99),source_key:'round',description:'VAT rounding (matches the client self-bill)',
      quantity:'1',unit_price:'0',net_amount:'0',vat_amount:delta,gross_amount:delta,display_order:2});
    m.totals.vat=vat;m.totals.gross=gross;m.vat_breakdown[0].vat_amount=vat;m.vat_breakdown[0].gross_amount=gross;
    assert.match(buildProfessionalInvoiceHtml(m),/VAT rounding/);assert.equal(m.lines.length,2);
  }
});
test('suppression shape, foreign identity, money, duplicated witness, visible same-ID and forged hashes fail closed in validator and direct PDF table',()=>{
  const mutations=[w=>({...w,gross_pence:1}),w=>({...w,invoice_id:id(7)}),w=>({...w,source_hash:'bad'}),w=>({...w,extra:true}),w=>({...w,contract:'OTHER'})];
  for(const change of mutations){const m=model();m.source_rounding_suppressions=[change(witness())];
    assert.throws(()=>validateFrozenInvoicePresentationModel(m),/ZERO_ROUNDING_SUPPRESSION_INVALID/);
    assert.throws(()=>renderInvoiceLineTable(m),/ZERO_ROUNDING_SUPPRESSION_INVALID/);
  }
  const m=model();m.source_rounding_suppressions=[witness(),witness()];assert.throws(()=>renderInvoiceLineTable(m),/ZERO_ROUNDING_SUPPRESSION_INVALID/);
  m.source_rounding_suppressions=[{...witness(),invoice_line_id:id(2)}];assert.throws(()=>renderInvoiceLineTable(m),/ZERO_ROUNDING_SUPPRESSION_INVALID/);
});
test('legacy actual owner projection requires the same exact witness and never hides a still-present zero line',()=>{
  const manifest={invoice:{id:id(1)},lines:[{id:id(2)}],source_rounding_suppressions:[witness()]};
  assert.equal(validateLegacyInvoiceManifestSuppressions(manifest),manifest);
  manifest.lines.push({id:id(99)});assert.throws(()=>validateLegacyInvoiceManifestSuppressions(manifest),/ZERO_ROUNDING_SUPPRESSION_INVALID/);
});
test('native frozen-model hash covers suppression witness identity; changed receipt hash cannot replay an old FINAL_ISSUE model hash',async()=>{
  const m=model();m.source_rounding_suppressions=[witness()];
  const hashModel=value=>createHash('sha256').update(invoiceQueueRuntimeInternals.postgresJsonbText(value)).digest('hex');
  const expected=hashModel(m);const changed=structuredClone(m);changed.source_rounding_suppressions[0].rounding_authority_sha256=hash(4);
  assert.notEqual(hashModel(changed),expected);
  await assert.rejects(()=>invoiceQueueRuntimeInternals.verifyFrozenPresentationModelHash('INVOICE_CORE',changed,
    {presentation_model_schema_version:'INVOICE_RENDER_MODEL_V1',presentation_model_hash:expected,template_version:'invoice-professional-v2'}),/RENDER_MODEL_HASH_MISMATCH/);
});
test('FIN complete candidate-window discovery precedes unchanged native I-1, then group and all sorted cycle locks before first revision INSERT',()=>{
  const fin=sql('04_complete_fin.sqlinc');
  const discovery=fin.indexOf('v_window_observed:=private.weekly_source_invoice_window_lock_inventory_v1');
  const inventory=fin.indexOf('v_finalisation_lock_operation:=private.bpay_next_source_current_lock_inventory_v1');
  assert.ok(discovery>=0&&inventory>discovery);
  assert.match(fin,/for v_window_lock in select cycle\.\*.*?order by cycle\.id for update/s);
  const group=fin.indexOf("where id=(v_lock_discovery#>>'{group,id}')::uuid for update"),cycles=fin.indexOf('for v_window_lock');
  assert.ok(group>=0&&cycles>group);
  assert.match(fin,/v_window_observed is distinct from v_window_discovery/);assert.match(fin,/WINDOW_SET_GREW_AFTER_LOCK/);
  const round=fin.indexOf('v_round_id:=private.weekly_source_vat_rounding_movement_insert_v1'),manifest=fin.indexOf('insert into public.weekly_source_client_manifests');
  assert.ok(round>=0&&manifest>round);assert.match(fin,/coalesce\(later_manifest\.invoice_window_cycle_id,later_manifest\.source_cycle_id\)=later_manifest\.source_cycle_id/);
});
test('complete admission/allocation/issue freeze/rebuild receipt authority and preserve native net/cardinality/total checks',()=>{
  const admit=sql('05_complete_admission.sqlinc'),validator=sql('06_complete_allocation_issue.sqlinc');
  assert.match(admit,/invoice_rounding_authority_json/);assert.match(admit,/v_receipt_map:=private\.weekly_source_invoice_admission_receipt_map_v1/);
  assert.match(admit,/'PLACED',true,v_invoice_window.id,v_invoice_window.id/);
  assert.match(admit,/v_manifest.final_revision_id,v_manifest.source_cycle_id/);
  assert.match(validator,/weekly_source_invoice_receipt_projection_assert_v1/);assert.match(validator,/weekly_source_invoice_admitted_member_money_v1/);
  assert.match(validator,/member_count not in \(1,2\)/);assert.match(validator,/WEEKLY_SOURCE_INVOICE_TOTAL_MISMATCH/);
  const owners=sql('03_receipt_and_window_owners.sqlinc');assert.match(owners,/Explicit original money/);
  assert.match(owners,/ROUNDING_RECEIPT_PRESENTATION_SET_INVALID/);assert.match(owners,/PRESENTATION_HASH_INVALID/);
});
test('all controlling Source pay selectors are canonical manifests and no manifest alias asks for movement-only fields',()=>{
  const pay=sql('09_complete_pay_consumers.sqlinc');
  assert.match(pay,/coalesce\(manifest.invoice_window_cycle_id,manifest.source_cycle_id\)=manifest.source_cycle_id/);
  assert.doesNotMatch(pay,/weekly_source_client_manifests (?:m|cm) where (?:m|cm)\.source_profile_kind/);
  assert.match(pay,/SELF_BILL_REPORT_LINE','INVOICE_VAT_ROUNDING','SOURCE_REBILL_LINE','SOURCE_INVOICE_CORRECTION/);
  const root=sql('10c_complete_root_report_consumer_union.sqlinc');assert.match(root,/SELF_BILL_REPORT_CORRECT_FINAL_UNSUPPORTED/);
});
test('complete native assembler qualifies before split, freezes supporting receipts and FINAL_ISSUE transport never rebuilds its stored snapshot',()=>{
  const snapshot=sql('11_complete_native_snapshot_assembler.sqlinc'),transport=sql('12_retained_final_issue_transport.sqlinc');
  assert.ok(snapshot.indexOf('source_rounding_suppression as materialized')<snapshot.indexOf('invoice_line_component_raw as materialized'));
  assert.match(snapshot,/source_rounding_suppressions/);assert.match(snapshot,/WEEKLY_SOURCE_INVOICE_RECEIPT_SUPPORT/);
  assert.match(snapshot,/digest\(m.presentation_model::text,'sha256'\)/);
  assert.equal((snapshot.match(/create or replace function private\._invoice_presentation_snapshot_batch\(/g)||[]).length,1);
  assert.doesNotMatch(snapshot,/\\(?:if|else|endif|gset)\b/);
  assert.match(transport,/purpose='FINAL_ISSUE' then.*?snapshot_json/s);
  assert.match(sql('14_complete_legacy_render_manifest.sqlinc'),/weekly_source_invoice_rounding_suppression_witness_v1\(l.id\)/);
});
test('generated manifest hashes every full owner, no new relation/origin, private service union preserves PP helper, Bank definitions are not emitted',()=>{
  const manifest=JSON.parse(sql('SOURCE_MANIFEST.json'));assert.equal(manifest.frozen_28_unchanged,true);
  for(const file of manifest.artifacts){const data=fs.readFileSync(file.file);assert.equal(data.length,file.bytes);assert.equal(createHash('sha256').update(data).digest('hex'),file.sha256);}
  assert.doesNotMatch(sql('01_schema.sqlinc'),/create table /i);
  assert.ok(manifest.functions.every(f=>!f.name.includes('bpay_')));
  assert.match(sql('13b_complete_acl_helper_service_union.sqlinc'),/weekly_source_pay_actor_allowed_v1\(uuid\)/);
  const census=JSON.parse(sql('READER_CENSUS.json'));assert.ok(census.rows.length>=100);
  assert.ok(census.rows.filter(r=>!r.successor).every(r=>r.function.includes('bpay_')));
});
