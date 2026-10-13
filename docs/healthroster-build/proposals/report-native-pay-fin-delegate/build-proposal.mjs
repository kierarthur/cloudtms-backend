import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';

const destination = path.dirname(fileURLToPath(import.meta.url));
const lane = path.resolve(destination, '../../../..');
const integrator = path.resolve(lane, '../healthroster-v2-backend-13102026');
const hash = value => crypto.createHash('sha256').update(value).digest('hex');
const inputs = [
  'supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/04_complete_fin.sqlinc',
  'supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/09_complete_pay_consumers.sqlinc',
  'supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/02_money_origins.sqlinc',
  'supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/03_receipt_and_window_owners.sqlinc',
  'supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/13_private_acl.sqlinc',
  'supabase/repeatable/13102026_1344_weekly_source_self_bill_report/13102026_1344_finalise.sqlinc',
  'supabase/repeatable/13102026_1344_weekly_source_self_bill_report/13102026_1344_binding.sqlinc',
  'supabase/repeatable/13102026_1344_weekly_source_self_bill_report/13102026_1344_pay_witness.sqlinc',
  'supabase/repeatable/13102026_1344_weekly_source_self_bill_report/13102026_1344_report_scope.sqlinc',
  'supabase/repeatable/05102026_0044_bpay_next_source_pay_query_admission_v2.sql',
  'supabase/repeatable/15092026_1534_weekly_source_ordinary_pay_projection_v1.sql',
  'supabase/repeatable/15092026_2336_weekly_source_finalisation_pay_orchestration_v1.sql',
  'supabase/repeatable/04102026_2120_weekly_source_approval_basis_v2.sql',
  'supabase/repeatable/04102026_2106_banking_pay_next_source_origin_state_v1.sql',
  'supabase/repeatable/05102026_0114_weekly_source_pay_query_evidence_v1.sql',
  'supabase/repeatable/05102026_0245_weekly_source_pay_query_c1_receipt_v1.sql',
  'supabase/repeatable/05102026_0258_weekly_source_pay_query_next_receipt_v1.sql',
  'supabase/repeatable/13102026_1438_weekly_source_protection_queries/01_wait_receipt.sqlinc',
  'supabase/repeatable/13102026_1438_weekly_source_protection_queries/02_wait_witness.sqlinc',
  'supabase/repeatable/healthroster_wp_u3_source_proposals/wp_u3_submission_context.sqlinc',
  'supabase/repeatable/07082026_2059_candidate_app_private_helpers_v1.sql',
  'supabase/repeatable/21072026_1820_00_import_review_internal_core.sql',
  'supabase/repeatable/05102026_0057_weekly_source_pay_query_scope_v1.sql',
  'supabase/repeatable/17092026_0200_weekly_source_rotation_authority_v1.sql',
  'supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql',
  'supabase/migrations/15092026_2335_weekly_source_finalisation_pay_orchestration.sql',
  'supabase/repeatable/21072026_1235_37_tsfin_write_current_snapshot_single_bounded.sql',
  'supabase/repeatable/21072026_1235_38_tsfin_write_snapshots_and_complete.sql',
  'supabase/repeatable/06092026_1636_candidate_advanced_expense_component_policy_v1.sql',
  'supabase/baseline/22082026_1500_cloudtms_test_structural_baseline.sql',
];
const source = p => fs.readFileSync(path.join(integrator, p), 'utf8');
const replaceOnce = (body, oldText, newText) => {
  if (body.split(oldText).length !== 2) throw new Error(`Expected one hunk: ${oldText.slice(0, 60)}`);
  return body.replace(oldText, () => newText);
};
// Preserve the full three-argument owner byte-for-byte. The new overload only
// adds one internal parameter, one scope admission hunk and one delegate arm.
const original = source(inputs[0]);
let fin = original;
const nl = original.includes('\r\n') ? '\r\n' : '\n';
const hunks = [];
function edit(oldText, newText) { hunks.push({ oldText, newText }); fin = replaceOnce(fin, oldText, newText); }
edit(`  p_prepare_only boolean${nl}`, `  p_prepare_only boolean,${nl}  p_report_vat_rounding_variant text${nl}`);
edit(`     or v_scope_kind not in ('CYCLE','NHSP_REPORT_SCOPE')${nl}     or (v_scope_kind='NHSP_REPORT_SCOPE') is distinct from (v_report_scope_id is not null)`,
  `     or v_scope_kind not in ('CYCLE','NHSP_REPORT_SCOPE','SELF_BILL_REPORT_SCOPE')${nl}     or (v_scope_kind in ('NHSP_REPORT_SCOPE','SELF_BILL_REPORT_SCOPE')) is distinct from (v_report_scope_id is not null)`);
const delegate = `
  -- Internal Report delegate only AFTER unchanged payload/ID/hash/admission.
  -- This arm writes invoice-only Report authority and acquires no I-1 family
  -- locks. The native complete ordinary lock ladder below is unchanged.
  if v_scope_kind='SELF_BILL_REPORT_SCOPE' then
    select * into v_scope from public.weekly_source_report_scopes where id=v_report_scope_id;
    select * into v_cycle from public.weekly_source_cycles where id=v_cycle_id;
    if v_scope.id is null or v_cycle.id is null or v_scope.self_bill_report_id is null
      or v_scope.source_cycle_id is distinct from v_cycle.id
      or v_scope.source_group_id is distinct from v_cycle.source_group_id then
      raise exception 'WEEKLY_SOURCE_REPORT_SCOPE_MISMATCH' using errcode='22023'; end if;
    perform private.weekly_source_office_authority_v1(v_actor,
      case when p_prepare_only then 'CORRECT_FINAL_SOURCE' else 'FINALISE_WEEK' end,
      v_cycle.source_group_id,v_scope.client_id,v_cycle.finalisation_week_ending);
    if p_prepare_only or p_correction_session_id is not null then
      raise exception 'WEEKLY_SOURCE_SELF_BILL_REPORT_CORRECT_FINAL_UNSUPPORTED' using errcode='55000'; end if;
    -- NULL is a held operational variant, never an implicit A choice.
    return private.weekly_source_self_bill_report_finalise_v1(p_request,p_report_vat_rounding_variant);
  end if;
`.replace(/\n/g,nl);
edit(`  perform private.weekly_source_pay_query_admit_v2();${nl}`, `  perform private.weekly_source_pay_query_admit_v2();${nl}${delegate}`);
fs.writeFileSync(path.join(destination,'04_complete_fin_overload.sqlinc'), fin);
fs.writeFileSync(path.join(destination,'FIN_HUNKS.json'), JSON.stringify(hunks,null,2)+'\n');
fs.writeFileSync(path.join(destination,'05_complete_report_finalise.sqlinc'),source(inputs[5]));
// Complete Report facts owner, the binding producer is preserved and is not
// redefined by this proposal. Wrapper delegates the extended real pay proof.
const binding = source(inputs[6]);
const marker='create or replace function private.weekly_source_report_shift_facts_v1';
if (binding.split(marker).length!==2) throw new Error('Report facts owner cardinality');
fs.writeFileSync(path.join(destination,'06_complete_report_shift_facts.sqlinc'),marker+binding.split(marker)[1]);
const pins = inputs.map(p => { const bytes = fs.readFileSync(path.join(integrator,p)); return { path:p,sha256:hash(bytes),bytes:bytes.length }; });
fs.writeFileSync(path.join(destination,'CONTROLLING_INPUTS.json'),JSON.stringify({ schema_version:'REPORT_NATIVE_FIN_INPUTS_V1',integrator_path:integrator.replaceAll('\\','/'),inputs:pins },null,2)+'\n');
console.log(JSON.stringify({generated:4,input_count:pins.length,fin_sha256:hash(fin),ordinary_body_preserved:true}));
