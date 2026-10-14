import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const workspace = path.resolve(here, '../../../../../../..');
const root = process.argv[2] || path.join(workspace, '.codex-worktrees/healthroster-v2-backend-13102026');
const source = path.join(here, 'source');
fs.mkdirSync(source, { recursive: true });
const hashes = [];
const sha = value => crypto.createHash('sha256').update(value).digest('hex');
function read(relative) {
  const bytes = fs.readFileSync(path.join(root, relative));
  hashes.push({ path: relative, sha256: sha(bytes), bytes: bytes.length });
  return bytes.toString('utf8').replace(/\r\n/g, '\n');
}
function owner(text, name) {
  const escaped = name.replaceAll('.', '\\.');
  const regex = new RegExp(`create or replace function ${escaped}\\([\\s\\S]*?\\$function\\$;`, 'i');
  const found = text.match(regex);
  assert.ok(found, `missing complete owner ${name}`);
  return found[0];
}
function replaceOnce(text, old, next) {
  assert.equal(text.split(old).length, 2, `changed source anchor ${old.slice(0, 80)}`);
  return text.replace(old, next);
}
function output(name, text) { fs.writeFileSync(path.join(source, name), text + '\n'); }
const nativeEdits = read('supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_invoice_apply_edits.sql');
const money = read('supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/06_complete_allocation_issue.sqlinc');
const moveInput = read('supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/07_complete_header_move.sqlinc');
const contextInput = read('supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/08_complete_batch_detail.sqlinc');
const acl = read('supabase/repeatable/03092026_1644_invoice_apply_edits_frozen_authority_acl_v1.sql');
read('supabase/repeatable/06012026_settings_finwindow_eligibletimesshets_plus_recompute_totals.sqls.sql');
read('supabase/repeatable/15092026_1534_weekly_source_invoice_admission_v1.sql');
read('supabase/repeatable/13102026_1134_weekly_source_rule_sets_v1.sql');
read('supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/11_complete_native_snapshot_assembler.sqlinc');
read('supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/14_complete_legacy_render_manifest.sqlinc');
read('supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql');
read('supabase/migrations/13102026_1430_weekly_source_invoice_money_origins.sql');
read('supabase/migrations/13102026_1680_weekly_source_invoice_window_receipt_schema.sql');
const earlyGuard = "     and not v_correction_placement_only\n";
const guardAddition = '     and not private.weekly_source_invoice_adjustment_payload_admit_v1(p_invoice_id,p_payload)\n';
const branchAnchor = '  -- Require invoice_week_start in header_snapshot_json.meta\n';
const branch = `  -- F4: exact new Source header plus frozen S15 only. All ordinary code below
  -- remains the native owner, including older self-bill and NHSP refusal.
  if v_inv.header_snapshot_json->>'schema_version'='WEEKLY_SOURCE_SELF_BILL_INVOICE_V1'
     and v_inv.header_snapshot_json#>>'{meta,source}'='WEEKLY_FINAL_SOURCE' then
    if private.weekly_source_invoice_adjustment_authority_v1(p_invoice_id,null)->>'allowed'='true' then
      return private.weekly_source_invoice_adjustments_apply_v1(p_invoice_id,p_payload,p_actor_user_id);
    end if;
    if p_payload ? 'add_adjustments' or p_payload ? 'remove_invoice_line_ids' then
      raise exception 'WEEKLY_SOURCE_INVOICE_ADJUSTMENTS_NOT_ALLOWED' using errcode='55000';
    end if;
  end if;

`;
let edits = replaceOnce(nativeEdits, earlyGuard, earlyGuard + guardAddition);
edits = replaceOnce(edits, branchAnchor, branch + branchAnchor);
assert.equal(edits.replace(guardAddition, '').replace(branch, ''), nativeEdits);
output('01_invoice_apply_edits.sqlinc', edits);
const unbound = `      and not exists(
        select 1 from public.weekly_source_invoice_line_bindings binding
        where binding.invoice_line_id=invoice_line.id
          and binding.invoice_id=p_invoice_id and binding.state='CURRENT'
      )`;
const exemption = `
      and private.weekly_source_invoice_adjustment_line_valid_v1(invoice_line.id,p_invoice_id) is distinct from true`;
let allocation = owner(money, 'private.weekly_source_invoice_allocation_assert_v1');
allocation = replaceOnce(allocation, unbound, unbound + exemption);
const emptyOld = `    if v_line_count<>0 then
      raise exception 'WEEKLY_SOURCE_INVOICE_EXTRA_LINE' using errcode='55000';
    end if;`;
const emptyNew = `    if exists(select 1 from public.invoice_lines invoice_line where invoice_line.invoice_id=p_invoice_id
      and private.weekly_source_invoice_adjustment_line_valid_v1(invoice_line.id,p_invoice_id) is distinct from true) then
      raise exception 'WEEKLY_SOURCE_INVOICE_EXTRA_LINE' using errcode='55000';
    end if;
    if v_invoice.subtotal_ex_vat is distinct from (select round(coalesce(sum(total_charge_ex_vat),0),2) from public.invoice_lines where invoice_id=p_invoice_id)
      or v_invoice.vat_amount is distinct from (select round(coalesce(sum(vat_amount),0),2) from public.invoice_lines where invoice_id=p_invoice_id)
      or v_invoice.total_inc_vat is distinct from (select round(coalesce(sum(total_inc_vat),0),2) from public.invoice_lines where invoice_id=p_invoice_id) then
      raise exception 'WEEKLY_SOURCE_INVOICE_TOTAL_MISMATCH' using errcode='55000';
    end if;`;
allocation = replaceOnce(allocation, emptyOld, emptyNew);
allocation = replaceOnce(allocation, "      'line_count',0,'binding_count',0,'empty',true", "      'line_count',v_line_count,'binding_count',0,'empty',v_line_count=0");
output('02_invoice_allocation_assert.sqlinc', allocation);
let issue = owner(money, 'private.weekly_source_invoice_issue_validate_v1');
issue = replaceOnce(issue, unbound, unbound + exemption);
output('03_invoice_issue_validate.sqlinc', issue);
output('04_invoice_move_atomic.sqlinc', owner(moveInput, 'public.weekly_source_invoice_move_atomic_v1'));
let context = owner(contextInput, 'public.weekly_source_invoice_edit_context_v1');
context = replaceOnce(context, '  v_result jsonb;\n', '  v_result jsonb;\n  v_adjustment_authority jsonb;\n  v_adjustments_allowed boolean:=false;\n');
const ctxAnchor = '  select pg_catalog.jsonb_build_object(\n';
const ctxPrelude = `  v_adjustment_authority:=private.weekly_source_invoice_adjustment_authority_v1(v_invoice.id,null);
  v_adjustments_allowed:=coalesce(v_adjustment_authority->>'allowed'='true',false)
    and v_invoice.status='DRAFT' and v_invoice.issued_at_utc is null and v_invoice.paid_at_utc is null
    and v_invoice.active_document_operation_id is null and v_invoice.active_issue_operation_id is null
    and pg_catalog.upper(coalesce(v_invoice.issue_state,'')) not in('VALIDATING','PREPARING_DOCUMENT','READY_TO_FINALISE')
    and exists(select 1 from public.tms_users u where u.id=v_actor and u.is_active and lower(btrim(u.role))='admin');

`;
context = replaceOnce(context, ctxAnchor, ctxPrelude + ctxAnchor);
context = replaceOnce(context, "    'is_weekly_source_invoice',true,\n", `    'is_weekly_source_invoice',true,
    'adjustments',pg_catalog.jsonb_build_object('contract_version','WEEKLY_SOURCE_INVOICE_ADJUSTMENTS_V1',
      'allowed',v_adjustments_allowed,'credits_shift_available',false,
      'default_vat_rate_pct',case when v_adjustments_allowed then v_adjustment_authority->>'vat_rate_pct' else null end),
`);
output('05_invoice_edit_context.sqlinc', context);
output('06_native_acl.sqlinc', acl);
output('07_new_owner_acl.sqlinc', `alter function private.weekly_source_invoice_allocation_assert_v1(uuid) owner to postgres;
alter function private.weekly_source_invoice_issue_validate_v1(uuid) owner to postgres;
alter function public.weekly_source_invoice_move_atomic_v1(jsonb) owner to postgres;
alter function public.weekly_source_invoice_edit_context_v1(jsonb) owner to postgres;
revoke all on function private.weekly_source_invoice_allocation_assert_v1(uuid) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_invoice_issue_validate_v1(uuid) from public,anon,authenticated,service_role;
revoke all on function public.weekly_source_invoice_move_atomic_v1(jsonb) from public,anon,authenticated,service_role;
revoke all on function public.weekly_source_invoice_edit_context_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_invoice_move_atomic_v1(jsonb) to service_role;
grant execute on function public.weekly_source_invoice_edit_context_v1(jsonb) to service_role;`);
fs.copyFileSync(path.join(here, 'f4-adjustment-owners.sqlinc'), path.join(source, '00_adjustment_owners.sqlinc'));
const includeList = fs.readdirSync(source).filter(name => name.endsWith('.sqlinc')).sort();
fs.writeFileSync(path.join(here, '13102026_1705_weekly_source_invoice_adjustments_v1.sql'),
  `-- PROPOSAL: root must integrate the complete controlling Money/Report union first.\n\\set ON_ERROR_STOP on\nbegin;\n${includeList.map(name => `\\ir source/${name}`).join('\n')}\ncommit;\n`);
fs.writeFileSync(path.join(here, 'F4_SOURCE_GUARD.json'), JSON.stringify({
  status:'PROPOSAL_SOURCE_ONLY_NOT_INSTALLED', inputs:hashes,
  ordinary_invoice_apply_edits_body_after_removing_two_declared_hunks_is_identical:true,
  unchanged_invoice_move_atomic_body_sha256:sha(owner(moveInput,'public.weekly_source_invoice_move_atomic_v1')),
  outputs:includeList.map(name => { const bytes=fs.readFileSync(path.join(source,name)); return {path:`source/${name}`,sha256:sha(bytes),bytes:bytes.length}; })
}, null, 2)+'\n');
console.log(JSON.stringify({status:'SOURCE_GENERATED',inputs:hashes.length,completeNativeOwners:5,includes:includeList.length}));
