import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {RULE_SET_CHOICES} from '../../broker/src/weekly-source/rule-set-validation.js';
const read=path=>fs.readFileSync(new URL('../../'+path,import.meta.url),'utf8').replaceAll('\r\n','\n');
const schema=read('supabase/migrations/13102026_1133_weekly_source_rule_sets_schema.sql');
const sql=read('supabase/repeatable/13102026_1134_weekly_source_rule_sets_v1.sql');
const acl=read('supabase/repeatable/15092026_1534_weekly_source_acl_contract_v1.sql');
const verifier=read('supabase/verification/15092026_1534_weekly_source_acl_contract_v1.sql');
const fixture=read('supabase/verification/13102026_1206_weekly_source_rule_sets_verification.sql');
const current=JSON.parse(read('supabase/release/current-contract.json'));
function body(name){const start=sql.toLowerCase().indexOf('create or replace function '+name.toLowerCase()+'(');assert.ok(start>=0,name);const end=sql.indexOf('$function$;',start);assert.ok(end>start);return sql.slice(start,end);}
test('three planned stores are closed, versioned and default-off, with provider-compatible RLS',()=>{
  assert.equal((schema.match(/create table public\./g)||[]).length,3);
  for(const table of ['weekly_source_rule_sets','weekly_source_rule_set_versions','weekly_source_group_rule_assignments']){
    assert.ok(schema.includes(`alter table public.${table} owner to postgres`));
    assert.ok(acl.includes(`('${table}',`));
    assert.ok(verifier.includes(`'${table}'`));
  }
  for(const [key,values] of Object.entries(RULE_SET_CHOICES)) for(const value of values) assert.ok(schema.includes(`'${value}'`),`${key}:${value}`);
  assert.match(schema,/feature_enabled boolean not null default false/);
  assert.match(schema,/exclude using gist[\s\S]*source_group_id with =[\s\S]*daterange\(effective_from_week_ending/);
  assert.match(schema,/enable row level security/);assert.match(schema,/force row level security/);
  assert.match(schema,/cloudtms_miget_service_owner_all/);assert.match(schema,/p\.polroles/);
});
test('new public owners self-reassert service-only grants and same-closure ACL union',()=>{
  for(const name of ['weekly_source_rule_sets_get_v1','weekly_source_rule_set_version_save_atomic_v1','weekly_source_group_rule_assign_atomic_v1','weekly_source_group_rule_feature_set_atomic_v1']){
    assert.match(body('public.'+name),/security definer/i);
    assert.match(body('public.'+name),/_weekly_source_settings_require_admin_v1/);
    assert.ok(sql.includes('public.'+name+'(jsonb)'));
    assert.ok(acl.includes(`('public.${name}(jsonb)')`));
    assert.ok(verifier.includes(`'public.${name}(jsonb)'`));
  }
  assert.ok(acl.includes("('private.weekly_source_pay_actor_allowed_v1(uuid)')"));
  assert.ok(verifier.includes("'private.weekly_source_pay_actor_allowed_v1(uuid)'"));
  assert.match(sql,/revoke all on function public\.[\s\S]*from public,anon,authenticated,service_role;/);
  assert.doesNotMatch(sql,/\\ir 15092026_1534_weekly_source_acl_contract_v1\.sql/);
  assert.match(sql,/notify pgrst, 'reload schema';\ncommit;/);
  // The full integration adds the two Report stores and Money hold ledger to
  // F1's102, with the same strict independently classified ACL check retained.
  assert.match(acl,/v_contract_count<>105/);assert.match(verifier,/v_immutable_count<>39[\s\S]*v_stateful_count<>47/);
});
test('blank NEW migration never requires a helper installed in a later repeatable',()=>{
  assert.doesNotMatch(schema,/create trigger/i);
  assert.match(sql,/drop trigger if exists weekly_source_immutable_record_guard on public\.weekly_source_rule_set_versions;/);
  assert.match(sql,/create trigger weekly_source_immutable_record_guard before update or delete[\s\S]*private\._weekly_source_immutable_record_guard_v1\(\)/);
  assert.match(sql,/create trigger weekly_source_immutable_truncate_guard before truncate[\s\S]*private\._weekly_source_immutable_record_guard_v1\(\)/);
});
test('no rule value is added to effective policy/NO_CHANGE and NULL remains the exact rule fallback',()=>{
  assert.doesNotMatch(sql,/create or replace function private\._weekly_source_effective_policy_v1/i);
  assert.doesNotMatch(sql,/create or replace function .*finalise_engine|create or replace function .*state_fingerprint/i);
  assert.match(body('private.weekly_source_effective_rule_set_v1'),/a\.feature_enabled[\s\S]*a\.feature_purpose='RULE_SET'/);
  assert.match(body('private.weekly_source_has_self_bill_rules_v1'),/effective_rule_set_v1\(p_client_id,p_cycle_id\) is not null/);
  assert.match(body('private.weekly_source_final_rule_version_guard_v1'),/new\.rule_set_version_id is distinct from old\.rule_set_version_id/);
});
test('actual Timesheet predicates share current physical and dated server scope',()=>{
  const agency=body('private.weekly_source_agency_invoice_check_only_v1');
  assert.match(agency,/t\.is_current/);assert.match(agency,/t\.sheet_scope::text='WEEKLY'/);
  assert.match(agency,/v_ts\.contract_id/);assert.match(agency,/invoice_sender'<>'AGENCY_INVOICE'/);
  assert.match(agency,/weekly_source_is_import_authoritative_v1/);assert.match(agency,/self_bill_no_invoices_sent/);
  const signed=body('private.weekly_source_signed_check_only_v1');
  assert.match(signed,/t\.sheet_scope::text in \('WEEKLY','DAILY'\)/);
  assert.match(signed,/f\.timesheet_version=v_ts\.version/);assert.match(signed,/v_client_count<>1/);
  assert.match(signed,/a\.feature_enabled[\s\S]*feature_purpose='SIGNED_CHECK_ONLY'[\s\S]*rule_set_version_id is null/);
  assert.match(signed,/TIMESHEET_AUTHORITY/);assert.match(signed,/INVOICE_EVIDENCE_REQUIRED/);
  assert.match(signed,/_import_review_effective_authority_core_v1/);assert.match(signed,/VALIDATION_ONLY/);
  assert.doesNotMatch(signed,/r2_auth_key|img_sha256_auth/);
  for(const predicate of [agency,signed]) {
    assert.match(predicate,/t\.archived_at_utc is null/);
    for(const [,column] of predicate.matchAll(/v_ts\.([a-z_]+)/g)) {
      assert.ok(current.relations.find(r=>r.schema==='public'&&r.name==='timesheets').columns.some(c=>c.name===column),`actual TS field ${column}`);
    }
  }
});
test('rollback fixture uses actual current legacy columns and has no broad truncate or external owner',()=>{
  for(const [,table,columns] of fixture.matchAll(/insert into public\.([a-z_]+)\s*\(([^)]+)\)/gi)) {
    const relation=current.relations.find(r=>r.schema==='public'&&r.name===table);
    if(!relation)continue; // New F1 tables are established by the reviewed migration.
    for(const column of columns.split(',').map(v=>v.trim())) assert.ok(relation.columns.some(c=>c.name===column),`${table}.${column}`);
  }
  assert.doesNotMatch(fixture,/truncate public\./i);
  assert.doesNotMatch(fixture,/perform .*authorise.*atomic|perform .*finalise_engine|perform .*send.*email/i);
  assert.match(fixture,/positive_exercised',false,'held_by_real_release_gate',true/);
});
test('activation holds an APPLYING successor and atomically delegates policy changes/restoration',()=>{
  const activation=body('public.weekly_source_group_rule_feature_set_atomic_v1');
  assert.match(activation,/order by started_at_utc desc,release_id desc limit 1/);
  assert.match(activation,/v_latest\.release_id<>v_release_id or v_latest\.status<>'VERIFIED'/);
  assert.match(activation,/jsonb_typeof\(p_request->'feature_enabled'\) is distinct from 'boolean'/);
  assert.match(activation,/v_assignment\.version<>v_expected/);
  assert.match(activation,/client_settings_versions/);assert.match(activation,/WEEKLY_SOURCE_RULE_CLIENT_RESTORE_STALE/);
  assert.match(activation,/public\.weekly_source_client_settings_save_atomic_v1/);
  assert.match(activation,/client_changes/);assert.match(activation,/before_policy_id/);assert.match(activation,/after_policy_id/);
  assert.match(body('private.weekly_source_configured_agency_invoice_v1'),/a\.feature_enabled and a\.feature_purpose='RULE_SET'/);
});
