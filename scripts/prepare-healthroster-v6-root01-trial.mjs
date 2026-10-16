import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {mapExecutableSqlSource} from './cloudtms-db-release-lib.mjs';
import {exactAtomicBody,scanManagedSql} from './cloudtms-managed-release-sql.mjs';

// AGENTS permits exact staged-blob constructor qualification before a new
// migration is reviewed/locked. No connection or SQL execution happens here.
const target='supabase/migrations/16102026_0159_nhsp_v6_01_physical_source_review_schema.sql';
const output=path.resolve('.codex-tmp/v6-root01-native-20261016');
const sha=v=>createHash('sha256').update(v).digest('hex');
const saved=fs.readFileSync(target);
const staged=execFileSync('git',['cat-file','blob',`:${target}`]);
assert.deepEqual(saved,staged,'exact staged migration bytes required');
assert.equal(sha(staged),'d46481d0cd53880041b126c554d34dccc39854ff3bf31ac98883f85949cc1d82');
const source=staged.toString('utf8');
const tables=[...source.matchAll(/create table private\.([a-z0-9_]+)/g)].map(m=>m[1]);
assert.equal(tables.length,23);assert.equal(new Set(tables).size,23);
const parents=[...new Set([...source.matchAll(/references (public\.[a-z0-9_]+)/g)].map(m=>m[1]))].sort();
const names=tables.map(n=>`'private.${n}'`).join(',');
const parentNames=parents.map(n=>`'${n}'`).join(',');
const frame=`select jsonb_build_object(
 'database',current_database(),
 'tables_absent',not exists(select 1 from unnest(array[${names}])n where to_regclass(n) is not null),
 'legacy_one',exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='LEGACY' and owner_epoch=1),
 'migration_ledger',(select md5(coalesce(string_agg(to_jsonb(t)::text,'|' order by to_jsonb(t)::text),'')) from private.cloudtms_migration_ledger t),
 'repeatable_ledger',(select md5(coalesce(string_agg(to_jsonb(t)::text,'|' order by to_jsonb(t)::text),'')) from private.cloudtms_repeatable_ledger t),
 'native_parent_catalogue',(select md5(coalesce(string_agg(x.identity||x.definition,'|' order by x.identity,x.definition),'')) from(
  select c.conrelid::regclass::text||':'||c.conname identity,pg_get_constraintdef(c.oid,true) definition
   from pg_constraint c where c.conrelid in(select to_regclass(n) from unnest(array[${parentNames}])n)
  union all select t.tgrelid::regclass::text||':'||t.tgname,pg_get_triggerdef(t.oid,true)
   from pg_trigger t where t.tgrelid in(select to_regclass(n) from unnest(array[${parentNames}])n)
 )x));
`;
const guard=`do $guard$ begin
 if current_database()<>'cloudtms_test_clone'
  or not exists(select 1 from private.cloudtms_database_identity where singleton and environment='TEST')
  or not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='LEGACY' and owner_epoch=1)
  or exists(select 1 from unnest(array[${names}])n where to_regclass(n) is not null)
 then raise exception 'V6_ROOT01_BASELINE_REFUSED';end if;
end;$guard$;
`;
const assertions=`do $assert$
declare n text;r pg_catalog.pg_class%rowtype;v_owner oid;v_policy_name text;v_role text;
begin
 select oid into strict v_owner from pg_roles where rolname=current_user;
 foreach n in array array[${tables.map(n=>`'${n}'`).join(',')}] loop
  select c.* into strict r from pg_class c join pg_namespace s on s.oid=c.relnamespace
   where s.nspname='private' and c.relname=n and c.relkind='r';
  if r.relowner<>v_owner or not r.relrowsecurity or not r.relforcerowsecurity then
   raise exception 'V6_ROOT01_OWNER_RLS_REFUSED: %',n;end if;
  v_policy_name:=case when n like 'weekly_source_pay_decision%' then 'cloudtms_source_pay_decision_owner_only'
   when n in('timesheet_contract_move_invoice_scopes','timesheet_contract_move_invoice_scope_lines',
    'timesheet_contract_move_billing_slices','timesheet_contract_move_replacement_groups',
    'timesheet_contract_move_replacement_members','timesheet_contract_move_scope_fences') then 'invoice_correction_owner'
   else 'timesheet_contract_move_owner_only' end;
  if (select count(*) from pg_policy where polrelid=r.oid)<>1
   or not exists(select 1 from pg_policy p where p.polrelid=r.oid and p.polname=v_policy_name
    and p.polcmd='*' and p.polroles=array[v_owner] and pg_get_expr(p.polqual,p.polrelid)='true'
    and pg_get_expr(p.polwithcheck,p.polrelid)='true') then
   raise exception 'V6_ROOT01_EXACT_OWNER_POLICY_REFUSED: %',n;end if;
  if exists(select 1 from aclexplode(coalesce(r.relacl,acldefault('r',r.relowner)))a where a.grantee<>v_owner)
   or exists(select 1 from pg_constraint c where c.conrelid=r.oid and not c.convalidated) then
   raise exception 'V6_ROOT01_ACL_OR_CONSTRAINT_REFUSED: %',n;end if;
  foreach v_role in array array['anon','authenticated','service_role'] loop
   if has_table_privilege(v_role,r.oid,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') then
    raise exception 'V6_ROOT01_ROLE_EXPOSED: % / %',n,v_role;end if;
  end loop;
 end loop;
 if not exists(select 1 from pg_constraint c where c.conrelid='public.timesheets'::regclass
  and c.conname='timesheets_archived_not_authorised_chk' and c.convalidated
  and pg_get_constraintdef(c.oid,true) like '%CONTRACT_MOVE%') then
  raise exception 'V6_ROOT01_EXACT_ARCHIVE_CONSTRAINT_MISSING';end if;
 raise notice 'V6_ROOT01_23_TABLES_OWNER_RLS_ACL_CONSTRUCTOR_PASS; BUSINESS_NATIVE_ACCEPTANCE_NOT_RUN';
end;$assert$;
`;
const previous=process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER;
let mapped;
try {
  process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER='CURRENT_USER';
  mapped=mapExecutableSqlSource(exactAtomicBody(source),target);
} finally {
  if(previous===undefined)delete process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER;
  else process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER=previous;
}
const sql='set local jit=off;\nset local pg_show_plans.is_enabled=off;\n'+guard+mapped+'\n'+assertions;
const manifest={target,source_sha256:sha(staged),source_bytes:staged.length,
  staged_blob:execFileSync('git',['rev-parse',`:${target}`],{encoding:'utf8'}).trim(),
  parent_commit:execFileSync('git',['rev-parse','HEAD'],{encoding:'utf8'}).trim(),tables,parents,
  sql_sha256:sha(sql),sql_bytes:Buffer.byteLength(sql),statements:scanManagedSql(sql).statements.length,
  frame_sha256:sha(frame),account:'kier@arthur-rai.co.uk',native_server:'cloudtms-miget-operations-kier-codex',
  scope:'Staged root01 DDL/23 private stores/owner-only RLS and ACL/validated keys only. No business rows, guards, functions, cut, valuation, producer, native/API/full-release acceptance.'};
assert(manifest.sql_bytes<=8_000_000&&manifest.statements<=1000);
fs.mkdirSync(output,{recursive:true});
for(const [name,value]of Object.entries({'TRIAL.sql':sql,'FINGERPRINT.sql':frame,'MANIFEST.json':JSON.stringify(manifest,null,2)+'\n'})){
 const file=path.join(output,name);if(fs.existsSync(file))assert.equal(fs.readFileSync(file,'utf8'),value,'preserve prior trial');
 else fs.writeFileSync(file,value);
}
console.log(JSON.stringify(manifest));
