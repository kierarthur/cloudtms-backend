// Finite catalogue-only boundary for the twelve exact heap scopes. No connection.
import fs from'node:fs';import {literal}from'./build-program.mjs';
const stores=JSON.parse(fs.readFileSync(new URL('./reference/source-store-metadata.json',import.meta.url)));
if(stores.relations.length!==12||stores.policies.length!==12)throw Error('CONTACT_QT_EXACT_STORE_METADATA_REQUIRED');
for(const r of stores.relations){
 const policies=stores.policies.filter(p=>p.schema===r.schema&&p.table===r.name);
 if(r.kind!=='r'||r.owner!=='postgres'||typeof r.rls!=='boolean'||typeof r.force_rls!=='boolean'||policies.length!==1||policies[0].name!=='cloudtms_miget_service_owner_all'
  ||policies[0].using!=='true'||policies[0].check!=='true'||policies[0].command!=='ALL'||policies[0].permissive!=='PERMISSIVE'
  ||JSON.stringify(policies[0].roles)!=='["postgres","service_role"]')throw Error('CONTACT_QT_UNREVIEWED_STORE_POLICY');
}
const rows=stores.relations.map(r=>`(${literal(r.schema+'.'+r.name)},${r.rls},${r.force_rls})`).join(',');
export const NATIVE_BOUNDARY_SQL=`(
 current_user='service_role'
 or exists(select 1 from pg_catalog.pg_event_trigger event_hook where event_hook.evtenabled<>'D')
 or exists(select 1 from (values ${rows}) expected_store(name,rls,force_rls)
  left join pg_catalog.pg_class relation_row on relation_row.oid=pg_catalog.to_regclass(expected_store.name)
  where relation_row.oid is null or relation_row.relkind<>'r'
   or pg_catalog.pg_get_userbyid(relation_row.relowner)<>current_user
   or relation_row.relrowsecurity is distinct from expected_store.rls
   or relation_row.relforcerowsecurity is distinct from expected_store.force_rls
   or exists(select 1 from pg_catalog.pg_inherits inheritance where inheritance.inhrelid=relation_row.oid or inheritance.inhparent=relation_row.oid)
   or exists(select 1 from pg_catalog.pg_rewrite rewrite_row where rewrite_row.ev_class=relation_row.oid)
   or (select count(*) from pg_catalog.pg_policy policy_row where policy_row.polrelid=relation_row.oid)<>1
   or not exists(select 1 from pg_catalog.pg_policy policy_row where policy_row.polrelid=relation_row.oid
    and policy_row.polname='cloudtms_miget_service_owner_all' and policy_row.polcmd='*' and policy_row.polpermissive
    and pg_catalog.pg_get_expr(policy_row.polqual,policy_row.polrelid)='true'
    and pg_catalog.pg_get_expr(policy_row.polwithcheck,policy_row.polrelid)='true'
    and array(select unnest(policy_row.polroles) order by 1)=
      (select array_agg(role_row.oid order by role_row.oid) from pg_catalog.pg_roles role_row where role_row.rolname in(current_user,'service_role'))))
 )`;
