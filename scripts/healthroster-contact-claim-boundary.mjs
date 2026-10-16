// Finite Health contact composition of the complete frozen Banking mail owner.
// No database access, signature/ACL change, or financial authority waiver.
import fs from 'node:fs';
import path from 'node:path';
import { canonical,digest,exactAtomicBody,scanManagedSql } from './cloudtms-managed-release-sql.mjs';
import { routineDefinitions } from './bpay-next-final-definers.mjs';

export const CONTACT_CLAIM_CORE_PATH='supabase/repeatable/includes/weekly_contact_query/082_native_claim_kernel.sqlinc';
export const CONTACT_CLAIM_BRIDGE_PATH='supabase/repeatable/includes/weekly_contact_query/083_native_claim_bridge.sqlinc';
export const CONTACT_CLAIM_ROOT='supabase/repeatable/16102026_0339_weekly_contact_query_native_lifecycle_v1.sql';
export const CONTACT_CLAIM_CORE_SIGNATURE='public._email_outbox_contact_claim_core_v1(integer,text,integer,boolean,uuid,text)';
export const CONTACT_CLAIM_MIGRATION=Object.freeze({
  version:'HEALTHROSTER_CONTACT_CLAIM_ATOMIC_COMPANION_V1',
  path:'supabase/migrations/16102026_0339_weekly_contact_query_lineage_and_claim_fence.sql',
  contentSha256:'82a3d0487cb483e692c7fdf590a75280a4dd88a7a1057b43a7fb39639da4e9e7',
  closureSha256:'d995cc3c49873dc4fd85103a341d09447a67a043df09ccb390310aaf212d9f27',
  parentPath:'supabase/repeatable/04102026_0558_banking_pay_next_legacy_email_claim_boundary_v1.sql',
  parentSha256:'fd5bc5ed576d81d02aade82b0111bb560e3ce51707259b76871b469f29e1fb9c',
  coreSha256:'3b013f5156a61a0b17a2f3545885f5cd0f89ba050d9418d71973b1003ef291ca',
  statementSha256:'8c620a02dbb1aab07d07486b8a73562b330aef9160e67ad7736814d396a2bac0'
});
const ORIGINAL_SIGNATURE='public.email_outbox_claim_ready_batch(integer,text,integer)';
const SELECTOR=`(mo.type<>'TIMESHEET_QUERY' or(p_contact_bridge and exists(
          select 1 from public.hr_issue_email_deliveries d join public.hr_issue_email_deliveries original on original.id=d.original_delivery_id
          where d.mail_outbox_id=mo.id and d.contact_managed and d.legacy_claim_lease_token is null
           and d.status='QUEUED' and d.retired_at_utc is null and original.original_delivery_id=original.id and original.payload_revision=1)))
        and `;
const STAMP=`    contact_claim as materialized (
      select case when p_contact_bridge then public._email_outbox_contact_claim_stamp_v1(
        coalesce(array_agg(p.id),'{}'::uuid[]),p_attempt_lease_token,v_now,v_now+make_interval(mins=>v_lease),p_agency,p_environment) else null end proof
      from picked p
    ),
`;
const BRIDGE=`create or replace function public.email_outbox_claim_ready_batch(p_limit integer,p_attempt_lease_token text,p_lease_minutes integer default 5)
returns setof public.mail_outbox language plpgsql security invoker set search_path='public','pg_temp'
as $function$
begin return query select * from public._email_outbox_contact_claim_core_v1(p_limit,p_attempt_lease_token,p_lease_minutes,false);end;
$function$;`;
function definition(source,signature){
  const normalized=source.replaceAll('\r\n','\n');
  const rows=scanManagedSql(normalized).statements.filter(row=>routineDefinitions(normalized.slice(row.start,row.end),'contact-claim.sql').includes(signature));
  if(rows.length!==1)throw Error('HEALTHROSTER_CONTACT_CLAIM_DEFINITION_COUNT');
  return normalized.slice(rows[0].start,rows[0].end);
}
function exactReplace(source,from,to,count){
  if(source.split(from).length!==count+1)throw Error('HEALTHROSTER_CONTACT_CLAIM_PARENT_SHAPE');
  return source.split(from).join(to);
}
export function assertContactClaimBoundary({parent,core,bridge}){
  let expected=definition(parent,ORIGINAL_SIGNATURE);
  expected=exactReplace(expected,'public.email_outbox_claim_ready_batch(','public._email_outbox_contact_claim_core_v1(',1);
  expected=exactReplace(expected,'  p_lease_minutes integer default 5','  p_lease_minutes integer default 5,p_contact_bridge boolean default false,p_agency uuid default null,p_environment text default null',1);
  expected=exactReplace(expected,"where mo.status='QUEUED'",'where '+SELECTOR+"mo.status='QUEUED'",2);
  expected=exactReplace(expected,'    updated as (',STAMP+'    updated as (',2);
  expected=exactReplace(expected,'from picked p where mo.id=p.id returning mo.*','from picked p cross join contact_claim proof where mo.id=p.id returning mo.*',2);
  expected=exactReplace(expected,'  end if;\n\nend;','  end if;\n  if p_contact_bridge then perform public._email_outbox_contact_claim_cleanup_v1(p_attempt_lease_token);end if;\n\nend;',1);
  if(definition(core,CONTACT_CLAIM_CORE_SIGNATURE)!==expected||definition(bridge,ORIGINAL_SIGNATURE)!==BRIDGE)
    throw Error('HEALTHROSTER_CONTACT_CLAIM_BOUNDARY_CHANGED');
}

// The immutable bootstrap contains an older core. Reinstall the complete
// corrected core BEFORE that unit's ledger/commit: no unsafe intermediate
// committed function and no rewrite of the published migration bytes.
export function contactClaimMigrationCompanionFor(closure,kind,{root,mapSource=value=>value}={}){
  const pin=CONTACT_CLAIM_MIGRATION;
  if(closure.path!==pin.path)return null;
  if(kind!=='migration'||closure.files.size!==1||closure.ordered.length!==1
    ||closure.files.get(pin.path)!==pin.contentSha256||closure.closureHash!==pin.closureSha256
    ||digest(closure.ordered[0].source)!==pin.contentSha256)
    throw Error('HEALTHROSTER_CONTACT_CLAIM_IMMUTABLE_SOURCE_CHANGED');
  const read=file=>canonical(fs.readFileSync(path.join(root,file),'utf8'));
  const parent=read(pin.parentPath),core=read(CONTACT_CLAIM_CORE_PATH),bridge=read(CONTACT_CLAIM_BRIDGE_PATH);
  if(digest(parent)!==pin.parentSha256||digest(core)!==pin.coreSha256)
    throw Error('HEALTHROSTER_CONTACT_CLAIM_COMPANION_SOURCE_CHANGED');
  assertContactClaimBoundary({parent,core,bridge});
  if(definition(closure.ordered[0].source,ORIGINAL_SIGNATURE)!==BRIDGE)
    throw Error('HEALTHROSTER_CONTACT_CLAIM_BOOTSTRAP_BRIDGE_CHANGED');
  const statement=definition(core,CONTACT_CLAIM_CORE_SIGNATURE);
  if(digest(statement)!==pin.statementSha256)throw Error('HEALTHROSTER_CONTACT_CLAIM_COMPANION_STATEMENT_CHANGED');
  return {version:pin.version,sourceRoot:root,
    migrationPath:pin.path,migrationSha256:pin.contentSha256,coreSha256:pin.coreSha256,
    statementSha256:pin.statementSha256,sql:mapSource(statement,CONTACT_CLAIM_CORE_PATH),
    sourceFiles:[{file:CONTACT_CLAIM_CORE_PATH,sha256:pin.coreSha256},
      {file:pin.parentPath,sha256:pin.parentSha256},
      {file:CONTACT_CLAIM_BRIDGE_PATH,sha256:digest(bridge)}]};
}

export function contactClaimMigrationSql(unit,mapSource=value=>value){
  if(unit.item.path!==CONTACT_CLAIM_MIGRATION.path||unit.exception||unit.ownerRlsEnvelope||unit.immutableGuardDependency
    ||unit.body!==exactAtomicBody(unit.closure.expanded)||unit.item.sha256!==CONTACT_CLAIM_MIGRATION.contentSha256
    ||!unit.contactClaimCompanion)
    throw Error('HEALTHROSTER_CONTACT_CLAIM_ATOMIC_SCOPE_INVALID');
  const expected=contactClaimMigrationCompanionFor(unit.closure,unit.kind,{root:unit.contactClaimCompanion.sourceRoot,mapSource});
  if(JSON.stringify(unit.contactClaimCompanion)!==JSON.stringify(expected))
    throw Error('HEALTHROSTER_CONTACT_CLAIM_ATOMIC_COMPANION_CHANGED');
  return expected.sql;
}
