import fs from 'node:fs';
import path from 'node:path';
import { inventory, calls, closureCalls, hash, assertRoot, here } from './read-source.mjs';
const root=path.resolve(process.argv[2]);
const own=path.resolve(here,'../..');
const commit='b9e2cbfcdf061a4e8fe07044e658c2daf043a362';
const observedHead=assertRoot(root,commit);
const inv=await inventory(root);
const fileFact=p=>{const b=fs.readFileSync(path.join(root,p));return {path:p,bytes:b.length,sha256:hash(b)};};
const files=['supabase/verification/17092026_0600_weekly_source_first_authorisation_v1.sql',
  'supabase/verification/17092026_0610_weekly_source_withdrawal_supersession_v1.sql',
  'supabase/verification/17092026_0800_weekly_source_workbench_seams_v1.sql',
  'supabase/verification/13102026_2111_healthroster_report_native_fin_worker_v1.sql'];
const verifiers=files.map(p=>{
  const cl=inv.readManagedClosure(p,root);const c=calls(cl.expanded);
  // SQL held by pg_temp expected-failure arguments is executed only by that
  // exact fixture helper; record all such SQL-like literals conservatively.
  const expected=c.dynamic.flatMap(d=>[...d.sql.matchAll(/\b((?:public|private)\.[a-z_0-9]+)\s*\(/gi)].map(m=>m[1]));
  const full=closureCalls(inv,[...c.direct,...c.dynamicCalls,...expected],c.writes);
  return {...fileFact(p),closureHash:cl.closureHash,includes:cl.ordered.map(l=>fileFact(l.path)),...c,
    conservativeCalleeClosure:full};
});
const pins=JSON.parse(fs.readFileSync(path.join(own,'proposals/automatic-native-delta/NATIVE_INPUT_PINS.json')));
const nativePinComparison=pins.inputs.map(p=>({...p,current:fileFact(p.path),unchanged:p.sha256===fileFact(p.path).sha256}));
const names=['private.weekly_source_effective_inventory_v1','private.weekly_source_inventory_approval_basis_v2',
 'private.weekly_source_entitlement_components_v1','private.weekly_source_initial_additional_components_v2',
 'private.weekly_source_local_publication_context_v2','public.weekly_exceptional_pay_complete_local_v1',
 'private.weekly_source_pay_query_local_receipt_v1','private.weekly_source_pay_query_c1_receipt_v1','private.weekly_source_pay_query_next_receipt_v1',
 'public.weekly_exceptional_pay_stage_c1_request_v1','private.weekly_source_automatic_reconcile_certificate_v1',
 'public.weekly_source_automatic_reconcile_atomic_v1','private.weekly_source_bulk_source_cas_v1',
 'private.weekly_source_bulk_producer_ready_v1','private.weekly_source_automatic_reconcile_producer_ready_v1',
 'private.timesheet_correction_native_original_reversal_v1','public.timesheet_correction_authorise_atomic_v1',
 'private.weekly_source_finalise_engine_v1'];
const nativeFinalOwners=names.map(name=>({name,owners:[...inv.functions.values()].filter(f=>f.name===name).map(({sql,body,...f})=>f)}));
const out={schema_version:'AUTOMATIC_B9E2_SOURCE_CENSUS_V1',sourceCommit:commit,observedHead,
 qualification:'OFFLINE_FULL_BYTE_AND_CONSERVATIVE_CALL_CENSUS_ONLY_NOT_RUNTIME_OR_DATABASE_COMPILE',
 canonicalRootCount:inv.roots.length,functionTypedSignatureKeyCount:inv.functions.size,
 verifiers,nativePinComparison,nativeFinalOwners,
 databaseCalls:false,networkCalls:false};
fs.writeFileSync(path.join(here,'SOURCE_CENSUS.json'),JSON.stringify(out,null,2)+'\n');
console.log(JSON.stringify({verifiers:verifiers.map(v=>({path:v.path,owners:v.conservativeCalleeClosure.owners.length,
 unresolved:v.conservativeCalleeClosure.unresolved,dynamic:v.executeLines})),
 pinChanges:nativePinComparison.filter(p=>!p.unchanged),nativeFinalOwners:nativeFinalOwners.map(n=>({name:n.name,owners:n.owners.map(f=>({path:f.path,key:f.key,sha256:f.statementSha256}))}))}));
