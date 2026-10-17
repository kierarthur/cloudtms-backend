// Source-only verification. No DB, network, subprocess, fixture mutation or C path.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';import {fileURLToPath,pathToFileURL} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(here,'../../../..'),root=path.resolve(own,'../healthroster-install2-14102026');
for(const p of [own,root])assert(fs.realpathSync(p).replaceAll('\\','/').startsWith('D:/Development/GitHub/'),'PHYSICAL_D_ROOT_REQUIRED');
const {scanManagedSql}=await import(pathToFileURL(path.join(own,'scripts/cloudtms-managed-release-sql.mjs')));
const read=p=>fs.readFileSync(p,'utf8').replaceAll('\r\n','\n'),sha=s=>createHash('sha256').update(s).digest('hex');
const guards=JSON.parse(read(path.join(here,'SOURCE_GUARDS.json'))),registry=JSON.parse(read(path.join(root,'docs/bpay-next/final-definer-closures.json')));
for(const row of guards.files)assert.equal(sha(read(path.join(root,row.file))),row.canonicalSha256,row.file);
for(const row of guards.finalOwners){const current=registry.chains.find(c=>c.signature===row.signature);assert(current,row.signature);assert.equal(current.finalRoot,row.finalRoot);assert.equal(current.finalPhysical,row.finalPhysical);}
const pack='D:/Development/Legacy-Work/C-tmp/healthroster-handover/HealthRoster_Builder_Pack_16102026/NHSP_AUTHORISATION_AUDIT_15102026_HANDOFF_V6';
assert(fs.realpathSync(pack).replaceAll('\\','/').startsWith('D:/Development/Legacy-Work/'),'PHYSICAL_D_PACK_REQUIRED');
for(const row of guards.policyFiles)assert.equal(sha(read(path.join(pack,row.file))),row.canonicalSha256,row.file);
for(const row of guards.readerPolicyBasis){const source=read(path.join(pack,row.file));const actual=row.line?source.split('\n')[row.line-1]:row.pointer.split('/').slice(1).reduce((v,k)=>v[k],JSON.parse(source));assert.equal(actual,row.exact,'exact current reader policy');}
const sql=read(path.join(here,'17102026_0120_daily_health_composition.sql')),parsed=scanManagedSql(sql);
assert.equal(parsed.statements.length,12);assert.equal(parsed.meta.length,0);
export function validateHealthSource(source){
 const functions=source.split(/(?=create or replace function private\.)/).filter(x=>x.startsWith('create or replace function'));
 assert.equal(functions.length,6,'exact six internal Health helpers');
 assert(!/create or replace function public\./.test(source),'no public completion');
 for(const name of ['daily_rate_correction_review_v1','daily_rate_correction_accept_v1','daily_rate_correction_result_v1'])assert(!source.includes('create or replace function private.'+name+'('),'no substitute missing public delegate');
 assert(!/\b(insert into public\.|update public\.|delete from|create table|bpay_next_daily_rate_correction_publish_v1\s*\()/i.test(source),'no business mutation/publisher');
 const basis=functions.find(x=>x.startsWith('create or replace function private.daily_rate_correction_health_basis_v1('));
 const contextAt=basis.indexOf('context:=private.daily_rate_correction_context_v1('),recheckAt=basis.indexOf('perform private.daily_rate_correction_health_actor_require_v1(actor,p_lock);');
 assert(contextAt>=0&&recheckAt>contextAt,'actual actor rechecked after context waits');
 assert(basis.indexOf("current_setting('transaction_isolation')")<contextAt&&basis.includes("is distinct from 'read committed'"),'old snapshot refusal before native locks');
 const accept=functions.find(x=>x.startsWith('create or replace function private.daily_rate_correction_health_accept_basis_v1('));
 const reason=functions.find(x=>x.startsWith('create or replace function private.daily_rate_correction_health_reason_note_v1('));
 assert(reason.includes("private.timesheet_contract_move_reason_v1(note,'DAILY_RATE_CORRECTION') is distinct from p_stored_reason"),'native canonical reason roundtrip');
 assert(accept.includes("'reason',private.daily_rate_correction_health_reason_note_v1(o.reason)"),'canonical stored reason must become exact raw Details');
 assert(accept.includes("private.daily_rate_correction_request_hash_v1(preview_args,'PREVIEW') is distinct from review.preview_request_hash"),'recovered reason must match immutable original request hash');
 const nativeAt=accept.indexOf('daily:=private.daily_rate_correction_health_basis_v1(preview_args,true);');
 const invoiceAt=accept.indexOf('invoice_preflight:=private.invoice_contract_move_preflight_v1(review.id,actor,true);');
 const metadataAt=accept.indexOf('into strict locked_o');assert(nativeAt>=0&&invoiceAt>nativeAt&&metadataAt>invoiceAt,'native then Invoice then metadata locks');
 assert(accept.includes('to_jsonb(locked_o) is distinct from to_jsonb(o)'),'post-wait operation CAS');
 const prepare=functions.find(x=>x.startsWith('create or replace function private.daily_rate_correction_health_review_prepare_v1('));
 assert(prepare.includes('insert into private.timesheet_contract_move_operations(')&&prepare.includes('insert into private.timesheet_contract_move_members('),'real bounded metadata initializer');
 assert(!/insert into private.timesheet_contract_move_(reviews|requests)/.test(prepare),'no pretend sealed review/request');
 const result=functions.find(x=>x.startsWith('create or replace function private.daily_rate_correction_health_result_basis_v1('));
 assert(!result.includes('perform private.daily_rate_correction_health_actor_require_v1('),'GET must not inherit mutation Admin gate');
 assert(result.includes('where u.id=p_actor_user_id and u.is_active'),'current active reader is required');
 assert(result.includes("'financial_command_id',null,'current_financial_cut_id',null")&&!result.includes("'placement_receipt',o.placement_receipt_json"),'no raw financial receipt/IDs before genuine Bank visibility join');
 return {functions:6,publicComposers:0,businessWrites:0};
}
validateHealthSource(sql);
assert.equal(scanManagedSql(read(path.join(here,'17102026_0120_daily_health_composition_reverse.sql'))).statements.length,6);
const invoice=read(path.join(root,'supabase/repeatable/16102026_0955_nhsp_v6_invoice_native_authority/01_complete_invoice_authorities.sqlinc'));
assert(invoice.includes("array['context','calculation_snapshot','normalized_snapshot','rate_provenance']"),'actual existing0955 Daily input');
const guard=read(path.join(root,'supabase/repeatable/includes/nhsp_v6_health_review_context.sqlinc'));
assert(guard.includes('o.expected_scope_hash is null or o.before_pay_cut_hash is null or o.before_invoice_hash is null'));
assert(guard.includes("raise exception 'CONTRACT_MOVE_INCOMPLETE_TRANSACTION'"));assert(guard.includes('deferrable initially deferred'));
const http=read(path.join(root,'broker/src/daily-rate-correction.js'));assert(http.includes("actor(request,env,deps,kind!=='result')"),'actualGET current-active reader boundary');
const publicServices=read(path.join(root,'supabase/repeatable/16102026_1342_nhsp_v6_daily_public_services/01_public_services.sqlinc'));
const publicRead=publicServices.slice(publicServices.indexOf('create or replace function public.timesheet_daily_rate_correction_result_v1'));
assert(publicRead.includes('u.id=actor and u.is_active')&&!publicRead.includes('weekly_source_pay_actor_allowed_v1'),'actualSQLresult active/current reader admission');
console.log('PASS: current pinned native sources/final owners;6 internal Health bodies/full reverse; existing0955 Daily ABI/deferred guard; reason roundtrip/current-role read/native-lock boundaries');
