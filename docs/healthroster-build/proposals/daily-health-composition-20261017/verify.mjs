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
const sql=read(path.join(here,'17102026_0120_daily_health_composition.sql')),parsed=scanManagedSql(sql);
assert.equal(parsed.statements.length,10);assert.equal(parsed.meta.length,0);
export function validateHealthSource(source){
 const functions=source.split(/(?=create or replace function private\.)/).filter(x=>x.startsWith('create or replace function'));
 assert.equal(functions.length,5,'exact five internal Health helpers');
 assert(!/create or replace function public\./.test(source),'no public completion');
 for(const name of ['daily_rate_correction_review_v1','daily_rate_correction_accept_v1','daily_rate_correction_result_v1'])assert(!source.includes('create or replace function private.'+name+'('),'no substitute missing public delegate');
 assert(!/\b(insert into public\.|update public\.|delete from|create table|bpay_next_daily_rate_correction_publish_v1\s*\()/i.test(source),'no business mutation/publisher');
 const basis=functions.find(x=>x.startsWith('create or replace function private.daily_rate_correction_health_basis_v1('));
 const contextAt=basis.indexOf('context:=private.daily_rate_correction_context_v1('),recheckAt=basis.indexOf('perform private.daily_rate_correction_health_actor_require_v1(actor,p_lock);');
 assert(contextAt>=0&&recheckAt>contextAt,'actual actor rechecked after context waits');
 assert(basis.indexOf("current_setting('transaction_isolation')")<contextAt&&basis.includes("is distinct from 'read committed'"),'old snapshot refusal before native locks');
 const accept=functions.find(x=>x.startsWith('create or replace function private.daily_rate_correction_health_accept_basis_v1('));
 const nativeAt=accept.indexOf('daily:=private.daily_rate_correction_health_basis_v1(preview_args,true);');
 const invoiceAt=accept.indexOf('invoice_preflight:=private.invoice_contract_move_preflight_v1(review.id,actor,true);');
 const metadataAt=accept.indexOf('into strict locked_o');assert(nativeAt>=0&&invoiceAt>nativeAt&&metadataAt>invoiceAt,'native then Invoice then metadata locks');
 assert(accept.includes('to_jsonb(locked_o) is distinct from to_jsonb(o)'),'post-wait operation CAS');
 const prepare=functions.find(x=>x.startsWith('create or replace function private.daily_rate_correction_health_review_prepare_v1('));
 assert(prepare.includes('insert into private.timesheet_contract_move_operations(')&&prepare.includes('insert into private.timesheet_contract_move_members('),'real bounded metadata initializer');
 assert(!/insert into private.timesheet_contract_move_(reviews|requests)/.test(prepare),'no pretend sealed review/request');
 return {functions:5,publicComposers:0,businessWrites:0};
}
validateHealthSource(sql);
assert.equal(scanManagedSql(read(path.join(here,'17102026_0120_daily_health_composition_reverse.sql'))).statements.length,5);
const invoice=read(path.join(root,'supabase/repeatable/16102026_0955_nhsp_v6_invoice_native_authority/01_complete_invoice_authorities.sqlinc'));
assert(invoice.includes("array['context','calculation_snapshot','normalized_snapshot','rate_provenance']"),'actual existing0955 Daily input');
const guard=read(path.join(root,'supabase/repeatable/includes/nhsp_v6_health_review_context.sqlinc'));
assert(guard.includes('o.expected_scope_hash is null or o.before_pay_cut_hash is null or o.before_invoice_hash is null'));
assert(guard.includes("raise exception 'CONTRACT_MOVE_INCOMPLETE_TRANSACTION'"));assert(guard.includes('deferrable initially deferred'));
console.log('PASS: exact7 native source files/9 final owners;5 internal Health bodies/full reverse; existing0955 Daily ABI/deferred incomplete guard; native-before-metadata/current-actor/isolation boundaries');
