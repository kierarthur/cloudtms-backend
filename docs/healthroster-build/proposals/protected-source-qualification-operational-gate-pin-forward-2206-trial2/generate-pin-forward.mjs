import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

// Readonly Git-object pin review. No database, remote service or root mutation.
const out=path.dirname(fileURLToPath(import.meta.url)), own=path.resolve(out,'../../../..');
const root=path.resolve(own,'../healthroster-v2-backend-13102026');
const before='b65df0997ac69188eadf4de2a88f70226fa99532', after='d67f94e19000a612ee4f1099c14429d8a8b4bee1';
const previous=path.resolve(out,'../protected-source-qualification-operational-gate-pin-forward-2206');
const strict=path.resolve(out,'../protected-source-qualification-operational-gate');
const sha=b=>createHash('sha256').update(b).digest('hex');
const git=args=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{cwd:root,maxBuffer:5_000_000});
const show=(commit,file)=>git(['show',`${commit}:${file}`]);
function preserve(dir,expectedHash,files){
  const bytes=fs.readFileSync(path.join(dir,'REVIEW_MANIFEST.json'));assert.equal(sha(bytes),expectedHash);
  const m=JSON.parse(bytes);assert.equal(fs.readdirSync(dir).length,files);
  for(const f of m.files){const b=fs.readFileSync(path.join(own,f.file));assert.equal(sha(b),f.sha256,f.file);assert.equal(b.length,f.bytes,f.file);}
}
preserve(strict,'66b5b97d5628be25a0555c20c3a5755bf9561bd760afcaaaaed1b9b09f71b700',45);
preserve(previous,'10db4d8d3728129f429a0cfaf8d8670dbbf1125bcc9be53b60ff7d06b1664e8f',6);
const verifier='supabase/verification/13102026_2206_weekly_source_automatic_native_delta_v1.sql';
const changed=git(['diff','--name-status',before,after]).toString('utf8').trim().split('\n').map(line=>line.split('\t'));
const expected=['docs/healthroster-build/PROGRESS.md',
 'docs/healthroster-build/checkpoint13102026/AUTOMATIC_SHORT_COMPILE.sql',
 'docs/healthroster-build/checkpoint13102026/AUTOMATIC_SHORT_COMPILE_FRESH.sql',
 'docs/healthroster-build/checkpoint13102026/AUTOMATIC_SHORT_COMPILE_PLAN.json',
 'docs/healthroster-build/checkpoint13102026/AUTOMATIC_SHORT_COMPILE_TRIAL_1.json',
 'docs/healthroster-build/checkpoint13102026/AUTOMATIC_SHORT_COMPILE_TRIAL_1_SQL_CAPSULE.json',
 'docs/healthroster-build/checkpoint13102026/AUTOMATIC_SHORT_COMPILE_TRIAL_2.json',
 'docs/healthroster-build/checkpoint13102026/AUTOMATIC_SHORT_COMPILE_TRIAL_2.sql',
 'docs/healthroster-build/checkpoint13102026/AUTOMATIC_SHORT_COMPILE_TRIAL_2_FRESH.sql',
 'docs/healthroster-build/checkpoint13102026/AUTOMATIC_SHORT_COMPILE_TRIAL_2_PLAN.json',
 'docs/healthroster-build/checkpoint13102026/AUTOMATIC_SHORT_COMPILE_TRIAL_2_SQL_CAPSULE.json',
 'docs/healthroster-build/checkpoint13102026/prepare_automatic_short_compile.mjs',
 'docs/healthroster-build/checkpoint13102026/prepare_automatic_short_compile_v2.mjs',verifier];
assert.deepEqual(changed.map(([,file])=>file),expected);
assert.deepEqual(changed.filter(([,file])=>!file.startsWith('docs/')),[['M',verifier]]);
const old=show(before,verifier).toString('utf8'), current=show(after,verifier).toString('utf8');
let reversed=current;
function reverse(from,to){assert.equal(reversed.split(from).length-1,1);reversed=reversed.replace(from,()=>to);}
reverse("if not exists(select 1 from pg_catalog.pg_proc retained_proc where retained_proc.oid=pg_catalog.to_regprocedure(r.signature)\n      and pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(retained_proc.prosrc,'UTF8')),'hex')=r.body_sha256) then", "if not exists(select 1 from pg_catalog.pg_proc p where p.oid=pg_catalog.to_regprocedure(r.signature)\n      and pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(p.prosrc,'UTF8')),'hex')=r.body_sha256) then");
reverse('  -- Prove the missing selector before calling the complete owner; it maps\n  -- SELECT STRICT no_data_found to its closed 55000 unavailable refusal.\n  -- Never touch a real request or accept an arbitrary exception.', '  -- Prove the missing selector before calling SELECT STRICT; never touch a real request.');
for(const name of ['WEEKLY_SOURCE_STAGED_OWNERSHIP_UNAVAILABLE','WEEKLY_SOURCE_AUTOMATIC_RECONCILE_DELTA_UNAVAILABLE']){
 const call=name==='WEEKLY_SOURCE_STAGED_OWNERSHIP_UNAVAILABLE'?'private.weekly_source_staged_publication_context_v1':'private.weekly_source_automatic_reconcile_delta_assert_v1';
 reverse(`    perform ${call}(missing_id,missing_id);\n  exception when sqlstate '55000' then\n    if sqlerrm<>'${name}' then raise; end if; refused:=true;\n  end;`, `    perform ${call}(missing_id,missing_id);\n  exception when no_data_found then refused:=true; end;`);
}
assert.equal(reversed,old);
const census=JSON.parse(fs.readFileSync(path.join(strict,'COMPLETE_CLOSURE_CENSUS.json')));
assert.equal(census.root_source_commit,before);assert.equal(census.base_private_count,232);assert.equal(census.planned_private_count,258);
const pins=census.root_2205_release_acl_local_receipt_verifier_pins.map(p=>{
 const file=p.path.replace('../healthroster-v2-backend-13102026/',''), a=show(before,file), b=show(after,file);
 assert.equal(sha(a),p.sha256);assert.equal(file===verifier||a.equals(b),true,file);
 return {file,before_sha256:sha(a),after_sha256:sha(b),bytes:b.length,byte_identical:a.equals(b)};
});
assert.equal(pins.length,16);assert.equal(pins.filter(p=>!p.byte_identical).length,1);
const full=git(['diff','--binary','--full-index','--no-ext-diff',before,after]);
const runtime=git(['diff','--binary','--full-index','--no-ext-diff',before,after,'--',verifier]);
fs.writeFileSync(path.join(out,'ROOT_B65_TO_D67_FULL_DIFF.patch'),full);
fs.writeFileSync(path.join(out,'VERIFIER_2206_COMPLETE_FORWARD.patch'),runtime);
const record={schema_version:'PROTECTED_SOURCE_STRICT_GATE_ROOT_PIN_FORWARD_TRIAL2_V1',status:'FROZEN_SOURCE_PIN_FORWARD_NATIVE_OPERATIONAL_ACCEPTANCE_UNEXECUTED',root_before:before,root_after:after,
 predecessor_strict45_commit:'adec5f8b8a3955ac725c0da15ee344ac891b7674',predecessor_strict45_manifest_sha256:'66b5b97d5628be25a0555c20c3a5755bf9561bd760afcaaaaed1b9b09f71b700',
 predecessor_pin6_manifest_sha256:'10db4d8d3728129f429a0cfaf8d8670dbbf1125bcc9be53b60ff7d06b1664e8f',frozen_predecessor_files_verified_unchanged:51,
 changed_files:changed.map(([status,file])=>{const b=show(after,file);return{status,file,sha256:sha(b),bytes:b.length};}),
 full_diff:{file:'ROOT_B65_TO_D67_FULL_DIFF.patch',sha256:sha(full),bytes:full.length},runtime_diff:{file:'VERIFIER_2206_COMPLETE_FORWARD.patch',sha256:sha(runtime),bytes:runtime.length},
 updated_root_input_pins:pins,complete_reverse_hunks_match_entire_b65_verifier:true,
 verifier_changes:['Retained-owner alias p becomes retained_proc','Missing-stage exact55000/name rather than raw no_data_found','Missing-delta exact55000/name rather than raw no_data_found','Adjacent explanatory comment'],
 root_private_count_preserved:232,planned_complete_private_union:258,tables_preserved:105,service_signatures_preserved:142,new_signatures:0,new_service_grants:0,
 operational_prerequisite_v2:false,actual_sql_trial_acceptance:false,actual_native_financial_acceptance:false,actual_activation:false,actual_api_ui_phone:false,lane_database_calls:0,
 source_evidence_rule:'The original b65 and cdaf trial failures retain their exact execution identities. This selected verifier change is not same-source equivalence; new execution evidence requires the actual new identity. Definition/ACL progress is not financial or operational qualification.',
 integration_rule:'Use d67f94e as the root pin/current2206 hash; keep the frozen strict45 and older pin6 unchanged. All2205 Local/receipt/helper/ACL/release-registration inputs are byte-identical. Union the full26 absent dependencies against root232; remeasure actual catalogue.'};
fs.writeFileSync(path.join(out,'ROOT_PIN_FORWARD.json'),JSON.stringify(record,null,2)+'\n');
console.log(JSON.stringify({source_only:true,frozen51_unchanged:true,complete_verifier_reverse_proof:true,preserved_pins:15,changed_pins:1,verifier_sha256:sha(Buffer.from(current)),root_after:after,planned_union:258}));
