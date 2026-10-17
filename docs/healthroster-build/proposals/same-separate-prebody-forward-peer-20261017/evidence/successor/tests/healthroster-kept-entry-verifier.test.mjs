import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {scanManagedSql} from '../scripts/cloudtms-managed-release-sql.mjs';
import {routineDefinitions} from '../scripts/bpay-next-final-definers.mjs';
import {prebodyDependencySource} from '../scripts/healthroster-prebody-dynamic-review.mjs';
const hash=value=>createHash('sha256').update(value).digest('hex');
const file='supabase/verification/17102026_0144_kept_pay_and_correction_entry_security.sql';
const source=fs.readFileSync(file,'utf8');
const expected=[...source.matchAll(/\('((?:private|public)\.[a-z_0-9]+\([^']*\))',(false|true),'([^']+)'\)/g)].map(m=>({signature:m[1],service:m[2]==='true',searchPath:m[3]}));
test('prebody proof reverses only the exact reviewed unrelated replay-pin successor',()=>{
 const proof=JSON.parse(fs.readFileSync('docs/healthroster-build/v6-owner-delivery-20261016/RPC_RELOAD_EXECUTION_PIN_SUCCESSOR_20261017.json'));
 const current=fs.readFileSync(proof.policy,'utf8'),prior=current.replace(proof.after,()=>proof.before);
 const input={file:proof.policy,sha256:hash(prior)};
 assert.equal(prebodyDependencySource(input,current),prior);
 assert.equal(prebodyDependencySource(input,current.replaceAll('\n','\r\n')),prior);
 assert.equal(prebodyDependencySource(input,prior),prior);
 for(const altered of [current+'\n// drift',current.replace(proof.after,''),current+proof.after,current.replace('SOURCE_REASSERT','BACKFILL')])
  assert.throws(()=>prebodyDependencySource(input,altered),/BOUND_SOURCE_CHANGED/);
 assert.equal(prebodyDependencySource({...input,file:'scripts/unreviewed.mjs'},current),current);
});
test('mandatory verifier search paths and service boundaries match ten complete actual final owners',()=>{
 assert.equal(expected.length,10);assert.equal(expected.filter(r=>r.service).length,2);
 const registry=JSON.parse(fs.readFileSync('docs/bpay-next/final-definer-closures.json'));
 for(const row of expected){
  const chains=registry.chains.filter(c=>c.signature===row.signature);assert.equal(chains.length,1);
  const text=fs.readFileSync(chains[0].finalPhysical,'utf8');
  const statements=scanManagedSql(text).statements.map(s=>text.slice(s.start,s.end)).filter(s=>routineDefinitions(s,chains[0].finalPhysical).includes(row.signature));
  assert.equal(statements.length,1);const header=statements[0].split(/\bas\s+\$[a-z_0-9]*\$/i)[0];
  assert.match(header,/security\s+definer/i);
  const actual=/set\s+search_path\s*(?:to\s+|=\s*)([^\n\r]+)/i.exec(header);assert(actual,row.signature);
  assert.equal(actual[1].replace(/[\s']/g,''),row.searchPath,row.signature);
  assert.equal(row.service,row.signature.startsWith('public.'));
 }
});
test('independent ACL expected-set additions reverse to the entire original mandatory verifier',()=>{
 const proof=JSON.parse(fs.readFileSync('docs/healthroster-build/v6-owner-delivery-20261016/KEPT_ENTRY_MANDATORY_ACL_VERIFIER_20261017.json'));
 const successor=JSON.parse(fs.readFileSync('docs/healthroster-build/v6-owner-delivery-20261016/LATER_KEEP_MANDATORY_VERIFIERS_20261017.json'));
 const same=JSON.parse(fs.readFileSync('docs/healthroster-build/v6-owner-delivery-20261016/SAME_SEPARATE_SECURITY_EXPECTED_SET_FORWARD_20261017.json'));
 let value=fs.readFileSync(proof.target,'utf8');assert.equal(hash(value),same.afterSha256);
 assert.equal(value.split(same.hunk.after).length,2);value=value.replace(same.hunk.after,()=>same.hunk.before);
 assert.equal(hash(value),same.beforeSha256);assert.equal(hash(value),successor.outputSha256);
 for(const h of [...successor.hunks].reverse()){assert.equal(value.split(h.after).length,2);value=value.replace(h.after,()=>h.before);}
 assert.equal(hash(value),successor.inputSha256);assert.equal(hash(value),proof.outputSha256);
 for(const h of [...proof.hunks].reverse()){assert.equal(value.split(h.after).length,2);value=value.replace(h.after,()=>h.before);}
 assert.equal(hash(value),proof.inputSha256);
 assert.equal(value,execFileSync('git',['show',proof.sourceCheckpoint+':'+proof.target],{encoding:'utf8'}));
 const registered=JSON.parse(fs.readFileSync('supabase/release/current-release.json'));
 for(const key of ['verificationFiles','newVerificationFiles'])assert.equal(registered[key].filter(p=>p===file).length,1);
});
