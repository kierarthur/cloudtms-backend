import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {inventory} from '../scripts/cloudtms-db-release-lib.mjs';

test('Signed recovery fixture disables only transaction-local compilation and retains safety checks',()=>{
 const sql=readFileSync(new URL('./01092026_1511_banking_pay_signed_recovery_draft_runtime_verification.sql',import.meta.url),'utf8');
 assert.match(sql,/begin;[\s\S]*set local jit=off;/i);
 assert.match(sql,/set local statement_timeout='45s';/i);
 assert.match(sql,/set local lock_timeout='5s';/i);
 assert.match(sql,/rollback;\s*$/i);
 for(const marker of ['BANKING_PAY_SIGNED_RECOVERY_FINALIZER_DID_NOT_SETTLE','BANKING_PAY_SIGNED_RECOVERY_ACTIVE_DUPLICATE_ACCEPTED','BANKING_PAY_SIGNED_RECOVERY_CANCELLED_RELEASE_DID_NOT_SETTLE','BANKING_PAY_SIGNED_RECOVERY_FINALIZER_CROSSED_PROVIDER_BOUNDARY']) assert.ok(sql.includes(marker),marker);
 assert.doesNotMatch(sql,/alter\s+(?:system|database|role)|set\s+(?:session\s+)?jit\s*=/i);
});

test('Installed-owner reconciliation includes only the five unchanged recorded authorities',()=>{
 const sources=[
  ['07092026_2013_banking_pay_unpaid_cancellation_communication_v2_prepare_v1.sql','bd5a39c5858471ce26192e168a19ffc4e04b68ce15686ab26c31a131c72d07dd'],
  ['07092026_2140_banking_pay_manual_carry_forward_economic_identity_v1.sql','aacb9d0b6befa6f419f91291da117939799a11908e43923e8de76a56a094b99c'],
  ['08092026_0518_banking_pay_candidate_dirty_cohort_authority_v1.sql','e2aa40b029ec3dc4a2a51e4ab52e80f506e361377fef74f140a1b92753aab52d'],
  ['08092026_1200_banking_pay_cancel_return_selection_intent_v1.sql','025db9ff0d659c9bf2274e69a4134b6f6a18d429c5e63b85d74f29187673aea2'],
  ['09092026_0020_banking_pay_no_money_workbench_return_v1.sql','1a219f5fc3edaab759b7be86d68ccf9e999f4d72d7f72a284e7baff427cf6466'],
 ];
 const all=inventory().repeatables;
 const root=all.find(x=>x.path.endsWith('22092026_1620_banking_pay_installed_owner_reassert_v1.sql'));
 assert.ok(root);
 const expected=sources.map(([name])=>'supabase/repeatable/'+name);
 assert.deepEqual(root.paths.filter(x=>x!==root.path).sort(),expected.sort());
 for(const [name,hash] of sources)assert.equal(all.find(x=>x.path.endsWith('/'+name)).sha256,hash,name);
});
