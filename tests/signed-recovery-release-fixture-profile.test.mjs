import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';

test('Signed recovery fixture disables only transaction-local compilation and retains safety checks',()=>{
 const sql=readFileSync(new URL('./01092026_1511_banking_pay_signed_recovery_draft_runtime_verification.sql',import.meta.url),'utf8');
 assert.match(sql,/begin;[\s\S]*set local jit=off;/i);
 assert.match(sql,/set local statement_timeout='45s';/i);
 assert.match(sql,/set local lock_timeout='5s';/i);
 assert.match(sql,/rollback;\s*$/i);
 for(const marker of ['BANKING_PAY_SIGNED_RECOVERY_FINALIZER_DID_NOT_SETTLE','BANKING_PAY_SIGNED_RECOVERY_ACTIVE_DUPLICATE_ACCEPTED','BANKING_PAY_SIGNED_RECOVERY_CANCELLED_RELEASE_DID_NOT_SETTLE','BANKING_PAY_SIGNED_RECOVERY_FINALIZER_CROSSED_PROVIDER_BOUNDARY']) assert.ok(sql.includes(marker),marker);
 assert.doesNotMatch(sql,/alter\s+(?:system|database|role)|set\s+(?:session\s+)?jit\s*=/i);
});
