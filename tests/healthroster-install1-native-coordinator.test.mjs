import assert from 'node:assert/strict';
import test from 'node:test';
import {parseOneJsonResult,runNativeInstall1} from '../scripts/healthroster-install1-native-coordinator.mjs';

test('read-only probe accepts one JSON result alongside harmless psql control output',()=>{
 assert.deepEqual(parseOneJsonResult('BEGIN\n{"identityTest":true,"customerKeyNull":true}\nROLLBACK\n'),{identityTest:true,customerKeyNull:true});
 assert.deepEqual(parseOneJsonResult('[{"path":"supabase/example.sql","sha256":"a"}]\n'),[{path:'supabase/example.sql',sha256:'a'}]);
});

test('ambiguous or malformed probe results fail without returning database payloads',()=>{
 for(const value of ['{"secret":"withheld"}\n{"second":"withheld"}\n','{"secret":"withheld"\n','plain server diagnostic']){
  assert.throws(()=>parseOneJsonResult(value),error=>/^NATIVE_PROBE_JSON_(?:ENVELOPE_REFUSED|INVALID)$/.test(error.message)&&!error.message.includes('withheld'));
 }
});

test('native coordinator cannot run from import or the test runner entry',async()=>{
 await assert.rejects(runNativeInstall1(),/^Error: EXPLICIT_OWNER_INSTALL1_ENTRY_REQUIRED$/);
});
