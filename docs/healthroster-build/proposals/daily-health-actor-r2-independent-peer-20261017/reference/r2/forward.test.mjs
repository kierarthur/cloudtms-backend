// Synthetic source-boundary negatives, never native receipts.
import test from'node:test';import assert from'node:assert/strict';import fs from'node:fs';import{createHash}from'node:crypto';
import{verifyNativeTouchDependency,buildActorProgram}from'./payload/docs/healthroster-build/proposals/daily-health-actor-native-proof-20261017/build-program.mjs';
const dir=new URL('./payload/docs/healthroster-build/proposals/daily-health-actor-native-proof-20261017/',import.meta.url),pins=JSON.parse(fs.readFileSync(new URL('SOURCE_PINS.json',dir))),raw=fs.readFileSync(new URL('NATIVE_DEPENDENCY_DIAG_R1.json',dir)),hash=b=>createHash('sha256').update(b).digest('hex');
const changed=fn=>{const doc=JSON.parse(raw);fn(doc);const b=Buffer.from(JSON.stringify(doc));return{b,p:{...structuredClone(pins),nativeDependencyProof:{...pins.nativeDependencyProof,sha256:hash(b)}}};};
const touch=d=>d.definitions.find(r=>r.definitions[0].function_name==='tms_touch_updated_at');
test('complete genuine saved raw touch definition has exact approved raw and full-LF predecessor hashes',()=>{
 assert.equal(verifyNativeTouchDependency(pins,raw),'d5d7808078fd3fa302415a450a4e8255e6a2519f0a5f29b89416583637499b63');const r=touch(JSON.parse(raw)).definitions[0];assert.equal(hash(r.definition.replaceAll('\r\n','\n')),pins.userStore.touch.definition_sha256);assert.notEqual(hash(r.definition),pins.userStore.touch.definition_sha256);
});
test('altered diagnostic bytes refuse before using any metadata',()=>{
 for(const b of[Buffer.concat([raw,Buffer.from(' ')]),Buffer.from('{}')])assert.throws(()=>verifyNativeTouchDependency(pins,b),/DIAGNOSTIC_BYTES/);
});
test('synthetic local diagnostic edits cannot authorize another function, non-readonly source or incomplete match',()=>{
 for(const fn of[d=>touch(d).read_only=false,d=>touch(d).authenticated=false,d=>touch(d).matched=2,d=>touch(d).definitions[0].identity_arguments='caller text',d=>touch(d).definitions[0].function_name='other']){const{b,p}=changed(fn);assert.throws(()=>verifyNativeTouchDependency(p,b),/COMPLETE_PROOF|READONLY_PROOF/);}
});
test('all-LF/all-CRLF, trailing whitespace and changed function logic refuse even under synthetic metadata hashes',()=>{
 for(const alter of[s=>s.replaceAll('\r\n','\n'),s=>s.replaceAll('\r\n','\n').replaceAll('\n','\r\n'),s=>s+' ',s=>s.replace('return new','return old')]){const{b,p}=changed(d=>{const r=touch(d).definitions[0];r.definition=alter(r.definition);r.rawSha256=hash(r.definition);r.completeLfSha256=hash(r.definition.replaceAll('\r\n','\n'));});assert.throws(()=>verifyNativeTouchDependency(p,b),/EXACT_NATIVE_TOUCH_SOURCE/);}
});
test('emitted native comparison is exact raw only and preserves genuine actor/default/constraint checks',async()=>{
 const r=await buildActorProgram({root:'D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026'});assert.equal(r.sqlSha256,'4157d0e2d9de71e1a5d6bdef8523b8aed3c8e6ff45b01c5dcfa1f7722813f314');assert(r.sql.includes(pins.userStore.touch.nativeRawDefinitionSha256));assert(!r.sql.includes(pins.userStore.touch.definition_sha256));assert.doesNotMatch(r.sql,/\b(?:replace|regexp_replace)\s*\(\s*pg_get_functiondef/i);assert.doesNotMatch(r.sql,/\bset\s+local\s+role\b/i);
 assert.equal(r.effectiveRoleAclChecks,3);assert.equal(r.directRoleExecution,'NOT_RUN');assert(!Object.hasOwn(r,'directRoleRefusals'));assert.equal(r.matrixChecks,12);assert.equal(r.matrixDenials,10);assert.equal(r.sameTransactionChanges,3);assert.equal(r.constraintRefusals,1);assert(r.sql.includes('DAILY_ACTOR_NATIVE_EXACT_AFTER_STATE_MISMATCH'));
});
