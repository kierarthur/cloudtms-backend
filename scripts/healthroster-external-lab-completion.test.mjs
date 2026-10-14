import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import path from 'node:path';
import {assertExternalLabCompletion,awaitExternalLabCompletion,publishExternalLabCompletion} from './healthroster-external-lab-completion.mjs';
const receiptRoot=new URL('../docs/healthroster-build/native-alias-first-use-20261014/',import.meta.url);
const actual=JSON.parse(fs.readFileSync(new URL('NATIVE_LAB_RESULT.json',receiptRoot))),preparation=JSON.parse(fs.readFileSync(new URL('PREPARATION.json',receiptRoot)));
const expected={marker:preparation.marker,sqlSha256:preparation.sqlSha256,statementCount:52,postmasterStart:'2026-10-14T09:07:33.240096+00:00'};
const mutate=fn=>{const r=structuredClone(actual);fn(r);return r;};
test('actual52-statement rolled-back helper receipt validates only the bounded claim',()=>{const r=assertExternalLabCompletion(actual,expected);assert.equal(r.statementCount,52);assert.match(r.scope,/caller must independently prove shared turn/);});
for(const [name,fn,code] of [
 ['wrong database',r=>{r.database='other';},'NATIVE_RESULT'],
 ['different SQL',r=>{r.sqlSha256='a'.repeat(64);},'NATIVE_RESULT'],
 ['missing statement',r=>{r.statements.pop();},'NATIVE_RESULT'],
 ['unexpected SQL error',r=>{r.statements[24].error='55000';},'NATIVE_RESULT'],
 ['truncated result',r=>{r.statements[24].rowsTruncated=true;},'NATIVE_RESULT'],
 ['not rolled back',r=>{r.rolledBack=false;},'NATIVE_RESULT'],
 ['wrong final marker',r=>{r.statements.at(-1).rows[0].jsonb_build_object.marker='hr_alias_lab_'+'0'.repeat(32);},'FINAL_ASSERTION'],
 ['not fresh',r=>{r.rollbackVerification.connectedFresh=false;},'FRESH_CONNECTION'],
 ['fixture present',r=>{r.rollbackVerification.statements[0].rows[0].jsonb_build_object.fixtureCounts[0].fixture_matches=1;},'FRESH_RESTORATION'],
 ['duplicate relation hides another',r=>{r.rollbackVerification.statements[0].rows[0].jsonb_build_object.fixtureCounts[1].relation='public.candidates';},'FRESH_RESTORATION'],
 ['postmaster restarted',r=>{r.rollbackVerification.statements[0].rows[0].jsonb_build_object.postmasterStart='later';},'FRESH_RESTORATION'],
 ['legacy owner changed',r=>{r.rollbackVerification.statements[0].rows[0].jsonb_build_object.legacyOne=false;},'FRESH_RESTORATION'],
])test('refuses '+name,()=>assert.throws(()=>assertExternalLabCompletion(mutate(fn),expected),new RegExp(code)));

const temporaryRoot=path.resolve('.codex-tmp');fs.mkdirSync(temporaryRoot,{recursive:true});
async function scratch(work){const dir=fs.mkdtempSync(path.join(temporaryRoot,'external-lab-completion-unit-'));try{return await work(dir);}finally{const checked=fs.realpathSync(dir);if(path.dirname(checked)!==fs.realpathSync(temporaryRoot)||!path.basename(checked).startsWith('external-lab-completion-unit-'))throw Error('OWN_UNIT_CLEANUP_TARGET_REFUSED');fs.rmSync(checked,{recursive:true});}}
test('atomic file completion returns after both ownership gates without stdin',()=>scratch(async receiptDir=>{const signal=new AbortController().signal,calls=[];const promise=awaitExternalLabCompletion({receiptDir,expected,signal,assertHeld:async phase=>{calls.push(phase);},timeoutMs:1000,pollMs:5});publishExternalLabCompletion({receiptDir,result:actual,expected});const r=await promise;assert.equal(r.validation.serverRolledBack,true);assert.deepEqual(calls,['before-external-lab-completion','external-lab-completion-validated']);}));
test('no receipt reaches bounded deadline and caller receives failure',()=>scratch(receiptDir=>assert.rejects(awaitExternalLabCompletion({receiptDir,expected,signal:new AbortController().signal,assertHeld:async()=>{},timeoutMs:15,pollMs:5}),/DEADLINE_NO_RESULT/)));
test('existing receipt cannot qualify a new turn',()=>scratch(async receiptDir=>{fs.writeFileSync(path.join(receiptDir,'NATIVE_LAB_RESULT.json'),JSON.stringify(actual));await assert.rejects(awaitExternalLabCompletion({receiptDir,expected,signal:new AbortController().signal,assertHeld:async()=>{throw Error('must not probe');},timeoutMs:15,pollMs:5}),/PREEXISTING_RECEIPT/);}));
test('own turn loss stops waiting immediately',()=>scratch(async receiptDir=>{const controller=new AbortController();const loss=new Error('OWN_TEST_TURN_LOST');controller.abort(loss);await assert.rejects(awaitExternalLabCompletion({receiptDir,expected,signal:controller.signal,assertHeld:async()=>{},timeoutMs:1000,pollMs:5}),error=>error===loss);}));
test('publisher cannot overwrite another completed result',()=>scratch(async receiptDir=>{const existing=path.join(receiptDir,'NATIVE_LAB_RESULT.json');fs.writeFileSync(existing,'existing receipt');assert.throws(()=>publishExternalLabCompletion({receiptDir,result:actual,expected}),/PREEXISTING_RECEIPT/);assert.equal(fs.readFileSync(existing,'utf8'),'existing receipt');}));
test('publisher refuses failed native result before writing files',()=>scratch(async receiptDir=>{assert.throws(()=>publishExternalLabCompletion({receiptDir,result:mutate(r=>{r.rolledBack=false;}),expected}),/NATIVE_RESULT/);assert.equal(fs.readdirSync(receiptDir).length,0);}));
