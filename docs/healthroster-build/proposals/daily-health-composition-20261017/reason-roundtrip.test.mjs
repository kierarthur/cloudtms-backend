// Executable cross-boundary vectors using the unchanged real Worker normalizer.
// The adjacent native SQL programme executes the SQL helper itself (NOT_RUN).
import test from 'node:test';import assert from 'node:assert/strict';
import {normalizeReason} from '../../../../../healthroster-install2-14102026/broker/src/contract-correction-v3.mjs';
const prefix='Applying updated rates';
function recoverCanonicalDetails(stored){
 if(typeof stored!=='string')throw Error('invalid');
 const note=stored===prefix?'':stored.startsWith(prefix+' — ')?stored.slice(prefix.length+3):null;
 if(note===null||normalizeReason(note,'DAILY_RATE_CORRECTION')!==stored)throw Error('invalid');
 return note;
}
test('native Worker vectors preserve all1969 Unicode characters beyond stored1969 length',()=>{
 for(const details of ['x'.repeat(1969),'😀'.repeat(1969),'e\u0301'.repeat(984)+'z']){
  const stored=normalizeReason(details,'DAILY_RATE_CORRECTION');
  assert.equal([...details].length,1969);assert([...stored].length>1969);
  assert.equal(recoverCanonicalDetails(stored),details);
  assert.equal(normalizeReason(recoverCanonicalDetails(stored),'DAILY_RATE_CORRECTION'),stored);
  assert.throws(()=>normalizeReason(stored,'DAILY_RATE_CORRECTION'),'old accept path wrongly treated stored reason as rawnote');
 }
});
test('empty, Unicode-trimmed and prefix-containing legitimate Details round-trip exactly',()=>{
 for(const details of ['', '\ufeff\u00a0 details \u3000',prefix,prefix+' — details']){
  const stored=normalizeReason(details,'DAILY_RATE_CORRECTION');
  assert.equal(recoverCanonicalDetails(stored),details.trim());
  assert.equal(normalizeReason(recoverCanonicalDetails(stored),'DAILY_RATE_CORRECTION'),stored);
 }
});
test('malformed stored canonical strings and overlong notes refuse without adding a prefix',()=>{
 for(const stored of [null,'',prefix+' — ',prefix+' - details',' '+prefix,prefix+' — details ',prefix+' — \u00a0details',prefix+' — '+'😀'.repeat(1970)]){
  assert.throws(()=>recoverCanonicalDetails(stored));
 }
});
