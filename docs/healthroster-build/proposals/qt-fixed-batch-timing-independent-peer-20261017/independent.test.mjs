// All rows/turns/outputs below are fictional source/transport vectors, never native proof.
import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import path from 'node:path';import vm from 'node:vm';import {fileURLToPath} from 'node:url';import {createHash} from 'node:crypto';
import {BatchTimingCapture,compileBatchTimingInput,batchTimingMarker,batchTimingProgressFor} from 'file:///D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/.codex-tmp/qt-fixed-batch-timing-source-20261017/payload/batch-timing-diagnostic.mjs';
import {joinNativeSnapshotFrames,BASE_KEYS,BACKGROUND_KEYS,SNAPSHOT_KEYS,compileNativeBatch} from 'file:///D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/.codex-tmp/qt-fixed-batch-timing-source-20261017/parent/batch-source.mjs';
import {SqlstateStderrCapture} from 'file:///D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/.codex-tmp/qt-fixed-batch-timing-source-20261017/parent/metadata-diagnostic.mjs';
const H="D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/.codex-tmp/qt-fixed-batch-timing-source-20261017",read=f=>fs.readFileSync(H+'/'+f),sha=b=>createHash('sha256').update(b).digest('hex');
const readerText=read('parent/fixed-metadata-reader.mjs').toString(),ctx=vm.createContext({Buffer,TextDecoder,SqlstateStderrCapture,joinNativeSnapshotFrames,sha,LIMITS:{stdoutBytes:64*1024*1024,stderrBytes:16384},check:(v,c)=>{if(!v)throw Error(c);}});
new vm.Script(readerText.slice(readerText.indexOf('export class BoundedMemoryCapture'),readerText.indexOf('function assertPins')).replace('export class ','globalThis.OriginalCapture=class ')).runInContext(ctx);
const peer=JSON.stringify({database:'cloudtms_test_clone',databaseOid:17,holderPid:11,readerPid:12,readOnly:true,isolation:'repeatable read',noRoleSwitch:true});
const values=Object.fromEntries(SNAPSHOT_KEYS.map(k=>[k,k==='backgroundRows'?{'private.candidate_daily_batch_receipts':[{keyHash:'a'.repeat(64),rowHash:'b'.repeat(64),controlled:'é›ª'}],'public.ts_financials_outbox':[]}:k==='backgroundSnapshotXip'?[]:k==='businessRelations'?[]:k==='backgroundWriterRoutines'?[]:k==='markerAbsent'||k==='identityTest'||k==='customerKeyNull'||k==='backgroundSnapshotEpochZero'?true:k==='applyingCount'||k==='otherActivityCount'?0:k==='module'?'LEGACY:1':k==='physicalDatabase'?'cloudtms_test_clone':'controlled']));
const native=keys=>'{'+SNAPSHOT_KEYS.filter(k=>keys.includes(k)).map(k=>JSON.stringify(k)+': '+JSON.stringify(values[k])).join(', ')+'}',base=native(BASE_KEYS),bg=native(BACKGROUND_KEYS),original=peer+'\n'+base+'\n'+bg+'\n';
const parts=[batchTimingMarker(0),batchTimingMarker(1),peer,batchTimingMarker(2),batchTimingMarker(3),batchTimingMarker(4),batchTimingMarker(5),base,bg,batchTimingMarker(6),batchTimingMarker(7)],wire=parts.join('\n')+'\n';
function fresh(options={}){const raw=[],delegate=new ctx.OriginalCapture('BEFORE',true),originalStdout=delegate.stdout.bind(delegate);delegate.stdout=b=>{raw.push(Buffer.from(b));originalStdout(b);};let n=0;const now=()=>{const x=options.clock?.(n)??{mono:n,wall:new Date(Date.UTC(2026,9,17,1,0,0,n)).toISOString()};n++;return x;};return{capture:new BatchTimingCapture(delegate,now),raw};}

for(const [name,wireValue] of [
 ['markerNUL',wire.replace(batchTimingMarker(4),batchTimingMarker(4)+'\0')],
 ['markerunicode',wire.replace(batchTimingMarker(5),batchTimingMarker(5)+'雪')],
 ['delayedduplicate',wire+parts[0]+'\n'],
 ['earlyJSON',peer+'\n'+wire],
 ['partialJSONafterRollback',wire+'{'],
 ['unclosedCRmarker',wire.slice(0,-1)+'\r']])test('independent original delegate refuses '+name,()=>{const h=fresh();for(let i=0;i<Buffer.byteLength(wireValue);i+=7)h.capture.stdout(Buffer.from(wireValue).subarray(i,i+7));assert.throws(()=>h.capture.finish());});
test('independent every split point of complete original wire preserves bytes and 8 ordered bounds',()=>{const b=Buffer.from(wire);for(let cut=0;cut<=b.length;cut++){const h=fresh();h.capture.stdout(b.subarray(0,cut));h.capture.stdout(b.subarray(cut));const r=h.capture.finish();assert.equal(Buffer.concat(h.raw).toString(),original);assert.deepEqual(r.timingProgress.complete.map(x=>x.ordinal),[1,2,3,4,5,6,7,8]);assert.equal(r.timingProgress.restorationQualified,false);}});
test('independent stderr after completed rollback cannot qualify',()=>{const h=fresh();h.capture.stdout(wire);h.capture.stderr('ERROR:  57014\n');assert.throws(()=>h.capture.finish());assert.equal(h.capture.diagnosticSqlstate(true),'57014');});
