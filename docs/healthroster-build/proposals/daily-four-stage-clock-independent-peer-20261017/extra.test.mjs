// Independent fictional RAM-only factory vectors; no actual native data or writes.
import fs from'node:fs';import{pathToFileURL}from'node:url';import assert from'node:assert/strict';import test from'node:test';
const own=new URL('./',import.meta.url),old='D:/Development/GitHub/.codex-worktrees/healthroster-v2-money-build-13102026/.codex-tmp/daily-original-stages-after-recorder-source-20261017-97359412021530d3f905efb354049bc4/',next='D:/Development/GitHub/.codex-worktrees/healthroster-v2-money-build-13102026/.codex-tmp/daily-original-stages-after-response-clock-forward-20261017-dacda1e88ccd48c7473b40c243d65e38/';
let code=fs.readFileSync(new URL('capture/test.mjs',own),'utf8').split("test('all four stages return")[0].replace("'./recorder.mjs'",JSON.stringify(pathToFileURL(next+'recorder.mjs').href));
const{vector}=await import('data:text/javascript;base64,'+Buffer.from(code+'\nexport{vector};').toString('base64'));
const oldPrepare=(await import(pathToFileURL(old+'recorder.mjs').href)).prepareOriginalStageAfter,newPrepare=(await import(pathToFileURL(next+'recorder.mjs').href)).prepareOriginalStageAfter;
for(const stage of['S2','S3']){
 test(stage+' mixed earlymain/laterfresh cannot masquerade as actual AFTER',()=>{const{a}=vector(stage),v=JSON.parse(a.auxAfterBytes),done=Date.parse(JSON.parse(a.responseBytes).finishedAt);v.statements[1].rows[0].s23_observer.observedAt=new Date(done-100).toISOString();v.rollbackVerification.statements[0].rows[0].s23_observer.observedAt=new Date(done+1).toISOString();a.auxAfterBytes=Buffer.from(JSON.stringify(v,null,2)+'\n');assert.equal(oldPrepare(a).records.length,4);assert.throws(()=>newPrepare(a),/ACTUAL_AUXILIARY_AFTER_RESPONSE_ORDER_REQUIRED/);});
 test(stage+' exact response boundary retains all four original byte images',()=>{const{a}=vector(stage),v=JSON.parse(a.auxAfterBytes),done=Date.parse(JSON.parse(a.responseBytes).finishedAt);v.statements[1].rows[0].s23_observer.observedAt=new Date(done).toISOString();v.rollbackVerification.statements[0].rows[0].s23_observer.observedAt=new Date(done+1).toISOString();a.auxAfterBytes=Buffer.from(JSON.stringify(v,null,2)+'\n');assert.deepEqual(newPrepare(a),oldPrepare(a));});
}

