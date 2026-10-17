// New operational owner tests; memory tools only. No source admission, child or native call.
import test from'node:test';import assert from'node:assert/strict';import fs from'node:fs';
import{runReadyCost}from'./root-tools.mjs';import{synthetic}from'./fixtures.mjs';import{validateCost}from'./validator.mjs';
const read=f=>fs.readFileSync(new URL('./'+f,import.meta.url),'utf8'),p=JSON.parse(read('RUNTIME_FORWARD.json')),NEW='docs/healthroster-build/proposals/qt-audit-readonly-cost1000-success-runtime-20261017',OLD='docs/healthroster-build/proposals/qt-audit-readonly-cost1000-20261017';
test('actual complete controller JSON_TEXT path uses only the new runtime root-io for all three operations',async()=>{
 const f=synthetic('JSON_TEXT'),out='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026/.codex-tmp/qt-audit-cost1000-20261017-'+'a'.repeat(32),ready={status:'COST_INPUT_READY',ownedDirectory:out,inputFile:'INPUT.json',nativeResultFile:'NATIVE_RESULT.json',remainingWorkMs:100000,holderStartedAt:new Date(Date.now()-1000).toISOString(),compatibility:'ROOT_REVIEW_REQUIRED',businessScope:'NOT_RUN',mode:'JSON_TEXT'},io=[];
 for(const row of[f.result.statements[1],f.result.statements[3],f.result.rollbackVerification.statements[0]])row.rows[0].cost_clock.observedAt=new Date(Date.now()-1000).toISOString();
 const tools={exec_command:async a=>{assert(a.cmd.includes(NEW+'/root-io.mjs'));assert(!a.cmd.includes(OLD+'/root-io.mjs'));io.push(a.cmd);return{exit_code:0,output:JSON.stringify(a.cmd.includes("'input'")?f.built:a.cmd.includes("'result'")?{status:'COST_VALIDATED_RESULT_SAVED',capture:validateCost(f.result,f.built)}:{status:'COST_CAPTURED_OWN_TURN_RELEASED',originalSharedTurnCompleted:true})};},mcp__cloudtms_miget_operations_kier_codex__miget_db_transaction_rollback:async input=>{assert.equal(input.label,f.built.input.label);return{structuredContent:f.result};},write_stdin:async a=>a.chars==='RESULT_READY\n'?{output:JSON.stringify({...ready,status:'COST_CAPTURED_PENDING_RELEASE'})+'\n'}:a.chars==='RELEASE\n'?{exit_code:0,output:JSON.stringify({status:'COST_CAPTURED_OWN_TURN_RELEASED',ownedDirectory:out,businessScope:'NOT_RUN',compatibility:'ROOT_REVIEW_REQUIRED_NOT_QUALIFIED'})+'\n'}:{exit_code:1,output:''}};
 const r=await runReadyCost(tools,{ready,mode:'JSON_TEXT',sourceCommit:'c'.repeat(40),sessionId:456});assert.equal(r.status,'BOUNDED_COST_CAPTURE_OWN_TURN_RELEASED');assert.equal(io.length,3);assert.equal(r.mode,'JSON_TEXT');
});
test('complete runtime owners are exactly two path rebases and one explicit actual receipt guard',()=>{
 for(const file of['root-tools.mjs','source.mjs']){const r=p.files.find(x=>x.file===file);let text=read('reference/runtime-before/'+file);for(const e of r.edits){assert.equal(text.split(e.before).length-1,e.count);text=text.split(e.before).join(e.after);}assert.equal(text,read(file));}
 assert.equal(p.files.find(x=>x.file==='root-tools.mjs').edits.length,1);assert.equal(p.files.find(x=>x.file==='source.mjs').edits.length,2);
 for(const file of['root-tools.mjs','source.mjs']){assert(!read(file).includes(OLD));assert(read(file).includes(NEW));}
});
test('validator and all original clock bounds remain complete raw parent bytes',()=>{
 const old=JSON.parse(read('history/RUNTIME_PARENT_MANIFEST.json')),r=old.files.find(x=>x.file==='validator.mjs');assert.equal(Buffer.byteLength(read('validator.mjs')),r.bytes);assert(read('validator.mjs').includes('t<=now+1000&&now-t<=30000'));assert(read('validator.mjs').includes('times[1]-times[0]<=30000'));
 assert(!p.files.some(r=>['validator.mjs','factory.mjs','hold.mjs','protocol.mjs','root-io.mjs'].includes(r.file)));
});
test('operational constants have no remaining old-path dispatch',()=>{
 for(const file of['root-tools.mjs','source.mjs','launch.mjs','hold.mjs','root-io.mjs','factory.mjs','validator.mjs','protocol.mjs','prepare-holder.mjs'])assert(!read(file).includes(OLD),file);
 const s=read('source.mjs');for(const literal of[p.receiptCommit,p.receiptSha256,'receipt.length===32625','[rootHead,receiptCommit,null]','COST_COMMITTED_ACTUAL_RECEIPT_REQUIRED'])assert(s.includes(literal),literal);
});
