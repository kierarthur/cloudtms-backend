// Original Controls is reused, including ABORT/EOF/order/refusal and owned stdin.pause disposal.
import fs from'node:fs';import{check}from'./factory.mjs';import{validateCost}from'./validator.mjs';
export{Controls}from'file:///D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/docs/healthroster-build/proposals/daily-health-s0-catalogue-holder-20261017/protocol.mjs';
export const BUDGET=Object.freeze({work:120000,cleanup:60000,whole:180000,resultBytes:65536,inputBytes:131072});
const names=new Set(['INPUT.json','INPUT_RECEIPT.json','CAPTURE_RECEIPT.json','SUMMARY.json','HOLD.json']);
export function save(out,name,value){check(names.has(name),'COST_FIXED_OUTPUT_REQUIRED');const b=Buffer.from(JSON.stringify(value,null,2)+'\n');check(b.length<=(name==='INPUT.json'?BUDGET.inputBytes:BUDGET.resultBytes),'COST_OUTPUT_BOUND_REQUIRED');fs.writeFileSync(out+'/'+name,b,{flag:'wx'});}
export function nativeResult(out){const p=out+'/NATIVE_RESULT.json',s=fs.lstatSync(p);check(s.isFile()&&!s.isSymbolicLink()&&s.size>0&&s.size<=BUDGET.resultBytes,'COST_NATIVE_FILE_REQUIRED');return JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(fs.readFileSync(p)));}
export async function costWork({turn,signal,controls,build,saveInput,readResult,saveReceipt,announce,within,validate=validateCost}){
 const active=()=>{within();check(!signal?.aborted,'COST_TURN_OR_BUDGET_LOST');controls.assertActive();};
 active();await turn.assertHeld('qt-cost1000-before-input');active();const built=build(turn.identity);saveInput(built);
 const ready=controls.wait('RESULT_READY');announce('COST_INPUT_READY');await ready;active();await turn.assertHeld('qt-cost1000-after-native');active();
 const receipt=validate(readResult(),built);active();saveReceipt(receipt);
 const release=controls.wait('RELEASE');announce('COST_CAPTURED_PENDING_RELEASE');await release;active();await turn.assertHeld('qt-cost1000-before-release');active();return receipt;
}