// Fixed public-input/result IO, only from an actual committed root source image.
// Never opens a private state/journal/binding; never calls a provider.
import fs from'node:fs';import path from'node:path';import{fileURLToPath}from'node:url';import{ROOT,source}from'./source.mjs';import{check,buildCostInput,MODES,exact}from'./factory.mjs';import{validateCost}from'./validator.mjs';import{BUDGET}from'./protocol.mjs';
const equal=(a,b)=>JSON.stringify(a)===JSON.stringify(b);
function directory(out){check(typeof out==='string'&&out.startsWith(ROOT+'/.codex-tmp/')&&/^qt-audit-cost1000-20261017-[a-f0-9]{32}$/.test(path.posix.basename(out))&&path.posix.dirname(out)===ROOT+'/.codex-tmp'&&fs.realpathSync.native(out).replaceAll('\\','/')===out,'COST_FIXED_OWNED_PATH_REQUIRED');}
function read(out,name,limit){const p=out+'/'+name,s=fs.lstatSync(p);check(s.isFile()&&!s.isSymbolicLink()&&s.size>0&&s.size<=limit,'COST_PUBLIC_REGULAR_BOUND_REQUIRED');return new TextDecoder('utf-8',{fatal:true}).decode(fs.readFileSync(p));}
export function operation(args){
 const[op,out,mode,commit,encoded]=args;check(['input','result','summary'].includes(op)&&MODES.includes(mode)&&/^[a-f0-9]{40}$/.test(commit??'')&&args.length===(op==='result'?5:4),'COST_FIXED_PUBLIC_OPERATION_REQUIRED');process.env.HR_QT_COST_SOURCE_COMMIT=commit;source();directory(out);
 const receipt=JSON.parse(read(out,'INPUT_RECEIPT.json',BUDGET.inputBytes)),input=JSON.parse(read(out,'INPUT.json',BUDGET.inputBytes)),built=buildCostInput(receipt.identity,mode);check(equal({input,receipt},built),'COST_ORIGINAL_EXACT_INPUT_REQUIRED');
 if(op==='input')return built;
 if(op==='summary'){const s=JSON.parse(read(out,'SUMMARY.json',BUDGET.resultBytes));check(s.status==='COST_CAPTURED_OWN_TURN_RELEASED'&&s.originalSharedTurnCompleted===true,'COST_ORIGINAL_RELEASE_SUMMARY_REQUIRED');return{status:s.status,originalSharedTurnCompleted:true};}
 check(typeof encoded==='string'&&/^[A-Za-z0-9+/]+={0,2}$/.test(encoded),'COST_ENCODED_CLOSED_RESULT_REQUIRED');const b=Buffer.from(encoded,'base64');check(b.length>0&&b.length<=BUDGET.resultBytes&&b.toString('base64')===encoded,'COST_RESULT_BYTE_BOUND_REQUIRED');const native=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(b));const capture=validateCost(native,built);fs.writeFileSync(out+'/NATIVE_RESULT.json',b,{flag:'wx'});return{status:'COST_VALIDATED_RESULT_SAVED',capture};
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url))try{console.log(JSON.stringify(operation(process.argv.slice(2))));}catch(e){console.log(JSON.stringify({status:'HOLD',code:/^COST_[A-Z0-9_]+$/.test(e?.message??'')?e.message:'COST_PUBLIC_IO_HELD'}));process.exitCode=1;}
