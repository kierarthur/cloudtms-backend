// Fixed catalogue-only capture protocol. No native/client/browser/auth/provider transport.
import fs from 'node:fs';import path from 'node:path';import {sha,check,exact} from '../daily-health-genuine-staged-operator-20261017/contract.mjs';
import {heaps} from '../daily-health-genuine-staged-preflight-batch-forward-20261017/s0-scope.mjs';
export const BUDGET=Object.freeze({work:120000,cleanup:60000,whole:180000,resultBytes:8_000_000});
export class Controls{
 constructor(input,signal){this.input=input;this.signal=signal;this.buffer='';this.pending=null;this.failure=null;this.onData=b=>{this.buffer+=b.toString();if(this.buffer.length>128)return this.fail('S0_CONTROL_BOUND_HELD');let i;while((i=this.buffer.indexOf('\n'))>=0){const line=this.buffer.slice(0,i).replace(/\r$/,'');this.buffer=this.buffer.slice(i+1);if(line==='ABORT')this.fail('S0_ROOT_ABORT');else if(!this.pending||line!==this.pending.expected)this.fail('S0_CONTROL_ORDER_HELD');else{const p=this.pending;this.pending=null;p.resolve();}}};this.onEnd=()=>this.fail('S0_INPUT_CLOSED');this.onAbort=()=>this.fail('S0_TURN_OR_BUDGET_LOST');input.on('data',this.onData);input.once('end',this.onEnd);signal.addEventListener('abort',this.onAbort,{once:true});if(signal.aborted)this.onAbort();}
 fail(code){this.failure??=Error(code);this.pending?.reject(this.failure);this.pending=null;}
 assertActive(){if(this.failure)throw this.failure;}
 wait(expected){if(this.failure)return Promise.reject(this.failure);if(this.pending)throw Error('S0_CONTROL_ALREADY_WAITING');return new Promise((resolve,reject)=>{this.pending={expected,resolve,reject};});}
 dispose(){this.input.removeListener('data',this.onData);this.input.removeListener('end',this.onEnd);this.signal.removeEventListener('abort',this.onAbort);}
}
const stable=v=>Array.isArray(v)?v.map(stable):v&&typeof v==='object'?Object.fromEntries(Object.keys(v).sort().map(k=>[k,stable(v[k])])):v;
export function captureEnvelope(result,receipt,{now=Date.now()}={}){
 check(result?.database==='cloudtms_test_clone'&&result.databaseTarget==='agency_test'&&result.label===receipt.label&&result.sqlSha256===receipt.sqlSha256
  &&result.statementCount===2&&result.accepted===true&&result.rolledBack===true&&!result.fatalError,'S0_NATIVE_CAPTURE_REFUSED');
 const statement=(s,n,kind)=>s&&exact(s,['statement','command','rowCount','fields','rows','rowsTruncated'])&&s.statement===n&&s.command===kind&&s.rowsTruncated===false&&Array.isArray(s.fields)&&Array.isArray(s.rows);
 check(Array.isArray(result.statements)&&result.statements.length===2&&statement(result.statements[0],1,'SET')&&result.statements[0].rowCount===null&&result.statements[0].fields.length===0&&result.statements[0].rows.length===0,'S0_NATIVE_SEQUENCE_REFUSED');
 const read=(s,n)=>{check(statement(s,n,'SELECT')&&s.rowCount===1&&s.fields.length===1&&s.fields[0]==='s0_catalogue'&&s.rows.length===1&&exact(s.rows[0],['s0_catalogue']),'S0_NATIVE_ONE_ROW_REQUIRED');return s.rows[0].s0_catalogue;};
 const main=read(result.statements[1],2),fresh=result.rollbackVerification;
 check(fresh?.connectedFresh===true&&fresh.database==='cloudtms_test_clone'&&Array.isArray(fresh.statements)&&fresh.statements.length===1,'S0_NATIVE_FRESH_REQUIRED');const after=read(fresh.statements[0],1);
 const fields=['version','database','databaseOid','readOnly','noRoleSwitch','currentSchemas','observedAt','holderGranted','relations','types','typeWalkComplete','indexes','collationBindings','operatorClasses','supportBindings'];
 for(const v of [main,after]){check(exact(v,fields)&&v.version==='DAILY_GENUINE_S0_CATALOGUE_ONLY_V1'&&v.database==='cloudtms_test_clone'&&v.databaseOid===receipt.identity.databaseOid&&v.readOnly===true&&v.noRoleSwitch===true&&v.holderGranted===true,'S0_NATIVE_IDENTITY_REFUSED');check(Array.isArray(v.relations)&&v.relations.length===15&&Array.isArray(v.types)&&Array.isArray(v.indexes)&&Array.isArray(v.currentSchemas)&&Array.isArray(v.operatorClasses)&&Array.isArray(v.supportBindings),'S0_NATIVE_METADATA_SHAPE_REQUIRED');check(JSON.stringify(v.relations.map(r=>r.schema+'.'+r.name).sort())===JSON.stringify(heaps.map(r=>r.schema+'.'+r.name).sort()),'S0_NATIVE_RELATION_SCOPE_REQUIRED');const t=Date.parse(v.observedAt);check(Number.isFinite(t)&&t<=now+1000&&now-t<=30000,'S0_NATIVE_FRESH_CLOCK_REQUIRED');}
 check(Date.parse(after.observedAt)>=Date.parse(main.observedAt),'S0_NATIVE_FRESH_ORDER_REQUIRED');
 const withoutClock=v=>{const {observedAt,...rest}=v;return stable(rest);};check(JSON.stringify(withoutClock(main))===JSON.stringify(withoutClock(after)),'S0_NATIVE_METADATA_CHANGED');
 return {version:'DAILY_S0_CATALOGUE_CAPTURE_ONLY_V1',sqlSha256:receipt.sqlSha256,verificationSqlSha256:receipt.verificationSqlSha256,
  mainMetadataSha256:sha(JSON.stringify(withoutClock(main))),freshMetadataSha256:sha(JSON.stringify(withoutClock(after))),relations:15,
  transportCaptured:true,compatibility:'ROOT_REVIEW_REQUIRED_NOT_QUALIFIED',businessScope:'NOT_RUN',canonicalRestoration:'NOT_APPLICABLE_NO_VERDICT_COPIED'};
}
export function save(out,name,value){check(['INPUT.json','INPUT_RECEIPT.json','CAPTURE_RECEIPT.json','SUMMARY.json','HOLD.json'].includes(name),'S0_FIXED_FILE_REQUIRED');const b=Buffer.from(JSON.stringify(value,null,2)+'\n');check(b.length<=BUDGET.resultBytes,'S0_FILE_BOUND_REQUIRED');fs.writeFileSync(path.join(out,name),b,{flag:'wx'});}
export function nativeResult(out){const file=path.join(out,'NATIVE_RESULT.json'),s=fs.lstatSync(file);check(s.isFile()&&!s.isSymbolicLink()&&s.size>0&&s.size<=BUDGET.resultBytes,'S0_NATIVE_FILE_REQUIRED');return JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(fs.readFileSync(file)));}
export async function catalogueWork({turn,signal,controls,build,saveInput,readResult,saveReceipt,announce,within}){
 const active=()=>{within();check(!signal?.aborted,'S0_TURN_OR_BUDGET_LOST');controls.assertActive();};
 active();await turn.assertHeld('s0-catalogue-before-input');active();const built=build(turn.identity);saveInput(built);
 const ready=controls.wait('RESULT_READY');announce('INPUT_READY');await ready;active();await turn.assertHeld('s0-catalogue-after-native-result');active();
 const receipt=captureEnvelope(readResult(),built.receipt);active();saveReceipt(receipt);const release=controls.wait('RELEASE');announce('CAPTURED_PENDING_ROOT_REVIEW_AND_RELEASE');await release;active();await turn.assertHeld('s0-catalogue-before-release');active();return receipt;
}
