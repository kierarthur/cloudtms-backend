// Fixed observer input/envelope guard only: no file, transport, auth or provider access.
import {createHash} from 'node:crypto';
import {buildFromReceipt,validateFrame} from 'file:///D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026/docs/healthroster-build/proposals/daily-health-observer-seven-field-turn-forward-20261017/runtime/observer-s1.mjs';
const sha=x=>createHash('sha256').update(x).digest('hex'),must=(x,c)=>{if(!x)throw Error('S1_AUX_'+c)};
const same=(a,b)=>JSON.stringify(a)===JSON.stringify(b);
const keys=(v,k)=>v&&typeof v==='object'&&!Array.isArray(v)&&Object.keys(v).sort().join('|')===[...k].sort().join('|');
export function prepare(mode,{original,phase,identity}){
 must(['METADATA','EFFECTS'].includes(mode)&&['BEFORE','AFTER','RECONCILE'].includes(phase),'FIXED_MODE_PHASE_REQUIRED');
 return buildFromReceipt(mode,{original,stage:'S1',phase,identity});
}
export function readAuxEnvelope(result,built,request,{now=Date.now(),maxAgeMs=30000}={}){
 must(maxAgeMs===30000||maxAgeMs===180000,'ORIGINAL_CLOCK_BOUND_REQUIRED');
 const rebuilt=prepare(built?.receipt?.mode,request);must(same(rebuilt,built),'EXACT_GENERATED_INPUT_REQUIRED');
 const input=built.input,r=built.receipt;
 must(sha(input.sql)===r.sqlSha256&&sha(input.verification_sql)===r.verificationSqlSha256,'INPUT_HASH_REQUIRED');
 must(result?.database==='cloudtms_test_clone'&&result.databaseTarget==='agency_test'&&result.label===input.label&&result.sqlSha256===r.sqlSha256&&result.statementCount===2&&result.accepted===true&&result.rolledBack===true&&!result.fatalError,'NATIVE_ENVELOPE_REQUIRED');
 const statement=(s,n,cmd)=>keys(s,['statement','command','rowCount','fields','rows','rowsTruncated'])&&s.statement===n&&s.command===cmd&&s.rowsTruncated===false&&Array.isArray(s.rows)&&Array.isArray(s.fields);
 must(Array.isArray(result.statements)&&result.statements.length===2&&statement(result.statements[0],1,'SET')&&result.statements[0].rowCount===null&&result.statements[0].fields.length===0&&result.statements[0].rows.length===0,'STATEMENT_ORDER_REQUIRED');
 const select=(s,n)=>{must(statement(s,n,'SELECT')&&s.rowCount===1&&same(s.fields,['s1_observer'])&&s.rows.length===1&&keys(s.rows[0],['s1_observer']),'EXACT_ONE_ROW_REQUIRED');return validateFrame(s.rows[0].s1_observer,r,{now,maxAgeMs})};
 const a=select(result.statements[1],2),fresh=result.rollbackVerification;
 must(fresh?.database==='cloudtms_test_clone'&&fresh.connectedFresh===true&&Array.isArray(fresh.statements)&&fresh.statements.length===1,'FRESH_CONNECTION_REQUIRED');
 const b=select(fresh.statements[0],1);must(Date.parse(b.observedAt)>=Date.parse(a.observedAt),'FRESH_ORDER_REQUIRED');
 // Shared counter/sequence changes remain observations, never automatic fixture attribution.
 return {version:'S1_AUXILIARY_NATIVE_OBSERVATION_V1',mode:r.mode,phase:r.phase,inputSqlSha256:r.sqlSha256,verificationSqlSha256:r.verificationSqlSha256,main:a,fresh:b,mainFreshObservationEqual:same(a.observation,b.observation),effectQualified:false,restorationClaimed:false};
}
