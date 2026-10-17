// Strict closed frames; no raw rows, digests or plans are returned by a successful diagnostic.
import{check,exact,buildCostInput,MODES,LIMIT}from'./factory.mjs';
const equal=(a,b)=>JSON.stringify(a)===JSON.stringify(b),pid=x=>Number.isSafeInteger(x)&&x>0,num=s=>{check(typeof s==='string'&&/^(0|[1-9][0-9]*)$/.test(s),'COST_CANONICAL_COUNTER_REQUIRED');return BigInt(s);};
const abi=JSON.parse((await import('node:fs')).default.readFileSync(new URL('./reference/NATIVE_ABI_REFERENCE.json',import.meta.url),'utf8'));
function statement(s,n,command){return exact(s,['statement','command','rowCount','fields','rows','rowsTruncated'])&&s.statement===n&&s.command===command&&s.rowsTruncated===false&&Array.isArray(s.fields)&&Array.isArray(s.rows);}
function one(s,n,field){check(statement(s,n,'SELECT')&&s.rowCount===1&&equal(s.fields,[field])&&s.rows.length===1&&exact(s.rows[0],[field]),'COST_EXACT_ONE_ROW_REQUIRED');return s.rows[0][field];}
export function validateCost(result,built,{now=Date.now()}={}){
 const r=built?.receipt;check(r&&MODES.includes(r.mode)&&equal(built,buildCostInput(r.identity,r.mode)),'COST_EXACT_DERIVED_INPUT_REQUIRED');
 check(Number.isFinite(now),'COST_VALID_NOW_REQUIRED');
 check(exact(result,abi.topKeys)&&equal(result.warnings,abi.warnings)&&result.fatalError===null&&result.database==='cloudtms_test_clone'&&result.databaseTarget==='agency_test'&&result.label===r.label&&result.sqlSha256===r.sqlSha256&&result.statementCount===4&&result.accepted===true&&result.rolledBack===true&&!result.fatalError,'COST_NATIVE_ROLLBACK_REQUIRED');
 check(Array.isArray(result.statements)&&result.statements.length===4&&statement(result.statements[0],1,'SET')&&result.statements[0].rowCount===null&&result.statements[0].fields.length===0&&result.statements[0].rows.length===0,'COST_EXACT_STATEMENT_SEQUENCE_REQUIRED');
 const start=one(result.statements[1],2,'cost_clock'),counts=one(result.statements[2],3,'cost_counters'),end=one(result.statements[3],4,'cost_clock'),fresh=result.rollbackVerification;
 check(exact(fresh,abi.freshKeys)&&fresh.connectedFresh===true&&fresh.database==='cloudtms_test_clone'&&Array.isArray(fresh.statements)&&fresh.statements.length===1,'COST_FRESH_CONNECTION_REQUIRED');const after=one(fresh.statements[0],1,'cost_clock');
 const clockKeys=['version','mode','phase','sampleLimit','database','databaseOid','backendPid','holderPid','holderGranted','readOnly','noRoleSwitch','observedAt','closureAdmitted'];
 const times=[];
 for(const [x,phase]of[[start,'START'],[end,'END'],[after,'FRESH']]){
  check(exact(x,clockKeys)&&x.version==='QT_AUDIT_COST_CLOCK_V1'&&x.mode===r.mode&&x.phase===phase&&x.sampleLimit===1000&&x.database==='cloudtms_test_clone'&&x.databaseOid===r.identity.databaseOid&&pid(x.backendPid)&&x.backendPid!==r.identity.backendPid&&x.holderPid===r.identity.backendPid&&x.holderGranted===true&&x.readOnly===true&&x.noRoleSwitch===true&&x.closureAdmitted===true,'COST_CLOCK_IDENTITY_OR_CLOSURE_REQUIRED');
  const t=Date.parse(x.observedAt);check(Number.isFinite(t)&&t<=now+1000&&now-t<=30000,'COST_NATIVE_FRESH_CLOCK_REQUIRED');times.push(t);
 }
 check(start.backendPid===end.backendPid&&times[0]<=times[1]&&times[1]<=times[2]&&times[1]-times[0]<=30000,'COST_NATIVE_CLOCK_ORDER_REQUIRED');
 check(exact(counts,['version','mode','sampleLimit','sampleCount','evaluatedCount','bytesTotal','database','backendPid','holderPid','holderGranted','readOnly','noRoleSwitch'])&&counts.version==='QT_AUDIT_COST_CONSUMED_V1'&&counts.mode===r.mode&&counts.sampleLimit===1000&&counts.database==='cloudtms_test_clone'&&counts.backendPid===start.backendPid&&counts.holderPid===r.identity.backendPid&&counts.holderGranted===true&&counts.readOnly===true&&counts.noRoleSwitch===true,'COST_CLOSED_CONSUMED_COUNTERS_REQUIRED');
 const n=num(counts.sampleCount),evaluated=num(counts.evaluatedCount),bytes=num(counts.bytesTotal);check(n<=BigInt(LIMIT)&&n===evaluated&&bytes<=n*2147483647n&&(n===0n?bytes===0n:bytes>0n),'COST_CONSUMPTION_BOUND_REQUIRED');
 if(['JSON_MD5','RECORD_SHA256'].includes(r.mode))check(bytes===32n*n,'COST_DIGEST_BYTES_REQUIRED');
 return{version:'QT_AUDIT_COST_CAPTURE_ONLY_V1',mode:r.mode,sampleLimit:1000,sampleCount:counts.sampleCount,evaluatedCount:counts.evaluatedCount,bytesTotal:counts.bytesTotal,nativeStartedAt:start.observedAt,nativeEndedAt:end.observedAt,nativeObservedSpanMs:times[1]-times[0],freshObservedAt:after.observedAt,nativeRolledBack:true,connectedFresh:true,holderGranted:true,readOnly:true,scope:'BOUNDED_NONREPRESENTATIVE_COST_OBSERVATION_ONLY',cause:'NOT_PROVED',wholeTablePerformance:'NOT_PROVED',assertionEquivalence:'NOT_PROVED',qtAcceptance:'NOT_RUN',businessFixtures:0};
}
