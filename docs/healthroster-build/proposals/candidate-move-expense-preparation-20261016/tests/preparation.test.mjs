import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';
import {decodeCandidateContractMove} from '../../candidate-contract-move-native-20261016/forward/candidate-contract-move.mjs';
const read=p=>fs.readFileSync(new URL('../'+p,import.meta.url),'utf8').replace(/\r\n/g,'\n');
const src=read('forward/01_current_expense_preparation.mjsinc');
const id=n=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
class HttpError extends Error {constructor(status,code){super(code);this.status=status;}}
const wf={id:id(1),candidate_id:id(2),scope:'WEEKLY',workflow_kind:'CONTRACT_EXPENSE',generation:3,contract_id:id(3),contract_week_id:id(4),anchor_timesheet_id:id(5),target_timesheet_id:null,policy_snapshot_json:{policy_fingerprint:'ORIGINAL_A'}};
const target=n=>({identity_kind:'TIMESHEET',id:id(n),path:'/candidate-app/v1/timesheets/'+id(n)});
const move=()=>({version:'CANDIDATE_CONTRACT_MOVE_V1',operation_id:id(10),family_root_id:id(5),placement_revision:'2',original_timesheet_id:id(5),current_timesheet_id:id(6),original_contract_label:'OriginalA',current_contract_label:'CurrentB',moved_at_utc:'2026-10-16T12:00:00.000000Z',evidence_origin_timesheet_id:id(5),current_mutable_timesheet_id:id(6),inherited_evidence:[],original_detail_target:target(5),current_detail_target:target(6),unchanged_facts_sha256:'a'.repeat(64),current_state:'PENDING',is_history:false});
const detail=()=>({contract_move:move(),contract_week:{id:id(7),contract_id:id(8)},timesheet:{id:id(6)},capabilities:{policy:{policy_fingerprint:'GENUINE_CURRENT_EXPENSE_POLICY'}},workflows:[{workflow_id:id(1),generation:3}]});
const common=()=>({structuredClone,decodeCandidateContractMove,CandidateHttpError:HttpError,isObject:v=>v!=null&&typeof v==='object'&&!Array.isArray(v),text:v=>String(v??''),UUID_RE:/^[0-9a-f-]{36}$/,forbiddenFinancialKeys:x=>Object.hasOwn(x,'canonical_tsfin_snapshot')?['canonical_tsfin_snapshot']:[],candidateRpcArgs:(a,e,p)=>p,correctionProjection:async()=>null});
const compile=d=>Function(...Object.keys(d),src+';return {prepareImmutableSubmission,buildCandidateWeeklySubmissionThroughCanonicalAuthority};')(...Object.values(d));
async function prepare({workflow=wf,work=detail(),carrier=null}={}){
 const seen={rpc:[]};const d={...common(),rpcCall:async(_,name,args)=>{seen.rpc.push({name,args});return args.p_workflow_id?work:carrier;},buildOfficialPresentationSnapshot:async(_,w)=>({original_contract_id:w.contract_id})};
 const nativeBoundary={buildWeeklySubmission:async args=>{seen.calculation=structuredClone(args);return {boundary:'CALCULATOR_NOT_EXECUTED'};},buildDailySubmission:async args=>{seen.daily=args;return {};}};
 const before=structuredClone(workflow);const result=await compile(d).prepareImmutableSubmission({},nativeBoundary,workflow,{immutable_submission:{expense_claim:{travel_amount:12}}},'same-original-key',{});assert.deepEqual(workflow,before);return {seen,result};
}
test('targetless expense retains genuine targetless native draft, current B worked anchor and purpose policy; original presentation/identity untouched',async()=>{
 const {seen,result}=await prepare();const w=seen.calculation.workflow;
 assert.equal(w.target_timesheet_id,null);assert.equal(w.anchor_timesheet_id,id(6));assert.equal(w.contract_week_id,id(7));assert.equal(w.contract_id,id(8));
 assert.equal(w.policy_snapshot_json.policy_fingerprint,'GENUINE_CURRENT_EXPENSE_POLICY');assert.equal(w._contract_move_current,false);
 assert.deepEqual(w._contract_move_expense_anchor,{timesheet_id:id(6),contract_week_id:id(7)});
 assert.equal(result.official_presentation.original_contract_id,id(3));assert.equal(seen.calculation.mutationKey,'same-original-key');assert.equal(seen.rpc.length,1);
});
const carrier=()=>({contract_move:{...move(),original_timesheet_id:id(11),evidence_origin_timesheet_id:id(11),current_timesheet_id:id(12),current_mutable_timesheet_id:id(12),original_detail_target:target(11),current_detail_target:target(12)},contract_week:{id:id(13),contract_id:id(8)},timesheet:{id:id(12)},workflows:[{workflow_id:id(1),generation:3}]});
test('existing immutable expense target resolves through its own actual detail/member, never substitutes worked B as expense carrier',async()=>{
 const {seen}=await prepare({workflow:{...wf,target_timesheet_id:id(11)},carrier:carrier()});
 assert.equal(seen.calculation.workflow.target_timesheet_id,id(12));assert.equal(seen.calculation.workflow.contract_week_id,id(13));assert.equal(seen.calculation.workflow.anchor_timesheet_id,id(6));
 assert.equal(seen.rpc[1].args.p_timesheet_id,id(11));assert.equal(seen.rpc[1].args.p_workflow_id,null);
});
test('carrier family/accepted Move/Contract/workflow generation/current target substitutions refuse before calculation',async()=>{
 for(const change of [c=>c.contract_move.family_root_id=id(90),c=>c.contract_move.operation_id=id(90),c=>c.contract_week.contract_id=id(90),c=>c.workflows[0].generation=4,c=>c.timesheet.id=id(90)]){
  const c=carrier();change(c);await assert.rejects(prepare({workflow:{...wf,target_timesheet_id:id(11)},carrier:c}));
 }
});
test('ordinary expense/Daily and moved hours preserve prior actual preparation identities',async()=>{
 const ordinary=await prepare({work:{}});assert.deepEqual(ordinary.seen.calculation.workflow,wf);
 const hours=await prepare({workflow:{...wf,workflow_kind:'CONTRACT_HOURS',target_timesheet_id:id(5)}});assert.equal(hours.seen.calculation.workflow.target_timesheet_id,id(6));assert.equal(hours.seen.calculation.workflow._contract_move_expense_anchor,undefined);
 const daily=await prepare({workflow:{...wf,workflow_kind:'DAILY',scope:'DAILY'}});assert.equal(daily.seen.daily.workflow.contract_id,id(3));assert.equal(daily.seen.rpc.length,0);
});
async function builder({existing=false,change=()=>{}}={}){
 const seen={calls:[],calculation:[]};const rows={contract:{id:id(8),candidate_id:id(2),client_id:id(20)},week:{id:existing?id(13):id(7),contract_id:id(8),timesheet_id:existing?id(12):id(6),week_ending_date:'2026-10-18',additional_seq:existing?1:0},anchorWeek:{id:id(7),contract_id:id(8),timesheet_id:id(6),week_ending_date:'2026-10-18'},anchor:{timesheet_id:id(6),contract_id:id(8),sheet_scope:'WEEKLY',line_type:'HOURS',is_current:true,archived_at_utc:null},carrier:{timesheet_id:id(12),contract_id:id(8),sheet_scope:'WEEKLY',line_type:'EXPENSES',is_current:true,version:2}};change(rows);
 const workflow={...wf,contract_id:id(8),contract_week_id:rows.week.id,target_timesheet_id:existing?id(12):null,anchor_timesheet_id:id(6),policy_snapshot_json:{policy_fingerprint:'CURRENT_PURPOSE'},_contract_move_current:existing,_contract_move_expense_anchor:{timesheet_id:id(6),contract_week_id:id(7)}};
 const deps={...common(),normaliseCandidateBreakInput:v=>v,deterministicCandidateUuid:async()=>id(99),sbGetOne:async(_,url)=>{seen.calls.push(url);const u=new URL(url),table=u.pathname.split('/').at(-1);if(table==='contract_weeks')return u.searchParams.get('id')==='eq.'+rows.week.id?rows.week:rows.anchorWeek;if(table==='contracts')return rows.contract;if(table==='timesheets')return u.searchParams.get('timesheet_id')==='eq.'+id(6)?rows.anchor:rows.carrier;if(table==='timesheets_financials')return {timesheet_id:id(12)};if(table==='candidates')return {id:id(2)};if(table==='clients')return {id:id(20)};throw Error('UNEXPECTED_TABLE');},buildWeeklyScheduleSegmentsSnapshot:async(...args)=>{seen.calculation.push(args);return {snapshot:{boundary:'NATIVE_CANONICAL_NOT_EXECUTED'}};}};
 const result=await compile(deps).buildCandidateWeeklySubmissionThroughCanonicalAuthority({SUPABASE_URL:'https://unit.invalid'},{workflow,factualSubmission:{expense_claim:{travel_amount:12}},mutationKey:'original-key'});return {seen,result};
}
test('targetless expense delegates same real canonical producer with prospective create, current B Contract and real worked anchor validated; no write',async()=>{
 const {seen,result}=await builder();assert.equal(seen.calculation.length,1);assert.equal(seen.calculation[0][3].id,id(8));assert.equal(seen.calculation[0][5].write_now,false);
 assert.equal(result.timesheet_create_json.line_type,'EXPENSES');assert.equal(result.timesheet_create_json.contract_id,id(8));assert.equal(result.canonical_tsfin_snapshot.boundary,'NATIVE_CANONICAL_NOT_EXECUTED');
 assert(seen.calls.every(v=>v.startsWith('https://unit.invalid/rest/v1/')));
});
test('existing carrier uses actual Expense B and separate current worked anchor',async()=>{const {seen,result}=await builder({existing:true});assert.equal(seen.calculation[0][1].timesheet_id,id(12));assert.equal(result.timesheet_create_json,null);assert.equal(seen.calculation[0][2].id,id(13));});
test('missing/revoked/foreign/different-week/nonworked anchor and wrong-role/nonadditional carrier refuse before canonical calculation',async()=>{
 for(const change of [r=>r.anchor=null,r=>r.anchor.is_current=false,r=>r.anchor.contract_id=id(90),r=>r.anchorWeek.week_ending_date='2026-10-11',r=>r.anchor.line_type='EXPENSES',r=>r.carrier.line_type='HOURS',r=>r.week.additional_seq=0])await assert.rejects(builder({existing:true,change}),/CANDIDATE_CONTEXT_STALE/);
});
test('full SQL successor changes only exact native WORKER_DRAFT purpose-policy eligibility; old pending/history and guards retained',()=>{
 const before=read('../candidate-native-pending-20261016/forward/11_candidate_detail.sqlinc'),after=read('forward/00_expense_draft_policy.sqlinc');
 assert.equal(after.replace("'DRAFT','WORKER_DRAFT','WORKER_SUBMITTED'","'DRAFT','WORKER_SUBMITTED'"),before);
 assert(after.includes('private._expense_approval_policy_v1(v_contract.client_id,v_contract.id,'));
 const wf=read('../candidate-native-pending-20261016/forward/10_native_workflow_admission.sqlinc');assert(wf.includes('CANDIDATE_MOVE_EXPENSE_NATIVE_SUBMISSION_NOT_QUALIFIED'));
});
