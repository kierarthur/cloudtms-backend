import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {test} from 'node:test';
import assert from 'node:assert/strict';
const here=path.dirname(fileURLToPath(import.meta.url));
const source=fs.readFileSync(path.join(here,'../forward/08_factual_snapshot.mjsinc'),'utf8');
class CandidateHttpError extends Error {constructor(status,code){super(code);this.status=status;this.code=code;}}
const validate=Function('CandidateHttpError',source+';return validateCandidateWorkflowFactualSnapshot;')(CandidateHttpError);
const normalise=Function('CandidateHttpError','validateCandidateWorkflowFactualSnapshot','isObject','upper','text','UUID_RE','normalizeCandidateDailyBookedSource',
 fs.readFileSync(path.join(here,'../forward/09_create_payload.mjsinc'),'utf8')+';return normaliseCandidateWorkflowCreatePayload;')(
 CandidateHttpError,validate,v=>v&&typeof v==='object'&&!Array.isArray(v),v=>String(v||'').toUpperCase(),v=>String(v||''),/^[0-9a-f-]{36}$/,v=>v);
const clone=v=>structuredClone(v);
const sample=()=>{const h={actual_schedule_json:[{row_key:'native-ui-row',date:'2026-10-12',work_date:'2026-10-12',start_time:'19:00',end_time:'07:00',ref_num:null,break_entry:{kind:'NO_BREAK',no_break:true,break_minutes:0}}],additional_units_week:{NIGHT:0},additional_units_per_day:{'2026-10-12':{NIGHT:0}}};return {...h,timesheet_patch_json:clone(h)};};
const invalid=v=>assert.throws(()=>validate(v),e=>e.status===400&&e.code==='CANDIDATE_WORKFLOW_PAYLOAD_INVALID');
test('exact factual body is returned without normalization or lost zero/NULL facts',()=>{const v=sample();assert.equal(validate(v),v);assert.equal(v.additional_units_week.NIGHT,0);assert.equal(v.actual_schedule_json[0].ref_num,null);});
test('hours patch comparison ignores object key order and preserves values',()=>{const v=sample();v.timesheet_patch_json={additional_units_per_day:v.additional_units_per_day,additional_units_week:v.additional_units_week,actual_schedule_json:v.actual_schedule_json};assert.equal(validate(v),v);});
test('expense-only facts use existing declared amounts, without rates',()=>{const v={expense_claim:{mileage_units:0,mileage_total_confirmed:false,travel_amount:12.5,accommodation_amount:0,other_amount:0,description:''}};assert.equal(validate(v),v);});
test('all three actual break variants are closed and overnight minutes agree',()=>{for(const e of [{kind:'NO_BREAK',no_break:true,break_minutes:0},{kind:'DURATION_MINUTES',break_minutes:30},{kind:'START_END_TIMES',break_start:'23:45',break_end:'00:15',calculated_break_minutes:30}]){const v=sample();v.actual_schedule_json[0].break_entry=e;v.timesheet_patch_json=clone({actual_schedule_json:v.actual_schedule_json,additional_units_week:v.additional_units_week,additional_units_per_day:v.additional_units_per_day});validate(v);}});
for(const [name,edit] of [
 ['missing unit map',v=>delete v.additional_units_week],['missing patch',v=>delete v.timesheet_patch_json],
 ['divergent patch hours',v=>v.timesheet_patch_json.actual_schedule_json[0].start_time='20:00'],
 ['divergent patch units',v=>v.timesheet_patch_json.additional_units_week.NIGHT=1],
 ['date rollover',v=>v.actual_schedule_json[0].date='2026-02-29'],['date zero year',v=>v.actual_schedule_json[0].date='0000-01-01'],
 ['clock 24',v=>v.actual_schedule_json[0].end_time='24:00'],['row identity too long',v=>v.actual_schedule_json[0].row_key='x'.repeat(201)],
 ['numeric reference',v=>v.actual_schedule_json[0].ref_num=7],['unit negative',v=>v.additional_units_week.NIGHT=-1],
 ['unit numeric string',v=>v.additional_units_week.NIGHT='0'],['unit infinity',v=>v.additional_units_week.NIGHT=Infinity],
 ['unit NaN',v=>v.additional_units_week.NIGHT=NaN],['unit excess',v=>v.additional_units_week.NIGHT=1000001],
 ['invalid unit code',v=>v.additional_units_week.pay_rate=10],['extra units day',v=>{for(let i=1;i<=8;i++)v.additional_units_per_day['2026-10-'+String(i).padStart(2,'0')]={};}],
 ['too many segments',v=>v.actual_schedule_json=Array.from({length:113},()=>v.actual_schedule_json[0])],
 ['extra source day-off field',v=>v.day_off_dates=['2026-10-12']],['financial total',v=>v.pay_total=10],
 ['canonical snapshot',v=>v.canonical_tsfin_snapshot={pay_total:10}],['policy snapshot',v=>v.policy_snapshot_json={}],
 ['nested segment rate',v=>v.actual_schedule_json[0].rate=0],['nested patch authority',v=>v.timesheet_patch_json.contract_id='not-owner'],
 ['duration zero',v=>v.actual_schedule_json[0].break_entry={kind:'DURATION_MINUTES',break_minutes:0}],
 ['duration fractional',v=>v.actual_schedule_json[0].break_entry={kind:'DURATION_MINUTES',break_minutes:1.5}],
 ['clock duration mismatch',v=>v.actual_schedule_json[0].break_entry={kind:'START_END_TIMES',break_start:'23:45',break_end:'00:15',calculated_break_minutes:29}],
 ['equal break clocks',v=>v.actual_schedule_json[0].break_entry={kind:'START_END_TIMES',break_start:'12:00',break_end:'12:00',calculated_break_minutes:1440}],
 ['no-break mixed fields',v=>v.actual_schedule_json[0].break_entry={kind:'NO_BREAK',no_break:true,break_minutes:0,break_start:'12:00'}],
 ['break context NULL mode',v=>v.break_entry_context={context_version:'CANDIDATE_BREAK_ENTRY_V1',context_token:'a'.repeat(64),mode:null}],
 ['break context extra',v=>v.break_entry_context={context_version:'CANDIDATE_BREAK_ENTRY_V1',context_token:'a'.repeat(64),mode:'DURATION_MINUTES',rate:0}],
 ['expense rate',v=>v.expense_claim={travel_amount:0,pay_rate:1}],['expense negative',v=>v.expense_claim={travel_amount:-1}],
 ['expense description limit',v=>v.expense_claim={description:'x'.repeat(2001)}]
])test('rejects '+name,()=>{const v=sample();edit(v);invalid(v);});
test('object and dependency closure reject primitive, NULL, empty and detached break facts',()=>{for(const v of [null,[],0,'',{}, {additional_units_week:{}},{break_entry_context:{}},{expense_claim:null}])invalid(v);});
test('1001-code unit map is rejected',()=>{const v=sample();v.additional_units_week=Object.fromEntries(Array.from({length:1001},(_,i)=>['U'+i,0]));invalid(v);});
test('new field survives real payload normalizer for all three Weekly kinds',()=>{for(const workflow_kind of ['CONTRACT_HOURS','CONTRACT_COMBINED','CONTRACT_EXPENSE']){const input_snapshot=workflow_kind==='CONTRACT_EXPENSE'?{expense_claim:{other_amount:0}}:sample();assert.equal(normalise({workflow_kind,scope:'WEEKLY',route:'ELECTRONIC',input_snapshot}).input_snapshot,input_snapshot);}});
test('Daily new-field injection is rejected while original snapshot-free Daily mapping is unchanged',()=>{const v={workflow_kind:'DAILY',scope:'DAILY',route:'PHONE',timesheet_id:'10000000-0000-4000-8000-000000000001'};assert.equal(normalise(v).target_timesheet_id,v.timesheet_id);assert.throws(()=>normalise({...v,input_snapshot:sample()}),e=>e.code==='CANDIDATE_WORKFLOW_PAYLOAD_INVALID');});
test('old snapshot-free Weekly calls retain exact keys and value',()=>{const v={workflow_kind:'CONTRACT_HOURS',scope:'WEEKLY',route:'ELECTRONIC',contract_id:'10000000-0000-4000-8000-000000000001'};assert.deepEqual(normalise(v),v);});
test('internal native expense category metadata is not a public factual CREATE snapshot',()=>invalid({expense_category_resubmission:{source_workflow_id:'x'}}));
