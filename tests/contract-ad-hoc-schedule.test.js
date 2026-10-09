import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {runInNewContext} from 'node:vm';
const source=await readFile(new URL('../broker/src/index.js',import.meta.url),'utf8');
const start=source.indexOf('async function generateContractWeeksInternal');
const end=source.indexOf('async function detectScheduleClashesInternal',start);
const generator=source.slice(start,end);
async function generate(adHoc,existing=[]) {
  const writes=[], templates=[];
  const fn=runInNewContext(`${generator}; generateContractWeeksInternal`,{
    sbGetOne:async(env,url)=>{assert.match(url,/is_ad_hoc/);return {id:'contract',client_id:'client',start_date:'2026-10-12',end_date:'2026-10-25',is_ad_hoc:adHoc,std_schedule_json:{mon:{start:'09:00',end:'17:00'}}};},
    computeWeekEndingInclusive:()=> '2026-10-25',enumerateWeekEndings:()=>['2026-10-18','2026-10-25'],toYmd:()=> '2026-10-09',
    sbFetch:async()=>({rows:existing}),buildPlannedScheduleFromTemplate:(template,we)=>{templates.push(template);return template?[{date:we,start:'09:00',end:'17:00'}]:[];},
    clampPlannedToWindow:raw=>raw,nowIso:()=> '2026-10-09T15:00:00Z',sbHeaders:()=>({}),
    fetch:async(url,opts)=>{assert.equal(opts.method,'POST');assert.match(url,/\/contract_weeks$/);const rows=JSON.parse(opts.body);writes.push(...rows);return {ok:true,json:async()=>rows};}
  });
  const result=await fn({SUPABASE_URL:'https://test.invalid'},'contract');return {result,writes,templates};
}
test('new ad hoc weeks have no guaranteed pattern, even for a legacy stale template',async()=>{
  const result=await generate(true);
  assert.equal(result.result.generated,2);
  assert.ok(result.templates.every(t=>t===null));
  assert.ok(result.writes.every(row=>row.planned_schedule_json===null&&row.timesheet_id===null&&row.status==='PLANNED'));
});
test('existing weeks and their worked/protected hours are not updated; replay inserts nothing',async()=>{
  const existing=[{week_ending_date:'2026-10-18',worked_hours:8,protected:true}];
  const result=await generate(true,existing);assert.equal(result.writes.length,1);assert.equal(result.writes[0].week_ending_date,'2026-10-25');assert.equal(existing[0].worked_hours,8);
  assert.equal((await generate(true,[...existing,{week_ending_date:'2026-10-25'}])).writes.length,0);
});
test('fixed-pattern generation retains its existing planned schedule',async()=>{
  assert.ok((await generate(false)).writes.every(row=>row.planned_schedule_json.length===1));
});
test('create ignores stale ad hoc templates and generates blank weeks; duplicate also admits blank ad hoc generation',()=>{
  const create=source.slice(source.indexOf('async function handleContractsCreate'),source.indexOf('async function handleContractsCreate')+22000);
  assert.match(create,/if \(!is_ad_hoc && body.std_schedule_json\)/);
  assert.match(create,/const std_hours_json = is_ad_hoc \? null/);
  assert.match(create,/row.is_ad_hoc === true \|\| !!row.std_schedule_json/);
  const publicGenerator=source.slice(source.indexOf('async function handleContractsGenerateWeeks'),source.indexOf('async function handleContractsGenerateWeeks')+11000);
  assert.match(publicGenerator,/'is_ad_hoc'/);
  assert.match(publicGenerator,/c.is_ad_hoc === true \? null : \(c.std_schedule_json \|\| null\)/);
});
