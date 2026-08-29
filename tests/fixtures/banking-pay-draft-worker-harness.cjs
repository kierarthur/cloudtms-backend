// Test-only extraction of unchanged Worker Draft authority. The only external
// request dependency is a strict in-memory PostgREST response; no network or
// operation runner is available to this VM.
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const crypto=require('node:crypto');
const source=fs.readFileSync(path.resolve(__dirname,'../../broker/src/index.js'),'utf8');
function between(text,start,end){const a=text.indexOf(start),b=text.indexOf(end,a+start.length);
  assert.ok(a>=0&&b>a,`Existing Draft source boundary: ${start}`);return text.slice(a,b);}
const owner=between(source,'async function handleBankingPayCreateDraft','async function handleTimesheetAdvancePayment');
const helpers=[between(owner,'  const trimStr =','  const jsonHeaders ='),
  between(owner,'  const cloneJson =','  const stableStringify ='),
  between(owner,'  const booleanFrom =','  const stringArrayFrom ='),
  between(owner,'  const numericOrNull =','  const isActiveDraftCreateStatus =')].join('\n');
const clone=value=>JSON.parse(JSON.stringify(value));
function install(snapshot){
  const requests=[];
  const context={env:{SUPABASE_URL:'https://draft-fixture.invalid'},encodeURIComponent,
    sbFetch:async(_env,address)=>{
      const url=new URL(address);assert.equal(url.origin,'https://draft-fixture.invalid');requests.push(url.pathname);
      if(url.pathname==='/rest/v1/banking_pay_workbench_sessions')return {rows:[clone(snapshot.session)]};
      assert.equal(url.pathname,'/rest/v1/banking_pay_workbench_preview_rows');
      assert.equal(url.searchParams.get('selected'),'eq.true');assert.equal(url.searchParams.get('status'),'eq.READY');
      assert.equal(url.searchParams.has('section'),false,'Physically blocked promoted deductions remain included');
      const offset=Number(url.searchParams.get('offset')),limit=Number(url.searchParams.get('limit'));
      return {rows:clone(snapshot.rows.slice(offset,offset+limit))};
    }};
  vm.runInNewContext(`${helpers}\nthis.read=fetchCurrentSessionSelectionForCreateDraft;`,context,{timeout:5000});
  return {read:scope=>context.read(snapshot.session.id,scope,{session_version:snapshot.session.version,
    selected_eligible_ready_row_count:snapshot.rows.length}),requests};
}
module.exports={install,clone,ownerHash:crypto.createHash('sha256').update(owner.replaceAll('\r\n','\n').trim()).digest('hex')};
