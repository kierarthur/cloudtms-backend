import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';import test from 'node:test';import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),read=n=>fs.readFileSync(path.join(dir,n),'utf8'),r=JSON.parse(read('ISSUED_CREDIT_FULL_REVERSE.json'));
const forward=read(r.output),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
test('complete unique successor reverse preserves the original45 credit counterexample',()=>{let s=forward;assert.equal(sha(s),r.forward_sha256);for(const h of [...r.hunks].reverse()){assert.equal(s.split(h.after).length,2);s=s.replace(h.after,()=>h.before);}assert.equal(sha(s),r.input_sha256);assert.ok(s.includes('l.timesheet_id=r.reversal_timesheet_id'));});
test('actual T0 clone identity and R0 metadata replace the invalid R0 line search',()=>{
 assert.ok(forward.includes("line.timesheet_id=r.reversed_timesheet_id and line.meta_json->>'reversal_timesheet_id'=r.reversal_timesheet_id::text"));
 assert.ok(!/\bl\.timesheet_id=r\.reversal_timesheet_id/.test(forward));assert.ok(!forward.includes("'validation_purpose','CREDIT_SOURCE'"));
});
test('native exact-clone resume owner supplies existing money and member authority',()=>{
 assert.ok(forward.includes('private._invoice_credit_resume_v1(r.original_invoice_id,'));assert.ok(forward.includes('native_credit.credit_note_id=cn.id'));
 assert.ok(!/\b(sum\(|round\(|total_charge_ex_vat)/i.test(forward));
});
test('actual issued frozen Credit Note and current real R0 invoice lock are required',()=>{
 for(const x of ["cn.status::text in ('ISSUED','PAID')",'cn.original_invoice_id=r.original_invoice_id',
  "'{credit_scope,reversal_timesheet_id}'", "'{credit_scope,reversed_timesheet_id}'",'financial.locked_by_invoice_id=cn.id',
  'financial.timesheet_version=reversal.version',"doc.purpose='FINAL_ISSUE'",'doc.verified_at_utc is not null'])assert.ok(forward.includes(x),x);
});
test('before-issue native pair without original issued Invoice has no invented credit requirement',()=>{
 assert.ok(forward.includes('and request.original_invoice_id is not null order by request.id loop'));
});
// UNIT_ONLY relationship vectors. Actual native resume/checks/clone/locking and
// credit first-use are required separately; no sums are duplicated here.
const basis=()=>({original:'I1',reversed:'T0',reversal:'R0',cnOriginal:'I1',headerReversed:'T0',headerReversal:'R0',lineTS:'T0',lineR0:'R0',
 cn:'CN',issued:true,document:true,r0Lock:'CN',nativeResume:'CN'});
const originalPredicate=x=>x.issued&&x.lineTS===x.reversal;
const currentPredicate=x=>x.issued&&x.document&&x.cnOriginal===x.original&&x.headerReversed===x.reversed&&x.headerReversal===x.reversal
 &&x.lineTS===x.reversed&&x.lineR0===x.reversal&&x.r0Lock===x.cn&&x.nativeResume===x.cn;
test('native-shaped T0/R0/CN relationship reproduces original refusal and successor permits exact native proof',()=>{
 const x=basis();assert.equal(originalPredicate(x),false);assert.equal(currentPredicate(x),true);
});
for(const [key,value] of [['issued',false],['document',false],['cnOriginal','foreign'],['headerReversed','otherT0'],['headerReversal','otherR0'],
 ['lineTS','R0'],['lineR0','otherR0'],['r0Lock',null],['nativeResume',null],['nativeResume','foreignCN']])test('foreign/missing native credit relationship '+key+' refuses',()=>{
 const x=basis();x[key]=value;assert.equal(currentPredicate(x),false);
});
console.log('UNIT_ONLY complete source reversal and actual identity relationship vectors; no native credit or financial operation.');
