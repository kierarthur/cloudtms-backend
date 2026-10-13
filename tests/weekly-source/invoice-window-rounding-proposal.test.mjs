import test from 'node:test';
import assert from 'node:assert/strict';
import {partitionInvoiceWindows,deriveRoundingReceiptAuthority} from '../../docs/healthroster-build/proposals/f5-window-rounding/invoice-window-receipt-proposal.mjs';
const id = n => `f5520000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const hash = n => String(n).padStart(64,'0');
const manifest = {id:id(1),hash:hash(1),finalRevisionId:id(2),clientId:id(3),invoiceWindowCycleId:id(4),ruleVersionId:id(5),ruleValuesSha256:hash(5),vatRatePct:'20'};
const rule = {invoice_sender:'CLIENT_SELF_BILL',invoice_vat_method:'ONCE_ON_TOTAL_HALF_UP'};
const member = (n,net,vat) => ({id:id(n),hash:hash(n),uploadRowId:id(n+100),sourceRowHash:hash(n+100),netPence:String(net),vatPence:String(vat),grossPence:String(BigInt(net)+BigInt(vat)),vatRatePct:'20'});
const release = (m,action,vat) => ({id:id(Number(m.id.slice(-12))+200),hash:hash(200),uploadRowId:m.uploadRowId,sourceRowHash:m.sourceRowHash,
  holdKind:'VAT_FIT',action,netPence:m.netPence,vatPence:String(vat),grossPence:String(BigInt(m.netPence)+BigInt(vat))});
const derive = (members,oldVat,method='ONCE_ON_TOTAL_HALF_UP') => deriveRoundingReceiptAuthority({manifest:{...manifest,memberWitnesses:members.map(m=>({id:m.id,hash:m.hash}))},
  rules:{...rule,invoice_vat_method:method},roundingMovement:oldVat===null?null:{id:id(9),hash:hash(9),originalVatPence:String(oldVat)},members});

test('post-final release uses exact original source and frozen total, without changing net/pay or original rounding',()=>{
  const m=member(10,10000,2100); m.release=release(m,'BILL_AT_NET_PLUS_20',2000);
  const a=derive([m],-100);
  assert.equal(a.originalRoundingVatPence,'-100'); assert.equal(a.admittedRoundingVatPence,'0');
  assert.equal(a.targetVatPence,'2000'); assert.equal(a.targetGrossPence,'12000');
  assert.equal(a.suppressVisibleRoundingLine,true); assert.equal(m.vatPence,'2100');
});
test('opposite raw VAT errors give D0 but mixed later release choices require existing frozen zero authority',()=>{
  const a=member(10,100,30),b=member(11,100,10);
  a.release=release(a,'BILL_AT_NET_PLUS_20',20); b.release=release(b,'BILL_AS_FILE',10);
  const result=derive([a,b],0);
  assert.equal(result.originalRoundingVatPence,'0'); assert.equal(result.admittedLineVatPence,'30');
  assert.equal(result.admittedRoundingVatPence,'10'); assert.equal(result.targetVatPence,'40'); assert.equal(result.targetGrossPence,'240');
  assert.equal(result.suppressVisibleRoundingLine,false);
  assert.throws(()=>derive([a,b],null),/FROZEN_ROUNDING_AUTHORITY_REQUIRED/);
});
test('other mixed choice produces negative rounding; all file choices keep zero visibly suppressed authority',()=>{
  const a=member(10,100,30),b=member(11,100,10);
  a.release=release(a,'BILL_AS_FILE',30); b.release=release(b,'BILL_AT_NET_PLUS_20',20);
  assert.equal(derive([a,b],0).admittedRoundingVatPence,'-10');
  b.release=release(b,'BILL_AS_FILE',10);
  assert.equal(derive([a,b],0).admittedRoundingVatPence,'0');
  assert.equal(derive([a,b],0).suppressVisibleRoundingLine,true);
});
test('D0 without VAT_FIT or releases has no rounding movement or suppressed internal line',()=>{
  const a=member(10,100,20),b=member(11,100,20),r=derive([a,b],null);
  assert.equal(r.roundingMovementId,null); assert.equal(r.admittedRoundingVatPence,'0'); assert.equal(r.suppressVisibleRoundingLine,false);
});
test('signed total grain uses the existing HALF_UP/TRUNCATE authority',()=>{
  const m=member(10,-3,-1);
  assert.equal(derive([m],0,'ONCE_ON_TOTAL_HALF_UP').targetVatPence,'-1');
  assert.equal(derive([m],1,'ONCE_ON_TOTAL_TRUNCATE').targetVatPence,'0');
});
test('receipt sets replay independently of order and reject missing, foreign or forged choice witnesses',()=>{
  const a=member(10,100,30),b=member(11,100,10);
  a.release=release(a,'BILL_AT_NET_PLUS_20',20); b.release=release(b,'BILL_AS_FILE',10);
  assert.deepEqual(derive([a,b],0),derive([b,a],0));
  const foreign=structuredClone(a); foreign.release.uploadRowId=id(999);
  assert.throws(()=>derive([foreign,b],0),/RELEASE_SCOPE_INVALID/);
  const forged=structuredClone(a); forged.release.vatPence='19'; forged.release.grossPence='119';
  assert.throws(()=>derive([forged,b],0),/RELEASE_CHOICE_MONEY_INVALID/);
  const unresolved=member(12,100,30); unresolved.unreleasedVatFit=true;
  assert.throws(()=>derive([unresolved],-10),/UNRELEASED_VAT_FIT/);
});
const sourceCycle={id:id(50),groupId:id(51)};
const windows=[{...sourceCycle,clientId:id(3),weekEnding:'2026-11-01',state:'OPEN',cutoffAtUtc:'2026-11-04T23:59:00Z',previousCutoffAtUtc:'2026-10-28T23:59:00Z'},
  {id:id(40),groupId:id(51),clientId:id(3),weekEnding:'2026-10-25',state:'OPEN',cutoffAtUtc:'2026-10-28T23:59:00Z',previousCutoffAtUtc:'2026-10-21T23:59:00Z'}];
const movement=(n,kind,role) => ({id:id(n),hash:hash(n),finalRevisionId:id(2),finalisationCycleId:sourceCycle.id,clientId:id(3),lineKind:kind,role,
  invoiceWindowCycleId:id(40),finalisedAtUtc:'2026-10-28T12:00:00Z',workDate:'2026-10-20',totalPayPence:'0'});
test('same genuine source revision partitions earlier correction window and current ordinary pay manifest without retagging movements',()=>{
  const pair=[movement(60,'SOURCE_INVOICE_CORRECTION','REVERSAL'),movement(61,'SOURCE_INVOICE_CORRECTION','REPLACEMENT')],ordinary=movement(62,'SOURCE_ORDINARY','POSITIVE');
  ordinary.totalPayPence='8000';
  const groups=partitionInvoiceWindows({sourceCycle,revisionId:id(2),movements:[...pair,ordinary],existingWindows:windows});
  assert.equal(groups.length,2); assert.equal(groups[0].invoiceWindowCycleId,id(40)); assert.equal(groups[0].payOwningManifest,false);
  assert.equal(groups[0].sourceCycleId,id(50)); assert.equal(groups[1].payOwningManifest,true);
  assert.ok([...pair,ordinary].every(m=>m.finalisationCycleId===id(50)&&m.finalRevisionId===id(2)));
});
test('window membership preserves inclusive cutoff/exclusive prior cutoff and refuses foreign/closed targets or pay leakage',()=>{
  const m=movement(60,'SOURCE_INVOICE_CORRECTION','REPLACEMENT');
  const invoke=(row=m,scope=windows)=>partitionInvoiceWindows({sourceCycle,revisionId:id(2),movements:[row],existingWindows:scope});
  assert.equal(invoke({...m,finalisedAtUtc:'2026-10-28T23:59:00Z'}).length,1);
  assert.throws(()=>invoke({...m,finalisedAtUtc:'2026-10-21T23:59:00Z'}),/TIMESTAMP_MISMATCH/);
  assert.throws(()=>invoke({...m,totalPayPence:'1'}),/PAY_FORBIDDEN/);
  assert.throws(()=>invoke(m,windows.map(w=>w.id===id(40)?{...w,clientId:id(8)}:w)),/FOREIGN_INVOICE_WINDOW/);
  assert.throws(()=>invoke(m,windows.map(w=>w.id===id(40)?{...w,state:'FINALISED'}:w)),/INVOICE_WINDOW_NOT_OPEN/);
});
