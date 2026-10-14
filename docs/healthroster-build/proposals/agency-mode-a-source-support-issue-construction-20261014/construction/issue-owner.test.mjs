import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';import test from 'node:test';import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),read=n=>fs.readFileSync(path.join(dir,n),'utf8'),sha=s=>crypto.createHash('sha256').update(s).digest('hex');
const validator=read('14_complete_native_issue_validator_with_agency_requirements.sqlinc'),direct=read('16_complete_actual_direct_issue_with_agency_callbacks.sqlinc'),asyncOwner=read('17_complete_actual_async_issue_with_agency_callbacks.sqlinc'),close=read('15_native_agency_issue_close.sqlinc'),checks=read('13_native_agency_issue_requirements.sqlinc');
const codes=['HEALTHROSTER_VALIDATION_REQUIRED','AGENCY_INVOICE_CREDIT_NOTE_NOT_ISSUED','HEALTHROSTER_EVIDENCE_MISSING'];
test('existing validator and both real issue owners reverse to complete pinned current bodies',()=>{
 const records=[JSON.parse(read('ISSUE_VALIDATOR_FULL_REVERSE.json')),...JSON.parse(read('ACTUAL_ISSUE_OWNERS_FULL_REVERSE.json')).files];
 for(const r of records){let s=read(r.output);assert.equal(sha(s),r.forward_sha256);for(const h of [...r.hunks].reverse()){assert.equal(s.split(h.after).length,2);s=s.replace(h.after,()=>h.before);}assert.equal(s,read(r.input));assert.equal(sha(s),r.input_sha256);}
});
test('only existing batch issue/send owner produces three added codes',()=>{
 assert.ok(validator.includes('private.weekly_source_agency_issue_requirements_v1(s.invoice_id)'));
 assert.ok(validator.includes('private.weekly_source_invoice_issue_blockers_v1(f.invoice_id,'));
 assert.ok(validator.includes('||coalesce(agency.codes,array[]::text[]) blockers'));
 for(const s of [direct,asyncOwner]){assert.ok(s.includes('private._invoice_issue_validate_batch('));assert.ok(!s.includes('private.weekly_source_agency_issue_requirements_v1('));}
});
test('delivery block arrays and issue/deliver eligibility all reflect same hard blocker authority',()=>{
 assert.equal((validator.match(/to_jsonb\(coalesce\(\(select a\.codes from agency_issue_requirements/g)||[]).length,2);
 assert.ok(validator.includes('cardinality(c.blockers)=0'));for(const code of codes)assert.ok(checks.includes(code));
});
test('actual direct callback follows new ISSUE update and native audit, not replay return',()=>{
 assert.ok(direct.indexOf('perform private.weekly_source_agency_issue_close_v1(p_invoice_id)')>direct.lastIndexOf("    'INVOICE_ISSUED',"));
 assert.ok(direct.indexOf('perform private.weekly_source_agency_issue_close_v1(p_invoice_id)')>direct.indexOf("set status = 'ISSUED'"));
 assert.equal((direct.match(/weekly_source_agency_issue_close_v1/g)||[]).length,1);
});
test('async recheck occurs after native invoice retention and before actual UPDATE',()=>{
 assert.ok(asyncOwner.indexOf('private._invoice_issue_validate_batch(',asyncOwner.indexOf('locked_invoices as materialized'))<asyncOwner.indexOf('  issued as ('));
 assert.ok(asyncOwner.includes('and c.payload_json'));assert.ok(asyncOwner.includes('and v.source_revision=i.document_revision::text'));
});
test('async observation consumes issued RETURNING IDs only in a subsequent statement',()=>{
 const at=asyncOwner.indexOf('into v_part,v_agency_issued_ids from outcomes;'),call=asyncOwner.indexOf('perform private.weekly_source_agency_issue_close_v1(v_agency_invoice_id)');
 assert.ok(at>=0&&call>at);assert.ok(asyncOwner.includes('(select coalesce(array_agg(id order by id),array[]::uuid[]) from issued)'));
 assert.ok(asyncOwner.slice(at,call).includes('foreach v_agency_invoice_id in array v_agency_issued_ids loop'));
});
test('ordinary/historical/credit invoice scopes keep native existing protocol',()=>{
 assert.ok(checks.includes("i.type::text<>'INVOICE' or i.status::text not in ('DRAFT','ON_HOLD') or i.issued_at_utc is not null"));
 assert.ok(close.includes("i.type::text<>'INVOICE' or i.status::text<>'ISSUED' or i.issued_at_utc is null"));
 assert.equal((asyncOwner.match(/weekly_source_agency_issue_close_v1/g)||[]).length,1);
});
test('issued Credit Note proof uses current native CREDIT_SOURCE purpose, never DRAFT ISSUE append authority',()=>{
 assert.ok(checks.includes("'validation_purpose','CREDIT_SOURCE'"));assert.ok(!checks.includes("'validation_purpose','ISSUE'"));
 assert.ok(checks.includes("cn.type::text='CREDIT_NOTE' and cn.status::text in ('ISSUED','PAID')"));
 assert.ok(checks.includes('native_credit.valid and native_credit.timesheet_id=r.reversal_timesheet_id'));
});
test('retirement retains DB CANCELLED resolved-null invariant and existing contact owner',()=>{
 assert.ok(close.includes("set state='CANCELLED',resolved_at_utc=null,resolution_kind=null"));
 assert.ok(close.includes('perform private.weekly_source_incident_contact_retire_v1(incident.id)'));
 assert.ok(!close.includes("set state='RESOLVED'"));assert.ok(!close.includes('weekly_discrepancy_events('));
});
test('exact real worker/client/week/publication and noncarried event gate precede retirement',()=>{
 for(const t of ["d.source_group_id=(facts->>'source_group_id')::uuid","d.source_cycle_id=(facts->>'source_cycle_id')::uuid",'x.contract_id=target.contract_id',
  "x.projection_publication_id=(facts->>'publication_id')::uuid",'e.work_date between target.week_ending_date-6 and target.week_ending_date',
  "s->>'work_event_id'=d.work_event_id::text",'for update of d loop'])assert.ok(close.includes(t),t);
});
test('real manual review, signed/Agency pending request, protection and certified WAIT cannot be retired',()=>{
 for(const t of ['private.weekly_source_manual_reviews','private.timesheet_correction_requests pending','private.weekly_source_agency_timesheet_change_requests pending',
  'private.weekly_source_check_only_shift_protections','private.weekly_source_protected_wait_receipt_v1','if protected then continue'])assert.ok(close.includes(t),t);
});
test('Source Missing/credit unknown remains open; no universal Agency pending/paid classifier',()=>{
 assert.ok(close.includes('private.weekly_source_kept_as_paid_context_for_publication_v1'));
 assert.ok(close.includes("source_decision->'decision_needed' is distinct from 'false'::jsonb"));
 assert.ok(!close.includes('coalesce((source_decision'));assert.ok(!close.includes('waiting_amount'));assert.ok(!close.includes('withheld_pence'));
});
test('audit actor/time is actual native issue authority, not fabricated current-XID/PID',()=>{
 assert.ok(close.includes("a.action='INVOICE_ISSUED'"));assert.ok(close.includes("(a.after_json->>'issue_at_utc')::timestamptz=i.issued_at_utc"));
 assert.ok(close.includes('id=actual_audit.actor_user_id and is_active'));assert.ok(!close.includes('pg_current_xact_id'));assert.ok(!close.includes('pg_backend_pid'));
});
// Explicit source-bound reference model of the retirement predicate. The
// native row/audit/contact/locking/permission/receipt schedules remain unrun.
const current=()=>({state:'OPEN',scope:true,family:'HEALTHROSTER_NOT_FINALISED',carried:false,manual:false,p0:false,agency:false,protection:false,qualifiedWait:false,sourceDecision:false});
const retire=r=>r.state==='OPEN'&&r.scope&&['HEALTHROSTER_NOT_FINALISED','SOURCE_MISSING_OR_NOT_AUTHORISED'].includes(r.family)
  &&!r.carried&&!r.manual&&!r.p0&&!r.agency&&!r.protection&&!r.qualifiedWait&&r.sourceDecision===false;
test('genuine uncarried clear Source subject may cancel once and replay has no second retirement',()=>{
 const r=current();assert.ok(retire(r));r.state='CANCELLED';assert.equal(retire(r),false);
});
for(const [field,value] of [['scope',false],['family','SOURCE_HOURS_DIFFER'],['carried',true],['manual',true],['p0',true],['agency',true],['protection',true],['qualifiedWait',true],['sourceDecision',null],['sourceDecision',true]])test('retirement reference counterexample '+field+'='+value,()=>{
 const r=current();r[field]=value;assert.equal(retire(r),false);
});
test('issue requirement known-unavailable contexts cannot clear validation or evidence authority',()=>{
 assert.ok(checks.includes('validation_required:=true'));assert.ok(checks.includes('evidence_missing:=true'));
 assert.ok(checks.includes("if native_message not like 'AGENCY_INVOICE_%' then raise;end if"));
});
console.log('UNIT_ONLY issue source/reversal/predicate models. No native issue/audit/contact or financial effect executed.');
