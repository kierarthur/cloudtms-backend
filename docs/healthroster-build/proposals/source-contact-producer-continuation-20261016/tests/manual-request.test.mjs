import {test} from 'node:test';
import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {readFileSync} from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
const base=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');const root=path.resolve(base,'../../../..');
const parserSource=execFileSync('git',['show','90ffcf3f40644280fcdb5b791a62a86613896d6e:broker/src/weekly-source/push-timing-office.mjs'],{cwd:root,encoding:'utf8'});
const parse=new Function(parserSource.replace(/\bexport\s+(?=(?:class|function|async function|const))/g,'')+'\nreturn parsePushTimingJson;')();
const api=new Function('parsePushTimingJson',readFileSync(path.join(base,'runtime/contact-manual-enquiry.mjs'),'utf8')
 .replace(/^import[^\n]+\n/,'').replace(/\bexport\s+(?=(?:class|function|const))/g,'')+'\nreturn {contactManualEnquiryRequest,parseContactManualEnquiryBody};')(parse);
const id=n=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const actor={id:id(1),is_active:true,role:'admin'};const deployment={agency_id:id(2),environment:'TEST'};
const body=()=>({action:'CONTACT_MANUAL_ENQUIRY_PREVIEW',payload:{version:'CONTACT_MANUAL_ENQUIRY_V1',request_id:id(3),audience:'MANAGER',
 reason:'Review this still-open issue',scopes:[{candidate_id:id(4),client_id:id(5),selected_issue_ids:[id(6)],
 expected_scope_revision:'9007199254740993',expected_checking_revision:'9223372036854775807'}]}});
test('exact commissioned request preserves bigint strings/reason and injects only authenticated host actor/deployment',()=>{
 const b=body();const request=api.contactManualEnquiryRequest(b,actor,deployment);
 assert.equal(request.scopes[0].expected_checking_revision,'9223372036854775807');
 assert.equal(request.scopes[0].expected_scope_revision,'9007199254740993');assert.equal(request.reason,b.payload.reason);
 assert.equal(request.actor_user_id,actor.id);assert.equal(request.agency_id,deployment.agency_id);
 assert.equal(Object.hasOwn(b.payload,'actor_user_id'),false);
});
for(const key of ['actor_user_id','agency_id','environment','recipient','body','send_ordinal','now_utc'])test(`browser ${key} cannot supply authority`,()=>{
 const b=body();b.payload[key]='untrusted';assert.throws(()=>api.contactManualEnquiryRequest(b,actor,deployment),/CONTACT_MANUAL_ENQUIRY_REQUEST_INVALID/);
});
for(const value of [9007199254740993,'01','-1','1e3','9223372036854775808'])test(`invalid revision ${value} refuses`,()=>{
 const b=body();b.payload.scopes[0].expected_checking_revision=value;assert.throws(()=>api.contactManualEnquiryRequest(b,actor,deployment));
});
test('duplicate scope and duplicate cross-scope issue refuse instead of truncating',()=>{
 const b=body();b.payload.scopes.push(structuredClone(b.payload.scopes[0]));assert.throws(()=>api.contactManualEnquiryRequest(b,actor,deployment));
 b.payload.scopes[1].client_id=id(8);assert.throws(()=>api.contactManualEnquiryRequest(b,actor,deployment));
});
test('actual shared parser refuses literal and escaped duplicate keys at any depth',()=>{
 const text=JSON.stringify(body());assert.throws(()=>api.parseContactManualEnquiryBody(text.replace('"reason":','"reason":"other","reason":')));
 assert.throws(()=>api.parseContactManualEnquiryBody(text.replace('"reason":','"reason":"other","rea\\u0073on":')));
 assert.throws(()=>api.parseContactManualEnquiryBody(text.replace('"client_id":','"client_id":"'+id(5)+'","client_id":')));
});
test('full500-issue request is accepted beyond old16KiB timing transport without truncation',()=>{
 const b=body();b.payload.scopes[0].selected_issue_ids=Array.from({length:500},(_,n)=>id(n+100));
 const parsed=api.parseContactManualEnquiryBody(JSON.stringify(b));assert.equal(api.contactManualEnquiryRequest(parsed,actor,deployment).scopes[0].selected_issue_ids.length,500);
 b.payload.scopes[0].selected_issue_ids.push(id(999));assert.throws(()=>api.contactManualEnquiryRequest(b,actor,deployment));
});
test('accept adds only exact genuine review ID/hash; no fresh reason/default or ordinal',()=>{
 const b=body();b.action='CONTACT_MANUAL_ENQUIRY_ACCEPT';b.payload.review_id=id(9);b.payload.review_digest='a'.repeat(64);
 const result=api.contactManualEnquiryRequest(b,actor,deployment);assert.equal(result.review_id,id(9));assert.equal(result.reason,b.payload.reason);
 delete b.payload.reason;assert.throws(()=>api.contactManualEnquiryRequest(b,actor,deployment));
});
test('inactive/nonadmin/payment-only actor cannot turn contact into financial permission',()=>{
 for(const user of [{...actor,is_active:false},{...actor,role:'user',payment_authoriser:true},{...actor,role:'user',payment_golden_key:true}])
  assert.throws(()=>api.contactManualEnquiryRequest(body(),user,deployment),/WEEKLY_SOURCE_OFFICE_ADMIN_REQUIRED/);
});
test('read-only current witness helper never creates a scope/journal or derives UI clock authority',()=>{
 const sql=readFileSync(path.join(base,'forward/01_checking_revision_and_history.sqlinc'),'utf8');
 const start=sql.indexOf('create or replace function private.weekly_contact_manual_enquiry_read_v1(');
 const read=sql.slice(start,sql.indexOf('$function$;',sql.indexOf('as $function$',start))+12);
 assert.doesNotMatch(read,/\b(insert|update|delete)\b/i);assert.doesNotMatch(read,/scope_lock|clock_timestamp|statement_timestamp/i);
 assert.match(read,/prospective:=prospective\+1/);assert.match(read,/'available_issue_ids',issues/);
 assert.match(sql,/n>1000 then raise exception 'CONTACT_CHECKING_SCOPE_CAPACITY_EXCEEDED'/);
});
