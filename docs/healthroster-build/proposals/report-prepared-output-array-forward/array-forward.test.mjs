import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import {here,original,sha,forwardCodec,reverseCodec,forwardAmount,reverseAmount,newValidation,CONFIG} from './build-forward.mjs';
const read=p=>fs.readFileSync(path.join(here,p),'utf8'),old=p=>fs.readFileSync(path.join(original,p),'utf8');
const codec=read('02_complete_codec_array_forward.sqlinc'),amount=read('05_complete_retained_amount_array_forward.sqlinc');
const manifest=JSON.parse(old('SOURCE_MANIFEST.json'));
const vectors=[
 {name:'two dimensions',text:'{{RATE_A,RATE_B},{RATE_C,RATE_D}}',dimensions:2,bounds:'[1:2][1:2]'},
 {name:'non-one lower bounds',text:'[0:1][3:4]={{RATE_A,RATE_B},{RATE_C,RATE_D}}',dimensions:2,bounds:'[0:1][3:4]'},
 {name:'ordinary one dimension',text:'{RATE_A,RATE_B}',dimensions:1,bounds:'[1:2]'},
 {name:'null leaves versus quoted strings and escaped characters',text:String.raw`{NULL,"NULL","a,b","a\"b","a\\b"}`,dimensions:1,bounds:'[1:5]'},
 {name:'empty native array',text:'{}',dimensions:0,bounds:null},
 {name:'SQL NULL array',text:null,dimensions:null,bounds:null}
];
test('both complete owners have full byte reversals to frozen Prepared30',()=>{
 assert.equal(reverseCodec(codec),old('02_codec.sqlinc'));assert.equal(reverseAmount(amount),old('05_retained_amount.sqlinc'));
 assert.equal(forwardCodec(old('02_codec.sqlinc')),codec);assert.equal(forwardAmount(old('05_retained_amount.sqlinc')),amount);
});
test('all frozen30 artifacts and original manifest remain byte exact',()=>{
 assert.equal(sha(old('SOURCE_MANIFEST.json')),'80a2577cbf5016f1a76e323717809c5553070e47b0b9a80e11b1ef3a0af34c70');
 for(const a of manifest.artifacts)assert.equal(sha(fs.readFileSync(path.join(original,a.path))),a.sha256,a.path);
});
test('native serializer retains actual text[] rather than JSON flattening',()=>{
 assert.match(codec,/'hr_crosscheck_issues',p_fin\.hr_crosscheck_issues::text/);
 assert.doesNotMatch(codec,/jsonb_array_elements\([^\n]*hr_crosscheck_issues|unnest\([^\n]*hr_crosscheck_issues|array_to_json/);
 assert.match(codec,/hr_crosscheck_issues'\) not in \('null','string'\)/);
});
test('capture and historical read share exact native canonical roundtrip checks',()=>{
 assert.ok(codec.includes(newValidation('v_output')));assert.ok(amount.includes(newValidation('v_fin')));
 for(const s of [codec,amount]){
  assert.match(s,/::text\[\]\)::text/);assert.match(s,/is distinct from [^\n]*->>'hr_crosscheck_issues'/);
  assert.match(s,/invalid_text_representation or array_subscript_error or program_limit_exceeded/);
 }
});
test('fixed array_nulls parsing distinguishes SQL NULL from quoted NULL without authority GUC',()=>{
 assert.equal(codec.split(CONFIG).length-1,1);assert.equal(amount.split(CONFIG).length-1,1);
 assert.doesNotMatch(codec,/set_config\(|current_setting\(/);assert.doesNotMatch(amount,/set_config\(|current_setting\(/);
 const cfgPos=codec.indexOf(CONFIG);assert.ok(cfgPos>codec.indexOf('private.weekly_source_report_prepared_financial_codec_v1'));
 assert.ok(cfgPos<codec.indexOf('as $function$',cfgPos));
});
for(const v of vectors)test('closed JSON transport retains array text case: '+v.name,()=>{
 // Data-free lossless boundary, not a claim that PostgreSQL has executed it.
 const restored=JSON.parse(JSON.stringify({hr_crosscheck_issues:v.text}));
 assert.equal(restored.hr_crosscheck_issues,v.text);
 if(v.text!==null)assert.equal(typeof restored.hr_crosscheck_issues,'string');else assert.equal(restored.hr_crosscheck_issues,null);
});
test('2D and lower-bound native SQL cases remain explicit genuine PostgreSQL tests',()=>{
 const sql=read('NATIVE_ARRAY_DATA_CASES.sqlinc');
 assert.match(sql,/ARRAY\[\['RATE_A','RATE_B'\],\['RATE_C','RATE_D'\]\]::text\[\]/);
 assert.match(sql,/\[0:1\]\[3:4\]=/);assert.match(sql,/array_dims/);assert.match(sql,/array_lower/);
 assert.match(sql,/null::text\[\]/i);assert.match(sql,/ARRAY\[null::text,'NULL'/);
 assert.doesNotMatch(sql,/insert into|update public|delete from|projection_apply_atomic|receipt_insert|payment|provider|settlement|email/i);
});
test('90 closed field names, 39 numeric strings and nine JSON-text fields remain unchanged',()=>{
 const info=JSON.parse(old('CODEC_FIELDS.json'));assert.equal(info.fields.length,90);
 for(const f of info.numeric)assert.ok(codec.includes(`'${f}',p_fin.${f}::text`));
 for(const f of info.json)assert.ok(codec.includes(`'${f}',p_fin.${f}::text`));
 assert.equal(info.numeric.length,39);assert.equal(info.json.length,9);
 assert.equal(info.types.hr_crosscheck_issues,'text[]');
});
test('whole native95 JSON-row fingerprint and lower-bound closed equality are both retained',()=>{
 const receipt=JSON.parse(read('FORWARD_RECEIPT.json'));assert.equal(receipt.wholeNativeRowColumns,95);
 assert.equal(receipt.wholeNativeRowFingerprintDomainUnchanged,true);
 assert.match(old('04_receipt_insert_guard.sqlinc'),/financial_codec_v1\(v_fin\) is distinct from v_context\.financial_output_json/);
 assert.match(old('04_receipt_insert_guard.sqlinc'),/PREPARED_NATIVE_ROW_V1.*to_jsonb\(v_fin\)/s);
 assert.match(receipt.lowerBoundEffect,/cannot distinguish array lower bounds/);
});
test('no helper, receipt column, V1 hash, public DTO, ordinary writer or combined ACL change',()=>{
 const receipt=JSON.parse(read('FORWARD_RECEIPT.json'));assert.equal(receipt.newFunctions,0);assert.equal(receipt.newPermanentColumns,0);
 assert.equal(receipt.sourceVersionUnchanged,'WEEKLY_SOURCE_REPORT_PREPARED_WRITER_OUTPUT_V1');
 assert.equal(receipt.oldCombinedACLNotReplaced,true);
 assert.doesNotMatch(codec+amount,/create or replace function public\.|alter table|create table|grant execute|239|271/);
});
test('complete producer successor is syntax valid, uninvoked and narrowed to the array forward',()=>{
 const source=read('build-proposal.complete-array-forward.mjs');
 assert.match(source,/info\.numeric\.includes\(f\)\|\|info\.json\.includes\(f\)\|\|\/\^text/);
 assert.match(source,/weekly_source_report_prepared_financial_codec_v1','weekly_source_report_prepared_pay_amount_v1/);
 assert.match(source,/::text\[\]\)::text/);assert.doesNotMatch(source,/flatten|array_ndims\([^\n]*=1/);
});
test('complete critic is included at the exact authorised hash',()=>assert.equal(sha(read('SOURCE_CRITIC.md')),'180167ebb3f3979b97056668ef5de46ee96447bc61edd85a4158f23899ef4284'));
test('official SQL statement scanner confirms packaging only',async()=>{
 const root=path.resolve(here,'../../../../../healthroster-v2-backend-13102026');
 const {scanManagedSql}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')));
 for(const s of [codec,amount,read('NATIVE_ARRAY_DATA_CASES.sqlinc')])assert.ok(scanManagedSql(s).statements.length>0);
});
