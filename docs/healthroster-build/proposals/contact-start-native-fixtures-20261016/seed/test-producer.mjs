// Pure production parser/adapter/projection tests; no RPC/database/provider mock.
import assert from 'node:assert/strict';
import fs from 'node:fs';import path from 'node:path';import os from 'node:os';import crypto from 'node:crypto';import zlib from 'node:zlib';
import {fileURLToPath,pathToFileURL} from 'node:url';import {execFileSync} from 'node:child_process';
const root=process.argv.find(x=>x.startsWith('--root='))?.slice(7);if(!root)throw Error('--root=PATH required');
const base=path.dirname(fileURLToPath(import.meta.url)),artifact=JSON.parse(fs.readFileSync(path.join(base,'ARTIFACT.json'),'utf8'));
const load=async p=>import(pathToFileURL(path.resolve(root,p)).href);
const {parseWeeklySourceFile}=await load('broker/src/weekly-source/index.js');
const {adaptWeeklySourceParserOutput}=await load('broker/src/weekly-source/upload-staging-adapter.mjs');
const {buildWeeklySourceProjectionRows:build}=await load('broker/src/weekly-source/upload-publication-owner.mjs');
const {ROSTER_WEEKLY_SUMMARY_ACTUAL_V1_HEADERS:headers}=await load('broker/src/weekly-source/profiles.js');
const bytes=fs.readFileSync(path.join(base,'zero-report.csv')),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
let cases=0;
const test=async(name,fn)=>{await fn();cases++;console.log('PASS '+name);};
await test('actual CSV byte digest/census',async()=>{
 assert.equal(sha(bytes),artifact.report.sha256);assert.equal(bytes.length,artifact.report.byte_count);
 const parsed=await parseWeeklySourceFile(bytes,{profileId:artifact.report.profile_id});
 assert.equal(parsed.ok,true);assert.equal(parsed.rows.length,2);assert.ok(parsed.rows.every(x=>x.rowKind==='SOURCE_ZERO'));
});
await test('actual adapter round trip and omission distinct from missing money',async()=>{
 const parsed=await parseWeeklySourceFile(bytes,{profileId:artifact.report.profile_id});
 const produced=adaptWeeklySourceParserOutput(parsed,{actor_user_id:artifact.begin_request.actor_user_id,original_filename:'contact-native-zero.csv',import_use:'CHECKING',
  coverage:{start_local_date:artifact.week_start,end_local_date:artifact.week_ending,confirmed_at_utc:artifact.begin_request.coverage_confirmed_at_utc}},
  {environment:'TEST',agency_id:artifact.begin_request.agency_id,source_group_id:artifact.begin_request.source_group_id,
   source_cycle_id:null,client_id:artifact.begin_request.client_id,client_name:'Contact Native Fixture Trust'});
 assert.deepEqual(produced.beginRequest,artifact.begin_request);
 assert.deepEqual(produced.normalisedRows,artifact.stage_request.normalised_rows);
 assert.deepEqual(produced.physicalRows,artifact.stage_request.physical_rows);
 assert.deepEqual(produced.expenseEvidence,artifact.stage_request.expense_evidence);
 assert.equal(produced.moneyEvidence.length,0);assert.equal(produced.expenseEvidence.length,2);
 assert.ok(produced.normalisedRows.every(r=>r.source_expense_pence==='0'&&r.source_expense_parse_state==='OMITTED_ZERO'&&r.source_shift_charge_pence===null));
});
await test('actual projection ZERO_SOURCE never invokes a financial snapshot',()=>{
 const rows=build(artifact.expected_context);assert.ok(rows.every(r=>r.upload_row_id===undefined&&r.effective_policy_fingerprint===undefined));
 assert.deepEqual(JSON.parse(JSON.stringify(rows.map((r,i)=>({...r,source_row_ordinal:i+2})))),artifact.projection_templates);
 assert.ok(rows.every(x=>x.mapping_state==='RESOLVED'&&x.link_kind==='ZERO_SOURCE'&&!Object.hasOwn(x,'economic_snapshot')));
});
await test('actual parser refuses header-only report',async()=>{
 const parsed=await parseWeeklySourceFile(Buffer.from(bytes.toString().split('\r\n')[0]+'\r\n'),{profileId:artifact.report.profile_id});
 assert.equal(parsed.ok,false);assert.ok(parsed.fatalErrors.some(e=>e.code==='ROSTER_SUMMARY_NO_DATA_ROWS'));
});
await test('exact 65-header profile rejects a changed required header',async()=>{
 await assert.rejects(parseWeeklySourceFile(Buffer.from(bytes.toString().replace('Line ID','Line Identity')),{profileId:artifact.report.profile_id}),
  e=>e.code==='SOURCE_PROFILE_NOT_RECOGNISED');
});
await test('actual parser refuses duplicate durable line IDs',async()=>{
 const parsed=await parseWeeklySourceFile(Buffer.from(bytes.toString().replaceAll('CONTACT-ZERO-C','CONTACT-ZERO-B')),{profileId:artifact.report.profile_id});
 assert.equal(parsed.ok,false);assert.ok(parsed.fatalErrors.some(e=>e.code==='ROSTER_SUMMARY_DUPLICATE_LINE_ID'));
});
await test('zero stays zero; a negative is not an omitted value',async()=>{
 const lines=bytes.toString().split('\r\n');const cells=lines[1].split(',');cells[headers.indexOf('Total Hours')]='"-1"';lines[1]=cells.join(',');
 const parsed=await parseWeeklySourceFile(Buffer.from(lines.join('\r\n')),{profileId:artifact.report.profile_id});
 assert.equal(parsed.ok,false);assert.ok(parsed.fatalErrors.some(e=>e.code==='ROSTER_SUMMARY_TOTAL_HOURS_INVALID'));
});
await test('actual projection refuses ambiguous Candidate mapping',()=>{
 const context=structuredClone(artifact.expected_context);context.rows[0].candidate_match_count=2;
 assert.equal(build(context)[0].blocker_code,'CANDIDATE_MAPPING_AMBIGUOUS');
});
await test('actual projection refuses a second qualifying Contract',()=>{
 const context=structuredClone(artifact.expected_context),row=context.rows[0];row.contracts.push({...row.contracts[0],contract_id:'f716c000-0000-4000-8000-000000000099'});
 assert.equal(build(context)[0].mapping_state,'CONTRACT_SELECTION_REQUIRED');
});
await test('actual projection refuses a foreign Candidate Contract',()=>{
 const context=structuredClone(artifact.expected_context);context.rows[0].contracts[0].candidate_id='f716c000-0000-4000-8000-000000000012';
 assert.equal(build(context)[0].mapping_state,'NO_ELIGIBLE_CONTRACT');
});
await test('real TEST PNG bytes have a valid RGBA/zlib shape and matching digest',()=>{
 const png=fs.readFileSync(path.join(base,'test-declaration.png'));assert.equal(sha(png),artifact.signature.sha256);assert.equal(png.length,artifact.signature.byte_count);
 assert.equal(png.subarray(0,8).toString('hex'),'89504e470d0a1a0a');assert.equal(png.readUInt32BE(16),artifact.signature.width);assert.equal(png.readUInt32BE(20),artifact.signature.height);
 const blocks=[];for(let off=8;off<png.length;){const length=png.readUInt32BE(off);if(png.subarray(off+4,off+8).toString()==='IDAT')blocks.push(png.subarray(off+8,off+8+length));off+=length+12;}
 const raw=zlib.inflateSync(Buffer.concat(blocks));assert.equal(raw.length,artifact.signature.height*(1+4*artifact.signature.width));assert.ok(raw.includes(0));
 assert.equal(artifact.signature.object_uploaded,false);assert.equal(artifact.native_execution,'NOT_RUN');
});
await test('repeatable preparation with exact reviewed timestamp and root source',()=>{
 const temp=fs.mkdtempSync(path.join(os.tmpdir(),'contact-seed-producer-unit-'));
 try{
  execFileSync(process.execPath,[path.join(base,'prepare-seed.mjs'),'--root='+root,'--as-of='+artifact.as_of,
   '--confirmed-at='+artifact.begin_request.coverage_confirmed_at_utc,'--root-commit='+artifact.root_commit,'--out='+temp]);
  for(const name of ['ARTIFACT.json','10_generated-artifact.sql','test-declaration.png','zero-report.csv'])assert.deepEqual(fs.readFileSync(path.join(temp,name)),fs.readFileSync(path.join(base,name)));
 }finally{const target=path.resolve(temp);if(path.dirname(target)!==path.resolve(os.tmpdir())||!path.basename(target).startsWith('contact-seed-producer-unit-'))throw Error('Refuse cleanup outside exact task temporary directory');fs.rmSync(target,{recursive:true});}
});
console.log(JSON.stringify({production_export_cases:cases,passed:true,native_database:'NOT_RUN',external_calls:0}));
