// Pure data fixture generation. No database, R2, upload route or sender call.
// The historical Nov-2026 workbook remains unchanged. Each new file is made
// from a real current native context, then read by the SAME actual parser.
import fs from 'node:fs';
import path from 'node:path';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import * as XLSX from 'xlsx';
import {parseWeeklySourceFile} from '../broker/src/weekly-source/index.js';
import {adaptWeeklySourceParserOutput} from '../broker/src/weekly-source/upload-staging-adapter.mjs';
// Exact current transitive relative source dependencies of the two real pure
// owners above. Package/module dependencies are bound by package/lock files.
export const f5ParserInputs=Object.freeze(['acacium-report-normalise.js','contract-qualification.js','coverage-warnings.mjs','csv-evidence.js',
 'economic-snapshot.js','errors.js','healthroster-normalise.js','html-evidence.js','index.js','manager-email.js','money.js','nhsp-normalise.js',
 'profiles.js','protected-target-schedule.js','report-upload-provenance.mjs','roster-summary-normalise.js','rule-set-validation.js','shared.js',
 'source-finalised-time.js','source-invoice-money.js','source-price-comparator.js','upload-staging-adapter.mjs','weekly-rate-owner.js','xlsx-evidence.js']
 .map(file=>'broker/src/weekly-source/'+file));
const fail=code=>{throw Error(code);};
const sha=bytes=>createHash('sha256').update(bytes).digest('hex');
const isoDate=value=>typeof value==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(value)&&new Date(value+'T12:00:00Z').toISOString().slice(0,10)===value;
const local=iso=>Object.fromEntries(new Intl.DateTimeFormat('en-GB',{timeZone:'Europe/London',year:'numeric',month:'2-digit',day:'2-digit',
 hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).formatToParts(new Date(iso)).filter(p=>p.type!=='literal').map(p=>[p.type,p.value]));
const dateAdd=(value,days)=>{const d=new Date(value+'T12:00:00Z');d.setUTCDate(d.getUTCDate()+days);return d.toISOString().slice(0,10);};
const operations=['weekly_source_upload_stage_begin_atomic_v1','weekly_source_upload_stage_rows_atomic_v1','weekly_source_upload_seal_atomic_v1','weekly_source_invoice_hold_release_atomic_v1'];
export async function generateF5SyntheticFixture(context){
 const auth=context?.authorization,c=context?.cycle,now=Date.parse(context?.observed_at_utc),end=Date.parse(c?.cutoff_at_utc),start=Date.parse(c?.previous_cutoff_at_utc);
 if(!auth||auth.schema_version!=='WEEKLY_SOURCE_OFF_RULE_TEST_AUTHORIZATION_V1'||auth.purpose!=='SOURCE_FIXTURE_SETUP'
  ||!c||!isoDate(c.finalisation_week_ending)||new Date(c.finalisation_week_ending+'T12:00:00Z').getUTCDay()!==0
  ||![now,start,end].every(Number.isFinite)||start>=end||end>=now||auth.week_ending!==c.finalisation_week_ending
  ||auth.source_cycle_id!==c.id||auth.source_group_id!==c.source_group_id||auth.environment!=='TEST'
  ||auth.fixture_manifest?.client_ids?.length!==1||auth.fixture_manifest.client_ids[0]!==c.scope_client_id
  ||!operations.every(op=>auth.allowed_operations?.includes(op))||c.current_complete_upload_id!==null
  ||c.current_projection_publication_id!==null||c.current_final_revision_id!==null||['FINALISING','FINALISED','CORRECTION_IN_PROGRESS'].includes(c.state))fail('F5_REAL_PAST_OPEN_CYCLE_REQUIRED');
 const workDate=dateAdd(c.finalisation_week_ending,-6);
 // Minute precision belongs to the synthetic export itself. Native creation
 // and confirmation use their actual clocks, never this source timestamp.
 let generated;
 for(let hour=1;hour<=24;hour++){
  const finalMs=Math.floor((end-hour*3600000)/60000)*60000;
  if(finalMs<=start||finalMs>=now)continue;
  const parts=local(finalMs),date=`${parts.year}-${parts.month}-${parts.day}`;
  if(date<workDate)continue;
  const finalToken=`${parts.day}/${parts.month}/${parts.year} ${parts.hour}:${parts.minute}`;
  const headers=['Request Id','Staff','Date','From','To','Break','Start','End','Actual Break','Hours','Finalised Date','Timesheet Finalised By','Agency Worker Pay','Actual Cost','Agency Commission Amount'];
  const sheet=XLSX.utils.aoa_to_sheet([headers,['F5-SYNTHETIC-000123','F5 synthetic Worker',workDate,'09:00','17:00',0,'09:00','17:00',0,'8:00',finalToken,'F5 synthetic manager','100.00','120.02','DO NOT MAP FEES']]);
  const book=XLSX.utils.book_new();book.Props={Title:'Explicit synthetic F5 regression; no genuine acceptance',CreatedDate:new Date(context.observed_at_utc)};
  XLSX.utils.book_append_sheet(book,sheet,'Export');const bytes=Buffer.from(XLSX.write(book,{type:'buffer',bookType:'xlsx'}));
  const parsed=await parseWeeklySourceFile(bytes,{profileId:'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1',effectiveRuleSet:{values:{
   invoice_sender:'CLIENT_SELF_BILL',invoice_amount_basis:'SOURCE_AMOUNT',source_amount_basis:'EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY',invoice_membership:'FINALISED_IN_CYCLE_WINDOW'}}});
  if(parsed.ok!==true||parsed.rows?.length!==1||Date.parse(parsed.rows[0].finalisedAtUtc)!==finalMs)continue;
  const adapted=adaptWeeklySourceParserOutput(parsed,{actor_user_id:auth.journey_actor_user_id,original_filename:'f5-synthetic-open-window.xlsx',
   import_use:'CHECKING',coverage:{start_local_date:workDate,end_local_date:c.finalisation_week_ending,confirmed_at_utc:context.observed_at_utc}},
   {environment:'TEST',agency_id:auth.agency_id,source_group_id:auth.source_group_id,source_cycle_id:c.id,client_id:c.scope_client_id});
  const row=adapted.normalisedRows[0];
  if(row.source_shift_charge_pence!=='10000'||row.source_vat_pence!=='2002'||row.source_gross_pence!=='12002'||adapted.moneyEvidence.length!==2)fail('F5_SYNTHETIC_ECONOMICS_CHANGED');
  const proof={schema_version:'F5_SYNTHETIC_OPEN_WINDOW_FILE_V1',source_file_sha256:sha(bytes),byte_count:bytes.length,source_bytes_base64:bytes.toString('base64'),
   source_cycle_id:c.id,source_group_id:c.source_group_id,client_id:c.scope_client_id,week_ending:c.finalisation_week_ending,work_date:workDate,
   finalised_at_utc:row.finalised_at_utc,window_start_at_utc:c.previous_cutoff_at_utc,window_end_at_utc:c.cutoff_at_utc,observed_at_utc:context.observed_at_utc,
   parser_version:parsed.parserVersion,adapter_version:'WEEKLY_SOURCE_UPLOAD_ADAPTER_V1',synthetic:true,genuine_acceptance:false,r2_or_route_proof:false};
  generated={proof,begin_request:adapted.beginRequest,stage_request:{physical_rows:adapted.physicalRows,normalised_rows:adapted.normalisedRows,money_evidence:adapted.moneyEvidence,expense_evidence:adapted.expenseEvidence}};break;
 }
 if(!generated)fail('F5_NATIVE_WINDOW_SYNTHETIC_FILE_UNREPRESENTABLE');
 return generated;
}
export function compileF5SyntheticVerifier({root,sourceCommit,verifier,generated}){
 const file=path.join(root,verifier),bytes=fs.readFileSync(file);
 const committed=execFileSync('git',['--no-replace-objects','cat-file','blob',sourceCommit+':'+verifier],{cwd:root,maxBuffer:2*1024*1024,stdio:['ignore','pipe','pipe']});
 if(!bytes.equals(committed))fail('F5_REGISTERED_VERIFIER_SOURCE_MISMATCH');
 const marker='-- F5_GENERATED_DATA_INSERT_REQUIRED';let source=bytes.toString('utf8');
 if(source.split(marker).length!==2||generated?.proof?.synthetic!==true)fail('F5_REGISTERED_DATA_TEMPLATE_REQUIRED');
 const input=JSON.stringify(generated).replaceAll("'","''");
 source=source.replace(marker,`insert into pg_temp.f5_generated_input(value) values ('${input}'::jsonb);`);
 // The existing protected psql adapter already sets ON_ERROR_STOP. Its SQL
 // mode cannot mix a psql meta command with the complete transaction text.
 source=source.replace(/^\\set ON_ERROR_STOP on\r?$/m,'');
 if(/^\s*\\/m.test(source))fail('F5_UNEXPECTED_VERIFIER_PSQL_COMMAND');
 return {sql:source,registered_verifier_sha256:sha(bytes)};
}
export function assertF5SyntheticProof(proof){
 const fields=['schema_version','source_file_sha256','byte_count','source_bytes_base64','source_cycle_id','source_group_id','client_id','week_ending','work_date',
  'finalised_at_utc','window_start_at_utc','window_end_at_utc','observed_at_utc','parser_version','adapter_version','synthetic','genuine_acceptance','r2_or_route_proof'].sort();
 if(!proof||JSON.stringify(Object.keys(proof).sort())!==JSON.stringify(fields)||proof.schema_version!=='F5_SYNTHETIC_OPEN_WINDOW_FILE_V1'
  ||proof.synthetic!==true||proof.genuine_acceptance!==false||proof.r2_or_route_proof!==false
  ||typeof proof.source_bytes_base64!=='string'||proof.source_bytes_base64.length>200000||!Number.isSafeInteger(proof.byte_count)||proof.byte_count<1)fail('F5_SYNTHETIC_FILE_PROOF_INVALID');
 const bytes=Buffer.from(proof.source_bytes_base64,'base64');
 if(bytes.toString('base64')!==proof.source_bytes_base64||bytes.length!==proof.byte_count||sha(bytes)!==proof.source_file_sha256
  ||!['source_cycle_id','source_group_id','client_id'].every(k=>typeof proof[k]==='string'&&/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(proof[k]))
  ||proof.parser_version!=='WEEKLY_SOURCE_STRICT_V1'||proof.adapter_version!=='WEEKLY_SOURCE_UPLOAD_ADAPTER_V1'
  ||!isoDate(proof.week_ending)||!isoDate(proof.work_date)||proof.work_date!==dateAdd(proof.week_ending,-6)
  ||!(Date.parse(proof.window_start_at_utc)<Date.parse(proof.finalised_at_utc)&&Date.parse(proof.finalised_at_utc)<=Date.parse(proof.window_end_at_utc)
   &&Date.parse(proof.window_end_at_utc)<Date.parse(proof.observed_at_utc)))fail('F5_SYNTHETIC_FILE_PROOF_INVALID');
 return proof;
}
