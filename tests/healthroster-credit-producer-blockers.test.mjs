import test from 'node:test';import assert from 'node:assert/strict';
import {startAutomaticCorrectionCredits} from '../broker/src/weekly-source/agency-invoice-credit-consumer.mjs';
const u=n=>'f7200000-0000-4000-8000-'+String(n).padStart(12,'0');
const row=(id,blocked=false)=>({request_id:u(id),root_timesheet_id:u(1),correction_id:'awc:'+u(1)+':'+u(id),state_revision:'4',source_revision:blocked?null:'a'.repeat(64),
 original_invoice_id:u(2),reversal_timesheet_id:u(id+100),reversed_timesheet_id:u(3),replacement_timesheet_id:u(id+200),client_id:u(4),candidate_id:u(5),
 week_ending_date:'2026-10-18',candidate_name:'Synthetic fixture',invoice_number:'FIXTURE',blocker_code:blocked?'CREDIT_SCOPE_AUTHORITY_UNPROVED':null,
 selection_key:'healthroster-credit:'+u(2)+':'+u(id+100),selectable:false,automatic:true});
const envelope=rows=>({contract_version:'TIMESHEET_CORRECTION_CREDIT_CANDIDATES_V1',rows});
test('an actual blocked producer row never registers a native credit command',async()=>{
 let calls=0;const result=await startAutomaticCorrectionCredits(envelope([row(10,true)]),async()=>{calls++;throw Error('must not execute');});
 assert.equal(calls,0);assert.deepEqual(result,[{selection_key:row(10,true).selection_key,original_invoice_id:u(2),reversal_timesheet_id:u(110),operation_id:null,operation_receipt:null,accepted:false,blocked:true,already_credited:false,code:'CREDIT_SCOPE_AUTHORITY_UNPROVED'}]);
});
test('a blocked row keeps its reason while the distinct proved row keeps its native operation receipt',async()=>{
 const calls=[];const result=await startAutomaticCorrectionCredits(envelope([row(10,true),row(11)]),async command=>{calls.push(command);return {per_command_results:[{command_type:'GENERATE_CREDIT_NOTE',operation_id:u(400),operation_type:'GENERATE_CREDIT_NOTE',accepted:true,status:'PENDING',phase:'RESOLVE',blocked:false}]};});
 assert.equal(calls.length,1);assert.equal(calls[0].credit_scope.reversal_timesheet_id,u(111));assert.equal(result[0].blocked,true);assert.equal(result[1].accepted,true);assert.equal(result[1].operation_receipt.phase,'RESOLVE');assert.equal(result[1].operation_id,u(400));
});
test('a malformed READY row refuses the entire producer envelope before any mutation',async()=>{
 let calls=0;const invalid={...row(11),source_revision:null};
 await assert.rejects(startAutomaticCorrectionCredits(envelope([row(10),invalid]),async()=>{calls++;}),error=>error.code==='CORRECTION_CREDIT_PROJECTION_INVALID');assert.equal(calls,0);
});

