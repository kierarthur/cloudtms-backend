// Purpose-bound setup caller. Root supplies its approved authenticated service
// RPC transport; this module has no credentials, URL, SQL connection or default
// network route. Each awaited rpc call must be one top-level RPC transaction.
import fs from 'node:fs';
import path from 'node:path';
import {createHash,randomUUID} from 'node:crypto';
const UUID=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const HASH=/^[0-9a-f]{64}$/;
const PAGE_KEYS=['ok','contract','actor_user_id','permission_view','binding','read_revision','eligible_count','missing_count','rows','has_more','next_cursor'];
const ROW_KEYS=['timesheet_id','timesheet_version','source_group_id','client_id','week_ending','native_cycle_week_ending',
  'source_cycle_id','witness_identity','submission_witness_sha256','admission_sha256'];
const WITNESS_KEYS=['authority','workflow_id','workflow_generation','hours_component_id','candidate_signature_component_id','document_version_id'];
const object=value=>value!==null&&typeof value==='object'&&!Array.isArray(value);
const closed=(value,keys)=>object(value)&&Object.keys(value).length===keys.length&&keys.every(key=>Object.hasOwn(value,key));
const uuid=value=>typeof value==='string'&&UUID.test(value);
const hash=value=>typeof value==='string'&&HASH.test(value);
const count=value=>Number.isSafeInteger(value)&&value>=0;
const copy=value=>JSON.parse(JSON.stringify(value));
const sha=bytes=>createHash('sha256').update(bytes).digest('hex');
const refuse=code=>{throw Error(code);};
function date(value){
  if(typeof value!=='string'||!/^\d{4}-\d{2}-\d{2}$/.test(value))return false;
  const parsed=new Date(value+'T00:00:00Z');return Number.isFinite(parsed.getTime())&&parsed.toISOString().slice(0,10)===value;
}
function row(value,missing=true){
  if(!closed(value,ROW_KEYS)||!uuid(value.timesheet_id)||!count(value.timesheet_version)
    ||!uuid(value.source_group_id)||!uuid(value.client_id)||!date(value.week_ending)||!date(value.native_cycle_week_ending)
    ||!hash(value.submission_witness_sha256)||!hash(value.admission_sha256)
    ||(missing?value.source_cycle_id!==null:!uuid(value.source_cycle_id)))refuse('HISTORICAL_MANIFEST_ROW_INVALID');
  const own=new Date(value.week_ending+'T00:00:00Z'),cycle=new Date(value.native_cycle_week_ending+'T00:00:00Z');
  // Validate the native mapping; never choose a date or manufacture a cycle.
  if(cycle.getUTCDay()!==0||cycle-own<0||cycle-own>6*86400000)refuse('HISTORICAL_MANIFEST_ROW_INVALID');
  const witness=value.witness_identity;
  if(!closed(witness,WITNESS_KEYS))refuse('HISTORICAL_MANIFEST_WITNESS_INVALID');
  if(witness.authority==='CANDIDATE_IMMUTABLE_HOURS'){
    if(!['workflow_id','hours_component_id','candidate_signature_component_id'].every(key=>uuid(witness[key]))
      ||!Number.isSafeInteger(witness.workflow_generation)||witness.workflow_generation<1||witness.document_version_id!==null)
      refuse('HISTORICAL_MANIFEST_WITNESS_INVALID');
  }else if(witness.authority==='VERIFIED_LEGACY_SOURCE_DOCUMENT'){
    if(!uuid(witness.document_version_id)||['workflow_id','workflow_generation','hours_component_id','candidate_signature_component_id']
      .some(key=>witness[key]!==null))refuse('HISTORICAL_MANIFEST_WITNESS_INVALID');
  }else refuse('HISTORICAL_MANIFEST_WITNESS_INVALID');
  return copy(value);
}
function page(value,actor){
  if(!closed(value,PAGE_KEYS)||value.ok!==true||value.contract!=='WEEKLY_SOURCE_SUBMISSION_MANIFEST_V1'
    ||value.actor_user_id!==actor||value.permission_view!=='ADMIN'||!hash(value.binding)||!hash(value.read_revision)
    ||!count(value.eligible_count)||!count(value.missing_count)||value.missing_count>value.eligible_count
    ||!Array.isArray(value.rows)||value.rows.length>100||value.rows.length>value.missing_count||typeof value.has_more!=='boolean'
    ||(value.has_more?(typeof value.next_cursor!=='string'||value.next_cursor.length<1||value.next_cursor.length>4096||value.rows.length===0):value.next_cursor!==null))
    refuse('HISTORICAL_MANIFEST_PAGE_INVALID');
  return {...copy(value),rows:value.rows.map(value=>row(value))};
}
function receipt(value,expected,actor){
  if(!closed(value,['ok','contract','actor_user_id','timesheet_id','metadata_only','descriptor'])||value.ok!==true
    ||value.contract!=='WEEKLY_SOURCE_HISTORICAL_SUBMISSION_PREPARE_V1'||value.actor_user_id!==actor
    ||value.timesheet_id!==expected.timesheet_id||value.metadata_only!==true)refuse('HISTORICAL_PREPARATION_RECEIPT_INVALID');
  const observed=row(value.descriptor,false);
  for(const key of ROW_KEYS.filter(key=>key!=='source_cycle_id')){
    if(key==='witness_identity'?WITNESS_KEYS.some(field=>observed[key][field]!==expected[key][field]):observed[key]!==expected[key])
      refuse('HISTORICAL_PREPARATION_RECEIPT_INVALID');
  }
  return copy(value);
}
function saveExact(directory,name,value){
  const file=path.join(directory,name),bytes=Buffer.from(JSON.stringify(value,null,2)+'\n');
  // Append a new immutable evidence file; never overwrite a prior run/receipt.
  const handle=fs.openSync(file,'wx');
  try{fs.writeFileSync(handle,bytes);fs.fsyncSync(handle);}finally{fs.closeSync(handle);}
  const expected=sha(bytes),observed=sha(fs.readFileSync(file));
  if(observed!==expected)refuse('HISTORICAL_PREPARATION_EVIDENCE_CHANGED');
  return {path:file,sha256:observed};
}
function finish(directory,report){
  try{report.result_evidence=saveExact(directory,'result.json',report);}
  catch{report.ok=false;report.result_evidence=null;report.evidence_persistence_failed=true;}
  return report;
}
async function collect(rpc,actor,maxEntries){
  const rows=[],seen=new Set(),cursors=new Set();let cursor,header,last;
  do{
    const value=page(await rpc('weekly_source_checklist_submission_manifest_v1',{
      p_request:{actor_user_id:actor,limit:100,...(cursor?{cursor}:{})}
    }),actor);
    if(!header)header=value;
    else if(['binding','read_revision','eligible_count','missing_count'].some(key=>value[key]!==header[key]))
      refuse('HISTORICAL_MANIFEST_REFRESH_REQUIRED');
    if(value.missing_count>maxEntries)refuse('HISTORICAL_MANIFEST_CAPACITY_EXCEEDED');
    for(const item of value.rows){
      if(seen.has(item.timesheet_id)||(last&&item.timesheet_id<=last))refuse('HISTORICAL_MANIFEST_ORDER_INVALID');
      seen.add(item.timesheet_id);last=item.timesheet_id;rows.push(item);
    }
    if(value.has_more){
      if(cursors.has(value.next_cursor))refuse('HISTORICAL_MANIFEST_CURSOR_REPEATED');
      cursors.add(value.next_cursor);cursor=value.next_cursor;
    }else cursor=null;
  }while(cursor);
  if(rows.length!==header.missing_count)refuse('HISTORICAL_MANIFEST_INCOMPLETE');
  return {contract:'WEEKLY_SOURCE_HISTORICAL_SETUP_MANIFEST_V1',actor_user_id:actor,permission_view:'ADMIN',
    binding:header.binding,read_revision:header.read_revision,eligible_count:header.eligible_count,missing_count:header.missing_count,rows};
}
/** Explicit setup only, while root's feature/native qualification gate is held.
 * No browser GET, business save, Authorise/UNA, Bank or email owner is called.
 * Never pass a transport which batches several RPCs into one SQL transaction.
 */
export async function prepareHistoricalChecklistMetadata({rpc,actorUserId,evidenceDirectory,runId=randomUUID(),maxEntries=10000}={}){
  if(typeof rpc!=='function'||!uuid(actorUserId)||typeof evidenceDirectory!=='string'||!path.isAbsolute(evidenceDirectory)
    ||!uuid(runId)||!Number.isSafeInteger(maxEntries)||maxEntries<1||maxEntries>100000)
    refuse('HISTORICAL_PREPARATION_CONTEXT_INVALID');
  fs.mkdirSync(evidenceDirectory,{recursive:true});const directory=path.join(evidenceDirectory,runId);fs.mkdirSync(directory);
  // Read the COMPLETE same-revision manifest before any metadata mutation.
  // Capacity/inconsistent/stale/invalid pages refuse before the first writer.
  const manifest=await collect(rpc,actorUserId,maxEntries);
  const manifestEvidence=saveExact(directory,'manifest.json',manifest),receipts=[];
  const report={ok:false,contract:'WEEKLY_SOURCE_HISTORICAL_SETUP_RESULT_V1',actor_user_id:actorUserId,
    status:'UNFINISHED',manifest:manifestEvidence,eligible_count:manifest.eligible_count,requested_count:manifest.rows.length,
    confirmed_count:0,receipts,qualification:false};
  for(const expected of manifest.rows){
    let outcome;
    try{
      // Exactly ONE top-level RPC for this actual Timesheet, sequentially.
      // The SQL owner rechecks witness/admission and I1/F1 -> G1 itself.
      const actual=receipt(await rpc('weekly_source_checklist_historical_prepare_atomic_v1',{p_request:{
        actor_user_id:actorUserId,timesheet_id:expected.timesheet_id,
        submission_witness_sha256:expected.submission_witness_sha256,admission_sha256:expected.admission_sha256
      }}),expected,actorUserId);
      outcome={timesheet_id:expected.timesheet_id,outcome:'CONFIRMED',receipt:actual};
      report.confirmed_count++;
    }catch{
      // A transport/decoder failure may follow a committed native transaction.
      // Preserve prior confirmations and the unknown outcome; NEVER retry.
      outcome={timesheet_id:expected.timesheet_id,outcome:'UNCONFIRMED',receipt:null};
    }
    let evidence;
    try{evidence=saveExact(directory,`receipt-${expected.timesheet_id}.json`,outcome);}
    catch{
      receipts.push({...outcome,evidence:null});report.status='STOPPED_EVIDENCE_PERSISTENCE_FAILED';
      // Preserve the actual confirmed/unknown result in the return value and
      // stop before another writer; do not relabel it as a rolled-back save.
      return report;
    }
    receipts.push({...outcome,evidence});
    if(outcome.outcome!=='CONFIRMED'){
      report.status='STOPPED_UNCONFIRMED_TRANSACTION';return finish(directory,report);
    }
  }
  // A fresh native census, from origin, proves remaining missing bookkeeping.
  // Its whole population count is authoritative; no synthetic local empty page.
  let verified;
  try{verified=page(await rpc('weekly_source_checklist_submission_manifest_v1',{p_request:{actor_user_id:actorUserId,limit:100}}),actorUserId);}
  catch{report.status='PREPARED_VERIFICATION_UNAVAILABLE';return finish(directory,report);}
  try{report.verification=saveExact(directory,'verification.json',verified);}
  catch{report.status='PREPARED_VERIFICATION_EVIDENCE_UNAVAILABLE';return finish(directory,report);}
  report.remaining_count=verified.missing_count;
  report.status=verified.missing_count===0?'METADATA_PREPARED':'MISSING_BOOKKEEPING_REMAINS';
  report.ok=verified.missing_count===0;
  return finish(directory,report);
}
