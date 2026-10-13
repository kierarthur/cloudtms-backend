import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { nativeCase } from './native-casebook.mjs';

function object(x){if(!x||typeof x!=='object'||Array.isArray(x))throw Error('NFX_OBJECT_REQUIRED');return x;}
function uuid(x){if(!/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/i.test(String(x)))throw Error('NFX_UUID_REQUIRED');return x;}
function literal(x){return `'${String(x).replaceAll("'","''")}'`;}
function json(x){return `${literal(JSON.stringify(x))}::jsonb`;}
function rpcSql(name,arg){if(!/^weekly_source_[a-z_0-9]+$/.test(name))throw Error('NFX_RPC_OUTSIDE_SOURCE');return `public.${name}(${json(arg)})`;}
function htmlCell(x){return `<td>${String(x).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;')}</td>`;}
function dateDisplay(x){const [y,m,d]=x.split('-');return `${d}/${m}/${y}`;}
function addDay(x,n){const d=new Date(x+'T12:00:00Z');d.setUTCDate(d.getUTCDate()+n);return d.toISOString().slice(0,10);}

// Pure local bridge: never opens a connection, fetches a real URL or runs SQL.
// Its input must be exported from the root-owned SAME psql transaction.
export async function buildNativePhase({sourceRoot,input}){
 object(input);if(input.contract!=='NFX_NATIVE_CONTEXT_V1')throw Error('NFX_CONTEXT_CONTRACT_REQUIRED');
 const c=nativeCase(input.case_id),context=object(input.context);
 const actor=uuid(input.actor_user_id),client=uuid(input.fixture_client_id),candidate=uuid(input.fixture_candidate_id),contract=uuid(input.fixture_contract_id);
 if(context.client_id&&context.client_id!==client)throw Error('NFX_FOREIGN_CLIENT_CONTEXT');
 if(context.environment&&context.environment!=='TEST')throw Error('NFX_TEST_CONTEXT_REQUIRED');
 const load=relative=>import(pathToFileURL(path.resolve(sourceRoot,relative)).href);
 if(input.phase==='STAGE'){
  if(c.kind!=='NULL_RULE_HEALTHROSTER')throw Error('NFX_STAGE_CASE_NOT_YET_SOURCE_REVIEWED');
  if(context.finalisation_week_ending!==c.weekEnding||context.effective_rule_set!=null)throw Error('NFX_NULL_RULE_WEEK_REQUIRED');
  const parser=await load('broker/src/weekly-source/index.js');
  const adapter=await load('broker/src/weekly-source/upload-staging-adapter.mjs');
  const headers=['Request Id','Staff','Date','From','To','Break','Start','End','Actual Break','Hours','Finalised Date','Timesheet Finalised By'];
  const keys=input.external_source_keys??[input.external_source_key],count=c.rowCount??1,dates=c.workDates??[c.workDate];
  if(!Array.isArray(keys)||keys.length!==count||new Set(keys).size!==count||keys.some(k=>!/^[A-Z0-9_-]{8,100}$/.test(k)))throw Error('NFX_SOURCE_KEY_INVALID');
  const workerName=input.fixture_candidate_name??'NFX October Nurse';
  if(!['NFX October Nurse','NFX October Multi Nurse'].includes(workerName))throw Error('NFX_FIXTURE_NAME_REQUIRED');
  const sourceRows=dates.map((date,i)=>[keys[i],workerName,dateDisplay(date),c.start,c.end,'0',c.start,c.end,'0','8:00',dateDisplay(date)+' 18:00','NFX fixture export owner']);
  const bytes=new TextEncoder().encode(`<html><body><table><tr>${headers.map(htmlCell).join('')}</tr>${sourceRows.map(values=>`<tr>${values.map(htmlCell).join('')}</tr>`).join('')}</table></body></html>`);
  const parsed=await parser.parseWeeklySourceFile(bytes,{profileId:'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1',expectedClient:context.client_name,effectiveRuleSet:context.effective_rule_set});
  if(!parsed.ok||parsed.rows.length!==count||parsed.rows.some(r=>r.actual.totalMinutes!==c.paidMinutes))throw Error('NFX_REAL_PARSER_VECTOR_FAILED');
  const adapted=adapter.adaptWeeklySourceParserOutput(parsed,{actor_user_id:actor,original_filename:c.id+'.html',import_use:'PREPARE_FINALISATION',coverage:{start_local_date:addDay(c.weekEnding,-6),end_local_date:c.weekEnding,confirmed_at_utc:input.database_observed_at_utc}},context);
  const sql=`do $nfx_stage$ declare begun jsonb;sealed jsonb;projection jsonb;upload uuid;begin
begun:=${rpcSql('weekly_source_upload_stage_begin_atomic_v1',adapted.beginRequest)};
if begun->>'ok' is distinct from 'true' or begun->>'status' in ('CONFLICT','DUPLICATE') then raise exception 'NFX_NATIVE_STAGE_BEGIN_FAILED';end if;
upload:=coalesce(begun->>'upload_id',begun->>'logical_upload_id')::uuid;
if upload is null then raise exception 'NFX_NATIVE_STAGE_ID_MISSING';end if;
update pg_temp.nfx_state set upload_id=upload;
perform public.weekly_source_upload_stage_rows_atomic_v1(${json({actor_user_id:actor,physical_rows:adapted.physicalRows,normalised_rows:adapted.normalisedRows,money_evidence:adapted.moneyEvidence,expense_evidence:adapted.expenseEvidence})}||jsonb_build_object('upload_id',upload));
sealed:=public.weekly_source_upload_seal_atomic_v1(jsonb_build_object('actor_user_id',${literal(actor)},'upload_id',upload));
if sealed->>'ok' is distinct from 'true' or sealed->>'status'<>'CURRENT' then raise exception 'NFX_NATIVE_SEAL_FAILED';end if;
projection:=public.weekly_source_projection_begin_atomic_v1(jsonb_build_object('actor_user_id',${literal(actor)},'upload_id',upload,'expected_authority_scope_version',sealed->'authority_scope_version'));
if projection->>'ok' is distinct from 'true' or projection->>'status'<>'BUILDING' then raise exception 'NFX_NATIVE_PROJECTION_BEGIN_FAILED';end if;
update pg_temp.nfx_state set publication_id=(projection->>'publication_id')::uuid;end;$nfx_stage$;`;
  return {sql,fixture_file:{byte_count:bytes.byteLength,sha256:parsed.sourceFileSha256,source_kind:parsed.sourceKind,storage_status:'LOCAL_AUTHORED_BYTES_ONLY_NO_R2_CLAIM'},parser_profile:parsed.profileId};
 }
 if(input.phase==='PROJECTION'){
  const count=c.rowCount??1;
  if(!Array.isArray(context.rows)||context.rows.length!==count)throw Error('NFX_ACTUAL_CONTEXT_ROW_COUNT_CHANGED');
  if(context.rows.some(row=>row.candidate_id!==candidate||row.client_id!==client||(row.contracts??[]).length!==1||row.contracts[0].contract_id!==contract))throw Error('NFX_FOREIGN_OR_AMBIGUOUS_PRICE_CONTEXT');
  const owner=await load('broker/src/weekly-source/upload-publication-owner.mjs');
  const rows=owner.buildWeeklySourceProjectionRows(context);
  if(rows.length!==count||rows.some(row=>row.mapping_state!=='RESOLVED'||row.contract_id!==contract))throw Error('NFX_REAL_PRICE_OWNER_UNRESOLVED');
  const publication=uuid(input.publication_id);
  return {sql:`do $nfx_projection$ declare published jsonb;begin
perform public.weekly_source_projection_rows_apply_atomic_v1(${literal(actor)}::uuid,${literal(publication)}::uuid,${json(rows)});
published:=public.weekly_source_projection_publish_atomic_v1(jsonb_build_object('actor_user_id',${literal(actor)},'publication_id',${literal(publication)}));
if published->>'ok' is distinct from 'true' or published->>'status'<>'CURRENT' then raise exception 'NFX_NATIVE_PUBLICATION_FAILED';end if;end;$nfx_projection$;`,producer:'buildWeeklySourceProjectionRows',seeded_outcomes:false};
 }
 if(input.phase==='PAY_SNAPSHOT'){
  const priorFetch=globalThis.fetch;const unexpected=[];const used=[];
  const responses=object(input.native_read_responses);
  globalThis.fetch=async(url,init={})=>{
   const parsed=new URL(String(url));const name=parsed.pathname.match(/^\/rest\/v1\/rpc\/(tsfin_load_context_batch|tsfin_load_weekly_context_batch)$/)?.[1];
   if(parsed.hostname!=='nfx-transaction.invalid'||init.method!=='POST'||!name){unexpected.push('UNDECLARED_NATIVE_READ');throw Error('NFX_NETWORK_AND_EXTRA_RPC_FORBIDDEN');}
   const args=JSON.parse(init.body);
   if(JSON.stringify(args)!==JSON.stringify({p_timesheet_ids:[input.root_context.root_timesheet_id]})){unexpected.push('FOREIGN_NATIVE_READ');throw Error('NFX_FOREIGN_NATIVE_READ');}
   if(!Object.hasOwn(responses,name))throw Error('NFX_ACTUAL_NATIVE_RESPONSE_REQUIRED');
   used.push(name);return Response.json(responses[name]);
  };
  try{
   const worker=await load('broker/src/index.js');
   const built=await worker.weeklySourceScenarioJourneyInternals.buildWeeklyCorrectFinalServiceSnapshot({SUPABASE_URL:'https://nfx-transaction.invalid',SUPABASE_SERVICE_ROLE_KEY:'NFX_LOCAL_BRIDGE_NONCREDENTIAL'},{root_context:input.root_context,actor_user_id:actor,final_revision_id:input.final_revision_id});
   if(unexpected.length||used.length!==2)throw Error('NFX_UNDECLARED_CALCULATOR_DEPENDENCY');
   return {service_snapshot:built,producer:'weeklySourceScenarioJourneyInternals.buildWeeklyCorrectFinalServiceSnapshot',native_response_owners:used,seeded_outcomes:false};
  }finally{globalThis.fetch=priorFetch;}
 }
 if(input.phase==='PAY_APPLY'){
  const result=object(input.calculator_result);
  if(result.producer!=='weeklySourceScenarioJourneyInternals.buildWeeklyCorrectFinalServiceSnapshot'
    ||result.seeded_outcomes!==false||!Array.isArray(result.native_response_owners)
    ||result.native_response_owners.length!==2||!result.native_response_owners.includes('tsfin_load_context_batch')
    ||!result.native_response_owners.includes('tsfin_load_weekly_context_batch'))throw Error('NFX_REAL_CALCULATOR_OUTPUT_REQUIRED');
  const snapshot=object(result.service_snapshot);
  return {sql:`do $nfx_pay_apply$ declare s pg_temp.nfx_state%rowtype;task jsonb;started jsonb;receipt jsonb;finished jsonb;begin
select * into strict s from pg_temp.nfx_state;
if s.actor_id<>${literal(actor)}::uuid or s.client_id<>${literal(client)}::uuid or s.candidate_id<>${literal(candidate)}::uuid or s.contract_id<>${literal(contract)}::uuid then raise exception 'NFX_PAY_OUTPUT_SCOPE_MISMATCH';end if;
task:=s.pay_run#>'{tasks,0}';
started:=public.weekly_source_finalisation_pay_task_start_atomic_v1(jsonb_build_object('schema_version','WEEKLY_SOURCE_FINALISATION_PAY_TASK_START_V1','actor_user_id',s.actor_id,'run_id',s.pay_run->>'run_id','task_id',task->>'task_id','expected_task_version',task->'version','expected_context_hash',task->>'prepared_context_hash'));
if started->>'ok' is distinct from 'true' or jsonb_typeof(started->'projection_request')<>'object' then raise exception 'NFX_NATIVE_PAY_START_FAILED';end if;
receipt:=public.weekly_source_ordinary_pay_projection_apply_atomic_v1((started->'projection_request')||jsonb_build_object('service_snapshot',${json(snapshot)}));
if receipt->>'ok' is distinct from 'true' or receipt->>'receipt_id' is null or receipt->>'receipt_hash' is null then raise exception 'NFX_NATIVE_PAY_APPLY_FAILED';end if;
finished:=public.weekly_source_finalisation_pay_task_finish_atomic_v1(jsonb_build_object('schema_version','WEEKLY_SOURCE_FINALISATION_PAY_TASK_FINISH_V1','actor_user_id',s.actor_id,'run_id',s.pay_run->>'run_id','task_id',task->>'task_id','expected_task_version',started#>'{task,version}','projection_receipt_id',receipt->>'receipt_id','projection_receipt_hash',receipt->>'receipt_hash'));
if finished->>'ok' is distinct from 'true' or finished#>>'{tasks,0,state}' not in ('PREPARED_FOR_AUTHORISATION','PROPOSED') then raise exception 'NFX_NATIVE_PAY_FINISH_FAILED';end if;
update pg_temp.nfx_state set pay_run=finished,pay_start=started;
end;$nfx_pay_apply$;`,producer:'native ordinary pay start/apply/finish',seeded_outcomes:false};
 }
 throw Error('NFX_PHASE_UNKNOWN');
}

if(process.argv[1]===new URL(import.meta.url).pathname.replace(/^\//,'').replaceAll('/','\\')||process.argv[1]?.endsWith('native-pure-bridge.mjs')){
 const sourceRoot=path.resolve(process.argv[2]),inputPath=path.resolve(process.argv[3]),outputPath=path.resolve(process.argv[4]);
 const input=JSON.parse(fs.readFileSync(inputPath,'utf8'));
 const result=await buildNativePhase({sourceRoot,input});
 if(result.sql){if(Buffer.byteLength(result.sql)>2_000_000)throw Error('NFX_ARTHUR_BYTE_LIMIT');fs.writeFileSync(outputPath,result.sql+'\n');}
 else fs.writeFileSync(outputPath,JSON.stringify(result,null,2)+'\n');
 console.log(JSON.stringify({contract:'NFX_PURE_BRIDGE_OUTPUT_V1',phase:input.phase,bytes:fs.statSync(outputPath).size,network_calls:0,database_calls:0,native_acceptance:false}));
}
