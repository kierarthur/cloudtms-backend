throw new Error('ARCHIVED_PROBE_ALREADY_COMPLETED_NO_CONNECTION');
import fs from 'node:fs';
import {pathToFileURL} from 'node:url';
const base='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub';
const root=base+'/.codex-worktrees/healthroster-install1-14102026';
const {withSharedRehearsalTurn,agencyChildEnvironment}=await import(pathToFileURL(root+'/scripts/healthroster-rehearsal-turn.mjs'));
const {executePsqlWithinOwnedTurn}=await import(pathToFileURL(root+'/scripts/healthroster-install1-full-rehearsal.mjs'));
const {digest}=await import(pathToFileURL(root+'/scripts/cloudtms-managed-release-sql.mjs'));
const {parseOneJsonResult}=await import(pathToFileURL(root+'/scripts/healthroster-install1-native-coordinator.mjs'));
const result={status:'READ_ONLY_DIAGNOSTIC_RUNNING',startedAt:new Date().toISOString()};
try{
 await withSharedRehearsalTurn({task:'Short readonly psql line-diagnostic and changed-table metadata probe',progressPath:base+'/.codex-worktrees/healthroster-v2-backend-13102026/docs/healthroster-build/PROGRESS.md',async work(turn){
  result.turn=turn.identity;
  const env=agencyChildEnvironment('healthroster_install1_diagnostic_probe');
  const sql=['\\set ON_ERROR_STOP off','\\set ON_ERROR_ROLLBACK on','\\set VERBOSITY verbose','\\set SHOW_CONTEXT never','begin read only;','\\echo HR_FILE_BEGIN 0','select 1 / 0;','\\invalid_native_probe','\\echo HR_FILE_END 0','rollback;','\\echo HR_EXPLICIT_ROLLBACK_COMPLETED'].join('\n')+'\n';
  const diagnostic=await executePsqlWithinOwnedTurn({turn,psqlPath:'C:/Program Files/PostgreSQL/18/bin/psql.exe',env,sql,sqlHash:digest(sql),marker:'hr_install1_'+'e'.repeat(32),expectedContractHash:'7e3c00b70366c2c036cfea419adf32c77df5b65070771591e75168b68f3336d6',diagnosticCodes:[],diagnosticCodesHash:digest('[]'),lineMap:[{startLine:7,endLine:7,file:'HARNESS_READONLY_DIAGNOSTIC_PROBE',kind:'control',statement:1,object:'SELECT_DIVISION_BY_ZERO'},{startLine:8,endLine:8,file:'HARNESS_READONLY_DIAGNOSTIC_PROBE',kind:'control',statement:2,object:'PSQL_INVALID_COMMAND'}]});
  if(diagnostic.errorCollectionComplete!==true||diagnostic.explicitRollback!==true||diagnostic.errors.length!==2||diagnostic.errors[0].sqlstate!=='22012'||diagnostic.errors[0].line!==7||diagnostic.errors[1].error!=='PSQL_CLIENT_ERROR'||diagnostic.errors[1].line!==8)throw Error('PSQL_NATIVE_LINE_MAPPING_NOT_PROVED');
  result.diagnostic=diagnostic;
  const query="select jsonb_build_object('physicalDatabase',current_database(),'batches',(select coalesce(jsonb_agg(to_jsonb(a)),'[]'::jsonb) from (select case when actor_class ~ '^[A-Z][A-Z0-9_]{0,60}$' then actor_class else 'WITHHELD_CLASS' end actor_class,case when operation_class ~ '^[A-Z][A-Z0-9_]{0,60}$' then operation_class else 'WITHHELD_CLASS' end operation_class,count(*) count,min(created_at_utc) first_created,max(created_at_utc) last_created from private.candidate_daily_batch_receipts where environment='TEST' and created_at_utc>='2026-10-14T01:27:19.683Z' group by 1,2) a),'outbox',(select jsonb_build_object('count',count(*),'min_attempts',min(attempt_count),'max_attempts',max(attempt_count),'earliest_retry',min(next_attempt_at),'latest_retry',max(next_attempt_at)) from public.ts_financials_outbox));";
  const probe=await turn.runOwnedChild({file:'C:/Program Files/PostgreSQL/18/bin/psql.exe',args:['-X','-q','-A','-t','-f','-','-v','ON_ERROR_STOP=on','-v','VERBOSITY=sqlstate'],input:'begin read only;\n'+query+'\nrollback;\n',options:{env,stdio:['pipe','pipe','pipe']}});
  if(probe.code!==0||/(?:ERROR|FATAL):/.test(probe.stderr))throw Error('CHANGED_TABLE_METADATA_PROBE_FAILED');
  result.metadata=parseOneJsonResult(probe.stdout);result.status='READ_ONLY_DIAGNOSTIC_PASS_NOT_REHEARSAL';
 }});
}catch(error){result.status='READ_ONLY_DIAGNOSTIC_HELD';result.failure=/^[A-Z][A-Z0-9_]+$/.test(error.code??error.message)?error.code??error.message:'PROBE_FAILURE_WITHHELD';process.exitCode=1;}
result.finishedAt=new Date().toISOString();const file='C:/tmp/healthroster-install1-diagnostics-20261014/PROBE_'+Date.now()+'.json';fs.writeFileSync(file,JSON.stringify(result,null,2)+'\n',{flag:'wx'});console.log(JSON.stringify(result));
