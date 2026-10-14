import fs from 'node:fs';
import {withSharedRehearsalTurn,agencyChildEnvironment} from '../scripts/healthroster-rehearsal-turn.mjs';
import {identityProbeSql,restorationSnapshotSql} from '../scripts/healthroster-install1-full-rehearsal.mjs';
const progressPath='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-v2-backend-13102026/docs/healthroster-build/PROGRESS.md';
const marker='hr_install1_94564519a7ea00e8a453e21972ed72b6';
const result=await withSharedRehearsalTurn({task:'I1 own read-only failed precheck diagnosis; no rehearsal SQL',progressPath,async work(turn){
 const env=agencyChildEnvironment('healthroster_install1_precheck_diagnosis');
 const run=async(phase,sql)=>{
  const r=await turn.runOwnedChild({file:'C:/Program Files/PostgreSQL/18/bin/psql.exe',args:['-X','-q','-A','-t','-v','ON_ERROR_STOP=on','-v','VERBOSITY=verbose','-f','-'],
   input:'begin isolation level repeatable read read only;\n'+sql+'\nrollback;\n',options:{env:{...env,PGOPTIONS:env.PGOPTIONS+' -c statement_timeout=0'},stdio:['pipe','pipe','pipe']},cancelOnLoss:true});
  console.log(JSON.stringify({phase,exitCode:r.code,sqlstate:r.stderr.match(/(?:ERROR|FATAL):\s*([A-Z0-9]{5})/)?.[1]??null,
   lines:[...r.stderr.matchAll(/psql:<stdin>:(\d+):/g)].map(m=>Number(m[1])),statementCharacters:r.stderr.match(/POSITION:\s*(\d+)/)?.[1]??null,
   undefinedFunction:['pg_input_is_valid','pg_snapshot_xmax','pg_current_snapshot','pg_snapshot_xip','pg_xact_status','private._candidate_daily_json_sha256_v1'].find(name=>r.stderr.includes('function '+name+'('))??null,
   operatorMetadata:r.stderr.match(/operator does not exist:\s*([a-z0-9_ ]{1,40})\s+([><=+*/-]{1,4})\s+([a-z0-9_ ]{1,40})/i)?.slice(1)??null}));
  if(r.code!==0)return null;const rows=r.stdout.split(/\r?\n/).filter(x=>/^[\[{]/.test(x));if(rows.length!==1)throw Error('DIAGNOSTIC_JSON_ENVELOPE_REFUSED');return JSON.parse(rows[0]);
 };
 const identity=await run('IDENTITY',identityProbeSql(marker));if(!identity)return {status:'IDENTITY_FAILED'};
 const relations=await run('RELATION_CENSUS',"select coalesce(jsonb_agg(jsonb_build_object('schema',n.nspname,'name',c.relname,'kind',c.relkind::text) order by n.nspname,c.relname),'[]'::jsonb) from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname in ('public','private') and c.relkind in ('r','p');");
 if(!relations)return {status:'CENSUS_FAILED'};
 const args={identity:turn.identity,marker,relations,exportContractSql:fs.readFileSync('supabase/release/export_contract.sql','utf8')};
 const full=await run('LIVE_SNAPSHOT',restorationSnapshotSql({...args,observeLiveBusiness:true}));
 if(!full){const ordinary=await run('ORIGINAL_SNAPSHOT',restorationSnapshotSql(args));return {status:ordinary?'LIVE_EXTENSION_FAILED':'BOTH_SNAPSHOTS_FAILED'};}
 return {status:'SNAPSHOT_PASS',backgroundWriterCount:full.backgroundWriterRoutines.length};
}});
console.log(JSON.stringify(result));
