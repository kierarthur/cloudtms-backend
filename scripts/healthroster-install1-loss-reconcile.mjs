// Own lost-attempt reconciliation: read only, no session cancellation/release.
import fs from 'node:fs';
import {withSharedRehearsalTurn,agencyChildEnvironment} from '../scripts/healthroster-rehearsal-turn.mjs';
import {identityProbeSql,restorationSnapshotSql} from '../scripts/healthroster-install1-full-rehearsal.mjs';
import {digest} from '../scripts/cloudtms-managed-release-sql.mjs';
const folder='C:/tmp/healthroster-install1-native-20261014T085448Z-cewJAg';
const files=fs.readdirSync(folder).filter(x=>x.endsWith('_RECEIPT.json')).sort();
const old=JSON.parse(fs.readFileSync(folder+'/'+files.at(-1)));
if(old.status!=='HELD_NOT_COMPLETE'||old.failure!=='TURN_CONNECTION_CLOSED')throw Error('EXACT_OWN_HELD_LOSS_REQUIRED');
const progressPath='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-v2-backend-13102026/docs/healthroster-build/PROGRESS.md';
const result=await withSharedRehearsalTurn({task:'I1 lost own516948 turn read-only restoration reconciliation',progressPath,async work(turn){
 const env=agencyChildEnvironment('healthroster_install1_loss_reconcile');
 const probe=async sql=>{const r=await turn.runOwnedChild({file:'C:/Program Files/PostgreSQL/18/bin/psql.exe',args:['-X','-q','-A','-t','-v','ON_ERROR_STOP=on','-v','VERBOSITY=sqlstate','-f','-'],
  input:'begin isolation level repeatable read read only;\n'+sql+'\nrollback;\n',options:{env:{...env,PGOPTIONS:env.PGOPTIONS+' -c statement_timeout=0'},stdio:['pipe','pipe','pipe']},cancelOnLoss:true});
  if(r.code!==0)throw Error('RECONCILIATION_READ_ONLY_FAILED');const lines=r.stdout.split(/\r?\n/).filter(x=>/^[\[{]/.test(x));if(lines.length!==1)throw Error('RECONCILIATION_JSON_REFUSED');return JSON.parse(lines[0]);};
 const infrastructure=await probe(`select jsonb_build_object('database',current_database(),'postmasterStartedAt',pg_postmaster_start_time(),'checkedAt',clock_timestamp(),
  'serverVersion',current_setting('server_version'),'oldTurnPresent',exists(select 1 from pg_stat_activity where datname=current_database() and pid=516948),
  'oldTurnLockPresent',exists(select 1 from pg_locks where database=324139 and pid=516948 and locktype='advisory' and classid=20261013 and objid=1 and objsubid=2 and granted),
  'ownRehearsalSessions',(select count(*) from pg_stat_activity where datname=current_database() and application_name='healthroster_install1_full'),
  'idleSessionTimeout',current_setting('idle_session_timeout'),'idleTransactionTimeout',current_setting('idle_in_transaction_session_timeout'),
  'statementTimeout',current_setting('statement_timeout'));`);
 console.log(JSON.stringify(infrastructure));
 const identity=await probe(identityProbeSql(old.marker));
 const relations=await probe("select coalesce(jsonb_agg(jsonb_build_object('schema',n.nspname,'name',c.relname,'kind',c.relkind::text) order by n.nspname,c.relname),'[]'::jsonb) from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname in ('public','private') and c.relkind in ('r','p');");
 const after=await probe(restorationSnapshotSql({identity:turn.identity,marker:old.marker,relations,exportContractSql:fs.readFileSync('supabase/release/export_contract.sql','utf8')}));
 const compared=Object.fromEntries(['ledgerFingerprint','catalogueFingerprint','relationCensus','businessCountsFingerprint','businessRowsFingerprint'].map(key=>[key,{before:old.before?.[key]??null,after:after[key],equal:old.before?.[key]===after[key]}]));
 const prior=new Map((old.before?.businessRelations??[]).map(r=>[r.relation,r]));
 const changes=after.businessRelations.filter(r=>r.count!==prior.get(r.relation)?.count||r.hash!==prior.get(r.relation)?.hash).map(r=>({relation:r.relation,beforeCount:prior.get(r.relation)?.count,afterCount:r.count,beforeHash:prior.get(r.relation)?.hash,afterHash:r.hash}));
 return {status:'LOST_ATTEMPT_RECONCILED_NOT_FULL_PASS',candidatePin:old.candidatePin,oldReceiptSha256:digest(fs.readFileSync(folder+'/'+files.at(-1))),infrastructure,identity,compared,businessChanges:changes,
  markerAbsent:after.markerAbsent,module:after.module,explicitFullRollbackProved:false,fixtureCaptureLost:true,fullSqlErrorCollectionLost:true,installation:false,otherSessionsCancelled:false,backgroundJobsPaused:false};
}});
fs.writeFileSync('docs/healthroster-build/install1-rehearsal/FULL_ATTEMPT3_LOSS_RECONCILIATION_14102026.json',JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify({status:result.status,markerAbsent:result.markerAbsent,module:result.module,compared:result.compared,businessChanges:result.businessChanges}));
