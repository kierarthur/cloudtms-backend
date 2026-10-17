// Fixed source-only packet builder. No filesystem, connection, holder or execution.
import {createHash} from 'node:crypto';
export const auditQuery="select md5(coalesce(string_agg(md5(to_jsonb(a)::text),'|' order by md5(to_jsonb(a)::text)),'')) from public.audit_events a";
export const settingNames=Object.freeze(['work_mem','temp_file_limit','max_parallel_workers_per_gather',
 'max_parallel_workers','max_worker_processes','max_parallel_maintenance_workers',
 'min_parallel_table_scan_size','min_parallel_index_scan_size','parallel_setup_cost','parallel_tuple_cost',
 'parallel_leader_participation','enable_sort','enable_incremental_sort','enable_presorted_aggregate',
 'enable_gathermerge','enable_parallel_append','enable_parallel_hash','debug_parallel_query',
 'jit','jit_above_cost','jit_inline_above_cost','jit_optimize_above_cost','jit_expressions','jit_tuple_deforming',
 'pg_show_plans.is_enabled','statement_timeout']);
const sha=x=>createHash('sha256').update(x).digest('hex');
const must=v=>{if(!v)throw Error('QT_PLAN_ORIGINAL_TURN_REQUIRED');};
function exactIdentity(v){
 const keys=['physicalDatabase','databaseOid','backendPid','turnId','key','takenAt','task'];
 must(v&&typeof v==='object'&&!Array.isArray(v)&&Object.keys(v).length===keys.length&&keys.every(k=>Object.hasOwn(v,k)));
 must(v.physicalDatabase==='cloudtms_test_clone'&&Number.isSafeInteger(v.databaseOid)&&v.databaseOid>0&&v.databaseOid<=4294967295
  &&Number.isSafeInteger(v.backendPid)&&v.backendPid>0&&v.backendPid<=2147483647
  &&typeof v.turnId==='string'&&/^hr_turn_[0-9a-f]{32}$/.test(v.turnId)&&JSON.stringify(v.key)==='[20261013,1]'
  &&typeof v.takenAt==='string'&&Number.isFinite(Date.parse(v.takenAt))&&new Date(v.takenAt).toISOString()===v.takenAt
  &&typeof v.task==='string'&&v.task.length>0&&v.task.length<=512);
 const copy=JSON.parse(JSON.stringify(v));Object.freeze(copy.key);return Object.freeze(copy);
}
function contextFields(identity){
 const held=`exists(select 1 from pg_catalog.pg_locks where locktype='advisory' and database=${identity.databaseOid} and pid=${identity.backendPid} and classid=20261013 and objid=1 and objsubid=2 and mode='ExclusiveLock' and granted)`;
 return `'version','QT_AUDIT_PLAN_CONTEXT_V1','database',current_database(),
 'databaseOid',(select oid::bigint from pg_catalog.pg_database where datname=current_database()),
 'backendPid',pg_backend_pid(),'holderPid',${identity.backendPid},'holderGranted',(${held}),
 'readOnly',current_setting('transaction_read_only')::boolean,'noRoleSwitch',current_user=session_user,
 'observedAt',clock_timestamp(),'testIdentity',exists(select 1 from private.cloudtms_database_identity where singleton and environment='TEST' and customer_key is null),
 'legacyOne',exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='LEGACY' and owner_epoch=1),
 'applyingCount',(select count(*)::text from private.cloudtms_database_releases where status='APPLYING')`;
}
export function buildInput(actualOriginalTurnIdentity){
 const identity=exactIdentity(actualOriginalTurnIdentity),fields=contextFields(identity);
 const settings=settingNames.map((name,i)=>`(${i+1},'${name}')`).join(',');
 const metadata=`with expected_settings(ordinal,name) as (values ${settings})
 select jsonb_build_object(${fields},
 'settings',(select jsonb_agg(jsonb_build_object('name',e.name,'present',s.name is not null,
   'setting',s.setting,'unit',s.unit,'source',s.source,'pendingRestart',s.pending_restart) order by e.ordinal)
  from expected_settings e left join pg_catalog.pg_settings s on s.name=e.name),
 'auditRelations',coalesce((select jsonb_agg(jsonb_build_object('schema',n.nspname,'name',c.relname,'oid',c.oid::bigint,
   'kind',c.relkind,'persistence',c.relpersistence,'owner',pg_catalog.pg_get_userbyid(c.relowner),
   'pages',c.relpages,'tuplesEstimate',c.reltuples,'allVisiblePages',c.relallvisible,'options',c.reloptions,
   'rls',c.relrowsecurity,'forceRls',c.relforcerowsecurity,
   'canSelect',pg_catalog.has_table_privilege(current_user,c.oid,'SELECT'),
   'canUseSchema',pg_catalog.has_schema_privilege(current_user,n.oid,'USAGE')) order by c.oid)
  from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace
  where n.nspname='public' and c.relname='audit_events'),'[]'::jsonb)) as qt_audit_plan_context;`;
 const sql="set local transaction_read_only=on;\nset local jit=off;\nset local pg_show_plans.is_enabled=off;\nset local work_mem='4MB';\nset local maintenance_work_mem='32MB';\nset local search_path=pg_catalog,public;\n"+metadata+'\nexplain (format json,verbose true,settings true) '+auditQuery+';';
 const verification_sql=`select jsonb_build_object(${fields}) as qt_audit_plan_context;`;
 const input=Object.freeze({database:'agency_test',label:'QT_AUDIT_PLAN_METADATA_20261017_'+identity.turnId,sql,verification_sql,
  continue_on_error:false,max_rows_per_statement:1,statement_timeout_ms:30000,lock_timeout_ms:2000});
 return Object.freeze({input,receipt:Object.freeze({version:'QT_AUDIT_PLAN_METADATA_RECEIPT_V1',identity,
  sqlSha256:sha(sql),verificationSqlSha256:sha(verification_sql),auditQuerySha256:sha(auditQuery),
  mainStatements:8,freshStatements:1,explainAnalyze:false,effectQualified:false,restorationClaimed:false})});
}
