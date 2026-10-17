// Builds one fixed read-only aggregate for the root's existing Kier rollback tool.
// This file has no connector, credential loader, SQL client or execution transport.
import fs from 'node:fs';
import {check,sha,uuid,CLIENT,CANDIDATE,MAPPING,BOOKING,STAGES,request} from './contract.mjs';
const lit=v=>"'"+String(v).replaceAll("'","''")+"'";
const metadata=JSON.parse(fs.readFileSync(new URL('./reference/audit/SOURCE_SCHEMA_EFFECT_BOUNDARIES.json',import.meta.url)));
const mappingMetadata=JSON.parse(fs.readFileSync(new URL('./reference/mapping/SOURCE_METADATA.json',import.meta.url)));
metadata.relations.push(mappingMetadata.relations.find(r=>r.name==='client_hospitals'));
metadata.callbackRoutines.push(mappingMetadata.triggerRoutine);
const find=name=>{const r=metadata.relations.find(r=>r.name===name&&r.schema==='public');check(r,'FIXTURE_REAL_RELATION_REQUIRED');return r;};
const column=(table,name)=>check(find(table).columns.some(c=>c.name===name),'FIXTURE_REAL_COLUMN_REQUIRED');
for(const [t,cols] of Object.entries({clients:['id','name'],client_hospitals:['id','client_id','hospital_name_norm','ward_hint'],client_settings:['id','client_id','effective_from'],candidates:['id','key_norm','display_name','nhsp_hr_name_aliases'],candidate_job_titles:['candidate_id'],rates_client_defaults:['id','client_id'],rates_candidate_overrides:['candidate_id','client_id'],timesheets:['timesheet_id','booking_id','occupant_key_norm','hospital_norm','candidate_hint_text'],timesheets_financials:['id','timesheet_id','candidate_id','client_id'],ts_financials_outbox:['id','timesheet_id','reason']}))for(const c of cols)column(t,c);
const routineNames=[...new Set([...metadata.callbackRoutines,...metadata.producerRoutineMetadata].map(r=>r.identity.split('(')[0])
 .concat(JSON.parse(fs.readFileSync(new URL('./reference/audit/FINAL_SOURCE_REVIEW.json',import.meta.url))).distinctCustomDirectRoutines.map(r=>r.name.split('.').at(-1))))].sort();
export const scopes=['clients','settings','mappings','candidates','candidateJobs','rates','overrides','timesheets','financials','outbox','audit'];
export function buildReadInput({stage,phase,identity,actorId,settingsId=null,rateId=null,timesheetId=null}){
 check(STAGES.includes(stage)&&['BEFORE','AFTER','RECONCILE'].includes(phase),'FIXTURE_NATIVE_PHASE_REQUIRED');
 check(uuid(actorId)&&Number.isSafeInteger(identity?.backendPid)&&identity.backendPid>0&&Number.isSafeInteger(identity.databaseOid)&&identity.databaseOid>0
  &&identity.physicalDatabase==='cloudtms_test_clone'&&JSON.stringify(identity.key)==='[20261013,1]'
  &&typeof identity.turnId==='string'&&/^hr_turn_[0-9a-f]{32}$/.test(identity.turnId),'FIXTURE_ORIGINAL_TURN_REQUIRED');
 for(const id of [settingsId,rateId,timesheetId])check(id===null||uuid(id),'FIXTURE_NATIVE_GENERATED_ID_REQUIRED');
 const actorBindingSha256=sha(actorId);
 const actorMatch=column=>`coalesce(encode(pg_catalog.sha256(pg_catalog.convert_to(${column}::text,'UTF8')),'hex')=${lit(actorBindingSha256)},false)`;
 const held=`exists(select 1 from pg_catalog.pg_locks where locktype='advisory' and database=${identity.databaseOid} and pid=${identity.backendPid} and classid=20261013 and objid=1 and objsubid=2 and mode='ExclusiveLock' and granted)`;
 const name=request('S1').name,key=request('S2').key_norm;
 const hash=q=>`encode(pg_catalog.sha256(pg_catalog.convert_to(coalesce((select jsonb_agg(to_jsonb(r) order by to_jsonb(r)::text) from (${q}) r),'[]'::jsonb)::text,'UTF8')),'hex')`;
 const where={clients:`select c.* from public.clients c where c.id=${lit(CLIENT)}::uuid or lower(btrim(c.name))=${lit(name.toLowerCase())}`,
  settings:`select s.* from public.client_settings s where s.client_id=${lit(CLIENT)}::uuid`,
  mappings:`select m.* from public.client_hospitals m where m.id=${lit(MAPPING)}::uuid or m.client_id=${lit(CLIENT)}::uuid or exists(select 1 from pg_catalog.jsonb_array_elements_text(case when pg_catalog.jsonb_typeof(m.hospital_name_norm)='array' then m.hospital_name_norm else '[]'::jsonb end) alias_value where pg_catalog.lower(pg_catalog.btrim(alias_value))=${lit(name.toLowerCase())})`,
  candidates:`select c.* from public.candidates c where c.id=${lit(CANDIDATE)}::uuid or lower(btrim(c.key_norm))=${lit(key)} or lower(btrim(c.display_name))=${lit(request('S2').display_name.toLowerCase())} or coalesce(c.nhsp_hr_name_aliases,'[]'::jsonb) ? ${lit(key)}`,
  candidateJobs:`select j.* from public.candidate_job_titles j where j.candidate_id=${lit(CANDIDATE)}::uuid`,
  rates:`select r.* from public.rates_client_defaults r where r.client_id=${lit(CLIENT)}::uuid`,
  overrides:`select r.* from public.rates_candidate_overrides r where r.candidate_id=${lit(CANDIDATE)}::uuid or r.client_id=${lit(CLIENT)}::uuid`,
  timesheets:`select t.* from public.timesheets t where t.booking_id=${lit(BOOKING)} or t.occupant_key_norm=${lit(key)} or t.hospital_norm=${lit(name.toLowerCase())} or t.candidate_hint_text->>'candidate_id'=${lit(CANDIDATE)}`,
  financials:`select f.* from public.timesheets_financials f where f.candidate_id=${lit(CANDIDATE)}::uuid or f.client_id=${lit(CLIENT)}::uuid or f.timesheet_id in (select timesheet_id from target_timesheets)`,
  outbox:`select o.* from public.ts_financials_outbox o where o.timesheet_id in (select timesheet_id from target_timesheets) or o.timesheet_id in (select timesheet_id from target_financials)${timesheetId?` or o.timesheet_id=${lit(timesheetId)}::uuid`:''}`,
  audit:`select a.* from public.audit_events a where a.object_id_text in (${[CLIENT,CANDIDATE,...(timesheetId?[timesheetId]:[])].map(lit).join(',')}) or a.after_json->>'client_id'=${lit(CLIENT)} or a.after_json->>'candidate_id'=${lit(CANDIDATE)}`};
 const ctes=`target_timesheets as (${where.timesheets}),target_financials as (${where.financials})`;
 const scopeSql=scopes.map(k=>`${lit(k)},jsonb_build_object('count',(select count(*) from (${where[k]}) r),'sha256',${hash(where[k])})`).join(',\n');
 const relationNames=metadata.relations.map(r=>r.name);
 const catalogue=`select jsonb_build_object('relations',(select jsonb_agg(jsonb_build_object('schema',n.nspname,'name',c.relname,'oid',c.oid,'owner',pg_get_userbyid(c.relowner),'acl',c.relacl,'kind',c.relkind,'rls',c.relrowsecurity,'forceRls',c.relforcerowsecurity,
   'columns',(select jsonb_agg(jsonb_build_object('name',a.attname,'type',format_type(a.atttypid,a.atttypmod),'notNull',a.attnotnull,'identity',a.attidentity,'generated',a.attgenerated,'default',pg_get_expr(d.adbin,d.adrelid)) order by a.attnum) from pg_catalog.pg_attribute a left join pg_catalog.pg_attrdef d on d.adrelid=a.attrelid and d.adnum=a.attnum where a.attrelid=c.oid and a.attnum>0 and not a.attisdropped),
   'constraints',(select jsonb_agg(pg_get_constraintdef(x.oid,true) order by x.conname) from pg_catalog.pg_constraint x where x.conrelid=c.oid),
   'triggers',(select jsonb_agg(jsonb_build_object('name',t.tgname,'enabled',t.tgenabled,'definition',pg_get_triggerdef(t.oid,true),'function',t.tgfoid::regprocedure::text) order by t.tgname) from pg_catalog.pg_trigger t where t.tgrelid=c.oid),
   'rules',(select jsonb_agg(pg_get_ruledef(r.oid,true) order by r.rulename) from pg_catalog.pg_rewrite r where r.ev_class=c.oid),
   'policies',(select jsonb_agg(jsonb_build_object('name',p.polname,'command',p.polcmd,'roles',p.polroles,'using',pg_get_expr(p.polqual,p.polrelid),'check',pg_get_expr(p.polwithcheck,p.polrelid)) order by p.polname) from pg_catalog.pg_policy p where p.polrelid=c.oid)) order by n.nspname,c.relname) from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relname in (${relationNames.map(lit).join(',')})),
  'routines',(select jsonb_agg(jsonb_build_object('identity',p.oid::regprocedure::text,'owner',pg_get_userbyid(p.proowner),'acl',p.proacl,'config',p.proconfig,'definer',p.prosecdef,'volatility',p.provolatile,'kind',p.prokind,'definitionSha256',encode(pg_catalog.sha256(pg_catalog.convert_to(pg_get_functiondef(p.oid),'UTF8')),'hex')) order by n.nspname,p.oid::regprocedure::text) from pg_catalog.pg_proc p join pg_catalog.pg_namespace n on n.oid=p.pronamespace where n.nspname in ('public','private') and p.proname in (${routineNames.map(lit).join(',')}))) as metadata`;
 const settingsExpected={...request('S1').client_settings,effective_from:'1900-01-01',client_id:CLIENT};
 const candidateExpected=request('S2'),rateExpected=request('S3');
 const exactOne=(query,predicate)=>`(select count(*)=1 and coalesce(bool_and(${predicate}),false) from (${query}) r)`;
 const schemaFacts={audit:`(select coalesce(bool_and(coalesce((${actorMatch('r.actor_user_id')} and ((r.action='CLIENT_CREATED_WITH_SETTINGS' and r.object_id_text=${lit(CLIENT)}) or (r.action='TIMESHEET_CREATED' and r.after_json->>'client_id'=${lit(CLIENT)} and r.after_json->>'candidate_id'=${lit(CANDIDATE)}))),false)),true) from (${where.audit}) r)`,
  clientMapping:exactOne(where.mappings,`r.id=${lit(MAPPING)}::uuid and r.client_id=${lit(CLIENT)}::uuid and r.hospital_name_norm=${lit(JSON.stringify(request('S1a').hospital_name_norm))}::jsonb and r.ward_hint is null`),
  settings:exactOne(where.settings,`to_jsonb(r) @> ${lit(JSON.stringify(settingsExpected))}::jsonb${settingsId?` and r.id=${lit(settingsId)}::uuid`:''}`),
  candidate:exactOne(where.candidates,`to_jsonb(r) @> ${lit(JSON.stringify(candidateExpected))}::jsonb and r.tms_ref is not null and r.job_title_id is null and coalesce(r.nhsp_hr_name_aliases,'[]'::jsonb)='[]'::jsonb`),
  rate:exactOne(where.rates,`to_jsonb(r) @> ${lit(JSON.stringify(rateExpected))}::jsonb and r.disabled_at_utc is null${rateId?` and r.id=${lit(rateId)}::uuid`:''}`),
  timesheet:exactOne(where.timesheets,`r.booking_id=${lit(BOOKING)} and r.is_current and r.version=1 and r.sheet_scope='DAILY' and r.line_type='HOURS' and r.submission_mode='MANUAL' and r.contract_id is null and not r.is_adjustment and r.authorised_at_server is null and r.parent_timesheet_id is null and r.job_title_norm='rn' and r.band='Band 6' and r.occupant_key_norm=${lit(key)} and r.worked_start_iso='2026-10-15T07:00:00Z'::timestamptz and r.worked_end_iso='2026-10-15T13:00:00Z'::timestamptz and r.break_minutes=0 and r.worked_minutes=360${timesheetId?` and r.timesheet_id=${lit(timesheetId)}::uuid`:''}`),
  financial:exactOne(where.financials,`r.is_current and r.timesheet_version=1 and r.basis='SELF_REPORTED' and r.processing_status='UNPROCESSED' and r.candidate_id=${lit(CANDIDATE)}::uuid and r.client_id=${lit(CLIENT)}::uuid and r.role='RN' and r.band='Band 6' and r.pay_method='PAYE' and not r.has_rate_issue and not r.has_pay_channel_issue and r.locked_by_invoice_id is null and r.paid_at_utc is null and r.authorised_at_utc is null and r.processed_at_utc is null and r.rate_source_refs_json->>'override_id' is null and r.policy_snapshot_json is not null and r.invoice_breakdown_json is not null${rateId?` and r.rate_source_refs_json->>'default_id'=${lit(rateId)}`:''}${timesheetId?` and r.timesheet_id=${lit(timesheetId)}::uuid`:''}`)};
 const rawSql=`set local transaction_read_only=on;
with ${ctes},catalogue as (${catalogue})
select jsonb_build_object('version','DAILY_GENUINE_NATIVE_AGGREGATE_V1','stage',${lit(stage)},'phase',${lit(phase)},
 'database',current_database(),'databaseOid',(select oid::bigint from pg_catalog.pg_database where datname=current_database()),'backendPid',pg_backend_pid(),
 'holderPid',${identity.backendPid},'holderGranted',(${held}),'readOnly',current_setting('transaction_read_only')::boolean,'noRoleSwitch',current_user=session_user,
 'observedAt',clock_timestamp(),'testIdentity',exists(select 1 from private.cloudtms_database_identity where singleton and environment='TEST' and customer_key is null),
 'legacyOne',exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='LEGACY' and owner_epoch=1),
 'applyingCount',(select count(*) from private.cloudtms_database_releases where status='APPLYING'),
 'actorAllowed',exists(select 1 from public.tms_users u where ${actorMatch('u.id')} and u.is_active and u.role='admin'),
 'scope',jsonb_build_object(${scopeSql}),
 'facts',jsonb_build_object(${Object.entries(schemaFacts).map(([k,v])=>`${lit(k)},${v}`).join(',')}),
 'ids',jsonb_build_object('settings',(select jsonb_agg(id order by id) from (${where.settings}) r),'rates',(select jsonb_agg(id order by id) from (${where.rates}) r),
  'timesheets',(select jsonb_agg(timesheet_id order by timesheet_id) from target_timesheets),'financials',(select jsonb_agg(id order by id) from target_financials)),
 'catalogueSha256',encode(pg_catalog.sha256(pg_catalog.convert_to((select metadata::text from catalogue),'UTF8')),'hex')) as fixture_read;`;
 // All real function calls resolve to their exact built-in owners, including variadic JSON/aggregates.
 // COALESCE and EXISTS are PostgreSQL syntax nodes, not callable dependency slots.
 const builtins=['jsonb_build_object','jsonb_agg','to_jsonb','encode','count','bool_and','lower','btrim','pg_get_userbyid','format_type','pg_get_expr','pg_get_constraintdef','pg_get_triggerdef','pg_get_ruledef','pg_get_functiondef','current_database','pg_backend_pid','current_setting','clock_timestamp'];
 const sql=rawSql.replace(new RegExp('(?<![A-Za-z0-9_.])('+builtins.join('|')+')\\(','g'),'pg_catalog.$1(');
 const verification=sql.slice(sql.indexOf('with '));
 const label='HR_DAILY_GENUINE_'+stage.toUpperCase()+'_'+phase+'_'+identity.turnId.slice(-32).toUpperCase();
 return {input:{database:'agency_test',label,sql,verification_sql:verification,continue_on_error:false,max_rows_per_statement:1,statement_timeout_ms:30000,lock_timeout_ms:2000},
  receipt:{version:'DAILY_GENUINE_READ_INPUT_V1',stage,phase,identity,label,sqlSha256:sha(sql),verificationSqlSha256:sha(verification),actorBindingSha256,
   generated:{settingsId,rateId,timesheetId},statements:2,verificationStatements:1,roleRule:'CURRENT_ACTIVE_ADMIN_IMPLIES_WPPP_PER_PUBLISHED_HELPER',native:'NOT_RUN'}};
}
export function readEnvelope(result,receipt,{now=Date.now(),maxAgeMs=30000}={}){
 check(result?.database==='cloudtms_test_clone'&&result.databaseTarget==='agency_test'&&result.label===receipt.label&&result.sqlSha256===receipt.sqlSha256
  &&result.statementCount===2&&result.accepted===true&&result.rolledBack===true&&!result.fatalError,'FIXTURE_NATIVE_ENVELOPE_REFUSED');
 const one=(s,number)=>{
  check(exactStatement(s,number,'SELECT')&&s.rowCount===1&&s.fields.length===1&&s.fields[0]==='fixture_read'
   &&s.rows.length===1&&Object.keys(s.rows[0]).join(',')==='fixture_read','FIXTURE_NATIVE_ONE_ROW_REQUIRED');
  const v=s.rows[0].fixture_read;check(v&&typeof v==='object'&&!Array.isArray(v),'FIXTURE_NATIVE_OBJECT_REQUIRED');return v;
 };
 check(Array.isArray(result.statements)&&result.statements.length===2&&exactStatement(result.statements[0],1,'SET')
  &&result.statements[0].rowCount===null&&result.statements[0].rows.length===0&&result.statements[0].fields.length===0,'FIXTURE_NATIVE_STATEMENT_SEQUENCE_REQUIRED');
 const a=one(result.statements[1],2),fresh=result.rollbackVerification;
 check(fresh?.connectedFresh===true&&fresh.database==='cloudtms_test_clone'&&Array.isArray(fresh.statements)&&fresh.statements.length===1,'FIXTURE_FRESH_VERIFICATION_REQUIRED');
 const b=one(fresh.statements[0],1);
 for(const v of [a,b]){
  check(v.version==='DAILY_GENUINE_NATIVE_AGGREGATE_V1'&&v.stage===receipt.stage&&v.phase===receipt.phase&&v.database==='cloudtms_test_clone'
   &&v.databaseOid===receipt.identity.databaseOid&&v.holderPid===receipt.identity.backendPid&&v.holderGranted===true&&v.readOnly===true&&v.noRoleSwitch===true
   &&Number.isSafeInteger(v.backendPid)&&v.backendPid>0&&v.backendPid!==v.holderPid&&v.testIdentity===true&&v.legacyOne===true&&v.applyingCount===0&&v.actorAllowed===true,'FIXTURE_NATIVE_IDENTITY_REQUIRED');
  const t=Date.parse(v.observedAt);check(Number.isFinite(t)&&t<=now+1000&&now-t<=maxAgeMs,'FIXTURE_FRESH_NATIVE_CLOCK_REQUIRED');
  check(Object.keys(v.scope??{}).sort().join(',')===[...scopes].sort().join(','),'FIXTURE_NATIVE_COMPLETE_SCOPE_REQUIRED');
  for(const r of Object.values(v.scope))check(Number.isSafeInteger(r.count)&&r.count>=0&&/^[0-9a-f]{64}$/.test(r.sha256),'FIXTURE_NATIVE_SCOPE_REQUIRED');
  check(/^[0-9a-f]{64}$/.test(v.catalogueSha256),'FIXTURE_NATIVE_CATALOGUE_REQUIRED');
 }
 for(const k of ['scope','facts','ids','catalogueSha256'])check(JSON.stringify(a[k])===JSON.stringify(b[k]),'FIXTURE_MAIN_FRESH_DISAGREEMENT');
 check(Date.parse(b.observedAt)>=Date.parse(a.observedAt),'FIXTURE_FRESH_CLOCK_ORDER_REQUIRED');
 // A fresh MCP client may reuse the physical B backend. No independent C-after/restoration verdict is claimed here.
 return b;
}
function exactStatement(s,n,command){return s&&Object.keys(s).sort().join(',')==='command,fields,rowCount,rows,rowsTruncated,statement'&&s.statement===n&&s.command===command&&s.rowsTruncated===false&&Array.isArray(s.rows)&&Array.isArray(s.fields);}
