// Fixed S2/S3 auxiliary observation only. No SQL client, API or execution.
import {createHash} from 'node:crypto';
import {actorBindingFromOriginal} from './original-stage-receipt.mjs';
export const CLIENT='f717d043-0000-4000-8000-000000000010';
export const CANDIDATE='f717d043-0000-4000-8000-000000000030';
export const KEY='daily-health-basis-20261017-candidate';
export const RELATIONS=['public.candidates','public.candidate_job_titles','public.rates_client_defaults','public.rates_candidate_overrides','public.timesheets','public.timesheets_financials','public.ts_financials_outbox','public.clients','public.app_change_counters','public.banking_pay_scope_change_transactions'];
export const SEQUENCES=['public.candidate_tmsref_seq','public.global_rev_seq','private.invoice_generate_candidate_change_seq','private.invoice_issue_candidate_change_seq'];
export const SCOPES=['candidatesCounter','counterTransaction'];
const sha=x=>createHash('sha256').update(x).digest('hex');
const lit=x=>"'"+String(x).replaceAll("'","''")+"'";
const fail=(v,c)=>{if(!v)throw Error('S23_OBSERVER_'+c);};
const exact=(v,keys)=>v&&typeof v==='object'&&!Array.isArray(v)&&Object.keys(v).sort().join(',')===[...keys].sort().join(',');
const UUID=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const HEX=/^[0-9a-f]{64}$/;
const INT63=/^(0|[1-9][0-9]{0,18})$/;
const bigint=x=>typeof x==='string'&&INT63.test(x)&&BigInt(x)<=9223372036854775807n;
const hashSql=q=>`pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(coalesce((select pg_catalog.string_agg(s.row_hash,'|' order by s.row_hash) from (select pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.to_jsonb(r)::text,'UTF8')),'hex') as row_hash from (${q}) r) s),''),'UTF8')),'hex')`;
export const scopedSql={
 candidatesCounter:`select c.* from public.app_change_counters c where c.entity_key='candidates'`,
 counterTransaction:`select t.* from public.banking_pay_scope_change_transactions t where exists(select 1 from public.app_change_counters c where c.entity_key='candidates' and c.scope_change_tx_token=t.tx_token)`
};
const relationValues=RELATIONS.map(x=>`(${lit(x)},${lit(x)}::regclass)`).join(',');
// No function body is returned. Trigger/default/rule/policy text is hashed.
const metadata=`with selected_relations as (select v.identity,v.relid from (values ${relationValues}) v(identity,relid)),
 selected_sequences as (select v.identity,v.relid from (values ${SEQUENCES.map(x=>`(${lit(x)},${lit(x)}::regclass)`).join(',')}) v(identity,relid)),
 trigger_functions as (select distinct t.tgfoid from pg_catalog.pg_trigger t join selected_relations s on s.relid=t.tgrelid where not t.tgisinternal),
 relation_rows as (select s.identity,c.relkind,c.relrowsecurity,c.relforcerowsecurity,pg_catalog.pg_get_userbyid(c.relowner) as owner,c.relacl,
  (select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('name',a.attname,'type',pg_catalog.format_type(a.atttypid,a.atttypmod),'notNull',a.attnotnull,'identity',a.attidentity,'generated',a.attgenerated,'defaultSha256',case when d.oid is null then null else pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.pg_get_expr(d.adbin,d.adrelid),'UTF8')),'hex') end) order by a.attnum) from pg_catalog.pg_attribute a left join pg_catalog.pg_attrdef d on d.adrelid=a.attrelid and d.adnum=a.attnum where a.attrelid=c.oid and a.attnum>0 and not a.attisdropped) as columns,
  (select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('name',k.conname,'kind',k.contype,'deferred',k.condeferred,'deferrable',k.condeferrable,'validated',k.convalidated,'target',case when k.confrelid=0 then null else k.confrelid::regclass::text end,'sha256',pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.pg_get_constraintdef(k.oid,true),'UTF8')),'hex')) order by k.conname) from pg_catalog.pg_constraint k where k.conrelid=c.oid) as constraints,
  (select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('name',t.tgname,'enabled',t.tgenabled,'internal',t.tgisinternal,'routine',t.tgfoid::regprocedure::text,'sha256',pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.pg_get_triggerdef(t.oid,true),'UTF8')),'hex')) order by t.tgname) from pg_catalog.pg_trigger t where t.tgrelid=c.oid) as triggers,
  (select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('name',r.rulename,'sha256',pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.pg_get_ruledef(r.oid,true),'UTF8')),'hex')) order by r.rulename) from pg_catalog.pg_rewrite r where r.ev_class=c.oid) as rules,
  (select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('name',p.polname,'command',p.polcmd,'roles',p.polroles,'usingSha256',case when p.polqual is null then null else pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.pg_get_expr(p.polqual,p.polrelid),'UTF8')),'hex') end,'checkSha256',case when p.polwithcheck is null then null else pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.pg_get_expr(p.polwithcheck,p.polrelid),'UTF8')),'hex') end) order by p.polname) from pg_catalog.pg_policy p where p.polrelid=c.oid) as policies,
  (select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('role',role_name,'select',pg_catalog.has_table_privilege(role_name,c.oid,'SELECT'),'insert',pg_catalog.has_table_privilege(role_name,c.oid,'INSERT'),'update',pg_catalog.has_table_privilege(role_name,c.oid,'UPDATE'),'delete',pg_catalog.has_table_privilege(role_name,c.oid,'DELETE')) order by role_name) from (values (current_user::text),('service_role'),('anon'),('authenticated')) role_values(role_name)) as effective_privileges
 from selected_relations s join pg_catalog.pg_class c on c.oid=s.relid)
select pg_catalog.jsonb_build_object('relations',(select pg_catalog.jsonb_agg(pg_catalog.to_jsonb(r) order by r.identity) from relation_rows r),
 'sequences',(select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('identity',s.identity,'owner',pg_catalog.pg_get_userbyid(c.relowner),'acl',c.relacl,'type',pg_catalog.format_type(q.seqtypid,null),'increment',q.seqincrement::text,'minimum',q.seqmin::text,'maximum',q.seqmax::text,'cache',q.seqcache::text,'cycle',q.seqcycle,'ownerSelect',pg_catalog.has_sequence_privilege(current_user,c.oid,'SELECT'),'serviceSelect',pg_catalog.has_sequence_privilege('service_role',c.oid,'SELECT'),'serviceUsage',pg_catalog.has_sequence_privilege('service_role',c.oid,'USAGE'),'serviceUpdate',pg_catalog.has_sequence_privilege('service_role',c.oid,'UPDATE')) order by s.identity) from selected_sequences s join pg_catalog.pg_class c on c.oid=s.relid join pg_catalog.pg_sequence q on q.seqrelid=c.oid),
 'triggerRoutines',(select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('identity',p.oid::regprocedure::text,'schema',n.nspname,'name',p.proname,'owner',pg_catalog.pg_get_userbyid(p.proowner),'acl',p.proacl,'definer',p.prosecdef,'kind',p.prokind,'volatility',p.provolatile,'config',p.proconfig,'definitionSha256',pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.pg_get_functiondef(p.oid),'UTF8')),'hex'),'ownerExecute',pg_catalog.has_function_privilege(current_user,p.oid,'EXECUTE'),'serviceExecute',pg_catalog.has_function_privilege('service_role',p.oid,'EXECUTE')) order by n.nspname,p.oid::regprocedure::text) from trigger_functions f join pg_catalog.pg_proc p on p.oid=f.tgfoid join pg_catalog.pg_namespace n on n.oid=p.pronamespace)) as observation`;
const effects=`select pg_catalog.jsonb_build_object('scope',pg_catalog.jsonb_build_object(${SCOPES.map(k=>`${lit(k)},pg_catalog.jsonb_build_object('count',(select pg_catalog.count(*)::text from (${scopedSql[k]}) r),'sha256',${hashSql(scopedSql[k])})`).join(',')}),
 'sequenceState',pg_catalog.jsonb_build_object(${SEQUENCES.map(s=>`${lit(s)},(select pg_catalog.jsonb_build_object('lastValue',last_value::text,'isCalled',is_called) from ${s})`).join(',')}),
 'counterSeq',(select c.seq::text from public.app_change_counters c where c.entity_key='candidates')) as observation`;
function buildBound(mode,{identity,actorId,phase,stage},actorHash=null){
 fail(['METADATA','EFFECTS'].includes(mode),'FIXED_MODE_REQUIRED');
 fail(['S2','S3'].includes(stage),'FIXED_STAGE_REQUIRED');
 fail(['BEFORE','AFTER','RECONCILE'].includes(phase),'FIXED_PHASE_REQUIRED');
 fail(exact(identity,['physicalDatabase','databaseOid','backendPid','key','turnId'])&&identity.physicalDatabase==='cloudtms_test_clone'&&Number.isSafeInteger(identity.databaseOid)&&identity.databaseOid>0&&Number.isSafeInteger(identity.backendPid)&&identity.backendPid>0&&JSON.stringify(identity.key)==='[20261013,1]'&&/^hr_turn_[0-9a-f]{32}$/.test(identity.turnId),'ORIGINAL_TURN_REQUIRED');
 if(actorHash===null)fail(typeof actorId==='string'&&UUID.test(actorId),'ACTOR_REQUIRED');
 else fail(typeof actorHash==='string'&&HEX.test(actorHash),'ACTOR_BINDING_REQUIRED');
 const actorBindingSha256=actorHash===null?sha(actorId):actorHash,turnBindingSha256=sha(identity.turnId),q=mode==='METADATA'?metadata:effects;
 const select=`with observed as (${q}) select pg_catalog.jsonb_build_object('version','S23_SCOPED_EFFECT_OBSERVER_V1','stage',${lit(stage)},'mode',${lit(mode)},'phase',${lit(phase)},'turnBindingSha256',${lit(turnBindingSha256)},'database',pg_catalog.current_database(),'databaseOid',(select oid from pg_catalog.pg_database where datname=pg_catalog.current_database()),'backendPid',pg_catalog.pg_backend_pid(),'holderPid',${identity.backendPid},'holderGranted',exists(select 1 from pg_catalog.pg_locks where locktype='advisory' and database=${identity.databaseOid} and pid=${identity.backendPid} and classid=20261013 and objid=1 and objsubid=2 and mode='ExclusiveLock' and granted),'readOnly',pg_catalog.current_setting('transaction_read_only')='on','noRoleSwitch',pg_catalog.current_setting('role')='none' and current_user=session_user,'observedAt',pg_catalog.clock_timestamp(),'testIdentity',exists(select 1 from private.cloudtms_database_identity where singleton and environment='TEST' and customer_key is null),'legacyOne',exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='LEGACY' and owner_epoch=1),'applyingCount',(select pg_catalog.count(*)::text from private.cloudtms_database_releases where status='APPLYING'),'actorAllowed',exists(select 1 from public.tms_users u where pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(u.id::text,'UTF8')),'hex')=${lit(actorBindingSha256)} and u.is_active and u.role='admin'),'observation',(select observation from observed)) as s23_observer;`;
 const sql='SET LOCAL transaction_read_only=on;\n'+select;
 fail(Buffer.byteLength(sql,'utf8')<32768,'FIXED_INPUT_BOUND');
 return {input:{database:'agency_test',label:'HR_DAILY_S23_'+stage+'_'+mode+'_'+phase+'_'+identity.turnId.slice(-32),sql,verification_sql:select,continue_on_error:false,max_rows_per_statement:1,statement_timeout_ms:30000,lock_timeout_ms:2000},receipt:{version:'S23_SCOPED_OBSERVER_INPUT_V1',stage,mode,phase,identity,actorBindingSha256,turnBindingSha256,sqlSha256:sha(sql),verificationSqlSha256:sha(select),native:'NOT_RUN',restoration:'NOT_A_RESTORATION_PROGRAMME'}};
}
export function build(mode,options){return buildBound(mode,options);}
export function buildFromReceipt(mode,request){
 fail(arguments.length===2&&exact(request,['original','stage','phase','identity'])&&['S2','S3'].includes(request.stage),'CLOSED_ORIGINAL_REQUEST_REQUIRED');
 const {original,stage,phase,identity}=request,actorHash=actorBindingFromOriginal(original,{stage,phase,identity});
 return buildBound(mode,{identity,phase,stage},actorHash);
}
export function validateFrame(v,receipt,{now=Date.now(),maxAgeMs=30000}={}){
 fail(exact(v,['version','stage','mode','phase','turnBindingSha256','database','databaseOid','backendPid','holderPid','holderGranted','readOnly','noRoleSwitch','observedAt','testIdentity','legacyOne','applyingCount','actorAllowed','observation']),'CLOSED_FRAME_REQUIRED');
 fail(v.version==='S23_SCOPED_EFFECT_OBSERVER_V1'&&v.stage===receipt.stage&&v.mode===receipt.mode&&v.phase===receipt.phase&&v.turnBindingSha256===receipt.turnBindingSha256&&v.database==='cloudtms_test_clone'&&v.databaseOid===receipt.identity.databaseOid&&v.holderPid===receipt.identity.backendPid&&Number.isSafeInteger(v.backendPid)&&v.backendPid>0&&v.backendPid!==v.holderPid&&v.holderGranted===true&&v.readOnly===true&&v.noRoleSwitch===true&&v.testIdentity===true&&v.legacyOne===true&&v.applyingCount==='0'&&v.actorAllowed===true,'NATIVE_GUARDS_REQUIRED');
 const t=Date.parse(v.observedAt);fail(Number.isFinite(t)&&t<=now+1000&&now-t<=maxAgeMs,'FRESH_CLOCK_REQUIRED');
 const o=v.observation;
 if(receipt.mode==='EFFECTS'){
  fail(exact(o,['scope','sequenceState','counterSeq'])&&exact(o.scope,SCOPES)&&exact(o.sequenceState,SEQUENCES),'CLOSED_EFFECTS_REQUIRED');
  for(const x of Object.values(o.scope))fail(exact(x,['count','sha256'])&&bigint(x.count)&&HEX.test(x.sha256),'NATIVE_COUNT_HASH_REQUIRED');
  for(const x of Object.values(o.sequenceState))fail(exact(x,['lastValue','isCalled'])&&bigint(x.lastValue)&&typeof x.isCalled==='boolean','NATIVE_SEQUENCE_REQUIRED');
  fail(o.counterSeq===null||bigint(o.counterSeq),'NATIVE_COUNTER_REQUIRED');
 }else{
  fail(exact(o,['relations','sequences','triggerRoutines'])&&Array.isArray(o.relations)&&Array.isArray(o.sequences)&&Array.isArray(o.triggerRoutines),'CLOSED_METADATA_REQUIRED');
  fail(o.relations.length===RELATIONS.length&&o.sequences.length===SEQUENCES.length&&o.relations.map(r=>r.identity).sort().join(',')===[...RELATIONS].sort().join(',')&&o.sequences.map(r=>r.identity).sort().join(',')===[...SEQUENCES].sort().join(','),'COMPLETE_METADATA_REQUIRED');
  // Metadata is evidence for root review, never automatic callback/effect admission.
 }
 return v;
}
