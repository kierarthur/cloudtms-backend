// Opaque actor hash from the genuine original generated pair, never a UUID.
// No fs, transport, auth, helper invocation, native or SQL execution.
import{createHash}from'node:crypto';
const sha=x=>createHash('sha256').update(x).digest('hex'),hex=/^[0-9a-f]{64}$/,uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const check=(v,c)=>{if(!v)throw Error('OBSERVER_ORIGINAL_'+c);},exact=(v,keys)=>v&&typeof v==='object'&&!Array.isArray(v)&&Object.keys(v).sort().join(',')===[...keys].sort().join(',');
const legacyIdentityKeys=['physicalDatabase','databaseOid','backendPid','key','turnId'];
const identityKeys=[...legacyIdentityKeys,'takenAt','task'];
export function validTurnIdentity(i,{legacyFive=false}={}){const seven=exact(i,identityKeys);return (seven||legacyFive&&exact(i,legacyIdentityKeys))&&i.physicalDatabase==='cloudtms_test_clone'&&Number.isSafeInteger(i.databaseOid)&&i.databaseOid>0&&Number.isSafeInteger(i.backendPid)&&i.backendPid>0&&Array.isArray(i.key)&&Object.keys(i.key).join(',')==='0,1'&&JSON.stringify(i.key)==='[20261013,1]'&&typeof i.turnId==='string'&&/^hr_turn_[0-9a-f]{32}$/.test(i.turnId)&&(!seven||typeof i.takenAt==='string'&&Number.isFinite(Date.parse(i.takenAt))&&new Date(i.takenAt).toISOString()===i.takenAt&&typeof i.task==='string'&&i.task.trim().length>0);}
export function actorBindingFromOriginal(original,expected){
 check(arguments.length===2&&exact(original,['input','receipt'])&&exact(expected,['stage','phase','identity']),'CLOSED_PAIR_AND_CONTEXT_REQUIRED');
 const {input,receipt:r}=original,{stage,phase,identity}=expected;
 check(['S1','S2','S3'].includes(stage)&&['BEFORE','AFTER','RECONCILE'].includes(phase)&&validTurnIdentity(identity),'SUPPORTED_STAGE_PHASE_TURN_REQUIRED');
 check(exact(r,['version','stage','phase','identity','label','sqlSha256','verificationSqlSha256','actorBindingSha256','generated','statements','verificationStatements','roleRule','native']),'CLOSED_RECEIPT_REQUIRED');
 check(r.version==='DAILY_GENUINE_READ_INPUT_V1'&&r.stage===stage&&r.phase===phase&&validTurnIdentity(r.identity)&&identityKeys.every(k=>JSON.stringify(r.identity[k])===JSON.stringify(identity[k])),'EXACT_RECEIPT_STAGE_PHASE_TURN_REQUIRED');
 check(typeof r.actorBindingSha256==='string'&&hex.test(r.actorBindingSha256)&&typeof r.sqlSha256==='string'&&hex.test(r.sqlSha256)&&typeof r.verificationSqlSha256==='string'&&hex.test(r.verificationSqlSha256),'HASH_REQUIRED');
 check(r.statements===2&&r.verificationStatements===1&&r.roleRule==='CURRENT_ACTIVE_ADMIN_IMPLIES_WPPP_PER_PUBLISHED_HELPER'&&r.native==='NOT_RUN'&&exact(r.generated,['settingsId','rateId','timesheetId'])&&Object.values(r.generated).every(v=>v===null||typeof v==='string'&&uuid.test(v)),'ORIGINAL_RECEIPT_CONTRACT_REQUIRED');
 check(exact(input,['database','label','sql','verification_sql','continue_on_error','max_rows_per_statement','statement_timeout_ms','lock_timeout_ms']),'CLOSED_INPUT_REQUIRED');
 const label='HR_DAILY_GENUINE_'+stage+'_'+phase+'_'+identity.turnId.slice(-32).toUpperCase();
 check(input.database==='agency_test'&&input.label===label&&r.label===label&&input.continue_on_error===false&&input.max_rows_per_statement===1&&input.statement_timeout_ms===30000&&input.lock_timeout_ms===2000,'ORIGINAL_INPUT_LIMITS_LABEL_REQUIRED');
 check(typeof input.sql==='string'&&typeof input.verification_sql==='string'&&Buffer.byteLength(JSON.stringify(input),'utf8')<=98304&&input.verification_sql.startsWith('with ')&&input.verification_sql.endsWith(' as fixture_read;')&&input.verification_sql.split(';').length===2&&input.sql==='set local transaction_read_only=on;\n'+input.verification_sql&&sha(input.sql)===r.sqlSha256&&sha(input.verification_sql)===r.verificationSqlSha256,'EXACT_ORIGINAL_SQL_HASH_REQUIRED');
 const actor=`'actorAllowed',exists(select 1 from public.tms_users u where coalesce(pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(u.id::text,'UTF8')),'hex')='${r.actorBindingSha256}',false) and u.is_active and u.role='admin')`;
 const holder=`'holderPid',${identity.backendPid},'holderGranted',(exists(select 1 from pg_catalog.pg_locks where locktype='advisory' and database=${identity.databaseOid} and pid=${identity.backendPid} and classid=20261013 and objid=1 and objsubid=2 and mode='ExclusiveLock' and granted))`;
 for(const literal of[actor,holder,`'stage','${stage}','phase','${phase}',`])check(input.verification_sql.split(literal).length===2,'ACTOR_AND_CONTEXT_SQL_BINDING_REQUIRED');
 check(input.verification_sql.split(r.actorBindingSha256).length===3,'BOTH_ORIGINAL_ACTOR_HASHES_REQUIRED');
 return r.actorBindingSha256;
}
