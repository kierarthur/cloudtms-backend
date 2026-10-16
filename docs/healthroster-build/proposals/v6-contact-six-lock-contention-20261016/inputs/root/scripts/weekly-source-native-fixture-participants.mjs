// Mount at scripts/weekly-source-native-fixture-participants.mjs.
// Pure committed-plan preparation. SQL independently proves current release,
// registration, vacancy, actor, owned mutex/turn and the real native creations.
import {domainHash,exact} from './weekly-source-protected-release-qualification.mjs';
export const nativeParticipantSchema='WEEKLY_SOURCE_NATIVE_FIXTURE_PARTICIPANTS_V1';
export const nativeSourceSchema='WEEKLY_SOURCE_NATIVE_FIXTURE_SOURCE_METADATA_V1';
const fail=code=>{throw Error(code);};
const uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const hex=/^[0-9a-f]{64}$/;
export function buildNativeFixtureParticipants(context,input){
 exact(input,['schema_version','authorization_id','purpose','setup_actor_user_id','expires_at_utc','participants','reason'],'NATIVE_FIXTURE_INPUT_INVALID');
 if(input.schema_version!==nativeParticipantSchema||input.purpose!=='NATIVE_FIXTURE_PARTICIPANTS'
  ||!uuid.test(input.authorization_id)||!uuid.test(input.setup_actor_user_id)
  ||typeof input.reason!=='string'||input.reason.length<1||input.reason.length>1000
  ||typeof input.expires_at_utc!=='string'||!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})$/.test(input.expires_at_utc)
  ||!Number.isFinite(Date.parse(input.expires_at_utc)))fail('NATIVE_FIXTURE_INPUT_INVALID');
 exact(input.participants,['clients','candidates'],'NATIVE_FIXTURE_PARTICIPANTS_INVALID');
 const all=[];
 for(const [key,keys,max]of [['clients',['id','name'],4],['candidates',['id','first_name','last_name'],16]]){
  const rows=input.participants[key];
  if(!Array.isArray(rows)||rows.length<1||rows.length>max)fail('NATIVE_FIXTURE_PARTICIPANTS_INVALID');
  for(const row of rows){
   exact(row,keys,'NATIVE_FIXTURE_PARTICIPANTS_INVALID');
   if(!uuid.test(row.id)||keys.filter(k=>k!=='id').some(k=>typeof row[k]!=='string'||row[k].length<1||row[k].length>(key==='clients'?160:80)))
    fail('NATIVE_FIXTURE_PARTICIPANTS_INVALID');
   all.push(row.id);
  }
  if(JSON.stringify(rows.map(r=>r.id))!==JSON.stringify([...new Set(rows.map(r=>r.id))].sort()))fail('NATIVE_FIXTURE_PARTICIPANTS_INVALID');
 }
 if(new Set(all).size!==all.length)fail('NATIVE_FIXTURE_PARTICIPANTS_INVALID');
 if(context.environment!=='TEST'||context.expectedDatabase!=='cloudtms_test_clone'||context.mode!=='UPGRADE'
  ||typeof context.releaseId!=='string'||!context.releaseId||!/^[0-9a-f]{40}$/.test(context.gitCommit)||!hex.test(context.expectedHash))
  fail('NATIVE_FIXTURE_POST_VERIFIED_CONTEXT_REQUIRED');
 const body={...input,release_id:context.releaseId,source_commit:context.gitCommit,contract_sha256:context.expectedHash};
 return {...body,authorization_sha256:domainHash(nativeParticipantSchema,body)};
}
const literal=value=>"'"+String(value).replaceAll("'","''")+"'";
// Existing protected root runner supplies its owner session; no connection,
// lock acquisition, BEGIN/COMMIT, side effect or scheduler control is hidden.
export async function createNativeFixtureParticipants({session,actorUserId,authorizationId,turnBackendPid}){
 if(typeof session?.execute!=='function'||!uuid.test(actorUserId)||!uuid.test(authorizationId)
  ||!Number.isSafeInteger(turnBackendPid)||turnBackendPid<1)fail('NATIVE_FIXTURE_OWNER_SESSION_REQUIRED');
 return JSON.parse(await session.execute('select private.weekly_source_native_fixture_participants_create_v1('
  +literal(actorUserId)+'::uuid,'+literal(authorizationId)+'::uuid,'+turnBackendPid+'::integer)::text;'));
}
export function buildNativeFixtureSourceMetadata(context,input){
 exact(input,['schema_version','authorization_id','purpose','setup_actor_user_id','expires_at_utc','metadata','reason'],'NATIVE_FIXTURE_SOURCE_INVALID');
 if(input.schema_version!==nativeSourceSchema||input.purpose!=='NATIVE_FIXTURE_SOURCE_METADATA'||!uuid.test(input.authorization_id)
  ||!uuid.test(input.setup_actor_user_id)||typeof input.reason!=='string'||input.reason.length<1||input.reason.length>1000
  ||typeof input.expires_at_utc!=='string'||!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})$/.test(input.expires_at_utc)
  ||!Number.isFinite(Date.parse(input.expires_at_utc)))fail('NATIVE_FIXTURE_SOURCE_INVALID');
 const m=input.metadata;
 exact(m,['agency_id','source_group_id','source_cycle_id','membership_id','policy_id','client_id','group_code','display_name',
  'week_ending','cutoff_weekday','cutoff_local_time','contract_refs'],'NATIVE_FIXTURE_SOURCE_INVALID');
 if(['agency_id','source_group_id','source_cycle_id','membership_id','policy_id','client_id'].some(k=>!uuid.test(m[k]))
  ||new Set(['source_group_id','source_cycle_id','membership_id','policy_id'].map(k=>m[k])).size!==4
  ||!/^WSQ_TEST_[A-Z0-9_]{1,71}$/.test(m.group_code)||typeof m.display_name!=='string'||m.display_name.length<1||m.display_name.length>160
  ||typeof m.week_ending!=='string'||!/^\d{4}-\d{2}-\d{2}$/.test(m.week_ending)||!Number.isFinite(Date.parse(m.week_ending+'T00:00:00Z'))
  ||new Date(m.week_ending+'T00:00:00Z').toISOString().slice(0,10)!==m.week_ending||new Date(m.week_ending+'T00:00:00Z').getUTCDay()!==0
  ||typeof m.cutoff_weekday!=='string'||!/^[0-6]$/.test(m.cutoff_weekday)||typeof m.cutoff_local_time!=='string'||!/^([01][0-9]|2[0-3]):[0-5][0-9]$/.test(m.cutoff_local_time)
  ||!Array.isArray(m.contract_refs)||m.contract_refs.length<1||m.contract_refs.length>16)fail('NATIVE_FIXTURE_SOURCE_INVALID');
 for(const ref of m.contract_refs){exact(ref,['contract_id','row_sha256'],'NATIVE_FIXTURE_SOURCE_INVALID');
  if(!uuid.test(ref.contract_id)||!hex.test(ref.row_sha256))fail('NATIVE_FIXTURE_SOURCE_INVALID');}
 if(JSON.stringify(m.contract_refs.map(r=>r.contract_id))!==JSON.stringify([...new Set(m.contract_refs.map(r=>r.contract_id))].sort()))fail('NATIVE_FIXTURE_SOURCE_INVALID');
 if(context.environment!=='TEST'||context.expectedDatabase!=='cloudtms_test_clone'||context.mode!=='UPGRADE'
  ||typeof context.releaseId!=='string'||!context.releaseId||!/^[0-9a-f]{40}$/.test(context.gitCommit)||!hex.test(context.expectedHash))
  fail('NATIVE_FIXTURE_POST_VERIFIED_CONTEXT_REQUIRED');
 const body={...input,release_id:context.releaseId,source_commit:context.gitCommit,contract_sha256:context.expectedHash};
 return {...body,authorization_sha256:domainHash(nativeSourceSchema,body)};
}
export async function createNativeFixtureSourceMetadata({session,actorUserId,authorizationId,turnBackendPid}){
 if(typeof session?.execute!=='function'||!uuid.test(actorUserId)||!uuid.test(authorizationId)
  ||!Number.isSafeInteger(turnBackendPid)||turnBackendPid<1)fail('NATIVE_FIXTURE_OWNER_SESSION_REQUIRED');
 return JSON.parse(await session.execute('select private.weekly_source_native_fixture_source_create_v1('
  +literal(actorUserId)+'::uuid,'+literal(authorizationId)+'::uuid,'+turnBackendPid+'::integer)::text;'));
}
