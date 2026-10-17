// Explicit compact owning-C projection. Pure source construction, no proof authority.
import {createHash} from 'node:crypto';
import {compileNativeBatch} from './batch-source.mjs';
const sha=s=>createHash('sha256').update(s).digest('hex');
const check=(v,c)=>{if(!v)throw Error('LOCAL_METADATA_CLAIM_'+c);};
export const CLAIM_MODE='CONTACT_OWNING_C_COMPACT_ROTA_ELIGIBILITY_V1';
export const ORIGINAL_BOUND_FIELDS_SHA256='fad1de9921b810536fd51b564655e047d060e965aa3f75b159ae1b3524354961';
export function copyNativeClaimCut(snapshot){
 check(snapshot?.backgroundSnapshotEpochZero===true&&typeof snapshot.backgroundSnapshotXmax==='string'&&/^[1-9][0-9]*$/.test(snapshot.backgroundSnapshotXmax)
  &&BigInt(snapshot.backgroundSnapshotXmax)<4294967296n&&Array.isArray(snapshot.backgroundSnapshotXip)
  &&snapshot.backgroundSnapshotXip.every(x=>typeof x==='string'&&/^[1-9][0-9]*$/.test(x)&&BigInt(x)<BigInt(snapshot.backgroundSnapshotXmax)),'NATIVE_CUT_REQUIRED');
 const cut=Object.freeze({xmax:snapshot.backgroundSnapshotXmax,xip:Object.freeze([...snapshot.backgroundSnapshotXip])});
 check(Buffer.byteLength(JSON.stringify(cut))<=16384,'CUT_CONTROL_BOUND');return cut;
}
export function eligibleClaimFields(fields,phase,cut){
 check(typeof fields==='string'&&sha(fields)===ORIGINAL_BOUND_FIELDS_SHA256,'ORIGINAL_COMPLETE_FIELDS_REQUIRED');
 check(phase==='BEFORE'||phase==='AFTER','PHASE_REQUIRED');
 const start=fields.indexOf("jsonb_build_object('keyHash',"),tail=",'createdAt',r.created_at_utc,'updatedAt',r.updated_at_utc,'completedAt',r.completed_at_utc)",end=fields.indexOf(tail,start)+tail.length;
 check(start>=0&&fields.split(tail).length===2&&end>start,'SINGLE_ORIGINAL_BATCH_ROW_REQUIRED');
 const original=fields.slice(start,end),flag=original.indexOf("'nativeClaimProved',"),flagValue=flag+"'nativeClaimProved',".length,time=original.indexOf(",'createdAt'",flagValue);
 const originalCoalesce=original.slice(flagValue,time);check(originalCoalesce.startsWith('coalesce((')&&originalCoalesce.endsWith('),false)'),'COMPLETE_COALESCE_REQUIRED');
 const beforeBase=original.slice(0,original.indexOf(",'xmin'"))+')',afterBase=original.slice(0,flag).replace(/,\s*$/,')'),extras='jsonb_build_object('+original.slice(flag);
 let projected=beforeBase;
 if(phase==='AFTER'){
  check(cut&&Object.keys(cut).sort().join('|')==='xip|xmax'&&Object.isFrozen(cut)&&Object.isFrozen(cut.xip),'PRIVATE_COPIED_CUT_REQUIRED');
  copyNativeClaimCut({backgroundSnapshotEpochZero:true,backgroundSnapshotXmax:cut.xmax,backgroundSnapshotXip:cut.xip});
  const active=cut.xip.length?'array['+cut.xip.map(x=>"'"+x+"'").join(',')+']::text[]':'array[]::text[]';
  const eligible="(r.xmin::text::numeric >= "+cut.xmax+"::numeric or r.xmin::text = any("+active+"))";
  projected='('+afterBase+' || case when '+eligible+' then '+extras+" else '{}'::jsonb end)";
 }else check(cut===undefined,'BEFORE_HAS_NO_CUT');
 return{fields:fields.slice(0,start)+projected+fields.slice(end),original,originalCoalesce,projected};
}
export function assertProjectedClaimFlags(snapshot,phase,cut){
 const rows=snapshot?.backgroundRows?.['private.candidate_daily_batch_receipts'];check(Array.isArray(rows),'COMPLETE_BATCH_MAP_REQUIRED');
 for(const row of rows){
  const eligible=phase==='AFTER'&&/^[1-9][0-9]*$/.test(row.xmin??'')&&(BigInt(row.xmin)>=BigInt(cut.xmax)||cut.xip.includes(row.xmin));
  const keys=phase==='BEFORE'?['keyHash','rowHash']:['keyHash','rowHash','xmin','xactStatus',...eligible?['nativeClaimProved','createdAt','updatedAt','completedAt']:[]];
  check(Object.keys(row).sort().join('|')===keys.sort().join('|'),'COMPACT_ROW_SHAPE_REQUIRED');
  if(eligible)check(typeof row.nativeClaimProved==='boolean','DERIVED_FLAG_BOOLEAN_REQUIRED');
 }
}
export function compileEligibleClaimBatch({referenceInput,referenceCanonicalSql,backgroundFields,managed,phase,cut}){
 check(referenceInput.split(referenceCanonicalSql).length===2,'REFERENCE_CANONICAL_REQUIRED');
 const projection=eligibleClaimFields(backgroundFields,phase,cut);
 check(referenceCanonicalSql.split(backgroundFields).length===2,'COMPLETE_FIELDS_EMBED_REQUIRED');
 const canonicalSql=referenceCanonicalSql.replace(backgroundFields,()=>projection.fields),input=referenceInput.replace(referenceCanonicalSql,()=>canonicalSql);
 const built=compileNativeBatch({input,canonicalSql},managed,projection.fields);
 return{...built,projection:{mode:CLAIM_MODE,phase,referenceCanonicalSqlSha256:sha(referenceCanonicalSql),projectionCanonicalSqlSha256:sha(canonicalSql),
  originalCompleteCoalesceSha256:sha(projection.originalCoalesce),originalCompleteBatchRowSha256:sha(projection.original),boundFieldsSha256:sha(backgroundFields),cutSha256:cut?sha(JSON.stringify(cut)):null,
  beforeRotaFields:['keyHash','rowHash'],afterAllRotaFields:['keyHash','rowHash','xmin','xactStatus'],afterEligibleAdditionalFields:['nativeClaimProved','createdAt','updatedAt','completedAt'],
  outboxMapsUnchanged:true,allRowHashesCountsKeysRetained:true,rawCanonicalEqualityClaimed:false,sqlErrorDomainEquivalent:false}};
}
