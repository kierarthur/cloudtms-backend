// Owner ruling 14 October: read-only attribution of live TEST background writes.
// This module neither connects nor pauses a writer. Raw rows/credentials never leave SQL.
import {digest,literal} from './cloudtms-managed-release-sql.mjs';
export const backgroundRelations=Object.freeze(['private.candidate_daily_batch_receipts','public.ts_financials_outbox']);
const validHash=h=>typeof h==='string'&&/^[a-f0-9]{64}$/.test(h);
const requireThat=(ok,code)=>{if(!ok)throw Error(code);};
// Only this actual validator can mint a qualification. A JSON status, copied
// receipt or stale qualification cannot admit changed fingerprints.
const qualifiedFrames=new WeakMap();
const snapshotBinding=value=>{const {liveRestoration,...snapshot}=value;return digest(JSON.stringify(snapshot));};
export function assertQualifiedLiveRestoration(before,after,proof){
 const frame=qualifiedFrames.get(proof);
 requireThat(frame&&frame.before===snapshotBinding(before)&&frame.after===snapshotBinding(after)
  &&frame.proof===digest(JSON.stringify(proof)),'REHEARSAL_LIVE_RESTORATION_UNBRANDED_OR_STALE');
 return true;
}
const hash=sql=>`encode(pg_catalog.sha256(pg_catalog.convert_to((${sql})::text,'UTF8')),'hex')`;
const key=relation=>relation===backgroundRelations[0]?'batch_receipt_id':'id';
export function backgroundSnapshotFieldsSql(){
 const batch=backgroundRelations[0],outbox=backgroundRelations[1];
 // Native claim provenance: the completed native receipt, item identity,
 // request/response hashes and lease interval all agree with the actual owner.
 const proof=`r.environment='TEST' and r.actor_class='SIGNED_SYSTEM' and r.operation_class='PROJECTION_CLAIM'
  and r.claim_target='MASTER_AVAILABILITY_SHEET' and r.claim_limit between 1 and 100
  and r.claim_request_id is not null and r.item_count=1
  and r.item_keys_json=jsonb_build_array('claim:'||r.claim_request_id::text)
  and r.state='COMPLETED' and r.terminal_http_status=200
  and r.terminal_response_body->>'batch_receipt_id'=r.batch_receipt_id::text
  and r.terminal_response_body->>'claim_request_id'=r.claim_request_id::text
  and r.terminal_response_body->'items'=r.claimed_items_json
  and (case when pg_input_is_valid(r.terminal_response_body->>'lease_set_expires_at','timestamptz')
    then (r.terminal_response_body->>'lease_set_expires_at')::timestamptz end)=r.lease_expires_at_utc
  and extract(epoch from r.lease_expires_at_utc-r.created_at_utc) between 30 and 600
  and extract(epoch from r.lease_expires_at_utc-r.created_at_utc)=trunc(extract(epoch from r.lease_expires_at_utc-r.created_at_utc))
  and r.request_hash=private._candidate_daily_json_sha256_v1(jsonb_build_object('operation','PROJECTION_CLAIM',
    'claim_request_id',r.claim_request_id,'target',r.claim_target,'claimant',r.lease_owner,
    'max_items',r.claim_limit,'lease_seconds',extract(epoch from r.lease_expires_at_utc-r.created_at_utc)::integer))
  and r.terminal_response_sha256=private._candidate_daily_json_sha256_v1(r.terminal_response_body)`;
 const batches=`select coalesce(jsonb_agg(jsonb_build_object('keyHash',${hash('r.batch_receipt_id')},
   'rowHash',${hash('to_jsonb(r)')},'xmin',r.xmin::text,'xactStatus',case when pg_snapshot_xmax(pg_current_snapshot())::text::numeric<4294967296 then pg_xact_status(r.xmin::text::xid8) end,
   'nativeClaimProved',coalesce((${proof}),false),'createdAt',r.created_at_utc,'updatedAt',r.updated_at_utc,'completedAt',r.completed_at_utc)
   order by r.batch_receipt_id),'[]'::jsonb) from ${batch} r`;
 const outboxes=`select coalesce(jsonb_agg(jsonb_build_object('keyHash',${hash('r.id')},'rowHash',${hash('to_jsonb(r)')},
   'stableHash',${hash("to_jsonb(r)-array['attempt_count','next_attempt_at']")},'xmin',r.xmin::text,
   'xactStatus',case when pg_snapshot_xmax(pg_current_snapshot())::text::numeric<4294967296 then pg_xact_status(r.xmin::text::xid8) end,
   'attemptCount',r.attempt_count,'nextAttemptAt',r.next_attempt_at,'createdAt',r.created_at)
   order by r.id),'[]'::jsonb) from ${outbox} r`;
 return `'backgroundObservedAt',clock_timestamp(),'backgroundSnapshotStartedAt',transaction_timestamp(),
  'backgroundSnapshotXmax',pg_snapshot_xmax(pg_current_snapshot())::text,
  'backgroundSnapshotEpochZero',pg_snapshot_xmax(pg_current_snapshot())::text::numeric<4294967296,
  'backgroundSnapshotXip',(select coalesce(jsonb_agg(x::text),'[]'::jsonb) from pg_snapshot_xip(pg_current_snapshot()) x),
  'backgroundRows',jsonb_build_object(${literal(batch)},(${batches}),${literal(outbox)},(${outboxes})),
  'backgroundWriterRoutines',(select coalesce(jsonb_agg(r order by r->>'schema',r->>'identity'),'[]'::jsonb)
    from catalogue c cross join lateral jsonb_array_elements(c.jsonb_build_object::jsonb->'routines') r
    where r->>'identity' ~ '^(candidate_daily_projection_claim_v1|_candidate_daily_json_sha256_v1|tsfin_dequeue_batch_ids|tsfin_work_fail)\\(')`;
}
export function transactionIdentitySql(){return `select 'HR_REHEARSAL_TRANSACTION_JSON '||jsonb_build_object('database',current_database(),'backendPid',pg_backend_pid(),'xid',pg_current_xact_id()::text,'epochZero',pg_current_xact_id()::text::numeric<4294967296)::text;`;}
export function fixtureObservationSql(unit){
 requireThat(Number.isSafeInteger(unit)&&unit>=0,'REHEARSAL_FIXTURE_UNIT_REFUSED');
 const pieces=backgroundRelations.map(relation=>`select ${literal(relation)} relation,${hash('r.'+key(relation))} key_hash from ${relation} r
   where pg_current_xact_id()::text::numeric<4294967296 and pg_xact_status(r.xmin::text::xid8)='in progress'`);
 return `reset role; reset session authorization;
 select 'HR_FIXTURE_OBSERVATION_JSON '||jsonb_build_object('unit',${unit},'database',current_database(),'backendPid',pg_backend_pid(),
   'xid',pg_current_xact_id()::text,'epochZero',pg_current_xact_id()::text::numeric<4294967296,
   'rows',(select coalesce(jsonb_agg(jsonb_build_object('relation',relation,'keyHash',key_hash) order by relation,key_hash),'[]'::jsonb)
     from (${pieces.join(' union all ')}) fixture_rows))::text;`;
}
export function validateFixtureObservations(transaction,observations,expectedUnits){
 requireThat(transaction?.database==='cloudtms_test_clone'&&transaction.epochZero===true&&/^[1-9][0-9]*$/.test(transaction.xid??'')
  &&Number.isSafeInteger(transaction.backendPid)&&transaction.backendPid>0&&BigInt(transaction.xid)<4294967296n,'REHEARSAL_TRANSACTION_IDENTITY_REFUSED');
 requireThat(Array.isArray(observations)&&JSON.stringify(observations.map(r=>r.unit))===JSON.stringify(expectedUnits),'REHEARSAL_FIXTURE_CAPTURE_INCOMPLETE');
 for(const r of observations){requireThat(r.database===transaction.database&&r.backendPid===transaction.backendPid&&r.xid===transaction.xid&&r.epochZero===true
  &&Array.isArray(r.rows),'REHEARSAL_FIXTURE_CAPTURE_IDENTITY_REFUSED');for(const row of r.rows)requireThat(backgroundRelations.includes(row.relation)&&validHash(row.keyHash),'REHEARSAL_FIXTURE_CAPTURE_ROW_REFUSED');}
 return observations;
}
export function freshFixtureProofSql(transaction,observations,before){
 const all=backgroundRelations.flatMap(relation=>{const originals=new Set(before.backgroundRows[relation].map(r=>r.keyHash));
  const keys=[...new Set(observations.flatMap(r=>r.rows.filter(v=>v.relation===relation&&!originals.has(v.keyHash)).map(v=>v.keyHash)))].sort();
  return [{relation,keys}];});
 for(const row of all)for(const h of row.keys)requireThat(validHash(h),'REHEARSAL_FIXTURE_KEY_HASH_REFUSED');
 const queries=all.map(({relation,keys})=>`select ${literal(relation)} relation,${keys.length} observed_fixture_keys,count(*) remaining
  from ${relation} r where ${hash('r.'+key(relation))}=any(array[${keys.map(literal).join(',')}]::text[])`);
 return `select jsonb_build_object('database',current_database(),'backendPid',pg_backend_pid(),'connectedFresh',true,
   'rehearsalBackendPid',${transaction.backendPid},'rehearsalXid',${literal(transaction.xid)},'parentStatus',pg_xact_status(${literal(transaction.xid)}::xid8),
   'relations',(select jsonb_agg(to_jsonb(q) order by relation) from (${queries.join(' union all ')}) q))::text;`;
}
function completeRows(snapshot,relation){
 const aggregates=new Map((snapshot.businessRelations??[]).map(r=>[r.relation,r]));
 requireThat(aggregates.size===(snapshot.businessRelations??[]).length,'REHEARSAL_BUSINESS_RELATION_DUPLICATE');
 const rows=snapshot.backgroundRows?.[relation],aggregate=aggregates.get(relation);
 requireThat(Array.isArray(rows)&&aggregate&&rows.length===aggregate.count,'REHEARSAL_BACKGROUND_MAP_INCOMPLETE');
 const result=new Map();for(const row of rows){requireThat(validHash(row.keyHash)&&validHash(row.rowHash)&&!result.has(row.keyHash),'REHEARSAL_BACKGROUND_ROW_MAP_REFUSED');result.set(row.keyHash,row);}
 requireThat(digest(rows.map(r=>r.rowHash).sort().join('|'))===aggregate.hash,'REHEARSAL_BACKGROUND_MAP_FINGERPRINT_MISMATCH');
 return result;
}
export function qualifyLiveRestoration(before,after,{transaction,observations,expectedUnits,fixtureProof,writerProof}={}){
 validateFixtureObservations(transaction,observations,expectedUnits);
 requireThat(fixtureProof?.database===transaction.database&&fixtureProof.connectedFresh===true&&fixtureProof.parentStatus==='aborted'
  &&fixtureProof.rehearsalBackendPid===transaction.backendPid&&fixtureProof.rehearsalXid===transaction.xid
  &&fixtureProof.backendPid!==transaction.backendPid&&fixtureProof.relations?.length===2
  &&JSON.stringify(fixtureProof.relations.map(r=>r.relation).sort())===JSON.stringify([...backgroundRelations].sort())
  &&fixtureProof.relations.every(r=>r.remaining===0),'REHEARSAL_FIXTURE_ABSENCE_UNPROVED');
 requireThat(writerProof?.matched===true&&Array.isArray(writerProof.routines)&&writerProof.routines.length>=3,'REHEARSAL_BACKGROUND_WRITER_SOURCE_UNPROVED');
 const left=new Map((before.businessRelations??[]).map(r=>[r.relation,r])),right=new Map((after.businessRelations??[]).map(r=>[r.relation,r]));
 requireThat(left.size===before.businessRelations.length&&right.size===after.businessRelations.length&&JSON.stringify([...left.keys()].sort())===JSON.stringify([...right.keys()].sort()),'REHEARSAL_RELATION_FINGERPRINT_SCOPE_DRIFT');
 for(const snapshot of [before,after])requireThat(snapshot.backgroundSnapshotEpochZero===true&&/^[1-9][0-9]*$/.test(snapshot.backgroundSnapshotXmax??'')
  &&BigInt(snapshot.backgroundSnapshotXmax)<4294967296n&&Array.isArray(snapshot.backgroundSnapshotXip)
  &&snapshot.backgroundSnapshotXip.every(x=>/^[1-9][0-9]*$/.test(x)&&BigInt(x)<BigInt(snapshot.backgroundSnapshotXmax)),'REHEARSAL_BACKGROUND_SNAPSHOT_EPOCH_UNPROVED');
 const start=Date.parse(before.backgroundSnapshotStartedAt),end=Date.parse(after.backgroundObservedAt);requireThat(Number.isFinite(start)&&Number.isFinite(end)&&end>=start
  &&BigInt(after.backgroundSnapshotXmax)>=BigInt(before.backgroundSnapshotXmax),'REHEARSAL_BACKGROUND_WINDOW_REFUSED');
 const originalKeys=new Map(backgroundRelations.map(relation=>[relation,new Set(completeRows(before,relation).keys())]));
 for(const relation of backgroundRelations)completeRows(after,relation);
 const ownKeys=new Set(observations.flatMap(r=>r.rows.filter(v=>!originalKeys.get(v.relation).has(v.keyHash)).map(v=>v.relation+'|'+v.keyHash))),qualified=[];
 for(const row of fixtureProof.relations){const expected=new Set(observations.flatMap(r=>r.rows.filter(v=>v.relation===row.relation&&!originalKeys.get(v.relation).has(v.keyHash)).map(v=>v.keyHash))).size;
  requireThat(Number.isSafeInteger(row.observed_fixture_keys)&&row.observed_fixture_keys===expected,'REHEARSAL_FIXTURE_ABSENCE_SCOPE_DRIFT');}
 for(const [relation,old]of left){const next=right.get(relation);if(old.count===next.count&&old.hash===next.hash)continue;
  requireThat(backgroundRelations.includes(relation),'REHEARSAL_UNATTRIBUTED_BUSINESS_CHANGE');
  const a=completeRows(before,relation),b=completeRows(after,relation);let created=0,updated=0;
  for(const [id,prior]of a)requireThat(b.has(id),'REHEARSAL_BACKGROUND_ROW_DISAPPEARED');
  for(const [id,current]of b){const prior=a.get(id);if(prior?.rowHash===current.rowHash)continue;
   requireThat(!ownKeys.has(relation+'|'+id)&&current.xactStatus==='committed'&&current.xmin!==transaction.xid&&/^[1-9][0-9]*$/.test(current.xmin??'')
    &&(BigInt(current.xmin)>=BigInt(before.backgroundSnapshotXmax)||before.backgroundSnapshotXip.includes(current.xmin)),'REHEARSAL_BACKGROUND_TRANSACTION_UNPROVED');
   if(relation===backgroundRelations[0]){
    const times=[current.createdAt,current.updatedAt,current.completedAt].map(Date.parse);
    requireThat(!prior&&current.nativeClaimProved===true&&times.every(t=>Number.isFinite(t)&&t<=end)
      &&times[0]<=times[1]&&times[1]<=times[2]
      &&(times[0]>=start||before.backgroundSnapshotXip.includes(current.xmin)),'REHEARSAL_BACKGROUND_CLAIM_UNPROVED');created++;
   }else{
    const next=Date.parse(current.nextAttemptAt),failureDelayMinutes=current.attemptCount>=7?60:Math.max(1,2**Math.max(0,current.attemptCount-1));
    const nativeOrigins=[next-300000,next-failureDelayMinutes*60000];
    requireThat(prior&&validHash(current.stableHash)&&current.stableHash===prior.stableHash&&Number.isSafeInteger(current.attemptCount)
      &&current.attemptCount>prior.attemptCount&&Number.isFinite(next)&&nativeOrigins.some(origin=>origin<=end
       &&(origin>=start||before.backgroundSnapshotXip.includes(current.xmin)))
      &&next>=Date.parse(prior.nextAttemptAt??0),'REHEARSAL_BACKGROUND_RETRY_UNPROVED');updated++;
   }
  }
  qualified.push({relation,beforeCount:old.count,afterCount:next.count,beforeHash:old.hash,afterHash:next.hash,created,updated,
    attribution:relation===backgroundRelations[0]?'NATIVE_SIGNED_SYSTEM_PROJECTION_CLAIM_RECEIPT':'EXISTING_NATIVE_FINANCE_OUTBOX_RETRY',fixtureMatches:0});
 }
 const result={status:'LIVE_TEST_RESTORATION_QUALIFIED',ownerRuling:'14 October 2026: background changes require attribution, never job pause',
  relationCount:left.size,unchangedBusinessRelations:left.size-qualified.length,qualifiedBusinessChanges:qualified,
  parentRollback:{database:transaction.database,backendPid:transaction.backendPid,xid:transaction.xid,status:'aborted',connectedFresh:true},
  fixtureAbsence:{allUnchangedRelationsBound:true,exceptionCaptureUnits:observations.length,observedExceptionFixtureKeys:fixtureProof.relations.reduce((n,r)=>n+r.observed_fixture_keys,0),remaining:0},
  writerProof,backgroundPaused:false};const qualifiedProof={...result,proofSha256:digest(JSON.stringify(result))};
 qualifiedFrames.set(qualifiedProof,{before:snapshotBinding(before),after:snapshotBinding(after),proof:digest(JSON.stringify(qualifiedProof))});
 return qualifiedProof;
}
