// Four exact catalogue-name DROP constructions, contingent on their actual
// checked compiler/prebody packets. Source construction only; no DB execution.
import fs from 'node:fs';import path from 'node:path';import{fileURLToPath}from'node:url';
import{canonical,digest,exactAtomicBody}from'./cloudtms-managed-release-sql.mjs';
import{compileManagedUnit,unitCheckpointSql,assertWriterSql,migrationLedgerSql}from'./cloudtms-managed-release.mjs';
import{compiledAtomicRehearsalUnit}from'./healthroster-install1-full-rehearsal.mjs';
import{mapExecutableSqlSource}from'./cloudtms-db-release-lib.mjs';
const repoRoot=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
export const reviewPath='docs/healthroster-build/v6-owner-delivery-20261016/F23_PREBODY_DYNAMIC_REVIEW_20261016/CLASSIFICATION.json';
export const reviewSha256='69d2b3aee092e78e36386642a8fbf65df406d5fc9f5d8e6a6d42bccf993895b0';
const targets=new Set(['supabase/migrations/13102026_1430_weekly_source_invoice_money_origins.sql','supabase/migrations/14102026_0400_healthroster_protected_source_capability.sql']);
const fail=code=>{throw Error('HEALTHROSTER_PREBODY_DYNAMIC_'+code);};
const same=(a,b)=>JSON.stringify(a)===JSON.stringify(b);
// The separately reviewed 16Oct RPC reload tail changed one unrelated replay
// pin. Reverse only that exact whole-file successor for the original prebody
// compiler review; the four immutable migration packets remain checked below.
export function prebodyDependencySource(input,value){
 const source=canonical(value);
 // Exact approved stdout/snapshot successors have no prebody compiler effect.
 const historicalDependencies=[{"file":"scripts/bpay-next-final-definers.mjs","priorSha256":"51ab7fd6d5c582ee7f4cab612502c4fc7ac11088cb1a996dd2cf1e6561468d09","currentSha256":"b6a8143f2bae077a4b72857419b4f8fab9f7652181f6ad3bf1fdcfbd4b77bafa","hunks":[{"before":"\nconst git = args => {\n  const result = spawnSync('git', args, { cwd: repoRoot, encoding: 'utf8', windowsHide: true });\n  if (result.status !== 0) throw Error('BPAY_NEXT_FINAL_DEFINER_GIT_FAILED');\n  return result.stdout.split(/\\r?\\n/).filter(Boolean).map(file => file.replaceAll('\\\\', '/'));\n","after":"\nconst git = args => {\n  const result = spawnSync('git', args, { cwd: repoRoot, encoding: 'utf8', maxBuffer: 8 * 1024 * 1024, windowsHide: true });\n  if (result.status !== 0) throw Error('BPAY_NEXT_FINAL_DEFINER_GIT_FAILED');\n  return result.stdout.split(/\\r?\\n/).filter(Boolean).map(file => file.replaceAll('\\\\', '/'));\n"}]},{"file":"scripts/healthroster-install1-full-rehearsal.mjs","priorSha256":"af2b6b591a8d6c251ad08507130bd2cf0cc9861d966055174e511de9c4102754","currentSha256":"31acd390acf2ef6dbbd062fbb157feea66f9b5d328440414e67b2bfd4a3f78ab","hunks":[{"before":"import {assertTurnIdentity,ownTurnActivityExclusionSql} from './healthroster-turn-control.mjs';\nimport {verifierClosure} from './healthroster-install1-verifier-census.mjs';\nimport {backgroundSnapshotFieldsSql,fixtureObservationSql,transactionIdentitySql,validateFixtureObservations,assertQualifiedLiveRestoration} from './healthroster-live-test-restoration.mjs';\nimport {assertFreshInstall2NativeGate} from './healthroster-install2-native-gate.mjs';\nimport {compileReviewedManagedEnvelope,managedEnvelopeReviewPath,managedCalleeAuditPath} from './healthroster-reviewed-managed-envelope.mjs';\n","after":"import {assertTurnIdentity,ownTurnActivityExclusionSql} from './healthroster-turn-control.mjs';\nimport {verifierClosure} from './healthroster-install1-verifier-census.mjs';\nimport {boundBackgroundSnapshotFieldsSql,fixtureObservationSql,transactionIdentitySql,validateFixtureObservations,assertQualifiedLiveRestoration} from './healthroster-live-test-restoration.mjs';\nimport {assertFreshInstall2NativeGate} from './healthroster-install2-native-gate.mjs';\nimport {compileReviewedManagedEnvelope,managedEnvelopeReviewPath,managedCalleeAuditPath} from './healthroster-reviewed-managed-envelope.mjs';\n"},{"before":"  chunks.push(`select ${literal(name)} relation,count(*) row_count,\n   encode(pg_catalog.sha256(pg_catalog.convert_to(coalesce(string_agg(row_hash,'|' order by row_hash),''),'UTF8')),'hex') row_fingerprint\n   from (select encode(pg_catalog.sha256(pg_catalog.convert_to(to_jsonb(t)::text,'UTF8')),'hex') row_hash from \"${row.schema}\".\"${row.name}\" t) hashed_rows`);\n }\n requireThat(chunks.length>0,'REHEARSAL_SNAPSHOT_BUSINESS_RELATIONS_REQUIRED');\n","after":"  chunks.push(`select ${literal(name)} relation,count(*) row_count,\n   encode(pg_catalog.sha256(pg_catalog.convert_to(coalesce(string_agg(row_hash,'|' order by row_hash),''),'UTF8')),'hex') row_fingerprint\n   from (select encode(pg_catalog.sha256(pg_catalog.convert_to(to_jsonb(t)::text,'UTF8')),'hex') row_hash from \"${row.schema}\".\"${row.name}\" t offset 0) hashed_rows`);\n }\n requireThat(chunks.length>0,'REHEARSAL_SNAPSHOT_BUSINESS_RELATIONS_REQUIRED');\n"},{"before":" 'repeatables',(select coalesce(jsonb_agg(to_jsonb(l) order by path),'[]'::jsonb) from private.cloudtms_repeatable_ledger l),\n 'releases',(select coalesce(jsonb_agg(to_jsonb(l) order by release_id),'[]'::jsonb) from private.cloudtms_database_releases l)) value),\n catalogue as (${exportQuery})\n select (identity_proof.jsonb_build_object::jsonb || jsonb_build_object(\n 'ledgerFingerprint',${hash('select value from ledger')},\n","after":" 'repeatables',(select coalesce(jsonb_agg(to_jsonb(l) order by path),'[]'::jsonb) from private.cloudtms_repeatable_ledger l),\n 'releases',(select coalesce(jsonb_agg(to_jsonb(l) order by release_id),'[]'::jsonb) from private.cloudtms_database_releases l)) value),\n catalogue as (${exportQuery})${observeLiveBusiness?', background_statement_snapshot as materialized (select pg_current_snapshot() snapshot_value)':''}\n select (identity_proof.jsonb_build_object::jsonb || jsonb_build_object(\n 'ledgerFingerprint',${hash('select value from ledger')},\n"},{"before":" 'businessRelations',(select coalesce(jsonb_agg(jsonb_build_object('relation',relation,'count',row_count,'hash',row_fingerprint) order by relation),'[]'::jsonb) from business),\n 'relationCensus',${literal(digest(JSON.stringify([...names].sort())))},\n ${observeLiveBusiness?backgroundSnapshotFieldsSql()+',':''}\n 'otherActivityCount',(select count(*) from pg_catalog.pg_stat_activity where datname=current_database() and ${ownTurnActivityExclusionSql(identity)} and (state <> 'idle' or xact_start is not null))))::text from identity_proof;`;\n}\n","after":" 'businessRelations',(select coalesce(jsonb_agg(jsonb_build_object('relation',relation,'count',row_count,'hash',row_fingerprint) order by relation),'[]'::jsonb) from business),\n 'relationCensus',${literal(digest(JSON.stringify([...names].sort())))},\n ${observeLiveBusiness?boundBackgroundSnapshotFieldsSql()+',':''}\n 'otherActivityCount',(select count(*) from pg_catalog.pg_stat_activity where datname=current_database() and ${ownTurnActivityExclusionSql(identity)} and (state <> 'idle' or xact_start is not null))))::text from identity_proof;`;\n}\n"}]},{"file":"scripts/healthroster-live-test-restoration.mjs","priorSha256":"fb4ffa42acf7cf319cdcb45a233babdb9720bb1bc73e4cb1f6ddcd3be696a4a6","currentSha256":"a91c1bf04f868ff372b20fb6beca239a7807fe9cc92c0c2682afffcd79554adc","hunks":[{"before":"const hash=sql=>`encode(pg_catalog.sha256(pg_catalog.convert_to((${sql})::text,'UTF8')),'hex')`;\nconst key=relation=>relation===backgroundRelations[0]?'batch_receipt_id':'id';\nexport function backgroundSnapshotFieldsSql(){\n const batch=backgroundRelations[0],outbox=backgroundRelations[1];\n // Native claim provenance: the completed native receipt, item identity,\n","after":"const hash=sql=>`encode(pg_catalog.sha256(pg_catalog.convert_to((${sql})::text,'UTF8')),'hex')`;\nconst key=relation=>relation===backgroundRelations[0]?'batch_receipt_id':'id';\nexport function backgroundSnapshotFieldsSql(){return renderBackgroundSnapshotFieldsSql(false);}\n// The existing zero-argument renderer remains byte-identical for legacy callers.\n// Only the owning snapshot query may bind this fixed one-row statement CTE.\nexport function boundBackgroundSnapshotFieldsSql(){return renderBackgroundSnapshotFieldsSql(true);}\nfunction renderBackgroundSnapshotFieldsSql(bound){\n const activeSnapshot=bound?'(select snapshot_value from background_statement_snapshot)':'pg_current_snapshot()';\n const batch=backgroundRelations[0],outbox=backgroundRelations[1];\n // Native claim provenance: the completed native receipt, item identity,\n"},{"before":"  and r.terminal_response_sha256=private._candidate_daily_json_sha256_v1(r.terminal_response_body)`;\n const batches=`select coalesce(jsonb_agg(jsonb_build_object('keyHash',${hash('r.batch_receipt_id')},\n   'rowHash',${hash('to_jsonb(r)')},'xmin',r.xmin::text,'xactStatus',case when pg_snapshot_xmax(pg_current_snapshot())::text::numeric<4294967296 then pg_xact_status(r.xmin::text::xid8) end,\n   'nativeClaimProved',coalesce((${proof}),false),'createdAt',r.created_at_utc,'updatedAt',r.updated_at_utc,'completedAt',r.completed_at_utc)\n   order by r.batch_receipt_id),'[]'::jsonb) from ${batch} r`;\n const outboxes=`select coalesce(jsonb_agg(jsonb_build_object('keyHash',${hash('r.id')},'rowHash',${hash('to_jsonb(r)')},\n   'stableHash',${hash(\"to_jsonb(r)-array['attempt_count','next_attempt_at']\")},'xmin',r.xmin::text,\n   'xactStatus',case when pg_snapshot_xmax(pg_current_snapshot())::text::numeric<4294967296 then pg_xact_status(r.xmin::text::xid8) end,\n   'attemptCount',r.attempt_count,'nextAttemptAt',r.next_attempt_at,'createdAt',r.created_at)\n   order by r.id),'[]'::jsonb) from ${outbox} r`;\n return `'backgroundObservedAt',clock_timestamp(),'backgroundSnapshotStartedAt',transaction_timestamp(),\n  'backgroundSnapshotXmax',pg_snapshot_xmax(pg_current_snapshot())::text,\n  'backgroundSnapshotEpochZero',pg_snapshot_xmax(pg_current_snapshot())::text::numeric<4294967296,\n  'backgroundSnapshotXip',(select coalesce(jsonb_agg(x::text),'[]'::jsonb) from pg_snapshot_xip(pg_current_snapshot()) x),\n  'backgroundRows',jsonb_build_object(${literal(batch)},(${batches}),${literal(outbox)},(${outboxes})),\n  'backgroundWriterRoutines',(select coalesce(jsonb_agg(r order by r->>'schema',r->>'identity'),'[]'::jsonb)\n","after":"  and r.terminal_response_sha256=private._candidate_daily_json_sha256_v1(r.terminal_response_body)`;\n const batches=`select coalesce(jsonb_agg(jsonb_build_object('keyHash',${hash('r.batch_receipt_id')},\n   'rowHash',${hash('to_jsonb(r)')},'xmin',r.xmin::text,'xactStatus',case when pg_snapshot_xmax(${activeSnapshot})::text::numeric<4294967296 then pg_xact_status(r.xmin::text::xid8) end,\n   'nativeClaimProved',coalesce((${proof}),false),'createdAt',r.created_at_utc,'updatedAt',r.updated_at_utc,'completedAt',r.completed_at_utc)\n   order by r.batch_receipt_id),'[]'::jsonb) from ${batch} r`;\n const outboxes=`select coalesce(jsonb_agg(jsonb_build_object('keyHash',${hash('r.id')},'rowHash',${hash('to_jsonb(r)')},\n   'stableHash',${hash(\"to_jsonb(r)-array['attempt_count','next_attempt_at']\")},'xmin',r.xmin::text,\n   'xactStatus',case when pg_snapshot_xmax(${activeSnapshot})::text::numeric<4294967296 then pg_xact_status(r.xmin::text::xid8) end,\n   'attemptCount',r.attempt_count,'nextAttemptAt',r.next_attempt_at,'createdAt',r.created_at)\n   order by r.id),'[]'::jsonb) from ${outbox} r`;\n return `'backgroundObservedAt',clock_timestamp(),'backgroundSnapshotStartedAt',transaction_timestamp(),\n  'backgroundSnapshotXmax',pg_snapshot_xmax(${activeSnapshot})::text,\n  'backgroundSnapshotEpochZero',pg_snapshot_xmax(${activeSnapshot})::text::numeric<4294967296,\n  'backgroundSnapshotXip',(select coalesce(jsonb_agg(x::text),'[]'::jsonb) from pg_snapshot_xip(${activeSnapshot}) x),\n  'backgroundRows',jsonb_build_object(${literal(batch)},(${batches}),${literal(outbox)},(${outboxes})),\n  'backgroundWriterRoutines',(select coalesce(jsonb_agg(r order by r->>'schema',r->>'identity'),'[]'::jsonb)\n"}]}];
 const historical=historicalDependencies.find(row=>row.file===input.file&&row.priorSha256===input.sha256);
 if(historical&&digest(source)!==input.sha256){
  if(digest(source)!==historical.currentSha256)fail('BOUND_SOURCE_CHANGED');
  let reversed=source;for(const h of [...historical.hunks].reverse()){if(reversed.split(h.after).length!==2)fail('BOUND_SOURCE_CHANGED');reversed=reversed.replace(h.after,()=>h.before);}
  if(digest(reversed)!==input.sha256)fail('BOUND_SOURCE_CHANGED');return reversed;
 }
 // The reviewed Same/Separate adapter has no compiler or migration-packet effect.
 // Reconstruct its exact complete prior bytes for this historical proof only.
 if(input.file==='broker/src/weekly-source/off-rule-native-proxy.mjs'
  &&input.sha256==='4606dc7e262d1870c548f7382a259ffb48a0d06729fa4b8adbcf184495b3cdef'&&digest(source)!==input.sha256){
  if(digest(source)!=='c46b57f1b923e57674a13a7d0b2d19a0745bd583257174aea0aecf86d0ac4a0c')fail('BOUND_SOURCE_CHANGED');
  const changes=[["const uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;","const uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;"],["export const sourceOperations=Object.freeze(['weekly_source_upload_context_v1'","export const sourceOperations=Object.freeze(['weekly_source_office_recheck_begin_v1','weekly_source_upload_context_v1'"],["'weekly_exceptional_pay_stage_c1_request_v1']);","'weekly_exceptional_pay_stage_c1_request_v1','weekly_source_missing_accept_context_v1','weekly_source_missing_accept_prepare_atomic_v1','weekly_source_later_change_decide_atomic_v1']);"]];let reversed=source;
  for(const[before,after]of changes){if(reversed.split(after).length!==2)fail('BOUND_SOURCE_CHANGED');reversed=reversed.replace(after,()=>before);}
  if(digest(reversed)!==input.sha256)fail('BOUND_SOURCE_CHANGED');return reversed;
 }
 const prior='069e6d1d2361c6c8026b193116db565d40503b9a2e338ddb75c4b1dadb15d415';
 if(input.file!=='scripts/cloudtms-managed-execution-policy.mjs'||input.sha256!==prior||digest(source)===prior)return source;
 if(digest(source)!=='ba1ed5d23cdd639e814c7e009bfa07502dd633511e9e0be265d2a8486fa495d1')fail('BOUND_SOURCE_CHANGED');
 const before="['repeatable/28092026_1234_stage2_hosted_final_authority_reassert_v1.sql','4b918f87f5c0e5878b1b338b8f0db1e3aa130375439d15734a2f7f41cd277f53','SOURCE_REASSERT']";
 const after="['repeatable/28092026_1234_stage2_hosted_final_authority_reassert_v1.sql','3b276d4fe9cdd614aa10d5779c299f233ffae59e792b47e155a89a831e8d923c','SOURCE_REASSERT']";
 if(source.split(after).length!==2)fail('BOUND_SOURCE_CHANGED');
 const reversed=source.replace(after,()=>before);
 if(digest(reversed)!==prior)fail('BOUND_SOURCE_CHANGED');return reversed;
}
function load(root){
 const read=file=>fs.readFileSync(path.join(root,file)),bytes=read(reviewPath);
 if(digest(bytes)!==reviewSha256)fail('REVIEW_CHANGED');const review=JSON.parse(bytes);
 if(review.version!=='HEALTHROSTER_FOUR_PREBODY_DYNAMIC_SOURCE_CLASSIFICATION_V1'
  ||review.status!=='SOURCE_CONSTRUCTION_ONLY_NATIVE0400_NOT_RUN'||review.native0400!=='NOT_RUN'
  ||review.sourceConstructionOnly!==true||review.points!==4||review.files.length!==2
  ||review.files.reduce((n,f)=>n+f.points.length,0)!==4||review.databaseCalls!==0)fail('REVIEW_SCOPE_CHANGED');
 for(const input of review.dependencies){const value=read(input.file);
  if(digest(prebodyDependencySource(input,value))!==input.sha256||(input.preservedRawSha256&&digest(value)!==input.preservedRawSha256))fail('BOUND_SOURCE_CHANGED');
 }
 // Detect stale critical exports after an on-disk implementation replacement.
 for(const [file,fn]of[['scripts/cloudtms-managed-release.mjs',compileManagedUnit],['scripts/cloudtms-managed-release.mjs',unitCheckpointSql],['scripts/cloudtms-managed-release.mjs',migrationLedgerSql],['scripts/healthroster-install1-full-rehearsal.mjs',compiledAtomicRehearsalUnit],['scripts/cloudtms-db-release-lib.mjs',mapExecutableSqlSource]])
  if(!canonical(read(file)).includes(canonical(fn.toString())))fail('LOADED_COMPILER_CHANGED');
 if(digest(canonical(assertWriterSql))!==review.writerGuardSha256)fail('LOADED_WRITER_CHANGED');
 const priorBytes=read(review.priorReview.path);if(digest(priorBytes)!==review.priorReview.sha256)fail('PRIOR_REVIEW_CHANGED');
 const prior=JSON.parse(priorBytes);if(prior.version!=='HEALTHROSTER_CURRENT_DYNAMIC_SOURCE_REVIEW_V1'||prior.rows.flatMap(r=>r.points).length!==99)fail('PRIOR_REVIEW_CHANGED');
 const seen=new Set();for(const file of review.files){
  if(!targets.has(file.path)||seen.has(file.path)||file.kind!=='migration'||file.points.length!==(file.path.includes('13102026_1430_')?3:1))fail('REVIEW_SCOPE_CHANGED');seen.add(file.path);
  const keys=new Set();for(const point of file.points){
   if(point.count!==1||keys.has(point.keySha256)||digest(point.key)!==point.keySha256||digest(point.statement)!==point.statementSha256)fail('POINT_CHANGED');keys.add(point.keySha256);
   const parsed=JSON.parse(point.key);if(parsed[0]!==point.signature||parsed[1]!=='DYNAMIC_SQL_UNCLASSIFIED'||parsed[2]!==point.statement)fail('POINT_CHANGED');
   const original=prior.rows.filter(r=>r.file===file.path&&r.signature===point.signature).flatMap(r=>r.points).filter(p=>p.key===point.key);if(original.length!==1||original[0].bodySha256!==point.bodySha256||original[0].statementSha256!==point.statementSha256)fail('PRIOR_POINT_CHANGED');
   if(digest(canonical(read(point.bodyFile)).trim())!==point.bodySha256)fail('PRODUCER_CHANGED');
  }
 }
 return review;
}
function checkedPacket(unit,file,review,root){
 const entry=review.files.find(f=>f.path===file);if(!entry||unit.item?.path!==file||unit.item.sha256!==entry.sha256||unit.kind!=='migration')fail('PACKET_SCOPE_CHANGED');
 if(unit.exception||unit.ownerRlsEnvelope||unit.immutableGuardDependency||unit.contactClaimCompanion)fail('FOREIGN_ENVELOPE');
 const receipt=unit[entry.receiptField],other=entry.receiptField==='moneyPredecessorCompanion'?'capabilityPredecessorCompanion':'moneyPredecessorCompanion';
 if(!receipt||unit[other]||receipt.sourceRoot!==root||receipt.placement!=='BEFORE_IMMUTABLE_BODY_SAME_TRANSACTION'
  ||receipt.version!==entry.version||receipt.companionPath!==entry.program.file||receipt.companionSha256!==entry.program.sha256
  ||!same(receipt.statementSha256,entry.program.statementSha256)||receipt.migrationPath!==file||receipt.migrationSha256!==entry.sha256
  ||receipt.closureSha256!==entry.closureSha256||receipt.sql!==canonical(fs.readFileSync(path.join(root,entry.program.file))))fail('RECEIPT_CHANGED');
 const variant=entry.variants.find(v=>v.sourceSha256===digest(unit.closure?.expanded??''));
 if(!variant||unit.closure.path!==file||unit.closure.closureHash!==entry.closureSha256
  ||unit.body!==exactAtomicBody(unit.closure.expanded)||digest(unit.body)!==variant.bodySha256||receipt.bodySha256!==variant.bodySha256)fail('BODY_CHANGED');
 const packet=unitCheckpointSql(unit,review.context),expected=`begin;\n${assertWriterSql}\n${receipt.sql}\n${unit.body}\n${assertWriterSql}\n${migrationLedgerSql(unit.item,review.context)}\ncommit;`;
 if(packet!==expected||digest(packet)!==variant.releasePacketSha256)fail('PACKET_ORDER_CHANGED');
 const rehearsal=compiledAtomicRehearsalUnit(unit);
 if(rehearsal.sql!==receipt.sql+'\n'+unit.body||rehearsal.sqlHash!==digest(rehearsal.sql)
  ||digest(rehearsal.sql)!==variant.rehearsalSqlSha256||digest(JSON.stringify(rehearsal))!==variant.rehearsalReceiptSha256
  ||!same(rehearsal,variant.rehearsalReceipt))fail('REHEARSAL_RECEIPT_CHANGED');
 return entry;
}
// Exposed for finite refusal tests; no callback, executor or alternate approval.
export function assertReviewedPrebodyPacket(unit,file,{root=repoRoot}={}){
 if(!targets.has(file))fail('PACKET_SCOPE_CHANGED');return checkedPacket(unit,file,load(root),root);
}
export function createReviewedPrebodyDynamicClassifier(root=repoRoot){
 return function reviewedPrebodyDynamicHazards(source,file,baseline,violations,parts){
  if(!targets.has(file))return baseline;const review=load(root),entry=review.files.find(f=>f.path===file);
  if(digest(canonical(source))!==entry.sha256||digest(canonical(fs.readFileSync(path.join(root,file))))!==entry.sha256)fail('IMMUTABLE_SOURCE_CHANGED');
  const raw=violations.filter(v=>v.hazard==='DYNAMIC_SQL_UNCLASSIFIED');if(raw.length!==entry.points.length)fail('POINT_CENSUS_CHANGED');
  const unit=compileManagedUnit({path:file,sha256:entry.sha256},'migration',{root,mapSource:mapExecutableSqlSource});checkedPacket(unit,file,review,root);
  const result=new Map(baseline);for(const point of entry.points){
   const actual=raw.filter(v=>digest(v.key)===point.keySha256);if(actual.length!==1||actual[0].key!==point.key)fail('POINT_CHANGED');
   const body=parts.filter(p=>p.signature===point.signature&&p.text.includes(point.statement));
   if(body.length!==1||body[0].unclassified||digest(body[0].text.trim())!==point.bodySha256)fail('PRODUCER_CHANGED');
   result.set(point.key,Math.max(result.get(point.key)??0,1));
  }return result;
 };
}
export const reviewedPrebodyDynamicHazards=createReviewedPrebodyDynamicClassifier();
